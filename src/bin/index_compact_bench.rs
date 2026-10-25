// End-to-end verification bench for the v13+v14 compact INT index
// work. Two profiles:
//
//   single (default): single-column BIGINT PK (Phase 1-4 INT compact)
//   lineitem        : TPC-H lineitem-shape 2-column PK + 3 single-INT
//                     secondaries, exercises v14 INT2 + LOAD-DATA
//                     bulk-load fast path (Fix A) together.
//
// Run with:
//   cargo run --release --bin index_compact_bench                 (single)
//   MYDB_BENCH_PROFILE=lineitem cargo run --release --bin index_compact_bench
//
// Env vars (single profile):
//   MYDB_BENCH_ROWS  — rows (default 200_000)
//
// Env vars (lineitem profile):
//   MYDB_BENCH_ORDERS  — order count (default 100_000; lineitem rows
//                        ≈ orders × ~4 = ~400k for sf-equivalent test)

use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use mydb::parser::{ColumnDef, DataType, IndexDef};
use mydb::storage::{
    Database, DurabilityMode, IndexBound, IndexKey, KeyPart, StartupValidationMode, StorageConfig,
};
use mydb::types::{QueryError, Value};

fn main() -> Result<(), QueryError> {
    let profile = env::var("MYDB_BENCH_PROFILE").unwrap_or_else(|_| "single".into());
    match profile.as_str() {
        "lineitem" => run_lineitem(),
        _ => run_single(),
    }
}

fn run_single() -> Result<(), QueryError> {
    let rows: i64 = env::var("MYDB_BENCH_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);
    let data_dir = bench_dir("mydb-index-bench");
    if data_dir.exists() {
        std::fs::remove_dir_all(&data_dir).map_err(io_err)?;
    }
    let db = Database::open_with_config(data_dir.clone(), bench_storage_config())?;

    db.create_table(
        "tbl",
        vec![
            column("id", DataType::BigInt),
            column("payload", DataType::BigInt),
        ],
        vec!["id".to_string()],
        vec![],
    )?;

    let insert_start = Instant::now();
    let chunk = 50_000i64;
    let mut next_id = 0i64;
    while next_id < rows {
        let end = (next_id + chunk).min(rows);
        let txn = db.begin_txn()?;
        db.insert_rows(
            txn,
            "test.tbl",
            (next_id..end)
                .map(|i| vec![Value::Int(i), Value::Int(i * 3)])
                .collect(),
        )?;
        db.commit_txn(txn.txn_id)?;
        next_id = end;
    }
    db.flush_dirty_data()?;
    let insert_elapsed = insert_start.elapsed();

    let idx_dir = mydb::storage::naming::schema_indexes_dir(&data_dir, "test");
    let idx_size = total_size(&idx_dir)?;

    let table = db.table("test.tbl")?;
    let pk = table
        .indexes
        .iter()
        .find(|i| i.primary)
        .expect("primary index");
    let lookup_count: i64 = 10_000;
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    let lookup_start = Instant::now();
    let mut found = 0i64;
    for _ in 0..lookup_count {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let target = (seed % rows as u64) as i64;
        let rows_found = db.lookup_index(&table, pk, IndexKey::from(vec![KeyPart::Int(target)]))?;
        found += rows_found.len() as i64;
    }
    let lookup_elapsed = lookup_start.elapsed();
    assert_eq!(found, lookup_count);

    let scan_start = Instant::now();
    let scanned = db.range_index_bounds_visible(
        &table,
        pk,
        Some(IndexBound {
            key: IndexKey::from(vec![KeyPart::Int(0)]),
            inclusive: true,
        }),
        Some(IndexBound {
            key: IndexKey::from(vec![KeyPart::Int(rows - 1)]),
            inclusive: true,
        }),
        db.read_only_statement_view(mydb::storage::IsolationLevel::ReadCommitted, None),
    )?;
    let scan_elapsed = scan_start.elapsed();
    assert_eq!(scanned.len() as i64, rows);

    let layout_counts = scan_layouts(&idx_dir);
    print_layout_dist(&layout_counts);

    println!("=== single (1-col BIGINT PK) ===");
    println!("rows                       : {}", rows);
    println!(
        "insert (incl. flush)       : {:.2} s",
        insert_elapsed.as_secs_f64()
    );
    println!(
        "idx file size              : {} bytes ({:.2} B/row, {:.1}x raw i64)",
        idx_size,
        idx_size as f64 / rows as f64,
        idx_size as f64 / (8.0 * rows as f64),
    );
    println!(
        "point lookup avg           : {:.2} µs ({} lookups in {:.2} ms)",
        lookup_elapsed.as_secs_f64() * 1_000_000.0 / lookup_count as f64,
        lookup_count,
        lookup_elapsed.as_secs_f64() * 1_000.0,
    );
    println!(
        "full range scan            : {:.2} ms ({:.2} M rows/s)",
        scan_elapsed.as_secs_f64() * 1_000.0,
        rows as f64 / scan_elapsed.as_secs_f64() / 1_000_000.0,
    );
    Ok(())
}

fn run_lineitem() -> Result<(), QueryError> {
    // sf=1 lineitem has ~6M rows over ~1.5M orders (4 lines/order avg).
    // Default to 100k orders × 4 lines = 400k rows for a fast bench.
    let orders: i64 = env::var("MYDB_BENCH_ORDERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);
    let lines_per_order: i64 = 4;
    let rows = orders * lines_per_order;

    let data_dir = bench_dir("mydb-index-bench-lineitem");
    if data_dir.exists() {
        std::fs::remove_dir_all(&data_dir).map_err(io_err)?;
    }
    let db = Database::open_with_config(data_dir.clone(), bench_storage_config())?;

    db.create_table(
        "lineitem",
        vec![
            column("l_orderkey", DataType::BigInt),
            column("l_linenumber", DataType::Int),
            column("l_partkey", DataType::Int),
            column("l_suppkey", DataType::Int),
            column("l_quantity", DataType::Int),
        ],
        // PRIMARY KEY (l_orderkey, l_linenumber)
        vec!["l_orderkey".to_string(), "l_linenumber".to_string()],
        // Three single-column INT secondaries — same shape as TPC-H
        // lineitem's orderkey/partkey/suppkey single-col indexes.
        vec![
            IndexDef {
                name: Some("idx_orderkey".to_string()),
                columns: vec!["l_orderkey".to_string()],
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
            },
            IndexDef {
                name: Some("idx_partkey".to_string()),
                columns: vec!["l_partkey".to_string()],
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
            },
            IndexDef {
                name: Some("idx_suppkey".to_string()),
                columns: vec!["l_suppkey".to_string()],
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
            },
        ],
    )?;

    let insert_start = Instant::now();
    let chunk_orders: i64 = 25_000;
    let mut next_order = 0i64;
    while next_order < orders {
        let end = (next_order + chunk_orders).min(orders);
        let txn = db.begin_txn()?;
        let mut rows_chunk: Vec<Vec<Value>> =
            Vec::with_capacity(((end - next_order) * lines_per_order) as usize);
        for ok in next_order..end {
            for line in 1..=lines_per_order {
                rows_chunk.push(vec![
                    Value::Int(ok),
                    Value::Int(line),
                    // partkey/suppkey deterministic but spread out so
                    // the per-page span is non-trivial (mimics TPC-H).
                    Value::Int((ok * 13 + line) % 200_000),
                    Value::Int((ok * 7 + line) % 10_000),
                    Value::Int(line * 10),
                ]);
            }
        }
        db.insert_rows(txn, "test.lineitem", rows_chunk)?;
        db.commit_txn(txn.txn_id)?;
        next_order = end;
    }
    db.flush_dirty_data()?;
    let insert_elapsed = insert_start.elapsed();

    let idx_dir = mydb::storage::naming::schema_indexes_dir(&data_dir, "test");
    let mut per_index_bytes: Vec<(String, u64)> = Vec::new();
    for entry in std::fs::read_dir(&idx_dir).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        if entry.path().extension().and_then(|s| s.to_str()) != Some("idx") {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let size = entry.metadata().map_err(io_err)?.len();
        per_index_bytes.push((name, size));
    }
    per_index_bytes.sort();
    let total_idx_size: u64 = per_index_bytes.iter().map(|(_, s)| *s).sum();

    let layout_counts = scan_layouts(&idx_dir);

    // PK point-lookup latency (composite key).
    let table = db.table("test.lineitem")?;
    let pk = table
        .indexes
        .iter()
        .find(|i| i.primary)
        .expect("primary index");
    let lookup_count: i64 = 5_000;
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    let pk_start = Instant::now();
    for _ in 0..lookup_count {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let ok = (seed % orders as u64) as i64;
        let line = ((seed >> 16) % lines_per_order as u64) as i64 + 1;
        db.lookup_index(
            &table,
            pk,
            IndexKey::from(vec![KeyPart::Int(ok), KeyPart::Int(line)]),
        )?;
    }
    let pk_elapsed = pk_start.elapsed();

    println!("=== lineitem (2-col PK + 3 single-INT secondaries) ===");
    println!("orders      : {}", orders);
    println!("rows        : {} ({} lines/order)", rows, lines_per_order);
    println!("insert      : {:.2} s", insert_elapsed.as_secs_f64());
    println!("\nindex file sizes:");
    for (name, size) in &per_index_bytes {
        println!(
            "  {name:<35} {:>10} bytes ({:.2} B/row)",
            size,
            *size as f64 / rows as f64
        );
    }
    println!(
        "  {:<35} {:>10} bytes ({:.2} B/row)",
        "TOTAL",
        total_idx_size,
        total_idx_size as f64 / rows as f64
    );
    println!();
    print_layout_dist(&layout_counts);
    println!(
        "\nPK (2-col) lookup avg      : {:.2} µs ({} lookups)",
        pk_elapsed.as_secs_f64() * 1_000_000.0 / lookup_count as f64,
        lookup_count
    );
    Ok(())
}

fn bench_dir(name: &str) -> PathBuf {
    env::var_os("MYDB_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(format!("/tmp/{name}")))
}

fn bench_storage_config() -> StorageConfig {
    StorageConfig {
        durability: DurabilityMode::Relaxed,
        checkpoint_interval: Duration::from_secs(60),
        checkpoint_wal_bytes: 64 * 1024 * 1024,
        lock_wait: Duration::from_secs(5),
        group_commit_window: Duration::ZERO,
        wal_segment_bytes: 64 * 1024 * 1024,
        buffer_pool_pages: 4_096,
        buffer_pool_io_threads: 1,
        buffer_pool_prefetch_pages: 4,
        undo_retention: Duration::from_secs(60),
        heap_update_reserve_bytes: 16 * 1024 / 4,
        startup_validation: StartupValidationMode::Off,
        startup_validate_indexes: false,
    }
}

fn column(name: &str, data_type: DataType) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type,
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

fn io_err(err: std::io::Error) -> QueryError {
    mydb::storage::error(mydb::error::catalog::ER_GET_ERRNO, format!("io: {err}"))
}

fn total_size(dir: &PathBuf) -> Result<u64, QueryError> {
    let mut total = 0u64;
    for entry in std::fs::read_dir(dir).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        if let Ok(meta) = entry.metadata() {
            total += meta.len();
        }
    }
    Ok(total)
}

fn scan_layouts(idx_dir: &PathBuf) -> std::collections::BTreeMap<u8, usize> {
    let mut layout_counts: std::collections::BTreeMap<u8, usize> = Default::default();
    const PAGE_SIZE: usize = 16384;
    let read_dir = match std::fs::read_dir(idx_dir) {
        Ok(d) => d,
        Err(_) => return layout_counts,
    };
    for entry in read_dir {
        let Ok(entry) = entry else { continue };
        if entry.path().extension().and_then(|s| s.to_str()) != Some("idx") {
            continue;
        }
        let Ok(bytes) = std::fs::read(entry.path()) else {
            continue;
        };
        let pages = bytes.len() / PAGE_SIZE;
        for p in 1..pages {
            let page = &bytes[p * PAGE_SIZE..(p + 1) * PAGE_SIZE];
            if u32::from_le_bytes(page[..4].try_into().unwrap()) != 0x4d_49_50_38 {
                continue;
            }
            let payload_off = 24;
            if page.len() < payload_off + 10 {
                continue;
            }
            let page_type = page[payload_off + 8];
            let layout_byte = page[payload_off + 9];
            let key = page_type.wrapping_mul(10).wrapping_add(layout_byte);
            *layout_counts.entry(key).or_default() += 1;
        }
    }
    layout_counts
}

fn print_layout_dist(layout_counts: &std::collections::BTreeMap<u8, usize>) {
    println!("page layout distribution:");
    for (key, count) in layout_counts {
        let page_type = key / 10;
        let layout = key % 10;
        let type_name = match page_type {
            1 => "leaf",
            2 => "internal",
            _ => "other",
        };
        let layout_name = match layout {
            0 => "generic",
            1 => "compact_time_u32",
            2 => "compact_date_u16",
            3 => "compact_decimal_i32",
            4 => "compact_decimal_i64",
            5 => "compact_int_u16",
            6 => "compact_int_u32",
            7 => "compact_int2",
            _ => "?",
        };
        println!("  {type_name:>8} {layout_name:>20} : {count}");
    }
}
