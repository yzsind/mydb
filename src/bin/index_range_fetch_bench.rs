//! Measure end-to-end non-covering index range scans with heap fetches.
//!
//! Usage:
//! ```bash
//! MYDB_MRR_SORT=off cargo run --release --bin index_range_fetch_bench
//! MYDB_MRR_SORT=on  cargo run --release --bin index_range_fetch_bench
//! ```
//!
//! Deliberately decorrelate secondary index order from PK/RowId order using `bucket = (id * 7919) % NB`.
//! RowIds become nearly random within a bucket-key range, defeating heap page batching.
//! `MYDB_MRR_SORT=on` sorts chunks by (page_id, slot) before heap reads,
//! making `with_cached_heap_page` run once per page rather than once per row.
//!
//! Example output:
//! ```
//! MYDB_MRR_SORT=on  rows=98000 q1=12.4ms q2=11.9ms q3=12.1ms ...
//! ```
//!
//! Run twice on the same host and compare medians.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mydb::engine::{QueryResponse, StorageQueryEngine};
use mydb::types::QueryError;

const ROWS: i64 = 200_000;
const BUCKETS: i64 = 200;
const RUNS: usize = 6;

fn apply_env_defaults() {
    // Memory mode and a small buffer pool amplify non-covering heap-fetch cache misses.
    // Users can override these defaults by exporting the same environment variables.
    for (k, v) in [
        ("MYDB_DURABILITY", "memory"),
        ("MYDB_BUFFER_POOL_PAGES", "4096"),
        ("MYDB_STARTUP_VALIDATION", "off"),
    ] {
        if env::var(k).is_err() {
            env::set_var(k, v);
        }
    }
}

fn scratch_dir() -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("mydb-index-range-bench-{pid}-{unique}"))
}

fn run(engine: &StorageQueryEngine, sql: &str) -> Result<Duration, QueryError> {
    let start = Instant::now();
    let resp = engine.execute(sql)?;
    let elapsed = start.elapsed();
    // Sanity check: stable row count.
    if let QueryResponse::ResultSet(rs) = resp {
        if rs.rows.is_empty() {
            eprintln!("warning: 0 rows returned for {sql}");
        }
    }
    Ok(elapsed)
}

fn main() -> Result<(), QueryError> {
    let mrr_sort = env::var("MYDB_MRR_SORT").unwrap_or_else(|_| "off".into());
    eprintln!("[setup] ROWS={ROWS}, BUCKETS={BUCKETS}, MYDB_MRR_SORT={mrr_sort}");

    apply_env_defaults();
    let dir = scratch_dir();
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("scratch dir");
    let engine = StorageQueryEngine::open(&dir)?;

    engine.execute(
        "CREATE TABLE bench (\
         id BIGINT PRIMARY KEY, \
         bucket INT NOT NULL, \
         a INT NOT NULL, \
         b INT NOT NULL, \
         c VARCHAR(48), \
         d VARCHAR(48), \
         INDEX idx_bucket(bucket))",
    )?;

    // Bulk insert. Low bucket/id correlation significantly scrambles RowIds in secondary scans.
    let batch = 5_000i64;
    let mut next = 1i64;
    while next <= ROWS {
        let to = (next + batch - 1).min(ROWS);
        let mut sql = String::from("INSERT INTO bench VALUES ");
        let mut first = true;
        for id in next..=to {
            if !first {
                sql.push(',');
            }
            first = false;
            let bucket = (id.wrapping_mul(7919)).rem_euclid(BUCKETS);
            sql.push_str(&format!(
                "({}, {}, {}, {}, 'pa-{}', 'pb-{}')",
                id,
                bucket,
                id % 31,
                id % 13,
                id,
                id
            ));
        }
        engine.execute(&sql)?;
        next = to + 1;
    }
    engine.execute("ANALYZE TABLE bench")?;
    eprintln!("[setup] table built, running queries");

    // Warm up once to exclude initial plan-cache construction and JIT overhead.
    let _ = run(
        &engine,
        "SELECT a, b, c FROM bench WHERE bucket BETWEEN 10 AND 80",
    )?;

    // Run RUNS repetitions and record each duration in microseconds.
    let mut rows_seen = 0usize;
    let mut times = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let dt = run(
            &engine,
            "SELECT a, b, c FROM bench WHERE bucket BETWEEN 10 AND 80",
        )?;
        times.push(dt);
    }
    // Read the row count separately from the timing loop to exclude output overhead.
    if let QueryResponse::ResultSet(rs) =
        engine.execute("SELECT a, b, c FROM bench WHERE bucket BETWEEN 10 AND 80")?
    {
        rows_seen = rs.rows.len();
    }

    let mut micros: Vec<u128> = times.iter().map(|d| d.as_micros()).collect();
    micros.sort_unstable();
    let median = micros[micros.len() / 2];
    let min = *micros.first().unwrap();
    let max = *micros.last().unwrap();

    println!(
        "MYDB_MRR_SORT={mrr_sort}  rows={rows_seen}  median={median}us  min={min}us  max={max}us  raw={micros:?}"
    );
    Ok(())
}
