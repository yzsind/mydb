//! Full-text index performance baseline (scratchpad, not checked in).
//!
//! Usage from the repository root:
//!   cp scratchpad/ft_bench.rs src/bin/ft_bench.rs
//! Add the following Cargo.toml [[bin]] entry:
//!     [[bin]]
//!     name = "ft_bench"
//!     path = "src/bin/ft_bench.rs"
//!     required-features = ["tools"]
//!   cargo run --release --features tools --bin ft_bench
//!
//! Measure CREATE, N INSERTs, implicit online index construction, and median latency for
//! natural-language, boolean, phrase, and prefix queries. Default N=10_000; override with FT_N.

use mydb::engine::{QueryResponse, StorageQueryEngine};
use std::fmt::Write as _;
use std::time::Instant;

const INSERT_BATCH_SIZE: usize = 1_000;

const CORPUS_WORDS: &[&str] = &[
    "database",
    "systems",
    "storage",
    "engine",
    "index",
    "btree",
    "hash",
    "sort",
    "join",
    "query",
    "planner",
    "execution",
    "transaction",
    "recovery",
    "wal",
    "buffer",
    "pool",
    "replication",
    "consensus",
    "protocol",
    "network",
    "concurrency",
    "isolation",
    "snapshot",
    "compaction",
    "cache",
    "column",
    "vector",
    "search",
    "similarity",
    "matching",
    "ranking",
    "score",
    "relevance",
    "distributed",
    "coordinator",
    "leader",
    "follower",
    "commit",
    "abort",
    "durability",
    "checkpoint",
    "checkpoint",
    "undo",
    "redo",
    "mvcc",
    "gc",
    "reorganize",
];

fn make_doc(i: usize) -> String {
    // Use 8-16 pseudorandom words per row to give realistic tf/df distributions.
    let mut w = Vec::new();
    let n = 8 + (i * 7 % 9);
    for k in 0..n {
        w.push(
            CORPUS_WORDS
                [(i.wrapping_mul(2654435761) ^ k.wrapping_mul(11400714819)) % CORPUS_WORDS.len()],
        );
    }
    w.join(" ")
}

fn median(mut v: Vec<u128>) -> u128 {
    v.sort_unstable();
    v[v.len() / 2]
}

fn exec_ok(engine: &StorageQueryEngine, sql: &str) {
    engine
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {}", e.message()));
}

fn count_rows(engine: &StorageQueryEngine, sql: &str) -> usize {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs.rows.len(),
        _ => 0,
    }
}

fn main() {
    let n: usize = std::env::var("FT_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let iters: usize = std::env::var("FT_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(21);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut dir = std::env::temp_dir();
    dir.push(format!("mydb-ft-bench-{}-{}", std::process::id(), ts));
    std::fs::create_dir_all(&dir).unwrap();
    let engine = StorageQueryEngine::open(&dir).unwrap();
    eprintln!("[ft_bench] N={n} iters={iters} dir={:?}", dir);

    // Schema + insert
    let t0 = Instant::now();
    exec_ok(
        &engine,
        "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, FULLTEXT KEY ft (body))",
    );
    eprintln!(
        "[ft_bench] CREATE TABLE + FULLTEXT: {} ms",
        t0.elapsed().as_millis()
    );

    let t0 = Instant::now();
    for batch_start in (0..n).step_by(INSERT_BATCH_SIZE) {
        let batch_end = (batch_start + INSERT_BATCH_SIZE).min(n);
        let mut sql = String::with_capacity((batch_end - batch_start) * 128);
        sql.push_str("INSERT INTO docs VALUES ");
        for i in batch_start..batch_end {
            if i != batch_start {
                sql.push(',');
            }
            let doc = make_doc(i).replace('\'', "");
            write!(&mut sql, "({}, '{}')", i, doc).unwrap();
        }
        exec_ok(&engine, &sql);
    }
    let ins_ms = t0.elapsed().as_millis();
    eprintln!(
        "[ft_bench] INSERT {} rows: {} ms ({} rows/s)",
        n,
        ins_ms,
        (n as u128 * 1000).saturating_div(ins_ms.max(1))
    );

    // Warm — first query pays optional lazy structures
    let _ = count_rows(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('database')",
    );

    // Natural language
    let mut samples = Vec::with_capacity(iters);
    let mut sum_rows = 0usize;
    for _ in 0..iters {
        let t = Instant::now();
        sum_rows += count_rows(
            &engine,
            "SELECT id FROM docs WHERE MATCH(body) AGAINST('database engine') ORDER BY id LIMIT 20",
        );
        samples.push(t.elapsed().as_micros());
    }
    eprintln!(
        "[ft_bench] NL 'database engine': median {} µs (~{} rows/query)",
        median(samples),
        sum_rows / iters
    );

    // Boolean
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        let _ = count_rows(
            &engine,
            "SELECT id FROM docs WHERE MATCH(body) AGAINST('+database -mysql' IN BOOLEAN MODE) LIMIT 20",
        );
        samples.push(t.elapsed().as_micros());
    }
    eprintln!(
        "[ft_bench] BOOL '+database -mysql': median {} µs",
        median(samples)
    );

    // Phrase (needs adjacency verification via positions)
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        let _ = count_rows(
            &engine,
            "SELECT id FROM docs WHERE MATCH(body) AGAINST('\"database engine\"' IN BOOLEAN MODE) LIMIT 20",
        );
        samples.push(t.elapsed().as_micros());
    }
    eprintln!(
        "[ft_bench] PHRASE '\"database engine\"': median {} µs",
        median(samples)
    );

    // Prefix
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        let _ = count_rows(
            &engine,
            "SELECT id FROM docs WHERE MATCH(body) AGAINST('data*' IN BOOLEAN MODE) LIMIT 20",
        );
        samples.push(t.elapsed().as_micros());
    }
    eprintln!("[ft_bench] PREFIX 'data*': median {} µs", median(samples));
}
