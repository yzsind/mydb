//! Microbench for ORDER BY / TopK code paths.
//!
//! Spins up an in-process `StorageQueryEngine` over a fresh tempdir,
//! populates a small synthetic table, then times several SQL shapes
//! that exercise the optimisations landed in `sort/mod.rs`,
//! `select/eval.rs`, `select/plan.rs`, `select/parallel.rs`, and
//! `planner.rs`:
//!
//!   - Single-column ORDER BY (`#E` Int fast path).
//!   - ORDER BY + LIMIT (`#A` TopK scratch reuse, `#D` ParallelTopK).
//!   - GROUP BY ... ORDER BY agg-fn (`#C` PlanOrderProgram aggregate
//!     fingerprint cache).
//!   - Large random sort (`#B` cycle-leader permute).
//!
//! Run as:
//!
//!     cargo run --release --bin sort_microbench -- --rows 100000
//!
//! It prints median + p99 latencies over a fixed number of iterations.
//! No client; runs entirely in-process, so the wall-clock isn't
//! polluted by MySQL packet I/O.

use std::env;
use std::time::{Duration, Instant};

use mydb::engine::api::QueryResponse;
use mydb::engine::StorageQueryEngine;

fn main() {
    let rows: usize = arg_value("--rows").unwrap_or(100_000);
    let iters: usize = arg_value("--iters").unwrap_or(20);
    let warmup: usize = arg_value("--warmup").unwrap_or(3);
    let groups: usize = arg_value("--groups").unwrap_or(50);

    let tmp_path =
        std::env::temp_dir().join(format!("mydb-sort-microbench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp_path);
    std::fs::create_dir_all(&tmp_path).expect("create tmpdir");
    // RAII cleanup at the end.
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(tmp_path.clone());
    let engine = StorageQueryEngine::open(&tmp_path).expect("open engine");

    eprintln!(
        "sort_microbench: rows={} iters={} warmup={} groups={}",
        rows, iters, warmup, groups
    );

    populate(&engine, rows, groups);

    let queries: &[(&str, &str)] = &[
        // #B + #E: full sort of N rows on a single Int column.
        ("Sort ASC (single Int col)", "SELECT id FROM t ORDER BY x"),
        // #A + #E: TopK with small k.
        (
            "TopK k=10 (Int col)",
            "SELECT id FROM t ORDER BY x LIMIT 10",
        ),
        (
            "TopK k=1000 (Int col)",
            "SELECT id FROM t ORDER BY x LIMIT 1000",
        ),
        // #A: TopK with LARGE k (still bounded).
        (
            "TopK k=10000 (Int col)",
            "SELECT id FROM t ORDER BY x LIMIT 10000",
        ),
        // #C: ORDER BY on aggregate result.
        (
            "GROUP BY + ORDER BY agg LIMIT 10",
            "SELECT g, COUNT(*) c FROM t GROUP BY g ORDER BY c DESC LIMIT 10",
        ),
        // #D: ParallelTopK candidate (depends on threshold).
        (
            "PARALLEL(4) ORDER BY + LIMIT 100",
            "SELECT /*+ PARALLEL(4) */ id FROM t WHERE x > 100 ORDER BY x LIMIT 100",
        ),
        // Mixed two-column ORDER BY (fast path declined → cycle-leader).
        (
            "ORDER BY x, id (two Int cols)",
            "SELECT id FROM t ORDER BY x, id",
        ),
    ];

    println!(
        "{:<48} {:>10} {:>10} {:>10}",
        "query", "median", "p10", "p99"
    );
    println!("{}", "-".repeat(80));
    for (label, sql) in queries {
        for _ in 0..warmup {
            run_once(&engine, sql);
        }
        let mut samples: Vec<Duration> = Vec::with_capacity(iters);
        for _ in 0..iters {
            samples.push(run_once(&engine, sql));
        }
        samples.sort();
        let median = samples[samples.len() / 2];
        let p10 = samples[samples.len() / 10];
        let p99 = samples[(samples.len() * 99) / 100];
        println!(
            "{:<48} {:>10} {:>10} {:>10}",
            label,
            fmt_dur(median),
            fmt_dur(p10),
            fmt_dur(p99),
        );
    }

    // A/B comparison: PARALLEL(4) hinted query, with MYDB_PARALLEL_SORT_DISABLE
    // toggled to compare serial-TopK vs. ParallelTopK on the same input.
    // This isolates the speedup contribution of #D (LIMIT pushdown into a
    // parallel sort path) from the other improvements.
    println!();
    println!(
        "--- A/B: ParallelTopK on vs. off (same query, MYDB_PARALLEL_SORT_DISABLE toggled) ---"
    );
    let ab_sql = "SELECT /*+ PARALLEL(4) */ id FROM t WHERE x > 100 ORDER BY x LIMIT 100";
    let ab_sql_no_limit = "SELECT /*+ PARALLEL(4) */ id FROM t WHERE x > 100 ORDER BY x";
    for (toggle_label, disable) in [("ParallelTopK ON", false), ("ParallelTopK OFF", true)] {
        set_disable_parallel_sort(disable);
        for sql_label_pair in [
            ("TopK k=100", ab_sql),
            ("Full sort, no LIMIT", ab_sql_no_limit),
        ] {
            let (sql_label, sql) = sql_label_pair;
            for _ in 0..warmup {
                run_once(&engine, sql);
            }
            let mut samples: Vec<Duration> = Vec::with_capacity(iters);
            for _ in 0..iters {
                samples.push(run_once(&engine, sql));
            }
            samples.sort();
            let median = samples[samples.len() / 2];
            println!(
                "  {:<28} {:<24} median {}",
                toggle_label,
                sql_label,
                fmt_dur(median),
            );
        }
    }
    set_disable_parallel_sort(false);
}

fn run_once(engine: &StorageQueryEngine, sql: &str) -> Duration {
    let t = Instant::now();
    let resp = engine.execute(sql).expect("execute");
    let _ = match resp {
        QueryResponse::ResultSet(rs) => rs.rows.len(),
        _ => 0,
    };
    t.elapsed()
}

/// Optionally toggle `MYDB_PARALLEL_SORT_DISABLE` so the same query
/// shape runs serial vs. parallel for an inline A/B comparison.
#[allow(dead_code)]
fn set_disable_parallel_sort(disable: bool) {
    if disable {
        std::env::set_var("MYDB_PARALLEL_SORT_DISABLE", "1");
    } else {
        std::env::remove_var("MYDB_PARALLEL_SORT_DISABLE");
    }
}

fn populate(engine: &StorageQueryEngine, n: usize, groups: usize) {
    engine
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT, g BIGINT)")
        .expect("create");
    let batch = 1000;
    for batch_start in (0..n).step_by(batch) {
        let batch_end = (batch_start + batch).min(n);
        let mut sql = String::from("INSERT INTO t VALUES ");
        // Simple LCG so x is pseudo-random but deterministic and
        // doesn't degenerate to already-sorted (which is the easy case
        // for any sort).
        for id in batch_start..batch_end {
            if id > batch_start {
                sql.push(',');
            }
            let scrambled = lcg(id as u64) as i64;
            let g = (id % groups) as i64;
            sql.push_str(&format!("({}, {}, {})", id, scrambled, g));
        }
        engine.execute(&sql).expect("insert");
    }
}

fn lcg(seed: u64) -> u64 {
    let mut x = seed.wrapping_add(0xA773_0BEA_C0DE_F00D);
    x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
    x ^ (x >> 17)
}

fn arg_value<T: std::str::FromStr>(flag: &str) -> Option<T> {
    let mut args = env::args();
    while let Some(a) = args.next() {
        if a == flag {
            return args.next().and_then(|v| v.parse().ok());
        }
    }
    None
}

fn fmt_dur(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1000 {
        format!("{us}µs")
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1_000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}
