//! `cbo_calibrate` — measure the host's relative cost of sequential IO,
//! random IO, hash build/probe, and in-memory sort, then print
//! recommended `cbo_*` GUC values that the running server can absorb
//! via `SET GLOBAL`.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --release --bin cbo_calibrate --
//!   --data-dir /tmp/cbo-calib \
//!   [--rows 200000]
//!   [--output-format text|sql]
//! ```
//!
//! The binary opens a private mydb database in `--data-dir`, runs
//! microbenchmarks, and prints the resulting coefficients. Pass
//! `--output-format sql` to get ready-to-paste `SET GLOBAL` statements.
//!
//! ## What it measures
//!
//! - `cbo_seq_page_cost` is anchored at 1.0 — every other coefficient
//!   is reported relative to it. The benchmark uses the time-per-page
//!   of a full table scan as the basis.
//! - `cbo_rnd_page_cost` is the ratio of (random PK lookup time) to
//!   (sequential scan time per page).
//! - `cbo_mem_page_cost` comes from fully warmed non-covering secondary
//!   range scans: a heap-correlated range is the baseline and a shuffled
//!   range reveals the cost of repeated logical buffer-page probes.
//! - `cbo_cpu_tuple_cost` is roughly the per-row overhead beyond IO,
//!   computed by subtracting the IO time from total scan time and
//!   dividing by row count.
//! - `cbo_hash_build_cost` / `cbo_hash_probe_cost` come from a
//!   self-join over the calibration table.
//! - `cbo_sort_cost_factor` is fitted so that
//!   `n × log2(n) × factor × cpu_operator_cost` ≈ measured sort time.
//!
//! Run on the production hardware with the production buffer-pool
//! size for best accuracy; the absolute numbers don't matter, only
//! the ratios.

use std::time::Instant;

use mydb::engine::api::{QueryResponse, SessionContext};
use mydb::engine::StorageQueryEngine;

const DEFAULT_ROWS: usize = 100_000;
const DEFAULT_TRIALS: usize = 7;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut data_dir: Option<String> = None;
    let mut rows = DEFAULT_ROWS;
    let mut output_format = "text".to_string();
    let mut trials = DEFAULT_TRIALS;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data-dir" => {
                i += 1;
                data_dir = Some(args[i].clone());
            }
            "--rows" => {
                i += 1;
                rows = args[i].parse().unwrap_or(DEFAULT_ROWS);
            }
            "--output-format" => {
                i += 1;
                output_format = args[i].clone();
            }
            "--trials" => {
                i += 1;
                trials = args[i].parse().unwrap_or(DEFAULT_TRIALS).max(3);
            }
            "-h" | "--help" => {
                print_usage();
                return;
            }
            unknown => {
                eprintln!("unknown argument: {unknown}");
                print_usage();
                std::process::exit(2);
            }
        }
        i += 1;
    }

    // Calibration writes a real database, so the user must pick a
    // path. Default to a process-local /tmp subfolder when nothing is
    // specified — but always print where we wrote so cleanup is easy.
    let dir_path = match data_dir {
        Some(p) => std::path::PathBuf::from(p),
        None => std::env::temp_dir().join(format!("mydb-cbo-calib-{}", std::process::id())),
    };
    std::fs::create_dir_all(&dir_path).expect("failed to create data dir");
    eprintln!("cbo_calibrate: data_dir = {}", dir_path.display());
    let engine = StorageQueryEngine::open(&dir_path).expect("failed to open storage engine");
    let coeffs = run_calibration(&engine, rows, trials);
    print_results(&coeffs, &output_format);
}

fn print_usage() {
    eprintln!(
        "Usage: cbo_calibrate [--data-dir PATH] [--rows N] [--trials N] \
         [--output-format text|sql]\n"
    );
}

#[derive(Debug, Clone, Copy)]
struct Coefficients {
    seq_page_cost: f64,
    rnd_page_cost: f64,
    cpu_tuple_cost: f64,
    cpu_operator_cost: f64,
    hash_build_cost: f64,
    hash_probe_cost: f64,
    sort_cost_factor: f64,
    mem_page_cost: f64,
}

impl Coefficients {
    fn defaults() -> Self {
        // Mirrors the Phase 1 defaults so the report is recognisable.
        Coefficients {
            seq_page_cost: 1.0,
            rnd_page_cost: 4.0,
            cpu_tuple_cost: 0.01,
            cpu_operator_cost: 0.0025,
            hash_build_cost: 0.02,
            hash_probe_cost: 0.02,
            sort_cost_factor: 2.0,
            mem_page_cost: 0.2,
        }
    }
}

fn run_calibration(engine: &StorageQueryEngine, rows: usize, trials: usize) -> Coefficients {
    let mut session = SessionContext::default();
    eprintln!(
        "cbo_calibrate: building calibration table with {} rows...",
        rows
    );
    exec(engine, &mut session, "DROP TABLE IF EXISTS cbo_calib");
    exec(
        engine,
        &mut session,
        "CREATE TABLE cbo_calib (id INT PRIMARY KEY, corr INT, scatter INT, v INT, \
         payload VARCHAR(120), INDEX idx_corr (corr), INDEX idx_scatter (scatter))",
    );
    // Insert in batches of 500 so individual statements stay small.
    let mut idx = 1usize;
    while idx <= rows {
        let end = (idx + 500).min(rows + 1);
        let mut sql = String::from("INSERT INTO cbo_calib VALUES ");
        let mut first = true;
        for i in idx..end {
            if !first {
                sql.push(',');
            }
            first = false;
            let scatter = (i.wrapping_mul(7919)) % rows.max(1);
            sql.push_str(&format!("({i}, {i}, {scatter}, {}, 'p{i}')", i % 1024));
        }
        exec(engine, &mut session, &sql);
        idx = end;
    }
    exec(engine, &mut session, "ANALYZE TABLE cbo_calib");

    let seq_sql = "SELECT /*+ FULL_SCAN(cbo_calib) */ payload FROM cbo_calib";
    require_plan(
        engine,
        &mut session,
        seq_sql,
        &["Table scan", "Parallel table scan"],
    );
    let seq = measure_trials(trials, || {
        exec_count(engine, &mut session, seq_sql);
    });

    // Same cardinality and wire/result construction without the Sort. The
    // difference isolates sorting instead of charging scan/materialisation
    // twice. No LIMIT is used: TopK is a different physical operator.
    let sort_sql = "SELECT /*+ FULL_SCAN(cbo_calib) */ id FROM cbo_calib ORDER BY v, id";
    require_plan(engine, &mut session, sort_sql, &["Sort"]);
    let sort_baseline_sql = "SELECT /*+ FULL_SCAN(cbo_calib) */ id FROM cbo_calib";
    let sort_baseline = measure_trials(trials, || {
        exec_count(engine, &mut session, sort_baseline_sql);
    });
    let sort = measure_trials(trials, || {
        exec_count(engine, &mut session, sort_sql);
    });

    // Random PK-lookup benchmark — touch ~1% of rows by id, randomly.
    let lookups = (rows / 100).max(100);
    require_plan(
        engine,
        &mut session,
        "SELECT v FROM cbo_calib WHERE id = 1",
        &["lookup"],
    );
    let rnd = measure_trials(trials, || {
        for k in 0..lookups {
            let id = (k * 9973) % rows + 1;
            let _ = exec(
                engine,
                &mut session,
                &format!("SELECT v FROM cbo_calib WHERE id = {id}"),
            );
        }
    });

    // Warm non-covering range scans. Both index paths return the same number
    // of rows and aggregate the same heap-only column. `idx_corr` follows
    // insertion/heap order while `idx_scatter` deliberately revisits pages in
    // pseudo-random order. Execute each once before the seven timed trials so
    // this measures buffer hits rather than storage latency.
    let range_rows = (rows / 10).max(1);
    let corr_range_sql = format!(
        "SELECT SUM(v) FROM cbo_calib FORCE INDEX (idx_corr) WHERE corr BETWEEN 1 AND {range_rows}"
    );
    let scatter_range_sql = format!(
        "SELECT SUM(v) FROM cbo_calib FORCE INDEX (idx_scatter) WHERE scatter BETWEEN 0 AND {}",
        range_rows.saturating_sub(1)
    );
    let seq_range_sql = format!(
        "SELECT /*+ FULL_SCAN(cbo_calib) */ SUM(v) FROM cbo_calib WHERE scatter BETWEEN 0 AND {}",
        range_rows.saturating_sub(1)
    );
    require_plan(engine, &mut session, &corr_range_sql, &["Index range scan"]);
    require_plan(
        engine,
        &mut session,
        &scatter_range_sql,
        &["Index range scan"],
    );
    require_plan(
        engine,
        &mut session,
        &seq_range_sql,
        &["Table scan", "Parallel table scan"],
    );
    exec_count(engine, &mut session, &corr_range_sql);
    exec_count(engine, &mut session, &scatter_range_sql);
    exec_count(engine, &mut session, &seq_range_sql);
    let corr_range = measure_trials(trials, || {
        exec_count(engine, &mut session, &corr_range_sql);
    });
    let scatter_range = measure_trials(trials, || {
        exec_count(engine, &mut session, &scatter_range_sql);
    });
    let seq_range = measure_trials(trials, || {
        exec_count(engine, &mut session, &seq_range_sql);
    });

    // Hash join benchmark — self-join via a separate aliased table.
    let hash_sql = "SELECT /*+ HASH_JOIN(a,b) FULL_SCAN(a) FULL_SCAN(b) */ COUNT(*) \
         FROM cbo_calib a JOIN cbo_calib b ON a.id = b.id";
    require_plan(engine, &mut session, hash_sql, &["Hash join"]);
    let hash = measure_trials(trials, || {
        exec_count(engine, &mut session, hash_sql);
    });

    let seq_ms = seq.median;
    let rnd_ms = rnd.median;
    let hash_ms = hash.median;
    let sort_ms = (sort.median - sort_baseline.median).max(0.001);

    eprintln!(
        "cbo_calibrate: seq={} rnd={} ({} lookups) correlated-range={} shuffled-range={} range-seq={} hash={} sort-exclusive={} ",
        seq, rnd, lookups, corr_range, scatter_range, seq_range, hash, sort_ms
    );

    derive_coefficients(
        rows,
        lookups,
        range_rows,
        seq_ms,
        rnd_ms,
        corr_range.median,
        scatter_range.median,
        seq_range.median,
        hash_ms,
        sort_ms,
    )
}

#[derive(Debug, Clone, Copy)]
struct Samples {
    cold: f64,
    median: f64,
    low: f64,
    high: f64,
}

impl std::fmt::Display for Samples {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:.1}ms (cold {:.1}, p10–p90 {:.1}–{:.1})",
            self.median, self.cold, self.low, self.high
        )
    }
}

fn measure_trials(mut trials: usize, mut operation: impl FnMut()) -> Samples {
    trials = trials.max(3);
    let mut values = Vec::with_capacity(trials);
    for _ in 0..trials {
        let start = Instant::now();
        operation();
        values.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    let cold = values[0];
    values.sort_by(f64::total_cmp);
    let percentile = |p: f64| {
        let index = ((values.len() - 1) as f64 * p).round() as usize;
        values[index]
    };
    Samples {
        cold,
        median: percentile(0.5),
        low: percentile(0.1),
        high: percentile(0.9),
    }
}

fn require_plan(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
    expected_any: &[&str],
) {
    let response = exec(engine, session, &format!("EXPLAIN FORMAT=TREE {sql}"));
    let rendered = match response {
        QueryResponse::ResultSet(rs) => rs
            .rows
            .first()
            .and_then(|row| row.first())
            .map(|value| value.to_display_string())
            .unwrap_or_default(),
        _ => String::new(),
    };
    assert!(
        plan_has_any_operator(&rendered, expected_any),
        "calibration query did not select any required operator {expected_any:?}: {rendered}"
    );
}

fn plan_has_any_operator(rendered: &str, expected_any: &[&str]) -> bool {
    let rendered = rendered.to_ascii_lowercase();
    expected_any
        .iter()
        .any(|expected| rendered.contains(&expected.to_ascii_lowercase()))
}

fn derive_coefficients(
    rows: usize,
    lookups: usize,
    range_rows: usize,
    seq_ms: f64,
    rnd_ms: f64,
    corr_range_ms: f64,
    scatter_range_ms: f64,
    seq_range_ms: f64,
    hash_ms: f64,
    sort_ms: f64,
) -> Coefficients {
    // Estimate page count from row count (rough: ~80 rows / 16KB page
    // for the calibration schema). The numbers don't have to be exact;
    // we only care about ratios.
    let seq_pages = ((rows as f64) / 80.0).max(1.0);
    let seq_per_page_ms = seq_ms / seq_pages;
    let seq_per_row_ms = seq_ms / rows as f64;

    let rnd_per_lookup_ms = rnd_ms / lookups.max(1) as f64;
    // Random IO ratio — anchored at seq_per_page_ms = 1.0.
    let rnd_ratio = (rnd_per_lookup_ms / seq_per_page_ms.max(1e-6)).max(1.0);

    // With the heap fully warmed, the shuffled range differs from the
    // correlated range primarily by repeated logical page probes. The
    // correlated leg performs roughly one probe per physical page, based on
    // the calibration table's observed/estimated density. Anchor the result
    // to a sequential page unit measured by the predicate-equivalent full
    // scan and clamp aggressively against timer noise or unusual hardware.
    let range_seq_per_page_ms = seq_range_ms / seq_pages;
    let correlated_page_probes = (range_rows as f64 / 80.0).ceil().max(1.0);
    let extra_probes = (range_rows as f64 - correlated_page_probes).max(1.0);
    let mem_page_cost = ((scatter_range_ms - corr_range_ms).max(0.0)
        / extra_probes
        / range_seq_per_page_ms.max(1e-6))
    .clamp(0.001, 1.0);

    // Per-tuple CPU: difference between observed scan time and what
    // pure-IO time would account for, divided by row count. Fall back
    // to default when the difference is negative (very cold cache).
    let scan_cpu_ms = (seq_per_row_ms - seq_per_page_ms / 80.0).max(0.0);
    let scan_cpu_units = scan_cpu_ms / seq_per_page_ms.max(1e-6);

    // Hash build/probe: hash_ms covers both sides, so divide by 2 and
    // by row count.
    let hash_per_row_ms = hash_ms / (2.0 * rows as f64);
    let hash_unit = (hash_per_row_ms / seq_per_page_ms.max(1e-6)).max(0.0);

    // Sort: solve `n log2(n) × factor × cpu_op = sort_ms` for `factor`.
    // We hold cpu_op at a default and back out the factor.
    let n = rows as f64;
    let log2n = n.max(2.0).log2();
    let cpu_op_default = 0.0025;
    let sort_factor =
        (sort_ms / seq_per_page_ms.max(1e-6) / (n * log2n * cpu_op_default)).clamp(0.5, 8.0);

    Coefficients {
        seq_page_cost: 1.0,
        rnd_page_cost: clamp(rnd_ratio, 1.0, 30.0),
        cpu_tuple_cost: clamp(scan_cpu_units, 0.001, 0.1),
        cpu_operator_cost: cpu_op_default,
        hash_build_cost: clamp(hash_unit, 0.005, 0.2),
        hash_probe_cost: clamp(hash_unit, 0.005, 0.2),
        sort_cost_factor: sort_factor,
        mem_page_cost,
    }
}

fn clamp(v: f64, lo: f64, hi: f64) -> f64 {
    v.max(lo).min(hi)
}

fn print_results(c: &Coefficients, format: &str) {
    let defaults = Coefficients::defaults();
    if format == "sql" {
        println!("-- cbo_calibrate recommendations");
        println!("SET GLOBAL cbo_seq_page_cost     = {:.6};", c.seq_page_cost);
        println!("SET GLOBAL cbo_rnd_page_cost     = {:.6};", c.rnd_page_cost);
        println!("SET GLOBAL cbo_mem_page_cost     = {:.6};", c.mem_page_cost);
        println!(
            "SET GLOBAL cbo_cpu_tuple_cost    = {:.6};",
            c.cpu_tuple_cost
        );
        println!(
            "SET GLOBAL cbo_cpu_operator_cost = {:.6};",
            c.cpu_operator_cost
        );
        println!(
            "SET GLOBAL cbo_hash_build_cost   = {:.6};",
            c.hash_build_cost
        );
        println!(
            "SET GLOBAL cbo_hash_probe_cost   = {:.6};",
            c.hash_probe_cost
        );
        println!(
            "SET GLOBAL cbo_sort_cost_factor  = {:.6};",
            c.sort_cost_factor
        );
        return;
    }
    println!("\nCalibrated CBO coefficients (relative to default):\n");
    print_row("seq_page_cost", c.seq_page_cost, defaults.seq_page_cost);
    print_row("rnd_page_cost", c.rnd_page_cost, defaults.rnd_page_cost);
    print_row("mem_page_cost", c.mem_page_cost, defaults.mem_page_cost);
    print_row("cpu_tuple_cost", c.cpu_tuple_cost, defaults.cpu_tuple_cost);
    print_row(
        "cpu_operator_cost",
        c.cpu_operator_cost,
        defaults.cpu_operator_cost,
    );
    print_row(
        "hash_build_cost",
        c.hash_build_cost,
        defaults.hash_build_cost,
    );
    print_row(
        "hash_probe_cost",
        c.hash_probe_cost,
        defaults.hash_probe_cost,
    );
    print_row(
        "sort_cost_factor",
        c.sort_cost_factor,
        defaults.sort_cost_factor,
    );
    println!(
        "\nTo apply, re-run with --output-format sql and pipe into mysql, or set\n\
         the corresponding `CBO_*` environment variables before launching mydb."
    );
}

fn print_row(name: &str, observed: f64, default: f64) {
    let ratio = observed / default;
    let arrow = if ratio > 1.05 {
        "↑"
    } else if ratio < 0.95 {
        "↓"
    } else {
        "≈"
    };
    println!(
        "  {:<22}  {:>10.4}  (default {:>8.4}, {} {:.2}×)",
        name, observed, default, arrow, ratio
    );
}

fn exec(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> QueryResponse {
    engine
        .execute_with_session(session, sql)
        .expect("calibration SQL failed")
}

fn exec_count(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> u64 {
    match exec(engine, session, sql) {
        QueryResponse::ResultSet(rs) => rs.rows.len() as u64,
        QueryResponse::StreamingResultSet(mut srs) => {
            let mut n = 0u64;
            while srs.cursor.next_row().expect("cursor failed").is_some() {
                n += 1;
            }
            n
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::plan_has_any_operator;

    #[test]
    fn seq_calibration_accepts_serial_and_parallel_scan_names() {
        let expected = &["Table scan", "Parallel table scan"];
        assert!(plan_has_any_operator("-> Table scan on t", expected));
        assert!(plan_has_any_operator(
            "-> Parallel table scan on t (workers=2)",
            expected
        ));
        assert!(plan_has_any_operator(
            "-> Parallel sort (workers=2)",
            &["Sort"]
        ));
        assert!(!plan_has_any_operator("-> Index range scan on t", expected));
    }
}
