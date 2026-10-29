// Deterministic in-process benchmark for executor operator pipelines.
//
// Results are emitted as JSON so a baseline and candidate build can be
// compared on the same machine without including MySQL protocol overhead.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use mydb::engine::api::{QueryResponse, SessionContext};
use mydb::engine::StorageQueryEngine;
use mydb::types::Value;
use serde::Serialize;

#[derive(Serialize)]
struct BenchReport {
    build_optimization: &'static str,
    target: String,
    vectorized_mode: String,
    rows: usize,
    warmup: usize,
    iterations: usize,
    analyze_overhead: bool,
    cases: Vec<CaseReport>,
    final_status: BTreeMap<String, u64>,
}

#[derive(Serialize)]
struct CaseReport {
    name: &'static str,
    sql: String,
    plan: String,
    output_rows: usize,
    result_digest: u64,
    sample_repeats: usize,
    samples_ns: Vec<u128>,
    median_ns: u128,
    analyze_samples_ns: Option<Vec<u128>>,
    analyze_median_ns: Option<u128>,
    analyze_overhead_pct: Option<f64>,
    rows_per_second: f64,
    peak_process_bytes: u64,
    spill_write_bytes: u64,
    spill_read_bytes: u64,
    order_by_key_rows_prepared: u64,
    status_delta: BTreeMap<String, u64>,
}

const CASES: &[(&str, &str)] = &[
    ("project_md5_serial","SELECT /*+ NO_PARALLEL FULL_SCAN(t) */ MD5(pad) FROM t"),
    ("project_md5_parallel2","SELECT /*+ PARALLEL(2) FULL_SCAN(t) */ MD5(pad) FROM t"),
    ("project_sha2_serial","SELECT /*+ NO_PARALLEL FULL_SCAN(t) */ SHA2(pad,256) FROM t"),
    ("project_sha2_parallel2","SELECT /*+ PARALLEL(2) FULL_SCAN(t) */ SHA2(pad,256) FROM t"),
    ("project_unicode_serial","SELECT /*+ NO_PARALLEL FULL_SCAN(t) */ CHAR_LENGTH(UPPER(pad)) FROM t"),
    ("project_unicode_parallel2","SELECT /*+ PARALLEL(2) FULL_SCAN(t) */ CHAR_LENGTH(UPPER(pad)) FROM t"),

    ("window_direct", "SELECT id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) FROM t"),
    ("window_expression", "SELECT id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v+id) FROM t"),
    ("window_range", "SELECT id, SUM(v) OVER (PARTITION BY k ORDER BY v RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) FROM t"),
    ("typed_conditionals", "SELECT SUM(IF(k%2=0,d1,d2)), SUM(COALESCE(d1,d2)), SUM(UNIX_TIMESTAMP('2025-01-02 03:04:05.123456')) FROM t"),
    ("metadata_columns", "SELECT TABLE_NAME,COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE FROM information_schema.columns WHERE TABLE_SCHEMA='test' AND TABLE_NAME='t'"),
    ("inlj_pair_covering", "SELECT /*+ ORDERED INDEX_NLJ(t,u) */ t.id,u.payload FROM t JOIN pair_rhs AS u ON t.id=u.id AND ABS(u.payload) < t.v % 5000"),
    ("inlj_pair_heap", "SELECT /*+ ORDERED INDEX_NLJ(t,u) */ t.id,u.payload FROM t JOIN u FORCE INDEX(PRIMARY) ON t.id=u.id AND u.payload < t.v % 5000"),
    ("inlj_pair_left", "SELECT /*+ ORDERED INDEX_NLJ(t,u) */ t.id,u.payload FROM t LEFT JOIN u FORCE INDEX(PRIMARY) ON t.id=u.id AND u.payload < t.v % 5000"),
    ("inlj_pair_semi", "SELECT /*+ INDEX_NLJ(u) */ t.id FROM t WHERE EXISTS (SELECT 1 FROM u FORCE INDEX(PRIMARY) WHERE t.id=u.id AND u.payload < t.v % 5000)"),
    ("inlj_pair_anti", "SELECT /*+ INDEX_NLJ(u) */ t.id FROM t WHERE NOT EXISTS (SELECT 1 FROM u FORCE INDEX(PRIMARY) WHERE t.id=u.id AND u.payload < t.v % 5000)"),
    ("index_forward", "SELECT pad,v FROM t FORCE INDEX(pad_v) WHERE pad >= 'row-' ORDER BY pad,v"),
    ("index_reverse", "SELECT pad,v FROM t FORCE INDEX(pad_v) WHERE pad >= 'row-' ORDER BY pad DESC,v DESC"),
    ("index_repair_forward", "SELECT pad,v FROM t FORCE INDEX(pad_v) WHERE pad >= 'row-' ORDER BY pad,v"),
    ("index_repair_reverse", "SELECT pad,v FROM t FORCE INDEX(pad_v) WHERE pad >= 'row-' ORDER BY pad DESC,v DESC"),
    ("index_repair_limit", "SELECT pad,v FROM t FORCE INDEX(pad_v) WHERE pad >= 'row-' ORDER BY pad,v LIMIT 257"),
    ("inlj_rows", "SELECT /*+ ORDERED INDEX_NLJ(t,u) */ t.id,u.payload,u.k1,u.k2,u.k3,u.k4 FROM t JOIN u ON t.id=u.id"),
    ("inlj_predicate", "SELECT /*+ ORDERED INDEX_NLJ(t,u) */ t.id,u.payload,u.k1,u.k2,u.k3,u.k4 FROM t JOIN u ON t.id=u.id AND u.payload < 5000"),
    ("scan_filter_project", "SELECT id, v FROM t WHERE v BETWEEN 100 AND 900000"),
    ("limit_selection", "SELECT id, v FROM t WHERE v > 10 LIMIT 257"),
    ("hashagg_low_cardinality", "SELECT k, COUNT(*), SUM(v) FROM t GROUP BY k"),
    ("hashagg_high_cardinality", "SELECT id, COUNT(*) FROM t GROUP BY id"),
    ("hashagg_cardinality_1", "SELECT id % 1, COUNT(*), SUM(v) FROM t GROUP BY id % 1"),
    ("hashagg_cardinality_4", "SELECT id % 4, COUNT(*), SUM(v) FROM t GROUP BY id % 4"),
    ("hashagg_cardinality_4096", "SELECT id % 4096, COUNT(*), SUM(v) FROM t GROUP BY id % 4096"),
    ("hashagg_cardinality_50pct", "SELECT id % 50000, COUNT(*), SUM(v) FROM t GROUP BY id % 50000"),
    (
        "hashagg_high_mixed",
        "SELECT id, COUNT(*), COUNT(v), SUM(v), AVG(v), MIN(v), MAX(v) FROM t GROUP BY id",
    ),
    (
        "hashagg_decimal_mixed",
        "SELECT k, COUNT(*), SUM(d1), SUM(d2), AVG(d1), MIN(d2), MAX(d2) FROM t GROUP BY k",
    ),
    (
        "hashagg_composite_2",
        "SELECT k1, k2, COUNT(*), SUM(v) FROM t GROUP BY k1, k2",
    ),
    (
        "hashagg_composite_6",
        "SELECT k1, k2, k3, k4, k5, k6, COUNT(*), SUM(v) FROM t GROUP BY k1, k2, k3, k4, k5, k6",
    ),
    (
        "hashagg_wide_string",
        "SELECT pad, COUNT(*), SUM(v) FROM t GROUP BY pad",
    ),
    (
        "hashagg_distinct",
        "SELECT k, COUNT(DISTINCT v), SUM(DISTINCT v) FROM t GROUP BY k",
    ),
    (
        "hashagg_conditional_family",
        "SELECT k, SUM(CASE WHEN id % 4 = 0 THEN d1 END), SUM(CASE WHEN id % 4 = 1 THEN d1 END), SUM(CASE WHEN id % 4 = 2 THEN d1 END), SUM(CASE WHEN id % 4 = 3 THEN d1 END) FROM t GROUP BY k",
    ),
    ("rowagg_ordered", "SELECT k, GROUP_CONCAT(v ORDER BY id DESC) FROM t GROUP BY k"),
    ("rowagg_composite_ordered", "SELECT k, k1, GROUP_CONCAT(v ORDER BY id DESC) FROM t GROUP BY k, k1"),
    ("rowagg_global_ordered", "SELECT GROUP_CONCAT(v ORDER BY id DESC) FROM t"),
    ("rowagg_join_ordered", "SELECT t.k, GROUP_CONCAT(u.payload ORDER BY t.id DESC) FROM t INNER JOIN u ON t.id = u.id GROUP BY t.k"),
    ("distinct", "SELECT DISTINCT v FROM t"),
    ("hashagg_join_count", "SELECT /*+ HASH_JOIN(t,u) */ t.id, COUNT(u.payload) FROM t LEFT JOIN u ON t.id = u.id GROUP BY t.id"),
    ("hashagg_join_mixed", "SELECT /*+ HASH_JOIN(t,u) */ t.k, COUNT(*), SUM(u.payload), AVG(u.payload), MIN(u.payload), MAX(u.payload) FROM t LEFT JOIN u ON t.id = u.id GROUP BY t.k"),
    ("hashagg_join_global", "SELECT /*+ HASH_JOIN(t,u) */ COUNT(*), SUM(u.payload), AVG(u.payload) FROM t LEFT JOIN u ON t.id = u.id"),
    ("hash_join", "SELECT t.id, u.payload FROM t INNER JOIN u ON t.id = u.id"),
    (
        "hash_join_int_1_row",
        "SELECT /*+ HASH_JOIN(t,u) */ t.id, u.payload FROM t FULL JOIN u ON t.id = u.id",
    ),
    (
        "hash_join_int_2",
        "SELECT /*+ HASH_JOIN(t,u) */ t.id, u.payload FROM t FULL JOIN u ON t.k1=u.k1 AND t.k2=u.k2",
    ),
    (
        "hash_join_int_3",
        "SELECT /*+ HASH_JOIN(t,u) */ t.id, u.payload FROM t FULL JOIN u ON t.k1=u.k1 AND t.k2=u.k2 AND t.k3=u.k3",
    ),
    (
        "hash_join_int_4",
        "SELECT /*+ HASH_JOIN(t,u) */ t.id, u.payload FROM t FULL JOIN u ON t.k1=u.k1 AND t.k2=u.k2 AND t.k3=u.k3 AND t.k4=u.k4",
    ),
    (
        "hash_join_int_5",
        "SELECT /*+ HASH_JOIN(t,u) */ t.id, u.payload FROM t FULL JOIN u ON t.k1=u.k1 AND t.k2=u.k2 AND t.k3=u.k3 AND t.k4=u.k4 AND t.k5=u.k5",
    ),
    (
        "hash_join_int_8",
        "SELECT /*+ HASH_JOIN(t,u) */ t.id, u.payload FROM t FULL JOIN u ON t.k1=u.k1 AND t.k2=u.k2 AND t.k3=u.k3 AND t.k4=u.k4 AND t.k5=u.k5 AND t.k6=u.k6 AND t.k7=u.k7 AND t.k8=u.k8",
    ),
    ("sort_expression", "SELECT id FROM t ORDER BY (v * 17 + id) DESC"),
    ("topn", "SELECT id, v FROM t ORDER BY v DESC LIMIT 100"),
    (
        "mixed_pipeline",
        "SELECT t.k, COUNT(*), SUM(u.payload) FROM t INNER JOIN u ON t.id = u.id WHERE t.v > 100 GROUP BY t.k ORDER BY SUM(u.payload) DESC LIMIT 50",
    ),
];

fn main() {
    let rows = arg_value("--rows").unwrap_or(100_000usize);
    let warmup = arg_value("--warmup").unwrap_or(2usize);
    let iterations = arg_value("--iterations").unwrap_or(7usize).max(1);
    let sample_min_ms = arg_value("--sample-min-ms").unwrap_or(50u128);
    let analyze_overhead = env::args().any(|arg| arg == "--analyze-overhead");
    let json_path = arg_value::<PathBuf>("--json");
    let selected_cases = arg_value::<String>("--case").map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .collect::<std::collections::HashSet<_>>()
    });
    let vectorized_mode = arg_value::<String>("--vectorized")
        .or_else(|| env::var("MYDB_VECTORIZED").ok())
        .unwrap_or_else(|| "auto".to_string());
    assert!(
        matches!(vectorized_mode.as_str(), "off" | "on" | "auto"),
        "--vectorized must be off, on, or auto"
    );
    env::set_var("MYDB_VECTORIZED", &vectorized_mode);
    if env::args().any(|arg| arg == "--expressions-only") {
        let json = serde_json::to_string_pretty(&mydb::engine::expression_bench::run()).unwrap();
        if let Some(path) = arg_value::<PathBuf>("--json") {
            fs::write(path, json).unwrap();
        } else {
            println!("{json}");
        }
        return;
    }

    let data_dir = env::temp_dir().join(format!(
        "mydb-executor-microbench-{}-{}",
        std::process::id(),
        rows
    ));
    let _ = fs::remove_dir_all(&data_dir);
    fs::create_dir_all(&data_dir).expect("create benchmark data directory");
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(data_dir.clone());

    let engine = StorageQueryEngine::open(&data_dir).expect("open benchmark engine");
    seed(&engine, rows);
    if selected_cases
        .as_ref()
        .is_none_or(|cases| cases.iter().any(|name| name.starts_with("index_")))
    {
        engine
            .execute("CREATE INDEX pad_v ON t(pad,v)")
            .expect("create ordered scan index");
    }

    if selected_cases
        .as_ref()
        .is_none_or(|cases| cases.contains("inlj_pair_covering"))
    {
        engine
            .execute("CREATE TABLE pair_rhs(id BIGINT,payload BIGINT,PRIMARY KEY(id,payload))")
            .expect("create covering join fixture");
        engine
            .execute("INSERT INTO pair_rhs SELECT id,payload FROM u")
            .expect("seed covering join fixture");
        engine
            .execute("ANALYZE TABLE pair_rhs")
            .expect("analyze covering join fixture");
    }
    let mut cases = Vec::with_capacity(CASES.len());
    for &(name, static_sql) in CASES {
        if selected_cases
            .as_ref()
            .is_some_and(|selected| !selected.contains(name))
        {
            continue;
        }
        let generated_sql;
        let sql = if name == "hashagg_cardinality_50pct" {
            generated_sql = format!(
                "SELECT id % {groups}, COUNT(*), SUM(v) FROM t GROUP BY id % {groups}",
                groups = (rows / 2).max(1),
            );
            generated_sql.as_str()
        } else {
            static_sql
        };
        // Keep a writer uncommitted so ordinary readers must merge old-key
        // MVCC repairs. The pre-write digest is an independent snapshot oracle.
        let repair_case = name.starts_with("index_repair_");
        let expected = repair_case.then(|| run_once_digest(&engine, sql).expect("snapshot oracle"));
        let mut writer = SessionContext::default();
        if repair_case {
            engine
                .execute_with_session(&mut writer, "BEGIN")
                .expect("begin repair writer");
            engine
                .execute_with_session(
                    &mut writer,
                    "UPDATE t SET pad=CONCAT('z',pad) WHERE id % 11 = 0",
                )
                .expect("create pending index repairs");
        }
        let QueryResponse::ResultSet(explain) = engine
            .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
            .expect("explain benchmark")
        else {
            panic!("expected EXPLAIN result")
        };
        let plan = explain
            .rows
            .iter()
            .flatten()
            .map(Value::to_display_string)
            .collect::<Vec<_>>()
            .join("\n");
        for _ in 0..warmup {
            run_once(&engine, sql).expect("warmup query");
            if analyze_overhead {
                run_analyze_execution_ns(&engine, sql).expect("EXPLAIN ANALYZE warmup query");
            }
        }
        let calibration_started = Instant::now();
        let mut output_rows = run_once(&engine, sql).expect("calibration query");
        let calibration_ns = calibration_started.elapsed().as_nanos().max(1);
        let sample_repeats = ((sample_min_ms.saturating_mul(1_000_000) + calibration_ns - 1)
            / calibration_ns)
            .clamp(1, 100) as usize;
        let before = status_snapshot(&engine);
        let mut samples_ns = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let started = Instant::now();
            for _ in 0..sample_repeats {
                output_rows = run_once(&engine, sql).expect("benchmark query");
            }
            samples_ns.push(started.elapsed().as_nanos() / sample_repeats as u128);
        }
        let after = status_snapshot(&engine);
        let delta_status = status_delta(&before, &after);
        let (digest_rows, result_digest) = run_once_digest(&engine, sql).expect("digest query");
        assert_eq!(
            digest_rows, output_rows,
            "unstable output row count for {name}"
        );
        if let Some(expected) = expected {
            assert_eq!(
                (digest_rows, result_digest),
                expected,
                "snapshot changed for {name}"
            );
        }
        let mut sorted = samples_ns.clone();
        sorted.sort_unstable();
        let mut median_ns = sorted[sorted.len() / 2];
        let (analyze_samples_ns, analyze_median_ns, analyze_overhead_pct) = if analyze_overhead {
            let mut paired_normal = Vec::with_capacity(iterations);
            let mut samples = Vec::with_capacity(iterations);
            for iteration in 0..iterations {
                let measure_normal = || {
                    let started = Instant::now();
                    for _ in 0..sample_repeats {
                        run_once(&engine, sql).expect("paired benchmark query");
                    }
                    started.elapsed().as_nanos() / sample_repeats as u128
                };
                let measure_analyze = || {
                    let mut execution_ns = 0u128;
                    for _ in 0..sample_repeats {
                        execution_ns = execution_ns.saturating_add(
                            run_analyze_execution_ns(&engine, sql)
                                .expect("EXPLAIN ANALYZE benchmark query"),
                        );
                    }
                    execution_ns / sample_repeats as u128
                };
                let (normal, analyzed) = if iteration % 2 == 0 {
                    (measure_normal(), measure_analyze())
                } else {
                    let analyzed = measure_analyze();
                    (measure_normal(), analyzed)
                };
                paired_normal.push(normal);
                samples.push(analyzed);
            }
            let mut normal_sorted = paired_normal.clone();
            normal_sorted.sort_unstable();
            median_ns = normal_sorted[normal_sorted.len() / 2];
            samples_ns = paired_normal;
            let mut sorted = samples.clone();
            sorted.sort_unstable();
            let analyze_median = sorted[sorted.len() / 2];
            let overhead = if median_ns == 0 {
                0.0
            } else {
                (analyze_median as f64 / median_ns as f64 - 1.0) * 100.0
            };
            (Some(samples), Some(analyze_median), Some(overhead))
        } else {
            (None, None, None)
        };
        let rows_per_second = if median_ns == 0 {
            0.0
        } else {
            rows as f64 * 1_000_000_000.0 / median_ns as f64
        };
        cases.push(CaseReport {
            name,
            sql: sql.to_string(),
            plan,
            output_rows,
            result_digest,
            sample_repeats,
            samples_ns,
            median_ns,
            analyze_samples_ns,
            analyze_median_ns,
            analyze_overhead_pct,
            rows_per_second,
            peak_process_bytes: peak_process_bytes(),
            // The tracked I/O counters are totals; summing them with
            // operator-specific counters would count the same bytes twice.
            spill_write_bytes: delta_status
                .get("spill_bytes_written")
                .copied()
                .unwrap_or(0),
            spill_read_bytes: delta_status.get("spill_bytes_read").copied().unwrap_or(0),
            order_by_key_rows_prepared: delta_status
                .get("order_by_key_rows_prepared")
                .copied()
                .unwrap_or(0),
            status_delta: delta_status,
        });
        if repair_case {
            engine
                .execute_with_session(&mut writer, "ROLLBACK")
                .expect("rollback repair writer");
        }
    }
    if let Some(selected) = selected_cases {
        let missing: Vec<_> = selected
            .iter()
            .filter(|name| !CASES.iter().any(|(known, _)| known == &name.as_str()))
            .collect();
        assert!(missing.is_empty(), "unknown --case value(s): {missing:?}");
    }

    let report = BenchReport {
        build_optimization: if cfg!(debug_assertions) {
            "debug-assertions"
        } else {
            "release"
        },
        target: format!("{}-{}", env::consts::ARCH, env::consts::OS),
        vectorized_mode,
        rows,
        warmup,
        iterations,
        analyze_overhead,
        cases,
        final_status: status_snapshot(&engine),
    };
    let json = serde_json::to_string_pretty(&report).expect("serialize benchmark report");
    if let Some(path) = json_path {
        fs::write(&path, &json).expect("write benchmark JSON");
        eprintln!("wrote {}", path.display());
    } else {
        println!("{json}");
    }
}

fn run_once_digest(
    engine: &StorageQueryEngine,
    sql: &str,
) -> Result<(usize, u64), mydb::types::QueryError> {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    let mut rows = Vec::new();
    match engine.execute(sql)? {
        QueryResponse::ResultSet(result) => {
            for row in &result.rows {
                rows.push(format!("{row:?}"));
            }
        }
        QueryResponse::StreamingResultSet(mut result) => {
            while let Some(row) = result.cursor.next_row()? {
                rows.push(format!("{row:?}"));
            }
        }
        _ => {}
    }
    // An aggregate/window ORDER BY does not order the result set. Only the
    // outer SELECT order controls whether the digest must retain row order.
    let mydb::parser::Statement::Select(select) =
        mydb::parser::parse_statement(sql).expect("benchmark SELECT parses")
    else {
        panic!("benchmark must be SELECT")
    };
    if select.order_by.is_empty() {
        rows.sort_unstable();
    }
    for row in &rows {
        row.hash(&mut hasher);
    }
    Ok((rows.len(), hasher.finish()))
}

fn run_once(engine: &StorageQueryEngine, sql: &str) -> Result<usize, mydb::types::QueryError> {
    match engine.execute(sql)? {
        QueryResponse::ResultSet(result) => Ok(result.rows.len()),
        QueryResponse::StreamingResultSet(mut result) => {
            let mut rows = 0usize;
            while result.cursor.next_row()?.is_some() {
                rows += 1;
            }
            Ok(rows)
        }
        _ => Ok(0),
    }
}

fn run_analyze_execution_ns(
    engine: &StorageQueryEngine,
    sql: &str,
) -> Result<u128, mydb::types::QueryError> {
    let response = engine.execute(&format!("EXPLAIN ANALYZE FORMAT=JSON {sql}"))?;
    let QueryResponse::ResultSet(result) = response else {
        panic!("EXPLAIN ANALYZE benchmark expected a materialized JSON result")
    };
    let Value::String(text) = &result.rows[0][0] else {
        panic!("EXPLAIN ANALYZE benchmark expected JSON text")
    };
    let json: serde_json::Value = serde_json::from_str(text).expect("parse ANALYZE benchmark JSON");
    let wall_ms = json["query_block"]["execution"]["wall_time_ms"]
        .as_f64()
        .expect("ANALYZE JSON execution.wall_time_ms");
    Ok((wall_ms * 1_000_000.0).round() as u128)
}

fn seed(engine: &StorageQueryEngine, rows: usize) {
    engine
        .execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT, v BIGINT, pad VARCHAR(64), d1 DECIMAL(18,2), d2 DECIMAL(18,3), \
             k1 BIGINT, k2 BIGINT, k3 BIGINT, k4 BIGINT, k5 BIGINT, k6 BIGINT, k7 BIGINT, k8 BIGINT)",
        )
        .expect("create t");
    engine
        .execute(
            "CREATE TABLE u (id BIGINT PRIMARY KEY, payload BIGINT, \
             k1 BIGINT, k2 BIGINT, k3 BIGINT, k4 BIGINT, k5 BIGINT, k6 BIGINT, k7 BIGINT, k8 BIGINT)",
        )
        .expect("create u");
    for start in (0..rows).step_by(500) {
        let end = (start + 500).min(rows);
        let mut t_sql = String::from("INSERT INTO t VALUES ");
        let mut u_sql = String::from("INSERT INTO u VALUES ");
        for id in start..end {
            if id > start {
                t_sql.push(',');
                u_sql.push(',');
            }
            let value = lcg(id as u64) % 1_000_003;
            let group = id % 128;
            let decimal_2 = value % 10_000_000;
            let decimal_3 = lcg(value) % 100_000_000;
            let keys = (1..=8)
                .map(|column| {
                    (id as u64)
                        .wrapping_mul((column as u64).wrapping_mul(2).wrapping_add(1))
                        .wrapping_add((column * 97) as u64)
                        .to_string()
                })
                .collect::<Vec<_>>()
                .join(",");
            t_sql.push_str(&format!(
                "({id},{group},{value},'row-{id:016x}',{decimal_2},{decimal_3},{keys})"
            ));
            u_sql.push_str(&format!("({id},{},{keys})", value % 10_007));
        }
        engine.execute(&t_sql).expect("seed t");
        engine.execute(&u_sql).expect("seed u");
    }
    engine.execute("ANALYZE TABLE t").expect("analyze t");
    engine.execute("ANALYZE TABLE u").expect("analyze u");
}

fn status_snapshot(engine: &StorageQueryEngine) -> BTreeMap<String, u64> {
    let Ok(QueryResponse::ResultSet(result)) = engine.execute("SHOW ENGINE MYDB STATUS") else {
        return BTreeMap::new();
    };
    result
        .rows
        .into_iter()
        .filter_map(|row| match row.as_slice() {
            [_, Value::String(name), Value::String(value), ..]
                if name.contains("memory")
                    || name.contains("spill")
                    || name.contains("worker")
                    || name == "order_by_key_rows_prepared"
                    || name.starts_with("hash_join_typed_key")
                    || name.starts_with("inlj_") =>
            {
                value.parse::<u64>().ok().map(|value| (name.clone(), value))
            }
            _ => None,
        })
        .collect()
}

fn status_delta(
    before: &BTreeMap<String, u64>,
    after: &BTreeMap<String, u64>,
) -> BTreeMap<String, u64> {
    after
        .iter()
        .map(|(name, value)| {
            (
                name.clone(),
                value.saturating_sub(before.get(name).copied().unwrap_or(0)),
            )
        })
        .collect()
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn peak_process_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the provided rusage on success.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return 0;
    }
    // SAFETY: successful getrusage initialized the value.
    let max_rss = unsafe { usage.assume_init() }.ru_maxrss.max(0) as u64;
    if cfg!(target_os = "linux") {
        max_rss.saturating_mul(1024)
    } else {
        max_rss
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn peak_process_bytes() -> u64 {
    0
}

fn lcg(seed: u64) -> u64 {
    seed.wrapping_add(0xA773_0BEA_C0DE_F00D)
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1)
        ^ (seed >> 17)
}

fn arg_value<T: std::str::FromStr>(flag: &str) -> Option<T> {
    let mut args = env::args();
    while let Some(arg) = args.next() {
        if arg == flag {
            return args.next().and_then(|value| value.parse().ok());
        }
    }
    None
}
