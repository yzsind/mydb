//! End-to-end HNSW performance / recall benchmark through the SQL engine.
//!
//! Ignored by default; run in release mode:
//!
//! ```text
//! VB_BACKEND=memory VB_N=100000 VB_DIM=128 \
//!   cargo nextest run --release --run-ignored only -E 'test(hnsw_end_to_end_bench)' --no-capture
//! ```
//!
//! Knobs (all optional): `VB_BACKEND` (memory|paged), `VB_N`, `VB_DIM`,
//! `VB_METRIC` (L2|COSINE), `VB_QUERIES`, `VB_THREADS`, `VB_CLUSTERS`,
//! `VB_SECTIONS` (comma list of base,filter,delete,rebuild,update,pending,checkpoint).
//! The data is a clustered gaussian mixture with a decaying per-dimension
//! spectrum and a few large-offset outlier dimensions, which is closer to
//! real embedding sets than uniform noise. Ground truth is exact brute force.

use super::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Rng(u64);

impl Rng {
    fn next_f64(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }

    fn gauss(&mut self) -> f32 {
        let (u, v) = (self.next_f64(), self.next_f64());
        ((-2.0 * u.ln()).sqrt() * (2.0 * std::f64::consts::PI * v).cos()) as f32
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_f64() * n as f64) as usize % n
    }
}

struct Dataset {
    dim: usize,
    cosine: bool,
    rows: Vec<Vec<f32>>,
    cluster: Vec<usize>,
    queries: Vec<Vec<f32>>,
    centroids: Vec<Vec<f32>>,
    spectrum: Vec<f32>,
    noise: f32,
}

impl Dataset {
    fn generate(
        n: usize,
        dim: usize,
        clusters: usize,
        queries: usize,
        cosine: bool,
        noise: f32,
    ) -> Self {
        let mut rng = Rng(0x5EED_0000_0000_0001 ^ (n as u64) ^ ((dim as u64) << 32));
        let spectrum: Vec<f32> = (0..dim)
            .map(|j| 1.0 / (1.0 + j as f32 / 8.0).sqrt())
            .collect();
        let centroids: Vec<Vec<f32>> = (0..clusters)
            .map(|_| (0..dim).map(|j| rng.gauss() * spectrum[j]).collect())
            .collect();
        let mut ds = Dataset {
            dim,
            cosine,
            rows: Vec::with_capacity(n),
            cluster: Vec::with_capacity(n),
            queries: Vec::with_capacity(queries),
            centroids,
            spectrum,
            noise,
        };
        for _ in 0..n {
            let c = rng.below(clusters);
            let v = ds.sample(&mut rng, c);
            ds.rows.push(v);
            ds.cluster.push(c);
        }
        for _ in 0..queries {
            let c = rng.below(clusters);
            let v = ds.sample(&mut rng, c);
            ds.queries.push(v);
        }
        ds
    }

    fn sample(&self, rng: &mut Rng, cluster: usize) -> Vec<f32> {
        let mut v: Vec<f32> = (0..self.dim)
            .map(|j| self.centroids[cluster][j] + self.noise * rng.gauss() * self.spectrum[j])
            .collect();
        // Outlier dimensions with a large constant offset (common in
        // transformer embeddings) stretch a single global quantization range.
        for j in (0..self.dim).step_by(64) {
            v[j] += 6.0;
        }
        if self.cosine {
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            v.iter_mut().for_each(|x| *x /= norm);
        }
        v
    }

    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        if self.cosine {
            let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
            let na: f32 = a.iter().map(|x| x * x).sum();
            let nb: f32 = b.iter().map(|x| x * x).sum();
            1.0 - dot / (na.sqrt() * nb.sqrt())
        } else {
            a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
        }
    }

    /// Exact top-k ids (1-based row ids) among rows accepted by `keep`.
    fn ground_truth(&self, q: &[f32], k: usize, keep: impl Fn(usize) -> bool) -> Vec<i64> {
        let mut scored: Vec<(f32, i64)> = (0..self.rows.len())
            .filter(|&i| keep(i))
            .map(|i| (self.distance(q, &self.rows[i]), i as i64 + 1))
            .collect();
        let k = k.min(scored.len());
        if k == 0 {
            return Vec::new();
        }
        scored.select_nth_unstable_by(k - 1, |a, b| a.0.total_cmp(&b.0));
        scored.truncate(k);
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));
        scored.into_iter().map(|(_, id)| id).collect()
    }
}

fn vtext(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 10 + 2);
    s.push('[');
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("{x:.6}"));
    }
    s.push(']');
    s
}

fn ids(engine: &StorageQueryEngine, sql: &str) -> Vec<i64> {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs
            .rows
            .into_iter()
            .map(|r| r[0].to_display_string().parse().unwrap())
            .collect(),
        other => panic!("expected a result set for `{sql}`, got {other:?}"),
    }
}

fn recall(found: &[i64], truth: &[i64]) -> f64 {
    if truth.is_empty() {
        return 1.0;
    }
    let hits = found.iter().filter(|id| truth.contains(id)).count();
    hits as f64 / truth.len() as f64
}

fn percentile(sorted: &[Duration], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx].as_secs_f64() * 1e3
}

struct Measure {
    recall: f64,
    rows: f64,
    qps: f64,
    p50_ms: f64,
    p99_ms: f64,
}

/// Run every query once single-threaded, reporting recall against `truth`.
fn measure(engine: &StorageQueryEngine, sqls: &[String], truths: &[Vec<i64>], k: usize) -> Measure {
    // Warm-up pass (graph build / caches) is excluded from timing.
    for sql in sqls.iter().take(sqls.len().min(8).min(sqls.len() / 4)) {
        ids(engine, sql);
    }
    let mut lat = Vec::with_capacity(sqls.len());
    let (mut rsum, mut rows) = (0.0, 0usize);
    let started = Instant::now();
    for (sql, truth) in sqls.iter().zip(truths) {
        let t = Instant::now();
        let found = ids(engine, sql);
        lat.push(t.elapsed());
        rows += found.len();
        let truth: Vec<i64> = truth.iter().take(k).copied().collect();
        rsum += recall(&found, &truth);
    }
    let total = started.elapsed();
    lat.sort();
    Measure {
        recall: rsum / sqls.len() as f64,
        rows: rows as f64 / sqls.len() as f64,
        qps: sqls.len() as f64 / total.as_secs_f64(),
        p50_ms: percentile(&lat, 0.5),
        p99_ms: percentile(&lat, 0.99),
    }
}

fn concurrent_qps(
    engine: &Arc<StorageQueryEngine>,
    sqls: &Arc<Vec<String>>,
    threads: usize,
) -> f64 {
    let rounds = 2usize;
    let started = Instant::now();
    std::thread::scope(|scope| {
        for t in 0..threads {
            let engine = Arc::clone(engine);
            let sqls = Arc::clone(sqls);
            scope.spawn(move || {
                for r in 0..rounds {
                    for i in 0..sqls.len() {
                        let sql = &sqls[(i + t * 7 + r) % sqls.len()];
                        ids(&engine, sql);
                    }
                }
            });
        }
    });
    (threads * rounds * sqls.len()) as f64 / started.elapsed().as_secs_f64()
}

fn report(label: &str, m: &Measure) {
    eprintln!(
        "[vb] {label:<34} recall@k={:.4} rows={:>5.2} qps={:>8.1} p50={:>7.3}ms p99={:>7.3}ms",
        m.recall, m.rows, m.qps, m.p50_ms, m.p99_ms
    );
}

fn knn_sql(ds: &Dataset, q: &[f32], k: usize, ef: Option<usize>, filter: Option<&str>) -> String {
    let hint = ef.map_or(String::new(), |ef| format!("/*+ HNSW_EF({ef}) */ "));
    let filter = filter.map_or(String::new(), |f| format!("WHERE {f} "));
    let metric = if ds.cosine { ", 'COSINE'" } else { "" };
    format!(
        "SELECT {hint}id FROM items {filter}ORDER BY DISTANCE(emb, '{}'{metric}) LIMIT {k}",
        vtext(q)
    )
}

#[test]
#[ignore = "benchmark; run explicitly in release mode"]
fn hnsw_end_to_end_bench() {
    let backend = std::env::var("VB_BACKEND").unwrap_or_else(|_| "memory".into());
    let n: usize = env_or("VB_N", 50_000);
    let dim: usize = env_or("VB_DIM", 128);
    let cosine = std::env::var("VB_METRIC").is_ok_and(|m| m.eq_ignore_ascii_case("COSINE"));
    let nq: usize = env_or("VB_QUERIES", 200);
    let threads: usize = env_or("VB_THREADS", 8);
    let clusters: usize = env_or("VB_CLUSTERS", 200);
    let sections = std::env::var("VB_SECTIONS")
        .unwrap_or_else(|_| "base,filter,delete,rebuild,update,pending,checkpoint".into());
    let section = |name: &str| sections.split(',').any(|s| s.trim() == name);
    let k = 10;
    let _env = [
        ("MYDB_VECTOR_STORAGE", backend.as_str()),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ]
    .map(|(k, v)| {
        std::env::set_var(k, v);
        k
    });

    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let mut config = memory_test_storage_config();
    config.durability = crate::storage::DurabilityMode::Relaxed;
    config.buffer_pool_pages = env_or("VB_BUFFER_POOL_PAGES", 262_144);
    config.checkpoint_interval = Duration::from_secs(3600);
    config.checkpoint_wal_bytes = 64 * 1024 * 1024 * 1024;
    let engine = Arc::new(
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap(),
    );
    eprintln!(
        "[vb] backend={backend} n={n} dim={dim} metric={} queries={nq} threads={threads}",
        if cosine { "COSINE" } else { "L2" }
    );

    let t = Instant::now();
    let ds = Dataset::generate(n, dim, clusters, nq, cosine, env_or("VB_NOISE", 1.0));
    eprintln!(
        "[vb] generate                         {:.2}s",
        t.elapsed().as_secs_f64()
    );
    engine
        .execute(&format!(
            "CREATE TABLE items (id INT PRIMARY KEY, cl INT NOT NULL, cat INT NOT NULL, \
             emb VECTOR({dim}) NOT NULL)"
        ))
        .unwrap();
    let t = Instant::now();
    for chunk in (0..n).collect::<Vec<_>>().chunks(500) {
        let values: Vec<String> = chunk
            .iter()
            .map(|&i| {
                format!(
                    "({}, {}, {}, '{}')",
                    i + 1,
                    ds.cluster[i],
                    (i + 1) % 100,
                    vtext(&ds.rows[i])
                )
            })
            .collect();
        engine
            .execute(&format!("INSERT INTO items VALUES {}", values.join(",")))
            .unwrap();
    }
    eprintln!(
        "[vb] load                             {:.2}s",
        t.elapsed().as_secs_f64()
    );
    let metric = if cosine {
        " COMMENT 'DISTANCE=COSINE'"
    } else {
        ""
    };
    let t = Instant::now();
    engine
        .execute(&format!(
            "CREATE INDEX idx_emb ON items(emb) USING HNSW{metric}"
        ))
        .unwrap();
    eprintln!(
        "[vb] create index (build)             {:.2}s",
        t.elapsed().as_secs_f64()
    );

    let all = |_: usize| true;
    let truths: Vec<Vec<i64>> = ds
        .queries
        .iter()
        .map(|q| ds.ground_truth(q, k, all))
        .collect();
    let base_sqls: Vec<String> = ds
        .queries
        .iter()
        .map(|q| knn_sql(&ds, q, k, None, None))
        .collect();
    assert!(
        matches!(engine.execute(&format!("EXPLAIN {}", base_sqls[0])), Ok(_)),
        "EXPLAIN must succeed"
    );

    if section("base") {
        report("knn ef=default", &measure(&engine, &base_sqls, &truths, k));
        for ef in [32usize, 128, 256] {
            let sqls: Vec<String> = ds
                .queries
                .iter()
                .map(|q| knn_sql(&ds, q, k, Some(ef), None))
                .collect();
            report(
                &format!("knn ef={ef}"),
                &measure(&engine, &sqls, &truths, k),
            );
        }
        let shared = Arc::new(base_sqls.clone());
        eprintln!(
            "[vb] knn concurrent x{threads:<2}                 qps={:.1}",
            concurrent_qps(&engine, &shared, threads)
        );
    }

    if section("filter") {
        for (label, filter, keep) in [
            (
                "filter 10% (cat<10)",
                "cat < 10",
                Box::new(|i: usize| (i + 1) % 100 < 10) as Box<dyn Fn(usize) -> bool>,
            ),
            (
                "filter 1% (cat=7)",
                "cat = 7",
                Box::new(|i: usize| (i + 1) % 100 == 7),
            ),
            (
                "filter 0.1% (cat=7,cl<20)",
                "cat = 7 AND cl < 20",
                Box::new(|i: usize| (i + 1) % 100 == 7 && ds.cluster[i] < 20),
            ),
            (
                "filter 0 match (cat=7,id%10=3)",
                "cat = 7 AND id % 10 = 3",
                Box::new(|_: usize| false),
            ),
        ] {
            // Highly selective filters can be very slow on a regressed
            // build; bound their query count so the run completes.
            let limit = if label.starts_with("filter 0") {
                env_or("VB_SLOW_FILTER_QUERIES", 5usize)
            } else {
                nq
            };
            let truths: Vec<Vec<i64>> = ds
                .queries
                .iter()
                .take(limit)
                .map(|q| ds.ground_truth(q, k, &keep))
                .collect();
            let sqls: Vec<String> = ds
                .queries
                .iter()
                .take(limit)
                .map(|q| knn_sql(&ds, q, k, None, Some(filter)))
                .collect();
            report(label, &measure(&engine, &sqls, &truths, k));
        }
    }

    // Rows deleted below stay deleted for the later sections.
    let mut deleted = vec![false; n];
    if section("delete") {
        // Delete every row of 30% of the clusters: tombstones concentrate
        // exactly where many queries land.
        let doomed = clusters * 3 / 10;
        let t = Instant::now();
        engine
            .execute(&format!("DELETE FROM items WHERE cl < {doomed}"))
            .unwrap();
        eprintln!(
            "[vb] delete 30% clusters              {:.2}s",
            t.elapsed().as_secs_f64()
        );
        for i in 0..n {
            deleted[i] = ds.cluster[i] < doomed;
        }
        let truths: Vec<Vec<i64>> = ds
            .queries
            .iter()
            .map(|q| ds.ground_truth(q, k, |i| !deleted[i]))
            .collect();
        report(
            "knn after delete",
            &measure(&engine, &base_sqls, &truths, k),
        );
        let t = Instant::now();
        engine.checkpoint().unwrap();
        engine.database().run_vector_maintenance_for_test();
        eprintln!(
            "[vb] checkpoint (compact)             {:.2}s",
            t.elapsed().as_secs_f64()
        );
        report(
            "knn after compaction",
            &measure(&engine, &base_sqls, &truths, k),
        );
        if section("rebuild") {
            // Reference point: a fresh CREATE INDEX over the surviving rows.
            engine.execute("DROP INDEX idx_emb ON items").unwrap();
            let t = Instant::now();
            engine
                .execute(&format!(
                    "CREATE INDEX idx_emb ON items(emb) USING HNSW{metric}"
                ))
                .unwrap();
            eprintln!(
                "[vb] rebuild index after delete       {:.2}s",
                t.elapsed().as_secs_f64()
            );
            report(
                "knn after fresh rebuild",
                &measure(&engine, &base_sqls, &truths, k),
            );
        }
    }

    if section("update") {
        let mut rng = Rng(0xFEED);
        let live: Vec<usize> = (0..n).filter(|&i| !deleted[i]).collect();
        let mut rows = ds.rows.clone();
        let updates = 2000.min(live.len());
        {
            // Single-row INSERTs into the warm index (incremental linking).
            let inserts = 2000usize;
            let wal = engine.database().wal_bytes_for_test();
            let t = Instant::now();
            for j in 0..inserts {
                let v = {
                    let c = rng.below(clusters);
                    ds.sample(&mut rng, c)
                };
                engine
                    .execute(&format!(
                        "INSERT INTO items VALUES ({}, 0, 0, '{}')",
                        30_000_000 + j,
                        vtext(&v)
                    ))
                    .unwrap();
            }
            let secs = t.elapsed().as_secs_f64();
            let wal = engine.database().wal_bytes_for_test() - wal;
            eprintln!(
                "[vb] {:<34} {:.1} rows/s wal={:.1}KiB/row",
                "insert single row",
                inserts as f64 / secs,
                wal as f64 / 1024.0 / inserts as f64
            );
            engine
                .execute("DELETE FROM items WHERE id >= 30000000")
                .unwrap();
        }
        for (label, large) in [("update small drift", false), ("update large move", true)] {
            let wal = engine.database().wal_bytes_for_test();
            let t = Instant::now();
            for _ in 0..updates {
                let i = live[rng.below(live.len())];
                let v = if large {
                    {
                        let c = rng.below(clusters);
                        ds.sample(&mut rng, c)
                    }
                } else {
                    let mut v = rows[i].clone();
                    for x in v.iter_mut() {
                        *x += 0.01 * rng.gauss();
                    }
                    v
                };
                engine
                    .execute(&format!(
                        "UPDATE items SET emb = '{}' WHERE id = {}",
                        vtext(&v),
                        i + 1
                    ))
                    .unwrap();
                rows[i] = if cosine {
                    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                    v.iter().map(|x| x / norm).collect()
                } else {
                    v
                };
            }
            let secs = t.elapsed().as_secs_f64();
            let wal = engine.database().wal_bytes_for_test() - wal;
            eprintln!(
                "[vb] {label:<34} {:.1} rows/s wal={:.1}KiB/row",
                updates as f64 / secs,
                wal as f64 / 1024.0 / updates as f64
            );
        }
        let truth_ds = Dataset {
            dim,
            cosine,
            rows,
            cluster: ds.cluster.clone(),
            queries: Vec::new(),
            centroids: Vec::new(),
            spectrum: Vec::new(),
            noise: 0.0,
        };
        let truths: Vec<Vec<i64>> = ds
            .queries
            .iter()
            .map(|q| truth_ds.ground_truth(q, k, |i| !deleted[i]))
            .collect();
        report(
            "knn after updates",
            &measure(&engine, &base_sqls, &truths, k),
        );
    }

    if section("pending") {
        // A large uncommitted transaction in another session must not tax
        // every concurrent KNN query.
        let pending_rows = 20_000;
        let mut session = SessionContext::default();
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        let mut rng = Rng(0xABCD);
        for base in (0..pending_rows).step_by(500) {
            let values: Vec<String> = (base..base + 500)
                .map(|j| {
                    let v = {
                        let c = rng.below(clusters);
                        ds.sample(&mut rng, c)
                    };
                    format!("({}, 0, 0, '{}')", 10_000_000 + j, vtext(&v))
                })
                .collect();
            engine
                .execute_with_session(
                    &mut session,
                    &format!("INSERT INTO items VALUES {}", values.join(",")),
                )
                .unwrap();
        }
        let sqls: Vec<String> = base_sqls.iter().take(50).cloned().collect();
        let t = Instant::now();
        for sql in &sqls {
            ids(&engine, sql);
        }
        eprintln!(
            "[vb] knn with {pending_rows} pending rows       avg={:.3}ms",
            t.elapsed().as_secs_f64() * 1e3 / sqls.len() as f64
        );
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
    }

    if section("checkpoint") {
        // Dirty the graph past the persistence gate, then time a checkpoint.
        let mut rng = Rng(0x1234);
        let extra = n / 5;
        for base in (0..extra).step_by(500) {
            let values: Vec<String> = (base..(base + 500).min(extra))
                .map(|j| {
                    let v = {
                        let c = rng.below(clusters);
                        ds.sample(&mut rng, c)
                    };
                    format!("({}, 0, 0, '{}')", 20_000_000 + j, vtext(&v))
                })
                .collect();
            engine
                .execute(&format!("INSERT INTO items VALUES {}", values.join(",")))
                .unwrap();
        }
        let t = Instant::now();
        engine.checkpoint().unwrap();
        eprintln!(
            "[vb] checkpoint (persist)             {:.2}s",
            t.elapsed().as_secs_f64()
        );
    }
}
