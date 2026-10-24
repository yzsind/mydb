//! Cold SQL parser and cache-normalization microbenchmarks.
//!
//! Parse original SQL, as the engine does before binding and planning.
//! Report normalization separately: the sum is a synthetic processing cost,
//! not end-to-end query latency. Allocation counters include all parser workers.
//! Shapes cover short DML, wide queries, bulk literals, comments, extensions,
//! prepared query syntax and compound routines. Compare real source revisions
//! under the same build profile; no simulated historical clone counts.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use mydb::engine::normalize_sql;
use mydb::parser::{parse_statement, parse_statements, Statement};

const DEFAULT_BATCH_SIZES: &[usize] = &[100, 1000, 5000];
const DEFAULT_ITERATIONS: usize = 20;
const DEFAULT_WARMUP: usize = 3;

/// Counting allocator: wraps `System` and increments process-wide counters
/// when `COUNTING` is enabled, including scoped parallel parser workers.
static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        System.alloc_zeroed(layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn build_sysbench_insert(rows: usize) -> String {
    // Sysbench's sbtest schema: id INTEGER AUTO_INCREMENT, k INTEGER,
    //   c CHAR(120), pad CHAR(60).
    // The prepare path emits `INSERT INTO sbtest<n>(k, c, pad) VALUES …`
    // with one tuple per row. We populate `c` and `pad` with realistic
    // alphanumeric padding so the tokenizer doesn't get an unrepresentatively
    // boring input.
    let c_value = "abcdefgh".repeat(15); // 120 bytes
    let pad_value = "01234567".repeat(7) + "ABCD"; // 60 bytes
    let mut sql = String::with_capacity(rows * 220 + 64);
    sql.push_str("INSERT INTO sbtest1(k, c, pad) VALUES");
    for i in 0..rows {
        if i > 0 {
            sql.push(',');
        }
        sql.push('(');
        sql.push_str(&((i % 1_000_000) as i64 + 1).to_string());
        sql.push_str(",'");
        sql.push_str(&c_value);
        sql.push_str("','");
        sql.push_str(&pad_value);
        sql.push_str("')");
    }
    sql
}

fn build_comment_heavy_insert(rows: usize) -> String {
    build_sysbench_insert(rows).replace("),(", "),/* row boundary ; ? -- bait */(")
}

fn time_loop<F: FnMut()>(mut f: F, iters: usize) -> f64 {
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    let elapsed = start.elapsed();
    elapsed.as_secs_f64() / iters as f64
}

struct ParseStats {
    label: &'static str,
    rows: usize,
    sql_bytes: usize,
    normalize_secs: f64,
    parse_secs: f64,
    allocations: u64,
    allocation_bytes: u64,
}

impl ParseStats {
    fn print(&self) {
        let normalize_ms = self.normalize_secs * 1000.0;
        let parse_ms = self.parse_secs * 1000.0;
        let total_ms = normalize_ms + parse_ms;
        let rows_per_sec = if total_ms > 0.0 {
            self.rows as f64 / (total_ms / 1000.0)
        } else {
            f64::INFINITY
        };
        if std::env::var("MYDB_PARSE_BENCH_FORMAT").as_deref() == Ok("jsonl") {
            println!(
                "{{\"label\":\"{}\",\"rows\":{},\"sql_bytes\":{},\"normalize_ms\":{:.6},\"parse_ms\":{:.6},\"total_ms\":{:.6},\"allocations_per_parse\":{},\"bytes_per_parse\":{}}}",
                self.label,
                self.rows,
                self.sql_bytes,
                normalize_ms,
                parse_ms,
                total_ms,
                self.allocations,
                self.allocation_bytes,
            );
            return;
        }
        println!(
            "[{label}] rows={rows:>5} sql_bytes={bytes:>7} \
             normalize_ms={normalize_ms:>8.3} parse_ms={parse_ms:>8.3} \
             total_ms={total_ms:>8.3} rows_per_sec={rps:>12.0} \
             allocations={allocations:>6} allocation_bytes={allocation_bytes:>10}",
            label = self.label,
            rows = self.rows,
            bytes = self.sql_bytes,
            normalize_ms = normalize_ms,
            parse_ms = parse_ms,
            total_ms = total_ms,
            rps = rows_per_sec,
            allocations = self.allocations,
            allocation_bytes = self.allocation_bytes,
        );
    }
}

fn allocation_stats<F: FnMut()>(mut work: F) -> (u64, u64) {
    const SAMPLES: u64 = 20;
    ALLOC_COUNT.store(0, Ordering::Relaxed);
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::SeqCst);
    for _ in 0..SAMPLES {
        work();
    }
    COUNTING.store(false, Ordering::SeqCst);
    (
        ALLOC_COUNT.load(Ordering::Relaxed) / SAMPLES,
        ALLOC_BYTES.load(Ordering::Relaxed) / SAMPLES,
    )
}

fn measure_insert(rows: usize, warmup: usize, iters: usize) -> ParseStats {
    let sql = build_sysbench_insert(rows);
    // Warm-up — primes the heap allocator and any thread-local caches.
    for _ in 0..warmup {
        let _ = parse_statement(&sql).expect("parse_statement");
    }
    let normalize_secs = time_loop(
        || {
            let _ = normalize_sql(&sql);
        },
        iters,
    );
    let parse_secs = time_loop(
        || {
            let stmt = parse_statement(&sql).expect("parse_statement");
            std::hint::black_box(&stmt);
        },
        iters,
    );
    // Sanity-check we actually parsed an INSERT — otherwise an early-exit
    // bug could make this benchmark accidentally fast and silently wrong.
    let stmt = parse_statement(&sql).expect("parse_statement");
    assert!(
        matches!(stmt, Statement::Insert(_)),
        "expected Statement::Insert, got {stmt:?}",
    );
    let (allocations, allocation_bytes) = allocation_stats(|| {
        std::hint::black_box(parse_statement(&sql).expect("parse_statement"));
    });
    ParseStats {
        label: "sysbench-insert",
        rows,
        sql_bytes: sql.len(),
        normalize_secs,
        parse_secs,
        allocations,
        allocation_bytes,
    }
}

/// Measure a single fixed-SQL shape over `iters` parses. `rows` is for
/// reporting only — the SQL itself is constant per call.
fn measure_shape(
    label: &'static str,
    sql: String,
    rows: usize,
    warmup: usize,
    iters: usize,
    expect: fn(&Statement) -> bool,
) -> ParseStats {
    for _ in 0..warmup {
        let _ = parse_statement(&sql).expect("parse_statement");
    }
    let normalize_secs = time_loop(
        || {
            let _ = normalize_sql(&sql);
        },
        iters,
    );
    let parse_secs = time_loop(
        || {
            let stmt = parse_statement(&sql).expect("parse_statement");
            std::hint::black_box(&stmt);
        },
        iters,
    );
    let stmt = parse_statement(&sql).expect("parse_statement");
    assert!(
        expect(&stmt),
        "{label}: unexpected Statement variant: {stmt:?}",
    );
    let (allocations, allocation_bytes) = allocation_stats(|| {
        std::hint::black_box(parse_statement(&sql).expect("parse_statement"));
    });
    ParseStats {
        label,
        rows,
        sql_bytes: sql.len(),
        normalize_secs,
        parse_secs,
        allocations,
        allocation_bytes,
    }
}

fn measure_batch_shape(
    label: &'static str,
    sql: String,
    expected_statements: usize,
    warmup: usize,
    iters: usize,
) -> ParseStats {
    for _ in 0..warmup {
        assert_eq!(
            parse_statements(&sql).expect("parse_statements").len(),
            expected_statements
        );
    }
    let parse_secs = time_loop(
        || {
            std::hint::black_box(parse_statements(&sql).expect("parse_statements"));
        },
        iters,
    );
    let (allocations, allocation_bytes) = allocation_stats(|| {
        std::hint::black_box(parse_statements(&sql).expect("parse_statements"));
    });
    ParseStats {
        label,
        rows: expected_statements,
        sql_bytes: sql.len(),
        normalize_secs: 0.0,
        parse_secs,
        allocations,
        allocation_bytes,
    }
}

fn build_point_select() -> String {
    // sysbench oltp_point_select shape.
    "SELECT c FROM sbtest1 WHERE id = 314159".to_string()
}

fn build_range_select() -> String {
    // sysbench oltp_read_only range-scan shape (100-row range).
    "SELECT c FROM sbtest1 WHERE id BETWEEN 1000 AND 1099 ORDER BY c".to_string()
}

fn build_update_index() -> String {
    // sysbench oltp_update_index shape.
    "UPDATE sbtest1 SET k = k + 1 WHERE id = 271828".to_string()
}

fn build_delete() -> String {
    // sysbench oltp_delete shape.
    "DELETE FROM sbtest1 WHERE id = 161803".to_string()
}

fn build_single_row_insert() -> String {
    // sysbench oltp_insert single-row shape (no batched VALUES list).
    let c = "abcdefgh".repeat(15);
    let pad = "01234567".repeat(7) + "ABCD";
    format!("INSERT INTO sbtest1(k, c, pad) VALUES (42, '{c}', '{pad}')")
}

fn build_join_select() -> String {
    // 2-table inner join — exercises a wider parser surface (FROM list,
    // JOIN, ON, ORDER BY).
    "SELECT a.id, b.k FROM sbtest1 a INNER JOIN sbtest2 b ON a.id = b.id \
     WHERE a.k > 100 ORDER BY a.id LIMIT 50"
        .to_string()
}

fn build_long_select() -> String {
    let projection = (0..80)
        .map(|idx| format!("t.c{} + {} AS x{}", idx % 12, idx, idx))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {projection} FROM wide_table t WHERE t.id BETWEEN 100 AND 200 \
         ORDER BY t.id LIMIT 50"
    )
}

fn build_rewrite_dense_ddl() -> String {
    "CREATE TABLE doc_metrics (id BIGINT UNSIGNED ZEROFILL, body TEXT, tags JSON, \
     FULLTEXT KEY ft_body (body) WITH PARSER ngram, \
     KEY mv_tags ((CAST(tags AS UNSIGNED ARRAY))) INVISIBLE) \
     PCTFREE=15 ZONE_MAP=ON ZONE_SIZE=1M"
        .to_string()
}

fn build_prepared_cte_window() -> String {
    "WITH c AS (SELECT ? AS tenant_id) \
     SELECT ?, d.total, SUM(?) OVER w \
     FROM (SELECT tenant_id, SUM(amount + ?) total FROM orders GROUP BY tenant_id) d \
     JOIN c ON c.tenant_id = d.tenant_id AND d.total > ? \
     WINDOW w AS (PARTITION BY ? ORDER BY ? ROWS BETWEEN ? PRECEDING AND ? FOLLOWING) \
     ORDER BY ? LIMIT ? OFFSET ?"
        .to_string()
}

fn build_comment_dense_select() -> String {
    "/* client metadata */ SELECT /*!80000 1 + */ 2 AS n, \
     '/* inert */;-- ?' AS s /* ordinary ; ? */, COUNT(*) \
     FROM t -- line ; ?\n WHERE id IN (1, 2, 3) # tail bait\n GROUP BY n"
        .to_string()
}

fn build_update_delete_batch(statements: usize) -> String {
    let mut sql = String::with_capacity(statements * 64);
    for idx in 0..statements {
        if idx % 2 == 0 {
            sql.push_str(&format!(
                "UPDATE accounts SET balance = balance + 1 WHERE id = {idx};"
            ));
        } else {
            sql.push_str(&format!("DELETE FROM audit_log WHERE id = {idx};"));
        }
    }
    sql
}

fn main() {
    let warmup = std::env::var("MYDB_PARSE_BENCH_WARMUP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_WARMUP);
    let iters = std::env::var("MYDB_PARSE_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_ITERATIONS);
    let batch_sizes: Vec<usize> = std::env::var("MYDB_PARSE_BENCH_ROWS")
        .ok()
        .map(|v| {
            v.split(',')
                .filter_map(|s| s.trim().parse::<usize>().ok())
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_BATCH_SIZES.to_vec());

    println!("parse_bench config: warmup={warmup} iters={iters} batch_sizes={batch_sizes:?}",);
    println!();

    // Batched INSERT (varying batch size).
    for &rows in &batch_sizes {
        let stats = measure_insert(rows, warmup, iters);
        stats.print();
    }
    measure_shape(
        "comment-heavy-insert",
        build_comment_heavy_insert(5000),
        5000,
        warmup,
        iters,
        |statement| matches!(statement, Statement::Insert(_)),
    )
    .print();
    println!();

    // Tight-loop shapes (constant SQL). Use higher iter count since each
    // parse is sub-millisecond and we want stable measurements. Each call
    // also verifies it produces the expected Statement variant so any
    // PreScan/Cow regression that silently mis-routes would assert here.
    let tight_iters = iters * 50;
    let tight_warmup = (warmup * 5).max(10);
    let shapes: &[(&'static str, fn() -> String, fn(&Statement) -> bool)] = &[
        ("point-select", build_point_select, |s| {
            matches!(s, Statement::Select(_))
        }),
        ("range-select", build_range_select, |s| {
            matches!(s, Statement::Select(_))
        }),
        ("join-select", build_join_select, |s| {
            matches!(s, Statement::Select(_))
        }),
        ("single-insert", build_single_row_insert, |s| {
            matches!(s, Statement::Insert(_))
        }),
        ("update-index", build_update_index, |s| {
            matches!(s, Statement::Update(_))
        }),
        ("delete-point", build_delete, |s| {
            matches!(s, Statement::Delete(_))
        }),
    ];
    for (label, build, expect) in shapes {
        let stats = measure_shape(label, build(), 1, tight_warmup, tight_iters, *expect);
        stats.print();
    }
    println!();

    // Refactor-specific coverage: long no-rewrite SQL, compatibility-heavy
    // DDL, prepared nested queries, comments/executable comments, batches and
    // compound routine bodies.
    for (label, sql, expect) in [
        (
            "vector-operator-select",
            "SELECT a <-> b AS distance, (a <#> b) AS negative_dot FROM embeddings ORDER BY distance LIMIT 20".to_owned(),
            (|s: &Statement| matches!(s, Statement::Select(_))) as fn(&Statement) -> bool,
        ),
        (
            "long-select",
            build_long_select(),
            (|s: &Statement| matches!(s, Statement::Select(_))) as fn(&Statement) -> bool,
        ),
        (
            "rewrite-dense-ddl",
            build_rewrite_dense_ddl(),
            (|s: &Statement| matches!(s, Statement::CreateTable(_))) as fn(&Statement) -> bool,
        ),
        (
            "prepared-cte-window",
            build_prepared_cte_window(),
            (|s: &Statement| matches!(s, Statement::WithCte(_))) as fn(&Statement) -> bool,
        ),
        (
            "update-modifiers",
            "UPDATE LOW_PRIORITY IGNORE t SET v=v+1 WHERE id=?".to_owned(),
            (|s: &Statement| matches!(s, Statement::Update(update) if update.ignore && update.low_priority)) as fn(&Statement) -> bool,
        ),
        (
            "delete-modifiers",
            "DELETE LOW_PRIORITY QUICK IGNORE FROM t WHERE id=?".to_owned(),
            (|s: &Statement| matches!(s, Statement::Delete(delete) if delete.ignore && delete.low_priority && delete.quick)) as fn(&Statement) -> bool,
        ),
        (
            "explain-select",
            "EXPLAIN SELECT id FROM t WHERE id=?".to_owned(),
            (|s: &Statement| matches!(s, Statement::Explain { .. })) as fn(&Statement) -> bool,
        ),
        (
            "explain-comment-table",
            "EXPLAIN /* header */ FORMAT=TREE TABLE t LIMIT 2".to_owned(),
            (|s: &Statement| matches!(s, Statement::Explain { .. })) as fn(&Statement) -> bool,
        ),
        (
            "explain-connection",
            "EXPLAIN FORMAT=JSON FOR /* target */ CONNECTION 7".to_owned(),
            (|s: &Statement| matches!(s, Statement::ExplainForConnection { connection_id: 7, .. })) as fn(&Statement) -> bool,
        ),
        (
            "transaction-begin",
            "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY".to_owned(),
            (|s: &Statement| matches!(s, Statement::Begin { read_only: Some(true), consistent_snapshot: true })) as fn(&Statement) -> bool,
        ),
        (
            "transaction-characteristics",
            "SET SESSION TRANSACTION READ ONLY, ISOLATION LEVEL SERIALIZABLE".to_owned(),
            (|s: &Statement| matches!(s, Statement::SetTransactionCharacteristics { .. })) as fn(&Statement) -> bool,
        ),
        (
            "set-autocommit",
            "SET @@SESSION.autocommit=OFF".to_owned(),
            (|s: &Statement| matches!(s, Statement::SetAutocommit(false))) as fn(&Statement) -> bool,
        ),
        (
            "set-charset",
            "SET CHARACTER SET utf8mb4".to_owned(),
            (|s: &Statement| matches!(s, Statement::SetCharacterSet { charset: Some(_) })) as fn(&Statement) -> bool,
        ),
        (
            "qualified-view",
            "CREATE VIEW app.v AS SELECT id FROM t".to_owned(),
            (|s: &Statement| matches!(s, Statement::CreateView { name, .. } if name.database() == Some("app") && name.object() == "v")) as fn(&Statement) -> bool,
        ),
        (
            "view-check-option",
            "CREATE VIEW v AS SELECT id FROM t WITH LOCAL CHECK OPTION /* tail */".to_owned(),
            (|s: &Statement| matches!(s, Statement::CreateView { check_option: Some(_), .. })) as fn(&Statement) -> bool,
        ),
        (
            "sequence-options",
            "CREATE SEQUENCE s AS BIGINT START WITH 1 INCREMENT BY 2 MINVALUE 1 MAXVALUE 10000 CACHE 16 NO CYCLE OWNED BY t.id".to_owned(),
            (|s: &Statement| matches!(s, Statement::CreateSequence { .. })) as fn(&Statement) -> bool,
        ),
        (
            "using-join-select",
            "SELECT l.id FROM left_table l JOIN right_table r USING(id)".to_owned(),
            (|s: &Statement| matches!(s, Statement::Select(_))) as fn(&Statement) -> bool,
        ),
        (
            "generate-series-select",
            "SELECT * FROM generate_series(1,10,2) WITH ORDINALITY AS g(n,seq)".to_owned(),
            (|s: &Statement| matches!(s, Statement::Select(_))) as fn(&Statement) -> bool,
        ),
        (
            "json-table-select",
            "SELECT * FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS(n INT PATH '$')) AS j".to_owned(),
            (|s: &Statement| matches!(s, Statement::Select(_))) as fn(&Statement) -> bool,
        ),
        (
            "table-cte",
            "WITH c AS (TABLE test.t ORDER BY id LIMIT 10) SELECT * FROM c".to_owned(),
            (|s: &Statement| matches!(s, Statement::WithCte(_))) as fn(&Statement) -> bool,
        ),
        (
            "with-update",
            "WITH c AS (SELECT ? AS id) UPDATE t SET v=(SELECT id FROM c) WHERE id=?".to_owned(),
            (|s: &Statement| matches!(s, Statement::WithCte(with) if matches!(*with.body, Statement::Update(_)))) as fn(&Statement) -> bool,
        ),
        (
            "with-delete",
            "WITH c AS (SELECT ? AS id) DELETE FROM t WHERE id IN (SELECT id FROM c)".to_owned(),
            (|s: &Statement| matches!(s, Statement::WithCte(with) if matches!(*with.body, Statement::Delete(_)))) as fn(&Statement) -> bool,
        ),
        (
            "comment-executable-select",
            build_comment_dense_select(),
            (|s: &Statement| matches!(s, Statement::Select(_))) as fn(&Statement) -> bool,
        ),
        (
            "compound-routine",
            "CREATE PROCEDURE p_bench() BEGIN SELECT ';' AS s; IF 1 = 1 THEN SELECT 2; END IF; END"
                .to_string(),
            (|s: &Statement| matches!(s, Statement::CreateRoutine(_))) as fn(&Statement) -> bool,
        ),
        (
            "compound-event",
            "CREATE EVENT e_bench ON SCHEDULE EVERY 1 DAY DO BEGIN SELECT ';' AS s; SELECT 2; END"
                .to_string(),
            (|s: &Statement| matches!(s, Statement::CreateEvent(_))) as fn(&Statement) -> bool,
        ),
    ] {
        measure_shape(label, sql, 1, tight_warmup, tight_iters, expect).print();
    }
    measure_batch_shape(
        "update-delete-batch-100",
        build_update_delete_batch(100),
        100,
        warmup,
        iters,
    )
    .print();
    println!();
}
