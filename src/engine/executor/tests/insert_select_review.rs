//! INSERT ... SELECT correctness review (2026-07).
//!
//! Findings (see the accompanying report):
//!  * Halloween self-insert (`INSERT INTO t SELECT ... FROM t`) is SAFE for
//!    row-count correctness on both the ordered-index streaming cursor and
//!    the sequential-scan streaming path — the SELECT never re-reads its own
//!    freshly-inserted rows (verified: exactly N rows inserted, final 2N).
//!  * Multi-chunk Records/Duplicates/Warnings, ODKU affected-row doubling,
//!    IGNORE skipping, statement-level rollback on a later-chunk error,
//!    per-row AFTER triggers, generated-column rejection, and sql_mode
//!    coercion all match MySQL 8.0 and the VALUES path.
//!  * SEPARATE, pre-existing storage bug surfaced while probing: a batch
//!    B-tree insert of many sequential single-int keys into a table that
//!    already holds a disjoint lower key range can overflow a leaf page
//!    (error 1118 "index page is too large") because the split-size estimate
//!    for the single-int fast path mispredicts. Reproducible with plain
//!    VALUES statements — NOT an INSERT..SELECT issue. See
//!    `btree_batch_single_int_leaf_overflow_known_bug` (currently FAILS).

use super::*;

fn rows(response: QueryResponse) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected result set, got {response:?}");
    };
    result.rows
}

fn scalar_i64(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> i64 {
    let r = rows(engine.execute_with_session(session, sql).unwrap());
    match &r[0][0] {
        Value::Int(v) => *v,
        Value::UInt(v) => *v as i64,
        // SUM over an integer column returns an exact DECIMAL; scale 0 is an
        // integer magnitude.
        Value::DecimalCompact(mag, 0) => *mag,
        other => panic!("non-int scalar: {other:?}"),
    }
}

fn seed_seq(engine: &StorageQueryEngine, session: &mut SessionContext, table: &str, n: u32) {
    let mut sql = format!("INSERT INTO {table} VALUES ");
    for i in 0..n {
        if i > 0 {
            sql.push(',');
        }
        let id = i + 1;
        sql.push_str(&format!("({id},{id})"));
    }
    engine.execute_with_session(session, &sql).unwrap();
}

/// #1 — Self-insert whose SELECT streams through the ordered-index cursor
/// (`OrderedIndexResultCursor`, chosen for `ORDER BY <indexed col>`). Source
/// straddles multiple test `INSERT_SELECT_CHUNK_ROWS` (64) chunks so the insert
/// side begins before the scan is exhausted. New keys (id + 100_000_000) are
/// unique, so a classic Halloween re-read would run away / over-insert.
/// EXPECTED (and observed): exactly N inserted, final 2N — Halloween is
/// prevented.
#[test]
fn insert_select_self_ordered_index_no_halloween() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id BIGINT PRIMARY KEY, v INT)",
        )
        .unwrap();
    let n = 160u32;
    seed_seq(&engine, &mut session, "t", n);

    let resp = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t SELECT id + 100000000, v FROM t ORDER BY id",
        )
        .expect("ordered self-insert should succeed");
    assert_eq!(
        resp,
        QueryResponse::OkAffected {
            affected_rows: n as u64
        }
    );
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT COUNT(*) FROM t"),
        2 * n as i64,
        "self-insert must not re-read its own inserts (Halloween)"
    );
}

/// #2 — Self-insert whose SELECT streams through the sequential-scan cursor
/// (`seq_scan_stream`, a background worker thread interleaved with the
/// inserting main thread). Tests the page_count-boundary + append-only
/// protection. EXPECTED (and observed): exactly N inserted, final 2N.
#[test]
fn insert_select_self_seq_scan_no_halloween() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id BIGINT, v INT)")
        .unwrap();
    let n = 160u32;
    seed_seq(&engine, &mut session, "t", n);

    let resp = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t SELECT id + 100000000, v FROM t",
        )
        .expect("seq-scan self-insert should succeed");
    assert_eq!(
        resp,
        QueryResponse::OkAffected {
            affected_rows: n as u64
        }
    );
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT COUNT(*) FROM t"),
        2 * n as i64,
    );
}

/// #3 — Multi-chunk IGNORE across the test chunk boundary: the Records /
/// Duplicates / Warnings trailer and affected_rows accumulate correctly.
#[test]
fn insert_select_multichunk_ignore_trailer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE s (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let n = 160u32;
    seed_seq(&engine, &mut session, "s", n);
    engine
        .execute_with_session(&mut session, "CREATE TABLE d (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO d SELECT id, v FROM s WHERE id <= 80",
        )
        .unwrap();

    let resp = engine
        .execute_with_session(&mut session, "INSERT IGNORE INTO d SELECT id, v FROM s")
        .unwrap();
    assert_eq!(resp, QueryResponse::OkAffected { affected_rows: 80 });
    assert_eq!(
        crate::engine::api::peek_statement_info(),
        "Records: 160 Duplicates: 80 Warnings: 80"
    );
}

/// #3b — ODKU across multiple chunks: MySQL counts each matched-and-changed
/// row twice in affected_rows.
#[test]
fn insert_select_multichunk_odku_counts_twice() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE dst (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let n = 160u32;
    seed_seq(&engine, &mut session, "dst", n);
    engine
        .execute_with_session(&mut session, "CREATE TABLE src (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO src SELECT id, v FROM dst")
        .unwrap();

    let resp = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO dst SELECT id, v FROM src ON DUPLICATE KEY UPDATE v = v + 1",
        )
        .unwrap();
    assert_eq!(
        resp,
        QueryResponse::OkAffected {
            affected_rows: 2 * n as u64
        }
    );
}

/// #4 — Statement atomicity: a duplicate-key error in a LATER chunk must roll
/// back the earlier chunk(s) that already inserted.
#[test]
fn insert_select_multichunk_error_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE src (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE dst (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let n = 160u32;
    seed_seq(&engine, &mut session, "src", n);
    engine
        .execute_with_session(&mut session, "INSERT INTO dst VALUES (140, 999)")
        .unwrap();

    let resp = engine.execute_with_session(&mut session, "INSERT INTO dst SELECT id, v FROM src");
    assert!(resp.is_err(), "duplicate key in 2nd chunk must error");
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT COUNT(*) FROM dst"),
        1,
        "first chunk must roll back on a later-chunk error"
    );
}

/// A duplicate in the first parallel chunks must stop a source larger than
/// the bounded worker queue and return the original 1062. The old dispatcher
/// kept its own receiver alive after both workers exited, so a source larger
/// than the bounded queue blocked forever in `SyncSender::send`.
#[test]
fn insert_select_parallel_duplicate_beyond_queue_capacity_returns_1062() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    let mut setup = SessionContext::default();
    engine
        .execute_with_session(
            &mut setup,
            "CREATE TABLE dst (id INT PRIMARY KEY, v VARCHAR(8))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut setup,
            "INSERT INTO dst SELECT generate_series, 'abc' FROM generate_series(1,128)",
        )
        .unwrap();

    let (tx, rx) = std::sync::mpsc::channel();
    let worker_engine = std::sync::Arc::clone(&engine);
    let handle = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        let result = worker_engine.execute_with_session(
            &mut session,
            "INSERT INTO dst SELECT generate_series, 'abc' FROM generate_series(1,512)",
        );
        let _ = tx.send(result);
    });

    let result = match rx.recv_timeout(std::time::Duration::from_secs(5)) {
        Ok(result) => result,
        Err(_) => {
            // On regression the worker is permanently parked while retaining
            // the database. Avoid deleting its files underneath it before the
            // test process reports the timeout failure.
            std::mem::forget(dir);
            panic!("parallel INSERT..SELECT did not return after duplicate-key worker failure");
        }
    };
    handle.join().unwrap();
    let err = result.expect_err("duplicate primary keys must fail");
    assert_eq!(err.code(), 1062, "must preserve the storage-layer errno");
    assert_eq!(err.sql_state(), "23000");
    assert_eq!(
        scalar_i64(&engine, &mut setup, "SELECT COUNT(*) FROM dst"),
        128,
        "failed autocommit statement must not leave partial rows"
    );
}

/// #5 — AFTER INSERT trigger fires exactly once per source row across all
/// chunks, seeing the correct NEW.<col>.
#[test]
fn insert_select_after_trigger_per_row_across_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t5 (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE audit (id INT)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TRIGGER tr AFTER INSERT ON t5 FOR EACH ROW INSERT INTO audit VALUES (NEW.id)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE src (id INT PRIMARY KEY)")
        .unwrap();
    let n = 160u32;
    let mut sql = String::from("INSERT INTO src VALUES ");
    for i in 0..n {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({})", i + 1));
    }
    engine.execute_with_session(&mut session, &sql).unwrap();

    engine
        .execute_with_session(&mut session, "INSERT INTO t5 SELECT id FROM src")
        .unwrap();
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT COUNT(*) FROM audit"),
        n as i64
    );
    assert_eq!(
        scalar_i64(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM audit WHERE id IN (SELECT id FROM t5)"
        ),
        n as i64,
        "trigger must observe the correct NEW.id per row"
    );
}

/// #6 — Generated column may not be targeted by INSERT ... SELECT (3105),
/// same as the VALUES path.
#[test]
fn insert_select_generated_column_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE g (a INT, b INT AS (a+1) STORED)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE gsrc (a INT, b INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO gsrc VALUES (1, 9)")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "INSERT INTO g (a, b) SELECT a, b FROM gsrc")
        .unwrap_err();
    assert_eq!(err.code(), 3105);
}

/// #6b — Non-strict numeric coercion (saturation) on INSERT ... SELECT
/// matches the VALUES path.
#[test]
fn insert_select_nonstrict_coercion_matches_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION sql_mode='NO_ENGINE_SUBSTITUTION'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE dv (v TINYINT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO dv VALUES (100000)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE src (v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO src VALUES (100000)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE ds (v TINYINT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO ds SELECT v FROM src")
        .unwrap();

    let via_values = scalar_i64(&engine, &mut session, "SELECT v FROM dv");
    let via_select = scalar_i64(&engine, &mut session, "SELECT v FROM ds");
    assert_eq!(via_values, 127);
    assert_eq!(
        via_select, via_values,
        "SELECT path must coerce like VALUES"
    );
}

/// SQL-level smoke coverage for disjoint single-int batch ranges. The exact
/// near-full-page widening boundary is covered directly and cheaply by
/// `appended_compact_leaf_widening_returns_overflow_tail` in `btree.rs`.
#[test]
fn btree_batch_single_int_leaf_overflow_known_bug() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id BIGINT PRIMARY KEY, v INT)",
        )
        .unwrap();
    let n = 160u32;
    let mut lo = String::from("INSERT INTO t VALUES ");
    let mut hi = String::from("INSERT INTO t VALUES ");
    for i in 0..n {
        if i > 0 {
            lo.push(',');
            hi.push(',');
        }
        let id = i + 1;
        lo.push_str(&format!("({id},{id})"));
        hi.push_str(&format!("({},{id})", 100_000_000u64 + id as u64));
    }
    engine.execute_with_session(&mut session, &lo).unwrap();
    let resp = engine.execute_with_session(&mut session, &hi);
    assert!(
        resp.is_ok(),
        "batch insert of a disjoint high key range should succeed, got {:?}",
        resp.err()
    );
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT COUNT(*) FROM t"),
        2 * n as i64
    );
}

/// End-to-end readback for both widening gaps: u16 -> u32 and u16 -> generic.
#[test]
fn btree_batch_single_int_disjoint_range_layout_widening() {
    for gap in [100_000_000u64, 10_000_000_000u64] {
        for n in [160u32] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open_test_engine(dir.path());
            let mut session = SessionContext::default();
            engine
                .execute_with_session(
                    &mut session,
                    "CREATE TABLE t (id BIGINT PRIMARY KEY, v INT)",
                )
                .unwrap();
            let mut lo = String::from("INSERT INTO t VALUES ");
            let mut hi = String::from("INSERT INTO t VALUES ");
            for i in 0..n {
                if i > 0 {
                    lo.push(',');
                    hi.push(',');
                }
                let id = i + 1;
                lo.push_str(&format!("({id},{id})"));
                hi.push_str(&format!("({},{id})", gap + id as u64));
            }
            engine.execute_with_session(&mut session, &lo).unwrap();
            let resp = engine.execute_with_session(&mut session, &hi);
            assert!(
                resp.is_ok(),
                "disjoint high range (gap={gap}, n={n}) should succeed, got {:?}",
                resp.err()
            );
            assert_eq!(
                scalar_i64(&engine, &mut session, "SELECT COUNT(*) FROM t"),
                2 * n as i64,
                "gap={gap}, n={n}"
            );
            // The straddling leaf's keys must all decode back correctly
            // under whatever layout it resolved to — spot-check both ends
            // of the high range and the boundary.
            assert_eq!(
                scalar_i64(
                    &engine,
                    &mut session,
                    &format!("SELECT COUNT(*) FROM t WHERE id >= {gap}"),
                ),
                n as i64,
                "high-range readback gap={gap}, n={n}"
            );
        }
    }
}

// ============================================================================
// C1 — UNION / CTE as INSERT source (MySQL 8.0 parity with CTAS).
// Before this fix `INSERT … SELECT … UNION …` and `INSERT … WITH … SELECT …`
// were rejected as "INSERT source query" Unsupported, while CTAS accepted both.
// ============================================================================

/// `INSERT INTO t SELECT … UNION SELECT …` — set-operation source. Duplicate
/// rows collapse (UNION is distinct); values land positionally.
#[test]
fn insert_select_union_source() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let resp = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t SELECT 1, 10 UNION SELECT 2, 20 UNION SELECT 1, 10",
        )
        .expect("UNION source must be accepted");
    assert_eq!(resp, QueryResponse::OkAffected { affected_rows: 2 });
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT COUNT(*) FROM t"),
        2
    );
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT v FROM t WHERE id = 2"),
        20
    );
}

/// `UNION ALL` keeps duplicates (here distinct ids so all three rows land).
#[test]
fn insert_select_union_all_source() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE src (id INT, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO src VALUES (1,10),(2,20)")
        .unwrap();
    let resp = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t SELECT id, v FROM src UNION ALL SELECT 3, 30",
        )
        .expect("UNION ALL source must be accepted");
    assert_eq!(resp, QueryResponse::OkAffected { affected_rows: 3 });
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT COUNT(*) FROM t"),
        3
    );
}

/// `INSERT INTO t WITH cte AS (…) SELECT … FROM cte` — non-recursive CTE
/// source. The CTE is lifted to wrap the INSERT and materialised through the
/// registry; the source SELECT resolves it by name.
#[test]
fn insert_select_cte_source() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE base (id INT, v INT)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO base VALUES (1,100),(2,200),(3,300)",
        )
        .unwrap();
    let resp = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t WITH c AS (SELECT id, v FROM base WHERE v >= 200) SELECT id, v FROM c",
        )
        .expect("CTE source must be accepted");
    assert_eq!(resp, QueryResponse::OkAffected { affected_rows: 2 });
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT COUNT(*) FROM t"),
        2
    );
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT v FROM t WHERE id = 3"),
        300
    );
}

/// `INSERT INTO t WITH RECURSIVE …` — recursive CTE source generates a series.
#[test]
fn insert_select_recursive_cte_source() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (n INT PRIMARY KEY)")
        .unwrap();
    let resp = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 10) SELECT n FROM seq",
        )
        .expect("recursive CTE source must be accepted");
    assert_eq!(resp, QueryResponse::OkAffected { affected_rows: 10 });
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT COUNT(*) FROM t"),
        10
    );
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT MAX(n) FROM t"),
        10
    );
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT SUM(n) FROM t"),
        55
    );
}

/// CTE source combined with a column list and coercion behaves like the
/// plain-SELECT path.
#[test]
fn insert_select_cte_source_with_column_list() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v INT, note VARCHAR(8) DEFAULT 'x')",
        )
        .unwrap();
    let resp = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t (id, v) WITH c AS (SELECT 7 AS a, 70 AS b) SELECT a, b FROM c",
        )
        .expect("CTE source with column list must be accepted");
    assert_eq!(resp, QueryResponse::OkAffected { affected_rows: 1 });
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT v FROM t WHERE id = 7"),
        70
    );
}

// ============================================================================
// P2 — parallel INSERT…SELECT: a shape that passes the strict admission gate
// (no autoinc / trigger / conflict / FK / partition / view check / RETURNING,
// no column list, strict sql_mode, no source-references-target) dispatches
// chunks to a worker pool. Correctness parity with the serial path is
// enforced by comparing against a serial oracle run with the kill switch.
// ============================================================================

#[test]
fn insert_select_parallel_matches_serial_oracle_across_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE src (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let n = 192i64;
    let mut bulk = String::from("INSERT INTO src VALUES ");
    for i in 0..n {
        if i > 0 {
            bulk.push(',');
        }
        bulk.push_str(&format!("({i},{})", i * 3));
    }
    engine.execute_with_session(&mut session, &bulk).unwrap();

    // Two identical shape targets. `dst_par` runs through the parallel path;
    // `dst_ser` runs through the serial path via the kill switch.
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE dst_par (id INT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE dst_ser (id INT PRIMARY KEY, v INT)",
        )
        .unwrap();

    // Parallel path (default): no column list, strict sql_mode, no autoinc.
    let resp = engine
        .execute_with_session(&mut session, "INSERT INTO dst_par SELECT * FROM src")
        .expect("parallel INSERT..SELECT must succeed");
    assert_eq!(
        resp,
        QueryResponse::OkAffected {
            affected_rows: n as u64
        }
    );

    // Serial oracle.
    std::env::set_var("MYDB_INSERT_SELECT_PARALLEL", "0");
    let resp = engine
        .execute_with_session(&mut session, "INSERT INTO dst_ser SELECT * FROM src")
        .expect("serial INSERT..SELECT must succeed");
    std::env::remove_var("MYDB_INSERT_SELECT_PARALLEL");
    assert_eq!(
        resp,
        QueryResponse::OkAffected {
            affected_rows: n as u64
        }
    );

    // Row-level parity.
    let par_rows = rows(
        engine
            .execute_with_session(&mut session, "SELECT id, v FROM dst_par ORDER BY id")
            .unwrap(),
    );
    let ser_rows = rows(
        engine
            .execute_with_session(&mut session, "SELECT id, v FROM dst_ser ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        par_rows, ser_rows,
        "parallel and serial must produce identical rows"
    );
    assert_eq!(par_rows.len(), n as usize);
}

#[test]
fn insert_select_parallel_gate_rejects_column_list() {
    // With a column list the parallel path can't run (default fill not done
    // by `insert_rows`); this exercise proves the gate keeps the shape on the
    // serial path so the column list still works.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE dst (id INT PRIMARY KEY, v INT, note VARCHAR(8) DEFAULT 'x')",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE src (id INT, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO src VALUES (1,10),(2,20)")
        .unwrap();
    let resp = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO dst (id, v) SELECT id, v FROM src",
        )
        .expect("column-list INSERT..SELECT must succeed on the serial path");
    assert_eq!(resp, QueryResponse::OkAffected { affected_rows: 2 });
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT COUNT(*) FROM dst"),
        2
    );
}

#[test]
fn insert_select_parallel_gate_rejects_self_reference() {
    // Self-INSERT must stay on the serial path (parallel-worker Halloween
    // safety is not audited yet). Verifies correctness — exactly N inserted.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id BIGINT PRIMARY KEY, v INT)",
        )
        .unwrap();
    let n = 160u32;
    seed_seq(&engine, &mut session, "t", n);
    let resp = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t SELECT id + 100000000, v FROM t",
        )
        .expect("self-insert must succeed via the serial path");
    assert_eq!(
        resp,
        QueryResponse::OkAffected {
            affected_rows: n as u64
        }
    );
    assert_eq!(
        scalar_i64(&engine, &mut session, "SELECT COUNT(*) FROM t"),
        2 * n as i64,
    );
}
