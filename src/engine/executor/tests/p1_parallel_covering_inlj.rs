//! P1-6 regression coverage for the parallel covering INLJ path
//! (`plan.rs:9540`, `execute_batched_inlj_covering` invoked via
//! `run_inlj_outer_chunks` when `inlj_outer_parallelism > 1`).
//!
//! Differential strategy: same query, same data, run twice — once with
//! parallelism forced ON (`MYDB_INLJ_OUTER_PARALLELISM=4`) and once
//! forced OFF (`MYDB_INLJ_OUTER_PARALLELISM=1`). The result rows must
//! be identical after ORDER BY.
//!
//! Trigger conditions for the parallel covering path (per `plan.rs:9482-9540`):
//! - the batched path uses its fixed actual-row thresholds (64 left rows when
//!   the real right side has at least 1k rows, or 8 when it has at least 256)
//! - inner join type (Inner / Left / Semi / Anti)
//! - right side covering (only the index is read, no heap fetch)
//! - equality predicate (plan.eq_exprs non-empty, plan.range none)
//!
//! Data shape: outer = 256 rows wide, inner has a covering composite
//! index `(k, v)` so the INLJ probe touches only the index.

use super::*;

fn rows_with_env(
    vars: &[(&'static str, Option<&str>)],
    setup: &[&str],
    sql: &str,
) -> Vec<Vec<Value>> {
    with_test_env(vars, || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        for s in setup {
            engine.execute(s).unwrap();
        }
        let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
            panic!("expected result set: {sql}");
        };
        rs.rows
    })
}

fn make_setup() -> Vec<String> {
    let mut setup = vec![
        "CREATE TABLE outer_t (id INT NOT NULL PRIMARY KEY, k INT NOT NULL)".to_string(),
        "CREATE TABLE inner_t (k INT NOT NULL, v INT NOT NULL, \
            PRIMARY KEY(k, v))"
            .to_string(),
    ];
    // outer: 256 rows, k cycles through 0..16 (so each k matches 16 inner rows)
    let mut outer_values = Vec::with_capacity(256);
    for i in 0..256 {
        outer_values.push(format!("({},{})", i, i % 16));
    }
    setup.push(format!(
        "INSERT INTO outer_t VALUES {}",
        outer_values.join(",")
    ));
    // inner: k=0..15, v=0..15; so each outer row matches 16 inner rows
    let mut inner_values = Vec::with_capacity(16 * 16);
    for k in 0..16 {
        for v in 0..16 {
            inner_values.push(format!("({},{})", k, v));
        }
    }
    setup.push(format!(
        "INSERT INTO inner_t VALUES {}",
        inner_values.join(",")
    ));
    setup
}

fn make_noncovering_probe_setup() -> Vec<String> {
    let mut setup = vec![
        "CREATE TABLE probe_outer (id INT NOT NULL PRIMARY KEY, k INT NOT NULL)".to_string(),
        "CREATE TABLE probe_inner (id INT NOT NULL PRIMARY KEY, k INT NOT NULL, payload INT NOT NULL, \
            INDEX idx_probe_k(k))"
            .to_string(),
    ];
    // Keep the real outer below the fixed outer-parallel threshold. Each key
    // repeats, so probing finds 512 join candidates backed by 256 distinct
    // heap RowIds. The post-probe reader therefore owns exactly two 128-row
    // tasks without consulting cardinality statistics.
    let outer_values: Vec<String> = (0..16).map(|id| format!("({id},{})", id % 8)).collect();
    setup.push(format!(
        "INSERT INTO probe_outer VALUES {}",
        outer_values.join(",")
    ));
    let inner_values: Vec<String> = (0..256)
        .map(|id| format!("({id},{},{})", id % 8, id * 3))
        .collect();
    setup.push(format!(
        "INSERT INTO probe_inner VALUES {}",
        inner_values.join(",")
    ));
    setup
}

#[test]
fn covering_inlj_parallel_vs_serial_inner_join_match() {
    let setup_owned = make_setup();
    let setup: Vec<&str> = setup_owned.iter().map(|s| s.as_str()).collect();
    // Force batched path (left=256 ≥ 8 because right=256).
    // Inner_t has PRIMARY KEY(k,v) — that's a covering composite index
    // so a probe by k uses the leading prefix and stays index-only.
    let sql = "SELECT outer_t.id, COUNT(*) \
               FROM outer_t \
               INNER JOIN inner_t ON inner_t.k = outer_t.k \
               GROUP BY outer_t.id \
               ORDER BY outer_t.id";
    let serial = rows_with_env(&[("MYDB_INLJ_OUTER_PARALLELISM", Some("1"))], &setup, sql);
    let parallel = rows_with_env(&[("MYDB_INLJ_OUTER_PARALLELISM", Some("4"))], &setup, sql);
    assert_eq!(
        serial, parallel,
        "parallel covering INLJ diverged from serial"
    );
    // Sanity: each outer row matches 16 inner rows.
    assert_eq!(serial.len(), 256);
    for row in &serial {
        assert_eq!(row[1], Value::Int(16));
    }
}

#[test]
fn covering_inlj_parallel_left_join_preserves_unmatched_outer() {
    let setup_owned = make_setup();
    let setup: Vec<&str> = setup_owned.iter().map(|s| s.as_str()).collect();
    // Force unmatched outer rows by querying k=64 (no such inner k).
    let sql = "SELECT outer_t.id, COUNT(inner_t.v) \
               FROM outer_t \
               LEFT JOIN inner_t ON inner_t.k = outer_t.k AND inner_t.k = 64 \
               GROUP BY outer_t.id \
               ORDER BY outer_t.id";
    let serial = rows_with_env(&[("MYDB_INLJ_OUTER_PARALLELISM", Some("1"))], &setup, sql);
    let parallel = rows_with_env(&[("MYDB_INLJ_OUTER_PARALLELISM", Some("4"))], &setup, sql);
    assert_eq!(serial, parallel);
    // All outer rows preserved, 0 matches each.
    assert_eq!(serial.len(), 256);
    for row in &serial {
        assert_eq!(row[1], Value::Int(0));
    }
}

#[test]
fn covering_inlj_parallel_semi_join_via_exists_matches_serial() {
    let setup_owned = make_setup();
    let setup: Vec<&str> = setup_owned.iter().map(|s| s.as_str()).collect();
    // EXISTS rewrites to SEMI INLJ; covering composite key probe.
    let sql = "SELECT outer_t.id FROM outer_t \
               WHERE EXISTS (SELECT 1 FROM inner_t WHERE inner_t.k = outer_t.k) \
               ORDER BY outer_t.id";
    let serial = rows_with_env(&[("MYDB_INLJ_OUTER_PARALLELISM", Some("1"))], &setup, sql);
    let parallel = rows_with_env(&[("MYDB_INLJ_OUTER_PARALLELISM", Some("4"))], &setup, sql);
    assert_eq!(serial, parallel);
    // Every outer row's k (0..15) has matches → all 256 outer rows kept.
    assert_eq!(serial.len(), 256);
}

#[test]
fn noncovering_inlj_post_probe_parallel_matches_serial() {
    use std::sync::atomic::Ordering;

    let setup_owned = make_noncovering_probe_setup();
    let setup: Vec<&str> = setup_owned.iter().map(|s| s.as_str()).collect();
    let sql = "SELECT /*+ ORDERED INDEX_NLJ(probe_outer, probe_inner) \
                      NO_HASH_JOIN(probe_outer, probe_inner) */ \
                      probe_outer.id, probe_inner.id, probe_inner.payload \
               FROM probe_outer JOIN probe_inner ON probe_inner.k = probe_outer.k \
               ORDER BY probe_outer.id, probe_inner.id";
    let serial = rows_with_env(
        &[
            ("MYDB_INLJ_OUTER_PARALLELISM", Some("1")),
            ("MYDB_INLJ_OUTER_PARALLEL_THRESHOLD", Some("100000")),
            ("MYDB_INLJ_BATCH_MIN_LEFT_ROWS", Some("1")),
            ("MYDB_INLJ_BATCH_MIN_RIGHT_ROWS", Some("0")),
            ("MYDB_INLJ_PROBE_TASK_ROWS", Some("128")),
        ],
        &setup,
        sql,
    );
    let before = crate::engine::executor::select::plan::INLJ_PROBE_PARALLEL_DECISIONS
        .load(Ordering::Relaxed);
    let parallel = rows_with_env(
        &[
            ("MYDB_INLJ_OUTER_PARALLELISM", Some("4")),
            ("MYDB_INLJ_OUTER_PARALLEL_THRESHOLD", Some("100000")),
            ("MYDB_INLJ_BATCH_MIN_LEFT_ROWS", Some("1")),
            ("MYDB_INLJ_BATCH_MIN_RIGHT_ROWS", Some("0")),
            ("MYDB_INLJ_PROBE_TASK_ROWS", Some("128")),
        ],
        &setup,
        sql,
    );
    let after = crate::engine::executor::select::plan::INLJ_PROBE_PARALLEL_DECISIONS
        .load(Ordering::Relaxed);

    assert_eq!(
        serial, parallel,
        "post-probe heap reads changed row order or values"
    );
    assert_eq!(parallel.len(), 512);
    assert!(
        after > before,
        "expected the fixed 128-RowId test tasks to run in parallel"
    );
}
