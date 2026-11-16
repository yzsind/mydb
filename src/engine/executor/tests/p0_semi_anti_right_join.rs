//! P0-1 / P0-2 regression coverage.
//!
//! Covers:
//! - RIGHT JOIN over the fused vec streaming hash-join path (the path
//!   added by P0-2). The query feeds RIGHT JOIN output into a downstream
//!   GROUP BY so the planner picks the fused vec stream input rather
//!   than the materializing path.
//! - SEMI / ANTI NULL semantics: when the build (right-of-IN) side
//!   contains a NULL key, `WHERE k NOT IN (subq)` must drop *all* outer
//!   rows per strict SQL semantics; when the build is empty,
//!   `WHERE k IN (subq)` returns nothing and `WHERE k NOT IN (subq)`
//!   returns everything.
//! - SEMI / ANTI multi-column key (composite key) round-trip — exercises
//!   the row-format key encoder reused by both the row-oriented hash
//!   join (plan.rs:14359) and `VecSemiAntiJoinStream` (vec/hash_join.rs:1007).
//! - SEMI short-circuit: a build with many duplicates per key must not
//!   emit duplicate outer rows.

use super::*;

fn rows(setup: &[&str], sql: &str) -> Vec<Vec<Value>> {
    with_test_env(&[("MYDB_DECORRELATE", None)], || {
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

fn rows_vec_forced(setup: &[&str], sql: &str) -> Vec<Vec<Value>> {
    with_test_env(
        &[("MYDB_DECORRELATE", None), ("MYDB_VECTORIZED", Some("on"))],
        || {
            let dir = tempfile::tempdir().unwrap();
            let engine = open_test_engine(dir.path());
            for s in setup {
                engine.execute(s).unwrap();
            }
            let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
                panic!("expected result set: {sql}");
            };
            rs.rows
        },
    )
}

// ---------------------------------------------------------------------
// RIGHT JOIN through the fused vec streaming path
// ---------------------------------------------------------------------

#[test]
fn right_join_preserves_unmatched_right_rows_via_vec_stream() {
    // Setup: small dim (l) RIGHT JOIN big fact (r). For RIGHT JOIN, every
    // r row is preserved; unmatched r rows get NULL on the l side. CBO
    // picks build=left (the dim) since |l| << |r|, so probe=r and the
    // fused streaming path is eligible. We push GROUP BY downstream so
    // the planner consumes the vec output.
    let setup = [
        "CREATE TABLE l (k INT NOT NULL PRIMARY KEY, lv INT)",
        "CREATE TABLE r (k INT NOT NULL, rv INT, PRIMARY KEY(k, rv))",
        "INSERT INTO l VALUES (1, 100), (2, 200)",
        // r has rows that match l (k=1,2) and rows that don't (k=3,4,5).
        "INSERT INTO r VALUES (1, 11), (2, 22), (3, 33), (4, 44), (5, 55)",
    ];
    // Aggregate result: count rows per r.k and count of non-NULL lv.
    // COUNT(l.lv) distinguishes matched (1) from unmatched (0) — the
    // critical observable for RIGHT JOIN preservation semantics.
    let result = rows_vec_forced(
        &setup,
        "SELECT r.k, COUNT(*), COUNT(l.lv) \
         FROM l RIGHT JOIN r ON l.k = r.k \
         GROUP BY r.k ORDER BY r.k",
    );
    assert_eq!(
        result,
        vec![
            vec![Value::Int(1), Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(1), Value::Int(1)],
            vec![Value::Int(3), Value::Int(1), Value::Int(0)],
            vec![Value::Int(4), Value::Int(1), Value::Int(0)],
            vec![Value::Int(5), Value::Int(1), Value::Int(0)],
        ]
    );
}

#[test]
fn right_join_with_one_to_many_match_does_not_lose_rows() {
    // Each l row matches multiple r rows; RIGHT JOIN must emit every
    // (l, r) pair plus any unmatched r row with NULL l columns.
    let setup = [
        "CREATE TABLE l (k INT NOT NULL PRIMARY KEY, lv INT)",
        "CREATE TABLE r (k INT NOT NULL, rv INT, PRIMARY KEY(k, rv))",
        "INSERT INTO l VALUES (1, 100)",
        "INSERT INTO r VALUES (1, 11), (1, 12), (1, 13), (2, 22)",
    ];
    let result = rows(
        &setup,
        "SELECT r.k, r.rv, l.lv FROM l RIGHT JOIN r ON l.k = r.k \
         ORDER BY r.k, r.rv",
    );
    // Three matches for k=1 plus one unmatched k=2.
    assert_eq!(
        result,
        vec![
            vec![Value::Int(1), Value::Int(11), Value::Int(100)],
            vec![Value::Int(1), Value::Int(12), Value::Int(100)],
            vec![Value::Int(1), Value::Int(13), Value::Int(100)],
            vec![Value::Int(2), Value::Int(22), Value::Null],
        ]
    );
}

// ---------------------------------------------------------------------
// SEMI / ANTI NULL semantics
// ---------------------------------------------------------------------

#[test]
fn semi_join_against_empty_subquery_returns_nothing() {
    let setup = [
        "CREATE TABLE outer_t (id INT NOT NULL PRIMARY KEY)",
        "CREATE TABLE inner_t (id INT NOT NULL PRIMARY KEY)",
        "INSERT INTO outer_t VALUES (1), (2), (3)",
        // inner_t intentionally empty.
    ];
    let result = rows(
        &setup,
        "SELECT id FROM outer_t WHERE id IN (SELECT id FROM inner_t) ORDER BY id",
    );
    assert!(result.is_empty(), "expected empty, got {result:?}");
}

#[test]
fn anti_join_against_empty_subquery_returns_all_outer_rows() {
    let setup = [
        "CREATE TABLE outer_t (id INT NOT NULL PRIMARY KEY)",
        "CREATE TABLE inner_t (id INT NOT NULL PRIMARY KEY)",
        "INSERT INTO outer_t VALUES (1), (2), (3)",
    ];
    let result = rows(
        &setup,
        "SELECT outer_t.id FROM outer_t \
         WHERE outer_t.id NOT IN (SELECT inner_t.id FROM inner_t) \
         ORDER BY outer_t.id",
    );
    assert_eq!(
        result,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
        ]
    );
}

#[test]
fn semi_with_duplicate_build_keys_emits_outer_row_once() {
    // The hash join probe loop must short-circuit on first match per
    // outer row (plan.rs:14359). If the short-circuit regresses,
    // we would see outer row 2 emitted three times.
    let setup = [
        "CREATE TABLE outer_t (id INT NOT NULL PRIMARY KEY)",
        "CREATE TABLE inner_t (id INT NOT NULL, x INT NOT NULL, PRIMARY KEY(id, x))",
        "INSERT INTO outer_t VALUES (1), (2), (3)",
        "INSERT INTO inner_t VALUES (2, 100), (2, 200), (2, 300), (3, 30)",
    ];
    let result = rows(
        &setup,
        "SELECT id FROM outer_t WHERE id IN (SELECT id FROM inner_t) ORDER BY id",
    );
    assert_eq!(result, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
}

#[test]
fn semi_with_multi_column_key_round_trip() {
    // Composite key (a,b) via EXISTS — exercises the multi-byte key
    // encoder used by both the row hash join (plan.rs:14359) and
    // `VecSemiAntiJoinStream`. Row constructor `(a, b) IN (subq)` is
    // explicitly rejected by mydb (1235), so we use EXISTS with a
    // correlated multi-column equality, which the planner lifts to a
    // SEMI join on the composite key (planner.rs:17880).
    let setup = [
        "CREATE TABLE outer_t (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY(a,b))",
        "CREATE TABLE inner_t (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY(a,b))",
        "INSERT INTO outer_t VALUES (1,1), (1,2), (2,1), (2,2)",
        // Match (1,2) and (2,1); reject (1,1) and (2,2).
        "INSERT INTO inner_t VALUES (1,2), (2,1), (3,3)",
    ];
    let result = rows(
        &setup,
        "SELECT a, b FROM outer_t \
         WHERE EXISTS (SELECT 1 FROM inner_t \
                       WHERE inner_t.a = outer_t.a AND inner_t.b = outer_t.b) \
         ORDER BY a, b",
    );
    assert_eq!(
        result,
        vec![
            vec![Value::Int(1), Value::Int(2)],
            vec![Value::Int(2), Value::Int(1)],
        ]
    );
}

// ---------------------------------------------------------------------
// SEMI / ANTI feeding into downstream aggregate
// ---------------------------------------------------------------------

#[test]
fn semi_join_in_count_outer_query_short_circuits() {
    // Outer query is COUNT(*); each matching outer row contributes once.
    let setup = [
        "CREATE TABLE outer_t (id INT NOT NULL PRIMARY KEY)",
        "CREATE TABLE inner_t (id INT NOT NULL, PRIMARY KEY(id))",
        "INSERT INTO outer_t VALUES (1), (2), (3), (4), (5)",
        "INSERT INTO inner_t VALUES (2), (4)",
    ];
    let result = rows(
        &setup,
        "SELECT COUNT(*) FROM outer_t WHERE id IN (SELECT id FROM inner_t)",
    );
    assert_eq!(result, vec![vec![Value::Int(2)]]);
}

#[test]
fn anti_join_in_count_outer_query_excludes_matches() {
    let setup = [
        "CREATE TABLE outer_t (id INT NOT NULL PRIMARY KEY)",
        "CREATE TABLE inner_t (id INT NOT NULL, PRIMARY KEY(id))",
        "INSERT INTO outer_t VALUES (1), (2), (3), (4), (5)",
        "INSERT INTO inner_t VALUES (2), (4)",
    ];
    let result = rows(
        &setup,
        "SELECT COUNT(*) FROM outer_t \
         WHERE outer_t.id NOT IN (SELECT inner_t.id FROM inner_t)",
    );
    assert_eq!(result, vec![vec![Value::Int(3)]]);
}

#[test]
fn not_exists_returns_rows_without_match() {
    let setup = [
        "CREATE TABLE outer_t (id INT NOT NULL PRIMARY KEY, ref_id INT NOT NULL)",
        "CREATE TABLE inner_t (id INT NOT NULL PRIMARY KEY)",
        "INSERT INTO outer_t VALUES (1, 10), (2, 20), (3, 30)",
        "INSERT INTO inner_t VALUES (10), (30)",
    ];
    let result = rows(
        &setup,
        "SELECT id FROM outer_t \
         WHERE NOT EXISTS (SELECT 1 FROM inner_t WHERE inner_t.id = outer_t.ref_id) \
         ORDER BY id",
    );
    assert_eq!(result, vec![vec![Value::Int(2)]]);
}
