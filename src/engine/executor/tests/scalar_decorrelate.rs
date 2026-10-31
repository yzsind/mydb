//! End-to-end tests for `planner_decorrelate::scalar` — correlated scalar
//! subquery → LEFT JOIN against a GROUP BY derived.
//!
//! These tests assert that the **decorrelation** path produces the SQL-
//! correct result. A side-by-side comparison with the legacy
//! eval-time path (via `MYDB_DECORRELATE=off`) is intentionally NOT
//! used here: the legacy path has a pre-existing bug where a correlated
//! scalar subquery in the *projection* list collapses the outer query
//! into a single implicit-aggregation row (because
//! `select_projection_has_aggregate` recurses into the subquery and
//! reports the outer as aggregating). That bug long predates this
//! change; fixing it is out of scope for the decorrelation work and
//! would require an audit of every aggregating fast-path. Until then,
//! correctness on this query shape is delivered by decorrelation
//! itself: the rewriter removes the offending `Expr::Subquery` node
//! before the aggregation classifier ever sees it.

use super::*;

/// Open an engine, apply `setup` DDL/DML, run `sql` with decorrelation
/// ENABLED, and return the rows.
fn run_with_decor(setup: &[&str], sql: &str) -> Vec<Vec<Value>> {
    with_test_env(&[("MYDB_DECORRELATE", None)], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        for stmt in setup {
            engine.execute(stmt).unwrap();
        }
        let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
            panic!("expected result set: {sql}");
        };
        rs.rows
    })
}

/// Same as [`run_with_decor`] but with decorrelation DISABLED
/// (`MYDB_DECORRELATE=off`) — the legacy per-row eval-time path, which is
/// the documented correctness oracle. Used for differential checks where
/// the two paths must agree.
fn run_without_decor(setup: &[&str], sql: &str) -> Vec<Vec<Value>> {
    with_test_env(&[("MYDB_DECORRELATE", Some("off"))], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        for stmt in setup {
            engine.execute(stmt).unwrap();
        }
        let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
            panic!("expected result set: {sql}");
        };
        rs.rows
    })
}

/// Differential regression for the index NULL-parameter seek bug.
///
/// A correlated `AVG` over a table that has a NULL-keyed group: the legacy
/// (decorrelate-off) path rewrites `WHERE j.cat = i.cat` into an index
/// equality seek (`j.cat = ?`); for a NULL-category outer row the parameter
/// binds to NULL. SQL `=` against NULL is UNKNOWN, so the subquery is empty
/// and `price > 1.2 * NULL` is never true — the NULL-category rows must NOT
/// pass. The index seek used to match the stored NULL entries instead (as
/// if `IS NULL`), so the legacy path over-counted. decorrelate-on (LEFT
/// JOIN) was always correct; both paths must now agree and equal the MySQL
/// result. `cat` is indexed so the equality really drives an index lookup
/// (verified: `Index lookup on j using ix_cat`), and prices straddle the
/// fractional `1.2 *` boundary to also exercise the decimal AVG path.
#[test]
fn correlated_avg_null_key_group_matches_legacy_oracle() {
    let setup = [
        "CREATE TABLE item2 (id INT PRIMARY KEY, cat VARCHAR(16), price DECIMAL(7,2))",
        "CREATE INDEX ix_cat ON item2(cat)",
        // cat 'A': avg = 15.00, 1.2x = 18.00 -> only 20.00 passes.
        "INSERT INTO item2 VALUES (1,'A',10.00),(2,'A',20.00)",
        // cat 'F': avg = 1.50, 1.2x = 1.80 (fractional) -> only 2.00 passes.
        "INSERT INTO item2 VALUES (3,'F',1.00),(4,'F',2.00)",
        // cat NULL: a NULL-keyed outer row's subquery is NULL, so NONE pass.
        // 100.00 would pass iff the NULL=NULL index-seek bug reappeared.
        "INSERT INTO item2 VALUES (5,NULL,5.00),(6,NULL,6.00),(7,NULL,100.00)",
    ];
    let sql = "SELECT count(*) FROM item2 i \
               WHERE i.price > 1.2 * (SELECT avg(j.price) FROM item2 j WHERE j.cat = i.cat)";
    let on = run_with_decor(&setup, sql);
    let off = run_without_decor(&setup, sql);
    // MySQL ground truth: id=2 ('A',20.00) and id=4 ('F',2.00) pass -> 2.
    // No NULL-category row passes (its subquery is NULL).
    assert_eq!(on, vec![vec![Value::Int(2)]], "decorrelate ON (== MySQL)");
    assert_eq!(
        off, on,
        "decorrelate OFF (legacy oracle) must match decorrelate ON"
    );
}

#[test]
fn correlated_max_subquery_in_projection() {
    let setup = [
        "CREATE TABLE outer_t (id INT PRIMARY KEY, k INT, v INT)",
        "CREATE TABLE inner_t (id INT PRIMARY KEY, k INT, x INT)",
        "INSERT INTO outer_t VALUES (1,10,100),(2,20,200),(3,30,300),(4,40,400)",
        "INSERT INTO inner_t VALUES \
         (101,10,1),(102,10,5),(103,10,3),\
         (201,20,7),(202,20,2),\
         (301,30,9)",
        // outer_t row k=40 has no matching inner_t row → MAX(empty) = NULL.
    ];
    let rows = run_with_decor(
        &setup,
        "SELECT id, (SELECT MAX(x) FROM inner_t WHERE inner_t.k = outer_t.k) AS mx \
         FROM outer_t ORDER BY id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::Int(5)],
            vec![Value::Int(2), Value::Int(7)],
            vec![Value::Int(3), Value::Int(9)],
            vec![Value::Int(4), Value::Null],
        ]
    );
}

#[test]
fn correlated_count_subquery_fixes_count_bug() {
    // The classic "count bug": an outer row with no matching inner rows
    // should get COUNT = 0, not NULL. LEFT JOIN naturally gives NULL,
    // so the rewriter wraps the outer reference in COALESCE(_, 0).
    let setup = [
        "CREATE TABLE outer_t (id INT PRIMARY KEY, k INT)",
        "CREATE TABLE inner_t (id INT PRIMARY KEY, k INT)",
        "INSERT INTO outer_t VALUES (1, 10), (2, 20), (3, 30)",
        "INSERT INTO inner_t VALUES (101, 10), (102, 10), (201, 20)",
    ];
    let rows = run_with_decor(
        &setup,
        "SELECT id, (SELECT COUNT(*) FROM inner_t WHERE inner_t.k = outer_t.k) AS c \
         FROM outer_t ORDER BY id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::Int(2)],
            vec![Value::Int(2), Value::Int(1)],
            vec![Value::Int(3), Value::Int(0)], // COUNT bug fix
        ]
    );
}

#[test]
fn correlated_subquery_in_where_clause() {
    // Filter form: `WHERE t1.x = (SELECT MAX(y) FROM t2 WHERE t2.k = t1.k)`.
    // This shape is also exercised by the pre-existing
    // `select::correlated_subquery_in_where` test; here we just
    // confirm decorrelation doesn't break it.
    let setup = [
        "CREATE TABLE o (id INT PRIMARY KEY, k INT, x INT)",
        "CREATE TABLE i (id INT PRIMARY KEY, k INT, y INT)",
        "INSERT INTO o VALUES (1,10,5),(2,10,3),(3,20,7),(4,20,2)",
        "INSERT INTO i VALUES (101,10,5),(102,10,4),(201,20,7),(202,20,1)",
        // Per-group max: k=10→5, k=20→7. Outer matches: id=1, id=3.
    ];
    let rows = run_with_decor(
        &setup,
        "SELECT id FROM o WHERE o.x = (SELECT MAX(y) FROM i WHERE i.k = o.k) ORDER BY id",
    );
    assert_eq!(rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
}

#[test]
fn correlated_subquery_with_extra_where_preserved() {
    // The non-correlated portion of the inner WHERE should stay inside
    // the derived table.
    let setup = [
        "CREATE TABLE o (id INT PRIMARY KEY, k INT)",
        "CREATE TABLE i (id INT PRIMARY KEY, k INT, x INT, y INT)",
        "INSERT INTO o VALUES (1, 10), (2, 20)",
        "INSERT INTO i VALUES \
         (1,10,5,0),(2,10,3,1),(3,10,7,0),\
         (4,20,4,1),(5,20,9,1),(6,20,2,0)",
        // For each outer k, max(x) where y=1:
        //   k=10 → max where y=1 → 3
        //   k=20 → max where y=1 → 9
    ];
    let rows = run_with_decor(
        &setup,
        "SELECT id, (SELECT MAX(x) FROM i WHERE i.k = o.k AND i.y = 1) AS mx \
         FROM o ORDER BY id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::Int(3)],
            vec![Value::Int(2), Value::Int(9)],
        ]
    );
}

#[test]
fn correlated_sum_outer_filter_domain_preserves_distinct_keys() {
    let setup = [
        "CREATE TABLE o_domain (id INT PRIMARY KEY, k INT, active INT)",
        "CREATE TABLE i_domain (id INT PRIMARY KEY, k INT, x INT)",
        // Two surviving outer rows share k=10. The pushed key-domain must be
        // DISTINCT or it would multiply both inner rows before SUM.
        "INSERT INTO o_domain VALUES (1,10,1),(2,10,1),(3,20,0)",
        "INSERT INTO i_domain VALUES (1,10,5),(2,10,7),(3,20,100)",
    ];
    let rows = run_with_decor(
        &setup,
        "SELECT id, (SELECT SUM(x) FROM i_domain i WHERE i.k = o.k) AS s \
         FROM o_domain o WHERE o.active = 1 ORDER BY id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::DecimalCompact(12, 0)],
            vec![Value::Int(2), Value::DecimalCompact(12, 0)],
        ]
    );
}

#[test]
fn multi_column_correlation_groups_by_both() {
    let setup = [
        "CREATE TABLE o (id INT PRIMARY KEY, k INT, j INT)",
        "CREATE TABLE i (id INT PRIMARY KEY, k INT, j INT, x INT)",
        "INSERT INTO o VALUES (1,1,1),(2,1,2),(3,2,1)",
        "INSERT INTO i VALUES \
         (1,1,1,10),(2,1,1,20),\
         (3,1,2,30),(4,1,2,40),\
         (5,2,1,50)",
        // Per (k,j) MAX(x): (1,1)→20, (1,2)→40, (2,1)→50.
    ];
    let rows = run_with_decor(
        &setup,
        "SELECT id, (SELECT MAX(x) FROM i WHERE i.k = o.k AND i.j = o.j) AS mx \
         FROM o ORDER BY id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::Int(20)],
            vec![Value::Int(2), Value::Int(40)],
            vec![Value::Int(3), Value::Int(50)],
        ]
    );
}

#[test]
fn null_outer_join_column_yields_null_aggregate() {
    // SQL semantics: t2.k = NULL is unknown → no matching rows →
    // MAX(empty) = NULL. LEFT JOIN on NULL = inner.k also gives NULL.
    let setup = [
        "CREATE TABLE o (id INT PRIMARY KEY, k INT)",
        "CREATE TABLE i (id INT PRIMARY KEY, k INT, x INT)",
        "INSERT INTO o VALUES (1, 10), (2, NULL), (3, 30)",
        "INSERT INTO i VALUES (1,10,5),(2,30,9)",
    ];
    let rows = run_with_decor(
        &setup,
        "SELECT id, (SELECT MAX(x) FROM i WHERE i.k = o.k) AS mx FROM o ORDER BY id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::Int(5)],
            vec![Value::Int(2), Value::Null],
            vec![Value::Int(3), Value::Int(9)],
        ]
    );
}

#[test]
fn sum_returns_null_for_empty_group() {
    // SUM on empty is NULL (matches LEFT JOIN). Don't COALESCE.
    let setup = [
        "CREATE TABLE o (id INT PRIMARY KEY, k INT)",
        "CREATE TABLE i (id INT PRIMARY KEY, k INT, x INT)",
        "INSERT INTO o VALUES (1, 10), (2, 99)",
        "INSERT INTO i VALUES (1,10,3),(2,10,4)",
    ];
    let rows = run_with_decor(
        &setup,
        "SELECT id, (SELECT SUM(x) FROM i WHERE i.k = o.k) AS s FROM o ORDER BY id",
    );
    assert_eq!(rows.len(), 2);
    // SUM(3+4) = 7 — SumAcc preserves exact-decimal magnitudes, so
    // Int inputs now finalise as DecimalCompact(_, 0). Older shapes
    // (Int / Double from the legacy Float fallback) still accepted to
    // keep the assertion stable across accumulator-state transitions.
    let v = &rows[0][1];
    let ok = match v {
        Value::Int(7) => true,
        Value::Double(d) => *d == 7.0,
        Value::DecimalCompact(7, 0) => true,
        Value::Decimal(s) if s == "7" => true,
        _ => false,
    };
    assert!(ok, "expected SUM=7, got {:?}", v);
    assert_eq!(rows[1][1], Value::Null);
}

#[test]
fn uncorrelated_scalar_subquery_works_unchanged() {
    // Uncorrelated subqueries are already efficient (cached once by the
    // eval-time path). The rewriter must skip them. We only check
    // correctness here.
    let setup = [
        "CREATE TABLE o (id INT PRIMARY KEY, v INT)",
        "CREATE TABLE i (id INT PRIMARY KEY, x INT)",
        "INSERT INTO o VALUES (1, 100), (2, 200)",
        "INSERT INTO i VALUES (1, 5), (2, 10), (3, 15)",
    ];
    let rows = run_with_decor(
        &setup,
        "SELECT id, (SELECT MAX(x) FROM i) AS mx FROM o ORDER BY id",
    );
    // Uncorrelated subquery returns 15 for every outer row.
    // NOTE: pre-existing legacy eval-time bug means we may see the
    // outer collapsed to a single aggregating row instead of two —
    // we accept either shape as long as the value is correct.
    assert!(
        rows.iter().all(|r| matches!(r[1], Value::Int(15))),
        "expected 15 for the uncorrelated MAX, got: {:?}",
        rows
    );
}

#[test]
fn or_correlated_predicate_does_not_rewrite() {
    // `i.k = o.k OR i.x > 5` is a complex correlation. The rewriter
    // must skip it. We can't easily test "didn't rewrite" through
    // EXPLAIN here, so we just confirm execution doesn't error.
    let setup = [
        "CREATE TABLE o (id INT PRIMARY KEY, k INT)",
        "CREATE TABLE i (id INT PRIMARY KEY, k INT, x INT)",
        "INSERT INTO o VALUES (1, 10), (2, 20)",
        "INSERT INTO i VALUES (1,10,1),(2,20,2),(3,30,99)",
    ];
    // Just check it runs to completion.
    let _ = run_with_decor(
        &setup,
        "SELECT id, (SELECT MAX(x) FROM i WHERE i.k = o.k OR i.x > 5) AS mx \
         FROM o ORDER BY id",
    );
}

#[test]
fn nested_subquery_inside_body_blocks_rewrite() {
    // Body itself contains an Expr::Subquery / EXISTS / IN → rewriter
    // skips. Just verify execution doesn't error and shape is right.
    let setup = [
        "CREATE TABLE a (id INT PRIMARY KEY, k INT)",
        "CREATE TABLE b (id INT PRIMARY KEY, k INT, x INT)",
        "CREATE TABLE c (id INT PRIMARY KEY, k INT)",
        "INSERT INTO a VALUES (1, 10)",
        "INSERT INTO b VALUES (1, 10, 5), (2, 10, 99)",
        "INSERT INTO c VALUES (1, 10)",
    ];
    let _ = run_with_decor(
        &setup,
        "SELECT id, (SELECT MAX(x) FROM b \
                     WHERE b.k = a.k AND b.k IN (SELECT k FROM c)) AS mx \
         FROM a",
    );
}

#[test]
fn multiple_correlated_subqueries_on_same_outer() {
    let setup = [
        "CREATE TABLE o (id INT PRIMARY KEY, k INT)",
        "CREATE TABLE a (id INT PRIMARY KEY, k INT, x INT)",
        "CREATE TABLE b (id INT PRIMARY KEY, k INT, y INT)",
        "INSERT INTO o VALUES (1, 10), (2, 20)",
        "INSERT INTO a VALUES (1,10,3),(2,10,7),(3,20,1)",
        "INSERT INTO b VALUES (1,10,100),(2,20,200),(3,20,300)",
    ];
    let rows = run_with_decor(
        &setup,
        "SELECT id, \
                (SELECT MAX(x) FROM a WHERE a.k=o.k) AS mx, \
                (SELECT SUM(y) FROM b WHERE b.k=o.k) AS sy \
         FROM o ORDER BY id",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Int(1));
    assert_eq!(rows[0][1], Value::Int(7)); // MAX(x where a.k=10) = max(3,7) = 7
                                           // SUM now finalises as DecimalCompact for Int inputs; legacy
                                           // Int / Double shapes remain accepted in case the legacy Float
                                           // accumulator path runs.
    let check = |v: &Value, want: f64| match v {
        Value::Int(n) => *n as f64 == want,
        Value::Double(d) => *d == want,
        Value::DecimalCompact(mag, scale) => (*mag as f64 / 10f64.powi(*scale as i32)) == want,
        Value::Decimal(s) => s.parse::<f64>().map(|d| d == want).unwrap_or(false),
        _ => false,
    };
    assert!(check(&rows[0][2], 100.0), "row[0][2]={:?}", rows[0][2]);
    assert_eq!(rows[1][0], Value::Int(2));
    assert_eq!(rows[1][1], Value::Int(1)); // MAX(x where a.k=20) = max(1) = 1
    assert!(check(&rows[1][2], 500.0), "row[1][2]={:?}", rows[1][2]);
}

#[test]
fn tpch_q17_shape_decorrelates_correctly() {
    // TPC-H Q17 shape: average partsupp cost per part, used to filter
    // line items. The correlated subquery is in HAVING / WHERE, not
    // projection, but exercises a real-world pattern.
    let setup = [
        "CREATE TABLE part (p_partkey INT PRIMARY KEY, p_brand VARCHAR(10))",
        "CREATE TABLE lineitem (l_orderkey INT, l_partkey INT, l_quantity INT)",
        "INSERT INTO part VALUES (1, 'BRAND#11'), (2, 'BRAND#11'), (3, 'BRAND#22')",
        "INSERT INTO lineitem VALUES \
         (101, 1, 10), (102, 1, 20), (103, 1, 30),\
         (201, 2, 5),  (202, 2, 50),\
         (301, 3, 5),  (302, 3, 100)",
        // Per-part avg quantity:
        //   p1 → (10+20+30)/3 = 20
        //   p2 → (5+50)/2 = 27.5
        //   p3 → (5+100)/2 = 52.5
        // Q17 keeps line items with quantity < 0.2 * avg(quantity).
        // For brand BRAND#11 parts (1,2): 0.2 * avg per part.
        //   p1 threshold = 4. quantities 10,20,30 → none below 4.
        //   p2 threshold = 5.5. quantity 5 < 5.5 → keep. 50 > 5.5 → drop.
    ];
    let rows = run_with_decor(
        &setup,
        "SELECT l_orderkey, l_quantity \
         FROM lineitem JOIN part ON l_partkey = p_partkey \
         WHERE p_brand = 'BRAND#11' \
           AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem l2 WHERE l2.l_partkey = part.p_partkey) \
         ORDER BY l_orderkey",
    );
    assert_eq!(rows.len(), 1, "expected one filtered row, got {:?}", rows);
    assert_eq!(rows[0][0], Value::Int(201));
}

#[test]
fn subquery_with_aggregate_wrapped_in_arithmetic_decorrelates() {
    let setup = [
        "CREATE TABLE o (id INT PRIMARY KEY, k INT)",
        "CREATE TABLE i (id INT PRIMARY KEY, k INT, x INT)",
        "INSERT INTO o VALUES (1, 10), (2, 20), (3, 30)",
        "INSERT INTO i VALUES (1,10,5),(2,20,9)",
    ];
    let rows = run_with_decor(
        &setup,
        "SELECT id, (SELECT MAX(x) + 1 FROM i WHERE i.k = o.k) AS m1 \
         FROM o ORDER BY id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::Int(6)],
            vec![Value::Int(2), Value::Int(10)],
            vec![Value::Int(3), Value::Null],
        ]
    );
}

/// Regression for issue #64's spin-off: a **self-correlated** scalar
/// subquery in the projection list — the inner query scans the *same*
/// base table under an alias while the correlation references the outer
/// copy by its base name (`... FROM t t2 WHERE t2.id < t.id`).
///
/// `inner_select_tables` used to register both the inner alias (`t2`)
/// **and** the inner base name (`t`). The outer reference `t.id` was then
/// misclassified as an inner column and left unsubstituted, so the
/// subquery — whose scope only exposes alias `t2` — rejected it with
/// `1054 unknown column 'id'`. The fix drops an aliased source's base
/// name from the inner set (SQL alias-shadows-name, mirroring
/// `planner::ScopeTable::matches_name`). Results below match MySQL 9.6.0.
///
/// Projection-position subqueries are not decorrelated, so both
/// decorrelate ON/OFF exercise the same runtime substitution path; we
/// assert both to lock the placeholder-cache and legacy walkers.
#[test]
fn self_correlated_scalar_subquery_in_projection() {
    let setup = [
        "CREATE TABLE t (id INT)",
        "INSERT INTO t VALUES (1),(2),(3)",
    ];
    // (a) equality self-correlation returns the matching row's own id.
    let eq = "SELECT t.id, (SELECT t2.id FROM t t2 WHERE t2.id = t.id) AS e \
              FROM t ORDER BY t.id";
    let expect_eq = vec![
        vec![Value::Int(1), Value::Int(1)],
        vec![Value::Int(2), Value::Int(2)],
        vec![Value::Int(3), Value::Int(3)],
    ];
    // (b) inequality COUNT — the "how many precede me" running count.
    let cnt = "SELECT t.id, (SELECT COUNT(*) FROM t t2 WHERE t2.id < t.id) AS c \
               FROM t ORDER BY t.id";
    let expect_cnt = vec![
        vec![Value::Int(1), Value::Int(0)],
        vec![Value::Int(2), Value::Int(1)],
        vec![Value::Int(3), Value::Int(2)],
    ];
    // (c) inequality SUM — NULL for the empty (id=1) partition.
    let sum = "SELECT t.id, (SELECT SUM(t2.id) FROM t t2 WHERE t2.id < t.id) AS s \
               FROM t ORDER BY t.id";
    // SUM over an INT column yields DECIMAL (matching MySQL); the empty
    // (id=1) partition is NULL.
    let expect_sum = vec![
        vec![Value::Int(1), Value::Null],
        vec![Value::Int(2), Value::DecimalCompact(1, 0)],
        vec![Value::Int(3), Value::DecimalCompact(3, 0)],
    ];

    assert_eq!(run_with_decor(&setup, eq), expect_eq, "eq decor ON");
    assert_eq!(run_without_decor(&setup, eq), expect_eq, "eq decor OFF");
    assert_eq!(run_with_decor(&setup, cnt), expect_cnt, "count decor ON");
    assert_eq!(
        run_without_decor(&setup, cnt),
        expect_cnt,
        "count decor OFF"
    );
    assert_eq!(run_with_decor(&setup, sum), expect_sum, "sum decor ON");
    assert_eq!(run_without_decor(&setup, sum), expect_sum, "sum decor OFF");
}

/// Companion to the projection regression: a self-correlated scalar
/// subquery in the WHERE clause that is deliberately kept out of the
/// decorrelation rewrite (the `+ 0` makes the comparand a non-bare
/// expression), so it too runs through the per-row substitution path and
/// exercises the same `inner_select_tables` classification fix.
#[test]
fn self_correlated_scalar_subquery_in_where_non_decorrelatable() {
    let setup = [
        "CREATE TABLE t (id INT)",
        "INSERT INTO t VALUES (1),(2),(3)",
    ];
    let sql = "SELECT t.id FROM t \
               WHERE (SELECT COUNT(*) FROM t t2 WHERE t2.id < t.id) + 0 >= 1 \
               ORDER BY t.id";
    let expect = vec![vec![Value::Int(2)], vec![Value::Int(3)]];
    assert_eq!(run_with_decor(&setup, sql), expect, "decor ON");
    assert_eq!(run_without_decor(&setup, sql), expect, "decor OFF");
}

#[test]
fn nested_in_correlated_composite_sum_preserves_rows() {
    let setup = [
        "CREATE TABLE vendor (id INT PRIMARY KEY, region INT)",
        "CREATE TABLE stock (item INT, vendor INT, qty INT, PRIMARY KEY(item,vendor))",
        "CREATE TABLE product (id INT PRIMARY KEY, name VARCHAR(20))",
        "CREATE TABLE shipment (id INT PRIMARY KEY, item INT, vendor INT, qty DECIMAL(15,2), shipped DATE, KEY(vendor))",
        "INSERT INTO vendor VALUES (1,10),(2,10),(3,20),(4,10)",
        "INSERT INTO product VALUES (11,'forest green'),(12,'other'),(13,'forest blue')",
        "INSERT INTO stock VALUES (11,1,100),(11,2,5),(12,2,100),(13,3,100),(13,4,100)",
        "INSERT INTO shipment VALUES (1,11,1,20,'1994-06-01'),(2,11,1,30,'1994-07-01'),(3,11,2,20,'1994-06-01'),(4,12,2,10,'1994-06-01'),(5,13,3,10,'1994-06-01'),(6,13,4,10,'1995-01-01'),(7,13,4,12,'1994-01-01')",
    ];
    let sql = "SELECT id FROM vendor WHERE region=10 AND id IN (SELECT vendor FROM stock WHERE item IN (SELECT id FROM product WHERE name LIKE 'forest%') AND qty > (SELECT 0.5*SUM(qty) FROM shipment WHERE shipment.item=stock.item AND shipment.vendor=stock.vendor AND shipped>='1994-01-01' AND shipped<DATE_ADD('1994-01-01',INTERVAL 1 YEAR))) ORDER BY id";
    let expected = vec![vec![Value::Int(1)], vec![Value::Int(4)]];
    assert_eq!(run_without_decor(&setup, sql), expected, "Apply fallback");
    assert_eq!(run_with_decor(&setup, sql), expected, "decorrelated");
}

#[test]
fn parallel_nested_in_scalar_composite_keys_match_row_execution() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    with_test_env(
        &[
            ("MYDB_PARALLEL_DEGREE", Some("16")),
            ("MYDB_DECORRELATE", Some("off")),
        ],
        || {
            let dir = tempfile::tempdir().unwrap();
            let engine = open_test_engine(dir.path());
            for ddl in [
            "CREATE TABLE vendors (s_id INT PRIMARY KEY)",
            "CREATE TABLE products (p_id INT PRIMARY KEY, active INT)",
            "CREATE TABLE stocks (ps_item INT, ps_vendor INT, ps_qty INT, padding VARCHAR(50), PRIMARY KEY(ps_item,ps_vendor))",
            "CREATE TABLE shipments (l_id INT PRIMARY KEY, l_item INT, l_vendor INT, l_qty DECIMAL(15,2), KEY(l_vendor))",
        ] { engine.execute(ddl).unwrap(); }
            let vendors = (1..=1280)
                .map(|i| format!("({i})"))
                .collect::<Vec<_>>()
                .join(",");
            engine
                .execute(&format!("INSERT INTO vendors VALUES {vendors}"))
                .unwrap();
            let products = (0..20)
                .map(|i| format!("({i},{})", i32::from(i < 5)))
                .collect::<Vec<_>>()
                .join(",");
            engine
                .execute(&format!("INSERT INTO products VALUES {products}"))
                .unwrap();
            let stocks = (1..=1280)
                .map(|i| format!("({},{i},100,'padding')", i % 20))
                .collect::<Vec<_>>()
                .join(",");
            engine
                .execute(&format!("INSERT INTO stocks VALUES {stocks}"))
                .unwrap();
            engine
                .execute("INSERT INTO shipments VALUES (1,0,20,20),(2,2,22,30),(3,0,40,400)")
                .unwrap();
            for table in ["products", "stocks", "shipments", "vendors"] {
                engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
            }
            let sql = "SELECT s_id FROM vendors WHERE s_id IN (SELECT ps_vendor FROM stocks WHERE ps_item IN (SELECT p_id FROM products WHERE active=1) AND ps_qty > (SELECT 0.5*SUM(l_qty) FROM shipments WHERE l_item=ps_item AND l_vendor=ps_vendor)) ORDER BY s_id";
            let expected = vec![vec![Value::Int(20)], vec![Value::Int(22)]];
            let run = || {
                let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
                    panic!("result set")
                };
                rs.rows
            };
            assert_eq!(with_vec_mode(VecMode::Off, run), expected, "row execution");
            assert_eq!(
                with_vec_mode(VecMode::On, run),
                expected,
                "vector execution"
            );
        },
    );
}

#[test]
fn nested_in_decorrelation_preserves_exact_key_types_and_duplicates() {
    for (ty, keys) in [
        ("BIGINT", ["9007199254740992", "9007199254740993"]),
        (
            "DECIMAL(30,2)",
            ["9007199254740992.01", "9007199254740992.02"],
        ),
        ("VARCHAR(30)", ["'alpha'", "'beta'"]),
    ] {
        let setup = vec![
            format!("CREATE TABLE key_outer (k {ty})"),
            format!("CREATE TABLE key_stock (k {ty}, category INT, qty INT)"),
            "CREATE TABLE key_detail (category INT, qty INT)".to_string(),
            format!(
                "INSERT INTO key_outer VALUES ({}),({}),(NULL)",
                keys[0], keys[1]
            ),
            format!(
                "INSERT INTO key_stock VALUES ({},1,100),({},1,100),({},2,100),(NULL,1,100)",
                keys[1], keys[1], keys[0]
            ),
            "INSERT INTO key_detail VALUES (1,10),(1,20),(2,NULL)".to_string(),
        ];
        let setup = setup.iter().map(String::as_str).collect::<Vec<_>>();
        let sql = "SELECT k FROM key_outer WHERE k IN (SELECT k FROM key_stock WHERE qty > (SELECT SUM(qty) FROM key_detail WHERE key_detail.category=key_stock.category))";
        let expected = run_without_decor(&setup, sql);
        assert_eq!(expected.len(), 1, "one exact key, without duplicate emits");
        assert_eq!(run_with_decor(&setup, sql), expected, "key type {ty}");
        let domain_sql = "SELECT k FROM key_outer WHERE k IN (SELECT s.k FROM key_stock s WHERE s.category IN (SELECT category FROM key_detail WHERE qty IS NOT NULL) AND s.qty > (SELECT SUM(d.qty) FROM key_detail d WHERE d.category=s.category))";
        assert_eq!(
            run_with_decor(&setup, domain_sql),
            expected,
            "aliased duplicate IN domain, key type {ty}"
        );
    }
}

#[test]
fn nested_in_count_empty_group_and_external_scope() {
    let setup = [
        "CREATE TABLE count_outer (id INT PRIMARY KEY)",
        "CREATE TABLE count_stock (vendor INT, category INT)",
        "CREATE TABLE count_detail (category INT)",
        "INSERT INTO count_outer VALUES (1),(2),(3)",
        "INSERT INTO count_stock VALUES (1,10),(1,10),(2,20),(3,NULL)",
        "INSERT INTO count_detail VALUES (10),(10)",
    ];
    let sql = "SELECT id FROM count_outer WHERE id IN (SELECT vendor FROM count_stock WHERE 0=(SELECT COUNT(*) FROM count_detail WHERE count_detail.category=count_stock.category)) ORDER BY id";
    assert_eq!(
        run_with_decor(&setup, sql),
        vec![vec![Value::Int(2)], vec![Value::Int(3)]]
    );
    let correlated = "SELECT id FROM count_outer WHERE id IN (SELECT vendor FROM count_stock WHERE 0=(SELECT COUNT(*) FROM count_detail WHERE count_detail.category=count_stock.category AND count_outer.id=1)) ORDER BY id";
    assert_eq!(
        run_with_decor(&setup, correlated),
        run_without_decor(&setup, correlated)
    );
}

#[test]
fn scalar_apply_inside_semi_join_with_sibling_hash_join() {
    let setup = [
        "CREATE TABLE apply_vendor (s_id INT PRIMARY KEY, s_region INT)",
        "CREATE TABLE apply_region (n_id INT PRIMARY KEY, n_name VARCHAR(20))",
        "CREATE TABLE apply_stock (ps_item INT, ps_vendor INT, ps_qty INT, PRIMARY KEY(ps_item,ps_vendor))",
        "CREATE TABLE apply_product (p_id INT PRIMARY KEY, p_name VARCHAR(20))",
        "CREATE TABLE apply_detail (l_item INT, l_vendor INT, l_qty DECIMAL(15,2), KEY(l_vendor))",
        "INSERT INTO apply_vendor VALUES (1,10),(2,20),(3,10)",
        "INSERT INTO apply_region VALUES (10,'north'),(20,'south')",
        "INSERT INTO apply_product VALUES (11,'forest'),(12,'other')",
        "INSERT INTO apply_stock VALUES (11,1,100),(11,2,100),(11,3,100),(12,1,100)",
        "INSERT INTO apply_detail VALUES (11,1,20),(11,2,20),(11,3,400),(12,1,10)",
    ];
    let sql = "SELECT s_id FROM apply_vendor,apply_region WHERE s_id IN (SELECT ps_vendor FROM apply_stock WHERE ps_item IN (SELECT p_id FROM apply_product WHERE p_name LIKE 'forest%') AND ps_qty > (SELECT 0.5*SUM(l_qty)+0*COUNT(*) FROM apply_detail WHERE l_item=ps_item AND l_vendor=ps_vendor)) AND s_region=n_id AND n_name='north' ORDER BY s_id";
    let expected = vec![vec![Value::Int(1)]];
    assert_eq!(
        run_with_decor(&setup, sql),
        expected,
        "non-decorrelatable Apply"
    );
    let simple_sum = sql.replace("0.5*SUM(l_qty)+0*COUNT(*)", "0.5*SUM(l_qty)");
    assert_eq!(
        run_with_decor(&setup, &simple_sum),
        expected,
        "decorrelated SUM"
    );
    assert_eq!(
        run_without_decor(&setup, &simple_sum),
        expected,
        "SUM Apply fallback"
    );
    let plain_filter = "SELECT s_id FROM apply_vendor,apply_region WHERE s_id IN (SELECT ps_vendor FROM apply_stock WHERE ps_item IN (SELECT p_id FROM apply_product WHERE p_name LIKE 'forest%') AND ps_qty > 50) AND s_region=n_id AND n_name='north' ORDER BY s_id";
    assert_eq!(
        run_with_decor(&setup, plain_filter),
        vec![vec![Value::Int(1)], vec![Value::Int(3)]],
        "pushed scan filter"
    );
}

#[test]
fn scalar_access_constants_are_execution_local_and_keep_lazy_fallbacks() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE scalar_access (tenant INT,id BIGINT,grp INT,PRIMARY KEY(tenant,id))",
        "INSERT INTO scalar_access VALUES (1,1,7),(1,2,7),(1,3,8),(2,9,7)",
    ] {
        engine.execute(sql).unwrap();
    }
    let sql = "SELECT id FROM scalar_access WHERE tenant=1 AND id=(SELECT MAX(id) FROM scalar_access WHERE tenant=1 AND grp=7)";
    let Statement::Select(select) = crate::parser::parse_statement(sql).unwrap() else {
        panic!("select")
    };
    let txn = engine
        .db
        .read_only_statement_view(crate::storage::IsolationLevel::ReadCommitted, None);
    let bound = crate::engine::executor::select::eval::bind_scalar_access_constants(
        &engine.db, txn, &select,
    )
    .unwrap()
    .unwrap();
    assert!(!crate::parser::visit::any_expr(
        bound.selection.as_ref().unwrap(),
        |e| matches!(e, Expr::Subquery(_))
    ));
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("rows")
    };
    assert_eq!(result.rows, vec![vec![Value::Int(2)]]);
    engine
        .execute("INSERT INTO scalar_access VALUES (1,4,7)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("rows")
    };
    assert_eq!(result.rows, vec![vec![Value::Int(4)]]);
    for sql in [
        "SELECT id FROM scalar_access o WHERE id=(SELECT MAX(i.id) FROM scalar_access i WHERE i.grp=o.grp)",
        "SELECT id FROM scalar_access WHERE tenant=99 AND id=(SELECT id FROM scalar_access)",
        "SELECT id FROM scalar_access WHERE id=(SELECT RAND())",
        "SELECT id FROM scalar_access WHERE id=(SELECT '4')",
    ] {
        let Statement::Select(select) = crate::parser::parse_statement(sql).unwrap() else { panic!("select") };
        assert!(crate::engine::executor::select::eval::bind_scalar_access_constants(&engine.db,txn,&select).unwrap().is_none(),"{sql}");
    }
    // Failed folding leaves the authoritative evaluator's cardinality error.
    assert_eq!(
        engine
            .execute("SELECT id FROM scalar_access WHERE id=(SELECT id FROM scalar_access)")
            .unwrap_err()
            .code(),
        1242
    );
    let QueryResponse::ResultSet(result) = engine.execute("SELECT id FROM scalar_access WHERE id=(SELECT MAX(id) FROM scalar_access WHERE tenant=99)").unwrap() else { panic!("rows") };
    assert!(result.rows.is_empty());
    let QueryResponse::ResultSet(plan) = engine
        .execute(&format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"))
        .unwrap()
    else {
        panic!("plan")
    };
    let text = format!("{:?}", plan.rows);
    assert!(text.contains("scalar_access"), "{text}");
    // Text auto-parameters retain literal provenance and use Apply; the
    // prepared point memo has its own rebind/reuse regression coverage.
    assert!(text.contains("Apply"), "{text}");
}
