//! Regression tests for the TPC-H Q20 "unknown column 'ps_partkey'"
//! mis-plan. The original bug had two cooperating root causes:
//!
//! 1. `build_in_subquery_semi_join` lifted the IN-subquery's WHERE
//!    out of the synthesised SEMI join's condition and into the outer
//!    SELECT's WHERE. A second `normalize_select_predicates` pass run
//!    by `plan_select` then re-applied the `IN → SemiJoin` rewrite to
//!    a predicate whose outer-side column had already been stripped
//!    by the first Semi (so the new Semi's condition referenced a
//!    column that no longer existed at execution time).
//!
//! 2. `collect_expr_columns` / `collect_expr_columns_by_table` stopped
//!    at `Expr::Subquery` / `Expr::Exists` boundaries, so column
//!    pruning would drop a base column that the subquery body needed
//!    as a correlated outer reference — e.g. `ps_partkey` referenced
//!    inside `(SELECT SUM(l_quantity) FROM lineitem WHERE l_partkey
//!    = ps_partkey)` got pruned from the partsupp scan even though
//!    the eval-time correlated rewrite needed it.
use super::*;

fn engine_with_data() -> (tempfile::TempDir, StorageQueryEngine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let setup = [
        "CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_name VARCHAR(25))",
        "CREATE TABLE supplier (s_suppkey INT PRIMARY KEY, s_name VARCHAR(25), \
            s_address VARCHAR(40), s_nationkey INT)",
        "CREATE TABLE partsupp (ps_partkey INT, ps_suppkey INT, ps_availqty INT, \
            PRIMARY KEY (ps_partkey, ps_suppkey))",
        "CREATE TABLE part (p_partkey INT PRIMARY KEY, p_name VARCHAR(55))",
        "CREATE TABLE lineitem (l_orderkey INT, l_linenumber INT, l_partkey INT, \
            l_suppkey INT, l_quantity DECIMAL(15,2), l_shipdate DATE, \
            PRIMARY KEY (l_orderkey, l_linenumber))",
        "INSERT INTO nation VALUES (24, 'CANADA')",
        "INSERT INTO supplier VALUES (1, 'S1', 'addr1', 24)",
        "INSERT INTO part VALUES (1, 'forest blue plum'), (2, 'other')",
        "INSERT INTO partsupp VALUES (1, 1, 100), (2, 1, 50)",
        "INSERT INTO lineitem VALUES (1, 1, 1, 1, 10.0, '1994-03-01')",
    ];
    for stmt in setup {
        engine.execute(stmt).expect(stmt);
    }
    (dir, engine)
}

fn with_decorrelate_enabled<R>(f: impl FnOnce() -> R) -> R {
    with_test_env(&[("MYDB_DECORRELATE", None)], f)
}

#[test]
fn q20_does_not_error() {
    with_decorrelate_enabled(|| {
        let (_dir, engine) = engine_with_data();
        let sql = "SELECT s_name, s_address
FROM supplier INNER JOIN nation ON s_nationkey = n_nationkey
WHERE s_suppkey IN (
    SELECT ps_suppkey FROM partsupp
    WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
      AND ps_availqty > (
          SELECT 0.5 * SUM(l_quantity) FROM lineitem
          WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
            AND l_shipdate >= '1994-01-01'
            AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
      )
) AND n_name = 'CANADA'
ORDER BY s_name";
        engine.execute(sql).expect("Q20 should plan and execute");
    });
}

#[test]
fn nested_in_subquery_with_nested_in() {
    // Outer IN's body contains another uncorrelated IN. Pre-fix this
    // mis-rewrote to a SemiJoin chain that referenced partsupp after
    // the first Semi had stripped it.
    with_decorrelate_enabled(|| {
        let (_dir, engine) = engine_with_data();
        let sql = "SELECT s_name FROM supplier INNER JOIN nation ON s_nationkey = n_nationkey
WHERE s_suppkey IN (
    SELECT ps_suppkey FROM partsupp
    WHERE ps_partkey IN (SELECT p_partkey FROM part)
)";
        engine
            .execute(sql)
            .expect("nested IN inside IN-subquery WHERE");
    });
}

#[test]
fn nested_in_subquery_with_literal_filter_in_inner_where() {
    // Sanity check: the simpler shape that already worked before the
    // fix still works.
    with_decorrelate_enabled(|| {
        let (_dir, engine) = engine_with_data();
        let sql = "SELECT s_name FROM supplier INNER JOIN nation ON s_nationkey = n_nationkey
WHERE s_suppkey IN (
    SELECT ps_suppkey FROM partsupp
    WHERE ps_availqty > 0
)";
        engine.execute(sql).expect("literal-only inner WHERE works");
    });
}

#[test]
fn nested_in_subquery_with_correlated_subquery_in_where() {
    // Inner WHERE has a correlated scalar subquery that references
    // partsupp columns. Pre-fix the partsupp scan would prune
    // `ps_partkey` because `collect_expr_columns` didn't peek inside
    // the scalar subquery's body.
    with_decorrelate_enabled(|| {
        let (_dir, engine) = engine_with_data();
        let sql = "SELECT s_name FROM supplier INNER JOIN nation ON s_nationkey = n_nationkey
WHERE s_suppkey IN (
    SELECT ps_suppkey FROM partsupp
    WHERE ps_availqty > (SELECT SUM(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey)
)";
        engine
            .execute(sql)
            .expect("correlated-scalar inside IN works");
    });
}
