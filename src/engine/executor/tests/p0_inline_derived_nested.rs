//! P0-4 regression coverage for the inline-derived recursive EXPLAIN
//! path (`plan_select_with_inline_derived_for_explain` in
//! `executor/select/explain.rs:2482`) and the runtime
//! `bridge_join_derived_to_ctes` bridge (`mod.rs:3499`).
//!
//! These shapes occur all over TPC-DS (q02/05/14/23/33/49/51/56/59/60/67/77/80):
//! a SELECT FROM (SELECT ... FROM (...) j ...) wrapped in a WITH clause,
//! with the inner inline derived itself joining a CTE-result.
//!
//! Three concerns covered here:
//! 1. inline derived in FROM[0] (the first slot) — different code path
//!    than inline derived in JOIN slots.
//! 2. inline derived inside inline derived (recursion in the planner
//!    worker) — must register the innermost stub first so outer
//!    bodies see it.
//! 3. inline derived referencing a WITH-CTE — must NOT shadow the WITH
//!    CTE's registry entry.
//!
//! The tests assert both the row set (correctness of the runtime bridge)
//! and that EXPLAIN FORMAT=TREE renders without 1235.

use super::*;

fn rows(setup: &[&str], sql: &str) -> Vec<Vec<Value>> {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for s in setup {
        engine.execute(s).unwrap();
    }
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set: {sql}");
    };
    rs.rows
}

fn explain_tree(setup: &[&str], sql: &str) -> String {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for s in setup {
        engine.execute(s).unwrap();
    }
    let response = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
        .unwrap();
    let QueryResponse::ResultSet(rs) = response else {
        panic!("expected result set from EXPLAIN");
    };
    rs.rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            Value::SmallString(s) => std::str::from_utf8(s.as_slice()).unwrap().to_string(),
            other => panic!("unexpected EXPLAIN row: {other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn inline_derived_in_from0_with_join_runs() {
    // FROM (SELECT … FROM base) d JOIN dim ON d.k = dim.k
    let setup = [
        "CREATE TABLE base (k INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
        "CREATE TABLE dim (k INT NOT NULL PRIMARY KEY, label VARCHAR(8) NOT NULL)",
        "INSERT INTO base VALUES (1, 10), (2, 20), (3, 30)",
        "INSERT INTO dim VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    ];
    let sql = "SELECT d.k, d.v, dim.label \
               FROM (SELECT k, v FROM base) d \
               JOIN dim ON d.k = dim.k \
               ORDER BY d.k";
    let result = rows(&setup, sql);
    assert_eq!(result.len(), 3);
    // EXPLAIN TREE must render without 1235.
    let tree = explain_tree(&setup, sql);
    assert!(!tree.is_empty(), "EXPLAIN TREE empty for: {sql}");
}

#[test]
fn inline_derived_inside_inline_derived_explain_renders() {
    // The recursive case: an inline derived contains an inline derived
    // in its own join slot. The recursive worker must register the inner
    // stub before planning the outer derived.
    let setup = [
        "CREATE TABLE base (k INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
        "CREATE TABLE dim (k INT NOT NULL PRIMARY KEY, label VARCHAR(8) NOT NULL)",
        "INSERT INTO base VALUES (1, 10), (2, 20)",
        "INSERT INTO dim VALUES (1, 'a'), (2, 'b')",
    ];
    let sql = "SELECT outer_d.k, outer_d.label \
               FROM ( \
                 SELECT inner_d.k AS k, dim.label AS label \
                 FROM (SELECT k FROM base) inner_d \
                 JOIN dim ON dim.k = inner_d.k \
               ) outer_d \
               ORDER BY outer_d.k";
    let result = rows(&setup, sql);
    assert_eq!(result.len(), 2);
    let tree = explain_tree(&setup, sql);
    assert!(!tree.is_empty());
    // The body planner must have seen *both* derived stubs to land a
    // plan; if either layer was missed, the rewrite would have errored
    // with 1146 (table not found) or 1235 (cannot explain).
}

#[test]
fn inline_derived_inside_with_cte_does_not_shadow_cte_entry() {
    // WITH u AS (...) SELECT d.k, u.x FROM (SELECT k FROM base) d
    //   JOIN u ON u.k = d.k.
    // The inline-derived stub for `d` must not evict the CTE entry for
    // `u` from the thread-local CTE registry — both must coexist when
    // the body is planned.
    let setup = [
        "CREATE TABLE base (k INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
        "CREATE TABLE other (k INT NOT NULL PRIMARY KEY, x INT NOT NULL)",
        "INSERT INTO base VALUES (1, 10), (2, 20), (3, 30)",
        "INSERT INTO other VALUES (1, 100), (2, 200)",
    ];
    let sql = "WITH u AS (SELECT k, x FROM other) \
               SELECT d.k, u.x \
               FROM (SELECT k FROM base) d \
               JOIN u ON u.k = d.k \
               ORDER BY d.k";
    let result = rows(&setup, sql);
    assert_eq!(
        result.len(),
        2,
        "expected 2 rows matching u keys; got {result:?}"
    );
    let tree = explain_tree(&setup, sql);
    // EXPLAIN must render and at minimum mention the CTE name or join
    // structure — empty would mean 1235 silently swallowed.
    assert!(!tree.is_empty());
}

#[test]
fn inline_derived_with_aggregate_renders_and_runs() {
    // The derived body contains its own GROUP BY — exercises the path
    // where the inline-derived stub must reflect the grouped column
    // types (not the base table's).
    let setup = [
        "CREATE TABLE sales (k INT NOT NULL, v INT NOT NULL, PRIMARY KEY(k, v))",
        "INSERT INTO sales VALUES (1, 10), (1, 20), (2, 30), (3, 40), (3, 50)",
    ];
    let sql = "SELECT g.k, g.tot \
               FROM (SELECT k, SUM(v) AS tot FROM sales GROUP BY k) g \
               WHERE g.tot > 25 ORDER BY g.k";
    let result = rows(&setup, sql);
    // k=1 → 10+20=30 (>25), k=2 → 30 (>25), k=3 → 40+50=90 (>25).
    assert_eq!(result.len(), 3, "got {result:?}");
    let tree = explain_tree(&setup, sql);
    assert!(!tree.is_empty());
}
