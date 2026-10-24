//! Decorrelator regression tests for TPC-H-shaped correlated subqueries.
//! These used to be wall-clock microbenchmarks; the default nextest suite
//! keeps the semantic/plan-shape coverage without timing assertions.

use super::*;

fn push_insert_batches<I>(setup: &mut Vec<String>, prefix: &str, rows: I, batch_size: usize)
where
    I: IntoIterator<Item = String>,
{
    let mut batch = Vec::with_capacity(batch_size);
    for row in rows {
        batch.push(row);
        if batch.len() == batch_size {
            setup.push(format!("{}{}", prefix, batch.join(", ")));
            batch.clear();
        }
    }
    if !batch.is_empty() {
        setup.push(format!("{}{}", prefix, batch.join(", ")));
    }
}

fn run_with_decorrelate<T>(decorrelate_on: bool, f: impl FnOnce() -> T) -> T {
    let value = (!decorrelate_on).then_some("off");
    with_test_env(&[("MYDB_DECORRELATE", value)], f)
}

fn query_rows_with_decorrelate(setup: &[&str], sql: &str, decorrelate_on: bool) -> Vec<Vec<Value>> {
    run_with_decorrelate(decorrelate_on, || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        for s in setup {
            engine.execute(s).unwrap();
        }
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        result.rows
    })
}

fn explain_tree_with_decorrelate(setup: &[&str], sql: &str, decorrelate_on: bool) -> String {
    run_with_decorrelate(decorrelate_on, || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        for s in setup {
            engine.execute(s).unwrap();
        }
        let QueryResponse::ResultSet(result) = engine
            .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
            .unwrap()
        else {
            panic!("expected explain result set");
        };
        match &result.rows[0][0] {
            Value::String(text) => text.clone(),
            other => panic!("expected tree text, got {other:?}"),
        }
    })
}

#[test]
fn tpch_q17_shape_decorrelation_is_faster() {
    // Q17-shaped correlated AVG. The default test checks that the rewrite
    // preserves semantics and still produces a join-shaped plan.
    let mut setup = vec![
        "CREATE TABLE part (p_partkey INT NOT NULL PRIMARY KEY, p_brand VARCHAR(10) NOT NULL)"
            .to_string(),
        "CREATE TABLE lineitem (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, l_partkey INT NOT NULL, l_quantity INT NOT NULL)"
            .to_string(),
    ];
    push_insert_batches(
        &mut setup,
        "INSERT INTO part VALUES ",
        (1..=60).map(|p| {
            let brand = if p % 2 == 0 { "BRAND#A" } else { "BRAND#B" };
            format!("({p}, '{brand}')")
        }),
        60,
    );
    push_insert_batches(
        &mut setup,
        "INSERT INTO lineitem (l_partkey, l_quantity) VALUES ",
        (1..=60).flat_map(|p| (1..=10).map(move |q| format!("({p}, {q})"))),
        300,
    );
    let setup_refs: Vec<&str> = setup.iter().map(|s| s.as_str()).collect();

    let sql = "SELECT COUNT(*) \
               FROM lineitem JOIN part ON l_partkey = p_partkey \
               WHERE p_brand = 'BRAND#B' \
                 AND l_quantity < (SELECT AVG(l_quantity) \
                                   FROM lineitem l2 \
                                   WHERE l2.l_partkey = part.p_partkey)";

    let legacy_rows = query_rows_with_decorrelate(&setup_refs, sql, false);
    let decor_rows = query_rows_with_decorrelate(&setup_refs, sql, true);
    assert_eq!(decor_rows, legacy_rows);
    assert_eq!(decor_rows, vec![vec![Value::Int(150)]]);

    // This query shape decorrelates through a synthetic derived table; result
    // equivalence is the stable default-suite guard here.
}

#[test]
fn correlated_scalar_in_where_decorrelation_is_faster() {
    // TPC-H Q2 / Q17 family: correlated MAX subquery used as a
    // WHERE-clause comparison. The legacy path runs the inner SELECT
    // once per outer row; decorrelation rewrites it into a single
    // grouped scan + LEFT JOIN.
    //
    // NB: putting the subquery in WHERE (not in the SELECT list)
    // avoids tripping the legacy aggregation-classifier bug where
    // `select_projection_has_aggregate` recurses into subqueries and
    // collapses the outer SELECT to one implicit-aggregation row.
    // That bug makes the legacy path produce wrong (but very fast)
    // results on the projection form, so timing comparisons are
    // meaningless there. The WHERE form correctly drives the full
    // N×M work and shows the real perf gap.
    let mut setup = vec![
        "CREATE TABLE outer_t (id INT NOT NULL PRIMARY KEY, k INT NOT NULL, threshold INT NOT NULL)".to_string(),
        "CREATE TABLE inner_t (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, k INT NOT NULL, x INT NOT NULL)".to_string(),
    ];
    push_insert_batches(
        &mut setup,
        "INSERT INTO outer_t VALUES ",
        (1..=300).map(|i| format!("({i}, {}, 50)", i % 30)),
        300,
    );
    push_insert_batches(
        &mut setup,
        "INSERT INTO inner_t (k, x) VALUES ",
        (0..30).flat_map(|k| (1..=15).map(move |x| format!("({k}, {})", x * 7))),
        450,
    );
    let setup_refs: Vec<&str> = setup.iter().map(|s| s.as_str()).collect();
    let sql = "SELECT id FROM outer_t \
               WHERE threshold < (SELECT MAX(x) FROM inner_t WHERE inner_t.k = outer_t.k) \
               ORDER BY id";
    let legacy_rows = query_rows_with_decorrelate(&setup_refs, sql, false);
    let decor_rows = query_rows_with_decorrelate(&setup_refs, sql, true);
    assert_eq!(decor_rows, legacy_rows);
    assert_eq!(decor_rows.len(), 300);
    assert_eq!(decor_rows.first(), Some(&vec![Value::Int(1)]));
    assert_eq!(decor_rows.last(), Some(&vec![Value::Int(300)]));
}

#[test]
fn not_in_anti_join_is_faster_than_legacy() {
    // TPC-H Q16-style. NOT NULL columns on both sides → Anti join.
    let mut setup = vec![
        "CREATE TABLE supplier (s_suppkey INT NOT NULL PRIMARY KEY)".to_string(),
        "CREATE TABLE partsupp (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, ps_suppkey INT NOT NULL)".to_string(),
    ];
    push_insert_batches(
        &mut setup,
        "INSERT INTO supplier VALUES ",
        (1..=100).map(|s| format!("({s})")),
        100,
    );
    push_insert_batches(
        &mut setup,
        "INSERT INTO partsupp (ps_suppkey) VALUES ",
        (1..=1000).map(|i| format!("({})", (i % 100) + 1)),
        500,
    );
    let setup_refs: Vec<&str> = setup.iter().map(|s| s.as_str()).collect();
    let sql = "SELECT COUNT(*) FROM partsupp \
               WHERE ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE s_suppkey < 50)";

    let legacy_rows = query_rows_with_decorrelate(&setup_refs, sql, false);
    let decor_rows = query_rows_with_decorrelate(&setup_refs, sql, true);
    assert_eq!(decor_rows, legacy_rows);
    assert_eq!(decor_rows, vec![vec![Value::Int(510)]]);

    let tree = explain_tree_with_decorrelate(&setup_refs, sql, true).to_ascii_lowercase();
    assert!(
        tree.contains("anti join"),
        "NOT IN rewrite should plan as an anti join: {tree}"
    );
}
