//! GROUP BY hidden-column regression with empty sql_mode and ONLY_FULL_GROUP_BY disabled.
//! Non-aggregate columns absent from GROUP BY are legal in projection, HAVING, and ORDER BY,
//! using a value from one row in the group (the first row in this implementation).
//!
//! Two previous defects:
//! 1. Fused row aggregation (including fused seqscan) filled nongrouped columns with NULL,
//!    making hidden values NULL and hidden-column HAVING predicates false.
//! 2. Vectorized aggregate output had only grouping columns, so hidden projections failed with 1054.
//!
//! The planner now adds an internal aggregate slot for each hidden-column reference.
//! `__hidden_first_value(col)`(`planner::collect_aggregate_calls`),
//! All aggregate paths collect first-row values in ordinary slots; output column evaluation
//! looks up the aggregate cache by slot fingerprint when a field is absent or a NULL placeholder
//! (eval::hidden_group_cache_value). Vector aggregation does not recognize this function name
//! and safely falls back to row execution.

use super::*;
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

fn rows_of(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(r) => r.rows,
        QueryResponse::StreamingResultSet(mut r) => {
            let mut out = Vec::new();
            while let Ok(Some(row)) = r.cursor.next_row() {
                out.push(row);
            }
            out
        }
        other => panic!("expected result set, got {other:?}"),
    }
}

fn setup_p(engine: &StorageQueryEngine) {
    execute_permissive(&engine, "CREATE TABLE p (a INT PRIMARY KEY, b INT)").unwrap();
    execute_permissive(&engine, "INSERT INTO p VALUES (1, 10), (2, 10)").unwrap();
}

/// Assert identical results through row and vector routes.
fn assert_both(engine: &StorageQueryEngine, sql: &str, expected: &[Vec<Value>]) {
    let legacy = with_vec_mode(VecMode::Off, || {
        rows_of(execute_permissive(&engine, sql).unwrap())
    });
    assert_eq!(legacy, expected, "row path: {sql}");
    let vec_rows = with_vec_mode(VecMode::On, || {
        rows_of(execute_permissive(&engine, sql).unwrap())
    });
    assert_eq!(vec_rows, expected, "vec path: {sql}");
}

/// Original user reproduction: project the hidden column directly.
#[test]
fn hidden_column_projection_returns_group_row_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_p(&engine);
    assert_both(
        &engine,
        "SELECT b, COUNT(*) FROM p GROUP BY a ORDER BY a",
        &[
            vec![Value::Int(10), Value::Int(1)],
            vec![Value::Int(10), Value::Int(1)],
        ],
    );
    // Qualified references also use the cache lookup.
    assert_both(
        &engine,
        "SELECT p.b, COUNT(*) FROM p GROUP BY a ORDER BY a",
        &[
            vec![Value::Int(10), Value::Int(1)],
            vec![Value::Int(10), Value::Int(1)],
        ],
    );
}

/// Hidden columns nested in arithmetic recurse through eval; scalar function wrappers
/// descend through eval's other arm via eval_plan_expr to column references.
#[test]
fn hidden_column_inside_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_p(&engine);
    assert_both(
        &engine,
        "SELECT b + 1, COUNT(*) FROM p GROUP BY a ORDER BY a",
        &[
            vec![Value::Int(11), Value::Int(1)],
            vec![Value::Int(11), Value::Int(1)],
        ],
    );
    assert_both(
        &engine,
        "SELECT ABS(b), COUNT(*) FROM p GROUP BY a ORDER BY a",
        &[
            vec![Value::Int(10), Value::Int(1)],
            vec![Value::Int(10), Value::Int(1)],
        ],
    );
}

/// HAVING references a hidden column; the old fused path returned NULL and removed every group.
#[test]
fn hidden_column_in_having() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_p(&engine);
    assert_both(
        &engine,
        "SELECT a, COUNT(*) FROM p GROUP BY a HAVING b > 5 ORDER BY a",
        &[
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(1)],
        ],
    );
    assert_both(
        &engine,
        "SELECT a, COUNT(*) FROM p GROUP BY a HAVING b > 99",
        &[],
    );
}

/// ORDER BY hidden b values differ between groups, requiring deterministic order.
#[test]
fn hidden_column_in_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    execute_permissive(&engine, "CREATE TABLE o (a INT PRIMARY KEY, b INT)").unwrap();
    execute_permissive(&engine, "INSERT INTO o VALUES (1, 30), (2, 10), (3, 20)").unwrap();
    assert_both(
        &engine,
        "SELECT a, COUNT(*) FROM o GROUP BY a ORDER BY b",
        &[
            vec![Value::Int(2), Value::Int(1)],
            vec![Value::Int(3), Value::Int(1)],
            vec![Value::Int(1), Value::Int(1)],
        ],
    );
}

/// An implicit single group without GROUP BY also uses the first row's value.
#[test]
fn hidden_column_with_implicit_single_group() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_p(&engine);
    assert_both(
        &engine,
        "SELECT b, COUNT(*) FROM p",
        &[vec![Value::Int(10), Value::Int(2)]],
    );
    // Empty table: hidden columns are NULL and COUNT is 0, matching MySQL.
    execute_permissive(&engine, "CREATE TABLE e (a INT, b INT)").unwrap();
    assert_both(
        &engine,
        "SELECT b, COUNT(*) FROM e",
        &[vec![Value::Null, Value::Int(0)]],
    );
}

#[test]
fn hidden_column_with_filtered_implicit_group() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    execute_permissive(
        &engine,
        "CREATE TABLE filtered_hidden (pk INT PRIMARY KEY, i INT)",
    )
    .unwrap();
    execute_permissive(
        &engine,
        "INSERT INTO filtered_hidden VALUES (1,11),(2,12),(3,13)",
    )
    .unwrap();
    assert_both(
        &engine,
        "SELECT MAX(pk), i FROM filtered_hidden WHERE pk < 2",
        &[vec![Value::Int(1), Value::Int(11)]],
    );
}

/// GROUP BY without aggregate calls also supports hidden columns.
#[test]
fn hidden_column_without_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_p(&engine);
    assert_both(
        &engine,
        "SELECT b FROM p GROUP BY a ORDER BY a",
        &[vec![Value::Int(10)], vec![Value::Int(10)]],
    );
}

/// A NULL first-row value must remain NULL, unlike MIN/MAX which skip NULLs.
#[test]
fn hidden_column_keeps_first_row_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    execute_permissive(&engine, "CREATE TABLE n (a INT PRIMARY KEY, b INT, c INT)").unwrap();
    execute_permissive(&engine, "INSERT INTO n VALUES (1, NULL, 5), (2, 7, 5)").unwrap();
    // One group with equal c values; insertion-order heap scan sees NULL b first.
    assert_both(
        &engine,
        "SELECT b, COUNT(*) FROM n GROUP BY c",
        &[vec![Value::Null, Value::Int(2)]],
    );
}

/// Aggregation over a join takes hidden columns from the nongrouped side.
#[test]
fn hidden_column_over_join_input() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    execute_permissive(&engine, "CREATE TABLE t1 (id INT PRIMARY KEY, g INT)").unwrap();
    execute_permissive(
        &engine,
        "CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT, x INT)",
    )
    .unwrap();
    execute_permissive(&engine, "INSERT INTO t1 VALUES (1, 100), (2, 200)").unwrap();
    execute_permissive(
        &engine,
        "INSERT INTO t2 VALUES (10, 1, 7), (11, 1, 7), (12, 2, 9)",
    )
    .unwrap();
    assert_both(
        &engine,
        "SELECT t2.x, COUNT(*) FROM t1 JOIN t2 ON t2.t1_id = t1.id \
         GROUP BY t1.g ORDER BY t1.g",
        &[
            vec![Value::Int(7), Value::Int(2)],
            vec![Value::Int(9), Value::Int(1)],
        ],
    );
}

/// Grouping columns remain unchanged when mixed with hidden columns.
#[test]
fn group_key_columns_still_resolve_from_fields() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_p(&engine);
    assert_both(
        &engine,
        "SELECT a, b, COUNT(*) FROM p GROUP BY a ORDER BY a",
        &[
            vec![Value::Int(1), Value::Int(10), Value::Int(1)],
            vec![Value::Int(2), Value::Int(10), Value::Int(1)],
        ],
    );
}

/// Forced PARALLEL aggregation also uses cache lookup; FirstValueAcc::merge_from
/// uses the first nonempty partial, keeping all hidden columns from the same row.
#[test]
fn hidden_column_with_parallel_hint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    execute_permissive(
        &engine,
        "CREATE TABLE big (a INT PRIMARY KEY, g INT, b INT)",
    )
    .unwrap();
    let values: Vec<String> = (1..=512)
        .map(|i| format!("({i}, {}, {})", i % 4, (i % 4) * 100))
        .collect();
    execute_permissive(
        &engine,
        &format!("INSERT INTO big VALUES {}", values.join(",")),
    )
    .unwrap();
    // b is functionally dependent on g and constant within a group, allowing exact assertions.
    assert_both(
        &engine,
        "SELECT /*+ PARALLEL(2) */ b, COUNT(*) FROM big GROUP BY g ORDER BY g",
        &[
            vec![Value::Int(0), Value::Int(128)],
            vec![Value::Int(100), Value::Int(128)],
            vec![Value::Int(200), Value::Int(128)],
            vec![Value::Int(300), Value::Int(128)],
        ],
    );
}

/// High-cardinality GROUP BY, hidden first-value, and TopN use the fused aggregate kernel.
/// Preserve first-value semantics and TopN ordering across input batches;
/// the kernel retains only grouping keys and K winning rows, not all aggregate output.
#[test]
fn high_cardinality_hidden_column_topn_across_batches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    execute_permissive(
        &engine,
        "CREATE TABLE hidden_topn (id INT PRIMARY KEY, payload VARCHAR(32))",
    )
    .unwrap();
    for start in (0..4096).step_by(256) {
        let values = (start..start + 256)
            .map(|id| format!("({id}, 'v{id:04}')"))
            .collect::<Vec<_>>()
            .join(",");
        execute_permissive(&engine, &format!("INSERT INTO hidden_topn VALUES {values}")).unwrap();
    }

    let sql = "SELECT id, payload FROM hidden_topn \
               GROUP BY id ORDER BY payload DESC LIMIT 10";
    let expected = (4086..4096)
        .rev()
        .map(|id| vec![Value::Int(id), Value::String(format!("v{id:04}"))])
        .collect::<Vec<_>>();
    let fused_before = super::super::select::vec_bridge::HIDDEN_FIRST_VALUE_TOPN_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_both(&engine, sql, &expected);
    assert!(
        super::super::select::vec_bridge::HIDDEN_FIRST_VALUE_TOPN_HITS
            .load(std::sync::atomic::Ordering::Relaxed)
            > fused_before,
        "ordinary vector SELECT must enter the fused hidden first-value TopN kernel"
    );

    let explain = rows_of(
        execute_permissive(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}")).unwrap(),
    );
    let plan = explain[0][0].to_display_string();
    assert!(
        plan.contains("aggregate_mode=first_value_topn"),
        "hidden first-value TopN must exercise the fused aggregate: {plan}"
    );
}

#[test]
fn compact_hidden_topn_group_key_preserves_collation_and_first_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    execute_permissive(
        &engine,
        "CREATE TABLE compact_hidden (id INT, label VARCHAR(32), payload INT)",
    )
    .unwrap();
    execute_permissive(
        &engine,
        "INSERT INTO compact_hidden VALUES \
             (1,'Alpha',10),(1,'alpha',99),(2,'Beta',20),(2,'BETA',88)",
    )
    .unwrap();
    assert_both(
        &engine,
        "SELECT id, label, payload FROM compact_hidden \
         GROUP BY label, id ORDER BY payload DESC LIMIT 10",
        &[
            vec![Value::Int(2), Value::String("Beta".into()), Value::Int(20)],
            vec![Value::Int(1), Value::String("Alpha".into()), Value::Int(10)],
        ],
    );
}

#[test]
fn unique_dimension_attribute_uses_fact_key_seen_set() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    execute_permissive(
        &engine,
        "CREATE TABLE fd_dim (id INT PRIMARY KEY, name VARCHAR(32))",
    )
    .unwrap();
    execute_permissive(
        &engine,
        "CREATE TABLE fd_fact (seq INT PRIMARY KEY, dim_id INT, amount INT)",
    )
    .unwrap();
    execute_permissive(
        &engine,
        "INSERT INTO fd_dim VALUES (1,'one'),(2,'two'),(3,'three')",
    )
    .unwrap();
    execute_permissive(
        &engine,
        "INSERT INTO fd_fact VALUES \
             (1,1,100),(2,1,999),(3,2,80),(4,2,777),(5,3,60)",
    )
    .unwrap();
    let sql = "SELECT /*+ HASH_JOIN(d, f) */ d.name AS name, f.dim_id AS id, f.amount \
               FROM fd_dim d JOIN fd_fact f ON d.id=f.dim_id \
               GROUP BY name, id ORDER BY f.amount DESC LIMIT 2";
    let before = super::super::select::vec_bridge::HIDDEN_FIRST_VALUE_FD_GROUP_ROWS
        .load(std::sync::atomic::Ordering::Relaxed);
    let expected = with_vec_mode(VecMode::Off, || {
        rows_of(execute_permissive(&engine, sql).unwrap())
    });
    let actual = with_vec_mode(VecMode::On, || {
        rows_of(execute_permissive(&engine, sql).unwrap())
    });
    assert_eq!(actual, expected);
    let explain = rows_of(
        execute_permissive(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}")).unwrap(),
    )[0][0]
        .to_display_string();
    assert!(
        super::super::select::vec_bridge::HIDDEN_FIRST_VALUE_FD_GROUP_ROWS
            .load(std::sync::atomic::Ordering::Relaxed)
            > before,
        "UNIQUE dimension FD was not used: {explain}"
    );
}

/// ROLLUP looks up hidden columns for normal rows and uses each bucket's first row for superaggregates.
#[test]
fn hidden_column_with_rollup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_p(&engine);
    let rows = rows_of(
        execute_permissive(
            &engine,
            "SELECT b, COUNT(*) FROM p GROUP BY a WITH ROLLUP ORDER BY 2, 1",
        )
        .unwrap(),
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(10), Value::Int(1)],
            vec![Value::Int(10), Value::Int(1)],
            vec![Value::Int(10), Value::Int(2)],
        ]
    );
}

/// Bare ORDER BY names matching select aliases still resolve as aliases rather than
/// hidden-column wrappers, which would cause 1054 while evaluating input slots.
#[test]
fn order_by_alias_not_treated_as_hidden_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_p(&engine);
    assert_both(
        &engine,
        "SELECT a, COUNT(*) AS cnt FROM p GROUP BY a ORDER BY cnt, a",
        &[
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(1)],
        ],
    );
}
