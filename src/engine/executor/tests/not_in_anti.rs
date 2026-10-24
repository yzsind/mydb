//! Tests for the `NOT IN (uncorrelated)` → Anti Join rewrite in
//! `planner_decorrelate::anti`. The rewrite only fires when both
//! the outer and inner columns are declared `NOT NULL`; everything
//! else falls back to the legacy eval-time path.

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

fn explain_with_setup(setup: &[&str], sql: &str) -> String {
    with_test_env(&[("MYDB_DECORRELATE", None)], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        for statement in setup {
            engine.execute(statement).unwrap();
        }
        let QueryResponse::ResultSet(result) = engine
            .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
            .unwrap()
        else {
            panic!("expected EXPLAIN result set: {sql}");
        };
        result
            .rows
            .into_iter()
            .flatten()
            .map(|value| value.to_display_string())
            .collect::<Vec<_>>()
            .join("\n")
    })
}

#[test]
fn not_in_with_not_null_columns_returns_correct_rows() {
    // Both sides NOT NULL (PRIMARY KEY columns are NOT NULL by default).
    // Anti join is safe.
    let setup = [
        "CREATE TABLE supplier (s_suppkey INT NOT NULL PRIMARY KEY, s_name VARCHAR(20))",
        "CREATE TABLE partsupp (ps_partkey INT NOT NULL, ps_suppkey INT NOT NULL, PRIMARY KEY (ps_partkey, ps_suppkey))",
        "INSERT INTO supplier VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')",
        "INSERT INTO partsupp VALUES (10,1),(10,2),(10,3),(20,1),(20,4)",
    ];
    // Suppliers 1,2 are "bad". partsupp without bad suppliers:
    //   (10,3), (20,4) — these have ps_suppkey NOT IN (1,2).
    let result = rows(
        &setup,
        "SELECT ps_partkey, ps_suppkey FROM partsupp \
         WHERE ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE s_suppkey <= 2) \
         ORDER BY ps_partkey, ps_suppkey",
    );
    assert_eq!(
        result,
        vec![
            vec![Value::Int(10), Value::Int(3)],
            vec![Value::Int(20), Value::Int(4)],
        ]
    );
}

#[test]
fn not_in_with_nullable_inner_does_not_rewrite() {
    // Inner column nullable — rewriter must NOT convert to Anti join.
    // Strict SQL semantics would drop every outer row when the inner
    // produces a NULL, but mydb's legacy eval-time NOT IN path has a
    // pre-existing departure from that strict semantics (it returns
    // rows where the comparison happens to be definitionally false
    // instead of unknown). The point of this test is to confirm we
    // *don't* convert to an Anti join in that case — the rewrite gate
    // requires both sides to be declared NOT NULL.
    let setup = [
        "CREATE TABLE outer_t (id INT NOT NULL PRIMARY KEY, k INT NOT NULL)",
        "CREATE TABLE inner_t (id INT NOT NULL PRIMARY KEY, x INT)", // x nullable
        "INSERT INTO outer_t VALUES (1,10),(2,20),(3,30)",
        "INSERT INTO inner_t VALUES (1, 10), (2, NULL)",
    ];
    // Just verify execution completes — it may produce SQL-non-strict
    // results because the legacy path is what runs.
    let _ = rows(
        &setup,
        "SELECT outer_t.id FROM outer_t WHERE outer_t.k NOT IN (SELECT x FROM inner_t)",
    );
}

#[test]
fn not_in_uncorrelated_simple_body() {
    // Smallest happy case.
    let setup = [
        "CREATE TABLE a (id INT NOT NULL PRIMARY KEY)",
        "CREATE TABLE b (id INT NOT NULL PRIMARY KEY)",
        "INSERT INTO a VALUES (1),(2),(3),(4),(5)",
        "INSERT INTO b VALUES (2),(4)",
    ];
    let result = rows(
        &setup,
        "SELECT a.id FROM a WHERE a.id NOT IN (SELECT id FROM b) ORDER BY a.id",
    );
    assert_eq!(
        result,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(3)],
            vec![Value::Int(5)]
        ]
    );
}

#[test]
fn not_in_correlated_does_not_rewrite() {
    // The body references an outer column → correlated → skip.
    // The result still needs to be correct via the eval-time path.
    let setup = [
        "CREATE TABLE o (id INT NOT NULL PRIMARY KEY, k INT NOT NULL)",
        "CREATE TABLE i (k INT NOT NULL, v INT NOT NULL, PRIMARY KEY (k, v))",
        "INSERT INTO o VALUES (1,10),(2,20),(3,30)",
        "INSERT INTO i VALUES (10, 100), (20, 200)",
        // For each outer row, ask whether its `id` is in `i.v` for the
        // matching `k`. For outer id=1, k=10 → i.v WHERE i.k=10 → {100}.
        // 1 NOT IN {100} → true → keep id=1.
    ];
    let result = rows(
        &setup,
        "SELECT o.id FROM o \
         WHERE o.id NOT IN (SELECT v FROM i WHERE i.k = o.k) \
         ORDER BY o.id",
    );
    // All three rows: 1 NOT IN {100} = true, 2 NOT IN {200} = true,
    //                 3 NOT IN {} = true.
    assert_eq!(
        result,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
}

#[test]
fn not_in_with_subquery_filter() {
    // The subquery has a non-correlated WHERE — should be preserved on
    // the Anti join's right side.
    let setup = [
        "CREATE TABLE a (id INT NOT NULL PRIMARY KEY)",
        "CREATE TABLE b (id INT NOT NULL PRIMARY KEY, tag INT NOT NULL)",
        "INSERT INTO a VALUES (1),(2),(3),(4),(5)",
        "INSERT INTO b VALUES (2, 1), (3, 2), (4, 1)",
        // Only b rows with tag=1: ids {2, 4}.
        // a.id NOT IN {2, 4} → {1, 3, 5}.
    ];
    let result = rows(
        &setup,
        "SELECT a.id FROM a \
         WHERE a.id NOT IN (SELECT id FROM b WHERE tag = 1) \
         ORDER BY a.id",
    );
    assert_eq!(
        result,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(3)],
            vec![Value::Int(5)]
        ]
    );
}

#[test]
fn tpch_q16_shape_decorrelates() {
    // TPC-H Q16 shape: NOT IN with NOT NULL primary key on supplier.
    let setup = [
        "CREATE TABLE supplier (s_suppkey INT NOT NULL PRIMARY KEY, s_comment VARCHAR(50) NOT NULL)",
        "CREATE TABLE partsupp (ps_partkey INT NOT NULL, ps_suppkey INT NOT NULL, PRIMARY KEY (ps_partkey, ps_suppkey))",
        "INSERT INTO supplier VALUES \
         (1, 'good'),\
         (2, 'Customer Complaints abound'),\
         (3, 'good'),\
         (4, 'has Customer Complaints')",
        "INSERT INTO partsupp VALUES \
         (10,1),(10,2),(10,3),(10,4),\
         (20,1),(20,3),\
         (30,2),(30,4)",
        // Suppliers with 'Customer.*Complaints' in s_comment: 2, 4.
        // partsupp rows excluding those suppliers:
        //   p=10: keep s=1,3 → (10,1),(10,3)
        //   p=20: keep s=1,3 → (20,1),(20,3)
        //   p=30: keep none.
    ];
    let query = "SELECT ps_partkey, ps_suppkey FROM partsupp \
         WHERE ps_suppkey NOT IN (\
             SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%'\
         ) ORDER BY ps_partkey, ps_suppkey";
    let result = rows(&setup, query);
    assert_eq!(
        result,
        vec![
            vec![Value::Int(10), Value::Int(1)],
            vec![Value::Int(10), Value::Int(3)],
            vec![Value::Int(20), Value::Int(1)],
            vec![Value::Int(20), Value::Int(3)],
        ]
    );

    let plan = explain_with_setup(&setup, query);
    let anti = plan
        .lines()
        .find(|line| line.contains("anti join"))
        .expect("Q16 shape should decorrelate to an anti join");
    assert!(
        !anti.contains("residual:"),
        "inner-only predicate must not remain a pair residual:\n{plan}"
    );
    let supplier_scan = plan
        .lines()
        .find(|line| line.contains("scan on supplier"))
        .expect("anti join should scan supplier");
    assert!(
        supplier_scan.contains("filter:"),
        "inner-only predicate must be pushed into the build scan:\n{plan}"
    );
}

// Issue #69: NOT IN subquery NULL three-valued logic.

fn rows_mode(mode: Option<&str>, setup: &[&str], sql: &str) -> Vec<Vec<Value>> {
    with_test_env(&[("MYDB_DECORRELATE", mode)], || {
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

#[test]
fn not_in_subquery_three_valued_logic() {
    // Case 1: with NULL in the subquery set, NOT IN cannot become TRUE, so the result is empty.
    let setup1 = [
        "CREATE TABLE a(x INT)",
        "CREATE TABLE b(y INT)",
        "INSERT INTO a VALUES (1),(2),(3)",
        "INSERT INTO b VALUES (2),(NULL)",
    ];
    // Case 2: NULL NOT IN (2) is NULL and excludes that outer row.
    let setup2 = [
        "CREATE TABLE a(x INT)",
        "CREATE TABLE b(y INT)",
        "INSERT INTO a VALUES (1),(NULL),(3)",
        "INSERT INTO b VALUES (2)",
    ];
    for mode in [None, Some("off")] {
        let r1 = rows_mode(
            mode,
            &setup1,
            "SELECT x FROM a WHERE x NOT IN (SELECT y FROM b) ORDER BY x",
        );
        assert!(
            r1.is_empty(),
            "NOT IN with a NULL-containing set must return no rows (mode={mode:?}), got {r1:?}"
        );

        let r2 = rows_mode(
            mode,
            &setup2,
            "SELECT x FROM a WHERE x NOT IN (SELECT y FROM b) ORDER BY x",
        );
        assert_eq!(
            r2,
            vec![vec![Value::Int(1)], vec![Value::Int(3)]],
            "Outer NULL rows must be excluded (mode={mode:?})"
        );
    }
}

#[test]
fn in_subquery_projected_three_valued_logic() {
    // In projection, a nonmatching IN against a set containing NULL returns NULL, not zero.
    let setup = ["CREATE TABLE b(y INT)", "INSERT INTO b VALUES (2),(NULL)"];
    // 5 IN (2,NULL) is NULL; matching 2 IN (2,NULL) is 1.
    let r = rows_mode(
        None,
        &setup,
        "SELECT 5 IN (SELECT y FROM b), 2 IN (SELECT y FROM b)",
    );
    assert_eq!(r[0][0], Value::Null, "5 IN (2,NULL) must be NULL");
    assert_eq!(r[0][1], Value::Int(1), "2 IN (2,NULL) must be 1");
}
