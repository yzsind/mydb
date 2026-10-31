//! Tests for `x IN (uncorrelated subquery)` → SemiJoin rewrite
//! (planner.rs::try_rewrite_subquery_predicate).
//!
//! The heuristic in `in_subquery_should_stay` keeps the original
//! `Expr::InSubquery` AST when the surrounding WHERE has another
//! literal equality on the same outer table — so the IndexInLookup
//! specialisation can drive a composite-index probe. Everything else
//! turns into a SemiJoin that the optimiser can cost-pick a hash or
//! index strategy for.

use super::*;

#[test]
fn closed_window_derived_membership_filters_dimension_before_join() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE membership_dim(k INT PRIMARY KEY,tag VARCHAR(10))",
        "CREATE TABLE membership_fact(id INT,k INT)",
        "INSERT INTO membership_dim VALUES(1,'A'),(2,'B'),(3,'A'),(4,NULL)",
        "INSERT INTO membership_fact VALUES(100,1),(200,2),(300,3),(400,4),(500,NULL)",
    ] {
        engine.execute(sql).unwrap();
    }
    let sql = "SELECT f.id FROM membership_fact f JOIN membership_dim d ON f.k=d.k WHERE d.tag IN \
        (SELECT q.tag FROM (SELECT d2.tag,RANK() OVER(PARTITION BY d2.tag ORDER BY d2.k) rn FROM membership_dim d2) q \
        WHERE q.rn<=1 AND q.tag='A') ORDER BY f.id";
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("rows")
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(100)], vec![Value::Int(300)]]
    );
    let QueryResponse::ResultSet(explain) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
        .unwrap()
    else {
        panic!("plan")
    };
    let plan = format!("{:?}", explain.rows);
    assert!(plan.contains("Apply"), "{plan}");
}

fn run(setup: &[&str], sql: &str) -> Vec<Vec<Value>> {
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

fn run_with_existence_keyset(setup: &[&str], sql: &str, enabled: bool) -> Vec<Vec<Value>> {
    let gate = if enabled { None } else { Some("off") };
    with_test_env(
        &[
            ("MYDB_DECORRELATE", None),
            ("MYDB_CTE_EXISTENCE_KEYSET", gate),
        ],
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

fn explain_tree(setup: &[&str], sql: &str) -> String {
    with_test_env(&[("MYDB_DECORRELATE", None)], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        for s in setup {
            engine.execute(s).unwrap();
        }
        let QueryResponse::ResultSet(rs) = engine
            .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
            .unwrap()
        else {
            panic!("expected explain result set");
        };
        match &rs.rows[0][0] {
            Value::String(s) => s.clone(),
            other => panic!("expected tree text, got {other:?}"),
        }
    })
}

fn explain_analyze_tree(setup: &[&str], sql: &str) -> String {
    with_test_env(&[("MYDB_DECORRELATE", None)], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        for s in setup {
            engine.execute(s).unwrap();
        }
        let QueryResponse::ResultSet(rs) = engine
            .execute(&format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"))
            .unwrap()
        else {
            panic!("expected explain analyze result set");
        };
        match &rs.rows[0][0] {
            Value::String(s) => s.clone(),
            other => panic!("expected tree text, got {other:?}"),
        }
    })
}

#[test]
fn in_subquery_with_no_index_rewrites_to_semi_join() {
    let setup = [
        "CREATE TABLE a (id INT PRIMARY KEY, k INT)",
        "CREATE TABLE b (id INT PRIMARY KEY)",
        "INSERT INTO a VALUES (1,10),(2,20),(3,30),(4,40)",
        "INSERT INTO b VALUES (10), (30)",
    ];
    let result = run(
        &setup,
        "SELECT id FROM a WHERE k IN (SELECT id FROM b) ORDER BY id",
    );
    assert_eq!(result, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);

    // Confirm the EXPLAIN tree shows a Semi join, not a `Using IN
    // subquery lookup` IndexInLookup.
    let tree = explain_tree(&setup, "SELECT id FROM a WHERE k IN (SELECT id FROM b)");
    let lower = tree.to_ascii_lowercase();
    assert!(
        lower.contains("semi") || lower.contains("hash join"),
        "expected Semi/Hash join in plan, got: {tree}"
    );
}

#[test]
fn in_subquery_with_sibling_literal_eq_keeps_index_lookup() {
    // The sibling `tenant_id = 1` keeps the IN-subquery so the
    // (tenant_id, item_id) composite index can drive the lookup.
    let setup = [
        "CREATE TABLE inventory (tenant_id INT, item_id INT, qty INT, \
                                 INDEX tenant_item_idx (tenant_id, item_id))",
        "CREATE TABLE recent_items (item_id INT)",
        "INSERT INTO inventory VALUES (1,100,8),(1,101,20),(1,102,3),(2,100,1)",
        "INSERT INTO recent_items VALUES (100), (102), (100)",
    ];
    let result = run(
        &setup,
        "SELECT item_id FROM inventory \
         WHERE tenant_id = 1 AND item_id IN (SELECT item_id FROM recent_items) \
         ORDER BY item_id",
    );
    assert_eq!(result, vec![vec![Value::Int(100)], vec![Value::Int(102)]]);

    let tree = explain_tree(
        &setup,
        "SELECT item_id FROM inventory \
         WHERE tenant_id = 1 AND item_id IN (SELECT item_id FROM recent_items)",
    );
    // Should mention the composite index — the In-lookup path is what
    // we explicitly chose to preserve. (Don't assert "semi join" here;
    // it shouldn't show up.)
    assert!(
        tree.contains("tenant_item_idx") || tree.to_ascii_lowercase().contains("subquery lookup"),
        "expected composite-index plan, got: {tree}"
    );
}

#[test]
fn in_subquery_inner_predicate_kept_post_rewrite() {
    // The inner subquery's own WHERE (`tag = 1`) survives the SemiJoin
    // rewrite as an extra outer-level conjunct.
    let setup = [
        "CREATE TABLE a (id INT PRIMARY KEY, k INT)",
        "CREATE TABLE b (id INT PRIMARY KEY, tag INT)",
        "INSERT INTO a VALUES (1,10),(2,20),(3,30)",
        "INSERT INTO b VALUES (10, 1), (20, 2), (30, 1)",
    ];
    let result = run(
        &setup,
        "SELECT id FROM a WHERE k IN (SELECT id FROM b WHERE tag = 1) ORDER BY id",
    );
    // b ids with tag=1: 10 and 30. So a rows with k in {10,30}: id=1,3.
    assert_eq!(result, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
}

#[test]
fn in_subquery_returns_empty_when_no_matches() {
    let setup = [
        "CREATE TABLE a (id INT PRIMARY KEY)",
        "CREATE TABLE b (id INT PRIMARY KEY)",
        "INSERT INTO a VALUES (1), (2)",
        "INSERT INTO b VALUES (99)",
    ];
    let result = run(&setup, "SELECT id FROM a WHERE id IN (SELECT id FROM b)");
    assert!(result.is_empty());
}

#[test]
fn in_subquery_against_cte_uses_physical_semi_join() {
    let setup = [
        "CREATE TABLE web_sales (order_no INT, warehouse_sk INT, ship_cost INT, net_profit INT)",
        "CREATE TABLE date_dim (d_date_sk INT PRIMARY KEY, d_date DATE)",
        "CREATE TABLE customer_address (ca_address_sk INT PRIMARY KEY, ca_state CHAR(2))",
        "CREATE TABLE web_site (web_site_sk INT PRIMARY KEY, web_company_name VARCHAR(20))",
        "INSERT INTO web_sales VALUES \
            (1,10,5,7),(1,20,6,8),(2,10,3,4),(3,30,9,2)",
        "INSERT INTO date_dim VALUES (1, '2000-03-01')",
        "INSERT INTO customer_address VALUES (1, 'GA')",
        "INSERT INTO web_site VALUES (1, 'pri')",
    ];
    let sql = "WITH ws_wh AS (\
                   SELECT ws1.order_no, ws1.warehouse_sk wh1, ws2.warehouse_sk wh2 \
                   FROM web_sales ws1, web_sales ws2 \
                   WHERE ws1.order_no = ws2.order_no \
                     AND ws1.warehouse_sk <> ws2.warehouse_sk\
               ) \
               SELECT COUNT(DISTINCT ws1.order_no), SUM(ws1.ship_cost), SUM(ws1.net_profit) \
               FROM web_sales ws1, date_dim, customer_address, web_site \
               WHERE d_date BETWEEN '2000-03-01' AND '2000-04-30' \
                 AND ws1.order_no IN (SELECT order_no FROM ws_wh)";

    let result = run(&setup, sql);
    assert_eq!(result[0][0], Value::Int(1));

    let tree = explain_tree(&setup, sql);
    let lower = tree.to_ascii_lowercase();
    assert!(
        lower.contains("cte scan"),
        "expected CTE scan in plan:\n{tree}"
    );
    assert!(
        lower.contains("semi") || lower.contains("hash join"),
        "expected physical semi/hash join instead of CTE nested-loop fallback:\n{tree}"
    );
}

#[test]
fn cte_existence_keyset_self_neq_matches_legacy_q95_shape() {
    let setup = [
        "CREATE TABLE web_sales (\
             order_no INT, warehouse_sk INT, ship_date_sk INT, ship_addr_sk INT, \
             web_site_sk INT, ship_cost INT, net_profit INT\
         )",
        "CREATE TABLE web_returns (order_no INT)",
        "CREATE TABLE date_dim (d_date_sk INT PRIMARY KEY, d_date DATE)",
        "CREATE TABLE customer_address (ca_address_sk INT PRIMARY KEY, ca_state CHAR(2))",
        "CREATE TABLE web_site (web_site_sk INT PRIMARY KEY, web_company_name VARCHAR(20))",
        "INSERT INTO web_sales VALUES \
             (1,10,1,1,1,5,7), (1,20,1,1,1,6,8), \
             (2,10,1,1,1,3,4), (2,10,1,1,1,4,5), \
             (3,NULL,1,1,1,9,2), (3,30,1,1,1,10,3), \
             (4,40,1,1,1,11,12), (4,50,1,1,1,13,14), \
             (5,60,1,1,2,100,100)",
        "INSERT INTO web_returns VALUES (1), (2), (3), (4)",
        "INSERT INTO date_dim VALUES (1, '2000-03-01')",
        "INSERT INTO customer_address VALUES (1, 'GA')",
        "INSERT INTO web_site VALUES (1, 'pri'), (2, 'other')",
    ];
    let sql = "WITH ws_wh AS (\
                   SELECT ws1.order_no, ws1.warehouse_sk wh1, ws2.warehouse_sk wh2 \
                   FROM web_sales ws1, web_sales ws2 \
                   WHERE ws1.order_no = ws2.order_no \
                     AND ws1.warehouse_sk <> ws2.warehouse_sk\
               ) \
               SELECT COUNT(DISTINCT ws1.order_no), SUM(ws1.ship_cost), SUM(ws1.net_profit) \
               FROM web_sales ws1, date_dim, customer_address, web_site \
               WHERE d_date BETWEEN '2000-03-01' AND '2000-04-30' \
                 AND ws1.ship_date_sk = d_date_sk \
                 AND ws1.ship_addr_sk = ca_address_sk \
                 AND ca_state = 'GA' \
                 AND ws1.web_site_sk = web_site.web_site_sk \
                 AND web_company_name = 'pri' \
                 AND ws1.order_no IN (SELECT order_no FROM ws_wh) \
                 AND ws1.order_no IN (SELECT wr.order_no \
                                      FROM web_returns wr, ws_wh \
                                      WHERE wr.order_no = ws_wh.order_no)";

    let legacy = run_with_existence_keyset(&setup, sql, false);
    let optimized = run_with_existence_keyset(&setup, sql, true);
    assert_eq!(optimized, legacy);
    assert_eq!(
        optimized,
        vec![vec![
            Value::Int(2),
            Value::DecimalCompact(35, 0),
            Value::DecimalCompact(41, 0),
        ]]
    );
}

#[test]
fn cte_existence_keyset_does_not_rewrite_visible_non_key_columns() {
    let setup = [
        "CREATE TABLE web_sales (order_no INT, warehouse_sk INT)",
        "INSERT INTO web_sales VALUES (1,10), (1,20), (2,10), (2,10)",
    ];
    let sql = "WITH ws_wh AS (\
                   SELECT ws1.order_no, ws1.warehouse_sk wh1, ws2.warehouse_sk wh2 \
                   FROM web_sales ws1, web_sales ws2 \
                   WHERE ws1.order_no = ws2.order_no \
                     AND ws1.warehouse_sk <> ws2.warehouse_sk\
               ) \
               SELECT order_no, wh1, wh2 FROM ws_wh ORDER BY order_no, wh1, wh2";

    let legacy = run_with_existence_keyset(&setup, sql, false);
    let optimized = run_with_existence_keyset(&setup, sql, true);
    assert_eq!(optimized, legacy);
    assert_eq!(
        optimized,
        vec![
            vec![Value::Int(1), Value::Int(10), Value::Int(20)],
            vec![Value::Int(1), Value::Int(20), Value::Int(10)],
        ]
    );
}

#[test]
fn cte_existence_keyset_skips_multisource_bare_cte_output_columns() {
    let setup = [
        "CREATE TABLE web_sales (order_no INT, warehouse_sk INT)",
        "CREATE TABLE marker (x INT)",
        "CREATE TABLE probe (id INT)",
        "INSERT INTO web_sales VALUES (1,10), (1,20), (2,10), (2,10)",
        "INSERT INTO marker VALUES (10)",
        "INSERT INTO probe VALUES (1), (2)",
    ];
    let sql = "WITH ws_wh AS (\
                   SELECT ws1.order_no, ws1.warehouse_sk wh1, ws2.warehouse_sk wh2 \
                   FROM web_sales ws1, web_sales ws2 \
                   WHERE ws1.order_no = ws2.order_no \
                     AND ws1.warehouse_sk <> ws2.warehouse_sk\
               ) \
               SELECT id FROM probe \
               WHERE id IN (SELECT order_no FROM marker, ws_wh WHERE wh1 = marker.x) \
               ORDER BY id";

    let legacy = run_with_existence_keyset(&setup, sql, false);
    let optimized = run_with_existence_keyset(&setup, sql, true);
    assert_eq!(optimized, legacy);
    assert_eq!(optimized, vec![vec![Value::Int(1)]]);
}

/// Regression: `col IN (SELECT col FROM cte)` where the CTE projects a
/// column with the SAME name as an outer column, AND the outer query is a
/// multi-table join with a sibling predicate that references that bare
/// column. This is the shape of TPC-DS q14/q23
/// (`ss_item_sk IN (SELECT ss_item_sk FROM cross_items) AND ss_item_sk = i_item_sk`).
///
/// Lifting the IN-subquery into a Semi join drags the CTE's same-named
/// column into the outer row scope, so the previously-unambiguous bare
/// `ssk` references would resolve to two fields at runtime → 1052. The
/// rewrite must bind each such reference back to its unique outer owner.
#[test]
fn in_subquery_semi_join_qualifies_colliding_sibling_refs() {
    let setup = [
        "CREATE TABLE ss (ssk INT, qty INT)",
        "CREATE TABLE it (isk INT PRIMARY KEY)",
        "INSERT INTO ss VALUES (1,5),(2,7),(3,9)",
        "INSERT INTO it VALUES (1),(2),(3)",
    ];
    // `ssk` lives only on ss in the outer scope; the CTE `ci` projects a
    // column it also names `ssk`. `ssk IN (...)` and the sibling
    // `ssk = isk` both reference the bare name.
    let result = run(
        &setup,
        "WITH ci AS (SELECT isk AS ssk FROM it WHERE isk <= 2) \
         SELECT COUNT(*) FROM ss, it \
         WHERE ssk IN (SELECT ssk FROM ci) AND ssk = isk",
    );
    // ss⋈it on ssk=isk → (1,2,3); keep ssk ∈ {1,2} → 2 rows.
    assert_eq!(result, vec![vec![Value::Int(2)]]);

    // The lifted plan must qualify the semi-join key to its owners.
    let tree = explain_tree(
        &setup,
        "WITH ci AS (SELECT isk AS ssk FROM it WHERE isk <= 2) \
         SELECT COUNT(*) FROM ss, it \
         WHERE ssk IN (SELECT ssk FROM ci) AND ssk = isk",
    );
    assert!(
        tree.to_ascii_lowercase().contains("semi"),
        "expected a Semi join in plan, got: {tree}"
    );
}

// ---- Correlated EXISTS with multi-table body → derived-table semi-lift
// (planner.rs::build_exists_semi_join_derived).  Targets TPC-DS Q35, Q10
// and similar shapes where the EXISTS body joins a fact table to a date /
// dimension table and correlates on the outer customer/order key.  The
// single-table path (build_exists_semi_join) rejects these because
// from.len() != 1; the derived path lifts the body as
// `SEMI JOIN (SELECT DISTINCT inner_key …) ON outer.k = sj.k`.

#[test]
fn correlated_multi_table_exists_lifted_as_derived_join() {
    let setup = [
        "CREATE TABLE customer (c_id INT PRIMARY KEY, c_name VARCHAR(16))",
        "CREATE TABLE date_dim (d_date_sk INT PRIMARY KEY, d_year INT)",
        "CREATE TABLE store_sales (ss_customer_sk INT, ss_sold_date_sk INT)",
        "INSERT INTO customer VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')",
        "INSERT INTO date_dim VALUES (10, 2001),(11, 2001),(12, 2002),(13, 2002)",
        "INSERT INTO store_sales VALUES (1,10),(2,11),(3,12),(3,13),(4,12)",
    ];
    let result = run(
        &setup,
        "SELECT c_id FROM customer c WHERE EXISTS (\
            SELECT 1 FROM store_sales, date_dim \
            WHERE c.c_id = ss_customer_sk \
              AND ss_sold_date_sk = d_date_sk \
              AND d_year = 2001) \
         ORDER BY c_id",
    );
    assert_eq!(result, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

    let tree = explain_tree(
        &setup,
        "SELECT c_id FROM customer c WHERE EXISTS (\
            SELECT 1 FROM store_sales, date_dim \
            WHERE c.c_id = ss_customer_sk \
              AND ss_sold_date_sk = d_date_sk \
              AND d_year = 2001)",
    );
    let lower = tree.to_ascii_lowercase();
    // The lifted derived alias is `__sj_exists_N` and the body's WHERE
    // residue collapses to a DISTINCT over the inner key, surfacing both
    // a `__sj_exists` token and a `distinct` operator in the plan.
    assert!(
        lower.contains("__sj_exists"),
        "expected derived-table semi-lift (__sj_exists alias) in plan, got: {tree}"
    );
    assert!(
        lower.contains("distinct"),
        "expected DISTINCT on the lifted correlation key, got: {tree}"
    );
    // No per-row filter remains over the outer customer scan — the
    // EXISTS evaluation has been replaced by the join itself.
    assert!(
        !lower.contains("filter: exists"),
        "expected EXISTS to be lifted out of the scan filter, got: {tree}"
    );
}

#[test]
fn correlated_multi_table_not_exists_keeps_per_row_path() {
    // NOT EXISTS (multi-table body) — we lift it as an Anti join over the
    // derived. The DISTINCT lifts to ensure exactly-one-anti-match semantics.
    let setup = [
        "CREATE TABLE customer (c_id INT PRIMARY KEY)",
        "CREATE TABLE date_dim (d_date_sk INT PRIMARY KEY, d_year INT)",
        "CREATE TABLE store_sales (ss_customer_sk INT, ss_sold_date_sk INT)",
        "INSERT INTO customer VALUES (1),(2),(3),(4)",
        "INSERT INTO date_dim VALUES (10, 2001),(11, 2002)",
        "INSERT INTO store_sales VALUES (1,10),(2,11),(3,10)",
    ];
    let result = run(
        &setup,
        "SELECT c_id FROM customer c WHERE NOT EXISTS (\
            SELECT 1 FROM store_sales, date_dim \
            WHERE c.c_id = ss_customer_sk \
              AND ss_sold_date_sk = d_date_sk \
              AND d_year = 2001) \
         ORDER BY c_id",
    );
    // Customers 2 (only 2002) and 4 (no sales) survive the NOT EXISTS.
    assert_eq!(result, vec![vec![Value::Int(2)], vec![Value::Int(4)]]);
}

#[test]
fn correlated_multi_table_exists_non_eq_correlation_falls_back() {
    // The correlation is `c.c_id > ss_customer_sk` — a non-equality
    // reference that the derived-lift rejects (would require Apply-style
    // unnesting). The legacy per-row eval path still produces the right
    // answer, so we just assert correctness here.
    let setup = [
        "CREATE TABLE customer (c_id INT PRIMARY KEY)",
        "CREATE TABLE date_dim (d_date_sk INT PRIMARY KEY, d_year INT)",
        "CREATE TABLE store_sales (ss_customer_sk INT, ss_sold_date_sk INT)",
        "INSERT INTO customer VALUES (1),(2),(3),(4)",
        "INSERT INTO date_dim VALUES (10, 2001),(11, 2001)",
        "INSERT INTO store_sales VALUES (1,10),(2,11)",
    ];
    let sql = "SELECT c_id FROM customer c WHERE EXISTS (\
            SELECT 1 FROM store_sales, date_dim \
            WHERE c.c_id > ss_customer_sk \
              AND ss_sold_date_sk = d_date_sk \
              AND d_year = 2001) \
         ORDER BY c_id";
    let result = run(&setup, sql);
    // Customers 2,3,4 have some ss row with ss_customer_sk < c_id.
    assert_eq!(
        result,
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)]
        ]
    );
    let tree = explain_tree(&setup, sql);
    assert!(
        tree.contains("Apply"),
        "the undecorrelated predicate must be represented by an explicit physical Apply before execution: {tree}"
    );
    let analyze = explain_analyze_tree(&setup, sql);
    let apply = analyze
        .lines()
        .find(|line| line.contains("Apply"))
        .expect("Apply analyze line");
    assert!(
        apply.contains("execution_mode=vector") && apply.contains("scalar_rows=4"),
        "Apply must evaluate correlation inside the current batch without a query-level executor fallback: {analyze}"
    );
}

#[test]
fn uncorrelated_scalar_apply_binds_once_then_uses_batch_expression() {
    let setup = [
        "CREATE TABLE outer_values (id INT PRIMARY KEY, v DECIMAL(10,2))",
        "CREATE TABLE inner_values (id INT PRIMARY KEY, x DECIMAL(10,2))",
        "INSERT INTO outer_values VALUES (1, 2.00), (2, 8.00), (3, 20.00)",
        "INSERT INTO inner_values VALUES (1, 3.00), (2, 7.00)",
    ];
    let sql = "SELECT id FROM outer_values \
               WHERE v > (SELECT AVG(x) FROM inner_values WHERE x > 0) \
               ORDER BY id";
    assert_eq!(
        run(&setup, sql),
        vec![vec![Value::Int(2)], vec![Value::Int(3)]]
    );
    let analyze = explain_analyze_tree(&setup, sql);
    let apply = analyze
        .lines()
        .find(|line| line.contains("Apply"))
        .expect("uncorrelated scalar predicate should be an explicit Apply");
    assert!(
        !apply.contains("expression_mode=scalar") && !apply.contains("scalar_rows="),
        "the scalar result should be bound once before the outer batch is opened: {analyze}"
    );
}

#[test]
fn volatile_uncorrelated_scalar_materializes_once_and_seeks_ordered_index() {
    let populate = |engine: &StorageQueryEngine| {
        engine.execute("CREATE TABLE random_pick (id INT PRIMARY KEY, status VARCHAR(16), payload INT, INDEX idx_status(status))").unwrap();
        insert_fixture_rows(
            engine,
            "test.random_pick",
            (1..=10_000i64).map(|n| {
                vec![
                    Value::Int(n),
                    Value::String(if n % 3 == 0 { "reject" } else { "accept" }.into()),
                    Value::Int(n),
                ]
            }),
        );
    };
    let sql = "SELECT id FROM random_pick r \
               WHERE r.id >= (SELECT ROUND(RAND(0) * (\
                   SELECT MAX(id) FROM random_pick WHERE status='accept'\
               ))) \
                 AND r.status='accept' \
               ORDER BY r.id LIMIT 1";

    // mydb deliberately treats an uncorrelated scalar query block as one
    // statement-scoped value even when its body contains RAND(). RAND(0)'s
    // first threshold is 1552, which is itself an accepted row and therefore
    // must become the physical PRIMARY lower bound.
    // Reuse the immutable fixture for the result, plan and execution metrics.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate(&engine);
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1552)]]);
    let row_oracle = with_test_env(
        &[("MYDB_DECORRELATE", None), ("MYDB_VECTORIZED", Some("off"))],
        || {
            let dir = tempfile::tempdir().unwrap();
            let engine = open_test_engine(dir.path());
            populate(&engine);
            let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
                panic!("expected row-oracle result set");
            };
            result.rows
        },
    );
    assert_eq!(row_oracle, vec![vec![Value::Int(1552)]]);

    let QueryResponse::ResultSet(result) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let tree = result.rows[0][0].to_display_string();
    assert!(
        tree.contains("Apply") && tree.contains("Index range scan on r using PRIMARY"),
        "the order-providing primary-key scan must compete with a predicate secondary index: {tree}"
    );
    assert!(
        !tree.contains("TopK"),
        "Apply preserves primary-key order, so LIMIT must not retain a blocking TopK: {tree}"
    );
    let scan = tree
        .lines()
        .find(|line| line.contains("Index range scan on r using PRIMARY"))
        .expect("ordered primary-key scan");
    assert!(
        scan.contains("status='accept'") && !scan.contains("RAND"),
        "the ordinary status conjunct must stay on the scan: {tree}"
    );
    let apply = tree
        .lines()
        .find(|line| line.contains("Apply"))
        .expect("Apply line");
    assert!(
        !apply.contains("status='accept'"),
        "Apply must own only the subquery conjunct: {tree}"
    );

    let analyze = super::explain_analyze_tree(&engine, sql);
    let apply = analyze
        .lines()
        .find(|line| line.contains("Apply"))
        .expect("Apply analyze line");
    let scalar_rows = apply
        .split_whitespace()
        .find_map(|part| part.strip_prefix("scalar_rows="))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(
        scalar_rows <= 1,
        "the retained Apply correctness check must see only the seek result: {analyze}"
    );
    let scan = analyze
        .lines()
        .find(|line| line.contains("Index range scan on r using PRIMARY"))
        .expect("analyzed ordered PRIMARY scan");
    let examined = scan
        .split_whitespace()
        .find_map(|part| part.strip_prefix("index_entries_examined="))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(u64::MAX);
    assert!(
        examined <= 2,
        "the materialized threshold must seek instead of scanning the PRIMARY prefix: {analyze}"
    );
}

#[test]
fn correlated_or_of_multi_table_exists_lifted_as_union_derived_join() {
    let setup = [
        "CREATE TABLE customer (c_id INT PRIMARY KEY, c_name VARCHAR(16))",
        "CREATE TABLE date_dim (d_date_sk INT PRIMARY KEY, d_year INT, d_qoy INT)",
        "CREATE TABLE store_sales (ss_customer_sk INT, ss_sold_date_sk INT)",
        "CREATE TABLE web_sales (ws_bill_customer_sk INT, ws_sold_date_sk INT)",
        "CREATE TABLE catalog_sales (cs_ship_customer_sk INT, cs_sold_date_sk INT)",
        "INSERT INTO customer VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')",
        "INSERT INTO date_dim VALUES (10, 2001, 1),(11, 2001, 3),(12, 2001, 4),(13, 2002, 1)",
        "INSERT INTO store_sales VALUES (1,10),(2,11),(3,10),(4,13)",
        "INSERT INTO web_sales VALUES (2,10),(4,12)",
        "INSERT INTO catalog_sales VALUES (3,11),(1,13)",
    ];
    let sql = "SELECT c_id FROM customer c \
               WHERE EXISTS (\
                   SELECT 1 FROM store_sales, date_dim \
                   WHERE c.c_id = ss_customer_sk \
                     AND ss_sold_date_sk = d_date_sk \
                     AND d_year = 2001 AND d_qoy < 4) \
                 AND (EXISTS (\
                   SELECT 1 FROM web_sales, date_dim \
                   WHERE c.c_id = ws_bill_customer_sk \
                     AND ws_sold_date_sk = d_date_sk \
                     AND d_year = 2001 AND d_qoy < 4) \
                  OR EXISTS (\
                   SELECT 1 FROM catalog_sales, date_dim \
                   WHERE c.c_id = cs_ship_customer_sk \
                     AND cs_sold_date_sk = d_date_sk \
                     AND d_year = 2001 AND d_qoy < 4)) \
               ORDER BY c_id";

    let result = run(&setup, sql);
    assert_eq!(result, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);

    let tree = explain_tree(&setup, sql);
    let lower = tree.to_ascii_lowercase();
    assert!(
        lower.contains("__sj_exists_or"),
        "expected OR-of-EXISTS union lift in plan, got:\n{tree}"
    );
    assert!(
        !lower.contains("filter: exists"),
        "expected EXISTS predicates to be lifted out of filters, got:\n{tree}"
    );
}

#[test]
fn correlated_or_of_exists_mismatched_outer_key_falls_back() {
    let setup = [
        "CREATE TABLE customer (c_id INT PRIMARY KEY, alt_id INT)",
        "CREATE TABLE web_sales (ws_bill_customer_sk INT)",
        "CREATE TABLE catalog_sales (cs_ship_customer_sk INT)",
        "INSERT INTO customer VALUES (1,10),(2,20),(3,30)",
        "INSERT INTO web_sales VALUES (1)",
        "INSERT INTO catalog_sales VALUES (20)",
    ];
    let sql = "SELECT c_id FROM customer c \
               WHERE EXISTS (SELECT 1 FROM web_sales WHERE c.c_id = ws_bill_customer_sk) \
                  OR EXISTS (SELECT 1 FROM catalog_sales WHERE c.alt_id = cs_ship_customer_sk) \
               ORDER BY c_id";

    let result = run(&setup, sql);
    assert_eq!(result, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

    let tree = explain_tree(&setup, sql);
    assert!(
        !tree.to_ascii_lowercase().contains("__sj_exists_or"),
        "mismatched OR keys must not use the union lift:\n{tree}"
    );
}

#[test]
fn correlated_or_with_not_exists_falls_back() {
    let setup = [
        "CREATE TABLE customer (c_id INT PRIMARY KEY)",
        "CREATE TABLE web_sales (ws_bill_customer_sk INT)",
        "CREATE TABLE catalog_sales (cs_ship_customer_sk INT)",
        "INSERT INTO customer VALUES (1),(2),(3)",
        "INSERT INTO web_sales VALUES (1)",
        "INSERT INTO catalog_sales VALUES (2)",
    ];
    let sql = "SELECT c_id FROM customer c \
               WHERE EXISTS (SELECT 1 FROM web_sales WHERE c.c_id = ws_bill_customer_sk) \
                  OR NOT EXISTS (SELECT 1 FROM catalog_sales WHERE c.c_id = cs_ship_customer_sk) \
               ORDER BY c_id";

    let result = run(&setup, sql);
    assert_eq!(result, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);

    let tree = explain_tree(&setup, sql);
    assert!(
        !tree.to_ascii_lowercase().contains("__sj_exists_or"),
        "OR with NOT EXISTS must not use the positive-exists union lift:\n{tree}"
    );
}

#[test]
fn correlated_or_with_non_eq_outer_predicate_falls_back() {
    let setup = [
        "CREATE TABLE customer (c_id INT PRIMARY KEY)",
        "CREATE TABLE web_sales (ws_bill_customer_sk INT)",
        "CREATE TABLE catalog_sales (cs_ship_customer_sk INT)",
        "INSERT INTO customer VALUES (1),(2),(3)",
        "INSERT INTO web_sales VALUES (1)",
        "INSERT INTO catalog_sales VALUES (1)",
    ];
    let sql = "SELECT c_id FROM customer c \
               WHERE EXISTS (SELECT 1 FROM web_sales WHERE c.c_id > ws_bill_customer_sk) \
                  OR EXISTS (SELECT 1 FROM catalog_sales WHERE c.c_id = cs_ship_customer_sk) \
               ORDER BY c_id";

    let result = run(&setup, sql);
    assert_eq!(
        result,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );

    let tree = explain_tree(&setup, sql);
    assert!(
        !tree.to_ascii_lowercase().contains("__sj_exists_or"),
        "non-equality correlation must not use the union lift:\n{tree}"
    );
}

#[test]
fn uncorrelated_in_with_nested_scalar_subquery_per_row_fast_path() {
    // TPC-DS Q58 shape: an uncorrelated `IN (subquery)` whose body itself
    // contains a nested scalar subquery. The nested subquery makes the
    // body ineligible for both semi-join decorrelation and the parametric
    // plan cache, so the predicate stays an eval-time `Expr::InSubquery`
    // evaluated once per outer row. The IN result set is constant across
    // every outer row (no outer references), so the second-and-later rows
    // must hit the per-statement memo via the uncorrelated fast path
    // instead of re-cloning/re-binding the subquery AST per row. This test
    // pins the *result*; the fast path is a pure performance optimisation
    // that must not change it.
    let setup = [
        "CREATE TABLE dd (d_date INT PRIMARY KEY, d_week_seq INT)",
        // anchor row d_date=3 lives in week 200; week 200 = {3, 4}.
        "INSERT INTO dd VALUES (1,100),(2,100),(3,200),(4,200),(5,300)",
        "CREATE TABLE facts (id INT PRIMARY KEY, d INT)",
        // Many outer rows so the per-row eval path is exercised repeatedly.
        "INSERT INTO facts VALUES \
            (1,1),(2,2),(3,3),(4,4),(5,5),(6,3),(7,4),(8,1),(9,4),(10,2)",
    ];
    let result = run(
        &setup,
        "SELECT COUNT(*) FROM facts WHERE d IN (\
            SELECT d_date FROM dd \
            WHERE d_week_seq = (SELECT d_week_seq FROM dd WHERE d_date = 3))",
    );
    // facts with d in {3,4}: ids 3,4,6,7,9 -> 5 rows.
    assert_eq!(result, vec![vec![Value::Int(5)]]);
}

#[test]
fn unique_in_subquery_batch_rebinds_null_prefix_and_collated_keys() {
    use crate::engine::api::{PreparedSelectMemoState, QueryEngine};
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE batch_target (tenant INT,item VARCHAR(20) COLLATE utf8mb4_0900_ai_ci,qty INT,PRIMARY KEY(tenant,item))",
        "CREATE TABLE batch_input (k VARCHAR(20) COLLATE utf8mb4_0900_ai_ci,bucket INT)",
        "INSERT INTO batch_target VALUES (1,'alpha',10),(1,'beta',20),(2,'gamma',30)",
        "INSERT INTO batch_input VALUES ('alpha',1),('ALPHA',1),('beta',1),(NULL,1),('missing',1),('gamma',2)",
    ] { engine.execute(sql).unwrap(); }
    let sql="SELECT qty FROM batch_target WHERE tenant=? AND item IN (SELECT k FROM batch_input WHERE bucket=?) ORDER BY qty";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedSelectMemoState::default();
    for (tenant, bucket, expected) in [
        (
            Value::Int(1),
            1,
            vec![vec![Value::Int(10)], vec![Value::Int(20)]],
        ),
        (Value::Null, 1, vec![]),
        (Value::Int(2), 2, vec![vec![Value::Int(30)]]),
        (
            Value::Int(1),
            1,
            vec![vec![Value::Int(10)], vec![Value::Int(20)]],
        ),
    ] {
        let response = engine
            .execute_prepared_select(
                &mut session,
                &mut state,
                &parsed,
                sql,
                &[tenant, Value::Int(bucket)],
            )
            .unwrap()
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(result) = response else {
            panic!("rows")
        };
        assert_eq!(result.rows, expected);
    }
}

#[test]
fn unique_in_subquery_batch_handles_multiple_probe_windows() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE batch_numbers (tenant INT,id INT,v INT,PRIMARY KEY(tenant,id))")
        .unwrap();
    engine.execute("CREATE TABLE batch_keys (id INT)").unwrap();
    insert_fixture_rows(
        &engine,
        "test.batch_numbers",
        (0..2100).map(|id| vec![Value::Int(1), Value::Int(id), Value::Int(id * 2)]),
    );
    insert_fixture_rows(
        &engine,
        "test.batch_keys",
        (0..2200).chain(0..2200).map(|id| vec![Value::Int(id)]),
    );
    let QueryResponse::ResultSet(result)=engine.execute("SELECT COUNT(*) FROM batch_numbers WHERE tenant=1 AND id IN (SELECT id FROM batch_keys)").unwrap() else {panic!("rows")};
    assert_eq!(result.rows, vec![vec![Value::Int(2100)]]);
}
