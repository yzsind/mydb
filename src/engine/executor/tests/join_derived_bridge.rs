//! Regression: inline derived tables in JOIN positions must take the CBO
//! planner path (a `CteScan` leaf joined via hash / index-nested-loop), not
//! the index-blind in-memory nested-loop fallback.
//!
//! Before the `bridge_join_derived_to_ctes` fix, any query whose join list
//! held an inline `(SELECT ...)` routed to `execute_select_with_cte_joins`,
//! which fully materialised every base table and joined with naive nested
//! loops — no index access, no join reordering. Comma-joined against a large
//! fact table this is catastrophic (TPC-DS Q8: 3.7 s vs ~0.12 s for the same
//! query written with a `WITH` CTE). The derived alias was also unresolvable
//! during `EXPLAIN FORMAT=TREE` (1146 "table '<alias>' does not exist").
//!
//! These tests pin both halves: the result matches the equivalent base-table
//! join (the oracle), and the plan now shows a `CteScan` joined by a real
//! join operator instead of erroring.

use super::*;

fn rows_of(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(r) => r.rows,
        other => panic!("expected result set, got {other:?}"),
    }
}

/// `dd` (date-like dim), `fact` (indexed on the dim key), `addr` (zip source).
/// Mirrors TPC-DS Q8: a fact table comma-joined with a dim and a derived
/// table, where the derived joins on a `SUBSTR` expression key.
fn setup() -> Vec<&'static str> {
    vec![
        "CREATE TABLE dd (d_id INT PRIMARY KEY, yr INT)",
        "CREATE TABLE fact (f_id INT PRIMARY KEY, d_id INT, zip VARCHAR(5), amt INT)",
        "CREATE INDEX idx_fact_d ON fact (d_id)",
        "CREATE TABLE addr (a_id INT PRIMARY KEY, zip VARCHAR(5), flag CHAR(1))",
        // dim: only yr=2000 (d_id=1) qualifies; d_id=2 is yr=1999.
        "INSERT INTO dd VALUES (1, 2000), (2, 1999)",
        // facts: 1,2,3 are in the 2000 dim; 4 is in the 1999 dim (filtered out).
        "INSERT INTO fact VALUES (1, 1, '37845', 10), (2, 1, '37999', 20), \
         (3, 1, '65000', 5), (4, 2, '37111', 99)",
        // addr: flag='Y' keeps prefixes 37 and 65; the 'N' row is filtered out.
        "INSERT INTO addr VALUES (1, '37000', 'Y'), (2, '65111', 'N'), (3, '65222', 'Y')",
        // A materialised equivalent of the derived body, used as the oracle.
        "CREATE TABLE addr_y AS SELECT zip FROM addr WHERE flag = 'Y'",
    ]
}

const DERIVED_QUERY: &str = "SELECT f.zip, SUM(f.amt) AS s \
     FROM fact f, dd, (SELECT zip FROM addr WHERE flag = 'Y') v \
     WHERE f.d_id = dd.d_id AND dd.yr = 2000 \
       AND SUBSTR(f.zip, 1, 2) = SUBSTR(v.zip, 1, 2) \
     GROUP BY f.zip ORDER BY f.zip";

const REAL_TABLE_QUERY: &str = "SELECT f.zip, SUM(f.amt) AS s \
     FROM fact f, dd, addr_y v \
     WHERE f.d_id = dd.d_id AND dd.yr = 2000 \
       AND SUBSTR(f.zip, 1, 2) = SUBSTR(v.zip, 1, 2) \
     GROUP BY f.zip ORDER BY f.zip";

#[test]
fn derived_in_comma_join_matches_base_table_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for s in setup() {
        engine.execute(s).unwrap();
    }

    let derived = rows_of(engine.execute(DERIVED_QUERY).unwrap());
    let oracle = rows_of(engine.execute(REAL_TABLE_QUERY).unwrap());

    // Same answer as the equivalent base-table join.
    assert_eq!(
        derived, oracle,
        "derived-in-join diverged from base-table join"
    );

    // And the concrete expected rows (zip groups in the 2000 dim, prefixes
    // matched against the flag='Y' zips): 37845→10, 37999→20, 65000→5.
    assert_eq!(derived.len(), 3, "rows: {derived:?}");
    let zips: Vec<&str> = derived
        .iter()
        .map(|r| crate::types::value_str(&r[0]).expect("zip should be a string value"))
        .collect();
    assert_eq!(zips, ["37845", "37999", "65000"]);
}

#[test]
fn derived_in_plain_equi_join_matches_oracle() {
    // The plain `key = key` derived join shape (no SUBSTR) — the worst case
    // for the old in-memory path. Verifies it too goes through the planner
    // and returns the indexed-join result.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for s in setup() {
        engine.execute(s).unwrap();
    }

    let derived = rows_of(
        engine
            .execute(
                "SELECT f.zip, f.amt FROM fact f, (SELECT d_id FROM dd WHERE yr = 2000) v \
                 WHERE f.d_id = v.d_id ORDER BY f.zip",
            )
            .unwrap(),
    );
    let oracle = rows_of(
        engine
            .execute(
                "SELECT f.zip, f.amt FROM fact f JOIN dd ON f.d_id = dd.d_id \
                 WHERE dd.yr = 2000 ORDER BY f.zip",
            )
            .unwrap(),
    );
    assert_eq!(derived, oracle);
    assert_eq!(derived.len(), 3, "rows: {derived:?}");
}

#[test]
fn explain_tree_lowers_join_derived_to_cte_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for s in setup() {
        engine.execute(s).unwrap();
    }

    // Previously this raised 1146 ("table 'v' does not exist"); now it plans.
    let QueryResponse::ResultSet(result) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {DERIVED_QUERY}"))
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let tree = match &result.rows[0][0] {
        Value::String(text) => text.clone(),
        other => panic!("expected tree text, got {other:?}"),
    };

    // The derived table is a CteScan leaf...
    assert!(
        tree.contains("CTE scan"),
        "derived table should lower to a CTE scan, got:\n{tree}"
    );
    // ...joined by a real join operator (hash / nested-loop), proving the
    // query went through the CBO planner rather than the in-memory fallback.
    assert!(
        tree.contains("Hash join") || tree.contains("nested loop join"),
        "plan should use a CBO join operator, got:\n{tree}"
    );
    // The substr equi-predicate is recognised as a join key on the derived
    // side (the in-memory fallback could not express this — it would degrade
    // to a post-filtered cartesian).
    assert!(
        tree.contains("keys: SUBSTRING(f.zip,1,2)=SUBSTRING(v.zip,1,2)"),
        "derived table should join on the substr key, got:\n{tree}"
    );
}

fn shared_group_query(left: &str, right: &str) -> String {
    format!("SELECT m.g,d.k,d.revenue,m.ave FROM (SELECT g,AVG(revenue) ave FROM ({left}) a GROUP BY g) m JOIN ({right}) d ON m.g=d.g WHERE d.revenue <= m.ave ORDER BY m.g,d.k")
}

#[test]
fn repeated_grouped_derived_shares_one_statement_spool_and_survives_spill() {
    use crate::engine::executor::select::SHARED_DERIVED_AGGREGATE_HITS;
    use std::sync::atomic::Ordering::Relaxed;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE shared_fact (g INT,k INT,v DECIMAL(18,2),bucket INT)")
        .unwrap();
    engine.execute("INSERT INTO shared_fact SELECT MOD(n,8),MOD(n,29),CASE WHEN MOD(n,11)=0 THEN NULL ELSE MOD(n,53) END,MOD(n,3) FROM generate_series(0,1199) gs(n)").unwrap();
    let leaf =
        "SELECT g,k,SUM(v) revenue FROM shared_fact WHERE bucket BETWEEN 1 AND 1+1 GROUP BY g,k";
    let sql = shared_group_query(leaf, leaf);
    let rows = |sql: &str| rows_of(engine.execute(sql).unwrap().materialize().unwrap());
    let expected =
        crate::util::env::with_vars(&[("MYDB_SHARED_DERIVED_AGGREGATE", Some("off"))], || {
            rows(&sql)
        });
    for spill in ["0", "64"] {
        crate::util::env::with_vars(&[("MYDB_CTE_SPILL_THRESHOLD_MB", Some(spill))], || {
            let before = SHARED_DERIVED_AGGREGATE_HITS.load(Relaxed);
            assert_eq!(rows(&sql), expected);
            assert_eq!(
                SHARED_DERIVED_AGGREGATE_HITS.load(Relaxed) - before,
                1,
                "expected a single shared producer"
            );
            let plan = rows(&format!("EXPLAIN ANALYZE {sql}"))[0][0].to_display_string();
            assert!(plan.contains("__mydb_shared_agg_"), "{plan}");
        });
    }
    // Re-execution after a write must not reuse an earlier statement's data.
    engine
        .execute("INSERT INTO shared_fact VALUES (0,1,999.99,1)")
        .unwrap();
    let expected =
        crate::util::env::with_vars(&[("MYDB_SHARED_DERIVED_AGGREGATE", Some("off"))], || {
            rows(&sql)
        });
    assert_eq!(rows(&sql), expected);
}

#[test]
fn shared_aggregate_parameters_compare_values_not_placeholder_positions() {
    use crate::engine::executor::select::SHARED_DERIVED_AGGREGATE_HITS;
    use std::sync::atomic::Ordering::Relaxed;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE shared_params (g INT,k INT,v INT,bucket INT)")
        .unwrap();
    engine.execute("INSERT INTO shared_params VALUES (1,1,10,1),(1,2,30,1),(1,1,90,2),(1,2,20,2),(2,1,NULL,1)").unwrap();
    engine
        .execute_with_session(
            &mut session,
            &format!(
                "PREPARE shared_stmt FROM '{}'",
                shared_group_query(
                    "SELECT g,k,SUM(v) revenue FROM shared_params WHERE bucket=? GROUP BY g,k",
                    "SELECT g,k,SUM(v) revenue FROM shared_params WHERE bucket=? GROUP BY g,k"
                )
            ),
        )
        .unwrap();
    for (left, right) in [(1, 1), (1, 2), (2, 2), (2, 1), (1, 1)] {
        engine
            .execute_with_session(&mut session, &format!("SET @l={left},@r={right}"))
            .unwrap();
        let before = SHARED_DERIVED_AGGREGATE_HITS.load(Relaxed);
        let actual = rows_of(
            engine
                .execute_with_session(&mut session, "EXECUTE shared_stmt USING @l,@r")
                .unwrap()
                .materialize()
                .unwrap(),
        );
        assert_eq!(
            SHARED_DERIVED_AGGREGATE_HITS.load(Relaxed) - before,
            u64::from(left == right)
        );
        let sql = shared_group_query(
            &format!(
                "SELECT g,k,SUM(v) revenue FROM shared_params WHERE bucket={left} GROUP BY g,k"
            ),
            &format!(
                "SELECT g,k,SUM(v) revenue FROM shared_params WHERE bucket={right} GROUP BY g,k"
            ),
        );
        let expected =
            crate::util::env::with_vars(&[("MYDB_SHARED_DERIVED_AGGREGATE", Some("off"))], || {
                rows_of(engine.execute(&sql).unwrap().materialize().unwrap())
            });
        assert_eq!(actual, expected);
    }
}

#[test]
fn shared_aggregate_rejects_warning_producing_arguments_and_volatile_scopes() {
    use crate::engine::executor::select::SHARED_DERIVED_AGGREGATE_HITS;
    use std::sync::atomic::Ordering::Relaxed;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE unsafe_shared (g INT,k INT,v VARCHAR(20))")
        .unwrap();
    engine
        .execute("INSERT INTO unsafe_shared VALUES (1,1,'bad'),(1,2,'3')")
        .unwrap();
    for leaf in [
        "SELECT g,k,SUM(v) revenue FROM unsafe_shared GROUP BY g,k",
        "SELECT g,k,SUM(RAND()) revenue FROM unsafe_shared GROUP BY g,k",
        "SELECT g,k,COUNT(*) revenue FROM unsafe_shared GROUP BY g,k LIMIT 1",
        "SELECT g,k,SUM(CAST(v AS SIGNED)) revenue FROM unsafe_shared GROUP BY g,k",
    ] {
        let sql = shared_group_query(leaf, leaf);
        let before = SHARED_DERIVED_AGGREGATE_HITS.load(Relaxed);
        let _ = engine.execute(&sql);
        assert_eq!(SHARED_DERIVED_AGGREGATE_HITS.load(Relaxed), before, "{sql}");
    }
}

#[test]
fn shared_aggregate_preserves_exact_types_nulls_aliases_and_transaction_writes() {
    use crate::engine::executor::select::SHARED_DERIVED_AGGREGATE_HITS;
    use std::sync::atomic::Ordering::Relaxed;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE share_types (g DATE,k BIGINT UNSIGNED,v DECIMAL(36,4))")
        .unwrap();
    engine.execute("INSERT INTO share_types VALUES ('2024-01-01',18446744073709551615,1.2500),('2024-01-01',18446744073709551615,NULL),(NULL,0,-2),(NULL,0,2),('2024-01-02',NULL,123456789012345678901234567890.1250),('2024-01-03',7,NULL)").unwrap();
    let leaf = "SELECT g,k,COUNT(*) n,COUNT(v) nn,SUM(v) s,AVG(v) a,MIN(v) lo,MAX(v) hi FROM share_types GROUP BY g,k";
    let sql = format!("SELECT __mydb_shared_agg_0.g,__mydb_shared_agg_0.k,__mydb_shared_agg_0.n,b.nn,__mydb_shared_agg_0.s,b.a,__mydb_shared_agg_0.lo,b.hi FROM ({leaf}) __mydb_shared_agg_0 JOIN ({leaf}) b ON __mydb_shared_agg_0.g<=>b.g AND __mydb_shared_agg_0.k<=>b.k ORDER BY __mydb_shared_agg_0.g,__mydb_shared_agg_0.k");
    let mut run = |enabled| {
        crate::util::env::with_vars(&[("MYDB_SHARED_DERIVED_AGGREGATE", Some(enabled))], || {
            rows_of(
                engine
                    .execute_with_session(&mut session, &sql)
                    .unwrap()
                    .materialize()
                    .unwrap(),
            )
        })
    };
    let before = SHARED_DERIVED_AGGREGATE_HITS.load(Relaxed);
    let initial = run("on");
    assert_eq!(SHARED_DERIVED_AGGREGATE_HITS.load(Relaxed) - before, 1);
    assert_eq!(initial, run("off"));
    let plan = rows_of(
        engine
            .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
            .unwrap(),
    )[0][0]
        .to_display_string();
    assert!(
        plan.contains("__mydb_shared_agg_1"),
        "generated name collided with an SQL alias: {plan}"
    );
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO share_types VALUES ('2024-01-01',18446744073709551615,100)",
        )
        .unwrap();
    let updated = rows_of(
        engine
            .execute_with_session(&mut session, &sql)
            .unwrap()
            .materialize()
            .unwrap(),
    );
    assert_ne!(updated, initial);
    let expected =
        crate::util::env::with_vars(&[("MYDB_SHARED_DERIVED_AGGREGATE", Some("off"))], || {
            rows_of(
                engine
                    .execute_with_session(&mut session, &sql)
                    .unwrap()
                    .materialize()
                    .unwrap(),
            )
        });
    assert_eq!(updated, expected);
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        rows_of(
            engine
                .execute_with_session(&mut session, &sql)
                .unwrap()
                .materialize()
                .unwrap()
        ),
        initial
    );
}
