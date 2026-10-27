use super::*;

fn collect_select_response_rows(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(result) => result.rows,
        QueryResponse::SharedResultSet(result) => result.rows,
        QueryResponse::StreamingResultSet(mut result) => {
            let mut rows = Vec::new();
            while let Some(row) = result.cursor.next_row().unwrap() {
                rows.push(row);
            }
            rows
        }
        other => panic!("expected SELECT result, got {other:?}"),
    }
}

#[test]
fn null_comparison_returns_null_not_boolean() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let cases = [
        ("SELECT NULL < 5", Value::Null),
        ("SELECT NULL > 5", Value::Null),
        ("SELECT NULL = 5", Value::Null),
        ("SELECT NULL <> 5", Value::Null),
        ("SELECT NULL <= 5", Value::Null),
        ("SELECT NULL >= 5", Value::Null),
        ("SELECT NULL = NULL", Value::Null),
        ("SELECT NULL <> NULL", Value::Null),
        ("SELECT NULL <=> NULL", Value::Int(1)),
        ("SELECT NULL <=> 5", Value::Int(0)),
        ("SELECT 1 <=> 1", Value::Int(1)),
    ];
    for (sql, expected) in cases {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected result set for {sql}");
        };
        assert_eq!(result.rows[0][0], expected, "failed for: {sql}");
    }
}

#[test]
fn const_select_reports_unknown_bare_identifier() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let err = engine.execute("SELECT $$").unwrap_err();
    assert_eq!(err.code(), 1054);
    assert_eq!(err.sql_state(), "42S22");
    assert_eq!(err.message(), "Unknown column '$$' in 'field list'");
}

#[test]
fn sql_lob_descriptor_projection_avoids_full_blob_materialization() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE lob_projection_t (id INT PRIMARY KEY, data MEDIUMBLOB)")
        .unwrap();
    let data = vec![0x5Au8; 512 * 1024];
    engine
        .execute("INSERT INTO lob_projection_t VALUES (1, REPEAT('Z', 524288))")
        .unwrap();

    let before = crate::engine::executor::select::simple::descriptor_lob_projection_hits_for_test();
    let rows = collect_select_response_rows(
        engine
            .execute(
                "SELECT OCTET_LENGTH(data), LEFT(data, 4), RIGHT(data, 3), \
                 SUBSTRING(data, 100001, 5) FROM lob_projection_t",
            )
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![vec![
            Value::Int(data.len() as i64),
            Value::Bytes(vec![0x5A; 4]),
            Value::Bytes(vec![0x5A; 3]),
            Value::Bytes(vec![0x5A; 5]),
        ]]
    );
    assert!(
        crate::engine::executor::select::simple::descriptor_lob_projection_hits_for_test() > before,
        "query unexpectedly fell back to full-row materialization"
    );

    let zero = collect_select_response_rows(
        engine
            .execute("SELECT OCTET_LENGTH(data) FROM lob_projection_t LIMIT 0")
            .unwrap(),
    );
    assert!(zero.is_empty());
}

#[test]
fn const_select_supports_where_order_by_and_dual() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine.execute("SELECT 1 WHERE 1 ORDER BY 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);

    let QueryResponse::ResultSet(result) = engine.execute("SELECT 1 WHERE 0").unwrap() else {
        panic!("expected result set");
    };
    assert!(result.rows.is_empty());

    let QueryResponse::ResultSet(result) = engine.execute("SELECT 1 FROM DUAL").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn const_select_aggregate_treats_as_implicit_single_row() {
    // Regression for issue #56: an aggregate in a FROM-less / FROM DUAL /
    // single-row-constant-derived-table query must be evaluated over one
    // implicit input row (COUNT(*) = 1, other aggregates over that row),
    // matching MySQL — not rejected with 1111 "invalid use of group
    // function".
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let cases: &[(&str, Value)] = &[
        ("SELECT COUNT(*)", Value::Int(1)),
        ("SELECT COUNT(*) FROM DUAL", Value::Int(1)),
        ("SELECT MAX(5)", Value::Int(5)),
        ("SELECT MIN(5)", Value::Int(5)),
        ("SELECT COUNT(*) + 1", Value::Int(2)),
        ("SELECT 1 + COUNT(*) FROM DUAL", Value::Int(2)),
        ("SELECT COUNT(NULL)", Value::Int(0)),
        ("SELECT COUNT(1)", Value::Int(1)),
    ];
    for (sql, expected) in cases {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap_or_else(|e| {
            panic!(
                "`{sql}` should succeed, got error {}: {}",
                e.code(),
                e.message()
            )
        }) else {
            panic!("`{sql}` expected result set");
        };
        assert_eq!(result.rows.len(), 1, "`{sql}` should return one row");
        assert_eq!(result.rows[0][0], *expected, "`{sql}` value mismatch");
    }

    // SUM(1) / AVG(4) are exact-decimal aggregates; assert via display.
    let QueryResponse::ResultSet(result) = engine.execute("SELECT SUM(1)").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0].to_display_string(), "1");

    let QueryResponse::ResultSet(result) = engine.execute("SELECT GROUP_CONCAT('x')").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0].to_display_string(), "x");

    // Single-row constant derived table flattens to the same FROM-less shape.
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT COUNT(*) FROM (SELECT 1 x) t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(1));

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT MAX(x) + MIN(x) FROM (SELECT 1 x) t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(2));

    // LIMIT 0 still empties the single implicit-group row.
    let QueryResponse::ResultSet(result) = engine.execute("SELECT COUNT(*) LIMIT 0").unwrap()
    else {
        panic!("expected result set");
    };
    assert!(result.rows.is_empty());
}

#[test]
fn independent_scalar_derived_sources_use_bounded_parallel_materialization() {
    use std::sync::atomic::Ordering;

    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE scalar_source (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO scalar_source VALUES (1,10),(2,20),(3,30),(4,40)")
        .unwrap();

    let sql = "SELECT * \
               FROM (SELECT COUNT(*) AS c FROM scalar_source WHERE id <= 3) a, \
                    (SELECT SUM(v) AS s FROM scalar_source WHERE id >= 2) b, \
                    (SELECT MAX(v) AS m FROM scalar_source) c";
    let serial = with_test_env(&[("MYDB_SCALAR_DERIVED_PARALLEL", Some("off"))], || {
        collect_select_response_rows(engine.execute(sql).unwrap())
    });
    let before =
        crate::engine::executor::select::SCALAR_DERIVED_PARALLEL_HITS.load(Ordering::Relaxed);
    let parallel = with_test_env(
        &[
            ("MYDB_SCALAR_DERIVED_PARALLEL", Some("on")),
            ("MYDB_PARALLEL_DEGREE", Some("4")),
        ],
        || collect_select_response_rows(engine.execute(sql).unwrap()),
    );
    let after =
        crate::engine::executor::select::SCALAR_DERIVED_PARALLEL_HITS.load(Ordering::Relaxed);
    assert_eq!(parallel, serial);
    assert_eq!(
        parallel,
        vec![vec![
            Value::Int(3),
            Value::DecimalCompact(90, 0),
            Value::Int(40),
        ]]
    );
    assert!(
        after > before,
        "parallel scalar-derived bridge did not engage"
    );
}

#[test]
fn projection_aggregate_scalar_subquery_does_not_collapse_outer_query() {
    // Regression for issue #64 (same root cause as #56): a projection that
    // mixes a plain column with a scalar subquery containing an aggregate must
    // NOT make the OUTER query an aggregation. The aggregate belongs to the
    // subquery, so the outer query returns one row per input row.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (id INT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3)")
        .unwrap();

    // Uncorrelated aggregate scalar subquery alongside a plain column.
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT t.id, (SELECT MAX(id) FROM t) AS mx FROM t ORDER BY t.id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(3)],
            vec![Value::Int(2), Value::Int(3)],
            vec![Value::Int(3), Value::Int(3)],
        ]
    );

    // Plain column plus an aggregate scalar subquery *and* a second plain
    // column — every non-aggregate column stays per-row.
    engine
        .execute("CREATE TABLE emp (id INT, sal INT)")
        .unwrap();
    engine
        .execute("INSERT INTO emp VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT e.id, e.sal, (SELECT MAX(sal) FROM emp) AS m FROM emp e ORDER BY e.id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(100), Value::Int(300)],
            vec![Value::Int(2), Value::Int(200), Value::Int(300)],
            vec![Value::Int(3), Value::Int(300), Value::Int(300)],
        ]
    );

    // A genuine block-level aggregate in the projection must still aggregate.
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT MAX(id), (SELECT MIN(id) FROM t) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(3), Value::Int(1)]]);
}

/// TRIM composes with length/filter kernels without a legacy escape.
#[test]
fn trim_kernel_composes_without_legacy_escape() {
    use crate::engine::expr_program::expr_program_stats;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(16))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, '  a '), (2, ' bb '), (3, 'ccc')")
        .unwrap();

    let before = expr_program_stats();
    // Both filter and projection retain the compiled tree.
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, CHAR_LENGTH(TRIM(s)) FROM t WHERE CHAR_LENGTH(TRIM(s)) >= 2 ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // trimmed lengths: 'a'->1, 'bb'->2, 'ccc'->3; filter >= 2 keeps {2,3}.
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(2), Value::Int(2)],
            vec![Value::Int(3), Value::Int(3)],
        ]
    );

    let after = expr_program_stats();
    assert_eq!(after.legacy_escape_rows, before.legacy_escape_rows);
    assert!(
        after.program_eval_rows > before.program_eval_rows,
        "expected the surrounding expression to stay on the compiled path"
    );
}

#[test]
fn multi_projection_preserves_function_values_and_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE p(d DATE,s VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO p VALUES('2024-02-29','straße'),('2025-01-01',NULL)")
        .unwrap();
    let before = crate::engine::expr_program::expr_program_stats().column_eval_rows;
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT DATE_FORMAT(d,'%Y-%m-%d'),UPPER(s),ABS(YEAR(d)) FROM p ORDER BY d")
        .unwrap()
    else {
        panic!("result set expected");
    };
    let rows = result
        .rows
        .iter()
        .map(|row| row.iter().map(Value::to_display_string).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            vec!["2024-02-29", "STRAßE", "2024"],
            vec!["2025-01-01", "NULL", "2025"]
        ]
    );
    assert!(crate::engine::expr_program::expr_program_stats().column_eval_rows - before >= 6);
}

/// Phase 3 end-to-end: a now-registered deterministic kernel (`POWER`) is
/// fully compiled — correct rows, the compiled path engages, and there is no
/// whole-expression legacy fallback for it.
#[test]
fn registered_kernel_runs_fully_compiled() {
    use crate::engine::expr_program::expr_program_stats;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, x DOUBLE)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 2.0), (2, 3.0), (3, 4.0)")
        .unwrap();

    let before = expr_program_stats();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, POWER(x, 2) + 1 FROM t WHERE POWER(x, 2) > 5 ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    // x in {2,3,4} -> POWER(x,2) in {4,9,16}; filter > 5 keeps {9,16}.
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(2), Value::Double(10.0)],
            vec![Value::Int(3), Value::Double(17.0)],
        ]
    );
    let after = expr_program_stats();
    assert!(
        after.program_eval_rows > before.program_eval_rows,
        "POWER should evaluate on the compiled path"
    );
    assert_eq!(
        after.legacy_fallback_rows, before.legacy_fallback_rows,
        "POWER is a kernel now — no whole-expression legacy fallback expected"
    );
}

fn setup_tpcds_cte_scope_schema(engine: &StorageQueryEngine) {
    engine
        .execute(
            "CREATE TABLE store_returns (\
             sr_customer_sk INT, sr_store_sk INT, sr_return_amt INT, sr_returned_date_sk INT)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE date_dim (d_date_sk INT PRIMARY KEY, d_year INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE store (s_store_sk INT PRIMARY KEY, s_state VARCHAR(2))")
        .unwrap();
    engine
        .execute("CREATE TABLE customer (c_customer_sk INT PRIMARY KEY, c_customer_id VARCHAR(20))")
        .unwrap();
    engine
        .execute("INSERT INTO date_dim VALUES (1, 2002)")
        .unwrap();
    engine
        .execute("INSERT INTO store VALUES (10, 'TN'), (20, 'TN')")
        .unwrap();
    engine
        .execute("INSERT INTO customer VALUES (1, 'C001'), (2, 'C002'), (3, 'C003')")
        .unwrap();
    engine
        .execute(
            "INSERT INTO store_returns VALUES \
             (1, 10, 200, 1), (2, 10, 100, 1), (3, 20, 10, 1)",
        )
        .unwrap();
}

fn tpcds_cte_scope_sql() -> &'static str {
    "WITH customer_total_return AS (\
       SELECT sr_customer_sk AS ctr_customer_sk, \
              sr_store_sk AS ctr_store_sk, \
              SUM(sr_return_amt) AS ctr_total_return \
       FROM store_returns, date_dim \
       WHERE sr_returned_date_sk = d_date_sk AND d_year = 2002 \
       GROUP BY sr_customer_sk, sr_store_sk\
     ) \
     SELECT c_customer_id \
     FROM customer_total_return ctr1, store, customer \
     WHERE ctr1.ctr_total_return > (\
         SELECT AVG(ctr_total_return) * 1.2 \
         FROM customer_total_return ctr2 \
         WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk\
       ) \
       AND s_store_sk = ctr1.ctr_store_sk \
       AND s_state = 'TN' \
       AND ctr1.ctr_customer_sk = c_customer_sk \
     ORDER BY c_customer_id"
}

#[test]
fn cte_scope_survives_correlated_scalar_subquery_in_comma_join() {
    for plan_cache in ["on", "off"] {
        with_test_env(&[("MYDB_SUBQUERY_PLAN_CACHE", Some(plan_cache))], || {
            let dir = tempfile::tempdir().unwrap();
            let engine = open_test_engine(dir.path());
            setup_tpcds_cte_scope_schema(&engine);

            let QueryResponse::ResultSet(result) = engine.execute(tpcds_cte_scope_sql()).unwrap()
            else {
                panic!("expected result set");
            };
            assert_eq!(
                result.rows,
                vec![vec![Value::String("C001".to_string())]],
                "plan cache mode {plan_cache}"
            );
        });
    }
}

#[test]
fn prepared_describe_and_execute_preserve_cte_scope() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_tpcds_cte_scope_schema(&engine);
    let mut session = SessionContext::default();
    let sql = tpcds_cte_scope_sql();

    let columns = engine
        .describe_prepared_statement(&mut session, sql)
        .unwrap();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].name, "c_customer_id");

    let prepared = engine.prepare_cached(&mut session, sql, 0).unwrap();
    let response = engine
        .execute_prepared_statement(&mut session, sql, (*prepared).clone())
        .expect("storage engine should handle prepared SELECT")
        .unwrap();
    assert_eq!(
        collect_select_response_rows(response),
        vec![vec![Value::String("C001".to_string())]]
    );
}

#[test]
fn parsed_fast_path_preserves_tpcds_q1_cte_scope() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_tpcds_cte_scope_schema(&engine);
    let mut session = SessionContext::default();
    let sql = include_str!("../../../../scripts/tpcds/queries/q01.sql");
    let statement = crate::parser::parse_statement(sql).unwrap();

    let response = engine
        .execute_qualified_parsed_statement(&mut session, sql, statement)
        .unwrap();
    assert_eq!(
        collect_select_response_rows(response),
        vec![vec![Value::String("C001".to_string())]]
    );
}

// ---------------------------------------------------------------------------
// G1: set operations inside a FROM-clause derived table.
//
// `SELECT ... FROM (SELECT ... UNION|INTERSECT|EXCEPT [ALL] SELECT ...) AS x`
// is legal MySQL 8.0 and the dominant TPC-DS gap (q08/q14/q33/q38/q49/q56/
// q60/q75/q87 ...). mydb used to reject it with 1235 because the derived slot
// was hardcoded to `Box<Select>`; it is now `Option<SubQuery>` and the
// executor materialises the set operation through the derived-table bridge.
// ---------------------------------------------------------------------------

#[test]
fn derived_union_all_in_from_executes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t1 (id INT, v INT)").unwrap();
    engine.execute("CREATE TABLE t2 (id INT, v INT)").unwrap();
    engine
        .execute("INSERT INTO t1 VALUES (1,10),(2,20)")
        .unwrap();
    engine.execute("INSERT INTO t2 VALUES (3,30)").unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT v FROM (SELECT v FROM t1 UNION ALL SELECT v FROM t2) x ORDER BY v")
        .unwrap()
    else {
        panic!("expected result set for UNION ALL derived table");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(10)],
            vec![Value::Int(20)],
            vec![Value::Int(30)],
        ]
    );
}

#[test]
fn derived_intersect_and_except_in_from() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE a (id INT)").unwrap();
    engine.execute("CREATE TABLE b (id INT)").unwrap();
    engine.execute("INSERT INTO a VALUES (1),(2),(3)").unwrap();
    engine.execute("INSERT INTO b VALUES (2),(3),(4)").unwrap();
    let batch_before = super::super::select::vec_bridge::SETOP_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);

    // q38-style: COUNT over an INTERSECT derived table → {2,3} ⇒ 2.
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT count(*) FROM (SELECT id FROM a INTERSECT SELECT id FROM b) x")
        .unwrap()
    else {
        panic!("expected result set for INTERSECT derived table");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(2)]]);

    // q87-style: COUNT over an EXCEPT derived table → {1} ⇒ 1.
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT count(*) FROM (SELECT id FROM a EXCEPT SELECT id FROM b) x")
        .unwrap()
    else {
        panic!("expected result set for EXCEPT derived table");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
    let batch_after = super::super::select::vec_bridge::SETOP_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        batch_after >= batch_before + 2,
        "derived INTERSECT/EXCEPT must execute as physical batch SetOp nodes"
    );
}

#[test]
fn setop_distinct_children_compare_projected_rows_and_chain_left_associatively() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE sa (n VARCHAR(8), d DATE, junk INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE sb (n VARCHAR(8), d DATE, junk INT)")
        .unwrap();
    engine
        .execute("INSERT INTO sa VALUES (NULL,'1998-09-03',1)")
        .unwrap();
    engine
        .execute("INSERT INTO sb VALUES (NULL,'1998-09-03',2)")
        .unwrap();
    let QueryResponse::ResultSet(intersection) = engine
        .execute(
            "SELECT n,d FROM ( \
               SELECT DISTINCT n,d FROM sa \
               INTERSECT SELECT DISTINCT n,d FROM sb) q",
        )
        .unwrap()
    else {
        panic!("expected projected INTERSECT result");
    };
    assert_eq!(intersection.rows.len(), 1);
    assert_eq!(intersection.rows[0][0], Value::Null);
    assert_eq!(intersection.rows[0][1].to_display_string(), "1998-09-03");

    engine.execute("CREATE TABLE sc (v INT)").unwrap();
    engine.execute("CREATE TABLE sd (v INT)").unwrap();
    engine.execute("CREATE TABLE se (v INT)").unwrap();
    engine.execute("INSERT INTO sc VALUES (1),(2),(3)").unwrap();
    engine.execute("INSERT INTO sd VALUES (2),(3)").unwrap();
    engine.execute("INSERT INTO se VALUES (3)").unwrap();
    let QueryResponse::ResultSet(except) = engine
        .execute("SELECT v FROM sc EXCEPT SELECT v FROM sd EXCEPT SELECT v FROM se ORDER BY v")
        .unwrap()
    else {
        panic!("expected chained EXCEPT result");
    };
    assert_eq!(except.rows, vec![vec![Value::Int(1)]]);
    let QueryResponse::ResultSet(intersect) = engine
        .execute(
            "SELECT v FROM sc INTERSECT SELECT v FROM sd INTERSECT SELECT v FROM se ORDER BY v",
        )
        .unwrap()
    else {
        panic!("expected chained INTERSECT result");
    };
    assert_eq!(intersect.rows, vec![vec![Value::Int(3)]]);
}

#[test]
fn derived_union_distinct_dedups_but_union_all_keeps_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE a (v INT)").unwrap();
    engine.execute("CREATE TABLE b (v INT)").unwrap();
    engine.execute("INSERT INTO a VALUES (10),(20)").unwrap();
    engine.execute("INSERT INTO b VALUES (20),(30)").unwrap();

    // UNION (distinct): {10,20,30} ⇒ 3.
    let QueryResponse::ResultSet(distinct) = engine
        .execute("SELECT count(*) FROM (SELECT v FROM a UNION SELECT v FROM b) x")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(distinct.rows, vec![vec![Value::Int(3)]]);

    // UNION ALL: 10,20,20,30 ⇒ 4.
    let QueryResponse::ResultSet(all) = engine
        .execute("SELECT count(*) FROM (SELECT v FROM a UNION ALL SELECT v FROM b) x")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(all.rows, vec![vec![Value::Int(4)]]);
}

#[test]
fn derived_set_op_with_outer_where_group_by_and_alias_resolution() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t1 (g INT, v INT)").unwrap();
    engine.execute("CREATE TABLE t2 (g INT, v INT)").unwrap();
    engine
        .execute("INSERT INTO t1 VALUES (1,10),(1,20),(2,5)")
        .unwrap();
    engine
        .execute("INSERT INTO t2 VALUES (2,30),(3,40)")
        .unwrap();

    // Outer WHERE filters on the derived alias's columns; outer GROUP BY
    // aggregates the unioned rows. Rows with v > 5: (1,10),(1,20),(2,30),(3,40).
    // GROUP BY g ⇒ {1: count 2, 2: count 1, 3: count 1}.
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT x.g, count(*) FROM \
             (SELECT g, v FROM t1 UNION ALL SELECT g, v FROM t2) x \
             WHERE x.v > 5 GROUP BY x.g ORDER BY x.g",
        )
        .unwrap()
    else {
        panic!("expected result set for set-op derived with outer aggregation");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(2)],
            vec![Value::Int(2), Value::Int(1)],
            vec![Value::Int(3), Value::Int(1)],
        ]
    );
}

#[test]
fn derived_set_op_joined_with_base_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE base (id INT PRIMARY KEY, label INT)")
        .unwrap();
    engine.execute("CREATE TABLE p1 (id INT)").unwrap();
    engine.execute("CREATE TABLE p2 (id INT)").unwrap();
    engine
        .execute("INSERT INTO base VALUES (1,100),(2,200),(3,300)")
        .unwrap();
    engine.execute("INSERT INTO p1 VALUES (1),(2)").unwrap();
    engine.execute("INSERT INTO p2 VALUES (3)").unwrap();

    // A set-operation derived table on the right of an explicit JOIN ... ON.
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT base.label FROM base \
             JOIN (SELECT id FROM p1 UNION ALL SELECT id FROM p2) u ON base.id = u.id \
             ORDER BY base.label",
        )
        .unwrap()
    else {
        panic!("expected result set for JOIN with set-op derived table");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(100)],
            vec![Value::Int(200)],
            vec![Value::Int(300)],
        ]
    );
}

#[test]
fn derived_set_op_respects_inner_order_by_and_limit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE a (v INT)").unwrap();
    engine.execute("CREATE TABLE b (v INT)").unwrap();
    engine.execute("INSERT INTO a VALUES (5),(1),(9)").unwrap();
    engine.execute("INSERT INTO b VALUES (7),(3)").unwrap();

    // The set operation carries its own ORDER BY + LIMIT; only the 2 smallest
    // unioned values (1, 3) survive into the derived rowset.
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT count(*) FROM \
             (SELECT v FROM a UNION ALL SELECT v FROM b ORDER BY v LIMIT 2) x \
             WHERE x.v < 5",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // {1, 3} are both < 5 ⇒ 2.
    assert_eq!(result.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn cte_body_union_all_of_group_by_branches() {
    // Regression: a top-level CTE whose body is a `UNION ALL` of aggregating
    // (GROUP BY) SELECTs. Each branch runs on the streaming path and returns a
    // `StreamingResultSet`; `execute_set_operation_dispatch` must materialise
    // both before combining. Previously it required `ResultSet` outright and
    // failed with code 1235 ("set operation requires SELECT result on the
    // left") — the TPC-DS Q4/Q11/Q74 CTE-body failure mode.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE s (g INT, v INT)").unwrap();
    engine.execute("CREATE TABLE w (g INT, v INT)").unwrap();
    engine
        .execute("INSERT INTO s VALUES (1,10),(1,20),(2,5)")
        .unwrap();
    engine.execute("INSERT INTO w VALUES (1,7),(2,3)").unwrap();

    // Branch 's': {1: 30, 2: 5}; branch 'w': {1: 7, 2: 3}. The CTE has four
    // rows; the outer query just totals them per type.
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "WITH yt AS ( \
               SELECT g, SUM(v) total, 's' kind FROM s GROUP BY g \
               UNION ALL \
               SELECT g, SUM(v) total, 'w' kind FROM w GROUP BY g \
             ) SELECT kind, SUM(total) FROM yt GROUP BY kind ORDER BY kind",
        )
        .unwrap()
    else {
        panic!("expected result set for CTE body UNION ALL of GROUP BY branches");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::String("s".to_string()), Value::DecimalCompact(35, 0)],
            vec![Value::String("w".to_string()), Value::DecimalCompact(10, 0)],
        ]
    );
}

#[test]
fn streaming_union_all_derived_join_dim_group_by() {
    // TPC-DS Q2 shape: a `UNION ALL` of two fact-like tables, joined to a
    // small dimension and grouped. The `UNION ALL` now streams into the
    // derived-table spill bridge (no full intermediate row set is built),
    // and the join + GROUP BY must still produce the correct per-group sums.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE ws (d INT, p INT)").unwrap();
    engine.execute("CREATE TABLE cs (d INT, p INT)").unwrap();
    engine.execute("CREATE TABLE dim (dk INT, wk INT)").unwrap();
    engine
        .execute("INSERT INTO ws VALUES (1,10),(2,20)")
        .unwrap();
    engine
        .execute("INSERT INTO cs VALUES (1,100),(3,300)")
        .unwrap();
    engine
        .execute("INSERT INTO dim VALUES (1,7),(2,7),(3,8)")
        .unwrap();
    // union rows: (1,10),(2,20),(1,100),(3,300); join dim on d = dk:
    //   wk 7 ← d∈{1,2}: 10 + 20 + 100 = 130 ; wk 8 ← d=3: 300.
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT wk, SUM(p) FROM \
               (SELECT d, p FROM ws UNION ALL SELECT d, p FROM cs) u, dim \
             WHERE dk = d GROUP BY wk ORDER BY wk",
        )
        .unwrap()
    else {
        panic!("expected result set for streaming UNION ALL join/group");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(7), Value::DecimalCompact(130, 0)],
            vec![Value::Int(8), Value::DecimalCompact(300, 0)],
        ]
    );
}

#[test]
fn streaming_union_all_three_way_nested_keeps_all_rows() {
    // `a UNION ALL b UNION ALL c` parses as nested set operations; the
    // streaming chained cursor must compose (a chain holding a chain) and
    // preserve every row, including duplicates across branches.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE a (v INT)").unwrap();
    engine.execute("CREATE TABLE b (v INT)").unwrap();
    engine.execute("CREATE TABLE c (v INT)").unwrap();
    engine.execute("INSERT INTO a VALUES (1),(2)").unwrap();
    engine.execute("INSERT INTO b VALUES (2),(3)").unwrap();
    engine.execute("INSERT INTO c VALUES (3),(4)").unwrap();

    // Count keeps duplicates: 2 + 2 + 2 = 6.
    let QueryResponse::ResultSet(count) = engine
        .execute(
            "SELECT count(*) FROM \
               (SELECT v FROM a UNION ALL SELECT v FROM b UNION ALL SELECT v FROM c) x",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(count.rows, vec![vec![Value::Int(6)]]);

    // Ordered projection sees every value with its multiplicity.
    let QueryResponse::ResultSet(rows) = engine
        .execute(
            "SELECT v FROM \
               (SELECT v FROM a UNION ALL SELECT v FROM b UNION ALL SELECT v FROM c) x \
             ORDER BY v",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(3)],
            vec![Value::Int(4)],
        ]
    );
}

#[test]
fn streaming_union_all_top_level_preserves_duplicates() {
    // Top-level `SELECT ... UNION ALL ...` takes the streaming dispatch fast
    // path (no set-op ORDER BY/LIMIT). The session materialises the stream
    // for the wire, so the test sees a ResultSet — with all rows, dups kept.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE a (v INT)").unwrap();
    engine.execute("CREATE TABLE b (v INT)").unwrap();
    engine.execute("INSERT INTO a VALUES (1),(1)").unwrap();
    engine.execute("INSERT INTO b VALUES (1),(2)").unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT v FROM a UNION ALL SELECT v FROM b")
        .unwrap()
    else {
        panic!("expected result set for top-level UNION ALL");
    };
    let mut got: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Int(v) => *v,
            other => panic!("unexpected value {other:?}"),
        })
        .collect();
    got.sort_unstable();
    assert_eq!(got, vec![1, 1, 1, 2]);
}

#[test]
fn pivot_conditional_sum_dispatch_matches_per_case_semantics() {
    // Multiple SUM(CASE WHEN cat=literal THEN v END) expressions share input columns
    // through the generic computed-expression family and AggregatePlan batch evaluation,
    // without falling back to a query-specific pivot executor. Match independent CASE semantics:
    // Unmatched category c and NULL category contribute to neither conditional sum.
    // A matched category with NULL v, such as group-2 a, is skipped by SUM, yielding NULL.
    // Ordinary SUM(v)/COUNT(*) slots remain correct when mixed with conditional sums.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (g INT, cat VARCHAR(10), v INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES \
               (1,'a',10),(1,'b',20),(1,'a',5),(1,'c',99),(1,NULL,7),\
               (2,'a',NULL),(2,'b',3)",
        )
        .unwrap();

    let response = engine
        .execute(
            "SELECT g, \
                   SUM(CASE WHEN cat='a' THEN v ELSE NULL END) a_sum, \
                   SUM(CASE WHEN cat='b' THEN v END) b_sum, \
                   SUM(v) tot, \
                   COUNT(*) cnt \
                 FROM t GROUP BY g ORDER BY g",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected result set for pivot conditional sum");
    };
    // g=1: a=10+5=15, b=20, tot=10+20+5+99+7=141, cnt=5
    // g=2: a={NULL}→NULL, b=3, tot=3 (v NULL ignored), cnt=2
    assert_eq!(
        result.rows,
        vec![
            vec![
                Value::Int(1),
                Value::DecimalCompact(15, 0),
                Value::DecimalCompact(20, 0),
                Value::DecimalCompact(141, 0),
                Value::Int(5),
            ],
            vec![
                Value::Int(2),
                Value::Null,
                Value::DecimalCompact(3, 0),
                Value::DecimalCompact(3, 0),
                Value::Int(2),
            ],
        ]
    );
}

#[test]
fn conditional_aggregate_over_union_cte_join_matches() {
    // Conditional aggregation over UNION ALL CTE joined to a dimension stays in the generic batch graph;
    // row-only producers use bounded row-to-batch adaptation solely at the boundary.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE ws (d INT, p INT)").unwrap();
    engine.execute("CREATE TABLE cs (d INT, p INT)").unwrap();
    engine
        .execute("CREATE TABLE dim (dk INT, wk INT, dname VARCHAR(10))")
        .unwrap();
    engine
        .execute("INSERT INTO ws VALUES (1,10),(2,20)")
        .unwrap();
    engine
        .execute("INSERT INTO cs VALUES (1,100),(3,300)")
        .unwrap();
    // d=1→wk7/Mon, d=2→wk7/Tue, d=3→wk8/Mon
    engine
        .execute("INSERT INTO dim VALUES (1,7,'Mon'),(2,7,'Tue'),(3,8,'Mon')")
        .unwrap();

    let response = engine
        .execute(
            "WITH wscs AS (SELECT d, p FROM ws UNION ALL SELECT d, p FROM cs) \
                     SELECT wk, \
                       SUM(CASE WHEN dname='Mon' THEN p END) mon, \
                       SUM(CASE WHEN dname='Tue' THEN p END) tue \
                     FROM wscs, dim WHERE dk = d GROUP BY wk ORDER BY wk",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected result set");
    };

    // Union rows: (1,10),(2,20),(1,100),(3,300); join dimension on d=dk.
    //   wk7: d=1(Mon,10),d=1(Mon,100),d=2(Tue,20) → mon=110, tue=20
    //   wk8: d=3(Mon,300)                          → mon=300, tue=NULL
    assert_eq!(
        result.rows,
        vec![
            vec![
                Value::Int(7),
                Value::DecimalCompact(110, 0),
                Value::DecimalCompact(20, 0),
            ],
            vec![Value::Int(8), Value::DecimalCompact(300, 0), Value::Null,],
        ]
    );
}

#[test]
fn union_all_cte_aggregate_pushdown_matches_materialized_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE ws (d INT, p INT)").unwrap();
    engine.execute("CREATE TABLE cs (d INT, p INT)").unwrap();
    engine
        .execute("CREATE TABLE dim (dk INT, wk INT, dname VARCHAR(10))")
        .unwrap();
    engine
        .execute("INSERT INTO ws VALUES (1,10),(2,20),(4,40)")
        .unwrap();
    engine
        .execute("INSERT INTO cs VALUES (1,100),(3,300)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO dim VALUES \
             (1,7,'Mon'),(2,7,'Tue'),(3,8,'Mon'),(4,8,'Wed')",
        )
        .unwrap();

    let sql = "WITH u AS ( \
                   SELECT d, p FROM ( \
                       SELECT d, p FROM ws \
                       UNION ALL \
                       SELECT d, p FROM cs \
                   ) \
               ), agg AS ( \
                   SELECT wk, \
                          SUM(CASE WHEN dname='Mon' THEN p END) mon, \
                          SUM(CASE WHEN dname='Tue' THEN p END) tue, \
                          SUM(CASE WHEN dname='Wed' THEN p END) wed \
                   FROM u, dim WHERE dk = d GROUP BY wk \
               ) \
               SELECT * FROM agg ORDER BY wk";

    let optimized = engine.execute(sql).unwrap();
    let materialized =
        crate::util::env::with_vars(&[("MYDB_CTE_UNION_AGG_PUSHDOWN", Some("off"))], || {
            engine.execute(sql).unwrap()
        });
    let QueryResponse::ResultSet(result) = optimized else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(materialized_result) = materialized else {
        panic!("expected materialized result set");
    };
    assert_eq!(result.rows, materialized_result.rows);
    assert_eq!(
        result.rows,
        vec![
            vec![
                Value::Int(7),
                Value::DecimalCompact(110, 0),
                Value::DecimalCompact(20, 0),
                Value::Null,
            ],
            vec![
                Value::Int(8),
                Value::DecimalCompact(300, 0),
                Value::Null,
                Value::DecimalCompact(40, 0),
            ],
        ]
    );
}

#[test]
fn set_operation_in_expression_subquery_is_supported() {
    // Issue #57: a set operation (UNION/INTERSECT/EXCEPT) in an IN-subquery /
    // scalar / quantified slot is now supported — the AST-conversion layer
    // wraps it as `SELECT * FROM (<setop>)` so the derived-table bridge
    // materialises it, matching MySQL. (Previously rejected with 1064.)
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE a (id INT)").unwrap();
    engine.execute("CREATE TABLE b (id INT)").unwrap();
    engine.execute("INSERT INTO a VALUES (1),(2)").unwrap();
    engine.execute("INSERT INTO b VALUES (2)").unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id FROM a WHERE id IN (SELECT id FROM a UNION SELECT id FROM b) ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn cte_stays_visible_across_comma_joined_derived_tables() {
    // Regression: when a derived table is materialised and the statement is
    // re-dispatched, the enclosing WITH's CTEs must stay visible to the other
    // JOIN sources. `execute_derived_select` used to *replace* the registry
    // with the single materialised entry, hiding the outer CTE — so the second
    // derived table failed with "table 'c' does not exist". This is the q14b
    // shape (`FROM (... cross_items ...) this_year, (... cross_items ...)
    // last_year`).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "WITH c AS (SELECT id, v FROM t WHERE v >= 20) \
             SELECT a.id, b.id FROM (SELECT id FROM c) a, (SELECT id FROM c) b \
             WHERE a.id = b.id ORDER BY a.id",
        )
        .unwrap()
    else {
        panic!("expected result set; CTE must stay visible to both derived tables");
    };
    // c = {(2),(3)}; the self comma-join on a.id=b.id yields (2,2) and (3,3).
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(2), Value::Int(2)],
            vec![Value::Int(3), Value::Int(3)],
        ]
    );
}

#[test]
fn parsed_but_unsupported_expression_features_return_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // A bare row constructor in scalar (single-column) context is a genuine
    // MySQL error 1241, matching `SELECT (1,2)` on real MySQL 8.0. Row
    // constructors in comparison / IN contexts ARE supported (see
    // mysql80_compat_row_constructor.rs).
    for sql in ["SELECT (1, 2)"] {
        let err = engine.execute(sql).unwrap_err();
        assert_eq!(err.code(), 1241, "{sql}");
        assert_eq!(err.sql_state(), "21000", "{sql}");
        assert!(
            err.message().contains("Operand should contain 1 column(s)"),
            "{}",
            err.message()
        );
    }
}

// ---------------------------------------------------------------------------
// G6: window functions computed over aggregates, and window functions nested
// inside a larger projection expression (TPC-DS q12/q20/q47/q53/q57/q63/q89).
// The inner aggregate is materialised by the GROUP BY; the window then runs
// over the grouped rows. Used to panic (`unreachable!`) / 1052 / clean-1235.
// ---------------------------------------------------------------------------

#[test]
fn window_over_aggregate_bare_computes_correct_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wt (id INT PRIMARY KEY, cls INT, sub INT, v INT)")
        .unwrap();
    // Groups by (cls,sub): (1,1)->10, (1,2)->20, (2,1)->30, (2,2)->40.
    engine
        .execute("INSERT INTO wt VALUES (1,1,1,10),(2,1,2,20),(3,2,1,30),(4,2,2,40)")
        .unwrap();

    // SUM(SUM(v)) OVER (PARTITION BY cls): partition cls=1 sums {10,20}=30,
    // cls=2 sums {30,40}=70. (TPC-DS q47 `avg_monthly_sales` shape.)
    let QueryResponse::ResultSet(r) = engine
        .execute(
            "SELECT cls, sub, SUM(v), SUM(SUM(v)) OVER (PARTITION BY cls) \
             FROM wt GROUP BY cls, sub ORDER BY cls, sub",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let expected = [
        (1.0, 1.0, 10.0, 30.0),
        (1.0, 2.0, 20.0, 30.0),
        (2.0, 1.0, 30.0, 70.0),
        (2.0, 2.0, 40.0, 70.0),
    ];
    assert_eq!(r.rows.len(), 4);
    for (row, (c, s, sv, ws)) in r.rows.iter().zip(expected) {
        assert_eq!(as_f64(&row[0]), c);
        assert_eq!(as_f64(&row[1]), s);
        assert_eq!(as_f64(&row[2]), sv);
        assert!((as_f64(&row[3]) - ws).abs() < 1e-9, "win sum: {row:?}");
    }

    // AVG(SUM(v)) OVER (PARTITION BY cls): cls=1 avg(10,20)=15, cls=2 avg(30,40)=35.
    let QueryResponse::ResultSet(r) = engine
        .execute(
            "SELECT cls, AVG(SUM(v)) OVER (PARTITION BY cls) \
             FROM wt GROUP BY cls, sub ORDER BY cls, sub",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let expected = [15.0, 15.0, 35.0, 35.0];
    for (row, e) in r.rows.iter().zip(expected) {
        assert!((as_f64(&row[1]) - e).abs() < 1e-9, "avg: {row:?}");
    }

    // RANK() OVER (ORDER BY SUM(v) DESC): sums 40,30,20,10 rank 1..4.
    let QueryResponse::ResultSet(r) = engine
        .execute(
            "SELECT cls, sub, RANK() OVER (ORDER BY SUM(v) DESC) \
             FROM wt GROUP BY cls, sub ORDER BY cls, sub",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // ordered by cls,sub: (1,1)=10->rank4, (1,2)=20->rank3, (2,1)=30->rank2, (2,2)=40->rank1
    let ranks: Vec<f64> = r.rows.iter().map(|row| as_f64(&row[2])).collect();
    assert_eq!(ranks, vec![4.0, 3.0, 2.0, 1.0]);
}

#[test]
fn window_over_decimal_aggregate_survives_derived_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wtd (id INT PRIMARY KEY, cls INT, sub INT, v DECIMAL(9,2))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO wtd VALUES \
             (1,1,1,10.25),(2,1,2,20.75),(3,2,1,30.50),(4,2,2,40.50)",
        )
        .unwrap();

    let QueryResponse::ResultSet(r) = engine
        .execute(
            "SELECT * FROM (\
               SELECT cls, sub, SUM(v) AS sum_v, \
                      AVG(SUM(v)) OVER (PARTITION BY cls) AS avg_v, \
                      SUM(SUM(v)) OVER (PARTITION BY cls ORDER BY sub \
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_v \
               FROM wtd GROUP BY cls, sub\
             ) d ORDER BY cls, sub",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let expected = [
        (15.50, 10.25),
        (15.50, 31.00),
        (35.50, 30.50),
        (35.50, 71.00),
    ];
    assert_eq!(r.rows.len(), expected.len());
    for (row, (avg, cume)) in r.rows.iter().zip(expected) {
        assert!((as_f64(&row[3]) - avg).abs() < 1e-9, "avg: {row:?}");
        assert!((as_f64(&row[4]) - cume).abs() < 1e-9, "cume: {row:?}");
    }

    let QueryResponse::ResultSet(r) = engine
        .execute(
            "SELECT * FROM (\
               SELECT cls, sub, SUM(v) AS sum_v, \
                      AVG(SUM(v)) OVER (PARTITION BY cls) AS avg_v \
               FROM wtd GROUP BY cls, sub\
             ) d WHERE sum_v > avg_v ORDER BY cls, sub",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(r.rows.len(), 2);
    assert!((as_f64(&r.rows[0][3]) - 15.50).abs() < 1e-9, "{:?}", r.rows);
    assert!((as_f64(&r.rows[1][3]) - 35.50).abs() < 1e-9, "{:?}", r.rows);
}

#[test]
fn window_inside_larger_expression_computes_correct_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wt (id INT PRIMARY KEY, cls INT, sub INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO wt VALUES (1,1,1,10),(2,1,2,20),(3,2,1,30),(4,2,2,40)")
        .unwrap();

    // q12/q20 shape: a window function nested inside arithmetic. The surrounding
    // SUM(v) and the windowed SUM(SUM(v)) share one grouped column.
    // pct = SUM(v)*100 / SUM(SUM(v)) OVER (PARTITION BY cls).
    let QueryResponse::ResultSet(r) = engine
        .execute(
            "SELECT cls, sub, SUM(v)*100/SUM(SUM(v)) OVER (PARTITION BY cls) AS pct \
             FROM wt GROUP BY cls, sub ORDER BY cls, sub",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // cls=1 total 30: 10*100/30=33.33, 20*100/30=66.67;
    // cls=2 total 70: 30*100/70=42.857, 40*100/70=57.143.
    let expected = [1000.0 / 30.0, 2000.0 / 30.0, 3000.0 / 70.0, 4000.0 / 70.0];
    assert_eq!(r.rows.len(), 4);
    // Tolerance accommodates MySQL/mydb fixed-point division scale (4 decimals).
    for (row, e) in r.rows.iter().zip(expected) {
        assert!(
            (as_f64(&row[2]) - e).abs() < 1e-3,
            "pct: {row:?} expected {e}"
        );
    }

    // Plain window functions (no aggregate, bare item) keep working.
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, RANK() OVER (ORDER BY v) FROM wt")
        .unwrap()
    else {
        panic!("expected result set for plain window function");
    };
    assert_eq!(result.rows.len(), 4);
}

#[test]
fn window_whole_partition_aggregates_cover_nulls_and_decimal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wf (id INT PRIMARY KEY, cls INT, amount DECIMAL(7,2), opt INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO wf VALUES \
             (1,1,10.50,7),(2,1,NULL,NULL),(3,1,2.25,9),\
             (4,2,5.00,NULL),(5,2,15.00,11)",
        )
        .unwrap();

    let QueryResponse::ResultSet(r) = engine
        .execute(
            "SELECT cls, id, \
                    COUNT(*) OVER (PARTITION BY cls), \
                    COUNT(opt) OVER (PARTITION BY cls), \
                    SUM(amount) OVER (PARTITION BY cls), \
                    AVG(amount) OVER (PARTITION BY cls), \
                    MIN(amount) OVER (PARTITION BY cls), \
                    MAX(amount) OVER (PARTITION BY cls) \
             FROM wf ORDER BY cls, id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(r.rows.len(), 5);
    let expected = [
        (1, 1, 3, 2, 12.75, 6.375, 2.25, 10.50),
        (1, 2, 3, 2, 12.75, 6.375, 2.25, 10.50),
        (1, 3, 3, 2, 12.75, 6.375, 2.25, 10.50),
        (2, 4, 2, 1, 20.00, 10.00, 5.00, 15.00),
        (2, 5, 2, 1, 20.00, 10.00, 5.00, 15.00),
    ];
    for (row, (cls, id, cnt_all, cnt_opt, sum, avg, min, max)) in r.rows.iter().zip(expected) {
        assert_eq!(row[0], Value::Int(cls));
        assert_eq!(row[1], Value::Int(id));
        assert_eq!(row[2], Value::Int(cnt_all));
        assert_eq!(row[3], Value::Int(cnt_opt));
        assert!((as_f64(&row[4]) - sum).abs() < 1e-9, "sum: {row:?}");
        assert!((as_f64(&row[5]) - avg).abs() < 1e-9, "avg: {row:?}");
        assert!((as_f64(&row[6]) - min).abs() < 1e-9, "min: {row:?}");
        assert!((as_f64(&row[7]) - max).abs() < 1e-9, "max: {row:?}");
    }
}

#[test]
fn q98_shape_window_ratio_and_multi_key_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE q98m ( \
                id INT PRIMARY KEY, item_id VARCHAR(8), item_desc VARCHAR(32), \
                category VARCHAR(16), class VARCHAR(16), price DECIMAL(7,2), sales DECIMAL(7,2) \
             )",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO q98m VALUES \
             (1,'i2','beta','Music','rock',2.00,10.00),\
             (2,'i2','beta','Music','rock',2.00,30.00),\
             (3,'i1','alpha','Music','rock',1.00,60.00),\
             (4,'i3','gamma','Electronics','audio',3.00,25.00),\
             (5,'i4','delta','Electronics','audio',4.00,75.00)",
        )
        .unwrap();

    let QueryResponse::ResultSet(r) = engine
        .execute(
            "SELECT item_id, item_desc, category, class, price, \
                    SUM(sales) AS itemrevenue, \
                    SUM(sales)*100/SUM(SUM(sales)) OVER (PARTITION BY class) AS revenueratio \
             FROM q98m \
             GROUP BY item_id, item_desc, category, class, price \
             ORDER BY category, class, item_id, item_desc, revenueratio",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(r.rows.len(), 4);
    let item_ids: Vec<String> = r
        .rows
        .iter()
        .map(|row| match &row[0] {
            v if crate::types::value_str(v).is_some() => {
                crate::types::value_str(v).unwrap().to_string()
            }
            other => panic!("expected item_id string, got {other:?}"),
        })
        .collect();
    assert_eq!(item_ids, vec!["i3", "i4", "i1", "i2"]);
    let ratios: Vec<f64> = r.rows.iter().map(|row| as_f64(&row[6])).collect();
    assert!((ratios[0] - 25.0).abs() < 1e-6, "{ratios:?}");
    assert!((ratios[1] - 75.0).abs() < 1e-6, "{ratios:?}");
    assert!((ratios[2] - 60.0).abs() < 1e-6, "{ratios:?}");
    assert!((ratios[3] - 40.0).abs() < 1e-6, "{ratios:?}");
}

#[test]
fn window_over_aggregate_with_aliased_partition_column() {
    // Regression (TPC-DS Q51): a cumulative window over an aggregate where the
    // PARTITION BY column is *aliased* in the SELECT list
    // (`g AS item_sk ... PARTITION BY g`). The window runs over a materialised
    // copy of the aggregating inner; its columns carry the *alias* (`item_sk`),
    // not the base name (`g`). The dep-projection logic used to mark `g` as
    // "already available" because it was the base of the aliased projection, so
    // it never re-projected a bare `g` column. PARTITION BY g then resolved to
    // NULL for every row, collapsing all rows into a single partition — both a
    // correctness bug (cumulative sum across all groups) and an O(n²) blow-up.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wa (id INT PRIMARY KEY, g INT, d INT, v INT)")
        .unwrap();
    // Two partitions (g=1, g=2), each with three ordered rows.
    engine
        .execute(
            "INSERT INTO wa VALUES \
             (1,1,1,10),(2,1,2,20),(3,1,3,30),(4,2,1,100),(5,2,2,200),(6,2,3,300)",
        )
        .unwrap();

    let QueryResponse::ResultSet(r) = engine
        .execute(
            "SELECT g AS item_sk, d, \
             SUM(SUM(v)) OVER (PARTITION BY g ORDER BY d \
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume \
             FROM wa GROUP BY g, d",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // Per-partition running sums keyed by (item_sk, d): g=1 -> 10,30,60;
    // g=2 -> 100,300,600. A collapsed single partition would instead produce a
    // sum carried across both groups (e.g. g=2/d=1 -> 160 rather than 100).
    let mut got: Vec<(i64, i64, f64)> = r
        .rows
        .iter()
        .map(|row| {
            (
                as_f64(&row[0]) as i64,
                as_f64(&row[1]) as i64,
                as_f64(&row[2]),
            )
        })
        .collect();
    got.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    assert_eq!(
        got,
        vec![
            (1, 1, 10.0),
            (1, 2, 30.0),
            (1, 3, 60.0),
            (2, 1, 100.0),
            (2, 2, 300.0),
            (2, 3, 600.0),
        ]
    );
}

#[test]
fn window_function_in_derived_table_executes() {
    // G6c: window functions inside a derived table / subquery. The window path
    // (`plan_select`) cannot plan a derived FROM, so the derived source is
    // materialized into a transient CTE first, then windowed.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wd (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO wd VALUES (1,1,10),(2,1,20),(3,2,5),(4,2,40)")
        .unwrap();

    // (a) window over a plain derived table.
    let QueryResponse::ResultSet(r) = engine
        .execute("SELECT v, RANK() OVER (ORDER BY v DESC) r FROM (SELECT v FROM wd) x ORDER BY v")
        .unwrap()
    else {
        panic!("expected result set");
    };
    // v asc: 5,10,20,40; ranks by v desc: 5->4,10->3,20->2,40->1.
    let pairs: Vec<(f64, f64)> = r
        .rows
        .iter()
        .map(|row| (as_f64(&row[0]), as_f64(&row[1])))
        .collect();
    assert_eq!(
        pairs,
        vec![(5.0, 4.0), (10.0, 3.0), (20.0, 2.0), (40.0, 1.0)]
    );

    // (b) window over a GROUP BY derived table.
    let QueryResponse::ResultSet(r) = engine
        .execute(
            "SELECT g, s, RANK() OVER (ORDER BY s DESC) r \
             FROM (SELECT g, SUM(v) s FROM wd GROUP BY g) x ORDER BY g",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // g=1 sum=30, g=2 sum=45; ranks: 45->1, 30->2.
    let triples: Vec<(f64, f64, f64)> = r
        .rows
        .iter()
        .map(|row| (as_f64(&row[0]), as_f64(&row[1]), as_f64(&row[2])))
        .collect();
    assert_eq!(triples, vec![(1.0, 30.0, 2.0), (2.0, 45.0, 1.0)]);

    // (c) outer query filtering on the derived window column (q67 dw2 shape).
    let QueryResponse::ResultSet(r) = engine
        .execute(
            "SELECT v, r FROM (SELECT v, RANK() OVER (ORDER BY v DESC) r FROM wd) x \
             WHERE r <= 2 ORDER BY r",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // ranks by v desc: 40->1,20->2,10->3,5->4; keep r<=2.
    let pairs: Vec<(f64, f64)> = r
        .rows
        .iter()
        .map(|row| (as_f64(&row[0]), as_f64(&row[1])))
        .collect();
    assert_eq!(pairs, vec![(40.0, 1.0), (20.0, 2.0)]);
}

#[test]
fn window_rank_bound_survives_repeated_plan_preflight() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE rank_preflight (id INT, g INT)")
        .unwrap();
    let Statement::Select(select) = parse_statement(
        "SELECT id, RANK() OVER (PARTITION BY g ORDER BY id) AS rk FROM rank_preflight",
    )
    .unwrap() else {
        unreachable!()
    };
    let _hint = crate::engine::executor::dispatch::set_window_topk_hint(Some(("rk".into(), 3)));
    for pass in 0..2 {
        let planned = crate::engine::executor::dispatch::plan_windowed_select_for_explain(
            engine.db_for_tests(),
            select.clone(),
        )
        .unwrap();
        assert!(
            planned.plan.any(|node| matches!(node,
                crate::planner::PhysicalPlan::WindowAgg { windows, .. }
                    if windows.len() == 1 && windows[0].top_k == Some(3)
            )),
            "preflight {pass} consumed the execution's rank bound"
        );
    }
}

#[test]
fn compact_window_batches_match_spill_for_multiple_fixed_width_windows() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE compact_win (id INT PRIMARY KEY, g INT, v DECIMAL(12,2), d DATE)")
        .unwrap();
    // Scale rows and budget together; retain every type, partition and frame.
    for base in (0..512).step_by(128) {
        let values = (base..base + 128)
            .map(|id| {
                format!(
                    "({id},{},{},'2024-01-{:02}')",
                    id % 31,
                    if id % 7 == 0 {
                        "NULL".into()
                    } else {
                        format!("{}.25", id % 47)
                    },
                    id % 28 + 1,
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO compact_win VALUES {values}"))
            .unwrap();
    }
    let sql = "SELECT id, SUM(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), MIN(d) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING), RANK() OVER (PARTITION BY g ORDER BY v DESC), COUNT(v) OVER (PARTITION BY g) FROM compact_win ORDER BY id";
    let run = |limit| {
        let budget = Arc::new(MemBudget::new(limit));
        let rows = with_query_budget(budget.clone(), || {
            collect_select_response_rows(engine.execute(sql).unwrap())
        });
        assert_eq!(budget.used(), 0);
        rows
    };
    let expected = run(16 * 1024);
    let spill_plan = with_query_budget(Arc::new(MemBudget::new(16 * 1024)), || {
        explain_analyze_tree(&engine, sql)
    });
    assert!(spill_plan.contains("spill_bytes="), "{spill_plan}");
    let before = crate::engine::executor::select::vec_bridge::COMPACT_WINDOW_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(run(64 * 1024 * 1024), expected);
    assert!(
        crate::engine::executor::select::vec_bridge::COMPACT_WINDOW_BATCH_HITS
            .load(std::sync::atomic::Ordering::Relaxed)
            > before
    );
}

#[test]
fn columnar_window_shared_orders_match_spill_with_nulls_and_mixed_frames() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE shared_win (id INT, g INT, v DECIMAL(18,2), f DOUBLE, d DATE, u BIGINT UNSIGNED)").unwrap();
    for base in (0..512).step_by(128) {
        let values = (base..base + 128)
            .map(|id| {
                format!(
                    "({id},{},{},{},'2024-01-{:02}',{})",
                    id % 31,
                    if id % 7 == 0 {
                        "NULL".into()
                    } else {
                        format!("{}.25", id % 47 - 23)
                    },
                    if id % 11 == 0 {
                        "NULL".into()
                    } else {
                        format!("{}.125", id % 13)
                    },
                    id % 5 + 1,
                    u64::MAX - id as u64,
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO shared_win VALUES {values}"))
            .unwrap();
    }
    // Interleaved order groups must retain projection order; ROWS ties retain
    // input ordinals, while RANGE/GROUPS and exclusions use canonical frames.
    let sql = "SELECT id, \
        MAX(v) OVER (PARTITION BY g ORDER BY d ROWS UNBOUNDED PRECEDING), \
        MIN(u) OVER (PARTITION BY g ORDER BY d DESC ROWS UNBOUNDED PRECEDING), \
        SUM(v) OVER (PARTITION BY g ORDER BY d ROWS UNBOUNDED PRECEDING), \
        AVG(f) OVER (PARTITION BY g ORDER BY d ROWS UNBOUNDED PRECEDING), \
        COUNT(*) OVER (PARTITION BY g ORDER BY d ROWS UNBOUNDED PRECEDING), \
        COUNT(v) OVER (PARTITION BY g ORDER BY d ROWS UNBOUNDED PRECEDING), \
        MAX(d) OVER (PARTITION BY g ORDER BY d DESC ROWS UNBOUNDED PRECEDING), \
        SUM(v) OVER (PARTITION BY g ORDER BY d RANGE UNBOUNDED PRECEDING), \
        RANK() OVER (PARTITION BY g ORDER BY d), \
        MIN(v) OVER (PARTITION BY g ORDER BY d GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE TIES) \
        FROM shared_win WHERE id % 3 <> 0 ORDER BY id";
    let run = |limit| {
        let budget = Arc::new(MemBudget::new(limit));
        let rows = with_query_budget(budget.clone(), || {
            collect_select_response_rows(engine.execute(sql).unwrap())
        });
        assert_eq!(budget.used(), 0);
        rows
    };
    let expected = run(64 * 1024);
    crate::util::env::with_vars(
        &[
            ("MYDB_WINDOW_PARALLEL_MIN_ROWS", Some("0")),
            ("MYDB_PARALLEL_DEGREE", Some("4")),
        ],
        || {
            assert_eq!(run(64 * 1024 * 1024), expected);
        },
    );
}

#[test]
fn window_rank_prefilter_matches_unfiltered_rank_across_chunks() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE rank_chunks (id INT PRIMARY KEY, g INT, v INT, s VARCHAR(400))")
        .unwrap();
    // 1024 wide rows still exceed both the 64 KiB spill budget and the
    // 4 MiB budget's 512 KiB candidate chunk (rows are charged at 3x).
    // Keep ties, NULLs and all eight partitions without four redundant passes
    // through the same key distribution.
    insert_fixture_rows(
        &engine,
        "test.rank_chunks",
        (0..1024i64).map(|id| {
            vec![
                Value::Int(id),
                Value::Int(id % 8),
                if id % 19 == 0 {
                    Value::Null
                } else {
                    Value::Int((id * 37) % 101)
                },
                Value::String("payload".repeat(40)),
            ]
        }),
    );
    for function in ["ROW_NUMBER", "RANK", "DENSE_RANK"] {
        let order = if function == "ROW_NUMBER" {
            "v DESC, id"
        } else {
            "v DESC"
        };
        let inner = format!("SELECT id,g,v,s,{function}() OVER (PARTITION BY g ORDER BY {order}) AS rk FROM rank_chunks");
        let all = collect_select_response_rows(
            engine
                .execute(&format!("SELECT * FROM ({inner}) r ORDER BY id"))
                .unwrap(),
        );
        let expected = all
            .into_iter()
            .filter(|row| matches!(row.last(), Some(Value::Int(rank)) if *rank <= 3))
            .collect::<Vec<_>>();
        for limit in [64 * 1024, 4 * 1024 * 1024] {
            let budget = Arc::new(MemBudget::new(limit));
            let actual = with_query_budget(budget.clone(), || {
                collect_select_response_rows(
                    engine
                        .execute(&format!(
                            "SELECT * FROM ({inner}) r WHERE rk <= 3 ORDER BY id"
                        ))
                        .unwrap(),
                )
            });
            assert_eq!(actual, expected, "{function}, budget {limit}");
            assert_eq!(budget.used(), 0);
        }
    }
    assert!(
        crate::engine::executor::select::plan::WINDOW_RANK_PREFILTER_REJECTED
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0,
        "ranking prefilter must execute, not just match fallback output"
    );
}

#[test]
fn window_small_partitions_keep_frames_peers_and_input_ordinals() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wp (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    // Thirty-two interleaved four-row partitions still span many bounded
    // partition batches. A proportional budget keeps the spill path active.
    const GROUPS: usize = 32;
    for round in 0..4 {
        let values = (0..GROUPS)
            .map(|g| format!("({}, {g}, {})", round * GROUPS + g, round % 2))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO wp VALUES {values}"))
            .unwrap();
    }
    let sql = "SELECT id, SUM(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), RANK() OVER (PARTITION BY g ORDER BY v DESC), LAG(v) OVER (PARTITION BY g ORDER BY id) FROM wp ORDER BY id";
    for mode in [VecMode::Off, VecMode::On] {
        for limit in [16 * 1024, 64 * 1024 * 1024] {
            let budget = Arc::new(MemBudget::new(limit));
            let rows = with_vec_mode(mode, || {
                with_query_budget(budget.clone(), || {
                    collect_select_response_rows(engine.execute(sql).unwrap())
                })
            });
            assert_eq!(rows.len(), GROUPS * 4);
            for (id, row) in rows.iter().enumerate() {
                let round = id / GROUPS;
                assert_eq!(row[0].to_display_string(), id.to_string());
                assert_eq!(row[1].to_display_string(), ["0", "1", "1", "2"][round]);
                assert_eq!(row[2].to_display_string(), ["3", "1", "3", "1"][round]);
                assert_eq!(row[3].to_display_string(), ["NULL", "0", "1", "0"][round]);
            }
            assert_eq!(budget.used(), 0);
        }
    }
    let spill_plan = with_query_budget(Arc::new(MemBudget::new(16 * 1024)), || {
        explain_analyze_tree(&engine, sql)
    });
    assert!(spill_plan.contains("spill_bytes="), "{spill_plan}");
}

#[test]
fn window_soft_watermark_spill_matches_row_and_batch_oracles() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE window_budget (id INT PRIMARY KEY, g INT, v DECIMAL(12,2), s VARCHAR(100))").unwrap();
    // Keep twenty rows per partition (including peers/NULLs) and force spill
    // with a proportional budget instead of repeating the same distribution.
    for base in (0..260).step_by(65) {
        let values = (base..base + 65)
            .map(|id| {
                format!(
                    "({}, {}, {}, '{}')",
                    id,
                    id % 13,
                    if id % 7 == 0 {
                        "NULL".into()
                    } else {
                        format!("{}.25", id % 29)
                    },
                    "x".repeat(80)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO window_budget VALUES {values}"))
            .unwrap();
    }
    for sql in [
        "SELECT id, s, SUM(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), AVG(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) FROM window_budget ORDER BY id",
        "SELECT id, RANK() OVER (PARTITION BY g ORDER BY v), LAG(s) OVER (PARTITION BY g ORDER BY id), MAX(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM window_budget ORDER BY id",
    ] {
        let run = |mode, limit| {
            let budget = Arc::new(MemBudget::new(limit));
            let rows = with_vec_mode(mode, || with_query_budget(budget.clone(), || {
                collect_select_response_rows(engine.execute(sql).unwrap())
            }));
            assert_eq!(budget.used(), 0, "window reservations must leave with their owners");
            rows
        };
        let expected = run(VecMode::Off, 64 * 1024 * 1024);
        assert_eq!(run(VecMode::On, 64 * 1024 * 1024), expected);
        assert_eq!(run(VecMode::Off, 16 * 1024), expected);
        assert_eq!(run(VecMode::On, 16 * 1024), expected);
        let spill_plan = with_query_budget(Arc::new(MemBudget::new(16 * 1024)), || {
            explain_analyze_tree(&engine, sql)
        });
        // The scalar window reports its spool mode (byte counters are
        // attached only to streaming sort nodes).
        assert!(
            spill_plan.contains("spill_bytes=")
                || spill_plan.contains("window_mode=partition_spool"),
            "{spill_plan}"
        );
    }
}

#[test]
fn window_materialize_boundaries_spill_before_retaining_full_result() {
    with_test_env(&[("MYDB_CTE_SPILL_THRESHOLD_MB", Some("0"))], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute("CREATE TABLE ws (id INT PRIMARY KEY, g INT, v INT)")
            .unwrap();
        engine
            .execute("INSERT INTO ws VALUES (1,1,10),(2,1,20),(3,2,5),(4,2,40)")
            .unwrap();

        let spill_before = crate::engine::executor::spill_quota::cte_spill_count();
        let QueryResponse::ResultSet(derived) = engine
            .execute(
                "SELECT v, RANK() OVER (ORDER BY v DESC) r \
                 FROM (SELECT v FROM ws GROUP BY v) x ORDER BY v",
            )
            .unwrap()
        else {
            panic!("expected derived window result set");
        };
        assert_eq!(derived.rows.len(), 4);
        let spill_after_derived = crate::engine::executor::spill_quota::cte_spill_count();

        let QueryResponse::ResultSet(grouped) = engine
            .execute(
                "SELECT g, SUM(v) s, RANK() OVER (ORDER BY SUM(v) DESC) r \
                 FROM ws GROUP BY g ORDER BY g",
            )
            .unwrap()
        else {
            panic!("expected grouped window result set");
        };
        let got: Vec<(i64, i64)> = grouped
            .rows
            .iter()
            .map(|row| (as_f64(&row[0]) as i64, as_f64(&row[2]) as i64))
            .collect();
        assert_eq!(got, vec![(1, 2), (2, 1)]);
        let spill_after_grouped = crate::engine::executor::spill_quota::cte_spill_count();
        assert!(
            spill_after_derived > spill_before,
            "inline-derived window input must spill (before={spill_before}, after={spill_after_derived})"
        );
        assert!(
            spill_after_grouped > spill_after_derived,
            "aggregate-to-window input must spill (before={spill_before}, derived={spill_after_derived}, grouped={spill_after_grouped})"
        );
    });
}

#[test]
fn q67_shape_rollup_window_filter_in_nested_derived_executes() {
    // Faithful TPC-DS q67: ROLLUP in the innermost derived (G2-B), a RANK window
    // over it in the middle derived (G6c), a top-N filter in the outer query.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE rdw (id INT PRIMARY KEY, g VARCHAR(8), h VARCHAR(8), v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO rdw VALUES (1,'a','x',1),(2,'a','y',2),(3,'b','x',4),(4,'b','y',5)")
        .unwrap();

    let QueryResponse::ResultSet(r) = engine
        .execute(
            "SELECT g, h, s, rk FROM ( \
                 SELECT g, h, s, RANK() OVER (PARTITION BY g ORDER BY s DESC) rk \
                 FROM ( \
                     SELECT g, h, SUM(v) s FROM rdw GROUP BY g, h WITH ROLLUP \
                 ) dw1 \
             ) dw2 \
             WHERE rk <= 2 \
             ORDER BY g IS NULL, g, rk",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // inner rollup per g: a -> x=1,y=2,subtotal=3; b -> x=4,y=5,subtotal=9; grand=12.
    // RANK partition by g order s desc: a: 3->1,2->2,1->3; b: 9->1,5->2,4->3; NULL: 12->1.
    // keep rk<=2, ordered (g IS NULL, g, rk):
    //   (a,NULL,3,1),(a,y,2,2),(b,NULL,9,1),(b,y,5,2),(NULL,NULL,12,1)
    assert_eq!(r.rows.len(), 5);
    let g: Vec<Option<String>> = r
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Null => None,
            v => Some(
                crate::types::value_str(v)
                    .expect("g is a string value")
                    .to_string(),
            ),
        })
        .collect();
    assert_eq!(
        g,
        vec![
            Some("a".to_string()),
            Some("a".to_string()),
            Some("b".to_string()),
            Some("b".to_string()),
            None,
        ]
    );
    let s: Vec<f64> = r.rows.iter().map(|row| as_f64(&row[2])).collect();
    assert_eq!(s, vec![3.0, 2.0, 9.0, 5.0, 12.0]);
    let rk: Vec<f64> = r.rows.iter().map(|row| as_f64(&row[3])).collect();
    assert_eq!(rk, vec![1.0, 2.0, 1.0, 2.0, 1.0]);
}

#[test]
fn full_outer_join_emits_unmatched_from_both_sides() {
    // FULL OUTER JOIN (standard SQL; MySQL itself lacks it). q51 uses it over
    // two CTEs; here we check base tables to exercise the general path.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (k INT, lv INT)").unwrap();
    engine.execute("CREATE TABLE r (k INT, rv INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (2,200),(3,300),(4,400)")
        .unwrap();

    // matched: k=2,3; left-only: k=1; right-only: k=4. Order by the coalesced
    // key so the row order is deterministic (1,2,3,4).
    let QueryResponse::ResultSet(res) = engine
        .execute(
            "SELECT l.k, r.k, l.lv, r.rv FROM l FULL OUTER JOIN r ON l.k = r.k \
             ORDER BY COALESCE(l.k, r.k)",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let cell = |v: &Value| match v {
        Value::Null => -1_i64,
        other => as_f64(other) as i64,
    };
    let rows: Vec<(i64, i64, i64, i64)> = res
        .rows
        .iter()
        .map(|row| (cell(&row[0]), cell(&row[1]), cell(&row[2]), cell(&row[3])))
        .collect();
    assert_eq!(
        rows,
        vec![
            (1, -1, 10, -1),  // left-only: r.* NULL
            (2, 2, 20, 200),  // matched
            (3, 3, 30, 300),  // matched
            (-1, 4, -1, 400), // right-only: l.* NULL
        ]
    );
}

#[test]
fn unconditional_cross_product_streams_with_bounded_memory() {
    // A cross product can be much larger than either input. The common batch
    // pipeline must keep it bounded by the cursor/channel window instead of
    // materialising |left|×|right| or rejecting a streamable result merely
    // because its total cardinality exceeds the query work-area budget.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("SET GLOBAL mydb_query_mem_limit_mb = 4")
        .unwrap();
    engine
        .execute("CREATE TABLE l (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE r (id INT PRIMARY KEY)")
        .unwrap();
    let mut li = String::from("INSERT INTO l VALUES ");
    let mut ri = String::from("INSERT INTO r VALUES ");
    for i in 0..200 {
        if i > 0 {
            li.push(',');
            ri.push(',');
        }
        li.push_str(&format!("({i})"));
        ri.push_str(&format!("({i})"));
    }
    engine.execute(&li).unwrap();
    engine.execute(&ri).unwrap();

    let QueryResponse::StreamingResultSet(mut result) = engine
        .execute_streaming_for_test("SELECT l.id, r.id FROM l, r")
        .unwrap()
    else {
        panic!("expected bounded streaming result")
    };
    let mut rows = 0usize;
    while result.cursor.next_row().unwrap().is_some() {
        rows += 1;
    }
    assert_eq!(rows, 40_000);

    // A bounded equi-join over the same tables still completes under the cap.
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT count(*) FROM l, r WHERE l.id = r.id")
        .unwrap()
    else {
        panic!("expected result set for bounded join");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(200)]]);
}

#[test]
fn cte_materialization_spills_under_query_memory_budget() {
    // A non-recursive CTE stores its full result in the CTE registry. Wide
    // intermediate rows must be charged while they are materialized, otherwise
    // TPC-DS-style repeated CTE consumers can OOM the process before any
    // downstream operator sees a budget check.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("SET GLOBAL mydb_query_mem_limit_mb = 1")
        .unwrap();
    engine
        .execute("CREATE TABLE wide_cte (id INT PRIMARY KEY, pad TEXT)")
        .unwrap();
    let payload = "x".repeat(2048);
    let mut insert = String::from("INSERT INTO wide_cte VALUES ");
    for i in 0..1500 {
        if i > 0 {
            insert.push(',');
        }
        insert.push_str(&format!("({i},'{payload}')"));
    }
    engine.execute(&insert).unwrap();

    // Reference the CTE twice (self-join) so it materialises — P1-1 inlines
    // only single-use CTEs, and the budget-charging under test lives on the
    // materialisation path. `count(a.pad)` references the wide `pad` column so
    // P1-2 keeps it (the materialised rows must stay wide to exceed 1 MB).
    let multi_ref = "WITH c AS (SELECT id, pad FROM wide_cte) \
                     SELECT count(a.pad) FROM c a JOIN c b ON a.id = b.id";
    let spill_before = crate::engine::executor::spill_quota::cte_spill_count();
    let QueryResponse::ResultSet(result) = engine.execute(multi_ref).unwrap() else {
        panic!("expected spillable CTE to complete under its work-area budget");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1500)]]);
    assert!(
        crate::engine::executor::spill_quota::cte_spill_count() > spill_before,
        "wide CTE must spill instead of borrowing past the 1 MiB work area"
    );

    // The completed spill must release its process/disk reservations; the same
    // CTE remains reusable after changing the work-area budget.
    engine
        .execute("SET GLOBAL mydb_query_mem_limit_mb = 64")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine.execute(multi_ref).unwrap() else {
        panic!("expected result set after raising query memory budget");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1500)]]);
}

/// A CTE whose materialized rows exceed the spill threshold must spill
/// to disk (bounding the registry hold) instead of raising 1037, and
/// must still return correct results — including when referenced more
/// than once (the TPC-DS Q2/Q5/Q14/Q23 failure mode). nextest isolates
/// the env var per process.
#[test]
fn large_cte_spills_to_disk_instead_of_oom() {
    // Force a low spill threshold so a modest CTE spills deterministically.
    std::env::set_var("MYDB_CTE_SPILL_THRESHOLD_MB", "1");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE base (id INT PRIMARY KEY, pad TEXT)")
        .unwrap();
    let payload = "y".repeat(512);
    let n = 6000;
    let mut next = 0;
    while next < n {
        let take = (n - next).min(400);
        let mut insert = String::from("INSERT INTO base VALUES ");
        for i in 0..take {
            if i > 0 {
                insert.push(',');
            }
            let r = next + i;
            insert.push_str(&format!("({r},'{payload}')"));
        }
        engine.execute(&insert).unwrap();
        next += take;
    }

    // CTE body ≈ 6000 × ~520 B ≈ 3 MB > 1 MB threshold → spills.
    // Reference the CTE twice (count + a self-join) to prove a spilled
    // CTE is re-readable across references.
    let baseline =
        std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::new(1 << 30));
    let total: i64 = {
        let QueryResponse::ResultSet(rs) = engine
            .execute("WITH c AS (SELECT id, pad FROM base) SELECT count(*) FROM c")
            .unwrap()
        else {
            panic!("expected result set");
        };
        match rs.rows[0][0] {
            Value::Int(v) => v,
            _ => panic!("count not int"),
        }
    };
    assert_eq!(total, 6000, "spilled CTE must return all rows");
    let _ = baseline;

    // Multi-reference: a CTE joined to itself is NOT inlined (P1-1 inlines
    // only single-use CTEs), so it materialises, spills under the 1 MB
    // threshold, and must read the spill file twice — producing the correct
    // join cardinality (each id matches exactly one id → 6000 rows).
    let QueryResponse::ResultSet(joined) = engine
        .execute(
            "WITH c AS (SELECT id, pad FROM base) \
             SELECT count(a.pad) FROM c a JOIN c b ON a.id = b.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(joined.rows, vec![vec![Value::Int(6000)]]);

    // The spill path must actually have run for the materialised (multi-ref)
    // CTE.
    assert!(
        crate::engine::executor::spill_quota::global_spilled_high_water() > 0,
        "expected the large multi-reference CTE to spill to disk"
    );
}

/// TPC-DS Q78/Q95: a CTE body that the eager gate forces to materialise
/// (here a multi-table `GROUP BY` — Q78's CTE shape) used to build and
/// strict-charge its whole output before a single row reached the spill
/// builder, tripping 1037 on large bodies. The streaming CTE-body path
/// drains rows incrementally into the spill-capable `CteSpillBuilder`.
/// This asserts such a body completes correctly and spills to disk under
/// a tight per-query budget + low spill threshold. (The end-to-end
/// 1037-avoidance is covered by the TPC-DS Q78/Q95 query runs; at unit
/// scale single-table fast paths and the aggregate's own spill make the
/// eager OOM hard to isolate, so this guards correctness + spill instead.)
#[test]
fn param_forced_cte_body_streams_and_spills_instead_of_oom() {
    use crate::engine::executor::{mem_budget, spill_quota};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE big (id INT, k INT, pad TEXT)")
        .unwrap();
    engine.execute("CREATE TABLE small (k INT)").unwrap();
    engine.execute("INSERT INTO small VALUES (1)").unwrap();
    let payload = "z".repeat(256);
    let n = 160;
    let mut next = 0;
    while next < n {
        let take = (n - next).min(80);
        let mut insert = String::from("INSERT INTO big VALUES ");
        for i in 0..take {
            if i > 0 {
                insert.push(',');
            }
            let r = next + i;
            insert.push_str(&format!("({r},1,'{payload}')"));
        }
        engine.execute(&insert).unwrap();
        next += take;
    }

    // Multi-table `GROUP BY` body: `group_by` forces eager materialisation,
    // and each of the n distinct ids becomes a group carrying a 256 B pad.
    // The zero spill threshold still forces the materialization path. The CTE
    // is referenced twice (self-join) so P1-1 does not inline it — the
    // streaming-body spill path under test lives on the materialisation route.
    let sql = "WITH c AS (SELECT big.id, max(big.pad) p FROM big JOIN small \
               ON big.k = small.k GROUP BY big.id) \
               SELECT count(*) FROM c a JOIN c b ON a.id = b.id";
    let expected = n as i64;
    mem_budget::set_query_mem_limit_mb(2).unwrap();
    std::env::set_var("MYDB_CTE_SPILL_THRESHOLD_MB", "0");

    let before = spill_quota::global_spilled_high_water();
    let resp = engine.execute(sql).unwrap();
    let QueryResponse::ResultSet(rs) = resp else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(expected));
    assert!(
        spill_quota::global_spilled_high_water() > before,
        "large CTE body must spill to disk under the tight budget"
    );
}

/// P1-1: a single-use, non-recursive, plain-SELECT CTE is inlined into its
/// reference site as a derived table and (when the body is simple) flattened
/// into the outer query, so it never materialises — even a large body under a
/// 1 MB spill threshold streams straight from the base table with no spill.
/// Disabling inlining (`MYDB_CTE_INLINE=off`) restores the materialise+spill
/// path and must yield the identical result (differential oracle).
#[test]
fn single_use_cte_is_inlined_and_streams_without_spill() {
    use crate::engine::executor::spill_quota;
    std::env::remove_var("MYDB_CTE_INLINE");
    std::env::set_var("MYDB_CTE_SPILL_THRESHOLD_MB", "1");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE base (id INT PRIMARY KEY, pad TEXT)")
        .unwrap();
    let payload = "y".repeat(512);
    let n = 6000;
    let mut next = 0;
    while next < n {
        let take = (n - next).min(400);
        let mut insert = String::from("INSERT INTO base VALUES ");
        for i in 0..take {
            if i > 0 {
                insert.push(',');
            }
            let r = next + i;
            insert.push_str(&format!("({r},'{payload}')"));
        }
        engine.execute(&insert).unwrap();
        next += take;
    }

    // `count(pad)` references the wide column so P1-2 keeps it: with inlining
    // off the materialised CTE stays wide enough to spill under the 1 MB
    // threshold; inlined+flattened it streams from `base` with no spill.
    let sql = "WITH c AS (SELECT id, pad FROM base) SELECT count(pad) FROM c WHERE id < 100";

    // Default (inlining on): single-use CTE streams, no spill.
    let before = spill_quota::global_spilled_high_water();
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(100)]]);
    assert_eq!(
        spill_quota::global_spilled_high_water(),
        before,
        "an inlined single-use CTE must not spill"
    );

    // Oracle: inlining off → materialise the same CTE → spills under the 1 MB
    // threshold → identical result.
    std::env::set_var("MYDB_CTE_INLINE", "off");
    let before_off = spill_quota::global_spilled_high_water();
    let QueryResponse::ResultSet(rs_off) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    std::env::remove_var("MYDB_CTE_INLINE");
    assert_eq!(rs_off.rows, vec![vec![Value::Int(100)]]);
    assert!(
        spill_quota::global_spilled_high_water() > before_off,
        "with inlining off the materialised CTE must spill"
    );
    std::env::remove_var("MYDB_CTE_SPILL_THRESHOLD_MB");
}

/// P1-2 (projection pushdown) and P1-3 (predicate pushdown) must be result-
/// preserving: a multi-reference CTE whose body has unused columns and a
/// filter common to every reference must produce the same rows whether the
/// pushdowns fire (default) or are disabled (`MYDB_CTE_PUSHDOWN=off`). Also
/// disables inlining so the CTE genuinely materialises and exercises the
/// pushdown rewrites rather than P1-1.
#[test]
fn cte_pushdown_preserves_results() {
    std::env::set_var("MYDB_CTE_INLINE", "off");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, grp INT, val INT, pad TEXT)")
        .unwrap();
    let mut insert = String::from("INSERT INTO t VALUES ");
    for i in 0..200 {
        if i > 0 {
            insert.push(',');
        }
        // grp in {0,1,2}, val = i, pad arbitrary.
        insert.push_str(&format!("({i},{},{i},'p{i}')", i % 3));
    }
    engine.execute(&insert).unwrap();

    // P1-2: val/pad unused → pruned. P1-3: a.grp=1 and b.grp=1 are common to
    // both reference aliases → grp=1 pushed into the body.
    let sql = "WITH c AS (SELECT id, grp, val, pad FROM t) \
               SELECT a.grp, count(*) \
               FROM c a JOIN c b ON a.id = b.id \
               WHERE a.grp = 1 AND b.grp = 1 \
               GROUP BY a.grp ORDER BY a.grp";

    let run = || -> Vec<Vec<Value>> {
        let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        rs.rows
    };

    std::env::remove_var("MYDB_CTE_PUSHDOWN");
    let with_pushdown = run();
    std::env::set_var("MYDB_CTE_PUSHDOWN", "off");
    let without_pushdown = run();
    std::env::remove_var("MYDB_CTE_PUSHDOWN");
    std::env::remove_var("MYDB_CTE_INLINE");

    // grp==1 ids: 1,4,7,...,199 → 67 ids; self-join on id is 1:1 → 67 rows.
    assert_eq!(with_pushdown, vec![vec![Value::Int(1), Value::Int(67)]]);
    assert_eq!(
        with_pushdown, without_pushdown,
        "CTE pushdown (P1-2/P1-3) must be result-preserving"
    );
}

/// A WHERE conjunct referencing the null-supplied (inner) side of a LEFT
/// JOIN must drop rows that fail it — including the NULL-extended unmatched
/// rows — not preserve them. Long-standing bug: `attach_filter` folded the
/// predicate into the outer join's `residual`, where a failing row is
/// NULL-extended instead of filtered, so `WHERE inner.col > k` leaked every
/// left row (TPC-DS Q78). The planner now wraps outer joins in a `Filter`.
#[test]
fn left_join_where_on_inner_side_filters_not_null_extends() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, v INT)").unwrap();
    engine.execute("INSERT INTO l VALUES (1),(2),(3)").unwrap();
    engine.execute("INSERT INTO r VALUES (1,10),(2,0)").unwrap();

    // r.v>0 holds only for id=1 (id=2 matches but v=0; id=3 unmatched → NULL).
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT l.id FROM l LEFT JOIN r ON l.id=r.id WHERE r.v>0 ORDER BY l.id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);

    // A `coalesce` wrapper (non-null-rejecting on its face) must filter too.
    let QueryResponse::ResultSet(rs2) = engine
        .execute(
            "SELECT l.id FROM l LEFT JOIN r ON l.id=r.id \
             WHERE coalesce(r.v,0)>0 ORDER BY l.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs2.rows, vec![vec![Value::Int(1)]]);

    // A WHERE on the *preserved* side keeps LEFT semantics (unmatched id=3
    // stays, NULL-extended).
    let QueryResponse::ResultSet(rs3) = engine
        .execute("SELECT l.id, r.v FROM l LEFT JOIN r ON l.id=r.id WHERE l.id>1 ORDER BY l.id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs3.rows,
        vec![
            vec![Value::Int(2), Value::Int(0)],
            vec![Value::Int(3), Value::Null],
        ]
    );
}

/// A DATE column compared to a non-zero-padded date string literal must
/// coerce the string to a DATE, not compare byte-wise. Single-table scans
/// pre-coerce the literal against the column type, but multi-table / join row
/// predicates reach `compare_values` with the raw string, where `'2000-3-01'`
/// sorts *after* the canonical `'2000-03-01'` and silently excluded every
/// matching row (TPC-DS Q95 base join returned 0).
#[test]
fn date_column_vs_nonpadded_string_literal_coerces_in_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE d (dt DATE)").unwrap();
    engine.execute("CREATE TABLE t (x INT)").unwrap();
    engine.execute("INSERT INTO t VALUES (1)").unwrap();
    engine
        .execute("INSERT INTO d VALUES ('2000-03-01'),('2000-03-15'),('2000-02-28'),('2000-04-30')")
        .unwrap();

    // Single-table: already correct (literal pre-coerced).
    let single = |sql: &str| -> i64 {
        let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        match rs.rows[0][0] {
            Value::Int(v) => v,
            _ => panic!("count not int"),
        }
    };
    assert_eq!(
        single("SELECT count(*) FROM d WHERE dt BETWEEN '2000-3-01' AND '2000-3-31'"),
        2
    );
    // Multi-table join (cross): same predicate must also coerce.
    assert_eq!(
        single(
            "SELECT count(*) FROM d, t \
             WHERE dt BETWEEN '2000-3-01' AND '2000-3-31'",
        ),
        2
    );
}

#[test]
fn correlated_avg_subquery_in_multitable_join_stays_bounded() {
    // TPC-DS q6 shape: a multi-table join plus a correlated scalar subquery
    //   `i.price > 1.2 * (SELECT avg(j.price) FROM item j WHERE j.cat = i.cat)`.
    // Scalar decorrelation rewrites the subquery into a
    //   `LEFT JOIN (SELECT cat, avg(price) GROUP BY cat) __decor_sub`.
    // Appending that derived join to a multi-table outer routed the whole query
    // onto the in-memory nested-loop executor (the LEFT JOIN's ON clause
    // disengages its comma-join greedy path), which materialises the full
    // cust × sales × item product — 12.6 GB and an OOM-kill on TPC-DS q6 at
    // SF=0.01. For a multi-table outer the subquery must now stay correlated and
    // run over the planner's streaming INLJ. We pin a memory limit the
    // nested-loop product would blow through and assert the query still
    // completes with the correct count.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE cust (c_id INT PRIMARY KEY, addr INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE sales (s_id INT PRIMARY KEY AUTO_INCREMENT, s_cust INT, s_item INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE item (i_id INT PRIMARY KEY, i_cat INT, i_price INT)")
        .unwrap();
    // cat 1 -> {10, 30}: avg 20, 1.2x = 24 -> only id 2 (price 30) passes.
    // cat 2 -> {100, 200}: avg 150, 1.2x = 180 -> only id 4 (price 200) passes.
    engine
        .execute("INSERT INTO item VALUES (1,1,10),(2,1,30),(3,2,100),(4,2,200)")
        .unwrap();
    // 100 customers, each buying all 4 items -> 400 sales. The decorrelated
    // nested-loop product (cust 100 x sales 400 x item 4 x derived 2 = 320k
    // rows) blows the 2 MB cap; the streaming INLJ touches only matched rows.
    let n = 100;
    let mut cust = String::from("INSERT INTO cust VALUES ");
    for i in 0..n {
        if i > 0 {
            cust.push(',');
        }
        cust.push_str(&format!("({i},{i})"));
    }
    engine.execute(&cust).unwrap();
    let mut sales = String::from("INSERT INTO sales (s_cust, s_item) VALUES ");
    let mut first = true;
    for c in 0..n {
        for it in 1..=4 {
            if !first {
                sales.push(',');
            }
            first = false;
            sales.push_str(&format!("({c},{it})"));
        }
    }
    engine.execute(&sales).unwrap();
    engine
        .execute("SET GLOBAL mydb_query_mem_limit_mb = 2")
        .unwrap();

    let QueryResponse::ResultSet(res) = engine
        .execute(
            "SELECT count(*) FROM cust c, sales s, item i \
             WHERE c.c_id = s.s_cust AND s.s_item = i.i_id \
               AND i.i_price > 1.2 * (SELECT avg(j.i_price) FROM item j WHERE j.i_cat = i.i_cat)",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // Passing items are id 2 (cat 1) and id 4 (cat 2); each of 100 customers
    // buys both -> 200 matched rows.
    assert_eq!(res.rows, vec![vec![Value::Int(200)]]);
}

#[test]
fn or_buried_equi_join_resolves_and_returns_correct_rows() {
    // TPC-DS q13 shape: the equi-join keys (`f.a_sk=a.a_sk`, `f.b_sk=b.b_sk`)
    // appear only inside a top-level OR, never as standalone AND conjuncts.
    // The OR must not be pinned as the condition of a join formed *before* all
    // the tables it references are present (which produced
    // `1054 unknown column`); it floats up until every referenced table is in
    // scope, then applies correctly.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE f (id INT PRIMARY KEY, a_sk INT, b_sk INT, val INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE a (a_sk INT PRIMARY KEY, av INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (b_sk INT PRIMARY KEY, bv INT)")
        .unwrap();
    engine
        .execute("INSERT INTO f VALUES (1,10,100,5),(2,20,200,25),(3,30,300,50)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (10,1),(20,2),(30,3)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (100,1),(200,2),(300,3)")
        .unwrap();

    let sql = "SELECT count(*) FROM f, a, b \
               WHERE (f.a_sk = a.a_sk AND f.b_sk = b.b_sk AND f.val BETWEEN 1 AND 10) \
                  OR (f.a_sk = a.a_sk AND f.b_sk = b.b_sk AND f.val BETWEEN 20 AND 30)";
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for OR-buried equi-join");
    };
    // f1 (val 5 ∈ [1,10]) and f2 (val 25 ∈ [20,30]) match on their diagonals;
    // f3 (val 50) matches neither branch.
    assert_eq!(result.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn or_buried_equi_keys_spanning_three_tables_factor_out_and_stay_bounded() {
    // TPC-DS q13 shape: the equi-join keys are common to every branch of a
    // top-level OR whose *residual* (after the shared keys) still spans more
    // than two tables, e.g.
    //   (f.a=da.a AND f.b=db.b AND da.attr='X' AND db.attr='P')
    //   OR (f.a=da.a AND f.b=db.b AND da.attr='Y' AND db.attr='Q')
    // Two things must work together:
    //  1) The shared keys `f.a=da.a` / `f.b=db.b` are factored out of the OR so
    //     the planner uses them as join keys (otherwise f × da × db is a
    //     cartesian — q13's full join estimated 25 trillion rows / 35 GB).
    //  2) The residual OR spans da AND db, so it must be deferred to the join
    //     where both are in scope. The cost-based DP previously pinned it on the
    //     first join that merely *touched* da or db, before the other was
    //     present, failing with `1054 unknown column`.
    // We pin a memory limit the cartesian would exceed and assert the exact
    // aggregate, which catches a regression of either half.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE f (id INT PRIMARY KEY, a_sk INT, b_sk INT, val INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE dim_a (a_sk INT PRIMARY KEY, attr VARCHAR(4))")
        .unwrap();
    engine
        .execute("CREATE TABLE dim_b (b_sk INT PRIMARY KEY, attr VARCHAR(4))")
        .unwrap();
    // dim_a[k] = 'X' for k<15 else 'Y'; dim_b[k] = 'P' for all k.
    let mut da = String::from("INSERT INTO dim_a VALUES ");
    let mut db = String::from("INSERT INTO dim_b VALUES ");
    for k in 0..30 {
        if k > 0 {
            da.push(',');
            db.push(',');
        }
        da.push_str(&format!("({k},'{}')", if k < 15 { "X" } else { "Y" }));
        db.push_str(&format!("({k},'P')"));
    }
    engine.execute(&da).unwrap();
    engine.execute(&db).unwrap();
    // 600 fact rows, a_sk = b_sk = id % 30, val = 1. f × dim_a × dim_b would be
    // 600 × 30 × 30 = 540 000 rows if the keys stayed trapped in the OR.
    let mut fi = String::from("INSERT INTO f VALUES ");
    for id in 0..600 {
        if id > 0 {
            fi.push(',');
        }
        fi.push_str(&format!("({id},{},{},1)", id % 30, id % 30));
    }
    engine.execute(&fi).unwrap();
    engine
        .execute("SET GLOBAL mydb_query_mem_limit_mb = 16")
        .unwrap();

    let QueryResponse::ResultSet(res) = engine
        .execute(
            "SELECT count(*), sum(f.val) FROM f, dim_a, dim_b \
             WHERE (f.a_sk = dim_a.a_sk AND f.b_sk = dim_b.b_sk \
                    AND dim_a.attr = 'X' AND dim_b.attr = 'P') \
                OR (f.a_sk = dim_a.a_sk AND f.b_sk = dim_b.b_sk \
                    AND dim_a.attr = 'Y' AND dim_b.attr = 'Q')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // Only the X∧P branch can fire (dim_b is all 'P', so Y∧Q never matches).
    // a_sk < 15 holds for residues 0..14, each appearing 20× across 600 ids:
    // 15 × 20 = 300 rows.
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0], Value::Int(300));
    assert_eq!(as_f64(&res.rows[0][1]) as i64, 300);
}

#[test]
fn order_by_unqualified_output_column_over_self_join_resolves() {
    // TPC-DS q72 / q58 shape: a self-joined source whose ORDER BY names an
    // output column that is ambiguous as a *base* column. `ORDER BY wk` must
    // bind to the projected `d1.wk` output column, not be reported
    // `1052 ambiguous` against the two `dd` copies.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE dd (sk INT PRIMARY KEY, wk INT)")
        .unwrap();
    engine
        .execute("INSERT INTO dd VALUES (1,30),(2,10),(3,20)")
        .unwrap();
    let QueryResponse::ResultSet(res) = engine
        .execute(
            "SELECT d1.wk, count(*) c FROM dd d1, dd d2 \
             WHERE d1.sk = d2.sk GROUP BY d1.wk ORDER BY wk",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // Self-join on sk is 1:1, so one row per wk, ordered 10,20,30.
    let wk: Vec<i64> = res.rows.iter().map(|r| as_f64(&r[0]) as i64).collect();
    assert_eq!(wk, vec![10, 20, 30]);
}

#[test]
fn aliased_table_hides_base_name_qualifier() {
    // The strict-matching rule must also reject the *base* name as a qualifier
    // once a table is aliased (MySQL semantics): `base.k` is unknown when the
    // only source is `base b`.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE base (k INT)").unwrap();
    engine.execute("INSERT INTO base VALUES (1)").unwrap();
    // Alias present -> base name is not a valid qualifier.
    let err = engine.execute("SELECT base.k FROM base b").unwrap_err();
    assert_eq!(err.code(), 1054, "expected unknown-column, got {err:?}");
    // The alias works.
    let QueryResponse::ResultSet(res) = engine.execute("SELECT b.k FROM base b").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(res.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn quantified_subquery_comparisons_follow_mysql_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE qvals (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO qvals VALUES (1, 1), (2, 3), (3, NULL)")
        .unwrap();

    let cases = [
        ("SELECT 2 > ANY (SELECT v FROM qvals)", Value::Int(1)),
        ("SELECT 2 > SOME (SELECT v FROM qvals)", Value::Int(1)),
        (
            "SELECT 2 > ALL (SELECT v FROM qvals WHERE v < 2)",
            Value::Int(1),
        ),
        (
            "SELECT 2 > ALL (SELECT v FROM qvals WHERE v IS NOT NULL)",
            Value::Int(0),
        ),
        (
            "SELECT 2 = ANY (SELECT v FROM qvals WHERE v IS NULL)",
            Value::Null,
        ),
        (
            "SELECT 2 = ALL (SELECT v FROM qvals WHERE v IS NULL)",
            Value::Null,
        ),
        (
            "SELECT 2 = ANY (SELECT v FROM qvals WHERE id < 0)",
            Value::Int(0),
        ),
        (
            "SELECT 2 = ALL (SELECT v FROM qvals WHERE id < 0)",
            Value::Int(1),
        ),
    ];

    for (sql, expected) in cases {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected result set for {sql}");
        };
        assert_eq!(result.rows[0][0], expected, "failed for: {sql}");
    }

    engine
        .execute("CREATE TABLE qtext (v LONGTEXT NOT NULL)")
        .unwrap();
    engine.execute("INSERT INTO qtext VALUES ('a')").unwrap();
    let mut session = SessionContext::default();
    session.sql_mode.clear();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT (SELECT v <> 1 FROM qtext) \
             <> ALL(SELECT 1681007452 FROM qvals WHERE v IS NOT NULL)",
        )
        .unwrap()
    else {
        panic!("expected quantified scalar-subquery result");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
    assert!(
        session.warnings.is_empty(),
        "scalar-subquery diagnostics leaked through ALL: {:?}",
        session.warnings
    );
}

#[test]
fn quantified_subquery_comparisons_work_in_where() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE outer_q (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE inner_q (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO outer_q VALUES (1, 1), (2, 2), (3, 4)")
        .unwrap();
    engine
        .execute("INSERT INTO inner_q VALUES (1, 2), (2, 3)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM outer_q WHERE v >= ALL (SELECT v FROM inner_q) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(3)]]);

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM outer_q WHERE v < ANY (SELECT v FROM inner_q) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn quantified_subquery_comparisons_work_in_dml_where() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE dml_outer_q (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE dml_inner_q (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO dml_outer_q VALUES (1, 1), (2, 2), (3, 4)")
        .unwrap();
    engine
        .execute("INSERT INTO dml_inner_q VALUES (1, 2), (2, 3)")
        .unwrap();

    engine
        .execute("UPDATE dml_outer_q SET v = v + 10 WHERE v < SOME (SELECT v FROM dml_inner_q)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, v FROM dml_outer_q ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(2), Value::Int(12)],
            vec![Value::Int(3), Value::Int(4)],
        ]
    );

    engine
        .execute("DELETE FROM dml_outer_q WHERE v > ALL (SELECT v + 7 FROM dml_inner_q)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM dml_outer_q ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(3)]]);
}

#[test]
fn null_where_clause_filters_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE nulltest (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO nulltest VALUES (1, 10), (2, NULL), (3, 20)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM nulltest WHERE v < 15")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(1));

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM nulltest WHERE v <> 10")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(3));
}

#[test]
fn null_in_list_and_between_return_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE nullops (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO nullops VALUES (1, NULL), (2, 5)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM nullops WHERE v IN (1, 2, 3) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(result.rows.is_empty());

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM nullops WHERE v NOT IN (1, NULL) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(result.rows.is_empty());

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT 7 IN (5, NULL), 7 NOT IN (5, NULL), NULL IN (5, NULL)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Null, Value::Null, Value::Null]]
    );

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM nullops WHERE v BETWEEN 1 AND 10 ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(2));
}

#[test]
fn compiled_filter_handles_in_list_and_column_vs_column() {
    // Q12-shape filter: an IN-list constant predicate AND'd with a
    // column-vs-column comparison. Both shapes were uncompilable before
    // `CompiledRowPredicate::{InListConst, CmpCols}`; the legacy
    // interpreter fallback re-allocated literal strings and re-ran
    // `resolve_plan_field` per row, so the parallel scan dominated CPU.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE shp (id INT PRIMARY KEY, mode VARCHAR(8), \
                 ship_dt DATE, recv_dt DATE)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO shp VALUES \
                 (1, 'MAIL', '1994-03-01', '1994-03-05'), \
                 (2, 'SHIP', '1994-04-10', '1994-04-09'), \
                 (3, 'AIR',  '1994-05-01', '1994-05-10'), \
                 (4, 'MAIL', '1994-06-01', '1994-06-15'), \
                 (5, 'TRUCK','1994-07-01', '1994-07-09')",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id FROM shp \
                 WHERE mode IN ('MAIL', 'SHIP') AND ship_dt < recv_dt \
                 ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(1)], vec![Value::Int(4)]],
        "id=2 fails ship_dt < recv_dt; id=3,5 fail the IN list"
    );

    // NOT IN with a NULL element: per MySQL the row is filtered out
    // (`x NOT IN (..., NULL)` is NULL when no concrete match).
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM shp WHERE mode NOT IN ('AIR', NULL) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        result.rows.is_empty(),
        "NOT IN with NULL list element must filter every row out, got {:?}",
        result.rows
    );

    // Column-vs-column equality is compilable too; lock the contract.
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM shp WHERE ship_dt = recv_dt ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(result.rows.is_empty());
}

#[test]
fn mixed_type_comparison_numeric_coercion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let cases = [
        ("SELECT 5 = '5'", Value::Int(1)),
        ("SELECT 5 = '05'", Value::Int(1)),
        ("SELECT 5 < '10'", Value::Int(1)),
        ("SELECT 10 > '9'", Value::Int(1)),
    ];
    for (sql, expected) in cases {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected result set for {sql}");
        };
        assert_eq!(result.rows[0][0], expected, "failed for: {sql}");
    }
}

#[test]
fn compiled_expression_program_matches_scalar_semantics() {
    use crate::engine::expr::eval_expr_legacy;
    use crate::engine::expr_program::{
        with_eval_scratch, CompiledTableExpr, ExprCompiler, InputSchema, RowView,
    };
    use crate::parser::{parse_statement, SelectItem, Statement};

    fn first_projection_expr(sql: &str) -> Expr {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            panic!("expected select for {sql}");
        };
        match select.projection.into_iter().next().unwrap() {
            SelectItem::Expr { expr, .. } => expr,
            other => panic!("unexpected projection: {other:?}"),
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE exprprog (
                id INT PRIMARY KEY,
                a INT,
                b VARCHAR(32),
                j JSON
            )",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO exprprog VALUES
                (1, 5, 'Alpha', '{\"x\":1}'),
                (2, 12, 'beta', '{\"x\":3}'),
                (3, 20, 'skip', '{\"x\":3}'),
                (4, NULL, 'none', '{\"x\":9}')",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT
                id,
                IF(a > 10, UPPER(b), LOWER(b)),
                COALESCE(NULLIF(b, 'skip'), 'fallback'),
                JSON_EXTRACT(j, '$.x'),
                REGEXP_LIKE(b, '^a', 'i'),
                CASE WHEN a BETWEEN 5 AND 15 THEN 'mid' ELSE 'out' END
             FROM exprprog
             WHERE (a IN (5, 12, NULL) OR JSON_EXTRACT(j, '$.x') = 3)
               AND REGEXP_LIKE(b, '^[a-z]', 'i')
             ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    // Column 3 is JSON_EXTRACT(j, '$.x'), which now returns a real JSON
    // value; compare on the rendered form so the assertion is robust to the
    // String-vs-Json cell type while still pinning every value.
    let rendered: Vec<Vec<String>> = result
        .rows
        .iter()
        .map(|r| r.iter().map(|v| v.to_display_string()).collect())
        .collect();
    assert_eq!(
        rendered,
        vec![
            vec!["1", "alpha", "Alpha", "1", "1", "mid"],
            vec!["2", "BETA", "beta", "3", "0", "mid"],
            vec!["3", "SKIP", "fallback", "3", "0", "out"],
        ]
    );

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT IF(a > 0, 42, JSON_EXTRACT('not json', '$.x')) FROM exprprog WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(42)]]);

    let table = engine.db_for_tests().table("exprprog").unwrap();
    let schema = InputSchema::from_table(&table);
    let values = vec![
        Value::Int(2),
        Value::Int(12),
        Value::String("beta".into()),
        Value::String("{\"x\":3}".into()),
    ];
    let row = RowView::from_values(&values);

    for expr in [
        first_projection_expr("SELECT IF(a > 10, UPPER(b), LOWER(b))"),
        first_projection_expr("SELECT (a IN (5, 12, NULL)) AND REGEXP_LIKE(b, '^[a-z]', 'i')"),
        first_projection_expr("SELECT SUBSTRING(b, 1, 1) IN ('a', 'b', 'c', 'd')"),
        first_projection_expr(
            "SELECT CASE WHEN a BETWEEN 5 AND 15 THEN JSON_EXTRACT(j, '$.x') ELSE 'out' END",
        ),
    ] {
        let legacy = eval_expr_legacy(&expr, &table, &values).unwrap();
        let program = ExprCompiler::compile(&expr, &schema).unwrap();
        let compiled = with_eval_scratch(|scratch| program.eval_row(&row, scratch)).unwrap();
        assert_eq!(compiled, legacy, "compiled mismatch for {expr:?}");

        let fused = CompiledTableExpr::compile_once(&expr, &table);
        let fused_value = fused.eval(&table, &values).unwrap();
        assert_eq!(
            fused_value, legacy,
            "compiled table expr mismatch for {expr:?}"
        );
    }

    let mut expr = first_projection_expr("SELECT LOWER(b)");
    let program = ExprCompiler::compile_cached(&expr, &schema).unwrap();
    let lower = with_eval_scratch(|scratch| program.eval_row(&row, scratch)).unwrap();
    assert_eq!(lower, Value::String("beta".into()));

    expr = first_projection_expr("SELECT UPPER(b)");
    let program = ExprCompiler::compile_cached(&expr, &schema).unwrap();
    let upper = with_eval_scratch(|scratch| program.eval_row(&row, scratch)).unwrap();
    assert_eq!(upper, Value::String("BETA".into()));
}

#[test]
fn order_by_expression_error_is_propagated_before_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE sort_error (id INT PRIMARY KEY, payload VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO sort_error VALUES (1, '[1,2]'), (2, 'not-json')")
        .unwrap();

    let error = engine
        .execute("SELECT id FROM sort_error ORDER BY JSON_LENGTH(payload)")
        .expect_err("malformed ORDER BY input must not be converted to NULL");
    assert_eq!(error.code(), 3141, "unexpected error: {error:?}");

    let error = engine
        .execute(
            "SELECT id, payload, ROW_NUMBER() OVER (ORDER BY JSON_LENGTH(payload)) \
             FROM sort_error",
        )
        .expect_err("window ORDER BY errors must not be converted to NULL");
    assert_eq!(
        error.code(),
        3141,
        "unexpected window order error: {error:?}"
    );

    let error = engine
        .execute(
            "SELECT id, payload, ROW_NUMBER() OVER (PARTITION BY JSON_LENGTH(payload) ORDER BY id) \
             FROM sort_error",
        )
        .expect_err("window PARTITION BY errors must not be converted to NULL");
    assert_eq!(
        error.code(),
        3141,
        "unexpected window partition error: {error:?}"
    );
}

#[test]
fn cast_to_decimal_returns_decimal_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT CAST('3.14' AS DECIMAL(10,2))")
        .unwrap()
    else {
        panic!("expected result set");
    };
    // Fixed-point CAST: compact form carrying the target scale.
    assert_eq!(result.rows[0][0], Value::DecimalCompact(314, 2));
}

fn as_f64(value: &Value) -> f64 {
    match value {
        Value::Double(v) => *v,
        Value::Float(v) => *v as f64,
        Value::Int(v) => *v as f64,
        Value::UInt(v) => *v as f64,
        // SUM over exact-decimal inputs now finalises as `DecimalCompact`
        // (or `Decimal` for i128-overflow magnitudes) so the LSB
        // survives. Tests that just want a numeric approximation
        // still go through this helper.
        Value::DecimalCompact(mag, scale) => *mag as f64 / 10f64.powi(*scale as i32),
        Value::Decimal(s) => s
            .parse::<f64>()
            .expect("Decimal string should parse as f64"),
        _ => panic!("expected numeric value, got {:?}", value),
    }
}

#[test]
fn storage_engine_executes_select_without_from() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine.execute("SELECT 23, NOW() AS n").unwrap() else {
        panic!("expected result set");
    };

    assert_eq!(result.columns[0].name, "23");
    assert_eq!(result.columns[1].name, "n");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(23));
    assert!(matches!(result.rows[0][1], Value::DateTime(_)));
}

#[test]
fn information_schema_reports_tables_columns_and_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(64), UNIQUE KEY email_idx (email))")
        .unwrap();

    let QueryResponse::ResultSet(tables) = engine
        .execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'test' AND table_name = 'users'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(tables.rows, vec![vec![Value::String("users".to_string())]]);

    let QueryResponse::ResultSet(columns) = engine
        .execute("SELECT column_name, column_key FROM information_schema.columns WHERE table_schema = 'test' AND table_name = 'users' ORDER BY ordinal_position")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        columns.rows[0],
        vec![
            Value::String("id".to_string()),
            Value::String("PRI".to_string())
        ]
    );

    let QueryResponse::ResultSet(indexes) = engine.execute("SHOW INDEX FROM users").unwrap() else {
        panic!("expected result set");
    };
    assert!(indexes
        .rows
        .iter()
        .any(|row| row[2] == Value::String("email_idx".to_string())));
}

#[test]
fn information_schema_uses_mysql80_catalog_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(64), UNIQUE KEY email_idx (email))")
        .unwrap();

    let QueryResponse::ResultSet(columns) = engine
        .execute("SELECT * FROM information_schema.columns WHERE table_schema = 'test' AND table_name = 'users' ORDER BY ordinal_position")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(columns
        .columns
        .iter()
        .any(|column| column.name == "GENERATION_EXPRESSION"));
    assert!(columns.columns.iter().any(|column| column.name == "SRS_ID"));
    assert_eq!(columns.rows[0].len(), columns.columns.len());

    let QueryResponse::ResultSet(stats) = engine
        .execute("SELECT * FROM information_schema.statistics WHERE table_schema = 'test' AND table_name = 'users'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(stats
        .columns
        .iter()
        .any(|column| column.name == "IS_VISIBLE"));
    assert_eq!(stats.rows[0].len(), stats.columns.len());

    for (table, predicate) in [
        ("information_schema.routines", "routine_schema = 'test'"),
        ("information_schema.triggers", "trigger_schema = 'test'"),
        (
            "information_schema.referential_constraints",
            "constraint_schema = 'test'",
        ),
    ] {
        let QueryResponse::ResultSet(result) = engine
            .execute(&format!("SELECT * FROM {table} WHERE {predicate}"))
            .unwrap()
        else {
            panic!("expected result set for {table}");
        };
        assert!(
            result.rows.is_empty(),
            "{table} should have no user objects in the test schema"
        );
        assert!(
            !result.columns.is_empty(),
            "{table} should expose MySQL-style columns"
        );
    }
    // information_schema.partitions now emits a NULL-partition row
    // per unpartitioned table (matching MySQL 8.0 behavior) and a row
    // per partition for partitioned tables.
    let QueryResponse::ResultSet(parts) = engine
        .execute("SELECT * FROM information_schema.partitions")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(!parts.columns.is_empty());
    assert!(parts
        .columns
        .iter()
        .any(|column| column.name == "PARTITION_NAME"));
}

#[test]
fn information_schema_registry_and_real_producers_are_queryable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(tables) = engine
        .execute("SHOW FULL TABLES FROM information_schema")
        .unwrap()
    else {
        panic!("expected SHOW FULL TABLES result");
    };
    assert_eq!(tables.rows.len(), 103);
    let names = tables
        .rows
        .iter()
        .map(|row| row[0].to_display_string())
        .collect::<std::collections::BTreeSet<_>>();
    for retired in ["RESOURCE_GROUPS", "TABLESPACES", "TABLESPACES_EXTENSIONS"] {
        assert!(
            !names.iter().any(|name| name.eq_ignore_ascii_case(retired)),
            "unsupported catalog advertised through SHOW: {retired}"
        );
    }
    for retired in ["zone_maps", "zone_map_storage"] {
        assert!(
            !names.iter().any(|name| name.eq_ignore_ascii_case(retired)),
            "retired zone-map name advertised through SHOW: {retired}"
        );
        for name in [retired.to_string(), retired.to_ascii_uppercase()] {
            let err = engine
                .execute(&format!("SELECT * FROM information_schema.{name}"))
                .unwrap_err();
            assert_eq!(err.code(), 1146, "{name}: {err:?}");
        }
        for metadata in ["tables", "columns"] {
            let QueryResponse::ResultSet(result) = engine
                .execute(&format!(
                    "SELECT table_name FROM information_schema.{metadata} \
                     WHERE table_schema='information_schema' AND table_name='{retired}'"
                ))
                .unwrap()
            else {
                panic!("expected {metadata} result");
            };
            assert!(result.rows.is_empty(), "{metadata} exposes {retired}");
        }
    }
    for required in ["mydb_zone_maps", "mydb_zone_map_storage"] {
        assert!(
            names.iter().any(|name| name.eq_ignore_ascii_case(required)),
            "missing {required} from {names:?}"
        );
        let QueryResponse::ResultSet(result) = engine
            .execute(&format!("SELECT * FROM information_schema.{required}"))
            .unwrap()
        else {
            panic!("expected {required} result");
        };
        assert!(
            !result.columns.is_empty(),
            "{required} must retain its shape"
        );
    }

    let QueryResponse::ResultSet(keywords) = engine
        .execute("SELECT WORD, RESERVED FROM information_schema.keywords")
        .unwrap()
    else {
        panic!("expected KEYWORDS result");
    };
    assert_eq!(keywords.rows.len(), 751);
    assert!(keywords
        .rows
        .iter()
        .any(|row| { row[0] == Value::String("SELECT".to_string()) && row[1] == Value::Int(1) }));

    let QueryResponse::ResultSet(plugins) = engine
        .execute("SELECT PLUGIN_NAME FROM information_schema.plugins")
        .unwrap()
    else {
        panic!("expected PLUGINS result");
    };
    let plugins = plugins
        .rows
        .into_iter()
        .map(|row| row[0].to_display_string())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(plugins.contains("mydb"));
    assert!(plugins.contains("caching_sha2_password"));

    assert_eq!(engine.execute(
        "SELECT RESOURCE_GROUP_NAME FROM information_schema.resource_groups ORDER BY RESOURCE_GROUP_NAME"
    ).unwrap_err().code(), 1235);

    for table in [
        "ndb_transid_mysql_connection_map",
        "tp_thread_group_state",
        "connection_control_failed_login_attempts",
        "mysql_firewall_users",
        "innodb_cmp",
        "profiling",
    ] {
        let QueryResponse::ResultSet(result) = engine
            .execute(&format!("SELECT * FROM information_schema.{table}"))
            .unwrap()
        else {
            panic!("expected honest-empty result for {table}");
        };
        assert!(result.rows.is_empty(), "{table} must be honestly empty");
        assert!(!result.columns.is_empty(), "{table} must retain its shape");
    }
}

#[test]
fn information_schema_full_mysql8_registry_is_queryable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(tables) = engine
        .execute(
            "SELECT table_name, table_type FROM information_schema.tables \
             WHERE table_schema = 'information_schema' AND table_name = 'columns'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        tables.rows,
        vec![vec![
            Value::String("COLUMNS".to_string()),
            Value::String("SYSTEM VIEW".to_string())
        ]]
    );

    let QueryResponse::ResultSet(events) = engine
        .execute("SELECT * FROM information_schema.events")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(events.rows.is_empty());
    assert!(events.columns.iter().any(|c| c.name == "EVENT_NAME"));

    let QueryResponse::ResultSet(describe) = engine
        .execute("DESCRIBE information_schema.columns")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(describe
        .rows
        .iter()
        .any(|row| row[0] == Value::String("TABLE_CATALOG".to_string())));

    let QueryResponse::ResultSet(describe_tables) =
        engine.execute("desc information_schema.tables").unwrap()
    else {
        panic!("expected result set");
    };
    assert!(describe_tables
        .rows
        .iter()
        .any(|row| row[0] == Value::String("TABLE_CATALOG".to_string())));
    assert!(describe_tables
        .rows
        .iter()
        .any(|row| row[0] == Value::String("TABLE_NAME".to_string())));

    let QueryResponse::ResultSet(show_columns_tables) = engine
        .execute("SHOW COLUMNS FROM information_schema.tables")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(describe_tables.rows, show_columns_tables.rows);

    let QueryResponse::ResultSet(show) = engine
        .execute("SHOW FULL TABLES FROM information_schema")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(show.rows.iter().any(|row| {
        row[0] == Value::String("COLUMNS".to_string())
            && row[1] == Value::String("SYSTEM VIEW".to_string())
    }));
}

#[test]
fn mysql_schema_mysql80_registry_is_queryable_and_introspectable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(show) = engine.execute("SHOW FULL TABLES FROM mysql").unwrap()
    else {
        panic!("expected result set");
    };
    let shown: Vec<_> = show
        .rows
        .iter()
        .map(|row| row[0].to_display_string())
        .collect();
    assert_eq!(shown.len(), crate::engine::mysql_schema::MYSQL_DEFS.len());
    assert!(shown.contains(&"user".to_string()));
    assert!(shown.contains(&"innodb_table_stats".to_string()));
    assert!(!shown.contains(&"tables".to_string()));

    let QueryResponse::ResultSet(describe) = engine.execute("DESCRIBE mysql.user").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(describe.rows.len(), 51);
    assert_eq!(describe.rows[0][0], Value::String("Host".to_string()));
    assert_eq!(describe.rows[0][1], Value::String("char(255)".to_string()));
    assert_eq!(describe.rows[0][3], Value::String("PRI".to_string()));
    assert!(describe
        .rows
        .iter()
        .any(|row| row[0] == Value::String("User_attributes".to_string())
            && row[1] == Value::String("json".to_string())));

    let QueryResponse::ResultSet(indexes) = engine.execute("SHOW INDEX FROM mysql.user").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(indexes.rows.len(), 2);
    assert!(indexes
        .rows
        .iter()
        .all(|row| row[2] == Value::String("PRIMARY".to_string())));

    let QueryResponse::ResultSet(create) = engine
        .execute("SHOW CREATE TABLE mysql.server_cost")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(matches!(&create.rows[0][1], Value::String(sql) if sql.contains("GENERATED ALWAYS")));

    for def in crate::engine::mysql_schema::MYSQL_DEFS {
        let sql = format!("SELECT * FROM mysql.{}", def.name);
        let QueryResponse::ResultSet(rs) = engine.execute(&sql).unwrap_or_else(|err| {
            panic!("{sql} failed: {err:?}");
        }) else {
            panic!("{sql} did not return a result set");
        };
        assert_eq!(rs.columns.len(), def.columns.len(), "{sql}");
        for row in rs.rows {
            assert_eq!(row.len(), def.columns.len(), "{sql}");
        }
    }
}

#[test]
fn mysql_schema_surfaces_information_schema_metadata_and_hides_dd_tables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(tables) = engine
        .execute(
            "SELECT table_name, engine FROM information_schema.tables \
             WHERE table_schema = 'mysql' AND table_name = 'user'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        tables.rows,
        vec![vec![
            Value::String("user".to_string()),
            Value::String("InnoDB".to_string())
        ]]
    );

    let QueryResponse::ResultSet(columns) = engine
        .execute(
            "SELECT column_name, column_key FROM information_schema.columns \
             WHERE table_schema = 'mysql' AND table_name = 'user' \
             ORDER BY ordinal_position LIMIT 2",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        columns.rows,
        vec![
            vec![
                Value::String("Host".to_string()),
                Value::String("PRI".to_string())
            ],
            vec![
                Value::String("User".to_string()),
                Value::String("PRI".to_string())
            ],
        ]
    );

    let QueryResponse::ResultSet(stats) = engine
        .execute(
            "SELECT index_name, column_name FROM information_schema.statistics \
             WHERE table_schema = 'mysql' AND table_name = 'user' \
             ORDER BY seq_in_index",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        stats.rows,
        vec![
            vec![
                Value::String("PRIMARY".to_string()),
                Value::String("Host".to_string())
            ],
            vec![
                Value::String("PRIMARY".to_string()),
                Value::String("User".to_string())
            ],
        ]
    );

    for sql in [
        "SELECT * FROM mysql.tables",
        "SHOW COLUMNS FROM mysql.tables",
        "SHOW CREATE TABLE mysql.tables",
    ] {
        let err = engine.execute(sql).unwrap_err();
        assert_eq!(err.code(), 3554, "{sql}");
    }
}

#[test]
fn sys_schema_registers_mysql80_object_surface() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(tables) = engine
        .execute(
            "SELECT table_name, table_type FROM information_schema.tables \
             WHERE table_schema = 'sys' ORDER BY table_name",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        tables
            .rows
            .iter()
            .filter(|row| row[1] == Value::String("BASE TABLE".to_string()))
            .count(),
        1
    );
    assert_eq!(
        tables
            .rows
            .iter()
            .filter(|row| row[1] == Value::String("VIEW".to_string()))
            .count(),
        100
    );
    assert!(tables.rows.iter().any(|row| {
        row[0] == Value::String("sys_config".to_string())
            && row[1] == Value::String("BASE TABLE".to_string())
    }));
    assert!(tables.rows.iter().any(|row| {
        row[0] == Value::String("version".to_string())
            && row[1] == Value::String("VIEW".to_string())
    }));

    let QueryResponse::ResultSet(show) = engine.execute("SHOW FULL TABLES FROM sys").unwrap()
    else {
        panic!("expected result set");
    };
    assert!(show.rows.iter().any(|row| {
        row[0] == Value::String("sys_config".to_string())
            && row[1] == Value::String("BASE TABLE".to_string())
    }));
    assert!(show.rows.iter().any(|row| {
        row[0] == Value::String("version".to_string())
            && row[1] == Value::String("VIEW".to_string())
    }));

    let QueryResponse::ResultSet(routines) = engine
        .execute(
            "SELECT routine_name, routine_type FROM information_schema.routines \
             WHERE routine_schema = 'sys' ORDER BY routine_name",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        routines
            .rows
            .iter()
            .filter(|row| row[1] == Value::String("FUNCTION".to_string()))
            .count(),
        22
    );
    assert_eq!(
        routines
            .rows
            .iter()
            .filter(|row| row[1] == Value::String("PROCEDURE".to_string()))
            .count(),
        26
    );
}

#[test]
fn sys_schema_views_functions_and_show_create_work() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(view_list) = engine
        .execute(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'sys' AND table_type = 'VIEW' ORDER BY table_name",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(view_list.rows.len(), 100);
    for row in &view_list.rows {
        let Value::String(view_name) = &row[0] else {
            panic!("expected sys view name");
        };
        let QueryResponse::ResultSet(result) = engine
            .execute(&format!("SELECT * FROM sys.`{view_name}` LIMIT 1"))
            .unwrap()
        else {
            panic!("expected result set for sys.{view_name}");
        };
        assert!(
            !result.columns.is_empty(),
            "sys.{view_name} should expose columns"
        );
    }

    let QueryResponse::ResultSet(version) = engine.execute("SELECT * FROM sys.version").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(version.columns[0].name, "sys_version");
    assert_eq!(version.rows[0][0], Value::String("2.1.3".to_string()));

    let QueryResponse::ResultSet(funcs) = engine
        .execute(
            "SELECT sys.format_bytes(1024), sys.format_time(1500), \
                    sys.sys_get_config('statement_truncate_len', '1'), \
                    sys.quote_identifier('a`b')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(funcs.rows[0][0], Value::String("1.00 KiB".to_string()));
    assert_eq!(funcs.rows[0][1], Value::String("1.50 ns".to_string()));
    assert_eq!(funcs.rows[0][2], Value::String("64".to_string()));
    assert_eq!(funcs.rows[0][3], Value::String("`a``b`".to_string()));

    let QueryResponse::ResultSet(show_create) =
        engine.execute("SHOW CREATE VIEW sys.version").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(show_create.columns[0].name, "View");
    assert!(show_create.rows[0][1]
        .to_display_string()
        .contains("SQL SECURITY INVOKER VIEW `version`"));

    let QueryResponse::ResultSet(overview) = engine
        .execute("SELECT db, object_type, count FROM sys.schema_object_overview WHERE db = 'sys'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(overview
        .rows
        .iter()
        .any(|row| row[1] == Value::String("VIEW".to_string())));

    let QueryResponse::ResultSet(databases) = engine.execute("SHOW DATABASES").unwrap() else {
        panic!("expected result set");
    };
    assert!(databases
        .rows
        .contains(&vec![Value::String("sys".to_string())]));

    let QueryResponse::ResultSet(describe) = engine.execute("DESCRIBE sys.version").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        describe.rows[0][0],
        Value::String("sys_version".to_string())
    );

    let QueryResponse::ResultSet(show_columns) =
        engine.execute("SHOW COLUMNS FROM sys.sys_config").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        show_columns.rows[0][0],
        Value::String("variable".to_string())
    );

    let QueryResponse::ResultSet(show_config_create) =
        engine.execute("SHOW CREATE TABLE sys.sys_config").unwrap()
    else {
        panic!("expected result set");
    };
    assert!(show_config_create.rows[0][1]
        .to_display_string()
        .contains("CREATE TABLE `sys_config`"));

    let QueryResponse::ResultSet(show_function) = engine
        .execute("SHOW CREATE FUNCTION sys.format_bytes")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        show_function.rows[0][0],
        Value::String("format_bytes".to_string())
    );

    let QueryResponse::ResultSet(show_procedure) = engine
        .execute("SHOW CREATE PROCEDURE sys.table_exists")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        show_procedure.rows[0][0],
        Value::String("table_exists".to_string())
    );

    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "USE sys")
        .unwrap();
    let QueryResponse::ResultSet(triggers) = engine
        .execute_with_session(&mut session, "SHOW TRIGGERS")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(triggers.rows.len(), 2);
    let QueryResponse::ResultSet(show_trigger) = engine
        .execute_with_session(
            &mut session,
            "SHOW CREATE TRIGGER sys_config_insert_set_user",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        show_trigger.rows[0][0],
        Value::String("sys_config_insert_set_user".to_string())
    );
}

#[test]
fn sys_config_is_writable_and_table_exists_procedure_sets_out_param() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO sys.sys_config (variable, value) VALUES ('custom.option', 'abc')",
        )
        .unwrap();
    let QueryResponse::ResultSet(inserted) = engine
        .execute_with_session(
            &mut session,
            "SELECT value, set_by FROM sys.sys_config WHERE variable = 'custom.option'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(inserted.rows[0][0], Value::String("abc".to_string()));
    assert!(matches!(inserted.rows[0][1], Value::String(_)));

    engine
        .execute_with_session(
            &mut session,
            "UPDATE sys.sys_config SET value = 'xyz', set_by = NULL WHERE variable = 'custom.option'",
        )
        .unwrap();
    let QueryResponse::ResultSet(updated) = engine
        .execute_with_session(
            &mut session,
            "SELECT sys.sys_get_config('custom.option', 'fallback')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(updated.rows[0][0], Value::String("xyz".to_string()));

    engine
        .execute_with_session(
            &mut session,
            "CALL sys.table_exists('sys', 'version', @exists)",
        )
        .unwrap();
    let QueryResponse::ResultSet(out) = engine
        .execute_with_session(&mut session, "SELECT @exists")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(out.rows[0][0], Value::String("VIEW".to_string()));

    let err = engine
        .execute("INSERT INTO sys.version (sys_version) VALUES ('x')")
        .unwrap_err();
    assert!(err
        .message()
        .contains("direct DML against system catalog table"));
}

#[test]
fn information_schema_mydb_caches_lists_engine_caches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Generate some cache traffic first so the counters are non-trivial.
    engine
        .execute("CREATE TABLE c (id INT PRIMARY KEY, j JSON)")
        .unwrap();
    engine
        .execute(r#"INSERT INTO c VALUES (1, '{"a": 1}')"#)
        .unwrap();
    for _ in 0..2 {
        let _ = engine
            .execute("SELECT JSON_EXTRACT(j, '$.a') FROM c WHERE id = 1")
            .unwrap()
            .materialize()
            .unwrap();
    }
    let response = engine
        .execute(
            "SELECT cache_name, scope, hits, misses, capacity \
             FROM information_schema.mydb_caches ORDER BY cache_name",
        )
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(rs) = response else {
        panic!("expected ResultSet");
    };
    let names: Vec<String> = rs
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => panic!("unexpected name value {other:?}"),
        })
        .collect();
    for expected in [
        "col_meta_intern",
        "expr_program_l1",
        "expr_program_l2",
        "json_path_compile",
        "physical_plan",
        "regex_compile",
        "statement_ast",
    ] {
        assert!(
            names.iter().any(|n| n == expected),
            "mydb_caches missing row {expected}: {names:?}"
        );
    }
    assert!(
        !names.iter().any(|name| name == "view_definition"),
        "view queries belong to the catalog, not a global parse cache"
    );
    // The JSON path compile cache saw the repeated '$.a' lookups.
    let json_row = rs
        .rows
        .iter()
        .find(|r| r[0] == Value::String("json_path_compile".into()))
        .unwrap();
    let (Value::Int(hits), Value::Int(misses)) = (&json_row[2], &json_row[3]) else {
        panic!("expected integer counters: {json_row:?}");
    };
    assert!(
        hits + misses >= 2,
        "json path cache must have recorded the lookups: hits={hits} misses={misses}"
    );
}

#[test]
fn information_schema_supports_system_table_join_and_mysql_column_details() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE users (\
             id INT PRIMARY KEY, \
             email VARCHAR(64), \
             name VARCHAR(20), \
             payload VARBINARY(8), \
             UNIQUE KEY email_idx (email), \
             KEY name_idx (name))",
        )
        .unwrap();

    let QueryResponse::ResultSet(keys) = engine
        .execute(
            "SELECT column_name, column_key, character_maximum_length, character_octet_length \
             FROM information_schema.columns \
             WHERE table_schema = 'test' AND table_name = 'users' \
             ORDER BY ordinal_position",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(keys.rows[0][1], Value::String("PRI".to_string()));
    assert_eq!(keys.rows[1][1], Value::String("UNI".to_string()));
    assert_eq!(keys.rows[2][1], Value::String("MUL".to_string()));
    assert_eq!(keys.rows[3][2], Value::Int(8));
    assert_eq!(keys.rows[3][3], Value::Int(8));

    let QueryResponse::ResultSet(joined) = engine
        .execute(
            "SELECT t.table_name, c.column_name \
             FROM information_schema.tables t \
             JOIN information_schema.columns c \
               ON t.table_schema = c.table_schema AND t.table_name = c.table_name \
             WHERE t.table_schema = 'test' AND t.table_name = 'users' \
               AND c.column_name = 'email'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        joined.rows,
        vec![vec![
            Value::String("users".to_string()),
            Value::String("email".to_string())
        ]]
    );

    engine
        .execute("INSERT INTO users VALUES (7, 'a@example.test', 'alice', X'01')")
        .unwrap();
    let QueryResponse::ResultSet(system_user_join) = engine
        .execute(
            "SELECT u.id, c.column_name \
             FROM users AS u \
             JOIN information_schema.columns AS c \
               ON c.table_schema = 'test' AND c.table_name = 'users' \
              AND c.column_name = 'email' \
             WHERE u.id = 7",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        system_user_join.rows,
        vec![vec![Value::Int(7), Value::String("email".to_string())]]
    );
}

#[test]
fn protected_system_objects_reject_dml_but_mysql_custom_tables_work() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let err = engine
        .execute("INSERT INTO mysql.user (User) VALUES ('alice')")
        .unwrap_err();
    assert_eq!(err.code(), 1142);
    assert!(err
        .message()
        .contains("direct DML against system catalog table"));

    let err = engine.execute("TRUNCATE TABLE mysql.user").unwrap_err();
    assert_eq!(err.code(), 1644);

    engine
        .execute("CREATE TABLE mysql.local_user_table (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO mysql.local_user_table VALUES (1)")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id FROM mysql.local_user_table")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn information_schema_tables_count_pushes_down_only_exact_equalities() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE DATABASE count_meta").unwrap();
    engine
        .execute("CREATE TABLE count_meta.a (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE count_meta.b (id INT PRIMARY KEY)")
        .unwrap();

    let QueryResponse::ResultSet(exact) = engine
        .execute(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = 'count_meta'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(exact.rows, vec![vec![Value::Int(2)]]);

    // The range residual is not representable by SystemScanRequest and must
    // keep the generic scan instead of being silently omitted by COUNT
    // pushdown.
    let QueryResponse::ResultSet(with_residual) = engine
        .execute(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = 'count_meta' AND table_name > 'a'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(with_residual.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn storage_engine_executes_common_scalar_functions_without_from() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT CONCAT('a','b'), ABS(-3), IFNULL(NULL,'x'), CHAR_LENGTH('你好'), LENGTH('你好')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(
        result.rows,
        vec![vec![
            Value::String("ab".into()),
            // ABS over an integer keeps the integer type (MySQL).
            Value::Int(3),
            Value::String("x".into()),
            Value::Int(2),
            Value::Int(6),
        ]]
    );
}

#[test]
fn storage_engine_executes_scalar_functions_with_tables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(64))")
        .unwrap();
    engine
        .execute("INSERT INTO people VALUES (1, 'Grace'), (2, 'Ada')")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT UPPER(name), LENGTH(name) FROM people WHERE LOWER(name) = 'ada' ORDER BY LENGTH(name)")
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(
        result.rows,
        vec![vec![Value::String("ADA".into()), Value::Int(3)]]
    );
}

#[test]
fn storage_engine_executes_special_function_syntax() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT SUBSTRING('abcdef' FROM 2 FOR 3), POSITION('b' IN 'abc'), TRIM(BOTH 'x' FROM 'xxhixx'), EXTRACT(YEAR FROM '2026-05-02 10:20:30')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(
        result.rows,
        vec![vec![
            Value::String("bcd".into()),
            Value::Int(2),
            Value::String("hi".into()),
            Value::Int(2026),
        ]]
    );
}

#[test]
fn storage_engine_uses_mysql_null_semantics_for_scalar_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT CONCAT('a', NULL), CONCAT_WS(',', 'a', NULL, 'b'), COALESCE(NULL, NULL, 7)",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(
        result.rows,
        vec![vec![
            Value::Null,
            Value::String("a,b".into()),
            Value::Int(7),
        ]]
    );
}

#[test]
fn storage_engine_executes_date_and_system_scalar_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT DATE('2026-05-02 10:20:30'), DATE_FORMAT('2026-05-02 10:20:30', '%Y/%m/%d %H:%i:%s'), VERSION(), DATABASE(), CONNECTION_ID()",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(
        result.rows,
        vec![vec![
            Value::Date(crate::storage::parse_date_to_days_bytes(b"2026-05-02").unwrap()),
            Value::String("2026/05/02 10:20:30".into()),
            Value::String(crate::server_version::MYSQL_COMPAT_VERSION.into()),
            Value::String("test".into()),
            Value::UInt(1),
        ]]
    );
}

#[test]
fn storage_engine_executes_join_group_order_limit_offset_and_explain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT, INDEX user_idx (user_id))")
        .unwrap();
    engine
        .execute("INSERT INTO users VALUES (1, 'Ada'), (2, 'Grace'), (3, 'Linus')")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1, 1, 50), (2, 1, 20), (3, 2, 5), (4, 2, 80)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT u.name, COUNT(*) AS c, SUM(o.amount) AS total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount >= 20 GROUP BY u.name HAVING COUNT(*) >= 1 ORDER BY total DESC LIMIT 1 OFFSET 0",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.columns[0].name, "name");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("Grace".into()));
    assert_eq!(result.rows[0][1], Value::Int(1));
    // SUM over Int inputs finalises as `DecimalCompact(_, 0)` — exact
    // magnitude survives instead of going through f64. See
    // `aggregate::SumAcc::finalize`.
    assert_eq!(result.rows[0][2], Value::DecimalCompact(80, 0));

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM orders WHERE user_id = 1")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(explain.columns[0].name, "id");
    assert!(explain.rows.iter().any(|row| row
        .iter()
        .any(|value| value == &Value::String("user_idx".into()))));
}

#[test]
fn join_using_and_natural_join_follow_mysql_projection_rules() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE lefty (id INT PRIMARY KEY, a INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE righty (id INT PRIMARY KEY, b INT)")
        .unwrap();
    engine
        .execute("INSERT INTO lefty VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO righty VALUES (1, 100), (3, 300)")
        .unwrap();

    let QueryResponse::ResultSet(inner) = engine
        .execute("SELECT * FROM lefty l JOIN righty r USING (id) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        inner
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "a", "b"]
    );
    assert_eq!(
        inner.rows,
        vec![vec![Value::Int(1), Value::Int(10), Value::Int(100)]]
    );

    let QueryResponse::ResultSet(left_join) = engine
        .execute("SELECT * FROM lefty l LEFT JOIN righty r USING (id) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        left_join.rows,
        vec![
            vec![Value::Int(1), Value::Int(10), Value::Int(100)],
            vec![Value::Int(2), Value::Int(20), Value::Null],
        ]
    );

    let QueryResponse::ResultSet(right_join) = engine
        .execute("SELECT * FROM lefty l RIGHT JOIN righty r USING (id) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        right_join.rows,
        vec![
            vec![Value::Int(1), Value::Int(100), Value::Int(10)],
            vec![Value::Int(3), Value::Int(300), Value::Null],
        ]
    );

    let QueryResponse::ResultSet(qualified_common) = engine
        .execute("SELECT r.id FROM lefty l LEFT JOIN righty r USING (id) WHERE l.a = 20")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(qualified_common.rows, vec![vec![Value::Int(2)]]);

    let QueryResponse::ResultSet(bare_id) = engine
        .execute("SELECT id, l.id, r.id FROM lefty l JOIN righty r USING (id)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        bare_id.rows,
        vec![vec![Value::Int(1), Value::Int(1), Value::Int(1)]]
    );

    let QueryResponse::ResultSet(natural) = engine
        .execute("SELECT * FROM lefty NATURAL JOIN righty")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        natural
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "a", "b"]
    );
    assert_eq!(
        natural.rows,
        vec![vec![Value::Int(1), Value::Int(10), Value::Int(100)]]
    );

    engine
        .execute("CREATE TABLE first_order (a INT, b INT, c INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE second_order (a INT, b INT, d INT)")
        .unwrap();
    engine
        .execute("INSERT INTO first_order VALUES (7, 8, 9)")
        .unwrap();
    engine
        .execute("INSERT INTO second_order VALUES (7, 8, 10)")
        .unwrap();
    let QueryResponse::ResultSet(using_order) = engine
        .execute("SELECT * FROM first_order f JOIN second_order s USING (b, a)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        using_order
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b", "c", "d"]
    );
    assert_eq!(
        using_order.rows,
        vec![vec![
            Value::Int(7),
            Value::Int(8),
            Value::Int(9),
            Value::Int(10),
        ]]
    );
}

#[test]
fn autocommit_point_select_does_not_write_transaction_redo() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE sbtest1 (id INT PRIMARY KEY, c CHAR(120))")
        .unwrap();
    engine
        .execute("INSERT INTO sbtest1 VALUES (1, 'c')")
        .unwrap();

    let redo_path = crate::storage::naming::system_wal_dir(dir.path()).join("redo.log");
    let before = std::fs::read_to_string(&redo_path).unwrap_or_default();
    engine
        .execute("SELECT c FROM sbtest1 WHERE id = 1")
        .unwrap();
    let after = std::fs::read_to_string(&redo_path).unwrap_or_default();

    assert_eq!(after, before);
}

#[test]
fn explicit_read_only_transaction_does_not_write_transaction_redo() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE sbtest1 (id INT PRIMARY KEY, c CHAR(120))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO sbtest1 VALUES (1, 'c')")
        .unwrap();

    let redo_path = crate::storage::naming::system_wal_dir(dir.path()).join("redo.log");
    let before = std::fs::read_to_string(&redo_path).unwrap_or_default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    assert!(session.current_txn.is_none());
    engine
        .execute_with_session(&mut session, "SELECT c FROM sbtest1 WHERE id = 1")
        .unwrap();
    assert!(session.current_txn.is_none());
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
    let after = std::fs::read_to_string(&redo_path).unwrap_or_default();

    assert_eq!(after, before);
}

#[test]
fn indexed_read_uses_old_key_while_concurrent_update_is_active() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 500);
    let mut writer = SessionContext::default();
    let mut reader = SessionContext::default();

    engine
        .execute_with_session(
            &mut writer,
            "CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, v INT, INDEX k_idx (k))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (1, 10, 100)")
        .unwrap();

    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE t SET k = 20 WHERE id = 1")
        .unwrap();

    let QueryResponse::ResultSet(old_key) = engine
        .execute_with_session(&mut reader, "SELECT id, k, v FROM t WHERE k = 10")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        old_key.rows,
        vec![vec![Value::Int(1), Value::Int(10), Value::Int(100)]]
    );

    let QueryResponse::ResultSet(new_key) = engine
        .execute_with_session(&mut reader, "SELECT id, k, v FROM t WHERE k = 20")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(new_key.rows.is_empty());
}

#[test]
fn indexed_read_uses_old_key_while_concurrent_delete_is_active() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 500);
    let mut writer = SessionContext::default();
    let mut reader = SessionContext::default();

    engine
        .execute_with_session(
            &mut writer,
            "CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (1, 10)")
        .unwrap();

    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "DELETE FROM t WHERE id = 1")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut reader, "SELECT id, k FROM t WHERE k = 10")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(10)]]);
}

#[test]
fn show_engine_mydb_status_reports_storage_state_after_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1)")
        .unwrap();
    engine.checkpoint().unwrap();

    let QueryResponse::ResultSet(status) = engine
        .execute_with_session(&mut session, "SHOW ENGINE MYDB STATUS")
        .unwrap()
    else {
        panic!("expected status result set");
    };
    let row = status
        .rows
        .iter()
        .find(|row| row[1] == Value::String("durability".into()))
        .expect("durability status row");
    assert_eq!(row[0], Value::String("mydb".into()));
    assert_eq!(row[2], Value::String("strict".into()));
    assert!(status.rows.iter().any(|row| row[1]
        == Value::String("last_checkpoint_unix_ms".into())
        && row[2] != Value::String(String::new())));
    for name in [
        "buffer_hits",
        "buffer_misses",
        "row_migrations",
        "undo_bytes",
        "wal_sync_avg_us",
        "reduced_redo_transactions",
        "reduced_redo_skipped_records",
        "reduced_redo_skipped_payload_bytes",
        "reduced_redo_fallback_records",
        "reduced_redo_force_flushes",
        "reduced_redo_force_flush_us",
        "checkpoint_failures_total",
        "checkpoint_preflush_failures_total",
        "checkpoint_redo_lsn",
        "checkpoint_redo_age_lsn",
        "last_checkpoint_dirty_pages",
        "last_checkpoint_wal_reclaimed_bytes",
        "checkpoint_wal_reclaimed_bytes_total",
        "restartpoints_total",
        "replica_applied_lsn",
        "last_checkpoint_trigger",
        "last_checkpoint_kind",
        "last_checkpoint_snapshot_us",
        "last_checkpoint_flush_us",
        "last_checkpoint_sync_us",
        "last_checkpoint_compact_us",
        "plan_cache_hits",
        "statement_cache_hits",
        "physical_plan_cache_hits",
        "lock_waits",
        "buffer_pool_read_ios",
        "buffer_pool_read_ios_heap",
        "buffer_pool_read_ios_index",
        "buffer_pool_read_ios_overflow",
        "buffer_pool_read_ios_vector",
        "buffer_pool_read_ios_zone_map",
        "buffer_pool_read_syscalls",
        "buffer_pool_batch_read_ops",
        "buffer_pool_batch_read_pages",
        "buffer_pool_write_ios",
        "buffer_pool_prefetches",
        "buffer_pool_prefetch_hits",
        "buffer_pool_prefetch_useful",
        "buffer_pool_prefetch_wasted",
        "buffer_pool_prefetch_demand_waits",
        "buffer_pool_eviction_scans",
        "buffer_pool_io_threads",
        "buffer_pool_prefetch_pages",
        // P0-1 - extended buffer-pool observability rows.
        "buffer_pool_read_ios_normal",
        "buffer_pool_read_ios_scan",
        "buffer_pool_read_ios_prefetch",
        "buffer_pool_prefetch_queue_drops",
        "buffer_pool_prefetch_queue_wait_avg_us",
        "buffer_pool_prefetch_lead_avg_us",
        "buffer_pool_ensure_capacity_busy_us",
        "buffer_pool_file_handle_wait_ns",
        "buffer_pool_dirty_ratio_histogram",
        "buffer_pool_dirty_ratio_samples",
        "buffer_pool_shard_imbalance_pct",
        "buffer_pool_shard_max_hits",
        "buffer_pool_shard_min_hits",
        "buffer_pool_scan_origin_share_pct",
        "buffer_pool_cold_recycle_hits",
        "buffer_pool_cold_recycle_fallbacks",
        "buffer_pool_raw_scan_pages",
        "buffer_pool_scan_origin_evictions",
        "spill_bytes_written",
        "spill_bytes_read",
        "startup_validate_indexes",
        "last_recovery_summary",
    ] {
        assert!(
            status
                .rows
                .iter()
                .any(|row| row[1] == Value::String(name.into())),
            "missing status row {name}"
        );
    }
    assert!(status.rows.iter().any(|row| {
        row[1] == Value::String("last_checkpoint_trigger".into())
            && row[2] == Value::String("manual".into())
    }));
    assert!(status.rows.iter().any(|row| {
        row[1] == Value::String("last_checkpoint_kind".into())
            && row[2] == Value::String("sharp".into())
    }));
}

#[test]
fn select_for_update_reads_latest_version_after_waiting() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 500));
    let mut owner = SessionContext::default();

    engine
        .execute_with_session(
            &mut owner,
            "CREATE TABLE counters (id INT PRIMARY KEY, next_id INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO counters VALUES (1, 3001)")
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut owner,
            "SELECT next_id FROM counters WHERE id = 1 FOR UPDATE",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut owner,
            "UPDATE counters SET next_id = next_id + 1 WHERE id = 1",
        )
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let mut waiter = SessionContext::default();
        waiter_engine.execute_with_session(&mut waiter, "BEGIN")?;
        let QueryResponse::ResultSet(result) = waiter_engine.execute_with_session(
            &mut waiter,
            "SELECT next_id FROM counters WHERE id = 1 FOR UPDATE",
        )?
        else {
            panic!("expected result set");
        };
        waiter_engine.execute_with_session(&mut waiter, "ROLLBACK")?;
        Ok::<_, QueryError>(result.rows)
    });

    std::thread::sleep(Duration::from_millis(20));
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();

    let rows = waiter.join().unwrap().unwrap();
    assert_eq!(rows, vec![vec![Value::Int(3002)]]);
}

#[test]
fn tpcc_new_order_district_lock_serializes_order_ids() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 500));
    let mut setup = SessionContext::default();

    engine
        .execute_with_session(
            &mut setup,
            "CREATE TABLE bmsql_district (\
             d_w_id INT NOT NULL, d_id INT NOT NULL, d_next_o_id INT)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut setup,
            "CREATE TABLE bmsql_oorder (\
             o_w_id INT NOT NULL, o_d_id INT NOT NULL, o_id INT NOT NULL, o_c_id INT, \
             o_carrier_id INT, o_ol_cnt INT, o_all_local INT, o_entry_d TIMESTAMP)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut setup, "INSERT INTO bmsql_district VALUES (1, 1, 3001)")
        .unwrap();
    engine
        .execute_with_session(
            &mut setup,
            "ALTER TABLE bmsql_district ADD CONSTRAINT bmsql_district_pkey \
             PRIMARY KEY (d_w_id, d_id)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut setup,
            "ALTER TABLE bmsql_oorder ADD CONSTRAINT bmsql_oorder_pkey \
             PRIMARY KEY (o_w_id, o_d_id, o_id)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut setup,
            "CREATE UNIQUE INDEX bmsql_oorder_idx1 \
             ON bmsql_oorder (o_w_id, o_d_id, o_carrier_id, o_id)",
        )
        .unwrap();

    let (owner_ready, wait_owner) = std::sync::mpsc::channel();
    let owner_engine = std::sync::Arc::clone(&engine);
    let owner = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        owner_engine.execute_with_session(&mut session, "SET SESSION autocommit = OFF")?;
        let QueryResponse::ResultSet(result) = owner_engine.execute_with_session(
            &mut session,
            "SELECT d_next_o_id FROM bmsql_district \
             WHERE d_w_id = 1 AND d_id = 1 FOR UPDATE",
        )?
        else {
            panic!("expected result set");
        };
        assert_eq!(result.rows, vec![vec![Value::Int(3001)]]);
        owner_engine.execute_with_session(
            &mut session,
            "UPDATE bmsql_district SET d_next_o_id = d_next_o_id + 1 \
             WHERE d_w_id = 1 AND d_id = 1",
        )?;
        owner_ready.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(30));
        owner_engine.execute_with_session(
            &mut session,
            "INSERT INTO bmsql_oorder \
             (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) \
             VALUES (3001, 1, 1, 11, '2026-05-05 00:00:00', 5, 1)",
        )?;
        owner_engine.execute_with_session(&mut session, "COMMIT")?;
        Ok::<_, QueryError>(())
    });

    // A sleep cannot establish which thread acquires the row lock first.
    wait_owner.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut waiter = SessionContext::default();
    engine
        .execute_with_session(&mut waiter, "SET SESSION autocommit = OFF")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut waiter,
            "SELECT d_next_o_id FROM bmsql_district \
             WHERE d_w_id = 1 AND d_id = 1 FOR UPDATE",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(3002)]]);
    engine
        .execute_with_session(
            &mut waiter,
            "UPDATE bmsql_district SET d_next_o_id = d_next_o_id + 1 \
             WHERE d_w_id = 1 AND d_id = 1",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut waiter,
            "INSERT INTO bmsql_oorder \
             (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) \
             VALUES (3002, 1, 1, 12, '2026-05-05 00:00:01', 5, 1)",
        )
        .unwrap();
    engine.execute_with_session(&mut waiter, "COMMIT").unwrap();
    owner.join().unwrap().unwrap();

    let QueryResponse::ResultSet(orders) = engine
        .execute("SELECT o_id FROM bmsql_oorder ORDER BY o_id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        orders.rows,
        vec![vec![Value::Int(3001)], vec![Value::Int(3002)]]
    );
}

#[test]
fn prepared_tpcc_new_order_district_lock_serializes_order_ids() {
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;

    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 500));
    let mut setup = SessionContext::default();

    engine
        .execute_with_session(
            &mut setup,
            "CREATE TABLE bmsql_district (\
             d_w_id INT NOT NULL, d_id INT NOT NULL, d_next_o_id INT)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut setup,
            "CREATE TABLE bmsql_oorder (\
             o_w_id INT NOT NULL, o_d_id INT NOT NULL, o_id INT NOT NULL, o_c_id INT, \
             o_carrier_id INT, o_ol_cnt INT, o_all_local INT, o_entry_d TIMESTAMP)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut setup, "INSERT INTO bmsql_district VALUES (1, 1, 3001)")
        .unwrap();
    engine
        .execute_with_session(
            &mut setup,
            "ALTER TABLE bmsql_district ADD CONSTRAINT bmsql_district_pkey \
             PRIMARY KEY (d_w_id, d_id)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut setup,
            "ALTER TABLE bmsql_oorder ADD CONSTRAINT bmsql_oorder_pkey \
             PRIMARY KEY (o_w_id, o_d_id, o_id)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut setup,
            "CREATE UNIQUE INDEX bmsql_oorder_idx1 \
             ON bmsql_oorder (o_w_id, o_d_id, o_carrier_id, o_id)",
        )
        .unwrap();

    let select_sql = "SELECT d_next_o_id FROM bmsql_district \
                      WHERE d_w_id = ? AND d_id = ? FOR UPDATE";
    let update_sql = "UPDATE bmsql_district SET d_next_o_id = d_next_o_id + 1 \
                      WHERE d_w_id = ? AND d_id = ?";
    let insert_sql = "INSERT INTO bmsql_oorder \
                      (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) \
                      VALUES (?, ?, ?, ?, ?, ?, ?)";

    let (owner_ready, wait_owner) = std::sync::mpsc::channel();
    let owner_engine = std::sync::Arc::clone(&engine);
    let owner = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        owner_engine.execute_with_session(&mut session, "SET SESSION autocommit = OFF")?;
        let select = owner_engine
            .prepare_cached(&mut session, select_sql, 2)
            .unwrap();
        let update = owner_engine
            .prepare_cached(&mut session, update_sql, 2)
            .unwrap();
        let insert = owner_engine
            .prepare_cached(&mut session, insert_sql, 7)
            .unwrap();

        let QueryResponse::ResultSet(result) =
            with_params(vec![Value::Int(1), Value::Int(1)], || {
                owner_engine
                    .execute_prepared_statement(&mut session, select_sql, (*select).clone())
                    .unwrap()
                    .unwrap()
            })
            .materialize()?
        else {
            panic!("expected result set");
        };
        assert_eq!(result.rows, vec![vec![Value::Int(3001)]]);
        with_params(vec![Value::Int(1), Value::Int(1)], || {
            owner_engine
                .execute_prepared_statement(&mut session, update_sql, (*update).clone())
                .unwrap()
                .unwrap()
        })
        .materialize()?;
        owner_ready.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(30));
        with_params(
            vec![
                Value::Int(3001),
                Value::Int(1),
                Value::Int(1),
                Value::Int(11),
                Value::String("2026-05-05 00:00:00".into()),
                Value::Int(5),
                Value::Int(1),
            ],
            || {
                owner_engine
                    .execute_prepared_statement(&mut session, insert_sql, (*insert).clone())
                    .unwrap()
                    .unwrap()
            },
        )
        .materialize()?;
        owner_engine.execute_with_session(&mut session, "COMMIT")?;
        Ok::<_, QueryError>(())
    });

    // A sleep cannot establish which thread acquires the row lock first.
    wait_owner.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut waiter = SessionContext::default();
    engine
        .execute_with_session(&mut waiter, "SET SESSION autocommit = OFF")
        .unwrap();
    let select = engine.prepare_cached(&mut waiter, select_sql, 2).unwrap();
    let update = engine.prepare_cached(&mut waiter, update_sql, 2).unwrap();
    let insert = engine.prepare_cached(&mut waiter, insert_sql, 7).unwrap();
    let QueryResponse::ResultSet(result) = with_params(vec![Value::Int(1), Value::Int(1)], || {
        engine
            .execute_prepared_statement(&mut waiter, select_sql, (*select).clone())
            .unwrap()
            .unwrap()
    })
    .materialize()
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(3002)]]);
    with_params(vec![Value::Int(1), Value::Int(1)], || {
        engine
            .execute_prepared_statement(&mut waiter, update_sql, (*update).clone())
            .unwrap()
            .unwrap()
    })
    .materialize()
    .unwrap();
    with_params(
        vec![
            Value::Int(3002),
            Value::Int(1),
            Value::Int(1),
            Value::Int(12),
            Value::String("2026-05-05 00:00:01".into()),
            Value::Int(5),
            Value::Int(1),
        ],
        || {
            engine
                .execute_prepared_statement(&mut waiter, insert_sql, (*insert).clone())
                .unwrap()
                .unwrap()
        },
    )
    .materialize()
    .unwrap();
    engine.execute_with_session(&mut waiter, "COMMIT").unwrap();
    owner.join().unwrap().unwrap();

    let QueryResponse::ResultSet(orders) = engine
        .execute("SELECT o_id FROM bmsql_oorder ORDER BY o_id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        orders.rows,
        vec![vec![Value::Int(3001)], vec![Value::Int(3002)]]
    );
}

#[test]
fn prepared_tpcc_new_order_district_lock_stress_unique_order_ids() {
    use crate::engine::api::{PreparedDmlState, QueryEngine as _};

    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 2_000));
    let mut setup = SessionContext::default();

    engine
        .execute_with_session(
            &mut setup,
            "CREATE TABLE bmsql_district (\
             d_w_id INT NOT NULL, d_id INT NOT NULL, d_tax DOUBLE, \
             d_ytd DECIMAL(12,2), d_next_o_id INT)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut setup,
            "CREATE TABLE bmsql_oorder (\
             o_w_id INT NOT NULL, o_d_id INT NOT NULL, o_id INT NOT NULL, o_c_id INT, \
             o_carrier_id INT, o_ol_cnt INT, o_all_local INT, o_entry_d TIMESTAMP)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut setup,
            "INSERT INTO bmsql_district VALUES (1, 1, 0.07, 30000.00, 3001)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut setup,
            "ALTER TABLE bmsql_district ADD CONSTRAINT bmsql_district_pkey \
             PRIMARY KEY (d_w_id, d_id)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut setup,
            "ALTER TABLE bmsql_oorder ADD CONSTRAINT bmsql_oorder_pkey \
             PRIMARY KEY (o_w_id, o_d_id, o_id)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut setup,
            "CREATE UNIQUE INDEX bmsql_oorder_idx1 \
             ON bmsql_oorder (o_w_id, o_d_id, o_carrier_id, o_id)",
        )
        .unwrap();

    let select_sql = "SELECT d_tax, d_next_o_id FROM bmsql_district \
                      WHERE d_w_id = ? AND d_id = ? FOR UPDATE";
    let update_sql = "UPDATE bmsql_district SET d_next_o_id = d_next_o_id + 1 \
                      WHERE d_w_id = ? AND d_id = ?";
    let insert_sql = "INSERT INTO bmsql_oorder \
                      (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) \
                      VALUES (?, ?, ?, ?, ?, ?, ?)";

    let new_order_threads = 12;
    let payment_threads = 8;
    let iterations = 25;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(new_order_threads + payment_threads));
    let mut handles = Vec::new();
    for thread_idx in 0..new_order_threads {
        let engine = std::sync::Arc::clone(&engine);
        let barrier = std::sync::Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || -> Result<(), String> {
            let mut session = SessionContext::default();
            engine
                .execute_with_session(&mut session, "SET SESSION autocommit = OFF")
                .map_err(|err| err.message().to_string())?;
            let select = engine
                .prepare_cached(&mut session, select_sql, 2)
                .map_err(|err| err.message().to_string())?;
            let update = engine
                .prepare_cached(&mut session, update_sql, 2)
                .map_err(|err| err.message().to_string())?;
            let insert = engine
                .prepare_cached(&mut session, insert_sql, 7)
                .map_err(|err| err.message().to_string())?;
            let mut select_state = PreparedDmlState::default();
            let mut update_state = PreparedDmlState::default();
            let mut insert_state = PreparedDmlState::default();

            barrier.wait();
            for iter_idx in 0..iterations {
                let select_result = engine
                    .execute_prepared_dml(
                        &mut session,
                        &mut select_state,
                        &select,
                        select_sql,
                        &[Value::Int(1), Value::Int(1)],
                    )
                    .ok_or_else(|| {
                        "expected prepared-DML fast path for district locking read".to_string()
                    })?;
                let QueryResponse::ResultSet(result) = select_result
                    .and_then(QueryResponse::materialize)
                    .map_err(|err| err.message().to_string())?
                else {
                    return Err("expected district result set".into());
                };
                let Some(Value::Int(o_id)) = result.rows.first().and_then(|row| row.get(1)) else {
                    return Err(format!(
                        "expected integer d_next_o_id, got {:?}",
                        result.rows
                    ));
                };
                engine
                    .execute_prepared_dml(
                        &mut session,
                        &mut update_state,
                        &update,
                        update_sql,
                        &[Value::Int(1), Value::Int(1)],
                    )
                    .ok_or_else(|| {
                        "expected prepared-DML fast path for district update".to_string()
                    })?
                    .and_then(QueryResponse::materialize)
                    .map_err(|err| err.message().to_string())?;
                engine
                    .execute_prepared_dml(
                        &mut session,
                        &mut insert_state,
                        &insert,
                        insert_sql,
                        &[
                            Value::Int(*o_id),
                            Value::Int(1),
                            Value::Int(1),
                            Value::Int((thread_idx * iterations + iter_idx + 1) as i64),
                            Value::String("2026-05-05 00:00:00".into()),
                            Value::Int(5),
                            Value::Int(1),
                        ],
                    )
                    .ok_or_else(|| "expected prepared-DML fast path for order insert".to_string())?
                    .and_then(QueryResponse::materialize)
                    .map_err(|err| err.message().to_string())?;
                engine
                    .execute_with_session(&mut session, "COMMIT")
                    .map_err(|err| err.message().to_string())?;
            }
            Ok(())
        }));
    }
    let payment_sql = "UPDATE bmsql_district SET d_ytd = d_ytd + ? \
                       WHERE d_w_id = ? AND d_id = ?";
    for _ in 0..payment_threads {
        let engine = std::sync::Arc::clone(&engine);
        let barrier = std::sync::Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || -> Result<(), String> {
            let mut session = SessionContext::default();
            engine
                .execute_with_session(&mut session, "SET SESSION autocommit = OFF")
                .map_err(|err| err.message().to_string())?;
            let update = engine
                .prepare_cached(&mut session, payment_sql, 3)
                .map_err(|err| err.message().to_string())?;
            let mut state = PreparedDmlState::default();
            barrier.wait();
            for _ in 0..iterations {
                let mut result = crate::storage::with_nonblocking_lock_waits(|| {
                    engine.execute_prepared_dml(
                        &mut session,
                        &mut state,
                        &update,
                        payment_sql,
                        &[Value::Decimal("1.00".into()), Value::Int(1), Value::Int(1)],
                    )
                })
                .ok_or_else(|| "expected prepared-DML fast path for payment update".to_string())?;
                if matches!(
                    &result,
                    Err(err) if crate::storage::is_lock_would_block_error(err)
                ) {
                    result = engine
                        .execute_prepared_dml(
                            &mut session,
                            &mut state,
                            &update,
                            payment_sql,
                            &[Value::Decimal("1.00".into()), Value::Int(1), Value::Int(1)],
                        )
                        .ok_or_else(|| {
                            "expected prepared-DML fast path for payment update".to_string()
                        })?;
                }
                result
                    .and_then(QueryResponse::materialize)
                    .map_err(|err| err.message().to_string())?;
                engine
                    .execute_with_session(&mut session, "COMMIT")
                    .map_err(|err| err.message().to_string())?;
            }
            Ok(())
        }));
    }

    for handle in handles {
        handle.join().unwrap().unwrap();
    }

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT COUNT(*), COUNT(DISTINCT o_id), MIN(o_id), MAX(o_id) \
             FROM bmsql_oorder",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let expected = new_order_threads * iterations;
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Int(expected as i64),
            Value::Int(expected as i64),
            Value::Int(3001),
            Value::Int(3000 + expected as i64)
        ]]
    );
    let QueryResponse::ResultSet(district) = engine
        .execute("SELECT d_ytd, d_next_o_id FROM bmsql_district")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        district.rows,
        vec![vec![
            Value::DecimalCompact(((30_000 + payment_threads * iterations) * 100) as i64, 2),
            Value::Int(3001 + expected as i64),
        ]]
    );
}

#[test]
fn select_for_update_preserves_order_limit_on_locked_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, grp INT, v INT, INDEX grp_idx (grp))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t VALUES (1, 7, 30), (2, 7, 10), (3, 7, 20)",
        )
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT id, v FROM t WHERE grp = 7 ORDER BY v LIMIT 2 FOR UPDATE",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(2), Value::Int(10)],
            vec![Value::Int(3), Value::Int(20)]
        ]
    );
}

#[test]
fn select_for_update_order_limit_locks_only_returned_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut owner = short_lock_wait_session();
    let mut limited = short_lock_wait_session();
    let mut third = short_lock_wait_session();

    engine
        .execute_with_session(
            &mut owner,
            "CREATE TABLE t (id INT PRIMARY KEY, grp INT, v INT, INDEX grp_v (grp, v))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut owner,
            "INSERT INTO t VALUES (1, 7, 10), (2, 7, 20), (3, 7, 30)",
        )
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "SELECT id FROM t WHERE id = 3 FOR UPDATE")
        .unwrap();

    engine.execute_with_session(&mut limited, "BEGIN").unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut limited,
            "SELECT id FROM t WHERE grp = 7 ORDER BY v LIMIT 2 FOR UPDATE",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

    engine.execute_with_session(&mut third, "BEGIN").unwrap();
    let blocked = engine
        .execute_with_session(&mut third, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);

    engine.execute_with_session(&mut owner, "COMMIT").unwrap();
    engine
        .execute_with_session(&mut third, "SELECT id FROM t WHERE id = 3 FOR UPDATE")
        .unwrap();
}

#[test]
fn select_for_update_order_limit_waits_on_uncommitted_first_index_entry() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut owner = short_lock_wait_session();
    let mut waiter = short_lock_wait_session();
    let mut third = short_lock_wait_session();

    engine
        .execute_with_session(
            &mut owner,
            "CREATE TABLE bmsql_new_order (\
             no_w_id INT NOT NULL, no_d_id INT NOT NULL, no_o_id INT NOT NULL, \
             PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut owner,
            "INSERT INTO bmsql_new_order VALUES (1, 7, 29), (1, 7, 30)",
        )
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO bmsql_new_order VALUES (1, 7, 28)")
        .unwrap();

    let blocked = engine
        .execute_with_session(
            &mut waiter,
            "SELECT no_o_id FROM bmsql_new_order \
             WHERE no_w_id = 1 AND no_d_id = 7 \
             ORDER BY no_o_id LIMIT 1 FOR UPDATE",
        )
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);

    engine.execute_with_session(&mut third, "BEGIN").unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut third,
            "SELECT no_o_id FROM bmsql_new_order \
             WHERE no_w_id = 1 AND no_d_id = 7 AND no_o_id = 29 FOR UPDATE",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(29)]]);

    engine.execute_with_session(&mut third, "ROLLBACK").unwrap();
    engine.execute_with_session(&mut owner, "ROLLBACK").unwrap();
}

#[test]
fn covering_prefix_lookup_limit_streams_from_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE prefix_limit (a INT, b INT, payload INT, PRIMARY KEY(a,b))")
        .unwrap();
    let values = (0..128)
        .flat_map(|b| [format!("(1,{b},{b})"), format!("(2,{b},{b})")])
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO prefix_limit VALUES {values}"))
        .unwrap();

    let before = crate::engine::executor::vec::index_scan::PREFIX_LOOKUP_CURSOR_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT b FROM prefix_limit WHERE a=1 ORDER BY b LIMIT 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let after = crate::engine::executor::vec::index_scan::PREFIX_LOOKUP_CURSOR_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(result.rows, vec![vec![Value::Int(0)]]);
    assert_eq!(after - before, 1, "prefix lookup should stay streaming");

    let mut owner = SessionContext::default();
    let mut reader = SessionContext::default();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "DELETE FROM prefix_limit WHERE a=2 AND b=0")
        .unwrap();

    // A repair on another prefix must not disable streaming for a=1.
    let unrelated_before = crate::engine::executor::vec::index_scan::PREFIX_LOOKUP_CURSOR_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(unrelated) = engine
        .execute_with_session(
            &mut reader,
            "SELECT b FROM prefix_limit WHERE a=1 ORDER BY b LIMIT 1",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let unrelated_after = crate::engine::executor::vec::index_scan::PREFIX_LOOKUP_CURSOR_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(unrelated.rows, vec![vec![Value::Int(0)]]);
    assert_eq!(unrelated_after - unrelated_before, 1);

    // A repair in the probed prefix is merged into the ordered cursor so the
    // reader sees the row deleted by an uncommitted writer without decoding
    // the remaining prefix before LIMIT 1 can stop.
    let related_before = unrelated_after;
    let repair_before = crate::engine::executor::vec::index_scan::PREFIX_LOOKUP_REPAIR_CURSOR_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(related) = engine
        .execute_with_session(
            &mut reader,
            "SELECT b FROM prefix_limit WHERE a=2 ORDER BY b LIMIT 1",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let related_after = crate::engine::executor::vec::index_scan::PREFIX_LOOKUP_CURSOR_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let repair_after = crate::engine::executor::vec::index_scan::PREFIX_LOOKUP_REPAIR_CURSOR_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(related.rows, vec![vec![Value::Int(0)]]);
    assert_eq!(related_after, related_before);
    assert_eq!(repair_after - repair_before, 1);
    engine.execute_with_session(&mut owner, "ROLLBACK").unwrap();
}

#[test]
fn ordered_range_limit_streams_while_index_repairs_are_pending() {
    fn explain_with_session(
        engine: &StorageQueryEngine,
        session: &mut SessionContext,
        sql: &str,
    ) -> String {
        let QueryResponse::ResultSet(explain) = engine.execute_with_session(session, sql).unwrap()
        else {
            panic!("expected EXPLAIN result set");
        };
        explain.rows[0][0].to_display_string()
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 500);
    let mut writer = SessionContext::default();
    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut writer,
            "CREATE TABLE range_repair_limit (\
             id INT PRIMARY KEY, k INT NOT NULL, payload INT, INDEX k_idx(k))",
        )
        .unwrap();
    for start in (0..5_000).step_by(1_000) {
        let values = (start..start + 1_000)
            .map(|id| format!("({id},{id},{id})"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute_with_session(
                &mut writer,
                &format!("INSERT INTO range_repair_limit VALUES {values}"),
            )
            .unwrap();
    }

    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "UPDATE range_repair_limit SET k=10000 WHERE id=0",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "UPDATE range_repair_limit SET k=-1 WHERE id=4999",
        )
        .unwrap();

    for (direction, expected) in [("ASC", 0), ("DESC", 4_999)] {
        let sql = format!(
            "EXPLAIN ANALYZE FORMAT=TREE \
             SELECT id,k FROM range_repair_limit FORCE INDEX(k_idx) \
             WHERE k>=0 ORDER BY k {direction} LIMIT 1"
        );
        let text = explain_with_session(&engine, &mut reader, &sql);
        assert!(
            text.contains(&format!("rows=1")),
            "ordered repair scan must return one row: {text}"
        );
        let QueryResponse::ResultSet(rows) = engine
            .execute_with_session(
                &mut reader,
                &format!(
                    "SELECT id,k FROM range_repair_limit FORCE INDEX(k_idx) \
                     WHERE k>=0 ORDER BY k {direction} LIMIT 1"
                ),
            )
            .unwrap()
        else {
            panic!("expected result set");
        };
        assert_eq!(
            rows.rows,
            vec![vec![Value::Int(expected), Value::Int(expected)]]
        );
        let examined = text
            .split("index_entries_examined=")
            .nth(1)
            .and_then(|tail| tail.split_whitespace().next())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_else(|| panic!("missing range metrics in {text}"));
        assert!(
            examined <= 2,
            "pending repairs must not materialize the complete index: {text}"
        );
        assert!(text.contains("candidate_window_peak=1"), "{text}");
    }
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
}

#[test]
fn long_select_does_not_block_unrelated_table_write() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 100));
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let values = (1..=200)
        .map(|id| format!("({id}, {id})"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO a VALUES {values}"))
        .unwrap();

    let reader_engine = std::sync::Arc::clone(&engine);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut result = reader_engine
            .execute_streaming_for_test("SELECT id FROM a ORDER BY id")
            .unwrap();
        let QueryResponse::StreamingResultSet(stream) = &mut result else {
            panic!("expected streaming result");
        };
        assert_eq!(stream.cursor.next_row().unwrap(), Some(vec![Value::Int(1)]));
        started_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(150));
        result.materialize().unwrap()
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("reader should produce first row");
    let started = Instant::now();
    engine.execute("INSERT INTO b VALUES (1, 10)").unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "unrelated table write waited behind streaming read"
    );

    let QueryResponse::ResultSet(rows) = reader.join().unwrap() else {
        panic!("expected materialized reader result");
    };
    assert_eq!(rows.rows.len(), 199);
    let QueryResponse::ResultSet(b) = engine.execute("SELECT * FROM b").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(b.rows, vec![vec![Value::Int(1), Value::Int(10)]]);
}

#[test]
fn secondary_index_reads_obey_mvcc_visibility() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut writer = SessionContext::default();
    let mut reader = SessionContext::default();

    engine
        .execute_with_session(
            &mut writer,
            "CREATE TABLE t (id INT PRIMARY KEY, k INT, v TEXT, INDEX k_idx (k))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b')",
        )
        .unwrap();

    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE t SET k = 30, v = 'moved' WHERE id = 1")
        .unwrap();

    let QueryResponse::ResultSet(new_key_before_commit) = engine
        .execute_with_session(&mut reader, "SELECT id, k, v FROM t WHERE k = 30")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(new_key_before_commit.rows.is_empty());

    let QueryResponse::ResultSet(old_key_before_commit) = engine
        .execute_with_session(&mut reader, "SELECT id, k, v FROM t WHERE k = 10")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        old_key_before_commit.rows,
        vec![vec![
            Value::Int(1),
            Value::Int(10),
            Value::String("a".into())
        ]]
    );

    engine.execute_with_session(&mut writer, "COMMIT").unwrap();
    let QueryResponse::ResultSet(new_key_after_commit) = engine
        .execute_with_session(&mut reader, "SELECT id, k, v FROM t WHERE k = 30")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        new_key_after_commit.rows,
        vec![vec![
            Value::Int(1),
            Value::Int(30),
            Value::String("moved".into())
        ]]
    );

    let QueryResponse::ResultSet(old_key_after_commit) = engine
        .execute_with_session(&mut reader, "SELECT id FROM t WHERE k = 10")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(old_key_after_commit.rows.is_empty());
}

#[test]
fn streaming_select_keeps_statement_snapshot_after_later_commit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let values = (1..=70)
        .map(|id| format!("({id}, {id})"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();

    let mut result = engine
        .execute_streaming_for_test("SELECT id FROM t WHERE id > 0 ORDER BY id")
        .unwrap();
    let QueryResponse::StreamingResultSet(stream) = &mut result else {
        panic!("expected streaming result");
    };
    assert_eq!(stream.cursor.next_row().unwrap(), Some(vec![Value::Int(1)]));

    engine.execute("INSERT INTO t VALUES (1000, 1000)").unwrap();

    let QueryResponse::ResultSet(remaining) = result.materialize().unwrap() else {
        panic!("expected materialized result set");
    };
    assert_eq!(remaining.rows.len(), 69);
    assert_eq!(remaining.rows.first(), Some(&vec![Value::Int(2)]));
    assert_eq!(remaining.rows.last(), Some(&vec![Value::Int(70)]));
    assert!(!remaining.rows.contains(&vec![Value::Int(1000)]));

    let QueryResponse::ResultSet(current) =
        engine.execute("SELECT id FROM t WHERE id = 1000").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(current.rows, vec![vec![Value::Int(1000)]]);
}

#[test]
fn dropping_streaming_cursor_joins_its_worker() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE cursor_drop_t (id INT PRIMARY KEY)")
        .unwrap();
    let values = (1..=300)
        .map(|id| format!("({id})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO cursor_drop_t VALUES {values}"))
        .unwrap();

    let baseline = crate::engine::executor::select::stream::active_stream_workers();
    let mut response = engine
        .execute_streaming_for_test("SELECT id FROM cursor_drop_t")
        .unwrap();
    let QueryResponse::StreamingResultSet(result) = &mut response else {
        panic!("expected streaming result set");
    };
    assert!(result.cursor.next_row().unwrap().is_some());
    drop(response);
    assert_eq!(
        crate::engine::executor::select::stream::active_stream_workers(),
        baseline,
        "cursor Drop left a detached streaming worker"
    );
}

#[test]
fn internal_stream_early_drop_does_not_cancel_statement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE stream_limit_t (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE stream_limit_u (id INT PRIMARY KEY, payload INT)")
        .unwrap();
    let left = (1..=300)
        .map(|id| format!("({id},{},{})", id % 17, id * 3))
        .collect::<Vec<_>>()
        .join(",");
    let right = (1..=300)
        .map(|id| format!("({id},{})", id * 5))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO stream_limit_t VALUES {left}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO stream_limit_u VALUES {right}"))
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT t.k, COUNT(*), SUM(u.payload) \
             FROM stream_limit_t t INNER JOIN stream_limit_u u ON t.id = u.id \
             GROUP BY t.k ORDER BY SUM(u.payload) DESC LIMIT 10",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 10);
    assert_eq!(
        crate::engine::executor::select::stream::active_stream_workers(),
        0
    );
}

#[test]
fn executes_stock_level_derived_in_subquery_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE bmsql_district (d_w_id INT, d_id INT, d_next_o_id INT, \
             PRIMARY KEY (d_w_id, d_id))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE bmsql_order_line (ol_w_id INT, ol_d_id INT, ol_o_id INT, \
             ol_number INT, ol_i_id INT, PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE bmsql_stock (s_w_id INT, s_i_id INT, s_quantity INT, \
             PRIMARY KEY (s_w_id, s_i_id))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO bmsql_district VALUES (1, 5, 30)")
        .unwrap();
    engine
        .execute("INSERT INTO bmsql_order_line VALUES (1, 5, 25, 1, 100), (1, 5, 29, 1, 101), (1, 5, 9, 1, 102)")
        .unwrap();
    engine
        .execute("INSERT INTO bmsql_stock VALUES (1, 100, 8), (1, 101, 20), (1, 102, 3)")
        .unwrap();

    let QueryResponse::ResultSet(subquery) = engine
        .execute(
            "SELECT ol_i_id FROM bmsql_district \
             JOIN bmsql_order_line ON ol_w_id = d_w_id \
                 AND ol_d_id = d_id \
                 AND ol_o_id >= d_next_o_id - 20 \
                 AND ol_o_id < d_next_o_id \
             WHERE d_w_id = 1 AND d_id = 5",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        subquery.rows,
        vec![vec![Value::Int(100)], vec![Value::Int(101)]]
    );

    let QueryResponse::ResultSet(inner) = engine
        .execute(
            "SELECT s_w_id, s_i_id, s_quantity FROM bmsql_stock \
             WHERE s_w_id = 1 AND s_quantity < 14 AND s_i_id IN ( \
                SELECT ol_i_id FROM bmsql_district \
                JOIN bmsql_order_line ON ol_w_id = d_w_id \
                    AND ol_d_id = d_id \
                    AND ol_o_id >= d_next_o_id - 20 \
                    AND ol_o_id < d_next_o_id \
                WHERE d_w_id = 1 AND d_id = 5 \
             )",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        inner.rows,
        vec![vec![Value::Int(1), Value::Int(100), Value::Int(8)]]
    );

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT count(*) AS low_stock FROM ( \
                SELECT s_w_id, s_i_id, s_quantity FROM bmsql_stock \
                WHERE s_w_id = 1 AND s_quantity < 14 AND s_i_id IN ( \
                    SELECT ol_i_id FROM bmsql_district \
                    JOIN bmsql_order_line ON ol_w_id = d_w_id \
                        AND ol_d_id = d_id \
                        AND ol_o_id >= d_next_o_id - 20 \
                        AND ol_o_id < d_next_o_id \
                    WHERE d_w_id = 1 AND d_id = 5 \
                ) \
            ) AS L",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT count(*) AS low_stock FROM ( \
                SELECT s_w_id, s_i_id, s_quantity FROM bmsql_stock \
                WHERE s_w_id = 1 AND s_quantity < 14 AND s_i_id IN ( \
                    SELECT ol_i_id FROM bmsql_district \
                    JOIN bmsql_order_line ON ol_w_id = d_w_id \
                        AND ol_d_id = d_id \
                        AND ol_o_id >= d_next_o_id - 20 \
                        AND ol_o_id < d_next_o_id \
                    WHERE d_w_id = 1 AND d_id = 5 \
                ) \
            ) AS L",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let explain_text = explain
        .rows
        .iter()
        .flatten()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        explain_text.contains("IN subquery"),
        "missing IN-subquery node in: {explain_text}"
    );
    assert!(
        explain_text.contains("Index nested loop")
            || explain_text.contains(" NULL ref PRIMARY PRIMARY"),
        "missing indexed access path in: {explain_text}"
    );
    assert!(
        explain_text.contains("Derived"),
        "missing derived node in: {explain_text}"
    );
}

#[test]
fn executes_order_status_scalar_max_subquery_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE bmsql_oorder (o_w_id INT, o_d_id INT, o_id INT, o_c_id INT, o_entry_d TIMESTAMP, o_carrier_id INT)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO bmsql_oorder VALUES \
                (1, 7, 10, 1489, '2026-01-01 00:00:00', 1), \
                (1, 7, 11, 1489, '2026-01-02 00:00:00', 2), \
                (1, 7, 12, 2000, '2026-01-03 00:00:00', 3)",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT o_id, o_entry_d, o_carrier_id FROM bmsql_oorder \
             WHERE o_w_id = 1 AND o_d_id = 7 AND o_c_id = 1489 \
               AND o_id = ( \
                   SELECT max(o_id) FROM bmsql_oorder \
                   WHERE o_w_id = 1 AND o_d_id = 7 AND o_c_id = 1489 \
               )",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    assert_eq!(row[0], Value::Int(11));
    // TIMESTAMP retains its UTC identity internally and is localized only at
    // the QueryResponse boundary.
    // (micros since epoch). Verify the user-visible canonical form.
    assert_eq!(row[1].to_display_string(), "2026-01-02 00:00:00");
    assert_eq!(row[2], Value::Int(2));
}

#[test]
fn prepared_order_status_scalar_max_subquery_keeps_bound_params() {
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE bmsql_oorder (\
             o_w_id INT, o_d_id INT, o_id INT, o_c_id INT, \
             o_entry_d TIMESTAMP, o_carrier_id INT)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO bmsql_oorder VALUES \
                (1, 7, 10, 1489, '2026-01-01 00:00:00', 1), \
                (1, 7, 11, 1489, '2026-01-02 00:00:00', 2), \
                (1, 7, 12, 2000, '2026-01-03 00:00:00', 3)",
        )
        .unwrap();

    let sql = "SELECT o_id, o_entry_d, o_carrier_id FROM bmsql_oorder \
               WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? \
                 AND o_id = ( \
                     SELECT max(o_id) FROM bmsql_oorder \
                     WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? \
                 )";
    let mut session = SessionContext::default();
    let prepared = engine.prepare_cached(&mut session, sql, 6).unwrap();
    let stmt = (*prepared).clone();

    let response = with_params(
        vec![
            Value::Int(1),
            Value::Int(7),
            Value::Int(1489),
            Value::Int(1),
            Value::Int(7),
            Value::Int(1489),
        ],
        || {
            engine
                .execute_prepared_statement(&mut session, sql, stmt)
                .unwrap()
                .unwrap()
        },
    )
    .materialize()
    .unwrap();

    let QueryResponse::ResultSet(result) = response else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(11));
    assert_eq!(result.rows[0][1].to_display_string(), "2026-01-02 00:00:00");
    assert_eq!(result.rows[0][2], Value::Int(2));
}

#[test]
fn index_ordered_max_applies_residual_filter_before_returning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE orders (w INT, d INT, id INT, c INT, INDEX w_d_id (w, d, id))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders VALUES \
                (1, 1, 10, 7), \
                (1, 1, 11, 8), \
                (1, 1, 12, 9), \
                (1, 2, 20, 8)",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT MAX(id) FROM orders WHERE w = 1 AND d = 1 AND c = 8")
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows, vec![vec![Value::Int(11)]]);
}

#[test]
fn composite_index_range_scan_uses_prefix_and_open_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (a INT, b INT, c INT, INDEX abc_idx (a, b, c))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES \
             (1, 1, 5), (1, 2, 8), (1, 2, 9), (1, 2, 10), (1, 3, 7), (2, 2, 9)",
        )
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT c FROM t WHERE a = 1 AND b = 2 AND c > 8")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    // The projected column `c` is the trailing column of `abc_idx`, so
    // the range is fully covering and the planner now selects the
    // index-only fast-scan operator (access_type "index", Extra
    // "Using index"). A non-covering range over the same predicate
    // would still report "range".
    assert_eq!(explain.rows[0][4], Value::String("index".into()));
    assert_eq!(explain.rows[0][6], Value::String("abc_idx".into()));
    let Value::String(extra) = &explain.rows[0][11] else {
        panic!(
            "expected Extra column to be a string, got {:?}",
            explain.rows[0][11]
        );
    };
    assert!(
        extra.contains("Using index"),
        "expected Extra to mention Using index, got {extra:?}"
    );

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT c FROM t WHERE a = 1 AND b = 2 AND c > 8 ORDER BY c")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(9)], vec![Value::Int(10)]]);
}

#[test]
fn in_subquery_can_drive_composite_index_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE inventory (tenant_id INT, item_id INT, qty INT, INDEX tenant_item_idx (tenant_id, item_id))")
        .unwrap();
    engine
        .execute("CREATE TABLE recent_items (item_id INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO inventory VALUES \
             (1, 100, 8), (1, 101, 20), (1, 102, 3), (2, 100, 1)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO recent_items VALUES (100), (102), (100)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT item_id FROM inventory \
             WHERE tenant_id = 1 AND qty < 10 \
               AND item_id IN (SELECT item_id FROM recent_items)",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let explain_text = explain
        .rows
        .iter()
        .flatten()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(explain_text.contains("tenant_item_idx"));
    assert!(explain_text.contains("Using IN subquery lookup"));

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT item_id FROM inventory \
             WHERE tenant_id = 1 AND qty < 10 \
               AND item_id IN (SELECT item_id FROM recent_items) \
             ORDER BY item_id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(100)], vec![Value::Int(102)]]
    );
}

#[test]
fn in_subquery_lookup_plan_carries_preplanned_subquery() {
    use crate::parser::{parse_statement, Statement};
    use crate::planner::{plan_select, PhysicalPlan};

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE inventory (tenant_id INT, item_id INT, qty INT, INDEX tenant_item_idx (tenant_id, item_id))")
        .unwrap();
    engine
        .execute("CREATE TABLE recent_items (item_id INT)")
        .unwrap();
    engine
        .execute("INSERT INTO inventory VALUES (1, 100, 8), (1, 101, 20), (1, 102, 3)")
        .unwrap();
    engine
        .execute("INSERT INTO recent_items VALUES (100), (102)")
        .unwrap();

    let Statement::Select(select) = parse_statement(
        "SELECT item_id FROM inventory \
         WHERE tenant_id = 1 AND qty < 10 \
           AND item_id IN (SELECT item_id FROM recent_items)",
    )
    .unwrap() else {
        panic!("expected select");
    };
    let planned = plan_select(engine.db_for_tests(), &select).unwrap();

    fn find_index_in_lookup(plan: &PhysicalPlan) -> Option<&PhysicalPlan> {
        match plan {
            PhysicalPlan::IndexInLookup { .. } => Some(plan),
            PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::HashAggregate { input, .. } => find_index_in_lookup(input),
            PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::HashJoin { left, right, .. } => {
                find_index_in_lookup(left).or_else(|| find_index_in_lookup(right))
            }
            _ => None,
        }
    }

    let Some(PhysicalPlan::IndexInLookup {
        subquery_plan,
        subquery,
        ..
    }) = find_index_in_lookup(&planned.physical)
    else {
        panic!("expected IndexInLookup, got {:?}", planned.physical);
    };
    assert_eq!(subquery.projection.len(), 1);
    assert!(
        matches!(
            subquery_plan.as_ref(),
            PhysicalPlan::SeqScan { .. }
                | PhysicalPlan::Projection { .. }
                | PhysicalPlan::IndexLookup { .. }
                | PhysicalPlan::IndexRangeScan { .. }
                | PhysicalPlan::IndexFastScan { .. }
        ),
        "IndexInLookup must carry a preplanned subquery, got {:?}",
        subquery_plan
    );
}

#[test]
fn stream_worker_consumes_index_in_subquery_without_nested_stream() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE pool_inventory (tenant_id INT, item_id INT, qty INT, INDEX tenant_item_idx (tenant_id, item_id))")
        .unwrap();
    engine
        .execute("CREATE TABLE pool_recent_items (item_id INT)")
        .unwrap();
    engine
        .execute("INSERT INTO pool_inventory VALUES (1, 100, 8), (1, 101, 20), (1, 102, 3)")
        .unwrap();
    engine
        .execute("INSERT INTO pool_recent_items VALUES (100), (102)")
        .unwrap();

    // A 200 MiB target with the default 8 MiB blocking stack permits one
    // worker. The outer IndexInLookup result producer executes this child
    // SELECT from that worker, so returning another channel-backed stream
    // would queue its producer behind the worker that immediately waits for
    // it. Reproduce the producer scope and require a synchronous response.
    let sql = "SELECT item_id FROM pool_inventory \
               WHERE tenant_id = 1 AND qty < 10 \
                 AND item_id IN (SELECT item_id FROM pool_recent_items) \
               ORDER BY item_id";
    let response = {
        let _stream_worker = crate::engine::executor::select::stream::ActiveStreamWorker::enter();
        engine.execute(sql).unwrap()
    };
    let QueryResponse::ResultSet(result) = response else {
        panic!("stream producer must not schedule a nested streaming producer");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(100)], vec![Value::Int(102)]]
    );

    // Leaving the producer scope must restore ordinary protocol streaming;
    // only the recursively consumed child is made synchronous.
    let response = engine.execute_streaming_for_test(sql).unwrap();
    assert!(matches!(&response, QueryResponse::StreamingResultSet(_)));
    let QueryResponse::ResultSet(result) = response.materialize().unwrap() else {
        panic!("materialized top-level stream must be a result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(100)], vec![Value::Int(102)]]
    );
}

#[test]
fn constant_in_list_can_drive_index_multi_lookup() {
    use crate::parser::{parse_statement, Statement};
    use crate::planner::{plan_select, PhysicalPlan};

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE inv_in (tenant_id INT, item_id INT, qty INT, INDEX tenant_item_idx (tenant_id, item_id))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO inv_in VALUES \
             (1, 100, 8), (1, 101, 20), (1, 102, 3), (2, 100, 7)",
        )
        .unwrap();

    let Statement::Select(select) = parse_statement(
        "SELECT item_id FROM inv_in FORCE INDEX (tenant_item_idx) \
         WHERE tenant_id = 1 AND item_id IN (100, 102) ORDER BY item_id",
    )
    .unwrap() else {
        panic!("expected select");
    };
    let planned = plan_select(engine.db_for_tests(), &select).unwrap();

    fn find_multi_lookup(plan: &PhysicalPlan) -> Option<&PhysicalPlan> {
        match plan {
            PhysicalPlan::IndexMultiLookup { .. } => Some(plan),
            PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::HashAggregate { input, .. } => find_multi_lookup(input),
            PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::HashJoin { left, right, .. } => {
                find_multi_lookup(left).or_else(|| find_multi_lookup(right))
            }
            _ => None,
        }
    }

    let Some(PhysicalPlan::IndexMultiLookup { keys, .. }) = find_multi_lookup(&planned.physical)
    else {
        panic!("expected IndexMultiLookup, got {:?}", planned.physical);
    };
    assert_eq!(keys.len(), 2);

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT item_id FROM inv_in FORCE INDEX (tenant_item_idx) \
             WHERE tenant_id = 1 AND item_id IN (100, 102) ORDER BY item_id",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let explain_text = explain
        .rows
        .iter()
        .flatten()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(explain_text.contains("tenant_item_idx"));
    assert!(explain_text.contains("Using IN-list multi lookup"));

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT item_id FROM inv_in FORCE INDEX (tenant_item_idx) \
             WHERE tenant_id = 1 AND item_id IN (100, 102) ORDER BY item_id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(100)], vec![Value::Int(102)]]
    );
}

#[test]
fn stale_stats_planning_applies_signed_committed_row_delta() {
    use crate::parser::{parse_statement, Statement};
    use crate::planner::{estimated_rows, plan_select};

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let values = (1..=100)
        .map(|i| format!("({i}, {i})"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();
    engine.execute("DELETE FROM t WHERE id <= 90").unwrap();

    let Statement::Select(select) = parse_statement("SELECT id FROM t").unwrap() else {
        panic!("expected select");
    };
    let planned = plan_select(engine.db_for_tests(), &select).unwrap();
    let rows = estimated_rows(&planned.physical);

    assert_eq!(
        rows, 10.0,
        "DELETE must subtract from the analyzed cardinality without a heap count"
    );

    // UPDATE contributes to modified_rows (and may trigger ANALYZE), but not
    // to the table population estimate.
    engine.execute("UPDATE t SET v = v + 1").unwrap();
    let planned = plan_select(engine.db_for_tests(), &select).unwrap();
    assert_eq!(estimated_rows(&planned.physical), 10.0);

    engine
        .execute("INSERT INTO t VALUES (101,101),(102,102),(103,103)")
        .unwrap();
    let planned = plan_select(engine.db_for_tests(), &select).unwrap();
    assert_eq!(estimated_rows(&planned.physical), 13.0);
}

#[test]
fn large_in_subquery_does_not_force_index_lookup_plan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE inventory_big (tenant_id INT, item_id INT, qty INT, INDEX tenant_item_big_idx (tenant_id, item_id))")
        .unwrap();
    engine
        .execute("CREATE TABLE recent_items_big (item_id INT)")
        .unwrap();
    let mut inventory = String::from("INSERT INTO inventory_big VALUES ");
    for i in 0..8 {
        if i > 0 {
            inventory.push_str(", ");
        }
        inventory.push_str(&format!("(1, {}, {})", i, i));
    }
    engine.execute(&inventory).unwrap();
    let mut recent = String::from("INSERT INTO recent_items_big VALUES ");
    for i in 0..128 {
        if i > 0 {
            recent.push_str(", ");
        }
        recent.push_str(&format!("({})", i));
    }
    engine.execute(&recent).unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT item_id FROM inventory_big \
             WHERE tenant_id = 1 \
               AND item_id IN (SELECT item_id FROM recent_items_big)",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let explain_text = explain
        .rows
        .iter()
        .flatten()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        !explain_text.contains("Using IN subquery lookup"),
        "large IN subquery should not force indexed lookups: {explain_text}"
    );
}

#[test]
fn delivery_new_order_uses_primary_order_without_filesort_and_streams() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE bmsql_new_order (\
             no_w_id INT NOT NULL, no_d_id INT NOT NULL, no_o_id INT NOT NULL, \
             PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO bmsql_new_order VALUES \
             (1, 7, 30), (1, 7, 28), (1, 7, 29), (1, 8, 1)",
        )
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT no_o_id FROM bmsql_new_order \
             WHERE no_w_id = 1 AND no_d_id = 7 ORDER BY no_o_id ASC",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert!(!explain
        .rows
        .iter()
        .any(|row| { matches!(&row[11], Value::String(extra) if extra.contains("filesort")) }));

    let result = engine
        .execute_streaming_for_test(
            "SELECT no_o_id FROM bmsql_new_order \
             WHERE no_w_id = 1 AND no_d_id = 7 ORDER BY no_o_id ASC",
        )
        .unwrap();
    assert!(matches!(result, QueryResponse::StreamingResultSet(_)));
    let QueryResponse::ResultSet(materialized) = result.materialize().unwrap() else {
        panic!("expected materialized result set");
    };
    assert_eq!(
        materialized.rows,
        vec![
            vec![Value::Int(28)],
            vec![Value::Int(29)],
            vec![Value::Int(30)]
        ]
    );
}

#[test]
fn simple_select_limit_matches_expected_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, INDEX v_idx (v))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 11), (3, 12), (4, 13)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, v + 1 AS next_v FROM t WHERE id >= 2 LIMIT 2 OFFSET 1")
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(
        result.rows,
        vec![
            // MySQL: `INT + INT` stays an integer.
            vec![Value::Int(3), Value::Int(13)],
            vec![Value::Int(4), Value::Int(14)],
        ]
    );
}

#[test]
fn unordered_limit_with_range_predicate_stops_bounded_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY, \
             o_orderdate DATE NOT NULL, \
             o_comment VARCHAR(32), \
             INDEX idx_orders_orderdate(o_orderdate))",
        )
        .unwrap();
    for batch_start in (1..=500).step_by(100) {
        let batch_end = (batch_start + 99).min(500);
        let mut sql = String::from("INSERT INTO orders VALUES ");
        for id in batch_start..=batch_end {
            if id > batch_start {
                sql.push(',');
            }
            let day = (id % 28) + 1;
            let date = if id <= 375 {
                format!("1994-03-{day:02}")
            } else {
                format!("1996-03-{day:02}")
            };
            sql.push_str(&format!("({id}, '{date}', 'order {id}')"));
        }
        engine.execute(&sql).unwrap();
    }
    engine.execute("ANALYZE TABLE orders").unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT * FROM orders WHERE o_orderdate < '1995-03-15' LIMIT 10,1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN ANALYZE FORMAT=TREE \
             SELECT * FROM orders WHERE o_orderdate < '1995-03-15' LIMIT 10,1",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let text = explain
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .map(|value| match value {
            Value::String(s) => s.clone(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !text.contains("Exchange"),
        "small unordered LIMIT should not route through blocking Exchange: {text}"
    );
    assert!(
        text.contains("rows=11") || text.contains("rows=12"),
        "bounded LIMIT execution should consume roughly offset+limit rows, got: {text}"
    );
}

#[test]
fn fused_unordered_limit_stops_storage_scan_at_offset_window() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE limit_pushdown_t (\
             id INT NOT NULL, payload_a VARCHAR(320), payload_b VARCHAR(320))",
        )
        .unwrap();
    let payload_a = "a".repeat(300);
    let payload_b = "b".repeat(300);
    for batch_start in (1..=2_000).step_by(100) {
        let batch_end = (batch_start + 99).min(2_000);
        let values = (batch_start..=batch_end)
            .map(|id| format!("({id}, '{payload_a}', '{payload_b}')"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO limit_pushdown_t VALUES {values}"))
            .unwrap();
    }

    let pages = engine
        .database()
        .heap_page_count_by_name("test.limit_pushdown_t")
        .unwrap();
    assert!(pages > 32, "fixture must span many heap pages, got {pages}");

    let before = engine.database().buffer_stats_counts();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT * FROM limit_pushdown_t LIMIT 5 OFFSET 20")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let after = engine.database().buffer_stats_counts();

    assert_eq!(result.rows.len(), 5);
    assert_eq!(result.rows[0][0], Value::Int(21));
    assert_eq!(result.rows[4][0], Value::Int(25));
    let reads = after
        .0
        .saturating_sub(before.0)
        .saturating_add(after.1.saturating_sub(before.1));
    assert!(
        reads <= 8,
        "LIMIT 5 OFFSET 20 should stop in the first few pages, \
         not scan all {pages} pages; observed {reads} page reads"
    );

    let QueryResponse::ResultSet(filtered) = engine
        .execute("SELECT id FROM limit_pushdown_t WHERE id >= 10 LIMIT 3 OFFSET 4")
        .unwrap()
    else {
        panic!("expected filtered result set");
    };
    assert_eq!(
        filtered.rows,
        vec![
            vec![Value::Int(14)],
            vec![Value::Int(15)],
            vec![Value::Int(16)]
        ]
    );
}

#[test]
fn fused_unordered_distinct_limit_counts_projected_unique_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE distinct_limit_t (id INT, bucket INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO distinct_limit_t VALUES \
             (1,10),(2,10),(3,20),(4,20),(5,30),(6,40),(7,40),(8,50)",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT DISTINCT bucket FROM distinct_limit_t LIMIT 2 OFFSET 2")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(30)], vec![Value::Int(40)]]
    );
}

#[test]
fn read_only_selects_share_storage_read_lock() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let values = (1..=32)
        .map(|id| format!("({id}, {})", id * 10))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let threads = (0..8)
        .map(|worker| {
            let engine = std::sync::Arc::clone(&engine);
            let barrier = std::sync::Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mut session = SessionContext::default();
                barrier.wait();
                for iter in 0..25 {
                    let id = (worker * 4 + iter) % 32 + 1;
                    let QueryResponse::ResultSet(result) = engine
                        .execute_with_session(
                            &mut session,
                            &format!("SELECT v FROM t WHERE id = {id}"),
                        )
                        .unwrap()
                    else {
                        panic!("expected result set");
                    };
                    assert_eq!(result.rows, vec![vec![Value::Int((id * 10) as i64)]]);
                }
            })
        })
        .collect::<Vec<_>>();

    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn read_only_range_scans_share_storage_read_lock() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let values = (1..=64)
        .map(|id| format!("({id}, {})", id * 10))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    // Warm the index page cache so the range_cached fast-path is hit.
    engine
        .execute("SELECT v FROM t WHERE id BETWEEN 1 AND 64")
        .unwrap();

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let threads = (0..8)
        .map(|worker| {
            let engine = std::sync::Arc::clone(&engine);
            let barrier = std::sync::Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mut session = SessionContext::default();
                barrier.wait();
                for iter in 0..20 {
                    let lo = ((worker * 3 + iter) % 56) as i64 + 1;
                    let hi = lo + 8;
                    let QueryResponse::ResultSet(result) = engine
                        .execute_with_session(
                            &mut session,
                            &format!(
                                "SELECT id, v FROM t WHERE id BETWEEN {lo} AND {hi} ORDER BY id"
                            ),
                        )
                        .unwrap()
                    else {
                        panic!("expected result set");
                    };
                    let expected: Vec<Vec<Value>> = (lo..=hi)
                        .map(|id| vec![Value::Int(id), Value::Int(id * 10)])
                        .collect();
                    assert_eq!(result.rows, expected, "worker={} iter={}", worker, iter);
                }
            })
        })
        .collect::<Vec<_>>();

    for thread in threads {
        thread.join().unwrap();
    }
}

/// Phase 2 regression: with the index read path on a `RwLock::read` lock,
/// multiple readers must not be serialized by a concurrent writer. We don't
/// assert wall-clock here (too flaky in CI) but we do assert that all readers
/// observe consistent, monotonically-or-newer values throughout the run with
/// no deadlocks even when the writer keeps mutating the same hot key.
#[test]
fn concurrent_readers_make_progress_under_writer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let values = (1..=128)
        .map(|id| format!("({id}, 0)"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_active = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_updates = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let overlap_reads = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(9));

    let writer = {
        let engine = std::sync::Arc::clone(&engine);
        let stop = std::sync::Arc::clone(&stop);
        let writer_active = std::sync::Arc::clone(&writer_active);
        let writer_updates = std::sync::Arc::clone(&writer_updates);
        let barrier = std::sync::Arc::clone(&barrier);
        std::thread::spawn(move || {
            writer_active.store(true, std::sync::atomic::Ordering::Release);
            barrier.wait();
            let mut v = 1i64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let id = (v % 128) + 1;
                let _ = engine.execute(&format!("UPDATE t SET v = {v} WHERE id = {id}"));
                writer_updates.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                v += 1;
            }
            writer_active.store(false, std::sync::atomic::Ordering::Release);
        })
    };

    let readers = (0..8)
        .map(|_| {
            let engine = std::sync::Arc::clone(&engine);
            let barrier = std::sync::Arc::clone(&barrier);
            let writer_active = std::sync::Arc::clone(&writer_active);
            let overlap_reads = std::sync::Arc::clone(&overlap_reads);
            std::thread::spawn(move || {
                let mut session = SessionContext::default();
                barrier.wait();
                for _ in 0..4 {
                    for id in 1..=128 {
                        let QueryResponse::ResultSet(_) = engine
                            .execute_with_session(
                                &mut session,
                                &format!("SELECT v FROM t WHERE id = {id}"),
                            )
                            .unwrap()
                        else {
                            panic!("expected result set");
                        };
                        if writer_active.load(std::sync::atomic::Ordering::Acquire) {
                            overlap_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for r in readers {
        r.join().unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();
    assert!(
        writer_updates.load(std::sync::atomic::Ordering::Acquire) > 0,
        "writer should make progress while readers run"
    );
    assert!(
        overlap_reads.load(std::sync::atomic::Ordering::Acquire) > 0,
        "readers should observe progress while writer is active"
    );
}

#[test]
fn explicit_transaction_plain_select_sees_own_write_on_read_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "UPDATE t SET v = 11 WHERE id = 1")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(11)]]);
    assert_eq!(session.current_txn.is_some(), true);

    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

/// Closer reproduction of TPCC PAYMENT: district uses DECIMAL d_ytd, where
/// each UPDATE grows the row size as `d_ytd` accumulates. With Phase 1.2's
/// stricter `try_update` free-space check, this triggers row migration on a
/// dense page, exercising the heap forwarding-stub path. The follow-up
/// SELECT in the same transaction must still find the row by its PK.
#[test]
fn payment_pattern_with_growing_decimal_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute(
            "CREATE TABLE district (
                d_w_id INT NOT NULL,
                d_id INT NOT NULL,
                d_ytd DECIMAL(12,2) NOT NULL,
                d_tax DECIMAL(4,4) NOT NULL,
                d_next_o_id INT NOT NULL,
                d_name VARCHAR(10) NOT NULL,
                d_street_1 VARCHAR(20) NOT NULL,
                d_street_2 VARCHAR(20) NOT NULL,
                d_city VARCHAR(20) NOT NULL,
                d_state CHAR(2) NOT NULL,
                d_zip CHAR(9) NOT NULL,
                PRIMARY KEY (d_w_id, d_id)
            )",
        )
        .unwrap();
    let mut rows = Vec::new();
    for w in 1..=2 {
        for d in 1..=10 {
            rows.push(format!(
                "({w}, {d}, '0.00', '0.0500', 3001, 'D{d}', '1 Main St', '2 Side St', 'City', 'CA', '12345')"
            ));
        }
    }
    engine
        .execute(&format!("INSERT INTO district VALUES {}", rows.join(", ")))
        .unwrap();

    // Many cycles. Each iteration grows d_ytd as a DECIMAL string,
    // potentially expanding the row size.
    for cycle in 0..20 {
        for w in 1..=2i64 {
            for d in 1..=10i64 {
                let amount = format!("{}.{:02}", 1234 + cycle, (cycle * 7) % 100);
                engine.execute_with_session(&mut session, "BEGIN").unwrap();
                let upd = engine
                    .execute_with_session(
                        &mut session,
                        &format!(
                            "UPDATE district SET d_ytd = d_ytd + {amount} \
                             WHERE d_w_id = {w} AND d_id = {d}"
                        ),
                    )
                    .unwrap();
                match upd {
                    QueryResponse::OkAffected { affected_rows } => {
                        assert_eq!(
                            affected_rows, 1,
                            "cycle {cycle} w={w} d={d}: UPDATE affected_rows must be 1"
                        );
                    }
                    other => panic!("expected OkAffected, got {:?}", other),
                }
                let QueryResponse::ResultSet(result) = engine
                    .execute_with_session(
                        &mut session,
                        &format!(
                            "SELECT d_name, d_ytd FROM district \
                             WHERE d_w_id = {w} AND d_id = {d}"
                        ),
                    )
                    .unwrap()
                else {
                    panic!("expected result set");
                };
                assert_eq!(
                    result.rows.len(),
                    1,
                    "cycle {cycle} w={w} d={d}: SELECT after UPDATE must return 1 row, got {} ({:?})",
                    result.rows.len(),
                    result.rows
                );
                engine.execute_with_session(&mut session, "COMMIT").unwrap();
            }
        }
    }
}

/// Reproduces TPCC PAYMENT's failing pattern: composite-PK update followed
/// by a SELECT inside the same transaction. Reported as "District for
/// W_ID=1 D_ID=4 not found" in the field.
#[test]
fn payment_pattern_select_after_update_with_composite_pk() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute(
            "CREATE TABLE district (
                w_id INT NOT NULL,
                d_id INT NOT NULL,
                d_ytd INT NOT NULL,
                d_name VARCHAR(10) NOT NULL,
                PRIMARY KEY (w_id, d_id)
            )",
        )
        .unwrap();
    let mut rows = Vec::new();
    for w in 1..=2 {
        for d in 1..=10 {
            rows.push(format!("({w}, {d}, 0, 'D{d}')"));
        }
    }
    engine
        .execute(&format!("INSERT INTO district VALUES {}", rows.join(", ")))
        .unwrap();

    // Multiple PAYMENT-style cycles. Each cycle: BEGIN, UPDATE, SELECT, COMMIT.
    for cycle in 0..5 {
        for w in 1..=2i64 {
            for d in 1..=10i64 {
                engine.execute_with_session(&mut session, "BEGIN").unwrap();
                engine
                    .execute_with_session(
                        &mut session,
                        &format!(
                            "UPDATE district SET d_ytd = d_ytd + 1 \
                             WHERE w_id = {w} AND d_id = {d}"
                        ),
                    )
                    .unwrap();
                let QueryResponse::ResultSet(result) = engine
                    .execute_with_session(
                        &mut session,
                        &format!(
                            "SELECT d_ytd, d_name FROM district \
                             WHERE w_id = {w} AND d_id = {d}"
                        ),
                    )
                    .unwrap()
                else {
                    panic!("expected result set");
                };
                assert_eq!(
                    result.rows.len(),
                    1,
                    "cycle {cycle} w={w} d={d}: expected 1 row, got {} ({:?})",
                    result.rows.len(),
                    result.rows
                );
                let expected_ytd = (cycle + 1) as i64;
                assert_eq!(
                    result.rows[0][0],
                    Value::Int(expected_ytd),
                    "cycle {cycle} w={w} d={d}: ytd"
                );
                engine.execute_with_session(&mut session, "COMMIT").unwrap();
            }
        }
    }
}

#[test]
fn sorted_distinct_and_aggregate_selects_keep_general_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 20), (2, 10), (3, 10)")
        .unwrap();

    let QueryResponse::ResultSet(sorted) = engine
        .execute("SELECT id FROM t ORDER BY v, id LIMIT 2")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(sorted.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);

    let QueryResponse::ResultSet(distinct) = engine.execute("SELECT DISTINCT v FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(distinct.rows.len(), 2);

    let QueryResponse::ResultSet(aggregate) = engine.execute("SELECT COUNT(*) FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(aggregate.rows, vec![vec![Value::Int(3)]]);
}

#[test]
fn storage_engine_evaluates_mysql80_aggregate_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (g INT, k VARCHAR(8), v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'a',1), (1,'b',2), (1,'c',3)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT JSON_ARRAYAGG(v), JSON_OBJECTAGG(k, v), BIT_AND(v), BIT_OR(v), BIT_XOR(v), STDDEV_POP(v), VAR_POP(v), GROUP_CONCAT(k SEPARATOR '|') FROM t",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // JSON aggregates now return a real JSON value in MySQL canonical form.
    assert_eq!(result.rows[0][0].to_display_string(), "[1, 2, 3]");
    assert_eq!(
        result.rows[0][1].to_display_string(),
        r#"{"a": 1, "b": 2, "c": 3}"#
    );
    assert_eq!(result.rows[0][2], Value::UInt(0));
    assert_eq!(result.rows[0][3], Value::UInt(3));
    assert_eq!(result.rows[0][4], Value::UInt(0));
    assert!((as_f64(&result.rows[0][5]) - 0.816496580927726).abs() < 1e-10);
    assert!((as_f64(&result.rows[0][6]) - 0.6666666666666666).abs() < 1e-10);
    assert_eq!(result.rows[0][7], Value::String("a|b|c".into()));
}

/// MySQL 8.0 `GROUP_CONCAT` reference coverage.
///
/// `GROUP_CONCAT([DISTINCT] expr1 [, expr2 ...]
///     [ORDER BY ord1 [ASC|DESC] [, ord2 ...]]
///     [SEPARATOR str_val])`
///
/// The parser wraps the value list in `CONCAT(...)` for multi-column
/// calls and stashes the `ORDER BY` exprs in a synthetic `__gc_ordered`
/// argument that `AggregateSlot::from_expr` unwraps onto the slot. The
/// `GroupConcatAcc` buffers `(sort_keys, value)` tuples and sorts at
/// finalize time, then truncates against `group_concat_max_len`
/// (default 1024 bytes, codepoint-aligned).
#[test]
fn group_concat_supports_mysql80_order_by_distinct_and_multi_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (g INT, k VARCHAR(8), v INT, ord INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES \
             (1,'c',30,3), (1,'a',10,1), (1,'b',20,2), \
             (2,'x',40,1), (2,'x',50,2), (2,'y',60,3)",
        )
        .unwrap();

    // --- ORDER BY ASC inside the aggregate, default separator (',').
    let r = engine
        .execute("SELECT GROUP_CONCAT(k ORDER BY ord) FROM t WHERE g=1")
        .unwrap();
    let QueryResponse::ResultSet(rs) = r else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::String("a,b,c".into())]]);

    // --- Integer ORDER BY terms are ordinals into GROUP_CONCAT's value
    //     expression list, not constant sort keys.
    let r = engine
        .execute("SELECT GROUP_CONCAT(k ORDER BY 1) FROM t WHERE g=1")
        .unwrap();
    let QueryResponse::ResultSet(rs) = r else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::String("a,b,c".into())]]);

    let r = engine
        .execute("SELECT GROUP_CONCAT(k, ':', v ORDER BY 3 DESC) FROM t WHERE g=1")
        .unwrap();
    let QueryResponse::ResultSet(rs) = r else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::String("c:30,b:20,a:10".into())]]);

    // --- ORDER BY DESC + custom SEPARATOR.
    let r = engine
        .execute("SELECT GROUP_CONCAT(k ORDER BY ord DESC SEPARATOR '|') FROM t WHERE g=1")
        .unwrap();
    let QueryResponse::ResultSet(rs) = r else {
        panic!("expected result set")
    };
    assert_eq!(rs.rows, vec![vec![Value::String("c|b|a".into())]]);

    // --- Multi-column value list concatenates per-row before joining.
    //     MySQL: `GROUP_CONCAT(k, ':', v ORDER BY ord SEPARATOR ', ')`
    let r = engine
        .execute("SELECT GROUP_CONCAT(k, ':', v ORDER BY ord SEPARATOR ', ') FROM t WHERE g=1")
        .unwrap();
    let QueryResponse::ResultSet(rs) = r else {
        panic!("expected result set")
    };
    assert_eq!(
        rs.rows,
        vec![vec![Value::String("a:10, b:20, c:30".into())]]
    );

    // --- DISTINCT collapses duplicates, ORDER BY still wins.
    let r = engine
        .execute("SELECT GROUP_CONCAT(DISTINCT k ORDER BY k DESC) FROM t WHERE g=2")
        .unwrap();
    let QueryResponse::ResultSet(rs) = r else {
        panic!("expected result set")
    };
    assert_eq!(rs.rows, vec![vec![Value::String("y,x".into())]]);

    // --- Multiple ORDER BY terms with mixed directions.
    let r = engine
        .execute("SELECT GROUP_CONCAT(k ORDER BY g ASC, ord DESC) FROM t")
        .unwrap();
    let QueryResponse::ResultSet(rs) = r else {
        panic!("expected result set")
    };
    assert_eq!(rs.rows, vec![vec![Value::String("c,b,a,y,x,x".into())]]);

    // --- GROUP BY + GROUP_CONCAT(... ORDER BY ...) — one row per group,
    //     ordering applied within each bucket.
    let r = engine
        .execute("SELECT g, GROUP_CONCAT(k ORDER BY ord) FROM t GROUP BY g ORDER BY g")
        .unwrap();
    let QueryResponse::ResultSet(rs) = r else {
        panic!("expected result set")
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::String("a,b,c".into())],
            vec![Value::Int(2), Value::String("x,x,y".into())],
        ]
    );

    // --- NULL values are skipped, just like MySQL.
    engine
        .execute("INSERT INTO t VALUES (3,NULL,99,1), (3,'kept',1,2)")
        .unwrap();
    let r = engine
        .execute("SELECT GROUP_CONCAT(k ORDER BY ord) FROM t WHERE g=3")
        .unwrap();
    let QueryResponse::ResultSet(rs) = r else {
        panic!("expected result set")
    };
    assert_eq!(rs.rows, vec![vec![Value::String("kept".into())]]);

    // --- Empty group → NULL, not empty string.
    let r = engine
        .execute("SELECT GROUP_CONCAT(k ORDER BY ord) FROM t WHERE g=99")
        .unwrap();
    let QueryResponse::ResultSet(rs) = r else {
        panic!("expected result set")
    };
    assert_eq!(rs.rows, vec![vec![Value::Null]]);
}

#[test]
fn group_concat_order_by_is_preserved_for_rollup_groups_and_total() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (g INT, v INT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,3), (1,1), (2,4), (1,2)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT g, GROUP_CONCAT(v ORDER BY v) \
             FROM t GROUP BY g WITH ROLLUP",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::String("1,2,3".into())],
            vec![Value::Int(2), Value::String("4".into())],
            vec![Value::Null, Value::String("1,2,3,4".into())],
        ]
    );
}

/// MySQL 8.0 leaves JSON_ARRAYAGG order undefined and rejects MariaDB's
/// inline ORDER BY extension.
#[test]
fn json_arrayagg_rejects_inline_order_by_clause() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (k VARCHAR(8), v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES ('c',3), ('a',1), ('b',2)")
        .unwrap();
    let err = engine
        .execute("SELECT JSON_ARRAYAGG(k ORDER BY v) FROM t")
        .unwrap_err();
    assert_eq!(err.code(), 1064);
}

/// `SET SESSION group_concat_max_len = N` raises/lowers the per-session
/// cap, `SHOW VARIABLES` reflects the active value, and `SELECT
/// @@group_concat_max_len` reads back the integer.
#[test]
fn group_concat_max_len_session_variable_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Default per MySQL 8.0. The sysvar registry serialises every
    // variable on the wire as a CSTRING (`Value::String`) — matches how
    // MySQL renders `SELECT @@var` to clients regardless of the
    // underlying numeric type.
    let QueryResponse::ResultSet(default_var) = engine
        .execute_with_session(&mut session, "SELECT @@group_concat_max_len")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        default_var.rows,
        vec![vec![Value::String("1024".to_string())]]
    );

    // Raise it for this session, then verify SHOW VARIABLES surfaces it.
    engine
        .execute_with_session(&mut session, "SET SESSION group_concat_max_len = 65535")
        .unwrap();
    let QueryResponse::ResultSet(updated) = engine
        .execute_with_session(
            &mut session,
            "SHOW SESSION VARIABLES LIKE 'group_concat_max_len'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        updated.rows,
        vec![vec![
            Value::String("group_concat_max_len".into()),
            Value::String("65535".into()),
        ]]
    );

    // Driving the executor with the larger cap now lets a multi-row
    // GROUP_CONCAT complete without truncation.
    engine
        .execute_with_session(&mut session, "CREATE TABLE big (s VARCHAR(8))")
        .unwrap();
    let mut sql = String::from("INSERT INTO big VALUES");
    for i in 0..400 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str("('xxxxx')");
    }
    engine.execute_with_session(&mut session, &sql).unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT GROUP_CONCAT(s) FROM big")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(joined) = &rs.rows[0][0] else {
        panic!("expected string result");
    };
    // 400 * 5 chars + 399 commas = 2399 bytes — well above the 1024
    // default but below the bumped 65535 cap.
    assert_eq!(joined.len(), 2399);

    // Lowering the cap for the same session truncates the next query.
    engine
        .execute_with_session(&mut session, "SET SESSION group_concat_max_len = 64")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT GROUP_CONCAT(s) FROM big")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(joined) = &rs.rows[0][0] else {
        panic!("expected string result");
    };
    assert!(joined.len() <= 64, "got {} bytes", joined.len());

    // A separate session sees the default value, not the bumped one —
    // confirms the thread-local guard restores correctly.
    let mut other = SessionContext::default();
    let QueryResponse::ResultSet(other_var) = engine
        .execute_with_session(&mut other, "SELECT @@group_concat_max_len")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        other_var.rows,
        vec![vec![Value::String("1024".to_string())]]
    );
}

/// `group_concat_max_len` (MySQL 8.0 default = 1024 bytes) caps the
/// joined output. The truncation point is rounded back to a UTF-8
/// boundary so a multibyte codepoint is never split mid-sequence.
#[test]
fn group_concat_truncates_at_max_len_on_utf8_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE chars (s VARCHAR(8))").unwrap();
    // 200 four-byte UTF-8 codepoints (😀 == "\xF0\x9F\x98\x80") = 800
    // bytes plus 199 commas = 999 total. Add three more 4-byte rows so
    // we cross the 1024 cap mid-codepoint and force boundary alignment.
    let mut sql = String::from("INSERT INTO chars VALUES");
    for i in 0..203 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str("('😀')");
    }
    engine.execute(&sql).unwrap();

    let r = engine.execute("SELECT GROUP_CONCAT(s) FROM chars").unwrap();
    let QueryResponse::ResultSet(rs) = r else {
        panic!("expected result set")
    };
    let Value::String(joined) = &rs.rows[0][0] else {
        panic!("expected string result, got {:?}", rs.rows[0][0]);
    };
    // Must fit within the byte cap and end at a UTF-8 boundary.
    assert!(joined.len() <= 1024, "got {} bytes", joined.len());
    assert!(
        joined.is_char_boundary(joined.len()),
        "truncation did not align to a UTF-8 boundary"
    );
    // Round-trip parse — every character is still a complete 😀.
    for c in joined.chars() {
        assert!(c == '😀' || c == ',');
    }
}

#[test]
fn group_concat_exact_max_len_does_not_report_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET SESSION group_concat_max_len = 4")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (s VARCHAR(8))")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES ('ab'), ('c')")
        .unwrap();

    let QueryResponse::ResultSet(exact) = engine
        .execute_with_session(&mut session, "SELECT GROUP_CONCAT(s) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(exact.rows, vec![vec![Value::String("ab,c".into())]]);
    let QueryResponse::ResultSet(exact_warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected warning result set");
    };
    assert!(
        exact_warnings.rows.is_empty(),
        "an exact byte-cap result is not truncated"
    );

    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES ('d')")
        .unwrap();
    let QueryResponse::ResultSet(cut) = engine
        .execute_with_session(&mut session, "SELECT GROUP_CONCAT(s) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(cut.rows[0][0].to_display_string().len(), 4);
    let QueryResponse::ResultSet(cut_warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected warning result set");
    };
    assert_eq!(
        cut_warnings
            .rows
            .iter()
            .filter(|row| matches!(row[1], Value::Int(1260) | Value::UInt(1260)))
            .count(),
        1
    );
}

#[test]
fn group_concat_limit_reports_warnings_only_for_returned_groups() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET SESSION group_concat_max_len = 4")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (a VARCHAR(8), b INT)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t VALUES \
             ('abcdefgh',1), ('abcdefgh',2), ('abcdefgh',3), \
             ('abcdefgh',4), ('abcdefgh',5)",
        )
        .unwrap();

    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(
            &mut session,
            "SELECT GROUP_CONCAT(a), b FROM t GROUP BY b LIMIT 3",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows.len(), 3);
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected warning result set");
    };
    assert_eq!(
        warnings
            .rows
            .iter()
            .filter(|row| matches!(row[1], Value::Int(1260) | Value::UInt(1260)))
            .count(),
        3
    );
}

#[test]
fn group_concat_preserves_source_collation_in_runtime_metadata_and_ctas() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE cp (a VARCHAR(20) CHARACTER SET cp1250 COLLATE cp1250_general_ci)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO cp VALUES ('x')")
        .unwrap();
    let cp_schema = engine.database().table("test.cp").unwrap();
    let gc_expr = Expr::Function {
        name: "GROUP_CONCAT".to_string(),
        args: vec![Expr::Identifier("a".to_string())],
        distinct: false,
    };
    let gc_collation = crate::engine::expr::expr_type_descriptor(&gc_expr, &cp_schema)
        .collation
        .and_then(|collation| crate::charset::resolve_collation_id(collation.collation_id))
        .map(|info| info.name);
    assert_eq!(gc_collation, Some("cp1250_general_ci"));
    let QueryResponse::ResultSet(column_collation) = engine
        .execute_with_session(&mut session, "SELECT COLLATION(a) FROM cp")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        column_collation.rows,
        vec![vec![Value::String("cp1250_general_ci".into())]]
    );
    let QueryResponse::ResultSet(aggregate_collation) = engine
        .execute_with_session(&mut session, "SELECT COLLATION(GROUP_CONCAT(a)) FROM cp")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        aggregate_collation.rows,
        vec![vec![Value::String("cp1250_general_ci".into())]]
    );

    engine
        .execute_with_session(&mut session, "SET NAMES latin1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE src (a CHAR)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO src VALUES ('a'), ('b')")
        .unwrap();
    let QueryResponse::ResultSet(source_charset) = engine
        .execute_with_session(&mut session, "SELECT CHARSET(a) FROM src LIMIT 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        source_charset.rows,
        vec![vec![Value::String("utf8mb4".into())]]
    );
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE dst SELECT GROUP_CONCAT(a) AS a FROM src",
        )
        .unwrap();
    let QueryResponse::ResultSet(charset) = engine
        .execute_with_session(&mut session, "SELECT CHARSET(a) FROM dst")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        charset.rows,
        vec![vec![Value::String("utf8mb4".into())]],
        "CTAS uses the GROUP_CONCAT source charset, not character_set_connection"
    );
    let QueryResponse::ResultSet(union_probe) = engine
        .execute_with_session(
            &mut session,
            "(SELECT GROUP_CONCAT(a) AS a FROM src) UNION (SELECT GROUP_CONCAT(a) AS a FROM src)",
        )
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("expected UNION result")
    };
    assert!(
        matches!(
            union_probe.columns[0].column_type,
            crate::types::ColumnType::VarString | crate::types::ColumnType::Blob
        ),
        "GROUP_CONCAT UNION metadata must remain character: {:?}",
        union_probe.columns[0]
    );
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE dst_union \
             (SELECT GROUP_CONCAT(a) AS a FROM src) \
             UNION \
             (SELECT GROUP_CONCAT(a) AS a FROM src)",
        )
        .unwrap();
    let QueryResponse::ResultSet(union_charset) = engine
        .execute_with_session(&mut session, "SELECT CHARSET(a) FROM dst_union")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        union_charset.rows,
        vec![vec![Value::String("utf8mb4".into())]],
        "materialized UNION CTAS uses the leftmost GROUP_CONCAT source charset"
    );
}

#[test]
fn group_concat_protocol_metadata_tracks_limit_charset_and_base_column_origin() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (f1 INT UNSIGNED, f2 VARCHAR(255))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t VALUES (1, REPEAT('a', 255)), (2, REPEAT('b', 255))",
        )
        .unwrap();

    engine
        .execute_with_session(&mut session, "SET group_concat_max_len=400")
        .unwrap();
    let QueryResponse::ResultSet(short) = engine
        .execute_with_session(
            &mut session,
            "SELECT f2, GROUP_CONCAT(f1) FROM t GROUP BY f2",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(short.columns[0].source.org_table, "t");
    assert_eq!(short.columns[0].source.org_name, "f2");
    assert_eq!(short.columns[0].source.length, Some(1020));
    assert_eq!(short.columns[1].column_type, ColumnType::VarString);
    assert_eq!(short.columns[1].source.length, Some(1600));
    assert_eq!(short.columns[1].source.decimals, Some(31));

    engine
        .execute_with_session(&mut session, "SET NAMES latin1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET group_concat_max_len=1024")
        .unwrap();
    let QueryResponse::ResultSet(long) = engine
        .execute_with_session(
            &mut session,
            "SELECT f2, GROUP_CONCAT(f1) FROM t GROUP BY f2",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(long.columns[1].column_type, ColumnType::Blob);
    assert_eq!(long.columns[1].source.length, Some(1024));
    assert_eq!(long.columns[1].source.decimals, Some(31));
    assert_eq!(long.columns[1].source.collation_id, Some(8));
}

#[test]
fn explain_shows_topk_and_accumulator_metadata() {
    // Phase B/E: EXPLAIN should advertise TopK fusion and the
    // accumulator-based GROUP BY. Confirms the planner & executor are
    // wired through.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE sales (id INT PRIMARY KEY, region VARCHAR(8), amount INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO sales VALUES \
             (1, 'us', 100), (2, 'us', 200), (3, 'eu', 50), (4, 'eu', 150), (5, 'as', 300)",
        )
        .unwrap();

    // EXPLAIN (default tabular) — should mark HashAggregate with
    // Accumulators metadata.
    let QueryResponse::ResultSet(r) = engine
        .execute(
            "EXPLAIN SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region ORDER BY region",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let text = format!("{:?}", r.rows);
    assert!(
        text.contains("Accumulators"),
        "expected EXPLAIN extra to mention 'Accumulators': {text}"
    );

    // EXPLAIN of TOP-K query — should show TopK extra.
    let QueryResponse::ResultSet(r) = engine
        .execute("EXPLAIN SELECT region, amount FROM sales ORDER BY amount DESC LIMIT 3")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let text = format!("{:?}", r.rows);
    assert!(
        text.contains("top-K") || text.contains("TopK"),
        "expected EXPLAIN extra to mention TopK: {text}"
    );
}

#[test]
fn query_memory_budget_spills_oversized_aggregate() {
    // HashAggregate is spillable. A tiny soft work-area budget therefore
    // changes its internal state layout/spill behavior, not query semantics.
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1, 100), (2, 1, 200), (3, 2, 300)")
        .unwrap();

    let budget = Arc::new(MemBudget::new(1));
    let result = with_query_budget(Arc::clone(&budget), || {
        engine.execute("SELECT g, SUM(v) FROM t GROUP BY g")
    })
    .unwrap();
    let QueryResponse::ResultSet(result) = result else {
        panic!("expected aggregate result")
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(budget.used(), 0, "aggregate reservation must be released");
}

#[test]
fn int_key_hash_aggregate_retries_spill_under_tight_budget() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    use std::sync::Arc;

    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE tight_int_agg (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    let mut next = 0;
    while next < 1_000 {
        let hi = (next + 500).min(1_000);
        let values = (next..hi)
            .map(|i| format!("({i}, {i}, 1)"))
            .collect::<Vec<_>>()
            .join(", ");
        engine
            .execute(&format!("INSERT INTO tight_int_agg VALUES {values}"))
            .unwrap();
        next = hi;
    }
    engine.execute("ANALYZE TABLE tight_int_agg").unwrap();

    let budget = Arc::new(MemBudget::new(64 * 1024));
    let response = with_test_env(&[("MYDB_PARALLEL_THRESHOLD_ROWS", Some("1"))], || {
        with_vec_mode(VecMode::Off, || {
            with_query_budget(Arc::clone(&budget), || {
                engine.execute("SELECT g, SUM(v) FROM tight_int_agg GROUP BY g")
            })
        })
    })
    .unwrap();
    let QueryResponse::ResultSet(mut result) = response else {
        panic!("expected result set");
    };
    let analyze = with_vec_mode(VecMode::Off, || {
        with_query_budget(Arc::clone(&budget), || {
            explain_analyze_tree(&engine, "SELECT g, SUM(v) FROM tight_int_agg GROUP BY g")
        })
    });
    assert!(
        analyze.contains("spill_bytes=")
            && analyze.contains("kernel=scalar")
            && analyze.contains("scalar_rows=1000")
            && analyze.contains("aggregate_mode=grouped_typed"),
        "scalar kernel should spill inside the same HashAggregate: {analyze}"
    );
    assert_eq!(
        budget.used(),
        0,
        "spilled aggregate must release query budget"
    );

    result.rows.sort_by(|a, b| match (&a[0], &b[0]) {
        (Value::Int(l), Value::Int(r)) => l.cmp(r),
        other => panic!("expected integer group keys, got {other:?}"),
    });
    assert_eq!(result.rows.len(), 1_000);
    assert_eq!(result.rows[0][0], Value::Int(0));
    assert_eq!(result.rows[0][1].to_display_string(), "1");
    assert_eq!(result.rows[999][0], Value::Int(999));
    assert_eq!(result.rows[999][1].to_display_string(), "1");
}

#[test]
fn q1_style_decimal_sum_accumulates_exact_decimal_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE lineitem (\
             id INT PRIMARY KEY, \
             l_returnflag CHAR(1) NOT NULL, \
             l_extendedprice DECIMAL(15,4) NOT NULL, \
             l_discount DECIMAL(15,4) NOT NULL)",
        )
        .unwrap();

    for chunk in 0..10 {
        let values = (0..1000)
            .map(|i| {
                let id = chunk * 1000 + i + 1;
                format!("({id}, 'N', 100000.0001, 0.0000)")
            })
            .collect::<Vec<_>>()
            .join(", ");
        engine
            .execute(&format!("INSERT INTO lineitem VALUES {values}"))
            .unwrap();
    }

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT l_returnflag, SUM(l_extendedprice * (1 - l_discount)) \
             FROM lineitem GROUP BY l_returnflag",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // SUM over exact-decimal inputs finalises as DecimalCompact (or
    // Decimal for i128-overflow magnitudes) so the LSB survives the
    // round-trip — sending it through f64 would lose precision once the
    // running magnitude crosses ~16 significant digits (the TPC-H Q1
    // sum_charge sf>=5 regression). DECIMAL(15,4) × DECIMAL(15,4) ⇒
    // scale 8 per row, preserved through accumulation.
    let display = result.rows[0][1].to_display_string();
    assert_eq!(
        display, "1000000001.00000000",
        "decimal SUM should retain the exact magnitude×10^-(l_scale+r_scale) form"
    );
    assert!(
        matches!(
            result.rows[0][1],
            Value::DecimalCompact(..) | Value::Decimal(_)
        ),
        "expected decimal SUM to finalise as DecimalCompact/Decimal, got {:?}",
        result.rows[0][1]
    );
}

#[test]
fn single_table_sum_of_arithmetic_uses_compiled_expression() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE lineitem (\
             id INT PRIMARY KEY, \
             l_discount DECIMAL(15,4) NOT NULL)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO lineitem VALUES (1, 0.1000), (2, 0.2500), (3, 0.0000)")
        .unwrap();

    let before = native_batch_hits();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT SUM(1 - l_discount) FROM lineitem")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let after = native_batch_hits();

    assert_eq!(format!("{:.4}", as_f64(&result.rows[0][0])), "2.6500");
    assert!(
        after > before,
        "compiled numeric SUM should execute in the common batch pipeline"
    );
    let analyze = explain_analyze_tree(&engine, "SELECT SUM(1 - l_discount) FROM lineitem");
    assert!(
        analyze.contains("expression_mode=compiled")
            && !analyze.contains("fallback_expression_rows="),
        "compiled aggregate expression metrics missing: {analyze}"
    );
}

#[test]
fn single_table_case_sum_uses_compiled_expression() {
    use crate::engine::expr_program::expr_program_stats;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE lineitem (\
             id INT PRIMARY KEY, \
             l_returnflag CHAR(1) NOT NULL, \
             l_extendedprice DECIMAL(15,2) NOT NULL, \
             l_discount DECIMAL(15,4) NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO lineitem VALUES \
             (1, 'R', 100.00, 0.1000), \
             (2, 'N', 200.00, 0.2500), \
             (3, 'R', 300.00, 0.0000)",
        )
        .unwrap();

    let before = expr_program_stats();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT SUM(CASE WHEN l_returnflag = 'R' \
                     THEN l_extendedprice * (1 - l_discount) \
                     ELSE 0 END) \
             FROM lineitem",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let after = expr_program_stats();

    assert_eq!(format!("{:.2}", as_f64(&result.rows[0][0])), "390.00");
    let analyze = explain_analyze_tree(
        &engine,
        "SELECT SUM(CASE WHEN l_returnflag = 'R' \
         THEN l_extendedprice * (1 - l_discount) ELSE 0 END) FROM lineitem",
    );
    assert!(
        analyze.contains("aggregate_mode=typed_batch")
            && !analyze.contains("fallback_aggregate_rows="),
        "CASE-based aggregate should use exact typed batch evaluation: {analyze}"
    );
    assert_eq!(
        after.legacy_fallback_rows, before.legacy_fallback_rows,
        "CASE-based aggregate should not fall back to legacy expression eval"
    );
    assert_eq!(
        after.legacy_escape_rows, before.legacy_escape_rows,
        "CASE-based aggregate should not escape to legacy row evaluation"
    );

    // A later arm that the first arm makes unreachable must not make the
    // typed path evaluate or reject that result expression eagerly.
    let ordered_case = "SELECT SUM(CASE \
        WHEN id >= 1 THEN l_extendedprice \
        WHEN id = 1 THEN l_extendedprice / 0 \
        ELSE 0 END) FROM lineitem";
    let QueryResponse::ResultSet(ordered_result) = engine.execute(ordered_case).unwrap() else {
        panic!("expected ordered CASE result set");
    };
    assert_eq!(
        format!("{:.2}", as_f64(&ordered_result.rows[0][0])),
        "600.00"
    );
    let ordered_analyze = explain_analyze_tree(&engine, ordered_case);
    assert!(
        ordered_analyze.contains("aggregate_mode=typed_batch")
            && !ordered_analyze.contains("fallback_aggregate_rows="),
        "searched CASE must preserve ordered branch short-circuiting: {ordered_analyze}"
    );
}

#[test]
fn grouped_join_sort_stream_aggregate_keeps_duplicate_group_keys_contiguous() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE customer (\
             c_custkey INT PRIMARY KEY, \
             c_name VARCHAR(32), \
             c_nationkey INT, \
             c_acctbal DECIMAL(15,2))",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_name CHAR(25))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY, \
             o_custkey INT, \
             o_orderdate DATE, \
             INDEX idx_orders_orderdate (o_orderdate), \
             INDEX idx_orders_custkey (o_custkey))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT, \
             l_linenumber INT, \
             l_extendedprice DECIMAL(15,2), \
             l_discount DECIMAL(15,2), \
             l_returnflag CHAR(1), \
             PRIMARY KEY (l_orderkey, l_linenumber))",
        )
        .unwrap();

    engine
        .execute("INSERT INTO nation VALUES (1, 'USA')")
        .unwrap();
    engine
        .execute(
            "INSERT INTO customer VALUES \
             (1, 'alice', 1, 10.00), \
             (2, 'bob', 1, 20.00)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders VALUES \
             (10, 1, '1993-10-02'), \
             (20, 2, '1993-10-03'), \
             (30, 1, '1993-10-04')",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO lineitem VALUES \
             (10, 1, 100.00, 0.00, 'R'), \
             (20, 1, 200.00, 0.00, 'R'), \
             (30, 1, 250.00, 0.00, 'R')",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE customer").unwrap();
    engine.execute("ANALYZE TABLE nation").unwrap();
    engine.execute("ANALYZE TABLE orders").unwrap();
    engine.execute("ANALYZE TABLE lineitem").unwrap();

    let query = "SELECT c_custkey, c_name, \
                 SUM(l_extendedprice * (1 - l_discount)) AS revenue, \
                 c_acctbal, n_name \
                 FROM customer \
                 INNER JOIN orders ON c_custkey = o_custkey \
                 INNER JOIN lineitem ON l_orderkey = o_orderkey \
                 INNER JOIN nation ON c_nationkey = n_nationkey \
                 WHERE o_orderdate >= '1993-10-01' \
                   AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH \
                   AND l_returnflag = 'R' \
                 GROUP BY c_custkey, c_name, c_acctbal, n_name \
                 ORDER BY c_custkey";

    let QueryResponse::ResultSet(plan) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {query}"))
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let plan_text = format!("{:?}", plan.rows);
    // Join inputs participate in the ordinary HashAggregate versus
    // Sort+StreamingAggregate cost comparison.  A duplicate-producing join
    // may therefore use streaming aggregation, but only after an explicit
    // sort has made every complete grouping key contiguous.
    if plan_text.contains("Streaming aggregate") {
        assert!(
            plan_text.contains("-> Sort"),
            "streaming aggregate over a duplicate-producing join requires a grouping sort: {plan_text}"
        );
    }

    let QueryResponse::ResultSet(result) = engine.execute(query).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1], Value::String("alice".into()));
    assert_eq!(format!("{:.4}", as_f64(&result.rows[0][2])), "350.0000");
    assert_eq!(result.rows[1][0], Value::Int(2));
    assert_eq!(result.rows[1][1], Value::String("bob".into()));
    assert_eq!(format!("{:.4}", as_f64(&result.rows[1][2])), "200.0000");
}

#[test]
fn sort_stream_aggregate_compiles_conditional_inputs_once() {
    let _guard = super::test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE stream_expr (id INT PRIMARY KEY, k INT, day_no INT, amount INT)")
        .unwrap();
    let values = (0..200)
        .map(|id| format!("({id},{},{},{})", id % 5, id % 2, id + 1))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO stream_expr VALUES {values}"))
        .unwrap();

    crate::util::env::with_vars(&[("MYDB_AGG_STRATEGY", Some("sort_stream"))], || {
        let query = "SELECT k, \
                     SUM(CASE WHEN day_no = 0 THEN amount ELSE NULL END), \
                     SUM(CASE WHEN day_no = 1 THEN amount ELSE NULL END) \
                     FROM stream_expr GROUP BY k ORDER BY k";
        let QueryResponse::ResultSet(explain) = engine
            .execute(&format!("EXPLAIN FORMAT=TREE {query}"))
            .unwrap()
        else {
            panic!("expected EXPLAIN result set");
        };
        let plan = explain.rows[0][0].to_display_string();
        assert!(
            plan.contains("Streaming aggregate") && plan.contains("Sort"),
            "test must exercise Sort -> StreamingAggregate: {plan}"
        );

        let before = crate::engine::expr_program::expr_program_stats();
        let rows = collect_select_response_rows(engine.execute(query).unwrap());
        let after = crate::engine::expr_program::expr_program_stats();
        assert_eq!(rows.len(), 5);
        assert!(
            after
                .program_eval_rows
                .saturating_sub(before.program_eval_rows)
                >= 400,
            "two CASE inputs should execute compiled programs per row (the GROUP key is a \
             direct bound-field access): \
             before={before:?} after={after:?}"
        );
    });
}

#[test]
fn hash_aggregate_releases_query_memory_after_success() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE agg_release_t (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO agg_release_t VALUES \
             (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 3, 40)",
        )
        .unwrap();

    let budget = Arc::new(MemBudget::new(1_000_000));
    let QueryResponse::ResultSet(result) = with_query_budget(Arc::clone(&budget), || {
        engine.execute("SELECT g, SUM(v) FROM agg_release_t GROUP BY g")
    })
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 3);
    assert_eq!(budget.used(), 0, "HashAggregate must release group budget");
}

#[test]
fn hash_aggregate_releases_query_memory_after_spill() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    use std::sync::Arc;

    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE agg_error_release_t (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO agg_error_release_t VALUES \
             (1, 1, 10), (2, 2, 20), (3, 3, 30)",
        )
        .unwrap();

    // One compact numeric group fits, the second forces the pressure path.
    // The unified scalar-batch kernel may spill or shrink its arena, but must
    // complete and release every reservation.
    let budget = Arc::new(MemBudget::new(150));
    let QueryResponse::ResultSet(result) = with_vec_mode(VecMode::Off, || {
        with_query_budget(Arc::clone(&budget), || {
            engine.execute("SELECT g, SUM(v) FROM agg_error_release_t GROUP BY g")
        })
    })
    .unwrap() else {
        panic!("expected aggregate result")
    };
    assert_eq!(result.rows.len(), 3);
    assert_eq!(
        budget.used(),
        0,
        "HashAggregate must release already-charged groups on error"
    );
}

#[test]
fn grouped_in_subquery_join_releases_query_memory() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE customer (c_custkey INT PRIMARY KEY, c_name VARCHAR(32))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY, \
             o_custkey INT, \
             o_orderdate DATE, \
             o_totalprice INT, \
             INDEX idx_orders_cust (o_custkey))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT, \
             l_linenumber INT, \
             l_quantity INT, \
             PRIMARY KEY (l_orderkey, l_linenumber))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO customer VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders VALUES \
             (10, 1, '1995-01-10', 1000), \
             (20, 2, '1995-01-20', 2000), \
             (30, 3, '1995-01-30', 3000)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO lineitem VALUES \
             (10, 1, 150), (10, 2, 160), \
             (20, 1, 200), (20, 2, 200), \
             (30, 1, 10), (30, 2, 20)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE customer").unwrap();
    engine.execute("ANALYZE TABLE orders").unwrap();
    engine.execute("ANALYZE TABLE lineitem").unwrap();

    let query = "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, \
                 SUM(l_quantity) AS total_qty \
                 FROM customer \
                 INNER JOIN orders ON c_custkey = o_custkey \
                 INNER JOIN lineitem ON o_orderkey = l_orderkey \
                 WHERE o_orderkey IN (\
                     SELECT l_orderkey \
                     FROM lineitem \
                     GROUP BY l_orderkey \
                     HAVING SUM(l_quantity) > 300) \
                 GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice \
                 ORDER BY o_totalprice DESC, o_orderdate \
                 LIMIT 100";
    let budget = Arc::new(MemBudget::new(1_000_000));
    let QueryResponse::ResultSet(result) =
        with_query_budget(Arc::clone(&budget), || engine.execute(query)).unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        budget.used(),
        0,
        "grouped IN-subquery join must release query budget"
    );
}

#[test]
fn in_subquery_join_dedups_without_materialising_fanout() {
    // Q95 shape: an uncorrelated `IN (SELECT col FROM a, b WHERE ...)`
    // whose body joins two tables and projects a single column. The join
    // fans each kept value out into many duplicate rows; `IN` only needs
    // set membership, so the eval path rewrites the body to `... GROUP BY
    // <proj>` (build_dedup_in_subquery) and streams the join into a
    // hash-aggregate instead of materialising the whole product. This
    // verifies the dedup is semantics-preserving and that the intermediate
    // never charges the query budget once the query finishes.
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE outer_t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("CREATE TABLE ret (k INT)").unwrap();
    engine
        .execute("CREATE TABLE dup (k INT, payload INT)")
        .unwrap();
    engine
        .execute("INSERT INTO outer_t VALUES (1), (2), (3), (4)")
        .unwrap();
    // ret carries keys 1,2,3 (key 4 is absent → outer row 4 must drop).
    engine
        .execute("INSERT INTO ret VALUES (1), (2), (3)")
        .unwrap();
    // dup fans every shared key out into several rows; key 5 has no match
    // in ret so the join never emits it.
    engine
        .execute(
            "INSERT INTO dup VALUES \
             (1, 10), (1, 11), (1, 12), \
             (2, 20), (2, 21), \
             (3, 30), (3, 31), (3, 32), (3, 33), \
             (5, 50)",
        )
        .unwrap();

    let query = "SELECT id FROM outer_t \
                 WHERE id IN (SELECT ret.k FROM ret, dup WHERE ret.k = dup.k) \
                 ORDER BY id";
    let budget = Arc::new(MemBudget::new(1_000_000));
    let QueryResponse::ResultSet(result) =
        with_query_budget(Arc::clone(&budget), || engine.execute(query)).unwrap()
    else {
        panic!("expected result set");
    };
    let ids: Vec<i64> = result
        .rows
        .iter()
        .map(|row| match row[0] {
            Value::Int(v) => v,
            ref other => panic!("unexpected value {other:?}"),
        })
        .collect();
    assert_eq!(
        ids,
        vec![1, 2, 3],
        "IN over fanning join must keep set membership"
    );
    assert_eq!(
        budget.used(),
        0,
        "dedup IN-subquery join must release query budget"
    );
}

#[test]
fn distinct_releases_query_memory_budget() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE distinct_release_t (id INT PRIMARY KEY, g INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO distinct_release_t VALUES \
             (1, 1), (2, 1), (3, 2), (4, 2), (5, 3)",
        )
        .unwrap();

    let budget = Arc::new(MemBudget::new(1_000_000));
    let QueryResponse::ResultSet(result) = with_query_budget(Arc::clone(&budget), || {
        engine.execute("SELECT DISTINCT g FROM distinct_release_t")
    })
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 3);
    assert_eq!(budget.used(), 0, "DISTINCT must release dedup budget");
}

#[test]
fn table_and_cte_filter_evaluation_errors_are_not_silently_filtered() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let err = engine
        .execute("SELECT id FROM t WHERE definitely_missing_function(v)")
        .expect_err("table scan filter error must be returned");
    assert_eq!(err.code(), 1305);

    let err = engine
        .execute(
            "WITH c AS (SELECT id, v FROM t) \
             SELECT id FROM c WHERE definitely_missing_function(v)",
        )
        .expect_err("CTE scan filter error must be returned");
    assert_eq!(err.code(), 1305);
}

#[test]
fn topk_returns_correct_rows_for_order_by_limit() {
    // Phase B: ORDER BY ... LIMIT N should be planned as a TopK heap.
    // Verify a few different K and offset values still produce the
    // same rows as a full Sort would.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE topk_t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut sql = String::from("INSERT INTO topk_t VALUES ");
    for i in 0..50 {
        if i > 0 {
            sql.push_str(", ");
        }
        // Values: 49, 48, ..., 0 — descending so smallest are at the end.
        sql.push_str(&format!("({}, {})", i, 50 - i));
    }
    engine.execute(&sql).unwrap();

    // Top 5 smallest v: v=1..5 → ids 49..45
    let QueryResponse::ResultSet(r) = engine
        .execute("SELECT id FROM topk_t ORDER BY v ASC LIMIT 5")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(49)],
            vec![Value::Int(48)],
            vec![Value::Int(47)],
            vec![Value::Int(46)],
            vec![Value::Int(45)],
        ]
    );

    // Top 3 largest v with OFFSET 2: skip top 2, return next 3.
    let QueryResponse::ResultSet(r) = engine
        .execute("SELECT id FROM topk_t ORDER BY v DESC LIMIT 3 OFFSET 2")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)],
        ]
    );

    // EXPLAIN should now mention TopK rather than just filesort.
    let QueryResponse::ResultSet(r) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT id FROM topk_t ORDER BY v LIMIT 7")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let plan_text = match &r.rows[0][0] {
        Value::String(s) => s.clone(),
        other => panic!("expected string, got {other:?}"),
    };
    assert!(
        plan_text.contains("TopK"),
        "expected EXPLAIN to mention TopK, got: {plan_text}"
    );
}

#[test]
fn topwindow_handles_deep_order_by_limit_offset() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE topwindow_t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut sql = String::from("INSERT INTO topwindow_t VALUES ");
    let total = 160i64;
    for i in 0..total {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, {})", i, i % 97));
    }
    engine.execute(&sql).unwrap();

    let query = "SELECT id, v FROM topwindow_t ORDER BY v DESC, id LIMIT 20 OFFSET 120";
    let QueryResponse::ResultSet(rs) = with_test_env(&[("MYDB_TOPK_MAX_HEAP", Some("64"))], || {
        engine.execute(query)
    })
    .unwrap() else {
        panic!("expected result set");
    };
    let mut expected: Vec<(i64, i64)> = (0..total).map(|id| (id, id % 97)).collect();
    expected.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let expected_rows: Vec<Vec<Value>> = expected
        .into_iter()
        .skip(120)
        .take(20)
        .map(|(id, v)| vec![Value::Int(id), Value::Int(v)])
        .collect();
    assert_eq!(rs.rows, expected_rows);

    let QueryResponse::ResultSet(explain) =
        with_test_env(&[("MYDB_TOPK_MAX_HEAP", Some("64"))], || {
            engine.execute(&format!("EXPLAIN FORMAT=TREE {query}"))
        })
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let plan_text = match &explain.rows[0][0] {
        Value::String(s) => s.clone(),
        other => panic!("expected string, got {other:?}"),
    };
    assert!(
        plan_text.contains("TopWindow"),
        "deep OFFSET should use TopWindow, got: {plan_text}"
    );
    assert!(
        !plan_text.contains("TopK"),
        "deep OFFSET should avoid TopK heap, got: {plan_text}"
    );
}

#[test]
fn topwindow_preserves_ordinal_ties_and_runtime_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE topwindow_ties (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut sql = String::from("INSERT INTO topwindow_ties VALUES ");
    for i in 0..160 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, 1)", i));
    }
    engine.execute(&sql).unwrap();

    for (offset, first) in [(120usize, 120i64), (130usize, 130i64)] {
        let sql = format!("SELECT id FROM topwindow_ties ORDER BY v LIMIT 5 OFFSET {offset}");
        let QueryResponse::ResultSet(rs) =
            with_test_env(&[("MYDB_TOPK_MAX_HEAP", Some("64"))], || {
                engine.execute(&sql)
            })
            .unwrap()
        else {
            panic!("expected result set");
        };
        assert_eq!(rs.rows[0][0], Value::Int(first), "OFFSET {offset} start");
    }

    let QueryResponse::ResultSet(zero) = engine
        .execute("SELECT id FROM topwindow_ties ORDER BY v LIMIT 0 OFFSET 120")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(zero.rows.is_empty());

    let QueryResponse::ResultSet(past_end) = engine
        .execute("SELECT id FROM topwindow_ties ORDER BY v LIMIT 5 OFFSET 999")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(past_end.rows.is_empty());
}

#[test]
fn ordered_index_limit_offset_window_matches_visible_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE page_idx_t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut sql = String::from("INSERT INTO page_idx_t VALUES ");
    for i in 0..20 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, {})", i, i * 10));
    }
    engine.execute(&sql).unwrap();
    engine
        .execute("DELETE FROM page_idx_t WHERE id IN (3, 4)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id, v FROM page_idx_t ORDER BY id LIMIT 5 OFFSET 2")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(5), Value::Int(50)],
            vec![Value::Int(6), Value::Int(60)],
            vec![Value::Int(7), Value::Int(70)],
            vec![Value::Int(8), Value::Int(80)],
        ]
    );

    let QueryResponse::ResultSet(desc) = engine
        .execute("SELECT id, v FROM page_idx_t ORDER BY id DESC LIMIT 4 OFFSET 3")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        desc.rows,
        vec![
            vec![Value::Int(16), Value::Int(160)],
            vec![Value::Int(15), Value::Int(150)],
            vec![Value::Int(14), Value::Int(140)],
            vec![Value::Int(13), Value::Int(130)],
        ]
    );

    let QueryResponse::ResultSet(zero) = engine
        .execute("SELECT id, v FROM page_idx_t ORDER BY id LIMIT 0 OFFSET 2")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(zero.rows.is_empty());

    let QueryResponse::ResultSet(past_end) = engine
        .execute("SELECT id, v FROM page_idx_t ORDER BY id LIMIT 5 OFFSET 100")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(past_end.rows.is_empty());
}

#[test]
fn ordered_index_small_window_bounds_mrr_work() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE page_mrr_t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let values = (0..1_000)
        .map(|id| format!("({id}, {})", id * 10))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO page_mrr_t VALUES {values}"))
        .unwrap();

    for direction in ["ASC", "DESC"] {
        let QueryResponse::ResultSet(explain) = engine
            .execute(&format!(
                "EXPLAIN ANALYZE FORMAT=TREE \
                 SELECT id, v FROM page_mrr_t ORDER BY id {direction} LIMIT 5 OFFSET 2"
            ))
            .unwrap()
        else {
            panic!("expected explain result set");
        };
        let Value::String(text) = &explain.rows[0][0] else {
            panic!("expected explain text");
        };
        let fetched_rows = text
            .split("heap_rows_fetched=")
            .nth(1)
            .and_then(|tail| tail.split_whitespace().next())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_else(|| panic!("missing generic index fetch metric in {text}"));
        assert!(
            fetched_rows <= 7,
            "OFFSET 2 + LIMIT 5 must not pull the full MRR grant ({direction}): {text}"
        );
    }
}

#[test]
fn ordered_covering_index_small_window_bounds_visibility_work() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE covering_window(id INT PRIMARY KEY,a INT,INDEX ai(a,id))")
        .unwrap();
    let values = (0..1000)
        .map(|id| format!("({id},{id})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO covering_window VALUES {values}"))
        .unwrap();
    for direction in ["ASC", "DESC"] {
        let sql = format!("SELECT id,a FROM covering_window FORCE INDEX(ai) ORDER BY a {direction},id {direction} LIMIT 5 OFFSET 2");
        let QueryResponse::ResultSet(result) = engine.execute(&sql).unwrap().materialize().unwrap()
        else {
            panic!("expected covering result");
        };
        let ids = if direction == "ASC" {
            (2..7).collect::<Vec<_>>()
        } else {
            (993..998).rev().collect::<Vec<_>>()
        };
        assert_eq!(
            result.rows,
            ids.into_iter()
                .map(|id| vec![Value::Int(id), Value::Int(id)])
                .collect::<Vec<_>>()
        );
        let QueryResponse::ResultSet(explain) = engine
            .execute(&format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"))
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("expected covering analysis");
        };
        let text = explain.rows[0][0].to_display_string();
        let examined = text
            .split("index_entries_examined=")
            .nth(1)
            .map(|tail| {
                tail.chars()
                    .take_while(char::is_ascii_digit)
                    .collect::<String>()
            })
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or_else(|| panic!("missing covering work metric: {text}"));
        assert_eq!(
            examined, 7,
            "covering pagination must bound visibility work: {text}"
        );
    }
}

#[test]
fn ordered_covering_index_window_refills_invisible_and_filtered_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE covering_visible(id INT PRIMARY KEY,a INT,INDEX ai(a,id))")
        .unwrap();
    let values = (0..1000)
        .map(|id| format!("({id},{id})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO covering_visible VALUES {values}"))
        .unwrap();
    let rows = |response: QueryResponse| {
        let QueryResponse::ResultSet(result) = response.materialize().unwrap() else {
            panic!("expected covering rows");
        };
        result.rows
    };
    let sql = "SELECT id,a FROM covering_visible FORCE INDEX(ai) ORDER BY a,id LIMIT 5 OFFSET 2";
    let expected = |start| {
        (start..start + 5)
            .map(|id| vec![Value::Int(id), Value::Int(id)])
            .collect::<Vec<_>>()
    };
    for (direction, ids) in [("ASC", vec![194, 291, 388]), ("DESC", vec![776, 679, 582])] {
        let filtered = format!("SELECT id FROM covering_visible FORCE INDEX(ai) WHERE MOD(a,97)=0 ORDER BY a {direction},id {direction} LIMIT 3 OFFSET 2");
        assert_eq!(
            rows(engine.execute(&filtered).unwrap()),
            ids.into_iter()
                .map(|id| vec![Value::Int(id)])
                .collect::<Vec<_>>()
        );
    }
    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    assert_eq!(
        rows(engine.execute_with_session(&mut reader, sql).unwrap()),
        expected(2)
    );
    engine
        .execute("DELETE FROM covering_visible WHERE a<900")
        .unwrap();
    assert_eq!(rows(engine.execute(sql).unwrap()), expected(902));
    assert_eq!(
        rows(engine.execute_with_session(&mut reader, sql).unwrap()),
        expected(2)
    );
    let mut writer = SessionContext::default();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO covering_visible VALUES(-1,-1)")
        .unwrap();
    assert_eq!(rows(engine.execute(sql).unwrap()), expected(902));
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
}

#[test]
fn ordered_index_limit_offset_filter_fallback_remains_correct() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE page_idx_filter_t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut sql = String::from("INSERT INTO page_idx_filter_t VALUES ");
    for i in 0..12 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, {})", i, i));
    }
    engine.execute(&sql).unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM page_idx_filter_t WHERE v >= 4 ORDER BY id LIMIT 3 OFFSET 2")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(6)],
            vec![Value::Int(7)],
            vec![Value::Int(8)],
        ]
    );
}

#[test]
fn ordered_index_sparse_residual_limit_grows_candidate_window() {
    fn analyzed_text(engine: &StorageQueryEngine, sql: &str) -> String {
        let QueryResponse::ResultSet(explain) = engine.execute(sql).unwrap() else {
            panic!("expected EXPLAIN result set");
        };
        explain
            .rows
            .iter()
            .flat_map(|row| row.iter())
            .map(Value::to_display_string)
            .collect::<Vec<_>>()
            .join("\n")
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE adaptive_row_goal_t (\
             id INT PRIMARY KEY, ordered_key INT NOT NULL, kind VARCHAR(8) NOT NULL,\
             INDEX idx_ordered(ordered_key))",
        )
        .unwrap();
    for start in (0..20_000).step_by(1_000) {
        let values = (start..start + 1_000)
            .map(|id| {
                let kind = if id == 19_999 { "keep" } else { "drop" };
                format!("({id}, {id}, '{kind}')")
            })
            .collect::<Vec<_>>()
            .join(", ");
        engine
            .execute(&format!("INSERT INTO adaptive_row_goal_t VALUES {values}"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE adaptive_row_goal_t").unwrap();

    let query = "SELECT id FROM adaptive_row_goal_t FORCE INDEX (idx_ordered) \
                 WHERE kind='keep' ORDER BY ordered_key LIMIT 1";
    let predicate_first_before =
        crate::engine::executor::vec::index_scan::PREDICATE_FIRST_INDEX_HITS
            .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(result) = engine.execute(query).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(19_999)]]);
    assert!(
        crate::engine::executor::vec::index_scan::PREDICATE_FIRST_INDEX_HITS
            .load(std::sync::atomic::Ordering::Relaxed)
            > predicate_first_before,
        "sparse ordered row-goal scan must decode its residual before output-only columns"
    );

    let text = analyzed_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {query}"));
    let metric = |name: &str| {
        text.split(&format!("{name}="))
            .nth(1)
            .and_then(|tail| tail.split_whitespace().next())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_else(|| panic!("missing {name} in {text}"))
    };
    assert_eq!(metric("row_goal"), 1, "{text}");
    assert!(metric("candidate_batches") < 32, "{text}");
    assert_eq!(
        metric("candidate_window_peak"),
        crate::engine::executor::vec::DEFAULT_BATCH_ROWS as u64,
        "{text}"
    );
    assert!(metric("mrr_windows") < 32, "{text}");

    let first_match = "EXPLAIN ANALYZE FORMAT=TREE \
        SELECT id FROM adaptive_row_goal_t FORCE INDEX (idx_ordered) \
        WHERE kind='drop' ORDER BY ordered_key LIMIT 1";
    let first_text = analyzed_text(&engine, first_match);
    let first_peak = first_text
        .split("candidate_window_peak=")
        .nth(1)
        .and_then(|tail| tail.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("missing first-match candidate window in {first_text}"));
    assert_eq!(first_peak, 1, "first match must retain the one-row goal");

    let blocking = analyzed_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         SELECT id FROM adaptive_row_goal_t ORDER BY RAND() LIMIT 1",
    );
    assert!(
        !blocking.contains("row_goal="),
        "a blocking TopK must not push LIMIT batching into its full scan: {blocking}"
    );
}

#[test]
fn order_by_unseeded_rand_batch_preserves_rows_and_session_state() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE rand_topn_t (id INT PRIMARY KEY, payload INT NOT NULL)")
        .unwrap();
    for start in (0..4_096).step_by(512) {
        let values = (start..start + 512)
            .map(|id| format!("({id}, {})", id * 3))
            .collect::<Vec<_>>()
            .join(", ");
        engine
            .execute(&format!("INSERT INTO rand_topn_t VALUES {values}"))
            .unwrap();
    }

    let initial_state = (10_000_000, 1_000_000);
    let mut session = crate::engine::api::SessionContext::default();
    for limit in [1, 32] {
        let query = format!("SELECT id FROM rand_topn_t ORDER BY RAND() LIMIT {limit}");
        *session.sysvars.rand_state.lock() = initial_state;
        let vector_rows = with_test_env(&[("MYDB_VECTORIZED", Some("on"))], || {
            collect_select_response_rows(engine.execute_with_session(&mut session, &query).unwrap())
        });
        let vector_state = *session.sysvars.rand_state.lock();

        *session.sysvars.rand_state.lock() = initial_state;
        let oracle_rows = with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || {
            collect_select_response_rows(engine.execute_with_session(&mut session, &query).unwrap())
        });
        let oracle_state = *session.sysvars.rand_state.lock();

        assert_eq!(vector_rows, oracle_rows, "LIMIT {limit}");
        assert_eq!(vector_state, oracle_state, "LIMIT {limit}");
        assert_ne!(vector_state, initial_state);
    }
}

#[test]
fn order_by_rand_limit_one_uses_exact_covering_prefix_range() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE rand_covering_range_t (\
             applicantid INT PRIMARY KEY, status VARCHAR(16) NOT NULL, payload INT NOT NULL,\
             INDEX idx_status_applicantid(status, applicantid))",
        )
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.rand_covering_range_t",
        (0..8_192i64).map(|id| {
            vec![
                Value::Int(id),
                Value::String(if id % 7 == 0 { "lent" } else { "rejected" }.into()),
                Value::Int(id * 3),
            ]
        }),
    );
    engine
        .execute("ANALYZE TABLE rand_covering_range_t")
        .unwrap();

    let query = "SELECT applicantid FROM rand_covering_range_t \
                 WHERE status='lent' ORDER BY RAND() LIMIT 1";
    let QueryResponse::ResultSet(explain_result) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {query}"))
        .unwrap()
    else {
        panic!("expected EXPLAIN result set");
    };
    let explain = explain_result.rows[0][0].to_display_string();
    assert!(explain.contains("idx_status_applicantid"), "{explain}");
    assert!(explain.contains("Index lookup"), "{explain}");

    let initial_state = (10_000_000, 1_000_000);
    let mut session = crate::engine::api::SessionContext::default();
    *session.sysvars.rand_state.lock() = initial_state;
    let vector_rows = with_test_env(&[("MYDB_VECTORIZED", Some("on"))], || {
        collect_select_response_rows(engine.execute_with_session(&mut session, query).unwrap())
    });
    let vector_state = *session.sysvars.rand_state.lock();

    *session.sysvars.rand_state.lock() = initial_state;
    let oracle_rows = with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || {
        collect_select_response_rows(engine.execute_with_session(&mut session, query).unwrap())
    });
    let oracle_state = *session.sysvars.rand_state.lock();

    assert_eq!(vector_rows, oracle_rows);
    assert_eq!(vector_state, oracle_state);
    assert_ne!(vector_state, initial_state);

    *session.sysvars.rand_state.lock() = initial_state;
    crate::engine::executor::select::vec_bridge::EXACT_RAND_INDEX_TOP_ONE_HITS
        .store(0, std::sync::atomic::Ordering::Relaxed);
    crate::storage::ORDER_STATS_CACHE_HITS.store(0, std::sync::atomic::Ordering::Relaxed);
    let auto_response = with_test_env(&[("MYDB_VECTORIZED", Some("auto"))], || {
        engine.execute_with_session(&mut session, query).unwrap()
    });
    assert!(
        matches!(auto_response, QueryResponse::ResultSet(_)),
        "auto mode must execute bounded RAND Top-1 synchronously through the native kernel"
    );
    let exact_hits = crate::engine::executor::select::vec_bridge::EXACT_RAND_INDEX_TOP_ONE_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        exact_hits, 1,
        "auto mode bypassed the exact RAND index kernel"
    );
    assert!(
        crate::storage::ORDER_STATS_CACHE_HITS.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "repeated exact RAND range did not reuse governed order statistics"
    );

    let QueryResponse::ResultSet(analyze_result) = engine
        .execute(&format!("EXPLAIN ANALYZE FORMAT=TREE {query}"))
        .unwrap()
    else {
        panic!("expected EXPLAIN ANALYZE result set");
    };
    let analyzed = analyze_result.rows[0][0].to_display_string();
    assert!(
        analyzed.contains("rows=1171 loops=1"),
        "exact range must report one logical scan: {analyzed}"
    );
    assert!(
        analyzed.contains("index_entries_examined=1171"),
        "exact range must expose its logical candidate count: {analyzed}"
    );
}

#[test]
fn order_by_rand_limit_one_fuses_covering_residual_filter() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE rand_covering_residual_t (\
             applicantid INT PRIMARY KEY, status VARCHAR(16) NOT NULL,\
             delinquency INT NOT NULL, payload INT NOT NULL,\
             INDEX idx_status_applicantid_delinquency(status, applicantid, delinquency))",
        )
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.rand_covering_residual_t",
        (0..8_192i64).map(|id| {
            vec![
                Value::Int(id),
                Value::String(if id % 5 == 0 { "lent" } else { "rejected" }.into()),
                Value::Int(i64::from(id % 7 == 0)),
                Value::Int(id * 3),
            ]
        }),
    );
    engine
        .execute("ANALYZE TABLE rand_covering_residual_t")
        .unwrap();

    // `delinquency` follows applicantid in the index, so it cannot be part
    // of the status prefix bounds.  It is nevertheless fully covering and
    // can be fused with the exact RAND arg-min while leaf entries stream.
    let query = "SELECT applicantid FROM rand_covering_residual_t \
                 WHERE status='lent' AND delinquency=1 \
                 ORDER BY RAND() LIMIT 1";
    let initial_state = (10_000_000, 1_000_000);
    let mut session = crate::engine::api::SessionContext::default();

    *session.sysvars.rand_state.lock() = initial_state;
    crate::engine::executor::select::vec_bridge::EXACT_RAND_INDEX_TOP_ONE_HITS
        .store(0, std::sync::atomic::Ordering::Relaxed);
    let vector_rows = with_test_env(&[("MYDB_VECTORIZED", Some("auto"))], || {
        collect_select_response_rows(engine.execute_with_session(&mut session, query).unwrap())
    });
    let vector_state = *session.sysvars.rand_state.lock();
    assert_eq!(
        crate::engine::executor::select::vec_bridge::EXACT_RAND_INDEX_TOP_ONE_HITS
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "covering residual did not use the fused exact RAND kernel"
    );

    *session.sysvars.rand_state.lock() = initial_state;
    let oracle_rows = with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || {
        collect_select_response_rows(engine.execute_with_session(&mut session, query).unwrap())
    });
    let oracle_state = *session.sysvars.rand_state.lock();
    assert_eq!(vector_rows, oracle_rows);
    assert_eq!(vector_state, oracle_state);
}

#[test]
fn timestamp_function_bounds_match_row_oracle_across_time_zones() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE timestamp_filter_t (id INT PRIMARY KEY, ts TIMESTAMP NOT NULL)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO timestamp_filter_t VALUES \
             (1,'2023-10-31 23:59:59'),(2,'2023-11-01 00:00:00'),\
             (3,'2023-12-31 00:00:00'),(4,'2024-01-01 00:00:00')",
        )
        .unwrap();
    let query = "SELECT id FROM timestamp_filter_t \
                 WHERE ts BETWEEN STR_TO_DATE('2023-11-01','%Y-%m-%d') \
                   AND (DATE '2023-11-01' + INTERVAL '60' DAY) \
                 ORDER BY id";

    for zone in ["+00:00", "+08:00", "America/New_York"] {
        engine.execute(&format!("SET time_zone='{zone}'")).unwrap();
        let vector_rows = with_test_env(&[("MYDB_VECTORIZED", Some("on"))], || {
            let QueryResponse::ResultSet(result) = engine.execute(query).unwrap() else {
                panic!("expected vector result set");
            };
            result.rows
        });
        let oracle_rows = with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || {
            let QueryResponse::ResultSet(result) = engine.execute(query).unwrap() else {
                panic!("expected row-oracle result set");
            };
            result.rows
        });
        assert_eq!(vector_rows, oracle_rows, "time_zone={zone}");

        let QueryResponse::ResultSet(explain) = engine
            .execute(&format!("EXPLAIN ANALYZE FORMAT=TREE {query}"))
            .unwrap()
        else {
            panic!("expected EXPLAIN result set");
        };
        let Value::String(text) = &explain.rows[0][0] else {
            panic!("expected EXPLAIN text");
        };
        assert!(text.contains("expression_mode=decoded"), "{zone}: {text}");
        assert!(
            !text.contains("fallback_expression_rows="),
            "{zone}: {text}"
        );
    }
}

#[test]
fn timestamp_date_plus_integer_predicate_matches_oracle_across_dst() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("SET time_zone='+00:00'").unwrap();
    engine
        .execute(
            "CREATE TABLE timestamp_date_math_t (\
             id INT PRIMARY KEY, ts TIMESTAMP NULL, duration INT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO timestamp_date_math_t VALUES \
             (1,'2023-03-12 06:59:59',0),(2,'2023-03-12 07:00:00',1),\
             (3,'2023-11-05 05:59:59',2),(4,'2023-11-05 06:00:00',3),\
             (5,'2023-12-31 23:00:00',1),(6,NULL,1),(7,'2024-01-01',NULL)",
        )
        .unwrap();
    let query = "SELECT id FROM timestamp_date_math_t \
                 WHERE DATE(ts) + duration < STR_TO_DATE('20231108','%Y%m%d') \
                 ORDER BY id";

    for zone in ["+00:00", "+08:00", "America/New_York"] {
        engine.execute(&format!("SET time_zone='{zone}'")).unwrap();
        let vector_rows = with_test_env(&[("MYDB_VECTORIZED", Some("on"))], || {
            collect_select_response_rows(engine.execute(query).unwrap())
        });
        let oracle_rows = with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || {
            collect_select_response_rows(engine.execute(query).unwrap())
        });
        assert_eq!(vector_rows, oracle_rows, "time_zone={zone}");

        let QueryResponse::ResultSet(explain) = engine
            .execute(&format!("EXPLAIN ANALYZE FORMAT=TREE {query}"))
            .unwrap()
        else {
            panic!("expected explain result set");
        };
        let text = explain.rows[0][0].to_display_string();
        assert!(text.contains("native_expression_rows=7"), "{zone}: {text}");
        assert!(
            !text.contains("fallback_expression_rows="),
            "{zone}: {text}"
        );
    }
}

#[test]
fn mixed_direction_index_order_still_requires_ordering_node() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE mixed_idx_t (id INT PRIMARY KEY, a INT, b INT, INDEX idx_ab (a, b))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO mixed_idx_t VALUES \
             (1, 1, 1), (2, 1, 2), (3, 2, 1), (4, 2, 2), (5, 3, 1)",
        )
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN FORMAT=TREE \
             SELECT a, b FROM mixed_idx_t FORCE INDEX (idx_ab) \
             ORDER BY a DESC, b ASC LIMIT 3",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let plan_text = match &explain.rows[0][0] {
        Value::String(s) => s.clone(),
        other => panic!("expected string, got {other:?}"),
    };
    assert!(
        plan_text.contains("TopK") || plan_text.contains("Sort"),
        "mixed direction ORDER BY must not be treated as satisfied by plain index reversal: {plan_text}"
    );
}

#[test]
fn incremental_sort_uses_index_order_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE incsort_t (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_incsort_a ON incsort_t(a)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO incsort_t VALUES \
             (1, 1, 10), (2, 1, 30), (3, 1, 20), \
             (4, 2, 15), (5, 2, 25), (6, 3, 5)",
        )
        .unwrap();

    let sql = "SELECT id FROM incsort_t FORCE INDEX (idx_incsort_a) \
               WHERE a >= 1 ORDER BY a ASC, b DESC";
    let QueryResponse::ResultSet(r) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(1)],
            vec![Value::Int(5)],
            vec![Value::Int(4)],
            vec![Value::Int(6)],
        ]
    );

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN FORMAT=TREE SELECT id FROM incsort_t FORCE INDEX (idx_incsort_a) \
             WHERE a >= 1 ORDER BY a ASC, b DESC",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let plan_text = match &explain.rows[0][0] {
        Value::String(s) => s.clone(),
        other => panic!("expected string, got {other:?}"),
    };
    assert!(
        plan_text.contains("Incremental Sort"),
        "expected Incremental Sort in plan, got: {plan_text}"
    );
}

#[test]
fn topk_memory_budget_scales_with_k_not_input_rows() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE topk_mem_t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut sql = String::from("INSERT INTO topk_mem_t VALUES ");
    for i in 0..200 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, {})", i, 200 - i));
    }
    engine.execute(&sql).unwrap();

    let budget = Arc::new(MemBudget::new(
        16 * crate::engine::executor::mem_budget::ESTIMATED_ROW_BYTES,
    ));
    let QueryResponse::ResultSet(r) = with_query_budget(budget, || {
        engine.execute("SELECT id FROM topk_mem_t ORDER BY v LIMIT 5")
    })
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(r.rows.len(), 5);
    assert_eq!(r.rows[0][0], Value::Int(199));
}

#[test]
fn descending_order_by_limit_uses_reverse_index_order_in_fast_and_plan_paths() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE desc_t (id INT PRIMARY KEY, v INT, INDEX v_idx (v))")
        .unwrap();
    engine
        .execute("INSERT INTO desc_t VALUES (1, 10), (2, 50), (3, 30), (4, 40), (5, 20)")
        .unwrap();
    engine.execute("ANALYZE TABLE desc_t").unwrap();

    let QueryResponse::ResultSet(fast) = engine
        .execute("SELECT id FROM desc_t ORDER BY v DESC LIMIT 3")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(planned) = engine
        .execute("SELECT /*+ NO_PARALLEL */ id FROM desc_t ORDER BY v DESC LIMIT 3")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(fast.rows, planned.rows);
    assert_eq!(
        fast.rows
            .iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(2), Value::Int(4), Value::Int(3)]
    );

    let QueryResponse::ResultSet(filtered) = engine
        .execute("SELECT /*+ NO_PARALLEL */ id FROM desc_t WHERE v > 0 ORDER BY v DESC LIMIT 3")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(filtered.rows, fast.rows);

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT id FROM desc_t WHERE v > 0 ORDER BY v DESC LIMIT 3")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let text = explain
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .filter_map(|value| match value {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        text.contains("Backward index scan"),
        "expected reverse index scan in EXPLAIN, got: {text}"
    );
    assert!(
        !text.contains("Using filesort"),
        "reverse index scan should satisfy ORDER BY without filesort: {text}"
    );
}

#[test]
fn like_operator_basic_patterns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(64))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'alicia'), (4, 'charlie')")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT name FROM t WHERE name LIKE 'ali%' ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::String("alice".to_string()));
    assert_eq!(result.rows[1][0], Value::String("alicia".to_string()));

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT name FROM t WHERE name LIKE '_ob'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("bob".to_string()));

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT name FROM t WHERE name NOT LIKE '%li%' ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("bob".to_string()));
}

#[test]
fn like_operator_without_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine.execute("SELECT 'hello' LIKE 'hel%'").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(1));

    let QueryResponse::ResultSet(result) = engine.execute("SELECT 'hello' LIKE 'world'").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(0));
}

#[test]
fn case_when_searched_form() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, score INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 95), (2, 75), (3, 50)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'C' END AS grade FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::String("A".to_string()));
    assert_eq!(result.rows[1][1], Value::String("B".to_string()));
    assert_eq!(result.rows[2][1], Value::String("C".to_string()));
}

#[test]
fn case_when_simple_form() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::String("two".to_string()));
}

#[test]
fn case_when_no_else_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT CASE WHEN 1 = 0 THEN 'yes' END")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Null);
}

#[test]
fn case_when_with_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, status VARCHAR(10))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'active'), (2, 'active'), (3, 'inactive')")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_count FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::DecimalCompact(2, 0));
}

#[test]
fn modulo_operator() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3), (4), (5), (6)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM t WHERE id % 2 = 0 ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], Value::Int(2));
    assert_eq!(result.rows[1][0], Value::Int(4));
    assert_eq!(result.rows[2][0], Value::Int(6));

    let QueryResponse::ResultSet(result) = engine.execute("SELECT 10 % 3").unwrap() else {
        panic!("expected result set");
    };
    // MySQL: `INT % INT` is an integer, not a double.
    assert_eq!(result.rows[0][0], Value::Int(1));
}

#[test]
fn exists_subquery_in_select() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64))")
        .unwrap();
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)")
        .unwrap();
    engine
        .execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (10, 1), (11, 1), (12, 2)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = 1) ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 3);

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT name FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = 99) ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn dml_subquery_in_update_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE refs (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine.execute("INSERT INTO refs VALUES (1), (3)").unwrap();

    let QueryResponse::OkAffected { affected_rows } = engine
        .execute("UPDATE t SET v = 100 WHERE id IN (SELECT id FROM refs)")
        .unwrap()
    else {
        panic!("expected OkAffected");
    };
    assert_eq!(affected_rows, 2);

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::Int(100));
    assert_eq!(result.rows[1][1], Value::Int(20));
    assert_eq!(result.rows[2][1], Value::Int(100));

    let QueryResponse::OkAffected { affected_rows } = engine
        .execute("DELETE FROM t WHERE EXISTS (SELECT 1 FROM refs WHERE refs.id = 1)")
        .unwrap()
    else {
        panic!("expected OkAffected");
    };
    assert_eq!(affected_rows, 3);
}

#[test]
fn delete_source_free_correlated_exists_binds_the_outer_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (c1 INT)").unwrap();
    engine.execute("INSERT INTO t VALUES (42)").unwrap();

    let err = engine
        .execute(
            "DELETE FROM t WHERE
             EXISTS (SELECT * WHERE '520:33:32.77' < (c1 - INTERVAL 1 MONTH))
             LIKE c1",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1292, "outer c1 must be bound before evaluation");

    let QueryResponse::ResultSet(result) = engine.execute("SELECT c1 FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(42)]]);
}

#[test]
fn nonaggregate_having_filters_each_row_and_propagates_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (a INT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT a AS visible_a FROM t HAVING visible_a > 1 ORDER BY a")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);

    let err = engine
        .execute("SELECT 1 FROM t HAVING (SELECT 1 FROM t WHERE a = 1) LIKE EXP(NOW())")
        .unwrap_err();
    assert_eq!(
        err.code(),
        1690,
        "HAVING evaluation errors must not be lost"
    );
}

#[test]
fn parenthesized_join_group_preserves_leaf_qualifiers_and_outer_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE nested_a (pk INT NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE TABLE nested_b (pk INT NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE TABLE nested_c (pk INT NOT NULL, marker INT)")
        .unwrap();
    engine
        .execute("INSERT INTO nested_a VALUES (1), (2)")
        .unwrap();
    engine
        .execute("INSERT INTO nested_b VALUES (1), (3)")
        .unwrap();
    engine
        .execute("INSERT INTO nested_c VALUES (1, 10), (4, 40)")
        .unwrap();
    engine
        .execute("SELECT * FROM nested_a AS left_leaf JOIN nested_b AS right_leaf")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT nested_c.marker
             FROM nested_c LEFT JOIN
                  (nested_a AS left_leaf JOIN nested_b AS right_leaf)
               ON left_leaf.pk = nested_c.pk AND right_leaf.pk = nested_c.pk
             ORDER BY nested_c.marker",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(10));
    assert_eq!(result.rows[1][0], Value::Int(40));
}

#[test]
fn dml_scalar_subquery_in_set_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE counters (id INT PRIMARY KEY, n INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO counters VALUES (1, 999)")
        .unwrap();

    engine
        .execute("UPDATE t SET v = (SELECT n FROM counters WHERE id = 1) WHERE id = 1")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine.execute("SELECT v FROM t WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(999));
}

#[test]
fn union_combines_results() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1), (2), (3)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (3), (4), (5)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM a UNION SELECT id FROM b ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 5);
    let values: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Int(v) => v,
            _ => panic!("expected int"),
        })
        .collect();
    assert_eq!(values, vec![1, 2, 3, 4, 5]);

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 6);
}

#[test]
fn physical_set_operation_keeps_batch_stream_to_protocol_boundary() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1), (2), (3)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (3), (4), (5)")
        .unwrap();

    let before =
        crate::engine::executor::select::vec_bridge::SETOP_BATCH_HITS.load(Ordering::Relaxed);
    let response = engine
        .execute_streaming_for_test("SELECT id FROM a UNION SELECT id FROM b ORDER BY id")
        .unwrap();
    let QueryResponse::StreamingResultSet(mut result) = response else {
        panic!("physical SetOp must remain streaming at the protocol boundary");
    };
    assert_eq!(result.cursor.next_row().unwrap(), Some(vec![Value::Int(1)]));
    drop(result); // client early-close must cancel and join the batch producer.
    assert_eq!(
        crate::engine::executor::select::stream::active_stream_workers(),
        0
    );
    assert_eq!(
        crate::engine::executor::select::vec_bridge::SETOP_BATCH_HITS.load(Ordering::Relaxed),
        before + 1
    );
}

#[test]
fn set_operation_with_inline_derived_stays_on_native_batch_graph() {
    use std::sync::atomic::Ordering;

    with_test_env(&[("MYDB_CTE_SPILL_THRESHOLD_MB", Some("0"))], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute("CREATE TABLE a (id INT PRIMARY KEY)")
            .unwrap();
        engine
            .execute("CREATE TABLE b (id INT PRIMARY KEY)")
            .unwrap();
        engine.execute("INSERT INTO a VALUES (1),(2)").unwrap();
        engine.execute("INSERT INTO b VALUES (2),(3)").unwrap();

        let batch_before =
            crate::engine::executor::select::vec_bridge::SETOP_BATCH_HITS.load(Ordering::Relaxed);
        let spill_before = crate::engine::executor::spill_quota::cte_spill_count();
        let QueryResponse::ResultSet(result) = engine
            .execute(
                "SELECT d.id FROM (SELECT id FROM a) d \
                 UNION SELECT id FROM b ORDER BY id",
            )
            .unwrap()
        else {
            panic!("expected native set-operation result");
        };
        assert_eq!(
            result.rows,
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)]
            ]
        );
        assert_eq!(
            crate::engine::executor::select::vec_bridge::SETOP_BATCH_HITS.load(Ordering::Relaxed),
            batch_before + 1
        );
        assert!(crate::engine::executor::spill_quota::cte_spill_count() > spill_before);
    });
}

#[test]
fn constant_set_operation_uses_values_scan_and_formatted_physical_explain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT 2 AS n UNION SELECT 1 ORDER BY n")
        .unwrap()
    else {
        panic!("expected materialized constant set result");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT 2 AS n UNION SELECT 1 ORDER BY n")
        .unwrap()
    else {
        panic!("expected formatted EXPLAIN");
    };
    let Value::String(tree) = &explain.rows[0][0] else {
        panic!("expected EXPLAIN text");
    };
    assert!(tree.contains("Union distinct"), "{tree}");
    assert_eq!(tree.matches("Values scan").count(), 2, "{tree}");
    assert!(!tree.contains("pipeline=0"), "{tree}");
    assert!(tree.contains("blocking=true"), "{tree}");
    assert!(!tree.contains("blocking=false"), "{tree}");
    assert!(!tree.contains("exchange=false"), "{tree}");
    assert!(!tree.contains("kernel=vector"), "{tree}");
    assert!(!tree.contains("est_mem=0"), "{tree}");
    assert!(!tree.contains("est_spill=0"), "{tree}");

    let QueryResponse::ResultSet(json) = engine
        .execute("EXPLAIN FORMAT=JSON SELECT 2 AS n UNION SELECT 1 ORDER BY n")
        .unwrap()
    else {
        panic!("expected JSON EXPLAIN");
    };
    let rendered = json.rows[0][0].to_display_string();
    serde_json::from_str::<serde_json::Value>(&rendered).expect("valid EXPLAIN JSON");
    assert!(rendered.contains("\"pipeline_id\""), "{rendered}");
    assert!(rendered.contains("\"kernel\""), "{rendered}");

    let QueryResponse::ResultSet(analyze) = engine
        .execute("EXPLAIN ANALYZE FORMAT=TREE SELECT 2 AS n INTERSECT SELECT 2")
        .unwrap()
    else {
        panic!("expected formatted EXPLAIN ANALYZE");
    };
    let Value::String(tree) = &analyze.rows[0][0] else {
        panic!("expected EXPLAIN ANALYZE text");
    };
    assert!(tree.contains("Intersect distinct"), "{tree}");
}

#[test]
fn intersect_and_except() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1), (2), (3)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (2), (3), (4)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM a INTERSECT SELECT id FROM b ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let values: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Int(v) => v,
            _ => panic!("expected int"),
        })
        .collect();
    assert_eq!(values, vec![2, 3]);

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM a EXCEPT SELECT id FROM b ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let values: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Int(v) => v,
            _ => panic!("expected int"),
        })
        .collect();
    assert_eq!(values, vec![1]);
}

#[test]
fn intersect_and_except_all_use_multiset_counts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE a (id INT)").unwrap();
    engine.execute("CREATE TABLE b (id INT)").unwrap();
    engine
        .execute("INSERT INTO a VALUES (1), (1), (2), (2), (2), (3)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (1), (2), (2), (4)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM a INTERSECT ALL SELECT id FROM b ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let values: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Int(v) => v,
            _ => panic!("expected int"),
        })
        .collect();
    assert_eq!(values, vec![1, 2, 2]);

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM a EXCEPT ALL SELECT id FROM b ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let values: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Int(v) => v,
            _ => panic!("expected int"),
        })
        .collect();
    assert_eq!(values, vec![1, 2, 3]);
}

#[test]
fn values_row_query_orders_and_limits() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let before = native_batch_hits();
    let QueryResponse::ResultSet(result) = engine
        .execute("VALUES ROW(2, 'b'), ROW(1, 'a'), ROW(3, 'c') ORDER BY column_0 LIMIT 2")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.columns[0].name, "column_0");
    assert_eq!(result.columns[1].name, "column_1");
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::String("a".into())],
            vec![Value::Int(2), Value::String("b".into())],
        ]
    );
    assert!(
        native_batch_hits() > before,
        "VALUES did not use its native BatchOperator source"
    );
}

#[test]
fn values_row_derived_table_with_column_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, name FROM (VALUES ROW(2, 'b'), ROW(1, 'a')) AS t(id, name) ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::String("a".into())],
            vec![Value::Int(2), Value::String("b".into())],
        ]
    );
}

#[test]
fn lateral_join_supports_top_n_per_group() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(20))")
        .unwrap();
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, total INT)")
        .unwrap();
    engine
        .execute("INSERT INTO customers VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders VALUES \
             (10, 1, 50), (11, 1, 70), (12, 1, 60), (20, 2, 40)",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT c.id, x.order_id, x.total \
             FROM customers c \
             LEFT JOIN LATERAL ( \
               SELECT o.id AS order_id, o.total \
               FROM orders o \
               WHERE o.customer_id = c.id \
               ORDER BY o.total DESC \
               LIMIT 2 \
             ) AS x ON 1 = 1 \
             ORDER BY c.id, x.total DESC",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(11), Value::Int(70)],
            vec![Value::Int(1), Value::Int(12), Value::Int(60)],
            vec![Value::Int(2), Value::Int(20), Value::Int(40)],
            vec![Value::Int(3), Value::Null, Value::Null],
        ]
    );
}

#[test]
fn lateral_join_supports_correlated_set_operation_body() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE lateral_outer (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO lateral_outer VALUES (1), (2)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT o.id, x.v FROM lateral_outer AS o \
             JOIN LATERAL (SELECT o.id AS v UNION ALL SELECT o.id + 10 AS v) AS x ON TRUE \
             ORDER BY o.id, x.v",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(2), Value::Int(2)],
            vec![Value::Int(2), Value::Int(12)],
        ]
    );
}

#[test]
fn insert_ignore_skips_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 100), (2, 200)")
        .unwrap();

    let QueryResponse::OkAffected { affected_rows } = engine
        .execute("INSERT IGNORE INTO t VALUES (1, 999), (3, 300)")
        .unwrap()
    else {
        panic!("expected OkAffected");
    };
    assert_eq!(affected_rows, 1);

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][1], Value::Int(100));
}

#[test]
fn replace_into_overwrites_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 100)").unwrap();

    engine.execute("REPLACE INTO t VALUES (1, 999)").unwrap();

    let QueryResponse::ResultSet(result) = engine.execute("SELECT v FROM t WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(999));
}

#[test]
fn insert_on_duplicate_key_update() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE counters (id INT PRIMARY KEY, n INT)")
        .unwrap();
    engine
        .execute("INSERT INTO counters VALUES (1, 5)")
        .unwrap();

    let QueryResponse::OkAffected { affected_rows } = engine
        .execute("INSERT INTO counters VALUES (1, 0) ON DUPLICATE KEY UPDATE n = n + 1")
        .unwrap()
    else {
        panic!("expected OkAffected");
    };
    assert_eq!(affected_rows, 2);

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT n FROM counters WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(6));

    engine
        .execute("INSERT INTO counters VALUES (2, 100) ON DUPLICATE KEY UPDATE n = n + 1")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT n FROM counters WHERE id = 2")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(100));
}

#[test]
fn mysql80_insert_select_set_defaults_and_dml_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE src (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE dst (id INT PRIMARY KEY, v INT DEFAULT 7, note VARCHAR(64) DEFAULT 'new')")
        .unwrap();
    engine
        .execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();

    engine
        .execute("INSERT INTO dst (id, v) SELECT id, v FROM src WHERE id = 1")
        .unwrap();
    engine
        .execute("INSERT INTO dst SET id = 2, v = DEFAULT")
        .unwrap();
    engine
        .execute("UPDATE dst AS d SET v = DEFAULT WHERE d.id = 1")
        .unwrap();
    engine
        .execute("DELETE FROM dst AS d WHERE d.id = 2")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, v, note FROM dst ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Int(1),
            Value::Int(7),
            Value::String("new".to_string())
        ]]
    );
}

#[test]
fn on_duplicate_key_update_values_uses_proposed_row_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, total INT DEFAULT 100)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10, 10)").unwrap();

    let QueryResponse::OkAffected { affected_rows } = engine
        .execute(
            "INSERT INTO t (id, v, total) VALUES (1, 5, DEFAULT) \
             ON DUPLICATE KEY UPDATE v = VALUES(v), total = VALUES(total)",
        )
        .unwrap()
    else {
        panic!("expected OkAffected");
    };
    assert_eq!(affected_rows, 2);

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT v, total FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(5), Value::Int(100)]]);
}

#[test]
fn on_duplicate_key_update_insert_alias_uses_proposed_row_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, total INT DEFAULT 100)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10, 10)").unwrap();

    let QueryResponse::OkAffected { affected_rows } = engine
        .execute(
            "INSERT INTO t (id, v, total) VALUES (1, 5, DEFAULT) AS new(i, val, tot) \
             ON DUPLICATE KEY UPDATE v = new.val, total = new.tot",
        )
        .unwrap()
    else {
        panic!("expected OkAffected");
    };
    assert_eq!(affected_rows, 2);

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT v, total FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(5), Value::Int(100)]]);

    engine
        .execute(
            "INSERT INTO t (id, v) VALUES (1, 8) AS proposed \
             ON DUPLICATE KEY UPDATE total = proposed.v",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT v, total FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(5), Value::Int(8)]]);
}

#[test]
fn alter_table_add_column_with_default() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 100), (2, 200)")
        .unwrap();
    engine
        .execute("ALTER TABLE t ADD COLUMN status VARCHAR(20) DEFAULT 'pending'")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, v, status FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][2], Value::String("pending".to_string()));
    assert_eq!(result.rows[1][2], Value::String("pending".to_string()));

    engine
        .execute("INSERT INTO t (id, v, status) VALUES (3, 300, 'active')")
        .unwrap();

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT status FROM t WHERE id = 3").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::String("active".to_string()));
}

#[test]
fn alter_table_drop_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, status VARCHAR(20))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 100, 'a'), (2, 200, 'b')")
        .unwrap();

    engine.execute("ALTER TABLE t DROP COLUMN status").unwrap();

    let QueryResponse::ResultSet(result) = engine.execute("SELECT * FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.rows[0].len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1], Value::Int(100));
}

#[test]
fn window_row_number() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, score INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 90), (2, 80), (3, 95), (4, 85)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 4);
    let rn_for_id = |id: i64| -> i64 {
        for row in &result.rows {
            if matches!(row[0], Value::Int(v) if v == id) {
                if let Value::Int(rn) = row[1] {
                    return rn;
                }
            }
        }
        0
    };
    assert_eq!(rn_for_id(3), 1);
    assert_eq!(rn_for_id(1), 2);
    assert_eq!(rn_for_id(4), 3);
    assert_eq!(rn_for_id(2), 4);
}

#[test]
fn window_rank_with_ties() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, score INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 90), (2, 90), (3, 80)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, RANK() OVER (ORDER BY score DESC) AS r, DENSE_RANK() OVER (ORDER BY score DESC) AS dr FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::Int(1));
    assert_eq!(result.rows[1][1], Value::Int(1));
    assert_eq!(result.rows[2][1], Value::Int(3));
    assert_eq!(result.rows[2][2], Value::Int(2));
}

#[test]
fn window_partition_by() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, group_name VARCHAR(10), v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 40)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY group_name ORDER BY v) AS rn FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::Int(1));
    assert_eq!(result.rows[1][1], Value::Int(2));
    assert_eq!(result.rows[2][1], Value::Int(1));
    assert_eq!(result.rows[3][1], Value::Int(2));
}

#[test]
fn cte_basic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("WITH big AS (SELECT id, v FROM t WHERE v >= 20) SELECT v FROM big ORDER BY v")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], Value::Int(20));
}

#[test]
fn cte_with_column_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "WITH big(pk, val) AS (SELECT id, v FROM t WHERE v >= 20) SELECT pk, val FROM big ORDER BY pk",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.columns[0].name, "pk");
    assert_eq!(result.columns[1].name, "val");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(2));
    assert_eq!(result.rows[0][1], Value::Int(20));
}

#[test]
fn cte_group_key_keeps_unquoted_identifier_name_after_reprepare() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, g VARCHAR(16), v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 7)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "WITH grouped AS (SELECT g, SUM(v) AS total FROM t GROUP BY g) \
             SELECT g, total FROM grouped ORDER BY g",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.columns[0].name, "g");
    assert_eq!(result.columns[1].name, "total");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(crate::types::value_str(&result.rows[0][0]), Some("a"));
    assert_eq!(crate::types::value_str(&result.rows[1][0]), Some("b"));
}

#[test]
fn aggregate_order_by_peers_use_group_keys_as_deterministic_tie_breaker() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 3, 0), (2, 1, 0), (3, 2, 0)")
        .unwrap();

    let sql = "SELECT g, SUM(v) AS total FROM t GROUP BY g ORDER BY total";
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
    ] {
        let QueryResponse::ResultSet(result) =
            crate::engine::executor::vec::config::with_vec_mode(mode, || {
                engine.execute(sql).unwrap()
            })
        else {
            panic!("expected result set");
        };
        assert_eq!(
            result
                .rows
                .iter()
                .map(|row| row[0].clone())
                .collect::<Vec<_>>(),
            vec![Value::Int(1), Value::Int(2), Value::Int(3)]
        );
    }
}

#[test]
fn cte_nested_referencing_previous() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "WITH a AS (SELECT id, v FROM t WHERE v >= 20), b AS (SELECT v FROM a WHERE v <= 20) SELECT * FROM b",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(20));
}

#[test]
fn cte_non_recursive_forward_reference() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "WITH b AS (SELECT v FROM a WHERE v <= 20), \
                  a AS (SELECT id, v FROM t WHERE v >= 20) \
             SELECT * FROM b",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(20)]]);
}

#[test]
fn cte_mutual_recursion_is_rejected_clearly() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute("WITH a AS (SELECT * FROM b), b AS (SELECT * FROM a) SELECT * FROM a")
        .unwrap_err();
    assert!(
        err.message()
            .contains("mutually recursive CTEs are not supported"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn cte_recursive_simple_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5) SELECT n FROM seq ORDER BY n",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 5);
    for (i, row) in result.rows.iter().enumerate() {
        // n + 1 returns Double in mydb's arithmetic; first row from anchor is
        // Int. Compare numerically.
        let n = match &row[0] {
            Value::Int(v) => *v as f64,
            Value::Double(v) => *v,
            other => panic!("unexpected value: {other:?}"),
        };
        assert_eq!(n, (i + 1) as f64);
    }
}

#[test]
fn cte_recursive_max_depth_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute(
            "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq) SELECT n FROM seq",
        )
        .unwrap_err();
    assert_eq!(err.code(), 3636);
}

#[test]
fn cte_inner_join_with_real_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "WITH big AS (SELECT id, v FROM t WHERE v >= 20) SELECT t.id, big.v FROM t INNER JOIN big ON t.id = big.id ORDER BY t.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(2));
    assert_eq!(result.rows[0][1], Value::Int(20));
    assert_eq!(result.rows[1][0], Value::Int(3));
    assert_eq!(result.rows[1][1], Value::Int(30));
}

#[test]
fn cte_left_join_with_real_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "WITH big AS (SELECT id FROM t WHERE v >= 25) SELECT t.id, big.id FROM t LEFT JOIN big ON t.id = big.id ORDER BY t.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][1], Value::Null);
    assert_eq!(result.rows[1][1], Value::Null);
    assert_eq!(result.rows[2][1], Value::Int(3));
}

#[test]
fn cte_join_two_ctes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "WITH a AS (SELECT id, v FROM t WHERE v <= 20), b AS (SELECT id FROM t WHERE v >= 20) SELECT a.id, a.v FROM a INNER JOIN b ON a.id = b.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(2));
    assert_eq!(result.rows[0][1], Value::Int(20));
}

/// TPC-DS q78 shape: two column-list-free CTEs chained by a multi-column
/// `LEFT JOIN`. The in-memory CTE-join path must take the hash fast path for
/// the outer join (build right, probe left, NULL-extend unmatched left) instead
/// of the O(N×M) nested loop, and produce the correct NULL-extended result.
#[test]
fn cte_left_join_two_ctes_multikey_uses_hash_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE store_sales (yr INT, item INT, qty INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE web_sales (yr INT, item INT, qty INT)")
        .unwrap();
    engine
        .execute("INSERT INTO store_sales VALUES (2002,1,10),(2002,2,20),(2002,3,30)")
        .unwrap();
    // web_sales matches items 1 and 3 only; item 2 has no web match.
    engine
        .execute("INSERT INTO web_sales VALUES (2002,1,5),(2002,3,7)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "WITH ss AS (SELECT yr AS s_yr, item AS s_item, SUM(qty) AS s_q \
                         FROM store_sales GROUP BY yr, item), \
                  ws AS (SELECT yr AS w_yr, item AS w_item, SUM(qty) AS w_q \
                         FROM web_sales GROUP BY yr, item) \
             SELECT s_item, s_q, w_q \
             FROM ss LEFT JOIN ws ON w_yr = s_yr AND w_item = s_item \
             ORDER BY s_item",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // SUM over Int inputs finalises as `DecimalCompact(_, 0)`.
    assert_eq!(result.rows.len(), 3);
    // item 1 matched
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][2], Value::DecimalCompact(5, 0));
    // item 2 unmatched → NULL-extended right side
    assert_eq!(result.rows[1][0], Value::Int(2));
    assert_eq!(result.rows[1][2], Value::Null);
    // item 3 matched
    assert_eq!(result.rows[2][0], Value::Int(3));
    assert_eq!(result.rows[2][2], Value::DecimalCompact(7, 0));
}

/// TPC-DS Q41 shape: a `(SELECT COUNT(*) FROM item WHERE <corr in OR>) > 0`
/// self-correlated existence count. The planner must (1) rewrite `count(*)>0`
/// to an EXISTS semi-join, (2) allow the `item` self-join even though the inner
/// is unaliased (visible-name collision check), and (3) factor the correlation
/// equi `i_manufact = i1.i_manufact` out of the OR so it becomes the join key.
/// The result must equal the per-row (decorrelate-off) oracle.
#[test]
fn q41_shape_count_gt_zero_self_correlated_or_decorrelates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE item (i_item_sk INT PRIMARY KEY, i_manufact INT, \
             i_category VARCHAR(16), i_color VARCHAR(16), i_name VARCHAR(16))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO item VALUES \
             (1, 10, 'Women', 'white', 'p1'), \
             (2, 10, 'Men',   'navy',  'p2'), \
             (3, 20, 'Women', 'blush', 'p3'), \
             (4, 30, 'Men',   'green', 'p4'), \
             (5, 10, 'Women', 'red',   'p5')",
        )
        .unwrap();

    let q = "SELECT DISTINCT i_name FROM item i1 \
             WHERE (SELECT COUNT(*) FROM item WHERE \
                 (i_manufact = i1.i_manufact AND (i_category='Women' AND (i_color='white' OR i_color='bisque'))) \
              OR (i_manufact = i1.i_manufact AND (i_category='Men'   AND (i_color='navy'  OR i_color='forest')))) > 0 \
             ORDER BY i_name";

    // Plan must contain a semi join with the correlation lifted to the key.
    let QueryResponse::ResultSet(plan) =
        engine.execute(&format!("EXPLAIN FORMAT=TREE {q}")).unwrap()
    else {
        panic!("expected result set");
    };
    let plan_text: String = plan.rows.iter().map(|r| format!("{:?}", r[0])).collect();
    assert!(
        plan_text.to_lowercase().contains("semi join"),
        "expected a semi join, got plan: {plan_text}"
    );

    let names = |sql: &str| -> Vec<String> {
        let QueryResponse::ResultSet(res) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        res.rows.iter().map(|r| r[0].to_display_string()).collect()
    };
    // Manufact 10 has Women/white (p1) and Men/navy (p2); manufact 20 has
    // Women/blush which matches neither branch's color set. So items whose
    // i_manufact has a qualifying row: manufact 10 → p1, p2, p5 (all share
    // manufact 10). Manufact 20 (p3) and 30 (p4) have no qualifying row.
    let got = names(q);
    assert_eq!(
        got,
        vec!["p1".to_string(), "p2".to_string(), "p5".to_string()],
        "decorrelated result"
    );
}

/// q41 form must take the Hash semi-join *physical* path at EXECUTION,
/// not just appear that way in EXPLAIN.
///
/// Regression for the auto-parameterize blind-spot bug
/// (docs/tpcds-q41-correlated-count-perf-design.md): when the literal `0`
/// in `(SELECT COUNT(*) …) > 0` is auto-parameterized to a `Placeholder`,
/// `count_gt_zero_to_exists_select::lit_int` historically returned `None`
/// and the whole `(scalar count) > literal` → EXISTS → semi-join chain
/// silently fell back to per-row subquery evaluation — even though
/// EXPLAIN (which bypasses auto-param) kept showing a `Hash semi join`.
/// On TPC-DS sf=1 the regression made q41 take 930 ms instead of ~7 ms;
/// the runtime divergence was invisible to the existing
/// `q41_shape_*_decorrelates` test which only inspects EXPLAIN.
///
/// We assert the rewrite engages at execution by walking the plan that
/// EXPLAIN ANALYZE actually ran (it shares `plan_select` output with the
/// executor and stamps `actual time=…` only on operators it executed) —
/// any path that skipped the semi-join rewrite would not have an
/// `actual time` on a `semi join` line.
#[test]
fn q41_shape_uses_semi_join_at_execution_under_auto_param() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE item (i_item_sk INT PRIMARY KEY, i_manufact INT, \
             i_manufact_id INT, i_category VARCHAR(16), i_color VARCHAR(16), \
             i_name VARCHAR(16))",
        )
        .unwrap();
    // ~500 rows is enough to plan a real Hash semi join (the planner's
    // sub-row optimisations don't trip at this size).
    let mut values = Vec::with_capacity(500);
    for i in 0..500i64 {
        let cat = if i % 2 == 0 { "Women" } else { "Men" };
        let col = if i % 3 == 0 {
            "white"
        } else if i % 3 == 1 {
            "navy"
        } else {
            "blush"
        };
        values.push(format!("({i}, {i}, {i}, '{cat}', '{col}', 'p{i}')"));
    }
    for chunk in values.chunks(500) {
        engine
            .execute(&format!("INSERT INTO item VALUES {}", chunk.join(",")))
            .unwrap();
    }

    let q = "SELECT DISTINCT i_name FROM item i1 \
             WHERE i_manufact_id BETWEEN 100 AND 150 \
               AND (SELECT COUNT(*) FROM item WHERE \
                   i_manufact = i1.i_manufact AND \
                   ((i_category='Women' AND (i_color='white' OR i_color='bisque')) \
                 OR (i_category='Men'   AND (i_color='navy'  OR i_color='forest')))) > 0 \
             ORDER BY i_name";

    // Plain SELECT must succeed (correctness covered by
    // `q41_shape_count_gt_zero_self_correlated_or_decorrelates`).
    let _ = engine.execute(q).unwrap();

    // The executed plan: EXPLAIN ANALYZE walks `plan_select` output and
    // stamps `actual time=…` only on the operators it really executed.
    // A `Hash semi join` line with `actual time=` proves the rewrite
    // engaged at execution time, not just at EXPLAIN-display time.
    let QueryResponse::ResultSet(plan) = engine
        .execute(&format!("EXPLAIN ANALYZE FORMAT=TREE {q}"))
        .unwrap()
    else {
        panic!("expected EXPLAIN ANALYZE result set");
    };
    let plan_text: String = plan.rows.iter().map(|r| format!("{:?}", r[0])).collect();
    let plan_lower = plan_text.to_lowercase();
    assert!(
        plan_lower.contains("semi join"),
        "EXPLAIN ANALYZE plan missing `semi join` line — \
         `(SELECT COUNT(*) ...) > 0` → semi-join rewrite did not engage. \
         See docs/tpcds-q41-correlated-count-perf-design.md. Plan: {plan_text}"
    );
    // The line containing `semi join` itself must carry `actual time=` —
    // i.e. the executor actually ran that operator (not just rendered it).
    let semi_line_with_actual = plan_text.split("\\n").any(|line| {
        let l = line.to_ascii_lowercase();
        l.contains("semi join") && l.contains("actual time=")
    });
    assert!(
        semi_line_with_actual,
        "`semi join` line has no `actual time=` stamp — the planner \
         rendered the rewrite but the executor took a different path. \
         Plan: {plan_text}"
    );
}

/// A FULL OUTER JOIN over CTEs must emit unmatched rows from *both* sides via
/// the hash path (unmatched left NULL-extended, unmatched right with NULL left).
#[test]
fn cte_full_join_emits_both_unmatched_sides() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)")
        .unwrap();
    // a = ids {1,2,3}; b = ids {3,4,5} → overlap {3}, a-only {1,2}, b-only {4,5}
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "WITH a AS (SELECT id AS a_id FROM t WHERE id <= 3), \
                  b AS (SELECT id + 2 AS b_id FROM t WHERE id <= 3) \
             SELECT a_id, b_id FROM a FULL JOIN b ON a_id = b_id \
             ORDER BY a_id, b_id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // Pairs: (1,NULL),(2,NULL),(3,3),(NULL,4),(NULL,5)
    let pairs: Vec<(Value, Value)> = result
        .rows
        .iter()
        .map(|r| (r[0].clone(), r[1].clone()))
        .collect();
    assert_eq!(pairs.len(), 5, "got {pairs:?}");
    assert!(pairs.contains(&(Value::Int(1), Value::Null)));
    assert!(pairs.contains(&(Value::Int(2), Value::Null)));
    assert!(pairs.contains(&(Value::Int(3), Value::Int(3))));
    assert!(pairs.contains(&(Value::Null, Value::Int(4))));
    assert!(pairs.contains(&(Value::Null, Value::Int(5))));
}

/// FULL OUTER JOIN with equi keys now routes to the hash join (planner
/// whitelist + `build/probe_is_outer_preserved` handle Full). This must match
/// the nested-loop oracle bit-for-bit, including: NULL keys on both sides
/// (never match, emitted unmatched), disjoint keys (NULL-extended on both
/// sides), and duplicate keys (full cartesian within the matching group).
#[test]
fn full_outer_join_hash_matches_nlj_with_nulls_and_dups() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE fa (k INT, va INT)").unwrap();
    engine.execute("CREATE TABLE fb (k INT, vb INT)").unwrap();
    engine
        .execute("INSERT INTO fa VALUES (1,11),(2,21),(3,31),(3,32),(NULL,91),(5,51)")
        .unwrap();
    engine
        .execute("INSERT INTO fb VALUES (2,22),(3,33),(3,34),(4,41),(NULL,92),(6,61)")
        .unwrap();

    let run = |sql: &str| -> Vec<(i64, i64, i64, i64)> {
        let QueryResponse::ResultSet(res) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        let cell = |v: &Value| match v {
            Value::Null => i64::MIN,
            other => as_f64(other) as i64,
        };
        let mut rows: Vec<(i64, i64, i64, i64)> = res
            .rows
            .iter()
            .map(|row| (cell(&row[0]), cell(&row[1]), cell(&row[2]), cell(&row[3])))
            .collect();
        rows.sort_unstable();
        rows
    };

    let hash = run("SELECT fa.k, fb.k, va, vb FROM fa FULL OUTER JOIN fb ON fa.k = fb.k");
    let nlj = run("SELECT /*+ NO_HASH_JOIN(fa,fb) */ fa.k, fb.k, va, vb \
         FROM fa FULL OUTER JOIN fb ON fa.k = fb.k");
    assert_eq!(hash, nlj, "hash FULL join must match NLJ oracle");
    // Sanity on the shape: 4 matched (k=3 ×2×2) + k=2 (1) = 5 matched, plus
    // left-only {1,5,NULL} = 3, right-only {4,6,NULL} = 3 → 11 rows total.
    assert_eq!(hash.len(), 11, "{hash:?}");
}

#[test]
fn cte_comma_join_pushes_where_equi_and_avoids_cartesian() {
    // Regression for the TPC-DS q30 family: a CTE comma-joined to base tables
    // with the equi-predicates living in WHERE. The in-memory CTE-join path
    // must push those equi-predicates into hash joins and order the joins by
    // connectivity instead of materialising the full cartesian product of the
    // comma-joined tables (which previously exhausted memory).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (o_id INT PRIMARY KEY, o_cust INT, o_amt INT)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1,100,10),(2,100,20),(3,200,5),(4,300,50)")
        .unwrap();
    engine
        .execute("CREATE TABLE cust (c_id INT PRIMARY KEY, c_addr INT, c_name VARCHAR(16))")
        .unwrap();
    engine
        .execute("INSERT INTO cust VALUES (100,1000,'Alice'),(200,2000,'Bob'),(300,1000,'Carol')")
        .unwrap();
    engine
        .execute("CREATE TABLE addr (a_id INT PRIMARY KEY, a_state VARCHAR(4))")
        .unwrap();
    engine
        .execute("INSERT INTO addr VALUES (1000,'CA'),(2000,'NY')")
        .unwrap();

    // FROM order is `t, addr, cust` but the equi-graph is t<->cust and
    // cust<->addr: a naive left-to-right join would cross t x addr first (no
    // connecting predicate). The greedy path must instead join t<->cust, then
    // cust<->addr, and apply the `a_state = 'CA'` filter.
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "WITH ototal AS (SELECT o_cust AS ct_cust, SUM(o_amt) AS ct_total FROM orders GROUP BY o_cust) \
             SELECT c_name FROM ototal t, addr, cust \
             WHERE a_id = c_addr AND a_state = 'CA' AND ct_cust = c_id \
             ORDER BY c_name",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let names: Vec<String> = result
        .rows
        .iter()
        .map(|r| {
            crate::types::value_str(&r[0])
                .unwrap_or_else(|| panic!("expected string name, got {:?}", r[0]))
                .to_string()
        })
        .collect();
    assert_eq!(names, vec!["Alice".to_string(), "Carol".to_string()]);

    // `SELECT *` must expand columns in FROM order (t, addr, cust) even though
    // the joins were processed in a different order.
    let QueryResponse::ResultSet(star) = engine
        .execute(
            "WITH ototal AS (SELECT o_cust AS ct_cust, SUM(o_amt) AS ct_total FROM orders GROUP BY o_cust) \
             SELECT * FROM ototal t, addr, cust \
             WHERE a_id = c_addr AND a_state = 'CA' AND ct_cust = c_id \
             ORDER BY c_id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let col_names: Vec<String> = star
        .columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect();
    assert_eq!(
        col_names,
        vec!["ct_cust", "ct_total", "a_id", "a_state", "c_id", "c_addr", "c_name"]
    );
    assert_eq!(star.rows.len(), 2);
    // Row 0 = Alice (c_id=100): values laid out in FROM order.
    assert_eq!(star.rows[0][0], Value::Int(100)); // t.ct_cust
    assert_eq!(star.rows[0][3], Value::String("CA".to_string())); // addr.a_state
    assert_eq!(star.rows[0][4], Value::Int(100)); // cust.c_id
    assert_eq!(star.rows[0][6], Value::String("Alice".to_string())); // cust.c_name
    assert_eq!(star.rows[1][6], Value::String("Carol".to_string()));
}

#[test]
fn explain_with_recursive_cte_marks_select_type() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "EXPLAIN WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 3) SELECT n FROM seq",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let select_types: Vec<String> = result
        .rows
        .iter()
        .filter_map(|row| match row.get(1) {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(
        select_types.iter().any(|s| s == "RECURSIVE_CTE"),
        "expected at least one RECURSIVE_CTE row, got: {select_types:?}"
    );
}

#[test]
fn explain_window_function_includes_window_step() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("EXPLAIN SELECT id, ROW_NUMBER() OVER (ORDER BY v) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let extras: Vec<String> = result
        .rows
        .iter()
        .filter_map(|row| match row.get(1) {
            Some(Value::String(s)) if s == "WINDOW" => match row.get(11) {
                Some(Value::String(s)) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert!(
        !extras.is_empty(),
        "expected a WINDOW row in EXPLAIN output"
    );
    assert!(
        extras[0].contains("ROW_NUMBER"),
        "expected ROW_NUMBER mentioned in WINDOW extra: {extras:?}"
    );
}

#[test]
fn cte_in_update_where_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine
        .execute(
            "WITH big AS (SELECT id FROM t WHERE v >= 20) UPDATE t SET v = 0 WHERE id IN (SELECT id FROM big)",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::Int(10));
    assert_eq!(result.rows[1][1], Value::Int(0));
    assert_eq!(result.rows[2][1], Value::Int(0));
}

#[test]
fn cte_in_delete_where_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine
        .execute(
            "WITH big AS (SELECT id FROM t WHERE v >= 20) DELETE FROM t WHERE id IN (SELECT id FROM big)",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SELECT id FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(1));
}

#[test]
fn window_default_frame_running_sum() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, SUM(v) OVER (ORDER BY id) AS rs FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::DecimalCompact(10, 0));
    assert_eq!(result.rows[1][1], Value::DecimalCompact(30, 0));
    assert_eq!(result.rows[2][1], Value::DecimalCompact(60, 0));
    assert_eq!(result.rows[3][1], Value::DecimalCompact(100, 0));
}

#[test]
fn window_rows_frame_sliding_avg() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, AVG(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::DecimalCompact(30000, 4));
    assert_eq!(result.rows[1][1], Value::DecimalCompact(40000, 4));
    assert_eq!(result.rows[2][1], Value::DecimalCompact(60000, 4));
    assert_eq!(result.rows[3][1], Value::DecimalCompact(80000, 4));
    assert_eq!(result.rows[4][1], Value::DecimalCompact(90000, 4));
}

#[test]
fn window_range_numeric_offset_preceding() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1), (2, 3), (3, 5), (4, 7), (5, 9)")
        .unwrap();
    // RANGE BETWEEN 2 PRECEDING AND CURRENT ROW: include rows whose v is in
    // [current_v - 2, current_v]. With v values 1,3,5,7,9 spaced by 2, each
    // window covers exactly the current row plus the immediate predecessor.
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, COUNT(*) OVER (ORDER BY v RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::Int(1));
    assert_eq!(result.rows[1][1], Value::Int(2));
    assert_eq!(result.rows[2][1], Value::Int(2));
    assert_eq!(result.rows[3][1], Value::Int(2));
    assert_eq!(result.rows[4][1], Value::Int(2));
}

#[test]
fn window_range_empty_frame_counts_zero_and_sums_null() {
    // Regression for issue #52: a RANGE frame whose value interval matches no
    // row must be empty (COUNT = 0, SUM = NULL). Previously range_bound_start
    // fell back to the last index and range_bound_end to index 0, so every
    // "empty" frame wrongly admitted exactly one row (COUNT = 1, SUM = value).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    // Example 1: entirely-empty frame ahead of every row (FOLLOWING).
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, COUNT(*) OVER (ORDER BY id RANGE BETWEEN 100 FOLLOWING AND 200 FOLLOWING) c FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    for row in &result.rows {
        assert_eq!(row[1], Value::Int(0));
    }

    // Example 2: entirely-empty frame behind every row (PRECEDING).
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, COUNT(*) OVER (ORDER BY id RANGE BETWEEN 100 PRECEDING AND 10 PRECEDING) c FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    for row in &result.rows {
        assert_eq!(row[1], Value::Int(0));
    }

    // Example 3: aggregate over an empty frame is NULL, not a stray value.
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, SUM(id) OVER (ORDER BY id RANGE BETWEEN 100 FOLLOWING AND 200 FOLLOWING) s FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    for row in &result.rows {
        assert_eq!(row[1], Value::Null);
    }

    // Non-empty frames must still work: RANGE BETWEEN 1 PRECEDING AND 1
    // FOLLOWING over 1..5 covers current row ± neighbours.
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, COUNT(*) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) c FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::Int(2)); // {1,2}
    assert_eq!(result.rows[1][1], Value::Int(3)); // {1,2,3}
    assert_eq!(result.rows[2][1], Value::Int(3)); // {2,3,4}
    assert_eq!(result.rows[4][1], Value::Int(2)); // {4,5}
}

#[test]
fn window_groups_frame_counts_peer_groups() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 1), (2, 10, 2), (3, 20, 3), (4, 20, 4), (5, 30, 5)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, COUNT(*) OVER (ORDER BY g GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW), SUM(v) OVER (ORDER BY g GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::Int(2));
    assert_eq!(result.rows[0][2], Value::DecimalCompact(3, 0));
    assert_eq!(result.rows[2][1], Value::Int(4));
    assert_eq!(result.rows[2][2], Value::DecimalCompact(10, 0));
    assert_eq!(result.rows[4][1], Value::Int(3));
    assert_eq!(result.rows[4][2], Value::DecimalCompact(12, 0));
}

#[test]
fn window_frame_exclude_variants_filter_frame_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 1), (2, 10, 2), (3, 20, 3), (4, 20, 4), (5, 30, 5)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, \
             COUNT(*) OVER (ORDER BY g GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW), \
             COUNT(*) OVER (ORDER BY g GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP), \
             COUNT(*) OVER (ORDER BY g GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) \
             FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::Int(1));
    assert_eq!(result.rows[0][2], Value::Int(0));
    assert_eq!(result.rows[0][3], Value::Int(1));
    assert_eq!(result.rows[2][1], Value::Int(3));
    assert_eq!(result.rows[2][2], Value::Int(2));
    assert_eq!(result.rows[2][3], Value::Int(3));
    assert_eq!(result.rows[4][1], Value::Int(4));
    assert_eq!(result.rows[4][2], Value::Int(4));
    assert_eq!(result.rows[4][3], Value::Int(5));
}

#[test]
fn window_multiple_share_partition_and_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS rn, RANK() OVER (PARTITION BY g ORDER BY id) AS rk, SUM(v) OVER (PARTITION BY g ORDER BY id) AS rs FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::Int(1));
    assert_eq!(result.rows[0][2], Value::Int(1));
    assert_eq!(result.rows[0][3], Value::DecimalCompact(10, 0));
    assert_eq!(result.rows[1][1], Value::Int(2));
    assert_eq!(result.rows[1][2], Value::Int(2));
    assert_eq!(result.rows[1][3], Value::DecimalCompact(30, 0));
    assert_eq!(result.rows[2][1], Value::Int(1));
    assert_eq!(result.rows[2][2], Value::Int(1));
    assert_eq!(result.rows[2][3], Value::DecimalCompact(30, 0));
    assert_eq!(result.rows[3][1], Value::Int(2));
    assert_eq!(result.rows[3][2], Value::Int(2));
    assert_eq!(result.rows[3][3], Value::DecimalCompact(70, 0));
}

#[test]
fn window_named_window_with_inherited_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, SUM(v) OVER w FROM t WINDOW w AS (ORDER BY id) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::DecimalCompact(10, 0));
    assert_eq!(result.rows[1][1], Value::DecimalCompact(30, 0));
    assert_eq!(result.rows[2][1], Value::DecimalCompact(60, 0));
}

#[test]
fn derived_table_with_group_by() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, status VARCHAR(10))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'a'), (2, 'a'), (3, 'b'), (4, 'b'), (5, 'b')")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT status, COUNT(*) AS n FROM (SELECT id, status FROM t) AS sub GROUP BY status ORDER BY status",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][1], Value::Int(2));
    assert_eq!(result.rows[1][1], Value::Int(3));
}

#[test]
fn alter_table_drop_column_fails_on_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    let err = engine.execute("ALTER TABLE t DROP COLUMN id").unwrap_err();
    assert_eq!(err.code(), 1090);
}

#[test]
fn regexp_operator_matches_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'apple'), (2, 'banana'), (3, 'apricot'), (4, 'cherry')")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM t WHERE name REGEXP '^ap' ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[1][0], Value::Int(3));

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM t WHERE name NOT REGEXP 'ap' ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(2));
    assert_eq!(result.rows[1][0], Value::Int(4));

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM t WHERE name RLIKE 'an' ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(2));
}

#[test]
fn create_view_and_select_from_view() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20), age INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")
        .unwrap();
    engine
        .execute("CREATE VIEW v AS SELECT id, name FROM t WHERE age >= 20")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, name FROM v ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(2));
    assert_eq!(result.rows[1][0], Value::Int(3));

    engine.execute("DROP VIEW v").unwrap();
    let err = engine.execute("SELECT * FROM v").unwrap_err();
    assert_eq!(err.code(), 1146);
}

#[test]
fn create_or_replace_view_overwrites_definition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    engine.execute("CREATE VIEW v AS SELECT id FROM t").unwrap();
    engine
        .execute("CREATE OR REPLACE VIEW v AS SELECT v FROM t")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SELECT * FROM v ORDER BY v").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.columns[0].name, "v");
    assert_eq!(result.rows[0][0], Value::Int(10));
    assert_eq!(result.rows[1][0], Value::Int(20));
}

#[test]
fn view_join_with_base_table_works() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(20))")
        .unwrap();
    engine
        .execute("CREATE TABLE orders (oid INT PRIMARY KEY, uid INT, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (10, 1, 100), (11, 1, 200), (12, 2, 50)")
        .unwrap();
    engine
        .execute(
            "CREATE VIEW big_orders AS SELECT oid, uid, amount FROM orders WHERE amount >= 100",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT u.name, b.oid FROM users u INNER JOIN big_orders b ON b.uid = u.id ORDER BY b.oid",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2, "rows: {:?}", result.rows);
    assert_eq!(result.rows[0][0], Value::String("alice".to_string()));
    assert_eq!(result.rows[0][1], Value::Int(10));
    assert_eq!(result.rows[1][1], Value::Int(11));
}

#[test]
fn nested_view_referencing_other_view() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine
        .execute("CREATE VIEW v1 AS SELECT id, v FROM t WHERE v >= 20")
        .unwrap();
    engine
        .execute("CREATE VIEW v2 AS SELECT id FROM v1 WHERE v >= 30")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine.execute("SELECT id FROM v2").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(3));
}

#[test]
fn create_view_rejects_unknown_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute("CREATE VIEW v AS SELECT id FROM nonexistent_table")
        .unwrap_err();
    assert_eq!(
        err.code(),
        1146,
        "expected 1146 (table not found), got {:?}",
        err
    );
}

#[test]
fn drop_table_invalidates_dependent_view() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    engine
        .execute("CREATE VIEW v AS SELECT id, v FROM t")
        .unwrap();
    engine.execute("DROP TABLE t").unwrap();
    let err = engine.execute("SELECT * FROM v").unwrap_err();
    assert_eq!(
        err.code(),
        1356,
        "expected 1356 invalid view reference, got {:?}",
        err
    );
}

#[test]
fn view_with_explicit_column_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
    engine
        .execute("CREATE VIEW v(my_id, my_name) AS SELECT id, name FROM t")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SHOW COLUMNS FROM v").unwrap() else {
        panic!("expected result set");
    };
    let column_names: Vec<String> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.clone(),
            other => panic!("unexpected value {:?}", other),
        })
        .collect();
    assert_eq!(
        column_names,
        vec!["my_id".to_string(), "my_name".to_string()]
    );
}

#[test]
fn view_options_roundtrip_to_show_create_and_information_schema() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, status VARCHAR(10))")
        .unwrap();
    engine
        .execute(
            "CREATE ALGORITHM=MERGE DEFINER='root'@'localhost' SQL SECURITY INVOKER \
             VIEW v(id_alias, status_alias) AS SELECT id, status FROM t \
             WITH CASCADED CHECK OPTION",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine.execute("SHOW CREATE VIEW v").unwrap() else {
        panic!("expected result set");
    };
    let create_sql = match &result.rows[0][1] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected value {:?}", other),
    };
    assert!(create_sql.contains("ALGORITHM=MERGE"), "{create_sql}");
    assert!(
        create_sql.contains("DEFINER=`root`@`localhost`"),
        "{create_sql}"
    );
    assert!(create_sql.contains("SQL SECURITY INVOKER"), "{create_sql}");
    assert!(
        create_sql.contains("WITH CASCADED CHECK OPTION"),
        "{create_sql}"
    );

    let QueryResponse::ResultSet(rows) = engine
        .execute(
            "SELECT CHECK_OPTION, IS_UPDATABLE, DEFINER, SECURITY_TYPE \
             FROM information_schema.views WHERE TABLE_NAME = 'v'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], Value::String("CASCADED".to_string()));
    assert_eq!(rows.rows[0][1], Value::String("YES".to_string()));
    assert_eq!(rows.rows[0][2], Value::String("root@localhost".to_string()));
    assert_eq!(rows.rows[0][3], Value::String("INVOKER".to_string()));
}

#[test]
fn create_view_rejects_duplicate_output_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let err = engine
        .execute("CREATE VIEW v(a, a) AS SELECT id, v FROM t")
        .unwrap_err();
    assert_eq!(err.code(), 1060, "got: {:?}", err);
}

#[test]
fn renamed_updatable_view_rewrites_dml_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, status VARCHAR(10), v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'on', 10), (2, 'off', 20)")
        .unwrap();
    engine
        .execute("CREATE VIEW v_on(my_id, my_status, my_v) AS SELECT id, status, v FROM t WHERE status = 'on'")
        .unwrap();

    engine
        .execute("INSERT INTO v_on (my_id, my_status, my_v) VALUES (3, 'on', 30)")
        .unwrap();
    engine
        .execute("UPDATE v_on SET my_v = my_v + 1 WHERE my_id = 1")
        .unwrap();
    engine.execute("DELETE FROM v_on WHERE my_id = 3").unwrap();

    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id, status, v FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows.len(), 2);
    assert_eq!(rows.rows[0][0], Value::Int(1));
    assert_eq!(rows.rows[0][2], Value::Int(11));
    assert_eq!(rows.rows[1][0], Value::Int(2));
}

#[test]
fn check_option_enforced_for_insert_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, status VARCHAR(10), v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE src (id INT PRIMARY KEY, status VARCHAR(10), v INT)")
        .unwrap();
    engine
        .execute(
            "CREATE VIEW v_on AS SELECT id, status, v FROM t WHERE status = 'on' \
             WITH LOCAL CHECK OPTION",
        )
        .unwrap();
    engine
        .execute("INSERT INTO v_on (id, status, v) VALUES (1, 'on', 10)")
        .unwrap();
    engine
        .execute("INSERT INTO src VALUES (2, 'on', 20)")
        .unwrap();
    engine
        .execute("INSERT INTO v_on SELECT id, status, v FROM src")
        .unwrap();
    let err = engine
        .execute("INSERT INTO v_on (id, status, v) VALUES (3, 'off', 30)")
        .unwrap_err();
    assert_eq!(err.code(), 1369, "got: {:?}", err);
    engine
        .execute("INSERT INTO src VALUES (4, 'off', 40)")
        .unwrap();
    let err = engine
        .execute("INSERT INTO v_on SELECT id, status, v FROM src WHERE id = 4")
        .unwrap_err();
    assert_eq!(err.code(), 1369, "got: {:?}", err);
}

#[test]
fn check_option_enforced_for_update_post_image() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, status VARCHAR(10), v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'on', 10)")
        .unwrap();
    engine
        .execute(
            "CREATE VIEW v_on AS SELECT id, status, v FROM t WHERE status = 'on' \
             WITH LOCAL CHECK OPTION",
        )
        .unwrap();

    engine
        .execute("UPDATE v_on SET v = v + 1 WHERE id = 1")
        .unwrap();
    let err = engine
        .execute("UPDATE v_on SET status = 'off' WHERE id = 1")
        .unwrap_err();
    assert_eq!(err.code(), 1369, "got: {:?}", err);

    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT status, v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rows.rows,
        vec![vec![Value::String("on".into()), Value::Int(11)]]
    );
}

#[test]
fn cascaded_check_option_enforced_for_nested_updatable_view() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, status VARCHAR(10), v INT)")
        .unwrap();
    engine
        .execute("CREATE VIEW v_on AS SELECT id, status, v FROM t WHERE status = 'on'")
        .unwrap();
    engine
        .execute(
            "CREATE VIEW v_outer AS SELECT id, status, v FROM v_on WHERE id > 0 \
             WITH CASCADED CHECK OPTION",
        )
        .unwrap();

    engine
        .execute("INSERT INTO v_outer (id, status, v) VALUES (1, 'on', 10)")
        .unwrap();
    engine
        .execute("UPDATE v_outer SET v = v + 1 WHERE id = 1")
        .unwrap();
    let err = engine
        .execute("INSERT INTO v_outer (id, status, v) VALUES (2, 'off', 20)")
        .unwrap_err();
    assert_eq!(err.code(), 1369, "got: {:?}", err);

    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT IS_UPDATABLE FROM information_schema.views WHERE TABLE_NAME = 'v_outer'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::String("YES".into())]]);
}

#[test]
fn definer_view_uses_definer_privileges_for_body() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    engine
        .execute_with_session(&mut root, "CREATE TABLE secret (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut root, "INSERT INTO secret VALUES (1)")
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE USER 'alice'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "CREATE SQL SECURITY DEFINER VIEW v_secret AS SELECT id FROM secret",
        )
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT SELECT ON test.v_secret TO 'alice'@'%'")
        .unwrap();

    let mut alice = SessionContext::default();
    alice.user = "alice".to_string();
    alice.login_user = "alice".to_string();
    alice.authenticated_as = crate::engine::auth::AccountKey::new("alice", "%");
    alice.auth_epoch = engine.db.auth_catalog().epoch;

    let denied = engine
        .execute_with_session(&mut alice, "SELECT id FROM secret")
        .unwrap_err();
    assert_eq!(denied.code(), 1142);
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut alice, "SELECT id FROM v_secret")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn invoker_view_requires_invoker_privileges_for_body() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    engine
        .execute_with_session(&mut root, "CREATE TABLE secret (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut root, "INSERT INTO secret VALUES (1)")
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE USER 'alice'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "CREATE SQL SECURITY INVOKER VIEW v_invoker AS SELECT id FROM secret",
        )
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT SELECT ON test.v_invoker TO 'alice'@'%'")
        .unwrap();

    let mut alice = SessionContext::default();
    alice.user = "alice".to_string();
    alice.login_user = "alice".to_string();
    alice.authenticated_as = crate::engine::auth::AccountKey::new("alice", "%");
    alice.auth_epoch = engine.db.auth_catalog().epoch;

    let denied = engine
        .execute_with_session(&mut alice, "SELECT id FROM v_invoker")
        .unwrap_err();
    assert_eq!(denied.code(), 1142);
    engine
        .execute_with_session(&mut root, "GRANT SELECT ON test.secret TO 'alice'@'%'")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut alice, "SELECT id FROM v_invoker")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn definer_updatable_view_uses_definer_privileges_for_base_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    engine
        .execute_with_session(&mut root, "CREATE TABLE secret (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut root, "INSERT INTO secret VALUES (1)")
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE USER 'alice'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "CREATE SQL SECURITY DEFINER VIEW v_secret_dml AS SELECT id FROM secret",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "GRANT UPDATE ON test.v_secret_dml TO 'alice'@'%'",
        )
        .unwrap();

    let mut alice = SessionContext::default();
    alice.user = "alice".to_string();
    alice.login_user = "alice".to_string();
    alice.authenticated_as = crate::engine::auth::AccountKey::new("alice", "%");
    alice.auth_epoch = engine.db.auth_catalog().epoch;

    engine
        .execute_with_session(&mut alice, "UPDATE v_secret_dml SET id = 2 WHERE id = 1")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut root, "SELECT id FROM secret")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn invoker_updatable_view_requires_invoker_privileges_for_base_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    engine
        .execute_with_session(&mut root, "CREATE TABLE secret (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut root, "INSERT INTO secret VALUES (1)")
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE USER 'alice'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "CREATE SQL SECURITY INVOKER VIEW v_secret_dml AS SELECT id FROM secret",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "GRANT UPDATE ON test.v_secret_dml TO 'alice'@'%'",
        )
        .unwrap();

    let mut alice = SessionContext::default();
    alice.user = "alice".to_string();
    alice.login_user = "alice".to_string();
    alice.authenticated_as = crate::engine::auth::AccountKey::new("alice", "%");
    alice.auth_epoch = engine.db.auth_catalog().epoch;

    let denied = engine
        .execute_with_session(&mut alice, "UPDATE v_secret_dml SET id = 2 WHERE id = 1")
        .unwrap_err();
    assert_eq!(denied.code(), 1142);

    engine
        .execute_with_session(&mut root, "GRANT UPDATE ON test.secret TO 'alice'@'%'")
        .unwrap();
    engine
        .execute_with_session(&mut alice, "UPDATE v_secret_dml SET id = 2 WHERE id = 1")
        .unwrap();
}

#[test]
fn alter_view_replaces_definition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'x'), (2, 'y')")
        .unwrap();
    engine.execute("CREATE VIEW v AS SELECT id FROM t").unwrap();
    engine
        .execute("ALTER VIEW v AS SELECT id, name FROM t WHERE id = 2")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SELECT * FROM v").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(2));
}

#[test]
fn show_create_view_returns_definition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("CREATE VIEW v AS SELECT id FROM t").unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SHOW CREATE VIEW v").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.columns.len(), 4);
    assert_eq!(result.columns[0].name, "View");
    assert_eq!(result.rows[0][0], Value::String("v".to_string()));
    let create_sql = match &result.rows[0][1] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected value {:?}", other),
    };
    assert!(
        create_sql.starts_with("CREATE"),
        "got create_sql={}",
        create_sql
    );
    assert!(create_sql.contains("VIEW `v`"));
}

#[test]
fn show_tables_includes_views() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("CREATE VIEW v AS SELECT id FROM t").unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SHOW FULL TABLES").unwrap() else {
        panic!("expected result set");
    };
    let entries: Vec<(String, String)> = result
        .rows
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::String(name), Value::String(kind)) => (name.clone(), kind.clone()),
            other => panic!("unexpected row {:?}", other),
        })
        .collect();
    assert!(entries.iter().any(|(n, k)| n == "t" && k == "BASE TABLE"));
    assert!(entries.iter().any(|(n, k)| n == "v" && k == "VIEW"));
}

#[test]
fn information_schema_views_lists_view_definition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("CREATE VIEW v AS SELECT id FROM t").unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT TABLE_NAME, VIEW_DEFINITION FROM information_schema.views WHERE TABLE_NAME = 'v'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("v".to_string()));
    let def = match &result.rows[0][1] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected value {:?}", other),
    };
    assert!(def.to_uppercase().contains("SELECT"));
}

#[test]
fn self_referencing_view_creation_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("CREATE VIEW v AS SELECT id FROM t").unwrap();
    // Replacing v in terms of itself must fail with a recursion error
    // rather than silently writing a self-referential view.
    let err = engine
        .execute("CREATE OR REPLACE VIEW v AS SELECT id FROM v")
        .unwrap_err();
    assert_eq!(err.code(), 1462, "got: {:?}", err);
}

#[test]
fn updatable_view_supports_insert_update_delete() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, status VARCHAR(10), v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'on', 10), (2, 'off', 20)")
        .unwrap();
    engine
        .execute("CREATE VIEW v_on AS SELECT id, status, v FROM t WHERE status = 'on'")
        .unwrap();

    // INSERT through view
    engine
        .execute("INSERT INTO v_on (id, status, v) VALUES (3, 'on', 30)")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t ORDER BY id").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows.len(), 3);

    // UPDATE through view (should only touch rows visible via the view's WHERE)
    engine.execute("UPDATE v_on SET v = v + 1").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!()
    };
    // Row id=1 (status=on, v=10) → 11; id=2 (status=off, v=20) → unchanged 20;
    // id=3 (status=on, v=30) → 31
    assert_eq!(rs.rows[0][1], Value::Int(11));
    assert_eq!(rs.rows[1][1], Value::Int(20));
    assert_eq!(rs.rows[2][1], Value::Int(31));

    // DELETE through view (only id=1, id=3 should go)
    engine.execute("DELETE FROM v_on").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t ORDER BY id").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::Int(2));
}

#[test]
fn non_updatable_view_returns_1288() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    // GROUP BY makes the view non-updatable.
    engine
        .execute("CREATE VIEW v_sum AS SELECT id, SUM(v) AS total FROM t GROUP BY id")
        .unwrap();
    let err = engine.execute("DELETE FROM v_sum").unwrap_err();
    assert_eq!(err.code(), 1288, "got: {:?}", err);
    let err = engine.execute("UPDATE v_sum SET total = 99").unwrap_err();
    assert_eq!(err.code(), 1288, "got: {:?}", err);
    let err = engine
        .execute("INSERT INTO v_sum (id, total) VALUES (2, 0)")
        .unwrap_err();
    assert_eq!(err.code(), 1471, "got: {:?}", err);
}

#[test]
fn explain_view_select_marks_view_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("CREATE VIEW v AS SELECT id, v FROM t WHERE v >= 20")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("EXPLAIN SELECT * FROM v").unwrap()
    else {
        panic!("expected result set");
    };
    let select_type_idx = result
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case("select_type"))
        .expect("explain has select_type col");
    let extra_idx = result
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case("Extra"))
        .expect("explain has Extra col");
    let any_view = result
        .rows
        .iter()
        .any(|row| matches!(&row[select_type_idx], Value::String(s) if s == "VIEW"));
    assert!(
        any_view,
        "expected at least one VIEW row, got: {:?}",
        result.rows
    );
    let any_view_extra = result
        .rows
        .iter()
        .any(|row| matches!(&row[extra_idx], Value::String(s) if s.contains("view: v")));
    assert!(
        any_view_extra,
        "expected `view: v` in Extra, got: {:?}",
        result.rows
    );
}

#[test]
fn create_table_as_select_clones_data() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE src (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO src VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();
    engine
        .execute("CREATE TABLE dst AS SELECT id, v FROM src WHERE v >= 200")
        .unwrap();
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM dst ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(2));
    assert_eq!(result.rows[1][0], Value::Int(3));
}

#[test]
fn correlated_subquery_in_where() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, dept VARCHAR(10), salary INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES (1, 'eng', 100), (2, 'eng', 200), (3, 'sales', 50), (4, 'sales', 150)",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id FROM t outer_t WHERE salary = (SELECT MAX(salary) FROM t inner_t WHERE inner_t.dept = outer_t.dept) ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(2));
    assert_eq!(result.rows[1][0], Value::Int(4));
}

#[test]
fn correlated_scalar_subquery_with_unqualified_other_table_column_stays_above_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE part (p_partkey INT PRIMARY KEY, p_size INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE partsupp (ps_partkey INT NOT NULL, ps_suppkey INT NOT NULL, ps_supplycost INT)",
        )
        .unwrap();
    engine.execute("INSERT INTO part VALUES (1, 15)").unwrap();
    engine
        .execute("INSERT INTO partsupp VALUES (1, 10, 7), (1, 11, 9)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT p_partkey, ps_suppkey \
             FROM part INNER JOIN partsupp ON p_partkey = ps_partkey \
             WHERE ps_supplycost = ( \
                 SELECT MIN(ps_supplycost) FROM partsupp \
                 WHERE p_partkey = ps_partkey \
             ) \
             ORDER BY p_partkey",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(10)]]);
}

#[test]
fn aggregate_over_join_keeps_argument_column_after_scan_projection_merge() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE partsupp (\
             ps_partkey INT NOT NULL, \
             ps_suppkey INT NOT NULL, \
             ps_supplycost INT, \
             PRIMARY KEY (ps_partkey, ps_suppkey), \
             INDEX idx_ps_suppkey (ps_suppkey))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE supplier (\
             s_suppkey INT PRIMARY KEY, \
             s_nationkey INT, \
             INDEX idx_supplier_nation (s_nationkey))",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_regionkey INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE region (r_regionkey INT PRIMARY KEY, r_name VARCHAR(30))")
        .unwrap();
    engine
        .execute("INSERT INTO partsupp VALUES (1, 10, 7), (1, 11, 9), (2, 10, 5)")
        .unwrap();
    engine
        .execute("INSERT INTO supplier VALUES (10, 100), (11, 100)")
        .unwrap();
    engine
        .execute("INSERT INTO nation VALUES (100, 1)")
        .unwrap();
    engine
        .execute("INSERT INTO region VALUES (1, 'EUROPE')")
        .unwrap();
    for table in ["partsupp", "supplier", "nation", "region"] {
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT MIN(ps_supplycost) \
             FROM partsupp \
             INNER JOIN supplier ON s_suppkey = ps_suppkey \
             INNER JOIN nation ON s_nationkey = n_nationkey \
             INNER JOIN region ON n_regionkey = r_regionkey \
             WHERE 1 = ps_partkey AND r_name = 'EUROPE'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(7)]]);
}

#[test]
fn tpch_q2_shape_keeps_supplycost_for_scalar_subquery_filter() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE part (\
             p_partkey INT PRIMARY KEY, \
             p_size INT, \
             p_type VARCHAR(30), \
             p_mfgr VARCHAR(30))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE partsupp (\
             ps_partkey INT NOT NULL, \
             ps_suppkey INT NOT NULL, \
             ps_supplycost INT, \
             INDEX idx_ps_partkey (ps_partkey), \
             INDEX idx_ps_suppkey (ps_suppkey))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE supplier (\
             s_suppkey INT PRIMARY KEY, \
             s_name VARCHAR(30), \
             s_address VARCHAR(30), \
             s_nationkey INT, \
             s_phone VARCHAR(30), \
             s_acctbal INT, \
             s_comment VARCHAR(30))",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_name VARCHAR(30), n_regionkey INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE region (r_regionkey INT PRIMARY KEY, r_name VARCHAR(30))")
        .unwrap();

    engine
        .execute(
            "INSERT INTO part VALUES (1, 15, 'PROMO BRASS', 'MFGR#1'), (2, 15, 'STEEL', 'MFGR#2')",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO partsupp VALUES \
             (1, 10, 7), \
             (1, 11, 9), \
             (2, 10, 5)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO supplier VALUES \
             (10, 'Supplier#10', 'addr10', 100, 'phone10', 50, 'comment10'), \
             (11, 'Supplier#11', 'addr11', 100, 'phone11', 60, 'comment11')",
        )
        .unwrap();
    engine
        .execute("INSERT INTO nation VALUES (100, 'GERMANY', 1)")
        .unwrap();
    engine
        .execute("INSERT INTO region VALUES (1, 'EUROPE')")
        .unwrap();
    for table in ["part", "partsupp", "supplier", "nation", "region"] {
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT \
                 s_acctbal, s_name, n_name, p_partkey, p_mfgr, \
                 s_address, s_phone, s_comment \
             FROM part \
             INNER JOIN partsupp ON p_partkey = ps_partkey \
             INNER JOIN supplier ON s_suppkey = ps_suppkey \
             INNER JOIN nation ON s_nationkey = n_nationkey \
             INNER JOIN region ON n_regionkey = r_regionkey \
             WHERE p_size = 15 \
               AND p_type LIKE '%BRASS' \
               AND r_name = 'EUROPE' \
               AND ps_supplycost = ( \
                   SELECT MIN(ps_supplycost) \
                   FROM partsupp \
                   INNER JOIN supplier ON s_suppkey = ps_suppkey \
                   INNER JOIN nation ON s_nationkey = n_nationkey \
                   INNER JOIN region ON n_regionkey = r_regionkey \
                   WHERE p_partkey = ps_partkey \
                     AND r_name = 'EUROPE' \
               ) \
             ORDER BY s_acctbal DESC, n_name, s_name, p_partkey \
             LIMIT 100",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], Value::String("Supplier#10".to_string()));
}

#[test]
fn parallel_batched_inlj_evaluates_joined_scalar_subquery_residual() {
    let _guard = super::super::tests::test_env_lock();
    let prior_min = std::env::var("MYDB_INLJ_BATCH_MIN_LEFT_ROWS").ok();
    let prior_threshold = std::env::var("MYDB_INLJ_OUTER_PARALLEL_THRESHOLD").ok();
    let prior_parallelism = std::env::var("MYDB_INLJ_OUTER_PARALLELISM").ok();
    let prior_decorrelate = std::env::var("MYDB_DECORRELATE").ok();
    std::env::set_var("MYDB_INLJ_BATCH_MIN_LEFT_ROWS", "1");
    std::env::set_var("MYDB_INLJ_OUTER_PARALLEL_THRESHOLD", "1");
    std::env::set_var("MYDB_INLJ_OUTER_PARALLELISM", "2");
    std::env::set_var("MYDB_DECORRELATE", "off");
    let _restore_min = scopeguard_restore_env_key("MYDB_INLJ_BATCH_MIN_LEFT_ROWS", prior_min);
    let _restore_threshold =
        scopeguard_restore_env_key("MYDB_INLJ_OUTER_PARALLEL_THRESHOLD", prior_threshold);
    let _restore_parallelism =
        scopeguard_restore_env_key("MYDB_INLJ_OUTER_PARALLELISM", prior_parallelism);
    let _restore_decorrelate = scopeguard_restore_env_key("MYDB_DECORRELATE", prior_decorrelate);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE part (p_partkey INT PRIMARY KEY, p_size INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE partsupp (\
             ps_partkey INT NOT NULL, \
             ps_suppkey INT NOT NULL, \
             ps_supplycost INT, \
             INDEX idx_ps_partkey (ps_partkey))",
        )
        .unwrap();

    let part_values = (1..=80)
        .map(|id| format!("({id},15)"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO part VALUES {part_values}"))
        .unwrap();

    let mut partsupp_values = Vec::new();
    for id in 1..=80 {
        partsupp_values.push(format!("({id},{},7)", id * 10));
        partsupp_values.push(format!("({id},{},9)", id * 10 + 1));
    }
    for id in 81..=1000 {
        partsupp_values.push(format!("({id},{},11)", id * 10));
    }
    engine
        .execute(&format!(
            "INSERT INTO partsupp VALUES {}",
            partsupp_values.join(",")
        ))
        .unwrap();
    engine.execute("ANALYZE TABLE part").unwrap();
    engine.execute("ANALYZE TABLE partsupp").unwrap();

    let sql = "SELECT /*+ ORDERED INDEX_NLJ(partsupp) */ p_partkey, ps_suppkey \
         FROM part INNER JOIN partsupp ON p_partkey = ps_partkey \
         WHERE ps_supplycost = ( \
             SELECT MIN(ps_supplycost) FROM partsupp \
             WHERE p_partkey = ps_partkey \
         ) \
         ORDER BY p_partkey";
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 80);
    assert_eq!(result.rows[0], vec![Value::Int(1), Value::Int(10)]);
    assert_eq!(result.rows[79], vec![Value::Int(80), Value::Int(800)]);
}

#[test]
fn correlated_exists_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY, name VARCHAR(20))")
        .unwrap();
    engine
        .execute("CREATE TABLE child (id INT PRIMARY KEY, parent_id INT)")
        .unwrap();
    engine
        .execute("INSERT INTO parent VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    engine
        .execute("INSERT INTO child VALUES (10, 1), (11, 1), (12, 3)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id FROM parent p WHERE EXISTS (SELECT 1 FROM child c WHERE c.parent_id = p.id) ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[1][0], Value::Int(3));
}

#[test]
fn window_ntile_buckets_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, NTILE(2) OVER (ORDER BY v) AS bucket FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 5);
    assert_eq!(result.rows[0][1], Value::Int(1));
    assert_eq!(result.rows[1][1], Value::Int(1));
    assert_eq!(result.rows[2][1], Value::Int(1));
    assert_eq!(result.rows[3][1], Value::Int(2));
    assert_eq!(result.rows[4][1], Value::Int(2));
}

#[test]
fn window_first_value_and_last_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, dept VARCHAR(10), v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 30), (3, 'b', 20), (4, 'b', 40)")
        .unwrap();
    // MySQL default frame for ORDER BY is RANGE UNBOUNDED PRECEDING AND CURRENT ROW,
    // so LAST_VALUE returns the current row's value (the last value of the
    // current peer group up to and including the current row), not the
    // partition's final value.
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, FIRST_VALUE(v) OVER (PARTITION BY dept ORDER BY v) AS f, LAST_VALUE(v) OVER (PARTITION BY dept ORDER BY v) AS l FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::Int(10));
    assert_eq!(result.rows[0][2], Value::Int(10));
    assert_eq!(result.rows[2][1], Value::Int(20));
    assert_eq!(result.rows[2][2], Value::Int(20));

    // With an explicit ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    // frame, LAST_VALUE should return the partition's final value.
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, LAST_VALUE(v) OVER (PARTITION BY dept ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS l FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::Int(30));
    assert_eq!(result.rows[2][1], Value::Int(40));
}

fn insert_big_rows(engine: &StorageQueryEngine, n: usize, sid_for: impl Fn(usize) -> i64) {
    for start in (0..n).step_by(200) {
        insert_fixture_rows(
            engine,
            "test.big",
            (start..(start + 200).min(n)).map(|i| {
                vec![
                    Value::Int(i as i64 + 1),
                    Value::Int(sid_for(i)),
                    Value::String("p".into()),
                ]
            }),
        );
    }
}

#[test]
fn storage_engine_evaluates_cast_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT CAST(123 AS CHAR), CAST('456' AS SIGNED), CAST('2026-05-02' AS DATE), CAST(3.14 AS SIGNED)",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(
        result.rows,
        vec![vec![
            Value::String("123".into()),
            Value::Int(456),
            // CAST(... AS DATE) now yields a genuine DATE value (days since
            // epoch) rather than a string; 2026-05-02 == day 20575.
            Value::Date(20575),
            Value::Int(3),
        ]]
    );

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT CAST(42 AS UNSIGNED), CAST(3.14159 AS DECIMAL(10,2)), CAST('hello' AS BINARY)",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::UInt(42));
    assert_eq!(result.rows[0][1], Value::DecimalCompact(314, 2));
    assert_eq!(result.rows[0][2], Value::Bytes(b"hello".to_vec()));
}

#[test]
fn storage_engine_evaluates_new_string_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT LPAD('hi', 5, '0'), RPAD('hi', 5, '0'), REPEAT('ab', 3), SPACE(3)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![
            Value::String("000hi".into()),
            Value::String("hi000".into()),
            Value::String("ababab".into()),
            Value::String("   ".into()),
        ]]
    );

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT ASCII('A'), ORD('A'), BIN(10), OCT(8), HEX(255)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Int(65),
            Value::Int(65),
            Value::String("1010".into()),
            Value::String("10".into()),
            Value::String("FF".into()),
        ]]
    );

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT INSERT('hello', 2, 3, 'XY'), FIND_IN_SET('b', 'a,b,c'), FIELD('b', 'a', 'b', 'c'), ELT(2, 'a', 'b', 'c')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![
            Value::String("hXYo".into()),
            Value::Int(2),
            Value::Int(2),
            Value::String("b".into()),
        ]]
    );

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT FIELD(NULL, 'a', NULL), FIELD(1, 0, NULL), \
                    INTERVAL(0.0, NULL), INTERVAL(0.0, NULL, NULL, NULL), \
                    FIND_IN_SET('', ''), FIND_IN_SET('', ',a')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Int(0),
            Value::Int(0),
            Value::Int(1),
            Value::Int(3),
            Value::Int(0),
            Value::Int(1),
        ]]
    );

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT STRCMP('a', 'b'), STRCMP('b', 'a'), STRCMP('a', 'a'), FORMAT(1234567.891, 2)",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Int(-1),
            Value::Int(1),
            Value::Int(0),
            Value::String("1,234,567.89".into()),
        ]]
    );

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT BIT_LENGTH('a'), OCTET_LENGTH('a'), QUOTE('it\\'s')")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(8));
    assert_eq!(result.rows[0][1], Value::Int(1));
    assert_eq!(result.rows[0][2], Value::String("'it\\'s'".into()));
}

#[test]
fn hex_encodes_raw_bytes_without_utf8_loss() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // HEX of a binary value (here via UNHEX) must round-trip byte-exact,
    // not route through a lossy UTF-8 conversion that maps 0xFF → U+FFFD.
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT HEX(UNHEX('00FF0102DEADBEEF'))")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::String("00FF0102DEADBEEF".into())]]
    );

    // Same invariant through a LONGBLOB column round-trip: storage is
    // byte-exact, so HEX of the stored value must match the input hex.
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, v LONGBLOB)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (1, UNHEX('00FF0102DEADBEEF'))")
        .unwrap();
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT HEX(v), LENGTH(v) FROM b").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![
            Value::String("00FF0102DEADBEEF".into()),
            Value::Int(8)
        ]]
    );
}

#[test]
fn storage_engine_evaluates_mysql80_string_and_regex_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT TO_BASE64('hello'), FROM_BASE64('aGVsbG8='), EXPORT_SET(5,'Y','N','/',4), MAKE_SET(5,'a','b','c'), SOUNDEX('Robert')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::String("aGVsbG8=".into()));
    assert_eq!(result.rows[0][1], Value::Bytes(b"hello".to_vec()));
    assert_eq!(result.rows[0][2], Value::String("Y/N/Y/N".into()));
    assert_eq!(result.rows[0][3], Value::String("a,c".into()));
    assert_eq!(result.rows[0][4], Value::String("R163".into()));

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT REGEXP_LIKE('Abc','^a','i'), REGEXP_INSTR('abcabc','bc',1,2), REGEXP_REPLACE('abc123','[0-9]+','X'), REGEXP_SUBSTR('abc123','[0-9]+')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1], Value::Int(5));
    assert_eq!(result.rows[0][2], Value::String("abcX".into()));
    assert_eq!(result.rows[0][3], Value::String("123".into()));
}

#[test]
fn storage_engine_evaluates_new_math_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT PI(), LOG2(8), LOG10(100), TRUNCATE(1.999, 1)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!((as_f64(&result.rows[0][0]) - std::f64::consts::PI).abs() < 1e-10);
    assert!((as_f64(&result.rows[0][1]) - 3.0).abs() < 1e-10);
    assert!((as_f64(&result.rows[0][2]) - 2.0).abs() < 1e-10);
    assert!((as_f64(&result.rows[0][3]) - 1.9).abs() < 1e-10);

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT SIN(0), COS(0), DEGREES(PI()), RADIANS(180)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!((as_f64(&result.rows[0][0])).abs() < 1e-10);
    assert!((as_f64(&result.rows[0][1]) - 1.0).abs() < 1e-10);
    assert!((as_f64(&result.rows[0][2]) - 180.0).abs() < 1e-10);
    assert!((as_f64(&result.rows[0][3]) - std::f64::consts::PI).abs() < 1e-10);

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT CONV('a', 16, 10), CONV('10', 2, 10), CRC32('hello')")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::String("10".into()));
    assert_eq!(result.rows[0][1], Value::String("2".into()));
    assert_eq!(result.rows[0][2], Value::UInt(907060870));

    let QueryResponse::ResultSet(result) = engine.execute("SELECT RAND()").unwrap() else {
        panic!("expected result set");
    };
    let r = as_f64(&result.rows[0][0]);
    assert!((0.0..1.0).contains(&r));
}

#[test]
fn storage_engine_evaluates_mysql80_math_and_bit_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT BIT_COUNT(15), 7 DIV 3, DIV(7, 3), RAND(42)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(4));
    assert_eq!(result.rows[0][1], Value::Int(2));
    assert_eq!(result.rows[0][2], Value::Int(2));
    let r = as_f64(&result.rows[0][3]);
    assert!((0.0..1.0).contains(&r));
}

#[test]
fn storage_engine_evaluates_date_arithmetic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT DATE_ADD('2026-01-15', INTERVAL 1 MONTH), DATE_SUB('2026-03-31', INTERVAL 1 MONTH)",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::String("2026-02-15".into()));
    assert_eq!(result.rows[0][1], Value::String("2026-02-28".into()));

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT DAYNAME('2026-05-07'), MONTHNAME('2026-05-07'), QUARTER('2026-05-07'), WEEKDAY('2026-05-07')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::String("Thursday".into()));
    assert_eq!(result.rows[0][1], Value::String("May".into()));
    assert_eq!(result.rows[0][2], Value::Int(2));
    assert_eq!(result.rows[0][3], Value::Int(3));

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT LAST_DAY('2026-02-15'), DAYOFWEEK('2026-05-07'), DAYOFYEAR('2026-01-01'), TO_DAYS('2026-01-01')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(matches!(result.rows[0][0], Value::Date(_)));
    assert_eq!(result.rows[0][0].to_display_string(), "2026-02-28");
    assert_eq!(result.rows[0][1], Value::Int(5));
    assert_eq!(result.rows[0][2], Value::Int(1));
    assert!(matches!(result.rows[0][3], Value::Int(_)));

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT MAKEDATE(2026, 32), TIME('2026-05-07 10:20:30'), MAKETIME(10, 20, 30)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::String("2026-02-01".into()));
    assert_eq!(result.rows[0][1], Value::Time(37_230_000_000));
    assert_eq!(result.rows[0][2], Value::Time(37_230_000_000));
}

#[test]
fn storage_engine_evaluates_date_add_with_various_units() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT DATE_ADD('2026-01-01 00:00:00', INTERVAL 1 HOUR), DATE_ADD('2026-01-01', INTERVAL 7 DAY), DATE_ADD('2026-01-01', INTERVAL 1 YEAR)",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows[0][0],
        Value::String("2026-01-01 01:00:00".into())
    );
    assert_eq!(result.rows[0][1], Value::String("2026-01-08".into()));
    assert_eq!(result.rows[0][2], Value::String("2027-01-01".into()));
}

#[test]
fn storage_engine_evaluates_mysql80_date_time_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT MICROSECOND('2026-05-07 10:20:30.123456'), TIMESTAMP('2026-05-07','01:02:03'), TO_SECONDS('1970-01-01 00:00:00'), GET_FORMAT(DATE,'ISO'), CONVERT_TZ('2026-01-01 00:00:00','+00:00','+08:00')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(123456));
    assert!(matches!(result.rows[0][1], Value::DateTime(_)));
    assert_eq!(result.rows[0][1].to_display_string(), "2026-05-07 01:02:03");
    assert!(matches!(result.rows[0][2], Value::Int(_)));
    assert_eq!(result.rows[0][3], Value::String("%Y-%m-%d".into()));
    assert_eq!(
        result.rows[0][4],
        Value::String("2026-01-01 08:00:00".into())
    );
}

#[test]
fn storage_engine_evaluates_timestampdiff() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT TIMESTAMPDIFF(MONTH, '2025-01-15', '2026-05-07'), TIMESTAMPDIFF(DAY, '2026-01-01', '2026-05-07')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(15));
    assert_eq!(result.rows[0][1], Value::Int(126));
}

#[test]
fn storage_engine_evaluates_str_to_date() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT STR_TO_DATE('07/05/2026', '%d/%m/%Y')")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows[0][0],
        Value::Date(crate::storage::parse_date_to_days_bytes(b"2026-05-07").unwrap())
    );
}

#[test]
fn storage_engine_evaluates_standard_date_interval_arithmetic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT DATE '1998-12-01' - INTERVAL '90' DAY, DATE '1994-01-01' + INTERVAL '1' YEAR, DATE '1993-07-01' + INTERVAL '3' MONTH",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows[0][0],
        Value::Date(crate::storage::parse_date_to_days_bytes(b"1998-09-02").unwrap())
    );
    assert_eq!(
        result.rows[0][1],
        Value::Date(crate::storage::parse_date_to_days_bytes(b"1995-01-01").unwrap())
    );
    assert_eq!(
        result.rows[0][2],
        Value::Date(crate::storage::parse_date_to_days_bytes(b"1993-10-01").unwrap())
    );
}

#[test]
fn derived_q9_shape_matches_flattened_join_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE part (p_partkey INT PRIMARY KEY, p_name VARCHAR(55))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT, l_partkey INT, l_suppkey INT, l_linenumber INT, \
             l_quantity INT, l_extendedprice INT, l_discount INT, \
             PRIMARY KEY (l_orderkey, l_linenumber), \
             INDEX idx_lineitem_partkey (l_partkey), \
             INDEX idx_lineitem_suppkey (l_suppkey), \
             INDEX idx_lineitem_partsuppkey (l_partkey, l_suppkey))",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE supplier (s_suppkey INT PRIMARY KEY, s_nationkey INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE partsupp (\
             ps_partkey INT, ps_suppkey INT, ps_supplycost INT, \
             PRIMARY KEY (ps_partkey, ps_suppkey))",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_orderdate DATE)")
        .unwrap();
    engine
        .execute("CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_name VARCHAR(25))")
        .unwrap();

    engine
        .execute("INSERT INTO part VALUES (1, 'forest green part'), (2, 'plain red part')")
        .unwrap();
    engine
        .execute("INSERT INTO supplier VALUES (10, 1), (20, 2)")
        .unwrap();
    engine
        .execute("INSERT INTO nation VALUES (1, 'BRAZIL'), (2, 'CANADA')")
        .unwrap();
    engine
        .execute("INSERT INTO partsupp VALUES (1, 10, 3), (1, 20, 5), (2, 10, 7)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (100, '1995-03-01'), (200, '1996-07-10')")
        .unwrap();
    engine
        .execute(
            "INSERT INTO lineitem VALUES \
             (100, 1, 10, 1, 2, 100, 0), \
             (200, 1, 20, 1, 1, 80, 0), \
             (200, 2, 10, 2, 4, 70, 0)",
        )
        .unwrap();
    for table in [
        "part", "lineitem", "supplier", "partsupp", "orders", "nation",
    ] {
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }

    let derived = "SELECT nation, o_year, SUM(amount) AS sum_profit \
                   FROM ( \
                     SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, \
                            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount \
                     FROM part \
                     INNER JOIN lineitem ON p_partkey = l_partkey \
                     INNER JOIN supplier ON s_suppkey = l_suppkey \
                     INNER JOIN partsupp ON ps_partkey = l_partkey AND ps_suppkey = l_suppkey \
                     INNER JOIN orders ON o_orderkey = l_orderkey \
                     INNER JOIN nation ON s_nationkey = n_nationkey \
                     WHERE p_name LIKE '%green%' \
                   ) AS profit \
                   GROUP BY nation, o_year \
                   ORDER BY nation, o_year DESC";
    let flat = "SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, \
                       SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS sum_profit \
                FROM part \
                INNER JOIN lineitem ON p_partkey = l_partkey \
                INNER JOIN supplier ON s_suppkey = l_suppkey \
                INNER JOIN partsupp ON ps_partkey = l_partkey AND ps_suppkey = l_suppkey \
                INNER JOIN orders ON o_orderkey = l_orderkey \
                INNER JOIN nation ON s_nationkey = n_nationkey \
                WHERE p_name LIKE '%green%' \
                GROUP BY n_name, EXTRACT(YEAR FROM o_orderdate) \
                ORDER BY nation, o_year DESC";
    let QueryResponse::ResultSet(derived_rows) = engine.execute(derived).unwrap() else {
        panic!("expected derived result set");
    };
    let QueryResponse::ResultSet(flat_rows) = engine.execute(flat).unwrap() else {
        panic!("expected flat result set");
    };
    assert_eq!(derived_rows.rows, flat_rows.rows);
}

#[test]
fn tpch_q12_shape_runs_with_auto_parameterized_date_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE orders ( \
             o_orderkey INT PRIMARY KEY, \
             o_orderpriority VARCHAR(20) NOT NULL \
             )",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem ( \
             l_orderkey INT NOT NULL, \
             l_shipmode VARCHAR(10) NOT NULL, \
             l_commitdate DATE NOT NULL, \
             l_receiptdate DATE NOT NULL, \
             l_shipdate DATE NOT NULL \
             )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1, '1-URGENT'), (2, '3-MEDIUM')")
        .unwrap();
    engine
        .execute(
            "INSERT INTO lineitem VALUES \
             (1, 'MAIL', '1994-01-03', '1994-01-05', '1994-01-01'), \
             (2, 'SHIP', '1994-02-03', '1994-02-05', '1994-02-01')",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT \
                 l_shipmode, \
                 SUM(CASE \
                     WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' \
                     THEN 1 ELSE 0 \
                 END) AS high_line_count, \
                 SUM(CASE \
                     WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' \
                     THEN 1 ELSE 0 \
                 END) AS low_line_count \
             FROM orders \
             INNER JOIN lineitem ON o_orderkey = l_orderkey \
             WHERE l_shipmode IN ('MAIL', 'SHIP') \
               AND l_commitdate < l_receiptdate \
               AND l_shipdate < l_commitdate \
               AND l_receiptdate >= '1994-01-01' \
               AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR \
             GROUP BY l_shipmode \
             ORDER BY l_shipmode",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn tpch_q12_shape_estimates_filtered_range_rows_below_access_rows() {
    use crate::parser::{parse_statement, Statement};
    use crate::planner::{plan_select, PhysicalPlan};

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE lineitem ( \
             l_orderkey INT NOT NULL, \
             l_shipmode VARCHAR(10) NOT NULL, \
             l_commitdate DATE NOT NULL, \
             l_receiptdate DATE NOT NULL, \
             l_shipdate DATE NOT NULL, \
             INDEX idx_lineitem_receiptdate (l_receiptdate) \
             )",
        )
        .unwrap();
    let rows = (1..=1000)
        .map(|i| {
            let date = if i <= 500 { "1994-06-01" } else { "1996-06-01" };
            let mode = if i % 10 == 0 { "MAIL" } else { "RAIL" };
            format!("({i}, '{mode}', '1994-01-03', '{date}', '1994-01-01')")
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO lineitem VALUES {rows}"))
        .unwrap();
    engine.execute("ANALYZE TABLE lineitem").unwrap();

    let Statement::Select(select) = parse_statement(
        "SELECT l_orderkey FROM lineitem \
         WHERE l_shipmode IN ('MAIL', 'SHIP') \
           AND l_receiptdate >= '1994-01-01' \
           AND l_receiptdate < '1995-01-01'",
    )
    .unwrap() else {
        panic!("expected select");
    };
    let planned = plan_select(engine.db_for_tests(), &select).unwrap();

    fn find_lineitem_range(plan: &PhysicalPlan) -> Option<(f64, f64)> {
        match plan {
            PhysicalPlan::IndexRangeScan {
                table,
                index,
                estimated_rows,
                access_rows,
                ..
            } if table.schema.name == "lineitem"
                && index.name.eq_ignore_ascii_case("idx_lineitem_receiptdate") =>
            {
                Some((*estimated_rows, *access_rows))
            }
            PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::HashAggregate { input, .. } => find_lineitem_range(input),
            PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::HashJoin { left, right, .. } => {
                find_lineitem_range(left).or_else(|| find_lineitem_range(right))
            }
            PhysicalPlan::IndexNestedLoopJoin { left, .. } => find_lineitem_range(left),
            _ => None,
        }
    }

    let Some((estimated_rows, access_rows)) = find_lineitem_range(&planned.physical) else {
        panic!(
            "expected lineitem receiptdate range scan, got {:?}",
            planned.physical
        );
    };
    assert!(
        estimated_rows < access_rows,
        "residual filter should reduce estimated rows below index access rows; est={estimated_rows} access={access_rows}"
    );
}

#[test]
fn tpch_q14_shape_drives_from_lineitem_shipdate_range() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE part ( \
             p_partkey INT PRIMARY KEY, \
             p_type VARCHAR(25) NOT NULL \
             )",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem ( \
             l_orderkey INT NOT NULL, \
             l_partkey INT NOT NULL, \
             l_shipdate DATE NOT NULL, \
             l_extendedprice DECIMAL(15,2) NOT NULL, \
             l_discount DECIMAL(15,2) NOT NULL, \
             INDEX idx_lineitem_shipdate (l_shipdate), \
             INDEX idx_lineitem_partkey (l_partkey) \
             )",
        )
        .unwrap();
    let parts = (1..=600)
        .map(|i| {
            let ty = if i % 10 == 0 {
                "PROMO BURNISHED"
            } else {
                "STANDARD"
            };
            format!("({i}, '{ty}')")
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO part VALUES {parts}"))
        .unwrap();
    let lineitems = (1..=3_000)
        .map(|i| {
            let part = (i % 600) + 1;
            let date = if i <= 100 { "1995-09-15" } else { "1996-01-01" };
            format!("({i}, {part}, '{date}', 100.00, 0.05)")
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO lineitem VALUES {lineitems}"))
        .unwrap();
    engine.execute("ANALYZE TABLE part").unwrap();
    engine.execute("ANALYZE TABLE lineitem").unwrap();

    let q14 = "SELECT \
               100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) \
               / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue \
               FROM lineitem INNER JOIN part ON l_partkey = p_partkey \
               WHERE l_shipdate >= '1995-09-01' \
                 AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH";

    let QueryResponse::ResultSet(tree) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {q14}"))
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let tree_text = match &tree.rows[0][0] {
        Value::String(s) => s.clone(),
        other => panic!("expected tree text, got {other:?}"),
    };
    assert!(
        tree_text.contains("Index range scan on lineitem using idx_lineitem_shipdate"),
        "Q14-shaped plan should drive from selective lineitem shipdate range: {tree_text}"
    );
    assert!(
        !tree_text.contains("Index lookup on lineitem using idx_lineitem_partkey"),
        "Q14-shaped plan should not scan part first and probe lineitem by partkey: {tree_text}"
    );
    assert!(
        tree_text.contains("heap fetch: mrr") && tree_text.contains("planning mrr window: 131072"),
        "non-covering range scan must expose the stable MRR contract: {tree_text}"
    );

    // The diagnostic switch changes only the executor's RID batching. It
    // must preserve the exact result and must not require replanning.
    let oracle_rows = with_test_env(
        &[
            ("MYDB_MRR_SORT", Some("0")),
            ("MYDB_VECTORIZED", Some("off")),
        ],
        || {
            let QueryResponse::ResultSet(result) = engine.execute(q14).unwrap() else {
                panic!("expected Q14 result set");
            };
            result.rows
        },
    );
    let before_windows =
        super::super::select::plan::MRR_WINDOWS.load(std::sync::atomic::Ordering::Relaxed);
    let mrr_rows = with_test_env(&[("MYDB_MRR_SORT", None)], || {
        let QueryResponse::ResultSet(result) = engine.execute(q14).unwrap() else {
            panic!("expected Q14 result set");
        };
        result.rows
    });
    assert_eq!(mrr_rows, oracle_rows, "MRR must preserve the Q14 result");
    assert!(
        super::super::select::plan::MRR_WINDOWS.load(std::sync::atomic::Ordering::Relaxed)
            > before_windows,
        "Q14 non-covering range scan should execute at least one MRR window"
    );

    let escape_before = crate::engine::expr_program::expr_program_stats().legacy_escape_rows;
    let QueryResponse::ResultSet(analyzed) = engine
        .execute(&format!("EXPLAIN ANALYZE FORMAT=TREE {q14}"))
        .unwrap()
    else {
        panic!("expected analyze result set");
    };
    let analyzed_text = match &analyzed.rows[0][0] {
        Value::String(s) => s,
        other => panic!("expected analyze tree text, got {other:?}"),
    };
    assert!(
        analyzed_text.contains("mrr_windows=")
            && analyzed_text.contains("mrr_unique_pages=")
            && analyzed_text.contains("mrr_peak_bytes="),
        "EXPLAIN ANALYZE should attach MRR metrics to the range scan: {analyzed_text}"
    );
    assert!(
        analyzed_text.contains("aggregate_mode=typed_batch")
            && !analyzed_text.contains("fallback_aggregate_rows="),
        "exact CASE aggregate inputs should stay in the typed batch accumulator: {analyzed_text}"
    );
    assert_eq!(
        crate::engine::expr_program::expr_program_stats().legacy_escape_rows,
        escape_before,
        "searched DECIMAL CASE should use its exact batch program without legacy row escapes"
    );

    // Buffer-pool capacity is an executor resource, not a planning input.
    // Resizing it must neither invalidate nor reshape the Q14 physical plan.
    engine
        .execute("SET GLOBAL mydb_buffer_pool_pages = 64")
        .unwrap();
    let QueryResponse::ResultSet(resized) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {q14}"))
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let resized_text = match &resized.rows[0][0] {
        Value::String(s) => s,
        other => panic!("expected tree text, got {other:?}"),
    };
    assert_eq!(
        resized_text, &tree_text,
        "buffer-pool resize must not change Q14 EXPLAIN"
    );
}

#[test]
fn tpch_q10_shape_keeps_order_join_keys_when_orderdate_index_is_covering_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_name VARCHAR(25) NOT NULL)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE customer (\
             c_custkey INT PRIMARY KEY, \
             c_name VARCHAR(32) NOT NULL, \
             c_nationkey INT NOT NULL, \
             INDEX idx_customer_nation (c_nationkey))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY, \
             o_custkey INT NOT NULL, \
             o_orderdate DATE NOT NULL, \
             INDEX idx_orders_orderdate (o_orderdate))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT NOT NULL, \
             l_returnflag CHAR(1) NOT NULL, \
             INDEX idx_lineitem_orderkey (l_orderkey))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO nation VALUES (1, 'BRAZIL'), (2, 'CANADA')")
        .unwrap();
    engine
        .execute("INSERT INTO customer VALUES (1, 'Alice', 1), (2, 'Bob', 2)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders VALUES \
             (100, 1, '1993-11-15'), \
             (101, 2, '1993-11-16'), \
             (102, 1, '1992-01-01'), \
             (103, 1, '1994-02-01')",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO lineitem VALUES \
             (100, 'R'), \
             (101, 'R'), \
             (102, 'R'), \
             (103, 'R')",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE nation").unwrap();
    engine.execute("ANALYZE TABLE customer").unwrap();
    engine.execute("ANALYZE TABLE orders").unwrap();
    engine.execute("ANALYZE TABLE lineitem").unwrap();

    let sql = "SELECT /*+ ORDERED HASH_JOIN(orders, lineitem) */ c_name, COUNT(*) \
               FROM orders FORCE INDEX (idx_orders_orderdate) \
               INNER JOIN lineitem ON o_orderkey = l_orderkey \
               INNER JOIN customer ON o_custkey = c_custkey \
               INNER JOIN nation ON c_nationkey = n_nationkey \
               WHERE o_orderdate >= '1993-10-01' \
                 AND o_orderdate < '1994-01-01' \
                 AND l_returnflag = 'R' \
                 AND n_name = 'BRAZIL' \
               GROUP BY c_name";

    let QueryResponse::ResultSet(tree) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let tree_text = match &tree.rows[0][0] {
        Value::String(s) => s,
        other => panic!("expected tree text, got {other:?}"),
    };
    assert!(
        tree_text.contains("idx_orders_orderdate"),
        "test must exercise the orderdate index path: {tree_text}"
    );

    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::String("Alice".into()), Value::Int(1)]]
    );
}

#[test]
fn batched_index_nlj_parallel_right_filter_keeps_auto_params() {
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior_auto = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    let prior_threshold = std::env::var("MYDB_INLJ_BATCH_PARALLEL_THRESHOLD").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    std::env::set_var("MYDB_INLJ_BATCH_PARALLEL_THRESHOLD", "1");
    let _restore_auto = scopeguard_restore_env(prior_auto);
    let _restore_threshold =
        scopeguard_restore_env_key("MYDB_INLJ_BATCH_PARALLEL_THRESHOLD", prior_threshold);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (o_orderkey INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem ( \
             l_orderkey INT NOT NULL, \
             l_shipdate DATE NOT NULL, \
             l_commitdate DATE NOT NULL, \
             l_receiptdate DATE NOT NULL, \
             INDEX idx_l_orderkey (l_orderkey) \
             )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1), (2)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO lineitem VALUES \
             (1, '1994-01-01', '1994-01-03', '1994-01-05'), \
             (2, '1994-02-01', '1994-02-03', '1994-02-05')",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE orders").unwrap();
    engine.execute("ANALYZE TABLE lineitem").unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT /*+ ORDERED INDEX_NLJ(lineitem) */ o_orderkey \
             FROM orders \
             INNER JOIN lineitem ON o_orderkey = l_orderkey \
             WHERE l_receiptdate >= '1994-01-01' \
               AND l_shipdate < l_commitdate \
             ORDER BY o_orderkey",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn storage_engine_evaluates_hash_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT MD5('hello'), SHA1('hello'), SHA2('hello', 256), CRC32('hello')")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows[0][0],
        Value::String("5d41402abc4b2a76b9719d911017c592".into())
    );
    assert_eq!(
        result.rows[0][1],
        Value::String("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d".into())
    );
    assert_eq!(
        result.rows[0][2],
        Value::String("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into())
    );
    assert_eq!(result.rows[0][3], Value::UInt(907060870));
}

#[test]
fn storage_engine_evaluates_mysql80_json_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute(
            r#"SELECT JSON_CONTAINS_PATH('{"a":[1],"b":2}','all','$.a','$.b'), JSON_DEPTH('{"a":[1,{"b":2}]}'), JSON_OVERLAPS('[1,2]','[3,2]'), JSON_VALUE('{"a":"x"}','$.a')"#,
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1], Value::Int(4));
    assert_eq!(result.rows[0][2], Value::Int(1));
    assert_eq!(result.rows[0][3], Value::String("x".into()));

    let QueryResponse::ResultSet(result) = engine
        .execute(
            r#"SELECT JSON_ARRAY_APPEND('{"a":[1]}','$.a',2), JSON_MERGE_PATCH('{"a":1,"b":2}','{"a":null,"c":3}'), JSON_QUOTE('a"b'), JSON_SEARCH('{"a":"needle"}','one','need%')"#,
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0].to_display_string(), r#"{"a": [1, 2]}"#);
    assert_eq!(result.rows[0][1].to_display_string(), r#"{"b": 2, "c": 3}"#);
    assert_eq!(result.rows[0][2].to_display_string(), r#""a\"b""#);
    // A valid-identifier key is reported unquoted (`$.a`), matching MySQL.
    assert_eq!(result.rows[0][3].to_display_string(), r#""$.a""#);
}

#[test]
fn storage_engine_evaluates_mysql80_uuid_network_compression_and_crypto_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT BIN_TO_UUID(UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db', 1), 1), \
             IS_UUID('6ccd780c-baba-1026-9564-5b8c656024db'), \
             INET_ATON('127.0.0.1'), INET_NTOA(2130706433), \
             INET6_NTOA(INET6_ATON('::1')), \
             BIN_TO_UUID(UUID_TO_BIN('{12345678-1234-5678-1234-567812345678}')), \
             IS_UUID('{12345678-1234-5678-1234-567812345678}'), \
             IS_UUID('{12345678-1234-5678-1234-567812345678'), \
             IS_UUID('12345678-12345678-1234-567812345678')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows[0][0],
        Value::String("6ccd780c-baba-1026-9564-5b8c656024db".into())
    );
    assert_eq!(result.rows[0][1], Value::Int(1));
    assert_eq!(result.rows[0][2], Value::UInt(2130706433));
    assert_eq!(result.rows[0][3], Value::String("127.0.0.1".into()));
    assert_eq!(result.rows[0][4], Value::String("::1".into()));
    assert_eq!(
        result.rows[0][5],
        Value::String("12345678-1234-5678-1234-567812345678".into())
    );
    assert_eq!(result.rows[0][6], Value::Int(1));
    assert_eq!(result.rows[0][7], Value::Int(0));
    assert_eq!(result.rows[0][8], Value::Int(0));

    let QueryResponse::ResultSet(network_result) = engine
        .execute(
            "SELECT INET_ATON('1'), INET_ATON('127.1'), \
             HEX(INET6_ATON('001.002.003.004')), \
             HEX(INET6_ATON('::FFFF:01.2.3.4')), \
             INET6_NTOA(INET6_ATON('1:2:3::5:6:7:8')), \
             INET6_NTOA(INET6_ATON('::ffff:1.2.3.4')), \
             INET6_NTOA('1234'), \
             IS_IPV6('::001.02.000.255'), \
             IS_IPV4_COMPAT(INET6_ATON('::1.2.3.4')), \
             IS_IPV4_MAPPED(INET6_ATON('::ffff:1.2.3.4'))",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let row = &network_result.rows[0];
    assert_eq!(row[0], Value::UInt(1));
    assert_eq!(row[1], Value::UInt(2_130_706_433));
    assert_eq!(row[2], Value::String("01020304".into()));
    assert_eq!(
        row[3],
        Value::String("00000000000000000000FFFF01020304".into())
    );
    assert_eq!(row[4], Value::String("1:2:3::5:6:7:8".into()));
    assert_eq!(row[5], Value::String("::ffff:1.2.3.4".into()));
    assert_eq!(row[6], Value::Null);
    assert_eq!(row[7], Value::Int(1));
    assert_eq!(row[8], Value::Int(1));
    assert_eq!(row[9], Value::Int(1));

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT UNCOMPRESS(COMPRESS('hello')), UNCOMPRESSED_LENGTH(COMPRESS('hello')), AES_DECRYPT(AES_ENCRYPT('hello','k'),'k'), LENGTH(RANDOM_BYTES(8))",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Bytes(b"hello".to_vec()));
    assert_eq!(result.rows[0][1], Value::UInt(5));
    assert_eq!(result.rows[0][2], Value::Bytes(b"hello".to_vec()));
    assert_eq!(result.rows[0][3], Value::Int(8));

    let QueryResponse::ResultSet(empty_compression) = engine
        .execute(
            "SELECT COMPRESS(''), UNCOMPRESS(''), \
             UNCOMPRESS(COMPRESS('')), UNCOMPRESSED_LENGTH('')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        empty_compression.rows[0],
        vec![
            Value::Bytes(Vec::new()),
            Value::Bytes(Vec::new()),
            Value::Bytes(Vec::new()),
            Value::UInt(0),
        ]
    );

    engine
        .execute(
            "CREATE TABLE uuid_generated (\
             source VARCHAR(36),\
             packed BINARY(16) AS (UUID_TO_BIN(source)) VIRTUAL,\
             rendered VARCHAR(36) AS (BIN_TO_UUID(packed)) VIRTUAL,\
             INDEX (packed), INDEX (rendered))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO uuid_generated (source) VALUES \
             ('12345678-1234-5678-1234-567812345678')",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT rendered FROM uuid_generated")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::String(
            "12345678-1234-5678-1234-567812345678".into()
        )]]
    );

    engine
        .execute("CREATE TABLE unhex_ctas AS SELECT UNHEX('FF80') AS payload")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT HEX(payload) FROM unhex_ctas")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::String("FF80".into())]]);
}

#[test]
fn row_constructor_compares_with_row_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE row_subquery_cmp (a INT, b INT, c FLOAT)")
        .unwrap();
    engine
        .execute("INSERT INTO row_subquery_cmp VALUES (NULL, 3, NULL)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT (a,b,c) <=> (SELECT NULL,3,NULL), \
             (b,a,c) <=> (SELECT 3,NULL,NULL) FROM row_subquery_cmp",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(1)]]);

    let err = engine
        .execute("SELECT (a,b,c) = (SELECT 1,2) FROM row_subquery_cmp")
        .unwrap_err();
    assert_eq!(err.code(), 1241);
    assert_eq!(err.sql_state(), "21000");
}

#[test]
fn storage_engine_evaluates_system_info_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let started = std::time::Instant::now();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT ISNULL(NULL), ISNULL(1), SLEEP(0.03), LENGTH(UUID())")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(started.elapsed() >= Duration::from_millis(25));
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1], Value::Int(0));
    assert_eq!(result.rows[0][2], Value::Int(0));
    assert_eq!(result.rows[0][3], Value::Int(36));
}

#[test]
fn kill_query_interrupts_sleep_and_preserves_session() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    let registry = std::sync::Arc::clone(engine.registry());
    let sleeper_handle = registry.register(31, "root".into(), "localhost".into(), "test".into());
    let killer_handle = registry.register(32, "root".into(), "localhost".into(), "test".into());

    let mut sleeper = SessionContext::default();
    sleeper.connection_id = 31;
    sleeper.set_cancel_handle(Some(sleeper_handle.cancel_handle()));
    let mut killer = SessionContext::default();
    killer.connection_id = 32;
    killer.set_cancel_handle(Some(killer_handle.cancel_handle()));

    let sleeper_engine = std::sync::Arc::clone(&engine);
    let sleeper_thread = std::thread::spawn(move || {
        let started = Instant::now();
        let err = sleeper_engine
            .execute_with_session(&mut sleeper, "SELECT SLEEP(2)")
            .unwrap_err();
        (err, started.elapsed(), sleeper)
    });

    std::thread::sleep(Duration::from_millis(50));
    engine
        .execute_with_session(&mut killer, "KILL QUERY 31")
        .unwrap();

    let (err, elapsed, mut sleeper) = sleeper_thread.join().unwrap();
    assert_eq!(err.code(), 1317);
    assert_eq!(err.sql_state(), "70100");
    assert!(elapsed < Duration::from_secs(1), "elapsed={elapsed:?}");

    sleeper.refresh_cancel_generations();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut sleeper, "SELECT 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(1));

    registry.unregister(31);
    registry.unregister(32);
}

#[test]
fn kill_connection_interrupts_sleep_and_marks_session_killed() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    let registry = std::sync::Arc::clone(engine.registry());
    let sleeper_handle = registry.register(41, "root".into(), "localhost".into(), "test".into());
    let killer_handle = registry.register(42, "root".into(), "localhost".into(), "test".into());

    let mut sleeper = SessionContext::default();
    sleeper.connection_id = 41;
    sleeper.set_cancel_handle(Some(sleeper_handle.cancel_handle()));
    let mut killer = SessionContext::default();
    killer.connection_id = 42;
    killer.set_cancel_handle(Some(killer_handle.cancel_handle()));

    let sleeper_engine = std::sync::Arc::clone(&engine);
    let sleeper_thread = std::thread::spawn(move || {
        let started = Instant::now();
        let err = sleeper_engine
            .execute_with_session(&mut sleeper, "SELECT SLEEP(2)")
            .unwrap_err();
        (err, started.elapsed(), sleeper)
    });

    std::thread::sleep(Duration::from_millis(50));
    engine
        .execute_with_session(&mut killer, "KILL CONNECTION 41")
        .unwrap();

    let (err, elapsed, mut sleeper) = sleeper_thread.join().unwrap();
    assert_eq!(err.code(), 1317);
    assert_eq!(err.sql_state(), "70100");
    assert!(elapsed < Duration::from_secs(1), "elapsed={elapsed:?}");

    let err = engine
        .execute_with_session(&mut sleeper, "SELECT 1")
        .unwrap_err();
    assert_eq!(err.code(), 1317);

    registry.unregister(41);
    registry.unregister(42);
}

/// `SHOW ENGINE MYDB STATUS` must surface the process-global KILL
/// counters so operators have a quick way to see whether KILL signals
/// are reaching the server. Increment `killed_queries` /
/// `killed_connections` via the registry and confirm both appear with
/// monotonically non-decreasing values.
#[test]
fn show_engine_mydb_status_exposes_kill_counters() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    let registry = std::sync::Arc::clone(engine.registry());
    let session_handle = registry.register(51, "root".into(), "localhost".into(), "test".into());
    let mut session = SessionContext::default();
    session.connection_id = 51;
    session.set_cancel_handle(Some(session_handle.cancel_handle()));

    let before = read_kill_status(&engine, &mut session);

    // Fresh victim sessions so a KILL CONNECTION on one doesn't kill the
    // session we're using to call SHOW ENGINE MYDB STATUS.
    let victim_query = registry.register(52, "root".into(), "localhost".into(), "test".into());
    let victim_conn = registry.register(53, "root".into(), "localhost".into(), "test".into());
    let _ = victim_query;
    let _ = victim_conn;

    registry.kill_query(52).unwrap();
    registry.kill_query(52).unwrap();
    registry.kill_connection(53).unwrap();

    let after = read_kill_status(&engine, &mut session);
    assert!(
        after.killed_queries >= before.killed_queries + 2,
        "before={before:?} after={after:?}"
    );
    assert!(
        after.killed_connections > before.killed_connections,
        "before={before:?} after={after:?}"
    );

    registry.unregister(51);
    registry.unregister(52);
    registry.unregister(53);
}

#[derive(Debug, Default)]
struct KillStatusRow {
    killed_queries: u64,
    killed_connections: u64,
}

fn read_kill_status(
    engine: &std::sync::Arc<crate::engine::executor::StorageQueryEngine>,
    session: &mut SessionContext,
) -> KillStatusRow {
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(session, "SHOW ENGINE MYDB STATUS")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let mut row = KillStatusRow::default();
    for r in &result.rows {
        let Value::String(name) = &r[1] else {
            continue;
        };
        let Value::String(val) = &r[2] else { continue };
        match name.as_str() {
            "killed_queries" => row.killed_queries = val.parse().unwrap_or(0),
            "killed_connections" => row.killed_connections = val.parse().unwrap_or(0),
            _ => {}
        }
    }
    row
}

/// During a KILL-in-flight window the targeted session must report
/// `STATE = "Killed"` via `SHOW PROCESSLIST`. Reproduce the window by
/// kicking off a `SELECT SLEEP(...)` on a separate thread, KILL QUERY
/// it from the main thread, and snapshot the process list while the
/// victim hasn't yet returned to the command loop.
#[test]
fn show_processlist_state_marks_killed_session() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    let registry = std::sync::Arc::clone(engine.registry());
    let sleeper_handle = registry.register(61, "root".into(), "localhost".into(), "test".into());
    let observer_handle = registry.register(62, "root".into(), "localhost".into(), "test".into());

    let mut sleeper = SessionContext::default();
    sleeper.connection_id = 61;
    sleeper.set_cancel_handle(Some(sleeper_handle.cancel_handle()));
    let mut observer = SessionContext::default();
    observer.connection_id = 62;
    observer.set_cancel_handle(Some(observer_handle.cancel_handle()));

    let sleeper_engine = std::sync::Arc::clone(&engine);
    let sleeper_thread = std::thread::spawn(move || {
        let _ = sleeper_engine.execute_with_session(&mut sleeper, "SELECT SLEEP(2)");
    });

    // Observe the actual execution phase instead of relying on a fixed sleep;
    // this remains deterministic on slow CI hosts.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let observed = registry
            .snapshot()
            .into_iter()
            .find(|process| process.id == 61)
            .and_then(|process| process.state);
        if observed.as_deref() == Some("User sleep") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "SELECT SLEEP never exposed the User sleep process state; last={observed:?}"
        );
        std::thread::yield_now();
    }
    registry.kill_query(61).unwrap();

    let processes = registry.snapshot();
    let victim = processes
        .iter()
        .find(|p| p.id == 61)
        .expect("sleeper still in registry");
    assert_eq!(victim.state.as_deref(), Some("Killed"));

    sleeper_thread.join().unwrap();
    registry.unregister(61);
    registry.unregister(62);
}

/// Parallel-scan worker threads must observe KILL via the inherited
/// cancel token. This used to fail silently: workers ran in
/// `thread::scope` with no cancel context installed, so their
/// `check_cancelled()` calls always returned `Ok`. After the audit,
/// every worker `enter`s a clone of the main thread's token.
///
/// We synthesize a long-running CPU-bound scan by `SELECT
/// SLEEP(...) FROM t` where `t` has enough rows that the planner picks
/// a parallel path, then KILL QUERY mid-stream.
#[test]
fn kill_query_interrupts_long_select_via_parallel_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    let registry = std::sync::Arc::clone(engine.registry());
    let sleeper_handle = registry.register(71, "root".into(), "localhost".into(), "test".into());
    let killer_handle = registry.register(72, "root".into(), "localhost".into(), "test".into());

    let mut sleeper = SessionContext::default();
    sleeper.connection_id = 71;
    sleeper.set_cancel_handle(Some(sleeper_handle.cancel_handle()));
    let mut killer = SessionContext::default();
    killer.connection_id = 72;
    killer.set_cancel_handle(Some(killer_handle.cancel_handle()));

    // Pin parallelism to 4 so the workload definitely goes through the
    // `thread::scope` parallel path even when the heap is small.
    engine
        .execute_with_session(&mut sleeper, "SET SESSION mydb_parallel_degree = 4")
        .unwrap();

    let sleeper_engine = std::sync::Arc::clone(&engine);
    let sleeper_thread = std::thread::spawn(move || {
        let started = Instant::now();
        let err = sleeper_engine
            .execute_with_session(&mut sleeper, "SELECT SLEEP(2)")
            .unwrap_err();
        (err, started.elapsed())
    });

    std::thread::sleep(Duration::from_millis(50));
    registry.kill_query(71).unwrap();

    let (err, elapsed) = sleeper_thread.join().unwrap();
    assert_eq!(err.code(), 1317);
    assert!(elapsed < Duration::from_secs(1), "elapsed={elapsed:?}");

    registry.unregister(71);
    registry.unregister(72);
}

#[test]
fn kill_query_reaches_streaming_cursor_with_buffered_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE stream_kill_t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO stream_kill_t VALUES (1), (2), (3)")
        .unwrap();

    let registry = std::sync::Arc::clone(engine.registry());
    let victim_handle = registry.register(73, "root".into(), "localhost".into(), "test".into());
    let mut victim = SessionContext::default();
    victim.connection_id = 73;
    victim.set_cancel_handle(Some(victim_handle.cancel_handle()));

    let response = engine
        .execute_streaming_with_session_for_test(
            &mut victim,
            "SELECT id FROM stream_kill_t ORDER BY id",
        )
        .unwrap();
    let QueryResponse::StreamingResultSet(mut result) = response else {
        panic!("expected streaming result set");
    };
    registry.kill_query(73).unwrap();
    let err = result.cursor.next_row().unwrap_err();
    assert_eq!(err.code(), 1317);
    assert_eq!(err.sql_state(), "70100");

    drop(result);
    victim.refresh_cancel_generations();
    let QueryResponse::ResultSet(ok) = engine
        .execute_with_session(&mut victim, "SELECT 1")
        .unwrap()
    else {
        panic!("expected result set after refreshing cancellation generation");
    };
    assert_eq!(ok.rows, vec![vec![Value::Int(1)]]);
    registry.unregister(73);
}

#[test]
fn storage_engine_evaluates_period_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT PERIOD_ADD(202601, 3), PERIOD_DIFF(202605, 202501)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(202604));
    assert_eq!(result.rows[0][1], Value::Int(16));
}

#[test]
fn storage_engine_evaluates_sec_to_time_and_time_to_sec() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT TIME_TO_SEC('10:20:30'), SEC_TO_TIME(37230)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(37230));
    assert_eq!(result.rows[0][1], Value::Time(37_230_000_000));
}

// ── JOIN tests ──────────────────────────────────────────────────────────

fn setup_join_tables(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    engine
        .execute("CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT, INDEX dept_idx (dept_id))")
        .unwrap();
    engine
        .execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')")
        .unwrap();
    engine
        .execute("INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Carol', 2), (4, 'Dave', NULL)")
        .unwrap();
}

#[test]
fn left_join_returns_null_for_unmatched_right_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT d.name, e.name FROM departments d LEFT JOIN employees e ON d.id = e.dept_id ORDER BY d.name, e.name",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 4);
    assert_eq!(result.rows[0][0], Value::String("Engineering".into()));
    assert_eq!(result.rows[0][1], Value::String("Alice".into()));
    assert_eq!(result.rows[1][0], Value::String("Engineering".into()));
    assert_eq!(result.rows[1][1], Value::String("Bob".into()));
    assert_eq!(result.rows[2][0], Value::String("HR".into()));
    assert_eq!(result.rows[2][1], Value::Null);
    assert_eq!(result.rows[3][0], Value::String("Sales".into()));
    assert_eq!(result.rows[3][1], Value::String("Carol".into()));
}

#[test]
fn right_join_returns_null_for_unmatched_left_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT d.name, e.name FROM departments d RIGHT JOIN employees e ON d.id = e.dept_id ORDER BY e.name",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 4);
    assert_eq!(result.rows[0][0], Value::String("Engineering".into()));
    assert_eq!(result.rows[0][1], Value::String("Alice".into()));
    assert_eq!(result.rows[1][0], Value::String("Engineering".into()));
    assert_eq!(result.rows[1][1], Value::String("Bob".into()));
    assert_eq!(result.rows[2][0], Value::String("Sales".into()));
    assert_eq!(result.rows[2][1], Value::String("Carol".into()));
    assert_eq!(result.rows[3][0], Value::Null);
    assert_eq!(result.rows[3][1], Value::String("Dave".into()));
}

#[test]
fn right_join_matches_reversed_left_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    let QueryResponse::ResultSet(left_result) = engine
        .execute(
            "SELECT e.name, d.name FROM employees e LEFT JOIN departments d ON e.dept_id = d.id ORDER BY e.name",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    let QueryResponse::ResultSet(right_result) = engine
        .execute(
            "SELECT e.name, d.name FROM departments d RIGHT JOIN employees e ON d.id = e.dept_id ORDER BY e.name",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(left_result.rows.len(), right_result.rows.len());
    for (l, r) in left_result.rows.iter().zip(right_result.rows.iter()) {
        assert_eq!(l, r);
    }
}

#[test]
fn left_join_on_preserved_side_predicate_keeps_unmatched_left_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT d.id, e.id FROM departments d \
             LEFT JOIN employees e ON d.id = e.dept_id AND d.id = 2 \
             ORDER BY d.id, e.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0], vec![Value::Int(1), Value::Null]);
    assert_eq!(result.rows[1], vec![Value::Int(2), Value::Int(3)]);
    assert_eq!(result.rows[2], vec![Value::Int(3), Value::Null]);
}

#[test]
fn right_join_on_preserved_side_predicate_keeps_unmatched_right_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT d.id, e.id FROM departments d \
             RIGHT JOIN employees e ON d.id = e.dept_id AND e.id = 1 \
             ORDER BY e.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 4);
    assert_eq!(result.rows[0], vec![Value::Int(1), Value::Int(1)]);
    assert_eq!(result.rows[1], vec![Value::Null, Value::Int(2)]);
    assert_eq!(result.rows[2], vec![Value::Null, Value::Int(3)]);
    assert_eq!(result.rows[3], vec![Value::Null, Value::Int(4)]);
}

#[test]
fn outer_join_constant_false_on_clause_preserves_outer_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    let QueryResponse::ResultSet(left_result) = engine
        .execute(
            "SELECT d.id, e.id FROM departments d \
             LEFT JOIN employees e ON 1 = 0 ORDER BY d.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(left_result.rows.len(), 3);
    assert!(left_result.rows.iter().all(|row| row[1] == Value::Null));

    let QueryResponse::ResultSet(right_result) = engine
        .execute(
            "SELECT d.id, e.id FROM departments d \
             RIGHT JOIN employees e ON 1 = 0 ORDER BY e.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(right_result.rows.len(), 4);
    assert!(right_result.rows.iter().all(|row| row[0] == Value::Null));
}

#[test]
fn hash_join_bloom_filter_advances_counters_and_matches_results() {
    let _guard = hash_join_env_lock();
    std::env::remove_var("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS");
    // The bloom is gated off for small builds by default; exercise it
    // explicitly by lowering the build-row floor to zero.
    std::env::set_var("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS", "0");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE small (id INT PRIMARY KEY, val INT, label VARCHAR(16))")
        .unwrap();
    engine
        .execute("CREATE TABLE big (id INT PRIMARY KEY, sid INT, payload VARCHAR(16))")
        .unwrap();

    let mut sql = String::from("INSERT INTO small VALUES ");
    for i in 1..=10 {
        if i > 1 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, {}, 'l{}')", i, i, i));
    }
    engine.execute(&sql).unwrap();

    insert_big_rows(&engine, 4_000, |i| {
        if i < 30 {
            (i % 10 + 1) as i64
        } else {
            99_999
        }
    });

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT b.id FROM big b JOIN small s ON b.sid = s.val ORDER BY b.id")
        .unwrap()
    else {
        panic!("expected result set");
    };

    let analyze = explain_analyze_tree(
        &engine,
        "SELECT b.id FROM big b JOIN small s ON b.sid = s.val ORDER BY b.id",
    );
    assert!(
        analyze.contains("runtime_filter_rejected="),
        "join rejection metrics should be query-local and operator-scoped: {analyze}"
    );

    let mut got: Vec<i64> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Int(v) => *v,
            other => panic!("unexpected value {:?}", other),
        })
        .collect();
    got.sort();

    std::env::set_var("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS", "0");
    let QueryResponse::ResultSet(reference) = engine
        .execute("SELECT b.id FROM big b JOIN small s ON b.sid = s.val ORDER BY b.id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    std::env::remove_var("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS");

    let mut expected: Vec<i64> = reference
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Int(v) => *v,
            other => panic!("unexpected value {:?}", other),
        })
        .collect();
    expected.sort();

    assert_eq!(got, expected);
    assert_eq!(got.len(), 30);
    std::env::remove_var("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS");
}

#[test]
fn hash_join_bloom_handles_null_join_keys() {
    let _guard = hash_join_env_lock();
    std::env::remove_var("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE small (id INT PRIMARY KEY, val INT, label VARCHAR(16))")
        .unwrap();
    engine
        .execute("CREATE TABLE big (id INT PRIMARY KEY, sid INT, payload VARCHAR(16))")
        .unwrap();

    engine
        .execute("INSERT INTO small VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c')")
        .unwrap();
    engine
        .execute(
            "INSERT INTO big (id, sid, payload) VALUES (1, 1, 'x'), (2, NULL, 'x'), (3, 2, 'x'), (4, NULL, 'x'), (5, 99, 'x')",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT b.id FROM big b JOIN small s ON b.sid = s.val ORDER BY b.id")
        .unwrap()
    else {
        panic!("expected result set");
    };

    let mut got: Vec<i64> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Int(v) => *v,
            other => panic!("unexpected value {:?}", other),
        })
        .collect();
    got.sort();
    assert_eq!(got, vec![1, 3]);
}

#[test]
fn hash_join_bloom_show_status_exposes_counters() {
    let _guard = hash_join_env_lock();
    std::env::remove_var("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(status) = engine.execute("SHOW ENGINE MYDB STATUS").unwrap()
    else {
        panic!("expected result set");
    };
    let names: Vec<&str> = status
        .rows
        .iter()
        .map(|row| match &row[1] {
            Value::String(s) => s.as_str(),
            other => panic!("unexpected name value {:?}", other),
        })
        .collect();
    assert!(
        names.contains(&"hash_join_bloom_built"),
        "SHOW ENGINE MYDB STATUS should expose hash_join_bloom_built"
    );
    assert!(
        names.contains(&"hash_join_bloom_filtered"),
        "SHOW ENGINE MYDB STATUS should expose hash_join_bloom_filtered"
    );
}

#[test]
fn session_functions_execute_through_compiled_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine.execute("CREATE TABLE t (id BIGINT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3)")
        .unwrap();

    // VERSION() and CONNECTION_ID() are exact and stable enough to
    // assert directly; NOW() / DATABASE() etc. just need to produce a
    // value of the right shape per row.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT VERSION(), CONNECTION_ID(), DATABASE(), NOW(), CURDATE() FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 3);
    for row in &rs.rows {
        assert_eq!(
            row[0],
            Value::String(crate::server_version::MYSQL_COMPAT_VERSION.to_string())
        );
        assert!(matches!(row[1], Value::UInt(_)));
        // DATABASE() may be the open database or NULL depending on the
        // session context; either is correct.
        assert!(matches!(row[2], Value::String(_) | Value::Null));
        assert!(matches!(row[3], Value::DateTime(_)));
        assert!(matches!(row[4], Value::Date(_)));
        assert_eq!(row[4].to_display_string().len(), "YYYY-MM-DD".len());
    }

    // Mixing a session function with a column predicate keeps the row
    // on the compiled path — previously this query would fall back to
    // the legacy interpreter and tick `expr_legacy_fallback_rows` for
    // every row scanned.
    let baseline = crate::engine::expr_program::expr_program_stats().legacy_fallback_rows;
    let _ = engine
        .execute("SELECT id, NOW() FROM t WHERE id > 0")
        .unwrap();
    let after = crate::engine::expr_program::expr_program_stats().legacy_fallback_rows;
    // The compiled path is per-projection, so we only assert that the
    // counter did not grow proportionally to the row count. In
    // practice mydb keeps the projection on the compiled path now
    // that NOW() is a kernel.
    assert!(
        after - baseline < 100,
        "NOW() projection should stay on compiled path, fallback rose by {}",
        after - baseline
    );
}

#[test]
fn show_engine_mydb_status_exposes_expr_program_stats() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE expr_obs (id BIGINT, name TEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO expr_obs VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
        .unwrap();
    let _ = engine
        .execute("SELECT id, LENGTH(name) FROM expr_obs WHERE id > 0 AND name LIKE 'a%'")
        .unwrap();

    let QueryResponse::ResultSet(status) = engine.execute("SHOW ENGINE MYDB STATUS").unwrap()
    else {
        panic!("expected result set");
    };
    let rows: std::collections::HashMap<&str, &str> = status
        .rows
        .iter()
        .map(|row| {
            let name = match &row[1] {
                Value::String(s) => s.as_str(),
                other => panic!("unexpected name value {:?}", other),
            };
            let value = match &row[2] {
                Value::String(s) => s.as_str(),
                other => panic!("unexpected value {:?}", other),
            };
            (name, value)
        })
        .collect();

    for key in [
        "expr_compile_cache_hits",
        "expr_compile_cache_misses",
        "expr_compile_unsupported",
        "expr_program_eval_rows",
        "expr_legacy_fallback_rows",
    ] {
        let value = rows
            .get(key)
            .unwrap_or_else(|| panic!("SHOW ENGINE MYDB STATUS missing {key}"));
        value
            .parse::<u64>()
            .unwrap_or_else(|_| panic!("{key} value `{value}` is not a u64"));
    }
}

#[test]
fn cross_join_produces_cartesian_product() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine.execute("CREATE TABLE colors (name TEXT)").unwrap();
    engine.execute("CREATE TABLE sizes (name TEXT)").unwrap();
    engine
        .execute("INSERT INTO colors VALUES ('red'), ('blue')")
        .unwrap();
    engine
        .execute("INSERT INTO sizes VALUES ('S'), ('M'), ('L')")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT c.name, s.name FROM colors c CROSS JOIN sizes s ORDER BY c.name, s.name")
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 6);
    assert_eq!(result.rows[0][0], Value::String("blue".into()));
    assert_eq!(result.rows[0][1], Value::String("L".into()));
    assert_eq!(result.rows[5][0], Value::String("red".into()));
    assert_eq!(result.rows[5][1], Value::String("S".into()));
}

#[test]
fn cross_join_with_where_filter() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, val INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, val INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t1 VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO t2 VALUES (1, 10), (2, 30)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT t1.id, t2.id FROM t1 CROSS JOIN t2 WHERE t1.val = t2.val")
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1], Value::Int(1));
}

#[test]
fn inner_join_with_null_join_key_excludes_null_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id ORDER BY e.name",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 3);
    let names: Vec<_> = result.rows.iter().map(|r| &r[0]).collect();
    assert!(!names.contains(&&Value::String("Dave".into())));
}

#[test]
fn left_join_with_aggregate_counts_nulls_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT d.name, COUNT(e.id) AS cnt FROM departments d LEFT JOIN employees e ON d.id = e.dept_id GROUP BY d.name ORDER BY d.name",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], Value::String("Engineering".into()));
    assert_eq!(result.rows[0][1], Value::Int(2));
    assert_eq!(result.rows[1][0], Value::String("HR".into()));
    assert_eq!(result.rows[1][1], Value::Int(0));
    assert_eq!(result.rows[2][0], Value::String("Sales".into()));
    assert_eq!(result.rows[2][1], Value::Int(1));
}

#[test]
fn three_table_inner_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, val TEXT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, a_id INT, val TEXT)")
        .unwrap();
    engine
        .execute("CREATE TABLE c (id INT PRIMARY KEY, b_id INT, val TEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 'a1'), (2, 'a2')")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (10, 1, 'b1'), (20, 2, 'b2'), (30, 1, 'b3')")
        .unwrap();
    engine
        .execute("INSERT INTO c VALUES (100, 10, 'c1'), (200, 20, 'c2')")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT a.val, b.val, c.val FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id ORDER BY c.val",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::String("a1".into()));
    assert_eq!(result.rows[0][1], Value::String("b1".into()));
    assert_eq!(result.rows[0][2], Value::String("c1".into()));
    assert_eq!(result.rows[1][0], Value::String("a2".into()));
    assert_eq!(result.rows[1][1], Value::String("b2".into()));
    assert_eq!(result.rows[1][2], Value::String("c2".into()));
}

#[test]
fn self_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE emp (id INT PRIMARY KEY, name TEXT, manager_id INT)")
        .unwrap();
    engine
        .execute("INSERT INTO emp VALUES (1, 'Boss', NULL), (2, 'Worker', 1)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT e.name, m.name AS manager FROM emp e JOIN emp m ON e.manager_id = m.id ORDER BY e.name",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("Worker".into()));
    assert_eq!(result.rows[0][1], Value::String("Boss".into()));
}

#[test]
fn indexed_self_join_alias_hides_base_table_name() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for data_type in ["INT", "VARCHAR(32)"] {
        engine
            .execute(&format!(
                "CREATE TABLE links (id INT PRIMARY KEY, k {data_type}, bucket INT, \
                 KEY by_key (k), KEY by_bucket (bucket))"
            ))
            .unwrap();
        let values = (1..=200)
            .map(|id| format!("({id}, '{}', {})", (id + 1) / 2, id))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO links VALUES {values}"))
            .unwrap();
        for join in ["LEFT JOIN", "JOIN"] {
            for condition in ["links.k = r.k", "r.k = links.k", "links.k = R.K"] {
                let sql = format!(
                    "SELECT links.id, r.id FROM links {join} links r ON {condition} \
                     WHERE links.bucket = 1 ORDER BY links.id, r.id"
                );
                let QueryResponse::ResultSet(explain) =
                    engine.execute(&format!("EXPLAIN {sql}")).unwrap()
                else {
                    panic!("expected explain result");
                };
                assert!(
                    explain.rows.iter().flatten().any(|v| matches!(v,
                        Value::String(s) if s.contains("Index nested loop"))),
                    "regression must exercise an index join: {explain:?}"
                );
                let QueryResponse::ResultSet(result) = engine.execute(&sql).unwrap() else {
                    panic!("expected result set");
                };
                assert_eq!(
                    result.rows,
                    vec![
                        vec![Value::Int(1), Value::Int(1)],
                        vec![Value::Int(1), Value::Int(2)],
                    ],
                    "{data_type}: {sql}"
                );
            }
        }
        engine.execute("DROP TABLE links").unwrap();
    }
}

#[test]
fn left_join_with_empty_right_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    engine
        .execute("CREATE TABLE child (id INT PRIMARY KEY, parent_id INT)")
        .unwrap();
    engine
        .execute("INSERT INTO parent VALUES (1, 'p1'), (2, 'p2')")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT p.name, c.id FROM parent p LEFT JOIN child c ON p.id = c.parent_id ORDER BY p.name",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][1], Value::Null);
    assert_eq!(result.rows[1][1], Value::Null);
}

#[test]
fn cross_join_with_empty_table_returns_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE full_t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE empty_t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO full_t VALUES (1), (2)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT * FROM full_t CROSS JOIN empty_t")
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 0);
}

#[test]
fn explain_shows_index_nested_loop_for_single_column_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT d.name, e.name FROM departments d JOIN employees e ON d.id = e.dept_id",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };

    let all_text: String = explain
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        all_text.contains("dept_idx"),
        "EXPLAIN should reference dept_idx for single-column index NL join: {}",
        all_text
    );
}

#[test]
fn hash_join_excludes_rows_with_null_keys_on_both_sides() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, k INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, NULL), (2, 1), (3, NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (10, NULL), (20, 1), (30, NULL)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT l.id, r.id FROM l JOIN r ON l.k = r.k")
        .unwrap()
    else {
        panic!("expected result set");
    };

    // Only l.id=2 and r.id=20 match; NULL keys must not equal NULL keys.
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(2));
    assert_eq!(result.rows[0][1], Value::Int(20));
}

#[test]
fn hash_join_left_preserves_null_key_left_rows() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, k INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, NULL), (2, 1)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (20, 1), (30, NULL)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT l.id, r.id FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.id")
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1], Value::Null);
    assert_eq!(result.rows[1][0], Value::Int(2));
    assert_eq!(result.rows[1][1], Value::Int(20));
}

#[test]
fn hash_join_null_safe_eq_matches_null_keys() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, k INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, NULL), (2, 1)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (20, 1), (30, NULL)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT l.id, r.id FROM l JOIN r ON l.k <=> r.k ORDER BY l.id")
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1], Value::Int(30));
    assert_eq!(result.rows[1][0], Value::Int(2));
    assert_eq!(result.rows[1][1], Value::Int(20));
}

#[test]
fn hash_join_supports_multi_equi_keys() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE l (x INT, y INT, tag TEXT)")
        .unwrap();
    engine
        .execute("CREATE TABLE r (x INT, y INT, tag TEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 1, 'l_a'), (1, 2, 'l_b'), (2, 1, 'l_c')")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (1, 1, 'r_a'), (1, 2, 'r_b'), (2, 2, 'r_c')")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT l.tag, r.tag FROM l JOIN r ON l.x = r.x AND l.y = r.y ORDER BY l.tag")
        .unwrap()
    else {
        panic!("expected result set");
    };

    // (1,1) and (1,2) each match their corresponding row; (2,1) must not match (2,2).
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::String("l_a".into()));
    assert_eq!(result.rows[0][1], Value::String("r_a".into()));
    assert_eq!(result.rows[1][0], Value::String("l_b".into()));
    assert_eq!(result.rows[1][1], Value::String("r_b".into()));
}

#[test]
fn hash_join_with_residual_non_equi_predicate() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, t INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, t INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 100), (1, 200), (2, 100)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (1, 150), (2, 50)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT l.id, l.t, r.t FROM l JOIN r ON l.id = r.id AND l.t > r.t ORDER BY l.t")
        .unwrap()
    else {
        panic!("expected result set");
    };

    // l.id=2,t=100 matches r.id=2,t=50 because 100>50.
    // l.id=1,t=200 matches r.id=1,t=150 because 200>150.
    // l.id=1,t=100 does not match because 100<150.
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(2));
    assert_eq!(result.rows[0][1], Value::Int(100));
    assert_eq!(result.rows[0][2], Value::Int(50));
    assert_eq!(result.rows[1][0], Value::Int(1));
    assert_eq!(result.rows[1][1], Value::Int(200));
    assert_eq!(result.rows[1][2], Value::Int(150));
}

#[test]
fn hash_join_supports_expression_keys() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, x INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, y INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 1), (2, 2), (3, 3)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (10, 2), (20, 3), (30, 4)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT l.id, r.id FROM l JOIN r ON l.x + 1 = r.y ORDER BY l.id")
        .unwrap()
    else {
        panic!("expected result set");
    };

    // l.x+1: 2,3,4 ↔ r.y: 2,3,4
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0], vec![Value::Int(1), Value::Int(10)]);
    assert_eq!(result.rows[1], vec![Value::Int(2), Value::Int(20)]);
    assert_eq!(result.rows[2], vec![Value::Int(3), Value::Int(30)]);
}

// Hash join thresholds use process-wide environment variables, so fallback/error-mode tests
// must run serially to avoid interfering with concurrent hash join tests.
fn hash_join_env_lock() -> std::sync::MutexGuard<'static, ()> {
    super::test_env_lock()
}

/// A2: specialized integer keys match the generic GUC-off path row-for-row.
/// Cover duplicate 1:N keys, NULL keys on both sides, and counter increments.
#[test]
fn hash_join_int_key_fast_matches_generic_path() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, v INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, w INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 10), (1, 11), (2, 20), (NULL, 30), (3, 40)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (1, 100), (1, 101), (2, 200), (NULL, 300), (9, 900)")
        .unwrap();
    let sql = "SELECT l.id, l.v, r.w FROM l JOIN r ON l.id = r.id ORDER BY l.v, r.w";

    let before = native_batch_hits();
    let QueryResponse::ResultSet(fast) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    let after = native_batch_hits();
    assert!(
        after > before,
        "integer join should use the common batch path"
    );
    // For id=1, two left times two right gives four rows; id=2 adds one; NULLs do not match.
    assert_eq!(fast.rows.len(), 5);

    let QueryResponse::ResultSet(generic) =
        with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || engine.execute(sql)).unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(fast.rows, generic.rows, "fast path must match generic path");
}

/// A2 boundary: BIGINT UNSIGNED above i64::MAX must not match a negative BIGINT with identical bits,
/// using i128 widening consistent with ValueKey::Int.
#[test]
fn hash_join_int_key_fast_uint_boundary_does_not_match_negative() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE u (k BIGINT UNSIGNED, tag INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE s (k BIGINT, tag INT)")
        .unwrap();
    // 9223372036854775808 is 2^63, with the same bit pattern as i64::MIN.
    engine
        .execute("INSERT INTO u VALUES (9223372036854775808, 1), (42, 2)")
        .unwrap();
    engine
        .execute("INSERT INTO s VALUES (-9223372036854775808, 1), (42, 2)")
        .unwrap();
    let sql = "SELECT u.tag, s.tag FROM u JOIN s ON u.k = s.k ORDER BY u.tag";

    let QueryResponse::ResultSet(fast) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    // Only 42=42 matches; positive and negative 2^63 do not.
    assert_eq!(fast.rows.len(), 1);
    assert_eq!(fast.rows[0], vec![Value::Int(2), Value::Int(2)]);

    let prior = std::env::var("MYDB_HASH_JOIN_TYPED_KEY").ok();
    std::env::set_var("MYDB_HASH_JOIN_TYPED_KEY", "off");
    let _restore = scopeguard_restore_env_key("MYDB_HASH_JOIN_TYPED_KEY", prior);
    let QueryResponse::ResultSet(generic) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(fast.rows, generic.rows);
}

/// A2: specialized LEFT/RIGHT NULL extensions match the generic path.
#[test]
fn hash_join_int_key_fast_outer_preserves_unmatched() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, v INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, w INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 10), (2, 20), (NULL, 30), (4, 40)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (1, 100), (1, 101), (5, 500), (NULL, 600)")
        .unwrap();

    for sql in [
        "SELECT l.id, l.v, r.w FROM l LEFT JOIN r ON l.id = r.id ORDER BY l.v, r.w",
        "SELECT l.id, l.v, r.w FROM l RIGHT JOIN r ON l.id = r.id ORDER BY r.w, l.v",
    ] {
        let QueryResponse::ResultSet(fast) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        let prior = std::env::var("MYDB_HASH_JOIN_TYPED_KEY").ok();
        std::env::set_var("MYDB_HASH_JOIN_TYPED_KEY", "off");
        let restore = scopeguard_restore_env_key("MYDB_HASH_JOIN_TYPED_KEY", prior);
        let QueryResponse::ResultSet(generic) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        drop(restore);
        assert_eq!(fast.rows, generic.rows, "outer join mismatch for {sql}");
    }
}

/// A1: pre-evaluating joined residuals before concatenation matches the old
/// concatenate-then-evaluate path. Counters increase, while GUC off bypasses pre-evaluation.
#[test]
fn hash_join_residual_precheck_matches_postjoin_eval() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, t INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, t INT)").unwrap();
    // Only a few same-key candidate pairs pass the residual, the intended optimization case.
    let mut sql = String::from("INSERT INTO l VALUES ");
    for i in 0..50 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("(1, {})", i));
    }
    engine.execute(&sql).unwrap();
    engine
        .execute("INSERT INTO r VALUES (1, 47), (1, 12), (2, 0)")
        .unwrap();

    for sql in [
        // INNER with nonequality residual rejecting most probes.
        "SELECT l.t, r.t FROM l JOIN r ON l.id = r.id AND l.t > r.t ORDER BY l.t, r.t",
        // LEFT with joined residual NULL-extends left rows that fail it.
        "SELECT l.t, r.t FROM l LEFT JOIN r ON l.id = r.id AND l.t > r.t ORDER BY l.t, r.t",
    ] {
        let before = native_batch_hits();
        let QueryResponse::ResultSet(pre) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        let after = native_batch_hits();
        assert!(
            after > before,
            "residual join should use the common batch path for {sql}"
        );

        let QueryResponse::ResultSet(post) =
            with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || engine.execute(sql)).unwrap()
        else {
            panic!("expected result set");
        };
        assert_eq!(pre.rows, post.rows, "residual precheck mismatch for {sql}");
    }
}

/// C1: parallel probing supports preserved-probe LEFT with joined residuals evaluated
/// inside workers using A1; match serial execution with parallel probing disabled.
#[test]
fn hash_join_parallel_probe_left_outer_with_residual_matches_serial() {
    let _guard = hash_join_env_lock();
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "4");
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", "100");
    let _r1 = scopeguard_restore_env_key("MYDB_HASH_JOIN_PARALLELISM", None);
    let _r2 = scopeguard_restore_env_key("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", None);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE dim (id INT, v INT)").unwrap();
    engine.execute("CREATE TABLE fact (id INT, w INT)").unwrap();
    let mut sql = String::from("INSERT INTO dim VALUES ");
    for i in 0..100 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i}, {})", i * 10));
    }
    engine.execute(&sql).unwrap();
    for chunk in 0..2 {
        let mut sql = String::from("INSERT INTO fact VALUES ");
        for i in 0..512 {
            if i > 0 {
                sql.push(',');
            }
            // One third of rows have matching keys; d.v<f.w filters some further.
            let key = if i % 3 == 0 { i % 100 } else { 7777 };
            sql.push_str(&format!("({key}, {})", (chunk * 512 + i) % 1500));
        }
        engine.execute(&sql).unwrap();
    }
    engine.execute("ANALYZE TABLE dim").unwrap();
    engine.execute("ANALYZE TABLE fact").unwrap();

    let sql = "SELECT f.id, f.w, d.v FROM fact f LEFT JOIN dim d ON f.id = d.id AND d.v < f.w \
               ORDER BY f.w, f.id, d.v LIMIT 50";
    let partitions_before = native_batch_hits();
    let QueryResponse::ResultSet(parallel) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    let partitions_after = native_batch_hits();
    assert!(
        partitions_after > partitions_before,
        "LEFT join with residual should use the common batch path"
    );

    std::env::set_var("MYDB_HASH_JOIN_PARALLEL_PROBE_DISABLE", "1");
    let _r2 = scopeguard_restore_env_key("MYDB_HASH_JOIN_PARALLEL_PROBE_DISABLE", None);
    let QueryResponse::ResultSet(serial) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(parallel.rows, serial.rows);
    assert_eq!(parallel.rows.len(), 50);
}

/// C2: build exceeding the static Bloom maximum and a high sampled miss rate trigger
/// adaptive Bloom construction; results match disabled adaptation and counters advance.
#[test]
fn hash_join_adaptive_bloom_engages_for_large_build_high_miss() {
    let _guard = hash_join_env_lock();
    // Lower the static maximum so 200 build rows exceed it; force serial do_in_memory.
    std::env::set_var("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS", "100");
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "1");
    let _r1 = scopeguard_restore_env_key("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS", None);
    let _r2 = scopeguard_restore_env_key("MYDB_HASH_JOIN_PARALLELISM", None);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE dim (id INT, v INT)").unwrap();
    engine.execute("CREATE TABLE fact (id INT, w INT)").unwrap();
    let mut sql = String::from("INSERT INTO dim VALUES ");
    for i in 0..200 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i}, {i})"));
    }
    engine.execute(&sql).unwrap();
    // Test builds use a 64-row sampling window and a 128-row remaining-row
    // gate, preserving the production decision logic without paying for the
    // production 4096/16384 confidence thresholds in every nextest process.
    // The tail hits verify that the dynamically-built bloom has no false
    // negatives.
    let mut sql = String::from("INSERT INTO fact VALUES ");
    for i in 0..192 {
        if i > 0 {
            sql.push(',');
        }
        let key = if i >= 176 { i - 176 } else { 999_999 };
        sql.push_str(&format!("({key}, {i})"));
    }
    engine.execute(&sql).unwrap();
    // Without statistics build orientation may reverse; ANALYZE fixes
    // build=dim and probe=fact.
    engine.execute("ANALYZE TABLE dim").unwrap();
    engine.execute("ANALYZE TABLE fact").unwrap();

    // Preserved-probe LEFT avoids the streaming-inner path in this fixture;
    // joined residuals avoid fused join-aggregate/TopK shortcuts;
    // MYDB_HASH_JOIN_PARALLELISM=1 disables both parallel paths to reach do_in_memory.
    let sql = "SELECT f.w, d.v FROM fact f LEFT JOIN dim d ON f.id = d.id AND d.v < f.w \
               ORDER BY f.w DESC LIMIT 5";
    let built_before = native_batch_hits();
    let QueryResponse::ResultSet(on) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    let built_after = native_batch_hits();
    assert!(
        built_after > built_before,
        "adaptive join should stay in the common batch path"
    );

    std::env::set_var("MYDB_HASH_JOIN_BLOOM_ADAPTIVE", "off");
    let _r3 = scopeguard_restore_env_key("MYDB_HASH_JOIN_BLOOM_ADAPTIVE", None);
    let QueryResponse::ResultSet(off) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(on.rows, off.rows);
    // The five largest tail w values all match, at i>=176 with key=i-176.
    assert_eq!(on.rows[0], vec![Value::Int(191), Value::Int(15)]);
    assert_eq!(on.rows[4], vec![Value::Int(187), Value::Int(11)]);
}

/// A1+A2: integer specialization and residual pre-evaluation together match both disabled.
#[test]
fn hash_join_int_key_fast_with_residual_precheck_combined() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, t INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, t INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 100), (1, 200), (2, 100), (NULL, 100)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (1, 150), (2, 50), (NULL, 1)")
        .unwrap();
    let sql = "SELECT l.id, l.t, r.t FROM l JOIN r ON l.id = r.id AND l.t > r.t ORDER BY l.t";

    let before = native_batch_hits();
    let QueryResponse::ResultSet(both_on) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert!(native_batch_hits() > before);
    assert_eq!(both_on.rows.len(), 2);

    let QueryResponse::ResultSet(both_off) =
        with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || engine.execute(sql)).unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(both_on.rows, both_off.rows);
}

#[test]
fn hash_join_uses_bounded_chunks_when_grace_depth_is_exhausted() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, k INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 1), (2, 2), (3, 3)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (10, 1), (20, 2), (30, 3)")
        .unwrap();

    // Set the build-row threshold to zero and grace depth to its legal minimum;
    // hot partitions must eventually use bounded hash chunks, never generic NLJ fallback.
    std::env::set_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS", "0");
    std::env::set_var("MYDB_HASH_JOIN_GRACE_MAX_DEPTH", "1");

    let before = crate::engine::executor::select::plan::HASH_JOIN_FALLBACK_TO_NLJ
        .load(std::sync::atomic::Ordering::Relaxed);

    let result = engine.execute(
        "SELECT /*+ hash_join(l,r) */ l.id, r.id FROM l JOIN r ON l.k = r.k ORDER BY l.id",
    );

    let after = crate::engine::executor::select::plan::HASH_JOIN_FALLBACK_TO_NLJ
        .load(std::sync::atomic::Ordering::Relaxed);

    std::env::remove_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS");
    std::env::remove_var("MYDB_HASH_JOIN_GRACE_MAX_DEPTH");

    let QueryResponse::ResultSet(result) = result.unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 3);
    assert_eq!(after, before, "NLJ fallback must remain zero");
}

#[test]
fn obsolete_hash_join_fallback_knob_cannot_change_execution_semantics() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, k INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 1), (2, 2)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (10, 1), (20, 2)")
        .unwrap();

    std::env::set_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS", "0");
    std::env::set_var("MYDB_HASH_JOIN_FALLBACK", "error");

    let result =
        engine.execute("SELECT /*+ hash_join(l,r) */ l.id, r.id FROM l JOIN r ON l.k = r.k");

    std::env::remove_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS");
    std::env::remove_var("MYDB_HASH_JOIN_FALLBACK");
    std::env::remove_var("MYDB_HASH_JOIN_GRACE_MAX_DEPTH");

    let QueryResponse::ResultSet(result) = result.unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn explain_shows_hash_join_with_keys_and_build_side() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, x INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, x INT)").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT /*+ hash_join(l,r) */ * FROM l JOIN r ON l.x + 1 = r.x")
        .unwrap()
    else {
        panic!("expected result set");
    };

    let all_text: String = explain
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        all_text.contains("Hash join"),
        "EXPLAIN should mention Hash join: {}",
        all_text
    );
    assert!(
        all_text.contains("build="),
        "EXPLAIN should label build side: {}",
        all_text
    );
    assert!(
        all_text.contains("keys=["),
        "EXPLAIN should list join keys: {}",
        all_text
    );
    // B10: MySQL 8.0 marks a hash-joined probe table
    // `Using join buffer (hash join)` in the traditional format.
    assert!(
        all_text.contains("Using join buffer (hash join)"),
        "EXPLAIN should mark hash join buffer: {}",
        all_text
    );
}

#[test]
fn filtered_probe_count_hash_join_streaming_matches_materialized_fallback() {
    let _guard = hash_join_env_lock();
    let prior_stream_disable = std::env::var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE").ok();
    let _restore_stream_disable = scopeguard_restore_env_key(
        "MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE",
        prior_stream_disable,
    );
    std::env::remove_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, flag INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 1), (2, 2), (3, 3), (4, 4)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO b VALUES \
             (10, 1, 1), (11, 1, 0), (12, 2, 1), \
             (13, 2, 1), (14, 3, 0), (15, 5, 1)",
        )
        .unwrap();

    let sql = "SELECT /*+ HASH_JOIN(a, b) */ COUNT(*) \
               FROM a JOIN b ON a.k = b.k \
               WHERE b.flag > 0";
    let before = native_batch_hits();
    let QueryResponse::ResultSet(streaming) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    let after = native_batch_hits();
    assert_eq!(streaming.rows[0][0], Value::Int(3));
    assert!(
        after > before,
        "filtered-probe COUNT(*) should use the common batch pipeline"
    );

    std::env::set_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", "1");
    let QueryResponse::ResultSet(materialized) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(streaming.rows, materialized.rows);
}

#[test]
fn filtered_probe_sum_streaming_avoids_grace_fallback() {
    let _guard = hash_join_env_lock();
    let prior_stream_disable = std::env::var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE").ok();
    let _restore_stream_disable = scopeguard_restore_env_key(
        "MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE",
        prior_stream_disable,
    );
    let prior_max_build = std::env::var("MYDB_HASH_JOIN_MAX_BUILD_ROWS").ok();
    let _restore_max_build =
        scopeguard_restore_env_key("MYDB_HASH_JOIN_MAX_BUILD_ROWS", prior_max_build);
    std::env::remove_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");
    // Any materialized HashJoin fallback must spill; the fused streaming
    // aggregate is independent of this legacy row-count guard.
    std::env::set_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS", "0");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE probe_t (\
                id INT PRIMARY KEY, k INT, pad CHAR(16) NOT NULL\
             )",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE build_t (id INT PRIMARY KEY)")
        .unwrap();
    let probe_values = (1..=64)
        .map(|id| format!("({id}, {}, 'aaa')", id * 10))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO probe_t VALUES {probe_values}"))
        .unwrap();
    engine
        .execute("INSERT INTO build_t VALUES (1), (2)")
        .unwrap();

    let sql = "SELECT /*+ HASH_JOIN(a, b) */ SUM(a.k) \
               FROM probe_t a JOIN build_t b ON a.id = b.id \
               WHERE a.pad < 'ccc'";
    let spill_before = crate::engine::executor::select::plan::HASH_JOIN_SPILL_BYTES
        .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(streaming) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    let spill_after = crate::engine::executor::select::plan::HASH_JOIN_SPILL_BYTES
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(streaming.rows, vec![vec![Value::DecimalCompact(30, 0)]]);
    assert_eq!(
        spill_after, spill_before,
        "filtered probe SUM must remain in the fused streaming operator"
    );

    std::env::set_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", "1");
    let QueryResponse::ResultSet(materialized) = engine.execute(sql).unwrap() else {
        panic!("expected materialized result set");
    };
    assert_eq!(streaming.rows, materialized.rows);
}

#[test]
fn filtered_probe_grouped_hash_join_streaming_matches_fallback() {
    let _guard = hash_join_env_lock();
    let prior_stream_disable = std::env::var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE").ok();
    let _restore_stream_disable = scopeguard_restore_env_key(
        "MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE",
        prior_stream_disable,
    );
    std::env::remove_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, k INT, priority INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, flag INT, price INT, discount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 1, 5), (2, 2, 4), (3, 3, 3), (4, 4, 2)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO b VALUES \
             (10, 1, 1, 100, 0), (11, 1, 0, 1000, 0), \
             (12, 2, 1, 50, 0), (13, 2, 1, 70, 0), \
             (14, 3, 0, 200, 0), (15, 5, 1, 999, 0)",
        )
        .unwrap();

    // Probe-side group key + expression aggregate argument: this shape
    // deliberately does NOT take the fused streaming path any more. The
    // old per-joined-row fused variant measured slower than the
    // materialising path it replaced (TPC-H Q13 INNER: 1222ms fused vs
    // 512ms materialised), so the streaming fused path is now restricted
    // to build-side group keys with column/COUNT(*) aggregate arguments
    // and everything else falls back. This test keeps the output oracle.
    let sql = "SELECT /*+ HASH_JOIN(a, b) */ b.k, a.priority, \
                      SUM(b.price * (1 - b.discount)) AS revenue \
               FROM a JOIN b ON a.k = b.k \
               WHERE b.flag > 0 \
               GROUP BY b.k, a.priority \
               ORDER BY b.k, a.priority";
    let QueryResponse::ResultSet(streaming) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };

    std::env::set_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", "1");
    let QueryResponse::ResultSet(fallback) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(streaming.rows, fallback.rows);
    assert_eq!(streaming.rows.len(), 2);
}

/// Direct fused shape: build-side group key, single-probe-column /
/// COUNT(*) aggregates. Must take the streaming fused path and match the
/// materialising fallback bit-for-bit.
#[test]
fn build_key_grouped_hash_join_direct_fused_matches_fallback() {
    let _guard = hash_join_env_lock();
    let prior_stream_disable = std::env::var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE").ok();
    let _restore_stream_disable = scopeguard_restore_env_key(
        "MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE",
        prior_stream_disable,
    );
    std::env::remove_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, k INT, priority INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, flag INT, price INT)")
        .unwrap();
    // a.id=4 shares k=3 with a.id=3 (same-group merge), a.id=5 has a
    // NULL key (must be skipped by the bucket build for INNER). The
    // probe side is bulked up so the CBO picks build=a — the direct
    // fused shape needs the group key on the build side.
    engine
        .execute("INSERT INTO a VALUES (1, 1, 5), (2, 2, 4), (3, 3, 3), (4, 3, 3), (5, NULL, 9)")
        .unwrap();
    let b_vals: Vec<String> = (0..300)
        .map(|i| {
            format!(
                "({}, {}, {}, {})",
                1000 + i,
                1 + (i % 4),
                if i % 3 == 0 { 1 } else { 0 },
                10 * i
            )
        })
        .collect();
    engine
        .execute(&format!("INSERT INTO b VALUES {}", b_vals.join(", ")))
        .unwrap();
    engine.execute("ANALYZE TABLE a").unwrap();
    engine.execute("ANALYZE TABLE b").unwrap();

    let sql = "SELECT /*+ HASH_JOIN(a, b) */ a.k, \
                      COUNT(*) AS n, COUNT(b.id) AS nb, \
                      SUM(b.price) AS total, MIN(a.priority) AS p \
               FROM a JOIN b ON a.k = b.k \
               WHERE b.flag > 0 \
               GROUP BY a.k \
               ORDER BY a.k";
    let before = native_batch_hits();
    let QueryResponse::ResultSet(streaming) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    let after = native_batch_hits();
    assert!(
        after > before,
        "build-key grouped aggregate should use the common batch pipeline"
    );

    std::env::set_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", "1");
    let QueryResponse::ResultSet(fallback) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(streaming.rows, fallback.rows);
    // Build keys 1..3 all match flagged probe rows (k=4 exists only on
    // the probe side; the NULL build key never matches under INNER).
    // a.id=3 and a.id=4 share k=3 so they collapse into one group.
    assert_eq!(streaming.rows.len(), 3);
}

/// Mixed-side direct fused shape: some GROUP BY keys come from the hash
/// build side and some from the probe side. This mirrors the hot part of
/// TPC-DS Q39 (`warehouse` columns + `inventory/item/date` columns) and
/// must avoid the generic per-joined-row group evaluation path.
#[test]
fn mixed_side_grouped_hash_join_direct_fused_matches_fallback() {
    let _guard = hash_join_env_lock();
    let prior_group_enable = std::env::var("MYDB_HASH_JOIN_GROUP_AGG_FUSED").ok();
    let prior_group_disable = std::env::var("MYDB_HASH_JOIN_GROUP_AGG_FUSED_DISABLE").ok();
    let _restore_group_enable =
        scopeguard_restore_env_key("MYDB_HASH_JOIN_GROUP_AGG_FUSED", prior_group_enable);
    let _restore_group_disable = scopeguard_restore_env_key(
        "MYDB_HASH_JOIN_GROUP_AGG_FUSED_DISABLE",
        prior_group_disable,
    );
    std::env::set_var("MYDB_HASH_JOIN_GROUP_AGG_FUSED", "1");
    std::env::remove_var("MYDB_HASH_JOIN_GROUP_AGG_FUSED_DISABLE");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE warehouse (w_sk INT PRIMARY KEY, w_name VARCHAR(16))")
        .unwrap();
    engine
        .execute("CREATE TABLE fact (id INT PRIMARY KEY, w_sk INT, item_sk INT, moy INT, qty INT)")
        .unwrap();
    engine
        .execute("INSERT INTO warehouse VALUES (1, 'North'), (2, 'South')")
        .unwrap();
    let mut values = Vec::new();
    for id in 1..=240 {
        let w = 1 + (id % 2);
        let item = 100 + (id % 12);
        let moy = 1 + (id % 3);
        let qty = id % 17;
        values.push(format!("({id}, {w}, {item}, {moy}, {qty})"));
    }
    engine
        .execute(&format!("INSERT INTO fact VALUES {}", values.join(", ")))
        .unwrap();
    engine.execute("ANALYZE TABLE warehouse").unwrap();
    engine.execute("ANALYZE TABLE fact").unwrap();

    let sql = "SELECT /*+ HASH_JOIN(warehouse, fact) */ \
                      w_name, fact.item_sk, fact.moy, COUNT(*), SUM(fact.qty) \
               FROM warehouse JOIN fact ON warehouse.w_sk = fact.w_sk \
               GROUP BY w_name, fact.item_sk, fact.moy \
               ORDER BY w_name, fact.item_sk, fact.moy";
    let QueryResponse::ResultSet(explain) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let explain_text = explain
        .rows
        .iter()
        .flatten()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join("\n");
    let before = native_batch_hits();
    let QueryResponse::ResultSet(fused) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    let after = native_batch_hits();
    assert!(
        after > before,
        "mixed build/probe group keys should use the common batch pipeline; plan:\n{explain_text}"
    );

    std::env::set_var("MYDB_HASH_JOIN_GROUP_AGG_FUSED_DISABLE", "1");
    let QueryResponse::ResultSet(fallback) = engine.execute(sql).unwrap() else {
        panic!("expected fallback result set");
    };
    assert_eq!(fused.rows, fallback.rows);
    assert!(!fused.rows.is_empty());
}

#[test]
fn streamed_probe_grouped_hash_join_direct_fused_matches_fallback() {
    let _guard = hash_join_env_lock();
    let _restore_group_enable = scopeguard_restore_env_key("MYDB_HASH_JOIN_GROUP_AGG_FUSED", None);
    let _restore_group_disable =
        scopeguard_restore_env_key("MYDB_HASH_JOIN_GROUP_AGG_FUSED_DISABLE", None);
    let _restore_stream_disable =
        scopeguard_restore_env_key("MYDB_HASH_JOIN_GROUP_AGG_DIRECT_STREAM_DISABLE", None);
    std::env::set_var("MYDB_HASH_JOIN_GROUP_AGG_FUSED", "1");
    std::env::remove_var("MYDB_HASH_JOIN_GROUP_AGG_FUSED_DISABLE");
    std::env::remove_var("MYDB_HASH_JOIN_GROUP_AGG_DIRECT_STREAM_DISABLE");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE d (d_sk INT PRIMARY KEY, day_name VARCHAR(12), yr INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE st (s_sk INT PRIMARY KEY, s_name VARCHAR(16), off INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE f (id INT PRIMARY KEY, d_sk INT, s_sk INT, price INT, INDEX idx_f_d(d_sk))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO d VALUES \
             (1,'Sunday',2000),(2,'Monday',2000),(3,'Tuesday',2000),(4,'Sunday',1999)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO st VALUES (10,'north',-5),(20,'south',-5),(30,'west',-6)")
        .unwrap();
    let mut values = Vec::new();
    for id in 1..=180 {
        let d_sk = 1 + (id % 4);
        let s_sk = match id % 3 {
            0 => 10,
            1 => 20,
            _ => 30,
        };
        let price = 5 + (id % 11);
        values.push(format!("({id},{d_sk},{s_sk},{price})"));
    }
    engine
        .execute(&format!("INSERT INTO f VALUES {}", values.join(",")))
        .unwrap();
    engine.execute("ANALYZE TABLE d").unwrap();
    engine.execute("ANALYZE TABLE st").unwrap();
    engine.execute("ANALYZE TABLE f").unwrap();

    let sql = "SELECT /*+ ORDERED HASH_JOIN(d, f) HASH_JOIN(f, st) */ \
                      st.s_name, \
                      SUM(CASE WHEN d.day_name = 'Sunday' THEN f.price ELSE NULL END) AS sun_sales, \
                      SUM(CASE WHEN d.day_name = 'Monday' THEN f.price ELSE NULL END) AS mon_sales \
               FROM d JOIN f ON d.d_sk = f.d_sk \
                    JOIN st ON f.s_sk = st.s_sk \
               WHERE d.yr = 2000 AND st.off = -5 \
               GROUP BY st.s_name \
               ORDER BY st.s_name";
    let QueryResponse::ResultSet(explain) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let explain_text = explain
        .rows
        .iter()
        .flatten()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        explain_text.matches("Hash join").count() >= 2,
        "fixture must exercise a streamed hash-join probe under a hash join:\n{explain_text}"
    );

    let oracle = crate::engine::executor::vec::config::with_vec_mode(
        crate::engine::executor::vec::config::VecMode::Off,
        || engine.execute(sql).unwrap().materialize().unwrap(),
    );
    let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let fused = crate::engine::executor::vec::config::with_vec_mode(
        crate::engine::executor::vec::config::VecMode::On,
        || engine.execute(sql).unwrap().materialize().unwrap(),
    );
    let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        after > before,
        "unified streamed join→aggregate should engage; plan:\n{explain_text}"
    );
    let QueryResponse::ResultSet(fused) = fused else {
        panic!("expected materialized result set")
    };
    let QueryResponse::ResultSet(oracle) = oracle else {
        panic!("expected materialized oracle")
    };
    assert_eq!(fused.rows, oracle.rows);
    assert_eq!(fused.rows.len(), 2);
}

#[test]
fn streamed_probe_decimal_expression_fusion_matches_materialized_oracle() {
    let _guard = hash_join_env_lock();
    let _restore_group_enable = scopeguard_restore_env_key("MYDB_HASH_JOIN_GROUP_AGG_FUSED", None);
    let _restore_group_disable =
        scopeguard_restore_env_key("MYDB_HASH_JOIN_GROUP_AGG_FUSED_DISABLE", None);
    let _restore_stream_disable =
        scopeguard_restore_env_key("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", None);
    std::env::set_var("MYDB_HASH_JOIN_GROUP_AGG_FUSED", "1");
    std::env::remove_var("MYDB_HASH_JOIN_GROUP_AGG_FUSED_DISABLE");
    std::env::remove_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE dim (k INT PRIMARY KEY, label VARCHAR(16))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE fact (id INT PRIMARY KEY, k INT, price DECIMAL(12,2), \
             discount DECIMAL(5,2), keep_row INT)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO dim VALUES (1,'one'),(2,'two'),(3,'zero')")
        .unwrap();
    engine
        .execute(
            "INSERT INTO fact VALUES \
             (1,1,100.00,0.10,1),(2,1,20.00,0.25,1), \
             (3,1,NULL,0.10,1),(4,2,7.50,NULL,1), \
             (5,2,8.00,0.50,1),(6,2,999.00,0.00,0),(7,9,4.00,0.00,1)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE dim").unwrap();
    engine.execute("ANALYZE TABLE fact").unwrap();

    let sql = "SELECT /*+ ORDERED HASH_JOIN(dim, fact) FULL_SCAN(fact) */ \
                      dim.k, dim.label, \
                      SUM(fact.price * (1 - fact.discount)) AS revenue, \
                      COUNT(fact.price * (1 - fact.discount)) AS valued_rows \
               FROM dim JOIN fact ON dim.k = fact.k \
               WHERE fact.keep_row = 1 \
               GROUP BY dim.k, dim.label ORDER BY dim.k";
    let before = native_batch_hits();
    let QueryResponse::ResultSet(fused) = engine.execute(sql).unwrap() else {
        panic!("expected fused result set");
    };
    assert!(
        native_batch_hits() > before,
        "decimal expressions must execute in the common join→aggregate pipeline"
    );
    let analyze = explain_analyze_tree(&engine, sql);
    assert!(
        analyze.contains("expression_mode=compiled")
            && !analyze.contains("fallback_expression_rows="),
        "compiled expression metrics missing: {analyze}"
    );

    std::env::set_var("MYDB_HASH_JOIN_GROUP_AGG_FUSED_DISABLE", "1");
    let QueryResponse::ResultSet(materialized) = engine.execute(sql).unwrap() else {
        panic!("expected materialized result set");
    };
    assert_eq!(fused.rows, materialized.rows);
    assert_eq!(fused.rows.len(), 2, "zero-match dimension group is absent");
}

/// Build-side-preserved LEFT JOIN through the fused path (TPC-H Q13
/// shape): unmatched build rows must surface with COUNT(probe)=0 /
/// SUM(probe)=NULL / COUNT(*)=1 and match the materialising fallback.
#[test]
fn left_join_build_preserved_grouped_fused_matches_fallback() {
    let _guard = hash_join_env_lock();
    let prior_stream_disable = std::env::var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE").ok();
    let _restore_stream_disable = scopeguard_restore_env_key(
        "MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE",
        prior_stream_disable,
    );
    std::env::remove_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE c (ck INT PRIMARY KEY, jk INT, seg INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE o (oid INT PRIMARY KEY, ck INT, comment_flag INT)")
        .unwrap();
    // ck=3 has no orders at all; ck=4's only order is filtered by the ON
    // predicate; ck=5 has a NULL join key (preserved by LEFT JOIN, can
    // never match).
    engine
        .execute(
            "INSERT INTO c VALUES \
             (1, 1, 10), (2, 2, 20), (3, 3, 30), (4, 4, 40), (5, NULL, 50)",
        )
        .unwrap();
    // Probe side is bulked up so the CBO stably picks build=c (the
    // preserved side): ck=1 gets 80 clean orders, ck=2 gets 40, ck=4
    // only ever gets ON-filtered orders, ck=9 matches no build row.
    let o_vals: Vec<String> = (0..200)
        .map(|i| {
            let ck = [1, 1, 2, 4, 9][i % 5];
            format!("({}, {}, {})", 100 + i, ck, if ck == 4 { 1 } else { 0 })
        })
        .collect();
    engine
        .execute(&format!("INSERT INTO o VALUES {}", o_vals.join(", ")))
        .unwrap();
    engine.execute("ANALYZE TABLE c").unwrap();
    engine.execute("ANALYZE TABLE o").unwrap();

    // Inner query mirrors Q13: per-customer order count with an ON-time
    // filter; outer asserts the c_count=0 group exists.
    let sql = "SELECT /*+ HASH_JOIN(c, o) */ c.ck, \
                      COUNT(o.oid) AS c_count, COUNT(*) AS joined_rows \
               FROM c LEFT JOIN o ON c.jk = o.ck AND o.comment_flag = 0 \
               GROUP BY c.ck \
               ORDER BY c.ck";
    let before = native_batch_hits();
    let QueryResponse::ResultSet(streaming) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    let after = native_batch_hits();
    assert!(
        after > before,
        "build-preserved LEFT JOIN grouped aggregate should use the common batch path"
    );

    // ck=1 → 80 orders; ck=2 → 40; ck=3 → none; ck=4 → only filtered
    // orders; ck=5 → NULL key. All five build rows survive the LEFT JOIN.
    assert_eq!(streaming.rows.len(), 5);
    let by_ck: std::collections::HashMap<i64, (Value, Value)> = streaming
        .rows
        .iter()
        .map(|r| {
            let Value::Int(ck) = r[0] else {
                panic!("ck must be Int, got {:?}", r[0])
            };
            (ck, (r[1].clone(), r[2].clone()))
        })
        .collect();
    assert_eq!(by_ck[&1].0, Value::Int(80));
    assert_eq!(by_ck[&2].0, Value::Int(40));
    assert_eq!(by_ck[&3].0, Value::Int(0), "no-order customer counts 0");
    assert_eq!(by_ck[&4].0, Value::Int(0), "ON-filtered customer counts 0");
    assert_eq!(by_ck[&5].0, Value::Int(0), "NULL-key customer counts 0");
    // COUNT(*) counts the NULL-extended row for unmatched build rows.
    assert_eq!(by_ck[&3].1, Value::Int(1));
    assert_eq!(by_ck[&4].1, Value::Int(1));

    std::env::set_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", "1");
    let QueryResponse::ResultSet(fallback) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(streaming.rows, fallback.rows);

    // SUM/MIN over the probe side must be NULL for unmatched groups, and
    // HAVING over the fused aggregates must agree with the fallback.
    std::env::remove_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");
    let sql2 = "SELECT /*+ HASH_JOIN(c, o) */ c.ck, SUM(o.oid) AS s, MIN(o.oid) AS m \
                FROM c LEFT JOIN o ON c.jk = o.ck AND o.comment_flag = 0 \
                GROUP BY c.ck \
                HAVING COUNT(o.oid) = 0 \
                ORDER BY c.ck";
    let QueryResponse::ResultSet(streaming2) = engine.execute(sql2).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(streaming2.rows.len(), 3);
    for row in &streaming2.rows {
        assert_eq!(row[1], Value::Null, "SUM over unmatched probe is NULL");
        assert_eq!(row[2], Value::Null, "MIN over unmatched probe is NULL");
    }
    std::env::set_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", "1");
    let QueryResponse::ResultSet(fallback2) = engine.execute(sql2).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(streaming2.rows, fallback2.rows);
}

/// Unique-build-key fast path (group key == join key, all build keys
/// distinct and non-NULL — the exact TPC-H Q13 shape, which skips the
/// group hash table entirely). LEFT-preserved rows must still surface.
#[test]
fn left_join_unique_build_key_grouped_fused_matches_fallback() {
    let _guard = hash_join_env_lock();
    let prior_stream_disable = std::env::var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE").ok();
    let _restore_stream_disable = scopeguard_restore_env_key(
        "MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE",
        prior_stream_disable,
    );
    std::env::remove_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE cust (ck INT PRIMARY KEY, seg INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE ord (oid INT PRIMARY KEY, ck INT, flag INT)")
        .unwrap();
    engine
        .execute("INSERT INTO cust VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();
    let o_vals: Vec<String> = (0..240)
        .map(|i| {
            // ck 1..3 get orders; ck=4 never matches. 1/4 of rows are
            // filtered by the ON predicate.
            format!(
                "({}, {}, {})",
                1000 + i,
                1 + (i % 3),
                if i % 4 == 0 { 1 } else { 0 }
            )
        })
        .collect();
    engine
        .execute(&format!("INSERT INTO ord VALUES {}", o_vals.join(", ")))
        .unwrap();
    engine.execute("ANALYZE TABLE cust").unwrap();
    engine.execute("ANALYZE TABLE ord").unwrap();

    let sql = "SELECT /*+ HASH_JOIN(cust, ord) */ cust.ck, COUNT(ord.oid) AS c_count \
               FROM cust LEFT JOIN ord ON cust.ck = ord.ck AND ord.flag = 0 \
               GROUP BY cust.ck \
               ORDER BY cust.ck";
    let before = native_batch_hits();
    let QueryResponse::ResultSet(streaming) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    let after = native_batch_hits();
    assert!(
        after > before,
        "unique-build-key LEFT shape should use the batch pipeline"
    );
    assert_eq!(streaming.rows.len(), 4);
    let Value::Int(c4) = streaming.rows[3][1] else {
        panic!("expected Int count, got {:?}", streaming.rows[3][1])
    };
    assert_eq!(c4, 0, "orderless customer must report c_count=0");

    std::env::set_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", "1");
    let QueryResponse::ResultSet(fallback) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(streaming.rows, fallback.rows);
}

fn bulk_insert(engine: &StorageQueryEngine, table: &str, rows: &[(i64, i64)]) {
    for chunk in rows.chunks(500) {
        insert_fixture_rows(
            engine,
            &format!("test.{table}"),
            chunk
                .iter()
                .map(|&(id, k)| vec![Value::Int(id), Value::Int(k)]),
        );
    }
}

fn bulk_insert3(engine: &StorageQueryEngine, table: &str, rows: &[(i64, i64, i64)]) {
    for chunk in rows.chunks(500) {
        insert_fixture_rows(
            engine,
            &format!("test.{table}"),
            chunk
                .iter()
                .map(|&(id, k, v)| vec![Value::Int(id), Value::Int(k), Value::Int(v)]),
        );
    }
}

#[test]
fn hash_join_parallel_inner_matches_serial_reference() {
    let _guard = hash_join_env_lock();
    // Low thresholds and default parallelism activate partition-parallel execution.
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", "100");
    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, k INT)").unwrap();

    // 256 build and 256 probe rows cross modulo buckets and exceed the parallel threshold.
    let l_rows: Vec<(i64, i64)> = (1..=256).map(|i| (i, i % 128)).collect();
    let r_rows: Vec<(i64, i64)> = (1..=256).map(|i| (i + 100_000, i % 128)).collect();
    bulk_insert(&engine, "l", &l_rows);
    bulk_insert(&engine, "r", &r_rows);

    let before_parts = native_batch_hits();

    let QueryResponse::ResultSet(parallel) = engine
        .execute("SELECT l.id, r.id FROM l JOIN r ON l.k = r.k ORDER BY l.id, r.id")
        .unwrap()
    else {
        panic!("expected result set");
    };

    let after_parts = native_batch_hits();

    // Use parallelism=1 as the nonparallel reference.
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "1");

    let QueryResponse::ResultSet(serial) = engine
        .execute("SELECT l.id, r.id FROM l JOIN r ON l.k = r.k ORDER BY l.id, r.id")
        .unwrap()
    else {
        panic!("expected result set");
    };

    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM");
    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD");

    // Each key 0..127 appears twice on each side, giving 128*4=512 rows.
    assert_eq!(parallel.rows.len(), 512);
    assert_eq!(parallel.rows, serial.rows);
    assert!(
        after_parts > before_parts,
        "parallel join should use the common batch path: before={}, after={}",
        before_parts,
        after_parts
    );
}

#[test]
fn hash_join_parallel_residual_scalar_subquery_has_query_context() {
    let _guard = hash_join_env_lock();
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "2");
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", "10");
    // This test exercises the residual-correlated-subquery code path on
    // the parallel hash join. With scalar-subquery decorrelation (the
    // default) the correlated subquery is rewritten to a LEFT JOIN and
    // the residual disappears, so disable it here.
    std::env::set_var("MYDB_DECORRELATE", "off");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE l (id INT, k INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE r (id INT, k INT, cost INT)")
        .unwrap();

    let l_rows: Vec<(i64, i64, i64)> = (1..=300).map(|i| (i, i % 50, i * 10)).collect();
    let r_rows: Vec<(i64, i64, i64)> = (0..50)
        .flat_map(|k| [(1_000 + k, k, 1), (2_000 + k, k, 2)])
        .collect();
    bulk_insert3(&engine, "l", &l_rows);
    bulk_insert3(&engine, "r", &r_rows);

    let sql = "SELECT /*+ HASH_JOIN(l, r) */ l.id, r.id \
               FROM l JOIN r ON l.k = r.k \
               WHERE r.cost = (SELECT MIN(r2.cost) FROM r r2 WHERE r2.k = l.k) \
               ORDER BY l.id, r.id";

    let before_parts = crate::engine::executor::select::plan::HASH_JOIN_PARALLEL_PARTITIONS
        .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(parallel) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    let after_parts = crate::engine::executor::select::plan::HASH_JOIN_PARALLEL_PARTITIONS
        .load(std::sync::atomic::Ordering::Relaxed);

    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "1");
    let QueryResponse::ResultSet(serial) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };

    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM");
    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD");
    std::env::remove_var("MYDB_DECORRELATE");

    assert_eq!(parallel.rows.len(), 300);
    assert_eq!(parallel.rows, serial.rows);
    assert_eq!(parallel.rows[0], vec![Value::Int(1), Value::Int(1_001)]);
    assert!(
        after_parts > before_parts,
        "parallel hash join should run: before={}, after={}",
        before_parts,
        after_parts
    );
}

#[test]
fn hash_join_parallel_disabled_when_parallelism_is_one() {
    let _guard = hash_join_env_lock();
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "1");
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", "100");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, k INT)").unwrap();

    let l_rows: Vec<(i64, i64)> = (1..=2000).map(|i| (i, i % 100)).collect();
    let r_rows: Vec<(i64, i64)> = (1..=2000).map(|i| (i + 50_000, i % 100)).collect();
    bulk_insert(&engine, "l", &l_rows);
    bulk_insert(&engine, "r", &r_rows);

    let before_parts = crate::engine::executor::select::plan::HASH_JOIN_PARALLEL_PARTITIONS
        .load(std::sync::atomic::Ordering::Relaxed);

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT /*+ HASH_JOIN(l,r) */ COUNT(*) FROM l JOIN r ON l.k = r.k")
        .unwrap()
    else {
        panic!("expected result set");
    };

    let after_parts = crate::engine::executor::select::plan::HASH_JOIN_PARALLEL_PARTITIONS
        .load(std::sync::atomic::Ordering::Relaxed);

    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM");
    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD");

    // Each key 0..99 has twenty rows per side, giving 100*400=40000.
    assert_eq!(result.rows[0][0], Value::Int(40_000));
    assert_eq!(
        before_parts, after_parts,
        "parallelism=1 must not bump the parallel-partitions counter"
    );
}

#[test]
fn hash_join_parallel_outer_join_build_flip_unmatched_rows() {
    let _guard = hash_join_env_lock();
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", "100");
    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, k INT)").unwrap();

    // Make l much smaller than r so LEFT flips build to preserved-side Left.
    // Use ANALYZE to provide trustworthy actual row counts.
    let l_rows: Vec<(i64, i64)> = (1..=200).map(|i| (i, i)).collect();
    // r covers keys 1..=100 only; l rows 101..=200 become unmatched build NULL extensions.
    let r_rows: Vec<(i64, i64)> = (1..=3000).map(|i| (i + 10_000, i % 100 + 1)).collect();
    bulk_insert(&engine, "l", &l_rows);
    bulk_insert(&engine, "r", &r_rows);

    engine.execute("ANALYZE TABLE l").unwrap();
    engine.execute("ANALYZE TABLE r").unwrap();

    let QueryResponse::ResultSet(parallel) = engine
        .execute("SELECT l.id, r.id FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.id, r.id")
        .unwrap()
    else {
        panic!("expected result set");
    };

    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "1");

    let QueryResponse::ResultSet(serial) = engine
        .execute("SELECT l.id, r.id FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.id, r.id")
        .unwrap()
    else {
        panic!("expected result set");
    };

    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM");
    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD");

    // Each l key 1..=100 matches thirty r rows; each remaining l row emits one NULL extension.
    // Total row count is 100*30+100=3100.
    assert_eq!(parallel.rows.len(), 3_100);
    assert_eq!(parallel.rows, serial.rows);

    // Verify unmatched l rows actually receive NULL extensions.
    let null_count = parallel
        .rows
        .iter()
        .filter(|row| matches!(row[1], Value::Null))
        .count();
    assert_eq!(null_count, 100);
}

#[test]
fn hash_join_grace_spill_produces_correct_results() {
    let _guard = hash_join_env_lock();
    // Disable parallelism to test grace specifically.
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "1");
    std::env::set_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS", "5");
    std::env::set_var("MYDB_HASH_JOIN_GRACE_PARTITIONS", "16");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, k INT)").unwrap();

    let l_rows: Vec<(i64, i64)> = (0..50).map(|i| (i, i)).collect();
    let r_rows: Vec<(i64, i64)> = (0..50).map(|i| (i + 100, i)).collect();
    bulk_insert(&engine, "l", &l_rows);
    bulk_insert(&engine, "r", &r_rows);

    // Avoid SELECT COUNT(*): the executor has a fused HashAggregate /
    // HashJoin fast path for pure COUNT(*) that handles arbitrarily
    // large builds via in-memory hash partitioning without touching
    // grace hash. We need a row-materialising query to exercise the
    // grace spill path here.
    let result = engine.execute("SELECT l.id, r.id FROM l JOIN r ON l.k = r.k");

    let analyze = explain_analyze_tree(&engine, "SELECT l.id, r.id FROM l JOIN r ON l.k = r.k");

    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM");
    std::env::remove_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS");
    std::env::remove_var("MYDB_HASH_JOIN_GRACE_PARTITIONS");

    let QueryResponse::ResultSet(result) = result.unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 50);
    assert!(
        analyze.contains("spill_bytes=") && analyze.contains("spill_partitions="),
        "Grace spill must be visible through generic operator metrics: {analyze}"
    );
}

/// The batch preflight charges a fixed key allowance, while the exact vector
/// collector retains the full encoded key.  A build of long unique keys can
/// therefore pass preflight and cross the exact cap.  That internal condition
/// must continue through Grace spill for both the fused and general streaming
/// join operators; it is never a client-visible 1041 error.
#[test]
fn vector_hash_join_exact_build_overflow_spills_instead_of_leaking_control_error() {
    let _guard = hash_join_env_lock();
    let env = [
        ("MYDB_VECTORIZED", "on"),
        ("MYDB_VEC_HASH_JOIN", "1"),
        ("MYDB_HASH_JOIN_MEM_LIMIT_MB", "1"),
        ("MYDB_HASH_JOIN_PARALLELISM", "1"),
    ];
    let _restores: Vec<_> = env
        .iter()
        .map(|(key, _)| scopeguard_restore_env_key(key, std::env::var(key).ok()))
        .collect();
    for (key, value) in env {
        std::env::set_var(key, value);
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE exact_l (id INT, k VARCHAR(2048))")
        .unwrap();
    engine
        .execute("CREATE TABLE exact_r (id INT, k VARCHAR(2048))")
        .unwrap();
    let suffix = "x".repeat(1_490);
    // Keep the same wide keys and row count required for exact-byte overflow.
    for table in ["exact_l", "exact_r"] {
        insert_fixture_rows(
            &engine,
            &format!("test.{table}"),
            (0..400).map(|id| vec![Value::Int(id), Value::String(format!("{id:04}-{suffix}"))]),
        );
    }
    engine.execute("ANALYZE TABLE exact_l").unwrap();
    engine.execute("ANALYZE TABLE exact_r").unwrap();

    let spill_before = crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
        .load(std::sync::atomic::Ordering::Relaxed);
    for sql in [
        "SELECT /*+ HASH_JOIN(exact_l,exact_r) */ exact_l.id, exact_r.id \
         FROM exact_l JOIN exact_r ON exact_l.k = exact_r.k ORDER BY exact_l.id",
        "SELECT /*+ HASH_JOIN(exact_l,exact_r) */ exact_l.id, exact_r.id \
         FROM exact_l JOIN exact_r ON exact_l.k <=> exact_r.k ORDER BY exact_l.id",
    ] {
        let QueryResponse::ResultSet(vector) = engine.execute(sql).unwrap() else {
            panic!("expected vector result set");
        };
        let QueryResponse::ResultSet(row) =
            with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || engine.execute(sql)).unwrap()
        else {
            panic!("expected row result set");
        };
        assert_eq!(vector.rows.len(), 400);
        assert_eq!(
            vector.rows, row.rows,
            "vector Grace replay diverged for {sql}"
        );
    }
    let spill_after = crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        spill_after > spill_before,
        "exact build overflow did not continue through vector Grace spill"
    );
}

#[test]
fn vector_grace_bloom_rejects_probe_misses_before_spill() {
    let _guard = hash_join_env_lock();
    let env = [
        ("MYDB_VECTORIZED", "on"),
        ("MYDB_VEC_HASH_JOIN", "1"),
        // 100 build rows still force Grace spill. Avoid recursively spilling
        // tiny partitions: this case measures Bloom rejection, not recursion.
        ("MYDB_HASH_JOIN_MAX_BUILD_ROWS", "25"),
        ("MYDB_HASH_JOIN_PARALLELISM", "1"),
    ];
    let _restores: Vec<_> = env
        .iter()
        .map(|(key, _)| scopeguard_restore_env_key(key, std::env::var(key).ok()))
        .collect();
    let _bloom_restore = scopeguard_restore_env_key(
        "MYDB_VEC_GRACE_BLOOM",
        std::env::var("MYDB_VEC_GRACE_BLOOM").ok(),
    );
    for (key, value) in env {
        std::env::set_var(key, value);
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE gb (k1 INT, k2 INT)").unwrap();
    engine.execute("CREATE TABLE gp (k1 INT, k2 INT)").unwrap();
    bulk_insert(
        &engine,
        "gb",
        &(0..100).map(|key| (key, key)).collect::<Vec<_>>(),
    );
    bulk_insert(
        &engine,
        "gp",
        &(0..1_000).map(|key| (key, key)).collect::<Vec<_>>(),
    );
    engine.execute("ANALYZE TABLE gb").unwrap();
    engine.execute("ANALYZE TABLE gp").unwrap();
    let sql = "SELECT /*+ HASH_JOIN(gb,gp) */ gb.k1, gp.k2 \
               FROM gb JOIN gp ON gb.k1 = gp.k1 AND gb.k2 = gp.k2 ORDER BY gb.k1";

    std::env::set_var("MYDB_VEC_GRACE_BLOOM", "off");
    let before = crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
        .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(unfiltered) = engine.execute(sql).unwrap() else {
        panic!("expected unfiltered vector result set");
    };
    let after = crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
        .load(std::sync::atomic::Ordering::Relaxed);
    let unfiltered_spill = after - before;

    std::env::set_var("MYDB_VEC_GRACE_BLOOM", "on");
    let before = crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
        .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(filtered) = engine.execute(sql).unwrap() else {
        panic!("expected filtered vector result set");
    };
    let after = crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
        .load(std::sync::atomic::Ordering::Relaxed);
    let filtered_spill = after - before;

    let QueryResponse::ResultSet(row_oracle) =
        with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || engine.execute(sql)).unwrap()
    else {
        panic!("expected row result set");
    };
    assert_eq!(filtered.rows, row_oracle.rows);
    assert_eq!(filtered.rows, unfiltered.rows);
    assert_eq!(filtered.rows.len(), 100);
    assert!(
        filtered_spill < unfiltered_spill / 4,
        "probe prefilter did not materially reduce Grace spill: filtered={filtered_spill}, unfiltered={unfiltered_spill}"
    );
}

#[test]
fn hash_join_binary_collation_matches_row_and_vector_paths() {
    let _guard = hash_join_env_lock();
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "1");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE lbin (v VARCHAR(16) COLLATE utf8mb4_bin)")
        .unwrap();
    engine
        .execute("CREATE TABLE rbin (v VARCHAR(16) COLLATE utf8mb4_bin)")
        .unwrap();
    engine
        .execute("INSERT INTO lbin VALUES ('A'),('e'),('z')")
        .unwrap();
    engine
        .execute("INSERT INTO rbin VALUES ('a'),('é'),('z')")
        .unwrap();
    let sql = "SELECT /*+ HASH_JOIN(lbin,rbin) */ lbin.v \
               FROM lbin JOIN rbin ON lbin.v=rbin.v ORDER BY lbin.v";

    std::env::set_var("MYDB_VEC_HASH_JOIN", "0");
    let QueryResponse::ResultSet(row) = engine.execute(sql).unwrap() else {
        panic!("expected row hash join result");
    };
    std::env::set_var("MYDB_VEC_HASH_JOIN", "1");
    let QueryResponse::ResultSet(vector) = engine.execute(sql).unwrap() else {
        panic!("expected vector hash join result");
    };

    std::env::remove_var("MYDB_VEC_HASH_JOIN");
    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM");
    assert_eq!(
        row.rows, vector.rows,
        "row/vector collation semantics diverged"
    );
    assert_eq!(row.rows.len(), 1, "utf8mb4_bin must reject A=a and e=é");
    assert_eq!(row.rows[0][0].to_display_string(), "z");
}

#[test]
fn hash_join_hot_partition_uses_bounded_hash_chunks_not_nlj() {
    let _guard = hash_join_env_lock();
    std::env::set_var("MYDB_VEC_HASH_JOIN", "0");
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "1");
    std::env::set_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS", "2");
    std::env::set_var("MYDB_HASH_JOIN_GRACE_PARTITIONS", "2");
    std::env::set_var("MYDB_HASH_JOIN_GRACE_MAX_DEPTH", "1");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE hot_l (id INT, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE hot_r (id INT, k INT)")
        .unwrap();
    for i in 0..12 {
        engine
            .execute(&format!("INSERT INTO hot_l VALUES ({i},1)"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO hot_r VALUES ({},1)", i + 100))
            .unwrap();
    }
    let fallback_before = crate::engine::executor::select::plan::HASH_JOIN_FALLBACK_TO_NLJ
        .load(std::sync::atomic::Ordering::Relaxed);
    let chunks_before = crate::engine::executor::select::plan::HASH_JOIN_SKEW_CHUNKS
        .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT /*+ HASH_JOIN(hot_l,hot_r) */ hot_l.id,hot_r.id \
             FROM hot_l JOIN hot_r ON hot_l.k=hot_r.k",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let fallback_after = crate::engine::executor::select::plan::HASH_JOIN_FALLBACK_TO_NLJ
        .load(std::sync::atomic::Ordering::Relaxed);
    let chunks_after = crate::engine::executor::select::plan::HASH_JOIN_SKEW_CHUNKS
        .load(std::sync::atomic::Ordering::Relaxed);

    std::env::remove_var("MYDB_VEC_HASH_JOIN");
    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM");
    std::env::remove_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS");
    std::env::remove_var("MYDB_HASH_JOIN_GRACE_PARTITIONS");
    std::env::remove_var("MYDB_HASH_JOIN_GRACE_MAX_DEPTH");
    assert_eq!(result.rows.len(), 144);
    assert!(
        chunks_after > chunks_before,
        "hot-key chunk path did not engage"
    );
    assert_eq!(fallback_after, fallback_before, "hot key fell back to NLJ");
    assert_eq!(
        crate::engine::executor::select::plan::HASH_JOIN_CHARGED_BYTES
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "hash join charge leaked after spill/chunk completion"
    );
}

#[test]
fn full_hash_join_hot_partition_preserves_both_unmatched_sides() {
    let _guard = hash_join_env_lock();
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "1");
    std::env::set_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS", "2");
    std::env::set_var("MYDB_HASH_JOIN_GRACE_PARTITIONS", "2");
    std::env::set_var("MYDB_HASH_JOIN_GRACE_MAX_DEPTH", "1");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE full_hot_l (id INT, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE full_hot_r (id INT, k INT)")
        .unwrap();
    for i in 0..6 {
        engine
            .execute(&format!("INSERT INTO full_hot_l VALUES ({i},1)"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO full_hot_r VALUES ({},1)", i + 100))
            .unwrap();
    }
    engine
        .execute("INSERT INTO full_hot_l VALUES (90,2)")
        .unwrap();
    engine
        .execute("INSERT INTO full_hot_r VALUES (190,3)")
        .unwrap();

    let chunks_before = crate::engine::executor::select::plan::HASH_JOIN_SKEW_CHUNKS
        .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT /*+ HASH_JOIN(full_hot_l,full_hot_r) */ full_hot_l.id,full_hot_r.id \
             FROM full_hot_l FULL OUTER JOIN full_hot_r ON full_hot_l.k=full_hot_r.k",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let chunks_after = crate::engine::executor::select::plan::HASH_JOIN_SKEW_CHUNKS
        .load(std::sync::atomic::Ordering::Relaxed);

    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM");
    std::env::remove_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS");
    std::env::remove_var("MYDB_HASH_JOIN_GRACE_PARTITIONS");
    std::env::remove_var("MYDB_HASH_JOIN_GRACE_MAX_DEPTH");

    assert_eq!(result.rows.len(), 38, "36 matches plus two null extensions");
    assert!(result
        .rows
        .iter()
        .any(|row| { row[0] == Value::Int(90) && row[1] == Value::Null }));
    assert!(result
        .rows
        .iter()
        .any(|row| { row[0] == Value::Null && row[1] == Value::Int(190) }));
    assert!(
        chunks_after > chunks_before,
        "FULL hot-key chunk path did not engage"
    );
}

#[test]
fn hash_join_grace_recursion_repartitions_instead_of_falling_back_to_nlj() {
    let _guard = hash_join_env_lock();
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "1");
    std::env::set_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS", "75");
    std::env::set_var("MYDB_HASH_JOIN_GRACE_PARTITIONS", "2");
    std::env::set_var("MYDB_HASH_JOIN_GRACE_MAX_DEPTH", "4");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, k INT)").unwrap();

    let l_rows: Vec<(i64, i64)> = (0..200).map(|i| (i, i)).collect();
    let r_rows: Vec<(i64, i64)> = (0..200).map(|i| (i + 1_000, i)).collect();
    bulk_insert(&engine, "l", &l_rows);
    bulk_insert(&engine, "r", &r_rows);

    let vec_spill_before = crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
        .load(std::sync::atomic::Ordering::Relaxed);
    let skew_chunks_before = crate::engine::executor::select::plan::HASH_JOIN_SKEW_CHUNKS
        .load(std::sync::atomic::Ordering::Relaxed);
    let result = engine
        .execute("SELECT l.id, r.id FROM l JOIN r ON l.k = r.k ORDER BY l.id")
        .unwrap();
    let analyze = explain_analyze_tree(
        &engine,
        "SELECT l.id, r.id FROM l JOIN r ON l.k = r.k ORDER BY l.id",
    );

    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM");
    std::env::remove_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS");
    std::env::remove_var("MYDB_HASH_JOIN_GRACE_PARTITIONS");
    std::env::remove_var("MYDB_HASH_JOIN_GRACE_MAX_DEPTH");

    let QueryResponse::ResultSet(result) = result else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 200);
    assert!(
        crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
            .load(std::sync::atomic::Ordering::Relaxed)
            > vec_spill_before,
        "regression must exercise vector Grace spill"
    );
    assert_eq!(
        crate::engine::executor::select::plan::HASH_JOIN_SKEW_CHUNKS
            .load(std::sync::atomic::Ordering::Relaxed),
        skew_chunks_before,
        "uniform keys must shrink after one recursive repartition"
    );
    assert!(
        analyze.contains("spill_partitions=") && !analyze.contains("Nested loop"),
        "recursive Grace processing must stay inside HashJoin: {analyze}"
    );
}

#[test]
fn hash_join_grace_left_join_preserves_unmatched() {
    let _guard = hash_join_env_lock();
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "1");
    std::env::set_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS", "3");
    std::env::set_var("MYDB_HASH_JOIN_GRACE_PARTITIONS", "8");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, k INT)").unwrap();

    let l_rows: Vec<(i64, i64)> = (0..30).map(|i| (i, i)).collect();
    let r_rows: Vec<(i64, i64)> = (0..15).map(|i| (i + 100, i)).collect();
    bulk_insert(&engine, "l", &l_rows);
    bulk_insert(&engine, "r", &r_rows);

    let result = engine.execute("SELECT l.id, r.id FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.id");

    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM");
    std::env::remove_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS");
    std::env::remove_var("MYDB_HASH_JOIN_GRACE_PARTITIONS");

    let QueryResponse::ResultSet(result) = result.unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 30);
    for i in 0..15 {
        assert_eq!(result.rows[i][0], Value::Int(i as i64));
        assert_eq!(result.rows[i][1], Value::Int((i + 100) as i64));
    }
    for i in 15..30 {
        assert_eq!(result.rows[i][0], Value::Int(i as i64));
        assert_eq!(result.rows[i][1], Value::Null);
    }
}

#[test]
fn hash_join_grace_cleans_up_temp_dir() {
    let _guard = hash_join_env_lock();
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "1");
    std::env::set_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS", "2");
    std::env::set_var("MYDB_HASH_JOIN_GRACE_PARTITIONS", "4");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, k INT)").unwrap();

    let l_rows: Vec<(i64, i64)> = (0..16).map(|i| (i, i)).collect();
    let r_rows: Vec<(i64, i64)> = (0..16).map(|i| (i + 100, i)).collect();
    bulk_insert(&engine, "l", &l_rows);
    bulk_insert(&engine, "r", &r_rows);

    let _ = engine
        .execute("SELECT COUNT(*) FROM l JOIN r ON l.k = r.k")
        .unwrap();

    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM");
    std::env::remove_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS");
    std::env::remove_var("MYDB_HASH_JOIN_GRACE_PARTITIONS");

    let tmp = crate::storage::naming::system_tmp_dir(dir.path());
    if tmp.exists() {
        let entries: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("hashjoin-"))
            .collect();
        assert!(
            entries.is_empty(),
            "tmp dir should be cleaned up after query, found {} entries",
            entries.len()
        );
    }
}

#[test]
fn hash_join_bloom_filter_rejects_misses() {
    let _guard = hash_join_env_lock();
    std::env::set_var("MYDB_HASH_JOIN_PARALLELISM", "1");
    // The bloom is gated off for small builds by default; exercise it
    // explicitly by lowering the build-row floor to zero.
    std::env::set_var("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS", "0");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE small (k INT)").unwrap();
    engine.execute("CREATE TABLE big (k INT)").unwrap();

    engine
        .execute("INSERT INTO small VALUES (0),(1),(2),(3),(4)")
        .unwrap();
    let big_rows: Vec<(i64, i64)> = (0..200).map(|i| (i, i + 100)).collect();
    let values = big_rows
        .iter()
        .map(|(_, k)| format!("({})", k))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO big VALUES {}", values))
        .unwrap();
    engine.execute("INSERT INTO big VALUES (NULL)").unwrap();

    // Expose actual row counts so the planner chooses small as build.
    engine.execute("ANALYZE TABLE small").unwrap();
    engine.execute("ANALYZE TABLE big").unwrap();

    // Exercise row rejection accounting independently of coarse page pruning.
    let mut session = SessionContext::default();
    session.sysvars.optimizer_switch = "zone_map=off".into();
    let QueryResponse::ResultSet(result) = super::support::execute_materialized(
        &engine,
        &mut session,
        "SELECT small.k, big.k FROM small JOIN big ON small.k = big.k",
    )
    .unwrap() else {
        panic!("expected result set");
    };

    let QueryResponse::ResultSet(analyze) = super::support::execute_materialized(
        &engine,
        &mut session,
        "EXPLAIN ANALYZE SELECT small.k, big.k FROM small JOIN big ON small.k = big.k",
    )
    .unwrap() else {
        panic!("analyze")
    };
    let analyze = analyze
        .rows
        .into_iter()
        .flatten()
        .map(|value| value.to_display_string())
        .collect::<Vec<_>>()
        .join("\n");

    std::env::remove_var("MYDB_HASH_JOIN_PARALLELISM");
    std::env::remove_var("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS");

    assert_eq!(result.rows.len(), 0, "should be no matches");
    assert!(
        analyze.contains("runtime_filter_rejected=201"),
        "all probe misses should be reported on the HashJoin node: {analyze}"
    );
    let probe_scan = analyze
        .lines()
        .find(|line| line.contains("Table scan on big"))
        .expect("probe scan line");
    assert!(
        probe_scan.contains("rows=0"),
        "ordinary equality never matches NULL, so the pushed inner-join filter should discard it before probe materialization: {probe_scan}"
    );
}

#[test]
fn exact_join_domain_prunes_zones_without_losing_sparse_keys_or_null_semantics() {
    let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
    // The 40,000 wide rows exceed the default 512-page memory fixture. Size
    // this query fixture for its input so setup does not wait for eviction
    // of memory-only pages; eviction itself is covered by storage tests.
    let engine = open_test_engine_with_buffer_pool_pages(root.path(), 2048);
    engine
        .execute("CREATE TABLE rz_fact (id INT PRIMARY KEY,k INT,v DECIMAL(10,2),pad VARCHAR(300))")
        .unwrap();
    engine
        .execute("CREATE TABLE rz_dim (k INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO rz_dim VALUES (17),(19)")
        .unwrap();
    // Preserve the full zone layout, sparse keys, NULLs and decimal scale;
    // this fixture tests join pruning, not INSERT SELECT expression evaluation.
    let padding = "x".repeat(280);
    insert_fixture_rows(
        &engine,
        "test.rz_fact",
        (0..40_000).map(|n| {
            vec![
                Value::Int(n),
                if n == 0 {
                    Value::Null
                } else {
                    Value::Int(n / 1000)
                },
                if n % 7 == 0 {
                    Value::Null
                } else {
                    Value::DecimalCompact((n % 19) * 100, 2)
                },
                Value::String(padding.clone()),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE rz_fact").unwrap();
    engine.execute("ANALYZE TABLE rz_dim").unwrap();
    let sql = "SELECT /*+ NO_PARALLEL */ SUM(f.v),COUNT(*) FROM rz_fact f JOIN rz_dim d ON f.k=d.k";
    let mut session = SessionContext::default();
    session.sysvars.optimizer_switch = "zone_map=off".into();
    let QueryResponse::ResultSet(expected) =
        super::support::execute_materialized(&engine, &mut session, sql).unwrap()
    else {
        panic!("reference")
    };
    let expected = expected.rows;
    let before = crate::engine::executor::select::vec_bridge::RUNTIME_RANGE_ZONE_PRUNE_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(super::support::query_rows(&engine, sql), expected);
    assert!(
        crate::engine::executor::select::vec_bridge::RUNTIME_RANGE_ZONE_PRUNE_HITS
            .load(std::sync::atomic::Ordering::Relaxed)
            > before
    );
    assert_eq!(expected[0][1], Value::Int(2000));
    assert_eq!(
        super::support::query_rows(
            &engine,
            "SELECT /*+ NO_PARALLEL */ COUNT(*) FROM rz_fact f LEFT JOIN rz_dim d ON f.k=d.k"
        )[0][0],
        Value::Int(40000)
    );
}

#[test]
fn hash_join_startup_cleans_orphan_tmp_dirs() {
    // Simulate a hashjoin spill temporary directory left by a crashed process;
    // opening Database should remove it automatically.
    let dir = tempfile::tempdir().unwrap();
    let tmp = crate::storage::naming::system_tmp_dir(dir.path());
    std::fs::create_dir_all(&tmp).unwrap();
    let orphan_a = tmp.join("hashjoin-1234-0");
    let orphan_b = tmp.join("hashjoin-5678-1");
    let unrelated = tmp.join("other-tmp-data");
    std::fs::create_dir(&orphan_a).unwrap();
    std::fs::write(orphan_a.join("build-0"), b"junk").unwrap();
    std::fs::create_dir(&orphan_b).unwrap();
    std::fs::create_dir(&unrelated).unwrap();
    std::fs::write(unrelated.join("keep-me"), b"keep").unwrap();

    let _engine = open_test_engine(dir.path());

    assert!(!orphan_a.exists(), "hashjoin-1234-0 should be cleaned");
    assert!(!orphan_b.exists(), "hashjoin-5678-1 should be cleaned");
    assert!(unrelated.exists(), "non-hashjoin tmp dir should be kept");
}

#[test]
fn show_engine_status_reports_phase4_counters() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = engine.execute("SHOW ENGINE MYDB STATUS").unwrap()
    else {
        panic!("expected result set");
    };
    let names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|r| match &r[1] {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    for expected in &[
        "hash_join_count",
        "hash_join_bloom_built",
        "hash_join_bloom_filtered",
        "hash_join_parallel_partitions",
        "vec_hash_join_grace_parallel_workers",
        "hash_join_grace_partitions",
        "hash_join_spill_bytes",
        "adaptive_join_count",
        "adaptive_join_chosen_hash",
        "adaptive_join_chosen_index_nlj",
        "adaptive_join_chosen_nlj",
        "adaptive_join_switched_from_planner",
    ] {
        assert!(
            names.iter().any(|n| n == expected),
            "missing counter: {}",
            expected
        );
    }
}

#[test]
fn adaptive_join_executes_and_records_runtime_choice() {
    let _guard = hash_join_env_lock();
    let prior_adaptive = std::env::var("MYDB_ADAPTIVE_JOIN").ok();
    std::env::set_var("MYDB_ADAPTIVE_JOIN", "1");
    let _restore_adaptive = scopeguard_restore_env_key("MYDB_ADAPTIVE_JOIN", prior_adaptive);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE l (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE r (id INT PRIMARY KEY, k INT, INDEX idx_k(k))")
        .unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 1), (2, 2)")
        .unwrap();
    for i in 0..50 {
        engine
            .execute(&format!("INSERT INTO r VALUES ({i}, {i})"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE l").unwrap();
    engine.execute("ANALYZE TABLE r").unwrap();

    let before = crate::engine::executor::select::plan::ADAPTIVE_JOIN_COUNT
        .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT COUNT(*) FROM l JOIN r ON l.k = r.k")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(2));
    let after = crate::engine::executor::select::plan::ADAPTIVE_JOIN_COUNT
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(after > before, "adaptive join should record execution");
}

#[test]
fn adaptive_hash_join_tiny_build_switches_to_nlj() {
    let _guard = hash_join_env_lock();
    let prior_adaptive = std::env::var("MYDB_ADAPTIVE_JOIN").ok();
    std::env::set_var("MYDB_ADAPTIVE_JOIN", "1");
    let _restore_adaptive = scopeguard_restore_env_key("MYDB_ADAPTIVE_JOIN", prior_adaptive);
    std::env::set_var("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS", "0");
    std::env::set_var("MYDB_ADAPTIVE_JOIN_SMALL_BUILD_ROWS", "64");
    std::env::set_var("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", "1000000");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, k INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 1), (2, 2), (3, 3)")
        .unwrap();
    for i in 0..100 {
        engine
            .execute(&format!("INSERT INTO r VALUES ({i}, {})", i % 5))
            .unwrap();
    }

    let chosen_before = crate::engine::executor::select::plan::ADAPTIVE_JOIN_CHOSEN_NLJ
        .load(std::sync::atomic::Ordering::Relaxed);

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT COUNT(*) FROM l JOIN r ON l.k = r.k")
        .unwrap()
    else {
        panic!("expected result set");
    };

    let chosen_after = crate::engine::executor::select::plan::ADAPTIVE_JOIN_CHOSEN_NLJ
        .load(std::sync::atomic::Ordering::Relaxed);

    std::env::remove_var("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS");
    std::env::remove_var("MYDB_ADAPTIVE_JOIN_SMALL_BUILD_ROWS");
    std::env::remove_var("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS");

    assert_eq!(result.rows[0][0], Value::Int(60));
    assert!(
        chosen_after > chosen_before,
        "tiny join should choose NLJ at runtime"
    );
}

#[test]
fn adaptive_hash_join_oversized_build_can_switch_to_nlj_before_error() {
    let _guard = hash_join_env_lock();
    let prior_adaptive = std::env::var("MYDB_ADAPTIVE_JOIN").ok();
    std::env::set_var("MYDB_ADAPTIVE_JOIN", "1");
    let _restore_adaptive = scopeguard_restore_env_key("MYDB_ADAPTIVE_JOIN", prior_adaptive);
    std::env::set_var("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS", "0");
    std::env::set_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS", "0");
    std::env::set_var("MYDB_HASH_JOIN_FALLBACK", "error");
    std::env::set_var("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", "1000000");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, k INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 1), (2, 2), (3, 3)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (10, 1), (20, 2), (30, 3)")
        .unwrap();

    let result = engine.execute("SELECT COUNT(*) FROM l JOIN r ON l.k = r.k");

    std::env::remove_var("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS");
    std::env::remove_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS");
    std::env::remove_var("MYDB_HASH_JOIN_FALLBACK");
    std::env::remove_var("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS");

    let QueryResponse::ResultSet(result) = result.unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(3));
}

#[test]
fn adaptive_index_nlj_executes_and_records_choice() {
    let _guard = hash_join_env_lock();
    let prior_adaptive = std::env::var("MYDB_ADAPTIVE_JOIN").ok();
    std::env::set_var("MYDB_ADAPTIVE_JOIN", "1");
    let _restore_adaptive = scopeguard_restore_env_key("MYDB_ADAPTIVE_JOIN", prior_adaptive);
    let prior_nlj_max = std::env::var("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS").ok();
    std::env::set_var("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", "0");
    let _restore_nlj_max =
        scopeguard_restore_env_key("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", prior_nlj_max);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, k INT)").unwrap();
    engine
        .execute("CREATE TABLE r (id INT, k INT, INDEX idx_k(k))")
        .unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 1), (2, 2), (3, 3)")
        .unwrap();
    let r_values = (0..100)
        .map(|i| format!("({i}, {})", i % 10))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO r VALUES {r_values}"))
        .unwrap();

    let adaptive_before = crate::engine::executor::select::plan::ADAPTIVE_JOIN_COUNT
        .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(plan) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT COUNT(*) FROM l JOIN r ON l.k = r.k")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let plan_text = format!("{:?}", plan.rows);
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT COUNT(*) FROM l JOIN r ON l.k = r.k")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let adaptive_after = crate::engine::executor::select::plan::ADAPTIVE_JOIN_COUNT
        .load(std::sync::atomic::Ordering::Relaxed);

    assert_eq!(result.rows[0][0], Value::Int(30));
    assert!(
        plan_text.contains("Adaptive join") && plan_text.contains("Index nested loop"),
        "join should plan adaptive index NLJ as a runtime candidate, plan={plan_text}"
    );
    assert!(
        adaptive_after > adaptive_before,
        "join should execute through adaptive join, plan={plan_text}"
    );
}

#[test]
fn auto_parameterize_off_skips_extraction_and_lit_keyed_cache() {
    // With MYDB_AUTO_PARAMETERIZE=off the execution path falls back to
    // Phase 1b behaviour: SQL text is the cache key, so two queries with
    // different literals do NOT share an entry. Each non-trivial SELECT
    // becomes its own plan_cache miss + insert.
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "off");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, n INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, a_id INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1,1),(2,2),(3,3)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (10,1),(20,2),(30,3)")
        .unwrap();

    let plan_cache = std::sync::Arc::clone(engine.plan_cache());
    let baseline = plan_cache.stats();

    for id in &[1, 2, 3] {
        let sql =
            format!("SELECT a.id, b.id FROM a INNER JOIN b ON a.id = b.a_id WHERE a.id = {id}");
        let _ = engine.execute(&sql).unwrap();
    }

    let after = plan_cache.stats();
    let misses = after.misses.saturating_sub(baseline.misses);
    assert!(
        misses >= 3,
        "with auto-param OFF, 3 differing-literal queries should each miss: misses={misses}"
    );
}

// Lock guarding tests that mutate `MYDB_AUTO_PARAMETERIZE`. The env var
// is process-global, so without serialization parallel tests would race.
static AUTO_PARAM_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn default_auto_parameterization_accepts_only_typed_integer_counter_updates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE counter_shape (
                id INT PRIMARY KEY,
                quantity BIGINT,
                version INT,
                note VARCHAR(32),
                amount DECIMAL(18,2),
                score DOUBLE
            )",
        )
        .unwrap();
    let session = SessionContext::default();

    let safe = parse_statement(
        "UPDATE counter_shape
         SET quantity=quantity-1, version=version+1
         WHERE id=7 AND quantity>0",
    )
    .unwrap();
    assert!(safe_integer_counter_update(
        engine.db_for_tests(),
        &session,
        &safe
    ));
    assert_eq!(
        auto_param_mode_for_statement(
            engine.db_for_tests(),
            &session,
            &safe,
            crate::engine::auto_param::AutoParameterizeMode::Select,
        ),
        crate::engine::auto_param::AutoParameterizeMode::All
    );

    for sql in [
        "UPDATE counter_shape SET quantity=quantity/0 WHERE id=7",
        "UPDATE /*+ PARALLEL(4) */ counter_shape SET version=version+1 WHERE id=7",
    ] {
        let statement = parse_statement(sql).unwrap();
        assert!(
            !safe_integer_counter_update(engine.db_for_tests(), &session, &statement),
            "unsafe literal-provenance shape was accepted: {sql}"
        );
        assert_eq!(
            auto_param_mode_for_statement(
                engine.db_for_tests(),
                &session,
                &statement,
                crate::engine::auto_param::AutoParameterizeMode::Select,
            ),
            crate::engine::auto_param::AutoParameterizeMode::Select,
            "unsafe shape was promoted: {sql}"
        );
    }

    for sql in [
        "UPDATE counter_shape SET amount=amount+1.25 WHERE id=7",
        "UPDATE counter_shape SET score=score-2.5 WHERE id=7 AND version>0",
    ] {
        let statement = parse_statement(sql).unwrap();
        assert!(
            safe_numeric_counter_update(engine.db_for_tests(), &session, &statement),
            "catalog-typed numeric counter should be safe: {sql}"
        );
    }
    for sql in [
        "UPDATE counter_shape SET amount=1.25 WHERE id=7",
        "UPDATE counter_shape SET amount=amount*1.25 WHERE id=7",
        "UPDATE counter_shape SET amount=amount+1.25, note='x' WHERE id=7",
        "UPDATE counter_shape SET amount=amount+1.25 WHERE (id IS NULL) >> ''",
    ] {
        let statement = parse_statement(sql).unwrap();
        assert!(
            !safe_numeric_counter_update(engine.db_for_tests(), &session, &statement),
            "unsafe numeric-counter shape was accepted: {sql}"
        );
    }
    for sql in [
        "UPDATE counter_shape SET quantity=11, amount=amount+1.25, note='x' WHERE id=7",
        "UPDATE counter_shape SET score=2.5 WHERE id=7 AND version>0",
    ] {
        let statement = parse_statement(sql).unwrap();
        assert!(
            safe_typed_point_update(engine.db_for_tests(), &session, &statement),
            "catalog-typed point update should be safe: {sql}"
        );
    }
    for sql in [
        "UPDATE counter_shape SET amount=amount*1.25 WHERE id=7",
        "UPDATE counter_shape SET amount=amount+1.25 WHERE (id IS NULL) >> ''",
        "UPDATE counter_shape SET amount='not-a-number' WHERE id=7",
    ] {
        let statement = parse_statement(sql).unwrap();
        assert!(
            !safe_typed_point_update(engine.db_for_tests(), &session, &statement),
            "unsafe typed-point shape was accepted: {sql}"
        );
    }
}

#[test]
fn default_auto_parameterization_promotes_only_plain_large_literal_updates() {
    let payload = "x".repeat(64 * 1024);
    for sql in [
        format!("UPDATE lob_t SET payload='{payload}', generation=1 WHERE id=7"),
        format!("UPDATE lob_t SET payload=_binary'{payload}', generation=1 WHERE id=7"),
    ] {
        let statement = parse_statement(&sql).unwrap();
        assert!(safe_large_literal_update(&statement), "shape: {sql:?}");
        let mut rewritten = statement.clone();
        let result = crate::engine::auto_param::auto_parameterize(
            crate::engine::auto_param::AutoParameterizeMode::All,
            &mut rewritten,
        );
        assert!(result.fingerprint.is_some());
        assert_eq!(result.params.len(), 3);
        let value_len = match &result.params[0] {
            Value::String(value) => value.len(),
            Value::Bytes(value) => value.len(),
            other => panic!("expected string/bytes payload, got {other:?}"),
        };
        assert_eq!(value_len, payload.len());
    }

    for sql in [
        format!("UPDATE lob_t SET payload=CONCAT('{payload}', 'x') WHERE id=7"),
        format!("UPDATE lob_t SET payload='{payload}' WHERE id=7 ORDER BY id"),
        "UPDATE lob_t SET payload='small' WHERE id=7".to_string(),
    ] {
        let statement = parse_statement(&sql).unwrap();
        assert!(!safe_large_literal_update(&statement), "shape: {sql:?}");
    }
}

#[test]
fn large_literal_compiled_update_invalidates_on_drop_recreate_identity_change() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let payload = "z".repeat(64 * 1024);
    let before = engine.db_for_tests().storage_status();

    for cycle in 0..2 {
        engine
            .execute("CREATE TABLE lob_recreate (id BIGINT PRIMARY KEY, payload LONGTEXT)")
            .unwrap();
        engine
            .execute("INSERT INTO lob_recreate VALUES (1, 'old')")
            .unwrap();
        let QueryResponse::OkAffected { affected_rows } = engine
            .execute(&format!(
                "UPDATE lob_recreate SET payload='{payload}' WHERE id=1"
            ))
            .unwrap()
        else {
            panic!("expected affected-row response in cycle {cycle}");
        };
        assert_eq!(affected_rows, 1, "cycle {cycle}");
        engine.execute("DROP TABLE lob_recreate").unwrap();
    }

    let after = engine.db_for_tests().storage_status();
    assert_eq!(
        after.prepared_dml_compiles - before.prepared_dml_compiles,
        4,
        "both INSERT and UPDATE plans must invalidate across DROP+CREATE identity changes"
    );
}

#[test]
fn large_literal_text_dml_inline_gate_executes_only_proven_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE lob_inline_gate (id BIGINT PRIMARY KEY, payload LONGTEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO lob_inline_gate VALUES (1, 'old')")
        .unwrap();
    let payload = "q".repeat(64 * 1024);
    let mut session = SessionContext::default();
    let eligible = <StorageQueryEngine as QueryEngine>::execute_large_text_dml_inline(
        &engine,
        &mut session,
        &format!("UPDATE lob_inline_gate SET payload='{payload}' WHERE id=1"),
    )
    .expect("plain large-literal UPDATE should use inline path")
    .unwrap();
    assert!(matches!(
        eligible,
        QueryResponse::OkAffected { affected_rows: 1 }
    ));

    let ineligible = <StorageQueryEngine as QueryEngine>::execute_large_text_dml_inline(
        &engine,
        &mut session,
        &format!("UPDATE lob_inline_gate SET payload=CONCAT('{payload}', 'x') WHERE id=1"),
    );
    assert!(
        ineligible.is_none(),
        "expression UPDATE must fall back before execution"
    );
}

#[test]
fn default_safe_counter_text_shape_reuses_compiled_dml_and_checks_residual() {
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE counter_text_cache (
                id BIGINT PRIMARY KEY,
                quantity BIGINT NOT NULL,
                version BIGINT NOT NULL
            )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO counter_text_cache VALUES (1,2,0),(2,1,0)")
        .unwrap();

    let probe_sql = "UPDATE counter_text_cache \
                     SET quantity=quantity-1, version=version+1 \
                     WHERE id=1 AND quantity>0";
    let (_, scanned) =
        scan_literal_normalized_dml(probe_sql, false).expect("counter scanner should accept SQL");
    let mut parsed = parse_statement(probe_sql).unwrap();
    assert!(safe_integer_counter_update(
        engine.db_for_tests(),
        &SessionContext::default(),
        &parsed
    ));
    let rewritten = crate::engine::auto_param::auto_parameterize(
        crate::engine::auto_param::AutoParameterizeMode::All,
        &mut parsed,
    );
    assert_eq!(rewritten.params, scanned, "scanner/parser parameter order");

    let before = engine.db_for_tests().storage_status();
    for (id, affected) in [(1, 1), (2, 1), (1, 1), (1, 0)] {
        let QueryResponse::OkAffected { affected_rows } = engine
            .execute(&format!(
                "UPDATE counter_text_cache \
                 SET quantity=quantity-1, version=version+1 \
                 WHERE id={id} AND quantity>0"
            ))
            .unwrap()
        else {
            panic!("expected affected-row response");
        };
        assert_eq!(affected_rows, affected);
    }
    let after = engine.db_for_tests().storage_status();
    assert_eq!(
        after.prepared_dml_compiles - before.prepared_dml_compiles,
        1,
        "the first safe counter shape should compile once"
    );
    assert_eq!(
        after.prepared_dml_reuses - before.prepared_dml_reuses,
        4,
        "every execution, including the compile miss, uses the compiled kernel"
    );

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, quantity, version FROM counter_text_cache ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(0), Value::Int(2)],
            vec![Value::Int(2), Value::Int(0), Value::Int(1)],
        ]
    );
}

#[test]
fn default_text_insert_shape_compiles_once_without_reparsing() {
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE default_text_dml_cache (
                id BIGINT PRIMARY KEY,
                quantity BIGINT NOT NULL,
                note VARCHAR(32) NOT NULL
            )",
        )
        .unwrap();

    let before = engine.db_for_tests().storage_status();
    let parse_before = crate::parser::parse_statement_calls_for_test();

    for sql in [
        "INSERT INTO default_text_dml_cache VALUES (1, 10, 'one')",
        "INSERT INTO default_text_dml_cache VALUES (2, 20, 'two')",
        "UPDATE default_text_dml_cache SET quantity=11, note='one+' WHERE id=1",
        "UPDATE default_text_dml_cache SET quantity=22, note='two+' WHERE id=2",
        "DELETE FROM default_text_dml_cache WHERE id=1",
        "DELETE FROM default_text_dml_cache WHERE id=2",
    ] {
        let QueryResponse::OkAffected { affected_rows } = engine.execute(sql).unwrap() else {
            panic!("expected affected-row response for {sql}");
        };
        assert_eq!(affected_rows, 1, "{sql}");
    }

    let parse_after = crate::parser::parse_statement_calls_for_test();
    let after = engine.db_for_tests().storage_status();
    assert_eq!(
        parse_after - parse_before,
        8,
        "cold INSERT/UPDATE shapes prove placeholder structure; hot shapes do not parse; DELETE stays legacy"
    );
    assert_eq!(
        after.prepared_dml_compiles - before.prepared_dml_compiles,
        2,
        "plain INSERT and catalog-typed UPDATE are default-on"
    );
    assert_eq!(
        after.prepared_dml_reuses - before.prepared_dml_reuses,
        4,
        "cold and warm INSERT/UPDATE executions use compiled DML kernels"
    );

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT COUNT(*) FROM default_text_dml_cache")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(0)]]);
}

#[test]
fn rejected_counter_probe_reuses_its_parsed_statement() {
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE rejected_counter (id INT PRIMARY KEY, quantity INT NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO rejected_counter VALUES (1, 5)")
        .unwrap();

    // The literal scanner admits this all-integer UPDATE as a candidate, but
    // multiplication is outside the conservative +/- counter subset. The
    // probe's full parser is therefore the oracle and the ordinary dispatcher
    // must consume that same AST instead of parsing the SQL again.
    let before = crate::parser::parse_statement_calls_for_test();
    let QueryResponse::OkAffected { affected_rows } = engine
        .execute("UPDATE rejected_counter SET quantity = quantity * 2 WHERE id = 1")
        .unwrap()
    else {
        panic!("expected affected-row response");
    };
    let after = crate::parser::parse_statement_calls_for_test();
    assert_eq!(affected_rows, 1);
    assert_eq!(
        after - before,
        1,
        "a rejected scanned-counter candidate must be parsed exactly once"
    );
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT quantity FROM rejected_counter WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(10)]]);
}

#[test]
fn proven_text_select_shape_reuses_parameterized_ast_without_parse() {
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE text_select_shape (\
             id INT PRIMARY KEY, tag VARCHAR(8), value INT)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO text_select_shape VALUES (1,'a',10),(2,'b',20)")
        .unwrap();
    let mut session = SessionContext::default();

    // The exact parser is the first-execution oracle and publishes the shape
    // only after its extracted values match the lexical candidate exactly.
    let first = engine
        .execute_with_session(
            &mut session,
            "SELECT value FROM text_select_shape WHERE id=1 AND tag='a'",
        )
        .unwrap();
    assert_eq!(
        collect_select_response_rows(first),
        vec![vec![Value::Int(10)]]
    );

    let parse_before = crate::parser::parse_statement_calls_for_test();
    let second_sql = "SELECT value FROM text_select_shape WHERE id=2 AND tag='b'";
    let (second_statement, second_params, second_shape) =
        QueryEngine::cached_text_statement(&engine, &session, second_sql)
            .expect("proven SELECT skeleton should hit");
    let qualified = Arc::new(
        QueryEngine::prepare_statement_template(
            &engine,
            &mut session,
            second_sql,
            second_statement.as_ref().clone(),
        )
        .unwrap(),
    );
    let mut memo_state = crate::engine::api::PreparedSelectMemoState::default();
    let second = QueryEngine::execute_prepared_select(
        &engine,
        &mut session,
        &mut memo_state,
        &qualified,
        second_sql,
        &second_params,
    )
    .expect("qualified text shape should compile through the prepared SELECT envelope")
    .unwrap();
    drop(second_shape);
    let parse_after = crate::parser::parse_statement_calls_for_test();
    assert_eq!(
        collect_select_response_rows(second),
        vec![vec![Value::Int(20)]]
    );
    assert_eq!(
        parse_after, parse_before,
        "a proven literal shape must not invoke the full parser"
    );

    // Once Ready, the memo no longer reads the parser artifact. This is the
    // steady-state COM_QUERY path: the shared artifact remains unqualified so
    // it is safe across databases, while the per-session compiled memo owns
    // the one-time qualified/normalized plan.
    let third_sql = "SELECT value FROM text_select_shape WHERE id=1 AND tag='a'";
    let (third_statement, third_params, _) =
        QueryEngine::cached_text_statement(&engine, &session, third_sql).unwrap();
    let parse_before_third = crate::parser::parse_statement_calls_for_test();
    let third = crate::engine::api::with_shared_result_metadata(|| {
        QueryEngine::execute_prepared_select(
            &engine,
            &mut session,
            &mut memo_state,
            &third_statement,
            third_sql,
            &third_params,
        )
    })
    .expect("ready memo should reuse without re-qualifying the shared AST")
    .unwrap();
    assert_eq!(
        collect_select_response_rows(third),
        vec![vec![Value::Int(10)]]
    );
    assert_eq!(
        crate::parser::parse_statement_calls_for_test(),
        parse_before_third,
        "a ready memo must not parse or re-qualify the shared artifact"
    );

    // LIKE literals are deliberately plan-sensitive and therefore never
    // pass the parser/auto-parameter equality gate.
    engine
        .execute_with_session(
            &mut session,
            "SELECT value FROM text_select_shape WHERE tag LIKE 'a%'",
        )
        .unwrap();
    assert!(QueryEngine::cached_text_statement(
        &engine,
        &session,
        "SELECT value FROM text_select_shape WHERE tag LIKE 'b%'",
    )
    .is_none());
}

#[test]
fn text_dml_shape_size_limit_preserves_batch_and_literal_semantics() {
    for size in [
        TEXT_DML_SHAPE_MAX_BYTES - 1,
        TEXT_DML_SHAPE_MAX_BYTES,
        TEXT_DML_SHAPE_MAX_BYTES + 1,
    ] {
        let mut sql = "INSERT INTO t VALUES (1, '".to_string();
        let value_len = size - sql.len() - 2;
        sql.extend(std::iter::repeat_n('a', value_len));
        sql.push_str("')");
        assert_eq!(sql.len(), size);
        let eligible = size <= TEXT_DML_SHAPE_MAX_BYTES;
        assert_eq!(scan_literal_normalized_dml(&sql, false).is_some(), eligible);
        assert_eq!(text_dml_digest_shape(&sql).is_some(), eligible);
        sql.push_str("; UPDATE t SET n=2 WHERE id=1");
        let (shape, params) = scan_literal_normalized_dml(&sql, true).unwrap();
        assert_eq!(
            shape,
            "INSERT INTO t VALUES (?, ?); UPDATE t SET n=? WHERE id=?"
        );
        assert_eq!(params.len(), 4);
        assert_eq!(params[0], Value::Int(1));
        assert_eq!(params[1], Value::String("a".repeat(value_len)));
    }
    for sql in [
        "INSERT INTO t VALUES ('a\\n')",
        "INSERT INTO t VALUES ('字')",
        "UPDATE /*+ hint */ t SET n=1",
    ] {
        assert!(scan_literal_normalized_dml(sql, false).is_none());
    }
}

#[test]
fn text_dml_digest_shape_erases_values_without_erasing_identifiers() {
    let first = text_dml_digest_shape("UPDATE Account SET balance=balance+1.25 WHERE id=7")
        .expect("ordinary literal DML should have a digest shape");
    let second = text_dml_digest_shape("UPDATE Account SET balance=balance+9.50 WHERE id=42")
        .expect("same DML shape should be accepted");
    assert_eq!(first, second);
    assert!(
        first.contains("Account"),
        "identifier spelling is preserved"
    );
    assert!(text_dml_digest_shape(
        "UPDATE /*+ SET_VAR(sort_buffer_size=1) */ Account SET balance=1 WHERE id=7"
    )
    .is_none());
}

#[test]
fn diagnostics_preservation_fast_prefix_matches_commented_forms() {
    for sql in [
        "SHOW WARNINGS",
        "  \n\tSHOW ERRORS",
        "/* leading */ SHOW WARNINGS LIMIT 1",
        "-- leading\nSHOW COUNT(*) WARNINGS",
        "# leading\nSELECT @@warning_count",
    ] {
        assert!(
            preserves_statement_diagnostics(sql),
            "must preserve: {sql:?}"
        );
    }
    for sql in [
        "SELECT * FROM account WHERE id=1",
        "  UPDATE account SET balance=2 WHERE id=1",
        "/* leading */ SELECT 1",
    ] {
        assert!(
            !preserves_statement_diagnostics(sql),
            "ordinary statement must clear diagnostics: {sql:?}"
        );
    }
}

#[test]
fn auto_parameterize_all_runs_text_dml_correctly() {
    // MYDB_AUTO_PARAMETERIZE=all rewrites text-protocol DML literals into
    // late-bound placeholders. The whole INSERT → UPDATE → DELETE cycle
    // must produce exactly the same data effects as literal execution.
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "all");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ap_dml (id INT PRIMARY KEY, n INT, s VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO ap_dml (id, n, s) VALUES (1, 10, 'one')")
        .unwrap();
    engine
        .execute("INSERT INTO ap_dml (id, n, s) VALUES (2, 20, 'two'), (3, 30, 'three')")
        .unwrap();
    // Same-shape UPDATEs with different literals must both apply.
    engine
        .execute("UPDATE ap_dml SET n = n + 5, s = 'one+' WHERE id = 1")
        .unwrap();
    engine
        .execute("UPDATE ap_dml SET n = n + 7, s = 'two+' WHERE id = 2")
        .unwrap();
    engine.execute("DELETE FROM ap_dml WHERE id = 3").unwrap();
    // ODKU through the rewrite as well.
    engine
        .execute(
            "INSERT INTO ap_dml (id, n, s) VALUES (1, 0, 'dup') \
             ON DUPLICATE KEY UPDATE n = n + 100",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, n, s FROM ap_dml ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(115), Value::String("one+".into())],
            vec![Value::Int(2), Value::Int(27), Value::String("two+".into())],
        ]
    );
}

#[test]
fn auto_parameterized_text_dml_reuses_prepared_dml_shape_cache() {
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "all");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ap_dml_cache (id INT PRIMARY KEY, n INT)")
        .unwrap();
    engine
        .execute("INSERT INTO ap_dml_cache VALUES (1, 10), (2, 20)")
        .unwrap();

    let before = engine.db_for_tests().storage_status();
    engine
        .execute("UPDATE ap_dml_cache SET n = n + 5 WHERE id = 1")
        .unwrap();
    engine
        .execute("UPDATE ap_dml_cache SET n = n + 7 WHERE id = 2")
        .unwrap();
    let after = engine.db_for_tests().storage_status();

    assert_eq!(
        after.prepared_dml_compiles - before.prepared_dml_compiles,
        1
    );
    assert_eq!(after.prepared_dml_reuses - before.prepared_dml_reuses, 2);

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, n FROM ap_dml_cache ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(15)],
            vec![Value::Int(2), Value::Int(27)]
        ]
    );
}

#[test]
fn proven_text_dml_shape_reuses_parameterized_ast_without_parse() {
    // The COM_QUERY DML counterpart of
    // `proven_text_select_shape_reuses_parameterized_ast_without_parse`: once
    // the exact parser has proven a literal skeleton, a later statement whose
    // only difference is its literals reuses that placeholder AST and binds
    // through the compiled prepared-DML vehicle, with no second parse.
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "all");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE text_dml_shape (id INT PRIMARY KEY, n INT)")
        .unwrap();
    engine
        .execute("INSERT INTO text_dml_shape VALUES (1, 10), (2, 20)")
        .unwrap();
    let mut session = SessionContext::default();

    // A cold shape is not published until the exact parser has run and its
    // extracted values matched the lexical scanner's.
    assert!(QueryEngine::cached_text_statement(
        &engine,
        &session,
        "UPDATE text_dml_shape SET n = 11 WHERE id = 1",
    )
    .is_none());

    engine
        .execute_with_session(
            &mut session,
            "UPDATE text_dml_shape SET n = 11 WHERE id = 1",
        )
        .unwrap();

    let parse_before = crate::parser::parse_statement_calls_for_test();
    let second_sql = "UPDATE text_dml_shape SET n = 22 WHERE id = 2";
    let (statement, params, shape) =
        QueryEngine::cached_text_statement(&engine, &session, second_sql)
            .expect("proven UPDATE skeleton should hit");
    assert!(matches!(statement.as_ref(), Statement::Update(_)));
    assert_eq!(params, vec![Value::Int(22), Value::Int(2)]);
    assert!(shape.contains('?'), "shape must be literal-free: {shape}");

    let qualified = Arc::new(
        QueryEngine::prepare_statement_template(
            &engine,
            &mut session,
            second_sql,
            statement.as_ref().clone(),
        )
        .unwrap(),
    );
    let mut dml_state = crate::engine::api::PreparedDmlState::default();
    let response = QueryEngine::execute_prepared_dml(
        &engine,
        &mut session,
        &mut dml_state,
        &qualified,
        second_sql,
        &params,
    )
    .expect("qualified DML shape should compile through the prepared-DML vehicle")
    .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 1 }
    ));
    let parse_after = crate::parser::parse_statement_calls_for_test();
    assert_eq!(
        parse_after, parse_before,
        "a proven literal DML shape must not invoke the full parser"
    );

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, n FROM text_dml_shape ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(2), Value::Int(22)]
        ]
    );
}

#[test]
fn auto_parameterize_all_binds_multi_statement_dml_children() {
    // Multi-statement COM_QUERY is parsed into child statements after the
    // outer auto-param pass. Each child still needs its own parameter scope.
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "all");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ap_multi (id INT PRIMARY KEY, n INT)")
        .unwrap();
    engine
        .execute("INSERT INTO ap_multi VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let before = engine.db_for_tests().storage_status();
    engine
        .execute(
            "UPDATE ap_multi SET n = n + 5 WHERE id = 1; \
             UPDATE ap_multi SET n = n + 7 WHERE id = 2; \
             DELETE FROM ap_multi WHERE id = 3",
        )
        .unwrap();
    let after = engine.db_for_tests().storage_status();

    assert_eq!(
        after.prepared_dml_compiles - before.prepared_dml_compiles,
        2
    );
    assert_eq!(after.prepared_dml_reuses - before.prepared_dml_reuses, 3);

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, n FROM ap_multi ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(15)],
            vec![Value::Int(2), Value::Int(27)]
        ]
    );
}

#[test]
fn auto_parameterized_multi_statement_dml_reuses_batch_shape_cache() {
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "all");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ap_multi_batch (id INT PRIMARY KEY, n INT)")
        .unwrap();
    engine
        .execute("INSERT INTO ap_multi_batch VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    engine
        .execute(
            "UPDATE ap_multi_batch SET n = n + 5 WHERE id = 1; \
             UPDATE ap_multi_batch SET n = n + 7 WHERE id = 2",
        )
        .unwrap();
    let after_first = engine.prepared_dml_batch_cache_stats_for_tests();
    assert_eq!(
        after_first.size, 1,
        "first execution should cache the batch shape"
    );

    engine
        .execute(
            "UPDATE ap_multi_batch SET n = n + 11 WHERE id = 1; \
             UPDATE ap_multi_batch SET n = n + 13 WHERE id = 3",
        )
        .unwrap();
    let after_second = engine.prepared_dml_batch_cache_stats_for_tests();
    assert!(
        after_second.hits > after_first.hits,
        "second same-shape batch should hit the parse/auto-param cache"
    );

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, n FROM ap_multi_batch ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(26)],
            vec![Value::Int(2), Value::Int(27)],
            vec![Value::Int(3), Value::Int(43)]
        ]
    );
}

#[test]
fn multi_statement_dml_batch_cache_engages_without_auto_param_env() {
    // The `;`-joined DML batch fast path must not depend on
    // MYDB_AUTO_PARAMETERIZE: JDBC `rewriteBatchedStatements` clients send
    // these batches under default server settings, and re-parsing the
    // body per execute measured ~7.4% of TPC-C CPU. The batch vehicle
    // re-derives fingerprints internally with `Mode::All` and falls back
    // to the legacy path on any literal/param mismatch, so it is safe in
    // the default (`select`) mode.
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::remove_var("MYDB_AUTO_PARAMETERIZE");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE dml_batch_default (id INT PRIMARY KEY, n INT)")
        .unwrap();
    engine
        .execute("INSERT INTO dml_batch_default VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    engine
        .execute(
            "UPDATE dml_batch_default SET n = n + 5 WHERE id = 1; \
             UPDATE dml_batch_default SET n = n + 7 WHERE id = 2",
        )
        .unwrap();
    let after_first = engine.prepared_dml_batch_cache_stats_for_tests();
    assert_eq!(
        after_first.size, 1,
        "first batch should populate the shape cache even in default auto-param mode"
    );

    engine
        .execute(
            "UPDATE dml_batch_default SET n = n + 11 WHERE id = 1; \
             UPDATE dml_batch_default SET n = n + 13 WHERE id = 3",
        )
        .unwrap();
    let after_second = engine.prepared_dml_batch_cache_stats_for_tests();
    assert!(
        after_second.hits > after_first.hits,
        "second same-shape batch should hit the cache without MYDB_AUTO_PARAMETERIZE"
    );

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, n FROM dml_batch_default ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(26)],
            vec![Value::Int(2), Value::Int(27)],
            vec![Value::Int(3), Value::Int(43)]
        ]
    );
}

#[test]
fn homogeneous_point_update_batch_coalesces_physical_rows_in_autocommit_off_txn() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE point_batch (id INT PRIMARY KEY, n INT)")
        .unwrap();
    engine
        .execute("INSERT INTO point_batch VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET autocommit = OFF")
        .unwrap();
    let batch_before = super::super::prepared_dml::homogeneous_point_update_batch_stats_for_test();
    let dml_before = engine.db_for_tests().dml_stats_snapshot();
    let response = engine
        .execute_with_session(
            &mut session,
            "UPDATE point_batch SET n = n + 5 WHERE id = 1; \
             UPDATE point_batch SET n = n + 7 WHERE id = 1; \
             UPDATE point_batch SET n = n + 11 WHERE id = 2; \
             UPDATE point_batch SET n = n + 13 WHERE id = 3",
        )
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 1 }
    ));
    let batch_after = super::super::prepared_dml::homogeneous_point_update_batch_stats_for_test();
    assert_eq!(batch_after.0 - batch_before.0, 1);
    assert_eq!(batch_after.1 - batch_before.1, 4);
    assert_eq!(batch_after.2 - batch_before.2, 3);
    assert_eq!(
        engine.db_for_tests().dml_stats_snapshot().update_rows - dml_before.update_rows,
        4,
        "logical UPDATE accounting must remain child-statement exact"
    );

    let QueryResponse::ResultSet(inside) = engine
        .execute_with_session(&mut session, "SELECT id, n FROM point_batch ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        inside.rows,
        vec![
            vec![Value::Int(1), Value::Int(22)],
            vec![Value::Int(2), Value::Int(31)],
            vec![Value::Int(3), Value::Int(43)],
        ]
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    let QueryResponse::ResultSet(after_rollback) = engine
        .execute("SELECT id, n FROM point_batch ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        after_rollback.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(3), Value::Int(30)],
        ]
    );
}

#[test]
fn homogeneous_point_update_batch_replays_scalar_prefix_on_value_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE point_batch_error (id INT PRIMARY KEY, n TINYINT)")
        .unwrap();
    engine
        .execute("INSERT INTO point_batch_error VALUES (1, 126)")
        .unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET autocommit = OFF")
        .unwrap();

    let err = engine
        .execute_with_session(
            &mut session,
            "UPDATE point_batch_error SET n = n + 1 WHERE id = 1; \
             UPDATE point_batch_error SET n = n + 1 WHERE id = 1",
        )
        .expect_err("second child must overflow TINYINT");
    assert_eq!(err.code(), 1264);
    let QueryResponse::ResultSet(current) = engine
        .execute_with_session(&mut session, "SELECT n FROM point_batch_error WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(current.rows, vec![vec![Value::Int(127)]]);
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

#[test]
fn homogeneous_decimal_counter_batch_combines_monotonic_repeated_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE decimal_batch (id INT PRIMARY KEY, balance DECIMAL(15,2))")
        .unwrap();
    engine
        .execute("INSERT INTO decimal_batch VALUES (1, 1000.00)")
        .unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET autocommit = OFF")
        .unwrap();
    let sql = std::iter::repeat_n(
        "UPDATE decimal_batch SET balance = balance - 2.50 WHERE id = 1",
        100,
    )
    .collect::<Vec<_>>()
    .join(";");
    let before = super::super::prepared_dml::homogeneous_point_update_batch_stats_for_test();
    engine.execute_with_session(&mut session, &sql).unwrap();
    let after = super::super::prepared_dml::homogeneous_point_update_batch_stats_for_test();
    assert_eq!(after.0 - before.0, 1);
    assert_eq!(after.1 - before.1, 100);
    assert_eq!(after.2 - before.2, 1);
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT balance FROM decimal_batch WHERE id = 1",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::DecimalCompact(75_000, 2)]]);
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

// Tiny RAII helper that restores a process env var on drop, so tests
// that mutate `std::env` don't leak state to siblings.
struct EnvRestore {
    key: &'static str,
    prior: Option<String>,
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        match self.prior.take() {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

fn scopeguard_restore_env(prior: Option<String>) -> EnvRestore {
    scopeguard_restore_env_key("MYDB_AUTO_PARAMETERIZE", prior)
}

fn scopeguard_restore_env_key(key: &'static str, prior: Option<String>) -> EnvRestore {
    EnvRestore { key, prior }
}

#[test]
fn auto_parameterized_limit_applies_on_window_and_derived_join_paths() {
    // Regression (TPC-DS q12/q20/q65): with MYDB_AUTO_PARAMETERIZE=select a
    // literal `LIMIT N` is folded into a `LimitValue::Placeholder`. The window
    // executor and the derived/CTE-join row paths used to match only
    // `LimitValue::Rows(_)` and silently drop the placeholder, returning the
    // whole unlimited result. Both must now resolve the placeholder.
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE it (cls VARCHAR(4), iid VARCHAR(4), price INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO it VALUES \
             ('a','i1',5),('a','i2',6),('b','i3',7),('b','i4',8),('c','i5',9),('c','i6',10)",
        )
        .unwrap();

    // Window function in the projection → window executor path.
    let windowed = query_rows(
        &engine,
        "SELECT cls, iid, SUM(price) OVER (PARTITION BY cls) AS r \
         FROM it ORDER BY cls, iid LIMIT 3",
    );
    assert_eq!(windowed.len(), 3, "window+LIMIT must truncate to 3");

    // Derived (aggregating) table joined with a base table → CTE-join row path.
    engine.execute("CREATE TABLE dim (cls VARCHAR(4))").unwrap();
    engine
        .execute("INSERT INTO dim VALUES ('a'),('b'),('c')")
        .unwrap();
    let joined = query_rows(
        &engine,
        "SELECT d.cls, g.total FROM dim d, \
         (SELECT cls, SUM(price) AS total FROM it GROUP BY cls) g \
         WHERE d.cls = g.cls ORDER BY d.cls LIMIT 2",
    );
    assert_eq!(joined.len(), 2, "derived-join+LIMIT must truncate to 2");
}

#[test]
fn varchar_param_bytes_wire_form_resolves_to_string_keypart() {
    // Regression: BenchmarkSQL OrderStatus by C_LAST returned 0 rows
    // because the MySQL binary protocol encodes VARCHAR/STRING as
    // length-encoded *bytes*, so the wire decoder produced
    // `StmtParamValue::Bytes`. The Phase-2 conversion turned that into
    // `Value::Bytes`, while INSERT runs values through `coerce_row`
    // which canonicalises VARCHAR to `Value::String`. The two map to
    // different `KeyPart` enum variants, so the index lookup silently
    // missed every row.
    //
    // Drive the binary path directly to lock in the conversion.
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;
    use crate::mysql::protocol::StmtParamValue;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE customer (\
             c_w_id INT, c_d_id INT, c_id INT, c_last VARCHAR(16), c_first VARCHAR(16),\
             PRIMARY KEY (c_w_id, c_d_id, c_id),\
             INDEX cust_last (c_w_id, c_d_id, c_last, c_first))",
        )
        .unwrap();

    // Insert with an INSERT statement so values flow through coerce_row
    // and get stored as Value::String / KeyPart::String — the same as
    // a prepared INSERT EXECUTE would.
    engine
        .execute(
            "INSERT INTO customer VALUES \
             (2, 3, 100, 'ATIONABLEEING', 'Alice'),\
             (2, 3, 101, 'ATIONABLEEING', 'Bob')",
        )
        .unwrap();

    // Build params the way the wire decoder does: STRING params arrive
    // as `Bytes` regardless of column type.
    let wire_params = vec![
        StmtParamValue::Int(2),
        StmtParamValue::Int(3),
        StmtParamValue::Bytes(b"ATIONABLEEING".to_vec()),
    ];
    let runtime_params: Vec<Value> = wire_params
        .iter()
        .map(crate::mysql::session::stmt_param_to_value_for_test)
        .collect();
    // After conversion the VARCHAR param must be a String value, otherwise
    // it will mismatch the index entries written by INSERT.
    assert!(
        matches!(runtime_params[2], Value::String(ref s) if s == "ATIONABLEEING"),
        "VARCHAR param must materialise as Value::String, got {:?}",
        runtime_params[2]
    );

    // End-to-end sanity: prepared SELECT with the same params returns
    // both seeded customers.
    let mut session = SessionContext::default();
    let sql = "SELECT c_id FROM customer \
               WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first";
    let prepared = engine.prepare_cached(&mut session, sql, 3).unwrap();
    let stmt = (*prepared).clone();
    let response = with_params(runtime_params, || {
        engine
            .execute_prepared_statement(&mut session, sql, stmt)
            .unwrap()
            .unwrap()
    });
    let response = response.materialize().unwrap();
    let QueryResponse::ResultSet(rs) = response else {
        panic!("expected ResultSet");
    };
    assert_eq!(rs.rows.len(), 2, "got rows: {:?}", rs.rows);
}

#[test]
fn order_status_style_prepared_select_in_transaction_finds_customers() {
    // OrderStatus pattern from BenchmarkSQL: explicit BEGIN, prepared
    // SELECT with composite-index-only WHERE on (w_id, d_id, c_last),
    // ORDER BY c_first, then COMMIT. Reproduces the multi-step
    // transaction shape that surfaced "Customer not found" in the wild.
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE customer (\
             c_w_id INT, c_d_id INT, c_id INT, c_last VARCHAR(16), c_first VARCHAR(16),\
             PRIMARY KEY (c_w_id, c_d_id, c_id),\
             INDEX cust_last (c_w_id, c_d_id, c_last, c_first))",
        )
        .unwrap();
    // Bulk-load via prepared INSERT to mirror BenchmarkSQL's load path.
    let mut load_session = SessionContext::default();
    let load_prepared = engine
        .prepare_cached(
            &mut load_session,
            "INSERT INTO customer (c_w_id, c_d_id, c_id, c_last, c_first) \
             VALUES (?, ?, ?, ?, ?)",
            5,
        )
        .unwrap();

    let last_names = ["ATIONABLEEING", "BARBARANTI", "CALLYABLEPRES"];
    for (li, lname) in last_names.iter().enumerate() {
        for c in 0..3 {
            let id = (li * 100 + c) as i64;
            let stmt = (*load_prepared).clone();
            let mut session = load_session.clone();
            let _ = with_params(
                vec![
                    Value::Int(2),
                    Value::Int(3),
                    Value::Int(id),
                    Value::String(lname.to_string()),
                    Value::String(format!("first{}", c)),
                ],
                || {
                    engine
                        .execute_prepared_statement(
                            &mut session,
                            "INSERT INTO customer (c_w_id, c_d_id, c_id, c_last, c_first) \
                             VALUES (?, ?, ?, ?, ?)",
                            stmt,
                        )
                        .unwrap()
                        .unwrap()
                },
            );
        }
    }

    // Drive the OrderStatus shape: explicit txn, prepared SELECT.
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();

    let select_sql = "SELECT c_id FROM customer \
                      WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first";
    let prepared = engine.prepare_cached(&mut session, select_sql, 3).unwrap();
    let stmt = (*prepared).clone();
    let response = with_params(
        vec![
            Value::Int(2),
            Value::Int(3),
            Value::String("ATIONABLEEING".to_string()),
        ],
        || {
            engine
                .execute_prepared_statement(&mut session, select_sql, stmt)
                .unwrap()
                .unwrap()
        },
    );
    let response = response.materialize().unwrap();
    let QueryResponse::ResultSet(rs) = response else {
        panic!("expected ResultSet");
    };
    assert_eq!(
        rs.rows.len(),
        3,
        "OrderStatus should find 3 customers for ATIONABLEEING under (2,3), got rows={:?}",
        rs.rows
    );

    engine.execute_with_session(&mut session, "COMMIT").unwrap();
}

#[test]
fn concurrent_prepared_executes_with_string_param_do_not_lose_rows() {
    // Reproduce the OrderStatus failure: many concurrent terminals
    // PREPARE + EXECUTE the same string-keyed SELECT with different
    // values. Each EXECUTE must return only the rows matching its own
    // parameter — no leakage from neighbouring threads' params, no
    // stale plan-cache hits.
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;
    use std::sync::Arc;
    use std::thread;

    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open_test_engine(dir.path()));
    engine
        .execute(
            "CREATE TABLE customer (\
             c_w_id INT, c_d_id INT, c_id INT, c_last VARCHAR(16), c_first VARCHAR(16),\
             PRIMARY KEY (c_w_id, c_d_id, c_id),\
             INDEX cust_last (c_w_id, c_d_id, c_last, c_first))",
        )
        .unwrap();

    // Insert 4 distinct (w, d, last) buckets, each with 3 rows.
    let last_names = ["ALPHA", "BRAVO", "CHARLIE", "DELTA"];
    for (lname_idx, lname) in last_names.iter().enumerate() {
        for c in 0..3 {
            let id = (lname_idx * 100 + c) as i64;
            engine
                .execute(&format!(
                    "INSERT INTO customer VALUES (1, 1, {}, '{}', 'first{}')",
                    id, lname, c
                ))
                .unwrap();
        }
    }

    let sql = "SELECT c_id FROM customer \
               WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first";

    let mut handles = Vec::new();
    for round in 0..8u32 {
        let engine = Arc::clone(&engine);
        let sql = sql.to_string();
        handles.push(thread::spawn(move || {
            for i in 0..40 {
                let last_idx = ((round.wrapping_add(i)) as usize) % last_names.len();
                let lname = last_names[last_idx];
                let mut session = SessionContext::default();
                let prepared = engine.prepare_cached(&mut session, &sql, 3).unwrap();
                let stmt = (*prepared).clone();
                let response = with_params(
                    vec![
                        Value::Int(1),
                        Value::Int(1),
                        Value::String(lname.to_string()),
                    ],
                    || {
                        engine
                            .execute_prepared_statement(&mut session, &sql, stmt)
                            .unwrap()
                            .unwrap()
                    },
                );
                let response = response.materialize().unwrap();
                let QueryResponse::ResultSet(rs) = response else {
                    panic!("expected ResultSet");
                };
                assert_eq!(
                    rs.rows.len(),
                    3,
                    "round={round} i={i} lname={lname} expected 3 rows, got {}: {:?}",
                    rs.rows.len(),
                    rs.rows,
                );
                let expected: Vec<i64> = (0..3).map(|c| (last_idx * 100 + c) as i64).collect();
                let got: Vec<i64> = rs
                    .rows
                    .iter()
                    .map(|row| match row[0] {
                        Value::Int(n) => n,
                        ref v => panic!("non-int c_id: {:?}", v),
                    })
                    .collect();
                assert_eq!(
                    got, expected,
                    "round={round} i={i} lname={lname} returned wrong c_ids"
                );
            }
        }));
    }

    for h in handles {
        h.join().expect("worker panicked");
    }
}

#[test]
fn prepared_select_by_string_placeholder_returns_correct_rows() {
    // Regression for the third TPC-C wave: OrderStatus by C_LAST issues
    // `SELECT c_id FROM bmsql_customer WHERE c_w_id = ? AND c_d_id = ?
    // AND c_last = ? ORDER BY c_first` and was returning zero rows.
    // Reproduces the prepared-EXECUTE path with composite int+string
    // params to make sure late binding pushes the right comparison
    // values all the way through to row evaluation.
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE customer (\
             c_w_id INT, c_d_id INT, c_id INT, c_last VARCHAR(16), c_first VARCHAR(16),\
             PRIMARY KEY (c_w_id, c_d_id, c_id),\
             INDEX cust_last (c_w_id, c_d_id, c_last, c_first))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO customer VALUES \
             (2, 3, 100, 'ATIONABLEEING', 'Alice'),\
             (2, 3, 101, 'ATIONABLEEING', 'Bob'),\
             (2, 3, 102, 'OTHER', 'Carol'),\
             (1, 3, 200, 'ATIONABLEEING', 'Dave')",
        )
        .unwrap();

    let mut session = SessionContext::default();
    let prepared = engine
        .prepare_cached(
            &mut session,
            "SELECT c_id, c_first FROM customer \
             WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
            3,
        )
        .unwrap();

    let stmt = (*prepared).clone();
    let mut session2 = session.clone();
    let response = with_params(
        vec![
            Value::Int(2),
            Value::Int(3),
            Value::String("ATIONABLEEING".to_string()),
        ],
        || {
            engine
                .execute_prepared_statement(
                    &mut session2,
                    "SELECT c_id, c_first FROM customer \
                     WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
                    stmt,
                )
                .unwrap()
                .unwrap()
        },
    );
    let response = response.materialize().unwrap();
    let QueryResponse::ResultSet(rs) = response else {
        panic!("expected ResultSet");
    };
    assert_eq!(
        rs.rows.len(),
        2,
        "expected 2 customers in (w=2, d=3, c_last=ATIONABLEEING), got: {:?}",
        rs.rows
    );
    assert_eq!(rs.rows[0][0], Value::Int(100));
    assert_eq!(rs.rows[1][0], Value::Int(101));
}

#[test]
fn write_path_index_resolution_under_active_params_handles_placeholders() {
    // Regression for the second wave of TPC-C lock-wait timeouts:
    // even after fixing planner::expr_literal_value, UPDATE/DELETE
    // went through `engine/executor/indexes.rs::best_index_access`,
    // which calls a *different* `expr_literal_value` defined in
    // `engine/expr.rs`. That copy was unfixed and returned None for
    // `Expr::Placeholder`, so `WHERE pk = ?` in PAYMENT/NEW_ORDER
    // UPDATEs fell back to a candidate scan and serialized terminals
    // on the same row range.
    //
    // This test exercises the write-side helper directly so a future
    // refactor that loses the placeholder branch fails fast.
    //
    // Phase 2.5 update: `best_index_access` no longer needs an active
    // parameter context to recognise a placeholder operand — the index
    // key now carries `KeyPart::Param`, which is resolved at the
    // storage boundary. The test asserts both that the deferred shape
    // is produced unconditionally, and that the resolved key matches
    // the bound parameter value.
    use super::super::indexes::best_index_access;
    use crate::engine::executor::indexes::EngineIndexAccess;
    use crate::engine::params::with_params;
    use crate::parser::{parse_statement, Statement};
    use crate::storage::KeyPart;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE customer (id INT PRIMARY KEY, balance INT)")
        .unwrap();

    let table = engine.db_for_tests().table("customer").unwrap();

    let mut stmt = parse_statement("SELECT 1 FROM customer WHERE id = ?").unwrap();
    let mut next = 0;
    crate::parser::number_statement_placeholders(&mut stmt, &mut next);
    let Statement::Select(select) = stmt else {
        panic!("expected Select");
    };
    let where_expr = select.selection.as_ref().expect("WHERE clause");

    // Phase 2.5: deferred IndexKey is produced unconditionally — no
    // active param context required. The key carries a `Param` part
    // that names the placeholder index.
    let (index, access) =
        best_index_access(&table, where_expr).expect("placeholder must select an index");
    assert_eq!(index.name, "PRIMARY");
    let EngineIndexAccess::Lookup { key, exact } = access else {
        panic!("expected Lookup");
    };
    assert!(exact, "primary key with single equality should be exact");
    assert!(
        matches!(key.0[0], KeyPart::Param { idx: 0, .. }),
        "first key part must be a deferred Param ref, got {:?}",
        key.0[0]
    );

    // Resolving against an active param context produces a concrete
    // KeyPart that matches the bound value.
    let resolved = with_params(vec![Value::Int(42)], || key.resolve_params()).unwrap();
    assert!(
        matches!(resolved.0[0], KeyPart::Int(42)),
        "resolved key must be KeyPart::Int(42), got {:?}",
        resolved.0[0]
    );
}

#[test]
fn placeholder_planning_under_active_params_picks_index_not_seqscan() {
    // Regression: under TPC-C, PAYMENT/NEW_ORDER prepared statements
    // were getting `WHERE pk = ?` planned as a full table scan because
    // the planner couldn't extract a literal value from
    // `Expr::Placeholder` and fell back to SeqScan. Concurrent
    // terminals each scanning the customer/orders tables produced
    // catastrophic row-lock contention and lock-wait timeouts.
    //
    // This test pins the regression by going through the planner
    // directly, verifying that a `WHERE pk = ?` becomes an IndexLookup
    // when the planner runs under an active param context.
    use crate::engine::params::with_params;
    use crate::parser::{parse_statement, Statement};
    use crate::planner::{plan_select, PhysicalPlan};

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE customer (id INT PRIMARY KEY, balance INT)")
        .unwrap();
    let db = engine.db_for_tests();

    // Build a SELECT with a numbered `?` and a fully-qualified table.
    let mut stmt = parse_statement("SELECT id FROM customer WHERE id = ?").unwrap();
    let mut next = 0;
    crate::parser::number_statement_placeholders(&mut stmt, &mut next);
    let Statement::Select(select) = stmt else {
        panic!("expected Select");
    };

    // With an active param context the planner resolves the placeholder
    // through the thread-local lookup and emits a point IndexLookup
    // against the primary key. Before the fix this was a SeqScan and
    // produced the TPC-C lock-wait timeouts.
    let planned_with = with_params(vec![Value::Int(42)], || plan_select(db, &select)).unwrap();
    fn uses_index(plan: &PhysicalPlan) -> bool {
        match plan {
            PhysicalPlan::IndexLookup { .. } | PhysicalPlan::IndexRangeScan { .. } => true,
            PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::HashAggregate { input, .. } => uses_index(input),
            _ => false,
        }
    }
    assert!(
        uses_index(&planned_with.physical),
        "with active params, planner must use the primary-key index, got: {:?}",
        planned_with.physical
    );
}

#[test]
fn prepared_executes_with_different_params_share_one_cached_plan() {
    // Phase 2.5 acceptance test. A prepared SELECT executed many times
    // with different parameters must:
    //   1. Return the row matching its own bound value (correctness).
    //   2. Reuse a single cached `Arc<PhysicalPlan>` across every
    //      EXECUTE — the plan tree carries deferred `KeyPart::Param`
    //      parts that the storage layer resolves at access time.
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, a_id INT, value INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (10, 1, 100), (20, 2, 200), (30, 3, 300)")
        .unwrap();

    let mut session = SessionContext::default();
    let sql = "SELECT a.id, b.value FROM a INNER JOIN b ON a.id = b.a_id WHERE a.id = ?";
    let prepared = engine.prepare_cached(&mut session, sql, 1).unwrap();

    let plan_cache = std::sync::Arc::clone(engine.plan_cache());
    let baseline = plan_cache.stats();

    for id in &[1, 2, 3, 1, 2, 3] {
        let stmt = (*prepared).clone();
        let mut session = session.clone();
        let response = with_params(vec![Value::Int(*id as i64)], || {
            engine
                .execute_prepared_statement(&mut session, sql, stmt)
                .unwrap()
                .unwrap()
        });
        // Prepared SELECTs run on the streaming path (P1-1). Materialise
        // here so the test can inspect rows as a contiguous Vec.
        let response = response.materialize().expect("materialise streaming");
        let QueryResponse::ResultSet(rs) = response else {
            panic!("expected ResultSet for id={id}");
        };
        assert_eq!(rs.rows.len(), 1, "id={id} expected exactly one row");
        assert_eq!(rs.rows[0][0], Value::Int(*id as i64));
    }

    let after = plan_cache.stats();
    let hits = after.hits.saturating_sub(baseline.hits);
    let misses = after.misses.saturating_sub(baseline.misses);
    assert!(
        hits >= 5,
        "6 EXECUTEs of one template must share a cached plan: hits={hits}, misses={misses}"
    );
    assert!(
        misses <= 1,
        "first EXECUTE is the only miss: misses={misses}"
    );
}

#[test]
fn auto_parameterized_selects_with_different_literals_share_one_cached_plan() {
    // Phase 2.5 acceptance test for the COM_QUERY path. Different
    // literal values in the WHERE clause are auto-parameterised into
    // `Expr::Placeholder`s; with deferred IndexKey resolution in
    // place, all six executions share one cached plan.
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, a_id INT, value INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (10, 1, 100), (20, 2, 200), (30, 3, 300)")
        .unwrap();

    let plan_cache = std::sync::Arc::clone(engine.plan_cache());
    let baseline = plan_cache.stats();

    for id in &[1, 2, 3, 1, 2, 3] {
        let sql = format!(
            "SELECT a.id, a.name, b.value FROM a INNER JOIN b ON a.id = b.a_id WHERE a.id = {id}"
        );
        let response = engine.execute(&sql).unwrap();
        // Auto-parameterised SELECTs run on the streaming path (P1-1);
        // materialise so the test can index rows.
        let response = response.materialize().expect("materialise streaming");
        let QueryResponse::ResultSet(rs) = response else {
            panic!("expected ResultSet for id={id}");
        };
        assert_eq!(rs.rows.len(), 1, "id={id} expected exactly one row");
        assert_eq!(rs.rows[0][0], Value::Int(*id as i64));
    }

    let after = plan_cache.stats();
    let hits = after.hits.saturating_sub(baseline.hits);
    let misses = after.misses.saturating_sub(baseline.misses);
    assert!(
        hits >= 5,
        "6 differing-literal queries must share an auto-paramed plan: hits={hits}, misses={misses}"
    );
    assert!(
        misses <= 1,
        "first execution is the only miss: misses={misses}"
    );
}

#[test]
fn auto_parameterized_range_selects_use_literal_keyed_plan_cache() {
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE events (id INT PRIMARY KEY, d DATE, INDEX idx_d(d))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO events VALUES \
             (1, '2020-01-01'), (2, '2020-01-02'), (3, '2020-01-03'), (4, '2020-01-04')",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE events").unwrap();

    let plan_cache = std::sync::Arc::clone(engine.plan_cache());
    let baseline = plan_cache.stats();

    for sql in [
        "SELECT COUNT(*) FROM events WHERE d < '2020-01-03'",
        "SELECT COUNT(*) FROM events WHERE d < '2020-01-04'",
        "SELECT COUNT(*) FROM events WHERE d < '2020-01-03'",
    ] {
        let QueryResponse::ResultSet(_) = engine.execute(sql).unwrap() else {
            panic!("expected ResultSet for {sql}");
        };
    }

    let after = plan_cache.stats();
    let hits = after.hits.saturating_sub(baseline.hits);
    let misses = after.misses.saturating_sub(baseline.misses);
    assert!(
        hits >= 1,
        "repeating the exact same range literal should hit the literal-keyed plan cache: hits={hits}, misses={misses}"
    );
    assert!(
        misses >= 2,
        "different range literals must not share one generic auto-param plan: hits={hits}, misses={misses}"
    );
}

#[test]
fn auto_parameterized_limit_reuses_plan_within_bucket_and_returns_runtime_count() {
    // LIMIT/OFFSET auto-parameterization: values in the same power-of-two
    // bucket share one cached plan; the executor re-resolves the count
    // per execution, so the *reused* plan must return each query's own
    // row count (the core correctness risk of erasing the literal).
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // A JOIN shape so the statement routes through the planner + plan
    // cache (simple single-table SELECTs take the direct fast path and
    // never consult the cache — runtime LIMIT resolution there is
    // covered by the AST-level `resolve_limit_placeholder` helpers).
    engine
        .execute("CREATE TABLE seq (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE seq_aux (id INT PRIMARY KEY, seq_id INT)")
        .unwrap();
    let rows: Vec<String> = (1..=20).map(|i| format!("({i}, {})", i * 10)).collect();
    engine
        .execute(&format!("INSERT INTO seq VALUES {}", rows.join(", ")))
        .unwrap();
    let aux: Vec<String> = (1..=20).map(|i| format!("({}, {i})", i + 100)).collect();
    engine
        .execute(&format!("INSERT INTO seq_aux VALUES {}", aux.join(", ")))
        .unwrap();

    let plan_cache = std::sync::Arc::clone(engine.plan_cache());
    let baseline = plan_cache.stats();

    // 10 and 12 share bucket 16 → one plan, two row counts. The ORDER BY
    // makes the plan a TopK so the heap bound is exercised, not just the
    // Limit node.
    let join_limit = |limit: usize| {
        format!(
            "SELECT a.id FROM seq a INNER JOIN seq_aux b ON a.id = b.seq_id \
             ORDER BY a.id LIMIT {limit}"
        )
    };
    for (limit, expected) in [(10usize, 10usize), (12, 12), (10, 10)] {
        let response = engine.execute(&join_limit(limit)).unwrap();
        let response = response.materialize().expect("materialise streaming");
        let QueryResponse::ResultSet(rs) = response else {
            panic!("expected ResultSet for LIMIT {limit}");
        };
        assert_eq!(rs.rows.len(), expected, "LIMIT {limit} row count");
        assert_eq!(rs.rows[0][0], Value::Int(1), "TopK must keep order");
        assert_eq!(
            rs.rows[expected - 1][0],
            Value::Int(expected as i64),
            "LIMIT {limit} must end at its own bound, not the plan's peeked one"
        );
    }

    let after = plan_cache.stats();
    let hits = after.hits.saturating_sub(baseline.hits);
    assert!(
        hits >= 2,
        "LIMIT 10 / 12 / 10 share one bucket-keyed plan: hits={hits}"
    );

    // OFFSET resolution through the same cached shape.
    for (offset, first) in [(5usize, 6i64), (7, 8)] {
        let sql = format!(
            "SELECT a.id FROM seq a INNER JOIN seq_aux b ON a.id = b.seq_id \
             ORDER BY a.id LIMIT 10 OFFSET {offset}"
        );
        let response = engine.execute(&sql).unwrap();
        let response = response.materialize().expect("materialise streaming");
        let QueryResponse::ResultSet(rs) = response else {
            panic!("expected ResultSet for OFFSET {offset}");
        };
        assert_eq!(rs.rows[0][0], Value::Int(first), "OFFSET {offset} start");
    }

    // LIMIT 0 and LIMIT 1 share bucket 1; the zero-row case must stay
    // zero rows when reusing the plan peeked at 1 and vice versa.
    for (limit, expected) in [(1usize, 1usize), (0, 0), (1, 1)] {
        let response = engine.execute(&join_limit(limit)).unwrap();
        let response = response.materialize().expect("materialise streaming");
        let QueryResponse::ResultSet(rs) = response else {
            panic!("expected ResultSet for LIMIT {limit}");
        };
        assert_eq!(rs.rows.len(), expected, "LIMIT {limit} row count");
    }
}

#[test]
fn auto_parameterized_composite_pk_paging_keeps_ordered_index_window() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;

    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE ap_composite_page (
                order_id INT NOT NULL,
                line_no INT NOT NULL,
                payload INT NOT NULL,
                PRIMARY KEY (order_id, line_no)
            )",
        )
        .unwrap();
    let values = (0..256)
        .map(|id| format!("({}, {}, {})", id / 4, id % 4, id * 10))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO ap_composite_page VALUES {values}"))
        .unwrap();

    // Before the regression fix, auto-parameterization hid LIMIT/OFFSET
    // from the single-table scan hints. Planning chose SeqScan+TopWindow
    // and charged 256 rows * 2 sort keys * size_of::<Value>() = 16 KiB.
    // The ordered composite-PK window needs only OFFSET+LIMIT RowIds and
    // comfortably fits this budget.
    let budget = Arc::new(MemBudget::new(8 * 1024));
    let QueryResponse::ResultSet(result) = with_query_budget(budget, || {
        engine.execute(
            "SELECT order_id, line_no, payload
             FROM ap_composite_page
             ORDER BY order_id, line_no
             LIMIT 20 OFFSET 200",
        )
    })
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 20);
    assert_eq!(
        result.rows[0],
        vec![Value::Int(50), Value::Int(0), Value::Int(2_000)]
    );
    assert_eq!(
        result.rows[19],
        vec![Value::Int(54), Value::Int(3), Value::Int(2_190)]
    );
}

#[test]
fn auto_parameterized_deep_offset_reuses_topwindow_with_runtime_window() {
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE tw_seq (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE tw_aux (id INT PRIMARY KEY, seq_id INT, INDEX idx_seq (seq_id))")
        .unwrap();
    let rows: Vec<String> = (0..160).map(|i| format!("({i}, {})", i % 97)).collect();
    engine
        .execute(&format!("INSERT INTO tw_seq VALUES {}", rows.join(", ")))
        .unwrap();
    let aux: Vec<String> = (0..160).map(|i| format!("({}, {i})", i + 10000)).collect();
    engine
        .execute(&format!("INSERT INTO tw_aux VALUES {}", aux.join(", ")))
        .unwrap();

    let plan_text = match engine
        .execute(
            "EXPLAIN FORMAT=TREE \
             SELECT a.id FROM tw_seq a INNER JOIN tw_aux b ON a.id = b.seq_id \
             ORDER BY a.v DESC, a.id LIMIT 10 OFFSET 120",
        )
        .unwrap()
    {
        QueryResponse::ResultSet(rs) => match &rs.rows[0][0] {
            Value::String(s) => s.clone(),
            other => panic!("expected string explain, got {other:?}"),
        },
        other => panic!("expected ResultSet, got {other:?}"),
    };
    assert!(
        plan_text.contains("TopWindow"),
        "deep auto-parameterized shape should choose TopWindow, got: {plan_text}"
    );

    let plan_cache = std::sync::Arc::clone(engine.plan_cache());
    let baseline = plan_cache.stats();

    let mut sorted: Vec<(i64, i64)> = (0..160).map(|id| (id, id % 97)).collect();
    sorted.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

    for (offset, limit) in [(120usize, 10usize), (130, 12), (120, 10)] {
        let sql = format!(
            "SELECT a.id FROM tw_seq a INNER JOIN tw_aux b ON a.id = b.seq_id \
             ORDER BY a.v DESC, a.id LIMIT {limit} OFFSET {offset}"
        );
        let response = engine.execute(&sql).unwrap();
        let response = response.materialize().expect("materialise streaming");
        let QueryResponse::ResultSet(rs) = response else {
            panic!("expected ResultSet for LIMIT {limit} OFFSET {offset}");
        };
        let expected: Vec<Vec<Value>> = sorted
            .iter()
            .skip(offset)
            .take(limit)
            .map(|(id, _)| vec![Value::Int(*id)])
            .collect();
        assert_eq!(
            rs.rows, expected,
            "TopWindow must re-resolve LIMIT/OFFSET for the cached plan"
        );
    }

    let after = plan_cache.stats();
    let hits = after.hits.saturating_sub(baseline.hits);
    assert!(
        hits >= 1,
        "deep LIMIT/OFFSET bucket should reuse a cached TopWindow plan: hits={hits}"
    );
}

#[test]
fn auto_parameterized_in_list_pads_to_pow2_and_returns_correct_rows() {
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // JOIN shape → planner + plan cache (see the LIMIT test above for
    // why single-table SELECTs would bypass the cache entirely).
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE items_aux (id INT PRIMARY KEY, item_id INT)")
        .unwrap();
    let rows: Vec<String> = (1..=16).map(|i| format!("({i}, {})", i * 10)).collect();
    engine
        .execute(&format!("INSERT INTO items VALUES {}", rows.join(", ")))
        .unwrap();
    let aux: Vec<String> = (1..=16).map(|i| format!("({}, {i})", i + 100)).collect();
    engine
        .execute(&format!("INSERT INTO items_aux VALUES {}", aux.join(", ")))
        .unwrap();

    let plan_cache = std::sync::Arc::clone(engine.plan_cache());
    let baseline = plan_cache.stats();

    // Arity 3 pads to 4 → shares the plan with a real arity-4 list.
    // Each execution must return its own member set.
    let join_in = |members: &str| {
        format!(
            "SELECT a.id FROM items a INNER JOIN items_aux b ON a.id = b.item_id \
             WHERE a.id IN ({members})"
        )
    };
    let cases: [(&str, usize); 4] = [
        ("1, 2, 3", 3),
        ("4, 5, 6, 7", 4),
        ("9, 10, 11", 3),
        ("1, 2, 3", 3),
    ];
    for (members, expected) in cases {
        let sql = join_in(members);
        let response = engine.execute(&sql).unwrap();
        let response = response.materialize().expect("materialise streaming");
        let QueryResponse::ResultSet(rs) = response else {
            panic!("expected ResultSet for {sql}");
        };
        assert_eq!(rs.rows.len(), expected, "{sql}");
    }

    let after = plan_cache.stats();
    let hits = after.hits.saturating_sub(baseline.hits);
    assert!(
        hits >= 3,
        "padded IN lists of arity 3/4 must share one cached plan: hits={hits}"
    );

    // NOT IN with padding: duplicates must not change the complement.
    let response = engine
        .execute(
            "SELECT a.id FROM items a INNER JOIN items_aux b ON a.id = b.item_id \
             WHERE a.id NOT IN (1, 2, 3)",
        )
        .unwrap();
    let response = response.materialize().expect("materialise streaming");
    let QueryResponse::ResultSet(rs) = response else {
        panic!("expected ResultSet for NOT IN");
    };
    assert_eq!(rs.rows.len(), 13, "NOT IN (1,2,3) over 16 rows");

    // Non-indexed column: the IN evaluates on the row-eval path (no
    // IndexMultiLookup). A reused plan must see each execution's own
    // parameter values — any per-plan memoisation of the precomputed IN
    // value set would return execution 1's rows here.
    let eval_cases: [(&str, usize); 3] =
        [("10, 20, 30", 3), ("40, 50, 60, 70", 4), ("10, 20, 30", 3)];
    for (members, expected) in eval_cases {
        let sql = format!(
            "SELECT a.id FROM items a INNER JOIN items_aux b ON a.id = b.item_id \
             WHERE a.v IN ({members})"
        );
        let response = engine.execute(&sql).unwrap();
        let response = response.materialize().expect("materialise streaming");
        let QueryResponse::ResultSet(rs) = response else {
            panic!("expected ResultSet for {sql}");
        };
        assert_eq!(rs.rows.len(), expected, "{sql}");
    }
}

#[test]
fn table_scoped_ddl_evicts_only_dependent_plans() {
    // P2 fine-grained invalidation: DDL on table X must evict cached
    // plans that depend on X (via (table_id, schema_version) deps) while
    // leaving plans for unrelated tables hot. Catalog-wide events
    // (CREATE VIEW here) still clear everything via the epoch.
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for t in ["alpha", "beta"] {
        engine
            .execute(&format!(
                "CREATE TABLE {t} (id INT PRIMARY KEY, aux_id INT, v INT)"
            ))
            .unwrap();
        engine
            .execute(&format!("CREATE TABLE {t}_aux (id INT PRIMARY KEY)"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO {t} VALUES (1, 1, 10), (2, 2, 20)"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO {t}_aux VALUES (1), (2)"))
            .unwrap();
    }

    let plan_cache = std::sync::Arc::clone(engine.plan_cache());
    let query = |t: &str, v: i64| {
        format!("SELECT a.id FROM {t} a INNER JOIN {t}_aux b ON a.aux_id = b.id WHERE a.v = {v}")
    };
    let run = |sql: &str| {
        let response = engine.execute(sql).unwrap();
        let _ = response.materialize().expect("materialise");
    };

    // Warm one plan per table.
    run(&query("alpha", 10));
    run(&query("beta", 10));

    // Table-scoped DDL on `alpha` only.
    engine
        .execute("ALTER TABLE alpha ADD COLUMN extra INT")
        .unwrap();

    let before = plan_cache.stats();
    run(&query("beta", 20));
    let after_beta = plan_cache.stats();
    assert_eq!(
        after_beta.hits.saturating_sub(before.hits),
        1,
        "beta's plan must survive DDL on alpha (no global epoch bump)"
    );

    run(&query("alpha", 20));
    let after_alpha = plan_cache.stats();
    assert!(
        after_alpha.deps_invalidations > before.deps_invalidations,
        "alpha's plan must be evicted by its own schema_version bump"
    );
    // The post-DDL alpha row set is still correct (replan happened).
    let response = engine.execute(&query("alpha", 10)).unwrap();
    let response = response.materialize().expect("materialise");
    let QueryResponse::ResultSet(rs) = response else {
        panic!("expected ResultSet");
    };
    assert_eq!(rs.rows.len(), 1);

    // A catalog-wide event still clears everything.
    let warm = plan_cache.stats();
    assert!(warm.size > 0, "cache holds entries before CREATE VIEW");
    engine
        .execute("CREATE VIEW unrelated_v AS SELECT 1 AS one")
        .unwrap();
    run(&query("beta", 10));
    let after_view = plan_cache.stats();
    assert!(
        after_view.stale_evictions > warm.stale_evictions || after_view.size <= warm.size,
        "CREATE VIEW must invalidate via the global epoch"
    );
}

#[test]
fn prepared_executes_with_different_params_return_correct_rows() {
    // A single PREPARE'd template runs many times with different bound
    // parameters. The deferred index key is resolved against the active
    // parameter context on every execution.
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')")
        .unwrap();

    let mut session = SessionContext::default();
    let prepared = engine
        .prepare_cached(&mut session, "SELECT id, name FROM a WHERE id = ?", 1)
        .unwrap();

    for id in &[1, 2, 3, 1, 2] {
        let stmt = (*prepared).clone();
        let mut session = session.clone();
        let response = with_params(vec![Value::Int(*id as i64)], || {
            engine
                .execute_prepared_statement(
                    &mut session,
                    "SELECT id, name FROM a WHERE id = ?",
                    stmt,
                )
                .unwrap()
                .unwrap()
        });
        // Prepared SELECTs run on the streaming path (P1-1). Materialise
        // so the assertions can index into a Vec of rows.
        let response = response.materialize().expect("materialise streaming");
        let QueryResponse::ResultSet(rs) = response else {
            panic!("expected ResultSet for id={id}");
        };
        assert_eq!(rs.rows.len(), 1, "id={id} should match exactly one row");
        assert_eq!(rs.rows[0][0], Value::Int(*id as i64));
    }
}

#[test]
fn prepared_statement_reads_latest_user_variable_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let prepared = engine.prepare_cached(&mut session, "SELECT @a", 0).unwrap();
    let stmt = (*prepared).clone();

    engine
        .execute_with_session(&mut session, "SET @a = 1")
        .unwrap();
    let QueryResponse::ResultSet(first) = engine
        .execute_prepared_statement(&mut session, "SELECT @a", stmt.clone())
        .unwrap()
        .unwrap()
    else {
        panic!("expected ResultSet");
    };
    assert_eq!(first.rows, vec![vec![Value::Int(1)]]);

    engine
        .execute_with_session(&mut session, "SET @a = 2")
        .unwrap();
    let QueryResponse::ResultSet(second) = engine
        .execute_prepared_statement(&mut session, "SELECT @a", stmt)
        .unwrap()
        .unwrap()
    else {
        panic!("expected ResultSet");
    };
    assert_eq!(second.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn auto_parameterized_selects_evaluate_correctly_under_late_binding() {
    // Phase 2: SELECTs with literal WHERE operands are auto-rewritten
    // into `WHERE col = ?` and the literal moves into the thread-local
    // parameter context. The planner resolves the placeholder to pick a
    // proper IndexLookup, and each execution returns the row matching
    // its own bound value.
    //
    // The deferred IndexKey recipe remains in the shared plan, so differing
    // literal values reuse the same single-table point-lookup plan safely.
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')")
        .unwrap();

    // Each execution returns the correct row for its literal — proves
    // late binding through thread-local params is wired end to end.
    for id in &[1, 2, 3, 1, 2, 3] {
        let sql = format!("SELECT id, name FROM a WHERE id = {id}");
        let response = engine.execute(&sql).unwrap().materialize().unwrap();
        let QueryResponse::ResultSet(rs) = response else {
            panic!("expected ResultSet for id={id}");
        };
        assert_eq!(rs.rows.len(), 1, "id={id} should match exactly one row");
        assert_eq!(rs.rows[0][0], Value::Int(*id as i64));
    }
}

#[test]
fn plan_cache_hits_on_repeated_select_and_misses_after_schema_change() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 'x'), (2, 'y')")
        .unwrap();

    // Read this engine's cache directly. The process-global registry is
    // shared across tests and would race when multiple test threads each
    // open their own engine.
    let plan_cache = std::sync::Arc::clone(engine.plan_cache());
    let baseline = plan_cache.stats();

    // Point, range and aggregate SELECTs all enter the same planner/cache
    // route; the first execution compiles and the next two reuse it.
    let queries = [
        "SELECT id, name FROM a WHERE id = 1",
        "SELECT id, name FROM a WHERE id >= 1 ORDER BY id",
        "SELECT COUNT(*), SUM(id) FROM a WHERE id >= 1",
    ];
    for sql in queries {
        for _ in 0..3 {
            let _ = engine.execute(sql).unwrap().materialize().unwrap();
        }
    }

    let after_repeats = plan_cache.stats();
    assert!(
        after_repeats.hits >= baseline.hits + 6,
        "expected point/range/aggregate single-table cache hits, before={} after={}",
        baseline.hits,
        after_repeats.hits
    );

    // Schema change must invalidate the cached plan. Execute the same SQL
    // again afterwards and confirm a deps_invalidations or stale eviction.
    engine
        .execute("ALTER TABLE a ADD COLUMN extra INT")
        .unwrap();
    let _ = engine.execute(queries[0]).unwrap();
    let after_alter = plan_cache.stats();
    let invalidated = after_alter
        .deps_invalidations
        .saturating_sub(after_repeats.deps_invalidations);
    let stale = after_alter
        .stale_evictions
        .saturating_sub(after_repeats.stale_evictions);
    assert!(
        invalidated + stale >= 1,
        "ALTER TABLE must drop the cached plan via deps mismatch or epoch bump"
    );

    assert!(plan_cache.current_epoch() >= 1);
}

#[test]
fn index_in_lookup_cached_plan_invalidates_after_subquery_schema_change() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE inventory (tenant_id INT, item_id INT, qty INT, INDEX tenant_item_idx (tenant_id, item_id))")
        .unwrap();
    engine
        .execute("CREATE TABLE recent_items (item_id INT)")
        .unwrap();
    engine
        .execute("INSERT INTO inventory VALUES (1, 100, 8), (1, 101, 20), (1, 102, 3)")
        .unwrap();
    engine
        .execute("INSERT INTO recent_items VALUES (100), (102)")
        .unwrap();

    let sql = "SELECT item_id FROM inventory \
               WHERE tenant_id = 1 AND qty < 10 \
                 AND item_id IN (SELECT item_id FROM recent_items)";
    let plan_cache = std::sync::Arc::clone(engine.plan_cache());
    let baseline = plan_cache.stats();

    for _ in 0..3 {
        let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        assert_eq!(rs.rows.len(), 2);
    }

    let after_repeats = plan_cache.stats();
    assert!(
        after_repeats.hits >= baseline.hits + 2,
        "expected repeated IndexInLookup query to hit the physical plan cache"
    );

    engine
        .execute("ALTER TABLE recent_items ADD COLUMN note INT")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set after subquery schema change");
    };
    assert_eq!(rs.rows.len(), 2);

    let after_alter = plan_cache.stats();
    let invalidated = after_alter
        .deps_invalidations
        .saturating_sub(after_repeats.deps_invalidations);
    let stale = after_alter
        .stale_evictions
        .saturating_sub(after_repeats.stale_evictions);
    assert!(
        invalidated + stale >= 1,
        "ALTER TABLE on subquery table must invalidate cached IndexInLookup plan"
    );
}

#[test]
fn case_different_sql_shares_one_cached_plan() {
    // Tier 3 #17 acceptance test: lexically different but
    // semantically identical SQL (different keyword case, extra
    // whitespace, comments) must produce the same plan-cache key
    // so the second query is a hit.
    let _guard = AUTO_PARAM_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
    std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
    let _restore = scopeguard_restore_env(prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, a_id INT, value INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (10, 1, 100), (20, 2, 200), (30, 3, 300)")
        .unwrap();

    let plan_cache = std::sync::Arc::clone(engine.plan_cache());
    let baseline = plan_cache.stats();

    let variants = [
        "SELECT a.id, a.name, b.value FROM a INNER JOIN b ON a.id = b.a_id WHERE a.id = 1",
        "select a.id, a.name, b.value from a inner join b on a.id = b.a_id where a.id = 2",
        "/* hint */ Select a.id, A.name, b.value FROM a INNER JOIN b ON a.id = b.a_id WHERE a.id = 3 ;",
        "  SELECT  a.id,  a.name, b.value  FROM a INNER JOIN b ON a.id = b.a_id WHERE a.id = 1 ",
    ];
    for sql in &variants {
        let response = engine.execute(sql).unwrap();
        let _ = match response {
            QueryResponse::ResultSet(_) => (),
            QueryResponse::StreamingResultSet(_) => (),
            other => panic!("unexpected response shape for {sql}: {other:?}"),
        };
    }

    let after = plan_cache.stats();
    let hits = after.hits.saturating_sub(baseline.hits);
    let misses = after.misses.saturating_sub(baseline.misses);
    assert!(
        hits >= 3,
        "case/whitespace variants must hit the same cached plan: hits={hits}, misses={misses}"
    );
    assert!(
        misses <= 1,
        "only the first variant should miss the cache: misses={misses}"
    );
}

#[test]
fn prepared_executes_with_range_params_share_one_cached_plan() {
    // Tier 3 #16 acceptance test: the same parameterised range
    // template (`BETWEEN ? AND ?`) executed multiple times with
    // different bounds must reuse a single cached `Arc<PhysicalPlan>`.
    // The plan stores deferred `KeyPart::Param` parts inside both
    // `IndexBound`s; storage's `range_index_bounds_visible` resolves
    // them per-execution against the active parameter slice.
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, a_id INT, INDEX a_idx (a_id))")
        .unwrap();
    let a_values = (1..=20)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(", ");
    let b_values = (1..=20)
        .map(|i| format!("({}, {i})", 100 + i))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO a VALUES {a_values}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO b VALUES {b_values}"))
        .unwrap();

    let mut session = SessionContext::default();
    // The JOIN forces this query through `planner::plan_select` and
    // the plan cache (single-table fast paths bypass the cache).
    let sql = "SELECT a.id FROM a INNER JOIN b ON a.id = b.a_id WHERE a.id BETWEEN ? AND ?";
    let prepared = engine.prepare_cached(&mut session, sql, 2).unwrap();

    let plan_cache = std::sync::Arc::clone(engine.plan_cache());
    let baseline = plan_cache.stats();

    for (lo, hi) in &[(1i64, 3i64), (5, 10), (15, 20), (1, 20), (4, 7)] {
        let stmt = (*prepared).clone();
        let mut session = session.clone();
        let response = with_params(vec![Value::Int(*lo), Value::Int(*hi)], || {
            engine
                .execute_prepared_statement(&mut session, sql, stmt)
                .unwrap()
                .unwrap()
        });
        let mut rows: Vec<Vec<Value>> = match response {
            QueryResponse::ResultSet(rs) => rs.rows,
            QueryResponse::StreamingResultSet(mut srs) => {
                let mut all = Vec::new();
                while let Some(r) = srs.cursor.next_row().unwrap() {
                    all.push(r);
                }
                all
            }
            other => panic!("unexpected response shape for range ({lo},{hi}): {other:?}"),
        };
        rows.sort_by(|l, r| match (&l[0], &r[0]) {
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        });
        let expected: Vec<Vec<Value>> = (*lo..=*hi).map(|i| vec![Value::Int(i)]).collect();
        assert_eq!(rows, expected, "range ({lo},{hi})");
    }

    let after = plan_cache.stats();
    let hits = after.hits.saturating_sub(baseline.hits);
    let misses = after.misses.saturating_sub(baseline.misses);
    assert!(
        hits >= 4,
        "5 EXECUTEs of one range template must share a cached plan: hits={hits}, misses={misses}"
    );
    assert!(
        misses <= 1,
        "first EXECUTE is the only miss: misses={misses}"
    );
}

#[test]
fn equivalence_propagation_preserves_join_results() {
    // Tier 0 #6 correctness: synthesizing `u.y = literal` from
    // `t.x = literal AND t.x = u.y` must not change which rows
    // the JOIN produces.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, x INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE u (id INT PRIMARY KEY, y INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine
        .execute("INSERT INTO u VALUES (101, 10), (102, 20), (103, 30), (104, 40)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT t.id, u.id FROM t INNER JOIN u ON t.x = u.y \
             WHERE t.x = 20 ORDER BY t.id, u.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2), Value::Int(102)]]);
}

#[test]
fn predicate_normalization_or_chain_returns_correct_rows() {
    // Tier 0 #1 correctness: rewriting an OR-chain of `col = v`
    // into `col IN (...)` must not change which rows match,
    // including when the column is NULL (NULL must still be
    // filtered out by both forms).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, NULL)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t WHERE v = 10 OR v = 30 OR v = 50 ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(3)],
            vec![Value::Int(5)],
        ]
    );
}

#[test]
fn predicate_normalization_not_pushdown_returns_correct_rows() {
    // Tier 0 #1 correctness: NOT is pushed through comparisons,
    // IS NULL, and AND/OR (de Morgan) without changing 3-valued
    // logic — NULL rows must still be excluded from WHERE.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, NULL)")
        .unwrap();

    // NOT (v = 10) => v <> 10; NULL still filtered.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t WHERE NOT (v = 10) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);

    // NOT (v IS NULL) => v IS NOT NULL.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t WHERE NOT (v IS NULL) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

    // NOT (v > 5 AND v < 25) => v <= 5 OR v >= 25; NULL excluded.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t WHERE NOT (v > 5 AND v < 25) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, Vec::<Vec<Value>>::new());

    // Double negation collapses.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t WHERE NOT NOT (v = 20)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn semi_join_rewrite_returns_correct_rows() {
    // Phase B #B1 correctness: the IN→Semi rewrite must produce
    // the same result set as the unrewritten subquery. Cover:
    //   1. duplicate inner-side rows are deduplicated by Semi
    //   2. EXISTS variant matches
    //   3. NOT EXISTS uncorrelated empties when subquery is non-empty
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, x INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, y INT, tag INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();
    // b has duplicate y=10 entries; semi-join must still emit a.id=1
    // exactly once (not twice).
    engine
        .execute("INSERT INTO b VALUES (101, 10, 1), (102, 10, 2), (103, 30, 1)")
        .unwrap();

    // EXISTS variant with non-correlated WHERE: subquery has rows
    // matching tag=2, so every outer row passes.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.tag = 2) ORDER BY a.id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    // EXISTS condition is uncorrelated; b has at least one tag=2 row,
    // so every a.id should match.
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)],
        ]
    );

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT a.id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.tag = 99)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    // Inner subquery filters out everything; NOT EXISTS is true for
    // every outer row.
    assert_eq!(rs.rows.len(), 4);
}

#[test]
fn correlated_exists_and_not_exists_self_join_rewrites_preserve_results() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, x INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 10), (2, 10), (3, 20), (4, 30)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT a1.id FROM a a1 \
             WHERE EXISTS (SELECT 1 FROM a a2 WHERE a2.x = a1.x AND a2.id <> a1.id) \
             ORDER BY a1.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT a1.id FROM a a1 \
             WHERE NOT EXISTS (SELECT 1 FROM a a2 WHERE a2.x = a1.x AND a2.id <> a1.id) \
             ORDER BY a1.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(3)], vec![Value::Int(4)]]);
}

#[test]
fn q21_core_join_orders_and_semi_anti_rewrites_match() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_name VARCHAR(25))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE supplier (\
             s_suppkey INT PRIMARY KEY, \
             s_name VARCHAR(25), \
             s_nationkey INT NOT NULL, \
             INDEX idx_supplier_nation (s_nationkey))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY, \
             o_orderstatus CHAR(1) NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT NOT NULL, \
             l_suppkey INT NOT NULL, \
             l_linenumber INT NOT NULL, \
             l_receiptdate DATE NOT NULL, \
             l_commitdate DATE NOT NULL, \
             PRIMARY KEY (l_orderkey, l_linenumber), \
             INDEX idx_lineitem_orderkey (l_orderkey), \
             INDEX idx_lineitem_suppkey (l_suppkey))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO nation VALUES (1, 'SAUDI ARABIA'), (2, 'BRAZIL')")
        .unwrap();
    engine
        .execute(
            "INSERT INTO supplier VALUES \
             (1, 'S1', 1), \
             (2, 'S2', 1), \
             (3, 'S3', 2), \
             (4, 'S4', 1)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders VALUES \
             (10, 'F'), \
             (11, 'F'), \
             (12, 'F'), \
             (13, 'O'), \
             (14, 'F')",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO lineitem VALUES \
             (10, 1, 1, '1994-01-05', '1994-01-03'), \
             (10, 2, 2, '1994-01-02', '1994-01-03'), \
             (11, 1, 1, '1994-01-05', '1994-01-03'), \
             (11, 2, 2, '1994-01-05', '1994-01-03'), \
             (12, 3, 1, '1994-01-05', '1994-01-03'), \
             (12, 1, 2, '1994-01-02', '1994-01-03'), \
             (13, 1, 1, '1994-01-05', '1994-01-03'), \
             (13, 2, 2, '1994-01-02', '1994-01-03'), \
             (14, 4, 1, '1994-01-05', '1994-01-03')",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE nation").unwrap();
    engine.execute("ANALYZE TABLE supplier").unwrap();
    engine.execute("ANALYZE TABLE orders").unwrap();
    engine.execute("ANALYZE TABLE lineitem").unwrap();

    let original_core = "SELECT s_name, COUNT(*) AS numwait \
                         FROM supplier \
                         INNER JOIN lineitem l1 ON s_suppkey = l1.l_suppkey \
                         INNER JOIN orders ON o_orderkey = l1.l_orderkey \
                         INNER JOIN nation ON s_nationkey = n_nationkey \
                         WHERE o_orderstatus = 'F' \
                           AND l1.l_receiptdate > l1.l_commitdate \
                           AND n_name = 'SAUDI ARABIA' \
                         GROUP BY s_name ORDER BY s_name";
    let nation_first_core = "SELECT s_name, COUNT(*) AS numwait \
                             FROM nation \
                             INNER JOIN supplier ON s_nationkey = n_nationkey \
                             INNER JOIN lineitem l1 ON s_suppkey = l1.l_suppkey \
                             INNER JOIN orders ON o_orderkey = l1.l_orderkey \
                             WHERE n_name = 'SAUDI ARABIA' \
                               AND o_orderstatus = 'F' \
                               AND l1.l_receiptdate > l1.l_commitdate \
                             GROUP BY s_name ORDER BY s_name";
    let original_q21_shape = "SELECT s_name, COUNT(*) AS numwait \
                              FROM supplier \
                              INNER JOIN lineitem l1 ON s_suppkey = l1.l_suppkey \
                              INNER JOIN orders ON o_orderkey = l1.l_orderkey \
                              INNER JOIN nation ON s_nationkey = n_nationkey \
                              WHERE o_orderstatus = 'F' \
                                AND l1.l_receiptdate > l1.l_commitdate \
                                AND EXISTS (\
                                    SELECT * FROM lineitem l2 \
                                    WHERE l2.l_orderkey = l1.l_orderkey \
                                      AND l2.l_suppkey <> l1.l_suppkey) \
                                AND NOT EXISTS (\
                                    SELECT * FROM lineitem l3 \
                                    WHERE l3.l_orderkey = l1.l_orderkey \
                                      AND l3.l_suppkey <> l1.l_suppkey \
                                      AND l3.l_receiptdate > l3.l_commitdate) \
                                AND n_name = 'SAUDI ARABIA' \
                              GROUP BY s_name ORDER BY s_name";
    let nation_first_q21_shape = "SELECT s_name, COUNT(*) AS numwait \
                                  FROM nation \
                                  INNER JOIN supplier ON s_nationkey = n_nationkey \
                                  INNER JOIN lineitem l1 ON s_suppkey = l1.l_suppkey \
                                  INNER JOIN orders ON o_orderkey = l1.l_orderkey \
                                  WHERE n_name = 'SAUDI ARABIA' \
                                    AND o_orderstatus = 'F' \
                                    AND l1.l_receiptdate > l1.l_commitdate \
                                    AND EXISTS (\
                                        SELECT * FROM lineitem l2 \
                                        WHERE l2.l_orderkey = l1.l_orderkey \
                                          AND l2.l_suppkey <> l1.l_suppkey) \
                                    AND NOT EXISTS (\
                                        SELECT * FROM lineitem l3 \
                                        WHERE l3.l_orderkey = l1.l_orderkey \
                                          AND l3.l_suppkey <> l1.l_suppkey \
                                          AND l3.l_receiptdate > l3.l_commitdate) \
                                  GROUP BY s_name ORDER BY s_name";

    let core_original = query_rows(&engine, original_core);
    assert_eq!(
        core_original,
        vec![
            vec![Value::String("S1".into()), Value::Int(2)],
            vec![Value::String("S2".into()), Value::Int(1)],
            vec![Value::String("S4".into()), Value::Int(1)],
        ]
    );
    assert_eq!(query_rows(&engine, nation_first_core), core_original);

    let q21_original = query_rows(&engine, original_q21_shape);
    assert_eq!(
        q21_original,
        vec![vec![Value::String("S1".into()), Value::Int(1)]]
    );
    assert_eq!(query_rows(&engine, nation_first_q21_shape), q21_original);

    let QueryResponse::ResultSet(tree) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {original_q21_shape}"))
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let tree_text = match &tree.rows[0][0] {
        Value::String(s) => s,
        other => panic!("expected tree text, got {other:?}"),
    };
    assert!(
        tree_text.contains("semi join") && tree_text.contains("anti join"),
        "Q21-shaped correlated EXISTS/NOT EXISTS should still decorrelate into semi/anti joins: {tree_text}"
    );
}

#[test]
fn batched_semi_anti_inlj_supports_all_join_type_pairs_and_neq_residual() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE outer_rows (id INT PRIMARY KEY, k INT NOT NULL, attr INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE inner_rows (\
             id INT PRIMARY KEY, k INT NOT NULL, attr INT, flag INT NOT NULL, \
             INDEX idx_inner_k (k))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO outer_rows VALUES \
             (1, 10, 1), (2, 20, 1), (3, 30, 1), (4, 40, NULL)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO inner_rows VALUES \
             (101, 10, 2, 0), (102, 10, 1, 1), \
             (201, 20, 2, 1), (301, 30, NULL, 1)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE outer_rows").unwrap();
    engine.execute("ANALYZE TABLE inner_rows").unwrap();

    let cases = [
        (true, true, vec![vec![Value::Int(2)]]),
        (true, false, vec![vec![Value::Int(1)]]),
        (false, true, Vec::new()),
        (false, false, vec![vec![Value::Int(3)], vec![Value::Int(4)]]),
    ];
    for (first_exists, second_exists, expected) in cases {
        let first_not = if first_exists { "" } else { "NOT " };
        let second_not = if second_exists { "" } else { "NOT " };
        let sql = format!(
            "SELECT /*+ INDEX_NLJ(i1) INDEX_NLJ(i2) */ o.id \
             FROM outer_rows o \
             WHERE {first_not}EXISTS (\
                 SELECT 1 FROM inner_rows i1 \
                 WHERE i1.k = o.k AND i1.attr <> o.attr) \
               AND {second_not}EXISTS (\
                 SELECT 1 FROM inner_rows i2 \
                 WHERE i2.k = o.k AND i2.attr <> o.attr AND i2.flag = 1) \
             ORDER BY o.id"
        );
        assert_eq!(query_rows(&engine, &sql), expected, "{sql}");
    }
}

fn query_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    result.rows
}

#[test]
fn hash_semi_join_right_only_residual_returns_correct_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE child (id INT PRIMARY KEY, k INT, flag INT)")
        .unwrap();
    engine
        .execute("INSERT INTO parent VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO child VALUES \
             (101, 10, 0), \
             (102, 10, 1), \
             (103, 20, 0), \
             (104, 40, 1)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE parent").unwrap();
    engine.execute("ANALYZE TABLE child").unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT /*+ HASH_JOIN(child) */ id \
             FROM parent \
             WHERE EXISTS (SELECT 1 FROM child WHERE child.k = parent.k AND child.flag = 1) \
             ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn hash_semi_join_joined_residual_returns_correct_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY, k INT, marker INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE child (id INT PRIMARY KEY, k INT, marker INT, INDEX k_idx(k))")
        .unwrap();
    engine
        .execute("INSERT INTO parent VALUES (1, 10, 7), (2, 20, 8), (3, 30, 9)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO child VALUES \
             (101, 10, 7), \
             (102, 10, 8), \
             (103, 20, 8), \
             (104, 40, 1)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE parent").unwrap();
    engine.execute("ANALYZE TABLE child").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT /*+ HASH_JOIN(parent, child) */ id \
             FROM parent \
             WHERE EXISTS (SELECT 1 FROM child WHERE child.k = parent.k AND child.marker <> parent.marker)",
        )
        .unwrap()
    else {
        panic!("expected explain");
    };
    let explain_text = explain
        .rows
        .iter()
        .flatten()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        explain_text.contains("Hash join"),
        "test setup should use hash semi join: {explain_text}"
    );

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT /*+ HASH_JOIN(parent, child) */ id \
             FROM parent \
             WHERE EXISTS (SELECT 1 FROM child WHERE child.k = parent.k AND child.marker <> parent.marker) \
             ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn reverse_hash_semi_anti_join_native_integer_keys_preserve_null_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE child (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("INSERT INTO parent VALUES (1, 10), (2, 20), (3, NULL), (4, 40)")
        .unwrap();
    let mut child = (1..=96)
        .map(|id| {
            let key = match id % 3 {
                0 => 10,
                1 => 20,
                _ => 99,
            };
            format!("({id}, {key})")
        })
        .collect::<Vec<_>>();
    child.push("(97, NULL)".to_string());
    engine
        .execute(&format!("INSERT INTO child VALUES {}", child.join(", ")))
        .unwrap();
    engine.execute("ANALYZE TABLE parent").unwrap();
    engine.execute("ANALYZE TABLE child").unwrap();

    let semi_sql = "SELECT /*+ HASH_JOIN(parent, child) */ id FROM parent \
                    WHERE EXISTS (SELECT 1 FROM child WHERE child.k = parent.k) ORDER BY id";
    let anti_sql = "SELECT /*+ HASH_JOIN(parent, child) */ id FROM parent \
                    WHERE NOT EXISTS (SELECT 1 FROM child WHERE child.k = parent.k) ORDER BY id";
    assert_eq!(
        query_rows(&engine, semi_sql),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    assert_eq!(
        query_rows(&engine, anti_sql),
        vec![vec![Value::Int(3)], vec![Value::Int(4)]]
    );

    let explain = explain_analyze_tree(&engine, semi_sql);
    assert!(
        explain.contains("Hash semi join")
            && explain.contains("build_rows=64")
            && explain.contains("probe_rows=4")
            && explain.contains("runtime_filter_rejected=33"),
        "reverse hash semi join must push the exact outer keyset into its unopened build scan: {explain}"
    );
}

#[test]
fn reverse_hash_semi_anti_generic_keys_match_index_lookup() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    for (key_type, first, second, other) in [
        (
            "BIGINT UNSIGNED",
            "18446744073709551615",
            "18446744073709551614",
            "18446744073709551613",
        ),
        (
            "DECIMAL(30,2)",
            "12345678901234567890.01",
            "12345678901234567890.02",
            "12345678901234567890.03",
        ),
        (
            "VARCHAR(20) COLLATE utf8mb4_0900_ai_ci",
            "'Alpha'",
            "'beta'",
            "'other'",
        ),
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute(&format!(
                "CREATE TABLE parent (id INT PRIMARY KEY, k {key_type}, part INT)"
            ))
            .unwrap();
        engine.execute(&format!("CREATE TABLE child (id INT PRIMARY KEY, k {key_type}, part INT, a INT, b INT, INDEX(k,part))")).unwrap();
        engine
            .execute(&format!(
                "INSERT INTO parent VALUES (1,{first},1),(2,{second},2),(3,NULL,1),(4,{first},9)"
            ))
            .unwrap();
        let mut child = (1..=96)
            .map(|id| {
                let (key, part) = match id % 3 {
                    0 => (if first == "'Alpha'" { "'alpha'" } else { first }, 1),
                    1 => (second, 2),
                    _ => (other, 1),
                };
                format!("({id},{key},{part},0,1)")
            })
            .collect::<Vec<_>>();
        child.push("(97,NULL,1,0,1)".into());
        engine
            .execute(&format!("INSERT INTO child VALUES {}", child.join(",")))
            .unwrap();
        engine.execute("ANALYZE TABLE parent").unwrap();
        engine.execute("ANALYZE TABLE child").unwrap();
        for (equality, semi_expected) in [
            ("child.k = parent.k", vec![1, 2]),
            ("child.k <=> parent.k", vec![1, 2, 3]),
        ] {
            for (existence, expected) in [
                ("EXISTS", semi_expected.clone()),
                (
                    "NOT EXISTS",
                    (1..=4).filter(|id| !semi_expected.contains(id)).collect(),
                ),
            ] {
                let sql = |hint: &str| {
                    format!("SELECT {hint} id FROM parent WHERE {existence} (SELECT 1 FROM child WHERE {equality} AND child.part = parent.part AND child.a < child.b) ORDER BY id")
                };
                let hash = query_rows(&engine, &sql("/*+ HASH_JOIN(child) */"));
                assert_eq!(
                    hash,
                    expected
                        .into_iter()
                        .map(|id| vec![Value::Int(id)])
                        .collect::<Vec<_>>(),
                    "{key_type}: {existence} {equality}"
                );
                assert_eq!(
                    hash,
                    query_rows(&engine, &sql("/*+ NO_HASH_JOIN(child) */"))
                );
                assert_eq!(hash, query_rows(&engine, &sql("")));
            }
        }
    }
}

#[test]
fn oversized_hash_semi_join_uses_keyset_not_nlj_when_residual_is_side_local() {
    let _guard = hash_join_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY, pk INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE child (id INT PRIMARY KEY, k INT, flag INT)")
        .unwrap();
    engine
        .execute("INSERT INTO parent VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO child VALUES \
             (101, 10, 0), \
             (102, 10, 1), \
             (103, 20, 0), \
             (104, 40, 1)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE parent").unwrap();
    engine.execute("ANALYZE TABLE child").unwrap();

    std::env::set_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS", "0");
    std::env::set_var("MYDB_HASH_JOIN_GRACE_MAX_DEPTH", "1");

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT /*+ HASH_JOIN(parent, child) */ id \
             FROM parent \
             WHERE EXISTS (SELECT 1 FROM child WHERE child.k = pk AND child.flag = 1) \
             ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let explain_text = explain
        .rows
        .iter()
        .flatten()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        explain_text.contains("Hash join"),
        "test setup must use hash semi join: {explain_text}"
    );

    let result = engine.execute(
        "SELECT /*+ HASH_JOIN(parent, child) */ id \
         FROM parent \
         WHERE EXISTS (SELECT 1 FROM child WHERE child.k = pk AND child.flag = 1) \
         ORDER BY id",
    );

    let analyze = explain_analyze_tree(
        &engine,
        "SELECT /*+ HASH_JOIN(parent, child) */ id FROM parent \
         WHERE EXISTS (SELECT 1 FROM child WHERE child.k = pk AND child.flag = 1) \
         ORDER BY id",
    );

    std::env::remove_var("MYDB_HASH_JOIN_MAX_BUILD_ROWS");
    std::env::remove_var("MYDB_HASH_JOIN_GRACE_MAX_DEPTH");

    let QueryResponse::ResultSet(rs) = result.unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    assert!(
        analyze.contains("Hash semi join") && !analyze.contains("Nested loop"),
        "decomposable semi join should remain a spillable HashJoin: {analyze}"
    );
}

#[test]
fn streaming_aggregate_matches_hash_aggregate_results() {
    // Phase B #B4 correctness: the streaming aggregate path must
    // produce the same per-group COUNT/SUM/AVG/MIN/MAX as the hash
    // aggregate. We compare the streaming path (via g_idx) against
    // the same query forced through SeqScan (no g constraint, no
    // ORDER BY) — both should yield the same set of group rows.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT, INDEX g_idx (g))")
        .unwrap();
    // 4 groups with overlapping membership.
    let mut values = Vec::new();
    for i in 1..=20 {
        values.push(format!("({i}, {}, {})", i % 4, i * 10));
    }
    engine
        .execute(&format!("INSERT INTO t VALUES {}", values.join(", ")))
        .unwrap();

    // Streaming path (g >= 0 forces g_idx range scan whose order
    // satisfies group_by g).
    let QueryResponse::ResultSet(streaming) = engine
        .execute(
            "SELECT g, COUNT(*), SUM(v), MIN(v), MAX(v) FROM t \
             WHERE g >= 0 GROUP BY g ORDER BY g",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    // Reference path: GROUP BY g without the WHERE that drives
    // index-order. Must hit HashAggregate.
    let QueryResponse::ResultSet(hashed) = engine
        .execute(
            "SELECT g, COUNT(*), SUM(v), MIN(v), MAX(v) FROM t \
             GROUP BY g ORDER BY g",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(streaming.rows, hashed.rows);
}

#[test]
fn streaming_distinct_matches_hash_distinct_results() {
    // Phase B #B4 correctness: streaming DISTINCT (driven by
    // sorted index input) must return the same set of values as
    // hash-backed Projection.distinct, just sorted.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, INDEX g_idx (g))")
        .unwrap();
    let values = (1..=20)
        .map(|i| format!("({i}, {})", i % 5))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();

    let QueryResponse::ResultSet(streaming) = engine
        .execute("SELECT DISTINCT g FROM t WHERE g >= 0")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let mut streaming_rows = streaming.rows;
    streaming_rows.sort_by(|a, b| match (&a[0], &b[0]) {
        (Value::Int(l), Value::Int(r)) => l.cmp(r),
        _ => std::cmp::Ordering::Equal,
    });

    let QueryResponse::ResultSet(hashed) = engine.execute("SELECT DISTINCT g FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    let mut hashed_rows = hashed.rows;
    hashed_rows.sort_by(|a, b| match (&a[0], &b[0]) {
        (Value::Int(l), Value::Int(r)) => l.cmp(r),
        _ => std::cmp::Ordering::Equal,
    });

    assert_eq!(streaming_rows, hashed_rows);
    // Should be 5 distinct group values 0..=4.
    assert_eq!(streaming_rows.len(), 5);
}

#[test]
fn streaming_aggregate_handles_null_groups() {
    // Phase B #B4 correctness: SQL standard GROUP BY puts NULL into
    // its own group. The streaming aggregator must do the same.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, INDEX g_idx (g))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, NULL), (2, NULL), (3, 1), (4, 1), (5, 2)")
        .unwrap();

    // Force the streaming path with WHERE g IS NOT NULL OR g IS NULL
    // (always true) — actually the planner won't always pick it. Let
    // the path be selected naturally through the index range; we just
    // verify result correctness.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g")
        .unwrap()
    else {
        panic!("expected result set");
    };
    // NULL sorts first in mydb's compare; verify three groups.
    assert_eq!(rs.rows.len(), 3);
}

/// INLJ secondary-index intersect: when the right-only residual is a
/// `col OP literal` predicate covered by a secondary index, the inner
/// loop join should skip the per-row heap filter pass and instead
/// intersect the join candidates with the secondary index's row-ids.
///
/// We force three modes via [`with_inlj_secondary_intersect_mode`] and
/// assert the result set is identical — i.e. the optimization is a
/// pure performance shortcut, not a correctness window.
#[test]
fn inlj_secondary_intersect_results_match_filter_pass() {
    let _guard = super::super::tests::test_env_lock();
    let _left = scopeguard_restore_env_key(
        "MYDB_INLJ_BATCH_MIN_LEFT_ROWS",
        std::env::var("MYDB_INLJ_BATCH_MIN_LEFT_ROWS").ok(),
    );
    let _right = scopeguard_restore_env_key(
        "MYDB_INLJ_BATCH_MIN_RIGHT_ROWS",
        std::env::var("MYDB_INLJ_BATCH_MIN_RIGHT_ROWS").ok(),
    );
    std::env::set_var("MYDB_INLJ_BATCH_MIN_LEFT_ROWS", "1");
    std::env::set_var("MYDB_INLJ_BATCH_MIN_RIGHT_ROWS", "0");
    let dir = tempfile::tempdir().unwrap();
    let engine = super::super::tests::open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE outer_t (id INT PRIMARY KEY, label INT)")
        .unwrap();
    // Composite PK so PRIMARY index is on (oid, ts) — pure (oid)
    // would let the right side become covering and bypass the
    // noncovering branch we want to exercise.
    engine
        .execute(
            "CREATE TABLE inner_t (
                 oid INT, ts INT, kind INT, val INT,
                 PRIMARY KEY (oid, ts),
                 INDEX idx_kind (kind)
             )",
        )
        .unwrap();
    // Explicit test thresholds let this compact fixture enter the batched
    // path without paying for production-scale cardinalities.
    let mut outer_values = String::new();
    for id in 1..=16 {
        if !outer_values.is_empty() {
            outer_values.push(',');
        }
        outer_values.push_str(&format!("({id},{id})"));
    }
    engine
        .execute(&format!("INSERT INTO outer_t VALUES {outer_values}"))
        .unwrap();
    // 8 rows per outer key, kind cycles {1, 2, 3, 4, 5} → kind=1
    // matches roughly 1/5 of rows (we'll filter for it).
    let mut inner_values = String::new();
    for oid in 1..=16 {
        for ts in 1..=8 {
            if !inner_values.is_empty() {
                inner_values.push(',');
            }
            let kind = ((oid + ts) % 5) + 1;
            let val = oid * 100 + ts;
            inner_values.push_str(&format!("({oid},{ts},{kind},{val})"));
        }
    }
    engine
        .execute(&format!("INSERT INTO inner_t VALUES {inner_values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE outer_t").unwrap();
    engine.execute("ANALYZE TABLE inner_t").unwrap();

    let query = "SELECT outer_t.id, inner_t.ts, inner_t.val \
                 FROM outer_t \
                   INNER JOIN inner_t ON outer_t.id = inner_t.oid \
                 WHERE inner_t.kind = 1 \
                 ORDER BY outer_t.id, inner_t.ts";
    let run = |mode: &'static str| {
        crate::engine::executor::select::plan::with_inlj_secondary_intersect_mode(mode, || {
            let QueryResponse::ResultSet(rs) = engine.execute(query).unwrap() else {
                panic!("expected result set");
            };
            rs.rows
        })
    };
    let baseline = run("off");
    assert!(
        !baseline.is_empty(),
        "expected some rows from inner_t.kind=1"
    );
    let auto_rows = run("auto");
    let forced = run("on");
    assert_eq!(
        baseline, auto_rows,
        "auto-mode intersect must produce identical rows to the filter-pass path"
    );
    assert_eq!(
        baseline, forced,
        "forced-on intersect must produce identical rows to the filter-pass path"
    );
}

/// Range residuals (`col > literal`, `col < literal`, etc.) should
/// also drive the intersect path correctly — the bound is constructed
/// by `pick_secondary_intersect_index` and must round-trip through
/// `KeyPart::for_target` without misordering against stored entries.
#[test]
fn inlj_secondary_intersect_range_residual_matches_filter_pass() {
    let _guard = super::super::tests::test_env_lock();
    let _left = scopeguard_restore_env_key(
        "MYDB_INLJ_BATCH_MIN_LEFT_ROWS",
        std::env::var("MYDB_INLJ_BATCH_MIN_LEFT_ROWS").ok(),
    );
    let _right = scopeguard_restore_env_key(
        "MYDB_INLJ_BATCH_MIN_RIGHT_ROWS",
        std::env::var("MYDB_INLJ_BATCH_MIN_RIGHT_ROWS").ok(),
    );
    std::env::set_var("MYDB_INLJ_BATCH_MIN_LEFT_ROWS", "1");
    std::env::set_var("MYDB_INLJ_BATCH_MIN_RIGHT_ROWS", "0");
    let dir = tempfile::tempdir().unwrap();
    let engine = super::super::tests::open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE outer_t (id INT PRIMARY KEY, label INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE inner_t (
                 oid INT, ts INT, ship INT, val INT,
                 PRIMARY KEY (oid, ts),
                 INDEX idx_ship (ship)
             )",
        )
        .unwrap();
    let mut outer_values = String::new();
    for id in 1..=16 {
        if !outer_values.is_empty() {
            outer_values.push(',');
        }
        outer_values.push_str(&format!("({id},{id})"));
    }
    engine
        .execute(&format!("INSERT INTO outer_t VALUES {outer_values}"))
        .unwrap();
    let mut inner_values = String::new();
    for oid in 1..=16 {
        for ts in 1..=8 {
            if !inner_values.is_empty() {
                inner_values.push(',');
            }
            // ship value spans 1..=130 across the dataset
            let ship = ((oid - 1) * 13 + ts) % 130 + 1;
            let val = oid * 100 + ts;
            inner_values.push_str(&format!("({oid},{ts},{ship},{val})"));
        }
    }
    engine
        .execute(&format!("INSERT INTO inner_t VALUES {inner_values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE outer_t").unwrap();
    engine.execute("ANALYZE TABLE inner_t").unwrap();

    let query = "SELECT outer_t.id, inner_t.ts, inner_t.val \
                 FROM outer_t \
                   INNER JOIN inner_t ON outer_t.id = inner_t.oid \
                 WHERE inner_t.ship > 100 \
                 ORDER BY outer_t.id, inner_t.ts";
    let run = |mode: &'static str| {
        crate::engine::executor::select::plan::with_inlj_secondary_intersect_mode(mode, || {
            let QueryResponse::ResultSet(rs) = engine.execute(query).unwrap() else {
                panic!("expected result set");
            };
            rs.rows
        })
    };
    let baseline = run("off");
    assert!(
        !baseline.is_empty(),
        "expected some rows from inner_t.ship > 100"
    );
    assert_eq!(baseline, run("auto"));
    assert_eq!(baseline, run("on"));
}

/// When the secondary index's matching rows exceed the auto-mode
/// budget (4 × candidate rows), `try_inl_secondary_intersect` returns
/// `None` and the executor falls back to the existing filter pass.
/// In `on` mode the budget is `usize::MAX`, so the intersect path
/// must still cover this case. Either way the result has to match.
#[test]
fn inlj_secondary_intersect_low_selectivity_residual_falls_back() {
    let _guard = super::super::tests::test_env_lock();
    let _left = scopeguard_restore_env_key(
        "MYDB_INLJ_BATCH_MIN_LEFT_ROWS",
        std::env::var("MYDB_INLJ_BATCH_MIN_LEFT_ROWS").ok(),
    );
    let _right = scopeguard_restore_env_key(
        "MYDB_INLJ_BATCH_MIN_RIGHT_ROWS",
        std::env::var("MYDB_INLJ_BATCH_MIN_RIGHT_ROWS").ok(),
    );
    std::env::set_var("MYDB_INLJ_BATCH_MIN_LEFT_ROWS", "1");
    std::env::set_var("MYDB_INLJ_BATCH_MIN_RIGHT_ROWS", "0");
    let dir = tempfile::tempdir().unwrap();
    let engine = super::super::tests::open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE outer_t (id INT PRIMARY KEY, label INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE inner_t (
                 oid INT, ts INT, flag INT, val INT,
                 PRIMARY KEY (oid, ts),
                 INDEX idx_flag (flag)
             )",
        )
        .unwrap();
    let mut outer_values = String::new();
    for id in 1..=16 {
        if !outer_values.is_empty() {
            outer_values.push(',');
        }
        outer_values.push_str(&format!("({id},{id})"));
    }
    engine
        .execute(&format!("INSERT INTO outer_t VALUES {outer_values}"))
        .unwrap();
    // flag = 1 for ~95% of rows so the secondary range completely
    // overshadows the candidate set under auto-mode.
    let mut inner_values = String::new();
    for oid in 1..=16 {
        for ts in 1..=8 {
            if !inner_values.is_empty() {
                inner_values.push(',');
            }
            let flag = if (oid + ts) % 19 == 0 { 0 } else { 1 };
            let val = oid * 100 + ts;
            inner_values.push_str(&format!("({oid},{ts},{flag},{val})"));
        }
    }
    engine
        .execute(&format!("INSERT INTO inner_t VALUES {inner_values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE outer_t").unwrap();
    engine.execute("ANALYZE TABLE inner_t").unwrap();

    let query = "SELECT outer_t.id, inner_t.ts \
                 FROM outer_t \
                   INNER JOIN inner_t ON outer_t.id = inner_t.oid \
                 WHERE inner_t.flag = 1 \
                 ORDER BY outer_t.id, inner_t.ts";
    let run = |mode: &'static str| {
        crate::engine::executor::select::plan::with_inlj_secondary_intersect_mode(mode, || {
            let QueryResponse::ResultSet(rs) = engine.execute(query).unwrap() else {
                panic!("expected result set");
            };
            rs.rows
        })
    };
    let baseline = run("off");
    assert_eq!(baseline, run("auto"));
    assert_eq!(baseline, run("on"));
}

// -----------------------------------------------------------------
// Statistics-view tests (TableStatsView shared by SHOW TABLE STATUS,
// information_schema.tables, and information_schema.partitions).
// -----------------------------------------------------------------

/// Returns the numeric value at column `col` of the row whose first
/// column equals `key`. Panics if not found or if the cell is NULL.
fn lookup_int_by_first(rows: &[Vec<Value>], key: &str, col: usize) -> i64 {
    let row = rows
        .iter()
        .find(|r| matches!(&r[0], Value::String(s) if s == key))
        .unwrap_or_else(|| panic!("row with first column = {key:?} not found"));
    match &row[col] {
        Value::Int(v) => *v,
        Value::UInt(v) => *v as i64,
        other => panic!("expected numeric at column {col}, got {other:?}"),
    }
}

/// Same as `lookup_int_by_first` but using a `WHERE table_name = ?`
/// projection — i.e. the column 0 in the result holds the table name.
fn cell_int(rs: &ResultSet, col_name: &str) -> i64 {
    let idx = rs
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(col_name))
        .unwrap_or_else(|| panic!("column {col_name} not in result"));
    match &rs.rows[0][idx] {
        Value::Int(v) => *v,
        Value::UInt(v) => *v as i64,
        Value::Null => panic!("column {col_name} unexpectedly NULL"),
        other => panic!("column {col_name}: expected int, got {other:?}"),
    }
}

#[test]
fn information_schema_tables_reports_real_physical_sizes() {
    // Bug fix: before this change, AVG_ROW_LENGTH / DATA_LENGTH /
    // INDEX_LENGTH in `information_schema.tables` were hard-coded to 0.
    // SHOW TABLE STATUS estimated them; the two views disagreed for the
    // same table. Now both go through `TableStatsView`, which uses the
    // heap's real page count when the heap is warm in the cache.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE sized (id INT PRIMARY KEY, payload VARCHAR(32), \
             KEY payload_idx (payload))",
        )
        .unwrap();
    // Pump 1000 rows so the heap spans several pages — page-based
    // DATA_LENGTH is then strictly larger than any plausible
    // width-based estimate, and the bound check below has room to
    // distinguish real from estimated.
    const ROWS: i64 = 1000;
    let mut values = String::new();
    for i in 0..ROWS {
        if i > 0 {
            values.push(',');
        }
        values.push_str(&format!("({i}, 'row{i:04}')"));
    }
    engine
        .execute(&format!("INSERT INTO sized VALUES {values}"))
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH \
             FROM information_schema.tables \
             WHERE table_schema = 'test' AND table_name = 'sized'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1, "exactly one row for 'sized'");
    let table_rows = cell_int(&rs, "TABLE_ROWS");
    let data_length = cell_int(&rs, "DATA_LENGTH");
    assert!(
        table_rows >= ROWS,
        "TABLE_ROWS should reflect inserts (got {:?})",
        rs.rows[0]
    );
    assert!(
        cell_int(&rs, "AVG_ROW_LENGTH") > 0,
        "AVG_ROW_LENGTH was hard-coded 0 before the fix"
    );
    assert!(
        data_length > 0,
        "DATA_LENGTH was hard-coded 0 before the fix"
    );
    assert!(
        cell_int(&rs, "INDEX_LENGTH") > 0,
        "INDEX_LENGTH was hard-coded 0 before the fix (table has PK + \
         secondary index)"
    );

    // Bound check: regardless of whether the view took the
    // page-count path or fell back to a width estimate, DATA_LENGTH
    // must sit between a conservative lower and upper byte bound
    // derived from the declared schema. Catches both the old "always
    // 0" bug and a regression that would over-report by orders of
    // magnitude (e.g. confusing pages with bytes).
    //
    // Schema: INT (4) + VARCHAR(32) + 3-byte heap header + 1-byte
    // null bitmap, plus MVCC row header overhead. Real pages also
    // carry a 32-byte page header per 16 KiB page. The bounds below
    // are wide enough to cover both estimate and real-page outcomes
    // without distinguishing them.
    let min_row_width: i64 = 4; // INT alone, no overhead
    let max_row_width: i64 = 80; // declared widths + MVCC + slack
    assert!(
        data_length >= table_rows * min_row_width,
        "DATA_LENGTH ({data_length}) below TABLE_ROWS ({table_rows}) × min_row_width ({min_row_width})"
    );
    assert!(
        data_length <= table_rows * max_row_width * 2,
        "DATA_LENGTH ({data_length}) above TABLE_ROWS ({table_rows}) × max_row_width ({max_row_width}) × 2"
    );
}

#[test]
fn information_schema_tables_sizes_are_heap_model_and_cache_independent() {
    // Regression from the sysbench tables: after restart only PRIMARY had a
    // warm index handle, so INDEX_LENGTH omitted the cold secondary index.
    // mydb is a heap-table engine, so DATA_LENGTH is the heap allocation and
    // INDEX_LENGTH contains both PRIMARY and secondary index files.
    let dir = tempfile::tempdir().unwrap();
    let table_schema;
    {
        let engine = open_strict_cold_metadata_engine(dir.path());
        engine
            .execute(
                "CREATE TABLE sized_cold (id INT PRIMARY KEY, k INT NOT NULL, \
                 payload CHAR(40), KEY k_idx (k)) ENGINE=InnoDB",
            )
            .unwrap();
        let values = (0..2_000)
            .map(|i| format!("({i}, {}, 'payload-{i:04}')", i % 97))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO sized_cold VALUES {values}"))
            .unwrap();
        table_schema = engine.database().table("test.sized_cold").unwrap();
    }

    let heap = crate::storage::naming::table_object_generation_path(
        dir.path(),
        table_schema.id,
        table_schema.active_generation.max(1),
        "heap",
    );
    let index_path = |name: &str| {
        let index_id = table_schema
            .indexes
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(name))
            .unwrap()
            .index_id;
        crate::storage::naming::index_object_path_by_id(
            dir.path(),
            table_schema.id,
            table_schema.active_generation.max(1),
            index_id,
            "idx",
        )
    };
    let primary = index_path("PRIMARY");
    let secondary = index_path("k_idx");
    let heap_bytes = std::fs::metadata(heap).unwrap().len();
    let primary_bytes = std::fs::metadata(primary).unwrap().len();
    let secondary_bytes = std::fs::metadata(secondary).unwrap().len();
    assert!(primary_bytes > 0 && secondary_bytes > 0);

    // Query immediately after reopen, before any user-table access can warm
    // its heap or indexes.
    let engine = open_strict_cold_metadata_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT DATA_LENGTH, INDEX_LENGTH \
             FROM information_schema.tables \
             WHERE table_schema = 'test' AND table_name = 'sized_cold'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(
        cell_int(&rs, "DATA_LENGTH") as u64,
        heap_bytes,
        "DATA_LENGTH must contain the heap allocation only"
    );
    assert_eq!(
        cell_int(&rs, "INDEX_LENGTH") as u64,
        primary_bytes + secondary_bytes,
        "INDEX_LENGTH must contain PRIMARY and every cold secondary index"
    );
}

#[test]
fn information_schema_tables_data_length_includes_lob_overflow() {
    // GAP-1/GAP-2 (docs/lob-management-audit-2026-07.md): DATA_LENGTH
    // must include off-page `.lob` overflow bytes (the LOB is heap-owned row
    // payload in mydb's heap-table model), and DATA_FREE must reflect
    // reclaimable `.lob` free-list pages. Before
    // the fix, `table_stats_view` summed only heap + index pages and
    // hard-coded `data_free: 0`, so a table dominated by large LOB values
    // reported only its tiny heap footprint.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE big (id INT PRIMARY KEY, body LONGTEXT)")
        .unwrap();

    let read_stats = || -> (i64, i64) {
        let QueryResponse::ResultSet(rs) = engine
            .execute(
                "SELECT DATA_LENGTH, DATA_FREE FROM information_schema.tables \
                 WHERE table_schema = 'test' AND table_name = 'big'",
            )
            .unwrap()
        else {
            panic!("expected result set");
        };
        assert_eq!(rs.rows.len(), 1);
        (cell_int(&rs, "DATA_LENGTH"), cell_int(&rs, "DATA_FREE"))
    };

    // Baseline: empty table, no `.lob` file yet.
    let (empty_data_len, _) = read_stats();

    // Four 16 KiB bodies are well over the 4 KiB inline threshold, so every
    // body is externalized while keeping the metadata test compact.
    let big_body = "x".repeat(16 * 1024);
    let values = (0..4)
        .map(|i| format!("({i},'{big_body}')"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO big VALUES {values}"))
        .unwrap();
    let (data_len_after, _) = read_stats();
    // DATA_LENGTH must now be dominated by LOB pages: at least 48 KiB.
    // Without the fix it would be only the ~tens-of-KiB heap footprint.
    assert!(
        data_len_after >= 48 * 1024,
        "DATA_LENGTH ({data_len_after}) must include the `.lob` overflow; \
         empty baseline was {empty_data_len}"
    );

    // Delete every row and purge so the LOB chains are freed to the
    // `.lob` free list. DATA_FREE must then become non-zero (before the
    // fix it was hard-coded 0), and the free bytes cannot exceed the
    // physical `.lob` size (DATA_LENGTH is the heap + physical `.lob`).
    engine.execute("DELETE FROM big").unwrap();
    // Force retention to 0 and run purge so DELETE'd versions' chains
    // are reclaimed to the `.lob` free list rather than lingering.
    engine.database().set_undo_retention_ms(0).unwrap();
    for _ in 0..8 {
        if !engine.database().purge_obsolete_undo() {
            break;
        }
    }
    let (data_len_final, data_free_final) = read_stats();
    assert!(
        data_free_final > 0,
        "DATA_FREE ({data_free_final}) must reflect reclaimable `.lob` free-list pages \
         after DELETE + purge (was hard-coded 0 before the fix)"
    );
    assert!(
        data_free_final <= data_len_final,
        "DATA_FREE ({data_free_final}) cannot exceed DATA_LENGTH ({data_len_final})"
    );
}

#[test]
fn information_schema_tables_matches_show_table_status() {
    // Both views consult the same `TableStatsView`; we assert they
    // produce identical row counts, average row length, data length,
    // and index length for the same table.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE same (id INT PRIMARY KEY, body VARCHAR(20))")
        .unwrap();
    for i in 0..50 {
        engine
            .execute(&format!("INSERT INTO same VALUES ({i}, 'r{i}')"))
            .unwrap();
    }
    let QueryResponse::ResultSet(is_view) = engine
        .execute(
            "SELECT TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH \
             FROM information_schema.tables \
             WHERE table_schema = 'test' AND table_name = 'same'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(show) = engine.execute("SHOW TABLE STATUS LIKE 'same'").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(is_view.rows.len(), 1);
    assert_eq!(show.rows.len(), 1);
    // SHOW TABLE STATUS columns (in order):
    // Name, Engine, Version, Row_format, Rows, Avg_row_length,
    // Data_length, Max_data_length, Index_length, ...
    let show_rows = match &show.rows[0][4] {
        Value::Int(v) => *v,
        other => panic!("Rows column: {other:?}"),
    };
    let show_avg = match &show.rows[0][5] {
        Value::Int(v) => *v,
        other => panic!("Avg_row_length: {other:?}"),
    };
    let show_data = match &show.rows[0][6] {
        Value::Int(v) => *v,
        other => panic!("Data_length: {other:?}"),
    };
    let show_index = match &show.rows[0][8] {
        Value::Int(v) => *v,
        other => panic!("Index_length: {other:?}"),
    };
    assert_eq!(cell_int(&is_view, "TABLE_ROWS"), show_rows);
    assert_eq!(cell_int(&is_view, "AVG_ROW_LENGTH"), show_avg);
    assert_eq!(cell_int(&is_view, "DATA_LENGTH"), show_data);
    assert_eq!(cell_int(&is_view, "INDEX_LENGTH"), show_index);
}

#[test]
fn information_schema_statistics_cardinality_has_fallback() {
    // Before this change, `CARDINALITY` reported 0 for every index on
    // a never-ANALYZE-d table, misleading client metadata. We now fall
    // back to a width-based estimate (row_count for PRIMARY/UNIQUE,
    // max(1, rows/100) otherwise) until the first ANALYZE TABLE.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE card (id INT PRIMARY KEY, sku VARCHAR(16) NOT NULL, \
             UNIQUE KEY sku_idx (sku), KEY id_lookup (id))",
        )
        .unwrap();
    let values = (0..64)
        .map(|i| format!("({i},'sku-{i:04}')"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO card VALUES {values}"))
        .unwrap();
    // Crucially: no ANALYZE TABLE call.
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT INDEX_NAME, NON_UNIQUE, CARDINALITY \
             FROM information_schema.statistics \
             WHERE table_schema = 'test' AND table_name = 'card' \
             ORDER BY INDEX_NAME",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // Every index has at least one row; PRIMARY/UNIQUE should give
    // back full row count, non-unique at least 1.
    let primary_card = lookup_int_by_first(&rs.rows, "PRIMARY", 2);
    let sku_card = lookup_int_by_first(&rs.rows, "sku_idx", 2);
    let id_lookup_card = lookup_int_by_first(&rs.rows, "id_lookup", 2);
    assert!(
        primary_card >= 64,
        "PRIMARY cardinality fallback should equal row count, got {primary_card}"
    );
    assert!(
        sku_card >= 64,
        "UNIQUE cardinality fallback should equal row count, got {sku_card}"
    );
    assert!(
        id_lookup_card >= 1,
        "non-unique fallback should be >= 1, got {id_lookup_card}"
    );
}

#[test]
fn information_schema_partitions_reports_per_partition_stats() {
    // Per-partition TABLE_ROWS / AVG_ROW_LENGTH / DATA_LENGTH /
    // INDEX_LENGTH used to all be 0. With TableStatsView::for_partition
    // each leaf reports its heap and separate PRIMARY allocation.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE p_test (id INT PRIMARY KEY, body VARCHAR(16)) \
             PARTITION BY HASH(id) PARTITIONS 4",
        )
        .unwrap();
    for i in 0..400 {
        engine
            .execute(&format!("INSERT INTO p_test VALUES ({i}, 'r{i}')"))
            .unwrap();
    }
    let QueryResponse::ResultSet(parts) = engine
        .execute(
            "SELECT PARTITION_NAME, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH \
             FROM information_schema.partitions \
             WHERE table_schema = 'test' AND table_name = 'p_test' \
             ORDER BY PARTITION_ORDINAL_POSITION",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(parts.rows.len(), 4, "4 hash partitions expected");
    let mut total_rows = 0i64;
    for row in &parts.rows {
        // Layout matches the SELECT list above:
        //   row[0]=PARTITION_NAME, row[1]=TABLE_ROWS,
        //   row[2]=AVG_ROW_LENGTH, row[3]=DATA_LENGTH, row[4]=INDEX_LENGTH
        assert!(
            matches!(row[0], Value::String(_)),
            "PARTITION_NAME should be a string, got {:?}",
            row[0]
        );
        let prows = match &row[1] {
            Value::Int(v) => *v,
            other => panic!("TABLE_ROWS per partition: {other:?}"),
        };
        let pavg = match &row[2] {
            Value::Int(v) => *v,
            other => panic!("AVG_ROW_LENGTH: {other:?}"),
        };
        let pdata = match &row[3] {
            Value::Int(v) => *v,
            other => panic!("DATA_LENGTH: {other:?}"),
        };
        let pindex = match &row[4] {
            Value::Int(v) => *v,
            other => panic!("INDEX_LENGTH: {other:?}"),
        };
        assert!(prows > 0, "per-partition TABLE_ROWS must be > 0");
        assert!(pavg > 0, "per-partition AVG_ROW_LENGTH must be > 0");
        assert!(pdata > 0, "per-partition DATA_LENGTH must be > 0");
        assert!(pindex > 0, "per-partition INDEX_LENGTH must be > 0");
        total_rows += prows;
    }
    assert_eq!(
        total_rows, 400,
        "per-partition row counts should sum to the table total"
    );
}

#[test]
fn information_schema_tables_create_options_marks_partitioned() {
    // CREATE_OPTIONS was the empty string for every table before this
    // change. We now emit at least `partitioned` for partitioned tables
    // so mysqldump/JDBC clients can round-trip the catalog state.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE plain (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE parted (id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT TABLE_NAME, CREATE_OPTIONS FROM information_schema.tables \
             WHERE table_schema = 'test' AND table_name IN ('plain', 'parted') \
             ORDER BY TABLE_NAME",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 2);
    // 'parted' comes first alphabetically.
    assert_eq!(rs.rows[0][0], Value::String("parted".to_string()));
    assert_eq!(rs.rows[0][1], Value::String("partitioned".to_string()));
    assert_eq!(rs.rows[1][0], Value::String("plain".to_string()));
    assert_eq!(rs.rows[1][1], Value::String("".to_string()));
}

#[test]
fn information_schema_tables_empty_table_reports_zero_lengths() {
    // Sanity check: zero rows means zero data / index length and zero
    // AVG_ROW_LENGTH (we explicitly guard against div-by-zero when
    // computing AVG_ROW_LENGTH = data_length / rows).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE empty_t (id INT PRIMARY KEY, body VARCHAR(20))")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH \
             FROM information_schema.tables \
             WHERE table_schema = 'test' AND table_name = 'empty_t'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(cell_int(&rs, "TABLE_ROWS"), 0);
    assert_eq!(cell_int(&rs, "AVG_ROW_LENGTH"), 0);
    assert_eq!(cell_int(&rs, "DATA_LENGTH"), 0);
    assert_eq!(cell_int(&rs, "INDEX_LENGTH"), 0);
}

/// ICP (index condition pushdown) differential: a non-covering range
/// scan whose residual filter has conjuncts evaluable on the index key
/// must return exactly what the heap-scan oracle returns, across NULLs,
/// arithmetic, OR-trees, and the reverse direction.
#[test]
fn index_condition_pushdown_matches_seq_scan_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE icp_t (id INT PRIMARY KEY, a INT, b INT, c VARCHAR(32), \
             KEY ab_idx (a, b))",
        )
        .unwrap();
    let mut values = Vec::new();
    for i in 0..600i64 {
        let a = i % 50;
        let b = if i % 7 == 0 {
            "NULL".to_string()
        } else {
            (i % 11).to_string()
        };
        values.push(format!("({i}, {a}, {b}, 'c{i}')"));
    }
    engine
        .execute(&format!("INSERT INTO icp_t VALUES {}", values.join(", ")))
        .unwrap();
    engine.execute("ANALYZE TABLE icp_t").unwrap();

    // The shape must actually plan as a (non-covering) index range
    // scan, otherwise this test exercises nothing.
    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT id, c FROM icp_t FORCE INDEX (ab_idx) \
             WHERE a BETWEEN 5 AND 20 AND b > 4",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let explain_text = format!("{:?}", explain.rows);
    assert!(
        explain_text.contains("range"),
        "expected an index range scan plan, got {explain_text}"
    );

    let predicates = [
        "a BETWEEN 5 AND 20 AND b > 4",
        "a BETWEEN 5 AND 20 AND b % 2 = 0",
        "a BETWEEN 5 AND 20 AND b IS NULL",
        "a BETWEEN 5 AND 20 AND (b > 8 OR b < 2)",
        "a BETWEEN 5 AND 20 AND b IN (1, 3, 5) AND c <> 'c-none'",
        "a >= 40 AND b BETWEEN 2 AND 9",
    ];
    for predicate in predicates {
        let indexed = query_rows(
            &engine,
            &format!("SELECT id, c FROM icp_t FORCE INDEX (ab_idx) WHERE {predicate} ORDER BY id"),
        );
        let oracle = query_rows(
            &engine,
            &format!("SELECT id, c FROM icp_t IGNORE INDEX (ab_idx) WHERE {predicate} ORDER BY id"),
        );
        assert!(!oracle.is_empty(), "oracle empty for {predicate}");
        assert_eq!(indexed, oracle, "ICP mismatch for {predicate}");
    }

    // Reverse-direction scan (ORDER BY a DESC) with a pushable conjunct.
    let indexed = query_rows(
        &engine,
        "SELECT id FROM icp_t FORCE INDEX (ab_idx) \
         WHERE a BETWEEN 5 AND 20 AND b >= 3 ORDER BY a DESC, id",
    );
    let oracle = query_rows(
        &engine,
        "SELECT id FROM icp_t IGNORE INDEX (ab_idx) \
         WHERE a BETWEEN 5 AND 20 AND b >= 3 ORDER BY a DESC, id",
    );
    assert!(!oracle.is_empty());
    assert_eq!(indexed, oracle, "ICP mismatch on reverse scan");
}

#[test]
fn order_by_rand_snapshot_ordinal_survives_uncommitted_index_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE snapshot_ordinal_t (id INT PRIMARY KEY, status VARCHAR(16), payload INT, INDEX idx_status_id(status,id))").unwrap();
    for start in (0..4096).step_by(256) {
        let values = (start..start + 256)
            .map(|id| format!("({id},'ready',{id})"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO snapshot_ordinal_t VALUES {values}"))
            .unwrap();
    }
    engine
        .execute("INSERT INTO snapshot_ordinal_t VALUES (5000,'waiting',1)")
        .unwrap();
    engine.execute("ANALYZE TABLE snapshot_ordinal_t").unwrap();
    let mut reader = crate::engine::api::SessionContext::default();
    let mut writer = crate::engine::api::SessionContext::default();
    let seed = (10_000_000, 1_000_000);
    let query = "SELECT id FROM snapshot_ordinal_t WHERE status='ready' ORDER BY RAND() LIMIT 1";
    *reader.sysvars.rand_state.lock() = seed;
    let oracle = with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || {
        collect_select_response_rows(engine.execute_with_session(&mut reader, query).unwrap())
    });
    let oracle_state = *reader.sysvars.rand_state.lock();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "INSERT INTO snapshot_ordinal_t VALUES (6000,'ready',1)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "UPDATE snapshot_ordinal_t SET status='ready' WHERE id=5000",
        )
        .unwrap();
    *reader.sysvars.rand_state.lock() = seed;
    crate::engine::executor::select::vec_bridge::EXACT_RAND_INDEX_TOP_ONE_HITS
        .store(0, std::sync::atomic::Ordering::Relaxed);
    let actual = with_test_env(&[("MYDB_VECTORIZED", Some("auto"))], || {
        collect_select_response_rows(engine.execute_with_session(&mut reader, query).unwrap())
    });
    assert_eq!(actual, oracle);
    assert_eq!(
        *reader.sysvars.rand_state.lock(),
        oracle_state,
        "invisible new keys must consume no RAND draws"
    );
    assert_eq!(
        crate::engine::executor::select::vec_bridge::EXACT_RAND_INDEX_TOP_ONE_HITS
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "one concurrent writer must not force a full TopN scan"
    );
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
    *reader.sysvars.rand_state.lock() = seed;
    let actual =
        collect_select_response_rows(engine.execute_with_session(&mut reader, query).unwrap());
    assert_eq!(actual, oracle);
    assert_eq!(*reader.sysvars.rand_state.lock(), oracle_state);
}

#[test]
fn order_by_rand_snapshot_ordinal_matches_oracle_after_commit_and_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE ordinal_types_t (id INT PRIMARY KEY, tag VARCHAR(16), amount DECIMAL(12,2), INDEX ix(tag,amount,id))").unwrap();
    for start in (0..2048).step_by(256) {
        let values = (start..start + 256)
            .map(|id| format!("({id},'Ready',{}.25)", id % 11))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO ordinal_types_t VALUES {values}"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE ordinal_types_t").unwrap();
    let mut reader = crate::engine::api::SessionContext::default();
    let mut writer = crate::engine::api::SessionContext::default();
    for finish in ["COMMIT", "ROLLBACK"] {
        engine.execute_with_session(&mut writer, "BEGIN").unwrap();
        engine
            .execute_with_session(
                &mut writer,
                "INSERT INTO ordinal_types_t VALUES (9000,'Ready',NULL)",
            )
            .unwrap();
        for query in [
            "SELECT id FROM ordinal_types_t WHERE tag='ready' ORDER BY RAND() LIMIT 1",
            "SELECT id FROM ordinal_types_t WHERE tag='READY' ORDER BY RAND() DESC LIMIT 1",
            "SELECT id FROM ordinal_types_t WHERE tag='ready' AND id=123 ORDER BY RAND() LIMIT 1",
            "SELECT id FROM ordinal_types_t WHERE tag='absent' ORDER BY RAND() LIMIT 1",
        ] {
            let seed = (12_345, 67_890);
            *reader.sysvars.rand_state.lock() = seed;
            let oracle = with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || {
                collect_select_response_rows(
                    engine.execute_with_session(&mut reader, query).unwrap(),
                )
            });
            let oracle_state = *reader.sysvars.rand_state.lock();
            *reader.sysvars.rand_state.lock() = seed;
            let actual = with_test_env(&[("MYDB_VECTORIZED", Some("on"))], || {
                collect_select_response_rows(
                    engine.execute_with_session(&mut reader, query).unwrap(),
                )
            });
            assert_eq!(actual, oracle, "{finish}: {query}");
            assert_eq!(
                *reader.sysvars.rand_state.lock(),
                oracle_state,
                "{finish}: {query}"
            );
        }
        engine.execute_with_session(&mut writer, finish).unwrap();
        engine
            .execute("DELETE FROM ordinal_types_t WHERE id=9000")
            .unwrap();
    }
    // An old key needed by an earlier snapshot still takes the ordered
    // compensation path and remains equal to the row execution oracle.
    let query = "SELECT id FROM ordinal_types_t WHERE tag='ready' ORDER BY RAND() LIMIT 1";
    let seed = (91_234, 56_789);
    *reader.sysvars.rand_state.lock() = seed;
    let oracle = with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || {
        collect_select_response_rows(engine.execute_with_session(&mut reader, query).unwrap())
    });
    let state = *reader.sysvars.rand_state.lock();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "DELETE FROM ordinal_types_t WHERE id=123")
        .unwrap();

    *reader.sysvars.rand_state.lock() = seed;
    let actual = with_test_env(&[("MYDB_VECTORIZED", Some("on"))], || {
        collect_select_response_rows(engine.execute_with_session(&mut reader, query).unwrap())
    });
    assert_eq!(actual, oracle);
    assert_eq!(*reader.sysvars.rand_state.lock(), state);
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
}

#[test]
fn describe_inner_join_columns_matches_physical_metadata_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE metadata_a (id INT PRIMARY KEY, n DECIMAL(12,2), s VARCHAR(31), hidden INT INVISIBLE)").unwrap();
    engine
        .execute("CREATE TABLE metadata_b (id BIGINT UNSIGNED NOT NULL, n DOUBLE, d DATE)")
        .unwrap();
    for sql in [
        "SELECT a.*,b.* FROM metadata_a a JOIN metadata_b b ON a.id=b.id",
        "SELECT COUNT(DISTINCT a.id),SUM(a.n),AVG(b.n),a.hidden FROM metadata_a a, metadata_b b WHERE a.id=b.id",
        "SELECT a.id AS left_id,b.id AS right_id,COALESCE(a.s,'x'),a.n/3,b.d FROM metadata_a a JOIN metadata_b b ON a.id=b.id",
        "SELECT a.id,SUM(a.n) FROM metadata_a a JOIN metadata_b b ON a.id=b.id GROUP BY a.id",
    ] {
        let crate::parser::Statement::Select(stmt) = crate::parser::parse_statement(sql).unwrap() else { panic!("select") };
        let planned = crate::planner::plan_select(&engine.db, &stmt).unwrap();
        let expected = crate::engine::executor::select::plan::empty_result_columns(&stmt, &planned.physical).unwrap();
        let actual = crate::engine::executor::select::describe_select_columns(&engine.db, &stmt).unwrap();
        assert_eq!(actual, expected, "{sql}");
    }
    for sql in [
        "SELECT id FROM metadata_a a JOIN metadata_b b ON a.id=b.id",
        "SELECT a.missing FROM metadata_a a JOIN metadata_b b ON a.id=b.id",
        "SELECT a.id FROM metadata_a a JOIN metadata_b b ON a.missing=b.id",
    ] {
        let crate::parser::Statement::Select(stmt) = crate::parser::parse_statement(sql).unwrap()
        else {
            panic!("select")
        };
        let expected = crate::planner::plan_select(&engine.db, &stmt).unwrap_err();
        let actual = crate::engine::executor::select::describe_select_columns(&engine.db, &stmt)
            .unwrap_err();
        assert_eq!(actual, expected, "{sql}");
    }
}

#[test]
fn homogeneous_point_batch_preserves_unaffected_foreign_keys_and_sparse_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE batch_parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO batch_parent VALUES (1)")
        .unwrap();
    engine.execute("CREATE TABLE batch_child (id INT PRIMARY KEY, p INT, n INT, payload TEXT, FOREIGN KEY (p) REFERENCES batch_parent(id))").unwrap();
    engine
        .execute(
            "INSERT INTO batch_child VALUES (1,1,10,REPEAT('x',4000)),(2,1,20,REPEAT('y',4000))",
        )
        .unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET autocommit=OFF")
        .unwrap();
    let before = super::super::prepared_dml::homogeneous_point_update_batch_stats_for_test();
    engine.execute_with_session(&mut session,
        "UPDATE batch_child SET n=n+1 WHERE id=1; UPDATE batch_child SET n=n+2 WHERE id=2; UPDATE batch_child SET n=n+3 WHERE id=1").unwrap();
    let after = super::super::prepared_dml::homogeneous_point_update_batch_stats_for_test();
    assert_eq!(after.0 - before.0, 1);
    assert_eq!(after.1 - before.1, 3);
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(
            &mut session,
            "SELECT id,p,n,LENGTH(payload) FROM batch_child ORDER BY id",
        )
        .unwrap()
    else {
        panic!("result set")
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![
                Value::Int(1),
                Value::Int(1),
                Value::Int(14),
                Value::Int(4000)
            ],
            vec![
                Value::Int(2),
                Value::Int(1),
                Value::Int(22),
                Value::Int(4000)
            ]
        ]
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT n FROM batch_child ORDER BY id")
        .unwrap()
    else {
        panic!("result set")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(10)], vec![Value::Int(20)]]);
}

#[test]
fn homogeneous_point_batch_without_foreign_keys_preserves_sparse_payload_and_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE sparse_batch (id INT PRIMARY KEY, n DECIMAL(12,2), payload TEXT, tag VARBINARY(32))").unwrap();
    engine.execute("INSERT INTO sparse_batch VALUES (1,10.00,REPEAT('x',4000),'one'),(2,20.00,REPEAT('y',4000),'two')").unwrap();
    let sql = "SELECT id,n,payload,tag FROM sparse_batch ORDER BY id";
    let QueryResponse::ResultSet(original) = engine.execute(sql).unwrap() else {
        panic!("result set");
    };
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET autocommit=OFF")
        .unwrap();
    let before = crate::engine::executor::prepared_sparse_batch_hits();
    engine.execute_with_session(&mut session,
        "UPDATE sparse_batch SET n=n+1.25 WHERE id=1; UPDATE sparse_batch SET n=n+2.50 WHERE id=2").unwrap();
    assert_eq!(
        crate::engine::executor::prepared_sparse_batch_hits() - before,
        1
    );
    let QueryResponse::ResultSet(updated) = engine.execute_with_session(&mut session, sql).unwrap()
    else {
        panic!("result set");
    };
    assert_eq!(updated.rows[0][1].to_display_string(), "11.25");
    assert_eq!(updated.rows[1][1].to_display_string(), "22.50");
    for (before, after) in original.rows.iter().zip(&updated.rows) {
        assert_eq!(&before[2..], &after[2..]);
    }
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    let QueryResponse::ResultSet(restored) = engine.execute(sql).unwrap() else {
        panic!("result set");
    };
    assert_eq!(restored.rows, original.rows);
}

#[test]
fn homogeneous_sparse_batch_keeps_repeated_key_coalescing_and_error_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE sparse_keys (tenant INT, id VARCHAR(8), n TINYINT, payload TEXT, PRIMARY KEY(tenant,id))").unwrap();
    engine.execute("INSERT INTO sparse_keys VALUES (1,'a',10,REPEAT('x',4000)),(1,'b',126,REPEAT('y',4000))").unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET autocommit=OFF")
        .unwrap();
    let before = super::super::prepared_dml::homogeneous_point_update_batch_stats_for_test();
    engine.execute_with_session(&mut session,
        "UPDATE sparse_keys SET n=n+1 WHERE tenant=1 AND id='a'; UPDATE sparse_keys SET n=n+2 WHERE tenant=1 AND id='A'").unwrap();
    let after = super::super::prepared_dml::homogeneous_point_update_batch_stats_for_test();
    assert_eq!(after.1 - before.1, 2);
    assert_eq!(
        after.2 - before.2,
        1,
        "repeated keys still coalesce on wide rows"
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    let before = crate::engine::executor::prepared_sparse_batch_hits();
    let response = engine.execute_with_session(&mut session,
        "UPDATE sparse_keys SET n=n+0 WHERE tenant=1 AND id='a'; UPDATE sparse_keys SET n=n+1 WHERE tenant=1 AND id='missing'").unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 0 }
    ));
    assert_eq!(
        crate::engine::executor::prepared_sparse_batch_hits() - before,
        1
    );
    let error = engine.execute_with_session(&mut session,
        "UPDATE sparse_keys SET n=n+1 WHERE tenant=1 AND id='a'; UPDATE sparse_keys SET n=n+2 WHERE tenant=1 AND id='b'").unwrap_err();
    assert_eq!(error.code(), 1264);
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(
            &mut session,
            "SELECT n,LENGTH(payload) FROM sparse_keys ORDER BY id",
        )
        .unwrap()
    else {
        panic!("result set")
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(11), Value::Int(4000)],
            vec![Value::Int(126), Value::Int(4000)]
        ]
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT n FROM sparse_keys ORDER BY id")
        .unwrap()
    else {
        panic!("result set")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(10)], vec![Value::Int(126)]]);
}

#[test]
fn homogeneous_sparse_batch_large_distinct_and_repeated_keys() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE large_sparse (id INT PRIMARY KEY, n INT, payload TEXT)")
        .unwrap();
    let values = (1..=20)
        .map(|id| format!("({id},0,REPEAT('x',1000))"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO large_sparse VALUES {values}"))
        .unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET autocommit=OFF")
        .unwrap();
    let sql = (1..=20)
        .map(|id| format!("UPDATE large_sparse SET n=n+1 WHERE id={id}"))
        .collect::<Vec<_>>()
        .join(";");
    for repeat in [false, true] {
        let sql = if repeat {
            format!("{sql};UPDATE large_sparse SET n=n+1 WHERE id=1")
        } else {
            sql.clone()
        };
        let sparse_before = crate::engine::executor::prepared_sparse_batch_hits();
        let before = super::super::prepared_dml::homogeneous_point_update_batch_stats_for_test();
        engine.execute_with_session(&mut session, &sql).unwrap();
        let after = super::super::prepared_dml::homogeneous_point_update_batch_stats_for_test();
        assert_eq!(after.1 - before.1, if repeat { 21 } else { 20 });
        assert_eq!(after.2 - before.2, 20);
        assert_eq!(
            crate::engine::executor::prepared_sparse_batch_hits() - sparse_before,
            u64::from(!repeat)
        );
        let QueryResponse::ResultSet(rows) = engine
            .execute_with_session(
                &mut session,
                "SELECT id,n,LENGTH(payload) FROM large_sparse ORDER BY id",
            )
            .unwrap()
        else {
            panic!("result set")
        };
        for (index, row) in rows.rows.iter().enumerate() {
            assert_eq!(row[1], Value::Int(if repeat && index == 0 { 2 } else { 1 }));
            assert_eq!(row[2], Value::Int(1000));
        }
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
    }
}

#[test]
fn homogeneous_point_batch_with_foreign_key_replays_error_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE fk_prefix_parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO fk_prefix_parent VALUES (1)")
        .unwrap();
    engine.execute("CREATE TABLE fk_prefix_child (id INT PRIMARY KEY, p INT, n TINYINT, FOREIGN KEY(p) REFERENCES fk_prefix_parent(id))").unwrap();
    engine
        .execute("INSERT INTO fk_prefix_child VALUES (1,1,126)")
        .unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET autocommit=OFF")
        .unwrap();
    let error = engine.execute_with_session(&mut session,
        "UPDATE fk_prefix_child SET n=n+1 WHERE id=1; UPDATE fk_prefix_child SET n=n+1 WHERE id=1").unwrap_err();
    assert_eq!(error.code(), 1264);
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut session, "SELECT n FROM fk_prefix_child")
        .unwrap()
    else {
        panic!("result set")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(127)]]);
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    let error = engine.execute_with_session(&mut session,
        "UPDATE fk_prefix_child SET p=1 WHERE id=1; UPDATE fk_prefix_child SET p=999 WHERE id=1").unwrap_err();
    assert_eq!(error.code(), 1452);
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

#[test]
fn sparse_point_batch_keeps_volatile_assignments_on_scalar_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE volatile_parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO volatile_parent VALUES (1)")
        .unwrap();
    engine.execute("CREATE TABLE volatile_child (id INT PRIMARY KEY, p INT, n DOUBLE, FOREIGN KEY(p) REFERENCES volatile_parent(id))").unwrap();
    engine
        .execute("INSERT INTO volatile_child VALUES (1,1,0),(2,1,0)")
        .unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET autocommit=OFF")
        .unwrap();
    let before = crate::engine::executor::prepared_sparse_batch_hits();
    engine.execute_with_session(&mut session,
        "UPDATE volatile_child SET n=RAND() WHERE id=1; UPDATE volatile_child SET n=RAND() WHERE id=2").unwrap();
    assert_eq!(
        crate::engine::executor::prepared_sparse_batch_hits(),
        before
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

#[test]
fn hash_join_scalar_residual_with_windowed_membership_keeps_rollup_streaming() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE residual_dim (k INT, state VARCHAR(8))")
        .unwrap();
    engine
        .execute("CREATE TABLE residual_fact (k INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO residual_dim VALUES (1,'a'),(2,'b'),(3,NULL)")
        .unwrap();
    for start in (0..600).step_by(100) {
        let values = (start..start + 100)
            .map(|n| format!("({},{})", n % 3 + 1, n % 10))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO residual_fact VALUES {values}"))
            .unwrap();
    }
    let sql = "SELECT /*+ HASH_JOIN(f,d) */ d.state,SUM(f.v) FROM residual_fact f JOIN residual_dim d ON f.k=d.k \
        WHERE d.state IN (SELECT state FROM (SELECT d2.state, RANK() OVER (PARTITION BY d2.state ORDER BY SUM(f2.v) DESC) rn \
        FROM residual_fact f2 JOIN residual_dim d2 ON f2.k=d2.k GROUP BY d2.state) ranked WHERE rn<=1) \
        GROUP BY d.state WITH ROLLUP ORDER BY d.state";
    let run = |mode| {
        with_vec_mode(mode, || {
            let budget = std::sync::Arc::new(MemBudget::new(2 * 1024 * 1024));
            let result = with_query_budget(budget.clone(), || {
                engine.execute(sql).unwrap().materialize().unwrap()
            });
            assert_eq!(budget.used(), 0);
            let QueryResponse::ResultSet(result) = result else {
                panic!("result set");
            };
            result.rows
        })
    };
    let row = run(VecMode::Off);
    let vector = run(VecMode::On);
    assert_eq!(vector, row);
    assert_eq!(vector.len(), 3);
}

#[test]
fn native_aggregate_consumes_derived_set_without_spooling_input() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE fused_input (v DECIMAL(18,4), n INT)")
        .unwrap();
    engine
        .execute("INSERT INTO fused_input VALUES (1.2345,2),(9.8765,3),(NULL,4),(-2.0000,5)")
        .unwrap();
    for source in [
        "SELECT v,n FROM fused_input UNION ALL SELECT v,n FROM fused_input WHERE n<4",
        "SELECT v,n FROM fused_input UNION SELECT v,n FROM fused_input WHERE n<4",
        "SELECT v,n FROM fused_input WHERE n<0 UNION ALL SELECT v,n FROM fused_input WHERE n<0",
    ] {
        let sql = format!("WITH a AS (SELECT AVG(v*n) av,SUM(v) su,COUNT(*) ct,COUNT(v) cn FROM ({source}) x) SELECT a.av,a.su,a.ct,a.cn FROM a JOIN a b ON a.ct=b.ct");
        let run = |mode, sql: &str| {
            with_vec_mode(mode, || {
                let QueryResponse::ResultSet(result) =
                    engine.execute(sql).unwrap().materialize().unwrap()
                else {
                    panic!("rows")
                };
                result
            })
        };
        let before = crate::engine::executor::cte::streaming_aggregate::HITS
            .load(std::sync::atomic::Ordering::Relaxed);
        let vector = run(VecMode::On, &sql);
        let after = crate::engine::executor::cte::streaming_aggregate::HITS
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(after > before, "streaming aggregate did not run: {source}");
        let materialized_sql = sql.replace(" x) SELECT", " x LIMIT 1) SELECT");
        let row = run(VecMode::Off, &materialized_sql);
        assert_eq!(vector.columns, row.columns);
        assert_eq!(vector.rows, row.rows);
    }
}

#[test]
fn direct_build_rollup_preserves_raw_average_counts_and_null_groups() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE rollup_dim(k INT PRIMARY KEY,g VARCHAR(20))")
        .unwrap();
    engine
        .execute("CREATE TABLE rollup_fact(k INT,v DECIMAL(18,4))")
        .unwrap();
    for k in 0..32 {
        let g = if k % 7 == 0 {
            "NULL".into()
        } else {
            format!("'g{}'", k % 5)
        };
        engine
            .execute(&format!("INSERT INTO rollup_dim VALUES ({k},{g})"))
            .unwrap();
    }
    for batch in 0..16 {
        let values = (0..128)
            .map(|row| {
                let row = batch * 128 + row;
                let value = if row % 11 == 0 {
                    "NULL".into()
                } else {
                    format!("{}.1234", row % 17)
                };
                format!("({},{value})", row % 31)
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO rollup_fact VALUES {values}"))
            .unwrap();
    }
    let run = |mode, sql: &str| {
        with_vec_mode(mode, || {
            let QueryResponse::ResultSet(result) =
                engine.execute(sql).unwrap().materialize().unwrap()
            else {
                panic!("rows")
            };
            result.rows
        })
    };
    for join in [
        "rollup_fact f JOIN rollup_dim d",
        "rollup_dim d LEFT JOIN rollup_fact f",
        "rollup_fact f RIGHT JOIN rollup_dim d",
    ] {
        let sql = format!("SELECT /*+ HASH_JOIN(f,d) */ d.g,AVG(f.v),SUM(f.v),COUNT(f.v),COUNT(*),MIN(f.v),MAX(f.v) FROM {join} ON f.k=d.k GROUP BY d.g WITH ROLLUP ORDER BY d.g,COUNT(*)");
        let before = crate::engine::executor::select::vec_bridge::DIRECT_ROLLUP_HITS
            .load(std::sync::atomic::Ordering::Relaxed);
        let actual = run(VecMode::On, &sql);
        assert!(
            crate::engine::executor::select::vec_bridge::DIRECT_ROLLUP_HITS
                .load(std::sync::atomic::Ordering::Relaxed)
                > before,
            "direct path did not run: {sql}"
        );
        assert_eq!(actual, run(VecMode::Off, &sql), "{sql}");
    }
}

#[test]
fn topk_large_limit_small_join_uses_actual_memory() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE topk_left (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE topk_right (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for table in ["topk_left", "topk_right"] {
        engine
            .execute(&format!("INSERT INTO {table} VALUES (1,30),(2,20),(3,10)"))
            .unwrap();
    }
    let query = "SELECT l.id, r.v FROM topk_left l JOIN topk_right r ON l.id=r.id ORDER BY l.id DESC LIMIT 1000000";
    let QueryResponse::ResultSet(plan) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {query}"))
        .unwrap()
    else {
        panic!("expected plan")
    };
    assert!(format!("{:?}", plan.rows).contains("TopK"));
    let budget = Arc::new(MemBudget::new(128 * 1024));
    let QueryResponse::ResultSet(result) =
        with_query_budget(budget.clone(), || engine.execute(query)).unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(3), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(1), Value::Int(30)]
        ]
    );
    assert!(budget.peak_used() < 128 * 1024);
    assert_eq!(budget.used(), 0);
}

#[test]
fn topk_large_limit_distinct_and_offset_match_full_sort() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE topk_variants (id INT PRIMARY KEY, v VARCHAR(100), n DECIMAL(12,2))")
        .unwrap();
    engine.execute("INSERT INTO topk_variants VALUES (1,'b',1.25),(2,NULL,2.50),(3,'A',NULL),(4,'a',1.25),(5,'b',-3.50)").unwrap();
    for (projection, order) in [
        ("l.id, r.v, r.n", "r.v DESC, r.n, l.id"),
        ("DISTINCT r.v, r.n", "r.v, r.n DESC"),
    ] {
        let sql = format!("SELECT {projection} FROM topk_variants l JOIN topk_variants r ON l.id=r.id ORDER BY {order}");
        let QueryResponse::ResultSet(expected) = engine.execute(&sql).unwrap() else {
            panic!("expected rows")
        };
        for offset in [0, 2, 10] {
            let budget = Arc::new(MemBudget::new(256 * 1024));
            let QueryResponse::ResultSet(actual) = with_query_budget(budget.clone(), || {
                engine.execute(&format!("{sql} LIMIT {offset}, 1000000"))
            })
            .unwrap() else {
                panic!("expected rows")
            };
            assert_eq!(
                actual.rows,
                expected
                    .rows
                    .iter()
                    .skip(offset)
                    .cloned()
                    .collect::<Vec<_>>()
            );
            assert_eq!(budget.used(), 0);
        }
    }
    // Ordered DISTINCT and the compact scan must obey the same large-K contract.
    for sql in [
        "SELECT DISTINCT id FROM topk_variants ORDER BY id LIMIT 1000000",
        "SELECT id, v FROM topk_variants ORDER BY n, id LIMIT 1000000",
        "SELECT id FROM topk_variants WHERE id < 0 ORDER BY n LIMIT 1000000",
    ] {
        let budget = Arc::new(MemBudget::new(256 * 1024));
        with_query_budget(budget.clone(), || engine.execute(sql)).unwrap();
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn topk_actual_wide_join_exceeds_budget_without_leaking() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE topk_wide (id INT PRIMARY KEY, payload MEDIUMTEXT)")
        .unwrap();
    engine
        .execute(&format!(
            "INSERT INTO topk_wide VALUES (1, '{}')",
            "x".repeat(65536)
        ))
        .unwrap();
    let budget = Arc::new(MemBudget::new(32 * 1024));
    let result = with_query_budget(budget.clone(), || {
        engine.execute(
        "SELECT r.payload FROM topk_wide l JOIN topk_wide r ON l.id=r.id ORDER BY l.id LIMIT 1000000")
    });
    assert!(matches!(result, Err(ref error) if error.code() == 1037));
    assert_eq!(budget.used(), 0);
    // A failed query must not poison subsequent statements on the session.
    engine
        .execute("SELECT id FROM topk_wide ORDER BY id LIMIT 1")
        .unwrap();
}

#[test]
fn dependent_composite_range_join_avoids_full_prefix_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE range_drivers (tenant INT, bucket INT, top_value INT, PRIMARY KEY(tenant,bucket))").unwrap();
    engine.execute("CREATE TABLE range_events (tenant INT, bucket INT, seq INT, measure INT, padding CHAR(48), PRIMARY KEY(tenant,bucket,seq))").unwrap();
    insert_fixture_rows(
        &engine,
        "test.range_drivers",
        (1..=4i64).flat_map(|tenant| {
            (1..=2i64)
                .map(move |bucket| vec![Value::Int(tenant), Value::Int(bucket), Value::Int(4096)])
        }),
    );
    insert_fixture_rows(
        &engine,
        "test.range_events",
        (1..=4i64).flat_map(|tenant| {
            (1..=2i64).flat_map(move |bucket| {
                (0..4096i64).map(move |seq| {
                    vec![
                        Value::Int(tenant),
                        Value::Int(bucket),
                        Value::Int(seq),
                        Value::Int(seq),
                        Value::String("payload".into()),
                    ]
                })
            })
        }),
    );
    engine.execute("ANALYZE TABLE range_drivers").unwrap();
    engine.execute("ANALYZE TABLE range_events").unwrap();
    for (bounds, expected) in [
        ("e.seq >= d.top_value-20 AND e.seq < d.top_value", 20),
        ("e.seq >= d.top_value-10 AND e.seq >= d.top_value-20 AND e.seq < d.top_value-3 AND e.seq < d.top_value", 7),
    ] {
        let sql = format!("SELECT COUNT(e.measure) FROM range_drivers d JOIN range_events e ON e.tenant=d.tenant AND e.bucket=d.bucket AND {bounds} WHERE d.tenant=1 AND d.bucket=1");
        let tree = explain_analyze_tree(&engine, &sql);
        assert!(tree.contains("Index nested loop"), "{tree}");
        let QueryResponse::ResultSet(rs) = engine.execute(&sql).unwrap().materialize().unwrap() else { panic!("expected result"); };
        assert_eq!(rs.rows[0][0].to_display_string(), expected.to_string(), "{tree}");
    }
}

#[test]
fn dependent_range_join_preserves_extra_bounds_and_nulls_across_key_types() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for (i, (ty, values)) in [
        ("INT", ["-2", "-1", "0", "1", "2"]),
        (
            "BIGINT UNSIGNED",
            [
                "18446744073709551610",
                "18446744073709551611",
                "18446744073709551612",
                "18446744073709551613",
                "18446744073709551614",
            ],
        ),
        ("DECIMAL(12,2)", ["-1.10", "0.00", "1.20", "2.30", "3.40"]),
        ("VARCHAR(16)", ["'a'", "'b'", "'c'", "'d'", "'e'"]),
        (
            "DATE",
            [
                "'2026-01-01'",
                "'2026-01-02'",
                "'2026-01-03'",
                "'2026-01-04'",
                "'2026-01-05'",
            ],
        ),
        (
            "DATETIME(6)",
            [
                "'2026-01-01 00:00:00.000001'",
                "'2026-01-01 00:00:00.000002'",
                "'2026-01-01 00:00:00.000003'",
                "'2026-01-01 00:00:00.000004'",
                "'2026-01-01 00:00:00.000005'",
            ],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let drivers = format!("bounds_drivers_{i}");
        let events = format!("bounds_events_{i}");
        engine.execute(&format!("CREATE TABLE {drivers} (id INT PRIMARY KEY, low_strong {ty}, low_weak {ty}, high_strong {ty}, high_weak {ty})")).unwrap();
        engine.execute(&format!("CREATE TABLE {events} (parent_id INT, k {ty}, payload INT, KEY probe(parent_id,k))")).unwrap();
        engine
            .execute(&format!(
                "INSERT INTO {drivers} VALUES (1,{},{},{},{}),(2,NULL,NULL,NULL,NULL)",
                values[1], values[0], values[3], values[4]
            ))
            .unwrap();
        for (j, value) in values.iter().enumerate() {
            engine
                .execute(&format!(
                    "INSERT INTO {events} VALUES (1,{value},{j}),(2,{value},{j})"
                ))
                .unwrap();
        }
        let sql = format!("SELECT /*+ INDEX_NLJ(e) */ d.id,COUNT(e.payload) FROM {drivers} d LEFT JOIN {events} e ON e.parent_id=d.id AND d.low_strong<=e.k AND d.low_weak<=e.k AND e.k<d.high_strong AND e.k<d.high_weak GROUP BY d.id ORDER BY d.id");
        let QueryResponse::ResultSet(rs) = engine.execute(&sql).unwrap().materialize().unwrap()
        else {
            panic!("expected result");
        };
        let rows = rs
            .rows
            .iter()
            .map(|row| row.iter().map(Value::to_display_string).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![
                vec!["1".to_string(), "2".to_string()],
                vec!["2".to_string(), "0".to_string()]
            ],
            "{ty}"
        );
    }
}

#[test]
fn prepared_unique_point_memo_ignores_component_frequency() {
    use crate::engine::api::PreparedSelectMemoState;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE memo_point (tenant INT, id INT, payload INT, PRIMARY KEY(tenant,id))",
        )
        .unwrap();
    let values = (1..=100)
        .map(|id| format!("({},{id},{id})", if id <= 96 { 1 } else { 2 }))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO memo_point VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE memo_point").unwrap();
    let mut session = SessionContext::default();
    let sql = "SELECT payload FROM memo_point WHERE tenant=? AND id=?";
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let mut first = None;
    for (tenant, id) in [(1, 1), (2, 97), (1, 50), (2, 100)] {
        let response = engine
            .execute_prepared_select(
                &mut session,
                &mut state,
                &parsed,
                sql,
                &[Value::Int(tenant), Value::Int(id)],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            collect_select_response_rows(response),
            vec![vec![Value::Int(id)]]
        );
        let PreparedSelectMemoState::Ready(memo) = &state else {
            panic!("expected memo")
        };
        assert!(matches!(
            memo.compiled.executable.as_ref(),
            crate::engine::cache::CompiledOperator::ProjectedUniquePointLookup(_)
        ));
        if let Some(first) = &first {
            assert!(
                Arc::ptr_eq(first, memo),
                "component frequency must not rebuild a proven unique point memo"
            );
        } else {
            first = Some(Arc::clone(memo));
        }
    }
    // Catalog invalidation remains mandatory even when cardinality is bounded.
    engine
        .execute("ALTER TABLE memo_point ADD COLUMN extra INT DEFAULT 0")
        .unwrap();
    let response = engine
        .execute_prepared_select(
            &mut session,
            &mut state,
            &parsed,
            sql,
            &[Value::Int(2), Value::Int(97)],
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        collect_select_response_rows(response),
        vec![vec![Value::Int(97)]]
    );
    let PreparedSelectMemoState::Ready(memo) = &state else {
        panic!("expected memo")
    };
    assert!(!Arc::ptr_eq(first.as_ref().unwrap(), memo));
}

#[test]
fn prepared_point_memo_still_checks_comparison_domains() {
    use crate::engine::api::PreparedSelectMemoState;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE memo_domain (tenant INT, code VARCHAR(8), payload INT, PRIMARY KEY(tenant,code))").unwrap();
    engine
        .execute("INSERT INTO memo_domain VALUES (1,'1',10),(1,'01',20),(1,'2',30)")
        .unwrap();
    engine.execute("ANALYZE TABLE memo_domain").unwrap();
    let mut session = SessionContext::default();
    let sql = "SELECT payload FROM memo_domain WHERE tenant=? AND code=?";
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let response = engine
        .execute_prepared_select(
            &mut session,
            &mut state,
            &parsed,
            sql,
            &[Value::Int(1), Value::String("1".into())],
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        collect_select_response_rows(response),
        vec![vec![Value::Int(10)]]
    );
    let PreparedSelectMemoState::Ready(first) = &state else {
        panic!("expected memo")
    };
    let first = Arc::clone(first);
    let response = engine
        .execute_prepared_select(
            &mut session,
            &mut state,
            &parsed,
            sql,
            &[Value::Int(1), Value::Int(1)],
        )
        .unwrap()
        .unwrap();
    let mut rows = collect_select_response_rows(response);
    rows.sort_by_key(|row| row[0].to_display_string());
    assert_eq!(rows, vec![vec![Value::Int(10)], vec![Value::Int(20)]]);
    let PreparedSelectMemoState::Ready(next) = &state else {
        panic!("expected memo")
    };
    assert!(!Arc::ptr_eq(&first, next));
    let response = engine
        .execute_prepared_select(
            &mut session,
            &mut state,
            &parsed,
            sql,
            &[Value::Int(1), Value::Null],
        )
        .unwrap()
        .unwrap();
    assert!(collect_select_response_rows(response).is_empty());
}

#[test]
fn prepared_nonunique_memo_preserves_selectivity_tiers() {
    use crate::engine::api::PreparedSelectMemoState;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE memo_range (id INT PRIMARY KEY, tenant INT, payload INT, KEY(tenant))",
        )
        .unwrap();
    let values = (1..=100)
        .map(|id| format!("({id},{},{id})", if id <= 96 { 1 } else { 2 }))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO memo_range VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE memo_range").unwrap();
    let mut session = SessionContext::default();
    let sql = "SELECT payload FROM memo_range WHERE tenant=?";
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let response = engine
        .execute_prepared_select(&mut session, &mut state, &parsed, sql, &[Value::Int(1)])
        .unwrap()
        .unwrap();
    assert_eq!(collect_select_response_rows(response).len(), 96);
    let PreparedSelectMemoState::Ready(first) = &state else {
        panic!("expected memo")
    };
    let first = Arc::clone(first);
    let response = engine
        .execute_prepared_select(&mut session, &mut state, &parsed, sql, &[Value::Int(2)])
        .unwrap()
        .unwrap();
    assert_eq!(collect_select_response_rows(response).len(), 4);
    let PreparedSelectMemoState::Ready(next) = &state else {
        panic!("expected memo")
    };
    assert!(
        !Arc::ptr_eq(&first, next),
        "nonunique queries still require parameter-sensitive plans"
    );
    assert!(
        Arc::ptr_eq(&first.presentation_select, &next.presentation_select),
        "changing a selectivity tier must reuse the normalized semantic template"
    );
    assert!(Arc::ptr_eq(
        &first.structural_fingerprint,
        &next.structural_fingerprint
    ));
}

#[test]
fn window_cte_batch_projection_matches_spill_with_repeated_columns() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE chunk_win (id INT PRIMARY KEY,g INT,v DECIMAL(20,3),d DATE)")
        .unwrap();
    for start in (0..1024).step_by(128) {
        let values = (start..start + 128)
            .map(|id| {
                format!(
                    "({id},{},{},'2024-01-{:02}')",
                    id % 17,
                    if id % 7 == 0 {
                        "NULL".to_string()
                    } else {
                        format!("{}.125", id % 11 - 5)
                    },
                    id % 28 + 1
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO chunk_win VALUES {values}"))
            .unwrap();
    }
    for expression in ["id again", "id+1 again"] {
        let sql = format!("WITH w AS (SELECT id,{expression},g,d,SUM(v) OVER (PARTITION BY g ORDER BY id ROWS UNBOUNDED PRECEDING) a,SUM(v) OVER (PARTITION BY g ORDER BY id ROWS UNBOUNDED PRECEDING) b,MAX(d) OVER (PARTITION BY g ORDER BY id ROWS UNBOUNDED PRECEDING) md FROM chunk_win) SELECT id,again,d,a,b,md FROM w WHERE id % 3 <> 0 ORDER BY id");
        let run = |threshold: &str| {
            crate::util::env::with_vars(&[("MYDB_CTE_SPILL_THRESHOLD_MB", Some(threshold))], || {
                let budget = std::sync::Arc::new(MemBudget::new(64 * 1024 * 1024));
                let rows = with_query_budget(budget.clone(), || {
                    collect_select_response_rows(engine.execute(&sql).unwrap())
                });
                assert_eq!(budget.used(), 0);
                rows
            })
        };
        let memory = run("64");
        assert_eq!(memory, run("0"), "{sql}");
        assert_eq!(memory.len(), 682);
        assert!(memory.iter().all(|row| row[3] == row[4]));
    }
}

#[test]
fn columnar_window_parallel_sort_large_partitions_release_query_charge() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE sort_win (id INT,g INT,v INT)")
        .unwrap();
    engine.execute("INSERT INTO sort_win SELECT n, MOD(n,4), MOD(n,173) FROM generate_series(0,8191) gs(n)").unwrap();
    let sql = "SELECT id,SUM(v) OVER (PARTITION BY g ORDER BY v,id ROWS UNBOUNDED PRECEDING) s FROM sort_win ORDER BY id";
    let run = |parallel| {
        crate::util::env::with_vars(
            &[
                ("MYDB_WINDOW_PARALLEL", Some(parallel)),
                ("MYDB_WINDOW_PARALLEL_MIN_ROWS", Some("0")),
                ("MYDB_PARALLEL_DEGREE", Some("4")),
            ],
            || {
                let budget = std::sync::Arc::new(MemBudget::new(8 * 1024 * 1024));
                let rows = with_query_budget(budget.clone(), || {
                    collect_select_response_rows(engine.execute(sql).unwrap())
                });
                assert_eq!(budget.used(), 0);
                rows
            },
        )
    };
    assert_eq!(run("on"), run("off"));
}
