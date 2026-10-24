//! Aggregate calls nested in scalar function arguments, fully supported by MySQL 8.0:
//! `COALESCE(SUM(x),0)`, `IFNULL(MAX(x),...)`, `ROUND(AVG(x),...)`, and related expressions.
//! Regression: `has_aggregate_expr` / `expr_has_aggregate` did not recurse into arguments
//! of non-aggregate functions, causing non-aggregate planning and row-level evaluation errors.
//! ERROR 1111 "invalid use of group function"。
use super::support::query_one_row as one_row;
use super::*;

#[test]
fn literal_keyed_ranges_keep_parallel_scans_and_prepared_ranges_keep_the_guard() {
    use crate::engine::auto_param::{auto_parameterize, AutoParameterizeMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parallel_literal (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO parallel_literal VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    let db = engine.db_for_tests();
    let sql = "SELECT /*+ PARALLEL(4) */ SUM(v) FROM test.parallel_literal WHERE id>=1 AND id<4";
    let mut text = parse_statement(sql).unwrap();
    let auto = auto_parameterize(AutoParameterizeMode::Select, &mut text);
    assert!(
        auto.fingerprint.is_none(),
        "range plans must remain literal-keyed"
    );
    let Statement::Select(mut text) = text else {
        unreachable!()
    };
    let Statement::Select(mut prepared) =
        parse_statement(&sql.replace("id>=1 AND id<4", "id>=? AND id<?")).unwrap()
    else {
        unreachable!()
    };
    let has_parallel_scan = |plan: &crate::planner::PhysicalPlan| {
        plan.any(|node| {
            matches!(
                node,
                crate::planner::PhysicalPlan::ParallelIndexRangeScan { .. }
                    | crate::planner::PhysicalPlan::ParallelIndexFastScan { .. }
            )
        })
    };
    crate::engine::params::with_params(vec![Value::Int(1), Value::Int(4)], || {
        crate::engine::executor::select::prepare_select_for_planning(db, &mut prepared).unwrap();
        let plan = crate::planner::plan_prepared_physical_select(db, &prepared).unwrap();
        assert!(
            !has_parallel_scan(&plan),
            "generic prepared range lost its small-range guard"
        );
    });
    crate::engine::params::with_auto_params(auto.params, || {
        crate::engine::executor::select::prepare_select_for_planning(db, &mut text).unwrap();
        let plan = crate::planner::plan_prepared_physical_select(db, &text).unwrap();
        assert!(
            has_parallel_scan(&plan),
            "literal-keyed text range was mistaken for a generic prepared range: {:?}",
            crate::planner::explain_rows(&plan)
        );
    });
}

#[test]
fn auto_parameterization_preserves_constant_predicate_estimates() {
    use crate::engine::auto_param::{auto_parameterize, AutoParameterizeMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE constant_estimate (id VARCHAR(64) PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO constant_estimate VALUES ('a', 1), ('b', 2), ('c', 3)")
        .unwrap();
    engine.execute("ANALYZE TABLE constant_estimate").unwrap();
    let db = engine.db_for_tests();
    let table = db.snapshot_table_schema("test.constant_estimate").unwrap();
    for predicate in [
        "1=1",
        "1=0",
        "2+2=4",
        "NULL IS NULL",
        "1",
        "0",
        "1 BETWEEN 0 AND 2",
        "1 IN (1,2)",
        "NOT (1=0)",
    ] {
        let mut statement = parse_statement(&format!("SELECT SUM(v) AS s FROM test.constant_estimate WHERE ({predicate}) AND id>='a' AND id<'z' AND v=3")).unwrap();
        let Statement::Select(mut literal) = statement.clone() else {
            unreachable!()
        };
        crate::engine::executor::select::prepare_select_for_planning(db, &mut literal).unwrap();
        let expected = crate::planner::estimate_filter_rows(
            &table,
            literal.selection.as_ref().unwrap(),
            300_000.0,
        );
        let params = auto_parameterize(AutoParameterizeMode::Select, &mut statement).params;
        assert_eq!(
            params,
            vec![Value::Int(3)],
            "constant predicate erased: {predicate}"
        );
        let Statement::Select(mut parameterized) = statement else {
            unreachable!()
        };
        crate::engine::params::with_auto_params(params, || {
            crate::engine::executor::select::prepare_select_for_planning(db, &mut parameterized)
                .unwrap();
            let actual = crate::planner::estimate_filter_rows(
                &table,
                parameterized.selection.as_ref().unwrap(),
                300_000.0,
            );
            assert_eq!(actual, expected, "cardinality changed: {predicate}");
        });
    }
}

#[test]
fn unsupported_fused_aggregates_reject_before_reading_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE preflight_agg (id INT PRIMARY KEY, payload LONGTEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO preflight_agg VALUES (1, REPEAT('wide', 16384))")
        .unwrap();
    let db = engine.db_for_tests();
    let schema = db.snapshot_table_schema("test.preflight_agg").unwrap();
    let plan = crate::planner::PhysicalPlan::SeqScan {
        table: crate::planner::PlanTable::new(schema.id, schema, None),
        filter: None,
        estimated_rows: 1.0,
        access_rows: 1.0,
        cost: crate::engine::cbo::Cost::ZERO,
        possible_keys: Vec::new(),
        runtime_filters: Vec::new(),
        hint_note: None,
        projected_columns: None,
    };
    let txn = crate::storage::TxnView::read_only(
        db.visible_scn(),
        crate::storage::IsolationLevel::RepeatableRead,
    );
    for projection in [
        "COUNT(*), SUM(LENGTH(payload))",
        "COUNT(*) + SUM(LENGTH(payload))",
        "VAR_POP(id)",
        "GROUP_CONCAT(id ORDER BY id DESC)",
        "SUM(id) + id",
    ] {
        let Statement::Select(stmt) =
            parse_statement(&format!("SELECT {projection} FROM preflight_agg")).unwrap()
        else {
            unreachable!()
        };
        let before = db.buffer_stats_counts();
        let result = crate::engine::executor::select::simple::execute_fused_single_table_operator(
            db, txn, &stmt, &plan,
        )
        .unwrap();
        let after = db.buffer_stats_counts();
        assert!(result.is_none(), "unsupported projection: {projection}");
        assert_eq!(
            after, before,
            "rejected projection read heap/LOB pages: {projection}"
        );
    }
}

#[test]
fn crc32_wide_mixed_aggregate_matches_legacy_in_all_execution_modes() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE crc_mixed (id INT PRIMARY KEY, payload LONGTEXT, raw LONGBLOB, amount DECIMAL(18,4), doc JSON, dt DATETIME(3), tm TIME(2))").unwrap();
    engine.execute("INSERT INTO crc_mixed VALUES
        (1, REPEAT('中🙂éabc',8192), X'FF00C380', 123.4500, JSON_OBJECT('x',1), '2026-09-11 01:02:03.456', '-12:34:56.78'),
        (2, '', X'', -0.1200, JSON_ARRAY(1,2), '2000-01-01 00:00:00.001', '00:00:00.01'),
        (3, NULL, NULL, NULL, NULL, NULL, NULL)").unwrap();
    let expression = "CRC32(CONCAT_WS(',',id,LEFT(payload,3),RIGHT(raw,3),amount,doc,dt,tm))";
    let Statement::Select(select) =
        parse_statement(&format!("SELECT {expression} FROM crc_mixed")).unwrap()
    else {
        unreachable!()
    };
    let SelectItem::Expr { expr, .. } = &select.projection[0] else {
        unreachable!()
    };
    let schema = engine
        .database()
        .snapshot_table_schema("test.crc_mixed")
        .unwrap();
    let QueryResponse::ResultSet(source) = engine
        .execute("SELECT * FROM crc_mixed ORDER BY id")
        .unwrap()
    else {
        panic!("source rows")
    };
    let checksums: Vec<u64> = source
        .rows
        .iter()
        .map(|row| {
            let Value::UInt(value) =
                crate::engine::expr::eval_expr_legacy(expr, &schema, row).unwrap()
            else {
                panic!("CRC32 result")
            };
            value
        })
        .collect();
    let expected = checksums.iter().sum::<u64>().to_string();
    for mode in [VecMode::Off, VecMode::Auto, VecMode::On] {
        with_vec_mode(mode, || {
            let row = one_row(
                &engine,
                &format!("SELECT COUNT(*), SUM({expression}) FROM crc_mixed WHERE id>=1 AND id<4"),
            );
            assert_eq!(row[0], Value::Int(3));
            assert_eq!(row[1].to_display_string(), expected);
            let empty = one_row(
                &engine,
                &format!("SELECT COUNT(*), SUM({expression}) FROM crc_mixed WHERE id>9"),
            );
            assert_eq!(empty, vec![Value::Int(0), Value::Null]);
            let QueryResponse::ResultSet(grouped) = engine
                .execute(&format!(
                    "SELECT id, SUM({expression}) FROM crc_mixed GROUP BY id ORDER BY id"
                ))
                .unwrap()
            else {
                panic!("grouped result")
            };
            assert_eq!(grouped.rows.len(), checksums.len());
            for (row, checksum) in grouped.rows.iter().zip(&checksums) {
                assert_eq!(row[1].to_display_string(), checksum.to_string());
            }
        });
    }
}

#[test]
fn aggregate_lob_prefix_pushdown_preserves_full_value_consumers_and_parameters() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE prefix_lob (id INT PRIMARY KEY, payload LONGTEXT, raw LONGBLOB)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO prefix_lob SELECT n,
        CASE WHEN MOD(n,4)=0 THEN NULL WHEN MOD(n,4)=1 THEN '' ELSE REPEAT('a中🙂b',4096) END,
        CASE WHEN MOD(n,4)=0 THEN NULL ELSE REPEAT(UNHEX('FF00'),4096) END
        FROM generate_series(1,32) gs(n)",
        )
        .unwrap();
    let expression = "CRC32(CONCAT_WS(',',LEFT(payload,5),LEFT(raw,3)))";
    let query = format!("SELECT COUNT(*), SUM({expression}) FROM prefix_lob FORCE INDEX(PRIMARY) WHERE id>=1 AND id<=32");
    let expected = with_vec_mode(VecMode::Off, || one_row(&engine, &query));
    let before = crate::storage::lob_full_materializations_for_test();
    let actual = with_vec_mode(VecMode::On, || one_row(&engine, &query));
    assert_eq!(actual, expected);
    assert_eq!(
        crate::storage::lob_full_materializations_for_test(),
        before,
        "prefix-only aggregate must not materialize complete LOB values"
    );

    // A full-value use in another aggregate or in the residual predicate
    // must dominate a prefix request for the same column.
    for sql in [
        "SELECT SUM(LENGTH(payload)), SUM(CRC32(LEFT(payload,5))) FROM prefix_lob FORCE INDEX(PRIMARY) WHERE id>=1 AND id<=32",
        "SELECT COUNT(*), SUM(CRC32(LEFT(payload,5))) FROM prefix_lob FORCE INDEX(PRIMARY) WHERE id>=1 AND id<=32 AND LENGTH(payload)>1000",
        "SELECT SUM(CRC32(CONCAT(LEFT(payload,5),payload))) FROM prefix_lob FORCE INDEX(PRIMARY) WHERE id>=1 AND id<=32",
        "SELECT SUM(CRC32(LEFT(payload,2))), SUM(CRC32(SUBSTRING(payload,2,9))) FROM prefix_lob FORCE INDEX(PRIMARY) WHERE id>=1 AND id<=32",
    ] {
        let expected = with_vec_mode(VecMode::Off, || one_row(&engine, sql));
        let actual = with_vec_mode(VecMode::On, || one_row(&engine, sql));
        assert_eq!(actual, expected, "{sql}");
    }

    let sql = "SELECT COUNT(*), SUM(CRC32(LEFT(payload,?))) FROM prefix_lob FORCE INDEX(PRIMARY) WHERE id>=1 AND id<=32";
    let mut session = SessionContext::default();
    let prepared = engine.prepare_cached(&mut session, sql, 1).unwrap();
    for n in [2, 9, 0, -1, 20_000, 5] {
        let expected = with_vec_mode(VecMode::Off, || {
            one_row(&engine, &sql.replace('?', &n.to_string()))
        });
        let response = with_vec_mode(VecMode::On, || {
            crate::engine::params::with_params(vec![Value::Int(n)], || {
                engine
                    .execute_prepared_statement(&mut session, sql, (*prepared).clone())
                    .unwrap()
                    .unwrap()
                    .materialize()
                    .unwrap()
            })
        });
        let QueryResponse::ResultSet(result) = response else {
            panic!("prepared result")
        };
        assert_eq!(result.rows, vec![expected], "prepared extent {n}");
    }

    // An invisible current row makes the direct sink decline. Its partially
    // decoded prefixes must be discarded, and undo must supply full values
    // from the reader's snapshot before the original expressions run.
    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut reader,
            "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
        )
        .unwrap();
    engine
        .execute("UPDATE prefix_lob SET payload=REPEAT('changed',4096) WHERE id=2")
        .unwrap();
    let response = with_vec_mode(VecMode::On, || {
        engine
            .execute_with_session(&mut reader, &query)
            .unwrap()
            .materialize()
            .unwrap()
    });
    let QueryResponse::ResultSet(result) = response else {
        panic!("snapshot result")
    };
    assert_eq!(result.rows, vec![expected]);
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
}

fn setup(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE w (w_id INT PRIMARY KEY, w_ytd DECIMAL(12,2), tag VARCHAR(8))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO w VALUES (1, 10.50, 'a'), (2, 20.25, 'a'), (3, NULL, 'b'), (4, 5.25, 'b')",
        )
        .unwrap();
}

fn dec(mag: i64, scale: u8) -> Value {
    Value::DecimalCompact(mag, scale)
}

#[test]
fn coalesce_wrapped_sum_with_count() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    let row = one_row(&engine, "SELECT COUNT(*), COALESCE(SUM(w_ytd), 0) FROM w");
    assert_eq!(row, vec![Value::Int(4), dec(3600, 2)]);
}

#[test]
fn coalesce_wrapped_sum_empty_table_returns_zero() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    // One implicit group with empty input: SUM is NULL and COALESCE returns 0, matching MySQL.
    let row = one_row(
        &engine,
        "SELECT COUNT(*), COALESCE(SUM(w_ytd), 0) FROM w WHERE w_id > 100",
    );
    assert_eq!(row, vec![Value::Int(0), Value::Int(0)]);
}

#[test]
fn ifnull_wrapped_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    let row = one_row(
        &engine,
        "SELECT IFNULL(MAX(w_ytd), -1), IFNULL(MIN(w_ytd), -1) FROM w WHERE tag = 'zzz'",
    );
    assert_eq!(row, vec![Value::Int(-1), Value::Int(-1)]);
}

#[test]
fn scalar_fn_mixing_aggregate_and_constants() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    // Mix ROUND(AVG(...)), COALESCE(SUM(...)), and COUNT(*) expressions.
    let row = one_row(
        &engine,
        "SELECT ROUND(AVG(w_ytd), 1), COALESCE(SUM(w_ytd), 0) + COUNT(*) FROM w",
    );
    assert_eq!(row, vec![dec(120, 1), dec(4000, 2)]);
}

#[test]
fn coalesce_wrapped_sum_with_group_by_and_having() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT tag, COALESCE(SUM(w_ytd), 0) FROM w \
             GROUP BY tag HAVING COALESCE(SUM(w_ytd), 0) > 10 ORDER BY tag",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::String("a".into()), dec(3075, 2)]]
    );
}

#[test]
fn nested_scalar_fns_around_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    // Multiple scalar wrappers: ABS(COALESCE(SUM(-w_ytd), 0)).
    let row = one_row(&engine, "SELECT ABS(COALESCE(SUM(-w_ytd), 0)) FROM w");
    assert_eq!(row, vec![dec(3600, 2)]);
}

#[test]
fn coalesce_aggregate_in_scalar_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    let row = one_row(
        &engine,
        "SELECT (SELECT COALESCE(SUM(w_ytd), 0) FROM w WHERE tag = 'b')",
    );
    assert_eq!(row, vec![dec(525, 2)]);
}

#[test]
fn source_free_wrapper_keeps_nested_table_scope_and_grandparent_correlation() {
    with_test_env(&[("MYDB_DECORRELATE", Some("off"))], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute("CREATE TABLE outer_rows (id INT PRIMARY KEY, status VARCHAR(32))")
            .unwrap();
        engine
            .execute("CREATE TABLE inner_rows (id INT PRIMARY KEY, status VARCHAR(32))")
            .unwrap();
        engine
            .execute("INSERT INTO outer_rows VALUES (3,'under_review'),(8,'under_review')")
            .unwrap();
        engine
            .execute("INSERT INTO inner_rows VALUES (2,'under_review'),(10,'under_review')")
            .unwrap();

        // The source-free wrapper has no local columns. Its nested SELECT does:
        // `id`/`status` belong to inner_rows and must not be captured from `o`.
        // MAX(id)=10, ROUND(0.5*10)=5, therefore id=8 is the first match. The
        // broken flat-scope binder substituted o.id/o.status and returned 3.
        let row = one_row(
            &engine,
            "SELECT o.id FROM outer_rows o
             WHERE o.id >= (
                 SELECT ROUND(0.5 * (
                     SELECT MAX(id) FROM inner_rows WHERE status='under_review'
                 ))
             )
             AND o.status='under_review'
             ORDER BY o.id LIMIT 1",
        );
        assert_eq!(row, vec![Value::Int(8)]);

        // Do not fix the local-scope bug by simply stopping at nested query
        // blocks: a genuine reference may cross multiple source-free levels.
        let QueryResponse::ResultSet(result) = engine
            .execute(
                "SELECT o.id FROM outer_rows o
                 WHERE (SELECT (SELECT o.id)) = o.id ORDER BY o.id",
            )
            .unwrap()
        else {
            panic!("expected result set");
        };
        assert_eq!(result.rows, vec![vec![Value::Int(3)], vec![Value::Int(8)]]);

        engine
            .execute("CREATE TABLE parent_rows (id INT PRIMARY KEY, k INT)")
            .unwrap();
        engine
            .execute("CREATE TABLE child_rows (id INT PRIMARY KEY, k INT, v INT)")
            .unwrap();
        engine
            .execute("INSERT INTO parent_rows VALUES (1,10),(2,20)")
            .unwrap();
        engine
            .execute("INSERT INTO child_rows VALUES (1,10,5),(2,10,7),(3,20,9)")
            .unwrap();
        // The outer wrapper is independent, but its nested MAX is correlated
        // to the wrapper's own row. Constant folding must leave this child in
        // place for per-parent-row binding instead of probing it as a global
        // statement constant.
        assert_eq!(
            one_row(
                &engine,
                "SELECT (SELECT p.id + (
                     SELECT MAX(c.v) FROM child_rows c WHERE c.k=p.k
                 ) FROM parent_rows p ORDER BY p.id LIMIT 1)",
            ),
            vec![Value::Int(8)]
        );
    });
}

#[test]
fn aggregate_in_where_reports_invalid_group_use() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(a INTEGER)").unwrap();
    engine.execute("INSERT INTO t VALUES (1),(2)").unwrap();

    // With a source-free outer query, MySQL owns a constant aggregate in the
    // outer block. Its single implicit row makes GROUP_CONCAT(1) equal "1",
    // so the inner predicate rejects both rows and the scalar subquery is NULL.
    assert_eq!(
        one_row(
            &engine,
            "SELECT (SELECT 1
                     FROM t
                     WHERE CONVERT(1 USING utf32) <> GROUP_CONCAT(1))"
        ),
        vec![Value::Null]
    );

    // A real local aggregate in WHERE remains illegal, and the validation
    // must recurse through scalar function arguments.
    let sql = "SELECT a FROM t WHERE COALESCE(SUM(a), 0) > 0";
    let err = engine.execute(sql).unwrap_err();
    assert_eq!(err.code(), 1111, "{sql}: unexpected error: {err:?}");
    assert_eq!(err.sql_state(), "HY000", "{sql}: {err:?}");
    assert_eq!(err.message(), "Invalid use of group function", "{sql}");

    // An aggregate owned by a nested query block remains valid when the
    // scalar subquery as a whole appears in the outer WHERE.
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT a FROM t WHERE (SELECT COUNT(*) FROM t) > 0 ORDER BY a")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn having_in_and_between_recursively_evaluate_aggregates() {
    assert_eq!(
        crate::engine::expr::null_cmp_binary(
            &Value::DecimalCompact(10_000, 4),
            crate::parser::BinaryOp::Eq,
            &Value::Int(1),
        )
        .unwrap(),
        Value::Int(1)
    );
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT tag, SUM(w_ytd) FROM w
             GROUP BY tag
             HAVING AVG(w_ytd) IN (5.25, 15.375)
                AND COUNT(*) BETWEEN 2 AND 3
             ORDER BY tag",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::String("a".into()), dec(3075, 2)],
            vec![Value::String("b".into()), dec(525, 2)],
        ]
    );

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT tag FROM w GROUP BY tag
             HAVING (AVG(1), COUNT(*)) IN ((1, 2), (2, 3))
             ORDER BY tag",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(diagnostics) = engine
        .execute(
            "SELECT tag, AVG(1), COUNT(*),
                    AVG(1) = 1,
                    (AVG(1), COUNT(*)) = (1, 2)
             FROM w GROUP BY tag ORDER BY tag",
        )
        .unwrap()
    else {
        panic!("expected diagnostic result set");
    };
    assert!(diagnostics
        .rows
        .iter()
        .all(|row| row[3] == Value::Int(1) && row[4] == Value::Int(1)));
    assert_eq!(
        result.rows,
        vec![
            vec![Value::String("a".into())],
            vec![Value::String("b".into())],
        ]
    );
}
