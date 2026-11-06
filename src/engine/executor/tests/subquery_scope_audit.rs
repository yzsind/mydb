use super::support::execute_materialized as exec;
use super::*;

fn audit(queries: &[(&str, &[&[&str]])]) {
    for cache in [None, Some("off")] {
        for decorrelate in [None, Some("off")] {
            with_test_env(
                &[
                    ("MYDB_SUBQUERY_PLAN_CACHE", cache),
                    ("MYDB_DECORRELATE", decorrelate),
                ],
                || {
                    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                    let engine = open_test_engine(dir.path());
                    let mut session = SessionContext::default();
                    for sql in [
                        "CREATE TABLE t (id INT, v INT)",
                        "INSERT INTO t VALUES (1,10),(2,20)",
                        "CREATE TABLE outer_t (id INT, x INT, k INT)",
                        "INSERT INTO outer_t VALUES (1,100,50),(2,200,60)",
                    ] {
                        exec(&engine, &mut session, sql).expect(sql);
                    }
                    for &(sql, expected) in queries {
                        for prepared in [false, true] {
                            let statement = if prepared {
                                exec(
                                    &engine,
                                    &mut session,
                                    &format!(
                                        "PREPARE audit_stmt FROM '{}'",
                                        sql.replace('\'', "''")
                                    ),
                                )
                                .expect(sql);
                                "EXECUTE audit_stmt"
                            } else {
                                sql
                            };
                            let QueryResponse::ResultSet(result) =
                                exec(&engine, &mut session, statement).expect(sql)
                            else {
                                panic!("expected rows");
                            };
                            let rows: Vec<Vec<String>> = result
                                .rows
                                .iter()
                                .map(|row| row.iter().map(Value::to_display_string).collect())
                                .collect();
                            assert_eq!(
                                rows, expected,
                                "{sql}: cache={cache:?}, decorrelate={decorrelate:?}"
                            );
                        }
                    }
                },
            );
        }
    }
}

#[test]
fn renamed_derived_columns_stay_local_to_scalar_subqueries() {
    audit(&[
        ("SELECT o.id, (SELECT MAX(x) FROM (SELECT v FROM t UNION ALL SELECT v FROM t) d(x)) AS q FROM outer_t o ORDER BY o.id", &[&["1", "20"], &["2", "20"]]),
        ("SELECT o.id, (SELECT MAX(x) FROM (WITH c AS (SELECT v FROM t) SELECT * FROM c) d(x)) AS q FROM outer_t o ORDER BY o.id", &[&["1", "20"], &["2", "20"]]),
        ("SELECT o.id, (SELECT MAX(d.x) FROM (SELECT v FROM t UNION ALL SELECT v FROM t) d(x)) AS q FROM outer_t o ORDER BY o.id", &[&["1", "20"], &["2", "20"]]),
        ("SELECT o.id, (SELECT MAX(x) FROM t JOIN (SELECT v FROM t) d(x) ON t.id=1) AS q FROM outer_t o ORDER BY o.id", &[&["1", "20"], &["2", "20"]]),
        ("SELECT o.id, (SELECT COUNT(*) FROM (SELECT v FROM t UNION ALL SELECT v FROM t) d(x) WHERE v=o.x) AS q FROM outer_t o JOIN t ON t.id=o.id ORDER BY o.id", &[&["1", "0"], &["2", "0"]]),
    ]);
}

#[test]
fn qualified_correlations_resolve_columns_through_reused_table_aliases() {
    audit(&[
        ("SELECT o.id, (SELECT a.x FROM t a WHERE a.id=o.id) AS q FROM outer_t a JOIN outer_t o ON a.id=o.id ORDER BY o.id", &[&["1", "100"], &["2", "200"]]),
        ("SELECT o.id, (SELECT (SELECT a.x) FROM t a WHERE a.id=o.id) AS q FROM outer_t a JOIN outer_t o ON a.id=o.id ORDER BY o.id", &[&["1", "100"], &["2", "200"]]),
        ("SELECT o.id, (SELECT (SELECT a.id) FROM t a WHERE a.id=o.id) AS q FROM outer_t a JOIN outer_t o ON a.id=o.id ORDER BY o.id", &[&["1", "1"], &["2", "2"]]),
        ("SELECT o.id, (SELECT a.x FROM t a JOIN outer_t b ON b.id=a.id WHERE a.id=o.id) AS q FROM outer_t a JOIN outer_t o ON a.id=o.id ORDER BY o.id", &[&["1", "100"], &["2", "200"]]),
    ]);
}

#[test]
fn correlated_window_operands_preserve_source_scope() {
    audit(&[
        ("SELECT o.id, (SELECT MAX(v) FROM (SELECT v+o.x AS v FROM t) d) AS q FROM outer_t o ORDER BY o.id", &[&["1", "120"], &["2", "220"]]),
        ("SELECT o.id, (SELECT MAX(v) FROM (WITH c AS (SELECT v+o.x AS v FROM t) SELECT v FROM c) d) AS q FROM outer_t o ORDER BY o.id", &[&["1", "120"], &["2", "220"]]),
        ("SELECT o.id, (SELECT MAX(r) FROM (SELECT ROW_NUMBER() OVER (ORDER BY o.x) AS r FROM t) d) AS q FROM outer_t o ORDER BY o.id", &[&["1", "2"], &["2", "2"]]),
        ("SELECT o.id, (SELECT SUM(v) OVER w FROM t WHERE id=o.id WINDOW w AS (ORDER BY o.x)) AS q FROM outer_t o ORDER BY o.id", &[&["1", "10"], &["2", "20"]]),
        ("SELECT o.id, (SELECT SUM(o.x) OVER (PARTITION BY o.k ORDER BY id) FROM t WHERE id=o.id) AS q FROM outer_t o ORDER BY o.id", &[&["1", "100"], &["2", "200"]]),
        ("SELECT o.id, (SELECT MAX(r) FROM (SELECT ROW_NUMBER() OVER w AS r FROM t WINDOW w AS (PARTITION BY o.k ORDER BY id)) d) AS q FROM outer_t o ORDER BY o.id", &[&["1", "2"], &["2", "2"]]),
    ]);
}

#[test]
fn correlated_tuple_operands_are_bound_in_both_execution_paths() {
    audit(&[
        ("SELECT o.id, (SELECT COUNT(*) FROM t WHERE (id,v)=(o.id,o.id*10)) AS q FROM outer_t o ORDER BY o.id", &[&["1", "1"], &["2", "1"]]),
        ("SELECT o.id, (SELECT COUNT(*) FROM t WHERE (id,v) IN ((o.id,o.id*10))) AS q FROM outer_t o ORDER BY o.id", &[&["1", "1"], &["2", "1"]]),
    ]);
}

#[test]
fn table_function_outputs_are_local_to_correlated_subqueries() {
    audit(&[
        ("SELECT o.id, (SELECT MAX(x) FROM generate_series(1,2) AS g(x)) AS q FROM outer_t o ORDER BY o.id", &[&["1", "2"], &["2", "2"]]),
        ("SELECT o.id, (SELECT MAX(g.x) FROM generate_series(1,2) AS g(x)) AS q FROM outer_t o ORDER BY o.id", &[&["1", "2"], &["2", "2"]]),
        ("SELECT o.id, (SELECT MAX(x) FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS (x INT PATH '$')) j) AS q FROM outer_t o ORDER BY o.id", &[&["1", "2"], &["2", "2"]]),
    ]);
}

#[test]
fn window_projection_aliases_do_not_capture_outer_columns() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t (id INT, v INT)",
        "INSERT INTO t VALUES (1,10),(2,20)",
        "CREATE TABLE outer_t (id INT, x INT)",
        "INSERT INTO outer_t VALUES (1,100),(2,200)",
    ] {
        exec(&engine, &mut session, sql).expect(sql);
    }
    for sql in [
        "SELECT o.id,(SELECT ROW_NUMBER() OVER (ORDER BY x) AS x FROM t WHERE id=o.id) FROM outer_t o",
        "SELECT o.id,(SELECT ROW_NUMBER() OVER w AS x FROM t WHERE id=o.id WINDOW w AS (ORDER BY x)) FROM outer_t o",
        "SELECT o.id,(SELECT ROW_NUMBER() OVER (PARTITION BY x ORDER BY id) AS x FROM t WHERE id=o.id) FROM outer_t o",
        "SELECT o.id,(SELECT ROW_NUMBER() OVER w AS x FROM t WHERE id=o.id WINDOW w AS (PARTITION BY x ORDER BY id)) FROM outer_t o",
    ] {
        assert_eq!(exec(&engine, &mut session, sql).expect_err(sql).code(), 1054, "{sql}");
    }
    audit(&[
        ("SELECT o.id,(SELECT ROW_NUMBER() OVER (ORDER BY x) AS r FROM t WHERE id=o.id) AS q FROM outer_t o ORDER BY o.id", &[&["1", "1"], &["2", "1"]]),
        ("SELECT o.id,(SELECT ROW_NUMBER() OVER (ORDER BY o.x) AS x FROM t WHERE id=o.id) AS q FROM outer_t o ORDER BY o.id", &[&["1", "1"], &["2", "1"]]),
        ("SELECT o.id,(SELECT ROW_NUMBER() OVER (ORDER BY v) AS v FROM t WHERE id=o.id) AS q FROM outer_t o ORDER BY o.id", &[&["1", "1"], &["2", "1"]]),
    ]);
}

#[test]
fn explicit_view_column_names_preserve_internal_alias_bindings() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t (id INT, v INT)",
        "INSERT INTO t VALUES (1,10),(2,20)",
        "CREATE VIEW renamed_v (renamed_key, renamed_total) AS SELECT MOD(id,2) AS bucket, SUM(v) AS subtotal FROM t GROUP BY bucket HAVING subtotal>=10 ORDER BY bucket DESC",
        "CREATE VIEW nested_v (a,b) AS SELECT renamed_key AS original_key, renamed_total AS original_total FROM renamed_v ORDER BY original_key DESC",
    ] { exec(&engine, &mut session, sql).expect(sql); }
    for (sql, expected) in [
        ("SELECT * FROM renamed_v", vec![vec!["1", "10"],vec!["0", "20"]]),
        ("SELECT * FROM nested_v", vec![vec!["1", "10"],vec!["0", "20"]]),
        ("SELECT renamed_key,renamed_total FROM renamed_v WHERE renamed_total=(SELECT MAX(renamed_total) FROM renamed_v)", vec![vec!["0", "20"]]),
    ] {
        let QueryResponse::ResultSet(result) = exec(&engine, &mut session, sql).expect(sql) else { panic!("expected rows"); };
        let rows: Vec<Vec<String>> = result.rows.iter().map(|row| row.iter().map(Value::to_display_string).collect()).collect();
        assert_eq!(rows, expected, "{sql}");
    }
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    let sql = "SELECT renamed_key FROM renamed_v WHERE renamed_total=(SELECT MAX(renamed_total) FROM renamed_v)";
    let QueryResponse::ResultSet(result) = exec(&engine, &mut session, sql).expect(sql) else {
        panic!("expected rows");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(0)]]);
    exec(&engine, &mut session, "ALTER VIEW renamed_v (new_key,new_total) AS SELECT MOD(id,2) AS bucket, SUM(v) AS subtotal FROM t GROUP BY bucket HAVING subtotal>10").unwrap();
    let QueryResponse::ResultSet(result) =
        exec(&engine, &mut session, "SELECT new_key FROM renamed_v").unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(0)]]);
}
