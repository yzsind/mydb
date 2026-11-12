use super::*;
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

fn fixture() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE items(k INT, label VARCHAR(20) COLLATE utf8mb4_bin, \
         amount DECIMAL(8,2), stamp DATETIME, payload VARBINARY(8))",
        "INSERT INTO items VALUES \
         (1,'z',9.50,'2026-01-03 00:00:00',X'7A'),\
         (2,'A',-1.25,'2026-01-01 00:00:00',X'41'),\
         (3,NULL,NULL,NULL,NULL),\
         (1,'z',9.50,'2026-01-03 00:00:00',X'7A')",
    ] {
        run(&engine, &mut session, sql).unwrap();
    }
    (dir, engine, session)
}

fn run(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
) -> Result<QueryResponse, QueryError> {
    engine
        .execute_with_session(session, sql)
        .and_then(QueryResponse::materialize)
}

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<String>> {
    let QueryResponse::ResultSet(result) =
        run(engine, session, sql).unwrap_or_else(|err| panic!("{sql}: {err:?}"))
    else {
        panic!("expected result set: {sql}");
    };
    result
        .rows
        .iter()
        .map(|row| row.iter().map(Value::to_display_string).collect())
        .collect()
}

#[test]
fn distinct_order_by_permissive_hidden_keys_types_and_limits() {
    let (_dir, engine, mut session) = fixture();
    for sql_mode in ["", "ERROR_FOR_DIVISION_BY_ZERO", "STRICT_TRANS_TABLES"] {
        run(&engine, &mut session, &format!("SET sql_mode='{sql_mode}'")).unwrap();
        for mode in [VecMode::Off, VecMode::On] {
            with_vec_mode(mode, || {
                for key in [
                    "label",
                    "amount",
                    "stamp",
                    "payload",
                    "LOWER(label)",
                    "COALESCE(amount, -100)",
                    "CASE WHEN label IS NULL THEN '' ELSE label END",
                ] {
                    for (suffix, expected) in [
                        ("", vec![vec!["3"], vec!["2"], vec!["1"]]),
                        (" LIMIT 2", vec![vec!["3"], vec!["2"]]),
                        (" LIMIT 1 OFFSET 1", vec![vec!["2"]]),
                    ] {
                        let sql = format!("SELECT DISTINCT k FROM items ORDER BY {key}{suffix}");
                        assert_eq!(
                            rows(&engine, &mut session, &sql),
                            expected,
                            "{sql_mode}: {sql}"
                        );
                    }
                }
                assert_eq!(
                    rows(
                        &engine,
                        &mut session,
                        "SELECT DISTINCT k FROM items ORDER BY label DESC"
                    ),
                    vec![vec!["1"], vec!["2"], vec!["3"]]
                );
                // DISTINCT equality still uses only the projection, with its own types.
                for (projection, expected) in [
                    ("amount", vec![vec!["9.50"], vec!["-1.25"], vec!["NULL"]]),
                    ("HEX(payload)", vec![vec!["7A"], vec!["41"], vec!["NULL"]]),
                    (
                        "CONCAT(k, ':', COALESCE(label, 'nil'))",
                        vec![vec!["1:z"], vec!["2:A"], vec!["3:nil"]],
                    ),
                ] {
                    let sql = format!("SELECT DISTINCT {projection} FROM items ORDER BY k");
                    assert_eq!(rows(&engine, &mut session, &sql), expected, "{sql}");
                }
            });
        }
    }
}

#[test]
fn distinct_order_by_permissive_nested_queries_and_join_fanout() {
    let (_dir, engine, mut session) = fixture();
    run(&engine, &mut session, "SET sql_mode=''").unwrap();
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            for sql in [
                "SELECT DISTINCT i.k FROM items i JOIN items j ON i.k=j.k ORDER BY i.label",
                "SELECT DISTINCT d.k FROM (SELECT k,label FROM items) d ORDER BY d.label",
                "SELECT d.k FROM (SELECT DISTINCT k FROM items ORDER BY label LIMIT 2) d ORDER BY d.k DESC",
                "WITH d AS (SELECT DISTINCT k FROM items ORDER BY label LIMIT 2) SELECT k FROM d ORDER BY k DESC",
            ] {
                let expected = if sql.contains("LIMIT 2") {
                    vec![vec!["3"], vec!["2"]]
                } else {
                    vec![vec!["3"], vec!["2"], vec!["1"]]
                };
                assert_eq!(rows(&engine, &mut session, sql), expected, "{sql}");
            }
        });
    }
}

#[test]
fn distinct_order_by_strict_mode_retains_validation_in_nested_scopes() {
    let (_dir, engine, mut session) = fixture();
    run(&engine, &mut session, "SET sql_mode='ONLY_FULL_GROUP_BY'").unwrap();
    for sql in [
        "SELECT DISTINCT k FROM items ORDER BY label",
        "SELECT DISTINCT k FROM items ORDER BY LOWER(label)",
        "SELECT DISTINCT i.k FROM items i JOIN items j ON i.k=j.k ORDER BY j.label",
        "SELECT DISTINCT d.k FROM (SELECT k,label FROM items) d ORDER BY d.label",
        "SELECT d.k FROM (SELECT DISTINCT k FROM items ORDER BY label LIMIT 2) d",
        "WITH d AS (SELECT DISTINCT k FROM items ORDER BY label LIMIT 2) SELECT k FROM d",
    ] {
        assert_eq!(
            run(&engine, &mut session, sql).unwrap_err().code(),
            3065,
            "{sql}"
        );
    }
    for sql in [
        "SELECT DISTINCT k FROM items ORDER BY k+1",
        "SELECT DISTINCT k AS n FROM items ORDER BY n",
        "SELECT DISTINCT LOWER(label) AS n FROM items ORDER BY LOWER(label)",
        "SELECT DISTINCT items.* FROM items ORDER BY label",
    ] {
        run(&engine, &mut session, sql).unwrap_or_else(|err| panic!("{sql}: {err:?}"));
    }
}

#[test]
fn distinct_order_by_mode_switches_do_not_reuse_permissive_validation() {
    let (_dir, engine, mut session) = fixture();
    let sql = "SELECT DISTINCT k FROM items ORDER BY label LIMIT 2";
    for _ in 0..2 {
        run(&engine, &mut session, "SET sql_mode=''").unwrap();
        assert_eq!(rows(&engine, &mut session, sql), vec![vec!["3"], vec!["2"]]);
        run(
            &engine,
            &mut session,
            "SET sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES'",
        )
        .unwrap();
        assert_eq!(run(&engine, &mut session, sql).unwrap_err().code(), 3065);
    }
}

#[test]
fn distinct_order_by_permissive_mode_still_rejects_invalid_columns() {
    let (_dir, engine, mut session) = fixture();
    run(&engine, &mut session, "SET sql_mode=''").unwrap();
    for (sql, code) in [
        ("SELECT DISTINCT k FROM items ORDER BY missing", 1054),
        (
            "SELECT DISTINCT i.k FROM items i JOIN items j ON i.k=j.k ORDER BY label",
            1052,
        ),
    ] {
        assert_eq!(
            run(&engine, &mut session, sql).unwrap_err().code(),
            code,
            "{sql}"
        );
    }
}

#[test]
fn distinct_order_by_prepared_binding_observes_session_mode() {
    let (_dir, engine, mut session) = fixture();
    let sql = "SELECT DISTINCT k FROM items WHERE k>=? ORDER BY label LIMIT ?";
    run(&engine, &mut session, "SET sql_mode=''").unwrap();
    let columns = <StorageQueryEngine as QueryEngine>::describe_prepared_statement(
        &engine,
        &mut session,
        sql,
    )
    .unwrap();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].name, "k");
    run(
        &engine,
        &mut session,
        &format!("PREPARE hidden_key FROM '{sql}'"),
    )
    .unwrap();
    run(&engine, &mut session, "SET @minimum=1, @page_size=2").unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "EXECUTE hidden_key USING @minimum,@page_size"
        ),
        vec![vec!["3"], vec!["2"]]
    );
    run(&engine, &mut session, "DEALLOCATE PREPARE hidden_key").unwrap();
    run(&engine, &mut session, "SET sql_mode='ONLY_FULL_GROUP_BY'").unwrap();
    for query in [
        sql,
        "SELECT DISTINCT i.k FROM items i JOIN items j ON i.k=j.k ORDER BY i.label",
        "SELECT DISTINCT d.k FROM (SELECT k,label FROM items) d ORDER BY d.label",
        "SELECT d.k FROM (SELECT DISTINCT k FROM items ORDER BY label LIMIT 2) d",
        "WITH d AS (SELECT DISTINCT k FROM items ORDER BY label LIMIT 2) SELECT k FROM d",
    ] {
        let error = <StorageQueryEngine as QueryEngine>::describe_prepared_statement(
            &engine,
            &mut session,
            query,
        )
        .unwrap_err();
        assert_eq!(error.code(), 3065, "{query}");
    }
    assert_eq!(
        run(
            &engine,
            &mut session,
            &format!("PREPARE hidden_key FROM '{sql}'")
        )
        .unwrap_err()
        .code(),
        3065
    );
}
