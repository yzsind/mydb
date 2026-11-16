use super::*;

/// Rewrite code tokens only: quoted values, JSON paths and comments retain
/// their exact bytes. `target` also covers functions used without parentheses.
pub(super) fn case_variant(sql: &str, target: Option<&str>, mixed: bool) -> String {
    use crate::sql_lex::{SqlLexMode, SqlTokenCursor, SqlTokenKind};
    let mut cursor = SqlTokenCursor::new(sql, SqlLexMode::default(), 80046);
    let mut out = sql.as_bytes().to_vec();
    while let Some(token) = cursor.next().unwrap() {
        if token.kind != SqlTokenKind::Word {
            continue;
        }
        let is_call = cursor
            .peek()
            .unwrap()
            .is_some_and(|next| next.kind == SqlTokenKind::Symbol(b'('));
        if is_call || target.is_some_and(|name| sql[token.span.clone()].eq_ignore_ascii_case(name))
        {
            for (i, byte) in out[token.span].iter_mut().enumerate() {
                *byte = if mixed && i % 2 == 1 {
                    byte.to_ascii_uppercase()
                } else {
                    byte.to_ascii_lowercase()
                };
            }
        }
    }
    String::from_utf8(out).unwrap()
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine
        .execute(sql)
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
    else {
        panic!("expected rows: {sql}")
    };
    result.rows
}

#[test]
fn function_case_semantic_equality_preserves_literal_payloads() {
    let parse = |sql| crate::parser::parse_expr_fragment(sql).unwrap();
    for (a, b) in [
        ("ABS(t.v)+LENGTH(t.s)", "aBs(T.V)+length(T.S)"),
        (
            "CASE WHEN ABS(v)>1 THEN UPPER(s) ELSE LOWER(s) END",
            "CASE WHEN abs(V)>1 THEN upper(S) ELSE lower(S) END",
        ),
        (
            "COALESCE(v,0) BETWEEN 1 AND 2",
            "coalesce(V,0) BETWEEN 1 AND 2",
        ),
        ("ABS(v) IN (1,2)", "abs(V) IN (1,2)"),
        ("UPPER(s) LIKE 'A%'", "upper(S) LIKE 'A%'"),
        (
            "SUM((SELECT MAX(v) FROM names))",
            "sum((SELECT max(V) FROM NAMES))",
        ),
    ] {
        assert!(
            crate::engine::expr_program::expr_semantically_eq(&parse(a), &parse(b)),
            "{a} / {b}"
        );
    }
    for (a, b) in [
        ("LOWER('A')", "lower('a')"),
        ("JSON_EXTRACT(j,'$.A')", "json_extract(J,'$.a')"),
        ("COUNT(v)", "count(DISTINCT v)"),
        ("ABS(v)+1", "abs(v)+2"),
        ("ABS(v) IN (1,2)", "abs(v) IN (1,3)"),
        ("UPPER(s) LIKE 'A%'", "upper(s) LIKE 'a%'"),
        ("SUM((SELECT LENGTH('A')))", "sum((SELECT length('a')))"),
    ] {
        assert!(
            !crate::engine::expr_program::expr_semantically_eq(&parse(a), &parse(b)),
            "{a} / {b}"
        );
    }
}

#[test]
fn function_case_grouping_and_ordering_bind_equivalent_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE names (v INT, s VARCHAR(8))")
        .unwrap();
    engine
        .execute("INSERT INTO names VALUES (-2,'a'),(2,'b'),(-1,'c'),(1,'d')")
        .unwrap();
    for sql_mode in ["", "ONLY_FULL_GROUP_BY"] {
        let mut session = SessionContext::default();
        engine
            .execute_with_session(&mut session, &format!("SET SESSION sql_mode='{sql_mode}'"))
            .unwrap();
        for (reference, variant) in [
            (
                "SELECT ABS(v),COUNT(*) FROM names GROUP BY ABS(v) ORDER BY ABS(v)",
                "SELECT aBs(v),couNT(*) FROM names GROUP BY ABS(v) ORDER BY abs(v)",
            ),
            (
                "SELECT ABS(v),COUNT(*) FROM names GROUP BY ABS(v) WITH ROLLUP",
                "SELECT aBs(v),couNT(*) FROM names GROUP BY ABS(v) WITH ROLLUP",
            ),
            (
                "SELECT ABS(v),GROUPING(ABS(v)),COUNT(*) FROM names GROUP BY ABS(v) WITH ROLLUP",
                "SELECT abs(v),gRoUpInG(aBs(v)),COUNT(*) FROM names GROUP BY ABS(v) WITH ROLLUP",
            ),
            (
                "SELECT DISTINCT ABS(v) FROM names ORDER BY ABS(v)",
                "SELECT DISTINCT aBs(v) FROM names ORDER BY ABS(v)",
            ),
        ] {
            let mut get = |sql: &str| {
                let QueryResponse::ResultSet(result) = engine
                    .execute_with_session(&mut session, sql)
                    .unwrap_or_else(|err| panic!("sql_mode={sql_mode} {sql}: {err}"))
                else {
                    panic!("expected rows")
                };
                result.rows
            };
            assert_eq!(
                get(reference),
                get(variant),
                "sql_mode={sql_mode}: {variant}"
            );
        }
    }
}

#[test]
fn function_case_aggregate_collection_reuses_semantic_slots() {
    let Statement::Select(select) = crate::parser::parse_statement(
        "SELECT SUM(ABS(v)),sum(abs(V)),COUNT(*),couNT(*) FROM names HAVING SuM(aBs(v))>0 ORDER BY count(*)"
    ).unwrap() else { panic!("expected select") };
    let slots = crate::planner::collect_aggregate_calls(&select);
    assert_eq!(
        slots.len(),
        2,
        "case variants must share aggregate state: {slots:?}"
    );
}

#[test]
fn function_case_cached_scalar_window_view_and_stored_calls() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE names (id INT PRIMARY KEY,v INT,s VARCHAR(8),j JSON,d DATETIME)")
        .unwrap();
    engine.execute("INSERT INTO names VALUES (1,-2,'Ab','{\"A\":1,\"a\":2}','2026-09-13 12:34:56'),(2,3,'xY','{\"A\":4,\"a\":5}','2025-01-02 01:02:03'),(3,NULL,NULL,NULL,NULL)").unwrap();
    let queries = [
        "SELECT ABS(v),ROUND(v/3,2),COALESCE(v,9),IF(v>0,1,0) FROM names ORDER BY id",
        "SELECT UPPER(s),LOWER(s),CONCAT(s,'A'),SUBSTRING(s,1,1),LENGTH(s),HEX(s) FROM names ORDER BY id",
        "SELECT JSON_EXTRACT(j,'$.A'),JSON_EXTRACT(j,'$.a'),JSON_UNQUOTE(JSON_EXTRACT(j,'$.A')) FROM names ORDER BY id",
        "SELECT YEAR(d),DATE_FORMAT(d,'%Y-%m-%d'),DAYOFWEEK(d),TIMESTAMPDIFF(DAY,d,d) FROM names ORDER BY id",
        "SELECT COUNT(*),SUM(v),AVG(v),MIN(s),MAX(s),COUNT(DISTINCT s),BIT_OR(v),VAR_POP(v) FROM names",
        "SELECT ROW_NUMBER() OVER (ORDER BY id),RANK() OVER (ORDER BY v),LAG(v) OVER (ORDER BY id),SUM(v) OVER (ORDER BY id) FROM names ORDER BY id",
    ];
    for mode in [VecMode::Off, VecMode::On, VecMode::Auto] {
        with_vec_mode(mode, || {
            for sql in queries {
                let reference = rows(&engine, sql);
                for mixed in [false, true] {
                    let changed = case_variant(sql, None, mixed);
                    assert_eq!(reference, rows(&engine, &changed), "{mode:?}: {changed}");
                }
            }
        });
    }
    engine
        .execute("CREATE VIEW case_view AS SELECT id,aBs(v) AS value FROM names")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT SUM(value) FROM case_view"),
        rows(&engine, "SELECT sUm(value) FROM case_view")
    );
    engine
        .execute("CREATE FUNCTION CaseFn(x INT) RETURNS INT DETERMINISTIC RETURN aBs(x)+1")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT CaseFn(v) FROM names ORDER BY id"),
        rows(&engine, "SELECT test.cAsEfN(v) FROM names ORDER BY id")
    );
}
