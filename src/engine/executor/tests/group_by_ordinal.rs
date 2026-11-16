//! GROUP BY ordinal regression: an integer literal references the Nth select item.
//! Previously the planner resolved ordinals only on a best-effort basis;
//! zero/out-of-range unwrap_or retained the literal and grouped the whole table by a constant
//! instead of returning 1054. Wildcard projections silently degraded similarly.
//!
//! parser::resolve_group_by_ordinals now substitutes centrally: zero/out-of-range yields
//! 1054 'group statement', wildcard plus ordinal yields 1235, and an aggregate target yields
//! 1056 (MySQL ER_WRONG_GROUP_FIELD). Grouping sets refer to group_by keys by index;
//! expression-level substitution keeps those indices valid.

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

fn setup_vt(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE vt (id INT PRIMARY KEY, d DATE, dt DATETIME(6))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO vt VALUES \
             (1, '2024-05-31', '2024-05-31 12:34:56.987654'), \
             (2, '1999-12-31', '1999-12-31 23:59:59'), \
             (3, NULL, NULL), \
             (4, '0000-00-00', '0000-00-00 00:00:00'), \
             (5, '2024-02-29', '2024-02-29 00:00:01')",
        )
        .unwrap();
}

/// Resolving the ordinal to its select expression matches explicit grouping;
/// previously constant grouping produced one group. Assert both row and vector execution.
#[test]
fn group_by_ordinal_resolves_to_select_expression() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    // Ordinal 2 references YEAR(dt); ORDER BY 2 makes output deterministic.
    let sql = "SELECT COUNT(*), YEAR(dt) FROM vt GROUP BY 2 ORDER BY 2";
    let expected = vec![
        vec![Value::Int(1), Value::Null],
        vec![Value::Int(1), Value::Int(0)],
        vec![Value::Int(1), Value::Int(1999)],
        vec![Value::Int(2), Value::Int(2024)],
    ];
    let legacy = with_vec_mode(VecMode::Off, || rows_of(engine.execute(sql).unwrap()));
    let vec_rows = with_vec_mode(VecMode::On, || rows_of(engine.execute(sql).unwrap()));
    assert_eq!(legacy, expected, "row path wrong groups: {sql}");
    assert_eq!(vec_rows, expected, "vec path wrong groups: {sql}");
}

/// A resolved ordinal may itself name a constant projection. Once another
/// non-constant grouping key remains, the constant lane is redundant and the
/// physical aggregate should not encode/store it for every input row.
#[test]
fn group_by_constant_ordinal_prunes_redundant_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE urls (id INT PRIMARY KEY, url TEXT NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO urls VALUES (1, 'Alpha'), (2, 'alpha'), (3, 'beta')")
        .unwrap();

    let sql = "SELECT 1, url, COUNT(*) FROM urls GROUP BY 1, url ORDER BY url";
    let rows = rows_of(engine.execute(sql).unwrap());
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::String("Alpha".into()), Value::Int(2),],
            vec![Value::Int(1), Value::String("beta".into()), Value::Int(1),],
        ]
    );

    let explain = with_test_env(&[("MYDB_AGG_STRATEGY", Some("hash"))], || {
        format!(
            "{:?}",
            rows_of(engine.execute(&format!("EXPLAIN {sql}")).unwrap())
        )
    });
    assert!(
        !explain.contains("2 group-key"),
        "constant GROUP BY key survived physical planning: {explain}"
    );
}

/// The final constant key is retained because it preserves the empty-input
/// difference between explicit grouping (zero rows) and an implicit aggregate
/// group (one COUNT=0 row).
#[test]
fn group_by_only_constant_keeps_empty_input_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE empty_t (id INT)").unwrap();
    let rows = rows_of(
        engine
            .execute("SELECT 1, COUNT(*) FROM empty_t GROUP BY 1")
            .unwrap(),
    );
    assert!(rows.is_empty());
}

/// Warning-free +/- literal expressions of a signed INT key are determined by
/// that key. They remain available to the projection but do not widen the
/// physical aggregate key.
#[test]
fn group_by_prunes_safe_integer_functional_dependencies() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE fd_keys (id INT PRIMARY KEY, x INT NOT NULL, wide BIGINT NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO fd_keys VALUES (1, 7, 7), (2, 7, 7), (3, -2, -2)")
        .unwrap();
    let sql = "SELECT x, x - 1, x + 2, COUNT(*) AS c FROM fd_keys \
               GROUP BY x, x - 1, x + 2 ORDER BY x";
    let expected = vec![
        vec![Value::Int(-2), Value::Int(-3), Value::Int(0), Value::Int(1)],
        vec![Value::Int(7), Value::Int(6), Value::Int(9), Value::Int(2)],
    ];
    let legacy = with_vec_mode(VecMode::Off, || rows_of(engine.execute(sql).unwrap()));
    let vector = with_vec_mode(VecMode::On, || rows_of(engine.execute(sql).unwrap()));
    assert_eq!(legacy, expected);
    assert_eq!(vector, expected);

    let crate::parser::Statement::Select(mut normalized) =
        crate::parser::parse_statement(sql).unwrap()
    else {
        panic!("expected SELECT")
    };
    crate::planner::resolve_group_by_refs(engine.db_for_tests(), &mut normalized);
    crate::planner::prune_redundant_functional_group_keys(engine.db_for_tests(), &mut normalized);
    assert_eq!(normalized.group_by.len(), 1);

    // BIGINT arithmetic can overflow and therefore keeps its original
    // per-row warning/error behavior and its physical grouping lane.
    let wide = "SELECT wide, wide - 1, COUNT(*) FROM fd_keys GROUP BY wide, wide - 1";
    let crate::parser::Statement::Select(mut normalized) =
        crate::parser::parse_statement(wide).unwrap()
    else {
        panic!("expected SELECT")
    };
    crate::planner::resolve_group_by_refs(engine.db_for_tests(), &mut normalized);
    crate::planner::prune_redundant_functional_group_keys(engine.db_for_tests(), &mut normalized);
    assert_eq!(normalized.group_by.len(), 2);
}

/// Zero/out-of-range ordinals must fail with 1054 'group statement', not group by a constant.
#[test]
fn group_by_ordinal_out_of_range_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    for (sql, pos) in &[
        ("SELECT YEAR(dt), COUNT(*) FROM vt GROUP BY 5", 5),
        ("SELECT YEAR(dt), COUNT(*) FROM vt GROUP BY 0", 0),
    ] {
        let err = engine.execute(sql).unwrap_err();
        assert_eq!(err.code(), 1054, "wrong code for {sql}: {err:?}");
        assert!(
            err.message()
                .contains(&format!("Unknown column '{pos}' in 'group statement'")),
            "wrong message for {sql}: {}",
            err.message()
        );
    }
    // GROUP BY -1 is a legal constant expression, not an ordinal, producing one group.
    let rows = rows_of(
        engine
            .execute("SELECT COUNT(*) FROM vt GROUP BY -1")
            .unwrap(),
    );
    assert_eq!(rows, vec![vec![Value::Int(5)]]);
}

/// Resolve wildcard ordinals after binding against actual expanded column width.
#[test]
fn group_by_ordinal_with_wildcard_binds_after_expansion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    assert_eq!(
        rows_of(
            engine
                .execute("SELECT * FROM vt GROUP BY 1 ORDER BY 1")
                .unwrap()
        ),
        rows_of(engine.execute("SELECT * FROM vt ORDER BY id").unwrap())
    );
    let err = engine.execute("SELECT * FROM vt GROUP BY 4").unwrap_err();
    assert_eq!(err.code(), 1054);
}

/// An ordinal referencing an aggregate fails with 1056 (ER_WRONG_GROUP_FIELD).
#[test]
fn group_by_ordinal_on_aggregate_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    let err = engine
        .execute("SELECT COUNT(*), YEAR(dt) FROM vt GROUP BY 1")
        .unwrap_err();
    assert_eq!(err.code(), 1056, "wrong code: {err:?}");
    assert!(
        err.message().contains("Can't group on"),
        "wrong message: {}",
        err.message()
    );
    // Report the alias when one exists.
    let err = engine
        .execute("SELECT COUNT(*) AS c, YEAR(dt) FROM vt GROUP BY 1")
        .unwrap_err();
    assert_eq!(err.code(), 1056);
    assert!(
        err.message().contains("'c'"),
        "wrong message: {}",
        err.message()
    );
}

/// ROLLUP and GROUPING() retain semantics after planner-side ordinal binding.
#[test]
fn group_by_ordinal_with_rollup_and_grouping() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    let sql = "SELECT YEAR(dt), COUNT(*), GROUPING(1) FROM vt \
               GROUP BY 1 WITH ROLLUP ORDER BY 3, 1";
    let rows = rows_of(engine.execute(sql).unwrap());
    assert_eq!(
        rows,
        vec![
            vec![Value::Null, Value::Int(1), Value::Int(0)],
            vec![Value::Int(0), Value::Int(1), Value::Int(0)],
            vec![Value::Int(1999), Value::Int(1), Value::Int(0)],
            vec![Value::Int(2024), Value::Int(2), Value::Int(0)],
            // Summary row: YEAR slot is NULL and GROUPING(1)=1.
            vec![Value::Null, Value::Int(5), Value::Int(1)],
        ],
        "rollup with ordinal group key: {sql}"
    );
}

// Alias grouping regressions are in group_by_alias.rs; planner::resolve_group_by_refs
// uses MySQL FROM-column-first precedence during prepare_select_for_planning.
