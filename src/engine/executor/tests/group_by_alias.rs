//! GROUP BY select-alias regression: MySQL resolves unqualified GROUP BY/HAVING names
//! against FROM columns before select aliases, the reverse of ORDER BY precedence.
//!
//! Two issues existed before the fix:
//! 1. prepare_select_for_planning validated the original statement before alias resolution.
//!    validate_select_column_references lacked GROUP BY aliases, so valid
//!    GROUP BY y failed with 1054 unknown column on every execution path.
//! 2. The planner always preferred aliases, incorrectly grouping SELECT b AS a
//!    by b rather than the actual FROM column a.
//!
//! planner::resolve_group_by_refs(db, select) now resolves the execution statement
//! in place during prepare; plan_select does the same on its clone. It substitutes
//! aliases only when the name is not a visible FROM column.

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

/// Expression-alias grouping matches explicit GROUP BY YEAR(dt) on row and vector paths.
#[test]
fn group_by_alias_resolves_to_expression() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    let expected = vec![
        vec![Value::Null, Value::Int(1)],
        vec![Value::Int(0), Value::Int(1)],
        vec![Value::Int(1999), Value::Int(1)],
        vec![Value::Int(2024), Value::Int(2)],
    ];
    let sql = "SELECT YEAR(dt) AS y, COUNT(*) FROM vt GROUP BY y ORDER BY y";
    let legacy = with_vec_mode(VecMode::Off, || rows_of(engine.execute(sql).unwrap()));
    let vec_rows = with_vec_mode(VecMode::On, || rows_of(engine.execute(sql).unwrap()));
    assert_eq!(legacy, expected, "row path: {sql}");
    assert_eq!(vec_rows, expected, "vec path: {sql}");
    // EXPLAIN also stops reporting unknown column.
    engine
        .execute("EXPLAIN SELECT YEAR(dt) AS y, COUNT(*) FROM vt GROUP BY y")
        .unwrap();
}

/// The ROLLUP executor path, bind_rollup_select_for_execution, resolves aliases too.
#[test]
fn group_by_alias_with_rollup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    let sql = "SELECT YEAR(dt) AS y, COUNT(*) FROM vt GROUP BY y WITH ROLLUP ORDER BY 2, 1";
    let rows = rows_of(engine.execute(sql).unwrap());
    assert_eq!(
        rows,
        vec![
            vec![Value::Null, Value::Int(1)],
            vec![Value::Int(0), Value::Int(1)],
            vec![Value::Int(1999), Value::Int(1)],
            vec![Value::Int(2024), Value::Int(2)],
            vec![Value::Null, Value::Int(5)],
        ]
    );
}

/// When an alias shadows a real column, MySQL GROUP BY prefers FROM column a:
/// two groups with COUNT=1, rather than one COUNT=2 group using alias b.
/// Hidden b must use the group's first-row value, 10. That fix is covered by
/// group_by_hidden_column.rs; assert the complete row here too.
#[test]
fn group_by_alias_shadowed_by_column_prefers_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE p (a INT PRIMARY KEY, b INT)")
        .unwrap();
    engine
        .execute("INSERT INTO p VALUES (1, 10), (2, 10)")
        .unwrap();
    let rows = rows_of(
        engine
            .execute("SELECT b AS a, COUNT(*) FROM p GROUP BY a ORDER BY 2")
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(10), Value::Int(1)],
            vec![Value::Int(10), Value::Int(1)],
        ],
        "must group by column a (2 groups), not alias target b; hidden b uses first-row value 10: {rows:?}"
    );
}

/// Unknown names still fail with 1054; alias resolution must not accept them.
#[test]
fn group_by_unknown_name_still_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    let err = engine
        .execute("SELECT YEAR(dt) AS y, COUNT(*) FROM vt GROUP BY zzz")
        .unwrap_err();
    assert_eq!(err.code(), 1054, "unexpected: {err:?}");
}

/// The in-memory CTE join path evaluates stmt.group_by directly, so resolution must
/// modify the execution statement during prepare to cover this path.
#[test]
fn group_by_alias_on_cte_join_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    let sql = "WITH c AS (SELECT id, dt FROM vt) \
               SELECT YEAR(c.dt) AS y, COUNT(*) FROM c JOIN vt ON c.id = vt.id \
               GROUP BY y ORDER BY y";
    let rows = rows_of(engine.execute(sql).unwrap());
    assert_eq!(
        rows,
        vec![
            vec![Value::Null, Value::Int(1)],
            vec![Value::Int(0), Value::Int(1)],
            vec![Value::Int(1999), Value::Int(1)],
            vec![Value::Int(2024), Value::Int(2)],
        ]
    );
}

/// HyBench AT3: both sides of an outer comma join are derived aggregates that
/// group by a select-list alias. The parent builds each derived output scope
/// before the child query executes, so early validation must recognize `sid`
/// as the child projection alias. Cover both text and prepared execution.
#[test]
fn group_by_alias_inside_derived_comma_join_and_prepared_query() {
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE savingaccount (accountid INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE transfer (
                id INT PRIMARY KEY, sourceid INT, targetid INT, amount DECIMAL(12,2)
             )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO savingaccount VALUES (1)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO transfer VALUES
                (1,1,2,10.00),(2,1,3,4.00),(3,2,1,20.00),(4,3,1,1.50)",
        )
        .unwrap();

    let literal_sql = "SELECT inbound.t_amount-outbound.t_amount AS t_amount FROM
         (SELECT sa.accountid AS sid, SUM(t.amount) AS t_amount
          FROM savingaccount sa, transfer t
          WHERE sa.accountid=1 AND sa.accountid=t.sourceid GROUP BY sid) AS outbound,
         (SELECT sa.accountid AS sid, SUM(t.amount) AS t_amount
          FROM savingaccount sa, transfer t
          WHERE sa.accountid=1 AND sa.accountid=t.targetid GROUP BY sid) AS inbound";
    let rows = rows_of(engine.execute(literal_sql).unwrap());
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_display_string(), "7.50");

    let prepared_sql = literal_sql
        .replacen("sa.accountid=1", "sa.accountid=?", 1)
        .replacen("sa.accountid=1", "sa.accountid=?", 1);
    let mut session = SessionContext::default();
    let prepared = engine
        .prepare_cached(&mut session, &prepared_sql, 2)
        .unwrap();
    let response = with_params(vec![Value::Int(1), Value::Int(1)], || {
        engine
            .execute_prepared_statement(&mut session, &prepared_sql, prepared.as_ref().clone())
            .unwrap()
            .unwrap()
    })
    .materialize()
    .unwrap();
    let rows = rows_of(response);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_display_string(), "7.50");
}

/// Combine alias grouping with ORDER BY ordinals/aliases to cover interactions with earlier fixes.
#[test]
fn group_by_alias_with_order_by_ordinal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    let sql = "SELECT YEAR(dt) AS y, COUNT(*) FROM vt GROUP BY y ORDER BY 1";
    let rows = rows_of(engine.execute(sql).unwrap());
    assert_eq!(
        rows,
        vec![
            vec![Value::Null, Value::Int(1)],
            vec![Value::Int(0), Value::Int(1)],
            vec![Value::Int(1999), Value::Int(1)],
            vec![Value::Int(2024), Value::Int(2)],
        ]
    );
}
