//! P1-A1: preserve SELECT modifiers rather than silently dropping them.
//!
//! Previously sqlparser parsed SQL_CALC_FOUND_ROWS but mydb's Select structure
//! discarded it before execution. These tests establish:
//!
//! Pass the modifier through Select.sql_calc_found_rows.
//! Emit the standard MySQL 8.0.17 deprecation warning 1287.
//! P2-C3 strip-limit prepass removes LIMIT/OFFSET and executes fully to count rows,
//! then slices in memory; FOUND_ROWS() returns the pre-LIMIT total.

use super::support::execute_materialized as exec;
use super::*;

#[test]
fn sql_calc_found_rows_emits_deprecation_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
    )
    .unwrap();

    // Run with SQL_CALC_FOUND_ROWS + LIMIT. Previously the modifier was
    // silently dropped — sqlparser parsed it but `convert_select` never
    // threaded it through. Now it round-trips and the dispatcher emits
    // warning 1287 matching MySQL 8.0.17's deprecation notice.
    let resp = exec(
        &engine,
        &mut session,
        "SELECT SQL_CALC_FOUND_ROWS id FROM t LIMIT 1",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = resp else {
        panic!("expected materialised result set");
    };
    assert_eq!(rs.rows.len(), 1, "LIMIT 1 returns one row");
    assert!(
        session.warnings.iter().any(|w| w.code() == 1287),
        "expected SQL_CALC_FOUND_ROWS deprecation warning 1287; got {:?}",
        session.warnings
    );

    // P2-C3: `FOUND_ROWS()` now returns the pre-LIMIT total (3), not the
    // LIMIT-truncated count (1). The strip-limit pre-pass runs the query with
    // LIMIT/OFFSET removed, records the total, and slices the window in memory.
    let resp = exec(&engine, &mut session, "SELECT FOUND_ROWS()").unwrap();
    let QueryResponse::ResultSet(rs) = resp else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows[0][0],
        Value::UInt(3),
        "FOUND_ROWS = pre-LIMIT total"
    );
}

#[test]
fn sql_calc_found_rows_with_offset_reports_total() {
    // P2-C3: OFFSET is also stripped for the total; the window is applied in
    // memory. 5 rows, LIMIT 2 OFFSET 1 → 2 rows returned, FOUND_ROWS = 5.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1),(2),(3),(4),(5)",
    )
    .unwrap();
    let resp = exec(
        &engine,
        &mut session,
        "SELECT SQL_CALC_FOUND_ROWS id FROM t ORDER BY id LIMIT 2 OFFSET 1",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = resp else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 2, "LIMIT 2 window");
    assert_eq!(rs.rows[0][0], Value::Int(2), "OFFSET 1 skips id=1");
    let resp = exec(&engine, &mut session, "SELECT FOUND_ROWS()").unwrap();
    let QueryResponse::ResultSet(rs) = resp else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::UInt(5), "FOUND_ROWS = full total");
}

#[test]
fn sql_calc_found_rows_off_does_not_warn() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1), (2)").unwrap();

    let resp = exec(&engine, &mut session, "SELECT id FROM t LIMIT 1").unwrap();
    let _ = resp;
    assert!(
        !session
            .warnings
            .iter()
            .any(|w| w.code() == 1287 && w.message().contains("SQL_CALC_FOUND_ROWS")),
        "ordinary LIMIT must not emit SQL_CALC_FOUND_ROWS deprecation; got {:?}",
        session.warnings
    );
}

fn explain_tables(engine: &StorageQueryEngine, sql: &str) -> Vec<String> {
    let QueryResponse::ResultSet(result) = engine.execute(&format!("EXPLAIN {sql}")).unwrap()
    else {
        panic!("expected EXPLAIN result set");
    };
    result
        .rows
        .iter()
        .filter_map(|row| match &row[2] {
            Value::String(table) if !table.starts_with('<') => Some(table.clone()),
            _ => None,
        })
        .collect()
}

fn straight_join_tempdir() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::Builder::new()
        .prefix("straight-join-")
        .tempdir_in("/tmp/mydb")
        .unwrap()
}

#[test]
fn straight_join_global_and_edge_ordering() {
    let dir = straight_join_tempdir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for table in ["a", "b", "c"] {
        exec(
            &engine,
            &mut session,
            &format!("CREATE TABLE {table} (id INT, v INT)"),
        )
        .unwrap();
    }
    exec(
        &engine,
        &mut session,
        "INSERT INTO a VALUES (1,10),(2,20),(3,30)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO b VALUES (1,100),(2,200),(3,300)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO c VALUES (1,1000)").unwrap();
    for table in ["a", "b", "c"] {
        exec(&engine, &mut session, &format!("ANALYZE TABLE {table}")).unwrap();
    }

    let global = "SELECT STRAIGHT_JOIN a.id FROM a JOIN b ON a.id=b.id JOIN c ON b.id=c.id";
    assert_eq!(explain_tables(&engine, global), ["a", "b", "c"]);
    let local = "SELECT a.id FROM a STRAIGHT_JOIN b ON a.id=b.id JOIN c ON c.id=a.id";
    let order = explain_tables(&engine, local);
    assert!(
        order.iter().position(|table| table == "a") < order.iter().position(|table| table == "b"),
        "left operand must precede right operand: {order:?}"
    );
    let QueryResponse::ResultSet(result) = exec(&engine, &mut session, local).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);

    let using = "SELECT a.id FROM a STRAIGHT_JOIN b USING (id) WHERE a.id=2";
    let QueryResponse::ResultSet(result) = exec(&engine, &mut session, using).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(2)]]);
    assert_eq!(explain_tables(&engine, using), ["a", "b"]);

    exec(&engine, &mut session, "CREATE TABLE d (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut session, "INSERT INTO d VALUES (1),(2)").unwrap();
    let const_lookup = "SELECT STRAIGHT_JOIN a.id FROM a JOIN d ON a.id=d.id WHERE d.id=1";
    assert_eq!(explain_tables(&engine, const_lookup), ["d", "a"]);
}

#[test]
fn straight_join_keeps_outer_and_derived_results() {
    let dir = straight_join_tempdir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE sj_left (id INT PRIMARY KEY)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE sj_right (id INT PRIMARY KEY)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO sj_left VALUES (1),(2)").unwrap();
    exec(&engine, &mut session, "INSERT INTO sj_right VALUES (1)").unwrap();
    let query = "SELECT STRAIGHT_JOIN l.id, r.id FROM sj_left l \
        LEFT JOIN sj_right r ON l.id=r.id ORDER BY l.id";
    let QueryResponse::ResultSet(result) = exec(&engine, &mut session, query).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Null],
        ]
    );
    let query = "SELECT d.id FROM (SELECT STRAIGHT_JOIN l.id FROM sj_left l \
        JOIN sj_right r ON l.id=r.id) AS d";
    let QueryResponse::ResultSet(result) = exec(&engine, &mut session, query).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);

    let QueryResponse::ResultSet(result) = exec(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM sj_left STRAIGHT_JOIN sj_right",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(2)]]);

    exec(
        &engine,
        &mut session,
        "CREATE VIEW sj_view AS SELECT STRAIGHT_JOIN l.id FROM sj_left l \
         JOIN sj_right r ON l.id=r.id",
    )
    .unwrap();
    let QueryResponse::ResultSet(result) =
        exec(&engine, &mut session, "SELECT id FROM sj_view").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);

    let QueryResponse::ResultSet(result) = exec(
        &engine,
        &mut session,
        "WITH q AS (SELECT id FROM sj_right) SELECT STRAIGHT_JOIN l.id \
         FROM sj_left l JOIN q ON l.id=q.id",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);

    exec(
        &engine,
        &mut session,
        "PREPARE sj_stmt FROM 'SELECT STRAIGHT_JOIN x.id FROM sj_left x \
         STRAIGHT_JOIN sj_right y ON x.id=y.id WHERE x.id=?'",
    )
    .unwrap();
    exec(&engine, &mut session, "SET @sj_id=1").unwrap();
    let QueryResponse::ResultSet(result) =
        exec(&engine, &mut session, "EXECUTE sj_stmt USING @sj_id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn straight_join_above_dp_limit_keeps_edge_dependency() {
    let dir = straight_join_tempdir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let mut sql = "SELECT j0.id FROM sj_0 j0".to_string();
    for i in 0..13 {
        exec(
            &engine,
            &mut session,
            &format!("CREATE TABLE sj_{i} (id INT)"),
        )
        .unwrap();
        exec(
            &engine,
            &mut session,
            &format!("INSERT INTO sj_{i} VALUES (1)"),
        )
        .unwrap();
        if i > 0 {
            let operator = if i == 6 { "STRAIGHT_JOIN" } else { "JOIN" };
            sql.push_str(&format!(" {operator} sj_{i} j{i}"));
        }
    }
    let order = explain_tables(&engine, &sql);
    let barrier = order.iter().position(|table| table == "j6").unwrap();
    for i in 0..6 {
        let source = format!("j{i}");
        assert!(
            order.iter().position(|table| table == &source).unwrap() < barrier,
            "{source} must precede j6: {order:?}"
        );
    }
    let QueryResponse::ResultSet(result) = exec(&engine, &mut session, &sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn straight_join_multi_table_dml() {
    let dir = straight_join_tempdir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE sj_a (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE sj_b (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO sj_a VALUES (1,0),(2,0)").unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO sj_b VALUES (1,10),(2,20)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "UPDATE sj_a STRAIGHT_JOIN sj_b ON sj_a.id=sj_b.id SET sj_a.v=sj_b.v",
    )
    .unwrap();
    let QueryResponse::ResultSet(result) =
        exec(&engine, &mut session, "SELECT v FROM sj_a ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(10)], vec![Value::Int(20)]]
    );
    exec(
        &engine,
        &mut session,
        "DELETE sj_a FROM sj_a STRAIGHT_JOIN sj_b ON sj_a.id=sj_b.id WHERE sj_b.id=1",
    )
    .unwrap();
    let QueryResponse::ResultSet(result) =
        exec(&engine, &mut session, "SELECT id FROM sj_a ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(2)]]);
}
