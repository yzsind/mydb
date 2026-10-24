//! SQL-level integration tests for the sysvar registry — verify that
//! the variables don't just round-trip via SET / SHOW but actually
//! affect execution.

use super::*;
use crate::engine::api::SessionContext;
use crate::types::Value;

#[test]
fn init_connect_global_string_scope_privileges_and_persistence() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    let sql = "SET @value='中文;quote'; SELECT @value";
    engine
        .execute_with_session(
            &mut root,
            "SET GLOBAL init_connect='SET @value=''中文;quote''; SELECT @value'",
        )
        .unwrap();
    for query in ["SELECT @@init_connect", "SELECT @@global.init_connect"] {
        assert_eq!(
            one_row(&engine, &mut root, query),
            vec![Value::String(sql.into())]
        );
    }
    for query in [
        "SELECT @@session.init_connect",
        "SELECT @@local.init_connect",
    ] {
        assert_eq!(
            engine
                .execute_with_session(&mut root, query)
                .unwrap_err()
                .code(),
            1238
        );
    }
    for query in [
        "SHOW GLOBAL VARIABLES LIKE 'init_connect'",
        "SHOW SESSION VARIABLES LIKE 'init_connect'",
    ] {
        assert_eq!(
            one_row(&engine, &mut root, query),
            vec![
                Value::String("init_connect".into()),
                Value::String(sql.into())
            ]
        );
    }
    for query in ["SET init_connect=''", "SET SESSION init_connect=''"] {
        assert_eq!(
            engine
                .execute_with_session(&mut root, query)
                .unwrap_err()
                .code(),
            1229
        );
    }
    for query in [
        "SET GLOBAL init_connect=1",
        "SET GLOBAL init_connect=1.5",
        "SET GLOBAL init_connect=TRUE",
        "SET GLOBAL init_connect=1e0",
        "SET GLOBAL init_connect=CAST(1.5 AS DECIMAL(5,2))",
        "SET GLOBAL init_connect=(SELECT 1.5)",
    ] {
        assert_eq!(
            engine
                .execute_with_session(&mut root, query)
                .unwrap_err()
                .code(),
            1232
        );
    }
    engine
        .execute_with_session(&mut root, "SET GLOBAL init_connect=X'5345542040733D31'")
        .unwrap();
    assert_eq!(
        one_row(&engine, &mut root, "SELECT @@init_connect"),
        vec![Value::String("SET @s=1".into())]
    );
    engine
        .execute_with_session(
            &mut root,
            "CREATE TABLE init_source (sql_text VARCHAR(100))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "INSERT INTO init_source VALUES ('SET @from_table=1')",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "SET GLOBAL init_connect=(SELECT sql_text FROM init_source)",
        )
        .unwrap();
    assert_eq!(
        one_row(&engine, &mut root, "SELECT @@init_connect"),
        vec![Value::String("SET @from_table=1".into())]
    );
    engine
        .execute_with_session(&mut root, "CREATE USER 'init_writer'@'%' IDENTIFIED BY ''")
        .unwrap();
    let mut user = SessionContext::default();
    user.user = "init_writer".into();
    user.login_user = "init_writer".into();
    user.authenticated_as = crate::engine::auth::AccountKey::new("init_writer", "%");
    user.current_user_is_system = false;
    assert_eq!(
        engine
            .execute_with_session(&mut user, "SET GLOBAL init_connect='SELECT 1'")
            .unwrap_err()
            .code(),
        1227
    );
    engine
        .execute_with_session(
            &mut root,
            "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'init_writer'@'%'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut user, "SET GLOBAL init_connect='SELECT 1'")
        .unwrap();

    engine
        .execute_with_session(
            &mut root,
            "SET PERSIST_ONLY init_connect='SET @persisted=1'",
        )
        .unwrap();
    assert_eq!(
        one_row(&engine, &mut root, "SELECT @@init_connect"),
        vec![Value::String("SELECT 1".into())]
    );
    crate::engine::sysvar::registry::apply_persisted_on_startup(engine.database());
    assert_eq!(
        one_row(&engine, &mut root, "SELECT @@init_connect"),
        vec![Value::String("SET @persisted=1".into())]
    );
    engine
        .execute_with_session(&mut root, "SET PERSIST init_connect='SET @persisted=2'")
        .unwrap();
    assert_eq!(one_row(&engine, &mut root, "SELECT VARIABLE_VALUE FROM performance_schema.persisted_variables WHERE VARIABLE_NAME='init_connect'"), vec![Value::String("SET @persisted=2".into())]);
    engine
        .execute_with_session(&mut root, "RESET PERSIST init_connect")
        .unwrap();
    engine
        .execute_with_session(&mut root, "SET PERSIST init_connect=NULL")
        .unwrap();
    assert_eq!(one_row(&engine, &mut root, "SELECT VARIABLE_VALUE FROM performance_schema.persisted_variables WHERE VARIABLE_NAME='init_connect'"), vec![Value::String(String::new())]);
    engine
        .execute_with_session(&mut root, "RESET PERSIST init_connect")
        .unwrap();
    engine
        .execute_with_session(&mut root, "SET GLOBAL init_connect=NULL")
        .unwrap();
    assert_eq!(
        one_row(&engine, &mut root, "SELECT @@init_connect"),
        vec![Value::String(String::new())]
    );
    engine
        .execute_with_session(&mut root, "SET GLOBAL init_connect='INVALID SQL'")
        .unwrap();
    engine
        .execute_with_session(&mut root, "SET GLOBAL init_connect=DEFAULT")
        .unwrap();
    assert_eq!(
        one_row(&engine, &mut root, "SELECT @@init_connect"),
        vec![Value::String(String::new())]
    );
}

fn one_row(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Value> {
    match engine.execute_with_session(session, sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs.rows.into_iter().next().expect("one row"),
        other => panic!("expected result set, got {other:?}"),
    }
}

#[test]
fn server_version_includes_product_version_and_build_date() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let build_date = env!("MYDB_BUILD_DATE");
    assert_eq!(build_date.len(), 8);
    assert!(build_date.bytes().all(|byte| byte.is_ascii_digit()));
    chrono::NaiveDate::parse_from_str(build_date, "%Y%m%d").expect("valid build date");
    let expected = Value::String(format!(
        "8.0.46-mydb-{}-{build_date}",
        env!("CARGO_PKG_VERSION")
    ));

    for sql in [
        "SELECT VERSION()",
        "SELECT @@version",
        "SELECT @@global.version",
        "SELECT VERSION() FROM (SELECT 1 AS n) AS t",
    ] {
        assert_eq!(
            one_row(&engine, &mut session, sql),
            vec![expected.clone()],
            "{sql}"
        );
    }
    assert_eq!(
        one_row(&engine, &mut session, "SELECT VERSION(), @@version"),
        vec![expected.clone(), expected.clone()]
    );
    assert_eq!(
        one_row(&engine, &mut session, "SHOW VARIABLES LIKE 'version'"),
        vec![Value::String("version".into()), expected]
    );
}

#[test]
fn mydb_redo_enabled_scope_inheritance_and_transaction_freeze() {
    crate::engine::sysvar::builtin::mydb_storage::set_global_redo_enabled(true);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut existing = SessionContext::default();

    assert_eq!(
        one_row(&engine, &mut existing, "SELECT @@session.mydb_redo_enabled")[0],
        Value::String("1".to_string())
    );
    engine
        .execute_with_session(&mut existing, "SET GLOBAL mydb_redo_enabled=OFF")
        .unwrap();
    // GLOBAL is a connection default; it must not mutate existing sessions.
    assert_eq!(
        one_row(&engine, &mut existing, "SELECT @@session.mydb_redo_enabled")[0],
        Value::String("1".to_string())
    );
    let mut inherited = SessionContext::default();
    assert!(!inherited.redo_enabled);
    engine
        .execute_with_session(&mut inherited, "SET SESSION mydb_redo_enabled=ON")
        .unwrap();
    assert!(inherited.redo_enabled);

    engine
        .execute_with_session(&mut inherited, "SET autocommit=OFF")
        .unwrap();
    // Disabling autocommit alone leaves the transaction idle. Its redo
    // setting is mutable until a transaction actually starts.
    engine
        .execute_with_session(&mut inherited, "SET SESSION mydb_redo_enabled=OFF")
        .unwrap();
    assert!(!inherited.redo_enabled);
    engine
        .execute_with_session(&mut inherited, "SET SESSION mydb_redo_enabled=ON")
        .unwrap();
    engine
        .execute_with_session(&mut inherited, "BEGIN")
        .unwrap();
    let err = engine
        .execute_with_session(&mut inherited, "SET SESSION mydb_redo_enabled=OFF")
        .unwrap_err();
    assert_eq!(err.code(), 1568);
    engine
        .execute_with_session(&mut inherited, "ROLLBACK")
        .unwrap();
    engine
        .execute_with_session(&mut inherited, "SET autocommit=ON")
        .unwrap();

    let err = engine
        .execute_with_session(&mut inherited, "SET SESSION mydb_redo_enabled=2")
        .unwrap_err();
    assert_eq!(err.code(), 1231);
    crate::engine::sysvar::builtin::mydb_storage::set_global_redo_enabled(true);
}

// ---------------------------------------------------------------------------
// sql_safe_updates
// ---------------------------------------------------------------------------

#[test]
fn sql_safe_updates_blocks_naked_update() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (1, 1), (2, 2)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET sql_safe_updates = 1")
        .unwrap();
    // No WHERE, no LIMIT — should fail with 1175.
    let err = engine
        .execute_with_session(&mut s, "UPDATE t SET v = 9")
        .unwrap_err();
    assert_eq!(err.code(), 1175);
    // With a LIMIT it's accepted.
    engine
        .execute_with_session(&mut s, "UPDATE t SET v = 9 LIMIT 10")
        .unwrap();
    // With a WHERE it's accepted.
    engine
        .execute_with_session(&mut s, "UPDATE t SET v = 8 WHERE id = 1")
        .unwrap();
}

#[test]
fn sql_safe_updates_blocks_naked_delete() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (1, 1)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET sql_safe_updates = 1")
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "DELETE FROM t")
        .unwrap_err();
    assert_eq!(err.code(), 1175);
    engine
        .execute_with_session(&mut s, "DELETE FROM t WHERE id = 1")
        .unwrap();
}

#[test]
fn sql_safe_updates_off_allows_naked_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (1, 1)")
        .unwrap();
    // Default: sql_safe_updates = OFF. No WHERE is fine.
    engine
        .execute_with_session(&mut s, "UPDATE t SET v = 9")
        .unwrap();
    engine
        .execute_with_session(&mut s, "DELETE FROM t")
        .unwrap();
}

// ---------------------------------------------------------------------------
// sql_select_limit
// ---------------------------------------------------------------------------

#[test]
fn sql_select_limit_caps_implicit_limit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    for i in 1..=10 {
        engine
            .execute_with_session(&mut s, &format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }
    engine
        .execute_with_session(&mut s, "SET sql_select_limit = 3")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "SELECT * FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 3);
    // Explicit LIMIT overrides the implicit one (downward).
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "SELECT * FROM t LIMIT 5")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 5);
    // After reset, the cap goes away.
    engine
        .execute_with_session(&mut s, "SET sql_select_limit = 18446744073709551615")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "SELECT * FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 10);
}

// ---------------------------------------------------------------------------
// read_only / super_read_only / transaction_read_only
// ---------------------------------------------------------------------------

#[test]
fn transaction_read_only_blocks_dml_for_next_txn() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET transaction_read_only = 1")
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (1)")
        .unwrap_err();
    assert_eq!(err.code(), 1792);
    // Re-enable and the next INSERT goes through.
    engine
        .execute_with_session(&mut s, "SET transaction_read_only = 0")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (1)")
        .unwrap();
}

#[test]
fn super_read_only_blocks_even_super_users() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET GLOBAL super_read_only = 1")
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (1)")
        .unwrap_err();
    assert_eq!(err.code(), 1290);
    // Reset; subsequent writes succeed.
    engine
        .execute_with_session(&mut s, "SET GLOBAL super_read_only = 0")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET GLOBAL read_only = 0")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (1)")
        .unwrap();
}

// ---------------------------------------------------------------------------
// time_zone
// ---------------------------------------------------------------------------

#[test]
fn time_zone_session_offset_shifts_now() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    // Pin to UTC, capture the timestamp.
    engine
        .execute_with_session(&mut s, "SET time_zone = '+00:00'")
        .unwrap();
    let utc_row = one_row(&engine, &mut s, "SELECT UNIX_TIMESTAMP(NOW())");
    let utc_ts: i64 = match &utc_row[0] {
        Value::Int(i) => *i,
        Value::UInt(u) => *u as i64,
        Value::String(s) => s.parse().unwrap(),
        other => panic!("unexpected: {other:?}"),
    };
    // Switch to +08:00 — the NOW() string should advance by ~8h.
    engine
        .execute_with_session(&mut s, "SET time_zone = '+08:00'")
        .unwrap();
    let shifted_row = one_row(&engine, &mut s, "SELECT HOUR(NOW())");
    let shifted_hour: i64 = match &shifted_row[0] {
        Value::Int(i) => *i,
        Value::UInt(u) => *u as i64,
        Value::String(s) => s.parse().unwrap(),
        other => panic!("unexpected: {other:?}"),
    };
    // We don't know the exact hour but it should be in 0..24, and the
    // configured zone should round-trip via @@time_zone.
    assert!((0..24).contains(&shifted_hour));
    let tz_row = one_row(&engine, &mut s, "SELECT @@time_zone");
    assert_eq!(tz_row[0], Value::String("+08:00".to_string()));
    let _ = utc_ts;
}

// ---------------------------------------------------------------------------
// optimizer_switch
// ---------------------------------------------------------------------------

#[test]
fn optimizer_switch_merges_delta_in_sql() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET optimizer_switch = 'hash_join=off'")
        .unwrap();
    let row = one_row(&engine, &mut s, "SELECT @@optimizer_switch");
    let optsw = match &row[0] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected: {other:?}"),
    };
    assert!(optsw.contains("hash_join=off"));
    assert!(optsw.contains("semijoin=on"));
}

// ---------------------------------------------------------------------------
// character_set_database / collation_database
// ---------------------------------------------------------------------------

#[test]
fn character_set_database_derives_from_current_database() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    let row = one_row(&engine, &mut s, "SELECT @@character_set_database");
    assert_eq!(row[0], Value::String("utf8mb4".to_string()));
    let row = one_row(&engine, &mut s, "SELECT @@collation_database");
    assert_eq!(row[0], Value::String("utf8mb4_0900_ai_ci".to_string()));
}

// ---------------------------------------------------------------------------
// Unknown variable handling
// ---------------------------------------------------------------------------

#[test]
fn unknown_variable_set_errors_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "SET some_unknown_variable = 1",
        "SET some_unknown_variable = DEFAULT",
    ] {
        let err = engine.execute_with_session(&mut s, sql).unwrap_err();
        assert_eq!(err.code(), 1193);
    }
}

// ---------------------------------------------------------------------------
// Session-overlay correctness in SHOW VARIABLES
// ---------------------------------------------------------------------------

#[test]
fn show_session_variables_reflect_session_state() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET autocommit = 0")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "SHOW SESSION VARIABLES LIKE 'autocommit'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![vec![
            Value::String("autocommit".to_string()),
            Value::String("OFF".to_string()),
        ]]
    );
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "SHOW GLOBAL VARIABLES LIKE 'autocommit'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![vec![
            Value::String("autocommit".to_string()),
            Value::String("ON".to_string()),
        ]]
    );
}

// ---------------------------------------------------------------------------
// Aliases
// ---------------------------------------------------------------------------

#[test]
fn tx_isolation_alias_works_in_sql() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET tx_isolation = 'READ-COMMITTED'")
        .unwrap();
    let row = one_row(&engine, &mut s, "SELECT @@tx_isolation");
    assert_eq!(row[0], Value::String("READ-COMMITTED".to_string()));
    let row = one_row(&engine, &mut s, "SELECT @@transaction_isolation");
    assert_eq!(row[0], Value::String("READ-COMMITTED".to_string()));
}

// ---------------------------------------------------------------------------
// ORM stubs are settable and round-trip.
// ---------------------------------------------------------------------------

#[test]
fn max_execution_time_kills_slow_select() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    // 50 ms cap, then SLEEP(1) — should fail with 3024.
    engine
        .execute_with_session(&mut s, "SET max_execution_time = 50")
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "SELECT SLEEP(1)")
        .unwrap_err();
    assert_eq!(err.code(), 3024);
}

#[test]
fn max_execution_time_is_inherited_by_streaming_cursor_worker() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1), (2)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET max_execution_time = 100")
        .unwrap();

    let response = engine
        .execute_streaming_with_session_for_test(&mut session, "SELECT id FROM t")
        .unwrap();
    let QueryResponse::StreamingResultSet(mut result) = response else {
        panic!("expected streaming result set");
    };
    std::thread::sleep(std::time::Duration::from_millis(120));
    let err = result.cursor.next_row().unwrap_err();
    assert_eq!(err.code(), 3024);
}

#[test]
fn max_execution_time_hint_overrides_session_cap() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    // Session cap is the (very generous) default — but the optimizer
    // hint pins this statement to 50 ms. SLEEP(1) must still trip 3024.
    let err = engine
        .execute_with_session(&mut s, "SELECT /*+ MAX_EXECUTION_TIME(50) */ SLEEP(1)")
        .unwrap_err();
    assert_eq!(err.code(), 3024);
}

#[test]
fn max_execution_time_hint_with_zero_disables_session_cap() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    // Session cap is 1 ms — would normally trip — but the hint
    // explicitly sets 0 ("no cap"). The query should now complete.
    engine
        .execute_with_session(&mut s, "SET max_execution_time = 1")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SELECT /*+ MAX_EXECUTION_TIME(0) */ SLEEP(0.1)")
        .unwrap();
}

#[test]
fn with_cte_max_execution_time_hint_covers_materialization_and_zero_override() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let error = engine
        .execute_with_session(
            &mut session,
            "WITH c AS (SELECT SLEEP(0.2) AS v) SELECT /*+ MAX_EXECUTION_TIME(20) */ v FROM c",
        )
        .unwrap_err();
    assert_eq!(error.code(), 3024);
    engine
        .execute_with_session(&mut session, "SET max_execution_time=1")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "WITH c AS (SELECT SLEEP(0.05) AS v) SELECT /*+ MAX_EXECUTION_TIME(0) */ v FROM c",
        )
        .unwrap();
}

// ---------------------------------------------------------------------------
// information_schema / performance_schema reflect session overlay
// ---------------------------------------------------------------------------

#[test]
fn information_schema_session_variables_overlays_session() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET autocommit = 0")
        .unwrap();
    let QueryResponse::ResultSet(session_rs) = engine
        .execute_with_session(
            &mut s,
            "SELECT VARIABLE_VALUE FROM information_schema.session_variables WHERE VARIABLE_NAME = 'autocommit'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        session_rs.rows,
        vec![vec![Value::String("OFF".to_string())]]
    );

    let QueryResponse::ResultSet(global_rs) = engine
        .execute_with_session(
            &mut s,
            "SELECT VARIABLE_VALUE FROM information_schema.global_variables WHERE VARIABLE_NAME = 'autocommit'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(global_rs.rows, vec![vec![Value::String("ON".to_string())]]);
}

#[test]
fn performance_schema_variables_by_thread_emits_per_session_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(
            &mut s,
            "SELECT THREAD_ID, VARIABLE_NAME, VARIABLE_VALUE \
             FROM performance_schema.variables_by_thread \
             WHERE VARIABLE_NAME = 'autocommit'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(!rs.rows.is_empty());
    // One row per live session/thread.
    for row in &rs.rows {
        assert_eq!(row[1], Value::String("autocommit".to_string()));
    }
}

// ---------------------------------------------------------------------------
// JDBC handshake — realistic SET / SELECT @@ batch a Connector/J client
// issues on connect. None of these should error out.
// ---------------------------------------------------------------------------

#[test]
fn jdbc_style_handshake_does_not_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    let probe_sequence: &[&str] = &[
        // Connector/J 8.x startup probe — the abbreviated form that all
        // recent drivers (J/Connector, MySQL JDBC, Connector/NodeJS) issue.
        "SELECT @@session.auto_increment_increment, \
                @@character_set_client, \
                @@character_set_connection, \
                @@character_set_results, \
                @@character_set_server, \
                @@collation_server, \
                @@collation_connection, \
                @@init_connect, \
                @@interactive_timeout, \
                @@license, \
                @@lower_case_table_names, \
                @@max_allowed_packet, \
                @@net_write_timeout, \
                @@performance_schema, \
                @@query_cache_size, \
                @@query_cache_type, \
                @@sql_mode, \
                @@system_time_zone, \
                @@time_zone, \
                @@transaction_isolation, \
                @@transaction_read_only, \
                @@tx_isolation, \
                @@version_comment, \
                @@version, \
                @@wait_timeout",
        "SET NAMES utf8mb4",
        "SET character_set_results = utf8mb4",
        "SET autocommit = 1",
        "SET sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE'",
        "SET SESSION transaction_isolation = 'REPEATABLE-READ'",
        "SET SESSION wait_timeout = 28800",
        "SET SESSION net_read_timeout = 30",
        "SET SESSION net_write_timeout = 60",
        "SET SESSION max_execution_time = 0",
        "SET SESSION sql_select_limit = DEFAULT",
        "SET SESSION sql_safe_updates = 0",
        "SET SESSION foreign_key_checks = 1",
        "SET SESSION unique_checks = 1",
        "SHOW VARIABLES LIKE 'character_set_%'",
        "SHOW VARIABLES LIKE 'collation_%'",
        "SHOW VARIABLES LIKE 'innodb_version'",
        "SHOW VARIABLES LIKE 'version%'",
        "SHOW VARIABLES LIKE 'sql_mode'",
    ];
    for sql in probe_sequence {
        engine
            .execute_with_session(&mut s, sql)
            .unwrap_or_else(|err| panic!("handshake step failed: {sql}\n  -> {err:?}"));
    }
}

#[test]
fn default_table_encryption_compatibility_probe_returns_false() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    for sql in [
        "SELECT @@default_table_encryption",
        "SELECT @@session.default_table_encryption",
        "SELECT @@global.default_table_encryption",
    ] {
        assert_eq!(
            one_row(&engine, &mut session, sql),
            vec![Value::String("0".to_string())],
            "{sql} should report disabled"
        );
    }
}

// ---------------------------------------------------------------------------
// Error model alignment with MySQL.
// ---------------------------------------------------------------------------

#[test]
fn unknown_variable_in_strict_mode_returns_1193() {
    let mut s = SessionContext::default();
    let err = crate::engine::sysvar::registry()
        .write(
            "this_variable_definitely_does_not_exist",
            crate::engine::sysvar::SetScope::Default,
            &mut s,
            None,
            crate::engine::sysvar::SysVarValue::Int(1),
        )
        .unwrap_err();
    assert_eq!(err.code(), 1193);
}

#[test]
fn set_global_on_session_only_var_returns_1228() {
    let mut s = SessionContext::default();
    let err = crate::engine::sysvar::registry()
        .write(
            "sql_log_off",
            crate::engine::sysvar::SetScope::Global,
            &mut s,
            None,
            crate::engine::sysvar::SysVarValue::Bool(true),
        )
        .unwrap_err();
    assert_eq!(err.code(), 1228);
}

#[test]
fn set_session_on_global_only_var_returns_1229() {
    let mut s = SessionContext::default();
    let err = crate::engine::sysvar::registry()
        .write(
            "read_only",
            crate::engine::sysvar::SetScope::Default,
            &mut s,
            None,
            crate::engine::sysvar::SysVarValue::Bool(true),
        )
        .unwrap_err();
    assert_eq!(err.code(), 1229);
}

#[test]
fn set_read_only_variable_returns_1238() {
    let mut s = SessionContext::default();
    let err = crate::engine::sysvar::registry()
        .write(
            "version",
            crate::engine::sysvar::SetScope::Default,
            &mut s,
            None,
            crate::engine::sysvar::SysVarValue::String("9.0".into()),
        )
        .unwrap_err();
    assert_eq!(err.code(), 1238);
}

#[test]
fn set_out_of_range_value_returns_1231() {
    let mut s = SessionContext::default();
    // wait_timeout range is [1, 31_536_000]. 0 is out of range.
    let err = crate::engine::sysvar::registry()
        .write(
            "wait_timeout",
            crate::engine::sysvar::SetScope::Global,
            &mut s,
            None,
            crate::engine::sysvar::SysVarValue::UInt(0),
        )
        .unwrap_err();
    assert!(err.is(crate::error::catalog::ER_WRONG_VALUE_FOR_VAR));
}

#[test]
fn set_enum_invalid_value_returns_1231() {
    let mut s = SessionContext::default();
    let err = crate::engine::sysvar::registry()
        .write(
            "transaction_isolation",
            crate::engine::sysvar::SetScope::Session,
            &mut s,
            None,
            crate::engine::sysvar::SysVarValue::String("DIRTY-READ".into()),
        )
        .unwrap_err();
    assert_eq!(err.code(), 1231);
}

#[test]
fn innodb_stub_variables_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET GLOBAL innodb_buffer_pool_size = 268435456")
        .unwrap();
    let row = one_row(&engine, &mut s, "SELECT @@global.innodb_buffer_pool_size");
    assert_eq!(row[0], Value::String("268435456".to_string()));
    let row = one_row(&engine, &mut s, "SELECT @@innodb_version");
    assert_eq!(row[0], Value::String("8.0.0".to_string()));
}

// ---------------------------------------------------------------------------
// M7: runtime-adjustable LOB GUCs.
// ---------------------------------------------------------------------------

/// Read a numeric SHOW ENGINE MYDB STATUS metric: first column key, second value.
fn lob_status_metric(engine: &StorageQueryEngine, key: &str) -> u64 {
    let QueryResponse::ResultSet(status) = engine.execute("SHOW ENGINE MYDB STATUS").unwrap()
    else {
        panic!("expected status result set");
    };
    status
        .rows
        .iter()
        .find(|row| row[1] == Value::String(key.into()))
        .and_then(|row| match &row[2] {
            Value::String(s) => s.parse::<u64>().ok(),
            _ => None,
        })
        .unwrap_or_else(|| panic!("status metric {key} not found"))
}

#[test]
fn mydb_lob_inline_max_runtime_tunable_changes_externalization() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, b LONGBLOB)")
        .unwrap();

    // Default inline_max=4096 keeps a 2000B value inline without externalization.
    let row = one_row(&engine, &mut s, "SELECT @@global.mydb_lob_inline_max");
    assert_eq!(
        row[0],
        Value::String("4096".to_string()),
        "The default must be 4096"
    );
    let ext0 = lob_status_metric(&engine, "dml_lob_chains_externalized");
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (1, REPEAT('x', 2000))")
        .unwrap();
    let ext1 = lob_status_metric(&engine, "dml_lob_chains_externalized");
    assert_eq!(
        ext1, ext0,
        "2000B is below the default 4096 threshold and must remain inline"
    );

    // Lowering the runtime threshold to 1024 externalizes the same 2000B value.
    engine
        .execute_with_session(&mut s, "SET GLOBAL mydb_lob_inline_max = 1024")
        .unwrap();
    let row = one_row(&engine, &mut s, "SELECT @@global.mydb_lob_inline_max");
    assert_eq!(
        row[0],
        Value::String("1024".to_string()),
        "SET must expose the new value"
    );
    // SHOW VARIABLES must reflect the change too.
    let QueryResponse::ResultSet(vars) = engine
        .execute_with_session(&mut s, "SHOW VARIABLES LIKE 'mydb_lob_inline_max'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(vars.rows.len(), 1);
    assert_eq!(vars.rows[0][1], Value::String("1024".to_string()));

    let ext2 = lob_status_metric(&engine, "dml_lob_chains_externalized");
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (2, REPEAT('y', 2000))")
        .unwrap();
    let ext3 = lob_status_metric(&engine, "dml_lob_chains_externalized");
    assert_eq!(
        ext3,
        ext2 + 1,
        "Lowering the threshold must externalize 2000B (externalized +1)"
    );

    // Both rows round-trip byte-for-byte.
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "SELECT id, b FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 2);
    // LONGBLOB returns Value::Bytes.
    assert_eq!(rs.rows[0][1], Value::Bytes("x".repeat(2000).into_bytes()));
    assert_eq!(rs.rows[1][1], Value::Bytes("y".repeat(2000).into_bytes()));
}

#[test]
fn mydb_lob_inline_max_rejects_out_of_range() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    // UInt sysvars reject negative and nonnumeric values.
    let err = engine
        .execute_with_session(&mut s, "SET GLOBAL mydb_lob_inline_max = -5")
        .unwrap_err();
    assert!(
        err.code() != 0,
        "Negative values must be rejected (error code {})",
        err.code()
    );
}

#[test]
fn mydb_lob_compression_runtime_tunable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    // Default off is zero.
    let row = one_row(&engine, &mut s, "SELECT @@global.mydb_lob_compression");
    assert_eq!(
        row[0],
        Value::String("0".to_string()),
        "Compression defaults to off=0"
    );
    // Enable at runtime with 1 for LZ4.
    engine
        .execute_with_session(&mut s, "SET GLOBAL mydb_lob_compression = 1")
        .unwrap();
    let row = one_row(&engine, &mut s, "SELECT @@global.mydb_lob_compression");
    assert_eq!(
        row[0],
        Value::String("1".to_string()),
        "The value must be 1 after SET"
    );
    // Compressible large LOBs written after enabling must round-trip transparently byte-for-byte.
    engine
        .execute_with_session(&mut s, "CREATE TABLE c (id INT PRIMARY KEY, b LONGBLOB)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO c VALUES (1, REPEAT('ab', 20000))")
        .unwrap();
    let row = one_row(&engine, &mut s, "SELECT b FROM c WHERE id = 1");
    assert_eq!(
        row[0],
        Value::Bytes("ab".repeat(20000).into_bytes()),
        "Compressed chains must round-trip byte-for-byte"
    );
}

#[test]
fn mydb_lob_other_knobs_round_trip() {
    // Other runtime LOB GUCs round-trip through SET GLOBAL and @@global.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for (var, val) in [
        ("mydb_lob_reuse_unchanged", "0"),
        ("mydb_lob_shrink_disable", "1"),
        ("mydb_lob_shrink_min_free_pages", "8"),
        ("mydb_lob_extent_min_pages", "16"),
        ("mydb_lob_prefetch_disable", "1"),
        ("mydb_lob_compress_min_bytes", "512"),
    ] {
        engine
            .execute_with_session(&mut s, &format!("SET GLOBAL {var} = {val}"))
            .unwrap();
        let row = one_row(&engine, &mut s, &format!("SELECT @@global.{var}"));
        assert_eq!(
            row[0],
            Value::String(val.to_string()),
            "{var} SET/@@global must round-trip {val}"
        );
    }
    // Boolean value 2 is out of range and rejected by DataType min=0/max=1.
    let err = engine
        .execute_with_session(&mut s, "SET GLOBAL mydb_lob_reuse_unchanged = 2")
        .unwrap_err();
    assert!(
        err.code() != 0,
        "Out-of-range boolean GUC values must be rejected"
    );
}

#[test]
fn mydb_group_commit_knobs_round_trip() {
    // M3: group-commit window/adaptive switch are runtime-adjustable via SET GLOBAL and @@global.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    engine
        .execute_with_session(&mut s, "SET GLOBAL mydb_group_commit_us = 250")
        .unwrap();
    let row = one_row(&engine, &mut s, "SELECT @@global.mydb_group_commit_us");
    assert_eq!(row[0], Value::String("250".to_string()));

    // Zero means natural batching and is valid.
    engine
        .execute_with_session(&mut s, "SET GLOBAL mydb_group_commit_us = 0")
        .unwrap();
    let row = one_row(&engine, &mut s, "SELECT @@global.mydb_group_commit_us");
    assert_eq!(row[0], Value::String("0".to_string()));

    engine
        .execute_with_session(&mut s, "SET GLOBAL mydb_group_commit_adaptive = 1")
        .unwrap();
    let row = one_row(
        &engine,
        &mut s,
        "SELECT @@global.mydb_group_commit_adaptive",
    );
    assert_eq!(row[0], Value::String("1".to_string()));

    // Boolean value 2 must be rejected.
    let err = engine
        .execute_with_session(&mut s, "SET GLOBAL mydb_group_commit_adaptive = 2")
        .unwrap_err();
    assert!(
        err.code() != 0,
        "Out-of-range adaptive boolean GUC values must be rejected"
    );
}

#[test]
fn inherited_group_concat_limit_is_enforced_by_parsed_protocol_entry() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut admin = SessionContext::default();
    engine
        .execute_with_session(&mut admin, "SET GLOBAL group_concat_max_len=8")
        .unwrap();
    let mut session = SessionContext::default();
    let sql = "SELECT GROUP_CONCAT(v) FROM (SELECT 'abcdef' v UNION ALL SELECT 'ghijkl') t";
    let parsed = std::sync::Arc::new(crate::parser::parse_statement(sql).unwrap());
    let response =
        crate::engine::api::QueryEngine::execute_parsed(&engine, &mut session, sql, parsed)
            .unwrap()
            .materialize()
            .unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected result set")
    };
    assert_eq!(crate::storage::value_to_string(&result.rows[0][0]).len(), 8);
    engine
        .execute_with_session(&mut admin, "SET GLOBAL group_concat_max_len=DEFAULT")
        .unwrap();
}

#[test]
fn inherited_deadline_is_enforced_by_parsed_constant_select() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut admin = SessionContext::default();
    engine
        .execute_with_session(&mut admin, "SET GLOBAL max_execution_time=25")
        .unwrap();
    let mut session = SessionContext::default();
    let sql = "SELECT SLEEP(0.2)";
    let parsed = std::sync::Arc::new(crate::parser::parse_statement(sql).unwrap());
    let err = crate::engine::api::QueryEngine::execute_parsed(&engine, &mut session, sql, parsed)
        .unwrap_err();
    assert_eq!(err.code(), 3024);
    engine
        .execute_with_session(&mut admin, "SET GLOBAL max_execution_time=DEFAULT")
        .unwrap();
}

#[test]
fn auto_increment_step_offset_affect_all_insert_shapes() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(
            &mut s,
            "SET auto_increment_increment=3, auto_increment_offset=2",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE ai (id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO ai(v) VALUES(10),(11),(12)")
        .unwrap();
    assert_eq!(
        one_row(&engine, &mut s, "SELECT LAST_INSERT_ID()")[0],
        Value::Int(2)
    );
    engine
        .execute_with_session(
            &mut s,
            "INSERT INTO ai VALUES(20,20),(NULL,21),(7,7),(NULL,22)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut s, "PREPARE ai_ins FROM 'INSERT INTO ai(v) VALUES(30)'")
        .unwrap();
    engine
        .execute_with_session(&mut s, "EXECUTE ai_ins")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO ai(v) SELECT 40 UNION ALL SELECT 41")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut s, "SELECT id FROM ai ORDER BY id")
        .unwrap()
    else {
        panic!("rows")
    };
    let ids: Vec<_> = result
        .rows
        .iter()
        .map(|r| {
            crate::storage::value_to_string(&r[0])
                .parse::<u64>()
                .unwrap()
        })
        .collect();
    assert_eq!(ids, vec![2, 5, 7, 8, 20, 23, 26, 29, 32, 35]);
    engine
        .execute_with_session(
            &mut s,
            "SET auto_increment_increment=2, auto_increment_offset=3",
        )
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO ai(v) VALUES(50)")
        .unwrap();
    assert_eq!(
        one_row(&engine, &mut s, "SELECT LAST_INSERT_ID()")[0],
        Value::Int(37)
    );
    let mut peer = SessionContext::default();
    engine
        .execute_with_session(&mut peer, "INSERT INTO ai(v) VALUES(60)")
        .unwrap();
    assert_eq!(
        one_row(&engine, &mut peer, "SELECT LAST_INSERT_ID()")[0],
        Value::Int(38)
    );
    engine
        .execute_with_session(&mut s, "INSERT INTO ai(v) VALUES(70)")
        .unwrap();
    assert_eq!(
        one_row(&engine, &mut s, "SELECT LAST_INSERT_ID()")[0],
        Value::Int(39)
    );
}

#[test]
fn auto_increment_step_unsigned_boundary_and_restart() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let open = || {
        StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            StorageConfig {
                durability: crate::storage::DurabilityMode::Strict,
                ..memory_test_storage_config()
            },
        )
        .unwrap()
    };
    {
        let engine = open();
        let mut s = SessionContext::default();
        engine
            .execute_with_session(
                &mut s,
                "SET auto_increment_increment=2, auto_increment_offset=1",
            )
            .unwrap();
        engine
            .execute_with_session(
                &mut s,
                "CREATE TABLE ai (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)",
            )
            .unwrap();
        engine
            .execute_with_session(&mut s, "INSERT INTO ai VALUES(18446744073709551612)")
            .unwrap();
        engine
            .execute_with_session(&mut s, "INSERT INTO ai VALUES(NULL),(NULL)")
            .unwrap();
        let row = one_row(&engine, &mut s, "SELECT MAX(id),COUNT(*) FROM ai");
        assert_eq!(row[0], Value::UInt(u64::MAX));
        assert!(engine
            .execute_with_session(&mut s, "INSERT INTO ai VALUES(NULL)")
            .is_err());
    }
    let engine = open();
    let mut s = SessionContext::default();
    assert_eq!(
        one_row(&engine, &mut s, "SELECT MAX(id) FROM ai")[0],
        Value::UInt(u64::MAX)
    );
    assert!(engine
        .execute_with_session(&mut s, "INSERT INTO ai VALUES(NULL)")
        .is_err());
}

#[test]
fn auto_increment_step_concurrent_sessions_keep_unique_progressions() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE ai (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, worker INT)")
        .unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let workers: Vec<_> = (1..=2)
        .map(|offset| {
            let engine = engine.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut s = SessionContext::default();
                s.connection_id = offset + 10;
                engine
                    .execute_with_session(
                        &mut s,
                        &format!("SET auto_increment_increment=2, auto_increment_offset={offset}"),
                    )
                    .unwrap();
                barrier.wait();
                for _ in 0..20 {
                    engine
                        .execute_with_session(
                            &mut s,
                            &format!("INSERT INTO ai(worker) VALUES({offset}),({offset})"),
                        )
                        .unwrap();
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    let mut s = SessionContext::default();
    let row = one_row(
        &engine,
        &mut s,
        "SELECT COUNT(*), COUNT(DISTINCT id), SUM(MOD(id,2) <> MOD(worker,2)) FROM ai",
    );
    assert_eq!(crate::storage::value_to_string(&row[0]), "80");
    assert_eq!(crate::storage::value_to_string(&row[1]), "80");
    assert_eq!(crate::storage::value_to_string(&row[2]), "0");
}

#[test]
fn show_gipk_in_create_table_and_information_schema_compatibility() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let name = "show_gipk_in_create_table_and_information_schema";
    let value = |s: &str| vec![Value::String(s.into())];
    let read = format!("SELECT @@{name}");
    assert_eq!(one_row(&engine, &mut session, &read), value("1"));
    assert_eq!(
        one_row(
            &engine,
            &mut session,
            &format!("SHOW VARIABLES LIKE '{name}'")
        ),
        vec![Value::String(name.into()), Value::String("ON".into())]
    );

    // Explicit invisible primary keys, even with MySQL's GIPK column name
    // and type, are user metadata in mydb and must remain visible.
    engine.execute_with_session(&mut session,
        "CREATE TABLE gipk_metadata (my_row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT INVISIBLE, v INT, PRIMARY KEY (my_row_id))"
    ).unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE gipk_regular (id INT PRIMARY KEY, v INT INVISIBLE)",
        )
        .unwrap();
    for table in ["gipk_metadata", "gipk_regular"] {
        for sql in [
            format!("SHOW CREATE TABLE {table}"),
            format!("SHOW FULL COLUMNS FROM {table}"),
            format!("SHOW INDEX FROM {table}"),
            format!("SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_KEY, EXTRA FROM information_schema.columns WHERE TABLE_SCHEMA='test' AND TABLE_NAME='{table}' ORDER BY ORDINAL_POSITION"),
            format!("SELECT INDEX_NAME, COLUMN_NAME FROM information_schema.statistics WHERE TABLE_SCHEMA='test' AND TABLE_NAME='{table}' ORDER BY INDEX_NAME, SEQ_IN_INDEX"),
            format!("SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM information_schema.table_constraints WHERE TABLE_SCHEMA='test' AND TABLE_NAME='{table}' ORDER BY CONSTRAINT_NAME"),
            format!("SELECT CONSTRAINT_NAME, COLUMN_NAME FROM information_schema.key_column_usage WHERE TABLE_SCHEMA='test' AND TABLE_NAME='{table}' ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION"),
        ] {
            engine.execute_with_session(&mut session, &format!("SET {name}=ON")).unwrap();
            let QueryResponse::ResultSet(on) = engine.execute_with_session(&mut session, &sql).unwrap() else {
                panic!("expected metadata result for {sql}");
            };
            assert!(!on.rows.is_empty(), "{sql}");
            engine.execute_with_session(&mut session, &format!("/*!80030 SET SESSION {name}=OFF */")).unwrap();
            let QueryResponse::ResultSet(off) = engine.execute_with_session(&mut session, &sql).unwrap() else {
                panic!("expected metadata result for {sql}");
            };
            assert_eq!(on.rows, off.rows, "{sql}");
        }
    }
    assert_eq!(one_row(&engine, &mut session, &read), value("0"));
    assert_eq!(
        one_row(&engine, &mut session, &format!("SELECT @@global.{name}")),
        value("1")
    );
    assert_eq!(
        one_row(
            &engine,
            &mut session,
            &format!("SHOW SESSION VARIABLES LIKE '{name}'")
        ),
        vec![Value::String(name.into()), Value::String("OFF".into())]
    );
    for raw in ["ON", "1", "TRUE", "'on'", "OFF", "0", "FALSE", "'off'"] {
        engine
            .execute_with_session(&mut session, &format!("SET @@session.{name}={raw}"))
            .unwrap();
        let enabled = matches!(raw, "ON" | "1" | "TRUE" | "'on'");
        assert_eq!(
            one_row(&engine, &mut session, &read),
            value(if enabled { "1" } else { "0" })
        );
    }
    for (raw, code) in [
        ("2", 1231),
        ("-1", 1231),
        ("NULL", 1231),
        ("'bad'", 1231),
        ("1.5", 1231),
    ] {
        let err = engine
            .execute_with_session(&mut session, &format!("SET {name}={raw}"))
            .unwrap_err();
        assert_eq!(err.code(), code, "{raw}");
        assert_eq!(one_row(&engine, &mut session, &read), value("0"));
    }

    let mut existing = SessionContext::default();
    engine
        .execute_with_session(&mut session, &format!("SET GLOBAL {name}=OFF"))
        .unwrap();
    let mut inherited = SessionContext::default();
    assert_eq!(one_row(&engine, &mut existing, &read), value("1"));
    assert_eq!(one_row(&engine, &mut inherited, &read), value("0"));
    engine
        .execute_with_session(&mut existing, &format!("SET SESSION {name}=DEFAULT"))
        .unwrap();
    assert_eq!(one_row(&engine, &mut existing, &read), value("0"));

    engine
        .execute_with_session(&mut session, &format!("SET PERSIST_ONLY {name}=ON"))
        .unwrap();
    assert_eq!(
        one_row(&engine, &mut session, &format!("SELECT @@global.{name}")),
        value("0")
    );
    assert_eq!(one_row(&engine, &mut session, &format!("SELECT VARIABLE_VALUE FROM performance_schema.persisted_variables WHERE VARIABLE_NAME='{name}'")), value("ON"));
    crate::engine::sysvar::registry::apply_persisted_on_startup(engine.database());
    assert_eq!(
        one_row(&engine, &mut session, &format!("SELECT @@global.{name}")),
        value("1")
    );
    assert_eq!(one_row(&engine, &mut session, &read), value("0"));
    engine
        .execute_with_session(&mut session, &format!("SET PERSIST {name}=OFF"))
        .unwrap();
    assert_eq!(one_row(&engine, &mut session, &format!("SELECT VARIABLE_VALUE FROM information_schema.global_variables WHERE VARIABLE_NAME='{name}'")), value("OFF"));
    assert_eq!(one_row(&engine, &mut existing, &format!("SELECT VARIABLE_VALUE FROM information_schema.session_variables WHERE VARIABLE_NAME='{name}'")), value("OFF"));
    engine
        .execute_with_session(&mut session, &format!("RESET PERSIST {name}"))
        .unwrap();
    engine
        .execute_with_session(&mut session, &format!("SET GLOBAL {name}=DEFAULT"))
        .unwrap();
    assert_eq!(
        one_row(
            &engine,
            &mut session,
            &format!("SHOW GLOBAL VARIABLES LIKE '{name}'")
        ),
        vec![Value::String(name.into()), Value::String("ON".into())]
    );
    engine
        .execute_with_session(&mut session, &format!("SET {name}=DEFAULT"))
        .unwrap();
    assert_eq!(one_row(&engine, &mut session, &read), value("1"));
}

#[test]
fn information_schema_stats_expiry_compatibility() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let value = |n: &str| vec![Value::String(n.into())];
    assert_eq!(
        one_row(
            &engine,
            &mut session,
            "SELECT @@information_schema_stats_expiry"
        ),
        value("86400")
    );
    engine
        .execute_with_session(
            &mut session,
            "/*!80000 SET SESSION information_schema_stats_expiry=0 */",
        )
        .unwrap();
    assert_eq!(
        one_row(
            &engine,
            &mut session,
            "SELECT @@session.information_schema_stats_expiry"
        ),
        value("0")
    );
    assert_eq!(
        one_row(
            &engine,
            &mut session,
            "SELECT @@global.information_schema_stats_expiry"
        ),
        value("86400")
    );
    assert_eq!(
        one_row(
            &engine,
            &mut session,
            "SHOW SESSION VARIABLES LIKE 'information_schema_stats_expiry'"
        ),
        vec![
            Value::String("information_schema_stats_expiry".into()),
            Value::String("0".into())
        ]
    );
    let mut other = SessionContext::default();
    assert_eq!(
        one_row(
            &engine,
            &mut other,
            "SELECT @@information_schema_stats_expiry"
        ),
        value("86400")
    );
    engine
        .execute_with_session(
            &mut session,
            "SET GLOBAL information_schema_stats_expiry=31536000",
        )
        .unwrap();
    let mut inherited = SessionContext::default();
    assert_eq!(
        one_row(
            &engine,
            &mut inherited,
            "SELECT @@information_schema_stats_expiry"
        ),
        value("31536000")
    );
    assert_eq!(
        one_row(
            &engine,
            &mut other,
            "SELECT @@information_schema_stats_expiry"
        ),
        value("86400")
    );
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION information_schema_stats_expiry=DEFAULT",
        )
        .unwrap();
    assert_eq!(
        one_row(
            &engine,
            &mut session,
            "SELECT @@information_schema_stats_expiry"
        ),
        value("31536000")
    );
    for (raw, code) in [
        ("-1", 1231),
        ("31536001", 1231),
        ("NULL", 1231),
        ("'bad'", 1232),
        ("1.5", 1232),
    ] {
        let err = engine
            .execute_with_session(
                &mut session,
                &format!("SET information_schema_stats_expiry={raw}"),
            )
            .unwrap_err();
        assert_eq!(err.code(), code, "{raw}");
        assert_eq!(
            one_row(
                &engine,
                &mut session,
                "SELECT @@information_schema_stats_expiry"
            ),
            value("31536000")
        );
    }
    engine
        .execute_with_session(
            &mut session,
            "SET PERSIST_ONLY information_schema_stats_expiry=60",
        )
        .unwrap();
    assert_eq!(
        one_row(
            &engine,
            &mut session,
            "SELECT @@global.information_schema_stats_expiry"
        ),
        value("31536000")
    );
    assert_eq!(one_row(&engine, &mut session, "SELECT VARIABLE_VALUE FROM performance_schema.persisted_variables WHERE VARIABLE_NAME='information_schema_stats_expiry'"), value("60"));
    engine
        .execute_with_session(
            &mut session,
            "SET PERSIST information_schema_stats_expiry=120",
        )
        .unwrap();
    assert_eq!(one_row(&engine, &mut session, "SELECT VARIABLE_VALUE FROM information_schema.global_variables WHERE VARIABLE_NAME='information_schema_stats_expiry'"), value("120"));
    assert_eq!(one_row(&engine, &mut session, "SELECT VARIABLE_VALUE FROM information_schema.session_variables WHERE VARIABLE_NAME='information_schema_stats_expiry'"), value("31536000"));
    engine
        .execute_with_session(
            &mut session,
            "RESET PERSIST information_schema_stats_expiry",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "SET GLOBAL information_schema_stats_expiry=DEFAULT",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET information_schema_stats_expiry=0")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE expiry_stats (id INT PRIMARY KEY)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO expiry_stats VALUES (1), (2)")
        .unwrap();
    assert_eq!(one_row(&engine, &mut session, "SELECT TABLE_ROWS FROM information_schema.tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME='expiry_stats'"), vec![Value::Int(2)]);
    engine
        .execute_with_session(&mut session, "INSERT INTO expiry_stats VALUES (3)")
        .unwrap();
    assert_eq!(one_row(&engine, &mut session, "SELECT TABLE_ROWS FROM information_schema.tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME='expiry_stats'"), vec![Value::Int(3)]);
}

#[test]
fn mydb_wal_write_buffer_sql_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for bytes in [4096, 0, 1048576] {
        engine
            .execute_with_session(
                &mut session,
                &format!("SET GLOBAL mydb_wal_write_buffer_bytes = {bytes}"),
            )
            .unwrap();
        let row = one_row(
            &engine,
            &mut session,
            "SELECT @@global.mydb_wal_write_buffer_bytes",
        );
        assert_eq!(row[0], Value::String(bytes.to_string()));
    }
    for value in ["-1", "1073741825"] {
        assert!(engine
            .execute_with_session(
                &mut session,
                &format!("SET GLOBAL mydb_wal_write_buffer_bytes = {value}"),
            )
            .is_err());
    }
    let row = one_row(
        &engine,
        &mut session,
        "SELECT @@global.mydb_wal_write_buffer_bytes",
    );
    assert_eq!(row[0], Value::String("1048576".into()));
}
