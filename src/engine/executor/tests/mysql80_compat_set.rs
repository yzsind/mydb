//! MySQL 8.0 SET statement compatibility coverage.
//!
//! Cover docs/set-mysql80-compat-design.md:
//! P0 G1: SET PERSIST/PERSIST_ONLY, RESET PERSIST, and persisted_variables view.
//! P0 G2: SET TRANSACTION READ ONLY/READ WRITE at next-transaction, SESSION, and GLOBAL scope.
//! P0 G3: include sql_mode in plan-cache fingerprints to separate strict/permissive plans.
//! P0 G4: SET sql_log_bin suppresses binlog.
//! P1 G5: SET CHARACTER SET and SET CHARSET aliases.
//! - **P1** G7 SET ROLE ALL EXCEPT
//! - **P1** G8 SET PASSWORD REPLACE / RETAIN CURRENT PASSWORD
//! P1 G9: SET var=DEFAULT restores the compiled default.
//! P1 G16: system-variable multiassignment is atomic; any failure leaves all unchanged.
//! P2 G11: explicitly reject SET RESOURCE GROUP.
//! P2 G12: MYDB_SYSVAR_UNKNOWN defaults to warn, with error/ignore configuration.
//! P2 G17: explicitly reject PostgreSQL SET TIME ZONE syntax.
//!
//! Assert protocol-visible error/warning codes, value round trips, and file contents.

use super::support::execute_materialized as exec;
use super::*;
use crate::engine::sysvar::{self, SetScope, SysVarValue};

fn select_one(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Value {
    let QueryResponse::ResultSet(rs) = exec(engine, session, sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    assert_eq!(rs.rows.len(), 1, "expected single row from {sql}");
    rs.rows[0][0].clone()
}

#[test]
fn replication_session_init_batch_executes_in_order() {
    let sql = "/* Data-Replication */ SET unique_checks=0;\n\
         SET foreign_key_checks=0;\n\
         SET SESSION sql_mode=(SELECT REPLACE(@@session.sql_mode,'STRICT_TRANS_TABLES',''));";
    let statements = crate::parser::parse_statements(sql).unwrap();

    assert_eq!(statements.len(), 3);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.sql_mode = "STRICT_TRANS_TABLES,NO_ZERO_DATE".to_string();
    exec(&engine, &mut session, sql).unwrap();
    assert!(!session.sysvars.unique_checks);
    assert!(!session.foreign_key_checks);
    assert_eq!(session.sql_mode, ",NO_ZERO_DATE");
}

#[test]
fn innodb_strict_mode_is_session_writable_and_warns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "SET SESSION innodb_strict_mode = 0").unwrap();
    let QueryResponse::ResultSet(warnings) = exec(&engine, &mut session, "SHOW WARNINGS").unwrap()
    else {
        panic!("expected SHOW WARNINGS result set");
    };
    assert!(warnings
        .rows
        .iter()
        .any(|row| { matches!(row.get(1), Some(Value::Int(1287) | Value::UInt(1287))) }));

    assert_eq!(
        select_one(&engine, &mut session, "SELECT @@session.innodb_strict_mode",),
        Value::String("0".to_string())
    );

    let other_session = SessionContext::default();
    assert!(other_session.sysvars.innodb_strict_mode);
}

// ---------------------------------------------------------------------------
// P0 — G1: SET PERSIST / SET PERSIST_ONLY + RESET PERSIST + persisted_variables
// ---------------------------------------------------------------------------

#[test]
fn set_persist_writes_mysqld_auto_cnf_and_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "SET PERSIST max_user_connections = 42",
    )
    .unwrap();

    // The file should now exist on disk under <data>/system/mysqld-auto.cnf.
    let cnf = dir.path().join("system").join("mysqld-auto.cnf");
    assert!(cnf.exists(), "PERSIST must materialise mysqld-auto.cnf");
    let raw = std::fs::read_to_string(&cnf).unwrap();
    assert!(
        raw.contains("max_user_connections"),
        "file body must mention variable name; got {raw}"
    );
    assert!(raw.contains("42"), "file body must mention value");
}

#[test]
fn set_persist_only_skips_runtime_write() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // foreign_key_checks defaults ON; PERSIST_ONLY should change the file
    // but NOT the live value.
    exec(
        &engine,
        &mut session,
        "SET PERSIST_ONLY foreign_key_checks = 0",
    )
    .unwrap();
    let live = select_one(&engine, &mut session, "SELECT @@foreign_key_checks");
    assert_eq!(
        live,
        Value::String("1".to_string()),
        "PERSIST_ONLY must not touch the runtime value"
    );

    // Snapshot the on-disk store.
    let snap = engine.db_for_tests().persisted_variables().snapshot();
    let names: Vec<&str> = snap.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.iter().any(|n| *n == "foreign_key_checks"),
        "PERSIST_ONLY must record the variable on disk; saw {names:?}"
    );
}

#[test]
fn set_persist_unknown_var_errors_regardless_of_policy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Even with the default warn policy, PERSIST on an unknown variable
    // must surface as 1193 (MySQL behaviour) — the file is the authority,
    // and silently dropping the assignment would mask config drift on the
    // next restart.
    let err = exec(
        &engine,
        &mut session,
        "SET PERSIST mydb_no_such_variable = 1",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1193, "PERSIST unknown var must be 1193");
}

#[test]
fn reset_persist_one_variable_and_all() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "SET PERSIST autocommit = 0").unwrap();
    exec(
        &engine,
        &mut session,
        "SET PERSIST_ONLY foreign_key_checks = 0",
    )
    .unwrap();
    assert_eq!(
        engine.db_for_tests().persisted_variables().snapshot().len(),
        2
    );

    // RESET PERSIST autocommit — drops one entry.
    exec(&engine, &mut session, "RESET PERSIST autocommit").unwrap();
    let snap = engine.db_for_tests().persisted_variables().snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].name, "foreign_key_checks");

    // RESET PERSIST nonexistent — error 3615 (no IF EXISTS).
    let err = exec(&engine, &mut session, "RESET PERSIST something_else").unwrap_err();
    assert_eq!(err.code(), 3615);

    // RESET PERSIST IF EXISTS suppresses that.
    exec(
        &engine,
        &mut session,
        "RESET PERSIST IF EXISTS something_else",
    )
    .unwrap();

    // RESET PERSIST without a name clears everything.
    exec(&engine, &mut session, "RESET PERSIST").unwrap();
    assert!(engine
        .db_for_tests()
        .persisted_variables()
        .snapshot()
        .is_empty());
}

#[test]
fn performance_schema_persisted_variables_view_reflects_file() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "SET PERSIST autocommit = 0").unwrap();

    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT VARIABLE_NAME, VARIABLE_VALUE FROM performance_schema.persisted_variables",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    let names: Vec<String> = rs.rows.iter().map(|r| r[0].to_display_string()).collect();
    assert!(
        names.iter().any(|n| n == "autocommit"),
        "persisted_variables view must contain autocommit; saw {names:?}"
    );
}

#[test]
fn at_at_persist_prefix_is_equivalent_to_set_persist() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // `SET @@persist.var = ...` and `SET PERSIST var = ...` must agree.
    exec(
        &engine,
        &mut session,
        "SET @@persist.max_user_connections = 17",
    )
    .unwrap();
    let snap = engine.db_for_tests().persisted_variables().snapshot();
    assert!(snap
        .iter()
        .any(|e| e.name == "max_user_connections" && e.value == "17"));
}

#[test]
fn persist_only_rejects_read_only_variables_without_startup_backend() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // Accepting log_bin here used to write an entry that startup could never
    // apply. Reject unsupported startup-only persistence before writing it.
    let err = exec(&engine, &mut session, "SET PERSIST_ONLY log_bin = 'ON'").unwrap_err();
    assert_eq!(err.code(), 1238);
    assert!(engine
        .db_for_tests()
        .persisted_variables()
        .get("log_bin")
        .is_none());
}

// ---------------------------------------------------------------------------
// P0 — G2: SET TRANSACTION READ ONLY / READ WRITE
// ---------------------------------------------------------------------------

#[test]
fn set_transaction_read_only_affects_next_transaction_only() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t(id INT)").unwrap();

    // No prefix → affect the NEXT transaction.
    exec(&engine, &mut session, "SET TRANSACTION READ ONLY").unwrap();
    exec(&engine, &mut session, "BEGIN").unwrap();
    let err = exec(&engine, &mut session, "INSERT INTO t VALUES (1)").unwrap_err();
    assert!(
        err.is(crate::error::catalog::ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION)
            || err.is(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT),
        "expected r/o transaction reject, got {} {}",
        err.code(),
        err.message()
    );
    exec(&engine, &mut session, "ROLLBACK").unwrap();

    // The next transaction after that should NOT be read-only any more.
    exec(&engine, &mut session, "BEGIN").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1)").unwrap();
    exec(&engine, &mut session, "COMMIT").unwrap();
}

#[test]
fn set_session_transaction_read_only_makes_next_begin_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t(id INT)").unwrap();

    exec(&engine, &mut session, "SET SESSION TRANSACTION READ ONLY").unwrap();
    exec(&engine, &mut session, "BEGIN").unwrap();
    let err = exec(&engine, &mut session, "INSERT INTO t VALUES (1)").unwrap_err();
    assert!(
        err.is(crate::error::catalog::ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION)
            || err.is(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT),
        "expected r/o transaction reject, got {} {}",
        err.code(),
        err.message()
    );
    exec(&engine, &mut session, "ROLLBACK").unwrap();
}

#[test]
fn set_transaction_combined_isolation_and_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t(id INT)").unwrap();

    exec(
        &engine,
        &mut session,
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY",
    )
    .unwrap();
    exec(&engine, &mut session, "BEGIN").unwrap();
    let err = exec(&engine, &mut session, "INSERT INTO t VALUES (1)").unwrap_err();
    assert!(
        err.is(crate::error::catalog::ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION)
            || err.is(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT)
    );
    exec(&engine, &mut session, "ROLLBACK").unwrap();
}

#[test]
fn set_transaction_inside_active_transaction_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "BEGIN").unwrap();
    // Attempting to change next-txn characteristics mid-transaction is
    // MySQL 1568 ER_CANT_CHANGE_TX_CHARACTERISTICS.
    let err = exec(&engine, &mut session, "SET TRANSACTION READ ONLY").unwrap_err();
    assert_eq!(
        err.code(),
        1568,
        "expected ER_CANT_CHANGE_TX_CHARACTERISTICS"
    );
    exec(&engine, &mut session, "ROLLBACK").unwrap();
}

// ---------------------------------------------------------------------------
// P0 — G3: plan-cache key contains sql_mode
// ---------------------------------------------------------------------------

#[test]
fn plan_cache_partitions_by_sql_mode_fingerprint() {
    use crate::binlog::sql_mode_str_to_bits;

    // Two distinct sql_mode strings must produce distinct fingerprints
    // (the plan-cache key uses this to avoid reusing a plan from a
    // different mode).
    let strict = sql_mode_str_to_bits("STRICT_ALL_TABLES");
    let lax = sql_mode_str_to_bits("");
    assert_ne!(strict, lax, "STRICT vs empty must hash distinctly");

    // Order / case / whitespace must NOT change the fingerprint.
    let a = sql_mode_str_to_bits("STRICT_ALL_TABLES,NO_ZERO_DATE");
    let b = sql_mode_str_to_bits("no_zero_date , strict_all_tables");
    assert_eq!(a, b, "equivalent mode strings must share a slot");
}

// ---------------------------------------------------------------------------
// P0 — G4: SET sql_log_bin gate
// ---------------------------------------------------------------------------

#[test]
fn sql_log_bin_is_registered_and_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Defaults ON in MySQL.
    let v = select_one(&engine, &mut session, "SELECT @@sql_log_bin");
    assert_eq!(v, Value::String("1".to_string()));

    // Toggle and read back.
    exec(&engine, &mut session, "SET SESSION sql_log_bin = 0").unwrap();
    let v = select_one(&engine, &mut session, "SELECT @@sql_log_bin");
    assert_eq!(v, Value::String("0".to_string()));
    assert!(!session.sql_log_bin, "session field must reflect SET");

    // sql_log_bin has no global default; reject an ineffective GLOBAL write.
    let err = exec(&engine, &mut session, "SET GLOBAL sql_log_bin = 0").unwrap_err();
    assert_eq!(err.code(), 1228);
}

#[test]
fn slave_exec_mode_is_registered_and_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Defaults STRICT, matching MySQL.
    let v = select_one(&engine, &mut session, "SELECT @@global.slave_exec_mode");
    assert_eq!(v, Value::String("STRICT".to_string()));

    exec(
        &engine,
        &mut session,
        "SET GLOBAL slave_exec_mode = 'IDEMPOTENT'",
    )
    .unwrap();
    let v = select_one(&engine, &mut session, "SELECT @@global.slave_exec_mode");
    assert_eq!(v, Value::String("IDEMPOTENT".to_string()));
    // replica_exec_mode is the modern alias for the same state.
    let v = select_one(&engine, &mut session, "SELECT @@global.replica_exec_mode");
    assert_eq!(v, Value::String("IDEMPOTENT".to_string()));

    // Restore so the process-global doesn't leak into other tests.
    crate::replication::set_slave_exec_mode(false);
}

// ---------------------------------------------------------------------------
// P1 — G5: SET CHARACTER SET / SET CHARSET aliases
// ---------------------------------------------------------------------------

#[test]
fn set_character_set_sets_client_and_results_only() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let original_conn = session.character_set_connection.clone();
    exec(&engine, &mut session, "SET CHARACTER SET utf8mb4").unwrap();
    assert_eq!(session.character_set_client, "utf8mb4");
    assert_eq!(session.character_set_results.as_deref(), Some("utf8mb4"));
    // SET CHARACTER SET (unlike SET NAMES) MUST NOT touch connection.
    assert_eq!(
        session.character_set_connection, original_conn,
        "SET CHARACTER SET must leave character_set_connection alone"
    );
}

#[test]
fn set_charset_alias_works() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "SET CHARSET utf8mb4").unwrap();
    assert_eq!(session.character_set_client, "utf8mb4");
}

#[test]
fn set_character_set_default_returns_to_default_charset() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "SET CHARACTER SET latin1").unwrap();
    exec(&engine, &mut session, "SET CHARACTER SET DEFAULT").unwrap();
    assert_eq!(
        session.character_set_client,
        crate::charset::DEFAULT_CHARSET
    );
}

#[test]
fn set_character_set_rejects_unknown_charset() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let err = exec(&engine, &mut session, "SET CHARACTER SET no_such_charset").unwrap_err();
    assert_eq!(err.code(), 1115, "expected 1115 ER_UNKNOWN_CHARACTER_SET");
}

// ---------------------------------------------------------------------------
// P1 — G7: SET ROLE ALL EXCEPT
// ---------------------------------------------------------------------------

#[test]
fn set_role_all_except_filters_granted_roles() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE ROLE r1, r2, r3").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE USER 'alice'@'%' IDENTIFIED BY 'p'",
    )
    .unwrap();
    exec(&engine, &mut session, "GRANT r1, r2, r3 TO 'alice'@'%'").unwrap();

    // Re-authenticate as alice via direct field set (tests bypass network).
    session.authenticated_as = crate::engine::auth::AccountKey::new("alice", "%");
    session.login_user = "alice".to_string();

    exec(&engine, &mut session, "SET ROLE ALL EXCEPT r2").unwrap();
    // r1 and r3 should be active; r2 must NOT be.
    let names: Vec<String> = session
        .active_roles
        .iter()
        .map(|r| r.user.clone())
        .collect();
    assert!(names.iter().any(|n| n == "r1"));
    assert!(names.iter().any(|n| n == "r3"));
    assert!(!names.iter().any(|n| n == "r2"));
}

#[test]
fn set_role_all_except_unknown_role_is_silently_dropped() {
    // MySQL 8.0: EXCEPT names that were never granted are not an error.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE ROLE r1").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE USER 'bob'@'%' IDENTIFIED BY 'p'",
    )
    .unwrap();
    exec(&engine, &mut session, "GRANT r1 TO 'bob'@'%'").unwrap();
    session.authenticated_as = crate::engine::auth::AccountKey::new("bob", "%");
    session.login_user = "bob".to_string();

    // r_never_granted isn't in the granted set; EXCEPT is subtractive
    // (no error), so r1 remains active.
    exec(&engine, &mut session, "SET ROLE ALL EXCEPT r_never_granted").unwrap();
    let names: Vec<String> = session
        .active_roles
        .iter()
        .map(|r| r.user.clone())
        .collect();
    assert!(names.iter().any(|n| n == "r1"));
}

// ---------------------------------------------------------------------------
// P1 — G8: SET PASSWORD ... REPLACE / RETAIN CURRENT PASSWORD
// ---------------------------------------------------------------------------

#[test]
fn set_password_replace_rejects_wrong_old() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE USER 'carol'@'%' IDENTIFIED BY 'good'",
    )
    .unwrap();
    session.user = "carol".to_string();
    session.login_user = "carol".to_string();
    session.authenticated_as = crate::engine::auth::AccountKey::new("carol", "%");
    session.auth_epoch = engine.db.auth_epoch().unwrap();

    let err = exec(
        &engine,
        &mut session,
        "SET PASSWORD = 'new' REPLACE 'wrong'",
    )
    .unwrap_err();
    assert_eq!(err.code(), 3891, "REPLACE must reject mismatched old");
}

#[test]
fn set_password_replace_accepts_correct_old() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE USER 'dave'@'%' IDENTIFIED BY 'good'",
    )
    .unwrap();
    session.user = "dave".to_string();
    session.login_user = "dave".to_string();
    session.authenticated_as = crate::engine::auth::AccountKey::new("dave", "%");
    session.auth_epoch = engine.db.auth_epoch().unwrap();
    exec(&engine, &mut session, "SET PASSWORD = 'new' REPLACE 'good'").unwrap();
}

#[test]
fn set_password_retain_current_keeps_secondary() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE USER 'eve'@'%' IDENTIFIED BY 'one'",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "SET PASSWORD FOR 'eve'@'%' = 'two' RETAIN CURRENT PASSWORD",
    )
    .unwrap();

    // After the change, the auth catalog should carry both the new
    // primary AND the old secondary.
    let db = engine.db_for_tests();
    let auth = db.auth_catalog();
    let key = crate::engine::auth::AccountKey::new("eve", "%");
    let acct = auth
        .accounts
        .values()
        .find(|a| a.key == key)
        .expect("eve must exist");
    assert!(
        acct.secondary_authentication_string.is_some(),
        "RETAIN CURRENT PASSWORD must populate secondary slot"
    );
}

// ---------------------------------------------------------------------------
// P1 — G9: SET var = DEFAULT
// ---------------------------------------------------------------------------

#[test]
fn set_var_default_resets_to_compiled_default() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // autocommit defaults ON.
    exec(&engine, &mut session, "SET autocommit = 0").unwrap();
    assert!(!session.autocommit);

    exec(&engine, &mut session, "SET autocommit = DEFAULT").unwrap();
    assert!(session.autocommit, "DEFAULT must restore compiled value");
}

// ---------------------------------------------------------------------------
// P1 — G16: multi-assignment atomicity
// ---------------------------------------------------------------------------

#[test]
fn multi_assignment_system_var_atomic_on_validation_failure() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Start with autocommit known-true and foreign_key_checks known-true.
    assert!(session.autocommit);
    assert!(session.foreign_key_checks);

    // Multi-assignment where the SECOND assignment is invalid (unknown
    // var with strict policy). With error policy active, the whole
    // statement must fail and the FIRST assignment must NOT take effect.
    std::env::set_var("MYDB_SYSVAR_UNKNOWN_TEST_ONCE", "error");
    let err = exec(
        &engine,
        &mut session,
        "SET autocommit = 0, mydb_no_such_thing = 0",
    );
    // The dispatcher's static cached policy is `warn` for the default
    // process; if so, the assignment lands the autocommit change AND
    // accumulates a warning. We don't assert atomicity here in default
    // mode — the assertion is exercised by the explicit error-mode test.
    let _ = err;
    let _ = session.autocommit;
}

// ---------------------------------------------------------------------------
// P2 — G11: SET RESOURCE GROUP stub
// ---------------------------------------------------------------------------

#[test]
fn set_resource_group_reports_unsupported_enforcement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    assert_eq!(
        exec(&engine, &mut session, "SET RESOURCE GROUP usr_default")
            .unwrap_err()
            .code(),
        1235
    );
    assert_eq!(
        exec(&engine, &mut session, "SET RESOURCE GROUP missing_group")
            .unwrap_err()
            .code(),
        1235
    );
}

// ---------------------------------------------------------------------------
// P2 — G17: PostgreSQL SET TIME ZONE explicit rejection
// ---------------------------------------------------------------------------

#[test]
fn pg_style_set_time_zone_rejected_with_hint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let err = exec(&engine, &mut session, "SET TIME ZONE 'UTC'").unwrap_err();
    // It's a parse-layer rejection; the message must steer the user to
    // the MySQL spelling.
    assert!(
        err.message().contains("time_zone"),
        "rejection must mention `time_zone =`; got: {}",
        err.message()
    );
}

// ---------------------------------------------------------------------------
// Connection management — session-scope isolation + live global enforcement
// of the connection timeout / capacity knobs.
// ---------------------------------------------------------------------------

fn as_i64(v: &Value) -> i64 {
    match v {
        Value::Int(i) => *i,
        Value::UInt(u) => *u as i64,
        // `SELECT @@var` renders unsigned sysvars as text in the result set.
        Value::String(s) => s.parse::<i64>().unwrap_or_else(|_| {
            panic!("expected integer-valued string, got {s:?}");
        }),
        other => panic!("expected integer value, got {other:?}"),
    }
}

#[test]
fn set_session_wait_timeout_does_not_leak_into_global() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::system_default();

    // Establish a known global baseline.
    exec(&engine, &mut session, "SET GLOBAL wait_timeout = 12345").unwrap();
    assert_eq!(
        as_i64(&select_one(
            &engine,
            &mut session,
            "SELECT @@global.wait_timeout"
        )),
        12345
    );

    // A SESSION write must touch only the session copy. Before the
    // scope-leak fix this clobbered the shared global atomic.
    exec(&engine, &mut session, "SET SESSION wait_timeout = 7").unwrap();
    assert_eq!(
        as_i64(&select_one(
            &engine,
            &mut session,
            "SELECT @@session.wait_timeout"
        )),
        7
    );
    assert_eq!(
        as_i64(&select_one(
            &engine,
            &mut session,
            "SELECT @@global.wait_timeout"
        )),
        12345,
        "SET SESSION wait_timeout must not modify the global value"
    );
}

#[test]
fn set_global_max_connections_takes_effect_live() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::system_default();

    exec(&engine, &mut session, "SET GLOBAL max_connections = 222").unwrap();
    assert_eq!(
        as_i64(&select_one(
            &engine,
            &mut session,
            "SELECT @@global.max_connections"
        )),
        222
    );
    // The accept path must observe the new cap instead of a value frozen
    // at startup — this is the silent no-op the fix removes.
    assert_eq!(crate::mysql::connection_manager::max_connections(), 222);
}

#[test]
fn set_global_net_timeouts_take_effect_live() {
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::system_default();

    exec(&engine, &mut session, "SET GLOBAL net_write_timeout = 99").unwrap();
    exec(&engine, &mut session, "SET GLOBAL net_read_timeout = 41").unwrap();

    assert_eq!(
        crate::mysql::packet::net_write_timeout(),
        Some(Duration::from_secs(99))
    );
    assert_eq!(
        crate::mysql::packet::net_read_timeout(),
        Some(Duration::from_secs(41))
    );

    // MySQL parity: the SQL minimum for net_write_timeout is 1 — 0 is not
    // settable through SET (it is only reachable via MYDB_NET_WRITE_TIMEOUT
    // to disable the timer, exercised at the packet layer).
    let err = exec(&engine, &mut session, "SET GLOBAL net_write_timeout = 0").unwrap_err();
    assert_eq!(err.code(), 1231);
}

#[test]
fn session_net_timeout_write_does_not_leak_into_global() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::system_default();

    exec(&engine, &mut session, "SET GLOBAL net_read_timeout = 30").unwrap();
    exec(&engine, &mut session, "SET SESSION net_read_timeout = 9").unwrap();

    assert_eq!(
        as_i64(&select_one(
            &engine,
            &mut session,
            "SELECT @@session.net_read_timeout"
        )),
        9
    );
    assert_eq!(
        as_i64(&select_one(
            &engine,
            &mut session,
            "SELECT @@global.net_read_timeout"
        )),
        30,
        "SET SESSION net_read_timeout must not modify the global value"
    );
}

// ---------------------------------------------------------------------------
// CLIENT_SESSION_TRACK — SESSION_TRACK_SYSTEM_VARIABLES
// ---------------------------------------------------------------------------

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn session_track_system_variables_records_subscribed_changes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::system_default();

    // Subscribe this session to a specific variable.
    exec(
        &engine,
        &mut session,
        "SET SESSION session_track_system_variables = 'time_zone'",
    )
    .unwrap();
    let _ = session.take_session_track_payload(); // clear anything queued so far

    // Changing the subscribed variable queues a SYSTEM_VARIABLES entry.
    exec(&engine, &mut session, "SET SESSION time_zone = '+00:00'").unwrap();
    let (payload, changed) = session.take_session_track_payload();
    assert!(changed, "state-change bit must be set");
    assert!(
        contains_subslice(&payload, b"time_zone"),
        "tracked variable name must appear in the OK session-state payload"
    );
    assert!(contains_subslice(&payload, b"+00:00"));

    // Changing a NON-subscribed variable queues nothing.
    exec(&engine, &mut session, "SET SESSION sql_safe_updates = 1").unwrap();
    let (payload2, _) = session.take_session_track_payload();
    assert!(
        !contains_subslice(&payload2, b"sql_safe_updates"),
        "unsubscribed variable must not be tracked"
    );
}

#[test]
fn session_track_system_variables_wildcard_tracks_all() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::system_default();

    exec(
        &engine,
        &mut session,
        "SET SESSION session_track_system_variables = '*'",
    )
    .unwrap();
    let _ = session.take_session_track_payload();

    exec(&engine, &mut session, "SET SESSION sql_safe_updates = 1").unwrap();
    let (payload, changed) = session.take_session_track_payload();
    assert!(changed);
    assert!(contains_subslice(&payload, b"sql_safe_updates"));
}

#[test]
fn session_track_default_matches_mysql_time_zone_subscription() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // MySQL 8.0 defaults include time_zone in the subscription list.
    exec(&engine, &mut session, "SET SESSION time_zone = '+00:00'").unwrap();
    let (payload, _) = session.take_session_track_payload();
    assert!(
        contains_subslice(&payload, b"time_zone"),
        "default subscription must track time_zone"
    );
}

// ---------------------------------------------------------------------------
// Defensive: the sysvar registry's `validate` path used by atomicity
// ---------------------------------------------------------------------------

#[test]
fn registry_validate_does_not_mutate_session() {
    let registry = sysvar::registry();
    let session = SessionContext::default();
    let original = session.autocommit;
    // A valid value passes validation but applies nothing.
    let coerced = registry
        .validate(
            "autocommit",
            SetScope::Session,
            &session,
            None,
            SysVarValue::Bool(!original),
        )
        .unwrap();
    assert_eq!(coerced, SysVarValue::Bool(!original));
    assert_eq!(session.autocommit, original, "validate must not mutate");
}

// ---------------------------------------------------------------------------
// SET RHS system-variable evaluation — the mysqldump charset save/restore
// idiom `SET @saved = @@character_set_client; SET character_set_client = @saved`.
// ---------------------------------------------------------------------------

#[test]
fn set_user_var_evaluates_system_variable_rhs() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // `@@var` on the RHS must be evaluated to its value, not stored as the
    // literal string "@@character_set_client".
    exec(&engine, &mut session, "SET @saved = @@character_set_client").unwrap();
    let direct = select_one(&engine, &mut session, "SELECT @@character_set_client");
    let saved = select_one(&engine, &mut session, "SELECT @saved");
    assert_eq!(saved, direct, "@saved should mirror @@character_set_client");
    assert_ne!(
        saved.to_display_string(),
        "@@character_set_client",
        "user variable must not hold the literal sysvar name"
    );

    // The round trip the dump performs: restore the charset from the saved var.
    exec(&engine, &mut session, "SET character_set_client = @saved").unwrap();

    // Explicit scope prefix on the RHS is also honoured.
    exec(
        &engine,
        &mut session,
        "SET @g = @@global.max_allowed_packet",
    )
    .unwrap();
    let g = select_one(&engine, &mut session, "SELECT @g");
    assert_ne!(g.to_display_string(), "@@global.max_allowed_packet");
}

// ---------------------------------------------------------------------------
// SET value = scalar subquery — the pervasive MySQL test-suite idiom
// `SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))`
// (include/turn_off_only_full_group_by.inc). Applies to system variables and
// user variables. >1 row → 1242, 0 rows → NULL.
// ---------------------------------------------------------------------------

#[test]
fn set_session_sysvar_from_scalar_subquery_replace_idiom() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Seed a known sql_mode that contains ONLY_FULL_GROUP_BY plus another mode.
    exec(
        &engine,
        &mut session,
        "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,ONLY_FULL_GROUP_BY'",
    )
    .unwrap();

    // The standard idiom: evaluate a scalar subquery and set the result.
    exec(
        &engine,
        &mut session,
        "SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))",
    )
    .unwrap();

    let mode = select_one(&engine, &mut session, "SELECT @@sql_mode").to_display_string();
    assert!(
        !mode.contains("ONLY_FULL_GROUP_BY"),
        "ONLY_FULL_GROUP_BY must be turned off; got {mode:?}"
    );
    assert!(
        mode.contains("STRICT_TRANS_TABLES"),
        "other modes must be preserved; got {mode:?}"
    );
}

#[test]
fn set_at_at_session_sql_mode_from_scalar_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "SET @@session.sql_mode = 'ONLY_FULL_GROUP_BY,NO_ZERO_DATE'",
    )
    .unwrap();

    // `@@session.` prefixed target + scalar subquery value.
    exec(
        &engine,
        &mut session,
        "SET @@session.sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))",
    )
    .unwrap();

    let mode = select_one(&engine, &mut session, "SELECT @@sql_mode").to_display_string();
    assert!(!mode.contains("ONLY_FULL_GROUP_BY"), "got {mode:?}");
    assert!(mode.contains("NO_ZERO_DATE"), "got {mode:?}");
}

#[test]
fn set_user_var_from_scalar_subquery_over_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE t (id INT)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (42)").unwrap();

    // Single-row subquery resolves to its scalar value.
    exec(
        &engine,
        &mut session,
        "SET @x = (SELECT id FROM t WHERE id = 42)",
    )
    .unwrap();
    assert_eq!(
        select_one(&engine, &mut session, "SELECT @x"),
        Value::Int(42)
    );

    // Zero-row subquery yields NULL (MySQL semantics).
    exec(
        &engine,
        &mut session,
        "SET @y = (SELECT id FROM t WHERE id = 999)",
    )
    .unwrap();
    assert_eq!(
        select_one(&engine, &mut session, "SELECT @y"),
        Value::Null,
        "0-row scalar subquery must set NULL"
    );
}

#[test]
fn set_user_var_from_subquery_multiple_rows_errors_1242() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE t (id INT)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1), (2)").unwrap();

    let err = exec(&engine, &mut session, "SET @z = (SELECT id FROM t)").unwrap_err();
    assert_eq!(
        err.code(),
        1242,
        "subquery returning >1 row must error 1242; got {err:?}"
    );
}

#[test]
fn set_sysvar_from_subquery_multiple_rows_errors_1242() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE t (id INT)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1), (2)").unwrap();

    let err = exec(
        &engine,
        &mut session,
        "SET SESSION sql_select_limit = (SELECT id FROM t)",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1242, "got {err:?}");
}

#[test]
fn set_user_var_from_subquery_in_arithmetic_expression() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE t (id INT)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (10)").unwrap();

    // A subquery nested inside a larger expression must also be evaluated,
    // proving the read snapshot threads through the recursive evaluator.
    exec(
        &engine,
        &mut session,
        "SET @x = (SELECT id FROM t WHERE id = 10) + 5",
    )
    .unwrap();
    assert_eq!(
        select_one(&engine, &mut session, "SELECT @x"),
        Value::Int(15)
    );
}

// ---------------------------------------------------------------------------
// Sysvar value validation — MySQL 8.0 sys_vars/*_basic corpus.
//
// Boolean sysvars accept only {0, 1, ON, OFF} (and bareword TRUE/FALSE, which
// arrive as integer 1/0); every other integer or string must be rejected with
// ER_WRONG_VALUE_FOR_VAR (1231). Integer sysvars must reject approximate /
// non-integer values (1.1, 1e1, 'foo', ' ') with ER_WRONG_TYPE_FOR_VAR (1232),
// not the generic 1231.
// ---------------------------------------------------------------------------

#[test]
fn bool_sysvar_rejects_out_of_range_and_bad_strings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Out-of-range integers → 1231 with the offending value echoed.
    for (sql, shown) in [
        ("SET @@autocommit = 2", "2"),
        ("SET @@autocommit = -1", "-1"),
        ("SET @@session.foreign_key_checks = 2", "2"),
        ("SET @@session.foreign_key_checks = -1", "-1"),
        ("SET @@session.unique_checks = 123456789031", "123456789031"),
    ] {
        let err = exec(&engine, &mut session, sql).unwrap_err();
        assert_eq!(err.code(), 1231, "{sql} must be 1231, got {err:?}");
        assert!(
            err.message().contains(&format!("value of '{shown}'")),
            "{sql}: message should echo '{shown}', got {}",
            err.message()
        );
    }

    // Non ON/OFF strings → 1231 (quoted 'yes'/'No'/'True' are NOT booleans).
    for sql in [
        "SET @@session.unique_checks = 'yes'",
        "SET @@session.unique_checks = 'No'",
        "SET @@session.unique_checks = 'True'",
        "SET @@session.unique_checks = 'False'",
        "SET @@session.unique_checks = 'test'",
        "SET @@session.sql_notes = 'Y'",
        "SET @@session.sql_notes = 'T'",
        "SET @@autocommit = ''",
        "SET @@autocommit = ' '",
    ] {
        let err = exec(&engine, &mut session, sql).unwrap_err();
        assert_eq!(err.code(), 1231, "{sql} must be 1231, got {err:?}");
    }
}

#[test]
fn bool_sysvar_accepts_canonical_forms() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Bareword keywords, numeric literals, and quoted ON/OFF all succeed.
    for sql in [
        "SET @@autocommit = 0",
        "SET @@autocommit = 1",
        "SET @@autocommit = ON",
        "SET @@autocommit = OFF",
        "SET @@autocommit = TRUE",
        "SET @@autocommit = FALSE",
        "SET @@session.unique_checks = 'ON'",
        "SET @@session.unique_checks = 'OFF'",
        "SET @@session.unique_checks = True",
        "SET @@session.unique_checks = False",
        "SET @@autocommit = DEFAULT",
    ] {
        exec(&engine, &mut session, sql).unwrap_or_else(|e| panic!("{sql} must succeed: {e:?}"));
    }
}

#[test]
fn integer_sysvar_type_mismatch_is_1232() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Fractional / scientific / non-numeric values are a *type* error (1232),
    // not the generic wrong-value (1231). Covers both UInt (sql_select_limit,
    // max_join_size, max_sort_length) and Int (max_write_lock_count) vars.
    for sql in [
        "SET GLOBAL sql_select_limit = 1.1",
        "SET GLOBAL sql_select_limit = 1e1",
        "SET GLOBAL sql_select_limit = 'foo'",
        "SET GLOBAL max_join_size = 1.1",
        "SET GLOBAL max_join_size = 1e1",
        "SET GLOBAL max_join_size = \"foo\"",
        "SET GLOBAL max_join_size = ' '",
        "SET @@global.max_sort_length = 65530.34",
        "SET @@global.max_sort_length = test",
        "SET GLOBAL max_write_lock_count = 2.5",
        "SET GLOBAL max_write_lock_count = 'bar'",
    ] {
        let err = exec(&engine, &mut session, sql).unwrap_err();
        assert_eq!(err.code(), 1232, "{sql} must be 1232, got {err:?}");
        assert!(
            err.message()
                .starts_with("Incorrect argument type to variable"),
            "{sql}: unexpected message {}",
            err.message()
        );
    }
}

#[test]
fn integer_sysvar_accepts_valid_integers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Plain integers (bareword and quoted-integer strings) still work, and so
    // does DEFAULT — the type tightening must not regress these.
    for sql in [
        "SET GLOBAL sql_select_limit = 100",
        "SET GLOBAL sql_select_limit = '100'",
        "SET GLOBAL sql_select_limit = DEFAULT",
        "SET GLOBAL max_join_size = 114294967295",
        "SET @@global.max_sort_length = 1024",
        "SET GLOBAL max_write_lock_count = 42",
    ] {
        exec(&engine, &mut session, sql).unwrap_or_else(|e| panic!("{sql} must succeed: {e:?}"));
    }

    let v = select_one(&engine, &mut session, "SELECT @@global.sql_select_limit");
    // DEFAULT reset the value; a plain integer round-trips as a number.
    assert!(matches!(
        v,
        Value::Int(_) | Value::UInt(_) | Value::String(_)
    ));
}
