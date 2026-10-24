use super::support::execute_materialized as exec;
use super::*;

#[test]
fn call_procedure_writes_out_parameter_and_returns_result_sets() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE p(IN a INT, OUT b INT)
             BEGIN
               SET b = a + 2;
               SELECT b AS answer;
             END",
    )
    .unwrap();

    let response = exec(&engine, &mut session, "CALL p(40, @out)").unwrap();
    let QueryResponse::MultiResult(results) = response else {
        panic!("expected multi-result CALL response");
    };
    assert!(matches!(results[0], QueryResponse::ResultSet(_)));
    assert_eq!(session.user_variable("out"), Some(&Value::Int(42)));
}

#[test]
fn stored_function_single_return_is_inlined_in_select() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION add_one(x INT) RETURNS INT DETERMINISTIC NO SQL RETURN x + 1",
    )
    .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT add_one(41) AS answer").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(42)]]);
}

#[test]
fn stored_function_compound_body_executes_for_constant_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION clamp_positive(x INT) RETURNS INT
         BEGIN
           IF x > 0 THEN
             RETURN x;
           END IF;
           RETURN 0;
         END",
    )
    .unwrap();

    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT clamp_positive(-4), clamp_positive(9)",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(0), Value::Int(9)]]);
}

#[test]
fn stored_function_dml_is_atomic_with_owning_statement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE routine_effects (id INT PRIMARY KEY, required_value INT NOT NULL)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION write_then_fail() RETURNS INT MODIFIES SQL DATA
         BEGIN
           INSERT INTO routine_effects VALUES (1, 1);
           INSERT INTO routine_effects VALUES (2, NULL);
           RETURN 1;
         END",
    )
    .unwrap();

    let err = exec(&engine, &mut session, "SELECT write_then_fail()").unwrap_err();
    assert!(err.is(crate::error::catalog::ER_BAD_NULL_ERROR), "{err:?}");
    let QueryResponse::ResultSet(rows) = exec(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM routine_effects",
    )
    .unwrap() else {
        panic!("expected count result");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(0)]]);
}

#[test]
fn non_inline_stored_function_with_column_argument_runs_row_by_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE t (v INT)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1), (2), (3)").unwrap();
    // Compound body (not DETERMINISTIC NO SQL) → not inlinable → row-by-row.
    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION plus_one(x INT) RETURNS INT
         BEGIN
           RETURN x + 1;
         END",
    )
    .unwrap();

    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT plus_one(v) FROM t ORDER BY v",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)]
        ]
    );
}

#[test]
fn reads_sql_data_function_executes_per_row_in_all_clauses() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE prices (id INT, price INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO prices VALUES (1, 100), (2, 200), (3, 300)",
    )
    .unwrap();
    exec(&engine, &mut session, "CREATE TABLE tax (rate INT)").unwrap();
    exec(&engine, &mut session, "INSERT INTO tax VALUES (10)").unwrap();
    // READS SQL DATA function whose body queries another table per call.
    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION with_tax(p INT) RETURNS INT READS SQL DATA
         BEGIN
           DECLARE r INT;
           SELECT rate INTO r FROM tax LIMIT 1;
           RETURN p + p * r / 100;
         END",
    )
    .unwrap();

    // projection
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT id, with_tax(price) FROM prices ORDER BY id",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(110)],
            vec![Value::Int(2), Value::Int(220)],
            vec![Value::Int(3), Value::Int(330)],
        ]
    );

    // WHERE
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT id FROM prices WHERE with_tax(price) > 200 ORDER BY id",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);

    // ORDER BY
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT id FROM prices ORDER BY with_tax(price) DESC",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(3)],
            vec![Value::Int(2)],
            vec![Value::Int(1)]
        ]
    );

    // scalar subquery position
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT (SELECT with_tax(price) FROM prices WHERE id = 1)",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(110)]]);
}

#[test]
fn handler_specificity_prefers_errno_over_general_class() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // A 1062-specific handler declared *before* the general SQLEXCEPTION
    // handler must still win when a 1062 error is raised.
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE which_handler(OUT tag INT)
         BEGIN
           DECLARE CONTINUE HANDLER FOR 1062 SET tag = 1062;
           DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET tag = 9999;
           SET tag = 0;
           SIGNAL SQLSTATE '23000' SET MYSQL_ERRNO = 1062;
         END",
    )
    .unwrap();

    exec(&engine, &mut session, "CALL which_handler(@tag)").unwrap();
    assert_eq!(session.user_variable("tag"), Some(&Value::Int(1062)));
}

#[test]
fn continue_handler_can_show_errors_and_resume_after_numeric_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE show_overflow()
         BEGIN
           DECLARE overflow CONDITION FOR SQLSTATE '22003';
           DECLARE CONTINUE HANDLER FOR overflow SHOW ERRORS;
           SET @continued = 0;
           SET @overflowed = 9223372036854775807 + 1;
           SET @continued = 1;
         END",
    )
    .unwrap();

    let response = exec(&engine, &mut session, "CALL show_overflow()").unwrap();
    assert_eq!(
        session.user_variable("continued"),
        Some(&Value::Int(1)),
        "CONTINUE handler must resume after the statement that raised 1690"
    );
    let QueryResponse::MultiResult(results) = response else {
        panic!("SHOW ERRORS in the handler must be returned as a CALL result");
    };
    let Some(QueryResponse::ResultSet(errors)) = results.first() else {
        panic!("expected SHOW ERRORS result set");
    };
    assert_eq!(errors.rows[0][1], Value::Int(1690));
}

#[test]
fn procedure_while_loop_and_locals_execute() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE sum_to(IN n INT, OUT out_sum INT)
         BEGIN
           DECLARE i INT DEFAULT 1;
           DECLARE total INT DEFAULT 0;
           WHILE i <= n DO
             SET total = total + i;
             SET i = i + 1;
           END WHILE;
           SET out_sum = total;
         END",
    )
    .unwrap();

    exec(&engine, &mut session, "CALL sum_to(5, @sum)").unwrap();
    assert_eq!(session.user_variable("sum"), Some(&Value::Int(15)));
}

#[test]
fn cursor_not_found_handler_can_exit_loop() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE t (v INT)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1), (2), (3)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE cursor_sum(OUT out_sum INT)
         BEGIN
           DECLARE done INT DEFAULT 0;
           DECLARE x INT DEFAULT 0;
           DECLARE total INT DEFAULT 0;
           DECLARE c CURSOR FOR SELECT v FROM t ORDER BY v;
           DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
           OPEN c;
           read_loop: LOOP
             FETCH c INTO x;
             IF done THEN
               LEAVE read_loop;
             END IF;
             SET total = total + x;
           END LOOP;
           CLOSE c;
           SET out_sum = total;
         END",
    )
    .unwrap();

    exec(&engine, &mut session, "CALL cursor_sum(@sum)").unwrap();
    assert_eq!(session.user_variable("sum"), Some(&Value::Int(6)));
}

#[test]
fn conditional_ddl_before_labeled_loop_can_run_repeatedly() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE recreate_result()
         BEGIN
           DROP TABLE IF EXISTS routine_result;
           CREATE TABLE routine_result (v INT);
           peeling: LOOP
             INSERT INTO routine_result VALUES (1);
             LEAVE peeling;
           END LOOP peeling;
         END",
    )
    .unwrap();

    exec(&engine, &mut session, "CALL recreate_result()").unwrap();
    exec(&engine, &mut session, "CALL recreate_result()").unwrap();
    let QueryResponse::ResultSet(result) =
        exec(&engine, &mut session, "SELECT v FROM routine_result").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn call_with_leading_comments_executes_procedure() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE commented_call(OUT result INT) SET result = 42",
    )
    .unwrap();

    exec(
        &engine,
        &mut session,
        "-- ============================================================\nCALL commented_call(@result);",
    )
    .unwrap();
    assert_eq!(session.user_variable("result"), Some(&Value::Int(42)));
}

#[test]
fn routine_metadata_is_visible() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE meta_p(IN x INT) COMMENT 'hello' SELECT x",
    )
    .unwrap();

    let QueryResponse::ResultSet(show) = engine.execute("SHOW CREATE PROCEDURE meta_p").unwrap()
    else {
        panic!("expected SHOW CREATE result");
    };
    assert!(show.rows[0][2].to_display_string().contains("meta_p"));

    let QueryResponse::ResultSet(params) = engine
        .execute(
            "SELECT PARAMETER_NAME FROM information_schema.parameters WHERE SPECIFIC_NAME = 'meta_p'",
        )
        .unwrap()
    else {
        panic!("expected parameters result");
    };
    assert_eq!(params.rows, vec![vec![Value::String("x".to_string())]]);
}

#[test]
fn routine_security_definer_and_invoker_are_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();

    exec(&engine, &mut root, "CREATE TABLE secret (v INT)").unwrap();
    exec(&engine, &mut root, "INSERT INTO secret VALUES (7)").unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE USER 'alice'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE PROCEDURE definer_p()
         SQL SECURITY DEFINER
         SELECT CURRENT_USER() AS who, v FROM secret",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE PROCEDURE invoker_p()
         SQL SECURITY INVOKER
         SELECT v FROM secret",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "GRANT EXECUTE ON PROCEDURE test.definer_p TO 'alice'@'%'",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "GRANT EXECUTE ON PROCEDURE test.invoker_p TO 'alice'@'%'",
    )
    .unwrap();

    let mut alice = SessionContext::default();
    alice.user = "alice".to_string();
    alice.login_user = "alice".to_string();
    alice.authenticated_as = crate::engine::auth::AccountKey::new("alice", "%");
    alice.auth_epoch = engine.db.auth_catalog().epoch;

    let QueryResponse::MultiResult(results) =
        exec(&engine, &mut alice, "CALL definer_p()").unwrap()
    else {
        panic!("expected multi-result");
    };
    let QueryResponse::ResultSet(rs) = &results[0] else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![vec![
            Value::String("root@localhost".to_string()),
            Value::Int(7)
        ]]
    );

    let denied = exec(&engine, &mut alice, "CALL invoker_p()").unwrap_err();
    assert_eq!(denied.code(), 1142);
}

#[test]
fn definer_security_requires_definer_execute_privilege() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();

    exec(
        &engine,
        &mut root,
        "CREATE USER 'routine_definer'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE DEFINER='routine_definer'@'%' PROCEDURE foreign_definer() SELECT 1",
    )
    .unwrap();

    let err = exec(&engine, &mut root, "CALL foreign_definer()").unwrap_err();
    assert_eq!(err.code(), 1370);

    exec(
        &engine,
        &mut root,
        "GRANT EXECUTE ON PROCEDURE test.foreign_definer TO 'routine_definer'@'%'",
    )
    .unwrap();
    exec(&engine, &mut root, "CALL foreign_definer()").unwrap();
}

#[test]
fn create_routine_notes_missing_definer_and_rejects_execution() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE DEFINER='missing'@'%' PROCEDURE p() SELECT 1",
    )
    .unwrap();
    assert!(session.warnings.iter().any(|w| w.code() == 1449));
    let err = exec(&engine, &mut session, "CALL p()").unwrap_err();
    assert_eq!(err.code(), 1449);
}

#[test]
fn create_routine_if_not_exists_preserves_existing_definition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE IF NOT EXISTS keep_existing() SET @kept = 1",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE IF NOT EXISTS keep_existing() SET @kept = 2",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL keep_existing()").unwrap();
    assert_eq!(session.user_variable("kept"), Some(&Value::Int(1)));
}

#[test]
fn show_routine_status_like_uses_mysql_wildcards() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE PROCEDURE p_alpha() SELECT 1").unwrap();
    exec(&engine, &mut session, "CREATE PROCEDURE p_beta() SELECT 1").unwrap();

    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SHOW PROCEDURE STATUS LIKE 'p_a%'").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][1], Value::String("p_alpha".to_string()));
}

/// Regression for a JDBC-style routine metadata query. Filtering the wide
/// information_schema.PARAMETERS row built its field lookup before GROUP BY;
/// the aggregate path then appended `$group.group_0` while retaining that
/// stale lookup, so projecting a grouped key failed with 1054
/// "unknown column 'group_0'".
#[test]
fn routine_metadata_left_join_group_concat_keeps_group_keys_visible() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE metadata_p(IN x INT, OUT label VARCHAR(16)) SELECT x",
    )
    .unwrap();

    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT DISTINCT ROUTINE_SCHEMA, ROUTINE_NAME, PARAMS.PARAMETER \
         FROM information_schema.ROUTINES \
         LEFT JOIN ( \
             SELECT SPECIFIC_SCHEMA, SPECIFIC_NAME, \
                    GROUP_CONCAT(CONCAT(DATA_TYPE, ' ', PARAMETER_NAME) \
                                 ORDER BY ORDINAL_POSITION SEPARATOR ', ') PARAMETER, \
                    ROUTINE_TYPE \
             FROM information_schema.PARAMETERS \
             GROUP BY SPECIFIC_SCHEMA, SPECIFIC_NAME, ROUTINE_TYPE \
         ) PARAMS \
           ON ROUTINES.ROUTINE_SCHEMA = PARAMS.SPECIFIC_SCHEMA \
          AND ROUTINES.ROUTINE_NAME = PARAMS.SPECIFIC_NAME \
          AND ROUTINES.ROUTINE_TYPE = PARAMS.ROUTINE_TYPE \
         WHERE ROUTINE_SCHEMA = 'test' \
         ORDER BY ROUTINE_SCHEMA",
    )
    .unwrap() else {
        panic!("expected routine metadata result set");
    };

    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::String("test".to_string()));
    assert_eq!(rs.rows[0][1], Value::String("metadata_p".to_string()));
    assert_eq!(
        rs.rows[0][2],
        Value::String("int x, varchar label".to_string())
    );
}

#[test]
fn show_profiles_and_binlog_events_return_empty_result_set() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // SHOW PROFILES — deprecated in MySQL 5.7+; mydb returns empty result.
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW PROFILES").unwrap() else {
        panic!("expected result set for SHOW PROFILES");
    };
    assert_eq!(rs.rows.len(), 0);
    assert_eq!(rs.columns.len(), 3);
    assert_eq!(rs.columns[0].name, "Query_ID");
    assert_eq!(rs.columns[1].name, "Duration");
    assert_eq!(rs.columns[2].name, "Query");

    // SHOW PROFILE — empty result with the canonical 2-column shape.
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW PROFILE").unwrap() else {
        panic!("expected result set for SHOW PROFILE");
    };
    assert_eq!(rs.rows.len(), 0);
    assert_eq!(rs.columns.len(), 2);
    assert_eq!(rs.columns[0].name, "Status");
    assert_eq!(rs.columns[1].name, "Duration");

    // SHOW BINLOG EVENTS — mydb has no MySQL binlog stream; empty result.
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW BINLOG EVENTS").unwrap()
    else {
        panic!("expected result set for SHOW BINLOG EVENTS");
    };
    assert_eq!(rs.rows.len(), 0);
    assert_eq!(rs.columns.len(), 6);
    assert_eq!(rs.columns[0].name, "Log_name");
    assert_eq!(rs.columns[5].name, "Info");

    // With the LIMIT / FROM / IN modifiers (still empty).
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SHOW BINLOG EVENTS IN 'binlog.000001' FROM 4 LIMIT 10",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 0);
}

#[test]
fn function_return_unsigned_is_preserved_in_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION unsigned_f() RETURNS INT UNSIGNED RETURN 1",
    )
    .unwrap();

    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT DTD_IDENTIFIER FROM information_schema.parameters WHERE SPECIFIC_NAME = 'unsigned_f' AND ORDINAL_POSITION = 0",
    )
    .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(
        rs.rows[0][0].to_display_string().to_ascii_uppercase(),
        "INT UNSIGNED"
    );
}

#[test]
fn signal_handlers_and_resignal_work() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE handled_signal(OUT out_v INT)
         BEGIN
           DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' SET out_v = 5;
           SET out_v = 0;
           SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'handled', MYSQL_ERRNO = 1701;
           SET out_v = out_v + 1;
         END",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL handled_signal(@v)").unwrap();
    assert_eq!(session.user_variable("v"), Some(&Value::Int(6)));

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE rethrow_signal()
         BEGIN
           DECLARE CONTINUE HANDLER FOR SQLEXCEPTION RESIGNAL;
           SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'boom', MYSQL_ERRNO = 1702;
         END",
    )
    .unwrap();
    let err = exec(&engine, &mut session, "CALL rethrow_signal()").unwrap_err();
    assert_eq!(err.code(), 1702);
    assert_eq!(err.message(), "boom");

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE signal_values(IN msg VARCHAR(64), IN errno INT)
         SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = msg, MYSQL_ERRNO = errno",
    )
    .unwrap();
    let err = exec(
        &engine,
        &mut session,
        "CALL signal_values('from parameter', 1703)",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1703);
    assert_eq!(err.message(), "from parameter");

    exec(&engine, &mut session, "SET @resignal_errno = 1704").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE rewrite_signal(IN rewritten VARCHAR(64))
         BEGIN
           DECLARE EXIT HANDLER FOR SQLEXCEPTION
             RESIGNAL SQLSTATE '45001'
               SET MESSAGE_TEXT = rewritten, MYSQL_ERRNO = @resignal_errno;
           SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'original', MYSQL_ERRNO = 1705;
         END",
    )
    .unwrap();
    let err = exec(
        &engine,
        &mut session,
        "CALL rewrite_signal('rewritten by handler')",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1704);
    assert_eq!(err.sql_state().as_ref(), "45001");
    assert_eq!(err.message(), "rewritten by handler");

    let err = exec(&engine, &mut session, "CALL signal_values('bad errno', 0)").unwrap_err();
    assert_eq!(err.code(), 1231);
}

#[test]
fn function_restrictions_reject_missing_return_and_dynamic_sql() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let err = exec(
        &engine,
        &mut session,
        "CREATE FUNCTION no_return() RETURNS INT BEGIN SET @x = 1; END",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1415);

    let err = exec(
        &engine,
        &mut session,
        "CREATE FUNCTION dyn_f() RETURNS INT
         BEGIN
           PREPARE s FROM 'SELECT 1';
           RETURN 1;
         END",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1336);
}

#[test]
fn routine_captures_and_replays_sql_mode() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "SET sql_mode = 'PIPES_AS_CONCAT'").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION concat_pipes() RETURNS VARCHAR(16)
         BEGIN
           RETURN 'a' || 'b';
         END",
    )
    .unwrap();

    // Metadata reflects the capture mode.
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT SQL_MODE FROM information_schema.routines WHERE ROUTINE_NAME = 'concat_pipes'",
        )
        .unwrap()
    else {
        panic!("expected routines result");
    };
    assert_eq!(
        rs.rows,
        vec![vec![Value::String("PIPES_AS_CONCAT".to_string())]]
    );

    // Behavior is replayed even after the session mode changes: `||` stays
    // string concatenation, not logical OR.
    exec(&engine, &mut session, "SET sql_mode = ''").unwrap();
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT concat_pipes()").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::String("ab".to_string())]]);
}

#[test]
fn routine_parameter_coercion_uses_invoker_mode_then_body_uses_captured_mode() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "SET sql_mode = 'ANSI'").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE lenient_param(IN value VARCHAR(2), OUT result VARCHAR(2))
         SET result = CONCAT(value, 'bcd')",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "SET sql_mode = 'STRICT_TRANS_TABLES'",
    )
    .unwrap();
    let err = exec(&engine, &mut session, "CALL lenient_param('abcd', @result)").unwrap_err();
    assert_eq!(err.code(), 1406);
    assert_eq!(session.sql_mode, "STRICT_TRANS_TABLES");

    // Once argument assignment succeeds, body/local/OUT coercion uses the
    // mode captured by CREATE/ALTER. This procedure was created non-strict,
    // so an overlong local assignment is truncated even for a strict caller.
    exec(&engine, &mut session, "CALL lenient_param('a', @result)").unwrap();
    assert_eq!(
        session.user_variable("result"),
        Some(&Value::String("ab".to_string()))
    );
    assert_eq!(session.sql_mode, "STRICT_TRANS_TABLES");

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE strict_param(IN value VARCHAR(2)) SET @seen = value",
    )
    .unwrap();
    exec(&engine, &mut session, "SET sql_mode = 'ANSI'").unwrap();
    exec(&engine, &mut session, "CALL strict_param('abcd')").unwrap();
    assert_eq!(
        session.user_variable("seen"),
        Some(&Value::String("ab".to_string()))
    );
    assert_eq!(session.sql_mode, "ANSI");
}

#[test]
fn procedure_recursion_respects_max_sp_recursion_depth() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE rec(IN n INT)
         BEGIN
           IF n > 0 THEN
             CALL rec(n - 1);
           END IF;
         END",
    )
    .unwrap();

    // Default depth 0 → any self-recursion is rejected with 1456.
    let err = exec(&engine, &mut session, "CALL rec(1)").unwrap_err();
    assert_eq!(err.code(), 1456);
    // Non-recursive call still works.
    exec(&engine, &mut session, "CALL rec(0)").unwrap();

    // Raising the limit allows deeper recursion but still caps it.
    exec(&engine, &mut session, "SET max_sp_recursion_depth = 5").unwrap();
    exec(&engine, &mut session, "CALL rec(3)").unwrap();
    let err = exec(&engine, &mut session, "CALL rec(6)").unwrap_err();
    assert_eq!(err.code(), 1456);
}

#[test]
fn stored_function_recursion_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION selfref(n INT) RETURNS INT
         BEGIN
           IF n <= 0 THEN
             RETURN 0;
           END IF;
           RETURN selfref(n - 1);
         END",
    )
    .unwrap();

    let err = exec(&engine, &mut session, "SELECT selfref(1)").unwrap_err();
    assert_eq!(err.code(), 1424);
}

#[test]
fn create_routine_with_foreign_definer_requires_set_user_id() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();

    exec(
        &engine,
        &mut root,
        "CREATE USER 'alice'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "GRANT CREATE ROUTINE ON test.* TO 'alice'@'%'",
    )
    .unwrap();

    let mut alice = SessionContext::default();
    alice.user = "alice".to_string();
    alice.login_user = "alice".to_string();
    alice.authenticated_as = crate::engine::auth::AccountKey::new("alice", "%");
    alice.auth_epoch = engine.db.auth_catalog().epoch;

    // alice may create routines but cannot set a foreign DEFINER.
    let err = exec(
        &engine,
        &mut alice,
        "CREATE DEFINER = 'bob'@'%' PROCEDURE alien() SELECT 1",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1227);

    // Her own account as DEFINER is fine.
    exec(
        &engine,
        &mut alice,
        "CREATE DEFINER = 'alice'@'%' PROCEDURE mine() SELECT 1",
    )
    .unwrap();
}

#[test]
fn get_diagnostics_reads_condition_and_row_count() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE t (v INT)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1), (2), (3)").unwrap();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE diag(OUT errno INT, OUT sqlstate_out VARCHAR(8), OUT msg VARCHAR(64), OUT rc INT)
         BEGIN
           DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
           BEGIN
             GET DIAGNOSTICS CONDITION 1
               errno = MYSQL_ERRNO, sqlstate_out = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
           END;
           UPDATE t SET v = v + 1;
           GET DIAGNOSTICS rc = ROW_COUNT;
           SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'boom', MYSQL_ERRNO = 1234;
         END",
    )
    .unwrap();

    exec(&engine, &mut session, "CALL diag(@e, @s, @m, @rc)").unwrap();
    assert_eq!(session.user_variable("e"), Some(&Value::Int(1234)));
    assert_eq!(
        session.user_variable("s"),
        Some(&Value::String("45000".to_string()))
    );
    assert_eq!(
        session.user_variable("m"),
        Some(&Value::String("boom".to_string()))
    );
    assert_eq!(session.user_variable("rc"), Some(&Value::Int(3)));
}

#[test]
fn call_with_literal_out_argument_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE outp(OUT b INT) SET b = 1",
    )
    .unwrap();

    let err = exec(&engine, &mut session, "CALL outp(1)").unwrap_err();
    assert_eq!(err.code(), 1414);
}

#[test]
fn alter_routine_updates_only_explicit_characteristics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "SET sql_mode = 'PIPES_AS_CONCAT'").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE alter_meta()
         DETERMINISTIC NO SQL SQL SECURITY INVOKER COMMENT 'before'
         SET @x = 1",
    )
    .unwrap();
    exec(&engine, &mut session, "SET sql_mode = ''").unwrap();
    exec(
        &engine,
        &mut session,
        "ALTER PROCEDURE alter_meta COMMENT 'after'",
    )
    .unwrap();

    let routine = engine
        .db
        .routine_definition(
            "test",
            crate::storage::RoutineKindSchema::Procedure,
            "alter_meta",
        )
        .unwrap();
    assert!(routine.deterministic);
    assert_eq!(
        routine.sql_data_access,
        crate::storage::RoutineSqlDataAccessSchema::NoSql
    );
    assert_eq!(
        routine.security_type,
        crate::storage::RoutineSecuritySchema::Invoker
    );
    assert_eq!(routine.comment, "after");
    assert_eq!(routine.sql_mode, "");
}

#[test]
fn non_deterministic_zero_arg_function_executes_once_per_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE fn_rows (v INT)").unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO fn_rows VALUES (1), (2), (3)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION next_counter() RETURNS INT
         BEGIN
           SET @routine_counter = COALESCE(@routine_counter, 0) + 1;
           RETURN @routine_counter;
         END",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT next_counter() FROM fn_rows ORDER BY v",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
}

#[test]
fn procedure_return_and_non_sql_language_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let err = exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE bad_return() RETURN 1",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1313);
    assert!(exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE bad_language() LANGUAGE JAVASCRIPT SELECT 1",
    )
    .is_err());
}

#[test]
fn nested_block_local_shadowing_restores_outer_binding() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE scoped(OUT result INT)
         BEGIN
           DECLARE x INT DEFAULT 1;
           BEGIN
             DECLARE x INT DEFAULT 2;
             SET x = x + 1;
           END;
           SET result = x;
         END",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL scoped(@result)").unwrap();
    assert_eq!(session.user_variable("result"), Some(&Value::Int(1)));
}

#[test]
fn qualified_routine_runs_in_its_own_database() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE DATABASE routine_a").unwrap();
    exec(&engine, &mut session, "CREATE DATABASE routine_b").unwrap();
    exec(&engine, &mut session, "CREATE TABLE routine_a.t (v INT)").unwrap();
    exec(&engine, &mut session, "CREATE TABLE routine_b.t (v INT)").unwrap();
    exec(&engine, &mut session, "INSERT INTO routine_a.t VALUES (7)").unwrap();
    exec(&engine, &mut session, "INSERT INTO routine_b.t VALUES (9)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE routine_a.read_t(OUT result INT)
         SELECT v INTO result FROM t",
    )
    .unwrap();
    exec(&engine, &mut session, "USE routine_b").unwrap();
    exec(&engine, &mut session, "CALL routine_a.read_t(@result)").unwrap();
    assert_eq!(session.user_variable("result"), Some(&Value::Int(7)));
    assert_eq!(session.current_database, "routine_b");
}

#[test]
fn missing_definer_error_restores_caller_sql_mode() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE USER 'gone'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();
    exec(&engine, &mut session, "SET sql_mode = 'PIPES_AS_CONCAT'").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE DEFINER = 'gone'@'%' FUNCTION gone_f() RETURNS INT RETURN 1",
    )
    .unwrap();
    exec(&engine, &mut session, "DROP USER 'gone'@'%'").unwrap();
    exec(&engine, &mut session, "SET sql_mode = ''").unwrap();
    let err = exec(&engine, &mut session, "SELECT gone_f()").unwrap_err();
    assert_eq!(err.code(), 1449);
    assert_eq!(session.sql_mode, "");
}

#[test]
fn stored_function_requires_execute_privilege_before_evaluation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();

    exec(
        &engine,
        &mut root,
        "CREATE FUNCTION private_f() RETURNS INT RETURN 1",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE USER 'fn_user'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();

    let mut user = SessionContext::default();
    user.user = "fn_user".to_string();
    user.login_user = "fn_user".to_string();
    user.authenticated_as = crate::engine::auth::AccountKey::new("fn_user", "%");
    user.auth_epoch = engine.db.auth_catalog().epoch;
    let err = exec(&engine, &mut user, "SELECT private_f()").unwrap_err();
    assert_eq!(err.code(), 1370);
}

#[test]
fn nested_function_execute_uses_current_definer_or_invoker_identity() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();

    exec(
        &engine,
        &mut root,
        "CREATE FUNCTION nested_private() RETURNS INT RETURN 7",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE PROCEDURE nested_definer(OUT result INT)
         SQL SECURITY DEFINER SET result = nested_private()",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE PROCEDURE nested_invoker(OUT result INT)
         SQL SECURITY INVOKER SET result = nested_private()",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE USER 'nested_user'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "GRANT EXECUTE ON PROCEDURE test.nested_definer TO 'nested_user'@'%'",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "GRANT EXECUTE ON PROCEDURE test.nested_invoker TO 'nested_user'@'%'",
    )
    .unwrap();

    let mut user = SessionContext::default();
    user.user = "nested_user".to_string();
    user.login_user = "nested_user".to_string();
    user.authenticated_as = crate::engine::auth::AccountKey::new("nested_user", "%");
    user.auth_epoch = engine.db.auth_catalog().epoch;
    exec(&engine, &mut user, "CALL nested_definer(@result)").unwrap();
    assert_eq!(user.user_variable("result"), Some(&Value::Int(7)));
    let err = exec(&engine, &mut user, "CALL nested_invoker(@result)").unwrap_err();
    assert_eq!(err.code(), 1370);
}

#[test]
fn function_restrictions_propagate_into_called_procedures() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE dynamic_child()
         BEGIN
           PREPARE child_stmt FROM 'SELECT 1';
           DEALLOCATE PREPARE child_stmt;
         END",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION calls_dynamic_child() RETURNS INT
         BEGIN
           CALL dynamic_child();
           RETURN 1;
         END",
    )
    .unwrap();
    let err = exec(&engine, &mut session, "SELECT calls_dynamic_child()").unwrap_err();
    assert_eq!(err.code(), 1336);

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE result_child() SELECT 1",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION calls_result_child() RETURNS INT
         BEGIN
           CALL result_child();
           RETURN 1;
         END",
    )
    .unwrap();
    let err = exec(&engine, &mut session, "SELECT calls_result_child()").unwrap_err();
    assert_eq!(err.code(), 1415);
}

#[test]
fn automatic_sp_privileges_are_granted_and_removed_with_routine() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    exec(
        &engine,
        &mut root,
        "CREATE USER 'routine_creator'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "GRANT CREATE ROUTINE ON test.* TO 'routine_creator'@'%'",
    )
    .unwrap();

    let mut creator = SessionContext::default();
    creator.user = "routine_creator".to_string();
    creator.login_user = "routine_creator".to_string();
    creator.authenticated_as = crate::engine::auth::AccountKey::new("routine_creator", "%");
    creator.auth_epoch = engine.db.auth_catalog().epoch;
    exec(
        &engine,
        &mut creator,
        "CREATE PROCEDURE auto_granted() SET @auto_grant_called = 1",
    )
    .unwrap();
    creator.auth_epoch = engine.db.auth_catalog().epoch;
    exec(&engine, &mut creator, "CALL auto_granted()").unwrap();
    assert_eq!(
        creator.user_variable("auto_grant_called"),
        Some(&Value::Int(1))
    );
    exec(&engine, &mut creator, "DROP PROCEDURE auto_granted").unwrap();
    assert!(!engine.db.auth_catalog().grants.iter().any(|grant| {
        grant.origin == crate::engine::auth::GrantOrigin::AutomaticRoutine
            && matches!(
                &grant.scope,
                crate::engine::auth::PrivilegeScope::Routine { routine, .. }
                    if routine.eq_ignore_ascii_case("auto_granted")
            )
    }));
}

#[test]
fn routine_metadata_requires_routine_privilege_and_redacts_definition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    exec(&engine, &mut root, "CREATE TABLE visible_table (v INT)").unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE PROCEDURE hidden_metadata() SELECT 42",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE USER 'metadata_user'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "GRANT SELECT ON test.visible_table TO 'metadata_user'@'%'",
    )
    .unwrap();

    let mut user = SessionContext::default();
    user.user = "metadata_user".to_string();
    user.login_user = "metadata_user".to_string();
    user.authenticated_as = crate::engine::auth::AccountKey::new("metadata_user", "%");
    user.auth_epoch = engine.db.auth_catalog().epoch;
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut user,
        "SELECT ROUTINE_NAME FROM information_schema.routines
         WHERE ROUTINE_NAME = 'hidden_metadata'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert!(rs.rows.is_empty());

    exec(
        &engine,
        &mut root,
        "GRANT EXECUTE ON PROCEDURE test.hidden_metadata TO 'metadata_user'@'%'",
    )
    .unwrap();
    user.auth_epoch = engine.db.auth_catalog().epoch;
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut user,
        "SELECT ROUTINE_DEFINITION FROM information_schema.routines
         WHERE ROUTINE_NAME = 'hidden_metadata'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Null]]);

    let QueryResponse::ResultSet(show) =
        exec(&engine, &mut user, "SHOW CREATE PROCEDURE hidden_metadata").unwrap()
    else {
        panic!("expected SHOW CREATE result set");
    };
    assert_eq!(show.rows[0][2], Value::Null);
}

#[test]
fn routine_declaration_labels_case_and_cursor_states_are_validated() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let err = exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE late_declare()
         BEGIN SET @x = 1; DECLARE x INT; END",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1337);

    let err = exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE duplicate_local()
         BEGIN DECLARE x INT; DECLARE x INT; END",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1331);

    let err = exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE missing_label() LEAVE nowhere",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1308);

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE no_case_match()
         CASE 1 WHEN 2 THEN SET @x = 2; END CASE",
    )
    .unwrap();
    let err = exec(&engine, &mut session, "CALL no_case_match()").unwrap_err();
    assert_eq!(err.code(), 1339);

    exec(&engine, &mut session, "CREATE TABLE cursor_state_t (v INT)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE cursor_twice()
         BEGIN
           DECLARE c CURSOR FOR SELECT v FROM cursor_state_t;
           OPEN c;
           OPEN c;
         END",
    )
    .unwrap();
    let err = exec(&engine, &mut session, "CALL cursor_twice()").unwrap_err();
    assert_eq!(err.code(), 1325);

    let err = exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE duplicate_handler()
         BEGIN
           DECLARE CONTINUE HANDLER FOR SQLSTATE '23000' SET @x = 1;
           DECLARE EXIT HANDLER FOR SQLSTATE '23000' SET @x = 2;
           SET @x = 0;
         END",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1413);

    let err = exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE missing_condition()
         BEGIN
           DECLARE CONTINUE HANDLER FOR condition_not_declared SET @x = 1;
           SET @x = 0;
         END",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1319);

    let err = exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE undo_handler()
         BEGIN
           DECLARE UNDO HANDLER FOR SQLEXCEPTION SET @x = 1;
           SET @x = 0;
         END",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1235);

    let err = exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE bad_signal_state() SIGNAL SQLSTATE '00000'",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1407);

    let err = exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE invalid_static_sql() SELEC definitely_not_sql",
    )
    .unwrap_err();
    assert!(
        err.is(crate::error::catalog::ER_PARSE_ERROR)
            || err.is(crate::error::catalog::ER_NOT_SUPPORTED_YET),
        "{err:?}"
    );
}

// ── Boolean type audit (2026-07): the stored-routine expression evaluator
//    must route BETWEEN / IN / IS NULL / LIKE through the shared 3VL SSOT
//    instead of erroring (1054) or mishandling NULL. Values verified against
//    MySQL 8.0. See docs/boolean-type-audit-2026-07.md. ──────────────────────
fn scalar(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Value {
    let QueryResponse::ResultSet(rs) = exec(engine, session, sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    rs.rows
        .into_iter()
        .next()
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}

#[test]
fn routine_3vl_is_null_between_in_and_like() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // IS NULL inside an IF condition (was: 1054 "expression not supported").
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE p_isnull(IN x INT, OUT r INT)
         BEGIN IF x IS NULL THEN SET r = 1; ELSE SET r = 0; END IF; END",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL p_isnull(NULL, @r)").unwrap();
    assert_eq!(scalar(&engine, &mut session, "SELECT @r"), Value::Int(1));
    exec(&engine, &mut session, "CALL p_isnull(5, @r)").unwrap();
    assert_eq!(scalar(&engine, &mut session, "SELECT @r"), Value::Int(0));

    // BETWEEN and IN as SET right-hand expressions (were: 1054).
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE p_btw(IN x INT, OUT r INT)
         BEGIN SET r = (x BETWEEN 1 AND 10); END",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL p_btw(5, @r)").unwrap();
    assert_eq!(scalar(&engine, &mut session, "SELECT @r"), Value::Int(1));
    exec(&engine, &mut session, "CALL p_btw(20, @r)").unwrap();
    assert_eq!(scalar(&engine, &mut session, "SELECT @r"), Value::Int(0));
    // BETWEEN with a NULL bound follows three-valued AND: 20 > 10 ⇒ FALSE.
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE p_btw_null(IN x INT, OUT r INT)
         BEGIN SET r = (x BETWEEN NULL AND 10); END",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL p_btw_null(20, @r)").unwrap();
    assert_eq!(scalar(&engine, &mut session, "SELECT @r"), Value::Int(0));
    exec(&engine, &mut session, "CALL p_btw_null(5, @r)").unwrap();
    assert_eq!(scalar(&engine, &mut session, "SELECT @r"), Value::Null);

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE p_in(IN x INT, OUT r INT)
         BEGIN SET r = (x IN (1, 2, 3)); END",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL p_in(2, @r)").unwrap();
    assert_eq!(scalar(&engine, &mut session, "SELECT @r"), Value::Int(1));
    exec(&engine, &mut session, "CALL p_in(9, @r)").unwrap();
    assert_eq!(scalar(&engine, &mut session, "SELECT @r"), Value::Int(0));
    // NOT IN with a NULL member: no match + NULL present ⇒ NULL.
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE p_notin(IN x INT, OUT r INT)
         BEGIN SET r = (x NOT IN (1, NULL)); END",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL p_notin(9, @r)").unwrap();
    assert_eq!(scalar(&engine, &mut session, "SELECT @r"), Value::Null);

    // LIKE with a NULL operand must be NULL, not 1 (value_to_string(NULL)
    // used to stringify to "NULL" and match the literal 'NULL').
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE p_like(IN x VARCHAR(10), OUT r INT)
         BEGIN SET r = (x LIKE 'NULL'); END",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL p_like(NULL, @r)").unwrap();
    assert_eq!(scalar(&engine, &mut session, "SELECT @r"), Value::Null);
    exec(&engine, &mut session, "CALL p_like('NULL', @r)").unwrap();
    assert_eq!(scalar(&engine, &mut session, "SELECT @r"), Value::Int(1));
}

#[test]
fn routine_sql_binds_locals_in_join_and_nested_query_clauses() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE routine_left (id INT)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE routine_right (id INT, score INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO routine_left VALUES (1), (2)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO routine_right VALUES (1, 5), (2, 20)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE local_in_query(IN minimum_score INT, OUT matches INT)
         BEGIN
           SELECT COUNT(*) INTO matches
             FROM routine_left l
             JOIN routine_right r
               ON r.id = l.id AND r.score >= minimum_score
            WHERE EXISTS (
              SELECT 1 FROM routine_right r2
               WHERE r2.id = l.id AND r2.score >= minimum_score
            );
         END",
    )
    .unwrap();

    exec(&engine, &mut session, "CALL local_in_query(10, @matches)").unwrap();
    assert_eq!(session.user_variable("matches"), Some(&Value::Int(1)));
}

#[test]
fn routine_dynamic_sql_prepares_eagerly_and_is_session_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE prepare_once()
         BEGIN
           PREPARE persistent_stmt FROM 'SELECT ? + 1';
         END",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE execute_later()
         BEGIN
           SET @prepared_arg = 41;
           EXECUTE persistent_stmt USING @prepared_arg;
           DEALLOCATE PREPARE persistent_stmt;
         END",
    )
    .unwrap();

    exec(&engine, &mut session, "CALL prepare_once()").unwrap();
    let QueryResponse::MultiResult(results) =
        exec(&engine, &mut session, "CALL execute_later()").unwrap()
    else {
        panic!("expected multi-result CALL response");
    };
    let QueryResponse::ResultSet(rs) = &results[0] else {
        panic!("expected dynamic SELECT result");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(42)]]);

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE invalid_prepare() PREPARE broken_stmt FROM 'SELEC 1'",
    )
    .unwrap();
    let err = exec(&engine, &mut session, "CALL invalid_prepare()").unwrap_err();
    assert_eq!(err.code(), 1064);

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE missing_deallocate() DEALLOCATE PREPARE absent_stmt",
    )
    .unwrap();
    let err = exec(&engine, &mut session, "CALL missing_deallocate()").unwrap_err();
    assert_eq!(err.code(), 1243);
}

#[test]
fn routine_dynamic_sql_cannot_capture_local_variables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE dynamic_local_scope()
         BEGIN
           DECLARE hidden_value INT DEFAULT 42;
           PREPARE local_stmt FROM 'SELECT hidden_value';
           EXECUTE local_stmt;
         END",
    )
    .unwrap();
    let err = exec(&engine, &mut session, "CALL dynamic_local_scope()").unwrap_err();
    assert_eq!(err.code(), 1054);
}

#[test]
fn routine_concat_preserves_declared_float_scale() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE concat_scaled_local()
         BEGIN
           DECLARE value DOUBLE(10,3);
           SET value = 100;
           SET @rendered = CONCAT('value=', value);
         END",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL concat_scaled_local()").unwrap();

    assert_eq!(
        session.user_variable("rendered"),
        Some(&Value::String("value=100.000".into()))
    );
}

#[test]
fn parsed_const_select_routes_stored_functions_through_full_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION const_route_f(x INT) RETURNS INT RETURN x + 1",
    )
    .unwrap();

    let parsed = engine
        .parse_cached_statement(&session, "SELECT const_route_f(41)")
        .unwrap();
    let response = <StorageQueryEngine as QueryEngine>::execute_parsed(
        &engine,
        &mut session,
        "SELECT const_route_f(41)",
        parsed,
    )
    .and_then(QueryResponse::materialize)
    .unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected stored-function result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(42)]]);
}

#[test]
fn stored_function_cannot_modify_invoking_statement_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE function_self_t(v INT)").unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO function_self_t VALUES (1)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION function_self_write() RETURNS INT MODIFIES SQL DATA
         BEGIN
           UPDATE function_self_t SET v = v + 1;
           RETURN 1;
         END",
    )
    .unwrap();

    let err = exec(
        &engine,
        &mut session,
        "SELECT function_self_write() FROM function_self_t",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1442);
    assert_eq!(
        scalar(&engine, &mut session, "SELECT v FROM function_self_t"),
        Value::Int(1)
    );
}

#[test]
fn routine_expressions_and_set_statements_support_system_variables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE routine_sysvars(OUT captured BIGINT)
         BEGIN
           SET SESSION group_concat_max_len = 2048;
           SET captured = @@group_concat_max_len;
         END",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL routine_sysvars(@captured)").unwrap();
    assert_eq!(session.user_variable("captured"), Some(&Value::Int(2048)));
    assert_eq!(session.group_concat_max_len, 2048);
}

#[test]
fn cursor_fetch_accepts_mysql_next_from_spelling() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE fetch_next_t(v INT)").unwrap();
    exec(&engine, &mut session, "INSERT INTO fetch_next_t VALUES (7)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE fetch_next_p(OUT result INT)
         BEGIN
           DECLARE c CURSOR FOR SELECT v FROM fetch_next_t;
           OPEN c;
           FETCH NEXT FROM c INTO result;
           CLOSE c;
         END",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL fetch_next_p(@result)").unwrap();
    assert_eq!(session.user_variable("result"), Some(&Value::Int(7)));
}

#[test]
fn routine_ddl_validates_headers_and_reports_if_exists_notes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let duplicate = exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE duplicate_params(IN x INT, IN x INT) SELECT x",
    )
    .unwrap_err();
    assert_eq!(duplicate.code(), 1330);

    let long_comment = "x".repeat(65);
    let err = exec(
        &engine,
        &mut session,
        &format!("CREATE PROCEDURE long_comment() COMMENT '{long_comment}' SELECT 1"),
    )
    .unwrap_err();
    assert_eq!(err.code(), 3511);

    exec(
        &engine,
        &mut session,
        "DROP PROCEDURE IF EXISTS missing_routine_note",
    )
    .unwrap();
    let QueryResponse::ResultSet(warnings) = exec(&engine, &mut session, "SHOW WARNINGS").unwrap()
    else {
        panic!("expected SHOW WARNINGS result");
    };
    assert!(warnings
        .rows
        .iter()
        .any(|row| row.get(1) == Some(&Value::Int(1305))));
}

#[test]
fn information_schema_routines_reports_sql_external_language() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE metadata_language() SELECT 1",
    )
    .unwrap();
    assert_eq!(
        scalar(
            &engine,
            &mut session,
            "SELECT external_language FROM information_schema.routines WHERE routine_schema='test' AND routine_name='metadata_language'",
        ),
        Value::String("SQL".into())
    );
}

#[test]
fn native_function_namespace_is_not_captured_by_stored_routine() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    exec(
        &engine,
        &mut root,
        "CREATE FUNCTION abs(x INT) RETURNS INT RETURN 99",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE USER 'native_user'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();

    let mut user = SessionContext::default();
    user.user = "native_user".into();
    user.login_user = "native_user".into();
    user.authenticated_as = crate::engine::auth::AccountKey::new("native_user", "%");
    user.auth_epoch = engine.db.auth_catalog().epoch;
    assert_eq!(
        scalar(
            &engine,
            &mut user,
            "SELECT ABS(v) FROM (SELECT -1 AS v) AS source",
        ),
        Value::Int(1)
    );

    assert_eq!(
        scalar(
            &engine,
            &mut root,
            "SELECT test.abs(v) FROM (SELECT -1 AS v) AS source",
        ),
        Value::Int(99)
    );
}

#[test]
fn sqlwarning_handlers_and_diagnostics_areas_follow_mysql_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "SET sql_mode = ''").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE routine_warning_t(v VARCHAR(1))",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE warning_handler(OUT hit INT, OUT errno INT)
         BEGIN
           DECLARE CONTINUE HANDLER FOR SQLWARNING
           BEGIN
             SET hit = 1;
             GET STACKED DIAGNOSTICS CONDITION 1 errno = MYSQL_ERRNO;
           END;
           SET hit = 0;
           INSERT INTO routine_warning_t VALUES ('ab');
         END",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL warning_handler(@hit, @errno)").unwrap();
    assert_eq!(session.user_variable("hit"), Some(&Value::Int(1)));
    assert_eq!(session.user_variable("errno"), Some(&Value::Int(1265)));

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE invalid_stacked() GET STACKED DIAGNOSTICS @n = NUMBER",
    )
    .unwrap();
    assert_eq!(
        exec(&engine, &mut session, "CALL invalid_stacked()")
            .unwrap_err()
            .code(),
        3004
    );

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE invalid_condition_number()
         GET CURRENT DIAGNOSTICS CONDITION 0 @e = MYSQL_ERRNO",
    )
    .unwrap();
    assert_eq!(
        exec(&engine, &mut session, "CALL invalid_condition_number()")
            .unwrap_err()
            .code(),
        1758
    );
}

#[test]
fn sql_prepared_call_supports_out_and_inout_placeholders() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE prepared_out(IN input INT, OUT output INT, INOUT `both` INT)
         BEGIN
           SET output = input + 1;
           SET `both` = `both` + 2;
         END",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "PREPARE prepared_call FROM 'CALL prepared_out(?, ?, ?)'",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "SET @input = 40, @output = 0, @both = 5",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "EXECUTE prepared_call USING @input, @output, @both",
    )
    .unwrap();
    assert_eq!(session.user_variable("output"), Some(&Value::Int(41)));
    assert_eq!(session.user_variable("both"), Some(&Value::Int(7)));
}

#[test]
fn signal_and_resignal_accept_named_sqlstate_conditions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE named_signal()
         BEGIN
           DECLARE named_failure CONDITION FOR SQLSTATE '45000';
           SIGNAL named_failure SET MESSAGE_TEXT = 'named condition';
         END",
    )
    .unwrap();
    let err = exec(&engine, &mut session, "CALL named_signal()").unwrap_err();
    assert_eq!(err.code(), 1644);
    assert_eq!(err.sql_state().as_ref(), "45000");
    assert_eq!(err.message(), "named condition");

    let bad = exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE bad_named_signal()
         BEGIN
           DECLARE errno_condition CONDITION FOR 1062;
           SIGNAL errno_condition;
         END",
    )
    .unwrap_err();
    assert_eq!(bad.code(), 1646);
}

#[test]
fn static_sql_parameter_slots_preserve_lexical_scope_and_column_resolution() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE routine_slot_t(v INT)").unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO routine_slot_t VALUES (7)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE routine_slots(OUT local_value INT, OUT column_value INT)
         BEGIN
           BEGIN
             DECLARE v INT DEFAULT 9;
             SELECT v FROM routine_slot_t INTO local_value;
           END;
           BEGIN
             SELECT v FROM routine_slot_t INTO column_value;
           END;
         END",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CALL routine_slots(@local_value, @column_value)",
    )
    .unwrap();
    assert_eq!(session.user_variable("local_value"), Some(&Value::Int(9)));
    assert_eq!(session.user_variable("column_value"), Some(&Value::Int(7)));
}

#[test]
fn binary_prepared_call_returns_marked_out_parameter_result_set() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE binary_out(IN input INT, OUT output INT, INOUT `both` INT)
         BEGIN
           SET output = input + 1;
           SET `both` = `both` + 2;
         END",
    )
    .unwrap();
    let mut statement = crate::parser::parse_statement("CALL binary_out(?, ?, ?)").unwrap();
    let mut count = 0;
    crate::parser::number_statement_placeholders(&mut statement, &mut count);
    assert_eq!(count, 3);
    let response = crate::engine::params::with_params(
        vec![Value::Int(40), Value::Null, Value::Int(5)],
        || {
            <StorageQueryEngine as QueryEngine>::execute_prepared_statement(
                &engine,
                &mut session,
                "CALL binary_out(?, ?, ?)",
                statement,
            )
            .expect("storage engine handles prepared CALL")
        },
    )
    .unwrap();
    let QueryResponse::MultiResult(results) = response else {
        panic!("expected prepared CALL multi-result");
    };
    let QueryResponse::ResultSet(out) = &results[results.len() - 2] else {
        panic!("expected OUT parameter result set before final status");
    };
    assert_eq!(out.rows, vec![vec![Value::Int(41), Value::Int(7)]]);
    assert!(out.columns.iter().all(|column| {
        column.source.org_table == crate::engine::executor::routines::PREPARED_CALL_OUT_MARKER
    }));
}

#[test]
fn routine_autocommit_releases_metadata_locks_on_success_and_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.connection_id = 9101;
    for sql in [
        "CREATE FUNCTION mdl_value() RETURNS INT RETURN 42",
        "CREATE PROCEDURE mdl_ok() SELECT mdl_value()",
        "CREATE PROCEDURE mdl_fail() SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='expected'",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    for sql in ["SELECT mdl_value()", "CALL mdl_ok()", "CALL mdl_fail()"] {
        let result = exec(&engine, &mut session, sql);
        assert_eq!(result.is_err(), sql.contains("fail"));
        assert!(session.current_txn.is_none());
        let locks = engine.db_for_tests().metadata_lock_snapshot();
        assert!(
            !locks.iter().any(|lock| lock.owner_thread_id == Some(9101)),
            "autocommit {sql} leaked locks: {locks:?}"
        );
    }
}

#[test]
fn routine_procedure_autocommit_keeps_completed_statements_after_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // An unrelated function must not change CALL transaction semantics.
    for sql in [
        "CREATE TABLE call_effects(id INT PRIMARY KEY)",
        "CREATE FUNCTION unrelated() RETURNS INT RETURN 1",
        "CREATE PROCEDURE partial_call() BEGIN INSERT INTO call_effects VALUES(1); SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='expected'; END",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    assert!(exec(&engine, &mut session, "CALL partial_call()").is_err());
    assert_eq!(
        scalar(&engine, &mut session, "SELECT COUNT(*) FROM call_effects"),
        Value::Int(1)
    );
}

#[test]
fn routine_procedure_can_leave_transaction_open_for_caller() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE call_pending(id INT)",
        "CREATE FUNCTION unrelated() RETURNS INT RETURN 1",
        "CREATE PROCEDURE start_work() BEGIN START TRANSACTION; INSERT INTO call_pending VALUES(1); END",
        "CALL start_work()",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    assert!(
        session.explicit_txn,
        "CALL must retain START TRANSACTION state"
    );
    exec(&engine, &mut session, "ROLLBACK").unwrap();
    assert_eq!(
        scalar(&engine, &mut session, "SELECT COUNT(*) FROM call_pending"),
        Value::Int(0)
    );
}

#[test]
fn routine_procedure_commit_changes_caller_transaction_state() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE call_committed(id INT)",
        "CREATE PROCEDURE finish_work() COMMIT",
        "START TRANSACTION",
        "INSERT INTO call_committed VALUES(1)",
        "CALL finish_work()",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    assert!(!session.explicit_txn, "CALL must retain COMMIT state");
    assert!(session.current_txn.is_none());
}

#[test]
fn routine_do_function_side_effects_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE do_effects(id INT PRIMARY KEY)",
        "CREATE FUNCTION do_failure() RETURNS INT MODIFIES SQL DATA BEGIN INSERT INTO do_effects VALUES(1); SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='expected'; RETURN 1; END",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    assert!(exec(&engine, &mut session, "DO do_failure()").is_err());
    assert_eq!(
        scalar(&engine, &mut session, "SELECT COUNT(*) FROM do_effects"),
        Value::Int(0)
    );
}

#[test]
fn routine_transaction_locks_survive_until_commit_rollback_or_reset() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.connection_id = 9201;
    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION held_fn() RETURNS INT RETURN 7",
    )
    .unwrap();
    for ending in ["COMMIT", "ROLLBACK", "RESET"] {
        exec(&engine, &mut session, "START TRANSACTION").unwrap();
        exec(&engine, &mut session, "SELECT held_fn()").unwrap();
        let locks = engine.db_for_tests().metadata_lock_snapshot();
        assert!(locks.iter().any(|lock| lock.owner_thread_id == Some(9201)
            && lock.object_name.as_deref() == Some("held_fn")
            && lock.lock_duration == "TRANSACTION"));
        if ending == "RESET" {
            engine.reset_session(&mut session).unwrap();
        } else {
            exec(&engine, &mut session, ending).unwrap();
        }
        assert!(!engine
            .db_for_tests()
            .metadata_lock_snapshot()
            .iter()
            .any(|lock| lock.owner_thread_id == Some(9201)));
    }
    let mut ddl_session = SessionContext::default();
    ddl_session.connection_id = 9202;
    exec(&engine, &mut ddl_session, "DROP FUNCTION held_fn").unwrap();
}

#[test]
fn routine_reset_releases_lazy_transaction_metadata_locks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.connection_id = 9203;
    for sql in [
        "CREATE TABLE lazy_mdl(id INT)",
        "SET autocommit=0",
        "SELECT * FROM lazy_mdl",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    assert!(
        session.current_txn.is_none(),
        "exercise storage-transaction-free reset path"
    );
    assert!(!engine.db_for_tests().metadata_lock_snapshot().is_empty());
    engine.close_session(&mut session).unwrap();
    assert!(!engine
        .db_for_tests()
        .metadata_lock_snapshot()
        .iter()
        .any(|lock| lock.owner_thread_id == Some(9203)));
}

#[test]
fn routine_call_retains_definition_locks_across_body_commit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.connection_id = 9204;
    // No functions exist: CALL must still resolve and protect its definition.
    exec(&engine, &mut session, "CREATE PROCEDURE locked_call(OUT n INT) BEGIN SELECT 1; COMMIT; SELECT COUNT(*) INTO n FROM performance_schema.metadata_locks WHERE OBJECT_NAME='locked_call' AND LOCK_STATUS='GRANTED'; END").unwrap();
    exec(&engine, &mut session, "CALL locked_call(@n)").unwrap();
    assert!(matches!(session.user_variable("n"), Some(Value::Int(n)) if *n > 0));
    assert!(session.current_txn.is_none());
    assert!(!engine
        .db_for_tests()
        .metadata_lock_snapshot()
        .iter()
        .any(|lock| lock.owner_thread_id == Some(9204)));
}

#[test]
fn routine_set_and_procedure_expression_functions_rollback_failed_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE expression_effects(id INT PRIMARY KEY)",
        "CREATE FUNCTION expression_fail() RETURNS INT MODIFIES SQL DATA BEGIN INSERT INTO expression_effects VALUES(1); SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='expected'; RETURN 0; END",
        "CREATE PROCEDURE expression_handler(OUT caught INT) BEGIN DECLARE v INT; DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' SET caught=1; SET caught=0; SET v=expression_fail(); END",
    ] { exec(&engine, &mut session, sql).unwrap(); }
    for sql in [
        "SET @v=expression_fail()",
        "DO expression_fail()",
        "SELECT expression_fail() INTO @v",
    ] {
        assert!(exec(&engine, &mut session, sql).is_err(), "{sql}");
        assert_eq!(
            scalar(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM expression_effects"
            ),
            Value::Int(0),
            "{sql}"
        );
    }
    exec(&engine, &mut session, "CALL expression_handler(@caught)").unwrap();
    assert_eq!(session.user_variable("caught"), Some(&Value::Int(1)));
    assert_eq!(
        scalar(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM expression_effects"
        ),
        Value::Int(0)
    );
}

#[test]
fn routine_procedure_subquery_observes_prior_autocommitted_body_write() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE subquery_effects(id INT)",
    )
    .unwrap();
    exec(&engine, &mut session, "CREATE PROCEDURE subquery_call(OUT n INT) BEGIN INSERT INTO subquery_effects VALUES(1); COMMIT; SET n=(SELECT COUNT(*) FROM subquery_effects); END").unwrap();
    exec(&engine, &mut session, "CALL subquery_call(@n)").unwrap();
    assert_eq!(session.user_variable("n"), Some(&Value::Int(1)));
}

#[test]
fn routine_function_result_metadata_uses_declared_type_even_for_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for (declaration, expected) in [
        ("INT", ColumnType::Long),
        ("BIGINT UNSIGNED", ColumnType::LongLong),
        (
            "DECIMAL(12,3)",
            ColumnType::Decimal {
                precision: 12,
                scale: 3,
            },
        ),
        ("DATE", ColumnType::Date),
        ("DATETIME(6)", ColumnType::DateTime),
        ("VARCHAR(20)", ColumnType::VarString),
        ("JSON", ColumnType::Json),
    ] {
        exec(
            &engine,
            &mut session,
            &format!("CREATE FUNCTION metadata_f() RETURNS {declaration} RETURN NULL"),
        )
        .unwrap();
        for query in ["SELECT metadata_f()", "SELECT test.metadata_f() WHERE 1=0"] {
            let QueryResponse::ResultSet(result) = exec(&engine, &mut session, query).unwrap()
            else {
                panic!("expected result set");
            };
            assert_eq!(
                result.columns[0].column_type, expected,
                "{declaration}: {query}"
            );
            if declaration.contains("UNSIGNED") {
                assert_ne!(
                    result.columns[0].source.flags & crate::mysql::protocol::UNSIGNED_FLAG,
                    0
                );
            }
            if declaration == "DATETIME(6)" {
                assert_eq!(result.columns[0].source.decimals, Some(6));
            }
        }
        exec(&engine, &mut session, "DROP FUNCTION metadata_f").unwrap();
    }
}

#[test]
fn routine_binary_prepared_functions_use_full_runtime_and_declared_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE prepared_values(id INT PRIMARY KEY)",
        "INSERT INTO prepared_values VALUES(1)",
        "CREATE FUNCTION prepared_f(x INT) RETURNS INT RETURN x+1",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    for sql in [
        "SELECT prepared_f(?)",
        "SELECT prepared_f(id) FROM prepared_values WHERE id=?",
    ] {
        let columns = <StorageQueryEngine as QueryEngine>::describe_prepared_statement(
            &engine,
            &mut session,
            sql,
        )
        .unwrap();
        assert_eq!(columns[0].column_type, ColumnType::Long);
        let mut parsed = crate::parser::parse_statement(sql).unwrap();
        let mut count = 0;
        crate::parser::number_statement_placeholders(&mut parsed, &mut count);
        let parsed = <StorageQueryEngine as QueryEngine>::prepare_statement_template(
            &engine,
            &mut session,
            sql,
            parsed,
        )
        .unwrap();
        let mut memo = crate::engine::api::PreparedSelectMemoState::default();
        let parsed = Arc::new(parsed);
        assert!(
            <StorageQueryEngine as QueryEngine>::execute_prepared_select(
                &engine,
                &mut session,
                &mut memo,
                &parsed,
                sql,
                &[Value::Int(1)]
            )
            .is_none()
        );
        let response = crate::engine::params::with_params(vec![Value::Int(1)], || {
            <StorageQueryEngine as QueryEngine>::execute_prepared_statement(
                &engine,
                &mut session,
                sql,
                (*parsed).clone(),
            )
            .unwrap()
        })
        .and_then(QueryResponse::materialize)
        .unwrap();
        let QueryResponse::ResultSet(result) = response else {
            panic!("expected rows");
        };
        assert_eq!(result.columns[0].column_type, ColumnType::Long);
        assert_eq!(result.rows, vec![vec![Value::Int(2)]]);
    }
}

#[test]
fn routine_empty_loops_remain_cancellable() {
    for body in [
        "LOOP END LOOP",
        "WHILE 1 DO END WHILE",
        "REPEAT UNTIL 0 END REPEAT",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(open_test_engine(dir.path()));
        let registry = Arc::clone(engine.registry());
        let handle = registry.register(9301, "root".into(), "localhost".into(), "test".into());
        let mut runner = SessionContext::default();
        runner.connection_id = 9301;
        runner.set_cancel_handle(Some(handle.cancel_handle()));
        exec(
            &engine,
            &mut runner,
            &format!("CREATE PROCEDURE empty_loop() {body}"),
        )
        .unwrap();
        let worker_engine = Arc::clone(&engine);
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = exec(&worker_engine, &mut runner, "CALL empty_loop()");
            send.send(result).unwrap();
        });
        std::thread::sleep(Duration::from_millis(50));
        exec(&engine, &mut SessionContext::default(), "KILL QUERY 9301").unwrap();
        let err = receive
            .recv_timeout(Duration::from_secs(5))
            .expect("empty loop ignored cancellation")
            .unwrap_err();
        assert_eq!(err.code(), 1317, "{body}");
        worker.join().unwrap();
        assert!(!engine
            .db_for_tests()
            .metadata_lock_snapshot()
            .iter()
            .any(|lock| lock.owner_thread_id == Some(9301)));
        registry.unregister(9301);
    }
}

#[test]
fn audit_nested_routine_branches_and_case_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION audit_nested(x INT) RETURNS INT
      BEGIN
        IF CASE WHEN x >= 0 THEN 1 ELSE 0 END = 1 THEN
          CASE CASE WHEN x > 0 THEN 1 ELSE 0 END
            WHEN 1 THEN
              IF x > 10 THEN RETURN 11;
              ELSEIF CASE WHEN x = 5 THEN 1 ELSE 0 END THEN
                CASE x WHEN 5 THEN RETURN 5; ELSE RETURN 6; END CASE;
              ELSE RETURN 1; END IF;
            ELSE RETURN 0;
          END CASE;
        ELSEIF x = -1 THEN
          IF x < 0 THEN RETURN -1; ELSE RETURN -2; END IF;
        ELSE RETURN -3; END IF;
      END",
    )
    .unwrap();
    for (input, expected) in [(11, 11), (5, 5), (2, 1), (0, 0), (-1, -1), (-9, -3)] {
        let QueryResponse::ResultSet(rs) = exec(
            &engine,
            &mut session,
            &format!("SELECT audit_nested({input})"),
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(rs.rows, vec![vec![Value::Int(expected)]]);
    }
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE audit_handler(OUT got INT) BEGIN
      DECLARE CONTINUE HANDLER FOR SQLSTATE '45000'
        IF 1 THEN IF 0 THEN SET got=1; ELSE SET got=2; END IF;
        ELSE SET got=3; END IF;
      IF 1 THEN SIGNAL SQLSTATE '45000'; ELSE SET got=4; END IF;
    END",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL audit_handler(@got)").unwrap();
    assert_eq!(session.user_variable("got"), Some(&Value::Int(2)));
}

#[test]
fn audit_routine_tail_comments_and_type_named_variables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE audit_names(OUT got INT) BEGIN
      DECLARE date, time INT DEFAULT 2;
      DECLARE `int` INT DEFAULT 3;
      SET got = date + time + `int`;
      lbl: LOOP
        IF IF(1, 1, 0) THEN LEAVE lbl; ELSE SET got=0; END /* gap */ IF /* tail */;
      END LOOP lbl /* note */;
    END -- trailing comment",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL audit_names(@got)").unwrap();
    assert_eq!(session.user_variable("got"), Some(&Value::Int(7)));
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE audit_quoted_label() BEGIN
        DECLARE n INT DEFAULT 0;
        `loop`: LOOP SET n=n+1;
          IF n<2 THEN ITERATE `loop`; ELSE LEAVE `loop`; END IF;
        END LOOP /* omitted closing label */;
        `lbl`: LOOP LEAVE `lbl`; END LOOP `lbl` /* explicit closing label */;
        SET @label_iterations=n;
    END",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL audit_quoted_label()").unwrap();
    assert_eq!(
        session.user_variable("label_iterations"),
        Some(&Value::Int(2))
    );
    assert!(exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE bad_label() BEGIN
        foo: LOOP LEAVE foo; END LOOP barfoo; END"
    )
    .is_err());
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE audit_date() BEGIN
        DECLARE date DATE; SET date='2024-01-02'; SELECT date;
    END /* trailing */",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL audit_date()").unwrap();
}

#[test]
fn audit_unhandled_coercion_warnings_continue_in_original_block() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "SET sql_mode=''").unwrap();
    for (name, handler) in [
        ("plain", ""),
        (
            "unrelated",
            "DECLARE CONTINUE HANDLER FOR 1062 SET @wrong=1;",
        ),
    ] {
        exec(
            &engine,
            &mut session,
            &format!(
                "CREATE PROCEDURE {name}(OUT got BIGINT) BEGIN
          DECLARE x INT; {handler}
          BEGIN SET x=99999999999; SET got=x; END;
          SET got=got+1;
        END"
            ),
        )
        .unwrap();
        exec(&engine, &mut session, &format!("CALL {name}(@got)")).unwrap();
        assert_eq!(session.user_variable("got"), Some(&Value::Int(2147483648)));
        assert!(session.warnings.iter().any(|w| w.code() == 1264));
    }
    for (name, action, expected) in [
        ("handled_continue", "CONTINUE", 8),
        ("handled_exit", "EXIT", 7),
    ] {
        exec(
            &engine,
            &mut session,
            &format!(
                "CREATE PROCEDURE {name}(OUT got INT) BEGIN
          DECLARE x INT;
          DECLARE {action} HANDLER FOR SQLEXCEPTION SET got=7;
          SET x=99999999999; SET got=got+1;
        END"
            ),
        )
        .unwrap();
        exec(&engine, &mut session, &format!("CALL {name}(@got)")).unwrap();
        assert_eq!(session.user_variable("got"), Some(&Value::Int(expected)));
    }
    exec(&engine, &mut session, "SET sql_mode='STRICT_TRANS_TABLES'").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE strict_overflow() BEGIN DECLARE x INT;
      SET x=99999999999; SET @should_not_run=1; END",
    )
    .unwrap();
    assert_eq!(
        exec(&engine, &mut session, "CALL strict_overflow()")
            .unwrap_err()
            .code(),
        1264
    );
    assert!(session.user_variable("should_not_run").is_none());
}

#[test]
fn audit_sys_builtin_names_are_already_existing_routines() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for (kind, name, body) in [
        ("PROCEDURE", "table_exists", "BEGIN SELECT 42; END"),
        ("FUNCTION", "format_bytes", "RETURNS INT RETURN 42"),
    ] {
        let sql = format!("CREATE {kind} sys.{name}() {body}");
        assert_eq!(exec(&engine, &mut session, &sql).unwrap_err().code(), 1304);
        exec(
            &engine,
            &mut session,
            &format!("CREATE {kind} IF NOT EXISTS sys.{name}() {body}"),
        )
        .unwrap();
        assert!(session.warnings.iter().any(|w| w.code() == 1304));
        let QueryResponse::ResultSet(rs) = exec(&engine, &mut session,
            &format!("SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA='sys' AND ROUTINE_NAME='{name}'")).unwrap() else { panic!() };
        assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    }
}

#[test]
fn audit_native_function_collision_warns_and_qualified_call_remains_available() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE DATABASE audit_native").unwrap();
    for name in ["upper", "sum"] {
        exec(
            &engine,
            &mut session,
            &format!("CREATE FUNCTION audit_native.{name}(x INT) RETURNS INT RETURN 42"),
        )
        .unwrap();
        assert!(session.warnings.iter().any(|w| w.code() == 1585));
        let QueryResponse::ResultSet(rs) = exec(
            &engine,
            &mut session,
            &format!("SELECT audit_native.{name}(1)"),
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(rs.rows, vec![vec![Value::Int(42)]]);
    }
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT upper('a'), sum(1)").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows[0][0], Value::String("A".into()));
}

#[test]
fn audit_user_variable_metadata_tracks_current_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE audit_out(OUT v INT) SET v=42",
    )
    .unwrap();
    exec(&engine, &mut session, "CALL audit_out(@v)").unwrap();
    for (assignment, expected) in [
        ("", ColumnType::LongLong),
        ("SET @v='text'", ColumnType::VarString),
        ("SET @v=21", ColumnType::LongLong),
        ("SET @v=NULL", ColumnType::VarString),
    ] {
        if !assignment.is_empty() {
            exec(&engine, &mut session, assignment).unwrap();
        }
        let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SELECT @v").unwrap() else {
            panic!()
        };
        assert_eq!(rs.columns[0].column_type, expected);
    }
}
