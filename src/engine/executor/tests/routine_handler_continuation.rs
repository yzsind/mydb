use super::support::execute_materialized as exec;
use super::*;

fn run_case(sql: &str, variables: &[(&str, Value)]) {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, sql).unwrap();
    exec(&engine, &mut session, "CALL p()").unwrap();
    for (name, expected) in variables {
        assert_eq!(session.user_variable(name), Some(expected), "{name}");
    }
}

#[test]
fn routine_continuation_resumes_at_warning_and_error_source_once() {
    for source in ["SELECT '123abc'+1", "SIGNAL SQLSTATE '45000'"] {
        let condition = if source.starts_with("SELECT") {
            "1292"
        } else {
            "SQLSTATE '45000'"
        };
        run_case(
            &format!(
                "CREATE PROCEDURE p() BEGIN
            DECLARE hits INT DEFAULT 0;
            DECLARE CONTINUE HANDLER FOR {condition} SET hits=hits+1;
            SET @inside=0;
            BEGIN {source}; SET @inside=1; END;
            SET @hits=hits;
        END"
            ),
            &[("inside", Value::Int(1)), ("hits", Value::Int(1))],
        );
    }
}

#[test]
fn routine_continuation_preserves_local_types_and_named_condition_scope() {
    run_case(
        "CREATE PROCEDURE p() BEGIN
        DECLARE x INT DEFAULT 10;
        DECLARE caught CONDITION FOR SQLSTATE '45000';
        DECLARE CONTINUE HANDLER FOR caught SET x=x+1;
        BEGIN
            DECLARE x DECIMAL(5,2) DEFAULT 2.5;
            BEGIN
                DECLARE x VARCHAR(20) DEFAULT 'inner';
                DECLARE caught CONDITION FOR SQLSTATE '45001';
                SIGNAL SQLSTATE '45000';
                SET @inner_value=x;
            END;
            SET @middle_value=x;
        END;
        SET @outer_value=x;
    END",
        &[
            ("inner_value", Value::String("inner".into())),
            ("middle_value", Value::DecimalCompact(250, 2)),
            ("outer_value", Value::Int(11)),
        ],
    );
}

#[test]
fn routine_continuation_preserves_shadowed_cursor_positions() {
    run_case(
        "CREATE PROCEDURE p() BEGIN
        DECLARE v INT;
        DECLARE c CURSOR FOR SELECT 11 UNION ALL SELECT 12;
        DECLARE CONTINUE HANDLER FOR SQLSTATE '45000'
          BEGIN FETCH c INTO v; SET @handler_cursor=v; END;
        OPEN c;
        BEGIN
            DECLARE inside_v INT;
            DECLARE c CURSOR FOR SELECT 21 UNION ALL SELECT 22;
            OPEN c;
            SIGNAL SQLSTATE '45000';
            FETCH c INTO inside_v;
            SET @inner_cursor=inside_v;
            CLOSE c;
        END;
        FETCH c INTO v;
        SET @outer_cursor=v;
        CLOSE c;
    END",
        &[
            ("handler_cursor", Value::Int(11)),
            ("inner_cursor", Value::Int(21)),
            ("outer_cursor", Value::Int(12)),
        ],
    );
}

#[test]
fn routine_continuation_resignal_resumes_handler_then_original_statement() {
    run_case(
        "CREATE PROCEDURE p() BEGIN
        DECLARE CONTINUE HANDLER FOR SQLSTATE '45000'
          SET @trace=CONCAT(@trace,'O');
        SET @trace='';
        BEGIN
            DECLARE CONTINUE HANDLER FOR SQLSTATE '45000'
              BEGIN
                SET @trace=CONCAT(@trace,'B');
                RESIGNAL SET MESSAGE_TEXT='forwarded';
                SET @trace=CONCAT(@trace,'H');
              END;
            SIGNAL SQLSTATE '45000';
            SET @trace=CONCAT(@trace,'I');
        END;
        SET @trace=CONCAT(@trace,'A');
    END",
        &[("trace", Value::String("BOHIA".into()))],
    );
}

#[test]
fn routine_continuation_keeps_nearest_exit_handler_boundary() {
    run_case(
        "CREATE PROCEDURE p() BEGIN
        DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' SET @wrong=1;
        SET @wrong=0,@bad=0,@exit_hit=0,@after=0;
        BEGIN
            DECLARE EXIT HANDLER FOR SQLSTATE '45000' SET @exit_hit=1;
            BEGIN SIGNAL SQLSTATE '45000'; SET @bad=1; END;
            SET @bad=2;
        END;
        SET @after=1;
    END",
        &[
            ("wrong", Value::Int(0)),
            ("bad", Value::Int(0)),
            ("exit_hit", Value::Int(1)),
            ("after", Value::Int(1)),
        ],
    );
}

#[test]
fn routine_continuation_preserves_loop_and_branch_continuations() {
    run_case(
        "CREATE PROCEDURE p() BEGIN
        DECLARE hits INT DEFAULT 0;
        DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' SET hits=hits+1;
        SET @iterations=0;
        BEGIN
            DECLARE i INT DEFAULT 0;
            WHILE i<3 DO
                IF i=1 THEN
                    SIGNAL SQLSTATE '45000';
                ELSE
                    CASE i WHEN 0 THEN SIGNAL SQLSTATE '45000';
                      ELSE SIGNAL SQLSTATE '45000'; END CASE;
                END IF;
                SET i=i+1;
                SET @iterations=@iterations+1;
            END WHILE;
        END;
        SET @hits=hits;
    END",
        &[("iterations", Value::Int(3)), ("hits", Value::Int(3))],
    );
}

#[test]
fn routine_continuation_applies_to_functions_and_triggers() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE FUNCTION f() RETURNS INT DETERMINISTIC BEGIN
           DECLARE x INT DEFAULT 1;
           DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' SET x=x+1;
           BEGIN SIGNAL SQLSTATE '45000'; SET x=x+10; END;
           RETURN x;
         END",
        "CREATE TABLE t(v INT)",
        "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW BEGIN
           DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' SET NEW.v=NEW.v+1;
           BEGIN SIGNAL SQLSTATE '45000'; SET NEW.v=NEW.v+10; END;
         END",
        "INSERT INTO t VALUES(1)",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    let QueryResponse::ResultSet(result) =
        exec(&engine, &mut session, "SELECT f(),v FROM t").unwrap()
    else {
        panic!()
    };
    assert_eq!(result.rows, vec![vec![Value::Int(12), Value::Int(12)]]);
}

#[test]
fn routine_continuation_does_not_reenter_handler_after_failure() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE p() BEGIN
        DECLARE CONTINUE HANDLER FOR SQLSTATE '45000'
          BEGIN SET @hits=@hits+1; SIGNAL SQLSTATE '45000'; END;
        SET @hits=0;
        BEGIN SIGNAL SQLSTATE '45000'; END;
    END",
    )
    .unwrap();
    assert_eq!(
        exec(&engine, &mut session, "CALL p()").unwrap_err().code(),
        1644
    );
    assert_eq!(session.user_variable("hits"), Some(&Value::Int(1)));
}

#[test]
fn routine_continuation_prefers_nearest_scope_over_error_specificity() {
    run_case(
        "CREATE PROCEDURE p() BEGIN
        DECLARE CONTINUE HANDLER FOR 1644 SET @wrong=1;
        SET @wrong=0,@middle_hit=0,@inside=0;
        BEGIN
            DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET @middle_hit=@middle_hit+1;
            BEGIN SIGNAL SQLSTATE '45000'; SET @inside=1; END;
        END;
    END",
        &[
            ("wrong", Value::Int(0)),
            ("middle_hit", Value::Int(1)),
            ("inside", Value::Int(1)),
        ],
    );
}

#[test]
fn routine_continuation_outer_exit_leaves_active_handler_and_declaring_block() {
    run_case(
        "CREATE PROCEDURE p() BEGIN
        DECLARE EXIT HANDLER FOR SQLSTATE '45001' SET @outer_hit=1;
        SET @outer_hit=0,@bad=0;
        BEGIN
            DECLARE CONTINUE HANDLER FOR SQLSTATE '45000'
              BEGIN SIGNAL SQLSTATE '45001'; SET @bad=1; END;
            BEGIN SIGNAL SQLSTATE '45000'; SET @bad=2; END;
            SET @bad=3;
        END;
        SET @bad=4;
    END",
        &[("outer_hit", Value::Int(1)), ("bad", Value::Int(0))],
    );
}
