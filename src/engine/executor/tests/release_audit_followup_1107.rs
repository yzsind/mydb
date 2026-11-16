use super::support::execute_materialized as exec;
use super::*;

#[test]
fn release_followup_warning_handlers_use_condition_level() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for (name, handler, expression, expected) in [
        ("warn", "SQLWARNING", "'123abc'+1", 1),
        ("exc", "SQLEXCEPTION", "'123abc'+1", 0),
        ("divide", "SQLEXCEPTION", "1/0", 0),
        ("errno", "1292", "'123abc'+1", 1),
        ("state", "SQLSTATE '22007'", "'123abc'+1", 1),
    ] {
        exec(&engine, &mut session, &format!("CREATE PROCEDURE {name}() BEGIN DECLARE EXIT HANDLER FOR {handler} SET @hit=1; SET @hit=0; SELECT {expression}; SET @after=1; END")).unwrap();
        exec(&engine, &mut session, "SET @after=0").unwrap();
        exec(&engine, &mut session, &format!("CALL {name}()")).unwrap();
        assert_eq!(
            session.user_variable("hit"),
            Some(&Value::Int(expected)),
            "{name}"
        );
        assert_eq!(
            session.user_variable("after"),
            Some(&Value::Int(1 - expected)),
            "{name}"
        );
    }
    exec(&engine, &mut session, "CREATE PROCEDURE multiple_warnings() BEGIN DECLARE CONTINUE HANDLER FOR 1292 SET @hit=1; SET @hit=0; SELECT '123abc'+1,1/0; END").unwrap();
    exec(&engine, &mut session, "CALL multiple_warnings()").unwrap();
    assert_eq!(session.user_variable("hit"), Some(&Value::Int(1)));
    exec(&engine, &mut session, "CREATE PROCEDURE nested_warning() BEGIN DECLARE EXIT HANDLER FOR SQLWARNING SET @outer_hit=1; SET @outer_hit=0; BEGIN SELECT '123abc'+1; END; SET @after=1; END").unwrap();
    exec(&engine, &mut session, "SET @after=0").unwrap();
    exec(&engine, &mut session, "CALL nested_warning()").unwrap();
    assert_eq!(session.user_variable("outer_hit"), Some(&Value::Int(1)));
    assert_eq!(session.user_variable("after"), Some(&Value::Int(0)));
    exec(&engine, &mut session, "CREATE PROCEDURE resignal_warning() BEGIN DECLARE CONTINUE HANDLER FOR 1292 RESIGNAL SET MESSAGE_TEXT='retained warning'; SELECT '123abc'+1; SET @after=1; END").unwrap();
    exec(&engine, &mut session, "SET @after=0").unwrap();
    exec(&engine, &mut session, "CALL resignal_warning()").unwrap();
    assert_eq!(session.user_variable("after"), Some(&Value::Int(1)));
    exec(&engine, &mut session, "CREATE PROCEDURE resignal_block() BEGIN DECLARE CONTINUE HANDLER FOR 1292 BEGIN RESIGNAL SET MESSAGE_TEXT='retained warning'; SET @inside=1; END; SELECT '123abc'+1; SET @after=1; END").unwrap();
    exec(&engine, &mut session, "SET @inside=0,@after=0").unwrap();
    exec(&engine, &mut session, "CALL resignal_block()").unwrap();
    assert_eq!(session.user_variable("inside"), Some(&Value::Int(1)));
    assert_eq!(session.user_variable("after"), Some(&Value::Int(1)));
}

#[test]
fn release_followup_time_endpoint_checked_before_fractional_quantization() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for fsp in [0, 3, 6] {
        exec(
            &engine,
            &mut session,
            &format!("CREATE TABLE tm{fsp}(v TIME({fsp}))"),
        )
        .unwrap();
        for mode in [
            "STRICT_TRANS_TABLES",
            "STRICT_ALL_TABLES,TIME_TRUNCATE_FRACTIONAL",
        ] {
            exec(&engine, &mut session, &format!("SET sql_mode='{mode}'")).unwrap();
            for value in ["838:59:59.000001", "-838:59:59.999999", "8385959.000001"] {
                let err = exec(
                    &engine,
                    &mut session,
                    &format!("INSERT INTO tm{fsp} VALUES('{value}')"),
                )
                .unwrap_err();
                assert_eq!(err.code(), 1292, "{fsp}: {mode}: {value}");
            }
        }
        exec(&engine, &mut session, "SET sql_mode='STRICT_TRANS_TABLES'").unwrap();
        let err = exec(
            &engine,
            &mut session,
            &format!("INSERT INTO tm{fsp} VALUES('838:59:59.0000005')"),
        )
        .unwrap_err();
        assert_eq!(err.code(), 1292);
        exec(
            &engine,
            &mut session,
            &format!(
                "INSERT INTO tm{fsp} VALUES('838:59:59'),('-838:59:59'),('838:59:58.9999999')"
            ),
        )
        .unwrap();
        exec(&engine, &mut session, &format!("DELETE FROM tm{fsp}")).unwrap();
        exec(&engine, &mut session, "SET sql_mode=''").unwrap();
        exec(
            &engine,
            &mut session,
            &format!("INSERT INTO tm{fsp} VALUES('838:59:59.000001')"),
        )
        .unwrap();
        assert_eq!(session.warnings.len(), 1);
        let QueryResponse::ResultSet(result) =
            exec(&engine, &mut session, &format!("SELECT v FROM tm{fsp}")).unwrap()
        else {
            panic!()
        };
        assert_eq!(result.rows, vec![vec![Value::Time(3_020_399_000_000)]]);
    }
}

#[test]
fn release_followup_sql_execute_keeps_bound_numeric_metadata() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "PREPARE p FROM 'SELECT ? + 1 AS n'").unwrap();
    exec(&engine, &mut session, "SET @n=41").unwrap();
    let QueryResponse::ResultSet(result) =
        exec(&engine, &mut session, "EXECUTE p USING @n").unwrap()
    else {
        panic!()
    };
    assert_eq!(result.rows, vec![vec![Value::Int(42)]]);
    assert_eq!(result.columns[0].column_type, ColumnType::LongLong);
    for (binding, expected) in [
        ("9007199254740992", Value::Int(9_007_199_254_740_993)),
        ("18446744073709551614", Value::UInt(u64::MAX)),
    ] {
        exec(&engine, &mut session, &format!("SET @n={binding}")).unwrap();
        let QueryResponse::ResultSet(result) =
            exec(&engine, &mut session, "EXECUTE p USING @n").unwrap()
        else {
            panic!()
        };
        assert_eq!(result.rows, vec![vec![expected.clone()]]);
        assert_eq!(result.columns[0].column_type, ColumnType::LongLong);
        assert_eq!(
            result.columns[0].source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0,
            matches!(expected, Value::UInt(_))
        );
    }
}
