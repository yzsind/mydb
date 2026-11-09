use super::support::{execute_materialized as exec, result_rows};
use super::*;

fn fixture() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine, SessionContext::default())
}

#[test]
fn audit1109_routine_parameter_defaults_are_rejected_at_creation() {
    let (_dir, engine, mut s) = fixture();
    for sql in [
        "CREATE PROCEDURE bad(a INT, b INT DEFAULT 5) SET @x=b",
        "CREATE FUNCTION bad(a INT DEFAULT 5) RETURNS INT RETURN a",
        "CREATE PROCEDURE bad(OUT a INT DEFAULT 5) SET a=5",
    ] {
        assert_eq!(
            exec(&engine, &mut s, sql).unwrap_err().code(),
            1064,
            "{sql}"
        );
    }
    exec(
        &engine,
        &mut s,
        "CREATE PROCEDURE good(a INT) BEGIN DECLARE b INT DEFAULT 5; SET @x=a+b; END",
    )
    .unwrap();
    exec(&engine, &mut s, "CALL good(2)").unwrap();
    assert_eq!(s.user_variable("x"), Some(&Value::Int(7)));
}

#[test]
fn audit1109_routine_varying_types_require_lengths() {
    let (_dir, engine, mut s) = fixture();
    for ty in ["VARCHAR", "CHARACTER VARYING", "VARBINARY", "NVARCHAR"] {
        for sql in [
            format!("CREATE PROCEDURE bad(a {ty}) SET @x=1"),
            format!("CREATE FUNCTION bad(a INT) RETURNS {ty} RETURN NULL"),
            format!("CREATE PROCEDURE bad() BEGIN DECLARE a {ty}; SET @x=1; END"),
        ] {
            assert_eq!(
                exec(&engine, &mut s, &sql).unwrap_err().code(),
                1064,
                "{sql}"
            );
        }
    }
    for (i, ty) in [
        "VARCHAR(12) CHARACTER SET utf8mb4",
        "VARBINARY(12)",
        "CHAR",
        "DECIMAL(10,2)",
        "ENUM('DEFAULT','x')",
        "INT UNSIGNED",
    ]
    .iter()
    .enumerate()
    {
        exec(
            &engine,
            &mut s,
            &format!("CREATE PROCEDURE good{i}(a {ty}) SET @x=1"),
        )
        .unwrap();
    }
}

#[test]
fn audit1109_unhandled_select_into_no_data_is_a_warning() {
    let (_dir, engine, mut s) = fixture();
    exec(&engine, &mut s, "CREATE TABLE t(a INT, b INT)").unwrap();
    exec(&engine, &mut s, "SET @x=7").unwrap();
    exec(&engine, &mut s, "SELECT a INTO @x FROM t").unwrap();
    assert_eq!(s.warnings.len(), 1);
    assert_eq!(s.warnings[0].code(), 1329);
    exec(
        &engine,
        &mut s,
        "CREATE PROCEDURE p() BEGIN SELECT a INTO @x FROM t; END",
    )
    .unwrap();
    exec(&engine, &mut s, "CALL p()").unwrap();
    assert_eq!(s.user_variable("x"), Some(&Value::Int(7)));
    assert_eq!(s.warnings.len(), 1);
    assert_eq!(s.warnings[0].code(), 1329);
    assert_eq!(s.warnings[0].sql_state().as_str(), "02000");
}

#[test]
fn audit1109_select_into_not_found_handler_consumes_condition() {
    let (_dir, engine, mut s) = fixture();
    exec(&engine, &mut s, "CREATE TABLE t(a INT)").unwrap();
    exec(&engine, &mut s, "CREATE PROCEDURE p() BEGIN DECLARE CONTINUE HANDLER FOR NOT FOUND SET @handled=1; SELECT a INTO @x FROM t; SET @after=1; END").unwrap();
    exec(&engine, &mut s, "CALL p()").unwrap();
    assert_eq!(s.user_variable("handled"), Some(&Value::Int(1)));
    assert_eq!(s.user_variable("after"), Some(&Value::Int(1)));
    assert!(s.warnings.is_empty());
}

#[test]
fn audit1109_select_into_checks_shape_on_empty_results() {
    let (_dir, engine, mut s) = fixture();
    exec(&engine, &mut s, "CREATE TABLE t(a INT, b INT)").unwrap();
    for query in [
        "SELECT a,b INTO @x FROM t",
        "SELECT * INTO @x FROM t",
        "SELECT a INTO @x,@y FROM t",
        "SELECT a,b INTO @x FROM t UNION ALL SELECT a,b FROM t",
    ] {
        assert_eq!(
            exec(&engine, &mut s, query).unwrap_err().code(),
            1222,
            "{query}"
        );
    }
    exec(
        &engine,
        &mut s,
        "CREATE PROCEDURE p() BEGIN SELECT a,b INTO @x FROM t; END",
    )
    .unwrap();
    assert_eq!(exec(&engine, &mut s, "CALL p()").unwrap_err().code(), 1222);
    exec(&engine, &mut s, "INSERT INTO t VALUES (1,2),(3,4)").unwrap();
    assert_eq!(
        exec(&engine, &mut s, "SELECT a INTO @x FROM t")
            .unwrap_err()
            .code(),
        1172
    );
    assert_eq!(
        exec(&engine, &mut s, "SELECT a,b INTO @x FROM t")
            .unwrap_err()
            .code(),
        1222
    );
}

#[test]
fn audit1109_missing_view_function_has_one_schema_prefix() {
    let (_dir, engine, mut s) = fixture();
    for name in ["missing", "test.missing"] {
        let err = exec(
            &engine,
            &mut s,
            &format!("CREATE VIEW v AS SELECT {name}(1) AS x"),
        )
        .unwrap_err();
        assert_eq!(err.code(), 1305);
        assert_eq!(err.message(), "FUNCTION test.missing does not exist");
    }
}

#[test]
fn audit1109_lone_end_is_a_syntax_error() {
    let (_dir, engine, mut s) = fixture();
    for sql in ["END", "END;", "/* comment */ END", "END WORK"] {
        assert_eq!(
            exec(&engine, &mut s, sql).unwrap_err().code(),
            1064,
            "{sql}"
        );
    }
    exec(&engine, &mut s, "CREATE PROCEDURE p() BEGIN SET @x=1; END").unwrap();
    exec(&engine, &mut s, "CALL p()").unwrap();
    exec(&engine, &mut s, "BEGIN").unwrap();
    exec(&engine, &mut s, "COMMIT").unwrap();
}

#[test]
fn audit1109_call_keeps_prior_statements_and_savepoints_need_transaction() {
    let (_dir, engine, mut s) = fixture();
    exec(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    exec(
        &engine,
        &mut s,
        "CREATE PROCEDURE fail() BEGIN INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); END",
    )
    .unwrap();
    assert_eq!(
        exec(&engine, &mut s, "CALL fail()").unwrap_err().code(),
        1062
    );
    assert_eq!(
        result_rows(exec(&engine, &mut s, "SELECT id FROM t").unwrap()),
        vec![vec![Value::Int(1)]]
    );
    exec(&engine, &mut s, "CREATE PROCEDURE sp() BEGIN SAVEPOINT s; INSERT INTO t VALUES (2); ROLLBACK TO s; INSERT INTO t VALUES (3); END").unwrap();
    exec(&engine, &mut s, "START TRANSACTION").unwrap();
    exec(&engine, &mut s, "CALL sp()").unwrap();
    assert_eq!(
        result_rows(exec(&engine, &mut s, "SELECT id FROM t ORDER BY id").unwrap()),
        vec![vec![Value::Int(1)], vec![Value::Int(3)]]
    );
    exec(&engine, &mut s, "ROLLBACK").unwrap();
}

#[test]
fn audit1109_odku_values_observe_before_insert_trigger() {
    let (_dir, engine, mut s) = fixture();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1,100)",
        "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW SET NEW.v=NEW.v*100",
        "CREATE TRIGGER bu BEFORE UPDATE ON t FOR EACH ROW SET NEW.v=NEW.v*1000",
        "INSERT INTO t VALUES (1,3) ON DUPLICATE KEY UPDATE v=VALUES(v)",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    assert_eq!(
        result_rows(exec(&engine, &mut s, "SELECT v FROM t").unwrap()),
        vec![vec![Value::Int(300000)]]
    );
}

#[test]
fn audit1109_insert_ignore_trigger_null_skips_row_with_warning() {
    let (_dir, engine, mut s) = fixture();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY, v INT NOT NULL)",
        "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW SET NEW.v=NULL",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    exec(&engine, &mut s, "INSERT IGNORE INTO t VALUES (1,5)").unwrap();
    assert!(s.warnings.iter().any(|w| w.code() == 1048));
    assert!(result_rows(exec(&engine, &mut s, "SELECT * FROM t").unwrap()).is_empty());
}

#[test]
fn audit1109_call_status_uses_last_statement_with_found_rows() {
    let (_dir, engine, mut s) = fixture();
    s.client_found_rows = true;
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1,1),(2,2)",
        "CREATE PROCEDURE p() BEGIN UPDATE t SET v=v; DELETE FROM t WHERE id=999; END",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    let QueryResponse::MultiResult(results) = exec(&engine, &mut s, "CALL p()").unwrap() else {
        panic!("CALL");
    };
    assert!(matches!(
        results.last(),
        Some(QueryResponse::OkAffected { affected_rows: 0 })
    ));
    assert!(crate::engine::api::statement_matched_rows().is_none());
}

#[test]
fn audit1109_current_user_definers_resolve_before_authorization_and_storage() {
    let (_dir, engine, mut s) = fixture();
    exec(&engine, &mut s, "CREATE TABLE t(id INT)").unwrap();
    for (i, account) in ["CURRENT_USER", "CURRENT_USER()"].iter().enumerate() {
        for sql in [
            format!("CREATE DEFINER={account} FUNCTION f{i}() RETURNS INT RETURN 5"),
            format!("CREATE DEFINER={account} PROCEDURE p{i}() SET @got=5"),
            format!("CREATE DEFINER={account} TRIGGER tr{i} BEFORE INSERT ON t FOR EACH ROW SET @triggered=5"),
            format!("CREATE DEFINER={account} EVENT e{i} ON SCHEDULE EVERY 1 DAY DISABLE DO SET @event=5"),
            format!("CREATE DEFINER={account} VIEW v{i} AS SELECT id FROM t"),
        ] {
            exec(&engine, &mut s, &sql).unwrap();
            assert!(s.warnings.is_empty(), "{sql}: {:?}", s.warnings);
        }
        assert_eq!(
            result_rows(exec(&engine, &mut s, &format!("SELECT f{i}()")).unwrap()),
            vec![vec![Value::Int(5)]]
        );
        exec(&engine, &mut s, &format!("CALL p{i}()")).unwrap();
        exec(&engine, &mut s, &format!("SELECT * FROM v{i}")).unwrap();
        let rows = result_rows(exec(&engine, &mut s, &format!("SELECT DEFINER FROM information_schema.events WHERE event_schema='test' AND event_name='e{i}'")).unwrap());
        assert_eq!(rows, vec![vec![Value::String("root@localhost".into())]]);
        exec(
            &engine,
            &mut s,
            &format!("ALTER DEFINER={account} EVENT e{i} COMMENT 'changed'"),
        )
        .unwrap();
        exec(
            &engine,
            &mut s,
            &format!("ALTER DEFINER={account} VIEW v{i} AS SELECT id FROM t"),
        )
        .unwrap();
    }
    exec(&engine, &mut s, "INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(s.user_variable("triggered"), Some(&Value::Int(5)));
}

#[test]
fn audit1109_current_user_definer_needs_no_set_user_id_and_preserves_literals() {
    let (_dir, engine, mut root) = fixture();
    exec(
        &engine,
        &mut root,
        "CREATE USER 'rvt_creator'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "GRANT CREATE ROUTINE ON test.* TO 'rvt_creator'@'%'",
    )
    .unwrap();
    let mut creator = SessionContext {
        user: "rvt_creator".into(),
        login_user: "rvt_creator".into(),
        authenticated_as: crate::engine::auth::AccountKey::new("rvt_creator", "%"),
        ..SessionContext::default()
    };
    exec(
        &engine,
        &mut creator,
        "CREATE DEFINER=CURRENT_USER() PROCEDURE own() SET @got=1",
    )
    .unwrap();
    assert!(creator.warnings.is_empty());
    exec(&engine, &mut creator, "CALL own()").unwrap();
    assert_eq!(creator.user_variable("got"), Some(&Value::Int(1)));
    assert_eq!(
        exec(
            &engine,
            &mut creator,
            "CREATE DEFINER='root'@'localhost' PROCEDURE other() SET @got=1"
        )
        .unwrap_err()
        .code(),
        1227
    );
    exec(
        &engine,
        &mut root,
        "CREATE DEFINER='CURRENT_USER()'@'%' PROCEDURE literal() SET @got=1",
    )
    .unwrap();
    assert!(root.warnings.iter().any(|w| w.code() == 1449));
    assert_eq!(
        exec(&engine, &mut root, "CALL literal()")
            .unwrap_err()
            .code(),
        1449
    );
    exec(
        &engine,
        &mut root,
        "CREATE DEFINER='missing'@'%' VIEW orphan AS SELECT 1 AS x",
    )
    .unwrap();
    exec(&engine, &mut root, "ALTER VIEW orphan AS SELECT 2 AS x").unwrap();
    assert_eq!(
        exec(&engine, &mut root, "SELECT * FROM orphan")
            .unwrap_err()
            .code(),
        1449
    );
    exec(
        &engine,
        &mut root,
        "ALTER DEFINER=CURRENT_USER() VIEW orphan AS SELECT 3 AS x",
    )
    .unwrap();
    assert!(root.warnings.is_empty());
    assert_eq!(
        result_rows(exec(&engine, &mut root, "SELECT * FROM orphan").unwrap()),
        vec![vec![Value::Int(3)]]
    );
}
