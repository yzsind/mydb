//! Execution-level regression coverage for doc_old/parser-bugs-audit.md.
use super::*;

fn run(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> QueryResponse {
    engine
        .execute_with_session(session, sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = run(engine, session, sql) else {
        panic!("{sql}: expected rows")
    };
    result.rows
}

#[test]
fn parser_audit_dml_values_and_no_silent_side_effects() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    run(
        &engine,
        &mut session,
        "CREATE TABLE t (escape INT, `exclude` INT, `default` INT)",
    );
    run(&engine, &mut session, "INSERT INTO t VALUES (1,2,3)");
    run(&engine, &mut session, "UPDATE t SET escape=4");
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT escape, exclude, `default` FROM t"
        ),
        vec![vec![Value::Int(4), Value::Int(2), Value::Int(3)]]
    );
    for sql in [
        "DELETE FROM t WHERE escape=4 AND escape.*",
        "SELECT DISTINCT ON (escape) escape FROM t",
    ] {
        assert!(
            engine.execute_with_session(&mut session, sql).is_err(),
            "{sql}"
        );
    }
    assert_eq!(
        rows(&engine, &mut session, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(1)]]
    );
    run(&engine, &mut session, "CREATE TABLE ins (a INT)");
    run(&engine, &mut session, "SET @v=NULL");
    run(&engine, &mut session, "INSERT INTO ins SELECT @v := 7");
    assert_eq!(
        rows(&engine, &mut session, "SELECT @v, a FROM ins"),
        vec![vec![Value::Int(7), Value::Int(7)]]
    );
    run(
        &engine,
        &mut session,
        "CREATE TABLE ctas AS SELECT @v := 8 AS a",
    );
    assert_eq!(
        rows(&engine, &mut session, "SELECT @v, a FROM ctas"),
        vec![vec![Value::Int(8), Value::String("8".into())]]
    );
    run(&engine, &mut session, "SET @'a b'=9");
    assert_eq!(
        rows(&engine, &mut session, "SELECT @`a b`, @\"a b\""),
        vec![vec![Value::Int(9), Value::Int(9)]]
    );
    run(
        &engine,
        &mut session,
        "SET autocommit=0, sql_mode='STRICT_TRANS_TABLES'",
    );
    assert!(!session.autocommit);
    let actual = rows(&engine, &mut session, "SELECT @@sql_mode");
    assert!(actual[0][0]
        .to_display_string()
        .contains("STRICT_TRANS_TABLES"));
}

#[test]
fn parser_audit_unicode_escapes_ddl_and_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "SELECT INTERVAL(1,2,3), '中文😀'",
        "SELECT NOW() - INTERVAL 1 DAY, '中文😀'",
        "SELECT 1, '中文😀' FROM (VALUES ROW(1),ROW(2)) AS t LIMIT 1",
    ] {
        assert_eq!(
            rows(&engine, &mut s, sql)[0][1].to_display_string(),
            "中文😀"
        );
    }
    assert_eq!(
        rows(&engine, &mut s, r"SELECT HEX('a\ad'), HEX('x\fd')")[0]
            .iter()
            .map(Value::to_display_string)
            .collect::<Vec<_>>(),
        ["616164", "786664"]
    );
    run(&engine, &mut s, "CREATE TABLE view (a INT)");
    run(&engine, &mut s, "ALTER TABLE view ADD COLUMN (b INT,c INT)");
    run(
        &engine,
        &mut s,
        "CREATE TABLE `my table` (a INT, INDEX idx_a(a))",
    );
    run(
        &engine,
        &mut s,
        "ALTER TABLE `my table` RENAME INDEX idx_a TO idx_b",
    );
    run(
        &engine,
        &mut s,
        "CREATE TABLE uni (e ENUM('中文😀','b') BINARY) COMMENT='中文注释😀'",
    );
    let ddl = rows(&engine, &mut s, "SHOW CREATE TABLE uni")[0][1].to_display_string();
    assert!(ddl.contains("中文注释😀"), "{ddl}");
    run(
        &engine,
        &mut s,
        r"CREATE TABLE gen (a INT, g VARCHAR(20) AS (CONCAT('x\\')))",
    );
    run(&engine, &mut s, "INSERT INTO gen(a) VALUES (1)");
    assert_eq!(
        rows(&engine, &mut s, "SELECT g FROM gen")[0][0].to_display_string(),
        "x\\"
    );
    run(
        &engine,
        &mut s,
        r"CREATE TABLE chk (b VARCHAR(20), CHECK (b <> 'x\\'))",
    );
    let ddl = rows(&engine, &mut s, "SHOW CREATE TABLE chk")[0][1].to_display_string();
    run(&engine, &mut s, "DROP TABLE chk");
    run(&engine, &mut s, &ddl);
    assert_eq!(
        engine
            .execute_with_session(&mut s, r"INSERT INTO chk VALUES ('x\\')")
            .unwrap_err()
            .code(),
        3819
    );
    for data_type in ["SET('a','b')", "ENUM('a','b')"] {
        run(
            &engine,
            &mut s,
            &format!("CREATE PROCEDURE p(IN x {data_type}) BEGIN SELECT x; END"),
        );
        let result = rows(&engine, &mut s, "SHOW CREATE PROCEDURE p");
        let ddl = result[0]
            .iter()
            .map(Value::to_display_string)
            .find(|v| v.starts_with("CREATE "))
            .unwrap();
        assert!(ddl.contains("'a'"), "{ddl}");
        run(&engine, &mut s, "DROP PROCEDURE p");
        run(&engine, &mut s, &ddl);
        run(&engine, &mut s, "DROP PROCEDURE p");
    }
}

#[test]
fn parser_audit_check_mixed_types_preserves_enforcement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for (i, ty, expr) in [
        (0, "INT", "a > '5'"),
        (1, "DECIMAL(10,2)", "a > '5'"),
        (2, "VARCHAR(10)", "a > 5"),
    ] {
        run(
            &engine,
            &mut s,
            &format!("CREATE TABLE ck{i} (a {ty}, CHECK ({expr}))"),
        );
        let table = engine
            .db
            .table_in_database("test", &format!("ck{i}"))
            .unwrap();
        let schema = crate::engine::expr_program::InputSchema::from_table(&table);
        let parsed = crate::parser::parse_expr_fragment(expr).unwrap();
        assert!(
            crate::engine::expr_program::ExprCompiler::compile_strict(&parsed, &schema).is_err()
        );
        run(
            &engine,
            &mut s,
            &format!("INSERT INTO ck{i} VALUES (6), (NULL)"),
        );
        let err = engine
            .execute_with_session(&mut s, &format!("INSERT INTO ck{i} VALUES (4)"))
            .unwrap_err();
        assert_eq!(err.code(), 3819);
    }
    assert!(engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE bad (a INT, CHECK (unknown_func(a) > '5'))"
        )
        .is_err());
    assert!(engine
        .execute_with_session(&mut s, "CREATE TABLE bad (a INT, CHECK (a > @v))")
        .is_err());
}

#[test]
fn parser_audit_accounts_batch_atomicity_and_policy() {
    use crate::engine::auth::{account_id, AccountKey};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    run(
        &engine,
        &mut s,
        "CREATE USER audit_a IDENTIFIED BY 'old-a', audit_b IDENTIFIED BY 'old-b'",
    );
    let old_a = engine.db.auth_catalog().accounts[&account_id(&AccountKey::new("audit_a", "%"))]
        .authentication_string
        .clone();
    run(
        &engine,
        &mut s,
        "ALTER USER audit_a, audit_b IDENTIFIED BY 'new-b' PASSWORD EXPIRE INTERVAL 5 DAY",
    );
    let auth = engine.db.auth_catalog();
    assert_eq!(
        auth.accounts[&account_id(&AccountKey::new("audit_a", "%"))].authentication_string,
        old_a,
        "auth_option only applies to its preceding account"
    );
    for user in ["audit_a", "audit_b"] {
        assert_eq!(
            auth.accounts[&account_id(&AccountKey::new(user, "%"))].password_lifetime_days,
            Some(5)
        );
    }
    let before = auth.accounts.clone();
    drop(auth);
    assert!(engine
        .execute_with_session(
            &mut s,
            "ALTER USER audit_a IDENTIFIED BY 'rollback-me', nonexistent_user IDENTIFIED BY 'x'"
        )
        .is_err());
    assert_eq!(
        engine.db.auth_catalog().accounts,
        before,
        "whole auth batch must roll back"
    );
    for sql in [
        "ALTER USER audit_a PASSWORD EXPIRE INTERVAL abc DAY",
        "ALTER USER audit_a PASSWORD EXPIRE INTERVAL 5 MONTH",
        "SET PASSWORD = PASSWORD('x')",
    ] {
        assert_eq!(
            engine.execute_with_session(&mut s, sql).unwrap_err().code(),
            1064
        );
    }
    run(&engine, &mut s, "GRANT SELECT ON *.* TO CURRENT_USER()");
    let result = rows(&engine, &mut s, "SHOW GRANTS");
    assert!(!result.is_empty());
    run(
        &engine,
        &mut s,
        "ALTER USER audit_a IDENTIFIED BY 'new-a', audit_b IDENTIFIED BY 'newer-b'",
    );
    let sql = credential_safe_binlog_sql(
        &engine.db,
        &s,
        "ALTER USER audit_a IDENTIFIED BY 'new-a', audit_b IDENTIFIED BY 'newer-b'",
    )
    .unwrap();
    assert!(!sql.contains("new-a") && !sql.contains("newer-b"), "{sql}");
    assert!(crate::parser::parse_statement(&sql).is_ok(), "{sql}");
    let replica_dir = tempfile::tempdir().unwrap();
    let replica = open_test_engine(replica_dir.path());
    let mut replay_session = SessionContext::default();
    run(&replica, &mut replay_session, "CREATE USER audit_a IDENTIFIED BY 'old-a', audit_b IDENTIFIED BY 'old-b' PASSWORD EXPIRE INTERVAL 5 DAY");
    run(&replica, &mut replay_session, &sql);
    for user in ["audit_a", "audit_b"] {
        let key = account_id(&AccountKey::new(user, "%"));
        assert_eq!(
            replica.db.auth_catalog().accounts[&key].authentication_string,
            engine.db.auth_catalog().accounts[&key].authentication_string
        );
        assert_eq!(
            replica.db.auth_catalog().accounts[&key].password_lifetime_days,
            Some(5)
        );
    }
    run(
        &engine,
        &mut s,
        "ALTER USER audit_a, audit_b PASSWORD HISTORY 2 ACCOUNT LOCK",
    );
    let policy_sql = credential_safe_binlog_sql(
        &engine.db,
        &s,
        "ALTER USER audit_a, audit_b PASSWORD HISTORY 2 ACCOUNT LOCK",
    )
    .unwrap();
    run(&replica, &mut replay_session, &policy_sql);
    for user in ["audit_a", "audit_b"] {
        let key = account_id(&AccountKey::new(user, "%"));
        let catalog = replica.db.auth_catalog();
        assert_eq!(catalog.accounts[&key].password_reuse_history, Some(2));
        assert!(catalog.accounts[&key].account_locked);
    }
}

#[test]
fn parser_audit_schema_literals_survive_mode_changes_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let open = || {
        let mut config = memory_test_storage_config();
        config.durability = crate::storage::DurabilityMode::Strict;
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap()
    };
    {
        let engine = open();
        let mut s = SessionContext::default();
        run(&engine, &mut s, "SET sql_mode='NO_BACKSLASH_ESCAPES'");
        run(
            &engine,
            &mut s,
            r"CREATE TABLE persisted (a INT, b VARCHAR(20), g VARCHAR(20) AS (CONCAT('x\')), CHECK (b <> 'x\'))",
        );
        run(
            &engine,
            &mut s,
            "INSERT INTO persisted(a,b) VALUES (1,'ok')",
        );
        assert_eq!(
            rows(&engine, &mut s, "SELECT HEX(g) FROM persisted")[0][0].to_display_string(),
            "785C"
        );
    }
    let engine = open();
    let mut s = SessionContext::default();
    run(
        &engine,
        &mut s,
        "INSERT INTO persisted(a,b) VALUES (2,'ok')",
    );
    assert_eq!(
        rows(&engine, &mut s, "SELECT HEX(g) FROM persisted WHERE a=2")[0][0].to_display_string(),
        "785C"
    );
    assert_eq!(
        engine
            .execute_with_session(&mut s, r"INSERT INTO persisted(a,b) VALUES (3,'x\\')")
            .unwrap_err()
            .code(),
        3819
    );
    run(&engine, &mut s, "SET sql_mode='NO_BACKSLASH_ESCAPES'");
    let ddl = rows(&engine, &mut s, "SHOW CREATE TABLE persisted")[0][1].to_display_string();
    run(&engine, &mut s, "DROP TABLE persisted");
    run(&engine, &mut s, &ddl);
    run(
        &engine,
        &mut s,
        "INSERT INTO persisted(a,b) VALUES (4,'ok')",
    );
    assert_eq!(
        rows(&engine, &mut s, "SELECT HEX(g) FROM persisted")[0][0].to_display_string(),
        "785C"
    );
    assert_eq!(
        engine
            .execute_with_session(&mut s, r"INSERT INTO persisted(a,b) VALUES (5,'x\')")
            .unwrap_err()
            .code(),
        3819
    );
}
