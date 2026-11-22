use super::support::query_rows as rows;
use super::*;

fn engine() -> (tempfile::TempDir, StorageQueryEngine) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine)
}

#[test]
fn audit1122_json_table_multi_match_and_document_types() {
    let (_dir, engine) = engine();
    for (sql, expected) in [
        (
            "SELECT j FROM JSON_TABLE('[1,2]', '$' COLUMNS(j JSON PATH '$[*]')) t",
            "[1, 2]",
        ),
        (
            "SELECT j FROM JSON_TABLE('[1]', '$' COLUMNS(j JSON PATH '$[*]')) t",
            "1",
        ),
    ] {
        assert_eq!(rows(&engine, sql)[0][0].to_display_string(), expected);
    }
    assert_eq!(
        rows(
            &engine,
            "SELECT j FROM JSON_TABLE('[1,2]', '$' COLUMNS(j INT PATH '$[*]')) t"
        ),
        vec![vec![Value::Null]]
    );
    for doc in ["5", "TRUE", "2.5"] {
        let sql = format!("SELECT * FROM JSON_TABLE({doc}, '$' COLUMNS(j INT PATH '$')) t");
        assert_eq!(engine.execute(&sql).unwrap_err().code(), 3146, "{doc}");
    }
    assert_eq!(
        engine
            .execute("SELECT * FROM JSON_TABLE(_binary'5', '$' COLUMNS(j INT PATH '$')) t")
            .unwrap_err()
            .code(),
        3144
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT * FROM JSON_TABLE('5', '$' COLUMNS(j INT PATH '$')) t"
        ),
        vec![vec![Value::Int(5)]]
    );
}

#[test]
fn audit1122_json_exponent_zero_and_value_responses() {
    let (_dir, engine) = engine();
    assert_eq!(
        rows(&engine, "SELECT CAST('[1e-0,-0,2.5E-0]' AS JSON)")[0][0].to_display_string(),
        "[1.0, 0, 2.5]"
    );
    for (sql, code) in [
        (
            r#"SELECT JSON_VALUE('{"a":1,"b":2}', '$.*' ERROR ON ERROR)"#,
            3967,
        ),
        (
            r#"SELECT JSON_VALUE('{"a":"x"}', '$.a' RETURNING SIGNED ERROR ON ERROR)"#,
            1690,
        ),
        (
            r#"SELECT JSON_VALUE('{"a":"xx"}', '$.a' RETURNING CHAR(1) ERROR ON ERROR)"#,
            1690,
        ),
        (r#"SELECT JSON_VALUE('{}', '$.a' ERROR ON EMPTY)"#, 3966),
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), code, "{sql}");
    }
    for clause in ["", "NULL ON EMPTY", "DEFAULT 'd' ON EMPTY"] {
        let sql = format!(
            r#"SELECT JSON_VALUE(CONCAT('{{"a":"', REPEAT('x',600), '"}}'), '$.a' {clause})"#
        );
        assert_eq!(rows(&engine, &sql), vec![vec![Value::Null]]);
    }
}

#[test]
fn audit1122_alter_expression_default_and_restore_views() {
    let (_dir, engine) = engine();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, j JSON)")
        .unwrap();
    engine
        .execute("ALTER TABLE t ALTER COLUMN j SET DEFAULT (JSON_OBJECT())")
        .unwrap();
    engine.execute("INSERT INTO t(id) VALUES (1)").unwrap();
    assert_eq!(
        rows(&engine, "SELECT j FROM t")[0][0].to_display_string(),
        "{}"
    );
    engine.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    engine.execute("DROP TABLE t").unwrap();
    assert_eq!(engine.execute("SELECT * FROM v").unwrap_err().code(), 1356);
    let name = engine.db.recyclebin_entries().unwrap()[0].name();
    engine.execute(&format!("RESTORE TABLE {name}")).unwrap();
    assert_eq!(rows(&engine, "SELECT id FROM v"), vec![vec![Value::Int(1)]]);
}

#[test]
fn audit1122_safe_updates_requires_a_usable_key() {
    let (_dir, engine) = engine();
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,v INT, KEY(v))",
        "INSERT INTO t VALUES (1,1)",
        "ALTER TABLE t DROP INDEX v",
        "SET sql_safe_updates=ON",
    ] {
        engine.execute_with_session(&mut s, sql).unwrap();
    }
    for sql in [
        "UPDATE t SET v=2 WHERE v=1",
        "DELETE FROM t WHERE v=1",
        "UPDATE t SET v=2 WHERE id+1=2",
        "DELETE FROM t WHERE id=1 OR v=1",
    ] {
        assert_eq!(
            engine.execute_with_session(&mut s, sql).unwrap_err().code(),
            1175,
            "{sql}"
        );
    }
    for sql in [
        "UPDATE t SET v=2 WHERE id=1",
        "UPDATE t SET v=3 WHERE v=2 LIMIT 1",
        "DELETE FROM t WHERE id IN (1,2)",
    ] {
        engine.execute_with_session(&mut s, sql).unwrap();
    }
}

#[test]
fn audit1122_ansi_and_tracking_scope() {
    let (_dir, engine) = engine();
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET sql_mode='ANSI'")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(
                &mut s,
                "SELECT id,v FROM (SELECT 1 id,2 v UNION ALL SELECT 1,3) x GROUP BY id"
            )
            .unwrap_err()
            .code(),
        1055
    );
    engine.execute_with_session(&mut s, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut s, "SET GLOBAL session_track_gtids='OFF'")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut s, "SET SESSION session_track_gtids='OFF'")
            .unwrap_err()
            .code(),
        1766
    );
    engine.execute_with_session(&mut s, "ROLLBACK").unwrap();
}

#[test]
fn audit1122_dangling_foreign_keys_reject_non_null_rows() {
    let (_dir, engine) = engine();
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE p(id INT PRIMARY KEY)",
        "CREATE TABLE c(id INT, FOREIGN KEY(id) REFERENCES p(id))",
        "SET foreign_key_checks=OFF",
        "DROP TABLE p",
        "SET foreign_key_checks=ON",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    for sql in [
        "INSERT INTO c VALUES (1)",
        "INSERT INTO c VALUES (NULL),(1)",
    ] {
        assert_eq!(
            engine
                .execute_with_session(&mut session, sql)
                .unwrap_err()
                .code(),
            1452
        );
    }
    engine
        .execute_with_session(&mut session, "INSERT INTO c VALUES (NULL)")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM c"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn audit1122_privileges_for_reads_and_schema_visibility() {
    let (_dir, engine) = engine();
    for sql in [
        "CREATE TABLE secret(i INT)",
        "INSERT INTO secret VALUES (1)",
        "CREATE USER 'limited'@'%' IDENTIFIED BY 'Test!Pass1122'",
        "GRANT UPDATE ON test.secret TO 'limited'@'%'",
        "GRANT AI_USE ON *.* TO 'limited'@'%'",
    ] {
        engine.execute(sql).unwrap();
    }
    let mut session = SessionContext::default();
    session.authenticated_as = crate::engine::auth::AccountKey::new("limited", "%");
    session.user = "limited".into();
    session.login_user = "limited".into();
    session.auth_epoch = engine.db.auth_catalog().epoch;
    for sql in ["UPDATE secret SET i=2 WHERE i=1", "UPDATE secret SET i=i+1"] {
        assert!(
            engine.execute_with_session(&mut session, sql).is_err(),
            "{sql}"
        );
    }
    engine
        .execute_with_session(&mut session, "UPDATE secret SET i=2")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "ALTER USER USER() IDENTIFIED BY 'New!Pass1122'",
        )
        .unwrap();
    let result = engine
        .execute_with_session(&mut session, "SHOW DATABASES")
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(result) = result else {
        panic!()
    };
    assert!(!result
        .rows
        .iter()
        .flatten()
        .any(|value| value.to_display_string() == "mysql"));
    assert_eq!(
        engine
            .execute("REVOKE PROXY ON 'limited'@'%' FROM 'limited'@'%'")
            .unwrap_err()
            .code(),
        1141
    );
}

#[test]
fn audit1122_returning_oom_rolls_back_and_releases_its_budget() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let (_dir, engine) = engine();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(1000))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,REPEAT('a',800)),(2,REPEAT('b',800)),(3,REPEAT('c',800))")
        .unwrap();
    let budget = Arc::new(MemBudget::new(1024));
    let result = with_query_budget(budget.clone(), || {
        engine.execute("DELETE FROM t RETURNING id,v")
    });
    assert_eq!(result.unwrap_err().code(), 1037);
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(3)]]
    );
    assert_eq!(budget.used(), 0);
}

#[test]
fn audit1122_invalid_ddl_job_ids_are_parse_errors() {
    for value in ["-1", "abc", "18446744073709551616", "1 trailing"] {
        assert_eq!(
            crate::parser::parse_statement(&format!("CANCEL DDL JOB {value}"))
                .unwrap_err()
                .spec
                .code(),
            1064
        );
    }
    assert!(matches!(
        crate::parser::parse_statement("CANCEL DDL JOB 42;").unwrap(),
        Statement::CancelDdlJob(42)
    ));
}

#[test]
fn audit1122_binlog_definers_and_dual_password_intent() {
    let (_dir, engine) = engine();
    let session = SessionContext::default();
    for sql in [
        "CREATE VIEW v AS SELECT 1",
        "CREATE SQL SECURITY INVOKER VIEW v AS SELECT 1",
        "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW SET @x=NEW.i",
        "CREATE PROCEDURE p() SELECT 1",
        "CREATE EVENT ev ON SCHEDULE EVERY 1 DAY DO SELECT 1",
    ] {
        let rewritten = credential_safe_binlog_sql(&engine.db, &session, sql).unwrap();
        assert!(
            rewritten.to_ascii_uppercase().contains("DEFINER="),
            "{rewritten}"
        );
        crate::parser::parse_statement(&rewritten).unwrap();
    }
    engine
        .execute("CREATE USER 'dual'@'%' IDENTIFIED BY 'Old!Pass1122'")
        .unwrap();
    let sql = "ALTER USER 'dual'@'%' IDENTIFIED BY 'New!Pass1122' RETAIN CURRENT PASSWORD";
    engine.execute(sql).unwrap();
    let rewritten = credential_safe_binlog_sql(&engine.db, &session, sql).unwrap();
    assert!(rewritten.contains("RETAIN CURRENT PASSWORD"));
    if let Some(options) = rewritten.find(" REQUIRE ") {
        assert!(rewritten.find(" RETAIN CURRENT PASSWORD").unwrap() < options);
    }
    assert!(!rewritten.contains("New!Pass1122"));
    for sql in [
        "ALTER USER 'dual'@'%' ACCOUNT LOCK",
        "ALTER USER 'dual'@'%' DISCARD OLD PASSWORD",
    ] {
        engine.execute(sql).unwrap();
        let rewritten = credential_safe_binlog_sql(&engine.db, &session, sql).unwrap();
        assert!(!rewritten.contains("IDENTIFIED"));
        if sql.contains("DISCARD") {
            assert!(rewritten.contains("DISCARD OLD PASSWORD"));
        }
    }
    assert!(engine
        .execute("ALTER USER 'dual'@'%' IDENTIFIED BY 'Another!1122' DISCARD OLD PASSWORD")
        .unwrap_err()
        .is(crate::error::catalog::ER_PARSE_ERROR));
}

#[test]
fn audit1122_failed_fetch_closes_cursor_and_releases_memory() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let (_dir, engine) = engine();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE fetch_t(i INT,v VARCHAR(1000))")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO fetch_t VALUES(1,REPEAT('a',800)),(2,REPEAT('b',800))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "DECLARE bounded_fetch CURSOR FOR SELECT * FROM fetch_t",
        )
        .unwrap();
    let budget = Arc::new(MemBudget::new(1024));
    let err = with_query_budget(budget.clone(), || {
        engine.execute_with_session(&mut session, "FETCH 2 FROM bounded_fetch")
    })
    .unwrap_err();
    assert_eq!(err.code(), 1037);
    assert_eq!(budget.used(), 0);
    assert_eq!(
        engine
            .execute_with_session(&mut session, "FETCH 1 FROM bounded_fetch")
            .unwrap_err()
            .code(),
        1326
    );
}

#[test]
fn audit1122_materialized_cursor_retains_charge_until_drop() {
    use crate::engine::executor::mem_budget;
    let original_mb = mem_budget::query_mem_limit_bytes()
        .div_ceil(1024 * 1024)
        .max(1);
    mem_budget::set_query_mem_limit_mb(1).unwrap();
    let baseline = mem_budget::controlled_memory_snapshot().0;
    let cursor =
        crate::engine::api::VecResultCursor::new_governed(vec![vec![Value::Bytes(vec![1; 8192])]])
            .unwrap();
    assert!(mem_budget::controlled_memory_snapshot().0 >= baseline + 8192);
    drop(cursor);
    assert_eq!(mem_budget::controlled_memory_snapshot().0, baseline);
    assert_eq!(
        crate::engine::api::VecResultCursor::new_governed(vec![vec![Value::Bytes(vec![
            1;
            2 * 1024
                * 1024
        ])]])
        .unwrap_err()
        .code(),
        1037
    );
    mem_budget::set_query_mem_limit_mb(original_mb).unwrap();
}

#[test]
fn audit1122_multi_value_index_qualifiers_preserve_join_ownership() {
    let (_dir, engine) = engine();
    engine
        .execute("CREATE TABLE tags_t(id INT PRIMARY KEY,tags JSON)")
        .unwrap();
    engine
        .execute("INSERT INTO tags_t VALUES(1,'[1]'),(2,'[2]'),(3,'[1,2]')")
        .unwrap();
    engine
        .execute("CREATE INDEX tags_i ON tags_t((CAST(tags AS UNSIGNED ARRAY)))")
        .unwrap();
    for source in ["tags", "tags_t.tags", "x.tags"] {
        let table = if source == "x.tags" {
            "tags_t x"
        } else {
            "tags_t"
        };
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT id FROM {table} WHERE 1 MEMBER OF ({source}) ORDER BY id")
            ),
            vec![vec![Value::Int(1)], vec![Value::Int(3)]]
        );
    }
    assert_eq!(rows(&engine,"SELECT x.id,y.id FROM tags_t x JOIN tags_t y ON x.id<y.id WHERE 1 MEMBER OF(x.tags) AND 2 MEMBER OF(y.tags) ORDER BY x.id,y.id"),vec![vec![Value::Int(1),Value::Int(2)],vec![Value::Int(1),Value::Int(3)]]);
}
