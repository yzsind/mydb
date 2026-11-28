use super::support::execute_materialized as exec;
use super::*;

fn fixture() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine, SessionContext::default())
}

fn rows(e: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    match exec(e, s, sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("expected rows for {sql}: {other:?}"),
    }
}

#[test]
fn audit1127_generated_trigger_assignment_is_ignored_before_write() {
    let (_dir, e, mut s) = fixture();
    for sql in [
        "CREATE TABLE g(a INT,b INT AS(a*2) STORED,c INT AS(a+1) VIRTUAL)",
        "CREATE TRIGGER bi BEFORE INSERT ON g FOR EACH ROW BEGIN SET NEW.b=99; SET NEW.c=99; SET NEW.a=NEW.a+1; END",
        "CREATE TRIGGER bu BEFORE UPDATE ON g FOR EACH ROW BEGIN SET NEW.b=99; SET NEW.c=99; SET NEW.a=NEW.a+1; END",
        "INSERT INTO g(a) VALUES(2)",
    ] { exec(&e, &mut s, sql).unwrap(); }
    assert_eq!(
        rows(&e, &mut s, "SELECT * FROM g"),
        vec![vec![Value::Int(3), Value::Int(6), Value::Int(4)]]
    );
    exec(&e, &mut s, "UPDATE g SET a=5").unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT * FROM g"),
        vec![vec![Value::Int(6), Value::Int(12), Value::Int(7)]]
    );
    assert_eq!(
        exec(&e, &mut s, "INSERT INTO g(a,b) VALUES(2,99)")
            .unwrap_err()
            .code(),
        3105
    );
}

#[test]
fn audit1127_odku_ignore_skips_check_failure() {
    for triggers in [false, true] {
        let (_dir, e, mut s) = fixture();
        for sql in [
            "CREATE TABLE t(id INT PRIMARY KEY,x INT)",
            "CREATE TABLE log(v INT)",
            "INSERT INTO t VALUES(1,10),(2,20),(3,30)",
            "CREATE VIEW v AS SELECT * FROM t WHERE x>0 WITH CHECK OPTION",
        ] {
            exec(&e, &mut s, sql).unwrap();
        }
        if triggers {
            exec(
                &e,
                &mut s,
                "CREATE TRIGGER bu BEFORE UPDATE ON t FOR EACH ROW INSERT INTO log VALUES(NEW.x)",
            )
            .unwrap();
        }
        exec(&e, &mut s, "UPDATE IGNORE v SET x=x-15").unwrap();
        exec(
            &e,
            &mut s,
            "INSERT IGNORE INTO v VALUES(2,99),(3,99) ON DUPLICATE KEY UPDATE x=x-10",
        )
        .unwrap();
        let warnings = rows(&e, &mut s, "SHOW WARNINGS");
        assert!(
            warnings.iter().any(|r| r[1] == Value::Int(1369)),
            "{warnings:?}"
        );
        assert_eq!(
            rows(&e, &mut s, "SELECT * FROM t ORDER BY id"),
            vec![
                vec![Value::Int(1), Value::Int(10)],
                vec![Value::Int(2), Value::Int(5)],
                vec![Value::Int(3), Value::Int(5)]
            ]
        );
    }
}

#[test]
fn audit1127_dml_rejects_unknown_qualifiers_before_scanning() {
    let (_dir, e, mut s) = fixture();
    exec(&e, &mut s, "CREATE TABLE t(id INT PRIMARY KEY,v INT)").unwrap();
    for populated in [false, true] {
        if populated {
            exec(&e, &mut s, "INSERT INTO t VALUES(1,10)").unwrap();
        }
        for sql in [
            "UPDATE t SET v=NEW.v",
            "UPDATE t SET v=bogus.v",
            "DELETE FROM t WHERE v=OLD.v",
            "INSERT INTO t VALUES(1,20) ON DUPLICATE KEY UPDATE v=NEW.v",
        ] {
            assert_eq!(exec(&e, &mut s, sql).unwrap_err().code(), 1054, "{sql}");
        }
    }
    exec(
        &e,
        &mut s,
        "INSERT INTO t VALUES(1,30) AS new ON DUPLICATE KEY UPDATE v=new.v+1",
    )
    .unwrap();
    exec(&e, &mut s, "UPDATE t AS old SET v=old.v+1").unwrap();
    exec(&e, &mut s, "CREATE TABLE source_rows(id INT,v INT)").unwrap();
    exec(&e, &mut s, "INSERT INTO source_rows VALUES(1,32)").unwrap();
    exec(
        &e,
        &mut s,
        "INSERT INTO t SELECT id,v FROM source_rows AS new ON DUPLICATE KEY UPDATE v=new.v",
    )
    .unwrap();
    assert_eq!(
        exec(
            &e,
            &mut s,
            "INSERT INTO t SELECT id,v FROM source_rows ON DUPLICATE KEY UPDATE v=NEW.v"
        )
        .unwrap_err()
        .code(),
        1054
    );

    assert_eq!(
        rows(&e, &mut s, "SELECT v FROM t"),
        vec![vec![Value::Int(32)]]
    );
}

#[test]
fn audit1127_catalog_object_keys_do_not_collide_and_survive_reopen() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_strict_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE DATABASE `x:a`",
        "CREATE DATABASE x",
        "CREATE TABLE `x:a`.t(i INT)",
        "CREATE TABLE x.t(i INT)",
        "CREATE TRIGGER `x:a`.b BEFORE INSERT ON `x:a`.t FOR EACH ROW SET @one=1",
        "CREATE TRIGGER x.`a:b` BEFORE INSERT ON x.t FOR EACH ROW SET @two=2",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    exec(&e, &mut s, "DROP TRIGGER x.`a:b`").unwrap();
    assert_eq!(
        rows(
            &e,
            &mut s,
            "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA='x:a'"
        ),
        vec![vec![Value::String("b".into())]]
    );
    drop(e);
    let e = open_strict_test_engine(dir.path());
    exec(&e, &mut s, "INSERT INTO `x:a`.t VALUES(1)").unwrap();
    assert_eq!(rows(&e, &mut s, "SELECT @one"), vec![vec![Value::Int(1)]]);
}

#[test]
fn audit1127_mvi_prefix_preserves_numeric_comparison() {
    let (_dir, e, mut s) = fixture();
    for sql in ["CREATE TABLE t(id INT PRIMARY KEY,vc VARCHAR(10),j JSON,KEY m(vc,(CAST(j AS UNSIGNED ARRAY))))",r#"INSERT INTO t VALUES(1,'01','[7]'),(2,'1','[7]'),(3,'1.0','[7]'),(4,'2','[7]')"#] {exec(&e,&mut s,sql).unwrap();}
    let expected = rows(
        &e,
        &mut s,
        "SELECT id FROM t IGNORE INDEX(m) WHERE vc=1 AND 7 MEMBER OF(j) ORDER BY id",
    );
    assert_eq!(
        rows(
            &e,
            &mut s,
            "SELECT id FROM t WHERE vc=1 AND 7 MEMBER OF(j) ORDER BY id"
        ),
        expected
    );
}

#[test]
fn audit1127_json_table_defaults_use_sql_literal_values() {
    let (_dir, e, mut s) = fixture();
    assert_eq!(
        rows(
            &e,
            &mut s,
            r#"SELECT * FROM JSON_TABLE('{}','$' COLUMNS(a VARCHAR(20) PATH '$.a' DEFAULT 'null' ON EMPTY,b VARCHAR(20) PATH '$.b' DEFAULT '"ab"' ON EMPTY)) jt"#
        ),
        vec![vec![
            Value::String("null".into()),
            Value::String("ab".into())
        ]]
    );
    assert_eq!(
        rows(
            &e,
            &mut s,
            r#"SELECT * FROM JSON_TABLE('{}','$' COLUMNS(a VARBINARY(10) PATH '$.a' DEFAULT 'null' ON EMPTY,b VARCHAR(10) PATH '$.b' DEFAULT 'true' ON EMPTY)) jt"#
        ),
        vec![vec![
            Value::Bytes(b"null".to_vec()),
            Value::String("true".into())
        ]]
    );
    assert_eq!(exec(&e,&mut s,r#"SELECT * FROM JSON_TABLE('{}','$' COLUMNS(a INT PATH '$.a' DEFAULT 'null' ON EMPTY)) jt"#).unwrap_err().code(),3156);
    assert_eq!(exec(&e,&mut s,r#"SELECT * FROM JSON_TABLE('{}','$' COLUMNS(a DATE PATH '$.a' DEFAULT 'null' ON EMPTY)) jt"#).unwrap_err().code(),3669);
    let json = rows(
        &e,
        &mut s,
        r#"SELECT * FROM JSON_TABLE('{}','$' COLUMNS(a JSON PATH '$.a' DEFAULT 'null' ON EMPTY)) jt"#,
    );
    assert!(matches!(&json[0][0], Value::Json(_)));
}

#[test]
fn audit1127_mvi_object_cast_reports_unsupported() {
    let (_dir, e, mut s) = fixture();
    exec(
        &e,
        &mut s,
        "CREATE TABLE t(id INT PRIMARY KEY,j JSON,KEY m((CAST(j AS UNSIGNED ARRAY))))",
    )
    .unwrap();
    assert_eq!(
        exec(&e, &mut s, r#"INSERT INTO t VALUES(1,'{"a":1}')"#)
            .unwrap_err()
            .code(),
        1235
    );
    assert!(rows(&e, &mut s, "SELECT * FROM t").is_empty());
}

#[test]
fn audit1127_mixed_drop_quotes_names_and_propagates_binlog_io_errors() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_strict_test_engine(dir.path());
    let binlog = dir.path().join("binlog");
    crate::binlog::init_global(
        dir.path().into(),
        crate::binlog::BinlogConfig {
            enabled: true,
            ..Default::default()
        },
    )
    .unwrap();
    struct Reset(std::path::PathBuf);
    impl Drop for Reset {
        fn drop(&mut self) {
            let _ = crate::binlog::init_global(self.0.clone(), Default::default());
        }
    }
    let _reset = Reset(dir.path().into());
    let mut s = SessionContext::default();
    exec(&e, &mut s, "CREATE TABLE `a``b`(id INT)").unwrap();
    exec(&e, &mut s, "CREATE TEMPORARY TABLE tmp(id INT)").unwrap();
    exec(&e, &mut s, "DROP TABLE `a``b`,tmp").unwrap();
    let events = rows(&e, &mut s, "SHOW BINLOG EVENTS");
    assert!(
        events.iter().any(|row| row
            .last()
            .unwrap()
            .to_display_string()
            .contains("DROP TABLE `test`.`a``b`")),
        "{events:?}"
    );
    exec(&e, &mut s, "CREATE TABLE persistent_drop(id INT)").unwrap();
    exec(&e, &mut s, "CREATE TEMPORARY TABLE tmp(id INT)").unwrap();
    let manifest = binlog.join("manifest.json");
    let saved = binlog.join("manifest.saved");
    std::fs::rename(&manifest, &saved).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let result = exec(&e, &mut s, "DROP TABLE persistent_drop,tmp");
    std::fs::remove_dir(&manifest).unwrap();
    std::fs::rename(&saved, &manifest).unwrap();
    assert_eq!(result.unwrap_err().code(), 1026);
}
