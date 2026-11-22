use super::support::{execute_materialized as exec, result_rows};
use super::*;

fn setup() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine, SessionContext::default())
}

#[test]
fn trigger_release_1122_implicit_commit_validation() {
    let (_dir, e, mut s) = setup();
    exec(&e, &mut s, "CREATE TABLE t(a INT)").unwrap();
    let mut failures = Vec::new();
    for body in [
        "CREATE TABLE z AS SELECT 1",
        "RENAME TABLE t TO renamed",
        "ALTER DATABASE test CHARACTER SET utf8mb4",
        "CREATE SEQUENCE seq",
        "ALTER SEQUENCE seq RESTART WITH 5",
        "DROP SEQUENCE seq",
        "CHECK TABLE t",
        "OPTIMIZE TABLE t",
        "REPAIR TABLE t",
        "CREATE EVENT ev ON SCHEDULE EVERY 1 DAY DO SET @a=1",
        "ALTER EVENT ev DISABLE",
        "DROP EVENT ev",
        "INSTALL COMPONENT 'file://component_validate_password'",
        "UNINSTALL COMPONENT 'file://component_validate_password'",
        "RESET MASTER",
        "XA START 'x'",
    ] {
        let sql = format!(
            "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW BEGIN IF 0 THEN {body}; END IF; END"
        );
        let result = exec(&e, &mut s, &sql);
        if !result.as_ref().is_err_and(|err| err.code() == 1422) {
            failures.push(format!("{body}: {result:?}"));
        }
        exec(&e, &mut s, "DROP TRIGGER IF EXISTS tr").unwrap();
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn trigger_release_1122_called_ddl_preserves_transaction() {
    let (_dir, e, mut s) = setup();
    for sql in [
        "CREATE TABLE t(a INT)",
        "CREATE TABLE u(b INT)",
        "INSERT INTO u VALUES(5)",
        "CREATE PROCEDURE p() CREATE TABLE z AS SELECT 1",
        "CREATE TRIGGER tr BEFORE UPDATE ON u FOR EACH ROW CALL p()",
        "START TRANSACTION",
        "INSERT INTO t VALUES(101)",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    let error = exec(&e, &mut s, "UPDATE u SET b=1").unwrap_err();
    exec(&e, &mut s, "ROLLBACK").unwrap();
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT * FROM t").unwrap()),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT * FROM u").unwrap()),
        vec![vec![Value::Int(5)]]
    );
    assert_eq!(error.code(), 1422);
}

#[test]
fn trigger_release_1122_generated_view_check_accepts_valid_row() {
    let (_dir, e, mut s) = setup();
    for sql in [
        "CREATE TABLE t(a INT, g INT GENERATED ALWAYS AS (a*2) STORED)",
        "CREATE VIEW v AS SELECT * FROM t WHERE g>10 WITH CHECK OPTION",
        "CREATE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW SET NEW.a=NEW.a+1",
        "INSERT INTO t(a) VALUES(6)",
        "UPDATE v SET a=7",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT * FROM t").unwrap()),
        vec![vec![Value::Int(8), Value::Int(16)]]
    );
}

#[test]
fn trigger_release_1122_generated_view_check_rejects_invalid_row() {
    let (_dir, e, mut s) = setup();
    for sql in [
        "CREATE TABLE t(a INT, g INT GENERATED ALWAYS AS (a*2) VIRTUAL)",
        "CREATE VIEW v AS SELECT * FROM t WHERE IFNULL(g,0)<10 WITH CHECK OPTION",
        "CREATE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW SET NEW.a=NEW.a",
        "INSERT INTO t(a) VALUES(4)",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    assert_eq!(
        exec(&e, &mut s, "UPDATE v SET a=8").unwrap_err().code(),
        1369
    );
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT * FROM t").unwrap()),
        vec![vec![Value::Int(4), Value::Int(8)]]
    );
}

#[test]
fn trigger_release_1122_self_writes_resolve_views_and_join_targets() {
    let mut failures = Vec::new();
    for body in [
        "UPDATE other,t SET t.id=t.id+100 WHERE other.id=t.id",
        "UPDATE other o JOIN t x ON o.id=x.id SET x.id=x.id+100",
        "UPDATE vt SET id=id+100",
        "UPDATE vv SET id=id+100",
        "DELETE x FROM other o JOIN t x ON o.id=x.id",
        "DELETE FROM vv",
        "INSERT INTO vv VALUES(9)",
    ] {
        let (_dir, e, mut s) = setup();
        for sql in [
            "CREATE TABLE t(id INT)",
            "CREATE TABLE other(id INT)",
            "INSERT INTO other VALUES(1)",
            "CREATE VIEW vt AS SELECT * FROM t",
            "CREATE VIEW vv AS SELECT * FROM vt",
        ] {
            exec(&e, &mut s, sql).unwrap();
        }
        exec(
            &e,
            &mut s,
            &format!("CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW {body}"),
        )
        .unwrap();
        let result = exec(&e, &mut s, "INSERT INTO t VALUES(1)");
        if !result.as_ref().is_err_and(|err| err.code() == 1442) {
            failures.push(format!("{body}: {result:?}"));
        } else {
            assert!(result_rows(exec(&e, &mut s, "SELECT * FROM t").unwrap()).is_empty());
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn trigger_release_1122_multi_update_generated_after_image() {
    let (_dir, e, mut s) = setup();
    for sql in [
        "CREATE TABLE a(id INT,v INT,g INT GENERATED ALWAYS AS(v*2) STORED)",
        "CREATE TABLE b(id INT)",
        "CREATE TABLE audit(old_g INT,new_g INT)",
        "CREATE TRIGGER bu BEFORE UPDATE ON a FOR EACH ROW SET NEW.v=NEW.v+1",
        "CREATE TRIGGER au AFTER UPDATE ON a FOR EACH ROW INSERT INTO audit VALUES(OLD.g,NEW.g)",
        "INSERT INTO a(id,v) VALUES(1,3)",
        "INSERT INTO b VALUES(1)",
        "UPDATE a,b SET a.v=4 WHERE a.id=b.id",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT * FROM audit").unwrap()),
        vec![vec![Value::Int(6), Value::Int(10)]]
    );
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT v,g FROM a").unwrap()),
        vec![vec![Value::Int(5), Value::Int(10)]]
    );
}

#[test]
fn trigger_release_1122_multi_update_noop_fires_after() {
    let (_dir, e, mut s) = setup();
    for sql in [
        "CREATE TABLE a(id INT,v INT)",
        "CREATE TABLE b(id INT)",
        "CREATE TABLE audit(v INT)",
        "CREATE TRIGGER au AFTER UPDATE ON a FOR EACH ROW INSERT INTO audit VALUES(NEW.v)",
        "INSERT INTO a VALUES(1,5)",
        "INSERT INTO b VALUES(1),(1)",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    let response = exec(&e, &mut s, "UPDATE a,b SET a.v=a.v WHERE a.id=b.id").unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 0 }
    ));
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT * FROM audit").unwrap()),
        vec![vec![Value::Int(5)]]
    );
}

#[test]
fn trigger_release_1122_drop_missing_note() {
    let (_dir, e, mut s) = setup();
    exec(&e, &mut s, "DROP TRIGGER IF EXISTS missing").unwrap();
    let rows = result_rows(exec(&e, &mut s, "SHOW WARNINGS").unwrap());
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0][0], Value::String("Note".into()));
    assert!(matches!(rows[0][1], Value::Int(1360) | Value::UInt(1360)));
    assert_eq!(
        exec(&e, &mut s, "DROP TRIGGER missing").unwrap_err().code(),
        1360
    );
}

#[test]
fn trigger_release_1122_name_length() {
    let (_dir, e, mut s) = setup();
    exec(&e, &mut s, "CREATE TABLE t(a INT)").unwrap();
    for name in ["a".repeat(64), "名".repeat(64)] {
        exec(
            &e,
            &mut s,
            &format!("CREATE TRIGGER test.`{name}` BEFORE INSERT ON t FOR EACH ROW SET NEW.a=1"),
        )
        .unwrap();
    }
    for name in ["a".repeat(65), "名".repeat(65)] {
        assert_eq!(
            exec(
                &e,
                &mut s,
                &format!(
                    "CREATE TRIGGER test.`{name}` BEFORE INSERT ON t FOR EACH ROW SET NEW.a=1"
                )
            )
            .unwrap_err()
            .code(),
            1059
        );
    }
}

#[test]
fn trigger_release_1122_binlog_default_definer() {
    let (_dir, e, mut s) = setup();
    s.authenticated_as = crate::engine::auth::AccountKey::new("trigger_owner", "%");
    for sql in [
        "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES(NEW.a)",
        "CREATE DEFINER=CURRENT_USER TRIGGER tr AFTER INSERT ON t FOR EACH ROW DO 1",
        "CREATE DEFINER=CURRENT_USER() TRIGGER tr AFTER INSERT ON t FOR EACH ROW DO 1",
        "CREATE /* CURRENT_USER */ DEFINER = CURRENT_USER ( ) TRIGGER tr AFTER INSERT ON t FOR EACH ROW DO 1",
        "CREATE DEFINER='other'@'localhost' TRIGGER tr AFTER INSERT ON t FOR EACH ROW DO 1",
    ] {
        let sql = credential_safe_binlog_sql(&e.db, &s, sql).unwrap();
        let Statement::CreateTrigger(def) = crate::parser::parse_statement(&sql).unwrap() else {
            panic!("{sql}")
        };
        let definer = def.definer.expect(&sql);
        assert_eq!(
            definer.user,
            if sql.contains("other") {
                "other"
            } else {
                "trigger_owner"
            }
        );
    }
}

#[test]
fn trigger_release_1122_multi_update_before_change_outside_set() {
    let (_dir, e, mut s) = setup();
    for sql in [
        "CREATE TABLE a(id INT,v INT,x INT,g INT GENERATED ALWAYS AS(x*2) STORED)",
        "CREATE TABLE b(id INT)",
        "CREATE TABLE audit(x INT,g INT)",
        "CREATE TRIGGER bu BEFORE UPDATE ON a FOR EACH ROW SET NEW.x=20",
        "CREATE TRIGGER au AFTER UPDATE ON a FOR EACH ROW INSERT INTO audit VALUES(NEW.x,NEW.g)",
        "INSERT INTO a(id,v,x) VALUES(1,5,10)",
        "INSERT INTO b VALUES(1)",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    let rows = result_rows(
        exec(
            &e,
            &mut s,
            "UPDATE a,b SET a.v=a.v WHERE a.id=b.id RETURNING NEW.x,NEW.g",
        )
        .unwrap(),
    );
    assert_eq!(rows, vec![vec![Value::Int(20), Value::Int(40)]]);
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT * FROM audit").unwrap()),
        rows
    );
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT x,g FROM a").unwrap()),
        rows
    );
}

#[test]
fn trigger_release_1122_multi_update_ignore_skips_after_and_returning() {
    let (_dir, e, mut s) = setup();
    for sql in [
        "CREATE TABLE a(id INT PRIMARY KEY,v INT UNIQUE)",
        "CREATE TABLE b(id INT)",
        "CREATE TABLE audit(id INT)",
        "CREATE TRIGGER au AFTER UPDATE ON a FOR EACH ROW INSERT INTO audit VALUES(NEW.id)",
        "INSERT INTO a VALUES(1,10),(2,20)",
        "INSERT INTO b VALUES(1)",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    assert!(result_rows(
        exec(
            &e,
            &mut s,
            "UPDATE IGNORE a,b SET a.v=20 WHERE a.id=b.id RETURNING a.*"
        )
        .unwrap()
    )
    .is_empty());
    assert!(result_rows(exec(&e, &mut s, "SELECT * FROM audit").unwrap()).is_empty());
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT v FROM a WHERE id=1").unwrap()),
        vec![vec![Value::Int(10)]]
    );
}

#[test]
fn trigger_release_1122_multi_update_noop_preserves_timestamp() {
    let (_dir, e, mut s) = setup();
    for sql in [
        "CREATE TABLE a(id INT,v INT,ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
        "CREATE TABLE b(id INT)",
        "CREATE TABLE audit(ts TIMESTAMP)",
        "CREATE TRIGGER au AFTER UPDATE ON a FOR EACH ROW INSERT INTO audit VALUES(NEW.ts)",
        "INSERT INTO a VALUES(1,5,'2000-01-01 00:00:00')",
        "INSERT INTO b VALUES(1)",
    ] { exec(&e, &mut s, sql).unwrap(); }
    let old = result_rows(exec(&e, &mut s, "SELECT ts FROM a").unwrap());
    let rows = result_rows(
        exec(
            &e,
            &mut s,
            "UPDATE a,b SET a.v=a.v WHERE a.id=b.id RETURNING NEW.ts",
        )
        .unwrap(),
    );
    assert_eq!(rows, old);
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT * FROM audit").unwrap()),
        old
    );
}

#[test]
fn trigger_release_1122_join_read_only_subject_is_allowed() {
    let (_dir, e, mut s) = setup();
    for sql in [
        "CREATE TABLE t(id INT)",
        "CREATE TABLE other(k INT,v INT)",
        "INSERT INTO other VALUES(1,10)",
        "CREATE TRIGGER ai AFTER INSERT ON t FOR EACH ROW UPDATE t JOIN other o ON t.id=o.k SET v=v+1",
        "INSERT INTO t VALUES(1)",
    ] { exec(&e, &mut s, sql).unwrap(); }
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT v FROM other").unwrap()),
        vec![vec![Value::Int(11)]]
    );
}

#[test]
fn trigger_release_1122_indirect_subject_write_rolls_back() {
    let (_dir, e, mut s) = setup();
    for sql in [
        "CREATE TABLE a(id INT)",
        "CREATE TABLE b(id INT)",
        "CREATE TRIGGER ai AFTER INSERT ON a FOR EACH ROW INSERT INTO b VALUES(NEW.id)",
        "CREATE TRIGGER bi AFTER INSERT ON b FOR EACH ROW DELETE FROM a WHERE id=NEW.id",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    assert_eq!(
        exec(&e, &mut s, "INSERT INTO a VALUES(1)")
            .unwrap_err()
            .code(),
        1442
    );
    for sql in ["SELECT * FROM a", "SELECT * FROM b"] {
        assert!(result_rows(exec(&e, &mut s, sql).unwrap()).is_empty());
    }
    exec(&e, &mut s, "DROP TRIGGER bi").unwrap();
    exec(&e, &mut s, "INSERT INTO a VALUES(1)").unwrap();
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT * FROM b").unwrap()),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn trigger_release_1122_depth_limit_rolls_back_and_restores_context() {
    let (_dir, e, mut s) = setup();
    for index in 0..34 {
        exec(&e, &mut s, &format!("CREATE TABLE c{index}(id INT)")).unwrap();
        if index > 0 {
            let prev = index - 1;
            exec(&e, &mut s, &format!("CREATE TRIGGER tr{prev} AFTER INSERT ON c{prev} FOR EACH ROW INSERT INTO c{index} VALUES(NEW.id)")).unwrap();
        }
    }
    assert_eq!(
        exec(&e, &mut s, "INSERT INTO c0 VALUES(1)")
            .unwrap_err()
            .code(),
        1456
    );
    for index in 0..34 {
        assert!(
            result_rows(exec(&e, &mut s, &format!("SELECT * FROM c{index}")).unwrap()).is_empty()
        );
    }
    exec(&e, &mut s, "INSERT INTO c1 VALUES(2)").unwrap();
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT * FROM c33").unwrap()),
        vec![vec![Value::Int(2)]]
    );
}
