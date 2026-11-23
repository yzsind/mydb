use super::support::{execute_materialized as exec, result_rows};
use super::*;

fn setup() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine, SessionContext::default())
}

#[test]
fn trigger_release_1123_user_variable_integer_type() {
    let (_dir, e, mut s) = setup();
    for sql in [
        "CREATE TABLE t(a INT)",
        "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW BEGIN SET @n=IFNULL(@n,0)+1; END",
        "INSERT INTO t VALUES(1),(2)",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    assert!(matches!(s.user_variable("n"), Some(Value::Int(2))));
    let QueryResponse::ResultSet(rs) = exec(&e, &mut s, "SELECT @n").unwrap() else {
        panic!()
    };
    assert_eq!(rs.columns[0].column_type, ColumnType::LongLong);
}

#[test]
fn trigger_release_1123_body_comments_preserved() {
    let (_dir, e, mut s) = setup();
    exec(&e, &mut s, "CREATE TABLE t(a INT)").unwrap();
    let body =
        "BEGIN /* inline 注释 ' */ SET @c=1; -- line comment\n SET @c=@c+1; # hash comment\n END";
    exec(
        &e,
        &mut s,
        &format!("CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW {body}"),
    )
    .unwrap();
    let rows = result_rows(exec(&e, &mut s, "SHOW CREATE TRIGGER tr").unwrap());
    assert!(rows[0][2].to_display_string().ends_with(body), "{:?}", rows);
    let rows = result_rows(exec(&e, &mut s, "SELECT ACTION_STATEMENT FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA='test' AND TRIGGER_NAME='tr'").unwrap());
    assert_eq!(rows, vec![vec![Value::String(body.into())]]);
    exec(&e, &mut s, "INSERT INTO t VALUES(1)").unwrap();
    assert!(matches!(s.user_variable("c"), Some(Value::Int(2))));
}

#[test]
fn trigger_release_1123_created_fraction_preserved() {
    let (_dir, e, mut s) = setup();
    exec(&e, &mut s, "CREATE TABLE t(a INT)").unwrap();
    exec(
        &e,
        &mut s,
        "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET @c=1",
    )
    .unwrap();
    let rows = result_rows(exec(&e, &mut s, "SHOW CREATE TRIGGER tr").unwrap());
    let created = rows[0][6].to_display_string();
    assert!(created.contains('.'), "{created}");
    let rows = result_rows(exec(&e, &mut s, "SHOW TRIGGERS").unwrap());
    assert_eq!(rows[0][5].to_display_string(), created);
    let rows = result_rows(exec(&e, &mut s, "SELECT CREATED FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA='test' AND TRIGGER_NAME='tr'").unwrap());
    assert_eq!(rows[0][0].to_display_string(), created);
}

#[test]
fn trigger_release_1123_user_variable_domains_and_called_procedure() {
    let (_dir, e, mut s) = setup();
    for sql in [
        "CREATE TABLE t(u BIGINT UNSIGNED,d DECIMAL(12,2),v VARCHAR(20))",
        "CREATE PROCEDURE p() BEGIN SET @n=COALESCE(@n,0)+1; SET @n=@n+1; END",
        "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW BEGIN CALL p(); SET @u=NEW.u; SET @d=COALESCE(@d,NEW.d)+0.25; SET @f=IFNULL(@f,1e0)+5e-1; SET @v=COALESCE(@v,NEW.v); SET @z=COALESCE(@z,NULL); END",
        "INSERT INTO t VALUES(18446744073709551615,1.25,'value'),(1,2.25,'other')",
    ] { exec(&e, &mut s, sql).unwrap(); }
    assert!(matches!(s.user_variable("n"), Some(Value::Int(4))));
    assert!(matches!(
        s.user_variable("u"),
        Some(Value::Int(1) | Value::UInt(1))
    ));
    assert!(s.user_variable("d").unwrap().exact_decimal().is_some());
    assert_eq!(s.user_variable("d").unwrap().to_display_string(), "1.75");
    assert!(matches!(s.user_variable("f"), Some(Value::Double(v)) if *v == 2.0));
    assert_eq!(s.user_variable("v"), Some(&Value::String("value".into())));
    assert!(matches!(s.user_variable("z"), Some(Value::Null)));
    let QueryResponse::ResultSet(rs) =
        exec(&e, &mut s, "SELECT IFNULL(@unset,0)+1,COALESCE(@unset,0)+1").unwrap()
    else {
        panic!()
    };
    assert!(rs
        .columns
        .iter()
        .all(|c| c.column_type == ColumnType::LongLong));
    assert!(rs.rows[0].iter().all(|v| matches!(v, Value::Int(1))));
}

#[test]
fn trigger_release_1123_source_and_time_survive_reopen() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = StorageQueryEngine::open(dir.path()).unwrap();
    let mut s = SessionContext::default();
    let body = "BEGIN /* preserved */ SET NEW.a=NEW.a+1; END";
    exec(&e, &mut s, "CREATE TABLE t(a INT)").unwrap();
    exec(
        &e,
        &mut s,
        &format!("CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW {body}"),
    )
    .unwrap();
    let created = result_rows(exec(&e, &mut s, "SHOW CREATE TRIGGER tr").unwrap())[0][6].clone();
    drop(e);
    let e = StorageQueryEngine::open(dir.path()).unwrap();
    let rows = result_rows(exec(&e, &mut s, "SHOW CREATE TRIGGER tr").unwrap());
    assert!(rows[0][2].to_display_string().ends_with(body));
    assert_eq!(rows[0][6], created);
    exec(&e, &mut s, "INSERT INTO t VALUES(1)").unwrap();
    assert_eq!(
        result_rows(exec(&e, &mut s, "SELECT * FROM t").unwrap()),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn trigger_release_1123_created_precision_and_time_zone() {
    let (_dir, e, mut s) = setup();
    exec(&e, &mut s, "CREATE TABLE t(a INT)").unwrap();
    exec(
        &e,
        &mut s,
        "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET @c=1",
    )
    .unwrap();
    let mut trigger = e.database().trigger_definition("test", "tr").unwrap();
    trigger.created_ms = 1_700_000_000_129;
    exec(&e, &mut s, "DROP TRIGGER tr").unwrap();
    e.database()
        .create_trigger("test", trigger, false, None)
        .unwrap();
    for (zone, expected) in [
        ("+00:00", "2023-11-14 22:13:20.12"),
        ("+08:00", "2023-11-15 06:13:20.12"),
    ] {
        exec(&e, &mut s, &format!("SET time_zone='{zone}'")).unwrap();
        for (sql, index) in [("SHOW CREATE TRIGGER tr", 6), ("SHOW TRIGGERS", 5), ("SELECT CREATED FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA='test' AND TRIGGER_NAME='tr'", 0)] {
            let QueryResponse::ResultSet(rs) = exec(&e, &mut s, sql).unwrap() else { panic!() };
            assert_eq!(rs.rows[0][index].to_display_string(), expected, "{sql}");
            assert_eq!(rs.columns[index].source.decimals, Some(2), "{sql}");
        }
    }
    assert_eq!(
        crate::engine::information_schema::trigger_time_value(0),
        Value::Null
    );
}
