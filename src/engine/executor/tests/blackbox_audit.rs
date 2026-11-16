use super::support::execute_materialized as exec;
use super::*;

#[test]
fn blackbox_text_markers_cannot_borrow_auto_parameters() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,a INT)",
        "INSERT INTO t VALUES(1,4),(2,3)",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    for sql in [
        "SELECT id FROM t WHERE a=? LIMIT 4",
        "SELECT ?",
        "SELECT id FROM t LIMIT ?",
        "SELECT * FROM (SELECT ? FROM t) d LIMIT 4",
        "WITH c AS (SELECT ? FROM t) SELECT * FROM c LIMIT 4",
        "SELECT 1; SELECT id FROM t WHERE a=? LIMIT 4",
        "UPDATE t SET a=? WHERE id=1",
        "INSERT INTO t VALUES(3,?)",
    ] {
        assert_eq!(
            exec(&engine, &mut session, sql).unwrap_err().code(),
            1064,
            "{sql}"
        );
    }
    exec(
        &engine,
        &mut session,
        "PREPARE p FROM 'SELECT id FROM t WHERE a=? LIMIT ?'",
    )
    .unwrap();
    exec(&engine, &mut session, "SET @a=4,@n=1").unwrap();
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "EXECUTE p USING @a,@n").unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    exec(
        &engine,
        &mut session,
        "SELECT '?',id FROM t WHERE a=4 LIMIT 1 /* ? */",
    )
    .unwrap();
    for sql in [
        "SELECT CAST('{}' AS JSON)->?",
        "SELECT CAST('{}' AS JSON)->>?",
    ] {
        assert_eq!(parse_statement(sql).unwrap_err().code(), 1064);
    }
    exec(&engine, &mut session, "SELECT JSON_OBJECT('a',4)->'$.a'=4").unwrap();
}

#[test]
fn blackbox_renamed_trigger_column_fails_atomically() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(a INT)",
        "CREATE TABLE audit_log(a INT)",
        "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW INSERT INTO audit_log VALUES(NEW.a)",
        "ALTER TABLE t CHANGE COLUMN a b INT",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    assert_eq!(
        exec(&engine, &mut session, "INSERT INTO t VALUES(42)")
            .unwrap_err()
            .code(),
        1054
    );
    for table in ["t", "audit_log"] {
        let QueryResponse::ResultSet(rs) =
            exec(&engine, &mut session, &format!("SELECT * FROM {table}")).unwrap()
        else {
            panic!("rows")
        };
        assert!(rs.rows.is_empty(), "{table}: {:?}", rs.rows);
    }
    exec(&engine, &mut session, "DROP TRIGGER tr").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW INSERT INTO audit_log VALUES(NEW.b)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES(42)").unwrap();
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT * FROM audit_log").unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(42)]]);
}

#[test]
fn blackbox_mbr_contains_distinguishes_boundary_and_covering() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rectangle = "ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))')";
    for (geometry, contained, covered) in [
        ("POINT(1 1)", 1, 1),
        ("POINT(0 1)", 0, 1),
        ("POINT(2 2)", 0, 1),
        ("POINT(3 1)", 0, 0),
        ("LINESTRING(0 0,0 2)", 0, 1),
        ("LINESTRING(0 1,2 1)", 1, 1),
        ("POLYGON((0 0,2 0,2 2,0 2,0 0))", 1, 1),
    ] {
        let other = format!("ST_GeomFromText('{geometry}')");
        let sql = format!("SELECT MBRContains({rectangle},{other}),MBRWithin({other},{rectangle}),MBRCovers({rectangle},{other}),MBRCoveredBy({other},{rectangle})");
        let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, &sql).unwrap() else {
            panic!("rows")
        };
        assert_eq!(
            rs.rows,
            vec![vec![
                Value::Int(contained),
                Value::Int(contained),
                Value::Int(covered),
                Value::Int(covered)
            ]],
            "{geometry}"
        );
    }
}

#[test]
fn blackbox_float_cast_checks_target_width_and_finiteness() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(v DOUBLE)",
        "INSERT INTO t VALUES(1e308),(-1e308)",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    for sql in [
        "SELECT CONVERT(1e308,FLOAT)",
        "SELECT CONVERT(-1e308,FLOAT)",
        "SELECT CONVERT('1.8e308',FLOAT)",
        "SELECT CAST(v AS FLOAT) FROM t",
        "SELECT FORMAT(CAST(v AS FLOAT),2) FROM t",
    ] {
        assert_eq!(
            exec(&engine, &mut session, sql).unwrap_err().code(),
            1690,
            "{sql}"
        );
    }
    exec(
        &engine,
        &mut session,
        "PREPARE f FROM 'SELECT CAST(? AS FLOAT)'",
    )
    .unwrap();
    exec(&engine, &mut session, "SET @n=1e308").unwrap();
    assert_eq!(
        exec(&engine, &mut session, "EXECUTE f USING @n")
            .unwrap_err()
            .code(),
        1690
    );
    exec(
        &engine,
        &mut session,
        "SELECT CAST(3e38 AS FLOAT),CAST(NULL AS FLOAT),CAST(1e308 AS DOUBLE)",
    )
    .unwrap();
}

#[test]
fn blackbox_physical_replica_reports_effective_read_only() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine.db.set_physical_replica_mode(true).unwrap();
    let mut session = SessionContext::default();
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT @@global.read_only,@@global.super_read_only",
    )
    .unwrap() else {
        panic!("rows")
    };
    assert_eq!(
        rs.rows[0]
            .iter()
            .map(Value::to_display_string)
            .collect::<Vec<_>>(),
        ["1", "1"]
    );
    assert!(exec(&engine, &mut session, "SET GLOBAL read_only=OFF").is_err());
    engine.db.set_physical_replica_mode(false).unwrap();
}
