use super::*;

#[test]
fn audit_parser_rejections_have_no_write_effects() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT DEFAULT 7)").unwrap();
    for sql in [
        "INSERT INTO t VALUES(1),(2) LIMIT 1",
        "INSERT INTO t VALUES(DEFAULT(1+2))",
        "INSERT INTO t VALUES(__MYDB_EMPTY_ROW__)",
    ] {
        assert!(engine.execute(sql).is_err(), "{sql}");
    }
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT * FROM t").unwrap() else {
        panic!()
    };
    assert!(rs.rows.is_empty());
}

#[test]
fn audit_engine_batch_parses_with_preceding_session_mode() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE modes(x INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO modes VALUES(42)")
        .unwrap();
    for (sql, value) in [
        (
            "SET sql_mode='ANSI_QUOTES'; SELECT \"x\" FROM modes",
            Value::Int(42),
        ),
        (
            "SET sql_mode=''; SELECT \"x\" FROM modes",
            Value::String("x".into()),
        ),
        (
            "SET sql_mode='PIPES_AS_CONCAT'; SELECT 'a'||'b'",
            Value::String("ab".into()),
        ),
    ] {
        let QueryResponse::ResultSet(rs) = engine.execute_with_session(&mut session, sql).unwrap()
        else {
            panic!()
        };
        assert_eq!(rs.rows, vec![vec![value]], "{sql}");
    }
    assert!(engine
        .execute_with_session(&mut session, "SET sql_mode='ANSI_QUOTES'; SELECT +")
        .is_err());
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT \"x\" FROM modes")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(42)]]);
}

#[test]
fn audit_derived_aliases_bind_after_output_expansion() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(a INT, b INT)").unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    for sql in [
        "SELECT x,y FROM (SELECT * FROM t) d(x,y) ORDER BY x",
        "SELECT x,y FROM (SELECT a,b FROM t WHERE a=1 UNION ALL SELECT a,b FROM t WHERE a=2) d(x,y) ORDER BY x",
        "WITH c AS (SELECT * FROM t) SELECT x,y FROM (SELECT * FROM c) d(x,y) ORDER BY x",
        "SELECT d.x,d.y FROM t JOIN (SELECT * FROM t) d(x,y) ON t.a=d.x ORDER BY d.x",
        "SELECT x,y FROM (SELECT * FROM (SELECT * FROM t) d1(m,n)) d2(x,y) ORDER BY x",
    ] {
        let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}")) else { panic!() };
        assert_eq!(rs.rows,vec![vec![Value::Int(1),Value::Int(10)],vec![Value::Int(2),Value::Int(20)]],"{sql}");
        assert_eq!(rs.columns[0].name,"x");
        assert_eq!(rs.columns[1].name,"y");
    }
    for sql in [
        "SELECT * FROM (SELECT * FROM t) d(x)",
        "SELECT * FROM (SELECT * FROM t) d(x,x)",
    ] {
        assert!(engine.execute(sql).is_err(), "{sql}");
    }
}

#[test]
fn audit_partition_bounds_use_canonical_expression_parser() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE p(a INT) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN(+10), PARTITION p1 VALUES LESS THAN(10+10), PARTITION p2 VALUES LESS THAN(MAXVALUE))").unwrap();
    engine.execute("INSERT INTO p VALUES(1),(15),(25)").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT * FROM p PARTITION(p1)").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(15)]]);
    assert!(engine
        .execute(
            "CREATE TABLE badp(a INT) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN(a+10))"
        )
        .is_err());
}

#[test]
fn audit_quoted_routine_labels_and_cursor_names() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE PROCEDURE p() BEGIN `a b`: LOOP LEAVE `a b`; END LOOP `a b`; SET @done=42; END",
        )
        .unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CALL p()")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT @done")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(42)]]);
    assert!(engine
        .execute("CREATE PROCEDURE badp() BEGIN OPEN c trailing; END")
        .is_err());
}

#[test]
fn audit_nested_query_tails_preserve_each_level() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(a INT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES(1),(2),(3),(4)")
        .unwrap();
    for sql in [
        "(SELECT a FROM t ORDER BY a DESC LIMIT 3) ORDER BY a LIMIT 1",
        "SELECT * FROM ((SELECT a FROM t ORDER BY a DESC LIMIT 3) ORDER BY a LIMIT 1) d",
    ] {
        let QueryResponse::ResultSet(rs) = engine
            .execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        else {
            panic!()
        };
        assert_eq!(rs.rows, vec![vec![Value::Int(2)]], "{sql}");
    }
}

#[test]
fn audit_targeted_locks_preserve_table_modes_and_wait_policies() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE l(id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE r(id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO l VALUES(1,10),(2,20)").unwrap();
    engine.execute("INSERT INTO r VALUES(1,30),(2,40)").unwrap();
    let mut a = SessionContext::default();
    let mut b = SessionContext::default();
    engine.execute_with_session(&mut a, "BEGIN").unwrap();
    engine.execute_with_session(&mut b, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut a,
            "SELECT l.id FROM l JOIN r ON l.id=r.id WHERE l.id=1 FOR UPDATE OF l",
        )
        .unwrap();
    assert!(engine
        .execute_with_session(&mut b, "SELECT * FROM r WHERE id=1 FOR UPDATE NOWAIT")
        .is_ok());
    assert_eq!(
        engine
            .execute_with_session(&mut b, "SELECT * FROM l WHERE id=1 FOR UPDATE NOWAIT")
            .unwrap_err()
            .code(),
        3572
    );
    engine.execute_with_session(&mut a, "ROLLBACK").unwrap();
    engine.execute_with_session(&mut b, "ROLLBACK").unwrap();
    engine.execute_with_session(&mut a, "BEGIN").unwrap();
    engine.execute_with_session(&mut b, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut a,
            "SELECT l.id FROM l JOIN r ON l.id=r.id WHERE l.id=1 FOR SHARE OF l FOR UPDATE OF r",
        )
        .unwrap();
    assert!(engine
        .execute_with_session(&mut b, "SELECT * FROM l WHERE id=1 FOR SHARE NOWAIT")
        .is_ok());
    assert_eq!(
        engine
            .execute_with_session(&mut b, "SELECT * FROM r WHERE id=1 FOR SHARE NOWAIT")
            .unwrap_err()
            .code(),
        3572
    );
    let QueryResponse::ResultSet(rs) = engine.execute_with_session(&mut b,"SELECT l.id FROM l JOIN r ON l.id=r.id ORDER BY l.id LIMIT 1 FOR UPDATE OF l,r SKIP LOCKED").unwrap() else { panic!() };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
    engine.execute_with_session(&mut a, "ROLLBACK").unwrap();
    engine.execute_with_session(&mut b, "ROLLBACK").unwrap();
    for sql in [
        "SELECT * FROM l a FOR UPDATE OF l",
        "SELECT * FROM l FOR UPDATE OF missing",
        "SELECT * FROM l FOR SHARE OF l FOR UPDATE OF l",
        "SELECT * FROM l FOR UPDATE FOR SHARE OF l",
    ] {
        assert!(engine.execute(sql).is_err(), "{sql}");
    }
}

#[test]
fn audit_version_warning_and_unique_constraint_symbol() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for _ in 0..2 {
        engine
            .execute_with_session(&mut session, "SELECT /*!800001+*/ 2")
            .unwrap();
        let QueryResponse::ResultSet(rs) = engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap()
        else {
            panic!()
        };
        assert!(
            rs.rows
                .iter()
                .any(|row| row.get(1) == Some(&Value::Int(4164))),
            "{:?}",
            rs.rows
        );
    }
    engine
        .execute("CREATE TABLE uq(id INT, CONSTRAINT symbol_name UNIQUE(id))")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SHOW INDEX FROM uq").unwrap() else {
        panic!()
    };
    assert_eq!(rs.rows[0][2], Value::String("symbol_name".into()));
}

#[test]
fn audit_not_generated_expression_survives_catalog_reload() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE negated(a INT, g INT AS(!a * 2) VIRTUAL)")
            .unwrap();
        engine
            .execute("INSERT INTO negated(a) VALUES(0),(1)")
            .unwrap();
        let QueryResponse::ResultSet(rs) =
            engine.execute("SELECT g FROM negated ORDER BY a").unwrap()
        else {
            panic!()
        };
        assert_eq!(rs.rows, vec![vec![Value::Int(2)], vec![Value::Int(0)]]);
    }
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT g FROM negated ORDER BY a").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)], vec![Value::Int(0)]]);
}

#[test]
fn audit_sql_prepared_untyped_parameter_keeps_runtime_metadata() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for source in [
        "SELECT ? AS x ORDER BY x",
        "WITH c AS(SELECT ? AS x) SELECT x FROM c",
        "SELECT ANY_VALUE(?) AS x",
        "SELECT COALESCE(?) AS x",
        "SELECT (SELECT ?) AS x",
    ] {
        engine
            .execute_with_session(&mut session, &format!("PREPARE p FROM '{source}'"))
            .unwrap();
        engine
            .execute_with_session(&mut session, "SET @v=42")
            .unwrap();
        let QueryResponse::ResultSet(rs) = engine
            .execute_with_session(&mut session, "EXECUTE p USING @v")
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(rs.rows, vec![vec![Value::Int(42)]]);
        assert_eq!(rs.columns[0].column_type, ColumnType::LongLong, "{source}");
        engine
            .execute_with_session(&mut session, "SET @v='abc'")
            .unwrap();
        let QueryResponse::ResultSet(rs) = engine
            .execute_with_session(&mut session, "EXECUTE p USING @v")
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(rs.rows, vec![vec![Value::String("abc".into())]]);
        assert_eq!(rs.columns[0].column_type, ColumnType::VarString, "{source}");
        engine
            .execute_with_session(&mut session, "DEALLOCATE PREPARE p")
            .unwrap();
    }
}

#[test]
fn audit_sql_prepare_keeps_syntax_mode_and_diagnostics_snapshot() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='PIPES_AS_CONCAT'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "PREPARE p FROM 'SELECT ? || ?'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET sql_mode='', @a='left', @b='right'")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "EXECUTE p USING @a,@b")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::String("leftright".into())]]);
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!()
    };
    assert!(rs.rows.is_empty(), "{:?}", rs.rows);
}
