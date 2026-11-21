use super::support::execute_materialized as exec;
use super::*;

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    match exec(engine, session, sql).unwrap() {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("{sql}: {other:?}"),
    }
}

#[test]
fn release1121_view_trigger_checks_final_insert_image_and_ignore() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(a INT PRIMARY KEY, b INT)",
        "CREATE VIEW v AS SELECT a,b FROM t WHERE b>10 WITH CHECK OPTION",
        "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW SET NEW.b=NEW.b-10",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    assert_eq!(
        exec(&engine, &mut s, "INSERT INTO v VALUES(1,15)")
            .unwrap_err()
            .code(),
        1369
    );
    assert!(rows(&engine, &mut s, "SELECT * FROM t").is_empty());
    assert_eq!(
        exec(&engine, &mut s, "INSERT INTO v VALUES(1,30),(2,15)")
            .unwrap_err()
            .code(),
        1369
    );
    assert!(rows(&engine, &mut s, "SELECT * FROM t").is_empty());
    exec(&engine, &mut s, "INSERT IGNORE INTO v VALUES(1,30),(2,15)").unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT * FROM t"),
        vec![vec![Value::Int(1), Value::Int(20)]]
    );
    exec(&engine, &mut s, "DROP TRIGGER bi").unwrap();
    exec(
        &engine,
        &mut s,
        "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW SET NEW.b=20",
    )
    .unwrap();
    exec(&engine, &mut s, "INSERT INTO v VALUES(3,0)").unwrap();
}

#[test]
fn release1121_view_replace_guards_all_unique_conflicts_and_atomicity() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY, u INT UNIQUE, visible INT)",
        "INSERT INTO t VALUES(1,10,0),(2,20,1)",
        "CREATE VIEW v AS SELECT id,u,visible FROM t WHERE visible=1",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    let original = rows(&engine, &mut s, "SELECT * FROM t ORDER BY id");
    for sql in [
        "REPLACE INTO v VALUES(1,30,1)",
        "REPLACE INTO v VALUES(3,10,1)",
        "REPLACE INTO v VALUES(2,20,1),(3,10,1)",
    ] {
        assert_eq!(
            exec(&engine, &mut s, sql).unwrap_err().code(),
            3059,
            "{sql}"
        );
        assert_eq!(
            rows(&engine, &mut s, "SELECT * FROM t ORDER BY id"),
            original
        );
    }
    exec(&engine, &mut s, "REPLACE INTO v VALUES(2,20,0)").unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT COUNT(*) FROM v"),
        vec![vec![Value::Int(0)]]
    );
}

#[test]
fn release1121_view_computed_columns_are_readable_and_real_columns_writable() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(a INT PRIMARY KEY,b INT DEFAULT 10)",
        "INSERT INTO t VALUES(1,10),(2,20)",
        "CREATE VIEW v AS SELECT a,b+1 AS bb FROM t",
        "CREATE VIEW w AS SELECT a,bb+1 AS cc FROM v",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    assert_eq!(rows(&engine,&mut s,"SELECT is_updatable FROM information_schema.views WHERE table_schema='test' AND table_name='v'"),vec![vec![Value::String("YES".into())]]);
    exec(&engine, &mut s, "UPDATE v SET a=a+10 WHERE bb=11").unwrap();
    exec(&engine, &mut s, "UPDATE w SET a=a+10 WHERE cc=12").unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT a FROM t ORDER BY a"),
        vec![vec![Value::Int(2)], vec![Value::Int(21)]]
    );
    for sql in [
        "UPDATE v SET bb=7",
        "INSERT INTO v VALUES(3,7)",
        "INSERT INTO v(bb) VALUES(7)",
    ] {
        assert_eq!(
            exec(&engine, &mut s, sql).unwrap_err().code(),
            1348,
            "{sql}"
        );
    }
    exec(&engine, &mut s, "INSERT INTO v(a) VALUES(3)").unwrap();
    exec(&engine, &mut s, "DELETE FROM w WHERE cc=12").unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT * FROM t"),
        vec![vec![Value::Int(2), Value::Int(20)]]
    );
}

#[test]
fn release1121_multitable_dml_accepts_read_only_view_sources() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,n INT)",
        "INSERT INTO t VALUES(1,2),(2,3)",
        "CREATE VIEW agg AS SELECT SUM(n) AS total FROM t",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    exec(
        &engine,
        &mut s,
        "UPDATE agg JOIN t ON agg.total=5 SET t.n=t.n+10",
    )
    .unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT SUM(n) FROM t")[0][0].to_display_string(),
        "25"
    );
    exec(
        &engine,
        &mut s,
        "DELETE t FROM agg JOIN t ON agg.total=25 WHERE t.id=1",
    )
    .unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT id FROM t"),
        vec![vec![Value::Int(2)]]
    );
    assert_eq!(
        exec(&engine, &mut s, "UPDATE agg JOIN t ON 1=1 SET agg.total=10")
            .unwrap_err()
            .code(),
        1288
    );
}

#[test]
fn release1121_view_maintenance_checks_definition_and_object_kind() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE t(a INT)").unwrap();
    exec(&engine, &mut s, "CREATE VIEW v AS SELECT a FROM t").unwrap();
    assert_eq!(
        rows(&engine, &mut s, "CHECK TABLE v")[0][3],
        Value::String("OK".into())
    );
    exec(&engine, &mut s, "FLUSH TABLES v").unwrap();
    for sql in ["ANALYZE TABLE v", "OPTIMIZE TABLE v", "REPAIR TABLE v"] {
        assert!(
            rows(&engine, &mut s, sql)[0][3]
                .to_display_string()
                .contains("not BASE TABLE"),
            "{sql}"
        );
    }
    exec(&engine, &mut s, "DROP TABLE t").unwrap();
    assert_eq!(
        exec(&engine, &mut s, "CHECK TABLE v").unwrap_err().code(),
        1356
    );
}

#[test]
fn release1121_routine_prepare_fetch_assignment_and_ddl_diagnostics() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(
        &engine,
        &mut s,
        "CREATE PROCEDURE p() BEGIN PREPARE x FROM 'SELECT * FROM missing'; END",
    )
    .unwrap();
    assert_eq!(exec(&engine, &mut s, "CALL p()").unwrap_err().code(), 1146);
    for (sql,code) in [
        ("CREATE PROCEDURE bad_cursor() BEGIN OPEN c; END",1324),
        ("CREATE PROCEDURE bad_fetch() BEGIN DECLARE c CURSOR FOR SELECT 1; FETCH c INTO missing; END",1327),
        ("CREATE FUNCTION missing_return() RETURNS INT BEGIN SET @x=1; END",1320),
        ("CREATE PROCEDURE bad_order() BEGIN DECLARE CONTINUE HANDLER FOR NOT FOUND SET @x=1; DECLARE c CURSOR FOR SELECT 1; END",1338)] {
        assert_eq!(exec(&engine,&mut s,sql).unwrap_err().code(),code,"{sql}");
    }
    exec(&engine,&mut s,"CREATE PROCEDURE fetch_count() BEGIN DECLARE x,y INT; DECLARE c CURSOR FOR SELECT 1 WHERE 0; DECLARE CONTINUE HANDLER FOR NOT FOUND SET @caught=1; OPEN c; FETCH c INTO x,y; END").unwrap();
    assert_eq!(
        exec(&engine, &mut s, "CALL fetch_count()")
            .unwrap_err()
            .code(),
        1328
    );
    exec(
        &engine,
        &mut s,
        "SET sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO'",
    )
    .unwrap();
    exec(&engine,&mut s,"CREATE PROCEDURE strict_assign() BEGIN DECLARE x INT DEFAULT 7; DECLARE CONTINUE HANDLER FOR 1365 SET @preserved=x; SET x=1/0; SET @after=x; END").unwrap();
    exec(&engine, &mut s, "CALL strict_assign()").unwrap();
    assert_eq!(s.user_variable("preserved"), Some(&Value::Int(7)));
    assert_eq!(s.user_variable("after"), Some(&Value::Int(7)));
    exec(
        &engine,
        &mut s,
        "CREATE PROCEDURE long_signal() SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT=REPEAT('x',513)",
    )
    .unwrap();
    assert_eq!(
        exec(&engine, &mut s, "CALL long_signal()")
            .unwrap_err()
            .code(),
        1648
    );
    exec(&engine,&mut s,"CREATE PROCEDURE empty_diag() BEGIN SET @m='unchanged'; GET CURRENT DIAGNOSTICS CONDITION 1 @m=MESSAGE_TEXT; END").unwrap();
    exec(&engine, &mut s, "CALL empty_diag()").unwrap();
    assert_eq!(
        s.user_variable("m"),
        Some(&Value::String("unchanged".into()))
    );
}

#[test]
fn release1121_routine_status_lists_all_schemas_and_function_reads_survive_replica_mode() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE DATABASE other").unwrap();
    exec(&engine, &mut s, "CREATE PROCEDURE other.p() SET @x=1").unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SHOW PROCEDURE STATUS WHERE Db='other'").len(),
        1
    );
    exec(&engine, &mut s, "CREATE TABLE t(id INT)").unwrap();
    exec(&engine, &mut s, "INSERT INTO t VALUES(1)").unwrap();
    exec(
        &engine,
        &mut s,
        "CREATE FUNCTION read_f() RETURNS INT DETERMINISTIC RETURN (SELECT COUNT(*) FROM t)",
    )
    .unwrap();
    exec(&engine,&mut s,"CREATE FUNCTION write_f() RETURNS INT MODIFIES SQL DATA BEGIN INSERT INTO t VALUES(2); RETURN 2; END").unwrap();
    engine.db.set_physical_replica_mode(true).unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT read_f()"),
        vec![vec![Value::Int(1)]]
    );
    assert!(exec(&engine, &mut s, "SELECT write_f()").is_err());
    assert_eq!(
        rows(&engine, &mut s, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(1)]]
    );
    engine.db.set_physical_replica_mode(false).unwrap();
}

#[test]
fn release1121_routine_call_retries_only_blocked_child_in_both_transaction_modes() {
    for (explicit, dynamic) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(engine_with_lock_wait_ms(dir.path(), 5000));
        let mut owner = SessionContext::default();
        owner.connection_id = 100;
        for sql in [
            "CREATE TABLE t(id INT PRIMARY KEY,n INT)",
            "INSERT INTO t VALUES(1,0)",
            "CREATE TABLE audit(id INT AUTO_INCREMENT PRIMARY KEY,n INT)",
            if dynamic {
                "CREATE PROCEDURE p(IN key_value INT) BEGIN INSERT INTO audit(n) VALUES(1); PREPARE q FROM 'UPDATE t SET n=n+1 WHERE id=(0*0)+?'; EXECUTE q USING key_value; DEALLOCATE PREPARE q; END"
            } else {
                "CREATE PROCEDURE p(IN key_value INT) BEGIN INSERT INTO audit(n) VALUES(1); UPDATE t SET n=n+1 WHERE id=(0*0)+key_value; END"
            },
            "BEGIN",
            "UPDATE t SET n=n+100 WHERE id=1",
        ] {
            exec(&engine, &mut owner, sql).unwrap();
        }
        let worker_engine = Arc::clone(&engine);
        let worker = std::thread::spawn(move || {
            let mut s = SessionContext::default();
            s.connection_id = 101;
            if explicit {
                exec(&worker_engine, &mut s, "BEGIN").unwrap();
            }
            exec(&worker_engine, &mut s, "CALL p(1)").unwrap();
            if explicit {
                exec(&worker_engine, &mut s, "COMMIT").unwrap();
            }
        });
        let start = Instant::now();
        while engine.db.data_lock_wait_snapshot().is_empty()
            && start.elapsed() < Duration::from_secs(2)
        {
            std::thread::sleep(Duration::from_millis(5));
        }
        std::thread::sleep(Duration::from_millis(50));
        exec(&engine, &mut owner, "COMMIT").unwrap();
        worker.join().unwrap();
        assert_eq!(
            rows(&engine, &mut owner, "SELECT COUNT(*) FROM audit"),
            vec![vec![Value::Int(1)]]
        );
        assert_eq!(
            rows(&engine, &mut owner, "SELECT n FROM t"),
            vec![vec![Value::Int(101)]]
        );
    }
}

#[test]
fn release1121_fulltext_boolean_lexing_hints_and_active_defaults() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(
        &engine,
        &mut s,
        "CREATE TABLE t(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body))",
    )
    .unwrap();
    exec(&engine,&mut s,"INSERT INTO t VALUES(1,'foo baz database'),(2,'foo bar full text'),(3,'bar qux data base'),(4,'baz qux full text')").unwrap();
    for (query, ids) in [
        ("+ foo", vec![1, 2]),
        ("foo - bar", vec![1]),
        ("data*base", vec![1, 3]),
        ("data*base*", vec![1, 3]),
        ("\"full text", vec![2, 4]),
    ] {
        assert_eq!(rows(&engine,&mut s,&format!("SELECT id FROM t WHERE MATCH(body) AGAINST('{query}' IN BOOLEAN MODE) ORDER BY id")),ids.into_iter().map(|id| vec![Value::Int(id)]).collect::<Vec<_>>(),"{query}");
    }
    assert_eq!(
        rows(
            &engine,
            &mut s,
            "SELECT id FROM t IGNORE INDEX(ft) WHERE MATCH(body) AGAINST('foo') ORDER BY id"
        ),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    for hint in [
        "IGNORE INDEX(ft)",
        "USE INDEX(PRIMARY)",
        "FORCE INDEX(PRIMARY)",
        "IGNORE INDEX FOR JOIN(ft)",
    ] {
        assert_eq!(
            exec(
                &engine,
                &mut s,
                &format!(
                    "SELECT id FROM t {hint} WHERE MATCH(body) AGAINST('foo' IN BOOLEAN MODE)"
                )
            )
            .unwrap_err()
            .code(),
            1191
        );
    }
    assert_eq!(rows(&engine, &mut s,
        "SELECT id FROM t IGNORE INDEX FOR ORDER BY(ft) WHERE MATCH(body) AGAINST('foo' IN BOOLEAN MODE) ORDER BY id"),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    assert_eq!(
        rows(
            &engine,
            &mut s,
            "SELECT @@global.innodb_ft_min_token_size,@@global.ft_min_word_len"
        )[0][0],
        rows(
            &engine,
            &mut s,
            "SELECT @@global.innodb_ft_min_token_size,@@global.ft_min_word_len"
        )[0][1]
    );
}

#[test]
fn release1121_routine_local_types_arguments_and_timestamp_results() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in ["CREATE FUNCTION set_f() RETURNS SET('a','b') DETERMINISTIC RETURN 'a'",
        "CREATE PROCEDURE set_p() BEGIN DECLARE v SET('a','b') DEFAULT 'a,b'; SET @set_value=v; END",
        "CREATE PROCEDURE arg_p(IN a INT) SET @arg=a",
        "CREATE PROCEDURE bit_p() BEGIN DECLARE v BIT(8) DEFAULT b'101'; SELECT v; END",
        "CREATE FUNCTION gbk_f() RETURNS VARCHAR(10) CHARACTER SET gbk DETERMINISTIC RETURN 'abc'"] {
        exec(&engine,&mut s,sql).unwrap();
    }
    exec(&engine, &mut s, "CALL set_p()").unwrap();
    assert_eq!(
        s.user_variable("set_value").unwrap().to_display_string(),
        "a,b"
    );
    assert_eq!(
        rows(&engine, &mut s, "SELECT set_f(),gbk_f()"),
        vec![vec![Value::String("a".into()), Value::String("abc".into())]]
    );
    exec(&engine, &mut s, "CALL arg_p((SELECT 42))").unwrap();
    assert_eq!(s.user_variable("arg"), Some(&Value::Int(42)));
    let QueryResponse::MultiResult(results) = exec(&engine, &mut s, "CALL bit_p()").unwrap() else {
        panic!()
    };
    let QueryResponse::ResultSet(result) = &results[0] else {
        panic!()
    };
    assert_eq!(
        result.columns[0].column_type,
        crate::types::ColumnType::Bit { bits: 8 }
    );
    exec(&engine, &mut s, "SET time_zone='+08:00'").unwrap();
    exec(&engine, &mut s, "CREATE TABLE ts(v TIMESTAMP)").unwrap();
    exec(
        &engine,
        &mut s,
        "INSERT INTO ts VALUES('2024-01-31 10:20:30')",
    )
    .unwrap();
    exec(&engine, &mut s, "CREATE PROCEDURE ts_p() SELECT v FROM ts").unwrap();
    for zone in ["+08:00", "-05:00"] {
        exec(&engine, &mut s, &format!("SET time_zone='{zone}'")).unwrap();
        let expected = rows(&engine, &mut s, "SELECT v FROM ts");
        let QueryResponse::MultiResult(results) = exec(&engine, &mut s, "CALL ts_p()").unwrap()
        else {
            panic!()
        };
        let QueryResponse::ResultSet(result) = &results[0] else {
            panic!()
        };
        assert_eq!(result.rows, expected);
    }
}

#[test]
fn release1121_routine_diagnostics_preserve_origins_resignal_and_handled_warnings() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut s, "INSERT INTO t VALUES(1)").unwrap();
    exec(&engine,&mut s,"CREATE PROCEDURE origin_p() BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN GET CURRENT DIAGNOSTICS CONDITION 1 @cl=CLASS_ORIGIN,@sub=SUBCLASS_ORIGIN; END; INSERT INTO t VALUES(1); END").unwrap();
    exec(&engine, &mut s, "CALL origin_p()").unwrap();
    assert_eq!(
        s.user_variable("cl"),
        Some(&Value::String("ISO 9075".into()))
    );
    exec(&engine, &mut s, "CREATE PROCEDURE missing_origin() BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN GET CURRENT DIAGNOSTICS CONDITION 1 @cl=CLASS_ORIGIN,@sub=SUBCLASS_ORIGIN; END; SELECT * FROM missing; END").unwrap();
    exec(&engine, &mut s, "CALL missing_origin()").unwrap();
    assert_eq!(
        s.user_variable("sub"),
        Some(&Value::String("ISO 9075".into()))
    );
    exec(&engine,&mut s,"CREATE PROCEDURE signal_origin() BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN GET CURRENT DIAGNOSTICS CONDITION 1 @cl=CLASS_ORIGIN,@sub=SUBCLASS_ORIGIN; END; SIGNAL SQLSTATE '45000'; END").unwrap();
    exec(&engine, &mut s, "CALL signal_origin()").unwrap();
    assert_eq!(s.user_variable("cl"), Some(&Value::String("".into())));
    exec(&engine,&mut s,"CREATE PROCEDURE resignal_p() BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '45000' RESIGNAL SQLSTATE '39000' SET MESSAGE_TEXT='morphed'; SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='orig'; END").unwrap();
    exec(&engine,&mut s,"CREATE PROCEDURE catch_p() BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '39000' BEGIN GET CURRENT DIAGNOSTICS @n=NUMBER; GET CURRENT DIAGNOSTICS CONDITION 1 @m1=MESSAGE_TEXT; GET CURRENT DIAGNOSTICS CONDITION 2 @m2=MESSAGE_TEXT; END; CALL resignal_p(); END").unwrap();
    exec(&engine, &mut s, "CALL catch_p()").unwrap();
    assert_eq!(s.user_variable("n"), Some(&Value::Int(2)));
    assert_eq!(s.user_variable("m1"), Some(&Value::String("orig".into())));
    assert_eq!(
        s.user_variable("m2"),
        Some(&Value::String("morphed".into()))
    );
    exec(&engine, &mut s, "SET sql_mode=''").unwrap();
    exec(&engine,&mut s,"CREATE PROCEDURE warn_p() BEGIN DECLARE x TINYINT; DECLARE CONTINUE HANDLER FOR SQLWARNING SET @caught=1; SET x=1000; END").unwrap();
    exec(&engine, &mut s, "CALL warn_p()").unwrap();
    assert!(rows(&engine, &mut s, "SHOW WARNINGS").is_empty());
}
