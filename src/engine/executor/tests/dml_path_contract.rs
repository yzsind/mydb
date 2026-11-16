use super::*;

fn exec(engine: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> QueryResponse {
    engine
        .execute_with_session(s, sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}

fn rows(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("{other:?}"),
    }
}

#[test]
fn dml_contract_trigger_odku_view_check() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,v INT)",
        "INSERT INTO t VALUES(1,1)",
        "CREATE VIEW v AS SELECT * FROM t WHERE v>0 WITH CHECK OPTION",
        "CREATE TRIGGER au AFTER UPDATE ON t FOR EACH ROW DO 1",
    ] {
        exec(&engine, &mut s, sql);
    }
    let err = engine
        .execute_with_session(
            &mut s,
            "INSERT INTO v VALUES(1,2) ON DUPLICATE KEY UPDATE v=-1",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1369);
    assert_eq!(
        rows(exec(&engine, &mut s, "SELECT v FROM t")),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn dml_contract_trigger_odku_timestamp_and_subquery() {
    for triggered in [false, true] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        let mut s = SessionContext::default();
        for sql in [
            "CREATE TABLE t(id INT PRIMARY KEY,v INT,ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
            "INSERT INTO t VALUES(1,1,'2000-01-01 00:00:00')",
        ] { exec(&engine, &mut s, sql); }
        if triggered {
            exec(
                &engine,
                &mut s,
                "CREATE TRIGGER au AFTER UPDATE ON t FOR EACH ROW DO 1",
            );
        }
        exec(
            &engine,
            &mut s,
            "INSERT INTO t(id,v) VALUES(1,2) ON DUPLICATE KEY UPDATE v=2",
        );
        assert_eq!(
            rows(exec(
                &engine,
                &mut s,
                "SELECT ts > '2000-01-02 00:00:00' FROM t"
            )),
            vec![vec![Value::Int(1)]],
            "triggered={triggered}"
        );
        exec(
            &engine,
            &mut s,
            "INSERT INTO t(id,v) VALUES(1,3) ON DUPLICATE KEY UPDATE v=(SELECT 7)",
        );
        assert_eq!(
            rows(exec(&engine, &mut s, "SELECT v FROM t")),
            vec![vec![Value::Int(7)]]
        );
    }
}

#[test]
fn dml_contract_update_ignore_returning_skips_failed_rows() {
    for ordered in [false, true] {
        for triggered in [false, true] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = open_test_engine(dir.path());
            let mut s = SessionContext::default();
            exec(
                &engine,
                &mut s,
                "CREATE TABLE t(id INT PRIMARY KEY,v INT UNIQUE)",
            );
            exec(&engine, &mut s, "INSERT INTO t VALUES(1,1),(2,2)");
            if triggered {
                exec(
                    &engine,
                    &mut s,
                    "CREATE TRIGGER bu BEFORE UPDATE ON t FOR EACH ROW DO 1",
                );
            }
            let order = if ordered { "ORDER BY id LIMIT 2" } else { "" };
            let got = rows(exec(
                &engine,
                &mut s,
                &format!("UPDATE IGNORE t SET v=1 WHERE id=2 RETURNING id,v {order}"),
            ));
            assert!(
                got.is_empty(),
                "ordered={ordered} triggered={triggered}: {got:?}"
            );
            assert_eq!(
                rows(exec(&engine, &mut s, "SELECT v FROM t WHERE id=2")),
                vec![vec![Value::Int(2)]]
            );
        }
    }
}

#[test]
fn dml_contract_update_returning_normalized_generated_image() {
    for ordered in [false, true] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        let mut s = SessionContext::default();
        exec(
            &engine,
            &mut s,
            "CREATE TABLE t(id INT PRIMARY KEY,v INT,g INT GENERATED ALWAYS AS (v*2) STORED)",
        );
        exec(&engine, &mut s, "INSERT INTO t(id,v) VALUES(1,1)");
        let order = if ordered { "ORDER BY id LIMIT 1" } else { "" };
        assert_eq!(
            rows(exec(
                &engine,
                &mut s,
                &format!("UPDATE t SET v='7' RETURNING v,g {order}")
            )),
            vec![vec![Value::Int(7), Value::Int(14)]]
        );
    }
}

#[test]
fn dml_contract_partition_trigger_odku() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,v INT) PARTITION BY HASH(id) PARTITIONS 4",
        "INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40)",
        "CREATE TRIGGER au AFTER UPDATE ON t FOR EACH ROW DO 1",
        "INSERT INTO t VALUES(3,31) ON DUPLICATE KEY UPDATE v=VALUES(v)",
    ] {
        exec(&engine, &mut s, sql);
    }
    assert_eq!(
        rows(exec(&engine, &mut s, "SELECT id,v FROM t ORDER BY id")),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(3), Value::Int(31)],
            vec![Value::Int(4), Value::Int(40)],
        ]
    );
}

#[test]
#[ignore = "SQL executor performance comparison; run on an idle host"]
fn dml_contract_benchmark() {
    for mode in [
        "point",
        "odku",
        "trigger_odku",
        "returning",
        "ordered",
        "delete",
        "trigger_delete",
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        let mut s = SessionContext::default();
        exec(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY,v INT)");
        let values = (0..1000)
            .map(|i| format!("({i},0)"))
            .collect::<Vec<_>>()
            .join(",");
        exec(&engine, &mut s, &format!("INSERT INTO t VALUES {values}"));
        if mode == "trigger_odku" {
            exec(
                &engine,
                &mut s,
                "CREATE TRIGGER au AFTER UPDATE ON t FOR EACH ROW DO 1",
            );
        }
        if mode == "trigger_delete" {
            exec(
                &engine,
                &mut s,
                "CREATE TRIGGER bd BEFORE DELETE ON t FOR EACH ROW DO 1",
            );
        }
        let start = Instant::now();
        for i in 0..3000 {
            let id = i % 1000;
            let sql = match mode {
                "odku" | "trigger_odku" => {
                    format!("INSERT INTO t VALUES({id},1) ON DUPLICATE KEY UPDATE v=v+1")
                }
                "returning" => format!("UPDATE t SET v=v+1 WHERE id={id} RETURNING v"),
                "ordered" => format!("UPDATE t SET v=v+1 WHERE id={id} ORDER BY id LIMIT 1"),
                "delete" | "trigger_delete" => {
                    exec(
                        &engine,
                        &mut s,
                        &format!("INSERT IGNORE INTO t VALUES({id},0)"),
                    );
                    format!("DELETE FROM t WHERE id={id}")
                }
                _ => format!("UPDATE t SET v=v+1 WHERE id={id}"),
            };
            exec(&engine, &mut s, &sql);
        }
        eprintln!(
            "DML_BENCH {mode} {:.3} us/op",
            start.elapsed().as_secs_f64() * 1e6 / 3000.0
        );
    }
}

#[test]
fn dml_contract_trigger_update_noop_fires_after_without_counting_change() {
    for ordered in [false, true] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        let mut s = SessionContext::default();
        for sql in [
            "CREATE TABLE t(id INT PRIMARY KEY,v INT)",
            "CREATE TABLE log(v INT)",
            "INSERT INTO t VALUES(1,1)",
            "CREATE TRIGGER au AFTER UPDATE ON t FOR EACH ROW INSERT INTO log VALUES(NEW.v)",
        ] {
            exec(&engine, &mut s, sql);
        }
        let order = if ordered { "ORDER BY id LIMIT 1" } else { "" };
        let result = exec(&engine, &mut s, &format!("UPDATE t SET v='1' {order}"));
        assert!(
            matches!(result, QueryResponse::OkAffected { affected_rows: 0 }),
            "{result:?}"
        );
        assert_eq!(
            rows(exec(&engine, &mut s, "SELECT v FROM log")),
            vec![vec![Value::Int(1)]]
        );
    }
}

#[test]
fn dml_contract_ignored_trigger_errors_still_abort() {
    for operation in ["UPDATE", "DELETE"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        let mut s = SessionContext::default();
        exec(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY,v INT)");
        exec(&engine, &mut s, "INSERT INTO t VALUES(1,1)");
        exec(&engine, &mut s, &format!("CREATE TRIGGER b BEFORE {operation} ON t FOR EACH ROW SIGNAL SQLSTATE '23000' SET MYSQL_ERRNO=1048, MESSAGE_TEXT='trigger failure'"));
        let sql = if operation == "UPDATE" {
            "UPDATE IGNORE t SET v=2 RETURNING v"
        } else {
            "DELETE IGNORE FROM t RETURNING v"
        };
        assert_eq!(
            engine.execute_with_session(&mut s, sql).unwrap_err().code(),
            1048
        );
        assert_eq!(
            rows(exec(&engine, &mut s, "SELECT v FROM t")),
            vec![vec![Value::Int(1)]]
        );
    }
}

#[test]
fn dml_contract_partition_migration_returns_final_image() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,v INT,g INT GENERATED ALWAYS AS(v*2) STORED) PARTITION BY HASH(id) PARTITIONS 4",
        "CREATE TABLE log(v INT,g INT)",
        "INSERT INTO t(id,v) VALUES(1,1),(2,2)",
        "CREATE TRIGGER au AFTER UPDATE ON t FOR EACH ROW INSERT INTO log VALUES(NEW.v,NEW.g)",
    ] { exec(&engine, &mut s, sql); }
    let got = rows(exec(
        &engine,
        &mut s,
        "UPDATE t SET id=3,v=7 WHERE id=1 RETURNING old.id,new.id,new.g",
    ));
    assert_eq!(
        got,
        vec![vec![Value::Int(1), Value::Int(3), Value::Int(14)]]
    );
    assert_eq!(
        rows(exec(&engine, &mut s, "SELECT v,g FROM log")),
        vec![vec![Value::Int(7), Value::Int(14)]]
    );
}

#[test]
fn dml_contract_replace_trigger_order_and_counts() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,v INT UNIQUE)",
        "CREATE TABLE log(seq INT AUTO_INCREMENT PRIMARY KEY,event VARCHAR(2))",
        "INSERT INTO t VALUES(1,10),(2,20)",
        "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW INSERT INTO log(event) VALUES('bi')",
        "CREATE TRIGGER bd BEFORE DELETE ON t FOR EACH ROW INSERT INTO log(event) VALUES('bd')",
        "CREATE TRIGGER ad AFTER DELETE ON t FOR EACH ROW INSERT INTO log(event) VALUES('ad')",
        "CREATE TRIGGER ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log(event) VALUES('ai')",
    ] {
        exec(&engine, &mut s, sql);
    }
    let result = exec(&engine, &mut s, "REPLACE INTO t VALUES(1,20)");
    assert!(
        matches!(result, QueryResponse::OkAffected { affected_rows: 3 }),
        "{result:?}"
    );
    let got = rows(exec(&engine, &mut s, "SELECT event FROM log ORDER BY seq"));
    assert_eq!(
        got,
        ["bi", "bd", "ad", "bd", "ad", "ai"]
            .into_iter()
            .map(|x| vec![Value::String(x.into())])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        rows(exec(&engine, &mut s, "SELECT id,v FROM t")),
        vec![vec![Value::Int(1), Value::Int(20)]]
    );
}

#[test]
fn dml_contract_trigger_rechecks_current_row_after_lock_wait() {
    for operation in ["UPDATE", "DELETE"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(open_test_engine(dir.path()));
        let mut owner = SessionContext::default();
        owner.connection_id = 8001;
        for sql in [
            "CREATE TABLE t(id INT PRIMARY KEY,v INT)",
            "CREATE TABLE log(v INT)",
            "INSERT INTO t VALUES(1,1)",
        ] {
            exec(&engine, &mut owner, sql);
        }
        exec(&engine, &mut owner, &format!("CREATE TRIGGER b BEFORE {operation} ON t FOR EACH ROW INSERT INTO log VALUES(OLD.v)"));
        exec(&engine, &mut owner, "BEGIN");
        exec(&engine, &mut owner, "UPDATE t SET v=2 WHERE id=1");
        exec(&engine, &mut owner, "DELETE FROM log");
        let other = Arc::clone(&engine);
        let waiter = std::thread::spawn(move || {
            let mut s = SessionContext::default();
            s.connection_id = 8002;
            let sql = if operation == "UPDATE" {
                "UPDATE t SET v=9 WHERE v=1"
            } else {
                "DELETE FROM t WHERE v=1"
            };
            other.execute_with_session(&mut s, sql)
        });
        // Synchronize on a real lock wait, not a timing-dependent sleep.
        let deadline = Instant::now() + Duration::from_secs(5);
        while engine.db_for_tests().data_lock_wait_snapshot().is_empty()
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        let waited = !engine.db_for_tests().data_lock_wait_snapshot().is_empty();
        exec(&engine, &mut owner, "COMMIT");
        let result = waiter.join().unwrap().unwrap();
        assert!(waited, "waiter never reached the row lock");
        assert!(
            matches!(result, QueryResponse::OkAffected { affected_rows: 0 }),
            "{result:?}"
        );
        assert_eq!(
            rows(exec(&engine, &mut owner, "SELECT v FROM t")),
            vec![vec![Value::Int(2)]]
        );
        assert!(rows(exec(&engine, &mut owner, "SELECT v FROM log")).is_empty());
    }
}
