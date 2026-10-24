use super::*;

fn audit_dir() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::tempdir_in("/tmp/mydb").unwrap()
}
fn rows(response: QueryResponse) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected rows")
    };
    result.rows
}

#[test]
fn audit_dropped_not_null_insert() {
    let dir = audit_dir();
    let e = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    e.execute_with_session(
        &mut session,
        "CREATE TABLE t(a INT PRIMARY KEY,b INT NOT NULL,c INT NOT NULL)",
    )
    .unwrap();
    e.execute_with_session(&mut session, "INSERT INTO t VALUES(1,2,3)")
        .unwrap();
    e.execute_with_session(&mut session, "ALTER TABLE t DROP COLUMN b")
        .unwrap();
    e.execute_with_session(&mut session, "INSERT INTO t VALUES(4,5)")
        .unwrap();
    e.execute_with_session(&mut session, "INSERT INTO t(a,c) VALUES(6,7),(8,9)")
        .unwrap();
    assert_eq!(
        e.execute_with_session(&mut session, "INSERT INTO t VALUES(10,NULL)")
            .unwrap_err()
            .code(),
        1048
    );
    assert_eq!(
        rows(
            e.execute_with_session(&mut session, "SELECT COUNT(*) FROM t")
                .unwrap()
        )[0][0],
        Value::Int(4)
    );
}

#[test]
fn audit_load_existing_unique_atomicity() {
    let dir = audit_dir();
    let e = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    e.execute_with_session(
        &mut session,
        "CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(30),UNIQUE KEY uk(v))",
    )
    .unwrap();
    e.execute_with_session(&mut session, "INSERT INTO t VALUES(1,'original')")
        .unwrap();
    let file = dir.path().join("load.tsv");
    for data in [
        "2\tnew\n1\tconflict\n",
        "2\tnew\n3\toriginal\n",
        "2\tsame\n3\tsame\n",
    ] {
        std::fs::write(&file, data).unwrap();
        let sql = format!(
            "LOAD DATA INFILE '{}' INTO TABLE t",
            crate::util::file::sql_path(&file)
        );
        assert_eq!(
            e.execute_with_session(&mut session, &sql)
                .unwrap_err()
                .code(),
            1062
        );
        assert_eq!(
            rows(
                e.execute_with_session(&mut session, "SELECT * FROM t ORDER BY id")
                    .unwrap()
            ),
            vec![vec![Value::Int(1), Value::String("original".into())]]
        );
    }
    std::fs::write(&file, "2\tnew\n3\tother\n").unwrap();
    e.execute_with_session(
        &mut session,
        &format!(
            "LOAD DATA INFILE '{}' INTO TABLE t",
            crate::util::file::sql_path(&file)
        ),
    )
    .unwrap();
    assert_eq!(
        rows(
            e.execute_with_session(&mut session, "SELECT COUNT(*) FROM t")
                .unwrap()
        )[0][0],
        Value::Int(3)
    );
}

#[test]
fn audit_partition_update_auto_increment() {
    let dir = audit_dir();
    let e = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for (name, new_id) in [("same_p", 101), ("cross_p", 100)] {
        e.execute_with_session(&mut session, &format!("CREATE TABLE {name}(id INT AUTO_INCREMENT PRIMARY KEY,v INT) PARTITION BY HASH(id) PARTITIONS 2")).unwrap();
        e.execute_with_session(&mut session, &format!("INSERT INTO {name}(v) VALUES(1)"))
            .unwrap();
        e.execute_with_session(
            &mut session,
            &format!("UPDATE {name} SET id={new_id} WHERE v=1"),
        )
        .unwrap();
        e.execute_with_session(&mut session, &format!("INSERT INTO {name}(v) VALUES(2)"))
            .unwrap();
        assert_eq!(
            rows(
                e.execute_with_session(&mut session, &format!("SELECT id FROM {name} WHERE v=2"))
                    .unwrap()
            )[0][0],
            Value::Int(new_id + 1)
        );
    }
}

#[test]
fn audit_unsigned_float_range() {
    let dir = audit_dir();
    let e = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    e.execute_with_session(&mut session, "SET sql_mode='STRICT_ALL_TABLES'")
        .unwrap();
    e.execute_with_session(&mut session, "CREATE TABLE t(u BIGINT UNSIGNED)")
        .unwrap();
    for value in ["-1.5e0", "-2.5e0", "1e30"] {
        assert_eq!(
            e.execute_with_session(&mut session, &format!("INSERT INTO t VALUES({value})"))
                .unwrap_err()
                .code(),
            1264
        );
    }
    e.execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    e.execute_with_session(&mut session, "INSERT INTO t VALUES(-1.5e0)")
        .unwrap();
    assert_eq!(
        rows(
            e.execute_with_session(&mut session, "SELECT u FROM t")
                .unwrap()
        )[0][0],
        Value::UInt(0)
    );
}

#[test]
fn audit_varchar_nonspace_overflow() {
    let dir = audit_dir();
    let e = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    e.execute_with_session(&mut session, "SET sql_mode='STRICT_ALL_TABLES'")
        .unwrap();
    e.execute_with_session(&mut session, "CREATE TABLE t(v VARCHAR(2))")
        .unwrap();
    for suffix in ["\n", "\t", "\r", "\u{a0}"] {
        assert_eq!(
            e.execute_with_session(&mut session, &format!("INSERT INTO t VALUES('ab{suffix}')"))
                .unwrap_err()
                .code(),
            1406
        );
    }
    e.execute_with_session(&mut session, "INSERT INTO t VALUES('ab ')")
        .unwrap();
    assert_eq!(
        rows(
            e.execute_with_session(&mut session, "SELECT v FROM t")
                .unwrap()
        )[0][0],
        Value::String("ab".into())
    );
}

#[test]
fn audit_event_rename_collision_atomicity() {
    let dir = audit_dir();
    let e = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for name in ["e1", "e2"] {
        e.execute_with_session(
            &mut session,
            &format!("CREATE EVENT {name} ON SCHEDULE EVERY 1 DAY DO SELECT 1"),
        )
        .unwrap();
    }
    let before = rows(
        e.execute_with_session(&mut session, "SHOW CREATE EVENT e1")
            .unwrap(),
    );
    assert_eq!(
        e.execute_with_session(
            &mut session,
            "ALTER EVENT e1 ON SCHEDULE EVERY 1 HOUR RENAME TO e2"
        )
        .unwrap_err()
        .code(),
        1537
    );
    assert_eq!(
        rows(
            e.execute_with_session(&mut session, "SHOW CREATE EVENT e1")
                .unwrap()
        ),
        before
    );
}

#[test]
fn audit_float_duplicate_diagnostic() {
    let dir = audit_dir();
    let e = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    e.execute_with_session(&mut session, "CREATE TABLE t(f FLOAT UNIQUE)")
        .unwrap();
    e.execute_with_session(&mut session, "INSERT INTO t VALUES(0)")
        .unwrap();
    let error = e
        .execute_with_session(&mut session, "INSERT INTO t VALUES(-0.0)")
        .unwrap_err();
    assert_eq!(error.code(), 1062);
    assert!(error.to_string().contains("'0'"), "{error}");
}

#[test]
fn audit_savepoint_replace_preserves_other_names() {
    let dir = audit_dir();
    let e = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT)",
        "BEGIN",
        "INSERT INTO t VALUES(1)",
        "SAVEPOINT a",
        "INSERT INTO t VALUES(2)",
        "SAVEPOINT b",
        "INSERT INTO t VALUES(3)",
        "SAVEPOINT a",
        "ROLLBACK TO b",
    ] {
        e.execute_with_session(&mut session, sql).unwrap();
    }
    assert_eq!(
        rows(
            e.execute_with_session(&mut session, "SELECT COUNT(*) FROM t")
                .unwrap()
        )[0][0],
        Value::Int(2)
    );
    assert_eq!(
        e.execute_with_session(&mut session, "ROLLBACK TO a")
            .unwrap_err()
            .code(),
        1305
    );
    e.execute_with_session(&mut session, "ROLLBACK").unwrap();
}

#[test]
fn audit_decimal_fk_requires_matching_definition() {
    let dir = audit_dir();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE p(d DECIMAL(10,2) PRIMARY KEY)")
        .unwrap();
    for ty in ["DECIMAL(12,4)", "DECIMAL(10,2) UNSIGNED"] {
        assert_eq!(
            e.execute(&format!(
                "CREATE TABLE c(d {ty},FOREIGN KEY(d) REFERENCES p(d))"
            ))
            .unwrap_err()
            .code(),
            3780
        );
    }
    e.execute("CREATE TABLE wider(d DECIMAL(20,2),FOREIGN KEY(d) REFERENCES p(d))")
        .unwrap();
    e.execute("CREATE TABLE c(d DECIMAL(10,2),FOREIGN KEY(d) REFERENCES p(d))")
        .unwrap();
    e.execute("INSERT INTO p VALUES(1.23)").unwrap();
    e.execute("INSERT INTO c VALUES(1.23)").unwrap();
    assert_eq!(
        e.execute("INSERT INTO c VALUES(1.24)").unwrap_err().code(),
        1452
    );
}

#[test]
fn audit_rename_included_column() {
    let dir = audit_dir();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(id INT PRIMARY KEY,k INT,v INT)")
        .unwrap();
    e.execute("CREATE INDEX ix ON t(k) INCLUDE(v)").unwrap();
    e.execute("INSERT INTO t VALUES(1,10,20)").unwrap();
    e.execute("ALTER TABLE t RENAME COLUMN v TO val").unwrap();
    e.execute("INSERT INTO t VALUES(2,11,21)").unwrap();
    assert_eq!(
        rows(e.execute("SELECT val FROM t WHERE k=11").unwrap())[0][0],
        Value::Int(21)
    );
}

#[test]
fn audit_rename_index_length() {
    let dir = audit_dir();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(id INT, KEY ix(id))").unwrap();
    assert_eq!(
        e.execute(&format!(
            "ALTER TABLE t RENAME INDEX ix TO {}",
            "x".repeat(65)
        ))
        .unwrap_err()
        .code(),
        1059
    );
    e.execute("INSERT INTO t VALUES(1)").unwrap();
}

#[test]
fn audit_exchange_rejects_different_prefix_index() {
    let dir = audit_dir();
    let e = open_test_engine(dir.path());
    e.execute(
        "CREATE TABLE p(id INT,v VARCHAR(20),KEY ix(v(3))) PARTITION BY HASH(id) PARTITIONS 2",
    )
    .unwrap();
    e.execute("CREATE TABLE t(id INT,v VARCHAR(20),KEY ix(v(5)))")
        .unwrap();
    assert_eq!(
        e.execute("ALTER TABLE p EXCHANGE PARTITION p0 WITH TABLE t")
            .unwrap_err()
            .code(),
        1736
    );
}
