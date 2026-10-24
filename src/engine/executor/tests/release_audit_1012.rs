use super::*;

fn setup() -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::Builder::new()
        .prefix("release-audit-")
        .tempdir_in("/tmp/mydb")
        .unwrap();
    let engine = open_strict_test_engine(dir.path());
    (dir, engine)
}

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<String>> {
    let response = engine
        .execute_with_session(session, sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(rs) = response else {
        panic!("expected result set")
    };
    rs.rows
        .into_iter()
        .map(|r| r.into_iter().map(|v| v.to_display_string()).collect())
        .collect()
}

#[test]
fn release_audit_change_include_restart() {
    let (dir, engine) = setup();
    for (name, alter) in [
        ("single", "CHANGE c cc INT"),
        ("compound", "CHANGE c cc BIGINT, ADD COLUMN d INT DEFAULT 7"),
    ] {
        engine
            .execute(&format!(
                "CREATE TABLE {name}(id INT PRIMARY KEY,a INT,c INT,KEY k(a) INCLUDE(c))"
            ))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO {name} VALUES(1,1,1)"))
            .unwrap();
        engine
            .execute(&format!("ALTER TABLE {name} {alter}"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO {name}(id,a,cc) VALUES(2,2,2)"))
            .unwrap();
        engine
            .execute(&format!("UPDATE {name} SET cc=3 WHERE id=1"))
            .unwrap();
        let mut s = SessionContext::default();
        engine.execute_with_session(&mut s, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut s, &format!("UPDATE {name} SET a=9"))
            .unwrap();
        engine.execute_with_session(&mut s, "ROLLBACK").unwrap();
        assert_eq!(
            rows(
                &engine,
                &mut s,
                &format!("SELECT cc FROM {name} ORDER BY id")
            ),
            vec![vec!["3"], vec!["2"]]
        );
    }
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    for name in ["single", "compound"] {
        engine
            .execute(&format!("INSERT INTO {name}(id,a,cc) VALUES(3,3,3)"))
            .unwrap();
    }
}

#[test]
fn release_audit_temp_rename_dependencies() {
    let (_dir, engine) = setup();
    let mut s = SessionContext::default();
    for sql in ["CREATE TEMPORARY TABLE t(id INT PRIMARY KEY,a INT,c INT, g INT AS (c+1) STORED, KEY k(a) INCLUDE(c), CHECK(c>0))", "ALTER TABLE t RENAME COLUMN c TO cc", "INSERT INTO t(id,a,cc) VALUES(1,2,3)"] {
        engine.execute_with_session(&mut s,sql).unwrap();
    }
    assert_eq!(
        rows(&engine, &mut s, "SELECT cc,g FROM t"),
        vec![vec!["3", "4"]]
    );
    assert!(engine
        .execute_with_session(&mut s, "INSERT INTO t(id,a,cc) VALUES(2,2,-1)")
        .is_err());
    engine
        .execute_with_session(&mut s, "CREATE TEMPORARY TABLE f(a INT,KEY ((a+1)))")
        .unwrap();
    assert!(engine
        .execute_with_session(&mut s, "ALTER TABLE f RENAME COLUMN a TO b")
        .is_err());
    engine
        .execute_with_session(&mut s, "INSERT INTO f VALUES(7)")
        .unwrap();
    assert_eq!(rows(&engine, &mut s, "SELECT a FROM f"), vec![vec!["7"]]);
}

#[test]
fn release_audit_drop_database_sequences_restart() {
    let (dir, engine) = setup();
    for sql in [
        "CREATE DATABASE dseq",
        "CREATE SEQUENCE dseq.s START WITH 1",
        "DROP DATABASE dseq",
        "CREATE DATABASE dseq",
    ] {
        engine.execute(sql).unwrap();
    }
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE SEQUENCE dseq.s START WITH 10")
        .unwrap();
}

#[test]
fn release_audit_failed_read_only_does_not_start_transaction() {
    let (_dir, engine) = setup();
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(a INT)",
        "SET autocommit=0",
        "SET TRANSACTION READ ONLY",
    ] {
        engine.execute_with_session(&mut s, sql).unwrap();
    }
    let err = engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES(1)")
        .unwrap_err();
    assert_eq!(err.code(), 1792);
    assert!(s.active_txn_isolation.is_none());
    engine
        .execute_with_session(&mut s, "SET TRANSACTION READ ONLY")
        .unwrap();
    engine
        .execute_with_session(&mut s, "XA START 'after-denied'")
        .unwrap();
    engine
        .execute_with_session(&mut s, "XA END 'after-denied'")
        .unwrap();
    engine
        .execute_with_session(&mut s, "XA ROLLBACK 'after-denied'")
        .unwrap();
}

#[test]
fn release_audit_prepared_dml_semantics_and_binding() {
    use crate::engine::api::PreparedDmlState;
    let (_dir, engine) = setup();
    let mut s = SessionContext::default();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,r DECIMAL(20,12))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,0),(2,0),(3,0)")
        .unwrap();
    let sql = "UPDATE t SET r=1/3 WHERE id=?";
    let parsed = engine.prepare_cached(&mut s, sql, 1).unwrap();
    let mut state = PreparedDmlState::default();
    engine
        .execute_prepared_dml(&mut s, &mut state, &parsed, sql, &[Value::Int(1)])
        .unwrap()
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET div_precision_increment=10")
        .unwrap();
    engine
        .execute_prepared_dml(
            &mut s,
            &mut state,
            &parsed,
            sql,
            &[Value::String("2".into())],
        )
        .unwrap()
        .unwrap();
    engine
        .execute_prepared_dml(&mut s, &mut state, &parsed, sql, &[Value::Int(3)])
        .unwrap()
        .unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT r FROM t ORDER BY id"),
        vec![
            vec!["0.333300000000"],
            vec!["0.333333333300"],
            vec!["0.333333333300"]
        ]
    );
}

#[test]
fn release_audit_load_columns_ignore_and_triggers() {
    let (dir, engine) = setup();
    let file = dir.path().join("load.tsv");
    std::fs::write(&file, "1\t300\n2\t5\n").unwrap();
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET sql_mode='STRICT_TRANS_TABLES'")
        .unwrap();
    for trigger in [false, true] {
        engine
            .execute_with_session(&mut s, "DROP TABLE IF EXISTS t")
            .unwrap();
        engine
            .execute_with_session(
                &mut s,
                "CREATE TABLE t(id INT PRIMARY KEY,v TINYINT,b INT DEFAULT 7)",
            )
            .unwrap();
        if trigger {
            engine
                .execute_with_session(
                    &mut s,
                    "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW SET @seen=NEW.id",
                )
                .unwrap();
        }
        engine
            .execute_with_session(
                &mut s,
                &format!(
                    "LOAD DATA INFILE '{}' IGNORE INTO TABLE t(id,v)",
                    crate::util::file::sql_path(&file)
                ),
            )
            .unwrap();
        assert_eq!(
            rows(&engine, &mut s, "SELECT * FROM t ORDER BY id"),
            vec![vec!["1", "127", "7"], vec!["2", "5", "7"]]
        );
        // Reading rows replaces statement warnings; coercion is asserted above.
    }
    std::fs::write(&file, "3\t6\n").unwrap();
    engine
        .execute_with_session(
            &mut s,
            &format!(
                "LOAD DATA INFILE '{}' INTO TABLE t(id,v)",
                crate::util::file::sql_path(&file)
            ),
        )
        .unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT b FROM t WHERE id=3"),
        vec![vec!["7"]]
    );
}

#[test]
fn release_audit_pad_introducer_partition() {
    let (_dir, engine) = setup();
    let mut s = SessionContext::default();
    assert_eq!(rows(&engine,&mut s,"SELECT LPAD('a',3,''),RPAD('中',2,''),HEX(LPAD(_binary'a',3,'')),CHAR_LENGTH(_latin1'中'),LPAD('abc',2,'')"),vec![vec!["","","","3","ab"]]);
    for name in ["my spaced table", "escaped`` table"] {
        engine
            .execute(&format!(
                "CREATE TABLE `{name}`(a INT) PARTITION BY HASH(a) PARTITIONS 2"
            ))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO `{name}` VALUES(1),(2)"))
            .unwrap();
        engine
            .execute(&format!("ALTER TABLE `{name}` TRUNCATE PARTITION ALL"))
            .unwrap();
        assert_eq!(
            rows(&engine, &mut s, &format!("SELECT COUNT(*) FROM `{name}`")),
            vec![vec!["0"]]
        );
    }
}

#[test]
fn release_audit_xa_unique_reservation_survives_restart() {
    let (dir, engine) = setup();
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,u INT UNIQUE)",
        "XA START 'reserve'",
        "INSERT INTO t VALUES(1,7)",
        "XA END 'reserve'",
        "XA PREPARE 'reserve'",
    ] {
        engine.execute_with_session(&mut s, sql).unwrap();
    }
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET innodb_lock_wait_timeout=1")
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES(2,7)")
        .unwrap_err();
    assert_eq!(
        err.code(),
        1205,
        "unique reservation must block a different row id: {err:?}"
    );
    engine
        .execute_with_session(&mut s, "XA ROLLBACK 'reserve'")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES(2,7)")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT id,u FROM t"),
        vec![vec!["2", "7"]]
    );
}

#[test]
fn release_audit_load_physical_rows_include_invisible_and_dropped_columns() {
    let (dir, engine) = setup();
    let mut session = SessionContext::default();
    let file = dir.path().join("physical.tsv");
    std::fs::write(&file, "1\t5\n2\t6\n").unwrap();
    for trigger in [false, true] {
        engine
            .execute_with_session(&mut session, "DROP TABLE IF EXISTS physical_load")
            .unwrap();
        engine.execute_with_session(&mut session, "CREATE TABLE physical_load(id INT PRIMARY KEY,a INT INVISIBLE, dropped INT,b INT DEFAULT 7)").unwrap();
        engine
            .execute_with_session(
                &mut session,
                "ALTER TABLE physical_load DROP COLUMN dropped",
            )
            .unwrap();
        if trigger {
            engine.execute_with_session(&mut session, "CREATE TRIGGER physical_tr AFTER INSERT ON physical_load FOR EACH ROW SET @physical_seen=NEW.a").unwrap();
        }
        engine
            .execute_with_session(&mut session, "INSERT INTO physical_load(id,a) VALUES(1,0)")
            .unwrap();
        for mode in ["IGNORE", "REPLACE"] {
            engine
                .execute_with_session(
                    &mut session,
                    &format!(
                        "LOAD DATA INFILE '{}' {mode} INTO TABLE physical_load(id,a)",
                        crate::util::file::sql_path(&file)
                    ),
                )
                .unwrap();
        }
        assert_eq!(
            rows(
                &engine,
                &mut session,
                "SELECT id,a,b FROM physical_load ORDER BY id"
            ),
            vec![vec!["1", "5", "7"], vec!["2", "6", "7"]]
        );
    }
}

#[test]
fn release_audit_load_ignore_fast_and_general_coercions_agree() {
    let (dir, engine) = setup();
    let mut session = SessionContext::default();
    let file = dir.path().join("coercions.tsv");
    std::fs::write(&file, "1\t-2\tabcdef\tbad\n2\t5\tok\t2024-02-29\n").unwrap();
    engine
        .execute_with_session(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'")
        .unwrap();
    let mut actual = Vec::new();
    for (table, suffix) in [("fast_coercions", ""), ("general_coercions", " SET id=id")] {
        engine.execute_with_session(&mut session, &format!("CREATE TABLE {table}(id INT PRIMARY KEY,n TINYINT UNSIGNED,s VARCHAR(3),d DATE)")).unwrap();
        engine
            .execute_with_session(
                &mut session,
                &format!(
                    "LOAD DATA INFILE '{}' IGNORE INTO TABLE {table}{suffix}",
                    crate::util::file::sql_path(&file)
                ),
            )
            .unwrap();
        assert!(!session.warnings.is_empty());
        actual.push(rows(
            &engine,
            &mut session,
            &format!("SELECT * FROM {table} ORDER BY id"),
        ));
    }
    assert_eq!(actual[0], actual[1]);
    assert_eq!(
        actual[0],
        vec![
            vec!["1", "0", "abc", "0000-00-00"],
            vec!["2", "5", "ok", "2024-02-29"]
        ]
    );
}
