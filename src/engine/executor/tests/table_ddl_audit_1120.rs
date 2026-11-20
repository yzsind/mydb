use super::*;

fn fixture() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::Builder::new()
        .prefix("ddl-1120-")
        .tempdir_in("/tmp/mydb")
        .unwrap()
}

fn reject(engine: &StorageQueryEngine, sql: &str, code: u16) {
    let err = engine.execute(sql).unwrap_err();
    assert_eq!(err.code(), code, "{sql}: {err:?}");
}

#[test]
fn ddl_1120_check_names_case_insensitive() {
    let dir = fixture();
    let e = open_test_engine(dir.path());
    for algorithm in ["", ", ALGORITHM=COPY"] {
        e.execute("CREATE TABLE t(a INT, CONSTRAINT mychk CHECK(a>0) NOT ENFORCED)")
            .unwrap();
        e.execute("INSERT INTO t VALUES(1)").unwrap();
        e.execute(&format!(
            "ALTER TABLE t ALTER CHECK MYCHK ENFORCED{algorithm}"
        ))
        .unwrap();
        reject(&e, "INSERT INTO t VALUES(-1)", 3819);
        e.execute(&format!("ALTER TABLE t DROP CHECK MYCHK{algorithm}"))
            .unwrap();
        e.execute("INSERT INTO t VALUES(-1)").unwrap();
        e.execute("DROP TABLE t").unwrap();
    }
}

#[test]
fn ddl_1120_drop_last_visible_column() {
    let dir = fixture();
    let e = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for kind in ["", "TEMPORARY "] {
        e.execute_with_session(
            &mut session,
            &format!("CREATE {kind}TABLE t(a INT INVISIBLE, b INT)"),
        )
        .unwrap();
        assert_eq!(
            e.execute_with_session(&mut session, "ALTER TABLE t DROP COLUMN b")
                .unwrap_err()
                .code(),
            4028
        );
        e.execute_with_session(&mut session, "INSERT INTO t(b) VALUES(2)")
            .unwrap();
        e.execute_with_session(&mut session, "SELECT * FROM t")
            .unwrap()
            .materialize()
            .unwrap();
        e.execute_with_session(
            &mut session,
            "ALTER TABLE t DROP COLUMN b, ADD COLUMN c INT",
        )
        .unwrap();
        e.execute_with_session(&mut session, "SELECT * FROM t")
            .unwrap()
            .materialize()
            .unwrap();
        e.execute_with_session(&mut session, "DROP TABLE t")
            .unwrap();
    }
}

#[test]
fn ddl_1120_primary_reserved_name() {
    let dir = fixture();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(a INT)").unwrap();
    for sql in [
        "CREATE INDEX `PRIMARY` ON t(a)",
        "ALTER TABLE t ADD INDEX `PRIMARY`(a)",
        "ALTER TABLE t ADD UNIQUE INDEX `primary`(a)",
    ] {
        reject(&e, sql, 1280);
    }
    e.execute("ALTER TABLE t ADD PRIMARY KEY(a)").unwrap();
}

#[test]
fn ddl_1120_prefix_bounds() {
    let dir = fixture();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(a VARCHAR(20), KEY ap(a(10)))")
        .unwrap();
    reject(&e, "ALTER TABLE t ADD INDEX bad(a(21))", 1089);
    reject(&e, "ALTER TABLE t MODIFY a VARCHAR(5)", 1089);
    e.execute("INSERT INTO t VALUES('abcdefghij')").unwrap();
}

#[test]
fn ddl_1120_object_names() {
    let dir = fixture();
    let e = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE test.b.c(i INT)",
        "CREATE TABLE ``.t(i INT)",
        "CREATE TABLE ``(i INT)",
        "CREATE TABLE ` `(i INT)",
    ] {
        assert!(e.execute(sql).is_err(), "{sql}");
    }
}

#[test]
fn ddl_1120_default_self_reference() {
    let dir = fixture();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(a INT)").unwrap();
    e.execute("INSERT INTO t VALUES(1)").unwrap();
    reject(&e, "ALTER TABLE t ADD d INT DEFAULT(d+1)", 3767);
    e.execute("ALTER TABLE t ADD d INT DEFAULT(a+1)").unwrap();
    e.execute("CREATE TABLE chain(a INT DEFAULT(1+1), b INT DEFAULT(a+1))")
        .unwrap();
    e.execute("INSERT INTO chain() VALUES()").unwrap();
    let QueryResponse::ResultSet(r) = e
        .execute("SELECT * FROM chain")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(r.rows, vec![vec![Value::Int(2), Value::Int(3)]]);
    reject(
        &e,
        "CREATE TABLE forward_expr(a INT DEFAULT(b+1), b INT DEFAULT(1+1))",
        3767,
    );
    e.execute("ALTER TABLE t ADD e INT DEFAULT(f+1), ADD f INT DEFAULT 1")
        .unwrap();
    reject(
        &e,
        "ALTER TABLE t ADD g INT DEFAULT(h+1), ADD h INT DEFAULT(1+1)",
        3767,
    );
}

#[test]
fn ddl_1120_null_error() {
    let dir = fixture();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(a INT)").unwrap();
    e.execute("INSERT INTO t VALUES(NULL)").unwrap();
    reject(&e, "ALTER TABLE t MODIFY a INT NOT NULL", 1138);
}

#[test]
fn ddl_1120_temp_spatial_index() {
    let dir = fixture();
    let e = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    e.execute_with_session(
        &mut session,
        "CREATE TEMPORARY TABLE t(g GEOMETRY NOT NULL)",
    )
    .unwrap();
    e.execute_with_session(&mut session, "ALTER TABLE t ADD INDEX gi(g)")
        .unwrap();
    let QueryResponse::ResultSet(r) = e
        .execute_with_session(&mut session, "SHOW INDEX FROM t")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert!(r.rows.iter().any(|r| {
        r.iter()
            .any(|v| matches!(v, Value::String(s) if s == "SPATIAL"))
    }));
}

#[test]
fn ddl_1120_auto_index_names() {
    let dir = fixture();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(a INT)").unwrap();
    e.execute("ALTER TABLE t ADD INDEX(a), ADD INDEX(a DESC)")
        .unwrap();
    let t = e.database().table("test.t").unwrap();
    assert!(t.indexes.iter().any(|i| i.name == "a_2"));
    let mut session = SessionContext::default();
    e.execute_with_session(
        &mut session,
        "CREATE TEMPORARY TABLE temp_auto(`primary` INT)",
    )
    .unwrap();
    e.execute_with_session(
        &mut session,
        "ALTER TABLE temp_auto ADD INDEX(`primary`), ADD INDEX(`primary` DESC)",
    )
    .unwrap();
    let QueryResponse::ResultSet(r) = e
        .execute_with_session(&mut session, "SHOW INDEX FROM temp_auto")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    for name in ["primary_2", "primary_3"] {
        assert!(r.rows.iter().any(|r| r[2] == Value::String(name.into())));
    }
    e.execute_with_session(&mut session, "DROP TABLE temp_auto")
        .unwrap();
}

#[test]
fn ddl_1120_numeric_and_parser_validation() {
    let dir = fixture();
    let e = open_test_engine(dir.path());
    for ty in ["FLOAT", "DOUBLE"] {
        reject(&e, &format!("CREATE TABLE t(a {ty}(0,0))"), 1439);
    }
    e.execute("CREATE TABLE t(doc TEXT)").unwrap();
    assert!(e
        .execute("ALTER TABLE t KEY_BLOCK_SIZE=999999999999")
        .is_err());
    reject(
        &e,
        "CREATE FULLTEXT INDEX ft ON t(doc) WITH PARSER myparser",
        1128,
    );
}

#[test]
fn ddl_1120_change_fk_column() {
    let dir = fixture();
    let e = StorageQueryEngine::open(dir.path()).unwrap();
    for sql in [
        "CREATE TABLE p(a INT PRIMARY KEY)",
        "CREATE TABLE c(a INT, CONSTRAINT fk FOREIGN KEY(a) REFERENCES p(a) ON DELETE CASCADE)",
        "INSERT INTO p VALUES(1)",
        "INSERT INTO c VALUES(1)",
        "ALTER TABLE c CHANGE a b INT",
        "ALTER TABLE p CHANGE a b INT",
    ] {
        e.execute(sql).unwrap();
    }
    reject(&e, "INSERT INTO c VALUES(2)", 1452);
    e.execute("DELETE FROM p WHERE b=1").unwrap();
    let QueryResponse::ResultSet(r) = e.execute("SELECT * FROM c").unwrap().materialize().unwrap()
    else {
        panic!()
    };
    assert!(r.rows.is_empty());
    drop(e);
    let e = StorageQueryEngine::open(dir.path()).unwrap();
    reject(&e, "INSERT INTO c VALUES(2)", 1452);
}

#[test]
fn ddl_1120_exchange_replication_generations() {
    fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let dest = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &dest);
            } else {
                std::fs::copy(entry.path(), dest).unwrap();
            }
        }
    }
    let md = fixture();
    let rd = fixture();
    let master = StorageQueryEngine::open(md.path()).unwrap();
    for sql in [
        "CREATE TABLE part(a INT) PARTITION BY RANGE(a)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)",
        "CREATE TABLE stage(a INT)",
        "INSERT INTO stage VALUES(1),(2)",
    ] {
        master.execute(sql).unwrap();
    }
    let source = master.database_for_replication();
    assert_eq!(
        source.table("test.part").unwrap().active_generation,
        source.table("test.stage").unwrap().active_generation
    );
    source.checkpoint().unwrap();
    copy_tree(md.path(), rd.path());
    let mut role = crate::repl::role::RoleState::new_standalone();
    role.role = crate::repl::role::Role::Replica;
    crate::repl::role::save(rd.path(), &role).unwrap();
    let start = source.wal_log().lock().next_lsn();
    let replica = StorageQueryEngine::open(rd.path()).unwrap();
    master
        .execute("ALTER TABLE part EXCHANGE PARTITION p0 WITH TABLE stage")
        .unwrap();
    let log = source.wal_log();
    let safe = log.lock().next_lsn() - 1;
    crate::wal::LogManager::sync_durable_to(&log, safe).unwrap();
    let target = replica.database_for_replication().clone();
    let mut applier = crate::repl::replica::applier::Applier::with_database(
        rd.path(),
        target.wal_log(),
        target,
        crate::repl::replica::progress::ReplicaProgress::default(),
    );
    let mut reader = crate::wal::WalReader::open(&log, start).unwrap();
    while let Some(chunk) = reader.next_chunk(safe, 1 << 20).unwrap() {
        applier.apply_chunk(&chunk, &role.node_uuid, 1).unwrap();
    }
    let QueryResponse::ResultSet(r) = replica
        .execute("SELECT a FROM part ORDER BY a")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn ddl_1120_exchange_rejects_generation_relocation_atomically() {
    let dir = fixture();
    let e = StorageQueryEngine::open(dir.path()).unwrap();
    for sql in ["CREATE TABLE part(a INT) PARTITION BY RANGE(a)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)", "CREATE TABLE stage(a INT)", "INSERT INTO stage VALUES(1),(2)", "ALTER TABLE stage FORCE"] { e.execute(sql).unwrap(); }
    assert_ne!(
        e.database().table("test.part").unwrap().active_generation,
        e.database().table("test.stage").unwrap().active_generation
    );
    reject(
        &e,
        "ALTER TABLE part EXCHANGE PARTITION p0 WITH TABLE stage",
        1235,
    );
    drop(e);
    let e = StorageQueryEngine::open(dir.path()).unwrap();
    let QueryResponse::ResultSet(r) = e
        .execute("SELECT a FROM stage ORDER BY a")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    let QueryResponse::ResultSet(r) = e
        .execute("SELECT * FROM part")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert!(r.rows.is_empty());
}

#[test]
fn ddl_1120_fk_rename_rejects_non_atomic_compound() {
    let dir = fixture();
    let e = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE p(a INT PRIMARY KEY)",
        "CREATE TABLE c(a INT,FOREIGN KEY(a) REFERENCES p(a))",
        "INSERT INTO p VALUES(1)",
    ] {
        e.execute(sql).unwrap();
    }
    reject(&e, "ALTER TABLE p CHANGE a b INT, ADD CHECK(b<0)", 1846);
    e.execute("INSERT INTO c VALUES(1)").unwrap();
    e.execute("SELECT a FROM p").unwrap();
    reject(&e, "INSERT INTO c VALUES(2)", 1452);
}
