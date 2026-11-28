use super::*;

fn fixture() -> (tempfile::TempDir, Arc<StorageQueryEngine>) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    let def = "(id INT PRIMARY KEY,v LONGTEXT,g GEOMETRY NOT NULL,SPATIAL sp(g),INDEX vi(v(8)))";
    engine.execute(&format!("CREATE TABLE t{def} PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)")).unwrap();
    engine.execute(&format!("CREATE TABLE o{def}")).unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'old',POINT(1,1)),(11,'retained',POINT(11,11))")
        .unwrap();
    engine
        .execute("INSERT INTO o VALUES(3,'incoming',POINT(3,3)),(4,'deleted',POINT(4,4))")
        .unwrap();
    (dir, engine)
}
fn rows(e: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(r) = e.execute(sql).unwrap().materialize().unwrap() else {
        panic!("rows")
    };
    r.rows
}

#[test]
fn exchange_validation_admits_both_writers_and_rechecks_commits() {
    let (dir, engine) = fixture();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut s = SessionContext::default();
        s.connection_id = 38101;
        worker.execute_with_session(&mut s, "ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut s = SessionContext::default();
    s.connection_id = 38102;
    s.sysvars.lock_wait_timeout = Some(0);
    let writes = [
        "BEGIN",
        "UPDATE o SET id=5,v=REPEAT('x',20000),g=POINT(5,5) WHERE id=3",
        "DELETE FROM o WHERE id=4",
        "INSERT INTO o VALUES(6,'six',POINT(6,6))",
        "UPDATE t SET id=2 WHERE id=1",
        "UPDATE t SET id=12 WHERE id=11",
        "SAVEPOINT s",
        "INSERT INTO o VALUES(99,'rolled back',POINT(99,99))",
        "ROLLBACK TO s",
        "COMMIT",
    ]
    .map(|sql| engine.execute_with_session(&mut s, sql));
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    for write in writes {
        write.expect("validation must admit writes on both participants");
    }
    result.unwrap();
    assert_eq!(
        rows(&engine, "SELECT id,LENGTH(v) FROM t ORDER BY id"),
        vec![
            vec![Value::Int(5), Value::Int(20000)],
            vec![Value::Int(6), Value::Int(3)],
            vec![Value::Int(12), Value::Int(8)]
        ]
    );
    assert_eq!(rows(&engine, "SELECT id FROM o"), vec![vec![Value::Int(2)]]);
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        rows(&engine, "SELECT id FROM t ORDER BY id"),
        vec![
            vec![Value::Int(5)],
            vec![Value::Int(6)],
            vec![Value::Int(12)]
        ]
    );
    assert_eq!(rows(&engine, "SELECT id FROM o"), vec![vec![Value::Int(2)]]);
    engine
        .execute("UPDATE t SET v=REPEAT('z',22000),g=POINT(7,7) WHERE id=5")
        .unwrap();
}

#[test]
fn exchange_leaves_unrelated_dirty_heap_and_preserves_physical_files() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE u(id INT PRIMARY KEY,b VARBINARY(1000))")
        .unwrap();
    e.checkpoint().unwrap();
    insert_fixture_rows(
        &e,
        "test.u",
        (0..2000).map(|i| vec![Value::Int(i), Value::Bytes(vec![b'u'; 1000])]),
    );
    let before = e.database().heap_dirty_pages_for_test("test.u");
    assert!(before > 0);
    e.execute("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o")
        .unwrap();
    assert!(
        e.database().heap_dirty_pages_for_test("test.u") > 0,
        "EXCHANGE must not flush the unrelated heap as a whole"
    );
    assert_eq!(
        rows(&e, "SELECT COUNT(*) FROM u"),
        vec![vec![Value::Int(2000)]]
    );
}

#[test]
fn exchange_rejects_late_out_of_bounds_commit_and_accepts_repaired_snapshot() {
    for repaired in [false, true] {
        let (_dir, e) = fixture();
        if repaired {
            e.execute("UPDATE o SET id=99 WHERE id=3").unwrap();
        }
        let source = e.database().table("test.t").unwrap();
        let other = e.database().table("test.o").unwrap();
        let hook = e.database().install_ddl_backfill_test_hook();
        let worker = e.clone();
        let ddl = std::thread::spawn(move || {
            worker.execute("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o,LOCK=NONE")
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let write = e.execute(if repaired {
            "UPDATE o SET id=5 WHERE id=99"
        } else {
            "UPDATE o SET id=99 WHERE id=3"
        });
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{result:?}");
        write.unwrap();
        if repaired {
            result.unwrap();
            assert_eq!(rows(&e, "SELECT id FROM o"), vec![vec![Value::Int(1)]]);
        } else {
            assert_eq!(result.unwrap_err().code(), 1737);
            assert_eq!(
                e.database().table("test.t").unwrap().storage_ids(),
                source.storage_ids()
            );
            assert_eq!(e.database().table("test.o").unwrap().id, other.id);
            assert_eq!(
                rows(&e, "SELECT id FROM o ORDER BY id"),
                vec![vec![Value::Int(4)], vec![Value::Int(99)]]
            );
        }
    }
}

#[test]
fn exchange_shared_and_exclusive_lock_contracts() {
    for mode in ["SHARED", "EXCLUSIVE"] {
        let (_dir, e) = fixture();
        let hook = e.database().install_ddl_backfill_test_hook();
        let worker = e.clone();
        let sql = format!("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o,LOCK={mode}");
        let ddl = std::thread::spawn(move || {
            let mut s = SessionContext::default();
            s.connection_id = 38201;
            worker.execute_with_session(&mut s, &sql)
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let mut s = SessionContext::default();
        s.connection_id = 38202;
        s.sysvars.lock_wait_timeout = Some(0);
        let reads = ["SELECT * FROM t", "SELECT * FROM o"].map(|sql| {
            e.execute_with_session(&mut s, sql)
                .and_then(QueryResponse::materialize)
        });
        let writes = [
            "UPDATE t SET id=2 WHERE id=1",
            "UPDATE o SET id=5 WHERE id=3",
            "ALTER TABLE o ADD extra INT",
        ]
        .map(|sql| e.execute_with_session(&mut s, sql));
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{result:?}");
        result.unwrap();
        for read in reads {
            if mode == "SHARED" {
                read.unwrap();
            } else {
                assert_eq!(read.unwrap_err().code(), 1205);
            }
        }
        for write in writes {
            assert_eq!(write.unwrap_err().code(), 1205);
        }
    }
}

#[test]
fn exchange_cancellation_and_publication_timeout_preserve_source_writes() {
    for cancel in [false, true] {
        let (_dir, e) = fixture();
        let source = e.database().table("test.t").unwrap();
        let hook = if cancel {
            e.database().install_ddl_backfill_test_hook()
        } else {
            e.database().install_online_copy_publish_test_hook()
        };
        let worker = e.clone();
        let ddl = std::thread::spawn(move || {
            let mut s = SessionContext::default();
            s.connection_id = 38301;
            s.sysvars.lock_wait_timeout = Some(0);
            worker.execute_with_session(
                &mut s,
                "ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o,LOCK=NONE",
            )
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let write = e.execute("UPDATE o SET v=REPEAT('x',20000) WHERE id=3");
        let mut reader = SessionContext::default();
        reader.connection_id = 38302;
        e.execute_with_session(&mut reader, "BEGIN").unwrap();
        e.execute_with_session(&mut reader, "SELECT * FROM o")
            .unwrap()
            .materialize()
            .unwrap();
        if cancel {
            let job = e
                .database()
                .alter_copy_job_snapshots()
                .into_iter()
                .find(|j| j.table_id == source.id && j.cancel_allowed)
                .unwrap();
            e.database().cancel_ddl_job(job.job_id).unwrap();
        }
        hook.release();
        let result = ddl.join().unwrap();
        e.execute_with_session(&mut reader, "ROLLBACK").unwrap();
        assert!(reached, "{result:?}");
        write.unwrap();
        assert_eq!(result.unwrap_err().code(), if cancel { 1317 } else { 1205 });
        assert_eq!(
            e.database().table("test.t").unwrap().storage_ids(),
            source.storage_ids()
        );
        assert_eq!(
            rows(&e, "SELECT LENGTH(v) FROM o WHERE id=3"),
            vec![vec![Value::Int(20000)]]
        );
        e.execute("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o,LOCK=NONE")
            .unwrap();
    }
}

#[test]
fn exchange_uses_bounded_retry_without_blocking_writers() {
    let (_dir, e) = fixture();
    let mut hook = e.database().install_ddl_backfill_test_hook();
    let worker = e.clone();
    let ddl = std::thread::spawn(move || {
        worker.execute("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o,LOCK=NONE")
    });
    for attempt in 0..8 {
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        if !reached {
            hook.release();
            panic!("attempt {attempt}: {:?}", ddl.join().unwrap());
        }
        let write = e.execute("UPDATE o SET v=CONCAT(v,'x') WHERE id=3");
        let next = (attempt < 7).then(|| e.database().install_ddl_backfill_test_hook());
        hook.release();
        write.unwrap();
        if let Some(next) = next {
            hook = next;
        }
    }
    assert_eq!(ddl.join().unwrap().unwrap_err().code(), 1205);
    assert_eq!(
        rows(&e, "SELECT LENGTH(v) FROM o WHERE id=3"),
        vec![vec![Value::Int(16)]]
    );
    assert_eq!(
        rows(&e, "SELECT id FROM t ORDER BY id"),
        vec![vec![Value::Int(1)], vec![Value::Int(11)]]
    );
}

#[test]
fn exchange_fences_older_snapshots_and_preserves_population() {
    let (dir, e) = fixture();
    let stale = e.database().table("test.o").unwrap();
    let point = rows(&e, "SELECT POINT(9,9)")[0][0].clone();
    let mut reader = SessionContext::default();
    reader.connection_id = 38402;
    reader.isolation = crate::storage::IsolationLevel::RepeatableRead;
    e.execute_with_session(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    e.execute("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o,LOCK=NONE")
        .unwrap();
    let old = e
        .execute_with_session(&mut reader, "SELECT * FROM o")
        .and_then(QueryResponse::materialize);
    e.execute_with_session(&mut reader, "ROLLBACK").unwrap();
    assert_eq!(old.unwrap_err().code(), 1412);
    e.execute("ALTER TABLE t CHECK PARTITION ALL").unwrap();
    assert_eq!(rows(&e,"SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA='test' AND TABLE_NAME IN('t','o') ORDER BY TABLE_NAME"),vec![vec![Value::Int(1)],vec![Value::Int(3)]]);
    drop(e);
    let e = open_strict_test_engine(dir.path());
    let txn = e.database().begin_txn().unwrap();
    let write = e.database().insert_bound_primary_test_row(
        txn,
        &stale,
        vec![Value::Int(99), Value::String("stale".into()), point],
    );
    e.database().rollback_txn(txn.txn_id).unwrap();
    assert_eq!(
        write.unwrap_err().code(),
        1412,
        "restart must restore ownership write floors"
    );
    e.execute("ALTER TABLE t CHECK PARTITION ALL").unwrap();
    e.execute("UPDATE o SET g=POINT(9,9),v=REPEAT('z',22000) WHERE id=1")
        .unwrap();
    assert_eq!(
        rows(&e, "SELECT id,LENGTH(v) FROM o"),
        vec![vec![Value::Int(1), Value::Int(22000)]]
    );
    assert_eq!(rows(&e,"SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA='test' AND TABLE_NAME IN('t','o') ORDER BY TABLE_NAME"),vec![vec![Value::Int(1)],vec![Value::Int(3)]]);
}

#[test]
#[ignore = "local EXCHANGE lock scaling comparison"]
fn exchange_lock_scaling_probe() {
    for count in [1000, 10000, 100000] {
        for mode in ["EXCLUSIVE", "NONE"] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let e = open_strict_test_engine(dir.path());
            let def =
                "(id INT PRIMARY KEY,v LONGTEXT,g GEOMETRY NOT NULL,SPATIAL sp(g),INDEX vi(v(8)))";
            e.execute(&format!("CREATE TABLE t{def} PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN({}),PARTITION p1 VALUES LESS THAN MAXVALUE)",count+1)).unwrap();
            e.execute(&format!("CREATE TABLE o{def}")).unwrap();
            e.execute(&format!(
                "INSERT INTO t VALUES(1,'outgoing',POINT(1,1)),({},'retained',POINT(2,2))",
                count + 2
            ))
            .unwrap();
            let point = rows(&e, "SELECT POINT(1,1)")[0][0].clone();
            insert_fixture_rows(
                &e,
                "test.o",
                (1..=count).map(|id| {
                    vec![
                        Value::Int(id),
                        Value::String("incoming".into()),
                        point.clone(),
                    ]
                }),
            );
            e.checkpoint().unwrap();
            let before = e.database().exchange_hold_us_for_test();
            let started = Instant::now();
            e.execute(&format!(
                "ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o,LOCK={mode}"
            ))
            .unwrap();
            eprintln!(
                "EXCHANGE_PROBE rows={count} mode={mode} elapsed_us={} hold_us={}",
                started.elapsed().as_micros(),
                e.database().exchange_hold_us_for_test() - before
            );
            assert_eq!(
                rows(&e, "SELECT COUNT(*) FROM t"),
                vec![vec![Value::Int(count + 1)]]
            );
            assert_eq!(
                rows(&e, "SELECT COUNT(*) FROM o"),
                vec![vec![Value::Int(1)]]
            );
        }
    }
}

#[test]
fn exchange_memory_identity_and_unsupported_transfer_contracts() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    for name in ["t", "o"] {
        e.execute(&format!("CREATE TABLE {name}(id INT PRIMARY KEY,v TEXT,INDEX vi(v(8))){}",
            if name == "t" {" PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)"} else {""})).unwrap();
    }
    e.execute("INSERT INTO t VALUES(1,REPEAT('a',20000)),(11,'retained')")
        .unwrap();
    e.execute("INSERT INTO o VALUES(3,REPEAT('b',24000))")
        .unwrap();
    e.execute("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o,LOCK=NONE")
        .unwrap();
    assert_eq!(
        rows(&e, "SELECT id,LENGTH(v) FROM o"),
        vec![vec![Value::Int(1), Value::Int(20000)]]
    );
    e.execute("DROP INDEX vi ON o").unwrap();
    e.execute("CREATE INDEX vi ON o(v(8))").unwrap();
    for mode in ["NONE", "SHARED"] {
        assert_eq!(
            e.execute(&format!(
                "ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o,LOCK={mode}"
            ))
            .unwrap_err()
            .code(),
            1235
        );
    }
    assert_eq!(
        rows(&e, "SELECT id,LENGTH(v) FROM o"),
        vec![vec![Value::Int(1), Value::Int(20000)]]
    );
}

#[test]
fn exchange_publication_process_child() {
    let Ok(path) = std::env::var("MYDB_EXCHANGE_CHILD_DIR") else {
        return;
    };
    let e = open_strict_test_engine(std::path::Path::new(&path));
    let def = "(id INT PRIMARY KEY,v LONGTEXT,g GEOMETRY NOT NULL,SPATIAL sp(g),INDEX vi(v(8)))";
    e.execute(&format!("CREATE TABLE t{def} PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)")).unwrap();
    e.execute(&format!("CREATE TABLE o{def}")).unwrap();
    e.execute("INSERT INTO t VALUES(1,REPEAT('a',20000),POINT(1,1)),(11,'retained',POINT(11,11))")
        .unwrap();
    e.execute("INSERT INTO o VALUES(3,REPEAT('b',24000),POINT(3,3))")
        .unwrap();
    e.execute("ANALYZE TABLE t,o").unwrap();
    let result = e.execute("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o,LOCK=NONE");
    assert!(
        std::env::var("MYDB_EXCHANGE_FAIL_STAGE").is_ok(),
        "crash point missed: {result:?}"
    );
    assert_eq!(result.unwrap_err().code(), 1030);
    assert!(e.database().ensure_operational().is_err());
    assert!(e
        .execute("INSERT INTO o VALUES(7,'unsafe',POINT(7,7))")
        .is_err());
}

#[test]
fn exchange_recovers_all_publication_failures_and_abrupt_exits() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    for crash in [false, true] {
        for phase in 1..=3 {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let log = std::fs::File::create(dir.path().join("child.log")).unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact","engine::executor::tests::ddl_exchange_online::exchange_publication_process_child","--nocapture"])
                .env("MYDB_EXCHANGE_CHILD_DIR",dir.path())
                .env(if crash {"MYDB_EXCHANGE_CRASH_STAGE"} else {"MYDB_EXCHANGE_FAIL_STAGE"},phase.to_string())
                .stdout(log.try_clone().unwrap()).stderr(log).status().unwrap();
            assert_eq!(
                status.code(),
                Some(if crash { 86 } else { 0 }),
                "{}",
                std::fs::read_to_string(dir.path().join("child.log")).unwrap()
            );
            let e = open_strict_test_engine(dir.path());
            assert_eq!(
                rows(&e, "SELECT id,LENGTH(v) FROM o"),
                vec![if phase == 1 {
                    vec![Value::Int(3), Value::Int(24000)]
                } else {
                    vec![Value::Int(1), Value::Int(20000)]
                }]
            );
            e.execute("ALTER TABLE t CHECK PARTITION ALL").unwrap();
            e.execute("UPDATE o SET v=REPEAT('z',26000),g=POINT(9,9)")
                .unwrap();
            let total=rows(&e,"SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA='test' AND TABLE_NAME IN('t','o') ORDER BY TABLE_NAME");
            assert_eq!(total, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
            drop(e);
            let e = open_strict_test_engine(dir.path());
            e.execute("ALTER TABLE t CHECK PARTITION ALL").unwrap();
            assert_eq!(
                rows(&e, "SELECT LENGTH(v) FROM o"),
                vec![vec![Value::Int(26000)]]
            );
        }
    }
}

#[test]
fn exchange_rejects_spatial_identity_relocation_without_touching_data() {
    let (_dir, e) = fixture();
    let source = e.database().table("test.t").unwrap().storage_ids();
    e.execute("DROP INDEX sp ON o").unwrap();
    e.execute("CREATE SPATIAL INDEX sp ON o(g)").unwrap();
    assert_eq!(
        e.execute("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o")
            .unwrap_err()
            .code(),
        1235
    );
    assert_eq!(e.database().table("test.t").unwrap().storage_ids(), source);
    assert_eq!(
        rows(&e, "SELECT id FROM o ORDER BY id"),
        vec![vec![Value::Int(3)], vec![Value::Int(4)]]
    );
    e.execute("UPDATE o SET g=POINT(9,9) WHERE id=3").unwrap();
}

#[test]
fn exchange_native_replication_reuses_physical_files_and_resets_population() {
    let (dir, source) = fixture();
    source.execute("ANALYZE TABLE t,o").unwrap();
    source.checkpoint().unwrap();
    let rd = tempfile::tempdir_in("/tmp/mydb").unwrap();
    crate::util::file::copy_fixture_tree(dir.path(), rd.path()).unwrap();
    let mut role = crate::repl::role::RoleState::new_standalone();
    role.role = crate::repl::role::Role::Replica;
    crate::repl::role::save(rd.path(), &role).unwrap();
    let db = source.database_for_replication();
    let mut start = db.wal_log().lock().next_lsn();
    for phase in 0..3 {
        let replica = open_strict_test_engine(rd.path());
        let stale = replica.database().table("test.o").unwrap();
        source.execute("UPDATE o SET v=CONCAT(v,'x')").unwrap();
        source
            .execute("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o,LOCK=NONE")
            .unwrap();
        let log = db.wal_log();
        let safe = log.lock().next_lsn() - 1;
        crate::wal::LogManager::sync_durable_to(&log, safe).unwrap();
        let target = replica.database_for_replication().clone();
        let mut applier = crate::repl::replica::applier::Applier::with_database(
            rd.path(),
            target.wal_log(),
            target.clone(),
            crate::repl::replica::progress::ReplicaProgress::default(),
        );
        let mut reader = crate::wal::WalReader::open(&log, start).unwrap();
        while let Some(chunk) = reader.next_chunk(safe, 1 << 20).unwrap() {
            applier.apply_chunk(&chunk, &role.node_uuid, 1).unwrap();
        }
        for query in ["SELECT id,v FROM t ORDER BY id","SELECT id,v FROM o ORDER BY id","SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA='test' AND TABLE_NAME IN('t','o') ORDER BY TABLE_NAME"] {
            assert_eq!(rows(&replica,query),rows(&source,query),"phase={phase} {query}");
        }
        if phase == 2 {
            target.set_physical_replica_mode(false).unwrap();
            let txn = target.begin_txn().unwrap();
            let point = rows(&source, "SELECT POINT(9,9)")[0][0].clone();
            let write = target.insert_bound_primary_test_row(
                txn,
                &stale,
                vec![Value::Int(99), Value::String("stale".into()), point],
            );
            target.rollback_txn(txn.txn_id).unwrap();
            assert_eq!(
                write.unwrap_err().code(),
                1412,
                "replay must fence the former standalone binding before promotion"
            );
        }
        target.flush_dirty_data().unwrap();
        let last = target.wal_log().lock().next_lsn() - 1;
        crate::wal::LogManager::sync_durable_to(&target.wal_log(), last).unwrap();
        start = safe + 1;
    }
}
