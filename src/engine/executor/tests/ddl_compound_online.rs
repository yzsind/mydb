use super::*;

fn rows(e: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = e.execute(sql).unwrap().materialize().unwrap() else {
        panic!("expected rows");
    };
    result.rows
}

fn assert_metadata(e: &StorageQueryEngine, published: bool) {
    let t = e.database().table("test.t").unwrap();
    let v = &t.columns[t.column_index("v").unwrap()];
    assert_eq!(v.nullable, !published);
    assert_eq!(v.default, Some(Value::Int(if published { 7 } else { 3 })));
    assert_eq!(t.comment, if published { "published" } else { "" });
    assert_eq!(
        t.columns[t.column_index("body").unwrap()].invisible,
        published
    );
    assert_eq!(
        t.indexes.iter().find(|i| i.name == "vi").unwrap().visible,
        !published
    );
    assert_eq!(t.check_constraints.len(), usize::from(published));
}

const MIXED: &str = "ALTER TABLE t MODIFY v INT NOT NULL,ALTER COLUMN v SET DEFAULT 7,ALTER COLUMN body SET INVISIBLE,ALTER INDEX vi INVISIBLE,ADD CONSTRAINT positive CHECK(v>0),COMMENT='published',LOCK=NONE";

fn setup(e: &StorageQueryEngine, partition: &str, initial_null: bool) {
    e.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,v INT DEFAULT 3,body TEXT,INDEX vi(v),FULLTEXT ft(body)){partition}")).unwrap();
    e.execute(&format!(
        "INSERT INTO t VALUES(1,{},'quartz amber'),(2,20,'ruby quartz')",
        if initial_null { "NULL" } else { "10" }
    ))
    .unwrap();
}

#[test]
fn compound_validation_replays_commits_repairs_and_rollback_without_partial_metadata() {
    for memory in [false, true] {
        for partition in ["", " PARTITION BY HASH(id) PARTITIONS 2"] {
            for after_scan in [false, true] {
                for invalid in [false, true] {
                    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
                    let e = Arc::new(if memory {
                        open_complete_memory_test_engine(dir.path())
                    } else {
                        open_strict_test_engine(dir.path())
                    });
                    setup(&e, partition, !invalid);
                    let source = e.database().table("test.t").unwrap();
                    let hook = if after_scan {
                        e.database().install_online_index_replay_test_hook()
                    } else {
                        e.database().install_ddl_backfill_test_hook()
                    };
                    let worker = e.clone();
                    let ddl = std::thread::spawn(move || {
                        let mut s = SessionContext::default();
                        s.connection_id = 41201;
                        worker.execute_with_session(&mut s, MIXED)
                    });
                    let reached = hook.wait_until_reached(Duration::from_secs(10));
                    assert_metadata(&e, false);
                    let mut writer = SessionContext::default();
                    writer.connection_id = 41202;
                    writer.sysvars.lock_wait_timeout = Some(0);
                    let writes = [
                        "BEGIN",
                        "UPDATE t SET id=4,v=11,body='quartz topaz' WHERE id=1",
                        "SAVEPOINT a",
                        "UPDATE t SET v=NULL WHERE id=2",
                        "ROLLBACK TO a",
                        "INSERT INTO t(id,body) VALUES(3,'quartz cobalt')",
                        "COMMIT",
                    ]
                    .map(|sql| e.execute_with_session(&mut writer, sql));
                    let bad = invalid.then(|| {
                        e.execute_with_session(&mut writer, "UPDATE t SET v=NULL WHERE id=2")
                    });
                    e.database().purge_obsolete_undo();
                    hook.release();
                    let result = ddl.join().unwrap();
                    assert!(reached, "{result:?}");
                    for write in writes {
                        write.unwrap();
                    }
                    if let Some(bad) = bad {
                        bad.unwrap();
                    }
                    if invalid {
                        assert_eq!(result.unwrap_err().code(), 1138);
                    } else {
                        result.unwrap();
                    }
                    assert_metadata(&e, !invalid);
                    let current = e.database().table("test.t").unwrap();
                    assert_eq!(current.active_generation, source.active_generation);
                    assert_eq!(current.storage_ids(), source.storage_ids());
                    if invalid {
                        assert_eq!(current.schema_version, source.schema_version);
                    }
                    assert_eq!(rows(&e,"SELECT id FROM t WHERE MATCH(body) AGAINST('quartz' IN BOOLEAN MODE) ORDER BY id"),vec![vec![Value::Int(2)],vec![Value::Int(3)],vec![Value::Int(4)]]);
                    assert_eq!(e.database().active_snapshot_count(), 0);
                    if !invalid {
                        let txn = e.database().begin_txn().unwrap();
                        let write = e.database().insert_bound_primary_test_row(
                            txn,
                            &source,
                            vec![Value::Int(99), Value::Null, Value::String("stale".into())],
                        );
                        e.database().rollback_txn(txn.txn_id).unwrap();
                        assert_eq!(write.unwrap_err().code(), 1412);
                        assert!(e.execute("INSERT INTO t(id,v) VALUES(99,-1)").is_err());
                    }
                    if !memory {
                        drop(e);
                        let e = open_strict_cold_metadata_engine(dir.path());
                        assert_metadata(&e, !invalid);
                        e.execute("UPDATE t SET body='quartz violet' WHERE id=4")
                            .unwrap();
                    }
                }
            }
        }
    }
}

#[test]
fn compound_check_and_metadata_publish_with_fk_backing_index_and_parent_edges() {
    for orphan in [false, true] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let e = Arc::new(open_strict_test_engine(dir.path()));
        e.execute("CREATE TABLE p(id INT PRIMARY KEY)").unwrap();
        e.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT DEFAULT 3,body TEXT)")
            .unwrap();
        e.execute("INSERT INTO p VALUES(1),(2)").unwrap();
        e.execute("INSERT INTO t VALUES(1,1,'a'),(2,2,'b')")
            .unwrap();
        let hook = e.database().install_ddl_backfill_test_hook();
        let worker = e.clone();
        let ddl = std::thread::spawn(move || {
            worker.execute("ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(v) REFERENCES p(id),ADD CONSTRAINT pos CHECK(v>0),ALTER COLUMN v SET DEFAULT 1,COMMENT='fk published',LOCK=NONE")
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let mut s = SessionContext::default();
        s.connection_id = 41203;
        s.sysvars.lock_wait_timeout = Some(0);
        let write = e.execute_with_session(
            &mut s,
            if orphan {
                "DELETE FROM p WHERE id=2"
            } else {
                "UPDATE t SET v=1 WHERE id=2"
            },
        );
        assert!(e
            .database()
            .table("test.t")
            .unwrap()
            .foreign_keys
            .is_empty());
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{result:?}");
        write.unwrap();
        if orphan {
            assert_eq!(result.unwrap_err().code(), 1452);
        } else {
            result.unwrap();
        }
        let t = e.database().table("test.t").unwrap();
        assert_eq!(t.foreign_keys.len(), usize::from(!orphan));
        assert_eq!(t.check_constraints.len(), usize::from(!orphan));
        assert_eq!(t.comment, if orphan { "" } else { "fk published" });
        assert!(t
            .indexes
            .iter()
            .all(|i| i.state == crate::storage::IndexState::Public));
        assert_eq!(
            e.database().table("test.p").unwrap().fk_referenced_by.len(),
            usize::from(!orphan)
        );
        drop(t);
        drop(e);
        let e = open_strict_cold_metadata_engine(dir.path());
        if !orphan {
            assert_eq!(
                e.execute("DELETE FROM p WHERE id=1").unwrap_err().code(),
                1451
            );
            assert_eq!(
                e.execute("INSERT INTO t VALUES(3,99,'bad')")
                    .unwrap_err()
                    .code(),
                1452
            );
        } else {
            e.execute("INSERT INTO t VALUES(3,99,'allowed')").unwrap();
        }
    }
}

#[test]
fn compound_metadata_candidate_preserves_backfills_and_publishes_once() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_strict_test_engine(dir.path());
    e.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    e.execute("INSERT INTO t VALUES(1)").unwrap();
    e.execute("ALTER TABLE t ADD v INT DEFAULT 3").unwrap();
    let before = e.database().table("test.t").unwrap();
    let mut reader = SessionContext::default();
    reader.connection_id = 41209;
    e.execute_with_session(&mut reader, "BEGIN").unwrap();
    e.execute_with_session(&mut reader, "SELECT id,v FROM t")
        .unwrap()
        .materialize()
        .unwrap();
    let mut ddl = SessionContext::default();
    ddl.connection_id = 41210;
    ddl.sysvars.lock_wait_timeout = Some(0);
    e.execute_with_session(&mut ddl, "ALTER TABLE t ALTER COLUMN v SET DEFAULT 7,ALTER COLUMN v SET INVISIBLE,COMMENT='one candidate',PCTFREE=17,LOCK=NONE").unwrap();
    e.execute_with_session(&mut reader, "COMMIT").unwrap();
    let after = e.database().table("test.t").unwrap();
    assert_eq!(after.schema_version, before.schema_version + 1);
    assert_eq!(after.pctfree, 17);
    assert_eq!(after.comment, "one candidate");
    assert_eq!(
        rows(&e, "SELECT v FROM t WHERE id=1"),
        vec![vec![Value::Int(3)]]
    );
    e.execute("INSERT INTO t(id) VALUES(2)").unwrap();
    assert_eq!(
        rows(&e, "SELECT v FROM t WHERE id=2"),
        vec![vec![Value::Int(7)]]
    );
    drop(after);
    drop(before);
    drop(e);
    let e = open_strict_cold_metadata_engine(dir.path());
    assert_eq!(
        rows(&e, "SELECT id,v FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(3)],
            vec![Value::Int(2), Value::Int(7)]
        ]
    );
}

#[test]
fn compound_validation_cancel_preserves_metadata_and_committed_writes() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = Arc::new(open_strict_test_engine(dir.path()));
    setup(&e, "", true);
    let hook = e.database().install_online_index_replay_test_hook();
    let worker = e.clone();
    let ddl = std::thread::spawn(move || worker.execute(MIXED));
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut s = SessionContext::default();
    s.connection_id = 41204;
    let write = e.execute_with_session(&mut s, "UPDATE t SET v=10 WHERE id=1");
    let jobs = e.database().alter_copy_job_snapshots();
    let cancelled = jobs
        .first()
        .map(|job| e.database().cancel_ddl_job(job.job_id));
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached);
    write.unwrap();
    cancelled.unwrap().unwrap();
    assert_eq!(result.unwrap_err().code(), 1317);
    assert_metadata(&e, false);
    assert_eq!(e.database().active_snapshot_count(), 0);
    e.execute(MIXED).unwrap();
    assert_metadata(&e, true);
}

#[test]
fn compound_validation_respects_explicit_shared_and_exclusive_locks() {
    for mode in ["SHARED", "EXCLUSIVE"] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let e = Arc::new(open_strict_test_engine(dir.path()));
        setup(&e, "", false);
        let hook = e.database().install_ddl_backfill_test_hook();
        let worker = e.clone();
        let sql = MIXED.replace("LOCK=NONE", &format!("LOCK={mode}"));
        let ddl = std::thread::spawn(move || worker.execute(&sql));
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let mut s = SessionContext::default();
        s.connection_id = 41205;
        s.sysvars.lock_wait_timeout = Some(0);
        let read = e.execute_with_session(&mut s, "SELECT id,v FROM t");
        let write = e.execute_with_session(&mut s, "UPDATE t SET v=12 WHERE id=1");
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{result:?}");
        result.unwrap();
        assert_eq!(write.unwrap_err().code(), 1205);
        if mode == "SHARED" {
            read.unwrap();
        } else {
            assert_eq!(read.unwrap_err().code(), 1205);
        }
        assert_metadata(&e, true);
    }
}

#[test]
fn compound_validation_old_repeatable_read_preserves_historical_null_facts() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = Arc::new(open_strict_test_engine(dir.path()));
    setup(&e, "", true);
    let mut reader = SessionContext::default();
    reader.connection_id = 41206;
    e.execute_with_session(
        &mut reader,
        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
    )
    .unwrap();
    e.execute_with_session(&mut reader, "BEGIN").unwrap();
    e.execute_with_session(&mut reader, "SELECT v FROM t WHERE id=1")
        .unwrap()
        .materialize()
        .unwrap();
    e.execute("UPDATE t SET v=10 WHERE id=1").unwrap();
    e.execute(MIXED).unwrap();
    let QueryResponse::ResultSet(result) = e
        .execute_with_session(&mut reader, "SELECT id FROM t WHERE v IS NULL ORDER BY id")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("rows");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
    e.execute_with_session(&mut reader, "COMMIT").unwrap();
    assert!(rows(&e, "SELECT id FROM t WHERE v IS NULL").is_empty());
}

#[test]
fn compound_validation_supports_domain_checks_with_metadata_and_rejects_physical_mixes() {
    for (source, target, good, bad) in [
        ("DECIMAL(8,2)", "DECIMAL(7,2)", "123.45", "123456.78"),
        ("VARBINARY(64)", "VARBINARY(32)", "'ok'", "REPEAT('x',40)"),
        ("LONGTEXT", "TEXT", "'ok'", "REPEAT('x',70000)"),
    ] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let e = open_strict_test_engine(dir.path());
        e.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,v {source})"))
            .unwrap();
        e.execute(&format!("INSERT INTO t VALUES(1,{good})"))
            .unwrap();
        let before = e.database().table("test.t").unwrap();
        e.execute(&format!(
            "ALTER TABLE t MODIFY v {target},COMMENT='domain',LOCK=NONE"
        ))
        .unwrap();
        assert_eq!(
            e.database().table("test.t").unwrap().active_generation,
            before.active_generation
        );
        assert_eq!(e.database().table("test.t").unwrap().comment, "domain");
        assert!(e
            .execute(&format!("INSERT INTO t VALUES(2,{bad})"))
            .is_err());
    }
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_strict_test_engine(dir.path());
    setup(&e, "", false);
    for sql in [
        "ALTER TABLE t MODIFY v BIGINT,COMMENT='no partial',LOCK=NONE",
        "ALTER TABLE t MODIFY v INT NOT NULL,ZONE_MAP=OFF,LOCK=NONE",
        "ALTER TABLE t ALTER COLUMN v SET DEFAULT 7,ALTER INDEX PRIMARY INVISIBLE,LOCK=NONE",
    ] {
        assert!(e.execute(sql).is_err(), "{sql}");
        assert_metadata(&e, false);
    }
}

fn run_compound_replication(partition: &str) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let source = open_strict_test_engine(dir.path());
    setup(&source, partition, false);
    source.checkpoint().unwrap();
    let rd = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    crate::util::file::copy_fixture_tree(dir.path(), rd.path()).unwrap();
    let mut role = crate::repl::role::RoleState::new_standalone();
    role.role = crate::repl::role::Role::Replica;
    crate::repl::role::save(rd.path(), &role).unwrap();
    let replica = open_strict_test_engine(rd.path());
    let stale = replica.database().table("test.t").unwrap();
    let db = source.database_for_replication();
    let start = db.wal_log().lock().next_lsn();
    source.execute(MIXED).unwrap();
    source
        .execute("UPDATE t SET v=12,body='quartz violet' WHERE id=1")
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
    assert_metadata(&replica, true);
    assert_eq!(
        rows(&replica, "SELECT id,v,body FROM t ORDER BY id"),
        rows(&source, "SELECT id,v,body FROM t ORDER BY id")
    );
    target.set_physical_replica_mode(false).unwrap();
    let txn = target.begin_txn().unwrap();
    let write = target.insert_bound_primary_test_row(
        txn,
        &stale,
        vec![Value::Int(99), Value::Null, Value::String("stale".into())],
    );
    target.rollback_txn(txn.txn_id).unwrap();
    let err = write.unwrap_err();
    assert_eq!(err.code(), 1412, "{partition}: {err:?}");
    drop(applier);
    drop(target);
    drop(replica);
    let replica = open_strict_cold_metadata_engine(rd.path());
    assert_metadata(&replica, true);
    assert_eq!(
        rows(&replica, "SELECT id,v FROM t ORDER BY id"),
        rows(&source, "SELECT id,v FROM t ORDER BY id")
    );
}

#[test]
fn compound_validation_native_replica_replays_complete_definition_and_write_floor() {
    run_compound_replication(" PARTITION BY HASH(id) PARTITIONS 2");
}

#[test]
fn compound_validation_native_replica_rejects_stale_ordinary_table_binding() {
    run_compound_replication("");
}

#[test]
fn compound_publication_crash_child() {
    let Ok(directory) = std::env::var("MYDB_COMPOUND_CRASH_DIR") else {
        return;
    };
    let phase = std::env::var("MYDB_COMPOUND_CRASH_PHASE")
        .unwrap()
        .parse()
        .unwrap();
    let e = open_strict_test_engine(std::path::Path::new(&directory));
    let with_fk = std::env::var_os("MYDB_COMPOUND_CRASH_FK").is_some();
    setup(
        &e,
        if with_fk {
            ""
        } else {
            " PARTITION BY HASH(id) PARTITIONS 2"
        },
        false,
    );
    if with_fk {
        e.execute("CREATE TABLE p(id INT PRIMARY KEY)").unwrap();
        e.execute("INSERT INTO p VALUES(10),(20)").unwrap();
    }
    e.checkpoint().unwrap();
    e.database()
        .set_constraint_publication_crash_for_test(phase);
    let sql = if with_fk {
        MIXED.replace(
            "LOCK=NONE",
            "ADD CONSTRAINT fk FOREIGN KEY(v) REFERENCES p(id),LOCK=NONE",
        )
    } else {
        MIXED.to_string()
    };
    e.execute(&sql).unwrap();
    panic!("crash point was not reached");
}

#[test]
fn compound_publication_recovers_old_or_complete_definition_after_process_exit() {
    for (phase, with_fk) in [
        (1, false),
        (2, false),
        (3, false),
        (1, true),
        (2, true),
        (3, true),
    ] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        if with_fk {
            command.env("MYDB_COMPOUND_CRASH_FK", "1");
        }
        let output = command
            .args([
                "--exact",
                "engine::executor::tests::ddl_compound_online::compound_publication_crash_child",
                "--nocapture",
            ])
            .env("MYDB_COMPOUND_CRASH_DIR", dir.path())
            .env("MYDB_COMPOUND_CRASH_PHASE", phase.to_string())
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "phase={phase}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let e = open_strict_cold_metadata_engine(dir.path());
        assert_metadata(&e, phase == 3);
        let t = e.database().table("test.t").unwrap();
        assert_eq!(t.foreign_keys.len(), usize::from(with_fk && phase == 3));
        assert!(t
            .indexes
            .iter()
            .all(|index| index.state == crate::storage::IndexState::Public));
        if with_fk {
            assert_eq!(
                e.database().table("test.p").unwrap().fk_referenced_by.len(),
                usize::from(phase == 3)
            );
        }
        assert_eq!(
            rows(&e, "SELECT id,v FROM t ORDER BY id"),
            vec![
                vec![Value::Int(1), Value::Int(10)],
                vec![Value::Int(2), Value::Int(20)]
            ]
        );
        e.execute("UPDATE t SET body='quartz violet' WHERE id=1")
            .unwrap();
        if phase != 3 {
            let sql = if with_fk {
                MIXED.replace(
                    "LOCK=NONE",
                    "ADD CONSTRAINT fk FOREIGN KEY(v) REFERENCES p(id),LOCK=NONE",
                )
            } else {
                MIXED.to_string()
            };
            e.execute(&sql).unwrap();
        }
        assert_metadata(&e, true);
        assert!(e.execute("INSERT INTO t(id,v) VALUES(99,NULL)").is_err());
        if with_fk {
            assert_eq!(
                e.execute("DELETE FROM p WHERE id=10").unwrap_err().code(),
                1451
            );
            let checked = rows(&e, "CHECK TABLE t");
            assert!(
                checked
                    .iter()
                    .all(|row| !row[2].to_display_string().eq_ignore_ascii_case("error")),
                "{checked:?}"
            );
        } else {
            e.execute("ALTER TABLE t CHECK PARTITION ALL,LOCK=NONE")
                .unwrap();
        }
        drop(e);
        let e = open_strict_test_engine(dir.path());
        assert_metadata(&e, true);
    }
}

#[test]
fn compound_validation_accepts_online_metadata_companions() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_strict_test_engine(dir.path());
    e.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,body TEXT,INDEX vi(v))")
        .unwrap();
    e.execute("INSERT INTO t VALUES(1,10,'quartz')").unwrap();
    e.execute("ALTER TABLE t MODIFY v INT NOT NULL,ALTER COLUMN v SET DEFAULT 7,ALTER INDEX vi INVISIBLE,LOCK=NONE").unwrap();
    let t = e.database().table("test.t").unwrap();
    assert!(!t.columns[t.column_index("v").unwrap()].nullable);
    assert_eq!(
        t.columns[t.column_index("v").unwrap()].default,
        Some(Value::Int(7))
    );
    assert!(!t.indexes.iter().find(|i| i.name == "vi").unwrap().visible);
    assert!(e.execute("INSERT INTO t VALUES(2,NULL,'bad')").is_err());
}

#[test]
#[ignore = "local strict-durability compound validation lock comparison"]
fn compound_validation_lock_probe() {
    for count in [1000, 10000, 100000] {
        for mode in ["DEFAULT", "NONE", "EXCLUSIVE"] {
            let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
            let e = open_strict_test_engine(dir.path());
            e.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,INDEX vi(v))")
                .unwrap();
            insert_fixture_rows(
                &e,
                "test.t",
                (0..count).map(|i| vec![Value::Int(i), Value::Int(i)]),
            );
            let before = e.database().online_constraint_perf_snapshot();
            let started = Instant::now();
            let result = e.execute(&format!(
                "ALTER TABLE t MODIFY v INT NOT NULL,ALTER COLUMN v SET DEFAULT 7,LOCK={mode}"
            ));
            let elapsed = started.elapsed().as_micros();
            let after = e.database().online_constraint_perf_snapshot();
            eprintln!(
                "COMPOUND_PROBE rows={count} mode={mode} elapsed_us={elapsed} barrier_us={} ok={}",
                after[0] - before[0],
                result.is_ok()
            );
            result.unwrap();
        }
    }
}
#[test]
fn compound_validation_publish_timeout_leaves_old_definition_and_writer_rollback() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = Arc::new(open_strict_test_engine(dir.path()));
    setup(&e, "", false);
    let hook = e.database().install_online_index_replay_test_hook();
    let worker = e.clone();
    let ddl = std::thread::spawn(move || {
        let mut s = SessionContext::default();
        s.connection_id = 41207;
        s.sysvars.lock_wait_timeout = Some(0);
        worker.execute_with_session(&mut s, MIXED)
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 41208;
    e.execute_with_session(&mut writer, "BEGIN").unwrap();
    let write = e.execute_with_session(&mut writer, "UPDATE t SET v=NULL WHERE id=1");
    hook.release();
    let result = ddl.join().unwrap();
    e.execute_with_session(&mut writer, "ROLLBACK").unwrap();
    assert!(reached);
    write.unwrap();
    assert_eq!(result.unwrap_err().code(), 1205);
    assert_metadata(&e, false);
    assert_eq!(e.database().active_snapshot_count(), 0);
    e.execute(MIXED).unwrap();
    assert_metadata(&e, true);
}

#[test]
fn compound_validation_failed_catalog_commit_requires_recovery_without_partial_durable_metadata() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_strict_test_engine(dir.path());
    setup(&e, "", false);
    e.checkpoint().unwrap();
    e.database().fail_next_catalog_commit_for_test();
    assert_eq!(e.execute(MIXED).unwrap_err().code(), 1030);
    assert!(e.execute("INSERT INTO t(id,v) VALUES(99,10)").is_err());
    drop(e);
    let e = open_strict_cold_metadata_engine(dir.path());
    assert_metadata(&e, false);
    e.execute(MIXED).unwrap();
    assert_metadata(&e, true);
}

#[test]
fn compound_metadata_parser_preserves_quoted_names_comments_and_nested_commas() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_strict_test_engine(dir.path());
    e.execute("CREATE TABLE `odd table`(`id col` INT PRIMARY KEY,`v col` INT DEFAULT 3)")
        .unwrap();
    e.execute("INSERT INTO `odd table` VALUES(1,10)").unwrap();
    e.execute("ALTER /* head */ TABLE `odd table` MODIFY `v col` INT NOT NULL COMMENT 'x,y',ALTER COLUMN `v col` SET DEFAULT (ABS(-7)),COMMENT='z,LOCK=EXCLUSIVE',LOCK=NONE").unwrap();
    let t = e.database().table("test.odd table").unwrap();
    assert_eq!(t.comment, "z,LOCK=EXCLUSIVE");
    assert_eq!(t.columns[t.column_index("v col").unwrap()].comment, "x,y");
    e.execute("INSERT INTO `odd table`(`id col`) VALUES(2)")
        .unwrap();
    assert_eq!(
        rows(&e, "SELECT `v col` FROM `odd table` WHERE `id col`=2"),
        vec![vec![Value::Int(7)]]
    );
}
