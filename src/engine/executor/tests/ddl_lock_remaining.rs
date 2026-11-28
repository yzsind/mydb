use super::*;

fn fixture(spatial: bool) -> (tempfile::TempDir, Arc<StorageQueryEngine>) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    let extra = if spatial {
        ",g GEOMETRY NOT NULL,SPATIAL sp(g)"
    } else {
        ""
    };
    engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,v INT,INDEX vi(v){extra}) PARTITION BY HASH(id) PARTITIONS 2")).unwrap();
    let extra = if spatial {
        ",ST_GeomFromText('POINT(1 1)')"
    } else {
        ""
    };
    engine
        .execute(&format!(
            "INSERT INTO t VALUES(1,10{extra}),(2,20{extra}),(3,30{extra})"
        ))
        .unwrap();
    (dir, engine)
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("expected rows: {sql}")
    };
    result.rows
}

fn assert_indexes(engine: &StorageQueryEngine) {
    let db = engine.database();
    let table = db.table("test.t").unwrap();
    let txn = db.begin_read_only_txn().unwrap();
    for sid in table.storage_ids() {
        let source = db
            .scan_rows_storage_range(&table, sid, txn, 0, u64::MAX)
            .unwrap();
        for index in &table.indexes {
            let mut expected = source
                .iter()
                .flat_map(|row| {
                    table
                        .index_keys(index, &row.values)
                        .unwrap()
                        .into_iter()
                        .map(move |key| (key, row.row_id))
                })
                .collect::<Vec<_>>();
            let mut leaf = table.as_ref().clone();
            leaf.id = sid;
            leaf.partitioning = None;
            let mut physical = index.clone();
            physical.unique = true;
            let mut actual = db
                .range_index_bounds_entries(&leaf, &physical, None, None)
                .unwrap();
            expected.sort();
            actual.sort();
            assert_eq!(actual, expected, "index {} in leaf {sid}", index.name);
        }
    }
    db.rollback_txn(txn.txn_id).unwrap();
}

fn writable_build(spatial: bool, action: &str) {
    let (_dir, engine) = fixture(spatial);
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let sql = format!("ALTER TABLE t {action}");
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 37101;
        worker.execute_with_session(&mut session, &sql)
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 37102;
    writer.sysvars.lock_wait_timeout = Some(0);
    let read = engine
        .execute_with_session(&mut writer, "SELECT * FROM t")
        .and_then(QueryResponse::materialize);
    let write = engine.execute_with_session(&mut writer, "UPDATE t SET id=4,v=40 WHERE id=1");
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{action}: {result:?}");
    read.expect("read must run during build");
    write.expect("write must run during build");
    result.unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT id,v FROM t FORCE INDEX(vi) WHERE v>=20 ORDER BY id"
        ),
        vec![
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(3), Value::Int(30)],
            vec![Value::Int(4), Value::Int(40)]
        ]
    );
    if spatial {
        assert_indexes(&engine);
    }
}

#[test]
fn remaining_spatial_partition_build_is_writable() {
    writable_build(true, "REBUILD PARTITION ALL");
}

#[test]
fn remaining_repair_partition_build_is_writable() {
    writable_build(false, "REPAIR PARTITION ALL");
}

#[test]
fn remaining_analyze_partition_scan_is_writable() {
    let (_dir, engine) = fixture(false);
    let hook = engine.database().install_analyze_batch_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 37201;
        worker.execute_with_session(&mut session, "ALTER TABLE t ANALYZE PARTITION ALL")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 37202;
    writer.sysvars.lock_wait_timeout = Some(0);
    let write = engine.execute_with_session(&mut writer, "UPDATE t SET v=40 WHERE id=1");
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    write.expect("ANALYZE PARTITION must admit writes during scan");
    result.unwrap();
    assert_eq!(
        rows(&engine, "SELECT v FROM t WHERE id=1"),
        vec![vec![Value::Int(40)]]
    );
}

#[test]
fn remaining_spatial_partition_operation_matrix() {
    for action in [
        "REBUILD PARTITION ALL, LOCK=NONE",
        "OPTIMIZE PARTITION ALL, LOCK=NONE",
        "REPAIR PARTITION ALL, LOCK=NONE",
        "ADD PARTITION PARTITIONS 1, LOCK=NONE",
        "COALESCE PARTITION 1, LOCK=NONE",
        "PARTITION BY KEY(id) PARTITIONS 3, LOCK=NONE",
        "REMOVE PARTITIONING, LOCK=NONE",
    ] {
        writable_build(true, action);
    }
}

#[test]
fn remaining_partition_spatial_final_tail_and_recovery() {
    for memory in [false, true] {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let open = || {
            if memory {
                open_complete_memory_test_engine(dir.path())
            } else {
                open_strict_test_engine(dir.path())
            }
        };
        let engine = Arc::new(open());
        engine.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,g GEOMETRY NOT NULL,INDEX vi(v),SPATIAL sp(g)) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
        engine
            .execute(
                "INSERT INTO t VALUES(1,10,POINT(1,1)),(8,80,POINT(8,8)),(12,120,POINT(12,12))",
            )
            .unwrap();
        let retained = engine
            .database()
            .table("test.t")
            .unwrap()
            .partitioning
            .as_ref()
            .unwrap()
            .storage_ids_for_name("p1");
        let build = engine.database().install_ddl_backfill_test_hook();
        let publish = engine.database().install_online_copy_publish_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 37301;
            worker.execute_with_session(&mut session,"ALTER TABLE t REORGANIZE PARTITION p0 INTO(PARTITION q0 VALUES LESS THAN(5),PARTITION q1 VALUES LESS THAN(10)), LOCK=NONE")
        });
        let reached = build.wait_until_reached(Duration::from_secs(10));
        let mut writer = SessionContext::default();
        writer.connection_id = 37302;
        writer.sysvars.lock_wait_timeout = Some(0);
        let writes = [
            "BEGIN",
            "UPDATE t SET id=11,v=110,g=POINT(11,11) WHERE id=1",
            "UPDATE t SET id=2,v=20,g=POINT(2,2) WHERE id=12",
            "SAVEPOINT s",
            "INSERT INTO t VALUES(4,40,POINT(4,4))",
            "ROLLBACK TO s",
            "COMMIT",
            "BEGIN",
            "UPDATE t SET g=POINT(99,99)",
            "ROLLBACK",
        ]
        .map(|sql| engine.execute_with_session(&mut writer, sql));
        build.release();
        let tail = publish.wait_until_reached(Duration::from_secs(10));
        let final_writes = [
            "DELETE FROM t WHERE id=8",
            "INSERT INTO t VALUES(3,30,POINT(3,3))",
            "UPDATE t SET g=POINT(22,22) WHERE id=2",
        ]
        .map(|sql| engine.execute_with_session(&mut writer, sql));
        publish.release();
        let result = ddl.join().unwrap();
        assert!(reached && tail, "memory={memory}: {result:?}");
        for write in writes.into_iter().chain(final_writes) {
            write.unwrap();
        }
        result.unwrap();
        assert_eq!(
            rows(&engine, "SELECT id,ST_X(g) FROM t ORDER BY id"),
            vec![
                vec![Value::Int(2), Value::Double(22.0)],
                vec![Value::Int(3), Value::Double(3.0)],
                vec![Value::Int(11), Value::Double(11.0)]
            ]
        );
        assert_eq!(
            retained,
            engine
                .database()
                .table("test.t")
                .unwrap()
                .partitioning
                .as_ref()
                .unwrap()
                .storage_ids_for_name("p1")
        );
        assert_indexes(&engine);
        // Memory mode has no durable reopen contract. Check continued writes
        // in-process; Strict mode below additionally validates WAL recovery.
        if memory {
            engine
                .execute("UPDATE t SET g=POINT(23,23) WHERE id=2")
                .unwrap();
            assert_indexes(&engine);
            continue;
        }
        drop(engine);
        let engine = open();
        assert_eq!(
            rows(
                &engine,
                "SELECT id,v FROM t FORCE INDEX(vi) WHERE v>=20 ORDER BY id"
            ),
            vec![
                vec![Value::Int(2), Value::Int(20)],
                vec![Value::Int(3), Value::Int(30)],
                vec![Value::Int(11), Value::Int(110)]
            ]
        );
        assert_indexes(&engine);
        engine
            .execute("UPDATE t SET g=POINT(23,23) WHERE id=2")
            .unwrap();
        assert_indexes(&engine);
    }
}

#[test]
fn remaining_partition_build_lock_contracts() {
    for (spatial, action, analyze) in [
        (true, "REBUILD PARTITION ALL", false),
        (false, "REPAIR PARTITION ALL", false),
        (false, "ANALYZE PARTITION ALL", true),
        (true, "TRUNCATE PARTITION p0", false),
    ] {
        for mode in ["SHARED", "EXCLUSIVE"] {
            let (_dir, engine) = fixture(spatial);
            let hook = if analyze {
                engine.database().install_analyze_batch_test_hook()
            } else {
                engine.database().install_ddl_backfill_test_hook()
            };
            let worker = engine.clone();
            let sql = format!("ALTER TABLE t {action}, LOCK={mode}");
            let ddl = std::thread::spawn(move || {
                let mut session = SessionContext::default();
                session.connection_id = 37401;
                worker.execute_with_session(&mut session, &sql)
            });
            let reached = hook.wait_until_reached(Duration::from_secs(10));
            let mut session = SessionContext::default();
            session.connection_id = 37402;
            session.sysvars.lock_wait_timeout = Some(0);
            let read = engine
                .execute_with_session(&mut session, "SELECT id FROM t")
                .and_then(QueryResponse::materialize);
            let write = engine.execute_with_session(&mut session, "UPDATE t SET v=v+1 WHERE id=1");
            let competing =
                engine.execute_with_session(&mut session, "ALTER TABLE t ADD COLUMN x INT");
            hook.release();
            let result = ddl.join().unwrap();
            assert!(reached, "{action}/{mode}: {result:?}");
            result.unwrap();
            if mode == "SHARED" {
                read.unwrap();
            } else {
                assert_eq!(read.unwrap_err().code(), 1205);
            }
            assert_eq!(write.unwrap_err().code(), 1205);
            assert_eq!(competing.unwrap_err().code(), 1205);
            engine.execute("UPDATE t SET v=v+1 WHERE id=1").unwrap();
        }
    }
}

#[test]
fn remaining_partition_cancellation_and_publish_timeout_keep_source_writes() {
    for action in ["REPAIR PARTITION ALL", "TRUNCATE PARTITION p1"] {
        for cancel in [false, true] {
            let (_dir, engine) = fixture(true);
            let source = engine.database().table("test.t").unwrap();
            let hook = if cancel {
                engine.database().install_ddl_backfill_test_hook()
            } else {
                engine.database().install_online_copy_publish_test_hook()
            };
            let worker = engine.clone();
            let ddl = std::thread::spawn(move || {
                let mut session = SessionContext::default();
                session.connection_id = 37501;
                session.sysvars.lock_wait_timeout = Some(0);
                worker.execute_with_session(
                    &mut session,
                    &format!("ALTER TABLE t {action}, LOCK=NONE"),
                )
            });
            let reached = hook.wait_until_reached(Duration::from_secs(10));
            let write = engine.execute("UPDATE t SET v=99,g=POINT(9,9) WHERE id=1");
            let mut reader = SessionContext::default();
            reader.connection_id = 37502;
            engine.execute_with_session(&mut reader, "BEGIN").unwrap();
            engine
                .execute_with_session(&mut reader, "SELECT * FROM t")
                .unwrap()
                .materialize()
                .unwrap();
            if cancel {
                let jobs = engine.database().alter_copy_job_snapshots();
                engine
                    .database()
                    .cancel_ddl_job(
                        jobs.iter()
                            .find(|j| j.table_id == source.id)
                            .unwrap()
                            .job_id,
                    )
                    .unwrap();
            }
            hook.release();
            let result = ddl.join().unwrap();
            engine
                .execute_with_session(&mut reader, "ROLLBACK")
                .unwrap();
            assert!(reached, "{result:?}");
            write.unwrap();
            assert_eq!(result.unwrap_err().code(), if cancel { 1317 } else { 1205 });
            assert_eq!(
                engine.database().table("test.t").unwrap().storage_ids(),
                source.storage_ids()
            );
            assert_eq!(
                rows(&engine, "SELECT v FROM t WHERE id=1"),
                vec![vec![Value::Int(99)]]
            );
            assert_indexes(&engine);
            engine
                .execute(&format!("ALTER TABLE t {action}, LOCK=NONE"))
                .unwrap();
            assert_indexes(&engine);
        }
    }
}

fn reported_rows(engine: &StorageQueryEngine) -> i64 {
    rows(engine,"SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA='test' AND TABLE_NAME='t'")[0][0].to_display_string().parse().unwrap()
}

#[test]
fn remaining_partial_analyze_preserves_opposing_leaf_deltas() {
    let (_dir, engine) = fixture(false);
    engine
        .execute("ALTER TABLE t ANALYZE PARTITION ALL")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(4,40)").unwrap();
    engine.execute("DELETE FROM t WHERE id=3").unwrap();
    engine
        .execute("ALTER TABLE t ANALYZE PARTITION p0, LOCK=NONE")
        .unwrap();
    assert_eq!(reported_rows(&engine), 3);
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn remaining_busy_analyze_keeps_unfolded_population() {
    let (_dir, engine) = fixture(false);
    let mut writer = SessionContext::default();
    writer.connection_id = 37602;
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES(4,40)")
        .unwrap();
    let hook = engine.database().install_analyze_batch_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 37601;
        session.sysvars.lock_wait_timeout = Some(0);
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t ANALYZE PARTITION ALL, LOCK=NONE",
        )
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let commit = engine.execute_with_session(&mut writer, "COMMIT");
    let insert = engine.execute("INSERT INTO t VALUES(5,50)");
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    commit.unwrap();
    insert.unwrap();
    result.unwrap();
    assert_eq!(reported_rows(&engine), 5);
    assert_eq!(engine.database().active_snapshot_count(), 0);
}

#[test]
fn remaining_analyze_snapshot_survives_zero_retention_purge() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let mut config = memory_test_storage_config();
    config.undo_retention = Duration::ZERO;
    let engine = Arc::new(
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap(),
    );
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,INDEX vi(v)) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10),(2,20),(3,30)")
        .unwrap();
    let hook = engine.database().install_analyze_batch_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 37701;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t ANALYZE PARTITION ALL, LOCK=NONE",
        )
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let pinned = engine.database().active_snapshot_count();
    let writes = (0..25)
        .map(|_| {
            let result = engine.execute("UPDATE t SET v=v+1 WHERE id=1");
            engine.database().purge_obsolete_undo();
            result
        })
        .collect::<Vec<_>>();
    let delete = engine.execute("DELETE FROM t WHERE id=3");
    engine.database().purge_obsolete_undo();
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    assert!(pinned > 0);
    for write in writes {
        write.unwrap();
    }
    delete.unwrap();
    result.unwrap();
    let table = engine.database().table("test.t").unwrap();
    assert_eq!(
        table
            .stats
            .partition_stats
            .values()
            .map(|s| s.row_count)
            .sum::<u64>(),
        3
    );
    assert_eq!(reported_rows(&engine), 2);
    assert_eq!(engine.database().active_snapshot_count(), 0);
}

#[test]
#[ignore = "local DDL lock scaling comparison"]
fn remaining_partition_lock_scaling_probe() {
    for count in [1000, 10000, 100000] {
        for (kind, action, spatial) in [
            ("spatial_rebuild", "REBUILD PARTITION ALL", true),
            ("repair", "REPAIR PARTITION ALL", false),
            (
                "spatial_repartition",
                "PARTITION BY KEY(id) PARTITIONS 3",
                true,
            ),
            ("analyze", "ANALYZE PARTITION ALL", false),
            ("truncate", "TRUNCATE PARTITION p0", false),
        ] {
            for mode in ["SHARED", "NONE"] {
                let (dir, engine) = fixture(spatial);
                engine.execute("DELETE FROM t").unwrap();
                let point = if spatial {
                    Some(rows(&engine, "SELECT POINT(1,1)")[0][0].clone())
                } else {
                    None
                };
                insert_fixture_rows(
                    &engine,
                    "test.t",
                    (0..count).map(|i| {
                        let mut row = vec![Value::Int(i), Value::Int(i)];
                        if let Some(point) = &point {
                            row.push(point.clone());
                        }
                        row
                    }),
                );
                engine.checkpoint().unwrap();
                let before = engine.database().ddl_copy_perf_snapshot();
                let started = Instant::now();
                engine
                    .execute(&format!("ALTER TABLE t {action}, LOCK={mode}"))
                    .unwrap();
                let elapsed = started.elapsed().as_micros();
                let after = engine.database().ddl_copy_perf_snapshot();
                eprintln!("REMAINING_DDL kind={kind} rows={count} mode={mode} elapsed_us={elapsed} barrier_us={}",after[22]-before[22]);
                assert_eq!(
                    rows(&engine, "SELECT COUNT(*) FROM t"),
                    vec![vec![Value::Int(if kind == "truncate" {
                        count / 2
                    } else {
                        count
                    })]]
                );
                drop(engine);
                drop(dir);
            }
        }
    }
}

#[test]
fn remaining_partition_crash_child() {
    let Some(dir) = std::env::var_os("MYDB_REMAINING_PARTITION_CRASH_DIR") else {
        return;
    };
    let engine = open_strict_test_engine(std::path::Path::new(&dir));
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,g GEOMETRY NOT NULL,INDEX vi(v),SPATIAL sp(g)) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10,POINT(1,1)),(2,20,POINT(2,2))")
        .unwrap();
    let action = std::env::var("MYDB_REMAINING_PARTITION_ACTION")
        .unwrap_or_else(|_| "REPAIR PARTITION ALL".into());
    engine
        .execute(&format!("ALTER TABLE t {action}, LOCK=NONE"))
        .unwrap();
    panic!("crash point did not exit");
}

#[test]
fn remaining_spatial_partition_crash_recovery() {
    for action in ["REPAIR PARTITION ALL", "TRUNCATE PARTITION ALL"] {
        for phase in [3, 4, 5, 6] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let log = std::fs::File::create(dir.path().join("child.log")).unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "engine::executor::tests::ddl_lock_remaining::remaining_partition_crash_child",
                    "--nocapture",
                ])
                .env("MYDB_REMAINING_PARTITION_CRASH_DIR", dir.path())
                .env("MYDB_REMAINING_PARTITION_ACTION", action)
                .env("MYDB_COPY_PUBLISH_CRASH", phase.to_string())
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .status()
                .unwrap();
            assert_eq!(
                status.code(),
                Some(86),
                "phase {phase}: {}",
                std::fs::read_to_string(dir.path().join("child.log")).unwrap()
            );
            let engine = open_strict_test_engine(dir.path());
            assert_eq!(
                rows(&engine, "SELECT id,v,ST_X(g) FROM t ORDER BY id"),
                if action.starts_with("TRUNCATE") && phase >= 4 {
                    vec![]
                } else {
                    vec![
                        vec![Value::Int(1), Value::Int(10), Value::Double(1.0)],
                        vec![Value::Int(2), Value::Int(20), Value::Double(2.0)],
                    ]
                }
            );
            assert_indexes(&engine);
            engine
                .execute("UPDATE t SET g=POINT(3,3),v=30 WHERE id=1")
                .unwrap();
            engine
                .execute("INSERT INTO t VALUES(3,30,POINT(3,3))")
                .unwrap();
            assert_indexes(&engine);
        }
    }
}

#[test]
fn remaining_fulltext_partition_none_rewrites_and_preserves_corpora() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,FULLTEXT ft(body)) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'quartz'),(2,'amber')")
        .unwrap();
    for action in [
        "REBUILD PARTITION ALL",
        "REPAIR PARTITION ALL",
        "COALESCE PARTITION 1",
        "TRUNCATE PARTITION ALL",
    ] {
        let before = engine.database().table("test.t").unwrap().storage_ids();
        engine
            .execute(&format!("ALTER TABLE t {action},LOCK=NONE"))
            .unwrap();
        assert_ne!(
            engine.database().table("test.t").unwrap().storage_ids(),
            before
        );
        assert_eq!(
            rows(&engine, "SELECT COUNT(*) FROM t"),
            vec![vec![Value::Int(if action.starts_with("TRUNCATE") {
                0
            } else {
                2
            })]]
        );
        let txn = engine.database().begin_read_only_txn().unwrap();
        super::fulltext_incremental::assert_corpora(&engine, "test.t", txn);
        engine.database().rollback_txn(txn.txn_id).unwrap();
    }
}

#[test]
fn remaining_truncate_partition_allows_writes_and_uses_publish_boundary() {
    for (spatial, memory) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(if memory {
            open_complete_memory_test_engine(dir.path())
        } else {
            open_strict_test_engine(dir.path())
        });
        let extra = if spatial {
            ",g GEOMETRY NOT NULL,SPATIAL sp(g)"
        } else {
            ""
        };
        engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,v INT,INDEX vi(v){extra}) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)")).unwrap();
        let extra = if spatial { ",POINT(1,1)" } else { "" };
        engine
            .execute(&format!(
                "INSERT INTO t VALUES(1,10{extra}),(2,20{extra}),(11,110{extra}),(12,120{extra})"
            ))
            .unwrap();
        let build = engine.database().install_ddl_backfill_test_hook();
        let publish = engine.database().install_online_copy_publish_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut s = SessionContext::default();
            s.connection_id = 37801;
            worker.execute_with_session(&mut s, "ALTER TABLE t TRUNCATE PARTITION p0")
        });
        let reached = build.wait_until_reached(Duration::from_secs(10));
        let mut s = SessionContext::default();
        s.connection_id = 37802;
        s.sysvars.lock_wait_timeout = Some(0);
        let writes = [
            "BEGIN".to_string(),
            format!("INSERT INTO t VALUES(3,30{extra}),(13,130{extra})"),
            "UPDATE t SET id=14,v=140 WHERE id=1".into(),
            "UPDATE t SET id=4,v=40 WHERE id=11".into(),
            "DELETE FROM t WHERE id=12".into(),
            "SAVEPOINT s".into(),
            format!("INSERT INTO t VALUES(21,210{extra})"),
            "ROLLBACK TO s".into(),
            "COMMIT".into(),
        ]
        .map(|sql| engine.execute_with_session(&mut s, &sql));
        build.release();
        let tail = publish.wait_until_reached(Duration::from_secs(10));
        let final_writes = [
            format!("INSERT INTO t VALUES(6,60{extra}),(15,150{extra})"),
            "DELETE FROM t WHERE id=13".into(),
        ]
        .map(|sql| engine.execute_with_session(&mut s, &sql));
        publish.release();
        let result = ddl.join().unwrap();
        assert!(reached && tail, "spatial={spatial}: {result:?}");
        for write in writes.into_iter().chain(final_writes) {
            write.expect("TRUNCATE must admit writes before publication");
        }
        result.unwrap();
        assert_eq!(
            rows(&engine, "SELECT id,v FROM t ORDER BY id"),
            vec![
                vec![Value::Int(14), Value::Int(140)],
                vec![Value::Int(15), Value::Int(150)]
            ]
        );
        assert_eq!(reported_rows(&engine), 2);
        assert_indexes(&engine);
        engine
            .execute(&format!("INSERT INTO t VALUES(5,50{extra})"))
            .unwrap();
        if memory {
            engine
                .execute("ALTER TABLE t TRUNCATE PARTITION ALL,LOCK=NONE")
                .unwrap();
            assert!(rows(&engine, "SELECT * FROM t").is_empty());
            continue;
        }
        drop(engine);
        let engine = open_strict_test_engine(dir.path());
        assert_eq!(
            rows(&engine, "SELECT id FROM t ORDER BY id"),
            vec![
                vec![Value::Int(5)],
                vec![Value::Int(14)],
                vec![Value::Int(15)]
            ]
        );
        assert_indexes(&engine);
        engine
            .execute("ALTER TABLE t TRUNCATE PARTITION ALL,LOCK=NONE")
            .unwrap();
        assert!(rows(&engine, "SELECT * FROM t").is_empty());
    }
}

#[test]
fn remaining_repair_reconstructs_missing_source_index_entries_while_writable() {
    let (_dir, engine) = fixture(true);
    let db = engine.database();
    let table = db.table("test.t").unwrap();
    let read = db.begin_read_only_txn().unwrap();
    let (sid, row) = table
        .storage_ids()
        .into_iter()
        .find_map(|sid| {
            db.scan_rows_storage_range(&table, sid, read, 0, u64::MAX)
                .unwrap()
                .into_iter()
                .next()
                .map(|row| (sid, row))
        })
        .unwrap();
    db.rollback_txn(read.txn_id).unwrap();
    let mut broken = table.as_ref().clone();
    broken.indexes.retain(|i| i.name == "vi");
    let txn = db.begin_txn().unwrap();
    db.delete_indexes_deferred_at(txn.txn_id, sid, &broken, &row.values, row.row_id)
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    assert_eq!(
        engine
            .execute("ALTER TABLE t CHECK PARTITION ALL,LOCK=NONE")
            .unwrap_err()
            .code(),
        1030
    );
    assert_eq!(
        db.physical_index_entries_for_test(&table, &broken.indexes[0])
            .unwrap()
            .len(),
        2
    );
    let hook = db.install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl =
        std::thread::spawn(move || worker.execute("ALTER TABLE t REPAIR PARTITION ALL,LOCK=NONE"));
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 37902;
    writer.sysvars.lock_wait_timeout = Some(0);
    let write = engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(4,40,POINT(4,4))");
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    write.unwrap();
    result.unwrap();
    assert_indexes(&engine);
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t FORCE INDEX(vi) WHERE v>=0"),
        vec![vec![Value::Int(4)]]
    );
}
