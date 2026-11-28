//! Physical ALTER on ordinary tables with FULLTEXT and spatial indexes.
use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("rows: {sql}")
    };
    result.rows
}

fn create_mixed(engine: &StorageQueryEngine) {
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,n INT,body TEXT,g GEOMETRY NOT NULL,INDEX ni(n),FULLTEXT ft(body),SPATIAL sp(g))").unwrap();
    engine.execute("INSERT INTO t VALUES(1,10,'quartz amber',ST_GeomFromText('POINT(1 1)')),(2,20,'silver quartz',ST_GeomFromText('POINT(2 2)'))").unwrap();
}

fn assert_derivatives(engine: &StorageQueryEngine) {
    let db = engine.database();
    let table = db.table("test.t").unwrap();
    let txn = db.begin_read_only_txn().unwrap();
    super::fulltext_incremental::assert_corpora(engine, "test.t", txn);
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
            assert_eq!(actual, expected, "index {}", index.name);
        }
    }
    db.rollback_txn(txn.txn_id).unwrap();
}

#[test]
fn special_alter_default_admits_readers_and_writers_during_copy() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    create_mixed(&engine);
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 30101;
        worker.execute_with_session(&mut session, "ALTER TABLE t MODIFY n BIGINT")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 30102;
    writer.sysvars.lock_wait_timeout = Some(0);
    let read = engine
        .execute_with_session(&mut writer, "SELECT id,n FROM t")
        .and_then(QueryResponse::materialize);
    let write = engine.execute_with_session(
        &mut writer,
        "UPDATE t SET n=11,body='final quartz',g=ST_GeomFromText('POINT(3 3)') WHERE id=1",
    );
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    result.unwrap();
    read.expect("COPY must admit readers");
    write.expect("COPY must admit writers");
    assert_eq!(
        rows(&engine, "SELECT n,ST_X(g) FROM t WHERE id=1"),
        vec![vec![Value::Int(11), Value::Double(3.0)]]
    );
    assert_derivatives(&engine);
}

#[test]
#[ignore = "local strict-durability lock and DML performance comparison"]
fn special_alter_lock_scaling_probe() {
    for count in [1000, 10000] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE control(id INT PRIMARY KEY,v INT,INDEX vi(v))")
            .unwrap();
        engine
            .execute("INSERT INTO control VALUES(1,0),(2,0),(3,0),(4,0)")
            .unwrap();
        let mut session = SessionContext::default();
        session.connection_id = 30201;
        for round in 0..5 {
            engine.execute_with_session(&mut session, "BEGIN").unwrap();
            let started = Instant::now();
            for i in 0..1000 {
                engine
                    .execute_with_session(
                        &mut session,
                        &format!("UPDATE control SET v=v+1 WHERE id={}", i % 4 + 1),
                    )
                    .unwrap();
            }
            engine.execute_with_session(&mut session, "COMMIT").unwrap();
            eprintln!(
                "SPECIAL_ALTER_DML rows={count} round={round} elapsed_us={}",
                started.elapsed().as_micros()
            );
        }
        engine.execute("CREATE TABLE t(id INT PRIMARY KEY,n INT,body TEXT,g GEOMETRY NOT NULL,FULLTEXT ft(body),SPATIAL sp(g))").unwrap();
        engine.execute("CREATE TABLE locked LIKE t").unwrap();
        let point = rows(&engine, "SELECT ST_GeomFromText('POINT(1 1)')")[0][0].clone();
        for table in ["test.t", "test.locked"] {
            insert_fixture_rows(
                &engine,
                table,
                (0..count).map(|i| {
                    vec![
                        Value::Int(i),
                        Value::Int(i),
                        Value::String(format!("quartz amber document{i}")),
                        point.clone(),
                    ]
                }),
            );
        }
        engine.checkpoint().unwrap();
        for (mode, table, sql) in [
            ("default", "t", "ALTER TABLE t MODIFY n BIGINT"),
            (
                "exclusive",
                "locked",
                "ALTER TABLE locked MODIFY n BIGINT, LOCK=EXCLUSIVE",
            ),
        ] {
            let before = engine.database().ddl_copy_perf_snapshot();
            let started = Instant::now();
            engine.execute(sql).unwrap();
            let after = engine.database().ddl_copy_perf_snapshot();
            eprintln!(
                "SPECIAL_ALTER_PROBE mode={mode} rows={count} elapsed_us={} hold_us={}",
                started.elapsed().as_micros(),
                after[22] - before[22]
            );
            assert_eq!(rows(&engine,&format!("SELECT COUNT(*) FROM {table} WHERE MATCH(body) AGAINST('quartz' IN BOOLEAN MODE)")),vec![vec![Value::Int(count)]]);
        }
    }
}

#[test]
fn special_alter_renames_columns_replays_statistics_savepoints_and_final_tail() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,n INT,body TEXT,title TEXT,active INT,g GEOMETRY NOT NULL,FULLTEXT ft(body),FULLTEXT fp(body) WHERE active=1,FULLTEXT ng(body,title) WITH PARSER ngram,SPATIAL sp(g),UNIQUE ni(n))").unwrap();
    engine.execute("INSERT INTO t VALUES(1,10,'quartz amber','数据库',1,ST_GeomFromText('POINT(1 1)')),(2,20,'silver quartz','全文检索',0,ST_GeomFromText('POINT(2 2)')),(3,30,NULL,NULL,1,ST_GeomFromText('POINT(3 3)')),(4,40,'','',0,ST_GeomFromText('POINT(4 4)'))").unwrap();
    let baseline = engine
        .database()
        .install_fulltext_copy_statistics_test_hook();
    let publish = engine.database().install_online_copy_publish_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 30301;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t CHANGE body content LONGTEXT, MODIFY n BIGINT, LOCK=NONE",
        )
    });
    let reached = baseline.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 30302;
    writer.sysvars.lock_wait_timeout = Some(0);
    let changes = (|| {
        for sql in [
            "BEGIN",
            "UPDATE t SET body='cobalt quartz quartz',title='全文数据库',active=0,g=ST_GeomFromText('POINT(7 7)') WHERE id=1",
            "DELETE FROM t WHERE id=2",
            "SAVEPOINT s",
            "INSERT INTO t VALUES(5,50,'discarded token','回滚',1,ST_GeomFromText('POINT(5 5)'))",
            "ROLLBACK TO s",
            "INSERT INTO t VALUES(6,60,'quartz zircon','数据库数据库',1,ST_GeomFromText('POINT(6 6)'))",
            "COMMIT",
        ] {
            engine.execute_with_session(&mut writer, sql)?;
        }
        Ok::<_, QueryError>(())
    })();
    baseline.release();
    if changes.is_err() {
        publish.release();
    }
    let publishing = publish.wait_until_reached(Duration::from_secs(10));
    let tail = engine.execute_with_session(
        &mut writer,
        "UPDATE t SET body='final quartz',g=ST_GeomFromText('POINT(9 9)') WHERE id=6",
    );
    publish.release();
    let result = ddl.join().unwrap();
    assert!(reached && publishing, "{result:?}");
    changes.unwrap();
    tail.unwrap();
    result.unwrap();
    assert_derivatives(&engine);
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM t WHERE MATCH(content) AGAINST('quartz' IN BOOLEAN MODE) ORDER BY id"
        ),
        vec![vec![Value::Int(1)], vec![Value::Int(6)]]
    );
    assert_eq!(
        rows(&engine, "SELECT ST_AsText(g) FROM t WHERE id=6"),
        vec![vec![Value::String("POINT(9 9)".into())]]
    );
    let scores = rows(
        &engine,
        "SELECT id,MATCH(content) AGAINST('quartz') FROM t ORDER BY id",
    );
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_derivatives(&engine);
    assert_eq!(
        rows(
            &engine,
            "SELECT id,MATCH(content) AGAINST('quartz') FROM t ORDER BY id"
        ),
        scores
    );
}

#[test]
fn special_alter_explicit_lock_modes_match_their_contracts() {
    for lock in ["NONE", "SHARED", "EXCLUSIVE"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        create_mixed(&engine);
        let hook = engine.database().install_ddl_backfill_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 30401;
            worker.execute_with_session(
                &mut session,
                &format!("ALTER TABLE t MODIFY n BIGINT, ALGORITHM=COPY, LOCK={lock}"),
            )
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let mut session = SessionContext::default();
        session.connection_id = 30402;
        session.sysvars.lock_wait_timeout = Some(0);
        let read = engine
            .execute_with_session(&mut session, "SELECT id FROM t")
            .and_then(QueryResponse::materialize);
        let write = engine.execute_with_session(&mut session, "UPDATE t SET n=11 WHERE id=1");
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{lock}: {result:?}");
        result.unwrap();
        if lock == "EXCLUSIVE" {
            assert_eq!(read.unwrap_err().code(), 1205);
        } else {
            read.unwrap();
        }
        if lock == "NONE" {
            write.unwrap();
        } else {
            assert_eq!(write.unwrap_err().code(), 1205);
        }
        assert_derivatives(&engine);
    }
}

#[test]
fn special_alter_failed_tail_conversion_and_cancellation_preserve_source() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine.execute("CREATE TABLE t(id INT PRIMARY KEY,n INT,body TEXT,g GEOMETRY NOT NULL,FULLTEXT ft(body),SPATIAL sp(g))").unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,10,'okay',ST_GeomFromText('POINT(1 1)'))")
            .unwrap();
        let old = engine.database().table("test.t").unwrap();
        let hook = engine.database().install_online_copy_publish_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 30501;
            worker.execute_with_session(
                &mut session,
                "ALTER TABLE t MODIFY body VARCHAR(4), LOCK=NONE",
            )
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let write = engine
            .execute("INSERT INTO t VALUES(2,20,'toolong quartz',ST_GeomFromText('POINT(2 2)'))");
        if cancel {
            let job = engine
                .database()
                .alter_copy_job_snapshots()
                .into_iter()
                .find(|job| job.table_id == old.id && job.cancel_allowed)
                .unwrap();
            engine.database().cancel_ddl_job(job.job_id).unwrap();
        }
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{result:?}");
        write.unwrap();
        assert_eq!(result.unwrap_err().code(), if cancel { 1317 } else { 1406 });
        let current = engine.database().table("test.t").unwrap();
        assert_eq!(current.active_generation, old.active_generation);
        assert_eq!(current.schema_version, old.schema_version);
        assert_derivatives(&engine);
        drop(engine);
        let engine = open_strict_test_engine(dir.path());
        assert_derivatives(&engine);
        assert_eq!(
            rows(&engine, "SELECT id,body FROM t WHERE id=2"),
            vec![vec![Value::Int(2), Value::String("toolong quartz".into())]]
        );
    }
}

#[test]
fn special_alter_charset_conversion_rejects_collation_duplicates_atomically() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,body VARCHAR(40) COLLATE utf8mb4_bin,FULLTEXT ft(body),UNIQUE bi(body))").unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'quartz'),(2,'QUARTZ')")
        .unwrap();
    let old = engine.database().table("test.t").unwrap();
    assert_eq!(engine.execute("ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, LOCK=NONE").unwrap_err().code(),1062);
    assert_eq!(
        engine.database().table("test.t").unwrap().schema_version,
        old.schema_version
    );
    assert_derivatives(&engine);
}

#[test]
fn special_alter_crash_child() {
    let Ok(phase) = std::env::var("MYDB_SPECIAL_ALTER_CRASH_PHASE") else {
        return;
    };
    let directory = std::env::var("MYDB_SPECIAL_ALTER_CRASH_DIR").unwrap();
    let engine = Arc::new(open_strict_test_engine(std::path::Path::new(&directory)));
    create_mixed(&engine);
    if phase == "cleanup" {
        let point = rows(&engine, "SELECT ST_GeomFromText('POINT(8 8)')")[0][0].clone();
        insert_fixture_rows(
            &engine,
            "test.t",
            (100..900).map(|id| {
                vec![
                    Value::Int(id),
                    Value::Int(id),
                    Value::String(format!("quartz retired{id}")),
                    point.clone(),
                ]
            }),
        );
    }
    engine.checkpoint().unwrap();
    let hook = if phase == "baseline" {
        engine
            .database()
            .install_fulltext_copy_statistics_test_hook()
    } else if phase == "before" {
        engine.database().install_online_copy_publish_test_hook()
    } else {
        engine.database().install_vector_copy_commit_test_hook()
    };
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 30601;
        worker
            .execute_with_session(&mut session, "ALTER TABLE t MODIFY n BIGINT, LOCK=NONE")
            .unwrap();
    });
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    if phase != "after" && phase != "cleanup" {
        engine.execute("UPDATE t SET n=11,body='committed quartz',g=ST_GeomFromText('POINT(3 3)') WHERE id=1").unwrap();
    }
    if phase == "cleanup" {
        hook.release();
        ddl.join().unwrap();
        engine
            .database()
            .maintain_catalog_retirements_until(|| false)
            .unwrap();
        assert_eq!(engine.database().catalog_retirement_count_for_test(), 1);
    }
    std::process::exit(86);
}

#[test]
fn special_alter_recovers_before_and_after_publication() {
    for phase in ["baseline", "before", "after", "cleanup"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::executor::tests::online_special_alter::special_alter_crash_child",
                "--nocapture",
            ])
            .env("MYDB_SPECIAL_ALTER_CRASH_PHASE", phase)
            .env("MYDB_SPECIAL_ALTER_CRASH_DIR", dir.path())
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{phase}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let engine = open_strict_test_engine(dir.path());
        assert_derivatives(&engine);
        assert_eq!(
            engine.database().table("test.t").unwrap().active_generation,
            if phase != "after" && phase != "cleanup" {
                1
            } else {
                2
            }
        );
        assert_eq!(
            rows(&engine, "SELECT n,ST_AsText(g) FROM t WHERE id=1"),
            vec![vec![
                Value::Int(if phase != "after" && phase != "cleanup" {
                    11
                } else {
                    10
                }),
                Value::String(
                    if phase != "after" && phase != "cleanup" {
                        "POINT(3 3)"
                    } else {
                        "POINT(1 1)"
                    }
                    .into()
                )
            ]]
        );
        engine
            .execute(
                "UPDATE t SET body='surviving quartz',g=ST_GeomFromText('POINT(8 8)') WHERE id=2",
            )
            .unwrap();
        assert_derivatives(&engine);
        engine
            .execute("ALTER TABLE t MODIFY n BIGINT, LOCK=NONE")
            .unwrap();
        assert_derivatives(&engine);
    }
}

#[test]
fn special_alter_partitioned_spatial_and_fulltext_replay_moves() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,n INT,g GEOMETRY NOT NULL,SPATIAL sp(g)) PARTITION BY HASH(id) PARTITIONS 4").unwrap();
    engine.execute("INSERT INTO t VALUES(1,10,ST_GeomFromText('POINT(1 1)')),(2,20,ST_GeomFromText('POINT(2 2)'))").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 30701;
        worker.execute_with_session(&mut session, "ALTER TABLE t MODIFY n BIGINT, LOCK=NONE")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let write = engine.execute("UPDATE t SET id=7,g=ST_GeomFromText('POINT(7 7)') WHERE id=2");
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    write.unwrap();
    result.unwrap();
    assert_derivatives(&engine);
    assert_eq!(
        rows(&engine, "SELECT id,ST_AsText(g) FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::String("POINT(1 1)".into())],
            vec![Value::Int(7), Value::String("POINT(7 7)".into())]
        ]
    );
    engine.execute("CREATE TABLE p(id INT PRIMARY KEY,n INT,body TEXT,FULLTEXT ft(body)) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
    let old = engine.database().table("test.p").unwrap();
    engine
        .execute("INSERT INTO p VALUES(1,10,'quartz')")
        .unwrap();
    engine
        .execute("ALTER TABLE p MODIFY n BIGINT, LOCK=NONE")
        .unwrap();
    assert!(engine.database().table("test.p").unwrap().schema_version > old.schema_version);
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM p WHERE MATCH(body) AGAINST('quartz' IN BOOLEAN MODE)"
        ),
        vec![vec![Value::Int(1)]]
    );
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_derivatives(&engine);
}

#[test]
fn special_alter_charset_conversion_replays_data_and_fences_old_generation_views() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,n INT,body TEXT,FULLTEXT ft(body),INDEX bi(body(20) DESC)) DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci").unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10,'quartz café'),(2,20,'amber')")
        .unwrap();
    let mut reader = SessionContext::default();
    reader.connection_id = 30804;
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 30801;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci, LOCK=NONE",
        )
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let write = engine.execute("UPDATE t SET body='quartz façade' WHERE id=2");
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    write.unwrap();
    result.unwrap();
    let old = engine
        .execute_with_session(&mut reader, "SELECT id FROM t")
        .and_then(QueryResponse::materialize);
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    assert_eq!(old.unwrap_err().code(), 1412);
    assert_eq!(
        rows(
            &engine,
            "SELECT CHARSET(body),COLLATION(body) FROM t WHERE id=1"
        ),
        vec![vec![
            Value::String("utf8mb4".into()),
            Value::String("utf8mb4_unicode_ci".into())
        ]]
    );
    engine
        .execute("INSERT INTO t VALUES(3,30,'quartz 数据库')")
        .unwrap();
    assert_derivatives(&engine);
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM t WHERE MATCH(body) AGAINST('quartz' IN BOOLEAN MODE) ORDER BY id"
        ),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_derivatives(&engine);
}

#[test]
fn special_alter_chases_large_posting_and_spatial_write_bursts() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    create_mixed(&engine);
    let hook = engine.database().install_online_copy_publish_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 31001;
        worker.execute_with_session(&mut session, "ALTER TABLE t MODIFY n BIGINT, LOCK=NONE")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let point = rows(&engine, "SELECT ST_GeomFromText('POINT(8 8)')")[0][0].clone();
    insert_fixture_rows(
        &engine,
        "test.t",
        (100..1600).map(|id| {
            vec![
                Value::Int(id),
                Value::Int(id),
                Value::String("burst quartz".into()),
                point.clone(),
            ]
        }),
    );
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    result.unwrap();
    assert_derivatives(&engine);
    assert_eq!(
        rows(
            &engine,
            "SELECT COUNT(*) FROM t WHERE MATCH(body) AGAINST('quartz' IN BOOLEAN MODE)"
        ),
        vec![vec![Value::Int(1502)]]
    );
}

#[test]
fn special_alter_retires_old_corpora_in_bounded_restartable_batches() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    create_mixed(&engine);
    let old = engine.database().table("test.t").unwrap();
    let point = rows(&engine, "SELECT ST_GeomFromText('POINT(8 8)')")[0][0].clone();
    insert_fixture_rows(
        &engine,
        "test.t",
        (100..900).map(|id| {
            vec![
                Value::Int(id),
                Value::Int(id),
                Value::String(format!("quartz retired{id}")),
                point.clone(),
            ]
        }),
    );
    engine
        .execute("ALTER TABLE t MODIFY n BIGINT, LOCK=NONE")
        .unwrap();
    assert_eq!(engine.database().catalog_retirement_count_for_test(), 1);
    let wakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let pending = wakes.clone();
    engine.database().set_catalog_retirement_waker(move || {
        pending.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });
    let hook = engine.database().install_retirement_cleanup_test_hook();
    let worker = engine.clone();
    let cleaner = std::thread::spawn(move || {
        worker
            .database()
            .maintain_catalog_retirements_until(|| false)
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 31101;
    writer.sysvars.lock_wait_timeout = Some(0);
    let write =
        engine.execute_with_session(&mut writer, "UPDATE t SET body='current quartz' WHERE id=1");
    hook.release();
    let cleaned = cleaner.join().unwrap();
    assert!(reached);
    write.unwrap();
    assert_eq!(cleaned.unwrap(), 1);
    assert_eq!(wakes.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(
        engine.database().catalog_retirement_count_for_test(),
        1,
        "one batch must not reclaim every term"
    );
    assert_derivatives(&engine);
    // A later index retirement must retain a counter-only manifest even
    // though it owns no physical files and its old generation is not live.
    engine.execute("DROP INDEX ni ON t").unwrap();
    engine.checkpoint().unwrap();
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_derivatives(&engine);
    for _ in 0..20 {
        if engine.database().catalog_retirement_count_for_test() == 0 {
            break;
        }
        engine
            .database()
            .maintain_catalog_retirements_until(|| false)
            .unwrap();
    }
    assert_eq!(engine.database().catalog_retirement_count_for_test(), 0);
    let counters = crate::storage::data_dictionary::dd_table_schema(
        crate::storage::data_dictionary::DD_FULLTEXT_COUNTERS,
    )
    .unwrap();
    let snapshot = engine.database().begin_read_only_txn().unwrap();
    assert!(engine
        .database()
        .prefix_index_entries_visible(
            &counters,
            &counters.indexes[0],
            crate::storage::IndexKey::from(vec![
                crate::storage::KeyPart::UInt(old.id),
                crate::storage::KeyPart::UInt(old.active_generation),
            ]),
            snapshot,
        )
        .unwrap()
        .is_empty());
    engine.database().rollback_txn(snapshot.txn_id).unwrap();
    assert_eq!(
        rows(&engine, "SELECT body FROM t WHERE id=1"),
        vec![vec![Value::String("current quartz".into())]]
    );
}
