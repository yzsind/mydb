//! Online column removal builds replacement indexes without rewriting the heap.
use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("rows")
    };
    result.rows
}

#[test]
fn drop_column_default_admits_dml_during_index_build() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,a INT,b INT,INDEX ab(a,b))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10,100),(2,20,200)")
        .unwrap();
    let generation = engine.database().table("test.t").unwrap().active_generation;
    engine
        .database()
        .force_cold_online_descriptor_for_test("test.t");
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 29101;
        worker.execute_with_session(&mut session, "ALTER TABLE t DROP COLUMN a")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 29102;
    writer.sysvars.lock_wait_timeout = Some(0);
    let insert = engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(3,30,300)");
    let update = engine.execute_with_session(&mut writer, "UPDATE t SET b=101 WHERE id=1");
    let read = engine
        .execute_with_session(&mut writer, "SELECT a FROM t ORDER BY id")
        .and_then(QueryResponse::materialize);
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "build was not reached: {result:?}");
    result.unwrap();
    insert.expect("writers must be admitted during the replacement build");
    update.unwrap();
    read.unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT id,b FROM t FORCE INDEX(ab) WHERE b>=0 ORDER BY id"
        ),
        vec![
            vec![Value::Int(1), Value::Int(101)],
            vec![Value::Int(2), Value::Int(200)],
            vec![Value::Int(3), Value::Int(300)]
        ]
    );
    assert_eq!(
        engine.database().table("test.t").unwrap().active_generation,
        generation
    );
}

#[test]
#[ignore = "local DDL hold-time and general DML comparison"]
fn drop_column_lock_scaling_probe() {
    for count in [1000, 10000, 100000] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,a INT,b VARCHAR(100),INDEX ab(a,b))")
            .unwrap();
        engine.execute("CREATE TABLE locked LIKE t").unwrap();
        engine
            .execute("CREATE TABLE control(id INT PRIMARY KEY,v INT,INDEX vi(v))")
            .unwrap();
        engine
            .execute("INSERT INTO control VALUES(1,0),(2,0),(3,0),(4,0)")
            .unwrap();
        let mut session = SessionContext::default();
        session.connection_id = 29201;
        for round in 0..5 {
            engine.execute_with_session(&mut session, "BEGIN").unwrap();
            let started = Instant::now();
            for step in 0..1000 {
                engine
                    .execute_with_session(
                        &mut session,
                        &format!("UPDATE control SET v=v+1 WHERE id={}", step % 4 + 1),
                    )
                    .unwrap();
            }
            engine.execute_with_session(&mut session, "COMMIT").unwrap();
            eprintln!(
                "DROP_COLUMN_DML_CONTROL rows={count} round={round} updates=1000 elapsed_us={}",
                started.elapsed().as_micros()
            );
        }
        insert_fixture_rows(
            &engine,
            "test.t",
            (0..count).map(|i| {
                vec![
                    Value::Int(i),
                    Value::Int(i),
                    Value::String(format!("value-{i:08}")),
                ]
            }),
        );
        insert_fixture_rows(
            &engine,
            "test.locked",
            (0..count).map(|i| {
                vec![
                    Value::Int(i),
                    Value::Int(i),
                    Value::String(format!("value-{i:08}")),
                ]
            }),
        );
        engine.checkpoint().unwrap();
        for (mode, table, sql) in [
            ("online", "t", "ALTER TABLE t DROP COLUMN a"),
            (
                "exclusive",
                "locked",
                "ALTER TABLE locked DROP COLUMN a, LOCK=EXCLUSIVE",
            ),
        ] {
            let before = engine.database().drop_column_hold_us_for_test();
            let started = Instant::now();
            engine.execute(sql).unwrap();
            eprintln!(
                "DROP_COLUMN_PROBE mode={mode} rows={count} elapsed_us={} hold_us={}",
                started.elapsed().as_micros(),
                engine.database().drop_column_hold_us_for_test() - before
            );
            assert_eq!(
                rows(
                    &engine,
                    &format!("SELECT COUNT(*) FROM {table} FORCE INDEX(ab) WHERE b>='value-'")
                ),
                vec![vec![Value::Int(count)]]
            );
        }
    }
}

#[test]
fn drop_column_replays_transactions_and_preserves_projected_keys() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,a INT,b INT,UNIQUE ab(a,b),INDEX ba(b,a))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300),(4,40,400)")
        .unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 29301;
        worker.execute_with_session(&mut session, "ALTER TABLE t DROP COLUMN a, LOCK=NONE")
    });
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    let mut writer = SessionContext::default();
    writer.connection_id = 29302;
    // The retained key stays unchanged, but its discarded component is updated
    // repeatedly. Old projected-key deletes must never erase the live entry.
    for sql in [
        "UPDATE t SET a=11 WHERE id=1",
        "UPDATE t SET a=12 WHERE id=1",
        "BEGIN",
        "UPDATE t SET b=201 WHERE id=2",
        "SAVEPOINT s",
        "UPDATE t SET b=999 WHERE id=2",
        "ROLLBACK TO s",
        "COMMIT",
        "BEGIN",
        "DELETE FROM t WHERE id=3",
        "INSERT INTO t VALUES(5,50,500)",
        "ROLLBACK",
        "DELETE FROM t WHERE id=4",
        "INSERT INTO t VALUES(6,60,600)",
    ] {
        engine.execute_with_session(&mut writer, sql).unwrap();
    }
    hook.release();
    ddl.join().unwrap().unwrap();
    let expected = vec![
        vec![Value::Int(1), Value::Int(100)],
        vec![Value::Int(2), Value::Int(201)],
        vec![Value::Int(3), Value::Int(300)],
        vec![Value::Int(6), Value::Int(600)],
    ];
    for index in ["ab", "ba"] {
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT id,b FROM t FORCE INDEX({index}) WHERE b>=0 ORDER BY id")
            ),
            expected
        );
    }
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES(7,100)")
            .unwrap_err()
            .code(),
        1062
    );
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        rows(
            &engine,
            "SELECT id,b FROM t FORCE INDEX(ab) WHERE b>=0 ORDER BY id"
        ),
        expected
    );
}

#[test]
fn drop_column_unique_validation_tracks_repairs_and_failure_is_atomic() {
    for (repair, late_conflict) in [(false, false), (true, false), (false, true)] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,a INT,b INT,UNIQUE ab(a,b))")
            .unwrap();
        engine
            .execute(if late_conflict {
                "INSERT INTO t VALUES(1,10,100),(2,20,200)"
            } else {
                "INSERT INTO t VALUES(1,10,100),(2,20,100)"
            })
            .unwrap();
        let original = engine.database().table("test.t").unwrap();
        let hook = engine.database().install_online_copy_publish_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 29401;
            worker.execute_with_session(&mut session, "ALTER TABLE t DROP COLUMN a")
        });
        assert!(hook.wait_until_reached(Duration::from_secs(10)));
        if repair {
            engine.execute("UPDATE t SET b=200 WHERE id=2").unwrap();
        }
        if late_conflict {
            engine.execute("UPDATE t SET b=100 WHERE id=2").unwrap();
        }
        hook.release();
        let result = ddl.join().unwrap();
        if repair {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().code(), 1062);
            let current = engine.database().table("test.t").unwrap();
            assert_eq!(current.schema_version, original.schema_version);
            assert_eq!(
                serde_json::to_value(&current.indexes).unwrap(),
                serde_json::to_value(&original.indexes).unwrap()
            );
            engine.execute("INSERT INTO t VALUES(3,30,100)").unwrap();
        }
        // A failed candidate must not poison the live table or ID reuse.
        engine.execute("CREATE INDEX survivor ON t(b)").unwrap();
        drop(engine);
        let engine = open_strict_test_engine(dir.path());
        assert_eq!(
            rows(
                &engine,
                "SELECT COUNT(*) FROM t FORCE INDEX(survivor) WHERE b>=0"
            ),
            vec![vec![Value::Int(if repair { 2 } else { 3 })]]
        );
    }
}

#[test]
fn drop_column_shared_build_allows_readers_and_excludes_writers() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,a INT,b INT,INDEX ab(a,b))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10,100)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 29501;
        worker.execute_with_session(&mut session, "ALTER TABLE t DROP COLUMN a, LOCK=SHARED")
    });
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    let mut session = SessionContext::default();
    session.connection_id = 29502;
    session.sysvars.lock_wait_timeout = Some(0);
    let read = engine.execute_with_session(&mut session, "SELECT a,b FROM t");
    let write = engine.execute_with_session(&mut session, "UPDATE t SET b=200");
    hook.release();
    ddl.join().unwrap().unwrap();
    read.unwrap();
    assert_eq!(write.unwrap_err().code(), 1205);
}

#[test]
fn drop_column_rebuilds_partitioned_primary_and_covering_typed_indexes() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(p INT,id INT,a INT,v VARCHAR(40) COLLATE utf8mb4_general_ci,d DECIMAL(12,3),day DATE,bin VARBINARY(20),PRIMARY KEY(p,id,a),INDEX av(a,v(12) DESC,d,day,bin)) PARTITION BY HASH(p) PARTITIONS 4").unwrap();
    engine.execute("INSERT INTO t VALUES(1,1,10,'Alpha',1.234,'2025-01-01',X'0102'),(2,2,20,'Beta',2.345,'2025-02-02',X'ff00'),(3,3,30,NULL,NULL,NULL,NULL)").unwrap();
    engine
        .execute("ALTER TABLE t DROP COLUMN a, ALGORITHM=INPLACE, LOCK=NONE")
        .unwrap();
    let expected = rows(&engine, "SELECT p,id,v,d,day,bin FROM t ORDER BY p,id");
    assert_eq!(
        rows(
            &engine,
            "SELECT p,id,v,d,day,bin FROM t FORCE INDEX(av) ORDER BY p,id"
        ),
        expected
    );
    engine
        .execute("UPDATE t SET p=7,v='Gamma' WHERE id=2")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT p,id FROM t FORCE INDEX(PRIMARY) ORDER BY p,id"
        ),
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(3), Value::Int(3)],
            vec![Value::Int(7), Value::Int(2)]
        ]
    );
    engine
        .execute(
            "CREATE TABLE covering(id INT PRIMARY KEY,a INT,b INT,c INT,INDEX ab(a,b) INCLUDE(c))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO covering VALUES(1,10,100,1000),(2,20,NULL,2000)")
        .unwrap();
    engine
        .execute("ALTER TABLE covering DROP COLUMN a")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT b,c FROM covering FORCE INDEX(ab) ORDER BY id"
        ),
        vec![
            vec![Value::Int(100), Value::Int(1000)],
            vec![Value::Null, Value::Int(2000)]
        ]
    );
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t FORCE INDEX(av)"),
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn drop_column_crash_child() {
    let Ok(phase) = std::env::var("MYDB_DROP_COLUMN_CRASH_PHASE") else {
        return;
    };
    let directory = std::env::var("MYDB_DROP_COLUMN_CRASH_DIR").unwrap();
    let engine = Arc::new(open_strict_test_engine(std::path::Path::new(&directory)));
    engine.execute("CREATE TABLE t(id INT AUTO_INCREMENT PRIMARY KEY,a INT,b INT,UNIQUE ab(a,b),INDEX ba(b,a))").unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10,100),(2,20,200)")
        .unwrap();
    engine.execute("ALTER TABLE t AUTO_INCREMENT=1000").unwrap();
    engine.checkpoint().unwrap();
    let hook = if phase == "before" {
        engine.database().install_online_copy_publish_test_hook()
    } else {
        engine.database().install_vector_copy_commit_test_hook()
    };
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 29601;
        worker
            .execute_with_session(&mut session, "ALTER TABLE t DROP COLUMN a")
            .unwrap();
    });
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    if phase == "before" {
        engine.execute("UPDATE t SET b=101 WHERE id=1").unwrap();
    }
    if phase == "mid_cleanup" {
        hook.release();
        ddl.join().unwrap();
        std::env::set_var("MYDB_RECYCLEBIN_TEST_CRASH", "after_file_delete");
        engine
            .database()
            .maintain_catalog_retirements_until(|| false)
            .unwrap();
        panic!("retirement crash point was not reached");
    }
    std::process::exit(86);
}

#[test]
fn drop_column_recovers_before_and_after_catalog_commit() {
    for phase in ["before", "after", "mid_cleanup"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::executor::tests::online_drop_column::drop_column_crash_child",
                "--nocapture",
            ])
            .env("MYDB_DROP_COLUMN_CRASH_PHASE", phase)
            .env("MYDB_DROP_COLUMN_CRASH_DIR", dir.path())
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{phase}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let engine = open_strict_test_engine(dir.path());
        let schema = engine.database().table("test.t").unwrap();
        assert_eq!(schema.column_index("a").is_ok(), phase == "before");
        assert_eq!(
            rows(
                &engine,
                "SELECT id,b FROM t FORCE INDEX(ba) WHERE b>=0 ORDER BY id"
            ),
            vec![
                vec![
                    Value::Int(1),
                    Value::Int(if phase == "before" { 101 } else { 100 })
                ],
                vec![Value::Int(2), Value::Int(200)]
            ]
        );
        engine
            .database()
            .maintain_catalog_retirements_until(|| false)
            .unwrap();
        engine
            .execute(if phase == "before" {
                "INSERT INTO t(a,b) VALUES(30,300)"
            } else {
                "INSERT INTO t(b) VALUES(300)"
            })
            .unwrap();
        assert_eq!(
            rows(&engine, "SELECT id FROM t WHERE b=300"),
            vec![vec![Value::Int(1000)]]
        );
        if phase == "before" {
            engine.execute("ALTER TABLE t DROP COLUMN a").unwrap();
        }
        engine
            .database()
            .maintain_catalog_retirements_until(|| false)
            .unwrap();
        engine.execute("INSERT INTO t(b) VALUES(400)").unwrap();
        assert_eq!(
            rows(&engine, "SELECT id FROM t FORCE INDEX(ab) WHERE b=400"),
            vec![vec![Value::Int(1001)]]
        );
    }
}

#[test]
fn drop_column_cancellation_and_catalog_failure_preserve_source() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,a INT,b INT,INDEX ab(a,b))")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,10,100)").unwrap();
        let original = engine.database().table("test.t").unwrap();
        let hook = engine.database().install_ddl_backfill_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 29701;
            if !cancel {
                worker.database().fail_next_catalog_commit_for_test();
            }
            worker.execute_with_session(&mut session, "ALTER TABLE t DROP COLUMN a")
        });
        assert!(hook.wait_until_reached(Duration::from_secs(10)));
        engine.execute("UPDATE t SET b=101 WHERE id=1").unwrap();
        engine.database().purge_obsolete_undo();
        assert!(engine.database().active_snapshot_count() > 0);
        if cancel {
            let job = engine
                .database()
                .alter_copy_job_snapshots()
                .into_iter()
                .find(|job| job.table_id == original.id)
                .unwrap();
            engine.database().cancel_ddl_job(job.job_id).unwrap();
        }
        hook.release();
        assert_eq!(
            ddl.join().unwrap().unwrap_err().code(),
            if cancel { 1317 } else { 1030 }
        );
        assert_eq!(engine.database().active_snapshot_count(), 0);
        assert_eq!(
            engine.database().table("test.t").unwrap().schema_version,
            original.schema_version
        );
        assert_eq!(
            rows(&engine, "SELECT a,b FROM t FORCE INDEX(ab) WHERE a=10"),
            vec![vec![Value::Int(10), Value::Int(101)]]
        );
        engine.execute("ALTER TABLE t DROP COLUMN a").unwrap();
        assert_eq!(
            rows(&engine, "SELECT b FROM t FORCE INDEX(ab) WHERE b=101"),
            vec![vec![Value::Int(101)]]
        );
    }
}

#[test]
fn drop_column_projects_functional_partial_and_included_columns() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,a INT,b VARCHAR(30),active INT,c INT,payload LONGBLOB,INDEX ab(a,(LOWER(b))) INCLUDE(c) WHERE active=1,INDEX included(b) INCLUDE(a,c))").unwrap();
    engine.execute("INSERT INTO t VALUES(1,10,'Alpha',1,100,REPEAT('x',100000)),(2,20,'Beta',0,200,REPEAT('y',100000))").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 29801;
        worker.execute_with_session(&mut session, "ALTER TABLE t DROP COLUMN a")
    });
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    engine
        .execute("UPDATE t SET active=1,b='GAMMA',c=201 WHERE id=2")
        .unwrap();
    hook.release();
    ddl.join().unwrap().unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT id,c FROM t FORCE INDEX(ab) WHERE active=1 AND LOWER(b)='gamma'"
        ),
        vec![vec![Value::Int(2), Value::Int(201)]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT id,c FROM t FORCE INDEX(included) WHERE b='GAMMA'"
        ),
        vec![vec![Value::Int(2), Value::Int(201)]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT CAST(SUM(LENGTH(payload)) AS SIGNED) FROM t"
        ),
        vec![vec![Value::Int(200000)]]
    );
}

#[test]
fn drop_column_final_barrier_drains_partition_moves_and_key_permutations() {
    for commit in [false, true] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine.execute("CREATE TABLE t(p INT,id INT,a INT,b INT,PRIMARY KEY(p,id,a),UNIQUE ab(p,a,b),INDEX ba(b,a)) PARTITION BY HASH(p) PARTITIONS 4").unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,1,10,100),(1,2,20,200),(2,3,30,300)")
            .unwrap();
        let hook = engine.database().install_online_copy_publish_test_hook();
        let worker = engine.clone();
        let (send, receive) = std::sync::mpsc::channel();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 29921;
            send.send(
                worker
                    .execute_with_session(&mut session, "ALTER TABLE t DROP COLUMN a")
                    .map(|_| ()),
            )
            .unwrap();
        });
        assert!(hook.wait_until_reached(Duration::from_secs(10)));
        let mut writer = SessionContext::default();
        writer.connection_id = 29922;
        for sql in [
            "BEGIN",
            "UPDATE t SET b=CASE id WHEN 1 THEN 200 ELSE 100 END WHERE p=1",
            "UPDATE t SET p=7 WHERE id=3",
        ] {
            engine.execute_with_session(&mut writer, sql).unwrap();
        }
        hook.release();
        let early = receive.recv_timeout(Duration::from_millis(30));
        engine
            .execute_with_session(&mut writer, if commit { "COMMIT" } else { "ROLLBACK" })
            .unwrap();
        let result = if let Ok(result) = early {
            panic!("publication passed an active source writer: {result:?}")
        } else {
            receive.recv_timeout(Duration::from_secs(10)).unwrap()
        };
        ddl.join().unwrap();
        result.unwrap();
        let expected = vec![
            vec![
                Value::Int(1),
                Value::Int(1),
                Value::Int(if commit { 200 } else { 100 }),
            ],
            vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(if commit { 100 } else { 200 }),
            ],
            vec![
                Value::Int(if commit { 7 } else { 2 }),
                Value::Int(3),
                Value::Int(300),
            ],
        ];
        for index in ["PRIMARY", "ab", "ba"] {
            assert_eq!(
                rows(
                    &engine,
                    &format!("SELECT p,id,b FROM t FORCE INDEX({index}) WHERE b>=0 ORDER BY id")
                ),
                expected
            );
        }
    }
}

#[test]
fn drop_column_primary_rebuild_preserves_historical_views_without_new_key_facts() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT,a INT,b INT,PRIMARY KEY(id,a))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10,100),(1,20,200)")
        .unwrap();
    let mut reader = SessionContext::default();
    reader.connection_id = 29931;
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    // This reader has not bound t, so its old snapshot does not block DDL.
    engine.execute("DELETE FROM t WHERE a=20").unwrap();
    engine.execute("ALTER TABLE t DROP COLUMN a").unwrap();
    for (sql, expected) in [
        (
            "SELECT id,b FROM t WHERE id=1 ORDER BY b",
            vec![
                vec![Value::Int(1), Value::Int(100)],
                vec![Value::Int(1), Value::Int(200)],
            ],
        ),
        (
            "SELECT COUNT(*) FROM t WHERE id=1",
            vec![vec![Value::Int(2)]],
        ),
        (
            "SELECT COUNT(DISTINCT id) FROM t",
            vec![vec![Value::Int(1)]],
        ),
        (
            "SELECT COUNT(*) FROM t x JOIN t y ON x.id=y.id",
            vec![vec![Value::Int(4)]],
        ),
    ] {
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut reader, sql)
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(result.rows, expected, "{sql}");
    }
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT id,b FROM t FORCE INDEX(PRIMARY)"),
        vec![vec![Value::Int(1), Value::Int(100)]]
    );
}

#[test]
fn drop_column_replays_multi_value_expression_keys() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,a INT,j JSON,INDEX aj(a,(CAST(j->'$.ids' AS UNSIGNED ARRAY))))").unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10,'{\"ids\":[1,2]}'),(2,20,'{\"ids\":[2,3]}')")
        .unwrap();
    let hook = engine.database().install_online_copy_publish_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 29941;
        worker.execute_with_session(&mut session, "ALTER TABLE t DROP COLUMN a")
    });
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    engine
        .execute("UPDATE t SET a=11,j='{\"ids\":[7,8]}' WHERE id=1")
        .unwrap();
    hook.release();
    ddl.join().unwrap().unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM t FORCE INDEX(aj) WHERE 7 MEMBER OF(j->'$.ids')"
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM t FORCE INDEX(aj) WHERE 2 MEMBER OF(j->'$.ids')"
        ),
        vec![vec![Value::Int(2)]]
    );
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM t FORCE INDEX(aj) WHERE 8 MEMBER OF(j->'$.ids')"
        ),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn drop_column_chases_large_write_bursts_before_publication() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,a INT,b INT,UNIQUE ab(a,b))")
        .unwrap();
    let hook = engine.database().install_online_copy_publish_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 29951;
        worker.execute_with_session(&mut session, "ALTER TABLE t DROP COLUMN a, LOCK=NONE")
    });
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    // Arrive after the writable catch-up pass. This burst exceeds the final
    // barrier's row budget and must be chased while source writers can run.
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..3000).map(|i| vec![Value::Int(i), Value::Int(i), Value::Int(i)]),
    );
    hook.release();
    ddl.join().unwrap().unwrap();
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t FORCE INDEX(ab) WHERE b>=0"),
        vec![vec![Value::Int(3000)]]
    );
    assert_eq!(
        rows(&engine, "SELECT id FROM t FORCE INDEX(ab) WHERE b=2999"),
        vec![vec![Value::Int(2999)]]
    );
    assert!(engine
        .database()
        .alter_copy_job_snapshots()
        .iter()
        .any(|job| job.operation == "DROP_COLUMN" && job.state == "COMMITTED"));
    assert_eq!(engine.database().active_snapshot_count(), 0);
}
