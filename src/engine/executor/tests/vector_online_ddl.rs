//! Writable vector construction must keep the candidate outside business maintenance.
use super::support::query_rows;
use super::vector_type::HnswEnv;
use super::*;

#[test]
fn vector_online_ddl_accepts_none_and_persists() {
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "paged"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
    ]);
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL)")
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,'[1,2]'),(2,'[4,5]')")
            .unwrap();
        engine
            .execute("CREATE INDEX ix ON t(v) USING HNSW LOCK=NONE")
            .unwrap();
        assert_eq!(
            query_rows(
                &engine,
                "SELECT id FROM t ORDER BY L2_DISTANCE(v,'[1,2]') LIMIT 1"
            ),
            vec![vec![Value::Int(1)]]
        );
    }
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        query_rows(
            &engine,
            "SELECT id FROM t ORDER BY L2_DISTANCE(v,'[1,2]') LIMIT 1"
        ),
        vec![vec![Value::Int(1)]]
    );
}

fn assert_knn(engine: &StorageQueryEngine) {
    assert_eq!(
        engine.database().pending_vector_journal_for_test(),
        (0, 0),
        "completed DDL must release its private WAL floors"
    );
    let sql = "SELECT /*+ HNSW_EF(128) IVF_NPROBE(128) */ id FROM t WHERE active=1 ORDER BY L2_DISTANCE(v,'[0,0]') LIMIT 100";
    let plan = query_rows(engine, &format!("EXPLAIN {sql}"))
        .into_iter()
        .flatten()
        .map(|v| v.to_display_string())
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    // Tiny filtered KNN may deliberately choose an exact route. Independently
    // inspect the graph below so that fallback cannot hide missing ANN rows.
    assert!(plan.contains("vector"), "vector plan required: {plan}");
    let db = engine.database();
    let table = db.table("test.t").unwrap();
    let index = table
        .indexes
        .iter()
        .find(|i| i.is_vector() && i.state == crate::storage::IndexState::Public)
        .unwrap();
    let expected = query_rows(engine, "SELECT * FROM t")
        .into_iter()
        .filter(|row| table.index_contains_row(index, row).unwrap())
        .map(|row| row[0].to_display_string())
        .collect::<std::collections::BTreeSet<_>>();
    let mut actual = std::collections::BTreeSet::new();
    let read = db.begin_read_only_txn().unwrap();
    for sid in table.storage_ids() {
        let query = crate::engine::vector::codec::parse_text("[0,0]").unwrap();
        let effort = if index.vector_algo() == crate::storage::schema::VectorAlgo::Hnsw {
            crate::engine::vector::VectorSearchEffort::Hnsw { ef: 128 }
        } else {
            crate::engine::vector::VectorSearchEffort::Ivf { nprobe: 128 }
        };
        let result = db
            .vector_index_search_for_partition(
                &table,
                index,
                sid,
                &query,
                crate::engine::vector::VectorSearchRequest {
                    candidate_limit: 100,
                    rerank_limit: 100,
                    effort,
                },
            )
            .unwrap();
        for (row_id, _) in result.candidates {
            if let Some(row) = db.read_row_at(&table, sid, row_id, read).unwrap() {
                if table.index_contains_row(index, &row.values).unwrap() {
                    actual.insert(row.values[0].to_display_string());
                }
            }
        }
    }
    db.rollback_txn(read.txn_id).unwrap();
    assert_eq!(
        actual, expected,
        "the graph itself must cover every live member"
    );
    assert_eq!(
        query_rows(engine, sql),
        query_rows(
            engine,
            &sql.replace("L2_DISTANCE(v,'[0,0]')", "L2_DISTANCE(v,'[0,0]')+0")
        )
    );
    for id in query_rows(engine, "SELECT id FROM t WHERE v IS NOT NULL ORDER BY id") {
        let id = id[0].to_display_string();
        let v = query_rows(
            engine,
            &format!("SELECT VECTOR_TO_STRING(v) FROM t WHERE id={id}"),
        )[0][0]
            .to_display_string();
        assert_eq!(
            query_rows(
                engine,
                &format!("SELECT id FROM t WHERE active=1 ORDER BY L2_DISTANCE(v,'{v}') LIMIT 1")
            ),
            query_rows(
                engine,
                &format!("SELECT id FROM t WHERE active=1 ORDER BY L2_DISTANCE(v,'{v}')+0 LIMIT 1")
            )
        );
    }
}

#[test]
fn vector_online_ddl_concurrent_tail_rollback_partitions_and_private_graph() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ]);
        for algo in ["HNSW", "IVF"] {
            for partition in [false, true] {
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let engine = Arc::new(open_strict_test_engine(dir.path()));
                engine
                    .execute(&format!(
                        "CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,active INT) {}",
                        if partition {
                            "PARTITION BY HASH(id) PARTITIONS 2"
                        } else {
                            ""
                        }
                    ))
                    .unwrap();
                engine
                    .execute(
                        "INSERT INTO t VALUES(1,'[1,1]',1),(2,'[2,2]',1),(3,'[3,3]',0),(4,'[4,4]',1)",
                    )
                    .unwrap();
                let scan = engine.database().install_ddl_backfill_test_hook();
                let tail = engine
                    .database()
                    .install_online_index_validation_test_hook();
                let worker = engine.clone();
                let ddl = std::thread::spawn(move || {
                    let mut session = SessionContext::default();
                    session.connection_id = 33001;
                    worker.execute_with_session(&mut session, &format!("ALTER TABLE t ADD INDEX ix(v) USING {algo} WHERE active=1, ADD INDEX ax(active), LOCK=NONE"))
                });
                let reached = scan.wait_until_reached(Duration::from_secs(10));
                let mut writer = SessionContext::default();
                writer.connection_id = 33002;
                writer.sysvars.lock_wait_timeout = Some(0);
                let changes = (|| -> Result<(), QueryError> {
                    for sql in [
                        "BEGIN",
                        "UPDATE t SET v='[9,9]',id=11 WHERE id=1",
                        "DELETE FROM t WHERE id=2",
                        "SAVEPOINT s",
                        "INSERT INTO t VALUES(20,'[20,20]',1)",
                        "UPDATE t SET v='[99,99]' WHERE id=3",
                        "ROLLBACK TO s",
                        "INSERT INTO t VALUES(5,'[5,5]',1)",
                        "UPDATE t SET active=1 WHERE id=3",
                        "COMMIT",
                    ] {
                        engine.execute_with_session(&mut writer, sql)?;
                    }
                    Ok(())
                })();
                scan.release();
                if changes.is_err() {
                    tail.release();
                }
                let validating = tail.wait_until_reached(Duration::from_secs(10));
                let private = !engine
                    .database()
                    .vector_index_runtime_stats()
                    .keys()
                    .any(|(_, name)| name == "ix");
                let final_writes = (|| -> Result<(), QueryError> {
                    for sql in [
                        "UPDATE t SET v='[0,0]' WHERE id=5",
                        "INSERT INTO t VALUES(6,'[6,6]',1)",
                        "DELETE FROM t WHERE id=11",
                        "UPDATE t SET active=0 WHERE id=3",
                    ] {
                        engine.execute_with_session(&mut writer, sql)?;
                    }
                    Ok(())
                })();
                // A checkpoint must retain a private resident base and its DDL tail.
                let checkpoint = engine.checkpoint();
                tail.release();
                let result = ddl.join().unwrap();
                assert!(
                    reached && validating && private,
                    "{backend}/{algo}/{partition}: {result:?}"
                );
                changes.unwrap();
                final_writes.unwrap();
                checkpoint.unwrap();
                result.unwrap();
                assert_knn(&engine);
                assert!(engine
                    .database()
                    .vector_index_runtime_stats()
                    .keys()
                    .any(|(_, name)| name == "ix"));
                drop(engine);
                let engine = open_strict_test_engine(dir.path());
                assert_knn(&engine);
                engine
                    .execute("INSERT INTO t VALUES(7,'[0.5,0.5]',1)")
                    .unwrap();
                assert_knn(&engine);
            }
        }
    }
}

#[test]
fn vector_online_ddl_failed_capture_and_cancel_do_not_fail_business_commits() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", backend)]);
        for cancel in [false, true] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = Arc::new(open_strict_test_engine(dir.path()));
            engine.execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,active INT,INDEX old(v) USING HNSW)").unwrap();
            engine.execute("INSERT INTO t VALUES(1,'[1,1]',1)").unwrap();
            let hook = engine
                .database()
                .install_online_index_validation_test_hook();
            let worker = engine.clone();
            let ddl = std::thread::spawn(move || {
                let mut s = SessionContext::default();
                s.connection_id = 33011;
                worker.execute_with_session(&mut s, "CREATE INDEX ix ON t(v) USING IVF LOCK=NONE")
            });
            let reached = hook.wait_until_reached(Duration::from_secs(10));
            if cancel {
                let job = engine
                    .database()
                    .alter_copy_job_snapshots()
                    .into_iter()
                    .find(|j| j.table == "t")
                    .unwrap();
                engine.database().cancel_ddl_job(job.job_id).unwrap();
            } else {
                engine.database().exhaust_vector_build_log_for_test();
            }
            let writes = engine
                .execute("UPDATE t SET v='[8,8]' WHERE id=1")
                .and_then(|_| engine.execute("INSERT INTO t VALUES(2,'[0,0]',1)"));
            hook.release();
            let result = ddl.join().unwrap();
            assert!(reached, "{result:?}");
            writes.unwrap();
            assert!(result.is_err());
            assert!(!engine
                .database()
                .table("test.t")
                .unwrap()
                .indexes
                .iter()
                .any(|i| i.name == "ix"));
            assert_knn(&engine);
            engine
                .execute("CREATE INDEX ix ON t(v) USING IVF LOCK=NONE")
                .unwrap();
            assert_knn(&engine);
            engine.checkpoint().unwrap();
            drop(engine);
            let engine = open_strict_test_engine(dir.path());
            assert_knn(&engine);
        }
    }
}

#[test]
fn vector_online_ddl_crash_child() {
    let Ok(phase) = std::env::var("MYDB_VECTOR_ONLINE_PHASE") else {
        return;
    };
    let phase = phase.parse::<u64>().unwrap();
    let dir = std::env::var("MYDB_VECTOR_ONLINE_DIR").unwrap();
    let algo = std::env::var("MYDB_VECTOR_ONLINE_ALGO").unwrap();
    let engine = Arc::new(open_strict_test_engine(std::path::Path::new(&dir)));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,active INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'[1,1]',1),(2,'[2,2]',1)")
        .unwrap();
    engine.checkpoint().unwrap();
    let hook = if phase == 21 {
        engine.database().install_ddl_backfill_test_hook()
    } else {
        engine
            .database()
            .install_online_index_validation_test_hook()
    };
    engine.database().set_vector_crash_phase_for_test(phase);
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut s = SessionContext::default();
        s.connection_id = 33021;
        worker.execute_with_session(
            &mut s,
            &format!("CREATE INDEX ix ON t(v) USING {algo} LOCK=NONE"),
        )
    });
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    engine.execute("UPDATE t SET v='[8,8]' WHERE id=1").unwrap();
    engine.execute("DELETE FROM t WHERE id=2").unwrap();
    engine.execute("INSERT INTO t VALUES(3,'[0,0]',1)").unwrap();
    engine.checkpoint().unwrap();
    hook.release();
    ddl.join().unwrap().unwrap();
    panic!("crash point was not reached");
}

#[test]
fn vector_online_ddl_recovers_each_publication_boundary() {
    for backend in ["memory", "paged"] {
        for algo in ["HNSW", "IVF"] {
            for phase in [21, 22, 23, 24] {
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let output = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "engine::executor::tests::vector_online_ddl::vector_online_ddl_crash_child",
                        "--nocapture",
                    ])
                    .env("MYDB_VECTOR_STORAGE", backend)
                    .env("MYDB_VECTOR_ONLINE_ALGO", algo)
                    .env("MYDB_VECTOR_ONLINE_PHASE", phase.to_string())
                    .env("MYDB_VECTOR_ONLINE_DIR", dir.path())
                    .output()
                    .unwrap();
                assert_eq!(
                    output.status.code(),
                    Some(86),
                    "{backend}/{algo}/{phase}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let _env = HnswEnv::set(&[
                    ("MYDB_VECTOR_STORAGE", backend),
                    ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
                ]);
                let engine = open_strict_test_engine(dir.path());
                assert_eq!(
                    query_rows(&engine, "SELECT id FROM t ORDER BY id"),
                    vec![vec![Value::Int(1)], vec![Value::Int(3)]]
                );
                assert_eq!(
                    engine
                        .database()
                        .table("test.t")
                        .unwrap()
                        .indexes
                        .iter()
                        .any(|i| i.name == "ix"),
                    phase == 24
                );
                if phase != 24 {
                    engine
                        .execute(&format!("CREATE INDEX ix ON t(v) USING {algo} LOCK=NONE"))
                        .unwrap();
                }
                assert_knn(&engine);
                engine.checkpoint().unwrap();
                drop(engine);
                let engine = open_strict_test_engine(dir.path());
                assert_knn(&engine);
            }
        }
    }
}

#[test]
fn vector_online_ddl_persistence_failure_is_candidate_local() {
    let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    for stage in [1, 2, 3, 4, 5] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,active INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,'[1,1]',1)").unwrap();
        engine
            .database()
            .inject_vector_persist_failure_for_test(stage);
        assert!(engine
            .execute("CREATE INDEX ix ON t(v) USING HNSW LOCK=NONE")
            .is_err());
        engine.execute("INSERT INTO t VALUES(2,'[0,0]',1)").unwrap();
        engine
            .execute("CREATE INDEX ix ON t(v) USING HNSW LOCK=NONE")
            .unwrap();
        assert_knn(&engine);
    }
}

#[test]
fn vector_online_ddl_memory_durability_empty_graph_and_shared_contract() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ]);
        for algo in ["HNSW", "IVF"] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = Arc::new(open_complete_memory_test_engine(dir.path()));
            engine
                .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,active INT)")
                .unwrap();
            engine
                .execute(&format!("CREATE INDEX ix ON t(v) USING {algo} LOCK=NONE"))
                .unwrap();
            engine
                .execute("INSERT INTO t VALUES(1,'[1,1]',1),(2,'[2,2]',1)")
                .unwrap();
            assert_knn(&engine);
            let hook = engine.database().install_ddl_backfill_test_hook();
            let worker = engine.clone();
            let ddl = std::thread::spawn(move || {
                let mut s = SessionContext::default();
                s.connection_id = 33031;
                worker.execute_with_session(
                    &mut s,
                    &format!("CREATE INDEX shared ON t(v) USING {algo} LOCK=SHARED"),
                )
            });
            let reached = hook.wait_until_reached(Duration::from_secs(10));
            let read = engine.execute("SELECT COUNT(*) FROM t");
            let mut writer = SessionContext::default();
            writer.connection_id = 33032;
            writer.sysvars.lock_wait_timeout = Some(0);
            let write =
                engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(3,'[3,3]',1)");
            hook.release();
            let result = ddl.join().unwrap();
            assert!(reached, "{result:?}");
            read.unwrap();
            assert_eq!(write.unwrap_err().code(), 1205);
            result.unwrap();
            engine.execute("INSERT INTO t VALUES(3,'[3,3]',1)").unwrap();
            assert_knn(&engine);
        }
    }
}

#[test]
fn vector_online_ddl_typed_quantized_builds_replay_and_reopen() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ]);
        for datatype in ["FLOAT16", "BFLOAT16", "INT8"] {
            for algo in ["HNSW", "IVF"] {
                for quant in ["none", "sq8", "sq4", "pq", "bq"] {
                    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                    let engine = Arc::new(open_strict_test_engine(dir.path()));
                    engine
                        .execute(&format!(
                            "CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2,{datatype}) NOT NULL,active INT)"
                        ))
                        .unwrap();
                    engine.execute("INSERT INTO t VALUES(1,'[1,1]',1),(2,'[2,2]',1),(3,'[3,3]',1),(4,'[4,4]',1)").unwrap();
                    let hook = engine
                        .database()
                        .install_online_index_validation_test_hook();
                    let worker = engine.clone();
                    let ddl = std::thread::spawn(move || {
                        let mut s = SessionContext::default();
                        s.connection_id = 33041;
                        worker.execute_with_session(&mut s,&format!("CREATE INDEX ix ON t(v) USING {algo} WITH(quantization={quant}) LOCK=NONE"))
                    });
                    let reached = hook.wait_until_reached(Duration::from_secs(10));
                    let write = engine
                        .execute("UPDATE t SET v='[0,0]' WHERE id=3")
                        .and_then(|_| engine.execute("INSERT INTO t VALUES(5,'[5,5]',1)"));
                    hook.release();
                    let result = ddl.join().unwrap();
                    assert!(reached, "{backend}/{datatype}/{algo}/{quant}: {result:?}");
                    write.unwrap();
                    result.unwrap();
                    assert_knn(&engine);
                    drop(engine);
                    let engine = open_strict_test_engine(dir.path());
                    assert_knn(&engine);
                }
            }
        }
    }
}

#[test]
fn vector_online_ddl_xa_crash_child() {
    let Ok(dir) = std::env::var("MYDB_VECTOR_ONLINE_XA_DIR") else {
        return;
    };
    let engine = Arc::new(open_strict_test_engine(std::path::Path::new(&dir)));
    engine
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,active INT,INDEX old(v) USING HNSW)",
        )
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,'[1,1]',1)").unwrap();
    engine.checkpoint().unwrap();
    let hook = engine
        .database()
        .install_online_index_validation_test_hook();
    let worker = engine.clone();
    std::thread::spawn(move || {
        let mut s = SessionContext::default();
        s.connection_id = 33051;
        worker
            .execute_with_session(&mut s, "CREATE INDEX ix ON t(v) USING IVF LOCK=NONE")
            .unwrap();
    });
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    let mut s = SessionContext::default();
    s.connection_id = 33052;
    for sql in [
        "XA START 'vector_build'",
        "UPDATE t SET v='[0,0]' WHERE id=1",
        "INSERT INTO t VALUES(2,'[2,2]',1)",
        "XA END 'vector_build'",
        "XA PREPARE 'vector_build'",
    ] {
        engine.execute_with_session(&mut s, sql).unwrap();
    }
    engine.checkpoint().unwrap();
    std::process::exit(86);
}

#[test]
fn vector_online_ddl_prepared_xa_survives_scrub_and_decision() {
    for backend in ["memory", "paged"] {
        for decision in ["COMMIT", "ROLLBACK"] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "engine::executor::tests::vector_online_ddl::vector_online_ddl_xa_crash_child",
                    "--nocapture",
                ])
                .env("MYDB_VECTOR_STORAGE", backend)
                .env("MYDB_VECTOR_ONLINE_XA_DIR", dir.path())
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(86),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let _env = HnswEnv::set(&[
                ("MYDB_VECTOR_STORAGE", backend),
                ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ]);
            let engine = open_strict_test_engine(dir.path());
            assert!(!engine
                .database()
                .table("test.t")
                .unwrap()
                .indexes
                .iter()
                .any(|i| i.name == "ix"));
            engine
                .execute(&format!("XA {decision} 'vector_build'"))
                .unwrap();
            assert_eq!(
                query_rows(&engine, "SELECT COUNT(*) FROM t"),
                vec![vec![Value::Int(if decision == "COMMIT" { 2 } else { 1 })]]
            );
            assert_knn(&engine);
            engine
                .execute("CREATE INDEX ix ON t(v) USING IVF LOCK=NONE")
                .unwrap();
            assert_knn(&engine);
            engine.checkpoint().unwrap();
        }
    }
}

#[test]
#[ignore = "local vector DDL exclusion and concurrent writer probe"]
fn vector_online_ddl_lock_probe() {
    let max = std::env::var("MYDB_VECTOR_PROBE_MAX")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(100000);
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", backend)]);
        for algo in ["HNSW", "IVF"] {
            for count in [1000, 10000, 100000].into_iter().filter(|n| *n <= max) {
                for mode in ["EXCLUSIVE", "NONE"] {
                    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                    let engine = Arc::new(open_strict_test_engine(dir.path()));
                    engine
                        .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(8) NOT NULL)")
                        .unwrap();
                    insert_fixture_rows(
                        &engine,
                        "test.t",
                        (1..=count).map(|id| {
                            vec![
                                Value::Int(id),
                                Value::Vector(
                                    crate::engine::vector::codec::parse_text(&format!(
                                        "[{},{},{},{},{},{},{},{}]",
                                        id % 97,
                                        id % 89,
                                        id % 83,
                                        id % 79,
                                        id % 73,
                                        id % 71,
                                        id % 67,
                                        id % 61
                                    ))
                                    .unwrap(),
                                ),
                            ]
                        }),
                    );
                    engine.checkpoint().unwrap();
                    let before = engine.database().storage_status();
                    let started = Instant::now();
                    let sql = format!(
                        "CREATE INDEX ix ON t(v) USING {algo} WITH({}) LOCK={mode}",
                        if algo == "HNSW" {
                            "m=8,ef_construction=64"
                        } else {
                            "nlist=16,kmeans_iters=4"
                        }
                    );
                    engine.execute(&sql).unwrap();
                    let elapsed = started.elapsed().as_micros();
                    let after = engine.database().storage_status();
                    let hold = (after.online_index_prepare_us - before.online_index_prepare_us)
                        + (after.online_index_publish_us - before.online_index_publish_us);
                    eprintln!("VECTOR_DDL_PROBE backend={backend} algo={algo} rows={count} mode={mode} elapsed_us={elapsed} hold_us={hold}");
                }
            }
        }
    }
}

#[test]
#[ignore = "local ordinary vector DML and KNN paired regression probe"]
fn vector_online_ddl_dml_probe() {
    let control_queries = std::env::var("MYDB_VECTOR_CONTROL_KNN_PROBE_ITERATIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(200);
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ]);
        for kind in ["btree", "HNSW", "IVF"] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = open_strict_test_engine(dir.path());
            engine
                .execute(&format!(
                    "CREATE TABLE t(id INT PRIMARY KEY,n INT,v VECTOR(2) NOT NULL {})",
                    if kind == "btree" {
                        ""
                    } else if kind == "HNSW" {
                        ",INDEX ix(v) USING HNSW"
                    } else {
                        ",INDEX ix(v) USING IVF WITH(nlist=4)"
                    }
                ))
                .unwrap();
            engine
                .execute(
                    "INSERT INTO t VALUES(1,0,'[1,1]'),(2,0,'[2,2]'),(3,0,'[3,3]'),(4,0,'[4,4]')",
                )
                .unwrap();
            let mut s = SessionContext::default();
            s.connection_id = 33061;
            let query_count = if kind == "btree" {
                control_queries
            } else {
                200
            };
            for round in 0..5 {
                let start = Instant::now();
                // Separate commits measure the ordinary vector commit path.
                for i in 0..200 {
                    engine
                        .execute_with_session(
                            &mut s,
                            &format!(
                                "UPDATE t SET n=n+1,v=IF(MOD(n,2)=0,'[5,5]','[6,6]') WHERE id={}",
                                i % 4 + 1
                            ),
                        )
                        .unwrap();
                }
                eprintln!(
                    "VECTOR_DML_PROBE backend={backend} kind={kind} round={round} elapsed_us={}",
                    start.elapsed().as_micros()
                );
                let start = Instant::now();
                for _ in 0..query_count {
                    engine
                        .execute_with_session(
                            &mut s,
                            "SELECT id FROM t ORDER BY L2_DISTANCE(v,'[0,0]') LIMIT 2",
                        )
                        .unwrap();
                }
                eprintln!(
                    "VECTOR_KNN_PROBE backend={backend} kind={kind} round={round} queries={query_count} elapsed_us={}",
                    start.elapsed().as_micros()
                );
            }
        }
    }
}

#[test]
fn vector_online_ddl_invalid_candidate_does_not_reject_committed_source_row() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", backend)]);
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,active INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,'[1,1]',1)").unwrap();
        let hook = engine
            .database()
            .install_online_index_validation_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut s = SessionContext::default();
            s.connection_id = 33091;
            worker.execute_with_session(
                &mut s,
                "CREATE INDEX ix ON t(v) USING HNSW WITH(metric=COSINE) LOCK=NONE",
            )
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let write = engine.execute("INSERT INTO t VALUES(2,'[0,0]',1)");
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{result:?}");
        write.unwrap();
        assert!(
            result.is_err(),
            "zero vector must invalidate the cosine candidate"
        );
        assert_eq!(
            query_rows(&engine, "SELECT COUNT(*) FROM t"),
            vec![vec![Value::Int(2)]]
        );
        engine
            .execute("CREATE INDEX ix ON t(v) USING HNSW WITH(metric=L2) LOCK=NONE")
            .unwrap();
    }
}

#[test]
fn vector_online_ddl_binary_metrics_capture_tail() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ]);
        for algo in ["HNSW", "IVF"] {
            for metric in ["HAMMING", "JACCARD"] {
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let engine = Arc::new(open_strict_test_engine(dir.path()));
                engine
                    .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(9,BINARY) NOT NULL)")
                    .unwrap();
                engine
                    .execute(
                        "INSERT INTO t VALUES(1,'[1,0,1,0,1,0,1,0,1]'),(2,'[0,1,0,1,0,1,0,1,0]')",
                    )
                    .unwrap();
                let hook = engine
                    .database()
                    .install_online_index_validation_test_hook();
                let worker = engine.clone();
                let ddl = std::thread::spawn(move || {
                    let mut s = SessionContext::default();
                    s.connection_id = 33101;
                    worker.execute_with_session(
                        &mut s,
                        &format!(
                            "CREATE INDEX ix ON t(v) USING {algo} WITH(metric={metric}) LOCK=NONE"
                        ),
                    )
                });
                let reached = hook.wait_until_reached(Duration::from_secs(10));
                let write = engine.execute("UPDATE t SET v='[0,0,0,0,0,0,0,0,0]' WHERE id=1");
                hook.release();
                let result = ddl.join().unwrap();
                assert!(reached, "{result:?}");
                write.unwrap();
                result.unwrap();
                let sql=format!("SELECT id FROM t FORCE INDEX(ix) ORDER BY {metric}_DISTANCE(v,TO_VECTOR('[0,0,0,0,0,0,0,0,0]','BINARY')) LIMIT 1");
                assert_eq!(query_rows(&engine, &sql), vec![vec![Value::Int(1)]]);
                drop(engine);
                let engine = open_strict_test_engine(dir.path());
                assert_eq!(query_rows(&engine, &sql), vec![vec![Value::Int(1)]]);
            }
        }
    }
}

#[test]
fn vector_online_ddl_mixed_special_indexes_publish_or_abort_together() {
    let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "paged")]);
    for fail in [false, true] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine.execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,body TEXT,g GEOMETRY NOT NULL)").unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,'[1,1]','quartz amber',POINT(1,1))")
            .unwrap();
        let hook = engine
            .database()
            .install_online_index_validation_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut s = SessionContext::default();
            s.connection_id = 33111;
            worker.execute_with_session(&mut s,"ALTER TABLE t ADD INDEX vi(v) USING HNSW,ADD FULLTEXT ft(body),ADD SPATIAL sp(g),LOCK=NONE")
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        if fail {
            engine.database().exhaust_vector_build_log_for_test();
        }
        let write =
            engine.execute("UPDATE t SET v='[0,0]',body='quartz cobalt',g=POINT(2,2) WHERE id=1");
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{result:?}");
        write.unwrap();
        assert_eq!(result.is_err(), fail);
        let table = engine.database().table("test.t").unwrap();
        assert_eq!(table.indexes.len(), if fail { 1 } else { 4 });
        if !fail {
            let read = engine.database().begin_read_only_txn().unwrap();
            super::fulltext_incremental::assert_corpora(&engine, "test.t", read);
            engine.database().rollback_txn(read.txn_id).unwrap();
            assert_eq!(
                query_rows(
                    &engine,
                    "SELECT id FROM t FORCE INDEX(vi) ORDER BY L2_DISTANCE(v,'[0,0]') LIMIT 1"
                ),
                vec![vec![Value::Int(1)]]
            );
        }
        engine
            .execute("INSERT INTO t VALUES(2,'[2,2]','surviving amber',POINT(3,3))")
            .unwrap();
        engine.checkpoint().unwrap();
    }
}

#[test]
#[ignore = "local actual writer progress and latency during vector construction"]
fn vector_online_ddl_writer_probe() {
    let count = std::env::var("MYDB_VECTOR_WRITER_ROWS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(10000);
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", backend)]);
        for algo in ["HNSW", "IVF"] {
            for mode in ["EXCLUSIVE", "NONE"] {
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let engine = Arc::new(open_strict_test_engine(dir.path()));
                engine
                    .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(8) NOT NULL)")
                    .unwrap();
                insert_fixture_rows(
                    &engine,
                    "test.t",
                    (1..=count).map(|id| {
                        vec![
                            Value::Int(id),
                            Value::Vector(
                                crate::engine::vector::codec::parse_text(&format!(
                                    "[{},{},{},{},{},{},{},{}]",
                                    id % 97,
                                    id % 89,
                                    id % 83,
                                    id % 79,
                                    id % 73,
                                    id % 71,
                                    id % 67,
                                    id % 61
                                ))
                                .unwrap(),
                            ),
                        ]
                    }),
                );
                engine.checkpoint().unwrap();
                let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let running = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let progress = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let (send, receive) = std::sync::mpsc::channel();
                let worker = engine.clone();
                let done = stop.clone();
                let active = running.clone();
                let progressed = progress.clone();
                let writer = std::thread::spawn(move || {
                    let mut s = SessionContext::default();
                    s.connection_id = 33121;
                    s.sysvars.lock_wait_timeout = Some(1);
                    let mut samples = Vec::new();
                    let mut errors = 0;
                    let mut turn = 0;
                    while !done.load(std::sync::atomic::Ordering::Acquire) {
                        let start = Instant::now();
                        let result = worker.execute_with_session(
                            &mut s,
                            &format!("UPDATE t SET v='[{},2,3,4,5,6,7,8]' WHERE id=1", turn % 2),
                        );
                        turn += 1;
                        samples.push(start.elapsed().as_micros() as u64);
                        match result {
                            Ok(_) => {
                                if active.load(std::sync::atomic::Ordering::Acquire) {
                                    progressed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                }
                            }
                            Err(err) => {
                                assert_eq!(err.code(), 1205, "{err}");
                                errors += 1;
                            }
                        }
                        let _ = send.send(());
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    (samples, errors)
                });
                receive.recv_timeout(Duration::from_secs(10)).unwrap();
                running.store(true, std::sync::atomic::Ordering::Release);
                let before = engine.database().storage_status();
                let start = Instant::now();
                let result = engine.execute(&format!(
                    "CREATE INDEX ix ON t(v) USING {algo} WITH({}) LOCK={mode}",
                    if algo == "HNSW" {
                        "m=8,ef_construction=64"
                    } else {
                        "nlist=16,kmeans_iters=4"
                    }
                ));
                let elapsed = start.elapsed().as_micros();
                running.store(false, std::sync::atomic::Ordering::Release);
                stop.store(true, std::sync::atomic::Ordering::Release);
                let (mut samples, errors) = writer.join().unwrap();
                result.unwrap();
                samples.sort_unstable();
                let after = engine.database().storage_status();
                let hold = after.online_index_prepare_us + after.online_index_publish_us
                    - before.online_index_prepare_us
                    - before.online_index_publish_us;
                let progress = progress.load(std::sync::atomic::Ordering::Relaxed);
                eprintln!("VECTOR_WRITER_PROBE backend={backend} algo={algo} rows={count} mode={mode} elapsed_us={elapsed} hold_us={hold} commits={progress} errors={errors} writer_p95_us={} writer_max_us={}",samples[samples.len()*95/100],samples.last().unwrap());
                if mode == "NONE" {
                    assert!(
                        progress >= 2,
                        "actual construction must admit business commits"
                    );
                }
            }
        }
    }
}

#[test]
fn vector_online_ddl_waits_for_decided_commit_maintenance_and_preserves_old_snapshot() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ]);
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine.execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,active INT,INDEX old(v) USING HNSW)").unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,'[1,1]',1),(2,'[2,2]',1)")
            .unwrap();
        let mut reader = SessionContext::default();
        reader.connection_id = 33131;
        engine
            .execute_with_session(&mut reader, "SET transaction_isolation='REPEATABLE-READ'")
            .unwrap();
        engine
            .execute_with_session(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .unwrap();
        let sql = "SELECT id,VECTOR_TO_STRING(v) FROM t ORDER BY L2_DISTANCE(v,'[0,0]') LIMIT 2";
        let expected = engine
            .execute_with_session(&mut reader, sql)
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(expected) = expected else {
            panic!()
        };
        let catchup = engine
            .database()
            .install_online_index_validation_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut s = SessionContext::default();
            s.connection_id = 33132;
            worker.execute_with_session(&mut s, "CREATE INDEX ix ON t(v) USING IVF LOCK=NONE")
        });
        let reached = catchup.wait_until_reached(Duration::from_secs(10));
        let completion = engine.database().install_vector_apply_test_hook();
        let writer = engine.clone();
        let commit = std::thread::spawn(move || {
            let mut s = SessionContext::default();
            s.connection_id = 33133;
            writer.execute_with_session(&mut s, "UPDATE t SET v='[0,0]' WHERE id=2")
        });
        let decided = completion.wait_until_reached(Duration::from_secs(10));
        catchup.release();
        // The source commit retains its table intent lock through maintenance.
        std::thread::sleep(Duration::from_millis(20));
        let building = engine
            .database()
            .table("test.t")
            .unwrap()
            .indexes
            .iter()
            .any(|i| i.name == "ix" && i.state == crate::storage::IndexState::Building);
        completion.release();
        let written = commit.join().unwrap();
        let result = ddl.join().unwrap();
        assert!(reached && decided && building, "{result:?}");
        written.unwrap();
        result.unwrap();
        let QueryResponse::ResultSet(actual) = engine
            .execute_with_session(&mut reader, sql)
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(actual.rows, expected.rows);
        engine.execute_with_session(&mut reader, "COMMIT").unwrap();
        assert_knn(&engine);
        engine.checkpoint().unwrap();
    }
}

#[test]
fn vector_online_ddl_cosine_validation_ignores_rolled_back_top_versions() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", backend)]);
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,active INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,'[1,1]',1)").unwrap();
        let scan = engine.database().install_ddl_backfill_test_hook();
        let tail = engine
            .database()
            .install_online_index_validation_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut s = SessionContext::default();
            s.connection_id = 33141;
            worker.execute_with_session(
                &mut s,
                "CREATE INDEX ix ON t(v) USING HNSW WITH(metric=COSINE) LOCK=NONE",
            )
        });
        let reached = scan.wait_until_reached(Duration::from_secs(10));
        let mut writer = SessionContext::default();
        writer.connection_id = 33142;
        let writes = engine
            .execute_with_session(&mut writer, "BEGIN")
            .and_then(|_| {
                engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(2,'[0,0]',1)")
            });
        scan.release();
        let validating = tail.wait_until_reached(Duration::from_secs(10));
        let rollback = engine.execute_with_session(&mut writer, "ROLLBACK");
        tail.release();
        let result = ddl.join().unwrap();
        assert!(reached && validating, "{result:?}");
        writes.unwrap();
        rollback.unwrap();
        result.unwrap();
        assert_eq!(
            query_rows(
                &engine,
                "SELECT id FROM t FORCE INDEX(ix) ORDER BY COSINE_DISTANCE(v,'[1,1]') LIMIT 1"
            ),
            vec![vec![Value::Int(1)]]
        );
        assert!(engine.execute("INSERT INTO t VALUES(3,'[0,0]',1)").is_err());
    }
}

#[test]
fn vector_online_ddl_first_quantization_model_trains_outside_publication_barrier() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ]);
        for algo in ["HNSW", "IVF"] {
            for quant in ["sq8", "pq"] {
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let engine = Arc::new(open_strict_test_engine(dir.path()));
                engine.execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2,FLOAT16) NOT NULL,active INT)").unwrap();
                let before = engine.database().vector_model_retries_for_test();
                let writer = engine.clone();
                *crate::storage::VECTOR_BUILD_BEFORE_PUBLISH_HOOK.lock() =
                    Some(Box::new(move || {
                        std::thread::spawn(move || {
                            let mut s = SessionContext::default();
                            s.connection_id = 33152;
                            let values = (1..=16)
                                .map(|id| format!("({id},'[{id},{id}]',1)"))
                                .collect::<Vec<_>>()
                                .join(",");
                            writer
                                .execute_with_session(
                                    &mut s,
                                    &format!("INSERT INTO t VALUES {values}"),
                                )
                                .unwrap();
                        })
                        .join()
                        .unwrap();
                    }));
                let mut s = SessionContext::default();
                s.connection_id = 33151;
                engine.execute_with_session(&mut s,&format!("CREATE INDEX ix ON t(v) USING {algo} WITH(quantization={quant}{}) LOCK=NONE",if quant=="pq"{",pq_m=1,pq_bits=4,pq_iterations=2"}else{""})).unwrap();
                assert!(
                    engine.database().vector_model_retries_for_test() > before,
                    "first model must defer publication: {backend}/{algo}/{quant}"
                );
                assert_knn(&engine);
                engine.checkpoint().unwrap();
                drop(engine);
                let engine = open_strict_test_engine(dir.path());
                assert_knn(&engine);
            }
        }
    }
}

#[test]
fn vector_online_ddl_model_crash_child() {
    let Ok(dir) = std::env::var("MYDB_VECTOR_MODEL_DIR") else {
        return;
    };
    let phase = std::env::var("MYDB_VECTOR_ONLINE_PHASE")
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let algo = std::env::var("MYDB_VECTOR_ONLINE_ALGO").unwrap();
    let quant = std::env::var("MYDB_VECTOR_MODEL_QUANT").unwrap();
    let engine = Arc::new(open_strict_test_engine(std::path::Path::new(&dir)));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2,FLOAT16) NOT NULL,active INT)")
        .unwrap();
    engine.checkpoint().unwrap();
    let hook = engine
        .database()
        .install_online_index_validation_test_hook();
    engine.database().set_vector_crash_phase_for_test(phase);
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut s = SessionContext::default();
        s.connection_id = 33161;
        worker.execute_with_session(
            &mut s,
            &format!(
                "CREATE INDEX ix ON t(v) USING {algo} WITH(quantization={quant}{}) LOCK=NONE",
                if quant == "pq" {
                    ",pq_m=1,pq_bits=4,pq_iterations=2"
                } else {
                    ""
                }
            ),
        )
    });
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    let values = (1..=16)
        .map(|id| format!("({id},'[{id},{id}]',1)"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    hook.release();
    ddl.join().unwrap().unwrap();
    panic!("model publication crash point was not reached");
}

#[test]
fn vector_online_ddl_recovers_first_model_with_catalog_decision() {
    for backend in ["memory", "paged"] {
        for algo in ["HNSW", "IVF"] {
            for quant in ["sq8", "pq"] {
                for phase in [23, 24] {
                    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                    let output=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","engine::executor::tests::vector_online_ddl::vector_online_ddl_model_crash_child","--nocapture"])
                        .env("MYDB_VECTOR_STORAGE",backend).env("MYDB_VECTOR_ONLINE_ALGO",algo).env("MYDB_VECTOR_ONLINE_PHASE",phase.to_string()).env("MYDB_VECTOR_MODEL_QUANT",quant).env("MYDB_VECTOR_MODEL_DIR",dir.path()).output().unwrap();
                    assert_eq!(
                        output.status.code(),
                        Some(86),
                        "{backend}/{algo}/{quant}/{phase}: {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                    let _env = HnswEnv::set(&[
                        ("MYDB_VECTOR_STORAGE", backend),
                        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
                        ("MYDB_HNSW_COMPACT_DISABLE", "1"),
                    ]);
                    let scans = crate::storage::Database::vector_build_heap_scans_for_test();
                    let engine = open_strict_test_engine(dir.path());
                    assert_eq!(
                        engine
                            .database()
                            .table("test.t")
                            .unwrap()
                            .indexes
                            .iter()
                            .any(|i| i.name == "ix"),
                        phase == 24
                    );
                    assert_eq!(
                        query_rows(&engine, "SELECT COUNT(*) FROM t"),
                        vec![vec![Value::Int(16)]]
                    );
                    if phase == 23 {
                        engine.execute(&format!("CREATE INDEX ix ON t(v) USING {algo} WITH(quantization={quant}{}) LOCK=NONE",if quant=="pq"{",pq_m=1,pq_bits=4,pq_iterations=2"}else{""})).unwrap();
                    }
                    assert_knn(&engine);
                    if phase == 24 {
                        assert_eq!(
                            crate::storage::Database::vector_build_heap_scans_for_test(),
                            scans,
                            "first-model recovery cannot rebuild the graph from heap"
                        );
                    }
                    engine.checkpoint().unwrap();
                    drop(engine);
                    let engine = open_strict_test_engine(dir.path());
                    assert_knn(&engine);
                }
            }
        }
    }
}
