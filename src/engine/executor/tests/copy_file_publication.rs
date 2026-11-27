use super::support::query_rows;
use super::vector_type::HnswEnv;
use super::*;
use std::path::Path;

fn fixture(engine: &StorageQueryEngine) {
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,n INT UNIQUE,b LONGTEXT,v VECTOR(2) NOT NULL,INDEX ix(v) USING HNSW)").unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES(1,10,'base','[1,1]'),(2,20,'base','[2,2]'),(3,30,'base','[3,3]')",
        )
        .unwrap();
}
#[track_caller]
fn check(engine: &StorageQueryEngine) {
    assert_eq!(
        query_rows(engine, "SELECT id,n,LENGTH(b) FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(11), Value::Int(20000)],
            vec![Value::Int(3), Value::Int(30), Value::Int(4)],
            vec![Value::Int(4), Value::Int(40), Value::Int(20000)]
        ]
    );
    assert_eq!(
        query_rows(
            engine,
            "SELECT id FROM t FORCE INDEX(ix) ORDER BY L2_DISTANCE(v,'[0,0]') LIMIT 1"
        ),
        vec![vec![Value::Int(4)]]
    );
    assert_eq!(
        query_rows(engine, "SELECT id FROM t FORCE INDEX(n) WHERE n=11"),
        vec![vec![Value::Int(1)]]
    );
}
fn writes(engine: &StorageQueryEngine) {
    let mut session = SessionContext::default();
    session.connection_id = 33401;
    session.sysvars.lock_wait_timeout = Some(0);
    for sql in [
        "UPDATE t SET n=11,b=REPEAT('x',20000),v='[9,9]' WHERE id=1",
        "DELETE FROM t WHERE id=2",
        "INSERT INTO t VALUES(4,40,REPEAT('y',20000),'[0,0]')",
        "BEGIN",
        "UPDATE t SET n=99 WHERE id=3",
        "SAVEPOINT s",
        "DELETE FROM t WHERE id=3",
        "ROLLBACK TO SAVEPOINT s",
        "ROLLBACK",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
}
#[test]
fn copy_file_publication_concurrent_lob_tail_and_post_release_sync() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ]);
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        fixture(&engine);
        let tail = engine.database().install_online_copy_publish_test_hook();
        let sync = engine.database().install_copy_file_stage_test_hook(5);
        let worker = engine.clone();
        let ddl =
            std::thread::spawn(move || worker.execute("ALTER TABLE t MODIFY n BIGINT,LOCK=NONE"));
        let reached = tail.wait_until_reached(Duration::from_secs(15));
        if reached {
            writes(&engine);
        }
        tail.release();
        let released = sync.wait_until_reached(Duration::from_secs(15));
        if released {
            assert!(engine
                .database()
                .copy_publication_floor_for_test()
                .is_some());
            let mut s = SessionContext::default();
            s.connection_id = 33402;
            s.sysvars.lock_wait_timeout = Some(0);
            engine
                .execute_with_session(&mut s, "UPDATE t SET n=n+1 WHERE id=3")
                .unwrap();
            engine
                .execute_with_session(&mut s, "UPDATE t SET n=n-1 WHERE id=3")
                .unwrap();
            engine.checkpoint().unwrap();
        }
        sync.release();
        let result = ddl.join().unwrap();
        assert!(reached && released, "{backend}: {result:?}");
        result.unwrap();
        assert!(engine
            .database()
            .copy_publication_floor_for_test()
            .is_none());
        check(&engine);
        drop(engine);
        let scans = crate::storage::Database::vector_build_heap_scans_for_test();
        let engine = open_strict_test_engine(dir.path());
        check(&engine);
        let table = engine.database().table("test.t").unwrap();
        let index = table.indexes.iter().find(|i| i.name == "ix").unwrap();
        engine
            .database()
            .vector_graph_get_or_build(&table, index)
            .unwrap();
        assert_eq!(
            crate::storage::Database::vector_build_heap_scans_for_test(),
            scans,
            "COPY winner must restore its graph image"
        );
        engine.checkpoint().unwrap();
        drop(engine);
        check(&open_strict_test_engine(dir.path()));
    }
}

#[test]
fn copy_file_publication_crash_child() {
    let Ok(dir) = std::env::var("MYDB_COPY_PUBLISH_DIR") else {
        return;
    };
    let engine = Arc::new(open_strict_test_engine(Path::new(&dir)));
    fixture(&engine);
    let tail = engine.database().install_online_copy_publish_test_hook();
    let capture = std::env::var_os("MYDB_COPY_CAPTURE_INDEX_BASE")
        .map(|_| engine.database().install_copy_file_stage_test_hook(2));
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || worker.execute("ALTER TABLE t MODIFY n BIGINT,LOCK=NONE"));
    if let Some(capture) = capture {
        let reached = capture.wait_until_reached(Duration::from_secs(15));
        if reached {
            let id = engine.database().table("test.t").unwrap().id;
            let intent: serde_json::Value = serde_json::from_slice(
                &std::fs::read(Path::new(&dir).join(format!("ddl/alter_copy/{id}.job"))).unwrap(),
            )
            .unwrap();
            for pair in intent["shadow_to_live"].as_array().unwrap() {
                let source = pair[0].as_str().unwrap();
                let target = pair[1].as_str().unwrap();
                if !source.ends_with(".idx") {
                    continue;
                }
                let saved = Path::new(&dir).join("saved-index-bases").join(target);
                std::fs::create_dir_all(saved.parent().unwrap()).unwrap();
                std::fs::copy(Path::new(&dir).join(source), saved).unwrap();
            }
        }
        capture.release();
        assert!(reached);
    }
    let phase = std::env::var("MYDB_COPY_PUBLISH_CRASH")
        .unwrap()
        .parse::<u8>()
        .unwrap();
    if phase >= 3 {
        let reached = tail.wait_until_reached(Duration::from_secs(15));
        if reached {
            writes(&engine);
            engine.checkpoint().unwrap();
        }
        tail.release();
        assert!(reached);
    } else {
        tail.release();
    }
    panic!("crash point was not reached: {:?}", ddl.join().unwrap());
}
#[test]
fn copy_file_publication_recovers_all_decision_and_sync_boundaries() {
    for backend in ["memory", "paged"] {
        for phase in 1..=6u8 {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "copy_file_publication_crash_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env("MYDB_COPY_PUBLISH_DIR", dir.path())
                .env("MYDB_COPY_PUBLISH_CRASH", phase.to_string())
                .env("MYDB_VECTOR_STORAGE", backend)
                .env("MYDB_HNSW_BRUTE_THRESHOLD", "0")
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(86),
                "{backend}/{phase}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let _env = HnswEnv::set(&[
                ("MYDB_VECTOR_STORAGE", backend),
                ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ]);
            let engine = open_strict_test_engine(dir.path());
            let table = engine.database().table("test.t").unwrap();
            assert_eq!(
                table.active_generation >= 2,
                phase >= 4,
                "{backend}/{phase}"
            );
            if phase >= 3 {
                check(&engine);
            } else {
                assert_eq!(
                    query_rows(&engine, "SELECT COUNT(*) FROM t"),
                    vec![vec![Value::Int(3)]]
                );
            }
            engine.checkpoint().unwrap();
            drop(engine);
            let engine = open_strict_test_engine(dir.path());
            if phase >= 3 {
                check(&engine);
            }
        }
    }
}
#[test]
fn copy_file_publication_sync_failure_preserves_decided_generation_for_recovery() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    fixture(&engine);
    engine.database().fail_copy_deferred_sync_for_test();
    let result = engine.execute("ALTER TABLE t MODIFY n BIGINT,LOCK=NONE");
    assert!(result.is_err());
    assert!(engine
        .database()
        .copy_publication_floor_for_test()
        .is_some());
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert!(engine.database().table("test.t").unwrap().active_generation >= 2);
    assert_eq!(
        query_rows(&engine, "SELECT n FROM t ORDER BY id"),
        vec![
            vec![Value::Int(10)],
            vec![Value::Int(20)],
            vec![Value::Int(30)]
        ]
    );
}

#[test]
fn copy_file_publication_cancelled_prebinding_retries_same_generation() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    fixture(&engine);
    let hook = engine.database().install_copy_file_stage_test_hook(2);
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || worker.execute("ALTER TABLE t MODIFY n BIGINT,LOCK=NONE"));
    let reached = hook.wait_until_reached(Duration::from_secs(15));
    if reached {
        engine
            .database()
            .cancel_ddl_job(
                engine
                    .database()
                    .alter_copy_job_snapshots()
                    .iter()
                    .find(|j| j.state != "SUCCEEDED")
                    .unwrap()
                    .job_id,
            )
            .unwrap();
    }
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached);
    assert!(result.is_err());
    assert!(engine
        .database()
        .copy_publication_floor_for_test()
        .is_none());
    assert_eq!(
        engine.database().table("test.t").unwrap().active_generation,
        1
    );
    engine
        .execute("ALTER TABLE t MODIFY n BIGINT,LOCK=NONE")
        .unwrap();
    assert_eq!(
        query_rows(&engine, "SELECT id,n FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(3), Value::Int(30)]
        ]
    );
}

fn remove_object_data(path: &Path) {
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            remove_object_data(&path);
        } else {
            std::fs::remove_file(path).unwrap();
        }
    }
}
#[test]
fn copy_file_publication_winner_reconstructs_without_private_or_final_files() {
    for backend in ["memory", "paged"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "copy_file_publication_crash_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("MYDB_COPY_PUBLISH_DIR", dir.path())
            .env("MYDB_COPY_PUBLISH_CRASH", "4")
            .env("MYDB_VECTOR_STORAGE", backend)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        remove_object_data(&dir.path().join("objects"));
        let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", backend)]);
        let engine = open_strict_test_engine(dir.path());
        check(&engine);
        engine.checkpoint().unwrap();
        drop(engine);
        check(&open_strict_test_engine(dir.path()));
    }
}

#[test]
fn copy_file_publication_partitioned_tail_sync_releases_writers() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", backend)]);
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine.execute("CREATE TABLE t(id INT PRIMARY KEY,n INT,b LONGTEXT,v VECTOR(2) NOT NULL,INDEX n(n),INDEX ix(v) USING HNSW) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
        engine.execute("INSERT INTO t VALUES(1,10,'base','[1,1]'),(2,20,'base','[2,2]'),(3,30,'base','[3,3]')").unwrap();
        let tail = engine.database().install_online_copy_publish_test_hook();
        let sync = engine.database().install_copy_file_stage_test_hook(5);
        let worker = engine.clone();
        let ddl =
            std::thread::spawn(move || worker.execute("ALTER TABLE t MODIFY n BIGINT,LOCK=NONE"));
        let reached = tail.wait_until_reached(Duration::from_secs(15));
        if reached {
            writes(&engine);
        }
        tail.release();
        let released = sync.wait_until_reached(Duration::from_secs(15));
        if released {
            let mut s = SessionContext::default();
            s.connection_id = 33410;
            s.sysvars.lock_wait_timeout = Some(0);
            engine
                .execute_with_session(&mut s, "UPDATE t SET n=n+1 WHERE id=3")
                .unwrap();
            engine
                .execute_with_session(&mut s, "UPDATE t SET n=n-1 WHERE id=3")
                .unwrap();
            engine.checkpoint().unwrap();
        }
        sync.release();
        let result = ddl.join().unwrap();
        assert!(reached && released, "{result:?}");
        result.unwrap();
        check(&engine);
        drop(engine);
        check(&open_strict_test_engine(dir.path()));
    }
}

#[test]
fn copy_file_publication_repairs_surviving_btrees_with_unsynced_tails() {
    fn restore_bases(root: &Path, source: &Path) {
        for entry in std::fs::read_dir(source).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                restore_bases(root, &path);
            } else {
                let target = root.join(path.strip_prefix(root.join("saved-index-bases")).unwrap());
                std::fs::copy(path, target).unwrap();
            }
        }
    }
    for parallel in ["0", "1"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "copy_file_publication_crash_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("MYDB_COPY_PUBLISH_DIR", dir.path())
            .env("MYDB_COPY_PUBLISH_CRASH", "4")
            .env("MYDB_COPY_CAPTURE_INDEX_BASE", "1")
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        // Model a power loss that preserves each synced bulk tree while
        // losing its later unsynced writes. Structurally valid stale indexes
        // must be rebuilt, even though the heap already has the decided tail.
        restore_bases(dir.path(), &dir.path().join("saved-index-bases"));
        let _env = HnswEnv::set(&[("MYDB_RECOVERY_PARALLEL", parallel)]);
        let engine = open_strict_test_engine(dir.path());
        check(&engine);
        assert!(query_rows(&engine, "SELECT id FROM t FORCE INDEX(n) WHERE n=10").is_empty());
        engine.checkpoint().unwrap();
        drop(engine);
        check(&open_strict_test_engine(dir.path()));
    }
}

#[test]
fn copy_file_publication_partial_binding_failure_preserves_source_and_retries() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    fixture(&engine);
    let table = engine.database().table("test.t").unwrap();
    let index = table.indexes.iter().find(|i| i.name == "n").unwrap();
    let conflict = crate::storage::naming::index_object_path_by_id(
        dir.path(),
        table.id,
        2,
        index.index_id,
        "idx",
    );
    std::fs::create_dir_all(conflict.parent().unwrap()).unwrap();
    std::fs::write(&conflict, b"injected publication conflict").unwrap();
    assert!(engine
        .execute("ALTER TABLE t MODIFY n BIGINT,LOCK=NONE")
        .is_err());
    assert_eq!(
        engine.database().table("test.t").unwrap().active_generation,
        1
    );
    assert!(
        !crate::storage::naming::table_object_generation_path(dir.path(), table.id, 2, "heap")
            .exists()
    );
    assert!(engine
        .database()
        .copy_publication_floor_for_test()
        .is_none());
    std::fs::remove_file(conflict).unwrap();
    engine
        .execute("ALTER TABLE t MODIFY n BIGINT,LOCK=NONE")
        .unwrap();
    assert_eq!(
        query_rows(&engine, "SELECT id FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
}

#[test]
fn copy_file_publication_manifest_failure_restores_source_catalog() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    fixture(&engine);
    let hook = engine.database().install_copy_file_stage_test_hook(2);
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || worker.execute("ALTER TABLE t MODIFY n BIGINT,LOCK=NONE"));
    let reached = hook.wait_until_reached(Duration::from_secs(15));
    if reached {
        engine.database().inject_vector_persist_failure_for_test(5);
    }
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached);
    assert!(result.is_err());
    assert_eq!(
        engine.database().table("test.t").unwrap().active_generation,
        1
    );
    assert_eq!(
        query_rows(&engine, "SELECT id FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
    assert!(engine
        .database()
        .copy_publication_floor_for_test()
        .is_none());
    engine
        .execute("ALTER TABLE t MODIFY n BIGINT,LOCK=NONE")
        .unwrap();
    drop(engine);
    assert_eq!(
        query_rows(
            &open_strict_test_engine(dir.path()),
            "SELECT id FROM t ORDER BY id"
        ),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
}
