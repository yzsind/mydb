//! Publication barriers exclude only bounded tail work and durable cutover.
use super::*;

fn assert_corpora(engine: &StorageQueryEngine) {
    let db = engine.database();
    let txn = db.begin_read_only_txn().unwrap();
    super::fulltext_incremental::assert_corpora(engine, "test.t", txn);
    db.rollback_txn(txn.txn_id).unwrap();
}

fn result_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(rows) = engine.execute(sql).unwrap().materialize().unwrap() else {
        panic!("expected rows: {sql}");
    };
    rows.rows
}

#[test]
fn fulltext_copy_statistics_replay_shared_corpora_savepoints_and_final_tail() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let expected;
    {
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine.execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,title TEXT,active INT,FULLTEXT f(body),FULLTEXT fp(body) WHERE active=1,FULLTEXT ng(body,title) WITH PARSER ngram)").unwrap();
        engine.execute("INSERT INTO t VALUES(1,'quartz amber','数据库',1),(2,'silver silver','全文检索',0),(3,NULL,NULL,1),(4,'','',0)").unwrap();
        let scan = engine
            .database()
            .install_fulltext_copy_statistics_test_hook();
        let publish = engine.database().install_online_copy_publish_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 28301;
            worker.execute_with_session(&mut session, "OPTIMIZE TABLE t")
        });
        let reached = scan.wait_until_reached(Duration::from_secs(10));
        let mut writer = SessionContext::default();
        writer.connection_id = 28302;
        writer.sysvars.lock_wait_timeout = Some(0);
        let changes =
            (|| {
                for sql in [
                "BEGIN",
                "UPDATE t SET body='cobalt quartz quartz',title='全文数据库',active=0 WHERE id=1",
                "DELETE FROM t WHERE id=2",
                "SAVEPOINT retained",
                "INSERT INTO t VALUES(5,'discarded token','回滚',1)",
                "UPDATE t SET body='discarded update' WHERE id=3",
                "ROLLBACK TO SAVEPOINT retained",
                "INSERT INTO t VALUES(6,'quartz zircon','数据库数据库',1)",
                "COMMIT",
            ] { engine.execute_with_session(&mut writer, sql)?; }
                Ok::<_, QueryError>(())
            })();
        scan.release();
        if changes.is_err() {
            publish.release();
        }
        let publishing = publish.wait_until_reached(Duration::from_secs(10));
        let tail = (|| {
            engine.execute_with_session(
                &mut writer,
                "UPDATE t SET body='final quartz',title='全文全文' WHERE id=6",
            )?;
            engine.execute_with_session(
                &mut writer,
                "INSERT INTO t VALUES(7,'amber quartz','检索',0)",
            )?;
            engine.execute_with_session(&mut writer, "DELETE FROM t WHERE id=4")?;
            Ok::<_, QueryError>(())
        })();
        publish.release();
        let result = ddl.join().unwrap();
        assert!(
            reached && publishing,
            "copy did not reach both phases: {result:?}"
        );
        changes.unwrap();
        tail.unwrap();
        result.unwrap();
        assert_corpora(&engine);
        expected = result_rows(
            &engine,
            "SELECT id,MATCH(body) AGAINST('quartz') FROM t ORDER BY id",
        );
    }
    let engine = open_strict_test_engine(dir.path());
    assert_corpora(&engine);
    assert_eq!(
        result_rows(
            &engine,
            "SELECT id,MATCH(body) AGAINST('quartz') FROM t ORDER BY id"
        ),
        expected
    );
}

#[test]
fn fulltext_copy_statistics_cover_offline_partitioned_and_changed_columns() {
    for partition in ["", "PARTITION BY HASH(id) PARTITIONS 3"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        {
            let engine = open_strict_test_engine(dir.path());
            engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,title TEXT,FULLTEXT f(body),FULLTEXT ng(body,title) WITH PARSER ngram) {partition}")).unwrap();
            engine.execute("INSERT INTO t VALUES(1,'quartz quartz','数据库'),(2,'amber','全文检索'),(3,NULL,NULL)").unwrap();
            engine
                .execute(
                    "ALTER TABLE t ADD COLUMN extra INT DEFAULT 7, ALGORITHM=COPY, LOCK=EXCLUSIVE",
                )
                .unwrap();
            assert_corpora(&engine);
            engine
                .execute("ALTER TABLE t MODIFY body LONGTEXT, ALGORITHM=COPY, LOCK=EXCLUSIVE")
                .unwrap();
            assert_corpora(&engine);
        }
        let engine = open_strict_test_engine(dir.path());
        assert_corpora(&engine);
    }
}

#[test]
fn retired_table_cleanup_preserves_same_name_replacements_and_rejects_stale_writers() {
    for partition in ["", "PARTITION BY HASH(id) PARTITIONS 3"] {
        for operation in [
            "TRUNCATE TABLE cleanup.t",
            "DROP TABLE cleanup.t",
            "DROP DATABASE cleanup",
        ] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = Arc::new(open_strict_test_engine(dir.path()));
            let db = engine.database();
            db.set_recyclebin_variable("mydb_recyclebin", 0).unwrap();
            engine.execute("CREATE DATABASE cleanup").unwrap();
            let create = format!(
                "CREATE TABLE cleanup.t(id INT PRIMARY KEY,v LONGTEXT,KEY vi(v(12))) {partition}"
            );
            engine.execute(&create).unwrap();
            engine
                .execute("INSERT INTO cleanup.t VALUES(1,REPEAT('retired ',10000))")
                .unwrap();
            let old = db.table("cleanup.t").unwrap();
            let read = db.begin_read_only_txn().unwrap();
            let old_row = db
                .scan_rows_with_storage("cleanup.t", read)
                .unwrap()
                .remove(0);
            db.rollback_txn(read.txn_id).unwrap();
            let hook = db.install_retirement_cleanup_test_hook();
            engine.execute(operation).unwrap();
            assert_eq!(db.catalog_retirement_count_for_test(), 1, "{operation}");
            if operation.starts_with("DROP DATABASE") {
                engine.execute("CREATE DATABASE cleanup").unwrap();
            }
            if operation.starts_with("DROP") {
                engine.execute(&create).unwrap();
            }
            let new = db.table("cleanup.t").unwrap();
            assert_ne!(new.id, old.id);
            let stale = db.begin_txn().unwrap();
            let stale_write = db.lock_rows_for_update(
                stale,
                &old,
                &[crate::storage::DmlRowRef {
                    storage_id: old_row.0,
                    row_id: old_row.1.row_id,
                }],
            );
            db.rollback_txn(stale.txn_id).unwrap();
            assert_eq!(stale_write.unwrap_err().code(), 1412, "{operation}");
            let worker = engine.clone();
            let cleanup = std::thread::spawn(move || {
                worker
                    .database()
                    .maintain_catalog_retirements_until(|| false)
            });
            let reached = hook.wait_until_reached(Duration::from_secs(10));
            let mut writer = SessionContext::default();
            writer.connection_id = 28401;
            writer.sysvars.lock_wait_timeout = Some(0);
            let write = engine.execute_with_session(
                &mut writer,
                "INSERT INTO cleanup.t VALUES(2,REPEAT('replacement ',10000))",
            );
            let read = engine
                .execute_with_session(
                    &mut writer,
                    "SELECT id FROM cleanup.t FORCE INDEX(vi) WHERE v LIKE 'replacement%'",
                )
                .and_then(QueryResponse::materialize);
            hook.release();
            cleanup.join().unwrap().unwrap();
            assert!(reached, "cleanup was not reached: {operation}");
            write.unwrap();
            read.unwrap();
            assert_eq!(
                result_rows(&engine, "SELECT id,LENGTH(v) FROM cleanup.t"),
                vec![vec![Value::Int(2), Value::Int(120000)]]
            );
            drop(engine);
            let engine = open_strict_test_engine(dir.path());
            assert_eq!(
                result_rows(&engine, "SELECT id,LENGTH(v) FROM cleanup.t"),
                vec![vec![Value::Int(2), Value::Int(120000)]]
            );
        }
    }
}

#[test]
fn ddl_publication_crash_child() {
    let Ok(phase) = std::env::var("MYDB_DDL_PUBLICATION_CRASH_PHASE") else {
        return;
    };
    let directory = std::env::var("MYDB_DDL_PUBLICATION_CRASH_DIR").unwrap();
    let engine = Arc::new(open_strict_test_engine(std::path::Path::new(&directory)));
    engine
        .database()
        .set_recyclebin_variable("mydb_recyclebin", 0)
        .unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,FULLTEXT f(body))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'quartz amber'),(2,'silver quartz')")
        .unwrap();
    engine.checkpoint().unwrap();
    if phase.starts_with("fulltext") {
        let hook = if phase == "fulltext_before_publish" {
            engine.database().install_online_copy_publish_test_hook()
        } else {
            engine.database().install_vector_copy_commit_test_hook()
        };
        let worker = engine.clone();
        std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 28501;
            worker
                .execute_with_session(&mut session, "OPTIMIZE TABLE t")
                .unwrap();
        });
        assert!(hook.wait_until_reached(Duration::from_secs(10)));
        if phase == "fulltext_before_publish" {
            let mut writer = SessionContext::default();
            writer.connection_id = 28502;
            engine
                .execute_with_session(
                    &mut writer,
                    "UPDATE t SET body='committed quartz' WHERE id=1",
                )
                .unwrap();
        }
    } else {
        let truncate = phase.starts_with("truncate");
        engine
            .execute(if truncate {
                "TRUNCATE TABLE t"
            } else {
                "DROP TABLE t"
            })
            .unwrap();
        if !truncate {
            engine
                .execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,FULLTEXT f(body))")
                .unwrap();
        }
        engine
            .execute("INSERT INTO t VALUES(3,'replacement zircon')")
            .unwrap();
        if phase.ends_with("mid_cleanup") {
            // FileDelete is durable before this injected abrupt exit.
            std::env::set_var("MYDB_RECYCLEBIN_TEST_CRASH", "after_file_delete");
            engine
                .database()
                .maintain_catalog_retirements_until(|| false)
                .unwrap();
            panic!("cleanup crash point was not reached");
        }
    }
    // Exit without destructors/checkpoint to exercise real WAL recovery.
    std::process::exit(86);
}

#[test]
fn ddl_publication_recovers_unpublished_fulltext_and_deferred_retirements() {
    for phase in [
        "fulltext_before_publish",
        "fulltext_after_publish",
        "truncate_before_cleanup",
        "truncate_mid_cleanup",
        "drop_before_cleanup",
        "drop_mid_cleanup",
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::executor::tests::ddl_publish_lock::ddl_publication_crash_child",
                "--nocapture",
            ])
            .env("MYDB_DDL_PUBLICATION_CRASH_PHASE", phase)
            .env("MYDB_DDL_PUBLICATION_CRASH_DIR", dir.path())
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{phase}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let engine = open_strict_test_engine(dir.path());
        assert_corpora(&engine);
        let expected = if phase == "fulltext_before_publish" {
            vec![
                vec![Value::Int(1), Value::String("committed quartz".into())],
                vec![Value::Int(2), Value::String("silver quartz".into())],
            ]
        } else if phase == "fulltext_after_publish" {
            vec![
                vec![Value::Int(1), Value::String("quartz amber".into())],
                vec![Value::Int(2), Value::String("silver quartz".into())],
            ]
        } else {
            vec![vec![
                Value::Int(3),
                Value::String("replacement zircon".into()),
            ]]
        };
        assert_eq!(
            result_rows(&engine, "SELECT id,body FROM t ORDER BY id"),
            expected,
            "{phase}"
        );
        engine
            .execute("UPDATE t SET body=CONCAT(body,' cobalt')")
            .unwrap();
        assert_corpora(&engine);
    }
}

#[test]
fn retirement_worker_wakes_without_waiting_for_zone_map_maintenance() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    engine
        .database()
        .set_recyclebin_variable("mydb_recyclebin", 0)
        .unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1)").unwrap();
    let hook = engine.database().install_retirement_cleanup_test_hook();
    engine.execute("DROP TABLE t").unwrap();
    let reached = hook.wait_until_reached(Duration::from_secs(3));
    let replacement = engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .and_then(|_| engine.execute("INSERT INTO t VALUES(2)"));
    hook.release();
    assert!(
        reached,
        "retirement worker slept through the commit notification"
    );
    replacement.unwrap();
    assert_eq!(
        result_rows(&engine, "SELECT id FROM t"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn retired_special_index_caches_do_not_affect_replacement_vectors_or_lobs() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine
            .database()
            .set_recyclebin_variable("mydb_recyclebin", 0)
            .unwrap();
        engine.execute("CREATE TABLE t(id INT PRIMARY KEY,body LONGTEXT,g GEOMETRY NOT NULL,emb VECTOR(3) NOT NULL,FULLTEXT f(body),SPATIAL sp(g))").unwrap();
        engine
            .execute("CREATE INDEX ve ON t(emb) USING HNSW")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,REPEAT('retired quartz ',3000),ST_GeomFromText('POINT(1 1)'),'[1,2,3]')").unwrap();
        engine.database().vector_graph_warm_all_for_test();
        engine.execute("TRUNCATE TABLE t").unwrap();
        let hook = engine.database().install_retirement_cleanup_test_hook();
        let worker = engine.clone();
        let cleanup = std::thread::spawn(move || {
            worker
                .database()
                .maintain_catalog_retirements_until(|| false)
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let write = engine.execute("INSERT INTO t VALUES(2,REPEAT('replacement zircon ',3000),ST_GeomFromText('POINT(2 2)'),'[3,2,1]')");
        let query = engine.execute("SELECT id FROM t WHERE MATCH(body) AGAINST('zircon') ORDER BY DISTANCE(emb,STRING_TO_VECTOR('[3,2,1]'),'EUCLIDEAN') LIMIT 1").and_then(QueryResponse::materialize);
        hook.release();
        cleanup.join().unwrap().unwrap();
        assert!(reached);
        write.unwrap();
        query.unwrap();
        assert_corpora(&engine);
        assert_eq!(
            result_rows(
                &engine,
                "SELECT id,LENGTH(body),ST_AsText(g),VECTOR_TO_STRING(emb) FROM t"
            ),
            vec![vec![
                Value::Int(2),
                Value::Int(57000),
                Value::String("POINT(2 2)".into()),
                Value::String("[3,2,1]".into())
            ]]
        );
    }
    let engine = open_strict_test_engine(dir.path());
    assert_corpora(&engine);
    assert_eq!(result_rows(&engine, "SELECT id FROM t ORDER BY DISTANCE(emb,STRING_TO_VECTOR('[3,2,1]'),'EUCLIDEAN') LIMIT 1"), vec![vec![Value::Int(2)]]);
}

#[test]
fn yielded_retirement_pass_resumes_after_the_last_visited_object() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .database()
        .set_recyclebin_variable("mydb_recyclebin", 0)
        .unwrap();
    for name in ["a", "b", "c"] {
        engine
            .execute(&format!("CREATE TABLE {name}(id INT PRIMARY KEY)"))
            .unwrap();
    }
    engine.execute("DROP TABLE a,b,c").unwrap();
    let calls = std::cell::Cell::new(0);
    let first = engine
        .database()
        .maintain_catalog_retirements_until(|| {
            calls.set(calls.get() + 1);
            calls.get() >= 3
        })
        .unwrap();
    assert_eq!(first, 1);
    assert_eq!(
        engine
            .database()
            .maintain_catalog_retirements_until(|| false)
            .unwrap(),
        2
    );
    assert_eq!(engine.database().catalog_retirement_count_for_test(), 0);
}

#[test]
fn fulltext_final_tail_budgets_deleted_documents_and_rolls_back_private_counters() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,body LONGTEXT,FULLTEXT f(body))")
        .unwrap();
    let body = "quartz ".repeat(1500);
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..512).map(|i| vec![Value::Int(i), Value::String(body.clone())]),
    );
    let original = engine.database().table("test.t").unwrap().active_generation;
    let hook = engine.database().install_online_copy_publish_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 28601;
        worker.execute_with_session(&mut session, "OPTIMIZE TABLE t")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(15));
    let mut writer = SessionContext::default();
    writer.connection_id = 28602;
    let deletion = engine.execute_with_session(&mut writer, "DELETE FROM t");
    hook.release();
    let response = ddl.join().unwrap();
    assert!(
        reached,
        "copy did not reach the publication boundary: {response:?}"
    );
    deletion.unwrap();
    let QueryResponse::ResultSet(result) = response.unwrap().materialize().unwrap() else {
        panic!("maintenance result")
    };
    assert!(
        result.rows.iter().any(|row| row
            .iter()
            .any(|cell| cell.to_display_string().contains("final tail exceeded"))),
        "{:?}",
        result.rows
    );
    assert_eq!(
        engine.database().table("test.t").unwrap().active_generation,
        original
    );
    assert_corpora(&engine);
    assert_eq!(
        result_rows(&engine, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(0)]]
    );
    engine
        .execute("INSERT INTO t VALUES(1000,'replacement cobalt')")
        .unwrap();
    assert_corpora(&engine);
}

#[test]
fn fulltext_copy_statistics_scan_admits_concurrent_writes() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'alpha bravo'),(2,'charlie delta')")
        .unwrap();
    let hook = engine
        .database()
        .install_fulltext_copy_statistics_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 28101;
        worker.execute_with_session(&mut session, "OPTIMIZE TABLE t")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 28102;
    writer.sysvars.lock_wait_timeout = Some(0);
    let write =
        engine.execute_with_session(&mut writer, "UPDATE t SET body='alpha echo' WHERE id=1");
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "statistics scan was not reached: {result:?}");
    result.unwrap();
    write.expect("the full statistics scan must run outside the publication barrier");
}

#[test]
fn truncate_returns_before_retired_file_cleanup() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine
        .database()
        .set_recyclebin_variable("mydb_recyclebin", 0)
        .unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v TEXT,KEY vi(v(12)))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'retired value')")
        .unwrap();
    let hook = engine.database().install_retirement_cleanup_test_hook();
    let worker = engine.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 28201;
        tx.send(worker.execute_with_session(&mut session, "TRUNCATE TABLE t"))
            .unwrap();
    });
    let completed = rx.recv_timeout(Duration::from_secs(1));
    if completed.is_err() {
        hook.release();
        ddl.join().unwrap();
        panic!("TRUNCATE waited for retired file cleanup while holding statement MDL");
    }
    completed.unwrap().unwrap();
    ddl.join().unwrap();
    let worker = engine.clone();
    let cleanup = std::thread::spawn(move || {
        worker
            .database()
            .maintain_catalog_retirements_until(|| false)
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 28202;
    writer.sysvars.lock_wait_timeout = Some(0);
    let write =
        engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(2,'replacement value')");
    hook.release();
    cleanup.join().unwrap().unwrap();
    assert!(reached, "background retirement cleanup was not reached");
    write.expect("replacement writes must proceed during retired file cleanup");
}

#[test]
#[ignore = "local publication-lock scaling measurement"]
fn fulltext_publication_lock_scaling_probe() {
    for count in [256, 4096] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine
            .database()
            .set_recyclebin_variable("mydb_recyclebin", 0)
            .unwrap();
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body))")
            .unwrap();
        let body = "quartz amber cobalt zircon copper silver violet indigo ".repeat(8);
        insert_fixture_rows(
            &engine,
            "test.t",
            (0..count).map(|i| vec![Value::Int(i), Value::String(body.clone())]),
        );
        engine.checkpoint().unwrap();
        let before = engine.database().ddl_copy_perf_snapshot();
        let started = Instant::now();
        engine.execute("OPTIMIZE TABLE t").unwrap();
        let elapsed = started.elapsed().as_micros();
        let after = engine.database().ddl_copy_perf_snapshot();
        let truncate = Instant::now();
        engine.execute("TRUNCATE TABLE t").unwrap();
        eprintln!("DDL_PUBLICATION_PROBE rows={count} optimize_us={elapsed} hold_us={} tail_rows={} truncate_us={}",
            after[22] - before[22], after[24] - before[24], truncate.elapsed().as_micros());
    }
}
