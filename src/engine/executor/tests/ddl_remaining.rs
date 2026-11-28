//! Remaining publication-lock work, verified against ordinary DML and recovery.
use super::*;

fn counter_count(engine: &StorageQueryEngine, parts: Vec<crate::storage::KeyPart>) -> usize {
    let db = engine.database();
    let table = crate::storage::data_dictionary::dd_table_schema(
        crate::storage::data_dictionary::DD_FULLTEXT_COUNTERS,
    )
    .unwrap();
    let read = db.begin_read_only_txn().unwrap();
    let count = db
        .prefix_index_entries_visible(
            &table,
            &table.indexes[0],
            crate::storage::IndexKey::from(parts),
            read,
        )
        .unwrap()
        .len();
    db.rollback_txn(read.txn_id).unwrap();
    count
}

fn leaf_fixture(engine: &StorageQueryEngine, count: i64) {
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,FULLTEXT ft(body),FULLTEXT fp(body) WHERE id>0) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(20000),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    insert_fixture_rows(
        engine,
        "test.t",
        (0..count).map(|id| vec![Value::Int(id), Value::String(format!("quartz leaf{id}"))]),
    );
    engine
        .execute("INSERT INTO t VALUES(30000,'retained amber')")
        .unwrap();
}

#[test]
fn ddl_leaf_counters_are_deferred() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(dir.path());
    leaf_fixture(&engine, 800);
    let table = engine.database().table("test.t").unwrap();
    let key = vec![crate::storage::KeyPart::UInt(table.id)];
    let before = counter_count(&engine, key.clone());
    engine.execute("ALTER TABLE t DROP PARTITION p0").unwrap();
    assert_eq!(
        counter_count(&engine, key),
        before,
        "partition publication must defer counter deletion"
    );
}

#[test]
#[ignore = "local remaining-DDL lock/latency comparison"]
fn ddl_remaining_lock_probe() {
    for count in [1000, 10000] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = open_strict_test_engine(dir.path());
        leaf_fixture(&engine, count);
        engine.checkpoint().unwrap();
        let before = engine.database().fulltext_drop_hold_us_for_test();
        let started = Instant::now();
        engine.execute("ALTER TABLE t DROP PARTITION p0").unwrap();
        eprintln!(
            "DDL_LEAF_PROBE rows={count} elapsed_us={} hold_us={}",
            started.elapsed().as_micros(),
            engine.database().fulltext_drop_hold_us_for_test() - before
        );
    }
}

fn drain(engine: &StorageQueryEngine) {
    for _ in 0..128 {
        if engine.database().catalog_retirement_count_for_test() == 0 {
            return;
        }
        engine
            .database()
            .maintain_catalog_retirements_until(|| false)
            .unwrap();
    }
    panic!("retirement queue did not drain");
}

fn assert_current(engine: &StorageQueryEngine) {
    let read = engine.database().begin_read_only_txn().unwrap();
    super::fulltext_incremental::assert_corpora(engine, "test.t", read);
    engine.database().rollback_txn(read.txn_id).unwrap();
}

#[test]
fn ddl_leaf_retirement_preserves_survivors_snapshots_and_root_reuse() {
    for operation in [
        "DROP PARTITION p0",
        "TRUNCATE PARTITION p0",
        "REBUILD PARTITION p0",
    ] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        leaf_fixture(&engine, 800);
        let old = engine.database().table("test.t").unwrap();
        let old_index = old.indexes.iter().find(|i| i.name == "ft").unwrap();
        let snapshot = engine.database().begin_read_only_txn().unwrap();
        let frozen = engine
            .database()
            .fulltext_corpus_statistics(snapshot, &old, old_index, &["quartz".into()], None)
            .unwrap();
        engine
            .execute(&format!("ALTER TABLE t {operation}"))
            .unwrap();
        let before = counter_count(&engine, vec![crate::storage::KeyPart::UInt(old.id)]);
        let hook = engine.database().install_retirement_cleanup_test_hook();
        let worker = engine.clone();
        let cleanup = std::thread::spawn(move || {
            worker
                .database()
                .maintain_catalog_retirements_until(|| false)
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let mut writer = SessionContext::default();
        writer.connection_id = 31801;
        writer.sysvars.lock_wait_timeout = Some(0);
        let result = engine.execute_with_session(
            &mut writer,
            "UPDATE t SET body='surviving amber' WHERE id=30000",
        );
        hook.release();
        cleanup.join().unwrap().unwrap();
        assert!(reached, "{operation}");
        result.unwrap();
        let after = counter_count(&engine, vec![crate::storage::KeyPart::UInt(old.id)]);
        assert!(
            after < before && before - after <= 256,
            "{operation}: {before}->{after}"
        );
        assert_current(&engine);
        drain(&engine);
        let retained = engine
            .database()
            .fulltext_corpus_statistics(snapshot, &old, old_index, &["quartz".into()], None)
            .unwrap();
        assert_eq!(retained.documents, frozen.documents);
        assert_eq!(retained.frequencies, frozen.frequencies);
        engine.database().rollback_txn(snapshot.txn_id).unwrap();
        // Reuse the logical root twice while the first root retirement remains pending.
        engine.execute("ALTER TABLE t REMOVE PARTITIONING").unwrap();
        engine
            .execute("ALTER TABLE t PARTITION BY HASH(id) PARTITIONS 2")
            .unwrap();
        engine.execute("ALTER TABLE t REMOVE PARTITIONING").unwrap();
        engine
            .execute("INSERT INTO t VALUES(40000,'reused root quartz')")
            .unwrap();
        assert_current(&engine);
        drain(&engine);
        assert_current(&engine);
        engine.checkpoint().unwrap();
        drop(engine);
        let engine = open_strict_test_engine(dir.path());
        drain(&engine);
        assert_current(&engine);
    }
}

#[test]
fn ddl_leaf_retirement_handles_subpartitions_and_shared_corpora() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,FULLTEXT ft(body),FULLTEXT alias(body)) PARTITION BY RANGE(id) SUBPARTITION BY HASH(id) SUBPARTITIONS 2(PARTITION p0 VALUES LESS THAN(1000),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..1200).map(|id| {
            vec![
                Value::Int(id),
                Value::String(format!("quartz document{id}")),
            ]
        }),
    );
    engine
        .execute("ALTER TABLE t TRUNCATE PARTITION p0")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(3,'new leaf quartz')")
        .unwrap();
    drain(&engine);
    assert_current(&engine);
    engine.execute("DROP INDEX ft ON t").unwrap();
    drain(&engine);
    assert_current(&engine);
}

#[test]
fn ddl_leaf_retirement_crash_child() {
    let Ok(phase) = std::env::var("MYDB_LEAF_CRASH_PHASE") else {
        return;
    };
    let directory = std::env::var("MYDB_LEAF_CRASH_DIR").unwrap();
    let engine = open_strict_test_engine(std::path::Path::new(&directory));
    leaf_fixture(&engine, 800);
    engine.checkpoint().unwrap();
    if phase == "before" {
        engine
            .database()
            .set_fulltext_retirement_crash_point_for_test(1);
    }
    if phase == "after" {
        engine
            .database()
            .set_fulltext_retirement_crash_point_for_test(2);
    }
    engine
        .execute("ALTER TABLE t TRUNCATE PARTITION p0")
        .unwrap();
    if phase == "batch" {
        engine
            .database()
            .set_fulltext_retirement_crash_point_for_test(3);
    }
    engine
        .database()
        .maintain_catalog_retirements_until(|| false)
        .unwrap();
    assert!(engine.database().catalog_retirement_count_for_test() > 0);
    std::process::exit(86);
}

#[test]
fn ddl_leaf_retirement_recovers_ddl_and_partial_batches() {
    for phase in ["before", "after", "batch", "resume"] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::executor::tests::ddl_remaining::ddl_leaf_retirement_crash_child",
                "--nocapture",
            ])
            .env("MYDB_LEAF_CRASH_PHASE", phase)
            .env("MYDB_LEAF_CRASH_DIR", dir.path())
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{phase}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let engine = open_strict_test_engine(dir.path());
        let QueryResponse::ResultSet(result) = engine
            .execute("SELECT COUNT(*) FROM t")
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(
            result.rows,
            vec![vec![Value::Int(if phase == "before" { 801 } else { 1 })]],
            "{phase}"
        );
        engine
            .execute("INSERT INTO t VALUES(9000,'replacement quartz')")
            .unwrap();
        drain(&engine);
        assert_current(&engine);
    }
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("{sql}")
    };
    result.rows
}

#[test]
fn ddl_primary_copy_replays_key_swaps_rollbacks_and_tail() {
    for partition in ["", " PARTITION BY HASH(id) PARTITIONS 3"] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,k INT NOT NULL,v VARCHAR(20),UNIQUE uk(k,id)){partition}")).unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,10,'one'),(2,20,'two'),(3,30,'three')")
            .unwrap();
        let hook = engine.database().install_ddl_backfill_test_hook();
        let publish = engine.database().install_online_copy_publish_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 31811;
            worker.execute_with_session(
                &mut session,
                "ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY(k,id), MODIFY v TEXT, LOCK=NONE",
            )
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let mut writer = SessionContext::default();
        writer.connection_id = 31812;
        writer.sysvars.lock_wait_timeout = Some(0);
        let changes = (|| -> Result<(), QueryError> {
            for sql in [
                "BEGIN",
                "UPDATE t SET k=99 WHERE id=1",
                "UPDATE t SET k=10 WHERE id=2",
                "UPDATE t SET k=20 WHERE id=1",
                "SAVEPOINT s",
                "DELETE FROM t WHERE id=3",
                "ROLLBACK TO s",
                "INSERT INTO t VALUES(4,40,'four')",
                "COMMIT",
            ] {
                engine.execute_with_session(&mut writer, sql)?;
            }
            Ok(())
        })();
        hook.release();
        let publishing = publish.wait_until_reached(Duration::from_secs(10));
        let tail = engine.execute_with_session(&mut writer, "UPDATE t SET v='tail' WHERE id=4");
        publish.release();
        let result = ddl.join().unwrap();
        assert!(reached && publishing, "{result:?}");
        changes.unwrap();
        tail.unwrap();
        result.unwrap();
        assert_eq!(
            rows(&engine, "SELECT id,k,v FROM t ORDER BY id"),
            vec![
                vec![Value::Int(1), Value::Int(20), Value::String("one".into())],
                vec![Value::Int(2), Value::Int(10), Value::String("two".into())],
                vec![Value::Int(3), Value::Int(30), Value::String("three".into())],
                vec![Value::Int(4), Value::Int(40), Value::String("tail".into())]
            ]
        );
        engine
            .execute("INSERT INTO t VALUES(4,40,'duplicate')")
            .unwrap_err();
        let table = engine.database().table("test.t").unwrap();
        assert_eq!(table.primary_key, vec!["k", "id"]);
        drop(engine);
        let engine = open_strict_test_engine(dir.path());
        assert_eq!(
            rows(&engine, "SELECT v FROM t WHERE k=40 AND id=4"),
            vec![vec![Value::String("tail".into())]]
        );
    }
}

#[test]
fn ddl_primary_copy_failure_keeps_source_and_contracts() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,k INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,7),(2,7),(3,NULL)")
        .unwrap();
    let before = engine.database().table("test.t").unwrap();
    for sql in [
        "ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY(k), LOCK=NONE",
        "ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY(k), ALGORITHM=INSTANT",
    ] {
        engine.execute(sql).unwrap_err();
        let after = engine.database().table("test.t").unwrap();
        assert_eq!(before.primary_key, after.primary_key);
        assert_eq!(before.active_generation, after.active_generation);
    }
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(3)]]
    );
    engine
        .execute("CREATE TABLE ai(id INT AUTO_INCREMENT PRIMARY KEY,v INT)")
        .unwrap();
    engine
        .execute("ALTER TABLE ai DROP PRIMARY KEY, ALGORITHM=COPY, LOCK=NONE")
        .unwrap_err();
    engine
        .execute("CREATE TABLE parent(id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE child(id INT,FOREIGN KEY(id) REFERENCES parent(id))")
        .unwrap();
    engine
        .execute("ALTER TABLE parent DROP PRIMARY KEY, ALGORITHM=COPY, LOCK=NONE")
        .unwrap_err();
}

#[test]
fn ddl_primary_copy_crash_child() {
    let Ok(phase) = std::env::var("MYDB_PRIMARY_CRASH_PHASE") else {
        return;
    };
    let directory = std::env::var("MYDB_PRIMARY_CRASH_DIR").unwrap();
    let engine = Arc::new(open_strict_test_engine(std::path::Path::new(&directory)));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,k INT NOT NULL)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    engine.checkpoint().unwrap();
    let hook = if phase == "before" {
        engine.database().install_online_copy_publish_test_hook()
    } else {
        engine.database().install_vector_copy_commit_test_hook()
    };
    let worker = engine.clone();
    let _ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 31821;
        worker
            .execute_with_session(
                &mut session,
                "ALTER TABLE t DROP PRIMARY KEY,ADD PRIMARY KEY(k),LOCK=NONE",
            )
            .unwrap();
    });
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    if phase == "before" {
        engine.execute("UPDATE t SET k=11 WHERE id=1").unwrap();
    }
    std::process::exit(86);
}

#[test]
fn ddl_primary_copy_recovers_atomic_publication() {
    for phase in ["before", "after"] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::executor::tests::ddl_remaining::ddl_primary_copy_crash_child",
                "--nocapture",
            ])
            .env("MYDB_PRIMARY_CRASH_PHASE", phase)
            .env("MYDB_PRIMARY_CRASH_DIR", dir.path())
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{phase}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let engine = open_strict_test_engine(dir.path());
        let table = engine.database().table("test.t").unwrap();
        assert_eq!(
            table.primary_key,
            vec![if phase == "before" { "id" } else { "k" }]
        );
        assert_eq!(
            rows(&engine, "SELECT k FROM t WHERE id=1"),
            vec![vec![Value::Int(if phase == "before" { 11 } else { 10 })]]
        );
        engine.execute("INSERT INTO t VALUES(3,30)").unwrap();
    }
}

fn assert_indexes(engine: &StorageQueryEngine) {
    let db = engine.database();
    let table = db.table("test.t").unwrap();
    let read = db.begin_read_only_txn().unwrap();
    for sid in table.storage_ids() {
        let source = db
            .scan_rows_storage_range(&table, sid, read, 0, u64::MAX)
            .unwrap();
        for index in &table.indexes {
            let mut expected = source
                .iter()
                .flat_map(|r| {
                    table
                        .index_keys(index, &r.values)
                        .unwrap()
                        .into_iter()
                        .map(move |key| (key, r.row_id))
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
            assert_eq!(actual, expected, "{}", index.name);
        }
    }
    db.rollback_txn(read.txn_id).unwrap();
}

#[test]
fn ddl_special_index_builds_allow_dml_and_capture_rollback_and_final_tail() {
    for partition in ["", " PARTITION BY HASH(id) PARTITIONS 3"] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,title TEXT,active INT,g GEOMETRY NOT NULL){partition}")).unwrap();
        engine.execute("INSERT INTO t VALUES(1,'quartz amber','数据库',1,ST_GeomFromText('POINT(1 1)')),(2,'silver quartz','全文',0,ST_GeomFromText('POINT(2 2)'))").unwrap();
        for (number,sql) in ["CREATE SPATIAL INDEX sp ON t(g) LOCK=NONE","ALTER TABLE t ADD FULLTEXT ft(body), ADD FULLTEXT alias(body) WHERE active=1, ADD FULLTEXT ng(body,title) WITH PARSER ngram, LOCK=NONE"].into_iter().enumerate() {
            let hook=engine.database().install_ddl_backfill_test_hook();
            let validation=engine.database().install_online_index_validation_test_hook();
            let worker=engine.clone();
            let ddl=std::thread::spawn(move||{let mut session=SessionContext::default();session.connection_id=31831;worker.execute_with_session(&mut session,sql)});
            let reached=hook.wait_until_reached(Duration::from_secs(10));
            let mut writer=SessionContext::default();writer.connection_id=31832;writer.sysvars.lock_wait_timeout=Some(0);
            let changes=(|| -> Result<(),QueryError>{
                for sql in ["BEGIN","UPDATE t SET body='committed quartz',g=ST_GeomFromText('POINT(3 3)') WHERE id=1","SAVEPOINT s","DELETE FROM t WHERE id=2","INSERT INTO t VALUES(4,'rolled back','回滚',1,ST_GeomFromText('POINT(4 4)'))","ROLLBACK TO s","COMMIT"] {engine.execute_with_session(&mut writer,sql)?;} Ok(())
            })();
            hook.release();
            let validating=validation.wait_until_reached(Duration::from_secs(10));
            let tail=engine.execute_with_session(&mut writer,&format!("INSERT INTO t VALUES({},'tail quartz','检索',1,ST_GeomFromText('POINT(5 5)'))",10+number));
            validation.release();
            let result=ddl.join().unwrap();assert!(reached && validating,"{result:?}");changes.unwrap();tail.unwrap();result.unwrap();
            assert_indexes(&engine);
        }
        assert_current(&engine);
        engine
            .execute("CREATE FULLTEXT INDEX ft2 ON t(body) LOCK=NONE")
            .unwrap();
        assert_current(&engine);
        drop(engine);
        let engine = open_strict_test_engine(dir.path());
        assert_indexes(&engine);
        assert_current(&engine);
    }
}

#[test]
fn ddl_special_index_crash_child() {
    let Ok(phase) = std::env::var("MYDB_SPECIAL_INDEX_PHASE") else {
        return;
    };
    let kind = std::env::var("MYDB_SPECIAL_INDEX_KIND").unwrap();
    let directory = std::env::var("MYDB_SPECIAL_INDEX_DIR").unwrap();
    let engine = Arc::new(open_strict_test_engine(std::path::Path::new(&directory)));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,g GEOMETRY NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'quartz amber',ST_GeomFromText('POINT(1 1)'))")
        .unwrap();
    engine.checkpoint().unwrap();
    if phase == "before" {
        let hook = engine
            .database()
            .install_online_index_validation_test_hook();
        let worker = engine.clone();
        let _ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 31841;
            worker
                .execute_with_session(
                    &mut session,
                    &format!(
                        "CREATE {kind} INDEX f ON t({}) LOCK=NONE",
                        if kind == "FULLTEXT" { "body" } else { "g" }
                    ),
                )
                .unwrap();
        });
        assert!(hook.wait_until_reached(Duration::from_secs(10)));
        engine
            .execute("INSERT INTO t VALUES(2,'committed quartz',ST_GeomFromText('POINT(2 2)'))")
            .unwrap();
    } else {
        engine
            .execute(&format!(
                "CREATE {kind} INDEX f ON t({}) LOCK=NONE",
                if kind == "FULLTEXT" { "body" } else { "g" }
            ))
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES(2,'committed quartz',ST_GeomFromText('POINT(2 2)'))")
            .unwrap();
    }
    std::process::exit(86);
}

#[test]
fn ddl_special_index_recovers_unpublished_and_committed_builds() {
    for kind in ["FULLTEXT", "SPATIAL"] {
        for phase in ["before", "after"] {
            let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "engine::executor::tests::ddl_remaining::ddl_special_index_crash_child",
                    "--nocapture",
                ])
                .env("MYDB_SPECIAL_INDEX_KIND", kind)
                .env("MYDB_SPECIAL_INDEX_PHASE", phase)
                .env("MYDB_SPECIAL_INDEX_DIR", dir.path())
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(86),
                "{kind}/{phase}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let engine = open_strict_test_engine(dir.path());
            let table = engine.database().table("test.t").unwrap();
            assert_eq!(
                table.indexes.iter().any(|i| i.name == "f"),
                phase == "after"
            );
            assert_eq!(
                rows(&engine, "SELECT COUNT(*) FROM t"),
                vec![vec![Value::Int(2)]]
            );
            if phase == "before" {
                engine
                    .execute(&format!(
                        "CREATE {kind} INDEX f ON t({}) LOCK=NONE",
                        if kind == "FULLTEXT" { "body" } else { "g" }
                    ))
                    .unwrap();
            }
            assert_indexes(&engine);
            assert_current(&engine);
        }
    }
}

#[test]
#[ignore = "local strict-durability DDL exclusion and ordinary DML comparison"]
fn ddl_next_phase_performance_probe() {
    for count in [1000, 10000] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE control(id INT PRIMARY KEY,v INT,INDEX vi(v))")
            .unwrap();
        engine
            .execute("INSERT INTO control VALUES(1,0),(2,0),(3,0),(4,0)")
            .unwrap();
        let mut writer = SessionContext::default();
        writer.connection_id = 31851;
        for round in 0..5 {
            engine.execute_with_session(&mut writer, "BEGIN").unwrap();
            let started = Instant::now();
            for i in 0..1000 {
                engine
                    .execute_with_session(
                        &mut writer,
                        &format!("UPDATE control SET v=v+1 WHERE id={}", i % 4 + 1),
                    )
                    .unwrap();
            }
            engine.execute_with_session(&mut writer, "COMMIT").unwrap();
            eprintln!(
                "DDL_NEXT_DML rows={count} round={round} elapsed_us={}",
                started.elapsed().as_micros()
            );
        }
        for kind in ["primary", "fulltext", "spatial", "auto", "nonstrict"] {
            engine.execute("CREATE TABLE t(id INT PRIMARY KEY,k INT NOT NULL,body TEXT,g GEOMETRY NOT NULL)").unwrap();
            engine.execute("CREATE TABLE locked LIKE t").unwrap();
            let point = rows(&engine, "SELECT ST_GeomFromText('POINT(1 1)')")[0][0].clone();
            for name in ["test.t", "test.locked"] {
                insert_fixture_rows(
                    &engine,
                    name,
                    (0..count).map(|id| {
                        vec![
                            Value::Int(id),
                            Value::Int(id),
                            Value::String(format!("quartz document{id}")),
                            point.clone(),
                        ]
                    }),
                );
            }
            engine.checkpoint().unwrap();
            for (mode, name) in [("default", "t"), ("exclusive", "locked")] {
                let action = match kind {
                    "primary" => "DROP PRIMARY KEY,ADD PRIMARY KEY(k)",
                    "fulltext" => "ADD FULLTEXT ft(body)",
                    "auto" => "ADD seq BIGINT AUTO_INCREMENT,ADD UNIQUE uq(seq)",
                    "nonstrict" => "MODIFY k TINYINT",
                    _ => "ADD SPATIAL sp(g)",
                };
                let sql = format!(
                    "ALTER TABLE {name} {action}{}",
                    if mode == "exclusive" {
                        ",LOCK=EXCLUSIVE"
                    } else {
                        ""
                    }
                );
                let mut session = SessionContext::default();
                session.connection_id = 31921;
                if kind == "nonstrict" {
                    engine
                        .execute_with_session(&mut session, "SET sql_mode=''")
                        .unwrap();
                }
                let copy_before = engine.database().ddl_copy_perf_snapshot();
                let index_before = engine.database().storage_status();
                let started = Instant::now();
                engine.execute_with_session(&mut session, &sql).unwrap();
                let elapsed = started.elapsed().as_micros();
                let index_after = engine.database().storage_status();
                let hold = if mode == "exclusive" {
                    elapsed as u64
                } else if matches!(kind, "primary" | "auto" | "nonstrict") {
                    engine.database().ddl_copy_perf_snapshot()[22] - copy_before[22]
                } else {
                    index_after.online_index_prepare_us + index_after.online_index_publish_us
                        - index_before.online_index_prepare_us
                        - index_before.online_index_publish_us
                };
                eprintln!("DDL_NEXT_PROBE kind={kind} mode={mode} rows={count} elapsed_us={elapsed} hold_us={hold}");
                assert_eq!(
                    rows(&engine, &format!("SELECT COUNT(*) FROM {name}")),
                    vec![vec![Value::Int(count)]]
                );
            }
            engine.execute("DROP TABLE t,locked").unwrap();
            drain(&engine);
        }
        let memory_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let memory = open_test_engine(memory_dir.path());
        for name in ["t", "locked"] {
            memory
                .execute(&format!(
                    "CREATE TABLE {name}(id INT PRIMARY KEY,k INT,INDEX ki(k))"
                ))
                .unwrap();
            insert_fixture_rows(
                &memory,
                &format!("test.{name}"),
                (0..count).map(|id| vec![Value::Int(id), Value::Int(id)]),
            );
        }
        for (mode, name) in [("default", "t"), ("exclusive", "locked")] {
            let before = memory.database().ddl_copy_perf_snapshot();
            let started = Instant::now();
            memory
                .execute(&format!(
                    "ALTER TABLE {name} MODIFY k BIGINT{}",
                    if mode == "exclusive" {
                        ",LOCK=EXCLUSIVE"
                    } else {
                        ""
                    }
                ))
                .unwrap();
            let elapsed = started.elapsed().as_micros();
            let hold = if mode == "exclusive" {
                elapsed as u64
            } else {
                memory.database().ddl_copy_perf_snapshot()[22] - before[22]
            };
            eprintln!("DDL_NEXT_PROBE kind=memory mode={mode} rows={count} elapsed_us={elapsed} hold_us={hold}");
        }
    }
}

#[test]
fn ddl_writable_copy_preserves_auto_assignments_across_replay_and_restart() {
    for memory in [false, true] {
        for partition in ["", " PARTITION BY HASH(id) PARTITIONS 2"] {
            let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
            let engine = Arc::new(if memory {
                open_complete_memory_test_engine(dir.path())
            } else {
                open_strict_test_engine(dir.path())
            });
            engine
                .execute(&format!(
                    "CREATE TABLE t(id INT PRIMARY KEY,body TEXT){partition}"
                ))
                .unwrap();
            engine
                .execute("INSERT INTO t VALUES(1,'one'),(2,'two')")
                .unwrap();
            let hook = engine.database().install_ddl_backfill_test_hook();
            let publish = engine.database().install_online_copy_publish_test_hook();
            let worker = engine.clone();
            let ddl = std::thread::spawn(move || {
                let mut session = SessionContext::default();
                session.connection_id = 31861;
                worker
                    .execute_with_session(
                        &mut session,
                        "SET auto_increment_increment=3,auto_increment_offset=2",
                    )
                    .unwrap();
                worker.execute_with_session(&mut session,"ALTER TABLE t ADD seq BIGINT UNSIGNED AUTO_INCREMENT FIRST,ADD UNIQUE uq(seq,id),MODIFY body LONGTEXT,LOCK=NONE")
            });
            let reached = hook.wait_until_reached(Duration::from_secs(10));
            let mut writer = SessionContext::default();
            writer.connection_id = 31862;
            writer.sysvars.lock_wait_timeout = Some(0);
            let changes = (|| -> Result<(), QueryError> {
                for sql in [
                    "BEGIN",
                    "UPDATE t SET body='committed' WHERE id=1",
                    "SAVEPOINT s",
                    "INSERT INTO t VALUES(5,'discarded')",
                    "ROLLBACK TO s",
                    "INSERT INTO t VALUES(3,'three')",
                    "COMMIT",
                ] {
                    engine.execute_with_session(&mut writer, sql)?;
                }
                Ok(())
            })();
            hook.release();
            let publishing = publish.wait_until_reached(Duration::from_secs(10));
            let tail = (|| -> Result<(), QueryError> {
                engine.execute_with_session(&mut writer, "UPDATE t SET body='tail' WHERE id=1")?;
                engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(4,'four')")?;
                Ok(())
            })();
            publish.release();
            let result = ddl.join().unwrap();
            assert!(
                reached && publishing,
                "memory={memory}, {partition}: {result:?}"
            );
            changes.unwrap();
            tail.unwrap();
            result.unwrap();
            assert_eq!(
                rows(&engine, "SELECT seq FROM t ORDER BY seq"),
                vec![
                    vec![Value::UInt(2)],
                    vec![Value::UInt(5)],
                    vec![Value::UInt(8)],
                    vec![Value::UInt(11)]
                ]
            );
            assert_eq!(
                rows(&engine, "SELECT id,seq FROM t WHERE id>=3 ORDER BY id"),
                vec![
                    vec![Value::Int(3), Value::UInt(8)],
                    vec![Value::Int(4), Value::UInt(11)]
                ]
            );
            assert_indexes(&engine);
            if !memory {
                drop(engine);
                let engine = open_strict_test_engine(dir.path());
                engine
                    .execute("INSERT INTO t(id,body) VALUES(6,'after restart')")
                    .unwrap();
                assert_eq!(
                    rows(&engine, "SELECT seq FROM t WHERE id=6"),
                    vec![vec![Value::UInt(12)]]
                );
                assert_indexes(&engine);
            }
        }
    }
}

#[test]
fn ddl_nonstrict_copy_warns_with_bounded_diagnostics_and_matches_exclusive() {
    for memory in [false, true] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = Arc::new(if memory {
            open_test_engine(dir.path())
        } else {
            open_strict_test_engine(dir.path())
        });
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,s VARCHAR(20),n VARCHAR(20),INDEX si(s))")
            .unwrap();
        engine.execute("CREATE TABLE locked LIKE t").unwrap();
        for table in ["t", "locked"] {
            engine
                .execute(&format!(
                    "INSERT INTO {table} VALUES(1,'long value','12garbage'),(2,'text','99999999')"
                ))
                .unwrap();
        }
        let hook = engine.database().install_ddl_backfill_test_hook();
        let publish = engine.database().install_online_copy_publish_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 31871;
            worker
                .execute_with_session(&mut session, "SET sql_mode='',max_error_count=2")
                .unwrap();
            worker.execute_with_session(
                &mut session,
                "ALTER TABLE t MODIFY s VARCHAR(3),MODIFY n SMALLINT,LOCK=NONE",
            )?;
            Ok::<_, QueryError>((session.warnings.len(), session.warning_count()))
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let mut writer = SessionContext::default();
        writer.connection_id = 31872;
        writer.sysvars.lock_wait_timeout = Some(0);
        let changes = (|| -> Result<(), QueryError> {
            for table in ["t", "locked"] {
                engine.execute_with_session(
                    &mut writer,
                    &format!("UPDATE {table} SET s='final string',n='-99999999' WHERE id=1"),
                )?;
            }
            Ok(())
        })();
        hook.release();
        let publishing = publish.wait_until_reached(Duration::from_secs(10));
        let tail = (|| -> Result<(), QueryError> {
            for table in ["t", "locked"] {
                engine.execute_with_session(
                    &mut writer,
                    &format!("INSERT INTO {table} VALUES(3,'another string','invalid')"),
                )?;
            }
            Ok(())
        })();
        publish.release();
        let result = ddl.join().unwrap();
        assert!(reached && publishing, "{result:?}");
        changes.unwrap();
        tail.unwrap();
        let (saved, total) = result.unwrap();
        assert_eq!(saved, 2);
        assert!(total > 2);
        let mut session = SessionContext::default();
        session.connection_id = 31873;
        engine
            .execute_with_session(&mut session, "SET sql_mode=''")
            .unwrap();
        engine
            .execute_with_session(
                &mut session,
                "ALTER TABLE locked MODIFY s VARCHAR(3),MODIFY n SMALLINT,LOCK=EXCLUSIVE",
            )
            .unwrap();
        assert_eq!(
            rows(&engine, "SELECT * FROM t ORDER BY id"),
            rows(&engine, "SELECT * FROM locked ORDER BY id")
        );
        assert_indexes(&engine);
    }
}

#[test]
fn ddl_auto_copy_exhaustion_preserves_source() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    insert_fixture_rows(&engine, "test.t", (0..256).map(|id| vec![Value::Int(id)]));
    engine
        .execute(
            "ALTER TABLE t ADD seq TINYINT UNSIGNED AUTO_INCREMENT,ADD UNIQUE uq(seq),LOCK=NONE",
        )
        .unwrap_err();
    assert!(engine
        .database()
        .table("test.t")
        .unwrap()
        .column_index("seq")
        .is_err());
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(256)]]
    );
    engine.execute("INSERT INTO t VALUES(300)").unwrap();
}

#[test]
fn ddl_auto_copy_crash_child() {
    let Ok(phase) = std::env::var("MYDB_AUTO_COPY_PHASE") else {
        return;
    };
    let directory = std::env::var("MYDB_AUTO_COPY_DIR").unwrap();
    let engine = Arc::new(open_strict_test_engine(std::path::Path::new(&directory)));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    engine.checkpoint().unwrap();
    let hook = if phase == "before" {
        engine.database().install_online_copy_publish_test_hook()
    } else {
        engine.database().install_vector_copy_commit_test_hook()
    };
    let worker = engine.clone();
    let _ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 31881;
        worker
            .execute_with_session(
                &mut session,
                "ALTER TABLE t ADD seq BIGINT AUTO_INCREMENT,ADD UNIQUE uq(seq),LOCK=NONE",
            )
            .unwrap();
    });
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    if phase == "before" {
        engine.execute("UPDATE t SET v=11 WHERE id=1").unwrap();
    }
    std::process::exit(86);
}

#[test]
fn ddl_auto_copy_recovers_assignment_and_allocator() {
    for phase in ["before", "after"] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::executor::tests::ddl_remaining::ddl_auto_copy_crash_child",
                "--nocapture",
            ])
            .env("MYDB_AUTO_COPY_PHASE", phase)
            .env("MYDB_AUTO_COPY_DIR", dir.path())
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{phase}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let engine = open_strict_test_engine(dir.path());
        assert_eq!(
            engine
                .database()
                .table("test.t")
                .unwrap()
                .column_index("seq")
                .is_ok(),
            phase == "after"
        );
        if phase == "after" {
            engine.execute("INSERT INTO t(id,v) VALUES(3,30)").unwrap();
            assert_eq!(
                rows(&engine, "SELECT seq FROM t ORDER BY seq"),
                vec![
                    vec![Value::Int(1)],
                    vec![Value::Int(2)],
                    vec![Value::Int(3)]
                ]
            );
        } else {
            assert_eq!(
                rows(&engine, "SELECT v FROM t WHERE id=1"),
                vec![vec![Value::Int(11)]]
            );
        }
        assert_indexes(&engine);
    }
}

#[test]
#[ignore = "local ordinary DML comparison against pre-change source"]
fn ddl_ordinary_dml_regression_probe() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(dir.path());
    for (kind, extra) in [
        ("btree", "INDEX ni(n)"),
        ("fulltext", "FULLTEXT ft(body)"),
        ("spatial", "SPATIAL sp(g)"),
        ("auto", "INDEX ni(n)"),
    ] {
        engine.execute(&format!("CREATE TABLE dml_{kind}(id INT{} PRIMARY KEY,n INT,body TEXT,g GEOMETRY NOT NULL,{extra})",if kind=="auto" {" AUTO_INCREMENT"}else{""})).unwrap();
        engine.execute(&format!("INSERT INTO dml_{kind} VALUES(1,0,'quartz amber',POINT(0,0)),(2,0,'quartz amber',POINT(0,0)),(3,0,'quartz amber',POINT(0,0)),(4,0,'quartz amber',POINT(0,0))")).unwrap();
        let mut session = SessionContext::default();
        session.connection_id = 31891;
        let update = match kind {
            "btree" => "n=n+1",
            "fulltext" => "n=n+1,body=IF(MOD(n,2)=0,'quartz amber','quartz cobalt')",
            _ => "n=n+1,g=POINT(n,n)",
        };
        for round in 0..5 {
            engine.execute_with_session(&mut session, "BEGIN").unwrap();
            let started = Instant::now();
            for i in 0..1000 {
                let sql = if kind == "auto" {
                    format!(
                        "INSERT INTO dml_auto(n,body,g) VALUES({},'quartz amber',POINT(0,0))",
                        i % 4
                    )
                } else {
                    format!("UPDATE dml_{kind} SET {update} WHERE id={}", i % 4 + 1)
                };
                engine.execute_with_session(&mut session, &sql).unwrap();
            }
            engine.execute_with_session(&mut session, "COMMIT").unwrap();
            eprintln!(
                "DDL_ORDINARY_DML kind={kind} round={round} elapsed_us={}",
                started.elapsed().as_micros()
            );
        }
    }
}

#[test]
fn ddl_fulltext_baseline_shard_does_not_lock_business_shards() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT)")
            .unwrap();
        insert_fixture_rows(
            &engine,
            "test.t",
            (0..800).map(|id| vec![Value::Int(id), Value::String(format!("quartz word{id}"))]),
        );
        let hook = engine
            .database()
            .install_fulltext_copy_statistics_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 31901;
            worker.execute_with_session(
                &mut session,
                "CREATE FULLTEXT INDEX ft ON t(body) LOCK=NONE",
            )
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let mut writer = SessionContext::default();
        writer.connection_id = 31902;
        writer.sysvars.lock_wait_timeout = Some(0);
        let writes = (|| -> Result<(), QueryError> {
            for n in 0..64 {
                engine.execute_with_session(
                    &mut writer,
                    &format!("UPDATE t SET body='quartz cobalt{n}' WHERE id=1"),
                )?;
            }
            Ok(())
        })();
        let cancelled = if cancel {
            let jobs = engine.database().alter_copy_job_snapshots();
            let job = jobs.iter().find(|j| j.table == "t").unwrap();
            engine.database().cancel_ddl_job(job.job_id)
        } else {
            Ok(())
        };
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{result:?}");
        writes.unwrap();
        cancelled.unwrap();
        if cancel {
            assert_eq!(result.unwrap_err().code(), 1317);
            assert!(!engine
                .database()
                .table("test.t")
                .unwrap()
                .indexes
                .iter()
                .any(|i| i.is_fulltext()));
            engine
                .execute("CREATE FULLTEXT INDEX ft ON t(body) LOCK=NONE")
                .unwrap();
        } else {
            result.unwrap();
        }
        drain(&engine);
        assert_current(&engine);
        let db = engine.database();
        let table = db.table("test.t").unwrap();
        let view = db.begin_read_only_txn().unwrap();
        let index = table.indexes.iter().find(|i| i.is_fulltext()).unwrap();
        let stats = db
            .fulltext_corpus_statistics(view, &table, index, &["quartz".into()], None)
            .unwrap();
        assert!(stats.counter_rows_read <= 130);
        assert_eq!(stats.documents, 800);
        db.rollback_txn(view.txn_id).unwrap();
        assert_indexes(&engine);
    }
}

#[test]
fn ddl_special_index_capture_handles_load_replace_delete_and_cancel() {
    for kind in ["FULLTEXT", "SPATIAL"] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,g GEOMETRY NOT NULL)")
            .unwrap();
        engine
            .execute(
                "INSERT INTO t VALUES(1,'quartz amber',POINT(1,1)),(2,'silver quartz',POINT(2,2))",
            )
            .unwrap();
        let column = if kind == "FULLTEXT" { "body" } else { "g" };
        for cancel in [true, false] {
            let hook = engine.database().install_ddl_backfill_test_hook();
            let worker = engine.clone();
            let sql = format!("CREATE {kind} INDEX ft ON t({column}) LOCK=NONE");
            let ddl = std::thread::spawn(move || {
                let mut session = SessionContext::default();
                session.connection_id = 31911;
                worker.execute_with_session(&mut session, &sql)
            });
            let reached = hook.wait_until_reached(Duration::from_secs(10));
            let input = dir.path().join("rows.csv");
            std::fs::write(
                &input,
                "1|load quartz|POINT(3 3)\n3|load amber|POINT(4 4)\n",
            )
            .unwrap();
            let mut writer = SessionContext::default();
            writer.connection_id = 31912;
            writer.sysvars.lock_wait_timeout = Some(0);
            let writes = (|| -> Result<(), QueryError> {
                engine.execute_with_session(&mut writer,&format!("LOAD DATA INFILE '{}' REPLACE INTO TABLE t FIELDS TERMINATED BY '|' (id,body,@g) SET g=ST_GeomFromText(@g)",crate::util::file::sql_path(&input)))?;
                engine.execute_with_session(&mut writer, "DELETE FROM t WHERE id=2")?;
                Ok(())
            })();
            let cancelled = if cancel {
                let jobs = engine.database().alter_copy_job_snapshots();
                engine.database().cancel_ddl_job(
                    jobs.iter()
                        .find(|j| j.table == "t" && j.state == "INDEX_BUILD")
                        .unwrap()
                        .job_id,
                )
            } else {
                Ok(())
            };
            hook.release();
            let result = ddl.join().unwrap();
            assert!(reached, "{result:?}");
            writes.unwrap();
            cancelled.unwrap();
            if cancel {
                assert_eq!(result.unwrap_err().code(), 1317);
            } else {
                result.unwrap();
            }
            assert_indexes(&engine);
            if !cancel {
                assert_current(&engine);
            }
        }
    }
}

#[test]
fn ddl_leaf_retirement_does_not_delete_reexchanged_storage_statistics() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine
        .database()
        .set_recyclebin_variable("mydb_recyclebin", 0)
        .unwrap();
    leaf_fixture(&engine, 800);
    engine.execute("CREATE TABLE stage(id INT PRIMARY KEY,body TEXT,FULLTEXT ft(body),FULLTEXT fp(body) WHERE id>0)").unwrap();
    engine
        .execute("INSERT INTO stage VALUES(9000,'staging amber')")
        .unwrap();
    engine
        .execute("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE stage")
        .unwrap();
    let hook = engine.database().install_retirement_cleanup_test_hook();
    let worker = engine.clone();
    let (done, finished) = std::sync::mpsc::channel();
    let cleanup = std::thread::spawn(move || {
        let result = worker
            .database()
            .maintain_catalog_retirements_until(|| false);
        let _ = done.send(());
        result
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    let reached = loop {
        if hook.wait_until_reached(Duration::from_millis(10)) {
            break true;
        }
        if finished.try_recv().is_ok() || Instant::now() >= deadline {
            break false;
        }
    };
    let before = engine.database().table("test.t").unwrap().storage_ids();
    let before_epoch = engine.database().catalog_publication_epoch();
    let worker = engine.clone();
    let exchange = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 31931;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE stage",
        )
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while (engine.database().table("test.t").unwrap().storage_ids() == before
        || engine.database().catalog_publication_epoch() == before_epoch)
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(1));
    }
    hook.release();
    cleanup.join().unwrap().unwrap();
    exchange.join().unwrap().unwrap();
    assert_current(&engine);
    assert!(
        !reached,
        "a storage id owned by another table can return through EXCHANGE"
    );
    engine.checkpoint().unwrap();
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    engine
        .database()
        .set_recyclebin_variable("mydb_recyclebin", 0)
        .unwrap();
    assert_current(&engine);
    engine.execute("DROP TABLE t,stage").unwrap();
    drain(&engine);
}

#[test]
fn ddl_auto_copy_replacing_old_auto_column_refreshes_allocator() {
    for memory in [false, true] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = if memory {
            open_test_engine(dir.path())
        } else {
            open_strict_test_engine(dir.path())
        };
        engine
            .execute("CREATE TABLE t(old_id INT AUTO_INCREMENT PRIMARY KEY,k INT)")
            .unwrap();
        insert_fixture_rows(
            &engine,
            "test.t",
            (1..=20).map(|id| vec![Value::Int(-id), Value::Int(id)]),
        );
        engine.execute("ALTER TABLE t DROP old_id,ADD seq BIGINT AUTO_INCREMENT,ADD PRIMARY KEY(seq),LOCK=NONE").unwrap();
        engine.execute("INSERT INTO t(k) VALUES(99)").unwrap();
        assert_eq!(
            rows(&engine, "SELECT seq FROM t WHERE k=99"),
            vec![vec![Value::Int(21)]]
        );
        assert_indexes(&engine);
    }
}
