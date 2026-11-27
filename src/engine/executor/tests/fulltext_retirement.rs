//! Deferred FULLTEXT statistics cleanup after index and table publication.
use super::*;

fn counters(engine: &StorageQueryEngine, prefix: &[u64]) -> usize {
    let db = engine.database();
    let schema = crate::storage::data_dictionary::dd_table_schema(
        crate::storage::data_dictionary::DD_FULLTEXT_COUNTERS,
    )
    .unwrap();
    let read = db.begin_read_only_txn().unwrap();
    let count = db
        .prefix_index_entries_visible(
            &schema,
            &schema.indexes[0],
            crate::storage::IndexKey::from(
                prefix
                    .iter()
                    .copied()
                    .map(crate::storage::KeyPart::UInt)
                    .collect::<Vec<_>>(),
            ),
            read,
        )
        .unwrap()
        .len();
    db.rollback_txn(read.txn_id).unwrap();
    count
}

fn populate(engine: &StorageQueryEngine, table: &str, count: i64) {
    engine
        .execute(&format!(
            "CREATE TABLE {table}(id INT PRIMARY KEY,body TEXT,FULLTEXT ft(body))"
        ))
        .unwrap();
    insert_fixture_rows(
        engine,
        &format!("test.{table}"),
        (0..count).map(|id| {
            vec![
                Value::Int(id),
                Value::String(format!("quartz document{id}")),
            ]
        }),
    );
}

#[test]
fn fulltext_retirement_drop_index_defers_counter_work() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(dir.path());
    populate(&engine, "t", 800);
    let table = engine.database().table("test.t").unwrap();
    let prefix = [
        table.id,
        table.active_generation,
        table.indexes[1].fulltext.as_ref().unwrap().corpus_id,
    ];
    let before = counters(&engine, &prefix);
    assert!(before > 256);
    let before_ddl = engine.database().catalog_publication_epoch();
    engine.execute("DROP INDEX ft ON t").unwrap();
    let published = engine.database().catalog_publication_epoch();
    assert!(published > before_ddl);
    assert_eq!(
        counters(&engine, &prefix),
        before,
        "DROP must defer counter deletion"
    );
    engine
        .database()
        .maintain_catalog_retirements_until(|| false)
        .unwrap();
    let after = counters(&engine, &prefix);
    assert!(after < before && before - after <= 256);
    assert_eq!(
        engine.database().catalog_publication_epoch(),
        published,
        "internal retirement must preserve prepared DML catalog caches"
    );
    drain(&engine);
    assert_eq!(engine.database().catalog_publication_epoch(), published);
}

#[test]
#[ignore = "local strict-durability DROP hold-time and ordinary DML comparison"]
fn fulltext_retirement_lock_scaling_probe() {
    for count in [1000, 10000] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine
            .database()
            .set_recyclebin_variable("mydb_recyclebin", 0)
            .unwrap();
        engine
            .execute("CREATE TABLE control(id INT PRIMARY KEY,v INT,INDEX vi(v))")
            .unwrap();
        engine
            .execute("INSERT INTO control VALUES(1,0),(2,0),(3,0),(4,0)")
            .unwrap();
        let mut session = SessionContext::default();
        session.connection_id = 31201;
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
                "FT_RETIRE_DML rows={count} round={round} elapsed_us={}",
                started.elapsed().as_micros()
            );
        }
        populate(&engine, "t", count);
        populate(&engine, "dropped", count);
        engine.checkpoint().unwrap();
        for (mode, sql) in [
            ("index", "DROP INDEX ft ON t"),
            ("table", "DROP TABLE dropped"),
        ] {
            let before = engine.database().fulltext_drop_hold_us_for_test();
            let started = Instant::now();
            engine.execute(sql).unwrap();
            let after = engine.database().fulltext_drop_hold_us_for_test();
            eprintln!(
                "FT_RETIRE_PROBE mode={mode} rows={count} elapsed_us={} hold_us={}",
                started.elapsed().as_micros(),
                after - before
            );
        }
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

fn assert_current(engine: &StorageQueryEngine, table: &str) {
    let db = engine.database();
    let read = db.begin_read_only_txn().unwrap();
    super::fulltext_incremental::assert_corpora(engine, table, read);
    db.rollback_txn(read.txn_id).unwrap();
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("rows: {sql}");
    };
    result.rows
}

#[test]
fn fulltext_retirement_shared_corpora_recreate_and_old_snapshot() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,title TEXT,active INT,FULLTEXT ft(body),FULLTEXT alias(body),FULLTEXT partial(body) WHERE active=1,FULLTEXT title_ft(title),FULLTEXT ng(body,title) WITH PARSER ngram)").unwrap();
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..800).map(|id| {
            vec![
                Value::Int(id),
                Value::String(format!("quartz retired{id}")),
                Value::String("数据库 amber".into()),
                Value::Int(id % 2),
            ]
        }),
    );
    let old = engine.database().table("test.t").unwrap();
    let index = old.indexes.iter().find(|index| index.name == "ft").unwrap();
    let corpus = index.fulltext.as_ref().unwrap().corpus_id;
    let prefix = [old.id, old.active_generation, corpus];
    let before = counters(&engine, &prefix);
    engine.execute("DROP INDEX ft ON t").unwrap();
    engine.execute("DROP INDEX alias ON t").unwrap();
    drain(&engine);
    assert_eq!(
        counters(&engine, &prefix),
        before,
        "partial alias still owns the corpus"
    );
    let snapshot = engine.database().begin_read_only_txn().unwrap();
    let frozen = engine
        .database()
        .fulltext_corpus_statistics(snapshot, &old, index, &["quartz".into()], None)
        .unwrap();
    engine.execute("DROP INDEX partial ON t").unwrap();
    assert_eq!(counters(&engine, &prefix), before);
    let hook = engine.database().install_retirement_cleanup_test_hook();
    let worker = engine.clone();
    let cleanup = std::thread::spawn(move || {
        worker
            .database()
            .maintain_catalog_retirements_until(|| false)
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 31301;
    writer.sysvars.lock_wait_timeout = Some(0);
    let changes = (|| -> Result<(), QueryError> {
        engine.execute_with_session(
            &mut writer,
            "UPDATE t SET body='current quartz',title='全文 amber' WHERE id=1",
        )?;
        engine.execute_with_session(&mut writer, "CREATE FULLTEXT INDEX ft ON t(body)")?;
        engine.execute_with_session(
            &mut writer,
            "INSERT INTO t VALUES(9000,'new quartz','检索 amber',1)",
        )?;
        engine
            .execute_with_session(&mut writer, "SELECT id FROM t WHERE id=1")?
            .materialize()?;
        Ok(())
    })();
    hook.release();
    cleanup.join().unwrap().unwrap();
    assert!(reached);
    changes.unwrap();
    let current = engine.database().table("test.t").unwrap();
    let new_corpus = current
        .indexes
        .iter()
        .find(|index| index.name == "ft")
        .unwrap()
        .fulltext
        .as_ref()
        .unwrap()
        .corpus_id;
    assert_ne!(corpus, new_corpus);
    drain(&engine);
    assert_eq!(counters(&engine, &prefix), 0);
    let retained = engine
        .database()
        .fulltext_corpus_statistics(snapshot, &old, index, &["quartz".into()], None)
        .unwrap();
    assert_eq!(retained.documents, frozen.documents);
    assert_eq!(retained.total_tokens, frozen.total_tokens);
    assert_eq!(retained.frequencies, frozen.frequencies);
    engine.database().rollback_txn(snapshot.txn_id).unwrap();
    assert_current(&engine, "test.t");
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_current(&engine, "test.t");
    assert_eq!(
        rows(&engine, "SELECT body FROM t WHERE id=1"),
        vec![vec![Value::String("current quartz".into())]]
    );
}

#[test]
fn fulltext_retirement_drop_tables_preserves_replacements_and_other_owners() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine
        .database()
        .set_recyclebin_variable("mydb_recyclebin", 0)
        .unwrap();
    populate(&engine, "t", 800);
    populate(&engine, "u", 700);
    populate(&engine, "survivor", 400);
    let old = engine.database().table("test.t").unwrap();
    let other = engine.database().table("test.u").unwrap();
    let snapshot = engine.database().begin_read_only_txn().unwrap();
    engine.execute("DROP TABLE t,u").unwrap();
    assert!(counters(&engine, &[old.id]) > 256);
    assert!(counters(&engine, &[other.id]) > 256);
    populate(&engine, "t", 2);
    assert_ne!(engine.database().table("test.t").unwrap().id, old.id);
    let hook = engine.database().install_retirement_cleanup_test_hook();
    let worker = engine.clone();
    let cleanup = std::thread::spawn(move || {
        worker
            .database()
            .maintain_catalog_retirements_until(|| false)
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 31302;
    writer.sysvars.lock_wait_timeout = Some(0);
    let changes = (|| -> Result<(), QueryError> {
        engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(3,'replacement quartz')")?;
        engine.execute_with_session(
            &mut writer,
            "UPDATE survivor SET body='surviving quartz' WHERE id=1",
        )?;
        engine
            .execute_with_session(
                &mut writer,
                "SELECT id FROM t WHERE MATCH(body) AGAINST('quartz' IN BOOLEAN MODE)",
            )?
            .materialize()?;
        Ok(())
    })();
    hook.release();
    cleanup.join().unwrap().unwrap();
    assert!(reached);
    changes.unwrap();
    drain(&engine);
    assert_eq!(counters(&engine, &[old.id]), 0);
    assert_eq!(counters(&engine, &[other.id]), 0);
    let frozen = engine
        .database()
        .fulltext_corpus_statistics(snapshot, &old, &old.indexes[1], &["quartz".into()], None)
        .unwrap();
    assert_eq!(frozen.documents, 800);
    assert_eq!(frozen.frequencies["quartz"], 800);
    engine.database().rollback_txn(snapshot.txn_id).unwrap();
    assert_current(&engine, "test.t");
    assert_current(&engine, "test.survivor");
    engine.checkpoint().unwrap();
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_current(&engine, "test.t");
    assert_current(&engine, "test.survivor");
}

#[test]
fn fulltext_retirement_multiple_corpora_and_generation_jobs_do_not_overwrite() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,a TEXT,b TEXT,c TEXT,FULLTEXT fa(a),FULLTEXT fab(a,b),FULLTEXT fb(b),FULLTEXT fc(c))").unwrap();
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..800).map(|id| {
            vec![
                Value::Int(id),
                Value::String(format!("quartz retired{id}")),
                Value::String(format!("amber item{id}")),
                Value::String(format!("retained corpus{id}")),
            ]
        }),
    );
    let old = engine.database().table("test.t").unwrap();
    engine.execute("ALTER TABLE t DROP COLUMN a").unwrap();
    let changed = engine.database().table("test.t").unwrap();
    assert_eq!(changed.active_generation, old.active_generation);
    for index in old
        .indexes
        .iter()
        .filter(|index| index.name == "fa" || index.name == "fab")
    {
        assert!(
            counters(
                &engine,
                &[
                    old.id,
                    old.active_generation,
                    index.fulltext.as_ref().unwrap().corpus_id
                ]
            ) > 0
        );
    }
    engine
        .database()
        .maintain_catalog_retirements_until(|| false)
        .unwrap();
    drain(&engine);
    for index in old
        .indexes
        .iter()
        .filter(|index| index.name == "fa" || index.name == "fab")
    {
        assert_eq!(
            counters(
                &engine,
                &[
                    old.id,
                    old.active_generation,
                    index.fulltext.as_ref().unwrap().corpus_id
                ]
            ),
            0
        );
    }
    assert_current(&engine, "test.t");
    engine.execute("DROP INDEX fc ON t").unwrap();
    engine
        .database()
        .maintain_catalog_retirements_until(|| false)
        .unwrap();
    let pending = engine.database().catalog_retirement_count_for_test();
    assert!(pending > 0);
    engine
        .execute("ALTER TABLE t MODIFY b LONGTEXT, ALGORITHM=COPY, LOCK=NONE")
        .unwrap();
    assert!(engine.database().catalog_retirement_count_for_test() > pending);
    drain(&engine);
    assert_eq!(counters(&engine, &[old.id, old.active_generation]), 0);
    assert_current(&engine, "test.t");
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_current(&engine, "test.t");
}

#[test]
fn fulltext_retirement_partitioned_corpora_and_recycle_restore() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,title TEXT,FULLTEXT ft(body),FULLTEXT ng(title) WITH PARSER ngram) PARTITION BY HASH(id) PARTITIONS 4").unwrap();
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..600).map(|id| {
            vec![
                Value::Int(id),
                Value::String(format!("quartz item{id}")),
                Value::String("全文数据库".into()),
            ]
        }),
    );
    let old = engine.database().table("test.t").unwrap();
    engine.execute("DROP INDEX ft ON t").unwrap();
    drain(&engine);
    assert_eq!(
        counters(
            &engine,
            &[
                old.id,
                old.active_generation,
                old.indexes[1].fulltext.as_ref().unwrap().corpus_id
            ]
        ),
        0
    );
    assert_current(&engine, "test.t");
    engine
        .database()
        .set_recyclebin_variable("mydb_recyclebin", 1)
        .unwrap();
    engine.execute("DROP TABLE t").unwrap();
    let entry = engine.database().recyclebin_entries().unwrap().remove(0);
    assert!(counters(&engine, &[old.id]) > 0);
    drain(&engine);
    engine
        .execute(&format!("RESTORE TABLE {}", entry.name()))
        .unwrap();
    assert_current(&engine, "test.t");
    engine.execute("DROP TABLE t").unwrap();
    let entry = engine.database().recyclebin_entries().unwrap().remove(0);
    engine
        .execute(&format!("PURGE RECYCLEBIN {}", entry.name()))
        .unwrap();
    drain(&engine);
    assert_eq!(counters(&engine, &[old.id]), 0);
}

#[test]
fn fulltext_retirement_stop_and_checkpoint_resume_are_bounded() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(dir.path());
    populate(&engine, "t", 900);
    let old = engine.database().table("test.t").unwrap();
    engine.execute("DROP INDEX ft ON t").unwrap();
    let before = counters(&engine, &[old.id]);
    let polls = std::sync::atomic::AtomicUsize::new(0);
    engine
        .database()
        .maintain_catalog_retirements_until(|| {
            polls.fetch_add(1, std::sync::atomic::Ordering::Relaxed) > 30
        })
        .unwrap();
    let after = counters(&engine, &[old.id]);
    assert!(after < before && before - after < 256);
    assert!(engine.database().catalog_retirement_count_for_test() > 0);
    engine
        .execute("CREATE FULLTEXT INDEX ft ON t(body)")
        .unwrap();
    engine.checkpoint().unwrap();
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    drain(&engine);
    assert_eq!(
        counters(
            &engine,
            &[
                old.id,
                old.active_generation,
                old.indexes[1].fulltext.as_ref().unwrap().corpus_id
            ]
        ),
        0
    );
    assert_current(&engine, "test.t");
}

#[test]
fn fulltext_retirement_crash_child() {
    let Ok(phase) = std::env::var("MYDB_FT_RETIRE_CRASH_PHASE") else {
        return;
    };
    let directory = std::env::var("MYDB_FT_RETIRE_CRASH_DIR").unwrap();
    let operation = std::env::var("MYDB_FT_RETIRE_CRASH_OPERATION").unwrap();
    let engine = open_strict_test_engine(std::path::Path::new(&directory));
    engine
        .database()
        .set_recyclebin_variable("mydb_recyclebin", 0)
        .unwrap();
    populate(&engine, "t", 800);
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
        .execute(if operation == "index" {
            "DROP INDEX ft ON t"
        } else {
            "DROP TABLE t"
        })
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
fn fulltext_retirement_crash_recovery_is_atomic_for_ddl_and_batch_cursor() {
    for operation in ["index", "table"] {
        for phase in ["before", "after", "batch", "resume"] {
            let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "engine::executor::tests::fulltext_retirement::fulltext_retirement_crash_child",
                    "--nocapture",
                ])
                .env("MYDB_FT_RETIRE_CRASH_PHASE", phase)
                .env("MYDB_FT_RETIRE_CRASH_DIR", dir.path())
                .env("MYDB_FT_RETIRE_CRASH_OPERATION", operation)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(86),
                "{operation}/{phase}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let engine = open_strict_test_engine(dir.path());
            let existing = engine.database().table("test.t");
            if phase == "before" {
                assert!(existing
                    .unwrap()
                    .indexes
                    .iter()
                    .any(|index| index.is_fulltext()));
                assert_current(&engine, "test.t");
            } else if operation == "table" {
                assert!(existing.is_err());
                populate(&engine, "t", 2);
            } else {
                assert!(!existing
                    .unwrap()
                    .indexes
                    .iter()
                    .any(|index| index.is_fulltext()));
                engine
                    .execute("CREATE FULLTEXT INDEX ft ON t(body)")
                    .unwrap();
            }
            engine
                .execute("INSERT INTO t VALUES(9000,'surviving quartz')")
                .unwrap();
            drain(&engine);
            assert_current(&engine, "test.t");
            let current = engine.database().table("test.t").unwrap();
            assert_eq!(
                counters(
                    &engine,
                    &[
                        current.id,
                        current.active_generation,
                        current.indexes[1].fulltext.as_ref().unwrap().corpus_id
                    ]
                ),
                counters(&engine, &[current.id])
            );
            assert_eq!(counters(&engine, &[]), counters(&engine, &[current.id]));
            engine.checkpoint().unwrap();
            drop(engine);
            let engine = open_strict_test_engine(dir.path());
            assert_current(&engine, "test.t");
        }
    }
}

#[test]
fn fulltext_retirement_memory_and_relaxed_backends_preserve_live_counters() {
    for memory in [true, false] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = if memory {
            open_test_engine(dir.path())
        } else {
            open_relaxed_test_engine(dir.path())
        };
        engine
            .database()
            .set_recyclebin_variable("mydb_recyclebin", 0)
            .unwrap();
        populate(&engine, "t", 600);
        let old = engine.database().table("test.t").unwrap();
        engine.execute("DROP INDEX ft ON t").unwrap();
        assert!(counters(&engine, &[old.id]) > 256);
        drain(&engine);
        assert_eq!(counters(&engine, &[old.id]), 0);
        engine
            .execute("CREATE FULLTEXT INDEX ft ON t(body)")
            .unwrap();
        assert_current(&engine, "test.t");
        engine.execute("DROP TABLE t").unwrap();
        populate(&engine, "t", 2);
        let current = engine.database().table("test.t").unwrap();
        assert_ne!(old.id, current.id);
        drain(&engine);
        assert_eq!(counters(&engine, &[old.id]), 0);
        assert_eq!(counters(&engine, &[]), counters(&engine, &[current.id]));
        assert_current(&engine, "test.t");
    }
}
