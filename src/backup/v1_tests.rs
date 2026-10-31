use super::coordinator::{Coordinator, Policy};
use super::repository::Repository;
use super::restore::Target;
use super::*;
use crate::engine::StorageQueryEngine;
fn root() -> tempfile::TempDir {
    tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap()
}
fn engine(path: &Path) -> StorageQueryEngine {
    StorageQueryEngine::open(path).unwrap()
}
fn job(coordinator: &Arc<Coordinator>, job: super::coordinator::Job) -> super::coordinator::Job {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let result = coordinator
            .jobs()
            .into_iter()
            .find(|candidate| candidate.id == job.id)
            .unwrap();
        if result.state != "RUNNING" {
            assert_eq!(
                result.state, "COMPLETED",
                "{} / {}: {}",
                result.kind, result.phase, result.error
            );
            return result;
        }
        assert!(Instant::now() < deadline, "job did not terminate");
        std::thread::sleep(Duration::from_millis(10));
    }
}
// Final assertions are complete and the temporary source will be deleted.
// Drain admitted backup jobs; engine Drop joins its remaining workers without
// adding a shutdown checkpoint that no subsequent recovery will observe.
fn discard_source(source: StorageQueryEngine, coordinator: Arc<Coordinator>) {
    coordinator.shutdown().unwrap();
    drop(coordinator);
    drop(source);
}

fn rows(db: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = db.execute(sql).unwrap() else {
        panic!("expected rows")
    };
    result.rows
}

#[test]
fn sealed_partition_indexes_use_leaf_heaps() {
    let root = root();
    let source = engine(&root.path().join("source"));
    for (table, partitioning) in [
        ("parts", "PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)"),
        ("subparts", "PARTITION BY RANGE(id) SUBPARTITION BY HASH(id) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)"),
    ] {
        source.execute(&format!("CREATE TABLE {table}(id INT PRIMARY KEY,v INT,INDEX iv(v)) {partitioning}")).unwrap();
        source.execute(&format!("INSERT INTO {table} VALUES(1,11),(2,12),(20,22),(21,23)")).unwrap();
    }
    let coordinator = Coordinator::get(source.db_handle()).unwrap();
    let repository = root.path().join("repository");
    let base = job(&coordinator, coordinator.backup(&repository).unwrap())
        .base_id
        .unwrap();
    let runtime = root.path().join("runtime");
    let reader = super::open_base(&repository, &base, &runtime).unwrap();
    let backup = StorageQueryEngine::open_backup_image(
        reader.data_dir.clone(),
        runtime,
        Default::default(),
        Arc::new(crate::engine::session_registry::SessionRegistry::new()),
        vec![],
    )
    .unwrap();
    for table in ["parts", "subparts"] {
        for sql in [
            format!("SELECT id,v FROM {table} ORDER BY id"),
            format!("SELECT id,v FROM {table} FORCE INDEX(PRIMARY) WHERE id>=2 ORDER BY id"),
            format!(
                "SELECT id,v FROM {table} FORCE INDEX(iv) WHERE v BETWEEN 12 AND 23 ORDER BY id"
            ),
        ] {
            assert_eq!(rows(&backup, &sql), rows(&source, &sql), "{sql}");
        }
    }
    backup.shutdown().unwrap();
    discard_source(source, coordinator);
}

#[test]
fn hybrid_rrf_immutable_backup_preserves_ranked_snapshot() {
    let root = root();
    let source = engine(&root.path().join("source"));
    source.execute("CREATE TABLE docs(id INT PRIMARY KEY,body TEXT,v VECTOR(2) NOT NULL,FULLTEXT KEY ft(body))").unwrap();
    source
        .execute(
            "INSERT INTO docs VALUES(1,'database guide','[1,0]'),(2,'database database','[0,1]')",
        )
        .unwrap();
    source
        .execute("CREATE INDEX hv ON docs(v) USING HNSW")
        .unwrap();
    let sql = "SELECT id,rrf_score,match_count FROM ((SELECT id FROM docs WHERE MATCH(body) AGAINST('database') ORDER BY MATCH(body) AGAINST('database') DESC,id LIMIT 2) RRF (SELECT /*+ HNSW_EF(32) */ id FROM docs ORDER BY L2_DISTANCE(v,TO_VECTOR('[1,0]')),id LIMIT 2)) h ORDER BY id";
    let expected = rows(&source, sql);
    let coordinator = Coordinator::get(source.db_handle()).unwrap();
    let repository = root.path().join("repository");
    let base = job(&coordinator, coordinator.backup(&repository).unwrap())
        .base_id
        .unwrap();
    source.execute("DELETE FROM docs WHERE id=1").unwrap();
    source
        .execute("UPDATE docs SET body='changed',v='[2,2]' WHERE id=2")
        .unwrap();
    let runtime = root.path().join("runtime");
    let reader = super::open_base(&repository, &base, &runtime).unwrap();
    let backup = StorageQueryEngine::open_backup_image(
        reader.data_dir.clone(),
        runtime,
        Default::default(),
        Arc::new(crate::engine::session_registry::SessionRegistry::new()),
        vec![],
    )
    .unwrap();
    assert_eq!(rows(&backup, sql), expected);
    assert!(backup.execute("DELETE FROM docs").is_err());
    backup.shutdown().unwrap();
    discard_source(source, coordinator);
}

#[test]
fn sealed_fulltext_reads_prepared_xa_visible_rows_and_scores() {
    for commit in [true, false] {
        let root = root();
        let preparation = crate::platform::fs::FixtureSyncBatch::new(root.path()).unwrap();
        let source = engine(&root.path().join("source"));
        let mut cases = Vec::new();
        for (case, change) in [
            "UPDATE docs SET body='prepared content' WHERE id=1",
            "DELETE FROM docs WHERE id=1",
            "INSERT INTO docs VALUES(3,'database uncommitted')",
        ]
        .into_iter()
        .enumerate()
        {
            let table = format!("docs_{case}");
            source
                .execute(&format!(
                    "CREATE TABLE {table}(id INT PRIMARY KEY, body TEXT, FULLTEXT KEY ft(body))"
                ))
                .unwrap();
            source.execute(&format!("INSERT INTO {table} VALUES(1,'backup database'),(2,'database history'),(4,'unrelated words')")).unwrap();
            cases.push((table, change, format!("fulltext-backup-{case}")));
        }
        preparation.finish().unwrap();
        for (table, change, xid) in &cases {
            let mut xa = crate::engine::SessionContext::default();
            for sql in [
                format!("XA START '{xid}'"),
                change.replace("docs", table),
                format!("XA END '{xid}'"),
                format!("XA PREPARE '{xid}'"),
            ] {
                source.execute_with_session(&mut xa, &sql).unwrap();
            }
        }
        let coordinator = Coordinator::get(source.db_handle()).unwrap();
        let repository = root.path().join("repository");
        let base = job(&coordinator, coordinator.backup(&repository).unwrap())
            .base_id
            .unwrap();
        for (_, _, xid) in &cases {
            source
                .execute(&format!(
                    "XA {} '{xid}'",
                    if commit { "COMMIT" } else { "ROLLBACK" }
                ))
                .unwrap();
        }
        for reopen in 0..2 {
            let runtime = root.path().join(format!("runtime-{reopen}"));
            let reader = super::open_base(&repository, &base, &runtime).unwrap();
            let query = StorageQueryEngine::open_backup_image(
                reader.data_dir.clone(),
                runtime,
                Default::default(),
                Arc::new(crate::engine::session_registry::SessionRegistry::new()),
                vec![],
            )
            .unwrap();
            for (table, _, _) in &cases {
                let query_rows = |sql: &str| rows(&query, &sql.replace("docs", table));
                assert_eq!(
                    query_rows("SELECT id,body FROM docs ORDER BY id")[0][1],
                    Value::String("backup database".into())
                );
                let scored = query_rows(
                    "SELECT id,MATCH(body) AGAINST('database') AS score FROM docs ORDER BY id",
                );
                assert_eq!(scored.len(), 3);
                assert!(scored[..2].iter().all(|row| row[1]
                    .to_display_string()
                    .parse::<f64>()
                    .unwrap()
                    > 0.0));
                assert_eq!(
                    scored[2][1].to_display_string().parse::<f64>().unwrap(),
                    0.0
                );
                for term in ["'database'", "'+data*' IN BOOLEAN MODE"] {
                    assert_eq!(
                        query_rows(&format!(
                            "SELECT id FROM docs WHERE MATCH(body) AGAINST({term}) ORDER BY id"
                        )),
                        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
                    );
                }
                assert_eq!(query_rows("SELECT id FROM docs WHERE MATCH(body) AGAINST('\"backup database\"' IN BOOLEAN MODE)"), vec![vec![Value::Int(1)]]);
                assert!(query_rows(
                    "SELECT id FROM docs WHERE MATCH(body) AGAINST('prepared uncommitted')"
                )
                .is_empty());
                let mut session = crate::engine::SessionContext::default();
                query.execute_with_session(&mut session, &format!("PREPARE score FROM 'SELECT id,MATCH(body) AGAINST(?) AS score FROM {table} ORDER BY id'")).unwrap();
                query
                    .execute_with_session(&mut session, "SET @term='database'")
                    .unwrap();
                let QueryResponse::ResultSet(prepared) = query
                    .execute_with_session(&mut session, "EXECUTE score USING @term")
                    .unwrap()
                else {
                    panic!("expected rows")
                };
                assert_eq!(prepared.rows, scored);
                query
                    .execute_with_session(&mut session, "DEALLOCATE PREPARE score")
                    .unwrap();
                assert!(query
                    .execute(&format!("UPDATE {table} SET body='forbidden'"))
                    .is_err());
            }
            query.shutdown().unwrap();
            drop(query);
            native::verify(&reader.data_dir).unwrap();
        }
        discard_source(source, coordinator);
    }
}

#[test]
fn typed_sql_rejects_retired_duplicate_and_unknown_options() {
    for sql in ["BACKUP DATABASE TO 'file:///tmp/mydb/repo'","BACKUP WAL TO 'file:///tmp/mydb/repo'","ALTER MYDB BACKUP TO 'file:///tmp/mydb/repo' SET WAL_ARCHIVE=MANUAL,MAX_PENDING_WAL_BYTES=1024","ALTER MYDB BACKUP TO 'file:///tmp/mydb/repo' SET WAL_ARCHIVE=DISABLED,DISCARD_PENDING=TRUE","SHOW MYDB BACKUP STATUS","SHOW MYDB BACKUP JOBS","SHOW MYDB BACKUPS FROM 'file:///tmp/mydb/repo'","CANCEL MYDB BACKUP 'job-id'"]{assert!(matches!(crate::parser::parse_statement(sql).unwrap(),Statement::BackupAdmin{..}),"{sql}");}
    for tail in [
        " TYPE FULL",
        " FORMAT NATIVE",
        " TYPE AUTO",
        " TYPE INCREMENTAL",
        " CONTINUOUS",
        " BASE 'old'",
        " ENCRYPTION_KEY_ID='key'",
        " GARBAGE",
    ] {
        assert!(crate::parser::parse_statement(&format!(
            "BACKUP DATABASE TO 'file:///tmp/mydb/repo'{tail}"
        ))
        .is_err());
    }
    for sql in ["ALTER MYDB BACKUP TO 'file:///tmp/mydb/repo' SET WAL_ARCHIVE=MANUAL,WAL_ARCHIVE=CONTINUOUS","ALTER MYDB BACKUP TO 'file:///tmp/mydb/repo' SET WAL_ARCHIVE=MANUAL,DISCARD_PENDING=TRUE","BACKUP WAL TO file:///tmp/mydb/repo","BACKUP WAL TO 'file:///tmp/mydb/repo'; SELECT 1"]{assert!(crate::parser::parse_statement(sql).is_err(),"{sql}");}
}
#[test]
fn standalone_single_base_is_queryable_offline_and_restores_read_only() {
    let root = root();
    let source = engine(&root.path().join("source"));
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v LONGBLOB)")
        .unwrap();
    source
        .execute("INSERT INTO t VALUES(1,REPEAT('abc',10000))")
        .unwrap();
    let coordinator = Coordinator::get(source.db_handle()).unwrap();
    let path = root.path().join("repository");
    let done = job(&coordinator, coordinator.backup(&path).unwrap());
    let id = done.base_id.unwrap();
    assert!(source.db_handle().capture.floor().is_none());
    assert!(coordinator.backup_wal(&path).is_err());
    source.shutdown().unwrap();
    drop(coordinator);
    drop(source);
    fs::remove_dir_all(root.path().join("source")).unwrap();
    let repo = Repository::open(&path, None).unwrap();
    let (_, image) = repo.base(&id).unwrap();
    assert!(image.seal.unwrap().continuation_ready);
    assert!(!path.join("chunks").exists());
    let reader = super::open_base(&path, &id, &root.path().join("runtime")).unwrap();
    let query = StorageQueryEngine::open_backup_image(
        reader.data_dir.clone(),
        root.path().join("runtime"),
        Default::default(),
        Arc::new(crate::engine::session_registry::SessionRegistry::new()),
        vec![],
    )
    .unwrap();
    assert_eq!(
        rows(&query, "SELECT LENGTH(v) FROM t WHERE id=1")[0][0],
        Value::Int(30000)
    );
    assert!(query.execute("DELETE FROM t").is_err());
    query.shutdown().unwrap();
    drop(query);
    let target = root.path().join("restored");
    let report =
        super::restore::restore(&path, Some(&id), None, Target::Base, &target, None, false)
            .unwrap();
    assert!(report.read_only);
    let query = engine(&target);
    assert_eq!(rows(&query, "SELECT COUNT(*) FROM t")[0][0], Value::Int(1));
    assert!(native::promote(&target).is_err());
    query.shutdown().unwrap();
    drop(query);
    native::promote(&target).unwrap();
    let query = engine(&target);
    query.execute("INSERT INTO t VALUES(2,'writable')").unwrap();
    query.shutdown().unwrap();
}
#[test]
fn manual_wal_archive_receipts_and_pitr_do_not_copy_database() {
    let root = root();
    let source = engine(&root.path().join("source"));
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(30),INDEX iv(v))")
        .unwrap();
    source.execute("INSERT INTO t VALUES(1,'base')").unwrap();
    let path = root.path().join("repo");
    let c = Coordinator::get(source.db_handle()).unwrap();
    c.configure(&path, Policy::Manual, false, None).unwrap();
    assert_eq!(
        c.control.lock().unwrap().as_ref().unwrap().lifecycle,
        "WAITING_BASE"
    );
    let base = job(&c, c.backup(&path).unwrap()).base_id.unwrap();
    let stream = c
        .control
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .stream_id
        .clone()
        .unwrap();
    source.execute("UPDATE t SET v='first' WHERE id=1").unwrap();
    let image_reads = IMAGE_READ_BYTES.load(Ordering::Relaxed);
    let prefix_reads = crate::wal::RAW_PREFIX_READ_BYTES.load(Ordering::Relaxed);
    let first = job(&c, c.backup_wal(&path).unwrap());
    assert_eq!(
        IMAGE_READ_BYTES.load(Ordering::Relaxed),
        image_reads,
        "BACKUP WAL must not read image files"
    );
    assert_eq!(
        crate::wal::RAW_PREFIX_READ_BYTES.load(Ordering::Relaxed) - prefix_reads,
        first.bytes,
        "archive must not rescan the baseline WAL prefix"
    );
    assert!(first.requested_lsn > 0 && first.durable_through_lsn >= first.requested_lsn);
    source
        .execute("UPDATE t SET v='second' WHERE id=1")
        .unwrap();
    job(&c, c.backup_wal(&path).unwrap());
    let target = root.path().join("at-first");
    super::restore::restore(
        &path,
        Some(&base),
        Some(&stream),
        Target::Lsn(first.requested_lsn),
        &target,
        None,
        false,
    )
    .unwrap();
    let query = engine(&target);
    assert_eq!(
        rows(&query, "SELECT id FROM t FORCE INDEX(iv) WHERE v='first'")[0][0],
        Value::Int(1)
    );
    query.shutdown().unwrap();
    let target = root.path().join("at-time");
    super::restore::restore(
        &path,
        Some(&base),
        Some(&stream),
        Target::Time(first.requested_source_time),
        &target,
        None,
        false,
    )
    .unwrap();
    let query = engine(&target);
    assert_eq!(
        rows(&query, "SELECT v FROM t WHERE id=1")[0][0],
        Value::String("first".into())
    );
    query.shutdown().unwrap();
    let idle = job(&c, c.backup_wal(&path).unwrap());
    assert!(idle.requested_source_time > first.requested_source_time);
    assert_eq!(fs::read_dir(path.join("bases")).unwrap().count(), 1);
    discard_source(source, c);
}
#[test]
fn prepared_xa_survives_base_and_source_decision() {
    for commit in [true, false] {
        let root = root();
        let source = engine(&root.path().join("source"));
        source
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(30),INDEX iv(v))")
            .unwrap();
        source.execute("INSERT INTO t VALUES(1,'base')").unwrap();
        let mut xa = crate::engine::SessionContext::default();
        for sql in [
            "XA START 'pitr-xa'",
            "UPDATE t SET v='prepared' WHERE id=1",
            "XA END 'pitr-xa'",
            "XA PREPARE 'pitr-xa'",
        ] {
            source.execute_with_session(&mut xa, sql).unwrap();
        }
        let path = root.path().join("repo");
        let c = Coordinator::get(source.db_handle()).unwrap();
        c.configure(&path, Policy::Manual, false, None).unwrap();
        let base = job(&c, c.backup(&path).unwrap()).base_id.unwrap();
        source
            .execute(if commit {
                "XA COMMIT 'pitr-xa'"
            } else {
                "XA ROLLBACK 'pitr-xa'"
            })
            .unwrap();
        let receipt = job(&c, c.backup_wal(&path).unwrap());
        let target = root.path().join("restored");
        super::restore::restore(
            &path,
            Some(&base),
            None,
            Target::Lsn(receipt.requested_lsn),
            &target,
            None,
            false,
        )
        .unwrap();
        let query = engine(&target);
        assert_eq!(
            rows(&query, "SELECT v FROM t WHERE id=1")[0][0],
            Value::String(if commit { "prepared" } else { "base" }.into())
        );
        query.shutdown().unwrap();
        discard_source(source, c);
    }
}
#[test]
fn continuous_manual_stop_and_reenable_use_distinct_streams() {
    let root = root();
    let source = engine(&root.path().join("source"));
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    let c = Coordinator::get(source.db_handle()).unwrap();
    let path = root.path().join("repo");
    c.configure(&path, Policy::Continuous, false, None).unwrap();
    job(&c, c.backup(&path).unwrap());
    let old = c
        .control
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .stream_id
        .clone();
    source.execute("INSERT INTO t VALUES(1)").unwrap();
    let target = source.db_handle().wal_log().lock().last_lsn();
    let deadline = Instant::now() + Duration::from_secs(10);
    while c.control.lock().unwrap().as_ref().unwrap().cursor < target {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(20));
    }
    c.configure(&path, Policy::Manual, false, None).unwrap();
    assert_eq!(c.control.lock().unwrap().as_ref().unwrap().stream_id, old);
    let stop = c
        .configure(&path, Policy::Disabled, false, None)
        .unwrap()
        .unwrap();
    job(&c, stop);
    assert!(source.db_handle().capture.floor().is_none());
    c.configure(&path, Policy::Manual, false, None).unwrap();
    assert_eq!(
        c.control.lock().unwrap().as_ref().unwrap().lifecycle,
        "WAITING_BASE"
    );
    assert!(c.backup_wal(&path).is_err());
    job(&c, c.backup(&path).unwrap());
    assert_ne!(c.control.lock().unwrap().as_ref().unwrap().stream_id, old);
    source.execute("INSERT INTO t VALUES(2)").unwrap();
    job(&c, c.backup_wal(&path).unwrap());
    discard_source(source, c);
}

#[test]
fn manual_restart_undo_compaction_and_savepoint_continue_offline() {
    std::env::set_var("MYDB_UNDO_WRITE_SHARDS", "1");
    std::env::set_var("MYDB_UNDO_COMPACTION", "off");
    let root = root();
    let path = root.path().join("repo");
    let source_path = root.path().join("source");
    let source = engine(&source_path);
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(80), INDEX iv(v))")
        .unwrap();
    source
        .execute("INSERT INTO t VALUES(1,'base'),(2,'stable')")
        .unwrap();
    source.db_handle().set_undo_segment_bytes(512).unwrap();
    source.db_handle().set_undo_retention_ms(0).unwrap();
    let c = Coordinator::get(source.db_handle()).unwrap();
    c.configure(&path, Policy::Manual, false, None).unwrap();
    let base = job(&c, c.backup(&path).unwrap()).base_id.unwrap();
    let mut session = crate::engine::SessionContext::default();
    for sql in [
        "BEGIN",
        "UPDATE t SET v='rolled-back' WHERE id=1",
        "SAVEPOINT s",
        "UPDATE t SET v='discarded' WHERE id=1",
        "ROLLBACK TO SAVEPOINT s",
        "ROLLBACK",
    ] {
        source.execute_with_session(&mut session, sql).unwrap();
    }
    for n in 0..36 {
        source
            .execute(&format!("UPDATE t SET v='version-{n}' WHERE id=1"))
            .unwrap();
    }
    // A pinned XA record shares segments with reclaimable churn. This
    // controls fragmentation rather than depending on background timing.
    for sql in [
        "XA START 'relocation'",
        "UPDATE t SET v='relocated' WHERE id=1",
        "XA END 'relocation'",
        "XA PREPARE 'relocation'",
    ] {
        source.execute_with_session(&mut session, sql).unwrap();
    }
    for key in 1000..1080 {
        source
            .execute(&format!("INSERT INTO t VALUES({key},'churn')"))
            .unwrap();
        source
            .execute(&format!("DELETE FROM t WHERE id={key}"))
            .unwrap();
        source.db_handle().purge_obsolete_undo();
    }
    let moved = source.db_handle().compact_undo_segments(100, 64).unwrap();
    assert!(moved > 0, "must exercise physical relocation");
    source.execute("XA COMMIT 'relocation'").unwrap();
    source.execute("UPDATE t SET v='final' WHERE id=1").unwrap();
    source.db_handle().checkpoint().unwrap();
    source.shutdown().unwrap();
    drop(c);
    drop(source);
    let source = engine(&source_path);
    let c = Coordinator::get(source.db_handle()).unwrap();
    let receipt = job(&c, c.backup_wal(&path).unwrap());
    source.shutdown().unwrap();
    drop(c);
    drop(source);
    fs::remove_dir_all(&source_path).unwrap();
    let target = root.path().join("restored");
    super::restore::restore(
        &path,
        Some(&base),
        None,
        Target::Lsn(receipt.requested_lsn),
        &target,
        None,
        false,
    )
    .unwrap();
    let query = engine(&target);
    assert_eq!(
        rows(&query, "SELECT id,v FROM t FORCE INDEX(iv) ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::String("final".into())],
            vec![Value::Int(2), Value::String("stable".into())]
        ]
    );
    query.shutdown().unwrap();
}

#[test]
fn wal_pressure_rejects_writes_but_archive_and_budget_management_work() {
    let root = root();
    let source = engine(&root.path().join("source"));
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v LONGBLOB)")
        .unwrap();
    let c = Coordinator::get(source.db_handle()).unwrap();
    let path = root.path().join("repo");
    c.configure(&path, Policy::Manual, false, None).unwrap();
    job(&c, c.backup(&path).unwrap());
    c.configure(&path, Policy::Manual, false, Some(64)).unwrap();
    assert!(source
        .execute("INSERT INTO t VALUES(1,REPEAT('x',10000))")
        .is_err());
    assert_eq!(rows(&source, "SELECT COUNT(*) FROM t")[0][0], Value::Int(0));
    job(&c, c.backup_wal(&path).unwrap());
    c.configure(&path, Policy::Manual, false, Some(64 * 1024 * 1024))
        .unwrap();
    source.execute("INSERT INTO t VALUES(2,'ok')").unwrap();
    job(&c, c.backup_wal(&path).unwrap());
    discard_source(source, c);
}

#[test]
fn idle_fences_are_bounded_and_export_import_is_self_contained() {
    let root = root();
    let source = engine(&root.path().join("source"));
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(20))")
        .unwrap();
    source.execute("INSERT INTO t VALUES(1,'base')").unwrap();
    let path = root.path().join("repo");
    let c = Coordinator::get(source.db_handle()).unwrap();
    c.configure(&path, Policy::Manual, false, None).unwrap();
    let base = job(&c, c.backup(&path).unwrap()).base_id.unwrap();
    source
        .execute("UPDATE t SET v='archived' WHERE id=1")
        .unwrap();
    job(&c, c.backup_wal(&path).unwrap());
    c.test_reversed_idle_targets().unwrap();
    let repo = Repository::open(&path, None).unwrap();
    let before = repo.position().unwrap();
    let mut last = None;
    for _ in 0..8 {
        last = Some(job(&c, c.backup_wal(&path).unwrap()));
    }
    assert_eq!(repo.position().unwrap(), before);
    let receipt = last.unwrap();
    let stream = c
        .control
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .stream_id
        .clone()
        .unwrap();
    assert_eq!(
        fs::read_dir(path.join("streams").join(&stream).join("fences"))
            .unwrap()
            .count(),
        1
    );
    let bundle = root.path().join("offline.bundle");
    super::management::export(
        &path,
        Some(&base),
        None,
        Target::Time(receipt.requested_source_time),
        &bundle,
        None,
    )
    .unwrap();
    source.shutdown().unwrap();
    drop(c);
    drop(source);
    fs::remove_dir_all(root.path().join("source")).unwrap();
    fs::remove_dir_all(&path).unwrap();
    let imported = root.path().join("imported");
    super::management::import(&imported, &bundle, None).unwrap();
    let target = root.path().join("restored");
    super::restore::restore(
        &imported,
        Some(&base),
        None,
        Target::Time(receipt.requested_source_time),
        &target,
        None,
        false,
    )
    .unwrap();
    let query = engine(&target);
    assert_eq!(
        rows(&query, "SELECT v FROM t WHERE id=1")[0][0],
        Value::String("archived".into())
    );
    query.shutdown().unwrap();
    let bytes = fs::read(&bundle).unwrap();
    let corrupt = root.path().join("truncated.bundle");
    fs::write(&corrupt, &bytes[..bytes.len() - 1]).unwrap();
    let bad = root.path().join("bad");
    assert!(super::management::import(&bad, &corrupt, None).is_err());
    assert!(!bad.exists());
}

fn terminal(c: &Arc<Coordinator>, id: &str) -> super::coordinator::Job {
    let until = Instant::now() + Duration::from_secs(60);
    loop {
        let value = c.jobs().into_iter().find(|j| j.id == id).unwrap();
        if value.state != "RUNNING" {
            return value;
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(10));
    }
}
#[test]
fn publication_interruptions_reconcile_without_releasing_protection_early() {
    let root = root();
    let source = engine(&root.path().join("source"));
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    source.execute("INSERT INTO t VALUES(1,10)").unwrap();
    let path = root.path().join("repo");
    let c = Coordinator::get(source.db_handle()).unwrap();
    c.configure(&path, Policy::Manual, false, None).unwrap();
    FAIL_SNAPSHOT_SYNC.store(true, Ordering::SeqCst);
    let failed = c.backup(&path).unwrap();
    assert_eq!(terminal(&c, &failed.id).state, "FAILED");
    assert!(source.db_handle().capture.floor().is_none());
    assert_eq!(
        c.control.lock().unwrap().as_ref().unwrap().lifecycle,
        "WAITING_BASE"
    );
    super::repository::FAIL_AFTER_BASE_RENAME.store(true, Ordering::SeqCst);
    let interrupted = c.backup(&path).unwrap();
    assert_eq!(terminal(&c, &interrupted.id).state, "FAILED");
    assert_eq!(
        c.control.lock().unwrap().as_ref().unwrap().lifecycle,
        "ACTIVE"
    );
    assert!(source.db_handle().capture.floor().is_some());
    let base = interrupted.base_id.unwrap();
    Repository::open(&path, None).unwrap().base(&base).unwrap();
    for point in [2, 3] {
        source
            .execute(&format!("UPDATE t SET v={} WHERE id=1", point * 10))
            .unwrap();
        super::coordinator::TEST_FAULT.store(point, Ordering::SeqCst);
        let request = c.backup_wal(&path).unwrap();
        let result = terminal(&c, &request.id);
        assert_eq!(result.state, "FAILED");
        assert!(result.durable_through_lsn >= request.requested_lsn);
        assert_eq!(
            source.db_handle().capture.leases()["archive"].floor,
            result.durable_through_lsn + 1
        );
    }
    let receipt = job(&c, c.backup_wal(&path).unwrap());
    let target = root.path().join("restored");
    super::restore::restore(
        &path,
        Some(&base),
        None,
        Target::Lsn(receipt.requested_lsn),
        &target,
        None,
        false,
    )
    .unwrap();
    let query = engine(&target);
    assert_eq!(rows(&query, "SELECT v FROM t")[0][0], Value::Int(30));
    query.shutdown().unwrap();
    discard_source(source, c);
}

#[test]
fn encrypted_wal_authentication_key_identity_and_offline_bundle() {
    let root = root();
    let key = root.path().join("keys");
    fs::write(&key, "42".repeat(32)).unwrap();
    let wrong = root.path().join("wrong");
    fs::write(&wrong, "24".repeat(32)).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for path in [&key, &wrong] {
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
    }
    std::env::set_var("MYDB_BACKUP_KEY_FILE", &key);
    let path = root.path().join("repo");
    Repository::init_with_encoding(&path, Some("key"), Some(&key), "NONE").unwrap();
    let source = engine(&root.path().join("source"));
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v LONGBLOB)")
        .unwrap();
    source.execute("INSERT INTO t VALUES(1,'base')").unwrap();
    let c = Coordinator::get(source.db_handle()).unwrap();
    c.configure(&path, Policy::Manual, false, None).unwrap();
    let base = job(&c, c.backup(&path).unwrap()).base_id.unwrap();
    source
        .execute("UPDATE t SET v=REPEAT('secret',10000) WHERE id=1")
        .unwrap();
    job(&c, c.backup_wal(&path).unwrap());
    assert!(Repository::open(&path, Some(&wrong)).is_err());
    let repo = Repository::open(&path, Some(&key)).unwrap();
    repo.base(&base).unwrap();
    let mut reference = None;
    repo.visit_live(&repo.position().unwrap(), |_, event| {
        if let super::repository::Publication::Range { range } = event {
            reference = range.chunks.first().cloned();
        }
        Ok(())
    })
    .unwrap();
    let reference = reference.unwrap();
    let bytes = repo.read_blob(&reference).unwrap();
    assert!(!bytes.is_empty());
    let blob = path.join("blobs/key").join(&reference.hash);
    let original = fs::read(&blob).unwrap();
    assert!(!original.windows(6).any(|b| b == b"secret"));
    let mut corrupt = original.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(&blob, &corrupt).unwrap();
    assert!(repo.read_blob(&reference).is_err());
    fs::write(&blob, &original).unwrap();
    let bundle = root.path().join("encrypted.bundle");
    super::management::export(
        &path,
        Some(&base),
        None,
        Target::Latest,
        &bundle,
        Some(&key),
    )
    .unwrap();
    source.shutdown().unwrap();
    drop(c);
    drop(source);
    fs::remove_dir_all(root.path().join("source")).unwrap();
    fs::remove_dir_all(&path).unwrap();
    let imported = root.path().join("imported");
    assert!(super::management::import(&imported, &bundle, Some(&wrong)).is_err());
    assert!(!imported.exists());
    super::management::import(&imported, &bundle, Some(&key)).unwrap();
    let target = root.path().join("restored");
    super::restore::restore(
        &imported,
        Some(&base),
        None,
        Target::Latest,
        &target,
        Some(&key),
        false,
    )
    .unwrap();
    let query = engine(&target);
    assert_eq!(
        rows(&query, "SELECT LENGTH(v),SHA2(v,256) FROM t WHERE id=1")[0][0],
        Value::Int(60000)
    );
    query.shutdown().unwrap();
}

#[test]
fn prepared_insert_delete_and_promoted_local_xa_decision_remain_isolated() {
    for commit in [true, false] {
        let root = root();
        let source = engine(&root.path().join("source"));
        source
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(30),INDEX iv(v))")
            .unwrap();
        source
            .execute("INSERT INTO t VALUES(1,'old'),(2,'deleted')")
            .unwrap();
        let mut xa = crate::engine::SessionContext::default();
        for sql in [
            "XA START 'local'",
            "INSERT INTO t VALUES(3,'pending')",
            "DELETE FROM t WHERE id=2",
            "XA END 'local'",
            "XA PREPARE 'local'",
        ] {
            source.execute_with_session(&mut xa, sql).unwrap();
        }
        let path = root.path().join("repo");
        let c = Coordinator::get(source.db_handle()).unwrap();
        let base = job(&c, c.backup(&path).unwrap()).base_id.unwrap();
        let target = root.path().join("restored");
        super::restore::restore(&path, Some(&base), None, Target::Base, &target, None, false)
            .unwrap();
        let query = engine(&target);
        assert_eq!(
            rows(&query, "SELECT id FROM t ORDER BY id"),
            vec![vec![Value::Int(1)], vec![Value::Int(2)]]
        );
        query.shutdown().unwrap();
        drop(query);
        native::promote(&target).unwrap();
        let query = engine(&target);
        query
            .execute(if commit {
                "XA COMMIT 'local'"
            } else {
                "XA ROLLBACK 'local'"
            })
            .unwrap();
        assert_eq!(
            rows(&query, "SELECT id FROM t ORDER BY id"),
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(if commit { 3 } else { 2 })]
            ]
        );
        query.shutdown().unwrap();
        discard_source(source, c);
    }
}

#[test]
fn durable_draining_restart_keeps_the_original_end_boundary() {
    let root = root();
    let source_path = root.path().join("source");
    let repository = root.path().join("repo");
    let source = engine(&source_path);
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(30))")
        .unwrap();
    source.execute("INSERT INTO t VALUES(1,'base')").unwrap();
    let c = Coordinator::get(source.db_handle()).unwrap();
    c.configure(&repository, Policy::Manual, false, None)
        .unwrap();
    let base = job(&c, c.backup(&repository).unwrap()).base_id.unwrap();
    source
        .execute("UPDATE t SET v='stop-boundary' WHERE id=1")
        .unwrap();
    let (end, _) = c.test_begin_drain().unwrap();
    source
        .execute("UPDATE t SET v='after-boundary' WHERE id=1")
        .unwrap();
    source.shutdown().unwrap();
    drop(c);
    drop(source);
    let source = engine(&source_path);
    let c = Coordinator::get(source.db_handle()).unwrap();
    let until = Instant::now() + Duration::from_secs(30);
    while c.control.lock().unwrap().as_ref().unwrap().lifecycle != "CLOSED" {
        assert!(Instant::now() < until, "{:?}", c.jobs());
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(c.control.lock().unwrap().as_ref().unwrap().cursor, end);
    assert!(source.db_handle().capture.floor().is_none());
    let target = root.path().join("restored");
    super::restore::restore(
        &repository,
        Some(&base),
        None,
        Target::Latest,
        &target,
        None,
        false,
    )
    .unwrap();
    let query = engine(&target);
    assert_eq!(
        rows(&query, "SELECT v FROM t")[0][0],
        Value::String("stop-boundary".into())
    );
    query.shutdown().unwrap();
    discard_source(source, c);
}

#[test]
fn cli_validation_never_initializes_unknown_or_legacy_directories() {
    let root = root();
    let repository = root.path().join("missing");
    for command in ["list", "verify", "restore", "prune"] {
        assert!(super::cli::run(vec![
            command.into(),
            "--repository".into(),
            repository.to_str().unwrap().into()
        ])
        .is_err());
        assert!(!repository.exists());
    }
    for options in [
        vec!["list", "--repository", "/tmp/mydb/x", "--backup", "latest"],
        vec![
            "init",
            "--repository",
            "/tmp/mydb/x",
            "--repository",
            "/tmp/mydb/y",
        ],
        vec![
            "restore",
            "--repository",
            "/tmp/mydb/x",
            "--target",
            "latest",
            "--target-lsn",
            "1",
        ],
        vec![
            "init",
            "--repository",
            "/tmp/mydb/x",
            "--wal-encoding",
            "BAD",
        ],
    ] {
        assert!(super::cli::run(options.into_iter().map(str::to_owned).collect()).is_err());
    }
    fs::create_dir(&repository).unwrap();
    fs::write(repository.join("legacy-user-data"), b"preserve").unwrap();
    assert!(Repository::init(&repository, None, None).is_err());
    assert_eq!(
        fs::read(repository.join("legacy-user-data")).unwrap(),
        b"preserve"
    );
}

#[test]
fn base_target_ignores_bad_tail_and_batch_targets_are_atomic() {
    let root = root();
    let source = engine(&root.path().join("source"));
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    source.execute("INSERT INTO t VALUES(1,10)").unwrap();
    let c = Coordinator::get(source.db_handle()).unwrap();
    let repository = root.path().join("repo");
    c.configure(&repository, Policy::Manual, false, None)
        .unwrap();
    let base = job(&c, c.backup(&repository).unwrap()).base_id.unwrap();
    source.execute("INSERT INTO t VALUES(2,20),(3,30)").unwrap();
    job(&c, c.backup_wal(&repository).unwrap());
    let repo = Repository::open(&repository, None).unwrap();
    let mut selected = None;
    repo.visit_live(&repo.position().unwrap(), |_, event| {
        if let super::repository::Publication::Range { range } = event {
            selected = Some(range);
        }
        Ok(())
    })
    .unwrap();
    let range = selected.unwrap();
    let mut split = None;
    super::restore::visit_range(&repo, &range, |first, frame, _| {
        if first < frame.header.lsn {
            split = Some(first);
        }
        Ok(())
    })
    .unwrap();
    if let Some(lsn) = split {
        let bad = root.path().join("split");
        assert!(super::restore::restore(
            &repository,
            Some(&base),
            None,
            Target::Lsn(lsn),
            &bad,
            None,
            false
        )
        .is_err());
        assert!(!bad.exists());
    }
    let blob = repository.join("blobs/plain").join(&range.chunks[0].hash);
    let mut bytes = fs::read(&blob).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(&blob, &bytes).unwrap();
    let exact = root.path().join("exact");
    super::restore::restore(
        &repository,
        Some(&base),
        None,
        Target::Base,
        &exact,
        None,
        false,
    )
    .unwrap();
    let query = engine(&exact);
    assert_eq!(
        rows(&query, "SELECT id,v FROM t"),
        vec![vec![Value::Int(1), Value::Int(10)]]
    );
    query.shutdown().unwrap();
    let bad = root.path().join("bad");
    assert!(super::restore::restore(
        &repository,
        Some(&base),
        None,
        Target::Latest,
        &bad,
        None,
        false
    )
    .is_err());
    assert!(!bad.exists());
    discard_source(source, c);
}

#[test]
fn missing_acknowledged_wal_is_broken_after_source_restart() {
    let root = root();
    let source_path = root.path().join("source");
    let repository = root.path().join("repo");
    let source = engine(&source_path);
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    let c = Coordinator::get(source.db_handle()).unwrap();
    c.configure(&repository, Policy::Manual, false, None)
        .unwrap();
    job(&c, c.backup(&repository).unwrap());
    source.execute("INSERT INTO t VALUES(1)").unwrap();
    job(&c, c.backup_wal(&repository).unwrap());
    let repo = Repository::open(&repository, None).unwrap();
    let mut blob = None;
    repo.visit_live(&repo.position().unwrap(), |_, event| {
        if let super::repository::Publication::Range { range } = event {
            blob = Some(repository.join("blobs/plain").join(&range.chunks[0].hash));
        }
        Ok(())
    })
    .unwrap();
    source.shutdown().unwrap();
    drop(c);
    drop(source);
    fs::remove_file(blob.unwrap()).unwrap();
    let source = engine(&source_path);
    let c = Coordinator::get(source.db_handle()).unwrap();
    assert_eq!(
        c.control.lock().unwrap().as_ref().unwrap().lifecycle,
        "BROKEN"
    );
    assert!(source.db_handle().capture.floor().is_some());
    assert!(c.backup_wal(&repository).is_err());
    c.configure(&repository, Policy::Disabled, true, None)
        .unwrap();
    assert!(source.db_handle().capture.floor().is_none());
    discard_source(source, c);
}

#[test]
fn sequence_frame_at_exactly_e_plus_one_survives_a_larger_local_checkpoint() {
    std::env::set_var("MYDB_SEQUENCE_WAL_BATCH", "1");
    let root = root();
    let source = engine(&root.path().join("source"));
    source.execute("CREATE SEQUENCE s CACHE 1").unwrap();
    let def = source.db_handle().resolve_sequence("test", "s").unwrap();
    let base = root.path().join("base");
    let mut first = None;
    let manifest = capture_database(
        source.db_handle(),
        &BackupRequest {
            repository: base.clone(),
        },
        &random_uuid(),
        &AtomicBool::new(false),
        |_, _, _, _| {},
        |end, _, _| {
            source.db_handle().sequence_setval(&def, 9000, false)?;
            let target = source.db_handle().wal_log().lock().last_lsn();
            assert_eq!(target, end + 1);
            first = Some(target);
            Ok(())
        },
    )
    .unwrap();
    let image = native::verify(&base).unwrap();
    assert!(image.seal.as_ref().unwrap().image_checkpoint > manifest.end_lsn);
    let target_lsn = first.unwrap();
    let prefix = snapshot_wal_files(source.db_handle()).unwrap();
    let target = root.path().join("target");
    let mut source_time = 0;
    for file in &prefix.files {
        crate::wal::visit_raw_prefix(&file.file.open().unwrap(), file.len, |_, last, raw| {
            if last == target_lsn {
                source_time = crate::wal::decode_frame_for_replication(raw)?
                    .header
                    .source_time_us;
            }
            Ok(())
        })
        .unwrap();
    }
    native::restore_image(&base, &target, target_lsn, source_time, false, |log| {
        for file in &prefix.files {
            crate::wal::visit_raw_prefix(
                &file.file.open().unwrap(),
                file.len,
                |first, last, raw| {
                    if last > manifest.end_lsn && last <= target_lsn {
                        assert_eq!(first, last);
                        let frame = crate::wal::decode_frame_for_replication(raw)?;
                        log.append_replication_frame(raw, &frame)?;
                    }
                    Ok(())
                },
            )?;
        }
        Ok(())
    })
    .unwrap();
    native::promote(&target).unwrap();
    let query = engine(&target);
    assert_eq!(rows(&query, "SELECT NEXTVAL('s')")[0][0], Value::Int(9000));
    query.shutdown().unwrap();
    source.shutdown().unwrap();
}

#[test]
fn retention_preserves_window_predecessor_and_acknowledged_head() {
    let root = root();
    let source = engine(&root.path().join("source"));
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    let repository = root.path().join("repo");
    let c = Coordinator::get(source.db_handle()).unwrap();
    c.configure(&repository, Policy::Manual, false, None)
        .unwrap();
    let first = job(&c, c.backup(&repository).unwrap()).base_id.unwrap();
    job(&c, c.backup_wal(&repository).unwrap());
    let initial = c.control.lock().unwrap().as_ref().unwrap().covered_time;
    let day = 86_400_000_000u64;
    source
        .db_handle()
        .wal_log()
        .lock()
        .source_clock()
        .fetch_max(initial + 2 * day, Ordering::SeqCst);
    let predecessor = job(&c, c.backup(&repository).unwrap()).base_id.unwrap();
    job(&c, c.backup_wal(&repository).unwrap());
    source
        .db_handle()
        .wal_log()
        .lock()
        .source_clock()
        .fetch_max(initial + 10 * day, Ordering::SeqCst);
    let latest = job(&c, c.backup(&repository).unwrap()).base_id.unwrap();
    job(&c, c.backup_wal(&repository).unwrap());
    let stream = c
        .control
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .stream_id
        .clone()
        .unwrap();
    super::management::prune(&repository, Some(&stream), Some("7d"), false, None).unwrap();
    assert!(repository.join("bases").join(&first).exists());
    super::management::prune(&repository, Some(&stream), Some("7d"), true, None).unwrap();
    assert!(!repository.join("bases").join(&first).exists());
    for id in [&predecessor, &latest] {
        assert!(repository.join("bases").join(id).exists());
    }
    Repository::open(&repository, None)
        .unwrap()
        .verify_coverage(
            &Repository::open(&repository, None)
                .unwrap()
                .position()
                .unwrap(),
        )
        .unwrap();
    let target = root.path().join("target");
    super::restore::restore(
        &repository,
        Some(&predecessor),
        None,
        Target::Latest,
        &target,
        None,
        false,
    )
    .unwrap();
    let query = engine(&target);
    assert!(rows(&query, "SELECT id FROM t").is_empty());
    query.shutdown().unwrap();
    discard_source(source, c);
}

#[test]
fn reader_that_begins_during_capture_can_write_after_e_and_be_a_pitr_loser() {
    std::env::set_var("MYDB_WAL_TXN_BUFFER", "0");
    let root = root();
    let source = engine(&root.path().join("source"));
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(30))")
        .unwrap();
    source.execute("INSERT INTO t VALUES(1,'base')").unwrap();
    let mut session = crate::engine::SessionContext::default();
    let mut begun = false;
    let base = root.path().join("base");
    let manifest = capture_database(
        source.db_handle(),
        &BackupRequest {
            repository: base.clone(),
        },
        &random_uuid(),
        &AtomicBool::new(false),
        |phase, _, _, _| {
            if phase == "COPY_DATA" && !begun {
                source.execute_with_session(&mut session, "BEGIN").unwrap();
                source
                    .execute_with_session(&mut session, "SELECT v FROM t")
                    .unwrap();
                begun = true;
            }
        },
        |end, _, _| {
            source.db_handle().capture.set_lease(
                "test-reader",
                Some(crate::storage::capture::WalLease {
                    history_retention_ms: source.db_handle().undo_retention_ms(),
                    floor: end + 1,
                    proof: "reader continuation regression".into(),
                }),
            )
        },
    )
    .unwrap();
    assert!(begun);
    source
        .execute_with_session(&mut session, "UPDATE t SET v='uncommitted' WHERE id=1")
        .unwrap();
    let prefix = snapshot_wal_files(source.db_handle()).unwrap();
    let target = root.path().join("target");
    native::restore_image(
        &base,
        &target,
        prefix.last_lsn,
        prefix.captured_unix_us,
        false,
        |log| {
            for file in &prefix.files {
                crate::wal::visit_raw_prefix(
                    &file.file.open().unwrap(),
                    file.len,
                    |first, last, raw| {
                        if last > manifest.end_lsn {
                            assert!(first > manifest.end_lsn);
                            let frame = crate::wal::decode_frame_for_replication(raw)?;
                            log.append_replication_frame(raw, &frame)?;
                        }
                        Ok(())
                    },
                )?;
            }
            Ok(())
        },
    )
    .unwrap();
    let query = engine(&target);
    assert_eq!(
        rows(&query, "SELECT v FROM t")[0][0],
        Value::String("base".into())
    );
    query.shutdown().unwrap();
    source
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    source
        .db_handle()
        .capture
        .set_lease("test-reader", None)
        .unwrap();
    source.shutdown().unwrap();
}

#[test]
fn source_restart_restores_protected_history_policy_and_rejects_memory_mode() {
    let root = root();
    let source_path = root.path().join("source");
    let repository = root.path().join("repo");
    let source = engine(&source_path);
    source.db_handle().set_undo_retention_ms(600_000).unwrap();
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    source.execute("INSERT INTO t VALUES(1,10)").unwrap();
    let c = Coordinator::get(source.db_handle()).unwrap();
    c.configure(&repository, Policy::Manual, false, None)
        .unwrap();
    job(&c, c.backup(&repository).unwrap());
    assert!(source.db_handle().set_undo_retention_ms(0).is_err());
    source.shutdown().unwrap();
    drop(c);
    drop(source);
    let mut config = StorageConfig::from_env();
    config.durability = DurabilityMode::Memory;
    assert!(Database::open_with_config(&source_path, config).is_err());
    let source = engine(&source_path);
    assert_eq!(source.db_handle().undo_retention_ms(), 600_000);
    let c = Coordinator::get(source.db_handle()).unwrap();
    source.execute("UPDATE t SET v=20 WHERE id=1").unwrap();
    job(&c, c.backup_wal(&repository).unwrap());
    discard_source(source, c);
}

#[test]
fn prepared_vectors_survive_sealing_local_decisions_and_interrupted_rebuilds() {
    std::env::set_var("MYDB_HNSW_BRUTE_THRESHOLD", "0");
    std::env::set_var("MYDB_HNSW_BRUTE_FILTER_ROWS", "0");
    let root = root();
    let sync = crate::platform::fs::FixtureSyncBatch::new(root.path()).unwrap();
    let source = engine(&root.path().join("source"));
    let mut cases = Vec::new();
    // Three nodes and exhaustive effort isolate MVCC from approximate recall.
    for (kind, committed_id) in [(0, 2), (1, 3), (2, 2), (3, 4)] {
        let name = format!("v_{kind}");
        source
            .execute(&format!(
                "CREATE TABLE {name}(id INT PRIMARY KEY,emb VECTOR(3) NOT NULL)"
            ))
            .unwrap();
        source
            .execute(&format!(
                "INSERT INTO {name} VALUES(1,'[10,0,0]'),(2,'[20,0,0]'),(3,'[30,0,0]')"
            ))
            .unwrap();
        source
            .execute(&format!("CREATE INDEX vi ON {name}(emb) USING HNSW"))
            .unwrap();
        let query = format!(
            "SELECT /*+ HNSW_EF(1024) */ id FROM {name} ORDER BY DISTANCE(emb,'[0,0,0]') LIMIT 1"
        );
        assert_eq!(rows(&source, &query)[0][0], Value::Int(1));
        let change = match kind {
            0 => format!("UPDATE {name} SET emb='[100000,0,0]' WHERE id=1"),
            1 => format!("UPDATE {name} SET emb='[0,0,0]' WHERE id=3"),
            2 => format!("DELETE FROM {name} WHERE id=1"),
            _ => format!("INSERT INTO {name} VALUES(4,'[0,0,0]')"),
        };
        cases.push((name, change, query, committed_id));
    }
    let mut xa = crate::engine::SessionContext::default();
    source
        .execute_with_session(&mut xa, "XA START 'vector-snapshot'")
        .unwrap();
    for (_, change, _, _) in &cases {
        source.execute_with_session(&mut xa, change).unwrap();
    }
    source
        .execute_with_session(&mut xa, "XA END 'vector-snapshot'")
        .unwrap();
    source
        .execute_with_session(&mut xa, "XA PREPARE 'vector-snapshot'")
        .unwrap();
    for (_, _, query, _) in &cases {
        assert_eq!(rows(&source, query)[0][0], Value::Int(1));
    }
    let repository = root.path().join("repo");
    let c = Coordinator::get(source.db_handle()).unwrap();
    let base = job(&c, c.backup(&repository).unwrap()).base_id.unwrap();
    let template = root.path().join("restored-template");
    super::restore::restore(
        &repository,
        Some(&base),
        None,
        Target::Base,
        &template,
        None,
        false,
    )
    .unwrap();
    let image = engine(&template);
    for (_, _, query, _) in &cases {
        assert_eq!(rows(&image, query)[0][0], Value::Int(1));
    }
    let xid = crate::xa::Xid::new(1, b"vector-snapshot".to_vec(), vec![]).unwrap();
    assert!(image.db_handle().commit_prepared(&xid).is_err());
    assert!(image.db_handle().rollback_prepared(&xid).is_err());
    image.shutdown().unwrap();
    drop(image);
    sync.finish().unwrap();
    let assert_state = |db: &StorageQueryEngine, commit: bool| {
        for (name, _, query, committed_id) in &cases {
            let id = if commit { *committed_id } else { 1 };
            assert_eq!(
                rows(db, query)[0][0],
                Value::Int(id),
                "{name}; commit={commit}"
            );
            // Check the index itself so an executor underfill fallback cannot
            // conceal a missing typed mutation in the shared decision.
            let handle = db.db_handle();
            let table = handle.table(name).unwrap();
            let index = table
                .indexes
                .iter()
                .find(|index| index.is_vector())
                .unwrap();
            let graph = handle.vector_graph_get_or_build(&table, index).unwrap();
            let query = crate::engine::vector::codec::parse_text("[0,0,0]").unwrap();
            let result = graph
                .search(
                    &query,
                    crate::engine::vector::VectorSearchRequest {
                        candidate_limit: 1,
                        rerank_limit: 1,
                        effort: crate::engine::vector::VectorSearchEffort::Hnsw { ef: 1024 },
                    },
                )
                .unwrap();
            let expected = handle
                .scan_rows(
                    &table.qualified_name(),
                    crate::storage::TxnView::read_only(
                        u64::MAX,
                        crate::storage::IsolationLevel::ReadCommitted,
                    ),
                )
                .unwrap()
                .into_iter()
                .find(|row| row.values.first() == Some(&Value::Int(id)))
                .unwrap()
                .row_id;
            assert_eq!(
                result.candidates[0].0, expected,
                "raw index {name}; commit={commit}"
            );
        }
    };
    for commit in [false, true] {
        for interrupt in [false, true] {
            let target = root.path().join(format!("restore-{commit}-{interrupt}"));
            crate::util::file::copy_fixture_tree(&template, &target).unwrap();
            native::promote(&target).unwrap();
            let writable = engine(&target);
            for (_, _, query, _) in &cases {
                assert_eq!(rows(&writable, query)[0][0], Value::Int(1));
            }
            if interrupt {
                writable.db_handle().test_interrupt_vector_completion();
            }
            let result = writable.execute(if commit {
                "XA COMMIT 'vector-snapshot'"
            } else {
                "XA ROLLBACK 'vector-snapshot'"
            });
            if interrupt {
                assert!(result.is_err());
                assert_eq!(
                    fs::read_dir(target.join("system/vector-completion"))
                        .unwrap()
                        .count(),
                    cases.len(),
                    "every affected index table needs a completion intent"
                );
            } else {
                result.unwrap();
                assert_state(&writable, commit);
            }
            drop(writable);
            let reopened = engine(&target);
            assert_state(&reopened, commit);
            assert!(fs::read_dir(target.join("system/vector-completion"))
                .unwrap()
                .next()
                .is_none());
            drop(reopened);
        }
    }
    source.execute("XA ROLLBACK 'vector-snapshot'").unwrap();
    c.shutdown().unwrap();
}

#[test]
fn closed_intent_releases_a_crash_leftover_archive_lease_before_recovery() {
    let root = root();
    let source_path = root.path().join("source");
    let repository = root.path().join("repo");
    let source = engine(&source_path);
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    let c = Coordinator::get(source.db_handle()).unwrap();
    c.configure(&repository, Policy::Manual, false, None)
        .unwrap();
    job(&c, c.backup(&repository).unwrap());
    c.test_closed_before_lease_release().unwrap();
    assert!(source.db_handle().capture.floor().is_some());
    source.shutdown().unwrap();
    drop(c);
    drop(source);
    let startup = prepare_archive_retention(&source_path).unwrap();
    assert!(
        crate::storage::capture::CaptureState::open(&source_path, false)
            .unwrap()
            .floor()
            .is_none()
    );
    let source = engine(&source_path);
    drop(startup);
    assert!(source.db_handle().capture.floor().is_none());
    source.execute("INSERT INTO t VALUES(1)").unwrap();
    source.shutdown().unwrap();
}

#[test]
fn missing_lease_registry_cannot_run_recovery_for_an_active_source() {
    let root = root();
    let source_path = root.path().join("source");
    let repository = root.path().join("repo");
    let source = engine(&source_path);
    let c = Coordinator::get(source.db_handle()).unwrap();
    c.configure(&repository, Policy::Manual, false, None)
        .unwrap();
    job(&c, c.backup(&repository).unwrap());
    source.shutdown().unwrap();
    drop(c);
    drop(source);
    fs::remove_file(source_path.join("system/wal-retention.json")).unwrap();
    fs::remove_file(source_path.join("system/wal-retention.id")).unwrap();
    let mut before = BTreeMap::new();
    for entry in fs::read_dir(source_path.join("system/wal")).unwrap() {
        let path = entry.unwrap().path();
        before.insert(path.clone(), fs::read(path).unwrap());
    }
    assert!(prepare_archive_retention(&source_path).is_err());
    assert!(StorageQueryEngine::open(&source_path).is_err());
    for (path, bytes) in before {
        assert_eq!(
            fs::read(path).unwrap(),
            bytes,
            "startup changed WAL without its required durable lease"
        );
    }
}

#[test]
fn full_backup_accepts_a_relative_source_and_rejects_repository_alias_opens() {
    let root = root();
    let old = std::env::current_dir().unwrap();
    struct Cwd(PathBuf);
    impl Drop for Cwd {
        fn drop(&mut self) {
            std::env::set_current_dir(&self.0).unwrap();
        }
    }
    let _cwd = Cwd(old);
    std::env::set_current_dir(root.path()).unwrap();
    let source = engine(Path::new("source"));
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    source.execute("INSERT INTO t VALUES(1)").unwrap();
    let repository = root.path().join("repo");
    let c = Coordinator::get(source.db_handle()).unwrap();
    let base = job(&c, c.backup(&repository).unwrap()).base_id.unwrap();
    #[cfg(unix)]
    {
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(repository.join("bases").join(&base).join("data"), &alias)
            .unwrap();
        assert!(StorageQueryEngine::open(&alias).is_err());
    }
    discard_source(source, c);
}

#[test]
fn export_requested_time_window_imports_without_the_source() {
    let root = root();
    let source = engine(&root.path().join("source"));
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    source.execute("INSERT INTO t VALUES(1,10)").unwrap();
    let c = Coordinator::get(source.db_handle()).unwrap();
    let repository = root.path().join("repo");
    c.configure(&repository, Policy::Manual, false, None)
        .unwrap();
    job(&c, c.backup(&repository).unwrap());
    source.execute("UPDATE t SET v=20 WHERE id=1").unwrap();
    job(&c, c.backup_wal(&repository).unwrap());
    let bundle = root.path().join("window.bundle");
    super::management::export_window(&repository, None, "7d", &bundle, None).unwrap();
    source.shutdown().unwrap();
    drop(c);
    drop(source);
    fs::remove_dir_all(&repository).unwrap();
    fs::remove_dir_all(root.path().join("source")).unwrap();
    let imported = root.path().join("imported");
    super::management::import(&imported, &bundle, None).unwrap();
    let restored = root.path().join("restored");
    super::restore::restore(
        &imported,
        None,
        None,
        Target::Latest,
        &restored,
        None,
        false,
    )
    .unwrap();
    let query = engine(&restored);
    assert_eq!(rows(&query, "SELECT v FROM t")[0][0], Value::Int(20));
    query.shutdown().unwrap();
}

#[test]
fn full_publication_parallel_to_archive_keeps_the_newest_base_status() {
    let root = root();
    let source = engine(&root.path().join("source"));
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    let repository = root.path().join("repo");
    let c = Coordinator::get(source.db_handle()).unwrap();
    c.configure(&repository, Policy::Manual, false, None)
        .unwrap();
    job(&c, c.backup(&repository).unwrap());
    source.execute("INSERT INTO t VALUES(1)").unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    *super::coordinator::ARCHIVE_SNAPSHOT_HOOK.lock().unwrap() = Some(Box::new(move || {
        ready_tx.send(()).unwrap();
        resume_rx.recv().unwrap();
    }));
    let request = c.backup_wal(&repository).unwrap();
    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let newer = job(&c, c.backup(&repository).unwrap()).base_id.unwrap();
    resume_tx.send(()).unwrap();
    job(&c, request);
    assert_eq!(
        c.control
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .latest_base
            .as_deref(),
        Some(newer.as_str())
    );
    discard_source(source, c);
}

#[test]
fn failed_idle_fence_does_not_advance_the_source_receipt() {
    let root = root();
    let source = engine(&root.path().join("source"));
    let repository = root.path().join("repo");
    let c = Coordinator::get(source.db_handle()).unwrap();
    c.configure(&repository, Policy::Manual, false, None)
        .unwrap();
    job(&c, c.backup(&repository).unwrap());
    job(&c, c.backup_wal(&repository).unwrap());
    let old = c.control.lock().unwrap().as_ref().unwrap().covered_time;
    super::coordinator::TEST_FAULT.store(4, Ordering::SeqCst);
    let request = c.backup_wal(&repository).unwrap();
    let result = terminal(&c, &request.id);
    assert_eq!(result.state, "FAILED");
    assert_eq!(result.covered_time, old);
    assert!(result.covered_time < request.requested_source_time);
    let done = job(&c, c.backup_wal(&repository).unwrap());
    assert!(done.covered_time >= done.requested_source_time);
    discard_source(source, c);
}

#[test]
fn startup_retention_guard_supports_an_absent_source_directory() {
    let root = root();
    let source_path = root.path().join("new/source");
    assert!(!source_path.exists());
    let guard = super::prepare_archive_retention(&source_path).unwrap();
    assert!(source_path.join("system/mydb.lock").is_file());
    let source = engine(&source_path);
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    source.execute("INSERT INTO t VALUES(1)").unwrap();
    assert!(source.db_handle().capture.floor().is_none());
    #[cfg(unix)]
    {
        let alias = root.path().join("source-alias");
        std::os::unix::fs::symlink(&source_path, &alias).unwrap();
        let alias_guard = super::prepare_archive_retention(&alias).unwrap();
        drop(alias_guard);
    }
    source.shutdown().unwrap();
    drop(source);
    drop(guard);
}

#[test]
fn all_vector_backends_and_fulltext_survive_base_and_pitr_without_heap_rebuilds() {
    struct Environment(Vec<(&'static str, Option<String>)>);
    impl Drop for Environment {
        fn drop(&mut self) {
            for (key, value) in &self.0 {
                if let Some(value) = value {
                    std::env::set_var(key, value);
                } else {
                    std::env::remove_var(key);
                }
            }
        }
    }
    let pairs = [
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ("MYDB_HNSW_COMPACT_DISABLE", "1"),
    ];
    let _environment = Environment(
        pairs
            .iter()
            .map(|(key, value)| {
                let old = std::env::var(key).ok();
                std::env::set_var(key, value);
                (*key, old)
            })
            .chain(std::iter::once((
                "MYDB_VECTOR_STORAGE",
                std::env::var("MYDB_VECTOR_STORAGE").ok(),
            )))
            .collect(),
    );
    let assert_index = |db: &StorageQueryEngine, sql: &str, index: &str| {
        let QueryResponse::ResultSet(plan) = db.execute(&format!("EXPLAIN {sql}")).unwrap() else {
            panic!("expected EXPLAIN rows")
        };
        let key = plan
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case("key"))
            .unwrap();
        assert!(
            plan.rows
                .iter()
                .any(|row| row[key].to_display_string() == index),
            "{index}: {:?}",
            plan.rows
        );
    };
    for backend in ["memory", "paged"] {
        std::env::set_var("MYDB_VECTOR_STORAGE", backend);
        let root = root();
        let preparation = crate::platform::fs::FixtureSyncBatch::new(root.path()).unwrap();
        let source = engine(&root.path().join("source"));
        let mut inputs = Vec::new();
        let mut fixtures = Vec::new();
        for datatype in ["FLOAT32", "FLOAT16", "BFLOAT16", "INT8", "BINARY"] {
            let table = format!("docs_{}", datatype.to_ascii_lowercase());
            let binary = datatype == "BINARY";
            let dim = if binary { 128 } else { 4 };
            let metric = if binary { "HAMMING" } else { "L2" };
            // Every vector index remains beyond twenty ordinary indexes.
            // Independent algorithms/codecs share immutable typed input and
            // fulltext state; FORCE INDEX and EXPLAIN pin every vector case.
            let indexes = (0..20)
                .map(|i| format!(",INDEX ordinary_{i}(extra)"))
                .collect::<String>();
            source.execute(&format!("CREATE TABLE {table}(id INT PRIMARY KEY,extra INT,body TEXT,v VECTOR({dim},{datatype}) NOT NULL,FULLTEXT KEY ft(body){indexes})")).unwrap();
            let values = (1..=20)
                .map(|id| {
                    let vector = if binary {
                        format!(
                            "[{}]",
                            (0..128)
                                .map(|bit| if bit < id { "1" } else { "0" })
                                .collect::<Vec<_>>()
                                .join(",")
                        )
                    } else {
                        format!("[{id},1,2,3]")
                    };
                    format!("({id},{id},'database baseline','{vector}')")
                })
                .collect::<Vec<_>>()
                .join(",");
            source
                .execute(&format!("INSERT INTO {table} VALUES {values}"))
                .unwrap();
            let zero = format!(
                "[{}]",
                std::iter::repeat_n("0", dim).collect::<Vec<_>>().join(",")
            );
            let query_vector = if binary {
                format!("TO_VECTOR('{zero}','BINARY')")
            } else {
                format!("TO_VECTOR('{zero}')")
            };
            let quantizations: &[&str] = if datatype == "FLOAT32" {
                &["none", "sq8", "sq4", "pq", "bq"]
            } else {
                &["none"]
            };
            for algorithm in ["HNSW", "IVF"] {
                for &quantization in quantizations {
                    let index = format!("vx_{}_{quantization}", algorithm.to_ascii_lowercase());
                    let options = if algorithm == "HNSW" {
                        "m=4,ef_construction=32,ef_search=128"
                    } else {
                        "nlist=2,nprobe=2"
                    };
                    let quant = if quantization == "pq" {
                        "quantization=pq,pq_m=2,pq_bits=4".to_owned()
                    } else {
                        format!("quantization={quantization}")
                    };
                    source.execute(&format!("CREATE INDEX {index} ON {table}(v) USING {algorithm} WITH(metric={metric},{options},{quant})")).unwrap();
                    let projection = format!("id,DISTANCE(v,{query_vector},'{metric}') d,body");
                    let sql = format!("SELECT {projection} FROM {table} FORCE INDEX({index}) ORDER BY d,id LIMIT 20");
                    let oracle = format!("SELECT /*+ NO_VECTOR_INDEX */ {projection} FROM {table} ORDER BY d,id LIMIT 20");
                    let expected_base = rows(&source, &oracle);
                    assert_index(&source, &sql, &index);
                    assert_eq!(
                        rows(&source, &sql),
                        expected_base,
                        "source {backend}/{table}/{index}"
                    );
                    fixtures.push((table.clone(), index, sql, oracle, expected_base));
                }
            }
            inputs.push((table, zero));
        }
        assert_eq!(fixtures.len(), 18);
        preparation.finish().unwrap();
        let scans = crate::storage::Database::vector_build_heap_scans_for_test();
        let repository = root.path().join("repo");
        let coordinator = Coordinator::get(source.db_handle()).unwrap();
        coordinator
            .configure(&repository, Policy::Manual, false, None)
            .unwrap();
        let base = job(&coordinator, coordinator.backup(&repository).unwrap())
            .base_id
            .unwrap();
        let base_dir = root.path().join("base");
        super::restore::restore(
            &repository,
            Some(&base),
            None,
            Target::Base,
            &base_dir,
            None,
            false,
        )
        .unwrap();
        let baseline = engine(&base_dir);
        for (table, index, sql, _, expected_base) in &fixtures {
            assert_index(&baseline, sql, index);
            assert_eq!(
                rows(&baseline, sql),
                *expected_base,
                "base {backend}/{table}/{index}"
            );
        }
        drop(baseline);
        assert_eq!(
            crate::storage::Database::vector_build_heap_scans_for_test(),
            scans,
            "base sealing or reading reconstructed {backend} indexes"
        );
        // Keep separate committed UPDATE/DELETE/INSERT phases. Each DML now
        // maintains all independent indexes on its typed table simultaneously.
        for (table, zero) in &inputs {
            source
                .execute(&format!(
                    "UPDATE {table} SET v='{zero}',body='database updateddelta' WHERE id=20"
                ))
                .unwrap();
            source
                .execute(&format!("DELETE FROM {table} WHERE id=1"))
                .unwrap();
            source
                .execute(&format!(
                    "INSERT INTO {table} VALUES(999,999,'database archiveddelta','{zero}')"
                ))
                .unwrap();
        }
        let expected_rows = fixtures
            .iter()
            .map(|(table, index, sql, oracle, _)| {
                let expected = rows(&source, oracle);
                assert_eq!(
                    rows(&source, sql),
                    expected,
                    "source DML {backend}/{table}/{index}"
                );
                expected
            })
            .collect::<Vec<_>>();
        let receipt = job(&coordinator, coordinator.backup_wal(&repository).unwrap());
        let target = root.path().join("pitr");
        super::restore::restore(
            &repository,
            Some(&base),
            None,
            Target::Lsn(receipt.requested_lsn),
            &target,
            None,
            false,
        )
        .unwrap();
        let restored = engine(&target);
        for ((table, index, sql, _, _), expected) in fixtures.iter().zip(expected_rows) {
            assert_index(&restored, sql, index);
            assert_eq!(
                rows(&restored, sql),
                expected,
                "PITR {backend}/{table}/{index}"
            );
        }
        for (table, _) in &inputs {
            assert_eq!(
                rows(
                    &restored,
                    &format!("SELECT id FROM {table} WHERE MATCH(body) AGAINST('updateddelta')")
                ),
                vec![vec![Value::Int(20)]],
                "{backend}/{table}"
            );
            assert_eq!(
                rows(
                    &restored,
                    &format!("SELECT id FROM {table} WHERE MATCH(body) AGAINST('archiveddelta')")
                ),
                vec![vec![Value::Int(999)]],
                "{backend}/{table}"
            );
            assert!(rows(&restored, "SHOW WARNINGS").is_empty());
        }
        assert_eq!(
            crate::storage::Database::vector_build_heap_scans_for_test(),
            scans,
            "PITR reconstructed {backend} indexes"
        );
        drop(restored);
        discard_source(source, coordinator);
    }
}

#[test]
fn resident_vector_seals_keep_source_lsn_for_prepared_future_decisions() {
    let prior = std::env::var("MYDB_VECTOR_STORAGE").ok();
    std::env::set_var("MYDB_VECTOR_STORAGE", "memory");
    std::env::set_var("MYDB_HNSW_BRUTE_THRESHOLD", "0");
    std::env::set_var("MYDB_HNSW_BRUTE_FILTER_ROWS", "0");
    let root = root();
    let preparation = crate::platform::fs::FixtureSyncBatch::new(root.path()).unwrap();
    let source = engine(&root.path().join("source"));
    let mut names = Vec::new();
    for (datatype, algorithm, quantization) in [
        ("FLOAT32", "HNSW", "none"),
        ("FLOAT16", "HNSW", "none"),
        ("FLOAT32", "IVF", "none"),
        ("FLOAT32", "HNSW", "pq"),
    ] {
        let name = format!("docs_{datatype}_{algorithm}_{quantization}");
        source
            .execute(&format!(
                "CREATE TABLE {name}(id INT PRIMARY KEY,v VECTOR(4,{datatype}) NOT NULL)"
            ))
            .unwrap();
        let values = (1..=20)
            .map(|id| format!("({id},'[{id},1,2,3]')"))
            .collect::<Vec<_>>()
            .join(",");
        source
            .execute(&format!("INSERT INTO {name} VALUES {values}"))
            .unwrap();
        let options = if algorithm == "HNSW" {
            "m=4,ef_construction=32,ef_search=128"
        } else {
            "nlist=2,nprobe=2"
        };
        let quant = if quantization == "pq" {
            "quantization=pq,pq_m=2,pq_bits=4".to_owned()
        } else {
            format!("quantization={quantization}")
        };
        source
            .execute(&format!(
                "CREATE INDEX vx ON {name}(v) USING {algorithm} WITH({options},{quant})"
            ))
            .unwrap();
        names.push(name);
    }
    preparation.finish().unwrap();
    let mut session = crate::engine::SessionContext::default();
    source
        .execute_with_session(&mut session, "XA START 'future-index-decision'")
        .unwrap();
    for name in &names {
        source
            .execute_with_session(
                &mut session,
                &format!("INSERT INTO {name} VALUES(999,'[0,0,0,0]')"),
            )
            .unwrap();
    }
    source
        .execute_with_session(&mut session, "XA END 'future-index-decision'")
        .unwrap();
    source
        .execute_with_session(&mut session, "XA PREPARE 'future-index-decision'")
        .unwrap();
    let scans = crate::storage::Database::vector_build_heap_scans_for_test();
    let repo = root.path().join("repo");
    let coordinator = Coordinator::get(source.db_handle()).unwrap();
    coordinator
        .configure(&repo, Policy::Manual, false, None)
        .unwrap();
    let base = job(&coordinator, coordinator.backup(&repo).unwrap())
        .base_id
        .unwrap();
    let baseline = root.path().join("baseline");
    super::restore::restore(
        &repo,
        Some(&base),
        None,
        Target::Base,
        &baseline,
        None,
        false,
    )
    .unwrap();
    let image = native::verify(&baseline).unwrap();
    for file in image
        .manifest
        .files
        .iter()
        .filter(|file| file.path.ends_with(".hnsw"))
    {
        let bytes = fs::read(baseline.join(&file.path)).unwrap();
        assert_eq!(&bytes[..8], b"MYDBIDX2");
        let covered = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        assert!(
            covered <= image.manifest.end_lsn,
            "{}: local seal position {covered} exceeded source end {}",
            file.path,
            image.manifest.end_lsn
        );
    }
    source.execute("XA COMMIT 'future-index-decision'").unwrap();
    let receipt = job(&coordinator, coordinator.backup_wal(&repo).unwrap());
    let target = root.path().join("decision");
    super::restore::restore(
        &repo,
        Some(&base),
        None,
        Target::Lsn(receipt.requested_lsn),
        &target,
        None,
        false,
    )
    .unwrap();
    let restored = engine(&target);
    for name in &names {
        assert_eq!(
            rows(
                &restored,
                &format!("SELECT id FROM {name} ORDER BY DISTANCE(v,'[0,0,0,0]') LIMIT 1")
            ),
            vec![vec![Value::Int(999)]],
            "{name}"
        );
        assert_eq!(
            crate::storage::Database::vector_build_heap_scans_for_test(),
            scans
        );
        assert!(rows(&restored, "SHOW WARNINGS").is_empty());
    }
    drop(restored);
    discard_source(source, coordinator);
    if let Some(prior) = prior {
        std::env::set_var("MYDB_VECTOR_STORAGE", prior);
    } else {
        std::env::remove_var("MYDB_VECTOR_STORAGE");
    }
}

#[test]
fn spatial_index_backup_restore_and_immutable_queries_keep_tree_pages() {
    let root = root();
    let source = engine(&root.path().join("source"));
    source.execute("CREATE TABLE spatial_backup(id INT PRIMARY KEY,g POINT NOT NULL SRID 0,SPATIAL KEY sp(g))").unwrap();
    source.execute("INSERT INTO spatial_backup VALUES(1,ST_GeomFromText('POINT(1 1)')),(2,ST_GeomFromText('POINT(20 20)'))").unwrap();
    let query="SELECT id FROM spatial_backup FORCE INDEX(sp) WHERE MBRIntersects(g,ST_GeomFromText('POINT(1 1)')) ORDER BY id";
    let expected = rows(&source, query);
    let mut xa = crate::engine::SessionContext::default();
    for sql in [
        "XA START 'spatial-backup'",
        "UPDATE spatial_backup SET g=ST_GeomFromText('POINT(40 40)') WHERE id=1",
        "XA END 'spatial-backup'",
        "XA PREPARE 'spatial-backup'",
    ] {
        source.execute_with_session(&mut xa, sql).unwrap();
    }
    let coordinator = Coordinator::get(source.db_handle()).unwrap();
    let repository = root.path().join("repository");
    let base = job(&coordinator, coordinator.backup(&repository).unwrap())
        .base_id
        .unwrap();
    source.execute("XA ROLLBACK 'spatial-backup'").unwrap();
    source
        .execute("UPDATE spatial_backup SET g=ST_GeomFromText('POINT(30 30)') WHERE id=1")
        .unwrap();
    let runtime = root.path().join("runtime");
    let reader = super::open_base(&repository, &base, &runtime).unwrap();
    let backup = StorageQueryEngine::open_backup_image(
        reader.data_dir.clone(),
        runtime,
        Default::default(),
        Arc::new(crate::engine::session_registry::SessionRegistry::new()),
        vec![],
    )
    .unwrap();
    assert_eq!(rows(&backup, query), expected);
    backup.shutdown().unwrap();
    drop(backup);
    let restored = root.path().join("restored");
    super::restore::restore(
        &repository,
        Some(&base),
        None,
        Target::Base,
        &restored,
        None,
        false,
    )
    .unwrap();
    let backup = engine(&restored);
    assert_eq!(rows(&backup, query), expected);
    backup.shutdown().unwrap();
    drop(backup);
    discard_source(source, coordinator);
}
