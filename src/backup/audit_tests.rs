//! Regression evidence for the October 19 backup audit.
use super::coordinator::{Coordinator, Job, Policy};
use super::repository::{Publication, Repository};
use super::restore::Target;
use super::*;
use crate::engine::StorageQueryEngine;

struct Fixture {
    source: StorageQueryEngine,
    c: Arc<Coordinator>,
    repo: PathBuf,
    base: String,
    root: tempfile::TempDir,
}
impl Fixture {
    fn new(policy: Policy) -> Self {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let sync = crate::platform::fs::FixtureSyncBatch::new(root.path()).unwrap();
        let source = StorageQueryEngine::open(root.path().join("source")).unwrap();
        source
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,p LONGBLOB,INDEX iv(v))")
            .unwrap();
        source
            .execute("INSERT INTO t VALUES(1,10,REPEAT('x',24000))")
            .unwrap();
        let c = Coordinator::get(source.db_handle()).unwrap();
        let repo = fs::canonicalize(root.path()).unwrap().join("repo");
        c.configure(&repo, policy, false, None).unwrap();
        let base = completed(&c, c.backup(&repo).unwrap()).base_id.unwrap();
        sync.finish().unwrap();
        Self {
            root,
            source,
            c,
            repo,
            base,
        }
    }
    fn open(&self) -> Repository {
        Repository::open(&self.repo, None).unwrap()
    }
    fn archive(&self) -> Job {
        self.source
            .execute("UPDATE t SET v=v+1 WHERE id=1")
            .unwrap();
        completed(&self.c, self.c.backup_wal(&self.repo).unwrap())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // Drain jobs before dropping the engine, then remove the private
        // directory last. Discarded fixture data needs no final archive or
        // sharp checkpoint; tests of graceful shutdown call it explicitly.
        let _ = self.c.shutdown();
    }
}
fn terminal(c: &Coordinator, id: &str) -> Job {
    let until = Instant::now() + Duration::from_secs(60);
    loop {
        let job = c.jobs().into_iter().find(|j| j.id == id).unwrap();
        if job.state != "RUNNING" {
            return job;
        }
        assert!(Instant::now() < until, "job did not terminate");
        std::thread::sleep(Duration::from_millis(5));
    }
}
fn completed(c: &Coordinator, job: Job) -> Job {
    let job = terminal(c, &job.id);
    assert_eq!(job.state, "COMPLETED", "{}", job.error);
    job
}

#[test]
fn audit_b01_blob_sync_error_cannot_acknowledge_or_release_wal() {
    let f = Fixture::new(Policy::Manual);
    f.source.execute("UPDATE t SET v=20 WHERE id=1").unwrap();
    let before = f.c.control.lock().unwrap().clone().unwrap();
    let floor = f.source.db_handle().capture.leases()["archive"].floor;
    faults::fail("object_parent_sync", &f.repo.join("blobs/plain"));
    let job = terminal(&f.c, &f.c.backup_wal(&f.repo).unwrap().id);
    assert_eq!(job.state, "FAILED", "unsynced blob was acknowledged");
    assert_eq!(
        f.source.db_handle().capture.leases()["archive"].floor,
        floor
    );
    assert_eq!(
        f.c.control.lock().unwrap().as_ref().unwrap().cursor,
        before.cursor
    );
    let receipt = completed(&f.c, f.c.backup_wal(&f.repo).unwrap());
    assert!(receipt.durable_through_lsn > before.cursor);
    let target = f.root.path().join("restored");
    super::restore::restore(
        &f.repo,
        Some(&f.base),
        None,
        Target::Latest,
        &target,
        None,
        false,
    )
    .unwrap();
    let db = StorageQueryEngine::open(target).unwrap();
    let QueryResponse::ResultSet(result) = db.execute("SELECT v,LENGTH(p) FROM t").unwrap() else {
        panic!()
    };
    assert_eq!(result.rows, vec![vec![Value::Int(20), Value::Int(24000)]]);
    db.shutdown().unwrap();
}

#[test]
fn audit_b02_promotion_interruption_preserves_a_valid_image() {
    let f = Fixture::new(Policy::Disabled);
    let target = fs::canonicalize(f.root.path()).unwrap().join("restored");
    super::restore::restore(
        &f.repo,
        Some(&f.base),
        None,
        Target::Base,
        &target,
        None,
        false,
    )
    .unwrap();
    faults::fail("snapshot_manifest_published", &target);
    let _ = native::promote(&target);
    assert!(
        native::inspect(&target).is_ok(),
        "promotion left mismatching markers"
    );
    native::promote(&target).unwrap();
    let db = StorageQueryEngine::open(&target).unwrap();
    db.execute("UPDATE t SET v=30 WHERE id=1").unwrap();
    db.shutdown().unwrap();
}

#[test]
fn audit_b03_invalid_target_does_not_poison_full_backup_admission() {
    let f = Fixture::new(Policy::Disabled);
    let bad = f.repo.join("..").join("bad");
    assert!(f.c.backup(&bad).is_err());
    completed(&f.c, f.c.backup(&f.repo).expect("capture admission leaked"));
    f.c.configure(&f.repo, Policy::Manual, false, None).unwrap();
}

#[test]
fn audit_b04_exact_base_targets_ignore_broken_later_time_fences() {
    let f = Fixture::new(Policy::Manual);
    f.archive();
    let repo = f.open();
    let (base, _) = repo.base(&f.base).unwrap();
    let stream = base.stream_id.as_ref().unwrap();
    fs::write(
        repo.root
            .join("streams")
            .join(stream)
            .join("fences/current"),
        b"{broken",
    )
    .unwrap();
    for (n, target) in [
        Target::Base,
        Target::Lsn(base.source_end_lsn),
        Target::Time(base.source_end_time),
    ]
    .into_iter()
    .enumerate()
    {
        let destination = f.root.path().join(format!("restore-{n}"));
        let report = super::restore::restore(
            &f.repo,
            Some(&f.base),
            None,
            target,
            &destination,
            None,
            false,
        )
        .unwrap();
        assert_eq!(report.restored_source_lsn, base.source_end_lsn);
        super::management::export(
            &f.repo,
            Some(&f.base),
            None,
            target,
            &f.root.path().join(format!("export-{n}")),
            None,
        )
        .unwrap();
    }
    assert!(super::restore::restore(
        &f.repo,
        Some(&f.base),
        None,
        Target::Latest,
        &f.root.path().join("bad"),
        None,
        false
    )
    .is_err());
}

#[test]
fn audit_b05_prune_retries_committed_but_unfinished_deletions() {
    let f = Fixture::new(Policy::Disabled);
    let next = completed(&f.c, f.c.backup(&f.repo).unwrap())
        .base_id
        .unwrap();
    let repo = f.open();
    repo.publish(Publication::Prune {
        bases: vec![f.base.clone()],
        ranges: vec![],
    })
    .unwrap();
    super::management::prune(&f.repo, None, None, true, None).unwrap();
    assert!(
        !repo.root.join("bases").join(&f.base).exists(),
        "committed GC deletion was forgotten"
    );
    repo.base(&next).unwrap();
    super::management::prune(&f.repo, None, None, true, None).unwrap();
}

#[test]
fn audit_b06_export_rejects_invalid_spans_without_panicking() {
    let f = Fixture::new(Policy::Manual);
    f.archive();
    let repo = f.open();
    let mut range = None;
    repo.visit_live(&repo.position().unwrap(), |_, event| {
        if let Publication::Range { range: r } = event {
            range = Some(r);
        }
        Ok(())
    })
    .unwrap();
    let mut range = range.unwrap();
    range.source_spans.clear();
    repo.publish(Publication::Range { range }).unwrap();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        super::management::export(
            &f.repo,
            Some(&f.base),
            None,
            Target::Latest,
            &f.root.path().join("bundle"),
            None,
        )
    }));
    assert!(outcome.is_ok(), "invalid range caused a panic");
    assert!(outcome.unwrap().is_err());
    assert!(!f.root.path().join("bundle").exists());
}

#[test]
fn audit_b07_stop_applies_the_entire_budget_configuration() {
    // This case checks the byte-budget patch, not a one-second wall-clock
    // capture deadline while other nextest processes are synchronizing files.
    std::env::set_var("MYDB_BACKUP_FREEZE_BUDGET_MS", "30000");
    let f = Fixture::new(Policy::Manual);
    let limit = 268435456;
    let stop =
        f.c.configure(&f.repo, Policy::Disabled, false, Some(limit))
            .unwrap()
            .unwrap();
    completed(&f.c, stop);
    assert_eq!(
        f.c.control
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .max_pending_wal_bytes,
        limit
    );
    assert_eq!(
        f.source
            .db_handle()
            .capture
            .budget
            .limit
            .load(Ordering::Acquire),
        limit
    );
    f.c.configure(&f.repo, Policy::Manual, false, None).unwrap();
    completed(&f.c, f.c.backup(&f.repo).unwrap());
    f.c.configure(&f.repo, Policy::Disabled, true, Some(limit * 2))
        .unwrap();
    assert_eq!(
        f.source
            .db_handle()
            .capture
            .budget
            .limit
            .load(Ordering::Acquire),
        limit * 2
    );
}

#[test]
fn audit_r04_copy_corruption_cannot_be_blessed_by_a_new_seal() {
    let f = Fixture::new(Policy::Disabled);
    let base = f.repo.join("bases").join(&f.base).join("data");
    let target = fs::canonicalize(f.root.path()).unwrap().join("copy");
    let expected = native::inspect(&base).unwrap().unwrap();
    let heap = expected
        .manifest
        .files
        .iter()
        .find(|file| file.path.ends_with(".heap") && file.len > 0)
        .unwrap()
        .path
        .clone();
    faults::install("restore_copy_complete", &target, {
        let target = target.clone();
        move || {
            let file = OpenOptions::new()
                .write(true)
                .open(target.join(&heap))
                .unwrap();
            write_all_at(&file, &[0x55; 16], 32).unwrap();
            Ok(())
        }
    });
    let image = native::inspect(&base).unwrap().unwrap();
    let outcome = native::restore_image(
        &base,
        &target,
        image.manifest.end_lsn,
        image.manifest.end_commit_unix_us,
        false,
        |_| Ok(()),
    );
    assert!(
        outcome.is_err(),
        "corrupted copied bytes received a fresh valid seal"
    );
}

#[test]
fn audit_r01_queued_archive_cannot_rebind_to_a_reenabled_stream() {
    let f = Fixture::new(Policy::Manual);
    f.source.execute("UPDATE t SET v=20 WHERE id=1").unwrap();
    let (ready_tx, ready) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    faults::install(
        "wal_before_execute",
        f.source.db_handle().data_dir(),
        move || {
            ready_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            Ok(())
        },
    );
    let old_stream =
        f.c.control
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .stream_id
            .clone();
    let old = f.c.backup_wal(&f.repo).unwrap();
    ready.recv_timeout(Duration::from_secs(30)).unwrap();
    f.c.configure(&f.repo, Policy::Disabled, true, None)
        .unwrap();
    f.c.configure(&f.repo, Policy::Manual, false, None).unwrap();
    completed(&f.c, f.c.backup(&f.repo).unwrap());
    let new = f.c.control.lock().unwrap().as_ref().unwrap().clone();
    assert_ne!(old_stream, new.stream_id);
    release.send(()).unwrap();
    let result = terminal(&f.c, &old.id);
    assert_eq!(
        result.state, "FAILED",
        "old request was rebound to the new stream"
    );
    assert_eq!(
        result.durable_through_lsn, 0,
        "receipt belongs to a different stream"
    );
    assert_eq!(
        f.c.control.lock().unwrap().as_ref().unwrap().cursor,
        new.cursor
    );
}

#[test]
fn audit_r02_scheduler_cannot_start_after_shutdown() {
    let f = Fixture::new(Policy::Continuous);
    let (ready_tx, ready) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    faults::install(
        "scheduler_before_start",
        f.source.db_handle().data_dir(),
        move || {
            ready_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            Ok(())
        },
    );
    let c = f.c.clone();
    let start = std::thread::spawn(move || c.test_start_scheduler());
    ready.recv_timeout(Duration::from_secs(30)).unwrap();
    f.c.shutdown().unwrap();
    release.send(()).unwrap();
    let _ = start.join().unwrap();
    let restarted = f.c.test_has_scheduler();
    f.c.shutdown().unwrap();
    assert!(
        !restarted,
        "a late completion restarted the scheduler after shutdown"
    );
}

#[test]
fn audit_r03_repository_initialization_is_retryable_after_interruption() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let path = fs::canonicalize(root.path()).unwrap().join("repo");
    faults::fail("repository_init_ready", &path);
    assert!(Repository::init(&path, None, None).is_err());
    let repo =
        Repository::init(&path, None, None).expect("partial initialization poisoned the target");
    assert_eq!(repo.position().unwrap().generation, 0);
    assert_eq!(
        fs::read_dir(root.path())
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_type().unwrap().is_dir())
            .count(),
        1
    );
}

#[test]
fn audit_r03_control_initialization_cannot_poison_source_startup() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let path = root.path().join("source");
    let db = StorageQueryEngine::open(&path).unwrap();
    let c = Coordinator::get(db.db_handle()).unwrap();
    faults::fail("control_identity_published", db.db_handle().data_dir());
    assert!(c
        .configure(&root.path().join("repo"), Policy::Manual, false, None)
        .is_err());
    db.shutdown().unwrap();
    drop(c);
    drop(db);
    let reopened =
        StorageQueryEngine::open(&path).expect("interrupted control creation prevented startup");
    reopened.shutdown().unwrap();
}

#[test]
fn audit_r03_failed_standalone_publication_is_reclaimed_by_gc() {
    let f = Fixture::new(Policy::Disabled);
    super::repository::FAIL_AFTER_BASE_RENAME.store(true, Ordering::SeqCst);
    let failed = terminal(&f.c, &f.c.backup(&f.repo).unwrap().id);
    assert_eq!(failed.state, "FAILED");
    let id = failed.base_id.unwrap();
    assert!(f.repo.join("bases").join(&id).exists());
    super::management::prune(&f.repo, None, None, true, None).unwrap();
    assert!(
        !f.repo.join("bases").join(&id).exists(),
        "unpublished owned base leaked"
    );
    f.open().base(&f.base).unwrap();
}

fn query_rows(db: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = db.execute(sql).unwrap() else {
        panic!("expected rows")
    };
    result.rows
}

#[test]
fn audit_r05_file_generations_and_authoritative_objects_continue_from_e() {
    let f = Fixture::new(Policy::Manual);
    for sql in [
        "CREATE TABLE mixed(id BIGINT UNSIGNED PRIMARY KEY,d DECIMAL(30,8),j JSON,s VARCHAR(100),ts DATETIME(6),g GEOMETRY)",
        "INSERT INTO mixed VALUES(1,12345678901234567890.12345678,JSON_OBJECT('n',1),'你好数据库','9999-12-31 23:59:59.999999',ST_GeomFromText('POINT(1 2)'))",
        "CREATE TABLE docs(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body))",
        "INSERT INTO docs VALUES(1,'backup database')",
        "CREATE TABLE vecs(id INT PRIMARY KEY,emb VECTOR(3) NOT NULL)",
        "INSERT INTO vecs VALUES(1,'[0,0,0]'),(2,'[1,0,0]')",
        "CREATE INDEX vi ON vecs(emb) USING HNSW",
        "CREATE TABLE ivfs(id INT PRIMARY KEY,emb VECTOR(3) NOT NULL)",
        "INSERT INTO ivfs VALUES(1,'[0,0,0]'),(2,'[1,0,0]')",
        "CREATE INDEX ii ON ivfs(emb) USING IVF_SQ8 COMMENT 'NLIST=1 NPROBE=1'",
        "CREATE SEQUENCE audit_seq",
    ] { f.source.execute(sql).unwrap(); }
    let queries = [
        "SELECT id,v,SHA2(p,256) FROM t ORDER BY id",
        "SELECT id FROM t FORCE INDEX(iv) WHERE v>=0 ORDER BY id",
        "SELECT id,CAST(d AS CHAR),CAST(j AS CHAR),s,CAST(ts AS CHAR),ST_AsText(g) FROM mixed ORDER BY id",
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('database') ORDER BY id",
        "SELECT id FROM vecs ORDER BY DISTANCE(emb,'[0,0,0]') LIMIT 2",
        "SELECT id FROM ivfs ORDER BY DISTANCE(emb,'[0,0,0]') LIMIT 2",
    ];
    let (pinned_tx, pinned) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    faults::install(
        "capture_files_pinned",
        f.source.db_handle().data_dir(),
        move || {
            pinned_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            Ok(())
        },
    );
    let (captured_tx, captured) = mpsc::channel();
    let (finish, finish_rx) = mpsc::channel();
    faults::install(
        "capture_complete",
        f.source.db_handle().data_dir(),
        move || {
            captured_tx.send(()).unwrap();
            finish_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            Ok(())
        },
    );
    let job = f.c.backup(&f.repo).unwrap();
    pinned.recv_timeout(Duration::from_secs(30)).unwrap();
    for sql in [
        "UPDATE t SET v=21,p=REPEAT('captured',10000) WHERE id=1",
        "UPDATE mixed SET j=JSON_OBJECT('n',2),s='捕获边界' WHERE id=1",
        "UPDATE vecs SET emb='[2,3,4]' WHERE id=2",
        "UPDATE ivfs SET emb='[2,3,4]' WHERE id=2",
        "UPDATE docs SET body='captured database' WHERE id=1",
        "SELECT NEXTVAL('audit_seq')",
    ] {
        f.source.execute(sql).unwrap();
    }
    assert_eq!(
        f.source.db_handle().compact_undo_segments(100, 8).unwrap(),
        0
    );
    f.source.db_handle().checkpoint().unwrap();
    let at_e = queries
        .iter()
        .map(|q| query_rows(&f.source, q))
        .collect::<Vec<_>>();
    release.send(()).unwrap();
    captured.recv_timeout(Duration::from_secs(30)).unwrap();
    for sql in [
        "TRUNCATE TABLE t",
        "INSERT INTO t VALUES(2,99,REPEAT('later',12000))",
        "RENAME TABLE mixed TO renamed_mixed",
        "UPDATE renamed_mixed SET j=JSON_OBJECT('n',3)",
        "UPDATE docs SET body='later database' WHERE id=1",
        "UPDATE vecs SET emb='[100,0,0]' WHERE id=1",
        "UPDATE ivfs SET emb='[100,0,0]' WHERE id=1",
        "SELECT NEXTVAL('audit_seq')",
    ] {
        f.source.execute(sql).unwrap();
    }
    finish.send(()).unwrap();
    let base = completed(&f.c, job).base_id.unwrap();
    let lease = super::open_base(&f.repo, &base, &f.root.path().join("runtime")).unwrap();
    let reader = StorageQueryEngine::open_backup_image(
        lease.data_dir.clone(),
        f.root.path().join("runtime"),
        Default::default(),
        Arc::new(crate::engine::session_registry::SessionRegistry::new()),
        vec![],
    )
    .unwrap();
    assert_eq!(
        queries
            .iter()
            .map(|q| query_rows(&reader, q))
            .collect::<Vec<_>>(),
        at_e
    );
    reader.shutdown().unwrap();
    completed(&f.c, f.c.backup_wal(&f.repo).unwrap());
    let target = f.root.path().join("continued");
    super::restore::restore(
        &f.repo,
        Some(&base),
        None,
        Target::Latest,
        &target,
        None,
        false,
    )
    .unwrap();
    let restored = StorageQueryEngine::open(target).unwrap();
    for q in queries {
        let q = q.replace("FROM mixed", "FROM renamed_mixed");
        assert_eq!(query_rows(&restored, &q), query_rows(&f.source, &q), "{q}");
    }
    restored.shutdown().unwrap();
}

#[test]
fn audit_b01_visible_blob_must_confirm_durability_on_each_retry() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let path = fs::canonicalize(root.path()).unwrap().join("repo");
    let repo = Repository::init(&path, None, None).unwrap();
    let bytes = b"unconfirmed immutable blob";
    for _ in 0..2 {
        faults::fail("object_parent_sync", &path.join("blobs/plain"));
        assert!(repo.store_blob(bytes).is_err());
    }
    let blob = repo.store_blob(bytes).unwrap();
    assert_eq!(repo.read_blob(&blob).unwrap(), bytes);
}

#[test]
fn audit_b02_mode_pointer_is_the_only_promotion_commit() {
    let f = Fixture::new(Policy::Disabled);
    for (n, point) in ["image_mode_before_publish", "image_mode_published"]
        .into_iter()
        .enumerate()
    {
        let target = fs::canonicalize(f.root.path())
            .unwrap()
            .join(format!("promote-{n}"));
        super::restore::restore(
            &f.repo,
            Some(&f.base),
            None,
            Target::Base,
            &target,
            None,
            false,
        )
        .unwrap();
        let marker = fs::read(target.join(native::MARKER)).unwrap();
        faults::fail(point, &target);
        assert!(native::promote(&target).is_err());
        let image = native::inspect(&target).unwrap().unwrap();
        assert_eq!(
            image.state,
            if n == 0 {
                native::State::ReadOnly
            } else {
                native::State::Writable
            }
        );
        assert_eq!(marker, fs::read(target.join(native::MARKER)).unwrap());
        native::promote(&target).unwrap();
    }
}

#[test]
fn audit_b05_partially_swept_directories_remain_retryable() {
    let f = Fixture::new(Policy::Disabled);
    let next = completed(&f.c, f.c.backup(&f.repo).unwrap())
        .base_id
        .unwrap();
    let repo = f.open();
    repo.publish(Publication::Prune {
        bases: vec![f.base.clone()],
        ranges: vec![],
    })
    .unwrap();
    let retired = repo.root.join("bases").join(&f.base);
    faults::fail("gc_before_delete", &retired);
    assert!(super::management::prune(&f.repo, None, None, true, None).is_err());
    assert!(repo.root.join("catalog/GC_PENDING").exists());
    fs::remove_file(retired.join("data").join(native::MARKER)).unwrap();
    super::management::prune(&f.repo, None, None, true, None).unwrap();
    assert!(!retired.exists());
    assert!(!repo.root.join("catalog/GC_PENDING").exists());
    repo.base(&next).unwrap();
}

#[test]
fn audit_r02_shutdown_waits_for_an_admitted_worker_registration() {
    let f = Fixture::new(Policy::Manual);
    let (ready_tx, ready) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    faults::install(
        "job_before_spawn",
        f.source.db_handle().data_dir(),
        move || {
            ready_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            Ok(())
        },
    );
    let submit = {
        let c = f.c.clone();
        std::thread::spawn(move || c.test_noop())
    };
    ready.recv_timeout(Duration::from_secs(30)).unwrap();
    let (closing_tx, closing) = mpsc::channel();
    faults::install(
        "shutdown_admission_closed",
        f.source.db_handle().data_dir(),
        move || {
            closing_tx.send(()).unwrap();
            Ok(())
        },
    );
    let (done_tx, done) = mpsc::channel();
    let shutdown = {
        let c = f.c.clone();
        std::thread::spawn(move || {
            c.shutdown().unwrap();
            done_tx.send(()).unwrap();
        })
    };
    closing.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(
        done.recv_timeout(Duration::from_millis(100)).is_err(),
        "shutdown forgot an admitted worker"
    );
    release.send(()).unwrap();
    let job = submit.join().unwrap().unwrap();
    shutdown.join().unwrap();
    assert_ne!(terminal(&f.c, &job.id).state, "RUNNING");
}

#[cfg(unix)]
fn crash_at(point: &'static str, path: &Path) {
    faults::install(point, path, faults::crash_process);
}

#[test]
#[cfg(unix)]
fn audit_crash_worker() {
    let Ok(root) = std::env::var("MYDB_AUDIT_CRASH_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let mode = std::env::var("MYDB_AUDIT_CRASH_MODE").unwrap();
    let point = std::env::var("MYDB_AUDIT_CRASH_POINT").unwrap();
    let point = match point.as_str() {
        "repository_init_ready" => "repository_init_ready",
        "directory_installed" => "directory_installed",
        "control_identity_published" => "control_identity_published",
        "image_mode_before_publish" => "image_mode_before_publish",
        "image_mode_published" => "image_mode_published",
        "restore_installed" => "restore_installed",
        "gc_before_delete" => "gc_before_delete",
        "gc_after_delete" => "gc_after_delete",
        "source_ack_saved" => "source_ack_saved",
        _ => panic!("unknown crash boundary"),
    };
    match mode.as_str() {
        "repository" => {
            crash_at(point, &root.join("repo"));
            Repository::init(&root.join("repo"), None, None).unwrap();
        }
        "control" => {
            let db = StorageQueryEngine::open(root.join("source")).unwrap();
            let path = if point == "directory_installed" {
                db.db_handle().data_dir().join("system/backup-control")
            } else {
                db.db_handle().data_dir().to_path_buf()
            };
            crash_at(point, &path);
            Coordinator::get(db.db_handle())
                .unwrap()
                .configure(&root.join("repo"), Policy::Manual, false, None)
                .unwrap();
        }
        "promote" => {
            crash_at(point, &root.join("image"));
            native::promote(&root.join("image")).unwrap();
        }
        "restore" => {
            crash_at(point, &root.join("image"));
            let base = std::env::var("MYDB_AUDIT_CRASH_BASE").unwrap();
            super::restore::restore(
                &root.join("repo"),
                Some(&base),
                None,
                Target::Base,
                &root.join("image"),
                None,
                false,
            )
            .unwrap();
        }
        "gc" => {
            let base = std::env::var("MYDB_AUDIT_CRASH_BASE").unwrap();
            crash_at(point, &root.join("repo/bases").join(base));
            super::management::prune(&root.join("repo"), None, None, true, None).unwrap();
        }
        "archive" => {
            let db = StorageQueryEngine::open(root.join("source")).unwrap();
            let c = Coordinator::get(db.db_handle()).unwrap();
            db.execute("UPDATE t SET v=20 WHERE id=1").unwrap();
            c.configure(&root.join("repo"), Policy::Manual, false, Some(1024))
                .unwrap();
            crash_at(point, db.db_handle().data_dir());
            let job = c.backup_wal(&root.join("repo")).unwrap();
            terminal(&c, &job.id);
        }
        "workspace" => {
            let destination = root.join("workspace-target");
            let operation = DirectoryOperation::new(&destination, "audit-work").unwrap();
            write_sync(&operation.data.join("payload"), b"private payload").unwrap();
            crash_at(point, &destination);
            faults::hit(point, &destination).unwrap();
        }
        _ => panic!("unknown crash mode"),
    }
    panic!("crash boundary was not reached");
}

#[cfg(unix)]
fn run_crash(root: &Path, mode: &str, point: &str, base: &str) {
    use std::os::unix::process::ExitStatusExt;
    let log = std::fs::File::create(root.join(format!("crash-{mode}-{point}.log"))).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "backup::audit_tests::audit_crash_worker",
            "--nocapture",
        ])
        .env("MYDB_AUDIT_CRASH_ROOT", root)
        .env("MYDB_AUDIT_CRASH_MODE", mode)
        .env("MYDB_AUDIT_CRASH_POINT", point)
        .env("MYDB_AUDIT_CRASH_BASE", base)
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .status()
        .unwrap();
    assert_eq!(status.signal(), Some(libc::SIGKILL), "{mode}/{point}");
}

#[test]
#[cfg(unix)]
fn audit_r03_sigkill_initialization_retries_without_manual_cleanup() {
    for (mode, points) in [
        (
            "repository",
            vec!["repository_init_ready", "directory_installed"],
        ),
        (
            "control",
            vec!["control_identity_published", "directory_installed"],
        ),
    ] {
        for point in points {
            let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
            let path = fs::canonicalize(root.path()).unwrap();
            run_crash(&path, mode, point, "");
            if mode == "repository" {
                Repository::init(&path.join("repo"), None, None).unwrap();
            } else {
                let db = StorageQueryEngine::open(path.join("source")).unwrap();
                let c = Coordinator::get(db.db_handle()).unwrap();
                c.configure(&path.join("repo"), Policy::Manual, false, None)
                    .unwrap();
                db.shutdown().unwrap();
            }
            assert!(!fs::read_dir(&path).unwrap().any(|e| {
                let entry = e.unwrap();
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".mydb-repository-init-")
                    && entry.file_type().unwrap().is_dir()
            }));
        }
    }
}

#[test]
#[cfg(unix)]
fn audit_b02_sigkill_promotion_has_one_recoverable_commit_point() {
    for point in ["image_mode_before_publish", "image_mode_published"] {
        let f = Fixture::new(Policy::Disabled);
        let root = fs::canonicalize(f.root.path()).unwrap();
        let image = root.join("image");
        super::restore::restore(
            &f.repo,
            Some(&f.base),
            None,
            Target::Base,
            &image,
            None,
            false,
        )
        .unwrap();
        let marker = fs::read(image.join(native::MARKER)).unwrap();
        run_crash(&root, "promote", point, "");
        native::inspect(&image).unwrap();
        if point == "image_mode_before_publish" {
            native::verify(&image).unwrap();
        }
        native::promote(&image).unwrap();
        assert_eq!(marker, fs::read(image.join(native::MARKER)).unwrap());
        let db = StorageQueryEngine::open(&image).unwrap();
        db.execute("UPDATE t SET v=77 WHERE id=1").unwrap();
        db.shutdown().unwrap();
    }
}

#[test]
#[cfg(unix)]
fn audit_b05_sigkill_sweep_retries_both_sides_of_unlink() {
    for point in ["gc_before_delete", "gc_after_delete"] {
        let f = Fixture::new(Policy::Disabled);
        let next = completed(&f.c, f.c.backup(&f.repo).unwrap())
            .base_id
            .unwrap();
        f.open()
            .publish(Publication::Prune {
                bases: vec![f.base.clone()],
                ranges: vec![],
            })
            .unwrap();
        run_crash(
            &fs::canonicalize(f.root.path()).unwrap(),
            "gc",
            point,
            &f.base,
        );
        super::management::prune(&f.repo, None, None, true, None).unwrap();
        assert!(!f.repo.join("bases").join(&f.base).exists());
        assert!(!f.repo.join("catalog/GC_PENDING").exists());
        f.open().base(&next).unwrap();
    }
}

#[test]
#[cfg(unix)]
fn audit_r03_installed_restore_can_confirm_after_a_crash() {
    let f = Fixture::new(Policy::Disabled);
    let root = fs::canonicalize(f.root.path()).unwrap();
    run_crash(&root, "restore", "restore_installed", &f.base);
    native::confirm_install(&root.join("image")).unwrap();
    let db = StorageQueryEngine::open(root.join("image")).unwrap();
    assert_eq!(
        query_rows(&db, "SELECT v FROM t"),
        vec![vec![Value::Int(10)]]
    );
    db.shutdown().unwrap();
}

#[test]
fn audit_b05_a_new_root_cancels_an_interrupted_blob_deletion() {
    let f = Fixture::new(Policy::Manual);
    let anchor = f.open().position().unwrap();
    f.archive();
    let repo = f.open();
    let mut blob = None;
    repo.visit_live(&repo.position().unwrap(), |_, event| {
        if let Publication::Range { range } = event {
            blob = Some(range.chunks[0].clone());
        }
        Ok(())
    })
    .unwrap();
    let blob = blob.unwrap();
    #[derive(Serialize)]
    struct Pending {
        version: u32,
        repository_id: String,
        anchor: super::repository::Position,
        paths: Vec<String>,
    }
    let payload = Pending {
        version: 1,
        repository_id: repo.header.id.clone(),
        anchor,
        paths: vec![format!("blobs/plain/{}", blob.hash)],
    };
    let bytes = serde_json::to_vec(&payload).unwrap();
    let envelope = serde_json::json!({"payload":payload,"digest":sha256_hex(&bytes)});
    fs::write(
        repo.root.join("catalog/GC_PENDING"),
        serde_json::to_vec(&envelope).unwrap(),
    )
    .unwrap();
    super::management::prune(&f.repo, None, None, true, None).unwrap();
    assert!(!repo.root.join("catalog/GC_PENDING").exists());
    repo.read_blob(&blob).unwrap();
    let target = f.root.path().join("restored");
    super::restore::restore(
        &f.repo,
        Some(&f.base),
        None,
        Target::Latest,
        &target,
        None,
        false,
    )
    .unwrap();
    let db = StorageQueryEngine::open(target).unwrap();
    assert_eq!(
        query_rows(&db, "SELECT v FROM t"),
        vec![vec![Value::Int(11)]]
    );
    db.shutdown().unwrap();
}

#[test]
fn audit_r04_nonpage_authority_must_match_the_original_copy_manifest() {
    let f = Fixture::new(Policy::Disabled);
    let base = f.repo.join("bases").join(&f.base).join("data");
    let target = fs::canonicalize(f.root.path())
        .unwrap()
        .join("nonpage-copy");
    faults::install("restore_copy_complete", &target, {
        let target = target.clone();
        move || {
            let file = OpenOptions::new()
                .write(true)
                .open(target.join("system/commit-ledger.v1"))
                .unwrap();
            write_all_at(&file, b"bad-head", 0).unwrap();
            Ok(())
        }
    });
    let image = native::inspect(&base).unwrap().unwrap();
    assert!(native::restore_image(
        &base,
        &target,
        image.manifest.end_lsn,
        image.manifest.end_commit_unix_us,
        false,
        |_| Ok(())
    )
    .is_err());
}

#[test]
fn audit_r03_prune_reclaims_only_recognized_atomic_blob_staging() {
    let f = Fixture::new(Policy::Manual);
    f.archive();
    let namespace = f.repo.join("blobs/plain");
    let temporary = namespace.join(format!("{}.tmp-{}", "a".repeat(64), random_uuid()));
    fs::write(&temporary, b"partial crash-leftover blob").unwrap();
    super::management::prune(&f.repo, None, None, true, None).unwrap();
    assert!(!temporary.exists());
    let unknown = namespace.join("user-notes");
    fs::write(&unknown, b"must not be deleted").unwrap();
    assert!(super::management::prune(&f.repo, None, None, true, None).is_err());
    assert_eq!(fs::read(&unknown).unwrap(), b"must not be deleted");
    fs::remove_file(unknown).unwrap();
    let target = f.root.path().join("restored");
    super::restore::restore(
        &f.repo,
        Some(&f.base),
        None,
        Target::Latest,
        &target,
        None,
        false,
    )
    .unwrap();
    let db = StorageQueryEngine::open(target).unwrap();
    assert_eq!(
        query_rows(&db, "SELECT v FROM t"),
        vec![vec![Value::Int(11)]]
    );
    db.shutdown().unwrap();
}

#[test]
#[cfg(unix)]
fn audit_r03_restart_repairs_a_confirmed_ack_with_an_older_lease() {
    restart_ack_boundary(false, false);
}

#[test]
#[cfg(unix)]
fn audit_r03_missing_confirmed_head_cannot_release_the_older_lease() {
    restart_ack_boundary(true, false);
}

#[test]
#[cfg(unix)]
fn audit_r03_catalog_progress_cannot_hide_stale_lease_byte_accounting() {
    restart_ack_boundary(false, true);
}

#[test]
fn audit_workspace_journal_survives_reappearing_unlinks_and_torn_registration() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let path = fs::canonicalize(root.path()).unwrap();
    let destination = path.join("target");
    let operation = DirectoryOperation::new(&destination, "audit-work").unwrap();
    let old = operation.data.parent().unwrap().to_path_buf();
    drop(operation);
    // Model a Drop unlink lost on crash. Ownership remains outside the tree.
    fs::create_dir_all(old.join("data")).unwrap();
    fs::write(old.join("data/partial"), b"interrupted cleanup").unwrap();
    let next = DirectoryOperation::new(&destination, "audit-work").unwrap();
    assert!(!next.data.join("partial").exists());
    drop(next);
    sync_dir(&path).unwrap();
    let journal = fs::read_dir(&path)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "lock"))
        .unwrap();
    fs::write(&journal, b"MYDBOP1\n{\"owner\":").unwrap();
    drop(DirectoryOperation::new(&destination, "audit-work").unwrap());
    fs::write(&journal, b"unrecognized user file").unwrap();
    assert!(DirectoryOperation::new(&destination, "audit-work").is_err());
    assert_eq!(fs::read(&journal).unwrap(), b"unrecognized user file");
}

#[test]
#[cfg(unix)]
fn audit_workspace_corrupt_journal_does_not_delete_unproven_payload() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let path = fs::canonicalize(root.path()).unwrap();
    run_crash(&path, "workspace", "repository_init_ready", "");
    let journal = fs::read_dir(&path)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "lock"))
        .unwrap();
    let operation = fs::read_dir(&path)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.is_dir())
        .unwrap();
    fs::write(&journal, b"MYDBOP1\n{broken").unwrap();
    assert!(DirectoryOperation::new(&path.join("workspace-target"), "audit-work").is_err());
    assert_eq!(
        fs::read(operation.join("data/payload")).unwrap(),
        b"private payload"
    );
}

#[test]
fn audit_time_target_cannot_use_a_later_ineligible_base_as_wal_coverage() {
    let f = Fixture::new(Policy::Manual);
    f.archive();
    f.source.execute("UPDATE t SET v=20 WHERE id=1").unwrap();
    let time = f.source.db_handle().backup_time_fence();
    let later = completed(&f.c, f.c.backup(&f.repo).unwrap())
        .base_id
        .unwrap();
    let repo = f.open();
    let (later_base, _) = repo.base(&later).unwrap();
    let stream = later_base.stream_id.as_deref();
    assert!(later_base.source_end_time > time);
    let outcome = super::restore::restore(
        &f.repo,
        None,
        stream,
        Target::Time(time),
        &f.root.path().join("not-covered"),
        None,
        false,
    );
    assert!(
        outcome.is_err(),
        "a later snapshot falsely proved an unarchived earlier time: {outcome:?}"
    );
    super::restore::restore(
        &f.repo,
        None,
        stream,
        Target::Time(later_base.source_end_time),
        &f.root.path().join("new-base"),
        None,
        false,
    )
    .unwrap();
    completed(&f.c, f.c.backup_wal(&f.repo).unwrap());
    let target = f.root.path().join("covered");
    super::restore::restore(
        &f.repo,
        None,
        stream,
        Target::Time(time),
        &target,
        None,
        false,
    )
    .unwrap();
    let db = StorageQueryEngine::open(target).unwrap();
    assert_eq!(
        query_rows(&db, "SELECT v FROM t"),
        vec![vec![Value::Int(20)]]
    );
    db.shutdown().unwrap();
}

#[cfg(unix)]
fn restart_ack_boundary(damage: bool, advance_catalog: bool) {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let path = fs::canonicalize(root.path()).unwrap();
    let repo = path.join("repo");
    let db = StorageQueryEngine::open(path.join("source")).unwrap();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,p LONGBLOB,INDEX iv(v))")
        .unwrap();
    db.execute("INSERT INTO t VALUES(1,10,REPEAT('x',24000))")
        .unwrap();
    let c = Coordinator::get(db.db_handle()).unwrap();
    c.configure(&repo, Policy::Manual, false, None).unwrap();
    completed(&c, c.backup(&repo).unwrap());
    let old_floor = db.db_handle().capture.leases()["archive"].floor;
    db.shutdown().unwrap();
    drop(c);
    drop(db);
    run_crash(&path, "archive", "source_ack_saved", "");
    let control = super::coordinator::read_control(&path.join("source"))
        .unwrap()
        .unwrap();
    assert!(control.cursor + 1 > old_floor);
    if advance_catalog {
        let other = StorageQueryEngine::open(path.join("other-source")).unwrap();
        let other_c = Coordinator::get(other.db_handle()).unwrap();
        completed(&other_c, other_c.backup(&repo).unwrap());
        other.shutdown().unwrap();
    }
    if damage {
        let repo = Repository::open(&repo, None).unwrap();
        let mut blob = None;
        repo.visit_live(&repo.position().unwrap(), |_, event| {
            if let Publication::Range { range } = event {
                blob = Some(range.chunks[0].hash.clone());
            }
            Ok(())
        })
        .unwrap();
        fs::remove_file(repo.root.join("blobs/plain").join(blob.unwrap())).unwrap();
    }
    let db = StorageQueryEngine::open(path.join("source")).unwrap();
    let c = Coordinator::get(db.db_handle()).unwrap();
    assert_eq!(
        query_rows(&db, "SELECT v FROM t"),
        vec![vec![Value::Int(20)]]
    );
    if damage {
        assert_eq!(
            c.control.lock().unwrap().as_ref().unwrap().lifecycle,
            "BROKEN"
        );
        assert_eq!(db.db_handle().capture.leases()["archive"].floor, old_floor);
    } else {
        assert_eq!(
            db.db_handle().capture.leases()["archive"].floor,
            control.cursor + 1,
            "a confirmed ACK left its obsolete retention lease behind"
        );
        let _checkpoint = db.db_handle().backup_maintenance_capture_guard().unwrap();
        let log = db.db_handle().wal_log();
        let mut log = log.lock();
        let prefixes = log.pin_prefix_after(control.cursor, None).unwrap();
        let mut bytes = 0;
        for prefix in &prefixes {
            crate::wal::visit_pinned_range(
                prefix,
                prefix.first_lsn.max(control.cursor + 1) - 1,
                u64::MAX,
                |_, _, _, _, raw| {
                    bytes += raw.len() as u64;
                    Ok(())
                },
            )
            .unwrap();
        }
        assert_eq!(
            db.db_handle()
                .capture
                .budget
                .pending
                .load(Ordering::Acquire),
            bytes,
            "acknowledged bytes remained charged after restart"
        );
        assert!(db.db_handle().capture.budget.check(1).is_ok());
    }
    db.shutdown().unwrap();
}
