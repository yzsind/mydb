//! First-release operational and scale coverage for the follow-up design.
use super::coordinator::{Coordinator, Policy};
use super::*;
use crate::engine::StorageQueryEngine;

struct Fixture {
    source: StorageQueryEngine,
    coordinator: Arc<Coordinator>,
    repository: PathBuf,
    base: String,
    root: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let sync = crate::platform::fs::FixtureSyncBatch::new(root.path()).unwrap();
        let source = StorageQueryEngine::open(root.path().join("source")).unwrap();
        source
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,INDEX iv(v))")
            .unwrap();
        source.execute("INSERT INTO t VALUES(1,10)").unwrap();
        let coordinator = Coordinator::get(source.db_handle()).unwrap();
        let repository = fs::canonicalize(root.path()).unwrap().join("repository");
        coordinator
            .configure(&repository, Policy::Manual, false, None)
            .unwrap();
        let base = Self::wait(&coordinator, coordinator.backup(&repository).unwrap())
            .base_id
            .unwrap();
        sync.finish().unwrap();
        Self {
            root,
            source,
            coordinator,
            repository,
            base,
        }
    }
    fn wait(c: &Coordinator, job: super::coordinator::Job) -> super::coordinator::Job {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let found = c.jobs().into_iter().find(|j| j.id == job.id).unwrap();
            if found.state != "RUNNING" {
                assert_eq!(found.state, "COMPLETED", "{}", found.error);
                return found;
            }
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // Stop admitted work before engine teardown. The temporary directory
        // drops last, so no final archive/checkpoint of discarded data is needed.
        let _ = self.coordinator.shutdown();
    }
}

#[test]
fn export_rejects_same_length_corruption_against_original_base_manifest() {
    let f = Fixture::new();
    let data = f.repository.join("bases").join(&f.base).join("data");
    let image = native::inspect(&data).unwrap().unwrap();
    let file = image
        .manifest
        .files
        .iter()
        .find(|file| file.len > 0)
        .unwrap();
    let path = data.join(&file.path);
    let mut bytes = fs::read(&path).unwrap();
    bytes[0] ^= 1;
    fs::write(path, bytes).unwrap();
    let output = f.root.path().join("corrupt.bundle");
    assert!(super::management::export(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Base,
        &output,
        None,
    )
    .is_err());
    assert!(!output.exists());
}

#[test]
fn dry_run_validates_target_without_creating_destination_or_parent() {
    let f = Fixture::new();
    let target = f.root.path().join("absent-parent/image");
    super::cli::run(vec![
        "restore".into(),
        "--repository".into(),
        f.repository.display().to_string(),
        "--base".into(),
        f.base.clone(),
        "--data-dir".into(),
        target.display().to_string(),
        "--dry-run".into(),
    ])
    .unwrap();
    assert!(!target.parent().unwrap().exists());
    assert_eq!(f.coordinator.jobs().len(), 1);
}

#[test]
fn preflight_and_restore_share_target_identity_and_exact_base_dependencies() {
    let f = Fixture::new();
    f.source.execute("UPDATE t SET v=20 WHERE id=1").unwrap();
    let done = Fixture::wait(
        &f.coordinator,
        f.coordinator.backup_wal(&f.repository).unwrap(),
    );
    let target = f.root.path().join("restored");
    let plan = super::plan::preflight(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Latest,
        &target,
        None,
    )
    .unwrap();
    assert_eq!(plan.actual_source_lsn, done.durable_through_lsn);
    assert!(plan.wal_frames > 0);
    assert!(plan.maximum_work_bytes.is_none());
    assert!(!target.exists());
    let report = super::restore::restore(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Latest,
        &target,
        None,
        false,
    )
    .unwrap();
    assert_eq!(report.restored_source_lsn, plan.actual_source_lsn);
    let repo = super::repository::Repository::open(&f.repository, None).unwrap();
    let mut blob = None;
    repo.visit_live(&repo.position().unwrap(), |_, event| {
        if let super::repository::Publication::Range { range } = event {
            blob = Some(range.chunks[0].hash.clone());
        }
        Ok(())
    })
    .unwrap();
    fs::remove_file(f.repository.join("blobs/plain").join(blob.unwrap())).unwrap();
    let unused = f.root.path().join("unused");
    assert!(super::plan::preflight(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Latest,
        &unused,
        None
    )
    .is_err());
    let base = super::plan::preflight(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Base,
        &unused,
        None,
    )
    .unwrap();
    assert_eq!(base.wal_frames, 0);
    assert!(!unused.exists());
}

#[test]
fn restore_authenticates_repository_payload_only_once() {
    let f = Fixture::new();
    f.source.execute("UPDATE t SET v=21 WHERE id=1").unwrap();
    Fixture::wait(
        &f.coordinator,
        f.coordinator.backup_wal(&f.repository).unwrap(),
    );
    let path = f.repository.clone();
    super::faults::install("blob_read", &f.repository, move || {
        super::faults::fail("blob_read", &path);
        Ok(())
    });
    let target = f.root.path().join("once");
    super::restore::restore(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Latest,
        &target,
        None,
        false,
    )
    .unwrap();
    let reader = StorageQueryEngine::open(&target).unwrap();
    let QueryResponse::ResultSet(rows) = reader.execute("SELECT v FROM t WHERE id=1").unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rows.rows[0][0], Value::Int(21));
    reader.shutdown().unwrap();
}

#[test]
fn operation_inspection_is_read_only_and_cannot_clean_an_active_owner() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let target = fs::canonicalize(root.path()).unwrap().join("target");
    let work = DirectoryOperation::new(&target, "restore-work").unwrap();
    fs::write(work.data.join("partial"), b"unfinished").unwrap();
    let report = DirectoryOperation::inspect(&target, "restore-work").unwrap();
    assert_eq!(report["workspace"], "ACTIVE");
    assert!(work.data.join("partial").exists());
    assert!(super::cli::run(vec![
        "inspect-operation".into(),
        "--path".into(),
        target.display().to_string(),
        "--kind".into(),
        "restore-work".into(),
        "--apply".into()
    ])
    .is_err());
    drop(work);
    let report = DirectoryOperation::inspect(&target, "restore-work").unwrap();
    assert_eq!(report["workspace"], "NO_PRIVATE_PAYLOAD");
}

#[test]
fn recoverability_and_key_inventory_distinguish_metadata_from_payload_verification() {
    let f = Fixture::new();
    f.source.execute("UPDATE t SET v=22 WHERE id=1").unwrap();
    let archived = Fixture::wait(
        &f.coordinator,
        f.coordinator.backup_wal(&f.repository).unwrap(),
    );
    let report = super::diagnostics::recoverability(&f.repository, None, true).unwrap();
    assert_eq!(report.validation, "BASE_SHA256_AND_WAL_AUTH_CRC");
    assert_eq!(report.streams.len(), 1);
    let coverage = &report.streams[0].intervals;
    assert_eq!(coverage.len(), 1);
    assert_eq!(coverage[0].through_lsn, archived.durable_through_lsn);
    let keys = super::diagnostics::keys(&f.repository, None).unwrap();
    assert_eq!(keys["baseline_encrypted"], false);
    assert_eq!(keys["dependent_ranges"], 1);
    Fixture::wait(
        &f.coordinator,
        f.coordinator
            .configure(&f.repository, Policy::Disabled, false, None)
            .unwrap()
            .unwrap(),
    );
    let closed = super::diagnostics::recoverability(&f.repository, None, false).unwrap();
    assert!(closed.streams[0].closed_at_lsn.is_some());
    assert!(!closed.streams[0].discarded);
}

#[test]
fn bounded_queue_records_monotonic_jobs_and_drains_every_admitted_task() {
    let f = Fixture::new();
    let limits = super::resources::limits().unwrap();
    let mut releases = Vec::new();
    for _ in 0..limits.workers {
        let (entered, ready) = mpsc::sync_channel(0);
        let (release, wait) = mpsc::sync_channel(0);
        f.coordinator.test_blocked_job(entered, wait).unwrap();
        ready.recv_timeout(Duration::from_secs(30)).unwrap();
        releases.push(release);
    }
    for _ in 0..limits.queue_capacity {
        f.coordinator.test_noop().unwrap();
    }
    assert!(f.coordinator.test_noop().is_err());
    let list = f.coordinator.jobs();
    assert!(list.windows(2).all(|w| w[0].sequence > w[1].sequence));
    for release in releases {
        release.send(()).unwrap();
    }
    f.coordinator.shutdown().unwrap();
    for job in f.coordinator.jobs() {
        assert_ne!(job.state, "RUNNING");
        assert!(job.created_unix_ms > 0);
        assert!(job.completed_unix_ms >= job.created_unix_ms);
    }
    assert!(f.coordinator.test_noop().is_err());
}

#[test]
fn capture_file_descriptors_are_bounded_across_inventory_growth() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let root = fs::canonicalize(root.path()).unwrap();
    fs::create_dir_all(root.join("objects")).unwrap();
    let limits = super::resources::limits().unwrap();
    let threshold = (limits.open_files / 4 / limits.workers).max(1);
    // Cross the actual admission boundary, then exceed the entire descriptor
    // budget. Thousands of additional files exercise the same reopening path.
    let count = limits.open_files * 2 + 1;
    for i in 0..count {
        if i == threshold {
            let files = collect_capture_files(&root).unwrap();
            assert_eq!(files.len(), threshold);
            assert!(files
                .iter()
                .all(|f| matches!(f.file, GenerationFile::Open(_))));
        }
        fs::write(
            root.join(format!("objects/{i:08}.bin")),
            (i as u64).to_le_bytes(),
        )
        .unwrap();
        if i == threshold {
            let files = collect_capture_files(&root).unwrap();
            assert_eq!(files.len(), threshold + 1);
            assert!(files
                .iter()
                .all(|f| matches!(f.file, GenerationFile::Reopen { .. })));
        }
    }
    let files = collect_capture_files(&root).unwrap();
    assert_eq!(files.len(), count);
    assert!(files
        .iter()
        .all(|f| matches!(f.file, GenerationFile::Reopen { .. })));
    for i in BTreeSet::from([0, threshold - 1, threshold, count - 1]) {
        let reference = root.join(format!("objects/{i:08}.bin"));
        fs::rename(&reference, root.join(format!("original-{i}"))).unwrap();
        fs::write(reference, b"replaced").unwrap();
    }
    for (i, file) in files.iter().enumerate() {
        let mut bytes = [0; 8];
        let handle = file.file.open().unwrap();
        read_exact_at(&handle, &mut bytes, 0).unwrap();
        assert_eq!(u64::from_le_bytes(bytes), i as u64);
    }
    drop(files);
    let report = DirectoryOperation::inspect(
        &root.join("system/backup-generation-pins"),
        "generation-pins",
    )
    .unwrap();
    assert_eq!(report["workspace"], "NO_PRIVATE_PAYLOAD");
}

#[test]
fn archive_and_capture_stream_more_than_five_hundred_twelve_segments() {
    let f = Fixture::new();
    // Segment construction is setup for the capture/archive contract. Drain
    // the completed source once before either operation observes the fixture.
    let sync = crate::platform::fs::FixtureSyncBatch::new(f.source.db_handle().data_dir()).unwrap();
    f.source
        .db_handle()
        .wal_log()
        .lock()
        .set_max_segment_bytes(4096);
    // Exercise the real segment and archive boundaries with one transaction.
    // Per-row autocommit barriers add no coverage to the file-count contract.
    let mut session = crate::engine::SessionContext::default();
    f.source
        .execute_with_session(&mut session, "START TRANSACTION")
        .unwrap();
    for _ in 0..520 {
        f.source
            .execute_with_session(&mut session, "UPDATE t SET v=v+1 WHERE id=1")
            .unwrap();
        f.source
            .db_handle()
            .wal_log()
            .lock()
            .rotate_segment()
            .unwrap();
    }
    f.source
        .execute_with_session(&mut session, "COMMIT")
        .unwrap();
    sync.finish().unwrap();
    let captured = snapshot_wal_files(f.source.db_handle()).unwrap();
    assert!(captured.files.len() > 512);
    assert!(captured
        .files
        .iter()
        .all(|f| matches!(f.file, GenerationFile::Reopen { .. })));
    drop(captured);
    let archived = Fixture::wait(
        &f.coordinator,
        f.coordinator.backup_wal(&f.repository).unwrap(),
    );
    let repo = super::repository::Repository::open(&f.repository, None).unwrap();
    let mut ranges = 0;
    repo.visit_live(&repo.position().unwrap(), |_, event| {
        if let super::repository::Publication::Range { range } = event {
            assert!(range.source_spans.len() <= 256);
            ranges += 1;
        }
        Ok(())
    })
    .unwrap();
    assert!(ranges >= 3);
    let target = f.root.path().join("many-segments");
    let report = super::restore::restore(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Latest,
        &target,
        None,
        false,
    )
    .unwrap();
    assert_eq!(report.restored_source_lsn, archived.durable_through_lsn);
    let reader = StorageQueryEngine::open(&target).unwrap();
    let QueryResponse::ResultSet(rows) = reader.execute("SELECT v FROM t WHERE id=1").unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rows.rows[0][0], Value::Int(530));
    reader.shutdown().unwrap();
}

#[test]
fn checkpoint_serves_live_state_without_rereading_cold_history_and_preserves_receipts() {
    let f = Fixture::new();
    let repo = super::repository::Repository::open(&f.repository, None).unwrap();
    let old = repo.position().unwrap();
    f.source.execute("UPDATE t SET v=23 WHERE id=1").unwrap();
    Fixture::wait(
        &f.coordinator,
        f.coordinator.backup_wal(&f.repository).unwrap(),
    );
    let checkpoint = repo.checkpoint().unwrap();
    assert_eq!(checkpoint.records, 2);
    let current = repo.position().unwrap();
    let mut before = Vec::new();
    repo.visit_live(&current, |_, event| {
        before.push(serde_json::to_string(&event).unwrap());
        Ok(())
    })
    .unwrap();
    assert_eq!(before.len(), 2);
    repo.visit_since(&current, &old, |_| Ok(())).unwrap();
    let cold = f.repository.join(format!(
        "catalog/generations/{:020}-{}",
        old.generation, old.digest
    ));
    let saved = fs::read(&cold).unwrap();
    fs::write(&cold, b"unread cold object").unwrap();
    let mut after = Vec::new();
    repo.visit_live(&current, |_, event| {
        after.push(serde_json::to_string(&event).unwrap());
        Ok(())
    })
    .unwrap();
    assert_eq!(after, before);
    fs::write(cold, saved).unwrap();
    let target = f.root.path().join("checkpoint-restore");
    super::restore::restore(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Latest,
        &target,
        None,
        false,
    )
    .unwrap();
    let mut bytes = fs::read(
        f.repository
            .join("catalog/checkpoints")
            .join(&checkpoint.digest),
    )
    .unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    fs::write(
        f.repository
            .join("catalog/checkpoints")
            .join(&checkpoint.digest),
        bytes,
    )
    .unwrap();
    assert!(repo.visit_live(&current, |_, _| Ok(())).is_err());
}

#[test]
fn checkpoint_pointer_ahead_of_a_pinned_reader_cannot_change_its_root() {
    let f = Fixture::new();
    let repo = super::repository::Repository::open(&f.repository, None).unwrap();
    let pinned = repo.position().unwrap();
    repo.checkpoint().unwrap();
    let mut count = 0;
    repo.visit_live(&pinned, |_, event| {
        if matches!(event, super::repository::Publication::Base { .. }) {
            count += 1;
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 1);
    repo.visit_since(&repo.position().unwrap(), &pinned, |_| Ok(()))
        .unwrap();
}

#[test]
fn resumable_import_revalidates_owned_blocks_and_binds_the_complete_input() {
    let f = Fixture::new();
    let bundle = f.root.path().join("bundle");
    super::management::export(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Base,
        &bundle,
        None,
    )
    .unwrap();
    let target = fs::canonicalize(f.root.path()).unwrap().join("imported");
    super::faults::fail("import_block_written", &target);
    assert!(super::management::import_with_resume(&target, &bundle, None, true).is_err());
    assert!(!target.exists());
    let owner = DirectoryOperation::inspect(&target, "import-work").unwrap();
    assert_eq!(owner["workspace"], "INTERRUPTED_PRIVATE");
    let identity = owner["input_digest"].as_str().unwrap().to_string();
    let altered = f.root.path().join("altered");
    let mut bytes = fs::read(&bundle).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    fs::write(&altered, &bytes).unwrap();
    assert!(super::management::import_with_resume(&target, &altered, None, true).is_err());
    assert_eq!(
        DirectoryOperation::inspect(&target, "import-work").unwrap()["input_digest"],
        identity
    );
    super::management::import_with_resume(&target, &bundle, None, true).unwrap();
    let imported = super::repository::Repository::open(&target, None).unwrap();
    imported.base(&f.base).unwrap();
    assert_eq!(
        DirectoryOperation::inspect(&target, "import-work").unwrap()["workspace"],
        "NO_PRIVATE_PAYLOAD"
    );
    let invalid = f.root.path().join("invalid-import");
    assert!(super::management::import(&invalid, &altered, None).is_err());
    assert!(!invalid.exists());
}

#[test]
fn interrupted_import_retains_no_capability_to_follow_a_replaced_workspace_link() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let root = fs::canonicalize(root.path()).unwrap();
    let target = root.join("target");
    let id = sha256_hex(b"immutable-input");
    let work = DirectoryOperation::resume(&target, "import-work", id.clone()).unwrap();
    let payload = work.data.clone();
    drop(work);
    let external = root.join("external");
    fs::create_dir(&external).unwrap();
    fs::write(external.join("user-file"), b"keep").unwrap();
    fs::remove_dir(&payload).unwrap();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&external, &payload).unwrap();
        assert!(DirectoryOperation::resume(&target, "import-work", id).is_err());
        assert_eq!(fs::read(external.join("user-file")).unwrap(), b"keep");
    }
}

#[test]
fn durable_verification_records_distinguish_failure_and_stale_success() {
    let f = Fixture::new();
    let verify = || {
        super::cli::run(vec![
            "verify".into(),
            "--repository".into(),
            f.repository.display().to_string(),
            "--record".into(),
        ])
    };
    verify().unwrap();
    let initial = super::diagnostics::latest(&f.repository).unwrap();
    assert_eq!(initial["VERIFY"]["outcome"], "PASSED");
    assert_eq!(initial["VERIFY"]["stale"], false);
    f.source.execute("UPDATE t SET v=24 WHERE id=1").unwrap();
    Fixture::wait(
        &f.coordinator,
        f.coordinator.backup_wal(&f.repository).unwrap(),
    );
    assert_eq!(
        super::diagnostics::latest(&f.repository).unwrap()["VERIFY"]["stale"],
        true
    );
    let blob = fs::read_dir(f.repository.join("blobs/plain"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|p| p.file_name().unwrap().to_string_lossy().len() == 64)
        .unwrap();
    let mut bytes = fs::read(&blob).unwrap();
    bytes[0] ^= 1;
    fs::write(&blob, bytes).unwrap();
    assert!(verify().is_err());
    let failed = super::diagnostics::latest(&f.repository).unwrap();
    assert_eq!(failed["VERIFY"]["outcome"], "FAILED");
    assert_eq!(failed["VERIFY"]["stale"], false);
}

#[test]
fn budget_emergency_patch_does_not_wait_for_bulk_archive_lock() {
    let f = Fixture::new();
    let (entered, ready) = mpsc::sync_channel(0);
    let (release, wait) = mpsc::sync_channel(0);
    f.coordinator.test_hold_archive(entered, wait).unwrap();
    ready.recv_timeout(Duration::from_secs(30)).unwrap();
    let coordinator = f.coordinator.clone();
    let repository = f.repository.clone();
    let (completed, result) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        let changed = coordinator.configure(&repository, Policy::Manual, false, Some(512 << 20));
        completed.send(changed.is_ok()).unwrap();
    });
    let changed = result.recv_timeout(Duration::from_secs(3));
    release.send(()).unwrap();
    thread.join().unwrap();
    assert_eq!(changed.unwrap(), true);
    assert_eq!(
        f.source
            .db_handle()
            .capture
            .budget
            .limit
            .load(Ordering::Acquire),
        512 << 20
    );
}

#[test]
fn capture_metrics_count_reads_and_all_frozen_stages() {
    let f = Fixture::new();
    let job = f
        .coordinator
        .jobs()
        .into_iter()
        .find(|job| job.kind == "DATABASE")
        .unwrap();
    assert_eq!(job.phase_remaining_bytes, Some(0));
    assert!(job.files_done > 0);
    let repo = super::repository::Repository::open(&f.repository, None).unwrap();
    let (_, image) = repo.base(&f.base).unwrap();
    let stats = &image.manifest.capture_stats;
    assert!(stats["copy_read_bytes"] >= stats["captured_bytes"] * 2);
    assert!(
        stats["initial_freeze_us"]
            >= stats["initial_drain_us"] + stats["initial_wal_and_lease_sync_us"]
    );
    let final_parts = [
        "final_drain_us",
        "final_checkpoint_us",
        "final_wal_snapshot_and_sync_us",
        "final_inventory_us",
        "final_private_copy_us",
        "final_handoff_us",
    ]
    .into_iter()
    .map(|key| stats[key])
    .sum::<u64>();
    assert!(stats["final_freeze_us"] >= final_parts);
    assert!(image.manifest.final_barrier_ms >= stats["final_freeze_us"] / 1000);
}

#[test]
fn optional_commit_feedback_is_bounded_and_expires_when_foreground_is_idle() {
    assert_eq!(
        super::resources::feedback_delay(0, 1_000_000, 0),
        Duration::ZERO
    );
    assert_eq!(
        super::resources::feedback_delay(100, 200, 1001),
        Duration::ZERO
    );
    assert_eq!(
        super::resources::feedback_delay(100, 200, 0),
        Duration::from_micros(100)
    );
    assert_eq!(
        super::resources::feedback_delay(1, 1_000_000, 0),
        Duration::from_millis(10)
    );
    let f = Fixture::new();
    let pressure = &f.source.db_handle().capture.commit_pressure;
    pressure.enabled.store(true, Ordering::Relaxed);
    f.source.execute("START TRANSACTION").unwrap();
    f.source.execute("SELECT 1").unwrap();
    f.source.execute("COMMIT").unwrap();
    assert_eq!(pressure.ewma_us.load(Ordering::Relaxed), 0);
    f.source.execute("UPDATE t SET v=25 WHERE id=1").unwrap();
    assert!(pressure.ewma_us.load(Ordering::Relaxed) > 0);
}

#[test]
fn packed_lineage_gc_preserves_old_receipts_and_bounds_inheritance_to_live_state() {
    let f = Fixture::new();
    let repo = super::repository::Repository::open(&f.repository, None).unwrap();
    let old = repo.position().unwrap();
    let (_, base) = repo.base(&f.base).unwrap();
    let control = f.coordinator.control.lock().unwrap().clone().unwrap();
    let stream = control.stream_id.unwrap();
    repo.test_seed_loose_fences(
        &stream,
        base.manifest.end_lsn,
        base.manifest.end_commit_unix_us,
        2100,
    )
    .unwrap();
    repo.checkpoint().unwrap();
    repo.verify_history().unwrap();
    assert!(repo.gc_history().unwrap() > 2000);
    assert_eq!(repo.gc_history().unwrap(), 0);
    assert!(
        fs::read_dir(f.repository.join("catalog/generations"))
            .unwrap()
            .count()
            < 10
    );
    let opened = super::repository::Repository::open(&f.repository, None).unwrap();
    let current = opened.position().unwrap();
    let mut inherited = 0;
    opened
        .visit_since(&current, &old, |_| {
            inherited += 1;
            Ok(())
        })
        .unwrap();
    assert!(
        inherited < 10,
        "inherited {inherited} historical events instead of bounded live state"
    );
    let mut forged = old.clone();
    forged.digest = sha256_hex(b"unrelated old catalog position");
    assert!(opened.visit_since(&current, &forged, |_| Ok(())).is_err());
    let mut whole = 0;
    opened
        .visit(&current, |_, _| {
            whole += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(whole, current.generation);
    opened.verify_history().unwrap();
    f.source.execute("UPDATE t SET v=27 WHERE id=1").unwrap();
    Fixture::wait(
        &f.coordinator,
        f.coordinator.backup_wal(&f.repository).unwrap(),
    );
    let target = f.root.path().join("packed-restore");
    super::restore::restore(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Latest,
        &target,
        None,
        false,
    )
    .unwrap();
    let reader = StorageQueryEngine::open(&target).unwrap();
    let QueryResponse::ResultSet(rows) = reader.execute("SELECT v FROM t WHERE id=1").unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rows.rows[0][0], Value::Int(27));
    reader.shutdown().unwrap();
}

#[test]
fn seal_releases_child_grants_before_parent_sync_with_a_single_bulk_slot() {
    std::env::set_var("MYDB_BACKUP_COPY_THREADS", "1");
    let f = Fixture::new();
    let grant = super::resources::BulkPermit::acquire(false).unwrap();
    assert!(grant.is_some());
    drop(grant);
    let repo = super::repository::Repository::open(&f.repository, None).unwrap();
    repo.base(&f.base).unwrap();
}

#[test]
fn bandwidth_wait_can_be_cancelled_without_waiting_for_a_low_rate_deadline() {
    std::env::set_var("MYDB_BACKUP_BANDWIDTH_BYTES_PER_SEC", "1");
    let flag = Arc::new(AtomicBool::new(false));
    let cancel = flag.clone();
    let (entered, ready) = mpsc::sync_channel(0);
    let (done, result) = mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        let _context = super::resources::ContextScope::worker(cancel);
        entered.send(()).unwrap();
        done.send(super::resources::throttle(CHUNK_BYTES as u64).is_err())
            .unwrap();
    });
    ready.recv_timeout(Duration::from_secs(3)).unwrap();
    flag.store(true, Ordering::Release);
    assert!(result.recv_timeout(Duration::from_secs(3)).unwrap());
    worker.join().unwrap();
}

#[test]
fn cancelled_full_waiting_for_process_admission_releases_its_capture_reservation() {
    let first = Fixture::new();
    let second = Fixture::new();
    let mut releases = Vec::new();
    for _ in 0..super::resources::limits().unwrap().workers {
        let (entered, ready) = mpsc::sync_channel(0);
        let (release, wait) = mpsc::sync_channel(0);
        first.coordinator.test_blocked_job(entered, wait).unwrap();
        ready.recv_timeout(Duration::from_secs(3)).unwrap();
        releases.push(release);
    }
    let queued = second.coordinator.backup(&second.repository).unwrap();
    second.coordinator.cancel(&queued.id).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut terminal = None;
    while Instant::now() < deadline {
        let job = second
            .coordinator
            .jobs()
            .into_iter()
            .find(|j| j.id == queued.id)
            .unwrap();
        if job.state != "RUNNING" {
            terminal = Some(job);
            break;
        }
        std::thread::yield_now();
    }
    for release in releases {
        release.send(()).unwrap();
    }
    let result = terminal.expect("cancelled admission remained blocked");
    assert_eq!(result.state, "CANCELLED");
    assert_eq!(result.started_unix_ms, 0);
    Fixture::wait(
        &second.coordinator,
        second.coordinator.backup(&second.repository).unwrap(),
    );
}

#[test]
fn fixed_target_restore_resumes_only_verified_base_blocks_after_recovery_interruption() {
    let f = Fixture::new();
    let target = fs::canonicalize(f.root.path())
        .unwrap()
        .join("resume-restore");
    super::faults::fail("restore_base_cached", &target);
    assert!(super::restore::restore_with_resume(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Base,
        &target,
        None,
        false,
        true
    )
    .is_err());
    assert!(!target.exists());
    let report = super::restore::restore_with_resume(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Base,
        &target,
        None,
        false,
        true,
    )
    .unwrap();
    assert!(report.reused_base_bytes > 0);
    let reader = StorageQueryEngine::open(&target).unwrap();
    let QueryResponse::ResultSet(rows) = reader.execute("SELECT v FROM t WHERE id=1").unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rows.rows[0][0], Value::Int(10));
    reader.shutdown().unwrap();
    assert!(super::restore::restore_with_resume(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Latest,
        &f.root.path().join("invalid-resume"),
        None,
        false,
        true
    )
    .is_err());
}

#[test]
fn archive_ack_cannot_overwrite_a_budget_patch_accepted_during_bulk_work() {
    let f = Fixture::new();
    f.source.execute("UPDATE t SET v=31 WHERE id=1").unwrap();
    let (entered, ready) = mpsc::sync_channel(0);
    let (release, wait) = mpsc::sync_channel(0);
    super::faults::install("archive_before_publish", &f.repository, move || {
        entered.send(()).unwrap();
        wait.recv().unwrap();
        Ok(())
    });
    let request = f.coordinator.backup_wal(&f.repository).unwrap();
    ready.recv_timeout(Duration::from_secs(30)).unwrap();
    let changed = f
        .coordinator
        .configure(&f.repository, Policy::Manual, false, Some(512 << 20));
    release.send(()).unwrap();
    changed.unwrap();
    Fixture::wait(&f.coordinator, request);
    assert_eq!(
        f.coordinator
            .control
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .max_pending_wal_bytes,
        512 << 20
    );
    assert_eq!(
        f.source
            .db_handle()
            .capture
            .budget
            .limit
            .load(Ordering::Acquire),
        512 << 20
    );
}

#[test]
fn one_off_full_reports_its_own_repository_and_immutable_boundary() {
    let f = Fixture::new();
    let original = f.coordinator.control.lock().unwrap().clone().unwrap();
    f.source.execute("UPDATE t SET v=32 WHERE id=1").unwrap();
    let other = f.root.path().join("one-off");
    let done = Fixture::wait(&f.coordinator, f.coordinator.backup(&other).unwrap());
    let repository = super::repository::Repository::open(&other, None).unwrap();
    assert_eq!(
        done.repository_id.as_deref(),
        Some(repository.header.id.as_str())
    );
    assert_eq!(done.durable_through_lsn, done.requested_lsn);
    assert_eq!(done.covered_time, done.requested_source_time);
    assert!(done.durable_through_lsn > original.cursor);
    assert_eq!(
        done.catalog_generation,
        repository.position().unwrap().generation
    );
    assert_eq!(
        f.coordinator
            .control
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .repository_id,
        original.repository_id
    );
}

#[test]
fn reapplying_policy_proves_a_repaired_head_before_rearming_a_broken_stream() {
    let f = Fixture::new();
    f.source.execute("UPDATE t SET v=33 WHERE id=1").unwrap();
    Fixture::wait(
        &f.coordinator,
        f.coordinator.backup_wal(&f.repository).unwrap(),
    );
    let blob = fs::read_dir(f.repository.join("blobs/plain"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.file_name().unwrap().to_string_lossy().len() == 64)
        .unwrap();
    let saved = fs::read(&blob).unwrap();
    fs::remove_file(&blob).unwrap();
    assert!(f.coordinator.test_verify_head().is_err());
    assert_eq!(
        f.coordinator
            .control
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .lifecycle,
        "BROKEN"
    );
    assert!(
        f.coordinator
            .configure(&f.repository, Policy::Manual, false, None)
            .is_err(),
        "a policy reapply accepted an unrepaired acknowledged head"
    );
    fs::write(blob, saved).unwrap();
    f.coordinator
        .configure(&f.repository, Policy::Manual, false, None)
        .unwrap();
    assert_eq!(
        f.coordinator
            .control
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .lifecycle,
        "ACTIVE"
    );
    f.source.execute("UPDATE t SET v=34 WHERE id=1").unwrap();
    Fixture::wait(
        &f.coordinator,
        f.coordinator.backup_wal(&f.repository).unwrap(),
    );
}

#[test]
fn discard_records_the_last_confirmed_position_without_inventing_wal_coverage() {
    let f = Fixture::new();
    f.source.execute("UPDATE t SET v=35 WHERE id=1").unwrap();
    let archived = Fixture::wait(
        &f.coordinator,
        f.coordinator.backup_wal(&f.repository).unwrap(),
    );
    f.source.execute("UPDATE t SET v=36 WHERE id=1").unwrap();
    f.coordinator
        .configure(&f.repository, Policy::Disabled, true, None)
        .unwrap();
    let catalog = super::diagnostics::recoverability(&f.repository, None, true).unwrap();
    assert!(catalog.streams[0].discarded);
    assert_eq!(
        catalog.streams[0].closed_at_lsn,
        Some(archived.durable_through_lsn)
    );
    assert_eq!(
        catalog.streams[0].intervals.last().unwrap().through_lsn,
        archived.durable_through_lsn
    );
    assert!(!f
        .source
        .db_handle()
        .capture
        .leases()
        .contains_key("archive"));
    let target = f.root.path().join("discard-restore");
    super::restore::restore(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Latest,
        &target,
        None,
        false,
    )
    .unwrap();
    let reader = StorageQueryEngine::open(&target).unwrap();
    let QueryResponse::ResultSet(rows) = reader.execute("SELECT v FROM t WHERE id=1").unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rows.rows[0][0], Value::Int(35));
    reader.shutdown().unwrap();
}

#[test]
fn key_rotation_keeps_old_dependencies_and_verifies_the_new_baseline_handoff() {
    let f = Fixture::new();
    Fixture::wait(
        &f.coordinator,
        f.coordinator
            .configure(&f.repository, Policy::Disabled, false, None)
            .unwrap()
            .unwrap(),
    );
    let old_key = f.root.path().join("old-key");
    let new_key = f.root.path().join("new-key");
    fs::write(&old_key, "42".repeat(32)).unwrap();
    fs::write(&new_key, "24".repeat(32)).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for key in [&old_key, &new_key] {
            fs::set_permissions(key, fs::Permissions::from_mode(0o600)).unwrap();
        }
    }
    let old = f.root.path().join("encrypted-old");
    let new = f.root.path().join("encrypted-new");
    super::repository::Repository::init(&old, Some("rotation-old"), Some(&old_key)).unwrap();
    super::repository::Repository::init(&new, Some("rotation-new"), Some(&new_key)).unwrap();
    std::env::set_var("MYDB_BACKUP_KEY_FILE", &old_key);
    f.coordinator
        .configure(&old, Policy::Manual, false, None)
        .unwrap();
    let old_base = Fixture::wait(&f.coordinator, f.coordinator.backup(&old).unwrap())
        .base_id
        .unwrap();
    let bundle = f.root.path().join("plaintext-base-in-encrypted-namespace");
    super::management::export(
        &old,
        Some(&old_base),
        None,
        super::restore::Target::Base,
        &bundle,
        None,
    )
    .unwrap();
    std::env::remove_var("MYDB_BACKUP_KEY_FILE");
    let imported = f.root.path().join("base-only-no-key");
    super::management::import(&imported, &bundle, None).unwrap();
    super::repository::Repository::open(&imported, None)
        .unwrap()
        .base(&old_base)
        .unwrap();
    std::env::set_var("MYDB_BACKUP_KEY_FILE", &old_key);
    f.source.execute("UPDATE t SET v=41 WHERE id=1").unwrap();
    Fixture::wait(&f.coordinator, f.coordinator.backup_wal(&old).unwrap());
    Fixture::wait(
        &f.coordinator,
        f.coordinator
            .configure(&old, Policy::Disabled, false, None)
            .unwrap()
            .unwrap(),
    );
    // Applications are quiescent across this first-release repository switch.
    // No cross-repository continuous-PITR claim is inferred from two snapshots.
    std::env::set_var("MYDB_BACKUP_KEY_FILE", &new_key);
    f.coordinator
        .configure(&new, Policy::Manual, false, None)
        .unwrap();
    let new_base = Fixture::wait(&f.coordinator, f.coordinator.backup(&new).unwrap())
        .base_id
        .unwrap();
    let dependencies = super::diagnostics::keys(&old, None).unwrap();
    assert_eq!(dependencies["wal_key_id"], "rotation-old");
    assert_eq!(dependencies["dependent_ranges"], 1);
    assert!(!dependencies["key_error"].is_null());
    assert_eq!(dependencies["baseline_encrypted"], false);
    for (repository, base, key, target, value, point) in [
        (
            &old,
            &old_base,
            &old_key,
            "old-restored",
            41,
            super::restore::Target::Latest,
        ),
        (
            &new,
            &new_base,
            &new_key,
            "new-base",
            41,
            super::restore::Target::Base,
        ),
    ] {
        let path = f.root.path().join(target);
        super::restore::restore(repository, Some(base), None, point, &path, Some(key), false)
            .unwrap();
        let reader = StorageQueryEngine::open(&path).unwrap();
        let QueryResponse::ResultSet(rows) = reader.execute("SELECT v FROM t WHERE id=1").unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(rows.rows[0][0], Value::Int(value));
        reader.shutdown().unwrap();
    }
    f.source.execute("UPDATE t SET v=42 WHERE id=1").unwrap();
    Fixture::wait(&f.coordinator, f.coordinator.backup_wal(&new).unwrap());
    assert!(super::plan::preflight(
        &new,
        Some(&new_base),
        None,
        super::restore::Target::Latest,
        &f.root.path().join("wrong-key"),
        Some(&old_key)
    )
    .is_err());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&old_key, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(super::repository::Repository::open(&old, Some(&old_key)).is_err());
        fs::set_permissions(&old_key, fs::Permissions::from_mode(0o600)).unwrap();
    }
}

#[test]
#[cfg(unix)]
fn followup_crash_worker() {
    let Some(root) = std::env::var_os("MYDB_FOLLOWUP_CRASH_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let mode = std::env::var("MYDB_FOLLOWUP_CRASH_MODE").unwrap();
    let point = std::env::var("MYDB_FOLLOWUP_CRASH_POINT").unwrap();
    let base = std::env::var("MYDB_FOLLOWUP_CRASH_BASE").unwrap();
    let point = match point.as_str() {
        "import_block_written" => "import_block_written",
        "restore_base_cached" => "restore_base_cached",
        "catalog_checkpoint_objects_published" => "catalog_checkpoint_objects_published",
        "catalog_checkpoint_committed" => "catalog_checkpoint_committed",
        "catalog_checkpoint_pointer_published" => "catalog_checkpoint_pointer_published",
        "catalog_history_before_delete" => "catalog_history_before_delete",
        "catalog_history_after_delete" => "catalog_history_after_delete",
        _ => panic!("unknown fault"),
    };
    let path = if mode == "import" {
        root.join("imported")
    } else if mode == "restore" {
        root.join("image")
    } else {
        root.join("repository")
    };
    super::faults::install(point, &path, super::faults::crash_process);
    match mode.as_str() {
        "import" => {
            super::management::import_with_resume(&path, &root.join("bundle"), None, true).unwrap()
        }
        "restore" => {
            super::restore::restore_with_resume(
                &root.join("repository"),
                Some(&base),
                None,
                super::restore::Target::Base,
                &path,
                None,
                false,
                true,
            )
            .unwrap();
        }
        "checkpoint" => {
            super::repository::Repository::open(&path, None)
                .unwrap()
                .checkpoint()
                .unwrap();
        }
        "history" => {
            super::repository::Repository::open(&path, None)
                .unwrap()
                .gc_history()
                .unwrap();
        }
        _ => panic!("unknown crash mode"),
    }
    panic!("crash boundary was not reached");
}

#[cfg(unix)]
fn killed_followup(root: &Path, mode: &str, point: &str, base: &str) {
    use std::os::unix::process::ExitStatusExt;
    let log = std::fs::File::create(root.join(format!("followup-kill-{point}.log"))).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "backup::followup_tests::followup_crash_worker",
            "--nocapture",
        ])
        .env("MYDB_FOLLOWUP_CRASH_ROOT", root)
        .env("MYDB_FOLLOWUP_CRASH_MODE", mode)
        .env("MYDB_FOLLOWUP_CRASH_POINT", point)
        .env("MYDB_FOLLOWUP_CRASH_BASE", base)
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .status()
        .unwrap();
    assert_eq!(status.signal(), Some(libc::SIGKILL), "{mode}/{point}");
}

#[test]
#[cfg(unix)]
fn sigkill_resumable_import_and_restore_revalidate_before_publication() {
    let f = Fixture::new();
    let root = fs::canonicalize(f.root.path()).unwrap();
    let bundle = root.join("bundle");
    super::management::export(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Base,
        &bundle,
        None,
    )
    .unwrap();
    killed_followup(&root, "import", "import_block_written", &f.base);
    assert!(!root.join("imported").exists());
    super::management::import_with_resume(&root.join("imported"), &bundle, None, true).unwrap();
    super::repository::Repository::open(&root.join("imported"), None)
        .unwrap()
        .base(&f.base)
        .unwrap();
    killed_followup(&root, "restore", "restore_base_cached", &f.base);
    assert!(!root.join("image").exists());
    let report = super::restore::restore_with_resume(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Base,
        &root.join("image"),
        None,
        false,
        true,
    )
    .unwrap();
    assert!(report.reused_base_bytes > 0);
    native::verify(&root.join("image")).unwrap();
}

#[test]
#[cfg(unix)]
fn sigkill_checkpoint_and_cold_sweep_preserve_exact_old_receipt_proofs() {
    for point in [
        "catalog_checkpoint_objects_published",
        "catalog_checkpoint_committed",
        "catalog_checkpoint_pointer_published",
        "catalog_history_before_delete",
        "catalog_history_after_delete",
    ] {
        let f = Fixture::new();
        let root = fs::canonicalize(f.root.path()).unwrap();
        let repository = super::repository::Repository::open(&f.repository, None).unwrap();
        let old = repository.position().unwrap();
        if point.starts_with("catalog_history") {
            repository.checkpoint().unwrap();
        }
        killed_followup(
            &root,
            if point.starts_with("catalog_history") {
                "history"
            } else {
                "checkpoint"
            },
            point,
            &f.base,
        );
        repository.checkpoint().unwrap();
        repository.gc_history().unwrap();
        repository.verify_history().unwrap();
        repository
            .visit_since(&repository.position().unwrap(), &old, |_| Ok(()))
            .unwrap();
        repository.base(&f.base).unwrap();
    }
}

#[test]
#[ignore = "explicit year-scale metadata validation; run separately from ordinary regression timing"]
fn packed_year_of_minute_archives_has_bounded_reads_and_exact_receipt_membership() {
    let f = Fixture::new();
    f.source.shutdown().unwrap();
    let repository = super::repository::Repository::open(&f.repository, None).unwrap();
    let state = f.coordinator.control.lock().unwrap().clone().unwrap();
    let (old, current) = repository
        .test_seed_packed_year(
            state.stream_id.as_deref().unwrap(),
            state.cursor,
            state.covered_time,
            365 * 24 * 60,
        )
        .unwrap();
    let reopened = super::repository::Repository::open(&f.repository, None).unwrap();
    let before = super::resources::io_snapshot();
    let mut inherited = 0;
    reopened
        .visit_since(&current, &old, |_| {
            inherited += 1;
            Ok(())
        })
        .unwrap();
    let after = super::resources::io_snapshot();
    assert_eq!(inherited, 1);
    let lineage_read = after["lineage_read_bytes"].as_u64().unwrap()
        - before["lineage_read_bytes"].as_u64().unwrap();
    assert!(
        lineage_read < 4 * 1024 * 1024,
        "loaded {lineage_read} bytes for a half-year-old position"
    );
    reopened.verify_history().unwrap();
    let report = super::diagnostics::recoverability(&f.repository, None, false).unwrap();
    assert_eq!(report.streams.len(), 1);
    assert_eq!(report.streams[0].intervals.len(), 1);
    println!(
        "{}",
        serde_json::json!({"events":365*24*60,"cadence_seconds":60,"simulated_year_seconds":365*24*60*60,"inherited_live_events":inherited,"membership_read_bytes":lineage_read,"loose_generation_files":fs::read_dir(f.repository.join("catalog/generations")).unwrap().count(),"history_pack_files":fs::read_dir(f.repository.join("catalog/history/packs")).unwrap().count()})
    );
}

#[test]
fn completed_stop_receipt_identifies_the_actual_close_publication() {
    let f = Fixture::new();
    f.source.execute("UPDATE t SET v=37 WHERE id=1").unwrap();
    let done = Fixture::wait(
        &f.coordinator,
        f.coordinator
            .configure(&f.repository, Policy::Disabled, false, None)
            .unwrap()
            .unwrap(),
    );
    let repo = super::repository::Repository::open(&f.repository, None).unwrap();
    assert_eq!(done.catalog_generation, repo.position().unwrap().generation);
    assert_eq!(
        f.coordinator
            .control
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .position,
        repo.position().unwrap()
    );
}

#[test]
fn low_memory_controller_paths_cover_parent_child_and_restore_at_128_and_512_mib() {
    std::env::set_var("MYDB_BUFFER_POOL_PAGES", "1024");
    std::env::set_var("MYDB_MEMORY_TARGET", "32M");
    std::env::set_var("MYDB_BACKUP_COPY_THREADS", "1");
    std::env::set_var("MYDB_BACKUP_CHUNK_MEMORY_BYTES", "8388608");
    for bytes in [128usize << 20, 512usize << 20] {
        crate::engine::executor::mem_budget::with_test_effective_memory_limit(bytes, || {
            let f = Fixture::new();
            f.source.execute("UPDATE t SET v=43 WHERE id=1").unwrap();
            Fixture::wait(
                &f.coordinator,
                f.coordinator.backup_wal(&f.repository).unwrap(),
            );
            let repository = super::repository::Repository::open(&f.repository, None).unwrap();
            let (_, image) = repository.base(&f.base).unwrap();
            assert_eq!(
                image.manifest.capture_stats["seal_managed_memory_limit_bytes"],
                (bytes * 90 / 100) as u64
            );
            let target = f.root.path().join("low-memory");
            super::restore::restore(
                &f.repository,
                Some(&f.base),
                None,
                super::restore::Target::Latest,
                &target,
                None,
                false,
            )
            .unwrap();
            let reader = StorageQueryEngine::open(&target).unwrap();
            let QueryResponse::ResultSet(rows) =
                reader.execute("SELECT v FROM t WHERE id=1").unwrap()
            else {
                panic!("rows")
            };
            assert_eq!(rows.rows[0][0], Value::Int(43));
            reader.shutdown().unwrap();
        });
    }
}

#[test]
fn complete_bundle_digest_does_not_authorize_duplicate_object_paths() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let path = root.path().join("duplicate-bundle");
    let mut writer = super::bundle::Writer::new(std::fs::File::create(&path).unwrap()).unwrap();
    writer.write_all(super::bundle::MAGIC).unwrap();
    for _ in 0..2 {
        let name = b"repository.json";
        let data = b"{}";
        writer
            .write_all(&(name.len() as u32).to_le_bytes())
            .unwrap();
        writer.write_all(name).unwrap();
        writer
            .write_all(&(data.len() as u64).to_le_bytes())
            .unwrap();
        writer.write_payload(data, root.path()).unwrap();
        writer.write_all(&Sha256::digest(data)).unwrap();
        writer.objects += 1;
    }
    writer.finish().unwrap();
    drop(writer);
    let target = root.path().join("duplicate-import");
    let error = super::management::import_with_resume(&target, &path, None, true).unwrap_err();
    assert!(error_detail(&error).contains("duplicate bundle object"));
    assert!(!target.exists());
}

#[test]
fn bundle_transcript_binds_payload_digests_and_object_order() {
    let f = Fixture::new();
    let bundle = f.root.path().join("transcript.bundle");
    super::management::export(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Base,
        &bundle,
        None,
    )
    .unwrap();
    let original = fs::read(bundle).unwrap();
    let mut objects = Vec::new();
    let mut offset = super::bundle::MAGIC.len();
    loop {
        let start = offset;
        let name_len =
            u32::from_le_bytes(original[offset..offset + 4].try_into().unwrap()) as usize;
        if name_len == 0 {
            break;
        }
        offset += 4 + name_len;
        let len = u64::from_le_bytes(original[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        let payload = offset;
        offset += len + 32;
        objects.push((start, payload, len, offset));
    }
    let (_, payload, len, end) = objects.iter().copied().find(|object| object.2 > 0).unwrap();
    for retag in [false, true] {
        let mut bytes = original.clone();
        bytes[payload] ^= 1;
        if retag {
            let digest = Sha256::digest(&bytes[payload..payload + len]);
            bytes[end - 32..end].copy_from_slice(&digest);
        }
        let input = f.root.path().join(format!("changed-{retag}.bundle"));
        fs::write(&input, bytes).unwrap();
        let target = f.root.path().join(format!("changed-{retag}-import"));
        let error = super::management::import(&target, &input, None).unwrap_err();
        assert!(error_detail(&error).contains(if retag {
            "bundle final digest"
        } else {
            "bundle file checksum"
        }));
        assert!(!target.exists());
    }
    let mut reordered = original[..objects[0].0].to_vec();
    reordered.extend_from_slice(&original[objects[1].0..objects[1].3]);
    reordered.extend_from_slice(&original[objects[0].0..objects[0].3]);
    reordered.extend_from_slice(&original[objects[1].3..]);
    let input = f.root.path().join("reordered.bundle");
    fs::write(&input, reordered).unwrap();
    let target = f.root.path().join("reordered-import");
    let error = super::management::import(&target, &input, None).unwrap_err();
    assert!(error_detail(&error).contains("bundle final digest"));
    assert!(!target.exists());
}

#[test]
fn corrupted_cached_blocks_are_repaired_from_the_original_manifest_not_resealed() {
    let f = Fixture::new();
    let target = fs::canonicalize(f.root.path())
        .unwrap()
        .join("repair-cached-block");
    super::faults::fail("restore_base_cached", &target);
    assert!(super::restore::restore_with_resume(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Base,
        &target,
        None,
        false,
        true
    )
    .is_err());
    let inspected = DirectoryOperation::inspect(&target, "restore-work").unwrap();
    let owner = DirectoryOperation::resume(
        &target,
        "restore-work",
        inspected["input_digest"].as_str().unwrap().into(),
    )
    .unwrap();
    let repo = super::repository::Repository::open(&f.repository, None).unwrap();
    let (_, image) = repo.base_for_open(&f.base).unwrap();
    let heap = image
        .manifest
        .files
        .iter()
        .find(|f| f.path.ends_with(".heap") && f.len > 0)
        .unwrap();
    let cached = owner.data.join("base-cache").join(&heap.path);
    let mut bytes = fs::read(&cached).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 1;
    fs::write(cached, bytes).unwrap();
    drop(owner);
    super::restore::restore_with_resume(
        &f.repository,
        Some(&f.base),
        None,
        super::restore::Target::Base,
        &target,
        None,
        false,
        true,
    )
    .unwrap();
    native::verify(&target).unwrap();
}

#[test]
fn online_capture_enforces_spool_budget_and_keeps_rollback_and_management_available() {
    std::env::set_var("MYDB_BACKUP_SPOOL_LIMIT_BYTES", "4194304");
    std::env::set_var("MYDB_BACKUP_COPY_THREADS", "1");
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let source = StorageQueryEngine::open(root.path().join("source")).unwrap();
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,p LONGBLOB)")
        .unwrap();
    source
        .execute("INSERT INTO t VALUES(1,10,REPEAT('x',8388608))")
        .unwrap();
    let repository = fs::canonicalize(root.path()).unwrap().join("repository");
    let coordinator = Coordinator::get(source.db_handle()).unwrap();
    coordinator
        .configure(&repository, Policy::Manual, false, None)
        .unwrap();
    for _ in 0..2 {
        let id = coordinator.backup(&repository).unwrap().id;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let job = coordinator
                .jobs()
                .into_iter()
                .find(|job| job.id == id)
                .unwrap();
            if job.state != "RUNNING" {
                assert_eq!(job.state, "FAILED");
                assert!(job.error.contains("spool budget"), "{}", job.error);
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!source.db_handle().capture.active.load(Ordering::Acquire));
    }
    let mut session = crate::engine::SessionContext::default();
    source.execute_with_session(&mut session, "BEGIN").unwrap();
    source
        .execute_with_session(&mut session, "UPDATE t SET v=99 WHERE id=1")
        .unwrap();
    source
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    let QueryResponse::ResultSet(rows) = source.execute("SELECT v,LENGTH(p) FROM t").unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rows.rows[0], vec![Value::Int(10), Value::Int(8388608)]);
    coordinator
        .configure_patch(&repository, None, false, Some(512 << 20))
        .unwrap();
    assert_eq!(
        source
            .db_handle()
            .capture
            .budget
            .limit
            .load(Ordering::Acquire),
        512 << 20
    );
    assert!(fs::read_dir(repository.join("staging"))
        .unwrap()
        .next()
        .is_none());
    source.shutdown().unwrap();
}

#[test]
fn started_job_timestamp_is_durable_before_capture_can_be_interrupted() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let source = StorageQueryEngine::open(root.path().join("source")).unwrap();
    let data = source.db_handle().data_dir().to_path_buf();
    let inspect = data.clone();
    let checked = Arc::new(AtomicBool::new(false));
    let verified = checked.clone();
    super::faults::install("capture_files_pinned", &data, move || {
        let envelope: serde_json::Value =
            serde_json::from_slice(&fs::read(inspect.join("system/backup-control/jobs")).unwrap())
                .unwrap();
        let payload: Vec<u8> = serde_json::from_value(envelope["payload"].clone()).unwrap();
        let jobs: Vec<super::coordinator::Job> = serde_json::from_slice(&payload).unwrap();
        let job = jobs.iter().find(|job| job.state == "RUNNING").unwrap();
        assert!(job.started_unix_ms >= job.created_unix_ms && job.started_unix_ms > 0);
        verified.store(true, Ordering::Release);
        Ok(())
    });
    let coordinator = Coordinator::get(source.db_handle()).unwrap();
    let repository = fs::canonicalize(root.path()).unwrap().join("repository");
    Fixture::wait(&coordinator, coordinator.backup(&repository).unwrap());
    assert!(checked.load(Ordering::Acquire));
    source.shutdown().unwrap();
}

#[test]
fn restore_aggregate_budget_worker() {
    let Some(root) = std::env::var_os("MYDB_TEST_RESTORE_AGGREGATE_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let base = fs::read_to_string(root.join("base-id")).unwrap();
    let error = super::restore::restore(
        &root.join("repository"),
        Some(&base),
        None,
        super::restore::Target::Latest,
        &root.join("budget-image"),
        None,
        false,
    )
    .unwrap_err();
    assert!(
        error_detail(&error).contains("spool budget"),
        "{}",
        error_detail(&error)
    );
    assert!(!root.join("budget-image").exists());
}

#[test]
fn restore_admits_the_combined_image_plan_and_wal_not_each_in_isolation() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let source = StorageQueryEngine::open(root.path().join("source")).unwrap();
    source
        .execute("CREATE TABLE b(id INT PRIMARY KEY,p LONGBLOB)")
        .unwrap();
    source
        .execute(&format!(
            "INSERT INTO b VALUES {}",
            (0..128)
                .map(|id| format!("({id},REPEAT('x',16384))"))
                .collect::<Vec<_>>()
                .join(",")
        ))
        .unwrap();
    let repository = fs::canonicalize(root.path()).unwrap().join("repository");
    let coordinator = Coordinator::get(source.db_handle()).unwrap();
    coordinator
        .configure(&repository, Policy::Manual, false, None)
        .unwrap();
    let base = Fixture::wait(&coordinator, coordinator.backup(&repository).unwrap())
        .base_id
        .unwrap();
    source.execute("UPDATE b SET p=REPEAT('y',16384)").unwrap();
    let archived = Fixture::wait(&coordinator, coordinator.backup_wal(&repository).unwrap());
    let repo = super::repository::Repository::open(&repository, None).unwrap();
    let (_, image) = repo.base(&base).unwrap();
    let image_bytes = image
        .manifest
        .files
        .iter()
        .map(|file| file.len)
        .sum::<u64>();
    // Checkpoint timing can change the image/WAL split. Choose a limit
    // strictly between the larger input and their combined size so this
    // tests aggregate admission rather than one particular file layout.
    let smaller = image_bytes.min(archived.bytes);
    assert!(smaller >= 2);
    let limit = image_bytes.max(archived.bytes) + smaller / 2;
    assert!(image_bytes < limit && archived.bytes < limit && image_bytes + archived.bytes > limit);
    source.shutdown().unwrap();
    fs::write(root.path().join("base-id"), &base).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "backup::followup_tests::restore_aggregate_budget_worker",
            "--nocapture",
        ])
        .env("MYDB_TEST_RESTORE_AGGREGATE_ROOT", root.path())
        .env("MYDB_BACKUP_SPOOL_LIMIT_BYTES", limit.to_string())
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn resumable_import_cannot_rewrite_a_hard_link_to_an_external_file() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let root = fs::canonicalize(root.path()).unwrap();
    let input = root.join("input.bundle");
    let payload = b"owned bundle contents";
    let mut writer = super::bundle::Writer::new(std::fs::File::create(&input).unwrap()).unwrap();
    writer.write_all(super::bundle::MAGIC).unwrap();
    writer.write_all(&15u32.to_le_bytes()).unwrap();
    writer.write_all(b"repository.json").unwrap();
    writer
        .write_all(&(payload.len() as u64).to_le_bytes())
        .unwrap();
    writer.write_payload(payload, &root).unwrap();
    writer.write_all(&Sha256::digest(payload)).unwrap();
    writer.objects = 1;
    writer.finish().unwrap();
    drop(writer);
    let target = root.join("imported");
    super::faults::fail("import_block_written", &target);
    assert!(super::management::import_with_resume(&target, &input, None, true).is_err());
    let owner = DirectoryOperation::inspect(&target, "import-work").unwrap();
    let work = DirectoryOperation::resume(
        &target,
        "import-work",
        owner["input_digest"].as_str().unwrap().to_string(),
    )
    .unwrap();
    let output = work.data.join("repository/repository.json");
    drop(work);
    let external = root.join("external-user-file");
    fs::write(&external, b"external data must remain unchanged").unwrap();
    fs::remove_file(&output).unwrap();
    fs::hard_link(&external, &output).unwrap();
    assert!(super::management::import_with_resume(&target, &input, None, true).is_err());
    assert_eq!(
        fs::read(&external).unwrap(),
        b"external data must remain unchanged"
    );
}

#[test]
fn resumable_base_cache_cannot_repair_a_hard_link_to_an_external_file() {
    let f = Fixture::new();
    let base = f.repository.join("bases").join(&f.base).join("data");
    let cache = f.root.path().join("base-cache");
    native::cache_base(&base, &cache).unwrap();
    let image = native::inspect(&base).unwrap().unwrap();
    let file = image
        .manifest
        .files
        .iter()
        .find(|file| file.len > 0)
        .unwrap();
    let target = cache.join(&file.path);
    let external = f.root.path().join("external-user-file");
    fs::write(&external, b"external data must remain unchanged").unwrap();
    fs::remove_file(&target).unwrap();
    fs::hard_link(&external, &target).unwrap();
    let outcome = native::cache_base(&base, &cache);
    assert_eq!(
        sha256_hex(&fs::read(&external).unwrap()),
        sha256_hex(b"external data must remain unchanged")
    );
    assert!(outcome.is_err());
}

#[test]
fn private_metadata_write_cannot_truncate_a_hard_link_to_an_external_file() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let external = root.path().join("user-file");
    let owned = root.path().join("owned-file");
    fs::write(&external, b"external data must remain unchanged").unwrap();
    fs::hard_link(&external, &owned).unwrap();
    let result = write_sync(&owned, b"replacement");
    assert_eq!(
        fs::read(&external).unwrap(),
        b"external data must remain unchanged"
    );
    assert!(result.is_err());
}
