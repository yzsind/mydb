//! Native, prepared database snapshots and their immutable open contract.
use super::*;

pub use crate::storage::image::{
    inspect, is_snapshot, ImageKind, ObjectReplayContract, SealDescriptor, Snapshot, State, MARKER,
};
pub(crate) use crate::storage::image::{validate_inventory, validate_open};
use crate::storage::image::{ID_FILE, VERSION};
pub(crate) use crate::storage::image_io::read_only_error;

pub(crate) fn enforce_statement(statement: &Statement) -> Result<(), QueryError> {
    // Structural wrappers must retain the same restrictions as their body,
    // including file output and management operations classified as reads.
    match statement {
        Statement::WithCte(with) => enforce_statement(&with.body)?,
        Statement::Explain {
            target,
            analyze: true,
            ..
        } => enforce_statement(target)?,
        _ => {}
    }
    let effects = crate::statement_effects::statement_effects(statement);
    if effects.writes_data
        || effects.changes_catalog
        || effects.replica_forbidden
        || matches!(
            statement,
            Statement::SelectIntoFile { .. }
                | Statement::LockTables(_)
                | Statement::ReplicationAdmin { .. }
                | Statement::MydbReplicationAdmin { .. }
                | Statement::BinlogAdmin { .. }
                | Statement::FlushLogs { .. }
        )
    {
        return Err(read_only_error());
    }
    if let Statement::BackupAdmin { command } = statement {
        if !command.is_read_only() {
            return Err(read_only_error());
        }
    }
    // Global setters can start services, persist files, or change log paths.
    // Session query settings remain usable on immutable snapshots.
    if let Statement::Set(assignments) = statement {
        if assignments.iter().any(|assignment| {
            matches!(
                assignment.scope,
                Some(
                    crate::parser::SetScope::Global
                        | crate::parser::SetScope::Persist
                        | crate::parser::SetScope::PersistOnly
                )
            )
        }) {
            return Err(read_only_error());
        }
    }
    Ok(())
}

#[cfg(test)]
use crate::storage::image_io::{immutable_path, open_data_file, SnapshotRuntime};

fn write_snapshot(path: &Path, snapshot: &Snapshot) -> Result<(), QueryError> {
    let bytes = serde_json::to_vec_pretty(snapshot)
        .map_err(|err| backup_err(format!("encode native snapshot marker: {err}")))?;
    if bytes.len() > MAX_METADATA_BYTES {
        return Err(backup_err(
            "native snapshot metadata exceeds the size limit",
        ));
    }
    atomic_replace(&path.join(MARKER), &bytes)?;
    #[cfg(test)]
    super::faults::hit("snapshot_manifest_published", path)?;
    crate::storage::image_io::persist_mode(
        path,
        match snapshot.state {
            State::Preparing => "preparing",
            State::ReadOnly => "read_only",
            State::Writable => "writable",
        },
        match snapshot.kind {
            ImageKind::SealedBase => "SealedBase",
            ImageKind::RestoredImage => "RestoredImage",
        },
        &bytes,
    )
}

/// Verify the seal, complete file inventory and every data chunk checksum.
pub fn verify(path: &Path) -> Result<Snapshot, QueryError> {
    verify_image(path, false)
}

fn verify_image(path: &Path, allow_writable: bool) -> Result<Snapshot, QueryError> {
    let snapshot = inspect(path)?.ok_or_else(|| backup_err("not a native snapshot directory"))?;
    if snapshot.state == State::ReadOnly {
        validate_open(path)?;
    } else if allow_writable && snapshot.state == State::Writable {
        validate_inventory(path, &snapshot)?;
    } else {
        return Err(backup_err(
            "only a completed read-only snapshot can be verified",
        ));
    }
    verify_file_contents(path, &snapshot)?;
    Ok(snapshot)
}

fn verify_file_contents(path: &Path, snapshot: &Snapshot) -> Result<(), QueryError> {
    let sources = collect_snapshot_files(path, true)?;
    if sources.len() != snapshot.manifest.files.len() {
        return Err(backup_err("native snapshot file inventory mismatch"));
    }
    let _metadata = ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::SystemMetadata,
        estimate_snapshot_metadata(&sources),
    )?;
    let mut tasks = Vec::new();
    for (file_index, (source, expected)) in sources.iter().zip(&snapshot.manifest.files).enumerate()
    {
        if source.relative != expected.path || source.len != expected.len {
            return Err(backup_err(format!(
                "native snapshot file mismatch: {}",
                expected.path
            )));
        }
        let mut offset = 0u64;
        for (chunk_index, chunk) in expected.chunks.iter().enumerate() {
            if chunk.len == 0
                || chunk.len > CHUNK_BYTES as u64
                || chunk.len > source.len.saturating_sub(offset)
            {
                return Err(backup_err("invalid native snapshot chunk length"));
            }
            tasks.push(SourceChunkTask {
                file_index,
                chunk_index,
                offset,
                len: chunk.len as usize,
            });
            offset += chunk.len;
        }
        if offset != source.len {
            return Err(backup_err(
                "native snapshot file checksum coverage mismatch",
            ));
        }
    }
    process_chunks_parallel(
        &tasks,
        0,
        |task, buffer, _| {
            let source = &sources[task.file_index];
            let input = source
                .file
                .open()
                .map_err(|e| io_err("reopen verified image generation", e))?;
            read_exact_at(&input, buffer, task.offset)
                .map_err(|err| io_err("verify native file", err))?;
            if sha256_hex(buffer)
                != snapshot.manifest.files[task.file_index].chunks[task.chunk_index].hash
            {
                return Err(backup_err(format!(
                    "native snapshot checksum mismatch: {} at {}",
                    source.relative, task.offset
                )));
            }
            Ok(())
        },
        |_, ()| {},
    )?;
    Ok(())
}

/// Lock the existing inode without changing any byte in the backup.
#[cfg(any(unix, windows))]
pub(crate) fn exclusive_lock(path: &Path) -> Result<File, QueryError> {
    let file = File::open(path.join("system/mydb.lock"))
        .map_err(|err| io_err("open native snapshot lock", err))?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(backup_err(
                "native snapshot is in use; stop the instance before promotion",
            ));
        }
    }
    #[cfg(windows)]
    if !crate::platform::lock::try_exclusive(&file)
        .map_err(|err| io_err("lock native snapshot", err))?
    {
        return Err(backup_err(
            "native snapshot is in use; stop the instance before promotion",
        ));
    }
    Ok(file)
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn exclusive_lock(_path: &Path) -> Result<File, QueryError> {
    Err(backup_err(
        "native snapshot promotion requires Unix until Windows backup durability is validated",
    ))
}

pub fn promote(path: &Path) -> Result<(), QueryError> {
    if crate::storage::image_io::repository_ancestor(path).is_some() {
        return Err(backup_err("repository bases cannot be promoted"));
    }
    let _lock = exclusive_lock(path)?;
    let existing = inspect(path)?.ok_or_else(|| backup_err("not a native snapshot directory"))?;
    if existing.state == State::Writable {
        return sync_dir(path);
    }
    let snapshot = verify(path)?;
    if snapshot.kind != ImageKind::RestoredImage {
        return Err(backup_err(
            "a repository base cannot be promoted; restore to an independent directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        for file in &snapshot.manifest.files {
            if fs::metadata(path.join(&file.path))
                .map_err(|err| io_err("inspect native promotion link count", err))?
                .nlink()
                > 1
            {
                return Err(backup_err("cannot promote hard-linked snapshot files; use a full copy or filesystem reflink clone"));
            }
        }
    }
    #[cfg(windows)]
    for file in &snapshot.manifest.files {
        if crate::platform::fs::link_count(&path.join(&file.path))
            .map_err(|err| io_err("inspect native promotion link count", err))?
            > 1
        {
            return Err(backup_err("cannot promote hard-linked snapshot files; use a full copy or filesystem reflink clone"));
        }
    }
    // Snapshot creation excludes node/source identity. First writable server
    // start creates independent standalone identity using the normal path.
    let marker = read_bounded(&path.join(MARKER), MAX_METADATA_BYTES)?;
    #[cfg(test)]
    super::faults::hit("snapshot_manifest_published", path)?;
    crate::storage::image_io::persist_mode(path, "writable", "RestoredImage", &marker)?;
    sync_dir(path)
}

/// Confirm a complete installation whose final parent sync was uncertain.
/// Modified writable databases are rejected by the original content manifest.
pub fn confirm_install(path: &Path) -> Result<(), QueryError> {
    let path = resolved_destination(path)?;
    if crate::storage::image_io::repository_ancestor(&path).is_some() {
        return Err(backup_err(
            "only an independent restored image can be confirmed",
        ));
    }
    let _lock = exclusive_lock(&path)?;
    let image = verify_image(&path, true)?;
    DirectoryOperation::cleanup(&path, "restore-work")?;
    if image.kind != ImageKind::RestoredImage {
        return Err(backup_err("not an independent restored image"));
    }
    sync_tree(&path)?;
    sync_dir(
        path.parent()
            .ok_or_else(|| backup_err("restored image has no parent"))?,
    )
}

pub(super) struct Staging {
    pub path: PathBuf,
    destination: PathBuf,
    registration: PathBuf,
    published: bool,
}

/// Called while the source directory is exclusively owned. Only registered
/// staging directories belonging to this source are candidates for removal.
pub(crate) use crate::storage::image::cleanup_abandoned_staging;

impl Staging {
    pub(super) fn new(source: &Path, destination: &Path, id: &str) -> Result<Self, QueryError> {
        let name = destination
            .file_name()
            .ok_or_else(|| backup_err("invalid native destination"))?;
        if destination.exists() {
            return Err(backup_err(
                "native backup destination must not exist; use a new directory for every snapshot",
            ));
        }
        let parent = destination
            .parent()
            .ok_or_else(|| backup_err("native backup destination needs a parent"))?;
        let source =
            fs::canonicalize(source).map_err(|err| io_err("resolve native source", err))?;
        let mut existing = parent;
        while !existing.exists() {
            existing = existing
                .parent()
                .ok_or_else(|| backup_err("cannot resolve native backup parent"))?;
        }
        if fs::canonicalize(existing)
            .map_err(|err| io_err("resolve native backup ancestor", err))?
            .starts_with(&source)
        {
            return Err(backup_err(
                "native backup destination must be outside the source data directory",
            ));
        }
        create_directory_durable(parent)?;
        let parent =
            fs::canonicalize(parent).map_err(|err| io_err("resolve native backup parent", err))?;
        if parent.starts_with(&source) {
            return Err(backup_err(
                "native backup destination must be outside the source data directory",
            ));
        }
        cleanup_abandoned_staging(&source)?;
        let path = parent.join(format!(".mydb-native-{id}"));
        if path.exists() {
            return Err(backup_err("native staging directory already exists"));
        }
        let registry = crate::storage::naming::system_dir(&source).join("native-staging");
        fs::create_dir_all(&registry)
            .map_err(|err| io_err("create native staging registry", err))?;
        sync_dir(registry.parent().expect("native staging registry parent"))?;
        let registration = registry.join(id);
        atomic_replace(
            &registration,
            &serde_json::to_vec(&path).map_err(|err| backup_err(err.to_string()))?,
        )?;
        sync_dir(&registry)?;
        fs::create_dir(&path).map_err(|err| io_err("create native backup staging", err))?;
        let stage = Self {
            destination: parent.join(name),
            path,
            registration,
            published: false,
        };
        write_sync(&stage.path.join(ID_FILE), id.as_bytes())?;
        write_snapshot(
            &stage.path,
            &Snapshot {
                _memory: None,
                version: VERSION,
                state: State::Preparing,
                kind: ImageKind::SealedBase,
                seal: None,
                manifest: BackupManifest {
                    backup_format_version: BACKUP_FORMAT_VERSION,
                    backup_id: id.into(),
                    source_id: String::new(),
                    created_unix_ms: now_ms(),
                    completed_unix_ms: 0,
                    final_barrier_ms: 0,
                    capture_stats: BTreeMap::new(),
                    start_lsn: 0,
                    end_lsn: 0,
                    end_commit_unix_us: 0,
                    history_retention_ms: 0,
                    storage_compatibility:
                        crate::storage::format_versions::native_replication_compatibility(),
                    files: Vec::new(),
                },
            },
        )?;
        Ok(stage)
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_dir_all(&self.path);
        }
        if !self.path.exists() {
            let _ = fs::remove_file(&self.registration);
        }
    }
}

pub(super) fn finish(
    mut stage: Staging,
    manifest: BackupManifest,
    cancelled: &AtomicBool,
    mut progress: impl FnMut(&str),
) -> Result<BackupManifest, QueryError> {
    let target = super::resources::limits()?.foreground_commit_target_us;
    let pressured = target > 0
        && manifest
            .capture_stats
            .get("foreground_commit_ewma_us")
            .copied()
            .unwrap_or(0)
            > target;
    write_snapshot(
        &stage.path,
        &Snapshot {
            _memory: None,
            version: VERSION,
            state: State::Preparing,
            kind: ImageKind::SealedBase,
            seal: None,
            manifest,
        },
    )?;
    check_cancelled(cancelled)?;
    let exe = std::env::current_exe()
        .map_err(|err| io_err("locate native preparation executable", err))?;
    let grants = super::resources::seal_grants(if pressured { 1 } else { usize::MAX })?;
    let mut command = std::process::Command::new(exe);
    #[cfg(test)]
    crate::platform::fs::inherit_fixture_sync_batch(&mut command, &stage.path);
    // Forward resolved resource settings, including option-file overlays.
    // Source connection/role/SQL startup settings are deliberately excluded.
    for key in [
        "MYDB_PROCESS_MEM_LIMIT_MB",
        "MYDB_MEMORY_TARGET",
        "MYDB_BUFFER_POOL_PAGES",
        "MYDB_BUFFER_POOL_PCT",
        "MYDB_BUFFER_POOL_DECODED_CACHE",
        "MYDB_BUFFER_POOL_DECODED_CACHE_MAX_PCT",
        "MYDB_EXECUTOR_MEMORY_TARGET",
        "MYDB_EXECUTOR_MEMORY_PCT",
        "MYDB_QUERY_MEM_LIMIT_MB",
        "MYDB_UNDO_MEM_SOFT_LIMIT",
        "MYDB_TEMP_DISK_LIMIT",
        "MYDB_TEMP_DISK_RESERVE_PCT",
        "MYDB_BACKUP_WORKERS",
        "MYDB_BACKUP_QUEUE_CAPACITY",
        "MYDB_BACKUP_OPEN_FILES",
        "MYDB_BACKUP_CHUNK_MEMORY_BYTES",
        "MYDB_BACKUP_SPOOL_LIMIT_BYTES",
        "MYDB_BACKUP_BANDWIDTH_BYTES_PER_SEC",
        "MYDB_BACKUP_FREEZE_BUDGET_MS",
        "MYDB_BACKUP_FOREGROUND_COMMIT_TARGET_US",
    ] {
        if let Some(value) = crate::startup_config::var_os(key) {
            command.env(key, value);
        }
    }
    command.env("MYDB_BACKUP_COPY_THREADS", grants.len().to_string());
    #[cfg(not(test))]
    command
        .arg("--seal-worker")
        .arg(&stage.path)
        .arg("--operation")
        .arg(stage.registration.file_name().unwrap());
    #[cfg(test)]
    command
        .args([
            "--exact",
            "backup::native::tests::prepare_worker_entry",
            "--nocapture",
        ])
        .env("MYDB_NATIVE_TEST_PREPARE", &stage.path)
        .env(
            "MYDB_NATIVE_TEST_MEMORY_LIMIT",
            crate::engine::executor::mem_budget::effective_memory_limit_bytes().to_string(),
        );
    // Inherited service configuration must not turn an isolated preparation
    // into a replica/archiver or redirect it to the source instance.
    command.env_remove("MYDB_BACKUP_REPOSITORY");
    command.env_remove("MYDB_WAL_FORCE_RECOVER");
    command.env_remove("MYDB_UNDO_FORCE_RECOVER");
    let diagnostics = ScratchDirectory::new(
        stage
            .path
            .parent()
            .ok_or_else(|| backup_err("seal staging has no parent"))?,
        "seal-logs",
    )?;
    let log_path = diagnostics.0.join("prepare.log");
    let log = File::create(&log_path).map_err(|err| io_err("create preparation log", err))?;
    command
        .stdout(
            log.try_clone()
                .map_err(|err| io_err("clone preparation log", err))?,
        )
        .stderr(log);
    let mut child = command
        .spawn()
        .map_err(|err| io_err("start native preparation", err))?;
    let status = loop {
        if cancelled.load(Ordering::Acquire) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(backup_err("native backup cancelled during preparation"));
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => std::thread::sleep(Duration::from_millis(5)),
            Err(err) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(io_err("wait for native preparation", err));
            }
        }
    };
    // The child has exited; release its grants before parent-side hashing
    // or syncing reacquires the same process-wide bulk slots.
    drop(grants);
    if !status.success() {
        let detail = fs::read_to_string(&log_path).unwrap_or_default();
        return Err(backup_err(format!(
            "native preparation failed ({status}): {}",
            detail.chars().take(8192).collect::<String>()
        )));
    }
    fs::remove_file(log_path).map_err(|err| io_err("remove preparation log", err))?;
    let snapshot =
        inspect(&stage.path)?.ok_or_else(|| backup_err("native preparation produced no marker"))?;
    if snapshot.state != State::ReadOnly {
        return Err(backup_err(
            "native preparation did not publish a queryable snapshot",
        ));
    }
    progress("PUBLISH_NATIVE");
    check_cancelled(cancelled)?;
    // A successful worker has synced the complete data tree, then atomically
    // published and synced the read-only marker. The parent has only read it;
    // repeating every file barrier here adds no durability. Publication still
    // requires the no-replace rename and the destination-parent barrier.
    rename_new_directory(&stage.path, &stage.destination)?;
    stage.published = true;
    sync_dir(stage.destination.parent().unwrap())?;
    Ok(snapshot.manifest)
}

pub(super) fn rename_new_directory(source: &Path, destination: &Path) -> Result<(), QueryError> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let source = std::ffi::CString::new(source.as_os_str().as_bytes())
            .map_err(|_| backup_err("invalid source path"))?;
        let destination = std::ffi::CString::new(destination.as_os_str().as_bytes())
            .map_err(|_| backup_err("invalid destination path"))?;
        #[cfg(target_os = "macos")]
        let result =
            unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
        #[cfg(target_os = "linux")]
        // Invoke the kernel directly: glibc only added its renameat2 wrapper
        // in 2.28. Preserve RENAME_NOREPLACE and propagate unsupported-kernel
        // errors rather than falling back to an overwriting rename.
        let result = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        return Err(backup_err(
            "atomic native backup publication requires Linux or macOS",
        ));
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        if result == 0 {
            return Ok(());
        }
        return Err(io_err(
            "publish native backup without overwriting destination",
            std::io::Error::last_os_error(),
        ));
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Storage::FileSystem::MoveFileExW;
        let source = crate::platform::fs::wide_path(source)
            .map_err(|err| io_err("native source path", err))?;
        let destination = crate::platform::fs::wide_path(destination)
            .map_err(|err| io_err("native destination path", err))?;
        // No REPLACE_EXISTING and no COPY_ALLOWED: publication remains on
        // one volume and atomically refuses an existing destination.
        if unsafe { MoveFileExW(source.as_ptr(), destination.as_ptr(), 0) } != 0 {
            return Ok(());
        }
        Err(io_err(
            "publish native backup without overwriting destination",
            std::io::Error::last_os_error(),
        ))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (source, destination);
        Err(backup_err("native backup publication requires Unix"))
    }
}

/// Worker entry: must run in an isolated process, never inside the source server.
pub fn prepare_directory_checked(path: &Path, operation: &str) -> Result<(), QueryError> {
    validate_id(operation)?;
    let image = inspect(path)?.ok_or_else(|| backup_err("missing seal operation marker"))?;
    if image.manifest.backup_id != operation || image.state != State::Preparing {
        return Err(backup_err("seal worker operation/mode mismatch"));
    }
    prepare_directory(path)
}

pub fn prepare_directory(path: &Path) -> Result<(), QueryError> {
    let _repository_lease = crate::storage::image_io::repository_ancestor(path)
        .map(|root| super::repository::Lease::acquire(&root, false))
        .transpose()?;
    let mut snapshot =
        inspect(path)?.ok_or_else(|| backup_err("missing native preparation marker"))?;
    if snapshot.state != State::Preparing || snapshot.manifest.files.is_empty() {
        return Err(backup_err(
            "directory is not a captured native backup awaiting preparation",
        ));
    }
    let _directory_lock = crate::storage::dir_lock::DataDirLock::acquire(path)?;
    validate_inventory(path, &snapshot)?;
    crate::wal::persist_source_clock(path, snapshot.manifest.end_commit_unix_us)?;
    let original =
        crate::wal::LogManager::scan_replay_with_mode(path, crate::wal::WalReadMode::Strict)?;
    let visibility = crate::storage::snapshot::SourceVisibility::from_wal(&original.records)?;
    let dependency_floor = visibility.dependency_floor();
    let work = path.join(".seal-wal");
    fs::create_dir(&work).map_err(|e| io_err("create sealed dependency workspace", e))?;
    let mut dependency_log = crate::wal::LogManager::open(&work)?;
    dependency_log.set_local_recovery(true);
    let source_end = snapshot.manifest.end_lsn;
    let mut first_dependency = None;
    if let Some(floor) = dependency_floor {
        for file in snapshot
            .manifest
            .files
            .iter()
            .filter(|f| f.class == FileClass::RecoveryWal)
        {
            crate::wal::visit_raw_prefix(
                &File::open(path.join(&file.path))
                    .map_err(|e| io_err("read source dependencies", e))?,
                file.len,
                |start, end, bytes| {
                    if end >= floor {
                        if first_dependency.is_none() {
                            let checkpoint_lsn =
                                start.checked_sub(1).filter(|n| *n > 0).ok_or_else(|| {
                                    backup_err("no position for source visibility checkpoint")
                                })?;
                            dependency_log.append_reserved(
                                checkpoint_lsn,
                                0,
                                visibility.checkpoint(source_end),
                            )?;
                            first_dependency = Some(start);
                        }
                        let frame = crate::wal::decode_frame_for_replication(bytes)?;
                        dependency_log.append_replication_frame(bytes, &frame)?;
                    }
                    Ok(())
                },
            )?;
        }
        if first_dependency.is_none() {
            return Err(backup_err("missing prepared recovery predecessor"));
        }
    } else {
        dependency_log.append_reserved(source_end, 0, visibility.checkpoint(source_end))?;
    }
    dependency_log.flush()?;
    drop(dependency_log);
    drop(original);
    {
        let context = crate::storage::recovery_context::RecoveryContext::seal(
            snapshot.manifest.start_lsn.saturating_add(1),
            source_end,
        )?;
        let db = Database::open_for_backup_preparation(
            path,
            context.at_source_time(snapshot.manifest.end_commit_unix_us),
        )?;
        db.prepare_snapshot_indexes()?;
        db.checkpoint_for_shutdown()?;
    }
    let image_checkpoint =
        crate::wal::LogManager::scan_replay_with_mode(path, crate::wal::WalReadMode::Strict)?
            .next_lsn
            .saturating_sub(1);
    // Only source-domain transaction dependencies survive. Local index/checkpoint
    // WAL is disposable after the private image has reached its sync barrier.
    sync_tree(path)?;
    fs::remove_dir_all(crate::storage::naming::system_wal_dir(path))
        .map_err(|e| io_err("retire seal WAL", e))?;
    fs::rename(
        crate::storage::naming::system_wal_dir(&work),
        crate::storage::naming::system_wal_dir(path),
    )
    .map_err(|e| io_err("install sealed dependencies", e))?;
    fs::remove_dir_all(&work).map_err(|e| io_err("remove seal workspace", e))?;
    let sealed =
        crate::wal::LogManager::scan_replay_with_mode(path, crate::wal::WalReadMode::Strict)?;
    if crate::storage::snapshot::SourceVisibility::from_wal(&sealed.records)? != visibility {
        return Err(backup_err(
            "sealed transaction state differs from the source boundary",
        ));
    }
    snapshot.seal = Some(SealDescriptor {
        version: 1,
        source_end_lsn: source_end,
        source_end_time: snapshot.manifest.end_commit_unix_us,
        source_clock_floor: snapshot.manifest.end_commit_unix_us,
        next_source_lsn: source_end
            .checked_add(1)
            .ok_or_else(|| backup_err("source position overflow"))?,
        continuation_floor: first_dependency,
        retained_recovery_ranges: first_dependency
            .map(|start| vec![(start, source_end)])
            .unwrap_or_default(),
        transaction_state_digest: sha256_hex(
            &serde_json::to_vec(&visibility).map_err(|e| backup_err(e.to_string()))?,
        ),
        visibility_state: visibility,
        object_replay_contracts: Vec::new(),
        image_checkpoint,
        query_ready: true,
        continuation_ready: true,
    });
    let _ = fs::remove_dir_all(path.join("system/backup"));
    let _ = fs::remove_dir_all(path.join("system/replication"));
    let _ = fs::remove_dir_all(path.join("system/tmp"));
    let files = hash_image_files(path)?;
    snapshot.seal.as_mut().unwrap().object_replay_contracts = files
        .iter()
        .filter(|f| f.class != FileClass::RecoveryWal)
        .map(|f| ObjectReplayContract {
            path: f.path.clone(),
            mode: if f.class == FileClass::Index || f.class == FileClass::Derived {
                "REBUILD_FROM_AUTHORITY"
            } else {
                "SOURCE_PHYSICAL"
            }
            .into(),
            version: 1,
        })
        .collect();
    snapshot.manifest.files = files;
    let limits = super::resources::limits()?;
    snapshot
        .manifest
        .capture_stats
        .insert("seal_copy_thread_limit".into(), limits.copy_threads as u64);
    snapshot.manifest.capture_stats.insert(
        "seal_chunk_memory_bytes".into(),
        limits.chunk_memory_bytes as u64,
    );
    snapshot.manifest.capture_stats.insert(
        "seal_memory_target_bytes".into(),
        crate::engine::executor::mem_budget::memory_target_bytes() as u64,
    );
    snapshot.manifest.capture_stats.insert(
        "seal_process_memory_limit_bytes".into(),
        crate::engine::executor::mem_budget::process_mem_limit_bytes() as u64,
    );
    snapshot.manifest.capture_stats.insert(
        "seal_managed_memory_limit_bytes".into(),
        crate::engine::executor::mem_budget::managed_memory_limit_bytes() as u64,
    );
    snapshot.manifest.completed_unix_ms = now_ms();
    // Data first, completion marker last. Staging remains private until the parent publishes it.
    sync_tree(path)?;
    snapshot.state = State::ReadOnly;
    write_snapshot(path, &snapshot)?;
    sync_dir(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::StorageQueryEngine;

    #[test]
    fn prepare_worker_entry() {
        if let Some(path) = std::env::var_os("MYDB_NATIVE_TEST_PREPARE") {
            if let Ok(limit) = std::env::var("MYDB_NATIVE_TEST_MEMORY_LIMIT") {
                let limit = limit.parse().unwrap();
                crate::engine::executor::mem_budget::with_test_effective_memory_limit(
                    limit,
                    || {
                        super::prepare_directory(std::path::Path::new(&path)).unwrap();
                    },
                );
            } else {
                super::prepare_directory(std::path::Path::new(&path)).unwrap();
            }
        }
    }

    #[test]
    fn parallel_image_checksums_cover_empty_files_and_corrupt_tail_chunks() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = StorageQueryEngine::open(root.path().join("source")).unwrap();
        let source = engine.db_handle().data_dir();
        let bytes = (0..CHUNK_BYTES * 2 + 37)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>();
        fs::create_dir_all(source.join("objects/checksum")).unwrap();
        fs::write(source.join("objects/checksum/large.bin"), &bytes).unwrap();
        fs::write(source.join("objects/checksum/empty.bin"), []).unwrap();
        let destination = root.path().join("snapshot");
        create_backup(
            engine.db_handle(),
            &BackupRequest {
                repository: destination.clone(),
            },
            &random_uuid(),
            &AtomicBool::new(false),
            |_, _, _| {},
        )
        .unwrap();
        let mut snapshot = verify(&destination).unwrap();
        let file = snapshot
            .manifest
            .files
            .iter_mut()
            .find(|f| f.path.ends_with("large.bin"))
            .unwrap();
        assert_eq!(file.chunks.len(), 3);
        for (actual, expected) in file.chunks.iter().zip(bytes.chunks(CHUNK_BYTES)) {
            assert_eq!(actual.hash, sha256_hex(expected));
            assert_eq!(actual.len, expected.len() as u64);
        }
        // Verification must honor the manifest's boundaries, including legal
        // short chunks in the middle of a file, rather than assuming 4 MiB.
        file.chunks = bytes
            .chunks(CHUNK_BYTES / 2 + 7)
            .map(|chunk| ChunkRef {
                hash: sha256_hex(chunk),
                len: chunk.len() as u64,
            })
            .collect();
        write_snapshot(&destination, &snapshot).unwrap();
        verify(&destination).unwrap();
        let file = OpenOptions::new()
            .write(true)
            .open(destination.join("objects/checksum/large.bin"))
            .unwrap();
        write_all_at(&file, &[255], bytes.len() as u64 - 1).unwrap();
        let error = verify(&destination).unwrap_err();
        assert!(error.message().contains("checksum mismatch"), "{error:?}");
        engine.shutdown().unwrap();
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn native_publication_preserves_existing_empty_directory() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let source = root.path().join("staged");
        let destination = root.path().join("published");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("payload"), b"complete snapshot").unwrap();
        fs::create_dir(&destination).unwrap();

        // An ordinary rename could replace this empty destination directory.
        assert!(rename_new_directory(&source, &destination).is_err());
        assert_eq!(
            fs::read(source.join("payload")).unwrap(),
            b"complete snapshot"
        );
        assert_eq!(fs::read_dir(&destination).unwrap().count(), 0);

        fs::remove_dir(&destination).unwrap();
        rename_new_directory(&source, &destination).unwrap();
        assert!(!source.exists());
        assert_eq!(
            fs::read(destination.join("payload")).unwrap(),
            b"complete snapshot"
        );
    }

    #[cfg(unix)]
    #[test]
    fn native_runtime_and_alias_paths_cannot_write_into_snapshot() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let snapshot = root.path().join("snapshot");
        fs::create_dir(&snapshot).unwrap();
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(&snapshot, &alias).unwrap();
        assert!(SnapshotRuntime::open_with_temp_dir(&snapshot, &alias.join("scratch")).is_err());
        assert!(!snapshot.join("scratch").exists());
        let external_temp = root.path().join("external-temp");
        fs::create_dir(&external_temp).unwrap();
        std::os::unix::fs::symlink(&snapshot, external_temp.join("mydb")).unwrap();
        assert!(SnapshotRuntime::open_with_temp_dir(&snapshot, &external_temp).is_err());
        fs::write(snapshot.join(ID_FILE), b"immutable-test").unwrap();
        let guard = SnapshotRuntime::open(&snapshot).unwrap();
        assert!(immutable_path(&alias.join("new.log")));
        assert!(immutable_path(&snapshot.join("new.log")));
        assert!(!immutable_path(&root.path().join("outside.log")));
        assert!(open_data_file(&alias.join("new.log")).is_err());
        assert!(!snapshot.join("new.log").exists());
        drop(guard);
    }

    #[test]
    fn native_staging_is_fail_closed_and_publication_never_overwrites() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let source = root.path().join("source");
        fs::create_dir(&source).unwrap();
        let target = root.path().join("target");
        assert!(Staging::new(&source, &root.path().join("absent/.."), &random_uuid()).is_err());
        assert!(!root.path().join("absent").exists());
        let stage = Staging::new(&source, &target, &random_uuid()).unwrap();
        assert!(is_snapshot(&stage.path).is_err());
        assert!(Database::open(&stage.path).is_err());
        fs::create_dir(&target).unwrap();
        fs::write(target.join("keep"), "user data").unwrap();
        assert!(rename_new_directory(&stage.path, &target).is_err());
        assert_eq!(
            fs::read_to_string(target.join("keep")).unwrap(),
            "user data"
        );
        let abandoned = stage.path.clone();
        drop(stage);
        assert!(!abandoned.exists());
        assert!(Staging::new(&source, &target, &random_uuid()).is_err());
    }

    #[test]
    fn fresh_audit_abandoned_staging_is_registered_and_reclaimed() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let source = root.path().join("source");
        fs::create_dir(&source).unwrap();
        let stage = Staging::new(&source, &root.path().join("snapshot"), &random_uuid()).unwrap();
        let path = stage.path.clone();
        let registration = stage.registration.clone();
        assert!(registration.exists());
        std::mem::forget(stage);
        let unrelated = root.path().join(".mydb-native-user-files");
        fs::create_dir(&unrelated).unwrap();
        fs::write(unrelated.join("keep"), b"keep").unwrap();
        cleanup_abandoned_staging(&source).unwrap();
        assert!(!path.exists());
        assert!(!registration.exists());
        assert_eq!(fs::read(unrelated.join("keep")).unwrap(), b"keep");
        cleanup_abandoned_staging(&source).unwrap();
    }

    #[test]
    fn fresh_audit_staging_identity_initialization_can_be_interrupted() {
        for partial_identity in [false, true] {
            let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
            let source = root.path().join("source");
            fs::create_dir(&source).unwrap();
            let id = random_uuid();
            let stage = Staging::new(&source, &root.path().join("snapshot"), &id).unwrap();
            let path = stage.path.clone();
            std::mem::forget(stage);
            fs::remove_dir_all(&path).unwrap();
            fs::create_dir(&path).unwrap();
            if partial_identity {
                fs::write(path.join(ID_FILE), &id.as_bytes()[..8]).unwrap();
            }
            cleanup_abandoned_staging(&source).unwrap();
            assert!(!path.exists());
        }
    }

    #[test]
    fn native_cancelled_backup_does_not_publish() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let source = StorageQueryEngine::open(root.path().join("source")).unwrap();
        let target = root.path().join("cancelled");
        assert!(create_backup(
            source.db_handle(),
            &BackupRequest {
                repository: target.clone(),
            },
            &random_uuid(),
            &AtomicBool::new(true),
            |_, _, _| {}
        )
        .is_err());
        assert!(!target.exists());
        assert!(!fs::read_dir(root.path()).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".mydb-native-")));
        source.shutdown().unwrap();
    }

    fn tree_hashes(path: &Path) -> BTreeMap<PathBuf, String> {
        fn walk(root: &Path, path: &Path, result: &mut BTreeMap<PathBuf, String>) {
            for entry in fs::read_dir(path).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_dir() {
                    walk(root, &path, result);
                } else {
                    result.insert(
                        path.strip_prefix(root).unwrap().to_path_buf(),
                        sha256_hex(&fs::read(path).unwrap()),
                    );
                }
            }
        }
        let mut result = BTreeMap::new();
        walk(path, path, &mut result);
        result
    }

    #[test]
    fn native_snapshot_round_trip_is_immutable_and_promotes() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let source = StorageQueryEngine::open(root.path().join("source")).unwrap();
        source.execute("CREATE SEQUENCE snapshot_seq").unwrap();
        source
            .execute(
                "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(64), payload LONGBLOB, INDEX iv(v))",
            )
            .unwrap();
        source
            .execute("INSERT INTO t VALUES (1, 'one', REPEAT('a', 20000)), (2, 'two', NULL)")
            .unwrap();
        let mut xa = crate::engine::api::SessionContext::default();
        for sql in [
            "XA START 'native-xa'",
            "INSERT INTO t VALUES(99,'pending',NULL)",
            "XA END 'native-xa'",
            "XA PREPARE 'native-xa'",
        ] {
            source.execute_with_session(&mut xa, sql).unwrap();
        }
        let xa_target = root.path().join("xa-snapshot");
        let xa_manifest = create_backup(
            source.db_handle(),
            &BackupRequest {
                repository: xa_target.clone(),
            },
            &random_uuid(),
            &AtomicBool::new(false),
            |_, _, _| {},
        )
        .unwrap();
        assert!(
            verify(&xa_target)
                .unwrap()
                .seal
                .unwrap()
                .visibility_state
                .prepared
                .len()
                == 1
        );
        let xa_query = StorageQueryEngine::open(&xa_target).unwrap();
        let QueryResponse::ResultSet(pending) =
            xa_query.execute("SELECT id FROM t WHERE id=99").unwrap()
        else {
            panic!("rows")
        };
        assert!(pending.rows.is_empty());
        xa_query.shutdown().unwrap();
        drop(xa_query);
        assert!(xa_manifest.end_lsn > 0);
        source
            .execute_with_session(&mut xa, "XA ROLLBACK 'native-xa'")
            .unwrap();
        let dest = root.path().join("snapshot");
        assert!(source
            .execute(&format!(
                "BACKUP DATABASE TO 'file://{}' TYPE INCREMENTAL",
                dest.display()
            ))
            .is_err());
        assert!(source
            .execute(&format!(
                "BACKUP DATABASE TO 'file://{}' TYPE FULL ENCRYPTION_KEY_ID='key'",
                dest.display()
            ))
            .is_err());
        assert!(Staging::new(
            source.db_handle().data_dir(),
            &source.db_handle().data_dir().join("nested/backup"),
            &random_uuid()
        )
        .is_err());
        assert!(!source.db_handle().data_dir().join("nested").exists());
        let manifest = create_backup(
            source.db_handle(),
            &BackupRequest {
                repository: dest.clone(),
            },
            &random_uuid(),
            &AtomicBool::new(false),
            |_, _, _| {},
        )
        .unwrap();
        assert!(!dest.join("chunks").exists());
        verify(&dest).unwrap();
        source.execute("UPDATE t SET v='later' WHERE id=1").unwrap();
        let before = tree_hashes(&dest);
        #[cfg(unix)]
        fn permissions(path: &Path, writable: bool) {
            use std::os::unix::fs::PermissionsExt;
            for entry in fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    permissions(&path, writable);
                } else {
                    fs::set_permissions(
                        path,
                        fs::Permissions::from_mode(if writable { 0o600 } else { 0o400 }),
                    )
                    .unwrap();
                }
            }
            fs::set_permissions(
                path,
                fs::Permissions::from_mode(if writable { 0o700 } else { 0o500 }),
            )
            .unwrap();
        }
        #[cfg(unix)]
        permissions(&dest, false);
        let snapshot = StorageQueryEngine::open(&dest).unwrap();
        assert!(snapshot.db_handle().is_snapshot_read_only());
        let QueryResponse::ResultSet(result) = snapshot
            .execute("SELECT id,v,LENGTH(payload) FROM t FORCE INDEX(iv) WHERE v='one'")
            .unwrap()
        else {
            panic!("expected rows");
        };
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Value::String("one".into()));
        snapshot.execute("START TRANSACTION").unwrap();
        snapshot.execute("SELECT COUNT(*) FROM t").unwrap();
        snapshot.execute("COMMIT").unwrap();
        for sql in [
            "INSERT INTO t VALUES (3,'three',NULL)",
            "UPDATE t SET v='bad'",
            "DELETE FROM t",
            "CREATE TABLE bad(id INT)",
            "ALTER SYSTEM CHECKPOINT",
            "SET PERSIST read_only=OFF",
            "SELECT * FROM t FOR UPDATE",
            "SET GLOBAL read_only=OFF",
            "SET GLOBAL super_read_only=OFF",
            "SELECT nextval('snapshot_seq')",
            "SELECT setval('snapshot_seq',100)",
        ] {
            assert!(snapshot.execute(sql).is_err(), "snapshot accepted {sql}");
        }
        assert!(
            promote(&dest).is_err(),
            "promotion must refuse an open snapshot"
        );
        snapshot.shutdown().unwrap();
        drop(snapshot);
        assert_eq!(
            tree_hashes(&dest),
            before,
            "read-only open/query/close changed snapshot bytes"
        );
        #[cfg(unix)]
        permissions(&dest, true);
        // Corruption is diagnosed without entering repair or modifying data.
        let checkpoint = manifest
            .files
            .iter()
            .find(|file| file.class == FileClass::RecoveryWal)
            .unwrap();
        let checkpoint_path = dest.join(&checkpoint.path);
        let original = fs::read(&checkpoint_path).unwrap();
        let mut corrupt = original.clone();
        *corrupt.last_mut().unwrap() ^= 0x20;
        fs::write(&checkpoint_path, corrupt).unwrap();
        assert!(StorageQueryEngine::open(&dest).is_err());
        assert!(verify(&dest).is_err());
        assert!(promote(&dest).is_err());
        fs::write(&checkpoint_path, original).unwrap();
        // Losing the completion marker must not turn a backup writable.
        let marker = fs::read(dest.join(MARKER)).unwrap();
        fs::remove_file(dest.join(MARKER)).unwrap();
        assert!(StorageQueryEngine::open(&dest).is_err());
        fs::write(dest.join(MARKER), &marker).unwrap();
        let mut bad_marker: Snapshot = serde_json::from_slice(&marker).unwrap();
        bad_marker.version += 1;
        write_snapshot(&dest, &bad_marker).unwrap();
        assert!(StorageQueryEngine::open(&dest).is_err());
        bad_marker.version -= 1;
        bad_marker.state = State::Preparing;
        write_snapshot(&dest, &bad_marker).unwrap();
        assert!(StorageQueryEngine::open(&dest).is_err());
        assert!(promote(&dest).is_err());
        write_snapshot(&dest, &serde_json::from_slice(&marker).unwrap()).unwrap();
        assert!(promote(&dest).is_err(), "sealed base is never promoted");
        let independent = root.path().join("independent");
        restore_image(
            &dest,
            &independent,
            manifest.end_lsn,
            manifest.end_commit_unix_us,
            false,
            |_| Ok(()),
        )
        .unwrap();
        let dest = independent;
        #[cfg(unix)]
        {
            let alias = root.path().join("shared-file");
            fs::hard_link(dest.join(&manifest.files[0].path), &alias).unwrap();
            assert!(
                promote(&dest).is_err(),
                "hard-linked backups must not become writable"
            );
            fs::remove_file(alias).unwrap();
        }
        promote(&dest).unwrap();
        promote(&dest).unwrap();
        let writable = StorageQueryEngine::open(&dest).unwrap();
        assert!(!writable.db_handle().is_snapshot_read_only());
        writable
            .execute("INSERT INTO t VALUES (3,'three',NULL)")
            .unwrap();
        writable.shutdown().unwrap();
        drop(writable);
        let reopened = StorageQueryEngine::open(&dest).unwrap();
        let QueryResponse::ResultSet(rows) =
            reopened.execute("SELECT id FROM t WHERE id=3").unwrap()
        else {
            panic!("expected rows");
        };
        assert_eq!(rows.rows.len(), 1);
        reopened.shutdown().unwrap();
        source.shutdown().unwrap();
    }
}

fn copy_source_files(source: &Path, destination: &Path) -> Result<Snapshot, QueryError> {
    validate_open(source)?;
    let image = inspect(source)?.ok_or_else(|| backup_err("missing source image"))?;
    if destination.exists()
        && fs::read_dir(destination)
            .map_err(|e| io_err("inspect image destination", e))?
            .next()
            .is_some()
    {
        return Err(backup_err("image destination is not empty"));
    }
    fs::create_dir_all(destination).map_err(|e| io_err("create image copy", e))?;
    // An incomplete private copy must never be mistaken for a writable database.
    fs::copy(source.join(ID_FILE), destination.join(ID_FILE))
        .map_err(|e| io_err("copy image identity", e))?;
    let total = image.manifest.files.iter().try_fold(0u64, |n, f| {
        n.checked_add(f.len)
            .ok_or_else(|| backup_err("image copy size overflow"))
    })?;
    super::resources::check_spool(total)?;
    for file in &image.manifest.files {
        let target = destination.join(&file.path);
        fs::create_dir_all(target.parent().unwrap()).map_err(|e| io_err("create image path", e))?;
        let _disk = reserve_disk_write(destination, file.len)?;
        super::resources::copy_file(&source.join(&file.path), &target, file.len)?;
    }
    fs::create_dir_all(destination.join("system"))
        .map_err(|e| io_err("create image system directory", e))?;
    // The final tree barrier persists this empty lock; no completion marker
    // is published for the intermediate source-shaped copy.
    fs::File::create(destination.join("system/mydb.lock"))
        .map_err(|e| io_err("create image lock", e))?;
    Ok(image)
}

pub(crate) fn restore_image(
    base: &Path,
    destination: &Path,
    target: u64,
    source_time: u64,
    writable: bool,
    append: impl FnOnce(&mut crate::wal::LogManager) -> Result<(), QueryError>,
) -> Result<Snapshot, QueryError> {
    let mut image = copy_source_files(base, destination)?;
    #[cfg(test)]
    super::faults::hit("restore_copy_complete", destination)?;
    // Compare copied bytes with the original manifest before any recovery
    // rewrites are allowed to replace their authoritative checksums.
    validate_inventory(destination, &image)?;
    if read_bounded(&destination.join(ID_FILE), 128)? != image.manifest.backup_id.as_bytes() {
        return Err(backup_err("copied image identity mismatch"));
    }
    verify_file_contents(destination, &image)?;
    if target < image.manifest.end_lsn {
        return Err(backup_err("target precedes sealed base"));
    }
    let source_end = image.manifest.end_lsn;
    image.state = State::Preparing;
    image.kind = ImageKind::RestoredImage;
    write_snapshot(destination, &image)?;
    if target > source_end {
        // Zone summaries have a local seal layout and are rebuilt from the
        // recovered heap. Source physical pages must never share that layout.
        for file in &image.manifest.files {
            if file.path.ends_with(".zm") {
                fs::remove_file(destination.join(&file.path))
                    .map_err(|e| io_err("retire derived zone map", e))?;
            }
        }
        let zone_manifest = destination.join("system/zone-map-checkpoint.manifest");
        if zone_manifest.exists() {
            fs::remove_file(zone_manifest).map_err(|e| io_err("retire zone map coverage", e))?;
        }
        {
            let mut log = crate::wal::LogManager::open(destination)?;
            append(&mut log)?;
            if log.last_lsn() != target {
                return Err(backup_err("restored source prefix does not reach target"));
            }
            log.flush()?;
        }
        let context =
            crate::storage::recovery_context::RecoveryContext::continuation(source_end, target)?
                .at_source_time(source_time);
        let mut config = StorageConfig::from_env();
        config.startup_validation = StartupValidationMode::Full;
        config.startup_validate_indexes = true;
        let db = Database::open_for_archive_restore(destination, config, context)?;
        db.prepare_snapshot_indexes()?;
        db.checkpoint_for_shutdown()?;
    }
    if target == source_end {
        // A standalone restored image owns a local WAL position domain. Its
        // next append must follow every page written by the offline sealer.
        let mut log = crate::wal::LogManager::open(destination)?;
        log.set_local_recovery(true);
        let checkpoint = log
            .last_lsn()
            .max(
                image
                    .seal
                    .as_ref()
                    .ok_or_else(|| backup_err("missing seal"))?
                    .image_checkpoint,
            )
            .checked_add(1)
            .ok_or_else(|| backup_err("local checkpoint overflow"))?;
        let visibility = image.seal.as_ref().unwrap().visibility_state.clone();
        log.append_reserved(checkpoint, 0, visibility.checkpoint(checkpoint))?;
        log.flush()?;
    }
    crate::wal::persist_source_clock(
        destination,
        source_time.max(image.manifest.end_commit_unix_us),
    )?;
    let scan = crate::wal::LogManager::scan_replay_with_mode(
        destination,
        crate::wal::WalReadMode::Strict,
    )?;
    let visibility = crate::storage::snapshot::SourceVisibility::from_wal(&scan.records)?;
    let seal = image
        .seal
        .as_mut()
        .ok_or_else(|| backup_err("missing base seal"))?;
    seal.source_end_lsn = target;
    seal.source_end_time = source_time.max(image.manifest.end_commit_unix_us);
    seal.source_clock_floor = seal.source_end_time;
    seal.next_source_lsn = target
        .checked_add(1)
        .ok_or_else(|| backup_err("target LSN overflow"))?;
    seal.image_checkpoint = scan.next_lsn.saturating_sub(1);
    seal.transaction_state_digest =
        sha256_hex(&serde_json::to_vec(&visibility).map_err(|e| backup_err(e.to_string()))?);
    seal.continuation_floor = visibility.dependency_floor();
    seal.retained_recovery_ranges = seal
        .continuation_floor
        .map(|floor| vec![(floor, target)])
        .unwrap_or_default();
    seal.visibility_state = visibility;
    seal.continuation_ready = false;
    image.manifest.end_lsn = target;
    image.manifest.end_commit_unix_us = seal.source_end_time;
    image.manifest.start_lsn = image.manifest.start_lsn.min(target);
    image.manifest.files = hash_image_files(destination)?;
    image.seal.as_mut().unwrap().object_replay_contracts = image
        .manifest
        .files
        .iter()
        .filter(|f| f.class != FileClass::RecoveryWal)
        .map(|f| ObjectReplayContract {
            path: f.path.clone(),
            mode: if f.class == FileClass::Index || f.class == FileClass::Derived {
                "REBUILD_FROM_AUTHORITY"
            } else {
                "SOURCE_PHYSICAL"
            }
            .into(),
            version: 1,
        })
        .collect();
    image.state = if writable {
        State::Writable
    } else {
        State::ReadOnly
    };
    sync_tree(destination)?;
    write_snapshot(destination, &image)?;
    sync_dir(destination)?;
    Ok(image)
}

fn hash_image_files(path: &Path) -> Result<Vec<BackupFile>, QueryError> {
    let sources = collect_snapshot_files(path, true)?;
    let _metadata = ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::SystemMetadata,
        estimate_snapshot_metadata(&sources),
    )?;
    let tasks = source_chunk_tasks(&sources);
    let mut chunks = (0..tasks.len()).map(|_| None).collect::<Vec<_>>();
    process_chunks_parallel(
        &tasks,
        0,
        |task, buffer, _| {
            let input = sources[task.file_index]
                .file
                .open()
                .map_err(|e| io_err("reopen image hash generation", e))?;
            read_exact_at(&input, buffer, task.offset).map_err(|err| io_err("hash image", err))?;
            Ok(ChunkRef {
                hash: sha256_hex(buffer),
                len: task.len as u64,
            })
        },
        |index, chunk| chunks[index] = Some(chunk),
    )?;
    let mut chunks = chunks.into_iter();
    sources
        .into_iter()
        .map(|source| {
            let count = source.len.div_ceil(CHUNK_BYTES as u64) as usize;
            Ok(BackupFile {
                path: source.relative,
                len: source.len,
                class: source.class,
                chunks: chunks
                    .by_ref()
                    .take(count)
                    .map(|chunk| {
                        chunk.ok_or_else(|| backup_err("image checksum result is missing"))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            })
        })
        .collect()
}

/// Resume only immutable baseline blocks. Recovery always receives a fresh
/// image, so an interrupted sealer's mutations are never accepted as input.
pub(super) fn cache_base(source: &Path, cache: &Path) -> Result<u64, QueryError> {
    validate_open(source)?;
    let image = inspect(source)?.ok_or_else(|| backup_err("missing immutable resume base"))?;
    if cache.exists() {
        reject_symlink_path(
            cache.parent().unwrap(),
            Path::new(cache.file_name().unwrap()),
        )?;
    } else {
        fs::create_dir(cache).map_err(|e| io_err("create resume base cache", e))?;
    }
    let _memory =
        ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, CHUNK_BYTES)?;
    let mut buffer = vec![0; CHUNK_BYTES];
    let mut reused = 0u64;
    for expected in &image.manifest.files {
        let target = cache.join(&expected.path);
        reject_symlink_path(cache, Path::new(&expected.path))?;
        fs::create_dir_all(target.parent().unwrap())
            .map_err(|e| io_err("create cached base path", e))?;
        let (mut output, _) = open_private_output(&target, true, true)?;
        let mut input = None;
        let mut offset = 0u64;
        use std::io::{Seek, SeekFrom};
        for chunk in &expected.chunks {
            let size =
                usize::try_from(chunk.len).map_err(|_| backup_err("invalid resume chunk size"))?;
            if size == 0 || size > CHUNK_BYTES {
                return Err(backup_err("invalid immutable resume chunk"));
            }
            output
                .seek(SeekFrom::Start(offset))
                .map_err(|e| io_err("seek cached base", e))?;
            let identical = match output.read_exact(&mut buffer[..size]) {
                Ok(()) => sha256_hex(&buffer[..size]) == chunk.hash,
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => false,
                Err(error) => return Err(io_err("verify cached base block", error)),
            };
            if identical {
                reused += chunk.len;
            } else {
                if input.is_none() {
                    input = Some(
                        std::fs::File::open(source.join(&expected.path))
                            .map_err(|e| io_err("open immutable base input", e))?,
                    );
                }
                let input = input.as_mut().unwrap();
                input
                    .seek(SeekFrom::Start(offset))
                    .and_then(|_| input.read_exact(&mut buffer[..size]))
                    .map_err(|e| io_err("read immutable base block", e))?;
                if sha256_hex(&buffer[..size]) != chunk.hash {
                    return Err(backup_err("immutable resume base checksum mismatch"));
                }
                super::resources::throttle(chunk.len)?;
                let _disk = reserve_disk_write(cache, chunk.len)?;
                output
                    .seek(SeekFrom::Start(offset))
                    .and_then(|_| output.write_all(&buffer[..size]))
                    .map_err(|e| io_err("write cached base block", e))?;
            }
            offset = offset
                .checked_add(chunk.len)
                .ok_or_else(|| backup_err("resume block offset overflow"))?;
        }
        if offset != expected.len {
            return Err(backup_err("immutable resume file length mismatch"));
        }
        if output
            .metadata()
            .map_err(|e| io_err("inspect cached base", e))?
            .len()
            != expected.len
        {
            output
                .set_len(expected.len)
                .map_err(|e| io_err("finish cached base length", e))?;
        }
    }
    write_sync(&cache.join(ID_FILE), image.manifest.backup_id.as_bytes())?;
    fs::create_dir_all(cache.join("system"))
        .map_err(|e| io_err("create cached image system", e))?;
    if !cache.join("system/mydb.lock").exists() {
        std::fs::File::create(cache.join("system/mydb.lock"))
            .map_err(|e| io_err("create cached image lock", e))?;
    }
    validate_inventory(cache, &image)?;
    verify_file_contents(cache, &image)?;
    write_snapshot(cache, &image)?;
    sync_tree(cache)?;
    Ok(reused)
}
