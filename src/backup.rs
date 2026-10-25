//! Single-tree physical baselines, optional WAL archiving and isolated recovery.

use crate::platform::file::{File, OpenOptions};
use crate::sql_lex::normalize_admin_sql as normalize_admin;
use crate::util::hex::encode_lower as encode_hex;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::engine::api::{QueryResponse, ResultSet};
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::parser::Statement;
use crate::storage::{error, Database, DurabilityMode, StartupValidationMode, StorageConfig};
use crate::types::{Column, ColumnType, QueryError, Value};

mod bundle;
mod cli;
mod coordinator;
mod diagnostics;
mod gc;
mod management;
pub mod native;
mod plan;
mod repository;
mod resources;
mod restore;
mod safety;
pub(crate) mod sql;
use crate::storage::file_generation::GenerationFile;
pub use coordinator::Policy as WalArchivePolicy;
use safety::*;
pub use sql::BackupCommand;
const BACKUP_FORMAT_VERSION: u32 = 1;
const CHUNK_BYTES: usize = 4 * 1024 * 1024;
const BLOB_MAGIC: &[u8; 8] = b"MYDBBK1\0";
#[cfg(test)]
static FAIL_SNAPSHOT_SYNC: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
mod audit_tests;
#[cfg(test)]
mod continuation_tests;
#[cfg(test)]
pub(crate) mod faults;
#[cfg(test)]
mod followup_tests;
#[cfg(test)]
mod safety_tests;
#[cfg(test)]
mod v1_tests;

pub use crate::storage::image::{BackupFile, BackupManifest, ChunkRef, FileClass};

#[derive(Clone, Deserialize)]

struct KeyFile {
    keys: HashMap<String, String>,
}

#[derive(Clone)]

struct CryptoKey {
    id: String,
    bytes: [u8; 32],
}

#[derive(Debug)]

struct SourceFile {
    path: PathBuf,
    relative: String,
    len: u64,
    class: FileClass,
    file: GenerationFile,
    _metadata: ControlledMemoryCharge,
}

#[derive(Debug)]

struct WalSnapshot {
    files: Vec<SourceFile>,
    last_lsn: u64,
    captured_unix_us: u64,
    hint: Option<(u64, u64)>,
}

#[derive(Debug)]

pub(crate) struct BackupRequest {
    pub repository: PathBuf,
}

impl Drop for CryptoKey {
    fn drop(&mut self) {
        self.bytes.fill(0);
    }
}

pub(crate) use crate::storage::capture::Admission;

pub(crate) fn ddl_admission<'a>(
    db: &'a Database,
    statement: &Statement,
) -> Result<Option<Admission<'a>>, QueryError> {
    if !crate::statement_effects::is_ddl_or_maintenance(statement)
        && !changes_source_lineage(statement)
    {
        return Ok(None);
    }
    let lease = Admission::enter(db)?;
    if db.capture.active.load(Ordering::Acquire) {
        return Err(backup_err("DDL/maintenance is blocked during capture"));
    }
    Ok(Some(lease))
}

fn changes_source_lineage(statement: &Statement) -> bool {
    let Statement::MydbReplicationAdmin { sql } = statement else {
        return false;
    };
    let sql = normalize_admin(sql);
    [
        "CHANGE MYDB MASTER",
        "DEMOTE TO REPLICA",
        "PROMOTE TO MASTER",
        "RESET MYDB REPLICATION",
    ]
    .iter()
    .any(|prefix| sql.starts_with(prefix))
}

pub fn wait_for_write_gate(
    db: &Database,
    txn_id: Option<u64>,
    statement: &Statement,
) -> Result<(), QueryError> {
    if db.capture.redo_required() && changes_source_lineage(statement) {
        return Err(backup_err(
            "stop WAL protection before changing the source lineage",
        ));
    }
    if db.capture.active.load(Ordering::Acquire)
        && crate::statement_effects::is_ddl_or_maintenance(statement)
    {
        return Err(backup_err(
            "DDL/maintenance is blocked during physical capture",
        ));
    }
    if !db.capture.frozen.load(Ordering::Acquire)
        || !crate::repl::is_role_write_statement(statement)
    {
        return Ok(());
    }
    if let Some(txn_id) = txn_id {
        if db.is_active_write_transaction(txn_id)? {
            return Ok(());
        }
    }
    db.capture.wait()
}

fn capture_database(
    db: &Arc<Database>,
    request: &BackupRequest,
    backup_id: &str,
    cancelled: &AtomicBool,
    mut progress: impl FnMut(&str, u64, u64, Option<u64>),
    mut boundary: impl FnMut(u64, u64, Option<(u64, u64)>) -> Result<(), QueryError>,
) -> Result<BackupManifest, QueryError> {
    let mut capture_stats = BTreeMap::new();
    if db.physical_replica_mode() {
        return Err(backup_err("online backup requires a standalone/master source; physical replica capture is not supported"));
    }
    reject_overlap(db.data_dir(), &request.repository)?;
    if db
        .capture
        .active
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(backup_err("another process-wide physical backup is active"));
    }
    let _active = InstanceCaptureGuard {
        db,
        lease: format!("capture/{backup_id}"),
    };
    let native_staging = native::Staging::new(db.data_dir(), &request.repository, backup_id)?;
    let source_id = load_or_create_source_id(db.data_dir())?;
    check_cancelled(cancelled)?;
    // Establish a valid on-disk page baseline without stopping writers. The
    // checkpoint is fuzzy-safe with concurrent DML; the following brief gate
    // only drains the boundary and pins its retained recovery floor.
    progress("INITIAL_CHECKPOINT", 0, 0, None);
    let checkpoint_started = Instant::now();
    db.checkpoint()?;
    capture_stats.insert(
        "initial_checkpoint_us".into(),
        micros(checkpoint_started.elapsed()),
    );
    check_cancelled(cancelled)?;
    progress("INITIAL_SNAPSHOT", 0, 0, None);
    let initial_freeze_started = Instant::now();
    let initial_freeze = db.capture.freeze();
    let drain_started = Instant::now();
    drain_backup_conflicts(db, Duration::from_secs(30), cancelled)?;
    capture_stats.insert("initial_drain_us".into(), micros(drain_started.elapsed()));
    if db.physical_replica_mode() {
        return Err(backup_err(
            "source became a physical replica before backup capture",
        ));
    }
    if load_or_create_source_id(db.data_dir())? != source_id {
        return Err(backup_err(
            "source lineage changed during backup initialization; retry",
        ));
    }
    let pin_started = Instant::now();
    let pin_floor = {
        let log_handle = db.wal_log();
        let mut log = log_handle.lock();
        log.flush()?;
        log.min_available_lsn()
    };
    db.capture
        .capture_floor
        .store(pin_floor.max(1), Ordering::Release);
    db.capture.set_lease(
        &_active.lease,
        Some(crate::storage::capture::WalLease {
            history_retention_ms: db.undo_retention_ms(),
            floor: pin_floor.max(1),
            proof: serde_json::to_string(&(request.repository.clone(), backup_id))
                .map_err(|e| backup_err(e.to_string()))?,
        }),
    )?;
    let start_lsn = pin_floor.saturating_sub(1);
    capture_stats.insert(
        "initial_wal_and_lease_sync_us".into(),
        micros(pin_started.elapsed()),
    );
    drop(initial_freeze);
    capture_stats.insert(
        "initial_freeze_us".into(),
        micros(initial_freeze_started.elapsed()),
    );
    // The WAL lease is already durable and physical DDL remains excluded.
    // Enumeration and inode pinning do not require stopping ordinary writers.
    progress("PIN_SOURCE_FILES", 0, 0, None);
    let inventory_started = Instant::now();
    let source_files = {
        let _maintenance = db.backup_maintenance_capture_guard()?;
        collect_capture_files(db.data_dir())?
    };
    capture_stats.insert(
        "initial_inventory_us".into(),
        micros(inventory_started.elapsed()),
    );
    #[cfg(test)]
    faults::hit("capture_files_pinned", db.data_dir())?;
    let mut snapshot_metadata = ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::SystemMetadata,
        estimate_snapshot_metadata(&source_files),
    )?;
    let mut files = Vec::with_capacity(source_files.len() + 8);
    let mut bytes_done = 0u64;
    let mut files_done = 0u64;
    let initial_total = source_files.iter().try_fold(0u64, |total, file| {
        total
            .checked_add(file.len)
            .ok_or_else(|| backup_err("capture size overflow"))
    })?;
    resources::check_spool(initial_total)?;
    progress("COPY_DATA", bytes_done, files_done, Some(initial_total));
    let initial_bytes = bytes_done;
    let initial_files = files_done;
    let (copied, read_bytes) = store_source_files_parallel(
        &source_files,
        &native_staging.path,
        Some(&db.capture),
        cancelled,
        |bytes, completed_files| {
            progress(
                "COPY_DATA",
                initial_bytes.saturating_add(bytes),
                initial_files.saturating_add(completed_files),
                Some(initial_total.saturating_sub(bytes)),
            )
        },
    )?;
    bytes_done = bytes_done.saturating_add(copied.iter().map(|file| file.len).sum::<u64>());
    files_done = files_done.saturating_add(copied.len() as u64);
    files.extend(copied);
    capture_stats.insert("copy_read_bytes".into(), read_bytes);
    drop(source_files);

    check_cancelled(cancelled)?;
    let existing = files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<BTreeSet<_>>();
    // Flush the bulk of the dirty working set while writers are still
    // admitted. The final in-freeze checkpoint normally has only the short
    // post-precheckpoint delta left, keeping the user-visible pause bounded.
    progress("PRECOPY_CHECKPOINT", bytes_done, files_done, None);
    db.checkpoint()?;
    check_cancelled(cancelled)?;
    progress("FINAL_BARRIER", bytes_done, files_done, None);
    let final_barrier_started = Instant::now();
    let freeze_budget = Duration::from_millis(resources::limits()?.freeze_budget_ms);
    let final_freeze = db.capture.freeze();
    let drain_started = Instant::now();
    drain_backup_conflicts(db, freeze_budget, cancelled)?;
    capture_stats.insert("final_drain_us".into(), micros(drain_started.elapsed()));
    let checkpoint_started = Instant::now();
    db.checkpoint()?;
    capture_stats.insert(
        "final_checkpoint_us".into(),
        micros(checkpoint_started.elapsed()),
    );
    // Newly discovered Zone Map files must not be copied after a later
    // rebuild has shrunk their bounds beyond this backup's WAL ceiling.
    let maintenance_capture = db.backup_maintenance_capture_guard()?;
    let zone_capture = db.zone_map_backup_capture_guard()?;
    let mut zone_snapshots = crate::repl::base_backup::BackupSnapshotFiles::default();
    let wal_started = Instant::now();
    let wal_snapshot = snapshot_wal_files(db)?;
    capture_stats.insert(
        "final_wal_snapshot_and_sync_us".into(),
        micros(wal_started.elapsed()),
    );
    let end_lsn = wal_snapshot.last_lsn;
    let end_commit_unix_us = wal_snapshot.captured_unix_us;
    if final_barrier_started.elapsed() > freeze_budget {
        return Err(backup_err(
            "final capture barrier exceeded its configured budget",
        ));
    }
    let inventory_started = Instant::now();
    let mut new_sources = collect_files(db.data_dir(), false, true, Some(&existing))?;
    capture_stats.insert(
        "final_inventory_us".into(),
        micros(inventory_started.elapsed()),
    );
    // Account for the original copy, private final generations, their staged
    // copies and the fixed WAL prefix before allocating final temporary files.
    let capture_peak = new_sources
        .iter()
        .flat_map(|file| [file.len, file.len])
        .chain(wal_snapshot.files.iter().map(|file| file.len))
        .try_fold(bytes_done, |total, len| {
            total
                .checked_add(len)
                .ok_or_else(|| backup_err("capture size overflow"))
        })?;
    resources::check_spool(capture_peak)?;
    let private_started = Instant::now();
    snapshot_metadata.try_grow(estimate_snapshot_metadata(&new_sources))?;
    for source in &mut new_sources {
        let mut input = source
            .file
            .open()
            .map_err(|e| io_err("reopen final generation", e))?
            .try_clone()
            .map_err(|e| io_err("clone final generation", e))?;
        let copy = zone_snapshots.capture_checked(db.data_dir(), &mut input, source.len, || {
            check_cancelled(cancelled)?;
            if final_barrier_started.elapsed() > freeze_budget {
                return Err(backup_err(
                    "final capture barrier exceeded its configured budget",
                ));
            }
            Ok(())
        })?;
        source.file = GenerationFile::immutable(
            zone_snapshots
                .last_path()
                .expect("captured snapshot path")
                .to_path_buf(),
            copy,
            source.len,
        )
        .map_err(|e| io_err("pin private final copy", e))?;
    }
    capture_stats.insert(
        "final_private_copy_us".into(),
        micros(private_started.elapsed()),
    );
    drop(zone_capture);
    drop(maintenance_capture);
    let handoff_started = Instant::now();
    boundary(end_lsn, end_commit_unix_us, wal_snapshot.hint)?;
    capture_stats.insert("final_handoff_us".into(), micros(handoff_started.elapsed()));
    if final_barrier_started.elapsed() > freeze_budget {
        return Err(backup_err(
            "final capture barrier exceeded its configured budget after synchronous handoff",
        ));
    }
    drop(final_freeze);
    capture_stats.insert(
        "final_freeze_us".into(),
        micros(final_barrier_started.elapsed()),
    );
    let final_barrier_ms = final_barrier_started
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;

    new_sources.extend(wal_snapshot.files);
    let final_total = new_sources.iter().map(|file| file.len).sum::<u64>();
    progress(
        "COPY_FINAL_FILES",
        bytes_done,
        files_done,
        Some(final_total),
    );
    let final_bytes = bytes_done;
    let final_files_done = files_done;
    let (copied, read_bytes) = store_source_files_parallel(
        &new_sources,
        &native_staging.path,
        Some(&db.capture),
        cancelled,
        |bytes, completed_files| {
            progress(
                "COPY_FINAL_FILES",
                final_bytes.saturating_add(bytes),
                final_files_done.saturating_add(completed_files),
                Some(final_total.saturating_sub(bytes)),
            )
        },
    )?;
    bytes_done = bytes_done.saturating_add(copied.iter().map(|file| file.len).sum::<u64>());
    files_done = files_done.saturating_add(copied.len() as u64);
    files.extend(copied);
    *capture_stats.entry("copy_read_bytes".into()).or_default() += read_bytes;
    capture_stats.insert("captured_bytes".into(), bytes_done);
    capture_stats.insert(
        "foreground_commit_ewma_us".into(),
        db.capture.commit_pressure.ewma_us.load(Ordering::Relaxed),
    );
    capture_stats.insert("captured_files".into(), files_done);
    capture_stats.insert(
        "data_fd_budget".into(),
        resources::limits()?.open_files as u64,
    );
    drop(new_sources);
    drop(zone_snapshots);
    files.sort_by(|a, b| a.path.cmp(&b.path));

    let now = now_ms();
    let manifest = BackupManifest {
        backup_format_version: BACKUP_FORMAT_VERSION,
        backup_id: backup_id.to_string(),
        source_id: source_id.clone(),
        created_unix_ms: now,
        completed_unix_ms: now,
        final_barrier_ms,
        capture_stats,
        start_lsn,
        end_lsn,
        end_commit_unix_us,
        history_retention_ms: db.undo_retention_ms(),
        storage_compatibility: crate::storage::format_versions::native_replication_compatibility(),
        files,
    };
    crate::wal::persist_source_clock(db.data_dir(), manifest.end_commit_unix_us)?;
    // Seed bytes are private before the offline worker starts.
    drop(_active);
    #[cfg(test)]
    faults::hit("capture_complete", db.data_dir())?;
    progress("PREPARING", bytes_done, files_done, None);
    native::finish(native_staging, manifest, cancelled, |phase| {
        progress(phase, bytes_done, files_done, None)
    })
}

fn estimate_snapshot_metadata(files: &[SourceFile]) -> usize {
    let chunks = files
        .iter()
        .map(|file| (file.len as usize).div_ceil(CHUNK_BYTES))
        .sum::<usize>();
    files
        .len()
        .saturating_mul(std::mem::size_of::<SourceFile>())
        .saturating_add(
            files
                .iter()
                .map(|file| file.relative.capacity().saturating_mul(3))
                .sum::<usize>(),
        )
        .saturating_add(chunks.saturating_mul(192))
        .max(64 * 1024)
}

fn drain_backup_conflicts(
    db: &Database,
    timeout: Duration,
    cancelled: &AtomicBool,
) -> Result<(), QueryError> {
    let deadline = Instant::now() + timeout;
    while db.has_active_archive_backup_conflicts()
        || db.capture.admissions.load(Ordering::SeqCst) != 0
    {
        check_cancelled(cancelled)?;
        if Instant::now() >= deadline {
            return Err(backup_err(format!(
                "timed out after {}ms draining active writers/physical DDL",
                timeout.as_millis()
            )));
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<(), QueryError> {
    if cancelled.load(Ordering::Acquire) {
        Err(backup_err("backup cancelled"))
    } else {
        Ok(())
    }
}

fn collect_snapshot_files(
    data_dir: &Path,
    include_wal: bool,
) -> Result<Vec<SourceFile>, QueryError> {
    collect_files(data_dir, include_wal, false, None)
}
fn collect_capture_files(data_dir: &Path) -> Result<Vec<SourceFile>, QueryError> {
    collect_files(data_dir, false, true, None)
}
struct SourceCollector<'a> {
    skip: Option<&'a BTreeSet<&'a str>>,
    files: Vec<SourceFile>,
    live: bool,
    owner: Option<Arc<DirectoryOperation>>,
    threshold: usize,
}
fn collect_files<'a>(
    data_dir: &Path,
    include_wal: bool,
    live: bool,
    skip: Option<&'a BTreeSet<&'a str>>,
) -> Result<Vec<SourceFile>, QueryError> {
    let limits = resources::limits()?;
    let mut files = SourceCollector {
        files: Vec::new(),
        skip,
        live,
        owner: None,
        threshold: (limits.open_files / 4 / limits.workers).max(1),
    };
    for directory in ["schemas", "objects", "system"] {
        reject_symlink_path(data_dir, Path::new(directory))?;
    }
    for root in [
        "schemas",
        "objects",
        "system/catalog",
        "system/undo",
        "system/xa-binlog",
    ] {
        collect_tree(data_dir, &data_dir.join(root), &mut files)?;
    }
    for relative in [
        "system/commit-ledger.v1",
        "system/commit-clock.v1",
        "system/history-index.v1",
        "system/tablespaces.json",
        "system/zone-map-checkpoint.manifest",
        "system/vector-purges.v1",
        "system/vector-maintenance.jobs",
    ] {
        let path = data_dir.join(relative);
        reject_symlink_path(data_dir, Path::new(relative))?;
        if path.is_file() {
            push_source_file(data_dir, path, &mut files, None)?;
        }
    }
    if include_wal {
        collect_tree(
            data_dir,
            &crate::storage::naming::system_wal_dir(data_dir),
            &mut files,
        )?;
    }
    files.files.sort_by(|a, b| a.relative.cmp(&b.relative));
    Ok(files.files)
}

fn collect_tree(data_dir: &Path, path: &Path, out: &mut SourceCollector) -> Result<(), QueryError> {
    if path
        .strip_prefix(data_dir)
        .map_or(true, |p| p.components().count() > 64)
    {
        return Err(backup_err("source directory depth exceeds budget"));
    }
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(io_err("backup metadata", err)),
    };
    if metadata.file_type().is_symlink() {
        return Err(backup_err(format!(
            "symlink is not allowed in the physical backup tree: {}",
            path.display()
        )));
    }
    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|e| io_err("backup read directory", e))? {
            collect_tree(
                data_dir,
                &entry.map_err(|e| io_err("backup read entry", e))?.path(),
                out,
            )?;
        }
    } else if metadata.is_file() {
        push_source_file(data_dir, path.to_path_buf(), out, None)?;
    }
    Ok(())
}

fn push_source_file(
    data_dir: &Path,
    path: PathBuf,
    out: &mut SourceCollector,
    len_override: Option<u64>,
) -> Result<(), QueryError> {
    let relative = relative_path(data_dir, &path)?;
    validate_restore_path(&relative)?;
    if out.skip.is_some_and(|set| set.contains(relative.as_str())) {
        return Ok(());
    }
    if out.files.len() >= 1_000_000 {
        return Err(backup_err("capture file inventory exceeds manifest limit"));
    }
    // Switch the entire set before opening another source descriptor. Mutable
    // sources retain owned hard links; immutable trees retain inode proofs.
    if out.files.len() == out.threshold {
        if out.live {
            out.owner = Some(Arc::new(DirectoryOperation::new(
                &data_dir.join("system/backup-generation-pins"),
                "generation-pins",
            )?));
        }
        for (index, source) in out.files.iter_mut().enumerate() {
            let old = std::mem::replace(
                &mut source.file,
                GenerationFile::Reopen {
                    path: PathBuf::new(),
                    identity: crate::storage::file_generation::Identity(0, 0),
                    minimum_len: 0,
                    _owner: None,
                },
            );
            let GenerationFile::Open(file) = old else {
                return Err(backup_err("unexpected file generation state"));
            };
            source.file = if let Some(owner) = &out.owner {
                GenerationFile::pin(
                    &source.path,
                    file,
                    owner.data.join(format!("{index:016x}")),
                    source.len,
                    owner.clone(),
                )
                .map_err(|e| io_err("pin source generation", e))?
            } else {
                GenerationFile::immutable(source.path.clone(), file, source.len)
                    .map_err(|e| io_err("bind immutable generation", e))?
            };
        }
    }
    let file = File::open(&path).map_err(|err| io_err("backup open source file", err))?;
    let len = len_override.unwrap_or(
        file.metadata()
            .map_err(|err| io_err("backup source metadata", err))?
            .len(),
    );
    let class = classify_file(&relative);
    let file = if out.files.len() >= out.threshold {
        if let Some(owner) = &out.owner {
            GenerationFile::pin(
                &path,
                file,
                owner.data.join(format!("{:016x}", out.files.len())),
                len,
                owner.clone(),
            )
            .map_err(|e| io_err("pin source generation", e))?
        } else {
            GenerationFile::immutable(path.clone(), file, len)
                .map_err(|e| io_err("bind immutable generation", e))?
        }
    } else {
        GenerationFile::Open(file)
    };
    let metadata = ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::SystemMetadata,
        std::mem::size_of::<SourceFile>() + relative.capacity() * 3 + 512,
    )?;
    out.files.push(SourceFile {
        path,
        relative,
        len,
        class,
        file,
        _metadata: metadata,
    });
    Ok(())
}

fn snapshot_wal_files(db: &Database) -> Result<WalSnapshot, QueryError> {
    let log_handle = db.wal_log();
    let mut log = log_handle.lock();
    log.flush()?;
    let mut infos = log.segment_snapshot();
    infos.retain(|info| info.first_lsn <= info.last_lsn);
    infos.sort_by_key(|info| info.segment_id);
    let last_lsn = infos.last().map(|info| info.last_lsn).unwrap_or(0);
    let hint = infos.last().map(|i| (i.segment_id, i.size_bytes));
    let threshold = (resources::limits()?.open_files / 4 / resources::limits()?.workers).max(1);
    let pins = if infos.len() > threshold {
        Some(Arc::new(DirectoryOperation::new(
            &wal_pin_target(db.data_dir())?,
            "wal-pins",
        )?))
    } else {
        None
    };
    let captured_unix_us = db.backup_time_fence();
    let mut out = Vec::with_capacity(infos.len());
    for info in infos {
        let relative = relative_path(db.data_dir(), &info.path)?;
        validate_restore_path(&relative)?;
        reject_symlink_path(db.data_dir(), Path::new(&relative))?;
        let file = File::open(&info.path).map_err(|err| io_err("backup open WAL segment", err))?;
        let file = if let Some(owner) = &pins {
            GenerationFile::pin(
                &info.path,
                file,
                owner.data.join(format!("{:020}", info.segment_id)),
                info.size_bytes,
                owner.clone(),
            )
            .map_err(|e| io_err("pin captured WAL generation", e))?
        } else {
            GenerationFile::Open(file)
        };
        let metadata = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            std::mem::size_of::<SourceFile>() + relative.capacity() * 3 + 512,
        )?;
        out.push(SourceFile {
            path: info.path,
            relative,
            len: info.size_bytes,
            class: FileClass::RecoveryWal,
            file,
            _metadata: metadata,
        });
    }
    drop(log);
    // Sync the exact captured prefix outside the append mutex. The opened
    // inodes and byte lengths stay fixed while newer commits remain free.
    #[cfg(test)]
    if FAIL_SNAPSHOT_SYNC.swap(false, Ordering::SeqCst) {
        return Err(backup_err("injected snapshot sync failure"));
    }
    crate::wal::LogManager::sync_durable_to(&log_handle, last_lsn)?;
    Ok(WalSnapshot {
        files: out,
        last_lsn,
        captured_unix_us,
        hint,
    })
}

fn classify_file(relative: &str) -> FileClass {
    match Path::new(relative).extension().and_then(|ext| ext.to_str()) {
        Some("idx") => FileClass::Index,
        Some("hnsw" | "hvec" | "vmanifest" | "vpub") => FileClass::VectorIndex,
        Some("zm") => FileClass::Derived,
        Some("log") if relative.starts_with("system/wal/") => FileClass::RecoveryWal,
        _ => FileClass::Authoritative,
    }
}

struct InstanceCaptureGuard<'a> {
    db: &'a Database,
    lease: String,
}
impl Drop for InstanceCaptureGuard<'_> {
    fn drop(&mut self) {
        let _ = self.db.capture.set_lease(&self.lease, None);
        self.db.capture.capture_floor.store(0, Ordering::Release);
        self.db.capture.active.store(false, Ordering::Release);
    }
}

#[derive(Clone, Copy)]
struct SourceChunkTask {
    file_index: usize,
    chunk_index: usize,
    offset: u64,
    len: usize,
}

struct PoolPanicGuard<'a>(&'a AtomicBool);

impl Drop for PoolPanicGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.store(true, Ordering::Release);
        }
    }
}

fn desired_chunk_parallelism(task_count: usize) -> usize {
    // Leave CPU capacity for foreground requests while backup workers hash
    // large chunks, including when the offline sealer runs in another process.
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .div_ceil(2)
        .min(task_count.max(1))
}

struct ChunkWorkerResources {
    _memory: ControlledMemoryCharge,
    _permit: resources::BulkPermit,
}

fn reserve_chunk_workers(
    task_count: usize,
    worker_bytes: usize,
) -> Result<Vec<ChunkWorkerResources>, QueryError> {
    if task_count == 0 {
        return Ok(Vec::new());
    }
    let limits = resources::limits()?;
    let desired = desired_chunk_parallelism(task_count)
        .min(limits.copy_threads)
        .min((limits.chunk_memory_bytes / worker_bytes).max(1));
    let mut charges = Vec::with_capacity(desired);
    let first = ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::NativeReplicationTransient,
        worker_bytes,
    )?;
    charges.push(ChunkWorkerResources {
        _memory: first,
        _permit: resources::BulkPermit::acquire(true)?.unwrap(),
    });
    for _ in 1..desired {
        let Some(charge) = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::NativeReplicationTransient,
            worker_bytes,
        ) else {
            break;
        };
        let Some(permit) = resources::BulkPermit::acquire(false)? else {
            break;
        };
        charges.push(ChunkWorkerResources {
            _memory: charge,
            _permit: permit,
        });
    }
    Ok(charges)
}

fn configure_chunk_worker_priority() {
    // Bulk checksums must yield CPU scheduling priority to request workers.
    // These are best-effort hints; admission and correctness never depend on
    // the operating system accepting a scheduling preference.
    #[cfg(target_os = "macos")]
    unsafe {
        libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_UTILITY, 0);
    }
    #[cfg(target_os = "linux")]
    unsafe {
        // Linux niceness is per thread. Use the explicit TID so this cannot
        // lower the priority of the server's foreground request threads.
        let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
        libc::setpriority(libc::PRIO_PROCESS, tid, 10);
    }
    #[cfg(windows)]
    unsafe {
        use windows_sys::Win32::System::Threading::{
            GetCurrentThread, SetThreadPriority, THREAD_PRIORITY_BELOW_NORMAL,
        };
        SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_BELOW_NORMAL);
    }
}

/// Share one bounded pool across capture, sealed-image hashing and verification.
/// Tasks are chunks rather than files so a single large heap uses every worker.
fn process_chunks_parallel<T: Send>(
    tasks: &[SourceChunkTask],
    scratch_bytes: usize,
    process: impl Fn(SourceChunkTask, &mut [u8], &mut [u8]) -> Result<T, QueryError> + Sync,
    mut completed: impl FnMut(usize, T),
) -> Result<(), QueryError> {
    let context = resources::context();
    let charges = reserve_chunk_workers(tasks.len(), CHUNK_BYTES + scratch_bytes)?;
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    // Bound queued results even if progress reporting is slower than workers.
    let (sender, receiver) = mpsc::sync_channel(charges.len().max(1));
    std::thread::scope(|scope| -> Result<(), QueryError> {
        for charge in charges {
            let context = context.clone();
            let sender = sender.clone();
            let next = &next;
            let failed = &failed;
            let process = &process;
            scope.spawn(move || {
                let _context = resources::ContextScope::enter(context);
                configure_chunk_worker_priority();
                let _memory = charge;
                let _panic = PoolPanicGuard(failed);
                let mut buffer = vec![0u8; CHUNK_BYTES];
                let mut scratch = vec![0u8; scratch_bytes];
                while !failed.load(Ordering::Acquire) {
                    let task_index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(task) = tasks.get(task_index).copied() else {
                        break;
                    };
                    let result = process(task, &mut buffer[..task.len], &mut scratch);
                    if result.is_err() {
                        failed.store(true, Ordering::Release);
                    }
                    if sender.send((task_index, result)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(sender);
        // Drop the receiver before joining workers on error, unblocking senders.
        let receiver = receiver;
        for _ in 0..tasks.len() {
            let (task_index, result) = receiver
                .recv()
                .map_err(|_| backup_err("backup chunk worker stopped unexpectedly"))?;
            completed(task_index, result?);
        }
        Ok(())
    })
}

fn source_chunk_tasks(sources: &[SourceFile]) -> Vec<SourceChunkTask> {
    let mut tasks = Vec::new();
    for (file_index, source) in sources.iter().enumerate() {
        for chunk_index in 0..source.len.div_ceil(CHUNK_BYTES as u64) {
            let offset = chunk_index * CHUNK_BYTES as u64;
            tasks.push(SourceChunkTask {
                file_index,
                chunk_index: chunk_index as usize,
                offset,
                len: (source.len - offset).min(CHUNK_BYTES as u64) as usize,
            });
        }
    }
    tasks
}

fn store_source_files_parallel(
    sources: &[SourceFile],
    native_dir: &Path,
    feedback: Option<&crate::storage::capture::CaptureState>,
    cancelled: &AtomicBool,
    mut progress: impl FnMut(u64, u64),
) -> Result<(Vec<BackupFile>, u64), QueryError> {
    let read_bytes = std::sync::atomic::AtomicU64::new(0);
    for source in sources {
        let path = native_dir.join(&source.relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| io_err("create capture directory", e))?;
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| io_err("create capture file", e))?;
        let _disk = reserve_disk_write(native_dir, source.len)?;
        file.set_len(source.len)
            .map_err(|e| io_err("size capture file", e))?;
    }
    let tasks = source_chunk_tasks(sources);
    let file_chunk_counts = sources
        .iter()
        .map(|source| source.len.div_ceil(CHUNK_BYTES as u64) as usize)
        .collect::<Vec<_>>();
    let mut completed_files = file_chunk_counts
        .iter()
        .filter(|count| **count == 0)
        .count() as u64;
    let mut completed_chunks = vec![0usize; sources.len()];
    let mut chunk_results = (0..tasks.len()).map(|_| None).collect::<Vec<_>>();
    let mut bytes_done = 0u64;
    process_chunks_parallel(
        &tasks,
        CHUNK_BYTES,
        |task, buffer, compare| {
            check_cancelled(cancelled)?;
            if let Some(state) = feedback {
                resources::feedback(state)?;
            }
            let source = &sources[task.file_index];
            let input = source
                .file
                .open()
                .map_err(|e| io_err("reopen captured generation", e))?;
            read_stable_chunk_with(
                task.offset,
                buffer,
                compare,
                &source.path,
                |buffer, offset| {
                    read_bytes.fetch_add(buffer.len() as u64, Ordering::Relaxed);
                    read_exact_at(&input, buffer, offset)
                },
            )?;
            let hash = sha256_hex(buffer);
            let _disk = reserve_disk_write(native_dir, task.len as u64)?;
            let output = OpenOptions::new()
                .write(true)
                .open(native_dir.join(&source.relative))
                .map_err(|e| io_err("open capture output", e))?;
            write_all_at(&output, buffer, task.offset)
                .map_err(|e| io_err("write capture file", e))?;
            resources::io()
                .image_write_bytes
                .fetch_add(task.len as u64, Ordering::Relaxed);
            Ok(ChunkRef {
                hash,
                len: task.len as u64,
            })
        },
        |task_index, chunk| {
            let task = tasks[task_index];
            bytes_done = bytes_done.saturating_add(chunk.len);
            chunk_results[task_index] = Some(chunk);
            completed_chunks[task.file_index] += 1;
            if completed_chunks[task.file_index] == file_chunk_counts[task.file_index] {
                completed_files = completed_files.saturating_add(1);
            }
            progress(bytes_done, completed_files);
        },
    )?;

    let mut files = Vec::with_capacity(sources.len());
    let mut result_index = 0usize;
    for (file_index, source) in sources.iter().enumerate() {
        let chunk_count = file_chunk_counts[file_index];
        let chunks = chunk_results[result_index..result_index + chunk_count]
            .iter_mut()
            .map(|chunk| {
                chunk
                    .take()
                    .ok_or_else(|| backup_err("backup chunk result is missing"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        result_index += chunk_count;
        files.push(BackupFile {
            path: source.relative.clone(),
            len: source.len,
            class: source.class,
            chunks,
        });
    }
    Ok((files, read_bytes.load(Ordering::Relaxed)))
}

#[cfg(test)]
fn read_stable_chunk(
    file: &File,
    offset: u64,
    output: &mut [u8],
    compare: &mut [u8],
    path: &Path,
) -> Result<(), QueryError> {
    read_stable_chunk_with(offset, output, compare, path, |buffer, position| {
        read_exact_at(file, buffer, position)
    })
}

fn read_stable_chunk_with(
    offset: u64,
    output: &mut [u8],
    compare: &mut [u8],
    path: &Path,
    mut read: impl FnMut(&mut [u8], u64) -> std::io::Result<()>,
) -> Result<(), QueryError> {
    let page_bytes = crate::storage::PAGE_SIZE;
    let paged = matches!(
        path.extension().and_then(|value| value.to_str()),
        Some("heap" | "idx" | "lob" | "ovf" | "hvec" | "zm")
    );
    // Compare bulk reads, retrying only pages changed by a concurrent writer.
    // Each accepted page still has two identical reads and a valid CRC.
    read(output, offset).map_err(|err| io_err("read backup chunk", err))?;
    let compare_bytes = compare.len() / page_bytes * page_bytes;
    assert!(compare_bytes > 0);
    for (batch_index, batch) in output.chunks_mut(compare_bytes).enumerate() {
        let batch_offset = offset + (batch_index * compare_bytes) as u64;
        let scratch = &mut compare[..batch.len()];
        read(scratch, batch_offset).map_err(|err| io_err("re-read backup chunk", err))?;
        for (page_idx, (page, scratch)) in batch
            .chunks_mut(page_bytes)
            .zip(scratch.chunks_mut(page_bytes))
            .enumerate()
        {
            let page_offset = batch_offset + (page_idx * page_bytes) as u64;
            let mut stable = false;
            for attempt in 0..8 {
                if page == scratch && (!paged || crate::storage::backup_page_valid(page)) {
                    stable = true;
                    break;
                }
                page.copy_from_slice(scratch);
                if attempt == 7 {
                    break;
                }
                std::thread::yield_now();
                read(scratch, page_offset).map_err(|err| {
                    backup_err(format!(
                        "re-read backup page {} at {page_offset}: {err}",
                        path.display()
                    ))
                })?;
            }
            if !stable {
                return Err(backup_err(format!(
                "page {} at offset {} failed stable-image/CRC validation after 8 reads; retry the backup",
                path.display(),
                page_offset
            )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
static IMAGE_READ_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
fn read_exact_at(file: &File, output: &mut [u8], offset: u64) -> std::io::Result<()> {
    resources::throttle(output.len() as u64)
        .map_err(|e| std::io::Error::other(error_detail(&e)))?;
    #[cfg(test)]
    IMAGE_READ_BYTES.fetch_add(output.len() as u64, Ordering::Relaxed);
    let result = crate::util::file::read_exact_at_counted(
        file,
        output,
        offset,
        false,
        Some("source file ended during backup"),
    );
    if let Ok(calls) = &result {
        resources::io()
            .image_read_bytes
            .fetch_add(output.len() as u64, Ordering::Relaxed);
        resources::io()
            .image_read_calls
            .fetch_add(*calls, Ordering::Relaxed);
    }
    result.map(|_| ())
}

#[cfg(test)]
fn encode_blob(plaintext: &[u8], key: Option<&CryptoKey>) -> Result<Vec<u8>, QueryError> {
    if plaintext.len() > MAX_METADATA_BYTES {
        return Err(backup_err(
            "backup metadata exceeds the supported size limit",
        ));
    }
    let digest = sha256_digest(plaintext);
    encode_blob_with_digest(plaintext, &digest, key)
}

#[cfg(test)]
fn encode_blob_with_digest(
    plaintext: &[u8],
    digest: &[u8; 32],
    key: Option<&CryptoKey>,
) -> Result<Vec<u8>, QueryError> {
    encode_blob_with_encoding(plaintext, digest, key, true)
}

fn encode_blob_with_encoding(
    plaintext: &[u8],
    digest: &[u8; 32],
    key: Option<&CryptoKey>,
    compress: bool,
) -> Result<Vec<u8>, QueryError> {
    let compressed = if compress {
        lz4_flex::compress_prepend_size(plaintext)
    } else {
        Vec::new()
    };
    let use_compressed =
        compress && compressed.len().saturating_mul(8) <= plaintext.len().saturating_mul(7);
    let mut payload = if use_compressed {
        compressed
    } else {
        plaintext.to_vec()
    };
    let mut flags = if use_compressed { 1u8 } else { 0u8 };
    let mut nonce = [0u8; 12];
    if let Some(key) = key {
        rand::rngs::OsRng.fill_bytes(&mut nonce);
        let cipher = Aes256Gcm::new_from_slice(&key.bytes)
            .map_err(|_| backup_err("invalid AES-256-GCM key"))?;
        payload = cipher
            .encrypt(Nonce::from_slice(&nonce), payload.as_ref())
            .map_err(|_| backup_err("AES-256-GCM backup encryption failed"))?;
        flags |= 2;
    }
    let mut out = Vec::with_capacity(8 + 1 + 12 + 8 + 8 + 32 + payload.len());
    out.extend_from_slice(BLOB_MAGIC);
    out.push(flags);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&(plaintext.len() as u64).to_le_bytes());
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(digest);
    out.extend_from_slice(&payload);
    Ok(out)
}

#[cfg(test)]
fn decode_blob(encoded: &[u8], key: Option<&CryptoKey>) -> Result<Vec<u8>, QueryError> {
    decode_blob_expected(encoded, key, None)
}

fn decode_blob_expected(
    encoded: &[u8],
    key: Option<&CryptoKey>,
    expected_hash: Option<&str>,
) -> Result<Vec<u8>, QueryError> {
    const HEADER: usize = 8 + 1 + 12 + 8 + 8 + 32;
    if encoded.len() < HEADER || &encoded[..8] != BLOB_MAGIC {
        return Err(backup_err("invalid backup blob header"));
    }
    let flags = encoded[8];
    if flags & !3 != 0 {
        return Err(backup_err("unsupported backup blob flags"));
    }
    let nonce = &encoded[9..21];
    let original_len = u64::from_le_bytes(encoded[21..29].try_into().unwrap());
    let limit = if expected_hash.is_some() {
        CHUNK_BYTES
    } else {
        MAX_METADATA_BYTES
    };
    if original_len > limit as u64 || encoded.len() > limit + 64 * 1024 {
        return Err(backup_err("backup blob exceeds decoded/encoded size limit"));
    }
    let _metadata_decode = if expected_hash.is_none() {
        Some(ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            original_len as usize * 3 + encoded.len(),
        )?)
    } else {
        None
    };
    let payload_len = u64::from_le_bytes(encoded[29..37].try_into().unwrap());
    let embedded_hash = &encoded[37..69];
    if let Some(requested_hash) = expected_hash {
        if requested_hash != encode_hex(embedded_hash) {
            return Err(backup_err(
                "backup blob plaintext checksum does not match its content address",
            ));
        }
    }
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| backup_err("backup blob payload is too large for this platform"))?;
    if payload_len != encoded.len() - HEADER {
        return Err(backup_err("backup blob payload length mismatch"));
    }
    let mut payload = encoded[HEADER..].to_vec();
    if flags & 2 != 0 {
        let key =
            key.ok_or_else(|| backup_err("backup is encrypted but no matching key was supplied"))?;
        let cipher = Aes256Gcm::new_from_slice(&key.bytes)
            .map_err(|_| backup_err("invalid AES-256-GCM key"))?;
        payload = cipher
            .decrypt(Nonce::from_slice(nonce), payload.as_ref())
            .map_err(|_| {
                backup_err("backup authentication failed (wrong key or corrupted data)")
            })?;
    }
    let plaintext = if flags & 1 != 0 {
        if payload.len() < 4
            || u32::from_le_bytes(payload[..4].try_into().unwrap()) as u64 != original_len
        {
            return Err(backup_err(
                "compressed backup size disagrees with bounded original length",
            ));
        }
        lz4_flex::decompress_size_prepended(&payload)
            .map_err(|_| backup_err("corrupt compressed backup blob"))?
    } else {
        payload
    };
    if plaintext.len() as u64 != original_len
        || Sha256::digest(&plaintext).as_slice() != embedded_hash
    {
        return Err(backup_err("backup blob plaintext checksum mismatch"));
    }
    Ok(plaintext)
}

fn resolve_key(
    key_id: Option<&str>,
    explicit_file: Option<&Path>,
) -> Result<Option<CryptoKey>, QueryError> {
    let Some(key_id) = key_id else {
        return Ok(None);
    };
    if key_id.eq_ignore_ascii_case("plain")
        || key_id.is_empty()
        || !key_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
    {
        return Err(backup_err(
            "backup encryption key id may contain only ASCII letters, digits, '-' and '_' and may not be 'plain'",
        ));
    }
    let path = explicit_file
        .map(Path::to_path_buf)
        .or_else(|| crate::startup_config::var_os("MYDB_BACKUP_KEY_FILE").map(PathBuf::from))
        .ok_or_else(|| {
            backup_err("ENCRYPTION_KEY_ID requires --key-file or MYDB_BACKUP_KEY_FILE")
        })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let mode = fs::metadata(&path)
            .map_err(|err| io_err("backup key file metadata", err))?
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            return Err(backup_err(format!(
                "backup key file {} must not be accessible by group/other (mode {:o})",
                path.display(),
                mode
            )));
        }
    }
    let raw = read_bounded(&path, 64 * 1024)?;
    let encoded = if let Ok(file) = serde_json::from_slice::<KeyFile>(&raw) {
        file.keys.get(key_id).cloned().ok_or_else(|| {
            backup_err(format!(
                "backup key id '{key_id}' is absent from {}",
                path.display()
            ))
        })?
    } else {
        String::from_utf8(raw)
            .map_err(|_| backup_err("backup key file must be JSON or 64 hexadecimal characters"))?
            .trim()
            .to_string()
    };
    let decoded = decode_hex(&encoded)?;
    if decoded.len() != 32 {
        return Err(backup_err(
            "AES-256-GCM backup key must contain exactly 32 bytes",
        ));
    }
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&decoded);
    Ok(Some(CryptoKey {
        id: key_id.to_string(),
        bytes,
    }))
}

fn load_or_create_source_id(data_dir: &Path) -> Result<String, QueryError> {
    #[derive(Serialize, Deserialize)]
    struct SourceIdentity {
        version: u32,
        source_id: String,
        #[serde(default)]
        lineage: String,
    }
    let lineage = crate::repl::role::load(data_dir)
        .map_err(|err| io_err("read backup source lineage", err))?
        .filter(|role| !role.timeline_uuid.is_empty() || !role.cluster_uuid.is_empty())
        .map(|role| {
            format!(
                "{}:{}:{}",
                role.cluster_uuid, role.timeline_uuid, role.generation
            )
        })
        .unwrap_or_default();
    let dir = data_dir.join("system/backup");
    let path = dir.join("source.json");
    if path.exists() {
        let identity: SourceIdentity = serde_json::from_slice(&read_bounded(&path, 4096)?)
            .map_err(|err| backup_err(format!("invalid backup source identity: {err}")))?;
        if identity.version != 1 || identity.source_id.is_empty() {
            return Err(backup_err("invalid backup source identity version/value"));
        }
        validate_id(&identity.source_id)?;
        if identity.lineage == lineage {
            return Ok(identity.source_id);
        }
    }
    create_directory_durable(&dir)?;
    let identity = SourceIdentity {
        version: 1,
        source_id: random_uuid(),
        lineage,
    };
    atomic_replace(
        &path,
        &serde_json::to_vec_pretty(&identity)
            .map_err(|err| backup_err(format!("serialize backup source identity: {err}")))?,
    )?;
    Ok(identity.source_id)
}

fn relative_path(root: &Path, path: &Path) -> Result<String, QueryError> {
    let relative = path.strip_prefix(root).map_err(|_| {
        backup_err(format!(
            "backup source {} escapes data directory {}",
            path.display(),
            root.display()
        ))
    })?;
    Ok(relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

fn validate_restore_path(relative: &str) -> Result<(), QueryError> {
    let path = Path::new(relative);
    if relative.split('/').count() > 64
        || relative.is_empty()
        || relative.len() > 4096
        || relative.contains(['\\', ':', '\0'])
        || relative
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(backup_err(format!("unsafe backup path '{relative}'")));
    }
    let allowed = relative.starts_with("schemas/")
        || relative.starts_with("objects/")
        || relative.starts_with("system/catalog/")
        || relative.starts_with("system/undo/")
        || relative.starts_with("system/xa-binlog/")
        || relative.starts_with("system/wal/")
        || matches!(
            relative,
            "system/commit-ledger.v1"
                | "system/commit-clock.v1"
                | "system/history-index.v1"
                | "system/tablespaces.json"
                | "system/zone-map-checkpoint.manifest"
                | "system/vector-purges.v1"
                | "system/vector-maintenance.jobs"
        );
    if !allowed {
        return Err(backup_err(format!(
            "backup path '{relative}' is outside the persistent restore set"
        )));
    }
    Ok(())
}

fn confirm_immutable(path: &Path) -> Result<(), QueryError> {
    let file = File::open(path).map_err(|e| io_err("open immutable backup object", e))?;
    crate::platform::fs::sync_before_atomic_rename_at(&file, path)
        .map_err(|e| io_err("confirm immutable backup object", e))?;
    let parent = path
        .parent()
        .ok_or_else(|| backup_err("immutable object has no parent"))?;
    #[cfg(test)]
    faults::hit("object_parent_sync", parent)?;
    sync_dir(parent)
}

fn atomic_write_new(path: &Path, bytes: &[u8]) -> Result<(), QueryError> {
    atomic_write_new_validated(path, bytes, || {
        if fs::symlink_metadata(path)
            .map_err(|e| io_err("inspect immutable object", e))?
            .file_type()
            .is_symlink()
            || read_bounded(path, bytes.len())? != bytes
        {
            return Err(backup_err("conflicting immutable object already exists"));
        }
        Ok(())
    })
}

/// A visible name is reusable only after content and durability are confirmed.
fn atomic_write_new_validated(
    path: &Path,
    bytes: &[u8],
    validate_existing: impl Fn() -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    atomic_write_new_impl(path, bytes, validate_existing, false)
}

/// Repository-v2 directories are installed and synced with the repository
/// genesis. Their names never need lazy creation during object publication.
fn atomic_write_new_ready(
    path: &Path,
    bytes: &[u8],
    validate_existing: impl Fn() -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    atomic_write_new_impl(path, bytes, validate_existing, true)
}
fn atomic_write_new_impl(
    path: &Path,
    bytes: &[u8],
    validate_existing: impl Fn() -> Result<(), QueryError>,
    parent_ready: bool,
) -> Result<(), QueryError> {
    let validate_regular = || -> Result<(), QueryError> {
        let metadata =
            fs::symlink_metadata(path).map_err(|e| io_err("inspect immutable object", e))?;
        if crate::platform::fs::is_link(&metadata) || !metadata.file_type().is_file() {
            return Err(backup_err("immutable object is not a regular file"));
        }
        validate_existing()
    };
    if path.exists() {
        validate_regular()?;
        return confirm_immutable(path);
    }
    if let Some(parent) = path.parent() {
        if parent_ready {
            reject_directory_links(parent)?;
            if !parent.is_dir() {
                return Err(backup_err("durable repository namespace is missing"));
            }
        } else {
            create_directory_durable(parent)?;
        }
    }
    let tmp = path.with_extension(format!("tmp-{}", random_uuid()));
    struct TemporaryFile(PathBuf);
    impl Drop for TemporaryFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }
    let _cleanup = TemporaryFile(tmp.clone());
    write_ordered(&tmp, bytes)?;
    match fs::hard_link(&tmp, path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            validate_regular()?;
            fs::remove_file(&tmp)
                .map_err(|error| io_err("remove competing object staging", error))?;
            return confirm_immutable(path);
        }
        Err(error) => return Err(io_err("publish immutable backup object", error)),
    }
    fs::remove_file(&tmp).map_err(|error| io_err("remove atomic-write staging", error))?;
    if let Some(parent) = path.parent() {
        #[cfg(test)]
        faults::hit("object_parent_sync", parent)?;
        sync_dir(parent)?;
    }
    Ok(())
}

fn sync_paths_parallel(paths: &[PathBuf]) -> Result<(), QueryError> {
    if paths.is_empty() {
        return Ok(());
    }
    let context = resources::context();
    let mut grants = vec![resources::BulkPermit::acquire(true)?.unwrap()];
    while grants.len() < resources::limits()?.copy_threads.min(paths.len()).max(1) {
        let Some(grant) = resources::BulkPermit::acquire(false)? else {
            break;
        };
        grants.push(grant);
    }
    let next = AtomicUsize::new(0);
    let (sender, receiver) = mpsc::sync_channel(grants.len().max(1));
    std::thread::scope(|scope| -> Result<(), QueryError> {
        // On an error, drop the receiver before joining workers so a blocked
        // producer cannot deadlock the scoped join.
        let receiver = receiver;
        #[cfg(target_os = "macos")]
        let mut devices = BTreeMap::new();
        #[cfg(target_os = "macos")]
        let device_limit =
            (resources::limits()?.open_files / 4 / resources::limits()?.workers).max(1);
        for grant in grants {
            let context = context.clone();
            let sender = sender.clone();
            let next = &next;
            scope.spawn(move || {
                let _context = resources::ContextScope::enter(context);
                let _grant = grant;
                configure_chunk_worker_priority();
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(path) = paths.get(index) else {
                        break;
                    };
                    #[cfg(target_os = "macos")]
                    let result = crate::platform::fs::sync_path_to_device(path)
                        .map_err(|err| io_err("sync backup object batch", err));
                    #[cfg(not(target_os = "macos"))]
                    let result = crate::platform::fs::sync_path(path)
                        .map_err(|err| io_err("sync backup object batch", err));
                    if result.is_ok() {
                        resources::io()
                            .helper_sync_calls
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    if sender.send(result).is_err() {
                        break;
                    }
                }
            });
        }
        drop(sender);
        for _ in 0..paths.len() {
            let synced = receiver
                .recv()
                .map_err(|_| backup_err("backup sync worker stopped unexpectedly"))??;
            #[cfg(target_os = "macos")]
            if let Some((device, file)) = synced {
                if !devices.contains_key(&device) && devices.len() >= device_limit {
                    let (_, oldest) = devices.pop_first().expect("device batch is nonempty");
                    File::sync_all(&oldest)
                        .map_err(|err| io_err("sync backup device batch", err))?;
                }
                devices.insert(device, file);
            }
            #[cfg(not(target_os = "macos"))]
            let _ = synced;
        }
        #[cfg(target_os = "macos")]
        for file in devices.values() {
            // All file fsyncs in this group have completed. F_FULLFSYNC drains
            // each device once before the caller can publish or acknowledge.
            File::sync_all(file).map_err(|err| io_err("sync backup device batch", err))?;
        }
        Ok(())
    })
}

fn atomic_replace(path: &Path, bytes: &[u8]) -> Result<(), QueryError> {
    atomic_replace_with_sync(path, bytes, sync_dir)
}

fn atomic_replace_with_sync(
    path: &Path,
    bytes: &[u8],
    sync_parent: impl FnOnce(&Path) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| io_err("create atomic-replace parent", err))?;
    }
    let tmp = path.with_extension(format!("tmp-{}", random_uuid()));
    struct ReplaceTemporary(PathBuf);
    impl Drop for ReplaceTemporary {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }
    let _cleanup = ReplaceTemporary(tmp.clone());
    write_ordered(&tmp, bytes)?;
    fs::rename(&tmp, path).map_err(|err| io_err("replace backup metadata", err))?;
    if let Some(parent) = path.parent() {
        sync_parent(parent)?;
    }
    Ok(())
}

fn write_sync(path: &Path, bytes: &[u8]) -> Result<(), QueryError> {
    write_with_sync(path, bytes, |file| {
        crate::platform::fs::sync_standard_file_at(file, path)
    })
}

/// Order private file data before publishing its name. The mandatory parent
/// directory sync after link/rename supplies the full durability barrier.
/// On macOS this avoids two drive-cache flushes for one atomic publication;
/// platforms without an ordering-only primitive keep their existing fsync.
fn write_ordered(path: &Path, bytes: &[u8]) -> Result<(), QueryError> {
    write_with_sync(path, bytes, |file| {
        #[cfg(windows)]
        return crate::platform::fs::sync_standard_file_at(file, path);
        #[cfg(not(windows))]
        crate::platform::fs::sync_before_atomic_rename_at(file, path)
    })
}

fn write_with_sync(
    path: &Path,
    bytes: &[u8],
    sync: impl FnOnce(&std::fs::File) -> std::io::Result<()>,
) -> Result<(), QueryError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| io_err("create file parent", err))?;
    }
    let (mut file, created) = open_private_output(path, false, true)?;
    if !created {
        file.set_len(0)
            .map_err(|err| io_err("truncate private backup file", err))?;
    }
    file.write_all(bytes)
        .map_err(|err| io_err("write backup file", err))?;
    sync(&file).map_err(|err| io_err("sync backup file", err))?;
    resources::io()
        .helper_sync_calls
        .fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn sync_tree(root: &Path) -> Result<(), QueryError> {
    let mut files = Vec::new();
    let mut dirs: BTreeMap<usize, Vec<PathBuf>> = BTreeMap::new();
    crate::util::file::visit_tree_postorder(
        root,
        "symlink in restore staging tree",
        &mut |path, metadata| {
            if metadata.is_dir() {
                dirs.entry(path.components().count())
                    .or_default()
                    .push(path.to_path_buf());
            } else if metadata.is_file() {
                files.push(path.to_path_buf());
            }
            Ok(())
        },
    )
    .map_err(|err| io_err("inspect restore tree", err))?;
    sync_paths_parallel(&files)?;
    for (_, paths) in dirs.into_iter().rev() {
        sync_paths_parallel(&paths)?;
    }
    Ok(())
}

fn sync_dir(path: &Path) -> Result<(), QueryError> {
    crate::platform::fs::sync_path(path).map_err(|err| io_err("sync directory", err))?;
    resources::io()
        .helper_sync_calls
        .fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    encode_hex(&sha256_digest(bytes))
}

fn sha256_digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn decode_hex(value: &str) -> Result<Vec<u8>, QueryError> {
    crate::util::hex::decode(value.as_bytes()).map_err(|err| {
        backup_err(match err {
            crate::util::hex::DecodeError::OddLength => "hexadecimal backup key has odd length",
            crate::util::hex::DecodeError::InvalidDigit => {
                "backup key contains a non-hexadecimal character"
            }
        })
    })
}

fn random_uuid() -> String {
    let mut bytes = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    crate::util::uuid::format(&bytes)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn backup_err(message: impl Into<String>) -> QueryError {
    error(crate::error::catalog::MYDB_ER_BACKUP_FAILED, message.into())
}

fn io_err(context: &str, err: std::io::Error) -> QueryError {
    crate::util::io_error::map_io_error_with_default(
        context,
        err,
        crate::error::catalog::MYDB_ER_BACKUP_FAILED,
    )
}

fn error_detail(err: &QueryError) -> String {
    match err.diagnostic_detail() {
        Some(detail) => format!("{}: {detail}", err.message()),
        None => err.message().to_owned(),
    }
}

fn column(name: &str, column_type: ColumnType) -> Column {
    Column {
        source: Default::default(),
        name: name.to_string(),
        column_type,
    }
}

fn repository_from_uri(value: &str) -> Result<PathBuf, QueryError> {
    let path = value.strip_prefix("file://").unwrap_or(value);
    #[cfg(windows)]
    let path = if value.starts_with("file://")
        && path.as_bytes().first() == Some(&b'/')
        && path.as_bytes().get(1).is_some_and(u8::is_ascii_alphabetic)
        && path.as_bytes().get(2) == Some(&b':')
    {
        &path[1..]
    } else {
        path
    };
    let path = PathBuf::from(path);
    if !path.is_absolute() {
        return Err(backup_err(
            "backup repository must be an absolute file:// URI/path",
        ));
    }
    Ok(path)
}

pub fn cli_main(args: Vec<String>) -> Result<(), QueryError> {
    cli::run(args)
}
#[derive(Debug)]
pub struct BackupStartupGuard {
    _lock: crate::storage::dir_lock::DataDirLock,
}
pub fn prepare_archive_retention(root: &Path) -> Result<BackupStartupGuard, QueryError> {
    // A first server boot reaches this guard before storage creates its tree.
    fs::create_dir_all(crate::storage::naming::system_dir(root))
        .map_err(|e| io_err("create startup lock directory", e))?;
    let lock = crate::storage::dir_lock::DataDirLock::acquire(root)?;
    DirectoryOperation::cleanup(
        &root.join("system/backup-generation-pins"),
        "generation-pins",
    )?;
    let state = coordinator::read_control(root)?;
    let capture = crate::storage::capture::CaptureState::open(root, false)?;
    let leases = capture.leases();
    if state.is_none() && leases.contains_key("archive") {
        return Err(backup_err(
            "durable archive lease has lost its source control state",
        ));
    }
    if let Some(state) = &state {
        if state.effective != coordinator::Policy::Disabled
            && (state.stream_id.is_some() || state.pending.is_some())
        {
            let expected = state
                .pending
                .as_ref()
                .map_or(state.cursor, |p| p.end)
                .checked_add(1)
                .ok_or_else(|| backup_err("source retention floor overflow"))?;
            if leases
                .get("archive")
                .is_none_or(|lease| lease.floor > expected)
            {
                return Err(backup_err("active backup control has a missing or advanced WAL lease; recovery was not started"));
            }
        }
    }
    // An interrupted capture cannot resume against a changing source. The
    // distinct pending archive lease protects every promised E+1 handoff.
    for name in leases.keys().filter(|name| name.starts_with("capture/")) {
        capture.set_lease(name, None)?;
    }
    if state
        .as_ref()
        .is_some_and(|s| s.effective == coordinator::Policy::Disabled && s.lifecycle == "CLOSED")
        && leases.contains_key("archive")
    {
        capture.set_lease("archive", None)?;
    }
    Ok(BackupStartupGuard { _lock: lock })
}
pub fn init_background_archiver(db: Arc<Database>) -> Result<(), QueryError> {
    coordinator::Coordinator::get(&db).map(|_| ())
}
pub(crate) fn shutdown_jobs(db: &Arc<Database>) -> Result<(), QueryError> {
    if db.is_snapshot_read_only() {
        return Ok(());
    }
    coordinator::Coordinator::get(db)?.shutdown()
}
pub(crate) fn shutdown_archiver(_db: &Arc<Database>) -> Result<(), QueryError> {
    Ok(())
}

#[cfg(test)]
fn create_backup(
    db: &Arc<Database>,
    request: &BackupRequest,
    id: &str,
    cancel: &AtomicBool,
    mut progress: impl FnMut(&str, u64, u64),
) -> Result<BackupManifest, QueryError> {
    capture_database(
        db,
        request,
        id,
        cancel,
        |phase, bytes, files, _| progress(phase, bytes, files),
        |_, _, _| Ok(()),
    )
}

fn write_all_at(file: &File, mut bytes: &[u8], mut offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    use std::os::unix::fs::FileExt;
    while !bytes.is_empty() {
        let written = file.write_at(bytes, offset)?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "image write made no progress",
            ));
        }
        bytes = &bytes[written..];
        offset += written as u64;
    }
    Ok(())
}

/// A direct reader holds the repository GC lease for the entire server lifetime.
pub struct BaseReader {
    pub data_dir: PathBuf,
    _lease: repository::Lease,
}
pub fn open_base(
    repository: &Path,
    base_id: &str,
    runtime: &Path,
) -> Result<BaseReader, QueryError> {
    let repo = repository::Repository::open(repository, None)?;
    let lease = repository::Lease::acquire(&repo.root, false)?;
    reject_overlap(&repo.root, runtime)?;
    let position = repo.position()?;
    let mut found = false;
    repo.visit_live(&position, |_, event| {
        if let repository::Publication::Base { base } = event {
            if base.id == base_id {
                found = true;
            }
        }
        Ok(())
    })?;
    if !found {
        return Err(backup_err("base is not published in the pinned catalog"));
    }
    repo.base_for_open(base_id)?;
    Ok(BaseReader {
        data_dir: repo.root.join("bases").join(base_id).join("data"),
        _lease: lease,
    })
}

fn wal_pin_target(root: &Path) -> Result<PathBuf, QueryError> {
    let absolute = if root.is_absolute() {
        root.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| io_err("resolve WAL pin source", e))?
            .join(root)
    };
    let absolute = resolved_destination(&absolute)?;
    let parent = absolute
        .parent()
        .ok_or_else(|| backup_err("source has no generation-pin parent"))?;
    Ok(parent.join(format!(
        ".mydb-wal-generation-{}",
        sha256_hex(absolute.as_os_str().to_string_lossy().as_bytes())
    )))
}

fn micros(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}
