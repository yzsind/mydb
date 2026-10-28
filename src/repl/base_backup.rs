//! Streaming base backup helpers for the mydb-native physical
//! replication path (Item 3).
//!
//! The master side ([`stream_base_backup`]) is invoked from the
//! per-replica WAL sender when the incoming handshake asks for a base
//! backup — either explicitly via the `request_base_backup` flag, or
//! implicitly because the replica's `requested_start_lsn` falls below
//! the master's retention floor. The function:
//!
//! 1. Forces a checkpoint so every dirty buffer-pool page is flushed
//!    to its `tables/<id>.heap` / `indexes/<id>.idx` file. The
//!    checkpoint's begin LSN becomes the "backup LSN": any WAL record
//!    with `lsn > backup_lsn` is replayed *on top* of the snapshot on
//!    the replica.
//! 2. Snapshots the file list under `<data-dir>/{tables,indexes}`,
//!    PLUS `replication/role.json` (so the replica preserves its
//!    chosen identity). The WAL segments and slot files are deliberately
//!    *not* included — the replica owns its own WAL and slot state.
//! 3. Streams `BaseBackupStart` (the manifest), then each file's
//!    bytes in 1 MB `BaseBackupChunk` messages, then `BaseBackupEnd`.
//!
//! The replica side ([`perform_base_backup_into`]) drives the inverse:
//! connect to a master, perform a Handshake with `request_base_backup
//! = true`, receive the manifest + chunks, write them under the local
//! data dir, and persist a `progress.json` whose `last_applied_lsn`
//! equals the master's `backup_lsn`. The caller then opens its
//! `Database` on that directory and (optionally) hands off to the
//! normal receiver loop for ongoing WAL streaming.

use crate::platform::file::{File, OpenOptions};
use std::fs;
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;

use crate::repl::protocol::{
    self, BaseBackupChunk, BaseBackupEnd, BaseBackupFile, BaseBackupStart, Handshake, HandshakeAck,
    Message, PROTOCOL_VERSION,
};
use crate::repl::replica::progress::{save_progress, ReplicaProgress};
use crate::repl::role::uuid_string_to_bytes;
use crate::storage::{error, Database};
use crate::types::QueryError;
use sha2::{Digest, Sha256};

const CHUNK_BYTES: usize = 1024 * 1024;

/// Subdirectories of `<data-dir>` whose contents go into a base
/// backup. With the v33 layout all user/dictionary heaps and indexes
/// live under `schemas/<db>/{tables,indexes}/`, so a recursive sweep
/// of `schemas/` captures every object file. `system/replication/role.json`
/// is special-cased below: only `role.json` is included from the
/// replication state.
const BACKUP_DIRS: &[&str] = &["schemas", "objects", "system/catalog", "system/undo"];
/// Authoritative sidecars that are not stored under `schemas/`. They are
/// append/checkpoint managed and must be seeded with the heap snapshot; WAL
/// only contains changes after the backup boundary.
const BACKUP_FILES: &[&str] = &[
    "system/commit-ledger.v1",
    "system/history-index.v1",
    "system/vector-purges.v1",
    "system/vector-maintenance.jobs",
    "system/commit-clock.v1",
];

/// Result of [`stream_base_backup`]: the LSN at which the master
/// captured the on-disk snapshot. WAL streaming resumes from
/// `backup_lsn + 1`.
#[derive(Debug, Clone, Copy)]
pub struct BaseBackupSnapshot {
    pub backup_lsn: u64,
}

struct BackupFreezeGuard<'a> {
    manager: Option<std::sync::Arc<crate::repl::MydbReplManager>>,
    capture_active: std::sync::Arc<std::sync::atomic::AtomicBool>,
    capture_floor: &'a std::sync::atomic::AtomicU64,
}

impl Drop for BackupFreezeGuard<'_> {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.take() {
            manager.backup_capture_frozen.store(false, Ordering::SeqCst);
        }
        self.capture_floor.store(0, Ordering::Release);
        self.capture_active.store(false, Ordering::Release);
    }
}

/// Private, stable copies of mutable page files. Keep these until transfer
/// completes; open source handles alone do not isolate in-place page writes.
#[derive(Default)]
pub(crate) struct BackupSnapshotFiles(Vec<PathBuf>);

impl Drop for BackupSnapshotFiles {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = fs::remove_file(path);
        }
    }
}

impl BackupSnapshotFiles {
    pub(crate) fn last_path(&self) -> Option<&Path> {
        self.0.last().map(PathBuf::as_path)
    }
    pub(crate) fn capture(
        &mut self,
        root: &Path,
        source: &mut File,
        len: u64,
    ) -> Result<File, QueryError> {
        self.capture_checked(root, source, len, || Ok(()))
    }
    pub(crate) fn capture_checked(
        &mut self,
        root: &Path,
        source: &mut File,
        len: u64,
        mut check: impl FnMut() -> Result<(), QueryError>,
    ) -> Result<File, QueryError> {
        let dir = root.join("system/replication");
        fs::create_dir_all(&dir).map_err(io_err)?;
        let path = dir.join(format!(
            ".base-backup-{}.snapshot",
            crate::repl::role::generate_uuid_string()
        ));
        let mut copy = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(io_err)?;
        self.0.push(path);
        let _memory=crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(crate::engine::executor::mem_budget::ControlledMemoryCategory::NativeReplicationTransient,64*1024)?;
        let mut buffer = vec![0; 64 * 1024];
        let mut remaining = len;
        while remaining > 0 {
            check()?;
            let take = remaining.min(buffer.len() as u64) as usize;
            source.read_exact(&mut buffer[..take]).map_err(io_err)?;
            copy.write_all(&buffer[..take]).map_err(io_err)?;
            remaining -= take as u64;
        }
        check()?;
        copy.seek(SeekFrom::Start(0)).map_err(io_err)?;
        Ok(copy)
    }
}

fn is_paged_zone_map(path: &Path) -> Result<bool, QueryError> {
    let mut file = File::open(path).map_err(io_err)?;
    let mut magic = [0u8; 4];
    match file.read_exact(&mut magic) {
        Ok(()) => Ok(u32::from_le_bytes(magic) == 0x5a_4d_50_31),
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(err) => Err(io_err(err)),
    }
}

// =====================================================================
// Master side
// =====================================================================

/// Build the manifest + send all base backup messages to the given
/// async writer. Returns the `backup_lsn` so the caller can resume WAL
/// streaming from `backup_lsn + 1`.
///
/// Disk I/O uses blocking `std::fs` for simplicity; the per-chunk
/// network send remains async. On modern OSes a 1 MB read is fast
/// enough that running it inline on the tokio worker thread is fine
/// — base backup is a one-shot operation per replica, not a hot path.
pub async fn stream_base_backup<W>(
    writer: &mut BufWriter<W>,
    db: &Database,
    data_dir: &Path,
) -> Result<BaseBackupSnapshot, QueryError>
where
    W: tokio::io::AsyncWriteExt + Unpin,
{
    let logical_gate = crate::storage::capture::logical_apply_gate(db)?;
    let logical_capture = logical_gate.try_write_arc().ok_or_else(|| {
        error(
            crate::error::catalog::MYDB_ER_BASE_BACKUP_FAILED,
            "base backup capture deferred: logical applier is completing a transaction",
        )
    })?;
    // Do not freeze a busy master while waiting for a long transaction.
    // The storage admission barrier also covers logical replication writers.
    if db.has_active_backup_conflicts() {
        return Err(error(
            crate::error::catalog::MYDB_ER_BASE_BACKUP_FAILED,
            "base backup capture deferred: active source writers; retry after they drain",
        ));
    }
    db.capture
        .active
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| {
            error(
                crate::error::catalog::MYDB_ER_BASE_BACKUP_FAILED,
                "another physical backup is active",
            )
        })?;
    let manager =
        crate::repl::global_manager().filter(|manager| manager.role() == crate::repl::Role::Master);
    if let Some(manager) = &manager {
        manager.backup_capture_frozen.store(true, Ordering::SeqCst);
    }
    let freeze_guard = BackupFreezeGuard {
        manager,
        capture_active: std::sync::Arc::clone(&db.capture.active),
        capture_floor: &db.capture.capture_floor,
    };
    let admission_freeze = db.capture.freeze();
    if db.has_active_backup_conflicts() || db.capture.admissions.load(Ordering::SeqCst) != 0 {
        return Err(error(
            crate::error::catalog::MYDB_ER_BASE_BACKUP_FAILED,
            "base backup capture deferred: source writer raced admission; retry",
        ));
    }
    // Capture the boundary BEFORE the fuzzy checkpoint. Replaying its tail
    // cannot skip a record published during the checkpoint flush window.
    let backup_lsn = db.wal_log().lock().next_lsn().saturating_sub(1);
    // Also protect the boundary without a process-global replication manager
    // (embedded services). A quiescent checkpoint must not collapse its tail.
    db.capture
        .capture_floor
        .store(backup_lsn.max(1), Ordering::Release);
    db.checkpoint()?;

    let mut snapshots = BackupSnapshotFiles::default();
    let opened = {
        // SQL write admission does not cover background purge/compaction.
        // Drain those physical mutations before flushing and copying files.
        let _maintenance = db.backup_maintenance_capture_guard()?;
        // Keep the LSN boundary above this guard: a rebuild completed after
        // checkpoint is replayed from its FPI records on top of this baseline.
        // Do not hold a parking_lot guard over asynchronous network transfer.
        let _zone_capture = db.zone_map_backup_capture_guard()?;
        // 2. Snapshot the file list. We sort by path so the protocol is
        //    deterministic (helps debugging — the manifest order matches
        //    chunk order).
        let mut files: Vec<(PathBuf, BaseBackupFile)> = Vec::new();
        for sub in BACKUP_DIRS {
            let dir = data_dir.join(sub);
            collect_files(&dir, sub, &mut files)?;
        }
        for relative in BACKUP_FILES {
            let path = data_dir.join(relative);
            let Ok(meta) = fs::metadata(&path) else {
                continue;
            };
            if meta.is_file() {
                files.push((
                    path,
                    BaseBackupFile {
                        path: (*relative).to_string(),
                        len: meta.len(),
                    },
                ));
            }
        }
        // Deliberately NOT included: the master's role.json. Shipping it
        // would overwrite the replica's identity with the MASTER's
        // node_uuid and role=master — the replica keeps its own role file.
        files.sort_by(|a, b| a.1.path.cmp(&b.1.path));
        let mut opened = Vec::<(PathBuf, BaseBackupFile)>::with_capacity(files.len());
        for (path, mut entry) in files {
            let mut file = File::open(&path).map_err(|err| {
                error(
                    crate::error::catalog::MYDB_ER_BASE_BACKUP_FAILED,
                    format!("base backup open {}: {err}", path.display()),
                )
            })?;
            // Windows directory enumeration can retain a stale length while
            // a writer remains open. Bind the manifest length to the opened
            // file identity, using its current EOF while writes are fenced.
            entry.len = file.metadata().map_err(io_err)?.len();
            // Open handles do not freeze mutable heap/undo/catalog pages.
            // Copy while source writes are fenced; release admission before
            // network transfer so a slow replica cannot stall source DML.
            file = snapshots.capture(data_dir, &mut file, entry.len)?;
            drop(file);
            opened.push((snapshots.last_path().unwrap().to_path_buf(), entry));
        }
        opened
    };
    drop(admission_freeze);
    drop(freeze_guard);
    drop(logical_capture);

    let manifest: Vec<BaseBackupFile> = opened.iter().map(|(_, m)| m.clone()).collect();
    let start = Message::BaseBackupStart(BaseBackupStart {
        backup_lsn,
        files: manifest,
    });
    protocol::write_frame_async(writer, &start)
        .await
        .map_err(io_err)?;

    // 3. Stream each file in CHUNK_BYTES-sized chunks. Each file
    //    finishes with an `eof: true` chunk so the replica can close
    //    out its output handle deterministically (even for empty
    //    files, which would otherwise have no chunks at all).
    let mut read_buf = vec![0u8; CHUNK_BYTES];
    for (path, entry) in opened {
        let mut file = tokio::fs::File::open(path).await.map_err(io_err)?;
        let mut offset: u64 = 0;
        let mut sha256 = Sha256::new();
        loop {
            let remaining = entry.len.saturating_sub(offset) as usize;
            let wanted = remaining.min(read_buf.len());
            let read = file.read(&mut read_buf[..wanted]).await.map_err(|err| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("base backup read: {err}"),
                )
            })?;
            let next_offset = offset.saturating_add(read as u64);
            if read == 0 && offset < entry.len {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "base backup source file {} shrank at offset {} (manifest length {})",
                        entry.path, offset, entry.len
                    ),
                ));
            }
            let eof = next_offset >= entry.len;
            sha256.update(&read_buf[..read]);
            let checksum = if eof {
                sha256.clone().finalize().into()
            } else {
                [0u8; 32]
            };
            let chunk = Message::BaseBackupChunk(BaseBackupChunk {
                path: entry.path.clone(),
                offset,
                bytes: read_buf[..read].to_vec(),
                eof,
                sha256: checksum,
            });
            protocol::write_frame_async(writer, &chunk)
                .await
                .map_err(io_err)?;
            offset = next_offset;
            if eof {
                break;
            }
        }
    }

    let end = Message::BaseBackupEnd(BaseBackupEnd { backup_lsn });
    protocol::write_frame_async(writer, &end)
        .await
        .map_err(io_err)?;
    writer.flush().await.map_err(io_err)?;
    Ok(BaseBackupSnapshot { backup_lsn })
}

/// Recursively walk `dir`, emitting one manifest entry per regular file.
/// The recorded `path` is `<relative_prefix>/<rel_path_under_dir>` using
/// `/` separators so the protocol stays platform-neutral.
fn collect_files(
    dir: &Path,
    relative_prefix: &str,
    out: &mut Vec<(PathBuf, BaseBackupFile)>,
) -> Result<(), QueryError> {
    if !dir.exists() {
        return Ok(());
    }
    let entries = fs::read_dir(dir).map_err(|err| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("base backup read_dir {}: {err}", dir.display()),
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("base backup dir entry: {err}"),
            )
        })?;
        let path = entry.path();
        let ft = entry.file_type().map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("base backup file_type: {err}"),
            )
        })?;
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let next_prefix = format!("{relative_prefix}/{name}");
        if ft.is_dir() {
            collect_files(&path, &next_prefix, out)?;
            continue;
        }
        if !ft.is_file() {
            continue;
        }
        // B-tree/vector indexes and legacy zone-map files are rebuilt locally.
        // Paged Zone Maps use physical redo, so their baselines
        // must travel with the heap; incremental WAL is not a full image.
        // DD B-trees are different: v46 uses them to assemble lazy
        // descriptors before user-index validation can run, so the sharp,
        // writer-frozen checkpoint must include mysql dictionary indexes.
        if path.extension().and_then(|ext| ext.to_str()) == Some("zm") && !is_paged_zone_map(&path)?
        {
            continue;
        }
        let meta = entry.metadata().map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("base backup meta: {err}"),
            )
        })?;
        out.push((
            path,
            BaseBackupFile {
                path: next_prefix,
                len: meta.len(),
            },
        ));
    }
    Ok(())
}

// =====================================================================
// Replica side
// =====================================================================

/// Connect to a master and pull a base backup into `data_dir`. The
/// directory's `tables/`, `indexes/`, `replication/role.json`, plus
/// `redo/` and `undo/`, are wiped before the new contents land, so
/// callers must pass either an empty path or one whose contents are
/// safe to drop.
///
/// The local `replica_uuid` is sent in the handshake (so the master
/// can reuse an existing slot for this replica). On success the
/// replica's `progress.json` is created with
/// `last_applied_lsn = backup_lsn`, so the subsequent WAL streaming
/// path resumes at `backup_lsn + 1`. Returns the backup LSN.
async fn read_backup_frame<R: tokio::io::AsyncReadExt + Unpin>(
    reader: &mut R,
) -> std::io::Result<Message> {
    read_backup_frame_with_timeout(reader, Duration::from_secs(60)).await
}

async fn read_backup_frame_with_timeout<R: tokio::io::AsyncReadExt + Unpin>(
    reader: &mut R,
    timeout: Duration,
) -> std::io::Result<Message> {
    tokio::time::timeout(timeout, protocol::read_frame_async(reader))
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "base backup frame read timed out",
            )
        })?
}

/// Remove only private transfer snapshots, after the data-directory lock is held.
pub(crate) fn cleanup_abandoned_snapshots(data_dir: &Path) -> Result<(), QueryError> {
    let dir = data_dir.join("system/replication");
    match fs::read_dir(&dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry.map_err(io_err)?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with(".base-backup-")
                    && name.ends_with(".snapshot")
                    && entry.file_type().map_err(io_err)?.is_file()
                {
                    fs::remove_file(entry.path()).map_err(io_err)?;
                }
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(io_err(err)),
    }
}

/// A failed first open can leave directories, empty DD heaps and a WAL header.
/// Those scaffolds are not a seeded database. Never infer emptiness when any
/// progress, catalog data, WAL records or user object files already exist.
pub(crate) fn replica_needs_initial_seed(data_dir: &Path) -> Result<bool, QueryError> {
    if crate::repl::replica::progress::progress_path(data_dir)
        .try_exists()
        .map_err(io_err)?
    {
        return Ok(false);
    }
    for table in crate::storage::data_dictionary::dd_tables() {
        let path =
            crate::storage::naming::table_object_generation_path(data_dir, table.id, 1, "heap");
        match fs::metadata(path) {
            Ok(metadata) if metadata.len() != 0 => return Ok(false),
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_err(err)),
        }
    }
    let wal_dir = crate::storage::naming::system_wal_dir(data_dir);
    match fs::read_dir(wal_dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry.map_err(io_err)?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("wal-")
                    && !name.ends_with(".tmp")
                    && entry.metadata().map_err(io_err)?.len() > crate::wal::WAL_HEADER_LEN as u64
                {
                    return Ok(false);
                }
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(io_err(err)),
    }
    for root in ["schemas", "objects"] {
        if contains_object_files(&data_dir.join(root))? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn contains_object_files(root: &Path) -> Result<bool, QueryError> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(io_err(err)),
    };
    for entry in entries {
        let entry = entry.map_err(io_err)?;
        if !entry.file_type().map_err(io_err)?.is_dir() || contains_object_files(&entry.path())? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn perform_base_backup_into(
    master_host: &str,
    master_port: u16,
    replica_uuid: &str,
    data_dir: &Path,
) -> Result<u64, QueryError> {
    perform_base_backup_into_with_config(
        master_host,
        master_port,
        replica_uuid,
        data_dir,
        crate::repl::config::ReplConfig::from_env(),
    )
    .await
}

pub(crate) async fn perform_base_backup_into_with_config(
    master_host: &str,
    master_port: u16,
    replica_uuid: &str,
    data_dir: &Path,
    config: crate::repl::config::ReplConfig,
) -> Result<u64, QueryError> {
    config.auth_token.validate()?;
    let _memory = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::NativeReplicationTransient,
        CHUNK_BYTES.saturating_mul(2),
    )?;
    recover_interrupted_install(data_dir)?;
    let master_host = master_host.trim_matches(['[', ']']);
    let addr = crate::repl::format_endpoint(master_host, master_port);
    let stream = tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(&addr))
        .await
        .map_err(|_| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("base backup connect {addr} timed out"),
            )
        })?
        .map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("base backup connect {addr}: {err}"),
            )
        })?;
    // Disable Nagle for the base-backup control frames (Pf2).
    if let Err(e) = stream.set_nodelay(true) {
        crate::util::logging::log_message(format_args!(
            "mydb-repl replica: base backup set_nodelay failed: {e}"
        ));
    }
    let stream = crate::repl::tls::connect(stream, &config, master_host)
        .await
        .map_err(io_err)?;
    let (rh, wh) = tokio::io::split(stream);
    let mut reader = BufReader::new(rh);
    let mut writer = BufWriter::new(wh);

    let replica_uuid_bytes = uuid_string_to_bytes(replica_uuid).ok_or_else(|| {
        error(
            crate::error::catalog::ER_WRONG_VALUE,
            "base backup replica UUID is invalid",
        )
    })?;
    let handshake = Handshake {
        auth_token: config.auth_token.clone(),
        protocol_version: PROTOCOL_VERSION,
        replica_uuid: replica_uuid_bytes,
        // 0 = ask for "from the very beginning", which the master
        // will treat as "you need a base backup first".
        requested_start_lsn: 0,
        last_known_master_uuid: [0; 16],
        last_known_epoch: 0,
        cluster_uuid: [0; 16],
        last_known_timeline_uuid: [0; 16],
        client_version: format!("mydb-base-backup/{}", env!("CARGO_PKG_VERSION")),
        request_base_backup: true,
        max_chunk_bytes: config.chunk_bytes.min(u32::MAX as usize) as u32,
        format_compatibility: crate::storage::format_versions::native_replication_compatibility(),
    };
    protocol::write_frame_async(&mut writer, &Message::Handshake(handshake))
        .await
        .map_err(io_err)?;
    writer.flush().await.map_err(io_err)?;

    let ack: HandshakeAck = match read_backup_frame(&mut reader).await {
        Ok(Message::HandshakeAck(a)) => a,
        Ok(other) => {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!(
                    "base backup: expected HandshakeAck, got {:?}",
                    other.msg_type()
                ),
            ))
        }
        Err(err) => return Err(io_err(err)),
    };
    if !ack.accepted {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("base backup handshake rejected: {}", ack.reason),
        ));
    }
    let local_format = crate::storage::format_versions::native_replication_compatibility();
    if ack.format_compatibility != local_format {
        return Err(error(
            crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
            format!(
                "base backup format mismatch: master='{}', replica='{}'",
                ack.format_compatibility, local_format
            ),
        ));
    }

    // After the handshake the master sends a `BaseBackupStart` because
    // we set `request_base_backup = true`. Anything else is a
    // protocol violation.
    let start: BaseBackupStart = match read_backup_frame(&mut reader).await {
        Ok(Message::BaseBackupStart(s)) => s,
        Ok(other) => {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!(
                    "base backup: expected BaseBackupStart, got {:?}",
                    other.msg_type()
                ),
            ))
        }
        Err(err) => return Err(io_err(err)),
    };
    // Retain admission for the manifest and its verification maps throughout
    // installation, rather than dropping all accounting after frame decode.
    let manifest_bytes = start
        .files
        .iter()
        .fold(start.files.len().saturating_mul(512), |bytes, file| {
            bytes.saturating_add(file.path.capacity().saturating_mul(3))
        });
    let _manifest_memory = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::NativeReplicationTransient,
        manifest_bytes,
    )?;
    let total_bytes = start.files.iter().try_fold(0u64, |sum, file| {
        sum.checked_add(file.len).ok_or_else(|| {
            error(
                crate::error::catalog::MYDB_ER_BASE_BACKUP_FAILED,
                "base backup manifest total length overflow",
            )
        })
    })?;
    if let Some(available) = available_space_bytes(data_dir) {
        let usable = available.saturating_mul(u64::from(
            100u8.saturating_sub(config.base_backup_disk_reserve_pct),
        )) / 100;
        if total_bytes > usable {
            return Err(error(
                crate::error::catalog::MYDB_ER_BASE_BACKUP_FAILED,
                format!(
                    "base backup requires {total_bytes} bytes but only {usable} bytes are available after {}% reserve",
                    config.base_backup_disk_reserve_pct
                ),
            ));
        }
    }
    let staging = backup_staging_path(data_dir, replica_uuid)?;
    if staging.exists() {
        remove_path(&staging).map_err(io_err)?;
    }
    fs::create_dir_all(&staging).map_err(io_err)?;
    let mut expected = std::collections::BTreeMap::<String, (u64, u64, bool)>::new();
    let mut checksums = std::collections::HashMap::<String, Sha256>::new();
    for file in &start.files {
        validate_backup_relative_path(&file.path)?;
        if expected
            .insert(file.path.clone(), (file.len, 0, false))
            .is_some()
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("base backup manifest contains duplicate path {}", file.path),
            ));
        }
        checksums.insert(file.path.clone(), Sha256::new());
    }
    // Recreate the layout's top-level directories. Per-database
    // schemas/<db>/ subdirs are recreated lazily as chunks arrive (the
    // chunk-write path also mkdir_p's the file's parent).
    for dir in [
        crate::storage::naming::schemas_root(&staging),
        crate::storage::naming::system_undo_dir(&staging),
    ] {
        fs::create_dir_all(&dir).map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("mkdir {}: {err}", dir.display()),
            )
        })?;
    }

    // Track open file handles by manifest entry so we don't reopen
    // for every chunk.
    let mut open_files: std::collections::HashMap<String, File> = std::collections::HashMap::new();
    let backup_lsn;
    loop {
        match read_backup_frame(&mut reader).await {
            Ok(Message::BaseBackupChunk(chunk)) => {
                validate_backup_relative_path(&chunk.path)?;
                let Some((declared_len, next_offset, saw_eof)) = expected.get_mut(&chunk.path)
                else {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("base backup chunk for undeclared path {}", chunk.path),
                    ));
                };
                if *saw_eof || chunk.offset != *next_offset {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!(
                            "base backup chunk offset mismatch for {}: got {}, expected {}",
                            chunk.path, chunk.offset, *next_offset
                        ),
                    ));
                }
                let end = chunk
                    .offset
                    .checked_add(chunk.bytes.len() as u64)
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "backup offset overflow",
                        )
                    })?;
                if end > *declared_len || chunk.eof != (end == *declared_len) {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!(
                            "base backup length/eof mismatch for {}: end {}, declared {}, eof {}",
                            chunk.path, end, *declared_len, chunk.eof
                        ),
                    ));
                }
                *next_offset = end;
                *saw_eof = chunk.eof;
                let hasher = checksums
                    .get_mut(&chunk.path)
                    .expect("manifest checksum state");
                hasher.update(&chunk.bytes);
                if chunk.eof {
                    let actual: [u8; 32] = hasher.clone().finalize().into();
                    if actual != chunk.sha256 {
                        return Err(error(
                            crate::error::catalog::MYDB_ER_BASE_BACKUP_FAILED,
                            format!("base backup SHA-256 mismatch for {}", chunk.path),
                        ));
                    }
                } else if chunk.sha256 != [0u8; 32] {
                    return Err(error(
                        crate::error::catalog::MYDB_ER_BASE_BACKUP_FAILED,
                        format!("non-EOF backup chunk carried checksum for {}", chunk.path),
                    ));
                }
                let abs = staging.join(&chunk.path);
                if let Some(parent) = abs.parent() {
                    fs::create_dir_all(parent).map_err(|err| {
                        error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            format!("mkdir {}: {err}", parent.display()),
                        )
                    })?;
                }
                let entry = match open_files.get_mut(&chunk.path) {
                    Some(f) => f,
                    None => {
                        let f = OpenOptions::new()
                            .create(true)
                            .write(true)
                            .truncate(true)
                            .open(&abs)
                            .map_err(|err| {
                                error(
                                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                                    format!("create {}: {err}", abs.display()),
                                )
                            })?;
                        open_files.insert(chunk.path.clone(), f);
                        open_files.get_mut(&chunk.path).unwrap()
                    }
                };
                entry.seek(SeekFrom::Start(chunk.offset)).map_err(|err| {
                    error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        format!("seek: {err}"),
                    )
                })?;
                entry.write_all(&chunk.bytes).map_err(|err| {
                    error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        format!("write chunk: {err}"),
                    )
                })?;
                if chunk.eof {
                    entry.sync_all().map_err(|err| {
                        error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            format!("flush chunk: {err}"),
                        )
                    })?;
                    open_files.remove(&chunk.path);
                }
            }
            Ok(Message::BaseBackupEnd(end)) => {
                if end.backup_lsn != start.backup_lsn {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "base backup start/end LSN mismatch",
                    ));
                }
                backup_lsn = end.backup_lsn;
                break;
            }
            Ok(other) => {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!(
                        "base backup: expected Chunk or End, got {:?}",
                        other.msg_type()
                    ),
                ));
            }
            Err(err) => return Err(io_err(err)),
        }
    }
    if !open_files.is_empty() || expected.values().any(|(_, _, eof)| !*eof) {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "base backup ended before every manifest file reached EOF",
        ));
    }

    // Sanity check: every file in the manifest must now exist with
    // the expected length.
    for f in &start.files {
        let abs = staging.join(&f.path);
        let meta = fs::metadata(&abs).map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("base backup verify {}: {err}", abs.display()),
            )
        })?;
        if meta.len() != f.len {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!(
                    "base backup verify {}: got {} bytes, expected {}",
                    abs.display(),
                    meta.len(),
                    f.len
                ),
            ));
        }
    }

    // Persist progress.last_applied_lsn = backup_lsn so the steady
    // state receiver resumes at backup_lsn + 1.
    let master_uuid = crate::repl::role::uuid_bytes_to_string(&ack.master_uuid);
    let progress = ReplicaProgress {
        received_lsn: backup_lsn,
        durable_lsn: backup_lsn,
        applied_lsn: backup_lsn,
        durable_applied_lsn: backup_lsn,
        last_master_uuid: master_uuid,
        last_master_epoch: ack.epoch,
        cluster_uuid: crate::repl::role::uuid_bytes_to_string(&ack.cluster_uuid),
        timeline_uuid: crate::repl::role::uuid_bytes_to_string(&ack.timeline_uuid),
        source_current_lsn: ack.current_lsn,
        source_durable_lsn: ack.durable_lsn,
        source_generation: ack.generation,
        source_heartbeat_ms: ack.heartbeat_ms,
        source_frozen: ack.source_frozen,
        source_fence_lsn: ack.fence_lsn,
        source_fence_token_hash: ack.fence_token_hash,
        last_update_ms: 0,
    };
    save_progress(&staging, &progress).map_err(|err| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("save progress: {err}"),
        )
    })?;
    if let Some(mut role_state) = crate::repl::role::load(data_dir).map_err(io_err)? {
        role_state.cluster_uuid.clone_from(&progress.cluster_uuid);
        role_state.timeline_uuid.clone_from(&progress.timeline_uuid);
        role_state.parent_timeline_uuid = None;
        role_state.fork_lsn = backup_lsn;
        role_state.generation = role_state.generation.max(1);
        crate::repl::role::save(&staging, &role_state).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("save replica cluster/timeline metadata: {err}"),
            )
        })?;
    }
    sync_tree(&staging)?;
    install_staged_backup(data_dir, &staging)?;
    if staging.exists() {
        remove_path(&staging).map_err(io_err)?;
    }
    Ok(backup_lsn)
}

/// Best-effort wipe of the directories the base backup will rewrite.
/// Leaves the rest of `data_dir` untouched (e.g. the replica's own
/// slot registry) — recovery on the next `Database::open` reconciles
/// the freshly-installed pages with the empty WAL.
const INSTALL_JOURNAL: &str = "system/replication/base_backup_install.json";
const PREVIOUS_ROOT: &str = ".native-repl-previous";
const INSTALL_TARGETS: &[&str] = &[
    "schemas",
    "objects",
    "system/catalog",
    "system/undo",
    "system/wal",
    "system/commit-ledger.v1",
    "system/commit-clock.v1",
    "system/history-index.v1",
    "system/vector-purges.v1",
    "system/vector-maintenance.jobs",
    "ddl",
    "system/replication/replica_progress.json",
    "system/replication/role.json",
];

fn backup_staging_path(data_dir: &Path, replica_uuid: &str) -> Result<PathBuf, QueryError> {
    let parent = data_dir.parent().ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "base backup data directory has no parent",
        )
    })?;
    let name = data_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "base backup data directory name is not valid UTF-8",
            )
        })?;
    Ok(parent.join(format!(".{name}.native-repl-stage-{replica_uuid}")))
}

#[cfg(unix)]
fn available_space_bytes(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `c_path` is NUL-terminated and `stat` points to writable storage
    // for exactly one statvfs result. The value is read only on success.
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let stat = unsafe { stat.assume_init() };
    Some((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

#[cfg(windows)]
fn available_space_bytes(path: &Path) -> Option<u64> {
    crate::platform::fs::available_space(path)
}

#[cfg(not(any(unix, windows)))]
fn available_space_bytes(_path: &Path) -> Option<u64> {
    None
}

fn validate_backup_relative_path(relative: &str) -> Result<(), QueryError> {
    use std::path::Component;
    let path = Path::new(relative);
    if relative.is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("unsafe base backup path '{relative}'"),
        ));
    }
    let allowed = relative.starts_with("schemas/")
        || relative.starts_with("objects/")
        || relative.starts_with("system/catalog/")
        || relative.starts_with("system/undo/")
        || BACKUP_FILES.contains(&relative);
    if !allowed {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("base backup path '{relative}' is outside authoritative targets"),
        ));
    }
    Ok(())
}

fn remove_path(path: &Path) -> std::io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

fn sync_tree(root: &Path) -> Result<(), QueryError> {
    crate::util::file::visit_tree_postorder(
        root,
        "symlink in base backup staging tree",
        &mut |path, _| crate::platform::fs::sync_path(path),
    )
    .map_err(io_err)
}

fn journal_path(data_dir: &Path) -> PathBuf {
    data_dir.join(INSTALL_JOURNAL)
}

pub(crate) fn recover_interrupted_install(data_dir: &Path) -> Result<(), QueryError> {
    let journal = journal_path(data_dir);
    if !journal.exists() {
        return Ok(());
    }
    let journal_value: serde_json::Value =
        serde_json::from_slice(&fs::read(&journal).map_err(io_err)?).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("invalid base backup install journal: {err}"),
            )
        })?;
    let staging = journal_value
        .get("staging")
        .and_then(|value| value.as_str())
        .map(PathBuf::from)
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "base backup install journal is missing staging path",
            )
        })?;
    let previous_root = data_dir.join(PREVIOUS_ROOT);
    if journal_value.get("state").and_then(|v| v.as_str()) == Some("completed") {
        // The commit record is durable before any old subtree is deleted.
        // A crash during cleanup must roll forward, never mix old/new trees.
        remove_path(&previous_root).map_err(io_err)?;
        fs::remove_file(&journal).map_err(io_err)?;
        sync_parent(&journal)?;
        return Ok(());
    }
    let originals = journal_value
        .get("original_targets")
        .and_then(|v| v.as_array());
    for relative in INSTALL_TARGETS.iter().rev() {
        let target = data_dir.join(relative);
        let previous = previous_root.join(relative);
        let staged = staging.join(relative);
        if previous.exists() {
            remove_path(&target).map_err(io_err)?;
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(io_err)?;
            }
            fs::rename(&previous, &target).map_err(io_err)?;
            sync_parent(&target)?;
            sync_parent(&previous)?;
        } else if !staged.exists()
            && originals
                .is_some_and(|targets| !targets.iter().any(|v| v.as_str() == Some(*relative)))
        {
            // No previous target and the staged object disappeared: it was a
            // fresh install completed before the crash. Remove it so retrying
            // starts from the old (empty) state rather than a partial backup.
            remove_path(&target).map_err(io_err)?;
            sync_parent(&target)?;
        }
    }
    remove_path(&previous_root).map_err(io_err)?;
    fs::remove_file(&journal).map_err(io_err)?;
    sync_parent(&journal)?;
    Ok(())
}

fn install_staged_backup(data_dir: &Path, staging: &Path) -> Result<(), QueryError> {
    let journal = journal_path(data_dir);
    if let Some(parent) = journal.parent() {
        fs::create_dir_all(parent).map_err(io_err)?;
    }
    let previous_root = data_dir.join(PREVIOUS_ROOT);
    remove_path(&previous_root).map_err(io_err)?;
    fs::create_dir_all(&previous_root).map_err(io_err)?;
    sync_parent(&previous_root)?;
    let mut journal_value = serde_json::json!({
        "state": "installing",
        "staging": staging.to_string_lossy(),
        "original_targets": INSTALL_TARGETS.iter().filter(|relative| data_dir.join(relative).exists()).collect::<Vec<_>>(),
    });
    let journal_bytes = serde_json::to_vec_pretty(&journal_value).map_err(|err| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("serialize base backup install journal: {err}"),
        )
    })?;
    crate::repl::durable::atomic_write_sync(
        &journal,
        &journal.with_extension("json.tmp"),
        &journal_bytes,
    )
    .map_err(io_err)?;

    let install_result = (|| -> Result<(), QueryError> {
        for relative in INSTALL_TARGETS {
            let target = data_dir.join(relative);
            let staged = staging.join(relative);
            let previous = previous_root.join(relative);
            if target.exists() {
                if let Some(parent) = previous.parent() {
                    fs::create_dir_all(parent).map_err(io_err)?;
                    // Persist every newly-created ancestor before moving the
                    // only old copy into it (including system/replication).
                    let mut ancestor = Some(parent);
                    while let Some(dir) = ancestor {
                        crate::platform::fs::sync_path(dir).map_err(io_err)?;
                        if dir == previous_root {
                            break;
                        }
                        ancestor = dir.parent();
                    }
                }
                fs::rename(&target, &previous).map_err(io_err)?;
                sync_parent(&previous)?;
                sync_parent(&target)?;
                sync_parent(previous.parent().expect("previous parent"))?;
            }
            if staged.exists() {
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent).map_err(io_err)?;
                }
                fs::rename(&staged, &target).map_err(io_err)?;
                sync_parent(&target)?;
                sync_parent(&staged)?;
            }
        }
        Ok(())
    })();
    if let Err(err) = install_result {
        recover_interrupted_install(data_dir)?;
        return Err(err);
    }

    journal_value["state"] = serde_json::Value::String("completed".into());
    crate::repl::durable::atomic_write_sync(
        &journal,
        &journal.with_extension("json.tmp"),
        &serde_json::to_vec(&journal_value)
            .map_err(|err| error(crate::error::catalog::ER_GET_ERRNO, err.to_string()))?,
    )
    .map_err(io_err)?;
    remove_path(&previous_root).map_err(io_err)?;
    fs::remove_file(&journal).map_err(io_err)?;
    if let Some(parent) = journal.parent() {
        crate::platform::fs::sync_path(parent).map_err(io_err)?;
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<(), QueryError> {
    if let Some(parent) = path.parent().filter(|parent| parent.exists()) {
        crate::platform::fs::sync_path(parent).map_err(io_err)?;
    }
    Ok(())
}

fn io_err(err: std::io::Error) -> QueryError {
    let detail = err.to_string();
    if let Some(security) = crate::repl::transport_security_error(&detail) {
        return security;
    }
    // Preserve structured storage diagnostics, including the dedicated
    // disk-full classification used by backup orchestration.
    crate::util::io_error::map_io_error_with_default(
        "base backup io",
        err,
        crate::error::catalog::ER_GET_ERRNO,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_completed_install_preserves_new_tree_after_partial_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let staging = root.path().join("stage");
        fs::create_dir_all(data.join("schemas/test")).unwrap();
        fs::write(data.join("schemas/test/value"), b"new").unwrap();
        fs::create_dir_all(data.join(PREVIOUS_ROOT).join("system/undo")).unwrap();
        let journal = journal_path(&data);
        fs::create_dir_all(journal.parent().unwrap()).unwrap();
        fs::write(
            &journal,
            serde_json::to_vec(&serde_json::json!({
                "state": "completed", "staging": staging, "original_targets": ["schemas"]
            }))
            .unwrap(),
        )
        .unwrap();
        recover_interrupted_install(&data).unwrap();
        assert_eq!(fs::read(data.join("schemas/test/value")).unwrap(), b"new");
        recover_interrupted_install(&data).unwrap();
    }

    #[test]
    fn audit_rollback_is_idempotent_after_previous_target_was_restored() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let staging = root.path().join("stage");
        fs::create_dir_all(data.join("schemas/test")).unwrap();
        fs::write(data.join("schemas/test/value"), b"old-restored").unwrap();
        fs::create_dir_all(data.join("objects")).unwrap();
        fs::write(data.join("objects/new"), b"fresh-partial-install").unwrap();
        let journal = journal_path(&data);
        fs::create_dir_all(journal.parent().unwrap()).unwrap();
        fs::write(
            &journal,
            serde_json::to_vec(&serde_json::json!({
                "state": "installing", "staging": staging, "original_targets": ["schemas"]
            }))
            .unwrap(),
        )
        .unwrap();
        recover_interrupted_install(&data).unwrap();
        assert_eq!(
            fs::read(data.join("schemas/test/value")).unwrap(),
            b"old-restored"
        );
        assert!(!data.join("objects").exists());
        recover_interrupted_install(&data).unwrap();
    }

    #[test]
    fn audit_install_commits_data_and_progress_together() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let staging = root.path().join("stage");
        for (tree, contents) in [(&data, "old"), (&staging, "new")] {
            for relative in [
                "schemas/test/value",
                "system/replication/replica_progress.json",
            ] {
                let target = tree.join(relative);
                fs::create_dir_all(target.parent().unwrap()).unwrap();
                fs::write(target, contents).unwrap();
            }
        }
        install_staged_backup(&data, &staging).unwrap();
        recover_interrupted_install(&data).unwrap();
        for relative in [
            "schemas/test/value",
            "system/replication/replica_progress.json",
        ] {
            assert_eq!(fs::read(data.join(relative)).unwrap(), b"new");
        }
        assert!(!data.join(PREVIOUS_ROOT).exists());
    }

    #[test]
    fn audit_install_recovers_every_subtree_rename_boundary() {
        // Alternate existing and fresh targets, including progress and role.
        // Crash after each old->previous / staged->target rename, then recover
        // twice. All targets must return to the same pre-install generation.
        for boundary in 0..=INSTALL_TARGETS.len() * 2 {
            let root = tempfile::tempdir().unwrap();
            let data = root.path().join("data");
            let staging = root.path().join("stage");
            let previous_root = data.join(PREVIOUS_ROOT);
            let mut originals = Vec::new();
            for (i, relative) in INSTALL_TARGETS.iter().enumerate() {
                let staged = staging.join(relative);
                fs::create_dir_all(staged.parent().unwrap()).unwrap();
                fs::write(staged, b"new").unwrap();
                if i % 2 == 0 {
                    originals.push(*relative);
                    let target = data.join(relative);
                    fs::create_dir_all(target.parent().unwrap()).unwrap();
                    fs::write(target, b"old").unwrap();
                }
            }
            let journal = journal_path(&data);
            fs::create_dir_all(journal.parent().unwrap()).unwrap();
            fs::write(
                &journal,
                serde_json::to_vec(&serde_json::json!({
                    "state": "installing", "staging": staging, "original_targets": originals
                }))
                .unwrap(),
            )
            .unwrap();
            for (i, relative) in INSTALL_TARGETS.iter().enumerate() {
                let target = data.join(relative);
                let previous = previous_root.join(relative);
                if i * 2 < boundary && target.exists() {
                    fs::create_dir_all(previous.parent().unwrap()).unwrap();
                    fs::rename(&target, previous).unwrap();
                }
                if i * 2 + 1 < boundary {
                    fs::create_dir_all(target.parent().unwrap()).unwrap();
                    fs::rename(staging.join(relative), target).unwrap();
                }
            }
            recover_interrupted_install(&data).unwrap();
            recover_interrupted_install(&data).unwrap();
            for (i, relative) in INSTALL_TARGETS.iter().enumerate() {
                let target = data.join(relative);
                if i % 2 == 0 {
                    assert_eq!(
                        fs::read(target).unwrap(),
                        b"old",
                        "boundary {boundary}, {relative}"
                    );
                } else {
                    assert!(!target.exists(), "boundary {boundary}, {relative}");
                }
            }
        }
    }

    #[test]
    fn zone_map_base_backup_keeps_paged_baseline_and_freezes_bytes() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let objects = dir.path().join("objects/tables/17");
        fs::create_dir_all(&objects).unwrap();
        let path = objects.join("23.zm");
        let mut bytes = vec![0; 16 * 1024];
        bytes[..4].copy_from_slice(&0x5a_4d_50_31u32.to_le_bytes());
        fs::write(&path, &bytes).unwrap();
        fs::write(objects.join("22.zm"), b"legacy sidecar").unwrap();
        let mut files = Vec::new();
        collect_files(&objects, "objects/tables/17", &mut files).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].1.path, "objects/tables/17/23.zm");
        let mut snapshots = BackupSnapshotFiles::default();
        let mut source = File::open(&path).unwrap();
        let mut snapshot = snapshots
            .capture(dir.path(), &mut source, bytes.len() as u64)
            .unwrap();
        fs::write(&path, b"replaced after capture").unwrap();
        let mut captured = Vec::new();
        snapshot.read_to_end(&mut captured).unwrap();
        assert_eq!(captured, bytes);
        let snapshot_path = snapshots.0[0].clone();
        drop(snapshot);
        drop(snapshots);
        assert!(!snapshot_path.exists());
    }

    #[test]
    fn zone_map_base_backup_rejects_short_baseline_and_cleans_snapshot() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("short.zm");
        fs::write(&path, b"short").unwrap();
        let mut source = File::open(&path).unwrap();
        let mut snapshots = BackupSnapshotFiles::default();
        assert!(snapshots
            .capture(dir.path(), &mut source, 16 * 1024)
            .is_err());
        let snapshot_path = snapshots.0[0].clone();
        drop(snapshots);
        assert!(!snapshot_path.exists());
    }

    #[test]
    fn base_backup_preserves_important_vector_and_fulltext_indexes() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("1.hvec"), b"derived native vector index").unwrap();
        std::fs::write(dir.path().join("2.idx"), b"derived btree").unwrap();
        let mut files = Vec::new();
        collect_files(dir.path(), "objects/indexes/1/1", &mut files).unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|file| file.1.path.ends_with("1.hvec")));
        assert!(files.iter().any(|file| file.1.path.ends_with("2.idx")));
    }

    #[test]
    fn tls_io_failure_maps_to_structured_replication_security_error() {
        let err = io_err(std::io::Error::other(
            "invalid peer certificate: UnknownIssuer",
        ));
        assert_eq!(
            err.code(),
            crate::error::catalog::MYDB_ER_REPLICATION_SECURITY.code()
        );
        assert!(err.message().contains("UnknownIssuer"));
    }
    use tempfile::TempDir;

    #[test]
    fn backup_paths_cannot_escape_authoritative_tree() {
        for bad in [
            "../role.json",
            "/tmp/x",
            "schemas/../system/x",
            "system/wal/x",
        ] {
            assert!(
                validate_backup_relative_path(bad).is_err(),
                "accepted {bad}"
            );
        }
        assert!(validate_backup_relative_path("schemas/test/tables/t.heap").is_ok());
        assert!(validate_backup_relative_path("system/undo/undo-0001.seg").is_ok());
    }

    #[test]
    fn interrupted_staged_install_rolls_back_previous_tree() {
        let root = TempDir::new().unwrap();
        let data = root.path().join("data");
        let staging = root.path().join("stage");
        fs::create_dir_all(data.join("schemas/test")).unwrap();
        fs::write(data.join("schemas/test/value"), b"old").unwrap();
        fs::create_dir_all(staging.join("schemas/test")).unwrap();
        fs::write(staging.join("schemas/test/value"), b"new").unwrap();

        let previous = data.join(PREVIOUS_ROOT).join("schemas");
        fs::create_dir_all(previous.parent().unwrap()).unwrap();
        fs::rename(data.join("schemas"), &previous).unwrap();
        fs::rename(staging.join("schemas"), data.join("schemas")).unwrap();
        let journal = journal_path(&data);
        fs::create_dir_all(journal.parent().unwrap()).unwrap();
        let bytes = serde_json::to_vec(&serde_json::json!({
            "state": "installing",
            "staging": staging,
        }))
        .unwrap();
        crate::repl::durable::atomic_write_sync(
            &journal,
            &journal.with_extension("json.tmp"),
            &bytes,
        )
        .unwrap();

        recover_interrupted_install(&data).unwrap();
        assert_eq!(fs::read(data.join("schemas/test/value")).unwrap(), b"old");
        assert!(!journal.exists());
    }
}

#[cfg(test)]
mod release_audit_tests {
    use super::*;
    #[test]
    fn failed_first_open_scaffolding_still_requires_seed_without_discarding_data() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        fs::create_dir_all(dir.path().join("schemas/mysql/tables")).unwrap();
        fs::create_dir_all(crate::storage::naming::system_catalog_dir(dir.path())).unwrap();
        let properties = crate::storage::naming::table_object_generation_path(
            dir.path(),
            crate::storage::data_dictionary::DD_DD_PROPERTIES,
            1,
            "heap",
        );
        fs::write(&properties, []).unwrap();
        drop(crate::wal::LogManager::open(dir.path()).unwrap());
        assert!(replica_needs_initial_seed(dir.path()).unwrap());

        fs::write(&properties, [1]).unwrap();
        assert!(!replica_needs_initial_seed(dir.path()).unwrap());
        fs::remove_file(&properties).unwrap();
        let object = dir.path().join("objects/tables/9/1.heap");
        fs::create_dir_all(object.parent().unwrap()).unwrap();
        fs::write(&object, []).unwrap();
        assert!(!replica_needs_initial_seed(dir.path()).unwrap());
        fs::remove_file(object).unwrap();
        let mut log = crate::wal::LogManager::open(dir.path()).unwrap();
        log.append(0, crate::wal::WalPayload::CheckpointBegin)
            .unwrap();
        log.flush().unwrap();
        drop(log);
        assert!(!replica_needs_initial_seed(dir.path()).unwrap());
    }

    #[tokio::test]
    async fn stalled_backup_frame_times_out() {
        let (mut reader, _writer) = tokio::io::duplex(64);
        let err = read_backup_frame_with_timeout(&mut reader, Duration::from_millis(20))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    }
    #[test]
    fn startup_cleanup_removes_only_private_snapshot_files() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let root = dir.path().join("system/replication");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join(".base-backup-abandoned.snapshot"), b"orphan").unwrap();
        fs::write(root.join("role.json"), b"keep").unwrap();
        cleanup_abandoned_snapshots(dir.path()).unwrap();
        assert!(!root.join(".base-backup-abandoned.snapshot").exists());
        assert!(root.join("role.json").exists());
    }
}
