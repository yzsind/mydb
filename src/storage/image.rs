//! Persistent image format and immutable-open validation owned by storage.
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::platform::file::File;
use crate::types::QueryError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
};
const BACKUP_FORMAT_VERSION: u32 = 1;
const CHUNK_BYTES: usize = 4 * 1024 * 1024;
const MAX_METADATA_BYTES: usize = 64 * 1024 * 1024;
const MAX_FILES: usize = 1_000_000;
fn backup_err(s: impl Into<String>) -> QueryError {
    super::error(crate::error::catalog::MYDB_ER_BACKUP_FAILED, s)
}
fn io_err(stage: &str, e: std::io::Error) -> QueryError {
    super::error(crate::error::catalog::ER_GET_ERRNO, format!("{stage}: {e}"))
}
fn sha256_hex(bytes: &[u8]) -> String {
    crate::util::hex::encode_lower(&Sha256::digest(bytes))
}
fn sync_dir(path: &Path) -> Result<(), QueryError> {
    crate::platform::fs::sync_path(path).map_err(|e| io_err("sync image directory", e))
}
fn write_sync(path: &Path, bytes: &[u8]) -> Result<(), QueryError> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| io_err("create image lock", e))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|e| io_err("sync image lock", e))
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]

pub enum FileClass {
    Authoritative,
    Index,
    VectorIndex,
    Derived,
    RecoveryWal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChunkRef {
    pub hash: String,
    pub len: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BackupFile {
    pub path: String,
    pub len: u64,
    pub class: FileClass,
    pub chunks: Vec<ChunkRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupManifest {
    pub backup_format_version: u32,
    pub backup_id: String,
    pub source_id: String,
    pub created_unix_ms: u64,
    pub completed_unix_ms: u64,
    pub final_barrier_ms: u64,
    #[serde(default)]
    pub capture_stats: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ai_key_ids: Vec<String>,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub end_commit_unix_us: u64,
    pub history_retention_ms: u64,
    pub storage_compatibility: String,
    pub files: Vec<BackupFile>,
}

pub const MARKER: &str = "mydb-snapshot.json";
pub(crate) const ID_FILE: &str = "mydb-snapshot.id";
pub(crate) const VERSION: u32 = super::format_versions::NATIVE_IMAGE_VERSION;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Preparing,
    ReadOnly,
    Writable,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ImageKind {
    SealedBase,
    RestoredImage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObjectReplayContract {
    pub path: String,
    pub mode: String,
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealDescriptor {
    pub version: u32,
    pub source_end_lsn: u64,
    pub source_end_time: u64,
    pub source_clock_floor: u64,
    pub next_source_lsn: u64,
    pub continuation_floor: Option<u64>,
    pub retained_recovery_ranges: Vec<(u64, u64)>,
    pub(crate) visibility_state: crate::storage::snapshot::SourceVisibility,
    pub transaction_state_digest: String,
    pub object_replay_contracts: Vec<ObjectReplayContract>,
    pub image_checkpoint: u64,
    pub query_ready: bool,
    pub continuation_ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    #[serde(skip)]
    pub(crate) _memory: Option<std::sync::Arc<ControlledMemoryCharge>>,
    pub version: u32,
    pub state: State,
    pub manifest: BackupManifest,
    pub kind: ImageKind,
    pub seal: Option<SealDescriptor>,
}

pub fn inspect(path: &Path) -> Result<Option<Snapshot>, QueryError> {
    let bytes = match File::open(path.join(MARKER)) {
        Ok(file) => {
            let mut bytes = Vec::new();
            file.take(MAX_METADATA_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(|err| io_err("read native snapshot marker", err))?;
            bytes
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if path.join(crate::storage::image_io::MODE_FILE).exists()
                || path.join(ID_FILE).exists()
                || path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with(".mydb-native-"))
            {
                return Err(backup_err("native snapshot completion marker is missing"));
            }
            return Ok(None);
        }
        Err(err) => return Err(io_err("read native snapshot marker", err)),
    };
    if bytes.len() > MAX_METADATA_BYTES {
        return Err(backup_err("native marker exceeds metadata size limit"));
    }
    let parse_memory = ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::SystemMetadata,
        bytes.len().saturating_mul(16),
    )?;
    let mode = crate::storage::image_io::read_mode(path)?
        .ok_or_else(|| backup_err("image mode marker is missing"))?;
    if mode.marker_digest != sha256_hex(&bytes) {
        return Err(backup_err("image mode/manifest digest mismatch"));
    }
    let mut snapshot: Snapshot = serde_json::from_slice(&bytes)
        .map_err(|err| backup_err(format!("invalid native snapshot marker: {err}")))?;
    snapshot._memory = Some(std::sync::Arc::new(parse_memory));
    let id = String::from_utf8(read_bounded(&path.join(ID_FILE), 128)?)
        .map_err(|_| backup_err("invalid native snapshot identity"))?;
    if id != snapshot.manifest.backup_id {
        return Err(backup_err("native snapshot identity mismatch"));
    }
    if snapshot.version != VERSION
        || snapshot.manifest.backup_format_version != BACKUP_FORMAT_VERSION
        || snapshot.manifest.storage_compatibility
            != crate::storage::format_versions::native_replication_compatibility()
    {
        return Err(backup_err("native snapshot storage format mismatch"));
    }
    if snapshot.state == State::ReadOnly {
        let seal = snapshot
            .seal
            .as_ref()
            .ok_or_else(|| backup_err("missing image seal"))?;
        if seal.version != 1
            || !seal.query_ready
            || (snapshot.kind == ImageKind::SealedBase && !seal.continuation_ready)
            || seal.source_end_lsn != snapshot.manifest.end_lsn
            || seal.source_end_time != snapshot.manifest.end_commit_unix_us
            || seal.next_source_lsn
                != seal
                    .source_end_lsn
                    .checked_add(1)
                    .ok_or_else(|| backup_err("sealed source position overflow"))?
            || seal.source_clock_floor < seal.source_end_time
            || sha256_hex(
                &serde_json::to_vec(&seal.visibility_state)
                    .map_err(|e| backup_err(e.to_string()))?,
            ) != seal.transaction_state_digest
        {
            return Err(backup_err("invalid image seal/source visibility"));
        }
    }
    if mode.kind
        != match snapshot.kind {
            ImageKind::SealedBase => "SealedBase",
            ImageKind::RestoredImage => "RestoredImage",
        }
    {
        return Err(backup_err("image kind/mode mismatch"));
    }
    let state = match mode.state.as_str() {
        "preparing" => State::Preparing,
        "read_only" => State::ReadOnly,
        "writable" => State::Writable,
        _ => return Err(backup_err("invalid image mode")),
    };
    // A completed restored manifest stays immutable through promotion. Only
    // the small, checksummed mode pointer commits the writable capability.
    if state != snapshot.state
        && !(snapshot.state == State::ReadOnly
            && snapshot.kind == ImageKind::RestoredImage
            && state == State::Writable)
    {
        return Err(backup_err("image kind/mode mismatch"));
    }
    snapshot.state = state;
    Ok(Some(snapshot))
}

pub fn is_snapshot(path: &Path) -> Result<bool, QueryError> {
    match inspect(path)? {
        Some(snapshot) if snapshot.state == State::Preparing => {
            Err(backup_err("native snapshot preparation is incomplete"))
        }
        Some(snapshot) => Ok(snapshot.state == State::ReadOnly),
        None => Ok(false),
    }
}

pub(crate) fn validate_id(id: &str) -> Result<(), QueryError> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        return Err(backup_err("invalid backup/source/key identifier"));
    }
    Ok(())
}

pub(crate) fn validate_hash(hash: &str) -> Result<(), QueryError> {
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(backup_err(
            "backup checksum must be 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

pub(crate) fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>, QueryError> {
    let file = File::open(path).map_err(|err| io_err(&format!("read {}", path.display()), err))?;
    if file
        .metadata()
        .map_err(|err| io_err("inspect backup object", err))?
        .len()
        > limit as u64
    {
        return Err(backup_err(format!(
            "backup object {} exceeds {limit} bytes",
            path.display()
        )));
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|err| io_err("read bounded backup object", err))?;
    if bytes.len() > limit {
        return Err(backup_err("backup object grew beyond its size limit"));
    }
    Ok(bytes)
}

pub(crate) fn validate_files(files: &[BackupFile]) -> Result<(), QueryError> {
    if files.len() > MAX_FILES {
        return Err(backup_err("backup file count exceeds the supported limit"));
    }
    let mut paths = BTreeSet::new();
    for file in files {
        validate_restore_path(&file.path)?;
        if !paths.insert(file.path.as_str()) {
            return Err(backup_err("duplicate backup file path"));
        }
        if file.class != classify_file(&file.path) {
            return Err(backup_err("backup file class does not match its path"));
        }
        if file.class == FileClass::RecoveryWal && !file.path.starts_with("system/wal/") {
            return Err(backup_err("recovery WAL must be under system/wal"));
        }
        let mut length = 0u64;
        for chunk in &file.chunks {
            validate_hash(&chunk.hash)?;
            if chunk.len == 0 || chunk.len > CHUNK_BYTES as u64 {
                return Err(backup_err("invalid backup chunk length"));
            }
            length = length
                .checked_add(chunk.len)
                .ok_or_else(|| backup_err("backup length overflow"))?;
        }
        if length != file.len {
            return Err(backup_err("backup file length does not match its chunks"));
        }
    }
    for path in &paths {
        let mut parent = Path::new(path).parent();
        while let Some(dir) = parent {
            if paths.contains(dir.to_str().unwrap_or("")) {
                return Err(backup_err("backup file/directory path conflict"));
            }
            parent = dir.parent();
        }
    }
    Ok(())
}

pub(crate) fn validate_manifest(manifest: &BackupManifest) -> Result<(), QueryError> {
    if manifest.ai_key_ids.len() > 16384
        || manifest
            .ai_key_ids
            .iter()
            .any(|id| id.len() != 64 || !id.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        return Err(crate::engine::ai::error(
            "ER_AI_ENCRYPTION_KEY_INVALID",
            "invalid master-key identifiers in backup manifest",
        ));
    }
    validate_id(&manifest.backup_id)?;
    validate_id(&manifest.source_id)?;
    if manifest.backup_format_version != BACKUP_FORMAT_VERSION
        || manifest.start_lsn > manifest.end_lsn
    {
        return Err(backup_err("invalid backup version or LSN range"));
    }
    validate_files(&manifest.files)?;
    if !manifest
        .files
        .iter()
        .any(|file| file.class == FileClass::RecoveryWal)
        || !manifest
            .files
            .iter()
            .any(|file| file.path.ends_with(".heap"))
    {
        return Err(backup_err(
            "backup inventory must contain catalog/data heaps and recovery WAL",
        ));
    }
    Ok(())
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

fn classify_file(relative: &str) -> FileClass {
    match Path::new(relative).extension().and_then(|ext| ext.to_str()) {
        Some("idx") => FileClass::Index,
        Some("hnsw" | "hvec" | "vmanifest" | "vpub") => FileClass::VectorIndex,
        Some("zm") => FileClass::Derived,
        Some("log") if relative.starts_with("system/wal/") => FileClass::RecoveryWal,
        _ => FileClass::Authoritative,
    }
}

pub(crate) fn validate_open(path: &Path) -> Result<(), QueryError> {
    let snapshot =
        inspect(path)?.ok_or_else(|| backup_err("native snapshot marker disappeared"))?;
    if snapshot.state != State::ReadOnly {
        return Err(backup_err(
            "native snapshot state changed while opening; retry",
        ));
    }
    validate_inventory(path, &snapshot)?;
    let seal = snapshot
        .seal
        .as_ref()
        .ok_or_else(|| backup_err("image seal missing"))?;
    let mut contracts = BTreeMap::new();
    for contract in &seal.object_replay_contracts {
        if contract.version != 1
            || ![
                "SOURCE_PHYSICAL",
                "SOURCE_INDEX_JOURNAL",
                "REBUILD_FROM_AUTHORITY",
            ]
            .contains(&contract.mode.as_str())
            || contracts
                .insert(contract.path.as_str(), contract.mode.as_str())
                .is_some()
        {
            return Err(backup_err("invalid object continuation contract"));
        }
    }
    for file in &snapshot.manifest.files {
        if file.class != FileClass::RecoveryWal && !contracts.contains_key(file.path.as_str()) {
            return Err(backup_err("image object lacks a continuation contract"));
        }
    }
    if contracts.len()
        != snapshot
            .manifest
            .files
            .iter()
            .filter(|f| f.class != FileClass::RecoveryWal)
            .count()
    {
        return Err(backup_err(
            "object continuation contract inventory mismatch",
        ));
    }
    let required = seal
        .visibility_state
        .prepared
        .iter()
        .map(|p| p.begin_lsn)
        .min();
    if snapshot.kind == ImageKind::SealedBase
        && (seal.continuation_floor != required
            || seal.retained_recovery_ranges
                != required
                    .map(|floor| vec![(floor, seal.source_end_lsn)])
                    .unwrap_or_default())
    {
        return Err(backup_err(
            "invalid prepared transaction dependency closure",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut wal_files = 0;
    for file in &snapshot.manifest.files {
        validate_restore_path(&file.path)?;
        if !seen.insert(&file.path) {
            return Err(backup_err("duplicate native snapshot file"));
        }
        let mut absolute = path.to_path_buf();
        for part in Path::new(&file.path).components() {
            absolute.push(part);
            if fs::symlink_metadata(&absolute)
                .map_err(|err| io_err("inspect native snapshot path", err))?
                .file_type()
                .is_symlink()
            {
                return Err(backup_err("symlinks are not allowed in native snapshots"));
            }
        }
        let metadata =
            fs::metadata(&absolute).map_err(|err| io_err("inspect native snapshot file", err))?;
        if !metadata.is_file() || metadata.len() != file.len {
            return Err(backup_err("native snapshot file length mismatch"));
        }
        if file.class == FileClass::RecoveryWal {
            wal_files += 1;
            let mut input = File::open(absolute).map_err(|err| io_err("read sealed WAL", err))?;
            let mut bytes = vec![0; CHUNK_BYTES];
            for chunk in &file.chunks {
                input
                    .read_exact(&mut bytes[..chunk.len as usize])
                    .map_err(|err| io_err("read sealed WAL chunk", err))?;
                if sha256_hex(&bytes[..chunk.len as usize]) != chunk.hash {
                    return Err(backup_err("sealed WAL checksum mismatch"));
                }
            }
        }
    }
    if wal_files == 0 {
        return Err(backup_err(
            "native snapshot must contain one prepared WAL file",
        ));
    }
    Ok(())
}

pub(crate) fn validate_inventory(path: &Path, snapshot: &Snapshot) -> Result<(), QueryError> {
    validate_manifest(&snapshot.manifest)?;
    let expected = snapshot
        .manifest
        .files
        .iter()
        .map(|file| (file.path.as_str(), file.len))
        .collect::<BTreeMap<_, _>>();
    fn census(
        root: &Path,
        path: &Path,
        expected: &BTreeMap<&str, u64>,
        count: &mut usize,
    ) -> Result<(), QueryError> {
        if path
            .strip_prefix(root)
            .map_or(true, |p| p.components().count() > 64)
        {
            return Err(backup_err("image directory depth exceeds budget"));
        }
        for entry in fs::read_dir(path).map_err(|e| io_err("enumerate image closure", e))? {
            let entry = entry.map_err(|e| io_err("read image closure", e))?;
            let ty = entry
                .file_type()
                .map_err(|e| io_err("inspect image closure", e))?;
            if ty.is_symlink() {
                return Err(backup_err("symlink in immutable image closure"));
            }
            if ty.is_dir() {
                census(root, &entry.path(), expected, count)?;
            } else if ty.is_file() {
                *count += 1;
                if *count > 1_000_005 {
                    return Err(backup_err("image file census exceeds budget"));
                }
                let relative = entry
                    .path()
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                if relative == crate::storage::image_io::MODE_PENDING
                    && entry
                        .metadata()
                        .map_err(|e| io_err("inspect pending mode", e))?
                        .len()
                        > 8192
                {
                    return Err(backup_err("pending image mode exceeds size limit"));
                }
                if !expected.contains_key(relative.as_str())
                    && !matches!(
                        relative.as_str(),
                        MARKER
                            | ID_FILE
                            | "mydb-image-mode.json"
                            | "mydb-image-mode.pending"
                            | "system/mydb.lock"
                    )
                {
                    return Err(backup_err(format!(
                        "unexpected persistent file in image: {relative}"
                    )));
                }
            } else {
                return Err(backup_err("unsupported object in image closure"));
            }
        }
        Ok(())
    }
    census(path, path, &expected, &mut 0)?;
    Ok(())
}

pub(crate) fn cleanup_abandoned_staging(source: &Path) -> Result<(), QueryError> {
    let registry = crate::storage::naming::system_dir(source).join("native-staging");
    let entries = match fs::read_dir(&registry) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(io_err("read native staging registry", err)),
    };
    for entry in entries {
        let entry = entry.map_err(|err| io_err("read native staging registration", err))?;
        let registration = entry.path();
        let id = entry.file_name().to_string_lossy().into_owned();
        if crate::util::uuid::parse(&id).is_none() {
            continue;
        }
        let path: PathBuf = serde_json::from_slice(
            &fs::read(&registration)
                .map_err(|err| io_err("read native staging registration", err))?,
        )
        .map_err(|err| backup_err(format!("invalid native staging registration: {err}")))?;
        if path
            .file_name()
            .is_none_or(|name| name != std::ffi::OsStr::new(&format!(".mydb-native-{id}")))
        {
            return Err(backup_err("native staging registration identity mismatch"));
        }
        match fs::symlink_metadata(&path) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_err("inspect abandoned native staging", err)),
            Ok(meta) => {
                let empty = meta.is_dir()
                    && fs::read_dir(&path)
                        .map_err(|err| io_err("inspect native staging contents", err))?
                        .next()
                        .is_none();
                let initializing = meta.is_dir()
                    && fs::read_dir(&path)
                        .map_err(|err| io_err("inspect native staging identity", err))?
                        .all(|entry| entry.is_ok_and(|entry| entry.file_name() == ID_FILE))
                    && fs::read(path.join(ID_FILE))
                        .is_ok_and(|bytes| id.as_bytes().starts_with(&bytes));
                if !meta.is_dir()
                    || (!empty
                        && !initializing
                        && fs::read_to_string(path.join(ID_FILE)).ok().as_deref() != Some(&id))
                {
                    return Err(backup_err("abandoned native staging identity mismatch"));
                }
                fs::create_dir_all(crate::storage::naming::system_dir(&path))
                    .map_err(|err| io_err("create staging lock directory", err))?;
                if !path.join("system/mydb.lock").exists() {
                    write_sync(&path.join("system/mydb.lock"), b"")?;
                }
                // A preparation child may outlive its source process. Keep its
                // files until it releases the directory lock; retry next open.
                let Ok(_lock) = crate::storage::dir_lock::DataDirLock::acquire_with_wait(
                    &path,
                    std::time::Duration::ZERO,
                ) else {
                    continue;
                };
                fs::remove_dir_all(&path)
                    .map_err(|err| io_err("remove abandoned native staging", err))?;
                sync_dir(path.parent().expect("registered staging parent"))?;
            }
        }
        fs::remove_file(&registration)
            .map_err(|err| io_err("remove native staging registration", err))?;
    }
    sync_dir(&registry)
}
