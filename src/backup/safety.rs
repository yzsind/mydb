//! Shared input validation and ownership for offline repository operations.
use super::*;

pub(super) const MAX_METADATA_BYTES: usize = 64 * 1024 * 1024;
pub(super) use crate::storage::image::{read_bounded, validate_hash, validate_id};

pub(super) fn resolved_destination(path: &Path) -> Result<PathBuf, QueryError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
    {
        return Err(backup_err(
            "backup paths must be absolute without dot components",
        ));
    }
    let mut prefix = PathBuf::new();
    for part in path.components() {
        prefix.push(part);
        if let Ok(meta) = fs::symlink_metadata(&prefix) {
            // macOS supplies these system aliases; arbitrary user links are rejected.
            let system_alias = cfg!(target_os = "macos")
                && matches!(prefix.to_str(), Some("/tmp" | "/var" | "/etc"));
            if crate::platform::fs::is_link(&meta) && !system_alias {
                return Err(backup_err("symlink backup destination is not allowed"));
            }
        }
    }
    let mut ancestor = path;
    let mut tail = Vec::new();
    while !ancestor.exists() {
        tail.push(
            ancestor
                .file_name()
                .ok_or_else(|| backup_err("invalid destination"))?,
        );
        ancestor = ancestor
            .parent()
            .ok_or_else(|| backup_err("invalid destination parent"))?;
    }
    let mut resolved =
        fs::canonicalize(ancestor).map_err(|err| io_err("resolve backup path", err))?;
    for part in tail.into_iter().rev() {
        resolved.push(part);
    }
    Ok(resolved)
}

pub(super) fn reject_overlap(a: &Path, b: &Path) -> Result<(), QueryError> {
    let a = if a.exists() {
        fs::canonicalize(a).map_err(|e| io_err("resolve existing backup source", e))?
    } else {
        resolved_destination(a)?
    };
    let b = resolved_destination(b)?;
    if a.starts_with(&b) || b.starts_with(&a) {
        return Err(backup_err(
            "backup source, repository and destination must not overlap",
        ));
    }
    Ok(())
}

pub(super) fn reject_symlink_path(root: &Path, relative: &Path) -> Result<(), QueryError> {
    let mut path = root.to_path_buf();
    for part in relative.components() {
        if !matches!(part, Component::Normal(_)) {
            return Err(backup_err("unsafe object path"));
        }
        path.push(part);
        match fs::symlink_metadata(&path) {
            Ok(meta) if crate::platform::fs::is_link(&meta) => {
                return Err(backup_err("symlink in backup repository"))
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_err("inspect repository path", err)),
        }
    }
    Ok(())
}

/// New inodes are private by exclusive creation. A reused writable inode must
/// be regular, must not be a reparse point and must have exactly one link.
pub(super) fn open_private_output(
    path: &Path,
    read: bool,
    reuse: bool,
) -> Result<(std::fs::File, bool), QueryError> {
    fn options(read: bool) -> std::fs::OpenOptions {
        let mut options = std::fs::OpenOptions::new();
        options.read(read).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.custom_flags(
                windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT,
            );
        }
        options
    }
    match options(read).create_new(true).open(path) {
        Ok(file) => Ok((file, true)),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && reuse => {
            let file = options(read)
                .open(path)
                .map_err(|error| io_err("open private backup output", error))?;
            let metadata = file
                .metadata()
                .map_err(|error| io_err("inspect private backup output", error))?;
            if !metadata.is_file() {
                return Err(backup_err("private backup output must be a regular file"));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.nlink() != 1 {
                    return Err(backup_err(
                        "private backup output must not have additional hard links",
                    ));
                }
            }
            #[cfg(windows)]
            {
                use std::os::windows::io::AsRawHandle;
                use windows_sys::Win32::Storage::FileSystem::{
                    GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
                    FILE_ATTRIBUTE_REPARSE_POINT,
                };
                // SAFETY: the borrowed handle is live and the output has its exact ABI layout.
                unsafe {
                    let mut info: BY_HANDLE_FILE_INFORMATION = std::mem::zeroed();
                    if GetFileInformationByHandle(file.as_raw_handle(), &mut info) == 0 {
                        return Err(io_err(
                            "inspect private backup output",
                            std::io::Error::last_os_error(),
                        ));
                    }
                    if info.nNumberOfLinks != 1
                        || info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
                    {
                        return Err(backup_err(
                            "private backup output must not have hard links or reparse points",
                        ));
                    }
                }
            }
            Ok((file, false))
        }
        Err(error) => Err(io_err("create private backup output", error)),
    }
}

pub(super) struct ScratchDirectory(pub(super) PathBuf);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryOwner {
    version: u32,
    destination: PathBuf,
    kind: String,
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input_digest: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerEnvelope {
    owner: DirectoryOwner,
    digest: String,
}
const WORKSPACE_MAGIC: &[u8; 8] = b"MYDBOP1\n";

/// The stable guard inode is both the OS lock and the durable ownership
/// journal. Serialized operations reuse one registered slot; they never
/// resume its old payload. A new registration first confirms old deletions.
/// Private payloads need no extra publication barriers.
pub(super) struct DirectoryOperation {
    operation: PathBuf,
    pub data: PathBuf,
    destination: PathBuf,
    pub id: String,
    _guard: std::fs::File,
    keep_payload: bool,
}
impl DirectoryOperation {
    fn prefix(destination: &Path, kind: &str) -> String {
        format!(
            ".mydb-{kind}-{}",
            sha256_hex(destination.as_os_str().to_string_lossy().as_bytes())
        )
    }
    fn initialization_lock(destination: &Path, kind: &str) -> Result<std::fs::File, QueryError> {
        validate_id(kind)?;
        let parent = destination
            .parent()
            .ok_or_else(|| backup_err("directory needs a parent"))?;
        reject_directory_links(parent)?;
        if !parent.is_dir() {
            create_directory_durable(parent)?;
        }
        let name = format!("{}.lock", Self::prefix(destination, kind));
        reject_symlink_path(parent, Path::new(&name))?;
        let (guard, _) = open_private_output(&parent.join(name), true, true)?;
        guard
            .lock()
            .map_err(|e| io_err("lock workspace guard", e))?;
        Ok(guard)
    }
    pub fn inspect(destination: &Path, kind: &str) -> Result<serde_json::Value, QueryError> {
        use std::io::{Seek, SeekFrom};
        validate_id(kind)?;
        let destination = resolved_destination(destination)?;
        let parent = destination
            .parent()
            .ok_or_else(|| backup_err("workspace needs a parent"))?;
        let name = format!("{}.lock", Self::prefix(&destination, kind));
        reject_symlink_path(parent, Path::new(&name))?;
        let path = parent.join(&name);
        if !path.exists() {
            return Ok(
                serde_json::json!({"destination":destination,"kind":kind,"workspace":"NOT_REGISTERED","destination_present":destination.exists()}),
            );
        }
        let mut guard = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| io_err("inspect workspace guard", e))?;
        match guard.try_lock() {
            Err(std::fs::TryLockError::WouldBlock) => {
                return Ok(
                    serde_json::json!({"destination":destination,"kind":kind,"workspace":"ACTIVE","destination_present":destination.exists()}),
                )
            }
            Err(std::fs::TryLockError::Error(e)) => {
                return Err(io_err("inspect workspace lock", e))
            }
            Ok(()) => {}
        }
        guard
            .seek(SeekFrom::Start(0))
            .map_err(|e| io_err("read workspace ownership", e))?;
        let mut input = Vec::new();
        (&mut guard)
            .take(16385)
            .read_to_end(&mut input)
            .map_err(|e| io_err("inspect workspace journal", e))?;
        if input.len() > 16384 || !input.starts_with(WORKSPACE_MAGIC) {
            return Err(backup_err("unrecognized workspace journal"));
        }
        let value: OwnerEnvelope = serde_json::from_slice(&input[WORKSPACE_MAGIC.len()..])
            .map_err(|e| backup_err(format!("invalid workspace ownership: {e}")))?;
        if sha256_hex(&serde_json::to_vec(&value.owner).map_err(|e| backup_err(e.to_string()))?)
            != value.digest
            || value.owner.version != 1
            || value.owner.destination != destination
            || value.owner.kind != kind
        {
            return Err(backup_err("workspace ownership/checksum mismatch"));
        }
        validate_id(&value.owner.id)?;
        let payload = parent.join(format!(
            "{}-{}",
            Self::prefix(&destination, kind),
            value.owner.id
        ));
        reject_symlink_path(parent, payload.strip_prefix(parent).unwrap())?;
        Ok(
            serde_json::json!({"destination":destination,"kind":kind,"operation_id":value.owner.id,"input_digest":value.owner.input_digest,"workspace":if payload.exists(){"INTERRUPTED_PRIVATE"}else{"NO_PRIVATE_PAYLOAD"},"destination_present":destination.exists(),"installation":"Inspect/verify the destination before confirming a visible installation; private payload ownership does not prove a committed image."}),
        )
    }
    pub fn cleanup(destination: &Path, kind: &str) -> Result<(), QueryError> {
        let parent = destination
            .parent()
            .ok_or_else(|| backup_err("directory needs a parent"))?;
        if !parent.exists() {
            return Ok(());
        }
        let prefix = format!("{}-", Self::prefix(destination, kind));
        let mut found = false;
        for entry in fs::read_dir(parent).map_err(|e| io_err("find workspace ownership", e))? {
            let entry = entry.map_err(|e| io_err("read workspace entry", e))?;
            found |= entry.file_name().to_string_lossy().starts_with(&prefix);
        }
        if !found {
            return Ok(());
        }
        let mut guard = Self::initialization_lock(destination, kind)?;
        Self::cleanup_locked(destination, kind, &mut guard, true, true).map(|_| ())
    }
    fn cleanup_locked(
        destination: &Path,
        kind: &str,
        guard: &mut std::fs::File,
        confirm_deletion: bool,
        discard_payload: bool,
    ) -> Result<Option<DirectoryOwner>, QueryError> {
        use std::io::{Seek, SeekFrom};
        let parent = destination.parent().unwrap();
        let prefix = format!("{}-", Self::prefix(destination, kind));
        let mut directories = Vec::new();
        for entry in fs::read_dir(parent).map_err(|e| io_err("find interrupted workspace", e))? {
            let entry = entry.map_err(|e| io_err("read workspace entry", e))?;
            if entry.file_name().to_string_lossy().starts_with(&prefix) {
                if !entry
                    .file_type()
                    .map_err(|e| io_err("inspect workspace", e))?
                    .is_dir()
                    || !directories.is_empty()
                {
                    return Err(backup_err("unrecognized workspace ownership"));
                }
                directories.push(entry.path());
            }
        }
        guard
            .seek(SeekFrom::Start(0))
            .map_err(|e| io_err("read workspace journal", e))?;
        let mut input = Vec::new();
        (&mut *guard)
            .take(16385)
            .read_to_end(&mut input)
            .map_err(|e| io_err("read workspace journal", e))?;
        if input.len() > 16384 {
            return Err(backup_err("workspace journal exceeds budget"));
        }
        let owner = if input.starts_with(WORKSPACE_MAGIC) {
            let envelope =
                serde_json::from_slice::<OwnerEnvelope>(&input[WORKSPACE_MAGIC.len()..]).ok();
            match envelope {
                Some(envelope)
                    if sha256_hex(
                        &serde_json::to_vec(&envelope.owner)
                            .map_err(|e| backup_err(e.to_string()))?,
                    ) == envelope.digest =>
                {
                    Some(envelope.owner)
                }
                _ if directories.is_empty() => None,
                _ => return Err(backup_err("incomplete workspace ownership journal")),
            }
        } else if WORKSPACE_MAGIC.starts_with(&input) && directories.is_empty() {
            None
        } else {
            return Err(backup_err("unrecognized workspace journal"));
        };
        if let Some(owner) = &owner {
            validate_id(&owner.id)?;
            if owner.version != 1 || owner.destination != destination || owner.kind != kind {
                return Err(backup_err("workspace ownership mismatch"));
            }
            let expected = parent.join(format!("{prefix}{}", owner.id));
            for directory in directories {
                if directory != expected {
                    return Err(backup_err("workspace operation identity mismatch"));
                }
                if discard_payload {
                    fs::remove_dir_all(directory)
                        .map_err(|e| io_err("clean interrupted workspace", e))?;
                }
            }
        } else if !directories.is_empty() {
            return Err(backup_err("workspace lacks a registered owner"));
        }
        // A stable slot keeps the same registered ID across retries. If a
        // new registration is needed, confirm every old unlink first.
        if confirm_deletion || owner.is_none() {
            sync_dir(parent)?;
        }
        Ok(owner)
    }
    pub fn new(destination: &Path, kind: &str) -> Result<Self, QueryError> {
        Self::begin(destination, kind, None)
    }
    pub fn resume(
        destination: &Path,
        kind: &str,
        input_digest: String,
    ) -> Result<Self, QueryError> {
        validate_hash(&input_digest)?;
        Self::begin(destination, kind, Some(input_digest))
    }
    fn begin(
        destination: &Path,
        kind: &str,
        input_digest: Option<String>,
    ) -> Result<Self, QueryError> {
        use std::io::{Seek, SeekFrom};
        let mut guard = Self::initialization_lock(destination, kind)?;
        let existing =
            Self::cleanup_locked(destination, kind, &mut guard, false, input_digest.is_none())?;
        let id = existing
            .as_ref()
            .map(|owner| owner.id.clone())
            .unwrap_or_else(random_uuid);
        let operation = destination
            .parent()
            .unwrap()
            .join(format!("{}-{id}", Self::prefix(destination, kind)));
        let changed = existing
            .as_ref()
            .is_none_or(|owner| owner.input_digest != input_digest);
        if changed && operation.exists() {
            return Err(backup_err("resume input identity changed; inspect and explicitly clean the previous operation"));
        }
        if changed {
            sync_dir(destination.parent().unwrap())?;
            let owner = DirectoryOwner {
                version: 1,
                destination: destination.into(),
                kind: kind.into(),
                id: id.clone(),
                input_digest: input_digest.clone(),
            };
            let digest =
                sha256_hex(&serde_json::to_vec(&owner).map_err(|e| backup_err(e.to_string()))?);
            let bytes = serde_json::to_vec(&OwnerEnvelope { owner, digest })
                .map_err(|e| backup_err(e.to_string()))?;
            if bytes.len() + WORKSPACE_MAGIC.len() > 16384 {
                return Err(backup_err("workspace owner exceeds budget"));
            }
            guard
                .seek(SeekFrom::Start(0))
                .map_err(|e| io_err("update workspace journal", e))?;
            guard
                .write_all(WORKSPACE_MAGIC)
                .and_then(|_| guard.write_all(&bytes))
                .and_then(|_| guard.set_len((WORKSPACE_MAGIC.len() + bytes.len()) as u64))
                .and_then(|_| guard.sync_all())
                .map_err(|e| io_err("persist workspace ownership", e))?;
        }
        let operation = destination
            .parent()
            .unwrap()
            .join(format!("{}-{id}", Self::prefix(destination, kind)));
        if !operation.exists() {
            fs::create_dir(&operation).map_err(|e| io_err("create private workspace", e))?;
        }
        let result = Self {
            data: operation.join("data"),
            operation,
            destination: destination.into(),
            id,
            _guard: guard,
            keep_payload: input_digest.is_some(),
        };
        if !result.data.exists() {
            fs::create_dir(&result.data).map_err(|e| io_err("create workspace payload", e))?;
        }
        reject_directory_links(&result.data)?;
        Ok(result)
    }
    pub fn finish(mut self) {
        self.keep_payload = false;
    }
    pub fn install(self) -> Result<(), QueryError> {
        sync_tree(&self.data)?;
        if self.destination.exists() {
            fs::remove_dir(&self.destination)
                .map_err(|e| io_err("initialization target is not empty", e))?;
        }
        native::rename_new_directory(&self.data, &self.destination)?;
        #[cfg(test)]
        super::faults::hit("directory_installed", &self.destination)?;
        sync_dir(self.destination.parent().unwrap())
    }
}
impl Drop for DirectoryOperation {
    fn drop(&mut self) {
        // Keep the journal's ID valid even if this unlink is lost on crash.
        // The next owner confirms deletion before replacing the record.
        if !self.keep_payload {
            let _ = fs::remove_dir_all(&self.operation);
        }
    }
}

/// Reserve retained WAL storage in small batches; per-record bookkeeping
/// stays local while every allocation remains covered by the governor.
pub(super) fn reject_directory_links(path: &Path) -> Result<(), QueryError> {
    let mut prefix = PathBuf::new();
    for part in path.components() {
        prefix.push(part);
        if let Ok(meta) = fs::symlink_metadata(&prefix) {
            // macOS supplies these system aliases; arbitrary user links are rejected.
            let system_alias = cfg!(target_os = "macos")
                && matches!(prefix.to_str(), Some("/tmp" | "/var" | "/etc"));
            if crate::platform::fs::is_link(&meta) && !system_alias {
                return Err(backup_err("symlink backup destination is not allowed"));
            }
        }
    }
    Ok(())
}

pub(super) fn create_directory_durable(path: &Path) -> Result<(), QueryError> {
    reject_directory_links(path)?;
    let mut ancestor = path;
    while !ancestor.exists() {
        ancestor = ancestor
            .parent()
            .ok_or_else(|| backup_err("directory has no existing ancestor"))?;
    }
    fs::create_dir_all(path).map_err(|err| io_err("create backup directory", err))?;
    // Persist descendants before their links in each parent. One final
    // device barrier covers all directory fsyncs on the same device.
    let directories = path
        .ancestors()
        .take_while(|current| current.starts_with(ancestor))
        .collect::<Vec<_>>();
    crate::platform::fs::sync_paths(directories.iter())
        .map_err(|error| io_err("sync backup directory ancestry", error))?;
    resources::io()
        .helper_sync_calls
        .fetch_add(directories.len() as u64, Ordering::Relaxed);
    Ok(())
}

impl ScratchDirectory {
    pub(super) fn new(parent: &Path, prefix: &str) -> Result<Self, QueryError> {
        let path = parent.join(format!(".{prefix}-{}", random_uuid()));
        fs::create_dir(&path).map_err(|err| io_err("create operation staging directory", err))?;
        Ok(Self(path))
    }
}

impl Drop for ScratchDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub(super) fn reserve_disk_write(
    path: &Path,
    bytes: u64,
) -> Result<crate::engine::executor::spill_quota::PendingSpillWrite, QueryError> {
    let mut root = path;
    while !root.is_dir() {
        root = root
            .parent()
            .ok_or_else(|| backup_err("disk admission has no existing parent"))?;
    }
    crate::engine::executor::spill_quota::charger_for_new_dir(root)?.reserve_physical_write(bytes)
}

#[cfg(test)]
pub(super) use crate::storage::image::validate_files;
