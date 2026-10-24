//! Immutable data-file capabilities and externally owned image runtime directories.
use crate::platform::file::{File, OpenOptions};
use crate::types::QueryError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex, OnceLock,
    },
};
pub(crate) const MODE_FILE: &str = "mydb-image-mode.json";
pub(crate) const MODE_PENDING: &str = "mydb-image-mode.pending";
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Mode {
    pub version: u32,
    pub state: String,
    pub kind: String,
    pub marker_digest: String,
}
fn backup_err(s: impl Into<String>) -> QueryError {
    crate::storage::error(crate::error::catalog::MYDB_ER_BACKUP_FAILED, s)
}
fn io_err(stage: &str, e: std::io::Error) -> QueryError {
    crate::storage::error(crate::error::catalog::ER_GET_ERRNO, format!("{stage}: {e}"))
}
fn random_uuid() -> String {
    format!("{:032x}", rand::random::<u128>())
}
pub(crate) fn read_only_error() -> QueryError {
    crate::storage::error(
        crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
        "immutable database image is read-only",
    )
}
pub(crate) fn read_mode(root: &Path) -> Result<Option<Mode>, QueryError> {
    let file = match std::fs::File::open(root.join(MODE_FILE)) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_err("read image mode", e)),
    };
    let mut bytes = Vec::new();
    file.take(8193)
        .read_to_end(&mut bytes)
        .map_err(|e| io_err("read image mode", e))?;
    if bytes.len() > 8192 {
        return Err(backup_err("image mode exceeds size limit"));
    }
    let mode: Mode = serde_json::from_slice(&bytes)
        .map_err(|e| backup_err(format!("invalid image mode: {e}")))?;
    if mode.version != super::format_versions::IMAGE_MODE_VERSION
        || !["preparing", "read_only", "writable"].contains(&mode.state.as_str())
        || !["SealedBase", "RestoredImage"].contains(&mode.kind.as_str())
        || (mode.kind == "SealedBase" && mode.state == "writable")
    {
        return Err(backup_err("unknown/inconsistent image mode"));
    }
    Ok(Some(mode))
}
pub(crate) fn persist_mode(
    root: &Path,
    state: &str,
    kind: &str,
    marker: &[u8],
) -> Result<(), QueryError> {
    let value = Mode {
        version: super::format_versions::IMAGE_MODE_VERSION,
        state: state.into(),
        kind: kind.into(),
        marker_digest: crate::util::hex::encode_lower(&Sha256::digest(marker)),
    };
    let bytes = serde_json::to_vec(&value).map_err(|e| backup_err(e.to_string()))?;
    // This reserved control file may survive a process crash. It never
    // grants capabilities; only the atomic MODE_FILE rename does that.
    let temp = root.join(MODE_PENDING);
    match fs::remove_file(&temp) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_err("retire pending image mode", error)),
    }
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(|e| io_err("create image mode", e))?;
        file.write_all(&bytes)
            .and_then(|_| {
                #[cfg(target_os = "macos")]
                {
                    crate::platform::fs::sync_before_atomic_rename_at(&file, &temp)
                }
                #[cfg(not(target_os = "macos"))]
                {
                    file.sync_all()
                }
            })
            .map_err(|e| io_err("sync image mode", e))?;
    }
    #[cfg(test)]
    if let Err(error) = crate::backup::faults::hit("image_mode_before_publish", root) {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    fs::rename(&temp, root.join(MODE_FILE)).map_err(|e| io_err("publish image mode", e))?;
    #[cfg(test)]
    crate::backup::faults::hit("image_mode_published", root)?;
    crate::platform::fs::sync_path(root).map_err(|e| io_err("sync image mode directory", e))
}
pub(crate) fn repository_ancestor(root: &Path) -> Option<PathBuf> {
    root.ancestors()
        .find(|path| path.join("repository.json").exists())
        .map(Path::to_path_buf)
}
struct RuntimeEntry {
    scratch: PathBuf,
    refs: usize,
}
static RUNTIMES: OnceLock<Mutex<HashMap<PathBuf, RuntimeEntry>>> = OnceLock::new();
static ACTIVE_RUNTIMES: AtomicUsize = AtomicUsize::new(0);

/// Registered before opening any data files, held through database shutdown.
#[derive(Debug)]
pub(crate) struct SnapshotRuntime {
    root: PathBuf,
}

impl SnapshotRuntime {
    pub(crate) fn open(root: &Path) -> Result<Self, QueryError> {
        Self::open_with_temp_dir(root, &std::env::temp_dir())
    }

    pub(crate) fn open_with_temp_dir(root: &Path, temp_dir: &Path) -> Result<Self, QueryError> {
        let root = fs::canonicalize(root).map_err(|err| io_err("resolve snapshot", err))?;
        let base = temp_dir.join("mydb");
        let ancestor = existing_ancestor(&base)
            .ok_or_else(|| backup_err("cannot resolve snapshot runtime parent"))?;
        let protected = repository_ancestor(&root).unwrap_or_else(|| root.clone());
        if ancestor.starts_with(&protected) {
            return Err(backup_err(
                "snapshot runtime temporary directory must be outside the backup",
            ));
        }
        let mut entries = RUNTIMES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap();
        if let Some(entry) = entries.get_mut(&root) {
            entry.refs += 1;
        } else {
            fs::create_dir_all(&base)
                .map_err(|err| io_err("create snapshot runtime parent", err))?;
            let scratch = base.join(format!("snapshot-runtime-{}", random_uuid()));
            fs::create_dir(&scratch).map_err(|err| io_err("create snapshot runtime", err))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&scratch, fs::Permissions::from_mode(0o700))
                    .map_err(|err| io_err("secure snapshot runtime", err))?;
            }
            entries.insert(root.clone(), RuntimeEntry { scratch, refs: 1 });
            ACTIVE_RUNTIMES.fetch_add(1, Ordering::Release);
        }
        Ok(Self { root })
    }
}

impl Drop for SnapshotRuntime {
    fn drop(&mut self) {
        let mut entries = RUNTIMES.get().unwrap().lock().unwrap();
        let entry = entries.get_mut(&self.root).unwrap();
        entry.refs -= 1;
        if entry.refs == 0 {
            let entry = entries.remove(&self.root).unwrap();
            ACTIVE_RUNTIMES.fetch_sub(1, Ordering::Release);
            let _ = fs::remove_dir_all(entry.scratch);
        }
    }
}

/// File-open boundary only; never called for cached page hits.
pub(crate) fn immutable_path(path: &Path) -> bool {
    let Some(resolved) = existing_ancestor(path) else {
        return false;
    };
    let start = if resolved.is_dir() {
        resolved.as_path()
    } else {
        resolved.parent().unwrap_or(&resolved)
    };
    for root in start.ancestors() {
        if root.join(MODE_FILE).exists() {
            return match read_mode(root) {
                Ok(Some(mode)) => mode.state == "read_only",
                _ => true,
            };
        }
        if root.join("mydb-snapshot.id").exists() || root.join("mydb-snapshot.json").exists() {
            return true;
        }
    }
    false
}

fn existing_ancestor(path: &Path) -> Option<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().ok()?.join(path)
    };
    let mut current = absolute.as_path();
    loop {
        if let Ok(resolved) = fs::canonicalize(current) {
            return Some(resolved);
        }
        current = current.parent()?;
    }
}

pub(crate) fn runtime_dir(root: &Path) -> Option<PathBuf> {
    if ACTIVE_RUNTIMES.load(Ordering::Acquire) == 0 {
        return None;
    }
    RUNTIMES
        .get()
        .unwrap()
        .lock()
        .unwrap()
        .get(root)
        .map(|entry| entry.scratch.clone())
}

pub(crate) fn open_data_file(path: &Path) -> std::io::Result<File> {
    if immutable_path(path) {
        File::open(path)
    } else {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        match options.open(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // Most data-file parents already exist. Avoid an EEXIST
                // mkdir/stat pair on every ordinary file-cache miss.
                if immutable_path(path) {
                    return File::open(path);
                }
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                options.open(path)
            }
            result => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_file_open_creates_missing_parents_and_preserves_existing_bytes() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let path = root.path().join("nested/objects/data.heap");
        let mut file = open_data_file(&path).unwrap();
        file.write_all(b"existing heap bytes").unwrap();
        drop(file);
        let mut reopened = open_data_file(&path).unwrap();
        let mut bytes = Vec::new();
        reopened.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"existing heap bytes");
        let blocked = root.path().join("not-a-directory");
        fs::write(&blocked, b"preserved").unwrap();
        assert!(open_data_file(&blocked.join("child.heap")).is_err());
        assert_eq!(fs::read(blocked).unwrap(), b"preserved");
    }
}
