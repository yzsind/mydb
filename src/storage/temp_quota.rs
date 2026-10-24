//! Session-local admission for explicit temporary tables. Storage is charged by
//! allocated heap/LOB/index extent (including cached pages), not logical rows.
//! Rollback may leave reusable pages allocated; TRUNCATE/DROP release them.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use crate::types::QueryError;
use parking_lot::Mutex;

#[derive(Debug, Default)]
struct Usage {
    tables: u64,
    storage: u64,
    undo: u64,
}

#[derive(Debug)]
pub(crate) struct SessionTempQuota {
    table_limit: u64,
    storage_limit: u64,
    undo_limit: u64,
    used: Mutex<Usage>,
}
impl Default for SessionTempQuota {
    fn default() -> Self {
        fn limit(name: &str, default: u64) -> u64 {
            crate::util::env::var(name)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(default)
        }
        Self::new(
            limit("MYDB_SESSION_TEMP_TABLE_LIMIT", 1024),
            limit("MYDB_SESSION_TEMP_STORAGE_LIMIT", 1024 * 1024 * 1024),
            limit("MYDB_SESSION_TEMP_UNDO_LIMIT", 64 * 1024 * 1024),
        )
    }
}
impl SessionTempQuota {
    pub(crate) fn new(tables: u64, storage: u64, undo: u64) -> Self {
        Self {
            table_limit: tables,
            storage_limit: storage,
            undo_limit: undo,
            used: Mutex::new(Usage::default()),
        }
    }
    fn check(value: u64, extra: u64, limit: u64, resource: &str) -> Result<u64, QueryError> {
        let projected = value
            .checked_add(extra)
            .ok_or_else(|| quota_error(resource, limit))?;
        if limit != 0 && projected > limit {
            return Err(quota_error(resource, limit));
        }
        Ok(projected)
    }
    fn reserve(
        self: &Arc<Self>,
        bytes: u64,
        undo: bool,
    ) -> Result<Arc<ByteReservation>, QueryError> {
        let mut used = self.used.lock();
        if undo {
            used.undo = Self::check(used.undo, bytes, self.undo_limit, "undo bytes")?;
        } else {
            used.storage = Self::check(used.storage, bytes, self.storage_limit, "storage bytes")?;
        }
        Ok(Arc::new(ByteReservation {
            quota: self.clone(),
            bytes,
            undo,
        }))
    }
    #[cfg(test)]
    pub(crate) fn usage(&self) -> (u64, u64, u64) {
        let u = self.used.lock();
        (u.tables, u.storage, u.undo)
    }
}
fn quota_error(resource: &str, limit: u64) -> QueryError {
    crate::error::catalog::ER_RECORD_FILE_FULL.error(format!(
        "Session temporary table {resource} quota exceeded (limit={limit}); drop or truncate temporary tables, end the transaction, or raise the corresponding session_temp_* startup limit"))
}

/// Clones of an undo record share the reservation until the last reader drops it.
#[derive(Debug)]
pub(crate) struct ByteReservation {
    quota: Arc<SessionTempQuota>,
    bytes: u64,
    undo: bool,
}
impl Drop for ByteReservation {
    fn drop(&mut self) {
        let mut u = self.quota.used.lock();
        if self.undo {
            u.undo = u.undo.saturating_sub(self.bytes);
        } else {
            u.storage = u.storage.saturating_sub(self.bytes);
        }
    }
}

struct TableQuota {
    quota: Arc<SessionTempQuota>,
    files: Mutex<HashMap<PathBuf, u64>>,
}
impl Drop for TableQuota {
    fn drop(&mut self) {
        let bytes: u64 = self.files.get_mut().values().sum();
        let mut used = self.quota.used.lock();
        used.tables = used.tables.saturating_sub(1);
        used.storage = used.storage.saturating_sub(bytes);
    }
}
type Key = (PathBuf, u64);
fn registry() -> &'static Mutex<HashMap<Key, Arc<TableQuota>>> {
    static TABLES: OnceLock<Mutex<HashMap<Key, Arc<TableQuota>>>> = OnceLock::new();
    TABLES.get_or_init(|| Mutex::new(HashMap::new()))
}
fn key(data_dir: &Path, table: u64) -> Key {
    (crate::storage::naming::system_tmp_dir(data_dir), table)
}

pub(crate) struct Registration {
    key: Key,
    committed: bool,
}
impl Registration {
    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        if !self.committed {
            registry().lock().remove(&self.key);
        }
    }
}
pub(crate) fn register(
    data_dir: &Path,
    table: u64,
    quota: &Arc<SessionTempQuota>,
) -> Result<Registration, QueryError> {
    {
        let mut used = quota.used.lock();
        used.tables = SessionTempQuota::check(used.tables, 1, quota.table_limit, "count")?;
    }
    let key = key(data_dir, table);
    registry().lock().insert(
        key.clone(),
        Arc::new(TableQuota {
            quota: quota.clone(),
            files: Mutex::new(HashMap::new()),
        }),
    );
    Ok(Registration {
        key,
        committed: false,
    })
}
pub(crate) fn unregister(data_dir: &Path, table: u64) {
    registry().lock().remove(&key(data_dir, table));
}

pub(crate) fn is_temporary_path(path: &Path) -> bool {
    path.parent()
        .and_then(Path::file_name)
        .and_then(|s| s.to_str())
        .is_some_and(|name| name == "temp_tables" || name == "temp_indexes")
}

fn file_owner(path: &Path) -> Option<Arc<TableQuota>> {
    let parent = path.parent()?;
    let name = parent.file_name()?.to_str()?;
    if name != "temp_tables" && name != "temp_indexes" {
        return None;
    }
    let table = path
        .file_name()?
        .to_str()?
        .split(['.', '_'])
        .next()?
        .parse::<u64>()
        .ok()?;
    registry()
        .lock()
        .get(&(parent.parent()?.to_path_buf(), table))
        .cloned()
}
/// Called before extent growth, including before changing allocator watermarks.
/// A conservative reservation survives IO failure; a retry reuses that credit.
pub(crate) fn reserve_extent(path: &Path, length: u64) -> Result<(), QueryError> {
    let Some(owner) = file_owner(path) else {
        return Ok(());
    };
    let mut files = owner.files.lock();
    let old = files.get(path).copied().unwrap_or(0);
    if length <= old {
        return Ok(());
    }
    let mut used = owner.quota.used.lock();
    used.storage = SessionTempQuota::check(
        used.storage,
        length - old,
        owner.quota.storage_limit,
        "storage bytes",
    )?;
    files.insert(path.to_path_buf(), length);
    Ok(())
}
/// Called only after truncation/deletion succeeds; ordinary cache eviction must
/// not release quota for storage that still exists.
pub(crate) fn release_files(data_dir: &Path, table: u64) {
    if let Some(owner) = registry().lock().get(&key(data_dir, table)).cloned() {
        let mut files = owner.files.lock();
        let bytes: u64 = files.values().sum();
        files.clear();
        let mut used = owner.quota.used.lock();
        used.storage = used.storage.saturating_sub(bytes);
    }
}
pub(crate) fn reserve_undo(
    data_dir: &Path,
    table: u64,
    bytes: u64,
) -> Result<Option<Arc<ByteReservation>>, QueryError> {
    let owner = registry().lock().get(&key(data_dir, table)).cloned();
    owner
        .map(|owner| owner.quota.reserve(bytes, true))
        .transpose()
}
pub(crate) fn reserve_snapshot(
    data_dir: &Path,
    table: u64,
    bytes: u64,
) -> Result<Option<Arc<ByteReservation>>, QueryError> {
    let owner = registry().lock().get(&key(data_dir, table)).cloned();
    owner
        .map(|owner| owner.quota.reserve(bytes, false))
        .transpose()
}

pub(crate) fn clear_database(data_dir: &Path) {
    let root = crate::storage::naming::system_tmp_dir(data_dir);
    registry().lock().retain(|(dir, _), _| dir != &root);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn undo_reservations_follow_last_owner_and_rejected_growth_is_free() {
        let quota = Arc::new(SessionTempQuota::new(1, 1024, 100));
        let charge = quota.reserve(80, true).unwrap();
        let reader = charge.clone();
        assert!(quota.reserve(21, true).is_err());
        assert_eq!(quota.usage().2, 80);
        drop(charge);
        assert_eq!(quota.usage().2, 80);
        drop(reader);
        assert_eq!(quota.usage().2, 0);
        quota.reserve(100, true).unwrap();
        assert_eq!(quota.usage().2, 0);
    }
}

pub(crate) fn release_file(path: &Path) {
    if let Some(owner) = file_owner(path) {
        let bytes = owner.files.lock().remove(path).unwrap_or(0);
        let mut used = owner.quota.used.lock();
        used.storage = used.storage.saturating_sub(bytes);
    }
}
pub(crate) fn rename_file(old: &Path, new: &Path) {
    if let Some(owner) = file_owner(old) {
        let mut files = owner.files.lock();
        if let Some(bytes) = files.remove(old) {
            files.insert(new.to_path_buf(), bytes);
        }
    }
}
