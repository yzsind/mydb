//! `SET PERSIST` / `SET PERSIST_ONLY` durable backend.
//!
//! Holds the in-memory shadow of `<data_dir>/system/mysqld-auto.cnf` and
//! writes the file back on every mutation. The on-disk JSON format mirrors
//! MySQL 8.0's so an operator can copy the file between MySQL and mydb for
//! migration testing.
//!
//! Startup applies the persisted entries as GLOBAL values before the server
//! starts accepting connections — see [`apply_persisted_on_startup`].
//!
//! Concurrency: every mutation goes through `entries` (a `Mutex`), and the
//! write half flushes the whole map under the same lock. `performance_schema.persisted_variables`
//! takes a snapshot under the same lock, so readers never see torn entries.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::value::SysVarValue;

/// JSON wrapper for the on-disk file. MySQL writes
/// `{"Version": 1, "mysql_server": {...}}` and ignores unknown keys; we
/// follow the same convention so a file written by either side round-trips.
#[derive(Debug, Serialize, Deserialize)]
struct OnDisk {
    #[serde(rename = "Version")]
    version: u32,
    #[serde(rename = "mysql_server", default)]
    mysql_server: BTreeMap<String, OnDiskEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OnDiskEntry {
    #[serde(rename = "Value")]
    value: String,
    #[serde(rename = "Metadata", default)]
    metadata: OnDiskMeta,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct OnDiskMeta {
    #[serde(rename = "Timestamp", default)]
    timestamp: u64,
    #[serde(rename = "User", default)]
    user: String,
    #[serde(rename = "Host", default)]
    host: String,
}

const FILE_VERSION: u32 = 1;
const FILE_NAME: &str = "mysqld-auto.cnf";

#[derive(Debug, Clone)]
pub struct PersistedEntry {
    pub name: String,
    pub value: String,
    pub timestamp_ms: u64,
    pub user: String,
    pub host: String,
}

/// Owner of the persisted-variables file. One per `Database`.
#[derive(Debug)]
pub struct PersistedVariables {
    path: PathBuf,
    load_error: Option<String>,
    entries: Mutex<BTreeMap<String, PersistedEntry>>,
}

impl PersistedVariables {
    /// Best-effort open: a corrupt or unreadable file degrades to an empty
    /// in-memory store with a warning. Used during startup where we never
    /// want to refuse to boot just because the persisted file got mangled.
    pub fn open_or_default(system_dir: &Path) -> Arc<Self> {
        match Self::open(system_dir) {
            Ok(v) => v,
            Err(err) => {
                eprintln!(
                    "mydb: persisted-variables file at {:?} unreadable; persisted writes disabled until the file is repaired and the server restarted: {}",
                    system_dir.join(FILE_NAME),
                    err
                );
                Arc::new(Self {
                    path: system_dir.join(FILE_NAME),
                    load_error: Some(err.to_string()),
                    entries: Mutex::new(BTreeMap::new()),
                })
            }
        }
    }

    /// Open (or create) the persisted file. Missing file → empty map.
    /// Returns the loaded store; corrupt file is reported as an error so
    /// the operator can intervene rather than silently dropping persisted
    /// state.
    pub fn open(system_dir: &Path) -> io::Result<Arc<Self>> {
        let path = system_dir.join(FILE_NAME);
        let entries = if path.exists() {
            let raw = fs::read_to_string(&path)?;
            if raw.trim().is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "empty persisted variables file",
                ));
            } else {
                let on_disk: OnDisk = serde_json::from_str(&raw)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                if on_disk.version != FILE_VERSION {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unsupported persisted variables version",
                    ));
                }
                let mut entries = BTreeMap::new();
                for (name, entry) in on_disk.mysql_server {
                    let name = canonical_name(&name);
                    if entries.contains_key(&name) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "duplicate canonical persisted variable",
                        ));
                    }
                    entries.insert(
                        name.clone(),
                        PersistedEntry {
                            name,
                            value: entry.value,
                            timestamp_ms: entry.metadata.timestamp,
                            user: entry.metadata.user,
                            host: entry.metadata.host,
                        },
                    );
                }
                entries
            }
        } else {
            BTreeMap::new()
        };
        Ok(Arc::new(Self {
            path,
            load_error: None,
            entries: Mutex::new(entries),
        }))
    }

    #[cfg(test)]
    /// Insert / replace one persisted variable and rewrite the file.
    /// `value` is serialised through `SysVarValue::to_display_string()` so
    /// the file holds canonical MySQL text representations.
    pub fn upsert(
        &self,
        name: &str,
        value: &SysVarValue,
        user: &str,
        host: &str,
        timestamp_ms: u64,
    ) -> io::Result<()> {
        self.check_writable()?;
        let name = canonical_name(name);
        let mut guard = self.entries.lock().unwrap();
        let mut candidate = guard.clone();
        candidate.insert(
            name.to_string(),
            PersistedEntry {
                name: name.to_string(),
                value: value.to_display_string(),
                timestamp_ms,
                user: user.to_string(),
                host: host.to_string(),
            },
        );
        self.flush_locked(&candidate)?;
        *guard = candidate;
        Ok(())
    }

    /// Persist before changing runtime state. Serialize the file update and
    /// backend application so a failed disk write cannot change a live setting.
    pub fn upsert_and_apply<F>(
        &self,
        name: &str,
        value: &SysVarValue,
        user: &str,
        host: &str,
        timestamp_ms: u64,
        apply: F,
    ) -> Result<(), crate::types::QueryError>
    where
        F: FnOnce() -> Result<(), crate::types::QueryError>,
    {
        let io_error = |e| super::errors::persist_write_failed(name, e);
        self.check_writable().map_err(io_error)?;
        let mut guard = self.entries.lock().unwrap();
        let mut candidate = guard.clone();
        let canonical = canonical_name(name);
        candidate.insert(
            canonical.clone(),
            PersistedEntry {
                name: canonical,
                value: value.to_display_string(),
                timestamp_ms,
                user: user.to_string(),
                host: host.to_string(),
            },
        );
        self.flush_locked(&candidate).map_err(io_error)?;
        if let Err(error) = apply() {
            if let Err(restore) = self.flush_locked(&guard) {
                *guard = candidate;
                return Err(super::errors::persist_write_failed(name,
                    format!("runtime update failed: {}; restoring persisted configuration failed: {restore}", error.message())));
            }
            return Err(error);
        }
        *guard = candidate;
        Ok(())
    }

    /// Remove one entry. Returns true if it was present.
    pub fn remove(&self, name: &str) -> io::Result<bool> {
        self.check_writable()?;
        let mut guard = self.entries.lock().unwrap();
        let mut candidate = guard.clone();
        let removed = candidate.remove(&canonical_name(name)).is_some();
        if removed {
            self.flush_locked(&candidate)?;
            *guard = candidate;
        }
        Ok(removed)
    }

    /// Drop every persisted entry, but never overwrite an unreadable file.
    pub fn clear(&self) -> io::Result<()> {
        self.check_writable()?;
        let mut guard = self.entries.lock().unwrap();
        self.flush_locked(&BTreeMap::new())?;
        guard.clear();
        Ok(())
    }

    pub fn check_writable(&self) -> io::Result<()> {
        if let Some(error) = &self.load_error {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                format!("persisted configuration is unreadable ({error}); repair the file and restart before changing persisted variables")));
        }
        Ok(())
    }

    /// Snapshot of every persisted entry, sorted by variable name.
    pub fn snapshot(&self) -> Vec<PersistedEntry> {
        let guard = self.entries.lock().unwrap();
        guard.values().cloned().collect()
    }

    /// Look up a single entry, mostly for `RESET PERSIST IF EXISTS`.
    pub fn get(&self, name: &str) -> Option<PersistedEntry> {
        let guard = self.entries.lock().unwrap();
        guard.get(&canonical_name(name)).cloned()
    }

    /// On-disk file path (`<data_dir>/system/mysqld-auto.cnf`).
    pub fn file_path(&self) -> &Path {
        &self.path
    }

    fn flush_locked(&self, entries: &BTreeMap<String, PersistedEntry>) -> io::Result<()> {
        let mut mysql_server = BTreeMap::new();
        for (name, entry) in entries {
            mysql_server.insert(
                name.clone(),
                OnDiskEntry {
                    value: entry.value.clone(),
                    metadata: OnDiskMeta {
                        timestamp: entry.timestamp_ms,
                        user: entry.user.clone(),
                        host: entry.host.clone(),
                    },
                },
            );
        }
        let payload = serde_json::to_vec_pretty(&OnDisk {
            version: FILE_VERSION,
            mysql_server,
        })
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        // Crash-atomic: write to <file>.tmp, fsync data + parent dir, rename
        // over the target so a torn write never leaves a half-written file.
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("cnf.tmp");
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(&payload)?;
            crate::platform::fs::sync_standard_before_atomic_rename_at(&f, &tmp)?;
        }
        fs::rename(&tmp, &self.path)?;
        if let Some(parent) = self.path.parent() {
            // Directory fsync ensures the rename is durable.
            crate::platform::fs::sync_path(parent)?;
        }
        Ok(())
    }
}

fn canonical_name(name: &str) -> String {
    super::registry()
        .get(name)
        .map(|v| v.def.name.to_string())
        .unwrap_or_else(|| name.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn upsert_and_reload_round_trip() {
        let dir = tempdir().unwrap();
        let sys = dir.path().join("system");
        fs::create_dir_all(&sys).unwrap();
        let store = PersistedVariables::open(&sys).unwrap();
        store
            .upsert(
                "max_connections",
                &SysVarValue::UInt(1234),
                "root",
                "%",
                1_700_000_000_000,
            )
            .unwrap();
        let reopened = PersistedVariables::open(&sys).unwrap();
        let snap = reopened.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].name, "max_connections");
        assert_eq!(snap[0].value, "1234");
        assert_eq!(snap[0].user, "root");
    }

    #[test]
    fn remove_then_persist() {
        let dir = tempdir().unwrap();
        let sys = dir.path().join("system");
        fs::create_dir_all(&sys).unwrap();
        let store = PersistedVariables::open(&sys).unwrap();
        store
            .upsert("autocommit", &SysVarValue::Bool(false), "root", "%", 1)
            .unwrap();
        assert!(store.remove("autocommit").unwrap());
        let reopened = PersistedVariables::open(&sys).unwrap();
        assert!(reopened.snapshot().is_empty());
    }

    #[test]
    fn clear_removes_all() {
        let dir = tempdir().unwrap();
        let sys = dir.path().join("system");
        fs::create_dir_all(&sys).unwrap();
        let store = PersistedVariables::open(&sys).unwrap();
        store
            .upsert("a", &SysVarValue::Bool(true), "root", "%", 1)
            .unwrap();
        store
            .upsert("b", &SysVarValue::Bool(true), "root", "%", 1)
            .unwrap();
        store.clear().unwrap();
        let reopened = PersistedVariables::open(&sys).unwrap();
        assert!(reopened.snapshot().is_empty());
    }

    #[test]
    fn missing_file_is_empty() {
        let dir = tempdir().unwrap();
        let sys = dir.path().join("system");
        let store = PersistedVariables::open(&sys).unwrap();
        assert!(store.snapshot().is_empty());
    }
}

#[cfg(test)]
mod audit_tests {
    use super::*;

    fn dir() -> tempfile::TempDir {
        std::fs::create_dir_all("/tmp/mydb/sysvar-fix").unwrap();
        tempfile::tempdir_in("/tmp/mydb/sysvar-fix").unwrap()
    }

    #[test]
    fn corrupt_file_is_never_overwritten() {
        for bytes in [
            b"{broken".as_slice(),
            b"",
            br#"{"Version":99,"mysql_server":{}}"#,
        ] {
            let dir = dir();
            let path = dir.path().join(FILE_NAME);
            fs::write(&path, bytes).unwrap();
            let store = PersistedVariables::open_or_default(dir.path());
            assert!(store
                .upsert("wait_timeout", &SysVarValue::UInt(10), "root", "%", 1)
                .is_err());
            assert!(store.remove("wait_timeout").is_err());
            assert!(store.clear().is_err());
            assert!(store.snapshot().is_empty());
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }

    #[test]
    fn aliases_case_and_failed_writes_preserve_state() {
        let dir = dir();
        let store = PersistedVariables::open(dir.path()).unwrap();
        store
            .upsert(
                "TX_ISOLATION",
                &SysVarValue::String("SERIALIZABLE".into()),
                "root",
                "%",
                1,
            )
            .unwrap();
        assert_eq!(store.snapshot()[0].name, "transaction_isolation");
        assert!(store.get("Tx_Isolation").is_some());
        assert!(store.remove("TRANSACTION_ISOLATION").unwrap());
        store
            .upsert("wait_timeout", &SysVarValue::UInt(10), "root", "%", 1)
            .unwrap();
        let before = fs::read(store.file_path()).unwrap();
        fs::create_dir(store.file_path().with_extension("cnf.tmp")).unwrap();
        assert!(store
            .upsert("wait_timeout", &SysVarValue::UInt(20), "root", "%", 1)
            .is_err());
        assert!(store.remove("wait_timeout").is_err());
        assert!(store.clear().is_err());
        assert_eq!(store.get("wait_timeout").unwrap().value, "10");
        assert_eq!(fs::read(store.file_path()).unwrap(), before);
    }
}
