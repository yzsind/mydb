//! Database-scoped writer admission, maintenance capture and durable WAL leases.
use crate::types::QueryError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WalLease {
    pub floor: u64,
    pub history_retention_ms: u64,
    pub proof: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u32,
    leases: BTreeMap<String, WalLease>,
    digest: String,
}
pub(crate) struct WalBudget {
    pub pending: AtomicU64,
    pub limit: AtomicU64,
    pub enabled: AtomicBool,
    pub root: PathBuf,
    disk: Mutex<Option<crate::engine::executor::spill_quota::SpillCharger>>,
}
impl Default for WalBudget {
    fn default() -> Self {
        Self {
            pending: AtomicU64::new(0),
            limit: AtomicU64::new(16 * 1024 * 1024 * 1024),
            enabled: AtomicBool::new(false),
            root: PathBuf::new(),
            disk: Mutex::new(None),
        }
    }
}
impl std::fmt::Debug for WalBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalBudget")
            .field("pending", &self.pending.load(Ordering::Acquire))
            .field("limit", &self.limit.load(Ordering::Acquire))
            .finish()
    }
}
impl WalBudget {
    pub(crate) fn reserve_disk(
        &self,
        bytes: u64,
        administrative: bool,
    ) -> Result<Option<crate::engine::executor::spill_quota::PendingSpillWrite>, QueryError> {
        if !self.enabled.load(Ordering::Acquire) {
            return Ok(None);
        }
        let mut disk = self.disk.lock().unwrap_or_else(|e| e.into_inner());
        if disk.is_none() {
            *disk = Some(crate::engine::executor::spill_quota::charger_for_new_dir(
                &self.root,
            )?);
        }
        let reserve = if administrative {
            bytes
        } else {
            bytes
                .saturating_add(self.pending.load(Ordering::Acquire))
                .saturating_add(64 * 1024 * 1024)
        };
        disk.as_ref()
            .unwrap()
            .reserve_physical_write(reserve)
            .map(Some)
    }
    pub(crate) fn check(&self, bytes: u64) -> Result<(), QueryError> {
        if self.enabled.load(Ordering::Acquire)
            && self
                .pending
                .load(Ordering::Acquire)
                .checked_add(bytes)
                .is_none_or(|n| n > self.limit.load(Ordering::Acquire))
        {
            return Err(err("pending WAL budget exhausted; archive WAL, increase MAX_PENDING_WAL_BYTES, or explicitly stop protection"));
        }
        Ok(())
    }
    pub(crate) fn appended(&self, bytes: u64) {
        if self.enabled.load(Ordering::Acquire) {
            let _ = self
                .pending
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    Some(n.saturating_add(bytes))
                });
        }
    }
    pub(crate) fn acknowledged(&self, bytes: u64) {
        let _ = self
            .pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_sub(bytes))
            });
    }
}
#[derive(Debug)]
pub(crate) struct CaptureState {
    pub commit_pressure: Arc<CommitPressure>,
    pub budget: Arc<WalBudget>,
    pub active: Arc<AtomicBool>,
    pub frozen: AtomicBool,
    pub admissions: AtomicUsize,
    pub capture_floor: AtomicU64,
    pub archive_floor: Arc<AtomicU64>,
    registry: Mutex<BTreeMap<String, WalLease>>,
    path: PathBuf,
    owner: Mutex<Option<std::thread::ThreadId>>,
    wait: Mutex<()>,
    cv: Condvar,
}
fn err(s: impl Into<String>) -> QueryError {
    super::error(crate::error::catalog::MYDB_ER_BACKUP_FAILED, s)
}
impl CaptureState {
    pub(crate) fn open(root: &Path, read_only: bool) -> Result<Self, QueryError> {
        let path = root.join("system/wal-retention.json");
        if !read_only && !path.exists() && root.join("system/wal-retention.id").exists() {
            return Err(err("durable WAL lease state is missing"));
        }
        let leases = if !read_only && path.exists() {
            let mut bytes = Vec::new();
            {
                use std::io::Read;
                std::fs::File::open(&path)
                    .map_err(|e| err(e.to_string()))?
                    .take(64 * 1024 + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|e| err(e.to_string()))?;
            }
            if bytes.len() > 64 * 1024 {
                return Err(err("WAL lease state exceeds budget"));
            }
            let value: Envelope = serde_json::from_slice(&bytes).map_err(|e| err(e.to_string()))?;
            let encoded = serde_json::to_vec(&value.leases).map_err(|e| err(e.to_string()))?;
            if value.version != 1
                || value.leases.len() > 64
                || value
                    .leases
                    .values()
                    .any(|l| l.floor == 0 || l.floor == u64::MAX)
                || crate::util::hex::encode_lower(&Sha256::digest(&encoded)) != value.digest
            {
                return Err(err("invalid durable WAL leases"));
            }
            value.leases
        } else {
            BTreeMap::new()
        };
        let floor = leases.values().map(|l| l.floor).min().unwrap_or(0);
        Ok(Self {
            commit_pressure: Arc::new(CommitPressure::default()),
            budget: Arc::new(WalBudget {
                enabled: AtomicBool::new(floor != 0),
                root: root.to_path_buf(),
                ..Default::default()
            }),
            active: Arc::new(AtomicBool::new(false)),
            frozen: AtomicBool::new(false),
            admissions: AtomicUsize::new(0),
            capture_floor: AtomicU64::new(0),
            archive_floor: Arc::new(AtomicU64::new(floor)),
            registry: Mutex::new(leases),
            path,
            owner: Mutex::new(None),
            wait: Mutex::new(()),
            cv: Condvar::new(),
        })
    }
    pub(crate) fn protected_history_retention_ms(&self) -> Result<Option<u64>, QueryError> {
        let leases = self.registry.lock().unwrap_or_else(|e| e.into_inner());
        let mut policy = None;
        for lease in leases.values() {
            if policy.is_some_and(|value| value != lease.history_retention_ms) {
                return Err(err("conflicting protected temporal history contracts"));
            }
            policy = Some(lease.history_retention_ms);
        }
        Ok(policy)
    }
    pub(crate) fn floor(&self) -> Option<u64> {
        [
            self.capture_floor.load(Ordering::Acquire),
            self.archive_floor.load(Ordering::Acquire),
        ]
        .into_iter()
        .filter(|n| *n > 0)
        .min()
    }
    pub(crate) fn redo_required(&self) -> bool {
        self.active.load(Ordering::Acquire) || self.floor().is_some()
    }
    pub(crate) fn ensure_write(&self) -> Result<(), QueryError> {
        self.budget.check(1)?;
        self.ensure_barrier()
    }
    pub(crate) fn ensure_barrier(&self) -> Result<(), QueryError> {
        if self.frozen.load(Ordering::SeqCst)
            && *self.owner.lock().unwrap() != Some(std::thread::current().id())
        {
            return Err(super::error(
                crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                "write blocked by capture admission barrier",
            ));
        }
        Ok(())
    }
    pub(crate) fn wait(&self) -> Result<(), QueryError> {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut lock = self.wait.lock().unwrap();
        while self.frozen.load(Ordering::SeqCst) {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(super::error(
                    crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                    "capture admission wait timed out",
                ));
            }
            lock = self.cv.wait_timeout(lock, left).unwrap().0;
        }
        Ok(())
    }
    pub(crate) fn freeze(&self) -> FreezeGuard<'_> {
        *self.owner.lock().unwrap() = Some(std::thread::current().id());
        self.frozen.store(true, Ordering::SeqCst);
        FreezeGuard(self)
    }
    pub(crate) fn leases(&self) -> BTreeMap<String, WalLease> {
        self.registry.lock().unwrap().clone()
    }
    pub(crate) fn set_lease(&self, name: &str, lease: Option<WalLease>) -> Result<(), QueryError> {
        let mut map = self.registry.lock().unwrap();
        let mut next = map.clone();
        if let Some(lease) = lease {
            if lease.floor == 0 || lease.floor == u64::MAX {
                return Err(err("invalid WAL lease floor"));
            }
            next.insert(name.into(), lease);
        } else {
            next.remove(name);
        }
        if next.len() > 64 {
            return Err(err("too many WAL leases"));
        }
        let bytes = serde_json::to_vec(&next).map_err(|e| err(e.to_string()))?;
        let state = Envelope {
            version: 1,
            digest: crate::util::hex::encode_lower(&Sha256::digest(&bytes)),
            leases: next.clone(),
        };
        let bytes = serde_json::to_vec(&state).map_err(|e| err(e.to_string()))?;
        if bytes.len() > 64 * 1024 {
            return Err(err("WAL lease metadata exceeds budget"));
        }
        let parent = self.path.parent().unwrap();
        std::fs::create_dir_all(parent).map_err(|e| err(e.to_string()))?;
        let temp = parent.join(format!(".leases-{:032x}", rand::random::<u128>()));
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)
                .map_err(|e| err(e.to_string()))?;
            file.write_all(&bytes)
                .and_then(|_| file.sync_all())
                .map_err(|e| err(e.to_string()))?;
        }
        std::fs::rename(&temp, &self.path).map_err(|e| err(e.to_string()))?;
        crate::platform::fs::sync_path(parent).map_err(|e| err(e.to_string()))?;
        let marker = parent.join("wal-retention.id");
        if !marker.exists() {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&marker)
                .map_err(|e| err(e.to_string()))?;
            file.write_all(b"MYDB_WAL_LEASES/1")
                .and_then(|_| file.sync_all())
                .map_err(|e| err(e.to_string()))?;
            crate::platform::fs::sync_path(parent).map_err(|e| err(e.to_string()))?;
        }
        let old_floor = self.archive_floor.load(Ordering::Acquire);
        if (old_floor == 0 || (name == "archive" && !map.contains_key("archive")))
            && !next.is_empty()
        {
            self.budget.pending.store(0, Ordering::Release);
        }
        self.budget
            .enabled
            .store(!next.is_empty(), Ordering::Release);
        *map = next;
        self.archive_floor.store(
            map.values().map(|l| l.floor).min().unwrap_or(0),
            Ordering::Release,
        );
        Ok(())
    }
}
pub(crate) struct FreezeGuard<'a>(&'a CaptureState);
impl Drop for FreezeGuard<'_> {
    fn drop(&mut self) {
        let _wait = self.0.wait.lock().unwrap();
        self.0.frozen.store(false, Ordering::SeqCst);
        *self.0.owner.lock().unwrap() = None;
        self.0.cv.notify_all();
    }
}

#[derive(Default)]
pub(crate) struct DatabaseServices(
    Mutex<std::collections::HashMap<std::any::TypeId, Arc<dyn std::any::Any + Send + Sync>>>,
);
impl std::fmt::Debug for DatabaseServices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseServices").finish_non_exhaustive()
    }
}
impl DatabaseServices {
    pub(crate) fn get_or_init<T: Send + Sync + 'static>(
        &self,
        init: impl FnOnce() -> Result<Arc<T>, QueryError>,
    ) -> Result<Arc<T>, QueryError> {
        let mut services = self.0.lock().unwrap();
        let key = std::any::TypeId::of::<T>();
        if let Some(value) = services.get(&key) {
            return value
                .clone()
                .downcast()
                .map_err(|_| err("database service type mismatch"));
        }
        if services.len() >= 32 {
            return Err(err("database service budget exceeded"));
        }
        let value = init()?;
        services.insert(key, value.clone());
        Ok(value)
    }
}

struct LogicalApplyGate(Arc<parking_lot::RwLock<()>>);

/// Coordinate physical snapshot capture with complete logical source commits,
/// including deferred durability and temporal sidecar publication.
pub(crate) fn logical_apply_gate(
    db: &super::Database,
) -> Result<Arc<parking_lot::RwLock<()>>, QueryError> {
    db.services
        .get_or_init(|| {
            Ok(Arc::new(LogicalApplyGate(Arc::new(
                parking_lot::RwLock::new(()),
            ))))
        })
        .map(|gate| Arc::clone(&gate.0))
}

use super::Database;
pub(crate) struct Admission<'a>(&'a crate::storage::capture::CaptureState);
impl<'a> Admission<'a> {
    pub(crate) fn enter(db: &'a Database) -> Result<Self, QueryError> {
        db.capture.admissions.fetch_add(1, Ordering::SeqCst);
        let guard = Self(&db.capture);
        db.capture.ensure_write()?;
        Ok(guard)
    }
    pub(crate) fn control(db: &'a Database) -> Result<Self, QueryError> {
        db.capture.admissions.fetch_add(1, Ordering::SeqCst);
        let guard = Self(&db.capture);
        db.capture.ensure_barrier()?;
        Ok(guard)
    }
    pub(crate) fn for_active_writer(db: &'a Database, txn_id: u64) -> Result<Self, QueryError> {
        db.capture.admissions.fetch_add(1, Ordering::SeqCst);
        let guard = Self(&db.capture);
        if !db.is_active_write_transaction(txn_id)? {
            db.capture.ensure_write()?;
        }
        Ok(guard)
    }
}
impl Drop for Admission<'_> {
    fn drop(&mut self) {
        self.0.admissions.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug, Default)]
pub(crate) struct CommitPressure {
    pub enabled: AtomicBool,
    pub ewma_us: AtomicU64,
    pub last_unix_ms: AtomicU64,
}
pub(crate) struct CommitSample {
    pressure: Arc<CommitPressure>,
    started: Instant,
}
impl Drop for CommitSample {
    fn drop(&mut self) {
        let sample = self.started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        let _ = self
            .pressure
            .ewma_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                Some(if old == 0 {
                    sample
                } else {
                    old.saturating_mul(7).saturating_add(sample) / 8
                })
            });
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(0);
        self.pressure.last_unix_ms.store(now, Ordering::Relaxed);
    }
}
impl CaptureState {
    pub(crate) fn commit_sample(&self) -> Option<CommitSample> {
        self.commit_pressure
            .enabled
            .load(Ordering::Relaxed)
            .then(|| CommitSample {
                pressure: self.commit_pressure.clone(),
                started: Instant::now(),
            })
    }
}
