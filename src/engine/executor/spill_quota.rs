//! P0-3 — temp-disk quota for executor spill files.
//!
//! Spilling protects us from OOM, but unbounded spill just moves the
//! blow-up from RAM to the filesystem (cascading grace recursion, a
//! cross-join with a bad estimate, …). This module is the symmetric
//! guard: every spill writer charges the bytes it is about to write
//! against two live counters —
//!
//!   * a **process-global** counter (`GLOBAL_SPILLED`), and
//!   * a **per-query** counter living on the query's [`MemBudget`]
//!     (so it is released automatically when the query finishes).
//!
//! Charges are *live*: they are added when bytes are written and
//! subtracted when the owning [`SpillTmpDir`] is dropped (which removes
//! the whole partition subtree). So the global counter tracks the real
//! on-disk footprint at any instant, not a cumulative total — a query
//! that spills, consumes, and spills again in sequential phases is not
//! penalised for its peak being lower than its sum.
//!
//! Two ceilings are enforced before any charge is admitted:
//!   * `MYDB_TEMP_DISK_LIMIT` (bytes; 0 = derive from free space) — the
//!     process-wide cap, additionally clamped so we always leave
//!     `MYDB_TEMP_DISK_RESERVE_PCT` of the volume free (statvfs).
//!   * `MYDB_TEMP_FILE_LIMIT_PER_QUERY` (bytes; 0 = unlimited).
//!
//! Exceeding either raises a clean error (1041, "out of temp disk
//! space") that unwinds the query, rather than letting the write hit
//! `ENOSPC` and take the process down.

use crate::util::atomic_accounting::saturating_sub_u64 as atomic_saturating_sub;
use crate::util::atomic_accounting::try_add_u64 as try_atomic_add_bounded;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use crate::types::QueryError;

mod manifest;
use manifest::{FileReadLease, FileRecord, FileRegistry};

/// Live process-wide spill bytes currently on disk.
static GLOBAL_SPILLED: AtomicU64 = AtomicU64::new(0);
/// High-water mark for observability.
static GLOBAL_SPILLED_HWM: AtomicU64 = AtomicU64::new(0);
// Keep pending writes conservative across mounts: distinct filesystem paths
// may draw from the same backing storage pool. Never hold this lock during
// file reads or writes; only the space probe and admission are serialized.
static PENDING_VOLUME_WRITES: parking_lot::Mutex<u64> = parking_lot::Mutex::new(0);

#[must_use = "keep the reservation alive until the physical write completes"]
#[derive(Debug)]
pub(crate) struct PendingSpillWrite {
    bytes: u64,
}

impl Drop for PendingSpillWrite {
    fn drop(&mut self) {
        if self.bytes != 0 {
            let mut pending = PENDING_VOLUME_WRITES.lock();
            *pending = pending.saturating_sub(self.bytes);
        }
    }
}
/// Count of 1041 rejections, for the advisor view.
static QUOTA_REJECTIONS: AtomicU64 = AtomicU64::new(0);
static GLOBAL_QUOTA_REJECTIONS: AtomicU64 = AtomicU64::new(0);
static PER_QUERY_QUOTA_REJECTIONS: AtomicU64 = AtomicU64::new(0);
static ACTIVE_SPILL_FILES: AtomicU64 = AtomicU64::new(0);
static SPILL_FILE_OPENS: AtomicU64 = AtomicU64::new(0);
static ORPHAN_SPILL_BYTES: AtomicU64 = AtomicU64::new(0);
static SPILL_CLEANUP_FAILURES: AtomicU64 = AtomicU64::new(0);
static SORT_SPILLS: AtomicU64 = AtomicU64::new(0);
static HASH_JOIN_SPILLS: AtomicU64 = AtomicU64::new(0);
static HASH_AGG_SPILLS: AtomicU64 = AtomicU64::new(0);
static DISTINCT_SPILLS: AtomicU64 = AtomicU64::new(0);
static CTE_SPILLS: AtomicU64 = AtomicU64::new(0);
const CONFIG_UNINITIALIZED: u64 = u64::MAX;
static GLOBAL_LIMIT_CONFIG: AtomicU64 = AtomicU64::new(CONFIG_UNINITIALIZED);
static PER_QUERY_LIMIT_CONFIG: AtomicU64 = AtomicU64::new(CONFIG_UNINITIALIZED);
static RESERVE_PCT_CONFIG: AtomicU64 = AtomicU64::new(CONFIG_UNINITIALIZED);

fn parse_bytes_env(name: &str) -> Option<u64> {
    crate::util::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
}

/// `MYDB_TEMP_DISK_LIMIT` in bytes (0 / unset → derive from free space).
fn env_global_limit_bytes() -> u64 {
    let cur = GLOBAL_LIMIT_CONFIG.load(Ordering::Acquire);
    if cur != CONFIG_UNINITIALIZED {
        return cur;
    }
    let value = parse_bytes_env("MYDB_TEMP_DISK_LIMIT").unwrap_or(0);
    let _ = GLOBAL_LIMIT_CONFIG.compare_exchange(
        CONFIG_UNINITIALIZED,
        value,
        Ordering::AcqRel,
        Ordering::Acquire,
    );
    GLOBAL_LIMIT_CONFIG.load(Ordering::Acquire)
}

/// `MYDB_TEMP_DISK_RESERVE_PCT` — fraction of the volume to always keep
/// free (default 10 %). Guards against filling the disk even when no
/// explicit `MYDB_TEMP_DISK_LIMIT` is set.
fn env_reserve_pct() -> u64 {
    let cur = RESERVE_PCT_CONFIG.load(Ordering::Acquire);
    if cur != CONFIG_UNINITIALIZED {
        return cur;
    }
    let value = crate::util::env::var("MYDB_TEMP_DISK_RESERVE_PCT")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|v| *v <= 90)
        .unwrap_or(10);
    let _ = RESERVE_PCT_CONFIG.compare_exchange(
        CONFIG_UNINITIALIZED,
        value,
        Ordering::AcqRel,
        Ordering::Acquire,
    );
    RESERVE_PCT_CONFIG.load(Ordering::Acquire)
}

/// `MYDB_TEMP_FILE_LIMIT_PER_QUERY` in bytes (0 / unset → unlimited).
fn env_per_query_limit_bytes() -> u64 {
    let cur = PER_QUERY_LIMIT_CONFIG.load(Ordering::Acquire);
    if cur != CONFIG_UNINITIALIZED {
        return cur;
    }
    let value = parse_bytes_env("MYDB_TEMP_FILE_LIMIT_PER_QUERY").unwrap_or(0);
    let _ = PER_QUERY_LIMIT_CONFIG.compare_exchange(
        CONFIG_UNINITIALIZED,
        value,
        Ordering::AcqRel,
        Ordering::Acquire,
    );
    PER_QUERY_LIMIT_CONFIG.load(Ordering::Acquire)
}

pub fn global_limit_bytes() -> u64 {
    env_global_limit_bytes()
}

pub fn per_query_limit_bytes() -> u64 {
    env_per_query_limit_bytes()
}

pub fn reserve_pct() -> u64 {
    env_reserve_pct()
}

pub fn set_global_limit_bytes(value: u64) {
    GLOBAL_LIMIT_CONFIG.store(value, Ordering::Release);
}

pub fn set_per_query_limit_bytes(value: u64) {
    PER_QUERY_LIMIT_CONFIG.store(value, Ordering::Release);
}

pub fn set_reserve_pct(value: u64) -> Result<(), QueryError> {
    if value > 90 {
        return Err(crate::storage::error(
            crate::error::catalog::ER_WRONG_TYPE_FOR_VAR,
            "variable 'mydb_temp_disk_reserve_pct' must be between 0 and 90",
        ));
    }
    RESERVE_PCT_CONFIG.store(value, Ordering::Release);
    Ok(())
}

/// Free bytes available to an unprivileged writer on the volume backing
/// `path`, via `statvfs`. Returns `None` on platforms / errors where we
/// can't tell (in which case the byte-cap path still applies).
#[derive(Clone, Copy)]
struct VolumeSpace {
    available: u64,
    total: u64,
    allocation_unit: u64,
}

impl VolumeSpace {
    fn headroom(self, reserve_pct: u64) -> u64 {
        let reserve = (u128::from(self.total) * u128::from(reserve_pct) / 100) as u64;
        self.available.saturating_sub(reserve)
    }

    fn pending_bytes(self, bytes: u64) -> Option<u64> {
        let unit = self.allocation_unit.max(1);
        bytes.checked_add(unit - 1).map(|n| n / unit * unit)
    }
}

pub(crate) fn available_bytes(path: &Path) -> Option<u64> {
    volume_free_bytes(path).map(|space| space.available)
}

fn volume_free_bytes(path: &Path) -> Option<VolumeSpace> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let c = CString::new(path.as_os_str().as_bytes()).ok()?;
        // SAFETY: zeroed statvfs is a valid initial state; we only read
        // scalar fields after a successful call.
        let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::statvfs(c.as_ptr(), &mut st) };
        if rc != 0 {
            return None;
        }
        // f_bavail = blocks free to non-root; f_frsize = fragment size.
        let frsize = if st.f_frsize != 0 {
            st.f_frsize as u64
        } else {
            st.f_bsize as u64
        };
        Some(VolumeSpace {
            available: (st.f_bavail as u64).saturating_mul(frsize),
            total: (st.f_blocks as u64).saturating_mul(frsize),
            allocation_unit: frsize.max(1),
        })
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            GetDiskFreeSpaceExW, GetDiskFreeSpaceW, GetVolumePathNameW,
        };
        let absolute = std::fs::canonicalize(path).ok()?;
        let path = absolute
            .as_os_str()
            .encode_wide()
            .chain(Some(0))
            .collect::<Vec<_>>();
        let mut available = 0u64;
        let mut total = 0u64;
        let mut root = vec![0u16; path.len().saturating_add(2).max(261)];
        let mut sectors = 0u32;
        let mut sector_bytes = 0u32;
        let mut free_clusters = 0u32;
        let mut clusters = 0u32;
        // SAFETY: input and output UTF-16 buffers live through both calls;
        // every scalar output points to an appropriately sized local value.
        if unsafe {
            GetVolumePathNameW(
                path.as_ptr(),
                root.as_mut_ptr(),
                u32::try_from(root.len()).ok()?,
            )
        } == 0
            || unsafe {
                GetDiskFreeSpaceW(
                    root.as_ptr(),
                    &mut sectors,
                    &mut sector_bytes,
                    &mut free_clusters,
                    &mut clusters,
                )
            } == 0
        {
            return None;
        }
        // SAFETY: path is a live nul-terminated UTF-16 string. The available
        // bytes output is correctly sized; optional outputs are null.
        (unsafe {
            GetDiskFreeSpaceExW(
                path.as_ptr(),
                &mut available,
                &mut total,
                std::ptr::null_mut(),
            )
        } != 0)
            .then_some(VolumeSpace {
                available,
                total,
                allocation_unit: u64::from(sectors)
                    .saturating_mul(u64::from(sector_bytes))
                    .max(1),
            })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        None
    }
}

/// The effective global ceiling right now, given the volume backing
/// `data_dir`: the smaller of the configured byte cap and "current
/// free minus the reserve fraction". Recomputed cheaply at each spill
/// dir creation so it tracks external disk pressure too.
fn effective_global_limit(data_dir: &Path) -> u64 {
    let configured = env_global_limit_bytes();
    let free = volume_free_bytes(data_dir);
    let from_free = free.map(|space| {
        // already-charged bytes live in our temp files, so the volume's
        // current free already reflects them; the headroom we may still
        // take is `free - reserve`.
        let headroom = space
            .headroom(env_reserve_pct())
            .saturating_sub(*PENDING_VOLUME_WRITES.lock());
        // Add what we already hold so the cap is expressed in the same
        // "live global" units the counter uses.
        headroom.saturating_add(GLOBAL_SPILLED.load(Ordering::Relaxed))
    });
    match (configured, from_free) {
        (0, Some(f)) => f,
        (0, None) => u64::MAX,
        (c, Some(f)) => c.min(f),
        (c, None) => c,
    }
}

#[derive(Clone, Copy)]
enum QuotaScope {
    Global,
    PerQuery,
    Volume,
}

fn quota_error(requested: u64, scope: QuotaScope, used: u64, limit: u64) -> QueryError {
    QUOTA_REJECTIONS.fetch_add(1, Ordering::Relaxed);
    let scope_name = match scope {
        QuotaScope::Global => {
            GLOBAL_QUOTA_REJECTIONS.fetch_add(1, Ordering::Relaxed);
            "global temp-disk"
        }
        QuotaScope::PerQuery => {
            PER_QUERY_QUOTA_REJECTIONS.fetch_add(1, Ordering::Relaxed);
            "per-query temp-file"
        }
        QuotaScope::Volume => {
            GLOBAL_QUOTA_REJECTIONS.fetch_add(1, Ordering::Relaxed);
            "spill filesystem"
        }
    };
    // A zero effective global ceiling comes from the filesystem probe:
    // configured zero means auto, not "disable spilling". Do not suggest
    // increasing a byte quota when the filesystem has no usable headroom.
    let message = if matches!(scope, QuotaScope::Volume) {
        format!("out of temp disk space: spill filesystem has {limit} bytes of headroom after the volume reserve, with {used} bytes reserved for pending writes; cannot reserve {requested} additional bytes. Free disk space or move the data directory to a filesystem with available space.")
    } else if matches!(scope, QuotaScope::Global) && limit == 0 {
        "out of temp disk space: no space available for spill on the temporary-file \
         filesystem (effective global temp-disk limit: 0 bytes). Free disk space or \
         move the data directory to a filesystem with available space. \
         Filesystem-reserved blocks are excluded even when running as root; \
         increasing MYDB_TEMP_DISK_LIMIT cannot override filesystem availability."
            .to_string()
    } else {
        let operation = if requested == 0 {
            "cannot create spill directory: no quota headroom remains".to_string()
        } else {
            format!("spill write of {requested} bytes would exceed the limit")
        };
        format!(
            "out of temp disk space: {operation} ({scope_name}: {used} / {limit} bytes in use). \
             Free disk space, reduce the query's spill footprint, or raise \
             MYDB_TEMP_DISK_LIMIT / MYDB_TEMP_FILE_LIMIT_PER_QUERY if a configured \
             quota is the constraint."
        )
    };
    crate::storage::error(crate::error::catalog::ER_OUT_OF_RESOURCES, message)
}

/// Per-spill-directory charger. Cloned cheaply into every writer under a
/// [`crate::engine::executor::select::grace::SpillTmpDir`]. Tracks the
/// bytes this directory has charged so they can be released wholesale
/// when the directory (and its files) are removed.
#[derive(Clone)]
pub(crate) struct SpillCharger {
    inner: Arc<ChargerInner>,
}

struct ChargerInner {
    /// Volume used for the dynamic free-space check. Unlike the old
    /// create-time snapshot, this is consulted for every physical write.
    data_dir: PathBuf,
    #[cfg(test)]
    limit_override: Option<u64>,
    /// Query that created the spill directory. Release must always debit
    /// this owner even when the directory crosses a worker-thread boundary.
    owner: Option<Arc<crate::engine::executor::mem_budget::MemBudget>>,
    /// Bytes charged through this charger (== bytes to release on drop).
    dir_bytes: AtomicU64,
    files: parking_lot::Mutex<FileRegistry>,
    lifecycle: parking_lot::Mutex<DirectoryLifecycle>,
    writeback_slot: AtomicUsize,
}

const WRITEBACK_DIRECTORY_SLOTS: usize = 256;

/// Bounded weak discovery metadata; it never retains a query, directory or
/// descriptor. Normal writes perform no registry lookup or writeback syscall.
fn writeback_directories(
) -> &'static [parking_lot::Mutex<Weak<ChargerInner>>; WRITEBACK_DIRECTORY_SLOTS] {
    static DIRECTORIES: OnceLock<
        [parking_lot::Mutex<Weak<ChargerInner>>; WRITEBACK_DIRECTORY_SLOTS],
    > = OnceLock::new();
    DIRECTORIES.get_or_init(|| std::array::from_fn(|_| parking_lot::Mutex::new(Weak::new())))
}

impl Drop for ChargerInner {
    fn drop(&mut self) {
        let index = self.writeback_slot.load(Ordering::Relaxed);
        if index < WRITEBACK_DIRECTORY_SLOTS {
            let mut slot = writeback_directories()[index].lock();
            if std::ptr::eq(slot.as_ptr(), self) {
                *slot = Weak::new();
            }
        }
    }
}

pub(super) struct WritebackBudget {
    bytes: u64,
    files: usize,
}

impl WritebackBudget {
    pub(super) fn new() -> Self {
        Self {
            bytes: 64 * 1024 * 1024,
            files: 256,
        }
    }
}

/// Request only spill I/O needed by physical-admission backpressure.
/// A locked manifest submits its own I/O through its recoverable reservation.
pub(crate) fn request_process_writeback() {
    let mut live: [Option<Arc<ChargerInner>>; WRITEBACK_DIRECTORY_SLOTS] =
        std::array::from_fn(|_| None);
    {
        for (slot, weak) in live.iter_mut().zip(writeback_directories()) {
            let upgraded = { weak.lock().upgrade() };
            *slot = upgraded;
        }
    }
    let mut budget = WritebackBudget::new();
    for inner in live.into_iter().flatten() {
        if budget.bytes == 0 || budget.files == 0 {
            break;
        }
        let charger = SpillCharger { inner };
        let Ok(lease) = charger.acquire_file() else {
            continue;
        };
        if let Some(files) = charger.inner.files.try_lock() {
            files.request_writeback(&mut budget, &lease);
        }
        // Release the manifest before the last lease can retire a directory.
        drop(lease);
    }
}

#[derive(Default)]
struct DirectoryLifecycle {
    open_files: usize,
    retired: bool,
    path: Option<PathBuf>,
}

struct SpillFileLease {
    charger: SpillCharger,
}

/// Raw positioned-I/O consumers retain the directory until their final shared
/// descriptor closes, just like the SQL codec readers and writers.
pub(crate) struct LeasedSpillFile {
    file: File,
    _lease: SpillFileLease,
}

impl std::ops::Deref for LeasedSpillFile {
    type Target = File;
    fn deref(&self) -> &File {
        &self.file
    }
}

impl Drop for SpillFileLease {
    fn drop(&mut self) {
        let path = {
            let mut lifecycle = self.charger.inner.lifecycle.lock();
            lifecycle.open_files -= 1;
            (lifecycle.open_files == 0)
                .then(|| lifecycle.path.take())
                .flatten()
        };
        ACTIVE_SPILL_FILES.fetch_sub(1, Ordering::Relaxed);
        if let Some(path) = path {
            cleanup_retired_directory(path, self.charger.clone());
        }
    }
}

struct OrphanDirectory {
    path: PathBuf,
    charger: SpillCharger,
    bytes: u64,
}

fn orphan_directories() -> &'static parking_lot::Mutex<Vec<OrphanDirectory>> {
    static ORPHANS: std::sync::OnceLock<parking_lot::Mutex<Vec<OrphanDirectory>>> =
        std::sync::OnceLock::new();
    ORPHANS.get_or_init(Default::default)
}

fn delete_directory(path: &Path, charger: &SpillCharger) -> bool {
    match std::fs::remove_dir_all(path) {
        Ok(()) => {
            release_dir(charger);
            true
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            release_dir(charger);
            true
        }
        Err(_) => false,
    }
}

fn cleanup_retired_directory(path: PathBuf, charger: SpillCharger) {
    if !delete_directory(&path, &charger) {
        let bytes = charger.dir_bytes();
        record_cleanup_failure(bytes);
        orphan_directories().lock().push(OrphanDirectory {
            path,
            charger,
            bytes,
        });
    }
}

/// Retry a bounded number of query-owned orphans outside the registry lock.
pub(crate) fn retry_orphan_cleanup() {
    let pending = {
        let mut orphans = orphan_directories().lock();
        let count = orphans.len().min(4);
        let start = orphans.len() - count;
        orphans.drain(start..).collect::<Vec<_>>()
    };
    for orphan in pending {
        if delete_directory(&orphan.path, &orphan.charger) {
            atomic_saturating_sub(&ORPHAN_SPILL_BYTES, orphan.bytes);
        } else {
            orphan_directories().lock().push(orphan);
        }
    }
}

impl SpillCharger {
    pub(crate) fn lease_file(&self, file: File) -> Result<LeasedSpillFile, QueryError> {
        let lease = self.acquire_file()?;
        SPILL_FILE_OPENS.fetch_add(1, Ordering::Relaxed);
        Ok(LeasedSpillFile {
            file,
            _lease: lease,
        })
    }

    /// Wrap a fresh raw stream without imposing a codec or cache advice.
    fn raw_writer(&self, mut file: File) -> Result<QuotaFile, QueryError> {
        if file.metadata().map_err(spill_io_error)?.len() != 0 {
            return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                .error("raw spill writer must start with an empty file"));
        }
        file.seek(SeekFrom::Start(0)).map_err(spill_io_error)?;
        let lease = self.acquire_file()?;
        SPILL_FILE_OPENS.fetch_add(1, Ordering::Relaxed);
        Ok(QuotaFile {
            file,
            charger: self.clone(),
            position: 0,
            file_len: 0,
            record: None,
            end_write_on_drop: false,
            _lease: lease,
        })
    }

    /// Logical credit for buffered/random writers. Actual positioned writes
    /// must separately retain reserve_physical_write until completion.
    pub(crate) fn charge_buffered_extent(&self, bytes: u64) -> Result<(), QueryError> {
        drop(self.reserve_write(bytes, bytes)?);
        Ok(())
    }

    pub(crate) fn reserve_physical_write(
        &self,
        bytes: u64,
    ) -> Result<PendingSpillWrite, QueryError> {
        self.reserve_write(0, bytes)
    }

    fn acquire_file(&self) -> Result<SpillFileLease, QueryError> {
        let mut lifecycle = self.inner.lifecycle.lock();
        if lifecycle.retired {
            return Err(
                crate::error::catalog::ER_UNKNOWN_ERROR.error("spill directory has already closed")
            );
        }
        lifecycle.open_files += 1;
        ACTIVE_SPILL_FILES.fetch_add(1, Ordering::Relaxed);
        Ok(SpillFileLease {
            charger: self.clone(),
        })
    }

    pub(crate) fn retire_directory(&self, path: PathBuf) {
        let remove_now = {
            let mut lifecycle = self.inner.lifecycle.lock();
            if lifecycle.retired {
                return;
            }
            lifecycle.retired = true;
            if lifecycle.open_files == 0 {
                true
            } else {
                lifecycle.path = Some(path.clone());
                false
            }
        };
        if remove_now {
            cleanup_retired_directory(path, self.clone());
        }
    }
    pub(crate) fn owner_query_id(&self) -> Option<u64> {
        self.inner.owner.as_ref().map(|owner| owner.query_id())
    }

    pub(crate) fn dir_bytes(&self) -> u64 {
        self.inner.dir_bytes.load(Ordering::Acquire)
    }
    #[cfg(test)]
    pub(crate) fn charge(&self, n: u64) -> Result<PendingSpillWrite, QueryError> {
        self.reserve_write(n, n)
    }

    /// Reserve logical extension quota and physical pending write space.
    /// Overwrites can allocate blocks too (for example on a CoW filesystem),
    /// even though their logical live-byte quota does not grow.
    pub(crate) fn reserve_write(
        &self,
        n: u64,
        write_bytes: u64,
    ) -> Result<PendingSpillWrite, QueryError> {
        let write_bytes = write_bytes.max(n);
        if write_bytes == 0 {
            return Ok(PendingSpillWrite { bytes: 0 });
        }
        // The probe and pending admission form one short critical section.
        // The returned token outlives this lock and covers the kernel write.
        let configured = env_global_limit_bytes();
        let per_query_limit = env_per_query_limit_bytes();
        let reserve_pct = env_reserve_pct();
        let mut pending = PENDING_VOLUME_WRITES.lock();
        let space = volume_free_bytes(&self.inner.data_dir);
        #[cfg(test)]
        let override_limit = self.inner.limit_override;
        #[cfg(not(test))]
        let override_limit: Option<u64> = None;
        let (volume_bytes, detected_limit) = if let Some(space) =
            space.filter(|_| override_limit.is_none())
        {
            let headroom = space.headroom(reserve_pct);
            let volume_bytes = space.pending_bytes(write_bytes).unwrap_or(u64::MAX);
            if volume_bytes > headroom.saturating_sub(*pending) {
                return Err(quota_error(
                    volume_bytes,
                    QuotaScope::Volume,
                    *pending,
                    headroom,
                ));
            }
            (
                volume_bytes,
                GLOBAL_SPILLED
                    .load(Ordering::Acquire)
                    .saturating_add(headroom.saturating_sub(*pending)),
            )
        } else {
            if configured == 0 && override_limit.is_none() {
                return Err(crate::error::catalog::ER_UNKNOWN_ERROR.error("cannot determine spill filesystem capacity; restore filesystem space probing or configure MYDB_TEMP_DISK_LIMIT as an explicit byte cap"));
            }
            (write_bytes, u64::MAX)
        };
        let next_pending = pending
            .checked_add(volume_bytes)
            .ok_or_else(|| quota_error(volume_bytes, QuotaScope::Volume, *pending, u64::MAX))?;
        let global_limit = override_limit.unwrap_or_else(|| {
            if configured == 0 {
                detected_limit
            } else {
                configured.min(detected_limit)
            }
        });
        if n == 0 {
            *pending = next_pending;
            return Ok(PendingSpillWrite {
                bytes: volume_bytes,
            });
        }
        // Per-query ceiling (only when a query budget is in scope).
        if let Some(b) = &self.inner.owner {
            let cur = b.spill_bytes();
            if !b.try_add_spill_bytes(n, per_query_limit) {
                return Err(quota_error(n, QuotaScope::PerQuery, cur, per_query_limit));
            }
        }
        // Global ceiling.
        let Some(now) = try_atomic_add_bounded(&GLOBAL_SPILLED, n, global_limit) else {
            // Roll back and reject.
            if let Some(b) = &self.inner.owner {
                b.sub_spill_bytes(n);
            }
            return Err(quota_error(
                n,
                QuotaScope::Global,
                GLOBAL_SPILLED.load(Ordering::Acquire),
                global_limit,
            ));
        };
        // Track high-water mark.
        let mut hwm = GLOBAL_SPILLED_HWM.load(Ordering::Relaxed);
        while now > hwm {
            match GLOBAL_SPILLED_HWM.compare_exchange_weak(
                hwm,
                now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(o) => hwm = o,
            }
        }
        if try_atomic_add_bounded(&self.inner.dir_bytes, n, u64::MAX).is_none() {
            atomic_saturating_sub(&GLOBAL_SPILLED, n);
            if let Some(b) = &self.inner.owner {
                b.sub_spill_bytes(n);
            }
            return Err(quota_error(n, QuotaScope::Global, u64::MAX, u64::MAX));
        }
        *pending = next_pending;
        Ok(PendingSpillWrite {
            bytes: volume_bytes,
        })
    }

    /// Release `n` bytes previously charged through this charger — used
    /// by operators that delete spill files mid-life (e.g. external
    /// sort's multi-pass merge, which removes consumed runs before the
    /// directory itself is dropped). Keeps the live counters accurate
    /// instead of waiting for the wholesale `release_dir` at drop.
    pub(crate) fn release(&self, n: u64) {
        if n == 0 {
            return;
        }
        let n = atomic_saturating_sub(&self.inner.dir_bytes, n);
        if n == 0 {
            return;
        }
        atomic_saturating_sub(&GLOBAL_SPILLED, n);
        if let Some(b) = &self.inner.owner {
            b.sub_spill_bytes(n);
        }
    }
}

#[derive(Debug)]
struct SpillQuotaIoError(QueryError);

impl std::fmt::Display for SpillQuotaIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.message().fmt(f)
    }
}

impl std::error::Error for SpillQuotaIoError {}

pub(crate) fn quota_as_io(error: QueryError) -> io::Error {
    let kind = if error.code() == 1037 {
        io::ErrorKind::OutOfMemory
    } else {
        io::ErrorKind::StorageFull
    };
    io::Error::new(kind, SpillQuotaIoError(error))
}

/// Recover a quota error transported through `std::io::Write`/`BufWriter`.
/// Callers should use this before mapping ordinary filesystem errors to a
/// catalogued I/O condition.
pub(crate) fn query_error_from_io(error: &io::Error) -> Option<QueryError> {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<SpillQuotaIoError>())
        .map(|wrapped| wrapped.0.clone())
        .or_else(|| match error.kind() {
            io::ErrorKind::OutOfMemory => {
                Some(crate::engine::executor::mem_budget::allocation_error(
                    format!("spill allocation: {error}"),
                ))
            }
            io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded => Some(
                crate::error::catalog::ER_OUT_OF_RESOURCES
                    .error(format!("out of temp disk space: {error}")),
            ),
            _ => None,
        })
}

fn spill_io_error(error: io::Error) -> QueryError {
    query_error_from_io(&error).unwrap_or_else(|| {
        crate::error::catalog::ER_UNKNOWN_ERROR.error(format!("spill I/O: {error}"))
    })
}

struct SpillBufferCharge {
    owner: Option<Arc<crate::engine::executor::mem_budget::MemBudget>>,
    bytes: usize,
}

impl SpillBufferCharge {
    fn new(
        owner: Option<Arc<crate::engine::executor::mem_budget::MemBudget>>,
        bytes: usize,
    ) -> Result<Self, QueryError> {
        if let Some(budget) = &owner {
            // Spill may be entered after a compatibility operator has already
            // crossed its query soft cap. Keep the buffer attributed to the
            // original owner, but allow it to borrow only while the process
            // hard limit still has room. This is the same bounded migration
            // rule as `force_charge`; it never bypasses the process ceiling.
            budget.try_charge_spill_buffer(bytes)?;
        }
        Ok(Self { owner, bytes })
    }

    fn required(
        owner: Option<Arc<crate::engine::executor::mem_budget::MemBudget>>,
        bytes: usize,
    ) -> Result<Self, QueryError> {
        if let Some(budget) = &owner {
            // Checksum replay cannot use direct I/O: it must validate one
            // whole block before exposing any bytes. Reclaim optional caches
            // and allocator pages before rejecting this bounded workspace.
            budget.force_charge_recoverable(bytes)?;
        }
        Ok(Self { owner, bytes })
    }

    /// A spill buffer is optional working memory. A rejected large buffer
    /// must not cancel the query that is trying to release resident state.
    /// Retain a small buffer if admitted, otherwise use direct file I/O.
    fn adaptive(
        owner: Option<Arc<crate::engine::executor::mem_budget::MemBudget>>,
        desired: usize,
    ) -> Self {
        for capacity in [desired, desired.min(8 * 1024), 0] {
            if let Ok(charge) = Self::new(owner.clone(), capacity) {
                return charge;
            }
            // The rejected capacity is optional. Let its owner return cached
            // pages, then try a smaller buffer instead of blocking all spill
            // workers behind the process-wide reclamation admission lock.
            crate::engine::memory_reclaimer::reclaim_spill_allocator_if_requested();
        }
        unreachable!("zero-byte reservations always succeed")
    }
}

impl Drop for SpillBufferCharge {
    fn drop(&mut self) {
        if let Some(owner) = &self.owner {
            owner.release(self.bytes);
        }
    }
}

struct QuotaFile {
    file: File,
    charger: SpillCharger,
    position: u64,
    file_len: u64,
    record: Option<Arc<FileRecord>>,
    end_write_on_drop: bool,
    _lease: SpillFileLease,
}

impl Drop for QuotaFile {
    fn drop(&mut self) {
        if self.end_write_on_drop {
            if let Some(record) = &self.record {
                record.end_write();
            }
        }
    }
}

/// Quota-accounted temporary file used by protocol producers such as
/// LOAD DATA LOCAL. Unlike executor partition spills it needs only one file,
/// but it shares the same global/per-query disk ceilings and cleanup counters.
pub(crate) struct ProtocolSpillFile {
    inner: QuotaFile,
    path: PathBuf,
    charger: SpillCharger,
}

impl ProtocolSpillFile {
    pub(crate) fn create(data_dir: &Path, connection_id: u32) -> Result<Self, QueryError> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let dir = crate::storage::naming::system_tmp_dir(data_dir).join("protocol");
        std::fs::create_dir_all(&dir).map_err(|err| {
            crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                format!("protocol spill directory '{}': {err}", dir.display()),
            )
        })?;
        let charger = charger_for_new_dir(&dir)?;
        for _ in 0..32 {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let path = dir.join(format!("local-infile-{connection_id}-{id}.tmp"));
            let lease = charger.acquire_file()?;
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => {
                    SPILL_FILE_OPENS.fetch_add(1, Ordering::Relaxed);
                    return Ok(Self {
                        inner: QuotaFile {
                            file,
                            charger: charger.clone(),
                            position: 0,
                            file_len: 0,
                            record: None,
                            end_write_on_drop: false,
                            _lease: lease,
                        },
                        path,
                        charger,
                    });
                }
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(err) => {
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        format!("protocol spill file '{}': {err}", path.display()),
                    ));
                }
            }
        }
        Err(crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "unable to allocate a unique protocol spill file",
        ))
    }

    pub(crate) fn rewind(&mut self) -> Result<(), QueryError> {
        self.inner.file.flush().map_err(|err| {
            query_error_from_io(&err).unwrap_or_else(|| {
                crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    format!("protocol spill flush: {err}"),
                )
            })
        })?;
        self.inner.seek(SeekFrom::Start(0)).map_err(|err| {
            crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                format!("protocol spill rewind: {err}"),
            )
        })?;
        Ok(())
    }
}

impl Write for ProtocolSpillFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let offset = self.inner.position;
        // Protocol LOAD execution can run outside a SELECT query-budget scope.
        // It owns one input file, so enforce that file's per-query ceiling even
        // without an executor owner. Scoped producers retain aggregate charging.
        if self.charger.inner.owner.is_none() {
            let limit = env_per_query_limit_bytes();
            let extension = offset
                .saturating_add(buf.len() as u64)
                .saturating_sub(self.inner.file_len);
            if limit != 0 && self.inner.file_len.saturating_add(extension) > limit {
                return Err(quota_as_io(quota_error(
                    extension,
                    QuotaScope::PerQuery,
                    self.inner.file_len,
                    limit,
                )));
            }
        }
        let written = self.inner.write(buf)?;
        // Replayable bulk input can be larger than the database's cgroup.
        // Submit temporary-file writeback as it arrives; durability belongs
        // to the published database generation, not to this replay copy.
        super::spill_hint::hint_writeback(&self.inner.file, offset, written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl Read for ProtocolSpillFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.file.read(buf)?;
        self.inner.position = self.inner.position.saturating_add(read as u64);
        crate::engine::executor::spill_hint::record_read(read as u64);
        Ok(read)
    }
}

impl Drop for ProtocolSpillFile {
    fn drop(&mut self) {
        crate::engine::executor::spill_hint::hint_dontneed(&self.inner.file);
        if std::fs::remove_file(&self.path).is_err() {
            SPILL_CLEANUP_FAILURES.fetch_add(1, Ordering::Relaxed);
        }
        release_dir(&self.charger);
    }
}

impl Write for QuotaFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if let Some(record) = &self.record {
            record
                .reserve_write(self.position, buf.len())
                .map_err(quota_as_io)?;
        }
        let requested = u64::try_from(buf.len()).unwrap_or(u64::MAX);
        let requested_end = self.position.saturating_add(requested);
        let reserved_extension = requested_end.saturating_sub(self.file_len);
        let reservation = self
            .charger
            .reserve_write(reserved_extension, requested)
            .map_err(quota_as_io)?;
        let result = self.file.write(buf);
        drop(reservation);
        match result {
            Ok(written) => {
                let written_u64 = written as u64;
                let written_end = self.position.saturating_add(written_u64);
                let actual_extension = written_end.saturating_sub(self.file_len);
                if actual_extension < reserved_extension {
                    self.charger.release(reserved_extension - actual_extension);
                }
                self.position = written_end;
                self.file_len = self.file_len.max(written_end);
                if let Some(record) = &self.record {
                    record.wrote(written_end - written_u64, &buf[..written], actual_extension);
                }
                crate::engine::executor::spill_hint::record_written(written_u64);
                Ok(written)
            }
            Err(error) => {
                self.charger.release(reserved_extension);
                Err(error)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl Seek for QuotaFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let position = self.file.seek(pos)?;
        self.position = position;
        Ok(position)
    }
}

/// Quota-aware buffered spill writer. Quota is reserved immediately before
/// bytes reach the kernel; a rejected reservation therefore cannot extend the
/// file. The buffer's resident memory is held against the creating query.
pub(crate) struct TrackedSpillWriter {
    writer: Option<BufWriter<QuotaFile>>,
    initial_len: u64,
    retain_cache: bool,
    _buffer_charge: SpillBufferCharge,
}

impl TrackedSpillWriter {
    /// Existing raw formats can share the same fallible buffer, quota and
    /// abort lifetime without changing their bytes or evicting their cache.
    pub(crate) fn from_raw_file(
        file: File,
        charger: SpillCharger,
        capacity: usize,
    ) -> Result<Self, QueryError> {
        let buffer_charge = SpillBufferCharge::adaptive(charger.inner.owner.clone(), capacity);
        let file = charger.raw_writer(file)?;
        Ok(Self {
            writer: Some(BufWriter::with_capacity(buffer_charge.bytes, file)),
            initial_len: 0,
            retain_cache: true,
            _buffer_charge: buffer_charge,
        })
    }

    pub(crate) fn create_new(
        path: &Path,
        charger: SpillCharger,
        buffer_capacity: usize,
    ) -> Result<Self, QueryError> {
        Self::create(path, charger, buffer_capacity, true, false)
    }

    pub(crate) fn create_truncate(
        path: &Path,
        charger: SpillCharger,
        buffer_capacity: usize,
    ) -> Result<Self, QueryError> {
        Self::create(path, charger, buffer_capacity, false, false)
    }

    /// Open an existing partition file (or create it) and append another
    /// complete spill batch. Only newly appended bytes are quota-charged and
    /// returned by `finish`.
    pub(crate) fn open_append(
        path: &Path,
        charger: SpillCharger,
        buffer_capacity: usize,
    ) -> Result<Self, QueryError> {
        Self::create(path, charger, buffer_capacity, false, true)
    }

    fn create(
        path: &Path,
        charger: SpillCharger,
        buffer_capacity: usize,
        create_new: bool,
        append: bool,
    ) -> Result<Self, QueryError> {
        let lease = charger.acquire_file()?;
        let file = OpenOptions::new()
            .create(true)
            .create_new(create_new)
            .append(append)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|error| {
                crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    format!("spill I/O: {error}"),
                )
            })?;
        crate::engine::executor::spill_hint::hint_sequential(&file);
        let length = file.metadata().map_err(spill_io_error)?.len();
        let record = charger.register_file(path)?;
        record.begin_write(append, length).map_err(spill_io_error)?;
        if !append {
            if let Err(error) = file.set_len(0) {
                record.end_write();
                return Err(spill_io_error(error));
            }
            record.truncated();
        }
        let initial_len = if append { length } else { 0 };
        let owner_cap = charger
            .inner
            .owner
            .as_ref()
            .map(|owner| (owner.limit() / 8).max(32 * 1024))
            .unwrap_or(1024 * 1024);
        let capacity = buffer_capacity.min(owner_cap).clamp(32 * 1024, 1024 * 1024);
        let buffer_charge = SpillBufferCharge::adaptive(charger.inner.owner.clone(), capacity);
        let capacity = buffer_charge.bytes;
        SPILL_FILE_OPENS.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            writer: Some(BufWriter::with_capacity(
                capacity,
                QuotaFile {
                    file,
                    charger,
                    position: initial_len,
                    file_len: initial_len,
                    record: Some(record),
                    end_write_on_drop: true,
                    _lease: lease,
                },
            )),
            initial_len,
            retain_cache: false,
            _buffer_charge: buffer_charge,
        })
    }

    pub(crate) fn bytes_written(&self) -> u64 {
        self.writer
            .as_ref()
            .expect("live spill writer")
            .get_ref()
            .file_len
    }

    /// Bytes already accepted by this writer, including the tail still held
    /// in its query-charged userspace buffer. This is useful for persistent
    /// partition writers whose per-append metrics must not force a flush.
    pub(crate) fn logical_bytes_written(&self) -> u64 {
        self.bytes_written().saturating_add(
            self.writer
                .as_ref()
                .expect("live spill writer")
                .buffer()
                .len() as u64,
        )
    }

    pub(crate) fn file(&self) -> &File {
        &self
            .writer
            .as_ref()
            .expect("live spill writer")
            .get_ref()
            .file
    }

    pub(crate) fn finish(mut self) -> Result<u64, QueryError> {
        self.flush().map_err(|error| {
            query_error_from_io(&error).unwrap_or_else(|| {
                crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    format!("spill I/O: {error}"),
                )
            })
        })?;
        let source = self.writer.as_mut().expect("live spill writer").get_mut();
        if let Some(record) = &source.record {
            record.seal(&mut source.file).map_err(spill_io_error)?;
        }
        if !self.retain_cache {
            crate::engine::executor::spill_hint::hint_dontneed(self.file());
        }
        Ok(self.bytes_written().saturating_sub(self.initial_len))
    }
}

impl Write for TrackedSpillWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.as_mut().expect("live spill writer").write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        // Preserve BufWriter's small-write fast path instead of rebuilding
        // the trait's generic partial-write loop for every scalar spill cell.
        // QuotaFile still owns all physical writes and quota checks.
        self.writer
            .as_mut()
            .expect("live spill writer")
            .write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.as_mut().expect("live spill writer").flush()
    }
}

impl Seek for TrackedSpillWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.writer.as_mut().expect("live spill writer").seek(pos)
    }
}

impl Drop for TrackedSpillWriter {
    fn drop(&mut self) {
        // Completion is explicit. On error/cancellation discard the buffered
        // tail instead of performing fallible, quota-charging I/O in Drop.
        if let Some(writer) = self.writer.take() {
            let _ = writer.into_parts();
        }
    }
}

/// Buffered partition output without a resident file descriptor. Each buffer
/// flush opens the private partition in append mode and closes it immediately;
/// high radix fanout therefore costs buffers, not one descriptor per partition.
pub(crate) struct BufferedSpillAppendWriter {
    writer: Option<BufWriter<ReopeningSpillFile>>,
    _buffer_charge: SpillBufferCharge,
}

struct ReopeningSpillFile {
    path: PathBuf,
    charger: SpillCharger,
    record: Option<Arc<FileRecord>>,
}

impl Write for ReopeningSpillFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let lease = self.charger.acquire_file().map_err(quota_as_io)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.path)?;
        let length = file.metadata()?.len();
        let record = match &self.record {
            Some(record) => record.clone(),
            None => {
                let record = self
                    .charger
                    .register_file(&self.path)
                    .map_err(quota_as_io)?;
                record.begin_write(true, length)?;
                self.record = Some(record.clone());
                record
            }
        };
        record.check_append_length(length)?;
        crate::engine::executor::spill_hint::hint_sequential(&file);
        SPILL_FILE_OPENS.fetch_add(1, Ordering::Relaxed);
        let mut file = QuotaFile {
            file,
            charger: self.charger.clone(),
            position: length,
            file_len: length,
            record: Some(record),
            end_write_on_drop: false,
            _lease: lease,
        };
        let result = file.write(buf);
        crate::engine::executor::spill_hint::hint_dontneed(&file.file);
        drop(file);
        result
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for ReopeningSpillFile {
    fn drop(&mut self) {
        if let Some(record) = &self.record {
            record.end_write();
        }
    }
}

impl BufferedSpillAppendWriter {
    pub(crate) fn new(path: &Path, charger: SpillCharger, buffer_capacity: usize) -> Self {
        let owner_cap = charger
            .inner
            .owner
            .as_ref()
            .map(|owner| (owner.limit() / 8).max(32 * 1024))
            .unwrap_or(1024 * 1024);
        let capacity = buffer_capacity.min(owner_cap).clamp(32 * 1024, 1024 * 1024);
        let buffer_charge = SpillBufferCharge::adaptive(charger.inner.owner.clone(), capacity);
        Self {
            writer: Some(BufWriter::with_capacity(
                buffer_charge.bytes,
                ReopeningSpillFile {
                    path: path.to_path_buf(),
                    charger,
                    record: None,
                },
            )),
            _buffer_charge: buffer_charge,
        }
    }

    pub(crate) fn finish(mut self) -> Result<(), QueryError> {
        self.flush().map_err(|error| {
            query_error_from_io(&error).unwrap_or_else(|| {
                crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    format!("spill I/O: {error}"),
                )
            })
        })?;
        let source = self.writer.as_ref().expect("live spill writer").get_ref();
        if let Some(record) = &source.record {
            let mut file = File::open(&source.path).map_err(spill_io_error)?;
            record.seal(&mut file).map_err(spill_io_error)?;
        }
        Ok(())
    }
}

impl Write for BufferedSpillAppendWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.as_mut().expect("live spill writer").write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.writer
            .as_mut()
            .expect("live spill writer")
            .write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.as_mut().expect("live spill writer").flush()
    }
}

impl Drop for BufferedSpillAppendWriter {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            let _ = writer.into_parts();
        }
    }
}

/// Delete a completed file before releasing exactly its live logical bytes.
pub(crate) fn remove_spill_file(path: &Path, charger: &SpillCharger) -> Result<(), QueryError> {
    // Retain the trusted reservation until every descriptor to an unlinked
    // inode closes. Mutable filesystem metadata must never debit other files.
    let record = charger.file_record(path)?;
    record.ensure_not_writing().map_err(spill_io_error)?;
    std::fs::remove_file(path).map_err(|error| {
        crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            format!("spill cleanup: {error}"),
        )
    })?;
    let removed = charger.inner.files.lock().remove(path);
    drop(removed);
    drop(record);
    Ok(())
}

struct TrackedReadFile {
    file: File,
    bytes_read: u64,
    position: u64,
    length: u64,
    _record: FileReadLease,
    failed: bool,
    _lease: SpillFileLease,
}

impl Read for TrackedReadFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.failed {
            return Err(manifest::invalid(
                "reader previously failed integrity validation",
            ));
        }
        if buf.is_empty() {
            return Ok(0);
        }
        if self.position >= self.length {
            return Ok(0);
        }
        let block_bytes = manifest::CHECKSUM_BLOCK_BYTES as u64;
        let start = self.position / block_bytes * block_bytes;
        let prefix = (self.position - start) as usize;
        let required = (self.length - start).min(block_bytes) as usize;
        if buf.len() < required {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "spill reader buffer cannot hold one checksum block",
            ));
        }
        let capacity = (buf.len() / manifest::CHECKSUM_BLOCK_BYTES)
            .max(1)
            .saturating_mul(manifest::CHECKSUM_BLOCK_BYTES);
        let count = (self.length - start).min(capacity as u64) as usize;
        if start != self.position {
            self.file.seek(SeekFrom::Start(start))?;
        }
        let mut filled = 0;
        while filled < count {
            let read = match self.file.read(&mut buf[filled..count]) {
                Ok(read) => read,
                Err(error) => {
                    self.failed = true;
                    return Err(error);
                }
            };
            self.bytes_read = self.bytes_read.saturating_add(read as u64);
            crate::engine::executor::spill_hint::record_read(read as u64);
            if read == 0 {
                self.failed = true;
                return Err(manifest::invalid("truncated checksum block"));
            }
            filled += read;
        }
        self.position = start + count as u64;
        if let Err(error) = self._record.validate_blocks(start, &buf[..count]) {
            self.failed = true;
            return Err(error);
        }
        // A seek may begin inside a checksum block. Validate its complete
        // bytes before exposing even a prefix to LIMIT or a random reader.
        if prefix != 0 {
            buf.copy_within(prefix..count, 0);
        }
        Ok(count - prefix)
    }
}

impl Seek for TrackedReadFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let position = self.file.seek(pos)?;
        self.position = position;
        Ok(position)
    }
}

/// Query-owned buffered reader for spill replay. The file descriptor and its
/// buffer remain attributed to the directory's original query even if replay
/// or drop occurs on another worker thread.
pub(crate) struct TrackedSpillReader {
    reader: BufReader<TrackedReadFile>,
    _buffer_charge: SpillBufferCharge,
}

impl TrackedSpillReader {
    pub(crate) fn open(
        path: &Path,
        charger: SpillCharger,
        buffer_capacity: usize,
    ) -> Result<Self, QueryError> {
        let lease = charger.acquire_file()?;
        let file = File::open(path).map_err(|error| {
            crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                format!("spill I/O: {error}"),
            )
        })?;
        crate::engine::executor::spill_hint::hint_sequential(&file);
        let length = file
            .metadata()
            .map_err(|error| {
                crate::error::catalog::ER_UNKNOWN_ERROR.error(format!("spill metadata: {error}"))
            })?
            .len();
        let record = charger.file_record(path)?;
        let record = record.acquire_read(length).map_err(spill_io_error)?;
        let owner_cap = charger
            .inner
            .owner
            .as_ref()
            .map(|owner| (owner.limit() / 8).max(32 * 1024))
            .unwrap_or(1024 * 1024);
        let capacity = buffer_capacity.min(owner_cap).clamp(32 * 1024, 1024 * 1024);
        let mut buffer_charge = SpillBufferCharge::adaptive(charger.inner.owner.clone(), capacity);
        let required = length.min(manifest::CHECKSUM_BLOCK_BYTES as u64) as usize;
        if buffer_charge.bytes < required {
            // An immutable file can require less than the normal I/O buffer.
            // One complete checksum block is mandatory before publishing data.
            drop(buffer_charge);
            buffer_charge = SpillBufferCharge::required(charger.inner.owner.clone(), required)?;
        }
        let capacity = buffer_charge.bytes;
        SPILL_FILE_OPENS.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            reader: BufReader::with_capacity(
                capacity,
                TrackedReadFile {
                    file,
                    bytes_read: 0,
                    position: 0,
                    length,
                    _record: record,
                    failed: false,
                    _lease: lease,
                },
            ),
            _buffer_charge: buffer_charge,
        })
    }

    pub(crate) fn file(&self) -> &File {
        &self.reader.get_ref().file
    }

    pub(crate) fn bytes_read(&self) -> u64 {
        self.reader.get_ref().bytes_read
    }

    pub(crate) fn remaining_bytes(&self) -> u64 {
        let source = self.reader.get_ref();
        source
            .length
            .saturating_sub(source.position)
            .saturating_add(self.reader.buffer().len() as u64)
    }

    pub(crate) fn memory_charge(&self) -> crate::engine::executor::mem_budget::ScopedCharge {
        crate::engine::executor::mem_budget::ScopedCharge::for_budget(
            self.reader.get_ref()._lease.charger.inner.owner.clone(),
        )
    }

    pub(crate) fn decode_limit(&self) -> usize {
        self.reader
            .get_ref()
            ._lease
            .charger
            .inner
            .owner
            .as_ref()
            .map_or(1024 * 1024, |owner| {
                (owner.limit() / 8).clamp(16 * 1024, 1024 * 1024)
            })
    }
}

impl Read for TrackedSpillReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        // Fixed-width spill values usually fit in the buffered tail. Delegate
        // directly while retaining TrackedReadFile's I/O accounting on refill.
        self.reader.read_exact(buf)
    }
}

impl Seek for TrackedSpillReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.reader.seek(pos)
    }
}

impl Drop for TrackedSpillReader {
    fn drop(&mut self) {
        crate::engine::executor::spill_hint::hint_dontneed(self.file());
    }
}

/// Divide a bounded spill-I/O buffer pool across concurrently-open writers.
pub(crate) fn writer_buffer_capacity(writer_count: usize) -> usize {
    let mut fair = crate::engine::executor::work_area::global_manager().fair_share_bytes();
    if let Some(budget) = crate::engine::executor::mem_budget::current_budget() {
        fair = fair.min(budget.limit());
    }
    let total = fair.saturating_div(8).min(8 * 1024 * 1024).max(32 * 1024);
    total
        .saturating_div(writer_count.max(1))
        .clamp(32 * 1024, 1024 * 1024)
}

/// Build a charger for a freshly-created spill directory under
/// `data_dir`. Performs the up-front free-space check: if the volume is
/// already below the reserve threshold we refuse to start spilling at
/// all (1041) rather than write a few blocks and then hit ENOSPC.
pub(crate) fn charger_for_new_dir(data_dir: &Path) -> Result<SpillCharger, QueryError> {
    retry_orphan_cleanup();
    let data_dir = std::fs::canonicalize(data_dir).map_err(spill_io_error)?;
    let global_limit = effective_global_limit(&data_dir);
    let cur = GLOBAL_SPILLED.load(Ordering::Relaxed);
    if cur >= global_limit {
        return Err(quota_error(0, QuotaScope::Global, cur, global_limit));
    }
    let owner = crate::engine::executor::mem_budget::current_budget();
    let charger = SpillCharger {
        inner: Arc::new(ChargerInner {
            data_dir,
            #[cfg(test)]
            limit_override: None,
            files: parking_lot::Mutex::new(FileRegistry::new(owner.clone())),
            owner,
            dir_bytes: AtomicU64::new(0),
            lifecycle: Default::default(),
            writeback_slot: AtomicUsize::new(WRITEBACK_DIRECTORY_SLOTS),
        }),
    };
    {
        for (index, entry) in writeback_directories().iter().enumerate() {
            let mut slot = entry.lock();
            if slot.strong_count() == 0 {
                *slot = Arc::downgrade(&charger.inner);
                charger.inner.writeback_slot.store(index, Ordering::Relaxed);
                break;
            }
        }
    }
    Ok(charger)
}

/// Release every byte a spill directory charged. Called from
/// `SpillTmpDir::drop` after the files are removed.
pub(crate) fn release_dir(charger: &SpillCharger) {
    charger.inner.files.lock().clear();
    let n = charger.inner.dir_bytes.swap(0, Ordering::AcqRel);
    if n == 0 {
        return;
    }
    atomic_saturating_sub(&GLOBAL_SPILLED, n);
    if let Some(b) = &charger.inner.owner {
        b.sub_spill_bytes(n);
    }
}

/// Live process-wide spilled bytes — for SHOW ENGINE / advisor.
pub fn global_spilled_bytes() -> u64 {
    GLOBAL_SPILLED.load(Ordering::Relaxed)
}
/// High-water mark of live spilled bytes.
pub fn global_spilled_high_water() -> u64 {
    GLOBAL_SPILLED_HWM.load(Ordering::Relaxed)
}
/// Number of 1041 quota rejections so far.
pub fn quota_rejections() -> u64 {
    QUOTA_REJECTIONS.load(Ordering::Relaxed)
}

pub fn global_quota_rejections() -> u64 {
    GLOBAL_QUOTA_REJECTIONS.load(Ordering::Relaxed)
}

pub fn per_query_quota_rejections() -> u64 {
    PER_QUERY_QUOTA_REJECTIONS.load(Ordering::Relaxed)
}

pub fn active_spill_files() -> u64 {
    ACTIVE_SPILL_FILES.load(Ordering::Relaxed)
}

pub fn spill_file_opens() -> u64 {
    SPILL_FILE_OPENS.load(Ordering::Relaxed)
}

pub fn orphan_spill_bytes() -> u64 {
    ORPHAN_SPILL_BYTES.load(Ordering::Relaxed)
}

pub fn cleanup_failures() -> u64 {
    SPILL_CLEANUP_FAILURES.load(Ordering::Relaxed)
}

pub(crate) fn record_cleanup_failure(bytes: u64) {
    SPILL_CLEANUP_FAILURES.fetch_add(1, Ordering::Relaxed);
    ORPHAN_SPILL_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

pub(crate) fn record_operator_spill(kind: &str) {
    let counter = match kind {
        "sort" | "indexsort" => &SORT_SPILLS,
        "hashjoin" | "vec-hashjoin" => &HASH_JOIN_SPILLS,
        "hashagg" => &HASH_AGG_SPILLS,
        "distinct" => &DISTINCT_SPILLS,
        "cte" => &CTE_SPILLS,
        _ => return,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn sort_spill_count() -> u64 {
    SORT_SPILLS.load(Ordering::Relaxed)
}

pub fn hash_join_spill_count() -> u64 {
    HASH_JOIN_SPILLS.load(Ordering::Relaxed)
}

pub fn hash_agg_spill_count() -> u64 {
    HASH_AGG_SPILLS.load(Ordering::Relaxed)
}

pub fn distinct_spill_count() -> u64 {
    DISTINCT_SPILLS.load(Ordering::Relaxed)
}

pub fn cte_spill_count() -> u64 {
    CTE_SPILLS.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn pressure_writeback_skips_locked_manifests_and_does_not_pin_the_query() {
        use crate::engine::executor::mem_budget::{self, MemBudget};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pressure.bin");
        let budget = Arc::new(MemBudget::new(128 * 1024));
        let charger = mem_budget::with_query_budget(budget.clone(), || {
            charger_for_new_dir(dir.path()).unwrap()
        });
        sealed_file(&charger, &path, &vec![42; 64 * 1024]);
        let lease = charger.acquire_file().unwrap();
        {
            let files = charger.inner.files.lock();
            // The caller may be reserving metadata under this very lock.
            request_process_writeback();
            let mut requests = WritebackBudget::new();
            files.request_writeback(&mut requests, &lease);
            assert!(requests.files < 256);
        }
        drop(lease);
        release_dir(&charger);
        drop(charger);
        assert_eq!(budget.used(), 0);
        let weak = Arc::downgrade(&budget);
        drop(budget);
        assert!(weak.upgrade().is_none());
    }

    fn tmp_charger(limit: u64) -> SpillCharger {
        let dir = std::env::current_dir().unwrap();
        SpillCharger {
            inner: Arc::new(ChargerInner {
                data_dir: dir,
                limit_override: Some(limit),
                owner: None,
                dir_bytes: AtomicU64::new(0),
                files: parking_lot::Mutex::new(FileRegistry::new(None)),
                lifecycle: Default::default(),
                writeback_slot: AtomicUsize::new(WRITEBACK_DIRECTORY_SLOTS),
            }),
        }
    }

    #[test]
    fn exhausted_volume_reports_free_space_instead_of_zero_byte_write() {
        let error = quota_error(0, QuotaScope::Global, 0, 0);
        assert_eq!(error.code(), 1041);
        assert!(error.message().contains("no space available for spill"));
        assert!(error.message().contains("Filesystem-reserved blocks"));
        assert!(!error.message().contains("spill write of 0 bytes"));
        assert!(!error.message().contains("Raise MYDB_TEMP_DISK_LIMIT"));
    }

    #[test]
    fn exhausted_quota_reports_directory_creation() {
        let error = quota_error(0, QuotaScope::Global, 1024, 1024);
        assert_eq!(error.code(), 1041);
        assert!(error.message().contains("cannot create spill directory"));
        assert!(error.message().contains("1024 / 1024 bytes in use"));
    }

    #[test]
    fn charge_and_release_round_trips_global() {
        let before = GLOBAL_SPILLED.load(Ordering::Relaxed);
        let c = tmp_charger(u64::MAX);
        drop(c.charge(1000).unwrap());
        assert_eq!(GLOBAL_SPILLED.load(Ordering::Relaxed), before + 1000);
        assert_eq!(c.dir_bytes(), 1000);
        release_dir(&c);
        assert_eq!(GLOBAL_SPILLED.load(Ordering::Relaxed), before);
        assert_eq!(c.dir_bytes(), 0);
    }

    #[test]
    fn global_ceiling_rejects_and_rolls_back() {
        let before = GLOBAL_SPILLED.load(Ordering::Relaxed);
        let rejects_before = global_quota_rejections();
        // Limit only `before + 500` more bytes.
        let c = tmp_charger(before + 500);
        drop(c.charge(400).unwrap());
        let err = c.charge(400).unwrap_err();
        assert_eq!(err.code(), 1041);
        assert_eq!(global_quota_rejections(), rejects_before + 1);
        // Rejected charge fully rolled back — only the 400 that fit remain.
        assert_eq!(GLOBAL_SPILLED.load(Ordering::Relaxed), before + 400);
        release_dir(&c);
        assert_eq!(GLOBAL_SPILLED.load(Ordering::Relaxed), before);
    }

    #[test]
    fn volume_reserve_is_a_fixed_fraction_of_total_capacity() {
        let gib = 1024 * 1024 * 1024;
        let space = VolumeSpace {
            available: 10 * gib,
            total: 100 * gib,
            allocation_unit: 4096,
        };
        assert_eq!(space.headroom(10), 0);
        assert_eq!(space.headroom(0), 10 * gib);
        let space = VolumeSpace {
            available: 50 * gib,
            total: 100 * gib,
            allocation_unit: 4096,
        };
        assert_eq!(space.headroom(10), 40 * gib);
        assert_eq!(space.headroom(90), 0);
    }

    #[test]
    fn simultaneous_unwritten_reservations_cannot_reuse_the_same_volume_headroom() {
        // Reserve logical credits only: this never allocates disk data or
        // approaches actual ENOSPC on the host filesystem.
        set_global_limit_bytes(0);
        set_reserve_pct(0).unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let first = charger_for_new_dir(root.path()).unwrap();
        let second = charger_for_new_dir(root.path()).unwrap();
        let available = volume_free_bytes(root.path()).unwrap().headroom(0);
        assert!(available >= 4);
        let requested = available / 4 * 3;
        let first_reservation = first.charge(requested).unwrap();
        let second_reservation = second.charge(requested);
        let rejected = second_reservation.is_err();
        drop((first_reservation, second_reservation));
        release_dir(&first);
        release_dir(&second);
        assert!(
            rejected,
            "two pending writes reused the same unconsumed filesystem headroom"
        );
    }

    #[test]
    fn statvfs_does_not_panic_on_real_dir() {
        // Whatever the platform, the free-space probe must not fault.
        let _ = volume_free_bytes(&PathBuf::from("."));
        let _ = effective_global_limit(&PathBuf::from("."));
    }

    #[test]
    fn pending_write_reserves_complete_allocation_units() {
        let space = VolumeSpace {
            available: 16384,
            total: 65536,
            allocation_unit: 4096,
        };
        assert_eq!(space.pending_bytes(0), Some(0));
        assert_eq!(space.pending_bytes(1), Some(4096));
        assert_eq!(space.pending_bytes(4096), Some(4096));
        assert_eq!(space.pending_bytes(4097), Some(8192));
        assert_eq!(space.pending_bytes(u64::MAX), None);
    }

    #[test]
    fn charger_releases_original_query_on_another_thread() {
        let before = global_spilled_bytes();
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(4096));
        let dir = tempfile::tempdir().unwrap();
        let charger =
            crate::engine::executor::mem_budget::with_query_budget(Arc::clone(&budget), || {
                charger_for_new_dir(dir.path()).unwrap()
            });
        drop(charger.charge(512).unwrap());
        assert_eq!(budget.spill_bytes(), 512);
        std::thread::spawn(move || release_dir(&charger))
            .join()
            .unwrap();
        assert_eq!(budget.spill_bytes(), 0);
        assert_eq!(global_spilled_bytes(), before);
    }

    #[test]
    fn per_query_limit_is_atomic_across_chargers() {
        set_per_query_limit_bytes(100);
        let before = global_spilled_bytes();
        let rejects_before = per_query_quota_rejections();
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(4096));
        let dir = tempfile::tempdir().unwrap();
        let (first, second) =
            crate::engine::executor::mem_budget::with_query_budget(Arc::clone(&budget), || {
                (
                    charger_for_new_dir(dir.path()).unwrap(),
                    charger_for_new_dir(dir.path()).unwrap(),
                )
            });
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let successes = Arc::new(AtomicU64::new(0));
        std::thread::scope(|scope| {
            for charger in [first.clone(), second.clone()] {
                let barrier = Arc::clone(&barrier);
                let successes = Arc::clone(&successes);
                scope.spawn(move || {
                    barrier.wait();
                    if charger.charge(75).is_ok() {
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
            barrier.wait();
        });
        assert_eq!(successes.load(Ordering::Relaxed), 1);
        assert_eq!(per_query_quota_rejections(), rejects_before + 1);
        assert_eq!(budget.spill_bytes(), 75);
        release_dir(&first);
        release_dir(&second);
        assert_eq!(budget.spill_bytes(), 0);
        assert_eq!(global_spilled_bytes(), before);
    }

    #[test]
    fn buffered_append_preserves_payload_quota_and_releases_resources() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("append.bin");
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
            1024 * 1024,
        ));
        let charger =
            crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
                charger_for_new_dir(dir.path()).unwrap()
            });
        let active = active_spill_files();
        let opens = spill_file_opens();
        let mut writer = BufferedSpillAppendWriter::new(&path, charger.clone(), 32 * 1024);
        assert_eq!(budget.used(), 32 * 1024);
        let mut expected = Vec::new();
        for value in 0u64..10_001 {
            writer.write_all(&value.to_le_bytes()).unwrap();
            expected.extend_from_slice(&value.to_le_bytes());
            assert_eq!(active_spill_files(), active);
        }
        // A value larger than the buffer also appends without retaining an FD.
        let wide = vec![0x5a; 64 * 1024];
        writer.write_all(&wide).unwrap();
        expected.extend_from_slice(&wide);
        writer.finish().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        assert_eq!(charger.dir_bytes(), expected.len() as u64);
        assert_eq!(budget.spill_bytes(), expected.len() as u64);
        assert!(
            budget.used() > 0,
            "the live file manifest remains query-owned"
        );
        assert_eq!(active_spill_files(), active);
        assert!(
            spill_file_opens() - opens < 10,
            "open per flush, not per value"
        );
        release_dir(&charger);
        assert_eq!(budget.spill_bytes(), 0);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn buffered_append_quota_rejection_preserves_existing_bytes() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("quota-append.bin");
        let charger = tmp_charger(global_spilled_bytes() + 3);
        let active = active_spill_files();
        let mut writer = BufferedSpillAppendWriter::new(&path, charger.clone(), 32 * 1024);
        writer.write_all(b"ok").unwrap();
        writer.flush().unwrap();
        writer.write_all(b"no").unwrap();
        assert_eq!(writer.finish().unwrap_err().code(), 1041);
        assert_eq!(std::fs::read(&path).unwrap(), b"ok");
        assert_eq!(charger.dir_bytes(), 2);
        assert_eq!(active_spill_files(), active);
        release_dir(&charger);
    }

    #[test]
    fn tracked_writer_rejects_before_extending_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("quota.bin");
        let charger = tmp_charger(global_spilled_bytes() + 3);
        let mut writer = TrackedSpillWriter::create_new(&path, charger.clone(), 32 * 1024).unwrap();
        writer.write_all(b"four").unwrap();
        let err = writer.finish().unwrap_err();
        assert_eq!(err.code(), 1041);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        release_dir(&charger);
    }

    #[test]
    fn tracked_exact_io_crosses_buffer_boundaries_and_reports_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("exact.bin");
        let charger = tmp_charger(global_spilled_bytes() + 128 * 1024);
        let mut writer = TrackedSpillWriter::create_new(&path, charger.clone(), 32 * 1024).unwrap();
        for value in 0u64..10_001 {
            writer.write_all(&value.to_le_bytes()).unwrap();
        }
        assert_eq!(writer.finish().unwrap(), 80_008);
        let mut reader = TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).unwrap();
        for value in 0u64..10_001 {
            let mut bytes = [0; 8];
            reader.read_exact(&mut bytes).unwrap();
            assert_eq!(u64::from_le_bytes(bytes), value);
        }
        assert_eq!(reader.bytes_read(), 80_008);
        assert_eq!(
            reader.read_exact(&mut [0; 1]).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        drop(reader);
        release_dir(&charger);
    }

    #[test]
    fn tracked_reader_buffer_releases_original_owner_on_worker_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("read.bin");
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
            128 * 1024,
        ));
        let charger =
            crate::engine::executor::mem_budget::with_query_budget(Arc::clone(&budget), || {
                charger_for_new_dir(dir.path()).unwrap()
            });
        let active_before = active_spill_files();
        sealed_file(&charger, &path, b"payload");
        let manifest_memory = budget.used();
        let reader = TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).unwrap();
        assert_eq!(budget.used(), manifest_memory + 32 * 1024);
        assert_eq!(active_spill_files(), active_before + 1);
        std::thread::spawn(move || drop(reader)).join().unwrap();
        assert_eq!(budget.used(), manifest_memory);
        assert_eq!(active_spill_files(), active_before);
        release_dir(&charger);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn rejected_spill_buffers_fall_back_to_direct_io_without_cancelling_query() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("direct.bin");
        // A plain budget avoids changing the process governor in a unit test.
        // Overflow provides an actual hard reservation rejection at both sizes.
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
            usize::MAX,
        ));
        budget.force_charge(usize::MAX - 4096).unwrap();
        let charger =
            crate::engine::executor::mem_budget::with_query_budget(Arc::clone(&budget), || {
                charger_for_new_dir(dir.path()).unwrap()
            });
        let mut writer =
            TrackedSpillWriter::create_new(&path, charger.clone(), 1024 * 1024).unwrap();
        assert_eq!(writer.writer.as_ref().unwrap().capacity(), 0);
        writer.write_all(b"direct I/O payload").unwrap();
        assert_eq!(writer.finish().unwrap(), 18);
        let manifest_memory = budget.used() - (usize::MAX - 4096);
        let mut reader = TrackedSpillReader::open(&path, charger.clone(), 1024 * 1024).unwrap();
        assert_eq!(reader.reader.capacity(), 18);
        let mut payload = [0; 18];
        reader.read_exact(&mut payload).unwrap();
        assert_eq!(&payload, b"direct I/O payload");
        assert_eq!(
            reader.read_exact(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        crate::engine::executor::mem_budget::with_query_budget(Arc::clone(&budget), || {
            crate::engine::executor::mem_budget::check_terminal_oom().unwrap();
        });
        drop(reader);
        assert_eq!(budget.used(), usize::MAX - 4096 + manifest_memory);
        release_dir(&charger);
        assert_eq!(budget.used(), usize::MAX - 4096);
    }

    #[test]
    fn required_checksum_buffer_reclaims_before_rejecting_and_can_retry() {
        use crate::engine::executor::mem_budget::{
            self, ControlledMemoryCategory, ControlledMemoryCharge, MemBudget,
        };

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checksum.bin");
        let budget = Arc::new(MemBudget::process_tracked_for_test(128 * 1024));
        let charger = mem_budget::with_query_budget(budget.clone(), || {
            charger_for_new_dir(dir.path()).unwrap()
        });
        let payload = vec![42; manifest::CHECKSUM_BLOCK_BYTES + 17];
        sealed_file(&charger, &path, &payload);
        let manifest_bytes = budget.used();
        mem_budget::with_test_effective_memory_limit(1024 * 1024, || {
            let available = mem_budget::operational_memory_limit_bytes()
                - mem_budget::controlled_memory_snapshot().0;
            let filler = ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::SystemMetadata,
                available,
            )
            .unwrap();
            let before = mem_budget::admission_reclaim_snapshot().attempts;
            assert!(TrackedSpillReader::open(&path, charger.clone(), 1024 * 1024).is_err());
            assert!(mem_budget::admission_reclaim_snapshot().attempts > before);
            assert_eq!(budget.used(), manifest_bytes);
            assert!(budget.terminal_oom().is_none());
            drop(filler);
            let mut reader = TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).unwrap();
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).unwrap();
            assert_eq!(actual, payload);
        });
        assert_eq!(budget.used(), manifest_bytes);
        release_dir(&charger);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn panic_unwind_cleans_query_spill_context() {
        let root = tempfile::tempdir().unwrap();
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
            256 * 1024,
        ));
        let global_before = global_spilled_bytes();
        let active_before = active_spill_files();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::engine::executor::mem_budget::with_query_budget(Arc::clone(&budget), || {
                let dir = crate::engine::executor::select::grace::SpillTmpDir::create_kind(
                    root.path(),
                    "hashagg",
                )
                .unwrap();
                let path = dir.path().join("state.bin");
                let mut writer =
                    TrackedSpillWriter::create_new(&path, dir.charger(), 32 * 1024).unwrap();
                writer.write_all(b"state").unwrap();
                writer.flush().unwrap();
                panic!("injected worker panic");
            });
        }));
        assert!(result.is_err());
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
        assert_eq!(global_spilled_bytes(), global_before);
        assert_eq!(active_spill_files(), active_before);
    }

    #[test]
    fn directory_retirement_waits_for_writer_and_discards_tail() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let before = global_spilled_bytes();
        let dir =
            crate::engine::executor::select::grace::SpillTmpDir::create_kind(root.path(), "audit")
                .unwrap();
        let path = dir.path().to_path_buf();
        let charger = dir.charger();
        let mut writer =
            TrackedSpillWriter::create_new(&path.join("tail"), charger.clone(), 32 * 1024).unwrap();
        writer.write_all(&[7; 128]).unwrap();
        drop(dir);
        assert!(path.exists(), "a live file lease retains its directory");
        assert!(TrackedSpillWriter::create_new(&path.join("late"), charger, 32 * 1024).is_err());
        drop(writer);
        assert!(!path.exists());
        assert_eq!(global_spilled_bytes(), before);
    }

    #[test]
    fn failed_directory_cleanup_is_retried_and_releases_quota() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let before = global_spilled_bytes();
        let orphans_before = orphan_spill_bytes();
        let dir =
            crate::engine::executor::select::grace::SpillTmpDir::create_kind(root.path(), "audit")
                .unwrap();
        let path = dir.path().to_path_buf();
        let moved = path.with_extension("moved");
        let bytes = dir.charger().dir_bytes();
        std::fs::rename(&path, &moved).unwrap();
        std::fs::write(&path, b"prevent directory removal").unwrap();
        drop(dir);
        assert_eq!(global_spilled_bytes(), before + bytes);
        assert_eq!(orphan_spill_bytes(), orphans_before + bytes);
        std::fs::remove_file(&path).unwrap();
        std::fs::rename(&moved, &path).unwrap();
        retry_orphan_cleanup();
        assert!(!path.exists());
        assert_eq!(global_spilled_bytes(), before);
        assert_eq!(orphan_spill_bytes(), orphans_before);
    }

    fn sealed_file(charger: &SpillCharger, path: &Path, bytes: &[u8]) {
        let mut writer = TrackedSpillWriter::create_new(path, charger.clone(), 32 * 1024).unwrap();
        writer.write_all(bytes).unwrap();
        writer.finish().unwrap();
    }

    #[test]
    fn completed_record_boundary_truncation_is_rejected() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("records");
        let charger = tmp_charger(u64::MAX);
        sealed_file(&charger, &path, b"record01record02");
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(8)
            .unwrap();
        assert!(TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).is_err());
        release_dir(&charger);
    }

    #[test]
    fn same_length_spill_corruption_is_rejected_during_replay() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("records");
        let charger = tmp_charger(u64::MAX);
        sealed_file(&charger, &path, b"record01record02");
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all(b"Record01")
            .unwrap();
        let mut reader = TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).unwrap();
        assert!(reader.read_to_end(&mut Vec::new()).is_err());
        drop(reader);
        release_dir(&charger);
    }

    #[test]
    fn flushed_but_unfinished_spill_is_not_a_complete_relation() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("records");
        let charger = tmp_charger(u64::MAX);
        let mut writer = TrackedSpillWriter::create_new(&path, charger.clone(), 32 * 1024).unwrap();
        writer.write_all(b"record01").unwrap();
        writer.flush().unwrap();
        drop(writer);
        assert!(TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).is_err());
        release_dir(&charger);
    }

    #[test]
    fn deleting_enlarged_file_cannot_release_another_files_quota() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        let charger = tmp_charger(u64::MAX);
        sealed_file(&charger, &first, b"12345678");
        sealed_file(&charger, &second, b"12345678");
        OpenOptions::new()
            .write(true)
            .open(&first)
            .unwrap()
            .set_len(1024)
            .unwrap();
        remove_spill_file(&first, &charger).unwrap();
        assert_eq!(charger.dir_bytes(), 8);
        release_dir(&charger);
    }

    #[cfg(unix)]
    #[test]
    fn unlinked_file_keeps_quota_until_its_last_reader_closes() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("records");
        let charger = tmp_charger(u64::MAX);
        sealed_file(&charger, &path, b"record01");
        let mut reader = TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).unwrap();
        remove_spill_file(&path, &charger).unwrap();
        assert_eq!(charger.dir_bytes(), 8);
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(data, b"record01");
        drop(reader);
        assert_eq!(charger.dir_bytes(), 0);
        release_dir(&charger);
    }

    #[test]
    fn append_and_backpatch_complete_with_their_actual_checksum() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("records");
        let charger = tmp_charger(u64::MAX);
        sealed_file(&charger, &path, b"record01");
        let mut writer =
            TrackedSpillWriter::open_append(&path, charger.clone(), 32 * 1024).unwrap();
        writer.write_all(b"record02").unwrap();
        assert_eq!(writer.finish().unwrap(), 8);
        let mut reader = TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(data, b"record01record02");
        drop(reader);
        let mut writer =
            TrackedSpillWriter::create_truncate(&path, charger.clone(), 32 * 1024).unwrap();
        writer.write_all(b"record03").unwrap();
        writer.seek(SeekFrom::Start(0)).unwrap();
        writer.write_all(b"R").unwrap();
        writer.finish().unwrap();
        assert_eq!(charger.dir_bytes(), 8);
        let mut reader = TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).unwrap();
        data.clear();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(data, b"Record03");
        drop(reader);
        release_dir(&charger);
    }

    #[test]
    fn missing_registered_partition_is_an_error_but_lazy_absence_is_empty() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("records");
        let charger = tmp_charger(u64::MAX);
        assert!(charger.absent_partition(&path).unwrap());
        sealed_file(&charger, &path, b"record01");
        std::fs::remove_file(&path).unwrap();
        assert!(charger.absent_partition(&path).is_err());
        release_dir(&charger);
    }

    #[test]
    fn swapped_same_length_files_are_rejected_against_their_own_manifest() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        let charger = tmp_charger(u64::MAX);
        sealed_file(&charger, &first, b"record01");
        sealed_file(&charger, &second, b"record02");
        std::fs::rename(&second, &first).unwrap();
        let mut reader = TrackedSpillReader::open(&first, charger.clone(), 32 * 1024).unwrap();
        assert!(reader.read_to_end(&mut Vec::new()).is_err());
        drop(reader);
        release_dir(&charger);
    }

    #[test]
    fn buffered_writer_retains_exclusive_completion_until_finish() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("records");
        let charger = tmp_charger(u64::MAX);
        let mut writer = BufferedSpillAppendWriter::new(&path, charger.clone(), 32 * 1024);
        writer.write_all(b"record01").unwrap();
        writer.flush().unwrap();
        assert!(TrackedSpillWriter::open_append(&path, charger.clone(), 32 * 1024).is_err());
        assert!(TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).is_err());
        assert!(remove_spill_file(&path, &charger).is_err());
        writer.write_all(b"record02").unwrap();
        writer.finish().unwrap();
        let mut reader = TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(data, b"record01record02");
        drop(reader);
        release_dir(&charger);
    }

    #[test]
    fn corrupt_prefix_is_rejected_before_a_limit_consumer_observes_it() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("records");
        let charger = tmp_charger(u64::MAX);
        sealed_file(&charger, &path, &vec![0x41; 128 * 1024]);
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all(b"corrupt!")
            .unwrap();
        let mut reader = TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).unwrap();
        assert!(reader.read_exact(&mut [0; 8]).is_err());
        drop(reader);
        release_dir(&charger);
    }

    #[test]
    fn open_reader_prevents_append_or_truncate_of_its_immutable_relation() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("records");
        let charger = tmp_charger(u64::MAX);
        sealed_file(&charger, &path, b"record01");
        let mut reader = TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).unwrap();
        assert!(TrackedSpillWriter::create_truncate(&path, charger.clone(), 32 * 1024).is_err());
        assert!(TrackedSpillWriter::open_append(&path, charger.clone(), 32 * 1024).is_err());
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        assert_eq!(data, b"record01");
        drop(reader);
        let mut writer =
            TrackedSpillWriter::open_append(&path, charger.clone(), 32 * 1024).unwrap();
        writer.write_all(b"record02").unwrap();
        writer.finish().unwrap();
        assert_eq!(charger.dir_bytes(), 16);
        release_dir(&charger);
    }

    #[test]
    fn checked_reader_handles_unaligned_seeks_and_small_or_direct_reads() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("records");
        let charger = tmp_charger(u64::MAX);
        let expected: Vec<u8> = (0..100_003).map(|n| (n % 251) as u8).collect();
        sealed_file(&charger, &path, &expected);
        let mut reader = TrackedSpillReader::open(&path, charger.clone(), 32 * 1024).unwrap();
        for (start, count) in [
            (1, 3),
            (32767, 7),
            (32768, 2),
            (2, 70_001),
            (99_999, 4),
            (100_003, 0),
        ] {
            reader.seek(SeekFrom::Start(start)).unwrap();
            let mut bytes = vec![0; count];
            reader.read_exact(&mut bytes).unwrap();
            assert_eq!(bytes, expected[start as usize..start as usize + count]);
            assert_eq!(
                reader.remaining_bytes(),
                expected.len() as u64 - start - count as u64
            );
        }
        drop(reader);
        release_dir(&charger);
    }

    #[test]
    fn raw_buffer_abort_discards_tail_and_defers_directory_cleanup() {
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let before = global_spilled_bytes();
        let dir =
            crate::engine::executor::select::grace::SpillTmpDir::create_kind(root.path(), "raw")
                .unwrap();
        let path = dir.path().to_path_buf();
        let charger = dir.charger();
        let file = File::create(path.join("raw")).unwrap();
        let mut writer =
            TrackedSpillWriter::from_raw_file(file, charger.clone(), 32 * 1024).unwrap();
        writer.write_all(b"uncommitted tail").unwrap();
        assert_eq!(std::fs::metadata(path.join("raw")).unwrap().len(), 0);
        drop(dir);
        assert!(path.exists());
        drop(writer);
        assert!(!path.exists());
        assert_eq!(charger.dir_bytes(), 0);
        assert_eq!(global_spilled_bytes(), before);
    }

    #[test]
    fn shared_raw_file_retains_quota_until_the_last_worker_closes_it() {
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let before = global_spilled_bytes();
        let dir =
            crate::engine::executor::select::grace::SpillTmpDir::create_kind(root.path(), "raw")
                .unwrap();
        let path = dir.path().to_path_buf();
        let charger = dir.charger();
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path.join("raw"))
            .unwrap();
        let file = Arc::new(charger.lease_file(file).unwrap());
        let reservation = charger.reserve_write(8, 8).unwrap();
        let mut raw: &File = &file;
        raw.write_all(b"raw-data").unwrap();
        drop(reservation);
        let mirror = file.clone();
        drop(dir);
        drop(file);
        assert!(path.exists());
        assert!(charger.dir_bytes() >= 8);
        std::thread::spawn(move || drop(mirror)).join().unwrap();
        assert!(!path.exists());
        assert_eq!(charger.dir_bytes(), 0);
        assert_eq!(global_spilled_bytes(), before);
    }
}
