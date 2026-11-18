//! Persistent recovery space and live write admission for a data volume.

use crate::error::catalog::{ER_DISK_FULL_NOWAIT, ER_WRONG_VALUE};
use crate::types::QueryError;
use parking_lot::{Mutex, MutexGuard};
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

const MIB: u64 = 1024 * 1024;
const RECOVERY_RESERVE_SUPPORTED: bool = cfg!(any(target_os = "linux", target_os = "macos"));
const DEFAULT_RECOVERY_RESERVE_BYTES: u64 = if RECOVERY_RESERVE_SUPPORTED {
    1024 * MIB
} else {
    0
};
const SAMPLE_MS: u64 = 10;
const RESERVE_FILE: &str = "system/disk-recovery.reserve";

#[derive(Debug)]
pub(crate) struct DiskPressure {
    root: PathBuf,
    reserve: u64,
    low: u64,
    resume: u64,
    wal_segment: u64,
    started: AtomicBool,
    armed: AtomicBool,
    limited: AtomicBool,
    available: AtomicU64,
    sampled_until: AtomicU64,
    clock: Instant,
    sampler: Mutex<()>,
    cleanup: Mutex<()>,
}

pub(crate) struct CleanupLease<'a> {
    _guard: MutexGuard<'a, ()>,
    binlog: Option<crate::binlog::LowSpaceDdlBinlog>,
}

impl CleanupLease<'_> {
    pub(crate) fn require_binlog_durability(&self) {
        if let Some(binlog) = &self.binlog {
            binlog.require_durability();
        }
    }
}

impl DiskPressure {
    pub(crate) fn open(root: &Path, wal_segment: u64) -> Result<Option<Self>, QueryError> {
        let reserve_file = root.join(RESERVE_FILE);
        let reserve = config_bytes("MYDB_DISK_RECOVERY_RESERVE_BYTES")?
            .unwrap_or(DEFAULT_RECOVERY_RESERVE_BYTES);
        if reserve == 0 {
            // Changing the startup configuration must not strand reserved
            // blocks. This path is reached with the data directory locked.
            remove_reserve(&reserve_file)?;
            return Ok(None);
        }
        if !RECOVERY_RESERVE_SUPPORTED {
            return Err(ER_WRONG_VALUE.error("disk recovery reserves require Linux or macOS"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let actual_device = |path: &Path| {
                fs::metadata(path).map(|meta| meta.dev()).map_err(|err| {
                    crate::util::io_error::map_io_error("disk protection volume", err)
                })
            };
            let device = actual_device(root)?;
            for directory in [
                "system",
                "system/wal",
                "system/undo",
                "objects",
                "system/catalog",
            ] {
                let path = root.join(directory);
                if path.exists() && actual_device(&path)? != device {
                    return Err(ER_WRONG_VALUE.error(
                        "disk recovery reserves require data and WAL files on the same filesystem",
                    ));
                }
            }
        }
        let low = config_bytes("MYDB_DISK_WRITE_MIN_FREE_BYTES")?
            .unwrap_or_else(|| wal_segment.saturating_mul(2).saturating_add(32 * MIB));
        if low < wal_segment.saturating_add(32 * MIB) {
            return Err(ER_WRONG_VALUE
                .error("disk_write_min_free_bytes must cover one WAL segment plus 32 MiB"));
        }
        let resume = low.saturating_add((low / 8).max(4 * MIB));
        let free = volume_available(root).unwrap_or(0);
        let mut armed = reserve_is_allocated(&reserve_file, reserve)?;
        // Only startup, under the directory lock, may spend the recovery
        // reserve. A second crash simply leaves the physical reserve absent.
        if !armed || free < reserve || free < low {
            if remove_reserve(&reserve_file)? {
                crate::util::logging::log_message(
                    "disk protection: released dedicated startup recovery reserve",
                );
            }
            armed = false;
        }
        Ok(Some(Self {
            root: root.to_owned(),
            reserve,
            low,
            resume,
            wal_segment,
            started: AtomicBool::new(false),
            armed: AtomicBool::new(armed),
            limited: AtomicBool::new(true),
            available: AtomicU64::new(free),
            sampled_until: AtomicU64::new(0),
            clock: Instant::now(),
            sampler: Mutex::new(()),
            cleanup: Mutex::new(()),
        }))
    }

    pub(crate) fn start(&self) {
        self.started.store(true, Ordering::Release);
        self.refresh(0, false);
    }

    pub(crate) fn available(&self) -> u64 {
        self.available.load(Ordering::Relaxed)
    }

    pub(crate) fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }

    pub(crate) fn limited(&self, dirty_bytes: u64) -> bool {
        self.started.load(Ordering::Acquire)
            && (self.refresh(dirty_bytes, false) || self.cleanup.is_locked())
    }

    #[cfg(unix)]
    pub(crate) fn spill_floor(&self, path: &Path, dirty_bytes: u64) -> u64 {
        use std::os::unix::fs::MetadataExt;
        let data_dev = fs::metadata(&self.root).map(|metadata| metadata.dev());
        let spill_dev = fs::metadata(path).map(|metadata| metadata.dev());
        if matches!((data_dev, spill_dev), (Ok(a), Ok(b)) if a != b) {
            0
        } else {
            self.low.saturating_add(dirty_bytes)
        }
    }

    pub(crate) fn check_write(&self, dirty_bytes: u64) -> Result<(), QueryError> {
        if self.limited(dirty_bytes) {
            return Err(limited_error(format!(
                "ordinary writes are paused ({} bytes available, minimum {} bytes plus pending data pages)",
                self.available(), self.low,
            )));
        }
        Ok(())
    }

    pub(crate) fn allows_wal_preallocation(&self, dirty_bytes: u64) -> bool {
        !self.limited(dirty_bytes)
            && self.available()
                >= self
                    .low
                    .saturating_add(dirty_bytes)
                    .saturating_add(self.wal_segment)
    }

    pub(crate) fn cleanup(
        &self,
        dirty_bytes: u64,
        manifest_bytes: u64,
        files: usize,
        binlogged_ddl: bool,
    ) -> Result<Option<CleanupLease<'_>>, QueryError> {
        if !self.started.load(Ordering::Acquire) {
            return Ok(None);
        }
        let guard = self
            .cleanup
            .try_lock()
            .ok_or_else(|| limited_error("another cleanup is running"))?;
        let binlog = if binlogged_ddl {
            crate::binlog::budget_low_space_ddl()?
        } else {
            None
        };
        self.refresh(dirty_bytes, true);
        // Cover segment rotation and the catalog/undo/redo and directory
        // effects of a bounded object retirement, plus pending data pages.
        // Inodes are checked by the volume probe even for small objects.
        let mut required = self
            .wal_segment
            .saturating_add(24 * MIB)
            .saturating_add(manifest_bytes.saturating_mul(16))
            .saturating_add((files as u64).saturating_mul(64 * 1024))
            .saturating_add(dirty_bytes);
        if let Some(binlog) = &binlog {
            let (available, unit) = volume_space(&binlog.directory)
                .ok_or_else(|| limited_error("cannot determine available binary log space"))?;
            let needed = binlog.required_bytes(unit);
            if same_volume(&self.root, &binlog.directory)? {
                required = required.saturating_add(needed);
            } else if available < needed {
                return Err(limited_error(format!(
                    "cleanup needs {needed} bytes on the binary log volume ({available} available)",
                )));
            }
        }
        if self.available() < required {
            let scope = if binlog.is_some() {
                " including binary log space"
            } else {
                ""
            };
            return Err(limited_error(format!(
                "cleanup needs at least {required} bytes of headroom{scope} ({} available)",
                self.available()
            )));
        }
        Ok(Some(CleanupLease {
            _guard: guard,
            binlog,
        }))
    }

    fn refresh(&self, dirty_bytes: u64, force: bool) -> bool {
        let now = self.clock.elapsed().as_millis() as u64;
        let free = self.available();
        // Account for unmaterialized dirty pages even between samples.
        if free < self.low.saturating_add(dirty_bytes) {
            self.limited.store(true, Ordering::Release);
        }
        if !force && now < self.sampled_until.load(Ordering::Acquire) {
            return self.limited.load(Ordering::Acquire);
        }
        let _guard = self.sampler.lock();
        if !force && now < self.sampled_until.load(Ordering::Acquire) {
            return self.limited.load(Ordering::Acquire);
        }
        let mut free = volume_available(&self.root).unwrap_or(0);
        if self.is_armed()
            && !reserve_is_allocated(&self.root.join(RESERVE_FILE), self.reserve).unwrap_or(false)
        {
            self.armed.store(false, Ordering::Release);
        }
        let resume = self.resume.saturating_add(dirty_bytes);
        if !self.is_armed() && !force {
            // A missing reserve can be rebuilt only out of ordinary headroom.
            // The caller's SQL never starts a write in the unarmed interval.
            if free >= resume.saturating_add(self.reserve) {
                if let Some(_cleanup) = self.cleanup.try_lock() {
                    self.rearm();
                    free = volume_available(&self.root).unwrap_or(0);
                }
            }
        }
        self.available.store(free, Ordering::Relaxed);
        let threshold = if self.limited.load(Ordering::Acquire) {
            resume
        } else {
            self.low.saturating_add(dirty_bytes)
        };
        let limited = !self.is_armed() || free < threshold;
        let prior = self.limited.swap(limited, Ordering::Release);
        if prior != limited {
            crate::util::logging::log_message(format_args!(
                "disk protection: ordinary writes {}; {free} bytes available",
                if limited { "paused" } else { "resumed" },
            ));
        }
        self.sampled_until
            .store(now.saturating_add(SAMPLE_MS), Ordering::Release);
        limited
    }

    fn rearm(&self) {
        let path = self.root.join(RESERVE_FILE);
        let result = create_reserve(&path, self.reserve);
        match result {
            Ok(()) => {
                self.armed.store(true, Ordering::Release);
                crate::util::logging::log_message(format_args!(
                    "disk protection: armed {} byte recovery reserve",
                    self.reserve
                ));
            }
            Err(err) => {
                crate::util::logging::log_message(format_args!("disk recovery reserve: {err}"));
            }
        }
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;

    #[test]
    fn recovery_space_is_allocated_and_released_independently_of_wal() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let system = dir.path().join("system");
        fs::create_dir(&system).unwrap();
        let wal = system.join("wal");
        fs::create_dir(&wal).unwrap();
        let record = wal.join("existing.wal");
        fs::write(&record, b"retained").unwrap();
        let reserve = dir.path().join(RESERVE_FILE);
        create_reserve(&reserve, 2 * MIB).unwrap();
        assert!(reserve_is_allocated(&reserve, 2 * MIB).unwrap());
        assert!(remove_reserve(&reserve).unwrap());
        assert!(!remove_reserve(&reserve).unwrap());
        assert_eq!(fs::read(&record).unwrap(), b"retained");
    }
}

pub(crate) fn limited_error(message: impl std::fmt::Display) -> QueryError {
    ER_DISK_FULL_NOWAIT.diagnostic(
        format!("Disk space protection: {message}"),
        "MYDB-DIAG-DISK-PRESSURE",
        "free space on the data volume or request an eligible single-object cleanup",
    )
}

fn config_bytes(name: &str) -> Result<Option<u64>, QueryError> {
    let Some(value) = crate::startup_config::var_os(name) else {
        return Ok(None);
    };
    value
        .to_str()
        .and_then(|raw| crate::startup_config::parse_scaled_u64(raw, 1))
        .filter(|&bytes| bytes <= i64::MAX as u64)
        .map(Some)
        .ok_or_else(|| ER_WRONG_VALUE.error(format!("invalid byte count for {name}")))
}

fn volume_available(path: &Path) -> Option<u64> {
    volume_space(path).map(|(bytes, _)| bytes)
}

fn same_volume(a: &Path, b: &Path) -> Result<bool, QueryError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let device = |path: &Path| {
            fs::metadata(path)
                .map(|meta| meta.dev())
                .map_err(|err| crate::util::io_error::map_io_error("cleanup volume", err))
        };
        Ok(device(a)? == device(b)?)
    }
    #[cfg(not(unix))]
    {
        let _ = (a, b);
        Err(limited_error("cannot identify cleanup volumes"))
    }
}

fn volume_space(path: &Path) -> Option<(u64, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
        let mut info = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        // SAFETY: both pointers are valid for statvfs and the result is
        // initialized when it succeeds.
        if unsafe { libc::statvfs(path.as_ptr(), info.as_mut_ptr()) } != 0 {
            return None;
        }
        let info = unsafe { info.assume_init() };
        let unit = info.f_frsize.max(1) as u64;
        if info.f_files != 0 && info.f_favail < 32 {
            return Some((0, unit));
        }
        Some(((info.f_bavail as u64).saturating_mul(unit), unit))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

fn reserve_is_allocated(path: &Path, bytes: u64) -> Result<bool, QueryError> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(crate::util::io_error::map_io_error(
                "disk recovery reserve",
                err,
            ))
        }
    };
    if !meta.is_file() {
        return Err(ER_WRONG_VALUE.error("disk recovery reserve must be a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(meta.nlink() == 1 && meta.len() == bytes && meta.blocks().saturating_mul(512) >= bytes)
    }
    #[cfg(not(unix))]
    {
        let _ = bytes;
        Ok(false)
    }
}

fn remove_reserve(path: &Path) -> Result<bool, QueryError> {
    let result = match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() => fs::remove_file(path)
            .and_then(|()| crate::platform::fs::sync_path(path.parent().unwrap())),
        Ok(_) => return Err(ER_WRONG_VALUE.error("disk recovery reserve must be a regular file")),
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => Err(err),
    };
    result
        .map(|()| true)
        .map_err(|err| crate::util::io_error::map_io_error("disk recovery reserve", err))
}

fn create_reserve(path: &Path, bytes: u64) -> Result<(), QueryError> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|err| crate::util::io_error::map_io_error("disk recovery reserve", err))?;
    let result = crate::wal::allocate_physical_range(&file, bytes)
        .and_then(|()| crate::platform::fs::sync_file_at(&file, path))
        .and_then(|()| crate::platform::fs::sync_path(path.parent().unwrap()));
    drop(file);
    let result = result
        .map_err(|err| crate::util::io_error::map_io_error("disk recovery reserve", err))
        .and_then(|()| {
            if reserve_is_allocated(path, bytes)? {
                Ok(())
            } else {
                Err(limited_error(
                    "filesystem could not physically allocate the recovery reserve",
                ))
            }
        });
    if result.is_err() {
        remove_reserve(path)?;
    }
    result
}
