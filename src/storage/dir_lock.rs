//! Exclusive per-data-directory lock.
//!
//! Prevents two `mydb` processes from opening the same `MYDB_DATA_DIR`
//! concurrently. Without it, restarting the server immediately after a
//! `SHUTDOWN` (the ordinary "stop then start" cycle) can race the tail of
//! the previous process's graceful shutdown: the accept listener is
//! dropped — and the TCP port freed — *before* the final checkpoint runs
//! its WAL collapse (`compact_collapsing_active` deletes every sealed
//! segment and installs one fresh ~96-byte segment). A new process that
//! binds the just-freed port and starts recovery then scans the WAL
//! directory while the old process is still mid-collapse and trips over a
//! segment that got deleted between `read_dir` and `open`:
//!
//! ```text
//! Error: "WAL I/O failed: No such file or directory (os error 2)"
//! ```
//!
//! Two live servers on one data directory is catastrophic regardless
//! (both append WAL, both mutate the catalog and buffer pool), so the fix
//! is to make the second open *wait for, then refuse* the directory —
//! never to corrupt it.
//!
//! Mechanism: `flock(LOCK_EX | LOCK_NB)` on `<data_dir>/system/mydb.lock`.
//! The advisory lock is tied to the open file description and released by
//! the kernel when the fd closes — on clean `Drop`, on `abort`/crash, and
//! (because Rust opens files `O_CLOEXEC`) across the `RESTART` in-place
//! `execv`, so the re-exec'd image re-acquires without deadlocking against
//! the descriptor it inherited.
//!
//! Same-process reopen is always allowed via a process-global refcounted
//! registry keyed by the canonical path. This keeps the crash-simulation
//! tests working — they `std::mem::forget` a `Database` (leaking its lock)
//! and immediately reopen the same directory in-process — while a
//! genuinely separate OS process is still refused: its `flock` conflicts
//! at the kernel level, which the in-process registry cannot mask.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::types::QueryError;

/// One held lock. The `File` keeps the `flock` alive; dropping it (when
/// `refs` reaches zero) closes the fd and releases the kernel lock.
struct LockEntry {
    /// Held solely for its RAII effect: closing this fd releases the
    /// kernel `flock`. Never read directly.
    #[allow(dead_code)]
    file: std::fs::File,
    refs: usize,
}

/// Process-global map of held data-directory locks, keyed by canonical
/// path. Guards against two in-process `Database` handles fighting over
/// the same directory's single `flock` (two open file descriptions on
/// one file conflict even within a process).
fn registry() -> &'static Mutex<HashMap<PathBuf, LockEntry>> {
    static R: OnceLock<Mutex<HashMap<PathBuf, LockEntry>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(HashMap::new()))
}

/// How long a fresh open waits for a conflicting *other process* to
/// release the lock before giving up. A restart racing the previous
/// process's final checkpoint only needs to wait out that checkpoint
/// (sub-second to a few seconds for a large WAL collapse), so the default
/// is generous; `MYDB_DATA_DIR_LOCK_WAIT_MS=0` fails fast instead.
fn lock_wait() -> Duration {
    let ms = std::env::var("MYDB_DATA_DIR_LOCK_WAIT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(10_000);
    Duration::from_millis(ms)
}

/// RAII guard held for the lifetime of a `Database`. While alive it counts
/// one reference against the process-global lock for its canonical path.
/// The last guard to drop closes the backing file and releases the
/// `flock`.
pub(crate) struct DataDirLock {
    canonical: PathBuf,
}

impl std::fmt::Debug for DataDirLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataDirLock")
            .field("canonical", &self.canonical)
            .finish()
    }
}

impl DataDirLock {
    /// Acquire the exclusive lock for `data_dir`. Returns immediately if
    /// this process already holds it (another live `Database`, or a
    /// crash-simulating `mem::forget`); waits up to [`lock_wait`] for a
    /// different process to release it, then errors with a clear message.
    pub(crate) fn acquire(data_dir: &Path) -> Result<Self, QueryError> {
        Self::acquire_with_wait(data_dir, lock_wait())
    }

    /// [`acquire`](Self::acquire) with an explicit wait budget. Split out so
    /// tests can exercise the "held by another process" timeout path
    /// deterministically without racing the process-global env var.
    pub(crate) fn acquire_with_wait(data_dir: &Path, wait: Duration) -> Result<Self, QueryError> {
        // Canonicalize so two spellings / symlinks of one directory map to
        // the same registry key. Falls back to the literal path if the
        // directory somehow can't be resolved (it was just created, so this
        // is defensive).
        let canonical = std::fs::canonicalize(data_dir).unwrap_or_else(|_| data_dir.to_path_buf());
        let lock_path = crate::storage::naming::system_dir(data_dir).join("mydb.lock");
        let deadline = Instant::now() + wait;
        loop {
            {
                let mut map = registry().lock().unwrap();
                if let Some(entry) = map.get_mut(&canonical) {
                    // Held by this process already — same-process reopen.
                    entry.refs += 1;
                    return Ok(DataDirLock { canonical });
                }
                match try_flock(&lock_path) {
                    FlockOutcome::Acquired(file) => {
                        map.insert(canonical.clone(), LockEntry { file, refs: 1 });
                        return Ok(DataDirLock { canonical });
                    }
                    // Held by another OS process: release the registry
                    // mutex and wait below (so opens of *other* dirs don't
                    // block behind this one).
                    FlockOutcome::Held => {}
                    FlockOutcome::Err(err) => {
                        return Err(crate::storage::error(
                            crate::error::catalog::ER_GET_ERRNO,
                            format!(
                                "failed to lock data directory {}: {}",
                                data_dir.display(),
                                err
                            ),
                        ));
                    }
                }
            }
            if Instant::now() >= deadline {
                let holder = read_holder_pid(&lock_path)
                    .map(|p| format!(" (held by process {p})"))
                    .unwrap_or_default();
                return Err(crate::storage::error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "data directory {} is locked by another mydb process{holder}; \
                         only one server may use a data directory at a time",
                        data_dir.display()
                    ),
                ));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for DataDirLock {
    fn drop(&mut self) {
        let mut map = registry().lock().unwrap();
        if let Some(entry) = map.get_mut(&self.canonical) {
            entry.refs -= 1;
            if entry.refs == 0 {
                // Dropping the `File` closes the fd, releasing the flock.
                map.remove(&self.canonical);
            }
        }
    }
}

enum FlockOutcome {
    Acquired(std::fs::File),
    Held,
    Err(std::io::Error),
}

#[cfg(unix)]
fn try_flock(lock_path: &Path) -> FlockOutcome {
    use std::io::Write;
    use std::os::unix::io::AsRawFd;
    let read_only = crate::storage::image_io::immutable_path(lock_path);
    let mut file = match std::fs::OpenOptions::new()
        .read(true)
        .write(!read_only)
        .create(!read_only)
        .truncate(false)
        .open(lock_path)
    {
        Ok(f) => f,
        Err(e) => return FlockOutcome::Err(e),
    };
    // Non-blocking exclusive lock. EWOULDBLOCK means a live holder exists;
    // any other errno is a real failure (e.g. an fs that doesn't support
    // flock — surface it rather than silently running unprotected).
    let rc = unsafe {
        libc::flock(
            file.as_raw_fd(),
            (if read_only {
                libc::LOCK_SH
            } else {
                libc::LOCK_EX
            }) | libc::LOCK_NB,
        )
    };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        return match err.raw_os_error() {
            Some(e) if e == libc::EWOULDBLOCK || e == libc::EAGAIN => FlockOutcome::Held,
            _ => FlockOutcome::Err(err),
        };
    }
    // Record our pid for diagnostics (best-effort). Truncate first: a stale
    // pid from a crashed prior holder must not linger.
    if !read_only {
        let _ = file.set_len(0);
        let _ = write!(file, "{}", std::process::id());
        let _ = file.flush();
    }
    FlockOutcome::Acquired(file)
}

/// Windows uses a mandatory byte-range lock outside the PID diagnostics.
#[cfg(windows)]
fn try_flock(lock_path: &Path) -> FlockOutcome {
    use std::io::Write;
    let read_only = crate::storage::image_io::immutable_path(lock_path);
    let mut file = match std::fs::OpenOptions::new()
        .read(true)
        .write(!read_only)
        .create(!read_only)
        .truncate(false)
        .open(lock_path)
    {
        Ok(file) => file,
        Err(error) => return FlockOutcome::Err(error),
    };
    match crate::platform::lock::try_exclusive(&file) {
        Ok(true) => {
            if !read_only {
                let _ = file.set_len(0);
                let _ = write!(file, "{}", std::process::id());
                let _ = file.flush();
            }
            FlockOutcome::Acquired(file)
        }
        Ok(false) => FlockOutcome::Held,
        Err(error) => FlockOutcome::Err(error),
    }
}

#[cfg(not(any(unix, windows)))]
fn try_flock(_lock_path: &Path) -> FlockOutcome {
    FlockOutcome::Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "data-directory locking is unsupported on this platform",
    ))
}

/// Best-effort read of the pid the current holder recorded in the lock
/// file, for a friendlier "locked by process N" diagnostic.
fn read_holder_pid(lock_path: &Path) -> Option<u32> {
    std::fs::read_to_string(lock_path)
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
}

#[cfg(test)]
mod process_tests {
    use super::*;
    use std::process::{Command, Stdio};

    #[test]
    fn lock_child() {
        let Some(root) = std::env::var_os("MYDB_LOCK_CHILD_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let lock = DataDirLock::acquire_with_wait(&root, Duration::ZERO);
        if std::env::var_os("MYDB_LOCK_CHILD_CONTEND").is_some() {
            assert!(lock
                .unwrap_err()
                .message()
                .contains("locked by another mydb process"));
            return;
        }
        let _lock = lock.unwrap();
        std::fs::write(root.join("child-ready"), b"ready").unwrap();
        loop {
            std::thread::park_timeout(Duration::from_secs(1));
        }
    }

    #[test]
    fn independent_process_lock_survives_contention_and_releases_on_crash() {
        let temp = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let root = temp.path();
        std::fs::create_dir_all(crate::storage::naming::system_dir(root)).unwrap();
        let child_command = || {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "storage::dir_lock::process_tests::lock_child",
                    "--nocapture",
                ])
                .env("MYDB_LOCK_CHILD_ROOT", root)
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            command
        };
        let lock = DataDirLock::acquire_with_wait(root, Duration::ZERO).unwrap();
        let same_process = DataDirLock::acquire_with_wait(root, Duration::ZERO).unwrap();
        let mut contender = child_command();
        assert!(contender
            .env("MYDB_LOCK_CHILD_CONTEND", "1")
            .status()
            .unwrap()
            .success());
        drop(same_process);
        drop(lock);
        let mut holder = child_command().spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !root.join("child-ready").exists() {
            if holder.try_wait().unwrap().is_some() || Instant::now() >= deadline {
                let _ = holder.kill();
                let _ = holder.wait();
                panic!("lock holder did not become ready");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let refused = DataDirLock::acquire_with_wait(root, Duration::ZERO).is_err();
        holder.kill().unwrap();
        holder.wait().unwrap();
        assert!(refused);
        let recovered = DataDirLock::acquire_with_wait(root, Duration::from_secs(2)).unwrap();
        drop(recovered);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::io::AsRawFd;

    fn make_system_dir(dir: &Path) {
        std::fs::create_dir_all(crate::storage::naming::system_dir(dir)).unwrap();
    }

    #[test]
    fn same_process_reopen_is_allowed() {
        // A crash-simulating `mem::forget(db)` leaks the first guard; the
        // reopen must still succeed in-process (the registry refcounts).
        let dir = tempfile::tempdir().unwrap();
        make_system_dir(dir.path());
        let a = DataDirLock::acquire(dir.path()).unwrap();
        let b = DataDirLock::acquire(dir.path()).unwrap();
        drop(a);
        drop(b);
        // Both refs dropped -> the flock is released; a fresh acquire works.
        let c = DataDirLock::acquire(dir.path()).unwrap();
        drop(c);
    }

    #[test]
    fn foreign_holder_blocks_then_release_unblocks() {
        // Simulate a *separate* mydb process holding the lock: a raw fd the
        // in-process registry knows nothing about. This is exactly the
        // restart-races-shutdown-collapse scenario the lock exists to stop.
        let dir = tempfile::tempdir().unwrap();
        make_system_dir(dir.path());
        let lock_path = crate::storage::naming::system_dir(dir.path()).join("mydb.lock");
        let foreign = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        assert_eq!(
            unsafe { libc::flock(foreign.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );

        // A bounded-wait acquire must give up rather than race into the
        // directory the "other process" is mutating.
        let err =
            DataDirLock::acquire_with_wait(dir.path(), Duration::from_millis(150)).unwrap_err();
        assert!(
            err.message().contains("locked by another mydb process"),
            "unexpected error: {}",
            err.message()
        );

        // Release the foreign holder; the next acquire succeeds.
        assert_eq!(
            unsafe { libc::flock(foreign.as_raw_fd(), libc::LOCK_UN) },
            0
        );
        drop(foreign);
        let ok = DataDirLock::acquire_with_wait(dir.path(), Duration::from_millis(150)).unwrap();
        drop(ok);
    }
}
