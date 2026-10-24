//! Process-level shutdown / restart coordinator.
//!
//! Single source of truth that lets any connection task (a SQL `SHUTDOWN`
//! / `RESTART` statement or a `COM_SHUTDOWN` command) or an OS signal
//! (SIGINT / SIGTERM) request a controlled server shutdown, and lets the
//! accept loop observe that request and run the graceful-shutdown
//! sequence exactly once.
//!
//! MySQL 8.0 parity:
//! - `SHUTDOWN` / `COM_SHUTDOWN` → intent `Halt` → process exits.
//! - `RESTART` → intent `Restart` → after the same graceful sequence the
//!   process re-execs its own binary on Unix (`execv`, same PID) or
//!   launches a replacement process on Windows.
//!
//! Both intents share one coordinator so concurrent requests collapse to
//! a single shutdown; the first request to win the `requested` CAS also
//! pins the intent (a later `SHUTDOWN` cannot downgrade an in-flight
//! `RESTART`, nor vice-versa).

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use tokio::sync::Notify;

use crate::util::logging::log_message;

/// Terminal action taken after the graceful-shutdown sequence completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownIntent {
    /// Exit the process (`SHUTDOWN` / `COM_SHUTDOWN` / signal).
    Halt,
    /// Relaunch the server binary (`RESTART`).
    Restart,
}

impl ShutdownIntent {
    fn as_u8(self) -> u8 {
        match self {
            ShutdownIntent::Halt => 0,
            ShutdownIntent::Restart => 1,
        }
    }

    fn from_u8(value: u8) -> Self {
        if value == 1 {
            ShutdownIntent::Restart
        } else {
            ShutdownIntent::Halt
        }
    }
}

/// Where a shutdown request originated (for the log line only).
#[derive(Debug, Clone)]
pub enum ShutdownSource {
    Signal(&'static str),
    Sql { conn_id: u32, user: String },
    Com { conn_id: u32 },
    Restart { conn_id: u32, user: String },
}

impl ShutdownSource {
    fn describe(&self) -> String {
        match self {
            ShutdownSource::Signal(name) => format!("signal {name}"),
            ShutdownSource::Sql { conn_id, user } => {
                format!("SQL SHUTDOWN (connection {conn_id}, user {user})")
            }
            ShutdownSource::Com { conn_id } => format!("COM_SHUTDOWN (connection {conn_id})"),
            ShutdownSource::Restart { conn_id, user } => {
                format!("SQL RESTART (connection {conn_id}, user {user})")
            }
        }
    }
}

/// Number of independent wake lists. Every idle command wait registers a
/// `Notified` future, and `Notify` guards its waiter list with one mutex, so a
/// single process-wide list makes every connection thread contend on the same
/// lock and cache line twice per statement. Waiters are spread across shards
/// by thread instead; `request_shutdown` wakes every shard.
const NOTIFY_SHARDS: usize = 64;

#[allow(clippy::declare_interior_mutable_const)]
const NOTIFY_INIT: Notify = Notify::const_new();

struct Coordinator {
    requested: AtomicBool,
    intent: AtomicU8,
    notify: [Notify; NOTIFY_SHARDS],
    next_shard: AtomicUsize,
    source: Mutex<Option<ShutdownSource>>,
}

static COORD: Coordinator = Coordinator {
    requested: AtomicBool::new(false),
    intent: AtomicU8::new(0),
    notify: [NOTIFY_INIT; NOTIFY_SHARDS],
    next_shard: AtomicUsize::new(0),
    source: Mutex::new(None),
};

thread_local! {
    static THREAD_SHARD: usize =
        COORD.next_shard.fetch_add(1, Ordering::Relaxed) % NOTIFY_SHARDS;
}

/// Request a controlled shutdown/restart. Idempotent: only the first
/// caller flips the flag, records the intent + source, and wakes the
/// accept loop. Later callers are no-ops (aside from a redundant wake).
pub fn request_shutdown(source: ShutdownSource, intent: ShutdownIntent) {
    if COORD
        .requested
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        COORD.intent.store(intent.as_u8(), Ordering::SeqCst);
        if let Ok(mut slot) = COORD.source.lock() {
            *slot = Some(source);
        }
    }
    // Wake the accept loop whether or not we won the CAS: a request that
    // raced just after the loop re-armed its `shutdown_notified()` future
    // must still be delivered.
    for shard in &COORD.notify {
        shard.notify_waiters();
    }
}

/// True once any shutdown/restart has been requested.
pub fn is_shutting_down() -> bool {
    COORD.requested.load(Ordering::SeqCst)
}

/// The pinned terminal action (only meaningful once `is_shutting_down`).
pub fn intent() -> ShutdownIntent {
    ShutdownIntent::from_u8(COORD.intent.load(Ordering::SeqCst))
}

/// Human-readable description of what triggered shutdown (for logging).
pub fn source_description() -> String {
    COORD
        .source
        .lock()
        .ok()
        .and_then(|slot| slot.as_ref().map(ShutdownSource::describe))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Resolves when a shutdown has been requested. Safe against the classic
/// lost-wakeup race: the `Notified` future is registered (`enable`) before
/// the flag is re-checked, and `request_shutdown` sets the flag *before*
/// `notify_waiters`, so no request can slip between the check and the wait.
pub async fn shutdown_notified() {
    let shard = THREAD_SHARD.with(|shard| *shard);
    let fut = COORD.notify[shard].notified();
    tokio::pin!(fut);
    fut.as_mut().enable();
    if is_shutting_down() {
        return;
    }
    fut.await;
}

// ----- restart (re-exec) support -----------------------------------------

struct LaunchArgs {
    exe: std::ffi::OsString,
    argv: Vec<std::ffi::OsString>,
}

static RESTART_USE_PERSISTED_ROLE: AtomicBool = AtomicBool::new(false);

pub(crate) fn use_persisted_role_on_restart() {
    RESTART_USE_PERSISTED_ROLE.store(true, Ordering::Release);
}

fn restart_environment(
    env: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
    persisted_role: bool,
) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    let mut env: Vec<_> = env
        .into_iter()
        .filter(|(key, _)| {
            !matches!(
                key.to_str(),
                Some("MYDB_WAL_FORCE_RECOVER" | "MYDB_UNDO_FORCE_RECOVER" | "MYDB_REPAIR_ROLE")
            ) && !(persisted_role && key == "MYDB_ROLE")
        })
        .collect();
    if persisted_role {
        env.push(("MYDB_ROLE".into(), "auto".into()));
    }
    env
}

static LAUNCH: OnceLock<LaunchArgs> = OnceLock::new();

/// Snapshot the program path and argv at startup so `RESTART` can re-exec
/// the exact same invocation. Call once from `main` before the runtime
/// mutates anything. Idempotent.
pub fn cache_launch_args() {
    let argv: Vec<_> = std::env::args_os().collect();
    let exe = std::env::current_exe().ok().or_else(|| {
        resolve_launch_path(argv.first()?.as_ref(), std::env::var_os("PATH").as_deref())
    });
    let _ = LAUNCH.set(LaunchArgs {
        exe: exe.map(Into::into).unwrap_or_default(),
        argv,
    });
}

/// Resolve argv[0] at startup, before cwd/PATH can change. execve itself
/// does not search PATH. Failure is a capability rejection before shutdown.
fn resolve_launch_path(
    argv0: &std::path::Path,
    search_path: Option<&std::ffi::OsStr>,
) -> Option<std::path::PathBuf> {
    fn executable(path: &std::path::Path) -> Option<std::path::PathBuf> {
        let path = path.canonicalize().ok()?;
        let metadata = path.metadata().ok()?;
        if !metadata.is_file() {
            return None;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o111 == 0 {
                return None;
            }
        }
        Some(path)
    }
    if argv0.as_os_str().is_empty() {
        return None;
    }
    if argv0.is_absolute() || argv0.components().count() > 1 {
        return executable(argv0);
    }
    std::env::split_paths(search_path?).find_map(|dir| executable(&dir.join(argv0)))
}

fn env_is_truthy(var: &str) -> bool {
    std::env::var_os(var)
        .map(|value| {
            let value = value.to_string_lossy();
            let value = value.trim();
            value == "1" || value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("on")
        })
        .unwrap_or(false)
}

/// Whether `RESTART` can relaunch the server, and is not disabled via
/// `MYDB_RESTART_DISABLE`, with a cached executable path. Otherwise error 3707.
pub fn restart_supported() -> bool {
    cfg!(any(unix, windows))
        && !env_is_truthy("MYDB_RESTART_DISABLE")
        && LAUNCH.get().is_some_and(|launch| !launch.exe.is_empty())
}

/// Drain-timeout for the graceful-shutdown connection wait, from
/// `MYDB_SHUTDOWN_TIMEOUT_MS` (default 60000). `0` skips the wait.
pub fn shutdown_timeout() -> std::time::Duration {
    let ms = std::env::var("MYDB_SHUTDOWN_TIMEOUT_MS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(60_000);
    std::time::Duration::from_millis(ms)
}

/// Replace the current process image with a fresh invocation of the same
/// binary (`execv`, same PID). Never returns on success. On failure (or
/// unsupported platform) it logs and exits non-zero, since the engine has
/// already been shut down and the process cannot serve.
///
/// Safe re-exec relies on all data-dir / WAL / lock / socket fds being
/// `O_CLOEXEC` (Rust std / tokio default on Linux+macOS), so `execve`
/// closes them and the fresh image re-opens, re-locks, and re-binds
/// without conflict. Must be called only after the final checkpoint has
/// durably flushed (see `graceful_shutdown`).
#[cfg(unix)]
pub fn re_exec() -> ! {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let Some(launch) = LAUNCH.get() else {
        log_message("RESTART: launch args were never cached; cannot re-exec, exiting");
        std::process::exit(1);
    };

    let path = match CString::new(launch.exe.as_bytes()) {
        Ok(path) => path,
        Err(_) => {
            log_message("RESTART: executable path contains NUL byte; cannot re-exec, exiting");
            std::process::exit(1);
        }
    };
    let mut argv: Vec<CString> = Vec::with_capacity(launch.argv.len().max(1));
    let persisted_role = RESTART_USE_PERSISTED_ROLE.load(Ordering::Acquire);
    for arg in &launch.argv {
        match CString::new(arg.as_bytes()) {
            Ok(c) => argv.push(c),
            Err(_) => {
                log_message("RESTART: argument contains NUL byte; cannot re-exec, exiting");
                std::process::exit(1);
            }
        }
    }
    if argv.is_empty() {
        argv.push(path.clone());
    }
    let mut ptrs: Vec<*const libc::c_char> = argv.iter().map(|c| c.as_ptr()).collect();
    ptrs.push(std::ptr::null());

    let env: Vec<CString> = restart_environment(std::env::vars_os(), persisted_role)
        .into_iter()
        .map(|(key, value)| {
            let mut bytes = key.as_bytes().to_vec();
            bytes.push(b'=');
            bytes.extend_from_slice(value.as_bytes());
            CString::new(bytes).expect("environment contains NUL")
        })
        .collect();
    let mut env_ptrs: Vec<*const libc::c_char> = env.iter().map(|s| s.as_ptr()).collect();
    env_ptrs.push(std::ptr::null());
    log_message("RESTART: re-executing server binary in place");
    // SAFETY: `path` and `ptrs` are valid, NUL-terminated C strings that
    // outlive the call; argv and the filtered environment are null-terminated.
    unsafe {
        libc::execve(path.as_ptr(), ptrs.as_ptr(), env_ptrs.as_ptr());
    }
    // Only reached if execv failed.
    let err = std::io::Error::last_os_error();
    log_message(format_args!("RESTART: execv failed: {err}; exiting"));
    std::process::exit(1);
}

#[cfg(windows)]
pub fn re_exec() -> ! {
    let Some(launch) = LAUNCH.get() else {
        log_message("RESTART: launch args were not cached; exiting");
        std::process::exit(1);
    };
    // Windows cannot replace a process image. Graceful shutdown has drained
    // connections and checkpointed; the child waits for the data-dir lock
    // while this process exits and closes its remaining handles. Inherit
    // cwd, environment and stdio, preserving the original invocation.
    let persisted_role = RESTART_USE_PERSISTED_ROLE.load(Ordering::Acquire);
    match std::process::Command::new(&launch.exe)
        .args(launch.argv.iter().skip(1))
        .env_clear()
        .envs(restart_environment(std::env::vars_os(), persisted_role))
        .spawn()
    {
        Ok(child) => {
            log_message(format_args!(
                "RESTART: relaunched as process {}",
                child.id()
            ));
            std::process::exit(0);
        }
        Err(error) => {
            log_message(format_args!("RESTART: relaunch failed: {error}; exiting"));
            std::process::exit(1);
        }
    }
}

#[cfg(not(any(unix, windows)))]
pub fn re_exec() -> ! {
    log_message("RESTART: in-process re-exec is unsupported on this platform; exiting");
    std::process::exit(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intent_roundtrip() {
        assert_eq!(ShutdownIntent::from_u8(0), ShutdownIntent::Halt);
        assert_eq!(ShutdownIntent::from_u8(1), ShutdownIntent::Restart);
        assert_eq!(ShutdownIntent::Halt.as_u8(), 0);
        assert_eq!(ShutdownIntent::Restart.as_u8(), 1);
    }

    #[test]
    fn env_truthiness() {
        std::env::set_var("MYDB_TEST_TRUTHY", "1");
        assert!(env_is_truthy("MYDB_TEST_TRUTHY"));
        std::env::set_var("MYDB_TEST_TRUTHY", "off");
        assert!(!env_is_truthy("MYDB_TEST_TRUTHY"));
        std::env::remove_var("MYDB_TEST_TRUTHY");
        assert!(!env_is_truthy("MYDB_TEST_TRUTHY"));
    }

    /// Waiters are spread over per-thread wake lists; one request must still
    /// reach every list, including waiters registered on distinct shards.
    #[test]
    fn shutdown_request_wakes_waiters_on_every_thread_shard() {
        const WAITERS: usize = 8;
        let ready = std::sync::Arc::new(std::sync::Barrier::new(WAITERS + 1));
        let threads: Vec<_> = (0..WAITERS)
            .map(|_| {
                let ready = std::sync::Arc::clone(&ready);
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    rt.block_on(async {
                        let wait = shutdown_notified();
                        tokio::pin!(wait);
                        // Register this thread's shard before announcing readiness.
                        assert!(futures_poll_once(wait.as_mut()).is_none());
                        ready.wait();
                        tokio::time::timeout(std::time::Duration::from_secs(10), wait)
                            .await
                            .expect("waiter must observe the shutdown request");
                    });
                })
            })
            .collect();
        ready.wait();
        request_shutdown(ShutdownSource::Signal("test"), ShutdownIntent::Halt);
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(is_shutting_down());
    }

    fn futures_poll_once<F: std::future::Future>(
        mut fut: std::pin::Pin<&mut F>,
    ) -> Option<F::Output> {
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        match fut.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(value) => Some(value),
            std::task::Poll::Pending => None,
        }
    }
}

#[cfg(all(test, unix))]
#[test]
fn shutdown_restart_resolves_path_before_exec() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let exe = dir.path().join("mydb");
    std::fs::write(&exe, b"test executable").unwrap();
    std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
    let expected = exe.canonicalize().unwrap();
    assert_eq!(
        resolve_launch_path(std::path::Path::new("mydb"), Some(dir.path().as_os_str())),
        Some(expected.clone())
    );
    let cwd = std::env::current_dir().unwrap();
    let mut relative = std::path::PathBuf::new();
    for _ in cwd
        .components()
        .filter(|part| matches!(part, std::path::Component::Normal(_)))
    {
        relative.push("..");
    }
    relative.push(exe.strip_prefix("/").unwrap());
    assert_eq!(resolve_launch_path(&relative, None), Some(expected.clone()));
    assert_eq!(resolve_launch_path(&exe, None), Some(expected));
    assert_eq!(
        resolve_launch_path(
            std::path::Path::new("missing"),
            Some(dir.path().as_os_str())
        ),
        None
    );
    std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(resolve_launch_path(&exe, None), None);
    assert_eq!(resolve_launch_path(dir.path(), None), None);
}

#[cfg(test)]
mod audit1022_restart_tests {
    use super::*;
    #[test]
    fn audit1022_resync_restart_uses_auto_for_environment_role() {
        let env = vec![
            ("MYDB_ROLE".into(), "master".into()),
            ("MYDB_DATA_DIR".into(), "/tmp/mydb/resync".into()),
        ];
        let changed = restart_environment(env, true);
        assert!(changed.contains(&("MYDB_ROLE".into(), "auto".into())));
        assert!(changed.contains(&("MYDB_DATA_DIR".into(), "/tmp/mydb/resync".into())));
        assert_eq!(changed.len(), 2);
    }
}
