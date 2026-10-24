use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

const DEFAULT_MAX_CONNECTIONS: usize = 1000;
const ADMIN_CONNECTION_RESERVE: usize = 10;
const DEFAULT_LISTEN_BACKLOG: u32 = 4096;
const DEFAULT_OPEN_FILES_RESERVE: u64 = 512;
const DEFAULT_OPEN_FILES_LIMIT: u64 = 65_535;

static ACTIVE_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);
/// Connections occupying one of the configured `max_connections` slots.
/// Kept separately from `ACTIVE_CONNECTIONS` because administrator-reserved
/// connections do not consume the configured ordinary-client capacity.
static ORDINARY_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);
/// Extra operational connections reserved for accounts holding
/// CONNECTION_ADMIN (SUPER implies it). MySQL guarantees at least one such
/// slot; mydb keeps ten so monitoring and recovery access remain available
/// during a large client fan-out. Privilege is checked after authentication.
static ADMIN_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);
static ACCEPTED_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
static REJECTED_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
static HANDSHAKE_SUCCESSES: AtomicU64 = AtomicU64::new(0);
static HANDSHAKE_FAILURES: AtomicU64 = AtomicU64::new(0);
static THREADS_CREATED: AtomicU64 = AtomicU64::new(0);
static CLOSED_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
/// Established clients that disappeared without a normal COM_QUIT / RELEASE.
static ABORTED_CLIENTS: AtomicU64 = AtomicU64::new(0);
/// Connections refused specifically because `max_connections` was
/// reached — the MySQL `Connection_errors_max_connections` status
/// counter. Tracked separately from `REJECTED_CONNECTIONS` so the
/// status variable maps 1:1 even if other refusal reasons are added.
static CONNECTION_ERRORS_MAX_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
/// Peak of `ACTIVE_CONNECTIONS` since startup / last FLUSH STATUS — the
/// MySQL `Max_used_connections` gauge. Bumped on each successful slot
/// acquire via a monotonic compare-and-max.
static MAX_USED_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);
/// Client statements executed by the server — the MySQL `Questions` /
/// `Queries` status counter. Incremented once per top-level `COM_QUERY` /
/// `COM_STMT_EXECUTE`. mydb does not separately count sub-statements run
/// inside stored programs, so `Queries` mirrors `Questions`.
static QUERIES_EXECUTED: AtomicU64 = AtomicU64::new(0);

/// Per-process upper bound on simultaneously open MySQL connections.
///
/// Initialised from `MYDB_MAX_CONNECTIONS` (default 1000) but read
/// **live** from the `max_connections` system variable so
/// `SET GLOBAL max_connections = N` takes effect for the next accepted
/// connection instead of being silently ignored. When the cap is
/// exceeded, new connections receive MySQL error 1040 ("Too many
/// connections") and are closed before any handshake state is allocated.
/// Lowering the cap never force-closes already-established connections
/// (matching MySQL); it only gates new accepts.
pub fn max_connections() -> usize {
    let configured = crate::engine::sysvar::builtin::connection::max_connections();
    if configured == 0 {
        DEFAULT_MAX_CONNECTIONS
    } else {
        usize::try_from(configured).unwrap_or(usize::MAX)
    }
}

/// File descriptors reserved for storage files, logs, the listener, and
/// short-lived rejected sockets. The connection accept cap is reduced when the
/// process nofile limit cannot safely cover `MYDB_MAX_CONNECTIONS + reserve`.
pub fn open_files_reserve() -> u64 {
    static RESERVE: OnceLock<u64> = OnceLock::new();
    *RESERVE.get_or_init(|| {
        std::env::var("MYDB_OPEN_FILES_RESERVE")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_OPEN_FILES_RESERVE)
    })
}

fn requested_open_files_limit() -> u64 {
    static REQUESTED: OnceLock<u64> = OnceLock::new();
    *REQUESTED.get_or_init(|| {
        crate::util::env::var("MYDB_OPEN_FILES_LIMIT")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_OPEN_FILES_LIMIT)
    })
}

#[derive(Debug, Clone, Copy)]
pub struct OpenFilesLimit {
    pub initial_soft: u64,
    pub soft: u64,
    pub hard: u64,
    pub requested: u64,
    pub reserve: u64,
    pub raised: bool,
    pub capped_by_hard_limit: bool,
    pub set_error: Option<i32>,
}

impl OpenFilesLimit {
    fn unavailable(requested: u64, reserve: u64, set_error: Option<i32>) -> Self {
        Self {
            initial_soft: 0,
            soft: 0,
            hard: 0,
            requested,
            reserve,
            raised: false,
            capped_by_hard_limit: false,
            set_error,
        }
    }

    pub fn effective_max_connections(self, configured_max: usize) -> usize {
        if self.soft == 0 {
            return configured_max;
        }
        let fd_capacity = self
            .soft
            .saturating_sub(self.reserve)
            .saturating_sub(ADMIN_CONNECTION_RESERVE as u64)
            .max(1);
        configured_max.min(fd_capacity.min(usize::MAX as u64) as usize)
    }
}

pub fn configure_open_files_limit() -> OpenFilesLimit {
    static LIMIT: OnceLock<OpenFilesLimit> = OnceLock::new();
    *LIMIT.get_or_init(configure_open_files_limit_inner)
}

pub fn effective_max_connections() -> usize {
    configure_open_files_limit().effective_max_connections(max_connections())
}

/// Listener backlog used by the server accept socket.
///
/// A larger default protects short connection bursts from overflowing
/// the kernel accept queue before the Tokio accept loop can drain it.
pub fn listen_backlog() -> u32 {
    static BACKLOG: OnceLock<u32> = OnceLock::new();
    *BACKLOG.get_or_init(|| {
        crate::util::env::var("MYDB_LISTEN_BACKLOG")
            .ok()
            .and_then(|raw| raw.parse::<u32>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_LISTEN_BACKLOG)
    })
}

/// Timeout for the initial MySQL handshake. `0` disables the timeout.
pub fn handshake_timeout() -> Option<Duration> {
    let secs = crate::engine::sysvar::builtin::connection::connect_timeout_secs();
    (secs != 0).then(|| Duration::from_secs(secs))
}

pub fn active_connection_count() -> usize {
    ACTIVE_CONNECTIONS.load(Ordering::Relaxed)
}

/// `FLUSH STATUS` — reset the process-global status counters that MySQL
/// 8.0 actually resets. MySQL leaves `Connections` and most `Innodb_*`
/// counters intact; the historically reset set is `Aborted_*` plus the
/// `max_used_connections` peak. mydb has no peak gauge to reset (active
/// is read live), so we zero the abort counters only — matching MySQL's
/// observable behaviour for the resettable subset.
pub fn reset_status_peaks() {
    REJECTED_CONNECTIONS.store(0, Ordering::Relaxed);
    HANDSHAKE_FAILURES.store(0, Ordering::Relaxed);
    ABORTED_CLIENTS.store(0, Ordering::Relaxed);
    CONNECTION_ERRORS_MAX_CONNECTIONS.store(0, Ordering::Relaxed);
    // MySQL's FLUSH STATUS resets Max_used_connections to the current
    // number of open connections, not to zero.
    MAX_USED_CONNECTIONS.store(active_connection_count(), Ordering::Relaxed);
}

pub fn connection_metrics() -> ConnectionMetrics {
    ConnectionMetrics {
        active: active_connection_count() as u64,
        accepted: ACCEPTED_CONNECTIONS.load(Ordering::Relaxed),
        rejected: REJECTED_CONNECTIONS.load(Ordering::Relaxed),
        handshake_successes: HANDSHAKE_SUCCESSES.load(Ordering::Relaxed),
        handshake_failures: HANDSHAKE_FAILURES.load(Ordering::Relaxed),
        threads_created: THREADS_CREATED.load(Ordering::Relaxed),
        closed: CLOSED_CONNECTIONS.load(Ordering::Relaxed),
        aborted_clients: ABORTED_CLIENTS.load(Ordering::Relaxed),
        connection_errors_max_connections: CONNECTION_ERRORS_MAX_CONNECTIONS
            .load(Ordering::Relaxed),
        max_used_connections: MAX_USED_CONNECTIONS.load(Ordering::Relaxed) as u64,
        max_connections: max_connections() as u64,
        effective_max_connections: effective_max_connections() as u64,
        listen_backlog: listen_backlog() as u64,
        handshake_timeout_secs: handshake_timeout().map(|d| d.as_secs()).unwrap_or(0),
        open_files_soft_limit: configure_open_files_limit().soft,
        open_files_hard_limit: configure_open_files_limit().hard,
        open_files_requested_limit: configure_open_files_limit().requested,
        open_files_reserve: open_files_reserve(),
        admin_connections: ADMIN_CONNECTIONS.load(Ordering::Relaxed) as u64,
        admin_connection_reserve: ADMIN_CONNECTION_RESERVE as u64,
        queries_executed: QUERIES_EXECUTED.load(Ordering::Relaxed),
    }
}

/// Count one client statement (`COM_QUERY` / `COM_STMT_EXECUTE`) for the
/// `Questions` / `Queries` status counters.
pub fn record_statement() {
    QUERIES_EXECUTED.fetch_add(1, Ordering::Relaxed);
}

pub fn record_statements(count: u64) {
    if count != 0 {
        QUERIES_EXECUTED.fetch_add(count, Ordering::Relaxed);
    }
}

pub fn record_internal_query() {
    QUERIES_EXECUTED.fetch_add(1, Ordering::Relaxed);
}

pub fn record_aborted_client() {
    ABORTED_CLIENTS.fetch_add(1, Ordering::Relaxed);
}

#[derive(Debug, Clone, Copy)]
pub struct ConnectionMetrics {
    pub active: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub handshake_successes: u64,
    pub handshake_failures: u64,
    pub threads_created: u64,
    pub closed: u64,
    pub aborted_clients: u64,
    pub connection_errors_max_connections: u64,
    pub max_used_connections: u64,
    pub max_connections: u64,
    pub effective_max_connections: u64,
    pub listen_backlog: u64,
    pub handshake_timeout_secs: u64,
    pub open_files_soft_limit: u64,
    pub open_files_hard_limit: u64,
    pub open_files_requested_limit: u64,
    pub open_files_reserve: u64,
    pub admin_connections: u64,
    pub admin_connection_reserve: u64,
    pub queries_executed: u64,
}

/// RAII guard incrementing/decrementing the global connection counter.
pub struct ConnectionSlot {
    admin_reserved: bool,
}

impl ConnectionSlot {
    #[cfg(test)]
    /// Reserves a slot if under the cap. Returns `None` once the cap is
    /// reached so the caller can reject the connection cleanly.
    pub fn acquire(cap: usize) -> Option<Self> {
        // MySQL Connections counts every accepted connection attempt,
        // successful or not (including authentication and max-connection
        // failures), rather than only established sessions.
        ACCEPTED_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
        match Self::try_acquire_ordinary(cap) {
            Some(slot) => Some(slot),
            None => {
                record_max_connection_rejection();
                None
            }
        }
    }

    /// Reserve an ordinary slot, or one of the administrator slots when all
    /// `cap` ordinary slots are occupied. A caller receiving an
    /// admin-reserved slot must authenticate the client and require the global
    /// CONNECTION_ADMIN privilege before completing the handshake.
    pub fn acquire_with_admin_reserve(cap: usize) -> Option<Self> {
        ACCEPTED_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
        if let Some(slot) = Self::try_acquire_ordinary(cap) {
            return Some(slot);
        }

        // Do not add a reserved connection after a live max_connections
        // reduction left more ordinary sessions than the new cap. Admission
        // resumes only after the ordinary population converges to the cap.
        if ORDINARY_CONNECTIONS.load(Ordering::Acquire) == cap {
            let mut admins = ADMIN_CONNECTIONS.load(Ordering::Relaxed);
            while admins < ADMIN_CONNECTION_RESERVE {
                match ADMIN_CONNECTIONS.compare_exchange_weak(
                    admins,
                    admins + 1,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let active = ACTIVE_CONNECTIONS.fetch_add(1, Ordering::Relaxed) + 1;
                        bump_max_used_connections(active);
                        return Some(Self {
                            admin_reserved: true,
                        });
                    }
                    Err(observed) => admins = observed,
                }
            }
        }

        // An ordinary connection may have closed while the reserved-slot CAS
        // raced. Give the newly available ordinary slot one final chance.
        if let Some(slot) = Self::try_acquire_ordinary(cap) {
            return Some(slot);
        }
        record_max_connection_rejection();
        None
    }

    pub fn requires_connection_admin(&self) -> bool {
        self.admin_reserved
    }

    fn try_acquire_ordinary(cap: usize) -> Option<Self> {
        let mut current = ORDINARY_CONNECTIONS.load(Ordering::Relaxed);
        loop {
            if current >= cap {
                return None;
            }
            match ORDINARY_CONNECTIONS.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let active = ACTIVE_CONNECTIONS.fetch_add(1, Ordering::Relaxed) + 1;
                    bump_max_used_connections(active);
                    return Some(Self {
                        admin_reserved: false,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        // Lower the aggregate first, then publish the vacated class-specific
        // slot. Otherwise a racing acquire can transiently overstate the
        // active-connection peak by one while this guard is being dropped.
        ACTIVE_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
        if self.admin_reserved {
            ADMIN_CONNECTIONS.fetch_sub(1, Ordering::Release);
        } else {
            ORDINARY_CONNECTIONS.fetch_sub(1, Ordering::Release);
        }
        CLOSED_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
    }
}

fn record_max_connection_rejection() {
    REJECTED_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
    CONNECTION_ERRORS_MAX_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
}

/// Record a post-authentication rejection of a client that reached the
/// administrator reserve but does not hold CONNECTION_ADMIN.
pub fn record_admin_reserve_rejection() {
    record_max_connection_rejection();
}

/// Monotonically raise the `Max_used_connections` peak to `observed`.
fn bump_max_used_connections(observed: usize) {
    let mut peak = MAX_USED_CONNECTIONS.load(Ordering::Relaxed);
    while observed > peak {
        match MAX_USED_CONNECTIONS.compare_exchange_weak(
            peak,
            observed,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(now) => peak = now,
        }
    }
}

pub fn record_handshake_success() {
    HANDSHAKE_SUCCESSES.fetch_add(1, Ordering::Relaxed);
}

pub fn record_handshake_failure() {
    HANDSHAKE_FAILURES.fetch_add(1, Ordering::Relaxed);
}

pub fn record_thread_created() {
    THREADS_CREATED.fetch_add(1, Ordering::Relaxed);
}

#[cfg(unix)]
fn configure_open_files_limit_inner() -> OpenFilesLimit {
    let requested = requested_open_files_limit();
    let reserve = open_files_reserve();
    // SAFETY: `getrlimit` and `setrlimit` are called with a valid resource
    // constant and pointers to initialized `rlimit` structs.
    unsafe {
        let mut current = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        if libc::getrlimit(libc::RLIMIT_NOFILE, current.as_mut_ptr()) != 0 {
            return OpenFilesLimit::unavailable(
                requested,
                reserve,
                std::io::Error::last_os_error().raw_os_error(),
            );
        }

        let mut limit = current.assume_init();
        let initial_soft = rlim_to_u64(limit.rlim_cur);
        let hard = rlim_to_u64(limit.rlim_max);
        let target = if hard == u64::MAX {
            requested
        } else {
            requested.min(hard)
        };
        let mut set_error = None;

        if initial_soft < target {
            limit.rlim_cur = u64_to_rlim(target);
            if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) != 0 {
                set_error = std::io::Error::last_os_error().raw_os_error();
            }
        }

        let mut after = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        if libc::getrlimit(libc::RLIMIT_NOFILE, after.as_mut_ptr()) != 0 {
            return OpenFilesLimit {
                initial_soft,
                soft: initial_soft,
                hard,
                requested,
                reserve,
                raised: false,
                capped_by_hard_limit: hard != u64::MAX && requested > hard,
                set_error: std::io::Error::last_os_error().raw_os_error().or(set_error),
            };
        }
        let after = after.assume_init();
        let soft = rlim_to_u64(after.rlim_cur);
        let hard = rlim_to_u64(after.rlim_max);
        OpenFilesLimit {
            initial_soft,
            soft,
            hard,
            requested,
            reserve,
            raised: soft > initial_soft,
            capped_by_hard_limit: hard != u64::MAX && requested > hard,
            set_error,
        }
    }
}

#[cfg(unix)]
fn rlim_to_u64(value: libc::rlim_t) -> u64 {
    if value == libc::RLIM_INFINITY {
        u64::MAX
    } else {
        value as u64
    }
}

#[cfg(unix)]
fn u64_to_rlim(value: u64) -> libc::rlim_t {
    if value == u64::MAX {
        libc::RLIM_INFINITY
    } else {
        value as libc::rlim_t
    }
}

#[cfg(not(unix))]
fn configure_open_files_limit_inner() -> OpenFilesLimit {
    OpenFilesLimit::unavailable(requested_open_files_limit(), open_files_reserve(), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    static CONNECTION_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn connection_test_guard() -> std::sync::MutexGuard<'static, ()> {
        CONNECTION_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[test]
    fn slot_acquire_rejects_at_cap_and_drop_releases() {
        let _guard = connection_test_guard();
        let before_active = active_connection_count();
        let before_ordinary = ORDINARY_CONNECTIONS.load(Ordering::Relaxed);
        let cap = before_ordinary + 1;
        let first = ConnectionSlot::acquire(cap).expect("slot should fit");
        assert_eq!(active_connection_count(), before_active + 1);
        assert!(ConnectionSlot::acquire(cap).is_none());
        drop(first);
        assert_eq!(active_connection_count(), before_active);
    }

    #[test]
    fn over_cap_increments_connection_errors_max_connections() {
        let _guard = connection_test_guard();
        let before = connection_metrics().connection_errors_max_connections;
        let ordinary = ORDINARY_CONNECTIONS.load(Ordering::Relaxed);
        let cap = ordinary + 1;
        let slot = ConnectionSlot::acquire(cap).expect("slot should fit");
        // A second acquire over the same cap is refused and bumps the
        // dedicated max-connections error counter (and rejected).
        assert!(ConnectionSlot::acquire(cap).is_none());
        let after = connection_metrics().connection_errors_max_connections;
        assert_eq!(after, before + 1);
        drop(slot);
    }

    #[test]
    fn max_used_connections_tracks_peak() {
        let _guard = connection_test_guard();
        let active = active_connection_count();
        let ordinary = ORDINARY_CONNECTIONS.load(Ordering::Relaxed);
        let peak_before = connection_metrics().max_used_connections;
        let slot = ConnectionSlot::acquire(ordinary + 5).expect("slot should fit");
        let peak_after = connection_metrics().max_used_connections;
        assert!(
            peak_after >= (active + 1) as u64,
            "peak must include this slot"
        );
        assert!(peak_after >= peak_before, "peak is monotonic");
        drop(slot);
    }

    #[test]
    fn admin_reserve_allows_ten_extra_connections() {
        let _guard = connection_test_guard();
        let ordinary = ORDINARY_CONNECTIONS.load(Ordering::Relaxed);
        let cap = ordinary + 1;
        let normal =
            ConnectionSlot::acquire_with_admin_reserve(cap).expect("last ordinary slot should fit");
        assert!(!normal.requires_connection_admin());

        let admins = (0..ADMIN_CONNECTION_RESERVE)
            .map(|_| {
                let slot = ConnectionSlot::acquire_with_admin_reserve(cap)
                    .expect("administrator reserve should fit");
                assert!(slot.requires_connection_admin());
                slot
            })
            .collect::<Vec<_>>();
        assert!(
            ConnectionSlot::acquire_with_admin_reserve(cap).is_none(),
            "administrator reserve must remain bounded"
        );

        drop(admins);
        let replacement = ConnectionSlot::acquire_with_admin_reserve(cap)
            .expect("dropping the administrator must release its reserve");
        assert!(replacement.requires_connection_admin());
        drop(replacement);
        drop(normal);
    }

    #[test]
    fn metrics_track_handshake_counters() {
        let before = connection_metrics();
        record_handshake_success();
        record_handshake_failure();
        let after = connection_metrics();
        assert_eq!(after.handshake_successes, before.handshake_successes + 1);
        assert_eq!(after.handshake_failures, before.handshake_failures + 1);
    }

    #[test]
    fn effective_max_connections_reserves_open_files() {
        let limit = OpenFilesLimit {
            initial_soft: 1024,
            soft: 1024,
            hard: 1024,
            requested: 2512,
            reserve: 512,
            raised: false,
            capped_by_hard_limit: true,
            set_error: None,
        };
        assert_eq!(limit.effective_max_connections(2000), 502);
    }

    #[test]
    fn effective_max_connections_keeps_configured_cap_when_soft_limit_fits() {
        let limit = OpenFilesLimit {
            initial_soft: 1024,
            soft: 4096,
            hard: 4096,
            requested: 2512,
            reserve: 512,
            raised: true,
            capped_by_hard_limit: false,
            set_error: None,
        };
        assert_eq!(limit.effective_max_connections(2000), 2000);
    }
}
