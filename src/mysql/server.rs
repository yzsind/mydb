use tokio_rustls::rustls::pki_types::pem::PemObject;

#[cfg(all(test, feature = "mysql-client"))]
#[path = "tls_client_tests.rs"]
mod tls_client_tests;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use rcgen::{generate_simple_self_signed, CertifiedKey};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;

use crate::engine::api::QueryEngine;
use crate::engine::session_registry::SessionRegistry;
use crate::mysql::connection_manager::handshake_timeout;
use crate::mysql::connection_manager::ConnectionSlot;
pub use crate::mysql::connection_manager::{
    active_connection_count, configure_open_files_limit, connection_metrics,
    effective_max_connections, listen_backlog, max_connections,
};
use crate::mysql::packet::write_packet_direct_unflushed;
use crate::mysql::protocol::pre_handshake_err_packet;
use crate::mysql::session::Session;
use crate::util::logging::log_message;

const ACCEPT_FD_PRESSURE_BACKOFF: Duration = Duration::from_millis(50);

pub struct MysqlServer<E> {
    addr: SocketAddr,
    engine: Arc<E>,
    registry: Arc<SessionRegistry>,
    tls_acceptor: Option<TlsAcceptor>,
}

impl<E> MysqlServer<E>
where
    E: QueryEngine,
{
    pub fn with_tls(
        addr: SocketAddr,
        engine: Arc<E>,
        tls_acceptor: Option<TlsAcceptor>,
        registry: Arc<SessionRegistry>,
    ) -> Self {
        Self {
            addr,
            engine,
            registry,
            tls_acceptor,
        }
    }

    pub async fn run(&self) -> std::io::Result<()> {
        crate::engine::status_counters::initialize_server_clock();
        let open_files = configure_open_files_limit();
        let listener = bind_listener(self.addr)?;
        let configured_cap = max_connections();
        let cap = effective_max_connections();
        if open_files.capped_by_hard_limit || cap < configured_cap {
            log_message(format_args!(
                "effective max connections is {cap} (configured {configured_cap}, open files soft={}, hard={}, reserve={})",
                open_files.soft, open_files.hard, open_files.reserve
            ));
        }
        if let Some(errno) = open_files.set_error {
            log_message(format_args!(
                "failed to raise open files limit to {}: errno {errno}; using soft limit {}",
                open_files.requested, open_files.soft
            ));
        }

        // A genuinely fatal listener error (e.g. EBADF/EINVAL — the
        // listening socket itself is broken) still runs the graceful
        // checkpoint below before propagating, so we never skip the final
        // WAL sync + dirty-page flush on the way out. Transient per-connection
        // errors do NOT land here (see `is_transient_accept_error`).
        let mut fatal_accept_error: Option<std::io::Error> = None;
        // Install the SIGINT/SIGTERM watcher once, not per accept.
        let mut os_signals = OsSignals::new();
        loop {
            // A `SHUTDOWN` / `RESTART` / `COM_SHUTDOWN` issued on another
            // connection may have flipped the flag between accepts; break
            // before parking so we don't accept one more connection.
            if crate::mysql::shutdown::is_shutting_down() {
                break;
            }
            // Re-read the connection cap every iteration so `SET GLOBAL
            // max_connections = N` takes effect for the next accepted
            // connection (the connection-manager contract). `max_connections()`
            // is a live atomic read; the fd-derived ceiling is cached, so
            // this is a couple of relaxed loads per accept.
            let cap = effective_max_connections();
            let accepted = tokio::select! {
                result = listener.accept() => result,
                _ = crate::mysql::shutdown::shutdown_notified() => break,
                _ = os_signals.recv() => {
                    crate::mysql::shutdown::request_shutdown(
                        crate::mysql::shutdown::ShutdownSource::Signal("SIGINT/SIGTERM"),
                        crate::mysql::shutdown::ShutdownIntent::Halt,
                    );
                    break;
                }
            };
            // A connection that arrived in the same poll as a shutdown
            // request is refused with ER_SERVER_SHUTDOWN before any
            // handshake state is allocated.
            if crate::mysql::shutdown::is_shutting_down() {
                if let Ok((stream, _)) = accepted {
                    spawn_reject(
                        stream,
                        pre_handshake_err_packet(
                            crate::error::catalog::ER_SERVER_SHUTDOWN,
                            "Server shutdown in progress".to_string(),
                        ),
                    );
                }
                break;
            }
            let (stream, peer_addr) = match accepted {
                Ok(accepted) => accepted,
                Err(err) if is_fd_pressure_error(&err) => {
                    log_message(format_args!(
                        "accept hit open-file pressure ({err}); active_connections={}; cap={cap}; backing off",
                        active_connection_count()
                    ));
                    tokio::time::sleep(ACCEPT_FD_PRESSURE_BACKOFF).await;
                    continue;
                }
                Err(err) if is_transient_accept_error(&err) => {
                    // Per-connection / spurious errors (a client that RST'd
                    // after landing in the accept queue → ECONNABORTED, an
                    // interrupted syscall → EINTR, transient kernel/firewall
                    // pressure → ENOBUFS/ENOMEM/EPERM). Dropping the whole
                    // listener here would let any remote peer kill the
                    // server, so log and keep accepting (matches MySQL/nginx).
                    log_message(format_args!(
                        "accept transient error ({err}); active_connections={}; continuing",
                        active_connection_count()
                    ));
                    continue;
                }
                Err(err) => {
                    // The listening socket itself is broken. Fall out of the
                    // accept loop and take the graceful path (final checkpoint
                    // + log flush) before propagating, rather than a bare
                    // `return` that skips it.
                    log_message(format_args!(
                        "fatal accept error ({err}); stopping listener and shutting down"
                    ));
                    fatal_accept_error = Some(err);
                    break;
                }
            };
            let _ = stream.set_nodelay(true);
            // Apply optional SO_SNDBUF / SO_RCVBUF overrides from
            // `MYDB_TCP_SNDBUF` / `MYDB_TCP_RCVBUF`. Defaults are
            // unspecified (OS default), so this is purely an opt-in
            // tuning knob. Increasing SO_SNDBUF helps large result-set
            // throughput by letting more bytes be in flight before the
            // server-side write blocks on the kernel send queue.
            tune_socket_buffers(&stream);
            // SO_KEEPALIVE + per-connection probe tuning so long-idle
            // sessions (connection pools, IDE editors) shed dead TCP
            // peers instead of pinning a slot in `ACTIVE_CONNECTIONS`
            // until the kernel's default 2-hour keepalive eventually
            // fires. Linux additionally gets TCP_USER_TIMEOUT so an
            // *active* write blocked by a wedged peer also fails fast.
            tune_socket_keepalive(&stream);

            let Some(slot) = ConnectionSlot::acquire_with_admin_reserve(cap) else {
                spawn_reject(
                    stream,
                    pre_handshake_err_packet(
                        crate::error::catalog::ER_CON_COUNT_ERROR,
                        format!("Too many connections (limit {cap})"),
                    ),
                );
                continue;
            };

            let engine = Arc::clone(&self.engine);
            let registry = Arc::clone(&self.registry);
            let tls_acceptor = self.tls_acceptor.clone();
            let connection_id = registry.allocate_thread_id();
            crate::mysql::connection_manager::record_thread_created();

            // Register the socket on its owner's reactor rather than retaining
            // an I/O registration tied to the listener's runtime.
            let stream = match stream.into_std() {
                Ok(stream) => stream,
                Err(err) => {
                    registry.unregister(connection_id);
                    log_message(format_args!(
                        "connection {connection_id} socket transfer failed: {err}"
                    ));
                    continue;
                }
            };
            let dispatched = crate::util::connection_dispatch::spawn(move || async move {
                let _slot = slot;
                let stream = match tokio::net::TcpStream::from_std(stream) {
                    Ok(stream) => stream,
                    Err(err) => {
                        registry.unregister(connection_id);
                        log_message(format_args!(
                            "connection {connection_id} reactor registration failed: {err}"
                        ));
                        return;
                    }
                };
                let mut session = match Session::try_new(
                    connection_id,
                    peer_addr,
                    stream,
                    tls_acceptor,
                    engine,
                    registry.clone(),
                ) {
                    Ok(session) => session,
                    Err((err, stream)) => {
                        registry.unregister(connection_id);
                        log_message(format_args!(
                            "connection {connection_id} from {peer_addr} rejected before handshake: {err}"
                        ));
                        spawn_reject(
                            stream,
                            pre_handshake_err_packet(
                                crate::error::catalog::ER_OUTOFMEMORY,
                                err.to_string(),
                            ),
                        );
                        return;
                    }
                };
                if _slot.requires_connection_admin() {
                    session.require_connection_admin(cap);
                }
                if let Err(err) = session.run().await {
                    log_message(format_args!(
                        "connection {connection_id} from {peer_addr} closed with error: {err}"
                    ));
                }
            });
            if let Err(err) = dispatched {
                fatal_accept_error = Some(err);
                break;
            }
        }
        // Stopped accepting: run the controlled-shutdown sequence. Never
        // returns when the intent is `Restart` (it re-execs in place).
        drop(listener);
        // Keep one shutdown future alive across repeated signals. A terminal
        // and its wrapper may both send SIGINT; neither repetition nor SIGTERM
        // may cancel a checkpoint or change an accepted RESTART into a halt.
        let shutdown = self.graceful_shutdown();
        tokio::pin!(shutdown);
        let mut repeated_signal_logged = false;
        loop {
            tokio::select! {
                biased;
                result = &mut shutdown => {
                    result?;
                    break;
                }
                _ = os_signals.recv() => {
                    if !repeated_signal_logged {
                        log_message("shutdown already in progress; waiting for graceful shutdown to complete");
                        repeated_signal_logged = true;
                    }
                }
            }
        }
        // Propagate a fatal listener error (if any) only after the graceful
        // checkpoint has run, so the non-zero exit reaches the supervisor
        // without costing us the final durable flush.
        match fatal_accept_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Controlled-shutdown sequence shared by SIGINT/SIGTERM, SQL
    /// `SHUTDOWN`/`RESTART`, and `COM_SHUTDOWN`: drain live connections
    /// (bounded), take the final checkpoint, flush logs, then either
    /// return (caller exits) or re-exec for `RESTART`.
    async fn graceful_shutdown(&self) -> std::io::Result<()> {
        use crate::mysql::shutdown::{intent, ShutdownIntent};

        log_message(format_args!(
            "shutdown in progress (source: {})",
            crate::mysql::shutdown::source_description()
        ));

        // Drain: give live connections a bounded window to finish and
        // release their slots (the SHUTDOWN-issuing connection self-closes
        // as soon as it has flushed its OK). Past the deadline, force the
        // stragglers closed and briefly wait again.
        let deadline = tokio::time::Instant::now() + crate::mysql::shutdown::shutdown_timeout();
        while active_connection_count() > 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if active_connection_count() > 0 {
            log_message(format_args!(
                "shutdown drain timed out with {} connection(s) still open; forcing close",
                active_connection_count()
            ));
            self.registry.kill_all();
            // A slot is released only after Session cleanup (including rollback).
            // Never checkpoint concurrently with a still-running client task.
            let deadline = tokio::time::Instant::now()
                + crate::mysql::shutdown::shutdown_timeout().max(Duration::from_secs(1));
            while active_connection_count() > 0 {
                if tokio::time::Instant::now() >= deadline {
                    log_message("shutdown cancellation timed out; forcing exit without checkpoint or restart");
                    // Returning would let runtime destruction wait forever for
                    // a stuck blocking query. Never checkpoint beside a writer.
                    std::process::exit(1);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        // Quiesce native WAL replication first. Its online applier and sender
        // tasks hold direct Database/WAL handles and must be joined before the
        // final checkpoint, not merely abandoned when the Tokio runtime drops.
        if let Some(manager) = crate::repl::global_manager() {
            manager.shutdown_all().await;
        }

        // Thread joins and checkpoint I/O must not block the signal watcher,
        // including on a single-worker runtime. All writers must finish before
        // the checkpoint; an unsuccessful stop cancels RESTART.
        let engine = Arc::clone(&self.engine);
        tokio::task::spawn_blocking(move || {
            let result = (|| {
                if let Some(manager) = crate::replication::global_manager() {
                    manager.stop_all_channels()?;
                }
                engine.shutdown()
            })();
            finish_shutdown(result)
        })
        .await
        .map_err(|err| std::io::Error::other(format!("shutdown worker failed: {err}")))??;

        match intent() {
            ShutdownIntent::Halt => {
                log_message("shutdown complete");
                Ok(())
            }
            ShutdownIntent::Restart => {
                // Re-exec in place (execv). Does not return on success.
                crate::mysql::shutdown::re_exec();
            }
        }
    }
}

// Flush observability logs on success AND failure. Preserve the storage error
// if flushing fails too; a successful checkpoint must not hide a log failure.
fn finish_shutdown(result: Result<(), crate::types::QueryError>) -> std::io::Result<()> {
    finish_shutdown_with_flush(result, || {
        if let Some(service) = crate::engine::logging::active_service() {
            if let Err(err) = service.flush(crate::engine::logging::FlushLogKind::All) {
                log_message(format_args!("shutdown log flush failed: {err:?}"));
            }
        }
    })
}

fn finish_shutdown_with_flush(
    result: Result<(), crate::types::QueryError>,
    flush: impl FnOnce(),
) -> std::io::Result<()> {
    flush();
    result.map_err(|err| {
        log_message(format_args!(
            "shutdown failed: {} ({})",
            err.message(),
            err.sql_state()
        ));
        std::io::Error::other(format!(
            "shutdown failed; restart cancelled: {} {:?}",
            err.message(),
            err.diagnostic_detail()
        ))
    })
}

#[cfg(test)]
#[test]
fn shutdown_flushes_logs_when_checkpoint_fails() {
    let mut flushed = false;
    let result = finish_shutdown_with_flush(
        Err(crate::storage::error(
            crate::error::catalog::ER_GET_ERRNO,
            "injected checkpoint failure",
        )),
        || flushed = true,
    );
    assert!(flushed);
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected checkpoint failure"));
}

/// Long-lived OS-signal watcher, created ONCE before the accept loop.
/// Previously the accept `select!` called a helper that installed a fresh
/// `SignalKind::terminate()` registration on *every* iteration — a global
/// registry lock + allocation per accepted connection under a high
/// connection-establishment rate, plus a tiny lost-signal window between
/// iterations. Holding the `Signal` across iterations removes both: tokio
/// `Signal` latches a delivery, so a SIGTERM that lands while the loop is
/// busy accepting is still observed on the next `recv`.
#[cfg(unix)]
struct OsSignals {
    /// `None` if the SIGTERM handler failed to install; we then fall back
    /// to Ctrl-C (SIGINT) only.
    term: Option<tokio::signal::unix::Signal>,
    _hup: Option<tokio::signal::unix::Signal>,
}

#[cfg(unix)]
impl OsSignals {
    fn new() -> Self {
        use tokio::signal::unix::{signal, SignalKind};
        let term = match signal(SignalKind::terminate()) {
            Ok(sig) => Some(sig),
            Err(err) => {
                log_message(format_args!(
                    "failed to install SIGTERM handler: {err}; SIGINT only"
                ));
                None
            }
        };
        let hup = signal(SignalKind::hangup())
            .unwrap_or_else(|err| panic!("failed to install SIGHUP handler: {err}"));
        Self {
            term,
            _hup: Some(hup),
        }
    }

    /// Resolve when SIGINT (Ctrl-C) or SIGTERM arrives. Cancel-safe: a
    /// `recv` that loses the accept `select!` race can be re-entered next
    /// iteration without dropping a pending signal.
    async fn recv(&mut self) {
        match &mut self.term {
            Some(term) => {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(err) = result {
                            log_message(format_args!("failed to listen for SIGINT: {err}"));
                            std::future::pending::<()>().await;
                        }
                    }
                    _ = term.recv() => {}
                }
            }
            None => {
                if let Err(err) = tokio::signal::ctrl_c().await {
                    log_message(format_args!("failed to listen for SIGINT: {err}"));
                    std::future::pending::<()>().await;
                }
            }
        }
    }
}

#[cfg(not(unix))]
struct OsSignals;

#[cfg(not(unix))]
impl OsSignals {
    fn new() -> Self {
        OsSignals
    }
    async fn recv(&mut self) {
        if let Err(err) = tokio::signal::ctrl_c().await {
            log_message(format_args!("failed to listen for shutdown signal: {err}"));
            std::future::pending::<()>().await;
        }
    }
}

fn is_fd_pressure_error(err: &std::io::Error) -> bool {
    matches!(err.raw_os_error(), Some(libc::EMFILE) | Some(libc::ENFILE))
}

/// Transient, per-connection or resource-pressure `accept(2)` errors that
/// must NOT tear down the whole listener. `ECONNABORTED` is remotely
/// triggerable (a client that RSTs after landing in the accept queue), so
/// treating it as fatal would let any peer kill the server — and skip the
/// graceful-shutdown checkpoint on the way out. `EINTR`/`EPROTO` are
/// spurious; `ENOBUFS`/`ENOMEM`/`EPERM` are transient kernel/firewall
/// pressure. Matches how MySQL and nginx treat these errnos: log and keep
/// accepting. `EMFILE`/`ENFILE` are handled separately with a backoff.
fn is_transient_accept_error(err: &std::io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::ECONNABORTED)
            | Some(libc::EINTR)
            | Some(libc::EPROTO)
            | Some(libc::ENOBUFS)
            | Some(libc::ENOMEM)
            | Some(libc::EPERM)
    )
}

/// Best-effort rejection of an inbound connection with a MySQL ERR packet,
/// spawned detached. Bounded by `handshake_timeout` so a client that never
/// reads its socket cannot pin the fd + tokio task indefinitely: these
/// rejections hold no `ConnectionSlot`, but they still consume an fd, which
/// under an fd-exhaustion attack is exactly the scarce resource the
/// rejection is supposed to relieve.
fn spawn_reject(stream: TcpStream, payload: Vec<u8>) {
    tokio::spawn(async move {
        let mut stream = stream;
        let write_and_close = async {
            let _ = write_packet_direct_unflushed(&mut stream, 0, &payload).await;
            let _ = stream.shutdown().await;
        };
        let timeout = handshake_timeout().unwrap_or(Duration::from_secs(10));
        let _ = tokio::time::timeout(timeout, write_and_close).await;
    });
}

/// Parse an optional positive byte count from an environment variable.
/// Returns `None` if unset, empty, or unparseable; rejects 0 so users
/// can't accidentally clamp the kernel send/recv queue to 0.
fn parse_byte_env(var: &str) -> Option<usize> {
    std::env::var(var)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
}

/// SO_SNDBUF override (`MYDB_TCP_SNDBUF`, bytes). `None` keeps the OS
/// default. Read once at startup; cached.
fn tcp_sndbuf_bytes() -> Option<usize> {
    static V: OnceLock<Option<usize>> = OnceLock::new();
    *V.get_or_init(|| parse_byte_env("MYDB_TCP_SNDBUF"))
}

/// SO_RCVBUF override (`MYDB_TCP_RCVBUF`, bytes). `None` keeps the OS
/// default. Read once at startup; cached.
fn tcp_rcvbuf_bytes() -> Option<usize> {
    static V: OnceLock<Option<usize>> = OnceLock::new();
    *V.get_or_init(|| parse_byte_env("MYDB_TCP_RCVBUF"))
}

/// Apply `MYDB_TCP_SNDBUF` / `MYDB_TCP_RCVBUF` to a freshly accepted
/// connection. unix-only — the rest of mydb is also unix-targeted
/// (the data layer uses fdatasync / fcntl pwrite etc.). On non-unix
/// targets this is a no-op so the build still compiles.
#[cfg(unix)]
fn tune_socket_buffers(stream: &TcpStream) {
    use std::os::unix::io::AsRawFd;
    let fd = stream.as_raw_fd();
    if let Some(size) = tcp_sndbuf_bytes() {
        let size_int: libc::c_int = size.min(libc::c_int::MAX as usize) as libc::c_int;
        // SAFETY: `fd` is a valid open socket for the lifetime of
        // `stream`; the pointer + length describe a valid `c_int`.
        // The setsockopt call is documented to be safe for arbitrary
        // values, returning -1 on failure (which we ignore — tuning
        // is best-effort and should never tear down the connection).
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &size_int as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
    if let Some(size) = tcp_rcvbuf_bytes() {
        let size_int: libc::c_int = size.min(libc::c_int::MAX as usize) as libc::c_int;
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &size_int as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

#[cfg(not(unix))]
fn tune_socket_buffers(_stream: &TcpStream) {}

/// TCP keepalive idle time (seconds). Default 120s. `0` disables
/// SO_KEEPALIVE for new connections (matches the pre-change behaviour).
fn tcp_keepalive_idle_secs() -> u32 {
    static V: OnceLock<u32> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MYDB_TCP_KEEPALIVE_SECS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u32>().ok())
            .unwrap_or(120)
    })
}

/// Interval between keepalive probes once the connection is idle.
/// Default 30s. Ignored when SO_KEEPALIVE is disabled.
fn tcp_keepalive_interval_secs() -> u32 {
    static V: OnceLock<u32> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MYDB_TCP_KEEPALIVE_INTERVAL_SECS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u32>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(30)
    })
}

/// Max unacknowledged keepalive probes before the kernel drops the
/// connection. Default 3. Ignored when SO_KEEPALIVE is disabled.
fn tcp_keepalive_probes() -> u32 {
    static V: OnceLock<u32> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MYDB_TCP_KEEPALIVE_PROBES")
            .ok()
            .and_then(|raw| raw.trim().parse::<u32>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(3)
    })
}

/// Linux-only TCP_USER_TIMEOUT (milliseconds). Caps how long a still-
/// unacknowledged outgoing write may linger before the kernel tears
/// the connection down — orthogonal to SO_KEEPALIVE, which only fires
/// when the connection is idle. Default 300s. `0` disables.
#[cfg(target_os = "linux")]
fn tcp_user_timeout_ms() -> u32 {
    static V: OnceLock<u32> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MYDB_TCP_USER_TIMEOUT_SECS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u32>().ok())
            .unwrap_or(300)
            .saturating_mul(1000)
    })
}

/// Apply SO_KEEPALIVE + per-connection keepalive timers and (on Linux)
/// TCP_USER_TIMEOUT. Best-effort: setsockopt failures are intentionally
/// silent so a kernel that lacks a particular knob does not abort the
/// session — the timeouts are defence-in-depth on top of `wait_timeout`
/// and `net_write_timeout`.
#[cfg(unix)]
fn tune_socket_keepalive(stream: &TcpStream) {
    use std::os::unix::io::AsRawFd;
    let fd = stream.as_raw_fd();
    let idle = tcp_keepalive_idle_secs();
    if idle == 0 {
        return;
    }
    let interval = tcp_keepalive_interval_secs();
    let probes = tcp_keepalive_probes();

    // SAFETY: `fd` is a valid open socket for the lifetime of `stream`;
    // each setsockopt call describes a single `c_int`. Failures are
    // ignored by design (see fn-level comment).
    unsafe {
        let on: libc::c_int = 1;
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_KEEPALIVE,
            &on as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );

        // Per-knob name differs between Linux and macOS / BSD; behaviour
        // (seconds of idle before first probe / interval between probes /
        // probe count) is the same once set.
        #[cfg(target_os = "linux")]
        let idle_opt = libc::TCP_KEEPIDLE;
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        let idle_opt = libc::TCP_KEEPALIVE;
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "ios")))]
        let idle_opt: libc::c_int = 0;

        if idle_opt != 0 {
            let val: libc::c_int = idle.min(libc::c_int::MAX as u32) as libc::c_int;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                idle_opt,
                &val as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }

        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios"))]
        {
            let val: libc::c_int = interval.min(libc::c_int::MAX as u32) as libc::c_int;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPINTVL,
                &val as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );

            let val: libc::c_int = probes.min(libc::c_int::MAX as u32) as libc::c_int;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPCNT,
                &val as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }

        #[cfg(target_os = "linux")]
        {
            let user_timeout = tcp_user_timeout_ms();
            if user_timeout != 0 {
                let val: libc::c_int = user_timeout.min(libc::c_int::MAX as u32) as libc::c_int;
                libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_USER_TIMEOUT,
                    &val as *const libc::c_int as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        // Silence the unused-binding warning on non-Linux + non-Mac.
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "ios")))]
        {
            let _ = interval;
            let _ = probes;
        }
    }
}

#[cfg(not(unix))]
fn tune_socket_keepalive(_stream: &TcpStream) {}

fn bind_listener(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(listen_backlog())
}

pub fn load_or_generate_tls_acceptor(
    configured_paths: Option<(&Path, &Path)>,
) -> std::io::Result<Option<TlsAcceptor>> {
    let Some((cert_path, key_path)) = configured_paths else {
        return generated_tls_acceptor().map(Some);
    };

    tls_acceptor_from_cert_and_key(load_certs(cert_path)?, load_private_key(key_path)?).map(Some)
}

fn generated_tls_acceptor() -> std::io::Result<TlsAcceptor> {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["localhost".to_string()]).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("failed to generate default SSL certificate: {err}"),
            )
        })?;
    let certs = vec![cert.der().clone()];
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    tls_acceptor_from_cert_and_key(certs, key)
}

fn tls_acceptor_from_cert_and_key(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> std::io::Result<TlsAcceptor> {
    let builder = ServerConfig::builder();
    let config = if let Some(ca_path) = crate::util::env::var_os("MYDB_SSL_CA") {
        let ca_certs = load_certs(Path::new(&ca_path))?;
        let mut roots = RootCertStore::empty();
        let (added, ignored) = roots.add_parsable_certificates(ca_certs);
        if added == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "SSL CA '{}' added no trust anchors (ignored {ignored})",
                    Path::new(&ca_path).display()
                ),
            ));
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .allow_unauthenticated()
            .build()
            .map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "invalid SSL CA verifier '{}': {err}",
                        Path::new(&ca_path).display()
                    ),
                )
            })?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    }
    .with_single_cert(certs, key)
    .map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid SSL certificate or key: {err}"),
        )
    })?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn load_certs(path: &Path) -> std::io::Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path).map_err(|err| {
        std::io::Error::new(
            err.kind(),
            format!("failed to open SSL certificate '{}': {err}", path.display()),
        )
    })?;
    let mut reader = std::io::BufReader::new(file);
    let certs = CertificateDer::pem_reader_iter(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "failed to parse SSL certificate '{}': {err}",
                    path.display()
                ),
            )
        })?;
    if certs.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "SSL certificate '{}' contains no certificates",
                path.display()
            ),
        ));
    }

    Ok(certs)
}

fn load_private_key(path: &Path) -> std::io::Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path).map_err(|err| {
        std::io::Error::new(
            err.kind(),
            format!("failed to open SSL key '{}': {err}", path.display()),
        )
    })?;
    let mut reader = std::io::BufReader::new(file);
    PrivateKeyDer::pem_reader_iter(&mut reader)
        .next()
        .transpose()
        .map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("failed to parse SSL key '{}': {err}", path.display()),
            )
        })?
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("SSL key '{}' contains no private key", path.display()),
            )
        })
}

#[cfg(all(unix, test))]
mod tests {
    use super::*;
    use std::os::unix::io::AsRawFd;
    use tokio::net::TcpListener;

    struct RejectAdmission;

    impl QueryEngine for RejectAdmission {
        fn execute(
            &self,
            _session: &mut crate::engine::api::SessionContext,
            _sql: &str,
        ) -> Result<crate::engine::api::QueryResponse, crate::types::QueryError> {
            unreachable!("admission must fail before SQL execution")
        }

        fn reclaim_connection_admission_memory(
            &self,
            _requested_bytes: usize,
        ) -> Result<(), crate::types::QueryError> {
            Err(crate::types::QueryError::known(
                crate::error::catalog::ER_OUTOFMEMORY,
                "connection memory admission rejected",
            ))
        }
    }

    #[tokio::test]
    async fn memory_admission_failure_preserves_socket_for_error_packet() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, peer) = listener.accept().await.unwrap();
        let (err, stream) = match Session::try_new(
            1,
            peer,
            stream,
            None,
            Arc::new(RejectAdmission),
            Arc::new(SessionRegistry::new()),
        ) {
            Ok(_) => panic!("admission unexpectedly succeeded"),
            Err(failure) => failure,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::OutOfMemory);
        spawn_reject(
            stream,
            pre_handshake_err_packet(crate::error::catalog::ER_OUTOFMEMORY, err.to_string()),
        );
        let packet = tokio::time::timeout(
            Duration::from_secs(2),
            crate::mysql::packet::read_packet(&mut client),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(packet.sequence_id, 0);
        assert_eq!(packet.payload[0], 0xff);
        assert_eq!(
            u16::from_le_bytes([packet.payload[1], packet.payload[2]]),
            1037
        );
        assert!(String::from_utf8_lossy(&packet.payload)
            .contains("connection memory admission rejected"));
    }

    /// Read an integer socket option back as a `c_int`. Returns the
    /// raw value the kernel reports, or `None` if the call failed.
    fn getsockopt_int(fd: i32, level: libc::c_int, name: libc::c_int) -> Option<libc::c_int> {
        let mut val: libc::c_int = 0;
        let mut len: libc::socklen_t = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: pointer and length describe a single `c_int`; fd is
        // a valid open socket for the lifetime of the test.
        let rc = unsafe {
            libc::getsockopt(
                fd,
                level,
                name,
                &mut val as *mut libc::c_int as *mut libc::c_void,
                &mut len,
            )
        };
        if rc != 0 {
            None
        } else {
            Some(val)
        }
    }

    #[tokio::test]
    async fn tune_socket_keepalive_enables_so_keepalive_and_idle_timer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client_join =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });
        let (server_stream, _) = listener.accept().await.unwrap();
        // Drop the client end after the test scope ends.
        let _client = client_join.await.unwrap();

        tune_socket_keepalive(&server_stream);
        let fd = server_stream.as_raw_fd();

        // SO_KEEPALIVE must be on.
        let on = getsockopt_int(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE)
            .expect("getsockopt SO_KEEPALIVE");
        assert_ne!(
            on, 0,
            "SO_KEEPALIVE should be enabled by tune_socket_keepalive"
        );

        // Idle timer must equal `tcp_keepalive_idle_secs()`. The exact
        // sockopt name differs between Linux and macOS; both are
        // measured in seconds.
        #[cfg(target_os = "linux")]
        let idle_opt = libc::TCP_KEEPIDLE;
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        let idle_opt = libc::TCP_KEEPALIVE;
        let idle = getsockopt_int(fd, libc::IPPROTO_TCP, idle_opt).expect("getsockopt idle timer");
        assert_eq!(
            idle as u32,
            tcp_keepalive_idle_secs(),
            "TCP keepalive idle seconds should match configured default",
        );

        // Probe interval + count are only adjustable on Linux/macOS;
        // both should reflect the configured defaults.
        let intv = getsockopt_int(fd, libc::IPPROTO_TCP, libc::TCP_KEEPINTVL)
            .expect("getsockopt TCP_KEEPINTVL");
        assert_eq!(intv as u32, tcp_keepalive_interval_secs());
        let cnt = getsockopt_int(fd, libc::IPPROTO_TCP, libc::TCP_KEEPCNT)
            .expect("getsockopt TCP_KEEPCNT");
        assert_eq!(cnt as u32, tcp_keepalive_probes());
    }
}

#[cfg(all(test, unix))]
#[test]
fn shutdown_process_bounds_and_signals_during_blocking_checkpoint() {
    use crate::engine::api::{QueryResponse, SessionContext};
    use crate::types::QueryError;
    use std::process::{Command, Stdio};

    struct BlockingEngine(std::path::PathBuf);
    impl QueryEngine for BlockingEngine {
        fn execute(&self, _: &mut SessionContext, _: &str) -> Result<QueryResponse, QueryError> {
            Ok(QueryResponse::Ok)
        }
        fn shutdown(&self) -> Result<(), QueryError> {
            std::fs::write(&self.0, b"checkpoint entered").unwrap();
            while !self.0.with_extension("release").exists() {
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok(())
        }
    }
    const CHILD: &str = "MYDB_TEST_SHUTDOWN_CHILD";
    if let Some(mode) = std::env::var_os(CHILD) {
        let marker =
            std::path::PathBuf::from(std::env::var_os("MYDB_TEST_SHUTDOWN_MARKER").unwrap());
        let _slot = (mode == "connection").then(|| ConnectionSlot::acquire(10).unwrap());
        crate::mysql::shutdown::request_shutdown(
            crate::mysql::shutdown::ShutdownSource::Signal("test"),
            crate::mysql::shutdown::ShutdownIntent::Halt,
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let server = MysqlServer::with_tls(
            "127.0.0.1:0".parse().unwrap(),
            Arc::new(BlockingEngine(marker)),
            None,
            Arc::new(SessionRegistry::new()),
        );
        runtime.block_on(server.run()).unwrap();
        assert_eq!(mode, "checkpoint", "stuck writer must not finish shutdown");
        return;
    }

    for mode in ["connection", "checkpoint"] {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let marker = dir.path().join("checkpoint");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "mysql::server::shutdown_process_bounds_and_signals_during_blocking_checkpoint",
                "--nocapture",
            ])
            .env(CHILD, mode)
            .env("MYDB_TEST_SHUTDOWN_MARKER", &marker)
            .env("MYDB_SHUTDOWN_TIMEOUT_MS", "0")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let start = std::time::Instant::now();
        let mut signalled = false;
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if mode == "checkpoint" && marker.exists() && !signalled {
                for signal in [libc::SIGINT, libc::SIGINT, libc::SIGTERM, libc::SIGTERM] {
                    // SAFETY: this is our live child with installed signal handlers.
                    assert_eq!(unsafe { libc::kill(child.id() as libc::pid_t, signal) }, 0);
                    std::thread::sleep(Duration::from_millis(100));
                    assert!(
                        child.try_wait().unwrap().is_none(),
                        "repeated signal {signal} interrupted checkpoint"
                    );
                }
                std::fs::write(marker.with_extension("release"), b"finish checkpoint").unwrap();
                signalled = true;
            }
            if start.elapsed() > Duration::from_secs(5) {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("shutdown hung in {mode}");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert_eq!(
            status.code(),
            Some(if mode == "checkpoint" { 0 } else { 1 }),
            "{mode}: unexpected child exit"
        );
        assert_eq!(
            marker.exists(),
            mode == "checkpoint",
            "checkpoint must never start beside a stuck connection"
        );
    }
}
