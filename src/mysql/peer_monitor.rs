//! Transport-close observation without consuming command or TLS bytes.

use std::io;
use std::sync::{Arc, OnceLock};
use tokio::net::TcpStream;

use crate::engine::session_registry::SessionHandle;

/// Peer closure must stay observable while a connection runs synchronous SQL.
/// Register both the close descriptor and its task on a separate I/O reactor;
/// spawning on the connection's current-thread runtime would still starve it.
fn monitor_runtime() -> io::Result<&'static tokio::runtime::Runtime> {
    static RUNTIME: OnceLock<io::Result<tokio::runtime::Runtime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            let _cpu_scope = crate::util::connection_dispatch::unpin_for_spawn()?;
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("mydb-peer-close")
                .enable_io()
                .enable_time()
                .build()
        })
        .as_ref()
        .map_err(|err| io::Error::new(err.kind(), format!("peer-close reactor: {err}")))
}

pub(crate) struct PeerDisconnectWatch(tokio::task::JoinHandle<()>);

impl PeerDisconnectWatch {
    pub(crate) fn new(stream: &TcpStream, session: Arc<SessionHandle>) -> io::Result<Self> {
        let runtime = monitor_runtime()?;
        let _entered = runtime.enter();
        let peer = PeerCloseMonitor::new(stream)?;
        Ok(Self(runtime.spawn(async move {
            peer.closed().await;
            session.peer_disconnected();
        })))
    }
}

impl Drop for PeerDisconnectWatch {
    fn drop(&mut self) {
        // Bound descriptor and session-handle ownership to the command loop,
        // including normal COM_QUIT, task cancellation and unwinding.
        self.0.abort();
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) struct PeerCloseMonitor {
    closed: tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>,
}

#[cfg(target_os = "linux")]
impl PeerCloseMonitor {
    pub(crate) fn new(stream: &TcpStream) -> io::Result<Self> {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

        // The outer Tokio registration watches this epoll descriptor. Its
        // inner interest contains only transport-close/error events: ordinary
        // command bytes cannot wake the observer or require MSG_PEEK. The
        // session keeps its transport alive until the command future ends.
        // Closing either descriptor removes the corresponding kernel watch.
        // SAFETY: epoll_create1 takes no pointers and returns a fresh owned fd.
        let raw = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the successful call above transferred this fresh fd to us.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        let mut event = libc::epoll_event {
            events: (libc::EPOLLRDHUP | libc::EPOLLHUP | libc::EPOLLERR) as u32,
            u64: 0,
        };
        // SAFETY: both descriptors are open for this call and event points to
        // an initialized epoll_event. The kernel copies it before returning.
        if unsafe {
            libc::epoll_ctl(
                fd.as_raw_fd(),
                libc::EPOLL_CTL_ADD,
                stream.as_raw_fd(),
                &mut event,
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: this private OwnedFd remains open and unchanged until the
        // AsyncFd deregisters it on drop; no mutable backing-fd access escapes.
        let closed = unsafe {
            tokio::io::unix::AsyncFd::register_with_interest(fd, tokio::io::Interest::READABLE)
        }
        .map_err(io::Error::from)?;
        Ok(Self { closed })
    }
}

#[cfg(target_os = "macos")]
impl PeerCloseMonitor {
    pub(crate) fn new(stream: &TcpStream) -> io::Result<Self> {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

        // A high private watermark suppresses ordinary data wakes without
        // changing SO_RCVLOWAT. Darwin caps it at the receive capacity, so
        // readiness must still be checked for a terminal event below.
        // SAFETY: kqueue takes no pointers and returns a fresh owned fd.
        let raw = unsafe { libc::kqueue() };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the successful kqueue call transferred this fd to us.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        // SAFETY: fd is open, and F_SETFD takes an integer flag argument.
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let event = libc::kevent {
            ident: stream.as_raw_fd() as _,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: libc::NOTE_LOWAT,
            data: i32::MAX as _,
            udata: std::ptr::null_mut(),
        };
        // SAFETY: both fds are open, event is initialized and copied by the
        // kernel, and a zero output count requires no eventlist or timeout.
        if unsafe {
            libc::kevent(
                fd.as_raw_fd(),
                &event,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the private fd stays open and unchanged until AsyncFd
        // deregisters it on drop; no mutable backing-fd access escapes.
        let closed = unsafe {
            tokio::io::unix::AsyncFd::register_with_interest(fd, tokio::io::Interest::READABLE)
        }
        .map_err(io::Error::from)?;
        Ok(Self { closed })
    }

    pub(crate) async fn closed(&self) {
        use std::os::fd::AsRawFd;
        loop {
            let Ok(mut ready) = self.closed.readable().await else {
                return;
            };
            match ready.try_io(|inner| {
                let mut event = std::mem::MaybeUninit::<libc::kevent>::uninit();
                let timeout = libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                };
                // SAFETY: the private queue stays open; event has space for
                // one result and the zero timeout makes this call nonblocking.
                let count = unsafe {
                    libc::kevent(
                        inner.get_ref().as_raw_fd(),
                        std::ptr::null(),
                        0,
                        event.as_mut_ptr(),
                        1,
                        &timeout,
                    )
                };
                if count < 0 {
                    return Err(io::Error::last_os_error());
                }
                if count == 0 {
                    return Err(io::Error::from(io::ErrorKind::WouldBlock));
                }
                // SAFETY: a positive count initialized the sole output slot.
                let event = unsafe { event.assume_init() };
                Ok(event.flags & (libc::EV_EOF | libc::EV_ERROR) != 0)
            }) {
                Ok(Ok(true)) => return,
                Ok(Ok(false)) | Err(_) => {}
                Ok(Err(err)) if err.kind() == io::ErrorKind::Interrupted => {}
                Ok(Err(_)) => return,
            }
        }
    }
}

#[cfg(target_os = "linux")]
impl PeerCloseMonitor {
    pub(crate) async fn closed(&self) {
        // The private event queue becomes readable only on closure/error.
        // No drain/rearm is needed because the first event ends the wait.
        let _ = self.closed.readable().await;
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) struct PeerCloseMonitor {
    stream: TcpStream,
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl PeerCloseMonitor {
    pub(crate) fn new(stream: &TcpStream) -> io::Result<Self> {
        #[cfg(unix)]
        let socket = {
            use std::os::fd::AsFd;
            std::net::TcpStream::from(stream.as_fd().try_clone_to_owned()?)
        };
        #[cfg(windows)]
        let socket = {
            use std::os::windows::io::AsSocket;
            std::net::TcpStream::from(stream.as_socket().try_clone_to_owned()?)
        };
        Ok(Self {
            stream: TcpStream::from_std(socket)?,
        })
    }

    pub(crate) async fn closed(&self) {
        let mut byte = [0u8; 1];
        loop {
            match self.stream.ready(tokio::io::Interest::READABLE).await {
                Ok(ready) if ready.is_read_closed() || ready.is_error() => return,
                Err(_) => return,
                _ => {}
            }
            match self.stream.peek(&mut byte).await {
                Ok(0) | Err(_) => return,
                Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn pair() -> (TcpStream, TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server)
    }

    #[tokio::test]
    async fn disconnect_watch_cancels_without_consuming_pipeline_bytes() {
        let (mut client, mut server) = pair().await;
        let registry = crate::engine::session_registry::SessionRegistry::new();
        let session = registry.register(1, "root".into(), "localhost".into(), "test".into());
        let _watch = PeerDisconnectWatch::new(&server, Arc::clone(&session)).unwrap();
        client.write_all(b"queued command").await.unwrap();
        client.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), session.notified())
            .await
            .unwrap();
        assert!(session
            .kill_flag()
            .load(std::sync::atomic::Ordering::Acquire));
        let mut bytes = Vec::new();
        server.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"queued command");
    }

    #[tokio::test]
    async fn dropping_disconnect_watch_stops_the_observer() {
        let (client, server) = pair().await;
        let registry = crate::engine::session_registry::SessionRegistry::new();
        let session = registry.register(1, "root".into(), "localhost".into(), "test".into());
        let watch = PeerDisconnectWatch::new(&server, Arc::clone(&session)).unwrap();
        let task = watch.0.abort_handle();
        drop(watch);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(client);
        assert!(!session
            .kill_flag()
            .load(std::sync::atomic::Ordering::Acquire));
        // The observer no longer retains the session handle or transport.
        assert_eq!(Arc::strong_count(&session), 2);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn queued_commands_do_not_wake_the_close_monitor() {
        use std::future::Future;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let (mut client, _server) = pair().await;
        let monitor = PeerCloseMonitor::new(&_server).unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polls);
        let (armed, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let closed = monitor.closed();
            tokio::pin!(closed);
            let mut armed = Some(armed);
            std::future::poll_fn(|cx| {
                observed.fetch_add(1, Ordering::SeqCst);
                let result = closed.as_mut().poll(cx);
                if let Some(armed) = armed.take() {
                    let _ = armed.send(());
                }
                result
            })
            .await;
        });
        ready.await.unwrap();
        let before = polls.load(Ordering::SeqCst);
        client.write_all(b"queued command").await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let after = polls.load(Ordering::SeqCst);
        drop(client);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after, before,
            "ordinary input must not schedule the close observer"
        );
    }

    #[tokio::test]
    async fn pipeline_bytes_survive_a_cancelled_close_wait() {
        let (mut client, mut server) = pair().await;
        let monitor = PeerCloseMonitor::new(&server).unwrap();
        client.write_all(b"queued command").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), monitor.closed())
                .await
                .is_err()
        );
        let mut bytes = [0u8; 14];
        server.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"queued command");
        drop(client);
        tokio::time::timeout(Duration::from_secs(2), monitor.closed())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn half_close_is_observed_with_unread_input() {
        let (mut client, mut server) = pair().await;
        let monitor = PeerCloseMonitor::new(&server).unwrap();
        client.write_all(b"queued command").await.unwrap();
        client.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), monitor.closed())
            .await
            .unwrap();
        let mut bytes = Vec::new();
        server.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"queued command");
    }

    #[tokio::test]
    async fn close_before_observer_registration_is_not_lost() {
        let (client, server) = pair().await;
        drop(client);
        let monitor = PeerCloseMonitor::new(&server).unwrap();
        tokio::time::timeout(Duration::from_secs(2), monitor.closed())
            .await
            .unwrap();
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn reset_is_observed_without_consuming_input() {
        let (client, server) = pair().await;
        let monitor = PeerCloseMonitor::new(&server).unwrap();
        client.set_zero_linger().unwrap();
        drop(client);
        tokio::time::timeout(Duration::from_secs(2), monitor.closed())
            .await
            .unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn dropping_observers_releases_their_descriptors() {
        let (_client, server) = pair().await;
        let count = || std::fs::read_dir("/proc/self/fd").unwrap().count();
        let before = count();
        for _ in 0..256 {
            drop(PeerCloseMonitor::new(&server).unwrap());
        }
        assert!(
            count() <= before,
            "closed observers must not retain descriptors"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn nonterminal_kqueue_readiness_does_not_signal_disconnect() {
        use std::os::fd::AsRawFd;
        let (mut client, mut server) = pair().await;
        let monitor = PeerCloseMonitor::new(&server).unwrap();
        // Darwin clamps NOTE_LOWAT at the socket high watermark. Lower it
        // explicitly to exercise a nonterminal event without a timing race.
        let event = libc::kevent {
            ident: server.as_raw_fd() as _,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: libc::NOTE_LOWAT,
            data: 1,
            udata: std::ptr::null_mut(),
        };
        // SAFETY: both descriptors are open and event is initialized. There
        // is no output array and the kernel copies the filter update.
        assert_eq!(
            unsafe {
                libc::kevent(
                    monitor.closed.get_ref().as_raw_fd(),
                    &event,
                    1,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                )
            },
            0
        );
        client.write_all(b"queued command").await.unwrap();
        let premature = tokio::time::timeout(Duration::from_millis(75), monitor.closed()).await;
        client.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), monitor.closed())
            .await
            .unwrap();
        let mut bytes = Vec::new();
        server.read_to_end(&mut bytes).await.unwrap();
        assert!(premature.is_err(), "ordinary data readiness is not EOF");
        assert_eq!(bytes, b"queued command");
    }
}
