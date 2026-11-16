//! Transport-close observation without consuming command or TLS bytes.

use std::io;
use tokio::net::TcpStream;

#[cfg(target_os = "linux")]
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

    pub(crate) async fn closed(&self) {
        // An epoll fd is readable when its ready list is nonempty. Every
        // subscribed event terminates this wait, so no event drain/rearm or
        // timer is needed. Cancelling a pending wait preserves the watch.
        let _ = self.closed.readable().await;
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) struct PeerCloseMonitor {
    stream: TcpStream,
}

#[cfg(not(target_os = "linux"))]
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

    #[cfg(target_os = "linux")]
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

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn reset_is_observed_without_consuming_input() {
        let (client, server) = pair().await;
        let monitor = PeerCloseMonitor::new(&server).unwrap();
        client.set_linger(Some(Duration::ZERO)).unwrap();
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
}
