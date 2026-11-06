//! Shared request workers amortize I/O wakes and can run another connection's
//! ready task when a worker is occupied. The configured request-worker count,
//! stack size, initialization hook, and independent SQL blocking pool remain.

use std::cell::OnceCell;
use std::future::Future;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::Poll;
use std::thread;

type ConnectionFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type ConnectionJob = Box<dyn FnOnce() -> ConnectionFuture + Send + 'static>;

#[derive(Default)]
struct Drain {
    mutex: parking_lot::Mutex<()>,
    idle: parking_lot::Condvar,
}

struct Dispatcher {
    handle: tokio::runtime::Handle,
    load: Arc<AtomicUsize>,
    drain: Arc<Drain>,
    /// This lock serializes admission with shutdown, never statement execution.
    shutdown: parking_lot::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    controller: parking_lot::Mutex<Option<thread::JoinHandle<()>>>,
}

thread_local! {
    /// Shared pool membership, including connections waiting for their first poll.
    static WORKER_LOAD: OnceCell<Arc<AtomicUsize>> = const { OnceCell::new() };
}

/// Keep shutdown waiting until the complete connection future has dropped,
/// including transaction cleanup on panic.
struct LoadGuard {
    load: Arc<AtomicUsize>,
    drain: Arc<Drain>,
}

impl Drop for LoadGuard {
    fn drop(&mut self) {
        if self.load.fetch_sub(1, Ordering::AcqRel) == 1 {
            let _guard = self.drain.mutex.lock();
            self.drain.idle.notify_all();
        }
    }
}

/// Only a globally sole connection may select the synchronous control shortcut.
/// A shared pool can move any sibling task onto this worker after a socket wake.
pub fn current_worker_owns_single_connection() -> bool {
    WORKER_LOAD.with(|load| {
        load.get()
            .is_some_and(|load| load.load(Ordering::Acquire) == 1)
    })
}

/// Embedded runtimes retain their caller's scheduling policy.
pub fn current_worker_has_sibling_connections() -> bool {
    WORKER_LOAD.with(|load| {
        load.get()
            .is_some_and(|load| load.load(Ordering::Acquire) > 1)
    })
}

async fn run_connection(job: ConnectionJob, guard: LoadGuard) {
    let _guard = guard;
    // A connection panic must not strand the drain count. Catch both factory
    // construction and polling, and drop the real future before its load guard.
    let result = match catch_unwind(AssertUnwindSafe(job)) {
        Ok(mut connection) => {
            std::future::poll_fn(|cx| {
                match catch_unwind(AssertUnwindSafe(|| connection.as_mut().poll(cx))) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
                    Err(panic) => Poll::Ready(Err(panic)),
                }
            })
            .await
        }
        Err(panic) => Err(panic),
    };
    if let Err(panic) = result {
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("unknown panic payload");
        crate::util::logging::log_message(format_args!(
            "connection worker task panicked: {message}"
        ));
    }
}

impl Dispatcher {
    fn new(count: usize, on_start: fn()) -> io::Result<Self> {
        let count = count.max(1);
        let load = Arc::new(AtomicUsize::new(0));
        let drain = Arc::new(Drain::default());
        let worker_load = Arc::clone(&load);
        let controller_load = Arc::clone(&load);
        let controller_drain = Arc::clone(&drain);
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        let (shutdown, stopping) = std::sync::mpsc::channel();
        // Runtime destruction may block and cannot run inside another Tokio
        // runtime. Its idle controller owns construction and final destruction;
        // only the configured workers poll connection futures.
        let controller = thread::Builder::new()
            .name("mydb-dispatch".into())
            .spawn(move || {
                let (initialized, workers_started) = std::sync::mpsc::channel();
                let next = AtomicUsize::new(0);
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(count)
                    .thread_stack_size(crate::util::blocking_pool::worker_stack_size())
                    .thread_name_fn(move || {
                        format!("mydb-net-{}", next.fetch_add(1, Ordering::Relaxed))
                    })
                    .on_thread_start(move || {
                        let result = catch_unwind(AssertUnwindSafe(|| {
                            WORKER_LOAD.with(|slot| {
                                let _ = slot.set(Arc::clone(&worker_load));
                            });
                            on_start();
                        }));
                        match result {
                            Ok(()) => {
                                let _ = initialized.send(Ok(()));
                            }
                            Err(panic) => {
                                let _ = initialized.send(Err(io::Error::other(
                                    "request worker initialization panicked",
                                )));
                                std::panic::resume_unwind(panic);
                            }
                        }
                    })
                    .enable_all()
                    .build();
                let runtime = match runtime {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        let _ = ready.send(Err(err));
                        return;
                    }
                };
                for _ in 0..count {
                    match workers_started.recv() {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            let _ = ready.send(Err(err));
                            return;
                        }
                        Err(_) => {
                            let _ = ready
                                .send(Err(io::Error::other("request worker failed to initialize")));
                            return;
                        }
                    }
                }
                if ready.send(Ok(runtime.handle().clone())).is_err() {
                    return;
                }
                let _ = stopping.recv();
                let mut guard = controller_drain.mutex.lock();
                while controller_load.load(Ordering::Acquire) != 0 {
                    controller_drain.idle.wait(&mut guard);
                }
                drop(guard);
                drop(runtime);
            })?;
        let handle = match started.recv() {
            Ok(Ok(handle)) => handle,
            result => {
                let _ = controller.join();
                return Err(match result {
                    Ok(Err(err)) => err,
                    _ => io::Error::other("request runtime failed to initialize"),
                });
            }
        };
        Ok(Self {
            handle,
            load,
            drain,
            shutdown: parking_lot::Mutex::new(Some(shutdown)),
            controller: parking_lot::Mutex::new(Some(controller)),
        })
    }

    fn spawn(&self, job: ConnectionJob) -> io::Result<()> {
        let admission = self.shutdown.lock();
        if admission.is_none() {
            return Err(io::Error::other("connection dispatcher is stopped"));
        }
        self.load.fetch_add(1, Ordering::AcqRel);
        let guard = LoadGuard {
            load: Arc::clone(&self.load),
            drain: Arc::clone(&self.drain),
        };
        self.handle.spawn(run_connection(job, guard));
        Ok(())
    }

    fn stop(&self) {
        if let Some(shutdown) = self.shutdown.lock().take() {
            let _ = shutdown.send(());
        }
        if let Some(controller) = self.controller.lock().take() {
            if controller.join().is_err() {
                crate::util::logging::log_message(format_args!(
                    "connection runtime controller panicked"
                ));
            }
        }
    }
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

static DISPATCHER: OnceLock<Dispatcher> = OnceLock::new();

/// Keep exactly the existing configured request-worker count. Embedded servers
/// without initialization retain the caller-supplied runtime.
pub fn initialize(count: usize, on_start: fn()) -> io::Result<()> {
    let dispatcher = Dispatcher::new(count, on_start)?;
    DISPATCHER
        .set(dispatcher)
        .map_err(|_| io::Error::other("connection dispatcher already initialized"))
}

pub fn spawn<F, Fut>(factory: F) -> io::Result<()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let job: ConnectionJob = Box::new(move || Box::pin(factory()));
    match DISPATCHER.get() {
        Some(dispatcher) => dispatcher.spawn(job),
        None => {
            tokio::spawn(async move { job().await });
            Ok(())
        }
    }
}

pub fn shutdown() {
    if let Some(dispatcher) = DISPATCHER.get() {
        dispatcher.stop();
    }
}

#[cfg(test)]
pub(crate) fn test_set_worker_connection_count(connections: usize) {
    WORKER_LOAD.with(|slot| {
        slot.set(Arc::new(AtomicUsize::new(connections)))
            .expect("fresh test reactor thread");
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_connection_can_use_an_idle_worker_while_another_worker_blocks() {
        let dispatcher = Dispatcher::new(2, || {}).unwrap();
        let (entered, entries) = std::sync::mpsc::channel();
        let (release_blocked, blocked) = std::sync::mpsc::sync_channel(0);
        let (release_idle, idle) = tokio::sync::oneshot::channel();
        let first_entered = entered.clone();
        dispatcher
            .spawn(Box::new(move || {
                Box::pin(async move {
                    first_entered.send(()).unwrap();
                    blocked.recv().unwrap();
                })
            }))
            .unwrap();
        entries
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        dispatcher
            .spawn(Box::new(move || {
                Box::pin(async move {
                    entered.send(()).unwrap();
                    idle.await.unwrap();
                })
            }))
            .unwrap();
        entries
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        let (completed, completion) = std::sync::mpsc::channel();
        dispatcher
            .spawn(Box::new(move || {
                Box::pin(async move { completed.send(()).unwrap() })
            }))
            .unwrap();
        let progressed = completion.recv_timeout(std::time::Duration::from_secs(1));
        // Always release both live connections before checking the result.
        release_blocked.send(()).unwrap();
        release_idle.send(()).unwrap();
        dispatcher.stop();
        assert!(
            progressed.is_ok(),
            "ready work must not wait behind an unrelated blocked worker"
        );
    }

    #[test]
    fn connection_keeps_pool_membership_across_io_and_timer_wakes_and_shutdown_drains() {
        static INITIALIZED: AtomicUsize = AtomicUsize::new(0);
        INITIALIZED.store(0, Ordering::Relaxed);
        let dispatcher = Dispatcher::new(2, || {
            INITIALIZED.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
        assert_eq!(INITIALIZED.load(Ordering::Relaxed), 2);
        let (completed, events) = std::sync::mpsc::channel();
        for _ in 0..4 {
            let completed = completed.clone();
            dispatcher
                .spawn(Box::new(move || {
                    Box::pin(async move {
                        let pool = WORKER_LOAD.with(|load| {
                            Arc::as_ptr(load.get().expect("request worker membership")) as usize
                        });
                        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                        let address = listener.local_addr().unwrap();
                        let connect = tokio::spawn(async move {
                            tokio::net::TcpStream::connect(address).await.unwrap()
                        });
                        let (_socket, _) = listener.accept().await.unwrap();
                        drop(connect.await.unwrap());
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        assert_eq!(
                            pool,
                            WORKER_LOAD.with(|load| {
                                Arc::as_ptr(load.get().expect("request worker membership")) as usize
                            })
                        );
                        completed.send(pool).unwrap();
                    })
                }))
                .unwrap();
        }
        dispatcher.stop();
        let owners = events.try_iter().collect::<Vec<_>>();
        assert_eq!(owners.len(), 4);
        assert_eq!(
            owners
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            1
        );
    }

    #[test]
    fn shutdown_drains_panicked_connections_and_rejects_new_jobs() {
        struct Cleanup(Arc<AtomicUsize>);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let dispatcher = Dispatcher::new(2, || {}).unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        dispatcher
            .spawn(Box::new(|| panic!("injected connection factory panic")))
            .unwrap();
        let cleanup = Arc::clone(&dropped);
        dispatcher
            .spawn(Box::new(move || {
                Box::pin(async move {
                    let _cleanup = Cleanup(cleanup);
                    tokio::task::yield_now().await;
                    panic!("injected connection poll panic");
                })
            }))
            .unwrap();
        let completed = Arc::new(AtomicUsize::new(0));
        let completion = Arc::clone(&completed);
        dispatcher
            .spawn(Box::new(move || {
                Box::pin(async move {
                    tokio::task::yield_now().await;
                    completion.fetch_add(1, Ordering::Relaxed);
                })
            }))
            .unwrap();
        // Shutdown must also be callable by a listener's existing runtime.
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async { dispatcher.stop() });
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        assert_eq!(completed.load(Ordering::Relaxed), 1);
        assert_eq!(dispatcher.load.load(Ordering::Acquire), 0);
        assert!(dispatcher.spawn(Box::new(|| Box::pin(async {}))).is_err());
    }
}

#[cfg(test)]
#[test]
fn transferred_socket_is_registered_on_request_runtime() {
    use std::io::{Read, Write};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dispatcher = Dispatcher::new(2, || {}).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let client = thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        stream.write_all(b"owner").unwrap();
        let mut response = [0; 5];
        stream.read_exact(&mut response).unwrap();
        assert_eq!(&response, b"owner");
    });
    let (stream, _) = listener.accept().unwrap();
    stream.set_nonblocking(true).unwrap();
    dispatcher
        .spawn(Box::new(move || {
            Box::pin(async move {
                let mut stream = tokio::net::TcpStream::from_std(stream).unwrap();
                let mut request = [0; 5];
                stream.read_exact(&mut request).await.unwrap();
                stream.write_all(&request).await.unwrap();
            })
        }))
        .unwrap();
    client.join().unwrap();
    dispatcher.stop();
}
