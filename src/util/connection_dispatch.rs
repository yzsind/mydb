//! Connection-owned reactors keep a session on one executor thread across
//! socket wakes. The configured request worker count is unchanged; blocking
//! SQL still uses the independent blocking pool and retains its deadlock guard.

use std::cell::OnceCell;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;

use tokio::sync::mpsc;
use tokio::task::JoinSet;

type ConnectionFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type ConnectionJob = Box<dyn FnOnce() -> ConnectionFuture + Send + 'static>;

enum Message {
    Connection(ConnectionJob),
    Stop,
}

struct Dispatcher {
    workers: Vec<mpsc::UnboundedSender<Message>>,
    /// Live connections owned by each worker, including ones still queued.
    loads: Vec<Arc<AtomicUsize>>,
    next: AtomicUsize,
    threads: parking_lot::Mutex<Vec<thread::JoinHandle<()>>>,
}

thread_local! {
    /// Connection count of the worker running on this thread, if any.
    static WORKER_LOAD: OnceCell<Arc<AtomicUsize>> = const { OnceCell::new() };
}

/// Decrements a worker's connection count when its connection task ends,
/// including by panic or cancellation.
struct LoadGuard(Arc<AtomicUsize>);

impl Drop for LoadGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// True on a request worker that currently owns exactly one connection.
/// Such a worker has nothing else to drive, so that connection may run
/// blocking statement work directly instead of handing it to the blocking
/// pool and parking until the result comes back.
pub fn current_worker_owns_single_connection() -> bool {
    WORKER_LOAD.with(|load| {
        load.get()
            .is_some_and(|load| load.load(Ordering::Acquire) == 1)
    })
}

impl Dispatcher {
    fn new(count: usize, on_start: fn()) -> io::Result<Self> {
        let mut dispatcher = Self {
            workers: Vec::with_capacity(count.max(1)),
            loads: Vec::with_capacity(count.max(1)),
            next: AtomicUsize::new(0),
            threads: parking_lot::Mutex::new(Vec::with_capacity(count.max(1))),
        };
        for id in 0..count.max(1) {
            let (sender, mut receiver) = mpsc::unbounded_channel();
            let (ready, started) = std::sync::mpsc::sync_channel(1);
            let load = Arc::new(AtomicUsize::new(0));
            let worker_load = Arc::clone(&load);
            // Connections may execute blocking statement work on this thread
            // (see `current_worker_owns_single_connection`), so give it the
            // same stack as a blocking-pool worker.
            let worker = thread::Builder::new()
                .name(format!("mydb-net-{id}"))
                .stack_size(crate::util::blocking_pool::worker_stack_size())
                .spawn(move || {
                WORKER_LOAD.with(|slot| {
                    let _ = slot.set(Arc::clone(&worker_load));
                });
                on_start();
                let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                    Ok(runtime) => { let _ = ready.send(Ok(())); runtime }
                    Err(err) => { let _ = ready.send(Err(err)); return; }
                };
                runtime.block_on(async move {
                    let mut connections = JoinSet::new();
                    loop {
                        tokio::select! {
                            message = receiver.recv() => match message {
                                Some(Message::Connection(job)) => {
                                    let guard = LoadGuard(Arc::clone(&worker_load));
                                    let connection = job();
                                    connections.spawn(async move {
                                        let _guard = guard;
                                        connection.await;
                                    });
                                }
                                Some(Message::Stop) | None => break,
                            },
                            result = connections.join_next(), if !connections.is_empty() => {
                                if let Some(Err(err)) = result {
                                    crate::util::logging::log_message(format_args!("connection worker task failed: {err}"));
                                }
                            }
                        }
                    }
                    // Stop arrives after the listener has drained sessions.
                    // Never abort a connection while it owns transaction state.
                    while let Some(result) = connections.join_next().await {
                        if let Err(err) = result {
                            crate::util::logging::log_message(format_args!("connection worker task failed: {err}"));
                        }
                    }
                });
            })?;
            dispatcher.workers.push(sender);
            dispatcher.loads.push(load);
            dispatcher.threads.get_mut().push(worker);
            started
                .recv()
                .map_err(|_| io::Error::other("connection worker failed to initialize"))??;
        }
        Ok(dispatcher)
    }

    fn spawn(&self, job: ConnectionJob) -> io::Result<()> {
        let index = self.pick_worker();
        self.loads[index].fetch_add(1, Ordering::AcqRel);
        self.workers[index]
            .send(Message::Connection(job))
            .map_err(|_| {
                self.loads[index].fetch_sub(1, Ordering::AcqRel);
                io::Error::other("connection worker is stopped")
            })
    }

    /// Least-loaded worker, scanning from a rotating start so ties spread.
    /// Keeping connections on otherwise idle workers preserves the
    /// one-connection-per-worker case that allows inline blocking work.
    fn pick_worker(&self) -> usize {
        let count = self.workers.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed) % count;
        let mut best = start;
        let mut best_load = usize::MAX;
        for offset in 0..count {
            let index = (start + offset) % count;
            let load = self.loads[index].load(Ordering::Acquire);
            if load < best_load {
                best = index;
                best_load = load;
                if load == 0 {
                    break;
                }
            }
        }
        best
    }

    fn stop(&self) {
        for worker in &self.workers {
            let _ = worker.send(Message::Stop);
        }
        for worker in self.threads.lock().drain(..) {
            let _ = worker.join();
        }
    }
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

static DISPATCHER: OnceLock<Dispatcher> = OnceLock::new();

/// The process entry point creates exactly the existing configured request
/// worker count. Embedded servers without this initialization retain Tokio's
/// caller-supplied runtime, including small single-thread test runtimes.
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
mod tests {
    use super::*;

    #[test]
    fn connection_keeps_owner_across_io_and_timer_wakes_and_shutdown_drains() {
        let dispatcher = Dispatcher::new(2, || {}).unwrap();
        let (completed, events) = std::sync::mpsc::channel();
        for _ in 0..4 {
            let completed = completed.clone();
            dispatcher
                .spawn(Box::new(move || {
                    Box::pin(async move {
                        let owner = thread::current().id();
                        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                        let address = listener.local_addr().unwrap();
                        let connect = tokio::spawn(async move {
                            tokio::net::TcpStream::connect(address).await.unwrap()
                        });
                        let (_socket, _) = listener.accept().await.unwrap();
                        drop(connect.await.unwrap());
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        assert_eq!(owner, thread::current().id());
                        completed.send(owner).unwrap();
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
            2
        );
    }
}

#[cfg(test)]
#[test]
fn transferred_socket_is_registered_on_connection_owner() {
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
