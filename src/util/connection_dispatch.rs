//! Connection-owned reactors retain socket locality. Unclaimed connection
//! factories can be reassigned when their selected worker cannot poll them.
//! Potentially blocking statement work keeps the pool-wide admission guard.

use crate::util::thread_spawn as thread;
use std::cell::{Cell, OnceCell};
use std::future::Future;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, OnceLock};
use std::task::Poll;
use std::time::{Duration, Instant};

use smallvec::SmallVec;
use tokio::task::JoinSet;

type ConnectionFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type ConnectionJob = Box<dyn FnOnce() -> ConnectionFuture + Send + 'static>;
const ADMISSION_RETRY: Duration = Duration::from_millis(1);

// Polling is written on every connection poll. Keep different workers' hot
// flags on separate cache lines; connection counts change only at admission
// and teardown.
#[repr(align(64))]
#[derive(Debug, Default)]
struct WorkerState {
    connections: AtomicUsize,
    polling: AtomicBool,
    worker_count: usize,
}

struct Worker {
    sender: tokio::sync::mpsc::UnboundedSender<Message>,
    state: Arc<WorkerState>,
}

enum Message {
    Connection(Arc<Admission>),
    Stop,
}

enum Event {
    Watch(Arc<Admission>),
    Idle,
    Stop,
}

struct Drain {
    load: Arc<AtomicUsize>,
    events: mpsc::Sender<Event>,
}

struct LoadGuard {
    worker: Arc<WorkerState>,
    drain: Arc<Drain>,
}

impl LoadGuard {
    fn move_to(&mut self, worker: &Arc<WorkerState>) {
        if !Arc::ptr_eq(&self.worker, worker) {
            worker.connections.fetch_add(1, Ordering::AcqRel);
            self.worker.connections.fetch_sub(1, Ordering::AcqRel);
            self.worker = Arc::clone(worker);
        }
    }
}

impl Drop for LoadGuard {
    fn drop(&mut self) {
        debug_assert!(self.worker.connections.load(Ordering::Acquire) != 0);
        self.worker.connections.fetch_sub(1, Ordering::AcqRel);
        if self.drain.load.fetch_sub(1, Ordering::AcqRel) == 1 {
            let _ = self.drain.events.send(Event::Idle);
        }
    }
}

struct Pending {
    // Drop a never-started factory before releasing its admission reservation.
    job: Option<ConnectionJob>,
    guard: Option<LoadGuard>,
    delivered: SmallVec<[usize; 2]>,
}

struct Admission {
    pending: parking_lot::Mutex<Pending>,
}

impl Admission {
    fn claim(&self, worker: &Arc<WorkerState>) -> Option<(ConnectionJob, LoadGuard)> {
        let (job, mut guard) = {
            let mut pending = self.pending.lock();
            let job = pending.job.take()?;
            let guard = pending.guard.take().expect("pending factory reservation");
            (job, guard)
        };
        guard.move_to(worker);
        Some((job, guard))
    }

    fn cancel(&self) -> bool {
        let canceled = {
            let mut pending = self.pending.lock();
            pending.job.take().map(|job| (job, pending.guard.take()))
        };
        // Captured destructors may call back into dispatch. Neither admission
        // mutex is held while dropping a rejected factory or its reservation.
        let result = canceled.is_some();
        drop(canceled);
        result
    }
}

struct Watch {
    admission: Arc<Admission>,
    retry_at: Instant,
}

struct Dispatcher {
    workers: Arc<[Worker]>,
    load: Arc<AtomicUsize>,
    drain: Arc<Drain>,
    next: AtomicUsize,
    // Serialize admission with shutdown, never with statement execution.
    admission: parking_lot::Mutex<Option<mpsc::Sender<Event>>>,
    controller: parking_lot::Mutex<Option<thread::JoinHandle<()>>>,
}

thread_local! {
    static WORKER_LOAD: OnceCell<Arc<AtomicUsize>> = const { OnceCell::new() };
    static CURRENT_WORKER: OnceCell<Arc<WorkerState>> = const { OnceCell::new() };
    static COMMANDS_THIS_POLL: Cell<u8> = const { Cell::new(0) };
    static REACTOR_CPU: Cell<Option<usize>> = const { Cell::new(None) };
}

/// Only a process-wide sole connection may execute potentially blocking work
/// synchronously. Keeping this conservative guard avoids stalling sibling
/// sessions, including ones admitted after a connection selected its reactor.
pub fn current_worker_owns_single_connection() -> bool {
    WORKER_LOAD.with(|load| {
        load.get()
            .is_some_and(|load| load.load(Ordering::Acquire) == 1)
    })
}

/// Embedded runtimes retain their caller-supplied scheduling policy.
pub fn current_worker_has_sibling_connections() -> bool {
    WORKER_LOAD.with(|load| {
        load.get()
            .is_some_and(|load| load.load(Ordering::Acquire) > 1)
    })
}

/// Complete transactions on an exclusive reactor while another reactor remains
/// available for new connections. Once the pool is saturated, new completion
/// attempts use the independent blocking pool. A later admission cannot acquire
/// locks on this single-threaded reactor before its current poll returns.
pub fn can_inline_transaction_control() -> bool {
    CURRENT_WORKER.with(|worker| {
        worker.get().map_or_else(
            || !current_worker_has_sibling_connections(),
            |worker| {
                let load = WORKER_LOAD.with(|load| {
                    load.get()
                        .expect("connection reactor membership")
                        .load(Ordering::Acquire)
                });
                worker.connections.load(Ordering::Acquire) == 1
                    && (load == 1 || load < worker.worker_count)
            },
        )
    })
}

/// Buffered protocol reads can remain ready without returning to the I/O
/// driver. Bound buffered bursts without adding an extra turn when ordinary
/// socket or executor waits have already returned control to the scheduler.
pub async fn yield_to_peer_connections() {
    let peers = CURRENT_WORKER.with(|worker| {
        worker
            .get()
            .is_some_and(|worker| worker.connections.load(Ordering::Acquire) > 1)
    });
    if peers {
        if COMMANDS_THIS_POLL.with(|commands| commands.get() != 0) {
            tokio::task::yield_now().await;
        }
        // A suspended I/O/executor operation resets this in the outer poll.
        // Resumption after our explicit yield starts a fresh command turn.
        COMMANDS_THIS_POLL.with(|commands| commands.set(1));
    }
}

struct Polling<'a>(&'a AtomicBool);
impl<'a> Polling<'a> {
    fn enter(state: &'a WorkerState) -> Self {
        state.polling.store(true, Ordering::Release);
        Self(&state.polling)
    }
}
impl Drop for Polling<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

async fn run_connection(admission: Arc<Admission>, worker: Arc<WorkerState>) {
    // Claim only on the first poll. A task queued behind blocking work has
    // not registered its socket yet, so another reactor can still admit it.
    let Some((job, guard)) = admission.claim(&worker) else {
        return;
    };
    let _guard = guard;
    let factory = {
        let _polling = Polling::enter(&worker);
        catch_unwind(AssertUnwindSafe(job))
    };
    let result = match factory {
        Ok(mut connection) => {
            std::future::poll_fn(|cx| {
                COMMANDS_THIS_POLL.with(|commands| commands.set(0));
                let _polling = Polling::enter(&worker);
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

static SERVER_PARALLELISM: OnceLock<std::num::NonZeroUsize> = OnceLock::new();
#[cfg(target_os = "linux")]
static SERVER_CPUS: OnceLock<Vec<usize>> = OnceLock::new();

pub(crate) fn reactor_server_parallelism() -> Option<std::num::NonZeroUsize> {
    REACTOR_CPU.with(|cpu| cpu.get().and_then(|_| SERVER_PARALLELISM.get().copied()))
}

fn affinity_targets(count: usize, allowed: &[usize], enabled: bool) -> Vec<Option<usize>> {
    if enabled && count <= allowed.len() {
        allowed[..count].iter().copied().map(Some).collect()
    } else {
        vec![None; count]
    }
}

fn worker_cpu_targets(count: usize) -> io::Result<Vec<Option<usize>>> {
    let mode = crate::util::env::var("MYDB_TOKIO_AFFINITY").unwrap_or_else(|_| "auto".into());
    let enabled = match mode.trim() {
        value if value.eq_ignore_ascii_case("auto") => true,
        value if value.eq_ignore_ascii_case("off") => false,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MYDB_TOKIO_AFFINITY must be auto or off",
            ))
        }
    };
    let fits_quota = crate::util::thread_spawn::available_parallelism()
        .is_ok_and(|parallelism| count <= parallelism.get());
    Ok(affinity_targets(
        count,
        &allowed_worker_cpus(),
        enabled && fits_quota,
    ))
}

#[cfg(target_os = "linux")]
fn allowed_worker_cpus() -> Vec<usize> {
    // Read the effective calling-thread mask, including taskset and cgroup
    // restrictions. Do not assume that logical CPU ids are contiguous.
    let mut mask: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    if unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&mask), &mut mask) } != 0 {
        return Vec::new();
    }
    (0..libc::CPU_SETSIZE as usize)
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &mask) })
        .collect()
}

#[cfg(not(target_os = "linux"))]
fn allowed_worker_cpus() -> Vec<usize> {
    Vec::new()
}

fn pin_worker_cpu(cpu: Option<usize>) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    if let Some(cpu) = cpu {
        let mut mask: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        unsafe { libc::CPU_SET(cpu, &mut mask) };
        if unsafe { libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask) } != 0 {
            return Err(io::Error::other(format!(
                "cannot bind connection reactor to CPU {cpu}: {}",
                io::Error::last_os_error()
            )));
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = cpu;
    REACTOR_CPU.with(|slot| slot.set(cpu));
    Ok(())
}

pub(crate) struct SpawnCpuScope {
    cpu: Option<usize>,
    _thread_bound: std::marker::PhantomData<std::rc::Rc<()>>,
}

#[cfg(target_os = "linux")]
fn restore_server_cpu_mask() -> io::Result<()> {
    let mut mask: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // The unbound main thread retains the current server/cgroup mask.
    if unsafe { libc::sched_getaffinity(libc::getpid(), std::mem::size_of_val(&mask), &mut mask) }
        != 0
    {
        return Err(io::Error::last_os_error());
    }
    if let Some(server) = SERVER_CPUS.get() {
        let mut selected: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        let mut any = false;
        for &cpu in server {
            if unsafe { libc::CPU_ISSET(cpu, &mask) } {
                unsafe { libc::CPU_SET(cpu, &mut selected) };
                any = true;
            }
        }
        // Retain the initializing caller's restriction when embedded in a
        // wider process. A disjoint cgroup move follows the new process mask.
        if any {
            mask = selected;
        }
    }
    if unsafe { libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub(crate) fn unpin_for_spawn() -> io::Result<SpawnCpuScope> {
    let cpu = REACTOR_CPU.with(Cell::take);
    #[cfg(target_os = "linux")]
    if let Some(cpu) = cpu {
        if let Err(error) = restore_server_cpu_mask() {
            REACTOR_CPU.with(|slot| slot.set(Some(cpu)));
            return Err(error);
        }
    }
    Ok(SpawnCpuScope {
        cpu,
        _thread_bound: std::marker::PhantomData,
    })
}

impl Drop for SpawnCpuScope {
    fn drop(&mut self) {
        if let Some(cpu) = self.cpu {
            if let Err(error) = pin_worker_cpu(Some(cpu)) {
                crate::util::logging::log_message(format_args!(
                    "cannot restore reactor CPU affinity: {error}"
                ));
            }
        }
    }
}

fn worker_main(
    worker: Arc<WorkerState>,
    load: Arc<AtomicUsize>,
    mut messages: tokio::sync::mpsc::UnboundedReceiver<Message>,
    on_start: fn(),
    started: mpsc::Sender<io::Result<()>>,
    cpu: Option<usize>,
) {
    let runtime = catch_unwind(AssertUnwindSafe(|| {
        pin_worker_cpu(cpu)?;
        CURRENT_WORKER.with(|slot| {
            slot.set(Arc::clone(&worker))
                .expect("fresh connection reactor state")
        });
        WORKER_LOAD.with(|slot| {
            slot.set(load).expect("fresh connection reactor thread");
        });
        on_start();
        tokio::runtime::Builder::new_current_thread()
            .on_thread_start(move || {
                #[cfg(target_os = "linux")]
                if cpu.is_some() {
                    if let Err(error) = restore_server_cpu_mask() {
                        crate::util::logging::log_message(format_args!(
                            "cannot restore reactor helper CPU mask: {error}"
                        ));
                    }
                }
            })
            .enable_all()
            .build()
    }));
    let runtime = match runtime {
        Ok(Ok(runtime)) => runtime,
        Ok(Err(error)) => {
            let _ = started.send(Err(error));
            return;
        }
        Err(_) => {
            let _ = started.send(Err(io::Error::other(
                "connection worker initialization panicked",
            )));
            return;
        }
    };
    let _ = started.send(Ok(()));
    runtime.block_on(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                message = messages.recv() => match message {
                    Some(Message::Connection(admission)) => {
                        connections.spawn(run_connection(admission, Arc::clone(&worker)));
                    }
                    Some(Message::Stop) | None => break,
                },
                result = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = result {
                        crate::util::logging::log_message(format_args!("connection task failed: {error}"));
                    }
                }
            }
        }
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result {
                crate::util::logging::log_message(format_args!("connection task failed: {error}"));
            }
        }
    });
}

fn stop_workers(workers: &[Worker], threads: Vec<thread::JoinHandle<()>>) {
    for worker in workers {
        let _ = worker.sender.send(Message::Stop);
    }
    for thread in threads {
        if thread.join().is_err() {
            crate::util::logging::log_message(format_args!("connection reactor panicked"));
        }
    }
}

fn retry_admission(admission: &Arc<Admission>, workers: &[Worker], next: &mut usize) -> bool {
    let target = {
        let mut pending = admission.pending.lock();
        if pending.job.is_none() || pending.delivered.len() == workers.len() {
            return false;
        }
        let mut best = None;
        let mut best_load = usize::MAX;
        for offset in 0..workers.len() {
            let index = (*next + offset) % workers.len();
            let worker = &workers[index];
            if worker.state.polling.load(Ordering::Acquire) || pending.delivered.contains(&index) {
                continue;
            }
            let load = worker.state.connections.load(Ordering::Acquire);
            if load < best_load {
                best = Some(index);
                best_load = load;
                if load == 0 {
                    break;
                }
            }
        }
        if let Some(index) = best {
            pending.delivered.push(index);
            *next = index.wrapping_add(1) % workers.len();
        }
        best
    };
    if let Some(index) = target {
        let _ = workers[index]
            .sender
            .send(Message::Connection(Arc::clone(admission)));
    }
    // Each worker receives at most one copy. Even unresponsive reactors cannot
    // grow an unbounded retry queue; exactly one first poll claims the factory.
    true
}

fn control_loop(workers: &[Worker], drain: &Drain, events: mpsc::Receiver<Event>) {
    let mut watches = Vec::<Watch>::new();
    let mut stopping = false;
    let mut next = 0;
    loop {
        if stopping && drain.load.load(Ordering::Acquire) == 0 {
            break;
        }
        let event = match watches.iter().map(|watch| watch.retry_at).min() {
            Some(deadline) => {
                events.recv_timeout(deadline.saturating_duration_since(Instant::now()))
            }
            None => events
                .recv()
                .map_err(|_| mpsc::RecvTimeoutError::Disconnected),
        };
        match event {
            Ok(Event::Watch(admission)) => watches.push(Watch {
                admission,
                retry_at: Instant::now() + ADMISSION_RETRY,
            }),
            Ok(Event::Stop) => stopping = true,
            Ok(Event::Idle) | Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
        let now = Instant::now();
        watches.retain_mut(|watch| {
            if watch.admission.pending.lock().job.is_none() {
                return false;
            }
            if watch.retry_at > now {
                return true;
            }
            watch.retry_at = now + ADMISSION_RETRY;
            retry_admission(&watch.admission, workers, &mut next)
        });
    }
}

impl Dispatcher {
    fn new(count: usize, on_start: fn()) -> io::Result<Self> {
        let count = count.max(1);
        // Capture CPU/quota-aware capacity before any reactor is bound.
        if let Ok(parallelism) = crate::util::thread_spawn::available_parallelism() {
            let _ = SERVER_PARALLELISM.set(parallelism);
        }
        #[cfg(target_os = "linux")]
        let _ = SERVER_CPUS.set(allowed_worker_cpus());
        let cpus = worker_cpu_targets(count)?;
        let (events, receiver) = mpsc::channel();
        let load = Arc::new(AtomicUsize::new(0));
        let drain = Arc::new(Drain {
            load: Arc::clone(&load),
            events: events.clone(),
        });
        let mut workers = Vec::with_capacity(count);
        let mut receivers = Vec::with_capacity(count);
        for _ in 0..count {
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            workers.push(Worker {
                sender,
                state: Arc::new(WorkerState {
                    worker_count: count,
                    ..WorkerState::default()
                }),
            });
            receivers.push(receiver);
        }
        let workers: Arc<[Worker]> = workers.into();
        let owned_workers = Arc::clone(&workers);
        let owned_drain = Arc::clone(&drain);
        let (ready, result) = mpsc::sync_channel(1);
        let controller = thread::Builder::new()
            .name("mydb-dispatch".into())
            .spawn(move || {
                let (started, results) = mpsc::channel();
                let mut threads = Vec::with_capacity(count);
                for (index, messages) in receivers.into_iter().enumerate() {
                    let worker = Arc::clone(&owned_workers[index].state);
                    let load = Arc::clone(&owned_drain.load);
                    let started = started.clone();
                    let cpu = cpus[index];
                    match thread::Builder::new()
                        .name(format!("mydb-net-{index}"))
                        .stack_size(crate::util::blocking_pool::worker_stack_size())
                        .spawn(move || worker_main(worker, load, messages, on_start, started, cpu))
                    {
                        Ok(worker) => threads.push(worker),
                        Err(error) => {
                            stop_workers(&owned_workers, threads);
                            let _ = ready.send(Err(error));
                            return;
                        }
                    }
                }
                drop(started);
                for _ in 0..count {
                    let error = match results.recv() {
                        Ok(Ok(())) => continue,
                        Ok(Err(error)) => error,
                        Err(_) => io::Error::other("connection worker failed to initialize"),
                    };
                    stop_workers(&owned_workers, threads);
                    let _ = ready.send(Err(error));
                    return;
                }
                if ready.send(Ok(())).is_ok() {
                    control_loop(&owned_workers, &owned_drain, receiver);
                }
                stop_workers(&owned_workers, threads);
            })?;
        match result.recv() {
            Ok(Ok(())) => Ok(Self {
                workers,
                load,
                drain,
                next: AtomicUsize::new(0),
                admission: parking_lot::Mutex::new(Some(events)),
                controller: parking_lot::Mutex::new(Some(controller)),
            }),
            error => {
                let _ = controller.join();
                Err(match error {
                    Ok(Err(error)) => error,
                    _ => io::Error::other("connection controller failed to initialize"),
                })
            }
        }
    }

    fn pick_worker(&self) -> usize {
        let start = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        (0..self.workers.len())
            .map(|offset| (start + offset) % self.workers.len())
            .min_by_key(|&index| {
                let state = &self.workers[index].state;
                (
                    state.connections.load(Ordering::Acquire),
                    state.polling.load(Ordering::Acquire),
                )
            })
            .expect("at least one connection worker")
    }

    fn spawn(&self, job: ConnectionJob) -> io::Result<()> {
        let admission = self.admission.lock();
        let controller = admission
            .as_ref()
            .ok_or_else(|| io::Error::other("connection dispatcher is stopped"))?
            .clone();
        let index = self.pick_worker();
        let worker = &self.workers[index];
        self.load.fetch_add(1, Ordering::AcqRel);
        worker.state.connections.fetch_add(1, Ordering::AcqRel);
        let ticket = Arc::new(Admission {
            pending: parking_lot::Mutex::new(Pending {
                job: Some(job),
                guard: Some(LoadGuard {
                    worker: Arc::clone(&worker.state),
                    drain: Arc::clone(&self.drain),
                }),
                delivered: smallvec::smallvec![index],
            }),
        });
        let watched = controller.send(Event::Watch(Arc::clone(&ticket))).is_ok();
        let delivered = watched
            && worker
                .sender
                .send(Message::Connection(Arc::clone(&ticket)))
                .is_ok();
        drop(admission);
        if !delivered && ticket.cancel() {
            return Err(io::Error::other("connection worker is stopped"));
        }
        Ok(())
    }

    fn stop(&self) {
        if let Some(controller) = self.admission.lock().take() {
            let _ = controller.send(Event::Stop);
        }
        if let Some(controller) = self.controller.lock().take() {
            let _ = controller.join();
        }
    }
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        self.stop();
    }
}
static DISPATCHER: OnceLock<Dispatcher> = OnceLock::new();

/// Keep the configured request-worker count, initialization hook and stack size.
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
    fn transaction_completion_uses_reactor_ownership_and_observes_new_peers() {
        for workers in [1, 2, 3] {
            let dispatcher = Dispatcher::new(workers, || {}).unwrap();
            let (entered, entries) = mpsc::channel();
            let (completed, completions) = mpsc::channel();
            let finish = Arc::new(tokio::sync::Barrier::new(2));
            let mut releases = Vec::new();
            for index in 0..2 {
                let entered = entered.clone();
                let completed = completed.clone();
                let finish = Arc::clone(&finish);
                let (release, start) = tokio::sync::oneshot::channel();
                releases.push(release);
                dispatcher
                    .spawn(Box::new(move || {
                        Box::pin(async move {
                            entered.send(can_inline_transaction_control()).unwrap();
                            start.await.unwrap();
                            completed
                                .send((
                                    current_worker_has_sibling_connections(),
                                    can_inline_transaction_control(),
                                ))
                                .unwrap();
                            // Retain both reservations until both observations finish.
                            finish.wait().await;
                        })
                    }))
                    .unwrap();
                assert_eq!(
                    entries.recv_timeout(Duration::from_secs(2)).unwrap(),
                    index == 0 || workers == 3
                );
            }
            for release in releases {
                release.send(()).unwrap();
            }
            dispatcher.stop();
            assert_eq!(
                completions.try_iter().collect::<Vec<_>>(),
                vec![(true, workers == 3); 2]
            );
        }
    }

    #[test]
    fn transaction_completion_keeps_a_reactor_available_for_new_connections() {
        let dispatcher = Dispatcher::new(3, || {}).unwrap();
        let (entered, entries) = mpsc::channel();
        let (completed, completions) = mpsc::channel();
        let mut starts = Vec::new();
        let mut finishes = Vec::new();
        for _ in 0..2 {
            let entered = entered.clone();
            let completed = completed.clone();
            let (start, started) = tokio::sync::oneshot::channel();
            let (finish, finished) = mpsc::channel();
            starts.push(start);
            finishes.push(finish);
            dispatcher
                .spawn(Box::new(move || {
                    Box::pin(async move {
                        entered.send(false).unwrap();
                        started.await.unwrap();
                        let inline = can_inline_transaction_control();
                        entered.send(inline).unwrap();
                        let released = finished.recv_timeout(Duration::from_secs(2)).is_ok();
                        completed.send(inline && released).unwrap();
                    })
                }))
                .unwrap();
            assert!(!entries.recv_timeout(Duration::from_secs(2)).unwrap());
        }
        for start in starts {
            start.send(()).unwrap();
        }
        for _ in 0..2 {
            assert!(entries.recv_timeout(Duration::from_secs(2)).unwrap());
        }
        dispatcher
            .spawn(Box::new(move || {
                Box::pin(async move {
                    let inline = can_inline_transaction_control();
                    for finish in finishes {
                        finish.send(()).unwrap();
                    }
                    completed.send(!inline).unwrap();
                })
            }))
            .unwrap();
        dispatcher.stop();
        assert_eq!(completions.try_iter().collect::<Vec<_>>(), vec![true; 3]);
    }

    #[test]
    fn transaction_completion_keeps_shared_peers_offloaded_after_other_connections_close() {
        let dispatcher = Dispatcher::new(3, || {}).unwrap();
        let (entered, entries) = mpsc::channel();
        let (observed, observations) = mpsc::channel();
        let finish = Arc::new(tokio::sync::Barrier::new(2));
        let mut clients = Vec::new();
        for _ in 0..4 {
            let entered = entered.clone();
            let observed = observed.clone();
            let finish = Arc::clone(&finish);
            let (release, action) = tokio::sync::oneshot::channel();
            dispatcher
                .spawn(Box::new(move || {
                    Box::pin(async move {
                        let owner = CURRENT_WORKER
                            .with(|worker| Arc::as_ptr(worker.get().unwrap()) as usize);
                        entered.send(owner).unwrap();
                        if action.await.unwrap() {
                            observed.send(can_inline_transaction_control()).unwrap();
                            finish.wait().await;
                        }
                    })
                }))
                .unwrap();
            clients.push((
                entries.recv_timeout(Duration::from_secs(2)).unwrap(),
                release,
            ));
        }
        let shared = clients
            .iter()
            .map(|(owner, _)| *owner)
            .find(|owner| clients.iter().filter(|(other, _)| other == owner).count() >= 2)
            .expect("four connections on three reactors share one owner");
        let mut peers = Vec::new();
        for (owner, release) in clients {
            if owner == shared && peers.len() < 2 {
                peers.push(release);
            } else {
                release.send(false).unwrap();
            }
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while dispatcher.load.load(Ordering::Acquire) != 2 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(dispatcher.load.load(Ordering::Acquire), 2);
        for release in peers {
            release.send(true).unwrap();
        }
        dispatcher.stop();
        assert_eq!(observations.try_iter().collect::<Vec<_>>(), vec![false; 2]);
    }

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

#[cfg(test)]
#[test]
fn admission_reassigns_an_unpolled_factory_and_never_executes_stale_deliveries() {
    let dispatcher = Dispatcher::new(2, || {}).unwrap();
    let (handle, handles) = mpsc::channel();
    let (release_first, first) = tokio::sync::oneshot::channel();
    dispatcher
        .spawn(Box::new(move || {
            Box::pin(async move {
                handle.send(tokio::runtime::Handle::current()).unwrap();
                let _ = first.await;
            })
        }))
        .unwrap();
    let owner = handles.recv_timeout(Duration::from_secs(2)).unwrap();
    let blocked_worker = dispatcher
        .workers
        .iter()
        .position(|worker| worker.state.connections.load(Ordering::Acquire) == 1)
        .unwrap();
    let (entered, entries) = mpsc::channel();
    let (release_second, second) = tokio::sync::oneshot::channel();
    dispatcher
        .spawn(Box::new(move || {
            Box::pin(async move {
                entered.send(()).unwrap();
                let _ = second.await;
            })
        }))
        .unwrap();
    entries.recv_timeout(Duration::from_secs(2)).unwrap();

    // A separately spawned child bypasses the connection poll marker. This
    // models a worker becoming unavailable after the initial selection.
    let (entered_blocker, blocker_started) = mpsc::channel();
    let (release_blocker, blocker) = mpsc::sync_channel(1);
    owner.spawn(async move {
        entered_blocker.send(()).unwrap();
        let _ = blocker.recv();
    });
    blocker_started
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    dispatcher.next.store(blocked_worker, Ordering::Relaxed);
    let executed = Arc::new(AtomicUsize::new(0));
    let execution = Arc::clone(&executed);
    let (completed, completion) = mpsc::channel();
    dispatcher
        .spawn(Box::new(move || {
            execution.fetch_add(1, Ordering::Relaxed);
            Box::pin(async move { completed.send(()).unwrap() })
        }))
        .unwrap();
    let progressed = completion.recv_timeout(Duration::from_secs(2));
    let _ = release_blocker.send(());
    let _ = release_first.send(());
    let _ = release_second.send(());
    dispatcher.stop();
    assert!(
        progressed.is_ok(),
        "unpolled factories must reach an idle reactor"
    );
    assert_eq!(executed.load(Ordering::Relaxed), 1);
    assert_eq!(dispatcher.load.load(Ordering::Acquire), 0);
    assert!(dispatcher.workers.iter().all(|worker| worker
        .state
        .connections
        .load(Ordering::Acquire)
        == 0));
}

#[cfg(test)]
#[test]
fn connection_retains_its_thread_after_socket_and_timer_wakes() {
    let dispatcher = Dispatcher::new(2, || {}).unwrap();
    let (completed, completions) = mpsc::channel();
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
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    assert_eq!(owner, thread::current().id());
                    completed.send(()).unwrap();
                })
            }))
            .unwrap();
    }
    dispatcher.stop();
    assert_eq!(completions.try_iter().count(), 4);
}

#[cfg(test)]
#[test]
fn worker_initialization_panic_returns_an_error_and_joins_started_workers() {
    assert!(Dispatcher::new(2, || panic!("injected initialization panic")).is_err());
}

#[cfg(test)]
#[test]
fn reactor_affinity_respects_sparse_masks_and_preserves_oversubscription_policy() {
    assert_eq!(
        affinity_targets(3, &[2, 8, 14, 25], true),
        vec![Some(2), Some(8), Some(14)]
    );
    assert_eq!(affinity_targets(5, &[2, 8, 14, 25], true), vec![None; 5]);
    assert_eq!(affinity_targets(2, &[2, 8, 14, 25], false), vec![None; 2]);
    assert_eq!(affinity_targets(1, &[], true), vec![None]);
}

#[cfg(all(test, target_os = "linux"))]
#[test]
fn reactor_affinity_only_changes_the_worker_thread_mask() {
    let original = allowed_worker_cpus();
    let Some(cpu) = original.first().copied() else {
        return;
    };
    crate::util::thread_spawn::spawn(move || {
        pin_worker_cpu(Some(cpu)).unwrap();
        assert_eq!(allowed_worker_cpus(), vec![cpu]);
    })
    .join()
    .unwrap();
    assert_eq!(allowed_worker_cpus(), original);
}

#[cfg(all(test, target_os = "linux"))]
#[test]
fn reactor_affinity_does_not_restrict_spawned_compute_workers() {
    let original = allowed_worker_cpus();
    let Some(cpu) = original.first().copied() else {
        return;
    };
    std::thread::spawn(move || {
        pin_worker_cpu(Some(cpu)).unwrap();
        assert_eq!(allowed_worker_cpus(), vec![cpu]);
        let expected = original.clone();
        let inherited = crate::util::thread_spawn::spawn(allowed_worker_cpus)
            .join()
            .unwrap();
        assert_eq!(inherited, expected);
        assert_eq!(allowed_worker_cpus(), vec![cpu]);
        let inherited = crate::util::thread_spawn::Builder::new()
            .spawn(allowed_worker_cpus)
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(inherited, expected);
        assert_eq!(allowed_worker_cpus(), vec![cpu]);
        let inherited = crate::util::thread_spawn::scope(|scope| {
            scope.spawn(allowed_worker_cpus).join().unwrap()
        });
        assert_eq!(inherited, expected);
        assert_eq!(allowed_worker_cpus(), vec![cpu]);
    })
    .join()
    .unwrap();
}

#[cfg(all(test, target_os = "linux"))]
#[test]
fn reactor_affinity_does_not_restrict_tokio_blocking_helpers() {
    let expected = allowed_worker_cpus();
    let dispatcher = Dispatcher::new(1, || {}).unwrap();
    let (done, result) = std::sync::mpsc::channel();
    dispatcher
        .spawn(Box::new(move || {
            Box::pin(async move {
                let reactor = allowed_worker_cpus();
                let inherited = tokio::task::spawn_blocking(allowed_worker_cpus)
                    .await
                    .unwrap();
                assert_eq!(allowed_worker_cpus(), reactor);
                done.send(inherited).unwrap();
            })
        }))
        .unwrap();
    assert_eq!(
        result.recv_timeout(Duration::from_secs(5)).unwrap(),
        expected
    );
    dispatcher.stop();
}

#[cfg(all(test, target_os = "linux"))]
#[test]
fn reactor_affinity_preserves_server_capacity_for_lazy_pool_sizing() {
    let expected = std::thread::available_parallelism().unwrap();
    let dispatcher = Dispatcher::new(1, || {}).unwrap();
    let (done, result) = std::sync::mpsc::channel();
    dispatcher
        .spawn(Box::new(move || {
            Box::pin(async move {
                done.send(crate::util::thread_spawn::available_parallelism().unwrap())
                    .unwrap();
            })
        }))
        .unwrap();
    assert_eq!(
        result.recv_timeout(Duration::from_secs(5)).unwrap(),
        expected
    );
    dispatcher.stop();
}

#[cfg(all(test, target_os = "linux"))]
#[test]
fn reactor_affinity_keeps_an_embedded_servers_narrower_cpu_boundary() {
    let original = allowed_worker_cpus();
    if original.len() < 2 {
        return;
    }
    let expected = original[..2].to_vec();
    std::thread::spawn(move || {
        let mut mask: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        for &cpu in &expected {
            unsafe { libc::CPU_SET(cpu, &mut mask) };
        }
        assert_eq!(
            unsafe { libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask) },
            0
        );
        let dispatcher = Dispatcher::new(1, || {}).unwrap();
        let (done, result) = std::sync::mpsc::channel();
        dispatcher
            .spawn(Box::new(move || {
                Box::pin(async move {
                    let inherited = crate::util::thread_spawn::spawn(allowed_worker_cpus)
                        .join()
                        .unwrap();
                    done.send(inherited).unwrap();
                })
            }))
            .unwrap();
        assert_eq!(
            result.recv_timeout(Duration::from_secs(5)).unwrap(),
            expected
        );
        dispatcher.stop();
    })
    .join()
    .unwrap();
    assert_eq!(allowed_worker_cpus(), original);
}

#[cfg(test)]
#[test]
fn admission_balances_connections_before_transient_poll_activity() {
    let dispatcher = Dispatcher::new(2, || {}).unwrap();
    dispatcher.workers[0]
        .state
        .connections
        .store(1, Ordering::Release);
    dispatcher.workers[0]
        .state
        .polling
        .store(true, Ordering::Release);
    dispatcher.workers[1]
        .state
        .connections
        .store(3, Ordering::Release);
    assert_eq!(dispatcher.pick_worker(), 0);
    dispatcher.workers[0]
        .state
        .connections
        .store(3, Ordering::Release);
    assert_eq!(dispatcher.pick_worker(), 1);
    for worker in dispatcher.workers.iter() {
        worker.state.polling.store(false, Ordering::Release);
        worker.state.connections.store(0, Ordering::Release);
    }
    dispatcher.stop();
}

#[cfg(test)]
#[test]
fn buffered_commands_allow_a_ready_peer_to_progress() {
    let dispatcher = Dispatcher::new(1, || {}).unwrap();
    let notify = Arc::new(tokio::sync::Notify::new());
    let (start, started) = tokio::sync::oneshot::channel();
    let (events, received) = mpsc::channel();
    let first_events = events.clone();
    let first_notify = Arc::clone(&notify);
    dispatcher
        .spawn(Box::new(move || {
            Box::pin(async move {
                started.await.unwrap();
                for command in 0..10 {
                    yield_to_peer_connections().await;
                    first_events.send(command).unwrap();
                    first_notify.notify_one();
                }
            })
        }))
        .unwrap();
    dispatcher
        .spawn(Box::new(move || {
            Box::pin(async move {
                notify.notified().await;
                events.send(99).unwrap();
            })
        }))
        .unwrap();
    start.send(()).unwrap();
    let order: Vec<_> = (0..11)
        .map(|_| received.recv_timeout(Duration::from_secs(5)).unwrap())
        .collect();
    dispatcher.stop();
    assert!(
        order.iter().position(|v| *v == 99).unwrap() < order.iter().position(|v| *v == 9).unwrap()
    );
}
