use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

/// Shutdown signal for a background worker that sleeps on a condition
/// variable.
///
/// The predicate is atomic because some workers also poll it while doing a
/// bounded batch of work.  The mutex closes the check-then-park race: both
/// [`wait_timeout`](Self::wait_timeout) and
/// [`request_shutdown`](Self::request_shutdown) synchronize through it, so a
/// shutdown notification cannot land between the worker's predicate check and
/// its actual wait.
pub(crate) struct BackgroundShutdown {
    requested: AtomicBool,
    wake_requested: AtomicBool,
    gate: Mutex<()>,
    wake: Condvar,
}

impl BackgroundShutdown {
    pub(crate) fn new() -> Self {
        Self {
            requested: AtomicBool::new(false),
            wake_requested: AtomicBool::new(false),
            gate: Mutex::new(()),
            wake: Condvar::new(),
        }
    }

    pub(crate) fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    /// Sleep until the timeout expires or shutdown is requested.
    ///
    /// Returns `true` when the caller should stop. Spurious wakeups are kept
    /// inside `wait_timeout_while` rather than being mistaken for a scheduled
    /// maintenance tick.
    pub(crate) fn wait_timeout(&self, timeout: Duration) -> bool {
        let guard = self.gate.lock().unwrap_or_else(|e| e.into_inner());
        if self.is_requested() {
            return true;
        }
        if self.wake_requested.swap(false, Ordering::AcqRel) {
            return false;
        }
        let _ = self
            .wake
            .wait_timeout_while(guard, timeout, |_| {
                !self.is_requested() && !self.wake_requested.load(Ordering::Acquire)
            })
            .unwrap_or_else(|e| e.into_inner());
        self.wake_requested.store(false, Ordering::Release);
        self.is_requested()
    }

    /// Publish a coalescing maintenance wake-up. A notification sent just
    /// before the worker parks remains pending, so the worker cannot miss the
    /// transition and sleep for its full idle backoff.
    pub(crate) fn request_wake(&self) {
        let _guard = self.gate.lock().unwrap_or_else(|e| e.into_inner());
        self.wake_requested.store(true, Ordering::Release);
        self.wake.notify_one();
    }

    /// Publish shutdown while holding the same mutex used to enter the wait.
    /// This is what makes the following notification impossible to miss.
    pub(crate) fn request_shutdown(&self) {
        let _guard = self.gate.lock().unwrap_or_else(|e| e.into_inner());
        self.requested.store(true, Ordering::Release);
        self.wake.notify_all();
    }
}

/// Common ownership and shutdown lifecycle for a named background thread.
/// Worker-specific cadence and work stay in the closure passed to [`spawn`].
pub(crate) struct BackgroundWorker {
    shutdown: Arc<BackgroundShutdown>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
}

impl BackgroundWorker {
    pub(crate) fn disabled() -> Self {
        Self {
            shutdown: Arc::new(BackgroundShutdown::new()),
            worker: Mutex::new(None),
        }
    }

    pub(crate) fn spawn(
        name: impl Into<String>,
        run: impl FnOnce(Arc<BackgroundShutdown>) + Send + 'static,
    ) -> Self {
        let shutdown = Arc::new(BackgroundShutdown::new());
        let worker_shutdown = Arc::clone(&shutdown);
        let worker = thread::Builder::new()
            .name(name.into())
            .spawn(move || run(worker_shutdown))
            .ok();
        Self {
            shutdown,
            worker: Mutex::new(worker),
        }
    }

    pub(crate) fn signal(&self) -> &BackgroundShutdown {
        &self.shutdown
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown.request_shutdown();
        if let Some(worker) = self.worker.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = worker.join();
        }
    }
}

impl Drop for BackgroundWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::{BackgroundShutdown, BackgroundWorker};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn shutdown_published_before_wait_does_not_sleep() {
        let shutdown = BackgroundShutdown::new();
        shutdown.request_shutdown();

        let started = Instant::now();
        assert!(shutdown.wait_timeout(Duration::from_secs(60)));
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn shutdown_wakes_a_parked_worker() {
        let shutdown = Arc::new(BackgroundShutdown::new());
        let worker_shutdown = Arc::clone(&shutdown);
        let worker =
            std::thread::spawn(move || worker_shutdown.wait_timeout(Duration::from_secs(60)));

        std::thread::sleep(Duration::from_millis(10));
        let started = Instant::now();
        shutdown.request_shutdown();
        assert!(worker.join().unwrap());
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn maintenance_wake_is_not_lost_before_or_during_wait() {
        let signal = Arc::new(BackgroundShutdown::new());
        signal.request_wake();
        let started = Instant::now();
        assert!(!signal.wait_timeout(Duration::from_secs(60)));
        assert!(started.elapsed() < Duration::from_millis(100));

        let worker_signal = Arc::clone(&signal);
        let worker =
            std::thread::spawn(move || worker_signal.wait_timeout(Duration::from_secs(60)));
        std::thread::sleep(Duration::from_millis(10));
        signal.request_wake();
        assert!(!worker.join().unwrap());
    }

    #[test]
    fn worker_shutdown_wakes_and_joins_thread() {
        let worker = BackgroundWorker::spawn("mydb-background-test", |shutdown| {
            assert!(shutdown.wait_timeout(Duration::from_secs(60)));
        });
        worker.shutdown();
    }
}
