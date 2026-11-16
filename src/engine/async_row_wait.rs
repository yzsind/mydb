//! Exact transaction wake tokens for the row-lock FIFO. Registry latches are
//! never held while entering storage's row or deadlock-graph latches.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crate::storage::NonblockingRowWaitTarget;

type WaiterMap = HashMap<u64, tokio::sync::watch::Sender<u64>, ahash::RandomState>;

fn notification_clock_ns() -> u64 {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    ORIGIN
        .get_or_init(Instant::now)
        .elapsed()
        .as_nanos()
        .clamp(1, u128::from(u64::MAX)) as u64
}

#[derive(Debug, Default)]
struct TimingCounters {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TimingSnapshot {
    pub count: u64,
    pub total_us: u64,
    pub max_us: u64,
}

impl TimingCounters {
    fn record(&self, elapsed: Duration) {
        let us = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(us, Ordering::Relaxed);
        self.max_us.fetch_max(us, Ordering::Relaxed);
    }

    fn snapshot(&self) -> TimingSnapshot {
        TimingSnapshot {
            count: self.count.load(Ordering::Relaxed),
            total_us: self.total_us.load(Ordering::Relaxed),
            max_us: self.max_us.load(Ordering::Relaxed),
        }
    }
}

struct WaitSample<'a> {
    counters: &'a TimingCounters,
    started: Instant,
}

impl Drop for WaitSample<'_> {
    fn drop(&mut self) {
        self.counters.record(self.started.elapsed());
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AsyncRowLockTimingStats {
    pub waits: TimingSnapshot,
    pub wake_delay: TimingSnapshot,
}

#[derive(Debug)]
pub(crate) struct AsyncRowLockRegistry {
    shards: Box<[parking_lot::Mutex<WaiterMap>]>,
    active: AtomicU64,
    waits: TimingCounters,
    wake_delay: TimingCounters,
}

impl Default for AsyncRowLockRegistry {
    fn default() -> Self {
        Self {
            shards: (0..64)
                .map(|_| parking_lot::Mutex::new(WaiterMap::default()))
                .collect(),
            active: AtomicU64::new(0),
            waits: TimingCounters::default(),
            wake_delay: TimingCounters::default(),
        }
    }
}

impl AsyncRowLockRegistry {
    fn shard(&self, txn: u64) -> &parking_lot::Mutex<WaiterMap> {
        &self.shards[txn as usize & (self.shards.len() - 1)]
    }

    pub(crate) fn has_waiters(&self) -> bool {
        self.active.load(Ordering::Acquire) != 0
    }

    pub(crate) fn register(
        self: &Arc<Self>,
        target: NonblockingRowWaitTarget,
    ) -> AsyncRowLockSignal {
        let (sender, receiver) = tokio::sync::watch::channel(0);
        self.active.fetch_add(1, Ordering::AcqRel);
        self.shard(target.txn_id)
            .lock()
            .insert(target.txn_id, sender.clone());
        AsyncRowLockSignal {
            receiver,
            sender,
            registry: Arc::clone(self),
            target,
        }
    }

    pub(crate) fn notify(&self, txn: u64) {
        if let Some(sender) = self.shard(txn).lock().get(&txn) {
            // watch maintains its own generation even when two timestamps
            // coincide. Keep the existing eight-byte payload and wake policy.
            sender.send_modify(|notified| *notified = notification_clock_ns());
        }
    }

    pub(crate) fn timing_stats(&self) -> AsyncRowLockTimingStats {
        AsyncRowLockTimingStats {
            waits: self.waits.snapshot(),
            wake_delay: self.wake_delay.snapshot(),
        }
    }
}

/// One row FIFO subscription. Storage checks eligibility again after
/// registration, closing a release-before-subscribe race without polling or
/// broadcasting to transactions waiting for unrelated rows.
pub struct AsyncRowLockSignal {
    receiver: tokio::sync::watch::Receiver<u64>,
    sender: tokio::sync::watch::Sender<u64>,
    registry: Arc<AsyncRowLockRegistry>,
    pub(crate) target: NonblockingRowWaitTarget,
}

impl AsyncRowLockSignal {
    pub async fn changed(&mut self) -> Result<(), tokio::sync::watch::error::RecvError> {
        // This wait lives outside synchronous statement instrumentation.
        // Record cancellation/drop as well as ordinary wake and timeout exits.
        let _sample = WaitSample {
            counters: &self.registry.waits,
            started: Instant::now(),
        };
        let result = self.receiver.changed().await;
        if result.is_ok() {
            let notified = *self.receiver.borrow();
            if notified != 0 {
                self.registry.wake_delay.record(Duration::from_nanos(
                    notification_clock_ns().saturating_sub(notified),
                ));
            }
        }
        result
    }
}

impl Drop for AsyncRowLockSignal {
    fn drop(&mut self) {
        let mut shard = self.registry.shard(self.target.txn_id).lock();
        if shard
            .get(&self.target.txn_id)
            .is_some_and(|sender| sender.same_channel(&self.sender))
        {
            shard.remove(&self.target.txn_id);
        }
        self.registry.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_old_registration_keeps_replacement_wake_token() {
        let registry = Arc::new(AsyncRowLockRegistry::default());
        let target = NonblockingRowWaitTarget {
            txn_id: 7,
            table_id: 3,
            row_id: crate::storage::RowId::new(1, 0),
            mode: crate::storage::LockMode::Exclusive,
        };
        let old = registry.register(target);
        let mut current = registry.register(target);
        drop(old);
        assert!(registry.has_waiters());
        registry.notify(target.txn_id);
        tokio::time::timeout(std::time::Duration::from_secs(1), current.changed())
            .await
            .unwrap()
            .unwrap();
        drop(current);
        assert!(!registry.has_waiters());
        let timings = registry.timing_stats();
        assert_eq!(timings.waits.count, 1);
        assert_eq!(timings.wake_delay.count, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_wait_records_time_and_keeps_the_next_notification() {
        use std::future::Future;
        use std::task::Poll;

        let registry = Arc::new(AsyncRowLockRegistry::default());
        let mut signal = registry.register(NonblockingRowWaitTarget {
            txn_id: 9,
            table_id: 1,
            row_id: crate::storage::RowId::new(1, 0),
            mode: crate::storage::LockMode::Exclusive,
        });
        {
            let waiting = signal.changed();
            tokio::pin!(waiting);
            std::future::poll_fn(|cx| {
                assert!(waiting.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        assert_eq!(registry.timing_stats().waits.count, 1);
        assert_eq!(registry.timing_stats().wake_delay.count, 0);
        registry.notify(9);
        tokio::time::timeout(Duration::from_secs(1), signal.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(registry.timing_stats().waits.count, 2);
        assert_eq!(registry.timing_stats().wake_delay.count, 1);
    }
}
