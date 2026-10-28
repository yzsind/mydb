//! Exact transaction wake tokens for the row-lock FIFO. Registry latches are
//! never held while entering storage's row or deadlock-graph latches.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::storage::NonblockingRowWaitTarget;

type WaiterMap = HashMap<u64, tokio::sync::watch::Sender<u64>, ahash::RandomState>;

#[derive(Debug)]
pub(crate) struct AsyncRowLockRegistry {
    shards: Box<[parking_lot::Mutex<WaiterMap>]>,
    active: AtomicU64,
}

impl Default for AsyncRowLockRegistry {
    fn default() -> Self {
        Self {
            shards: (0..64)
                .map(|_| parking_lot::Mutex::new(WaiterMap::default()))
                .collect(),
            active: AtomicU64::new(0),
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
            sender.send_modify(|generation| *generation = generation.wrapping_add(1));
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
        self.receiver.changed().await
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
    }
}
