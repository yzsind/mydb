use super::SessionSeqCache;
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::types::QueryError;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};

#[derive(Debug)]
pub struct SessionSeqData {
    pub caches: HashMap<u64, SessionSeqCache>,
    pub currval: HashMap<u64, i64>,
    pub lastval: Option<u64>,
    charge: ControlledMemoryCharge,
}

fn accounted_bytes(caches: usize, current: usize) -> usize {
    std::mem::size_of::<SessionSeqData>()
        + 2 * std::mem::size_of::<usize>()
        + caches.saturating_mul(2 * (std::mem::size_of::<(u64, SessionSeqCache)>() + 8))
        + current.saturating_mul(2 * (std::mem::size_of::<(u64, i64)>() + 8))
}

impl SessionSeqData {
    fn new(charged: bool) -> Result<Self, QueryError> {
        Ok(Self {
            caches: HashMap::new(),
            currval: HashMap::new(),
            lastval: None,
            charge: ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::SystemMetadata,
                if charged { accounted_bytes(0, 0) } else { 0 },
            )?,
        })
    }

    fn clone_for_write(&self, keep_cache: bool) -> Result<Self, QueryError> {
        let charge = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            accounted_bytes(
                if keep_cache {
                    self.caches.capacity()
                } else {
                    0
                },
                self.currval.capacity(),
            ),
        )?;
        Ok(Self {
            caches: if keep_cache {
                self.caches.clone()
            } else {
                HashMap::new()
            },
            currval: self.currval.clone(),
            lastval: self.lastval,
            charge,
        })
    }

    fn reserve(&mut self, id: u64, cache: bool, current: bool) -> Result<(), QueryError> {
        let grow_cache =
            cache && self.caches.len() == self.caches.capacity() && !self.caches.contains_key(&id);
        let grow_current = current
            && self.currval.len() == self.currval.capacity()
            && !self.currval.contains_key(&id);
        if !grow_cache && !grow_current {
            return Ok(());
        }
        let envelope = accounted_bytes(
            if grow_cache {
                self.caches.capacity().saturating_mul(4).saturating_add(16)
            } else {
                self.caches.capacity()
            },
            if grow_current {
                self.currval.capacity().saturating_mul(4).saturating_add(16)
            } else {
                self.currval.capacity()
            },
        );
        self.charge
            .try_grow(envelope.saturating_sub(self.charge.bytes()))?;
        let result = (|| {
            if grow_cache {
                self.caches.try_reserve(1).map_err(|_| {
                    crate::storage::error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "sequence session cache allocation failed",
                    )
                })?;
            }
            if grow_current {
                self.currval.try_reserve(1).map_err(|_| {
                    crate::storage::error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "sequence currval allocation failed",
                    )
                })?;
            }
            Ok(())
        })();
        let actual = accounted_bytes(self.caches.capacity(), self.currval.capacity());
        debug_assert!(actual <= self.charge.bytes());
        self.charge
            .shrink(self.charge.bytes().saturating_sub(actual));
        result
    }
}

/// Read snapshots share immutable, governed state. A cloned execution context
/// never inherits an issuable CACHE block from the original connection.
#[derive(Debug)]
pub struct SessionSeqState {
    data: Option<Arc<SessionSeqData>>,
    keep_cache: bool,
}

impl Default for SessionSeqState {
    fn default() -> Self {
        Self {
            data: None,
            keep_cache: true,
        }
    }
}

impl Clone for SessionSeqState {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            keep_cache: false,
        }
    }
}

impl Deref for SessionSeqState {
    type Target = SessionSeqData;
    fn deref(&self) -> &Self::Target {
        static EMPTY: LazyLock<SessionSeqData> =
            LazyLock::new(|| SessionSeqData::new(false).expect("zero-byte sequence state"));
        self.data.as_deref().unwrap_or(&EMPTY)
    }
}

impl SessionSeqState {
    pub(crate) fn try_mut(&mut self) -> Result<&mut SessionSeqData, QueryError> {
        if self.data.is_none() {
            self.data = Some(Arc::new(SessionSeqData::new(true)?));
        }
        let data = self
            .data
            .as_mut()
            .expect("initialized session sequence state");
        if Arc::strong_count(data) != 1 {
            *data = Arc::new(data.clone_for_write(self.keep_cache)?);
        }
        let data = Arc::get_mut(data).expect("private sequence state");
        if !self.keep_cache {
            data.caches = HashMap::new();
            let actual = accounted_bytes(0, data.currval.capacity());
            data.charge
                .shrink(data.charge.bytes().saturating_sub(actual));
        }
        self.keep_cache = true;
        Ok(data)
    }

    pub(crate) fn take_cached(
        &mut self,
        id: u64,
        counter: u64,
        epoch: u64,
    ) -> Result<Option<i64>, QueryError> {
        if self.data.is_none() || !self.keep_cache {
            return Ok(None);
        }
        let data = self.try_mut()?;
        let value = data.caches.get_mut(&id).and_then(|cache| {
            if cache.counter_id != counter || cache.role_epoch != epoch || cache.remaining == 0 {
                return None;
            }
            let value = cache.next;
            cache.remaining -= 1;
            if cache.remaining > 0 {
                cache.next = cache
                    .next
                    .checked_add(cache.increment)
                    .expect("validated sequence interval");
            }
            Some(value)
        });
        if let Some(value) = value {
            *data
                .currval
                .get_mut(&id)
                .expect("CACHE owns a currval entry") = value;
            data.lastval = Some(id);
        }
        Ok(value)
    }

    pub(crate) fn prepare_refill(&mut self, id: u64) -> Result<(), QueryError> {
        self.try_mut()?.reserve(id, true, true)
    }
    pub(crate) fn prepare_setval(&mut self, id: u64, called: bool) -> Result<(), QueryError> {
        if called || self.data.is_some() {
            self.try_mut()?.reserve(id, false, called)?;
        }
        Ok(())
    }
    pub fn record(&mut self, id: u64, value: i64) -> Result<(), QueryError> {
        let data = self.try_mut()?;
        data.reserve(id, false, true)?;
        data.currval.insert(id, value);
        data.lastval = Some(id);
        Ok(())
    }
    pub fn invalidate(&mut self, id: u64) -> Result<(), QueryError> {
        if self.data.is_some() {
            self.try_mut()?.caches.remove(&id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_session_snapshots_do_not_duplicate_cached_ranges_or_memory() {
        let before = crate::engine::executor::mem_budget::controlled_memory_snapshot().0;
        {
            let mut original = SessionSeqState::default();
            original.prepare_refill(1).unwrap();
            original.try_mut().unwrap().caches.insert(
                1,
                SessionSeqCache {
                    counter_id: 1,
                    role_epoch: 0,
                    next: 2,
                    remaining: 9,
                    increment: 1,
                },
            );
            original.record(1, 1).unwrap();
            let mut copy = original.clone();
            assert_eq!(copy.currval.get(&1), Some(&1));
            assert!(copy.try_mut().unwrap().caches.is_empty());
            copy.record(1, 100).unwrap();
            assert_eq!(original.currval.get(&1), Some(&1));
            assert_eq!(original.caches.get(&1).unwrap().next, 2);
            assert!(crate::engine::executor::mem_budget::controlled_memory_snapshot().0 > before);
        }
        assert_eq!(
            crate::engine::executor::mem_budget::controlled_memory_snapshot().0,
            before
        );
    }
}
