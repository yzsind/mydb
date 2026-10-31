//! Object-sharded ticket state and an independent owner-to-shard directory.
//! Lock order is owner-directory shards, then object shards, in ascending
//! index order. Only real conflicts freeze all shards for deadlock detection.

use super::*;
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use parking_lot::{Mutex, MutexGuard};

const SHARDS: usize = 32;
type OwnerMap = hashbrown::HashMap<u64, u32, ahash::RandomState>;

struct Owners {
    masks: OwnerMap,
    complete: bool,
    charge: ControlledMemoryCharge,
}

impl Default for Owners {
    fn default() -> Self {
        Self {
            masks: Default::default(),
            complete: true,
            charge: ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)
                .expect("zero-byte metadata reservation"),
        }
    }
}

impl Owners {
    fn remember(&mut self, owner: u64, mask: u32) {
        if let Some(current) = self.masks.get_mut(&owner) {
            *current |= mask;
            return;
        }
        if self.masks.len() == self.masks.capacity() {
            let bytes = (self.masks.len() + 1).next_power_of_two().max(4)
                * 2
                * (std::mem::size_of::<(u64, u32)>() + 1);
            if !self
                .charge
                .try_grow_optional(bytes.saturating_sub(self.charge.bytes()))
            {
                // This directory is only a routing accelerator. Exact ticket
                // scans remain available when its optional memory is denied.
                self.complete = false;
                return;
            }
        }
        self.masks.insert(owner, mask);
    }
}

pub(super) struct MdlStateLatch {
    hash: ahash::RandomState,
    owners: [Mutex<Owners>; SHARDS],
    objects: [Mutex<MdlObjectMap>; SHARDS],
    next_ticket: AtomicU64,
    _charge: ControlledMemoryCharge,
}

impl MdlStateLatch {
    pub(super) fn try_new() -> Result<Self, QueryError> {
        let charge = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            std::mem::size_of::<Self>(),
        )?;
        Ok(Self {
            hash: ahash::RandomState::new(),
            owners: std::array::from_fn(|_| Mutex::new(Owners::default())),
            objects: std::array::from_fn(|_| Mutex::new(MdlObjectMap::default())),
            next_ticket: AtomicU64::new(0),
            _charge: charge,
        })
    }
}

impl std::fmt::Debug for MdlStateLatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MdlStateLatch")
            .field("shards", &SHARDS)
            .finish()
    }
}

type OwnerGuards<'a> = SmallVec<[(usize, MutexGuard<'a, Owners>); 1]>;
type ObjectGuards<'a> = SmallVec<[(usize, MutexGuard<'a, MdlObjectMap>); 4]>;

pub(super) struct MdlObjects<'a> {
    hash: &'a ahash::RandomState,
    guards: ObjectGuards<'a>,
}

impl<'a> MdlObjects<'a> {
    fn index(&self, key: &MdlObjectKey) -> usize {
        let shard = self.hash.hash_one(key) as usize % SHARDS;
        self.guards
            .iter()
            .position(|(index, _)| *index == shard)
            .expect("metadata object shard is locked")
    }
    pub(super) fn get(&self, key: &MdlObjectKey) -> Option<&ObjectLockState> {
        self.guards[self.index(key)].1.get(key)
    }
    pub(super) fn get_mut(&mut self, key: &MdlObjectKey) -> Option<&mut ObjectLockState> {
        let index = self.index(key);
        self.guards[index].1.get_mut(key)
    }
    pub(super) fn contains_key(&self, key: &MdlObjectKey) -> bool {
        self.get(key).is_some()
    }
    pub(super) fn entry(
        &mut self,
        key: MdlObjectKey,
    ) -> hashbrown::hash_map::Entry<'_, MdlObjectKey, ObjectLockState, ahash::RandomState> {
        let index = self.index(&key);
        self.guards[index].1.entry(key)
    }
    pub(super) fn remove(&mut self, key: &MdlObjectKey) -> Option<ObjectLockState> {
        let index = self.index(key);
        self.guards[index].1.remove(key)
    }
    pub(super) fn values(&self) -> impl Iterator<Item = &ObjectLockState> + use<'a, '_> {
        self.guards.iter().flat_map(|(_, map)| map.values())
    }
    pub(super) fn values_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut ObjectLockState> + use<'a, '_> {
        self.guards.iter_mut().flat_map(|(_, map)| map.values_mut())
    }
    pub(super) fn iter_mut(
        &mut self,
    ) -> impl Iterator<Item = (&MdlObjectKey, &mut ObjectLockState)> + use<'a, '_> {
        self.guards.iter_mut().flat_map(|(_, map)| map.iter_mut())
    }
    pub(super) fn retain(
        &mut self,
        mut predicate: impl FnMut(&MdlObjectKey, &mut ObjectLockState) -> bool,
    ) {
        for (_, map) in &mut self.guards {
            map.retain(&mut predicate);
        }
    }
}

pub(super) struct MdlState<'a> {
    // Release object guards before owner-directory guards.
    pub(super) objects: MdlObjects<'a>,
    owners: OwnerGuards<'a>,
    pub(super) next_ticket_id: &'a AtomicU64,
}

/// Keep one owner's routing entry stable while acquiring a recipe one object
/// at a time. Compatible GLOBAL tickets must not extend their shard's hold
/// across every table in the recipe. On failure, reacquire the recipe shards
/// in index order to undo just this attempt's grants and promotions.
pub(super) struct MdlOwnerRequest<'a> {
    latch: &'a MdlStateLatch,
    owners: OwnerGuards<'a>,
    mask: u32,
}

impl<'a> MdlOwnerRequest<'a> {
    pub(super) fn lock_key(&self, key: &MdlObjectKey) -> MdlState<'_> {
        self.latch.lock_key(key)
    }
    pub(super) fn into_state(self) -> MdlState<'a> {
        self.latch.objects(self.mask, self.owners)
    }
}

impl MdlState<'_> {
    pub(super) fn forget_if_empty(&mut self, owner: u64) {
        if self.objects.values().any(|object| {
            object
                .granted
                .iter()
                .chain(&object.waiting)
                .any(|t| t.owner_id == owner)
        }) {
            return;
        }
        let index = owner as usize % SHARDS;
        self.owners
            .iter_mut()
            .find(|(i, _)| *i == index)
            .expect("owner directory is locked")
            .1
            .masks
            .remove(&owner);
    }

    pub(super) fn rebuild_owners(&mut self) {
        assert_eq!(self.owners.len(), SHARDS);
        for (_, owners) in &mut self.owners {
            owners.masks.clear();
            owners.complete = true;
        }
        for (index, map) in &self.objects.guards {
            for object in map.values() {
                for ticket in object.granted.iter().chain(&object.waiting) {
                    self.owners[ticket.owner_id as usize % SHARDS]
                        .1
                        .remember(ticket.owner_id, 1 << index);
                }
            }
        }
    }
}

impl MdlStateLatch {
    fn objects<'a>(&'a self, mask: u32, owners: OwnerGuards<'a>) -> MdlState<'a> {
        let guards = (0..SHARDS)
            .filter(|index| mask & (1 << index) != 0)
            .map(|index| (index, self.objects[index].lock()))
            .collect();
        MdlState {
            objects: MdlObjects {
                hash: &self.hash,
                guards,
            },
            owners,
            next_ticket_id: &self.next_ticket,
        }
    }
    pub(super) fn lock(&self) -> MdlState<'_> {
        let owners = self
            .owners
            .iter()
            .enumerate()
            .map(|(index, shard)| (index, shard.lock()))
            .collect();
        self.objects(u32::MAX, owners)
    }
    pub(super) fn lock_requests(&self, owner: u64, requests: &[MdlRequest]) -> MdlState<'_> {
        self.owner_requests(owner, requests).into_state()
    }
    pub(super) fn owner_requests(
        &self,
        owner: u64,
        requests: &[MdlRequest],
    ) -> MdlOwnerRequest<'_> {
        let index = owner as usize % SHARDS;
        let mut directory = self.owners[index].lock();
        let mask = requests.iter().fold(0, |mask, r| {
            mask | (1 << (self.hash.hash_one(&r.key) as usize % SHARDS))
        });
        directory.remember(owner, mask);
        MdlOwnerRequest {
            latch: self,
            owners: smallvec::smallvec![(index, directory)],
            mask,
        }
    }
    pub(super) fn lock_owner(&self, owner: u64) -> MdlState<'_> {
        let index = owner as usize % SHARDS;
        let directory = self.owners[index].lock();
        let mask = if directory.complete {
            directory.masks.get(&owner).copied().unwrap_or(0)
        } else {
            u32::MAX
        };
        self.objects(mask, smallvec::smallvec![(index, directory)])
    }
    pub(super) fn lock_key(&self, key: &MdlObjectKey) -> MdlState<'_> {
        self.objects(
            1 << (self.hash.hash_one(key) as usize % SHARDS),
            SmallVec::new(),
        )
    }
}

pub(super) fn next_mdl_ticket_id(counter: &AtomicU64) -> u64 {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .expect("metadata ticket ids exhausted")
        + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn different_keys(manager: &MdlManager) -> (MdlObjectKey, MdlObjectKey) {
        let first = MdlObjectKey::table("test", "first");
        let first_index = manager.state.hash.hash_one(&first) as usize % SHARDS;
        let second = (0..128)
            .map(|i| MdlObjectKey::table("test", &format!("other_{i}")))
            .find(|key| manager.state.hash.hash_one(key) as usize % SHARDS != first_index)
            .unwrap();
        (first, second)
    }
    fn request(key: MdlObjectKey) -> MdlRequest {
        MdlRequest {
            key,
            lock_type: MdlLockType::Exclusive,
        }
    }

    #[test]
    fn unrelated_object_acquisition_does_not_wait_for_another_shard() {
        let manager = Arc::new(MdlManager::default());
        let (first, second) = different_keys(&manager);
        let held = manager.state.lock_key(&first);
        let (send, receive) = std::sync::mpsc::channel();
        let worker = Arc::clone(&manager);
        let thread = std::thread::spawn(move || {
            send.send(worker.acquire_many(
                2,
                Some(2),
                MdlDuration::Transaction,
                vec![request(second)],
                "test",
            ))
            .unwrap();
        });
        let result = receive.recv_timeout(std::time::Duration::from_secs(2));
        drop(held);
        thread.join().unwrap();
        result
            .expect("unrelated metadata shard was blocked")
            .unwrap();
        manager.release_owner(2);
    }

    #[test]
    fn deadlock_detection_includes_wait_edges_across_shards() {
        let manager = MdlManager::default();
        let (a, b) = different_keys(&manager);
        manager
            .acquire_many(
                1,
                Some(1),
                MdlDuration::Transaction,
                vec![request(a.clone())],
                "test",
            )
            .unwrap();
        manager
            .acquire_many(
                2,
                Some(2),
                MdlDuration::Transaction,
                vec![request(b.clone())],
                "test",
            )
            .unwrap();
        let pending = manager
            .acquire_many(
                1,
                Some(1),
                MdlDuration::Transaction,
                vec![request(b.clone())],
                "test",
            )
            .unwrap_err();
        assert!(crate::storage::core::is_lock_wait_pending_error(&pending));
        let deadlock = manager
            .acquire_many(
                2,
                Some(2),
                MdlDuration::Transaction,
                vec![request(a)],
                "test",
            )
            .unwrap_err();
        assert!(deadlock.is(crate::error::catalog::ER_LOCK_DEADLOCK));
        manager.release_owner(2);
        manager
            .acquire_many(
                1,
                Some(1),
                MdlDuration::Transaction,
                vec![request(b)],
                "test",
            )
            .unwrap();
        manager.release_owner(1);
        assert!(manager.snapshot().is_empty());
    }

    #[test]
    fn unavailable_owner_index_falls_back_to_exact_release() {
        let manager = MdlManager::default();
        let (a, b) = different_keys(&manager);
        manager
            .acquire_many(
                1,
                Some(1),
                MdlDuration::Transaction,
                vec![request(a), request(b)],
                "test",
            )
            .unwrap();
        {
            let mut directory = manager.state.owners[1].lock();
            directory.masks.clear();
            directory.complete = false;
        }
        assert!(manager.release_owner(1));
        assert!(manager.snapshot().is_empty());
    }
}
