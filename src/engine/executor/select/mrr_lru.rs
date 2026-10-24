//! O(1) LRU cache used by the INLJ row-at-a-time path to memoise B-tree
//! lookups for repeated outer index bounds inside a single batch of outer
//! rows.
//!
//! Why a dedicated type instead of a third-party crate: the cache lives in
//! one hot function (`execute_index_nested_loop_join_from_left_rows`),
//! mydb already has `hashbrown`, and avoiding the extra crate keeps the
//! dependency budget tight. The implementation is a standard intrusive
//! doubly-linked-list-of-nodes inside a `Vec` indexed by free slot,
//! paired with a `HashMap<K, NodeIdx>` for O(1) lookup. No `unsafe` and
//! no `Rc<RefCell<...>>`.
//!
//! Semantics:
//! - `get` returns the value AND promotes the entry to most-recently-used.
//! - `insert` overwrites an existing key (and promotes), otherwise inserts;
//!   when at capacity, the least-recently-used entry is evicted first.
//! - Capacity 0 disables the cache (every insert is a no-op, every get a
//!   miss).
//! - Capacity 1 degenerates to a single slot (covered by tests).
//! - Counters are read by the caller (the cache only tracks state; the
//!   caller updates global atomics via [`MrrLru::record_hit`] etc.).
//!
//! Time complexity: every op is O(1) amortised.

use hashbrown::HashMap;
use std::hash::Hash;

const NIL: usize = usize::MAX;

struct Node<K, V> {
    key: K,
    value: V,
    prev: usize,
    next: usize,
}

/// O(1) LRU cache. See module docs.
pub(crate) struct MrrLru<K: Eq + Hash + Clone, V> {
    nodes: Vec<Node<K, V>>,
    map: HashMap<K, usize>,
    head: usize,
    tail: usize,
    cap: usize,
}

impl<K, V> MrrLru<K, V>
where
    K: Eq + Hash + Clone,
{
    /// Create a cache holding at most `cap` entries. `cap == 0` builds a
    /// no-op cache (insert silently drops, get always misses).
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(cap.min(1024)),
            map: HashMap::with_capacity(cap.min(1024)),
            head: NIL,
            tail: NIL,
            cap,
        }
    }

    #[cfg(test)]
    /// Current entry count.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[cfg(test)]
    /// Configured capacity.
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Get an entry by key. Returns `None` on miss. On hit the entry is
    /// promoted to most-recently-used. When the entry is already the
    /// head we skip the unlink/relink so the hot read path is a single
    /// hashbrown lookup + one comparison.
    pub fn get(&mut self, key: &K) -> Option<&V> {
        if self.cap == 0 {
            return None;
        }
        let idx = *self.map.get(key)?;
        if idx != self.head {
            self.detach(idx);
            self.attach_at_head(idx);
        }
        Some(&self.nodes[idx].value)
    }

    /// Insert a `(key, value)` pair. If the key already exists its value
    /// is overwritten and it is promoted. Otherwise a new entry is added,
    /// evicting the least-recently-used entry when at capacity.
    ///
    /// Returns the evicted `(key, value)` when an eviction occurred.
    /// Capacity 0 silently drops the insert and returns `None`.
    pub fn insert(&mut self, key: K, value: V) -> Option<(K, V)> {
        if self.cap == 0 {
            return None;
        }
        if let Some(&idx) = self.map.get(&key) {
            self.nodes[idx].value = value;
            self.detach(idx);
            self.attach_at_head(idx);
            return None;
        }
        let evicted = if self.map.len() >= self.cap {
            self.pop_tail()
        } else {
            None
        };
        let idx = self.nodes.len();
        self.nodes.push(Node {
            key: key.clone(),
            value,
            prev: NIL,
            next: NIL,
        });
        self.map.insert(key, idx);
        self.attach_at_head(idx);
        evicted
    }

    fn detach(&mut self, idx: usize) {
        let (prev, next) = (self.nodes[idx].prev, self.nodes[idx].next);
        if prev != NIL {
            self.nodes[prev].next = next;
        } else {
            self.head = next;
        }
        if next != NIL {
            self.nodes[next].prev = prev;
        } else {
            self.tail = prev;
        }
        self.nodes[idx].prev = NIL;
        self.nodes[idx].next = NIL;
    }

    fn attach_at_head(&mut self, idx: usize) {
        let old_head = self.head;
        self.nodes[idx].prev = NIL;
        self.nodes[idx].next = old_head;
        if old_head != NIL {
            self.nodes[old_head].prev = idx;
        } else {
            // List was empty.
            self.tail = idx;
        }
        self.head = idx;
    }

    /// Remove and return the LRU entry. We physically remove the node
    /// from `self.nodes` by swap_remove, which means the entry that was
    /// at `nodes.len() - 1` now sits at the tail's old index — update the
    /// map and its neighbours to keep the linked list intact.
    fn pop_tail(&mut self) -> Option<(K, V)> {
        let tail = self.tail;
        if tail == NIL {
            return None;
        }
        self.detach(tail);
        let last = self.nodes.len() - 1;
        let evicted_node = if tail == last {
            self.nodes.pop().expect("tail==last so vec has at least 1")
        } else {
            // Swap-remove rewrites index `tail` with whatever was at
            // `last`; fix up the swapped-in node's pointers and its map
            // entry.
            let moved = self.nodes.swap_remove(tail);
            let swapped = &self.nodes[tail];
            let (sk, sp, sn) = (swapped.key.clone(), swapped.prev, swapped.next);
            if sp != NIL {
                self.nodes[sp].next = tail;
            } else {
                self.head = tail;
            }
            if sn != NIL {
                self.nodes[sn].prev = tail;
            } else {
                self.tail = tail;
            }
            self.map.insert(sk, tail);
            moved
        };
        self.map.remove(&evicted_node.key);
        Some((evicted_node.key, evicted_node.value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_zero_disables_cache() {
        let mut lru: MrrLru<i32, i32> = MrrLru::with_capacity(0);
        assert_eq!(lru.insert(1, 10), None);
        assert_eq!(lru.insert(2, 20), None);
        assert_eq!(lru.get(&1), None);
        assert_eq!(lru.len(), 0);
        assert_eq!(lru.capacity(), 0);
    }

    #[test]
    fn capacity_one_keeps_only_latest() {
        let mut lru: MrrLru<i32, i32> = MrrLru::with_capacity(1);
        assert_eq!(lru.insert(1, 10), None);
        // Inserting k=2 must evict k=1.
        assert_eq!(lru.insert(2, 20), Some((1, 10)));
        assert_eq!(lru.get(&1), None);
        assert_eq!(lru.get(&2), Some(&20));
        assert_eq!(lru.len(), 1);
    }

    #[test]
    fn basic_lru_eviction_order() {
        let mut lru: MrrLru<i32, i32> = MrrLru::with_capacity(3);
        lru.insert(1, 10);
        lru.insert(2, 20);
        lru.insert(3, 30);
        // All three should be present.
        assert_eq!(lru.get(&1), Some(&10));
        assert_eq!(lru.get(&2), Some(&20));
        assert_eq!(lru.get(&3), Some(&30));
        // After the three gets above, ORDER (MRU→LRU) is 3,2,1.
        // Insert k=4 — k=1 (LRU) must be evicted.
        let ev = lru.insert(4, 40);
        assert_eq!(ev, Some((1, 10)));
        assert_eq!(lru.get(&1), None);
        assert_eq!(lru.get(&4), Some(&40));
    }

    #[test]
    fn get_promotes_to_mru() {
        let mut lru: MrrLru<i32, i32> = MrrLru::with_capacity(3);
        lru.insert(1, 10);
        lru.insert(2, 20);
        lru.insert(3, 30);
        // Touch k=1 — now order (MRU→LRU) is 1,3,2.
        assert_eq!(lru.get(&1), Some(&10));
        // Insert k=4 — k=2 (now LRU) must be evicted, not k=1.
        let ev = lru.insert(4, 40);
        assert_eq!(ev, Some((2, 20)));
        assert_eq!(lru.get(&1), Some(&10));
        assert_eq!(lru.get(&2), None);
        assert_eq!(lru.get(&3), Some(&30));
        assert_eq!(lru.get(&4), Some(&40));
    }

    #[test]
    fn insert_existing_key_overwrites_and_promotes() {
        let mut lru: MrrLru<i32, i32> = MrrLru::with_capacity(3);
        lru.insert(1, 10);
        lru.insert(2, 20);
        lru.insert(3, 30);
        // Order MRU→LRU after inserts (no gets): 3,2,1.
        // Overwrite k=1=99 — now k=1 must be MRU and value updated.
        let ev = lru.insert(1, 99);
        assert_eq!(ev, None);
        assert_eq!(lru.len(), 3);
        assert_eq!(lru.get(&1), Some(&99));
        // After the get above the order MRU→LRU is 1,3,2 (since get also
        // promoted; the overwrite already promoted, but get on the head
        // is a no-op move).
        // Insert k=4 — k=2 (LRU) must go.
        let ev = lru.insert(4, 40);
        assert_eq!(ev, Some((2, 20)));
        assert_eq!(lru.get(&2), None);
        assert_eq!(lru.get(&3), Some(&30));
    }

    #[test]
    fn matches_hashmap_under_cap() {
        // Equivalence with naive HashMap when capacity is never reached:
        // every key inserted under cap should still be present.
        let mut lru: MrrLru<i32, i32> = MrrLru::with_capacity(16);
        let mut hm: std::collections::HashMap<i32, i32> = std::collections::HashMap::new();
        for i in 0..10 {
            lru.insert(i, i * 7);
            hm.insert(i, i * 7);
        }
        for i in 0..10 {
            assert_eq!(lru.get(&i).copied(), hm.get(&i).copied(), "key {}", i);
        }
        assert_eq!(lru.len(), hm.len());
    }

    #[test]
    fn many_insertions_drop_oldest_lru_only() {
        // After 100 sequential inserts into a cap=8 cache (with no
        // intervening gets), the 8 most-recent keys are present and all
        // older keys have been evicted in FIFO order.
        let mut lru: MrrLru<i32, i32> = MrrLru::with_capacity(8);
        for i in 0..100 {
            lru.insert(i, i);
        }
        assert_eq!(lru.len(), 8);
        for i in 0..92 {
            assert_eq!(lru.get(&i), None, "stale key {} should be gone", i);
        }
        for i in 92..100 {
            assert_eq!(lru.get(&i).copied(), Some(i));
        }
    }

    #[test]
    fn churn_with_promotion_keeps_hot_keys() {
        // Simulate a workload: keys 0..3 are "hot" (touched every loop),
        // the rest are cold one-shots. The hot keys must survive.
        let mut lru: MrrLru<i32, i32> = MrrLru::with_capacity(8);
        for i in 0..4 {
            lru.insert(i, i);
        }
        for cold in 100..200 {
            // Promote each hot key first.
            for hot in 0..4 {
                let _ = lru.get(&hot);
            }
            lru.insert(cold, cold);
        }
        for hot in 0..4 {
            assert_eq!(lru.get(&hot).copied(), Some(hot), "hot key {} evicted", hot);
        }
    }
}
