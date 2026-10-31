//! Exact, byte-bounded reuse for repeated compression inputs.

use std::num::NonZeroUsize;

use super::{ControlledMemoryCategory, ControlledMemoryCharge};

const BUDGET: usize = 2 * 1024 * 1024;
const MAX_INPUT: usize = 16 * 1024;
const FILTER_WORDS: usize = 2048;
// Covers the fixed hash table, filter, sentinels and allocator overhead.
const FIXED_BYTES: usize = 512 * 1024;
const ENTRY_BYTES: usize = 128;

pub(super) struct Memo {
    entries: lru::LruCache<u64, Entry, ahash::RandomState>,
    filter: Box<[u64]>,
    hasher: ahash::RandomState,
    misses: usize,
    bytes: usize,
    wasted: usize,
    cooldown: usize,
    _charge: ControlledMemoryCharge,
}

struct Entry {
    input: Vec<u8>,
    output: Vec<u8>,
    reused: bool,
}

impl Memo {
    pub(super) fn eligible(input: &[u8]) -> bool {
        input.len() <= MAX_INPUT
    }

    pub(super) fn try_new() -> Option<Self> {
        let charge = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::DerivedCache,
            BUDGET,
        )?;
        let hasher = ahash::RandomState::new();
        Some(Self {
            entries: lru::LruCache::with_hasher(
                NonZeroUsize::new(BUDGET / 512).unwrap(),
                hasher.clone(),
            ),
            filter: vec![0; FILTER_WORDS].into_boxed_slice(),
            hasher,
            misses: 0,
            bytes: FIXED_BYTES,
            wasted: 0,
            cooldown: 0,
            _charge: charge,
        })
    }

    fn get_hashed(&mut self, input: &[u8], hash: u64) -> Option<Vec<u8>> {
        self.entries.get_mut(&hash).and_then(|entry| {
            if entry.input != input {
                return None;
            }
            entry.reused = true;
            Some(entry.output.clone())
        })
    }

    pub(super) fn lookup(&mut self, input: &[u8]) -> (Option<Vec<u8>>, Option<u64>) {
        if self.cooldown > 0 {
            self.cooldown -= 1;
            return (None, None);
        }
        // Hash the input once. Both LRU operations and the admission filter
        // reuse this fingerprint; complete bytes still prove every hit.
        let hash = self.hasher.hash_one(input);
        if let Some(output) = self.get_hashed(input, hash) {
            return (Some(output), None);
        }
        (None, self.admit_hash(hash).then_some(hash))
    }

    fn admit_hash(&mut self, hash: u64) -> bool {
        if self.misses == FILTER_WORDS * 4 {
            self.filter.fill(0);
            self.misses = 0;
        }
        self.misses += 1;
        let mut repeated = true;
        for bit in [hash as usize, (hash >> 32) as usize] {
            let word = bit / 64 % FILTER_WORDS;
            let mask = 1 << (bit % 64);
            repeated &= self.filter[word] & mask != 0;
            self.filter[word] |= mask;
        }
        repeated
    }

    fn cost(key: &[u8], value: &[u8]) -> usize {
        ENTRY_BYTES + key.len() + value.len()
    }

    pub(super) fn insert(&mut self, input: &[u8], output: &[u8], hash: u64) {
        let cost = Self::cost(input, output);
        if cost > (BUDGET - FIXED_BYTES) / 16 {
            return;
        }
        if let Some(old) = self.entries.pop(&hash) {
            self.bytes -= Self::cost(&old.input, &old.output);
        }
        while self.bytes + cost > BUDGET || self.entries.len() == self.entries.cap().get() {
            let Some((_, value)) = self.entries.pop_lru() else {
                return;
            };
            self.bytes -= Self::cost(&value.input, &value.output);
            self.wasted = if value.reused { 0 } else { self.wasted + 1 };
            if self.wasted >= 128 {
                // A working set larger than this byte budget must not pay
                // for endless key copies and LRU churn. Retry after a bounded
                // uncached interval so a later, reusable workload can recover.
                self.entries.clear();
                self.bytes = FIXED_BYTES;
                self.filter.fill(0);
                self.misses = 0;
                self.wasted = 0;
                self.cooldown = FILTER_WORDS * 4;
                return;
            }
        }
        self.entries.put(
            hash,
            Entry {
                input: input.to_vec(),
                output: output.to_vec(),
                reused: false,
            },
        );
        self.bytes += cost;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_is_exact_and_byte_bounded_across_eviction() {
        let mut memo = Memo::try_new().unwrap();
        for i in 0u64..10000 {
            let key = i.to_le_bytes();
            assert!(memo.lookup(&key).0.is_none());
            let hash = memo.lookup(&key).1.unwrap();
            memo.insert(&key, &vec![i as u8; 1024], hash);
            assert_eq!(memo.lookup(&key).0, Some(vec![i as u8; 1024]));
            assert!(memo.bytes <= BUDGET);
        }
        assert!(memo.lookup(&0u64.to_le_bytes()).0.is_none());
        assert!(memo.lookup(&9999u64.to_le_bytes()).0.is_some());
    }

    #[test]
    fn unique_inputs_do_not_fill_the_result_cache() {
        let mut memo = Memo::try_new().unwrap();
        let admitted = (0u64..100000)
            .filter(|i| memo.lookup(&i.to_le_bytes()).1.is_some())
            .count();
        assert!(admitted < 2000, "{admitted}");
        assert!(memo.entries.is_empty());
    }

    #[test]
    fn oversized_working_sets_back_off_and_later_reuse_recovers() {
        let mut memo = Memo::try_new().unwrap();
        for _ in 0..8 {
            for i in 0u64..1000 {
                let mut key = vec![b'x'; 4096];
                key[..8].copy_from_slice(&i.to_le_bytes());
                let (_, admission) = memo.lookup(&key);
                if let Some(hash) = admission {
                    memo.insert(&key, b"encoded", hash);
                }
            }
        }
        assert!(memo.cooldown > 0);
        assert!(memo.entries.is_empty());
        for _ in 0..20000 {
            let (_, admission) = memo.lookup(b"repeated");
            if let Some(hash) = admission {
                memo.insert(b"repeated", b"encoded", hash);
            }
        }
        assert_eq!(memo.lookup(b"repeated").0, Some(b"encoded".to_vec()));
        assert!(memo.bytes <= BUDGET);
    }

    #[test]
    fn fingerprint_collisions_never_return_another_inputs_bytes() {
        let mut memo = Memo::try_new().unwrap();
        memo.insert(b"left", b"left encoded", 42);
        assert!(memo.get_hashed(b"right", 42).is_none());
        memo.insert(b"right", b"right encoded", 42);
        assert!(memo.get_hashed(b"left", 42).is_none());
        assert_eq!(
            memo.get_hashed(b"right", 42),
            Some(b"right encoded".to_vec())
        );
        assert_eq!(
            memo.bytes,
            FIXED_BYTES + Memo::cost(b"right", b"right encoded")
        );
    }
}
