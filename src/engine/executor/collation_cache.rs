//! Bounded, operator-owned reuse of canonical string comparison weights.

use super::mem_budget::{current_budget, ScopedCharge};

const TEXT_BYTES: usize = 64;
const KEY_BYTES: usize = 256;
const MAX_SLOTS: usize = 4096;

struct Entry {
    text: [u8; TEXT_BYTES],
    key: [u8; KEY_BYTES],
    text_len: u8,
    key_len: u16,
    collation: u16,
    occupied: bool,
}

pub(crate) struct CollationKeyCache {
    entries: Vec<Entry>,
    hash: ahash::RandomState,
    _charge: ScopedCharge,
}

impl CollationKeyCache {
    pub(crate) fn new() -> Self {
        // This is optional workspace, never a reason to spill or exceed a
        // statement grant. Fixed-size slots bound both text and key storage;
        // collisions replace a slot only after checking the complete input.
        let cap = current_budget().map_or(1 << 20, |budget| budget.remaining() / 128);
        let slots = (cap / std::mem::size_of::<Entry>()).min(MAX_SLOTS);
        let slots = if slots < 64 {
            0
        } else {
            1usize << (usize::BITS - 1 - slots.leading_zeros())
        };
        let mut charge = ScopedCharge::new(0);
        let mut entries = Vec::new();
        let bytes = slots * std::mem::size_of::<Entry>();
        if slots > 0 && charge.add_within(bytes, cap) {
            if entries.try_reserve_exact(slots).is_ok() {
                entries.resize_with(slots, || Entry {
                    text: [0; TEXT_BYTES],
                    key: [0; KEY_BYTES],
                    text_len: 0,
                    key_len: 0,
                    collation: 0,
                    occupied: false,
                });
            } else {
                charge.release_bytes(bytes);
            }
        }
        Self {
            entries,
            hash: ahash::RandomState::new(),
            _charge: charge,
        }
    }

    pub(crate) fn with_key<R>(
        &mut self,
        collation: u16,
        text: &str,
        consume: impl FnOnce(&[u8]) -> R,
    ) -> R {
        if self.entries.is_empty() || text.len() > TEXT_BYTES {
            return crate::charset::with_collation_key_by_id(collation, text, consume);
        }
        let index = self.hash.hash_one((collation, text)) as usize & (self.entries.len() - 1);
        let entry = &mut self.entries[index];
        if entry.occupied
            && entry.collation == collation
            && usize::from(entry.text_len) == text.len()
            && &entry.text[..text.len()] == text.as_bytes()
        {
            return consume(&entry.key[..usize::from(entry.key_len)]);
        }
        crate::charset::with_collation_key_by_id(collation, text, |key| {
            if key.len() <= KEY_BYTES {
                entry.text[..text.len()].copy_from_slice(text.as_bytes());
                entry.key[..key.len()].copy_from_slice(key);
                entry.text_len = text.len() as u8;
                entry.key_len = key.len() as u16;
                entry.collation = collation;
                entry.occupied = true;
            }
            consume(key)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_weights_preserve_domains_unicode_and_eviction() {
        let mut cache = CollationKeyCache::new();
        // Force collisions while retaining the admitted allocation.
        cache.entries.truncate(1);
        for _ in 0..3 {
            for id in [45, 46, 255, crate::charset::BINARY_COLLATION_ID] {
                for text in [
                    "",
                    "a",
                    "A",
                    "a ",
                    "é",
                    "e\u{301}",
                    "Straße",
                    "a\0b",
                    &"界".repeat(90),
                ] {
                    let expected =
                        crate::charset::with_collation_key_by_id(id, text, |key| key.to_vec());
                    for _ in 0..2 {
                        assert_eq!(cache.with_key(id, text, |key| key.to_vec()), expected);
                    }
                }
            }
        }
    }

    #[test]
    fn optional_weights_disable_under_tight_grant_and_release_memory() {
        use std::sync::Arc;
        for limit in [4096, 64 << 20] {
            let budget = Arc::new(super::super::mem_budget::MemBudget::new(limit));
            super::super::mem_budget::with_query_budget(budget.clone(), || {
                let cache = CollationKeyCache::new();
                assert_eq!(cache.entries.is_empty(), limit == 4096);
                assert!(budget.used() <= limit / 128);
                drop(cache);
                assert_eq!(budget.used(), 0);
            });
        }
    }
}
