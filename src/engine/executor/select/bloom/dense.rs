use crate::engine::executor::mem_budget::{MemBudget, OwnedMemCharge};
use std::sync::Arc;

/// A streaming exact candidate alongside a complete Bloom filter. Allocation
/// failure or an excessively wide domain discards only this optional candidate.
/// Word-aligned geometric growth supports arbitrary arrival order and signed keys.
pub(super) struct DenseIntFilter {
    base: i128,
    words: Vec<u64>,
    max_words: usize,
    budget: Arc<MemBudget>,
    charge: Option<OwnedMemCharge>,
}

impl DenseIntFilter {
    pub(super) fn new(budget: Arc<MemBudget>, max_bytes: usize) -> Self {
        Self {
            base: 0,
            words: Vec::new(),
            max_words: max_bytes / 8,
            budget,
            charge: None,
        }
    }

    #[inline]
    pub(super) fn contains(&self, key: i128) -> bool {
        let Some(offset) = key
            .checked_sub(self.base)
            .and_then(|v| usize::try_from(v).ok())
        else {
            return false;
        };
        self.words
            .get(offset / 64)
            .is_some_and(|word| word & (1 << (offset % 64)) != 0)
    }

    pub(super) fn insert(&mut self, key: i128) -> Option<()> {
        let offset = key
            .checked_sub(self.base)
            .and_then(|v| usize::try_from(v).ok());
        if let Some(offset) = offset.filter(|v| v / 64 < self.words.len()) {
            self.words[offset / 64] |= 1 << (offset % 64);
            return Some(());
        }
        self.grow(key)?;
        let offset = usize::try_from(key.checked_sub(self.base)?).ok()?;
        self.words[offset / 64] |= 1 << (offset % 64);
        Some(())
    }

    #[cold]
    fn grow(&mut self, key: i128) -> Option<()> {
        let key_base = key & !63;
        let low = if self.words.is_empty() {
            key_base
        } else {
            self.base.min(key_base)
        };
        let key_words = usize::try_from(key.checked_sub(low)? / 64)
            .ok()?
            .checked_add(1)?;
        let old_offset = if self.words.is_empty() {
            0
        } else {
            usize::try_from(self.base.checked_sub(low)? / 64).ok()?
        };
        let needed = key_words.max(old_offset.checked_add(self.words.len())?);
        let new_len = needed.max(64).checked_next_power_of_two()?;
        if new_len > self.max_words {
            return None;
        }
        // Leave geometric slack in the direction of growth. Without front
        // slack a descending stream would double capacity for each new word.
        let low = if !self.words.is_empty() && key < self.base {
            self.base
                .checked_sub(((new_len - self.words.len()) * 64) as i128)
                .unwrap_or(i128::MIN)
        } else {
            low
        };
        let old_offset = if self.words.is_empty() {
            0
        } else {
            usize::try_from(self.base.checked_sub(low)? / 64).ok()?
        };
        // Reserve the new allocation before allocating; the old allocation
        // remains charged throughout copying. A failed optional grant is local.
        let charge =
            OwnedMemCharge::new_optional_pipeline(self.budget.clone(), new_len.checked_mul(8)?)
                .ok()?;
        let mut words = Vec::new();
        words.try_reserve_exact(new_len).ok()?;
        words.resize(new_len, 0);
        words[old_offset..old_offset + self.words.len()].copy_from_slice(&self.words);
        self.words = words;
        self.charge = Some(charge);
        self.base = low;
        Some(())
    }
}
