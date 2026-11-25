//! Exact integer membership with a small write-back bitmap segment.

use crate::engine::executor::memory_footprint::hash_table_capacity_bytes;
use crate::engine::executor::memory_grant::{GrantResize, OperatorMemoryGrant};
use crate::types::{QueryError, Value};
use crate::util::fast_hash::{ah_hash_map, AhHashMap};

// A segment uses no more payload than the old inline canonical row key.
// Sparse input therefore remains bounded by ordinary hash-table accounting,
// while nearby integers share bits instead of repeating whole row keys.
type Bits = [u64; 4];

pub(super) struct IntegerKeys {
    cold: AhHashMap<i128, Bits>,
    hot: Option<i128>,
    bits: Bits,
    hot_stored: bool,
    null_seen: bool,
}

impl IntegerKeys {
    pub(super) fn new() -> Self {
        Self {
            cold: ah_hash_map(),
            hot: None,
            bits: [0; 4],
            hot_stored: false,
            null_seen: false,
        }
    }

    /// None requests the canonical spill path, including unexpected physical
    /// representations. No row is accepted until its reservation succeeds.
    #[inline]
    pub(super) fn insert(
        &mut self,
        row: &[Value],
        grant: &mut OperatorMemoryGrant,
        limit: usize,
        probes: usize,
    ) -> Result<Option<bool>, QueryError> {
        let [value] = row else {
            return Ok(None);
        };
        let number = match value {
            Value::Int(n) => Some(i128::from(*n)),
            Value::UInt(n) => Some(i128::from(*n)),
            Value::Null => None,
            _ => return Ok(None),
        };
        let block = number.map(|n| n >> 8);
        if grant.charged_bytes() == 0 || probes & 255 == 0 || block != self.hot {
            let grows = block.is_some()
                && block != self.hot
                && self.hot.is_some()
                && !self.hot_stored
                && self.cold.len() == self.cold.capacity();
            let capacity = if grows {
                self.cold.capacity().saturating_mul(2).max(8)
            } else {
                self.cold.capacity()
            };
            let bytes = hash_table_capacity_bytes::<(i128, Bits)>(capacity)
                .saturating_add(std::mem::size_of::<Self>());
            if bytes > limit {
                return Ok(None);
            }
            if bytes > grant.charged_bytes() || grant.charged_bytes() == 0 || probes & 255 == 0 {
                let target = bytes.max(grant.charged_bytes()).max(64 * 1024).min(limit);
                if matches!(grant.resize(target), GrantResize::Spill) {
                    return Ok(None);
                }
            }
            if grows {
                self.cold
                    .try_reserve(capacity - self.cold.len())
                    .map_err(|_| {
                        crate::engine::executor::mem_budget::allocation_error(
                            "recursive integer keys",
                        )
                    })?;
            }
        }
        let Some(number) = number else {
            return Ok(Some(!std::mem::replace(&mut self.null_seen, true)));
        };
        let block = number >> 8;
        if self.hot != Some(block) {
            if let Some(old) = self.hot {
                if self.hot_stored {
                    *self.cold.get_mut(&old).expect("cached integer segment") = self.bits;
                } else {
                    self.cold.insert(old, self.bits);
                }
            }
            self.hot = Some(block);
            match self.cold.get(&block) {
                Some(bits) => {
                    self.bits = *bits;
                    self.hot_stored = true;
                }
                None => {
                    self.bits = [0; 4];
                    self.hot_stored = false;
                }
            }
        }
        let bit = (number & 255) as usize;
        let word = &mut self.bits[bit >> 6];
        let mask = 1u64 << (bit & 63);
        let fresh = *word & mask == 0;
        *word |= mask;
        Ok(Some(fresh))
    }

    pub(super) fn drain(
        self,
        mut consume: impl FnMut(Value) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        fn block_values(
            block: i128,
            words: Bits,
            consume: &mut impl FnMut(Value) -> Result<(), QueryError>,
        ) -> Result<(), QueryError> {
            crate::engine::cancel::check_cancelled()?;
            for (word, mut bits) in words.into_iter().enumerate() {
                while bits != 0 {
                    let number =
                        (block << 8) + (word * 64 + bits.trailing_zeros() as usize) as i128;
                    bits &= bits - 1;
                    let value = if let Ok(n) = i64::try_from(number) {
                        Value::Int(n)
                    } else {
                        Value::UInt(u64::try_from(number).expect("stored integer domain"))
                    };
                    consume(value)?;
                }
            }
            Ok(())
        }
        for (block, bits) in self.cold {
            if Some(block) != self.hot {
                block_values(block, bits, &mut consume)?;
            }
        }
        if let Some(block) = self.hot {
            block_values(block, self.bits, &mut consume)?;
        }
        if self.null_seen {
            consume(Value::Null)?;
        }
        Ok(())
    }
}
