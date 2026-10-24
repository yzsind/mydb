use std::hash::{Hash, Hasher};

/// Exact membership state for a single integer-shaped runtime join key.
///
/// Dense domains use a bitmap; sparse domains retain a sorted key array and
/// binary-search it. The builder reserves query memory before allocating and
/// the reservation follows this object until the last `Arc` owner drops it.
pub struct ExactIntKeySet {
    storage: ExactIntKeyStorage,
    _charge: crate::engine::executor::mem_budget::ScopedCharge,
}

enum ExactIntKeyStorage {
    Range { min: i128, max: i128 },
    Dense { min: i128, words: Box<[u64]> },
    Sorted(Box<[i128]>),
}

impl std::fmt::Debug for ExactIntKeySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.storage {
            ExactIntKeyStorage::Range { min, max } => f
                .debug_struct("ExactIntKeySet::Range")
                .field("min", min)
                .field("max", max)
                .finish(),
            ExactIntKeyStorage::Dense { min, words } => f
                .debug_struct("ExactIntKeySet::Dense")
                .field("min", min)
                .field("bytes", &(words.len() * std::mem::size_of::<u64>()))
                .finish(),
            ExactIntKeyStorage::Sorted(keys) => f
                .debug_struct("ExactIntKeySet::Sorted")
                .field("keys", &keys.len())
                .finish(),
        }
    }
}

impl ExactIntKeySet {
    pub(crate) fn bounds(&self) -> Option<(i128, i128)> {
        match &self.storage {
            ExactIntKeyStorage::Range { min, max } => Some((*min, *max)),
            ExactIntKeyStorage::Sorted(keys) => Some((*keys.first()?, *keys.last()?)),
            ExactIntKeyStorage::Dense { min, words } => {
                let first = words.iter().position(|word| *word != 0)?;
                let last = words.iter().rposition(|word| *word != 0)?;
                let lo = first
                    .checked_mul(64)?
                    .checked_add(words[first].trailing_zeros() as usize)?;
                let hi = last
                    .checked_mul(64)?
                    .checked_add(63 - words[last].leading_zeros() as usize)?;
                Some((min.checked_add(lo as i128)?, min.checked_add(hi as i128)?))
            }
        }
    }
    #[inline]
    pub(crate) fn contains(&self, key: i128) -> bool {
        match &self.storage {
            ExactIntKeyStorage::Range { min, max } => key >= *min && key <= *max,
            ExactIntKeyStorage::Dense { min, words } => key
                .checked_sub(*min)
                .and_then(|offset| usize::try_from(offset).ok())
                .and_then(|offset| {
                    words
                        .get(offset >> 6)
                        .map(|word| word & (1u64 << (offset & 63)))
                })
                .is_some_and(|bit| bit != 0),
            ExactIntKeyStorage::Sorted(keys) => keys.binary_search(&key).is_ok(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match &self.storage {
            ExactIntKeyStorage::Range { min, max } => (max - min + 1) as usize,
            ExactIntKeyStorage::Dense { words, .. } => {
                words.iter().map(|word| word.count_ones() as usize).sum()
            }
            ExactIntKeyStorage::Sorted(keys) => keys.len(),
        }
    }

    /// Visit every exact key once without materialising a second key array.
    /// Dense filters reconstruct keys from set bits; sparse filters lend their
    /// already-sorted storage. Callers use this to turn an exact join runtime
    /// filter into bounded B-tree probes.
    pub(crate) fn for_each_key(&self, mut visit: impl FnMut(i128)) {
        match &self.storage {
            ExactIntKeyStorage::Range { min, .. } => {
                for offset in 0..self.len() {
                    visit(*min + offset as i128);
                }
            }
            ExactIntKeyStorage::Dense { min, words } => {
                for (word_index, &word) in words.iter().enumerate() {
                    let mut remaining = word;
                    while remaining != 0 {
                        let bit = remaining.trailing_zeros() as usize;
                        let offset = word_index.saturating_mul(64).saturating_add(bit);
                        visit(min.saturating_add(offset as i128));
                        remaining &= remaining - 1;
                    }
                }
            }
            ExactIntKeyStorage::Sorted(keys) => {
                for &key in keys.iter() {
                    visit(key);
                }
            }
        }
    }
}

pub(crate) struct ExactIntKeySetBuilder {
    keys: Vec<i128>,
    charge: crate::engine::executor::mem_budget::ScopedCharge,
}

impl ExactIntKeySetBuilder {
    pub(crate) fn with_capacity(capacity: usize) -> Result<Self, crate::types::QueryError> {
        let requested = capacity.saturating_mul(std::mem::size_of::<i128>());
        let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        charge.add(requested)?;
        let mut keys = Vec::new();
        crate::engine::executor::mem_budget::try_reserve_vec_exact(
            &mut keys,
            capacity,
            "exact integer runtime filter",
        )?;
        let actual = keys.capacity().saturating_mul(std::mem::size_of::<i128>());
        charge.add(actual.saturating_sub(requested))?;
        Ok(Self { keys, charge })
    }

    #[inline]
    pub(crate) fn insert(&mut self, key: i128) {
        self.keys.push(key);
    }

    pub(crate) fn finish(mut self) -> Result<ExactIntKeySet, crate::types::QueryError> {
        self.keys.sort_unstable();
        self.keys.dedup();
        let Some((&min, &max)) = self.keys.first().zip(self.keys.last()) else {
            return Ok(self.finish_sorted());
        };
        let span = max
            .checked_sub(min)
            .and_then(|span| span.checked_add(1))
            .and_then(|span| u128::try_from(span).ok());
        if span == Some(self.keys.len() as u128) {
            // The sorted, deduplicated keys cover every integer in the range.
            // Keep exact membership without an allocation or per-probe bitmap
            // load. Drop storage before returning its budget reservation.
            drop(self.keys);
            self.charge.release_bytes(self.charge.charged());
            return Ok(ExactIntKeySet {
                storage: ExactIntKeyStorage::Range { min, max },
                _charge: self.charge,
            });
        }
        let dense_bytes = span.map(|span| span.div_ceil(8));
        let sorted_bytes =
            (self.keys.capacity() as u128).saturating_mul(std::mem::size_of::<i128>() as u128);
        if let (Some(span), Some(dense_bytes)) = (span, dense_bytes) {
            if dense_bytes <= sorted_bytes && dense_bytes <= 64 * 1024 * 1024 {
                let words_len = usize::try_from(span.div_ceil(64)).map_err(|_| {
                    crate::engine::executor::mem_budget::allocation_error(
                        "exact integer runtime filter domain is too large",
                    )
                })?;
                let requested = words_len.saturating_mul(std::mem::size_of::<u64>());
                self.charge.add(requested)?;
                let mut words = Vec::new();
                crate::engine::executor::mem_budget::try_reserve_vec_exact(
                    &mut words,
                    words_len,
                    "exact integer runtime filter bitmap",
                )?;
                let actual = words.capacity().saturating_mul(std::mem::size_of::<u64>());
                self.charge.add(actual.saturating_sub(requested))?;
                words.resize(words_len, 0);
                for key in self.keys {
                    let offset = usize::try_from(key - min).expect("bounded dense key offset");
                    words[offset >> 6] |= 1u64 << (offset & 63);
                }
                let words = words.into_boxed_slice();
                let retained = words.len() * std::mem::size_of::<u64>();
                self.charge
                    .release_bytes(self.charge.charged().saturating_sub(retained));
                return Ok(ExactIntKeySet {
                    storage: ExactIntKeyStorage::Dense { min, words },
                    _charge: self.charge,
                });
            }
        }
        Ok(self.finish_sorted())
    }

    fn finish_sorted(mut self) -> ExactIntKeySet {
        let keys = self.keys.into_boxed_slice();
        let retained = keys.len() * std::mem::size_of::<i128>();
        self.charge
            .release_bytes(self.charge.charged().saturating_sub(retained));
        ExactIntKeySet {
            storage: ExactIntKeyStorage::Sorted(keys),
            _charge: self.charge,
        }
    }
}

/// A simple Bloom filter for early skipping on the hash join probe side, outer-key
/// pre-filtering on the INLJ probe side, and similar scenarios.
///
/// - Size fixed at ≥10 bits/key by expected key count, k=4, theoretical FPR ≈ 1.2%
/// - Deterministic chunked hashing scans the key once, then avalanche mix derives double hashing
/// - Repeatedly inserting the same key set is idempotent; contains always returns false when nothing was inserted
/// - The interface is generic over `Hash`, compatible with the historical `&[ValueKey]`
///   while also taking `&IndexKey` or byte slices (`&[u8]`) directly, avoiding extra type conversions on INLJ paths
#[derive(Clone)]
pub struct BloomFilter {
    bits: Vec<u64>,
    bit_count: u64,
    k: u32,
    legacy_double_hash: bool,
    // 0 = generic Hash key, 1 = one canonical integer, 2 = an ordered pair
    // of canonical integers. Typed modes let fact scans avoid ValueKey slice
    // construction and generic Hash framing on every probe.
    int_key_arity: u8,
    // Optional filters must keep their governor reservation until the last
    // runtime-filter clone releases the bitmap.  Most existing callers charge
    // their containing operator and therefore leave this empty.
    _charge: Option<std::sync::Arc<crate::engine::executor::mem_budget::OwnedMemCharge>>,
}

impl std::fmt::Debug for BloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomFilter")
            .field(
                "bytes",
                &(self.bits.capacity() * std::mem::size_of::<u64>()),
            )
            .field("bit_count", &self.bit_count)
            .field("k", &self.k)
            .finish()
    }
}

impl BloomFilter {
    /// Bitmap layout: `>=10 bits/key` (floor 64 bits), rounded up to a power of two.
    ///
    /// Bloom probes sit in scan hot loops; a power-of-two layout lets every bit probe
    /// use a mask instead of a 64-bit integer modulo. Rounding up only lowers the FPR,
    /// and the memory increase per filter stays strictly below 2x.
    /// `new` and `estimated_byte_size` share it, keeping "estimated" and "actual" consistent.
    #[inline]
    fn bit_layout(expected_entries: usize) -> (u64, usize) {
        let n = expected_entries.max(1);
        let minimum_bits = (n as u64).saturating_mul(10).max(64);
        if matches!(
            crate::util::env::var("MYDB_BLOOM_POWER_OF_TWO_DISABLE")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "1" | "on" | "true" | "yes"
        ) {
            return (minimum_bits, minimum_bits.div_ceil(64) as usize);
        }
        let bit_count = minimum_bits
            .checked_next_power_of_two()
            .unwrap_or(1u64 << 63);
        let words = (bit_count / 64) as usize;
        (bit_count, words)
    }

    pub(crate) fn new(expected_entries: usize) -> Self {
        let (bit_count, words) = Self::bit_layout(expected_entries);
        Self {
            bits: vec![0u64; words],
            bit_count,
            // Four probes are the established accuracy/CPU baseline.
            k: 4,
            legacy_double_hash: matches!(
                crate::util::env::var("MYDB_BLOOM_SINGLE_HASH_DISABLE")
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .as_str(),
                "1" | "on" | "true" | "yes"
            ),
            int_key_arity: 0,
            _charge: None,
        }
    }

    /// Build a filter whose keys are one canonical integer component. Insert
    /// and probe then hash the i128 lane directly, avoiding ValueKey/slice
    /// hashing in fact-table scans. This mode is explicit so generic filters
    /// retain their established byte representation.
    pub(crate) fn new_int(expected_entries: usize) -> Self {
        let mut filter = Self::new(expected_entries);
        filter.int_key_arity = 1;
        filter
    }

    /// Build a filter over an ordered pair of canonical integer components.
    /// This is the common composite-FK runtime-filter shape.
    #[cfg(test)]
    pub(crate) fn new_int_pair(expected_entries: usize) -> Self {
        let mut filter = Self::new(expected_entries);
        filter.int_key_arity = 2;
        filter
    }

    pub(crate) fn uses_int_keys(&self) -> bool {
        self.int_key_arity == 1
    }

    pub(crate) fn uses_int_pair_keys(&self) -> bool {
        self.int_key_arity == 2
    }

    pub(crate) fn attach_charge(
        &mut self,
        charge: std::sync::Arc<crate::engine::executor::mem_budget::OwnedMemCharge>,
    ) {
        self._charge = Some(charge);
    }

    #[cfg(test)]
    /// Current bitmap size in bytes, to expose bloom memory usage via EXPLAIN / metrics.
    pub(crate) fn byte_size(&self) -> usize {
        self.bits.len() * 8
    }

    /// Heap bytes a bloom built for `expected_entries` keys will occupy — the static
    /// counterpart of `byte_size`, so hash join memory accounting (P0/B4) can reserve without actually building.
    pub(crate) fn estimated_byte_size(expected_entries: usize) -> usize {
        Self::bit_layout(expected_entries).1 * 8
    }

    pub(crate) fn insert<K: Hash + ?Sized>(&mut self, key: &K) {
        let (h1, h2) = hash_pair(key, self.legacy_double_hash);
        let mask = self.bit_count - 1;
        let power_of_two = self.bit_count.is_power_of_two();
        let bits = self.bits.as_mut_slice();
        let mut probe = h1;
        for _ in 0..self.k {
            let idx = if power_of_two {
                probe & mask
            } else {
                probe % self.bit_count
            };
            // SAFETY: idx < bit_count = bits.len() * 64.
            unsafe {
                let word = bits.get_unchecked_mut((idx / 64) as usize);
                *word |= 1u64 << (idx % 64);
            }
            probe = probe.wrapping_add(h2);
        }
    }

    pub(crate) fn contains<K: Hash + ?Sized>(&self, key: &K) -> bool {
        let (h1, h2) = hash_pair(key, self.legacy_double_hash);
        let mask = self.bit_count - 1;
        let power_of_two = self.bit_count.is_power_of_two();
        let bits = self.bits.as_slice();
        let mut probe = h1;
        for _ in 0..self.k {
            let idx = if power_of_two {
                probe & mask
            } else {
                probe % self.bit_count
            };
            unsafe {
                let word = bits.get_unchecked((idx / 64) as usize);
                if *word & (1u64 << (idx % 64)) == 0 {
                    return false;
                }
            }
            probe = probe.wrapping_add(h2);
        }
        true
    }

    /// Probe the same key representation used at construction, including
    /// typed filters pushed from a vector join into a row/Value adapter.
    /// A dynamic lane mismatch is inconclusive: only the parent join may
    /// reject it, so optional filtering must conservatively admit the row.
    pub(crate) fn contains_value_keys(&self, keys: &[crate::engine::expr::ValueKey]) -> bool {
        use crate::engine::expr::ValueKey;
        match (self.int_key_arity, keys) {
            (1, [ValueKey::Int(key)]) => self.contains_int(*key),
            (2, [ValueKey::Int(first), ValueKey::Int(second)]) => {
                self.contains_int_pair(*first, *second)
            }
            (0, keys) => self.contains(keys),
            _ => true,
        }
    }

    pub(crate) fn insert_int(&mut self, key: i128) {
        debug_assert!(self.uses_int_keys());
        let (h1, h2) = hash_pair_int(key, self.legacy_double_hash);
        self.insert_hash_pair(h1, h2);
    }

    pub(crate) fn contains_int(&self, key: i128) -> bool {
        debug_assert!(self.uses_int_keys());
        let (h1, h2) = hash_pair_int(key, self.legacy_double_hash);
        self.contains_hash_pair(h1, h2)
    }

    #[cfg(test)]
    pub(crate) fn insert_int_pair(&mut self, first: i128, second: i128) {
        debug_assert!(self.uses_int_pair_keys());
        let (h1, h2) = hash_pair_int_pair(first, second, self.legacy_double_hash);
        self.insert_hash_pair(h1, h2);
    }

    pub(crate) fn contains_int_pair(&self, first: i128, second: i128) -> bool {
        debug_assert!(self.uses_int_pair_keys());
        let (h1, h2) = hash_pair_int_pair(first, second, self.legacy_double_hash);
        self.contains_hash_pair(h1, h2)
    }

    #[inline]
    fn insert_hash_pair(&mut self, h1: u64, h2: u64) {
        let mask = self.bit_count - 1;
        let power_of_two = self.bit_count.is_power_of_two();
        let bits = self.bits.as_mut_slice();
        let mut probe = h1;
        for _ in 0..self.k {
            let idx = if power_of_two {
                probe & mask
            } else {
                probe % self.bit_count
            };
            unsafe {
                *bits.get_unchecked_mut((idx / 64) as usize) |= 1u64 << (idx % 64);
            }
            probe = probe.wrapping_add(h2);
        }
    }

    #[inline]
    fn contains_hash_pair(&self, h1: u64, h2: u64) -> bool {
        let mask = self.bit_count - 1;
        let power_of_two = self.bit_count.is_power_of_two();
        let bits = self.bits.as_slice();
        let mut probe = h1;
        for _ in 0..self.k {
            let idx = if power_of_two {
                probe & mask
            } else {
                probe % self.bit_count
            };
            unsafe {
                if *bits.get_unchecked((idx / 64) as usize) & (1u64 << (idx % 64)) == 0 {
                    return false;
                }
            }
            probe = probe.wrapping_add(h2);
        }
        true
    }
}

#[inline]
fn hash_pair_int(key: i128, legacy_double_hash: bool) -> (u64, u64) {
    let mut first = SimpleHasher::new(0xc6a4_a793_5bd1_e995, legacy_double_hash);
    first.write_i128(key);
    let h1 = first.finish();
    if !legacy_double_hash {
        return (h1, mix64(h1 ^ 0x52c6_a2b8_7783_2f7d) | 1);
    }
    let mut second = SimpleHasher::new(0x52c6_a2b8_7783_2f7d, true);
    second.write_i128(key);
    let h2 = second.finish();
    (h1, if h2 == 0 { 1 } else { h2 })
}

#[inline]
fn hash_pair_int_pair(first_key: i128, second_key: i128, legacy_double_hash: bool) -> (u64, u64) {
    let hash = |seed, legacy| {
        let mut state = SimpleHasher::new(seed, legacy);
        // Frame the arity so this representation remains distinct if typed
        // modes are ever combined in one bitmap.
        state.write_u8(2);
        state.write_i128(first_key);
        state.write_i128(second_key);
        state.finish()
    };
    let h1 = hash(0xc6a4_a793_5bd1_e995, legacy_double_hash);
    if !legacy_double_hash {
        return (h1, mix64(h1 ^ 0x52c6_a2b8_7783_2f7d) | 1);
    }
    let h2 = hash(0x52c6_a2b8_7783_2f7d, true);
    (h1, if h2 == 0 { 1 } else { h2 })
}

/// Bloom double hashing only needs two well-distributed deterministic values;
/// it does not require walking the key twice. Hash the key once, then use a
/// SplitMix64 finalizer to derive an independent odd step. The odd step also
/// prevents short cycles with the power-of-two bitmap layout.
#[inline]
fn hash_pair<K: Hash + ?Sized>(key: &K, legacy_double_hash: bool) -> (u64, u64) {
    let mut s1 = SimpleHasher::new(0xc6a4_a793_5bd1_e995, legacy_double_hash);
    key.hash(&mut s1);
    let h1 = s1.finish();
    if !legacy_double_hash {
        return (h1, mix64(h1 ^ 0x52c6_a2b8_7783_2f7d) | 1);
    }
    let mut s2 = SimpleHasher::new(0x52c6_a2b8_7783_2f7d, true);
    key.hash(&mut s2);
    let h2_raw = s2.finish();
    (h1, if h2_raw == 0 { 1 } else { h2_raw })
}

#[inline]
fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// A minimal, deterministic 64-bit Hasher, for stable bloom bits across processes/threads.
/// std `DefaultHasher` seeds randomly per process, unsuitable for cross-process spill; a fixed seed is used here.
struct SimpleHasher {
    state: u64,
    legacy_bytewise: bool,
}

impl SimpleHasher {
    fn new(seed: u64, legacy_bytewise: bool) -> Self {
        Self {
            state: seed,
            legacy_bytewise,
        }
    }
}

impl SimpleHasher {
    /// Absorb a word that is already an integer directly, skipping the whole
    /// "bytes → length mix → chunks_exact loop → tail padding" pipeline.
    ///
    /// `Hasher`'s default integer implementations all forward to `write(&n.to_ne_bytes())`, so
    /// `[ValueKey::Int(_)]`, the most common bloom key, takes 3 `write` calls (slice length
    /// prefix + enum discriminant + i128 payload), ~7 mix64 rounds and two slice iterations.
    /// Runtime filters run per row on fact-table scans (~6.8M probes per TPC-H Q9
    /// query), so this path's constants show up directly in scan wall time.
    ///
    /// The bit pattern differs from the old implementation, but blooms live only in process
    /// memory (never spilled, never cross-version), and insert and contains share this function, so self-consistency is unaffected.
    #[inline]
    fn absorb_word(&mut self, word: u64) {
        self.state = mix64(self.state ^ word);
    }
}

macro_rules! simple_hasher_write_int {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(
            #[inline]
            fn $method(&mut self, value: $ty) {
                if self.legacy_bytewise {
                    self.write(&value.to_ne_bytes());
                } else {
                    self.absorb_word(value as u64);
                }
            }
        )*
    };
}

impl Hasher for SimpleHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    simple_hasher_write_int!(
        write_u8(u8),
        write_u16(u16),
        write_u32(u32),
        write_u64(u64),
        write_usize(usize),
        write_i8(i8),
        write_i16(i16),
        write_i32(i32),
        write_i64(i64),
        write_isize(isize),
    );

    #[inline]
    fn write_u128(&mut self, value: u128) {
        if self.legacy_bytewise {
            self.write(&value.to_ne_bytes());
        } else {
            self.absorb_word(value as u64);
            self.absorb_word((value >> 64) as u64);
        }
    }

    #[inline]
    fn write_i128(&mut self, value: i128) {
        self.write_u128(value as u128);
    }

    fn write(&mut self, bytes: &[u8]) {
        if !self.legacy_bytewise {
            // `Hash` feeds integers (including ValueKey::Int's i128) as
            // native byte slices. Absorb eight bytes at a time instead of
            // paying one xor + multiply per byte. Include the write length
            // and tail length so successive Hash::write calls stay framed.
            self.state ^= mix64(bytes.len() as u64 ^ 0xa076_1d64_78bd_642f);
            let mut chunks = bytes.chunks_exact(8);
            for chunk in &mut chunks {
                let word = u64::from_le_bytes(chunk.try_into().expect("exact 8-byte chunk"));
                self.state = mix64(self.state ^ word);
            }
            let tail = chunks.remainder();
            if !tail.is_empty() {
                let mut word = [0u8; 8];
                word[..tail.len()].copy_from_slice(tail);
                self.state =
                    mix64(self.state ^ u64::from_le_bytes(word) ^ ((tail.len() as u64) << 56));
            }
            return;
        }
        for b in bytes {
            self.state ^= *b as u64;
            self.state = self.state.wrapping_mul(0x100_0000_01b3);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::expr::ValueKey;
    use crate::storage::schema::{IndexKey, KeyPart};

    #[test]
    fn contiguous_exact_filter_handles_signed_edges_and_duplicates() {
        for min in [i128::MIN, -3, 0, i128::MAX - 6] {
            let expected = (0..7).map(|offset| min + offset).collect::<Vec<_>>();
            let mut builder = ExactIntKeySetBuilder::with_capacity(14).unwrap();
            for &key in expected.iter().rev().chain(expected.iter()) {
                builder.insert(key);
            }
            let filter = builder.finish().unwrap();
            assert!(matches!(filter.storage, ExactIntKeyStorage::Range { .. }));
            assert_eq!(filter._charge.charged(), 0);
            assert_eq!(filter.len(), expected.len());
            assert_eq!(
                filter.bounds(),
                Some((*expected.first().unwrap(), *expected.last().unwrap()))
            );
            let mut actual = Vec::new();
            filter.for_each_key(|key| actual.push(key));
            assert_eq!(actual, expected);
            for key in expected {
                assert!(filter.contains(key));
            }
            if let Some(key) = min.checked_sub(1) {
                assert!(!filter.contains(key));
            }
            if let Some(key) = min.checked_add(7) {
                assert!(!filter.contains(key));
            }
        }
    }

    #[test]
    fn exact_int_filter_retains_only_live_storage_reservation() {
        for keys in [vec![1, 3, 5], vec![i128::MIN, i128::MAX], Vec::new()] {
            let mut builder = ExactIntKeySetBuilder::with_capacity(128).unwrap();
            for &key in &keys {
                builder.insert(key);
            }
            let filter = builder.finish().unwrap();
            let allocated = match &filter.storage {
                ExactIntKeyStorage::Range { .. } => 0,
                ExactIntKeyStorage::Dense { words, .. } => words.len() * 8,
                ExactIntKeyStorage::Sorted(keys) => keys.len() * 16,
            };
            assert_eq!(filter._charge.charged(), allocated);
        }
    }

    #[test]
    fn exact_int_key_set_uses_dense_domain_without_false_results() {
        let mut builder = ExactIntKeySetBuilder::with_capacity(8).unwrap();
        for key in [-3, -1, 0, 1, 1, 4, 7, 9] {
            builder.insert(key);
        }
        let filter = builder.finish().unwrap();
        assert_eq!(filter.len(), 7);
        assert_eq!(filter.bounds(), Some((-3, 9)));
        for key in [-3, -1, 0, 1, 4, 7, 9] {
            assert!(filter.contains(key));
        }
        for key in [-4, -2, 2, 8, 10] {
            assert!(!filter.contains(key));
        }
        let mut visited = Vec::new();
        filter.for_each_key(|key| visited.push(key));
        assert_eq!(visited, vec![-3, -1, 0, 1, 4, 7, 9]);
        assert!(matches!(filter.storage, ExactIntKeyStorage::Dense { .. }));
    }

    #[test]
    fn exact_int_key_set_uses_sorted_storage_for_sparse_i128_domain() {
        let keys = [
            i128::MIN + 1,
            -9,
            0,
            1_u128.wrapping_shl(100) as i128,
            i128::MAX,
        ];
        let mut builder = ExactIntKeySetBuilder::with_capacity(keys.len()).unwrap();
        for key in keys {
            builder.insert(key);
        }
        let filter = builder.finish().unwrap();
        assert_eq!(filter.len(), keys.len());
        assert_eq!(filter.bounds(), Some((i128::MIN + 1, i128::MAX)));
        for key in keys {
            assert!(filter.contains(key));
        }
        assert!(!filter.contains(-8));
        assert!(!filter.contains(i128::MAX - 1));
        let mut visited = Vec::new();
        filter.for_each_key(|key| visited.push(key));
        assert_eq!(visited, keys);
        assert!(matches!(filter.storage, ExactIntKeyStorage::Sorted(_)));
    }

    #[test]
    fn bloom_no_false_negatives() {
        let mut bf = BloomFilter::new(1024);
        let keys: Vec<Vec<ValueKey>> = (0..512).map(|i| vec![ValueKey::Int(i as i128)]).collect();
        for k in &keys {
            bf.insert(k.as_slice());
        }
        for k in &keys {
            assert!(
                bf.contains(k.as_slice()),
                "bloom should not produce false negatives"
            );
        }
    }

    #[test]
    fn integer_bloom_direct_lane_has_no_false_negatives() {
        let mut filter = BloomFilter::new_int(4_096);
        for key in -2_048..2_048 {
            filter.insert_int(key);
        }
        for key in -2_048..2_048 {
            assert!(filter.contains_int(key));
        }
        let rejected = (20_000..24_096)
            .filter(|key| !filter.contains_int(*key))
            .count();
        assert!(rejected >= 3_900, "rejected {rejected} of 4096 misses");
    }

    #[test]
    fn integer_pair_bloom_direct_lane_has_no_false_negatives() {
        let mut filter = BloomFilter::new_int_pair(4_096);
        for first in 0..64 {
            for second in 0..64 {
                filter.insert_int_pair(first, second * 17);
            }
        }
        for first in 0..64 {
            for second in 0..64 {
                assert!(filter.contains_int_pair(first, second * 17));
            }
        }
        let rejected = (10_000..14_096)
            .filter(|key| !filter.contains_int_pair(*key, -*key))
            .count();
        assert!(rejected >= 3_900, "rejected {rejected} of 4096 misses");
    }

    #[test]
    fn bloom_layout_is_power_of_two_and_covers_target_density() {
        for entries in [0usize, 1, 7, 1_000, 10_664, 1_000_000] {
            let (bits, words) = BloomFilter::bit_layout(entries);
            assert!(bits.is_power_of_two());
            assert_eq!(bits, words as u64 * 64);
            let target = (entries.max(1) as u64).saturating_mul(10).max(64);
            assert!(bits >= target);
        }
    }

    #[test]
    fn bloom_no_false_negatives_composite() {
        let mut bf = BloomFilter::new(50);
        let keys: Vec<Vec<ValueKey>> = (0..50)
            .map(|i| {
                vec![
                    ValueKey::Int(i as i128),
                    ValueKey::string_from_str(&format!("v{i}")),
                ]
            })
            .collect();
        for k in &keys {
            bf.insert(k.as_slice());
        }
        for k in &keys {
            assert!(
                bf.contains(k.as_slice()),
                "bloom should not produce false negatives for composite keys"
            );
        }
    }

    #[test]
    fn bloom_rejects_majority_of_misses() {
        let mut bf = BloomFilter::new(1000);
        for i in 0..1000 {
            bf.insert([ValueKey::Int(i as i128)].as_slice());
        }
        let misses = (10_000..11_000)
            .filter(|i| !bf.contains([ValueKey::Int(*i as i128)].as_slice()))
            .count();
        // At 1% FPR, at least 90% of misses should be rejected.
        assert!(misses >= 900, "rejected {} of 1000 misses", misses);
    }

    #[test]
    fn bloom_accepts_index_key_directly() {
        // IndexKey implements Hash, so the generic interface accepts it directly,
        // avoiding redundant IndexKey-to-ValueKey conversion in INLJ.
        let mut bf = BloomFilter::new(128);
        let keys: Vec<IndexKey> = (0..128)
            .map(|i| IndexKey::from(vec![KeyPart::Int(i as i64)]))
            .collect();
        for k in &keys {
            bf.insert(k);
        }
        for k in &keys {
            assert!(
                bf.contains(k),
                "bloom should accept IndexKey via generic Hash"
            );
        }
        // Most misses should be rejected.
        let misses = (1_000..2_000)
            .filter(|i| !bf.contains(&IndexKey::from(vec![KeyPart::Int(*i as i64)])))
            .count();
        assert!(misses >= 900, "rejected {} of 1000 IndexKey misses", misses);
    }
}
