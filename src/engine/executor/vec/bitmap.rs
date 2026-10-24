//! Compact bit-packed NULL / boolean bitmap.
//!
//! 64-bit-word aligned with little-endian bit order inside the word so that
//! word-level AND/OR/NOT operations directly correspond to per-row predicate
//! AND/OR/NOT. Used both as the NULL mask of a `ColumnVector` and as the
//! output of vectorized boolean expressions.

#[derive(Debug, Clone)]
pub struct Bitmap {
    // Exactly word_count(len) logical words; spare capacity is retained for
    // reuse. Bits beyond len in the last logical word must remain clear.
    words: Vec<u64>,
    len: usize,
}

impl Bitmap {
    pub(crate) fn try_clone(&self) -> Result<Self, crate::types::QueryError> {
        Ok(Self {
            words: crate::engine::executor::mem_budget::try_copy_slice(
                &self.words,
                "batch bitmap copy",
            )?,
            len: self.len,
        })
    }
    pub fn new_all_valid(len: usize) -> Self {
        let words = vec![u64::MAX; word_count(len)];
        let mut b = Self { words, len };
        b.trim_tail();
        b
    }

    pub fn new_all_invalid(len: usize) -> Self {
        Self {
            words: vec![0u64; word_count(len)],
            len,
        }
    }

    #[cfg(test)]
    pub fn with_capacity(len: usize) -> Self {
        Self::new_all_invalid(len)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Heap bytes occupied by the backing word buffer. Used by the
    /// build-side size cap so the vectorized hash join can fall back to
    /// the row-based grace path before an oversized build is fully
    /// materialised (P0/B1).
    #[inline]
    pub fn memory_bytes(&self) -> usize {
        self.words.capacity() * std::mem::size_of::<u64>()
    }

    #[inline]
    pub fn get(&self, idx: usize) -> bool {
        debug_assert!(idx < self.len);
        let (w, b) = (idx >> 6, idx & 63);
        (self.words[w] >> b) & 1 != 0
    }

    #[inline]
    pub fn set(&mut self, idx: usize, value: bool) {
        debug_assert!(idx < self.len);
        let (w, b) = (idx >> 6, idx & 63);
        let mask = 1u64 << b;
        if value {
            self.words[w] |= mask;
        } else {
            self.words[w] &= !mask;
        }
    }

    #[inline]
    pub fn set_true(&mut self, idx: usize) {
        let (w, b) = (idx >> 6, idx & 63);
        self.words[w] |= 1u64 << b;
    }

    /// Merge a comparison kernel's packed lanes into one bitmap word.
    /// The caller must leave bits beyond the logical length clear.
    #[inline]
    pub(super) fn or_word(&mut self, index: usize, bits: u64) {
        debug_assert!(index * 64 < self.len);
        debug_assert!(self.len - index * 64 >= 64 || bits >> (self.len - index * 64) == 0);
        self.words[index] |= bits;
    }

    /// Append one bit, growing the word array as needed. New words start
    /// zeroed so the tail stays clean for `count_ones`.
    #[inline]
    pub fn push(&mut self, value: bool) {
        let idx = self.len;
        self.len += 1;
        let w = idx >> 6;
        if w == self.words.len() {
            self.words.push(0);
        }
        if value {
            self.words[w] |= 1u64 << (idx & 63);
        }
    }

    /// Mark `idx` invalid in a validity lane whose unset positions default to
    /// valid, first extending the lane with valid bits when `idx` lies past
    /// its preallocated length. Accumulators size the lane for the nominal
    /// batch, but leaf-granular producers may append more rows than that.
    #[inline]
    pub fn set_null_growing(&mut self, idx: usize) {
        if idx >= self.len {
            self.resize_valid(idx + 1);
        }
        let (w, b) = (idx >> 6, idx & 63);
        self.words[w] &= !(1u64 << b);
    }

    /// Set the logical length to `len`, truncating or appending valid bits.
    pub fn resize_valid(&mut self, len: usize) {
        if len <= self.len {
            self.truncate(len);
            return;
        }
        let old = self.len;
        if old & 63 != 0 {
            self.words[old >> 6] |= u64::MAX << (old & 63);
        }
        self.len = len;
        self.words.resize(word_count(len), u64::MAX);
        self.trim_tail();
    }

    /// Shrink to `new_len` bits, dropping whole words and clearing the retained
    /// word's tail. Keeps allocation capacity. No-op when `new_len >= len`.
    pub fn truncate(&mut self, new_len: usize) {
        if new_len >= self.len {
            return;
        }
        self.len = new_len;
        let words = word_count(new_len);
        if words < self.words.len() {
            self.words.truncate(words);
        }
        self.trim_tail();
    }

    /// Count the number of `true` bits.
    pub fn count_ones(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// Element-wise AND with `other`. Panics if lengths differ.
    pub fn and(&self, other: &Self) -> Self {
        self.combine(other, |a, b| a & b)
    }

    /// Clear every bit outside `[start, start + len)` in place.
    pub fn retain_range(&mut self, start: usize, len: usize) {
        let end = start.saturating_add(len).min(self.len);
        if start >= end {
            self.words.fill(0);
            return;
        }
        let first_word = start >> 6;
        let last_word = (end - 1) >> 6;
        self.words[..first_word].fill(0);
        self.words[last_word + 1..].fill(0);
        self.words[first_word] &= u64::MAX << (start & 63);
        let tail = end & 63;
        if tail != 0 {
            self.words[last_word] &= (1u64 << tail) - 1;
        }
    }

    /// Element-wise OR with `other`. Panics if lengths differ.
    pub fn or(&self, other: &Self) -> Self {
        self.combine(other, |a, b| a | b)
    }

    #[inline]
    fn combine(&self, other: &Self, operation: impl Fn(u64, u64) -> u64) -> Self {
        assert_eq!(self.len, other.len, "bitmap length mismatch");
        let words = self
            .words
            .iter()
            .zip(other.words.iter())
            .map(|(&a, &b)| operation(a, b))
            .collect();
        let mut out = Self {
            words,
            len: self.len,
        };
        out.trim_tail();
        out
    }

    pub fn not(&self) -> Self {
        let words: Vec<u64> = self.words.iter().map(|w| !w).collect();
        let mut out = Self {
            words,
            len: self.len,
        };
        out.trim_tail();
        out
    }

    pub fn iter_true(&self) -> BitmapTrueIter<'_> {
        BitmapTrueIter {
            map: self,
            cursor: 0,
        }
    }

    /// Reset to `len` bits, all `false`, reusing the backing word buffer
    /// (no reallocation when capacity already suffices). For hot loops
    /// that need a fresh all-invalid mask per iteration — e.g. the
    /// block-major NLJ recomputes a per-inner-row match mask `|inner|`
    /// times and must not allocate each time.
    pub fn fill_false(&mut self, len: usize) {
        self.len = len;
        self.words.clear();
        self.words.resize(word_count(len), 0);
    }

    /// Reset to `len` bits, all `true`, with the tail past `len` zeroed so
    /// `count_ones` / word-level AND stay correct. Reuses the word buffer.
    /// Used to seed a conjunction mask before AND-ing in each predicate
    /// term.
    pub fn fill_true(&mut self, len: usize) {
        self.len = len;
        self.words.clear();
        self.words.resize(word_count(len), u64::MAX);
        self.trim_tail();
    }

    /// Element-wise AND into `self` (in place). Panics if lengths differ.
    /// The in-place counterpart of [`Bitmap::and`] — avoids allocating a
    /// fresh result buffer when accumulating a conjunction of predicate
    /// terms in a hot loop.
    pub fn and_assign(&mut self, other: &Self) {
        assert_eq!(self.len, other.len, "bitmap length mismatch");
        for (a, b) in self.words.iter_mut().zip(other.words.iter()) {
            *a &= *b;
        }
    }

    fn trim_tail(&mut self) {
        let tail = self.len & 63;
        if tail != 0 {
            if let Some(last) = self.words.last_mut() {
                let mask = (1u64 << tail) - 1;
                *last &= mask;
            }
        }
    }
}

#[inline]
fn word_count(bits: usize) -> usize {
    bits.div_ceil(64)
}

pub struct BitmapTrueIter<'a> {
    map: &'a Bitmap,
    cursor: usize,
}

impl<'a> Iterator for BitmapTrueIter<'a> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        while self.cursor < self.map.len {
            let i = self.cursor;
            self.cursor += 1;
            if self.map.get(i) {
                return Some(i);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_bits(bitmap: &Bitmap, expected: &[bool]) {
        assert_eq!(bitmap.len(), expected.len());
        assert_eq!(bitmap.words.len(), word_count(expected.len()));
        assert_eq!(
            bitmap.count_ones(),
            expected.iter().filter(|&&bit| bit).count()
        );
        assert_eq!(
            bitmap.iter_true().collect::<Vec<_>>(),
            expected
                .iter()
                .enumerate()
                .filter_map(|(index, &bit)| bit.then_some(index))
                .collect::<Vec<_>>()
        );
        for (index, &bit) in expected.iter().enumerate() {
            assert_eq!(bitmap.get(index), bit);
        }
    }

    #[test]
    fn truncated_bitmaps_combine_invert_and_regrow_like_logical_bits() {
        let lengths = [0, 1, 2, 63, 64, 65, 127, 128, 129, 257];
        for initial in lengths {
            let mut source = Bitmap::new_all_invalid(initial);
            for index in 0..initial {
                source.set(index, index % 3 != 1);
            }
            for keep in lengths {
                let keep = keep.min(initial);
                let mut actual = source.clone();
                actual.truncate(keep);
                let mut expected: Vec<_> = (0..keep).map(|i| i % 3 != 1).collect();
                assert_bits(&actual, &expected);
                assert_bits(
                    &actual.not(),
                    &expected.iter().map(|bit| !bit).collect::<Vec<_>>(),
                );
                let mut other = Bitmap::new_all_invalid(keep);
                for index in 0..keep {
                    other.set(index, index % 2 == 0);
                }
                let and: Vec<_> = expected
                    .iter()
                    .enumerate()
                    .map(|(i, &bit)| bit && i % 2 == 0)
                    .collect();
                let or: Vec<_> = expected
                    .iter()
                    .enumerate()
                    .map(|(i, &bit)| bit || i % 2 == 0)
                    .collect();
                assert_bits(&actual.and(&other), &and);
                assert_bits(&other.and(&actual), &and);
                assert_bits(&actual.or(&other), &or);
                assert_bits(&other.or(&actual), &or);
                let mut assigned = actual.clone();
                assigned.and_assign(&other);
                assert_bits(&assigned, &and);
                for index in 0..130 {
                    let bit = index % 5 == 0;
                    actual.push(bit);
                    expected.push(bit);
                }
                assert_bits(&actual, &expected);
                assert_bits(
                    &actual.not(),
                    &expected.iter().map(|bit| !bit).collect::<Vec<_>>(),
                );
            }
        }
    }

    #[test]
    fn word_counts_cover_extreme_lengths_without_overflow() {
        for bits in [0, 1, 63, 64, 65, usize::MAX - 63, usize::MAX] {
            assert_eq!(word_count(bits), bits / 64 + usize::from(bits % 64 != 0));
        }
    }

    #[test]
    fn truncate_not_does_not_count_dropped_storage_words() {
        let mut bitmap = Bitmap::new_all_valid(130);
        let capacity = bitmap.words.capacity();
        bitmap.truncate(3);
        assert_eq!(bitmap.iter_true().collect::<Vec<_>>(), [0, 1, 2]);
        assert_eq!(bitmap.count_ones(), 3);
        assert_eq!(bitmap.not().count_ones(), 0);
        assert_eq!(bitmap.words.len(), word_count(3));
        assert_eq!(bitmap.words.capacity(), capacity);
    }

    #[test]
    fn validity_lane_grows_past_preallocated_length() {
        for initial in [0usize, 1, 63, 64, 65, 128] {
            for extra in [1usize, 2, 63, 64, 65, 200] {
                let target = initial + extra;
                let mut lane = Bitmap::new_all_valid(initial);
                if initial > 0 {
                    lane.set_null_growing(0);
                }
                lane.set_null_growing(target - 1);
                lane.resize_valid(target + 3);
                let expected: Vec<bool> = (0..target + 3)
                    .map(|i| !((initial > 0 && i == 0) || i == target - 1))
                    .collect();
                assert_bits(&lane, &expected);
                assert_eq!(lane.words.len(), word_count(target + 3));
            }
        }
        let mut shrink = Bitmap::new_all_valid(130);
        shrink.resize_valid(5);
        assert_eq!(shrink.count_ones(), 5);
    }

    #[test]
    fn all_valid_count() {
        let b = Bitmap::new_all_valid(100);
        assert_eq!(b.count_ones(), 100);
        for i in 0..100 {
            assert!(b.get(i));
        }
    }

    #[test]
    fn set_and_get() {
        let mut b = Bitmap::new_all_invalid(200);
        b.set(0, true);
        b.set(63, true);
        b.set(64, true);
        b.set(199, true);
        assert!(b.get(0));
        assert!(b.get(63));
        assert!(b.get(64));
        assert!(b.get(199));
        assert!(!b.get(1));
        assert!(!b.get(128));
        assert_eq!(b.count_ones(), 4);
    }

    #[test]
    fn and_or_not() {
        let mut a = Bitmap::new_all_invalid(64);
        let mut c = Bitmap::new_all_invalid(64);
        a.set(0, true);
        a.set(2, true);
        c.set(0, true);
        c.set(3, true);
        let and = a.and(&c);
        assert!(and.get(0));
        assert!(!and.get(2));
        assert!(!and.get(3));
        let or = a.or(&c);
        assert!(or.get(0) && or.get(2) && or.get(3));
        let not_a = a.not();
        assert!(!not_a.get(0));
        assert!(not_a.get(1));
    }

    #[test]
    fn iter_true() {
        let mut b = Bitmap::new_all_invalid(10);
        b.set(1, true);
        b.set(5, true);
        b.set(9, true);
        let v: Vec<usize> = b.iter_true().collect();
        assert_eq!(v, vec![1, 5, 9]);
    }

    #[test]
    fn tail_trimmed_invalid_bits_zero() {
        let b = Bitmap::new_all_valid(3);
        assert_eq!(b.count_ones(), 3);
    }

    #[test]
    fn fill_false_resets_and_reuses() {
        let mut b = Bitmap::new_all_valid(130);
        b.fill_false(70);
        assert_eq!(b.len(), 70);
        assert_eq!(b.count_ones(), 0);
        b.set(5, true);
        b.fill_false(70);
        assert_eq!(b.count_ones(), 0, "fill_false must clear prior bits");
    }

    #[test]
    fn fill_true_sets_exactly_len_bits() {
        let mut b = Bitmap::new_all_invalid(10);
        b.fill_true(70);
        assert_eq!(b.len(), 70);
        assert_eq!(b.count_ones(), 70, "tail past len must stay zero");
        b.fill_true(3);
        assert_eq!(b.count_ones(), 3);
    }

    #[test]
    fn and_assign_matches_and() {
        let mut a = Bitmap::new_all_invalid(80);
        let mut c = Bitmap::new_all_invalid(80);
        for i in [0usize, 2, 64, 70, 79] {
            a.set(i, true);
        }
        for i in [0usize, 3, 64, 71, 79] {
            c.set(i, true);
        }
        let expected = a.and(&c);
        a.and_assign(&c);
        assert_eq!(a.count_ones(), expected.count_ones());
        for i in 0..80 {
            assert_eq!(a.get(i), expected.get(i));
        }
        assert!(a.get(0) && a.get(64) && a.get(79));
        assert!(!a.get(2) && !a.get(3));
    }
}
