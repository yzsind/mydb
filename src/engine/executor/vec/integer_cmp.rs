//! Integer predicates produce packed masks without a read/modify/write per
//! matching row. AVX2 is selected once per batch, never in physical planning.
//! No scratch allocation: both kernels OR into the caller's existing bitmap.

use super::bitmap::Bitmap;

pub(super) const EQ: u8 = 0;
pub(super) const NE: u8 = 1;
pub(super) const LT: u8 = 2;
pub(super) const LE: u8 = 3;
pub(super) const GT: u8 = 4;
pub(super) const GE: u8 = 5;

#[inline]
fn matches<T: Ord, const OP: u8>(a: T, b: T) -> bool {
    match OP {
        EQ => a == b,
        NE => a != b,
        LT => a < b,
        LE => a <= b,
        GT => a > b,
        GE => a >= b,
        _ => unreachable!("integer comparison"),
    }
}

fn scalar<T: Copy + Ord, const OP: u8>(a: &[T], rhs: T, out: &mut Bitmap, word: usize) {
    for (index, chunk) in a.chunks(64).enumerate() {
        let mut bits = 0u64;
        for (lane, value) in chunk.iter().enumerate() {
            bits |= u64::from(matches::<T, OP>(*value, rhs)) << lane;
        }
        out.or_word(word + index, bits);
    }
}

pub(super) fn i64_cmp<const OP: u8>(a: &[i64], rhs: i64, out: &mut Bitmap) {
    assert!(out.len() >= a.len());
    #[cfg(target_arch = "x86_64")]
    if a.len() >= 64 && std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: runtime dispatch verifies AVX2; the kernel loads only full
        // groups within each 64-element slice and handles the tail scalarly.
        unsafe { avx2_i64::<OP>(a, rhs, out) };
        return;
    }
    scalar::<_, OP>(a, rhs, out, 0);
}

pub(super) fn i32_cmp<const OP: u8>(a: &[i32], rhs: i32, out: &mut Bitmap) {
    assert!(out.len() >= a.len());
    #[cfg(target_arch = "x86_64")]
    if a.len() >= 64 && std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: same runtime feature and in-bounds load checks as above.
        unsafe { avx2_i32::<OP>(a, rhs, out) };
        return;
    }
    scalar::<_, OP>(a, rhs, out, 0);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_i64<const OP: u8>(a: &[i64], rhs: i64, out: &mut Bitmap) {
    use std::arch::x86_64::*;
    let rhs = _mm256_set1_epi64x(rhs);
    let mut chunks = a.chunks_exact(64);
    for (word, chunk) in chunks.by_ref().enumerate() {
        let mut bits = 0u64;
        for lane in (0..64).step_by(4) {
            let lhs = unsafe { _mm256_loadu_si256(chunk.as_ptr().add(lane).cast()) };
            let cmp = match OP {
                EQ | NE => _mm256_cmpeq_epi64(lhs, rhs),
                LT | GE => _mm256_cmpgt_epi64(rhs, lhs),
                GT | LE => _mm256_cmpgt_epi64(lhs, rhs),
                _ => unreachable!("integer comparison"),
            };
            let mut mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp)) as u64;
            if matches!(OP, NE | LE | GE) {
                mask ^= 15;
            }
            bits |= mask << lane;
        }
        out.or_word(word, bits);
    }
    let rhs = _mm256_extract_epi64::<0>(rhs);
    scalar::<_, OP>(chunks.remainder(), rhs, out, a.len() / 64);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_i32<const OP: u8>(a: &[i32], rhs: i32, out: &mut Bitmap) {
    use std::arch::x86_64::*;
    let rhs = _mm256_set1_epi32(rhs);
    let mut chunks = a.chunks_exact(64);
    for (word, chunk) in chunks.by_ref().enumerate() {
        let mut bits = 0u64;
        for lane in (0..64).step_by(8) {
            let lhs = unsafe { _mm256_loadu_si256(chunk.as_ptr().add(lane).cast()) };
            let cmp = match OP {
                EQ | NE => _mm256_cmpeq_epi32(lhs, rhs),
                LT | GE => _mm256_cmpgt_epi32(rhs, lhs),
                GT | LE => _mm256_cmpgt_epi32(lhs, rhs),
                _ => unreachable!("integer comparison"),
            };
            let mut mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp)) as u64;
            if matches!(OP, NE | LE | GE) {
                mask ^= 255;
            }
            bits |= mask << lane;
        }
        out.or_word(word, bits);
    }
    let rhs = _mm256_extract_epi32::<0>(rhs);
    scalar::<_, OP>(chunks.remainder(), rhs, out, a.len() / 64);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check<const OP: u8>() {
        let values: Vec<i64> = (0..260)
            .map(|i| match i % 9 {
                0 => i64::MIN,
                1 => i64::MAX,
                2 => -1,
                3 => 0,
                4 => 1,
                5 => i32::MIN as i64,
                6 => i32::MAX as i64,
                _ => i * 987654321 - 10000,
            })
            .collect();
        for offset in 0..4 {
            for len in [0, 1, 3, 4, 7, 8, 31, 63, 64, 65, 127, 128, 129, 255] {
                let a = &values[offset..offset + len];
                for rhs in [i64::MIN, -1, 0, 1, i64::MAX] {
                    let mut actual = Bitmap::new_all_invalid(len + 9);
                    // Existing set bits and bits beyond the input survive.
                    actual.set_true(len + 3);
                    if len > 0 {
                        actual.set_true(0);
                    }
                    let mut expected = actual.clone();
                    scalar::<_, OP>(a, rhs, &mut expected, 0);
                    i64_cmp::<OP>(a, rhs, &mut actual);
                    assert_eq!(
                        actual.iter_true().collect::<Vec<_>>(),
                        expected.iter_true().collect::<Vec<_>>()
                    );
                    let narrow: Vec<i32> = a.iter().map(|v| *v as i32).collect();
                    let mut actual = Bitmap::new_all_invalid(len);
                    i32_cmp::<OP>(&narrow, rhs as i32, &mut actual);
                    for i in 0..len {
                        assert_eq!(actual.get(i), matches::<_, OP>(narrow[i], rhs as i32));
                    }
                }
            }
        }
    }

    #[test]
    fn integer_comparison_masks_match_scalar_at_word_and_vector_boundaries() {
        check::<EQ>();
        check::<NE>();
        check::<LT>();
        check::<LE>();
        check::<GT>();
        check::<GE>();
    }
}
