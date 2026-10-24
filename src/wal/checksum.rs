//! Prepare length-dependent CRC work before entering the WAL append mutex.
use std::sync::atomic::{AtomicU64, Ordering};

const IDENTITY: u32 = 0x8000_0000;
const POLYNOMIAL: u32 = 0xedb8_8320;
// Each slot atomically stores both its length key and shift operator. Cache
// collisions only repeat preparation; they cannot pair a length with another
// record's operator. The cache is bounded and independent of payload contents.
static SHIFTS: [AtomicU64; 64] = [const { AtomicU64::new(0) }; 64];

#[derive(Clone, Copy)]
pub(super) struct CrcShift(u32);

impl CrcShift {
    #[inline]
    pub(super) fn for_len(len: usize) -> Self {
        if len == 0 {
            return Self(IDENTITY);
        }
        let slot = &SHIFTS[(len ^ (len >> 7)) & (SHIFTS.len() - 1)];
        let cacheable = len <= u32::MAX as usize;
        if cacheable {
            let cached = slot.load(Ordering::Relaxed);
            if cached >> 32 == len as u64 {
                return Self(cached as u32);
            }
        }
        // Ask the reference implementation to shift the field identity.
        // No payload or zero-filled padding needs to be allocated or hashed.
        let mut operator = crc32fast::Hasher::new_with_initial(IDENTITY);
        operator.combine(&crc32fast::Hasher::new_with_initial_len(0, len as u64));
        let operator = operator.finalize();
        if cacheable {
            slot.store(
                ((len as u64) << 32) | u64::from(operator),
                Ordering::Relaxed,
            );
        }
        Self(operator)
    }

    #[inline]
    fn apply(self, mut value: u32) -> u32 {
        if self.0 == IDENTITY {
            return value;
        }
        // CRC-32/IEEE field multiplication in reflected representation.
        // Preparation has already combined every length bit into one operator.
        let mut operator = self.0;
        let mut result = 0;
        for _ in 0..32 {
            result ^= value & (operator >> 31).wrapping_neg();
            operator <<= 1;
            value = (value >> 1) ^ ((value & 1).wrapping_neg() & POLYNOMIAL);
        }
        result
    }
}

#[derive(Clone, Copy)]
pub(super) struct PayloadChecksum {
    value: u32,
    shift: CrcShift,
}

impl PayloadChecksum {
    #[inline]
    pub(super) fn new(bytes: &[u8]) -> Self {
        Self::from_crc(crc32fast::hash(bytes), CrcShift::for_len(bytes.len()))
    }

    #[inline]
    pub(super) fn from_crc(value: u32, shift: CrcShift) -> Self {
        Self { value, shift }
    }

    #[inline]
    pub(super) fn with_prefix(self, prefix: u32) -> u32 {
        self.shift.apply(prefix) ^ self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_checksum_matches_full_byte_crc_across_payload_sizes() {
        for len in (0..600).chain([1024, 4096, 65_536, 4 * 1024 * 1024]) {
            let bytes = (0..len).map(|i| (i * 37 + len) as u8).collect::<Vec<_>>();
            let payload = PayloadChecksum::new(&bytes);
            for seed in [0u64, 1, 0x1234_5678_abcd_ef01, u64::MAX] {
                let mut header = [0x5a; 36];
                header[8..16].copy_from_slice(&seed.to_le_bytes());
                let mut expected = crc32fast::Hasher::new();
                expected.update(&header);
                expected.update(&bytes);
                assert_eq!(
                    payload.with_prefix(crc32fast::hash(&header)),
                    expected.finalize(),
                    "len={len}, seed={seed}"
                );
            }
        }
    }

    #[test]
    fn prepared_shift_covers_length_boundaries_without_allocating_payloads() {
        for len in [
            0,
            1,
            7,
            31,
            32,
            63,
            64,
            255,
            256,
            257,
            1023,
            1024,
            65_535,
            65_536,
            u32::MAX as usize,
            usize::MAX,
        ] {
            for prefix in [0, 1, 0x8000_0000, 0x7fff_ffff, 0x1234_abcd, u32::MAX] {
                let mut expected = crc32fast::Hasher::new_with_initial(prefix);
                expected.combine(&crc32fast::Hasher::new_with_initial_len(0, len as u64));
                assert_eq!(
                    CrcShift::for_len(len).apply(prefix),
                    expected.finalize(),
                    "len={len}, prefix={prefix}"
                );
            }
        }
    }

    #[test]
    fn shift_cache_collisions_remain_correct_across_threads() {
        std::thread::scope(|scope| {
            for lane in 0..4 {
                scope.spawn(move || {
                    for i in 0..1000 {
                        let len = 1 + (i * 64 + lane * 7) % 8192;
                        let prefix = (i as u32).wrapping_mul(0x9e37_79b9);
                        let mut expected = crc32fast::Hasher::new_with_initial(prefix);
                        expected.combine(&crc32fast::Hasher::new_with_initial_len(0, len as u64));
                        assert_eq!(CrcShift::for_len(len).apply(prefix), expected.finalize());
                    }
                });
            }
        });
    }
}
