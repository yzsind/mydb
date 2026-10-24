//! Zero-allocation utilities for MySQL PAD SPACE collation semantics.
//!
//! In PAD SPACE collations (the historical CHAR/VARCHAR default in MySQL),
//! trailing ASCII spaces are ignored when comparing two strings. The naive
//! implementation `s.trim_end_matches(' ')` works for `&str`, but several hot
//! paths instead want a length / byte-slice view of the trimmed prefix
//! without allocating an intermediate `String`. This module provides that.

/// Length of `bytes` after stripping trailing ASCII spaces.
#[inline]
pub fn trim_trailing_spaces_len(bytes: &[u8]) -> usize {
    #[cfg(target_arch = "aarch64")]
    {
        if bytes.len() >= 32 {
            return trim_neon(bytes);
        }
    }
    trim_scalar(bytes)
}

/// Borrowed slice of `bytes` with trailing ASCII spaces removed.
#[inline]
pub fn pad_space_trim_end(bytes: &[u8]) -> &[u8] {
    &bytes[..trim_trailing_spaces_len(bytes)]
}

/// Borrowed `&str` view with trailing ASCII spaces removed. Cheaper than
/// `s.trim_end_matches(' ')` for long all-space tails on aarch64.
#[inline]
pub fn pad_space_trim_end_str(s: &str) -> &str {
    let n = trim_trailing_spaces_len(s.as_bytes());
    // SAFETY: ASCII space is a single-byte UTF-8 codepoint, so removing only
    // trailing 0x20 bytes never splits a multi-byte codepoint.
    unsafe { std::str::from_utf8_unchecked(s.as_bytes().get_unchecked(..n)) }
}

#[inline(always)]
fn trim_scalar(bytes: &[u8]) -> usize {
    let mut i = bytes.len();
    while i > 0 && bytes[i - 1] == b' ' {
        i -= 1;
    }
    i
}

#[cfg(target_arch = "aarch64")]
fn trim_neon(bytes: &[u8]) -> usize {
    use std::arch::aarch64::{
        vceqq_u8, vdupq_n_u8, vgetq_lane_u64, vld1q_u8, vreinterpretq_u64_u8,
    };

    let mut i = bytes.len();
    let space = unsafe { vdupq_n_u8(b' ') };

    // Skip whole 16-byte chunks that are entirely spaces.
    while i >= 16 {
        let chunk = unsafe { vld1q_u8(bytes.as_ptr().add(i - 16)) };
        let cmp = unsafe { vceqq_u8(chunk, space) };
        let pair = unsafe { vreinterpretq_u64_u8(cmp) };
        let lo = unsafe { vgetq_lane_u64(pair, 0) };
        let hi = unsafe { vgetq_lane_u64(pair, 1) };
        if lo != u64::MAX || hi != u64::MAX {
            // The non-space byte is somewhere in this 16-byte window.
            // Fall through to scalar tail scan over at most 16 bytes.
            break;
        }
        i -= 16;
    }

    while i > 0 && bytes[i - 1] == b' ' {
        i -= 1;
    }
    i
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trim_empty() {
        assert_eq!(pad_space_trim_end(b""), b"");
        assert_eq!(pad_space_trim_end(b"     "), b"");
    }

    #[test]
    fn trim_short() {
        assert_eq!(pad_space_trim_end(b"abc"), b"abc");
        assert_eq!(pad_space_trim_end(b"abc   "), b"abc");
        assert_eq!(pad_space_trim_end(b"a b c"), b"a b c");
        assert_eq!(pad_space_trim_end(b"a b c "), b"a b c");
    }

    #[test]
    fn trim_long_all_spaces() {
        let mut v = vec![b'x'; 5];
        v.extend(std::iter::repeat(b' ').take(200));
        assert_eq!(pad_space_trim_end(&v), b"xxxxx");
    }

    #[test]
    fn trim_long_with_internal_spaces() {
        let mut v = b"Hello World".to_vec();
        v.extend(std::iter::repeat(b' ').take(100));
        assert_eq!(pad_space_trim_end(&v), b"Hello World");
    }

    #[test]
    fn trim_str_ascii() {
        assert_eq!(pad_space_trim_end_str("hi   "), "hi");
        assert_eq!(pad_space_trim_end_str(""), "");
        assert_eq!(pad_space_trim_end_str("   "), "");
    }

    #[test]
    fn trim_chunk_boundary() {
        // Non-space at exact 16-byte boundary
        let mut v = vec![b'a'; 16];
        v.extend(std::iter::repeat(b' ').take(16));
        assert_eq!(pad_space_trim_end(&v).len(), 16);
        let mut v = vec![b'a'; 17];
        v.extend(std::iter::repeat(b' ').take(15));
        assert_eq!(pad_space_trim_end(&v).len(), 17);
    }
}
