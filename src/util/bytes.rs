//! Checked cursor reads and trusted-buffer word access shared by binary codecs.
/// Read a checked fixed-width word, advancing only after a complete read.
/// Callers retain their format-specific error codes and diagnostics.
#[inline]
pub(crate) fn read_word<const N: usize>(buf: &[u8], pos: &mut usize) -> Option<[u8; N]> {
    let end = pos.checked_add(N)?;
    let word = buf.get(*pos..end)?.try_into().ok()?;
    *pos = end;
    Some(word)
}

#[inline]
pub(crate) fn read_u16(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(buf[offset..][..2].try_into().unwrap())
}
#[inline]
pub(crate) fn read_u32(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(buf[offset..][..4].try_into().unwrap())
}
#[inline]
pub(crate) fn read_u64(buf: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(buf[offset..][..8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trusted_words_preserve_unaligned_little_endian_values() {
        let bytes: Vec<u8> = (0..64).map(|i| (i * 137 + 19) as u8).collect();
        for offset in 0..=bytes.len() - 8 {
            let expected = (0..8).fold(0u64, |word, i| {
                word | (u64::from(bytes[offset + i]) << (i * 8))
            });
            assert_eq!(read_u16(&bytes, offset), expected as u16);
            assert_eq!(read_u32(&bytes, offset), expected as u32);
            assert_eq!(read_u64(&bytes, offset), expected);
        }
        assert_eq!(read_u16(&bytes, bytes.len() - 2), 0xca41);
        assert_eq!(read_u32(&bytes, bytes.len() - 4), 0xca41b82f);
    }

    #[test]
    fn checked_words_preserve_cursor_on_truncation_and_overflow() {
        let bytes = [0x34, 0x12, 0x78, 0x56];
        let mut pos = 0;
        assert_eq!(
            read_word::<2>(&bytes, &mut pos).map(u16::from_le_bytes),
            Some(0x1234)
        );
        assert_eq!(pos, 2);
        assert_eq!(read_word::<4>(&bytes, &mut pos), None);
        assert_eq!(pos, 2);
        pos = usize::MAX;
        assert_eq!(read_word::<2>(&bytes, &mut pos), None);
        assert_eq!(pos, usize::MAX);
    }
}
