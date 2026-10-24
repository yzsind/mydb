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
    u16::from_le_bytes([buf[offset], buf[offset + 1]])
}
#[inline]
pub(crate) fn read_u32(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ])
}
#[inline]
pub(crate) fn read_u64(buf: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
        buf[offset + 4],
        buf[offset + 5],
        buf[offset + 6],
        buf[offset + 7],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

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
