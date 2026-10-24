//! Hex byte conversion shared by SQL rendering, authentication and storage.

#[inline]
pub(crate) fn encode_upper(bytes: &[u8]) -> String {
    encode::<true>(bytes)
}

#[inline]
pub(crate) fn encode_lower(bytes: &[u8]) -> String {
    encode::<false>(bytes)
}

#[inline]
pub(crate) fn append_upper(out: &mut String, bytes: &[u8]) {
    append::<true>(out, bytes);
}

#[inline]
pub(crate) fn append_lower(out: &mut String, bytes: &[u8]) {
    append::<false>(out, bytes);
}

#[inline]
fn encode<const UPPER: bool>(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    append::<UPPER>(&mut out, bytes);
    out
}

#[inline]
fn append<const UPPER: bool>(out: &mut String, bytes: &[u8]) {
    let digits = if UPPER {
        b"0123456789ABCDEF"
    } else {
        b"0123456789abcdef"
    };
    out.reserve(bytes.len() * 2);
    for &byte in bytes {
        out.push(digits[(byte >> 4) as usize] as char);
        out.push(digits[(byte & 0x0f) as usize] as char);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecodeError {
    OddLength,
    InvalidDigit,
}

/// Decode pairs of ASCII hex digits. Whitespace, signs and prefixes are not
/// accepted; callers with SQL padding or UUID separator rules own that policy.
pub(crate) fn decode(bytes: &[u8]) -> Result<Vec<u8>, DecodeError> {
    if bytes.len() % 2 != 0 {
        return Err(DecodeError::OddLength);
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let high = digit(pair[0]).ok_or(DecodeError::InvalidDigit)?;
        let low = digit(pair[1]).ok_or(DecodeError::InvalidDigit)?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

#[inline]
pub(crate) const fn digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_decoding_rejects_non_digits_and_preserves_error_priority() {
        for byte in 0..=255u8 {
            let expected = if byte.is_ascii_hexdigit() {
                (byte as char).to_digit(16).map(|value| value as u8)
            } else {
                None
            };
            assert_eq!(digit(byte), expected);
            for pair in [[byte, b'0'], [b'0', byte]] {
                assert_eq!(decode(&pair).is_ok(), expected.is_some());
            }
        }
        for value in ["€a", "🙂", "+A", "-1", " 1", "1 ", "0x"] {
            assert_eq!(
                decode(value.as_bytes()),
                Err(DecodeError::InvalidDigit),
                "{value:?}"
            );
        }
        assert_eq!(decode(b""), Ok(Vec::new()));
        assert_eq!(decode(b"g"), Err(DecodeError::OddLength));
        assert_eq!(decode("€".as_bytes()), Err(DecodeError::OddLength));
        let bytes: Vec<_> = (0..=255).collect();
        for len in 0..=bytes.len() {
            assert_eq!(
                decode(encode_lower(&bytes[..len]).as_bytes()).unwrap(),
                bytes[..len]
            );
            assert_eq!(
                decode(encode_upper(&bytes[..len]).as_bytes()).unwrap(),
                bytes[..len]
            );
        }
    }

    #[test]
    fn hex_encoders_preserve_every_byte_case_and_existing_utf8_prefix() {
        let bytes: Vec<_> = (0..=255).collect();
        for len in 0..=bytes.len() {
            let input = &bytes[..len];
            let lower: String = input.iter().map(|byte| format!("{byte:02x}")).collect();
            let upper: String = input.iter().map(|byte| format!("{byte:02X}")).collect();
            assert_eq!(encode_lower(input), lower);
            assert_eq!(encode_upper(input), upper);
            for prefix in ["", "0x", "X'", "标签🙂:"] {
                let mut out = String::with_capacity(prefix.len() + input.len() * 4 + 1);
                out.push_str(prefix);
                append_upper(&mut out, input);
                append_lower(&mut out, input);
                out.push('\'');
                assert_eq!(out, format!("{prefix}{upper}{lower}'"));
            }
        }
    }
}
