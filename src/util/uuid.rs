//! UUID parsing and canonical formatting shared by SQL, binlog and replication.

pub(crate) fn parse(value: &str) -> Option<[u8; 16]> {
    let mut bytes = value.bytes().filter(|byte| *byte != b'-');
    let mut result = [0; 16];
    for byte in &mut result {
        *byte = (nibble(bytes.next()?)? << 4) | nibble(bytes.next()?)?;
    }
    bytes.next().is_none().then_some(result)
}

use crate::util::hex::digit as nibble;

pub(crate) fn format(bytes: &[u8; 16]) -> String {
    let mut out = String::with_capacity(36);
    for range in [0..4, 4..6, 6..8, 8..10, 10..16] {
        if !out.is_empty() {
            out.push('-');
        }
        crate::util::hex::append_lower(&mut out, &bytes[range]);
    }
    out
}

/// Append canonical ASCII directly to an owning SQL column buffer.
pub(crate) fn append_ascii(bytes: &[u8; 16], out: &mut Vec<u8>) {
    for (index, byte) in bytes.iter().copied().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            out.push(b'-');
        }
        out.push(b"0123456789abcdef"[(byte >> 4) as usize]);
        out.push(b"0123456789abcdef"[(byte & 15) as usize]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_format_preserves_every_byte_and_group_boundary() {
        for position in 0..16 {
            for byte in 0..=255 {
                let mut bytes = [0x5a; 16];
                bytes[position] = byte;
                let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
                let expected = format!(
                    "{}-{}-{}-{}-{}",
                    &hex[..8],
                    &hex[8..12],
                    &hex[12..16],
                    &hex[16..20],
                    &hex[20..]
                );
                let actual = format(&bytes);
                let mut appended = b"prefix".to_vec();
                append_ascii(&bytes, &mut appended);
                assert_eq!(&appended[6..], expected.as_bytes());
                assert_eq!(actual, expected);
                assert_eq!(parse(&actual), Some(bytes));
            }
        }
    }

    #[test]
    fn accepts_hex_and_rejects_trailing_or_non_ascii_data() {
        let canonical = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        let bytes = parse(canonical).unwrap();
        assert_eq!(format(&bytes), canonical);
        assert_eq!(parse(&canonical.to_uppercase()), Some(bytes));
        assert_eq!(parse(&canonical.replace('-', "")), Some(bytes));
        for invalid in [
            format!("{canonical}ff"),
            format!("{canonical}x"),
            "é1234567890123456789012345678901".into(),
            "abc".into(),
        ] {
            assert_eq!(parse(&invalid), None, "{invalid}");
        }
    }
}
