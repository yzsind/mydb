//! Self-delimiting set keys with the scalar oracle's exact equality domain.
use super::{SetRowKey, Value};

fn integer(value: i128, key: &mut SetRowKey) {
    let (tag, width) = if i8::try_from(value).is_ok() {
        (6, 1)
    } else if i16::try_from(value).is_ok() {
        (7, 2)
    } else if i32::try_from(value).is_ok() {
        (8, 4)
    } else if i64::try_from(value).is_ok() {
        (9, 8)
    } else {
        (10, 16)
    };
    // Tags 0..=5 are reserved by the canonical scalar encoding. Width is
    // encoded in the tag, so adjacent fields cannot alias one another.
    key.push(tag);
    key.extend_from_slice(&value.to_le_bytes()[..width]);
}

fn decimal(mut magnitude: i128, mut scale: u8, key: &mut SetRowKey) {
    while scale > 0 && magnitude % 10 == 0 {
        magnitude /= 10;
        scale -= 1;
    }
    if scale == 0 {
        integer(magnitude, key);
        return;
    }
    // Match BigInt::to_signed_bytes_le, including the sign byte, without
    // allocating a BigInt for ordinary fixed-width DECIMAL values.
    let bytes = magnitude.to_le_bytes();
    let mut width = bytes.len();
    while width > 1
        && ((bytes[width - 1] == 0 && bytes[width - 2] & 0x80 == 0)
            || (bytes[width - 1] == 255 && bytes[width - 2] & 0x80 != 0))
    {
        width -= 1;
    }
    key.push(5);
    key.push(width as u8);
    key.extend_from_slice(&bytes[..width]);
    key.push(scale);
}

pub(super) fn write_value(value: &Value, key: &mut SetRowKey) {
    match value {
        Value::Int(value) => integer(i128::from(*value), key),
        Value::UInt(value) => integer(i128::from(*value), key),
        Value::Date(value) => integer(i128::from(*value), key),
        Value::Time(value) | Value::DateTime(value) | Value::Timestamp(value) => {
            integer(i128::from(*value), key)
        }
        Value::Year(value) => integer(
            if *value == 0 {
                0
            } else {
                1900 + i128::from(*value)
            },
            key,
        ),
        Value::DecimalCompact(value, scale) => decimal(i128::from(*value), *scale, key),
        Value::Decimal128(value, scale) => decimal(*value, *scale, key),
        _ => {
            let start = key.len();
            crate::engine::expr::write_value_inline_key(value, key);
            // Floats, textual decimals and wide decimals can also normalize
            // to the scalar integer domain. Apply exactly the same packing.
            if key[start] == 1 {
                let value =
                    i128::from_le_bytes(key[start + 1..].try_into().expect("scalar integer key"));
                key.truncate(start);
                integer(value, key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_decimal_keys_match_the_canonical_domain_at_every_signed_boundary() {
        for magnitude in [
            i128::MIN,
            i128::MIN + 1,
            i64::MIN as i128,
            -32769,
            -32768,
            -129,
            -128,
            -1,
            0,
            1,
            127,
            128,
            255,
            256,
            32767,
            32768,
            u64::MAX as i128,
            i128::MAX,
        ] {
            for scale in [0, 1, 2, 9, 30, 38] {
                let narrow = Value::Decimal128(magnitude, scale);
                let wide = Value::DecimalWide(Box::new(crate::types::DecimalWide {
                    magnitude: magnitude.into(),
                    scale,
                }));
                let key = |value: &Value| {
                    let mut key = SetRowKey::new();
                    write_value(value, &mut key);
                    key
                };
                assert_eq!(key(&narrow), key(&wide), "{magnitude}, scale {scale}");
            }
        }
        let mut keys = std::collections::HashSet::new();
        for a in -256..=256 {
            for b in [-129, -1, 0, 1, 127, 128, 256] {
                let mut key = SetRowKey::new();
                write_value(&Value::Int(a), &mut key);
                write_value(&Value::Int(b), &mut key);
                assert!(keys.insert(key), "tuple fields must remain self-delimiting");
            }
        }
    }
}
