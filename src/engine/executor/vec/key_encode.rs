//! Compact canonical SQL-key encoder shared by hash-based vectorized
//! operators (HashAggregate, Distinct, HashJoin, TopN and window peers).
//!
//! The encoder is a small wrapper around `Vec<u8>` that writes a typed
//! tag byte followed by a fixed-width or length-prefixed value. The
//! resulting byte sequence is suitable both as a `HashMap` key and as a
//! partition routing input (the first 8 bytes of the FNV hash decide
//! the partition).
//!
//! Format choices:
//! - Tags identify legacy `ValueKey` equality classes, not raw `Value`
//!   variants. Numerically equal INT/UINT/DECIMAL values intentionally encode
//!   alike, while numeric `1` and string `"1"` remain distinct.
//! - Integer / float types are written little-endian for parity with
//!   the row-codec on heap pages.
//! - Variable-width types are length-prefixed by a `u32` so reading
//!   back is bounded.

use crate::engine::expr::ValueKey;
use crate::types::Value;

use super::batch::ColumnVector;

pub fn encode(v: &Value, buf: &mut Vec<u8>) {
    encode_with_collation(v, None, buf)
}

/// Canonical SQL hash key codec shared by every vector state operator.
/// Its tags/payloads mirror legacy `ValueKey`, including numeric
/// cross-type equality, DECIMAL normalization, canonical NaN and ±0.
pub fn encode_with_collation(v: &Value, collation_id: Option<u16>, buf: &mut Vec<u8>) {
    match (v, collation_id) {
        (Value::String(s), Some(id)) => push_string_key(s, Some(id), buf),
        (Value::SmallString(bytes), Some(id)) => {
            // SAFETY: SmallString always contains valid UTF-8.
            push_string_key(
                unsafe { std::str::from_utf8_unchecked(bytes) },
                Some(id),
                buf,
            )
        }
        _ => encode_value_key(ValueKey::from(v), buf),
    }
}

fn encode_value_key(key: ValueKey, buf: &mut Vec<u8>) {
    match key {
        ValueKey::Null => buf.push(0),
        ValueKey::Int(value) => {
            buf.push(1);
            buf.extend_from_slice(&value.to_le_bytes());
        }
        ValueKey::Float(bits) => {
            buf.push(2);
            buf.extend_from_slice(&bits.to_le_bytes());
        }
        ValueKey::String(bytes) => {
            buf.push(3);
            push_len_bytes(&bytes, buf);
        }
        ValueKey::Bytes(bytes) => {
            buf.push(4);
            push_len_bytes(&bytes, buf);
        }
        ValueKey::Decimal(bytes) => {
            buf.push(5);
            push_len_bytes(&bytes, buf);
        }
    }
}

/// Decode only a complete scalar key in the canonical integer equality class.
/// Range routing must inspect canonical bytes, not raw column types: equal
/// INT/UINT/integral DECIMAL keys must always choose the same partition.
#[inline]
pub(crate) fn single_integer_key(key: &[u8]) -> Option<i128> {
    if key.len() != 17 || key[0] != 1 {
        return None;
    }
    Some(i128::from_le_bytes(key[1..].try_into().ok()?))
}

fn push_len_bytes(bytes: &[u8], buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Push the canonical String tag plus a length-prefixed collation key.
#[inline]
fn push_string_key(s: &str, collation_id: Option<u16>, buf: &mut Vec<u8>) {
    buf.push(3);
    match collation_id {
        Some(id) => crate::charset::with_collation_key_by_id(id, s, |key| push_len_bytes(key, buf)),
        None => crate::charset::with_default_string_key(s, |key| push_len_bytes(key, buf)),
    }
}

/// Encode one column row through the same canonical SQL codec as the
/// legacy executor. Fixed-width and byte lanes avoid a `Value` envelope;
/// Decimal128 and Generic use `value_at` as the correctness fallback.
#[inline]
#[cfg(test)]
pub fn encode_from_column_row(col: &ColumnVector, row: usize, buf: &mut Vec<u8>) {
    encode_from_column_row_with_collation(col, row, None, buf);
}

#[inline]
pub fn encode_from_column_row_with_collation(
    col: &ColumnVector,
    row: usize,
    collation_id: Option<u16>,
    buf: &mut Vec<u8>,
) {
    if col.is_null(row) {
        buf.push(0);
        return;
    }
    encode_nonnull_from_column_row_with_collation(col, row, collation_id, buf);
}

/// Caller has already checked the column's NULL state. Hash-key construction
/// uses this after applying its per-key NULL-safe equality policy.
#[inline]
pub(crate) fn encode_nonnull_from_column_row_with_collation(
    col: &ColumnVector,
    row: usize,
    collation_id: Option<u16>,
    buf: &mut Vec<u8>,
) {
    let int = |value: i128, buf: &mut Vec<u8>| {
        buf.push(1);
        buf.extend_from_slice(&value.to_le_bytes());
    };
    match col {
        ColumnVector::Null { .. } => buf.push(0),
        ColumnVector::Bool { values, .. } => int(values.get(row) as i128, buf),
        ColumnVector::I64 { data, .. } => int(data[row] as i128, buf),
        ColumnVector::U64 { data, .. } => int(data[row] as i128, buf),
        ColumnVector::F64 { data, .. } => {
            encode_value_key(ValueKey::from(&Value::Double(data[row])), buf)
        }
        ColumnVector::Date32 { data, .. } => int(data[row] as i128, buf),
        ColumnVector::Time64 { data, .. }
        | ColumnVector::DateTime64 { data, .. }
        | ColumnVector::Timestamp64 { data, .. } => int(data[row] as i128, buf),
        ColumnVector::Decimal32 { data, scale, .. } => encode_value_key(
            ValueKey::from(&Value::DecimalCompact(data[row] as i64, *scale)),
            buf,
        ),
        ColumnVector::Decimal64 { data, scale, .. } => encode_value_key(
            ValueKey::from(&Value::DecimalCompact(data[row], *scale)),
            buf,
        ),
        ColumnVector::Utf8 { offsets, data, .. } => {
            let bytes = &data[offsets[row] as usize..offsets[row + 1] as usize];
            // SAFETY: Utf8 lane construction validates this invariant.
            push_string_key(
                unsafe { std::str::from_utf8_unchecked(bytes) },
                collation_id,
                buf,
            );
        }
        ColumnVector::Bytes { offsets, data, .. }
        | ColumnVector::Json { offsets, data, .. }
        | ColumnVector::Geometry { offsets, data, .. }
        | ColumnVector::Vector { offsets, data, .. } => {
            let bytes = &data[offsets[row] as usize..offsets[row + 1] as usize];
            buf.push(4);
            push_len_bytes(bytes, buf);
        }
        ColumnVector::Decimal128 { .. }
        | ColumnVector::DecimalWide { .. }
        | ColumnVector::Generic { .. } => {
            encode_with_collation(&col.value_at(row), collation_id, buf)
        }
    }
}

/// Encode a conservative prefix key for COUNT Top-K sketches. Fixed-width
/// lanes remain exact; text lanes retain only the first collation-key bytes.
/// Equal SQL keys therefore always hash together, while prefix collisions can
/// only raise the sketch's residual upper bound.
#[inline]
pub fn encode_from_column_row_with_collation_prefix(
    col: &ColumnVector,
    row: usize,
    collation_id: Option<u16>,
    string_prefix_bytes: usize,
    buf: &mut Vec<u8>,
) {
    if col.is_null(row) {
        buf.push(0);
        return;
    }
    if let ColumnVector::Utf8 { offsets, data, .. } = col {
        let bytes = &data[offsets[row] as usize..offsets[row + 1] as usize];
        let value = unsafe { std::str::from_utf8_unchecked(bytes) };
        buf.push(3);
        let length_offset = buf.len();
        buf.extend_from_slice(&0u32.to_le_bytes());
        let prefix_offset = buf.len();
        crate::charset::collation_key_prefix_by_id_into(
            collation_id.unwrap_or(crate::charset::UTF8MB4_0900_AI_CI_ID),
            value,
            string_prefix_bytes,
            buf,
        );
        let prefix_len = (buf.len() - prefix_offset) as u32;
        buf[length_offset..length_offset + 4].copy_from_slice(&prefix_len.to_le_bytes());
        return;
    }
    encode_from_column_row_with_collation(col, row, collation_id, buf);
}

/// FNV-1a 64-bit hash. Tiny, deterministic, branch-free; we only use it
/// for partition routing — the HashMap inside each partition still does
/// its own SipHash. Replace with ahash if/when it shows up in profiles.
#[inline]
pub fn fxhash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_integer_partition_key_preserves_canonical_equality() {
        for (value, expected) in [
            (Value::Int(i64::MIN), i64::MIN as i128),
            (Value::UInt(u64::MAX), u64::MAX as i128),
            (Value::Int(12), 12),
            (Value::UInt(12), 12),
            (Value::Double(12.0), 12),
            (Value::DecimalCompact(1200, 2), 12),
        ] {
            assert_eq!(single_integer_key(&key(&value)), Some(expected));
        }
        for value in [
            Value::Null,
            Value::Double(1.5),
            Value::DecimalCompact(125, 2),
            Value::String("12".into()),
            Value::Bytes(vec![1; 16]),
        ] {
            assert_eq!(single_integer_key(&key(&value)), None);
        }
        let mut composite = key(&Value::Int(12));
        encode(&Value::Null, &mut composite);
        assert_eq!(single_integer_key(&composite), None);
        for value in [i128::MIN, i128::MAX] {
            let mut encoded = vec![1];
            encoded.extend_from_slice(&value.to_le_bytes());
            assert_eq!(single_integer_key(&encoded), Some(value));
        }
    }

    #[test]
    fn different_types_never_collide() {
        let mut a = Vec::new();
        let mut b = Vec::new();
        encode(&Value::Int(1), &mut a);
        encode(&Value::String("1".into()), &mut b);
        assert_ne!(a, b);
    }

    #[test]
    fn same_value_same_bytes() {
        let mut a = Vec::new();
        let mut b = Vec::new();
        encode(&Value::Int(42), &mut a);
        encode(&Value::Int(42), &mut b);
        assert_eq!(a, b);
    }

    #[test]
    fn null_is_single_byte_sentinel() {
        let mut a = Vec::new();
        encode(&Value::Null, &mut a);
        assert_eq!(a, vec![0]);
    }

    fn key(value: &Value) -> Vec<u8> {
        let mut out = Vec::new();
        encode(value, &mut out);
        out
    }

    #[test]
    fn canonical_numeric_keys_match_legacy_value_key() {
        assert_eq!(key(&Value::Int(1)), key(&Value::UInt(1)));
        assert_eq!(key(&Value::Int(1)), key(&Value::Double(1.0)));
        assert_eq!(key(&Value::DecimalCompact(100, 2)), key(&Value::Int(1)));
        assert_eq!(key(&Value::Double(-0.0)), key(&Value::Double(0.0)));
        assert_eq!(
            key(&Value::Double(f64::NAN)),
            key(&Value::Double(-f64::NAN))
        );
    }

    #[test]
    fn explicit_collation_controls_string_key() {
        let mut ci_a = Vec::new();
        let mut ci_b = Vec::new();
        encode_with_collation(&Value::String("A".into()), Some(255), &mut ci_a);
        encode_with_collation(&Value::String("a".into()), Some(255), &mut ci_b);
        assert_eq!(ci_a, ci_b);

        let mut bin_a = Vec::new();
        let mut bin_b = Vec::new();
        encode_with_collation(&Value::String("A".into()), Some(63), &mut bin_a);
        encode_with_collation(&Value::String("a".into()), Some(63), &mut bin_b);
        assert_ne!(bin_a, bin_b);
    }

    #[test]
    fn encode_from_column_row_matches_value_path_for_typed_lanes() {
        // For every typed lane the optimised column-direct encoder must
        // produce the byte-identical key to `encode(&col.value_at(row))`
        // so partition / bucket lookups stay consistent regardless of
        // which path filled the key buffer.
        let inputs: Vec<(&str, ColumnVector)> = vec![
            (
                "i64",
                ColumnVector::from_values(vec![Value::Int(7), Value::Null, Value::Int(-3)]),
            ),
            (
                "f64",
                ColumnVector::from_values(vec![Value::Double(1.5), Value::Double(-7.25)]),
            ),
            (
                "u64",
                ColumnVector::from_values(vec![Value::UInt(u64::MAX), Value::Null]),
            ),
            (
                "timestamp",
                ColumnVector::from_values(vec![Value::Timestamp(1_700_000_000_000_000)]),
            ),
            (
                "json",
                ColumnVector::from_values(vec![Value::Json(br#"{"x":1}"#.to_vec())]),
            ),
            (
                "date",
                ColumnVector::from_values(vec![Value::Date(19000), Value::Null]),
            ),
            (
                "dt",
                ColumnVector::from_values(vec![Value::DateTime(1_700_000_000_000_000)]),
            ),
            (
                "dec",
                ColumnVector::from_values(vec![
                    Value::DecimalCompact(123_456, 2),
                    Value::DecimalCompact(-99, 2),
                ]),
            ),
            (
                "utf8",
                ColumnVector::from_values(vec![
                    Value::String("A".into()),
                    Value::String("foo bar".into()),
                    Value::Null,
                ]),
            ),
        ];
        for (label, col) in &inputs {
            for row in 0..col.len() {
                let mut from_value = Vec::new();
                encode(&col.value_at(row), &mut from_value);
                let mut from_column = Vec::new();
                encode_from_column_row(col, row, &mut from_column);
                assert_eq!(
                    from_column, from_value,
                    "lane {label} row {row}: column-direct encode differs from value-path encode"
                );
            }
        }
    }
}
