//! MySQL 8.0 binary-JSON encoder used by row-based binlog events.
//!
//! A binary JSON document is `type-byte || payload`. Container payloads use
//! absolute offsets from the beginning of the container payload and choose
//! the compact 16-bit layout whenever every offset and the total byte size fit.

use super::jsonb::{JsonType, JsonbRef};
use crate::types::QueryError;

const SMALL_OBJECT: u8 = 0x00;
const LARGE_OBJECT: u8 = 0x01;
const SMALL_ARRAY: u8 = 0x02;
const LARGE_ARRAY: u8 = 0x03;
const LITERAL: u8 = 0x04;
const INT16: u8 = 0x05;
const UINT16: u8 = 0x06;
const INT32: u8 = 0x07;
const UINT32: u8 = 0x08;
const INT64: u8 = 0x09;
const UINT64: u8 = 0x0a;
const DOUBLE: u8 = 0x0b;
const STRING: u8 = 0x0c;
const OPAQUE: u8 = 0x0f;
const MYSQL_TYPE_DATE: u8 = 10;
const MYSQL_TYPE_TIME: u8 = 11;
const MYSQL_TYPE_DATETIME: u8 = 12;
const MYSQL_TYPE_NEWDECIMAL: u8 = 246;
const DECIMAL_GROUP_BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

#[derive(Debug)]
struct EncodedValue {
    ty: u8,
    payload: Vec<u8>,
}

/// Convert mydb's canonical JSON representation to the byte format carried
/// by a MySQL `MYSQL_TYPE_JSON` row event.
pub(crate) fn encode(bytes: &[u8]) -> Result<Vec<u8>, QueryError> {
    let node = JsonbRef::new(bytes)?;
    node.validate()?;
    let value = encode_node(node)?;
    let mut out = Vec::with_capacity(1 + value.payload.len());
    out.push(value.ty);
    out.extend_from_slice(&value.payload);
    Ok(out)
}

fn encode_node(node: JsonbRef<'_>) -> Result<EncodedValue, QueryError> {
    Ok(match node.json_type() {
        JsonType::Null => literal(0),
        JsonType::Boolean => literal(if node.as_bool().unwrap_or(false) {
            1
        } else {
            2
        }),
        JsonType::Integer => encode_signed(node.as_i64().unwrap_or(0)),
        JsonType::UnsignedInteger => {
            let value = node
                .exact_number()
                .and_then(|(magnitude, scale)| (scale == 0).then_some(magnitude))
                .and_then(|magnitude| u64::try_from(magnitude).ok())
                .unwrap_or(0);
            encode_unsigned(value)
        }
        JsonType::Double => EncodedValue {
            ty: DOUBLE,
            payload: node.as_f64().unwrap_or(0.0).to_le_bytes().to_vec(),
        },
        JsonType::String => encode_string(node.as_str().unwrap_or_default().as_bytes()),
        // MySQL's binary JSON keeps temporal SQL scalars in OPAQUE leaves.
        JsonType::Date => encode_temporal_opaque(
            MYSQL_TYPE_DATE,
            pack_mysql_date(node.as_str().unwrap_or_default()).unwrap_or_default(),
        ),
        JsonType::Time => encode_temporal_opaque(
            MYSQL_TYPE_TIME,
            pack_mysql_time(node.as_str().unwrap_or_default()).unwrap_or_default(),
        ),
        JsonType::Timestamp => encode_temporal_opaque(
            7,
            pack_mysql_datetime(node.as_str().unwrap_or_default()).unwrap_or_default(),
        ),
        JsonType::Opaque => {
            let (ty, data) = node
                .opaque_value()
                .ok_or_else(|| super::invalid_json_text(1, "opaque JSON"))?;
            let mut payload = vec![ty];
            write_variable_length(data.len(), &mut payload);
            payload.extend_from_slice(data);
            EncodedValue {
                ty: OPAQUE,
                payload,
            }
        }
        JsonType::Datetime => encode_temporal_opaque(
            MYSQL_TYPE_DATETIME,
            pack_mysql_datetime(node.as_str().unwrap_or_default()).unwrap_or_default(),
        ),
        JsonType::Decimal => encode_decimal_opaque(&node.number_text().unwrap_or_default())?,
        JsonType::Array => {
            let values = node
                .array_iter()
                .map(encode_node)
                .collect::<Result<Vec<_>, _>>()?;
            encode_container(None, values)?
        }
        JsonType::Object => {
            let mut entries = node
                .object_iter()
                .map(|(key, value)| Ok((key.as_bytes().to_vec(), encode_node(value)?)))
                .collect::<Result<Vec<_>, QueryError>>()?;
            entries.sort_by(|(a, _), (b, _)| (a.len(), a).cmp(&(b.len(), b)));
            let (keys, values): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
            encode_container(Some(keys), values)?
        }
    })
}

fn encode_temporal_opaque(column_type: u8, packed: i64) -> EncodedValue {
    let data = packed.to_le_bytes();
    let mut payload = Vec::with_capacity(10);
    payload.push(column_type);
    write_variable_length(data.len(), &mut payload);
    payload.extend_from_slice(&data);
    EncodedValue {
        ty: OPAQUE,
        payload,
    }
}

fn encode_opaque(column_type: u8, data: &[u8]) -> EncodedValue {
    let mut payload = Vec::with_capacity(data.len() + 6);
    payload.push(column_type);
    write_variable_length(data.len(), &mut payload);
    payload.extend_from_slice(data);
    EncodedValue {
        ty: OPAQUE,
        payload,
    }
}

fn encode_decimal_opaque(text: &str) -> Result<EncodedValue, QueryError> {
    let (negative, unsigned) = text
        .strip_prefix('-')
        .map_or((false, text), |value| (true, value));
    let (integer, fraction) = unsigned
        .split_once('.')
        .map_or((unsigned, ""), |(integer, fraction)| (integer, fraction));
    if integer.is_empty()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(super::json_err(
            crate::error::catalog::ER_INVALID_JSON_VALUE_FOR_CAST,
            "invalid DECIMAL JSON scalar",
        ));
    }
    let integer = integer.trim_start_matches('0');
    let integer = if integer.is_empty() { "0" } else { integer };
    let precision = integer.len().checked_add(fraction.len()).ok_or_else(|| {
        super::json_err(
            crate::error::catalog::ER_JSON_VALUE_TOO_BIG,
            "DECIMAL JSON scalar is too large",
        )
    })?;
    if precision > 65 || fraction.len() > 30 {
        return Err(super::json_err(
            crate::error::catalog::ER_NUMERIC_JSON_VALUE_OUT_OF_RANGE,
            "DECIMAL JSON scalar is out of range",
        ));
    }
    let mut packed = Vec::with_capacity(2 + (precision + 1) / 2);
    packed.push(precision as u8);
    packed.push(fraction.len() as u8);
    let prefix = integer.len() % 9;
    let mut cursor = 0usize;
    if prefix != 0 {
        write_decimal_group(
            integer[..prefix].parse().unwrap_or(0),
            DECIMAL_GROUP_BYTES[prefix],
            negative,
            &mut packed,
        );
        cursor = prefix;
    }
    while cursor < integer.len() {
        write_decimal_group(
            integer[cursor..cursor + 9].parse().unwrap_or(0),
            4,
            negative,
            &mut packed,
        );
        cursor += 9;
    }
    cursor = 0;
    while fraction.len().saturating_sub(cursor) >= 9 {
        write_decimal_group(
            fraction[cursor..cursor + 9].parse().unwrap_or(0),
            4,
            negative,
            &mut packed,
        );
        cursor += 9;
    }
    let suffix = fraction.len() - cursor;
    if suffix != 0 {
        write_decimal_group(
            fraction[cursor..].parse().unwrap_or(0),
            DECIMAL_GROUP_BYTES[suffix],
            negative,
            &mut packed,
        );
    }
    if let Some(first) = packed.get_mut(2) {
        *first ^= 0x80;
    }
    Ok(encode_opaque(MYSQL_TYPE_NEWDECIMAL, &packed))
}

fn write_decimal_group(value: i32, width: usize, negative: bool, out: &mut Vec<u8>) {
    let value = if negative { !value } else { value };
    let bytes = value.to_be_bytes();
    out.extend_from_slice(&bytes[4 - width..]);
}

fn parse_fraction(value: Option<&str>) -> Option<u32> {
    let value = value.unwrap_or_default();
    if !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let mut micros = value
        .bytes()
        .take(6)
        .fold(0u32, |acc, byte| acc * 10 + u32::from(byte - b'0'));
    for _ in value.len().min(6)..6 {
        micros *= 10;
    }
    Some(micros)
}

fn pack_fraction(int_part: i64, micros: u32) -> i64 {
    (int_part << 24) + i64::from(micros)
}

fn pack_mysql_date(value: &str) -> Option<i64> {
    let mut parts = value.split('-');
    let year = parts.next()?.parse::<i64>().ok()?;
    let month = parts.next()?.parse::<i64>().ok()?;
    let day = parts.next()?.parse::<i64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    let ymd = ((year * 13 + month) << 5) | day;
    Some(pack_fraction(ymd << 17, 0))
}

fn pack_mysql_datetime(value: &str) -> Option<i64> {
    let (date, time) = value.split_once(' ')?;
    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse::<i64>().ok()?;
    let month = date_parts.next()?.parse::<i64>().ok()?;
    let day = date_parts.next()?.parse::<i64>().ok()?;
    if date_parts.next().is_some() {
        return None;
    }
    let (hms, fraction) = time
        .split_once('.')
        .map_or((time, None), |(hms, fraction)| (hms, Some(fraction)));
    let mut time_parts = hms.split(':');
    let hour = time_parts.next()?.parse::<i64>().ok()?;
    let minute = time_parts.next()?.parse::<i64>().ok()?;
    let second = time_parts.next()?.parse::<i64>().ok()?;
    if time_parts.next().is_some() {
        return None;
    }
    let ymd = ((year * 13 + month) << 5) | day;
    let hms = (hour << 12) | (minute << 6) | second;
    Some(pack_fraction((ymd << 17) | hms, parse_fraction(fraction)?))
}

fn pack_mysql_time(value: &str) -> Option<i64> {
    let (negative, value) = value
        .strip_prefix('-')
        .map_or((false, value), |value| (true, value));
    let (hms, fraction) = value
        .split_once('.')
        .map_or((value, None), |(hms, fraction)| (hms, Some(fraction)));
    let mut parts = hms.split(':');
    let hour = parts.next()?.parse::<i64>().ok()?;
    let minute = parts.next()?.parse::<i64>().ok()?;
    let second = parts.next()?.parse::<i64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    let packed = pack_fraction(
        (hour << 12) | (minute << 6) | second,
        parse_fraction(fraction)?,
    );
    Some(if negative { -packed } else { packed })
}

fn literal(value: u8) -> EncodedValue {
    EncodedValue {
        ty: LITERAL,
        payload: vec![value],
    }
}

fn encode_signed(value: i64) -> EncodedValue {
    if let Ok(value) = i16::try_from(value) {
        EncodedValue {
            ty: INT16,
            payload: value.to_le_bytes().to_vec(),
        }
    } else if let Ok(value) = i32::try_from(value) {
        EncodedValue {
            ty: INT32,
            payload: value.to_le_bytes().to_vec(),
        }
    } else {
        EncodedValue {
            ty: INT64,
            payload: value.to_le_bytes().to_vec(),
        }
    }
}

fn encode_unsigned(value: u64) -> EncodedValue {
    if let Ok(value) = u16::try_from(value) {
        EncodedValue {
            ty: UINT16,
            payload: value.to_le_bytes().to_vec(),
        }
    } else if let Ok(value) = u32::try_from(value) {
        EncodedValue {
            ty: UINT32,
            payload: value.to_le_bytes().to_vec(),
        }
    } else {
        EncodedValue {
            ty: UINT64,
            payload: value.to_le_bytes().to_vec(),
        }
    }
}

fn encode_string(value: &[u8]) -> EncodedValue {
    let mut payload = Vec::with_capacity(value.len() + 5);
    write_variable_length(value.len(), &mut payload);
    payload.extend_from_slice(value);
    EncodedValue {
        ty: STRING,
        payload,
    }
}

fn write_variable_length(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn encode_container(
    keys: Option<Vec<Vec<u8>>>,
    values: Vec<EncodedValue>,
) -> Result<EncodedValue, QueryError> {
    if values.len() <= u16::MAX as usize {
        if let Some(payload) = build_container(keys.as_deref(), &values, false) {
            return Ok(EncodedValue {
                ty: if keys.is_some() {
                    SMALL_OBJECT
                } else {
                    SMALL_ARRAY
                },
                payload,
            });
        }
    }
    let payload = build_container(keys.as_deref(), &values, true).ok_or_else(|| {
        super::json_err(
            crate::error::catalog::ER_JSON_VALUE_TOO_BIG,
            "JSON value is too big for MySQL binary JSON",
        )
    })?;
    Ok(EncodedValue {
        ty: if keys.is_some() {
            LARGE_OBJECT
        } else {
            LARGE_ARRAY
        },
        payload,
    })
}

fn build_container(
    keys: Option<&[Vec<u8>]>,
    values: &[EncodedValue],
    large: bool,
) -> Option<Vec<u8>> {
    let offset_size = if large { 4 } else { 2 };
    let count = values.len();
    let key_entry_size = offset_size + 2;
    let value_entry_size = offset_size + 1;
    let key_entries_size = if keys.is_some() {
        count.checked_mul(key_entry_size)?
    } else {
        0
    };
    let header_size = (offset_size * 2)
        .checked_add(key_entries_size)?
        .checked_add(count.checked_mul(value_entry_size)?)?;
    let key_bytes = match keys {
        Some(keys) => keys
            .iter()
            .map(Vec::len)
            .try_fold(0usize, usize::checked_add)?,
        None => 0,
    };
    let external_bytes = values
        .iter()
        .filter(|value| !is_inline(value.ty, large))
        .map(|value| value.payload.len())
        .try_fold(0usize, usize::checked_add)?;
    let total_size = header_size
        .checked_add(key_bytes)?
        .checked_add(external_bytes)?;
    if (!large && total_size > u16::MAX as usize) || total_size > u32::MAX as usize {
        return None;
    }
    if keys
        .into_iter()
        .flatten()
        .any(|key| key.len() > u16::MAX as usize)
    {
        return None;
    }

    let mut out = Vec::with_capacity(total_size);
    write_offset(count, large, &mut out)?;
    write_offset(total_size, large, &mut out)?;

    let mut next_key_offset = header_size;
    if let Some(keys) = keys {
        for key in keys {
            write_offset(next_key_offset, large, &mut out)?;
            out.extend_from_slice(&(key.len() as u16).to_le_bytes());
            next_key_offset += key.len();
        }
    }
    let mut next_value_offset = header_size + key_bytes;
    for value in values {
        out.push(value.ty);
        if is_inline(value.ty, large) {
            let width = offset_size;
            out.extend_from_slice(&value.payload[..value.payload.len().min(width)]);
            out.resize(out.len() + width.saturating_sub(value.payload.len()), 0);
        } else {
            write_offset(next_value_offset, large, &mut out)?;
            next_value_offset += value.payload.len();
        }
    }
    if let Some(keys) = keys {
        for key in keys {
            out.extend_from_slice(key);
        }
    }
    for value in values {
        if !is_inline(value.ty, large) {
            out.extend_from_slice(&value.payload);
        }
    }
    debug_assert_eq!(out.len(), total_size);
    Some(out)
}

fn is_inline(ty: u8, large: bool) -> bool {
    matches!(ty, LITERAL | INT16 | UINT16) || (large && matches!(ty, INT32 | UINT32))
}

fn write_offset(value: usize, large: bool, out: &mut Vec<u8>) -> Option<()> {
    if large {
        out.extend_from_slice(&u32::try_from(value).ok()?.to_le_bytes());
    } else {
        out.extend_from_slice(&u16::try_from(value).ok()?.to_le_bytes());
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_common::io::ParseBuf;
    use mysql_common::proto::MyDeserialize;

    #[test]
    fn encoded_documents_are_accepted_by_mysql_common() {
        for text in [
            "null",
            "true",
            "-40000",
            "70000",
            r#""hello""#,
            "[1,true,\"x\",{\"a\":2}]",
            r#"{"a":1,"longer":[2,3],"z":null}"#,
        ] {
            let source = crate::engine::json::parse_text(text, 1, "test").unwrap();
            let encoded = encode(&source).unwrap();
            let decoded =
                mysql_common::binlog::jsonb::Value::deserialize((), &mut ParseBuf(&encoded))
                    .unwrap();
            let dom = serde_json::Value::from(decoded.parse().unwrap());
            let expected = serde_json::from_str::<serde_json::Value>(text).unwrap();
            assert_eq!(dom, expected, "{text}");
        }
    }

    #[test]
    fn temporal_scalars_use_mysql_opaque_encoding() {
        for (value, expected) in [
            (crate::types::Value::Date(0), "1970-01-01"),
            (
                crate::types::Value::Time(12 * 3_600_000_000 + 34 * 60_000_000),
                "12:34:00.000000",
            ),
            (
                crate::types::Value::DateTime(0),
                "1970-01-01 00:00:00.000000",
            ),
        ] {
            let source = crate::engine::json::scalar_from_value(&value);
            let encoded = encode(&source).unwrap();
            assert_eq!(encoded[0], OPAQUE);
            let decoded =
                mysql_common::binlog::jsonb::Value::deserialize((), &mut ParseBuf(&encoded))
                    .unwrap();
            let dom = serde_json::Value::from(decoded.parse().unwrap());
            assert_eq!(dom, serde_json::Value::String(expected.into()));
        }
    }

    #[test]
    fn decimal_scalars_use_mysql_packed_decimal_opaque_encoding() {
        for expected in ["0", "1234.5678", "-0.125", "999999999999999999.01"] {
            let source = crate::engine::json::scalar_from_value(&crate::types::Value::Decimal(
                expected.into(),
            ));
            let encoded = encode(&source).unwrap();
            assert_eq!(encoded[0], OPAQUE);
            let decoded =
                mysql_common::binlog::jsonb::Value::deserialize((), &mut ParseBuf(&encoded))
                    .unwrap();
            let dom = serde_json::Value::from(decoded.parse().unwrap());
            assert_eq!(dom, serde_json::Value::String(expected.into()));
        }
    }
}
