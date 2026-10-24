//! Scalar Value payloads shared by row and columnar temporary files.
//! File framing and typed/compressed column lanes remain with each operator.

use crate::types::{QueryError, Value};
use std::io::{Read, Write};

pub(crate) const VALUE_NULL: u8 = 0;
pub(crate) const VALUE_INT: u8 = 1;
pub(crate) const VALUE_UINT: u8 = 2;
pub(crate) const VALUE_FLOAT: u8 = 3;
pub(crate) const VALUE_DOUBLE: u8 = 4;
pub(crate) const VALUE_DECIMAL: u8 = 5;
pub(crate) const VALUE_BYTES: u8 = 6;
pub(crate) const VALUE_STRING: u8 = 7;
// Phase 2-bin: dedicated tags for the binary temporal / decimal Value
// variants so spilled partitions round-trip in binary form (no string
// re-format on read-back).
pub(crate) const VALUE_DECIMAL_COMPACT: u8 = 8;
pub(crate) const VALUE_DATE: u8 = 9;
pub(crate) const VALUE_TIME: u8 = 10;
pub(crate) const VALUE_DATETIME: u8 = 11;
pub(crate) const VALUE_YEAR: u8 = 12;
pub(crate) const VALUE_JSON: u8 = 13;
pub(crate) const VALUE_GEOMETRY: u8 = 14;
pub(crate) const VALUE_VECTOR: u8 = 15;
pub(crate) const VALUE_TIMESTAMP: u8 = 16;
pub(crate) const VALUE_DECIMAL128: u8 = 17;
pub(crate) const VALUE_DECIMAL_WIDE: u8 = 18;

pub(crate) fn write_u32<W: Write>(w: &mut W, v: u32) -> std::io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

pub(crate) fn write_str<W: Write>(w: &mut W, s: &str) -> std::io::Result<()> {
    let b = s.as_bytes();
    write_bytes(w, b)
}

pub(crate) fn write_bytes<W: Write>(w: &mut W, b: &[u8]) -> std::io::Result<()> {
    let length = u32::try_from(b.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "spill value exceeds its length encoding",
        )
    })?;
    write_u32(w, length)?;
    w.write_all(b)
}

pub(crate) fn read_u32<R: Read>(r: &mut R) -> std::io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

/// Only EOF before the first byte is an empty stream. A partial header is
/// corrupt, including when a reader returns one byte per read.
pub(crate) fn read_optional_u32<R: Read>(r: &mut R) -> std::io::Result<Option<u32>> {
    let mut bytes = [0; 4];
    match r.read_exact(&mut bytes[..1]) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    r.read_exact(&mut bytes[1..])?;
    Ok(Some(u32::from_le_bytes(bytes)))
}

pub(crate) fn read_str<R: Read>(r: &mut R) -> std::io::Result<String> {
    let buf = read_bytes(r)?;
    String::from_utf8(buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

pub(crate) fn read_bytes<R: Read>(r: &mut R) -> std::io::Result<Vec<u8>> {
    read_bytes_accounted(r, None, None)
}

fn read_bytes_accounted<R: Read>(
    r: &mut R,
    budget: Option<&mut super::mem_budget::ScopedCharge>,
    available: Option<u64>,
) -> std::io::Result<Vec<u8>> {
    let len = read_u32(r)? as usize;
    if available.is_some_and(|available| len as u64 > available.saturating_sub(4)) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "spill value length exceeds its record",
        ));
    }
    read_exact_bytes_accounted(r, len, budget)
}

pub(crate) fn read_exact_bytes_accounted<R: Read>(
    r: &mut R,
    len: usize,
    mut budget: Option<&mut super::mem_budget::ScopedCharge>,
) -> std::io::Result<Vec<u8>> {
    if let Some(budget) = budget.as_mut() {
        budget
            .add_force(len)
            .map_err(super::spill_quota::quota_as_io)?;
    }
    // A corrupt length must not allocate/touch the entire declared payload
    // before discovering EOF. Grow only after each bounded read succeeds.
    let mut buf = Vec::new();
    let mut chunk = [0; 8192];
    while buf.len() < len {
        if len > chunk.len() {
            crate::engine::cancel::check_cancelled().map_err(super::spill_quota::quota_as_io)?;
        }
        let count = (len - buf.len()).min(chunk.len());
        r.read_exact(&mut chunk[..count])?;
        if buf.capacity() - buf.len() < count {
            let capacity = buf
                .capacity()
                .saturating_mul(2)
                .max(buf.len() + count)
                .min(len);
            // Reallocation may keep the old allocation live while allocating
            // its replacement. The final payload reservation covers `len`,
            // so only the overlap above that amount needs a temporary lease.
            let mut overlap = budget.as_ref().map(|charge| charge.sibling());
            if let Some(overlap) = overlap.as_mut() {
                overlap
                    .add_force(buf.capacity().saturating_add(capacity).saturating_sub(len))
                    .map_err(super::spill_quota::quota_as_io)?;
            }
            buf.try_reserve_exact(capacity - buf.len()).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "unable to allocate spill value",
                )
            })?;
        }
        buf.extend_from_slice(&chunk[..count]);
    }
    Ok(buf)
}

#[inline]
pub(crate) fn value_tag(v: &Value) -> u8 {
    match v {
        Value::Null => VALUE_NULL,
        Value::Int(_) => VALUE_INT,
        Value::UInt(_) => VALUE_UINT,
        Value::Float(_) => VALUE_FLOAT,
        Value::Double(_) => VALUE_DOUBLE,
        Value::Decimal(_) => VALUE_DECIMAL,
        Value::Bytes(_) => VALUE_BYTES,
        Value::String(_) | Value::SmallString(_) => VALUE_STRING,
        Value::DecimalCompact(_, _) => VALUE_DECIMAL_COMPACT,
        Value::Decimal128(_, _) => VALUE_DECIMAL128,
        Value::DecimalWide(_) => VALUE_DECIMAL_WIDE,
        Value::Date(_) => VALUE_DATE,
        Value::Time(_) => VALUE_TIME,
        Value::DateTime(_) => VALUE_DATETIME,
        Value::Timestamp(_) => VALUE_TIMESTAMP,
        Value::Year(_) => VALUE_YEAR,
        Value::Json(_) => VALUE_JSON,
        Value::Geometry(_) => VALUE_GEOMETRY,
        Value::Vector(_) => VALUE_VECTOR,
    }
}

pub(crate) fn write_value_payload<W: Write>(w: &mut W, v: &Value) -> std::io::Result<()> {
    match v {
        Value::Null => Ok(()),
        Value::Int(i) => w.write_all(&i.to_le_bytes()),
        Value::UInt(u) => w.write_all(&u.to_le_bytes()),
        Value::Float(f) => w.write_all(&f.to_bits().to_le_bytes()),
        Value::Double(f) => w.write_all(&f.to_bits().to_le_bytes()),
        Value::Decimal(s) => write_str(w, s),
        Value::Bytes(b) => write_bytes(w, b),
        Value::String(s) => write_str(w, s),
        Value::SmallString(b) => write_str(w, unsafe { std::str::from_utf8_unchecked(b) }),
        Value::DecimalCompact(mag, scale) => {
            w.write_all(&mag.to_le_bytes())?;
            w.write_all(&[*scale])
        }
        Value::Decimal128(mag, scale) => {
            w.write_all(&mag.to_le_bytes())?;
            w.write_all(&[*scale])
        }
        Value::DecimalWide(value) => {
            let bytes = value.magnitude.to_signed_bytes_le();
            write_bytes(w, &bytes)?;
            w.write_all(&[value.scale])
        }
        Value::Date(d) => w.write_all(&d.to_le_bytes()),
        Value::Time(t) => w.write_all(&t.to_le_bytes()),
        Value::DateTime(dt) => w.write_all(&dt.to_le_bytes()),
        Value::Timestamp(ts) => w.write_all(&ts.to_le_bytes()),
        Value::Year(y) => w.write_all(&[*y]),
        Value::Json(b) => write_bytes(w, b),
        Value::Geometry(b) => write_bytes(w, b),
        Value::Vector(b) => write_bytes(w, b),
    }
}

#[inline]
pub(crate) fn write_value<W: Write>(w: &mut W, value: &Value) -> std::io::Result<()> {
    w.write_all(&[value_tag(value)])?;
    write_value_payload(w, value)
}

pub(crate) fn read_payload<R: Read>(
    r: &mut R,
    tag: u8,
    inline_strings: bool,
    io_err: impl Fn(std::io::Error) -> QueryError + Copy,
) -> Result<Value, QueryError> {
    read_payload_accounted(r, tag, inline_strings, io_err, None)
}

pub(crate) fn read_payload_accounted<R: Read>(
    r: &mut R,
    tag: u8,
    inline_strings: bool,
    io_err: impl Fn(std::io::Error) -> QueryError + Copy,
    budget: Option<&mut super::mem_budget::ScopedCharge>,
) -> Result<Value, QueryError> {
    read_payload_limited(r, tag, inline_strings, io_err, budget, None)
}

/// A framed reader supplies the remaining record length so a corrupt value
/// cannot reserve more memory than there are bytes available in that record.
pub(crate) fn read_payload_limited<R: Read>(
    r: &mut R,
    tag: u8,
    inline_strings: bool,
    io_err: impl Fn(std::io::Error) -> QueryError + Copy,
    mut budget: Option<&mut super::mem_budget::ScopedCharge>,
    available: Option<u64>,
) -> Result<Value, QueryError> {
    Ok(match tag {
        VALUE_NULL => Value::Null,
        VALUE_INT => {
            let mut buf = [0u8; 8];
            r.read_exact(&mut buf).map_err(io_err)?;
            Value::Int(i64::from_le_bytes(buf))
        }
        VALUE_UINT => {
            let mut buf = [0u8; 8];
            r.read_exact(&mut buf).map_err(io_err)?;
            Value::UInt(u64::from_le_bytes(buf))
        }
        VALUE_FLOAT => {
            let mut buf = [0u8; 4];
            r.read_exact(&mut buf).map_err(io_err)?;
            Value::Float(f32::from_bits(u32::from_le_bytes(buf)))
        }
        VALUE_DOUBLE => {
            let mut buf = [0u8; 8];
            r.read_exact(&mut buf).map_err(io_err)?;
            Value::Double(f64::from_bits(u64::from_le_bytes(buf)))
        }
        VALUE_DECIMAL => Value::Decimal(
            String::from_utf8(
                read_bytes_accounted(r, budget.as_deref_mut(), available).map_err(io_err)?,
            )
            .map_err(|error| io_err(std::io::Error::new(std::io::ErrorKind::InvalidData, error)))?,
        ),
        VALUE_BYTES => {
            Value::Bytes(read_bytes_accounted(r, budget.as_deref_mut(), available).map_err(io_err)?)
        }
        VALUE_STRING => {
            let bytes =
                read_bytes_accounted(r, budget.as_deref_mut(), available).map_err(io_err)?;
            // Reject corrupt text in every execution lane. Preserve the batch
            // lane's inline representation without allocating a heap String.
            if inline_strings && bytes.len() <= crate::types::SMALL_STRING_INLINE_CAP {
                std::str::from_utf8(&bytes).map_err(|error| {
                    io_err(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
                })?;
                Value::SmallString(crate::types::SmallStringBuf::from_slice(&bytes))
            } else {
                Value::String(String::from_utf8(bytes).map_err(|error| {
                    io_err(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
                })?)
            }
        }
        VALUE_DECIMAL_COMPACT => {
            let mut buf = [0u8; 8];
            r.read_exact(&mut buf).map_err(io_err)?;
            let mag = i64::from_le_bytes(buf);
            let mut s = [0u8; 1];
            r.read_exact(&mut s).map_err(io_err)?;
            Value::DecimalCompact(mag, s[0])
        }
        VALUE_DECIMAL128 => {
            let mut buf = [0u8; 16];
            r.read_exact(&mut buf).map_err(io_err)?;
            let mut scale = [0u8; 1];
            r.read_exact(&mut scale).map_err(io_err)?;
            Value::Decimal128(i128::from_le_bytes(buf), scale[0])
        }
        VALUE_DECIMAL_WIDE => {
            let bytes =
                read_bytes_accounted(r, budget.as_deref_mut(), available).map_err(io_err)?;
            if let Some(budget) = budget.as_deref_mut() {
                budget.add_force(
                    bytes
                        .len()
                        .saturating_add(std::mem::size_of::<crate::types::DecimalWide>()),
                )?;
            }
            let mut scale = [0u8; 1];
            r.read_exact(&mut scale).map_err(io_err)?;
            Value::DecimalWide(Box::new(crate::types::DecimalWide {
                magnitude: num_bigint::BigInt::from_signed_bytes_le(&bytes),
                scale: scale[0],
            }))
        }
        VALUE_DATE => {
            let mut buf = [0u8; 4];
            r.read_exact(&mut buf).map_err(io_err)?;
            Value::Date(i32::from_le_bytes(buf))
        }
        VALUE_TIME => {
            let mut buf = [0u8; 8];
            r.read_exact(&mut buf).map_err(io_err)?;
            Value::Time(i64::from_le_bytes(buf))
        }
        VALUE_DATETIME => {
            let mut buf = [0u8; 8];
            r.read_exact(&mut buf).map_err(io_err)?;
            Value::DateTime(i64::from_le_bytes(buf))
        }
        VALUE_TIMESTAMP => {
            let mut buf = [0u8; 8];
            r.read_exact(&mut buf).map_err(io_err)?;
            Value::Timestamp(i64::from_le_bytes(buf))
        }
        VALUE_YEAR => {
            let mut buf = [0u8; 1];
            r.read_exact(&mut buf).map_err(io_err)?;
            Value::Year(buf[0])
        }
        VALUE_JSON => {
            Value::Json(read_bytes_accounted(r, budget.as_deref_mut(), available).map_err(io_err)?)
        }
        VALUE_GEOMETRY => Value::Geometry(
            read_bytes_accounted(r, budget.as_deref_mut(), available).map_err(io_err)?,
        ),
        VALUE_VECTOR => Value::Vector(
            read_bytes_accounted(r, budget.as_deref_mut(), available).map_err(io_err)?,
        ),
        other => {
            return Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                format!("invalid value tag in spill file: {}", other),
            ))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error(e: std::io::Error) -> QueryError {
        crate::storage::error(crate::error::catalog::ER_UNKNOWN_ERROR, e.to_string())
    }

    #[test]
    fn every_value_lane_round_trips_and_rejects_truncation() {
        let values = vec![
            Value::Null,
            Value::Int(i64::MIN),
            Value::UInt(u64::MAX),
            Value::Float(f32::from_bits(0x7fc00123)),
            Value::Double(-0.0),
            Value::Decimal("123.450".into()),
            Value::DecimalCompact(-123, 2),
            Value::Decimal128(i128::MIN, 10),
            Value::DecimalWide(Box::new(crate::types::DecimalWide {
                magnitude: num_bigint::BigInt::from(i128::MAX) * 1000,
                scale: 4,
            })),
            Value::String("短文本".into()),
            Value::String("wide".repeat(256)),
            Value::SmallString(crate::types::SmallStringBuf::from_slice(b"inline")),
            Value::Bytes(vec![0, 255]),
            Value::Date(crate::types::ZERO_DATE),
            Value::Time(-123),
            Value::DateTime(crate::types::ZERO_DATETIME),
            Value::Timestamp(12345678),
            Value::Year(99),
            Value::Json(vec![1, 2]),
            Value::Geometry(vec![3, 4]),
            Value::Vector(vec![5, 6]),
        ];
        for value in values {
            let mut encoded = Vec::new();
            write_value(&mut encoded, &value).unwrap();
            let mut batch_encoded = Vec::new();
            crate::engine::executor::vec::spill::write_value(&mut batch_encoded, &value).unwrap();
            assert_eq!(encoded, batch_encoded);
            for inline in [false, true] {
                let decoded = read_payload(&mut &encoded[1..], encoded[0], inline, error).unwrap();
                let mut replay = Vec::new();
                write_value(&mut replay, &decoded).unwrap();
                // Byte comparison also checks NaN payloads and signed zero.
                assert_eq!(replay, encoded, "{value:?}, inline={inline}");
            }
            for end in 0..encoded.len() {
                assert!(
                    crate::engine::executor::vec::spill::read_value(&mut &encoded[..end]).is_err(),
                    "accepted truncated {value:?} at {end}"
                );
            }
        }
        assert!(read_payload(&mut &[][..], 254, false, error).is_err());
    }
}
