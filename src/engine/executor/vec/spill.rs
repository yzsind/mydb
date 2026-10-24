//! Phase 6 — columnar spill format.
//!
//! When a hash-aggregate / hash-join / sort exceeds its memory budget,
//! the operator must spill one or more partitions to disk. Phase 6
//! standardises the file layout so all spill consumers share the same
//! encode / decode helpers and so the loser-tree merge in external
//! sort can reuse the columnar reader.
//!
//! Format choices:
//! - Length-prefixed batch envelope (`num_rows`, `num_cols`, then one
//!   column block per column).
//! - Each column carries a layout tag. Native integer lanes use a base
//!   plus 1/2/4/8-byte deltas; native byte/string lanes store offsets and
//!   one contiguous payload. Mixed types and NULLs retain per-value tags.
//! - The tagged-value and delta-varint frame-of-reference blocks remain
//!   available for generic columns. Native blocks avoid per-row Value
//!   construction while retaining the same values and numeric precision.

use crate::types::{QueryError, Value};

use super::batch::{Batch, ColumnAccumulator, ColumnVector};
use super::schema::BatchSchema;
use std::io::{Read, Write};
use std::sync::Arc;

// Column-block format tags.
const BLOCK_RAW_VALUES: u8 = 0x10;
const BLOCK_INT64_FOR: u8 = 0x11; // frame-of-reference + delta-varint i64 column
const BLOCK_BYTES: u8 = 0x12;
const BLOCK_UTF8: u8 = 0x13;
const BLOCK_PACKED_I64: u8 = 0x14;

/// Write a `Batch` to `w` in the wire format described above. Each
/// column is preceded by a block tag that tells the reader how the
/// payload was encoded. Integer columns (i.e. every value is
/// `Value::Int` with no NULLs) automatically pick the
/// frame-of-reference path; mixed-type columns fall back to the raw
/// tagged-value list.
pub fn write_batch<W: Write>(w: &mut W, batch: &Batch) -> Result<(), QueryError> {
    if batch.schema().len() != batch.columns().len() {
        return Err(
            crate::error::catalog::ER_UNKNOWN_ERROR.error("spill batch schema width mismatch")
        );
    }
    if batch.selection().is_some() {
        let _scratch = crate::engine::executor::mem_budget::force_charge_scoped(
            batch.memory_bytes().saturating_add(256),
        )?;
        let indices = batch
            .visible_indices()
            .map(|row| row as u32)
            .collect::<Vec<_>>();
        let columns = batch
            .columns()
            .iter()
            .map(|column| column.take_indices(&indices))
            .collect();
        return write_batch(
            w,
            &Batch::with_row_count(batch.schema().clone(), columns, indices.len()),
        );
    }
    let n = batch.num_rows();
    let count = |value| {
        u32::try_from(value).map_err(|_| {
            crate::error::catalog::ER_UNKNOWN_ERROR.error("spill batch count exceeds its encoding")
        })
    };
    write_u32(w, count(n)?)?;
    write_u32(w, count(batch.columns().len())?)?;
    for c in batch.columns() {
        if let ColumnVector::Generic { values } = c {
            let size = values.iter().try_fold(0u32, |total, value| match value {
                Value::Bytes(bytes) => u32::try_from(bytes.len())
                    .ok()
                    .and_then(|len| total.checked_add(len)),
                _ => None,
            });
            if size.is_some() {
                // DISTINCT partials and other binary states can arrive as a
                // Generic lane. Borrow their payloads instead of cloning
                // every Value merely to serialize it again.
                w.write_all(&[BLOCK_BYTES]).map_err(io_err)?;
                let mut offset = 0u32;
                write_u32(w, offset)?;
                for value in values {
                    if let Value::Bytes(bytes) = value {
                        offset += bytes.len() as u32;
                    }
                    write_u32(w, offset)?;
                }
                for value in values {
                    if let Value::Bytes(bytes) = value {
                        w.write_all(bytes).map_err(io_err)?;
                    }
                }
                continue;
            }
        }
        if let ColumnVector::I64 { data, nulls: None } = c {
            w.write_all(&[BLOCK_PACKED_I64]).map_err(io_err)?;
            let base = data.iter().min().copied().unwrap_or(0);
            let max = data.iter().max().copied().unwrap_or(base);
            let delta = max.wrapping_sub(base) as u64;
            let width = if delta <= u8::MAX as u64 {
                1
            } else if delta <= u16::MAX as u64 {
                2
            } else if delta <= u32::MAX as u64 {
                4
            } else {
                8
            };
            w.write_all(&[width as u8]).map_err(io_err)?;
            w.write_all(&base.to_le_bytes()).map_err(io_err)?;
            // Spill runs at the memory frontier. Never allocate another
            // column-sized encoding buffer while its resident input is live.
            let mut bytes = [0u8; 8192];
            for chunk in data.chunks(bytes.len() / width) {
                for (slot, &value) in bytes.chunks_exact_mut(width).zip(chunk) {
                    slot.copy_from_slice(&(value.wrapping_sub(base) as u64).to_le_bytes()[..width]);
                }
                w.write_all(&bytes[..chunk.len() * width]).map_err(io_err)?;
            }
            continue;
        }
        let bytes = match c {
            ColumnVector::Bytes {
                offsets,
                data,
                nulls: None,
            } => Some((BLOCK_BYTES, offsets, data)),
            ColumnVector::Utf8 {
                offsets,
                data,
                nulls: None,
            } if std::str::from_utf8(data).is_ok_and(|text| {
                offsets
                    .iter()
                    .all(|&offset| text.is_char_boundary(offset as usize))
            }) =>
            {
                Some((BLOCK_UTF8, offsets, data))
            }
            _ => None,
        };
        if let Some((tag, offsets, data)) = bytes.filter(|(_, offsets, data)| {
            offsets.len() == n.saturating_add(1)
                && offsets.first() == Some(&0)
                && offsets
                    .last()
                    .is_some_and(|&last| last as usize == data.len())
        }) {
            w.write_all(&[tag]).map_err(io_err)?;
            for &offset in offsets {
                write_u32(w, offset)?;
            }
            w.write_all(data).map_err(io_err)?;
            continue;
        }
        if let ColumnVector::Generic { values } = c {
            if values.iter().all(|value| matches!(value, Value::Int(_))) {
                w.write_all(&[BLOCK_INT64_FOR]).map_err(io_err)?;
                let ints = values.iter().map(|value| match value {
                    Value::Int(value) => *value,
                    _ => unreachable!(),
                });
                write_int64_for_iter(w, ints)?;
            } else {
                w.write_all(&[BLOCK_RAW_VALUES]).map_err(io_err)?;
                for value in values {
                    write_value(w, value)?;
                }
            }
        } else {
            w.write_all(&[BLOCK_RAW_VALUES]).map_err(io_err)?;
            for row in 0..n {
                write_value(w, &c.value_at(row))?;
            }
        }
    }
    Ok(())
}

/// Read a `Batch` previously written by `write_batch`.
#[cfg(test)]
pub fn read_batch<R: Read>(r: &mut R, schema: Arc<BatchSchema>) -> Result<Batch, QueryError> {
    let nrows = read_u32(r)? as usize;
    read_batch_body(
        r,
        schema,
        nrows,
        crate::engine::executor::mem_budget::ScopedCharge::new(0),
    )
}

/// Read the next batch from a file containing concatenated spill batches.
/// Clean EOF before a new batch returns `None`; a partial prefix remains a
/// hard spill-I/O error so truncated temp files are never silently accepted.
#[cfg(test)]
pub fn read_batch_opt<R: Read>(
    r: &mut R,
    schema: Arc<BatchSchema>,
) -> Result<Option<Batch>, QueryError> {
    let mut prefix = [0u8; 4];
    let first = r.read(&mut prefix[..1]).map_err(io_err)?;
    if first == 0 {
        return Ok(None);
    }
    r.read_exact(&mut prefix[1..]).map_err(io_err)?;
    let nrows = u32::from_le_bytes(prefix) as usize;
    read_batch_body(
        r,
        schema,
        nrows,
        crate::engine::executor::mem_budget::ScopedCharge::new(0),
    )
    .map(Some)
}

pub(crate) fn read_owned_batch(
    reader: &mut crate::engine::executor::spill_quota::TrackedSpillReader,
    schema: Arc<BatchSchema>,
) -> Result<Option<Batch>, QueryError> {
    let Some(nrows) =
        crate::engine::executor::spill_value::read_optional_u32(reader).map_err(io_err)?
    else {
        return Ok(None);
    };
    let charge = reader.memory_charge();
    read_batch_body(reader, schema, nrows as usize, charge).map(Some)
}

fn reserve_decoded<T>(values: &mut Vec<T>, count: usize, limit: usize) -> Result<(), QueryError> {
    if values.capacity() - values.len() < count {
        let target = values
            .capacity()
            .saturating_mul(2)
            .max(values.len().saturating_add(count))
            .min(limit);
        crate::engine::executor::mem_budget::try_reserve_vec_exact(
            values,
            target - values.len(),
            "spill decoded vector",
        )?;
    }
    Ok(())
}

fn read_batch_body<R: Read>(
    r: &mut R,
    schema: Arc<BatchSchema>,
    nrows: usize,
    mut charge: crate::engine::executor::mem_budget::ScopedCharge,
) -> Result<Batch, QueryError> {
    use crate::engine::executor::mem_budget;
    let ncols = read_u32(r)? as usize;
    if ncols != schema.len() {
        return Err(crate::error::catalog::ER_UNKNOWN_ERROR
            .error(format!("spill schema width {ncols} != {}", schema.len())));
    }
    charge.add_force(
        ncols
            .saturating_mul(std::mem::size_of::<ColumnVector>())
            .saturating_add(128),
    )?;
    let mut columns = mem_budget::try_vec_with_capacity(ncols, "spill decoded columns")?;
    for column in 0..ncols {
        crate::engine::cancel::check_cancelled()?;
        let mut tag = [0u8; 1];
        r.read_exact(&mut tag).map_err(io_err)?;
        match tag[0] {
            BLOCK_RAW_VALUES => {
                charge.add_force(nrows.saturating_mul(std::mem::size_of::<Value>()))?;
                let mut col_values =
                    mem_budget::try_vec_with_capacity(nrows.min(1024), "spill raw cells")?;
                for row in 0..nrows {
                    if row % 1024 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let value = read_value_accounted(r, &mut charge)?;
                    reserve_decoded(&mut col_values, 1, nrows)?;
                    col_values.push(value);
                }
                // from_values may retain a typed mirror while the raw cells
                // still own their payload. Admit that conversion first.
                charge.add_force(mem_budget::estimate_row_bytes(&col_values))?;
                columns.push(column_from_spill_values(
                    schema.fields().get(column),
                    col_values,
                ));
            }
            BLOCK_INT64_FOR => {
                charge.add_force(nrows.saturating_mul(8))?;
                let ints = read_int64_for(r, nrows)?;
                columns.push(ColumnVector::I64 {
                    data: ints,
                    nulls: None,
                });
            }
            BLOCK_PACKED_I64 => {
                let (width, base) = read_packed_header(r)?;
                charge.add_force(nrows.saturating_mul(8))?;
                let mut data =
                    mem_budget::try_vec_with_capacity(nrows.min(1024), "spill integer lane")?;
                let mut bytes = [0u8; 8192];
                while data.len() < nrows {
                    crate::engine::cancel::check_cancelled()?;
                    let count = (nrows - data.len()).min(bytes.len() / width);
                    let chunk = &mut bytes[..count * width];
                    r.read_exact(chunk).map_err(io_err)?;
                    reserve_decoded(&mut data, count, nrows)?;
                    data.extend(
                        chunk
                            .chunks_exact(width)
                            .map(|value| unpack_integer(value, base)),
                    );
                }
                columns.push(ColumnVector::I64 { data, nulls: None });
            }
            BLOCK_BYTES | BLOCK_UTF8 => {
                let offset_count = nrows
                    .checked_add(1)
                    .ok_or_else(|| mem_budget::allocation_error("spill offset count overflow"))?;
                charge.add_force(offset_count.saturating_mul(4))?;
                let mut offsets = mem_budget::try_vec_with_capacity(
                    offset_count.min(1024),
                    "spill byte offsets",
                )?;
                for _ in 0..offset_count {
                    let offset = read_u32(r)?;
                    if offsets.last().is_some_and(|&previous| previous > offset)
                        || (offsets.is_empty() && offset != 0)
                    {
                        return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                            .error("invalid spill byte offsets"));
                    }
                    reserve_decoded(&mut offsets, 1, offset_count)?;
                    offsets.push(offset);
                }
                let data = crate::engine::executor::spill_value::read_exact_bytes_accounted(
                    r,
                    offsets[nrows] as usize,
                    Some(&mut charge),
                )
                .map_err(io_err)?;
                columns.push(if tag[0] == BLOCK_UTF8 {
                    if !std::str::from_utf8(&data).is_ok_and(|text| {
                        offsets
                            .iter()
                            .all(|&offset| text.is_char_boundary(offset as usize))
                    }) {
                        return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                            .error("invalid spill UTF-8 lane"));
                    }
                    ColumnVector::Utf8 {
                        offsets,
                        data,
                        nulls: None,
                    }
                } else {
                    ColumnVector::Bytes {
                        offsets,
                        data,
                        nulls: None,
                    }
                });
            }
            other => {
                return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                    .error(format!("unknown spill block tag {other:#x}")))
            }
        }
    }
    let batch = Batch::with_row_count(schema, columns, nrows);
    let resident = batch
        .memory_bytes()
        .saturating_add(ncols.saturating_mul(std::mem::size_of::<ColumnVector>()))
        .saturating_add(128);
    if resident > charge.charged() {
        charge.add_force(resident - charge.charged())?;
    } else {
        charge.release_bytes(charge.charged() - resident);
    }
    batch.with_memory_charge(charge)
}

fn read_packed_header<R: Read>(reader: &mut R) -> Result<(usize, i64), QueryError> {
    let mut width = [0];
    reader.read_exact(&mut width).map_err(io_err)?;
    if !matches!(width[0], 1 | 2 | 4 | 8) {
        return Err(crate::error::catalog::ER_UNKNOWN_ERROR.error("invalid spill integer width"));
    }
    let mut base = [0; 8];
    reader.read_exact(&mut base).map_err(io_err)?;
    Ok((width[0] as usize, i64::from_le_bytes(base)))
}

fn unpack_integer(bytes: &[u8], base: i64) -> i64 {
    let mut delta = [0; 8];
    delta[..bytes.len()].copy_from_slice(bytes);
    base.wrapping_add(u64::from_le_bytes(delta) as i64)
}

/// Restore temporal lanes from the declared spill schema. Value-only lane
/// inference deliberately leaves DATE/TIME/DATETIME as `Generic`; that is
/// safe for semantics but disables fixed-width hash-aggregate replay after a
/// spill. Restrict schema-guided reconstruction to temporal types so aggregate
/// transport placeholders (notably exact DECIMAL SUM stored under a `Double`
/// spill field) keep their actual tagged value and precision.
fn column_from_spill_values(
    field: Option<&super::schema::BatchField>,
    values: Vec<Value>,
) -> ColumnVector {
    // Generic values carry their runtime tags. Promoting tagged Float cells
    // to an F64 lane makes the output depend on which values happened to
    // share a spill block, and loses FLOAT NaN payload bits.
    if values.iter().any(|value| matches!(value, Value::Float(_))) {
        return ColumnVector::Generic { values };
    }
    let Some(field) = field else {
        return ColumnVector::from_values(values);
    };
    if !matches!(
        field.column_type,
        crate::types::ColumnType::Date
            | crate::types::ColumnType::Time
            | crate::types::ColumnType::DateTime
            | crate::types::ColumnType::Timestamp
    ) {
        return ColumnVector::from_values(values);
    }
    let mut accumulator = ColumnAccumulator::for_field(field, values.len());
    for value in &values {
        accumulator.push(value);
    }
    accumulator.finish()
}

/// Returns Some(values) when every entry is `Value::Int`. Used to pick
/// the frame-of-reference encoding path.
/// Frame-of-reference + zigzag-varint i64 column encoder.
/// Layout: `[base (i64 LE)] [count (u32 LE)] [varint(delta_zigzag)]*`.
#[cfg(test)]
fn write_int64_for<W: Write>(w: &mut W, values: &[i64]) -> Result<(), QueryError> {
    write_int64_for_iter(w, values.iter().copied())
}

fn write_int64_for_iter<W: Write>(
    w: &mut W,
    values: impl ExactSizeIterator<Item = i64> + Clone,
) -> Result<(), QueryError> {
    let base = values.clone().min().unwrap_or(0);
    w.write_all(&base.to_le_bytes()).map_err(io_err)?;
    write_u32(w, values.len() as u32)?;
    for value in values {
        write_zigzag_varint(w, value.wrapping_sub(base))?;
    }
    Ok(())
}

fn read_int64_for<R: Read>(r: &mut R, nrows: usize) -> Result<Vec<i64>, QueryError> {
    let mut base_bytes = [0u8; 8];
    r.read_exact(&mut base_bytes).map_err(io_err)?;
    let base = i64::from_le_bytes(base_bytes);
    let count = read_u32(r)? as usize;
    if count != nrows {
        return Err(crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            format!("spill int64-FOR count {count} mismatches row count {nrows}"),
        ));
    }
    let mut out = crate::engine::executor::mem_budget::try_vec_with_capacity(
        nrows.min(1024),
        "spill FOR lane",
    )?;
    for _ in 0..nrows {
        let delta = read_zigzag_varint(r)?;
        reserve_decoded(&mut out, 1, nrows)?;
        out.push(base.wrapping_add(delta));
    }
    Ok(out)
}

fn write_zigzag_varint<W: Write>(w: &mut W, value: i64) -> Result<(), QueryError> {
    // ZigZag-encode signed → unsigned to keep the varint short for
    // small negative deltas as well as positive ones.
    let mut n = ((value << 1) ^ (value >> 63)) as u64;
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            w.write_all(&[byte]).map_err(io_err)?;
            return Ok(());
        } else {
            w.write_all(&[byte | 0x80]).map_err(io_err)?;
        }
    }
}

fn read_zigzag_varint<R: Read>(r: &mut R) -> Result<i64, QueryError> {
    let mut n: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let mut byte = [0u8; 1];
        r.read_exact(&mut byte).map_err(io_err)?;
        n |= ((byte[0] & 0x7f) as u64) << shift;
        if byte[0] & 0x80 == 0 {
            // ZigZag decode.
            let signed = (n >> 1) as i64 ^ -((n & 1) as i64);
            return Ok(signed);
        }
        shift += 7;
        if shift >= 64 {
            return Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "spill varint overflow",
            ));
        }
    }
}

fn write_u32<W: Write>(w: &mut W, n: u32) -> Result<(), QueryError> {
    w.write_all(&n.to_le_bytes()).map_err(io_err)
}

fn read_u32<R: Read>(r: &mut R) -> Result<u32, QueryError> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b).map_err(io_err)?;
    Ok(u32::from_le_bytes(b))
}

pub(crate) fn write_value<W: Write>(w: &mut W, v: &Value) -> Result<(), QueryError> {
    crate::engine::executor::spill_value::write_value(w, v).map_err(io_err)
}

pub(crate) fn read_value_accounted<R: Read>(
    r: &mut R,
    charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
) -> Result<Value, QueryError> {
    let mut tag = [0];
    r.read_exact(&mut tag).map_err(io_err)?;
    crate::engine::executor::spill_value::read_payload_accounted(
        r,
        tag[0],
        true,
        io_err,
        Some(charge),
    )
}

pub(crate) fn read_value<R: Read>(r: &mut R) -> Result<Value, QueryError> {
    let mut tag = [0u8; 1];
    r.read_exact(&mut tag).map_err(io_err)?;
    crate::engine::executor::spill_value::read_payload(r, tag[0], true, io_err)
}

fn io_err(e: std::io::Error) -> QueryError {
    if let Some(error) = crate::engine::executor::spill_quota::query_error_from_io(&e) {
        return error;
    }
    crate::storage::error(
        crate::error::catalog::ER_UNKNOWN_ERROR,
        format!("spill io: {e}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::vec::batch::ColumnVector;
    use crate::engine::executor::vec::schema::BatchField;
    use crate::types::ColumnType;

    #[test]
    fn spill_value_rejects_corrupt_text() {
        // Mutate only payload bytes: both codecs must reject invalid UTF-8,
        // rather than silently changing a spilled SQL value during replay.
        for value in [Value::String("x".into()), Value::Decimal("1".into())] {
            let mut bytes = Vec::new();
            write_value(&mut bytes, &value).unwrap();
            *bytes.last_mut().unwrap() = 0xff;
            assert!(read_value(&mut bytes.as_slice()).is_err());
        }
    }

    #[test]
    fn typed_spill_lanes_match_value_codec_and_reject_truncation() {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: String::new(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::Blob,
            source: Default::default(),
        }]));
        for values in [
            vec![Value::Int(i64::MIN), Value::Int(0), Value::Int(i64::MAX)],
            vec![Value::Int(0), Value::Int(255), Value::Int(100)],
            vec![Value::Int(-32768), Value::Int(32767), Value::Int(0)],
            vec![
                Value::Int(i32::MIN as i64),
                Value::Int(i32::MAX as i64),
                Value::Int(0),
            ],
            vec![
                Value::Bytes(vec![]),
                Value::Bytes(vec![0, 255, 128]),
                Value::Bytes(vec![7; 4096]),
            ],
            vec![
                Value::String(String::new()),
                Value::String("é中🙂".into()),
                Value::String("long".repeat(1000)),
            ],
            vec![Value::Null, Value::Bytes(vec![]), Value::Bytes(vec![255])],
            vec![Value::Int(7), Value::String("mixed".into()), Value::Null],
        ] {
            let column = if values.iter().all(|value| matches!(value, Value::Bytes(_))) {
                let mut offsets = vec![0];
                let mut data = Vec::new();
                for value in &values {
                    if let Value::Bytes(bytes) = value {
                        data.extend_from_slice(bytes);
                    }
                    offsets.push(data.len() as u32);
                }
                ColumnVector::Bytes {
                    offsets,
                    data,
                    nulls: None,
                }
            } else {
                ColumnVector::from_values(values.clone())
            };
            let batch = Batch::new(schema.clone(), vec![column]);
            let mut encoded = Vec::new();
            write_batch(&mut encoded, &batch).unwrap();
            let decoded = read_batch(&mut std::io::Cursor::new(&encoded), schema.clone()).unwrap();
            let mut legacy = Vec::new();
            write_u32(&mut legacy, values.len() as u32).unwrap();
            write_u32(&mut legacy, 1).unwrap();
            legacy.push(BLOCK_RAW_VALUES);
            for value in &values {
                write_value(&mut legacy, value).unwrap();
            }
            let oracle = read_batch(&mut std::io::Cursor::new(legacy), schema.clone()).unwrap();
            for row in 0..values.len() {
                assert_eq!(
                    decoded.columns()[0].value_at(row),
                    oracle.columns()[0].value_at(row)
                );
            }
            encoded.pop();
            assert!(read_batch(&mut std::io::Cursor::new(encoded), schema.clone()).is_err());
        }
        // An ill-formed Utf8 lane must retain value_at's replacement semantics.
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::Utf8 {
                offsets: vec![0, 1],
                data: vec![255],
                nulls: None,
            }],
        );
        let mut encoded = Vec::new();
        write_batch(&mut encoded, &batch).unwrap();
        let decoded = read_batch(&mut std::io::Cursor::new(encoded), schema).unwrap();
        assert_eq!(
            decoded.columns()[0].value_at(0),
            batch.columns()[0].value_at(0)
        );
    }

    #[test]
    fn typed_spill_rejects_invalid_offsets_and_utf8_boundaries() {
        let schema = Arc::new(BatchSchema::new(vec![super::super::schema::BatchField {
            table: String::new(),
            alias: None,
            name: "v".into(),
            column_type: crate::types::ColumnType::Blob,
            source: Default::default(),
        }]));
        for (tag, offsets, bytes) in [
            (BLOCK_BYTES, vec![1, 2], vec![0, 0]),
            (BLOCK_BYTES, vec![0, 3, 2], vec![0, 0]),
            (BLOCK_UTF8, vec![0, 1, 2], "é".as_bytes().to_vec()),
        ] {
            let mut encoded = Vec::new();
            write_u32(&mut encoded, offsets.len() as u32 - 1).unwrap();
            write_u32(&mut encoded, 1).unwrap();
            encoded.push(tag);
            for offset in offsets {
                write_u32(&mut encoded, offset).unwrap();
            }
            encoded.extend(bytes);
            assert!(read_batch(&mut std::io::Cursor::new(encoded), schema.clone()).is_err());
        }
    }

    #[test]
    fn packed_spill_streams_across_chunk_boundaries_for_every_width() {
        struct BoundedIo<T>(T);
        impl<T: Write> Write for BoundedIo<T> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                assert!(bytes.len() <= 8192, "unbounded spill encoding buffer");
                self.0.write(bytes)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                self.0.flush()
            }
        }
        impl<T: Read> Read for BoundedIo<T> {
            fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                assert!(bytes.len() <= 8192, "unbounded spill decoding buffer");
                self.0.read(bytes)
            }
        }
        let schema = Arc::new(BatchSchema::new(vec![super::super::schema::BatchField {
            table: String::new(),
            alias: None,
            name: "v".into(),
            column_type: crate::types::ColumnType::Blob,
            source: Default::default(),
        }]));
        for maximum in [255, 65535, u32::MAX as i64, i64::MAX] {
            let values: Vec<i64> = (0..17003)
                .map(|i| if i % 2 == 0 { 0 } else { maximum })
                .collect();
            let batch = Batch::new(
                schema.clone(),
                vec![ColumnVector::I64 {
                    data: values.clone(),
                    nulls: None,
                }],
            );
            let mut writer = BoundedIo(Vec::new());
            write_batch(&mut writer, &batch).unwrap();
            let mut reader = BoundedIo(std::io::Cursor::new(writer.0));
            let decoded = read_batch(&mut reader, schema.clone()).unwrap();
            for (i, expected) in values.iter().enumerate() {
                assert_eq!(decoded.columns()[0].value_at(i), Value::Int(*expected));
            }
        }
    }

    #[test]
    fn zigzag_varint_roundtrips_all_widths() {
        for v in [0i64, 1, -1, 127, -127, i64::MIN, i64::MAX, 12345, -67890] {
            let mut buf = Vec::new();
            write_zigzag_varint(&mut buf, v).unwrap();
            let mut cur = std::io::Cursor::new(buf);
            let back = read_zigzag_varint(&mut cur).unwrap();
            assert_eq!(back, v, "varint roundtrip failed for {v}");
        }
    }

    #[test]
    fn frame_of_reference_compresses_dense_integers() {
        // 1000 monotonically increasing i64s starting at a huge base
        // — the deltas should all fit in a single varint byte.
        let values: Vec<i64> = (10_000_000i64..10_001_000).collect();
        let mut buf = Vec::new();
        write_int64_for(&mut buf, &values).unwrap();
        // Header (8 base + 4 count = 12 bytes) + 1000 single-byte
        // varints = ~1012 bytes. Without FOR each Value would consume
        // 9 bytes (tag + i64), giving 9000 bytes. ≥ 8× compression.
        assert!(
            buf.len() < values.len() * 2,
            "expected ≥ 4.5× compression, got {} bytes for {} ints",
            buf.len(),
            values.len()
        );
        let mut cur = std::io::Cursor::new(buf);
        let back = read_int64_for(&mut cur, values.len()).unwrap();
        assert_eq!(back, values);
    }

    #[test]
    fn batch_writer_routes_int_columns_through_for() {
        let s = Arc::new(BatchSchema::new(vec![BatchField {
            table: String::new(),
            alias: None,
            name: "k".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }]));
        let ints: Vec<Value> = (0i64..1000).map(Value::Int).collect();
        let b = Batch::new(s.clone(), vec![ColumnVector::from_values(ints.clone())]);
        let mut buf = Vec::new();
        write_batch(&mut buf, &b).unwrap();
        let mut cur = std::io::Cursor::new(buf);
        let r = read_batch(&mut cur, s).unwrap();
        assert_eq!(r.num_rows(), 1000);
        for i in 0..1000 {
            assert_eq!(r.columns()[0].value_at(i), Value::Int(i as i64));
        }
    }

    #[test]
    fn roundtrip_simple_batch() {
        let s = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: String::new(),
                alias: None,
                name: "k".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: String::new(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::VarString,
                source: Default::default(),
            },
        ]));
        let b = Batch::new(
            s.clone(),
            vec![
                ColumnVector::from_values(vec![Value::Int(1), Value::Int(2), Value::Null]),
                ColumnVector::from_values(vec![
                    Value::String("a".into()),
                    Value::Null,
                    Value::String("c".into()),
                ]),
            ],
        );
        let mut buf = Vec::new();
        write_batch(&mut buf, &b).unwrap();
        let mut cur = std::io::Cursor::new(buf);
        let r = read_batch(&mut cur, s).unwrap();
        assert_eq!(r.num_rows(), 3);
        assert_eq!(r.columns()[0].value_at(0), Value::Int(1));
        assert!(r.columns()[1].is_null(1));
        let value = r.columns()[1].value_at(2);
        assert_eq!(crate::types::value_str(&value), Some("c"));
    }

    #[test]
    fn roundtrip_decimal128_and_wide_binary_lanes() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: String::new(),
                alias: None,
                name: "d128".into(),
                column_type: ColumnType::Decimal {
                    precision: 38,
                    scale: 2,
                },
                source: Default::default(),
            },
            BatchField {
                table: String::new(),
                alias: None,
                name: "d65".into(),
                column_type: ColumnType::Decimal {
                    precision: 65,
                    scale: 0,
                },
                source: Default::default(),
            },
        ]));
        let wide = Value::DecimalWide(Box::new(crate::types::DecimalWide {
            magnitude: num_bigint::BigInt::parse_bytes(
                b"99999999999999999999999999999999999999999999999999999999999999999",
                10,
            )
            .unwrap(),
            scale: 0,
        }));
        let batch = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::from_values(vec![Value::Decimal128(i128::MAX, 2)]),
                ColumnVector::from_values(vec![wide.clone()]),
            ],
        );
        let mut bytes = Vec::new();
        write_batch(&mut bytes, &batch).unwrap();
        let decoded = read_batch(&mut std::io::Cursor::new(bytes), schema).unwrap();
        assert_eq!(
            decoded.columns()[0].value_at(0),
            Value::Decimal128(i128::MAX, 2)
        );
        assert_eq!(decoded.columns()[1].value_at(0), wide);
    }

    #[test]
    fn reads_concatenated_batches_until_clean_eof() {
        let s = Arc::new(BatchSchema::new(vec![BatchField {
            table: String::new(),
            alias: None,
            name: "k".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }]));
        let first = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Int(1),
                Value::Int(2),
            ])],
        );
        let second = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![Value::Int(3)])],
        );
        let mut bytes = Vec::new();
        write_batch(&mut bytes, &first).unwrap();
        write_batch(&mut bytes, &second).unwrap();
        let mut cursor = std::io::Cursor::new(bytes);
        assert_eq!(
            read_batch_opt(&mut cursor, s.clone())
                .unwrap()
                .unwrap()
                .num_rows(),
            2
        );
        assert_eq!(
            read_batch_opt(&mut cursor, s.clone())
                .unwrap()
                .unwrap()
                .num_rows(),
            1
        );
        assert!(read_batch_opt(&mut cursor, s).unwrap().is_none());
    }
}

#[cfg(test)]
mod spill_decode_regressions {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::engine::executor::vec::schema::BatchField;

    #[test]
    fn decoded_batch_retains_its_memory_owner_after_reader_drop() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let budget = Arc::new(MemBudget::new(8 * 1024 * 1024));
        with_query_budget(budget.clone(), || {
            let schema = Arc::new(BatchSchema::new(vec![BatchField {
                table: "t".into(),
                alias: None,
                name: "v".into(),
                column_type: crate::types::ColumnType::VarString,
                source: Default::default(),
            }]));
            let dir = crate::engine::executor::select::grace::SpillTmpDir::create_kind(
                root.path(),
                "audit",
            )
            .unwrap();
            let path = dir.path().join("batch");
            let mut writer = crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
                &path,
                dir.charger(),
                32 * 1024,
            )
            .unwrap();
            let input = Batch::new(
                schema.clone(),
                vec![ColumnVector::from_values(vec![
                    Value::String(
                        "owned".repeat(128)
                    );
                    32
                ])],
            );
            write_batch(&mut writer, &input).unwrap();
            writer.finish().unwrap();
            drop(input);
            let mut reader = crate::engine::executor::spill_quota::TrackedSpillReader::open(
                &path,
                dir.charger(),
                32 * 1024,
            )
            .unwrap();
            let batch = read_batch_opt(&mut reader, schema).unwrap().unwrap();
            drop(reader);
            drop(dir);
            assert!(
                budget.used() >= batch.memory_bytes(),
                "decoded payload lost its reservation"
            );
            let view = batch.clone();
            let bytes = budget.used();
            drop(batch);
            assert_eq!(
                budget.used(),
                bytes,
                "shared view must retain the same owner"
            );
            drop(view);
            assert_eq!(budget.used(), 0);
        });
    }

    #[test]
    fn column_free_spill_batch_preserves_row_count() {
        let schema = Arc::new(BatchSchema::new(vec![]));
        let input = Batch::with_row_count(schema.clone(), vec![], 17);
        let mut bytes = Vec::new();
        write_batch(&mut bytes, &input).unwrap();
        let output = read_batch_opt(&mut bytes.as_slice(), schema)
            .unwrap()
            .unwrap();
        assert_eq!(output.num_rows(), 17);
    }

    #[test]
    fn malformed_schema_and_large_truncated_value_return_errors() {
        let schema = Arc::new(BatchSchema::new(vec![]));
        let mut bytes = Vec::new();
        write_u32(&mut bytes, 1).unwrap();
        write_u32(&mut bytes, 1).unwrap();
        assert!(read_batch_opt(&mut bytes.as_slice(), schema).is_err());
        let mut bytes = vec![crate::engine::executor::spill_value::VALUE_BYTES];
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(read_value(&mut bytes.as_slice()).is_err());
    }

    #[test]
    fn selected_spill_preserves_duplicates_and_visible_order() {
        use crate::engine::executor::vec::batch::Selection;
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: String::new(),
            alias: None,
            name: "v".into(),
            column_type: crate::types::ColumnType::LongLong,
            source: Default::default(),
        }]));
        let input = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Int(7),
                Value::Null,
                Value::Int(9),
            ])],
        )
        .with_selection(Selection::Indices(vec![2, 0, 2, 1]));
        let mut bytes = Vec::new();
        write_batch(&mut bytes, &input).unwrap();
        let output = read_batch_opt(&mut bytes.as_slice(), schema)
            .unwrap()
            .unwrap();
        assert_eq!(output.num_rows(), 4);
        assert_eq!(
            (0..4).map(|row| output.row(row)).collect::<Vec<_>>(),
            vec![
                vec![Value::Int(9)],
                vec![Value::Int(7)],
                vec![Value::Int(9)],
                vec![Value::Null]
            ]
        );
    }

    #[test]
    fn decoded_columns_keep_owner_through_moves_and_copy_on_write() {
        let budget = Arc::new(MemBudget::new(8 * 1024 * 1024));
        with_query_budget(budget.clone(), || {
            let schema = Arc::new(BatchSchema::new(vec![BatchField {
                table: String::new(),
                alias: None,
                name: "v".into(),
                column_type: crate::types::ColumnType::VarString,
                source: Default::default(),
            }]));
            let input = Batch::new(
                schema.clone(),
                vec![ColumnVector::from_values(vec![
                    Value::String(
                        "owned".repeat(128)
                    );
                    32
                ])],
            );
            let mut bytes = Vec::new();
            write_batch(&mut bytes, &input).unwrap();
            let batch = read_batch_opt(&mut bytes.as_slice(), schema.clone())
                .unwrap()
                .unwrap();
            let original = budget.used();
            let mut copy = batch.clone();
            let columns = copy.take_columns().unwrap();
            let moved = Batch::from_owned_columns(schema, columns);
            assert!(budget.used() > original);
            assert_eq!(batch.row(0), moved.row(0));
            drop(batch);
            assert!(budget.used() >= moved.memory_bytes());
            drop(moved);
            drop(copy);
            assert_eq!(budget.used(), 0);
        });
    }
}
