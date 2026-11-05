//! Typed temporary columns retain validity and physical lanes across replay.

use super::super::bitmap::Bitmap;
use super::*;

const NULL: u8 = 0;
const BOOL: u8 = 1;
const I64: u8 = 2;
const U64: u8 = 3;
const F64: u8 = 4;
const DATE: u8 = 5;
const TIME: u8 = 6;
const DATETIME: u8 = 7;
const TIMESTAMP: u8 = 8;
const DECIMAL32: u8 = 9;
const DECIMAL64: u8 = 10;
const DECIMAL128: u8 = 11;
const UTF8: u8 = 12;
const BYTES: u8 = 13;
const JSON: u8 = 14;
const GEOMETRY: u8 = 15;

trait Fixed: Copy {
    const WIDTH: usize;
    fn encode(self, bytes: &mut [u8]);
    fn decode(bytes: &[u8]) -> Self;
}

macro_rules! fixed {
    ($($ty:ty),+) => {$(impl Fixed for $ty {
        const WIDTH: usize = std::mem::size_of::<Self>();
        fn encode(self, bytes: &mut [u8]) { bytes.copy_from_slice(&self.to_le_bytes()); }
        fn decode(bytes: &[u8]) -> Self { Self::from_le_bytes(bytes.try_into().expect("fixed spill width")) }
    })+};
}
fixed!(i32, u32, i64, u64, i128, f64);

fn invalid(message: &str) -> QueryError {
    io_err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))
}

fn write_fixed<W: Write, T: Fixed>(writer: &mut W, values: &[T]) -> Result<(), QueryError> {
    let mut bytes = [0u8; 8192];
    for chunk in values.chunks(bytes.len() / T::WIDTH) {
        crate::engine::cancel::check_cancelled()?;
        for (value, slot) in chunk.iter().zip(bytes.chunks_exact_mut(T::WIDTH)) {
            value.encode(slot);
        }
        writer
            .write_all(&bytes[..chunk.len() * T::WIDTH])
            .map_err(io_err)?;
    }
    Ok(())
}

fn read_fixed<R: Read, T: Fixed>(
    reader: &mut R,
    count: usize,
    charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
) -> Result<Vec<T>, QueryError> {
    let mut values = Vec::<T>::new();
    let mut bytes = [0u8; 8192];
    while values.len() < count {
        crate::engine::cancel::check_cancelled()?;
        let rows = (count - values.len()).min(bytes.len() / T::WIDTH);
        let chunk = &mut bytes[..rows * T::WIDTH];
        reader.read_exact(chunk).map_err(io_err)?;
        // Admit capacity only after a bounded read succeeds. A corrupt row
        // count must neither allocate nor reserve its unread payload.
        if values.capacity() - values.len() < rows {
            let old = values.capacity();
            let target = old.saturating_mul(2).max(values.len() + rows).min(count);
            charge.add_force((target - old).saturating_mul(T::WIDTH))?;
            // Reallocation may briefly retain both buffers.
            let mut overlap = charge.sibling();
            overlap.add_force(old.saturating_mul(T::WIDTH))?;
            values
                .try_reserve_exact(target - values.len())
                .map_err(|_| {
                    crate::engine::executor::mem_budget::allocation_error("typed spill lane")
                })?;
            if values.capacity() > target {
                charge.add_force((values.capacity() - target).saturating_mul(T::WIDTH))?;
            }
        }
        values.extend(chunk.chunks_exact(T::WIDTH).map(T::decode));
    }
    Ok(values)
}

fn byte<R: Read>(reader: &mut R) -> Result<u8, QueryError> {
    let mut value = [0];
    reader.read_exact(&mut value).map_err(io_err)?;
    Ok(value[0])
}

fn read_bitmap<R: Read>(
    reader: &mut R,
    rows: usize,
    charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
) -> Result<Bitmap, QueryError> {
    let words = read_fixed::<_, u64>(reader, rows.div_ceil(64), charge)?;
    Bitmap::from_words(words, rows).ok_or_else(|| invalid("invalid typed spill bitmap padding"))
}

fn valid_offsets(offsets: &[u32], data: &[u8], rows: usize) -> bool {
    offsets.len() == rows.saturating_add(1)
        && offsets.first() == Some(&0)
        && offsets
            .last()
            .is_some_and(|last| *last as usize == data.len())
        && offsets.windows(2).all(|pair| pair[0] <= pair[1])
}

pub(super) fn write_column<W: Write>(
    writer: &mut W,
    column: &ColumnVector,
    rows: usize,
) -> Result<bool, QueryError> {
    let (kind, nulls) = match column {
        ColumnVector::Null { .. } => (NULL, None),
        ColumnVector::Bool { nulls, .. } => (BOOL, nulls.as_ref()),
        ColumnVector::I64 { nulls, .. } => (I64, nulls.as_ref()),
        ColumnVector::U64 { nulls, .. } => (U64, nulls.as_ref()),
        ColumnVector::F64 { nulls, .. } => (F64, nulls.as_ref()),
        ColumnVector::Date32 { nulls, .. } => (DATE, nulls.as_ref()),
        ColumnVector::Time64 { nulls, .. } => (TIME, nulls.as_ref()),
        ColumnVector::DateTime64 { nulls, .. } => (DATETIME, nulls.as_ref()),
        ColumnVector::Timestamp64 { nulls, .. } => (TIMESTAMP, nulls.as_ref()),
        ColumnVector::Decimal32 { nulls, .. } => (DECIMAL32, nulls.as_ref()),
        ColumnVector::Decimal64 { nulls, .. } => (DECIMAL64, nulls.as_ref()),
        ColumnVector::Decimal128 { nulls, .. } => (DECIMAL128, nulls.as_ref()),
        ColumnVector::Utf8 { nulls, .. } => (UTF8, nulls.as_ref()),
        ColumnVector::Bytes { nulls, .. } => (BYTES, nulls.as_ref()),
        ColumnVector::Json { nulls, .. } => (JSON, nulls.as_ref()),
        ColumnVector::Geometry { nulls, .. } => (GEOMETRY, nulls.as_ref()),
        ColumnVector::DecimalWide { .. }
        | ColumnVector::Vector { .. }
        | ColumnVector::Generic { .. } => return Ok(false),
    };
    if column.len() != rows || nulls.is_some_and(|mask| mask.len() != rows) {
        return Err(invalid("typed spill column length mismatch"));
    }
    if let ColumnVector::Utf8 { offsets, data, .. }
    | ColumnVector::Bytes { offsets, data, .. }
    | ColumnVector::Json { offsets, data, .. }
    | ColumnVector::Geometry { offsets, data, .. } = column
    {
        if !valid_offsets(offsets, data, rows) {
            return Err(invalid("invalid typed spill offsets"));
        }
        if kind == UTF8
            && !std::str::from_utf8(data).is_ok_and(|text| {
                offsets
                    .iter()
                    .all(|offset| text.is_char_boundary(*offset as usize))
            })
        {
            // Ill-formed Utf8 lanes deliberately retain value_at's binary
            // fallback, including unused payload under a NULL position.
            return Ok(false);
        }
    }
    writer.write_all(&[BLOCK_TYPED, kind]).map_err(io_err)?;
    if kind == NULL {
        return Ok(true);
    }
    writer
        .write_all(&[u8::from(nulls.is_some())])
        .map_err(io_err)?;
    if let Some(mask) = nulls {
        write_fixed(writer, mask.words())?;
    }
    match column {
        ColumnVector::Bool { values, .. } => write_fixed(writer, values.words())?,
        ColumnVector::I64 { data, .. }
        | ColumnVector::Time64 { data, .. }
        | ColumnVector::DateTime64 { data, .. }
        | ColumnVector::Timestamp64 { data, .. } => write_fixed(writer, data)?,
        ColumnVector::U64 { data, .. } => write_fixed(writer, data)?,
        ColumnVector::F64 { data, .. } => write_fixed(writer, data)?,
        ColumnVector::Date32 { data, .. } => write_fixed(writer, data)?,
        ColumnVector::Decimal32 { data, scale, .. } => {
            writer.write_all(&[*scale]).map_err(io_err)?;
            write_fixed(writer, data)?;
        }
        ColumnVector::Decimal64 { data, scale, .. } => {
            writer.write_all(&[*scale]).map_err(io_err)?;
            write_fixed(writer, data)?;
        }
        ColumnVector::Decimal128 { data, scale, .. } => {
            writer.write_all(&[*scale]).map_err(io_err)?;
            write_fixed(writer, data)?;
        }
        ColumnVector::Utf8 { offsets, data, .. }
        | ColumnVector::Bytes { offsets, data, .. }
        | ColumnVector::Json { offsets, data, .. }
        | ColumnVector::Geometry { offsets, data, .. } => {
            write_fixed(writer, offsets)?;
            writer.write_all(data).map_err(io_err)?;
        }
        _ => unreachable!("typed spill kind selected before writing"),
    }
    Ok(true)
}

pub(super) fn read_column<R: Read>(
    reader: &mut R,
    rows: usize,
    charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
) -> Result<ColumnVector, QueryError> {
    let kind = byte(reader)?;
    if kind == NULL {
        return Ok(ColumnVector::Null { len: rows });
    }
    if !(BOOL..=GEOMETRY).contains(&kind) {
        return Err(invalid("unknown typed spill column kind"));
    }
    let nulls = match byte(reader)? {
        0 => None,
        1 => Some(read_bitmap(reader, rows, charge)?),
        _ => return Err(invalid("invalid typed spill null mask")),
    };
    macro_rules! lane {
        ($variant:ident,$ty:ty) => {
            ColumnVector::$variant {
                data: read_fixed::<_, $ty>(reader, rows, charge)?,
                nulls,
            }
        };
    }
    macro_rules! decimal {
        ($variant:ident,$ty:ty) => {{
            let scale = byte(reader)?;
            ColumnVector::$variant {
                data: read_fixed::<_, $ty>(reader, rows, charge)?,
                scale,
                nulls,
            }
        }};
    }
    Ok(match kind {
        BOOL => ColumnVector::Bool {
            values: read_bitmap(reader, rows, charge)?,
            nulls,
        },
        I64 => lane!(I64, i64),
        U64 => lane!(U64, u64),
        F64 => lane!(F64, f64),
        DATE => lane!(Date32, i32),
        TIME => lane!(Time64, i64),
        DATETIME => lane!(DateTime64, i64),
        TIMESTAMP => lane!(Timestamp64, i64),
        DECIMAL32 => decimal!(Decimal32, i32),
        DECIMAL64 => decimal!(Decimal64, i64),
        DECIMAL128 => decimal!(Decimal128, i128),
        UTF8 | BYTES | JSON | GEOMETRY => {
            let count = rows
                .checked_add(1)
                .ok_or_else(|| invalid("typed spill offset count overflow"))?;
            let offsets = read_fixed::<_, u32>(reader, count, charge)?;
            if offsets.first() != Some(&0) || !offsets.windows(2).all(|pair| pair[0] <= pair[1]) {
                return Err(invalid("invalid typed spill offsets"));
            }
            let length = *offsets.last().expect("at least one offset") as usize;
            let data = crate::engine::executor::spill_value::read_exact_bytes_accounted(
                reader,
                length,
                Some(charge),
            )
            .map_err(io_err)?;
            if kind == UTF8
                && !std::str::from_utf8(&data).is_ok_and(|text| {
                    offsets
                        .iter()
                        .all(|offset| text.is_char_boundary(*offset as usize))
                })
            {
                return Err(invalid("invalid typed spill UTF-8 lane"));
            }
            match kind {
                UTF8 => ColumnVector::Utf8 {
                    offsets,
                    data,
                    nulls,
                },
                BYTES => ColumnVector::Bytes {
                    offsets,
                    data,
                    nulls,
                },
                JSON => ColumnVector::Json {
                    offsets,
                    data,
                    nulls,
                },
                GEOMETRY => ColumnVector::Geometry {
                    offsets,
                    data,
                    nulls,
                },
                _ => unreachable!(),
            }
        }
        _ => unreachable!("typed spill kind validated"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget, ScopedCharge};

    #[test]
    fn truncated_typed_lane_does_not_charge_the_forged_row_count() {
        let budget = Arc::new(MemBudget::new(64 * 1024));
        with_query_budget(budget.clone(), || {
            let mut charge = ScopedCharge::new(0);
            assert!(read_column(&mut [F64, 0].as_slice(), 1_000_000, &mut charge).is_err());
            drop(charge);
            assert_eq!(budget.used(), 0);
            assert!(
                budget.peak_used() < 16 * 1024,
                "unread rows must not reserve megabytes"
            );
        });
    }

    #[test]
    fn typed_lane_rejects_unknown_kind_bad_mask_and_padding() {
        for payload in [vec![255], vec![U64, 2], {
            let mut bytes = vec![I64, 1];
            bytes.extend_from_slice(&8u64.to_le_bytes());
            bytes
        }] {
            assert!(read_column(&mut payload.as_slice(), 3, &mut ScopedCharge::new(0)).is_err());
        }
    }

    #[test]
    fn typed_text_rejects_bad_offsets_and_split_codepoints() {
        for offsets in [[0u32, 2, 1], [0, 1, 2]] {
            let mut bytes = vec![UTF8, 0];
            for offset in offsets {
                bytes.extend_from_slice(&offset.to_le_bytes());
            }
            bytes.extend_from_slice("é".as_bytes());
            assert!(read_column(&mut bytes.as_slice(), 2, &mut ScopedCharge::new(0)).is_err());
        }
    }

    #[test]
    fn nullable_float_crosses_bitmap_word_boundary_without_changing_bits() {
        let mut nulls = Bitmap::new_all_invalid(65);
        nulls.set(0, true);
        nulls.set(64, true);
        let mut data = vec![0.0; 65];
        data[0] = -0.0;
        data[64] = f64::from_bits(0x7ff8000000000042);
        let input = ColumnVector::F64 {
            data,
            nulls: Some(nulls),
        };
        let mut bytes = Vec::new();
        write_column(&mut bytes, &input, 65).unwrap();
        let output = read_column(&mut bytes[1..].as_ref(), 65, &mut ScopedCharge::new(0)).unwrap();
        for row in 0..65 {
            match (input.value_at(row), output.value_at(row)) {
                (crate::types::Value::Double(a), crate::types::Value::Double(b)) => {
                    assert_eq!(a.to_bits(), b.to_bits())
                }
                (a, b) => assert_eq!(a, b),
            }
        }
    }
}
