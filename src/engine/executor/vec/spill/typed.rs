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
const DECIMAL32_PACKED: u8 = 16;
const DECIMAL64_PACKED: u8 = 17;

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
    // Radix partitions often emit short columns. Clearing an 8 KiB scratch
    // for a few values can cost more than the entire encoded payload.
    match values.len().saturating_mul(T::WIDTH) {
        0..=256 => write_fixed_buffer::<W, T, 256>(writer, values),
        257..=1024 => write_fixed_buffer::<W, T, 1024>(writer, values),
        _ => write_fixed_buffer::<W, T, 8192>(writer, values),
    }
}

fn write_fixed_buffer<W: Write, T: Fixed, const N: usize>(
    writer: &mut W,
    values: &[T],
) -> Result<(), QueryError> {
    let mut bytes = [0u8; N];
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

trait PackedSigned: Fixed {
    fn signed(self) -> i64;
    fn from_signed(value: i64) -> Result<Self, QueryError>;
}
impl PackedSigned for i64 {
    fn signed(self) -> i64 {
        self
    }
    fn from_signed(value: i64) -> Result<Self, QueryError> {
        Ok(value)
    }
}
impl PackedSigned for i32 {
    fn signed(self) -> i64 {
        i64::from(self)
    }
    fn from_signed(value: i64) -> Result<Self, QueryError> {
        i32::try_from(value).map_err(|_| invalid("packed decimal coefficient exceeds its lane"))
    }
}

fn packed_bounds<T: PackedSigned>(values: &[T]) -> Option<(i64, usize)> {
    let base = values.iter().map(|value| value.signed()).min()?;
    let max = values.iter().map(|value| value.signed()).max()?;
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
    // Avoid adding a base/header to tiny or incompressible columns.
    (values.len().saturating_mul(T::WIDTH) > values.len().saturating_mul(width).saturating_add(9))
        .then_some((base, width))
}

fn write_packed_signed<W: Write, T: PackedSigned>(
    writer: &mut W,
    values: &[T],
    base: i64,
    width: usize,
) -> Result<(), QueryError> {
    writer.write_all(&[width as u8]).map_err(io_err)?;
    writer.write_all(&base.to_le_bytes()).map_err(io_err)?;
    match values.len().saturating_mul(width) {
        0..=256 => write_packed_signed_buffer::<W, T, 256>(writer, values, base, width),
        257..=1024 => write_packed_signed_buffer::<W, T, 1024>(writer, values, base, width),
        _ => write_packed_signed_buffer::<W, T, 8192>(writer, values, base, width),
    }
}
fn write_packed_signed_buffer<W: Write, T: PackedSigned, const N: usize>(
    writer: &mut W,
    values: &[T],
    base: i64,
    width: usize,
) -> Result<(), QueryError> {
    let mut bytes = [0u8; N];
    for chunk in values.chunks(N / width) {
        for (slot, value) in bytes.chunks_exact_mut(width).zip(chunk) {
            slot.copy_from_slice(
                &(value.signed().wrapping_sub(base) as u64).to_le_bytes()[..width],
            );
        }
        writer
            .write_all(&bytes[..chunk.len() * width])
            .map_err(io_err)?;
    }
    Ok(())
}
fn read_packed_signed<R: Read, T: PackedSigned>(
    reader: &mut R,
    rows: usize,
    charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
) -> Result<Vec<T>, QueryError> {
    let (width, base) = read_packed_header(reader)?;
    let mut values = Vec::new();
    let mut bytes = [0u8; 1024];
    while values.len() < rows {
        crate::engine::cancel::check_cancelled()?;
        let count = (rows - values.len()).min(bytes.len() / width);
        let block = &mut bytes[..count * width];
        reader.read_exact(block).map_err(io_err)?;
        if values.capacity() - values.len() < count {
            let old = values.capacity();
            let target = old.saturating_mul(2).max(values.len() + count).min(rows);
            charge.add_force((target - old).saturating_mul(T::WIDTH))?;
            let mut overlap = charge.sibling();
            overlap.add_force(old.saturating_mul(T::WIDTH))?;
            values
                .try_reserve_exact(target - values.len())
                .map_err(|_| {
                    crate::engine::executor::mem_budget::allocation_error("packed decimal lane")
                })?;
            if values.capacity() > target {
                charge.add_force((values.capacity() - target).saturating_mul(T::WIDTH))?;
            }
        }
        for cell in block.chunks_exact(width) {
            values.push(T::from_signed(unpack_integer(cell, base))?);
        }
    }
    Ok(values)
}

pub(super) fn write_column<W: Write>(
    writer: &mut W,
    column: &ColumnVector,
    rows: usize,
) -> Result<bool, QueryError> {
    let packed = match column {
        ColumnVector::Decimal32 { data, .. } => packed_bounds(data),
        ColumnVector::Decimal64 { data, .. } => packed_bounds(data),
        _ => None,
    };
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
        ColumnVector::Decimal32 { nulls, .. } => (
            if packed.is_some() {
                DECIMAL32_PACKED
            } else {
                DECIMAL32
            },
            nulls.as_ref(),
        ),
        ColumnVector::Decimal64 { nulls, .. } => (
            if packed.is_some() {
                DECIMAL64_PACKED
            } else {
                DECIMAL64
            },
            nulls.as_ref(),
        ),
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
            if let Some((base, width)) = packed {
                write_packed_signed(writer, data, base, width)?;
            } else {
                write_fixed(writer, data)?;
            }
        }
        ColumnVector::Decimal64 { data, scale, .. } => {
            writer.write_all(&[*scale]).map_err(io_err)?;
            if let Some((base, width)) = packed {
                write_packed_signed(writer, data, base, width)?;
            } else {
                write_fixed(writer, data)?;
            }
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
    if !(BOOL..=DECIMAL64_PACKED).contains(&kind) {
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
        DECIMAL32_PACKED => {
            let scale = byte(reader)?;
            ColumnVector::Decimal32 {
                data: read_packed_signed::<_, i32>(reader, rows, charge)?,
                scale,
                nulls,
            }
        }
        DECIMAL64_PACKED => {
            let scale = byte(reader)?;
            ColumnVector::Decimal64 {
                data: read_packed_signed::<_, i64>(reader, rows, charge)?,
                scale,
                nulls,
            }
        }
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
    fn packed_decimals_preserve_coefficients_scale_nulls_and_boundaries() {
        for (base, span) in [(i64::MIN, 31), (-50000, 500), (i64::MAX - 100000, 100000)] {
            let mut data = (0..2051).map(|row| base + row % 23).collect::<Vec<_>>();
            data[2050] = base + span;
            let mut nulls = Bitmap::new_all_valid(data.len());
            nulls.set(64, false);
            let input = ColumnVector::Decimal64 {
                data,
                scale: 17,
                nulls: Some(nulls),
            };
            let mut encoded = Vec::new();
            write_column(&mut encoded, &input, input.len()).unwrap();
            assert_eq!(encoded[1], DECIMAL64_PACKED);
            assert!(encoded.len() < input.len() * 8);
            let output = read_column(
                &mut encoded[1..].as_ref(),
                input.len(),
                &mut ScopedCharge::new(0),
            )
            .unwrap();
            assert_eq!(
                (0..input.len())
                    .map(|i| input.value_at(i))
                    .collect::<Vec<_>>(),
                (0..output.len())
                    .map(|i| output.value_at(i))
                    .collect::<Vec<_>>()
            );
            for end in [1, 2, encoded.len() - 1] {
                assert!(read_column(
                    &mut encoded[1..end].as_ref(),
                    input.len(),
                    &mut ScopedCharge::new(0)
                )
                .is_err());
            }
        }
        for input in [
            ColumnVector::Decimal32 {
                data: (0..513).map(|i| -100 + i % 17).collect(),
                scale: 2,
                nulls: None,
            },
            ColumnVector::Decimal64 {
                data: vec![i64::MIN, i64::MAX],
                scale: 0,
                nulls: None,
            },
            ColumnVector::Decimal32 {
                data: vec![i32::MIN, i32::MAX],
                scale: 9,
                nulls: None,
            },
        ] {
            let mut encoded = Vec::new();
            write_column(&mut encoded, &input, input.len()).unwrap();
            let output = read_column(
                &mut encoded[1..].as_ref(),
                input.len(),
                &mut ScopedCharge::new(0),
            )
            .unwrap();
            for row in 0..input.len() {
                assert_eq!(input.value_at(row), output.value_at(row));
            }
        }
        let mut malformed = vec![DECIMAL32_PACKED, 0, 2, 1];
        malformed.extend_from_slice(&i64::MAX.to_le_bytes());
        malformed.push(0);
        assert!(read_column(&mut malformed.as_slice(), 1, &mut ScopedCharge::new(0)).is_err());
        let budget = Arc::new(MemBudget::new(64 * 1024));
        with_query_budget(budget.clone(), || {
            let mut bytes = vec![DECIMAL64_PACKED, 0, 2, 1];
            bytes.extend_from_slice(&0i64.to_le_bytes());
            assert!(
                read_column(&mut bytes.as_slice(), 1_000_000, &mut ScopedCharge::new(0)).is_err()
            );
            assert!(budget.peak_used() < 16 * 1024);
        });
        assert_eq!(budget.used(), 0);
    }

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
