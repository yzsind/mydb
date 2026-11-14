//! Bounded, lossless LZ4 blocks for repetitive native variable-width lanes.
//! A failed optional grant or poor compression keeps the existing wire block.
use super::super::bitmap::Bitmap;
use super::*;
use crate::engine::executor::mem_budget::{
    self, ControlledMemoryCategory, ControlledMemoryCharge, ScopedCharge,
};

const MAX_PAYLOAD: usize = 4 << 20;
fn invalid(message: &str) -> QueryError {
    crate::error::catalog::ER_UNKNOWN_ERROR.error(message)
}

pub(super) fn try_write<W: Write>(
    writer: &mut W,
    column: &ColumnVector,
    rows: usize,
) -> Result<bool, QueryError> {
    let (offsets, data, nulls, utf8) = match column {
        ColumnVector::Utf8 {
            offsets,
            data,
            nulls,
        } => (offsets, data, nulls, true),
        ColumnVector::Bytes {
            offsets,
            data,
            nulls,
        } => (offsets, data, nulls, false),
        _ => return Ok(false),
    };
    if !(1024..=MAX_PAYLOAD).contains(&data.len())
        || offsets.len() != rows.saturating_add(1)
        || offsets.first() != Some(&0)
        || offsets.last().is_none_or(|&p| p as usize != data.len())
        || offsets.windows(2).any(|pair| pair[0] > pair[1])
        || nulls.as_ref().is_some_and(|bits| bits.len() != rows)
    {
        return Ok(false);
    }
    if utf8
        && !std::str::from_utf8(data)
            .is_ok_and(|text| offsets.iter().all(|&p| text.is_char_boundary(p as usize)))
    {
        return Ok(false);
    }
    crate::engine::cancel::check_cancelled()?;
    let sample = data.len().min(8192);
    let bound = lz4_flex::block::get_maximum_output_size(sample);
    let Some(sample_workspace) = ControlledMemoryCharge::try_new_optional(
        ControlledMemoryCategory::Executor,
        bound.saturating_add(65536),
    ) else {
        return Ok(false);
    };
    let mut buffer = Vec::new();
    if buffer.try_reserve_exact(bound).is_err() {
        return Ok(false);
    }
    buffer.resize(bound, 0);
    let Ok(sample_size) = lz4_flex::block::compress_into(&data[..sample], &mut buffer) else {
        return Ok(false);
    };
    if sample_size.saturating_add(5).saturating_mul(8) >= sample.saturating_mul(7) {
        return Ok(false);
    }
    let full_bound = lz4_flex::block::get_maximum_output_size(data.len());
    let mut workspace = None;
    if sample != data.len() {
        drop(buffer);
        drop(sample_workspace);
        workspace = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::Executor,
            full_bound.saturating_add(65536),
        );
        if workspace.is_none() {
            return Ok(false);
        }
        buffer = Vec::new();
        if buffer.try_reserve_exact(full_bound).is_err() {
            return Ok(false);
        }
        buffer.resize(full_bound, 0);
    }
    let size = if sample == data.len() {
        sample_size
    } else {
        let Ok(size) = lz4_flex::block::compress_into(data, &mut buffer) else {
            return Ok(false);
        };
        size
    };
    if size.saturating_add(5).saturating_mul(8) >= data.len().saturating_mul(7) {
        return Ok(false);
    }
    // Keep all raw offsets, payload under NULL cells, and validity bits intact.
    // No output has been written while this optional encoding is selected.
    writer
        .write_all(&[
            if utf8 {
                BLOCK_COMPRESSED_UTF8
            } else {
                BLOCK_COMPRESSED_BYTES
            },
            u8::from(nulls.is_some()),
        ])
        .map_err(io_err)?;
    typed::write_fixed(writer, offsets)?;
    if let Some(bits) = nulls {
        typed::write_fixed(writer, bits.words())?;
    }
    write_u32(writer, size as u32)?;
    writer.write_all(&buffer[..size]).map_err(io_err)?;
    drop(workspace);
    Ok(true)
}

pub(super) fn read<R: Read>(
    reader: &mut R,
    rows: usize,
    utf8: bool,
    charge: &mut ScopedCharge,
) -> Result<ColumnVector, QueryError> {
    let mut flag = [0];
    reader.read_exact(&mut flag).map_err(io_err)?;
    if flag[0] > 1 {
        return Err(invalid("invalid compressed spill validity tag"));
    }
    let count = rows
        .checked_add(1)
        .ok_or_else(|| invalid("compressed spill offset count overflow"))?;
    let offsets = typed::read_fixed::<_, u32>(reader, count, charge)?;
    if offsets.first() != Some(&0)
        || offsets.last().is_none_or(|&p| p as usize > MAX_PAYLOAD)
        || offsets.windows(2).any(|pair| pair[0] > pair[1])
    {
        return Err(invalid("invalid compressed spill offsets"));
    }
    let nulls = if flag[0] == 1 {
        let count = rows.div_ceil(64);
        let words = typed::read_fixed::<_, u64>(reader, count, charge)?;
        Some(
            Bitmap::from_words(words, rows)
                .ok_or_else(|| invalid("invalid compressed spill bitmap padding"))?,
        )
    } else {
        None
    };
    let expanded = offsets[rows] as usize;
    let size = read_u32(reader)? as usize;
    if expanded == 0 || size >= expanded {
        return Err(invalid("invalid compressed spill payload size"));
    }
    let compressed = crate::engine::executor::spill_value::read_exact_bytes_accounted(
        reader,
        size,
        Some(charge),
    )
    .map_err(io_err)?;
    charge.add_force(expanded)?;
    let mut data = mem_budget::try_vec_with_capacity(expanded, "decompressed spill payload")?;
    data.resize(expanded, 0);
    let actual = lz4_flex::block::decompress_into(&compressed, &mut data)
        .map_err(|_| invalid("invalid LZ4 spill payload"))?;
    if actual != expanded {
        return Err(invalid("decompressed spill size mismatch"));
    }
    if utf8
        && !std::str::from_utf8(&data)
            .is_ok_and(|text| offsets.iter().all(|&p| text.is_char_boundary(p as usize)))
    {
        return Err(invalid("invalid compressed spill UTF-8 lane"));
    }
    Ok(if utf8 {
        ColumnVector::Utf8 {
            offsets,
            data,
            nulls,
        }
    } else {
        ColumnVector::Bytes {
            offsets,
            data,
            nulls,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn repetitive_spill_lanes_compress_losslessly_and_reject_corruption() {
        for binary in [false, true] {
            for nullable in [false, true] {
                let values = (0..8192)
                    .map(|n| {
                        if nullable && n % 17 == 0 {
                            Value::Null
                        } else if binary {
                            Value::Bytes(if n % 3 == 0 {
                                vec![]
                            } else {
                                vec![0, 255, 3, 4, 0, 12]
                            })
                        } else {
                            Value::String(if n % 3 == 0 {
                                String::new()
                            } else {
                                "Äa短文本 ".repeat(12)
                            })
                        }
                    })
                    .collect::<Vec<_>>();
                let column = if binary {
                    let mut offsets = vec![0];
                    let mut data = Vec::new();
                    let mut bits = Bitmap::new_all_valid(values.len());
                    for (row, value) in values.iter().enumerate() {
                        match value {
                            Value::Null => bits.set(row, false),
                            Value::Bytes(b) => data.extend_from_slice(b),
                            _ => unreachable!(),
                        };
                        offsets.push(data.len() as u32);
                    }
                    ColumnVector::Bytes {
                        offsets,
                        data,
                        nulls: nullable.then_some(bits),
                    }
                } else {
                    ColumnVector::from_values(values)
                };
                let mut bytes = Vec::new();
                assert!(try_write(&mut bytes, &column, 8192).unwrap());
                assert!(bytes.len() < 40000, "{}", bytes.len());
                let actual =
                    read(&mut &bytes[1..], 8192, !binary, &mut ScopedCharge::new(0)).unwrap();
                for row in 0..8192 {
                    assert!(actual.value_at(row).storage_eq(&column.value_at(row)));
                }
                for end in [1, 2, 6, bytes.len() - 1] {
                    assert!(read(
                        &mut &bytes[1..end],
                        8192,
                        !binary,
                        &mut ScopedCharge::new(0)
                    )
                    .is_err());
                }
                let mut broken = bytes.clone();
                broken[3..7].copy_from_slice(&u32::MAX.to_le_bytes());
                assert!(read(&mut &broken[1..], 8192, !binary, &mut ScopedCharge::new(0)).is_err());
            }
        }
    }
}
