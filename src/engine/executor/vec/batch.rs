//! Columnar batch primitives.
//!
//! A [`Batch`] is the unit of data flowing through the vectorized executor.
//! Each `Batch` carries up to ~8K rows organised as one [`ColumnVector`] per
//! output column, plus an optional row-selection vector. Variable-length
//! columns use Arrow-style `offsets + data` layout so future zero-copy
//! interop with Arrow remains possible without committing to `arrow-rs`
//! today.
//!
//! Hot-path callers should prefer the typed accessors on `ColumnVector`
//! (e.g. `as_i64_slice`) and avoid going through `value_at`, which always
//! materialises a `Value`.

use std::sync::Arc;

use crate::types::Value;

use super::bitmap::Bitmap;
use super::schema::BatchSchema;

/// Default morsel size, sized for the L2 footprint of the common multi-column
/// analytical scan. Operators may coalesce morsels internally, while memory
/// pressure can reduce their working batches without changing the plan.
pub const DEFAULT_BATCH_ROWS: usize = 32768;

fn materialized_utf8(bytes: &[u8]) -> Result<&str, ()> {
    if bytes.len() >= 64 {
        simdutf8::basic::from_utf8(bytes).map_err(|_| ())
    } else {
        std::str::from_utf8(bytes).map_err(|_| ())
    }
}

/// Per-column data buffer.
///
/// Fixed-width variants store their values in a contiguous `Box<[T]>`; the
/// optional `nulls` bitmap is `Some` when the column may contain NULLs.
/// Variable-length variants use a typical offsets/data split. `Generic` is
/// the row-format fallback for `Value` shapes that don't (yet) have a
/// typed lane — most notably the legacy `Value::Decimal(String)` and
/// `Value::UInt`.
#[derive(Debug, Clone)]
pub enum ColumnVector {
    /// All rows are NULL — a cheap sentinel so projection rewrites that
    /// drop a column can still preserve schema width.
    Null { len: usize },
    Bool {
        values: Bitmap,
        nulls: Option<Bitmap>,
    },
    I64 {
        data: Vec<i64>,
        nulls: Option<Bitmap>,
    },
    U64 {
        data: Vec<u64>,
        nulls: Option<Bitmap>,
    },
    F64 {
        data: Vec<f64>,
        nulls: Option<Bitmap>,
    },
    Date32 {
        data: Vec<i32>,
        nulls: Option<Bitmap>,
    },
    Time64 {
        data: Vec<i64>,
        nulls: Option<Bitmap>,
    },
    DateTime64 {
        data: Vec<i64>,
        nulls: Option<Bitmap>,
    },
    Timestamp64 {
        data: Vec<i64>,
        nulls: Option<Bitmap>,
    },
    /// Compact DECIMAL with a uniform scale; the scale is stored on the
    /// vector rather than per-value to keep the data buffer cache-friendly.
    ///
    /// Three width tiers based on the maximum observed magnitude in the
    /// column:
    ///   `Decimal32`  — fits ±2.1·10⁹  (covers DECIMAL(P≤9, S))
    ///   `Decimal64`  — fits ±9.2·10¹⁸ (covers DECIMAL(P≤18, S))
    ///   `Decimal128` — full ±1.7·10³⁸ (fallback for DECIMAL(P>18, S))
    ///
    /// Narrower lanes are SIMD-native on AVX2/AVX-512/NEON, while i128 is
    /// not — picking the narrowest fitting lane lets `sum_*_simd` and
    /// `add_*_simd` kernels in `expr_batch` auto-vectorise.
    Decimal32 {
        data: Vec<i32>,
        scale: u8,
        nulls: Option<Bitmap>,
    },
    Decimal64 {
        data: Vec<i64>,
        scale: u8,
        nulls: Option<Bitmap>,
    },
    Decimal128 {
        data: Vec<i128>,
        scale: u8,
        nulls: Option<Bitmap>,
    },
    DecimalWide {
        data: Vec<crate::types::DecimalWide>,
        scale: u8,
        nulls: Option<Bitmap>,
    },
    Utf8 {
        offsets: Vec<u32>,
        data: Vec<u8>,
        nulls: Option<Bitmap>,
    },
    Bytes {
        offsets: Vec<u32>,
        data: Vec<u8>,
        nulls: Option<Bitmap>,
    },
    Json {
        offsets: Vec<u32>,
        data: Vec<u8>,
        nulls: Option<Bitmap>,
    },
    Geometry {
        offsets: Vec<u32>,
        data: Vec<u8>,
        nulls: Option<Bitmap>,
    },
    Vector {
        offsets: Vec<u32>,
        data: Vec<u8>,
        nulls: Option<Bitmap>,
    },
    /// Fallback bucket for values that don't fit a typed lane. Each row
    /// is a self-contained `Value`. NULL is encoded as `Value::Null`.
    Generic { values: Vec<Value> },
}

impl ColumnVector {
    pub(crate) fn owned_value_bytes(&self, row: usize) -> usize {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        let payload = if self.is_null(row) {
            0
        } else {
            match self {
                Self::Utf8 { offsets, .. }
                | Self::Bytes { offsets, .. }
                | Self::Json { offsets, .. }
                | Self::Geometry { offsets, .. }
                | Self::Vector { offsets, .. } => (offsets[row + 1] - offsets[row]) as usize,
                Self::Generic { values } => values[row].allocated_bytes(),
                Self::DecimalWide { data, .. } => std::mem::size_of::<crate::types::DecimalWide>()
                    .saturating_add(data[row].magnitude.bits().div_ceil(8) as usize)
                    .saturating_add(32),
                _ => 0,
            }
        };
        std::mem::size_of::<Value>().saturating_add(payload)
    }
    fn try_clone_owned(&self) -> Result<Self, crate::types::QueryError> {
        use crate::engine::executor::mem_budget;
        let bitmap = |value: &Option<Bitmap>| value.as_ref().map(Bitmap::try_clone).transpose();
        macro_rules! lane {
            ($kind:ident, $data:expr, $nulls:expr) => {
                Self::$kind {
                    data: mem_budget::try_copy_slice($data, "batch lane copy")?,
                    nulls: bitmap($nulls)?,
                }
            };
            ($kind:ident, $data:expr, $scale:expr, $nulls:expr) => {
                Self::$kind {
                    data: mem_budget::try_copy_slice($data, "batch decimal lane copy")?,
                    scale: *$scale,
                    nulls: bitmap($nulls)?,
                }
            };
        }
        Ok(match self {
            Self::Null { len } => Self::Null { len: *len },
            Self::Bool { values, nulls } => Self::Bool {
                values: values.try_clone()?,
                nulls: bitmap(nulls)?,
            },
            Self::I64 { data, nulls } => lane!(I64, data, nulls),
            Self::U64 { data, nulls } => lane!(U64, data, nulls),
            Self::F64 { data, nulls } => lane!(F64, data, nulls),
            Self::Date32 { data, nulls } => lane!(Date32, data, nulls),
            Self::Time64 { data, nulls } => lane!(Time64, data, nulls),
            Self::DateTime64 { data, nulls } => lane!(DateTime64, data, nulls),
            Self::Timestamp64 { data, nulls } => lane!(Timestamp64, data, nulls),
            Self::Decimal32 { data, scale, nulls } => lane!(Decimal32, data, scale, nulls),
            Self::Decimal64 { data, scale, nulls } => lane!(Decimal64, data, scale, nulls),
            Self::Decimal128 { data, scale, nulls } => lane!(Decimal128, data, scale, nulls),
            Self::DecimalWide { data, scale, nulls } => {
                let mut copy =
                    mem_budget::try_vec_with_capacity(data.len(), "batch wide decimal copy")?;
                copy.extend(data.iter().cloned());
                Self::DecimalWide {
                    data: copy,
                    scale: *scale,
                    nulls: bitmap(nulls)?,
                }
            }
            Self::Utf8 {
                offsets,
                data,
                nulls,
            } => Self::Utf8 {
                offsets: mem_budget::try_copy_slice(offsets, "batch offset copy")?,
                data: mem_budget::try_copy_bytes(data, "batch byte lane copy")?,
                nulls: bitmap(nulls)?,
            },
            Self::Bytes {
                offsets,
                data,
                nulls,
            } => Self::Bytes {
                offsets: mem_budget::try_copy_slice(offsets, "batch offset copy")?,
                data: mem_budget::try_copy_bytes(data, "batch byte lane copy")?,
                nulls: bitmap(nulls)?,
            },
            Self::Json {
                offsets,
                data,
                nulls,
            } => Self::Json {
                offsets: mem_budget::try_copy_slice(offsets, "batch offset copy")?,
                data: mem_budget::try_copy_bytes(data, "batch byte lane copy")?,
                nulls: bitmap(nulls)?,
            },
            Self::Geometry {
                offsets,
                data,
                nulls,
            } => Self::Geometry {
                offsets: mem_budget::try_copy_slice(offsets, "batch offset copy")?,
                data: mem_budget::try_copy_bytes(data, "batch byte lane copy")?,
                nulls: bitmap(nulls)?,
            },
            Self::Vector {
                offsets,
                data,
                nulls,
            } => Self::Vector {
                offsets: mem_budget::try_copy_slice(offsets, "batch offset copy")?,
                data: mem_budget::try_copy_bytes(data, "batch byte lane copy")?,
                nulls: bitmap(nulls)?,
            },
            Self::Generic { values } => {
                let mut copy =
                    mem_budget::try_vec_with_capacity(values.len(), "batch generic lane copy")?;
                for value in values {
                    copy.push(mem_budget::try_clone_value(
                        value,
                        "batch generic cell copy",
                    )?);
                }
                Self::Generic { values: copy }
            }
        })
    }

    pub(crate) fn valid_utf8_at(&self, row: usize) -> Option<&str> {
        let Self::Utf8 { offsets, data, .. } = self else {
            return None;
        };
        materialized_utf8(data.get(*offsets.get(row)? as usize..*offsets.get(row + 1)? as usize)?)
            .ok()
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnVector::Null { len } => *len,
            ColumnVector::Bool { values, .. } => values.len(),
            ColumnVector::I64 { data, .. } => data.len(),
            ColumnVector::U64 { data, .. } => data.len(),
            ColumnVector::F64 { data, .. } => data.len(),
            ColumnVector::Date32 { data, .. } => data.len(),
            ColumnVector::Time64 { data, .. } => data.len(),
            ColumnVector::DateTime64 { data, .. } => data.len(),
            ColumnVector::Timestamp64 { data, .. } => data.len(),
            ColumnVector::Decimal32 { data, .. } => data.len(),
            ColumnVector::Decimal64 { data, .. } => data.len(),
            ColumnVector::Decimal128 { data, .. } => data.len(),
            ColumnVector::DecimalWide { data, .. } => data.len(),
            ColumnVector::Utf8 { offsets, .. } => offsets.len().saturating_sub(1),
            ColumnVector::Bytes { offsets, .. } => offsets.len().saturating_sub(1),
            ColumnVector::Json { offsets, .. }
            | ColumnVector::Geometry { offsets, .. }
            | ColumnVector::Vector { offsets, .. } => offsets.len().saturating_sub(1),
            ColumnVector::Generic { values } => values.len(),
        }
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Estimated heap bytes occupied by this column's data buffers
    /// (including the NULL bitmap). Used by the vectorized hash join's
    /// build-side size cap to bound in-memory materialisation and fall
    /// back to the common grace-spill strategy on oversized builds.
    /// Resident accounting follows allocated capacity, never transient
    /// logical length.
    pub fn memory_bytes(&self) -> usize {
        let null_bytes =
            |nulls: &Option<Bitmap>| nulls.as_ref().map(|b| b.memory_bytes()).unwrap_or(0);
        match self {
            ColumnVector::Null { .. } => 0,
            ColumnVector::Bool { values, nulls } => {
                values.memory_bytes().saturating_add(null_bytes(nulls))
            }
            ColumnVector::I64 { data, nulls }
            | ColumnVector::Timestamp64 { data, nulls }
            | ColumnVector::Time64 { data, nulls }
            | ColumnVector::DateTime64 { data, nulls } => data
                .capacity()
                .saturating_mul(std::mem::size_of::<i64>())
                .saturating_add(null_bytes(nulls)),
            ColumnVector::U64 { data, nulls } => data
                .capacity()
                .saturating_mul(std::mem::size_of::<u64>())
                .saturating_add(null_bytes(nulls)),
            ColumnVector::F64 { data, nulls } => data
                .capacity()
                .saturating_mul(std::mem::size_of::<f64>())
                .saturating_add(null_bytes(nulls)),
            ColumnVector::Date32 { data, nulls } => data
                .capacity()
                .saturating_mul(std::mem::size_of::<i32>())
                .saturating_add(null_bytes(nulls)),
            ColumnVector::Decimal32 { data, nulls, .. } => data
                .capacity()
                .saturating_mul(std::mem::size_of::<i32>())
                .saturating_add(null_bytes(nulls)),
            ColumnVector::Decimal64 { data, nulls, .. } => data
                .capacity()
                .saturating_mul(std::mem::size_of::<i64>())
                .saturating_add(null_bytes(nulls)),
            ColumnVector::Decimal128 { data, nulls, .. } => data
                .capacity()
                .saturating_mul(std::mem::size_of::<i128>())
                .saturating_add(null_bytes(nulls)),
            ColumnVector::DecimalWide { data, nulls, .. } => data
                .capacity()
                .saturating_mul(std::mem::size_of::<crate::types::DecimalWide>())
                .saturating_add(
                    data.iter()
                        .map(|value| {
                            let limb_bits = usize::BITS as u64;
                            (value.magnitude.bits().div_ceil(limb_bits) as usize)
                                .max(1)
                                .next_power_of_two()
                                .saturating_mul(std::mem::size_of::<usize>())
                        })
                        .sum::<usize>(),
                )
                .saturating_add(null_bytes(nulls)),
            ColumnVector::Utf8 {
                offsets,
                data,
                nulls,
            }
            | ColumnVector::Bytes {
                offsets,
                data,
                nulls,
            }
            | ColumnVector::Json {
                offsets,
                data,
                nulls,
            }
            | ColumnVector::Geometry {
                offsets,
                data,
                nulls,
            }
            | ColumnVector::Vector {
                offsets,
                data,
                nulls,
            } => offsets
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>())
                .saturating_add(data.capacity())
                .saturating_add(null_bytes(nulls)),
            ColumnVector::Generic { values } => {
                use crate::engine::executor::memory_footprint::AllocatedBytes;
                let mut total = values
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Value>());
                for v in values {
                    total = total.saturating_add(v.allocated_bytes());
                }
                total
            }
        }
    }

    /// Returns true if the row at `idx` is NULL.
    pub fn is_null(&self, idx: usize) -> bool {
        match self {
            ColumnVector::Null { .. } => true,
            ColumnVector::Bool { nulls, .. }
            | ColumnVector::I64 { nulls, .. }
            | ColumnVector::U64 { nulls, .. }
            | ColumnVector::F64 { nulls, .. }
            | ColumnVector::Date32 { nulls, .. }
            | ColumnVector::Time64 { nulls, .. }
            | ColumnVector::DateTime64 { nulls, .. }
            | ColumnVector::Timestamp64 { nulls, .. }
            | ColumnVector::Decimal32 { nulls, .. }
            | ColumnVector::Decimal64 { nulls, .. }
            | ColumnVector::Decimal128 { nulls, .. }
            | ColumnVector::DecimalWide { nulls, .. }
            | ColumnVector::Utf8 { nulls, .. }
            | ColumnVector::Bytes { nulls, .. }
            | ColumnVector::Json { nulls, .. }
            | ColumnVector::Geometry { nulls, .. }
            | ColumnVector::Vector { nulls, .. } => {
                nulls.as_ref().map(|b| !b.get(idx)).unwrap_or(false)
            }
            ColumnVector::Generic { values } => matches!(values[idx], Value::Null),
        }
    }

    /// Number of non-NULL rows in the column. `nulls() == None` does
    /// NOT mean "no NULLs" for every lane: `Null` is all-NULL and
    /// `Generic` encodes NULL inline as `Value::Null` — both lanes have
    /// no bitmap. COUNT(col) must come through here, never through a
    /// bare `nulls()` popcount.
    pub fn count_non_null(&self) -> usize {
        match self {
            ColumnVector::Null { .. } => 0,
            ColumnVector::Generic { values } => {
                values.iter().filter(|v| !matches!(v, Value::Null)).count()
            }
            _ => match self.nulls() {
                Some(b) => b.count_ones(),
                None => self.len(),
            },
        }
    }

    /// Non-NULL rows among `selection` positions. Same lane caveats as
    /// [`Self::count_non_null`].
    pub fn count_non_null_selected(&self, selection: &[u32]) -> usize {
        match self {
            ColumnVector::Null { .. } => 0,
            ColumnVector::Generic { values } => selection
                .iter()
                .filter(|&&r| !matches!(values[r as usize], Value::Null))
                .count(),
            _ => match self.nulls() {
                Some(b) => selection.iter().filter(|&&r| b.get(r as usize)).count(),
                None => selection.len(),
            },
        }
    }

    /// Materialise the value at `idx` as a `Value`. Hot paths should
    /// avoid this — go through the typed slices directly.
    pub(crate) fn typed_value_at(
        &self,
        idx: usize,
        column_type: crate::types::ColumnType,
    ) -> Value {
        match (column_type, self.value_at(idx)) {
            (crate::types::ColumnType::Float, Value::Double(value)) => Value::Float(value as f32),
            (_, value) => value,
        }
    }

    pub fn value_at(&self, idx: usize) -> Value {
        if self.is_null(idx) {
            return Value::Null;
        }
        match self {
            ColumnVector::Null { .. } => Value::Null,
            ColumnVector::Bool { values, .. } => Value::Int(values.get(idx) as i64),
            ColumnVector::I64 { data, .. } => Value::Int(data[idx]),
            ColumnVector::U64 { data, .. } => Value::UInt(data[idx]),
            ColumnVector::F64 { data, .. } => Value::Double(data[idx]),
            ColumnVector::Date32 { data, .. } => Value::Date(data[idx]),
            ColumnVector::Time64 { data, .. } => Value::Time(data[idx]),
            ColumnVector::DateTime64 { data, .. } => Value::DateTime(data[idx]),
            ColumnVector::Timestamp64 { data, .. } => Value::Timestamp(data[idx]),
            ColumnVector::Decimal32 { data, scale, .. } => {
                Value::DecimalCompact(data[idx] as i64, *scale)
            }
            ColumnVector::Decimal64 { data, scale, .. } => Value::DecimalCompact(data[idx], *scale),
            ColumnVector::Decimal128 { data, scale, .. } => {
                let v = data[idx];
                if let Ok(small) = i64::try_from(v) {
                    Value::DecimalCompact(small, *scale)
                } else {
                    Value::Decimal128(v, *scale)
                }
            }
            ColumnVector::DecimalWide { data, .. } => {
                Value::DecimalWide(Box::new(data[idx].clone()))
            }
            ColumnVector::Utf8 { offsets, data, .. } => {
                let s = &data[offsets[idx] as usize..offsets[idx + 1] as usize];
                match materialized_utf8(s) {
                    Ok(_) if s.len() <= crate::types::SMALL_STRING_INLINE_CAP => {
                        Value::SmallString(crate::types::SmallStringBuf::from_slice(s))
                    }
                    Ok(text) => Value::String(text.to_owned()),
                    // A binary fallback may carry arbitrary octets. Preserve
                    // them instead of merging different invalid sequences.
                    Err(_) => Value::Bytes(s.to_vec()),
                }
            }
            ColumnVector::Bytes { offsets, data, .. } => {
                let b = &data[offsets[idx] as usize..offsets[idx + 1] as usize];
                Value::Bytes(b.to_vec())
            }
            ColumnVector::Json { offsets, data, .. } => {
                Value::Json(data[offsets[idx] as usize..offsets[idx + 1] as usize].to_vec())
            }
            ColumnVector::Geometry { offsets, data, .. } => {
                Value::Geometry(data[offsets[idx] as usize..offsets[idx + 1] as usize].to_vec())
            }
            ColumnVector::Vector { offsets, data, .. } => Value::Vector(
                crate::engine::vector::codec::decode_value(
                    &data[offsets[idx] as usize..offsets[idx + 1] as usize],
                )
                .expect("validated vector batch encoding"),
            ),
            ColumnVector::Generic { values } => values[idx].clone(),
        }
    }

    /// Fallible materialisation for paths that retain row values. Typed
    /// fixed-width lanes stay allocation-free; user-sized string/binary lanes
    /// reserve explicitly so allocator pressure becomes MySQL 1037.
    pub fn try_value_at(&self, idx: usize) -> Result<Value, crate::types::QueryError> {
        if self.is_null(idx) {
            return Ok(Value::Null);
        }
        match self {
            ColumnVector::Utf8 { offsets, data, .. } => {
                let bytes = &data[offsets[idx] as usize..offsets[idx + 1] as usize];
                match materialized_utf8(bytes) {
                    Ok(_) if bytes.len() <= crate::types::SMALL_STRING_INLINE_CAP => Ok(
                        Value::SmallString(crate::types::SmallStringBuf::from_slice(bytes)),
                    ),
                    Ok(text) => Ok(Value::String(
                        crate::engine::executor::mem_budget::try_copy_string(
                            text,
                            "vector UTF-8 value",
                        )?,
                    )),
                    Err(_) => Ok(Value::Bytes(
                        crate::engine::executor::mem_budget::try_copy_bytes(
                            bytes,
                            "vector binary UTF-8 fallback",
                        )?,
                    )),
                }
            }
            ColumnVector::Bytes { offsets, data, .. } => {
                let bytes = &data[offsets[idx] as usize..offsets[idx + 1] as usize];
                Ok(Value::Bytes(
                    crate::engine::executor::mem_budget::try_copy_bytes(
                        bytes,
                        "vector binary value",
                    )?,
                ))
            }
            ColumnVector::Json { offsets, data, .. } => {
                let bytes = &data[offsets[idx] as usize..offsets[idx + 1] as usize];
                Ok(Value::Json(
                    crate::engine::executor::mem_budget::try_copy_bytes(
                        bytes,
                        "vector JSON value",
                    )?,
                ))
            }
            ColumnVector::Geometry { offsets, data, .. } => {
                let bytes = &data[offsets[idx] as usize..offsets[idx + 1] as usize];
                Ok(Value::Geometry(
                    crate::engine::executor::mem_budget::try_copy_bytes(
                        bytes,
                        "vector geometry value",
                    )?,
                ))
            }
            ColumnVector::Vector { offsets, data, .. } => {
                let bytes = &data[offsets[idx] as usize..offsets[idx + 1] as usize];
                Ok(Value::Vector(crate::engine::vector::codec::decode_value(
                    bytes,
                )?))
            }
            ColumnVector::Generic { values } => {
                crate::engine::executor::mem_budget::try_clone_value(
                    &values[idx],
                    "generic vector value",
                )
            }
            _ => Ok(self.value_at(idx)),
        }
    }

    pub fn materialized_value_bytes(&self, idx: usize) -> usize {
        let base = std::mem::size_of::<Value>();
        if self.is_null(idx) {
            return base;
        }
        let payload = match self {
            ColumnVector::Utf8 { offsets, .. }
            | ColumnVector::Bytes { offsets, .. }
            | ColumnVector::Json { offsets, .. }
            | ColumnVector::Geometry { offsets, .. }
            | ColumnVector::Vector { offsets, .. } => (offsets[idx + 1] - offsets[idx]) as usize,
            ColumnVector::DecimalWide { data, .. } => {
                let limbs = data[idx].magnitude.bits().div_ceil(usize::BITS as u64) as usize;
                std::mem::size_of::<crate::types::DecimalWide>()
                    .saturating_add(limbs.max(1).next_power_of_two() * std::mem::size_of::<usize>())
            }
            ColumnVector::Generic { values } => {
                return crate::engine::executor::mem_budget::estimate_value_bytes(&values[idx]);
            }
            _ => 0,
        };
        base.saturating_add(payload)
    }

    pub fn as_i64_slice(&self) -> Option<&[i64]> {
        match self {
            ColumnVector::I64 { data, .. } => Some(data),
            _ => None,
        }
    }

    /// Borrow the underlying `&[i32]` of a `Decimal32` lane. Returns
    /// `None` for any other variant — SIMD kernels in `expr_batch` use
    /// this to dispatch the narrow path without re-materialising values.
    pub fn as_decimal_i32_slice(&self) -> Option<(&[i32], u8)> {
        match self {
            ColumnVector::Decimal32 { data, scale, .. } => Some((data, *scale)),
            _ => None,
        }
    }

    /// Borrow the underlying `&[i64]` of a `Decimal64` lane.
    pub fn as_decimal_i64_slice(&self) -> Option<(&[i64], u8)> {
        match self {
            ColumnVector::Decimal64 { data, scale, .. } => Some((data, *scale)),
            _ => None,
        }
    }

    /// Borrow the underlying `&[i128]` of a `Decimal128` lane.
    pub fn as_decimal_i128_slice(&self) -> Option<(&[i128], u8)> {
        match self {
            ColumnVector::Decimal128 { data, scale, .. } => Some((data, *scale)),
            _ => None,
        }
    }

    /// Common scale across the three decimal lanes. Returns `None` for
    /// non-decimal variants.
    pub fn decimal_scale(&self) -> Option<u8> {
        match self {
            ColumnVector::Decimal32 { scale, .. }
            | ColumnVector::Decimal64 { scale, .. }
            | ColumnVector::Decimal128 { scale, .. } => Some(*scale),
            _ => None,
        }
    }

    pub fn nulls(&self) -> Option<&Bitmap> {
        match self {
            ColumnVector::Bool { nulls, .. }
            | ColumnVector::I64 { nulls, .. }
            | ColumnVector::U64 { nulls, .. }
            | ColumnVector::F64 { nulls, .. }
            | ColumnVector::Date32 { nulls, .. }
            | ColumnVector::Time64 { nulls, .. }
            | ColumnVector::DateTime64 { nulls, .. }
            | ColumnVector::Timestamp64 { nulls, .. }
            | ColumnVector::Decimal32 { nulls, .. }
            | ColumnVector::Decimal64 { nulls, .. }
            | ColumnVector::Decimal128 { nulls, .. }
            | ColumnVector::DecimalWide { nulls, .. }
            | ColumnVector::Utf8 { nulls, .. }
            | ColumnVector::Bytes { nulls, .. }
            | ColumnVector::Json { nulls, .. }
            | ColumnVector::Geometry { nulls, .. }
            | ColumnVector::Vector { nulls, .. } => nulls.as_ref(),
            _ => None,
        }
    }

    /// Exchange rows by position into a new vector of the same lane.
    /// `u32::MAX` is a NULL sentinel — hash join outer joins use it to inject
    /// NULLs into unmatched rows, no separate NULL-extension path needed. A NULL
    /// source row or the sentinel both set the null bit; the bitmap collapses back to `None` when no NULLs remain.
    pub fn take_indices(&self, indices: &[u32]) -> ColumnVector {
        self.gather_indices(indices, false)
    }

    /// Use the selected payload size for byte-bounded, pre-admitted copies.
    /// The source average can over-reserve arbitrarily for skewed selections.
    pub(crate) fn take_indices_exact(&self, indices: &[u32]) -> ColumnVector {
        self.gather_indices(indices, true)
    }

    fn gather_indices(&self, indices: &[u32], exact_capacity: bool) -> ColumnVector {
        const NULL_IDX: u32 = u32::MAX;
        // INNER joins and filtered projections overwhelmingly gather real row
        // ids. Compute that property once per column instead of re-testing the
        // sentinel in every lane's inner loop. When the source lane also has no
        // validity bitmap, the gather is a pure indexed copy and need not
        // allocate/initialise an all-valid bitmap that will immediately be
        // discarded.
        let has_null_sentinel = indices.contains(&NULL_IDX);
        macro_rules! gather_fixed {
            ($data:expr, $nulls:expr, |$d:ident, $n:ident| $build:expr) => {{
                let src = $data;
                let src_nulls = $nulls.as_ref();
                let mut $d = Vec::with_capacity(indices.len());
                if src_nulls.is_none() && !has_null_sentinel {
                    $d.extend(indices.iter().map(|&idx| src[idx as usize]));
                    let $n = None;
                    $build
                } else {
                    let mut out_nulls = Bitmap::new_all_valid(indices.len());
                    let mut any_null = false;
                    for (i, &idx) in indices.iter().enumerate() {
                        if idx == NULL_IDX {
                            $d.push(Default::default());
                            out_nulls.set(i, false);
                            any_null = true;
                        } else {
                            let r = idx as usize;
                            if src_nulls.map(|b| !b.get(r)).unwrap_or(false) {
                                $d.push(Default::default());
                                out_nulls.set(i, false);
                                any_null = true;
                            } else {
                                $d.push(src[r]);
                            }
                        }
                    }
                    let $n = any_null.then_some(out_nulls);
                    $build
                }
            }};
        }
        match self {
            ColumnVector::Null { .. } => ColumnVector::Null { len: indices.len() },
            ColumnVector::Bool { values, nulls } => {
                let mut out_values = Bitmap::new_all_invalid(indices.len());
                let mut out_nulls = Bitmap::new_all_valid(indices.len());
                let mut any_null = false;
                for (i, &idx) in indices.iter().enumerate() {
                    if idx == NULL_IDX {
                        out_nulls.set(i, false);
                        any_null = true;
                    } else {
                        let r = idx as usize;
                        if nulls.as_ref().map(|b| !b.get(r)).unwrap_or(false) {
                            out_nulls.set(i, false);
                            any_null = true;
                        } else if values.get(r) {
                            out_values.set_true(i);
                        }
                    }
                }
                ColumnVector::Bool {
                    values: out_values,
                    nulls: any_null.then_some(out_nulls),
                }
            }
            ColumnVector::I64 { data, nulls } => {
                gather_fixed!(data, nulls, |d, n| ColumnVector::I64 { data: d, nulls: n })
            }
            ColumnVector::U64 { data, nulls } => {
                gather_fixed!(data, nulls, |d, n| ColumnVector::U64 { data: d, nulls: n })
            }
            ColumnVector::F64 { data, nulls } => {
                gather_fixed!(data, nulls, |d, n| ColumnVector::F64 { data: d, nulls: n })
            }
            ColumnVector::Date32 { data, nulls } => {
                gather_fixed!(data, nulls, |d, n| ColumnVector::Date32 {
                    data: d,
                    nulls: n
                })
            }
            ColumnVector::Time64 { data, nulls } => {
                gather_fixed!(data, nulls, |d, n| ColumnVector::Time64 {
                    data: d,
                    nulls: n
                })
            }
            ColumnVector::DateTime64 { data, nulls } => {
                gather_fixed!(data, nulls, |d, n| ColumnVector::DateTime64 {
                    data: d,
                    nulls: n
                })
            }
            ColumnVector::Timestamp64 { data, nulls } => {
                gather_fixed!(data, nulls, |d, n| ColumnVector::Timestamp64 {
                    data: d,
                    nulls: n
                })
            }
            ColumnVector::Decimal32 { data, scale, nulls } => {
                let scale = *scale;
                gather_fixed!(data, nulls, |d, n| ColumnVector::Decimal32 {
                    data: d,
                    scale,
                    nulls: n,
                })
            }
            ColumnVector::Decimal64 { data, scale, nulls } => {
                let scale = *scale;
                gather_fixed!(data, nulls, |d, n| ColumnVector::Decimal64 {
                    data: d,
                    scale,
                    nulls: n,
                })
            }
            ColumnVector::Decimal128 { data, scale, nulls } => {
                let scale = *scale;
                gather_fixed!(data, nulls, |d, n| ColumnVector::Decimal128 {
                    data: d,
                    scale,
                    nulls: n,
                })
            }
            ColumnVector::DecimalWide { data, scale, nulls } => {
                let mut gathered = Vec::with_capacity(indices.len());
                let mut out_nulls = Bitmap::new_all_valid(indices.len());
                let mut any_null = false;
                for (index, &row) in indices.iter().enumerate() {
                    if row == NULL_IDX
                        || nulls
                            .as_ref()
                            .map(|bitmap| !bitmap.get(row as usize))
                            .unwrap_or(false)
                    {
                        gathered.push(crate::types::DecimalWide {
                            magnitude: 0u8.into(),
                            scale: *scale,
                        });
                        out_nulls.set(index, false);
                        any_null = true;
                    } else {
                        gathered.push(data[row as usize].clone());
                    }
                }
                ColumnVector::DecimalWide {
                    data: gathered,
                    scale: *scale,
                    nulls: any_null.then_some(out_nulls),
                }
            }
            ColumnVector::Utf8 {
                offsets,
                data,
                nulls,
            }
            | ColumnVector::Bytes {
                offsets,
                data,
                nulls,
            }
            | ColumnVector::Json {
                offsets,
                data,
                nulls,
            }
            | ColumnVector::Geometry {
                offsets,
                data,
                nulls,
            }
            | ColumnVector::Vector {
                offsets,
                data,
                nulls,
            } => {
                let mut out_offsets: Vec<u32> = Vec::with_capacity(indices.len() + 1);
                out_offsets.push(0);
                // A proportional reservation avoids repeated realloc+memmove
                // for ordinary variable-width gathers. It is an estimate, not
                // an ownership charge: this is the same short-lived output
                // batch allocation as before and its capacity remains visible
                // to the surrounding batch/work-area accounting.
                let average_width = data.len().div_ceil(self.len().max(1));
                let capacity = if exact_capacity {
                    indices.iter().fold(0usize, |bytes, &row| {
                        if row == NULL_IDX || self.is_null(row as usize) {
                            bytes
                        } else {
                            bytes.saturating_add(
                                (offsets[row as usize + 1] - offsets[row as usize]) as usize,
                            )
                        }
                    })
                } else {
                    average_width.saturating_mul(indices.len())
                };
                let mut out_data: Vec<u8> = Vec::with_capacity(capacity);
                if nulls.is_none() && !has_null_sentinel {
                    for &idx in indices {
                        let r = idx as usize;
                        out_data
                            .extend_from_slice(&data[offsets[r] as usize..offsets[r + 1] as usize]);
                        out_offsets.push(out_data.len() as u32);
                    }
                    return match self {
                        ColumnVector::Utf8 { .. } => ColumnVector::Utf8 {
                            offsets: out_offsets,
                            data: out_data,
                            nulls: None,
                        },
                        ColumnVector::Bytes { .. } => ColumnVector::Bytes {
                            offsets: out_offsets,
                            data: out_data,
                            nulls: None,
                        },
                        ColumnVector::Json { .. } => ColumnVector::Json {
                            offsets: out_offsets,
                            data: out_data,
                            nulls: None,
                        },
                        ColumnVector::Geometry { .. } => ColumnVector::Geometry {
                            offsets: out_offsets,
                            data: out_data,
                            nulls: None,
                        },
                        ColumnVector::Vector { .. } => ColumnVector::Vector {
                            offsets: out_offsets,
                            data: out_data,
                            nulls: None,
                        },
                        _ => unreachable!(),
                    };
                }
                let mut out_nulls = Bitmap::new_all_valid(indices.len());
                let mut any_null = false;
                for (i, &idx) in indices.iter().enumerate() {
                    if idx == NULL_IDX {
                        out_nulls.set(i, false);
                        any_null = true;
                    } else {
                        let r = idx as usize;
                        if nulls.as_ref().map(|b| !b.get(r)).unwrap_or(false) {
                            out_nulls.set(i, false);
                            any_null = true;
                        } else {
                            out_data.extend_from_slice(
                                &data[offsets[r] as usize..offsets[r + 1] as usize],
                            );
                        }
                    }
                    out_offsets.push(out_data.len() as u32);
                }
                let nulls = any_null.then_some(out_nulls);
                match self {
                    ColumnVector::Utf8 { .. } => ColumnVector::Utf8 {
                        offsets: out_offsets,
                        data: out_data,
                        nulls,
                    },
                    ColumnVector::Bytes { .. } => ColumnVector::Bytes {
                        offsets: out_offsets,
                        data: out_data,
                        nulls,
                    },
                    ColumnVector::Json { .. } => ColumnVector::Json {
                        offsets: out_offsets,
                        data: out_data,
                        nulls,
                    },
                    ColumnVector::Geometry { .. } => ColumnVector::Geometry {
                        offsets: out_offsets,
                        data: out_data,
                        nulls,
                    },
                    ColumnVector::Vector { .. } => ColumnVector::Vector {
                        offsets: out_offsets,
                        data: out_data,
                        nulls,
                    },
                    _ => unreachable!(),
                }
            }
            ColumnVector::Generic { values } => ColumnVector::Generic {
                values: indices
                    .iter()
                    .map(|&idx| {
                        if idx == NULL_IDX {
                            Value::Null
                        } else {
                            values[idx as usize].clone()
                        }
                    })
                    .collect(),
            },
        }
    }

    /// Append the listed rows of `src` to `self`, keeping the typed lane
    /// when both sides agree. Lane / decimal-scale mismatch promotes
    /// `self` losslessly to `Generic` (a fallback for cross-batch lane drift, e.g.
    /// a batch that happens to be all NULL and derives a `Null` lane). The hash
    /// join build side uses it to accumulate batched input into one contiguous typed column.
    pub fn append_rows_from(&mut self, src: &ColumnVector, rows: &[usize]) {
        if rows.is_empty() {
            return;
        }
        // Self is still an empty Null lane (initial state) and src is a typed lane:
        // swap in an empty same-type lane and take the typed append path.
        if matches!(self, ColumnVector::Null { len: 0 })
            && !matches!(src, ColumnVector::Null { .. })
        {
            *self = src.empty_like();
        }
        macro_rules! append_fixed {
            ($data:expr, $nulls:expr, $src_data:expr, $src_nulls:expr) => {{
                let base = $data.len();
                if let Some(sn) = $src_nulls.as_ref() {
                    let has_any_new_null = rows.iter().any(|&r| !sn.get(r));
                    if has_any_new_null && $nulls.is_none() {
                        *$nulls = Some(Bitmap::new_all_valid(base));
                    }
                }
                match ($nulls.as_mut(), $src_nulls.as_ref()) {
                    (Some(nulls), Some(sn)) => {
                        for &r in rows {
                            $data.push($src_data[r]);
                            nulls.push(sn.get(r));
                        }
                    }
                    (Some(nulls), None) => {
                        for &r in rows {
                            $data.push($src_data[r]);
                            nulls.push(true);
                        }
                    }
                    (None, _) => {
                        // src has no NULLs among the selected rows (checked above); pure data copy.
                        for &r in rows {
                            $data.push($src_data[r]);
                        }
                    }
                }
            }};
        }
        let appended = match (&mut *self, src) {
            (ColumnVector::Null { len }, ColumnVector::Null { .. }) => {
                *len += rows.len();
                true
            }
            (
                ColumnVector::I64 { data, nulls },
                ColumnVector::I64 {
                    data: sd,
                    nulls: sn,
                },
            ) => {
                append_fixed!(data, nulls, sd, sn);
                true
            }
            (
                ColumnVector::U64 { data, nulls },
                ColumnVector::U64 {
                    data: sd,
                    nulls: sn,
                },
            ) => {
                append_fixed!(data, nulls, sd, sn);
                true
            }
            (
                ColumnVector::F64 { data, nulls },
                ColumnVector::F64 {
                    data: sd,
                    nulls: sn,
                },
            ) => {
                append_fixed!(data, nulls, sd, sn);
                true
            }
            (
                ColumnVector::Date32 { data, nulls },
                ColumnVector::Date32 {
                    data: sd,
                    nulls: sn,
                },
            ) => {
                append_fixed!(data, nulls, sd, sn);
                true
            }
            (
                ColumnVector::Time64 { data, nulls },
                ColumnVector::Time64 {
                    data: sd,
                    nulls: sn,
                },
            ) => {
                append_fixed!(data, nulls, sd, sn);
                true
            }
            (
                ColumnVector::DateTime64 { data, nulls },
                ColumnVector::DateTime64 {
                    data: sd,
                    nulls: sn,
                },
            ) => {
                append_fixed!(data, nulls, sd, sn);
                true
            }
            (
                ColumnVector::Timestamp64 { data, nulls },
                ColumnVector::Timestamp64 {
                    data: sd,
                    nulls: sn,
                },
            ) => {
                append_fixed!(data, nulls, sd, sn);
                true
            }
            (
                ColumnVector::Decimal32 { data, scale, nulls },
                ColumnVector::Decimal32 {
                    data: sd,
                    scale: ss,
                    nulls: sn,
                },
            ) if *scale == *ss => {
                append_fixed!(data, nulls, sd, sn);
                true
            }
            (
                ColumnVector::Decimal64 { data, scale, nulls },
                ColumnVector::Decimal64 {
                    data: sd,
                    scale: ss,
                    nulls: sn,
                },
            ) if *scale == *ss => {
                append_fixed!(data, nulls, sd, sn);
                true
            }
            (
                ColumnVector::Decimal128 { data, scale, nulls },
                ColumnVector::Decimal128 {
                    data: sd,
                    scale: ss,
                    nulls: sn,
                },
            ) if *scale == *ss => {
                append_fixed!(data, nulls, sd, sn);
                true
            }
            (
                ColumnVector::Bool { values, nulls },
                ColumnVector::Bool {
                    values: sv,
                    nulls: sn,
                },
            ) => {
                let base = values.len();
                if let Some(snb) = sn.as_ref() {
                    if nulls.is_none() && rows.iter().any(|&r| !snb.get(r)) {
                        *nulls = Some(Bitmap::new_all_valid(base));
                    }
                }
                for &r in rows {
                    values.push(sv.get(r));
                    if let Some(nb) = nulls.as_mut() {
                        nb.push(sn.as_ref().map(|b| b.get(r)).unwrap_or(true));
                    }
                }
                true
            }
            (
                ColumnVector::Utf8 {
                    offsets,
                    data,
                    nulls,
                },
                ColumnVector::Utf8 {
                    offsets: so,
                    data: sd,
                    nulls: sn,
                },
            )
            | (
                ColumnVector::Bytes {
                    offsets,
                    data,
                    nulls,
                },
                ColumnVector::Bytes {
                    offsets: so,
                    data: sd,
                    nulls: sn,
                },
            ) => {
                let base = offsets.len() - 1;
                if let Some(snb) = sn.as_ref() {
                    if nulls.is_none() && rows.iter().any(|&r| !snb.get(r)) {
                        *nulls = Some(Bitmap::new_all_valid(base));
                    }
                }
                for &r in rows {
                    let is_null = sn.as_ref().map(|b| !b.get(r)).unwrap_or(false);
                    if !is_null {
                        data.extend_from_slice(&sd[so[r] as usize..so[r + 1] as usize]);
                    }
                    offsets.push(data.len() as u32);
                    if let Some(nb) = nulls.as_mut() {
                        nb.push(!is_null);
                    }
                }
                true
            }
            (
                ColumnVector::Json {
                    offsets,
                    data,
                    nulls,
                },
                ColumnVector::Json {
                    offsets: so,
                    data: sd,
                    nulls: sn,
                },
            )
            | (
                ColumnVector::Geometry {
                    offsets,
                    data,
                    nulls,
                },
                ColumnVector::Geometry {
                    offsets: so,
                    data: sd,
                    nulls: sn,
                },
            )
            | (
                ColumnVector::Vector {
                    offsets,
                    data,
                    nulls,
                },
                ColumnVector::Vector {
                    offsets: so,
                    data: sd,
                    nulls: sn,
                },
            ) => {
                let base = offsets.len() - 1;
                if let Some(snb) = sn.as_ref() {
                    if nulls.is_none() && rows.iter().any(|&r| !snb.get(r)) {
                        *nulls = Some(Bitmap::new_all_valid(base));
                    }
                }
                for &r in rows {
                    let is_null = sn.as_ref().map(|b| !b.get(r)).unwrap_or(false);
                    if !is_null {
                        data.extend_from_slice(&sd[so[r] as usize..so[r + 1] as usize]);
                    }
                    offsets.push(data.len() as u32);
                    if let Some(nb) = nulls.as_mut() {
                        nb.push(!is_null);
                    }
                }
                true
            }
            (ColumnVector::Generic { values }, _) => {
                values.extend(rows.iter().map(|&r| src.value_at(r)));
                true
            }
            _ => false,
        };
        if !appended {
            // Lane mismatch: losslessly degrade to Generic, then append.
            let mut values: Vec<Value> = Vec::with_capacity(self.len() + rows.len());
            for i in 0..self.len() {
                values.push(self.value_at(i));
            }
            values.extend(rows.iter().map(|&r| src.value_at(r)));
            *self = ColumnVector::Generic { values };
        }
    }

    /// Empty vector in the same lane as `self` (init helper for `append_rows_from`).
    fn empty_like(&self) -> ColumnVector {
        match self {
            ColumnVector::Null { .. } => ColumnVector::Null { len: 0 },
            ColumnVector::Bool { .. } => ColumnVector::Bool {
                values: Bitmap::new_all_invalid(0),
                nulls: None,
            },
            ColumnVector::I64 { .. } => ColumnVector::I64 {
                data: Vec::new(),
                nulls: None,
            },
            ColumnVector::U64 { .. } => ColumnVector::U64 {
                data: Vec::new(),
                nulls: None,
            },
            ColumnVector::F64 { .. } => ColumnVector::F64 {
                data: Vec::new(),
                nulls: None,
            },
            ColumnVector::Date32 { .. } => ColumnVector::Date32 {
                data: Vec::new(),
                nulls: None,
            },
            ColumnVector::Time64 { .. } => ColumnVector::Time64 {
                data: Vec::new(),
                nulls: None,
            },
            ColumnVector::DateTime64 { .. } => ColumnVector::DateTime64 {
                data: Vec::new(),
                nulls: None,
            },
            ColumnVector::Timestamp64 { .. } => ColumnVector::Timestamp64 {
                data: Vec::new(),
                nulls: None,
            },
            ColumnVector::Decimal32 { scale, .. } => ColumnVector::Decimal32 {
                data: Vec::new(),
                scale: *scale,
                nulls: None,
            },
            ColumnVector::Decimal64 { scale, .. } => ColumnVector::Decimal64 {
                data: Vec::new(),
                scale: *scale,
                nulls: None,
            },
            ColumnVector::Decimal128 { scale, .. } => ColumnVector::Decimal128 {
                data: Vec::new(),
                scale: *scale,
                nulls: None,
            },
            ColumnVector::DecimalWide { scale, .. } => ColumnVector::DecimalWide {
                data: Vec::new(),
                scale: *scale,
                nulls: None,
            },
            ColumnVector::Utf8 { .. } => ColumnVector::Utf8 {
                offsets: vec![0],
                data: Vec::new(),
                nulls: None,
            },
            ColumnVector::Bytes { .. } => ColumnVector::Bytes {
                offsets: vec![0],
                data: Vec::new(),
                nulls: None,
            },
            ColumnVector::Json { .. } => ColumnVector::Json {
                offsets: vec![0],
                data: Vec::new(),
                nulls: None,
            },
            ColumnVector::Geometry { .. } => ColumnVector::Geometry {
                offsets: vec![0],
                data: Vec::new(),
                nulls: None,
            },
            ColumnVector::Vector { .. } => ColumnVector::Vector {
                offsets: vec![0],
                data: Vec::new(),
                nulls: None,
            },
            ColumnVector::Generic { .. } => ColumnVector::Generic { values: Vec::new() },
        }
    }

    /// Construct a `ColumnVector` by pulling rows out of a `Value` iterator.
    /// Picks the narrowest typed lane that fits every value, falling back
    /// to `Generic` when types are mixed.
    pub fn from_values(values: Vec<Value>) -> Self {
        let mut kind = Kind::Unset;
        for v in &values {
            kind = kind.merge(value_kind(v));
            if kind == Kind::Generic {
                break;
            }
        }
        match kind {
            Kind::Unset | Kind::AllNull => ColumnVector::Null { len: values.len() },
            Kind::I64 => {
                let mut data = vec![0i64; values.len()];
                let mut nulls = Bitmap::new_all_valid(values.len());
                let mut any_null = false;
                for (i, v) in values.iter().enumerate() {
                    match v {
                        Value::Null => {
                            nulls.set(i, false);
                            any_null = true;
                        }
                        Value::Int(x) => data[i] = *x,
                        _ => unreachable!(),
                    }
                }
                ColumnVector::I64 {
                    data,
                    nulls: any_null.then_some(nulls),
                }
            }
            Kind::U64 => {
                let mut data = vec![0u64; values.len()];
                let mut nulls = Bitmap::new_all_valid(values.len());
                let mut any_null = false;
                for (i, v) in values.iter().enumerate() {
                    match v {
                        Value::Null => {
                            nulls.set(i, false);
                            any_null = true;
                        }
                        Value::UInt(x) => data[i] = *x,
                        _ => unreachable!(),
                    }
                }
                ColumnVector::U64 {
                    data,
                    nulls: any_null.then_some(nulls),
                }
            }
            Kind::F64 => {
                let mut data = vec![0f64; values.len()];
                let mut nulls = Bitmap::new_all_valid(values.len());
                let mut any_null = false;
                for (i, v) in values.iter().enumerate() {
                    match v {
                        Value::Null => {
                            nulls.set(i, false);
                            any_null = true;
                        }
                        Value::Float(x) => data[i] = *x as f64,
                        Value::Double(x) => data[i] = *x,
                        Value::Int(x) => data[i] = *x as f64,
                        Value::UInt(x) => data[i] = *x as f64,
                        _ => unreachable!(),
                    }
                }
                ColumnVector::F64 {
                    data,
                    nulls: any_null.then_some(nulls),
                }
            }
            Kind::Utf8 => {
                let mut offsets = Vec::with_capacity(values.len() + 1);
                let mut data = Vec::new();
                let mut nulls = Bitmap::new_all_valid(values.len());
                let mut any_null = false;
                offsets.push(0u32);
                for (i, v) in values.iter().enumerate() {
                    match v {
                        Value::Null => {
                            nulls.set(i, false);
                            any_null = true;
                        }
                        Value::String(s) => data.extend_from_slice(s.as_bytes()),
                        Value::SmallString(b) => data.extend_from_slice(b.as_slice()),
                        _ => unreachable!(),
                    }
                    offsets.push(data.len() as u32);
                }
                ColumnVector::Utf8 {
                    offsets,
                    data,
                    nulls: any_null.then_some(nulls),
                }
            }
            Kind::Decimal { scale } => {
                // Two-pass lane selection: scan magnitudes once to find
                // max_abs, then pick the narrowest SIMD-native lane that
                // fits. Per-row magnitude already fits in i64 because
                // `Value::DecimalCompact` carries an i64. Larger values
                // arrive via `Value::Decimal(String)` and fall through
                // to `Generic`.
                let mut max_abs: u128 = 0;
                let mut any_null = false;
                for v in &values {
                    match v {
                        Value::Null => any_null = true,
                        Value::DecimalCompact(m, _) => {
                            let abs = u128::from(m.unsigned_abs());
                            if abs > max_abs {
                                max_abs = abs;
                            }
                        }
                        Value::Decimal128(m, _) => {
                            let abs = m.unsigned_abs();
                            if abs > max_abs {
                                max_abs = abs;
                            }
                        }
                        _ => {} // unreachable per `value_kind`
                    }
                }
                let mut nulls = Bitmap::new_all_valid(values.len());
                if max_abs <= i32::MAX as u128 {
                    let mut data = vec![0i32; values.len()];
                    for (i, v) in values.iter().enumerate() {
                        match v {
                            Value::Null => nulls.set(i, false),
                            Value::DecimalCompact(m, _) => data[i] = *m as i32,
                            Value::Decimal128(m, _) => data[i] = *m as i32,
                            _ => unreachable!(),
                        }
                    }
                    ColumnVector::Decimal32 {
                        data,
                        scale,
                        nulls: any_null.then_some(nulls),
                    }
                } else if max_abs <= i64::MAX as u128 {
                    let mut data = vec![0i64; values.len()];
                    for (i, v) in values.iter().enumerate() {
                        match v {
                            Value::Null => nulls.set(i, false),
                            Value::DecimalCompact(m, _) => data[i] = *m,
                            Value::Decimal128(m, _) => data[i] = *m as i64,
                            _ => unreachable!(),
                        }
                    }
                    ColumnVector::Decimal64 {
                        data,
                        scale,
                        nulls: any_null.then_some(nulls),
                    }
                } else {
                    let mut data = vec![0i128; values.len()];
                    for (i, v) in values.iter().enumerate() {
                        match v {
                            Value::Null => nulls.set(i, false),
                            Value::DecimalCompact(m, _) => data[i] = i128::from(*m),
                            Value::Decimal128(m, _) => data[i] = *m,
                            _ => unreachable!(),
                        }
                    }
                    ColumnVector::Decimal128 {
                        data,
                        scale,
                        nulls: any_null.then_some(nulls),
                    }
                }
            }
            Kind::DecimalWide { scale } => {
                let mut data = Vec::with_capacity(values.len());
                let mut nulls = Bitmap::new_all_valid(values.len());
                let mut any_null = false;
                for (index, value) in values.into_iter().enumerate() {
                    match value {
                        Value::Null => {
                            any_null = true;
                            nulls.set(index, false);
                            data.push(crate::types::DecimalWide {
                                magnitude: 0u8.into(),
                                scale,
                            });
                        }
                        Value::DecimalWide(value) => data.push(*value),
                        _ => unreachable!(),
                    }
                }
                ColumnVector::DecimalWide {
                    data,
                    scale,
                    nulls: any_null.then_some(nulls),
                }
            }
            Kind::Date32 => {
                let mut data = vec![0i32; values.len()];
                let mut nulls = Bitmap::new_all_valid(values.len());
                let mut any_null = false;
                for (row, value) in values.iter().enumerate() {
                    match value {
                        Value::Null => {
                            nulls.set(row, false);
                            any_null = true;
                        }
                        Value::Date(value) => data[row] = *value,
                        _ => unreachable!(),
                    }
                }
                ColumnVector::Date32 {
                    data,
                    nulls: any_null.then_some(nulls),
                }
            }
            Kind::Time64 | Kind::DateTime64 | Kind::Timestamp64 => {
                let mut data = vec![0i64; values.len()];
                let mut nulls = Bitmap::new_all_valid(values.len());
                let mut any_null = false;
                for (row, value) in values.iter().enumerate() {
                    match value {
                        Value::Null => {
                            nulls.set(row, false);
                            any_null = true;
                        }
                        Value::Time(value) | Value::DateTime(value) | Value::Timestamp(value) => {
                            data[row] = *value
                        }
                        _ => unreachable!(),
                    }
                }
                let nulls = any_null.then_some(nulls);
                match kind {
                    Kind::Time64 => ColumnVector::Time64 { data, nulls },
                    Kind::DateTime64 => ColumnVector::DateTime64 { data, nulls },
                    Kind::Timestamp64 => ColumnVector::Timestamp64 { data, nulls },
                    _ => unreachable!(),
                }
            }
            Kind::Json => build_semantic_bytes(values, SemanticBytesKind::Json),
            Kind::Geometry => build_semantic_bytes(values, SemanticBytesKind::Geometry),
            Kind::Vector => build_semantic_bytes(values, SemanticBytesKind::Vector),
            Kind::Generic => ColumnVector::Generic { values },
        }
    }

    #[cfg(test)]
    /// Exchange rows at the given indices into a new `ColumnVector` of the
    /// same shape. Used by `Batch::take` and Top-N output materialisation.
    pub fn exchange(&self, indices: &[u32]) -> ColumnVector {
        // Slow general path. Typed callers can specialise.
        let values: Vec<Value> = indices.iter().map(|&i| self.value_at(i as usize)).collect();
        ColumnVector::from_values(values)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum Kind {
    Unset,
    AllNull,
    I64,
    U64,
    F64,
    /// All non-NULL rows are `Value::DecimalCompact(_, scale)` with the
    /// same `scale`. Lane width (Decimal32 vs Decimal64) is decided
    /// later from the actual magnitude range.
    Decimal {
        scale: u8,
    },
    DecimalWide {
        scale: u8,
    },
    Utf8,
    Date32,
    Time64,
    DateTime64,
    Timestamp64,
    Json,
    Geometry,
    Vector,
    Generic,
}

impl Kind {
    fn merge(self, other: Kind) -> Kind {
        use Kind::*;
        match (self, other) {
            (a, b) if a == b => a,
            (Unset, x) | (x, Unset) => x,
            (AllNull, x) | (x, AllNull) => x,
            // Two decimal kinds with different scales can't share a lane
            // (the vector stores one scale for the whole column), so
            // fall back to Generic. Same-scale decimal merges to itself
            // via the `a == b` arm above.
            (Decimal { .. }, _) | (_, Decimal { .. }) => Generic,
            (DecimalWide { .. }, _) | (_, DecimalWide { .. }) => Generic,
            // Mixed numeric kinds fall back to `Generic` rather than
            // silently widening to F64. The legacy aggregate kernels
            // preserve `Int` arithmetic when every input is an integer;
            // a column-wise upcast here would break that round-trip.
            // `update_state` in `vec::hash_agg` handles every numeric
            // variant on the `Generic` column path via `value_at`.
            _ => Generic,
        }
    }
}

fn value_kind(v: &Value) -> Kind {
    match v {
        Value::Null => Kind::AllNull,
        Value::Int(_) => Kind::I64,
        Value::UInt(_) => Kind::U64,
        Value::Float(_) | Value::Double(_) => Kind::F64,
        Value::DecimalCompact(_, scale) => Kind::Decimal { scale: *scale },
        Value::Decimal128(_, scale) => Kind::Decimal { scale: *scale },
        Value::DecimalWide(value) => Kind::DecimalWide { scale: value.scale },
        Value::String(_) | Value::SmallString(_) => Kind::Utf8,
        Value::Date(_) => Kind::Date32,
        Value::Time(_) => Kind::Time64,
        Value::DateTime(_) => Kind::DateTime64,
        Value::Timestamp(_) => Kind::Timestamp64,
        Value::Json(_) => Kind::Json,
        Value::Geometry(_) => Kind::Geometry,
        Value::Vector(_) => Kind::Vector,
        _ => Kind::Generic,
    }
}

#[derive(Clone, Copy)]
enum SemanticBytesKind {
    Json,
    Geometry,
    Vector,
}

fn build_semantic_bytes(values: Vec<Value>, kind: SemanticBytesKind) -> ColumnVector {
    let mut offsets = Vec::with_capacity(values.len() + 1);
    let mut data = Vec::new();
    let mut nulls = Bitmap::new_all_valid(values.len());
    let mut any_null = false;
    offsets.push(0);
    for (i, value) in values.iter().enumerate() {
        match value {
            Value::Null => {
                nulls.set(i, false);
                any_null = true;
            }
            Value::Json(bytes) if matches!(kind, SemanticBytesKind::Json) => {
                data.extend_from_slice(bytes)
            }
            Value::Geometry(bytes) if matches!(kind, SemanticBytesKind::Geometry) => {
                data.extend_from_slice(bytes)
            }
            Value::Vector(bytes) if matches!(kind, SemanticBytesKind::Vector) => {
                data.extend_from_slice(&bytes.encode())
            }
            _ => unreachable!(),
        }
        offsets.push(data.len() as u32);
    }
    let nulls = any_null.then_some(nulls);
    match kind {
        SemanticBytesKind::Json => ColumnVector::Json {
            offsets,
            data,
            nulls,
        },
        SemanticBytesKind::Geometry => ColumnVector::Geometry {
            offsets,
            data,
            nulls,
        },
        SemanticBytesKind::Vector => ColumnVector::Vector {
            offsets,
            data,
            nulls,
        },
    }
}

/// Align an i128 decimal magnitude carried at `from_scale` to the lane's
/// declared `to_scale`. Used by typed accumulator paths to coerce a
/// `Value::DecimalCompact(_, s)` into the canonical scale before it lands
/// in the i32 / i64 lane. Callers that picked the narrow lane are
/// expected to have guaranteed the value fits, but a broken contract
/// used to `wrapping_mul` — silently returning garbage into an
/// aggregator. Upscale now goes through `checked_mul` and saturates on
/// overflow so the resulting lane truncation is at least monotonic; the
/// downscale path delegates to the shared kernel so it rounds half-away
/// like `expr.rs`, matching the row-oriented aggregator's behaviour.
#[inline]
fn align_decimal_magnitude(magnitude: i128, from_scale: u8, to_scale: u8) -> i128 {
    use std::cmp::Ordering;
    match from_scale.cmp(&to_scale) {
        Ordering::Equal => magnitude,
        Ordering::Less => {
            let shift = match crate::util::decimal::pow10_i128((to_scale - from_scale) as u32) {
                Some(shift) => shift,
                None => {
                    debug_assert!(false, "decimal upscale beyond i128 range");
                    return if magnitude < 0 { i128::MIN } else { i128::MAX };
                }
            };
            magnitude.checked_mul(shift).unwrap_or_else(|| {
                debug_assert!(false, "decimal upscale overflows i128");
                if magnitude < 0 {
                    i128::MIN
                } else {
                    i128::MAX
                }
            })
        }
        Ordering::Greater => {
            crate::util::decimal::rescale_round_i128(magnitude, from_scale, to_scale).unwrap_or(0)
        }
    }
}

/// Row-selection vector. When a Batch carries a `Selection`, downstream
/// operators must iterate `selection.iter()` instead of `0..num_rows`.
#[derive(Debug, Clone)]
pub enum Selection {
    /// Contiguous half-open window represented without allocating an index
    /// vector. Used by LIMIT/OFFSET and dense slicing operators.
    Range { start: u32, len: u32 },
    /// Dense list of row indices into the underlying ColumnVector(s).
    Indices(Vec<u32>),
    /// Bit-packed mask aligned with row indices.
    Mask(Bitmap),
}

impl Selection {
    pub fn count(&self) -> usize {
        match self {
            Selection::Range { len, .. } => *len as usize,
            Selection::Indices(v) => v.len(),
            Selection::Mask(b) => b.count_ones(),
        }
    }

    pub fn iter(&self) -> SelectionIter<'_> {
        match self {
            Selection::Range { start, len } => {
                SelectionIter::Range(*start as usize..start.saturating_add(*len) as usize)
            }
            Selection::Indices(v) => SelectionIter::Indices(v.iter()),
            Selection::Mask(b) => SelectionIter::Mask(b.iter_true()),
        }
    }

    /// Intersect two physical-row selections while retaining compact Range
    /// and Mask representations where possible.
    pub fn intersect(self, other: Selection, num_rows: usize) -> Selection {
        match (self, other) {
            (
                Selection::Range {
                    start: left_start,
                    len: left_len,
                },
                Selection::Range {
                    start: right_start,
                    len: right_len,
                },
            ) => {
                let start = left_start.max(right_start);
                let end = left_start
                    .saturating_add(left_len)
                    .min(right_start.saturating_add(right_len));
                Selection::Range {
                    start,
                    len: end.saturating_sub(start),
                }
            }
            (Selection::Mask(left), Selection::Mask(right)) => Selection::Mask(left.and(&right)),
            (Selection::Range { start, len }, Selection::Mask(mut mask))
            | (Selection::Mask(mut mask), Selection::Range { start, len }) => {
                debug_assert_eq!(mask.len(), num_rows);
                mask.retain_range(start as usize, len as usize);
                Selection::Mask(mask)
            }
            (Selection::Indices(indices), Selection::Mask(mask))
            | (Selection::Mask(mask), Selection::Indices(indices)) => Selection::Indices(
                indices
                    .into_iter()
                    .filter(|index| (*index as usize) < num_rows && mask.get(*index as usize))
                    .collect(),
            ),
            (Selection::Range { start, len }, Selection::Indices(indices))
            | (Selection::Indices(indices), Selection::Range { start, len }) => {
                let end = start.saturating_add(len);
                Selection::Indices(
                    indices
                        .into_iter()
                        .filter(|index| *index >= start && *index < end)
                        .collect(),
                )
            }
            (Selection::Indices(left), Selection::Indices(right)) => {
                let right: std::collections::HashSet<u32> = right.into_iter().collect();
                Selection::Indices(
                    left.into_iter()
                        .filter(|index| right.contains(index))
                        .collect(),
                )
            }
        }
    }
}

pub enum SelectionIter<'a> {
    Range(std::ops::Range<usize>),
    Indices(std::slice::Iter<'a, u32>),
    Mask(crate::engine::executor::vec::bitmap::BitmapTrueIter<'a>),
}

impl<'a> Iterator for SelectionIter<'a> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        match self {
            SelectionIter::Range(it) => it.next(),
            SelectionIter::Indices(it) => it.next().map(|x| *x as usize),
            SelectionIter::Mask(it) => it.next(),
        }
    }
}

pub(crate) struct BatchCharge(
    crate::engine::executor::mem_budget::ScopedCharge,
    // A transform can add an owned allocation while retaining the original
    // column owner. Keep both until the final zero-copy view is dropped.
    Option<Arc<BatchCharge>>,
    Option<Arc<BatchCharge>>,
);

impl BatchCharge {
    pub(crate) fn shared(
        mut charge: crate::engine::executor::mem_budget::ScopedCharge,
    ) -> Result<Arc<Self>, crate::types::QueryError> {
        charge.add_force(std::mem::size_of::<Self>() + 16)?;
        Ok(Arc::new(Self(charge, None, None)))
    }
}

impl std::fmt::Debug for BatchCharge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BatchCharge")
            .field(&self.0.charged())
            .field(&self.1.is_some())
            .field(&self.2.is_some())
            .finish()
    }
}

/// Keep the allocation and its reservation together, including zero-copy
/// selection views and column moves through projections/window operators.
#[derive(Debug, Default)]
pub(crate) struct OwnedColumns {
    columns: Vec<ColumnVector>,
    charge: Option<Arc<BatchCharge>>,
    rows: usize,
}

impl std::ops::Deref for OwnedColumns {
    type Target = Vec<ColumnVector>;
    fn deref(&self) -> &Self::Target {
        &self.columns
    }
}

impl std::ops::DerefMut for OwnedColumns {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.columns
    }
}

impl OwnedColumns {
    pub(crate) fn attach_charge(
        &mut self,
        charge: crate::engine::executor::mem_budget::ScopedCharge,
    ) {
        self.charge = Some(Arc::new(BatchCharge(charge, self.charge.take(), None)));
    }

    fn try_clone(&self) -> Result<Self, crate::types::QueryError> {
        use crate::engine::executor::mem_budget;
        let mut charge = self
            .charge
            .as_ref()
            .map(|owner| owner.0.sibling())
            .unwrap_or_else(|| mem_budget::ScopedCharge::new(0));
        let bytes = self
            .columns
            .capacity()
            .saturating_mul(std::mem::size_of::<ColumnVector>())
            .saturating_add(
                self.columns
                    .iter()
                    .map(ColumnVector::memory_bytes)
                    .sum::<usize>(),
            );
        charge.add_force(bytes)?;
        let mut columns =
            mem_budget::try_vec_with_capacity(self.columns.len(), "shared batch column copy")?;
        for column in &self.columns {
            columns.push(column.try_clone_owned()?);
        }
        Ok(Self {
            columns,
            charge: Some(Arc::new(BatchCharge(charge, None, None))),
            rows: self.rows,
        })
    }

    pub(crate) fn into_parts(self) -> (Vec<ColumnVector>, Option<Arc<BatchCharge>>) {
        (self.columns, self.charge)
    }

    pub(crate) fn from_parts(
        columns: Vec<ColumnVector>,
        charge: Option<Arc<BatchCharge>>,
        rows: usize,
    ) -> Self {
        Self {
            columns,
            charge,
            rows,
        }
    }

    pub(crate) fn replace_columns(mut self, columns: Vec<ColumnVector>) -> Self {
        self.columns = columns;
        self
    }
}

/// A single morsel of columnar data flowing through the executor.
#[derive(Debug, Clone)]
pub struct Batch {
    pub(crate) schema: Arc<BatchSchema>,
    /// Immutable column buffers shared by selection-only batch views.
    ///
    /// Boolean predicate evaluation repeatedly narrows the visible rows while
    /// retaining the same physical columns.  Keeping the column vector behind
    /// an `Arc` makes those views O(1) instead of cloning every fixed/variable
    /// width buffer at each AND/OR node.  The one executor path that rewrites a
    /// column uses `Arc::make_mut`, so an independently owned batch still
    /// mutates in place while a shared batch preserves value semantics.
    pub(crate) columns: Arc<OwnedColumns>,
    pub(crate) num_rows: usize,
    pub(crate) selection: Option<Selection>,
}

impl Batch {
    pub fn new(schema: Arc<BatchSchema>, columns: Vec<ColumnVector>) -> Self {
        let num_rows = columns.first().map(|c| c.len()).unwrap_or(0);
        debug_assert!(
            columns.iter().all(|c| c.len() == num_rows),
            "all columns must have equal length"
        );
        Self {
            schema,
            columns: Arc::new(OwnedColumns {
                columns,
                charge: None,
                rows: num_rows,
            }),
            num_rows,
            selection: None,
        }
    }

    /// [`Batch::new`] derives the row count from the first column,
    /// which reads as 0 for a zero-column projection — e.g. a bare
    /// `COUNT(*)` scan that materialises no payload columns. This
    /// constructor takes the row count explicitly so column-free
    /// batches stay countable.
    pub fn with_row_count(
        schema: Arc<BatchSchema>,
        columns: Vec<ColumnVector>,
        num_rows: usize,
    ) -> Self {
        debug_assert!(
            columns.iter().all(|c| c.len() == num_rows),
            "all columns must have equal length"
        );
        Self {
            schema,
            columns: Arc::new(OwnedColumns {
                columns,
                charge: None,
                rows: num_rows,
            }),
            num_rows,
            selection: None,
        }
    }

    #[cfg(test)]
    pub fn empty(schema: Arc<BatchSchema>) -> Self {
        Self {
            schema,
            columns: Arc::new(OwnedColumns::default()),
            num_rows: 0,
            selection: None,
        }
    }

    pub fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    pub fn columns(&self) -> &[ColumnVector] {
        &self.columns
    }

    /// Move out uniquely owned columns, cloning only when another batch view
    /// still shares them.  Operators that consume and reshape a batch should
    /// use this instead of assuming `Batch::clone` duplicated every buffer.
    pub(crate) fn take_columns(&mut self) -> Result<OwnedColumns, crate::types::QueryError> {
        match Arc::try_unwrap(std::mem::take(&mut self.columns)) {
            Ok(columns) => Ok(columns),
            Err(columns) => columns.try_clone(),
        }
    }

    pub(crate) fn into_columns(self) -> Result<OwnedColumns, crate::types::QueryError> {
        match Arc::try_unwrap(self.columns) {
            Ok(columns) => Ok(columns),
            Err(columns) => columns.try_clone(),
        }
    }

    pub(crate) fn from_owned_columns(schema: Arc<BatchSchema>, columns: OwnedColumns) -> Self {
        let num_rows = columns.rows;
        Self {
            schema,
            columns: Arc::new(columns),
            num_rows,
            selection: None,
        }
    }

    pub(crate) fn memory_charge(&self) -> crate::engine::executor::mem_budget::ScopedCharge {
        self.columns
            .charge
            .as_ref()
            .map(|charge| charge.0.sibling())
            .unwrap_or_else(|| crate::engine::executor::mem_budget::ScopedCharge::new(0))
    }

    pub(crate) fn with_shared_charge(
        mut self,
        owner: Arc<BatchCharge>,
    ) -> Result<Self, crate::types::QueryError> {
        let columns =
            Arc::get_mut(&mut self.columns).expect("fresh output batch has unique columns");
        columns.charge = Some(match columns.charge.take() {
            None => owner,
            Some(existing) => {
                let mut metadata = owner.0.sibling();
                metadata.add_force(std::mem::size_of::<BatchCharge>() + 16)?;
                Arc::new(BatchCharge(metadata, Some(existing), Some(owner)))
            }
        });
        Ok(self)
    }

    pub(crate) fn with_memory_charge(
        mut self,
        mut charge: crate::engine::executor::mem_budget::ScopedCharge,
    ) -> Result<Self, crate::types::QueryError> {
        let actual = self
            .memory_bytes()
            .saturating_add(
                self.columns
                    .capacity()
                    .saturating_mul(std::mem::size_of::<ColumnVector>()),
            )
            .saturating_add(128);
        if actual > charge.charged() {
            if let Err(error) = charge.add_force(actual - charge.charged()) {
                // An error must destroy the columns before their transferred
                // reservation is released to another concurrent consumer.
                drop(self);
                return Err(error);
            }
        } else {
            charge.release_bytes(charge.charged() - actual);
        }
        Arc::get_mut(&mut self.columns)
            .expect("newly decoded columns are unique")
            .charge = Some(Arc::new(BatchCharge(charge, None, None)));
        Ok(self)
    }

    pub(crate) fn try_columns_mut(
        &mut self,
    ) -> Result<&mut Vec<ColumnVector>, crate::types::QueryError> {
        if Arc::get_mut(&mut self.columns).is_none() {
            self.columns = Arc::new(self.columns.try_clone()?);
        }
        Ok(&mut Arc::get_mut(&mut self.columns)
            .expect("unique columns after copy")
            .columns)
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Estimated heap bytes across all columns (see
    /// [`ColumnVector::memory_bytes`]). Drives the vectorized hash
    /// join's build-side size cap (P0/B1).
    pub fn memory_bytes(&self) -> usize {
        self.columns
            .iter()
            .fold(0usize, |acc, c| acc.saturating_add(c.memory_bytes()))
    }

    pub fn selection(&self) -> Option<&Selection> {
        self.selection.as_ref()
    }

    /// Number of logical rows visible after applying any selection.
    pub fn visible_rows(&self) -> usize {
        match &self.selection {
            Some(sel) => sel.count(),
            None => self.num_rows,
        }
    }

    /// Iterate logical row indices without materialising an intermediate
    /// `Vec`. Operators that only need a single pass should use this instead
    /// of collecting `selection.iter()`.
    pub fn visible_indices(&self) -> BatchVisibleIter<'_> {
        match &self.selection {
            Some(selection) => BatchVisibleIter::Selected(selection.iter()),
            None => BatchVisibleIter::All(0..self.num_rows),
        }
    }

    pub fn with_selection(mut self, sel: Selection) -> Self {
        self.selection = Some(sel);
        self
    }

    /// Materialise the row at `physical_idx` as an owned `Vec<Value>`.
    /// Hot paths should avoid this and go through column slices.
    pub fn row(&self, physical_idx: usize) -> Vec<Value> {
        self.columns
            .iter()
            .zip(self.schema.fields())
            .map(|(c, field)| c.typed_value_at(physical_idx, field.column_type))
            .collect()
    }
}

pub enum BatchVisibleIter<'a> {
    All(std::ops::Range<usize>),
    Selected(SelectionIter<'a>),
}

impl Iterator for BatchVisibleIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BatchVisibleIter::All(iter) => iter.next(),
            BatchVisibleIter::Selected(iter) => iter.next(),
        }
    }
}

// ---------------------------------------------------------------
// Schema-aware columnar accumulator
// ---------------------------------------------------------------

use crate::types::ColumnType;

/// Schema-driven columnar accumulator that pushes raw data extracted from
/// `&Value` references directly into typed buffers — no `Value::clone`,
/// no type-inference pass. Created once per column per batch; call
/// [`push`] for each row, then [`finish`] to produce a `ColumnVector`.
pub enum ColumnAccumulator {
    I64 {
        data: Vec<i64>,
        nulls: Bitmap,
        any_null: bool,
    },
    U64 {
        data: Vec<u64>,
        nulls: Bitmap,
        any_null: bool,
    },
    F64 {
        data: Vec<f64>,
        nulls: Bitmap,
        any_null: bool,
    },
    Date32 {
        data: Vec<i32>,
        nulls: Bitmap,
        any_null: bool,
    },
    Time64 {
        data: Vec<i64>,
        nulls: Bitmap,
        any_null: bool,
    },
    DateTime64 {
        data: Vec<i64>,
        nulls: Bitmap,
        any_null: bool,
    },
    Timestamp64 {
        data: Vec<i64>,
        nulls: Bitmap,
        any_null: bool,
    },
    /// Compile-time-narrow DECIMAL accumulators. `ColumnAccumulator::for_type`
    /// picks `Decimal32` for `DECIMAL(P≤9, S)`, `Decimal64` for
    /// `DECIMAL(P≤18, S)`, and falls back to `Decimal128` only for
    /// `DECIMAL(P>18, S)`. The narrow variants store values straight in
    /// their SIMD-native lane, skipping the i128 → narrow conversion that
    /// the legacy max-abs path performs at `finish()`.
    Decimal32 {
        data: Vec<i32>,
        scale: u8,
        nulls: Bitmap,
        any_null: bool,
    },
    Decimal64 {
        data: Vec<i64>,
        scale: u8,
        nulls: Bitmap,
        any_null: bool,
    },
    /// Generic DECIMAL accumulator: stores values as i128 and tracks the
    /// maximum observed absolute value so `finish()` can still downgrade
    /// to a narrower lane when the caller used the legacy `for_type`
    /// path without precision info.
    Decimal128 {
        data: Vec<i128>,
        scale: Option<u8>,
        nulls: Bitmap,
        any_null: bool,
        /// Maximum |value| observed across non-NULL rows. Maintained
        /// incrementally on each `push` so `finish` can pick the lane in
        /// O(1) instead of scanning `data` again.
        max_abs: u128,
    },
    Utf8 {
        offsets: Vec<u32>,
        data: Vec<u8>,
        nulls: Bitmap,
        any_null: bool,
    },
    Bytes {
        offsets: Vec<u32>,
        data: Vec<u8>,
        nulls: Bitmap,
        any_null: bool,
    },
    Json {
        offsets: Vec<u32>,
        data: Vec<u8>,
        nulls: Bitmap,
        any_null: bool,
    },
    Geometry {
        offsets: Vec<u32>,
        data: Vec<u8>,
        nulls: Bitmap,
        any_null: bool,
    },
    Vector {
        offsets: Vec<u32>,
        data: Vec<u8>,
        nulls: Bitmap,
        any_null: bool,
    },
    /// Fallback for types that can't be mapped to a typed lane.
    Generic { values: Vec<Value> },
}

impl ColumnAccumulator {
    pub fn for_field(field: &super::schema::BatchField, capacity: usize) -> Self {
        let unsigned = field.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0;
        if field.source.flags
            & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
            != 0
        {
            return ColumnAccumulator::Generic {
                values: Vec::with_capacity(capacity),
            };
        }
        match field.column_type {
            ColumnType::Tiny
            | ColumnType::Short
            | ColumnType::MediumInt
            | ColumnType::Long
            | ColumnType::LongLong
                if unsigned =>
            {
                Self::u64(capacity)
            }
            _ => Self::for_type(field.column_type, capacity),
        }
    }

    /// Append a lossless integer leaf projection without constructing a
    /// short-lived `Value` envelope for every covering-index entry.
    #[inline]
    pub(crate) fn push_lossless_i64(&mut self, value: Option<i64>) {
        match self {
            ColumnAccumulator::I64 {
                data,
                nulls,
                any_null,
            } => {
                let index = data.len();
                if let Some(value) = value {
                    data.push(value);
                } else {
                    data.push(0);
                    nulls.set_null_growing(index);
                    *any_null = true;
                }
            }
            ColumnAccumulator::U64 {
                data,
                nulls,
                any_null,
            } => {
                let index = data.len();
                if let Some(value) = value {
                    data.push(value as u64);
                } else {
                    data.push(0);
                    nulls.set_null_growing(index);
                    *any_null = true;
                }
            }
            _ => match value {
                Some(value) => self.push(&Value::Int(value)),
                None => self.push(&Value::Null),
            },
        }
    }

    fn u64(capacity: usize) -> Self {
        ColumnAccumulator::U64 {
            data: Vec::with_capacity(capacity),
            nulls: Bitmap::new_all_valid(capacity),
            any_null: false,
        }
    }

    fn semantic_bytes(kind: SemanticBytesKind, capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);
        let nulls = Bitmap::new_all_valid(capacity);
        match kind {
            SemanticBytesKind::Json => ColumnAccumulator::Json {
                offsets,
                data: Vec::new(),
                nulls,
                any_null: false,
            },
            SemanticBytesKind::Geometry => ColumnAccumulator::Geometry {
                offsets,
                data: Vec::new(),
                nulls,
                any_null: false,
            },
            SemanticBytesKind::Vector => ColumnAccumulator::Vector {
                offsets,
                data: Vec::new(),
                nulls,
                any_null: false,
            },
        }
    }

    /// Create an accumulator for the given schema type, pre-sized for
    /// `capacity` rows.
    pub fn for_type(col_type: ColumnType, capacity: usize) -> Self {
        match col_type {
            ColumnType::Tiny
            | ColumnType::Short
            | ColumnType::MediumInt
            | ColumnType::Long
            | ColumnType::LongLong => ColumnAccumulator::I64 {
                data: Vec::with_capacity(capacity),
                nulls: Bitmap::new_all_valid(capacity),
                any_null: false,
            },
            // YEAR has no i64 lane: the I64 accumulator's `push` only knows
            // `Value::Int/UInt`, so a `Value::Year` would fall to its catch-all
            // and be silently NULLed. Route it through the `Generic` lane (as
            // `ColumnVector::from_values` already does) so the `Value::Year`
            // round-trips losslessly. Gives up per-column vectorisation for
            // YEAR but yields correct results.
            ColumnType::Year => ColumnAccumulator::Generic {
                values: Vec::with_capacity(capacity),
            },
            ColumnType::Float | ColumnType::Double => ColumnAccumulator::F64 {
                data: Vec::with_capacity(capacity),
                nulls: Bitmap::new_all_valid(capacity),
                any_null: false,
            },
            ColumnType::Date => ColumnAccumulator::Date32 {
                data: Vec::with_capacity(capacity),
                nulls: Bitmap::new_all_valid(capacity),
                any_null: false,
            },
            ColumnType::Time => ColumnAccumulator::Time64 {
                data: Vec::with_capacity(capacity),
                nulls: Bitmap::new_all_valid(capacity),
                any_null: false,
            },
            ColumnType::DateTime => ColumnAccumulator::DateTime64 {
                data: Vec::with_capacity(capacity),
                nulls: Bitmap::new_all_valid(capacity),
                any_null: false,
            },
            // TIMESTAMP carries a UTC instant and must retain its distinct
            // Value variant until the session boundary applies time_zone.
            // DateTime64 materialises as Value::DateTime, so use the generic
            // lane until a dedicated Timestamp64 vector lane exists.
            ColumnType::Timestamp => ColumnAccumulator::Timestamp64 {
                data: Vec::with_capacity(capacity),
                nulls: Bitmap::new_all_valid(capacity),
                any_null: false,
            },
            ColumnType::Decimal { precision, scale } => {
                // Pick the narrowest accumulator width that fits every
                // possible DECIMAL(P) value:
                //   P ≤  9 → i32 (max 9 × 9 = 999_999_999  < i32::MAX)
                //   P ≤ 18 → i64 (max 18 × 9 ≈ 10^18 ≈ i64::MAX)
                //   P ≤ 38 → i128
                // The known-scale path skips the per-row `Option<u8>`
                // initialisation that the legacy Decimal128 accumulator
                // used to discover scale at runtime.
                if precision <= 9 {
                    ColumnAccumulator::Decimal32 {
                        data: Vec::with_capacity(capacity),
                        scale,
                        nulls: Bitmap::new_all_valid(capacity),
                        any_null: false,
                    }
                } else if precision <= 18 {
                    ColumnAccumulator::Decimal64 {
                        data: Vec::with_capacity(capacity),
                        scale,
                        nulls: Bitmap::new_all_valid(capacity),
                        any_null: false,
                    }
                } else if precision <= 38 {
                    ColumnAccumulator::Decimal128 {
                        data: Vec::with_capacity(capacity),
                        scale: Some(scale),
                        nulls: Bitmap::new_all_valid(capacity),
                        any_null: false,
                        max_abs: 0,
                    }
                } else {
                    // MySQL DECIMAL supports precision beyond the i128
                    // domain. Keep those values in their exact Value form;
                    // a typed Decimal128 lane would otherwise silently turn
                    // DecimalWide into NULL during a storage-direct scan.
                    ColumnAccumulator::Generic {
                        values: Vec::with_capacity(capacity),
                    }
                }
            }
            ColumnType::VarString => ColumnAccumulator::Utf8 {
                offsets: {
                    let mut v = Vec::with_capacity(capacity + 1);
                    v.push(0);
                    v
                },
                data: Vec::new(),
                nulls: Bitmap::new_all_valid(capacity),
                any_null: false,
            },
            ColumnType::Json => Self::semantic_bytes(SemanticBytesKind::Json, capacity),
            ColumnType::Blob => ColumnAccumulator::Bytes {
                offsets: {
                    let mut v = Vec::with_capacity(capacity + 1);
                    v.push(0);
                    v
                },
                data: Vec::new(),
                nulls: Bitmap::new_all_valid(capacity),
                any_null: false,
            },
            // VECTOR / GEOMETRY must keep their typed `Value` variant
            // (`Value::Vector` / `Value::Geometry`) so downstream operators
            // see exactly what the row pipeline does. The byte-backed `Bytes`
            // lane is type-erased: its `value_at` always yields `Value::Bytes`,
            // and — worse — its `push(&Value)` only recognises `Value::Bytes`,
            // silently dropping a `Value::Vector` to a zero-length entry (which
            // then fails VECTOR validation downstream). The `Generic` lane
            // round-trips the value losslessly; the byte-direct `put_vector` /
            // `put_geometry` fast paths fall through to its `other =>` arm.
            ColumnType::Geometry => Self::semantic_bytes(SemanticBytesKind::Geometry, capacity),
            ColumnType::Vector => Self::semantic_bytes(SemanticBytesKind::Vector, capacity),
            // BIT is numeric but *unsigned* up to 64 bits: the I64 lane's
            // `push` stores `Value::UInt(x)` as `x as i64`, which flips
            // BIT(64) values ≥ 2^63 negative and diverges from the row-wise
            // oracle (filter/order/aggregate all mis-order). Route through
            // the `Generic` lane so `Value::UInt` round-trips losslessly —
            // gives up per-column vectorisation for BIT (a rare column type)
            // but keeps vec == row-wise for the full unsigned range.
            ColumnType::Bit { .. } => Self::u64(capacity),
        }
    }

    /// Push a single value. The value is borrowed — raw data is extracted
    /// and written directly into the typed buffer without cloning.
    #[inline]
    pub fn push(&mut self, value: &Value) {
        match self {
            ColumnAccumulator::I64 {
                data,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                match value {
                    Value::Null => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                    Value::Int(x) => data.push(*x),
                    Value::UInt(x) => data.push(*x as i64),
                    // Unexpected type: treat as fallback (should not happen
                    // when schema types match, but safety first).
                    _ => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                }
            }
            ColumnAccumulator::U64 {
                data,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                match value {
                    Value::Null => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                    Value::UInt(x) => data.push(*x),
                    Value::Int(x) if *x >= 0 => data.push(*x as u64),
                    _ => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                }
            }
            ColumnAccumulator::F64 {
                data,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                match value {
                    Value::Null => {
                        data.push(0.0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                    Value::Double(x) => data.push(*x),
                    Value::Float(x) => data.push(*x as f64),
                    Value::Int(x) => data.push(*x as f64),
                    Value::UInt(x) => data.push(*x as f64),
                    _ => {
                        data.push(0.0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                }
            }
            ColumnAccumulator::Date32 {
                data,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                match value {
                    Value::Null => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                    Value::Date(d) => data.push(*d),
                    _ => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                }
            }
            ColumnAccumulator::Time64 {
                data,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                match value {
                    Value::Null => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                    Value::Time(t) => data.push(*t),
                    _ => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                }
            }
            ColumnAccumulator::DateTime64 {
                data,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                match value {
                    Value::Null => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                    Value::DateTime(dt) => data.push(*dt),
                    _ => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                }
            }
            ColumnAccumulator::Timestamp64 {
                data,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                match value {
                    Value::Null => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                    Value::Timestamp(ts) => data.push(*ts),
                    _ => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                }
            }
            ColumnAccumulator::Decimal32 {
                data,
                scale: stored_scale,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                match value {
                    Value::Null => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                    Value::DecimalCompact(mag, s) => {
                        let aligned = align_decimal_magnitude(*mag as i128, *s, *stored_scale);
                        data.push(aligned as i32);
                    }
                    Value::Int(x) => data.push(*x as i32),
                    _ => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                }
            }
            ColumnAccumulator::Decimal64 {
                data,
                scale: stored_scale,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                match value {
                    Value::Null => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                    Value::DecimalCompact(mag, s) => {
                        let aligned = align_decimal_magnitude(*mag as i128, *s, *stored_scale);
                        data.push(aligned as i64);
                    }
                    Value::Int(x) => data.push(*x),
                    _ => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                }
            }
            ColumnAccumulator::Decimal128 {
                data,
                scale,
                nulls,
                any_null,
                max_abs,
            } => {
                let idx = data.len();
                match value {
                    Value::Null => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                    Value::DecimalCompact(mag, s) => {
                        let m = align_decimal_magnitude(*mag as i128, *s, scale.unwrap_or(*s));
                        data.push(m);
                        let abs = m.unsigned_abs();
                        if abs > *max_abs {
                            *max_abs = abs;
                        }
                        if scale.is_none() {
                            *scale = Some(*s);
                        }
                    }
                    Value::Decimal128(mag, s) => {
                        let m = align_decimal_magnitude(*mag, *s, scale.unwrap_or(*s));
                        data.push(m);
                        *max_abs = (*max_abs).max(m.unsigned_abs());
                        if scale.is_none() {
                            *scale = Some(*s);
                        }
                    }
                    Value::Decimal(text) => {
                        if let Some((mag, value_scale)) =
                            crate::util::decimal::parse_exact_decimal_str(text)
                        {
                            let m = align_decimal_magnitude(
                                mag,
                                value_scale,
                                scale.unwrap_or(value_scale),
                            );
                            data.push(m);
                            *max_abs = (*max_abs).max(m.unsigned_abs());
                            if scale.is_none() {
                                *scale = Some(value_scale);
                            }
                        } else {
                            data.push(0);
                            nulls.set_null_growing(idx);
                            *any_null = true;
                        }
                    }
                    // Legacy Decimal(String) — fall through to generic would
                    // be safest, but for now try to keep the typed lane.
                    Value::Int(x) => {
                        let m = *x as i128;
                        data.push(m);
                        let abs = m.unsigned_abs();
                        if abs > *max_abs {
                            *max_abs = abs;
                        }
                    }
                    _ => {
                        data.push(0);
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                }
            }
            ColumnAccumulator::Utf8 {
                offsets,
                data,
                nulls,
                any_null,
            } => {
                let idx = offsets.len() - 1;
                match value {
                    Value::Null => {
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                    Value::String(s) => data.extend_from_slice(s.as_bytes()),
                    Value::SmallString(b) => data.extend_from_slice(b.as_slice()),
                    // Decimal(String) stored as Utf8 in older schemas
                    Value::Decimal(s) => data.extend_from_slice(s.as_bytes()),
                    _ => {
                        // unexpected: treat as empty string
                    }
                }
                offsets.push(data.len() as u32);
            }
            ColumnAccumulator::Bytes {
                offsets,
                data,
                nulls,
                any_null,
            } => {
                let idx = offsets.len() - 1;
                match value {
                    Value::Null => {
                        nulls.set_null_growing(idx);
                        *any_null = true;
                    }
                    Value::Bytes(b) => data.extend_from_slice(b),
                    _ => {}
                }
                offsets.push(data.len() as u32);
            }
            ColumnAccumulator::Json {
                offsets,
                data,
                nulls,
                any_null,
            }
            | ColumnAccumulator::Geometry {
                offsets,
                data,
                nulls,
                any_null,
            }
            | ColumnAccumulator::Vector {
                offsets,
                data,
                nulls,
                any_null,
            } => {
                let idx = offsets.len() - 1;
                let bytes = match value {
                    Value::Null => {
                        nulls.set_null_growing(idx);
                        *any_null = true;
                        None
                    }
                    Value::Json(bytes) | Value::Geometry(bytes) => Some(bytes.as_slice()),
                    Value::Vector(bytes) => {
                        data.extend_from_slice(&bytes.encode());
                        offsets.push(data.len() as u32);
                        return;
                    }
                    _ => None,
                };
                if let Some(bytes) = bytes {
                    data.extend_from_slice(bytes);
                }
                offsets.push(data.len() as u32);
            }
            ColumnAccumulator::Generic { values } => {
                values.push(value.clone());
            }
        }
    }

    /// Finalize into a `ColumnVector`, consuming the accumulator.
    /// The nulls bitmap was allocated at the nominal batch capacity: a
    /// trailing partial batch has fewer rows and MUST be truncated (a
    /// capacity-length bitmap makes `count_ones()` see phantom valid rows),
    /// while a leaf-granular producer may have appended more rows, whose
    /// lane must be extended to the data length.
    pub fn finish(self) -> ColumnVector {
        fn trim(mut nulls: Bitmap, rows: usize, any_null: bool) -> Option<Bitmap> {
            if !any_null {
                return None;
            }
            nulls.resize_valid(rows);
            Some(nulls)
        }
        match self {
            ColumnAccumulator::I64 {
                data,
                nulls,
                any_null,
            } => {
                if data.is_empty() {
                    return ColumnVector::Null { len: 0 };
                }
                let nulls = trim(nulls, data.len(), any_null);
                ColumnVector::I64 { data, nulls }
            }
            ColumnAccumulator::U64 {
                data,
                nulls,
                any_null,
            } => {
                if data.is_empty() {
                    return ColumnVector::Null { len: 0 };
                }
                let nulls = trim(nulls, data.len(), any_null);
                ColumnVector::U64 { data, nulls }
            }
            ColumnAccumulator::F64 {
                data,
                nulls,
                any_null,
            } => {
                if data.is_empty() {
                    return ColumnVector::Null { len: 0 };
                }
                let nulls = trim(nulls, data.len(), any_null);
                ColumnVector::F64 { data, nulls }
            }
            ColumnAccumulator::Date32 {
                data,
                nulls,
                any_null,
            } => {
                if data.is_empty() {
                    return ColumnVector::Null { len: 0 };
                }
                let nulls = trim(nulls, data.len(), any_null);
                ColumnVector::Date32 { data, nulls }
            }
            ColumnAccumulator::Time64 {
                data,
                nulls,
                any_null,
            } => {
                if data.is_empty() {
                    return ColumnVector::Null { len: 0 };
                }
                let nulls = trim(nulls, data.len(), any_null);
                ColumnVector::Time64 { data, nulls }
            }
            ColumnAccumulator::DateTime64 {
                data,
                nulls,
                any_null,
            } => {
                if data.is_empty() {
                    return ColumnVector::Null { len: 0 };
                }
                let nulls = trim(nulls, data.len(), any_null);
                ColumnVector::DateTime64 { data, nulls }
            }
            ColumnAccumulator::Timestamp64 {
                data,
                nulls,
                any_null,
            } => {
                if data.is_empty() {
                    return ColumnVector::Null { len: 0 };
                }
                let nulls = trim(nulls, data.len(), any_null);
                ColumnVector::Timestamp64 { data, nulls }
            }
            ColumnAccumulator::Decimal32 {
                data,
                scale,
                nulls,
                any_null,
            } => {
                if data.is_empty() {
                    return ColumnVector::Null { len: 0 };
                }
                let nulls = trim(nulls, data.len(), any_null);
                ColumnVector::Decimal32 { data, scale, nulls }
            }
            ColumnAccumulator::Decimal64 {
                data,
                scale,
                nulls,
                any_null,
            } => {
                if data.is_empty() {
                    return ColumnVector::Null { len: 0 };
                }
                let nulls = trim(nulls, data.len(), any_null);
                ColumnVector::Decimal64 { data, scale, nulls }
            }
            ColumnAccumulator::Decimal128 {
                data,
                scale,
                nulls,
                any_null,
                max_abs,
            } => {
                if data.is_empty() {
                    return ColumnVector::Null { len: 0 };
                }
                let scale = scale.unwrap_or(0);
                let nulls_out = trim(nulls, data.len(), any_null);
                // Pick the narrowest SIMD-native lane that fits every
                // observed magnitude. The `max_abs` cap is `i{N}::MAX as u128`
                // — `i32::MAX = 0x7FFFFFFF`, `i64::MAX = 0x7FFFFFFFFFFFFFFF`.
                // This is the entry point that opens AVX2/NEON SIMD on
                // downstream SUM/ADD/CMP kernels.
                if max_abs <= i32::MAX as u128 {
                    let narrowed: Vec<i32> = data.into_iter().map(|v| v as i32).collect();
                    ColumnVector::Decimal32 {
                        data: narrowed,
                        scale,
                        nulls: nulls_out,
                    }
                } else if max_abs <= i64::MAX as u128 {
                    let narrowed: Vec<i64> = data.into_iter().map(|v| v as i64).collect();
                    ColumnVector::Decimal64 {
                        data: narrowed,
                        scale,
                        nulls: nulls_out,
                    }
                } else {
                    ColumnVector::Decimal128 {
                        data,
                        scale,
                        nulls: nulls_out,
                    }
                }
            }
            ColumnAccumulator::Utf8 {
                offsets,
                data,
                nulls,
                any_null,
            } => {
                if offsets.len() <= 1 {
                    return ColumnVector::Null { len: 0 };
                }
                let nulls = trim(nulls, offsets.len() - 1, any_null);
                ColumnVector::Utf8 {
                    offsets,
                    data,
                    nulls,
                }
            }
            ColumnAccumulator::Bytes {
                offsets,
                data,
                nulls,
                any_null,
            } => {
                if offsets.len() <= 1 {
                    return ColumnVector::Null { len: 0 };
                }
                let nulls = trim(nulls, offsets.len() - 1, any_null);
                ColumnVector::Bytes {
                    offsets,
                    data,
                    nulls,
                }
            }
            ColumnAccumulator::Json {
                offsets,
                data,
                nulls,
                any_null,
            } => {
                if offsets.len() <= 1 {
                    return ColumnVector::Null { len: 0 };
                }
                let rows = offsets.len() - 1;
                ColumnVector::Json {
                    offsets,
                    data,
                    nulls: trim(nulls, rows, any_null),
                }
            }
            ColumnAccumulator::Geometry {
                offsets,
                data,
                nulls,
                any_null,
            } => {
                if offsets.len() <= 1 {
                    return ColumnVector::Null { len: 0 };
                }
                let rows = offsets.len() - 1;
                ColumnVector::Geometry {
                    offsets,
                    data,
                    nulls: trim(nulls, rows, any_null),
                }
            }
            ColumnAccumulator::Vector {
                offsets,
                data,
                nulls,
                any_null,
            } => {
                if offsets.len() <= 1 {
                    return ColumnVector::Null { len: 0 };
                }
                let rows = offsets.len() - 1;
                ColumnVector::Vector {
                    offsets,
                    data,
                    nulls: trim(nulls, rows, any_null),
                }
            }
            ColumnAccumulator::Generic { values } => {
                if values.is_empty() {
                    return ColumnVector::Null { len: 0 };
                }
                // Wide DECIMAL metadata selects the exact generic decoder,
                // but its observed coefficients often fit native lanes.
                // Recover those lanes after decoding so spill/CTE readers do
                // not force subsequent numeric windows and aggregates back
                // to scalar storage solely because declared precision is 65.
                if values.iter().all(|value| {
                    matches!(
                        value,
                        Value::Null | Value::DecimalCompact(..) | Value::Decimal128(..)
                    )
                }) {
                    return ColumnVector::from_values(values);
                }
                ColumnVector::Generic { values }
            }
        }
    }

    /// Finalize a storage-fed accumulator and validate a UTF-8 lane once as a
    /// contiguous buffer. Row offsets are also checked as character boundaries
    /// so invalid byte sequences cannot become valid by spanning two values.
    pub fn finish_validated(self) -> Result<ColumnVector, crate::types::QueryError> {
        let column = self.finish();
        if let ColumnVector::Utf8 { offsets, data, .. } = &column {
            let value = simdutf8::basic::from_utf8(data).map_err(|_| {
                crate::error::catalog::ER_GET_ERRNO.error("invalid utf8 in column payload")
            })?;
            if offsets
                .iter()
                .any(|&offset| !value.is_char_boundary(offset as usize))
            {
                return Err(crate::error::catalog::ER_GET_ERRNO
                    .error("invalid utf8 boundary in column payload"));
            }
        }
        Ok(column)
    }
}

/// Row-decode sink that writes straight into a slice of [`ColumnAccumulator`]
/// lanes. The vectorized scan bridge uses this to skip the per-row
/// `Vec<Value>` intermediate that [`ValueVecSink`] produces, eliminating
/// the `core::ptr::drop_in_place<Value>` 12 % self-time and
/// `ColumnAccumulator::push` 10 % self-time that the Q1 profile attributed
/// to materialising and dropping `Value` envelopes for every projected
/// column.
///
/// Each typed `put_*` matches on the destination lane and writes the raw
/// value directly. Lane / op mismatches (e.g. a `put_i64` against a Utf8
/// accumulator — unreachable when the storage column kind matches the
/// schema) fall back to the legacy [`ColumnAccumulator::push`] path so
/// behaviour stays byte-identical with the pre-sink code.
pub struct BatchAccumSink<'a> {
    prefix_limits: &'a [Option<usize>],
    accs: &'a mut [ColumnAccumulator],
    mapping: Option<&'a [usize]>,
}

impl<'a> BatchAccumSink<'a> {
    pub(crate) fn with_prefix_limits(mut self, limits: &'a [Option<usize>]) -> Self {
        self.prefix_limits = limits;
        self
    }
    #[inline]
    pub fn new(accs: &'a mut [ColumnAccumulator]) -> Self {
        Self {
            prefix_limits: &[],
            accs,
            mapping: None,
        }
    }

    /// Decode projection slot `i` directly into accumulator `mapping[i]`.
    /// Predicate-first scans use this for the output columns that were not
    /// already decoded into predicate scratch.
    #[inline]
    pub fn mapped(accs: &'a mut [ColumnAccumulator], mapping: &'a [usize]) -> Self {
        Self {
            prefix_limits: &[],
            accs,
            mapping: Some(mapping),
        }
    }

    #[inline]
    fn accumulator(&mut self, col: usize) -> &mut ColumnAccumulator {
        let index = self.mapping.map_or(col, |mapping| mapping[col]);
        &mut self.accs[index]
    }
}

impl<'a> crate::storage::RowDecodeSink for BatchAccumSink<'a> {
    fn column_prefix_limit(&self, col: usize) -> Option<usize> {
        let index = self.mapping.map_or(col, |mapping| mapping[col]);
        self.prefix_limits.get(index).copied().flatten()
    }
    #[inline]
    fn validates_utf8_in_put_string(&self) -> bool {
        true
    }

    #[inline]
    fn put_null(&mut self, col: usize) {
        match self.accumulator(col) {
            ColumnAccumulator::I64 {
                data,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                data.push(0);
                nulls.set(idx, false);
                *any_null = true;
            }
            ColumnAccumulator::U64 {
                data,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                data.push(0);
                nulls.set(idx, false);
                *any_null = true;
            }
            ColumnAccumulator::F64 {
                data,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                data.push(0.0);
                nulls.set(idx, false);
                *any_null = true;
            }
            ColumnAccumulator::Date32 {
                data,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                data.push(0);
                nulls.set(idx, false);
                *any_null = true;
            }
            ColumnAccumulator::Time64 {
                data,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                data.push(0);
                nulls.set(idx, false);
                *any_null = true;
            }
            ColumnAccumulator::DateTime64 {
                data,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                data.push(0);
                nulls.set(idx, false);
                *any_null = true;
            }
            ColumnAccumulator::Timestamp64 {
                data,
                nulls,
                any_null,
            } => {
                let idx = data.len();
                data.push(0);
                nulls.set(idx, false);
                *any_null = true;
            }
            ColumnAccumulator::Decimal32 {
                data,
                nulls,
                any_null,
                ..
            } => {
                let idx = data.len();
                data.push(0);
                nulls.set(idx, false);
                *any_null = true;
            }
            ColumnAccumulator::Decimal64 {
                data,
                nulls,
                any_null,
                ..
            } => {
                let idx = data.len();
                data.push(0);
                nulls.set(idx, false);
                *any_null = true;
            }
            ColumnAccumulator::Decimal128 {
                data,
                nulls,
                any_null,
                ..
            } => {
                let idx = data.len();
                data.push(0);
                nulls.set(idx, false);
                *any_null = true;
            }
            ColumnAccumulator::Utf8 {
                offsets,
                data,
                nulls,
                any_null,
            } => {
                let idx = offsets.len() - 1;
                nulls.set(idx, false);
                *any_null = true;
                offsets.push(data.len() as u32);
            }
            ColumnAccumulator::Bytes {
                offsets,
                data,
                nulls,
                any_null,
            } => {
                let idx = offsets.len() - 1;
                nulls.set(idx, false);
                *any_null = true;
                offsets.push(data.len() as u32);
            }
            ColumnAccumulator::Json {
                offsets,
                data,
                nulls,
                any_null,
            }
            | ColumnAccumulator::Geometry {
                offsets,
                data,
                nulls,
                any_null,
            }
            | ColumnAccumulator::Vector {
                offsets,
                data,
                nulls,
                any_null,
            } => {
                let idx = offsets.len() - 1;
                nulls.set(idx, false);
                *any_null = true;
                offsets.push(data.len() as u32);
            }
            ColumnAccumulator::Generic { values } => {
                values.push(Value::Null);
            }
        }
    }
    #[inline]
    fn put_i64(&mut self, col: usize, value: i64) {
        match self.accumulator(col) {
            ColumnAccumulator::I64 { data, .. } => data.push(value),
            ColumnAccumulator::F64 { data, .. } => data.push(value as f64),
            ColumnAccumulator::Decimal32 {
                data,
                scale: stored_scale,
                ..
            } => {
                let aligned = align_decimal_magnitude(value as i128, 0, *stored_scale);
                data.push(aligned as i32);
            }
            ColumnAccumulator::Decimal64 {
                data,
                scale: stored_scale,
                ..
            } => {
                let aligned = align_decimal_magnitude(value as i128, 0, *stored_scale);
                data.push(aligned as i64);
            }
            ColumnAccumulator::Decimal128 {
                data,
                scale,
                max_abs,
                ..
            } => {
                let m = value as i128;
                data.push(m);
                let abs = m.unsigned_abs();
                if abs > *max_abs {
                    *max_abs = abs;
                }
                if scale.is_none() {
                    *scale = Some(0);
                }
            }
            other => other.push(&Value::Int(value)),
        }
    }
    #[inline]
    fn put_u64(&mut self, col: usize, value: u64) {
        match self.accumulator(col) {
            ColumnAccumulator::U64 { data, .. } => data.push(value),
            ColumnAccumulator::I64 { data, .. } => data.push(value as i64),
            ColumnAccumulator::F64 { data, .. } => data.push(value as f64),
            other => other.push(&Value::UInt(value)),
        }
    }
    #[inline]
    fn put_f32(&mut self, col: usize, value: f32) {
        match self.accumulator(col) {
            ColumnAccumulator::F64 { data, .. } => data.push(value as f64),
            other => other.push(&Value::Float(value)),
        }
    }
    #[inline]
    fn put_f64(&mut self, col: usize, value: f64) {
        match self.accumulator(col) {
            ColumnAccumulator::F64 { data, .. } => data.push(value),
            other => other.push(&Value::Double(value)),
        }
    }
    #[inline(always)]
    fn put_decimal_compact(&mut self, col: usize, magnitude: i64, scale: u8) {
        match self.accumulator(col) {
            ColumnAccumulator::Decimal32 {
                data,
                scale: stored_scale,
                ..
            } => {
                if scale == *stored_scale {
                    data.push(magnitude as i32);
                } else {
                    let aligned = align_decimal_magnitude(magnitude as i128, scale, *stored_scale);
                    data.push(aligned as i32);
                }
            }
            ColumnAccumulator::Decimal64 {
                data,
                scale: stored_scale,
                ..
            } => {
                if scale == *stored_scale {
                    data.push(magnitude);
                } else {
                    let aligned = align_decimal_magnitude(magnitude as i128, scale, *stored_scale);
                    data.push(aligned as i64);
                }
            }
            ColumnAccumulator::Decimal128 {
                data,
                scale: opt_scale,
                max_abs,
                ..
            } => {
                let m = magnitude as i128;
                data.push(m);
                let abs = m.unsigned_abs();
                if abs > *max_abs {
                    *max_abs = abs;
                }
                if opt_scale.is_none() {
                    *opt_scale = Some(scale);
                }
            }
            other => other.push(&Value::DecimalCompact(magnitude, scale)),
        }
    }
    #[inline]
    fn put_decimal_wide(&mut self, col: usize, magnitude: i128, scale: u8) {
        // Demote to compact when it fits; otherwise route the wide value
        // through ValueVecSink semantics so the Decimal(String) lane
        // stays exercised for P>18 precisions.
        if let Ok(small) = i64::try_from(magnitude) {
            self.put_decimal_compact(col, small, scale);
            return;
        }
        match self.accumulator(col) {
            ColumnAccumulator::Decimal128 {
                data,
                scale: opt_scale,
                max_abs,
                ..
            } => {
                data.push(magnitude);
                let abs = magnitude.unsigned_abs();
                if abs > *max_abs {
                    *max_abs = abs;
                }
                if opt_scale.is_none() {
                    *opt_scale = Some(scale);
                }
            }
            other => {
                let negative = magnitude < 0;
                let s =
                    crate::storage::format_decimal_v3(magnitude.unsigned_abs(), scale, negative);
                other.push(&Value::Decimal(s));
            }
        }
    }
    #[inline]
    fn put_date(&mut self, col: usize, days: i32) {
        match self.accumulator(col) {
            ColumnAccumulator::Date32 { data, .. } => data.push(days),
            other => other.push(&Value::Date(days)),
        }
    }
    #[inline]
    fn put_year(&mut self, col: usize, year: u8) {
        match self.accumulator(col) {
            ColumnAccumulator::I64 { data, .. } => data.push(year as i64),
            other => other.push(&Value::Year(year)),
        }
    }
    #[inline]
    fn put_time(&mut self, col: usize, micros: i64) {
        match self.accumulator(col) {
            ColumnAccumulator::Time64 { data, .. } => data.push(micros),
            other => other.push(&Value::Time(micros)),
        }
    }
    #[inline]
    fn put_datetime(&mut self, col: usize, micros: i64) {
        match self.accumulator(col) {
            ColumnAccumulator::DateTime64 { data, .. } => data.push(micros),
            other => other.push(&Value::DateTime(micros)),
        }
    }
    #[inline]
    fn put_bytes(&mut self, col: usize, bytes: &[u8]) {
        match self.accumulator(col) {
            ColumnAccumulator::Bytes { offsets, data, .. } => {
                data.extend_from_slice(bytes);
                offsets.push(data.len() as u32);
            }
            ColumnAccumulator::Utf8 { offsets, data, .. } => {
                // CHAR(N) binary collations land here; the legacy sink
                // copies bytes through Value::Bytes which the Utf8 lane
                // doesn't accept either — keep the same fallback for
                // strict parity.
                data.extend_from_slice(bytes);
                offsets.push(data.len() as u32);
            }
            other => other.push(&Value::Bytes(bytes.to_vec())),
        }
    }
    #[inline]
    fn put_string(&mut self, col: usize, bytes: &[u8]) {
        match self.accumulator(col) {
            ColumnAccumulator::Utf8 { offsets, data, .. } => {
                data.extend_from_slice(bytes);
                offsets.push(data.len() as u32);
            }
            other => {
                // Match ValueVecSink: prefer the SmallString inline lane
                // when it fits so downstream Value-mode operators see
                // exactly what they would see today.
                if bytes.len() <= crate::types::SMALL_STRING_INLINE_CAP {
                    other.push(&Value::SmallString(
                        crate::types::SmallStringBuf::from_slice(bytes),
                    ));
                } else if let Ok(s) = std::str::from_utf8(bytes) {
                    other.push(&Value::String(s.to_owned()));
                } else {
                    other.push(&Value::Bytes(bytes.to_vec()));
                }
            }
        }
    }
    #[inline]
    fn put_json(&mut self, col: usize, bytes: &[u8]) {
        match self.accumulator(col) {
            ColumnAccumulator::Json { offsets, data, .. } => {
                data.extend_from_slice(bytes);
                offsets.push(data.len() as u32);
            }
            ColumnAccumulator::Utf8 { offsets, data, .. } => {
                data.extend_from_slice(bytes);
                offsets.push(data.len() as u32);
            }
            ColumnAccumulator::Bytes { offsets, data, .. } => {
                data.extend_from_slice(bytes);
                offsets.push(data.len() as u32);
            }
            other => other.push(&Value::Json(bytes.to_vec())),
        }
    }
    #[inline]
    fn put_geometry(&mut self, col: usize, bytes: &[u8]) {
        match self.accumulator(col) {
            ColumnAccumulator::Geometry { offsets, data, .. } => {
                data.extend_from_slice(bytes);
                offsets.push(data.len() as u32);
            }
            ColumnAccumulator::Bytes { offsets, data, .. } => {
                data.extend_from_slice(bytes);
                offsets.push(data.len() as u32);
            }
            other => other.push(&Value::Geometry(bytes.to_vec())),
        }
    }
    #[inline]
    fn put_vector(&mut self, col: usize, bytes: &[u8]) {
        self.put_value(col, Value::Vector(bytes.to_vec().into()));
    }
    #[inline]
    fn put_value(&mut self, col: usize, value: Value) {
        self.accumulator(col).push(&value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulators_past_nominal_capacity_keep_validity_aligned_with_data() {
        // Leaf-granular covering scans append whole index leaves, so a batch
        // can exceed the capacity its accumulator was sized for.
        let capacity = 128;
        let rows = 3 * capacity + 5;
        let null_at = |row: usize| row < 7 || row == 2 * capacity + 1;
        for column_type in [ColumnType::LongLong, ColumnType::VarString] {
            let mut lossless = ColumnAccumulator::for_type(ColumnType::LongLong, capacity);
            let mut generic = ColumnAccumulator::for_type(column_type, capacity);
            for row in 0..rows {
                let value = if null_at(row) {
                    Value::Null
                } else if column_type == ColumnType::LongLong {
                    Value::Int(row as i64)
                } else {
                    Value::String(row.to_string())
                };
                lossless.push_lossless_i64((!null_at(row)).then_some(row as i64));
                generic.push(&value);
            }
            for column in [lossless.finish(), generic.finish()] {
                assert_eq!(column.len(), rows);
                let validity = column.nulls().expect("nullable column");
                assert_eq!(validity.len(), rows);
                for row in 0..rows {
                    assert_eq!(column.is_null(row), null_at(row), "row {row}");
                }
                assert_eq!(validity.count_ones(), rows - 8);
            }
        }
    }

    #[test]
    fn partial_batch_null_mask_counts_only_materialized_rows_after_not() {
        let mut accumulator = ColumnAccumulator::for_type(ColumnType::LongLong, 130);
        for value in [Value::Int(7), Value::Null, Value::Int(9)] {
            accumulator.push(&value);
        }
        let column = accumulator.finish();
        let validity = column.nulls().expect("nullable column");
        assert_eq!(column.len(), 3);
        assert_eq!(validity.count_ones(), 2);
        let nulls = validity.not();
        assert_eq!(nulls.count_ones(), 1);
        assert_eq!(nulls.iter_true().collect::<Vec<_>>(), [1]);
        assert_eq!(Selection::Mask(nulls).count(), 1);
    }

    #[test]
    fn sqlproc_sus_wide_decimal_null_count_and_binary_utf8_fallback() {
        let value = crate::types::DecimalWide {
            magnitude: num_bigint::BigInt::from(1),
            scale: 40,
        };
        let mut nulls = Bitmap::new_all_valid(2);
        nulls.set(1, false);
        let column = ColumnVector::DecimalWide {
            data: vec![value.clone(), value],
            scale: 40,
            nulls: Some(nulls),
        };
        assert_eq!(column.count_non_null(), 1);
        let binary = ColumnVector::Utf8 {
            offsets: vec![0, 1],
            data: vec![0xff],
            nulls: None,
        };
        assert_eq!(binary.value_at(0), Value::Bytes(vec![0xff]));
        assert_eq!(binary.try_value_at(0).unwrap(), Value::Bytes(vec![0xff]));
    }

    #[test]
    fn utf8_materialization_keeps_binary_fallback_at_vector_boundaries() {
        for length in [0, 1, 22, 31, 32, 33, 63, 64, 65, 127, 128, 129] {
            for invalid in [false, true] {
                let mut bytes = "Я".repeat(length).into_bytes();
                if invalid {
                    bytes.push(0xff);
                }
                let expected = if invalid {
                    Value::Bytes(bytes.clone())
                } else {
                    Value::String(String::from_utf8(bytes.clone()).unwrap())
                };
                let column = ColumnVector::Utf8 {
                    offsets: vec![0, bytes.len() as u32],
                    data: bytes,
                    nulls: None,
                };
                assert_eq!(column.value_at(0), expected);
                assert_eq!(column.try_value_at(0).unwrap(), expected);
            }
        }
    }
    use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
    use crate::types::ColumnType;

    fn schema_of(types: &[ColumnType]) -> Arc<BatchSchema> {
        let fields: Vec<BatchField> = types
            .iter()
            .enumerate()
            .map(|(i, ty)| BatchField {
                table: String::new(),
                alias: None,
                name: format!("c{i}"),
                column_type: *ty,
                source: Default::default(),
            })
            .collect();
        Arc::new(BatchSchema::new(fields))
    }

    #[test]
    fn build_i64_column_from_values() {
        let col = ColumnVector::from_values(vec![Value::Int(1), Value::Null, Value::Int(3)]);
        assert_eq!(col.len(), 3);
        assert!(col.is_null(1));
        assert_eq!(col.value_at(0), Value::Int(1));
        assert_eq!(col.value_at(2), Value::Int(3));
    }

    #[test]
    fn build_utf8_column_from_values() {
        let col = ColumnVector::from_values(vec![
            Value::String("foo".into()),
            Value::Null,
            Value::String("baz".into()),
        ]);
        assert_eq!(col.len(), 3);
        assert!(col.is_null(1));
        let value = col.value_at(0);
        assert_eq!(crate::types::value_str(&value), Some("foo"));
    }

    #[test]
    fn mapped_accumulator_sink_routes_every_projection_slot() {
        use crate::storage::RowDecodeSink;

        let schema = schema_of(&[
            ColumnType::VarString,
            ColumnType::LongLong,
            ColumnType::LongLong,
        ]);
        let mut accumulators: Vec<_> = schema
            .fields()
            .iter()
            .map(|field| ColumnAccumulator::for_field(field, 2))
            .collect();
        {
            let mut sink = BatchAccumSink::mapped(&mut accumulators, &[2, 0]);
            sink.put_i64(0, 42);
            sink.put_string(1, b"mapped");
        }

        let columns: Vec<_> = accumulators
            .into_iter()
            .map(ColumnAccumulator::finish)
            .collect();
        assert_eq!(columns[0].value_at(0), Value::String("mapped".into()));
        assert_eq!(columns[1].len(), 0);
        assert_eq!(columns[2].value_at(0), Value::Int(42));
    }

    #[test]
    fn empty_batch_smoke() {
        let s = schema_of(&[ColumnType::LongLong]);
        let b = Batch::empty(s.clone());
        assert_eq!(b.num_rows(), 0);
        assert_eq!(b.visible_rows(), 0);
    }

    #[test]
    fn batch_with_selection() {
        let s = schema_of(&[ColumnType::LongLong]);
        let col = ColumnVector::from_values(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
        let b = Batch::new(s, vec![col]).with_selection(Selection::Indices(vec![0, 2]));
        assert_eq!(b.num_rows(), 3);
        assert_eq!(b.visible_rows(), 2);
        let sel: Vec<usize> = b.selection().unwrap().iter().collect();
        assert_eq!(sel, vec![0, 2]);
    }

    #[test]
    fn selection_only_clone_shares_columns_and_copy_on_write_preserves_values() {
        let schema = schema_of(&[ColumnType::LongLong]);
        let original = Batch::new(
            schema,
            vec![ColumnVector::from_values(vec![
                Value::Int(10),
                Value::Int(20),
            ])],
        );
        let mut selected = original.clone().with_selection(Selection::Indices(vec![1]));
        assert!(Arc::ptr_eq(&original.columns, &selected.columns));
        assert_eq!(selected.visible_rows(), 1);

        selected.try_columns_mut().unwrap()[0] =
            ColumnVector::from_values(vec![Value::Int(30), Value::Int(40)]);
        assert!(!Arc::ptr_eq(&original.columns, &selected.columns));
        assert_eq!(original.columns()[0].value_at(0), Value::Int(10));
        assert_eq!(selected.columns()[0].value_at(0), Value::Int(30));
    }

    #[test]
    fn range_selection_iterates_without_index_storage() {
        let schema = schema_of(&[ColumnType::LongLong]);
        let column = ColumnVector::from_values((0..8).map(Value::Int).collect());
        let batch =
            Batch::new(schema, vec![column]).with_selection(Selection::Range { start: 2, len: 3 });
        assert_eq!(batch.visible_rows(), 3);
        assert_eq!(batch.visible_indices().collect::<Vec<_>>(), vec![2, 3, 4]);
    }

    #[test]
    fn selection_composition_covers_range_indices_and_mask() {
        let range = Selection::Range { start: 2, len: 5 };
        let indices = Selection::Indices(vec![0, 2, 4, 6, 8]);
        assert_eq!(
            range
                .clone()
                .intersect(indices.clone(), 10)
                .iter()
                .collect::<Vec<_>>(),
            vec![2, 4, 6]
        );

        let mut mask = Bitmap::new_all_invalid(10);
        for index in [1, 2, 4, 7, 8] {
            mask.set_true(index);
        }
        assert_eq!(
            range
                .clone()
                .intersect(Selection::Mask(mask.clone()), 10)
                .iter()
                .collect::<Vec<_>>(),
            vec![2, 4]
        );
        assert_eq!(
            indices
                .intersect(Selection::Mask(mask), 10)
                .iter()
                .collect::<Vec<_>>(),
            vec![2, 4, 8]
        );
        assert_eq!(
            range
                .intersect(Selection::Range { start: 5, len: 4 }, 10)
                .iter()
                .collect::<Vec<_>>(),
            vec![5, 6]
        );
    }

    // -------------------------------------------------------------
    // New decimal lane regression tests.
    // -------------------------------------------------------------

    #[test]
    fn from_values_keeps_temporal_columns_in_native_lanes() {
        let cases = [
            vec![
                Value::Date(i32::MIN),
                Value::Date(-1),
                Value::Date(20000),
                Value::Null,
            ],
            vec![
                Value::Time(-1_000_001),
                Value::Time(0),
                Value::Time(1_000_001),
                Value::Null,
            ],
            vec![
                Value::DateTime(i64::MIN),
                Value::DateTime(-1),
                Value::DateTime(1_000_001),
                Value::Null,
            ],
        ];
        for values in cases {
            let column = ColumnVector::from_values(values.clone());
            assert!(matches!(
                column,
                ColumnVector::Date32 { .. }
                    | ColumnVector::Time64 { .. }
                    | ColumnVector::DateTime64 { .. }
            ));
            assert!(!column.is_null(0));
            assert!(column.is_null(3));
            assert_eq!(
                (0..values.len())
                    .map(|row| column.value_at(row))
                    .collect::<Vec<_>>(),
                values
            );
        }
    }

    #[test]
    fn from_values_picks_decimal32_lane_for_small_magnitudes() {
        // All magnitudes fit in i32 (±2.1·10⁹).
        let col = ColumnVector::from_values(vec![
            Value::DecimalCompact(1234, 2),
            Value::DecimalCompact(-9999, 2),
            Value::DecimalCompact(0, 2),
            Value::Null,
        ]);
        assert!(matches!(col, ColumnVector::Decimal32 { .. }));
        assert_eq!(col.decimal_scale(), Some(2));
        assert_eq!(col.value_at(0), Value::DecimalCompact(1234, 2));
        assert_eq!(col.value_at(1), Value::DecimalCompact(-9999, 2));
        assert_eq!(col.value_at(2), Value::DecimalCompact(0, 2));
        assert!(col.is_null(3));
    }

    #[test]
    fn from_values_picks_decimal64_when_magnitude_exceeds_i32() {
        // 10^10 > i32::MAX ≈ 2.1·10⁹.
        let col = ColumnVector::from_values(vec![
            Value::DecimalCompact(10_000_000_000, 2),
            Value::DecimalCompact(-99_999_999, 2),
        ]);
        assert!(matches!(col, ColumnVector::Decimal64 { .. }));
        let (data, scale) = col.as_decimal_i64_slice().unwrap();
        assert_eq!(scale, 2);
        assert_eq!(data, &[10_000_000_000_i64, -99_999_999]);
    }

    #[test]
    fn from_values_keeps_decimal128_and_wide_binary_lanes() {
        let decimal128 = Value::Decimal128(10_000_000_000_000_000_000i128, 2);
        let column = ColumnVector::from_values(vec![decimal128.clone()]);
        assert!(matches!(column, ColumnVector::Decimal128 { .. }));
        assert_eq!(column.value_at(0), decimal128);

        let wide = Value::from_exact_decimal(crate::util::decimal::ExactDecimal::from_big(
            num_bigint::BigInt::parse_bytes(
                b"99999999999999999999999999999999999999999999999999999999999999999",
                10,
            )
            .unwrap(),
            0,
        ));
        let column = ColumnVector::from_values(vec![wide.clone()]);
        assert!(matches!(column, ColumnVector::DecimalWide { .. }));
        assert_eq!(column.value_at(0), wide);
    }

    #[test]
    fn from_values_falls_back_to_generic_on_mixed_scale() {
        // Two different scales can't share a single lane.
        let col = ColumnVector::from_values(vec![
            Value::DecimalCompact(1234, 2),
            Value::DecimalCompact(5678, 4),
        ]);
        assert!(matches!(col, ColumnVector::Generic { .. }));
    }

    #[test]
    fn column_accumulator_narrows_to_decimal32() {
        // P ≤ 9 routes straight to the Decimal32 lane without going via
        // the i128 max-abs path.
        let mut acc = ColumnAccumulator::for_type(
            ColumnType::Decimal {
                precision: 9,
                scale: 2,
            },
            4,
        );
        acc.push(&Value::DecimalCompact(100, 2));
        acc.push(&Value::DecimalCompact(-200, 2));
        acc.push(&Value::Null);
        acc.push(&Value::DecimalCompact(900_000_000_i64, 2));
        let col = acc.finish();
        assert!(matches!(col, ColumnVector::Decimal32 { .. }));
        assert!(col.is_null(2));
    }

    #[test]
    fn wide_declared_decimal_recovers_observed_lanes_without_losing_scale_or_width() {
        for (values, native) in [
            (
                vec![
                    Value::DecimalCompact(123, 2),
                    Value::Null,
                    Value::DecimalCompact(-456, 2),
                ],
                true,
            ),
            (
                vec![
                    Value::Decimal128(i128::MAX / 100, 2),
                    Value::DecimalCompact(1, 2),
                ],
                true,
            ),
            (
                vec![Value::DecimalCompact(123, 2), Value::DecimalCompact(456, 3)],
                false,
            ),
            (
                vec![
                    Value::Decimal("123.00".into()),
                    Value::DecimalCompact(456, 2),
                ],
                false,
            ),
        ] {
            let mut accumulator = ColumnAccumulator::for_type(
                ColumnType::Decimal {
                    precision: 65,
                    scale: 2,
                },
                values.len(),
            );
            for value in &values {
                accumulator.push(value)
            }
            let column = accumulator.finish();
            assert_eq!(!matches!(column, ColumnVector::Generic { .. }), native);
            for (row, expected) in values.iter().enumerate() {
                assert_eq!(&column.value_at(row), expected)
            }
        }
    }

    #[test]
    fn column_accumulator_promotes_to_decimal64_when_exceeds_i32() {
        // P = 18 → Decimal64 lane directly (no max-abs scan).
        let mut acc = ColumnAccumulator::for_type(
            ColumnType::Decimal {
                precision: 18,
                scale: 2,
            },
            4,
        );
        acc.push(&Value::DecimalCompact(100, 2));
        acc.push(&Value::DecimalCompact(5_000_000_000_i64, 2));
        let col = acc.finish();
        assert!(matches!(col, ColumnVector::Decimal64 { .. }));
    }

    #[test]
    fn column_accumulator_falls_back_to_decimal128_for_large_precision() {
        // P > 18 needs the i128 lane.
        let mut acc = ColumnAccumulator::for_type(
            ColumnType::Decimal {
                precision: 30,
                scale: 2,
            },
            2,
        );
        acc.push(&Value::DecimalCompact(123, 2));
        acc.push(&Value::DecimalCompact(456, 2));
        let col = acc.finish();
        // Decimal128 finish window may downgrade for fits-i32 magnitudes;
        // we just require it's one of the decimal lanes with the right
        // scale and contents.
        assert_eq!(col.decimal_scale(), Some(2));
        assert_eq!(col.len(), 2);
    }

    #[test]
    fn sum_decimal_column_matches_naive_across_lanes() {
        use crate::engine::executor::vec::expr_batch::sum_decimal_column;
        // Same logical values, different lane widths — kernel must
        // return identical totals.
        let small_vals: Vec<Value> = (1..=100).map(|i| Value::DecimalCompact(i, 2)).collect();
        let cv32 = ColumnVector::from_values(small_vals.clone());
        let (sum_i32, scale_i32) = sum_decimal_column(&cv32).expect("decimal lane");
        assert!(matches!(cv32, ColumnVector::Decimal32 { .. }));
        assert_eq!(scale_i32, 2);
        let expect: i128 = (1..=100i128).sum();
        assert_eq!(sum_i32, expect);

        let mid_vals: Vec<Value> = (1..=100)
            .map(|i| Value::DecimalCompact(i * 10_000_000_000_i64, 2))
            .collect();
        let cv64 = ColumnVector::from_values(mid_vals);
        let (sum_i64, _) = sum_decimal_column(&cv64).expect("decimal lane");
        assert!(matches!(cv64, ColumnVector::Decimal64 { .. }));
        let expect64: i128 = (1..=100i128).map(|i| i * 10_000_000_000_i128).sum();
        assert_eq!(sum_i64, expect64);
    }

    #[test]
    fn unsigned_field_round_trips_full_u64_range() {
        let field = BatchField {
            table: "t".into(),
            alias: None,
            name: "u".into(),
            column_type: ColumnType::LongLong,
            source: crate::types::ColumnSource {
                flags: crate::mysql::protocol::UNSIGNED_FLAG,
                ..Default::default()
            },
        };
        let mut acc = ColumnAccumulator::for_field(&field, 3);
        let values = [
            Value::UInt(0),
            Value::UInt(i64::MAX as u64 + 1),
            Value::UInt(u64::MAX),
        ];
        for value in &values {
            acc.push(value);
        }
        let column = acc.finish();
        assert!(matches!(column, ColumnVector::U64 { .. }));
        for (row, expected) in values.iter().enumerate() {
            assert_eq!(&column.value_at(row), expected);
        }
    }

    #[test]
    fn logical_typed_lanes_preserve_value_variants() {
        let cases = vec![
            (Value::Timestamp(4_102_444_799_123_456), "timestamp"),
            (Value::Json(br#"{"a":1}"#.to_vec()), "json"),
            (Value::Geometry(vec![1, 2, 3]), "geometry"),
            (Value::Vector(vec![4, 5, 6, 0].into()), "vector"),
        ];
        for (value, label) in cases {
            let column = ColumnVector::from_values(vec![value.clone(), Value::Null]);
            assert_eq!(column.value_at(0), value, "{label}");
            assert_eq!(column.value_at(1), Value::Null, "{label}");
            assert!(!matches!(column, ColumnVector::Generic { .. }), "{label}");
        }
    }

    #[test]
    fn take_indices_null_free_fast_path_preserves_typed_lanes() {
        let integers = ColumnVector::from_values(vec![Value::Int(10), Value::Int(20)]);
        let gathered = integers.take_indices(&[1, 0, 1]);
        assert!(matches!(&gathered, ColumnVector::I64 { nulls: None, .. }));
        assert_eq!(gathered.value_at(0), Value::Int(20));

        let strings = ColumnVector::from_values(vec![
            Value::String("alpha".into()),
            Value::String("beta".into()),
        ]);
        let gathered = strings.take_indices(&[1, 0]);
        assert!(matches!(&gathered, ColumnVector::Utf8 { nulls: None, .. }));
        assert_eq!(gathered.value_at(0), Value::String("beta".into()));
        assert_eq!(gathered.value_at(1), Value::String("alpha".into()));
    }

    #[test]
    fn utf8_value_at_inlines_valid_short_strings_and_preserves_invalid_fallback() {
        let valid = ColumnVector::from_values(vec![Value::String("short".into())]);
        assert!(matches!(valid.value_at(0), Value::SmallString(_)));

        let invalid = ColumnVector::Utf8 {
            offsets: vec![0, 1],
            data: vec![0xff],
            nulls: None,
        };
        assert_eq!(invalid.value_at(0), Value::Bytes(vec![0xff]));
    }
}
