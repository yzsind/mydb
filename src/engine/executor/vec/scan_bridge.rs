//! Row-scan-to-Batch bridge.
//!
//! Storage's `scan_rows_columns_streaming_range` hands the engine one
//! `&[Value]` per heap row. The vectorized executor wants `Batch`es of
//! `ColumnVector`s. This module is the adapter: it accumulates incoming
//! rows into per-column `ColumnAccumulator`s, flushing a `Batch` every
//! `batch_size` rows (and once more for the trailing partial batch).
//!
//! It does **not** change the storage scan path — row decoding still
//! produces `Vec<Value>` per row. The win comes downstream:
//!
//! 1. `ColumnAccumulator::push(&Value)` extracts the raw payload straight
//!    into a typed buffer (i64 / i32 / Date32 / Decimal lane) without
//!    cloning the `Value` envelope.
//! 2. Downstream operators (`HashAggregateCore::consume_batch`) can then read
//!    column slices directly — bypassing per-row `Value` match dispatch
//!    inside accumulators (`accumulate_numeric_state` does that match
//!    once per row; `update_state_from_column` does it once per batch).
//! 3. Decimal sums in single-implicit-group / per-group SUM paths fold
//!    through `sum_decimal_column_with_nulls`, which the row-fused path
//!    can't reach.
//!
//! This is the first half of "list batched scan API"; a follow-up can
//! sink the accumulator down into `decode_row_schema_columns_into` so
//! the row codec writes straight into the typed buffer, eliminating the
//! intermediate `Vec<Value>` entirely.

use crate::types::Value;

/// Allocation-free scalar produced by the row codec for predicate columns.
/// Fixed-width SQL types stay in their native representation; uncommon
/// variable-width/fallback values retain a `Value` so the legacy predicate
/// evaluator remains available for every row shape.
#[derive(Clone, Debug)]
pub(crate) enum PredicateValue {
    Null,
    I64(i64),
    U64(u64),
    F64(f64),
    Decimal(i128, u8),
    Date(i32),
    Year(u8),
    #[allow(dead_code)] // Kept for typed TIME decoding once row codec emits it directly.
    Time(i64),
    #[allow(dead_code)] // Kept for typed DATETIME decoding once row codec emits it directly.
    DateTime(i64),
    Timestamp(i64),
    Value(Value),
}

impl PredicateValue {
    #[inline]
    pub(crate) fn to_value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::I64(value) => Value::Int(*value),
            Self::U64(value) => Value::UInt(*value),
            Self::F64(value) => Value::Double(*value),
            Self::Decimal(value, scale) => i64::try_from(*value)
                .map(|value| Value::DecimalCompact(value, *scale))
                .unwrap_or_else(|_| {
                    Value::Decimal(crate::storage::format_decimal_v3(
                        value.unsigned_abs(),
                        *scale,
                        *value < 0,
                    ))
                }),
            Self::Date(value) => Value::Date(*value),
            Self::Year(value) => Value::Year(*value),
            Self::Time(value) => Value::Time(*value),
            Self::DateTime(value) => Value::DateTime(*value),
            Self::Timestamp(value) => Value::Timestamp(*value),
            Self::Value(value) => value.clone(),
        }
    }

    /// Append a predicate scratch value to an output accumulator. Fixed-width
    /// variants construct only a stack `Value`; variable-width values are
    /// already owned by the scratch and are borrowed without another clone.
    #[inline]
    pub(crate) fn push_to(&self, accumulator: &mut super::batch::ColumnAccumulator) {
        use super::batch::ColumnAccumulator;

        macro_rules! push_fixed {
            ($data:expr, $value:expr) => {{
                $data.push($value);
                return;
            }};
        }
        match self {
            Self::Value(value) => accumulator.push(value),
            Self::I64(value) => match accumulator {
                ColumnAccumulator::I64 { data, .. } => push_fixed!(data, *value),
                _ => accumulator.push(&self.to_value()),
            },
            Self::U64(value) => match accumulator {
                ColumnAccumulator::U64 { data, .. } => push_fixed!(data, *value),
                _ => accumulator.push(&self.to_value()),
            },
            Self::F64(value) => match accumulator {
                ColumnAccumulator::F64 { data, .. } => push_fixed!(data, *value),
                _ => accumulator.push(&self.to_value()),
            },
            Self::Date(value) => match accumulator {
                ColumnAccumulator::Date32 { data, .. } => push_fixed!(data, *value),
                _ => accumulator.push(&self.to_value()),
            },
            Self::Time(value) => match accumulator {
                ColumnAccumulator::Time64 { data, .. } => push_fixed!(data, *value),
                _ => accumulator.push(&self.to_value()),
            },
            Self::DateTime(value) => match accumulator {
                ColumnAccumulator::DateTime64 { data, .. } => push_fixed!(data, *value),
                _ => accumulator.push(&self.to_value()),
            },
            Self::Timestamp(value) => match accumulator {
                ColumnAccumulator::Timestamp64 { data, .. } => push_fixed!(data, *value),
                _ => accumulator.push(&self.to_value()),
            },
            Self::Decimal(value, value_scale) => match accumulator {
                ColumnAccumulator::Decimal32 { data, scale, .. }
                    if scale == value_scale && i32::try_from(*value).is_ok() =>
                {
                    push_fixed!(data, *value as i32)
                }
                ColumnAccumulator::Decimal64 { data, scale, .. }
                    if scale == value_scale && i64::try_from(*value).is_ok() =>
                {
                    push_fixed!(data, *value as i64)
                }
                ColumnAccumulator::Decimal128 {
                    data,
                    scale,
                    max_abs,
                    ..
                } if scale.is_none_or(|scale| scale == *value_scale) => {
                    *scale = Some(*value_scale);
                    *max_abs = (*max_abs).max(value.unsigned_abs());
                    push_fixed!(data, *value)
                }
                _ => accumulator.push(&self.to_value()),
            },
            Self::Null | Self::Year(_) => accumulator.push(&self.to_value()),
        }
    }

    /// Canonical hash/equality key without first allocating a `Value` for the
    /// fixed-width lanes. Runtime Bloom probes execute this for every scanned
    /// candidate, so integer and temporal join keys must stay allocation-free.
    #[inline]
    pub(crate) fn to_value_key(&self) -> crate::engine::expr::ValueKey {
        use crate::engine::expr::ValueKey;
        match self {
            Self::Null => ValueKey::Null,
            Self::I64(value) => ValueKey::Int(*value as i128),
            Self::U64(value) => ValueKey::Int(*value as i128),
            Self::Date(value) => ValueKey::Int(*value as i128),
            Self::Year(value) => ValueKey::Int(if *value == 0 {
                0
            } else {
                1900 + *value as i128
            }),
            Self::Time(value) | Self::DateTime(value) | Self::Timestamp(value) => {
                ValueKey::Int(*value as i128)
            }
            // Reuse the canonical float/decimal/string encoders for the less
            // common lanes. The returned key owns any variable-width bytes.
            _ => ValueKey::from(&self.to_value()),
        }
    }
}

pub(crate) struct PredicateValueSink<'a> {
    values: &'a mut [PredicateValue],
}

impl<'a> PredicateValueSink<'a> {
    #[inline]
    pub(crate) fn new(values: &'a mut [PredicateValue]) -> Self {
        // A successful fast decode covers the complete projection and calls
        // one putter (including put_null) for every slot. If the layout does
        // not cover it, the caller ignores this scratch and takes the legacy
        // fallback. Clearing the whole slice for every input row therefore
        // only doubled the writes on full-table predicate scans.
        Self { values }
    }
}

impl crate::storage::RowDecodeSink for PredicateValueSink<'_> {
    #[inline]
    fn put_null(&mut self, col: usize) {
        self.values[col] = PredicateValue::Null;
    }
    #[inline]
    fn put_i64(&mut self, col: usize, value: i64) {
        self.values[col] = PredicateValue::I64(value);
    }
    #[inline]
    fn put_u64(&mut self, col: usize, value: u64) {
        self.values[col] = PredicateValue::U64(value);
    }
    #[inline]
    fn put_f32(&mut self, col: usize, value: f32) {
        self.values[col] = PredicateValue::F64(value as f64);
    }
    #[inline]
    fn put_f64(&mut self, col: usize, value: f64) {
        self.values[col] = PredicateValue::F64(value);
    }
    #[inline]
    fn put_decimal_compact(&mut self, col: usize, magnitude: i64, scale: u8) {
        self.values[col] = PredicateValue::Decimal(magnitude as i128, scale);
    }
    #[inline]
    fn put_decimal_wide(&mut self, col: usize, magnitude: i128, scale: u8) {
        self.values[col] = PredicateValue::Decimal(magnitude, scale);
    }
    #[inline]
    fn put_date(&mut self, col: usize, days: i32) {
        self.values[col] = PredicateValue::Date(days);
    }
    #[inline]
    fn put_year(&mut self, col: usize, year: u8) {
        self.values[col] = PredicateValue::Year(year);
    }
    #[inline]
    fn put_time(&mut self, col: usize, micros: i64) {
        self.values[col] = PredicateValue::Time(micros);
    }
    #[inline]
    fn put_datetime(&mut self, col: usize, micros: i64) {
        self.values[col] = PredicateValue::DateTime(micros);
    }
    #[inline]
    fn put_timestamp(&mut self, col: usize, micros: i64) {
        self.values[col] = PredicateValue::Timestamp(micros);
    }
    #[inline]
    fn put_bytes(&mut self, col: usize, bytes: &[u8]) {
        self.values[col] = PredicateValue::Value(Value::Bytes(bytes.to_vec()));
    }
    #[inline]
    fn put_string(&mut self, col: usize, bytes: &[u8]) {
        // RowDecodeSink's default contract validates VarString payloads
        // immediately before this call. Do not repeat the UTF-8 pass for
        // every rejected predicate row.
        // SAFETY: this sink does not opt into `validates_utf8_in_put_string`,
        // so `put_var_through_sink` has already accepted these bytes as UTF-8.
        let value = unsafe { std::str::from_utf8_unchecked(bytes) };
        self.values[col] =
            PredicateValue::Value(if bytes.len() <= crate::types::SMALL_STRING_INLINE_CAP {
                // Predicate-first scans decode this scratch for every input
                // row, including rows rejected immediately afterwards. Keep
                // short CHAR/VARCHAR values in the same inline lane as the
                // normal row decoder so a `CHAR(1) = const` filter performs
                // no allocator traffic. `SmallString` and `String` share the
                // exact same collation/comparison semantics.
                let _ = value;
                Value::SmallString(crate::types::SmallStringBuf::from_slice(bytes))
            } else {
                Value::String(value.to_owned())
            });
    }
    #[inline]
    fn put_json(&mut self, col: usize, bytes: &[u8]) {
        self.values[col] = PredicateValue::Value(Value::Json(bytes.to_vec()));
    }
    #[inline]
    fn put_geometry(&mut self, col: usize, bytes: &[u8]) {
        self.values[col] = PredicateValue::Value(Value::Geometry(bytes.to_vec()));
    }
    #[inline]
    fn put_vector(&mut self, col: usize, bytes: &[u8]) {
        self.values[col] = PredicateValue::Value(Value::Vector(bytes.to_vec()));
    }
    #[inline]
    fn put_value(&mut self, col: usize, value: Value) {
        self.values[col] = PredicateValue::Value(value);
    }
}
