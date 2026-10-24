//! Order-preserving (memcmp-able) sort-key encoding.
//!
//! The generic Schwartzian path (`sort_by_cached_keys`) materialises a
//! `Vec<Value>` per row and pays a `Value`-enum dispatch (plus a
//! collation walk for strings) on every one of the ~N·log N
//! comparisons. This module instead encodes each row's ORDER BY key
//! once into a contiguous byte arena such that plain `&[u8]` ordering
//! equals `compare_sort_keys` ordering — the sort hot loop becomes a
//! memcmp the optimiser auto-vectorises, and the per-key `Value` heap
//! allocations disappear into one arena.
//!
//! Field layout (concatenation of per-field segments is comparable
//! because every segment is prefix-free):
//!
//! ```text
//! <null-prefix byte> [payload]
//!   null prefix: NULL → 0x00 when NULLS FIRST, 0x02 when NULLS LAST;
//!                non-NULL → 0x01. Never inverted for DESC — placement
//!                is owned by `SortField::nulls_first` alone.
//!   payload (absent for NULL), inverted bytewise for DESC:
//!     Int/Time/DateTime  sign-flipped u64  big-endian (8B)
//!     UInt               u64               big-endian (8B)
//!     Date               sign-flipped u32  big-endian (4B)
//!     Year               u8                              (1B)
//!     DecimalCompact     sign-flipped u64  big-endian (8B), uniform scale
//!     Float/Double       IEEE total-order bits, BE (4B/8B), NaN bails
//!     String/SmallString default-collation weight bytes, 0x00 escaped
//!                        as 0x00 0xFF, terminated by 0x00 0x00
//! ```
//!
//! Bytewise complement of a prefix-free segment reverses its order,
//! which is exactly DESC. The encoder is intentionally conservative:
//! any value shape whose `compare_values` semantics are not reproduced
//! bit-exactly by the table above (mixed lanes in one field, NaN,
//! cross-scale decimals, JSON/Geometry/Vector/Bytes/legacy Decimal)
//! returns `false` and the caller falls back to the cached-keys path —
//! that path remains the correctness oracle.

use std::cell::Cell;

use crate::types::Value;

use super::{apply_sorted_perm_in_place_raw, SortField};

/// Per-field value lane pinned by the first non-NULL value. Later rows
/// must stay in the same lane or the encoder bails — two lanes would
/// produce incomparable byte patterns.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Lane {
    Int,
    UInt,
    Float,
    Double,
    Str,
    Decimal(u8),
    Decimal128(u8),
    Date,
    Time,
    DateTime,
    Timestamp,
    Year,
}

thread_local! {
    static FORCE_MODE: Cell<Option<bool>> = const { Cell::new(None) };
}

/// Test/diagnostic override for the current thread; restores on drop of
/// the closure scope. Mirrors `vec::config::with_vec_mode`.
#[cfg(test)]
pub(crate) fn with_sort_key_encode<R>(enabled: bool, f: impl FnOnce() -> R) -> R {
    FORCE_MODE.with(|c| {
        let prev = c.replace(Some(enabled));
        let out = f();
        c.set(prev);
        out
    })
}

/// `MYDB_SORT_KEY_ENCODE` = `off`/`0`/`false` disables the encoded
/// path (cached-keys fallback everywhere); `on`/`auto` (default)
/// enables it. Read per sort call — once per query, not per row.
pub(crate) fn sort_key_encode_enabled() -> bool {
    if let Some(forced) = FORCE_MODE.with(|c| c.get()) {
        return forced;
    }
    match crate::util::env::var("MYDB_SORT_KEY_ENCODE") {
        Ok(v) => !matches!(v.to_ascii_lowercase().as_str(), "off" | "0" | "false"),
        Err(_) => true,
    }
}

struct Encoder<'a> {
    fields: &'a [SortField],
    lanes: Vec<Option<Lane>>,
    arena: Vec<u8>,
    /// Row key i occupies `arena[offsets[i]..offsets[i+1]]`.
    offsets: Vec<usize>,
    /// Scratch for collation weight bytes, reused across rows.
    weight: Vec<u8>,
}

impl<'a> Encoder<'a> {
    fn new(fields: &'a [SortField], rows: usize) -> Self {
        Self {
            fields,
            lanes: vec![None; fields.len()],
            arena: Vec::with_capacity(rows.saturating_mul(fields.len()).saturating_mul(9)),
            offsets: {
                let mut v = Vec::with_capacity(rows + 1);
                v.push(0);
                v
            },
            weight: Vec::new(),
        }
    }

    /// Encode one row's keys. `false` = unsupported shape; the caller
    /// must abandon the whole encoded sort (the arena is now garbage).
    fn push_row(&mut self, keys: &[Value]) -> bool {
        debug_assert_eq!(keys.len(), self.fields.len());
        for (idx, (value, field)) in keys.iter().zip(self.fields.iter()).enumerate() {
            if !self.push_field(idx, value, *field) {
                return false;
            }
        }
        self.offsets.push(self.arena.len());
        true
    }

    fn push_field(&mut self, field_idx: usize, value: &Value, field: SortField) -> bool {
        if matches!(value, Value::Null) {
            self.arena.push(if field.nulls_first { 0x00 } else { 0x02 });
            return true;
        }
        self.arena.push(0x01);
        let lane = match value_lane(value) {
            Some(lane) => lane,
            None => return false,
        };
        match &mut self.lanes[field_idx] {
            slot @ None => *slot = Some(lane),
            Some(existing) if *existing == lane => {}
            Some(_) => return false,
        }
        let payload_start = self.arena.len();
        match value {
            Value::Int(v) => self
                .arena
                .extend_from_slice(&((*v as u64) ^ (1u64 << 63)).to_be_bytes()),
            Value::UInt(v) => self.arena.extend_from_slice(&v.to_be_bytes()),
            Value::Time(v) | Value::DateTime(v) | Value::Timestamp(v) => self
                .arena
                .extend_from_slice(&((*v as u64) ^ (1u64 << 63)).to_be_bytes()),
            Value::Date(v) => self
                .arena
                .extend_from_slice(&((*v as u32) ^ (1u32 << 31)).to_be_bytes()),
            Value::Year(v) => self.arena.push(*v),
            Value::DecimalCompact(mag, _scale) => self
                .arena
                .extend_from_slice(&((*mag as u64) ^ (1u64 << 63)).to_be_bytes()),
            Value::Decimal128(magnitude, _) => self
                .arena
                .extend_from_slice(&((*magnitude as u128) ^ (1u128 << 127)).to_be_bytes()),
            Value::Double(v) => {
                if v.is_nan() {
                    return false;
                }
                self.arena
                    .extend_from_slice(&f64_order_bits(*v).to_be_bytes());
            }
            Value::Float(v) => {
                if v.is_nan() {
                    return false;
                }
                self.arena
                    .extend_from_slice(&f32_order_bits(*v).to_be_bytes());
            }
            Value::String(s) => self.push_str_payload(s.as_str(), field.collation_id),
            Value::SmallString(s) => {
                let s = std::str::from_utf8(s.as_slice()).unwrap_or("");
                self.push_str_payload(s, field.collation_id)
            }
            _ => return false,
        }
        if !field.ascending {
            for b in &mut self.arena[payload_start..] {
                *b = !*b;
            }
        }
        true
    }

    fn push_str_payload(&mut self, s: &str, collation: Option<u16>) {
        match collation {
            Some(id) => crate::charset::collation_key_by_id_into(id, s, &mut self.weight),
            None => crate::charset::default_string_key_into(s, &mut self.weight),
        }
        for &b in &self.weight {
            if b == 0x00 {
                self.arena.push(0x00);
                self.arena.push(0xFF);
            } else {
                self.arena.push(b);
            }
        }
        self.arena.push(0x00);
        self.arena.push(0x00);
    }

    #[inline]
    fn key(&self, row: u32) -> &[u8] {
        &self.arena[self.offsets[row as usize]..self.offsets[row as usize + 1]]
    }
}

fn value_lane(value: &Value) -> Option<Lane> {
    Some(match value {
        Value::Int(_) => Lane::Int,
        Value::UInt(_) => Lane::UInt,
        Value::Float(_) => Lane::Float,
        Value::Double(_) => Lane::Double,
        Value::String(_) | Value::SmallString(_) => Lane::Str,
        Value::DecimalCompact(_, scale) => Lane::Decimal(*scale),
        Value::Decimal128(_, scale) => Lane::Decimal128(*scale),
        Value::Date(value) if crate::types::decode_tagged_date(*value).is_some() => return None,
        Value::Date(_) => Lane::Date,
        Value::Time(_) => Lane::Time,
        Value::DateTime(value) if crate::types::decode_tagged_datetime(*value).is_some() => {
            return None
        }
        Value::DateTime(_) => Lane::DateTime,
        Value::Timestamp(_) => Lane::Timestamp,
        Value::Year(_) => Lane::Year,
        _ => return None,
    })
}

/// SQL sort keys equate signed zeros; storage's shared transform preserves
/// their bits. NaNs are rejected by `push_field` before reaching this helper.
#[inline]
fn f64_order_bits(v: f64) -> u64 {
    crate::util::ordered_float::f64_to_order_u64(if v == 0.0 { 0.0 } else { v })
}

#[inline]
fn f32_order_bits(v: f32) -> u32 {
    crate::util::ordered_float::f32_to_order_u32(if v == 0.0 { 0.0 } else { v })
}

/// Sort `items` by memcmp-encoded keys. Returns `false` (items
/// untouched) when any row's key shape is outside the encodable set —
/// the caller falls back to `sort_by_cached_keys`.
pub(crate) fn try_sort_by_encoded_keys<T>(
    items: &mut Vec<T>,
    fields: &[SortField],
    mut key_fn: impl FnMut(&T) -> Vec<Value>,
) -> bool {
    let n = items.len();
    if n < 2 || fields.is_empty() {
        return n < 2;
    }
    debug_assert!(n <= u32::MAX as usize);
    let mut enc = Encoder::new(fields, n);
    for item in items.iter() {
        let keys = key_fn(item);
        if !enc.push_row(&keys) {
            return false;
        }
    }
    let mut order: Vec<u32> = (0..n as u32).collect();
    order.sort_unstable_by(|&a, &b| enc.key(a).cmp(enc.key(b)).then_with(|| a.cmp(&b)));
    apply_sorted_perm_in_place_raw(items.as_mut_slice(), order.into_iter());
    true
}

/// Encode borrowed, already-evaluated keys under a query-owned reservation.
/// ASCII collation keys have a proven byte bound; other text stays on the
/// general comparator. No input row is moved until every key is validated.
pub(crate) fn try_sort_by_borrowed_encoded_keys<T>(
    items: &mut Vec<T>,
    fields: &[SortField],
    key_fn: impl Fn(&T) -> &[Value],
) -> Result<bool, crate::types::QueryError> {
    let count = items.len();
    if count < 2 || fields.is_empty() {
        return Ok(true);
    }
    if count > u32::MAX as usize || !sort_key_encode_enabled() {
        return Ok(false);
    }
    let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    if charge
        .add(
            fields
                .len()
                .saturating_mul(std::mem::size_of::<Option<Lane>>())
                .saturating_add(128),
        )
        .is_err()
    {
        return Ok(false);
    }
    let mut lanes = vec![None; fields.len()];
    let mut arena_bytes = 0usize;
    let mut weight_bytes = 0usize;
    for (row, item) in items.iter().enumerate() {
        if row % 4096 == 0 {
            crate::engine::cancel::check_cancelled()?;
        }
        let keys = key_fn(item);
        if keys.len() != fields.len() {
            return Ok(false);
        }
        for (column, (value, field)) in keys.iter().zip(fields).enumerate() {
            let bytes = if matches!(value, Value::Null) {
                1
            } else {
                let Some(lane) = value_lane(value) else {
                    return Ok(false);
                };
                if lanes[column].is_some_and(|prior| prior != lane) {
                    return Ok(false);
                }
                lanes[column] = Some(lane);
                match value {
                    Value::String(_) | Value::SmallString(_) => {
                        let text = match value {
                            Value::String(text) => text.as_bytes(),
                            Value::SmallString(text) => text.as_slice(),
                            _ => unreachable!(),
                        };
                        if field.collation_id.is_none() || !text.is_ascii() {
                            return Ok(false);
                        }
                        let text = std::str::from_utf8(text).expect("ASCII text validated above");
                        let (bytes, zeros) = crate::charset::ascii_collation_sort_key_size(
                            field.collation_id.unwrap(),
                            text,
                        );
                        // ASCII writers may stage the source span before
                        // removing primary-ignorable bytes.
                        weight_bytes = weight_bytes.max(bytes.max(text.len()));
                        bytes.saturating_add(zeros).saturating_add(3)
                    }
                    Value::Float(value) if value.is_nan() => return Ok(false),
                    Value::Double(value) if value.is_nan() => return Ok(false),
                    Value::Float(_) | Value::Date(_) => 5,
                    Value::Year(_) => 2,
                    Value::Decimal128(..) => 17,
                    _ => 9,
                }
            };
            let Some(total) = arena_bytes.checked_add(bytes) else {
                return Ok(false);
            };
            arena_bytes = total;
        }
    }
    let Some(work_bytes) = count
        .checked_add(1)
        .and_then(|rows| {
            rows.checked_mul(std::mem::size_of::<usize>() + 2 * std::mem::size_of::<u32>())
        })
        .and_then(|bytes| bytes.checked_add(arena_bytes))
        .and_then(|bytes| bytes.checked_add(weight_bytes))
    else {
        return Ok(false);
    };
    if charge.add(work_bytes).is_err() {
        return Ok(false);
    }
    let mut encoder = Encoder {
        fields,
        lanes,
        arena: crate::engine::executor::mem_budget::try_vec_with_capacity(
            arena_bytes,
            "encoded sort arena",
        )?,
        offsets: crate::engine::executor::mem_budget::try_vec_with_capacity(
            count + 1,
            "encoded sort offsets",
        )?,
        weight: crate::engine::executor::mem_budget::try_vec_with_capacity(
            weight_bytes,
            "encoded sort collation scratch",
        )?,
    };
    encoder.offsets.push(0);
    for (row, item) in items.iter().enumerate() {
        if row % 4096 == 0 {
            crate::engine::cancel::check_cancelled()?;
        }
        if !encoder.push_row(key_fn(item)) {
            return Ok(false);
        }
    }
    debug_assert!(encoder.arena.len() <= arena_bytes);
    let mut order = crate::engine::executor::mem_budget::try_vec_with_capacity(
        count,
        "encoded sort permutation",
    )?;
    order.extend(0..count as u32);
    order.sort_unstable_by(|&left, &right| {
        encoder
            .key(left)
            .cmp(encoder.key(right))
            .then_with(|| left.cmp(&right))
    });
    apply_sorted_perm_in_place_raw(items.as_mut_slice(), order.into_iter());
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::super::{compare_sort_keys, select_nth_by_precomputed_keys, sort_by_cached_keys};
    use super::*;

    #[test]
    fn encoded_float_keys_keep_signed_zero_ties_and_reject_nan() {
        for single_precision in [false, true] {
            for ascending in [false, true] {
                let fields = [SortField {
                    ascending,
                    nulls_first: true,
                    collation_id: None,
                }];
                let rows: Vec<_> = [0.0, -0.0, 1.0, -0.0, -1.0, 0.0]
                    .into_iter()
                    .enumerate()
                    .map(|(i, value)| {
                        (
                            i,
                            if single_precision {
                                Value::Float(value as f32)
                            } else {
                                Value::Double(value)
                            },
                        )
                    })
                    .collect();
                let mut expected = rows.clone();
                sort_by_cached_keys(&mut expected, &fields, |row| vec![row.1.clone()]);
                let mut actual = rows;
                assert!(try_sort_by_encoded_keys(&mut actual, &fields, |row| vec![
                    row.1.clone()
                ]));
                assert_eq!(
                    actual.iter().map(|row| row.0).collect::<Vec<_>>(),
                    expected.iter().map(|row| row.0).collect::<Vec<_>>()
                );
                let nan = if single_precision {
                    Value::Float(f32::from_bits(0xff80_0001))
                } else {
                    Value::Double(f64::from_bits(0xfff0_0000_0000_0001))
                };
                actual.push((6, nan));
                let ids: Vec<_> = actual.iter().map(|row| row.0).collect();
                assert!(!try_sort_by_encoded_keys(&mut actual, &fields, |row| vec![
                    row.1.clone()
                ]));
                assert_eq!(actual.iter().map(|row| row.0).collect::<Vec<_>>(), ids);
            }
        }
    }

    #[test]
    fn encoded_keys_honor_explicit_string_collation() {
        for collation in ["utf8mb4_bin", "utf8mb4_0900_ai_ci", "latin1_swedish_ci"] {
            let fields = [SortField {
                ascending: true,
                nulls_first: false,
                collation_id: Some(crate::charset::collation_id(collation)),
            }];
            assert_matches_oracle(
                ["a", "B", "é", "e", "A", "b", "a ", "a\0"]
                    .into_iter()
                    .map(|value| vec![Value::String(value.into())])
                    .collect(),
                &fields,
            );
        }
    }

    #[test]
    fn borrowed_encoding_is_budgeted_stable_and_keeps_fallback_input() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        use std::sync::Arc;
        let fields = [SortField {
            ascending: false,
            nulls_first: true,
            collation_id: Some(crate::charset::collation_id("utf8mb4_bin")),
        }];
        let rows = (0..1000)
            .map(|id| {
                (
                    id,
                    vec![if id % 11 == 0 {
                        Value::Null
                    } else {
                        Value::String(format!("key-{}", id % 37))
                    }],
                )
            })
            .collect::<Vec<_>>();
        let mut expected = rows.clone();
        sort_by_cached_keys(&mut expected, &fields, |row| row.1.clone());
        let budget = Arc::new(MemBudget::new(1024 * 1024));
        let mut actual = rows.clone();
        with_query_budget(budget.clone(), || {
            assert!(try_sort_by_borrowed_encoded_keys(&mut actual, &fields, |row| &row.1).unwrap());
        });
        assert_eq!(actual, expected);
        assert_eq!(budget.used(), 0);
        let small = Arc::new(MemBudget::new(1024));
        let mut unchanged = rows.clone();
        with_query_budget(small.clone(), || {
            assert!(
                !try_sort_by_borrowed_encoded_keys(&mut unchanged, &fields, |row| &row.1).unwrap()
            );
        });
        assert_eq!(unchanged, rows);
        assert_eq!(small.used(), 0);
    }

    fn lcg(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        *seed
    }

    /// Differential oracle: sorting via encoded keys must equal the
    /// cached-keys (compare_sort_keys) ordering bit-exactly, including
    /// the stability tiebreak.
    fn assert_matches_oracle(rows: Vec<Vec<Value>>, fields: &[SortField]) {
        let tagged: Vec<(usize, Vec<Value>)> = rows.into_iter().enumerate().collect();
        let mut encoded = tagged.clone();
        let ok = try_sort_by_encoded_keys(&mut encoded, fields, |(_, keys)| keys.clone());
        assert!(ok, "encoder unexpectedly bailed");
        let mut oracle = tagged;
        sort_by_cached_keys(&mut oracle, fields, |(_, keys)| keys.clone());
        let enc_tags: Vec<usize> = encoded.iter().map(|(t, _)| *t).collect();
        let ora_tags: Vec<usize> = oracle.iter().map(|(t, _)| *t).collect();
        assert_eq!(enc_tags, ora_tags);
    }

    fn field_grid() -> Vec<SortField> {
        vec![
            SortField {
                ascending: true,
                nulls_first: true,
                collation_id: None,
            },
            SortField {
                ascending: true,
                nulls_first: false,
                collation_id: None,
            },
            SortField {
                ascending: false,
                nulls_first: true,
                collation_id: None,
            },
            SortField {
                ascending: false,
                nulls_first: false,
                collation_id: None,
            },
        ]
    }

    #[test]
    fn int_with_nulls_matches_oracle_in_all_directions() {
        for field in field_grid() {
            let mut seed = 0xDEAD_0001u64;
            let rows: Vec<Vec<Value>> = (0..512)
                .map(|_| {
                    let r = lcg(&mut seed);
                    vec![if r % 5 == 0 {
                        Value::Null
                    } else {
                        Value::Int((r % 97) as i64 - 48)
                    }]
                })
                .collect();
            assert_matches_oracle(rows, &[field]);
        }
    }

    #[test]
    fn strings_match_oracle_including_collation_folds() {
        // Mixed case / accents / CJK / trailing spaces / embedded NUL /
        // combining marks — anything where weight-key order could
        // diverge from the live comparator.
        let pool = [
            "abc",
            "ABC",
            "abc ",
            " abc",
            "ab",
            "abcd",
            "Ä",
            "a\u{0308}",
            "ä",
            "中文",
            "中",
            "zzz",
            "",
            "a\0b",
            "A\0B",
            "ss",
            "ß",
            "œ",
            "oe",
        ];
        for field in field_grid() {
            let mut seed = 0xBEE5_0002u64;
            let rows: Vec<Vec<Value>> = (0..400)
                .map(|_| {
                    let r = lcg(&mut seed) as usize;
                    vec![if r % 7 == 0 {
                        Value::Null
                    } else {
                        Value::String(pool[r % pool.len()].to_string())
                    }]
                })
                .collect();
            assert_matches_oracle(rows, &[field]);
        }
    }

    #[test]
    fn small_string_and_string_share_a_lane() {
        let mut rows = Vec::new();
        for s in ["pear", "Apple", "fig", "apple", "PEAR"] {
            rows.push(vec![Value::String(s.to_string())]);
            rows.push(vec![Value::SmallString(
                crate::types::SmallStringBuf::from_slice(s.as_bytes()),
            )]);
        }
        assert_matches_oracle(
            rows,
            &[SortField {
                ascending: true,
                nulls_first: true,
                collation_id: None,
            }],
        );
    }

    #[test]
    fn doubles_dates_decimals_match_oracle() {
        for field in field_grid() {
            let mut seed = 0xCAFE_0003u64;
            let doubles: Vec<Vec<Value>> = (0..512)
                .map(|_| {
                    let r = lcg(&mut seed);
                    vec![match r % 11 {
                        0 => Value::Null,
                        1 => Value::Double(0.0),
                        2 => Value::Double(-0.0),
                        3 => Value::Double(f64::INFINITY),
                        4 => Value::Double(f64::NEG_INFINITY),
                        _ => Value::Double(((r % 2001) as f64 - 1000.0) / 8.0),
                    }]
                })
                .collect();
            assert_matches_oracle(doubles, &[field]);

            let dates: Vec<Vec<Value>> = (0..256)
                .map(|_| {
                    let r = lcg(&mut seed);
                    vec![match r % 9 {
                        0 => Value::Null,
                        1 => Value::Date(i32::MIN), // zero-date sentinel
                        _ => Value::Date((r % 40000) as i32 - 20000),
                    }]
                })
                .collect();
            assert_matches_oracle(dates, &[field]);

            let decimals: Vec<Vec<Value>> = (0..256)
                .map(|_| {
                    let r = lcg(&mut seed);
                    vec![if r % 6 == 0 {
                        Value::Null
                    } else {
                        Value::DecimalCompact((r % 100000) as i64 - 50000, 2)
                    }]
                })
                .collect();
            assert_matches_oracle(decimals, &[field]);
        }
    }

    #[test]
    fn multi_field_mixed_directions_match_oracle() {
        let fields = [
            SortField {
                ascending: true,
                nulls_first: false,
                collation_id: None,
            },
            SortField {
                ascending: false,
                nulls_first: true,
                collation_id: None,
            },
            SortField {
                ascending: true,
                nulls_first: true,
                collation_id: None,
            },
        ];
        let mut seed = 0xF00D_0004u64;
        let strings = ["red", "green", "blue", "RED", ""];
        let rows: Vec<Vec<Value>> = (0..1024)
            .map(|_| {
                let a = lcg(&mut seed);
                let b = lcg(&mut seed);
                let c = lcg(&mut seed);
                vec![
                    if a % 4 == 0 {
                        Value::Null
                    } else {
                        Value::Int((a % 13) as i64)
                    },
                    if b % 5 == 0 {
                        Value::Null
                    } else {
                        Value::String(strings[b as usize % strings.len()].to_string())
                    },
                    if c % 6 == 0 {
                        Value::Null
                    } else {
                        Value::Date((c % 1000) as i32)
                    },
                ]
            })
            .collect();
        assert_matches_oracle(rows, &fields);
    }

    #[test]
    fn bails_on_mixed_lanes_nan_and_unsupported() {
        let fields = [SortField {
            ascending: true,
            nulls_first: true,
            collation_id: None,
        }];
        // Mixed Int/UInt lane.
        let mut rows = vec![
            (0usize, vec![Value::Int(1)]),
            (1usize, vec![Value::UInt(2)]),
        ];
        assert!(!try_sort_by_encoded_keys(&mut rows, &fields, |(_, k)| k.clone()));
        // NaN.
        let mut rows = vec![
            (0usize, vec![Value::Double(1.0)]),
            (1usize, vec![Value::Double(f64::NAN)]),
        ];
        assert!(!try_sort_by_encoded_keys(&mut rows, &fields, |(_, k)| k.clone()));
        // Cross-scale decimals.
        let mut rows = vec![
            (0usize, vec![Value::DecimalCompact(100, 2)]),
            (1usize, vec![Value::DecimalCompact(100, 3)]),
        ];
        assert!(!try_sort_by_encoded_keys(&mut rows, &fields, |(_, k)| k.clone()));
        // Unsupported lane (Bytes).
        let mut rows = vec![(0usize, vec![Value::Bytes(vec![1, 2])])];
        rows.push((1usize, vec![Value::Bytes(vec![0, 1])]));
        assert!(!try_sort_by_encoded_keys(&mut rows, &fields, |(_, k)| k.clone()));
    }

    #[test]
    fn select_nth_partitions_like_full_sort() {
        let fields = [SortField {
            ascending: true,
            nulls_first: false,
            collation_id: None,
        }];
        let mut seed = 0xABCD_0005u64;
        let rows: Vec<(usize, Vec<Value>)> = (0..300)
            .map(|i| {
                let r = lcg(&mut seed);
                (
                    i,
                    vec![if r % 7 == 0 {
                        Value::Null
                    } else {
                        Value::Int((r % 50) as i64)
                    }],
                )
            })
            .collect();
        let nth = 40;
        let mut selected = rows.clone();
        let keys = selected.iter().map(|(_, keys)| keys.clone()).collect();
        select_nth_by_precomputed_keys(&mut selected, &fields, nth, keys);
        let mut sorted = rows;
        let ok = try_sort_by_encoded_keys(&mut sorted, &fields, |(_, k)| k.clone());
        assert!(ok);
        // The selected prefix must be exactly the sorted prefix as a set.
        let mut lhs: Vec<usize> = selected[..nth].iter().map(|(t, _)| *t).collect();
        let mut rhs: Vec<usize> = sorted[..nth].iter().map(|(t, _)| *t).collect();
        lhs.sort_unstable();
        rhs.sort_unstable();
        assert_eq!(lhs, rhs);
    }

    /// Micro-benchmark (release only): `cargo nextest run --release
    /// sort_key_encode_micro --run-ignored all --no-capture`. Times the
    /// encoded path against the cached-keys oracle on 1M short strings
    /// and 1M ints-with-NULLs.
    #[test]
    #[ignore = "manual micro-benchmark"]
    fn sort_key_encode_micro() {
        let mut seed = 0x5EED_BEEFu64;
        let n = 1_000_000usize;
        let strings: Vec<(u32, Vec<Value>)> = (0..n)
            .map(|i| {
                let r = lcg(&mut seed);
                (
                    i as u32,
                    vec![Value::String(format!("Customer#{:09}", r % 150_000))],
                )
            })
            .collect();
        let fields = [SortField {
            ascending: true,
            nulls_first: true,
            collation_id: None,
        }];
        for (label, encode) in [("cached-keys", false), ("encoded", true)] {
            let mut data = strings.clone();
            let start = std::time::Instant::now();
            if encode {
                assert!(try_sort_by_encoded_keys(&mut data, &fields, |(_, k)| k.clone()));
            } else {
                sort_by_cached_keys(&mut data, &fields, |(_, k)| k.clone());
            }
            eprintln!("string 1M {label}: {:?}", start.elapsed());
        }
        let ints: Vec<(u32, Vec<Value>)> = (0..n)
            .map(|i| {
                let r = lcg(&mut seed);
                (
                    i as u32,
                    vec![if r % 10 == 0 {
                        Value::Null
                    } else {
                        Value::Int((r % 1_000_000) as i64)
                    }],
                )
            })
            .collect();
        for (label, encode) in [("cached-keys", false), ("encoded", true)] {
            let mut data = ints.clone();
            let start = std::time::Instant::now();
            if encode {
                assert!(try_sort_by_encoded_keys(&mut data, &fields, |(_, k)| k.clone()));
            } else {
                sort_by_cached_keys(&mut data, &fields, |(_, k)| k.clone());
            }
            eprintln!("int+null 1M {label}: {:?}", start.elapsed());
        }
    }

    /// Sanity for the prefix-free string escape: "a" vs "ab" must order
    /// the same as the comparator in both directions (the classic
    /// complement-of-prefix bug this encoding exists to avoid).
    #[test]
    fn string_prefix_pairs_order_correctly_desc() {
        let rows = vec![
            vec![Value::String("a".into())],
            vec![Value::String("ab".into())],
            vec![Value::String("b".into())],
            vec![Value::String("a".into())],
        ];
        for field in field_grid() {
            assert_matches_oracle(rows.clone(), &[field]);
        }
    }

    #[test]
    fn oracle_vs_encoded_agree_on_compare_direction() {
        // Spot-check transitivity anchor: encoded order of two known
        // values equals compare_sort_keys.
        let fields = [SortField {
            ascending: false,
            nulls_first: false,
            collation_id: None,
        }];
        let a = vec![Value::Int(5)];
        let b = vec![Value::Int(9)];
        let cmp = compare_sort_keys(&fields, &a, &b);
        let mut items = vec![(0usize, a), (1usize, b)];
        assert!(try_sort_by_encoded_keys(&mut items, &fields, |(_, k)| k.clone()));
        match cmp {
            std::cmp::Ordering::Less => assert_eq!(items[0].0, 0),
            std::cmp::Ordering::Greater => assert_eq!(items[0].0, 1),
            std::cmp::Ordering::Equal => {}
        }
    }
}
