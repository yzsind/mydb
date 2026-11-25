//! Numeric/temporal sort keys read directly from fixed-width typed lanes.
//! NULL retains the canonical one-byte prefix: sparse keys need neither
//! padding nor larger work-area reservations than the scalar encoder.
use super::*;

struct Key<'a> {
    column: &'a ColumnVector,
    field: SortField,
    width: usize,
}

pub(super) struct Plan<'a> {
    keys: smallvec::SmallVec<[Key<'a>; 4]>,
    stride: usize,
}

impl<'a> Plan<'a> {
    pub(super) fn bind(
        columns: &[&'a ColumnVector],
        types: &[(ColumnType, u16)],
        fields: &[SortField],
    ) -> Option<Self> {
        let mut keys = smallvec::SmallVec::new();
        let mut stride = 0usize;
        for ((&column, &(kind, flags)), &field) in columns.iter().zip(types).zip(fields) {
            let integer = matches!(
                kind,
                ColumnType::Tiny
                    | ColumnType::Short
                    | ColumnType::MediumInt
                    | ColumnType::Long
                    | ColumnType::LongLong
                    | ColumnType::Year
                    | ColumnType::Bit { .. }
            );
            let unsigned = flags & crate::mysql::protocol::UNSIGNED_FLAG != 0;
            let width = match column {
                ColumnVector::I64 { .. } | ColumnVector::Bool { .. } if integer && !unsigned => 8,
                ColumnVector::U64 { .. } if integer && unsigned => 8,
                ColumnVector::F64 { .. }
                    if matches!(kind, ColumnType::Float | ColumnType::Double) =>
                {
                    8
                }
                ColumnVector::Date32 { .. } if kind == ColumnType::Date => 4,
                ColumnVector::Time64 { .. } if kind == ColumnType::Time => 8,
                ColumnVector::DateTime64 { .. } if kind == ColumnType::DateTime => 8,
                ColumnVector::Timestamp64 { .. } if kind == ColumnType::Timestamp => 8,
                ColumnVector::Decimal32 { scale, .. } | ColumnVector::Decimal64 { scale, .. } => {
                    match kind {
                        ColumnType::Decimal {
                            precision,
                            scale: target,
                        } if *scale == target && precision <= 38 => {
                            if precision <= 18 {
                                8
                            } else {
                                16
                            }
                        }
                        _ => return None,
                    }
                }
                ColumnVector::Decimal128 { scale, .. } => match kind {
                    ColumnType::Decimal {
                        precision,
                        scale: target,
                    } if *scale == target && precision <= 38 => {
                        if precision <= 18 {
                            8
                        } else {
                            16
                        }
                    }
                    _ => return None,
                },
                ColumnVector::Null { .. } => match kind {
                    _ if integer => 8,
                    ColumnType::Float
                    | ColumnType::Double
                    | ColumnType::Time
                    | ColumnType::DateTime
                    | ColumnType::Timestamp => 8,
                    ColumnType::Date => 4,
                    ColumnType::Decimal { precision, .. } if precision <= 38 => {
                        if precision <= 18 {
                            8
                        } else {
                            16
                        }
                    }
                    _ => return None,
                },
                _ => return None,
            };
            stride = stride.checked_add(width + 1)?;
            keys.push(Key {
                column,
                field,
                width,
            });
        }
        Some(Self { keys, stride })
    }

    pub(super) fn encoded_bytes(&self, batch: &Batch) -> usize {
        self.keys
            .iter()
            .fold(
                batch.visible_rows().saturating_mul(self.keys.len()),
                |bytes, key| {
                    let non_null = if batch.selection().is_none() {
                        key.column.count_non_null()
                    } else {
                        batch
                            .visible_indices()
                            .filter(|&row| !key.column.is_null(row))
                            .count()
                    };
                    bytes.saturating_add(non_null.saturating_mul(key.width))
                },
            )
            .min(batch.visible_rows().saturating_mul(self.stride))
    }

    pub(super) fn encode(&self, row: usize, out: &mut Vec<u8>) -> bool {
        for key in &self.keys {
            if key.column.is_null(row) {
                out.push(if key.field.nulls_first { 0 } else { 2 });
                continue;
            }
            out.push(1);
            let start = out.len();
            let signed = |value: i64| ((value as u64) ^ (1u64 << 63)).to_be_bytes();
            let decimal = |value: i128, out: &mut Vec<u8>| {
                if key.width == 8 {
                    let Ok(value) = i64::try_from(value) else {
                        return false;
                    };
                    out.extend_from_slice(&signed(value));
                } else {
                    out.extend_from_slice(&((value as u128) ^ (1u128 << 127)).to_be_bytes());
                }
                true
            };
            match key.column {
                ColumnVector::Bool { values, .. } => {
                    out.extend_from_slice(&signed(i64::from(values.get(row))))
                }
                ColumnVector::I64 { data, .. }
                | ColumnVector::Time64 { data, .. }
                | ColumnVector::Timestamp64 { data, .. } => {
                    out.extend_from_slice(&signed(data[row]))
                }
                ColumnVector::U64 { data, .. } => out.extend_from_slice(&data[row].to_be_bytes()),
                ColumnVector::F64 { data, .. } => {
                    let value = data[row];
                    if value.is_nan() {
                        return false;
                    }
                    let bits = crate::util::ordered_float::f64_to_order_u64(if value == 0.0 {
                        0.0
                    } else {
                        value
                    });
                    out.extend_from_slice(&bits.to_be_bytes());
                }
                ColumnVector::Date32 { data, .. } => {
                    if crate::types::decode_tagged_date(data[row]).is_some() {
                        return false;
                    }
                    out.extend_from_slice(&((data[row] as u32) ^ (1u32 << 31)).to_be_bytes());
                }
                ColumnVector::DateTime64 { data, .. } => {
                    if crate::types::decode_tagged_datetime(data[row]).is_some() {
                        return false;
                    }
                    out.extend_from_slice(&signed(data[row]));
                }
                ColumnVector::Decimal32 { data, .. } => {
                    if !decimal(i128::from(data[row]), out) {
                        return false;
                    }
                }
                ColumnVector::Decimal64 { data, .. } => {
                    if !decimal(i128::from(data[row]), out) {
                        return false;
                    }
                }
                ColumnVector::Decimal128 { data, .. } => {
                    if !decimal(data[row], out) {
                        return false;
                    }
                }
                _ => return false,
            }
            if !key.field.ascending {
                for byte in &mut out[start..] {
                    *byte = !*byte;
                }
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_page_keys_match_sql_comparison_for_nulls_extremes_and_directions() {
        let cases = vec![
            (
                ColumnType::LongLong,
                0,
                vec![
                    Value::Null,
                    Value::Int(i64::MIN),
                    Value::Int(0),
                    Value::Int(i64::MAX),
                    Value::Null,
                ],
            ),
            (
                ColumnType::LongLong,
                crate::mysql::protocol::UNSIGNED_FLAG,
                vec![
                    Value::Null,
                    Value::UInt(0),
                    Value::UInt(i64::MAX as u64),
                    Value::UInt(u64::MAX),
                    Value::Null,
                ],
            ),
            (
                ColumnType::Double,
                0,
                vec![
                    Value::Null,
                    Value::Double(f64::NEG_INFINITY),
                    Value::Double(-0.0),
                    Value::Double(0.0),
                    Value::Double(f64::INFINITY),
                ],
            ),
            (
                ColumnType::Decimal {
                    precision: 12,
                    scale: 2,
                },
                0,
                vec![
                    Value::Null,
                    Value::DecimalCompact(-100, 2),
                    Value::DecimalCompact(0, 2),
                    Value::DecimalCompact(100, 2),
                    Value::Null,
                ],
            ),
            (
                ColumnType::Decimal {
                    precision: 30,
                    scale: 2,
                },
                0,
                vec![
                    Value::Null,
                    Value::Decimal128(i128::MIN / 100, 2),
                    Value::Decimal128(0, 2),
                    Value::Decimal128(i128::MAX / 100, 2),
                    Value::Null,
                ],
            ),
            (
                ColumnType::Date,
                0,
                vec![
                    Value::Null,
                    Value::Date(-20),
                    Value::Date(0),
                    Value::Date(20),
                    Value::Null,
                ],
            ),
            (
                ColumnType::DateTime,
                0,
                vec![
                    Value::Null,
                    Value::DateTime(-20),
                    Value::DateTime(0),
                    Value::DateTime(20),
                    Value::Null,
                ],
            ),
            (
                ColumnType::Time,
                0,
                vec![
                    Value::Null,
                    Value::Time(-20),
                    Value::Time(0),
                    Value::Time(20),
                    Value::Null,
                ],
            ),
            (
                ColumnType::Timestamp,
                0,
                vec![
                    Value::Null,
                    Value::Timestamp(-20),
                    Value::Timestamp(0),
                    Value::Timestamp(20),
                    Value::Null,
                ],
            ),
        ];
        for (kind, flags, values) in cases {
            let first = ColumnVector::from_values(values.clone());
            let second = ColumnVector::from_values(
                (0..values.len()).map(|i| Value::Int(i as i64)).collect(),
            );
            for ascending in [true, false] {
                for nulls_first in [true, false] {
                    let fields = [
                        SortField {
                            ascending,
                            nulls_first,
                            collation_id: None,
                        },
                        SortField {
                            ascending: !ascending,
                            nulls_first: !nulls_first,
                            collation_id: None,
                        },
                    ];
                    let plan = Plan::bind(
                        &[&first, &second],
                        &[(kind, flags), (ColumnType::LongLong, 0)],
                        &fields,
                    )
                    .unwrap();
                    let mut keys = Vec::new();
                    for row in 0..values.len() {
                        let mut key = Vec::new();
                        assert!(plan.encode(row, &mut key));
                        assert!(key.len() <= plan.stride);
                        let normalized = canonical_decimal_key(values[row].clone(), kind).unwrap();
                        let mut oracle = RowEncoder::new(fields.to_vec().into()).unwrap();
                        let mut expected = Vec::new();
                        assert!(oracle.encode(
                            &[normalized, second.value_at(row)],
                            &[],
                            &mut expected
                        ));
                        assert_eq!(key, expected, "canonical {kind:?} row {row}");
                        keys.push(key);
                    }
                    for a in 0..values.len() {
                        for b in 0..values.len() {
                            let expected = crate::engine::executor::sort::compare_value_for_sort(
                                &values[a], &values[b], fields[0],
                            )
                            .then_with(|| {
                                crate::engine::executor::sort::compare_value_for_sort(
                                    &second.value_at(a),
                                    &second.value_at(b),
                                    fields[1],
                                )
                            });
                            assert_eq!(keys[a].cmp(&keys[b]), expected, "{kind:?} {a} {b}");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn unproven_fixed_domains_decline_before_publishing_a_page() {
        let field = [SortField {
            ascending: true,
            nulls_first: true,
            collation_id: None,
        }];
        let nan = ColumnVector::from_values(vec![Value::Double(f64::NAN)]);
        let plan = Plan::bind(&[&nan], &[(ColumnType::Double, 0)], &field).unwrap();
        assert!(!plan.encode(0, &mut Vec::new()));
        let decimal = ColumnVector::from_values(vec![Value::Decimal128(i128::MAX, 2)]);
        let plan = Plan::bind(
            &[&decimal],
            &[(
                ColumnType::Decimal {
                    precision: 12,
                    scale: 2,
                },
                0,
            )],
            &field,
        )
        .unwrap();
        assert!(!plan.encode(0, &mut Vec::new()));
        assert!(Plan::bind(
            &[&decimal],
            &[(
                ColumnType::Decimal {
                    precision: 30,
                    scale: 3
                },
                0
            )],
            &field
        )
        .is_none());
        let tagged = ColumnVector::from_values(vec![Value::Date(
            crate::types::encode_tagged_date(2000, 0, 1).unwrap(),
        )]);
        let plan = Plan::bind(&[&tagged], &[(ColumnType::Date, 0)], &field).unwrap();
        assert!(!plan.encode(0, &mut Vec::new()));
    }
}
