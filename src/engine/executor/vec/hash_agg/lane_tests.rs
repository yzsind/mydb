//! Aggregate lane dispatch and BIT state boundary coverage.

use super::*;

#[test]
fn aggregate_lane_dispatch_matches_row_updates_for_all_native_families() {
    let kinds = [
        AggregateKind::CountStar,
        AggregateKind::Count,
        AggregateKind::Sum,
        AggregateKind::Avg,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::BitAnd,
        AggregateKind::BitOr,
        AggregateKind::BitXor,
        AggregateKind::VarPop,
        AggregateKind::VarSamp,
        AggregateKind::StddevPop,
        AggregateKind::StddevSamp,
        AggregateKind::CountDistinct,
        AggregateKind::SumDistinct,
        AggregateKind::AvgDistinct,
    ];
    let inputs = [
        vec![Value::Int(4), Value::Null, Value::Int(2), Value::Int(4)],
        vec![
            Value::UInt(u64::MAX),
            Value::Null,
            Value::UInt(2),
            Value::UInt(4),
        ],
        vec![
            Value::Double(4.0),
            Value::Null,
            Value::Double(2.0),
            Value::Double(4.0),
        ],
        vec![
            Value::DecimalCompact(400, 2),
            Value::Null,
            Value::DecimalCompact(200, 2),
            Value::DecimalCompact(400, 2),
        ],
        vec![
            Value::DecimalCompact(i64::MAX, 2),
            Value::Null,
            Value::DecimalCompact(200, 2),
            Value::DecimalCompact(400, 2),
        ],
        vec![
            Value::Decimal128(i128::MAX, 2),
            Value::Null,
            Value::Decimal128(200, 2),
            Value::Decimal128(400, 2),
        ],
        vec![
            Value::String("4".into()),
            Value::Null,
            Value::String("2".into()),
            Value::String("4".into()),
        ],
        vec![Value::Null; 4],
        vec![],
    ];
    for values in inputs {
        let all: Vec<_> = (0..values.len() as u32).collect();
        let repeated: Vec<_> = all.iter().rev().chain(&all).copied().collect();
        for col in [
            ColumnVector::from_values(values.clone()),
            ColumnVector::Generic { values },
        ] {
            for kind in kinds {
                let spec = AggregateSpec {
                    comparison_collation: None,
                    func: kind,
                    input_column: 0,
                };
                for selection in [
                    None,
                    Some(all.as_slice()),
                    Some(repeated.as_slice()),
                    Some(&[]),
                ] {
                    let rows = selection.unwrap_or(&all);
                    let mut expected = AggState::default();
                    let expected_result = rows.iter().try_for_each(|&row| {
                        update_state(&mut expected, &spec, &col, row as usize)
                    });
                    let mut actual = AggState::default();
                    let actual_result = update_state_from_lane(&mut actual, &spec, &col, selection);
                    assert_eq!(
                        actual_result.as_ref().err().map(|err| err.code()),
                        expected_result.as_ref().err().map(|err| err.code()),
                        "error for {kind:?}"
                    );
                    if actual_result.is_ok() {
                        let expected = expected.finalize(kind);
                        let actual = actual.finalize(kind);
                        match (&actual, &expected) {
                            (Value::Double(a), Value::Double(b))
                                if a.is_finite() && b.is_finite() =>
                            {
                                assert!(
                                    (a - b).abs()
                                        <= 8.0 * f64::EPSILON * a.abs().max(b.abs()).max(1.0),
                                    "{kind:?}: {actual:?} != {expected:?}"
                                );
                            }
                            _ => assert_eq!(actual, expected, "{kind:?}"),
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn bit_lanes_and_partial_merges_preserve_unsigned_bits_and_empty_inputs() {
    let bits = [u64::MAX, 0, 1 << 63, 3];
    for unsigned in [false, true] {
        let values: Vec<_> = bits
            .iter()
            .enumerate()
            .map(|(row, &bits)| match (row, unsigned) {
                (1, _) => Value::Null,
                (_, true) => Value::UInt(bits),
                (_, false) => Value::Int(bits as i64),
            })
            .collect();
        let columns = [
            ColumnVector::from_values(values.clone()),
            ColumnVector::Generic { values },
        ];
        for col in &columns {
            for kind in [
                AggregateKind::BitAnd,
                AggregateKind::BitOr,
                AggregateKind::BitXor,
            ] {
                let spec = AggregateSpec {
                    comparison_collation: None,
                    func: kind,
                    input_column: 0,
                };
                // Include empty, all-NULL, reordered and repeated selections.
                for rows in [vec![], vec![1], vec![3, 2, 1, 0, 3], vec![0, 1, 2, 3]] {
                    let identity = if kind == AggregateKind::BitAnd {
                        u64::MAX
                    } else {
                        0
                    };
                    let expected = rows.iter().filter(|&&row| row != 1).fold(
                        identity,
                        |acc, &row| match kind {
                            AggregateKind::BitAnd => acc & bits[row as usize],
                            AggregateKind::BitOr => acc | bits[row as usize],
                            AggregateKind::BitXor => acc ^ bits[row as usize],
                            _ => unreachable!(),
                        },
                    );
                    let mut selected = AggState::default();
                    update_state_from_lane(&mut selected, &spec, col, Some(&rows)).unwrap();
                    assert_eq!(selected.finalize(kind), Value::UInt(expected));

                    let mut merged = AggState::default().into_partial();
                    for chunk in rows.chunks(2) {
                        let mut partial = AggState::default();
                        update_state_from_lane(&mut partial, &spec, col, Some(chunk)).unwrap();
                        merged.merge_from(&partial.into_partial(), kind);
                    }
                    merged.merge_from(&AggState::default().into_partial(), kind);
                    assert_eq!(merged.finalize(kind), Value::UInt(expected));
                    if rows == [0, 1, 2, 3] {
                        let mut full = AggState::default();
                        update_state_from_lane(&mut full, &spec, col, None).unwrap();
                        assert_eq!(full.finalize(kind), Value::UInt(expected));
                    }
                }
            }
        }
    }
}
