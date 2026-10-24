use super::*;
use aggregate::WindowAggregate;

fn evaluate(name: &str, values: &[Value], frame: WindowFrame, ordered: bool) -> Vec<Value> {
    let expr = Expr::WindowFunction {
        name: if name == "COUNT(*)" { "COUNT" } else { name }.into(),
        args: vec![if name == "COUNT(*)" {
            Expr::Wildcard
        } else {
            Expr::Identifier("v".into())
        }],
        spec: Box::new(crate::parser::WindowSpec {
            partition_by: Vec::new(),
            order_by: if ordered {
                vec![OrderBy {
                    expr: Expr::Identifier("id".into()),
                    asc: true,
                    nulls_first: None,
                }]
            } else {
                Vec::new()
            },
            frame: Some(frame.clone()),
            inherits: None,
        }),
    };
    let columns = [
        Column {
            name: "id".into(),
            column_type: ColumnType::LongLong,
            ..Column::default()
        },
        Column {
            name: "v".into(),
            column_type: ColumnType::Decimal {
                precision: 20,
                scale: 2,
            },
            ..Column::default()
        },
    ];
    let rows: Vec<_> = values
        .iter()
        .enumerate()
        .map(|(i, value)| vec![Value::Int(i as i64), value.clone()])
        .collect();
    evaluate_window_per_partition(
        &expr,
        &frame,
        &[(0..values.len()).collect()],
        &columns,
        &rows,
    )
    .unwrap()
}

fn frame(end: WindowFrameBound, exclusion: WindowFrameExclusion) -> WindowFrame {
    WindowFrame {
        // RANGE forces prefix evaluation through the scalar path.
        units: WindowFrameUnits::Range,
        start: WindowFrameBound::UnboundedPreceding,
        end,
        exclusion,
    }
}

fn assert_numbers(actual: &[Value], expected: &[Option<&str>]) {
    assert_eq!(actual.len(), expected.len());
    for (value, expected) in actual.iter().zip(expected) {
        if let Some(expected) = expected {
            assert!(
                compare_values(value, &Value::Decimal((*expected).into()))
                    .unwrap()
                    .is_eq(),
                "{value:?} != {expected}"
            );
        } else {
            assert_eq!(value, &Value::Null);
        }
    }
}

#[test]
fn window_aggregate_whole_prefix_and_excluded_frames_preserve_nulls_and_exact_values() {
    let values = [
        Value::Null,
        Value::DecimalCompact(125, 2),
        Value::DecimalCompact(250, 2),
        Value::Null,
    ];
    for (name, whole, prefix, excluded) in [
        (
            "COUNT",
            Some("2"),
            [Some("0"), Some("1"), Some("2"), Some("2")],
            [Some("2"), Some("1"), Some("1"), Some("2")],
        ),
        (
            "COUNT(*)",
            Some("4"),
            [Some("1"), Some("2"), Some("3"), Some("4")],
            [Some("3"); 4],
        ),
        (
            "SUM",
            Some("3.75"),
            [None, Some("1.25"), Some("3.75"), Some("3.75")],
            [Some("3.75"), Some("2.50"), Some("1.25"), Some("3.75")],
        ),
        (
            "AVG",
            Some("1.875"),
            [None, Some("1.25"), Some("1.875"), Some("1.875")],
            [Some("1.875"), Some("2.50"), Some("1.25"), Some("1.875")],
        ),
        (
            "MIN",
            Some("1.25"),
            [None, Some("1.25"), Some("1.25"), Some("1.25")],
            [Some("1.25"), Some("2.50"), Some("1.25"), Some("1.25")],
        ),
        (
            "MAX",
            Some("2.50"),
            [None, Some("1.25"), Some("2.50"), Some("2.50")],
            [Some("2.50"), Some("2.50"), Some("1.25"), Some("2.50")],
        ),
    ] {
        let whole_values = evaluate(
            name,
            &values,
            frame(
                WindowFrameBound::UnboundedFollowing,
                WindowFrameExclusion::NoOthers,
            ),
            false,
        );
        assert_numbers(&whole_values, &[whole; 4]);
        assert_numbers(
            &evaluate(
                name,
                &values,
                frame(WindowFrameBound::CurrentRow, WindowFrameExclusion::NoOthers),
                true,
            ),
            &prefix,
        );
        assert_numbers(
            &evaluate(
                name,
                &values,
                frame(
                    WindowFrameBound::UnboundedFollowing,
                    WindowFrameExclusion::CurrentRow,
                ),
                true,
            ),
            &excluded,
        );
        if matches!(name, "SUM" | "AVG") {
            assert!(
                whole_values.iter().all(|v| v.exact_decimal().is_some()),
                "exact arithmetic lost for {name}"
            );
        }
        let empty = WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowFrameBound::CurrentRow,
            end: WindowFrameBound::CurrentRow,
            exclusion: WindowFrameExclusion::CurrentRow,
        };
        assert_numbers(
            &evaluate(name, &values, empty, true),
            &[if name.starts_with("COUNT") {
                Some("0")
            } else {
                None
            }; 4],
        );
    }
}

#[test]
fn window_aggregate_extrema_move_owned_values_and_default_states_are_empty() {
    for (name, expected) in [("MIN", "alpha"), ("MAX", "omega")] {
        with_window_aggregate!(name, false, State, {
            let mut state = State::default();
            state.update(Value::Null).unwrap();
            for s in ["omega", "alpha", "middle"] {
                state.update(Value::String(s.into())).unwrap();
            }
            assert_eq!(state.value(), Value::String(expected.into()));
            assert_eq!(state.finish(), Value::String(expected.into()));
            assert_eq!(State::default().finish(), Value::Null);
        });
    }
}

#[test]
fn window_aggregate_preserves_wide_exact_and_approximate_result_types() {
    for (values, sum, average, approximate) in [
        (
            [Value::UInt(u64::MAX), Value::Int(1)],
            "18446744073709551616",
            "9223372036854775808",
            false,
        ),
        (
            [Value::Double(1.25), Value::Double(2.5)],
            "3.75",
            "1.875",
            true,
        ),
    ] {
        for (name, expected) in [("SUM", sum), ("AVG", average)] {
            for (end, ordered) in [
                (WindowFrameBound::UnboundedFollowing, false),
                (WindowFrameBound::CurrentRow, true),
            ] {
                let output = evaluate(
                    name,
                    &values,
                    frame(end, WindowFrameExclusion::NoOthers),
                    ordered,
                );
                assert_numbers(&output[1..], &[Some(expected)]);
                assert_eq!(matches!(output[1], Value::Double(_)), approximate);
                assert_eq!(output[1].exact_decimal().is_some(), !approximate);
            }
        }
    }
}

#[test]
fn window_frame_cardinality_matches_position_exclusion_at_all_boundaries() {
    for start in 0..5 {
        for end in 0..5 {
            for current in 0..5 {
                for peer_first in 0..5 {
                    for peer_last in 0..5 {
                        for exclusion in [
                            WindowFrameExclusion::NoOthers,
                            WindowFrameExclusion::CurrentRow,
                            WindowFrameExclusion::Group,
                            WindowFrameExclusion::Ties,
                        ] {
                            let range = WindowFrameRange {
                                start,
                                end,
                                current,
                                peer_first,
                                peer_last,
                                exclusion,
                            };
                            let expected = (start..=end)
                                .filter(|&pos| !frame_position_excluded(&range, pos))
                                .count();
                            assert_eq!(range.included_len(), expected,
                                "frame {start}..={end}, current {current}, peers {peer_first}..={peer_last}, {exclusion:?}");
                        }
                    }
                }
            }
        }
    }
}
