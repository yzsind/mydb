//! Aggregate lane dispatch and BIT state boundary coverage.

use super::*;

#[test]
fn final_1107_lane_fallback_retains_prior_spills_nulls_and_unsigned_domain() {
    for dictionary in [false, true] {
        for spill_first in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let schema = Arc::new(BatchSchema::new(vec![
                BatchField {
                    table: "t".into(),
                    alias: None,
                    name: "k".into(),
                    column_type: ColumnType::LongLong,
                    source: Default::default(),
                },
                BatchField {
                    table: "t".into(),
                    alias: None,
                    name: "label".into(),
                    column_type: ColumnType::VarString,
                    source: Default::default(),
                },
            ]));
            let groups = if dictionary { vec![0, 1] } else { vec![0] };
            let specs = vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountStar,
                input_column: 0,
            }];
            let shared = Arc::new(SharedHashAggSpill::new(
                root.path().to_path_buf(),
                schema.clone(),
                &groups,
                specs.clone(),
            ));
            let mut actual = HashAggregateCore::with_budget(
                groups.clone(),
                specs.clone(),
                schema.clone(),
                Arc::new(MemBudget::new(128 * 1024 * 1024)),
                root.path(),
            );
            if dictionary {
                actual = actual.with_shared_spill(shared.clone());
            }
            let mut oracle = HashAggregateCore::new(groups, specs.clone());
            oracle.fixed_width_probed = true;
            oracle.packed64_probed = true;
            oracle.dense_i64_probed = true;
            oracle.dictionary_probed = true;
            for step in 0..3 {
                let keys = if step == 0 {
                    ColumnVector::from_values(vec![
                        Value::Int(-1),
                        Value::Int(1),
                        Value::Int(1),
                        Value::Null,
                    ])
                } else {
                    ColumnVector::Generic {
                        values: vec![
                            Value::UInt(u64::MAX),
                            Value::UInt(1),
                            Value::UInt(2),
                            Value::Null,
                        ],
                    }
                };
                let batch = Batch::new(
                    schema.clone(),
                    vec![
                        keys,
                        ColumnVector::from_values(vec![Value::String("label".into()); 4]),
                    ],
                );
                actual.consume_batch(&batch).unwrap();
                oracle.consume_batch(&batch).unwrap();
                if step == 0 && spill_first {
                    if dictionary {
                        assert!(actual.dictionary_count.is_some());
                        actual.do_spill_dictionary_count().unwrap();
                    } else {
                        assert!(actual.fixed_width.is_some());
                        actual.do_spill_fixed_width().unwrap();
                    }
                }
            }
            let result = if dictionary {
                merge_worker_partition_snapshots(
                    vec![
                        (0..NUM_PARTITIONS)
                            .map(|p| actual.take_partition_merged_snapshot(p).unwrap())
                            .collect(),
                        shared.partition_snapshots().unwrap(),
                    ],
                    &specs,
                )
                .unwrap()
            } else {
                actual.finalize().unwrap()
            };
            let canonical = |rows: Vec<Vec<Value>>| {
                let mut rows: Vec<Vec<String>> = rows
                    .iter()
                    .map(|r| r.iter().map(Value::to_display_string).collect())
                    .collect();
                rows.sort();
                rows
            };
            assert_eq!(
                canonical(result),
                canonical(oracle.finalize().unwrap()),
                "dictionary={dictionary} spill_first={spill_first}"
            );
        }
    }
}

#[test]
fn final_1107_dictionary_group_lane_drift_preserves_prefix() {
    let root = tempfile::tempdir().unwrap();
    let schema = Arc::new(BatchSchema::new(vec![
        BatchField {
            table: "t".into(),
            alias: None,
            name: "label".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        },
        BatchField {
            table: "t".into(),
            alias: None,
            name: "k".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        },
    ]));
    let specs = vec![AggregateSpec {
        comparison_collation: None,
        func: AggregateKind::CountStar,
        input_column: 0,
    }];
    let shared = Arc::new(SharedHashAggSpill::new(
        root.path().to_path_buf(),
        schema.clone(),
        &[0, 1],
        specs.clone(),
    ));
    let mut worker = HashAggregateCore::with_budget(
        vec![0, 1],
        specs.clone(),
        schema.clone(),
        Arc::new(MemBudget::new(128 * 1024 * 1024)),
        root.path(),
    )
    .with_shared_spill(shared.clone());
    let mut oracle = HashAggregateCore::new(vec![0, 1], specs.clone());
    for generic in [false, true] {
        let values = vec![Value::Int(1); 128];
        let batch = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::from_values(vec![Value::String("label".into()); 128]),
                if generic {
                    ColumnVector::Generic { values }
                } else {
                    ColumnVector::from_values(values)
                },
            ],
        );
        oracle.consume_batch(&batch).unwrap();
        worker.consume_batch(&batch).unwrap();
        if !generic {
            assert!(worker.dictionary_count.is_some());
        }
    }
    let actual = merge_worker_partition_snapshots(
        vec![
            (0..NUM_PARTITIONS)
                .map(|p| worker.take_partition_merged_snapshot(p).unwrap())
                .collect(),
            shared.partition_snapshots().unwrap(),
        ],
        &specs,
    )
    .unwrap();
    assert_eq!(actual, oracle.finalize().unwrap());
}

#[test]
fn final_1107_fixed_width_group_lane_drift_preserves_prefix() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let schema = Arc::new(BatchSchema::new(vec![
        BatchField {
            table: "t".into(),
            alias: None,
            name: "k".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        },
        BatchField {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::Decimal {
                precision: 20,
                scale: 2,
            },
            source: Default::default(),
        },
    ]));
    for generic in [false, true] {
        for func in [AggregateKind::CountStar, AggregateKind::Sum] {
            // Lane drift spills the completed prefix even without memory pressure.
            // Keep that I/O inside this fixture, independent of MYDB_DATA_DIR.
            let mut core = HashAggregateCore::with_budget(
                vec![0],
                vec![AggregateSpec {
                    comparison_collation: None,
                    func,
                    input_column: 1,
                }],
                schema.clone(),
                Arc::new(MemBudget::new(128 * 1024 * 1024)),
                root.path(),
            );
            let first = Batch::new(
                schema.clone(),
                vec![
                    ColumnVector::from_values(vec![Value::Int(1), Value::Int(1)]),
                    ColumnVector::from_values(vec![Value::DecimalCompact(100, 2); 2]),
                ],
            );
            core.consume_batch(&first).unwrap();
            assert!(core.fixed_width.is_some());
            let keys = if generic {
                ColumnVector::Generic {
                    values: vec![Value::Int(1), Value::Int(2)],
                }
            } else {
                ColumnVector::from_values(vec![Value::UInt(1), Value::UInt(2)])
            };
            let second = Batch::new(
                schema.clone(),
                vec![
                    keys,
                    ColumnVector::from_values(vec![Value::DecimalCompact(100, 2); 2]),
                ],
            );
            core.consume_batch(&second).unwrap();
            let spill_path = core.spill_dir.as_ref().map(|dir| dir.path().to_path_buf());
            if generic {
                let path = spill_path
                    .as_ref()
                    .expect("lane drift must spill its prefix");
                assert!(path.starts_with(root.path()));
                assert!(core.metrics().did_spill);
            }
            let mut rows = core.finalize().unwrap();
            if let Some(path) = spill_path {
                assert!(
                    !path.exists(),
                    "finalization must remove the private spill directory"
                );
            }
            rows.sort_by_key(|r| r[0].to_display_string());
            let rows: Vec<Vec<String>> = rows
                .iter()
                .map(|r| r.iter().map(Value::to_display_string).collect())
                .collect();
            let expected = if func == AggregateKind::CountStar {
                [vec!["1", "3"], vec!["2", "1"]]
            } else {
                [vec!["1", "3.00"], vec!["2", "1.00"]]
            };
            assert_eq!(rows, expected, "generic={generic} func={func:?}");
        }
    }
}

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
