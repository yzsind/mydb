//! Cross-check the two partial-state consumers independently of SQL routing.
use super::*;

#[test]
fn partial_batches_and_spill_replay_cover_all_aggregate_kinds() {
    let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let collation = crate::charset::resolve_collation("utf8mb4_general_ci")
        .unwrap()
        .id;
    let input_schema = BatchSchema::new(vec![
        BatchField {
            table: "t".into(),
            alias: None,
            name: "k".into(),
            column_type: ColumnType::VarString,
            source: crate::types::ColumnSource {
                collation_id: Some(collation),
                ..Default::default()
            },
        },
        BatchField {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::Decimal {
                precision: 18,
                scale: 2,
            },
            source: Default::default(),
        },
    ]);
    let aggs: Vec<_> = [
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
    ]
    .into_iter()
    .map(|func| AggregateSpec {
        comparison_collation: None,
        func,
        input_column: 1,
    })
    .collect();
    let schema = Arc::new(build_spill_schema(&input_schema, &[0], &aggs));
    let mut batches = Vec::new();
    for name in ["Alpha", "ALPHA", "alpha"] {
        let mut part = Partition::default();
        for key in [Value::String(name.into()), Value::Null] {
            let mut states = vec![AggState::default(); aggs.len()];
            let values = if matches!(key, Value::Null) {
                vec![Value::Null]
            } else {
                vec![
                    Value::Null,
                    Value::DecimalCompact(125, 2),
                    Value::DecimalCompact(250, 2),
                    Value::DecimalCompact(125, 2),
                ]
            };
            for (state, spec) in states.iter_mut().zip(&aggs) {
                for value in &values {
                    if spec.func == AggregateKind::CountStar {
                        state.update_count_star_repeated(1);
                    } else {
                        state.update_ref(spec.func, value).unwrap();
                    }
                }
            }
            let mut encoded = Vec::new();
            super::super::key_encode::encode_with_collation(&key, Some(collation), &mut encoded);
            part.buckets
                .push(GroupBucket::new(encoded, vec![key], states));
        }
        batches.push(partition_to_spill_batch(&part, &schema, &[0], &aggs));
    }
    let resident = merge_partition_batches(batches.clone(), schema.clone(), &aggs).unwrap();
    // Independent oracle: feed the original inputs directly, bypassing both
    // partial serialization and partial replay. A shared codec bug must not
    // pass merely because resident and disk consumers agree.
    for bucket in &resident {
        let mut expected = vec![AggState::default(); aggs.len()];
        for _ in 0..3 {
            let values = if matches!(bucket.key_values[0], Value::Null) {
                vec![Value::Null]
            } else {
                vec![
                    Value::Null,
                    Value::DecimalCompact(125, 2),
                    Value::DecimalCompact(250, 2),
                    Value::DecimalCompact(125, 2),
                ]
            };
            for (state, spec) in expected.iter_mut().zip(&aggs) {
                for value in &values {
                    if spec.func == AggregateKind::CountStar {
                        state.update_count_star_repeated(1);
                    } else {
                        state.update_ref(spec.func, value).unwrap();
                    }
                }
            }
        }
        for ((actual, expected), spec) in bucket.aggs.iter().zip(&expected).zip(&aggs) {
            let actual = actual.finalize(spec.func);
            let expected = expected.finalize(spec.func);
            match (&actual, &expected) {
                (Value::Double(a), Value::Double(b)) => assert!(
                    (a - b).abs() <= 1e-12 * b.abs().max(1.0),
                    "{:?}: {actual:?} != {expected:?}",
                    spec.func
                ),
                _ => assert_eq!(actual, expected, "{:?}", spec.func),
            }
        }
    }
    let tmp = SpillTmpDir::create_kind(root.path(), "partial-merge-test").unwrap();
    let spill_dir = tmp.path().to_path_buf();
    let path = tmp.path().join("partials");
    let mut writer = crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
        &path,
        tmp.charger(),
        4096,
    )
    .unwrap();
    for batch in &batches {
        spill::write_batch(&mut writer, batch).unwrap();
    }
    writer.finish().unwrap();
    let replayed = merge_partition(
        Partition::default(),
        &[path.clone()],
        schema.clone(),
        &aggs,
        tmp.charger(),
    )
    .unwrap();
    let output = |buckets: Vec<GroupBucket>| {
        let mut rows: Vec<_> = buckets
            .into_iter()
            .map(|bucket| {
                let mut values = vec![bucket.key_values[0]
                    .to_display_string()
                    .to_ascii_lowercase()];
                values.extend(
                    bucket
                        .aggs
                        .iter()
                        .zip(&aggs)
                        .map(|(state, spec)| state.finalize(spec.func).to_display_string()),
                );
                values
            })
            .collect();
        rows.sort();
        rows
    };
    let expected = output(resident);
    assert_eq!(
        expected.len(),
        2,
        "collation-equivalent keys merge across partials"
    );
    let alpha = expected.iter().find(|row| row[0] == "alpha").unwrap();
    assert_eq!(alpha[1], "12");
    assert_eq!(alpha[2], "9");
    assert_eq!(alpha[14], "2", "COUNT DISTINCT unions overlapping partials");
    assert_eq!(output(replayed), expected);
    // An existing resident bucket must merge with disk partials as well.
    let first = merge_partition_batches(vec![batches[0].clone()], schema.clone(), &aggs).unwrap();
    let part = Partition {
        buckets: first,
        ..Default::default()
    };
    let mixed =
        merge_partition(part, &[path.clone()], schema.clone(), &aggs, tmp.charger()).unwrap();
    let mut four = batches.clone();
    four.insert(0, batches[0].clone());
    assert_eq!(
        output(mixed),
        output(merge_partition_batches(four, schema.clone(), &aggs).unwrap())
    );
    // Corrupt input remains a fallible I/O/decode error, never an empty group.
    std::fs::write(&path, [0xff]).unwrap();
    assert!(merge_partition(Partition::default(), &[path], schema, &aggs, tmp.charger()).is_err());
    drop(tmp);
    assert!(!spill_dir.exists());
}

/// In-memory version of `merge_partition` for use by the parallel
/// merge path. The caller has already gathered the worker partials for
/// a single partition as `Batch`es (no spill file round-trip needed).
fn merge_partition_batches(
    batches: Vec<Batch>,
    schema: Arc<BatchSchema>,
    aggs: &[AggregateSpec],
) -> Result<Vec<GroupBucket>, QueryError> {
    let layout = SpillLayout::for_schema(&schema, aggs)?;
    let mut by_key: GroupMap<usize> = GroupMap::default();
    let mut buckets: Vec<GroupBucket> = Vec::new();
    let mut key_buf = Vec::with_capacity(64);
    for batch in batches {
        merge_partial_batch_into(
            &batch,
            &schema,
            aggs,
            &layout,
            &mut by_key,
            &mut buckets,
            &mut key_buf,
        )?;
    }
    Ok(buckets)
}

#[test]
fn mixed_spill_slots_and_fixed_replay_match_original_inputs() {
    let input = BatchSchema::new(vec![
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
            column_type: ColumnType::LongLong,
            source: Default::default(),
        },
    ]);
    // Unequal partial sizes and means exercise Welford merge, while slots of
    // widths 1/2/3 in both orders expose offset drift in adjacent aggregates.
    let chunks = [
        vec![Value::Int(-10), Value::Null],
        vec![Value::Int(4), Value::Int(4), Value::Int(20)],
        vec![Value::Int(7)],
    ];
    let kinds = [
        AggregateKind::Avg,
        AggregateKind::VarSamp,
        AggregateKind::Count,
        AggregateKind::SumDistinct,
        AggregateKind::Max,
        AggregateKind::StddevPop,
        AggregateKind::Min,
        AggregateKind::Sum,
        AggregateKind::CountStar,
    ];
    for fixed in [false, true] {
        for reverse in [false, true] {
            let mut aggs = kinds
                .iter()
                .copied()
                .filter(|kind| {
                    !fixed
                        || matches!(
                            kind,
                            AggregateKind::CountStar
                                | AggregateKind::Count
                                | AggregateKind::Sum
                                | AggregateKind::Avg
                                | AggregateKind::Min
                                | AggregateKind::Max
                        )
                })
                .map(|func| AggregateSpec {
                    comparison_collation: None,
                    func,
                    input_column: 1,
                })
                .collect::<Vec<_>>();
            if reverse {
                aggs.reverse();
            }
            let schema = Arc::new(build_spill_schema(&input, &[0], &aggs));
            let layout = SpillLayout::new(1, &aggs);
            let mut expected = vec![AggState::default(); aggs.len()];
            let batches = chunks
                .iter()
                .map(|values| {
                    let mut partial = vec![AggState::default(); aggs.len()];
                    for ((state, oracle), spec) in partial.iter_mut().zip(&mut expected).zip(&aggs)
                    {
                        for value in values {
                            if spec.func == AggregateKind::CountStar {
                                state.update_count_star_repeated(1);
                                oracle.update_count_star_repeated(1);
                            } else {
                                state.update_ref(spec.func, value).unwrap();
                                oracle.update_ref(spec.func, value).unwrap();
                            }
                        }
                    }
                    let part = Partition {
                        buckets: vec![GroupBucket::new(vec![], vec![Value::Int(1)], partial)],
                        ..Default::default()
                    };
                    partition_to_spill_batch(&part, &schema, &[0], &aggs)
                })
                .collect::<Vec<_>>();
            let actual = if fixed {
                let mut arena =
                    FixedWidthStateArena::try_new_for_partial(&batches[0], 1, &aggs).unwrap();
                for batch in &batches {
                    arena.merge_partial_batch(batch, 1, &layout, &aggs).unwrap();
                }
                // Also cover the typed writer, not just the generic writer.
                let batch = fixed_width_partition_to_spill_batch(&arena, &[0], &schema, &aggs);
                merge_partition_batches(vec![batch], schema, &aggs)
                    .unwrap()
                    .remove(0)
                    .aggs
            } else {
                merge_partition_batches(batches, schema, &aggs)
                    .unwrap()
                    .remove(0)
                    .aggs
            };
            for ((actual, expected), spec) in actual.iter().zip(&expected).zip(&aggs) {
                let actual = actual.finalize(spec.func);
                let expected = expected.finalize(spec.func);
                match (&actual, &expected) {
                    (Value::Double(a), Value::Double(b)) => assert!(
                        (a - b).abs() <= 1e-12 * b.abs().max(1.0),
                        "{:?}: {actual:?} != {expected:?}",
                        spec.func
                    ),
                    _ => assert_eq!(actual, expected, "{:?}", spec.func),
                }
            }
        }
    }
}
