use super::*;
use vec_hash_agg::{AggMetrics, AggregateKind, AggregateSpec, HashAggregateCore};

#[test]
fn expression_statistics_preserve_native_fallback_and_uncomputed_contracts() {
    for computed in [false, true] {
        for native in [false, true] {
            for (native_rows, fallback_rows) in [(0, 0), (17, 0), (17, 3), (u64::MAX, 1)] {
                let mut metrics = AggMetrics {
                    peak_memory_bytes: 999,
                    spill_bytes: 123,
                    native_aggregate_rows: native_rows,
                    fallback_aggregate_rows: fallback_rows,
                    expression_mode: Some("previous"),
                    native_expression_rows: 7,
                    fallback_expression_rows: 8,
                    ..Default::default()
                };
                metrics.record_expression_rows(computed, native);
                assert_eq!(metrics.peak_memory_bytes, 999);
                assert_eq!(metrics.spill_bytes, 123);
                if !computed {
                    assert_eq!(
                        (
                            metrics.expression_mode,
                            metrics.native_expression_rows,
                            metrics.fallback_expression_rows
                        ),
                        (Some("previous"), 7, 8)
                    );
                } else {
                    let expected_mode = match (fallback_rows, native) {
                        (0, true) => "compiled",
                        (0, false) => "legacy_escape",
                        _ => "scalar",
                    };
                    assert_eq!(metrics.expression_mode, Some(expected_mode));
                    assert_eq!(
                        metrics.native_expression_rows,
                        if native { native_rows } else { 0 }
                    );
                    assert_eq!(
                        metrics.fallback_expression_rows,
                        if native {
                            fallback_rows
                        } else {
                            native_rows.saturating_add(fallback_rows)
                        }
                    );
                }
            }
        }
    }
}

#[test]
fn aggregate_metadata_preserves_group_order_sources_and_optional_schema() {
    use crate::mysql::protocol::{NOT_NULL_FLAG, PRI_KEY_FLAG, UNSIGNED_FLAG};
    use crate::types::ColumnSource;
    use AggregateKind::*;
    for column_type in [
        ColumnType::LongLong,
        ColumnType::Double,
        ColumnType::Decimal {
            precision: 18,
            scale: 3,
        },
        ColumnType::VarString,
        ColumnType::Blob,
        ColumnType::Date,
        ColumnType::DateTime,
        ColumnType::Json,
    ] {
        let source = ColumnSource {
            schema: "test".into(),
            table: "a".into(),
            org_table: "t".into(),
            org_name: "v".into(),
            length: Some(257),
            collation_id: Some(255),
            flags: NOT_NULL_FLAG | PRI_KEY_FLAG | UNSIGNED_FLAG,
            ..Default::default()
        };
        let field = BatchField {
            table: "t".into(),
            alias: Some("a".into()),
            name: "v".into(),
            column_type,
            source: source.clone(),
        };
        let input = Arc::new(BatchSchema::new(vec![field.clone()]));
        let kinds = [
            CountStar,
            Count,
            Sum,
            Avg,
            Min,
            Max,
            BitAnd,
            BitOr,
            BitXor,
            VarPop,
            VarSamp,
            StddevPop,
            StddevSamp,
            CountDistinct,
            SumDistinct,
            AvgDistinct,
        ];
        let specs = kinds.map(|func| AggregateSpec {
            comparison_collation: None,
            func,
            input_column: 0,
        });
        let bridge = build_hash_agg_output_schema(&input, &[0, 0], &specs);
        let mut core = HashAggregateCore::new(vec![0, 0], specs.to_vec());
        core.consume_batch(&Batch::new(
            input.clone(),
            vec![ColumnVector::from_values(vec![])],
        ))
        .unwrap();
        let serial = core.output_schema();
        assert_eq!(bridge.fields().len(), kinds.len() + 2);
        for (a, b) in bridge.fields().iter().zip(serial.fields()) {
            assert_eq!(
                (&a.table, &a.alias, &a.name, a.column_type, &a.source),
                (&b.table, &b.alias, &b.name, b.column_type, &b.source)
            );
        }
        assert_eq!(bridge.fields()[0].source, source);
        assert_eq!(bridge.fields()[1].name, field.name);
        for (i, kind) in kinds.into_iter().enumerate() {
            let result = &bridge.fields()[i + 2];
            assert_eq!(result.table, "__agg");
            assert_eq!(result.name, format!("a{i}"));
            assert_eq!(result.alias, None);
            match kind {
                Min | Max => {
                    assert_eq!(result.column_type, column_type);
                    let nullable = ColumnSource {
                        schema: String::new(),
                        table: String::new(),
                        org_table: String::new(),
                        org_name: String::new(),
                        flags: UNSIGNED_FLAG,
                        ..source.clone()
                    };
                    assert_eq!(result.source, nullable);
                }
                BitAnd | BitOr | BitXor => {
                    assert_eq!(result.column_type, ColumnType::LongLong);
                    assert_eq!(result.source.flags, UNSIGNED_FLAG);
                    assert_eq!(result.source.org_name, "");
                }
                CountStar | Count | CountDistinct => {
                    assert_eq!(result.column_type, ColumnType::LongLong);
                    assert_eq!(result.source, ColumnSource::default());
                }
                _ => assert_eq!(result.source, ColumnSource::default()),
            }
        }
        let unknown = HashAggregateCore::new(vec![4, 2], specs.to_vec()).output_schema();
        assert_eq!(unknown.fields().len(), specs.len() + 2);
        for (i, group) in unknown.fields()[..2].iter().enumerate() {
            assert_eq!(group.name, format!("g{i}"));
            assert_eq!(group.column_type, ColumnType::VarString);
            assert_eq!(group.source, ColumnSource::default());
        }
        let empty = vec_hash_agg::aggregate_output_schema(Some(input.as_ref()), &[], &[]);
        assert!(empty.fields().is_empty());
    }
}

#[test]
fn aggregate_metadata_preserves_temporal_scales_in_serial_and_parallel() {
    for column_type in [
        ColumnType::Date,
        ColumnType::Time,
        ColumnType::DateTime,
        ColumnType::Timestamp,
    ] {
        for decimals in [0, 4, 6] {
            let schema = Arc::new(BatchSchema::new(vec![BatchField {
                table: "t".into(),
                alias: None,
                name: "v".into(),
                column_type,
                source: crate::types::ColumnSource {
                    decimals: Some(decimals),
                    ..Default::default()
                },
            }]));
            let aggs = [
                AggregateKind::Sum,
                AggregateKind::Avg,
                AggregateKind::SumDistinct,
                AggregateKind::AvgDistinct,
            ]
            .map(|func| AggregateSpec {
                comparison_collation: None,
                func,
                input_column: 0,
            });
            let parallel = build_hash_agg_output_schema(&schema, &[], &aggs);
            let mut core = HashAggregateCore::new(Vec::new(), aggs.to_vec());
            core.consume_batch(&Batch::new(schema, vec![ColumnVector::from_values(vec![])]))
                .unwrap();
            let output = core.output_schema();
            for (i, field) in output.fields().iter().enumerate() {
                let scale = if column_type == ColumnType::Date {
                    0
                } else {
                    decimals
                };
                let scale = scale + if i % 2 == 1 { 4 } else { 0 };
                assert!(
                    matches!(field.column_type, ColumnType::Decimal { scale: actual, .. } if actual == scale),
                    "{column_type:?}({decimals}) {:?}: {:?}",
                    aggs[i].func,
                    field.column_type,
                );
                assert_eq!(field.column_type, parallel.fields()[i].column_type);
            }
        }
    }
}
