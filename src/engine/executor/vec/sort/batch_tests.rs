use super::super::stream::{BatchOrdering, BatchPartitioning, MaterializedBatchOperator};
use super::*;

#[test]
fn encoded_batch_run_sort_matches_sql_comparison_with_nulls_and_collation_ties() {
    for collation in ["utf8mb4_bin", "utf8mb4_0900_ai_ci", "latin1_swedish_ci"] {
        for ascending in [true, false] {
            let fields = vec![
                crate::engine::executor::sort::SortField {
                    ascending,
                    nulls_first: !ascending,
                    collation_id: Some(crate::charset::collation_id(collation)),
                },
                crate::engine::executor::sort::SortField {
                    ascending: !ascending,
                    nulls_first: ascending,
                    collation_id: None,
                },
            ];
            let make_rows = || {
                (0..600)
                    .map(|ordinal| SortRow {
                        keys: SortKeyValues::Many(vec![
                            if ordinal % 11 == 0 {
                                Value::Null
                            } else {
                                Value::String(
                                    ["a", "A", "b", "B", "a ", "a\0"][ordinal as usize % 6].into(),
                                )
                            },
                            if ordinal % 7 == 0 {
                                Value::Null
                            } else {
                                Value::Int(ordinal as i64 % 5)
                            },
                        ]),
                        payload: vec![Value::UInt(ordinal)],
                        ordinal,
                    })
                    .collect::<Vec<_>>()
            };
            let mut expected = make_rows();
            expected.sort_unstable_by(|a, b| compare_rows(a, b, &fields, false));
            let mut actual = make_rows();
            sort_rows(&mut actual, &fields, false).unwrap();
            assert_eq!(
                actual.iter().map(|r| r.ordinal).collect::<Vec<_>>(),
                expected.iter().map(|r| r.ordinal).collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn zero_column_sort_preserves_cardinality_in_memory_and_spill() {
    for limit in [1, 1024 * 1024] {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(BatchSchema::new(vec![]));
        let input = MaterializedBatchOperator::new(
            schema.clone(),
            vec![Batch::with_row_count(schema.clone(), vec![], 7)],
        );
        let program = crate::engine::expr_program::ExprCompiler::compile(
            &crate::parser::Expr::BoundValue(Value::Int(1)),
            &crate::engine::expr_program::InputSchema::default(),
        )
        .unwrap();
        let mut tuning = crate::engine::executor::tuning::current_or_capture();
        tuning.sort_run_bytes = Some(limit);
        tuning.sort_merge_fanin = 2;
        let _tuning = crate::engine::executor::tuning::enter(tuning);
        let mut sort = ExternalSortBatchOperator::new(
            None,
            Box::new(input),
            schema,
            BatchProperties {
                ordering: BatchOrdering::Ordered(vec![]),
                partitioning: BatchPartitioning::Single,
            },
            vec![SortKeySource::Program {
                program: Arc::new(program),
                integer: None,
            }],
            vec![crate::engine::executor::sort::SortField {
                ascending: true,
                nulls_first: true,
                collation_id: None,
            }],
            dir.path(),
        );
        let mut count = 0;
        while let Some(batch) = sort.next_batch().unwrap() {
            assert!(batch.columns().is_empty());
            count += batch.num_rows();
        }
        assert_eq!(count, 7, "run limit {limit}");
    }
}

#[test]
fn spill_batches_move_owned_values_and_keep_key_ordinal_payload_order() {
    for multiple_keys in [false, true] {
        let key = vec![3u8; 4096];
        let payload = vec![7u8; 8192];
        let key_ptr = key.as_ptr();
        let payload_ptr = payload.as_ptr();
        let keys = if multiple_keys {
            SortKeyValues::Many(vec![Value::Bytes(key), Value::Null])
        } else {
            SortKeyValues::One(Value::Bytes(key))
        };
        let mut rows = vec![SortRow {
            keys,
            payload: vec![Value::Bytes(payload)],
            ordinal: 42,
        }];
        let payload_schema = BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "b".into(),
            column_type: ColumnType::Blob,
            source: Default::default(),
        }]);
        let key_count = if multiple_keys { 2 } else { 1 };
        let batch = rows_to_batch(spill_schema(&payload_schema, key_count), rows.iter_mut());
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.columns()[key_count].value_at(0), Value::UInt(42));
        for (column, ptr) in [(0, key_ptr), (key_count + 1, payload_ptr)] {
            let ColumnVector::Generic { values } = &batch.columns()[column] else {
                panic!("expected generic bytes lane")
            };
            let Value::Bytes(bytes) = &values[0] else {
                panic!("expected bytes")
            };
            assert_eq!(
                bytes.as_ptr(),
                ptr,
                "spill conversion cloned an owned payload"
            );
        }
        assert!(rows[0].payload.is_empty());
        assert!(rows[0]
            .keys
            .iter()
            .all(|value| matches!(value, Value::Null)));
        if multiple_keys {
            assert_eq!(batch.columns()[1].value_at(0), Value::Null);
        }
    }
}

#[test]
fn failed_spill_releases_reservations_and_temporary_files() {
    use crate::engine::executor::{
        mem_budget::{with_query_budget, MemBudget},
        spill_quota,
    };
    struct RestoreLimit(u64);
    impl Drop for RestoreLimit {
        fn drop(&mut self) {
            spill_quota::set_per_query_limit_bytes(self.0);
        }
    }
    let _restore = RestoreLimit(spill_quota::per_query_limit_bytes());
    spill_quota::set_per_query_limit_bytes(1);
    let dir = tempfile::tempdir().unwrap();
    let budget = Arc::new(MemBudget::new(8 * 1024 * 1024));
    with_query_budget(budget.clone(), || {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "b".into(),
            column_type: ColumnType::Blob,
            source: Default::default(),
        }]));
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Bytes(vec![1; 64]),
                Value::Bytes(vec![2; 64]),
            ])],
        );
        let input = MaterializedBatchOperator::new(schema.clone(), vec![batch]);
        let mut tuning = crate::engine::executor::tuning::current_or_capture();
        tuning.sort_run_bytes = Some(1);
        let _tuning = crate::engine::executor::tuning::enter(tuning);
        let mut sort = ExternalSortBatchOperator::new(
            None,
            Box::new(input),
            schema,
            BatchProperties {
                ordering: BatchOrdering::Ordered(vec![]),
                partitioning: BatchPartitioning::Single,
            },
            vec![SortKeySource::Column(0)],
            vec![crate::engine::executor::sort::SortField {
                ascending: true,
                nulls_first: true,
                collation_id: None,
            }],
            dir.path(),
        );
        let error = sort
            .next_batch()
            .err()
            .expect("one-byte quota must reject the spill");
        assert_eq!(error.code(), 1041);
        drop(sort);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
    });
    let tmp = crate::storage::naming::system_tmp_dir(dir.path());
    assert!(!tmp.exists() || std::fs::read_dir(tmp).unwrap().next().is_none());
}

#[test]
fn sort_row_memory_counts_capacity_and_inline_keys_exactly() {
    use crate::engine::executor::memory_footprint::AllocatedBytes;
    for key_count in [0, 1, 3] {
        let mut payload = Vec::with_capacity(16);
        let mut bytes = Vec::with_capacity(1000);
        bytes.push(1);
        payload.push(Value::Bytes(bytes));
        let keys = if key_count == 1 {
            SortKeyValues::One(Value::String(String::from("key")))
        } else {
            let mut values = Vec::with_capacity(8);
            values.resize(key_count, Value::Int(1));
            SortKeyValues::Many(values)
        };
        let row = SortRow {
            keys,
            payload,
            ordinal: 0,
        };
        let key_slots = match &row.keys {
            SortKeyValues::One(_) => 0,
            SortKeyValues::Many(values) => values.capacity(),
        };
        let expected = std::mem::size_of::<SortRow>()
            + (key_slots + row.payload.capacity()) * std::mem::size_of::<Value>()
            + row
                .keys
                .iter()
                .chain(&row.payload)
                .map(AllocatedBytes::allocated_bytes)
                .sum::<usize>();
        assert_eq!(estimate_sort_row(&row), expected, "{key_count} keys");
    }
}

#[test]
fn wide_computed_sort_keys_use_chunks_without_leaking_hidden_columns() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let dir = tempfile::tempdir().unwrap();
    let budget = Arc::new(MemBudget::new(1024 * 1024));
    with_query_budget(budget.clone(), || {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: String::new(),
            alias: None,
            name: "n".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }]));
        let input = super::super::stream::MaterializedBatchOperator::new(
            schema.clone(),
            vec![Batch::new(
                schema.clone(),
                vec![ColumnVector::from_values((0..32).map(Value::Int).collect())],
            )],
        );
        let expr =
            crate::parser::parse_expr_fragment("CONCAT(REPEAT('x',65536),LPAD(n,6,'0'))").unwrap();
        let program = expr_batch::compile_for_batch(&expr, &schema)
            .unwrap()
            .unwrap();
        let mut tuning = crate::engine::executor::tuning::current_or_capture();
        tuning.sort_run_bytes = Some(256 * 1024);
        tuning.sort_merge_fanin = 2;
        let _tuning = crate::engine::executor::tuning::enter(tuning);
        let mut sort = ExternalSortBatchOperator::new(
            None,
            Box::new(input),
            schema,
            BatchProperties::UNORDERED_SINGLE,
            vec![SortKeySource::Program {
                program,
                integer: None,
            }],
            vec![crate::engine::executor::sort::SortField {
                ascending: false,
                nulls_first: true,
                collation_id: None,
            }],
            dir.path(),
        );
        let mut expected = 31;
        while let Some(batch) = sort.next_batch().unwrap() {
            assert_eq!(batch.columns().len(), 1);
            for row in batch.visible_indices() {
                assert_eq!(batch.columns()[0].value_at(row), Value::Int(expected));
                expected -= 1;
            }
        }
        assert_eq!(expected, -1);
        drop(sort);
        assert!(budget.peak_used() < 3 * 1024 * 1024);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
    });
}
