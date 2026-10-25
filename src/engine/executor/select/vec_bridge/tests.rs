use super::hash_join::{
    direct_count_group_matched, merge_direct_count_lane_parallel, vec_hash_join_admission_cap,
};
use super::*;

#[test]
fn scalar_projection_metadata_uses_bound_temporal_decimal_and_unsigned_domains() {
    for (column_type, flags) in [
        (ColumnType::Date, 0),
        (ColumnType::DateTime, 0),
        (
            ColumnType::Decimal {
                precision: 18,
                scale: 2,
            },
            0,
        ),
        (ColumnType::LongLong, crate::mysql::protocol::UNSIGNED_FLAG),
    ] {
        let mut source = crate::types::ColumnSource::default();
        source.flags = flags;
        let input = ["a", "b"]
            .into_iter()
            .map(|name| PlanField {
                meta: super::super::plan::intern_col_meta(ColMeta {
                    invisible: false,
                    table: String::new(),
                    alias: None,
                    column: name.into(),
                    column_type,
                    source: source.clone(),
                }),
                value: Value::Null,
            })
            .collect::<Vec<_>>();
        let expr = crate::parser::parse_expr_fragment("CASE WHEN 1=1 THEN a ELSE b END").unwrap();
        let output = query_projection_template(
            &[SelectItem::Expr {
                expr: expr.clone(),
                alias: Some("v".into()),
            }],
            &input,
            &[VecResultProjection::Scalar(expr)],
        )
        .unwrap();
        assert_eq!(output[0].column_type, column_type);
        assert_eq!(
            output[0].source.flags & crate::mysql::protocol::UNSIGNED_FLAG,
            flags & crate::mysql::protocol::UNSIGNED_FLAG
        );
    }
}

#[test]
fn in_memory_cte_reads_bounded_projected_batches_without_an_admissible_mirror() {
    let columns = vec![
        crate::types::Column {
            name: "k".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        },
        crate::types::Column {
            name: "v".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        },
    ];
    let rows = (0..30_000)
        .map(|key| {
            vec![
                Value::Int(key),
                Value::String(format!("{key}-{}", "x".repeat(256))),
            ]
        })
        .collect::<Vec<_>>();
    let mut cte = crate::engine::api::CteResult::materialized(columns.clone(), rows);
    cte.consumer_count = 2;
    let cte = Arc::new(cte);
    for filtered in [false, true] {
        let budget = Arc::new(mem_budget::MemBudget::new(64 * 1024));
        mem_budget::with_query_budget(budget.clone(), || {
            crate::engine::executor::cte::with_cte_registry(
                std::collections::HashMap::from([("c".into(), cte.clone())]),
                || {
                    let plan = PhysicalPlan::CteScan {
                        name: "c".into(),
                        alias: Some("alias".into()),
                        columns: columns.clone(),
                        filter: filtered.then(|| {
                            crate::parser::parse_expr_fragment("k>=100 AND k<=2500").unwrap()
                        }),
                        estimated_rows: 30_000.0,
                        cost: Default::default(),
                        projected_columns: Some(Arc::new(vec![1, 0, 1])),
                    };
                    let mut input = try_build_cte_batch_scan(&plan).unwrap().unwrap();
                    assert_eq!(
                        budget.used(),
                        0,
                        "opening a CTE reader must not transpose the relation"
                    );
                    let first = input.stream.next_batch().unwrap().unwrap();
                    assert!(first.num_rows() < 30_000);
                    let mut actual = first
                        .visible_indices()
                        .map(|row| first.row(row))
                        .collect::<Vec<_>>();
                    while let Some(batch) = input.stream.next_batch().unwrap() {
                        actual.extend(batch.visible_indices().map(|row| batch.row(row)));
                    }
                    let expected = cte
                        .rows()
                        .iter()
                        .filter(|row| {
                            !filtered
                                || matches!(row[0], Value::Int(key) if (100..=2500).contains(&key))
                        })
                        .map(|row| vec![row[1].clone(), row[0].clone(), row[1].clone()])
                        .collect::<Vec<_>>();
                    assert_eq!(actual, expected);
                    drop(input);
                    assert_eq!(budget.used(), 0);
                    assert!(
                        budget.peak_used() < 1024 * 1024,
                        "reader memory must follow one batch"
                    );
                },
            );
        });
    }
}

#[test]
fn ordered_key_only_distinct_crosses_selected_and_empty_batches() {
    use crate::engine::executor::vec::batch::Selection;
    use crate::engine::executor::vec::stream::MaterializedBatchOperator;
    let schema = Arc::new(BatchSchema::new(vec![BatchField {
        table: "t".into(),
        alias: None,
        name: "k".into(),
        column_type: ColumnType::LongLong,
        source: Default::default(),
    }]));
    let batch = |values: Vec<Value>| {
        let rows = values.len();
        Batch::with_row_count(
            schema.clone(),
            vec![ColumnVector::from_values(values)],
            rows,
        )
    };
    let batches = vec![
        batch(vec![]),
        batch(vec![Value::Null, Value::Int(999), Value::Null])
            .with_selection(Selection::Indices(vec![0, 2])),
        batch(vec![Value::Null, Value::Int(-2), Value::Int(-2)]),
        batch(vec![]),
        batch(vec![Value::Int(-2), Value::Int(-2)]),
        batch(vec![Value::Int(-2), Value::Int(0), Value::Int(1)]),
    ];
    let mut stream = StreamingDistinctBatchOperator {
        upstream: Box::new(MaterializedBatchOperator::new(schema.clone(), batches)),
        schema,
        properties: BatchProperties::UNORDERED_SINGLE,
        key_columns: vec![0],
        last_key: Vec::new(),
        has_last_key: false,
        key_scratch: Vec::new(),
        aggregate_plan: None,
    };
    let mut values = Vec::new();
    while let Some(batch) = stream.next_batch().unwrap() {
        values.extend(
            batch
                .visible_indices()
                .map(|row| batch.columns()[0].value_at(row)),
        );
    }
    assert_eq!(
        values,
        vec![Value::Null, Value::Int(-2), Value::Int(0), Value::Int(1)]
    );
}
// The integration tests sit at the SQL level (executor::tests::vec_routing)
// because they need a real Database / PhysicalPlan / TxnView. The
// bridge itself is exercised through that surface.

#[test]
fn discovery_scan_stratifies_disjoint_ranges_without_duplicates() {
    use super::super::parallel::ScanTask;
    let original = std::collections::VecDeque::from(vec![
        ScanTask {
            storage_id: 1,
            page_lo: 10,
            page_hi: 110,
        },
        ScanTask {
            storage_id: 2,
            page_lo: 500,
            page_hi: 600,
        },
    ]);
    let mut tasks = original.clone();
    sample_scan_tasks(&mut tasks, 4);
    assert_eq!(
        tasks
            .into_iter()
            .map(|task| (task.storage_id, task.page_lo, task.page_hi))
            .collect::<Vec<_>>(),
        vec![(1, 35, 36), (1, 85, 86), (2, 525, 526), (2, 575, 576)]
    );
    let mut tasks = original.clone();
    sample_scan_tasks(&mut tasks, 1000);
    assert_eq!(tasks.len(), 200);
    let pages = tasks
        .iter()
        .map(|task| (task.storage_id, task.page_lo))
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(pages.len(), 200);
    for task in tasks {
        assert!(original
            .iter()
            .any(|range| range.storage_id == task.storage_id
                && range.page_lo <= task.page_lo
                && task.page_hi <= range.page_hi));
    }
    let mut empty = std::collections::VecDeque::new();
    sample_scan_tasks(&mut empty, 8192);
    assert!(empty.is_empty());
    let mut tasks = original;
    sample_scan_tasks(&mut tasks, 0);
    assert!(tasks.is_empty());
}

#[test]
fn integer_count_proof_bounds_omissions_null_unsigned_and_overflow() {
    let schema = Arc::new(BatchSchema::new(vec![BatchField {
        table: "t".into(),
        alias: None,
        name: "x".into(),
        column_type: ColumnType::LongLong,
        source: Default::default(),
    }]));
    let candidates = [(None, 0), (Some(-1), 1), (Some(i128::from(u64::MAX)), 2)]
        .into_iter()
        .collect::<hashbrown::HashMap<_, _, ahash::RandomState>>();
    let signed = Batch::new(
        schema.clone(),
        vec![ColumnVector::from_values(vec![
            Value::Null,
            Value::Int(-1),
            Value::Int(7),
            Value::Int(7),
            Value::Int(7),
        ])],
    );
    let unsigned = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![
            Value::UInt(u64::MAX),
            Value::UInt(u64::MAX),
            Value::UInt(7),
            Value::Null,
        ])],
    );
    let mut first = IntegerCountProofState::new(3);
    first.consume(&signed, &candidates).unwrap();
    let mut second = IntegerCountProofState::new(3);
    second.consume(&unsigned, &candidates).unwrap();
    first.merge(second);
    assert_eq!(first.counts, vec![2, 1, 2]);
    assert_eq!(first.rows, 9);
    assert_eq!(first.residual_bound(), 4);
    // Even deliberately saturated sketch cells cannot certify an answer.
    let indexes = count_topk_integer_cms_indexes(count_topk_integer_hash(Some(7)));
    for (depth, index) in indexes.into_iter().enumerate() {
        first.cms[depth * INTEGER_COUNT_TOPK_CMS_WIDTH + index] = u32::MAX;
    }
    first.consume(&signed, &candidates).unwrap();
    assert!(first.overflow);
    assert_eq!(first.residual_bound(), u64::MAX);
    let cancel = crate::engine::cancel::QueryCancelToken::noop();
    cancel.cancel_local();
    let _guard = crate::engine::cancel::enter(cancel);
    assert_eq!(
        first.consume(&signed, &candidates).unwrap_err().code(),
        1317
    );
}

#[test]
fn stable_template_reuses_governed_batch_and_input_schema() {
    let meta = super::super::plan::intern_col_meta(ColMeta {
        table: "db.t".into(),
        column: "id".into(),
        column_type: ColumnType::LongLong,
        ..Default::default()
    });
    let first_template = vec![PlanField {
        meta: meta.clone(),
        value: Value::Int(1),
    }];
    let second_template = vec![PlanField {
        meta,
        value: Value::Int(2),
    }];

    let first = template_to_batch_schema(&first_template);
    let second = template_to_batch_schema(&second_template);

    assert!(Arc::ptr_eq(&first, &second));
    assert!(first.cached_input().is_some());
}

#[test]
fn grace_locality_sampling_covers_late_rows_between_prefix_bands() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(root.path()).unwrap());
    let schema = Arc::new(BatchSchema::new(vec![test_batch_field(
        ColumnType::LongLong,
    )]));
    let output_schema = Arc::new(BatchSchema::new(vec![
        test_batch_field(ColumnType::LongLong),
        test_batch_field(ColumnType::LongLong),
    ]));
    let make_batch = |values| {
        Batch::new(
            schema.clone(),
            vec![ColumnVector::I64 {
                data: values,
                nulls: None,
            }],
        )
    };
    // Parallel scans can first deliver a short prefix from each distant
    // physical range. Most later keys lie INSIDE, not outside, its min/max.
    let is_prefix = |key: i64| key % 8192 < 128;
    let prefix = make_batch((0..32768).filter(|key| is_prefix(*key)).collect());
    let tail = make_batch((0..32768).filter(|key| !is_prefix(*key)).collect());
    let probe_batch = make_batch((0..32768).collect());
    let spec = vec_hash_join::VecJoinSpec {
        build_keys: &[0],
        probe_keys: &[0],
        key_collations: &[None],
        null_safe: &[false],
        build_cols_first: true,
        out_schema: output_schema,
        residual: None,
    };
    let budget = Arc::new(mem_budget::MemBudget::new(128 * 1024 * 1024));
    crate::util::env::with_vars(
        &[
            ("MYDB_HASH_JOIN_RANGE_PARTITION", Some("on")),
            ("MYDB_PARALLEL_DEGREE", Some("1")),
        ],
        || {
            mem_budget::with_query_budget(budget, || {
                let before = super::super::plan::HASH_JOIN_GRACE_REPARTITIONS
                    .load(std::sync::atomic::Ordering::Relaxed);
                let mut build = MaterializedBatchOperator::new(schema.clone(), vec![tail]);
                let mut probe = MaterializedBatchOperator::new(schema.clone(), vec![probe_batch]);
                let output = run_vec_grace_hash_join_stream(
                    &db,
                    TxnView::read_only(0, crate::storage::IsolationLevel::ReadCommitted),
                    vec![prefix],
                    None,
                    &mut build,
                    &mut probe,
                    &[0],
                    &[0],
                    &[false],
                    &[None],
                    crate::parser::JoinType::Inner,
                    Some(&spec),
                    schema.clone(),
                    None,
                    8,
                    vec_hash_join::BuildCap {
                        max_rows: 6000,
                        max_bytes: 64 * 1024 * 1024,
                    },
                    1,
                    0,
                    true,
                )
                .unwrap();
                let rows = vec_hash_join::flatten_batches_to_rows(output.collect().unwrap());
                assert_eq!(rows.len(), 32768);
                let mut keys = rows
                    .into_iter()
                    .map(|row| {
                        assert_eq!(row[0], row[1]);
                        match row[0] {
                            Value::Int(key) => key,
                            _ => panic!("integer key"),
                        }
                    })
                    .collect::<Vec<_>>();
                keys.sort_unstable();
                assert_eq!(keys, (0..32768).collect::<Vec<_>>());
                assert_eq!(super::super::plan::HASH_JOIN_GRACE_REPARTITIONS.load(std::sync::atomic::Ordering::Relaxed) - before, 0,
                "a uniform complete build must not inherit the gaps in its parallel-arrival prefix");
            });
        },
    );
}

#[test]
fn grace_range_sample_is_bounded_and_preserves_outliers_and_fallbacks() {
    let schema = Arc::new(BatchSchema::new(vec![test_batch_field(
        ColumnType::LongLong,
    )]));
    let batch = Batch::new(
        schema,
        vec![ColumnVector::I64 {
            data: (0..4096).collect(),
            nulls: None,
        }],
    );
    let budget = Arc::new(mem_budget::MemBudget::new(1024 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        let prefix = vec![batch.clone()];
        let range =
            GraceRangePartitioning::from_prefix(&prefix, &[0], &[0], &[false], &[None], 16, 0)
                .unwrap();
        assert!(budget.used() <= (16 * 32 + 16) * 16 + 256);
        let route = |value| {
            let mut key = Vec::new();
            crate::engine::executor::vec::key_encode::encode(&Value::Int(value), &mut key);
            range.partition(&key)
        };
        assert!(route(-1).is_some_and(|bucket| bucket < 16));
        assert!(route(i64::MAX).is_some_and(|bucket| bucket < 16));
        let routes = (0..4096).map(route).collect::<Vec<_>>();
        assert!(routes.windows(2).all(|pair| pair[0] <= pair[1]));
        assert!(routes
            .iter()
            .all(|route| route.is_some_and(|value| value < 16)));
        assert_eq!(range.partition(&[0]), None);
        assert!(
            GraceRangePartitioning::from_prefix(&prefix, &[0], &[0], &[false], &[None], 16, 1)
                .is_none()
        );
        assert!(GraceRangePartitioning::from_prefix(
            &prefix,
            &[0, 0],
            &[0, 0],
            &[false, false],
            &[None, None],
            16,
            0
        )
        .is_none());
        assert_eq!(budget.used(), 0);
        let negative = Batch::new(
            batch.schema().clone(),
            vec![ColumnVector::I64 {
                data: (-2048..2048).collect(),
                nulls: None,
            }],
        );
        let negative_range =
            GraceRangePartitioning::from_prefix(&[negative], &[0], &[0], &[false], &[None], 16, 0)
                .unwrap();
        let mut counts = [0usize; 16];
        for value in -16384..16384 {
            let mut encoded = Vec::new();
            crate::engine::executor::vec::key_encode::encode(&Value::Int(value), &mut encoded);
            counts[negative_range.partition(&encoded).unwrap()] += 1;
        }
        assert_eq!(
            counts, [2048; 16],
            "cross-zero and unseen keys must not collapse into edge buckets"
        );
        assert!(
            GraceRangePartitioning::from_prefix(&prefix, &[0], &[0], &[false], &[None], 3, 0)
                .is_none()
        );
        let selected = batch
            .clone()
            .with_selection(Selection::Indices((1024..2048).collect()));
        let selected_range =
            GraceRangePartitioning::from_prefix(&[selected], &[0], &[0], &[false], &[None], 16, 0)
                .unwrap();
        assert_eq!(
            selected_range.shift, 6,
            "unselected values must not widen the radix ranges"
        );
        for values in [
            vec![Value::Int(7); 1024],
            vec![Value::Null; 1024],
            vec![Value::String("wide".repeat(1024)); 1024],
        ] {
            let skewed = Batch::new(
                batch.schema().clone(),
                vec![ColumnVector::from_values(values)],
            );
            assert!(GraceRangePartitioning::from_prefix(
                &[skewed],
                &[0],
                &[0],
                &[false],
                &[None],
                16,
                0
            )
            .is_none());
        }
        assert_eq!(budget.used(), 0);
    });
    let tiny = Arc::new(mem_budget::MemBudget::new(64));
    mem_budget::with_query_budget(tiny.clone(), || {
        assert!(GraceRangePartitioning::from_prefix(
            &[batch],
            &[0],
            &[0],
            &[false],
            &[None],
            16,
            0
        )
        .is_none());
    });
    assert_eq!(tiny.used(), 0);
}

#[test]
fn grace_range_partitions_match_hash_oracle_for_all_join_preservation_modes() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Database::open_with_config(
            root.path(),
            crate::engine::executor::tests::memory_test_storage_config(),
        )
        .unwrap(),
    );
    for kind in ["signed", "unsigned", "decimal", "date", "text"] {
        let value = |key: i64| match kind {
            "unsigned" => Value::UInt(key as u64),
            "decimal" => Value::DecimalCompact(key * 100, 2),
            "date" => Value::Date(key as i32),
            "text" => Value::String(format!("k{key}")),
            _ => Value::Int(key),
        };
        let schema = |table: &str| {
            Arc::new(BatchSchema::new(
                ["k", "v"]
                    .into_iter()
                    .map(|name| BatchField {
                        table: table.into(),
                        alias: None,
                        name: name.into(),
                        column_type: if name == "v" {
                            ColumnType::LongLong
                        } else {
                            match kind {
                                "decimal" => ColumnType::Decimal {
                                    precision: 20,
                                    scale: 2,
                                },
                                "date" => ColumnType::Date,
                                "text" => ColumnType::VarString,
                                _ => ColumnType::LongLong,
                            }
                        },
                        source: if name == "k" && kind == "unsigned" {
                            crate::types::ColumnSource {
                                flags: crate::mysql::protocol::UNSIGNED_FLAG,
                                ..Default::default()
                            }
                        } else {
                            Default::default()
                        },
                    })
                    .collect(),
            ))
        };
        let build_schema = schema("b");
        let probe_schema = schema("p");
        let mut fields = probe_schema.fields().to_vec();
        fields.extend(build_schema.fields().iter().cloned());
        let joined = Arc::new(BatchSchema::new(fields));
        let residual = Expr::Binary {
            left: Box::new(Expr::CompoundIdentifier(vec!["p".into(), "v".into()])),
            op: BinaryOp::NotEq,
            right: Box::new(Expr::CompoundIdentifier(vec!["b".into(), "v".into()])),
        };
        let program = expr_batch::BatchExprProgram::compile(&residual, &joined)
            .unwrap()
            .unwrap();
        let make_batch = |schema, keys: Vec<Value>| {
            let len = keys.len();
            Batch::new(
                schema,
                vec![
                    ColumnVector::from_values(keys),
                    ColumnVector::I64 {
                        data: (0..len).map(|row| (row % 3) as i64).collect(),
                        nulls: None,
                    },
                ],
            )
        };
        let prefix = make_batch(
            build_schema.clone(),
            (0..64)
                .map(|row| {
                    if row % 17 == 0 {
                        Value::Null
                    } else {
                        value(row / 2)
                    }
                })
                .collect(),
        );
        let tail = make_batch(
            build_schema.clone(),
            (0..16)
                .map(|row| match row % 5 {
                    0 => value(1000),
                    1 => value(1000000),
                    2 => value(12),
                    3 if kind == "decimal" => Value::DecimalCompact(125, 2),
                    3 => value(1),
                    _ => Value::Null,
                })
                .collect(),
        );
        let probe_batch = make_batch(
            probe_schema.clone(),
            (0..80)
                .map(value)
                .chain([
                    Value::Null,
                    value(1000),
                    value(1000000),
                    if kind == "decimal" {
                        Value::DecimalCompact(125, 2)
                    } else {
                        value(1)
                    },
                    value(12),
                ])
                .collect(),
        );
        for null_safe in [false, true] {
            for join in [
                crate::parser::JoinType::Inner,
                crate::parser::JoinType::Left,
                crate::parser::JoinType::Right,
                crate::parser::JoinType::Full,
                crate::parser::JoinType::Semi,
                crate::parser::JoinType::Anti,
            ] {
                let mut oracle = None;
                for max_rows in [0, 32] {
                    let spec = vec_hash_join::VecJoinSpec {
                        build_keys: &[0],
                        probe_keys: &[0],
                        key_collations: &[None],
                        null_safe: &[null_safe],
                        build_cols_first: false,
                        out_schema: joined.clone(),
                        residual: Some(&program),
                    };
                    let run = |enabled| {
                        if enabled == "oracle" {
                            // The oracle is independent of the spill cap. Build it once
                            // in memory instead of writing a second set of spill files.
                            let mut build = MaterializedBatchOperator::new(
                                build_schema.clone(),
                                vec![prefix.clone(), tail.clone()],
                            );
                            let mut probe = MaterializedBatchOperator::new(
                                probe_schema.clone(),
                                vec![probe_batch.clone()],
                            );
                            let result = match join {
                                crate::parser::JoinType::Inner => {
                                    vec_hash_join::run_inner_hash_join(
                                        &mut build, &mut probe, &spec,
                                    )
                                }
                                crate::parser::JoinType::Left => {
                                    vec_hash_join::run_preserved_probe_hash_join(
                                        &mut build, &mut probe, &spec,
                                    )
                                }
                                crate::parser::JoinType::Right => {
                                    vec_hash_join::run_preserved_build_hash_join(
                                        &mut build, &mut probe, &spec,
                                    )
                                }
                                crate::parser::JoinType::Full => {
                                    vec_hash_join::run_full_hash_join(&mut build, &mut probe, &spec)
                                }
                                crate::parser::JoinType::Semi | crate::parser::JoinType::Anti => {
                                    vec_hash_join::run_semi_anti_hash_join(
                                        &mut build,
                                        &mut probe,
                                        &[0],
                                        &[0],
                                        &[None],
                                        &[null_safe],
                                        join == crate::parser::JoinType::Anti,
                                        probe_schema.clone(),
                                        Some((&program, joined.clone())),
                                    )
                                }
                                _ => unreachable!(),
                            }
                            .unwrap();
                            let mut rows = vec_hash_join::flatten_batches_to_rows(result)
                                .into_iter()
                                .map(|row| format!("{row:?}"))
                                .collect::<Vec<_>>();
                            rows.sort();
                            return rows;
                        }
                        crate::util::env::with_vars(
                            &[
                                ("MYDB_HASH_JOIN_RANGE_PARTITION", Some(enabled)),
                                ("MYDB_PARALLEL_DEGREE", Some("1")),
                            ],
                            || {
                                let budget = Arc::new(mem_budget::MemBudget::new(16 * 1024 * 1024));
                                mem_budget::with_query_budget(budget.clone(), || {
                                    let mut build = MaterializedBatchOperator::new(
                                        build_schema.clone(),
                                        vec![tail.clone()],
                                    );
                                    let mut probe = MaterializedBatchOperator::new(
                                        probe_schema.clone(),
                                        vec![probe_batch.clone()],
                                    );
                                    let result = run_vec_grace_hash_join_stream(
                                        &db,
                                        TxnView::read_only(
                                            0,
                                            crate::storage::IsolationLevel::ReadCommitted,
                                        ),
                                        vec![prefix.clone()],
                                        None,
                                        &mut build,
                                        &mut probe,
                                        &[0],
                                        &[0],
                                        &[null_safe],
                                        &[None],
                                        join,
                                        Some(&spec),
                                        probe_schema.clone(),
                                        Some((&program, joined.clone())),
                                        2,
                                        vec_hash_join::BuildCap {
                                            max_rows,
                                            max_bytes: 4 * 1024 * 1024,
                                        },
                                        1,
                                        0,
                                        true,
                                    )
                                    .unwrap()
                                    .collect()
                                    .unwrap();
                                    let mut rows = vec_hash_join::flatten_batches_to_rows(result)
                                        .into_iter()
                                        .map(|row| format!("{row:?}"))
                                        .collect::<Vec<_>>();
                                    rows.sort();
                                    rows
                                })
                            },
                        )
                    };
                    let oracle = oracle.get_or_insert_with(|| run("oracle"));
                    let before = VEC_HASH_JOIN_RANGE_PARTITION_COUNT
                        .load(std::sync::atomic::Ordering::Relaxed);
                    let actual = run("on");
                    assert_eq!(
                        actual, *oracle,
                        "kind={kind} join={join:?} null_safe={null_safe} max_rows={max_rows}"
                    );
                    assert_eq!(
                        VEC_HASH_JOIN_RANGE_PARTITION_COUNT
                            .load(std::sync::atomic::Ordering::Relaxed)
                            > before,
                        kind != "text"
                    );
                }
            }
        }
    }
}

#[test]
fn grace_fanout_uses_stable_build_size_and_respects_bounds() {
    let rows = 1_024;
    let fields = (0..10)
        .map(|index| BatchField {
            table: "t".into(),
            alias: None,
            name: format!("c{index}"),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        })
        .collect::<Vec<_>>();
    let batch = Batch::new(
        Arc::new(BatchSchema::new(fields)),
        (0..10)
            .map(|_| ColumnVector::I64 {
                data: vec![1; rows],
                nulls: None,
            })
            .collect(),
    );
    let cap = vec_hash_join::BuildCap {
        max_rows: u64::MAX,
        max_bytes: 256 * 1024 * 1024,
    };

    assert_eq!(
        vec_grace_partition_count(128, &[], 80_000_000.0, cap, 4),
        16
    );
    assert_eq!(
        vec_grace_partition_count(16, &[batch.clone()], 80_000_000.0, cap, 4),
        16
    );
    let small = vec_grace_partition_count(128, &[batch.clone()], rows as f64, cap, 4);
    let large = vec_grace_partition_count(128, &[batch], 80_000_000.0, cap, 4);
    assert_eq!(small, 16);
    assert_eq!(large, 128);
    assert!(large.is_power_of_two());
}

#[test]
fn scan_task_is_returned_as_bounded_contiguous_morsels() {
    let mut tasks = std::collections::VecDeque::from([
        super::super::parallel::ScanTask {
            storage_id: 7,
            page_lo: 10,
            page_hi: 50,
        },
        super::super::parallel::ScanTask {
            storage_id: 9,
            page_lo: 3,
            page_hi: 6,
        },
    ]);
    let first = pop_bounded_scan_task(&mut tasks, 16).unwrap();
    let second = pop_bounded_scan_task(&mut tasks, 16).unwrap();
    let third = pop_bounded_scan_task(&mut tasks, 16).unwrap();
    let fourth = pop_bounded_scan_task(&mut tasks, 16).unwrap();

    assert_eq!(
        (first.storage_id, first.page_lo, first.page_hi),
        (7, 10, 26)
    );
    assert_eq!(
        (second.storage_id, second.page_lo, second.page_hi),
        (7, 26, 42)
    );
    assert_eq!(
        (third.storage_id, third.page_lo, third.page_hi),
        (7, 42, 50)
    );
    assert_eq!(
        (fourth.storage_id, fourth.page_lo, fourth.page_hi),
        (9, 3, 6)
    );
    assert!(tasks.is_empty());
}

struct FixedPlanRowStream {
    remaining: usize,
}

impl super::super::plan::PlanStream for FixedPlanRowStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        Ok(Some(PlanRow::from_fields(Vec::new())))
    }
}

#[test]
fn fused_inlj_batches_progress_above_soft_watermark() {
    let budget = Arc::new(mem_budget::MemBudget::new(64));
    mem_budget::with_query_budget(budget.clone(), || {
        let held = mem_budget::force_charge_scoped(128).unwrap();
        let mut source = FixedPlanRowStream { remaining: 600 };
        let mut finished = false;
        let mut total = 0;
        let mut chunks = 0;
        while !finished {
            let (rows, charge) =
                pull_fused_inlj_left_chunk(&mut source, 256, &mut finished).unwrap();
            assert!(rows.len() <= 256);
            assert_eq!(charge.charged(), 768 * std::mem::size_of::<PlanRow>());
            total += rows.len();
            chunks += 1;
        }
        assert_eq!(total, 600);
        assert_eq!(chunks, 3, "a held producer must not reduce MRR to one row");
        assert_eq!(budget.used(), held.charged());
    });
    assert_eq!(budget.used(), 0);
}

#[test]
fn fused_inlj_shrinks_when_executor_work_area_is_full() {
    use crate::engine::executor::work_area::{SpillHint, WorkAreaHandle};
    let producer = WorkAreaHandle::register();
    let target = producer.manager().target_bytes();
    assert!(matches!(producer.try_charge(target), SpillHint::Continue));
    let budget = Arc::new(mem_budget::MemBudget::new(64));
    mem_budget::with_query_budget(budget.clone(), || {
        let mut source = FixedPlanRowStream { remaining: 3 };
        let mut finished = false;
        let (rows, charge) = pull_fused_inlj_left_chunk(&mut source, 256, &mut finished).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(!finished);
        assert_eq!(charge.charged(), 3 * std::mem::size_of::<PlanRow>());
    });
    assert_eq!(budget.used(), 0);
    drop(producer);
}

#[test]
fn fused_inlj_left_input_is_pulled_in_bounded_chunks() {
    let mut source = FixedPlanRowStream { remaining: 7 };
    let mut finished = false;

    let (first, first_charge) = pull_fused_inlj_left_chunk(&mut source, 3, &mut finished).unwrap();
    assert_eq!(first.len(), 3);
    assert!(!finished);
    assert!(first_charge.charged() > 0);

    let (second, _) = pull_fused_inlj_left_chunk(&mut source, 3, &mut finished).unwrap();
    assert_eq!(second.len(), 3);
    assert!(!finished);

    let (third, _) = pull_fused_inlj_left_chunk(&mut source, 3, &mut finished).unwrap();
    assert_eq!(third.len(), 1);
    assert!(finished);
    assert_eq!(source.remaining, 0);
}

#[test]
fn count_topk_discovered_candidates_keep_hottest_within_budget() {
    let budget = Arc::new(mem_budget::MemBudget::new(64 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        let mut hottest = (0..128)
            .map(|count| (vec![Value::String("x".repeat(1024))], count))
            .collect::<Vec<_>>();
        let mut charge = mem_budget::ScopedCharge::new_strict(0, "candidate-test").unwrap();
        assert!(charge_count_topk_discovered_candidates(
            &mut hottest,
            4,
            &mut charge
        ));
        assert!(!hottest.is_empty() && hottest.len() < 128);
        assert_eq!(hottest[0].1, 127);
        assert_eq!(hottest.last().unwrap().1, 128 - hottest.len() as u32);
        assert!(budget.used() <= budget.limit());
        drop(hottest);
        drop(charge);
        assert_eq!(budget.used(), 0);
        budget.charge(budget.limit()).unwrap();
        let mut charge = mem_budget::ScopedCharge::new_strict(0, "candidate-test").unwrap();
        let mut hottest = vec![(vec![Value::Int(1)], 10)];
        assert!(!charge_count_topk_discovered_candidates(
            &mut hottest,
            4,
            &mut charge
        ));
        assert!(hottest.is_empty());
        assert_eq!(charge.charged(), 0);
    });
    budget.release(budget.used());
}

#[test]
fn topn_externalizes_wide_candidates_without_materializing_its_output() {
    assert_wide_topn_is_bounded(1, 1024 * 1024);
}

#[test]
fn parallel_topn_externalizes_wide_candidates_with_a_shared_total_budget() {
    assert_wide_topn_is_bounded(4, 1024 * 1024);
}

#[test]
fn parallel_topn_resident_output_is_owned_and_byte_bounded() {
    assert_wide_topn_is_bounded(4, 128 * 1024 * 1024);
}

fn assert_wide_topn_is_bounded(parallel_workers: usize, budget_bytes: usize) {
    struct WideRows {
        schema: Arc<BatchSchema>,
        next: usize,
    }
    impl BatchOperator for WideRows {
        fn schema(&self) -> &Arc<BatchSchema> {
            &self.schema
        }
        fn properties(&self) -> BatchProperties {
            BatchProperties {
                ordering: Default::default(),
                partitioning: BatchPartitioning::RoundRobin { partitions: 4 },
            }
        }
        fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
            if self.next == 3000 {
                return Ok(None);
            }
            let end = (self.next + 32).min(3000);
            let keys = (self.next..end)
                .map(|n| Value::Int((n % 29) as i64))
                .collect();
            let values = (self.next..end)
                .map(|n| Value::String(format!("{n:08}{}", "x".repeat(8192))))
                .collect();
            self.next = end;
            Ok(Some(Batch::new(
                self.schema.clone(),
                vec![
                    ColumnVector::from_values(keys),
                    ColumnVector::from_values(values),
                ],
            )))
        }
    }
    let schema = Arc::new(BatchSchema::new(vec![
        test_batch_field(ColumnType::LongLong),
        test_batch_field(ColumnType::VarString),
    ]));
    let budget = Arc::new(mem_budget::MemBudget::new(budget_bytes));
    let mut expected = (0..3000usize).collect::<Vec<_>>();
    expected.sort_by_key(|n| n % 29);
    mem_budget::with_query_budget(budget.clone(), || {
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut topn = TopNBatchOperator {
            data_dir: root.path().to_path_buf(),
            external_output: None,
            upstream: Box::new(WideRows {
                schema: schema.clone(),
                next: 0,
            }),
            schema,
            properties: BatchProperties::default(),
            sources: vec![TopNKeySource::Column(0)],
            row_major_keys: false,
            specs: vec![vec_topn::SortColumnSpec::asc(0)],
            collations: vec![None],
            parallel_workers,
            k: 2048,
            offset: 17,
            output: None,
            reservation: None,
            exact_rand_index_top_one: None,
        };
        let mut observed = 17;
        let mut retained = None;
        while let Some(batch) = topn.next_batch().expect("wide TopN must spill") {
            // The byte target permits one indivisible input row plus the
            // typed lane's offsets/capacity. It must never scale with K.
            assert!(
                batch.memory_bytes() <= (256 + 16) * 1024,
                "output must stay byte bounded: {} bytes",
                batch.memory_bytes()
            );
            for row in batch.visible_indices() {
                let n = expected[observed];
                assert_eq!(
                    batch.columns()[0].value_at(row),
                    Value::Int((n % 29) as i64)
                );
                let Value::String(value) = batch.columns()[1].value_at(row) else {
                    unreachable!()
                };
                assert_eq!(&value[..8], format!("{n:08}"));
                observed += 1;
            }
            if retained.is_none() {
                retained = Some(batch);
            }
        }
        assert_eq!(observed, 2048);
        drop(topn);
        assert!(budget.used() >= retained.as_ref().unwrap().memory_bytes());
        drop(retained);
    });
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.spill_bytes(), 0);
    assert!(
        budget.peak_used() < (5 * 1024 * 1024).max(budget.limit() / 2),
        "peak {}",
        budget.peak_used()
    );
}

#[test]
fn rand_top_one_wide_output_retains_its_owner_after_the_operator_closes() {
    let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let schema = Arc::new(BatchSchema::new(vec![test_batch_field(
        ColumnType::VarString,
    )]));
    let expr = crate::parser::parse_expr_fragment("RAND()").unwrap();
    let program = expr_batch::compile_for_batch(&expr, &schema)
        .unwrap()
        .unwrap();
    let budget = Arc::new(mem_budget::MemBudget::new(1024 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        let input = MaterializedBatchOperator::new(
            schema.clone(),
            vec![Batch::new(
                schema.clone(),
                vec![ColumnVector::Generic {
                    values: vec![Value::String("x".repeat(64 * 1024)); 4],
                }],
            )],
        );
        let mut topn = TopNBatchOperator {
            data_dir: root.path().to_path_buf(),
            external_output: None,
            upstream: Box::new(input),
            schema,
            properties: Default::default(),
            sources: vec![TopNKeySource::UnseededRand(program)],
            row_major_keys: false,
            specs: vec![vec_topn::SortColumnSpec::asc(0)],
            collations: vec![None],
            parallel_workers: 1,
            k: 1,
            offset: 0,
            output: None,
            reservation: None,
            exact_rand_index_top_one: None,
        };
        let batch = topn.next_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        drop(topn);
        assert!(
            budget.used() >= batch.memory_bytes(),
            "unowned wide Top-1 output"
        );
        drop(batch);
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn count_topk_discovery_shrinks_and_releases_query_reservation() {
    let budget = Arc::new(mem_budget::MemBudget::new(129 * 1024 * 1024));
    budget.charge(24 * 1024 * 1024).unwrap();
    mem_budget::with_query_budget(budget.clone(), || {
        let before = budget.used();
        let (rows, charge) = count_topk_discovery_sample().expect("small sample fits");
        assert!(rows >= DEFAULT_BATCH_ROWS && rows < 1024 * 1024);
        assert!(charge.charged() <= (budget.limit() - before) / 2);
        assert_eq!(budget.used(), before + charge.charged());
        drop(charge);
        assert_eq!(budget.used(), before);
        budget.charge(budget.limit() - before - 1024).unwrap();
        assert!(count_topk_discovery_sample().is_none());
        assert_eq!(budget.used(), budget.limit() - 1024);
    });
    budget.release(budget.used());
}

#[test]
fn inlj_runtime_chunk_shrinks_with_live_query_headroom() {
    let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
        64 * 1024 * 1024,
    ));
    budget.charge(60 * 1024 * 1024).unwrap();
    crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
        assert_eq!(super::super::plan::inlj_runtime_chunk_rows(131_072), 1_024);
        assert_eq!(super::super::plan::inlj_runtime_chunk_rows(512), 512);
    });
    budget.release(60 * 1024 * 1024);
    assert_eq!(budget.used(), 0);
}

#[test]
fn native_cursor_preserves_primary_oom_without_consumer_tls() {
    use crate::engine::api::ResultCursor;

    for chunks in [false, true] {
        for oom in [false, true] {
            let budget = Arc::new(mem_budget::MemBudget::new(1024));
            let context = super::super::execution_context::ExecutionContext::capture_with_budget(
                TxnView::read_only(0, crate::storage::IsolationLevel::ReadCommitted),
                budget,
            )
            .fork_worker_cancellation();
            let cancellation = context.cancellation().clone();
            let error = if oom {
                context.terminal_error(
                    crate::error::catalog::ER_OUTOFMEMORY.error("native output budget exhausted"),
                )
            } else {
                cancellation.cancel_local();
                crate::engine::cancel::interrupted_error()
            };
            let expected = error.clone();
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            tx.send(Err(error)).unwrap();
            drop(tx);
            let observed = std::thread::spawn(move || {
                assert!(mem_budget::current_budget().is_none());
                let mut cursor = NativePlanBatchCursor {
                    rx: Some(rx),
                    worker: None,
                    cancellation,
                    budget: context.budget().clone(),
                    current: None,
                    current_charge: None,
                    current_rows: Vec::new().into_iter(),
                    warning_sink: Default::default(),
                };
                if chunks {
                    cursor.next_chunk().err().expect("terminal cursor error")
                } else {
                    cursor.next_row().unwrap_err()
                }
            })
            .join()
            .unwrap();
            assert_eq!(observed, expected, "chunks={chunks}, oom={oom}");
        }
    }
}

#[test]
fn native_cursor_preserves_shared_oom_on_channel_error_or_close() {
    use crate::engine::api::ResultCursor;

    for chunks in [false, true] {
        for queued_error in [false, true] {
            let budget = Arc::new(mem_budget::MemBudget::new(1024));
            let error = budget.latch_terminal_oom(
                crate::error::catalog::ER_OUTOFMEMORY.error("native source budget exhausted"),
            );
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            if queued_error {
                tx.send(Err(crate::engine::cancel::interrupted_error()))
                    .unwrap();
            }
            drop(tx);
            let mut cursor = NativePlanBatchCursor {
                rx: Some(rx),
                worker: None,
                cancellation: crate::engine::cancel::CancellationSnapshot::capture().fork_local(),
                budget,
                current: None,
                current_charge: None,
                current_rows: Vec::new().into_iter(),
                warning_sink: Default::default(),
            };
            let observed = if chunks {
                cursor.next_chunk().err().expect("terminal cursor error")
            } else {
                cursor.next_row().unwrap_err()
            };
            assert_eq!(
                observed, error,
                "chunks={chunks}, queued_error={queued_error}"
            );
        }
    }
}

#[test]
fn native_cursor_reports_producer_panic_instead_of_clean_eof() {
    use crate::engine::api::ResultCursor;

    for chunks in [false, true] {
        for panics in [false, true] {
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            let worker = std::thread::spawn(move || {
                drop(tx);
                if panics {
                    panic!("injected native producer panic");
                }
            });
            let mut cursor = NativePlanBatchCursor {
                rx: Some(rx),
                worker: Some(worker),
                cancellation: crate::engine::cancel::CancellationSnapshot::capture().fork_local(),
                budget: Arc::new(mem_budget::MemBudget::new(1024)),
                current: None,
                current_charge: None,
                current_rows: Vec::new().into_iter(),
                warning_sink: Default::default(),
            };
            let result = if chunks {
                cursor.next_chunk().map(|chunk| chunk.is_some())
            } else {
                cursor.next_row().map(|row| row.is_some())
            };
            if panics {
                let error = result.unwrap_err();
                assert!(error.is(crate::error::catalog::ER_INTERNAL_ERROR));
                assert!(error.message().contains("injected native producer panic"));
            } else {
                assert!(!result.unwrap());
            }
            assert!(cursor.worker.is_none());
        }
    }
}

#[test]
fn native_result_cursor_holds_batch_reservation_until_consumed() {
    use crate::engine::api::ResultCursor;
    let schema = Arc::new(BatchSchema::new(vec![test_batch_field(
        ColumnType::LongLong,
    )]));
    let budget = Arc::new(mem_budget::MemBudget::new(1024 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        let context = super::super::execution_context::ExecutionContext::capture(
            TxnView::read_only(0, crate::storage::IsolationLevel::RepeatableRead),
        )
        .fork_worker_cancellation();
        let batch = Batch::new(
            schema,
            vec![ColumnVector::I64 {
                data: vec![7; 128],
                nulls: None,
            }],
        );
        let bytes = batch.memory_bytes();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        assert!(send_parallel_scan_message(&sender, &context, Ok(batch)));
        drop(sender);
        let mut cursor = NativePlanBatchCursor {
            rx: Some(receiver),
            worker: None,
            cancellation: context.cancellation().clone(),
            budget: budget.clone(),
            current: None,
            current_charge: None,
            current_rows: Vec::new().into_iter(),
            warning_sink: context.warning_sink().clone(),
        };
        for _ in 0..128 {
            assert_eq!(cursor.next_row().unwrap(), Some(vec![Value::Int(7)]));
            assert_eq!(budget.used(), bytes);
        }
        assert!(cursor.next_row().unwrap().is_none());
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn parallel_scan_queue_accounts_bytes_until_consume_or_disconnect() {
    let schema = Arc::new(BatchSchema::new(vec![test_batch_field(
        ColumnType::LongLong,
    )]));
    let budget = Arc::new(mem_budget::MemBudget::new(1024 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        let context = super::super::execution_context::ExecutionContext::capture(
            TxnView::read_only(0, crate::storage::IsolationLevel::RepeatableRead),
        );
        let batch = Batch::new(
            schema,
            vec![ColumnVector::I64 {
                data: vec![7; 128],
                nulls: None,
            }],
        );
        let bytes = batch.memory_bytes();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        assert!(send_parallel_scan_message(
            &sender,
            &context,
            Ok(batch.clone())
        ));
        assert_eq!(budget.used(), bytes);
        let queued = receiver.recv().unwrap().unwrap();
        assert_eq!(budget.used(), bytes);
        drop(queued);
        assert_eq!(budget.used(), 0);
        drop(receiver);
        assert!(!send_parallel_scan_message(&sender, &context, Ok(batch)));
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn scan_morsels_and_decode_capacity_follow_byte_budget() {
    let schema = BatchSchema::new(
        (0..32)
            .map(|_| test_batch_field(ColumnType::LongLong))
            .collect(),
    );
    let budget = Arc::new(mem_budget::MemBudget::new(1024 * 1024));
    mem_budget::with_query_budget(budget, || {
        let rows = scan_decode_batch_rows(&schema);
        assert!(rows < DEFAULT_BATCH_ROWS);
        assert!(rows * 32 * std::mem::size_of::<Value>() <= scan_worker_buffer_bytes().unwrap());
        assert!(
            bounded_scan_morsel_pages(VEC_PARALLEL_SCAN_MORSEL_PAGES)
                < VEC_PARALLEL_SCAN_MORSEL_PAGES
        );
        assert_eq!(bounded_scan_morsel_pages(1), 1);
        with_scan_limit_hint(Some(1), || assert_eq!(scan_decode_batch_rows(&schema), 1));
    });
}

#[test]
fn oversized_hash_build_spills_instead_of_borrowing_its_own_query_quota() {
    use crate::engine::executor::work_area::WorkAreaHandle;
    let schema = Arc::new(BatchSchema::new(vec![test_batch_field(
        ColumnType::VarString,
    )]));
    let budget = Arc::new(mem_budget::MemBudget::new(64 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        // Deliberately low cardinality estimate, but plenty of global
        // executor space: neither may override the build's own watermark.
        let work_area = WorkAreaHandle::register();
        let batches = (0..32)
            .map(|_| {
                Batch::new(
                    schema.clone(),
                    vec![ColumnVector::from_values(vec![Value::String(
                        "x".repeat(8192),
                    )])],
                )
            })
            .collect();
        let mut input = MaterializedBatchOperator::new(schema, batches);
        let (prefix, overflow, charge) = drain_vec_join_build_to_cap(
            &mut input,
            vec_hash_join::BuildCap {
                max_rows: 0,
                max_bytes: 0,
            },
            Some(&work_area),
        )
        .unwrap();
        assert!(
            overflow,
            "an oversized build must spill before starving its consumer"
        );
        assert!(
            prefix.len() < 32,
            "stop at the crossing batch without draining the source"
        );
        assert!(
            input.next_batch().unwrap().is_some(),
            "spill continues the same input"
        );
        assert!(charge.charged() <= budget.limit() + 16 * 1024);
        drop(prefix);
        drop(charge);
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn small_hash_build_can_drain_an_occupied_parent_without_replay() {
    use crate::engine::executor::work_area::WorkAreaHandle;
    let schema = Arc::new(BatchSchema::new(vec![test_batch_field(
        ColumnType::VarString,
    )]));
    let budget = Arc::new(mem_budget::MemBudget::new(64 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        let mut parent = mem_budget::ScopedCharge::new(0);
        parent.add_force(budget.limit()).unwrap();
        let work_area = WorkAreaHandle::register();
        let mut input = MaterializedBatchOperator::new(
            schema.clone(),
            vec![Batch::new(
                schema,
                vec![ColumnVector::from_values(vec![Value::String(
                    "x".repeat(8192),
                )])],
            )],
        );
        let (prefix, overflow, charge) = drain_vec_join_build_to_cap(
            &mut input,
            vec_hash_join::BuildCap {
                max_rows: 0,
                max_bytes: 0,
            },
            Some(&work_area),
        )
        .unwrap();
        assert!(
            !overflow,
            "a small dependent build can still borrow pipeline headroom"
        );
        assert_eq!(prefix.len(), 1);
        drop((prefix, charge, parent));
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn grace_small_replay_progress_survives_occupied_parent_soft_budget() {
    let schema = Arc::new(BatchSchema::new(vec![test_batch_field(
        ColumnType::LongLong,
    )]));
    let budget = Arc::new(mem_budget::MemBudget::new(1024 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        budget.force_charge(budget.limit()).unwrap();
        let mut input = MaterializedBatchOperator::new(
            schema.clone(),
            vec![Batch::new(
                schema,
                vec![ColumnVector::I64 {
                    data: (0..128).collect(),
                    nulls: None,
                }],
            )],
        );
        let (batches, overflow, charge) = drain_vec_join_build_to_cap_impl(
            &mut input,
            vec_hash_join::BuildCap {
                max_rows: 0,
                max_bytes: 64 * 1024,
            },
            None,
            64 * 1024,
        )
        .unwrap();
        assert!(
            !overflow,
            "repartitioning cannot free a blocking parent's memory"
        );
        assert_eq!(batches.iter().map(Batch::visible_rows).sum::<usize>(), 128);
        drop((batches, charge));
        assert_eq!(budget.used(), budget.limit());
        budget.release(budget.limit());
    });
    assert_eq!(budget.used(), 0);
}

#[test]
fn grace_materialization_boundary_stops_reading_at_budget() {
    let field = test_batch_field(ColumnType::LongLong);
    let template = vec![batch_field_template(&field)];
    let schema = Arc::new(BatchSchema::new(vec![field]));
    let budget = Arc::new(mem_budget::MemBudget::new(4096));
    let mut read = 0;
    mem_budget::with_query_budget(budget.clone(), || {
        let batches = std::iter::from_fn(|| {
            read += 1;
            assert!(
                read < 100,
                "materializer read the complete stream before enforcing budget"
            );
            Some(Ok(Batch::new(
                schema.clone(),
                vec![ColumnVector::from_values(vec![Value::Int(1); 8])],
            )))
        });
        assert_eq!(
            materialize_join_batches(batches, &template)
                .unwrap_err()
                .code(),
            1037
        );
    });
    assert_eq!(budget.used(), 0);
}

#[test]
fn grace_output_spills_selected_rows_and_releases_lazy_readers() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(root.path()).unwrap());
    let schema = Arc::new(BatchSchema::new(vec![
        test_batch_field(ColumnType::LongLong),
        test_batch_field(ColumnType::VarString),
    ]));
    let budget = Arc::new(mem_budget::MemBudget::new(64 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        let batches = (0..16)
            .map(|part| {
                Batch::new(
                    schema.clone(),
                    vec![
                        ColumnVector::I64 {
                            data: (0..1024).map(|row| part * 1024 + row).collect(),
                            nulls: None,
                        },
                        ColumnVector::from_values(
                            (0..1024)
                                .map(|row| {
                                    if row % 3 == 0 {
                                        Value::Null
                                    } else {
                                        Value::String(format!("value-{row}"))
                                    }
                                })
                                .collect(),
                        ),
                    ],
                )
                .with_selection(Selection::Indices(vec![1, 3, 17, 1023]))
            })
            .collect();
        let mut output = GraceJoinOutput::store(&db, schema, batches).unwrap();
        assert!(matches!(
            output.parts.front(),
            Some(GraceJoinPart::Disk { reader: None, .. })
        ));
        assert!(
            budget.used() < 32 * 1024,
            "only file metadata, no read buffer, remains resident"
        );
        let mut seen = 0;
        while let Some(batch) = output.next_batch().unwrap() {
            assert_eq!(batch.num_rows(), 4);
            for (row, original) in [1, 3, 17, 1023].into_iter().enumerate() {
                assert_eq!(
                    batch.columns()[0].value_at(row),
                    Value::Int(seen * 1024 + original)
                );
                assert_eq!(
                    batch.columns()[1].value_at(row),
                    if original % 3 == 0 {
                        Value::Null
                    } else {
                        Value::String(format!("value-{original}"))
                    }
                );
            }
            seen += 1;
        }
        assert_eq!(seen, 16);
        assert_eq!(budget.used(), 0);
        assert!(output.parts.is_empty());
    });
}

#[test]
fn grace_semi_anti_residual_keeps_all_build_matches() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(root.path()).unwrap());
    let schema = |table: &str| {
        Arc::new(BatchSchema::new(
            ["k", "v"]
                .into_iter()
                .map(|name| BatchField {
                    table: table.into(),
                    alias: None,
                    name: name.into(),
                    column_type: ColumnType::LongLong,
                    source: Default::default(),
                })
                .collect(),
        ))
    };
    let build_schema = schema("b");
    let probe_schema = schema("p");
    let mut fields = probe_schema.fields().to_vec();
    fields.extend(build_schema.fields().iter().cloned());
    let candidates = Arc::new(BatchSchema::new(fields));
    let residual = Expr::Binary {
        left: Box::new(Expr::CompoundIdentifier(vec!["p".into(), "v".into()])),
        op: crate::parser::BinaryOp::NotEq,
        right: Box::new(Expr::CompoundIdentifier(vec!["b".into(), "v".into()])),
    };
    let program = expr_batch::BatchExprProgram::compile(&residual, &candidates)
        .unwrap()
        .unwrap();
    let batch = |schema: Arc<BatchSchema>, pairs: Vec<(i64, i64)>| {
        Batch::new(
            schema,
            vec![
                ColumnVector::I64 {
                    data: pairs.iter().map(|p| p.0).collect(),
                    nulls: None,
                },
                ColumnVector::I64 {
                    data: pairs.iter().map(|p| p.1).collect(),
                    nulls: None,
                },
            ],
        )
    };
    for join in [crate::parser::JoinType::Semi, crate::parser::JoinType::Anti] {
        let mut build = MaterializedBatchOperator::new(
            build_schema.clone(),
            vec![batch(
                build_schema.clone(),
                (0..256).flat_map(|k| [(k, 0), (k, k % 2)]).collect(),
            )],
        );
        let mut probe = MaterializedBatchOperator::new(
            probe_schema.clone(),
            vec![batch(
                probe_schema.clone(),
                (0..256).map(|k| (k, 0)).collect(),
            )],
        );
        let result = run_vec_grace_hash_join(
            &db,
            TxnView::read_only(0, crate::storage::IsolationLevel::ReadCommitted),
            Vec::new(),
            None,
            &mut build,
            &mut probe,
            &[0],
            &[0],
            &[false],
            &[None],
            join,
            None,
            probe_schema.clone(),
            Some((&program, candidates.clone())),
            4,
            vec_hash_join::BuildCap {
                max_rows: 0,
                max_bytes: 4 * 1024 * 1024,
            },
            2,
            0,
        )
        .unwrap();
        let rows = vec_hash_join::flatten_batches_to_rows(result);
        assert_eq!(rows.len(), 128, "join={join:?}");
        for row in rows {
            let Value::Int(k) = row[0] else {
                panic!("integer key");
            };
            assert_eq!(k % 2 == 1, join == crate::parser::JoinType::Semi);
        }
    }
}

#[test]
fn grace_parallel_replay_does_not_reserve_the_build_twice() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    let schema = Arc::new(BatchSchema::new(vec![test_batch_field(
        ColumnType::LongLong,
    )]));
    let output_schema = Arc::new(BatchSchema::new(vec![
        test_batch_field(ColumnType::LongLong),
        test_batch_field(ColumnType::LongLong),
    ]));
    let make_input = || {
        MaterializedBatchOperator::new(
            schema.clone(),
            vec![Batch::new(
                schema.clone(),
                vec![ColumnVector::I64 {
                    data: (0..256).collect(),
                    nulls: None,
                }],
            )],
        )
    };
    let mut build = make_input();
    let mut probe = make_input();
    let residual = expr_batch::BatchExprProgram::compile(&Expr::Placeholder(0), &output_schema)
        .unwrap()
        .unwrap();
    let spec = vec_hash_join::VecJoinSpec {
        build_keys: &[0],
        probe_keys: &[0],
        key_collations: &[None],
        null_safe: &[false],
        build_cols_first: true,
        out_schema: output_schema.clone(),
        residual: Some(&residual),
    };
    // Include the real reader buffers and capacity-sized 32768-row lanes.
    // Their concurrent footprint fits 4 MiB, even before either worker drains.
    let budget = Arc::new(mem_budget::MemBudget::new(4 * 1024 * 1024));
    let before =
        super::super::plan::HASH_JOIN_GRACE_REPARTITIONS.load(std::sync::atomic::Ordering::Relaxed);
    let workers_before =
        VEC_HASH_JOIN_GRACE_PARALLEL_WORKERS.load(std::sync::atomic::Ordering::Relaxed);
    let batches = crate::engine::params::with_params(vec![Value::Int(1)], || {
        crate::util::env::with_vars(
            &[
                ("MYDB_PARALLEL_DEGREE", Some("2")),
                ("MYDB_PARALLEL_DISABLE", Some("0")),
            ],
            || {
                mem_budget::with_query_budget(budget.clone(), || {
                    run_vec_grace_hash_join(
                        &db,
                        TxnView::read_only(0, crate::storage::IsolationLevel::ReadCommitted),
                        Vec::new(),
                        None,
                        &mut build,
                        &mut probe,
                        &[0],
                        &[0],
                        &[false],
                        &[None],
                        crate::parser::JoinType::Inner,
                        Some(&spec),
                        output_schema,
                        None,
                        2,
                        vec_hash_join::BuildCap {
                            max_rows: 0,
                            max_bytes: 4 * 1024 * 1024,
                        },
                        2,
                        0,
                    )
                    .unwrap()
                })
            },
        )
    });
    let mut keys = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            assert_eq!(
                batch.columns()[0].value_at(row),
                batch.columns()[1].value_at(row)
            );
            let Value::Int(key) = batch.columns()[0].value_at(row) else {
                panic!("integer key");
            };
            keys.push(key);
        }
    }
    keys.sort_unstable();
    assert_eq!(keys, (0..256).collect::<Vec<_>>());
    assert!(
        VEC_HASH_JOIN_GRACE_PARALLEL_WORKERS.load(std::sync::atomic::Ordering::Relaxed)
            > workers_before
    );
    assert_eq!(
        super::super::plan::HASH_JOIN_GRACE_REPARTITIONS.load(std::sync::atomic::Ordering::Relaxed),
        before,
        "budget-resident partitions must not recursively spill",
    );
    assert_eq!(budget.used(), 0, "replay reservations must be released");
}

#[test]
fn grace_releases_unused_index_allowance_before_optional_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    let schema = Arc::new(BatchSchema::new(vec![test_batch_field(
        ColumnType::LongLong,
    )]));
    let output = Arc::new(BatchSchema::new(vec![
        test_batch_field(ColumnType::LongLong),
        test_batch_field(ColumnType::LongLong),
    ]));
    let batch = Batch::new(
        schema.clone(),
        vec![ColumnVector::I64 {
            data: (0..256).collect(),
            nulls: None,
        }],
    );
    let mut remaining = MaterializedBatchOperator::new(schema.clone(), Vec::new());
    let mut probe = MaterializedBatchOperator::new(schema, vec![batch.clone()]);
    let budget = Arc::new(mem_budget::MemBudget::new(4 * 1024 * 1024));
    let before =
        super::super::plan::HASH_JOIN_BLOOM_BUILT.load(std::sync::atomic::Ordering::Relaxed);
    let result = mem_budget::with_query_budget(budget.clone(), || {
        // The abandoned index allowance fills the soft quota. Grace must
        // keep the batches charged, release only unused headroom, and
        // admit the optional filter without raising any memory limit.
        let charge = mem_budget::ScopedCharge::new_strict(budget.limit(), "grace prefix").unwrap();
        let spec = vec_hash_join::VecJoinSpec {
            build_keys: &[0],
            probe_keys: &[0],
            key_collations: &[None],
            null_safe: &[false],
            build_cols_first: true,
            out_schema: output.clone(),
            residual: None,
        };
        run_vec_grace_hash_join(
            &db,
            TxnView::read_only(0, crate::storage::IsolationLevel::ReadCommitted),
            vec![batch],
            Some(charge),
            &mut remaining,
            &mut probe,
            &[0],
            &[0],
            &[false],
            &[None],
            crate::parser::JoinType::Inner,
            Some(&spec),
            output,
            None,
            2,
            vec_hash_join::BuildCap {
                max_rows: 0,
                max_bytes: 4 * 1024 * 1024,
            },
            2,
            0,
        )
        .unwrap()
    });
    assert_eq!(result.iter().map(Batch::visible_rows).sum::<usize>(), 256);
    assert!(
        super::super::plan::HASH_JOIN_BLOOM_BUILT.load(std::sync::atomic::Ordering::Relaxed)
            > before
    );
    drop(result);
    drop(probe);
    assert_eq!(budget.used(), 0);
}

#[test]
fn grace_parallel_worker_cap_preserves_the_operator_grant() {
    let cap = vec_hash_join::BuildCap {
        max_rows: 10_003,
        max_bytes: 64 * 1024 * 1024 + 3,
    };
    let worker = vec_grace_worker_cap(cap, 4).expect("four workers should fit the grant");
    assert_eq!(worker.max_rows, 2_500);
    assert_eq!(worker.max_bytes, 16 * 1024 * 1024);
    assert!(worker.max_rows.saturating_mul(4) <= cap.max_rows);
    assert!(worker.max_bytes.saturating_mul(4) <= cap.max_bytes);
}

#[test]
fn vec_hash_join_derived_soft_quota_defers_to_registered_work_area() {
    let derived = vec_hash_join_admission_cap(10_000, 64 * 1024 * 1024, false, true);
    assert_eq!(derived.max_rows, 10_000);
    assert_eq!(derived.max_bytes, 0);

    let explicit = vec_hash_join_admission_cap(10_000, 64 * 1024 * 1024, true, true);
    assert_eq!(explicit.max_bytes, 64 * 1024 * 1024);

    let no_governor = vec_hash_join_admission_cap(10_000, 64 * 1024 * 1024, false, false);
    assert_eq!(no_governor.max_bytes, 64 * 1024 * 1024);
}

fn test_batch_field(column_type: ColumnType) -> BatchField {
    BatchField {
        table: String::new(),
        alias: None,
        name: "v".into(),
        column_type,
        source: Default::default(),
    }
}

#[test]
fn sparse_scan_batches_coalesce_and_preserve_visible_order() {
    let schema = Arc::new(BatchSchema::new(vec![test_batch_field(
        ColumnType::LongLong,
    )]));
    let first = Batch::new(
        schema.clone(),
        vec![ColumnVector::from_values(vec![
            Value::Int(10),
            Value::Int(20),
            Value::Int(30),
            Value::Int(40),
        ])],
    )
    .with_selection(Selection::Indices(vec![3]));
    let second = Batch::new(
        schema.clone(),
        vec![ColumnVector::from_values(vec![
            Value::Int(50),
            Value::Null,
            Value::Int(70),
            Value::Int(80),
        ])],
    )
    .with_selection(Selection::Indices(vec![1, 2]));
    let upstream = Box::new(MaterializedBatchOperator::new(schema, vec![first, second]));
    let mut stream = SparseSelectionCoalesceStream::new(upstream);

    let output = stream.next_batch().unwrap().unwrap();
    assert_eq!(output.num_rows(), 3);
    assert!(output.selection().is_none());
    assert_eq!(output.columns()[0].value_at(0), Value::Int(40));
    assert_eq!(output.columns()[0].value_at(1), Value::Null);
    assert_eq!(output.columns()[0].value_at(2), Value::Int(70));
    assert!(stream.next_batch().unwrap().is_none());
}

#[test]
fn setop_passthrough_accepts_identical_and_decimal_widening_columns() {
    let decimal = ColumnVector::Decimal64 {
        data: vec![12_345, -67_890],
        scale: 2,
        nulls: None,
    };
    let source = test_batch_field(ColumnType::Decimal {
        precision: 10,
        scale: 2,
    });
    let wider = test_batch_field(ColumnType::Decimal {
        precision: 18,
        scale: 2,
    });
    assert!(SetOpBatchOperator::can_passthrough_column(
        &decimal, &source, &source
    ));
    assert!(SetOpBatchOperator::can_passthrough_column(
        &decimal, &source, &wider
    ));
    assert!(SetOpBatchOperator::can_passthrough_column(
        &ColumnVector::I64 {
            data: vec![1, 2],
            nulls: None,
        },
        &test_batch_field(ColumnType::LongLong),
        &test_batch_field(ColumnType::LongLong),
    ));
}

#[test]
fn setop_passthrough_rejects_decimal_rescale_and_narrowing() {
    let decimal = ColumnVector::Decimal64 {
        data: vec![12_345],
        scale: 2,
        nulls: None,
    };
    let source = test_batch_field(ColumnType::Decimal {
        precision: 10,
        scale: 2,
    });
    for target in [
        ColumnType::Decimal {
            precision: 9,
            scale: 2,
        },
        ColumnType::Decimal {
            precision: 12,
            scale: 4,
        },
    ] {
        assert!(!SetOpBatchOperator::can_passthrough_column(
            &decimal,
            &source,
            &test_batch_field(target),
        ));
    }
    assert!(!SetOpBatchOperator::can_passthrough_column(
        &ColumnVector::I64 {
            data: vec![1],
            nulls: None,
        },
        &test_batch_field(ColumnType::LongLong),
        &test_batch_field(ColumnType::Decimal {
            precision: 20,
            scale: 2,
        }),
    ));
}

#[test]
fn grace_parallel_worker_cap_keeps_finite_row_limit_finite() {
    let cap = vec_hash_join::BuildCap {
        max_rows: 2,
        max_bytes: 4 * 64 * 1024,
    };
    let worker = vec_grace_worker_cap(cap, 4).expect("minimum byte grants should be admitted");
    assert_eq!(
        worker.max_rows, 1,
        "zero means unlimited and is not a valid quotient"
    );
    assert_eq!(worker.max_bytes, 64 * 1024);
    assert!(vec_grace_worker_cap(cap, 1).is_none());
    assert!(vec_grace_worker_cap(
        vec_hash_join::BuildCap {
            max_rows: 100,
            max_bytes: 4 * 64 * 1024 - 1,
        },
        4,
    )
    .is_none());
}

#[test]
fn expr_column_index_resolves_unqualified_unique_name() {
    let template = vec![PlanField {
        meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta {
            invisible: false,
            table: "t".into(),
            alias: None,
            column: "x".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }),
        value: Value::Null,
    }];
    assert_eq!(
        expr_column_index_in_template(&Expr::Identifier("x".into()), &template),
        Some(0)
    );
}

#[test]
fn expr_column_index_rejects_ambiguous() {
    let template = vec![
        PlanField {
            meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta {
                invisible: false,
                table: "t".into(),
                alias: None,
                column: "x".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            }),
            value: Value::Null,
        },
        PlanField {
            meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta {
                invisible: false,
                table: "u".into(),
                alias: None,
                column: "x".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            }),
            value: Value::Null,
        },
    ];
    assert_eq!(
        expr_column_index_in_template(&Expr::Identifier("x".into()), &template),
        None
    );
}

#[test]
fn limit_batch_stream_stops_on_exact_window_across_batches() {
    let schema = Arc::new(BatchSchema::new(vec![BatchField {
        table: "t".into(),
        alias: None,
        name: "x".into(),
        column_type: ColumnType::LongLong,
        source: Default::default(),
    }]));
    let batches = vec![
        Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values((0..5).map(Value::Int).collect())],
        ),
        Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values((5..10).map(Value::Int).collect())],
        ),
    ];
    let upstream = Box::new(MaterializedBatchOperator::new(schema.clone(), batches));
    let properties = upstream.properties();
    let mut stream = LimitBatchStream {
        upstream,
        schema,
        properties,
        skip: 3,
        remaining: 4,
    };
    let mut values = Vec::new();
    while let Some(batch) = stream.next_batch().unwrap() {
        match batch.selection() {
            Some(selection) => {
                values.extend(selection.iter().map(|row| batch.columns()[0].value_at(row)))
            }
            None => {
                values.extend((0..batch.num_rows()).map(|row| batch.columns()[0].value_at(row)))
            }
        }
    }
    assert_eq!(
        values,
        vec![Value::Int(3), Value::Int(4), Value::Int(5), Value::Int(6)]
    );
}

#[test]
fn mixed_computed_programs_route_numeric_slots_independently() {
    let schema = Arc::new(BatchSchema::new(vec![
        BatchField {
            table: "t".into(),
            alias: None,
            name: "a".into(),
            column_type: ColumnType::Decimal {
                precision: 10,
                scale: 2,
            },
            source: Default::default(),
        },
        BatchField {
            table: "t".into(),
            alias: None,
            name: "b".into(),
            column_type: ColumnType::Decimal {
                precision: 10,
                scale: 2,
            },
            source: Default::default(),
        },
    ]));
    let batch = Batch::new(
        schema,
        vec![
            ColumnVector::Decimal64 {
                data: vec![100, 200],
                scale: 2,
                nulls: None,
            },
            ColumnVector::Decimal64 {
                data: vec![50, 25],
                scale: 2,
                nulls: None,
            },
        ],
    );
    let programs = vec![
        None,
        Some(ComputedNumericProgram::Arithmetic(
            super::super::plan::CompiledNumericExpr::Binary {
                left: Box::new(super::super::plan::CompiledNumericExpr::Column(0)),
                op: BinaryOp::Mul,
                right: Box::new(super::super::plan::CompiledNumericExpr::Column(1)),
            },
        )),
    ];
    assert!(try_eval_numeric_computed_column(&programs, 0, &batch).is_none());
    let result = try_eval_numeric_computed_column(&programs, 1, &batch)
        .expect("a numeric slot following a non-numeric slot must retain its kernel");
    let ColumnVector::Decimal128 { data, scale, .. } = result else {
        panic!("decimal multiplication should widen to Decimal128")
    };
    assert_eq!(scale, 4);
    assert_eq!(data, [5_000, 5_000]);

    let mut row_major = try_eval_row_major_numeric_columns(&programs, &batch)
        .expect("a non-numeric neighbor must not disable the numeric lane");
    assert!(row_major[0].is_none());
    assert!(matches!(
        row_major[1].take(),
        Some(ColumnVector::Decimal128 { .. })
    ));

    let integer_batch = Batch::new(
        Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "i".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }])),
        vec![ColumnVector::I64 {
            data: vec![1, 2],
            nulls: None,
        }],
    );
    let rejected = vec![Some(ComputedNumericProgram::Arithmetic(
        super::super::plan::CompiledNumericExpr::Column(0),
    ))];
    assert!(try_eval_row_major_numeric_columns(&rejected, &integer_batch).is_none());
}

#[test]
fn equality_case_family_treats_explicit_else_null_as_absent() {
    let fields = vec![
        BatchField {
            table: "t".into(),
            alias: None,
            name: "day_name".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        },
        BatchField {
            table: "t".into(),
            alias: None,
            name: "sales".into(),
            column_type: ColumnType::Decimal {
                precision: 7,
                scale: 2,
            },
            source: Default::default(),
        },
    ];
    let schema = Arc::new(BatchSchema::new(fields.clone()));
    let template = fields.iter().map(batch_field_template).collect::<Vec<_>>();
    let make_case = |day: &str| Expr::Case {
        operand: None,
        conditions: vec![Expr::Binary {
            left: Box::new(Expr::Identifier("day_name".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(crate::parser::Literal::String(day.into()))),
        }]
        .into_boxed_slice(),
        results: vec![Expr::Identifier("sales".into())],
        else_result: Some(Box::new(Expr::Literal(crate::parser::Literal::Null))),
    };
    let programs = ["Sunday", "Monday"]
        .into_iter()
        .map(|day| ComputedNumericProgram::compile(&make_case(day), &template, &schema))
        .collect::<Vec<_>>();
    assert!(programs.iter().all(Option::is_some));

    let batch = Batch::new(
        schema,
        vec![
            ColumnVector::from_values(vec![
                Value::String("Sunday".into()),
                Value::String("Monday".into()),
                Value::String("Tuesday".into()),
            ]),
            ColumnVector::Decimal64 {
                data: vec![100, 200, 300],
                scale: 2,
                nulls: None,
            },
        ],
    );
    let output = try_eval_equality_case_family(&programs, &batch)
        .expect("compatible equality CASE inputs should share one typed pass");
    assert_eq!(output[0].value_at(0), Value::DecimalCompact(100, 2));
    assert_eq!(output[0].value_at(1), Value::Null);
    assert_eq!(output[1].value_at(0), Value::Null);
    assert_eq!(output[1].value_at(1), Value::DecimalCompact(200, 2));
    assert_eq!(output[1].value_at(2), Value::Null);
}

#[test]
fn searched_equality_integer_case_builds_typed_count_lane() {
    let field = BatchField {
        table: "t".into(),
        alias: None,
        name: "status".into(),
        column_type: ColumnType::VarString,
        source: Default::default(),
    };
    let schema = Arc::new(BatchSchema::new(vec![field.clone()]));
    let template = vec![batch_field_template(&field)];
    let expr = Expr::Case {
        operand: None,
        conditions: vec![Expr::Binary {
            left: Box::new(Expr::Identifier("status".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(crate::parser::Literal::String(
                "accept".into(),
            ))),
        }]
        .into_boxed_slice(),
        results: vec![Expr::Literal(crate::parser::Literal::Int(1))],
        else_result: Some(Box::new(Expr::Literal(crate::parser::Literal::Null))),
    };
    let program = ComputedNumericProgram::compile(&expr, &template, &schema)
        .expect("COUNT(CASE equality) should compile to a typed lane");
    assert!(matches!(
        program,
        ComputedNumericProgram::SearchedEqualityCaseI64 { .. }
    ));
    let batch = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![
            Value::String("accept".into()),
            Value::String("reject".into()),
            Value::Null,
        ])],
    );
    let column = program
        .try_eval_batch_decimal(&batch)
        .expect("typed equality CASE evaluation");
    assert_eq!(column.value_at(0), Value::Int(1));
    assert_eq!(column.value_at(1), Value::Null);
    assert_eq!(column.value_at(2), Value::Null);
}

#[test]
fn parallel_direct_count_merge_and_match_bitmap_cover_shard_boundaries() {
    let mut target = (0..10_003).map(|value| value as u64).collect::<Vec<_>>();
    let mut source = vec![2u64; target.len()];
    source[77] = u64::MAX;
    merge_direct_count_lane_parallel(&mut target, &source, 4);
    assert_eq!(target[0], 2);
    assert_eq!(target[10_002], 10_004);
    assert_eq!(target[77], u64::MAX, "COUNT merge must saturate");

    let mut words = vec![0u64; 3];
    for group in [0usize, 63, 64, 129] {
        words[group / u64::BITS as usize] |= 1u64 << (group % u64::BITS as usize);
    }
    for group in [0usize, 63, 64, 129] {
        assert!(direct_count_group_matched(&words, group));
    }
    for group in [1usize, 62, 65, 128, 191] {
        assert!(!direct_count_group_matched(&words, group));
    }
}

#[test]
fn duplicate_fingerprint_selection_keeps_repeats_collisions_and_k_singletons() {
    let schema = Arc::new(BatchSchema::new(vec![
        BatchField {
            table: "t".into(),
            alias: None,
            name: "a".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        },
        BatchField {
            table: "t".into(),
            alias: None,
            name: "b".into(),
            column_type: ColumnType::Long,
            source: Default::default(),
        },
    ]));
    let batch = Batch::new(
        schema.clone(),
        vec![
            ColumnVector::I64 {
                data: vec![1, 2, 1, 3, 4, 4, 4],
                nulls: None,
            },
            ColumnVector::I64 {
                data: vec![10, 20, 10, 30, 40, 40, 40],
                nulls: None,
            },
        ],
    );
    let mut records = (0..batch.num_rows())
        .map(|row| {
            (u64::from(fixed_integer_group_fingerprint(&batch, &[0, 1], row).unwrap()) << 32)
                | row as u64
        })
        .collect::<Vec<_>>();
    radix_sort_fingerprint_records(&mut records, 4).unwrap();
    let mut compact_groups = Vec::new();
    for (column, field) in schema.fields().iter().enumerate() {
        let mut compact = CompactIntegerColumn::new(field, batch.num_rows()).unwrap();
        for row in 0..batch.num_rows() {
            compact.push(&batch.columns()[column], row).unwrap();
        }
        compact_groups.push(compact);
    }
    let null_masks = vec![0; batch.num_rows()];
    let (selected, _charge) =
        select_duplicate_fingerprint_ordinals(&mut records, 1, 2, |ordinal| {
            compact_integer_group_fingerprint(
                &compact_groups,
                &null_masks,
                &[0, 1],
                ordinal as usize,
            )
        })
        .unwrap();
    let first = selected
        .iter()
        .map(|&row| batch.columns()[0].value_at(row as usize))
        .collect::<Vec<_>>();
    assert_eq!(first.len(), 6);
    assert_eq!(
        first
            .iter()
            .filter(|value| **value == Value::Int(1))
            .count(),
        2
    );
    assert_eq!(
        first
            .iter()
            .filter(|value| **value == Value::Int(4))
            .count(),
        3
    );

    // A primary collision is pruned when the independent fingerprint
    // proves that both keys are singletons.
    let mut collision_records = vec![(7u64 << 32), (7u64 << 32) | 1, (9u64 << 32) | 2];
    assert_eq!(
        select_duplicate_fingerprint_ordinals(&mut collision_records, 0, 2, |row| Some(row))
            .unwrap()
            .0,
        Vec::<u32>::new()
    );

    // A collision in both fingerprints is still retained for canonical
    // key comparison, keeping the algorithm exact for adversarial input.
    let mut combined_collision = vec![(7u64 << 32), (7u64 << 32) | 1];
    assert_eq!(
        select_duplicate_fingerprint_ordinals(&mut combined_collision, 0, 2, |_| Some(11))
            .unwrap()
            .0,
        vec![0, 1]
    );

    let mut compact = CompactIntegerColumn::new(&schema.fields()[1], batch.num_rows()).unwrap();
    let mut null_masks = Vec::new();
    for row in 0..batch.num_rows() {
        compact.push(&batch.columns()[1], row).unwrap();
        null_masks.push(0);
    }
    let gathered = compact.gather(&selected, &null_masks, 1);
    assert_eq!(gathered.len(), selected.len());
    for (output, &input) in selected.iter().enumerate() {
        assert_eq!(
            gathered.value_at(output),
            batch.columns()[1].value_at(input as usize)
        );
    }

    let unsigned_field = BatchField {
        table: "t".into(),
        alias: None,
        name: "u".into(),
        column_type: ColumnType::Short,
        source: crate::types::ColumnSource {
            flags: crate::mysql::protocol::UNSIGNED_FLAG,
            ..Default::default()
        },
    };
    let mut validity = Bitmap::new_all_valid(2);
    validity.set(1, false);
    let unsigned = ColumnVector::U64 {
        data: vec![u16::MAX as u64, 0],
        nulls: Some(validity),
    };
    let mut compact = CompactIntegerColumn::new(&unsigned_field, 2).unwrap();
    compact.push(&unsigned, 0).unwrap();
    compact.push(&unsigned, 1).unwrap();
    let gathered = compact.gather(&[1, 0], &[0, 0b1], 0b1);
    assert_eq!(gathered.value_at(0), Value::Null);
    assert_eq!(gathered.value_at(1), Value::UInt(u16::MAX as u64));
}

#[test]
fn aggregate_scan_work_steals_without_repeating_pages() {
    let charge = Arc::new(
        mem_budget::ControlledMemoryCharge::try_new(
            mem_budget::ControlledMemoryCategory::StorageScratch,
            4096,
        )
        .unwrap(),
    );
    let work = AggregateScanWork {
        queues: vec![
            parking_lot::Mutex::new(std::collections::VecDeque::from([
                super::super::parallel::ScanTask {
                    storage_id: 1,
                    page_lo: 0,
                    page_hi: 97,
                },
                super::super::parallel::ScanTask {
                    storage_id: 2,
                    page_lo: 0,
                    page_hi: 53,
                },
            ])),
            parking_lot::Mutex::new(std::collections::VecDeque::new()),
        ],
        morsel_pages: 3,
        _charge: charge,
    };
    let mut pages = std::thread::scope(|scope| {
        (0..4)
            .map(|worker| {
                let work = &work;
                scope.spawn(move || {
                    let mut pages = Vec::new();
                    while let Some(task) = work.next(worker) {
                        assert!(task.page_hi - task.page_lo <= 3);
                        pages.extend(
                            (task.page_lo..task.page_hi).map(|page| (task.storage_id, page)),
                        );
                    }
                    pages
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(pages.len(), 150);
    pages.sort_unstable();
    pages.dedup();
    assert_eq!(pages.len(), 150);
}

#[test]
fn parallel_fingerprint_radix_matches_stable_key_order() {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut records = (0..100_000u64)
        .map(|ordinal| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            ((state as u32 as u64) << 32) | ordinal
        })
        .collect::<Vec<_>>();
    let mut expected = records.clone();
    expected.sort_by_key(|record| record >> 32);
    radix_sort_fingerprint_records(&mut records, 4).unwrap();
    assert_eq!(records, expected);
}

#[test]
fn native_cursor_chunks_preserve_partial_selection_and_reservation_ownership() {
    use crate::engine::api::{ResultChunk, ResultCursor};
    let budget = Arc::new(mem_budget::MemBudget::new(1024 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        let schema = Arc::new(BatchSchema::new(vec![test_batch_field(
            ColumnType::LongLong,
        )]));
        let batch = Batch::new(
            schema,
            vec![ColumnVector::from_values(vec![
                Value::Int(9),
                Value::Int(8),
                Value::Int(7),
                Value::Int(6),
            ])],
        )
        .with_selection(Selection::Indices(vec![3, 1]));
        let mut charge = mem_budget::ScopedCharge::new(0);
        charge.add(128).unwrap();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        tx.send(Ok(QueuedScanBatch {
            batch,
            _charge: charge,
        }))
        .unwrap();
        drop(tx);
        let mut cursor = NativePlanBatchCursor {
            rx: Some(rx),
            worker: None,
            cancellation: crate::engine::cancel::CancellationSnapshot::capture().fork_local(),
            budget: budget.clone(),
            current: None,
            current_charge: None,
            current_rows: Vec::new().into_iter(),
            warning_sink: Default::default(),
        };
        assert_eq!(cursor.next_row().unwrap(), Some(vec![Value::Int(6)]));
        let Some(ResultChunk::Columnar(owned)) = cursor.next_chunk().unwrap() else {
            panic!("expected a batch")
        };
        assert_eq!(
            owned
                .batch
                .visible_indices()
                .map(|row| owned.batch.row(row))
                .collect::<Vec<_>>(),
            vec![vec![Value::Int(8)]]
        );
        assert!(cursor.next_row().unwrap().is_none());
        assert!(cursor.next_chunk().unwrap().is_none());
        drop(cursor);
        assert_eq!(
            budget.used(),
            128,
            "batch must retain its producer reservation"
        );
        drop(owned);
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn byte_chunked_decimal_inputs_keep_the_numeric_batch_kernel() {
    use crate::engine::executor::vec::scheduler::hash_agg_driver::HashAggBatchTransform;
    let field = |name: &str| BatchField {
        table: String::new(),
        alias: None,
        name: name.into(),
        column_type: ColumnType::Decimal {
            precision: 10,
            scale: 2,
        },
        source: Default::default(),
    };
    let schema = Arc::new(BatchSchema::new(vec![field("a"), field("b")]));
    let expr = crate::parser::parse_expr_fragment("a * b").unwrap();
    let program = expr_batch::compile_for_batch(&expr, &schema)
        .unwrap()
        .unwrap();
    let programs = vec![program; 3];
    assert!(crate::engine::executor::vec::evaluated_batch::needs_chunks(
        &programs,
        DEFAULT_BATCH_ROWS
    ));
    let numeric =
        ComputedNumericProgram::Arithmetic(super::super::plan::CompiledNumericExpr::Binary {
            left: Box::new(super::super::plan::CompiledNumericExpr::Column(0)),
            op: BinaryOp::Mul,
            right: Box::new(super::super::plan::CompiledNumericExpr::Column(1)),
        });
    let mut fields = schema.fields().to_vec();
    fields.extend([field("x"), field("y"), field("z")]);
    let transform = ComputeColumnsWorkerTransform {
        programs,
        numeric_programs: vec![Some(numeric); 3],
        schema: Arc::new(BatchSchema::new(fields)),
        row_major: true,
    };
    let budget = Arc::new(mem_budget::MemBudget::new(8 * 1024 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        let input = Batch::new(
            schema,
            vec![
                ColumnVector::Decimal64 {
                    data: vec![100; DEFAULT_BATCH_ROWS],
                    scale: 2,
                    nulls: None,
                },
                ColumnVector::Decimal64 {
                    data: vec![50; DEFAULT_BATCH_ROWS],
                    scale: 2,
                    nulls: None,
                },
            ],
        );
        let mut rows = 0;
        let mut held = None;
        transform
            .prepare_and_consume(input, &mut |batch| {
                assert!(batch.num_rows() < DEFAULT_BATCH_ROWS);
                for column in &batch.columns()[2..] {
                    let ColumnVector::Decimal128 { data, scale, .. } = column else {
                        panic!("byte chunks must preserve the numeric batch kernel");
                    };
                    assert_eq!(*scale, 4);
                    assert!(data.iter().all(|value| *value == 5000));
                }
                rows += batch.num_rows();
                held = Some(batch);
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, DEFAULT_BATCH_ROWS);
        assert!(budget.used() > 0);
        drop(held);
        assert_eq!(budget.used(), 0);
        assert!(budget.peak_used() < 8 * 1024 * 1024);
    });
}

#[test]
fn bounded_decimal_morsel_retains_input_buffers_without_rechunking() {
    use crate::engine::executor::vec::scheduler::hash_agg_driver::HashAggBatchTransform;
    let field = |name: &str| BatchField {
        table: String::new(),
        alias: None,
        name: name.into(),
        column_type: ColumnType::Decimal {
            precision: 10,
            scale: 2,
        },
        source: Default::default(),
    };
    let schema = Arc::new(BatchSchema::new(vec![field("a"), field("b")]));
    let expr = crate::parser::parse_expr_fragment("a * b").unwrap();
    let program = expr_batch::compile_for_batch(&expr, &schema)
        .unwrap()
        .unwrap();
    let programs = vec![program; 2];
    assert!(crate::engine::executor::vec::evaluated_batch::needs_chunks(
        &programs,
        DEFAULT_BATCH_ROWS
    ));
    let numeric =
        ComputedNumericProgram::Arithmetic(super::super::plan::CompiledNumericExpr::Binary {
            left: Box::new(super::super::plan::CompiledNumericExpr::Column(0)),
            op: BinaryOp::Mul,
            right: Box::new(super::super::plan::CompiledNumericExpr::Column(1)),
        });
    let mut fields = schema.fields().to_vec();
    fields.extend([field("x"), field("y")]);
    let transform = ComputeColumnsWorkerTransform {
        programs,
        numeric_programs: vec![Some(numeric); 2],
        schema: Arc::new(BatchSchema::new(fields)),
        row_major: true,
    };
    let budget = Arc::new(mem_budget::MemBudget::new(16 * 1024 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        let input = Batch::new(
            schema,
            vec![
                ColumnVector::Decimal64 {
                    data: vec![100; DEFAULT_BATCH_ROWS],
                    scale: 2,
                    nulls: None,
                },
                ColumnVector::Decimal64 {
                    data: vec![50; DEFAULT_BATCH_ROWS],
                    scale: 2,
                    nulls: None,
                },
            ],
        );
        let source_ptr = match &input.columns()[0] {
            ColumnVector::Decimal64 { data, .. } => data.as_ptr(),
            _ => unreachable!(),
        };
        let mut rows = 0;
        let mut held = None;
        transform
            .prepare_and_consume(input, &mut |batch| {
                assert_eq!(
                    batch.num_rows(),
                    DEFAULT_BATCH_ROWS,
                    "bounded typed output must not copy and split its input"
                );
                match &batch.columns()[0] {
                    ColumnVector::Decimal64 { data, .. } => assert_eq!(data.as_ptr(), source_ptr),
                    _ => panic!("input lane changed"),
                }
                for column in &batch.columns()[2..] {
                    let ColumnVector::Decimal128 { data, scale, .. } = column else {
                        panic!("byte chunks must preserve the numeric batch kernel");
                    };
                    assert_eq!(*scale, 4);
                    assert!(data.iter().all(|value| *value == 5000));
                }
                rows += batch.num_rows();
                held = Some(batch);
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, DEFAULT_BATCH_ROWS);
        assert!(budget.used() > 0);
        drop(held);
        assert_eq!(budget.used(), 0);
        assert!(budget.peak_used() < 8 * 1024 * 1024);
    });
}

#[test]
fn read_only_projection_batch_precomputes_bounded_columns() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(root.path()).unwrap());
    let fields = vec![
        BatchField {
            table: "p".into(),
            alias: None,
            name: "d".into(),
            column_type: ColumnType::Date,
            source: Default::default(),
        },
        BatchField {
            table: "p".into(),
            alias: None,
            name: "s".into(),
            column_type: ColumnType::VarString,
            source: crate::types::ColumnSource {
                length: Some(128),
                ..Default::default()
            },
        },
    ];
    let schema = Arc::new(BatchSchema::new(fields.clone()));
    let batch = Batch::new(
        schema.clone(),
        vec![
            ColumnVector::Date32 {
                data: vec![0, 1],
                nulls: None,
            },
            ColumnVector::from_values(vec![Value::String("straße".into()), Value::Null]),
        ],
    );
    let programs = ["DATE_FORMAT(d,'%Y-%m-%d')", "UPPER(s)", "ABS(YEAR(d))"]
        .iter()
        .map(|sql| {
            let ast = crate::parser::parse_expr_fragment(sql).unwrap();
            expr_batch::compile_for_batch(&ast, &schema)
                .unwrap()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let output = Arc::new(BatchSchema::new(
        programs
            .iter()
            .enumerate()
            .map(|(i, program)| computed_batch_field(program, "p", format!("o{i}")).unwrap())
            .collect(),
    ));
    let mut projection = QueryProjectionBatchOperator {
        db: &db,
        txn: TxnView::read_only(0, crate::storage::IsolationLevel::ReadCommitted),
        input: Box::new(MaterializedBatchOperator::new(schema, vec![batch])),
        input_template: fields.iter().map(batch_field_template).collect(),
        projection: programs
            .into_iter()
            .map(VecResultProjection::Expr)
            .collect(),
        row_major: true,
        schema: output,
        eval_ctx: super::super::eval::EvalQueryContext::default(),
    };
    let before = crate::engine::expr_program::expr_program_stats().column_eval_rows;
    let result = crate::engine::executor::vec::config::with_vec_mode(
        crate::engine::executor::vec::config::VecMode::On,
        || projection.next_batch(),
    )
    .unwrap()
    .unwrap();
    let rows = (0..result.num_rows())
        .map(|row| {
            result
                .columns()
                .iter()
                .map(|column| column.value_at(row).to_display_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            vec!["1970-01-01", "STRAßE", "1970"],
            vec!["1970-01-02", "NULL", "1970"]
        ]
    );
    assert!(crate::engine::expr_program::expr_program_stats().column_eval_rows - before >= 6);
}
