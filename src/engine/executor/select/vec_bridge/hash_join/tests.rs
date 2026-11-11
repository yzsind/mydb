use super::*;
use crate::engine::executor::vec::{batch::ColumnAccumulator, schema::BatchField};
use crate::types::{ColumnType, Value};

impl<'a> HashJoinBatchOperator<'a> {
    /// Exercise the production operator with in-memory child streams.
    pub(crate) fn for_test(
        build: &mut dyn BatchOperator,
        probe: Box<dyn BatchOperator + 'a>,
        build_keys: &[usize],
        probe_keys: &[usize],
        key_collations: &[Option<u16>],
        schema: Arc<BatchSchema>,
        build_is_left: bool,
        join_type: vec_hash_join::VecJoinType,
    ) -> Result<Self, QueryError> {
        let mut operator = Self::for_test_uninitialized(
            build,
            probe,
            build_keys,
            probe_keys,
            key_collations,
            schema,
            build_is_left,
            join_type,
        )?;
        operator.initialize()?;
        Ok(operator)
    }

    fn for_test_uninitialized(
        build: &mut dyn BatchOperator,
        probe: Box<dyn BatchOperator + 'a>,
        build_keys: &[usize],
        probe_keys: &[usize],
        key_collations: &[Option<u16>],
        schema: Arc<BatchSchema>,
        build_is_left: bool,
        join_type: vec_hash_join::VecJoinType,
    ) -> Result<Self, QueryError> {
        static DB: std::sync::OnceLock<Arc<Database>> = std::sync::OnceLock::new();
        let db = DB
            .get_or_init(|| {
                let dir = tempfile::tempdir().unwrap().keep();
                Arc::new(Database::open(dir).unwrap())
            })
            .clone();
        let mut batches = Vec::new();
        while let Some(batch) = build.next_batch()? {
            batches.push(batch);
        }
        let operator = Self {
            db,
            txn: TxnView::read_only(0, crate::storage::IsolationLevel::ReadCommitted),
            build: Box::new(MaterializedBatchOperator::new(
                build.schema().clone(),
                batches,
            )),
            probe_schema: probe.schema().clone(),
            probe,
            build_plan: None,
            build_keys: build_keys.to_vec(),
            probe_keys: probe_keys.to_vec(),
            probe_key_exprs: Vec::new(),
            key_collations: key_collations.to_vec(),
            null_safe: vec![false; build_keys.len()],
            join_type: match join_type {
                vec_hash_join::VecJoinType::Inner => crate::parser::JoinType::Inner,
                vec_hash_join::VecJoinType::Left if build_is_left => crate::parser::JoinType::Right,
                vec_hash_join::VecJoinType::Left => crate::parser::JoinType::Left,
            },
            build_is_left,
            schema,
            prefer_spill_key_locality: false,
            residual: None,
            state: None,
            output: None,
            grace_output: None,
            spill_output_columns: None,
            work_area: None,
            build_memory_charge: None,
            finished: false,
            empty_build_probe_passthrough: false,
            reverse_semi_anti: false,
            pushed_runtime_rejected: None,
            pushed_exact_runtime_rejected: None,
            pushed_runtime_rejected_reported: 0,
        };
        Ok(operator)
    }

    pub(crate) fn set_output_data_dir(&mut self, path: &std::path::Path) {
        if let Some(state) = self.state.as_mut() {
            state.pending.set_data_dir(path);
        }
    }
}

#[test]
fn reverse_existence_join_compacts_retained_capacity_and_charges_distinct_keys() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::parser::JoinType;

    for representation in 0..3 {
        let native = representation != 1;
        for join_type in [JoinType::Semi, JoinType::Anti] {
            let schema = Arc::new(BatchSchema::new(vec![BatchField {
                table: "t".into(),
                alias: None,
                name: "k".into(),
                column_type: if native {
                    ColumnType::LongLong
                } else {
                    ColumnType::VarString
                },
                source: Default::default(),
            }]));
            let key = |n| {
                if native {
                    Value::Int(n)
                } else {
                    Value::String(format!("key-{n}"))
                }
            };
            let mut batches = Vec::new();
            for n in 0..40 {
                let mut accumulator = ColumnAccumulator::for_field(&schema.fields()[0], 65536);
                for value in [key(99), key(1), Value::Null, key(2)] {
                    accumulator.push(&value);
                }
                let column = if representation == 2 && n == 20 {
                    let mut values = Vec::with_capacity(65536);
                    values.extend([key(99), key(1), Value::Null, key(2)]);
                    ColumnVector::Generic { values }
                } else {
                    accumulator.finish()
                };
                let batch = Batch::new(schema.clone(), vec![column]);
                let batch = if n % 2 == 0 {
                    batch
                } else {
                    batch.with_selection(Selection::Indices(vec![3, 2, 1]))
                };
                assert!(batch.memory_bytes() > 100_000);
                batches.push(batch);
            }
            let mut build_column = ColumnAccumulator::for_field(&schema.fields()[0], 10_000);
            for _ in 0..10_000 {
                build_column.push(&key(1));
            }
            let mut build = MaterializedBatchOperator::new(
                schema.clone(),
                vec![Batch::new(schema.clone(), vec![build_column.finish()])],
            );
            let mut operator = HashJoinBatchOperator::for_test_uninitialized(
                &mut build,
                Box::new(MaterializedBatchOperator::new(schema.clone(), batches)),
                &[0],
                &[0],
                &[],
                schema,
                false,
                vec_hash_join::VecJoinType::Inner,
            )
            .unwrap();
            operator.join_type = join_type;
            operator.reverse_semi_anti = true;
            let budget = Arc::new(MemBudget::new(64 * 1024));
            with_query_budget(budget.clone(), || {
                assert!(operator.try_initialize_reverse_semi_anti().unwrap());
                assert!(operator.build_memory_charge.as_ref().unwrap().charged() < budget.limit());
                let mut actual = Vec::new();
                while let Some(batch) = operator.next_batch().unwrap() {
                    actual.extend(
                        batch
                            .visible_indices()
                            .map(|row| batch.columns()[0].value_at(row)),
                    );
                }
                let expected = if join_type == JoinType::Semi {
                    vec![key(1); 40]
                } else {
                    (0..40)
                        .flat_map(|n| {
                            if n % 2 == 0 {
                                vec![key(99), Value::Null, key(2)]
                            } else {
                                vec![key(2), Value::Null]
                            }
                        })
                        .collect()
                };
                assert_eq!(actual, expected);
                drop(operator);
                assert_eq!(budget.used(), 0);
            });
        }
    }
}

#[test]
fn reverse_existence_join_admission_replays_prefix_and_unconsumed_remainder() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct CountedProbe {
        inner: MaterializedBatchOperator,
        pulls: Arc<AtomicUsize>,
    }
    impl BatchOperator for CountedProbe {
        fn schema(&self) -> &Arc<BatchSchema> {
            self.inner.schema()
        }
        fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
            self.pulls.fetch_add(1, Ordering::Relaxed);
            self.inner.next_batch()
        }
    }
    let schema = Arc::new(BatchSchema::new(vec![BatchField {
        table: "t".into(),
        alias: None,
        name: "k".into(),
        column_type: ColumnType::LongLong,
        source: Default::default(),
    }]));
    let batches = (0..10)
        .map(|n| {
            Batch::new(
                schema.clone(),
                vec![ColumnVector::I64 {
                    data: vec![n, -1],
                    nulls: None,
                }],
            )
            .with_selection(Selection::Indices(vec![0]))
        })
        .collect();
    let mut build = MaterializedBatchOperator::new(schema.clone(), Vec::new());
    let pulls = Arc::new(AtomicUsize::new(0));
    let mut operator = HashJoinBatchOperator::for_test_uninitialized(
        &mut build,
        Box::new(CountedProbe {
            inner: MaterializedBatchOperator::new(schema.clone(), batches),
            pulls: pulls.clone(),
        }),
        &[0],
        &[0],
        &[],
        schema,
        false,
        vec_hash_join::VecJoinType::Inner,
    )
    .unwrap();
    operator.join_type = crate::parser::JoinType::Anti;
    operator.reverse_semi_anti = true;
    let budget = Arc::new(MemBudget::new(1));
    with_query_budget(budget.clone(), || {
        operator.initialize().unwrap();
        assert!(operator.empty_build_probe_passthrough);
        assert!(
            budget.used() > 0,
            "the replay source must retain the prefix charge for an empty ANTI build"
        );
        assert_eq!(
            pulls.load(Ordering::Relaxed),
            1,
            "admission must stop retaining input at the crossing batch"
        );
        let mut actual = Vec::new();
        while let Some(batch) = operator.next_batch().unwrap() {
            actual.extend(
                batch
                    .visible_indices()
                    .map(|row| batch.columns()[0].value_at(row)),
            );
        }
        assert_eq!(actual, (0..10).map(Value::Int).collect::<Vec<_>>());
        assert_eq!(
            budget.used(),
            0,
            "consuming the prefix must release its reservation"
        );
        drop(operator);
        assert_eq!(budget.used(), 0);
    });
}

impl HashJoinBatchOperator<'_> {
    pub(crate) fn test_payload(&self) -> Option<&vec_hash_join::BuildPayload> {
        self.state.as_ref().map(|state| &state.payload)
    }
}

#[test]
fn residual_existence_domain_reduction_preserves_each_probe_row() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::parser::{BinaryOp, JoinType};

    let schema = |table: &str, names: &[&str]| {
        Arc::new(BatchSchema::new(
            names
                .iter()
                .map(|name| BatchField {
                    table: table.into(),
                    alias: None,
                    name: (*name).into(),
                    column_type: ColumnType::LongLong,
                    source: Default::default(),
                })
                .collect(),
        ))
    };
    let probe_schema = schema("p", &["k", "v", "id"]);
    let build_schema = schema("b", &["k", "v"]);
    let pair_schema = Arc::new(BatchSchema::new(
        probe_schema
            .fields()
            .iter()
            .chain(build_schema.fields())
            .cloned()
            .collect(),
    ));
    let probe = Batch::new(
        probe_schema.clone(),
        vec![
            ColumnVector::from_values(vec![
                Value::Int(7),
                Value::Int(7),
                Value::Int(8),
                Value::Int(9),
                Value::Null,
            ]),
            ColumnVector::from_values(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Null,
                Value::Int(3),
                Value::Int(4),
            ]),
            ColumnVector::from_values((1..=5).map(Value::Int).collect()),
        ],
    );
    let predicate = Expr::Binary {
        left: Box::new(Expr::CompoundIdentifier(vec!["b".into(), "v".into()])),
        op: BinaryOp::NotEq,
        right: Box::new(Expr::CompoundIdentifier(vec!["p".into(), "v".into()])),
    };
    for (null_safe, late_match) in [(false, false), (true, false), (false, true), (true, true)] {
        for join_type in [JoinType::Semi, JoinType::Anti] {
            let mut build_keys = vec![Value::Int(99); 1000];
            let mut build_values = vec![Value::Int(0); 1000];
            build_keys.extend([
                Value::Int(7),
                Value::Int(7),
                Value::Int(8),
                Value::Int(9),
                Value::Null,
            ]);
            build_values.extend([
                Value::Int(1),
                Value::Int(1),
                Value::Null,
                Value::Int(3),
                Value::Int(0),
            ]);
            if late_match {
                build_keys.extend(std::iter::repeat_n(Value::Int(7), 65));
                build_values.extend(std::iter::repeat_n(Value::Int(1), 65));
                build_keys.push(Value::Int(7));
                build_values.push(Value::Int(2));
            }
            let mut build = MaterializedBatchOperator::new(
                build_schema.clone(),
                vec![Batch::new(
                    build_schema.clone(),
                    vec![
                        ColumnVector::from_values(build_keys),
                        ColumnVector::from_values(build_values),
                    ],
                )],
            );
            let mut operator = HashJoinBatchOperator::for_test_uninitialized(
                &mut build,
                Box::new(MaterializedBatchOperator::new(
                    probe_schema.clone(),
                    vec![probe.clone()],
                )),
                &[0],
                &[0],
                &[],
                pair_schema.clone(),
                false,
                vec_hash_join::VecJoinType::Inner,
            )
            .unwrap();
            operator.join_type = join_type;
            operator.null_safe = vec![null_safe];
            operator.reverse_semi_anti = true;
            operator.residual = Some((
                expr_batch::BatchExprProgram::compile(&predicate, &pair_schema)
                    .unwrap()
                    .unwrap(),
                pair_schema.clone(),
            ));
            let budget = Arc::new(MemBudget::new(1024 * 1024));
            with_query_budget(budget.clone(), || {
                operator.initialize().unwrap();
                assert_eq!(
                    operator.test_payload().unwrap().columns[0].len(),
                    (if null_safe { 5 } else { 4 }) + if late_match { 66 } else { 0 }
                );
                let mut actual = Vec::new();
                while let Some(batch) = operator.next_batch().unwrap() {
                    actual.extend(
                        batch
                            .visible_indices()
                            .map(|row| batch.columns()[2].value_at(row)),
                    );
                }
                let mut matched = if late_match { vec![1, 2] } else { vec![2] };
                if null_safe {
                    matched.push(5);
                }
                let expected: Vec<_> = (1..=5)
                    .filter(|id| matched.contains(id) == (join_type == JoinType::Semi))
                    .map(Value::Int)
                    .collect();
                assert_eq!(actual, expected);
                drop(operator);
                assert_eq!(budget.used(), 0);
            });
        }
    }
}

#[test]
fn build_domain_compacts_selected_rows_and_releases_budget_on_refusal() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let schema = Arc::new(BatchSchema::new(vec![BatchField {
        table: "t".into(),
        alias: None,
        name: "k".into(),
        column_type: ColumnType::LongLong,
        source: Default::default(),
    }]));
    let probe = Batch::new(
        schema.clone(),
        vec![ColumnVector::from_values(vec![Value::Int(2)])],
    );
    let domain =
        build_domain::BuildDomain::from_probe(&[probe.clone()], &[0], &[false], &[], true, 1)
            .unwrap();
    let input = Batch::new(
        schema.clone(),
        vec![ColumnVector::from_values(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(2),
            Value::Null,
        ])],
    )
    .with_selection(Selection::Indices(vec![1, 3]));
    let mut stream = build_domain::BuildDomainStream::new(
        Box::new(MaterializedBatchOperator::new(schema, vec![input])),
        domain,
        false,
        vec![0],
        vec![false],
        Vec::new(),
    );
    let output = stream.next_batch().unwrap().unwrap();
    assert_eq!(output.num_rows(), 1);
    assert!(output.selection().is_none());
    assert_eq!(output.columns()[0].value_at(0), Value::Int(2));
    assert!(stream.next_batch().unwrap().is_none());
    let budget = Arc::new(MemBudget::new(1));
    with_query_budget(budget.clone(), || {
        assert!(
            build_domain::BuildDomain::from_probe(&[probe], &[0], &[true], &[], false, 1).is_none()
        );
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn dense_preserved_count_matches_outer_join_for_holes_nulls_duplicates_and_extremes() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::engine::executor::vec::hash_agg::{AggregateKind, AggregateSpec, HashAggregateCore};
    struct UnopenedProbe(Arc<BatchSchema>);
    impl BatchOperator for UnopenedProbe {
        fn schema(&self) -> &Arc<BatchSchema> {
            &self.0
        }
        fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
            panic!("an empty preserved build must not open its probe");
        }
    }
    for (keys, dense) in [
        (vec![Value::Int(-3), Value::Int(0), Value::Int(2)], true),
        (vec![Value::Int(-3), Value::Int(-3), Value::Int(2)], false),
        (vec![Value::Int(-3), Value::Null, Value::Int(2)], false),
        (vec![Value::Int(i64::MIN), Value::Int(i64::MAX)], false),
        (vec![Value::Int(i64::MAX - 2), Value::Int(i64::MAX)], true),
        (Vec::new(), true),
    ] {
        for (build_left, budget_bytes) in [
            (true, 4 * 1024 * 1024),
            (false, 4 * 1024 * 1024),
            (true, 64 * 1024),
            (false, 64 * 1024),
        ] {
            let dense = dense && (budget_bytes > 64 * 1024 || keys.is_empty());
            let field = |table: &str, name: &str| BatchField {
                table: table.into(),
                alias: None,
                name: name.into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            };
            let build_schema = Arc::new(BatchSchema::new(vec![field("b", "k")]));
            let probe_schema = Arc::new(BatchSchema::new(vec![field("p", "k"), field("p", "v")]));
            let mut fields = build_schema.fields().to_vec();
            if build_left {
                fields.extend_from_slice(probe_schema.fields());
            } else {
                fields.splice(0..0, probe_schema.fields().iter().cloned());
            }
            let schema = Arc::new(BatchSchema::new(fields));
            let probe_keys = vec![
                Value::Int(-3),
                Value::Int(-3),
                Value::Int(0),
                Value::Int(1),
                Value::Null,
                Value::Int(i64::MAX),
            ];
            let values = vec![
                Value::Int(4),
                Value::Null,
                Value::Null,
                Value::Int(9),
                Value::Int(8),
                Value::Int(5),
            ];
            let make_column = |schema: &BatchSchema, column: usize, values: &[Value]| {
                let mut accumulator =
                    ColumnAccumulator::for_field(&schema.fields()[column], values.len());
                for value in values {
                    accumulator.push(value);
                }
                accumulator.finish()
            };
            let mut build = MaterializedBatchOperator::new(
                build_schema.clone(),
                keys.chunks(2)
                    .map(|keys| {
                        let mut column = make_column(&build_schema, 0, keys);
                        if let ColumnVector::I64 { data, .. } = &mut column {
                            // A scan tail can own much more capacity than its
                            // visible keys. Admission must retain compact data.
                            data.reserve(1024 * 1024);
                        }
                        Batch::new(build_schema.clone(), vec![column])
                    })
                    .collect(),
            );
            let probe = MaterializedBatchOperator::new(
                probe_schema.clone(),
                vec![Batch::new(
                    probe_schema.clone(),
                    vec![
                        if build_left {
                            make_column(&probe_schema, 0, &probe_keys)
                        } else {
                            ColumnVector::Generic {
                                values: probe_keys.clone(),
                            }
                        },
                        make_column(&probe_schema, 1, &values),
                    ],
                )],
            );
            let probe: Box<dyn BatchOperator> = if keys.is_empty() {
                Box::new(UnopenedProbe(probe_schema))
            } else {
                Box::new(probe)
            };
            let mut operator = HashJoinBatchOperator::for_test_uninitialized(
                &mut build,
                probe,
                &[0],
                &[0],
                &[],
                schema.clone(),
                build_left,
                vec_hash_join::VecJoinType::Inner,
            )
            .unwrap();
            operator.join_type = if build_left {
                crate::parser::JoinType::Left
            } else {
                crate::parser::JoinType::Right
            };
            let group = if build_left { 0 } else { 2 };
            let value = if build_left { 2 } else { 1 };
            let specs = vec![
                AggregateSpec {
                    func: AggregateKind::Count,
                    input_column: 1,
                    comparison_collation: None,
                },
                AggregateSpec {
                    func: AggregateKind::CountStar,
                    input_column: 0,
                    comparison_collation: None,
                },
                AggregateSpec {
                    func: AggregateKind::Count,
                    input_column: 0,
                    comparison_collation: None,
                },
            ];
            let budget = Arc::new(MemBudget::new(budget_bytes));
            with_query_budget(budget.clone(), || {
                let mut projected_key = schema.fields()[group].clone();
                projected_key.name = "projected_key".into();
                let projected_schema = Arc::new(BatchSchema::new(vec![
                    projected_key,
                    schema.fields()[value].clone(),
                ]));
                let mut aggregate = HashAggregateCore::new(vec![0], specs);
                aggregate.set_input_schema(projected_schema);
                let saved = aggregate
                    .remap_direct_input_columns(&[group, value])
                    .unwrap();
                aggregate.set_input_schema(schema);
                assert_eq!(
                    operator
                        .try_dense_build_count(
                            &mut aggregate,
                            &[(group, 0)],
                            &[
                                Some(vec_hash_agg::DirectJoinColumn::Probe(1)),
                                None,
                                Some(vec_hash_agg::DirectJoinColumn::Build(0))
                            ]
                        )
                        .unwrap(),
                    dense,
                    "keys={keys:?} build_left={build_left}"
                );
                if !dense {
                    aggregate.consume_stream(&mut operator).unwrap();
                }
                aggregate.restore_direct_input_columns(saved);
                let mut output = aggregate.finalize_stream().unwrap();
                assert_eq!(output.schema().fields()[0].name, "projected_key");
                let mut actual = Vec::new();
                while let Some(batch) = output.next_batch().unwrap() {
                    for row in batch.visible_indices() {
                        actual.push(
                            batch
                                .columns()
                                .iter()
                                .map(|column| column.value_at(row))
                                .collect::<Vec<_>>(),
                        );
                    }
                }
                let mut expected = std::collections::BTreeMap::<Option<i64>, [i64; 3]>::new();
                for key in &keys {
                    let group = match key {
                        Value::Int(value) => Some(*value),
                        _ => None,
                    };
                    let counts = expected.entry(group).or_default();
                    let matches = probe_keys
                        .iter()
                        .zip(&values)
                        .filter(|(probe, _)| group.is_some() && *probe == key)
                        .collect::<Vec<_>>();
                    if matches.is_empty() {
                        counts[1] += 1;
                        counts[2] += i64::from(group.is_some());
                    } else {
                        for (_, value) in matches {
                            counts[0] += i64::from(*value != Value::Null);
                            counts[1] += 1;
                            counts[2] += 1;
                        }
                    }
                }
                let mut expected = expected
                    .into_iter()
                    .map(|(key, counts)| {
                        std::iter::once(key.map_or(Value::Null, Value::Int))
                            .chain(counts.into_iter().map(Value::Int))
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                actual.sort_by_key(|row| format!("{row:?}"));
                expected.sort_by_key(|row| format!("{row:?}"));
                assert_eq!(actual, expected, "keys={keys:?} left={build_left}");
                drop(output);
                drop(operator);
                assert_eq!(budget.used(), 0);
            });
        }
    }
}

#[test]
fn packed_composite_join_matches_duplicates_and_outer_null_extension() {
    let schema = Arc::new(BatchSchema::new(
        (0..3)
            .map(|i| BatchField {
                table: "t".into(),
                alias: None,
                name: format!("k{i}"),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            })
            .collect(),
    ));
    let make = |rows: &[[Option<i64>; 3]]| {
        Batch::new(
            schema.clone(),
            (0..3)
                .map(|c| {
                    ColumnVector::from_values(
                        rows.iter()
                            .map(|r| r[c].map(Value::Int).unwrap_or(Value::Null))
                            .collect(),
                    )
                })
                .collect(),
        )
    };
    let mut build_rows: Vec<_> = (0..3000)
        .map(|i| [Some(-5), Some(i % 997), Some(i % 17)])
        .collect();
    build_rows[2999] = build_rows[0];
    let probes = [
        [Some(-5), Some(0), Some(0)],
        [Some(-5), Some(21), Some(4)],
        [Some(-4), Some(0), Some(0)],
        [Some(-5), None, Some(0)],
    ];
    let output_schema = Arc::new(BatchSchema::new(
        schema
            .fields()
            .iter()
            .chain(schema.fields())
            .cloned()
            .collect(),
    ));
    for outer in [false, true] {
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
            16 * 1024 * 1024,
        ));
        crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
            let mut build = MaterializedBatchOperator::new(schema.clone(), vec![make(&build_rows)]);
            let probe = MaterializedBatchOperator::new(schema.clone(), vec![make(&probes)]);
            let mut join = HashJoinBatchOperator::for_test(
                &mut build,
                Box::new(probe),
                &[0, 1, 2],
                &[0, 1, 2],
                &[],
                output_schema.clone(),
                false,
                if outer {
                    vec_hash_join::VecJoinType::Left
                } else {
                    vec_hash_join::VecJoinType::Inner
                },
            )
            .unwrap();
            assert!(join.state.as_ref().unwrap().payload.packed_index.is_some());
            let mut actual = Vec::new();
            while let Some(batch) = join.next_batch().unwrap() {
                actual.extend(batch.visible_indices().map(|r| batch.row(r)));
            }
            let mut expected = Vec::new();
            for probe in probes {
                let matches: Vec<_> = build_rows
                    .iter()
                    .filter(|build| **build == probe && probe.iter().all(Option::is_some))
                    .collect();
                if matches.is_empty() && outer {
                    expected.push(
                        probe
                            .iter()
                            .map(|v| v.map(Value::Int).unwrap_or(Value::Null))
                            .chain([Value::Null, Value::Null, Value::Null])
                            .collect::<Vec<_>>(),
                    );
                } else {
                    expected.extend(matches.into_iter().map(|build| {
                        probe
                            .iter()
                            .chain(build)
                            .map(|v| v.map(Value::Int).unwrap_or(Value::Null))
                            .collect::<Vec<_>>()
                    }));
                }
            }
            assert_eq!(actual, expected);
        });
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn packed_two_key_joins_preserve_duplicates_nulls_and_payloads() {
    let schema = Arc::new(BatchSchema::new(
        (0..2)
            .map(|i| BatchField {
                table: "t".into(),
                alias: None,
                name: format!("k{i}"),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            })
            .collect(),
    ));
    let make = |rows: &[[Option<i64>; 2]]| {
        Batch::new(
            schema.clone(),
            (0..2)
                .map(|c| {
                    ColumnVector::from_values(
                        rows.iter()
                            .map(|r| r[c].map(Value::Int).unwrap_or(Value::Null))
                            .collect(),
                    )
                })
                .collect(),
        )
    };
    let mut rows = (0..3000)
        .map(|i| [Some(i % 997), Some((i % 17) * 10000)])
        .collect::<Vec<_>>();
    rows.extend([[Some(0), Some(0)], [None, Some(0)], [Some(0), None]]);
    let probes = [
        [Some(0), Some(0)],
        [Some(0), Some(0)],
        [Some(21), Some(40000)],
        [Some(-1), Some(0)],
        [None, Some(0)],
        [Some(0), None],
    ];
    for join_type in [
        crate::parser::JoinType::Semi,
        crate::parser::JoinType::Anti,
        crate::parser::JoinType::Inner,
        crate::parser::JoinType::Left,
    ] {
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
            16 * 1024 * 1024,
        ));
        crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
            let mut build = MaterializedBatchOperator::new(schema.clone(), vec![make(&rows)]);
            let probe = MaterializedBatchOperator::new(schema.clone(), vec![make(&probes)]);
            let mut join = HashJoinBatchOperator::for_test_uninitialized(
                &mut build,
                Box::new(probe),
                &[0, 1],
                &[0, 1],
                &[],
                Arc::new(BatchSchema::new(
                    schema
                        .fields()
                        .iter()
                        .chain(schema.fields())
                        .cloned()
                        .collect(),
                )),
                false,
                vec_hash_join::VecJoinType::Inner,
            )
            .unwrap();
            join.join_type = join_type;
            join.initialize().unwrap();
            assert!(join.state.as_ref().unwrap().payload.packed_index.is_some());
            let mut actual = Vec::new();
            while let Some(batch) = join.next_batch().unwrap() {
                actual.extend(batch.visible_indices().map(|r| batch.row(r)));
            }
            let mut expected = Vec::new();
            for probe in &probes {
                let hits = rows
                    .iter()
                    .filter(|build| probe.iter().all(Option::is_some) && *build == probe)
                    .collect::<Vec<_>>();
                let left = probe
                    .iter()
                    .map(|v| v.map(Value::Int).unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                match join_type {
                    crate::parser::JoinType::Semi if !hits.is_empty() => expected.push(left),
                    crate::parser::JoinType::Anti if hits.is_empty() => expected.push(left),
                    crate::parser::JoinType::Inner | crate::parser::JoinType::Left => {
                        if hits.is_empty() && join_type == crate::parser::JoinType::Left {
                            expected
                                .push(left.into_iter().chain([Value::Null, Value::Null]).collect());
                        } else {
                            for build in hits {
                                expected.push(
                                    left.iter()
                                        .cloned()
                                        .chain(
                                            build
                                                .iter()
                                                .map(|v| v.map(Value::Int).unwrap_or(Value::Null)),
                                        )
                                        .collect(),
                                );
                            }
                        }
                    }
                    _ => {}
                }
            }
            assert_eq!(actual, expected);
        });
        assert_eq!(budget.used(), 0);
    }
}
