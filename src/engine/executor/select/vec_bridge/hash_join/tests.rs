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
                std::fs::create_dir_all("/tmp/mydb/hash-join-tests").unwrap();
                let dir = tempfile::tempdir_in("/tmp/mydb/hash-join-tests")
                    .unwrap()
                    .keep();
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
