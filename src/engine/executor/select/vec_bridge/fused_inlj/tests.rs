use super::*;

fn schema() -> Arc<BatchSchema> {
    Arc::new(BatchSchema::new(
        [
            ("i", ColumnType::LongLong),
            ("u", ColumnType::LongLong),
            ("s", ColumnType::VarString),
            ("d", ColumnType::Date),
            (
                "n",
                ColumnType::Decimal {
                    precision: 38,
                    scale: 2,
                },
            ),
            ("b", ColumnType::Blob),
        ]
        .into_iter()
        .map(|(name, column_type)| BatchField {
            table: "t".into(),
            alias: None,
            name: name.into(),
            column_type,
            source: crate::types::ColumnSource {
                flags: if name == "u" {
                    crate::mysql::protocol::UNSIGNED_FLAG
                } else {
                    0
                },
                ..Default::default()
            },
        })
        .collect(),
    ))
}

#[test]
fn fused_probe_chunks_coalesce_sparse_batches_without_losing_values() {
    let schema = schema();
    let mut batches = Vec::new();
    let mut expected = Vec::new();
    for n in 0..100i64 {
        batches.push(Batch::empty(schema.clone()));
        let values = (0..4)
            .map(|r| {
                vec![
                    if n == 0 {
                        Value::Null
                    } else {
                        Value::Int(n * 4 + r)
                    },
                    Value::UInt(u64::MAX - r as u64),
                    if r == 0 {
                        Value::Null
                    } else {
                        Value::String(format!("名字-{n}-{r}"))
                    },
                    Value::Date(20260101 + r as i32),
                    Value::Decimal128(
                        if n % 2 == 0 {
                            123
                        } else {
                            i128::from(i64::MAX) * 100
                        },
                        2,
                    ),
                    Value::Bytes(vec![0, 255, n as u8, r as u8]),
                ]
            })
            .collect::<Vec<_>>();
        let columns = (0..6)
            .map(|c| ColumnVector::from_values(values.iter().map(|r| r[c].clone()).collect()))
            .collect();
        let batch =
            Batch::new(schema.clone(), columns).with_selection(Selection::Indices(vec![3, 0, 3]));
        expected.extend(batch.visible_indices().map(|r| batch.row(r)));
        batches.push(batch);
    }
    let mut input = MaterializedBatchOperator::new(schema, batches);
    let mut cursor = InljBatchCursor::default();
    let budget = Arc::new(mem_budget::MemBudget::new(128 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        let mut actual = Vec::new();
        let mut chunks = 0;
        while let Some((batch, charge)) = cursor.pull(&mut input, 256).unwrap() {
            assert!(charge.charged() <= budget.limit());
            actual.extend(batch.visible_indices().map(|r| batch.row(r)));
            chunks += 1;
        }
        assert_eq!(actual, expected);
        assert!(
            chunks < 10,
            "tiny source morsels must not become separate probes: {chunks}"
        );
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn fused_probe_wide_payload_keeps_shared_source_buffers() {
    let schema = Arc::new(BatchSchema::new(vec![BatchField {
        table: "t".into(),
        alias: None,
        name: "b".into(),
        column_type: ColumnType::Blob,
        source: Default::default(),
    }]));
    let source = Batch::new(
        schema.clone(),
        vec![ColumnVector::from_values(vec![Value::Bytes(vec![7; 8192])])],
    );
    let mut input = MaterializedBatchOperator::new(schema, vec![source.clone()]);
    let mut cursor = InljBatchCursor::default();
    let budget = Arc::new(mem_budget::MemBudget::new(4096));
    mem_budget::with_query_budget(budget.clone(), || {
        let (batch, charge) = cursor.pull(&mut input, 64).unwrap().unwrap();
        assert!(Arc::ptr_eq(&batch.columns, &source.columns));
        assert_eq!(batch.row(0), source.row(0));
        assert_eq!(charge.charged(), 64);
        drop(batch);
        drop(charge);
        assert!(cursor.pull(&mut input, 64).unwrap().is_none());
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn fused_probe_error_releases_partially_coalesced_payload() {
    struct Failing {
        schema: Arc<BatchSchema>,
        first: Option<Batch>,
    }
    impl BatchOperator for Failing {
        fn schema(&self) -> &Arc<BatchSchema> {
            &self.schema
        }
        fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
            if let Some(batch) = self.first.take() {
                return Ok(Some(batch));
            }
            Err(crate::error::catalog::ER_INTERNAL_ERROR.error("source failed"))
        }
    }
    let schema = Arc::new(BatchSchema::new(vec![BatchField {
        table: "t".into(),
        alias: None,
        name: "i".into(),
        column_type: ColumnType::LongLong,
        source: Default::default(),
    }]));
    let mut input = Failing {
        schema: schema.clone(),
        first: Some(Batch::new(
            schema,
            vec![ColumnVector::I64 {
                data: vec![1],
                nulls: None,
            }],
        )),
    };
    let mut cursor = InljBatchCursor::default();
    let budget = Arc::new(mem_budget::MemBudget::new(64 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        assert!(cursor.pull(&mut input, 64).is_err());
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn global_existence_scan_and_budget_replay_match_index_probes() {
    fn fused(plan: &PhysicalPlan) -> Option<&PhysicalPlan> {
        if super::super::super::plan::inlj_consecutive_semi_anti_left(plan).is_some() {
            return Some(plan);
        }
        plan.children().into_iter().find_map(fused)
    }
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine =
        crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            crate::engine::executor::tests::memory_test_storage_config(),
        )
        .unwrap();
    for sql in [
        "CREATE TABLE probes(id INT PRIMARY KEY,k BIGINT,a BIGINT,b BIGINT,payload TEXT)",
        "CREATE TABLE candidates(id INT PRIMARY KEY,k INT,a BIGINT,b BIGINT,flag INT,INDEX ix(k))",
        "INSERT INTO candidates VALUES(1,1,-9223372036854775808,21,1),(2,1,9223372036854775807,20,0),(3,1,NULL,NULL,1),(4,2,10,20,1),(5,2,10,20,1),(6,NULL,11,21,1)",
    ] {engine.execute(sql).unwrap();}
    for id in 0..12 {
        engine
            .execute(&format!(
                "INSERT INTO probes VALUES({id},{},{},{},'{}')",
                if id % 4 == 3 {
                    "NULL".into()
                } else {
                    (id % 3 + 1).to_string()
                },
                if id % 5 == 0 { "NULL" } else { "10" },
                if id % 7 == 0 { "NULL" } else { "20" },
                "x".repeat(4096)
            ))
            .unwrap();
    }
    engine
        .execute("INSERT INTO probes VALUES(99,4294967297,10,20,'outside INT domain')")
        .unwrap();
    engine.execute("ANALYZE TABLE probes,candidates").unwrap();
    let db = engine.db_for_tests();
    let writer = db.begin_txn().unwrap();
    let right = db.table("test.candidates").unwrap();
    let row = db
        .scan_rows("test.candidates", writer)
        .unwrap()
        .into_iter()
        .find(|row| row.values[0] == Value::Int(1))
        .unwrap();
    db.update_row_by_id(writer, &right, row.row_id, |values| {
        let mut values = values.to_vec();
        values[2] = Value::Int(10);
        values[3] = Value::Int(20);
        values[4] = Value::Int(0);
        Ok(values)
    })
    .unwrap();
    let txn = TxnView::read_only(
        db.visible_scn(),
        crate::storage::IsolationLevel::ReadCommitted,
    );
    for first_not in [false, true] {
        for second_not in [false, true] {
            let sql = format!("SELECT /*+ INDEX_NLJ(o,r) INDEX_NLJ(o,s) */ o.* FROM probes o WHERE {}EXISTS(SELECT 1 FROM candidates r WHERE r.k=o.k AND r.a<>o.a) AND {}EXISTS(SELECT 1 FROM candidates s WHERE s.k=o.k AND s.b<>o.b AND s.flag>0)",
                if first_not {"NOT "} else {""}, if second_not {"NOT "} else {""});
            let crate::parser::Statement::Select(select) =
                crate::parser::parse_statement(&sql).unwrap()
            else {
                panic!()
            };
            let plan = crate::planner::plan_select(db, &select).unwrap().physical;
            let plan = fused(&plan).expect("fixture must have a fused pair");
            let left = super::super::super::plan::inlj_consecutive_semi_anti_left(plan).unwrap();
            let left_template = plan_output_template(left);
            let template = plan_output_template(plan);
            assert_eq!(left_template.len(), template.len());
            let mut expected = super::super::super::plan::execute_plan_rows(db, txn, plan)
                .unwrap()
                .iter()
                .map(|row| {
                    row.fields
                        .iter()
                        .map(|field| field.value_ref().clone())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            expected.sort_by_key(|row| format!("{:?}", row[0]));
            let mut input = try_native_vec_input_stream(db, txn, left).unwrap().unwrap();
            let input_schema = input.stream.schema().clone();
            let mut batches = Vec::new();
            while let Some(batch) = input.stream.next_batch().unwrap() {
                let visible: Vec<_> = batch.visible_indices().map(|row| row as u32).collect();
                for rows in visible.chunks(4) {
                    batches.push(Batch::with_row_count(
                        input_schema.clone(),
                        batch
                            .columns()
                            .iter()
                            .map(|column| column.take_indices(rows))
                            .collect(),
                        rows.len(),
                    ));
                }
            }
            for limit in [64 << 20, 16 << 10] {
                let budget = Arc::new(mem_budget::MemBudget::new(limit));
                mem_budget::with_query_budget(budget.clone(), || {
                    let spec = super::super::super::plan::compile_fused_neq_batch_spec(
                        plan,
                        &left_template,
                    )
                    .unwrap();
                    let mut operator = FusedNeqBatchOperator {
                        plan: Some(plan),
                        db,
                        txn,
                        upstream: Box::new(MaterializedBatchOperator::new(
                            input_schema.clone(),
                            batches.clone(),
                        )),
                        spec,
                        schema: template_to_batch_schema(&template),
                        output_columns: (0..template.len()).collect(),
                        cursor: Default::default(),
                        output_charge: None,
                        global_checked: false,
                        replay: Default::default(),
                        retained_charge: None,
                        retained_area: None,
                        replay_input: None,
                        replay_cursor: Default::default(),
                        global_output: None,
                    };
                    let first = operator.next_batch().unwrap();
                    assert_eq!(operator.global_output.is_some(), limit > (1 << 20));
                    let mut actual = Vec::new();
                    let mut next = first;
                    while let Some(batch) = next {
                        actual.extend(batch.visible_indices().map(|row| batch.row(row)));
                        next = operator.next_batch().unwrap();
                    }
                    actual.sort_by_key(|row| format!("{:?}", row[0]));
                    assert_eq!(
                        actual, expected,
                        "first_not={first_not} second_not={second_not} limit={limit}"
                    );
                    drop(operator);
                });
                assert_eq!(budget.used(), 0);
            }
        }
    }
    db.rollback_txn(writer.txn_id).unwrap();
}
