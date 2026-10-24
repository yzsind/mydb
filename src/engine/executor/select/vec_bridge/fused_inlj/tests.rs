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
