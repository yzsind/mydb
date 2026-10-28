//! Exercise the row aggregate entry points directly; SQL routing may otherwise
//! select a native batch operator and leave the row oracle untested.
use super::*;
use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
use crate::storage::IsolationLevel;

fn fixture(n: usize) -> (Vec<PlanRow>, PhysicalPlan) {
    let columns = [
        ("g", ColumnType::LongLong),
        ("k", ColumnType::VarString),
        (
            "v",
            ColumnType::Decimal {
                precision: 18,
                scale: 2,
            },
        ),
        ("id", ColumnType::LongLong),
    ];
    let values: Vec<Vec<Value>> = (0..n)
        .map(|i| {
            vec![
                if i % 7 == 0 {
                    Value::Null
                } else {
                    Value::Int((i % 3) as i64)
                },
                Value::String(format!("key{}", i % 2)),
                if i % 5 == 0 {
                    Value::Null
                } else {
                    Value::DecimalCompact((i % 11) as i64 * 25, 2)
                },
                Value::Int(i as i64),
            ]
        })
        .collect();
    let rows = values
        .iter()
        .map(|values| {
            PlanRow::from_fields(
                columns
                    .iter()
                    .zip(values)
                    .map(|((name, ty), value)| PlanField::new("t", None, *name, *ty, value.clone()))
                    .collect(),
            )
        })
        .collect();
    let plan = PhysicalPlan::ValuesScan {
        rows: values
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| {
                        Expr::Literal(match v {
                            Value::Null => crate::parser::Literal::Null,
                            Value::Int(v) => crate::parser::Literal::Int(v),
                            Value::String(v) => crate::parser::Literal::String(v),
                            v => crate::parser::Literal::Decimal(v.to_display_string()),
                        })
                    })
                    .collect()
            })
            .collect(),
        columns: columns
            .iter()
            .map(|(name, ty)| Column {
                name: (*name).into(),
                column_type: *ty,
                source: crate::types::ColumnSource {
                    table: "t".into(),
                    ..Default::default()
                },
            })
            .collect(),
        filter: None,
        estimated_rows: n as f64,
        cost: crate::engine::cbo::Cost::scalar(1.0),
    };
    (rows, plan)
}

fn spec(sql: &str) -> (Select, Vec<super::super::aggregate::AggregateSlot>) {
    let Statement::Select(select) = crate::parser::parse_statement(sql).unwrap() else {
        panic!("select")
    };
    let mut exprs: Vec<&Expr> = select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expr { expr, .. } => Some(expr),
            _ => None,
        })
        .collect();
    exprs.extend(select.having.iter());
    let slots = super::super::aggregate::collect_aggregate_slots(&exprs);
    (select, slots)
}

fn output(rows: &[PlanRow], slots: &[super::super::aggregate::AggregateSlot]) -> Vec<Vec<String>> {
    let mut values: Vec<_> =
        rows.iter()
            .map(|row| {
                row.fields
                    .iter()
                    .filter(|f| f.table == "$group")
                    .map(|f| f.value.to_display_string())
                    .chain(slots.iter().map(|slot| {
                        row.agg_values.as_ref().unwrap()[&slot.key].to_display_string()
                    }))
                    .collect::<Vec<_>>()
            })
            .collect();
    values.sort();
    values
}

#[test]
fn sqlfix_minmax_row_and_row_spill_use_declared_collation() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    let collation = crate::charset::resolve_collation("utf8mb4_bin").unwrap().id;
    let rows = ["Z", "a"]
        .into_iter()
        .map(|value| {
            PlanRow::from_fields(vec![PlanField {
                meta: intern_col_meta(ColMeta {
                    invisible: false,
                    table: "t".into(),
                    alias: None,
                    column: "s".into(),
                    column_type: ColumnType::VarString,
                    source: crate::types::ColumnSource {
                        collation_id: Some(collation),
                        ..Default::default()
                    },
                }),
                value: Value::String(value.into()),
            }])
        })
        .collect::<Vec<_>>();
    let (select, slots) = spec("SELECT MIN(s), MAX(s) FROM t");
    let resident = aggregate_plan_rows(rows.clone(), &select.group_by, None, &slots).unwrap();
    let spilled = aggregate_plan_rows_spilled(&db, rows, &select.group_by, None, &slots).unwrap();
    assert_eq!(output(&resident, &slots), vec![vec!["Z", "a"]]);
    assert_eq!(output(&spilled, &slots), vec![vec!["Z", "a"]]);
}

#[test]
fn row_spill_reports_missing_occupied_partition() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let tmp = SpillTmpDir::create_kind(dir.path(), "missing-rowagg").unwrap();
    let (rows, _) = fixture(1);
    let schema = Arc::new(rows[0].fields.iter().cloned().collect::<Vec<_>>());
    let (_, slots) = spec("SELECT COUNT(*) FROM t");
    // The producer claims this bucket was completed. Losing its file must
    // return an error, rather than silently treating it as an empty bucket.
    assert!(merge_row_hash_agg_spill_partitions(&tmp, &[7], &schema, &[], &slots, None).is_err());
}

#[test]
fn row_spill_does_not_open_unused_partitions_for_one_group() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = Arc::new(
        Database::open_with_config(
            dir.path(),
            crate::engine::executor::tests::memory_test_storage_config(),
        )
        .unwrap(),
    );
    let (rows, plan) = fixture(1025);
    let (select, slots) = spec("SELECT COUNT(*), SUM(v), VAR_POP(id) FROM t");
    let expected = aggregate_plan_rows(rows.clone(), &select.group_by, None, &slots).unwrap();
    let budget = Arc::new(MemBudget::new(64 * 1024 * 1024));
    for streaming in [false, true] {
        let before = crate::engine::executor::spill_quota::spill_file_opens();
        let actual = with_query_budget(budget.clone(), || {
            if streaming {
                aggregate_plan_stream_spilled(
                    &db,
                    TxnView::read_only(0, IsolationLevel::RepeatableRead),
                    &plan,
                    &select.group_by,
                    None,
                    &slots,
                )
            } else {
                aggregate_plan_rows_spilled(&db, rows.clone(), &select.group_by, None, &slots)
            }
        })
        .unwrap();
        assert_eq!(output(&actual, &slots), output(&expected, &slots));
        assert_eq!(budget.used(), 0);
        let opens = crate::engine::executor::spill_quota::spill_file_opens() - before;
        assert!(
            opens <= 4,
            "single occupied group opened {opens} spill files; streaming={streaming}"
        );
    }
}

#[test]
fn row_aggregate_entry_points_preserve_group_and_accumulator_semantics() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    let txn = TxnView::read_only(0, IsolationLevel::RepeatableRead);
    for n in [0, 1, 63, 64, 1023, 1024, 1025] {
        let (rows, plan) = fixture(n);
        for group in ["", " GROUP BY g", " GROUP BY g, k"] {
            for funcs in [
                "COUNT(*), COUNT(v), SUM(v), AVG(v), MIN(v), MAX(v)",
                "COUNT(DISTINCT v), SUM(DISTINCT v), AVG(DISTINCT v)",
                "GROUP_CONCAT(k ORDER BY id DESC), BIT_XOR(id), VAR_POP(id)",
            ] {
                let sql = format!("SELECT {funcs} FROM t{group}");
                let (select, slots) = spec(&sql);
                let memory =
                    aggregate_plan_rows(rows.clone(), &select.group_by, None, &slots).unwrap();
                let stream = aggregate_plan_stream_in_memory(
                    &db,
                    txn,
                    &plan,
                    &select.group_by,
                    None,
                    &slots,
                )
                .unwrap();
                let spill =
                    aggregate_plan_rows_spilled(&db, rows.clone(), &select.group_by, None, &slots)
                        .unwrap();
                let expected = output(&memory, &slots);
                assert_eq!(expected, output(&stream, &slots), "stream n={n}: {sql}");
                assert_eq!(expected, output(&spill, &slots), "spill n={n}: {sql}");
                assert_eq!(memory.is_empty(), n == 0 && !group.is_empty(), "{sql}");
                if funcs.starts_with("COUNT(*)") && group.is_empty() {
                    assert_eq!(
                        memory[0].agg_values.as_ref().unwrap()[&slots[0].key],
                        Value::Int(n as i64)
                    );
                }
            }
        }
    }
}

#[test]
fn row_aggregate_having_and_budget_exit_contracts() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    let txn = TxnView::read_only(0, IsolationLevel::RepeatableRead);
    let (select, slots) = spec("SELECT COUNT(*), SUM(v) FROM t GROUP BY g HAVING COUNT(*) > 19");
    let (rows, plan) = fixture(70);
    let expected = aggregate_plan_rows(
        rows.clone(),
        &select.group_by,
        select.having.as_ref(),
        &slots,
    )
    .unwrap();
    let actual = aggregate_plan_stream_in_memory(
        &db,
        txn,
        &plan,
        &select.group_by,
        select.having.as_ref(),
        &slots,
    )
    .unwrap();
    assert!(!expected.is_empty());
    assert_eq!(output(&expected, &slots), output(&actual, &slots));
    let budget = Arc::new(MemBudget::new(1));
    let error = with_query_budget(budget.clone(), || {
        aggregate_plan_rows(rows, &select.group_by, None, &slots)
    })
    .err()
    .expect("budget rejection");
    assert!(error.is(crate::error::catalog::ER_OUTOFMEMORY));
    assert_eq!(budget.used(), 0);
    let token = crate::engine::cancel::QueryCancelToken::noop();
    token.cancel_local();
    let _scope = crate::engine::cancel::enter(token);
    let (rows, _) = fixture(70);
    let error = aggregate_plan_rows(rows, &select.group_by, None, &slots)
        .err()
        .expect("cancelled");
    assert!(error.is(crate::error::catalog::ER_QUERY_INTERRUPTED));
}

#[test]
fn row_aggregate_stream_errors_preserve_retry_policy_and_release_charges() {
    let (select, slots) = spec("SELECT COUNT(*) FROM t GROUP BY g");
    let budget = Arc::new(MemBudget::new(1));
    let mut stream = MaterializedStream::new(fixture(3).0);
    let error = with_query_budget(budget.clone(), || {
        aggregate_row_stream(&mut stream, &select.group_by, None, &slots)
    })
    .err()
    .unwrap();
    assert!(is_hash_agg_stream_overflow(&error));
    assert_eq!(budget.used(), 0);

    struct FailingStream(Option<PlanRow>);
    impl PlanStream for FailingStream {
        fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
            match self.0.take() {
                Some(row) => Ok(Some(row)),
                None => Err(crate::error::catalog::ER_QUERY_INTERRUPTED.error("upstream failure")),
            }
        }
    }
    let budget = Arc::new(MemBudget::new(1024 * 1024));
    let mut stream = FailingStream(fixture(1).0.pop());
    let error = with_query_budget(budget.clone(), || {
        aggregate_row_stream(&mut stream, &select.group_by, None, &slots)
    })
    .err()
    .unwrap();
    assert!(error.is(crate::error::catalog::ER_QUERY_INTERRUPTED));
    assert!(!is_hash_agg_stream_overflow(&error));
    assert_eq!(budget.used(), 0);

    for sql in [
        "SELECT SUM(missing_column) FROM t GROUP BY g",
        "SELECT COUNT(*) FROM t GROUP BY g HAVING missing_column > 0",
    ] {
        let (select, slots) = spec(sql);
        let (rows, _) = fixture(3);
        let mut stream = MaterializedStream::new(rows.clone());
        let row_error = with_query_budget(budget.clone(), || {
            aggregate_plan_rows(rows, &select.group_by, select.having.as_ref(), &slots)
        })
        .err()
        .unwrap();
        let stream_error = with_query_budget(budget.clone(), || {
            aggregate_row_stream(
                &mut stream,
                &select.group_by,
                select.having.as_ref(),
                &slots,
            )
        })
        .err()
        .unwrap();
        assert!(row_error.is(crate::error::catalog::ER_BAD_FIELD_ERROR));
        assert!(stream_error.is(crate::error::catalog::ER_BAD_FIELD_ERROR));
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn row_aggregate_hidden_first_value_retains_source_fields_and_group_metadata() {
    let (select, slots) = spec("SELECT __hidden_first_value(id), COUNT(*) FROM t GROUP BY g, k");
    let (rows, _) = fixture(30);
    let mut stream = MaterializedStream::new(rows.clone());
    let memory = aggregate_plan_rows(rows, &select.group_by, None, &slots).unwrap();
    let streamed = aggregate_row_stream(&mut stream, &select.group_by, None, &slots).unwrap();
    assert_eq!(output(&memory, &slots), output(&streamed, &slots));
    for row in memory.iter().chain(&streamed) {
        let first = &row.agg_values.as_ref().unwrap()[&slots[0].key];
        assert_eq!(
            &row.fields.iter().find(|f| f.column == "id").unwrap().value,
            first
        );
        let markers: Vec<_> = row.fields.iter().filter(|f| f.table == "$group").collect();
        assert_eq!(markers.len(), 2);
        for (index, column) in ["g", "k"].iter().enumerate() {
            assert_eq!(markers[index].column, format!("group_{index}"));
            assert_eq!(markers[index].column_type, ColumnType::VarString);
            assert_eq!(
                markers[index].value,
                row.fields
                    .iter()
                    .find(|f| f.column == *column)
                    .unwrap()
                    .value
            );
        }
    }
}
