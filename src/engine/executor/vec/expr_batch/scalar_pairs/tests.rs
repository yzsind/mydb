use super::*;
use crate::engine::executor::vec::schema::BatchField;

fn fixture(build_first: bool) -> (Arc<BatchSchema>, Vec<ColumnVector>, Batch) {
    let field = |name: &str, kind| BatchField {
        table: "t".into(),
        alias: None,
        name: name.into(),
        column_type: kind,
        source: Default::default(),
    };
    let probe_fields = vec![
        field("s", ColumnType::VarString),
        field("p", ColumnType::LongLong),
    ];
    let build_fields = vec![
        field("k", ColumnType::VarString),
        field("d", ColumnType::LongLong),
    ];
    let schema = Arc::new(BatchSchema::new(if build_first {
        build_fields
            .clone()
            .into_iter()
            .chain(probe_fields.clone())
            .collect()
    } else {
        probe_fields
            .clone()
            .into_iter()
            .chain(build_fields)
            .collect()
    }));
    let probe = Batch::new(
        Arc::new(BatchSchema::new(probe_fields)),
        vec![
            ColumnVector::from_values(vec![
                Value::String("abcdefghij".repeat(100)),
                Value::String("a中🙂bc".into()),
                Value::Null,
                Value::String("abcd".into()),
            ]),
            ColumnVector::from_values(vec![
                Value::Int(7),
                Value::Int(2),
                Value::Null,
                Value::Int(i64::MIN),
            ]),
        ],
    )
    .with_selection(Selection::Indices(vec![3, 0, 1, 0, 2]));
    let build = vec![
        ColumnVector::from_values(vec![
            Value::String("a".into()),
            Value::String("中".into()),
            Value::Null,
        ]),
        ColumnVector::from_values(vec![Value::Int(3), Value::Int(0), Value::Int(-1)]),
    ];
    (schema, build, probe)
}

#[test]
fn scalar_pairs_match_gathered_oracle_in_both_orientations_and_diagnostics() {
    for build_first in [false, true] {
        let (schema, build, probe) = fixture(build_first);
        // Repeated/reordered physical row ids exercise both decoder caches
        // across more than one word of result bits and deferred conditions.
        let build_rows = (0..73).map(|i| [0, 0, 1, 2][i % 4]).collect::<Vec<_>>();
        let probe_rows = (0..73).map(|i| [0, 0, 1, 2, 1][i % 5]).collect::<Vec<_>>();
        for sql in [
            "SUBSTRING(s,(p-1) DIV d+1,1)=k OR p DIV d>2",
            "CASE WHEN p DIV d>1 THEN 'bad' ELSE '1' END",
            "(p DIV d) + (d DIV 0)",
        ] {
            let expr = crate::parser::parse_expr_fragment(sql).unwrap();
            let program = BatchExprProgram::compile(&expr, &schema).unwrap().unwrap();
            assert!(program.scalar_pairs_eligible(&schema));
            let gathered = crate::engine::executor::vec::hash_join::gather_residual_pairs_batch(
                &build,
                &probe,
                &build_rows,
                &probe_rows,
                build_first,
                &schema,
                &program.referenced_columns(),
            );
            let run = |direct| {
                let sink = crate::engine::api::StatementWarningSink::default();
                let result = crate::engine::api::with_statement_warning_sink(sink.clone(), || {
                    if direct {
                        program
                            .eval_scalar_pairs(
                                &build,
                                &probe,
                                &build_rows,
                                &probe_rows,
                                build_first,
                                &schema,
                            )
                            .map(|value| value.expect("ordered scalar lane"))
                    } else {
                        program.eval_predicate_bitmap(&gathered)
                    }
                })
                .map(|mask| {
                    (0..build_rows.len())
                        .map(|r| mask.get(r))
                        .collect::<Vec<_>>()
                });
                let warnings = sink.lock().clone();
                (result, warnings)
            };
            assert_eq!(run(true), run(false), "{sql}; build_first={build_first}");
        }
        let expr = crate::parser::parse_expr_fragment("p DIV d > 0").unwrap();
        let program = BatchExprProgram::compile(&expr, &schema).unwrap().unwrap();
        let gathered = crate::engine::executor::vec::hash_join::gather_residual_pairs_batch(
            &build,
            &probe,
            &[2],
            &[3],
            build_first,
            &schema,
            &program.referenced_columns(),
        );
        assert_eq!(
            program
                .eval_scalar_pairs(&build, &probe, &[2], &[3], build_first, &schema)
                .unwrap_err(),
            program.eval_predicate_bitmap(&gathered).unwrap_err()
        );
    }
}

#[test]
fn scalar_pair_admission_refusal_has_no_evaluation_or_warning_side_effects() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let (schema, build, probe) = fixture(false);
    let expr = crate::parser::parse_expr_fragment("p DIV d > 0").unwrap();
    let program = BatchExprProgram::compile(&expr, &schema).unwrap().unwrap();
    let budget = Arc::new(MemBudget::new(0));
    let sink = crate::engine::api::StatementWarningSink::default();
    let before = program.expression_rows();
    crate::engine::api::with_statement_warning_sink(sink.clone(), || {
        with_query_budget(budget.clone(), || {
            assert!(program
                .eval_scalar_pairs(&build, &probe, &[1], &[0], false, &schema)
                .unwrap()
                .is_none());
        });
    });
    assert!(sink.lock().is_empty());
    assert_eq!(before, program.expression_rows());
    assert_eq!(budget.used(), 0);
    let native = crate::parser::parse_expr_fragment("p > 0").unwrap();
    let native = BatchExprProgram::compile(&native, &schema)
        .unwrap()
        .unwrap();
    assert!(native
        .eval_scalar_pairs(&build, &probe, &[0], &[0], false, &schema)
        .unwrap()
        .is_none());
}

#[test]
fn scalar_pairs_decline_heap_cell_layouts_before_evaluating() {
    let (schema, build, probe) = fixture(false);
    let expr = crate::parser::parse_expr_fragment("SUBSTRING(s,p DIV d,1)=k").unwrap();
    let program = BatchExprProgram::compile(&expr, &schema).unwrap().unwrap();
    let mut columns = probe.columns().to_vec();
    columns[0] = ColumnVector::Generic {
        values: (0..probe.num_rows())
            .map(|row| probe.columns()[0].value_at(row))
            .collect(),
    };
    let generic = Batch::new(probe.schema().clone(), columns);
    let before = program.expression_rows();
    assert!(program
        .eval_scalar_pairs(&build, &generic, &[1], &[0], false, &schema)
        .unwrap()
        .is_none());
    assert_eq!(before, program.expression_rows());
}
