use super::*;
use crate::engine::executor::vec::schema::BatchField;
use crate::parser::{Literal, SelectItem, Statement};

fn expr(text: &str) -> Expr {
    let Statement::Select(select) =
        crate::parser::parse_statement(&format!("SELECT {text}")).unwrap()
    else {
        panic!("SELECT expected")
    };
    let SelectItem::Expr { expr, .. } = select.projection.into_iter().next().unwrap() else {
        panic!("expression expected")
    };
    expr
}

fn schema(column_type: ColumnType) -> Arc<BatchSchema> {
    Arc::new(BatchSchema::new(vec![BatchField {
        table: "t".into(),
        alias: None,
        name: "x".into(),
        column_type,
        source: Default::default(),
    }]))
}

/// This oracle calls eval_row directly: eval_value_per_row can itself select a
/// typed temporal kernel and is therefore not an independent scalar oracle.
fn scalar_values(program: &ExprProgram, batch: &Batch) -> Result<Vec<Value>, QueryError> {
    let mut values = vec![Value::Null; batch.num_rows()];
    for row in batch.visible_indices() {
        let input: Vec<_> = batch
            .columns()
            .iter()
            .map(|column| column.value_at(row))
            .collect();
        values[row] =
            with_eval_scratch(|scratch| program.eval_row(&RowView::from_values(&input), scratch))?;
    }
    Ok(values)
}

#[test]
fn vector_at_open_binds_once_across_batches_and_rebinds() {
    use crate::engine::vector::codec::{parse_typed_text, text_parse_calls};
    use crate::vector::VectorDataType;
    let schema = schema(ColumnType::Vector);
    for datatype in [
        VectorDataType::Float32,
        VectorDataType::Float16,
        VectorDataType::BFloat16,
        VectorDataType::Int8,
        VectorDataType::Binary,
    ] {
        let vector = Value::Vector(parse_typed_text("[1,0,1,0]", datatype).unwrap());
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(vec![vector; 130])],
        );
        let metrics: &[&str] = if datatype == VectorDataType::Binary {
            &["hamming", "jaccard"]
        } else {
            &["l2", "cosine", "dot"]
        };
        for constructor in ["TO_VECTOR", "STRING_TO_VECTOR"] {
            for metric in metrics {
                let expression = expr(&format!("DISTANCE(x,{constructor}(?,?),'{metric}')"));
                for text in ["[0,1,1,0]", "[1,0,0,1]"] {
                    crate::engine::params::with_params(
                        vec![
                            Value::String(text.into()),
                            Value::String(datatype.name().into()),
                        ],
                        || {
                            let oracle = compile_for_batch(&expression, &schema).unwrap().unwrap();
                            let expected = scalar_values(&oracle, &batch).unwrap();
                            let before = text_parse_calls();
                            let program = compile_for_batch_at_open(&expression, &schema)
                                .unwrap()
                                .unwrap();
                            for _ in 0..3 {
                                assert_eq!(eval_value_per_row(&program, &batch).unwrap(), expected);
                                let selected = batch
                                    .clone()
                                    .with_selection(Selection::Indices(vec![0, 65, 129]));
                                let actual = eval_value_per_row(&program, &selected).unwrap();
                                for row in 0..130 {
                                    assert_eq!(
                                        actual[row],
                                        if [0, 65, 129].contains(&row) {
                                            expected[row].clone()
                                        } else {
                                            Value::Null
                                        }
                                    );
                                }
                            }
                            assert_eq!(
                                text_parse_calls() - before,
                                1,
                                "{datatype}/{metric}/{text}"
                            );
                        },
                    );
                }
            }
        }
    }
}

#[test]
fn vector_at_open_preserves_selection_errors_and_warning_counts() {
    let schema = schema(ColumnType::LongLong);
    let batch = Batch::new(
        schema.clone(),
        vec![ColumnVector::from_values(vec![
            Value::Int(-1),
            Value::Int(1),
            Value::Int(-2),
        ])],
    );
    let expression = expr("IF(x>0,L2_DISTANCE(TO_VECTOR('[1,0]'),TO_VECTOR('[bad]')),0)");
    crate::engine::api::clear_statement_warnings();
    let program = compile_for_batch_at_open(&expression, &schema)
        .unwrap()
        .unwrap();
    assert!(crate::engine::api::statement_warnings_snapshot().is_empty());
    let selected = batch.clone().with_selection(Selection::Indices(vec![0, 2]));
    assert_eq!(
        eval_value_per_row(&program, &selected).unwrap(),
        vec![Value::Double(0.0), Value::Null, Value::Double(0.0)]
    );
    let oracle = compile_for_batch(&expression, &schema).unwrap().unwrap();
    assert_eq!(
        eval_value_per_row(&program, &batch).unwrap_err().code(),
        scalar_values(&oracle, &batch).unwrap_err().code()
    );

    let expression = expr(
        "L2_DISTANCE(TO_VECTOR(CONCAT('[',CAST('1tail' AS SIGNED),',0]')),TO_VECTOR('[0,1]'))",
    );
    let oracle = compile_for_batch(&expression, &schema).unwrap().unwrap();
    crate::engine::api::clear_statement_warnings();
    let program = compile_for_batch_at_open(&expression, &schema)
        .unwrap()
        .unwrap();
    assert!(crate::engine::api::statement_warnings_snapshot().is_empty());
    for batch in [&batch, &selected] {
        crate::engine::api::clear_statement_warnings();
        let expected = scalar_values(&oracle, batch).unwrap();
        let warnings = crate::engine::api::statement_warnings_snapshot();
        assert_eq!(warnings.len(), batch.visible_rows());
        crate::engine::api::clear_statement_warnings();
        assert_eq!(eval_value_per_row(&program, batch).unwrap(), expected);
        assert_eq!(crate::engine::api::statement_warnings_snapshot(), warnings);
    }
    crate::engine::api::clear_statement_warnings();
}

#[test]
fn ordinary_vector_batch_program_keeps_dynamic_parameters() {
    let schema = schema(ColumnType::LongLong);
    let expression = expr("TO_VECTOR(?)");
    let batch = Batch::new(
        schema.clone(),
        vec![ColumnVector::from_values(vec![Value::Int(0)])],
    );
    let program = crate::engine::params::with_params(vec![Value::String("[1,0]".into())], || {
        compile_for_batch(&expression, &schema).unwrap().unwrap()
    });
    for text in ["[1,0]", "[0,1]"] {
        crate::engine::params::with_params(vec![Value::String(text.into())], || {
            let current = compile_for_batch_at_open(&expression, &schema)
                .unwrap()
                .unwrap();
            assert_eq!(
                eval_value_per_row(&program, &batch).unwrap(),
                eval_value_per_row(&current, &batch).unwrap()
            );
        });
    }
}

#[test]
fn streaming_predicate_preserves_selection_nulls_and_diagnostics() {
    for (column_type, expression, column) in [
        (
            ColumnType::LongLong,
            "x / 0",
            ColumnVector::from_values(
                (0..130)
                    .map(|i| {
                        if i % 7 == 0 {
                            Value::Null
                        } else {
                            Value::Int(i)
                        }
                    })
                    .collect(),
            ),
        ),
        (
            ColumnType::VarString,
            "x + 1",
            ColumnVector::from_values(
                (0..130)
                    .map(|i| {
                        if i % 7 == 0 {
                            Value::Null
                        } else {
                            Value::String(format!("{i}tail"))
                        }
                    })
                    .collect(),
            ),
        ),
    ] {
        let schema = schema(column_type);
        let program = compile_for_batch(&expr(expression), &schema)
            .unwrap()
            .unwrap();
        let base = Batch::new(schema, vec![column]);
        let mut mask = Bitmap::new_all_invalid(130);
        for i in [0, 2, 63, 64, 65, 129] {
            mask.set_true(i);
        }
        for selection in [
            None,
            Some(Selection::Range { start: 63, len: 4 }),
            Some(Selection::Indices(vec![129, 2, 0, 65, 129])),
            Some(Selection::Mask(mask)),
            Some(Selection::Indices(Vec::new())),
        ] {
            let batch = selection.map_or_else(
                || base.clone(),
                |selection| base.clone().with_selection(selection),
            );
            crate::engine::api::clear_statement_warnings();
            let expected = scalar_values(&program, &batch).unwrap();
            let expected_warnings = crate::engine::api::statement_warnings_snapshot();
            crate::engine::api::clear_statement_warnings();
            let counter = std::sync::atomic::AtomicU64::new(0);
            let actual = eval_fallback_truth_mask(&program, &batch, &counter).unwrap();
            assert_eq!(
                crate::engine::api::statement_warnings_snapshot(),
                expected_warnings,
                "{expression}"
            );
            for (row, value) in expected.iter().enumerate() {
                assert_eq!(
                    actual.truth.get(row),
                    crate::engine::expr::truthy(value),
                    "{expression} row {row}"
                );
                assert_eq!(
                    actual.null.get(row),
                    matches!(value, Value::Null),
                    "{expression} row {row}"
                );
            }
            assert_eq!(
                counter.load(std::sync::atomic::Ordering::Relaxed),
                batch.visible_rows() as u64
            );
        }
    }
}

#[test]
fn selected_kernel_revalidates_parameters_and_physical_lanes() {
    let schema = schema(ColumnType::LongLong);
    let expression = Expr::Binary {
        left: Box::new(Expr::Identifier("x".into())),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Placeholder(0)),
    };
    let program = compile_for_batch(&expression, &schema).unwrap().unwrap();
    let kernel = PredicateKernel::select(&program);
    assert_eq!(kernel, PredicateKernel::ColumnConstant);
    // Reuse the same program/kernel across integral, floating and generic
    // lanes and changing parameter types. Unsupported lanes must fall back.
    for column in [
        ColumnVector::from_values(vec![Value::Int(-1), Value::Null, Value::Int(7)]),
        ColumnVector::from_values(vec![
            Value::Double(-0.0),
            Value::Double(f64::NAN),
            Value::Null,
        ]),
        ColumnVector::from_values(vec![
            Value::String("2".into()),
            Value::String("bad".into()),
            Value::Null,
        ]),
    ] {
        let batch = Batch::new(schema.clone(), vec![column]);
        for parameter in [
            Value::Int(1),
            Value::UInt(u64::MAX),
            Value::Null,
            Value::String("2oops".into()),
        ] {
            crate::engine::params::with_params(vec![parameter], || {
                crate::engine::api::clear_statement_warnings();
                let expected = scalar_values(&program, &batch).unwrap();
                let warnings = crate::engine::api::statement_warnings_snapshot();
                crate::engine::api::clear_statement_warnings();
                let actual = try_native_truth_mask(kernel, &program, &batch).unwrap_or_else(|| {
                    eval_fallback_truth_mask(
                        &program,
                        &batch,
                        &std::sync::atomic::AtomicU64::new(0),
                    )
                    .unwrap()
                });
                for (row, value) in expected.iter().enumerate() {
                    assert_eq!(actual.truth.get(row), crate::engine::expr::truthy(value));
                    assert_eq!(actual.null.get(row), matches!(value, Value::Null));
                }
                assert_eq!(crate::engine::api::statement_warnings_snapshot(), warnings);
            });
        }
    }
}

#[test]
fn cached_dependencies_include_legacy_escapes_and_follow_folding() {
    let input = InputSchema::new(
        ["x", "y", "z"]
            .into_iter()
            .map(|name| InputColumn {
                table: "t".into(),
                alias: None,
                name: name.into(),
                column_type: ColumnType::LongLong,
                unsigned: false,
                collation_id: None,
            })
            .collect(),
    );
    let expression = expr("x + z + x");
    for program in [
        ExprCompiler::compile(&expression, &input).unwrap(),
        ExprCompiler::compile_legacy_oracle(&expression, &input).unwrap(),
    ] {
        assert_eq!(program.referenced_columns(), [0, 2]);
        assert_eq!(
            program.referenced_columns().as_ptr(),
            program.referenced_columns().as_ptr()
        );
        let values = [Value::Int(3), Value::Int(99), Value::Int(5)];
        assert_eq!(
            with_eval_scratch(|scratch| program.eval_row(&RowView::from_values(&values), scratch))
                .unwrap(),
            Value::Int(11)
        );
    }
    let folded = ExprCompiler::compile(
        &Expr::Binary {
            left: Box::new(Expr::Literal(Literal::Int(1))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(Literal::Int(2))),
        },
        &input,
    )
    .unwrap();
    assert!(folded.referenced_columns().is_empty());
}

#[test]
fn predicate_string_truthiness_retains_the_post_evaluation_diagnostic_phase() {
    let schema = schema(ColumnType::VarString);
    let base = Batch::new(
        schema.clone(),
        vec![ColumnVector::from_values(vec![
            Value::String("1tail".into()),
            Value::String("2tail".into()),
        ])],
    );
    for sql in [
        "x",
        "IF(1 / 0, 'unused', x)",
        "IF(x = '2tail', JSON_EXTRACT('invalid', '$'), x)",
    ] {
        let program = compile_for_batch(&expr(sql), &schema).unwrap().unwrap();
        for indices in [vec![1, 0, 1], vec![0, 1]] {
            let batch = base.clone().with_selection(Selection::Indices(indices));
            crate::engine::api::clear_statement_warnings();
            // Old fallback first evaluates all rows, then coerces only their
            // final values in physical order. An evaluation error skips that
            // second phase entirely, including its string-conversion warnings.
            let expected = scalar_values(&program, &batch).map(|values| {
                values
                    .iter()
                    .map(|v| (crate::engine::expr::truthy(v), matches!(v, Value::Null)))
                    .collect::<Vec<_>>()
            });
            let expected_warnings = crate::engine::api::statement_warnings_snapshot();
            crate::engine::api::clear_statement_warnings();
            let actual =
                eval_fallback_truth_mask(&program, &batch, &std::sync::atomic::AtomicU64::new(0))
                    .map(|mask| {
                        (0..batch.num_rows())
                            .map(|row| (mask.truth.get(row), mask.null.get(row)))
                            .collect::<Vec<_>>()
                    });
            assert_eq!(actual, expected, "{sql}");
            assert_eq!(
                crate::engine::api::statement_warnings_snapshot(),
                expected_warnings,
                "{sql}"
            );
        }
    }
}
