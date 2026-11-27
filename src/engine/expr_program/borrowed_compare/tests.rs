use super::*;

fn schema(collation: u16) -> InputSchema {
    InputSchema::new(
        [
            ("s", ColumnType::VarString),
            ("k", ColumnType::VarString),
            ("p", ColumnType::LongLong),
            ("n", ColumnType::LongLong),
        ]
        .into_iter()
        .map(|(name, column_type)| InputColumn {
            table: "t".into(),
            alias: None,
            name: name.into(),
            column_type,
            unsigned: false,
            collation_id: Some(collation),
        })
        .collect(),
    )
}

fn run(
    program: &ExprProgram,
    values: &[Value],
    ordinary: bool,
) -> (Result<Value, QueryError>, crate::engine::api::SqlWarnings) {
    let sink = crate::engine::api::StatementWarningSink::default();
    let row = RowView::from_values(values);
    let result = crate::engine::api::with_statement_warning_sink(sink.clone(), || {
        with_eval_scratch(|scratch| {
            if ordinary {
                let ExprNode::Binary {
                    left,
                    right,
                    op,
                    semantics,
                } = &program.root
                else {
                    panic!("binary root")
                };
                let left = left.eval_ref(&row, scratch)?;
                let right = right.eval_ref(&row, scratch)?;
                semantics.compare(left.as_value(), *op, right.as_value())
            } else {
                program.eval_row(&row, scratch)
            }
        })
    });
    let warnings = sink.lock().clone();
    (result, warnings)
}

#[test]
fn borrowed_substring_comparisons_match_materialized_values_and_diagnostics() {
    let sources = [
        Value::String("Aé中🙂bc \0".into()),
        Value::String("".into()),
        Value::SmallString(crate::types::SmallStringBuf::from_slice(b"abc ")),
        Value::String("a".repeat(1000)),
        Value::Bytes(vec![0xff, 0, 65]),
        Value::Int(1234),
        Value::Null,
    ];
    let extents = [
        Value::Int(i64::MIN),
        Value::Int(-5),
        Value::Int(-1),
        Value::Int(0),
        Value::Int(1),
        Value::Int(3),
        Value::Int(i64::MAX),
        Value::UInt(u64::MAX),
        Value::String("2.5x".into()),
        Value::DecimalCompact(15, 1),
        Value::Decimal128(i128::MAX, 0),
        Value::Decimal("999999999999999999999999999.5".into()),
        Value::Float(-2.5),
        Value::Double(1.5),
        Value::Null,
    ];
    for collation in ["utf8mb4_0900_ai_ci", "utf8mb4_bin", "utf8mb4_unicode_ci"] {
        let schema = schema(crate::charset::resolve_collation(collation).unwrap().id);
        for sql in [
            "SUBSTRING(s,p,n)=k",
            "k <=> SUBSTRING(s,p)",
            "SUBSTRING(s,2,3)<>k",
            "SUBSTRING(s,p,n)>=SUBSTRING(k,n,p)",
        ] {
            let ast = crate::parser::parse_expr_fragment(sql).unwrap();
            let program = ExprCompiler::compile(&ast, &schema).unwrap();
            for s in &sources {
                for p in &extents {
                    for k in [
                        Value::String("é中🙂".into()),
                        Value::String("A".into()),
                        Value::String("a ".into()),
                        Value::Int(12),
                        Value::Null,
                    ] {
                        let values = [s.clone(), k, p.clone(), p.clone()];
                        assert_eq!(
                            run(&program, &values, false),
                            run(&program, &values, true),
                            "{collation}; {sql}; {values:?}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn homogeneous_text_comparison_matches_full_oracle_with_numeric_scopes() {
    for collation in ["utf8mb4_0900_ai_ci", "utf8mb4_bin", "utf8mb4_unicode_ci"] {
        let semantics = BinarySemantics {
            collation_id: Some(crate::charset::resolve_collation(collation).unwrap().id),
            ..Default::default()
        };
        for left in ["1", "01", "A", "á", "中", "x ", "", "🙂"] {
            for right in ["01", "a", "a ", "中", "x", "", "🙂"] {
                for op in [
                    BinaryOp::Eq,
                    BinaryOp::NullSafeEq,
                    BinaryOp::NotEq,
                    BinaryOp::Lt,
                    BinaryOp::LtEq,
                    BinaryOp::Gt,
                    BinaryOp::GtEq,
                ] {
                    let check = || {
                        let (l, r) = (Value::String(left.into()), Value::String(right.into()));
                        let oracle =
                            crate::engine::expr::with_numeric_comparison_context_from_provenance(
                                semantics.comparison,
                                &l,
                                &r,
                                || {
                                    crate::engine::expr::with_compare_collation(
                                        semantics.collation_id,
                                        || null_cmp_binary(&l, op, &r),
                                    )
                                },
                            );
                        assert_eq!(semantics.compare(&l, op, &r), oracle);
                    };
                    check();
                    crate::engine::expr::with_numeric_comparison_context_from_provenance(
                        Default::default(),
                        &Value::DecimalCompact(1, 0),
                        &Value::String("1".into()),
                        check,
                    );
                }
            }
        }
    }
}

#[test]
fn borrowed_substring_parameters_rebind_without_replaying_evaluation() {
    let schema = schema(crate::charset::UTF8MB4_0900_AI_CI_ID);
    let ast = crate::parser::parse_expr_fragment("SUBSTRING(s,?,?)=?").unwrap();
    let program = ExprCompiler::compile(&ast, &schema).unwrap();
    for p in [
        Value::Int(2),
        Value::Int(-3),
        Value::String("2x".into()),
        Value::Null,
    ] {
        crate::engine::params::with_params(
            vec![p, Value::Int(1), Value::String("中".into())],
            || {
                let values = [
                    Value::String("a中🙂".into()),
                    Value::Null,
                    Value::Int(1),
                    Value::Int(1),
                ];
                assert_eq!(run(&program, &values, false), run(&program, &values, true));
            },
        );
    }
}

#[test]
fn borrowed_comparison_admission_uses_only_stable_text_leaves() {
    let schema = schema(crate::charset::UTF8MB4_0900_AI_CI_ID);
    let ast = crate::parser::parse_expr_fragment("SUBSTRING(s,p,n)=k").unwrap();
    let program = ExprCompiler::compile(&ast, &schema).unwrap();
    let ExprNode::Binary { left, .. } = &program.root else {
        panic!("binary root")
    };
    let values = [
        Value::String("a中🙂".into()),
        Value::String("中".into()),
        Value::Int(2),
        Value::Int(1),
    ];
    let row = RowView::from_values(&values);
    assert!(substring_input(left, &row).is_some());
    with_eval_scratch(|scratch| {
        assert!(matches!(
            operand(left, &row, scratch).unwrap(),
            Operand::Text("中")
        ));
        Ok(())
    })
    .unwrap();
    let values = [
        Value::Bytes(b"abc".to_vec()),
        Value::Null,
        Value::Int(1),
        Value::Int(1),
    ];
    assert!(substring_input(left, &RowView::from_values(&values)).is_none());
    let ast = crate::parser::parse_expr_fragment("SUBSTRING(CONCAT(s,k),p,n)=k").unwrap();
    let program = ExprCompiler::compile(&ast, &schema).unwrap();
    let ExprNode::Binary { left, .. } = &program.root else {
        panic!("binary root")
    };
    assert!(substring_input(left, &row).is_none());
}
