use super::*;
use crate::engine::api::SessionContext;
use crate::engine::executor::vec::schema::BatchField;

fn compile(sql: &str, kind: ColumnType) -> (ExprProgram, Arc<BatchSchema>) {
    let schema = Arc::new(BatchSchema::new(vec![BatchField {
        table: "t".into(),
        alias: None,
        name: "x".into(),
        column_type: kind,
        source: crate::types::ColumnSource {
            length: Some(128),
            ..Default::default()
        },
    }]));
    let ast = crate::parser::parse_expr_fragment(sql).unwrap();
    let program = ExprCompiler::compile(
        &ast,
        &crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema),
    )
    .unwrap();
    (program, schema)
}
fn oracle(program: &ExprProgram, batch: &Batch) -> Result<Vec<Value>, QueryError> {
    (0..batch.num_rows())
        .map(|row| {
            let input: Vec<_> = batch
                .columns()
                .iter()
                .zip(batch.schema().fields())
                .map(|(column, field)| column.typed_value_at(row, field.column_type))
                .collect();
            with_eval_scratch(|scratch| program.eval_row(&RowView::from_values(&input), scratch))
        })
        .collect()
}
fn parity(sql: &str, kind: ColumnType, values: Vec<Value>) {
    let (program, schema) = compile(sql, kind);
    let batch = Batch::new(schema, vec![ColumnVector::from_values(values)]);
    let expected = oracle(&program, &batch).unwrap();
    let column = program
        .try_eval_column(&batch)
        .unwrap_or_else(|| panic!("kernel declined {sql}"));
    let actual: Vec<_> = (0..batch.num_rows()).map(|r| column.value_at(r)).collect();
    assert_eq!(actual, expected, "{sql}");
}
#[test]
fn composed_string_columns_match_scalar_unicode_binary_and_null_semantics() {
    let values = vec![
        Value::Null,
        Value::String("".into()),
        Value::String("  aßİ中文ΐ🙂  ".into()),
        Value::String("xyzxyz".into()),
        Value::String("a\0b".into()),
    ];
    for sql in [
        "REVERSE(x)",
        "LOWER(UPPER(x))",
        "TRIM(x)",
        "LTRIM(RTRIM(x))",
        "LENGTH(REVERSE(x))",
        "CHAR_LENGTH(LOWER(x))",
        "LENGTH(REVERSE(x)) > 0",
        "CHAR_LENGTH(x) = 3",
        "LEFT(REVERSE(x),3)",
        "RIGHT(x,1000)",
        "RIGHT(x,0)",
        "RIGHT(x,-1)",
        "SUBSTRING(x,-3,2)",
        "SUBSTRING(x,-1000,2)",
        "SUBSTRING(x,0,2)",
        "CONCAT(REVERSE(x),'中文',x)",
        "CONCAT(x,NULL)",
        "REPLACE(x,'xyz','🙂')",
        "REPLACE(x,'','abc')",
    ] {
        parity(sql, ColumnType::VarString, values.clone());
    }
    // Binary collation metadata is independent of UTF-8 validity.
    for sql in [
        "REVERSE(x)",
        "RIGHT(x,1000)",
        "SUBSTRING(x,-2,1)",
        "CONCAT(x,X'FF00')",
    ] {
        let (_, schema) = compile(sql, ColumnType::Blob);
        // Recompile with the actual declared binary source.
        let mut field = schema.fields()[0].clone();
        field.source.flags |= crate::mysql::protocol::BINARY_FLAG;
        field.source.collation_id = Some(crate::charset::BINARY_COLLATION_ID);
        let schema = Arc::new(BatchSchema::new(vec![field]));
        let ast = crate::parser::parse_expr_fragment(sql).unwrap();
        let program = ExprCompiler::compile(
            &ast,
            &crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema),
        )
        .unwrap();
        let mut valid = Bitmap::new_all_valid(3);
        valid.set(2, false);
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::Bytes {
                data: vec![0xff, 0, 0xc3],
                offsets: vec![0, 3, 3, 3],
                nulls: Some(valid),
            }],
        );
        let out = program.try_eval_column(&batch).unwrap_or_else(|| {
            panic!(
                "binary kernel declined {sql}: {:?}; lane {:?}",
                program.root,
                batch.columns()[0]
            )
        });
        assert_eq!(
            (0..batch.num_rows())
                .map(|r| out.value_at(r))
                .collect::<Vec<_>>(),
            oracle(&program, &batch).unwrap(),
            "{sql}"
        );
        let generic = Batch::new(
            schema,
            vec![ColumnVector::Generic {
                values: vec![
                    Value::Bytes(vec![0xff, 0, 0xc3]),
                    Value::Bytes(vec![]),
                    Value::Null,
                ],
            }],
        );
        assert!(program.try_eval_column(&generic).is_none());
        let fallback =
            crate::engine::executor::vec::expr_batch::eval_to_column(&program, &generic).unwrap();
        assert_eq!(
            (0..generic.num_rows())
                .map(|r| fallback.value_at(r))
                .collect::<Vec<_>>(),
            oracle(&program, &generic).unwrap()
        );
    }
}
#[test]
fn all_null_physical_lanes_do_not_become_empty_strings_or_zero() {
    for sql in [
        "REVERSE(x)",
        "TRIM(x)",
        "LENGTH(x)",
        "CHAR_LENGTH(x)",
        "LEFT(x,2)",
        "RIGHT(x,200)",
        "SUBSTRING(x,-1,2)",
        "CONCAT(x,'a')",
    ] {
        parity(sql, ColumnType::VarString, vec![Value::Null; 3]);
    }
    for kind in [
        ColumnType::Date,
        ColumnType::DateTime,
        ColumnType::Timestamp,
    ] {
        parity("DATE_FORMAT(x,'%Y-%m-%d')", kind, vec![Value::Null; 3]);
    }
}

#[test]
fn composed_integer_temporal_columns_keep_overflow_and_null_fallback() {
    let date = |y, m, d| {
        Value::Date(
            (NaiveDate::from_ymd_opt(y, m, d).unwrap()
                - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32,
        )
    };
    for sql in [
        "DATE(x)",
        "ABS(YEAR(x))",
        "ABS(EXTRACT(YEAR FROM x))",
        "EXTRACT(YEAR FROM x)+0",
        "FLOOR(YEAR(x))",
        "CEIL(YEAR(x))",
        "YEAR(x)+0",
        "YEAR(x)*100+MONTH(x)",
        "ABS(YEAR(x)) >= 2000",
        "SIGN(YEAR(x))",
    ] {
        parity(
            sql,
            ColumnType::Date,
            vec![
                date(0, 1, 1),
                date(1999, 12, 31),
                Value::Null,
                date(9999, 12, 31),
                Value::Date(crate::types::ZERO_DATE),
            ],
        );
    }
    let (program, schema) = compile("ABS(x)", ColumnType::LongLong);
    let batch = Batch::new(
        schema.clone(),
        vec![ColumnVector::I64 {
            data: vec![1, i64::MIN],
            nulls: None,
        }],
    );
    assert!(!program.column_parallel_safe(&schema));
    assert!(program.try_eval_column(&batch).is_none());
    assert!(oracle(&program, &batch).is_err());
    let (program, schema) = compile("x+1", ColumnType::LongLong);
    let batch = Batch::new(
        schema,
        vec![ColumnVector::I64 {
            data: vec![i64::MAX],
            nulls: None,
        }],
    );
    assert!(program.try_eval_column(&batch).is_none());
    assert!(oracle(&program, &batch).is_err());
}
#[test]
fn tagged_date_components_are_identical_in_scalar_and_composed_columns() {
    let dates = vec![
        Value::Date(crate::types::encode_tagged_date(2014, 2, 31).unwrap()),
        Value::Date(crate::types::encode_tagged_date(2024, 0, 1).unwrap()),
        Value::Null,
    ];
    for sql in [
        "DATE(x)",
        "YEAR(x)",
        "MONTH(x)",
        "DAY(x)",
        "QUARTER(x)",
        "ABS(YEAR(x))",
        "YEAR(x)+0",
    ] {
        parity(sql, ColumnType::Date, dates.clone());
    }
    assert_eq!(
        crate::engine::expr::extract_datetime_part("YEAR", "YEAR", &dates[0]).unwrap(),
        Value::Int(2014)
    );
    assert_eq!(
        crate::engine::expr::extract_datetime_part("MONTH", "MONTH", &dates[1]).unwrap(),
        Value::Int(0)
    );
    let value = Value::DateTime(
        crate::types::encode_tagged_datetime(2024, 0, 1, 23, 59, 58, 123456).unwrap(),
    );
    for sql in [
        "YEAR(x)",
        "MONTH(x)",
        "DAY(x)",
        "QUARTER(x)",
        "HOUR(x)",
        "MINUTE(x)",
        "SECOND(x)",
        "ABS(YEAR(x))",
    ] {
        parity(sql, ColumnType::DateTime, vec![value.clone(), Value::Null]);
    }
}

#[test]
fn parsed_date_formats_cover_specifiers_locales_week_boundaries_and_time_zones() {
    let mut session = SessionContext::default();
    let format="%Y %y %m %c %d %e %H %k %h %I %l %i %s %S %f %j %w %a %W %b %M %p %r %T %D %U %u %V %v %X %x %% %q 中文%";
    for locale in [
        "en_US", "zh_CN", "de_DE", "fr_FR", "es_ES", "it_IT", "pt_BR", "ja_JP", "ko_KR",
    ] {
        session.sysvars.lc_time_names = locale.into();
        for zone in ["+00:00", "+08:00", "America/New_York"] {
            session.sysvars.time_zone = zone.into();
            crate::engine::expr::with_session_expr_info(&session, || {
                let values = vec![
                    Value::Null,
                    Value::DateTime(-62_167_219_200_000_000),
                    Value::DateTime(1_609_459_199_123_456),
                    Value::DateTime(1_710_054_000_654_321),
                    Value::DateTime(253_402_214_399_999_999),
                ];
                parity(
                    &format!("DATE_FORMAT(x,'{format}')"),
                    ColumnType::DateTime,
                    values.clone(),
                );
                parity(
                    &format!("DATE_FORMAT(x,'{format}')"),
                    ColumnType::Timestamp,
                    values
                        .into_iter()
                        .skip(2)
                        .map(|v| {
                            if let Value::DateTime(n) = v {
                                Value::Timestamp(n)
                            } else {
                                v
                            }
                        })
                        .collect(),
                );
            });
        }
    }
    let (program, schema) = compile("DATE_FORMAT(x,'%Y-%m-%d')", ColumnType::Date);
    let batch = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![Value::Date(
            crate::types::ZERO_DATE,
        )])],
    );
    assert!(program.try_eval_column(&batch).is_none());
}
#[test]
fn selection_preserves_alignment_and_dynamic_formats_retain_the_oracle() {
    let (program, schema) = compile("REVERSE(x)", ColumnType::VarString);
    let batch = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![
            Value::String("a".into()),
            Value::String("b".into()),
        ])],
    )
    .with_selection(crate::engine::executor::vec::batch::Selection::Indices(
        vec![1],
    ));
    let column = program.try_eval_column(&batch).unwrap();
    assert_eq!(column.value_at(0), Value::Null);
    assert_eq!(column.value_at(1), Value::String("b".into()));
    let (program, schema) = compile("DATE_FORMAT(x,x)", ColumnType::Date);
    assert!(!program.has_column_contract(&schema));
    for sql in [
        "CONCAT(x,'x')",
        "TRIM(x)",
        "LOWER(x)",
        "REVERSE(x)",
        "MD5(x)",
        "SHA2(x,512)",
    ] {
        let (program, _) = compile(sql, ColumnType::VarString);
        assert!(
            crate::engine::executor::vec::evaluated_batch::result_bound(&program).is_some(),
            "{sql}"
        );
    }
}

#[test]
fn prepared_columns_rebind_values_without_retaining_previous_formats_or_extents() {
    let ast = crate::parser::parse_expr_fragment("DATE_FORMAT(x,?)").unwrap();
    let (_, schema) = compile("x", ColumnType::DateTime);
    let program = crate::engine::params::with_params(vec![Value::String("%Y".into())], || {
        ExprCompiler::compile_cached(
            &ast,
            &crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema),
        )
        .unwrap()
    });
    let batch = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![
            Value::DateTime(1_710_054_000_123_456),
            Value::Null,
        ])],
    );
    for format in ["%Y", "%M", "%Y-%m-%d %H:%i:%s.%f", "%W 中文"] {
        crate::engine::params::with_params(vec![Value::String(format.into())], || {
            let output = program.try_eval_column(&batch).unwrap();
            assert_eq!(
                (0..batch.num_rows())
                    .map(|r| output.value_at(r))
                    .collect::<Vec<_>>(),
                oracle(&program, &batch).unwrap()
            );
        });
    }
    let (_, schema) = compile("x", ColumnType::VarString);
    let ast = crate::parser::parse_expr_fragment("RIGHT(x,?)").unwrap();
    let program = crate::engine::params::with_params(vec![Value::Int(1)], || {
        ExprCompiler::compile(
            &ast,
            &crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema),
        )
        .unwrap()
    });
    let batch = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![
            Value::String("中文🙂".into()),
            Value::Null,
        ])],
    );
    for count in [-1, 0, 1, 2, i64::MAX] {
        crate::engine::params::with_params(vec![Value::Int(count)], || {
            let output = program.try_eval_column(&batch).unwrap();
            assert_eq!(
                (0..batch.num_rows())
                    .map(|r| output.value_at(r))
                    .collect::<Vec<_>>(),
                oracle(&program, &batch).unwrap()
            );
        });
    }
}

#[test]
fn invisible_invalid_dates_and_duplicate_selection_indices_keep_null_alignment() {
    let (program, schema) = compile("DATE_FORMAT(x,'%Y-%m-%d')", ColumnType::Date);
    let batch = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![
            Value::Date(crate::types::ZERO_DATE),
            Value::Date(0),
            Value::Null,
        ])],
    )
    .with_selection(crate::engine::executor::vec::batch::Selection::Indices(
        vec![1, 2, 1],
    ));
    let column = program.try_eval_column(&batch).unwrap();
    assert_eq!(
        (0..3).map(|r| column.value_at(r)).collect::<Vec<_>>(),
        vec![Value::Null, Value::String("1970-01-01".into()), Value::Null]
    );
}

#[test]
fn small_result_does_not_materialize_an_unbounded_intermediate_column() {
    let literal = "x".repeat(128 * 1024);
    let sql = format!("LENGTH(CONCAT(x,'{literal}'))");
    let (program, schema) = compile(&sql, ColumnType::VarString);
    let batch = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![
            Value::String("a".into());
            64
        ])],
    );
    assert!(program.try_eval_column(&batch).is_none());
}

#[test]
fn date_identity_declines_mismatched_physical_lanes() {
    let (program, schema) = compile("DATE(x)", ColumnType::Date);
    let batch = Batch::new(
        schema,
        vec![ColumnVector::I64 {
            data: vec![20240101],
            nulls: None,
        }],
    );
    assert!(program.try_eval_column(&batch).is_none());
    let fallback =
        crate::engine::executor::vec::expr_batch::eval_to_column(&program, &batch).unwrap();
    assert_eq!(fallback.value_at(0), oracle(&program, &batch).unwrap()[0]);
}

#[test]
fn trim_rechecks_binary_lanes_under_text_metadata() {
    for sql in ["TRIM(x)", "LTRIM(x)", "RTRIM(x)"] {
        let (program, schema) = compile(sql, ColumnType::VarString);
        let batch = Batch::new(
            schema,
            vec![ColumnVector::Bytes {
                data: b" a ".to_vec(),
                offsets: vec![0, 3],
                nulls: None,
            }],
        );
        assert!(program.try_eval_column(&batch).is_none());
        let fallback =
            crate::engine::executor::vec::expr_batch::eval_to_column(&program, &batch).unwrap();
        assert_eq!(
            fallback.value_at(0),
            oracle(&program, &batch).unwrap()[0],
            "{sql}"
        );
    }
}
