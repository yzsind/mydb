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
    let format = "%Y %y %m %c %d %e %H %k %h %I %l %i %s %S %f %j %w %a %W %b %M %p %r %T %D %U %u %V %v %X %x %% %q 中文%";
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
fn small_result_chunks_intermediates_and_declines_oversized_single_rows() {
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
    let result = program
        .try_eval_column(&batch)
        .expect("bounded intermediate morsels");
    assert!((0..64).all(|row| result.value_at(row) == Value::Int(128 * 1024 + 1)));
    let literal = "x".repeat(5 * 1024 * 1024);
    let (program, schema) = compile(
        &format!("LENGTH(CONCAT(x,'{literal}'))"),
        ColumnType::VarString,
    );
    let batch = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![Value::String("a".into())])],
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

#[test]
fn digest_columns_match_scalar_for_all_hashes_octets_nulls_and_selections() {
    for sql in [
        "HEX(x)",
        "MD5(x)",
        "SHA(x)",
        "SHA1(x)",
        "SHA2(x,0)",
        "SHA2(x,224)",
        "SHA2(x,256)",
        "SHA2(x,384)",
        "SHA2(x,512)",
        "SHA2(MD5(x),256)",
        "LENGTH(SHA1(x))",
    ] {
        let values = vec![
            Value::Null,
            Value::String(String::new()),
            Value::String("a\0ß中文🙂".into()),
            Value::String("abc".into()),
            Value::String("x".repeat(70000)),
        ];
        parity(sql, ColumnType::VarString, values.clone());
        for binary in [false, true] {
            let (program, schema) = compile(
                sql,
                if binary {
                    ColumnType::Blob
                } else {
                    ColumnType::VarString
                },
            );
            let values = if binary {
                vec![
                    Value::Bytes(vec![0xff, 0, 0xc3]),
                    Value::Null,
                    Value::Bytes(vec![]),
                    Value::Bytes(b"abc".to_vec()),
                    Value::Bytes(vec![0; 70000]),
                ]
            } else {
                values.clone()
            };
            let mut batch = Batch::new(schema, vec![ColumnVector::from_values(values)]);
            let expected = oracle(&program, &batch).unwrap();
            batch.selection = Some(crate::engine::executor::vec::batch::Selection::Indices(
                vec![4, 0, 4, 2],
            ));
            let output =
                crate::engine::executor::vec::expr_batch::eval_to_column(&program, &batch).unwrap();
            for index in batch.visible_indices() {
                assert_eq!(
                    output.value_at(index),
                    expected[index],
                    "{sql}, binary={binary}, row={index}"
                );
            }
        }
        parity(sql, ColumnType::VarString, vec![Value::Null; 3]);
    }
    for sql in ["SHA2(x,123)", "SHA2(x,NULL)", "SHA2(x,x)"] {
        let (program, schema) = compile(sql, ColumnType::VarString);
        assert!(!program.column_parallel_safe(&schema), "{sql}");
        assert!(program
            .try_eval_column(&Batch::new(
                schema,
                vec![ColumnVector::from_values(vec![Value::String("abc".into())])]
            ))
            .is_none());
    }
}

#[test]
fn digest_scalar_preserves_typed_oracle_and_charset_octets() {
    for (kind, value) in [
        (ColumnType::LongLong, Value::Int(-123)),
        (ColumnType::Float, Value::Float(1.25)),
        (ColumnType::Double, Value::Double(1.25)),
        (
            ColumnType::Decimal {
                precision: 20,
                scale: 3,
            },
            Value::DecimalCompact(12340, 3),
        ),
        (ColumnType::Bit { bits: 9 }, Value::UInt(257)),
        (ColumnType::Date, Value::Date(19782)),
        (ColumnType::DateTime, Value::DateTime(1710000000123456)),
        (ColumnType::Timestamp, Value::Timestamp(1710000000123456)),
        (ColumnType::Json, Value::String("{\"x\":1}".into())),
        (ColumnType::VarString, Value::String("é中文".into())),
        (ColumnType::Blob, Value::Bytes(vec![0, 0xff, 0xc3])),
    ] {
        for sql in [
            "HEX(x)",
            "MD5(x)",
            "SHA(x)",
            "SHA1(x)",
            "SHA2(x,0)",
            "SHA2(x,224)",
            "SHA2(x,512)",
        ] {
            let (program, schema) = compile(sql, kind);
            let input = crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema);
            let ast = crate::parser::parse_expr_fragment(sql).unwrap();
            let legacy = ExprCompiler::compile_legacy_oracle(&ast, &input).unwrap();
            let values = [value.clone()];
            let row = RowView::from_values(&values);
            let expected = with_eval_scratch(|scratch| legacy.eval_row(&row, scratch)).unwrap();
            assert_eq!(
                with_eval_scratch(|scratch| program.eval_row(&row, scratch)).unwrap(),
                expected,
                "{kind:?} {sql}"
            );
        }
    }
    for collation in [8, 54, 63, 255] {
        let (_, schema) = compile("MD5(x)", ColumnType::VarString);
        let mut field = schema.fields()[0].clone();
        field.source.collation_id = Some(collation);
        let schema = BatchSchema::new(vec![field]);
        let input = crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema);
        for sql in ["MD5(x)", "SHA1(x)", "SHA2(x,256)"] {
            let ast = crate::parser::parse_expr_fragment(sql).unwrap();
            let program = ExprCompiler::compile(&ast, &input).unwrap();
            let legacy = ExprCompiler::compile_legacy_oracle(&ast, &input).unwrap();
            let values = [Value::String("é".into())];
            let row = RowView::from_values(&values);
            assert_eq!(
                with_eval_scratch(|scratch| program.eval_row(&row, scratch)).unwrap(),
                with_eval_scratch(|scratch| legacy.eval_row(&row, scratch)).unwrap(),
                "{collation} {sql}"
            );
        }
    }
}

#[test]
fn sha2_length_parameters_rebind_without_reusing_a_previous_kernel() {
    let (program, schema) = compile("SHA2(x,?)", ColumnType::VarString);
    let batch = Batch::new(
        schema.clone(),
        vec![ColumnVector::from_values(vec![
            Value::String("abc".into()),
            Value::Null,
        ])],
    );
    for length in [0, 224, 512, 123, 256] {
        crate::engine::params::with_params(vec![Value::Int(length)], || {
            if length == 123 {
                assert!(!program.column_parallel_safe(&schema));
                assert!(program.try_eval_column(&batch).is_none());
            } else {
                assert!(program.column_parallel_safe(&schema));
                let column = program.try_eval_column(&batch).unwrap();
                assert_eq!(
                    (0..2).map(|i| column.value_at(i)).collect::<Vec<_>>(),
                    oracle(&program, &batch).unwrap()
                );
            }
        });
    }
}

#[test]
fn network_columns_preserve_uuid_spellings_swap_nulls_and_composition() {
    let uuid = "00112233-4455-6677-8899-aabbccddeeff";
    let values = vec![
        Value::Null,
        Value::String(uuid.into()),
        Value::String(uuid.to_uppercase()),
        Value::String(uuid.replace('-', "")),
        Value::String(format!("{{{uuid}}}")),
    ];
    for sql in [
        "IS_UUID(x)",
        "HEX(UUID_TO_BIN(x))",
        "HEX(UUID_TO_BIN(x,1))",
        "BIN_TO_UUID(UUID_TO_BIN(x))",
        "BIN_TO_UUID(UUID_TO_BIN(x,1),1)",
    ] {
        parity(sql, ColumnType::VarString, values.clone());
    }
    let values = [i64::MIN, -255, -1, 0, 1, 255, i64::MAX]
        .into_iter()
        .map(Value::Int)
        .chain([Value::Null])
        .collect::<Vec<_>>();
    for sql in [
        "x%255",
        "CAST(x AS CHAR)",
        "CONCAT('value=',x)",
        "CONCAT(x%255,'::')",
        "LPAD(CAST(x AS CHAR),12,'0')",
        "RPAD(CAST(x AS CHAR),32,'中文')",
        "IS_UUID(CONCAT('00112233-4455-6677-8899-',LPAD(CAST(x AS CHAR),12,'0')))",
    ] {
        parity(sql, ColumnType::LongLong, values.clone());
    }
}

#[test]
fn network_columns_preserve_ipv4_ipv6_and_binary_predicates() {
    let values = ["127.0.0.1", "001.002.003.004", "255.255.255.255", "0.0.0.0"]
        .into_iter()
        .map(|s| Value::String(s.into()))
        .chain([Value::Null])
        .collect::<Vec<_>>();
    for sql in [
        "INET_ATON(x)",
        "IS_IPV4(x)",
        "INET_NTOA(INET_ATON(x))",
        "HEX(INET6_ATON(x))",
        "INET6_NTOA(INET6_ATON(x))",
    ] {
        parity(sql, ColumnType::VarString, values.clone());
    }
    let values = [
        "::",
        "::1",
        "::2",
        "2001:db8::1",
        "::192.000.002.001",
        "::ffff:192.000.002.001",
        "1:2:3:4:5:0:7:8",
    ]
    .into_iter()
    .map(|s| Value::String(s.into()))
    .chain([Value::Null])
    .collect::<Vec<_>>();
    for sql in [
        "IS_IPV6(x)",
        "HEX(INET6_ATON(x))",
        "INET6_NTOA(INET6_ATON(x))",
        "IS_IPV4_COMPAT(INET6_ATON(x))",
        "IS_IPV4_MAPPED(INET6_ATON(x))",
    ] {
        parity(sql, ColumnType::VarString, values.clone());
    }
    for sql in ["IS_UUID(x)", "IS_IPV4(x)", "IS_IPV6(x)"] {
        parity(
            sql,
            ColumnType::VarString,
            vec![
                Value::Null,
                Value::String("invalid🙂".into()),
                Value::String("".into()),
            ],
        );
    }
}

#[test]
fn network_speculation_declines_invalid_values_and_unsafe_reordering() {
    for sql in ["UUID_TO_BIN(x)", "INET_ATON(x)", "INET6_ATON(x)"] {
        let (program, schema) = compile(sql, ColumnType::VarString);
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(vec![Value::String(
                "invalid".into(),
            )])],
        );
        assert!(program.has_column_contract(&schema), "{sql}");
        assert!(!program.column_parallel_safe(&schema), "{sql}");
        assert!(program.try_eval_column(&batch).is_none(), "{sql}");
    }
    for sql in ["IS_UUID(x)", "IS_IPV4(x)", "IS_IPV6(x)"] {
        let (program, schema) = compile(sql, ColumnType::VarString);
        assert!(program.column_parallel_safe(&schema), "{sql}");
    }
    for sql in ["x%0", "x%(-1)"] {
        let (program, schema) = compile(sql, ColumnType::LongLong);
        assert!(!program.column_parallel_safe(&schema), "{sql}");
        let batch = Batch::new(
            schema,
            vec![ColumnVector::from_values(vec![Value::Int(i64::MIN)])],
        );
        assert!(program.try_eval_column(&batch).is_none(), "{sql}");
    }
}

#[test]
fn padding_columns_preserve_unicode_truncation_empty_pad_and_packet_fallback() {
    let values = ["", "🙂中é", "ascii", "a\0b"]
        .into_iter()
        .map(|s| Value::String(s.into()))
        .chain([Value::Null])
        .collect::<Vec<_>>();
    for sql in [
        "LPAD(x,0,'🙂')",
        "RPAD(x,2,'文🙂')",
        "LPAD(x,9,'文🙂')",
        "RPAD(x,9,'')",
    ] {
        parity(sql, ColumnType::VarString, values.clone());
    }
    let (program, schema) = compile("LPAD(x,1000000000,'🙂')", ColumnType::VarString);
    let batch = Batch::new(schema, vec![ColumnVector::from_values(values)]);
    assert!(program.try_eval_column(&batch).is_none());
}

#[test]
fn large_covering_scan_batches_keep_composed_column_execution_bounded() {
    let values = (0..32768)
        .map(|n| {
            if n % 31 == 0 {
                Value::Null
            } else {
                Value::Int(n)
            }
        })
        .collect::<Vec<_>>();
    for sql in [
        "IS_UUID(CONCAT('00112233-4455-6677-8899-',LPAD(CAST(x AS CHAR),12,'0')))",
        "HEX(UUID_TO_BIN(CONCAT('00112233-4455-6677-8899-',LPAD(CAST(x AS CHAR),12,'0'))))",
        "BIN_TO_UUID(UUID_TO_BIN(CONCAT('00112233-4455-6677-8899-',LPAD(CAST(x AS CHAR),12,'0'))))",
        "HEX(INET6_ATON(CONCAT('2001:db8::',x%100)))",
    ] {
        let (program, schema) = compile(sql, ColumnType::LongLong);
        let batch = Batch::new(schema, vec![ColumnVector::from_values(values.clone())]);
        let (_, work) = workspace(&program.root, &batch).unwrap();
        assert!(
            work > crate::engine::executor::vec::evaluated_batch::byte_target() * 2,
            "test must exercise morsels: {sql}"
        );
        assert!(program.column_output_bound(&batch).is_some(), "{sql}");
        let result = program
            .try_eval_column(&batch)
            .unwrap_or_else(|| panic!("large batch declined: {sql}"));
        let expected = oracle(&program, &batch).unwrap();
        assert_eq!(
            (0..batch.num_rows())
                .map(|r| result.value_at(r))
                .collect::<Vec<_>>(),
            expected,
            "{sql}"
        );
    }
}

#[test]
fn bound_metadata_marker_folds_without_retaining_payload_columns() {
    for (sql, expected) in [
        ("CHARSET(__COLLATED_CONST(x,'latin1_bin',2))", "latin1"),
        (
            "COLLATION(__COLLATED_CONST(x,'latin1_bin',2))",
            "latin1_bin",
        ),
    ] {
        let (program, _) = compile(sql, ColumnType::VarString);
        assert!(
            matches!(&program.root, ExprNode::Const(Value::String(s)) if s==expected),
            "{sql}: {:?}",
            program.root
        );
        assert!(program.referenced_columns().is_empty(), "{sql}");
    }
}

#[test]
fn minimum_modulus_keeps_the_full_remainder_range_and_overflow_order() {
    let (program, schema) = compile("(x % (-9223372036854775808)) + 1", ColumnType::LongLong);
    assert!(!program.column_parallel_safe(&schema));
    let batch = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![Value::Int(i64::MAX)])],
    );
    assert!(program.try_eval_column(&batch).is_none());
    parity(
        "x % (-9223372036854775808)",
        ColumnType::LongLong,
        vec![
            Value::Int(i64::MIN),
            Value::Int(-i64::MAX),
            Value::Int(i64::MAX),
            Value::Int(0),
            Value::Null,
        ],
    );
}

fn json_value(text: &str) -> Value {
    Value::Json(crate::engine::json::parse_text(text, 1, "test").unwrap())
}

#[test]
fn typed_json_columns_preserve_nulls_numbers_strings_containers_and_composition() {
    let mut values = vec![Value::Null];
    for text in [
        "null",
        "true",
        "false",
        "-9223372036854775808",
        "18446744073709551615",
        "123.456700",
        "1e-308",
        r#""escaped\n文🙂\u0000""#,
        "[]",
        "{}",
        r#"{"a":null,"b":[1,2]}"#,
        r#"{"a":"文🙂","b":[{},true],"a.b":42}"#,
        r#"{"a":{"nested":[1,2]},"b":[]}"#,
        "[1,null,{},\"text\"]",
    ] {
        values.push(json_value(text));
    }
    for value in [
        Value::Date(0),
        Value::DateTime(1234567),
        Value::Time(-1234567),
        Value::DecimalCompact(12345, 3),
    ] {
        values.push(Value::Json(crate::engine::json::scalar_from_value(&value)));
    }
    for ty in [15, 16, 249, 255] {
        values.push(Value::Json(crate::engine::json::opaque_scalar(
            ty,
            &[0, 255, 128, 1],
        )));
    }
    for sql in [
        "JSON_EXTRACT(x,'$')",
        "JSON_EXTRACT(x,'$.a')",
        "JSON_EXTRACT(JSON_EXTRACT(x,'$.a'),'$[0]')",
        "x->'$.a'",
        "x->>'$.a'",
        "JSON_UNQUOTE(x)",
        "JSON_TYPE(x)",
        "JSON_LENGTH(x)",
        "JSON_LENGTH(x,'$.a')",
        "JSON_VALID(x)",
        "JSON_TYPE(JSON_EXTRACT(x,'$.a'))",
        "JSON_UNQUOTE(JSON_EXTRACT(x,'$.a'))",
        "LENGTH(JSON_UNQUOTE(JSON_EXTRACT(x,'$.a')))",
        "REVERSE(JSON_UNQUOTE(JSON_EXTRACT(x,'$.a')))",
    ] {
        parity(sql, ColumnType::Json, values.clone());
    }
    for sql in [
        "JSON_EXTRACT(x,'$.a')",
        "x->>'$.a'",
        "JSON_TYPE(x)",
        "JSON_LENGTH(x)",
        "JSON_VALID(x)",
    ] {
        parity(sql, ColumnType::Json, vec![Value::Null; 3]);
    }
}

#[test]
fn typed_json_selected_rows_skip_corruption_and_keep_duplicate_indices() {
    let values = vec![
        Value::Json(vec![0xff]),
        json_value(r#"{"a":"文🙂"}"#),
        json_value(r#"{"a":null}"#),
        Value::Null,
    ];
    for sql in [
        "JSON_EXTRACT(x,'$.a')",
        "x->>'$.a'",
        "JSON_TYPE(x)",
        "JSON_LENGTH(x)",
        "JSON_VALID(x)",
    ] {
        let (program, schema) = compile(sql, ColumnType::Json);
        let batch = Batch::new(schema, vec![ColumnVector::from_values(values.clone())])
            .with_selection(crate::engine::executor::vec::batch::Selection::Indices(
                vec![1, 2, 1, 3],
            ));
        let column = program
            .try_eval_column(&batch)
            .unwrap_or_else(|| panic!("{sql}"));
        assert_eq!(column.value_at(0), Value::Null);
        for row in batch.visible_indices() {
            let input = [values[row].clone()];
            let expected = with_eval_scratch(|scratch| {
                program.eval_row(&RowView::from_values(&input), scratch)
            })
            .unwrap();
            assert_eq!(column.value_at(row), expected, "{sql}, row={row}");
        }
    }
}

#[test]
fn json_path_parameters_rebind_and_unsafe_grammars_retain_ordered_oracle() {
    let (program, schema) = compile("JSON_UNQUOTE(JSON_EXTRACT(x,?))", ColumnType::Json);
    let batch = Batch::new(
        schema.clone(),
        vec![ColumnVector::from_values(vec![
            json_value(r#"{"a":1,"b":[2,3]}"#),
            Value::Null,
        ])],
    );
    for path in [
        "$.a",
        "$.b[last]",
        "$",
        "$.missing",
        "$.*",
        "invalid",
        "$.a",
    ] {
        crate::engine::params::with_params(vec![Value::String(path.into())], || {
            let concrete = path != "$.*" && path != "invalid";
            assert_eq!(program.column_parallel_safe(&schema), concrete, "{path}");
            if concrete {
                let column = program.try_eval_column(&batch).unwrap();
                assert_eq!(
                    (0..2).map(|row| column.value_at(row)).collect::<Vec<_>>(),
                    oracle(&program, &batch).unwrap()
                );
            } else {
                assert!(program.try_eval_column(&batch).is_none());
            }
        });
    }
    for sql in [
        "JSON_EXTRACT(x,'$.*')",
        "JSON_EXTRACT(x,'$','$.a')",
        "JSON_EXTRACT(x,NULL)",
        "JSON_EXTRACT(x,'bad')",
    ] {
        let (program, schema) = compile(sql, ColumnType::Json);
        assert!(!program.column_parallel_safe(&schema), "{sql}");
        assert!(
            program
                .try_eval_column(&Batch::new(
                    schema,
                    vec![ColumnVector::from_values(vec![json_value("null")])]
                ))
                .is_none(),
            "{sql}"
        );
    }
    let (program, schema) = compile("JSON_EXTRACT(x,'$.a')", ColumnType::VarString);
    assert!(!program.has_column_contract(&schema));
}

#[test]
fn json_column_output_budget_declines_large_scalars_without_poisoning_query() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let (program, schema) = compile("JSON_UNQUOTE(x)", ColumnType::Json);
    let value = json_value(&serde_json::to_string(&"x".repeat(256 * 1024)).unwrap());
    let batch = Batch::new(schema, vec![ColumnVector::from_values(vec![value])]);
    let budget = Arc::new(MemBudget::new(64 * 1024));
    with_query_budget(budget.clone(), || {
        assert!(program.try_eval_column(&batch).is_none());
        assert!(budget.terminal_oom().is_none());
        crate::engine::cancel::check_cancelled().unwrap();
    });
}

#[test]
fn json_additional_reads_keep_container_scalar_opaque_and_sql_null_semantics() {
    let values = vec![
        Value::Null,
        json_value("null"),
        json_value("true"),
        json_value("1"),
        json_value("1.0"),
        json_value(r#""1""#),
        json_value("[]"),
        json_value("[1,2]"),
        json_value("[[1,2]]"),
        json_value("{}"),
        json_value(r#"{"a":1,"b":[1,2],"other":{}}"#),
        json_value(r#"{"a":null,"b":false}"#),
        json_value(r#"{"a":[1,2],"b":{"a":1}}"#),
        Value::Json(crate::engine::json::opaque_scalar(255, &[0, 255, 128])),
    ];
    for sql in [
        "JSON_KEYS(x)",
        "JSON_KEYS(x,'$.b')",
        "JSON_STORAGE_SIZE(x)",
        "JSON_STORAGE_FREE(x)",
        "JSON_CONTAINS_PATH(x,'one','$.a','$.b')",
        "JSON_CONTAINS_PATH(x,'all','$.a','$.b')",
        "JSON_CONTAINS_PATH(x,'one','$')",
        "JSON_OVERLAPS(x,'null')",
        "JSON_OVERLAPS(x,'1')",
        "JSON_OVERLAPS(x,'1.0')",
        "JSON_OVERLAPS(x,'[1,2]')",
        "JSON_OVERLAPS(x,'[[2,3]]')",
        "JSON_OVERLAPS(x,'{\"a\":1}')",
        "JSON_OVERLAPS(x,'{\"a\":[1,2]}')",
    ] {
        parity(sql, ColumnType::Json, values.clone());
    }
    let vals = vec![
        Value::Null,
        Value::String("".into()),
        Value::String("文🙂\0\n\"\\".into()),
        Value::String("123.000".into()),
    ];
    parity("JSON_QUOTE(x)", ColumnType::VarString, vals);
}

#[test]
fn json_value_integer_lane_defers_conversions_defaults_and_ast_sensitive_paths() {
    let values = vec![
        Value::Null,
        json_value("null"),
        json_value("true"),
        json_value("false"),
        json_value("-9223372036854775808"),
        json_value("18446744073709551615"),
        json_value("0"),
    ];
    for sql in [
        "JSON_VALUE(x,'$' RETURNING SIGNED)",
        "JSON_VALUE(x,'$' RETURNING UNSIGNED)",
    ] {
        parity(sql, ColumnType::Json, values.clone());
        let (program, schema) = compile(sql, ColumnType::Json);
        assert!(!program.column_parallel_safe(&schema));
    }
    for sql in [
        "JSON_VALUE(x,'$' RETURNING DECIMAL(65,30))",
        "JSON_VALUE(x,'$' RETURNING UNSIGNED DEFAULT 1 ON ERROR)",
    ] {
        let (program, schema) = compile(sql, ColumnType::Json);
        assert!(!program.has_column_contract(&schema));
    }
    let (program, schema) = compile("JSON_VALUE(x,'$' RETURNING UNSIGNED)", ColumnType::Json);
    for value in [json_value("\"123\""), json_value("1.5"), json_value("[]")] {
        assert!(program
            .try_eval_column(&Batch::new(
                schema.clone(),
                vec![ColumnVector::from_values(vec![value])]
            ))
            .is_none());
    }
}

#[test]
fn json_primitive_constructors_keep_full_unsigned_strings_and_duplicate_keys() {
    for values in [
        vec![
            Value::Null,
            Value::Int(i64::MIN),
            Value::Int(0),
            Value::Int(i64::MAX),
        ],
        vec![
            Value::Null,
            Value::UInt(0),
            Value::UInt(i64::MAX as u64 + 1),
            Value::UInt(u64::MAX),
        ],
    ] {
        for sql in ["JSON_ARRAY(x,x)", "JSON_OBJECT('long',x,'a',x,'long',x)"] {
            parity(sql, ColumnType::LongLong, values.clone());
        }
    }
    for sql in ["JSON_ARRAY(x)", "JSON_OBJECT('',x,'文🙂',x,'a',x,'a',x)"] {
        parity(
            sql,
            ColumnType::VarString,
            vec![
                Value::Null,
                Value::String(String::new()),
                Value::String("文🙂\0\n\"\\".into()),
            ],
        );
    }
    // Rich descriptors and syntax-sensitive literals keep the scalar oracle.
    for (sql, kind) in [
        ("JSON_ARRAY(TRUE,x)", ColumnType::LongLong),
        ("JSON_ARRAY(x)", ColumnType::Double),
        (
            "JSON_OBJECT('a',x)",
            ColumnType::Decimal {
                precision: 65,
                scale: 30,
            },
        ),
        ("JSON_ARRAY(x)", ColumnType::Date),
        ("JSON_ARRAY(x)", ColumnType::Json),
    ] {
        let (program, schema) = compile(sql, kind);
        assert!(!program.has_column_contract(&schema), "{sql}/{kind:?}");
    }
    for flag in [
        crate::mysql::protocol::ENUM_FLAG,
        crate::mysql::protocol::SET_FLAG,
        crate::mysql::protocol::BINARY_FLAG,
    ] {
        let (_, schema) = compile("x", ColumnType::VarString);
        let mut field = schema.fields()[0].clone();
        field.source.flags |= flag;
        let schema = Arc::new(BatchSchema::new(vec![field]));
        let ast = crate::parser::parse_expr_fragment("JSON_ARRAY(x)").unwrap();
        let program = ExprCompiler::compile(
            &ast,
            &crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema),
        )
        .unwrap();
        assert!(!program.has_column_contract(&schema));
    }
}

#[test]
fn json_pretty_and_first_search_preserve_paths_escapes_types_and_nulls() {
    let mut values = vec![
        Value::Null,
        json_value("null"),
        json_value("true"),
        json_value("18446744073709551615"),
        json_value(
            r#"{"a":["needle",{"a.b":"a%b","quote\"\\":"a_b"}],"中文":"needle","empty":{}}"#,
        ),
        json_value(r#""文🙂\u0000\n\\\"""#),
        Value::Json(crate::engine::json::opaque_scalar(255, &[0, 255, 128])),
    ];
    for value in [
        Value::Date(0),
        Value::Time(-1234567),
        Value::DateTime(1234567),
        Value::DecimalCompact(12345, 3),
    ] {
        values.push(Value::Json(crate::engine::json::scalar_from_value(&value)));
    }
    for sql in [
        "JSON_PRETTY(x)",
        "LENGTH(JSON_PRETTY(x))",
        "JSON_SEARCH(x,'one','%')",
        "JSON_SEARCH(x,'ONE','needle',NULL)",
        r"JSON_SEARCH(x,'one','a!%b','!')",
        "JSON_SEARCH(x,'one','a_b','')",
    ] {
        parity(sql, ColumnType::Json, values.clone());
    }
    for sql in [
        "JSON_SEARCH(x,'all','%')",
        "JSON_SEARCH(x,'one','%',NULL,'$.a')",
        "JSON_SEARCH(x,'one',x)",
        "JSON_SEARCH(x,'one','%','ab')",
    ] {
        let (program, schema) = compile(sql, ColumnType::Json);
        assert!(!program.has_column_contract(&schema), "{sql}");
    }
}

#[test]
fn borrowed_json_extract_matches_scalar_error_order_wrap_depth_and_multiple_paths() {
    let mut docs = vec![
        Value::Null,
        json_value("null"),
        json_value(r#"{"a":[1,{"a":2}],"b":{"a":3},"s":"文🙂"}"#),
        json_value("[1,2,3]"),
        Value::String("invalid".into()),
        Value::Bytes(b"null".to_vec()),
    ];
    let mut deep = serde_json::json!(0);
    for _ in 0..99 {
        deep = serde_json::json!({"a":deep});
    }
    docs.push(Value::Json(crate::engine::json::from_serde(&deep)));
    for paths in [
        vec!["$**.a"],
        vec!["$.*"],
        vec!["$[0 to 0]"],
        vec!["$.a", "$.b", "$.a"],
        vec!["bad"],
        vec!["$.missing"],
    ] {
        let sql = format!(
            "JSON_EXTRACT(x,{})",
            paths
                .iter()
                .map(|p| format!("'{p}'"))
                .collect::<Vec<_>>()
                .join(",")
        );
        let (program, schema) = compile(&sql, ColumnType::Json);
        for doc in &docs {
            let mut args = vec![Expr::BoundValue(doc.clone())];
            args.extend(
                paths
                    .iter()
                    .map(|p| Expr::BoundValue(Value::String((*p).into()))),
            );
            let expected =
                crate::engine::expr::eval_function("JSON_EXTRACT", &args, |arg| match arg {
                    Expr::BoundValue(v) => Ok(v.clone()),
                    _ => unreachable!(),
                });
            let batch = Batch::new(
                schema.clone(),
                vec![ColumnVector::from_values(vec![doc.clone()])],
            );
            let actual = oracle(&program, &batch).map(|mut values| values.remove(0));
            match (actual, expected) {
                (Ok(a), Ok(b)) => assert_eq!(a, b, "{sql}/{doc:?}"),
                (Err(a), Err(b)) => {
                    assert_eq!(a, b, "{sql}");
                }
                (a, b) => panic!("{sql}: {a:?} != {b:?}"),
            }
        }
    }
}

#[test]
fn temporal_reads_match_scalar_across_calendar_boundaries_and_fractional_seconds() {
    let days = [
        (0, 1, 1),
        (0, 2, 28),
        (0, 3, 1),
        (1582, 10, 15),
        (1900, 2, 28),
        (1969, 12, 31),
        (1970, 1, 1),
        (1999, 12, 31),
        (2000, 2, 29),
        (2020, 12, 31),
        (2021, 1, 1),
        (9999, 12, 31),
    ];
    let mut dates = vec![Value::Null];
    let mut datetimes = vec![Value::Null];
    for (y, m, d) in days {
        let date = chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap();
        dates.push(Value::Date(date.num_days_from_ce() - 719163));
        for (h, mi, s, us) in [(0, 0, 0, 0), (12, 34, 56, 123456), (23, 59, 59, 999999)] {
            datetimes.push(Value::DateTime(
                date.and_hms_micro_opt(h, mi, s, us)
                    .unwrap()
                    .and_utc()
                    .timestamp_micros(),
            ));
        }
    }
    for sql in [
        "DATE(x)",
        "TIME(x)",
        "TIMESTAMP(x)",
        "DAYOFWEEK(x)",
        "DAYOFYEAR(x)",
        "WEEKDAY(x)",
        "WEEK(x)",
        "WEEKOFYEAR(x)",
        "YEARWEEK(x)",
        "TO_DAYS(x)",
        "TO_SECONDS(x)",
        "LAST_DAY(x)",
        "DAYNAME(x)",
        "MONTHNAME(x)",
        "MICROSECOND(x)",
        "TIME_TO_SEC(x)",
    ] {
        parity(sql, ColumnType::Date, dates.clone());
        parity(sql, ColumnType::DateTime, datetimes.clone());
    }
    for sql in ["TIME(x)", "TIME_TO_SEC(x)", "MICROSECOND(x)"] {
        parity(
            sql,
            ColumnType::Time,
            vec![
                Value::Null,
                Value::Time(-3_020_399_000_000),
                Value::Time(-86400123456),
                Value::Time(-1),
                Value::Time(0),
                Value::Time(1234567),
                Value::Time(3_020_399_000_000),
            ],
        );
    }
}

#[test]
fn temporal_read_programs_rebind_week_modes_locales_and_timestamp_time_zones() {
    let mut session = SessionContext::default();
    let programs = [
        "DAYNAME(x)",
        "MONTHNAME(x)",
        "WEEK(x)",
        "WEEKOFYEAR(x)",
        "YEARWEEK(x)",
        "DATE(x)",
        "TIME(x)",
        "TIMESTAMP(x)",
        "TO_SECONDS(x)",
    ]
    .into_iter()
    .map(|sql| (sql, compile(sql, ColumnType::Timestamp)))
    .collect::<Vec<_>>();
    for locale in ["en_US", "zh_CN", "fr_FR", "de_DE", "ja_JP"] {
        session.sysvars.lc_time_names = locale.into();
        for zone in ["+00:00", "+08:00", "-12:00", "America/New_York"] {
            session.sysvars.time_zone = zone.into();
            for mode in 0..8 {
                session.sysvars.default_week_format = mode;
                crate::engine::expr::with_session_expr_info(&session, || {
                    for (sql, (program, schema)) in &programs {
                        let batch = Batch::new(
                            schema.clone(),
                            vec![ColumnVector::from_values(vec![
                                Value::Null,
                                Value::Timestamp(-1),
                                Value::Timestamp(1609459199123456),
                                Value::Timestamp(1710053999999999),
                                Value::Timestamp(1730613599123456),
                            ])],
                        );
                        let expected = oracle(program, &batch).unwrap();
                        let column = program
                            .try_eval_column(&batch)
                            .unwrap_or_else(|| panic!("{sql}/{locale}/{zone}/{mode}"));
                        assert_eq!(
                            (0..batch.num_rows())
                                .map(|r| column.value_at(r))
                                .collect::<Vec<_>>(),
                            expected,
                            "{sql}/{locale}/{zone}/{mode}"
                        );
                        assert!(!program.column_parallel_safe(schema));
                    }
                    for explicit in [
                        -9223372036854775808i64,
                        -1,
                        0,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9223372036854775807,
                    ] {
                        for name in ["WEEK", "YEARWEEK"] {
                            parity(
                                &format!("{name}(x,{explicit})"),
                                ColumnType::DateTime,
                                vec![Value::DateTime(1609459199123456), Value::Null],
                            );
                        }
                    }
                });
            }
        }
    }
}

#[test]
fn temporal_reads_decline_diagnostic_sources_and_skip_invisible_invalid_rows() {
    for sql in [
        "DATE(x)",
        "TIME(x)",
        "TIMESTAMP(x)",
        "DAYOFWEEK(x)",
        "DAYOFYEAR(x)",
        "WEEKDAY(x)",
        "WEEK(x)",
        "WEEKOFYEAR(x)",
        "YEARWEEK(x)",
        "TO_DAYS(x)",
        "TO_SECONDS(x)",
        "LAST_DAY(x)",
        "DAYNAME(x)",
        "MONTHNAME(x)",
        "MICROSECOND(x)",
        "TIME_TO_SEC(x)",
    ] {
        let (program, schema) = compile(sql, ColumnType::DateTime);
        for value in [
            crate::types::ZERO_DATETIME,
            crate::types::encode_tagged_datetime(2020, 0, 1, 12, 34, 56, 123456).unwrap(),
            i64::MAX,
        ] {
            let batch = Batch::new(
                schema.clone(),
                vec![ColumnVector::from_values(vec![
                    Value::DateTime(0),
                    Value::DateTime(value),
                    Value::Null,
                ])],
            );
            assert!(program.try_eval_column(&batch).is_none(), "{sql}/{value}");
            let selected = batch.with_selection(
                crate::engine::executor::vec::batch::Selection::Indices(vec![0, 2, 0]),
            );
            let column = program.try_eval_column(&selected).unwrap();
            let expected = oracle(
                &program,
                &Batch::new(
                    schema.clone(),
                    vec![ColumnVector::from_values(vec![Value::DateTime(0)])],
                ),
            )
            .unwrap()
            .remove(0);
            assert_eq!(column.value_at(0), expected, "{sql}");
            assert_eq!(column.value_at(1), Value::Null, "{sql}");
            assert_eq!(column.value_at(2), Value::Null, "{sql}");
        }
        let (program, schema) = compile(sql, ColumnType::VarString);
        assert!(!program.has_column_contract(&schema), "{sql}");
    }
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let (program, schema) = compile("MONTHNAME(x)", ColumnType::DateTime);
    let batch = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![Value::DateTime(0); 4096])],
    );
    let budget = Arc::new(MemBudget::new(1024));
    with_query_budget(budget.clone(), || {
        assert!(program.try_eval_column(&batch).is_none());
        assert!(budget.terminal_oom().is_none());
        crate::engine::cancel::check_cancelled().unwrap();
    });
    assert_eq!(budget.used(), 0);
}

fn typed_binary_values(values: Vec<Value>) -> ColumnVector {
    let mut data = Vec::new();
    let mut offsets = Vec::with_capacity(values.len() + 1);
    offsets.push(0);
    let mut nulls = Bitmap::new_all_valid(values.len());
    for (row, value) in values.iter().enumerate() {
        match value {
            Value::Null => nulls.set(row, false),
            Value::Bytes(bytes) => data.extend_from_slice(bytes),
            _ => panic!("binary fixture requires bytes"),
        }
        offsets.push(data.len() as u32);
    }
    ColumnVector::Bytes {
        data,
        offsets,
        nulls: Some(nulls),
    }
}

fn checked_legacy_column(sql: &str, schema: Arc<BatchSchema>, columns: Vec<ColumnVector>) {
    let ast = crate::parser::parse_expr_fragment(sql).unwrap();
    let input = crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema);
    let program =
        ExprCompiler::compile(&ast, &input).unwrap_or_else(|e| panic!("compile {sql}: {e:?}"));
    let legacy = ExprCompiler::compile_legacy_oracle(&ast, &input).unwrap();
    let batch = Batch::new(schema, columns);
    let expected = oracle(&legacy, &batch).unwrap();
    assert_eq!(
        oracle(&program, &batch).unwrap(),
        expected,
        "compiled scalar: {sql}"
    );
    let output = program
        .try_eval_column(&batch)
        .unwrap_or_else(|| panic!("native declined {sql}"));
    assert_eq!(
        (0..batch.num_rows())
            .map(|r| output.value_at(r))
            .collect::<Vec<_>>(),
        expected,
        "column: {sql}"
    );
}

#[test]
fn exact_decimal_columns_keep_rounding_scale_sign_and_no_float_conversion() {
    let values = vec![
        Value::Null,
        Value::DecimalCompact(0, 6),
        Value::DecimalCompact(1, 6),
        Value::DecimalCompact(-1, 6),
        Value::DecimalCompact(500000, 6),
        Value::DecimalCompact(-500000, 6),
        Value::DecimalCompact(1999999, 6),
        Value::DecimalCompact(-1999999, 6),
        Value::Decimal128(9_999_999_999_999_999_999, 6),
        Value::Decimal128(-9_999_999_999_999_999_999, 6),
    ];
    let (_, schema) = compile(
        "x",
        ColumnType::Decimal {
            precision: 20,
            scale: 6,
        },
    );
    for sql in ["FLOOR(x)", "CEIL(x)", "CEILING(x)"] {
        checked_legacy_column(
            sql,
            schema.clone(),
            vec![ColumnVector::from_values(values.clone())],
        );
    }
    for places in [
        -2147483649i64,
        -38,
        -8,
        -1,
        0,
        1,
        2,
        6,
        30,
        2147483648,
        i64::MAX,
    ] {
        for name in ["ROUND", "TRUNCATE"] {
            checked_legacy_column(
                &format!("{name}(x,{places})"),
                schema.clone(),
                vec![ColumnVector::from_values(values.clone())],
            );
        }
    }
    let vals = vec![Value::Null, Value::DecimalCompact(0, 6), Value::Null];
    checked_legacy_column(
        "ROUND(x,2)",
        schema.clone(),
        vec![ColumnVector::from_values(vals)],
    );
    for name in ["ROUND", "TRUNCATE"] {
        let sql = format!("{name}(x,?)");
        let ast = crate::parser::parse_expr_fragment(&sql).unwrap();
        let input = crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema);
        let program = ExprCompiler::compile(&ast, &input).unwrap();
        let legacy = ExprCompiler::compile_legacy_oracle(&ast, &input).unwrap();
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(values.clone())],
        );
        for p in [
            Value::Int(-3),
            Value::Int(0),
            Value::Int(2),
            Value::UInt(u64::MAX),
            Value::Int(6),
        ] {
            for auto in [false, true] {
                let check = || {
                    let expected = oracle(&legacy, &batch).unwrap();
                    let actual = program.try_eval_column(&batch).unwrap();
                    assert_eq!(
                        (0..batch.num_rows())
                            .map(|r| actual.value_at(r))
                            .collect::<Vec<_>>(),
                        expected,
                        "{name}/{p:?}/auto={auto}"
                    );
                };
                if auto {
                    crate::engine::params::with_auto_params(vec![p.clone()], check);
                } else {
                    crate::engine::params::with_params(vec![p.clone()], check);
                }
            }
        }
    }
}

#[test]
fn numeric_integer_columns_preserve_full_unsigned_range_and_signed_extremes() {
    let (_, schema) = compile("x", ColumnType::LongLong);
    let mut field = schema.fields()[0].clone();
    field.source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
    let unsigned_schema = Arc::new(BatchSchema::new(vec![field]));
    let unsigned = vec![
        Value::Null,
        Value::UInt(0),
        Value::UInt(1),
        Value::UInt(i64::MAX as u64),
        Value::UInt(i64::MAX as u64 + 1),
        Value::UInt(u64::MAX),
    ];
    for sql in [
        "BIN(x)",
        "OCT(x)",
        "HEX(x)",
        "BIT_COUNT(x)",
        "CAST(x AS CHAR)",
        "FLOOR(x)",
        "CEIL(x)",
        "ROUND(x,2)",
        "TRUNCATE(x,0)",
    ] {
        checked_legacy_column(
            sql,
            unsigned_schema.clone(),
            vec![ColumnVector::from_values(unsigned.clone())],
        );
    }
    let signed = vec![
        Value::Null,
        Value::Int(i64::MIN),
        Value::Int(-1),
        Value::Int(0),
        Value::Int(1),
        Value::Int(i64::MAX),
    ];
    for sql in [
        "BIN(x)",
        "OCT(x)",
        "HEX(x)",
        "BIT_COUNT(x)",
        "ROUND(x,0)",
        "TRUNCATE(x,2)",
        "MOD(x,7)",
        "MOD(x,-7)",
        "MOD(x,-9223372036854775808)",
    ] {
        checked_legacy_column(
            sql,
            schema.clone(),
            vec![ColumnVector::from_values(signed.clone())],
        );
    }
    let (program, schema) = compile(
        "ROUND(x,-38)",
        ColumnType::Decimal {
            precision: 38,
            scale: 0,
        },
    );
    let batch = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![Value::Decimal128(
            i128::MAX,
            0,
        )])],
    );
    assert!(program.try_eval_column(&batch).is_none());
    let (program, schema) = compile("MOD(x,0)", ColumnType::LongLong);
    assert!(!program.has_column_contract(&schema));
}

#[test]
fn decimal_character_columns_keep_scale_full_coefficients_nulls_and_selection() {
    for scale in [0, 1, 6, 30, 38, 255] {
        let (program, schema) = compile(
            "CAST(x AS CHAR)",
            ColumnType::Decimal {
                precision: 65,
                scale,
            },
        );
        let values = vec![
            Value::Null,
            Value::Decimal128(0, scale),
            Value::Decimal128(1, scale),
            Value::Decimal128(-1, scale),
            Value::Decimal128(i128::MIN, scale),
            Value::Decimal128(i128::MAX, scale),
            Value::Decimal128(15000, scale),
        ];
        checked_legacy_column(
            "CAST(x AS CHAR)",
            schema.clone(),
            vec![ColumnVector::from_values(values)],
        );
        let selected = Batch::new(
            schema.clone(),
            vec![ColumnVector::Decimal128 {
                data: vec![i128::MIN, 0, i128::MAX],
                scale,
                nulls: None,
            }],
        )
        .with_selection(crate::engine::executor::vec::batch::Selection::Indices(
            vec![2, 0, 2],
        ));
        let actual = program.try_eval_column(&selected).unwrap();
        for row in [0, 2] {
            assert_eq!(
                actual.value_at(row).to_display_string(),
                crate::util::decimal::format_decimal_parts(
                    if row == 0 { i128::MIN } else { i128::MAX },
                    scale
                )
            );
        }
    }
    // Width-limited casts retain their truncation diagnostics in the oracle.
    let (program, schema) = compile(
        "CAST(x AS CHAR(2))",
        ColumnType::Decimal {
            precision: 20,
            scale: 6,
        },
    );
    assert!(!program.has_column_contract(&schema));
}

#[test]
fn statement_temporal_columns_rebind_clock_zone_precision_and_keep_invalid_fallback() {
    let programs = [
        "CURDATE()",
        "CURRENT_DATE()",
        "NOW(6)",
        "CURRENT_TIMESTAMP(3)",
        "CURTIME(2)",
        "UTC_TIMESTAMP(5)",
        "UTC_DATE()",
        "UTC_TIME(4)",
        "NOW(?)",
    ]
    .into_iter()
    .map(|sql| (sql, compile(sql, ColumnType::LongLong)))
    .collect::<Vec<_>>();
    let mut session = SessionContext::default();
    for timestamp in [1609459199.123456, 1710053999.654321, 1730613599.999999] {
        session.sysvars.timestamp_override = timestamp;
        for zone in ["+00:00", "+08:00", "America/New_York"] {
            session.sysvars.time_zone = zone.into();
            for precision in [0, 3, 6] {
                crate::engine::api::clear_statement_now_freeze();
                crate::engine::expr::with_session_expr_info(&session, || {
                    crate::engine::params::with_params(vec![Value::Int(precision)], || {
                        for (sql, (program, schema)) in &programs {
                            let batch = Batch::new(
                                schema.clone(),
                                vec![ColumnVector::from_values(vec![Value::Int(1); 7])],
                            );
                            let expected = oracle(program, &batch).unwrap();
                            let actual = program
                                .try_eval_column(&batch)
                                .unwrap_or_else(|| panic!("{sql}"));
                            assert_eq!(
                                (0..7).map(|r| actual.value_at(r)).collect::<Vec<_>>(),
                                expected,
                                "{sql}/{timestamp}/{zone}/{precision}"
                            );
                        }
                    });
                });
            }
        }
    }
    for sql in [
        "NOW(7)",
        "CURTIME(-1)",
        "UTC_TIME('invalid')",
        "NOW(x)",
        "SYSDATE()",
    ] {
        let (program, schema) = compile(sql, ColumnType::LongLong);
        assert!(!program.has_column_contract(&schema), "{sql}");
    }
    session.sysvars.timestamp_override = 253402290000.0;
    session.sysvars.time_zone = "+14:00".into();
    crate::engine::api::clear_statement_now_freeze();
    crate::engine::expr::with_session_expr_info(&session, || {
        let (program, schema) = compile("NOW(6)", ColumnType::LongLong);
        let batch = Batch::new(
            schema,
            vec![ColumnVector::from_values(vec![Value::Int(1); 3])],
        );
        crate::engine::api::clear_statement_warnings();
        assert!(program.try_eval_column(&batch).is_none());
        assert_eq!(crate::engine::api::statement_warning_count(), 0);
        let result = oracle(&program, &batch).unwrap();
        assert_eq!(result, vec![Value::Null; 3]);
        assert_eq!(crate::engine::api::statement_warning_count(), 3);
        crate::engine::api::clear_statement_warnings();
    });
    crate::engine::api::clear_statement_now_freeze();
}

#[test]
fn bound_coercibility_markers_and_weight_buffers_keep_typed_execution() {
    let (_, source) = compile("x", ColumnType::VarString);
    let mut field = source.fields()[0].clone();
    field.source.collation_id = Some(crate::charset::UTF8MB4_0900_AI_CI_ID);
    let schema = Arc::new(BatchSchema::new(vec![field]));
    for level in [0, 2, 4, 5] {
        checked_legacy_column(
            &format!("COERCIBILITY(__COLLATED_CONST(x,'utf8mb4_bin',{level}))"),
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Null,
                Value::String("aß文".into()),
            ])],
        );
    }
    let mut data = Vec::with_capacity(16 * 1024 * 1024);
    let mut offsets = Vec::with_capacity(8192);
    offsets.push(0);
    for _ in 0..64 {
        data.extend_from_slice("aß文🙂 ".as_bytes());
        offsets.push(data.len() as u32);
    }
    let batch = Batch::new(
        schema.clone(),
        vec![ColumnVector::Utf8 {
            data,
            offsets,
            nulls: None,
        }],
    );
    let ast = crate::parser::parse_expr_fragment("HEX(WEIGHT_STRING(x))").unwrap();
    let input = crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema);
    let program = ExprCompiler::compile(&ast, &input).unwrap();
    let legacy = ExprCompiler::compile_legacy_oracle(&ast, &input).unwrap();
    assert!(program.column_output_bound(&batch).is_some());
    let actual = program.try_eval_column(&batch).unwrap();
    assert_eq!(
        (0..64).map(|r| actual.value_at(r)).collect::<Vec<_>>(),
        oracle(&legacy, &batch).unwrap()
    );
}

#[test]
fn metadata_and_character_wrapper_columns_preserve_nulls_collations_and_comparisons() {
    let (_, source) = compile("x", ColumnType::VarString);
    let values = vec![
        Value::Null,
        Value::String("".into()),
        Value::String("a".into()),
        Value::String("A ".into()),
        Value::String("ß\0\u{301}文🙂\u{fdfa}".into()),
        Value::String("\u{feff}start".into()),
    ];
    for name in [
        "utf8mb4_0900_ai_ci",
        "utf8mb4_bin",
        "utf8mb4_unicode_ci",
        "latin1_swedish_ci",
    ] {
        let mut field = source.fields()[0].clone();
        field.source.collation_id = Some(crate::charset::resolve_collation(name).unwrap().id);
        let schema = Arc::new(BatchSchema::new(vec![field]));
        for sql in [
            "COERCIBILITY(x)",
            "CONVERT(x USING utf8mb4)",
            "CONVERT(x USING binary)",
            "x COLLATE utf8mb4_bin",
        ] {
            checked_legacy_column(
                sql,
                schema.clone(),
                vec![ColumnVector::from_values(values.clone())],
            );
        }
    }
    for collation in ["utf8mb4_0900_ai_ci", "utf8mb4_bin", "utf8mb4_unicode_ci"] {
        for op in ["=", "<>", "<", "<=", ">", ">="] {
            for sql in [
                format!("x COLLATE {collation} {op} 'a'"),
                format!("'a' {op} x COLLATE {collation}"),
            ] {
                checked_legacy_column(
                    &sql,
                    source.clone(),
                    vec![ColumnVector::from_values(values.clone())],
                );
            }
        }
    }
    let mut field = source.fields()[0].clone();
    field.source.collation_id = Some(crate::charset::UTF8MB4_0900_AI_CI_ID);
    let schema = Arc::new(BatchSchema::new(vec![field]));
    for sql in ["WEIGHT_STRING(x)", "HEX(WEIGHT_STRING(x))"] {
        checked_legacy_column(
            sql,
            schema.clone(),
            vec![ColumnVector::from_values(values.clone())],
        );
    }
    // Nested failures and lossy conversions remain on their ordered oracle.
    for sql in ["COERCIBILITY(SUBSTRING(x,1,1))", "CONVERT(x USING utf8mb3)"] {
        let ast = crate::parser::parse_expr_fragment(sql).unwrap();
        let program = ExprCompiler::compile(
            &ast,
            &crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema),
        )
        .unwrap();
        assert!(!program.has_column_contract(&schema), "{sql}");
    }
}

#[test]
fn borrowed_string_columns_match_legacy_unicode_binary_null_and_overlap_rules() {
    let text = vec![
        Value::Null,
        Value::String("".into()),
        Value::String("abc,a,文🙂,aaaa".into()),
        Value::String("a\0'\\\u{1a}\n文🙂".into()),
    ];
    let (_, schema) = compile("x", ColumnType::VarString);
    for sql in [
        "QUOTE(x)",
        "REPEAT(x,2)",
        "REPEAT(x,0)",
        "REPEAT(x,-1)",
        "SUBSTRING_INDEX(x,',',2)",
        "SUBSTRING_INDEX(x,',',-2)",
        "SUBSTRING_INDEX(x,'aa',-1)",
        "SUBSTRING_INDEX(x,'',5)",
        "SUBSTRING_INDEX(x,',',-9223372036854775808)",
        "UNHEX(HEX(x))",
    ] {
        checked_legacy_column(
            sql,
            schema.clone(),
            vec![ColumnVector::from_values(text.clone())],
        );
    }
    let mut field = schema.fields()[0].clone();
    field.column_type = ColumnType::Blob;
    field.source.flags |= crate::mysql::protocol::BINARY_FLAG;
    field.source.collation_id = Some(crate::charset::BINARY_COLLATION_ID);
    let binary_schema = Arc::new(BatchSchema::new(vec![field]));
    let bytes = vec![
        Value::Null,
        Value::Bytes(vec![]),
        Value::Bytes(vec![255, 0, 128, 65, 65, 65]),
        Value::Bytes(b"aaa,aa,,a".to_vec()),
    ];
    for sql in [
        "REPEAT(x,3)",
        "SUBSTRING_INDEX(x,',',-1)",
        "SUBSTRING_INDEX(x,'aa',-1)",
        "UNHEX(HEX(x))",
    ] {
        checked_legacy_column(
            sql,
            binary_schema.clone(),
            vec![typed_binary_values(bytes.clone())],
        );
    }
    checked_legacy_column(
        "UNHEX(x)",
        schema.clone(),
        vec![ColumnVector::from_values(vec![
            Value::Null,
            Value::String("".into()),
            Value::String("F".into()),
            Value::String("AAA".into()),
            Value::String("00FFab01".into()),
        ])],
    );
    let (program, schema) = compile("UNHEX(x)", ColumnType::VarString);
    assert!(program
        .try_eval_column(&Batch::new(
            schema,
            vec![ColumnVector::from_values(vec![Value::String("Ā".into())])]
        ))
        .is_none());
}

#[test]
fn insert_columns_keep_character_positions_binary_octets_and_extreme_extents() {
    let (_, schema) = compile("x", ColumnType::VarString);
    let values = vec![
        Value::Null,
        Value::String("".into()),
        Value::String("a文🙂e\u{301}\0z".into()),
        Value::String("abcdef".into()),
    ];
    for pos in [i64::MIN, -1, 0, 1, 2, 4, 7, i64::MAX] {
        for len in [i64::MIN, -1, 0, 1, 3, i64::MAX] {
            checked_legacy_column(
                &format!("INSERT(x,{pos},{len},'替🙂')"),
                schema.clone(),
                vec![ColumnVector::from_values(values.clone())],
            );
        }
    }
    let mut field = schema.fields()[0].clone();
    field.column_type = ColumnType::Blob;
    field.source.flags |= crate::mysql::protocol::BINARY_FLAG;
    field.source.collation_id = Some(crate::charset::BINARY_COLLATION_ID);
    let binary = Arc::new(BatchSchema::new(vec![field]));
    for sql in [
        "INSERT(x,2,3,'文')",
        "INSERT(x,1,-1,'')",
        "INSERT(x,2,0,'x')",
        "INSERT(x,0,9,'x')",
    ] {
        checked_legacy_column(
            sql,
            binary.clone(),
            vec![typed_binary_values(vec![
                Value::Null,
                Value::Bytes(vec![]),
                Value::Bytes(vec![255, 0, 128, 65, 66]),
            ])],
        );
    }
}

#[test]
fn conv_columns_keep_all_bases_unsigned_limits_and_ordered_diagnostic_fallback() {
    let (_, schema) = compile("x", ColumnType::VarString);
    for from in 2..=36 {
        let mut digits = Vec::new();
        crate::engine::expr::append_conv_digits(u64::MAX, from, &mut digits);
        let maximum = String::from_utf8(digits).unwrap();
        for to in [2, 8, 10, 16, 36, -2, -10, -16, -36] {
            for from in [from, -from] {
                checked_legacy_column(
                    &format!("CONV(x,{from},{to})"),
                    schema.clone(),
                    vec![ColumnVector::from_values(vec![
                        Value::Null,
                        Value::String("0".into()),
                        Value::String(" -1 ".into()),
                        Value::String("+1".into()),
                        Value::String(maximum.clone()),
                        Value::String(format!("-{maximum}")),
                    ])],
                );
            }
        }
    }
    let mut field = schema.fields()[0].clone();
    field.column_type = ColumnType::LongLong;
    field.source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
    checked_legacy_column(
        "CONV(CAST(x AS CHAR),10,16)",
        Arc::new(BatchSchema::new(vec![field])),
        vec![ColumnVector::from_values(vec![
            Value::Null,
            Value::UInt(0),
            Value::UInt(u64::MAX),
        ])],
    );
    for value in ["", " ", "1z", "-", "18446744073709551616", "文"] {
        let (program, schema) = compile("CONV(x,10,16)", ColumnType::VarString);
        let batch = Batch::new(
            schema,
            vec![ColumnVector::from_values(vec![Value::String(value.into())])],
        );
        assert!(program.try_eval_column(&batch).is_none(), "{value}");
    }
    // Selection must not evaluate an unselected diagnostic-producing value.
    let (program, schema) = compile("CONV(x,10,16)", ColumnType::VarString);
    let batch = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![
            Value::String("255".into()),
            Value::String("invalid".into()),
            Value::Null,
        ])],
    )
    .with_selection(crate::engine::executor::vec::batch::Selection::Indices(
        vec![0, 2],
    ));
    let output = program.try_eval_column(&batch).unwrap();
    assert_eq!(output.value_at(0), Value::String("FF".into()));
    assert_eq!(output.value_at(2), Value::Null);
}

#[test]
fn strcmp_columns_keep_collation_padding_expansions_and_nulls() {
    let (_, schema) = compile("x", ColumnType::VarString);
    for collation in [
        "utf8mb4_0900_ai_ci",
        "utf8mb4_general_ci",
        "utf8mb4_bin",
        "utf8mb4_unicode_ci",
        "utf8mb4_0900_as_cs",
        "utf8mb4_turkish_ci",
    ] {
        let mut field = schema.fields()[0].clone();
        field.source.collation_id = Some(crate::charset::resolve_collation(collation).unwrap().id);
        let schema = Arc::new(BatchSchema::new(vec![field]));
        for needle in ["", "a", "a ", "ss", "ß", "É", "中🙂", "İ"] {
            checked_legacy_column(
                &format!("STRCMP(x,'{needle}')"),
                schema.clone(),
                vec![ColumnVector::from_values(vec![
                    Value::Null,
                    Value::String("".into()),
                    Value::String("A ".into()),
                    Value::String("ß".into()),
                    Value::String("e\u{301}".into()),
                    Value::String("中🙂".into()),
                    Value::String("ı".into()),
                ])],
            );
        }
    }
}

#[test]
fn format_columns_keep_exact_rounding_all_locale_groups_and_padding() {
    let (_, schema) = compile(
        "x",
        ColumnType::Decimal {
            precision: 20,
            scale: 6,
        },
    );
    let values = vec![
        Value::Null,
        Value::DecimalCompact(0, 6),
        Value::DecimalCompact(1234567890000, 6),
        Value::DecimalCompact(-2675000, 6),
        Value::DecimalCompact(999999995000, 6),
        Value::DecimalCompact(-1, 6),
    ];
    for locale in [
        "en_US", "de_DE", "ru_RU", "fr_FR", "de_CH", "en_IN", "ta_IN", "ar_SA", " EN_us ",
    ] {
        for decimals in [-1, 0, 2, 6, 30, 99] {
            checked_legacy_column(
                &format!("FORMAT(x,{decimals},'{locale}')"),
                schema.clone(),
                vec![ColumnVector::from_values(values.clone())],
            );
        }
    }
    parity(
        "FORMAT(x,2,'en_IN')",
        ColumnType::LongLong,
        vec![Value::Int(i64::MIN), Value::Int(i64::MAX), Value::Null],
    );
    let mut unsigned = schema.fields()[0].clone();
    unsigned.column_type = ColumnType::LongLong;
    unsigned.source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
    checked_legacy_column(
        "FORMAT(x,2)",
        Arc::new(BatchSchema::new(vec![unsigned])),
        vec![ColumnVector::from_values(vec![
            Value::UInt(u64::MAX),
            Value::UInt(0),
            Value::Null,
        ])],
    );
    let (program, schema) = compile(
        "FORMAT(x,30)",
        ColumnType::Decimal {
            precision: 38,
            scale: 0,
        },
    );
    assert!(program
        .try_eval_column(&Batch::new(
            schema,
            vec![ColumnVector::from_values(vec![Value::Decimal128(
                i128::MAX,
                0
            )])]
        ))
        .is_none());
    for sql in [
        "FORMAT(x,2,'unknown')",
        "FORMAT(x,2,NULL)",
        "FORMAT(x,'bad')",
    ] {
        let (program, schema) = compile(sql, ColumnType::LongLong);
        assert!(!program.has_column_contract(&schema), "{sql}");
    }
    for (locale, expected) in [
        ("en_US", "1,234,567.89"),
        ("en_IN", "12,34,567.89"),
        ("de_DE", "1.234.567,89"),
        ("fr_FR", "1234567,89"),
        ("de_CH", "1'234'567.89"),
    ] {
        let (program, schema) = compile(
            &format!("FORMAT(x,2,'{locale}')"),
            ColumnType::Decimal {
                precision: 20,
                scale: 6,
            },
        );
        let batch = Batch::new(
            schema,
            vec![ColumnVector::from_values(vec![Value::DecimalCompact(
                1234567890000,
                6,
            )])],
        );
        assert_eq!(
            program.try_eval_column(&batch).unwrap().value_at(0),
            Value::String(expected.into())
        );
    }
}

#[test]
fn integer_text_producers_keep_null_skip_full_bits_unicode_and_boundaries() {
    let (_, signed) = compile("x", ColumnType::LongLong);
    let values = vec![
        Value::Null,
        Value::Int(0),
        Value::Int(1),
        Value::Int(-1),
        Value::Int(i64::MIN),
        Value::Int(i64::MAX),
        Value::Int(0x12345678),
    ];
    for sql in [
        "CHAR(x,66,NULL)",
        "CHAR(x%26+65,66)",
        "SPACE(x%16)",
        "EXPORT_SET(x,'文','🙂',':',16)",
        "EXPORT_SET(x,'Y','N')",
        "EXPORT_SET(x,'Y','N','',0)",
        "EXPORT_SET(x,'Y','N',',',-1)",
        "MAKE_SET(x,'a',NULL,'文','','🙂')",
        "ELT(x%7,'a',NULL,'文','','🙂')",
    ] {
        checked_legacy_column(
            sql,
            signed.clone(),
            vec![ColumnVector::from_values(values.clone())],
        );
    }
    let mut unsigned = signed.fields()[0].clone();
    unsigned.source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
    let unsigned = Arc::new(BatchSchema::new(vec![unsigned]));
    for sql in [
        "CHAR(x)",
        "EXPORT_SET(x,'Y','N',',',64)",
        "MAKE_SET(x,'a','b','c')",
        "ABS(x)",
    ] {
        checked_legacy_column(
            sql,
            unsigned.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Null,
                Value::UInt(0),
                Value::UInt(1),
                Value::UInt(u64::MAX),
            ])],
        );
    }
    for sql in ["ABS(x)"] {
        checked_legacy_column(
            sql,
            compile("x", ColumnType::Double).1,
            vec![ColumnVector::from_values(vec![
                Value::Null,
                Value::Double(-2.5),
                Value::Double(-0.0),
                Value::Double(f64::MAX),
            ])],
        );
        checked_legacy_column(
            sql,
            compile(
                "x",
                ColumnType::Decimal {
                    precision: 38,
                    scale: 6,
                },
            )
            .1,
            vec![ColumnVector::from_values(vec![
                Value::Null,
                Value::DecimalCompact(-1001, 6),
                Value::Decimal128(-999999999999999999999, 6),
            ])],
        );
    }
    let (_, text) = compile("x", ColumnType::VarString);
    for sql in [
        "SOUNDEX(x)",
        "FIND_IN_SET('a',x)",
        "FIND_IN_SET('',x)",
        "FIND_IN_SET('É',x)",
    ] {
        checked_legacy_column(
            sql,
            text.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Null,
                Value::String("".into()),
                Value::String(",a,A,,é,É".into()),
                Value::String("a文🙂ÀRobert Ashcraft".into()),
            ])],
        );
    }
    let (program, schema) = compile("SPACE(x)", ColumnType::LongLong);
    let selected = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![
            Value::Int(3),
            Value::Int(i64::MAX),
            Value::Null,
        ])],
    )
    .with_selection(crate::engine::executor::vec::batch::Selection::Indices(
        vec![0, 2],
    ));
    let output = program.try_eval_column(&selected).unwrap();
    assert_eq!(output.value_at(0), Value::String("   ".into()));
    assert_eq!(output.value_at(2), Value::Null);
}

#[test]
fn spatial_programs_lower_without_ast_escape_and_preserve_aliases_and_nulls() {
    let schema = compile("x", ColumnType::Geometry).1;
    let geom = crate::engine::geo::write_geometry(
        &crate::engine::geo::read_wkb(&[
            1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64,
        ])
        .map(|geom| crate::engine::geo::types::Geometry { srid: 0, geom })
        .unwrap(),
    );
    let batch = Batch::new(
        schema.clone(),
        vec![ColumnVector::from_values(vec![
            Value::Null,
            Value::Geometry(geom),
        ])],
    );
    let input = crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema);
    for sql in [
        "ST_ASTEXT(x)",
        "HEX(ST_ASWKB(x))",
        "ST_DIMENSION(x)",
        "ST_ISEMPTY(x)",
        "ST_ASTEXT(ST_SWAPXY(x))",
        "ST_ASTEXT(GEOMETRYCOLLECTION(x,x))",
        "ST_ASWKT(x)",
    ] {
        let ast = crate::parser::parse_expr_fragment(sql).unwrap();
        let program = ExprCompiler::compile(&ast, &input).unwrap();
        let legacy = ExprCompiler::compile_legacy_oracle(&ast, &input).unwrap();
        assert!(!program.capability().legacy_escape, "{sql}");
        assert_eq!(
            oracle(&program, &batch).unwrap(),
            oracle(&legacy, &batch).unwrap(),
            "{sql}"
        );
    }
    let (program, schema) = compile(
        "ST_GEOMFROMGEOJSON(JSON_OBJECT('type','Point','coordinates',JSON_ARRAY(x=1,x>0)))",
        ColumnType::LongLong,
    );
    assert!(program.capability().legacy_escape);
    let batch = Batch::new(schema, vec![ColumnVector::from_values(vec![Value::Int(1)])]);
    assert!(oracle(&program, &batch).is_err());
    let (program, schema) = compile("ELT(1,CAST(x>0 AS JSON))", ColumnType::LongLong);
    assert!(program.capability().legacy_escape);
    let batch = Batch::new(schema, vec![ColumnVector::from_values(vec![Value::Int(1)])]);
    let input = crate::engine::executor::vec::expr_batch::batch_schema_to_input(batch.schema());
    let ast = crate::parser::parse_expr_fragment("ELT(1,CAST(x>0 AS JSON))").unwrap();
    let legacy = ExprCompiler::compile_legacy_oracle(&ast, &input).unwrap();
    assert_eq!(
        oracle(&program, &batch).unwrap(),
        oracle(&legacy, &batch).unwrap()
    );
}

#[test]
fn new_codecs_parsers_and_unsigned_arithmetic_keep_scalar_semantics() {
    let (_, strings) = compile("x", ColumnType::VarString);
    for sql in ["FROM_BASE64(TO_BASE64(x))", "TO_BASE64(x)"] {
        checked_legacy_column(
            sql,
            strings.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Null,
                Value::String("".into()),
                Value::String("文🙂\0".repeat(40)),
                Value::String("hello".into()),
            ])],
        );
    }
    checked_legacy_column(
        "STR_TO_DATE(x,'%Y-%m-%d %H:%i:%s.%f')",
        strings.clone(),
        vec![ColumnVector::from_values(vec![
            Value::Null,
            Value::String("2024-02-29 23:59:59.123456".into()),
            Value::String("1999-01-01 00:00:00.000001".into()),
        ])],
    );
    let mut aes_field = strings.fields()[0].clone();
    aes_field.source.collation_id = Some(crate::charset::UTF8MB4_0900_AI_CI_ID);
    let aes_schema = Arc::new(BatchSchema::new(vec![aes_field]));
    checked_legacy_column(
        "AES_DECRYPT(AES_ENCRYPT(x,'key'),'key')",
        aes_schema,
        vec![ColumnVector::from_values(vec![
            Value::Null,
            Value::String("".into()),
            Value::String("文🙂\0".repeat(40)),
            Value::String("hello".into()),
        ])],
    );
    let mut source = strings.fields()[0].clone();
    source.source.collation_id = Some(crate::charset::UTF8MB4_0900_AI_CI_ID);
    let mut password = source.clone();
    password.name = "password".into();
    password.column_type = ColumnType::Double;
    password.source.decimals = Some(2);
    let schema = Arc::new(BatchSchema::new(vec![source, password]));
    let input = crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema);
    let ast = crate::parser::parse_expr_fragment("AES_ENCRYPT(x,password)").unwrap();
    assert!(ExprCompiler::compile(&ast, &input).is_err());
    let (_, schema) = compile("x", ColumnType::LongLong);
    let mut field = schema.fields()[0].clone();
    field.source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
    let schema = Arc::new(BatchSchema::new(vec![field]));
    for sql in [
        "x+7",
        "CAST(x+9223372036854775808 AS UNSIGNED)>9223372036854775809",
        "CAST(x AS UNSIGNED)%7",
        "x*3",
    ] {
        checked_legacy_column(
            sql,
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Null,
                Value::UInt(0),
                Value::UInt(1),
                Value::UInt(100),
            ])],
        );
    }
    let (program, schema) = compile("CAST(x AS UNSIGNED)+1", ColumnType::LongLong);
    assert!(!program.has_column_contract(&schema));
}

#[test]
fn deep_numeric_contract_binding_remains_linear_when_unsigned_kernels_decline() {
    let (_, schema) = compile("x", ColumnType::LongLong);
    let mut unsigned = schema.fields()[0].clone();
    unsigned.name = "u".into();
    unsigned.source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
    let schema = Arc::new(BatchSchema::new(vec![schema.fields()[0].clone(), unsigned]));
    let input = crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema);
    for seed in ["x", "x+u"] {
        let mut sql = seed.to_string();
        for _ in 0..48 {
            sql = format!("({sql}+1)");
        }
        let ast = crate::parser::parse_expr_fragment(&sql).unwrap();
        let program = ExprCompiler::compile(&ast, &input).unwrap();
        CONTRACT_BIND_VISITS.with(|visits| visits.set(0));
        let _ = program.has_column_contract(&schema);
        let visits = CONTRACT_BIND_VISITS.with(std::cell::Cell::get);
        assert!(visits < 256, "{seed}: {visits} binding visits");
    }
}

#[test]
fn conditional_and_bit_columns_preserve_null_truth_short_circuit_and_overflow_fallback() {
    let (_, schema) = compile("x", ColumnType::LongLong);
    for sql in [
        "IF(x<0,1,2)",
        "CASE x%3 WHEN 0 THEN x WHEN 1 THEN 7 ELSE NULL END",
        "CASE WHEN x<0 THEN 1 WHEN x=0 THEN 2 ELSE 3 END",
        "x&7",
        "x|7",
        "x^7",
        "x<<2",
        "x>>2",
        "x<<64",
        "x<<-1",
    ] {
        checked_legacy_column(
            sql,
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Null,
                Value::Int(i64::MIN),
                Value::Int(-1),
                Value::Int(0),
                Value::Int(i64::MAX),
            ])],
        );
    }
    for sql in ["IF(x<0,x+1,0)", "CASE WHEN x<0 THEN ABS(x) ELSE 1 END"] {
        let (program, schema) = compile(sql, ColumnType::LongLong);
        assert!(
            program
                .try_eval_column(&Batch::new(
                    schema,
                    vec![ColumnVector::from_values(vec![Value::Int(i64::MIN)])]
                ))
                .is_none(),
            "{sql}"
        );
    }
    let (_, schema) = compile("x", ColumnType::VarString);
    for sql in [
        "IF(LENGTH(x)>2,x,'小')",
        "CASE WHEN LENGTH(x)=0 THEN '' ELSE x END",
        "TRIM(BOTH 'xy' FROM x)",
        "TRIM(LEADING '文' FROM x)",
        "TRIM(TRAILING '🙂' FROM x)",
        "TRIM(BOTH '' FROM x)",
    ] {
        checked_legacy_column(
            sql,
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Null,
                Value::String("".into()),
                Value::String("xyxy文hello🙂xyxy".into()),
                Value::String("文文abc🙂🙂".into()),
            ])],
        );
    }
}

#[test]
fn regexp_columns_keep_collation_flags_unicode_offsets_occurrences_and_nulls() {
    let (_, schema) = compile("x", ColumnType::VarString);
    for collation in [
        "utf8mb4_0900_ai_ci",
        "utf8mb4_general_ci",
        "utf8mb4_0900_as_cs",
        "utf8mb4_bin",
        "utf8mb4_turkish_ci",
    ] {
        let mut field = schema.fields()[0].clone();
        field.source.collation_id = Some(crate::charset::resolve_collation(collation).unwrap().id);
        let schema = Arc::new(BatchSchema::new(vec![field]));
        for pattern in ["[0-9]+", "é", "文|🙂", "^|$", ".", "a.*b", "[[:alpha:]]+"] {
            for flags in ["", "i", "c", "ic", "ci", "m", "n", "u"] {
                for expr in [
                    format!("REGEXP_LIKE(x,'{pattern}','{flags}')"),
                    format!("REGEXP_INSTR(x,'{pattern}',1,1,0,'{flags}')"),
                    format!("REGEXP_INSTR(x,'{pattern}',1,2,1,'{flags}')"),
                    format!("REGEXP_SUBSTR(x,'{pattern}',1,2,'{flags}')"),
                ] {
                    checked_legacy_column(
                        &expr,
                        schema.clone(),
                        vec![ColumnVector::from_values(vec![
                            Value::Null,
                            Value::String("".into()),
                            Value::String("文🙂12é34É".into()),
                            Value::String("a\nb\0e\u{301}".into()),
                        ])],
                    );
                }
            }
        }
    }
    for expr in [
        "REGEXP_INSTR(x,'[0-9]+',2,0,1)",
        "REGEXP_SUBSTR(x,'[0-9]+',2,2)",
    ] {
        checked_legacy_column(
            expr,
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Null,
                Value::String("文🙂12é34".into()),
                Value::String("abcdef".into()),
            ])],
        );
    }
    for expr in [
        "REGEXP_LIKE(x,'[')",
        "REGEXP_SUBSTR(x,'a',0)",
        "REGEXP_INSTR(x,'a',1,1,2)",
        "REGEXP_LIKE(x,'a','bad')",
        "REGEXP_INSTR(x,'a',3)",
    ] {
        let (program, schema) = compile(expr, ColumnType::VarString);
        assert!(
            program
                .try_eval_column(&Batch::new(
                    schema,
                    vec![ColumnVector::from_values(vec![Value::String("a".into())])]
                ))
                .is_none(),
            "{expr}"
        );
    }
    for expr in [
        "REGEXP_LIKE(x,'[')",
        "REGEXP_INSTR(x,'[')",
        "REGEXP_SUBSTR(x,'[')",
    ] {
        checked_legacy_column(expr, schema.clone(), vec![ColumnVector::Null { len: 3 }]);
    }
    // An invisible out-of-range position must not abort a selected row.
    let (program, schema) = compile("REGEXP_SUBSTR(x,'a',2)", ColumnType::VarString);
    let batch = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![
            Value::String("aaa".into()),
            Value::String("".into()),
            Value::Null,
        ])],
    )
    .with_selection(crate::engine::executor::vec::batch::Selection::Indices(
        vec![0, 2],
    ));
    let output = program.try_eval_column(&batch).unwrap();
    assert_eq!(output.value_at(0), Value::String("a".into()));
    assert_eq!(output.value_at(2), Value::Null);
}

#[test]
fn concat_ws_columns_keep_null_skipping_and_unsigned_casts() {
    let (_, s) = compile("x", ColumnType::VarString);
    let mut n = s.fields()[0].clone();
    n.name = "n".into();
    n.column_type = ColumnType::LongLong;
    n.source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
    let schema = Arc::new(BatchSchema::new(vec![s.fields()[0].clone(), n]));
    let columns = vec![
        ColumnVector::from_values(vec![
            Value::Null,
            Value::String("文🙂".into()),
            Value::String("".into()),
            Value::Null,
        ]),
        ColumnVector::from_values(vec![
            Value::UInt(u64::MAX),
            Value::Null,
            Value::UInt(0),
            Value::Null,
        ]),
    ];
    for sql in [
        "CONCAT_WS('/',x,CAST(n AS CHAR))",
        "CONCAT_WS('',x,CAST(n AS CHAR),x)",
        "CONCAT_WS('文🙂',x,NULL,CAST(n AS CHAR))",
    ] {
        checked_legacy_column(sql, schema.clone(), columns.clone());
    }
}

#[test]
fn string_search_columns_preserve_collations_expansions_character_boundaries_and_rebinding() {
    let values = vec![
        Value::Null,
        Value::String("".into()),
        Value::String("ABC abc".into()),
        Value::String("Straße ss Æ æ".into()),
        Value::String("中文🙂dbⅧviii".into()),
        Value::String("a\u{301}áÉe\u{327}İı\0".into()),
    ];
    for collation in [
        "utf8mb4_0900_ai_ci",
        "utf8mb4_general_ci",
        "utf8mb4_unicode_ci",
        "utf8mb4_bin",
        "utf8mb4_0900_as_cs",
        "utf8mb4_german2_ci",
        "utf8mb4_turkish_ci",
    ] {
        let info = crate::charset::resolve_collation(collation).unwrap();
        let (_, schema) = compile("x", ColumnType::VarString);
        let mut field = schema.fields()[0].clone();
        field.source.collation_id = Some(info.id);
        let schema = Arc::new(BatchSchema::new(vec![field]));
        let input = crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema);
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(values.clone())],
        );
        for needle in [
            "", "a", "A", "ss", "ß", "Æ", "ae", "Ⅷ", "viii", "中文", "🙂", "\u{301}", "\0",
        ] {
            for start in [-1, 0, 1, 2, 4, 20, i64::MAX] {
                for name in ["LOCATE", "INSTR", "POSITION"] {
                    let col = Expr::Identifier("x".into());
                    let n = Expr::Literal(Literal::String(needle.into()));
                    let args = if name == "INSTR" {
                        vec![col, n]
                    } else if name == "POSITION" {
                        vec![n, col]
                    } else {
                        vec![n, col, Expr::Literal(Literal::Int(start))]
                    };
                    let ast = Expr::Function {
                        name: name.into(),
                        args,
                        distinct: false,
                    };
                    let program = ExprCompiler::compile(&ast, &input).unwrap();
                    let legacy = ExprCompiler::compile_legacy_oracle(&ast, &input).unwrap();
                    let expected = oracle(&legacy, &batch).unwrap();
                    let actual = program
                        .try_eval_column(&batch)
                        .unwrap_or_else(|| panic!("{collation}/{name}/{needle:?}/{start}"));
                    assert_eq!(
                        (0..batch.num_rows())
                            .map(|r| actual.value_at(r))
                            .collect::<Vec<_>>(),
                        expected,
                        "{collation}/{name}/{needle:?}/{start}"
                    );
                }
            }
        }
        let ast = crate::parser::parse_expr_fragment("LOCATE(?,x,?)").unwrap();
        let program = ExprCompiler::compile(&ast, &input).unwrap();
        let legacy = ExprCompiler::compile_legacy_oracle(&ast, &input).unwrap();
        for needle in ["ss", "a", "中文", "", "a"] {
            crate::engine::params::with_params(
                vec![Value::String(needle.into()), Value::Int(1)],
                || {
                    let actual = program.try_eval_column(&batch).unwrap();
                    assert_eq!(
                        (0..batch.num_rows())
                            .map(|r| actual.value_at(r))
                            .collect::<Vec<_>>(),
                        oracle(&legacy, &batch).unwrap()
                    );
                },
            );
        }
    }
}

#[test]
fn numeric_selected_rows_preserve_unsigned_and_decimal_lanes_without_observing_hidden_values() {
    let (_, schema) = compile("x", ColumnType::LongLong);
    let mut field = schema.fields()[0].clone();
    field.source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
    let schema = Arc::new(BatchSchema::new(vec![field]));
    for sql in ["FLOOR(x)", "ROUND(x,0)", "BIN(x)", "BIT_COUNT(x)"] {
        let ast = crate::parser::parse_expr_fragment(sql).unwrap();
        let program = ExprCompiler::compile(
            &ast,
            &crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema),
        )
        .unwrap();
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::UInt(u64::MAX),
                Value::UInt(1),
                Value::Null,
            ])],
        )
        .with_selection(crate::engine::executor::vec::batch::Selection::Indices(
            vec![0, 2, 0],
        ));
        let out = program.try_eval_column(&batch).unwrap();
        let expected = oracle(
            &program,
            &Batch::new(
                schema.clone(),
                vec![ColumnVector::from_values(vec![Value::UInt(u64::MAX)])],
            ),
        )
        .unwrap()
        .remove(0);
        assert_eq!(out.value_at(0), expected, "{sql}");
        assert_eq!(out.value_at(1), Value::Null);
        assert_eq!(out.value_at(2), Value::Null);
    }
    let (program, schema) = compile(
        "ROUND(x,2)",
        ColumnType::Decimal {
            precision: 38,
            scale: 6,
        },
    );
    let batch = Batch::new(
        schema.clone(),
        vec![ColumnVector::from_values(vec![
            Value::DecimalCompact(1234567, 6),
            Value::Decimal128(i128::MAX, 6),
            Value::Null,
        ])],
    )
    .with_selection(crate::engine::executor::vec::batch::Selection::Indices(
        vec![0, 2, 0],
    ));
    let out = program.try_eval_column(&batch).unwrap();
    assert_eq!(out.value_at(0), Value::DecimalCompact(123, 2));
    assert_eq!(out.value_at(1), Value::Null);
    assert_eq!(out.value_at(2), Value::Null);
    let (program, schema) = compile(
        "FLOOR(x)",
        ColumnType::Decimal {
            precision: 20,
            scale: 2,
        },
    );
    assert!(!program.has_column_contract(&schema));
}

#[test]
fn string_binary_search_and_speculative_budget_declines_preserve_the_oracle() {
    let (_, schema) = compile("x", ColumnType::Blob);
    let mut field = schema.fields()[0].clone();
    field.source.flags |= crate::mysql::protocol::BINARY_FLAG;
    field.source.collation_id = Some(crate::charset::BINARY_COLLATION_ID);
    let schema = Arc::new(BatchSchema::new(vec![field]));
    for sql in [
        "LOCATE('a',x)",
        "INSTR(x,'a')",
        "POSITION('a' IN x)",
        "LOCATE('a',x,3)",
    ] {
        checked_legacy_column(
            sql,
            schema.clone(),
            vec![typed_binary_values(vec![
                Value::Null,
                Value::Bytes(vec![255, 128, b'a', 0, b'a']),
                Value::Bytes(vec![]),
            ])],
        );
    }
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    for sql in [
        "LOCATE('a',x)",
        "REPEAT(x,2)",
        "QUOTE(x)",
        "SUBSTRING_INDEX(x,'a',2)",
    ] {
        let (program, schema) = compile(sql, ColumnType::VarString);
        let batch = Batch::new(
            schema,
            vec![ColumnVector::from_values(vec![Value::String(
                "a文🙂".repeat(10000),
            )])],
        );
        let budget = Arc::new(MemBudget::new(1024));
        with_query_budget(budget.clone(), || {
            assert!(program.try_eval_column(&batch).is_none(), "{sql}");
            assert!(budget.terminal_oom().is_none());
            crate::engine::cancel::check_cancelled().unwrap();
        });
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn signed_extrema_columns_keep_extremes_nulls_selection_and_mixed_type_fallback() {
    let (_, schema) = compile("x", ColumnType::LongLong);
    let values = vec![
        Value::Null,
        Value::Int(i64::MIN),
        Value::Int(-7),
        Value::Int(0),
        Value::Int(7),
        Value::Int(i64::MAX),
    ];
    for sql in [
        "GREATEST(x,7)",
        "LEAST(x,-7)",
        "GREATEST(x,x,7)",
        "LEAST(x,3,-5)",
        "GREATEST(x,NULL)",
        "GREATEST(LENGTH(CAST(x AS CHAR)),7)",
    ] {
        checked_legacy_column(
            sql,
            schema.clone(),
            vec![ColumnVector::from_values(values.clone())],
        );
    }
    let (program, schema) = compile("GREATEST(ABS(x),7)", ColumnType::LongLong);
    let hidden = Batch::new(
        schema.clone(),
        vec![ColumnVector::from_values(vec![
            Value::Int(3),
            Value::Int(i64::MIN),
            Value::Null,
        ])],
    );
    assert!(program.try_eval_column(&hidden).is_none());
    assert!(oracle(&program, &hidden).is_err());
    let selected = hidden.with_selection(crate::engine::executor::vec::batch::Selection::Indices(
        vec![0, 2, 0],
    ));
    let actual = program.try_eval_column(&selected).unwrap();
    assert_eq!(
        (0..3).map(|row| actual.value_at(row)).collect::<Vec<_>>(),
        vec![Value::Int(7), Value::Null, Value::Null]
    );
    for kind in [
        ColumnType::Double,
        ColumnType::VarString,
        ColumnType::Decimal {
            precision: 20,
            scale: 6,
        },
    ] {
        let (program, schema) = compile("GREATEST(x,7)", kind);
        assert!(!program.has_column_contract(&schema));
    }
    let (_, schema) = compile("x", ColumnType::LongLong);
    let mut field = schema.fields()[0].clone();
    field.source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
    let schema = Arc::new(BatchSchema::new(vec![field]));
    let ast = crate::parser::parse_expr_fragment("LEAST(x,7)").unwrap();
    let program = ExprCompiler::compile(
        &ast,
        &crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema),
    )
    .unwrap();
    assert!(!program.has_column_contract(&schema));
}

#[test]
fn named_literal_programs_keep_value_and_reject_nonliteral_name_or_value() {
    let (_, schema) = compile("x", ColumnType::LongLong);
    for sql in [
        "NAME_CONST('answer',42)",
        "NAME_CONST('',NULL)",
        "NAME_CONST('a','标签🙂')",
        "NAME_CONST('a',X'00FF')",
    ] {
        checked_legacy_column(
            sql,
            schema.clone(),
            vec![ColumnVector::from_values(vec![Value::Int(1), Value::Null])],
        );
    }
    let ast = crate::parser::parse_expr_fragment("NAME_CONST('a',42)").unwrap();
    let input = crate::engine::executor::vec::expr_batch::batch_schema_to_input(&schema);
    assert!(ExprCompiler::compile_strict(&ast, &input).is_err());
    for sql in [
        "NAME_CONST(x,1)",
        "NAME_CONST('a',x)",
        "NAME_CONST('a',1/0)",
    ] {
        let (program, schema) = compile(sql, ColumnType::LongLong);
        assert!(!program.has_column_contract(&schema), "{sql}");
    }
}

#[test]
fn fixed_temporal_shifts_keep_typed_lanes_zones_precision_and_overflow_fallback() {
    let mut session = SessionContext::default();
    let micros = chrono::NaiveDate::from_ymd_opt(2024, 3, 10)
        .unwrap()
        .and_hms_micro_opt(6, 30, 1, 123456)
        .unwrap()
        .and_utc()
        .timestamp_micros();
    for zone in ["+00:00", "+08:00", "America/New_York"] {
        session.sysvars.time_zone = zone.into();
        crate::engine::expr::with_session_expr_info(&session, || {
            for (kind, values) in [
                (
                    ColumnType::DateTime,
                    vec![
                        Value::Null,
                        Value::DateTime(micros),
                        Value::DateTime(micros + 86_400_000_000),
                    ],
                ),
                (
                    ColumnType::Timestamp,
                    vec![
                        Value::Null,
                        Value::Timestamp(micros),
                        Value::Timestamp(micros + 86_400_000_000),
                    ],
                ),
                (
                    ColumnType::Date,
                    vec![Value::Null, Value::Date(19792), Value::Date(19793)],
                ),
            ] {
                let (_, schema) = compile("x", kind);
                for sql in [
                    "DATE_ADD(x,INTERVAL 1 DAY)",
                    "DATE_SUB(x,INTERVAL 1 DAY)",
                    "ADDDATE(x,2)",
                    "SUBDATE(x,2)",
                    "DATE_ADD(x,INTERVAL 1 WEEK)",
                    "DATE_ADD(x,INTERVAL 1 HOUR)",
                    "DATE_SUB(x,INTERVAL 2 MINUTE)",
                    "DATE_ADD(x,INTERVAL 7 SECOND)",
                    "DATE_ADD(x,INTERVAL 123 MICROSECOND)",
                ] {
                    checked_legacy_column(
                        sql,
                        schema.clone(),
                        vec![ColumnVector::from_values(values.clone())],
                    );
                }
            }
        });
    }
    let (program, schema) = compile("DATE_ADD(x,INTERVAL 1 DAY)", ColumnType::DateTime);
    let batch = Batch::new(
        schema,
        vec![ColumnVector::from_values(vec![Value::DateTime(
            crate::types::MAX_DATETIME_MICROS,
        )])],
    );
    crate::engine::api::clear_statement_warnings();
    assert!(program.try_eval_column(&batch).is_none());
    assert_eq!(crate::engine::api::statement_warning_count(), 0);
    let _ = oracle(&program, &batch);
    assert!(crate::engine::api::statement_warning_count() > 0);
    crate::engine::api::clear_statement_warnings();
}
