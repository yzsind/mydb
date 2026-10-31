use super::*;
use crate::parser::{BinaryOp, Expr, Literal};
use crate::storage::schema::{ColumnSchema, ColumnState, DataTypeKind, TableSchema};
use crate::storage::TableStats;

#[test]
fn sqlproc_sus_full_join_fallback_preserves_both_sides() {
    let scan = |table: &str| PhysicalPlan::ValuesScan {
        rows: vec![],
        columns: vec![Column {
            name: "a".into(),
            column_type: ColumnType::LongLong,
            source: crate::types::ColumnSource {
                table: table.into(),
                ..Default::default()
            },
        }],
        filter: None,
        estimated_rows: 2.0,
        cost: crate::engine::cbo::Cost::scalar(1.0),
    };
    let row = |table: &str, value| {
        PlanRow::from_fields(vec![PlanField::new(
            table,
            None,
            "a",
            ColumnType::LongLong,
            Value::Int(value),
        )])
    };
    let left = scan("l");
    let right = scan("r");
    for build in [BuildSide::Left, BuildSide::Right] {
        let l = vec![row("l", 1), row("l", 2)];
        let r = vec![row("r", 2), row("r", 3)];
        let (build_rows, probe_rows) = if matches!(build, BuildSide::Left) {
            (l, r)
        } else {
            (r, l)
        };
        let result = execute_hash_join_nlj_fallback(
            &left,
            &right,
            crate::parser::JoinType::Full,
            &[Expr::CompoundIdentifier(vec!["l".into(), "a".into()])],
            &[Expr::CompoundIdentifier(vec!["r".into(), "a".into()])],
            &[false],
            build,
            None,
            build_rows,
            probe_rows,
        )
        .unwrap();
        let mut result: Vec<Vec<String>> = result
            .into_iter()
            .map(|row| {
                row.fields
                    .iter()
                    .map(|field| field.value.to_display_string())
                    .collect()
            })
            .collect();
        result.sort();
        assert_eq!(
            result,
            vec![vec!["1", "NULL"], vec!["2", "2"], vec!["NULL", "3"]]
        );
    }
}

#[test]
fn sqlproc_sus_streaming_distinct_keys_preserve_null_and_field_boundaries() {
    let row = |values: Vec<Value>| {
        PlanRow::from_fields(
            values
                .into_iter()
                .enumerate()
                .map(|(i, value)| {
                    PlanField::new("t", None, format!("c{i}"), ColumnType::VarString, value)
                })
                .collect(),
        )
    };
    let key = |values| {
        let mut key = Vec::new();
        projected_dedup_key_into(&row(values), &[SelectItem::Wildcard], &mut key).unwrap();
        key
    };
    assert_ne!(
        key(vec![Value::Null]),
        key(vec![Value::String("NULL".into())])
    );
    assert_ne!(
        key(vec![
            Value::String("a\0b".into()),
            Value::String("c".into())
        ]),
        key(vec![
            Value::String("a".into()),
            Value::String("b\0c".into())
        ])
    );
    assert_ne!(
        key(vec![Value::Bytes(vec![0xc3])]),
        key(vec![Value::Bytes(vec![0xff])])
    );
    assert_ne!(
        key(vec![Value::Null]),
        key(vec![Value::Bytes(b"NULL".to_vec())])
    );
}

#[test]
fn sqlproc_sus_ordered_aggregate_forced_spill_matches_memory() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    let Statement::Select(select) =
        crate::parser::parse_statement("SELECT GROUP_CONCAT(v ORDER BY k) FROM t GROUP BY g")
            .unwrap()
    else {
        panic!("SELECT")
    };
    let SelectItem::Expr { expr, .. } = &select.projection[0] else {
        panic!("expression")
    };
    let slots = super::super::aggregate::collect_aggregate_slots(&[expr]);
    let input: Vec<PlanRow> = [("a", 3), ("b", 1), ("c", 2)]
        .into_iter()
        .map(|(v, k)| {
            PlanRow::from_fields(vec![
                PlanField::new("t", None, "g", ColumnType::LongLong, Value::Int(1)),
                PlanField::new(
                    "t",
                    None,
                    "v",
                    ColumnType::VarString,
                    Value::String(v.into()),
                ),
                PlanField::new("t", None, "k", ColumnType::LongLong, Value::Int(k)),
            ])
        })
        .collect();
    let memory = aggregate_plan_rows(input.clone(), &select.group_by, None, &slots).unwrap();
    let spill = aggregate_plan_rows_spilled(&db, input, &select.group_by, None, &slots).unwrap();
    let value =
        |rows: &[PlanRow]| rows[0].agg_values.as_ref().unwrap()[&slots[0].key].to_display_string();
    assert_eq!(value(&memory), "b,c,a");
    assert_eq!(value(&spill), value(&memory));
}

#[test]
fn exec_audit_neq_summary_requires_compatible_domains() {
    let scan = |name: &str, column_type, collation_id| PhysicalPlan::ValuesScan {
        rows: vec![],
        columns: vec![Column {
            name: name.into(),
            column_type,
            source: crate::types::ColumnSource {
                collation_id,
                ..Default::default()
            },
        }],
        filter: None,
        estimated_rows: 1.0,
        cost: crate::engine::cbo::Cost::scalar(1.0),
    };
    let residual = Expr::Binary {
        left: Box::new(Expr::Identifier("b".into())),
        op: BinaryOp::NotEq,
        right: Box::new(Expr::Identifier("p".into())),
    };
    for (left_type, right_type, collation, eligible) in [
        (ColumnType::LongLong, ColumnType::LongLong, None, true),
        (ColumnType::Long, ColumnType::LongLong, None, true),
        (
            ColumnType::Decimal {
                precision: 10,
                scale: 1,
            },
            ColumnType::Decimal {
                precision: 20,
                scale: 2,
            },
            None,
            true,
        ),
        (ColumnType::Float, ColumnType::Double, None, true),
        (
            ColumnType::Decimal {
                precision: 10,
                scale: 1,
            },
            ColumnType::Double,
            None,
            false,
        ),
        (
            ColumnType::VarString,
            ColumnType::VarString,
            Some(46),
            false,
        ),
    ] {
        let left = scan("b", left_type, collation);
        let right = scan("p", right_type, collation);
        let spec = HashJoinSpec {
            left_plan: &left,
            right_plan: &right,
            join_type: crate::parser::JoinType::Anti,
            build_keys: &[],
            probe_keys: &[],
            build_key_plan: None,
            probe_key_plan: None,
            null_safe: &[],
            build_is_left: true,
            residual: Some(&residual),
            output_projection: None,
        };
        assert_eq!(
            compile_semi_anti_neq_residual(&spec, Some(&residual)).is_some(),
            eligible
        );
    }
}

#[test]
fn exec_audit_cte_in_null_truth_table() {
    for negated in [false, true] {
        for (value, expected) in [
            (Value::Int(1), false),
            (Value::Int(2), !negated),
            (Value::Null, false),
        ] {
            let pred = CteRowPredicate::InList {
                col: 0,
                values: vec![Value::Int(2), Value::Null],
                negated,
            };
            assert_eq!(eval_cte_row_predicate(&pred, &[value]).unwrap(), expected);
        }
        let pred = CteRowPredicate::InList {
            col: 0,
            values: vec![Value::Int(2)],
            negated,
        };
        assert!(!eval_cte_row_predicate(&pred, &[Value::Null]).unwrap());
    }
}

#[test]
fn exec_audit_distinct_keys_are_typed_and_length_prefixed() {
    let field = |name: &str, value: Value| PlanField {
        meta: intern_col_meta(ColMeta {
            invisible: false,
            table: "t".into(),
            alias: None,
            column: name.into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        }),
        value,
    };
    let mut rows = vec![
        PlanRow::from_fields(vec![field("a", Value::Int(1)), field("b", Value::Null)]),
        PlanRow::from_fields(vec![
            field("a", Value::String("1".into())),
            field("b", Value::Null),
        ]),
        PlanRow::from_fields(vec![
            field("a", Value::String("a\0b".into())),
            field("b", Value::String("c".into())),
        ]),
        PlanRow::from_fields(vec![
            field("a", Value::String("a".into())),
            field("b", Value::String("b\0c".into())),
        ]),
    ];
    rows.push(rows[0].clone());
    let temp = tempfile::tempdir().unwrap();
    dedup_projected_rows(&mut rows, &[SelectItem::Wildcard], temp.path()).unwrap();
    assert_eq!(rows.len(), 4);
}

#[test]
fn storage_predicate_safety_preserves_pure_kernels_and_rejects_nested_opaque_calls() {
    let schema = decimal_fact_schema();
    for (sql, safe) in [
        ("quantity < 24 AND discount BETWEEN 0.05 AND 0.07", true),
        (
            "ship_date >= '1994-01-01' AND ship_date < '1995-01-01'",
            true,
        ),
        ("ABS(quantity - 10) < 5", true),
        ("quantity < 24 AND GET_LOCK('storage_safety',0)=1", false),
        ("quantity < 24 OR SLEEP(0)=0", false),
        (
            "CASE WHEN quantity < 24 THEN 1 ELSE GET_LOCK('storage_safety',0) END=1",
            false,
        ),
        ("COALESCE(GET_LOCK('storage_safety',0),0)=1", false),
        ("quantity = @stored_variable", false),
    ] {
        let Statement::Select(select) =
            crate::parser::parse_statement(&format!("SELECT * FROM fact WHERE {sql}")).unwrap()
        else {
            panic!("SELECT expected");
        };
        let expr = select.selection.unwrap();
        let predicate = CompiledPredicate::compile(&expr, &schema).unwrap();
        assert_eq!(predicate.page_latch_safe(), safe, "{sql}");
        assert_eq!(
            CompiledPredicate::compile_for_storage(&expr, &schema).is_some(),
            safe,
            "{sql}"
        );
    }
}

#[test]
fn materialized_cte_scalar_result_uses_hard_admission_after_parent_soft_quota() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(root.path()).unwrap());
    let columns = vec![Column {
        name: "v".into(),
        column_type: ColumnType::LongLong,
        source: Default::default(),
    }];
    let cte = Arc::new(crate::engine::api::CteResult::materialized(
        columns.clone(),
        vec![vec![Value::Int(7)]],
    ));
    let plan = PhysicalPlan::CteScan {
        name: "c".into(),
        alias: None,
        columns,
        filter: None,
        estimated_rows: 1.0,
        cost: Default::default(),
        projected_columns: None,
    };
    let Statement::Select(select) = crate::parser::parse_statement("SELECT v FROM c").unwrap()
    else {
        panic!("select");
    };
    let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
        64 * 1024,
    ));
    crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
        budget.force_charge(budget.limit()).unwrap();
        crate::engine::executor::cte::with_cte_registry(
            std::collections::HashMap::from([("c".into(), cte)]),
            || {
                let txn = TxnView::read_only(0, crate::storage::IsolationLevel::RepeatableRead);
                let response = with_vec_mode(VecMode::Off, || {
                    execute_physical_select(&db, txn, &select, &plan)
                })
                .unwrap()
                .materialize()
                .unwrap();
                let QueryResponse::ResultSet(result) = response else {
                    panic!("result set");
                };
                assert_eq!(result.rows, vec![vec![Value::Int(7)]]);
            },
        );
        assert_eq!(budget.used(), budget.limit());
        budget.release(budget.limit());
    });
    assert_eq!(budget.used(), 0);
}

#[test]
fn spilled_cte_scalar_filter_stream_does_not_evaluate_future_rows() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(root.path()).unwrap());
    let columns = vec![
        Column {
            name: "k".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        },
        Column {
            name: "v".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        },
    ];
    let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
        64 * 1024,
    ));
    crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
        let mut builder = crate::engine::executor::cte::CteSpillBuilder::new(
            columns.clone(),
            root.path().to_path_buf(),
        );
        builder
            .push(vec![Value::Int(1), Value::String("x".repeat(300 * 1024))])
            .unwrap();
        builder
            .push(vec![Value::Int(2), Value::String("invalid json".into())])
            .unwrap();
        let cte = Arc::new(builder.finish().unwrap());
        assert!(cte.spill.is_some());
        let Statement::Select(select) = crate::parser::parse_statement(
            "SELECT * FROM c WHERE CASE WHEN k=1 THEN 1 ELSE JSON_EXTRACT(v,'$.x') END",
        )
        .unwrap() else {
            panic!("select");
        };
        let plan = PhysicalPlan::CteScan {
            name: "c".into(),
            alias: None,
            columns,
            filter: select.selection,
            estimated_rows: 2.0,
            cost: Default::default(),
            projected_columns: None,
        };
        crate::engine::executor::cte::with_cte_registry(
            std::collections::HashMap::from([("c".into(), cte)]),
            || {
                let txn = TxnView::read_only(0, crate::storage::IsolationLevel::RepeatableRead);
                let mut stream = execute_plan_stream(&db, txn, &plan).unwrap();
                let first = stream.next().unwrap().unwrap();
                assert_eq!(first.fields[0].value, Value::Int(1));
                assert!(
                    stream.next().is_err(),
                    "invalid JSON belongs to the second pull"
                );
            },
        );
    });
    assert_eq!(budget.used(), 0);
}

#[test]
fn spilled_cte_native_replay_preserves_unsigned_decimal_null_and_owned_text() {
    let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = Arc::new(Database::open(root.path()).unwrap());
    let mut unsigned = crate::types::ColumnSource::default();
    unsigned.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
    let columns = vec![
        Column {
            name: "u".into(),
            column_type: ColumnType::LongLong,
            source: unsigned,
        },
        Column {
            name: "d".into(),
            column_type: ColumnType::Decimal {
                precision: 22,
                scale: 3,
            },
            source: Default::default(),
        },
        Column {
            name: "s".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        },
    ];
    let expected = vec![
        vec![
            Value::UInt(u64::MAX),
            Value::Decimal128(123456789012, 3),
            Value::String("x".repeat(300 * 1024)),
        ],
        vec![Value::UInt(3), Value::Null, Value::Null],
    ];
    let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
        32 * 1024,
    ));
    crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
        let mut builder = crate::engine::executor::cte::CteSpillBuilder::new(
            columns.clone(),
            root.path().to_path_buf(),
        );
        for row in &expected {
            builder.push(row.clone()).unwrap();
        }
        let cte = Arc::new(builder.finish().unwrap());
        assert!(cte.spill.is_some());
        let plan = PhysicalPlan::CteScan {
            name: "c".into(),
            alias: None,
            columns,
            filter: None,
            estimated_rows: 2.0,
            cost: Default::default(),
            projected_columns: None,
        };
        crate::engine::executor::cte::with_cte_registry(
            std::collections::HashMap::from([("c".into(), cte)]),
            || {
                let txn = TxnView::read_only(0, crate::storage::IsolationLevel::RepeatableRead);
                let mut stream =
                    super::super::vec_bridge::try_native_plan_row_stream(&db, txn, &plan)
                        .unwrap()
                        .expect("native CTE replay");
                for expected in &expected {
                    let row = stream.next().unwrap().unwrap();
                    assert_eq!(
                        row.fields
                            .iter()
                            .map(|f| f.value.to_display_string())
                            .collect::<Vec<_>>(),
                        expected
                            .iter()
                            .map(Value::to_display_string)
                            .collect::<Vec<_>>()
                    );
                }
                assert!(stream.next().unwrap().is_none());
            },
        );
    });
    assert_eq!(budget.used(), 0);
}

fn col(name: &str, data_type: DataTypeKind, pos: usize) -> ColumnSchema {
    ColumnSchema {
        not_null_from_scn: 0,
        name: name.to_string(),
        data_type,
        nullable: false,
        unsigned: false,
        invisible: false,
        zerofill: false,
        auto_increment: false,
        default: None,
        read_backfill_default: None,
        default_expr: None,
        on_update_expr: None,
        column_id: (pos + 1) as u32,
        state: ColumnState::Public,
        physical_pos: pos as u32,
        logical_pos: pos as u32,
        ordinal_position: (pos + 1) as u32,
        charset: None,
        collation: None,
        comment: String::new(),
        generated_expr: None,
        generated_stored: false,
        generated_column_names: Vec::new(),
        generated_expr_ast: None,
        generated_program: None,
    }
}

fn decimal_fact_schema() -> TableSchema {
    let mut schema = TableSchema {
        id: 1,
        object_id: 1,
        active_generation: 1,
        database: "test".to_string(),
        name: "fact".to_string(),
        engine: "mydb".to_string(),
        charset: "utf8mb4".to_string(),
        collation: crate::charset::DEFAULT_COLLATION.to_string(),
        comment: String::new(),
        created_ms: 0,
        updated_ms: 0,
        table_type: "BASE TABLE".to_string(),
        columns: vec![
            col(
                "quantity",
                DataTypeKind::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
                0,
            ),
            col(
                "extended_price",
                DataTypeKind::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
                1,
            ),
            col(
                "discount",
                DataTypeKind::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
                2,
            ),
            col("ship_date", DataTypeKind::Date, 3),
        ],
        primary_key: Vec::new(),
        indexes: Vec::new(),
        check_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        fk_referenced_by: Vec::new(),
        stats: TableStats {
            analyze_revision: 0,
            explicit_histograms: std::collections::BTreeMap::new(),
            row_count: 0,
            heap_page_count: 0,
            sample_fraction: None,
            confidence: None,
            columns: std::collections::BTreeMap::new(),
            indexes: std::collections::BTreeMap::new(),
            partition_stats: std::collections::BTreeMap::new(),
            conditional_key_domains: Vec::new(),
            multi_column_stats: std::collections::BTreeMap::new(),
            modified_rows_since_analyze: 0,
            analyzed_at_unix_ms: Some(0),
            last_analyze_live_baseline: 0,
            last_analyze_modification_baseline: 0,
        },
        system: false,
        schema_version: 0,
        system_time_epoch_us: 0,
        row_generation_txn_id: 0,
        next_column_id: 5,
        active_layout_id: 1,
        row_layouts: Vec::new(),
        pending_primary_key: None,
        primary_definition_scn: 0,
        next_index_id: 0,
        primary_publication_job_id: 0,
        column_name_map: Default::default(),
        column_id_map: std::collections::HashMap::new(),
        row_codec: Default::default(),
        partitioning: None,
        pctfree: 0,
        pctused: 0,
        mysql_table_options: Vec::new(),
    };
    schema.build_column_index();
    schema
}

#[test]
fn decoded_predicate_resolves_collation_once_and_handles_boolean_tree() {
    let mut schema = decimal_fact_schema();
    schema.columns = vec![col("pad", DataTypeKind::VarChar { len: Some(120) }, 0)];
    schema.build_column_index();
    let expr = Expr::Binary {
        left: Box::new(Expr::Identifier("pad".into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(Literal::String("AbC".into()))),
    };
    let predicate = DecodedPredicateProgram::compile(&expr, &schema)
        .expect("simple text equality should compile");
    let mut results = vec![None; predicate.leaf_count()];
    assert!(predicate.put_string(0, b"abc", &mut results));
    assert_eq!(predicate.finish(&results), Some(true));
    results.fill(None);
    assert!(predicate.put_string(0, b"abd", &mut results));
    assert_eq!(predicate.finish(&results), Some(false));
    results.fill(None);
    assert!(!predicate.put_string(1, b"abc", &mut results));
    assert_eq!(predicate.finish(&results), None);
    results.fill(None);
    assert!(!predicate.put_string(0, b"\xff", &mut results));
    assert_eq!(predicate.finish(&results), None);
}

#[test]
fn decoded_substring_in_mixed_decimal_matches_scalar_three_valued_oracle() {
    use crate::engine::executor::vec::scan_bridge::PredicateValue;
    let mut schema = decimal_fact_schema();
    schema.columns = vec![
        col("s", DataTypeKind::VarChar { len: Some(120) }, 0),
        col(
            "n",
            DataTypeKind::Decimal {
                precision: Some(18),
                scale: Some(2),
            },
            1,
        ),
    ];
    schema.build_column_index();
    for call in [
        "SUBSTRING(s,1,2)",
        "SUBSTR(s,-2,2)",
        "SUBSTRING(s,0,2)",
        "SUBSTRING(s,1,0)",
        "SUBSTRING(s,-99,4)",
        "SUBSTRING(s,2)",
    ] {
        for negated in ["", "NOT"] {
            for null in ["", ", NULL"] {
                let member = format!("{call} {negated} IN ('13','a ','中🙂','x'{null})");
                for sql in [
                    format!("n > 0.00 AND {member}"),
                    format!("NOT (n <= 0.00 OR {member})"),
                ] {
                    let Statement::Select(select) =
                        crate::parser::parse_statement(&format!("SELECT * FROM fact WHERE {sql}"))
                            .unwrap()
                    else {
                        panic!("SELECT expected")
                    };
                    let mut expr = select.selection.unwrap();
                    crate::engine::expr::fold_constant_subexprs(&mut expr);
                    let mut decoded = DecodedPredicateProgram::compile(&expr, &schema)
                        .unwrap_or_else(|| panic!("mixed borrowed predicate: {sql}"));
                    decoded.prepare_for_scan(1_000_000.0);
                    let scalar = CompiledPredicate::compile(&expr, &schema).unwrap();
                    for text in [
                        None,
                        Some(""),
                        Some("13-x"),
                        Some("A "),
                        Some("a"),
                        Some("中🙂尾"),
                        Some("xx中🙂"),
                        Some("e\u{301}"),
                    ] {
                        for number in [None, Some(-1), Some(0), Some(123)] {
                            let mut results = vec![None; decoded.leaf_count()];
                            let s = if let Some(text) = text {
                                assert!(decoded.put_string(0, text.as_bytes(), &mut results));
                                Value::String(text.into())
                            } else {
                                assert!(decoded.put_null(0, &mut results));
                                Value::Null
                            };
                            let n = if let Some(number) = number {
                                assert!(decoded.put_numeric(
                                    1,
                                    PredicateValue::Decimal(number, 2),
                                    &mut results
                                ));
                                Value::DecimalCompact(number as i64, 2)
                            } else {
                                assert!(decoded.put_null(1, &mut results));
                                Value::Null
                            };
                            assert_eq!(
                                decoded.finish(&results),
                                Some(scalar.eval(&[s, n]).unwrap()),
                                "{sql}, text={text:?}, number={number:?}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn decoded_predicate_ascii_ci_like_matches_without_owned_value() {
    let mut schema = decimal_fact_schema();
    schema.columns = vec![col("name", DataTypeKind::VarChar { len: Some(55) }, 0)];
    schema.build_column_index();
    let expr = Expr::Like {
        expr: Box::new(Expr::Identifier("name".into())),
        pattern: Box::new(Expr::Literal(Literal::String("forest%".into()))),
        negated: false,
        escape_char: None,
    };
    let predicate = DecodedPredicateProgram::compile(&expr, &schema)
        .expect("borrowed ASCII LIKE should compile");
    let mut results = vec![None; predicate.leaf_count()];
    assert!(predicate.put_string(0, b"FoReSt green", &mut results));
    assert_eq!(predicate.finish(&results), Some(true));
    results.fill(None);
    assert!(predicate.put_string(0, b"desert green", &mut results));
    assert_eq!(predicate.finish(&results), Some(false));
}

#[test]
fn compiled_string_comparison_avoids_per_row_expr_program() {
    let mut schema = decimal_fact_schema();
    let pos = schema.columns.len();
    schema
        .columns
        .push(col("returnflag", DataTypeKind::Char { len: Some(1) }, pos));
    schema.build_column_index();
    let expr = Expr::Binary {
        left: Box::new(Expr::Identifier("returnflag".into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(Literal::String("R".into()))),
    };
    let predicate = CompiledPredicate::compile(&expr, &schema).expect("compile string cmp");
    assert!(matches!(predicate.root, PredicateNode::StringCmp { .. }));

    let mut row = vec![Value::Null; schema.columns.len()];
    row[pos] = Value::String("R".into());
    assert!(predicate.eval(&row).unwrap());
    row[pos] = Value::String("N".into());
    assert!(!predicate.eval(&row).unwrap());
}

#[test]
fn compiled_string_in_preserves_collation_and_null_semantics_on_typed_lane() {
    use crate::engine::executor::vec::scan_bridge::PredicateValue;

    let mut schema = decimal_fact_schema();
    let pos = schema.columns.len();
    schema
        .columns
        .push(col("ship_mode", DataTypeKind::Char { len: Some(10) }, pos));
    schema.build_column_index();
    let expr = Expr::InList {
        expr: Box::new(Expr::Identifier("ship_mode".into())),
        list: vec![
            Expr::Literal(Literal::String("MAIL".into())),
            Expr::Literal(Literal::String("SHIP".into())),
            Expr::Literal(Literal::Null),
        ],
        negated: false,
    };
    let predicate = CompiledPredicate::compile(&expr, &schema).expect("compile string IN");
    assert!(matches!(predicate.root, PredicateNode::StringIn { .. }));
    assert!(predicate.supports_typed_eval());

    let mut row = vec![Value::Null; schema.columns.len()];
    row[pos] = Value::SmallString(crate::types::SmallStringBuf::from_slice(b"mail"));
    let mut typed = vec![PredicateValue::Null; schema.columns.len()];
    typed[pos] = PredicateValue::Value(row[pos].clone());
    assert!(predicate.eval(&row).unwrap());
    assert_eq!(predicate.eval_typed(&typed).unwrap(), Some(true));

    row[pos] = Value::SmallString(crate::types::SmallStringBuf::from_slice(b"RAIL"));
    typed[pos] = PredicateValue::Value(row[pos].clone());
    assert!(!predicate.eval(&row).unwrap());
    assert_eq!(predicate.eval_typed(&typed).unwrap(), Some(false));
}

#[test]
fn compiled_same_type_column_comparison_stays_on_typed_lane() {
    use crate::engine::executor::vec::scan_bridge::PredicateValue;

    let mut schema = decimal_fact_schema();
    let commit_pos = schema.columns.len();
    schema
        .columns
        .push(col("commit_date", DataTypeKind::Date, commit_pos));
    let receipt_pos = schema.columns.len();
    schema
        .columns
        .push(col("receipt_date", DataTypeKind::Date, receipt_pos));
    schema.build_column_index();
    let expr = Expr::Binary {
        left: Box::new(Expr::Identifier("commit_date".into())),
        op: BinaryOp::Lt,
        right: Box::new(Expr::Identifier("receipt_date".into())),
    };
    let predicate = CompiledPredicate::compile(&expr, &schema).expect("compile column cmp");
    assert!(matches!(predicate.root, PredicateNode::ColumnCmp { .. }));
    assert!(predicate.supports_typed_eval());

    let commit = parse_yyyy_mm_dd_days("1994-06-15").unwrap();
    let receipt = parse_yyyy_mm_dd_days("1994-06-17").unwrap();
    let mut row = vec![Value::Null; schema.columns.len()];
    row[commit_pos] = Value::Date(commit);
    row[receipt_pos] = Value::Date(receipt);
    let mut typed = vec![PredicateValue::Null; schema.columns.len()];
    typed[commit_pos] = PredicateValue::Date(commit);
    typed[receipt_pos] = PredicateValue::Date(receipt);
    assert!(predicate.eval(&row).unwrap());
    assert_eq!(predicate.eval_typed(&typed).unwrap(), Some(true));
}

#[test]
fn compiled_timestamp_date_arithmetic_and_null_check_stay_on_typed_lanes() {
    use crate::engine::executor::vec::scan_bridge::PredicateValue;

    let mut schema = decimal_fact_schema();
    let timestamp_pos = schema.columns.len();
    schema.columns.push(col(
        "contract_timestamp",
        DataTypeKind::Timestamp { fsp: 6 },
        timestamp_pos,
    ));
    let duration_pos = schema.columns.len();
    schema
        .columns
        .push(col("duration", DataTypeKind::Int, duration_pos));
    schema.build_column_index();

    let timestamp = Expr::Identifier("contract_timestamp".into());
    let due = Expr::Binary {
        left: Box::new(Expr::Function {
            name: "DATE".into(),
            args: vec![timestamp.clone()],
            distinct: false,
        }),
        op: BinaryOp::Add,
        right: Box::new(Expr::Identifier("duration".into())),
    };
    let expr = Expr::Binary {
        left: Box::new(Expr::IsNotNull(Box::new(timestamp))),
        op: BinaryOp::And,
        right: Box::new(Expr::Binary {
            left: Box::new(due),
            op: BinaryOp::Lt,
            right: Box::new(Expr::BoundValue(Value::Date(
                parse_yyyy_mm_dd_days("1970-01-10").unwrap(),
            ))),
        }),
    };
    let predicate = CompiledPredicate::compile(&expr, &schema).expect("compile predicate");
    assert!(predicate.supports_typed_eval());

    for (timestamp, duration) in [
        (Some(0), 5),
        (Some(0), 20),
        (Some(crate::types::ZERO_DATETIME), 5),
        (None, 5),
    ] {
        let mut row = vec![Value::Null; schema.columns.len()];
        let mut typed = vec![PredicateValue::Null; schema.columns.len()];
        if let Some(timestamp) = timestamp {
            row[timestamp_pos] = Value::Timestamp(timestamp);
            typed[timestamp_pos] = PredicateValue::Timestamp(timestamp);
        }
        row[duration_pos] = Value::Int(duration);
        typed[duration_pos] = PredicateValue::I64(duration);
        let oracle = predicate.eval(&row).unwrap();
        assert_eq!(predicate.eval_typed(&typed).unwrap(), Some(oracle));
    }
}

fn decimal_fact_filter() -> Expr {
    let shipdate = Expr::Identifier("ship_date".to_string());
    let discount = Expr::Identifier("discount".to_string());
    let quantity = Expr::Identifier("quantity".to_string());
    Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(Expr::Binary {
                left: Box::new(shipdate.clone()),
                op: BinaryOp::GtEq,
                right: Box::new(Expr::Literal(Literal::String("1994-01-01".to_string()))),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::Binary {
                left: Box::new(shipdate),
                op: BinaryOp::Lt,
                right: Box::new(Expr::BoundValue(Value::String("1995-01-01".to_string()))),
            }),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::Binary {
            left: Box::new(Expr::Between {
                expr: Box::new(discount),
                low: Box::new(Expr::Literal(Literal::Float(0.05))),
                high: Box::new(Expr::Literal(Literal::Float(0.07))),
                negated: false,
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::Binary {
                left: Box::new(quantity),
                op: BinaryOp::Lt,
                right: Box::new(Expr::Literal(Literal::Int(24))),
            }),
        }),
    }
}

#[test]
fn projected_expression_schema_rebases_columns_without_copying_statistics() {
    let mut schema = decimal_fact_schema();
    schema.stats.row_count = 60_000_000;
    let projected = projected_schema_view(&schema, &[3, 0]);
    assert_eq!(projected.columns[0].name, "ship_date");
    assert_eq!(projected.columns[0].column_id, schema.columns[3].column_id);
    assert_eq!(projected.columns[0].physical_pos, 0);
    assert_eq!(projected.columns[1].physical_pos, 1);
    assert_eq!(projected.column_index("quantity").unwrap(), 1);
    assert_eq!(projected.stats.row_count, 0);
    assert!(projected.stats.columns.is_empty());
    let expr = Expr::Binary {
        left: Box::new(Expr::Identifier("quantity".into())),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Literal(Literal::Int(10))),
    };
    let values = vec![Value::Date(0), Value::DecimalCompact(1200, 2)];
    let compiled = compile_projected_row_predicate(&expr, &projected).unwrap();
    assert!(compiled.eval(&values).unwrap());
    assert_eq!(
        compiled.eval(&values).unwrap(),
        crate::engine::expr::eval_bool(&expr, &projected, &values).unwrap()
    );
}

#[test]
fn compiled_decimal_predicate_matches_generic_eval() {
    let schema = decimal_fact_schema();
    let expr = decimal_fact_filter();
    let compiled = CompiledPredicate::compile(&expr, &schema).expect("compiled predicate");
    let values = vec![
        Value::DecimalCompact(2300, 2),
        Value::DecimalCompact(100000, 2),
        Value::DecimalCompact(6, 2),
        Value::Date(parse_yyyy_mm_dd_days("1994-06-15").unwrap()),
    ];
    assert_eq!(
        compiled.eval(&values).unwrap(),
        crate::engine::expr::eval_bool(&expr, &schema, &values).unwrap()
    );
    let rejected = vec![
        Value::DecimalCompact(2500, 2),
        Value::DecimalCompact(100000, 2),
        Value::DecimalCompact(6, 2),
        Value::Date(parse_yyyy_mm_dd_days("1994-06-15").unwrap()),
    ];
    assert_eq!(
        compiled.eval(&rejected).unwrap(),
        crate::engine::expr::eval_bool(&expr, &schema, &rejected).unwrap()
    );
}

#[test]
fn typed_compound_predicate_matches_value_oracle() {
    use crate::engine::executor::vec::scan_bridge::PredicateValue;

    let schema = decimal_fact_schema();
    let expr = decimal_fact_filter();
    let compiled = CompiledPredicate::compile(&expr, &schema).expect("compiled predicate");
    assert!(
        compiled.supports_typed_eval(),
        "DATE constants and numeric BETWEEN should keep the whole Q6-shaped predicate on typed lanes"
    );
    for (quantity, discount, date) in [
        (2300, 6, "1994-06-15"),
        (2400, 6, "1994-06-15"),
        (2300, 4, "1994-06-15"),
        (2300, 7, "1995-01-01"),
    ] {
        let date = parse_yyyy_mm_dd_days(date).unwrap();
        let row = vec![
            Value::DecimalCompact(quantity, 2),
            Value::DecimalCompact(100000, 2),
            Value::DecimalCompact(discount, 2),
            Value::Date(date),
        ];
        let typed = vec![
            PredicateValue::Decimal(quantity as i128, 2),
            PredicateValue::Decimal(100000, 2),
            PredicateValue::Decimal(discount as i128, 2),
            PredicateValue::Date(date),
        ];
        let oracle = compiled.eval(&row).unwrap();
        assert_eq!(compiled.eval_typed(&typed).unwrap(), Some(oracle));
    }
}

#[test]
fn typed_date_comparison_rejects_uncoercible_constant() {
    let schema = decimal_fact_schema();
    let expr = Expr::Binary {
        left: Box::new(Expr::Identifier("ship_date".to_string())),
        op: BinaryOp::GtEq,
        right: Box::new(Expr::Literal(Literal::String("not-a-date".to_string()))),
    };
    let compiled = CompiledPredicate::compile(&expr, &schema).expect("program fallback");
    assert!(
        !compiled.supports_typed_eval(),
        "invalid temporal text must retain the semantic oracle"
    );
    let row = vec![
        Value::DecimalCompact(2300, 2),
        Value::DecimalCompact(100000, 2),
        Value::DecimalCompact(6, 2),
        Value::Date(parse_yyyy_mm_dd_days("1994-06-15").unwrap()),
    ];
    let compiled_err = compiled.eval(&row).unwrap_err();
    let oracle_err = crate::engine::expr::eval_bool(&expr, &schema, &row).unwrap_err();
    assert_eq!(compiled_err.code(), 1525);
    assert_eq!(compiled_err.sql_state().as_ref(), "HY000");
    assert_eq!(
        (compiled_err.code(), compiled_err.sql_state()),
        (oracle_err.code(), oracle_err.sql_state())
    );
}

#[test]
fn projected_temporal_between_precoerces_bounds_and_matches_oracle() {
    let mut schema = decimal_fact_schema();
    schema.columns.push(col(
        "created_at",
        DataTypeKind::DateTime { fsp: 6 },
        schema.columns.len(),
    ));
    schema.columns.push(col(
        "observed_at",
        DataTypeKind::Timestamp { fsp: 6 },
        schema.columns.len(),
    ));
    schema.build_column_index();

    for (column, stored) in [
        (
            "ship_date",
            Value::Date(parse_yyyy_mm_dd_days("1994-06-15").unwrap()),
        ),
        (
            "created_at",
            Value::DateTime(
                crate::engine::expr::parse_datetime(&Value::String(
                    "1994-06-15 12:30:00".to_string(),
                ))
                .unwrap()
                .and_utc()
                .timestamp_micros(),
            ),
        ),
        (
            "observed_at",
            Value::Timestamp(
                crate::engine::expr::parse_datetime(&Value::String(
                    "1994-06-15 12:30:00".to_string(),
                ))
                .unwrap()
                .and_utc()
                .timestamp_micros(),
            ),
        ),
    ] {
        let expr = Expr::Between {
            expr: Box::new(Expr::Identifier(column.to_string())),
            low: Box::new(Expr::Literal(Literal::String(
                "1994-01-01 00:00:00".to_string(),
            ))),
            high: Box::new(Expr::Literal(Literal::String(
                "1994-12-31 23:59:59".to_string(),
            ))),
            negated: false,
        };
        let compiled = compile_projected_row_predicate(&expr, &schema).expect("compiled BETWEEN");
        let CompiledRowPredicate::Between { low, high, .. } = &compiled else {
            panic!("expected compiled BETWEEN");
        };
        match schema
            .columns
            .get(projected_column_position(&Expr::Identifier(column.to_string()), &schema).unwrap())
            .map(|column| &column.data_type)
        {
            Some(DataTypeKind::Date) => {
                assert!(matches!(low, Value::Date(_)));
                // A non-midnight DATETIME boundary cannot be truncated to DATE.
                assert!(matches!(high, Value::String(_)));
            }
            Some(DataTypeKind::DateTime { .. }) => {
                assert!(matches!(low, Value::DateTime(_)));
                assert!(matches!(high, Value::DateTime(_)));
            }
            Some(DataTypeKind::Timestamp { .. }) => {
                assert!(matches!(low, Value::Timestamp(_)));
                assert!(matches!(high, Value::Timestamp(_)));
            }
            other => panic!("unexpected temporal type: {other:?}"),
        }
        let mut row = vec![
            Value::DecimalCompact(2300, 2),
            Value::DecimalCompact(100000, 2),
            Value::DecimalCompact(6, 2),
            Value::Date(parse_yyyy_mm_dd_days("1994-06-15").unwrap()),
            Value::DateTime(0),
            Value::Timestamp(0),
        ];
        let column_pos = schema.column_index(column).unwrap();
        row[column_pos] = stored;
        assert_eq!(
            compiled.eval(&row).unwrap(),
            crate::engine::expr::eval_bool(&expr, &schema, &row).unwrap()
        );
    }
}

#[test]
fn projected_temporal_comparisons_precoerce_once_and_match_oracle() {
    let mut schema = decimal_fact_schema();
    schema.columns.push(col(
        "created_at",
        DataTypeKind::DateTime { fsp: 6 },
        schema.columns.len(),
    ));
    schema.columns.push(col(
        "observed_at",
        DataTypeKind::Timestamp { fsp: 6 },
        schema.columns.len(),
    ));
    schema.build_column_index();
    let instant =
        crate::engine::expr::parse_datetime(&Value::String("1994-06-15 12:30:00".to_string()))
            .unwrap()
            .and_utc()
            .timestamp_micros();
    let timestamp_instant =
        crate::engine::expr::session_local_datetime_to_utc_timestamp_micros(instant).unwrap();
    for (column, literal, stored) in [
        (
            "ship_date",
            "1994-06-15",
            Value::Date(parse_yyyy_mm_dd_days("1994-06-15").unwrap()),
        ),
        (
            "created_at",
            "1994-06-15 12:30:00",
            Value::DateTime(instant),
        ),
        (
            "observed_at",
            "1994-06-15 12:30:00",
            Value::Timestamp(timestamp_instant),
        ),
    ] {
        for op in [BinaryOp::Lt, BinaryOp::LtEq, BinaryOp::Gt, BinaryOp::GtEq] {
            let expr = Expr::Binary {
                left: Box::new(Expr::Identifier(column.to_string())),
                op,
                right: Box::new(Expr::Literal(Literal::String(literal.to_string()))),
            };
            let compiled =
                compile_projected_row_predicate(&expr, &schema).expect("compiled comparison");
            let mut row = vec![
                Value::DecimalCompact(2300, 2),
                Value::DecimalCompact(100000, 2),
                Value::DecimalCompact(6, 2),
                Value::Date(parse_yyyy_mm_dd_days("1994-06-15").unwrap()),
                Value::DateTime(0),
                Value::Timestamp(0),
            ];
            row[schema.column_index(column).unwrap()] = stored.clone();
            assert_eq!(
                compiled.eval(&row).unwrap(),
                crate::engine::expr::eval_bool(&expr, &schema, &row).unwrap(),
                "{column} {op:?} {literal}"
            );
        }
    }
}

#[test]
fn typed_scan_predicate_compiles_timestamp_between_without_value_fallback() {
    let mut schema = decimal_fact_schema();
    schema.columns.push(col(
        "observed_at",
        DataTypeKind::Timestamp { fsp: 6 },
        schema.columns.len(),
    ));
    schema.build_column_index();
    let expr = Expr::Between {
        expr: Box::new(Expr::Identifier("observed_at".to_string())),
        low: Box::new(Expr::Literal(Literal::String(
            "2023-11-01 00:00:00".to_string(),
        ))),
        high: Box::new(Expr::Literal(Literal::String(
            "2023-12-31 00:00:00".to_string(),
        ))),
        negated: false,
    };
    let compiled = CompiledPredicate::compile(&expr, &schema).expect("timestamp BETWEEN");
    assert!(
        compiled.supports_typed_eval(),
        "timestamp range must stay in the predicate-first typed lane"
    );
    let (_, low, high) = compiled
        .inclusive_timestamp_range()
        .expect("timestamp BETWEEN must expose the direct decode range");
    assert!(low < high);
    let instant =
        crate::engine::expr::parse_datetime(&Value::String("2023-11-15 12:00:00".to_string()))
            .unwrap()
            .and_utc()
            .timestamp_micros();
    let timestamp =
        crate::engine::expr::session_local_datetime_to_utc_timestamp_micros(instant).unwrap();
    let mut typed =
        vec![crate::engine::executor::vec::scan_bridge::PredicateValue::Null; schema.columns.len()];
    typed[schema.column_index("observed_at").unwrap()] =
        crate::engine::executor::vec::scan_bridge::PredicateValue::Timestamp(timestamp);
    assert_eq!(compiled.eval_typed(&typed).unwrap(), Some(true));
}

#[test]
fn compiled_runtime_filter_uses_fixed_keys_and_preserves_null_semantics() {
    let mut schema = decimal_fact_schema();
    schema.columns = vec![
        col("a", DataTypeKind::BigInt, 0),
        col("b", DataTypeKind::Int, 1),
        col("d", DataTypeKind::Date, 2),
        col("text_key", DataTypeKind::VarChar { len: Some(20) }, 3),
    ];
    schema.build_column_index();

    let values = vec![
        Value::Int(-7),
        Value::UInt(11),
        Value::Date(parse_yyyy_mm_dd_days("1994-06-15").unwrap()),
        Value::String("alpha".to_string()),
    ];
    let mut bloom = crate::engine::executor::select::bloom::BloomFilter::new(8);
    let fixed_key = [
        ValueKey::from(&values[0]),
        ValueKey::from(&values[1]),
        ValueKey::from(&values[2]),
    ];
    bloom.insert(&fixed_key[..]);
    let filter = RuntimeFilter::BloomKey {
        keys: ["a", "b", "d"]
            .into_iter()
            .map(|name| Expr::Identifier(name.to_string()))
            .collect(),
        null_safe: vec![false, true, false],
        filter: Arc::new(bloom),
        rejected: None,
    };
    let compiled = compile_runtime_filter_probe_projected(&filter, &schema, &[0, 1, 2, 3]).unwrap();
    assert!(matches!(
        &compiled.key,
        CompiledRuntimeFilterKey::Fixed3 { .. }
    ));
    assert!(runtime_filters_admit_values_indexed(
        &[Some(compiled.clone())],
        &values
    ));

    let mut rejected = values.clone();
    rejected[1] = Value::UInt(12);
    assert!(!runtime_filters_admit_values_indexed(
        &[Some(compiled.clone())],
        &rejected
    ));

    let mut unsafe_null = values.clone();
    unsafe_null[0] = Value::Null;
    assert!(
        !runtime_filters_admit_values_indexed(&[Some(compiled)], &unsafe_null),
        "ordinary equality NULL cannot match and should be rejected before probing"
    );

    let mut string_bloom = crate::engine::executor::select::bloom::BloomFilter::new(4);
    let string_key = [ValueKey::from(&values[3])];
    string_bloom.insert(&string_key[..]);
    let string_filter = RuntimeFilter::BloomKey {
        keys: vec![Expr::Identifier("text_key".to_string())],
        null_safe: vec![false],
        filter: Arc::new(string_bloom),
        rejected: None,
    };
    let generic =
        compile_runtime_filter_probe_projected(&string_filter, &schema, &[0, 1, 2, 3]).unwrap();
    assert!(matches!(
        &generic.key,
        CompiledRuntimeFilterKey::Generic { .. }
    ));
    assert!(runtime_filters_admit_values_indexed(
        &[Some(generic)],
        &values
    ));
}

#[test]
fn compiled_exact_integer_runtime_filter_has_no_false_positives() {
    let mut schema = decimal_fact_schema();
    schema.columns = vec![
        col("a", DataTypeKind::BigInt, 0),
        col("text_key", DataTypeKind::VarChar { len: Some(20) }, 1),
    ];
    schema.build_column_index();

    let mut builder =
        crate::engine::executor::select::bloom::ExactIntKeySetBuilder::with_capacity(4).unwrap();
    for key in [-7, 0, 11, 11] {
        builder.insert(key);
    }
    let filter = RuntimeFilter::ExactIntKey {
        keys: vec![Expr::Identifier("a".to_string())],
        null_safe: vec![false],
        filter: Arc::new(builder.finish().unwrap()),
        rejected: Arc::new(std::sync::OnceLock::new()),
    };
    let compiled = compile_runtime_filter_probe_projected(&filter, &schema, &[0, 1]).unwrap();
    assert!(matches!(
        &compiled.key,
        CompiledRuntimeFilterKey::Fixed1 { index: 0, .. }
    ));
    for value in [Value::Int(-7), Value::UInt(0), Value::UInt(11)] {
        assert!(runtime_filters_admit_values_indexed(
            &[Some(compiled.clone())],
            &[value, Value::String("unused".to_string())]
        ));
    }
    use crate::engine::executor::vec::scan_bridge::PredicateValue;
    let mut rejected_count = [0u64];
    assert!(runtime_filters_admit_predicate_values_indexed_deferred(
        &[Some(compiled.clone())],
        &[PredicateValue::I64(-7), PredicateValue::Null],
        &mut rejected_count,
    ));
    assert_eq!(rejected_count, [0]);
    assert!(!runtime_filters_admit_predicate_values_indexed_deferred(
        &[Some(compiled.clone())],
        &[PredicateValue::I64(12), PredicateValue::Null],
        &mut rejected_count,
    ));
    assert_eq!(rejected_count, [1]);
    assert!(!runtime_filters_admit_values_indexed(
        &[Some(compiled.clone())],
        &[Value::Int(12), Value::String("unused".to_string())]
    ));
    assert!(!runtime_filters_admit_values_indexed(
        &[Some(compiled.clone())],
        &[Value::Null, Value::String("unused".to_string())]
    ));
    assert!(compiled.contains(&[ValueKey::string_from_str("schema mismatch")]));
}

#[test]
fn compiled_integer_pair_runtime_filter_uses_typed_probe_lane() {
    use crate::engine::executor::vec::scan_bridge::PredicateValue;

    let mut schema = decimal_fact_schema();
    schema.columns = vec![
        col("part_id", DataTypeKind::BigInt, 0),
        col("supplier_id", DataTypeKind::Int, 1),
    ];
    schema.build_column_index();
    let mut bloom = BloomFilter::new_int_pair(4);
    bloom.insert_int_pair(7, 11);
    bloom.insert_int_pair(-3, 9);
    let filter = RuntimeFilter::BloomKey {
        keys: ["part_id", "supplier_id"]
            .into_iter()
            .map(|name| Expr::Identifier(name.to_string()))
            .collect(),
        null_safe: vec![false, false],
        filter: Arc::new(bloom),
        rejected: None,
    };
    let probes = vec![Some(
        compile_runtime_filter_probe_projected(&filter, &schema, &[0, 1]).unwrap(),
    )];
    let mut rejected = [0u64];
    assert!(runtime_filters_admit_predicate_values_indexed_deferred(
        &probes,
        &[PredicateValue::I64(7), PredicateValue::I64(11)],
        &mut rejected,
    ));
    assert!(!runtime_filters_admit_predicate_values_indexed_deferred(
        &probes,
        &[PredicateValue::I64(7), PredicateValue::I64(12)],
        &mut rejected,
    ));
    assert_eq!(rejected, [1]);
    assert!(
        probes[0]
            .as_ref()
            .unwrap()
            .contains(&[ValueKey::string_from_str("schema mismatch")]),
        "unsupported runtime value must defer to the parent join"
    );
}

#[test]
fn runtime_filter_rejections_are_deferred_and_flushed_exactly() {
    use std::sync::atomic::Ordering;

    let mut schema = decimal_fact_schema();
    schema.columns = vec![col("a", DataTypeKind::BigInt, 0)];
    schema.build_column_index();
    let mut builder =
        crate::engine::executor::select::bloom::ExactIntKeySetBuilder::with_capacity(2).unwrap();
    builder.insert(7);
    let rejected = Arc::new(std::sync::OnceLock::new());
    let observed = Arc::new(std::sync::atomic::AtomicU64::new(0));
    rejected.set(Arc::clone(&observed)).unwrap();
    let filter = RuntimeFilter::ExactIntKey {
        keys: vec![Expr::Identifier("a".to_string())],
        null_safe: vec![false],
        filter: Arc::new(builder.finish().unwrap()),
        rejected,
    };
    let probes = vec![Some(
        compile_runtime_filter_probe_projected(&filter, &schema, &[0]).unwrap(),
    )];
    let mut counts = vec![0u64; probes.len()];
    assert!(runtime_filters_admit_values_indexed_deferred(
        &probes,
        &[Value::Int(7)],
        &mut counts,
    ));
    for value in [8, 9, 10] {
        assert!(!runtime_filters_admit_values_indexed_deferred(
            &probes,
            &[Value::Int(value)],
            &mut counts,
        ));
    }
    assert_eq!(observed.load(Ordering::Relaxed), 0);
    flush_runtime_filter_rejections(&probes, &mut counts);
    assert_eq!(observed.load(Ordering::Relaxed), 3);
    assert_eq!(counts, [0]);
}

#[test]
fn compiled_runtime_keyset_filters_projected_candidates() {
    let mut schema = decimal_fact_schema();
    schema.columns = vec![
        col("payload", DataTypeKind::VarChar { len: Some(20) }, 0),
        col("dimension_id", DataTypeKind::BigInt, 1),
    ];
    schema.build_column_index();
    let mut membership = BloomFilter::new(2);
    membership.insert(&[ValueKey::from(&Value::Int(7))]);
    membership.insert(&[ValueKey::from(&Value::UInt(11))]);
    let filter = RuntimeFilter::KeySet {
        keys: vec![Expr::Identifier("dimension_id".to_string())],
        null_safe: vec![false],
        values: Arc::new(vec![vec![Value::Int(7)], vec![Value::UInt(11)]]),
        filter: Arc::new(membership),
    };
    let compiled =
        compile_runtime_filter_probe_projected(&filter, &schema, &[1]).expect("compiled");
    assert!(matches!(
        &compiled.key,
        CompiledRuntimeFilterKey::Fixed1 { index: 0, .. }
    ));
    assert!(runtime_filters_admit_values_indexed(
        &[Some(compiled.clone())],
        &[Value::Int(7)]
    ));
    assert!(!runtime_filters_admit_values_indexed(
        &[Some(compiled.clone())],
        &[Value::Int(8)]
    ));
    assert!(
        !runtime_filters_admit_values_indexed(&[Some(compiled)], &[Value::Null]),
        "ordinary equality can reject NULL before the parent join"
    );
    let mut null_membership = BloomFilter::new(1);
    null_membership.insert(&[ValueKey::from(&Value::Null)]);
    let null_safe = RuntimeFilter::KeySet {
        keys: vec![Expr::Identifier("dimension_id".to_string())],
        null_safe: vec![true],
        values: Arc::new(vec![vec![Value::Null]]),
        filter: Arc::new(null_membership),
    };
    let compiled_null_safe =
        compile_runtime_filter_probe_projected(&null_safe, &schema, &[1]).expect("compiled");
    assert!(
        runtime_filters_admit_values_indexed(&[Some(compiled_null_safe)], &[Value::Null]),
        "NULL-safe equality must retain a matching NULL build key"
    );
    let table = PlanTable::new(1, Arc::new(schema), Some("f".to_string()));
    assert_eq!(
        runtime_filter_schema_indices(&filter, &table),
        Some(vec![1])
    );
    let foreign = RuntimeFilter::KeySet {
        keys: vec![Expr::CompoundIdentifier(vec![
            "g".to_string(),
            "dimension_id".to_string(),
        ])],
        null_safe: vec![false],
        values: Arc::new(vec![vec![Value::Int(8)]]),
        filter: Arc::new(BloomFilter::new(1)),
    };
    assert_eq!(
        runtime_filter_schema_indices(&foreign, &table),
        None,
        "a key qualified by another relation must not bind by column name"
    );
}

#[test]
fn runtime_keyset_index_estimate_scales_and_saturates() {
    assert_eq!(
        estimate_runtime_keyset_index_entries(30_000_000, 50_000, 10_000),
        Some(6_000_000)
    );
    assert_eq!(
        estimate_runtime_keyset_index_entries(30_000_000, 50_000, 1),
        Some(600)
    );
    assert_eq!(
        estimate_runtime_keyset_index_entries(30_000_000, 50_000, 60_000),
        Some(30_000_000)
    );
    assert_eq!(estimate_runtime_keyset_index_entries(100, 0, 1), None);
}

#[test]
fn compiled_not_between_preserves_where_null_semantics() {
    let schema = decimal_fact_schema();
    let expr = Expr::Between {
        expr: Box::new(Expr::Identifier("quantity".to_string())),
        low: Box::new(Expr::Literal(Literal::Null)),
        high: Box::new(Expr::Literal(Literal::Float(24.0))),
        negated: true,
    };
    let compiled = CompiledPredicate::compile(&expr, &schema).expect("compiled predicate");
    for quantity in [
        Value::DecimalCompact(2300, 2),
        Value::DecimalCompact(2500, 2),
    ] {
        let row = vec![
            quantity,
            Value::DecimalCompact(100000, 2),
            Value::DecimalCompact(6, 2),
            Value::Date(parse_yyyy_mm_dd_days("1994-06-15").unwrap()),
        ];
        assert_eq!(
            compiled.eval(&row).unwrap(),
            crate::engine::expr::eval_bool(&expr, &schema, &row).unwrap()
        );
    }
}

#[test]
fn compiled_predicate_handles_expression_in_const_set() {
    let mut schema = TableSchema {
        id: 2,
        object_id: 2,
        active_generation: 1,
        database: "test".to_string(),
        name: "addr".to_string(),
        engine: "mydb".to_string(),
        charset: "utf8mb4".to_string(),
        collation: crate::charset::DEFAULT_COLLATION.to_string(),
        comment: String::new(),
        created_ms: 0,
        updated_ms: 0,
        table_type: "BASE TABLE".to_string(),
        columns: vec![col("zip", DataTypeKind::VarChar { len: Some(10) }, 0)],
        primary_key: Vec::new(),
        indexes: Vec::new(),
        check_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        fk_referenced_by: Vec::new(),
        stats: TableStats {
            analyze_revision: 0,
            explicit_histograms: std::collections::BTreeMap::new(),
            row_count: 0,
            heap_page_count: 0,
            sample_fraction: None,
            confidence: None,
            columns: std::collections::BTreeMap::new(),
            indexes: std::collections::BTreeMap::new(),
            partition_stats: std::collections::BTreeMap::new(),
            conditional_key_domains: Vec::new(),
            multi_column_stats: std::collections::BTreeMap::new(),
            modified_rows_since_analyze: 0,
            analyzed_at_unix_ms: Some(0),
            last_analyze_live_baseline: 0,
            last_analyze_modification_baseline: 0,
        },
        system: false,
        schema_version: 0,
        system_time_epoch_us: 0,
        row_generation_txn_id: 0,
        next_column_id: 2,
        active_layout_id: 1,
        row_layouts: Vec::new(),
        pending_primary_key: None,
        primary_definition_scn: 0,
        next_index_id: 0,
        primary_publication_job_id: 0,
        column_name_map: Default::default(),
        column_id_map: std::collections::HashMap::new(),
        row_codec: Default::default(),
        partitioning: None,
        pctfree: 0,
        pctused: 0,
        mysql_table_options: Vec::new(),
    };
    schema.build_column_index();
    let expr = Expr::InList {
        expr: Box::new(Expr::Function {
            name: "SUBSTRING".to_string(),
            args: vec![
                Expr::Identifier("zip".to_string()),
                Expr::Literal(Literal::Int(1)),
                Expr::Literal(Literal::Int(5)),
            ],
            distinct: false,
        }),
        list: ["12345", "23456", "34567", "45678", "56789"]
            .into_iter()
            .map(|s| Expr::Literal(Literal::String(s.to_string())))
            .collect(),
        negated: false,
    };
    let compiled = CompiledPredicate::compile(&expr, &schema).expect("compiled predicate");
    for values in [
        vec![Value::String("12345-6789".to_string())],
        vec![Value::String("99999".to_string())],
        vec![Value::Null],
    ] {
        assert_eq!(
            compiled.eval(&values).unwrap(),
            crate::engine::expr::eval_bool(&expr, &schema, &values).unwrap()
        );
    }
}

#[test]
fn compiled_decimal_multiply_matches_generic_eval() {
    let schema = decimal_fact_schema();
    let expr = Expr::Binary {
        left: Box::new(Expr::Identifier("extended_price".to_string())),
        op: BinaryOp::Mul,
        right: Box::new(Expr::Identifier("discount".to_string())),
    };
    let compiled = CompiledNumericExpr::compile(&expr, &schema).expect("compiled numeric expr");
    let values = vec![
        Value::DecimalCompact(2300, 2),
        Value::DecimalCompact(123456, 2),
        Value::DecimalCompact(6, 2),
        Value::Date(parse_yyyy_mm_dd_days("1994-06-15").unwrap()),
    ];
    let fast = compiled.eval(&values).unwrap().unwrap();
    let generic = crate::engine::expr::eval_expr(&expr, &schema, &values).unwrap();
    assert_eq!(fast, value_to_f64_fast(&generic).unwrap());
}

#[test]
fn compiled_numeric_expr_matches_generic_null_and_div_zero() {
    let schema = decimal_fact_schema();
    let expr = Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(Expr::Identifier("extended_price".to_string())),
            op: BinaryOp::Add,
            right: Box::new(Expr::Identifier("quantity".to_string())),
        }),
        op: BinaryOp::Div,
        right: Box::new(Expr::Identifier("discount".to_string())),
    };
    let compiled = CompiledNumericExpr::compile(&expr, &schema).expect("compiled numeric expr");
    let values = vec![
        Value::DecimalCompact(2300, 2),
        Value::DecimalCompact(123456, 2),
        Value::DecimalCompact(6, 2),
        Value::Date(parse_yyyy_mm_dd_days("1994-06-15").unwrap()),
    ];
    let fast = compiled.eval(&values).unwrap().unwrap();
    let generic = crate::engine::expr::eval_expr(&expr, &schema, &values).unwrap();
    // The f64 fast path (numeric predicates only) keeps full precision,
    // whereas the generic path now rounds exact division to a scaled
    // DECIMAL per MySQL (`div_precision_increment`). They therefore agree
    // up to that rounding, not bit-for-bit.
    let generic_f64 = value_to_f64_fast(&generic).unwrap();
    assert!(
        (fast - generic_f64).abs() <= 1e-3,
        "fast {fast} vs generic {generic_f64}"
    );

    let mut with_null = values.clone();
    with_null[1] = Value::Null;
    assert_eq!(compiled.eval(&with_null).unwrap(), None);

    let mut div_zero = values;
    div_zero[2] = Value::DecimalCompact(0, 2);
    assert_eq!(compiled.eval(&div_zero).unwrap(), None);
}

#[test]
fn batch_decimal_overflow_declines_to_scalar_oracle() {
    use crate::engine::executor::vec::batch::ColumnVector;

    let add = CompiledNumericExpr::Binary {
        left: Box::new(CompiledNumericExpr::Column(0)),
        op: BinaryOp::Add,
        right: Box::new(CompiledNumericExpr::Const(Value::Int(1))),
    };
    assert!(
        add.try_eval_batch_decimal(
            &[ColumnVector::Decimal64 {
                data: vec![i64::MAX],
                scale: 0,
                nulls: None,
            }],
            1,
        )
        .is_none(),
        "i64 add overflow must never wrap inside a decimal batch"
    );

    let align = CompiledNumericExpr::Binary {
        left: Box::new(CompiledNumericExpr::Column(0)),
        op: BinaryOp::Add,
        right: Box::new(CompiledNumericExpr::Const(Value::DecimalCompact(1, 1))),
    };
    assert!(
        align
            .try_eval_batch_decimal(
                &[ColumnVector::Decimal64 {
                    data: vec![i64::MAX],
                    scale: 0,
                    nulls: None,
                }],
                1,
            )
            .is_none(),
        "scale alignment overflow must decline the batch kernel"
    );

    let multiply = CompiledNumericExpr::Binary {
        left: Box::new(CompiledNumericExpr::Column(0)),
        op: BinaryOp::Mul,
        right: Box::new(CompiledNumericExpr::Const(Value::Int(2))),
    };
    assert!(
        multiply
            .try_eval_batch_decimal(
                &[ColumnVector::Decimal128 {
                    data: vec![i128::MAX],
                    scale: 0,
                    nulls: None,
                }],
                1,
            )
            .is_none(),
        "i128 multiply overflow must decline the batch kernel"
    );
}

#[test]
fn batch_decimal_subtracts_wide_products_without_scalar_fallback() {
    use crate::engine::executor::vec::batch::ColumnVector;

    let expression = CompiledNumericExpr::Binary {
        left: Box::new(CompiledNumericExpr::Binary {
            left: Box::new(CompiledNumericExpr::Column(0)),
            op: BinaryOp::Mul,
            right: Box::new(CompiledNumericExpr::Column(1)),
        }),
        op: BinaryOp::Sub,
        right: Box::new(CompiledNumericExpr::Binary {
            left: Box::new(CompiledNumericExpr::Column(2)),
            op: BinaryOp::Mul,
            right: Box::new(CompiledNumericExpr::Column(3)),
        }),
    };
    let columns = [
        ColumnVector::Decimal64 {
            data: vec![10_000, 20_000],
            scale: 2,
            nulls: None,
        },
        ColumnVector::Decimal64 {
            data: vec![90, 80],
            scale: 2,
            nulls: None,
        },
        ColumnVector::Decimal64 {
            data: vec![5_000, 3_000],
            scale: 2,
            nulls: None,
        },
        ColumnVector::Decimal64 {
            data: vec![200, 400],
            scale: 2,
            nulls: None,
        },
    ];
    let result = expression
        .try_eval_batch_decimal(&columns, 2)
        .expect("wide products should stay in the batch decimal kernel");
    let ColumnVector::Decimal128 { data, scale, .. } = result else {
        panic!("wide products must produce a Decimal128 lane")
    };
    assert_eq!(scale, 4);
    assert_eq!(data, [-100_000, 400_000]);
}

#[test]
fn batch_integer_arithmetic_is_exact_and_declines_overflow() {
    use crate::engine::executor::vec::batch::ColumnVector;
    use crate::engine::executor::vec::bitmap::Bitmap;

    let expression = CompiledNumericExpr::Binary {
        left: Box::new(CompiledNumericExpr::Binary {
            left: Box::new(CompiledNumericExpr::Column(0)),
            op: BinaryOp::Mul,
            right: Box::new(CompiledNumericExpr::Const(Value::UInt(17))),
        }),
        op: BinaryOp::Add,
        right: Box::new(CompiledNumericExpr::Column(1)),
    };
    let mut validity = Bitmap::new_all_valid(3);
    validity.set(1, false);
    let result = expression
        .try_eval_batch_integer(
            &[
                ColumnVector::I64 {
                    data: vec![10, 20, 30],
                    nulls: Some(validity.clone()),
                },
                ColumnVector::I64 {
                    data: vec![1, 2, 3],
                    nulls: None,
                },
            ],
            3,
        )
        .expect("signed integer expression uses typed lane");
    assert_eq!(result.value_at(0), Value::Int(171));
    assert_eq!(result.value_at(1), Value::Null);
    assert_eq!(result.value_at(2), Value::Int(513));

    assert!(
        expression
            .try_eval_batch_integer(
                &[
                    ColumnVector::I64 {
                        data: vec![i64::MAX],
                        nulls: None,
                    },
                    ColumnVector::I64 {
                        data: vec![1],
                        nulls: None,
                    },
                ],
                1,
            )
            .is_none(),
        "signed overflow must return control to the scalar oracle"
    );
}

#[test]
fn compiled_null_safe_eq_matches_generic_null_semantics() {
    let schema = decimal_fact_schema();
    let expr = Expr::Binary {
        left: Box::new(Expr::Identifier("discount".to_string())),
        op: BinaryOp::NullSafeEq,
        right: Box::new(Expr::Literal(Literal::Null)),
    };
    let compiled = CompiledPredicate::compile(&expr, &schema).expect("compiled predicate");
    let non_null = vec![
        Value::DecimalCompact(2300, 2),
        Value::DecimalCompact(100000, 2),
        Value::DecimalCompact(6, 2),
        Value::Date(parse_yyyy_mm_dd_days("1994-06-15").unwrap()),
    ];
    let null = vec![
        Value::DecimalCompact(2300, 2),
        Value::DecimalCompact(100000, 2),
        Value::Null,
        Value::Date(parse_yyyy_mm_dd_days("1994-06-15").unwrap()),
    ];
    assert_eq!(
        compiled.eval(&non_null).unwrap(),
        crate::engine::expr::eval_bool(&expr, &schema, &non_null).unwrap()
    );
    assert_eq!(
        compiled.eval(&null).unwrap(),
        crate::engine::expr::eval_bool(&expr, &schema, &null).unwrap()
    );
}

#[test]
fn predicate_first_selector_requires_low_selectivity_and_payload_savings() {
    assert!(should_use_predicate_first_projected_scan(
        120_000.0,
        3_000_000.0,
        &[2, 4, 5],
        &[3, 4],
    ));
    assert!(!should_use_predicate_first_projected_scan(
        2_700_000.0,
        3_000_000.0,
        &[2],
        &[0, 1, 2, 3, 4],
    ));
    assert!(!should_use_predicate_first_projected_scan(
        120_000.0,
        0.0,
        &[2, 4, 5],
        &[3, 4],
    ));
    assert!(!should_use_predicate_first_projected_scan(
        120_000.0,
        3_000_000.0,
        &[2, 4, 5],
        &[2, 4],
    ));
    assert!(!should_use_predicate_first_projected_scan(
        f64::NAN,
        3_000_000.0,
        &[2, 4, 5],
        &[3, 4],
    ));
    assert!(!should_use_predicate_first_projected_scan(
        120_000.0,
        f64::INFINITY,
        &[2, 4, 5],
        &[3, 4],
    ));
}

#[test]
fn aliased_inlj_residual_reads_columns_outside_output_projection() {
    let schema = decimal_fact_schema();
    let table = PlanTable::new(1, Arc::new(schema), Some("f".to_string()));
    let residual = Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(Expr::CompoundIdentifier(vec![
                "f".to_string(),
                "quantity".to_string(),
            ])),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int(0))),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::Binary {
            left: Box::new(Expr::CompoundIdentifier(vec![
                "f".to_string(),
                "discount".to_string(),
            ])),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int(0))),
        }),
    };

    // The parent only consumes extended_price + ship_date.  The INLJ
    // must still perform a separate narrow read of quantity + discount
    // before evaluating the right-only residual.
    assert_eq!(
        right_only_filter_columns(Some(&residual), &table, &[1, 3]),
        Some(vec![0, 2])
    );
}

#[test]
fn global_mrr_requires_both_real_outer_work_and_cache_pressure() {
    assert!(should_use_global_mrr(10_000, 50_000, 3_840));
    assert!(!should_use_global_mrr(10_000, 3_840, 3_840));
    assert!(!should_use_global_mrr(511, 50_000, 3_840));
    assert!(!should_use_global_mrr(10_000, 0, 3_840));
}

#[test]
fn inlj_key_grouping_sorts_keys_and_preserves_duplicate_outer_rows() {
    let keyed_left = vec![
        (IndexKey::from(vec![KeyPart::Int(30)]), 0),
        (IndexKey::from(vec![KeyPart::Int(10)]), 1),
        (IndexKey::from(vec![KeyPart::Int(30)]), 2),
        (IndexKey::from(vec![KeyPart::Int(20)]), 3),
        (IndexKey::from(vec![KeyPart::Int(10)]), 4),
    ];

    let (keys, left_for_key) = group_sorted_inlj_keys(keyed_left);
    assert_eq!(
        keys,
        vec![
            IndexKey::from(vec![KeyPart::Int(10)]),
            IndexKey::from(vec![KeyPart::Int(20)]),
            IndexKey::from(vec![KeyPart::Int(30)]),
        ]
    );
    assert_eq!(left_for_key[0].as_slice(), &[1, 4]);
    assert_eq!(left_for_key[1].as_slice(), &[3]);
    assert_eq!(left_for_key[2].as_slice(), &[0, 2]);
}

#[test]
fn inlj_entry_position_mapping_handles_monotonic_and_unordered_row_ids() {
    let row_id = |slot_id| RowId {
        page_id: 7,
        slot_id,
    };
    let right_pairs = vec![(row_id(2), Vec::new()), (row_id(4), Vec::new())];

    let monotonic = vec![(0, row_id(2)), (1, row_id(3)), (2, row_id(4))];
    assert_eq!(
        map_inlj_entries_to_right_positions(&monotonic, &right_pairs).unwrap(),
        vec![0, -1, 1]
    );

    let unordered = vec![(0, row_id(4)), (1, row_id(2)), (2, row_id(3))];
    assert_eq!(
        map_inlj_entries_to_right_positions(&unordered, &right_pairs).unwrap(),
        vec![1, 0, -1]
    );

    let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(1024));
    budget.charge(1024).unwrap();
    crate::engine::executor::mem_budget::with_query_budget(Arc::clone(&budget), || {
        assert_eq!(
            map_inlj_entries_to_right_positions(&unordered, &right_pairs).unwrap(),
            vec![1, 0, -1],
            "unordered RowIds must use the binary-search oracle when the optional map has no headroom"
        );
    });
    budget.release(1024);
    assert_eq!(budget.used(), 0);
}

#[test]
fn projected_distinct_spills_saved_keys_and_preserves_first_hidden_payload() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let root = tempfile::tempdir().unwrap();
    let collation = crate::charset::resolve_collation("utf8mb4_0900_ai_ci")
        .unwrap()
        .id;
    let key_meta = intern_col_meta(ColMeta {
        invisible: false,
        table: "t".into(),
        alias: None,
        column: "k".into(),
        column_type: ColumnType::VarString,
        source: crate::types::ColumnSource {
            collation_id: Some(collation),
            ..Default::default()
        },
    });
    let marker_meta = intern_col_meta(ColMeta {
        invisible: false,
        table: "t".into(),
        alias: None,
        column: "marker".into(),
        column_type: ColumnType::LongLong,
        source: Default::default(),
    });
    let mut rows = Vec::new();
    for pass in 0..2 {
        for key in 0..300 {
            let text = format!("K{key:06}{}", "X".repeat(256));
            rows.push(PlanRow::from_fields(vec![
                PlanField {
                    meta: key_meta.clone(),
                    value: Value::String(if pass == 0 { text } else { text.to_lowercase() }),
                },
                PlanField {
                    meta: marker_meta.clone(),
                    value: Value::Int(pass),
                },
            ]));
        }
    }
    let budget = Arc::new(MemBudget::new(64 * 1024));
    with_query_budget(budget.clone(), || {
        let projection = vec![SelectItem::Expr {
            expr: Expr::Identifier("k".into()),
            alias: None,
        }];
        dedup_projected_rows(&mut rows, &projection, root.path()).unwrap();
        assert_eq!(rows.len(), 300);
        for (key, row) in rows.iter().enumerate() {
            assert_eq!(
                crate::types::value_str(&row.fields[0].value),
                Some(format!("K{key:06}{}", "X".repeat(256)).as_str())
            );
            assert_eq!(row.fields[1].value, Value::Int(0));
        }
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
    });
}
