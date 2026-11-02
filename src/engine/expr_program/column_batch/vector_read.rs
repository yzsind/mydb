//! Native distance expressions borrow batch payloads and bound query vectors.
//! A speculative decline publishes no diagnostics; the ordered scalar oracle
//! retains invalid dimensions, incompatible metrics and non-native coercions.
use super::*;
use crate::engine::vector::{
    codec,
    distance::{self, Metric},
};
use crate::vector::{VectorDataType, VectorRef, VectorValue};

#[derive(Clone, Copy)]
enum Operand<'a> {
    Column {
        index: usize,
        datatype: VectorDataType,
        dimension: Option<u32>,
    },
    Constant(Option<&'a VectorValue>),
}
impl<'a> Operand<'a> {
    fn bind(expr: &'a Expr, table: &TableSchema, schema: &BatchSchema) -> Option<Self> {
        match expr {
            Expr::Identifier(name) => {
                let index = table.find_column_index(name)?;
                let field = schema.fields().get(index)?;
                if field.column_type != ColumnType::Vector {
                    return None;
                }
                let (datatype, dimension) = field.source.vector_type?;
                Some(Self::Column {
                    index,
                    datatype,
                    dimension,
                })
            }
            Expr::BoundValue(Value::Vector(value)) => {
                codec::validate_internal(value).ok()?;
                Some(Self::Constant(Some(value)))
            }
            Expr::BoundValue(Value::Null) | Expr::Literal(Literal::Null) => {
                Some(Self::Constant(None))
            }
            _ => None,
        }
    }
    fn domain(self) -> Option<(VectorDataType, Option<u32>)> {
        match self {
            Self::Column {
                datatype,
                dimension,
                ..
            } => Some((datatype, dimension)),
            Self::Constant(Some(value)) => Some((value.datatype, Some(value.dimension))),
            Self::Constant(None) => None,
        }
    }
    fn at<'b>(&'b self, batch: &'b Batch, row: usize) -> Option<Option<VectorRef<'b>>> {
        match self {
            Self::Constant(value) => Some(value.map(|value| VectorRef {
                datatype: value.datatype,
                dimension: value.dimension,
                payload: &value.payload,
            })),
            Self::Column {
                index,
                datatype,
                dimension,
            } => {
                let column = batch.columns().get(*index)?;
                if column.is_null(row) {
                    return Some(None);
                }
                let value = match column {
                    ColumnVector::Vector { offsets, data, .. } => {
                        let bytes =
                            data.get(*offsets.get(row)? as usize..*offsets.get(row + 1)? as usize)?;
                        let value = VectorRef::decode(bytes)?;
                        codec::validate_internal(&value).ok()?;
                        value
                    }
                    ColumnVector::Generic { values } => {
                        let Value::Vector(value) = values.get(row)? else {
                            return None;
                        };
                        codec::validate_internal(value).ok()?;
                        VectorRef {
                            datatype: value.datatype,
                            dimension: value.dimension,
                            payload: &value.payload,
                        }
                    }
                    _ => return None,
                };
                if value.datatype != *datatype || dimension.is_some_and(|d| d != value.dimension) {
                    return None;
                }
                Some(Some(value))
            }
        }
    }
}

pub(super) struct VectorRead<'a> {
    left: Operand<'a>,
    right: Operand<'a>,
    metric: Metric,
    similarity: bool,
    parallel: bool,
}
impl<'a> VectorRead<'a> {
    /// Only a constant probe and a fixed compatible column may share query
    /// reductions. Row/row expressions and unsupported native pairs retain
    /// the scalar typed scorer.
    fn constant_probe(&self) -> Option<(&'a VectorValue, VectorDataType, bool)> {
        let (query, datatype, dimension, constant_left) = match (self.left, self.right) {
            (
                Operand::Constant(Some(query)),
                Operand::Column {
                    datatype,
                    dimension,
                    ..
                },
            ) => (query, datatype, dimension, true),
            (
                Operand::Column {
                    datatype,
                    dimension,
                    ..
                },
                Operand::Constant(Some(query)),
            ) => (query, datatype, dimension, false),
            _ => return None,
        };
        if !matches!(datatype, VectorDataType::Float16 | VectorDataType::BFloat16)
            || dimension != Some(query.dimension)
            || !matches!(self.metric, Metric::L2 | Metric::Dot | Metric::Cosine)
            || query.datatype == VectorDataType::Binary
        {
            return None;
        }
        Some((query, datatype, constant_left))
    }

    fn prepare_probe(
        &self,
        batch: &Batch,
    ) -> Option<(
        distance::PreparedNativeDistance,
        bool,
        crate::engine::executor::mem_budget::ScopedCharge,
    )> {
        let (query, datatype, constant_left) = self.constant_probe()?;
        if batch.visible_rows() <= 1 {
            return None;
        }
        let bytes = (query.dimension as usize)
            .checked_mul(4)?
            .checked_add(128)?;
        let mut charge = batch.memory_charge();
        let cap = charge
            .remaining()
            .min(crate::engine::executor::vec::evaluated_batch::byte_target());
        if !charge.add_within(bytes, cap) {
            return None;
        }
        let prepared = distance::PreparedNativeDistance::try_new(query, datatype, self.metric)?;
        Some((prepared, constant_left, charge))
    }

    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        let ExprNode::LegacyEscape {
            expr,
            schema: table,
            ..
        } = node
        else {
            return None;
        };
        let Expr::Function {
            name,
            args,
            distinct: false,
        } = expr.as_ref()
        else {
            return None;
        };
        if !(2..=3).contains(&args.len()) {
            return None;
        }
        // Comparing short ASCII names does not allocate a normalized name on
        // every contract/workspace/evaluation probe of unrelated expressions.
        let similarity =
            name.eq_ignore_ascii_case("DOT_PRODUCT") || name.eq_ignore_ascii_case("INNER_PRODUCT");
        let generic =
            name.eq_ignore_ascii_case("DISTANCE") || name.eq_ignore_ascii_case("VEC_DISTANCE");
        let metric = if generic {
            None
        } else if args.len() != 2 {
            return None;
        } else if name.eq_ignore_ascii_case("L2_DISTANCE") {
            Some(Metric::L2)
        } else if name.eq_ignore_ascii_case("COSINE_DISTANCE") {
            Some(Metric::Cosine)
        } else if similarity || name.eq_ignore_ascii_case("MAX_INNER_PRODUCT_DISTANCE") {
            Some(Metric::Dot)
        } else if name.eq_ignore_ascii_case("HAMMING_DISTANCE") {
            Some(Metric::Hamming)
        } else if name.eq_ignore_ascii_case("JACCARD_DISTANCE") {
            Some(Metric::Jaccard)
        } else {
            return None;
        };
        let left = Operand::bind(&args[0], table, schema)?;
        let right = Operand::bind(&args[1], table, schema)?;
        let metric = match metric {
            Some(metric) => metric,
            None if args.len() == 3 => {
                let text = match &args[2] {
                    Expr::Literal(Literal::String(text))
                    | Expr::BoundValue(Value::String(text)) => text.as_str(),
                    Expr::BoundValue(Value::SmallString(text)) => {
                        std::str::from_utf8(text.as_slice()).ok()?
                    }
                    _ => return None,
                };
                Metric::parse(text)?
            }
            None => {
                if [left, right]
                    .iter()
                    .filter_map(|operand| operand.domain())
                    .any(|(ty, _)| ty == VectorDataType::Binary)
                {
                    Metric::Hamming
                } else {
                    Metric::L2
                }
            }
        };
        let domains = (left.domain(), right.domain());
        let parallel = match domains {
            (Some((lt, Some(ld))), Some((rt, Some(rd)))) => {
                ld == rd
                    && if matches!(metric, Metric::Hamming | Metric::Jaccard) {
                        lt == VectorDataType::Binary && rt == VectorDataType::Binary
                    } else {
                        lt != VectorDataType::Binary && rt != VectorDataType::Binary
                    }
            }
            (None, _) | (_, None) => true,
            _ => false,
        };
        Some(Self {
            left,
            right,
            metric,
            similarity,
            parallel,
        })
    }
    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: Domain::Approximate,
            parallel: self.parallel,
        }
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let output = batch.num_rows().checked_mul(9)?.checked_add(128)?;
        Some((output, output))
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        // Probe decoding is optional. A rejected reservation must retain the
        // native typed kernel, without making its existing workspace larger
        // or forcing an otherwise valid batch through the scalar fallback.
        let prepared_native = self.prepare_probe(batch);
        let mut data = vec![0.0; batch.num_rows()];
        let mut nulls = Bitmap::new_all_invalid(batch.num_rows());
        for (ordinal, row) in batch.visible_indices().enumerate() {
            if ordinal & crate::engine::cancel::SCALAR_ROW_CANCEL_MASK == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            let left = self.left.at(batch, row)?;
            let right = self.right.at(batch, row)?;
            let (Some(left), Some(right)) = (left, right) else {
                continue;
            };
            let value = if let Some((prepared, constant_left, _charge)) = &prepared_native {
                let candidate = if *constant_left { &right } else { &left };
                prepared.score(candidate).ok()?
            } else {
                distance::typed_ordering_distance(&left, &right, self.metric).ok()?
            };
            if value.is_nan() {
                continue;
            }
            data[row] = if self.similarity {
                -value
            } else if self.metric == Metric::L2 {
                value.sqrt()
            } else {
                value
            };
            nulls.set_true(row);
        }
        Some(ColumnVector::F64 {
            data,
            nulls: Some(nulls),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::vec::batch::Selection;
    use crate::engine::executor::vec::schema::BatchField;
    use crate::types::ColumnSource;

    fn schema(datatype: VectorDataType, dim: Option<u32>) -> Arc<BatchSchema> {
        Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::Vector,
            source: ColumnSource {
                vector_type: Some((datatype, dim)),
                ..Default::default()
            },
        }]))
    }
    fn program(
        schema: &BatchSchema,
        name: &str,
        query: Value,
        metric: Option<&str>,
    ) -> Arc<ExprProgram> {
        let mut args = vec![Expr::Identifier("v".into()), Expr::BoundValue(query)];
        if let Some(metric) = metric {
            args.push(Expr::Literal(Literal::String(metric.into())));
        }
        let expr = Expr::Function {
            name: name.into(),
            args,
            distinct: false,
        };
        Arc::new(
            ExprCompiler::compile(
                &expr,
                &InputSchema::new(
                    schema
                        .fields()
                        .iter()
                        .map(|field| InputColumn {
                            table: field.table.clone(),
                            alias: field.alias.clone(),
                            name: field.name.clone(),
                            column_type: field.column_type,
                            unsigned: false,
                            collation_id: None,
                        })
                        .collect(),
                ),
            )
            .unwrap(),
        )
    }
    fn check(program: &ExprProgram, batch: &Batch) {
        assert!(program.has_column_contract(batch.schema()));
        let actual = program
            .try_eval_column(batch)
            .expect("native distance column");
        for row in 0..batch.num_rows() {
            if !batch.visible_indices().any(|r| r == row) {
                assert_eq!(actual.value_at(row), Value::Null);
                continue;
            }
            let values = vec![batch.columns()[0].value_at(row)];
            let expected = with_eval_scratch(|scratch| {
                program.eval_row(&RowView::from_values(&values), scratch)
            })
            .unwrap();
            assert_eq!(actual.value_at(row), expected, "row {row}");
        }
    }
    fn vector(datatype: VectorDataType, dim: u32, zero: bool) -> VectorValue {
        let text = format!(
            "[{}]",
            (0..dim)
                .map(|i| if zero {
                    "0".into()
                } else {
                    if datatype == VectorDataType::Int8 {
                        ((i as i32 % 7) - 3).to_string()
                    } else {
                        (((i as i32 % 7) - 3) as f32 * 0.37 + 0.001).to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join(",")
        );
        codec::parse_typed_text(&text, datatype).unwrap()
    }
    #[test]
    fn optional_native_probe_denial_keeps_column_kernel_and_original_workspace() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        for datatype in [VectorDataType::Float16, VectorDataType::BFloat16] {
            let schema = schema(datatype, Some(129));
            let batch = Batch::new(
                schema.clone(),
                vec![ColumnVector::from_values(vec![
                    Value::Vector(vector(datatype, 129, false)),
                    Value::Vector(vector(datatype, 129, true)),
                    Value::Null,
                ])],
            );
            let program = program(
                &schema,
                "COSINE_DISTANCE",
                Value::Vector(vector(VectorDataType::Float32, 129, false)),
                None,
            );
            let budget = Arc::new(MemBudget::new(64));
            let actual = with_query_budget(budget.clone(), || {
                program
                    .try_eval_column(&batch)
                    .expect("optional probe denial must retain native output")
            });
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.peak_used(), (batch.num_rows() * 9 + 128) * 2);
            assert!(budget.terminal_oom().is_none());
            for row in 0..batch.num_rows() {
                let values = vec![batch.columns()[0].value_at(row)];
                let expected = with_eval_scratch(|scratch| {
                    program.eval_row(&RowView::from_values(&values), scratch)
                })
                .unwrap();
                assert_eq!(actual.value_at(row), expected);
            }
        }
    }

    #[test]
    fn prepared_constant_distance_columns_preserve_bits_in_both_operand_orders() {
        for dim in [65, 129, 768] {
            for datatype in [
                VectorDataType::Float32,
                VectorDataType::Float16,
                VectorDataType::BFloat16,
                VectorDataType::Int8,
            ] {
                let schema = schema(datatype, Some(dim));
                let values = vec![
                    Value::Vector(vector(datatype, dim, false)),
                    Value::Null,
                    Value::Vector(vector(datatype, dim, true)),
                ];
                let batch = Batch::new(schema, vec![ColumnVector::from_values(values.clone())]);
                for query_type in [
                    VectorDataType::Float32,
                    VectorDataType::Float16,
                    VectorDataType::BFloat16,
                    VectorDataType::Int8,
                ] {
                    for zero in [false, true] {
                        let query = vector(query_type, dim, zero);
                        for metric in [Metric::L2, Metric::Dot, Metric::Cosine] {
                            for constant_left in [false, true] {
                                let column = Operand::Column {
                                    index: 0,
                                    datatype,
                                    dimension: Some(dim),
                                };
                                let constant = Operand::Constant(Some(&query));
                                let read = VectorRead {
                                    left: if constant_left { constant } else { column },
                                    right: if constant_left { column } else { constant },
                                    metric,
                                    similarity: false,
                                    parallel: true,
                                };
                                let actual = read.evaluate(&batch).unwrap();
                                let (output, work) = read.workspace(&batch).unwrap();
                                assert_eq!(work, output);
                                for (i, value) in values.iter().enumerate() {
                                    let Value::Vector(row) = value else {
                                        assert_eq!(actual.value_at(i), Value::Null);
                                        continue;
                                    };
                                    let distance = if constant_left {
                                        distance::typed_ordering_distance(&query, row, metric)
                                    } else {
                                        distance::typed_ordering_distance(row, &query, metric)
                                    }
                                    .unwrap();
                                    if distance.is_nan() {
                                        assert_eq!(actual.value_at(i), Value::Null);
                                    } else {
                                        let Value::Double(value) = actual.value_at(i) else {
                                            panic!("double required")
                                        };
                                        let expected = if metric == Metric::L2 {
                                            distance.sqrt()
                                        } else {
                                            distance
                                        };
                                        assert_eq!(value.to_bits(),expected.to_bits(),"{datatype:?}/{query_type:?}/{dim}/{metric:?}/{constant_left}");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn native_distance_columns_match_scalar_for_all_numeric_types_and_metrics() {
        let types = [
            VectorDataType::Float32,
            VectorDataType::Float16,
            VectorDataType::BFloat16,
            VectorDataType::Int8,
        ];
        for dim in [1, 3, 17, 65] {
            for left in types {
                let schema = schema(left, Some(dim));
                let batch = Batch::new(
                    schema.clone(),
                    vec![ColumnVector::from_values(vec![
                        Value::Vector(vector(left, dim, false)),
                        Value::Vector(vector(left, dim, true)),
                        Value::Null,
                    ])],
                );
                for right in types {
                    for (name, metric) in [
                        ("L2_DISTANCE", None),
                        ("COSINE_DISTANCE", None),
                        ("DOT_PRODUCT", None),
                        ("INNER_PRODUCT", None),
                        ("MAX_INNER_PRODUCT_DISTANCE", None),
                        ("DISTANCE", None),
                        ("VEC_DISTANCE", Some("COSINE")),
                        ("DISTANCE", Some("DOT")),
                    ] {
                        check(
                            &program(
                                &schema,
                                name,
                                Value::Vector(vector(right, dim, false)),
                                metric,
                            ),
                            &batch,
                        );
                    }
                }
            }
        }
    }
    #[test]
    fn native_binary_distance_columns_match_scalar_with_tail_bits_and_nulls() {
        for dim in [1, 7, 9, 65] {
            let schema = schema(VectorDataType::Binary, Some(dim));
            let vector = |value| {
                codec::parse_typed_text(
                    &format!("[{}]", vec![value; dim as usize].join(",")),
                    VectorDataType::Binary,
                )
                .unwrap()
            };
            let batch = Batch::new(
                schema.clone(),
                vec![ColumnVector::from_values(vec![
                    Value::Vector(vector("1")),
                    Value::Vector(vector("0")),
                    Value::Null,
                ])],
            );
            for query in [
                Value::Vector(vector("1")),
                Value::Vector(vector("0")),
                Value::Null,
            ] {
                for (name, metric) in [
                    ("HAMMING_DISTANCE", None),
                    ("JACCARD_DISTANCE", None),
                    ("DISTANCE", None),
                    ("VEC_DISTANCE", Some("JACCARD")),
                ] {
                    check(&program(&schema, name, query.clone(), metric), &batch);
                }
            }
        }
    }
    #[test]
    fn native_distance_selection_skips_invalid_hidden_rows_and_preserves_errors() {
        let datatype = VectorDataType::Float32;
        let schema = schema(datatype, None);
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Vector(vector(datatype, 3, false)),
                Value::Vector(vector(datatype, 5, false)),
            ])],
        );
        let program = program(
            &schema,
            "COSINE_DISTANCE",
            Value::Vector(vector(datatype, 3, false)),
            None,
        );
        assert!(!program.column_parallel_safe(&schema));
        let selected = batch.clone().with_selection(Selection::Indices(vec![0]));
        check(&program, &selected);
        assert!(program.try_eval_column(&batch).is_none());
        let error = with_eval_scratch(|scratch| {
            program.eval_row(
                &RowView::from_values(&[batch.columns()[0].value_at(1)]),
                scratch,
            )
        })
        .unwrap_err();
        assert!(error.message().contains("dimension mismatch"));
    }
    #[test]
    fn native_distance_column_pairs_and_budget_accounting_match_scalar() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        let left = VectorDataType::Float16;
        let right = VectorDataType::Float32;
        let mut fields = schema(left, Some(17)).fields().to_vec();
        let mut second = fields[0].clone();
        second.name = "w".into();
        second.source.vector_type = Some((right, Some(17)));
        fields.push(second);
        let schema = Arc::new(BatchSchema::new(fields));
        let input = InputSchema::new(
            schema
                .fields()
                .iter()
                .map(|field| InputColumn {
                    table: field.table.clone(),
                    alias: None,
                    name: field.name.clone(),
                    column_type: field.column_type,
                    unsigned: false,
                    collation_id: None,
                })
                .collect(),
        );
        let expression = Expr::Function {
            name: "COSINE_DISTANCE".into(),
            args: vec![Expr::Identifier("v".into()), Expr::Identifier("w".into())],
            distinct: false,
        };
        let program = ExprCompiler::compile(&expression, &input).unwrap();
        let batch = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::from_values(vec![
                    Value::Vector(vector(left, 17, false)),
                    Value::Null,
                    Value::Vector(vector(left, 17, true)),
                ]),
                ColumnVector::from_values(vec![Value::Vector(vector(right, 17, false)); 3]),
            ],
        );
        assert!(program.column_parallel_safe(&schema));
        let actual = program.try_eval_column(&batch).unwrap();
        for row in 0..3 {
            let values = batch
                .columns()
                .iter()
                .map(|c| c.value_at(row))
                .collect::<Vec<_>>();
            let expected = with_eval_scratch(|scratch| {
                program.eval_row(&RowView::from_values(&values), scratch)
            })
            .unwrap();
            assert_eq!(actual.value_at(row), expected);
        }
        let budget = Arc::new(MemBudget::new(64));
        with_query_budget(budget.clone(), || {
            assert!(program.try_eval_column(&batch).is_some())
        });
        assert_eq!(budget.used(), 0);
        assert!(budget.peak_used() >= batch.num_rows() * 9);
    }

    #[test]
    fn native_distance_incompatible_domains_retain_scalar_errors() {
        let schema = schema(VectorDataType::Binary, Some(9));
        let value = codec::parse_typed_text("[1,0,1,0,1,0,1,0,1]", VectorDataType::Binary).unwrap();
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(vec![Value::Vector(value)])],
        );
        for (name, query) in [
            (
                "COSINE_DISTANCE",
                Value::Vector(vector(VectorDataType::Float32, 9, false)),
            ),
            (
                "HAMMING_DISTANCE",
                Value::Vector(codec::parse_typed_text("[1,0]", VectorDataType::Binary).unwrap()),
            ),
        ] {
            let program = program(&schema, name, query, None);
            assert!(!program.column_parallel_safe(&schema));
            assert!(program.try_eval_column(&batch).is_none());
            assert!(with_eval_scratch(|scratch| program.eval_row(
                &RowView::from_values(&[batch.columns()[0].value_at(0)]),
                scratch
            ))
            .is_err());
        }
    }

    #[test]
    fn native_distance_extreme_f32_values_retain_wide_fallback_and_null_cosine() {
        let datatype = VectorDataType::Float32;
        let schema = schema(datatype, Some(3));
        let values = ["[1e38,-1e38,1e-40]", "[1e-40,0,-1e-40]", "[0,0,0]"];
        let vectors = values.map(|text| codec::parse_typed_text(text, datatype).unwrap());
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(
                vectors.iter().cloned().map(Value::Vector).collect(),
            )],
        );
        for query in vectors {
            for name in ["L2_DISTANCE", "COSINE_DISTANCE", "DOT_PRODUCT"] {
                check(
                    &program(&schema, name, Value::Vector(query.clone()), None),
                    &batch,
                );
            }
        }
    }
}
