//! Identity domains, semantic options and score explanations.
use super::*;

pub(crate) fn describe_columns(db: &Database, rrf: &RrfFusion) -> Result<Vec<Column>, QueryError> {
    if !(2..=8).contains(&rrf.branches.len()) {
        return Err(invalid("requires 2..8 branches"));
    }
    let first = rrf
        .branches
        .first()
        .ok_or_else(|| invalid("at least two branches are required"))?;
    let mut columns = super::super::describe_subquery_columns(db, &first.query)?;
    if columns.is_empty() {
        return Err(invalid("identity must contain at least one column"));
    }
    let mut names = std::collections::HashSet::new();
    for column in &columns {
        if !names.insert(column.name.to_ascii_lowercase())
            || ["rrf_score", "best_rank", "match_count", "rrf_details"]
                .iter()
                .any(|name| column.name.eq_ignore_ascii_case(name))
        {
            return Err(invalid(
                "identity column names must be unique and must not use fusion output names",
            ));
        }
        if matches!(
            identity_type(column).kind,
            crate::engine::expr_type::ExprTypeKind::String {
                kind: crate::engine::expr_type::StringKind::Enum
                    | crate::engine::expr_type::StringKind::Set
            }
        ) || column.source.dynamic_parameter
            || matches!(
                column.column_type,
                ColumnType::Json | ColumnType::Vector | ColumnType::Geometry
            )
        {
            return Err(crate::error::catalog::ER_NOT_SUPPORTED_YET.error("RRF identity requires a comparable scalar type; cast untyped parameters explicitly"));
        }
    }
    for branch in &rrf.branches[1..] {
        let other = super::super::describe_subquery_columns(db, &branch.query)?;
        if other.len() != columns.len() {
            return Err(invalid(
                "branches must project the same number of identity columns",
            ));
        }
        for (left, right) in columns.iter().zip(&other) {
            if identity_type(left) != identity_type(right) {
                return Err(invalid("identity column types, signedness, precision and collations must match; use explicit CAST"));
            }
        }
    }
    for column in &mut columns {
        if matches!(
            identity_type(column).kind,
            crate::engine::expr_type::ExprTypeKind::String { .. }
        ) {
            column.source.collation_id = identity_type(column).collation_id;
        }
    }
    for (name, column_type) in [
        ("rrf_score", ColumnType::Double),
        ("best_rank", ColumnType::LongLong),
        ("match_count", ColumnType::LongLong),
    ] {
        let mut source = crate::types::ColumnSource::default();
        source.flags = crate::mysql::protocol::NOT_NULL_FLAG;
        if column_type == ColumnType::LongLong {
            source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
        }
        columns.push(Column {
            name: name.into(),
            column_type,
            source,
        });
    }
    if rrf.score_details {
        columns.push(Column {
            name: "rrf_details".into(),
            column_type: ColumnType::Json,
            source: crate::types::ColumnSource {
                flags: crate::mysql::protocol::NOT_NULL_FLAG,
                ..Default::default()
            },
        });
    }
    Ok(columns)
}

pub(super) fn invalid(message: &str) -> QueryError {
    crate::error::catalog::ER_WRONG_ARGUMENTS.error(format!("RRF: {message}"))
}

fn number(expr: &Expr) -> Result<f64, QueryError> {
    let value = super::super::eval::eval_plan_expr(expr, &super::super::plan::PlanRow::default())?;
    let n = match value {
        Value::Int(v) => v as f64,
        Value::UInt(v) => v as f64,
        Value::Float(v) => v as f64,
        Value::Double(v) => v,
        Value::Decimal(_)
        | Value::DecimalCompact(..)
        | Value::Decimal128(..)
        | Value::DecimalWide(_) => value
            .to_display_string()
            .parse::<f64>()
            .map_err(|_| invalid("invalid decimal option"))?,
        _ => return Err(invalid("options require numeric non-NULL values")),
    };
    if !n.is_finite() || n < 0.0 {
        return Err(invalid("options must be finite and nonnegative"));
    }
    Ok(n)
}

pub(crate) fn bind_options(rrf: &RrfFusion) -> Result<(f64, Vec<f64>), QueryError> {
    let constant_value = super::super::eval::eval_plan_expr(
        &rrf.rank_constant,
        &super::super::plan::PlanRow::default(),
    )?;
    if constant_value.exact_decimal().is_some()
        && crate::util::decimal::canonical_decimal_str(&constant_value.to_display_string())
            .is_none_or(|value| value.contains('.'))
    {
        return Err(invalid("RANK_CONSTANT must be an exact integer"));
    }
    let constant = number(&rrf.rank_constant)?;
    if constant.fract() != 0.0 || constant > u32::MAX as f64 {
        return Err(invalid("RANK_CONSTANT must be an integer in 0..4294967295"));
    }
    let weights = rrf
        .weights
        .iter()
        .map(number)
        .collect::<Result<Vec<_>, _>>()?;
    if weights.len() != rrf.branches.len() || !weights.iter().any(|w| *w > 0.0) {
        return Err(invalid(
            "WEIGHTS must match branches and contain a positive value",
        ));
    }
    let bound = weights.iter().map(|w| w / (constant + 1.0)).sum::<f64>();
    if !bound.is_finite() {
        return Err(invalid("weights overflow the finite score range"));
    }
    Ok((constant, weights))
}

pub(super) fn ranker(db: &Database, query: &QueryExpr) -> serde_json::Value {
    let Some(select) = query.leftmost_select() else {
        return serde_json::json!({"kind":"sql_order"});
    };
    if let Some(Expr::Function { name, args, .. }) =
        select.order_by.first().map(|order| &order.expr)
    {
        if name.eq_ignore_ascii_case(crate::parser::FT_MATCH_FUNCTION) {
            return serde_json::json!({"kind":"bm25","higher_is_better":true});
        }
        use crate::engine::vector::Metric;
        let metric = match name.to_ascii_uppercase().as_str() {
            "L2_DISTANCE" => Some(Metric::L2),
            "COSINE_DISTANCE" => Some(Metric::Cosine),
            "DOT_DISTANCE" | "MAX_INNER_PRODUCT_DISTANCE" => Some(Metric::Dot),
            "HAMMING_DISTANCE" => Some(Metric::Hamming),
            "JACCARD_DISTANCE" => Some(Metric::Jaccard),
            "DISTANCE" | "VEC_DISTANCE" if args.len() == 3 => super::super::eval::eval_plan_expr(
                &args[2],
                &super::super::plan::PlanRow::default(),
            )
            .ok()
            .and_then(|value| match &value {
                Value::Bytes(bytes) => std::str::from_utf8(bytes).ok().and_then(Metric::parse),
                _ => crate::types::value_str(&value).and_then(Metric::parse),
            }),
            "DISTANCE" | "VEC_DISTANCE" if args.len() == 2 => {
                // Describe operands in their original CTE/derived scope. Default
                // distance is HAMMING when either operand is a BINARY vector.
                fn project_operands(query: &mut QueryExpr, args: &[Expr]) {
                    match query {
                        QueryExpr::WithCte(with) => project_operands(&mut with.body, args),
                        QueryExpr::Select(select) => {
                            select.projection = args
                                .iter()
                                .enumerate()
                                .map(|(i, expr)| SelectItem::Expr {
                                    expr: expr.clone(),
                                    alias: Some(format!("operand_{i}")),
                                })
                                .collect();
                        }
                        _ => {}
                    }
                }
                let mut operands = query.clone();
                project_operands(&mut operands, args);
                super::super::describe_subquery_columns(db, &operands)
                    .ok()
                    .map(|columns| {
                        if columns.iter().any(|column| {
                            column.source.vector_type.is_some_and(|(datatype, _)| {
                                datatype == crate::vector::VectorDataType::Binary
                            })
                        }) {
                            Metric::Hamming
                        } else {
                            Metric::L2
                        }
                    })
            }
            _ => None,
        };
        if let Some(metric) = metric {
            return serde_json::json!({"kind":"vector_distance","metric":metric.as_str(),"higher_is_better":false});
        }
    }
    serde_json::json!({"kind":"sql_order"})
}

pub(super) fn ordered_json(value: &Value) -> serde_json::Value {
    let kind = match value {
        Value::Null => "NULL",
        Value::Int(_) => "INT64",
        Value::UInt(_) => "UINT64",
        Value::Float(_) => "FLOAT32",
        Value::Double(_) => "FLOAT64",
        Value::Decimal(_)
        | Value::DecimalCompact(..)
        | Value::Decimal128(..)
        | Value::DecimalWide(_) => "DECIMAL",
        Value::String(_) | Value::SmallString(_) => "STRING",
        Value::Bytes(_) => "BINARY",
        Value::Date(_) => "DATE",
        Value::Time(_) => "TIME",
        Value::DateTime(_) => "DATETIME",
        Value::Timestamp(_) => "TIMESTAMP",
        Value::Year(_) => "YEAR",
        Value::Json(_) => "JSON",
        Value::Geometry(_) => "GEOMETRY",
        Value::Vector(_) => "VECTOR",
    };
    let encoded = match value {
        Value::Decimal(_)
        | Value::DecimalCompact(..)
        | Value::Decimal128(..)
        | Value::DecimalWide(_) => serde_json::Value::String(value.to_display_string()),
        Value::Bytes(bytes) => serde_json::Value::String(
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        ),
        _ => crate::engine::expr::value_to_json(value),
    };
    serde_json::json!({"type":kind,"value":encoded})
}

pub(in crate::engine::executor) fn statement_has_fusion(
    statement: &crate::parser::Statement,
) -> bool {
    struct Find(bool);
    impl crate::parser::visit::AstVisitor for Find {
        fn enter_query(&mut self, query: &QueryExpr) {
            self.0 |= matches!(query, QueryExpr::Rrf(_));
        }
        fn walk_query_children(&self, _: &QueryExpr) -> bool {
            !self.0
        }
        fn continue_expr_walk(&self) -> bool {
            !self.0
        }
    }
    let mut find = Find(false);
    crate::parser::visit::visit_statement(&mut find, statement);
    find.0
}

/// Compare logical SQL domains rather than catalog presentation metadata.
/// Nullability and coercibility do not change an already typed identity;
/// numeric display lengths and absent/default temporal precision are likewise
/// not distinct key domains.
fn identity_type(column: &Column) -> crate::engine::expr_type::ExprType {
    use crate::engine::expr_type::{ExprType, ExprTypeKind, StringKind};
    let mut ty = ExprType::from_column_type(column.column_type, &column.source);
    ty.nullable = false;
    ty.coercibility = 0;
    if let ExprTypeKind::String { kind } = ty.kind {
        ty.collation_id = Some(ty.collation_id.unwrap_or_else(|| {
            if matches!(kind, StringKind::Blob) {
                crate::charset::BINARY_COLLATION_ID
            } else {
                crate::engine::expr::connection_collation_id()
            }
        }));
    } else {
        ty.max_octets = None;
        ty.collation_id = None;
    }
    ty
}

pub(super) fn coerce_identity(value: Value, column: &Column) -> Result<Value, QueryError> {
    let compatible = match (&value, column.column_type) {
        (Value::Null, _) => true,
        (
            Value::Int(_) | Value::UInt(_),
            ColumnType::Tiny
            | ColumnType::Short
            | ColumnType::MediumInt
            | ColumnType::Long
            | ColumnType::LongLong
            | ColumnType::Bit { .. },
        ) => true,
        (Value::Float(_), ColumnType::Float) | (Value::Double(_), ColumnType::Double) => true,
        (Value::String(_) | Value::SmallString(_), ColumnType::VarString | ColumnType::Blob) => {
            column.source.collation_id != Some(crate::charset::BINARY_COLLATION_ID)
        }
        (Value::Bytes(_), ColumnType::VarString | ColumnType::Blob) => {
            column.source.collation_id == Some(crate::charset::BINARY_COLLATION_ID)
        }
        (
            Value::DecimalCompact(_, actual) | Value::Decimal128(_, actual),
            ColumnType::Decimal { scale, .. },
        ) => *actual == scale,
        (Value::DecimalWide(actual), ColumnType::Decimal { scale, .. }) => actual.scale == scale,
        (Value::Date(_), ColumnType::Date)
        | (Value::Time(_), ColumnType::Time)
        | (Value::DateTime(_), ColumnType::DateTime)
        | (Value::Timestamp(_), ColumnType::Timestamp)
        | (Value::Year(_), ColumnType::Year) => true,
        _ => false,
    };
    if compatible {
        return Ok(value);
    }
    let value = crate::engine::executor::dispatch::coerce_setop_value(
        value,
        column.column_type,
        &column.source,
    )?;
    crate::engine::expr::coerce_result_to_expr_type(value, identity_type(column))
}

/// Semantic plan metadata, independent of internal spool identities.
pub(crate) fn plan_metadata(spec: &RrfFusion, columns: &[Column]) -> serde_json::Value {
    let width = columns.len() - 3 - usize::from(spec.score_details);
    serde_json::json!({
        "score_details": spec.score_details,
        "identity": columns[..width].iter().map(|column| serde_json::json!({
            "name": column.name, "type": format!("{:?}", identity_type(column)),
        })).collect::<Vec<_>>(),
        "branches": spec.branches.iter().enumerate().map(|(i,branch)| {
            let select = branch.query.leftmost_select().expect("validated fusion branch");
            serde_json::json!({"branch_id":i+1, "name":branch.name,
                "order_by": select.order_by.iter().map(|order|serde_json::json!({
                    "expression":crate::parser::expr_to_sql(&order.expr),"ascending":order.asc,
                })).collect::<Vec<_>>(), "limit":format!("{:?}",select.limit),
            })
        }).collect::<Vec<_>>(),
    })
}
