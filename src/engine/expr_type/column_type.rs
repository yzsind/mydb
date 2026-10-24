//! ColumnType projection for schema-bound expression consumers. Bind-time
//! branch/type rules live together here; runtime evaluators supply lookups.
use crate::engine::expr::{
    function_metadata_decimals, json_value_type, normalized_decimal_big_parts,
    resolved_stored_function_type, session_div_precision_increment, session_user_variable,
};
use crate::parser::{BinaryOp, Expr, Literal, UnaryOp};
use crate::types::{ColumnType, Value};

pub(crate) fn unify_branch_type_iter<'a>(
    branches: impl Iterator<Item = &'a Expr>,
    type_of: &mut impl FnMut(&Expr) -> ColumnType,
) -> ColumnType {
    let mut acc: Option<ColumnType> = None;
    for b in branches {
        if matches!(b, Expr::Literal(Literal::Null)) {
            continue;
        }
        let t = type_of(b);
        match acc {
            None => acc = Some(t),
            Some(prev) if prev == t => {}
            Some(prev) => {
                let source = crate::types::ColumnSource::default();
                let left = crate::engine::expr_type::ExprType::from_column_type(prev, &source);
                let right = crate::engine::expr_type::ExprType::from_column_type(t, &source);
                acc = Some(
                    crate::engine::expr_type::aggregate_branch_types(left, right).column_type(),
                );
            }
        }
    }
    acc.unwrap_or(ColumnType::VarString)
}

/// ColumnType compatibility adapter. Rich bind-time descriptors remain in
/// expr_type; callers provide only schema lookup, never their own recursion.
pub(crate) fn schema_expr_result_type(
    expr: &Expr,
    column_type: &mut impl FnMut(&Expr) -> ColumnType,
) -> ColumnType {
    infer_column_type::<true>(expr, column_type)
}

/// Resolve CASE branches by borrowing the AST, without cloning subtrees.
pub(crate) fn case_result_type(
    results: &[Expr],
    else_result: Option<&Expr>,
    mut type_of: impl FnMut(&Expr) -> ColumnType,
) -> ColumnType {
    unify_branch_type_iter(results.iter().chain(else_result), &mut type_of)
}

#[inline]
fn is_integer_column_type(column_type: ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Tiny
            | ColumnType::Short
            | ColumnType::MediumInt
            | ColumnType::Long
            | ColumnType::LongLong
            | ColumnType::Year
            | ColumnType::Bit { .. }
    )
}

/// Shared arithmetic result classifier used by context-free and schema-aware
/// result metadata builders. In particular, integer +/-/*/% integer remains
/// an integer; classifying it as DOUBLE makes CTAS round values above 2^53.
pub(crate) fn binary_expression_result_type(
    op: BinaryOp,
    left: ColumnType,
    right: ColumnType,
) -> ColumnType {
    let decimal_spec = |column_type: ColumnType| match column_type {
        ColumnType::Decimal { precision, scale } => Some(
            crate::util::decimal::DecimalResultSpec::new(precision, scale),
        ),
        integer if is_integer_column_type(integer) => {
            Some(crate::util::decimal::DecimalResultSpec::new(20, 0))
        }
        _ => None,
    };
    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Mod
            if is_integer_column_type(left) && is_integer_column_type(right) =>
        {
            ColumnType::LongLong
        }
        BinaryOp::Div if decimal_spec(left).is_some() && decimal_spec(right).is_some() => {
            let spec = crate::util::decimal::DecimalResultSpec::divide(
                decimal_spec(left).unwrap(),
                decimal_spec(right).unwrap(),
                session_div_precision_increment(),
            );
            ColumnType::Decimal {
                precision: spec.precision,
                scale: spec.scale,
            }
        }
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Mod
            if decimal_spec(left).is_some() && decimal_spec(right).is_some() =>
        {
            let left = decimal_spec(left).unwrap();
            let right = decimal_spec(right).unwrap();
            let spec = match op {
                BinaryOp::Add | BinaryOp::Sub => {
                    crate::util::decimal::DecimalResultSpec::add_sub(left, right)
                }
                BinaryOp::Mul => crate::util::decimal::DecimalResultSpec::multiply(left, right),
                BinaryOp::Mod => crate::util::decimal::DecimalResultSpec::modulo(left, right),
                _ => unreachable!(),
            };
            ColumnType::Decimal {
                precision: spec.precision,
                scale: spec.scale,
            }
        }
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            ColumnType::Double
        }
        BinaryOp::Arrow => ColumnType::Json,
        BinaryOp::LongArrow => ColumnType::VarString,
        _ => ColumnType::LongLong,
    }
}

pub(crate) fn cast_target_name(args: &[Expr]) -> Option<&str> {
    match args.get(1) {
        Some(Expr::Literal(Literal::String(target))) => Some(target.as_str()),
        _ => None,
    }
}

fn cast_result_type(args: &[Expr]) -> ColumnType {
    let Some(target) = cast_target_name(args) else {
        return ColumnType::VarString;
    };
    match target.to_ascii_uppercase().as_str() {
        "SIGNED" | "UNSIGNED" => ColumnType::LongLong,
        "DECIMAL" => {
            let int_arg = |idx: usize| match args.get(idx) {
                Some(Expr::Literal(Literal::Int(value))) => u8::try_from(*value).ok(),
                _ => None,
            };
            ColumnType::Decimal {
                precision: int_arg(2).unwrap_or(10),
                scale: int_arg(3).unwrap_or(0),
            }
        }
        "DOUBLE" => ColumnType::Double,
        "FLOAT" => ColumnType::Float,
        "DATE" => ColumnType::Date,
        "YEAR" => ColumnType::Year,
        "DATETIME" => ColumnType::DateTime,
        "TIME" => ColumnType::Time,
        "JSON" => ColumnType::Json,
        "BINARY" => ColumnType::Blob,
        _ => ColumnType::VarString,
    }
}

fn decimal_literal_result_type(text: &str) -> ColumnType {
    use num_traits::Signed;
    let Some((magnitude, scale)) = normalized_decimal_big_parts(text).or_else(|| {
        // Oversized exact literals are clamped to DECIMAL(65), not converted
        // through binary floating point.
        crate::util::decimal::parse_exact_decimal_big(text).map(|(magnitude, scale)| {
            let max = num_bigint::BigInt::from(10u8)
                .pow(crate::util::decimal::MAX_DECIMAL_PRECISION as u32)
                - num_bigint::BigInt::from(1u8);
            let clamped = if magnitude.is_negative() { -max } else { max };
            (clamped, scale.min(crate::util::decimal::MAX_DECIMAL_SCALE))
        })
    }) else {
        return ColumnType::Double;
    };
    let significant_digits = magnitude.abs().to_str_radix(10).len();
    // DECIMAL always reserves at least one integer digit. In particular,
    // literal 0.0 is DECIMAL(2,1), not the impossible DECIMAL(1,1) shape.
    let precision = significant_digits
        .max(scale as usize + 1)
        .clamp(1, crate::types::MAX_DECIMAL_PRECISION as usize) as u8;
    ColumnType::Decimal { precision, scale }
}

fn binary_decimal_value_result_type(value: &Value) -> Option<ColumnType> {
    use num_traits::Signed;
    let exact = value.exact_decimal()?;
    let source_scale = exact.scale();
    let (magnitude, _) = exact.into_big_parts();
    if source_scale > crate::util::decimal::MAX_DECIMAL_SCALE {
        // A folded expression can retain MySQL's 81-digit scratch scale, but
        // a materialized/result DECIMAL is always shaped to DECIMAL(65,30).
        return Some(ColumnType::Decimal {
            precision: crate::util::decimal::MAX_DECIMAL_PRECISION,
            scale: crate::util::decimal::MAX_DECIMAL_SCALE,
        });
    }
    let digits = magnitude.abs().to_str_radix(10).len();
    let precision = digits
        .max(source_scale as usize)
        .clamp(1, crate::util::decimal::MAX_DECIMAL_PRECISION as usize) as u8;
    Some(ColumnType::Decimal {
        precision,
        scale: source_scale.min(precision),
    })
}

pub(crate) fn expr_result_type(expr: &Expr) -> ColumnType {
    infer_column_type::<false>(expr, &mut |_| ColumnType::VarString)
}

fn infer_column_type<const SCHEMA: bool>(
    expr: &Expr,
    column_type: &mut impl FnMut(&Expr) -> ColumnType,
) -> ColumnType {
    crate::util::stack::ensure_stack(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        || infer_column_type_checked::<SCHEMA>(expr, column_type),
    )
}

fn infer_column_type_checked<const SCHEMA: bool>(
    expr: &Expr,
    column_type: &mut impl FnMut(&Expr) -> ColumnType,
) -> ColumnType {
    let recurse = &mut |child: &Expr| infer_column_type::<SCHEMA>(child, column_type);
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) if SCHEMA => column_type(expr),
        Expr::Literal(Literal::Int(_) | Literal::UInt(_) | Literal::Bool(_)) => {
            ColumnType::LongLong
        }
        Expr::Literal(Literal::Float(_)) => ColumnType::Double,
        Expr::Literal(Literal::Decimal(s)) => decimal_literal_result_type(s),
        Expr::Literal(Literal::String(_)) | Expr::Literal(Literal::Null) => ColumnType::VarString,
        Expr::Default(_) | Expr::DefaultQualified(_) | Expr::Tuple(_) => ColumnType::VarString,
        Expr::GroupKeyRef { expr, .. } => recurse(expr),
        Expr::Grouping(_) => ColumnType::LongLong,
        Expr::BoundValue(value) => match value {
            Value::Int(_) | Value::UInt(_) => ColumnType::LongLong,
            Value::Float(_) => ColumnType::Float,
            Value::Double(_) => ColumnType::Double,
            Value::Decimal(s) => decimal_literal_result_type(s),
            Value::DecimalCompact(..) | Value::Decimal128(..) | Value::DecimalWide(..) => {
                binary_decimal_value_result_type(value).unwrap_or(ColumnType::VarString)
            }
            Value::Bytes(_) => ColumnType::Blob,
            Value::Geometry(_) => ColumnType::Geometry,
            // A folded vector constant (e.g. `STRING_TO_VECTOR('[..]')` over
            // literal args) keeps the VECTOR wire type.
            Value::Vector(_) => ColumnType::Vector,
            Value::Date(_) => ColumnType::Date,
            Value::Time(_) => ColumnType::Time,
            Value::DateTime(_) => ColumnType::DateTime,
            Value::Timestamp(_) => ColumnType::Timestamp,
            Value::Year(_) => ColumnType::Year,
            Value::Json(_) => ColumnType::Json,
            _ => ColumnType::VarString,
        },
        // NOT yields a boolean (0/1/NULL) → BIGINT, like the comparison
        // operators below; other unary ops (`-`, `~`) keep the operand's
        // numeric type.
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => {
            if SCHEMA {
                recurse(expr)
            } else {
                ColumnType::LongLong
            }
        }
        Expr::Unary { expr, .. } => recurse(expr),
        Expr::Binary { left, op, right } => {
            // A hex/bit literal becomes an unsigned integer in arithmetic
            // context. MySQL retains the smallest native integer family for
            // literals up to four bytes (`0x9999 + 0` -> INT UNSIGNED) and
            // widens five-to-eight-byte values to BIGINT UNSIGNED.
            let hexbit_numeric_type = |operand: &Expr| match operand {
                Expr::Literal(Literal::HexBytes(bytes) | Literal::BitBytes(bytes))
                    if bytes.len() <= 4 =>
                {
                    Some(ColumnType::Long)
                }
                Expr::Literal(Literal::HexBytes(bytes) | Literal::BitBytes(bytes))
                    if bytes.len() <= 8 =>
                {
                    Some(ColumnType::LongLong)
                }
                _ => None,
            };
            if !SCHEMA
                && matches!(
                    op,
                    BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Mod
                )
            {
                if let Some(integer_type) =
                    hexbit_numeric_type(left).or_else(|| hexbit_numeric_type(right))
                {
                    return integer_type;
                }
            }
            binary_expression_result_type(*op, recurse(left), recurse(right))
        }
        Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::QuantifiedComparison { .. }
        | Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. } => ColumnType::LongLong,
        Expr::UserVariable(name) => session_user_variable(name)
            .map(|value| expr_result_type(&Expr::BoundValue(value)))
            .unwrap_or(ColumnType::VarString),
        Expr::Case {
            results,
            else_result,
            ..
        } => case_result_type(results, else_result.as_deref(), recurse),
        Expr::Function { name, args, .. } if name.eq_ignore_ascii_case("CAST") => {
            cast_result_type(args)
        }
        Expr::Function { name, args, .. } => {
            if let Some(descriptor) = resolved_stored_function_type(name) {
                return descriptor.column_type();
            }
            // Context-free typing: column-ref branches can't be resolved here,
            // so schema-aware callers (single_table / plan paths) pass their
            // own `type_of`. Falls back to name-based classification.
            conditional_function_result_type(name, args, recurse)
                .unwrap_or_else(|| function_result_type(name))
        }
        _ => ColumnType::VarString,
    }
}

/// Result type of the conditional functions (IFNULL / COALESCE / IF / NULLIF)
/// whose type follows their argument branches — MySQL aggregates the branch
/// types rather than deriving from the function name. Returns `None` when
/// `name` is not one of them (caller should fall back to [`function_result_type`]).
///
/// `type_of` types an argument sub-expression: schema-aware callers pass a
/// closure that resolves column references against their table/plan, so
/// `COALESCE(int_col, int_col)` types as BIGINT rather than VARCHAR. A NULL
/// literal branch carries no type and is skipped; the remaining branches must
/// agree, otherwise we conservatively return VARCHAR rather than guess a
/// numeric widening that could diverge from the runtime value.
pub(crate) fn conditional_function_result_type<F>(
    name: &str,
    args: &[Expr],
    mut type_of: F,
) -> Option<ColumnType>
where
    F: FnMut(&Expr) -> ColumnType,
{
    let upper = crate::engine::sys_schema::NormalizedFuncName::new(name);
    match upper.as_str() {
        spatial if crate::engine::geo::sql::is_geo_function(spatial) => {
            crate::engine::geo::sql::result_type(spatial, args.len())
        }
        // IFNULL(a,b) / COALESCE(a,b,..): aggregate over all branches.
        "IFNULL" | "COALESCE" => Some(unify_branch_types(args, 0, &mut type_of)),
        // IF(cond, a, b): the two result branches (skip the condition).
        "IF" => Some(unify_branch_types(args, 1, &mut type_of)),
        // NULLIF(a,b): the result is always `a` (or NULL), so it takes a's type.
        "NULLIF" => Some(args.first().map(type_of).unwrap_or(ColumnType::VarString)),
        "ABS" => Some(args.first().map(type_of).unwrap_or(ColumnType::Double)),
        "ROUND" | "TRUNCATE" => {
            let input = args.first().map(&mut type_of).unwrap_or(ColumnType::Double);
            Some(match input {
                ColumnType::Float | ColumnType::Double => ColumnType::Double,
                ColumnType::Decimal {
                    precision,
                    mut scale,
                } => {
                    if let Some(Expr::Literal(Literal::Int(d))) = args.get(1) {
                        scale = if *d <= 0 {
                            0
                        } else {
                            scale.min((*d).min(i64::from(u8::MAX)) as u8)
                        };
                    }
                    ColumnType::Decimal { precision, scale }
                }
                integer if is_integer_column_type(integer) => ColumnType::LongLong,
                _ => ColumnType::Double,
            })
        }
        "CEIL" | "CEILING" | "FLOOR" => {
            let input = args.first().map(&mut type_of).unwrap_or(ColumnType::Double);
            Some(match input {
                ColumnType::Float | ColumnType::Double => ColumnType::Double,
                ColumnType::Decimal { precision, scale } => {
                    let integer_digits = precision
                        .saturating_sub(scale)
                        .saturating_add(u8::from(scale > 0));
                    if integer_digits <= 18 {
                        ColumnType::LongLong
                    } else {
                        ColumnType::Decimal {
                            precision: integer_digits,
                            scale: 0,
                        }
                    }
                }
                integer if is_integer_column_type(integer) => ColumnType::LongLong,
                _ => ColumnType::Double,
            })
        }
        "UNIX_TIMESTAMP" if !args.is_empty() => {
            let fsp =
                function_metadata_decimals(name, args).unwrap_or_else(|| match type_of(&args[0]) {
                    ColumnType::VarString | ColumnType::Blob
                        if matches!(args[0], Expr::Literal(_)) =>
                    {
                        0
                    }
                    ColumnType::VarString | ColumnType::Blob => 6,
                    ColumnType::Decimal { scale, .. } => scale.min(6),
                    _ => 0,
                });
            Some(if fsp == 0 {
                ColumnType::LongLong
            } else {
                ColumnType::Decimal {
                    precision: 20,
                    scale: fsp,
                }
            })
        }
        "STR_TO_DATE" => {
            let Some(Expr::Literal(Literal::String(fmt))) = args.get(1) else {
                return Some(ColumnType::DateTime);
            };
            let date = fmt.chars().any(|c| "YymcMbdDejUVuvXx".contains(c));
            let time = fmt.chars().any(|c| "HkhIliSsfrTp".contains(c));
            Some(if date && time {
                ColumnType::DateTime
            } else if time {
                ColumnType::Time
            } else {
                ColumnType::Date
            })
        }
        "JSON_VALUE" => Some(json_value_result_type(args)),
        _ => None,
    }
}

fn json_value_result_type(args: &[Expr]) -> ColumnType {
    let Some(Expr::Literal(Literal::String(returning))) = args.get(2) else {
        return ColumnType::VarString;
    };
    let (target, params) = json_value_type(returning);
    match target.to_ascii_uppercase().as_str() {
        "SIGNED" | "SIGNED INTEGER" | "INTEGER" | "UNSIGNED" | "UNSIGNED INTEGER" => {
            ColumnType::LongLong
        }
        "FLOAT" => ColumnType::Float,
        "DOUBLE" | "REAL" => ColumnType::Double,
        "DECIMAL" => ColumnType::Decimal {
            precision: params
                .first()
                .copied()
                .unwrap_or(10)
                .clamp(1, crate::types::MAX_DECIMAL_PRECISION.into()) as u8,
            scale: params.get(1).copied().unwrap_or(0).clamp(0, 30) as u8,
        },
        "DATE" => ColumnType::Date,
        "TIME" => ColumnType::Time,
        "DATETIME" => ColumnType::DateTime,
        _ => ColumnType::VarString,
    }
}

fn unify_branch_types<F>(args: &[Expr], skip: usize, type_of: &mut F) -> ColumnType
where
    F: FnMut(&Expr) -> ColumnType,
{
    unify_branch_type_iter(args.iter().skip(skip), type_of)
}

crate::engine::builtin_catalog::declare_default_result_type!();

pub(crate) fn function_result_type(name: &str) -> ColumnType {
    let upper_name = crate::engine::sys_schema::NormalizedFuncName::new(name);
    let upper = upper_name.as_str();
    // Sequence calls are session operations, but their result domain is always
    // signed BIGINT. It must not depend on constant folding or sampled rows.
    if matches!(upper, "NEXTVAL" | "CURRVAL" | "LASTVAL" | "SETVAL") {
        return ColumnType::LongLong;
    }
    if let Some(result_type) = crate::engine::geo::sql::result_type(upper, 1) {
        if crate::engine::geo::sql::is_geo_function(upper) {
            return result_type;
        }
    }
    // Vector functions: STRING_TO_VECTOR/TO_VECTOR yield VECTOR; the distance /
    // dot-product family yields DOUBLE; VECTOR_DIM yields an integer.
    if crate::engine::vector::sql::returns_vector(upper) {
        return ColumnType::Vector;
    }
    if upper == "VECTOR_TO_BINARY" {
        return ColumnType::Blob;
    }
    if upper == "VECTOR_DATATYPE" {
        return ColumnType::VarString;
    }
    if matches!(
        upper,
        "DISTANCE"
            | "VEC_DISTANCE"
            | "L2_DISTANCE"
            | "COSINE_DISTANCE"
            | "HAMMING_DISTANCE"
            | "JACCARD_DISTANCE"
            | "VECTOR_NORM"
            | "MAX_INNER_PRODUCT_DISTANCE"
            | "DOT_PRODUCT"
            | "INNER_PRODUCT"
    ) {
        return ColumnType::Double;
    }
    if upper == "VECTOR_DIM" {
        return ColumnType::Long;
    }
    if let Some(spec) = crate::engine::expr_type::builtin_spec(upper) {
        return spec.result.column_type();
    }
    default_result_type(upper)
}
