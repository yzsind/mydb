//! Checked numeric composition with a diagnostic-free declared-domain proof.
use super::*;

#[derive(Clone, Copy)]
struct Exact {
    min: i128,
    max: i128,
    scale: u8,
    decimal: bool,
    unsigned: bool,
}
#[derive(Clone, Copy)]
enum Kind {
    Null,
    Exact(Exact),
    Approximate { min: f64, max: f64, finite: bool },
}
#[derive(Clone, Copy)]
struct Proof {
    kind: Kind,
    parallel: bool,
}

pub(super) struct Arithmetic<'a> {
    node: &'a ExprNode,
    proof: Proof,
}
impl<'a> Arithmetic<'a> {
    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        if !matches!(
            node,
            ExprNode::Binary {
                op: BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div,
                ..
            } | ExprNode::Unary {
                op: UnaryOp::Neg,
                ..
            }
        ) && !matches!(
            node,
            ExprNode::Function {
                kernel: FunctionKernel::Coalesce | FunctionKernel::IfNull,
                ..
            }
        ) {
            return None;
        }
        Some(Self {
            node,
            proof: describe(node, schema)?,
        })
    }
    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: match self.proof.kind {
                Kind::Null => Domain::Null,
                Kind::Exact(exact) if exact.decimal => Domain::Exact,
                Kind::Exact(exact) if exact.unsigned => Domain::Unsigned,
                Kind::Exact(exact) => Domain::Integer(exact.min as i64, exact.max as i64),
                Kind::Approximate { .. } => Domain::Approximate,
            },
            parallel: self.proof.parallel,
        }
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        match self.node {
            ExprNode::Function { args, .. } => evaluate_coalesce(args, self.proof, batch),
            ExprNode::Unary { expr, .. } => {
                let column = operand(expr, batch)?;
                let proof = describe(expr, batch.schema())?;
                if matches!(proof.kind, Kind::Approximate { .. }) {
                    match column.as_ref() {
                        ColumnVector::F64 { .. } => {}
                        ColumnVector::Generic { values }
                            if values.iter().all(|value| {
                                matches!(value, Value::Null | Value::Float(_) | Value::Double(_))
                            }) => {}
                        ColumnVector::Null { .. } => {}
                        _ => return None,
                    }
                }
                let (leading, leading_kind, op) =
                    if matches!(self.proof.kind, Kind::Approximate { .. }) {
                        (
                            ColumnVector::F64 {
                                data: vec![-1.0; batch.num_rows()],
                                nulls: None,
                            },
                            approximate(-1.0),
                            BinaryOp::Mul,
                        )
                    } else {
                        (
                            ColumnVector::I64 {
                                data: vec![0; batch.num_rows()],
                                nulls: None,
                            },
                            Kind::Exact(integer(0, 0, false)),
                            BinaryOp::Sub,
                        )
                    };
                evaluate_binary(
                    op,
                    self.proof,
                    &leading,
                    leading_kind,
                    None,
                    &column,
                    proof.kind,
                    float_leaf(expr, batch.schema()),
                    batch.num_rows(),
                )
            }
            ExprNode::Binary {
                left, op, right, ..
            } => {
                let left_column = operand(left, batch)?;
                let right_column = operand(right, batch)?;
                evaluate_binary(
                    *op,
                    self.proof,
                    &left_column,
                    describe(left, batch.schema())?.kind,
                    float_leaf(left, batch.schema()),
                    &right_column,
                    describe(right, batch.schema())?.kind,
                    float_leaf(right, batch.schema()),
                    batch.num_rows(),
                )
            }
            _ => None,
        }
    }
}

fn integer(min: i128, max: i128, unsigned: bool) -> Exact {
    Exact {
        min,
        max,
        scale: 0,
        decimal: false,
        unsigned,
    }
}
fn decimal(min: i128, max: i128, scale: u8) -> Exact {
    Exact {
        min,
        max,
        scale,
        decimal: true,
        unsigned: false,
    }
}
fn float_leaf(node: &ExprNode, schema: &BatchSchema) -> Option<bool> {
    if let ExprNode::Column(column) = node {
        Some(schema.fields().get(*column)?.column_type == ColumnType::Float)
    } else {
        None
    }
}
fn describe(node: &ExprNode, schema: &BatchSchema) -> Option<Proof> {
    let pure = |kind| {
        Some(Proof {
            kind,
            parallel: true,
        })
    };
    match node {
        ExprNode::Function {
            kernel: FunctionKernel::Coalesce | FunctionKernel::IfNull,
            args,
            result_coercion,
            ..
        } if !args.is_empty()
            && (!matches!(
                node,
                ExprNode::Function {
                    kernel: FunctionKernel::IfNull,
                    ..
                }
            ) || args.len() == 2) =>
        {
            describe_coalesce(args, *result_coercion, schema)
        }
        ExprNode::Column(column) => {
            let field = schema.fields().get(*column)?;
            if field.source.dynamic_parameter {
                return None;
            }
            let unsigned = field.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0;
            let bits = match field.column_type {
                ColumnType::Tiny => 8,
                ColumnType::Short => 16,
                ColumnType::MediumInt => 24,
                ColumnType::Long => 32,
                ColumnType::LongLong => 64,
                ColumnType::Bit { bits } => {
                    return pure(Kind::Exact(integer(0, (1i128 << bits) - 1, true)));
                }
                ColumnType::Decimal { precision, scale } => {
                    let max = 10i128.checked_pow(u32::from(precision))?.checked_sub(1)?;
                    return pure(Kind::Exact(decimal(-max, max, scale)));
                }
                ColumnType::Float | ColumnType::Double => {
                    let max = if field.column_type == ColumnType::Float {
                        f64::from(f32::MAX)
                    } else {
                        f64::MAX
                    };
                    return pure(Kind::Approximate {
                        min: -max,
                        max,
                        finite: false,
                    });
                }
                _ => return None,
            };
            pure(Kind::Exact(if unsigned {
                integer(0, (1i128 << bits) - 1, true)
            } else {
                integer(-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1, false)
            }))
        }
        ExprNode::Const(_) | ExprNode::Placeholder(_) => pure(match constant(node)?.as_ref() {
            Value::Null => Kind::Null,
            Value::Int(value) => {
                Kind::Exact(integer(i128::from(*value), i128::from(*value), false))
            }
            Value::UInt(value) => {
                Kind::Exact(integer(i128::from(*value), i128::from(*value), true))
            }
            Value::DecimalCompact(value, scale) => {
                Kind::Exact(decimal(i128::from(*value), i128::from(*value), *scale))
            }
            Value::Decimal128(value, scale) => Kind::Exact(decimal(*value, *value, *scale)),
            Value::Float(value) => approximate(f64::from(*value)),
            Value::Double(value) => approximate(*value),
            _ => return None,
        }),
        ExprNode::Unary {
            op: UnaryOp::Neg,
            expr,
            ..
        } => {
            let source = describe(expr, schema)?;
            match source.kind {
                Kind::Null => pure(Kind::Null),
                Kind::Exact(value) if !value.unsigned => {
                    let result = Exact {
                        min: value.max.checked_neg()?,
                        max: value.min.checked_neg()?,
                        ..value
                    };
                    if !result.decimal
                        && (result.min < i64::MIN as i128 || result.max > i64::MAX as i128)
                    {
                        return None;
                    }
                    Some(Proof {
                        kind: Kind::Exact(result),
                        parallel: source.parallel,
                    })
                }
                Kind::Approximate { min, max, finite } => Some(Proof {
                    kind: Kind::Approximate {
                        min: -max,
                        max: -min,
                        finite,
                    },
                    parallel: source.parallel && finite,
                }),
                _ => None,
            }
        }
        ExprNode::Binary {
            left,
            op,
            right,
            semantics,
        } if matches!(
            op,
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div
        ) && !semantics.unsigned_subtraction =>
        {
            let a = describe(left, schema)?;
            let b = describe(right, schema)?;
            if matches!(a.kind, Kind::Null) || matches!(b.kind, Kind::Null) {
                return Some(Proof {
                    kind: Kind::Null,
                    parallel: a.parallel && b.parallel,
                });
            }
            if let (Kind::Exact(a_kind), Kind::Exact(b_kind)) = (a.kind, b.kind) {
                if *op == BinaryOp::Div {
                    return None;
                }
                let decimal = a_kind.decimal || b_kind.decimal;
                let scale = if *op == BinaryOp::Mul {
                    a_kind.scale.checked_add(b_kind.scale)?
                } else {
                    a_kind.scale.max(b_kind.scale)
                };
                if scale > crate::util::decimal::MAX_DECIMAL_SCALE {
                    return None;
                }
                let (amin, amax, bmin, bmax) = if *op == BinaryOp::Mul {
                    (a_kind.min, a_kind.max, b_kind.min, b_kind.max)
                } else {
                    let af = 10i128.checked_pow(u32::from(scale - a_kind.scale))?;
                    let bf = 10i128.checked_pow(u32::from(scale - b_kind.scale))?;
                    (
                        a_kind.min.checked_mul(af)?,
                        a_kind.max.checked_mul(af)?,
                        b_kind.min.checked_mul(bf)?,
                        b_kind.max.checked_mul(bf)?,
                    )
                };
                let apply = |x: i128, y: i128| match op {
                    BinaryOp::Add => x.checked_add(y),
                    BinaryOp::Sub => x.checked_sub(y),
                    _ => x.checked_mul(y),
                };
                let values = [
                    apply(amin, bmin)?,
                    apply(amin, bmax)?,
                    apply(amax, bmin)?,
                    apply(amax, bmax)?,
                ];
                let min = *values.iter().min()?;
                let max = *values.iter().max()?;
                let unsigned = !decimal && (a_kind.unsigned || b_kind.unsigned);
                if !decimal
                    && ((unsigned && (min < 0 || max > u64::MAX as i128))
                        || (!unsigned && (min < i64::MIN as i128 || max > i64::MAX as i128)))
                {
                    return None;
                }
                return Some(Proof {
                    kind: Kind::Exact(Exact {
                        min,
                        max,
                        scale,
                        decimal,
                        unsigned,
                    }),
                    parallel: a.parallel && b.parallel,
                });
            }
            let (amin, amax, af) = approximate_bounds(a.kind)?;
            let (bmin, bmax, bf) = approximate_bounds(b.kind)?;
            let zero = *op == BinaryOp::Div && bmin <= 0.0 && bmax >= 0.0;
            let apply = |x: f64, y: f64| match op {
                BinaryOp::Add => x + y,
                BinaryOp::Sub => x - y,
                BinaryOp::Mul => x * y,
                _ => x / y,
            };
            let values = [
                apply(amin, bmin),
                apply(amin, bmax),
                apply(amax, bmin),
                apply(amax, bmax),
            ];
            let finite = af && bf && !zero && values.iter().all(|value| value.is_finite());
            let (min, max) = if finite {
                (
                    values.into_iter().fold(f64::INFINITY, f64::min),
                    values.into_iter().fold(f64::NEG_INFINITY, f64::max),
                )
            } else {
                (-f64::INFINITY, f64::INFINITY)
            };
            Some(Proof {
                kind: Kind::Approximate { min, max, finite },
                parallel: a.parallel && b.parallel && finite,
            })
        }
        _ => None,
    }
}

fn describe_coalesce(
    args: &[ExprNode],
    coercion: Option<crate::engine::expr_type::ExprType>,
    schema: &BatchSchema,
) -> Option<Proof> {
    use crate::engine::expr_type::ExprTypeKind;
    let mut result: Option<Exact> = None;
    for arg in args {
        let child = describe(arg, schema)?;
        // Eager column evaluation is allowed only when even an unchosen
        // branch cannot publish a warning, error or observable side effect.
        if !child.parallel {
            return None;
        }
        let mut value = match child.kind {
            Kind::Null => continue,
            Kind::Exact(value) => value,
            _ => return None,
        };
        match coercion.map(|value| value.kind) {
            Some(ExprTypeKind::Decimal {
                precision, scale, ..
            }) => {
                let factor = 10i128.checked_pow(u32::from(scale.checked_sub(value.scale)?))?;
                value.min = value.min.checked_mul(factor)?;
                value.max = value.max.checked_mul(factor)?;
                let limit = 10i128.checked_pow(u32::from(precision))?.checked_sub(1)?;
                if value.min < -limit || value.max > limit {
                    return None;
                }
                value.scale = scale;
                value.decimal = true;
                value.unsigned = false;
            }
            Some(ExprTypeKind::Integer { unsigned, .. })
                if !value.decimal && value.unsigned == unsigned => {}
            None => {}
            _ => return None,
        }
        if let Some(previous) = &mut result {
            if previous.scale != value.scale
                || previous.decimal != value.decimal
                || previous.unsigned != value.unsigned
            {
                return None;
            }
            previous.min = previous.min.min(value.min);
            previous.max = previous.max.max(value.max);
        } else {
            result = Some(value);
        }
    }
    Some(Proof {
        kind: result.map_or(Kind::Null, Kind::Exact),
        parallel: true,
    })
}

fn evaluate_coalesce(args: &[ExprNode], proof: Proof, batch: &Batch) -> Option<ColumnVector> {
    let Kind::Exact(result) = proof.kind else {
        return matches!(proof.kind, Kind::Null).then_some(ColumnVector::Null {
            len: batch.num_rows(),
        });
    };
    let inputs = args
        .iter()
        .map(|arg| Some((operand(arg, batch)?, describe(arg, batch.schema())?.kind)))
        .collect::<Option<Vec<_>>>()?;
    let mut values = crate::engine::executor::mem_budget::try_vec_with_capacity::<i128>(
        batch.num_rows(),
        "coalesce numeric output",
    )
    .ok()?;
    let mut valid = Bitmap::new_all_valid(batch.num_rows());
    let mut any_null = false;
    for row in 0..batch.num_rows() {
        if row % 1024 == 0 {
            crate::engine::cancel::check_cancelled().ok()?;
        }
        let mut chosen = None;
        for (column, kind) in &inputs {
            if column.is_null(row) {
                continue;
            }
            let Kind::Exact(source) = kind else {
                return None;
            };
            let factor = 10i128.checked_pow(u32::from(result.scale.checked_sub(source.scale)?))?;
            chosen = Some(exact_at(column, row, *source)?.checked_mul(factor)?);
            break;
        }
        values.push(chosen.unwrap_or(0));
        if chosen.is_none() {
            any_null = true;
            valid.set(row, false);
        }
    }
    let nulls = any_null.then_some(valid);
    Some(if result.decimal {
        ColumnVector::Decimal128 {
            data: values,
            scale: result.scale,
            nulls,
        }
    } else if result.unsigned {
        ColumnVector::U64 {
            data: values
                .into_iter()
                .map(u64::try_from)
                .collect::<Result<Vec<_>, _>>()
                .ok()?,
            nulls,
        }
    } else {
        ColumnVector::I64 {
            data: values
                .into_iter()
                .map(i64::try_from)
                .collect::<Result<Vec<_>, _>>()
                .ok()?,
            nulls,
        }
    })
}
fn approximate(value: f64) -> Kind {
    if value.is_finite() {
        Kind::Approximate {
            min: value,
            max: value,
            finite: true,
        }
    } else {
        Kind::Approximate {
            min: -f64::INFINITY,
            max: f64::INFINITY,
            finite: false,
        }
    }
}
fn approximate_bounds(kind: Kind) -> Option<(f64, f64, bool)> {
    match kind {
        Kind::Exact(value) => {
            let divisor = 10f64.powi(i32::from(value.scale));
            Some((value.min as f64 / divisor, value.max as f64 / divisor, true))
        }
        Kind::Approximate { min, max, finite } => Some((min, max, finite)),
        Kind::Null => None,
    }
}

fn exact_at(column: &ColumnVector, row: usize, expected: Exact) -> Option<i128> {
    let (value, scale, decimal, unsigned) = match column {
        ColumnVector::I64 { data, .. } => (i128::from(data[row]), 0, false, false),
        ColumnVector::U64 { data, .. } => (i128::from(data[row]), 0, false, true),
        ColumnVector::Bool { values, .. } => (i128::from(values.get(row)), 0, false, false),
        ColumnVector::Decimal32 { data, scale, .. } => (i128::from(data[row]), *scale, true, false),
        ColumnVector::Decimal64 { data, scale, .. } => (i128::from(data[row]), *scale, true, false),
        ColumnVector::Decimal128 { data, scale, .. } => (data[row], *scale, true, false),
        ColumnVector::Generic { values } => match &values[row] {
            Value::Int(value) => (i128::from(*value), 0, false, false),
            Value::UInt(value) => (i128::from(*value), 0, false, true),
            Value::DecimalCompact(value, scale) => (i128::from(*value), *scale, true, false),
            Value::Decimal128(value, scale) => (*value, *scale, true, false),
            _ => return None,
        },
        _ => return None,
    };
    (scale == expected.scale && decimal == expected.decimal && unsigned == expected.unsigned)
        .then_some(value)
}
fn float_at(column: &ColumnVector, row: usize, expected: Kind, narrow: bool) -> Option<f64> {
    if let Kind::Exact(exact) = expected {
        return Some(exact_at(column, row, exact)? as f64 / 10f64.powi(i32::from(exact.scale)));
    }
    match column {
        ColumnVector::F64 { data, .. } => Some(if narrow {
            f64::from(data[row] as f32)
        } else {
            data[row]
        }),
        ColumnVector::Generic { values } => {
            let value = &values[row];
            if let Value::Double(value) = value {
                return Some(if narrow {
                    f64::from(*value as f32)
                } else {
                    *value
                });
            }
            matches!(
                value,
                Value::Int(_)
                    | Value::UInt(_)
                    | Value::Float(_)
                    | Value::DecimalCompact(..)
                    | Value::Decimal128(..)
            )
            .then(|| crate::engine::expr::numeric(value).ok())
            .flatten()
        }
        _ => None,
    }
}
#[allow(clippy::too_many_arguments)]
fn evaluate_binary(
    op: BinaryOp,
    proof: Proof,
    left: &ColumnVector,
    a: Kind,
    af: Option<bool>,
    right: &ColumnVector,
    b: Kind,
    bf: Option<bool>,
    rows: usize,
) -> Option<ColumnVector> {
    if matches!(proof.kind, Kind::Null) {
        return Some(ColumnVector::Null { len: rows });
    }
    let mut valid = Bitmap::new_all_valid(rows);
    let mut any_null = false;
    macro_rules! numeric_loop {
        ($ty:ty, $calculate:expr, $finish:expr) => {{
            let mut data = crate::engine::executor::mem_budget::try_vec_with_capacity::<$ty>(
                rows,
                "column arithmetic output",
            )
            .ok()?;
            for row in 0..rows {
                if row % 1024 == 0 {
                    crate::engine::cancel::check_cancelled().ok()?;
                }
                if left.is_null(row) || right.is_null(row) {
                    valid.set(row, false);
                    any_null = true;
                    data.push(Default::default());
                } else {
                    data.push(($calculate)(row)?);
                }
            }
            let nulls = any_null.then_some(valid);
            Some(($finish)(data, nulls))
        }};
    }
    match proof.kind {
        Kind::Approximate { .. } => numeric_loop!(
            f64,
            |row| {
                let l = float_at(left, row, a, af.unwrap_or(false))?;
                let r = float_at(right, row, b, bf.unwrap_or(false))?;
                if !l.is_finite() || !r.is_finite() || (op == BinaryOp::Div && r == 0.0) {
                    return None;
                }
                let value = match op {
                    BinaryOp::Add => l + r,
                    BinaryOp::Sub => l - r,
                    BinaryOp::Mul => l * r,
                    BinaryOp::Div => l / r,
                    _ => return None,
                };
                value.is_finite().then_some(value)
            },
            |data, nulls| ColumnVector::F64 { data, nulls }
        ),
        Kind::Exact(result) => {
            let (Kind::Exact(a), Kind::Exact(b)) = (a, b) else {
                return None;
            };
            let af = if op == BinaryOp::Mul {
                1
            } else {
                10i128.checked_pow(u32::from(result.scale - a.scale))?
            };
            let bf = if op == BinaryOp::Mul {
                1
            } else {
                10i128.checked_pow(u32::from(result.scale - b.scale))?
            };
            let calculate = |row| {
                let l = exact_at(left, row, a)?.checked_mul(af)?;
                let r = exact_at(right, row, b)?.checked_mul(bf)?;
                match op {
                    BinaryOp::Add => l.checked_add(r),
                    BinaryOp::Sub => l.checked_sub(r),
                    BinaryOp::Mul => l.checked_mul(r),
                    _ => None,
                }
            };
            if result.decimal {
                numeric_loop!(i128, calculate, |data, nulls| ColumnVector::Decimal128 {
                    data,
                    scale: result.scale,
                    nulls
                })
            } else if result.unsigned {
                numeric_loop!(
                    u64,
                    |row| u64::try_from(calculate(row)?).ok(),
                    |data, nulls| ColumnVector::U64 { data, nulls }
                )
            } else {
                numeric_loop!(
                    i64,
                    |row| i64::try_from(calculate(row)?).ok(),
                    |data, nulls| ColumnVector::I64 { data, nulls }
                )
            }
        }
        Kind::Null => unreachable!(),
    }
}
