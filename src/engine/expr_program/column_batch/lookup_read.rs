//! Integer comparison and bounded choice domains retain eager argument order
//! without per-row argument vectors or lossy mixed-type coercions.
use super::*;
use std::borrow::Cow;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Field,
    FieldDouble,
    Interval,
    Elt,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Family {
    Signed,
    Unsigned,
    Text,
    Binary,
}
pub(super) struct LookupRead<'a> {
    kind: Kind,
    args: Cow<'a, [ExprNode]>,
    family: Family,
    width: usize,
}
enum Input<'a> {
    Constant(Cow<'a, Value>),
    Column(Cow<'a, ColumnVector>),
}
impl Input<'_> {
    fn number(&self, row: usize) -> Option<i128> {
        match self {
            Self::Constant(v) => match v.as_ref() {
                Value::Int(n) => Some(i128::from(*n)),
                Value::UInt(n) => Some(i128::from(*n)),
                _ => None,
            },
            Self::Column(v) if !v.is_null(row) => match v.as_ref() {
                ColumnVector::I64 { data, .. } => Some(i128::from(data[row])),
                ColumnVector::U64 { data, .. } => Some(i128::from(data[row])),
                _ => None,
            },
            _ => None,
        }
    }
    fn bytes(&self, row: usize) -> Option<&[u8]> {
        match self {
            Self::Constant(v) => match v.as_ref() {
                Value::Bytes(b) => Some(b),
                v => crate::types::value_str(v).map(str::as_bytes),
            },
            Self::Column(v) if !v.is_null(row) => match v.as_ref() {
                ColumnVector::Utf8 { offsets, data, .. }
                | ColumnVector::Bytes { offsets, data, .. } => {
                    Some(&data[offsets[row] as usize..offsets[row + 1] as usize])
                }
                _ => None,
            },
            _ => None,
        }
    }
}
impl<'a> LookupRead<'a> {
    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        let (kind, args) = match node {
            ExprNode::Function {
                kernel:
                    FunctionKernel::Field(crate::engine::expr::FieldComparisonDomain::ExactNumeric),
                args,
                ..
            } => (Kind::Field, Cow::Borrowed(args.as_slice())),
            ExprNode::Function {
                kernel: FunctionKernel::Field(crate::engine::expr::FieldComparisonDomain::Double),
                args,
                ..
            } => (Kind::FieldDouble, Cow::Borrowed(args.as_slice())),
            ExprNode::Function {
                kernel: FunctionKernel::LegacyDeterministic("ELT"),
                args,
                ..
            } => (Kind::Elt, Cow::Borrowed(args.as_slice())),
            ExprNode::LegacyEscape {
                expr,
                schema: table,
                ..
            } => {
                let Expr::Function {
                    name,
                    args,
                    distinct: false,
                } = expr.as_ref()
                else {
                    return None;
                };
                if !name.eq_ignore_ascii_case("INTERVAL")
                    && !name.eq_ignore_ascii_case("__INTERVAL_LIST")
                {
                    return None;
                }
                if args.len() > 256 || !bounded_bound_literals(args) {
                    return None;
                }
                let mut input = None;
                let nodes = args
                    .iter()
                    .map(|arg| {
                        Some(match arg {
                            Expr::Identifier(name) => {
                                ExprNode::Column(table.find_column_index(name)?)
                            }
                            Expr::Literal(v) => ExprNode::Const(literal_value(v)),
                            Expr::BoundValue(v) => ExprNode::Const(v.clone()),
                            Expr::Placeholder(i) => ExprNode::Placeholder(*i),
                            _ => compile_node(
                                arg,
                                input.get_or_insert_with(|| InputSchema::from_table(table)),
                            )
                            .map(fold_bound_integer_sign)
                            .ok()?,
                        })
                    })
                    .collect::<Option<Vec<_>>>()?;
                (Kind::Interval, Cow::Owned(nodes))
            }
            _ => return None,
        };
        if args.len() < 2 || args.len() > 256 {
            return None;
        }
        let integer = |arg: &ExprNode| {
            let proof = contract(arg, schema)?;
            (proof.parallel
                && matches!(
                    proof.domain,
                    Domain::Integer(..) | Domain::Unsigned | Domain::Null
                ))
            .then_some(())
        };
        integer(&args[0])?;
        let mut family = None;
        let mut width = 8usize;
        for arg in &args[1..] {
            if kind != Kind::Elt {
                integer(arg)?;
                continue;
            }
            let proof = contract(arg, schema)?;
            if !proof.parallel {
                return None;
            }
            let next = match proof.domain {
                Domain::Null => continue,
                Domain::Integer(..) => Family::Signed,
                Domain::Unsigned => Family::Unsigned,
                Domain::Text => Family::Text,
                Domain::Binary => Family::Binary,
                _ => return None,
            };
            if family.is_some_and(|previous| previous != next) {
                return None;
            }
            family = Some(next);
            if matches!(next, Family::Text | Family::Binary) {
                width = width.max(usize::try_from(string_bound(arg, schema)?).ok()?);
            }
        }
        Some(Self {
            kind,
            args,
            family: family.unwrap_or(Family::Signed),
            width,
        })
    }
    pub(super) fn contract(&self) -> Contract {
        Contract {
            parallel: true,
            domain: if self.kind == Kind::Elt {
                match self.family {
                    Family::Signed => Domain::Integer(i64::MIN, i64::MAX),
                    Family::Unsigned => Domain::Unsigned,
                    Family::Text => Domain::Text,
                    Family::Binary => Domain::Binary,
                }
            } else {
                Domain::Integer(
                    if self.kind == Kind::Interval { -1 } else { 0 },
                    self.args.len() as i64 - 1,
                )
            },
        }
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let output = batch
            .num_rows()
            .checked_mul(self.width.checked_add(8)?)?
            .checked_add(512)?;
        let mut work = output.checked_mul(2)?.checked_add(
            self.args
                .len()
                .checked_mul(std::mem::size_of::<Input<'_>>() + std::mem::size_of::<ExprNode>())?
                .checked_mul(2)?,
        )?;
        for arg in self.args.iter() {
            if let Some(v) = constant(arg) {
                use crate::engine::executor::memory_footprint::AllocatedBytes;
                work = work.checked_add(v.allocated_bytes().checked_mul(2)?)?;
            } else {
                work = work.checked_add(workspace(arg, batch)?.1)?;
            }
        }
        Some((output, work.checked_add(1024)?))
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        let inputs = self
            .args
            .iter()
            .map(|node| {
                Some(match constant(node) {
                    Some(v) => Input::Constant(v),
                    None => Input::Column(operand(node, batch)?),
                })
            })
            .collect::<Option<Vec<_>>>()?;
        if inputs.iter().enumerate().any(|(i, input)| match input {
            Input::Column(column) => {
                if self.kind != Kind::Elt
                    || i == 0
                    || matches!(self.family, Family::Signed | Family::Unsigned)
                {
                    !matches!(
                        column.as_ref(),
                        ColumnVector::I64 { .. }
                            | ColumnVector::U64 { .. }
                            | ColumnVector::Null { .. }
                    )
                } else {
                    !matches!(
                        column.as_ref(),
                        ColumnVector::Utf8 { .. }
                            | ColumnVector::Bytes { .. }
                            | ColumnVector::Null { .. }
                    )
                }
            }
            _ => false,
        }) {
            return None;
        }
        let mut numbers = Vec::new();
        let mut unsigned = Vec::new();
        let mut bytes = Vec::new();
        let mut offsets = Vec::new();
        let text = self.kind == Kind::Elt && matches!(self.family, Family::Text | Family::Binary);
        if text {
            offsets.reserve(batch.num_rows() + 1);
            offsets.push(0);
        } else if self.kind == Kind::Elt && self.family == Family::Unsigned {
            unsigned.reserve(batch.num_rows());
        } else {
            numbers.reserve(batch.num_rows());
        }
        let mut nulls = Bitmap::new_all_valid(batch.num_rows());
        let mut any_null = false;
        for row in 0..batch.num_rows() {
            if row & 63 == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            let first = inputs[0].number(row);
            match self.kind {
                Kind::Field | Kind::FieldDouble => numbers.push(first.map_or(0, |first| {
                    inputs[1..]
                        .iter()
                        .position(|input| {
                            input.number(row).is_some_and(|value| {
                                if self.kind == Kind::FieldDouble {
                                    value as f64 == first as f64
                                } else {
                                    value == first
                                }
                            })
                        })
                        .map_or(0, |index| index as i64 + 1)
                })),
                Kind::Interval => {
                    let mut index = -1;
                    if let Some(first) = first {
                        index = 0;
                        for input in &inputs[1..] {
                            if input.number(row).is_none_or(|n| n as f64 <= first as f64) {
                                index += 1;
                            } else {
                                break;
                            }
                        }
                    }
                    numbers.push(index);
                }
                Kind::Elt => {
                    let chosen = first
                        .and_then(|n| usize::try_from(n).ok())
                        .filter(|&index| index > 0 && index < inputs.len());
                    let present = if text {
                        chosen
                            .and_then(|index| inputs[index].bytes(row))
                            .map(|value| bytes.extend_from_slice(value))
                            .is_some()
                    } else {
                        let value = chosen.and_then(|index| inputs[index].number(row));
                        if self.family == Family::Unsigned {
                            unsigned.push(value.unwrap_or(0) as u64);
                        } else {
                            numbers.push(value.unwrap_or(0) as i64);
                        }
                        value.is_some()
                    };
                    if !present {
                        any_null = true;
                        nulls.set(row, false);
                    }
                    if text {
                        offsets.push(u32::try_from(bytes.len()).ok()?);
                    }
                }
            }
        }
        let nulls = any_null.then_some(nulls);
        Some(if text {
            if self.family == Family::Binary {
                ColumnVector::Bytes {
                    offsets,
                    data: bytes,
                    nulls,
                }
            } else {
                ColumnVector::Utf8 {
                    offsets,
                    data: bytes,
                    nulls,
                }
            }
        } else if self.kind == Kind::Elt && self.family == Family::Unsigned {
            ColumnVector::U64 {
                data: unsigned,
                nulls,
            }
        } else {
            ColumnVector::I64 {
                data: numbers,
                nulls,
            }
        })
    }
}
