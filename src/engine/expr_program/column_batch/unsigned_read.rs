//! Exact unsigned arithmetic without a signed or floating-point round trip.
use super::*;

pub(super) fn bitwise(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::BitAnd
            | BinaryOp::BitOr
            | BinaryOp::BitXor
            | BinaryOp::ShiftLeft
            | BinaryOp::ShiftRight
    )
}

pub(super) enum UnsignedRead<'a> {
    Identity(&'a ExprNode),
    Binary {
        left: &'a ExprNode,
        right: &'a ExprNode,
        op: BinaryOp,
    },
}
impl<'a> UnsignedRead<'a> {
    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        if let ExprNode::Function {
            kernel: FunctionKernel::LegacyDeterministic("CAST"),
            args,
            ..
        } = node
        {
            if args.len() == 2
                && text(&args[1])?.eq_ignore_ascii_case("UNSIGNED")
                && matches!(super::contract(&args[0], schema)?.domain, Domain::Unsigned)
            {
                return Some(Self::Identity(&args[0]));
            }
        }
        let ExprNode::Binary {
            left,
            right,
            op,
            semantics,
        } = node
        else {
            return None;
        };
        if (!semantics.unsigned_operands && !bitwise(*op))
            || semantics.unsigned_subtraction
            || !(bitwise(*op)
                || comparison(*op)
                || matches!(
                    op,
                    BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Mod
                ))
        {
            return None;
        }
        let (a, b) = (
            super::contract(left, schema)?.domain,
            super::contract(right, schema)?.domain,
        );
        let positive = |d| {
            matches!(d, Domain::Unsigned | Domain::Null)
                || matches!(d, Domain::Integer(min,_) if min >= 0)
        };
        if bitwise(*op) {
            let numeric = |d| matches!(d, Domain::Integer(..) | Domain::Unsigned | Domain::Null);
            return (numeric(a) && numeric(b)).then_some(Self::Binary {
                left,
                right,
                op: *op,
            });
        }
        (positive(a)
            && positive(b)
            && (matches!(a, Domain::Unsigned) || matches!(b, Domain::Unsigned)))
        .then_some(Self::Binary {
            left,
            right,
            op: *op,
        })
    }
    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: if matches!(self, Self::Binary { op, .. } if comparison(*op)) {
                Domain::Integer(0, 1)
            } else {
                Domain::Unsigned
            },
            parallel: false,
        }
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let bytes = batch.num_rows().checked_mul(16)?;
        let work = match self {
            Self::Identity(input) => super::workspace(input, batch)?.1,
            Self::Binary { left, right, .. } => super::workspace(left, batch)?
                .1
                .checked_add(super::workspace(right, batch)?.1)?,
        };
        Some((
            bytes,
            work.checked_add(bytes.checked_mul(2)?)?.checked_add(512)?,
        ))
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        let Self::Binary { left, right, op } = self else {
            let Self::Identity(input) = self else {
                unreachable!()
            };
            let input = operand(input, batch)?;
            return matches!(
                input.as_ref(),
                ColumnVector::U64 { .. } | ColumnVector::Null { .. }
            )
            .then(|| input.into_owned());
        };
        let left = operand(left, batch)?;
        let right = operand(right, batch)?;
        let value = |column: &ColumnVector, row| match column {
            ColumnVector::U64 { data, .. } => Some(data[row]),
            ColumnVector::I64 { data, .. } if bitwise(*op) => Some(data[row] as u64),
            ColumnVector::I64 { data, .. } => u64::try_from(data[row]).ok(),
            _ => None,
        };
        let mut values = Vec::with_capacity(batch.num_rows());
        let mut bools = Vec::with_capacity(if comparison(*op) { batch.num_rows() } else { 0 });
        let mut nulls = Bitmap::new_all_valid(batch.num_rows());
        for row in 0..batch.num_rows() {
            if row & 63 == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            if left.is_null(row) || right.is_null(row) {
                nulls.set(row, false);
                values.push(0);
                bools.push(0);
                continue;
            }
            let (a, b) = (value(&left, row)?, value(&right, row)?);
            if comparison(*op) {
                let truth = match op {
                    BinaryOp::Eq => a == b,
                    BinaryOp::NotEq => a != b,
                    BinaryOp::Lt => a < b,
                    BinaryOp::LtEq => a <= b,
                    BinaryOp::Gt => a > b,
                    BinaryOp::GtEq => a >= b,
                    _ => return None,
                };
                bools.push(i64::from(truth));
            } else {
                values.push(match op {
                    BinaryOp::Add => a.checked_add(b)?,
                    BinaryOp::Sub => a.checked_sub(b)?,
                    BinaryOp::Mul => a.checked_mul(b)?,
                    BinaryOp::Mod => a.checked_rem(b)?,
                    BinaryOp::BitAnd => a & b,
                    BinaryOp::BitOr => a | b,
                    BinaryOp::BitXor => a ^ b,
                    BinaryOp::ShiftLeft => u32::try_from(b)
                        .ok()
                        .and_then(|n| a.checked_shl(n))
                        .unwrap_or(0),
                    BinaryOp::ShiftRight => u32::try_from(b)
                        .ok()
                        .and_then(|n| a.checked_shr(n))
                        .unwrap_or(0),
                    _ => return None,
                });
            }
        }
        Some(if comparison(*op) {
            ColumnVector::I64 {
                data: bools,
                nulls: Some(nulls),
            }
        } else {
            ColumnVector::U64 {
                data: values,
                nulls: Some(nulls),
            }
        })
    }
}
