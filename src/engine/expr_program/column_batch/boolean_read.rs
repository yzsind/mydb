//! Diagnostic-free three-valued logic over exact integer lanes.
use super::*;
enum Kind {
    Not,
    IsTrue,
    IsFalse,
    IsNull,
    IsNotNull,
    Logical(BinaryOp),
    NullSafeEqual,
}
pub(super) struct BooleanRead<'a> {
    left: &'a ExprNode,
    right: Option<&'a ExprNode>,
    kind: Kind,
}
impl<'a> BooleanRead<'a> {
    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        let (left, right, kind) = match node {
            ExprNode::Unary {
                op: UnaryOp::Not,
                expr,
                ..
            } => (expr.as_ref(), None, Kind::Not),
            ExprNode::IsNull(expr) => (expr.as_ref(), None, Kind::IsNull),
            ExprNode::IsNotNull(expr) => (expr.as_ref(), None, Kind::IsNotNull),
            ExprNode::Function {
                kernel: FunctionKernel::IsTrue,
                args,
                ..
            } if args.len() == 1 => (&args[0], None, Kind::IsTrue),
            ExprNode::Function {
                kernel: FunctionKernel::IsFalse,
                args,
                ..
            } if args.len() == 1 => (&args[0], None, Kind::IsFalse),
            ExprNode::Binary {
                left,
                right,
                op: BinaryOp::And | BinaryOp::Or | BinaryOp::Xor,
                ..
            } => (
                left.as_ref(),
                Some(right.as_ref()),
                Kind::Logical(match node {
                    ExprNode::Binary { op, .. } => *op,
                    _ => unreachable!(),
                }),
            ),
            ExprNode::Binary {
                left,
                right,
                op: BinaryOp::NullSafeEq,
                ..
            } => (left.as_ref(), Some(right.as_ref()), Kind::NullSafeEqual),
            _ => return None,
        };
        let a = super::contract(left, schema)?;
        if !a.parallel {
            return None;
        }
        let numeric = |d| matches!(d, Domain::Integer(..) | Domain::Unsigned | Domain::Null);
        if !matches!(kind, Kind::IsNull | Kind::IsNotNull) && !numeric(a.domain) {
            return None;
        }
        if let Some(right) = right {
            let b = super::contract(right, schema)?;
            if !b.parallel || !numeric(b.domain) {
                return None;
            }
            if matches!(kind, Kind::NullSafeEqual) {
                let signed = |d| matches!(d, Domain::Integer(..) | Domain::Null);
                let unsigned = |d| {
                    matches!(d, Domain::Unsigned | Domain::Null)
                        || matches!(d,Domain::Integer(min,_) if min>=0)
                };
                if !(signed(a.domain) && signed(b.domain)
                    || unsigned(a.domain) && unsigned(b.domain))
                {
                    return None;
                }
            }
        }
        Some(Self { left, right, kind })
    }
    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: Domain::Integer(0, 1),
            parallel: true,
        }
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let bytes = batch.num_rows().checked_mul(16)?;
        let mut child = super::workspace(self.left, batch)?.1;
        if let Some(right) = self.right {
            child = child.checked_add(super::workspace(right, batch)?.1)?;
        }
        Some((
            bytes,
            child.checked_add(bytes.checked_mul(2)?)?.checked_add(512)?,
        ))
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        let left = operand(self.left, batch)?;
        let right = match self.right {
            Some(right) => Some(operand(right, batch)?),
            None => None,
        };
        let numeric = |column: &ColumnVector, row| -> Option<i128> {
            if column.is_null(row) {
                return None;
            }
            match column {
                ColumnVector::I64 { data, .. } => Some(i128::from(data[row])),
                ColumnVector::U64 { data, .. } => Some(i128::from(data[row])),
                ColumnVector::Bool { values, .. } => Some(i128::from(values.get(row))),
                _ => None,
            }
        };
        if !matches!(self.kind, Kind::IsNull | Kind::IsNotNull)
            && std::iter::once(&left).chain(right.iter()).any(|column| {
                !matches!(
                    column.as_ref(),
                    ColumnVector::I64 { .. }
                        | ColumnVector::U64 { .. }
                        | ColumnVector::Bool { .. }
                        | ColumnVector::Null { .. }
                )
            })
        {
            return None;
        }
        let mut data = Vec::with_capacity(batch.num_rows());
        let mut nulls = Bitmap::new_all_valid(batch.num_rows());
        for row in 0..batch.num_rows() {
            if row & 63 == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            let a = numeric(&left, row);
            let ta = a.map(|n| n != 0);
            let value = match self.kind {
                Kind::IsNull => Some(left.is_null(row)),
                Kind::IsNotNull => Some(!left.is_null(row)),
                Kind::IsTrue => Some(ta == Some(true)),
                Kind::IsFalse => Some(ta == Some(false)),
                Kind::Not => ta.map(|v| !v),
                Kind::NullSafeEqual => Some(a == numeric(right.as_deref()?, row)),
                Kind::Logical(op) => {
                    let b = numeric(right.as_deref()?, row).map(|n| n != 0);
                    match op {
                        BinaryOp::And => {
                            if ta == Some(false) || b == Some(false) {
                                Some(false)
                            } else if ta == Some(true) && b == Some(true) {
                                Some(true)
                            } else {
                                None
                            }
                        }
                        BinaryOp::Or => {
                            if ta == Some(true) || b == Some(true) {
                                Some(true)
                            } else if ta == Some(false) && b == Some(false) {
                                Some(false)
                            } else {
                                None
                            }
                        }
                        BinaryOp::Xor => ta.zip(b).map(|(a, b)| a ^ b),
                        _ => return None,
                    }
                }
            };
            if value.is_none() {
                nulls.set(row, false);
            }
            data.push(i64::from(value.unwrap_or(false)));
        }
        Some(ColumnVector::I64 {
            data,
            nulls: Some(nulls),
        })
    }
}
