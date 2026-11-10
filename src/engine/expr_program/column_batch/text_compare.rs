//! Exact text comparisons that compose with boolean and conditional kernels.
//! Mixed coercions, binary values and ENUM/SET retain the ordered row oracle.
use super::*;

pub(super) struct TextCompare<'a> {
    left: &'a ExprNode,
    right: &'a ExprNode,
    op: BinaryOp,
    collation_id: Option<u16>,
}

impl<'a> TextCompare<'a> {
    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        let ExprNode::Binary {
            left,
            right,
            op,
            semantics,
        } = node
        else {
            return None;
        };
        if !comparison(*op) && *op != BinaryOp::NullSafeEq {
            return None;
        }
        // An enclosing numeric coercion scope must keep its own oracle.
        let _ = crate::engine::expr::compare_borrowed_strings("", "")?;
        let text = |node: &ExprNode| -> Option<bool> {
            if let ExprNode::Column(index) = node {
                let field = schema.fields().get(*index)?;
                return (matches!(field.column_type, ColumnType::VarString | ColumnType::Blob)
                    && field.source.flags
                        & (crate::mysql::protocol::BINARY_FLAG
                            | crate::mysql::protocol::ENUM_FLAG
                            | crate::mysql::protocol::SET_FLAG)
                        == 0
                    && field.source.collation_id != Some(crate::charset::BINARY_COLLATION_ID))
                .then_some(true);
            }
            match constant(node)?.as_ref() {
                Value::Null => Some(false),
                Value::String(_) => Some(true),
                Value::SmallString(bytes) => std::str::from_utf8(bytes).ok().map(|_| true),
                _ => None,
            }
        };
        let (a, b) = (text(left)?, text(right)?);
        if !a && !b {
            return None;
        }
        Some(Self {
            left,
            right,
            op: *op,
            collation_id: semantics.collation_id,
        })
    }

    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: Domain::Integer(0, 1),
            parallel: true,
        }
    }

    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let output = batch.num_rows().checked_mul(16)?.checked_add(256)?;
        let (a, aw) = super::workspace(self.left, batch)?;
        let (b, bw) = super::workspace(self.right, batch)?;
        // Include borrowed payloads as well: collation implementations may
        // need temporary weights proportional to the compared text.
        Some((
            output,
            output
                .checked_add(a)?
                .checked_add(aw)?
                .checked_add(b)?
                .checked_add(bw)?,
        ))
    }

    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        let left = operand(self.left, batch)?;
        let right = operand(self.right, batch)?;
        if [left.as_ref(), right.as_ref()].iter().any(|column| {
            column.len() != batch.num_rows()
                || !matches!(
                    column,
                    ColumnVector::Utf8 { .. } | ColumnVector::Null { .. }
                )
        }) {
            return None;
        }
        fn text_at(column: &ColumnVector, row: usize) -> Option<Option<&str>> {
            if column.is_null(row) {
                return Some(None);
            }
            let ColumnVector::Utf8 { data, offsets, .. } = column else {
                return None;
            };
            let bytes = data.get(*offsets.get(row)? as usize..*offsets.get(row + 1)? as usize)?;
            Some(Some(std::str::from_utf8(bytes).ok()?))
        }
        crate::engine::expr::with_compare_collation(self.collation_id, || {
            let mut data = Vec::with_capacity(batch.num_rows());
            let mut nulls = Bitmap::new_all_valid(batch.num_rows());
            for row in 0..batch.num_rows() {
                if row & 63 == 0 {
                    crate::engine::cancel::check_cancelled().ok()?;
                }
                let (a, b) = (text_at(&left, row)?, text_at(&right, row)?);
                let value = match (a, b) {
                    (Some(a), Some(b)) => {
                        let order = crate::engine::expr::compare_borrowed_strings(a, b)?;
                        Some(match self.op {
                            BinaryOp::Eq | BinaryOp::NullSafeEq => order.is_eq(),
                            BinaryOp::NotEq => !order.is_eq(),
                            BinaryOp::Lt => order.is_lt(),
                            BinaryOp::LtEq => !order.is_gt(),
                            BinaryOp::Gt => order.is_gt(),
                            BinaryOp::GtEq => !order.is_lt(),
                            _ => return None,
                        })
                    }
                    (a, b) if self.op == BinaryOp::NullSafeEq => Some(a.is_none() && b.is_none()),
                    _ => None,
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
        })
    }
}
