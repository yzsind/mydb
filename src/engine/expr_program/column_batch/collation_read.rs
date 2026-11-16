//! Composable text COLLATE/LIKE payload kernels. Descriptors remain bound
//! from the original AST; binary decoding and mixed coercions keep the oracle.
use super::*;

pub(super) struct CollationRead<'a> {
    value: &'a ExprNode,
    binary: bool,
}

impl<'a> CollationRead<'a> {
    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        let ExprNode::Function {
            kernel: FunctionKernel::LegacyDeterministic("COLLATE"),
            args,
            ..
        } = node
        else {
            return None;
        };
        let [value, ExprNode::Const(Value::String(target))] = args.as_slice() else {
            return None;
        };
        let info = crate::charset::resolve_collation(target).ok()?;
        let proof = super::contract(value, schema)?;
        if !proof.parallel || !matches!(proof.domain, Domain::Text) {
            return None;
        }
        Some(Self {
            value,
            binary: info.charset == "binary",
        })
    }

    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: if self.binary {
                Domain::Binary
            } else {
                Domain::Text
            },
            parallel: true,
        }
    }

    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let (output, work) = super::workspace(self.value, batch)?;
        Some((output, work.checked_add(output)?.checked_add(256)?))
    }

    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        let value = operand(self.value, batch)?;
        if !matches!(
            value.as_ref(),
            ColumnVector::Utf8 { .. } | ColumnVector::Null { .. }
        ) {
            return None;
        }
        let value = value.into_owned();
        match value {
            ColumnVector::Utf8 {
                offsets,
                data,
                nulls,
            } if self.binary => Some(ColumnVector::Bytes {
                offsets,
                data,
                nulls,
            }),
            value => Some(value),
        }
    }
}

pub(super) struct LikeRead<'a> {
    value: &'a ExprNode,
    plan: Option<&'a LikePlan>,
    pattern: Option<&'a ExprNode>,
    negated: bool,
    escape: Option<char>,
    collation: Option<u16>,
}

impl<'a> LikeRead<'a> {
    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        let result = match node {
            ExprNode::LikeConst {
                expr,
                plan,
                negated,
            } => Self {
                value: expr,
                plan: Some(plan),
                pattern: None,
                negated: *negated,
                escape: None,
                collation: None,
            },
            ExprNode::Like {
                expr,
                pattern,
                negated,
                escape_char,
                collation_id,
            } if matches!(
                pattern.as_ref(),
                ExprNode::Const(_) | ExprNode::Placeholder(_)
            ) =>
            {
                if !matches!(
                    constant(pattern)?.as_ref(),
                    Value::String(_) | Value::SmallString(_) | Value::Null
                ) {
                    return None;
                }
                Self {
                    value: expr,
                    plan: None,
                    pattern: Some(pattern),
                    negated: *negated,
                    escape: *escape_char,
                    collation: *collation_id,
                }
            }
            _ => return None,
        };
        let proof = super::contract(result.value, schema)?;
        (proof.parallel && matches!(proof.domain, Domain::Text | Domain::Null)).then_some(result)
    }

    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: Domain::Integer(0, 1),
            parallel: true,
        }
    }

    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let (child, work) = super::workspace(self.value, batch)?;
        let output = batch.num_rows().checked_mul(16)?.checked_add(256)?;
        Some((output, work.checked_add(child)?.checked_add(output)?))
    }

    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        let cached = if let Some(pattern) = self.pattern {
            let value = constant(pattern)?;
            if matches!(value.as_ref(), Value::Null) {
                return Some(ColumnVector::Null {
                    len: batch.num_rows(),
                });
            }
            let text = crate::types::value_str(value.as_ref())?;
            Some(crate::engine::func_cache::cached_like_plan(
                text,
                self.escape,
                self.collation,
            ))
        } else {
            None
        };
        let plan = self
            .plan
            .or_else(|| cached.as_deref().map(|plan| &**plan))?;
        let value = operand(self.value, batch)?;
        if value.len() != batch.num_rows()
            || !matches!(
                value.as_ref(),
                ColumnVector::Utf8 { .. } | ColumnVector::Null { .. }
            )
        {
            return None;
        }
        let mut data = Vec::with_capacity(batch.num_rows());
        let mut nulls = Bitmap::new_all_valid(batch.num_rows());
        let mut any_null = false;
        for row in 0..batch.num_rows() {
            if row & 63 == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            if value.is_null(row) {
                data.push(0);
                nulls.set(row, false);
                any_null = true;
                continue;
            }
            let ColumnVector::Utf8 {
                offsets,
                data: bytes,
                ..
            } = value.as_ref()
            else {
                return None;
            };
            let bytes = bytes.get(*offsets.get(row)? as usize..*offsets.get(row + 1)? as usize)?;
            let text = std::str::from_utf8(bytes).ok()?;
            data.push(i64::from(plan.matches_str(text) != self.negated));
        }
        Some(ColumnVector::I64 {
            data,
            nulls: any_null.then_some(nulls),
        })
    }
}
