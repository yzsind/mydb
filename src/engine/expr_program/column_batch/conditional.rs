//! Value-only conditional projection. Potential diagnostics and mixed coercion
//! domains retain row-wise short-circuit evaluation.
use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Family {
    Null,
    Signed,
    Unsigned,
    Text,
}
enum Choice<'a> {
    If(&'a ExprNode, &'a ExprNode, &'a ExprNode),
    Case {
        operand: Option<&'a ExprNode>,
        conditions: &'a [ExprNode],
        results: &'a [ExprNode],
        otherwise: Option<&'a ExprNode>,
    },
}
pub(super) struct Conditional<'a> {
    choice: Choice<'a>,
    family: Family,
}
impl<'a> Conditional<'a> {
    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        let (choice, coercion) = match node {
            ExprNode::Function {
                kernel: FunctionKernel::If,
                args,
                result_coercion,
                ..
            } if args.len() == 3 => (Choice::If(&args[0], &args[1], &args[2]), result_coercion),
            ExprNode::Case {
                operand,
                conditions,
                results,
                else_result,
                result_coercion,
            } => (
                Choice::Case {
                    operand: operand.as_deref(),
                    conditions,
                    results,
                    otherwise: else_result.as_deref(),
                },
                result_coercion,
            ),
            _ => return None,
        };
        let numeric = |node: &ExprNode| {
            let proof = super::contract(node, schema)?;
            (proof.parallel
                && matches!(
                    proof.domain,
                    Domain::Integer(..) | Domain::Unsigned | Domain::Null
                ))
            .then_some(())
        };
        let mut family = Family::Null;
        let mut branch = |node: &ExprNode| -> Option<()> {
            let proof = super::contract(node, schema)?;
            if !proof.parallel {
                return None;
            }
            if let ExprNode::Column(i) = node {
                if schema.fields().get(*i)?.source.flags
                    & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
                    != 0
                {
                    return None;
                }
            }
            let next = match proof.domain {
                Domain::Null => return Some(()),
                Domain::Integer(..) => Family::Signed,
                Domain::Unsigned => Family::Unsigned,
                Domain::Text => Family::Text,
                _ => return None,
            };
            if family != Family::Null && family != next {
                return None;
            }
            family = next;
            Some(())
        };
        match &choice {
            Choice::If(condition, a, b) => {
                numeric(condition)?;
                branch(a)?;
                branch(b)?;
            }
            Choice::Case {
                operand,
                conditions,
                results,
                otherwise,
            } => {
                if conditions.len() != results.len() {
                    return None;
                }
                if let Some(operand) = operand {
                    numeric(operand)?;
                }
                for condition in *conditions {
                    numeric(condition)?;
                }
                for result in *results {
                    branch(result)?;
                }
                if let Some(otherwise) = otherwise {
                    branch(otherwise)?;
                }
            }
        }
        if let Some(coercion) = coercion {
            use crate::engine::expr_type::{ExprTypeKind, StringKind};
            let compatible = match family {
                Family::Null => true,
                Family::Signed => matches!(
                    coercion.kind,
                    ExprTypeKind::Integer {
                        unsigned: false,
                        ..
                    }
                ),
                Family::Unsigned => {
                    matches!(coercion.kind, ExprTypeKind::Integer { unsigned: true, .. })
                }
                Family::Text => matches!(
                    coercion.kind,
                    ExprTypeKind::String {
                        kind: StringKind::Character | StringKind::Text
                    }
                ),
            };
            if !compatible {
                return None;
            }
        }
        Some(Self { choice, family })
    }
    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: match self.family {
                Family::Null => Domain::Null,
                Family::Signed => Domain::Integer(i64::MIN, i64::MAX),
                Family::Unsigned => Domain::Unsigned,
                Family::Text => Domain::Text,
            },
            parallel: true,
        }
    }
    fn nodes(&self) -> Vec<&'a ExprNode> {
        match &self.choice {
            Choice::If(c, a, b) => vec![*c, *a, *b],
            Choice::Case {
                operand,
                conditions,
                results,
                otherwise,
            } => operand
                .iter()
                .copied()
                .chain(conditions.iter())
                .chain(results.iter())
                .chain(otherwise.iter().copied())
                .collect(),
        }
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let mut payload = batch.num_rows().checked_mul(16)?.checked_add(8)?;
        let mut work = 0usize;
        for node in self.nodes() {
            let (bytes, child) = super::workspace(node, batch)?;
            payload = payload.max(bytes);
            work = work.checked_add(child)?.checked_add(bytes)?;
        }
        Some((
            payload,
            work.checked_add(payload.checked_mul(2)?)?
                .checked_add(1024)?,
        ))
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        if self.family == Family::Null {
            return Some(ColumnVector::Null {
                len: batch.num_rows(),
            });
        }
        let (cases, results, otherwise, base) = match &self.choice {
            Choice::If(c, a, b) => (
                vec![operand(c, batch)?],
                vec![operand(a, batch)?],
                Some(operand(b, batch)?),
                None,
            ),
            Choice::Case {
                operand: base,
                conditions,
                results,
                otherwise,
            } => (
                conditions
                    .iter()
                    .map(|node| operand(node, batch))
                    .collect::<Option<Vec<_>>>()?,
                results
                    .iter()
                    .map(|node| operand(node, batch))
                    .collect::<Option<Vec<_>>>()?,
                match otherwise {
                    Some(node) => Some(operand(node, batch)?),
                    None => None,
                },
                match base {
                    Some(node) => Some(operand(node, batch)?),
                    None => None,
                },
            ),
        };
        if cases.iter().chain(base.iter()).any(|column| {
            !matches!(
                column.as_ref(),
                ColumnVector::I64 { .. }
                    | ColumnVector::U64 { .. }
                    | ColumnVector::Bool { .. }
                    | ColumnVector::Null { .. }
            )
        }) {
            return None;
        }
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
        let mut signed = Vec::new();
        let mut unsigned = Vec::new();
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        if self.family == Family::Text {
            offsets.reserve(batch.num_rows() + 1);
            offsets.push(0);
        }
        let mut nulls = Bitmap::new_all_valid(batch.num_rows());
        for row in 0..batch.num_rows() {
            if row & 63 == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            let chosen = cases.iter().position(|condition| {
                let condition = numeric(condition, row);
                if let Some(base) = &base {
                    let base = numeric(base, row);
                    base.is_some() && base == condition
                } else {
                    condition.is_some_and(|n| n != 0)
                }
            });
            let result = chosen
                .map(|i| results[i].as_ref())
                .or_else(|| otherwise.as_deref());
            let valid = result.is_some_and(|column| !column.is_null(row));
            if !valid {
                nulls.set(row, false);
            }
            match self.family {
                Family::Signed => signed.push(if valid {
                    let ColumnVector::I64 { data, .. } = result? else {
                        return None;
                    };
                    data[row]
                } else {
                    0
                }),
                Family::Unsigned => unsigned.push(if valid {
                    let ColumnVector::U64 { data, .. } = result? else {
                        return None;
                    };
                    data[row]
                } else {
                    0
                }),
                Family::Text => {
                    if valid {
                        data.extend_from_slice(bytes_at(result?, row)?);
                    }
                    offsets.push(u32::try_from(data.len()).ok()?);
                }
                Family::Null => unreachable!(),
            }
        }
        Some(match self.family {
            Family::Signed => ColumnVector::I64 {
                data: signed,
                nulls: Some(nulls),
            },
            Family::Unsigned => ColumnVector::U64 {
                data: unsigned,
                nulls: Some(nulls),
            },
            Family::Text => ColumnVector::Utf8 {
                data,
                offsets,
                nulls: Some(nulls),
            },
            Family::Null => unreachable!(),
        })
    }
}
