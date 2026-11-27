//! Borrow text slices at comparison boundaries; mixed domains keep Value coercion.
use super::*;

enum Operand<'a> {
    Value(ValueRef<'a>),
    Text(&'a str),
}

impl<'a> Operand<'a> {
    fn text(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text),
            Self::Value(value) => crate::types::value_str(value.as_value()),
        }
    }

    fn materialize(self) -> ValueRef<'a> {
        match self {
            Self::Value(value) => value,
            Self::Text(text) => ValueRef::Owned(Value::String(text.to_owned())),
        }
    }
}

// Admission reads only immutable leaves, before evaluating either operand.
// A refusal cannot duplicate warnings or side effects in the ordinary path.
fn substring_input<'a>(node: &'a ExprNode, row: &'a dyn RowSource) -> Option<&'a Value> {
    let input = match node {
        ExprNode::SubstringConst { expr, .. } => expr.as_ref(),
        ExprNode::Function {
            kernel: FunctionKernel::Substring,
            args,
            result_coercion: None,
            ..
        } if (2..=3).contains(&args.len()) => &args[0],
        _ => return None,
    };
    let value = match input {
        ExprNode::Column(index) => row.value_ref(*index)?,
        ExprNode::Const(value) => value,
        _ => return None,
    };
    (matches!(value, Value::Null) || crate::types::value_str(value).is_some()).then_some(value)
}

fn operand<'a>(
    node: &'a ExprNode,
    row: &'a dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Operand<'a>, QueryError> {
    let Some(input) = substring_input(node, row) else {
        return node.eval_ref(row, scratch).map(Operand::Value);
    };
    let Some(text) = crate::types::value_str(input) else {
        return Ok(Operand::Value(ValueRef::Borrowed(input)));
    };
    let (start, len) = match node {
        ExprNode::SubstringConst { start, len, .. } => (*start, *len),
        ExprNode::Function {
            args,
            collation_id,
            suppress_string_truncation,
            ..
        } => {
            let mut evaluate = || {
                crate::engine::expr::with_compare_collation(*collation_id, || {
                    let start = args[1].eval(row, scratch)?;
                    if matches!(start, Value::Null) {
                        return Ok(None);
                    }
                    let len = if args.len() == 3 {
                        let len = args[2].eval(row, scratch)?;
                        if matches!(len, Value::Null) {
                            return Ok(None);
                        }
                        Some(len)
                    } else {
                        None
                    };
                    Ok(Some((
                        crate::engine::expr::string_extent_arg(&start),
                        len.as_ref().map(crate::engine::expr::string_extent_arg),
                    )))
                })
            };
            let bounds = if *suppress_string_truncation {
                crate::engine::expr::with_suppressed_expression_warnings(evaluate)
            } else {
                evaluate()
            }?;
            let Some(bounds) = bounds else {
                return Ok(Operand::Value(ValueRef::Owned(Value::Null)));
            };
            bounds
        }
        _ => unreachable!(),
    };
    Ok(Operand::Text(crate::engine::expr::mysql_substring_slice(
        text, start, len,
    )))
}

// Keep comparison temporaries out of the shared arithmetic dispatcher's
// stack frame; mixed numeric arithmetic remains a separate hot path.
#[inline(never)]
pub(super) fn evaluate(
    left: &ExprNode,
    op: BinaryOp,
    right: &ExprNode,
    semantics: &BinarySemantics,
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    if substring_input(left, row).is_none() && substring_input(right, row).is_none() {
        let left = left.eval_ref(row, scratch)?;
        let right = right.eval_ref(row, scratch)?;
        return semantics.compare(left.as_value(), op, right.as_value());
    }
    {
        let left = operand(left, row, scratch)?;
        let right = operand(right, row, scratch)?;
        if let (Some(left), Some(right)) = (left.text(), right.text()) {
            if let Some(value) = semantics.compare_text(left, op, right) {
                return Ok(value);
            }
        }
        // Conversion is deferred until both operands have run. No expression
        // is re-evaluated when an enclosing coercion scope needs the oracle.
        semantics.compare(
            left.materialize().as_value(),
            op,
            right.materialize().as_value(),
        )
    }
}

#[cfg(test)]
mod tests;
