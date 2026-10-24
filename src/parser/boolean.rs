//! Ownership-preserving boolean AST operations shared by query rewrites.
use super::{BinaryOp, Expr};

/// Flatten AND in source order, transferring nodes without cloning them.
pub(crate) fn split_conjuncts_owned(expr: Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            split_conjuncts_owned(*left, out);
            split_conjuncts_owned(*right, out);
        }
        other => out.push(other),
    }
}

/// Preserve source evaluation order without making rewrite output linear-depth.
pub(crate) fn combine_conjuncts_left(parts: Vec<Expr>) -> Option<Expr> {
    balanced_boolean_chain(parts, BinaryOp::And)
}

pub(crate) fn balanced_boolean_chain(mut parts: Vec<Expr>, op: BinaryOp) -> Option<Expr> {
    if parts.len() <= 2 {
        let right = parts.pop()?;
        return Some(match parts.pop() {
            Some(left) => Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            None => right,
        });
    }
    while parts.len() > 1 {
        let mut level = Vec::with_capacity(parts.len().div_ceil(2));
        let mut iter = parts.into_iter();
        while let Some(left) = iter.next() {
            level.push(match iter.next() {
                Some(right) => Expr::Binary {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                },
                None => left,
            });
        }
        parts = level;
    }
    parts.pop()
}

/// Reverse an ordering comparison when swapping its operands.
#[inline]
pub(crate) fn flip_cmp(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}

/// Borrow AND conjuncts in source order without cloning expression nodes.
pub(crate) fn collect_expr_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            collect_expr_conjuncts(left, out);
            collect_expr_conjuncts(right, out);
        }
        other => out.push(other),
    }
}

/// Borrow OR disjuncts in source order without cloning expression nodes.
pub(crate) fn collect_expr_disjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            collect_expr_disjuncts(left, out);
            collect_expr_disjuncts(right, out);
        }
        other => out.push(other),
    }
}

/// Shared comparison classification expands into the caller's hot match.
macro_rules! is_comparison {
    ($op:expr) => {
        matches!(
            $op,
            $crate::parser::BinaryOp::Eq
                | $crate::parser::BinaryOp::NullSafeEq
                | $crate::parser::BinaryOp::NotEq
                | $crate::parser::BinaryOp::Lt
                | $crate::parser::BinaryOp::LtEq
                | $crate::parser::BinaryOp::Gt
                | $crate::parser::BinaryOp::GtEq
        )
    };
}
pub(crate) use is_comparison;
