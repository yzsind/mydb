//! Index constraint representation and bound assembly shared by read and write planning.
//! Candidate costing, locking and residual coverage remain at the call sites.
use super::{expr_column_name, expr_literal_string, flip_cmp, normalize_expr_column};
use crate::parser::{BinaryOp, Expr};
use crate::storage::{IndexBound, IndexKey, IndexSchema, KeyPart, ParamTarget, TableSchema};
use crate::types::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(crate) enum ConstraintValue {
    Concrete(Value),
    Param(usize),
}
impl ConstraintValue {
    pub(crate) fn try_into_key_part(self, target: ParamTarget) -> Option<KeyPart> {
        match self {
            Self::Concrete(value) => KeyPart::for_target(value, target).ok(),
            Self::Param(idx) => Some(KeyPart::Param { idx, target }),
        }
    }
    // The read planner retains its existing failed-conversion sentinel. Write
    // candidate selection instead keeps an absent bound and a residual filter.
    pub(super) fn into_key_part(self, target: ParamTarget) -> KeyPart {
        self.try_into_key_part(target).unwrap_or(KeyPart::Null)
    }
}
#[derive(Debug, Clone)]
pub(crate) struct ColumnRange {
    pub(crate) low: Option<(ConstraintValue, bool)>,
    pub(crate) high: Option<(ConstraintValue, bool)>,
    pub(crate) like_prefix: Option<String>,
    pub(crate) like_param: Option<(usize, Option<char>)>,
}

pub(crate) fn target_for_index_column(
    table: &TableSchema,
    index: &IndexSchema,
    pos: usize,
) -> crate::storage::ParamTarget {
    // Functional key part: derive the param target from the
    // expression's inferred result type. Otherwise look the column up
    // in the table schema. Falling back to String when the column
    // can't be resolved keeps prepared-statement param binding
    // resilient even for unusual catalog states.
    if let Some(expr) = index.key_expression(pos) {
        if let Some(Expr::Function { name, args, .. }) = expr.ast.as_ref() {
            if (name.eq_ignore_ascii_case("CAST") || name.eq_ignore_ascii_case("CONVERT"))
                && args.len() >= 2
            {
                if let Some(Expr::Literal(crate::parser::Literal::String(target))) = args.last() {
                    let target = target.trim().to_ascii_uppercase();
                    if target.starts_with("UNSIGNED") {
                        return crate::storage::ParamTarget::UInt;
                    }
                    if target.starts_with("SIGNED") {
                        return crate::storage::ParamTarget::Int;
                    }
                    if target.starts_with("JSON") {
                        return crate::storage::ParamTarget::JsonScalar;
                    }
                }
            }
        }
        return crate::storage::ParamTarget::for_column(&expr.data_type, false);
    }
    let column_name = index.columns[pos].as_str();
    table
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(column_name))
        .map(|c| {
            crate::storage::ParamTarget::for_column_with_collation(
                &c.data_type,
                c.unsigned,
                c.collation.as_deref(),
            )
        })
        .unwrap_or(crate::storage::ParamTarget::String)
}

pub(crate) fn like_prefix_key_parts(
    index: &IndexSchema,
    pos: usize,
    target: crate::storage::ParamTarget,
    prefix: &str,
) -> Option<(KeyPart, Option<KeyPart>)> {
    let value = if matches!(target, crate::storage::ParamTarget::Bytes) {
        Value::Bytes(prefix.as_bytes().to_vec())
    } else {
        Value::String(prefix.to_string())
    };
    let low = index.apply_prefix_to_key_part(pos, KeyPart::for_target(value, target).ok()?);
    let high_value = match &low {
        KeyPart::Bytes(bytes) => {
            let mut next = bytes.clone();
            loop {
                let last = next.last_mut()?;
                if *last != u8::MAX {
                    *last += 1;
                    break Value::Bytes(next);
                }
                next.pop();
            }
        }
        KeyPart::CollatedString {
            collation_id,
            value,
        } => crate::engine::expr::like_prefix_successor_for_collation(
            value,
            Some(crate::charset::collation_name_for_id(*collation_id)),
        )?,
        KeyPart::String(value) => crate::engine::expr::like_prefix_successor(value)?,
        _ => return None,
    };
    let high = index.apply_prefix_to_key_part(pos, KeyPart::for_target(high_value, target).ok()?);
    Some((low, Some(high)))
}

pub(crate) fn like_pattern_param_target(
    target: crate::storage::ParamTarget,
    index: &IndexSchema,
    pos: usize,
    upper: bool,
    escape: Option<char>,
) -> Option<crate::storage::ParamTarget> {
    use crate::storage::ParamTarget;
    let (collation_id, binary, char_semantics) = match target {
        ParamTarget::Bytes => (None, true, false),
        ParamTarget::CharString => (None, false, true),
        ParamTarget::String => (None, false, false),
        ParamTarget::CollatedCharString { collation_id } => (Some(collation_id), false, true),
        ParamTarget::CollatedString { collation_id } => (Some(collation_id), false, false),
        _ => return None,
    };
    Some(ParamTarget::LikePattern {
        collation_id,
        binary,
        char_semantics,
        upper,
        escape,
        prefix_len: index.column_prefix_len(pos).map(|len| len as u32),
    })
}

pub(crate) fn record_range(
    ranges: &mut HashMap<String, ColumnRange>,
    column: &str,
    op: BinaryOp,
    value: ConstraintValue,
) {
    let range = ranges
        .entry(normalize_expr_column(column))
        .or_insert(ColumnRange {
            low: None,
            high: None,
            like_prefix: None,
            like_param: None,
        });
    match op {
        BinaryOp::Gt => range.low = Some((value, false)),
        BinaryOp::GtEq => range.low = Some((value, true)),
        BinaryOp::Lt => range.high = Some((value, false)),
        BinaryOp::LtEq => range.high = Some((value, true)),
        _ => {}
    }
}

/// NULL equality and LIKE eligibility are shared SQL rules. Extraction is supplied
/// by each entry so DML does not start folding session-dependent constants.
pub(crate) fn collect_index_constraints(
    table: &TableSchema,
    expr: &Expr,
    eq_values: &mut HashMap<String, ConstraintValue>,
    ranges: &mut HashMap<String, ColumnRange>,
    extract: &impl Fn(&Expr) -> Option<ConstraintValue>,
) {
    let extract_for = |column_expr: &Expr, value_expr: &Expr| {
        let value = extract(value_expr)?;
        let column = expr_column_name(column_expr).and_then(|name| table.column_index(name).ok());
        Some(match (value, column) {
            (ConstraintValue::Concrete(value), Some(idx)) => {
                ConstraintValue::Concrete(crate::engine::expr::contextual_numeric_literal(
                    value,
                    value_expr,
                    &table.columns[idx].data_type,
                ))
            }
            (value, _) => value,
        })
    };
    match expr {
        Expr::Binary { left, op, right } if *op == BinaryOp::And => {
            collect_index_constraints(table, left, eq_values, ranges, extract);
            collect_index_constraints(table, right, eq_values, ranges, extract);
        }
        Expr::Binary { left, op, right } if *op == BinaryOp::Eq => {
            if let (Some(column), Some(value)) = (expr_column_name(left), extract_for(left, right))
            {
                if !matches!(value, ConstraintValue::Concrete(Value::Null)) {
                    eq_values.insert(normalize_expr_column(column), value);
                }
            } else if let (Some(value), Some(column)) =
                (extract_for(right, left), expr_column_name(right))
            {
                if !matches!(value, ConstraintValue::Concrete(Value::Null)) {
                    eq_values.insert(normalize_expr_column(column), value);
                }
            }
        }
        Expr::IsNull(inner) => {
            if let Some(column) = expr_column_name(inner) {
                eq_values.insert(
                    normalize_expr_column(column),
                    ConstraintValue::Concrete(Value::Null),
                );
            }
        }
        Expr::Binary { left, op, right }
            if matches!(
                op,
                BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq
            ) =>
        {
            if let (Some(column), Some(value)) = (expr_column_name(left), extract_for(left, right))
            {
                record_range(ranges, column, *op, value);
            } else if let (Some(value), Some(column)) =
                (extract_for(right, left), expr_column_name(right))
            {
                record_range(ranges, column, flip_cmp(*op), value);
            }
        }
        Expr::Between {
            expr,
            negated: false,
            low,
            high,
        } => {
            if let (Some(column), Some(low), Some(high)) = (
                expr_column_name(expr),
                extract_for(expr, low),
                extract_for(expr, high),
            ) {
                record_range(ranges, column, BinaryOp::GtEq, low);
                record_range(ranges, column, BinaryOp::LtEq, high);
            }
        }
        Expr::Like {
            expr,
            pattern,
            negated: false,
            escape_char,
        } => {
            let Some(column) = expr_column_name(expr)
                .and_then(|name| table.column_index(name).ok())
                .map(|i| &table.columns[i])
            else {
                return;
            };
            if !matches!(
                column.data_type,
                crate::storage::DataTypeKind::Char { .. }
                    | crate::storage::DataTypeKind::VarChar { .. }
                    | crate::storage::DataTypeKind::Text { .. }
                    | crate::storage::DataTypeKind::Binary { .. }
                    | crate::storage::DataTypeKind::VarBinary { .. }
                    | crate::storage::DataTypeKind::Blob { .. }
            ) {
                return;
            }
            if let (Some(column), Expr::Placeholder(idx)) =
                (expr_column_name(expr), pattern.as_ref())
            {
                let range = ranges
                    .entry(normalize_expr_column(column))
                    .or_insert(ColumnRange {
                        low: None,
                        high: None,
                        like_prefix: None,
                        like_param: None,
                    });
                range.like_param = Some((*idx, *escape_char));
                return;
            }
            if let (Some(column), Some(pattern)) =
                (expr_column_name(expr), expr_literal_string(pattern))
            {
                // B12: pass ESCAPE through to the extractor.
                if let Some(shape) =
                    crate::engine::expr::analyze_like_sargable_pattern(&pattern, *escape_char)
                {
                    if let crate::engine::expr::LikeSargablePattern::Literal(literal) = shape {
                        eq_values.insert(
                            normalize_expr_column(column),
                            ConstraintValue::Concrete(Value::String(literal)),
                        );
                        return;
                    }
                    let crate::engine::expr::LikeSargablePattern::Prefix(prefix) = shape else {
                        unreachable!()
                    };
                    record_range(
                        ranges,
                        column,
                        BinaryOp::GtEq,
                        ConstraintValue::Concrete(Value::String(prefix.clone())),
                    );
                    let collation = table
                        .columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(column))
                        .and_then(|c| c.collation.as_deref());
                    if let Some(high) =
                        crate::engine::expr::like_prefix_successor_for_collation(&prefix, collation)
                    {
                        record_range(
                            ranges,
                            column,
                            BinaryOp::Lt,
                            ConstraintValue::Concrete(high),
                        );
                    }
                    if let Some(range) = ranges.get_mut(&normalize_expr_column(column)) {
                        range.like_prefix = Some(prefix);
                    }
                }
            }
        }
        _ => {}
    }
}

pub(crate) fn build_range_bounds(
    index: &IndexSchema,
    parts: &[KeyPart],
    range_pos: usize,
    range_target: ParamTarget,
    range: &ColumnRange,
    convert: impl Fn(ConstraintValue, ParamTarget) -> Option<KeyPart>,
) -> Option<(Option<IndexBound>, Option<IndexBound>)> {
    let mut low_parts = parts.to_vec();
    let mut high_parts = parts.to_vec();
    let prefix_parts = range
        .like_prefix
        .as_deref()
        .and_then(|prefix| like_prefix_key_parts(index, range_pos, range_target, prefix));
    if index.column_prefix_len(range_pos).is_some()
        && range.like_prefix.is_some()
        && prefix_parts.is_none()
    {
        return None;
    }
    let runtime_parts = range.like_param.and_then(|(idx, escape)| {
        Some((
            KeyPart::Param {
                idx,
                target: like_pattern_param_target(range_target, index, range_pos, false, escape)?,
            },
            KeyPart::Param {
                idx,
                target: like_pattern_param_target(range_target, index, range_pos, true, escape)?,
            },
        ))
    });
    if range.like_param.is_some() && runtime_parts.is_none() {
        return None;
    }
    let mut low = if let Some((low_part, _)) = &runtime_parts {
        low_parts.push(low_part.clone());
        Some(IndexBound {
            key: IndexKey::from(low_parts),
            inclusive: true,
        })
    } else if let Some((low_part, _)) = &prefix_parts {
        low_parts.push(low_part.clone());
        Some(IndexBound {
            key: IndexKey::from(low_parts),
            inclusive: true,
        })
    } else {
        range.low.as_ref().and_then(|(value, inclusive)| {
            low_parts.push(convert(value.clone(), range_target)?);
            Some(IndexBound {
                key: IndexKey::from(low_parts),
                // Rounding to a DECIMAL key may land on a qualifying row.
                // Include the rounded endpoint and retain the exact residual.
                inclusive: *inclusive
                    || matches!(
                        range_target,
                        ParamTarget::Decimal { .. } | ParamTarget::Date
                    ),
            })
        })
    };
    let mut high = if let Some((_, high_part)) = &runtime_parts {
        high_parts.push(high_part.clone());
        Some(IndexBound {
            key: IndexKey::from(high_parts),
            inclusive: false,
        })
    } else if let Some((_, Some(high_part))) = &prefix_parts {
        high_parts.push(high_part.clone());
        Some(IndexBound {
            key: IndexKey::from(high_parts),
            inclusive: false,
        })
    } else {
        range.high.as_ref().and_then(|(value, inclusive)| {
            high_parts.push(convert(value.clone(), range_target)?);
            Some(IndexBound {
                key: IndexKey::from(high_parts),
                // Rounding to a DECIMAL key may land on a qualifying row.
                // Include the rounded endpoint and retain the exact residual.
                inclusive: *inclusive
                    || matches!(
                        range_target,
                        ParamTarget::Decimal { .. } | ParamTarget::Date
                    ),
            })
        })
    };
    if !parts.is_empty() {
        let prefix = IndexBound {
            key: IndexKey::from(parts.to_vec()),
            inclusive: true,
        };
        if low.is_none() {
            low = Some(prefix.clone());
        }
        if high.is_none() {
            high = Some(prefix);
        }
    }
    Some((low, high))
}

/// Reject literal bounds whose SQL comparison domain differs from stored key
/// ordering. Deferred parameters keep their existing prepare/bind lifecycle.
#[inline]
pub(crate) fn concrete_comparison_preserved(target: ParamTarget, value: &ConstraintValue) -> bool {
    match (target, value) {
        (_, ConstraintValue::Param(_)) => true,
        (ParamTarget::Int, ConstraintValue::Concrete(Value::Int(_)))
        | (ParamTarget::UInt, ConstraintValue::Concrete(Value::UInt(_))) => true,
        _ => super::index_in_lookup_preserves_comparison(target, std::slice::from_ref(value)),
    }
}
