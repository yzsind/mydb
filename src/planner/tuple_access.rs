//! Row-constructor access paths. Every candidate is a superset; the original
//! expression remains the residual. Never expand independent IN domains.
use super::*;

pub(super) fn matches_part(index: &IndexSchema, pos: usize, expr: &Expr) -> bool {
    match index.key_part(pos) {
        Some(crate::storage::IndexKeyPart::Column(name)) => {
            expr_column_name(expr).is_some_and(|column| column.eq_ignore_ascii_case(name))
        }
        Some(crate::storage::IndexKeyPart::Expression(part)) => part
            .ast
            .as_ref()
            .is_some_and(|ast| expression_fingerprint(ast) == expression_fingerprint(expr)),
        _ => false,
    }
}

/// Lossless conversion is essential for later tuple components: rounding a
/// component changes which suffix is compared. A residual cannot recover rows
/// that such an incorrectly narrowed bound excluded.
pub(super) fn key_part(
    table: &TableSchema,
    index: &IndexSchema,
    pos: usize,
    value: &Expr,
) -> Option<KeyPart> {
    // Nested parameter expressions must stay executable expressions; folding
    // CAST(?) or DATE_ADD(?) here would bake the first binding into a plan.
    if !matches!(value, Expr::Placeholder(_)) && expr_has_placeholder(value) {
        return None;
    }
    let constraint = expr_to_plan_constraint(value)?;
    let target = target_for_index_column(table, index, pos);
    if !write_in_lookup_preserves_comparison(target, std::slice::from_ref(&constraint)) {
        return None;
    }
    let runtime = match &constraint {
        PlanConstraintValue::Concrete(v) => v.clone(),
        PlanConstraintValue::Param(i) => crate::engine::params::current_param(*i)?,
    };
    if let PlanConstraintValue::Param(idx) = constraint {
        // Cache keys distinguish signedness/sign and native temporal lanes.
        // Accept only conversions safe for EVERY value in that class. Float,
        // decimal and textual temporal bindings may round or overflow, so
        // those keep a shorter prefix instead of sniffing the first value.
        use crate::storage::ParamTarget as T;
        let safe = match (target, &runtime) {
            (T::Int, Value::Int(_)) | (T::UInt, Value::UInt(_)) => true,
            (T::UInt, Value::Int(v)) => *v >= 0,
            (T::Int, Value::UInt(v)) => *v <= i64::MAX as u64,
            (
                T::String
                | T::CharString
                | T::CollatedString { .. }
                | T::CollatedCharString { .. }
                | T::Bytes,
                Value::String(_) | Value::SmallString(_) | Value::Bytes(_),
            ) => true,
            (T::Date, Value::Date(_))
            | (T::Time, Value::Time(_))
            | (T::DateTime, Value::DateTime(_))
            | (T::Timestamp, Value::Timestamp(_)) => true,
            _ => false,
        };
        return safe.then_some(KeyPart::Param { idx, target });
    }
    let part = KeyPart::for_target(runtime.clone(), target).ok()?;
    if matches!(part, KeyPart::Null) && !matches!(runtime, Value::Null) {
        return None;
    }
    if !matches!(runtime, Value::Null) {
        let data_type = match index.key_part(pos)? {
            crate::storage::IndexKeyPart::Column(name) => {
                &table
                    .columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(name))?
                    .data_type
            }
            crate::storage::IndexKeyPart::Expression(part) => &part.data_type,
        };
        let converted = part.to_value(data_type);
        let collation = match target {
            crate::storage::ParamTarget::CollatedString { collation_id }
            | crate::storage::ParamTarget::CollatedCharString { collation_id } => {
                Some(collation_id)
            }
            _ => None,
        };
        if !crate::engine::expr::with_compare_collation(collation, || {
            crate::engine::expr::compare_values(&runtime, &converted)
                .is_ok_and(|order| order == std::cmp::Ordering::Equal)
        }) {
            return None;
        }
    }
    Some(match constraint {
        PlanConstraintValue::Param(idx) => KeyPart::Param { idx, target },
        _ => index.apply_prefix_to_key_part(pos, part),
    })
}

fn equality_part(
    table: &TableSchema,
    index: &IndexSchema,
    eq: &HashMap<String, PlanConstraintValue>,
    pos: usize,
) -> Option<KeyPart> {
    let crate::storage::IndexKeyPart::Column(name) = index.key_part(pos)? else {
        return None;
    };
    let expr = match eq.get(&normalize_expr_column(name))? {
        PlanConstraintValue::Concrete(v) => Expr::BoundValue(v.clone()),
        PlanConstraintValue::Param(i) => Expr::Placeholder(*i),
    };
    key_part(table, index, pos, &expr)
}

fn equality_prefix(
    table: &TableSchema,
    index: &IndexSchema,
    eq: &HashMap<String, PlanConstraintValue>,
    end: usize,
) -> Option<Vec<KeyPart>> {
    (0..end)
        .map(|pos| equality_part(table, index, eq, pos))
        .collect()
}

fn point_candidate(
    table: &TableSchema,
    index: &IndexSchema,
    lhs: &[Expr],
    rows: &[&Expr],
    eq: &HashMap<String, PlanConstraintValue>,
) -> Option<IndexAccess> {
    if lhs.is_empty() || rows.is_empty() {
        return None;
    }
    let tuples = rows
        .iter()
        .map(|row| match row {
            Expr::Tuple(items) if items.len() == lhs.len() => Some(items),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    let mut keys = vec![Vec::new(); rows.len()];
    let mut used_tuple = false;
    for pos in 0..index.columns.len() {
        let tuple_pos = lhs.iter().position(|expr| matches_part(index, pos, expr));
        let parts = if let Some(tuple_pos) = tuple_pos {
            tuples
                .iter()
                .map(|row| key_part(table, index, pos, &row[tuple_pos]))
                .collect::<Option<Vec<_>>>()
        } else {
            equality_part(table, index, eq, pos).map(|part| vec![part; rows.len()])
        };
        let Some(parts) = parts else { break };
        used_tuple |= tuple_pos.is_some();
        for (key, part) in keys.iter_mut().zip(parts) {
            key.push(part);
        }
    }
    if !used_tuple || keys[0].is_empty() {
        return None;
    }
    let len = keys[0].len();
    let exact = len == index.columns.len()
        && index.key_parts_are_exact(len)
        && !keys.iter().flatten().any(|p| matches!(p, KeyPart::Null));
    let mut keys = keys.into_iter().map(IndexKey::from).collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();
    Some(IndexAccess {
        index: index.clone(),
        kind: IndexAccessKind::MultiLookup { keys, exact },
    })
}

fn range_candidate(
    table: &TableSchema,
    index: &IndexSchema,
    lhs: &[Expr],
    rhs: &Expr,
    op: BinaryOp,
    eq: &HashMap<String, PlanConstraintValue>,
) -> Option<IndexAccess> {
    let Expr::Tuple(values) = rhs else {
        return None;
    };
    if lhs.is_empty() || lhs.len() != values.len() {
        return None;
    }
    let start = (0..index.columns.len()).find(|&pos| matches_part(index, pos, &lhs[0]))?;
    let prefix = equality_prefix(table, index, eq, start)?;
    // Prefix indexes cannot order full values, even when the truncated stored
    // prefix is equality-constrained before the row range.
    if (0..start).any(|pos| index.column_prefix_len(pos).is_some()) {
        return None;
    }
    let mut parts = prefix.clone();
    for (offset, (expr, value)) in lhs.iter().zip(values).enumerate() {
        let pos = start + offset;
        if !matches_part(index, pos, expr) || index.column_prefix_len(pos).is_some() {
            break;
        }
        let Some(part) = key_part(table, index, pos, value) else {
            break;
        };
        // SQL NULL does not decide a lexicographic comparison. Retaining an
        // inclusive shorter bound is a safe superset for the deciding prefix.
        if matches!(part, KeyPart::Null) {
            break;
        }
        parts.push(part);
    }
    if parts.len() == start {
        return None;
    }
    let inclusive =
        parts.len() < start + lhs.len() || matches!(op, BinaryOp::LtEq | BinaryOp::GtEq);
    let bound = Some(IndexBound {
        key: IndexKey::from(parts),
        inclusive,
    });
    let prefix_bound = (!prefix.is_empty()).then(|| IndexBound {
        key: IndexKey::from(prefix),
        inclusive: true,
    });
    let (low, high) = if matches!(op, BinaryOp::Gt | BinaryOp::GtEq) {
        (bound, prefix_bound)
    } else {
        (prefix_bound, bound)
    };
    Some(IndexAccess {
        index: index.clone(),
        kind: IndexAccessKind::Range { low, high },
    })
}

fn has_row_predicate(expr: &Expr) -> bool {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => has_row_predicate(left) || has_row_predicate(right),
        Expr::Binary { left, right, .. } => {
            matches!(left.as_ref(), Expr::Tuple(_)) || matches!(right.as_ref(), Expr::Tuple(_))
        }
        Expr::InList { expr, .. } | Expr::Between { expr, .. } => {
            matches!(expr.as_ref(), Expr::Tuple(_))
        }
        _ => false,
    }
}

pub(super) fn candidates(
    table: &TableSchema,
    expr: &Expr,
    eq: &HashMap<String, PlanConstraintValue>,
) -> Vec<IndexAccess> {
    if !has_row_predicate(expr) {
        return Vec::new();
    }
    let mut conjuncts = Vec::new();
    flatten_and(expr, &mut conjuncts);
    let mut out = Vec::new();
    for index in table.iter_queryable_indexes() {
        let mut ranges = Vec::new();
        for conjunct in &conjuncts {
            match conjunct {
                Expr::InList {
                    expr,
                    list,
                    negated: false,
                } => {
                    if let Expr::Tuple(lhs) = expr.as_ref() {
                        if let Some(candidate) =
                            point_candidate(table, index, lhs, &list.iter().collect::<Vec<_>>(), eq)
                        {
                            out.push(candidate);
                        }
                    }
                }
                Expr::Binary { left, op, right }
                    if matches!(
                        op,
                        BinaryOp::Eq
                            | BinaryOp::NullSafeEq
                            | BinaryOp::Gt
                            | BinaryOp::GtEq
                            | BinaryOp::Lt
                            | BinaryOp::LtEq
                    ) =>
                {
                    for (lhs, rhs, op) in [
                        (left.as_ref(), right.as_ref(), *op),
                        (right.as_ref(), left.as_ref(), flip_cmp(*op)),
                    ] {
                        let Expr::Tuple(lhs) = lhs else { continue };
                        if matches!(op, BinaryOp::Eq | BinaryOp::NullSafeEq) {
                            if let Some(candidate) = point_candidate(table, index, lhs, &[rhs], eq)
                            {
                                out.push(candidate);
                            }
                        } else if let Some(candidate) =
                            range_candidate(table, index, lhs, rhs, op, eq)
                        {
                            ranges.push(candidate);
                        }
                    }
                }
                Expr::Between {
                    expr,
                    low,
                    high,
                    negated: false,
                } => {
                    if let Expr::Tuple(lhs) = expr.as_ref() {
                        for (rhs, op) in [
                            (low.as_ref(), BinaryOp::GtEq),
                            (high.as_ref(), BinaryOp::LtEq),
                        ] {
                            if let Some(candidate) = range_candidate(table, index, lhs, rhs, op, eq)
                            {
                                ranges.push(candidate);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        // A lower and upper bound can be paired without ordering their
        // values. For competing bounds on the SAME side, compare only
        // concrete keys; retain the first parameter bound as a safe superset.
        // This preserves both endpoints of parameterized BETWEEN.
        let mut merged_low: Option<IndexBound> = None;
        let mut merged_high: Option<IndexBound> = None;
        for candidate in &ranges {
            let IndexAccessKind::Range { low, high } = &candidate.kind else {
                unreachable!()
            };
            for (merged, next, lower) in [
                (&mut merged_low, low, true),
                (&mut merged_high, high, false),
            ] {
                let Some(next) = next else { continue };
                match merged {
                    None => *merged = Some(next.clone()),
                    Some(old)
                        if old.inclusive
                            && old.key.0.len() < next.key.0.len()
                            && old.key.0.iter().zip(&next.key.0).all(|(a, b)| a == b) =>
                    {
                        *old = next.clone();
                    }
                    Some(old)
                        if old.key.0.len() == next.key.0.len()
                            && old
                                .key
                                .0
                                .iter()
                                .chain(&next.key.0)
                                .all(|p| !matches!(p, KeyPart::Param { .. })) =>
                    {
                        let cmp = next.key.cmp(&old.key);
                        if (lower && cmp.is_gt()) || (!lower && cmp.is_lt()) {
                            *old = next.clone();
                        } else if cmp.is_eq() {
                            old.inclusive &= next.inclusive;
                        }
                    }
                    _ => {}
                }
            }
        }
        if merged_low.is_some() || merged_high.is_some() {
            out.push(IndexAccess {
                index: index.clone(),
                kind: IndexAccessKind::Range {
                    low: merged_low,
                    high: merged_high,
                },
            });
        }
        out.extend(ranges);
    }
    out
}

/// Single-source predicates have already been resolved to this table before
/// index matching. Their spelling (column vs alias.column) is not semantic.
pub(super) fn expression_fingerprint(expr: &Expr) -> u64 {
    crate::engine::expr_program::unqualified_expr_fingerprint(expr)
}
