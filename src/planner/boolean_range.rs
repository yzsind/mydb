//! Safe B-tree envelopes for boolean scalar comparisons. AND intersects;
//! OR takes the convex hull (never an intersection). Residuals remain intact.
//! Work is bounded independently of generated SQL size; no global DNF product.
use super::*;
use std::cmp::Ordering;

const MAX_NODES: usize = 4096;
const MAX_BRANCHES: usize = 128;
const MAX_TERMS: usize = 4096;

type Bounds = (Option<IndexBound>, Option<IndexBound>);

/// Also used before auto-parameterization: equalities inside a range OR must
/// keep their literals so branch endpoints can be compared during planning.
pub(crate) fn has_range_or(expr: &Expr) -> bool {
    let mut stack = vec![(expr, false)];
    let mut visited = 0;
    let mut found = false;
    while let Some((expr, in_or)) = stack.pop() {
        visited += 1;
        if visited > MAX_NODES {
            return false;
        }
        if let Expr::Binary { left, op, right } = expr {
            if matches!(op, BinaryOp::And | BinaryOp::Or) {
                let in_or = in_or || *op == BinaryOp::Or;
                stack.push((left, in_or));
                stack.push((right, in_or));
            } else if in_or
                && matches!(
                    op,
                    BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq
                )
            {
                found = true;
            }
        }
    }
    found
}

/// Compare endpoints on the SAME side, including the implicit infinite
/// suffix of a prefix bound. Param IDs are identities, never ordered values.
fn cmp_bound(a: &IndexBound, b: &IndexBound, lower: bool) -> Option<Ordering> {
    for (a, b) in a.key.0.iter().zip(&b.key.0) {
        if a == b {
            continue;
        }
        if matches!(a, KeyPart::Param { .. }) || matches!(b, KeyPart::Param { .. }) {
            return None;
        }
        let cmp = a.cmp(b);
        if !cmp.is_eq() {
            return Some(cmp);
        }
    }
    if a.key.0.len() != b.key.0.len() {
        let (short, a_short) = if a.key.0.len() < b.key.0.len() {
            (a, true)
        } else {
            (b, false)
        };
        let short_first = short.inclusive == lower;
        return Some(if short_first == a_short {
            Ordering::Less
        } else {
            Ordering::Greater
        });
    }
    Some(if lower {
        b.inclusive.cmp(&a.inclusive)
    } else {
        a.inclusive.cmp(&b.inclusive)
    })
}

fn intersect_side(old: &mut Option<IndexBound>, next: Option<IndexBound>, lower: bool) {
    let Some(next) = next else { return };
    if old.as_ref().is_none_or(|old| {
        cmp_bound(&next, old, lower).is_some_and(|c| if lower { c.is_gt() } else { c.is_lt() })
    }) {
        *old = Some(next);
    }
}

fn hull_side(a: Option<IndexBound>, b: Option<IndexBound>, lower: bool) -> Option<IndexBound> {
    let (a, b) = (a?, b?);
    match cmp_bound(&a, &b, lower) {
        Some(c) => Some(if if lower { c.is_le() } else { c.is_ge() } {
            a
        } else {
            b
        }),
        None => {
            // Different live parameters have no stable order. Only retain
            // a shared symbolic prefix; initialization can refine it later.
            let parts = a
                .key
                .0
                .iter()
                .zip(&b.key.0)
                .take_while(|(a, b)| a == b)
                .map(|(a, _)| a.clone())
                .collect::<Vec<_>>();
            (!parts.is_empty()).then(|| IndexBound {
                key: IndexKey::from(parts),
                inclusive: true,
            })
        }
    }
}

fn conjunctions<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    let mut stack = vec![expr];
    while let Some(expr) = stack.pop() {
        if let Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } = expr
        {
            stack.push(right);
            stack.push(left);
        } else {
            out.push(expr);
        }
    }
}

/// Bounded expansion of ONE top-level conjunct. Independent lower/upper
/// ladders are handled separately, avoiding their Cartesian product.
fn branches<'a>(expr: &'a Expr, depth: usize, budget: &mut usize) -> Option<Vec<Vec<&'a Expr>>> {
    if depth > 128 || *budget == 0 {
        return None;
    }
    *budget -= 1;
    let Expr::Binary { left, op, right } = expr else {
        return Some(vec![vec![expr]]);
    };
    if !matches!(op, BinaryOp::And | BinaryOp::Or) {
        return Some(vec![vec![expr]]);
    }
    let lhs = branches(left, depth + 1, budget)?;
    let rhs = branches(right, depth + 1, budget)?;
    let count = if *op == BinaryOp::Or {
        lhs.len().checked_add(rhs.len())?
    } else {
        lhs.len().checked_mul(rhs.len())?
    };
    if count > MAX_BRANCHES {
        return None;
    }
    let terms = if *op == BinaryOp::Or {
        lhs.iter().chain(&rhs).map(Vec::len).sum::<usize>()
    } else {
        lhs.iter().map(Vec::len).sum::<usize>() * rhs.len()
            + rhs.iter().map(Vec::len).sum::<usize>() * lhs.len()
    };
    if terms > *budget || terms > MAX_TERMS {
        return None;
    }
    *budget -= terms;
    if *op == BinaryOp::Or {
        return Some(lhs.into_iter().chain(rhs).collect());
    }
    Some(
        lhs.iter()
            .flat_map(|a| {
                rhs.iter()
                    .map(move |b| a.iter().chain(b).copied().collect())
            })
            .collect(),
    )
}

fn part(
    table: &TableSchema,
    index: &IndexSchema,
    pos: usize,
    value: &Expr,
    runtime: bool,
) -> Option<KeyPart> {
    if index.column_prefix_len(pos).is_some() || !expr_is_deterministic(value) {
        return None;
    }
    // Each runtime value is checked afresh. No value-derived proof enters
    // the reusable plan, including decimal precision and parameter equality.
    let bound;
    let value = if runtime {
        if let Expr::Placeholder(i) = value {
            bound = Expr::BoundValue(crate::engine::params::current_param(*i)?);
            &bound
        } else {
            value
        }
    } else {
        value
    };
    let part = super::tuple_access::key_part(table, index, pos, value)
        .or_else(|| integer_text_part(table, index, pos, value))?;
    (!matches!(part, KeyPart::Null)).then_some(part)
}

fn integer_text_part(
    table: &TableSchema,
    index: &IndexSchema,
    pos: usize,
    value: &Expr,
) -> Option<KeyPart> {
    use crate::storage::ParamTarget;
    if expr_has_placeholder(value) {
        return None;
    }
    let target = target_for_index_column(table, index, pos);
    if !matches!(target, ParamTarget::Int | ParamTarget::UInt) {
        return None;
    }
    // Never sniff a parameter into a reusable bound. Runtime refinement
    // supplies BoundValue and validates the new binding on every execution.
    let PlanConstraintValue::Concrete(value) = expr_to_plan_constraint(value)? else {
        return None;
    };
    let text = match &value {
        Value::String(text) => text.as_str(),
        Value::SmallString(text) => std::str::from_utf8(text).ok()?,
        Value::Bytes(text) => std::str::from_utf8(text).ok()?,
        _ => return None,
    };
    let parsed = crate::util::decimal::parse_lenient_f64(text);
    let (magnitude, scale) = crate::util::decimal::parse_exact_decimal_str(text)?;
    let integer = crate::util::decimal::rescale_trunc_i128(magnitude, scale, 0)?;
    // Both exact/dynamic and f64/literal comparison domains must agree.
    // Beyond 2^53, several integer keys can compare equal to one text value.
    if !parsed.clean
        || integer.unsigned_abs() >= (1u128 << 53)
        || parsed.value != integer as f64
        || crate::util::decimal::rescale_trunc_i128(integer, 0, scale)? != magnitude
    {
        return None;
    }
    match target {
        ParamTarget::Int => Some(KeyPart::Int(i64::try_from(integer).ok()?)),
        ParamTarget::UInt => Some(KeyPart::UInt(u64::try_from(integer).ok()?)),
        _ => None,
    }
}

fn branch_bounds(
    table: &TableSchema,
    index: &IndexSchema,
    terms: &[&Expr],
    runtime: bool,
) -> Bounds {
    let mut prefix = Vec::new();
    for pos in 0..index.columns.len() {
        let mut eq = None;
        let mut low = None;
        let mut high = None;
        for term in terms {
            let Expr::Binary { left, op, right } = term else {
                continue;
            };
            if !matches!(
                op,
                BinaryOp::Eq | BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq
            ) {
                continue;
            }
            let (value, op) = if super::tuple_access::matches_part(index, pos, left) {
                (right.as_ref(), *op)
            } else if super::tuple_access::matches_part(index, pos, right) {
                (left.as_ref(), flip_cmp(*op))
            } else {
                continue;
            };
            let Some(part) = part(table, index, pos, value, runtime) else {
                continue;
            };
            if op == BinaryOp::Eq {
                eq = Some(part);
                continue;
            }
            let mut key = prefix.clone();
            key.push(part);
            let bound = Some(IndexBound {
                key: IndexKey::from(key),
                inclusive: matches!(op, BinaryOp::GtEq | BinaryOp::LtEq),
            });
            if matches!(op, BinaryOp::Gt | BinaryOp::GtEq) {
                intersect_side(&mut low, bound, true);
            } else {
                intersect_side(&mut high, bound, false);
            }
        }
        if let Some(eq) = eq {
            prefix.push(eq);
            continue;
        }
        let prefix_bound = (!prefix.is_empty()).then(|| IndexBound {
            key: IndexKey::from(prefix),
            inclusive: true,
        });
        return (low.or_else(|| prefix_bound.clone()), high.or(prefix_bound));
    }
    let bound = (!prefix.is_empty()).then(|| IndexBound {
        key: IndexKey::from(prefix),
        inclusive: true,
    });
    (bound.clone(), bound)
}

fn bounds(table: &TableSchema, index: &IndexSchema, expr: &Expr, runtime: bool) -> Option<Bounds> {
    if !has_range_or(expr)
        || index.has_multi_valued_parts()
        || !index.index_type.eq_ignore_ascii_case("BTREE")
    {
        return None;
    }
    let mut conjuncts = Vec::new();
    conjunctions(expr, &mut conjuncts);
    // Standalone equalities can supply an index prefix to every OR branch.
    let equalities = conjuncts
        .iter()
        .copied()
        .filter(|c| {
            matches!(
                c,
                Expr::Binary {
                    op: BinaryOp::Eq,
                    ..
                }
            )
        })
        .collect::<Vec<_>>();
    let mut result = (None, None);
    let mut budget = MAX_TERMS;
    for conjunct in conjuncts {
        let Some(branches) = branches(conjunct, 0, &mut budget) else {
            continue;
        };
        let prefix_work = branches.len().saturating_mul(equalities.len());
        if prefix_work > budget {
            continue;
        }
        budget -= prefix_work;
        let mut hull = None;
        for mut branch in branches {
            branch.extend(equalities.iter().copied());
            let next = branch_bounds(table, index, &branch, runtime);
            hull = Some(match hull {
                None => next,
                Some((low, high)) => (hull_side(low, next.0, true), hull_side(high, next.1, false)),
            });
        }
        if let Some((low, high)) = hull {
            intersect_side(&mut result.0, low, true);
            intersect_side(&mut result.1, high, false);
        }
    }
    (result.0.is_some() || result.1.is_some()).then_some(result)
}

pub(super) fn candidate(
    table: &TableSchema,
    index: &IndexSchema,
    expr: &Expr,
) -> Option<IndexAccess> {
    let (low, high) = bounds(table, index, expr, false)?;
    Some(IndexAccess {
        index: index.clone(),
        kind: IndexAccessKind::Range { low, high },
    })
}

/// Refine only the query-local cursor bounds; the complete OR residual stays
/// in place. Safe for cached plans, forward/reverse scans and MVCC repair.
pub(crate) fn refine_runtime(
    table: &TableSchema,
    index: &IndexSchema,
    filter: Option<&Expr>,
    low: &mut Option<IndexBound>,
    high: &mut Option<IndexBound>,
) {
    let Some(filter) = filter.filter(|f| expr_has_placeholder(f)) else {
        return;
    };
    let Some((new_low, new_high)) = bounds(table, index, filter, true) else {
        return;
    };
    for (old, next, lower) in [(low, new_low, true), (high, new_high, false)] {
        if let (Some(old_bound), Some(next_bound)) = (old.as_ref(), next.as_ref()) {
            if cmp_bound(next_bound, old_bound, lower).is_none() {
                // Both bounds independently contain every qualifying row.
                // Prefer the freshly checked concrete bound over an opaque
                // symbolic one; do not pre-resolve LIKE/NULL parameter rules.
                *old = next;
                continue;
            }
        }
        intersect_side(old, next, lower);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boolean_range_prefix_endpoint_order_matches_membership() {
        let mut bounds = Vec::new();
        for a in -1..=1 {
            for inclusive in [false, true] {
                bounds.push(IndexBound {
                    key: IndexKey::from(vec![KeyPart::Int(a)]),
                    inclusive,
                });
                for b in -1..=1 {
                    bounds.push(IndexBound {
                        key: IndexKey::from(vec![KeyPart::Int(a), KeyPart::Int(b)]),
                        inclusive,
                    });
                }
            }
        }
        let allows = |bound: &IndexBound, row: &[i64; 3], lower| {
            let cmp = row
                .iter()
                .zip(&bound.key.0)
                .map(|(v, k)| KeyPart::Int(*v).cmp(k))
                .find(|c| !c.is_eq())
                .unwrap_or(Ordering::Equal);
            if lower {
                cmp.is_gt() || cmp.is_eq() && bound.inclusive
            } else {
                cmp.is_lt() || cmp.is_eq() && bound.inclusive
            }
        };
        for lower in [false, true] {
            for a in &bounds {
                for b in &bounds {
                    let cmp = cmp_bound(a, b, lower).unwrap();
                    let tighter = if lower { cmp.is_ge() } else { cmp.is_le() };
                    for x in -2..=2 {
                        for y in -2..=2 {
                            for z in -2..=2 {
                                let row = [x, y, z];
                                if tighter {
                                    assert!(
                                        !allows(a, &row, lower) || allows(b, &row, lower),
                                        "{a:?} {b:?}"
                                    );
                                } else {
                                    assert!(
                                        !allows(b, &row, lower) || allows(a, &row, lower),
                                        "{a:?} {b:?}"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn boolean_range_expansion_budget_and_parameter_hull() {
        let parse = |sql: &str| {
            let Statement::Select(select) = crate::parser::parse_statement(sql).unwrap() else {
                panic!()
            };
            select.selection.unwrap()
        };
        let terms = (0..10)
            .map(|i| format!("(a>{i} OR b>{i})"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let expr = parse(&format!("SELECT 1 FROM t WHERE ({terms}) OR a=1"));
        assert!(has_range_or(&expr));
        let mut budget = MAX_TERMS;
        assert!(branches(&expr, 0, &mut budget).is_none());
        let param = |i| IndexBound {
            key: IndexKey::from(vec![
                KeyPart::Int(1),
                KeyPart::Param {
                    idx: i,
                    target: crate::storage::ParamTarget::Int,
                },
            ]),
            inclusive: false,
        };
        for lower in [true, false] {
            let bound = hull_side(Some(param(0)), Some(param(1)), lower).unwrap();
            assert_eq!(bound.key, IndexKey::from(vec![KeyPart::Int(1)]));
            assert!(bound.inclusive);
            assert!(cmp_bound(&param(0), &param(1), lower).is_none());
        }
    }
}
