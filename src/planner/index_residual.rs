//! Remove only predicates proved by the selected, exact B-tree bounds.
use super::*;

pub(crate) fn all_covered(
    expr: &Expr,
    table: &TableSchema,
    index: &IndexSchema,
    low: Option<&IndexBound>,
    high: Option<&IndexBound>,
) -> bool {
    if let Expr::Binary {
        left,
        op: BinaryOp::And,
        right,
    } = expr
    {
        return all_covered(left, table, index, low, high)
            && all_covered(right, table, index, low, high);
    }
    covered(expr, table, index, low, high)
}

pub(crate) fn lookup_covered(
    expr: &Expr,
    table: &TableSchema,
    index: &IndexSchema,
    key: &IndexKey,
) -> bool {
    let bound = IndexBound {
        key: key.clone(),
        inclusive: true,
    };
    all_covered(expr, table, index, Some(&bound), Some(&bound))
}

thread_local! {
    // Bit sets of lossless target domains, never concrete binding values.
    static PARAM_DOMAINS: std::cell::RefCell<Option<Vec<u8>>> = const { std::cell::RefCell::new(None) };
}

pub(super) struct ParamDomainsGuard(Option<Vec<u8>>);
impl Drop for ParamDomainsGuard {
    fn drop(&mut self) {
        PARAM_DOMAINS.with(|slot| {
            slot.replace(self.0.take());
        });
    }
}

fn target_bit(target: crate::storage::ParamTarget) -> u8 {
    use crate::storage::ParamTarget as T;
    match target {
        T::Int => 1,
        T::UInt => 2,
        T::String | T::CollatedString { .. } => 4,
        T::Date => 8,
        T::Time => 16,
        T::DateTime => 32,
        _ => 0,
    }
}

fn value_domains(value: &Value) -> u8 {
    match value {
        Value::Int(v) => {
            if *v >= 0 {
                3
            } else {
                1
            }
        }
        Value::UInt(v) => {
            if *v <= i64::MAX as u64 {
                3
            } else {
                2
            }
        }
        Value::String(_) => 4,
        Value::Date(_) => 8,
        Value::Time(_) => 16,
        Value::DateTime(_) => 32,
        _ => 0,
    }
}

pub(super) fn enter_param_domains(params: &[Value]) -> ParamDomainsGuard {
    ParamDomainsGuard(
        PARAM_DOMAINS.with(|slot| slot.replace(Some(params.iter().map(value_domains).collect()))),
    )
}

/// Compact identity of everything [`exact_param`] can observe about the
/// installed statement parameters: their count and lossless-domain bits.
/// Coverage proofs over a fixed plan are a pure function of this value, so
/// callers may memoize them under it. `None` when the domains are published
/// explicitly or the parameters do not fit the packed form.
pub(crate) fn param_domain_signature() -> Option<u64> {
    const BITS: u32 = 6;
    const MAX_PARAMS: usize = 9;
    if PARAM_DOMAINS.with(|slot| slot.borrow().is_some()) {
        return None;
    }
    crate::engine::params::with_current_params(|params, _| {
        let params = params?;
        if params.len() > MAX_PARAMS {
            return None;
        }
        let mut signature = params.len() as u64;
        for (index, value) in params.iter().enumerate() {
            signature |= u64::from(value_domains(value)) << (4 + BITS * index as u32);
        }
        Some(signature)
    })
}

// These parameter domains are part of the compiled-plan cache signature.
// Never prove exactness from a particular string's numeric contents.
fn exact_param(idx: usize, target: crate::storage::ParamTarget) -> bool {
    let bit = target_bit(target);
    let published = PARAM_DOMAINS.with(|slot| {
        slot.borrow()
            .as_ref()
            .and_then(|domains| domains.get(idx).copied())
    });
    let domains = published.or_else(|| {
        crate::engine::params::current_param(idx)
            .as_ref()
            .map(value_domains)
    });
    domains.is_some_and(|domains| domains & bit != 0)
}

pub(super) fn prune(plan: &mut PhysicalPlan) {
    walk_plan_mut(plan, &mut |node| match node {
        PhysicalPlan::IndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            ..
        }
        | PhysicalPlan::ParallelIndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            ..
        } => {
            prune_filter(&table.schema, index, low.as_ref(), high.as_ref(), filter);
        }
        PhysicalPlan::IndexLookup {
            covering: true,
            table,
            index,
            key,
            filter,
            ..
        } => {
            let bound = IndexBound {
                key: key.clone(),
                inclusive: true,
            };
            prune_filter(&table.schema, index, Some(&bound), Some(&bound), filter);
        }
        _ => {}
    });
}

fn prune_filter(
    table: &TableSchema,
    index: &IndexSchema,
    low: Option<&IndexBound>,
    high: Option<&IndexBound>,
    filter: &mut Option<Expr>,
) {
    // Partition routing still uses the original filter as pruning metadata.
    if table.is_partitioned() {
        return;
    }
    // Runtime boolean range refinement needs the equality prefix as well
    // as the OR. Keep that small predicate intact in reusable plans.
    if filter
        .as_ref()
        .is_some_and(|expr| expr_has_placeholder(expr) && boolean_range::has_range_or(expr))
    {
        return;
    }
    let Some(expr) = filter.take() else { return };
    let mut conjuncts = Vec::new();
    split_conjuncts(&expr, &mut conjuncts);
    conjuncts.retain(|expr| !covered(expr, table, index, low, high));
    *filter = combine_conjuncts(conjuncts);
}

fn covered(
    expr: &Expr,
    table: &TableSchema,
    index: &IndexSchema,
    low: Option<&IndexBound>,
    high: Option<&IndexBound>,
) -> bool {
    if let Expr::Between {
        expr,
        low: lo,
        high: hi,
        negated: false,
    } = expr
    {
        return covered(
            &Expr::Binary {
                left: expr.clone(),
                op: BinaryOp::GtEq,
                right: lo.clone(),
            },
            table,
            index,
            low,
            high,
        ) && covered(
            &Expr::Binary {
                left: expr.clone(),
                op: BinaryOp::LtEq,
                right: hi.clone(),
            },
            table,
            index,
            low,
            high,
        );
    }
    let Expr::Binary { left, op, right } = expr else {
        return false;
    };
    let (column, value, op) = if let Some(column) = expr_column_name(left) {
        (column, right.as_ref(), *op)
    } else if let Some(column) = expr_column_name(right) {
        (column, left.as_ref(), flip_cmp(*op))
    } else {
        return false;
    };
    if !matches!(
        op,
        BinaryOp::Eq | BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq
    ) {
        return false;
    }
    let Some(pos) = index
        .columns
        .iter()
        .position(|c| c.eq_ignore_ascii_case(column))
    else {
        return false;
    };
    if index.column_prefix_len(pos).is_some() || index.key_expression(pos).is_some() {
        return false;
    }
    let Ok(col) = table.column_index(column) else {
        return false;
    };
    let target = target_for_index_column(table, index, pos);
    let key = match value {
        Expr::Placeholder(idx) if exact_param(*idx, target) => KeyPart::Param { idx: *idx, target },
        _ => {
            let Some(value) = expr_literal_value(value) else {
                return false;
            };
            let Some(key) = exact_key(value, target) else {
                return false;
            };
            key
        }
    };
    // Later key parts are ordered only inside a fixed equality prefix.
    if pos > 0
        && !matches!((low, high), (Some(l), Some(h)) if l.key.0.len() > pos && h.key.0.len() > pos && l.key.0[..pos] == h.key.0[..pos])
    {
        return false;
    }
    let lower = low.filter(|b| b.key.0.len() > pos).map(|b| &b.key.0[pos]);
    let upper = high.filter(|b| b.key.0.len() > pos).map(|b| &b.key.0[pos]);
    // An upper bound alone can admit NULL index entries, whereas SQL
    // comparisons reject them. Equality/lower bounds exclude NULL.
    if table.columns[col].nullable
        && !lower.is_some_and(|k| match k {
            KeyPart::Null => false,
            KeyPart::Param { idx, target } => exact_param(*idx, *target),
            _ => true,
        })
    {
        return false;
    }
    let lower_ok = |strict: bool| {
        low.is_some_and(|b| {
            b.key.0.len() == pos + 1
                && ((b.key.0[pos] == key && (!strict || !b.inclusive))
                    || (!matches!(b.key.0[pos], KeyPart::Param { .. })
                        && !matches!(key, KeyPart::Param { .. })
                        && b.key.0[pos] > key))
        })
    };
    let upper_ok = |strict: bool| {
        high.is_some_and(|b| {
            b.key.0.len() == pos + 1
                && ((b.key.0[pos] == key && (!strict || !b.inclusive))
                    || (!matches!(b.key.0[pos], KeyPart::Param { .. })
                        && !matches!(key, KeyPart::Param { .. })
                        && b.key.0[pos] < key))
        })
    };
    match op {
        BinaryOp::Eq => lower == Some(&key) && upper == Some(&key),
        BinaryOp::Gt => lower_ok(true),
        BinaryOp::GtEq => lower_ok(false),
        BinaryOp::Lt => upper_ok(true),
        BinaryOp::LtEq => upper_ok(false),
        _ => false,
    }
}

/// Do not drop the semantic oracle for lossy literal casts.
/// In particular integer truncation, decimal rescaling and FLOAT rounding
/// can make a search key merely a candidate bound.
fn exact_key(value: Value, target: crate::storage::ParamTarget) -> Option<KeyPart> {
    use crate::storage::ParamTarget as T;
    if let T::Decimal { precision, scale } = target {
        let numeric = match &value {
            Value::String(text) => {
                Value::Decimal(crate::util::decimal::canonical_decimal_str(text)?)
            }
            _ => value.clone(),
        };
        numeric.exact_decimal()?;
        let key = KeyPart::for_target(value, target).ok()?;
        let restored = key.to_value(&DataTypeKind::Decimal {
            precision: Some(precision),
            scale: Some(scale),
        });
        return (crate::engine::expr::compare_numeric_for_sort(&numeric, &restored)
            == Some(std::cmp::Ordering::Equal))
        .then_some(key);
    }
    if matches!(target, T::Int | T::UInt | T::Float32 | T::Float64)
        && (value.exact_decimal().is_some() || matches!(value, Value::Float(_) | Value::Double(_)))
    {
        // Check the round trip instead of assuming a numeric cast is lossless.
        let key = KeyPart::for_target(value.clone(), target).ok()?;
        let restored = key.to_value(&DataTypeKind::Double {
            precision: None,
            scale: None,
        });
        if matches!(&restored, Value::Float(v) if !v.is_finite() || *v == 0.0)
            || matches!(&restored, Value::Double(v) if !v.is_finite() || *v == 0.0)
        {
            return None;
        }
        return (crate::engine::expr::compare_numeric_for_sort(&value, &restored)
            == Some(std::cmp::Ordering::Equal))
        .then_some(key);
    }
    if let Value::String(text) = &value {
        let data_type = match target {
            T::Date => Some(DataTypeKind::Date),
            T::Time => Some(DataTypeKind::Time { fsp: 0 }),
            T::DateTime => Some(DataTypeKind::DateTime { fsp: 0 }),
            _ => None,
        };
        if let Some(data_type) = data_type {
            let key = KeyPart::for_target(value.clone(), target).ok()?;
            return (key.to_value(&data_type).to_display_string() == *text).then_some(key);
        }
    }
    let exact = match (&value, target) {
        (Value::Int(_), T::Int)
        | (Value::UInt(_), T::UInt)
        | (Value::Date(_), T::Date)
        | (Value::Time(_), T::Time)
        | (Value::DateTime(_), T::DateTime)
        | (Value::Year(_), T::Year)
        | (Value::Bytes(_), T::Bytes)
        | (Value::String(_), T::String | T::CollatedString { .. }) => true,
        (Value::Float(v), T::Float32) => v.is_finite() && *v != 0.0,
        (Value::Double(v), T::Float64) => v.is_finite() && *v != 0.0,
        (Value::Int(v), T::UInt) => *v >= 0,
        (Value::UInt(v), T::Int) => *v <= i64::MAX as u64,
        (Value::String(v), T::Int) => v
            .parse::<i64>()
            .is_ok_and(|v| v.unsigned_abs() < (1u64 << 53)),
        (Value::String(v), T::UInt) => v.parse::<u64>().is_ok_and(|v| v < (1u64 << 53)),
        _ => false,
    };
    if !exact {
        return None;
    }
    KeyPart::for_target(value, target).ok()
}
