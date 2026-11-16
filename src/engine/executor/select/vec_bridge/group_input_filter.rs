//! Capture a proven group-key restriction in one aggregate's deferred input.
use super::*;
use crate::planner::RuntimeFilter;
#[cfg(test)]
pub(crate) static HITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
thread_local! {
    static FILTER: std::cell::RefCell<Option<(usize,RuntimeFilter)>> = const {std::cell::RefCell::new(None)};
}
pub(super) fn with_filter<T>(filter: Option<(usize, RuntimeFilter)>, run: impl FnOnce() -> T) -> T {
    struct Restore(Option<(usize, RuntimeFilter)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            FILTER.with(|f| *f.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(FILTER.with(|f| f.replace(filter)));
    run()
}
pub(super) fn current(plan: &PhysicalPlan) -> Option<RuntimeFilter> {
    FILTER.with(|f| {
        f.borrow()
            .as_ref()
            .filter(|(key, _)| *key == plan as *const PhysicalPlan as usize)
            .map(|(_, filter)| filter.clone())
    })
}
pub(super) fn target(plan: &PhysicalPlan, filter: &RuntimeFilter) -> Option<usize> {
    let (keys, null_safe) = match filter {
        RuntimeFilter::BloomKey {
            keys, null_safe, ..
        }
        | RuntimeFilter::ExactIntKey {
            keys, null_safe, ..
        }
        | RuntimeFilter::KeySet {
            keys, null_safe, ..
        } => (keys, null_safe),
    };
    if null_safe.iter().any(|safe| *safe) || keys.is_empty() {
        return None;
    }
    if let Some(aggregate) = plan.ordinary_aggregate_input() {
        if matches!(plan, PhysicalPlan::StreamingAggregate { .. }) {
            return None;
        }
        let template = plan_output_template(aggregate.input);
        for key in keys {
            let index = expr_column_index_in_template(key, &template)?;
            if !matches!(
                template[index].column_type,
                ColumnType::Tiny
                    | ColumnType::Short
                    | ColumnType::MediumInt
                    | ColumnType::Long
                    | ColumnType::LongLong
            ) || !aggregate
                .group_by
                .iter()
                .any(|group| expr_column_index_in_template(group, &template) == Some(index))
            {
                return None;
            }
        }
        return Some(plan as *const PhysicalPlan as usize);
    }
    match plan {
        PhysicalPlan::Projection { input, .. } | PhysicalPlan::Materialize { input, .. } => {
            target(input, filter)
        }
        _ => None,
    }
}

/// An integer subtuple from one relation is a necessary superset of the full
/// join domain. It can travel to the scan without depending on sibling keys.
pub(in crate::engine::executor) fn producer_keys(
    plan: &PhysicalPlan,
    expressions: &[Expr],
) -> Option<(Vec<usize>, Vec<Expr>)> {
    if let Some(aggregate) = plan.ordinary_aggregate_input() {
        if matches!(plan, PhysicalPlan::StreamingAggregate { .. }) {
            return None;
        }
        let template = plan_output_template(aggregate.input);
        let budget = mem_budget::current_budget()?;
        let force = cfg!(test)
            && crate::util::env::var("MYDB_GROUPED_CTE_DOMAIN").is_ok_and(|v| v == "force");
        let bytes_per_group = 64usize
            .saturating_add(aggregate.group_by.len().saturating_mul(32))
            .saturating_add(aggregate.aggregates.len().saturating_mul(32));
        if !force
            && crate::planner::estimated_rows(plan) * bytes_per_group as f64
                <= budget.limit().saturating_mul(2) as f64
        {
            return None;
        }
        let mut lanes: std::collections::BTreeMap<String, Vec<(usize, Expr)>> = Default::default();
        for (position, expr) in expressions.iter().enumerate() {
            let Some(index) = expr_column_index_in_template(expr, &template) else {
                continue;
            };
            if !matches!(
                template[index].column_type,
                ColumnType::Tiny
                    | ColumnType::Short
                    | ColumnType::MediumInt
                    | ColumnType::Long
                    | ColumnType::LongLong
            ) {
                continue;
            }
            if !aggregate
                .group_by
                .iter()
                .any(|g| expr_column_index_in_template(g, &template) == Some(index))
            {
                continue;
            }
            lanes
                .entry(template[index].table.to_ascii_lowercase())
                .or_default()
                .push((position, expr.clone()));
        }
        let selected = lanes
            .into_values()
            .filter(|v| v.len() >= 2)
            .max_by_key(Vec::len)?;
        return Some(selected.into_iter().unzip());
    }
    match plan {
        PhysicalPlan::Projection { input, .. } | PhysicalPlan::Materialize { input, .. } => {
            producer_keys(input, expressions)
        }
        _ => None,
    }
}
