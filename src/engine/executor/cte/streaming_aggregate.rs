//! Fuse a single aggregate consumer with a native set-operation producer.
//! No partial AVG rewrite: the canonical accumulator sees the original rows.
use super::*;
use crate::planner::PhysicalPlan;

#[cfg(test)]
pub(crate) static HITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn scalar_argument(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_)
        | Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Placeholder(_)
        | Expr::Wildcard => true,
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            scalar_argument(expr)
        }
        Expr::Binary { left, right, .. } => scalar_argument(left) && scalar_argument(right),
        Expr::Function { args, .. } => args.iter().all(scalar_argument),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_deref().is_none_or(scalar_argument)
                && conditions.iter().chain(results).all(scalar_argument)
                && else_result.as_deref().is_none_or(scalar_argument)
        }
        _ => false,
    }
}

fn prepared_source<'q>(statement: impl Into<crate::parser::QueryRef<'q>>) -> bool {
    let statement = statement.into();
    match statement {
        crate::parser::QueryRef::Select(select) => {
            select.derived.is_none()
                && select.joins.iter().all(|join| join.derived.is_none())
                && !select.for_update
                && !select.sql_calc_found_rows
                && select.lock_clause.is_none()
        }
        crate::parser::QueryRef::SetOperation(set) => {
            prepared_source(&set.left) && prepared_source(&set.right)
        }
        _ => false,
    }
}

pub(super) fn try_materialize<'q>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: impl Into<crate::parser::QueryRef<'q>>,
    cte: &CteDef,
) -> Result<Option<CteResult>, QueryError> {
    let statement = statement.into();
    let crate::parser::QueryRef::Select(select) = statement else {
        return Ok(None);
    };
    let Some(crate::parser::SubQuery::SetOp(source)) = &select.derived else {
        return Ok(None);
    };
    if select.from.len() != 1
        || !select.joins.is_empty()
        || select.selection.is_some()
        || !select.group_by.is_empty()
        || !select.grouping_sets.is_empty()
        || select.group_by_modifier != crate::parser::GroupByModifier::None
        || select.having.is_some()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.distinct
        || select.for_update
        || select.lock_clause.is_some()
        || select.sql_calc_found_rows
        || !select.query_hints.is_empty()
        || select_has_nondeterministic(select)
        || !prepared_source(source)
        || !statement_is_deterministic_for_cte_cache(source)
    {
        return Ok(None);
    }
    // Unqualified arguments bind directly to the set operation's output
    // names. Other scopes stay on the existing materialization path.
    if select.projection.is_empty() || !select.projection.iter().all(|item| {
        matches!(item, SelectItem::Expr { expr: Expr::Function { name, args, .. }, .. }
            if matches!(name.to_ascii_uppercase().as_str(), "SUM" | "AVG" | "COUNT"
                | "STDDEV_POP" | "STDDEV_SAMP" | "VAR_POP" | "VAR_SAMP" | "BIT_AND" | "BIT_OR" | "BIT_XOR")
                && args.iter().all(scalar_argument))
    }) { return Ok(None) }
    let Some(input) = crate::planner::plan_query_physical(db, source)? else {
        return Ok(None);
    };
    let input_cost = crate::planner::plan_cost(&input);
    let cost = crate::engine::cbo::operator_cost::hash_aggregate(
        &crate::engine::cbo::CboContext::from_db(db),
        &input_cost,
        crate::planner::estimated_rows(&input),
        1.0,
        select.projection.len() as f64 * 32.0,
    );
    let mut plan = PhysicalPlan::Projection {
        input: Box::new(PhysicalPlan::HashAggregate {
            input: Box::new(input),
            group_by: Vec::new(),
            having: None,
            aggregates: crate::planner::collect_aggregate_calls(select),
            estimated_rows: 1.0,
            cost: cost.clone(),
        }),
        projection: select.projection.clone(),
        distinct: false,
        estimated_rows: 1.0,
        cost,
    };
    crate::planner::parallelize_fused_aggregate(&mut plan);
    if !super::super::select::vec_bridge::preflight_query_output_supported(&plan)? {
        return Ok(None);
    }
    let txn = cte_statement_view(db, session);
    let budget = super::super::mem_budget::current_budget()
        .unwrap_or_else(|| Arc::new(super::super::mem_budget::MemBudget::from_env()));
    let stream = with_cte_materialization(|| {
        let context =
            super::super::select::execution_context::ExecutionContext::capture_with_budget(
                txn, budget,
            );
        super::super::select::vec_bridge::try_stream_native_plan(
            Arc::clone(db),
            txn,
            Arc::new(plan),
            context,
        )
    })?;
    let Some(stream) = stream else {
        return Ok(None);
    };
    #[cfg(test)]
    HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    route_response_to_cte_result(db, QueryResponse::StreamingResultSet(stream), cte).map(Some)
}
