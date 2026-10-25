//! N-ary RRF planning. Retrieval windows are never reduced by outer LIMIT.
use super::*;
use crate::parser::{Literal, QueryExpr, RrfFusion};

fn normalize(query: &mut QueryExpr) -> Result<(), QueryError> {
    match query {
        QueryExpr::WithCte(with) => normalize(&mut with.body),
        QueryExpr::Select(select) => {
            crate::engine::params::validate_limit(select.limit.as_ref())?;
            crate::engine::params::validate_limit(select.offset.as_ref())?;
            if limit_usize(select.offset.as_ref()).unwrap_or(0) != 0 {
                return Err(crate::error::catalog::ER_NOT_SUPPORTED_YET
                    .error("RRF branch OFFSET must be zero"));
            }
            for order in &mut select.order_by {
                let ordinal = match &order.expr {
                    Expr::Literal(Literal::Int(n)) if *n > 0 => Some(*n as usize),
                    Expr::Literal(Literal::UInt(n)) if *n > 0 => usize::try_from(*n).ok(),
                    _ => None,
                };
                order.expr = if let Some(ordinal) = ordinal {
                    match select.projection.get(ordinal - 1) {
                        Some(SelectItem::Expr { expr, .. }) => expr.clone(),
                        _ => {
                            return Err(crate::error::catalog::ER_BAD_FIELD_ERROR
                                .error("RRF ORDER BY position is outside the identity projection"))
                        }
                    }
                } else {
                    resolve_order_alias(&order.expr, &select.projection).clone()
                };
            }
            for item in &select.projection {
                if let SelectItem::Expr { expr, .. } = item {
                    if !select.order_by.iter().any(|order| order.expr == *expr) {
                        select.order_by.push(OrderBy {
                            expr: expr.clone(),
                            asc: true,
                            nulls_first: None,
                        });
                    }
                }
            }
            Ok(())
        }
        _ => Err(crate::error::catalog::ER_NOT_SUPPORTED_YET
            .error("RRF requires ordered SELECT branches")),
    }
}

fn input(db: &Database, query: &QueryExpr) -> Result<PhysicalPlan, QueryError> {
    match query {
        QueryExpr::Select(select) => Ok(plan_select(db, select)?.physical),
        QueryExpr::WithCte(with) => {
            let stubs = crate::engine::executor::cte::describe_definitions(&with.ctes, |query| {
                crate::engine::executor::select::describe_subquery_columns(db, query)
            })?;
            crate::engine::executor::cte::with_cte_registry_extended(stubs, || {
                input(db, &with.body)
            })
        }
        _ => Err(crate::error::catalog::ER_NOT_SUPPORTED_YET
            .error("RRF requires ordered SELECT branches")),
    }
}

pub(super) fn build(db: &Database, rrf: &RrfFusion) -> Result<PhysicalPlan, QueryError> {
    let columns = crate::engine::executor::select::hybrid::describe_columns(db, rrf)?;
    let mut spec = rrf.clone();
    for branch in &mut spec.branches {
        crate::parser::rrf::validate_branch(&branch.query)
            .map_err(|error| error.spec.error(error.message))?;
        normalize(&mut branch.query)?;
        if branch.query.leftmost_select().is_some_and(|select| {
            select.projection.iter().any(
                |item| matches!(item,SelectItem::Expr{expr,..} if !expr_is_deterministic(expr)),
            )
        }) {
            return Err(crate::error::catalog::ER_NOT_SUPPORTED_YET
                .error("RRF identity expressions must be repeatable"));
        }
        if spec.score_details
            && branch.query.leftmost_select().is_some_and(|select| {
                select
                    .order_by
                    .iter()
                    .any(|order| expr_has_volatile_evaluation(&order.expr))
            })
        {
            return Err(crate::error::catalog::ER_NOT_SUPPORTED_YET
                .error("RRF SCORE_DETAILS requires repeatable ordering expressions"));
        }
    }
    let mut inputs = spec
        .branches
        .iter()
        .map(|branch| input(db, &branch.query))
        .collect::<Result<Vec<_>, _>>()?;
    for input in &mut inputs {
        input.walk_mut(|node| {
            if let PhysicalPlan::VectorIndexScan { order_by, .. } = node {
                if order_by.len() == 1 {
                    order_by.push(OrderBy {
                        expr: Expr::Literal(Literal::Int(0)),
                        asc: true,
                        nulls_first: None,
                    });
                }
            }
        });
    }
    let rows = inputs.iter().map(estimated_rows).sum::<f64>();
    let mut cost = inputs
        .iter()
        .fold(crate::engine::cbo::Cost::scalar(0.0), |cost, plan| {
            cost.add(&plan_cost(plan))
        });
    cost.total += rows.max(1.0) * 0.02;
    cost.startup = cost.total;
    cost.cpu_units += rows;
    cost.mem_bytes = cost
        .mem_bytes
        .max(rows * (128.0 + columns.len() as f64 * 32.0));
    Ok(PhysicalPlan::RrfFusion {
        inputs,
        spec: Arc::new(spec),
        output_window: crate::engine::executor::select::hybrid::current_output_window(),
        columns,
        estimated_rows: rows,
        cost,
    })
}
