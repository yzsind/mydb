//! Column binding with explicit query, alias and window-frame scope boundaries.

use super::*;

pub(super) fn validate_expr_columns(
    db: &Database,
    expr: &Expr,
    scope: &ColumnScope<'_>,
    aliases: Option<&HashSet<String>>,
    output_only: bool,
) -> Result<(), QueryError> {
    crate::util::stack::with_stack(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        || validate_expr_columns_checked(db, expr, scope, aliases, output_only),
    )?
}

fn validate_expr_columns_checked(
    db: &Database,
    expr: &Expr,
    scope: &ColumnScope<'_>,
    aliases: Option<&HashSet<String>>,
    output_only: bool,
) -> Result<(), QueryError> {
    let validate = move |expr: &Expr| validate_expr_columns(db, expr, scope, aliases, output_only);
    match expr {
        Expr::Identifier(name) => {
            if aliases.is_some_and(|aliases| aliases.contains(&name.to_ascii_lowercase())) {
                Ok(())
            } else {
                scope.resolve_column(None, name, output_only)
            }
        }
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last().ok_or_else(|| {
                error(crate::error::catalog::ER_BAD_FIELD_ERROR, "unknown column")
            })?;
            let qualifier = if parts.len() >= 2 {
                Some(parts[parts.len() - 2].as_str())
            } else {
                None
            };
            scope.resolve_column(qualifier, column, output_only)
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => validate(expr),
        Expr::Binary { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            validate(left)?;
            validate(right)
        }
        Expr::InList { expr, list, .. }
        | Expr::QuantifiedComparison {
            left: expr,
            right: QuantifiedComparisonRight::List(list),
            ..
        } => {
            validate(expr)?;
            list.iter().try_for_each(validate)
        }
        Expr::InSubquery { expr, subquery, .. }
        | Expr::QuantifiedComparison {
            left: expr,
            right: QuantifiedComparisonRight::Subquery(subquery),
            ..
        } => {
            validate(expr)?;
            validate_select_column_references_inner(db, subquery, Some(scope))
        }
        Expr::Subquery(subquery) | Expr::Exists { subquery, .. } => {
            validate_select_column_references_inner(db, subquery, Some(scope))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate(expr)?;
            validate(low)?;
            validate(high)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            // Conditions precede results so multiple invalid references retain
            // the same first diagnostic as the planner's binding order.
            operand
                .iter()
                .map(Box::as_ref)
                .chain(conditions.iter())
                .chain(results.iter())
                .chain(else_result.iter().map(Box::as_ref))
                .try_for_each(validate)
        }
        Expr::Function { args, .. } => args.iter().try_for_each(validate),
        Expr::DefaultQualified(parts) => {
            let name = parts.last().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    "empty DEFAULT reference",
                )
            })?;
            scope.resolve_column(
                parts.iter().rev().nth(1).map(String::as_str),
                name,
                output_only,
            )
        }
        Expr::Default(Some(name)) => {
            if aliases.is_some_and(|aliases| aliases.contains(&name.to_ascii_lowercase())) {
                return Err(error(
                    crate::error::catalog::ER_NO_DEFAULT_FOR_FIELD,
                    format!("Field '{name}' doesn't have a default value"),
                ));
            }
            scope.resolve_column(None, name, output_only)
        }
        Expr::Default(None) => Ok(()),
        Expr::Grouping(_) => Ok(()),
        Expr::Tuple(items) => items.iter().try_for_each(validate),
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                frame,
                ..
            } = spec.as_ref();

            args.iter()
                .chain(partition_by.iter())
                .chain(order_by.iter().map(|item| &item.expr))
                .try_for_each(validate)?;
            if let Some(frame) = frame {
                validate_window_frame_columns(db, frame, scope)?;
            }
            Ok(())
        }
        Expr::Wildcard
        | Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Placeholder(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. } => Ok(()),
    }
}

pub(super) fn validate_window_frame_columns(
    db: &Database,
    frame: &crate::parser::WindowFrame,
    scope: &ColumnScope<'_>,
) -> Result<(), QueryError> {
    validate_window_frame_bound_columns(db, &frame.start, scope)?;
    validate_window_frame_bound_columns(db, &frame.end, scope)
}

fn validate_window_frame_bound_columns(
    db: &Database,
    bound: &crate::parser::WindowFrameBound,
    scope: &ColumnScope<'_>,
) -> Result<(), QueryError> {
    match bound {
        crate::parser::WindowFrameBound::Preceding(expr)
        | crate::parser::WindowFrameBound::Following(expr) => {
            validate_expr_columns(db, expr, scope, None, true)
        }
        crate::parser::WindowFrameBound::UnboundedPreceding
        | crate::parser::WindowFrameBound::UnboundedFollowing
        | crate::parser::WindowFrameBound::CurrentRow => Ok(()),
    }
}
