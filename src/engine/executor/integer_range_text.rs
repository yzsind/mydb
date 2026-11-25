//! Parser-proven text templates for bounded ordered integer primary ranges.
//! These templates are execution vehicles, never permission or row caches.
use super::*;

pub(super) fn is_template(statement: &Statement) -> bool {
    let Statement::Select(select) = statement else {
        return false;
    };
    if select.from.len() != 1
        || !select.joins.is_empty()
        || select.derived.is_some()
        || select.distinct
        || !select.group_by.is_empty()
        || select.having.is_some()
        || select.order_by.len() != 1
        || !select.projection.iter().all(|item| {
            matches!(
                item,
                SelectItem::Expr {
                    expr: Expr::Identifier(_) | Expr::CompoundIdentifier(_),
                    ..
                }
            )
        })
    {
        return false;
    }
    matches!(select.selection.as_ref(), Some(Expr::Binary { left, op: BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq, right })
        if matches!(left.as_ref(),Expr::Placeholder(_)) || matches!(right.as_ref(),Expr::Placeholder(_)))
}

fn integer(expr: &Expr, params: &[Value]) -> Option<Value> {
    match expr {
        Expr::Literal(crate::parser::Literal::Int(n)) => Some(Value::Int(*n)),
        Expr::Literal(crate::parser::Literal::UInt(n)) => Some(Value::UInt(*n)),
        Expr::Placeholder(i) => match params.get(*i)? {
            Value::Int(_) | Value::UInt(_) => Some(params[*i].clone()),
            _ => None,
        },
        _ => None,
    }
}

fn count(value: &crate::parser::LimitValue, params: &[Value]) -> Option<usize> {
    match value {
        crate::parser::LimitValue::Rows(n) => Some(*n),
        crate::parser::LimitValue::Placeholder(i)
        | crate::parser::LimitValue::OffsetCommaPlaceholder(i) => match params.get(*i)? {
            Value::UInt(n) => usize::try_from(*n).ok(),
            Value::Int(n) => usize::try_from(*n).ok(),
            _ => None,
        },
    }
}

pub(super) fn candidate(
    db: &Database,
    session: &SessionContext,
    statement: &Statement,
    params: &[Value],
) -> bool {
    let Statement::Select(select) = statement else {
        return false;
    };
    if select.from.len() != 1
        || !select.joins.is_empty()
        || select.derived.is_some()
        || select.distinct
        || select.sql_calc_found_rows
        || !select.group_by.is_empty()
        || select.having.is_some()
        || select.for_update
        || select.lock_clause.is_some()
        || select.order_by.len() != 1
        || !select.query_hints.is_empty()
        || statement_refs_temp_table(session, statement)
    {
        return false;
    }
    let source = &select.from[0];
    if source.system_time.is_some()
        || source.sample.is_some()
        || !source.partitions.is_empty()
        || !source.index_hints.is_empty()
    {
        return false;
    }
    let Some(limit) = select.limit.as_ref().and_then(|v| count(v, params)) else {
        return false;
    };
    let offset = match &select.offset {
        None => 0,
        Some(v) => match count(v, params) {
            Some(n) => n,
            None => return false,
        },
    };
    if limit == 0 || limit.saturating_add(offset) > select::simple::COMPACT_ORDERED_WINDOW_ROWS {
        return false;
    }
    let Some(name) = select.table.as_deref() else {
        return false;
    };
    let (database, name) = split_table_name(&session.current_database, name);
    if is_system_database(&database) {
        return false;
    }
    let Ok(table) = db.table_in_database(&database, &name) else {
        return false;
    };
    let Some(primary) = table
        .iter_queryable_indexes()
        .find(|i| i.primary && i.columns.len() == 1)
    else {
        return false;
    };
    if !select::simple::primary_row_goal_cost_is_stable(&table, primary)
        || db.row_security_may_be_configured(table.object_id)
    {
        return false;
    }
    let Ok(pk) = table.column_index(&primary.columns[0]) else {
        return false;
    };
    if !table.columns[pk].data_type.is_integer() {
        return false;
    }
    let column = |expr: &Expr| {
        parameterized_update_column_name(expr).and_then(|name| table.column_index(name).ok())
    };
    if column(&select.order_by[0].expr) != Some(pk) {
        return false;
    }
    let Some(Expr::Binary {
        left,
        op: BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq,
        right,
    }) = select.selection.as_ref()
    else {
        return false;
    };
    if !((column(left) == Some(pk) && integer(right, params).is_some())
        || (column(right) == Some(pk) && integer(left, params).is_some()))
    {
        return false;
    }
    let mut projected = Vec::new();
    for item in &select.projection {
        let SelectItem::Expr { expr, .. } = item else {
            return false;
        };
        let Some(index) = column(expr) else {
            return false;
        };
        if projected.contains(&index) || table.columns[index].generated_expr_ast.is_some() {
            return false;
        }
        projected.push(index);
    }
    !projected.is_empty()
}

/// Recreate literal provenance before the ordinary planner if a live catalog,
/// policy, session or execution mode cannot use the bounded prepared vehicle.
pub(super) fn restore(statement: &Statement, params: &[Value]) -> Option<Statement> {
    if !is_template(statement) {
        return None;
    }
    let Statement::Select(mut select) = statement.clone() else {
        return None;
    };
    let mut valid = true;
    crate::parser::visit::rewrite_value_expr(select.selection.as_mut()?, |expr| {
        if let Expr::Placeholder(i) = expr {
            *expr = match params.get(*i) {
                Some(Value::Int(n)) => Expr::Literal(crate::parser::Literal::Int(*n)),
                Some(Value::UInt(n)) => Expr::Literal(crate::parser::Literal::UInt(*n)),
                _ => {
                    valid = false;
                    return true;
                }
            };
        }
        true
    });
    for limit in [&mut select.limit, &mut select.offset] {
        if let Some(value) = limit.as_ref() {
            *limit = Some(crate::parser::LimitValue::Rows(count(value, params)?));
        }
    }
    valid.then_some(Statement::Select(select))
}
