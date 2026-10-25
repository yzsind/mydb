//! Proof boundaries for safe outer retrieval windows.
use super::*;

/// An enclosing, proven row-preserving ORDER BY/LIMIT. Keep lexical bindings
/// so a cached physical node cannot retain a previous execution's row goal.
#[derive(Debug, Clone)]
pub struct OutputWindow {
    pub limit: crate::parser::LimitValue,
    pub offset: Option<crate::parser::LimitValue>,
}
thread_local! {static OUTPUT_WINDOW:std::cell::RefCell<Option<OutputWindow>>=const {std::cell::RefCell::new(None)};}
pub(in crate::engine::executor::select) struct WindowScope(Option<OutputWindow>);
impl Drop for WindowScope {
    fn drop(&mut self) {
        OUTPUT_WINDOW.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}
pub(crate) fn current_output_window() -> Option<OutputWindow> {
    OUTPUT_WINDOW.with(|slot| slot.borrow().clone())
}
fn enter_window(window: Option<OutputWindow>) -> WindowScope {
    WindowScope(OUTPUT_WINDOW.with(|slot| slot.replace(window)))
}

fn transparent(select: &Select) -> bool {
    select.joins.is_empty()
        && select.from.len() == 1
        && select.selection.is_none()
        && select.having.is_none()
        && select.group_by.is_empty()
        && !select.distinct
        && !select.sql_calc_found_rows
        && !select.for_update
        && select.lock_clause.is_none()
        && select.projection.iter().all(|item| {
            matches!(
                item,
                SelectItem::Wildcard
                    | SelectItem::QualifiedWildcard(_)
                    | SelectItem::Expr {
                        expr: Expr::Identifier(_) | Expr::CompoundIdentifier(_),
                        ..
                    }
            )
        })
}
fn source_keys(db: &Database, query: &QueryExpr) -> Result<Option<Vec<String>>, QueryError> {
    match query {
        QueryExpr::Rrf(spec) => Ok(Some(
            describe_columns(db, spec)?
                .into_iter()
                .take(
                    spec.branches[0]
                        .query
                        .leftmost_select()
                        .map(|select| select.projection.len())
                        .unwrap_or(0),
                )
                .map(|column| column.name)
                .collect(),
        )),
        QueryExpr::Select(select)
            if transparent(select)
                && select.limit.is_none()
                && select.offset.is_none()
                && select.order_by.is_empty()
                && select.projection.len() == 1
                && matches!(select.projection[0], SelectItem::Wildcard) =>
        {
            match &select.derived {
                Some(derived) => source_keys(db, derived),
                None => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

pub(in crate::engine::executor::select) fn window_for_select(
    db: &Database,
    select: &Select,
) -> Result<WindowScope, QueryError> {
    if !transparent(select) {
        return Ok(enter_window(None));
    }
    let Some(source) = select.derived.as_ref() else {
        return Ok(enter_window(None));
    };
    let Some(keys) = source_keys(db, source)? else {
        return Ok(enter_window(None));
    };
    if select.limit.is_none()
        && select.offset.is_none()
        && select.order_by.is_empty()
        && select.projection.len() == 1
        && matches!(select.projection[0], SelectItem::Wildcard)
    {
        return Ok(enter_window(current_output_window()));
    }
    let Some(limit) = select.limit.as_ref() else {
        return Ok(enter_window(None));
    };
    crate::engine::params::validate_limit(Some(limit))?;
    crate::engine::params::validate_limit(select.offset.as_ref())?;
    if super::super::limit_value_or_zero(Some(limit))? == 0 {
        return Ok(enter_window(Some(OutputWindow {
            limit: limit.clone(),
            offset: select.offset.clone(),
        })));
    }
    let expected = std::iter::once(("rrf_score".to_string(), false))
        .chain(std::iter::once(("best_rank".to_string(), true)))
        .chain(keys.into_iter().map(|name| (name, true)))
        .collect::<Vec<_>>();
    if select.order_by.is_empty() || select.order_by.len() > expected.len() {
        return Ok(enter_window(None));
    }
    for (order, (expected_name, ascending)) in select.order_by.iter().zip(&expected) {
        let name = match &order.expr {
            Expr::Identifier(name) => Some(name.as_str()),
            Expr::CompoundIdentifier(parts) => parts.last().map(String::as_str),
            _ => None,
        };
        if order.asc != *ascending
            || name.is_none_or(|name| !name.eq_ignore_ascii_case(expected_name))
        {
            return Ok(enter_window(None));
        }
    }
    Ok(enter_window(Some(OutputWindow {
        limit: limit.clone(),
        offset: select.offset.clone(),
    })))
}

pub(crate) fn window_json(window: &Option<OutputWindow>) -> serde_json::Value {
    fn value(limit: &crate::parser::LimitValue) -> serde_json::Value {
        match limit {
            crate::parser::LimitValue::Rows(rows) => serde_json::json!(rows),
            crate::parser::LimitValue::Placeholder(index)
            | crate::parser::LimitValue::OffsetCommaPlaceholder(index) => {
                serde_json::json!({"parameter":index})
            }
        }
    }
    window.as_ref().map(|window|serde_json::json!({"limit":value(&window.limit),"offset":window.offset.as_ref().map(value).unwrap_or_else(||serde_json::json!(0))})).unwrap_or(serde_json::Value::Null)
}
