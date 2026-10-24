//! Borrowed-to-owned scalar rewriting without cloning the tree before walking it.

use super::*;

/// Replace a node with `Some` and leave the replacement opaque; `None` maps
/// its scalar children. Query and window nodes retain their binding scopes.
/// CASE conditions precede results. Quantified lists precede their left
/// operand, matching the existing owned binders' first-error ordering.
pub(crate) fn try_map_value_expr<E>(
    expr: &Expr,
    rewrite: &mut impl FnMut(&Expr) -> Result<Option<Expr>, E>,
) -> Result<Expr, E> {
    if let Some(replacement) = rewrite(expr)? {
        return Ok(replacement);
    }
    let map_box = |expr: &Expr, rewrite: &mut _| try_map_value_expr(expr, rewrite).map(Box::new);
    let map_list = |items: &[Expr], rewrite: &mut _| {
        items
            .iter()
            .map(|expr| try_map_value_expr(expr, rewrite))
            .collect::<Result<Vec<_>, E>>()
    };
    Ok(match expr {
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: map_box(expr, rewrite)?,
        },
        Expr::Binary { left, op, right } => Expr::Binary {
            left: map_box(left, rewrite)?,
            op: *op,
            right: map_box(right, rewrite)?,
        },
        Expr::IsNull(expr) => Expr::IsNull(map_box(expr, rewrite)?),
        Expr::IsNotNull(expr) => Expr::IsNotNull(map_box(expr, rewrite)?),
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: map_box(expr, rewrite)?,
            list: map_list(list, rewrite)?,
            negated: *negated,
        },
        Expr::QuantifiedComparison {
            left,
            op,
            quantifier,
            right,
        } => {
            let right = match right {
                QuantifiedComparisonRight::Subquery(query) => {
                    QuantifiedComparisonRight::Subquery(query.clone())
                }
                QuantifiedComparisonRight::List(list) => {
                    QuantifiedComparisonRight::List(map_list(list, rewrite)?)
                }
            };
            Expr::QuantifiedComparison {
                left: map_box(left, rewrite)?,
                op: *op,
                quantifier: *quantifier,
                right,
            }
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: map_box(expr, rewrite)?,
            low: map_box(low, rewrite)?,
            high: map_box(high, rewrite)?,
            negated: *negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
        } => Expr::Like {
            expr: map_box(expr, rewrite)?,
            pattern: map_box(pattern, rewrite)?,
            negated: *negated,
            escape_char: *escape_char,
        },
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => Expr::Case {
            operand: operand
                .as_deref()
                .map(|expr| map_box(expr, rewrite))
                .transpose()?,
            conditions: map_list(conditions, rewrite)?.into_boxed_slice(),
            results: map_list(results, rewrite)?,
            else_result: else_result
                .as_deref()
                .map(|expr| map_box(expr, rewrite))
                .transpose()?,
        },
        Expr::Function {
            name,
            args,
            distinct,
        } => Expr::Function {
            name: name.clone(),
            args: map_list(args, rewrite)?,
            distinct: *distinct,
        },
        Expr::GroupKeyRef { index, expr } => Expr::GroupKeyRef {
            index: *index,
            expr: map_box(expr, rewrite)?,
        },
        Expr::Tuple(items) => Expr::Tuple(map_list(items, rewrite)?),
        Expr::WindowFunction { .. }
        | Expr::InSubquery { .. }
        | Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::Wildcard
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. }
        | Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Placeholder(_)
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Grouping(_) => expr.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn projection(sql: &str) -> Expr {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            panic!("expected SELECT");
        };
        let SelectItem::Expr { expr, .. } = select.projection.into_iter().next().unwrap() else {
            panic!("expected expression");
        };
        expr
    }

    #[test]
    fn owned_mapping_preserves_case_diagnostic_order() {
        let expr =
            projection("SELECT CASE operand WHEN c1 THEN r1 WHEN c2 THEN r2 ELSE fallback END");
        let mut visited = Vec::new();
        let mapped = try_map_value_expr(&expr, &mut |node| {
            if let Expr::Identifier(name) = node {
                visited.push(name.clone());
                if name == "c2" {
                    return Err("second condition");
                }
            }
            Ok(None)
        });
        assert_eq!(mapped, Err("second condition"));
        assert_eq!(visited, ["operand", "c1", "c2"]);
    }

    #[test]
    fn owned_mapping_keeps_nested_query_and_window_bindings_opaque() {
        for sql in [
            "SELECT outer_col + (SELECT nested_col)",
            "SELECT outer_col IN (SELECT nested_col)",
            "SELECT SUM(nested_col) OVER (ORDER BY ordering_col)",
        ] {
            let expr = projection(sql);
            let mut visited = Vec::new();
            let mapped: Result<_, ()> = try_map_value_expr(&expr, &mut |node| {
                if let Expr::Identifier(name) = node {
                    visited.push(name.clone());
                    return Ok(Some(Expr::Identifier("replacement".into())));
                }
                Ok(None)
            });
            assert!(mapped.is_ok());
            assert!(!visited
                .iter()
                .any(|name| name == "nested_col" || name == "ordering_col"));
            if matches!(expr, Expr::WindowFunction { .. } | Expr::InSubquery { .. }) {
                assert_eq!(mapped.unwrap(), expr);
            } else {
                assert_eq!(visited, ["outer_col"]);
            }
        }
    }

    #[test]
    fn quantified_owned_binding_reports_list_failure_before_left_operand() {
        let expr = Expr::QuantifiedComparison {
            left: Box::new(Expr::Identifier("left".into())),
            op: BinaryOp::Eq,
            quantifier: SubqueryQuantifier::Any,
            right: QuantifiedComparisonRight::List(vec![Expr::Identifier("right".into())]),
        };
        let mapped = try_map_value_expr(&expr, &mut |node| match node {
            Expr::Identifier(name) => Err(name.clone()),
            _ => Ok(None),
        });
        assert_eq!(mapped, Err("right".into()));
    }

    #[test]
    fn mutable_rewriting_stops_on_failure_and_keeps_replacements_opaque() {
        let mut expr = projection("SELECT a + b + c");
        let mut visited = Vec::new();
        let result = try_rewrite_value_expr(&mut expr, |node| {
            if let Expr::Identifier(name) = node {
                visited.push(name.clone());
                match name.as_str() {
                    "a" => {
                        *node = Expr::Function {
                            name: "COALESCE".into(),
                            args: vec![Expr::Identifier("replacement_child".into())],
                            distinct: false,
                        };
                        return Ok(false);
                    }
                    "b" => return Err("stop"),
                    _ => {}
                }
            }
            Ok(true)
        });
        assert_eq!(result, Err("stop"));
        assert_eq!(visited, ["a", "b"]);
    }
}
