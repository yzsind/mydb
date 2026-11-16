//! Query SQL for policy inspection. Reject unsupported source surfaces
//! explicitly rather than returning scalar display placeholders.
use super::{quote_ident, render_expr_options, SqlEncodeError};
use crate::parser::*;

fn expr(expr: &Expr) -> String {
    render_expr_options(expr, true, true)
}
fn names(names: &[String]) -> String {
    names
        .iter()
        .map(|name| quote_ident(name))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn validate_select(select: &Select) -> Result<(), SqlEncodeError> {
    if select.values.is_some()
        || select.json_table.is_some()
        || !select.query_hints.is_empty()
        || select.for_update
        || select.lock_clause.is_some()
        || select.sql_calc_found_rows
    {
        return Err(SqlEncodeError(
            "query source/hint encoding is not supported",
        ));
    }
    for reference in select
        .from
        .iter()
        .chain(select.joins.iter().map(|join| &join.table))
    {
        if reference.sample.is_some()
            || reference.generate_series.is_some()
            || !reference.index_hints.is_empty()
        {
            return Err(SqlEncodeError(
                "table source/hint encoding is not supported",
            ));
        }
    }
    if select
        .joins
        .iter()
        .any(|join| matches!(join.join_type, JoinType::Semi | JoinType::Anti))
    {
        return Err(SqlEncodeError(
            "bound join has no source SQL representation",
        ));
    }
    if select
        .grouping_sets
        .iter()
        .flatten()
        .any(|index| *index >= select.group_by.len())
    {
        return Err(SqlEncodeError("invalid grouping set index"));
    }
    Ok(())
}

fn source(reference: &TableRef, derived: Option<&QueryExpr>) -> String {
    let mut sql = derived.map_or_else(
        || {
            reference
                .name
                .split('.')
                .map(quote_ident)
                .collect::<Vec<_>>()
                .join(".")
        },
        |query| format!("({})", query_sql(query)),
    );
    if !reference.partitions.is_empty() {
        sql.push_str(&format!(" PARTITION ({})", names(&reference.partitions)));
    }
    if let Some(version) = &reference.system_time {
        sql.push_str(&format!(" FOR SYSTEM_TIME AS OF {}", expr(&version.expr)));
    }
    if let Some(alias) = &reference.alias {
        sql.push_str(&format!(" AS {}", quote_ident(alias)));
    }
    if !reference.column_aliases.is_empty() {
        sql.push_str(&format!(" ({})", names(&reference.column_aliases)));
    }
    sql
}
fn order_sql(order: &[OrderBy]) -> String {
    order
        .iter()
        .map(|key| {
            format!(
                "{} {}{}",
                expr(&key.expr),
                if key.asc { "ASC" } else { "DESC" },
                key.nulls_first.map_or("", |first| if first {
                    " NULLS FIRST"
                } else {
                    " NULLS LAST"
                })
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}
fn limit_sql(limit: &LimitValue) -> String {
    match limit {
        LimitValue::Rows(rows) => rows.to_string(),
        _ => unreachable!("policy parameters are rejected"),
    }
}
fn tail(
    sql: &mut String,
    order: &[OrderBy],
    limit: Option<&LimitValue>,
    offset: Option<&LimitValue>,
) {
    if !order.is_empty() {
        sql.push_str(&format!(" ORDER BY {}", order_sql(order)));
    }
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {}", limit_sql(limit)));
        if let Some(offset) = offset {
            sql.push_str(&format!(" OFFSET {}", limit_sql(offset)));
        }
    } else if let Some(offset) = offset {
        sql.push_str(&format!(
            " LIMIT 18446744073709551615 OFFSET {}",
            limit_sql(offset)
        ));
    }
}
pub(super) fn select_sql(select: &Select) -> String {
    let projection = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::Wildcard => "*".into(),
            SelectItem::QualifiedWildcard(name) => format!(
                "{}.*",
                name.split('.')
                    .map(quote_ident)
                    .collect::<Vec<_>>()
                    .join(".")
            ),
            SelectItem::Expr { expr: value, alias } => {
                let mut sql = expr(value);
                if let Some(alias) = alias {
                    sql.push_str(&format!(" AS {}", quote_ident(alias)));
                }
                sql
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!(
        "SELECT {}{}{}",
        if select.distinct { "DISTINCT " } else { "" },
        if select.straight_join {
            "STRAIGHT_JOIN "
        } else {
            ""
        },
        projection
    );
    if !select.from.is_empty() {
        sql.push_str(" FROM ");
        sql.push_str(
            &select
                .from
                .iter()
                .enumerate()
                .map(|(index, reference)| {
                    source(
                        reference,
                        (index == 0).then_some(select.derived.as_ref()).flatten(),
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    for join in &select.joins {
        let kind = if join.straight {
            "STRAIGHT_JOIN"
        } else {
            match join.join_type {
                JoinType::Inner => "INNER JOIN",
                JoinType::Left => "LEFT JOIN",
                JoinType::Right => "RIGHT JOIN",
                JoinType::Full => "FULL JOIN",
                JoinType::Cross => "CROSS JOIN",
                _ => unreachable!("validated source join"),
            }
        };
        sql.push_str(&format!(
            " {}{} {}{}",
            if join.natural { "NATURAL " } else { "" },
            kind,
            if join.lateral { "LATERAL " } else { "" },
            source(&join.table, join.derived.as_ref())
        ));
        if !join.natural {
            if let Some(condition) = &join.condition {
                sql.push_str(&format!(" ON {}", expr(condition)));
            }
            if !join.using_columns.is_empty() {
                sql.push_str(&format!(" USING ({})", names(&join.using_columns)));
            }
        }
    }
    if let Some(filter) = &select.selection {
        sql.push_str(&format!(" WHERE {}", expr(filter)));
    }
    if !select.group_by.is_empty() || !select.grouping_sets.is_empty() {
        let keys = select.group_by.iter().map(expr).collect::<Vec<_>>();
        let group = match select.group_by_modifier {
            GroupByModifier::None => keys.join(", "),
            GroupByModifier::Rollup => format!("ROLLUP ({})", keys.join(", ")),
            GroupByModifier::Cube => format!("CUBE ({})", keys.join(", ")),
            GroupByModifier::GroupingSets => format!(
                "GROUPING SETS ({})",
                select
                    .grouping_sets
                    .iter()
                    .map(|set| format!(
                        "({})",
                        set.iter()
                            .map(|index| keys[*index].as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        };
        sql.push_str(&format!(" GROUP BY {group}"));
    }
    if let Some(having) = &select.having {
        sql.push_str(&format!(" HAVING {}", expr(having)));
    }
    if !select.named_windows.is_empty() {
        sql.push_str(" WINDOW ");
        sql.push_str(
            &select
                .named_windows
                .iter()
                .map(|window| {
                    format!(
                        "{} AS ({})",
                        quote_ident(&window.name),
                        window_sql(
                            &window.partition_by,
                            &window.order_by,
                            window.frame.as_ref(),
                            window.inherits.as_deref()
                        )
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    tail(
        &mut sql,
        &select.order_by,
        select.limit.as_ref(),
        select.offset.as_ref(),
    );
    sql
}
fn query_sql(query: &QueryExpr) -> String {
    match query {
        QueryExpr::Select(select) => select_sql(select),
        QueryExpr::SetOp(set) => {
            let mut sql = format!(
                "({}) {}{} ({})",
                query_sql(&set.left),
                match set.op {
                    SetOp::Union => "UNION",
                    SetOp::Intersect => "INTERSECT",
                    SetOp::Except => "EXCEPT",
                },
                if set.all { " ALL" } else { "" },
                query_sql(&set.right)
            );
            tail(
                &mut sql,
                &set.order_by,
                set.limit.as_ref(),
                set.offset.as_ref(),
            );
            sql
        }
        QueryExpr::WithCte(with) => format!(
            "WITH {}{} {}",
            if with.recursive { "RECURSIVE " } else { "" },
            with.ctes
                .iter()
                .map(|cte| format!(
                    "{}{} AS ({})",
                    quote_ident(&cte.name),
                    if cte.columns.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", names(&cte.columns))
                    },
                    query_sql(&cte.query)
                ))
                .collect::<Vec<_>>()
                .join(", "),
            query_sql(&with.body)
        ),
        QueryExpr::Rrf(_) => unreachable!("rank-fusion encoding is rejected"),
    }
}
pub(super) fn window_sql(
    partition: &[Expr],
    order: &[OrderBy],
    frame: Option<&WindowFrame>,
    inherits: Option<&str>,
) -> String {
    let mut parts = Vec::new();
    if let Some(name) = inherits {
        parts.push(quote_ident(name));
    }
    if !partition.is_empty() {
        parts.push(format!(
            "PARTITION BY {}",
            partition.iter().map(expr).collect::<Vec<_>>().join(", ")
        ));
    }
    if !order.is_empty() {
        parts.push(format!("ORDER BY {}", order_sql(order)));
    }
    if let Some(frame) = frame {
        let bound = |bound: &WindowFrameBound| match bound {
            WindowFrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".into(),
            WindowFrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".into(),
            WindowFrameBound::CurrentRow => "CURRENT ROW".into(),
            WindowFrameBound::Preceding(value) => format!("{} PRECEDING", expr(value)),
            WindowFrameBound::Following(value) => format!("{} FOLLOWING", expr(value)),
        };
        parts.push(format!(
            "{:?} BETWEEN {} AND {} EXCLUDE {}",
            frame.units,
            bound(&frame.start),
            bound(&frame.end),
            match frame.exclusion {
                WindowFrameExclusion::NoOthers => "NO OTHERS",
                WindowFrameExclusion::CurrentRow => "CURRENT ROW",
                WindowFrameExclusion::Group => "GROUP",
                WindowFrameExclusion::Ties => "TIES",
            }
        ));
    }
    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    fn policy_expression(sql: &str) -> Expr {
        let Statement::RowSecurity(crate::parser::rls::RlsStatement::Create { policy, .. }) =
            parse_statement(&format!("CREATE POLICY p ON t USING({sql})")).unwrap()
        else {
            panic!()
        };
        policy.using.unwrap()
    }
    #[test]
    fn executable_policy_display_round_trips_query_and_window_syntax() {
        for sql in [
            "EXISTS(SELECT 1 FROM authz a WHERE a.id=t.id)",
            "tenant IN(SELECT tenant FROM authz WHERE enabled=TRUE ORDER BY tenant DESC LIMIT 3)",
            "tenant=ANY(SELECT tenant FROM authz)",
            "EXISTS(SELECT COUNT(*) FROM authz GROUP BY tenant HAVING COUNT(*)>1)",
            "EXISTS(SELECT ROW_NUMBER() OVER(PARTITION BY tenant ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM authz)",
        ] {
            let original = policy_expression(sql);
            let encoded = crate::parser::try_query_expr_to_sql(&original).unwrap();
            assert_eq!(policy_expression(&encoded), original, "{sql}: {encoded}");
        }
    }
    #[test]
    fn unsupported_policy_display_is_an_error_instead_of_a_placeholder() {
        let expression = policy_expression("EXISTS(SELECT /*+ PARALLEL(4) */ 1 FROM authz)");
        assert!(crate::parser::try_query_expr_to_sql(&expression).is_err());
    }
}
