//! Subquery transformation passes that run after `normalize_select_predicates`
//! has done its bookkeeping but **before** physical planning. These rewrites
//! turn nested subqueries — including correlated ones — into join shapes the
//! main planner already knows how to execute well (semi/anti join, left join
//! against a pre-aggregated derived table, inlined CTE-like scans). When a
//! rewrite can't be applied safely, the original `Expr::Subquery` /
//! `Expr::InSubquery` / `Expr::Exists` node is left alone and the eval-time
//! fallback in `engine::executor::select::eval` still produces a correct
//! answer.
//!
//! Modules:
//!   * [`sublink`] — classification of nested subqueries by syntactic shape
//!     and correlation pattern. Pure-AST analysis, no DB lookup, no planning.
//!   * (future) `scalar` — rewrite correlated scalar subqueries to a LEFT
//!     JOIN against a GROUP BY derived table.
//!   * (future) `derived` — push predicates into derived tables and inline
//!     trivial wrappers.
//!
//! All sub-modules are designed to be invoked from
//! [`crate::planner::normalize_select_predicates`] (after the existing
//! Phase B #B1 Semi/Anti pass) and to be safe no-ops on input they don't
//! recognise.

pub mod anti;
pub mod derived;
pub mod scalar;
pub mod sublink;

fn source_schema(
    db: &crate::storage::Database,
    name: &str,
) -> Option<std::sync::Arc<crate::storage::TableSchema>> {
    if let Some(cte) = crate::engine::executor::cte::lookup_cte(name) {
        return Some(std::sync::Arc::new(
            crate::storage::TableSchema::synthetic_for_planning(name, &cte.columns),
        ));
    }
    db.table(name).ok()
}

/// Catalog and CTE name scope shared by scalar and semi/anti rewrites.
/// Include derived outputs so bare outer references cannot be mistaken for
/// inner-only predicates during decorrelation.
pub(crate) fn select_scope(
    db: &crate::storage::Database,
    select: &crate::parser::Select,
) -> sublink::Scope {
    let mut scope = sublink::Scope::new();
    for (table, derived) in select
        .from
        .iter()
        .map(|table| (table, select.derived.as_ref()))
        .chain(
            select
                .joins
                .iter()
                .map(|join| (&join.table, join.derived.as_ref())),
        )
    {
        let schema = source_schema(db, &table.name);
        scope.add_table(&table.name);
        if let Some(alias) = &table.alias {
            scope.add_table(alias);
        }
        if let Some(derived) = derived {
            if !table.column_aliases.is_empty() {
                for name in &table.column_aliases {
                    scope.add_column(name);
                }
            } else if let Some(body) = derived.as_select() {
                if let Ok(names) = crate::planner::query_output_column_names(db, body) {
                    for name in names {
                        scope.add_column(&name);
                    }
                }
            }
        } else if let Some(schema) = schema {
            if let Some(bare) = table.name.rsplit('.').next() {
                if bare != table.name {
                    scope.add_table(bare);
                }
            }
            for column in &schema.columns {
                scope.add_column(&column.name);
            }
        }
    }
    scope
}

/// Grouping/deduplication may replace an equality comparison only when both
/// keys inhabit the same comparison domain. Unknown expressions stay on Apply.
pub(crate) fn column_comparison_domain(
    db: &crate::storage::Database,
    sources: &[crate::parser::TableRef],
    expr: &crate::parser::Expr,
) -> Option<crate::storage::ParamTarget> {
    use crate::parser::Expr;
    let (qualifier, name) = match expr {
        Expr::Identifier(name) => (None, name),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            (Some(&parts[parts.len() - 2]), parts.last()?)
        }
        _ => return None,
    };
    let mut domains = sources.iter().filter_map(|source| {
        if qualifier.is_some_and(|q| {
            !source
                .alias
                .as_ref()
                .unwrap_or(&source.name)
                .eq_ignore_ascii_case(q)
                && (source.alias.is_some()
                    || !source
                        .name
                        .rsplit('.')
                        .next()
                        .unwrap_or(&source.name)
                        .eq_ignore_ascii_case(q))
        }) {
            return None;
        }
        let schema = source_schema(db, &source.name)?;
        let column = &schema.columns[schema.column_index(name).ok()?];
        Some(crate::storage::ParamTarget::for_column_with_collation(
            &column.data_type,
            column.unsigned,
            column.collation.as_deref().or(Some(&schema.collation)),
        ))
    });
    let domain = domains.next()?;
    domains.next().is_none().then_some(domain)
}
