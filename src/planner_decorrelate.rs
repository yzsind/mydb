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

/// Catalog-backed name scope shared by scalar and semi/anti rewrites.
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
        let schema = db.table(&table.name);
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
        } else if let Ok(schema) = schema {
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
