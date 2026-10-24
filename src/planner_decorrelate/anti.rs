//! `NOT IN (uncorrelated subquery)` → Anti Join rewrite.
//!
//! SQL's `NOT IN` has trap NULL semantics: `x NOT IN (1, NULL)` is
//! always UNKNOWN (so the surrounding `WHERE` filter drops the row),
//! and the same holds when the IN-list comes from a subquery that may
//! produce NULLs. A naïve Anti join ignores this and emits the outer
//! row whenever no inner key matches, which is wrong.
//!
//! Our rewrite is conservative: we only convert `NOT IN` to an Anti
//! join when:
//!
//! - The outer-side expression is a bare column reference to a
//!   declared `NOT NULL` column.
//! - The inner projection is a bare column reference to a declared
//!   `NOT NULL` column.
//! - The subquery body is uncorrelated and structurally simple
//!   (`is_simple_body` rules).
//!
//! Under these constraints the NULL trap can't fire — neither side can
//! produce NULL — so the Anti join's plain "no-match → emit" semantics
//! matches SQL exactly. Anything that doesn't pass the gate falls
//! back to the eval-time / `IndexInLookup` path.

use super::sublink::expr_is_outer_column as expr_touches_outer;
use crate::parser::boolean::{
    combine_conjuncts_left as combine_conjuncts, split_conjuncts_owned as split_conjuncts,
};
use crate::parser::{BinaryOp, Expr, Join, JoinType, Select, SelectItem, TableRef};
use crate::storage::Database;

use super::sublink::{is_simple_body, Scope};

/// Rewrite eligible `NOT IN` subqueries in `select`'s WHERE clause to
/// Anti joins against the referenced table. Returns the number of
/// predicates rewritten.
pub fn rewrite_not_in_to_anti(db: &Database, select: &mut Select) -> usize {
    if select.straight_join || select.joins.iter().any(|join| join.straight) {
        return 0;
    }
    let Some(predicate) = select.selection.take() else {
        return 0;
    };
    let mut conjuncts = Vec::new();
    split_conjuncts(predicate, &mut conjuncts);

    let mut keep = Vec::new();
    let mut new_joins = Vec::new();
    let mut rewrote = 0;
    for conj in conjuncts {
        match try_rewrite_one(db, select, conj) {
            Ok((join, extra)) => {
                new_joins.push(join);
                if let Some(e) = extra {
                    keep.push(e);
                }
                rewrote += 1;
            }
            Err(original) => keep.push(original),
        }
    }
    select.selection = combine_conjuncts(keep);
    select.joins.extend(new_joins);
    rewrote
}

/// Try to rewrite `conj` into an Anti join. On success returns
/// `(synthesized_join, optional_extra_where)` — the join is appended
/// to `select.joins`. Predicates from the inner query block belong to the
/// synthesized join condition, not to the outer WHERE: keeping them in the
/// outer block strands a right-only predicate as a pair residual and makes
/// the anti join build every inner row. On failure returns the original
/// conjunct so the caller can keep it.
fn try_rewrite_one(
    db: &Database,
    outer: &Select,
    conj: Expr,
) -> Result<(Join, Option<Expr>), Expr> {
    let (outer_expr, subquery) = match &conj {
        Expr::InSubquery {
            expr,
            subquery,
            negated: true,
        } => (expr.as_ref().clone(), (**subquery).clone()),
        _ => return Err(conj),
    };

    // Body structural check.
    if !is_simple_body(&subquery) {
        return Err(conj);
    }
    if subquery.projection.len() != 1 {
        return Err(conj);
    }

    // Body uncorrelated check.
    let outer_scope = crate::planner_decorrelate::select_scope(db, outer);
    let inner_scope = crate::planner_decorrelate::select_scope(db, &subquery);
    if subquery_references_outer(&subquery, &outer_scope, &inner_scope) {
        return Err(conj);
    }

    // NOT NULL check on the outer-side expression.
    let outer_col = match outer_column_ref(&outer_expr) {
        Some(c) => c,
        None => return Err(conj),
    };
    if !column_is_not_null(db, outer, &outer_col) {
        return Err(conj);
    }

    // NOT NULL check on the inner-side projection.
    let inner_proj_expr = match &subquery.projection[0] {
        SelectItem::Expr { expr, .. } => expr,
        _ => return Err(conj),
    };
    let inner_col = match outer_column_ref(inner_proj_expr) {
        Some(c) => c,
        None => return Err(conj),
    };
    if !column_is_not_null_in_table(db, &subquery, &inner_col) {
        return Err(conj);
    }

    // Build the Anti join. Use the subquery's FROM as the right side.
    let sub_table = subquery.from.first().cloned().expect("simple_body").clone();
    // Pick a qualifier that the planner's `matches_table` will resolve.
    // Prefer an explicit alias, falling back to the bare table name
    // (after stripping any database prefix the parser may have left in
    // `t.name` — `ScopeTable` is keyed on `schema.name` which is the
    // bare form). Don't use the full `test.b` form here: it ends up as
    // a single identifier piece that fails the eq_ignore_ascii_case
    // check against `b`.
    let qualifier = sub_table.alias.clone().unwrap_or_else(|| {
        sub_table
            .name
            .rsplit('.')
            .next()
            .unwrap_or(&sub_table.name)
            .to_string()
    });
    // Flattening equal visible names would bind the inner reference to the
    // outer table. Keep the query-block boundary in that case.
    if outer
        .from
        .iter()
        .chain(outer.joins.iter().map(|join| &join.table))
        .any(|table| {
            table
                .alias
                .as_deref()
                .unwrap_or_else(|| table.name.rsplit('.').next().unwrap_or(&table.name))
                .eq_ignore_ascii_case(&qualifier)
        })
    {
        return Err(conj);
    }
    // Promote the inner projection expression to a qualified reference
    // — the validator must be able to disambiguate the inner column
    // against any other table the rewrite leaves in scope.
    let inner_side = Expr::CompoundIdentifier(vec![qualifier, inner_col.column.clone()]);
    let mut outer_expr = outer_expr;
    crate::planner::qualify_outer_idents_by_owner(db, &mut outer_expr, outer);
    let join_cond = Expr::Binary {
        left: Box::new(outer_expr),
        op: BinaryOp::Eq,
        right: Box::new(inner_side),
    };
    let mut inner_filter = subquery.selection.clone();
    if let Some(filter) = &mut inner_filter {
        crate::planner::qualify_outer_idents_by_owner(db, filter, &subquery);
    }
    let condition = inner_filter.map_or(join_cond.clone(), |filter| Expr::Binary {
        left: Box::new(join_cond),
        op: BinaryOp::And,
        right: Box::new(filter),
    });
    let join = Join {
        table: sub_table,
        join_type: JoinType::Anti,
        condition: Some(condition),
        using_columns: Vec::new(),
        natural: false,
        derived: None,
        straight: false,
        lateral: false,
    };
    Ok((join, None))
}

#[derive(Debug, Clone)]
struct ColumnLoc {
    /// Qualifier (table/alias) when written, lowercase.
    qualifier: Option<String>,
    /// Column name, lowercase.
    column: String,
}

fn outer_column_ref(expr: &Expr) -> Option<ColumnLoc> {
    match expr {
        Expr::Identifier(name) => Some(ColumnLoc {
            qualifier: None,
            column: name.to_ascii_lowercase(),
        }),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => Some(ColumnLoc {
            qualifier: Some(parts[parts.len() - 2].to_ascii_lowercase()),
            column: parts[parts.len() - 1].to_ascii_lowercase(),
        }),
        _ => None,
    }
}

/// Returns true when `col`, used in `outer`'s scope, resolves to a
/// declared `NOT NULL` catalog column.
fn column_is_not_null(db: &Database, outer: &Select, col: &ColumnLoc) -> bool {
    // Track null-supplying sides in join order, including RIGHT/FULL joins
    // that null-extend the complete accumulated left input.
    let mut candidate_tables: Vec<(&TableRef, bool)> =
        outer.from.iter().map(|table| (table, false)).collect();
    for join in &outer.joins {
        if matches!(join.join_type, JoinType::Right | JoinType::Full) {
            for (_, nullable) in &mut candidate_tables {
                *nullable = true;
            }
        }
        candidate_tables.push((
            &join.table,
            matches!(join.join_type, JoinType::Left | JoinType::Full),
        ));
    }
    for (t, null_extended) in candidate_tables {
        if let Some(qualifier) = &col.qualifier {
            let alias = t.alias.as_deref().unwrap_or(&t.name).to_ascii_lowercase();
            let bare = t
                .name
                .rsplit('.')
                .next()
                .unwrap_or(&t.name)
                .to_ascii_lowercase();
            if alias != *qualifier && bare != *qualifier {
                continue;
            }
        }
        if let Ok(schema) = db.table(&t.name) {
            for c in &schema.columns {
                if c.name.eq_ignore_ascii_case(&col.column) {
                    return !null_extended && !c.nullable;
                }
            }
        }
    }
    false
}

fn column_is_not_null_in_table(db: &Database, subquery: &Select, col: &ColumnLoc) -> bool {
    let Some(t) = subquery.from.first() else {
        return false;
    };
    let Ok(schema) = db.table(&t.name) else {
        return false;
    };
    for c in &schema.columns {
        if c.name.eq_ignore_ascii_case(&col.column) {
            return !c.nullable;
        }
    }
    false
}

/// True if the subquery references any column / table from the outer
/// scope.
fn subquery_references_outer(subquery: &Select, outer: &Scope, inner: &Scope) -> bool {
    let mut found = false;
    let mut visit = |e: &Expr| {
        if found {
            return;
        }
        if expr_touches_outer(e, outer, inner) {
            found = true;
        }
    };
    if let Some(s) = &subquery.selection {
        walk_expr_pre(s, &mut visit);
    }
    for item in &subquery.projection {
        if let SelectItem::Expr { expr, .. } = item {
            walk_expr_pre(expr, &mut visit);
        }
    }
    for j in &subquery.joins {
        if let Some(c) = &j.condition {
            walk_expr_pre(c, &mut visit);
        }
    }
    for e in &subquery.group_by {
        walk_expr_pre(e, &mut visit);
    }
    if let Some(h) = &subquery.having {
        walk_expr_pre(h, &mut visit);
    }
    for ob in &subquery.order_by {
        walk_expr_pre(&ob.expr, &mut visit);
    }
    found
}

fn walk_expr_pre(expr: &Expr, f: &mut impl FnMut(&Expr)) {
    crate::parser::visit::any_expr_matching(
        expr,
        |node| {
            f(node);
            // This correlation check owns only window arguments, not OVER.
            if let Expr::WindowFunction { args, .. } = node {
                for arg in args {
                    walk_expr_pre(arg, f);
                }
            }
            false
        },
        crate::parser::visit::scalar_query_operand_children,
    );
}

#[cfg(test)]
mod traversal_tests {
    use super::*;

    #[test]
    fn correlation_walk_preserves_case_order_and_window_argument_scope() {
        let expr = crate::parser::parse_expr_fragment(
            "CASE WHEN a THEN b WHEN c THEN d ELSE e END + SUM(w) OVER (PARTITION BY p ORDER BY o)",
        )
        .unwrap();
        let mut names = Vec::new();
        walk_expr_pre(&expr, &mut |node| {
            if let Expr::Identifier(name) = node {
                names.push(name.clone());
            }
        });
        assert_eq!(names, ["a", "c", "b", "d", "e", "w"]);
        let expr = crate::parser::parse_expr_fragment("x IN (SELECT y FROM t WHERE z=1)").unwrap();
        names.clear();
        walk_expr_pre(&expr, &mut |node| {
            if let Expr::Identifier(name) = node {
                names.push(name.clone());
            }
        });
        assert_eq!(names, ["x"], "inner SELECT owns its identifiers");
    }
}
