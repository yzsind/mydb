//! Sublink (nested subquery) classification.
//!
//! Pure-AST analysis of `Expr::Exists` / `Expr::InSubquery` / `Expr::Subquery`
//! against a known outer scope. Produces a structured [`SubLinkInfo`] that
//! downstream rewriters in [`super`] use to decide whether to decorrelate to
//! a join or leave the subquery for eval-time row-by-row evaluation.
//!
//! Design notes:
//!
//! * **No DB lookups.** The caller precomputes the set of outer table
//!   aliases / outer column names and inner table aliases / inner column
//!   names. This keeps the classifier pure and easy to unit-test against
//!   handcrafted AST nodes.
//! * **Conservative.** When in doubt we report
//!   [`CorrelationShape::Complex`], which makes downstream rewriters fall
//!   back to the safe eval-time path. False negatives are fine; false
//!   positives (claiming `OnlyEquality` when correlation is actually
//!   non-equality) would break SQL semantics.
//! * **Equality pairs are the key product.** When a subquery is correlated
//!   only through `outer.k = inner.x` conjunctive equalities, the scalar
//!   rewriter can promote those `outer.k` references into GROUP BY keys
//!   on the inner side and turn the whole construct into a LEFT JOIN.

use crate::parser::boolean::collect_expr_conjuncts as split_conjuncts;
use std::collections::HashSet;

use crate::parser::{BinaryOp, Expr, QuantifiedComparisonRight, Select, SelectItem};

/// Sublink categories (analogous to PostgreSQL's `SubLinkType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubLinkKind {
    /// `EXISTS (...)` (negated = `NOT EXISTS`).
    Existential { negated: bool },
    /// `x IN (SELECT y FROM ...)` (negated = `NOT IN`).
    Quantified { negated: bool },
    /// Standalone `(SELECT ...)` returning a single scalar value.
    /// `aggregated` is true when the projection includes an aggregate
    /// function (`MAX`, `SUM`, `COUNT`, ...) — relevant because the
    /// rewrite to LEFT JOIN preserves single-row semantics only when
    /// the subquery is "naturally" a single-row producer (either via
    /// aggregate or via `LIMIT 1`).
    Scalar { aggregated: bool },
}

/// One outer-scope column referenced from inside the subquery body.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CorrelatedRef {
    /// Lowercase column name as referenced.
    pub column: String,
    /// Lowercase qualifier (table/alias) if the reference was qualified.
    pub qualifier: Option<String>,
}

/// Coarse description of how the subquery body refers to outer columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrelationShape {
    /// No outer-scope reference inside the subquery body.
    Uncorrelated,
    /// Every outer reference appears as one side of a top-level
    /// conjunctive equality (`outer.k = inner.x` or `inner.x = outer.k`)
    /// in the subquery WHERE / JOIN-ON. Safe to decorrelate to a
    /// hash semi/anti-join or to a `GROUP BY` derived table.
    OnlyEquality,
    /// At least one outer reference appears outside of a top-level
    /// conjunctive equality (OR, inequality, function arg, ...).
    /// Requires general Apply-style unnesting; current rewriters
    /// punt on this and keep the eval-time path.
    Complex,
}

/// An `outer.k = inner.x` (or reversed) equality lifted out of the
/// subquery's WHERE / JOIN-ON clauses. The `outer` side records which
/// outer column was referenced; `inner_expr` is the literal AST node
/// from the inner side — usually an identifier, but may be a
/// function call (e.g. `LOWER(inner.x) = outer.k`) which the rewriter
/// can use as a GROUP BY key as-is.
#[derive(Debug, Clone)]
pub struct EqualityPair {
    pub outer: CorrelatedRef,
    pub inner_expr: Expr,
}

/// Classification result for one subquery node.
#[derive(Debug, Clone)]
pub struct SubLinkInfo {
    pub kind: SubLinkKind,
    pub shape: CorrelationShape,
    /// Deduplicated outer references encountered anywhere in the body.
    pub correlated_refs: Vec<CorrelatedRef>,
    /// True when the body is a single-table, no-join, no-group-by,
    /// no-having, no-order-by, no-limit, no-offset, no-distinct,
    /// no-`FOR UPDATE`, no-named-window, no-derived-FROM select.
    /// "Simple" enough to push into a SemiJoin/AntiJoin shape without
    /// further structural work.
    pub simple_body: bool,
    /// True when the projection contains at least one aggregate
    /// function. Independent of `GROUP BY` — `SELECT MAX(x) FROM t`
    /// has no GROUP BY but is aggregated.
    pub aggregate_in_projection: bool,
    /// Extracted top-level equality pairs, only meaningful when
    /// `shape == OnlyEquality`.
    pub equality_pairs: Vec<EqualityPair>,
}

/// Read-only view of an outer scope's table / column identifiers.
/// Lowercase, deduplicated. The caller is responsible for ensuring
/// `tables` covers every alias the user could have written
/// (`outer.t.col`, `t.col`, just `col`).
#[derive(Debug, Default, Clone)]
pub struct Scope {
    pub tables: HashSet<String>,
    pub columns: HashSet<String>,
}

impl Scope {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub fn with_table(mut self, name: impl Into<String>) -> Self {
        self.tables.insert(name.into().to_ascii_lowercase());
        self
    }

    #[cfg(test)]
    pub fn with_column(mut self, name: impl Into<String>) -> Self {
        self.columns.insert(name.into().to_ascii_lowercase());
        self
    }

    pub fn add_table(&mut self, name: &str) {
        self.tables.insert(name.to_ascii_lowercase());
    }

    pub fn add_column(&mut self, name: &str) {
        self.columns.insert(name.to_ascii_lowercase());
    }
}

/// Classify the given sublink expression. Returns `None` when `sub_expr`
/// is not a subquery node — callers can use this as a quick filter.
///
/// `inner` is the precomputed scope of the subquery body (every table
/// alias it visibly uses and every column declared on those tables);
/// `outer` is the surrounding SELECT's scope.
pub fn classify_sublink(sub_expr: &Expr, outer: &Scope, inner: &Scope) -> Option<SubLinkInfo> {
    let (kind, subquery) = match sub_expr {
        Expr::Exists { subquery, negated } => (
            SubLinkKind::Existential { negated: *negated },
            subquery.as_ref(),
        ),
        Expr::InSubquery {
            subquery, negated, ..
        } => (
            SubLinkKind::Quantified { negated: *negated },
            subquery.as_ref(),
        ),
        Expr::Subquery(subquery) => {
            let aggregated = projection_has_aggregate(subquery.as_ref());
            (SubLinkKind::Scalar { aggregated }, subquery.as_ref())
        }
        _ => return None,
    };

    let simple_body = is_simple_body(subquery);
    let aggregate_in_projection = projection_has_aggregate(subquery);

    let analysis = analyze_correlation(subquery, outer, inner);
    Some(SubLinkInfo {
        kind,
        shape: analysis.shape,
        correlated_refs: analysis.refs,
        simple_body,
        aggregate_in_projection,
        equality_pairs: analysis.equality_pairs,
    })
}

#[derive(Debug, Default)]
struct CorrelationAnalysis {
    shape: CorrelationShape,
    refs: Vec<CorrelatedRef>,
    equality_pairs: Vec<EqualityPair>,
}

impl Default for CorrelationShape {
    fn default() -> Self {
        CorrelationShape::Uncorrelated
    }
}

fn analyze_correlation(subquery: &Select, outer: &Scope, inner: &Scope) -> CorrelationAnalysis {
    let mut out = CorrelationAnalysis::default();
    let mut all_refs: HashSet<CorrelatedRef> = HashSet::new();

    // Pass 1: walk the WHERE / JOIN-ON predicates as conjunctive equalities.
    let mut conjuncts = Vec::new();
    if let Some(selection) = &subquery.selection {
        split_conjuncts(selection, &mut conjuncts);
    }
    for join in &subquery.joins {
        if let Some(condition) = &join.condition {
            split_conjuncts(condition, &mut conjuncts);
        }
    }

    let mut any_correlation = false;
    let mut any_outside_equality = false;
    let mut consumed_conjuncts: Vec<bool> = vec![false; conjuncts.len()];

    for (idx, conj) in conjuncts.iter().enumerate() {
        match try_split_equality(conj, outer, inner) {
            EqualityKind::OuterInner(pair) => {
                all_refs.insert(pair.outer.clone());
                out.equality_pairs.push(pair);
                consumed_conjuncts[idx] = true;
                any_correlation = true;
            }
            EqualityKind::InnerOnly => {
                // Inner-only equality — not a correlation, no effect.
            }
            EqualityKind::ContainsOuter => {
                // Outer reference but not a clean equality: complex.
                collect_outer_refs(conj, outer, inner, &mut all_refs);
                any_correlation = true;
                any_outside_equality = true;
            }
            EqualityKind::Neither => {
                // No outer reference; ignore.
            }
        }
    }

    // Pass 2: any remaining places — projection, group_by, having, order_by —
    // that mention outer columns mean `Complex` (we don't try to push them
    // through aggregation yet).
    let projection_iter = subquery.projection.iter().filter_map(|item| match item {
        SelectItem::Expr { expr, .. } => Some(expr),
        _ => None,
    });
    let mut visit_expr = |expr: &Expr| {
        let before = all_refs.len();
        collect_outer_refs(expr, outer, inner, &mut all_refs);
        if all_refs.len() > before {
            any_correlation = true;
            any_outside_equality = true;
        }
    };
    for expr in projection_iter {
        visit_expr(expr);
    }
    for expr in &subquery.group_by {
        visit_expr(expr);
    }
    if let Some(having) = &subquery.having {
        visit_expr(having);
    }
    for ob in &subquery.order_by {
        visit_expr(&ob.expr);
    }

    out.refs = all_refs.into_iter().collect();
    out.refs.sort_by(|a, b| {
        a.qualifier
            .cmp(&b.qualifier)
            .then_with(|| a.column.cmp(&b.column))
    });

    out.shape = if !any_correlation {
        CorrelationShape::Uncorrelated
    } else if any_outside_equality {
        // If anything outside a clean equality references outer, we
        // can't safely reduce the subquery to a hash equi-join. Drop
        // any speculatively-collected equality pairs to avoid the
        // rewriter using them.
        out.equality_pairs.clear();
        CorrelationShape::Complex
    } else {
        CorrelationShape::OnlyEquality
    };

    out
}

enum EqualityKind {
    /// A binary equality with exactly one side referring only to outer
    /// columns and the other side referring only to inner columns
    /// (or constants).
    OuterInner(EqualityPair),
    /// A binary equality involving outer references but not in the
    /// clean "outer on one side, inner on the other" shape.
    ContainsOuter,
    /// A binary equality referencing only inner columns / constants.
    InnerOnly,
    /// No outer or inner identifier participation at the top binary
    /// level; the expression may still be complex.
    Neither,
}

fn try_split_equality(expr: &Expr, outer: &Scope, inner: &Scope) -> EqualityKind {
    let Expr::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    } = expr
    else {
        // Not a top-level equality. Check whether it touches outer.
        let mut refs = HashSet::new();
        collect_outer_refs(expr, outer, inner, &mut refs);
        if refs.is_empty() {
            return EqualityKind::Neither;
        }
        return EqualityKind::ContainsOuter;
    };
    let left_is_outer_col = as_outer_column(left, outer, inner);
    let right_is_outer_col = as_outer_column(right, outer, inner);
    let left_has_inner = expr_references_inner(left, outer, inner);
    let right_has_inner = expr_references_inner(right, outer, inner);
    let mut left_outer = HashSet::new();
    let mut right_outer = HashSet::new();
    collect_outer_refs(left, outer, inner, &mut left_outer);
    collect_outer_refs(right, outer, inner, &mut right_outer);

    match (left_is_outer_col, right_is_outer_col) {
        (Some(outer_ref), None)
            if left_outer.len() == 1 && right_outer.is_empty() && right_has_inner =>
        {
            EqualityKind::OuterInner(EqualityPair {
                outer: outer_ref,
                inner_expr: (**right).clone(),
            })
        }
        (None, Some(outer_ref))
            if right_outer.len() == 1 && left_outer.is_empty() && left_has_inner =>
        {
            EqualityKind::OuterInner(EqualityPair {
                outer: outer_ref,
                inner_expr: (**left).clone(),
            })
        }
        _ => {
            // Either no outer ref at all, or outer ref but not in the
            // clean "bare-column on one side, inner-only on the other".
            if left_outer.is_empty() && right_outer.is_empty() {
                EqualityKind::InnerOnly
            } else {
                EqualityKind::ContainsOuter
            }
        }
    }
}

/// If `expr` is a bare identifier or `qualifier.column` reference that
/// resolves to the outer scope (and not the inner scope), return the
/// canonical `CorrelatedRef`. Otherwise `None`. Used to enforce
/// "outer side must be a column, not a function call" in equality
/// shape detection.
fn as_outer_column(expr: &Expr, outer: &Scope, inner: &Scope) -> Option<CorrelatedRef> {
    match expr {
        Expr::Identifier(name) => {
            let lc = name.to_ascii_lowercase();
            if outer.columns.contains(&lc) && !inner.columns.contains(&lc) {
                Some(CorrelatedRef {
                    column: lc,
                    qualifier: None,
                })
            } else {
                None
            }
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = parts[parts.len() - 2].to_ascii_lowercase();
            let column = parts[parts.len() - 1].to_ascii_lowercase();
            if outer.tables.contains(&qualifier) && !inner.tables.contains(&qualifier) {
                Some(CorrelatedRef {
                    column,
                    qualifier: Some(qualifier),
                })
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Walks `expr` collecting any identifier references that resolve to the
/// outer scope but **not** the inner scope. Both `outer` and `inner` may
/// contain overlapping names (e.g. both have a column called `id`); when
/// an unqualified reference matches both, we treat it as inner to stay
/// safe — the user has to qualify if they wanted the outer one.
fn collect_outer_refs(expr: &Expr, outer: &Scope, inner: &Scope, out: &mut HashSet<CorrelatedRef>) {
    struct Collector<'a> {
        outer: &'a Scope,
        inner: &'a Scope,
        out: &'a mut HashSet<CorrelatedRef>,
    }
    impl crate::parser::visit::AstVisitor for Collector<'_> {
        fn enter_expr(&mut self, expr: &Expr) {
            let Self { outer, inner, out } = self;
            match expr {
                Expr::Identifier(name) => {
                    let lc = name.to_ascii_lowercase();
                    // Unqualified: outer reference only if outer has the column
                    // AND inner does not. Ambiguous cases (both have it) → inner.
                    if outer.columns.contains(&lc) && !inner.columns.contains(&lc) {
                        out.insert(CorrelatedRef {
                            column: lc,
                            qualifier: None,
                        });
                    }
                }
                Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                    let qualifier = parts[parts.len() - 2].to_ascii_lowercase();
                    let column = parts[parts.len() - 1].to_ascii_lowercase();
                    // Qualified: outer iff qualifier names an outer table AND
                    // qualifier doesn't also name an inner one.
                    if outer.tables.contains(&qualifier) && !inner.tables.contains(&qualifier) {
                        out.insert(CorrelatedRef {
                            column,
                            qualifier: Some(qualifier),
                        });
                    }
                }
                _ => {}
            }
        }
        fn enter_select(&mut self, select: &Select) {
            collect_outer_refs_in_select(select, self.outer, self.inner, self.out);
        }
        fn walk_select_children(&self, _: &Select) -> bool {
            false
        }
        fn walk_window_frame_children(&self, _: &crate::parser::WindowFrame) -> bool {
            false
        }
        fn case_results_interleaved(&self) -> bool {
            false
        }
    }
    crate::parser::visit::visit_expr(&mut Collector { outer, inner, out }, expr);
}

fn collect_outer_refs_in_select(
    select: &Select,
    outer: &Scope,
    inner: &Scope,
    out: &mut HashSet<CorrelatedRef>,
) {
    // Nested subqueries can themselves reference the original outer
    // scope, so we descend through them with the same `outer`. We do
    // NOT add the nested subquery's own inner tables to `inner`
    // here — for our purposes, anything that wasn't already in inner
    // is "below" us and a deeper outer reference will still resolve
    // to `outer` correctly.
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            collect_outer_refs(expr, outer, inner, out);
        }
    }
    if let Some(selection) = &select.selection {
        collect_outer_refs(selection, outer, inner, out);
    }
    for join in &select.joins {
        if let Some(condition) = &join.condition {
            collect_outer_refs(condition, outer, inner, out);
        }
    }
    if let Some(having) = &select.having {
        collect_outer_refs(having, outer, inner, out);
    }
    for expr in &select.group_by {
        collect_outer_refs(expr, outer, inner, out);
    }
    for ob in &select.order_by {
        collect_outer_refs(&ob.expr, outer, inner, out);
    }
}

fn expr_references_inner(expr: &Expr, outer: &Scope, inner: &Scope) -> bool {
    match expr {
        Expr::Identifier(name) => {
            let lc = name.to_ascii_lowercase();
            inner.columns.contains(&lc)
                || (!outer.columns.contains(&lc) && !inner.columns.contains(&lc))
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = parts[parts.len() - 2].to_ascii_lowercase();
            inner.tables.contains(&qualifier)
        }
        Expr::CompoundIdentifier(_) => false,
        Expr::Wildcard
        | Expr::Literal(_)
        | Expr::Placeholder(_)
        | Expr::BoundValue(_)
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Grouping(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. } => false,
        Expr::Tuple(items) => items
            .iter()
            .any(|item| expr_references_inner(item, outer, inner)),
        Expr::GroupKeyRef { expr, .. } => expr_references_inner(expr, outer, inner),
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            expr_references_inner(expr, outer, inner)
        }
        Expr::Binary { left, right, .. } => {
            expr_references_inner(left, outer, inner) || expr_references_inner(right, outer, inner)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_inner(expr, outer, inner)
                || list.iter().any(|e| expr_references_inner(e, outer, inner))
        }
        Expr::InSubquery { expr, .. } => expr_references_inner(expr, outer, inner),
        Expr::QuantifiedComparison { left, right, .. } => {
            expr_references_inner(left, outer, inner)
                || match right {
                    QuantifiedComparisonRight::Subquery(_) => false,
                    QuantifiedComparisonRight::List(list) => {
                        list.iter().any(|e| expr_references_inner(e, outer, inner))
                    }
                }
        }
        Expr::Subquery(_) | Expr::Exists { .. } => false,
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_inner(expr, outer, inner)
                || expr_references_inner(low, outer, inner)
                || expr_references_inner(high, outer, inner)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_references_inner(expr, outer, inner)
                || expr_references_inner(pattern, outer, inner)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|o| expr_references_inner(o, outer, inner))
                || conditions
                    .iter()
                    .any(|c| expr_references_inner(c, outer, inner))
                || results
                    .iter()
                    .any(|r| expr_references_inner(r, outer, inner))
                || else_result
                    .as_ref()
                    .is_some_and(|e| expr_references_inner(e, outer, inner))
        }
        Expr::Function { args, .. } | Expr::WindowFunction { args, .. } => {
            args.iter().any(|a| expr_references_inner(a, outer, inner))
        }
    }
}

/// True when the body is structurally simple enough for the Semi/Anti-join
/// rewriter to consume. Matches the pre-existing rule in
/// [`crate::planner::subquery_is_simple_for_semi`] so a `SubLinkInfo` with
/// `simple_body = true` is exactly the set of bodies the Phase B #B1
/// rewriter would have accepted.
pub fn is_simple_body(subquery: &Select) -> bool {
    crate::parser::query_shape::simple_single_source_body(subquery)
        && !projection_has_aggregate(&subquery.projection)
        && !projection_has_window(&subquery.projection)
}

/// Aggregate-function presence in a SELECT's projection list. Accepts
/// either the projection slice or a whole `Select`.
pub(crate) fn projection_has_aggregate<P: ProjectionLike + ?Sized>(p: &P) -> bool {
    p.scan(true, false)
}

pub(crate) fn projection_has_window<P: ProjectionLike + ?Sized>(p: &P) -> bool {
    p.scan(false, true)
}

pub(crate) trait ProjectionLike {
    fn scan(&self, check_aggregate: bool, check_window: bool) -> bool;
}

impl ProjectionLike for [SelectItem] {
    fn scan(&self, check_aggregate: bool, check_window: bool) -> bool {
        self.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => {
                expr_contains_aggregate_or_window(expr, check_aggregate, check_window)
            }
            _ => false,
        })
    }
}

impl ProjectionLike for Vec<SelectItem> {
    fn scan(&self, check_aggregate: bool, check_window: bool) -> bool {
        self.as_slice().scan(check_aggregate, check_window)
    }
}

impl ProjectionLike for Select {
    fn scan(&self, check_aggregate: bool, check_window: bool) -> bool {
        self.projection.scan(check_aggregate, check_window)
    }
}

fn expr_contains_aggregate_or_window(
    expr: &Expr,
    check_aggregate: bool,
    check_window: bool,
) -> bool {
    match expr {
        Expr::Function { name, .. } if check_aggregate => is_aggregate_function(name),
        Expr::WindowFunction { .. } if check_window => true,
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            expr_contains_aggregate_or_window(expr, check_aggregate, check_window)
        }
        Expr::Binary { left, right, .. } => {
            expr_contains_aggregate_or_window(left, check_aggregate, check_window)
                || expr_contains_aggregate_or_window(right, check_aggregate, check_window)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate_or_window(expr, check_aggregate, check_window)
                || list
                    .iter()
                    .any(|e| expr_contains_aggregate_or_window(e, check_aggregate, check_window))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate_or_window(expr, check_aggregate, check_window)
                || expr_contains_aggregate_or_window(low, check_aggregate, check_window)
                || expr_contains_aggregate_or_window(high, check_aggregate, check_window)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_contains_aggregate_or_window(expr, check_aggregate, check_window)
                || expr_contains_aggregate_or_window(pattern, check_aggregate, check_window)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_ref().is_some_and(|e| {
                expr_contains_aggregate_or_window(e, check_aggregate, check_window)
            }) || conditions
                .iter()
                .any(|c| expr_contains_aggregate_or_window(c, check_aggregate, check_window))
                || results
                    .iter()
                    .any(|r| expr_contains_aggregate_or_window(r, check_aggregate, check_window))
                || else_result.as_ref().is_some_and(|e| {
                    expr_contains_aggregate_or_window(e, check_aggregate, check_window)
                })
        }
        Expr::Function { args, .. } => args
            .iter()
            .any(|a| expr_contains_aggregate_or_window(a, check_aggregate, check_window)),
        Expr::WindowFunction { args, .. } => args
            .iter()
            .any(|a| expr_contains_aggregate_or_window(a, check_aggregate, check_window)),
        _ => false,
    }
}

/// Reuse the parser's aggregate registry when deciding query-block ownership.
pub fn is_aggregate_function(name: &str) -> bool {
    crate::parser::is_aggregate_function(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{BinaryOp, Expr, Literal, Select, SelectItem, TableRef};

    fn empty_select() -> Select {
        Select {
            distinct: false,
            projection: vec![],
            from: vec![TableRef {
                name: "inner_t".into(),
                alias: None,
                system_time: None,
                index_hints: vec![],
                partitions: vec![],
                column_aliases: Vec::new(),
                generate_series: None,
            }],
            joins: vec![],
            selection: None,
            group_by: vec![],
            grouping_sets: Vec::new(),
            group_by_modifier: crate::parser::GroupByModifier::None,
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
            table: Some("inner_t".into()),
            values: None,
            json_table: None,
            derived: None,
            no_merge: false,
            straight_join: false,
            for_update: false,
            lock_clause: None,
            sql_calc_found_rows: false,
            named_windows: vec![],
            query_hints: vec![],
            hint_warnings: vec![],
        }
    }

    fn id(name: &str) -> Expr {
        Expr::Identifier(name.into())
    }
    fn qid(qual: &str, col: &str) -> Expr {
        Expr::CompoundIdentifier(vec![qual.into(), col.into()])
    }
    fn lit(v: i64) -> Expr {
        Expr::Literal(Literal::Int(v))
    }
    fn eq(l: Expr, r: Expr) -> Expr {
        Expr::Binary {
            left: Box::new(l),
            op: BinaryOp::Eq,
            right: Box::new(r),
        }
    }
    fn and(l: Expr, r: Expr) -> Expr {
        Expr::Binary {
            left: Box::new(l),
            op: BinaryOp::And,
            right: Box::new(r),
        }
    }
    fn or(l: Expr, r: Expr) -> Expr {
        Expr::Binary {
            left: Box::new(l),
            op: BinaryOp::Or,
            right: Box::new(r),
        }
    }

    fn scopes() -> (Scope, Scope) {
        // Outer: table `o`, columns `o.k`, `o.v`. Inner: `inner_t.x`, `inner_t.k`.
        let outer = Scope::new()
            .with_table("o")
            .with_column("k")
            .with_column("v");
        let inner = Scope::new()
            .with_table("inner_t")
            .with_column("x")
            .with_column("k");
        (outer, inner)
    }

    #[test]
    fn uncorrelated_exists() {
        let mut sub = empty_select();
        sub.projection = vec![SelectItem::Expr {
            expr: lit(1),
            alias: None,
        }];
        sub.selection = Some(eq(qid("inner_t", "x"), lit(42)));
        let e = Expr::Exists {
            subquery: Box::new(sub),
            negated: false,
        };
        let (outer, inner) = scopes();
        let info = classify_sublink(&e, &outer, &inner).unwrap();
        assert_eq!(info.kind, SubLinkKind::Existential { negated: false });
        assert_eq!(info.shape, CorrelationShape::Uncorrelated);
        assert!(info.correlated_refs.is_empty());
        assert!(info.simple_body);
        assert!(!info.aggregate_in_projection);
    }

    #[test]
    fn correlated_exists_only_equality_qualified() {
        let mut sub = empty_select();
        sub.projection = vec![SelectItem::Expr {
            expr: lit(1),
            alias: None,
        }];
        // WHERE inner_t.k = o.k AND inner_t.x > 0
        sub.selection = Some(and(
            eq(qid("inner_t", "k"), qid("o", "k")),
            Expr::Binary {
                left: Box::new(qid("inner_t", "x")),
                op: BinaryOp::Gt,
                right: Box::new(lit(0)),
            },
        ));
        let e = Expr::Exists {
            subquery: Box::new(sub),
            negated: false,
        };
        let (outer, inner) = scopes();
        let info = classify_sublink(&e, &outer, &inner).unwrap();
        assert_eq!(info.shape, CorrelationShape::OnlyEquality);
        assert_eq!(info.correlated_refs.len(), 1);
        assert_eq!(info.correlated_refs[0].column, "k");
        assert_eq!(info.correlated_refs[0].qualifier.as_deref(), Some("o"));
        assert_eq!(info.equality_pairs.len(), 1);
        // inner side of the equality should be `inner_t.k`
        match &info.equality_pairs[0].inner_expr {
            Expr::CompoundIdentifier(parts) => {
                assert_eq!(parts, &vec!["inner_t".to_string(), "k".to_string()])
            }
            other => panic!("inner side not as expected: {:?}", other),
        }
    }

    #[test]
    fn correlated_exists_complex_via_or() {
        let mut sub = empty_select();
        sub.projection = vec![SelectItem::Expr {
            expr: lit(1),
            alias: None,
        }];
        // WHERE inner_t.k = o.k OR inner_t.x > 0
        sub.selection = Some(or(
            eq(qid("inner_t", "k"), qid("o", "k")),
            Expr::Binary {
                left: Box::new(qid("inner_t", "x")),
                op: BinaryOp::Gt,
                right: Box::new(lit(0)),
            },
        ));
        let e = Expr::Exists {
            subquery: Box::new(sub),
            negated: false,
        };
        let (outer, inner) = scopes();
        let info = classify_sublink(&e, &outer, &inner).unwrap();
        assert_eq!(info.shape, CorrelationShape::Complex);
        // equality pairs cleared on Complex
        assert!(info.equality_pairs.is_empty());
        // outer ref still recorded for diagnostics
        assert!(!info.correlated_refs.is_empty());
    }

    #[test]
    fn correlated_via_function_arg_is_complex() {
        let mut sub = empty_select();
        sub.projection = vec![SelectItem::Expr {
            expr: lit(1),
            alias: None,
        }];
        // WHERE LOWER(o.k) = inner_t.k
        sub.selection = Some(eq(
            Expr::Function {
                name: "lower".into(),
                args: vec![qid("o", "k")],
                distinct: false,
            },
            qid("inner_t", "k"),
        ));
        let e = Expr::Exists {
            subquery: Box::new(sub),
            negated: false,
        };
        let (outer, inner) = scopes();
        let info = classify_sublink(&e, &outer, &inner).unwrap();
        // `LOWER(o.k)` is not a bare outer column → mixed side → Complex.
        assert_eq!(info.shape, CorrelationShape::Complex);
    }

    #[test]
    fn scalar_subquery_with_aggregate() {
        // (SELECT MAX(x) FROM inner_t WHERE inner_t.k = o.k)
        let mut sub = empty_select();
        sub.projection = vec![SelectItem::Expr {
            expr: Expr::Function {
                name: "max".into(),
                args: vec![id("x")],
                distinct: false,
            },
            alias: None,
        }];
        sub.selection = Some(eq(qid("inner_t", "k"), qid("o", "k")));
        let e = Expr::Subquery(Box::new(sub));
        let (outer, inner) = scopes();
        let info = classify_sublink(&e, &outer, &inner).unwrap();
        assert_eq!(info.kind, SubLinkKind::Scalar { aggregated: true });
        assert_eq!(info.shape, CorrelationShape::OnlyEquality);
        assert!(info.aggregate_in_projection);
    }

    #[test]
    fn in_subquery_uncorrelated() {
        // x IN (SELECT k FROM inner_t WHERE x > 0)
        let mut sub = empty_select();
        sub.projection = vec![SelectItem::Expr {
            expr: id("k"),
            alias: None,
        }];
        sub.selection = Some(Expr::Binary {
            left: Box::new(id("x")),
            op: BinaryOp::Gt,
            right: Box::new(lit(0)),
        });
        let e = Expr::InSubquery {
            expr: Box::new(id("k")), // outer-side expr ignored by classifier
            subquery: Box::new(sub),
            negated: false,
        };
        let (outer, inner) = scopes();
        let info = classify_sublink(&e, &outer, &inner).unwrap();
        assert_eq!(info.kind, SubLinkKind::Quantified { negated: false });
        assert_eq!(info.shape, CorrelationShape::Uncorrelated);
    }

    #[test]
    fn ambiguous_unqualified_column_treated_as_inner() {
        // Both outer scope and inner scope have a column `k` (see `scopes()`).
        // An unqualified `k` inside the subquery should be inner, not a
        // correlation.
        let mut sub = empty_select();
        sub.projection = vec![SelectItem::Expr {
            expr: lit(1),
            alias: None,
        }];
        sub.selection = Some(eq(id("k"), lit(7)));
        let e = Expr::Exists {
            subquery: Box::new(sub),
            negated: false,
        };
        let (outer, inner) = scopes();
        let info = classify_sublink(&e, &outer, &inner).unwrap();
        assert_eq!(info.shape, CorrelationShape::Uncorrelated);
    }

    #[test]
    fn non_subquery_returns_none() {
        let e = lit(1);
        let (outer, inner) = scopes();
        assert!(classify_sublink(&e, &outer, &inner).is_none());
    }

    #[test]
    fn simple_body_rejects_join() {
        let mut sub = empty_select();
        sub.joins.push(crate::parser::Join {
            table: TableRef {
                name: "other".into(),
                alias: None,
                system_time: None,
                index_hints: vec![],
                partitions: vec![],
                column_aliases: Vec::new(),
                generate_series: None,
            },
            join_type: crate::parser::JoinType::Inner,
            condition: None,
            using_columns: Vec::new(),
            natural: false,
            derived: None,
            straight: false,
            lateral: false,
        });
        sub.projection = vec![SelectItem::Expr {
            expr: lit(1),
            alias: None,
        }];
        assert!(!is_simple_body(&sub));
    }

    #[test]
    fn simple_body_rejects_aggregate_projection() {
        let mut sub = empty_select();
        sub.projection = vec![SelectItem::Expr {
            expr: Expr::Function {
                name: "count".into(),
                args: vec![Expr::Wildcard],
                distinct: false,
            },
            alias: None,
        }];
        assert!(!is_simple_body(&sub));
    }
}

pub(crate) fn expr_is_outer_column(expr: &Expr, outer: &Scope, inner: &Scope) -> bool {
    match expr {
        Expr::Identifier(name) => {
            let lc = name.to_ascii_lowercase();
            // Unqualified outer reference only when outer has the
            // column and inner does NOT.
            outer.columns.contains(&lc) && !inner.columns.contains(&lc)
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let q = parts[parts.len() - 2].to_ascii_lowercase();
            outer.tables.contains(&q) && !inner.tables.contains(&q)
        }
        _ => false,
    }
}
