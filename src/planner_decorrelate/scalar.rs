//! Scalar-subquery decorrelation. Rewrites
//!
//! ```sql
//! SELECT t1.*, (SELECT agg(t2.x) FROM t2 WHERE t2.k = t1.k) AS s FROM t1
//! ```
//!
//! into
//!
//! ```sql
//! SELECT t1.*, COALESCE_OR_NULL(__decor_0.__scalar_0) AS s
//! FROM t1
//! LEFT JOIN (SELECT t2.k AS __corr_key_0, agg(t2.x) AS __scalar_0
//!            FROM t2 GROUP BY t2.k) __decor_0
//!        ON __decor_0.__corr_key_0 = t1.k
//! ```
//!
//! Trading N×M per-outer-row subquery evaluations for a single grouped
//! scan + hash join — the canonical OLAP decorrelation. Conservative
//! gates ensure SQL semantics are preserved (COUNT bug fix, only-equality
//! correlation, single-projection, no LIMIT/ORDER BY/DISTINCT).
//!
//! This rewriter is one-shot per call: deeply-nested scalar subqueries
//! get only their outermost level rewritten; subsequent calls (or
//! recursive planning of the resulting derived) take care of the rest.

use crate::parser::boolean::split_conjuncts_owned;

use crate::parser::{
    BinaryOp, Expr, Join, JoinType, Literal, Select, SelectItem, SubQuery, TableRef,
};
use crate::storage::{Database, TableSchema};

use super::sublink::{
    classify_sublink, is_aggregate_function, CorrelatedRef, CorrelationShape, EqualityPair, Scope,
    SubLinkInfo, SubLinkKind,
};

/// Entry point. Walks `select` and rewrites every eligible correlated
/// scalar subquery into a LEFT JOIN against a pre-aggregated derived
/// table. Returns the number of subqueries decorrelated.
///
/// Idempotent: a second call on an already-rewritten `select` finds
/// nothing to do (the LEFT JOIN's derived no longer contains an
/// `Expr::Subquery`).
pub fn decorrelate_scalar_subqueries(db: &Database, select: &mut Select) -> usize {
    // Validate the original correlated references before lifting them out of
    // a grouped query. Derived inputs later become CTE scans and lose their
    // grouping-key provenance, so preserve the proven group-constant result
    // explicitly with the same hidden-first aggregate used by GROUP BY.
    let grouped = !select.group_by.is_empty() || !select.grouping_sets.is_empty();
    if grouped && !body_contains_subquery(select) {
        return 0;
    }
    if grouped && crate::planner::check_only_full_group_by(db, select).is_err() {
        return 0;
    }
    // Derived inputs and their joins are ordinary BatchOperator subplans now,
    // so multi-table outers can use one grouped subplan plus a hash join rather
    // than invoking an Apply body once per distinct correlation key.
    let outer_scope = crate::planner_decorrelate::select_scope(db, select);
    let outer_sources = collect_outer_base_sources(select);
    let outer_selection = select.selection.clone();
    let mut state = RewriteState {
        db,
        outer_scope,
        outer_sources,
        outer_selection,
        new_joins: Vec::new(),
        next_id: select.joins.len() as u32, // start IDs after existing joins
        rewrote: 0,
        aggregate_replacements: grouped,
    };

    for item in &mut select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            visit_expr(&mut state, expr);
        }
    }
    if let Some(selection) = &mut select.selection {
        state.aggregate_replacements = false;
        visit_expr(&mut state, selection);
    }
    state.aggregate_replacements = grouped;
    if let Some(having) = &mut select.having {
        visit_expr(&mut state, having);
    }
    for ob in &mut select.order_by {
        visit_expr(&mut state, &mut ob.expr);
    }

    select.joins.extend(state.new_joins);
    state.rewrote
}

struct RewriteState<'a> {
    db: &'a Database,
    outer_scope: Scope,
    outer_sources: Vec<TableRef>,
    outer_selection: Option<Expr>,
    new_joins: Vec<Join>,
    next_id: u32,
    rewrote: usize,
    aggregate_replacements: bool,
}

fn visit_expr(state: &mut RewriteState, expr: &mut Expr) {
    crate::util::stack::ensure_stack(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        || visit_expr_checked(state, expr),
    )
}

fn visit_expr_checked(state: &mut RewriteState, expr: &mut Expr) {
    if matches!(expr, Expr::Subquery(_)) {
        // This pass owns the outer expression, never the nested query body.
        try_rewrite_scalar(state, expr);
        return;
    }
    let aggregate_replacements = state.aggregate_replacements;
    if matches!(expr, Expr::Function { name, .. } if crate::planner::is_aggregate_function(name)) {
        state.aggregate_replacements = false;
    }
    crate::parser::visit::for_each_expr_child_mut(expr, |child| visit_expr(state, child));
    state.aggregate_replacements = aggregate_replacements;
}

/// Try to decorrelate `expr` in place if it's a `Expr::Subquery` matching
/// every eligibility rule. Returns `true` if rewritten.
fn try_rewrite_scalar(state: &mut RewriteState, expr: &mut Expr) -> bool {
    let sub_box = match expr {
        Expr::Subquery(b) => b,
        _ => return false,
    };
    // Refuse to descend into bodies that themselves still contain a
    // subquery node — we'd need a multi-pass driver to handle those
    // and the safety analysis below assumes a flat body.
    if body_contains_subquery(sub_box) {
        return false;
    }

    let inner_scope = crate::planner_decorrelate::select_scope(state.db, sub_box);

    // Re-package as &Expr::Subquery for the classifier.
    let probe_expr = Expr::Subquery(sub_box.clone());
    let info: SubLinkInfo = match classify_sublink(&probe_expr, &state.outer_scope, &inner_scope) {
        Some(i) => i,
        None => return false,
    };

    // Hard gates.
    let SubLinkKind::Scalar { aggregated: true } = info.kind else {
        return false;
    };
    if info.shape != CorrelationShape::OnlyEquality {
        return false;
    }
    // For scalar decorrelation the body's structural shape rules differ
    // from `is_simple_body` (which is tuned for the Semi/Anti rewriter
    // and rejects aggregates in projection — exactly what scalar
    // decorrelation *requires*). Apply our own gate here.
    if !is_simple_body_for_scalar(sub_box) {
        return false;
    }
    if info.equality_pairs.is_empty() {
        return false;
    }
    if sub_box.projection.len() != 1 {
        return false;
    }
    // Aggregate must be pure (no nested aggregate, no window). Trust
    // `info.aggregate_in_projection`.
    if !info.aggregate_in_projection {
        return false;
    }
    // Accept a bare aggregate or one non-COUNT aggregate wrapped in
    // NULL-propagating literal arithmetic. JOIN-position derived inputs are
    // lowered to CteScan leaves before physical planning, so these rewrites no
    // longer force the outer query onto the materialize-everything fallback.
    let projection_expr = match &sub_box.projection[0] {
        SelectItem::Expr { expr, .. } => expr.clone(),
        _ => return false,
    };
    if !is_decorrelatable_projection_expr(&projection_expr) {
        return false;
    }

    // Build the rewrite. `id` is unique per derived so column-name
    // collisions never happen even if the outer SELECT pulls in
    // several decorrelated subqueries; we thread it into both the
    // table alias and the per-key column names.
    let id = state.next_id;
    state.next_id += 1;
    let derived_alias = format!("__decor_sub_{id}");
    let scalar_alias = format!("__scalar_{id}");

    let mut rewrite = match build_decorrelation(
        sub_box,
        &info,
        &derived_alias,
        &scalar_alias,
        &projection_expr,
        id,
    ) {
        Some(r) => r,
        None => return false,
    };
    add_outer_key_domain(state, &info.equality_pairs, id, &mut rewrite.derived_join);
    *expr = if state.aggregate_replacements {
        Expr::Function {
            name: crate::parser::HIDDEN_FIRST_VALUE_FN.to_string(),
            args: vec![rewrite.replacement_expr],
            distinct: false,
        }
    } else {
        rewrite.replacement_expr
    };
    state.new_joins.push(rewrite.derived_join);
    state.rewrote += 1;
    true
}

struct Decorrelation {
    replacement_expr: Expr,
    derived_join: Join,
}

fn build_decorrelation(
    subquery: &Select,
    info: &SubLinkInfo,
    derived_alias: &str,
    scalar_alias: &str,
    projection_expr: &Expr,
    derived_id: u32,
) -> Option<Decorrelation> {
    // 1) Strip the correlated equalities from the subquery's WHERE.
    let remaining_where =
        strip_lifted_equalities(subquery.selection.as_ref(), &info.equality_pairs);

    // 2) Build the derived projection:
    //    [inner_expr_0 AS __corr_key_<id>_0, ..., agg AS __scalar_<id>]
    let mut derived_projection: Vec<SelectItem> = info
        .equality_pairs
        .iter()
        .enumerate()
        .map(|(idx, pair)| SelectItem::Expr {
            expr: pair.inner_expr.clone(),
            alias: Some(corr_key_alias(derived_id, idx)),
        })
        .collect();
    derived_projection.push(SelectItem::Expr {
        expr: projection_expr.clone(),
        alias: Some(scalar_alias.to_string()),
    });

    // 3) GROUP BY all the inner-side expressions of the lifted equalities.
    let group_by: Vec<Expr> = info
        .equality_pairs
        .iter()
        .map(|p| p.inner_expr.clone())
        .collect();

    // 4) The derived body inherits FROM (single table per simple_body).
    let mut derived = subquery.clone();
    derived.projection = derived_projection;
    derived.selection = remaining_where;
    for join in &mut derived.joins {
        join.condition = strip_lifted_equalities(join.condition.as_ref(), &info.equality_pairs);
    }
    derived.group_by = group_by;
    derived.order_by.clear();
    derived.limit = None;
    derived.offset = None;
    derived.distinct = false;
    derived.for_update = false;
    derived.named_windows.clear();
    derived.query_hints.clear();
    derived.hint_warnings.clear();

    // 5) Build the LEFT JOIN. The right side is the derived SELECT,
    //    wrapped in a TableRef whose alias is `derived_alias`.
    let join_condition = build_join_condition(&info.equality_pairs, derived_alias, derived_id);
    let derived_join = Join {
        table: TableRef {
            name: derived_alias.to_string(),
            alias: Some(derived_alias.to_string()),
            system_time: None,
            index_hints: vec![],
            partitions: vec![],
            column_aliases: Vec::new(),
            generate_series: None,
        },
        join_type: JoinType::Left,
        condition: Some(join_condition),
        using_columns: Vec::new(),
        natural: false,
        derived: Some(SubQuery::Select(Box::new(derived))),
        straight: false,
        lateral: false,
    };

    // 6) Build the outer-side replacement expression. COUNT needs a
    //    COALESCE(_, 0) wrap because LEFT JOIN gives NULL where the
    //    original empty-group `COUNT(*)` would give 0.
    let col_ref =
        Expr::CompoundIdentifier(vec![derived_alias.to_string(), scalar_alias.to_string()]);
    let replacement_expr = if is_count_aggregate(projection_expr) {
        Expr::Function {
            name: "coalesce".into(),
            args: vec![col_ref, Expr::Literal(Literal::Int(0))],
            distinct: false,
        }
    } else {
        col_ref
    };

    Some(Decorrelation {
        replacement_expr,
        derived_join,
    })
}

/// Restrict a decorrelated aggregate to the correlation-key domain that can
/// survive deterministic predicates on one outer base table.  The domain is
/// represented as an ordinary `SELECT key...` derived input, so the
/// executor/CBO can use the same scan, materialize, join and spill operators
/// as every other query graph.
///
/// This is deliberately a semantic superset of the final outer rows: only
/// conjuncts that reference the correlation-key table alone are copied.  A
/// later multi-table predicate may shrink the outer rows further, but can
/// never introduce a key absent from this domain. A semi join prevents duplicate
/// outer keys from multiplying rows before MIN/SUM/AVG/etc. are updated.
fn add_outer_key_domain(
    state: &RewriteState<'_>,
    pairs: &[EqualityPair],
    derived_id: u32,
    derived_join: &mut Join,
) {
    let Some(selection) = state.outer_selection.as_ref() else {
        return;
    };
    let Some((source, schema)) = resolve_common_outer_source(state, pairs) else {
        return;
    };

    let mut conjuncts = Vec::new();
    split_conjuncts_owned(selection.clone(), &mut conjuncts);
    let mut pushed = conjuncts
        .into_iter()
        .filter(|expr| deterministic_expr_on_source(state.db, expr, source, &schema))
        .collect::<Vec<_>>();
    if pushed.is_empty() {
        return;
    }

    let source_alias = format!("__corr_domain_src_{derived_id}");
    for expr in &mut pushed {
        rewrite_source_qualifier(expr, source, &source_alias);
    }
    let domain_filter = combine_and(pushed);
    let domain_alias = format!("__corr_domain_{derived_id}");
    let domain_projection = pairs
        .iter()
        .enumerate()
        .map(|(index, pair)| SelectItem::Expr {
            expr: Expr::CompoundIdentifier(vec![source_alias.clone(), pair.outer.column.clone()]),
            alias: Some(domain_key_alias(derived_id, index)),
        })
        .collect();
    let mut domain_source = source.clone();
    domain_source.alias = Some(source_alias);
    let domain = Select {
        table: Some(domain_source.name.clone()),
        from: vec![domain_source],
        projection: domain_projection,
        selection: domain_filter,
        ..Select::default()
    };

    let mut conditions = pairs.iter().enumerate().map(|(index, pair)| Expr::Binary {
        left: Box::new(pair.inner_expr.clone()),
        op: BinaryOp::Eq,
        right: Box::new(Expr::CompoundIdentifier(vec![
            domain_alias.clone(),
            domain_key_alias(derived_id, index),
        ])),
    });
    let Some(first) = conditions.next() else {
        return;
    };
    let condition = conditions.fold(first, |left, right| Expr::Binary {
        left: Box::new(left),
        op: BinaryOp::And,
        right: Box::new(right),
    });

    let Some(SubQuery::Select(derived)) = derived_join.derived.as_mut() else {
        return;
    };
    derived.joins.push(Join {
        table: TableRef {
            name: domain_alias.clone(),
            alias: Some(domain_alias),
            system_time: None,
            index_hints: Vec::new(),
            partitions: Vec::new(),
            column_aliases: Vec::new(),
            generate_series: None,
        },
        join_type: JoinType::Semi,
        condition: Some(condition),
        using_columns: Vec::new(),
        natural: false,
        derived: Some(SubQuery::Select(Box::new(domain))),
        straight: false,
        lateral: false,
    });
}

fn collect_outer_base_sources(select: &Select) -> Vec<TableRef> {
    let mut sources = Vec::new();
    for (index, source) in select.from.iter().enumerate() {
        if index == 0 && select.derived.is_some() {
            continue;
        }
        if source.generate_series.is_none() {
            sources.push(source.clone());
        }
    }
    for join in &select.joins {
        if join.derived.is_none() && join.table.generate_series.is_none() {
            sources.push(join.table.clone());
        }
    }
    sources
}

fn resolve_common_outer_source<'a>(
    state: &'a RewriteState<'_>,
    pairs: &[EqualityPair],
) -> Option<(&'a TableRef, std::sync::Arc<TableSchema>)> {
    let first = pairs.first()?;
    let mut matches = state.outer_sources.iter().filter_map(|source| {
        let schema = state.db.table(&source.name).ok()?;
        if correlated_ref_matches_source(&first.outer, source, &schema) {
            Some((source, schema))
        } else {
            None
        }
    });
    let candidate = matches.next()?;
    // An unqualified correlation key must resolve to exactly one source.
    if first.outer.qualifier.is_none() && matches.next().is_some() {
        return None;
    }
    if pairs
        .iter()
        .all(|pair| correlated_ref_matches_source(&pair.outer, candidate.0, &candidate.1))
    {
        Some(candidate)
    } else {
        None
    }
}

fn correlated_ref_matches_source(
    correlated: &CorrelatedRef,
    source: &TableRef,
    schema: &TableSchema,
) -> bool {
    schema
        .columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(&correlated.column))
        && correlated
            .qualifier
            .as_deref()
            .is_none_or(|qualifier| source_matches_qualifier(source, qualifier))
}

fn source_matches_qualifier(source: &TableRef, qualifier: &str) -> bool {
    source
        .alias
        .as_deref()
        .is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier))
        || source.name.eq_ignore_ascii_case(qualifier)
        || source
            .name
            .rsplit('.')
            .next()
            .is_some_and(|bare| bare.eq_ignore_ascii_case(qualifier))
}

fn deterministic_expr_on_source(
    db: &Database,
    expr: &Expr,
    source: &TableRef,
    schema: &TableSchema,
) -> bool {
    let column = |name: &str| {
        schema
            .columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(name))
    };
    match expr {
        Expr::Identifier(name) => column(name),
        Expr::CompoundIdentifier(parts) => {
            let Some(name) = parts.last() else {
                return false;
            };
            let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
            column(name) && qualifier.is_none_or(|q| source_matches_qualifier(source, q))
        }
        Expr::Literal(_) | Expr::BoundValue(_) | Expr::Placeholder(_) => true,
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => deterministic_expr_on_source(db, expr, source, schema),
        Expr::Binary { left, right, .. } => {
            deterministic_expr_on_source(db, left, source, schema)
                && deterministic_expr_on_source(db, right, source, schema)
        }
        Expr::InList { expr, list, .. } => {
            deterministic_expr_on_source(db, expr, source, schema)
                && list
                    .iter()
                    .all(|item| deterministic_expr_on_source(db, item, source, schema))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            deterministic_expr_on_source(db, expr, source, schema)
                && deterministic_expr_on_source(db, low, source, schema)
                && deterministic_expr_on_source(db, high, source, schema)
        }
        Expr::Like { expr, pattern, .. } => {
            deterministic_expr_on_source(db, expr, source, schema)
                && deterministic_expr_on_source(db, pattern, source, schema)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_deref()
                .is_none_or(|expr| deterministic_expr_on_source(db, expr, source, schema))
                && conditions
                    .iter()
                    .all(|expr| deterministic_expr_on_source(db, expr, source, schema))
                && results
                    .iter()
                    .all(|expr| deterministic_expr_on_source(db, expr, source, schema))
                && else_result
                    .as_deref()
                    .is_none_or(|expr| deterministic_expr_on_source(db, expr, source, schema))
        }
        Expr::Tuple(items) => items
            .iter()
            .all(|expr| deterministic_expr_on_source(db, expr, source, schema)),
        Expr::InSubquery { expr, subquery, .. } => {
            deterministic_expr_on_source(db, expr, source, schema)
                && deterministic_single_source_subquery(db, subquery)
        }
        // Function volatility metadata is not yet exposed to this AST pass;
        // conservatively leave every function in the outer query only.
        Expr::Function { .. }
        | Expr::WindowFunction { .. }
        | Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::QuantifiedComparison { .. }
        | Expr::Wildcard
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Grouping(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. } => false,
    }
}

/// A duplicated domain filter must neither depend on the enclosing row nor
/// evaluate volatile expressions twice. Restrict IN bodies to independently
/// bound, deterministic single-source SELECTs; unsupported shapes simply keep
/// the wider (but correct) correlation domain.
fn deterministic_single_source_subquery(db: &Database, select: &Select) -> bool {
    if select.from.len() != 1
        || !select.joins.is_empty()
        || select.derived.is_some()
        || select.values.is_some()
        || select.json_table.is_some()
        || !select.group_by.is_empty()
        || !select.grouping_sets.is_empty()
        || select.having.is_some()
        || select.for_update
        || select.lock_clause.is_some()
        || select.sql_calc_found_rows
        || !select.named_windows.is_empty()
        || select.from[0].generate_series.is_some()
        || crate::planner::validate_select_column_references(db, select).is_err()
    {
        return false;
    }
    let source = &select.from[0];
    let Ok(schema) = db.table(&source.name) else {
        return false;
    };
    select.projection.iter().all(|item| match item {
        SelectItem::Expr { expr, .. } => deterministic_expr_on_source(db, expr, source, &schema),
        _ => false,
    }) && select.selection.as_ref().is_none_or(|expr| deterministic_expr_on_source(db, expr, source, &schema))
        && select.order_by.iter().all(|order| deterministic_expr_on_source(db, &order.expr, source, &schema))
        // LIMIT without a total ordering need not choose the same row set on
        // two independently planned executions, even on a stable snapshot.
        && select.limit.is_none()
        && select.offset.is_none()
}

fn rewrite_source_qualifier(expr: &mut Expr, source: &TableRef, replacement: &str) {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier_index = parts.len() - 2;
            if source_matches_qualifier(source, &parts[qualifier_index]) {
                parts[qualifier_index] = replacement.to_string();
            }
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => rewrite_source_qualifier(expr, source, replacement),
        Expr::Binary { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            rewrite_source_qualifier(left, source, replacement);
            rewrite_source_qualifier(right, source, replacement);
        }
        Expr::InSubquery { expr, .. } => rewrite_source_qualifier(expr, source, replacement),
        Expr::InList { expr, list, .. } => {
            rewrite_source_qualifier(expr, source, replacement);
            for item in list {
                rewrite_source_qualifier(item, source, replacement);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_source_qualifier(expr, source, replacement);
            rewrite_source_qualifier(low, source, replacement);
            rewrite_source_qualifier(high, source, replacement);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(expr) = operand {
                rewrite_source_qualifier(expr, source, replacement);
            }
            for expr in conditions.iter_mut().chain(results.iter_mut()) {
                rewrite_source_qualifier(expr, source, replacement);
            }
            if let Some(expr) = else_result {
                rewrite_source_qualifier(expr, source, replacement);
            }
        }
        Expr::Tuple(items) | Expr::Function { args: items, .. } => {
            for expr in items {
                rewrite_source_qualifier(expr, source, replacement);
            }
        }
        _ => {}
    }
}

fn combine_and(mut expressions: Vec<Expr>) -> Option<Expr> {
    let first = expressions.pop()?;
    Some(
        expressions
            .into_iter()
            .fold(first, |right, left| Expr::Binary {
                left: Box::new(left),
                op: BinaryOp::And,
                right: Box::new(right),
            }),
    )
}

fn domain_key_alias(derived_id: u32, index: usize) -> String {
    format!("__domain_key_{derived_id}_{index}")
}

fn corr_key_alias(derived_id: u32, idx: usize) -> String {
    format!("__corr_key_{derived_id}_{idx}")
}

/// Build `derived_alias.__corr_key_<id>_0 = <outer.k> [AND ...]`.
fn build_join_condition(pairs: &[EqualityPair], derived_alias: &str, derived_id: u32) -> Expr {
    fn outer_to_expr(outer: &CorrelatedRef) -> Expr {
        match &outer.qualifier {
            Some(q) => Expr::CompoundIdentifier(vec![q.clone(), outer.column.clone()]),
            None => Expr::Identifier(outer.column.clone()),
        }
    }

    let mut parts = pairs.iter().enumerate().map(|(idx, pair)| Expr::Binary {
        left: Box::new(Expr::CompoundIdentifier(vec![
            derived_alias.to_string(),
            corr_key_alias(derived_id, idx),
        ])),
        op: BinaryOp::Eq,
        right: Box::new(outer_to_expr(&pair.outer)),
    });
    let first = parts.next().expect("at least one equality pair");
    parts.fold(first, |acc, next| Expr::Binary {
        left: Box::new(acc),
        op: BinaryOp::And,
        right: Box::new(next),
    })
}

/// Walks the WHERE conjuncts and drops every top-level
/// `outer.k = inner_expr` (or reversed) that appears in `lifted`.
/// The remaining conjuncts are re-AND-ed; returns `None` if nothing
/// remains.
fn strip_lifted_equalities(where_expr: Option<&Expr>, lifted: &[EqualityPair]) -> Option<Expr> {
    let where_expr = where_expr?;
    let mut conjuncts: Vec<Expr> = Vec::new();
    split_conjuncts_owned(where_expr.clone(), &mut conjuncts);
    let kept: Vec<Expr> = conjuncts
        .into_iter()
        .filter(|c| !is_lifted_equality(c, lifted))
        .collect();
    if kept.is_empty() {
        return None;
    }
    let mut it = kept.into_iter();
    let first = it.next().unwrap();
    Some(it.fold(first, |acc, n| Expr::Binary {
        left: Box::new(acc),
        op: BinaryOp::And,
        right: Box::new(n),
    }))
}

fn is_lifted_equality(conj: &Expr, lifted: &[EqualityPair]) -> bool {
    let Expr::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    } = conj
    else {
        return false;
    };
    lifted.iter().any(|pair| {
        equality_matches(&pair.outer, &pair.inner_expr, left, right)
            || equality_matches(&pair.outer, &pair.inner_expr, right, left)
    })
}

fn equality_matches(
    outer: &CorrelatedRef,
    inner_expr: &Expr,
    side_a: &Expr,
    side_b: &Expr,
) -> bool {
    is_outer_ref(side_a, outer) && exprs_equal(side_b, inner_expr)
}

fn is_outer_ref(expr: &Expr, target: &CorrelatedRef) -> bool {
    match expr {
        Expr::Identifier(name) => {
            target.qualifier.is_none() && name.eq_ignore_ascii_case(&target.column)
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = parts[parts.len() - 2].to_ascii_lowercase();
            let column = parts[parts.len() - 1].to_ascii_lowercase();
            target
                .qualifier
                .as_deref()
                .map(|q| q == qualifier)
                .unwrap_or(false)
                && column == target.column
        }
        _ => false,
    }
}

/// Structural equality between two AST `Expr` nodes, used to match the
/// inner side of an equality against a lifted pair's `inner_expr`. We
/// can lean on `Expr: PartialEq` (the parser derives it) and ignore
/// case for identifiers; the parser preserves user case in
/// `Identifier`/`CompoundIdentifier`, so we normalise.
fn exprs_equal(a: &Expr, b: &Expr) -> bool {
    match (a, b) {
        (Expr::Identifier(la), Expr::Identifier(lb)) => la.eq_ignore_ascii_case(lb),
        (Expr::CompoundIdentifier(la), Expr::CompoundIdentifier(lb)) => {
            la.len() == lb.len()
                && la
                    .iter()
                    .zip(lb.iter())
                    .all(|(x, y)| x.eq_ignore_ascii_case(y))
        }
        _ => a == b,
    }
}

/// True when `expr` is `COUNT(*)` or `COUNT(<anything>)`. The LEFT JOIN
/// fallback returns NULL on an empty match, but the original subquery
/// would have returned 0 for COUNT; this is the well-known "count bug"
/// of correlated subquery unnesting.
fn is_count_aggregate(expr: &Expr) -> bool {
    matches!(expr, Expr::Function { name, .. } if name.eq_ignore_ascii_case("count"))
}

/// True when `expr` is exactly `agg(...)` with no enclosing arithmetic
/// or function. Used as the current gate for scalar decorrelation —
/// see the call site for the policy reason this stays bare-only.
#[cfg(test)]
fn is_bare_aggregate_call(expr: &Expr) -> bool {
    matches!(expr, Expr::Function { name, distinct: _, .. } if is_aggregate_function(name))
}

/// True when `expr` mentions a COUNT aggregate anywhere in its tree.
/// Used as a hard gate before we accept a wrapped-aggregate projection:
/// `lit * COUNT(...)` would inherit the count bug from the wrapped
/// aggregate even though the outer operator preserves NULL.
fn expr_contains_count(expr: &Expr) -> bool {
    crate::parser::visit::any_expr_matching(
        expr,
        |node| matches!(node, Expr::Function { name, .. } if name.eq_ignore_ascii_case("count")),
        crate::parser::visit::scalar_expr_children,
    )
}

/// True when `expr` is decorrelatable as a scalar-subquery projection:
/// either a bare `agg(...)` call, or a single non-COUNT aggregate
/// wrapped by null-propagating arithmetic against literals
/// (e.g. `0.2 * AVG(x)`, `MAX(x) - 1`, `SUM(x) / 7.0`).
///
/// Wrapping is safe specifically when (1) every non-aggregate sub-tree
/// is NULL-propagating — i.e. NULL flows through the operator unchanged,
/// so a missing-group `NULL` from the LEFT JOIN matches the original
/// subquery's empty-group `NULL` — and (2) the inner aggregate is not
/// COUNT, whose empty-group result is `0` rather than NULL.
///
/// Bare COUNT is still accepted: `build_decorrelation` plants a
/// `COALESCE(__scalar, 0)` around it to recover the correct empty-group
/// answer. But COUNT wrapped in arithmetic can't be fixed that way —
/// `lit * NULL` would become `lit * 0` after the COALESCE, changing
/// semantics — so we refuse those.
fn is_decorrelatable_projection_expr(expr: &Expr) -> bool {
    // Bare COUNT → handled by the existing COALESCE(_, 0) wrap.
    if is_count_aggregate(expr) {
        return true;
    }
    // Any *other* COUNT usage (inside arithmetic, inside a function arg,
    // ...) re-introduces the count bug because the LEFT JOIN NULL would
    // need to be COALESCEd to 0 inside the wrap, which we don't do.
    if expr_contains_count(expr) {
        return false;
    }
    is_decorrelatable_inner(expr, false)
}

fn is_decorrelatable_inner(expr: &Expr, saw_aggregate: bool) -> bool {
    match expr {
        Expr::Function { name, .. } if is_aggregate_function(name) => {
            // Exactly one aggregate is allowed; nested aggregates are
            // already rejected by `info.aggregate_in_projection`, but
            // be defensive.
            !saw_aggregate
        }
        Expr::Binary { left, op, right } => {
            use crate::parser::BinaryOp::*;
            // Arithmetic is NULL-propagating: x op NULL = NULL.
            // Note: comparison and logical operators are NOT (they
            // collapse NULL into FALSE under MySQL semantics for
            // certain wrappers like IS NULL). Stay strict here.
            if !matches!(op, Add | Sub | Mul | Div | Mod) {
                return false;
            }
            let l_has_agg = expr_contains_aggregate(left);
            let r_has_agg = expr_contains_aggregate(right);
            if l_has_agg && r_has_agg {
                return false;
            }
            if l_has_agg {
                is_decorrelatable_inner(left, saw_aggregate) && is_pure_constant(right)
            } else if r_has_agg {
                is_pure_constant(left) && is_decorrelatable_inner(right, saw_aggregate)
            } else {
                false
            }
        }
        _ => false,
    }
}

#[allow(dead_code)]
fn expr_contains_aggregate(expr: &Expr) -> bool {
    crate::parser::visit::any_expr_matching(
        expr,
        |node| matches!(node, Expr::Function { name, .. } if super::sublink::is_aggregate_function(name)),
        crate::parser::visit::scalar_expr_children,
    )
}

/// True when `expr` is a constant value (literal, bound parameter, or
/// nested arithmetic over those) — no column references, no function
/// calls, no aggregates. Used to confirm a wrapped-aggregate projection
/// is `lit op agg` shape, not `col op agg`.
#[allow(dead_code)]
fn is_pure_constant(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Placeholder(_) | Expr::BoundValue(_) => true,
        Expr::Unary { expr, .. } => is_pure_constant(expr),
        Expr::Binary { left, right, .. } => is_pure_constant(left) && is_pure_constant(right),
        _ => false,
    }
}

/// Structural check tailored for scalar-subquery decorrelation. Unlike
/// [`super::sublink::is_simple_body`] (which is for Semi/Anti rewrites
/// and rejects aggregates), this one *requires* the projection to be
/// a single aggregate — that's enforced elsewhere — and just makes
/// sure nothing else gets in the way of pre-aggregating into a GROUP
/// BY derived table.
fn is_simple_body_for_scalar(s: &crate::parser::Select) -> bool {
    use super::sublink::projection_has_window;
    s.from.len() == 1
        && s.joins.iter().all(|join| {
            matches!(join.join_type, JoinType::Inner | JoinType::Cross)
                && join.derived.is_none()
                && !join.lateral
                && join.table.generate_series.is_none()
        })
        && s.group_by.is_empty()
        && s.having.is_none()
        && s.order_by.is_empty()
        && s.limit.is_none()
        && s.offset.is_none()
        && !s.distinct
        && !s.for_update
        && s.derived.is_none()
        && s.named_windows.is_empty()
        && !projection_has_window(&s.projection)
}

/// True when the subquery body still mentions another Expr::Subquery /
/// InSubquery / Exists anywhere. We refuse to decorrelate over such
/// nested constructs in a single pass.
fn body_contains_subquery(select: &Select) -> bool {
    crate::parser::visit::select_body_contains_subquery(select)
}

#[cfg(test)]
mod tests {
    // Most of the heavy testing is done as end-to-end SQL tests in
    // src/engine/executor/tests/scalar_decorrelate.rs so we exercise
    // the planner + executor + storage path. The unit tests below
    // pin down a handful of pure-AST behaviours that don't need a
    // database to be meaningful.

    use super::*;
    use crate::parser::{BinaryOp, Expr, Literal, TableRef};

    fn id(name: &str) -> Expr {
        Expr::Identifier(name.into())
    }
    fn qid(qual: &str, col: &str) -> Expr {
        Expr::CompoundIdentifier(vec![qual.into(), col.into()])
    }
    fn lit(v: i64) -> Expr {
        Expr::Literal(Literal::Int(v))
    }

    fn empty_inner_select() -> Select {
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

    #[test]
    fn strip_lifted_equalities_drops_matching_conjunct() {
        // WHERE inner_t.k = o.k AND inner_t.x > 0
        let conj = Expr::Binary {
            left: Box::new(Expr::Binary {
                left: Box::new(qid("inner_t", "k")),
                op: BinaryOp::Eq,
                right: Box::new(qid("o", "k")),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::Binary {
                left: Box::new(qid("inner_t", "x")),
                op: BinaryOp::Gt,
                right: Box::new(lit(0)),
            }),
        };
        let pair = EqualityPair {
            outer: CorrelatedRef {
                column: "k".into(),
                qualifier: Some("o".into()),
            },
            inner_expr: qid("inner_t", "k"),
        };
        let kept = strip_lifted_equalities(Some(&conj), &[pair]).unwrap();
        match kept {
            Expr::Binary {
                left,
                op: BinaryOp::Gt,
                right,
            } => {
                assert!(matches!(*left, Expr::CompoundIdentifier(_)));
                assert!(matches!(*right, Expr::Literal(Literal::Int(0))));
            }
            other => panic!("expected x > 0 remaining, got {:?}", other),
        }
    }

    #[test]
    fn strip_lifted_equalities_all_lifted_returns_none() {
        let conj = Expr::Binary {
            left: Box::new(qid("inner_t", "k")),
            op: BinaryOp::Eq,
            right: Box::new(qid("o", "k")),
        };
        let pair = EqualityPair {
            outer: CorrelatedRef {
                column: "k".into(),
                qualifier: Some("o".into()),
            },
            inner_expr: qid("inner_t", "k"),
        };
        assert!(strip_lifted_equalities(Some(&conj), &[pair]).is_none());
    }

    #[test]
    fn build_join_condition_multi_key() {
        let pairs = vec![
            EqualityPair {
                outer: CorrelatedRef {
                    column: "k".into(),
                    qualifier: Some("o".into()),
                },
                inner_expr: qid("inner_t", "k"),
            },
            EqualityPair {
                outer: CorrelatedRef {
                    column: "j".into(),
                    qualifier: Some("o".into()),
                },
                inner_expr: qid("inner_t", "j"),
            },
        ];
        let cond = build_join_condition(&pairs, "__decor_sub_0", 0);
        // Expect: __decor_sub_0.__corr_key_0_0 = o.k AND __decor_sub_0.__corr_key_0_1 = o.j
        match cond {
            Expr::Binary {
                op: BinaryOp::And, ..
            } => {}
            other => panic!("expected AND, got {:?}", other),
        }
    }

    #[test]
    fn body_contains_subquery_finds_nested() {
        let mut sub = empty_inner_select();
        sub.selection = Some(Expr::Exists {
            subquery: Box::new(empty_inner_select()),
            negated: false,
        });
        assert!(body_contains_subquery(&sub));
    }

    #[test]
    fn is_count_aggregate_recognises_count_only() {
        assert!(is_count_aggregate(&Expr::Function {
            name: "count".into(),
            args: vec![Expr::Wildcard],
            distinct: false,
        }));
        assert!(is_count_aggregate(&Expr::Function {
            name: "COUNT".into(),
            args: vec![id("x")],
            distinct: false,
        }));
        assert!(!is_count_aggregate(&Expr::Function {
            name: "max".into(),
            args: vec![id("x")],
            distinct: false,
        }));
    }

    #[test]
    fn is_bare_aggregate_requires_aggregate() {
        assert!(is_bare_aggregate_call(&Expr::Function {
            name: "max".into(),
            args: vec![id("x")],
            distinct: false,
        }));
        // Wrapped in arithmetic — not bare.
        let plus = Expr::Binary {
            left: Box::new(Expr::Function {
                name: "max".into(),
                args: vec![id("x")],
                distinct: false,
            }),
            op: BinaryOp::Add,
            right: Box::new(lit(1)),
        };
        assert!(!is_bare_aggregate_call(&plus));
        // Non-aggregate function.
        assert!(!is_bare_aggregate_call(&Expr::Function {
            name: "lower".into(),
            args: vec![id("x")],
            distinct: false,
        }));
    }

    /// `is_decorrelatable_projection_expr` is the deferred-gate variant
    /// kept dormant until `plan_select` learns to handle the derived
    /// joins it produces. Lock down its behaviour now so the policy
    /// stays correct when we flip the gate.
    #[test]
    fn decorrelatable_projection_accepts_bare_and_wrapped() {
        // Bare aggregate
        assert!(is_decorrelatable_projection_expr(&Expr::Function {
            name: "max".into(),
            args: vec![id("x")],
            distinct: false,
        }));
        // `lit * agg` and `agg / lit` — null-propagating wraps
        let mul = Expr::Binary {
            left: Box::new(Expr::Literal(Literal::Float(0.2))),
            op: BinaryOp::Mul,
            right: Box::new(Expr::Function {
                name: "avg".into(),
                args: vec![id("x")],
                distinct: false,
            }),
        };
        assert!(is_decorrelatable_projection_expr(&mul));
        let div = Expr::Binary {
            left: Box::new(Expr::Function {
                name: "sum".into(),
                args: vec![id("x")],
                distinct: false,
            }),
            op: BinaryOp::Div,
            right: Box::new(lit(7)),
        };
        assert!(is_decorrelatable_projection_expr(&div));
        // Bare COUNT — accepted (existing COALESCE wrap handles count bug).
        let bare_count = Expr::Function {
            name: "count".into(),
            args: vec![Expr::Wildcard],
            distinct: false,
        };
        assert!(is_decorrelatable_projection_expr(&bare_count));
        // Wrapped COUNT — reject; the COALESCE wrap can't reach inside.
        let count_wrap = Expr::Binary {
            left: Box::new(lit(1)),
            op: BinaryOp::Add,
            right: Box::new(Expr::Function {
                name: "count".into(),
                args: vec![Expr::Wildcard],
                distinct: false,
            }),
        };
        assert!(!is_decorrelatable_projection_expr(&count_wrap));
        // Non-aggregate function — reject.
        assert!(!is_decorrelatable_projection_expr(&Expr::Function {
            name: "lower".into(),
            args: vec![id("x")],
            distinct: false,
        }));
        // `col * agg` — column reference is not a pure constant, reject.
        let col_times_agg = Expr::Binary {
            left: Box::new(id("k")),
            op: BinaryOp::Mul,
            right: Box::new(Expr::Function {
                name: "avg".into(),
                args: vec![id("x")],
                distinct: false,
            }),
        };
        assert!(!is_decorrelatable_projection_expr(&col_times_agg));
    }
}
