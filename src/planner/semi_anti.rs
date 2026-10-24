//! IN/EXISTS lifting and semi/anti join eligibility.
use super::*;

// ============================================================
// Phase B #B1: IN/EXISTS/NOT EXISTS → Semi/Anti join rewrite
//
// Removes the per-outer-row subquery evaluation by lifting an
// uncorrelated `IN (SELECT y FROM t WHERE pred)` (or EXISTS variant)
// into an additional `Join { table: t, join_type: Semi }` against
// the outer SELECT. The join condition for IN is `outer = t.y`;
// for EXISTS we synthesize a trivially-true condition (the subquery
// already proves "any row exists in t after pred").
//
// Conservative gates ensure correctness without column-nullability
// info or correlation-handling: only "simple" subqueries (single
// FROM table, no joins/aggregates/group/having/order/limit/distinct/
// derived) and only when the subquery doesn't reference outer
// columns. NOT IN is intentionally skipped — its NULL semantics
// require either nullability metadata or a NULL-aware Anti probe,
// neither of which we wire up in this phase.
// ============================================================

pub(super) fn rewrite_subqueries_to_semi_anti(db: &Database, select: &mut Select) {
    // MySQL excludes both STRAIGHT_JOIN forms from semijoin conversion in
    // this query block. A lifted table would also alter the ordered source set.
    if select.straight_join || select.joins.iter().any(|join| join.straight) {
        return;
    }
    let Some(predicate) = select.selection.take() else {
        return;
    };
    let joins_before = select.joins.len();
    let mut conjuncts = Vec::new();
    split_conjuncts(&predicate, &mut conjuncts);
    let sibling_conjuncts = conjuncts.clone();
    let mut keep: Vec<Expr> = Vec::new();
    let mut subquery_extra_conjuncts: Vec<Expr> = Vec::new();
    for conj in conjuncts {
        match try_rewrite_subquery_predicate(
            db,
            select,
            &sibling_conjuncts,
            conj,
            &mut subquery_extra_conjuncts,
        ) {
            None => {} // rewritten in-place into select.joins
            Some(original) => keep.push(original),
        }
    }
    keep.extend(subquery_extra_conjuncts);
    select.selection = combine_conjuncts(keep);

    // A lifted Semi/Anti join drags its inner source's columns into the
    // outer row scope (the inner table is marked invisible for name
    // resolution, but the runtime row still carries its fields). Any
    // *other* unqualified reference to a column whose name the inner
    // source also defines — e.g. TPC-DS q14/q23's `ss_item_sk = i_item_sk`
    // sitting beside `ss_item_sk IN (SELECT ss_item_sk FROM cross_items)` —
    // was unambiguous before the rewrite but now resolves ambiguously at
    // execution time. Bind each such reference to its unique visible owner.
    if select.joins.len() > joins_before {
        qualify_collisions_after_semi_lift(db, select);
    }
}

/// Post-pass after `rewrite_subqueries_to_semi_anti` lifts one or more
/// Semi/Anti joins: qualify every bare identifier across the outer SELECT
/// that became ambiguous because a lifted inner source shares the column
/// name. Best-effort — bails if the scope can't be built.
fn qualify_collisions_after_semi_lift(db: &Database, select: &mut Select) {
    let Ok(scope) = build_column_scope(db, select, None) else {
        return;
    };
    let scope = ScopeWrap(&scope, true);
    if let Some(sel) = select.selection.as_mut() {
        qualify_idents_with_scope_inner(sel, &scope);
    }
    for join in &mut select.joins {
        if let Some(cond) = join.condition.as_mut() {
            qualify_idents_with_scope_inner(cond, &scope);
        }
    }
    if let Some(having) = select.having.as_mut() {
        qualify_idents_with_scope_inner(having, &scope);
    }
    // GROUP BY / ORDER BY / projection are intentionally left alone: the
    // collisions this pass repairs come from WHERE / JOIN-ON predicates,
    // and those clauses interact with already-bound ROLLUP key references
    // (`group_N`) whose expression identity must not be rewritten here.
}

/// Returns `None` if the predicate was rewritten (and the caller should
/// drop it from WHERE), or `Some(original)` to put it back.
fn try_rewrite_subquery_predicate(
    db: &Database,
    select: &mut Select,
    sibling_conjuncts: &[Expr],
    predicate: Expr,
    extra_conjuncts: &mut Vec<Expr>,
) -> Option<Expr> {
    if let Some(join) = build_or_exists_union_join(db, select, &predicate) {
        select.joins.push(join);
        return None;
    }
    if let Some(predicate) = build_or_exists_keyset_predicate(db, select, &predicate) {
        return Some(predicate);
    }
    // `(SELECT COUNT(*) FROM t [WHERE p]) > 0` is exactly `EXISTS (SELECT 1
    // FROM t [WHERE p])`. Recognise it and drive the same EXISTS → semi-join
    // rewrite, so a correlated existence-count (TPC-DS Q41's `item`
    // self-correlation) becomes a hash/INLJ semi join instead of a per-row
    // scalar subquery that re-scans the inner table for every outer row.
    if let Some(exists_select) = count_gt_zero_to_exists_select(&predicate) {
        if let Some((join, sub_where)) =
            build_exists_semi_join(db, select, &exists_select, JoinType::Semi)
        {
            select.joins.push(join);
            if let Some(extra) = sub_where {
                extra_conjuncts.push(extra);
            }
            return None;
        }
    }
    match predicate {
        // `x IN (uncorrelated simple subquery)` — Phase 3.
        //
        // mydb's planner has a specialised `IndexInLookup` path for the
        // shape `WHERE a = X AND b IN (SELECT b FROM small)` that uses a
        // composite `(a, b)` index to drive per-subquery-row probes —
        // generally cheaper than building a hash of the subquery and
        // then scanning the outer. We preserve that specialisation by
        // only rewriting when there's no AND-conjuncted equality on
        // another column of the same table that an index could use.
        // Everything else turns into a SemiJoin so the optimiser can
        // pick HashSemi / parallel probe based on cost.
        Expr::InSubquery {
            expr,
            subquery,
            negated: false,
        } => {
            if in_subquery_should_stay(db, select, sibling_conjuncts, &expr, &subquery) {
                Some(Expr::InSubquery {
                    expr,
                    subquery,
                    negated: false,
                })
            } else if let Some(join) =
                build_in_subquery_semi_join(db, select, &expr, &subquery, JoinType::Semi)
            {
                select.joins.push(join);
                None
            } else {
                Some(Expr::InSubquery {
                    expr,
                    subquery,
                    negated: false,
                })
            }
        }
        Expr::Exists {
            subquery,
            negated: false,
        } => {
            // For multi-table EXISTS bodies, the decorrelated derived already
            // applies DISTINCT on the correlation keys, so an INNER join over
            // it is semantically identical to a SEMI join — each outer row
            // matches at most one derived row. We pick INNER so the
            // join-order optimiser can freely reorder the lift; SEMI is
            // pinned at the top of the plan (probe-LEFT-build-RIGHT) and
            // would defeat the whole point of lifting a small existence-key
            // set when the outer is huge (TPC-DS Q35).
            //
            // The single-table path keeps SEMI: it reuses the body table
            // directly (no DISTINCT wrapper), so it can have duplicate
            // right-side keys; SEMI guarantees one-emit-per-outer-row there.
            if let Some((join, sub_where)) =
                build_exists_semi_join(db, select, &subquery, JoinType::Semi).or_else(|| {
                    build_exists_semi_join_derived(db, select, &subquery, JoinType::Inner)
                })
            {
                select.joins.push(join);
                if let Some(extra) = sub_where {
                    extra_conjuncts.push(extra);
                }
                None
            } else {
                Some(Expr::Exists {
                    subquery,
                    negated: false,
                })
            }
        }
        Expr::Exists {
            subquery,
            negated: true,
        } => {
            if let Some((join, sub_where)) =
                build_exists_semi_join(db, select, &subquery, JoinType::Anti).or_else(|| {
                    build_exists_semi_join_derived(db, select, &subquery, JoinType::Anti)
                })
            {
                select.joins.push(join);
                if let Some(extra) = sub_where {
                    extra_conjuncts.push(extra);
                }
                None
            } else {
                Some(Expr::Exists {
                    subquery,
                    negated: true,
                })
            }
        }
        other => Some(other),
    }
}

/// Build a `Join { table, join_type, condition }` from a simple
/// `IN (SELECT col FROM t [WHERE pred])` subquery. Wired up by Phase 3
/// — the caller in `try_rewrite_subquery_predicate` skips the rewrite
/// when `in_subquery_should_stay` detects a composite-index
/// opportunity that `IndexInLookup` could exploit.
///
/// The subquery's WHERE clause is folded into the synthesized join's
/// condition rather than returned for the caller to lift up to the
/// outer SELECT's WHERE. Lifting it caused TPC-H Q20 to mis-plan: the
/// outer's `s_suppkey IN (SELECT … WHERE ps_partkey IN (…) AND …)`
/// would push `ps_partkey IN (…)` to the outer WHERE, where a SECOND
/// `normalize_select_predicates` pass (in `plan_select`) would try to
/// rewrite *that* into a SemiJoin too — using `ps_partkey` as a key
/// against a row stream from which the previous Semi-rewrite had
/// already stripped partsupp. Keeping `sub_where` inside the join
/// condition prevents that second pass from picking it up
/// (it iterates `select.selection` only) while still letting
/// `push_join_on_predicates` push partsupp-only conjuncts down to the
/// partsupp base scan, since Semi/Anti right-side ON pushdown is safe.
fn build_in_subquery_semi_join(
    db: &Database,
    outer: &Select,
    outer_expr: &Expr,
    subquery: &Select,
    join_type: JoinType,
) -> Option<crate::parser::Join> {
    if !subquery_is_simple_for_semi(subquery) {
        return None;
    }
    if subquery.projection.len() != 1 {
        return None;
    }
    let sub_table = subquery.from.first()?.clone();
    if outer_uses_table(outer, &sub_table.name, sub_table.alias.as_deref()) {
        return None;
    }
    if subquery_references_outer_select(outer, subquery) {
        return None;
    }
    // Keep the query-block boundary when its scalar aggregates can be
    // decorrelated. Moving its WHERE into this semi join's ON would hide
    // those subqueries from the scalar pass, and attaching their LEFT joins
    // to the outer block would change the semi join's scope/cardinality.
    if decorrelate_enabled() && validate_select_column_references(db, subquery).is_ok() {
        let mut derived = subquery.clone();
        if crate::planner_decorrelate::scalar::decorrelate_scalar_subqueries(db, &mut derived) > 0 {
            let alias = unique_synthetic_alias(outer, "__sj_in");
            let key = "__sj_value".to_string();
            let SelectItem::Expr {
                alias: output_alias,
                ..
            } = &mut derived.projection[0]
            else {
                return None;
            };
            *output_alias = Some(key.clone());
            let mut left = outer_expr.clone();
            qualify_outer_idents_by_owner(db, &mut left, outer);
            return Some(crate::parser::Join {
                table: TableRef {
                    name: alias.clone(),
                    alias: Some(alias.clone()),
                    system_time: None,
                    index_hints: Vec::new(),
                    partitions: Vec::new(),
                    column_aliases: Vec::new(),
                    generate_series: None,
                },
                join_type,
                condition: Some(Expr::Binary {
                    left: Box::new(left),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::CompoundIdentifier(vec![alias, key])),
                }),
                using_columns: Vec::new(),
                natural: false,
                derived: Some(SubQuery::Select(Box::new(derived))),
                straight: false,
                lateral: false,
            });
        }
    }
    let inner_scope = build_column_scope(db, subquery, None).ok()?;
    let mut proj_expr = match &subquery.projection[0] {
        SelectItem::Expr { expr, .. } => expr.clone(),
        _ => return None,
    };
    qualify_idents_with_scope(&mut proj_expr, &inner_scope, false);
    // Qualify the outer side too — when the user wrote `WHERE col IN
    // (subq)`, `col` is an unqualified Identifier that becomes ambiguous
    // after the SemiJoin brings the inner table into scope (the inner
    // projection often shares the same column name — e.g. TPC-DS q14/q23
    // `ss_item_sk IN (SELECT ss_item_sk FROM cross_items)`). Bind each
    // bare identifier to the unique outer table that actually owns it so
    // it no longer collides with the inner column of the same name. Works
    // for a multi-table outer (where the legacy `single_outer_alias`
    // bailed) by resolving column ownership against the outer scope.
    let mut outer_left = outer_expr.clone();
    qualify_outer_idents_by_owner(db, &mut outer_left, outer);
    let eq_condition = Expr::Binary {
        left: Box::new(outer_left),
        op: BinaryOp::Eq,
        right: Box::new(proj_expr),
    };
    let condition = if let Some(mut w) = subquery.selection.clone() {
        qualify_idents_with_scope(&mut w, &inner_scope, false);
        Expr::Binary {
            left: Box::new(eq_condition),
            op: BinaryOp::And,
            right: Box::new(w),
        }
    } else {
        eq_condition
    };
    Some(crate::parser::Join {
        table: sub_table,
        join_type,
        condition: Some(condition),
        using_columns: Vec::new(),
        natural: false,
        derived: None,
        straight: false,
        lateral: false,
    })
}

/// Walk `expr` and qualify every bare `Expr::Identifier(col)` with the
/// unique outer table that owns `col`, so a predicate lifted into a new
/// SemiJoin's ON clause doesn't collide with an inner column of the same
/// name. Resolution is against `outer`'s own FROM/JOIN scope (visible
/// tables only — already-lifted Semi/Anti right sides are invisible and
/// excluded). Best-effort: if the scope can't be built, or a column has
/// zero / multiple owners (already-qualified refs, expressions, or a name
/// genuinely ambiguous in the outer query), the identifier is left as-is.
pub(crate) fn qualify_outer_idents_by_owner(db: &Database, expr: &mut Expr, outer: &Select) {
    let Ok(scope) = build_column_scope(db, outer, None) else {
        return;
    };
    qualify_idents_with_scope(expr, &scope, false);
}

/// Qualify every bare `Expr::Identifier(col)` in `expr` with the unique
/// *visible* table that owns it.
///
/// `only_if_invisible_collision = false` qualifies whenever the name has a
/// single visible owner (used for an expression we already know belongs to
/// the outer scope). `true` qualifies only names that ALSO appear on an
/// invisible (lifted Semi/Anti) source — i.e. references that were
/// unambiguous before the rewrite but would now resolve ambiguously at
/// runtime against the combined row. Names with zero / multiple visible
/// owners are left untouched for normal resolution.
fn qualify_idents_with_scope(
    expr: &mut Expr,
    scope: &ColumnScope<'_>,
    only_if_invisible_collision: bool,
) {
    let scope = ScopeWrap(scope, only_if_invisible_collision);
    qualify_idents_with_scope_inner(expr, &scope);
}

struct ScopeWrap<'a, 'b>(&'a ColumnScope<'b>, bool);

fn qualify_idents_with_scope_inner(expr: &mut Expr, scope: &ScopeWrap<'_, '_>) {
    let ScopeWrap(cols, only_if_invisible_collision) = *scope;
    crate::parser::visit::rewrite_value_expr(expr, |expr| {
        match expr {
            Expr::Identifier(name) => {
                let mut visible_owner: Option<String> = None;
                let mut visible_count = 0usize;
                let mut invisible_count = 0usize;
                for table in &cols.tables {
                    if table.has_column(name) {
                        if table.visible {
                            visible_count += 1;
                            visible_owner = Some(table.alias.clone().unwrap_or_else(|| {
                                table
                                    .name
                                    .rsplit('.')
                                    .next()
                                    .unwrap_or(&table.name)
                                    .to_string()
                            }));
                        } else {
                            invisible_count += 1;
                        }
                    }
                }
                let needed = !only_if_invisible_collision || invisible_count > 0;
                if needed && visible_count == 1 {
                    if let Some(qualifier) = visible_owner {
                        *expr = Expr::CompoundIdentifier(vec![qualifier, name.clone()]);
                    }
                }
            }
            Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
            | Expr::WindowFunction { .. } => return false,
            _ => {}
        }
        true
    });
}

fn and_conjuncts(parts: Vec<Expr>) -> Option<Expr> {
    parts.into_iter().reduce(|acc, p| Expr::Binary {
        left: Box::new(acc),
        op: BinaryOp::And,
        right: Box::new(p),
    })
}

pub(super) fn or_disjuncts(parts: Vec<Expr>) -> Option<Expr> {
    parts.into_iter().reduce(|acc, p| Expr::Binary {
        left: Box::new(acc),
        op: BinaryOp::Or,
        right: Box::new(p),
    })
}

/// Factor conjuncts common to every branch of a top-level OR:
/// `(K ∧ P) ∨ (K ∧ Q) ⟹ K ∧ (P ∨ Q)`. A correlated equi-key buried inside an
/// OR (TPC-DS Q41's `(i_manufact=i1.i_manufact AND …) OR (i_manufact=i1.i_manufact
/// AND …)`) then surfaces as a top-level conjunct that the semi-join key
/// extractor can lift into the join key, instead of staying a per-row residual
/// that forces a full inner re-scan per outer row. Pure boolean algebra; a
/// no-op when the OR has <2 branches or no shared conjunct.
fn factor_common_conjuncts_from_or(expr: Expr) -> Expr {
    let branches = split_disjuncts_top(&expr);
    if branches.len() < 2 {
        return expr;
    }
    let branch_conjuncts: Vec<Vec<Expr>> = branches
        .iter()
        .map(|b| {
            let mut v = Vec::new();
            split_conjuncts(b, &mut v);
            v
        })
        .collect();
    // Conjuncts of the first branch that appear in *every* branch.
    let mut common: Vec<Expr> = Vec::new();
    for c in &branch_conjuncts[0] {
        if !common.iter().any(|x| x == c)
            && branch_conjuncts[1..]
                .iter()
                .all(|bc| bc.iter().any(|x| x == c))
        {
            common.push(c.clone());
        }
    }
    if common.is_empty() {
        return expr;
    }
    let mut remainders = Vec::with_capacity(branches.len());
    let mut any_empty_remainder = false;
    for bc in &branch_conjuncts {
        let rem: Vec<Expr> = bc
            .iter()
            .filter(|c| !common.iter().any(|x| x == *c))
            .cloned()
            .collect();
        if rem.is_empty() {
            // This branch is satisfied by the common part alone, so the OR of
            // remainders is unconditionally TRUE.
            any_empty_remainder = true;
        } else if let Some(r) = and_conjuncts(rem) {
            remainders.push(r);
        }
    }
    let common_expr = and_conjuncts(common).expect("common is non-empty");
    if any_empty_remainder {
        return common_expr;
    }
    match or_disjuncts(remainders) {
        Some(or_part) => Expr::Binary {
            left: Box::new(common_expr),
            op: BinaryOp::And,
            right: Box::new(or_part),
        },
        None => common_expr,
    }
}

/// Recognise `(SELECT COUNT(*) FROM t [WHERE p]) > 0` — and the equivalent
/// `0 < (...)`, `(...) >= 1`, `1 <= (...)` — returning the inner select with
/// its aggregate projection replaced by `SELECT 1` so it can drive an EXISTS
/// semi-join. The projection MUST be stripped: `EXISTS (SELECT COUNT(*) …)`
/// is always true (COUNT returns one row even over an empty table), whereas
/// `COUNT(*) > 0` is false for an empty result — so wrapping the count
/// subquery in EXISTS directly would be a correctness bug.
fn count_gt_zero_to_exists_select(predicate: &Expr) -> Option<Box<Select>> {
    // Resolve an integer threshold from a literal, a BoundValue, or a
    // Placeholder that has a value bound in the active parameter context.
    // The shape rewrite (`(SELECT COUNT(*) …) > 0` → semi join) is decided
    // entirely from the threshold *value* (only `> 0` / `>= 1` qualify),
    // and the auto-parameterize fingerprint already includes the comparison
    // operator + the parameter index, so two queries that differ only in
    // the threshold value live in distinct plan-cache entries — letting us
    // peek the runtime value here is safe (a different binding picks its
    // own plan, not the rewritten one).
    let lit_int = |e: &Expr| -> Option<i64> {
        use crate::types::Value as V;
        match e {
            Expr::Literal(crate::parser::Literal::Int(n)) => Some(*n),
            Expr::Literal(crate::parser::Literal::UInt(n)) => i64::try_from(*n).ok(),
            Expr::BoundValue(V::Int(n)) => Some(*n),
            Expr::BoundValue(V::UInt(n)) => i64::try_from(*n).ok(),
            Expr::Placeholder(idx) => match crate::engine::params::current_param(*idx)? {
                V::Int(n) => Some(n),
                V::UInt(n) => i64::try_from(n).ok(),
                _ => None,
            },
            _ => None,
        }
    };
    // Normalise to (subquery_side, op_is_strict_gt, threshold), where the
    // subquery is conceptually on the left of a `>`/`>=` against threshold.
    let (sub, strict, threshold) = match predicate {
        Expr::Binary {
            left,
            op: BinaryOp::Gt,
            right,
        } => (left, true, lit_int(right)?),
        Expr::Binary {
            left,
            op: BinaryOp::GtEq,
            right,
        } => (left, false, lit_int(right)?),
        Expr::Binary {
            left,
            op: BinaryOp::Lt,
            right,
        } => (right, true, lit_int(left)?),
        Expr::Binary {
            left,
            op: BinaryOp::LtEq,
            right,
        } => (right, false, lit_int(left)?),
        _ => return None,
    };
    // "non-empty" is `> 0` or `>= 1`. Any other threshold (e.g. `> 5`) is a
    // real count comparison, not existence — leave it alone.
    let is_non_empty_test = (strict && threshold == 0) || (!strict && threshold == 1);
    if !is_non_empty_test {
        return None;
    }
    let Expr::Subquery(sel) = sub.as_ref() else {
        return None;
    };
    if sel.projection.len() != 1
        || !sel.group_by.is_empty()
        || sel.having.is_some()
        || sel.distinct
        || sel.limit.is_some()
        || sel.offset.is_some()
    {
        return None;
    }
    let SelectItem::Expr { expr, .. } = &sel.projection[0] else {
        return None;
    };
    // COUNT(*) only — `COUNT(col)` counts non-NULL `col`, which would need an
    // `IS NOT NULL` guard in the lifted predicate that we don't synthesize.
    let Expr::Function {
        name,
        args,
        distinct,
    } = expr
    else {
        return None;
    };
    if *distinct || !name.eq_ignore_ascii_case("count") {
        return None;
    }
    if !(args.len() == 1 && matches!(args[0], Expr::Wildcard)) {
        return None;
    }
    let mut stripped = sel.clone();
    stripped.projection = vec![SelectItem::Expr {
        expr: Expr::Literal(crate::parser::Literal::Int(1)),
        alias: None,
    }];
    Some(stripped)
}

/// EXISTS / NOT EXISTS variant: no equality condition is synthesized;
/// the subquery's body already encodes "row exists when these rows
/// pass the WHERE". The join condition becomes the subquery's WHERE
/// (or `None` when the subquery has no WHERE — then EXISTS is true
/// iff `t` has at least one row, encoded as a Cross-style match).
fn build_exists_semi_join(
    db: &Database,
    outer: &Select,
    subquery: &Select,
    join_type: JoinType,
) -> Option<(crate::parser::Join, Option<Expr>)> {
    if !subquery_is_simple_for_semi(subquery) {
        return None;
    }
    let sub_table = subquery.from.first()?.clone();
    if outer_uses_table(outer, &sub_table.name, sub_table.alias.as_deref()) {
        return None;
    }
    // The subquery's WHERE becomes the join condition. A bare inner-column
    // identifier resolves to the inner table in the subquery scope, but once
    // the inner table is lifted next to the outer it becomes ambiguous when the
    // outer shares the column name — most acutely in a self-join (TPC-DS Q41's
    // `item` EXISTS over `item i1`), where `i_manufact = i1.i_manufact` would
    // otherwise bind the bare `i_manufact` to the outer `i1`, collapsing to the
    // tautology `i1.i_manufact = i1.i_manufact`. Qualify only the identifiers
    // the INNER table actually owns (schema-aware, via the subquery's own
    // scope) — a bare *outer* correlation column (e.g. TPC-H Q4's
    // `l_orderkey = o_orderkey`, where `o_orderkey` is not a lineitem column)
    // is left unqualified so it still resolves to the outer table.
    let condition = subquery.selection.clone().map(|mut w| {
        if let Ok(inner_scope) = build_column_scope(db, subquery, None) {
            qualify_idents_with_scope(&mut w, &inner_scope, false);
        }
        // Surface an equi-key shared by every OR branch so the semi-join key
        // extractor can use it (Q41).
        factor_common_conjuncts_from_or(w)
    });
    // `None` condition means "any row in t qualifies", which the executor
    // models with an unconditional inner-row probe.
    let join = crate::parser::Join {
        table: sub_table,
        join_type,
        condition,
        using_columns: Vec::new(),
        natural: false,
        derived: None,
        straight: false,
        lateral: false,
    };
    Some((join, None))
}

/// Decorrelate a multi-table-body EXISTS/NOT-EXISTS subquery by wrapping
/// the body as a derived-table Semi/Anti-join. Generalisation of
/// [`build_exists_semi_join`], which only handles single-table bodies.
///
/// Handles the canonical correlated-existential shape
/// `EXISTS(SELECT 1 FROM A, B [, ...]
///         WHERE outer.k = inner.x AND <inner-only preds>)`,
/// rewriting it to:
///
/// ```text
/// outer SEMI JOIN (SELECT DISTINCT inner.x AS __sj_k_0
///                  FROM A, B [, ...]
///                  WHERE <inner-only preds>) sj
///       ON outer.k = sj.__sj_k_0
/// ```
///
/// The DISTINCT mirrors PG's HashAggregate-above-scan dedup: a SEMI join
/// emits each outer row at most once regardless of right-side duplicates,
/// but pre-deduping caps the hash-build size at the distinct correlation
/// keys instead of the full inner row count.
///
/// Eligibility (conservative on purpose; falls back to per-row eval when
/// any condition fails):
/// * Body has no aggregation / GROUP BY / HAVING / DISTINCT / ORDER BY /
///   LIMIT / OFFSET / FOR UPDATE / window / derived FROM / VALUES.
/// * All body JOINs are INNER (outer joins can't be lifted into the
///   derived without changing semantics).
/// * Every conjunctive predicate in the body WHERE is either purely
///   inner (kept inside the derived) or a clean
///   `outer.k = inner.x` equality (pulled out as a correlation pair).
/// * JOIN-ON conditions reference only inner tables (any outer ref in a
///   JOIN-ON bails — would otherwise leak across the derived boundary).
/// * Projection / group_by / having / order_by reference only inner.
/// * At least one correlation pair exists (otherwise the rewrite would
///   produce a cross product against an unconditioned derived; the
///   degenerate uncorrelated case is left to per-row eval).
fn build_exists_semi_join_derived(
    db: &Database,
    outer: &Select,
    subquery: &Select,
    join_type: JoinType,
) -> Option<(crate::parser::Join, Option<Expr>)> {
    let branch = exists_subquery_to_keyset_branch(db, outer, subquery)?;
    let pairs = branch.key_pairs;
    // Build derived: SELECT DISTINCT inner_side AS __sj_k_i FROM ... WHERE residue
    let mut derived = branch.inner_select;
    derived.distinct = true;
    derived.projection.clear();
    let mut key_aliases: Vec<String> = Vec::with_capacity(pairs.len());
    for (i, (_outer_side, inner_side)) in pairs.iter().enumerate() {
        let alias = format!("__sj_k_{}", i);
        derived.projection.push(SelectItem::Expr {
            expr: inner_side.clone(),
            alias: Some(alias.clone()),
        });
        key_aliases.push(alias);
    }
    let derived_alias = unique_synthetic_alias(outer, "__sj_exists");
    let mut cond_parts: Vec<Expr> = Vec::with_capacity(pairs.len());
    for ((outer_side, _inner_side), key_name) in pairs.iter().zip(key_aliases.iter()) {
        let mut outer_left = outer_side.clone();
        qualify_outer_idents_by_owner(db, &mut outer_left, outer);
        cond_parts.push(Expr::Binary {
            left: Box::new(outer_left),
            op: BinaryOp::Eq,
            right: Box::new(Expr::CompoundIdentifier(vec![
                derived_alias.clone(),
                key_name.clone(),
            ])),
        });
    }
    let condition = combine_conjuncts(cond_parts);

    let table = crate::parser::TableRef {
        name: derived_alias.clone(),
        alias: Some(derived_alias),
        system_time: None,
        index_hints: Vec::new(),
        partitions: Vec::new(),
        column_aliases: Vec::new(),
        generate_series: None,
    };
    let join = crate::parser::Join {
        table,
        join_type,
        condition,
        using_columns: Vec::new(),
        natural: false,
        derived: Some(SubQuery::Select(Box::new(derived))),
        straight: false,
        lateral: false,
    };
    Some((join, None))
}

#[derive(Clone)]
struct ExistsKeySetBranch {
    key_pairs: Vec<(Expr, Expr)>,
    inner_select: Select,
}

/// Different outer keys cannot share a UNION join. Materialize each inner
/// equality domain once and probe it independently. In a positive WHERE
/// disjunction, IN's UNKNOWN and EXISTS's FALSE reject exactly the same rows.
fn build_or_exists_keyset_predicate(
    db: &Database,
    outer: &Select,
    predicate: &Expr,
) -> Option<Expr> {
    let branches = split_disjuncts_top(predicate);
    if branches.len() < 2 {
        return None;
    }
    let mut predicates = Vec::with_capacity(branches.len());
    for branch in branches {
        let Expr::Exists {
            subquery,
            negated: false,
        } = branch
        else {
            return None;
        };
        let mut branch = exists_subquery_to_keyset_branch(db, outer, &subquery)?;
        let mut outer_keys = Vec::with_capacity(branch.key_pairs.len());
        branch.inner_select.projection.clear();
        for (outer_key, inner_key) in branch.key_pairs {
            outer_keys.push(outer_key);
            branch.inner_select.projection.push(SelectItem::Expr {
                expr: inner_key,
                alias: None,
            });
        }
        let expr = if outer_keys.len() == 1 {
            outer_keys.pop()?
        } else {
            Expr::Tuple(outer_keys)
        };
        predicates.push(Expr::InSubquery {
            expr: Box::new(expr),
            subquery: Box::new(branch.inner_select),
            negated: false,
        });
    }
    or_disjuncts(predicates)
}

/// Decorrelate `EXISTS(a) OR EXISTS(b) ...` where every branch is a
/// positive correlated EXISTS with the same outer equality keys. The OR is
/// equivalent to joining against the UNION DISTINCT of each branch's inner
/// key set, which avoids per-candidate subquery evaluation for TPC-DS Q35
/// and any query with the same general shape.
fn build_or_exists_union_join(
    db: &Database,
    outer: &Select,
    predicate: &Expr,
) -> Option<crate::parser::Join> {
    let branches = split_disjuncts_top(predicate);
    if branches.len() < 2 {
        return None;
    }
    let mut key_branches = Vec::with_capacity(branches.len());
    for branch in branches {
        let Expr::Exists {
            subquery,
            negated: false,
        } = branch
        else {
            return None;
        };
        key_branches.push(exists_subquery_to_keyset_branch(db, outer, &subquery)?);
    }
    let first_outer_keys: Vec<_> = key_branches
        .first()?
        .key_pairs
        .iter()
        .map(|(outer, _)| outer.clone())
        .collect();
    if first_outer_keys.is_empty()
        || key_branches.iter().any(|branch| {
            !branch
                .key_pairs
                .iter()
                .map(|(outer, _)| outer)
                .eq(first_outer_keys.iter())
        })
    {
        return None;
    }

    let key_aliases: Vec<String> = (0..first_outer_keys.len())
        .map(|i| format!("__sj_k_{}", i))
        .collect();
    let mut statements = Vec::with_capacity(key_branches.len());
    for mut branch in key_branches {
        branch.inner_select.projection.clear();
        for ((_, inner_key), alias) in branch.key_pairs.into_iter().zip(key_aliases.iter()) {
            branch.inner_select.projection.push(SelectItem::Expr {
                expr: inner_key,
                alias: Some(alias.clone()),
            });
        }
        branch.inner_select.distinct = false;
        statements.push(crate::parser::QueryExpr::from(branch.inner_select));
    }

    let mut iter = statements.into_iter();
    let mut union_stmt = iter.next()?;
    for right in iter {
        union_stmt = crate::parser::QueryExpr::from(SetOperation {
            op: SetOp::Union,
            all: false,
            left: union_stmt,
            right: right,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        });
    }

    let derived_alias = unique_synthetic_alias(outer, "__sj_exists_or");
    let mut cond_parts = Vec::with_capacity(first_outer_keys.len());
    for (outer_key, key_name) in first_outer_keys.iter().zip(key_aliases.iter()) {
        let mut outer_left = outer_key.clone();
        qualify_outer_idents_by_owner(db, &mut outer_left, outer);
        cond_parts.push(Expr::Binary {
            left: Box::new(outer_left),
            op: BinaryOp::Eq,
            right: Box::new(Expr::CompoundIdentifier(vec![
                derived_alias.clone(),
                key_name.clone(),
            ])),
        });
    }

    Some(crate::parser::Join {
        table: crate::parser::TableRef {
            name: derived_alias.clone(),
            alias: Some(derived_alias),
            system_time: None,
            index_hints: Vec::new(),
            partitions: Vec::new(),
            column_aliases: Vec::new(),
            generate_series: None,
        },
        join_type: JoinType::Inner,
        condition: combine_conjuncts(cond_parts),
        using_columns: Vec::new(),
        natural: false,
        derived: Some(union_stmt),
        straight: false,
        lateral: false,
    })
}

fn exists_subquery_to_keyset_branch(
    db: &Database,
    outer: &Select,
    subquery: &Select,
) -> Option<ExistsKeySetBranch> {
    if !subquery_body_is_decorrelatable(subquery) {
        return None;
    }
    if subquery
        .selection
        .as_ref()
        .is_some_and(expr_has_volatile_evaluation)
        || subquery.joins.iter().any(|join| {
            join.condition
                .as_ref()
                .is_some_and(expr_has_volatile_evaluation)
        })
    {
        return None;
    }
    let outer_scope = crate::planner_decorrelate::select_scope(db, outer);
    let inner_scope = crate::planner_decorrelate::select_scope(db, subquery);

    let body_tables = subquery
        .from
        .iter()
        .chain(subquery.joins.iter().map(|j| &j.table));
    for t in body_tables {
        if outer_uses_table(outer, &t.name, t.alias.as_deref()) {
            return None;
        }
    }
    for j in &subquery.joins {
        if !matches!(j.join_type, JoinType::Inner | JoinType::Cross) {
            return None;
        }
        if let Some(c) = &j.condition {
            if expr_touches_scope(c, &outer_scope, &inner_scope) {
                return None;
            }
        }
    }
    for item in &subquery.projection {
        if let SelectItem::Expr { expr, .. } = item {
            if expr_touches_scope(expr, &outer_scope, &inner_scope) {
                return None;
            }
        }
    }
    for e in &subquery.group_by {
        if expr_touches_scope(e, &outer_scope, &inner_scope) {
            return None;
        }
    }
    if let Some(h) = &subquery.having {
        if expr_touches_scope(h, &outer_scope, &inner_scope) {
            return None;
        }
    }
    for ob in &subquery.order_by {
        if expr_touches_scope(&ob.expr, &outer_scope, &inner_scope) {
            return None;
        }
    }

    let selection = subquery.selection.as_ref()?;
    let mut conjuncts = Vec::new();
    split_conjuncts(selection, &mut conjuncts);
    let mut residue = Vec::new();
    let mut key_pairs = Vec::new();
    for conj in conjuncts {
        match classify_outer_inner_conjunct(&conj, &outer_scope, &inner_scope) {
            ConjClass::OuterInnerEq {
                outer_side,
                inner_side,
            } => {
                if expr_has_volatile_evaluation(&outer_side)
                    || expr_has_volatile_evaluation(&inner_side)
                {
                    return None;
                }
                key_pairs.push((outer_side, inner_side));
            }
            ConjClass::InnerOnly => residue.push(conj),
            ConjClass::ContainsOuter => return None,
        }
    }
    if key_pairs.is_empty() {
        return None;
    }

    let mut inner_select = subquery.clone();
    inner_select.selection = combine_conjuncts(residue);
    inner_select.for_update = false;
    inner_select.lock_clause = None;
    inner_select.sql_calc_found_rows = false;
    inner_select.named_windows.clear();
    inner_select.query_hints.clear();
    inner_select.hint_warnings.clear();
    inner_select.group_by_modifier = GroupByModifier::None;
    inner_select.grouping_sets.clear();
    inner_select.order_by.clear();
    inner_select.limit = None;
    inner_select.offset = None;

    Some(ExistsKeySetBranch {
        key_pairs,
        inner_select,
    })
}

/// Like `subquery_is_simple_for_semi` but allows multiple FROM tables and
/// INNER joins. Used by `build_exists_semi_join_derived`.
fn subquery_body_is_decorrelatable(subquery: &Select) -> bool {
    // Nested sublinks have their own scope; lifting their containing predicate
    // into an independent derived table would detach outer references.
    if subquery
        .selection
        .as_ref()
        .is_some_and(sip_expr_has_subquery)
        || subquery
            .joins
            .iter()
            .any(|j| j.condition.as_ref().is_some_and(sip_expr_has_subquery))
    {
        return false;
    }
    if subquery.from.is_empty() {
        return false;
    }
    if subquery.from.iter().any(|t| t.generate_series.is_some()) {
        return false;
    }
    if subquery.derived.is_some() {
        return false;
    }
    if subquery.values.is_some() || subquery.json_table.is_some() {
        return false;
    }
    if !subquery.group_by.is_empty()
        || subquery.having.is_some()
        || !subquery.order_by.is_empty()
        || subquery.limit.is_some()
        || subquery.offset.is_some()
        || subquery.distinct
        || subquery.for_update
        || !subquery.named_windows.is_empty()
    {
        return false;
    }
    if matches!(
        subquery.group_by_modifier,
        GroupByModifier::Rollup | GroupByModifier::Cube | GroupByModifier::GroupingSets
    ) {
        return false;
    }
    if !subquery.grouping_sets.is_empty() {
        return false;
    }
    if has_aggregate_select_items(&subquery.projection)
        || has_window_in_projection(&subquery.projection)
    {
        return false;
    }
    // We only decorrelate inner-join (or comma-list CROSS) bodies. Outer /
    // semi / anti joins can't be safely lifted into a derived without
    // rewriting null-pad semantics or losing existential pruning. CROSS
    // is semantically equivalent to INNER with a true condition and is
    // produced by sqlparser for comma-separated FROM lists — `FROM a, b
    // WHERE …` is the canonical Q35 shape, so we accept it.
    if subquery
        .joins
        .iter()
        .any(|j| !matches!(j.join_type, JoinType::Inner | JoinType::Cross))
    {
        return false;
    }
    if subquery
        .joins
        .iter()
        .any(|j| j.lateral || j.derived.is_some())
    {
        return false;
    }
    true
}

enum ConjClass {
    /// Clean `outer.k = inner.x` (or reversed) equality — the side facing
    /// the outer scope and the side facing the inner scope are returned
    /// in `outer_side`/`inner_side` respectively.
    OuterInnerEq { outer_side: Expr, inner_side: Expr },
    /// Predicate references only inner tables / constants.
    InnerOnly,
    /// Predicate references outer columns but not in the clean equality
    /// shape. Caller should bail.
    ContainsOuter,
}

fn classify_outer_inner_conjunct(
    expr: &Expr,
    outer: &crate::planner_decorrelate::sublink::Scope,
    inner: &crate::planner_decorrelate::sublink::Scope,
) -> ConjClass {
    let Expr::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    } = expr
    else {
        if expr_touches_scope(expr, outer, inner) {
            return ConjClass::ContainsOuter;
        }
        return ConjClass::InnerOnly;
    };
    let left_touches_outer = expr_touches_scope(left, outer, inner);
    let right_touches_outer = expr_touches_scope(right, outer, inner);
    let left_touches_inner = expr_touches_inner(left, outer, inner);
    let right_touches_inner = expr_touches_inner(right, outer, inner);
    let left_is_outer_col = left_touches_outer
        && !left_touches_inner
        && !has_aggregate_expr(left)
        && !expr_has_volatile_evaluation(left);
    let right_is_outer_col = right_touches_outer
        && !right_touches_inner
        && !has_aggregate_expr(right)
        && !expr_has_volatile_evaluation(right);

    match (left_is_outer_col, right_is_outer_col) {
        (true, false) if !right_touches_outer && right_touches_inner => ConjClass::OuterInnerEq {
            outer_side: (**left).clone(),
            inner_side: (**right).clone(),
        },
        (false, true) if !left_touches_outer && left_touches_inner => ConjClass::OuterInnerEq {
            outer_side: (**right).clone(),
            inner_side: (**left).clone(),
        },
        _ => {
            if left_touches_outer || right_touches_outer {
                ConjClass::ContainsOuter
            } else {
                ConjClass::InnerOnly
            }
        }
    }
}

fn expr_touches_scope(
    expr: &Expr,
    outer: &crate::planner_decorrelate::sublink::Scope,
    inner: &crate::planner_decorrelate::sublink::Scope,
) -> bool {
    let mut hit = false;
    walk_expr_idents(expr, &mut |id_expr| {
        if hit {
            return;
        }
        if expr_resolves_to_outer_column(id_expr, outer, inner) {
            hit = true;
        }
    });
    hit
}

fn expr_touches_inner(
    expr: &Expr,
    outer: &crate::planner_decorrelate::sublink::Scope,
    inner: &crate::planner_decorrelate::sublink::Scope,
) -> bool {
    let mut hit = false;
    walk_expr_idents(expr, &mut |id_expr| {
        if hit {
            return;
        }
        match id_expr {
            Expr::Identifier(name) => {
                let lc = name.to_ascii_lowercase();
                if inner.columns.contains(&lc) {
                    hit = true;
                }
            }
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                let qualifier = parts[parts.len() - 2].to_ascii_lowercase();
                if inner.tables.contains(&qualifier) && !outer.tables.contains(&qualifier) {
                    hit = true;
                }
            }
            _ => {}
        }
    });
    hit
}

/// Walk `expr` calling `f` on every Identifier / CompoundIdentifier node.
/// Descends through aggregate-free shapes; subquery-bearing nodes are
/// visited as opaque (caller's classification only needs surface-level
/// outer/inner detection, and the eligibility gate already rejects bodies
/// with nested EXISTS in the WHERE).
fn walk_expr_idents(expr: &Expr, f: &mut impl FnMut(&Expr)) {
    crate::parser::visit::inspect_value_expr(
        expr,
        |expr| {
            if matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                f(expr);
            }
            !matches!(
                expr,
                Expr::InSubquery { .. }
                    | Expr::QuantifiedComparison { .. }
                    | Expr::Subquery(_)
                    | Expr::Exists { .. }
                    | Expr::WindowFunction { .. }
                    | Expr::GroupKeyRef { .. }
            )
        },
        |_| {},
    );
}

/// Pick a synthetic alias for a derived-table rewrite that doesn't collide
/// with any visible name in `outer`'s FROM/JOIN list. Numeric suffix is
/// bumped on collision.
fn unique_synthetic_alias(outer: &Select, prefix: &str) -> String {
    let mut used: HashSet<String> = HashSet::new();
    let mut record = |name: &str, alias: Option<&str>| {
        used.insert(name.to_ascii_lowercase());
        if let Some(a) = alias {
            used.insert(a.to_ascii_lowercase());
        }
    };
    for t in &outer.from {
        record(&t.name, t.alias.as_deref());
    }
    for j in &outer.joins {
        record(&j.table.name, j.table.alias.as_deref());
    }
    let mut i = 0usize;
    loop {
        let candidate = format!("{}_{}", prefix, i);
        if !used.contains(&candidate.to_ascii_lowercase()) {
            return candidate;
        }
        i += 1;
    }
}

/// Decide whether to leave an `x IN (subq)` predicate at the WHERE
/// level (so the IndexInLookup specialisation can take it) rather than
/// lifting it into a Semi join.
///
/// The rule is structural and conservative: any sibling top-level
/// equality of the form `<outer expr's table>.col = <const-ish>` in
/// the same WHERE block is taken as evidence that the user may have
/// arranged the predicates to hit a composite `(col, in_column)`
/// index. We prefer the index path in that case. Otherwise we let the
/// SemiJoin rewrite proceed and let the optimiser cost-pick.
fn in_subquery_should_stay(
    db: &Database,
    select: &Select,
    sibling_conjuncts: &[Expr],
    outer_expr: &Expr,
    subquery: &Select,
) -> bool {
    // Multi-table outer: each table needs an explicit qualifier on the
    // IN-column anyway, and the composite-index opportunity is rarer
    // — let the SemiJoin path take over.
    if !select.joins.is_empty() {
        return false;
    }
    // Single-table outer: any AND-conjuncted equality with a literal /
    // bound value is treated as evidence that the user may be hitting
    // a composite index. Keep the IN-subquery as-is so the planner's
    // `IndexInLookup` path can fire. Without an applicable index the
    // executor still uses the eval-time cached IN, which is just as
    // cheap as the SemiJoin for a single-table outer.
    if sibling_conjuncts.iter().any(|c| {
        matches!(c, Expr::Binary { left, op: BinaryOp::Eq, right }
            if column_eq_constantish(left, right) || column_eq_constantish(right, left))
    }) {
        return true;
    }
    // A filtered keyset over a simple indexed column is already handled by
    // IndexInLookup, which probes the outer index once per key. Rewriting it
    // to a semi join first removes that access candidate and can turn a tiny
    // dimension range into a full fact-table scan. This is the shape produced
    // by UNION-derived predicate pushdown, but the rule is structural and
    // applies to any selective filtered keyset.
    let Some(column) = expr_column_name(outer_expr) else {
        return false;
    };
    let Some(base) = select.from.first().filter(|_| select.joins.is_empty()) else {
        return false;
    };
    subquery.selection.is_some()
        && db.table(&base.name).is_ok_and(|table| {
            table.iter_queryable_indexes().any(|index| {
                index
                    .columns
                    .first()
                    .is_some_and(|indexed| indexed.eq_ignore_ascii_case(column))
            })
        })
}

/// True when `col` is a column reference (qualified or not) and `lit`
/// is a constant-ish expression (literal / bound value / placeholder).
/// Helper for [`in_subquery_should_stay`].
fn column_eq_constantish(col: &Expr, lit: &Expr) -> bool {
    matches!(col, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
        && matches!(
            lit,
            Expr::Literal(_) | Expr::BoundValue(_) | Expr::Placeholder(_)
        )
}

fn subquery_is_simple_for_semi(subquery: &Select) -> bool {
    crate::parser::query_shape::simple_single_source_body(subquery)
        && !has_aggregate_select_items(&subquery.projection)
        && !has_window_in_projection(&subquery.projection)
}

pub(super) fn has_window_in_projection(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_has_window_function(expr),
        _ => false,
    })
}

pub(crate) fn expr_has_window_function(expr: &Expr) -> bool {
    crate::parser::visit::try_inspect_value_expr(expr, |expr| {
        if matches!(expr, Expr::WindowFunction { .. }) {
            return Err(());
        }
        // This planner eligibility probe deliberately excludes row tuples,
        // bound grouping keys and subquery operands, unlike window execution.
        Ok(!matches!(
            expr,
            Expr::Tuple(_)
                | Expr::GroupKeyRef { .. }
                | Expr::InSubquery { .. }
                | Expr::QuantifiedComparison { .. }
        ))
    })
    .is_err()
}

/// True when lifting a subquery table that becomes visible as
/// `alias.unwrap_or(name)` into the outer FROM would collide with an outer
/// table's *visible* name. Comparison is visible-name vs visible-name: an
/// aliased outer table `item AS i1` is visible only as `i1`, never as `item`,
/// so a subquery `FROM item` (visible as `item`) does NOT collide and the
/// EXISTS/IN → semi-join rewrite can proceed (the lifted join is a normal
/// self-join `item i1 JOIN item ON …`). Using `matches_table` here instead
/// would match on the shared underlying name and wrongly bail, forcing the
/// correlated subquery to per-row eval (e.g. TPC-DS Q41's `item` self-join).
fn outer_uses_table(outer: &Select, name: &str, alias: Option<&str>) -> bool {
    let visible_name = alias.unwrap_or(name);
    outer
        .from
        .iter()
        .chain(outer.joins.iter().map(|j| &j.table))
        .any(|t| {
            let outer_visible = t.alias.as_deref().unwrap_or(&t.name);
            visible_name.eq_ignore_ascii_case(outer_visible)
        })
}

/// Conservative correlation check: walk the subquery's WHERE / ON
/// clauses and confirm no qualified column references a table that
/// the OUTER select uses (and that the SUBQUERY itself does not).
/// Unqualified identifiers are accepted only if the subquery's own
/// table has a column of that name; otherwise we err on the side
/// of "correlated → don't rewrite".
fn subquery_references_outer_select(outer: &Select, subquery: &Select) -> bool {
    // Both the bare table name (`a`) and any database-qualified form
    // (`test.a`) must be enrolled — qualified statements pass the
    // full path through `t.name` while compound identifiers in the
    // body only carry the bare name as their qualifier.
    let table_keys = |t: &TableRef| {
        let lowered = t.name.to_ascii_lowercase();
        let mut keys = vec![lowered.clone()];
        if let Some(bare) = lowered.rsplit('.').next() {
            if bare != lowered {
                keys.push(bare.to_string());
            }
        }
        if let Some(a) = &t.alias {
            keys.push(a.to_ascii_lowercase());
        }
        keys
    };
    let outer_tables: HashSet<String> = outer
        .from
        .iter()
        .chain(outer.joins.iter().map(|j| &j.table))
        .flat_map(table_keys)
        .collect();
    let sub_tables: HashSet<String> = subquery.from.iter().flat_map(table_keys).collect();
    let mut referenced = false;
    let mut walk = |expr: &Expr| {
        if referenced {
            return;
        }
        if subquery_expr_references_outer(expr, &outer_tables, &sub_tables) {
            referenced = true;
        }
    };
    if let Some(e) = &subquery.selection {
        walk(e);
    }
    for item in &subquery.projection {
        if let SelectItem::Expr { expr, .. } = item {
            walk(expr);
        }
    }
    referenced
}

pub(super) fn subquery_expr_references_outer(
    expr: &Expr,
    outer_tables: &HashSet<String>,
    sub_tables: &HashSet<String>,
) -> bool {
    crate::parser::visit::try_inspect_value_expr(expr, |expr| {
        if let Expr::CompoundIdentifier(parts) = expr {
            if parts.len() >= 2 {
                let qualifier = parts[parts.len() - 2].to_ascii_lowercase();
                if outer_tables.contains(&qualifier) && !sub_tables.contains(&qualifier) {
                    return Err(());
                }
            }
        }
        Ok(!matches!(expr, Expr::WindowFunction { .. }))
    })
    .is_err()
}
