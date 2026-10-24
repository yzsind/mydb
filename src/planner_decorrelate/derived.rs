//! Derived-table predicate pushdown.
//!
//! When an outer query says
//!
//! ```sql
//! SELECT * FROM (SELECT a, b FROM big WHERE c > 0) sub WHERE sub.a > 10
//! ```
//!
//! we want the planner to evaluate `a > 10` *inside* the derived scan
//! rather than after materialising every row that survives `c > 0`.
//! This pass walks the outer `WHERE` conjuncts and pushes the ones that
//! reference only safely-pushdownable columns of the derived body
//! into the derived's `WHERE` (or skips them, leaving them to evaluate
//! at the outer level).
//!
//! Eligibility rules (kept conservative so we never break semantics):
//! - The derived must have no `LIMIT` / `OFFSET` / `DISTINCT` / `HAVING`
//!   / window function / `FOR UPDATE` (any of those make the predicate
//!   filter the wrong row set if pushed below them).
//! - WHERE pushdown only accepts columns of the derived projection
//!   that are *direct* column references to the inner body
//!   (`SELECT inner_col` or `SELECT inner_col AS x`). Derived columns
//!   that are arithmetic / function / aggregate results are not pushed.
//! - If the derived has `GROUP BY`, the conjunct may only reference
//!   columns that appear in the GROUP BY (post-aggregate predicates
//!   are translated to HAVING for an ordinary grouped, single-source body).
//!
//! Companion to [`crate::engine::executor::select::try_flatten_simple_derived_select`]
//! which actually inlines the derived body when its shape allows.
//! That existing inliner already handles the simplest "no-op derived"
//! shape; this module focuses on the cases where the body has its own
//! `WHERE` / `JOIN` and we want to push the outer filter through.

use crate::parser::boolean::{
    combine_conjuncts_left as combine_conjuncts, split_conjuncts_owned as split_conjuncts,
};
use std::collections::{HashMap, HashSet};

use crate::parser::{BinaryOp, Expr, JoinType, Select, SelectItem, SetOp, SubQuery};
use crate::storage::Database;

/// Apply derived-table predicate pushdown to `select` in place. Returns
/// the number of conjuncts that were successfully pushed.
pub fn pushdown_derived_predicates(select: &mut Select) -> usize {
    let mut pushed = 0;
    // Handle FROM derived. Only an ordinary SELECT derived table is eligible
    // for predicate pushdown; a set-operation derived table is left untouched.
    if let Some(inner) = select.derived.as_ref().and_then(SubQuery::as_select) {
        if !derived_pushdown_safe(inner) {
            return 0;
        }
        let alias_map = build_alias_map(inner);
        let mut output_names = HashSet::new();
        let unique_outputs = inner
            .projection
            .iter()
            .filter_map(|item| {
                let SelectItem::Expr { expr, alias } = item else {
                    return None;
                };
                alias.clone().or_else(|| direct_column_name(expr))
            })
            .all(|name| output_names.insert(name.to_ascii_lowercase()));
        let having_map = if !unique_outputs
            || inner.group_by.is_empty()
            || inner.group_by_modifier != crate::parser::GroupByModifier::None
            || !inner.grouping_sets.is_empty()
            || inner.lock_clause.is_some()
            || select.from.len() != 1
            || !select.joins.is_empty()
            || !inner.projection.iter().all(|item| {
                matches!(item,
                SelectItem::Expr { expr, .. } if repeatable_aggregate_filter(expr))
            }) {
            HashMap::new()
        } else {
            inner
                .projection
                .iter()
                .filter_map(|item| {
                    let SelectItem::Expr { expr, alias } = item else {
                        return None;
                    };
                    let allowed = inner.group_by.contains(expr)
                        || matches!(expr, Expr::Function { .. })
                            && expr_has_aggregate(expr)
                            && repeatable_aggregate_filter(expr);
                    if !allowed {
                        return None;
                    }
                    let name = alias.clone().or_else(|| direct_column_name(expr))?;
                    Some((name.to_ascii_lowercase(), expr.clone()))
                })
                .collect()
        };
        let derived_alias = select
            .from
            .first()
            .map(|t| t.alias.clone().unwrap_or_else(|| t.name.clone()))
            .unwrap_or_default()
            .to_ascii_lowercase();
        if alias_map.is_none() && having_map.is_empty() {
            return 0;
        }
        let selection = match select.selection.take() {
            Some(s) => s,
            None => return 0,
        };
        let mut conjuncts = Vec::new();
        split_conjuncts(selection, &mut conjuncts);
        let mut keep: Vec<Expr> = Vec::new();
        let mut pushdown: Vec<Expr> = Vec::new();
        let mut having_pushdown: Vec<Expr> = Vec::new();
        for conj in conjuncts {
            if let Some(rewritten) = alias_map
                .as_ref()
                .and_then(|map| rewrite_for_pushdown(&conj, &derived_alias, map))
            {
                pushdown.push(rewritten);
                pushed += 1;
            } else if let Some(rewritten) = repeatable_aggregate_filter(&conj)
                .then(|| rewrite_for_pushdown(&conj, &derived_alias, &having_map))
                .flatten()
            {
                having_pushdown.push(rewritten);
                pushed += 1;
            } else {
                keep.push(conj);
            }
        }
        if !having_pushdown.is_empty() {
            if let Some(inner) = select.derived.as_mut().and_then(SubQuery::as_select_mut) {
                inner.having = combine_conjuncts(having_pushdown);
            }
        }
        select.selection = combine_conjuncts(keep);
        if !pushdown.is_empty() {
            // Re-borrow as &mut after the take().
            if let Some(inner_mut) = select.derived.as_mut().and_then(SubQuery::as_select_mut) {
                let mut combined = inner_mut.selection.take().map_or_else(Vec::new, |e| {
                    let mut v = Vec::new();
                    split_conjuncts(e, &mut v);
                    v
                });
                combined.extend(pushdown);
                inner_mut.selection = combine_conjuncts(combined);
            }
        }
    }
    pushed
}

fn repeatable_aggregate_filter(expr: &Expr) -> bool {
    crate::planner::expr_is_deterministic(expr)
        && crate::parser::visit::try_inspect_value_expr(expr, |node| {
            if matches!(
                node,
                Expr::Default(_) | Expr::DefaultQualified(_) | Expr::Grouping(_)
            ) {
                return Err(());
            }
            if let Expr::Function { name, .. } = node {
                if name.contains('.')
                    || !(crate::parser::is_aggregate_function(name)
                        || name.eq_ignore_ascii_case("CAST")
                        || crate::engine::cbo::function_cost::is_known_builtin_function(name))
                {
                    return Err(());
                }
            }
            Ok(true)
        })
        .is_ok()
}

/// Apply every derived-table predicate pushdown that needs catalog context.
///
/// The legacy [`pushdown_derived_predicates`] handles ordinary SELECT
/// derived tables with purely local predicates. This entry point also handles
/// the common star-schema shape where a `UNION ALL` derived table is joined to
/// a small filtered dimension:
///
/// ```sql
/// FROM (SELECT fact_date AS date_sk FROM f
///       UNION ALL
///       SELECT return_date AS date_sk FROM r) u,
///      date_dim d
/// WHERE u.date_sk = d.d_date_sk AND d.d_date BETWEEN ...
/// ```
///
/// It synthesizes `u.date_sk IN (SELECT d_date_sk FROM date_dim WHERE ...)`
/// and pushes that predicate into each UNION branch, where the existing
/// `IndexInLookup` machinery can use the branch-local date index.
pub fn pushdown_derived_predicates_with_db(db: &Database, select: &mut Select) -> usize {
    let mut pushed = pushdown_derived_predicates(select);
    pushed += pushdown_union_all_derived_predicates(db, select);
    pushed
}

fn union_derived_pushdown_enabled() -> bool {
    match crate::util::env::var("MYDB_UNION_DERIVED_PUSHDOWN") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false" | "no"
        ),
        Err(_) => true,
    }
}

#[derive(Clone)]
struct SourceInfo {
    alias: String,
    table_name: String,
    table_ref: crate::parser::TableRef,
    columns: HashSet<String>,
}

#[derive(Clone)]
struct ResolvedColumn {
    alias: String,
    column: String,
    is_derived: bool,
}

struct UnionPushdownContext {
    derived_alias: String,
    derived_columns: HashMap<String, usize>,
    source_by_alias: HashMap<String, SourceInfo>,
    source_owner_by_column: HashMap<String, Option<String>>,
}

fn pushdown_union_all_derived_predicates(db: &Database, select: &mut Select) -> usize {
    // Do not consult the rule gate unless a derived source exists. This keeps
    // the common base-table path O(1) and free of process-environment locks.
    if select.derived.is_none() && select.joins.iter().all(|join| join.derived.is_none()) {
        return 0;
    }
    if !union_derived_pushdown_enabled() {
        return 0;
    }
    if select.for_update || select.lock_clause.is_some() {
        return 0;
    }
    if select
        .joins
        .iter()
        .any(|join| join.lateral || !matches!(join.join_type, JoinType::Inner | JoinType::Cross))
    {
        return 0;
    }
    // Discover against the original scope, then mutate only the selected
    // producer. FROM order and join association do not determine eligibility.
    let mut staged = Vec::new();
    if let (Some(SubQuery::SetOp(set)), Some(table)) = (&select.derived, select.from.first()) {
        let alias = table.alias.as_deref().unwrap_or(&table.name);
        staged.push((
            None,
            union_source_pushdown_candidates(db, select, set, alias),
        ));
    }
    for (index, join) in select.joins.iter().enumerate() {
        if let Some(SubQuery::SetOp(set)) = &join.derived {
            let alias = join.table.alias.as_deref().unwrap_or(&join.table.name);
            staged.push((
                Some(index),
                union_source_pushdown_candidates(db, select, set, alias),
            ));
        }
    }
    let mut pushed = 0;
    for (index, candidates) in staged {
        let target = match index {
            None => &mut select.derived,
            Some(index) => &mut select.joins[index].derived,
        };
        let Some(SubQuery::SetOp(set)) = target else {
            continue;
        };
        for candidate in candidates {
            pushed += apply_union_branch_predicate(&mut **set, &candidate);
        }
    }
    pushed
}

fn union_source_pushdown_candidates(
    db: &Database,
    select: &Select,
    set_stmt: &crate::parser::SetOperation,
    derived_alias: &str,
) -> Vec<UnionPushdownCandidate> {
    let set_op = set_stmt;
    if !is_pure_union_all_setop(set_op) {
        return Vec::new();
    }
    if select
        .joins
        .iter()
        .any(|j| !matches!(j.join_type, JoinType::Inner | JoinType::Cross))
    {
        return Vec::new();
    }
    let Some(derived_columns) = union_output_columns(set_stmt) else {
        return Vec::new();
    };
    let sources = collect_outer_sources(db, select, &derived_alias);
    if sources.is_empty() {
        return Vec::new();
    }
    let ctx = UnionPushdownContext {
        derived_alias: derived_alias.to_ascii_lowercase(),
        derived_columns,
        source_owner_by_column: source_owner_by_column(&sources),
        source_by_alias: sources.into_iter().map(|s| (s.alias.clone(), s)).collect(),
    };

    let mut conjuncts = Vec::new();
    if let Some(selection) = &select.selection {
        split_conjuncts(selection.clone(), &mut conjuncts);
    }
    for join in &select.joins {
        if let Some(condition) = &join.condition {
            split_conjuncts(condition.clone(), &mut conjuncts);
        }
    }
    let source_filters = collect_single_source_filters(&ctx, &conjuncts);
    if source_filters.is_empty() {
        return Vec::new();
    }

    let mut candidates = Vec::new();
    for conj in &conjuncts {
        let Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } = conj
        else {
            continue;
        };
        let (Some(l), Some(r)) = (
            resolve_union_outer_column(left, &ctx),
            resolve_union_outer_column(right, &ctx),
        ) else {
            continue;
        };
        let (derived, source) = match (l.is_derived, r.is_derived) {
            (true, false) => (l, r),
            (false, true) => (r, l),
            _ => continue,
        };
        let Some(filters) = source_filters.get(&source.alias) else {
            continue;
        };
        if filters.is_empty() {
            continue;
        }
        let Some(&derived_index) = ctx.derived_columns.get(&derived.column) else {
            continue;
        };
        let Some(source_info) = ctx.source_by_alias.get(&source.alias) else {
            continue;
        };
        let Some(branch_exprs) = collect_union_branch_exprs(set_stmt, derived_index) else {
            continue;
        };
        if !union_branch_key_domains_match(db, set_stmt, derived_index, source_info, &source.column)
        {
            continue;
        }
        candidates.push(UnionPushdownCandidate {
            source: source_info.clone(),
            source_column: source.column,
            source_filters: filters.clone(),
            branch_exprs,
        });
    }

    candidates
}

struct UnionPushdownCandidate {
    source: SourceInfo,
    source_column: String,
    source_filters: Vec<Expr>,
    branch_exprs: Vec<Expr>,
}

fn repeatable_union_filter(expr: &Expr) -> bool {
    crate::planner::expr_is_deterministic(expr)
        && crate::parser::visit::try_inspect_value_expr(expr, |node| {
            // DEFAULT may evaluate a catalog expression such as UUID().
            // GROUPING belongs to an aggregate scope rather than this source.
            if matches!(
                node,
                Expr::Default(_) | Expr::DefaultQualified(_) | Expr::Grouping(_)
            ) {
                return Err(());
            }
            if let Expr::Function { name, .. } = node {
                if name.contains('.') {
                    return Err(());
                }
                if !name.eq_ignore_ascii_case("CAST")
                    && !crate::engine::cbo::function_cost::is_known_builtin_function(name)
                {
                    return Err(());
                }
            }
            Ok(true)
        })
        .is_ok()
}

/// Compare in the same typed domain before and after UNION coercion. A
/// mixed signedness, precision or collation producer retains its outer filter.
fn union_branch_key_domains_match(
    db: &Database,
    set: &crate::parser::SetOperation,
    index: usize,
    source: &SourceInfo,
    source_column: &str,
) -> bool {
    let Ok(schema) = db.table(&source.table_name) else {
        return false;
    };
    let Ok(column) = schema.column_index(source_column) else {
        return false;
    };
    let column = &schema.columns[column];
    let target = (
        column.data_type.clone(),
        column.unsigned,
        column
            .collation
            .as_deref()
            .map(crate::charset::collation_id),
    );
    fn matches<'q>(
        db: &Database,
        query: impl Into<crate::parser::QueryRef<'q>>,
        index: usize,
        target: &(crate::storage::DataTypeKind, bool, Option<u16>),
    ) -> bool {
        match query.into() {
            crate::parser::QueryRef::SetOperation(set) => {
                matches(db, &set.left, index, target) && matches(db, &set.right, index, target)
            }
            crate::parser::QueryRef::Select(select) => {
                let Some(SelectItem::Expr { expr, .. }) = select.projection.get(index) else {
                    return false;
                };
                let (qualifier, name) = match expr {
                    Expr::Identifier(name) => (None, name.as_str()),
                    Expr::CompoundIdentifier(parts) if parts.len() >= 2 => (
                        Some(parts[parts.len() - 2].as_str()),
                        parts.last().unwrap().as_str(),
                    ),
                    _ => return false,
                };
                let mut resolved = None;
                for (table, derived) in select
                    .from
                    .iter()
                    .enumerate()
                    .map(|(i, table)| (table, i == 0 && select.derived.is_some()))
                    .chain(
                        select
                            .joins
                            .iter()
                            .map(|join| (&join.table, join.derived.is_some())),
                    )
                {
                    let alias = table
                        .alias
                        .clone()
                        .unwrap_or_else(|| short_table_name(&table.name));
                    if qualifier.is_some_and(|qualifier| !qualifier.eq_ignore_ascii_case(&alias)) {
                        continue;
                    }
                    if derived {
                        return false;
                    }
                    let Ok(schema) = db.table(&table.name) else {
                        return false;
                    };
                    let Ok(column) = schema.column_index(name) else {
                        continue;
                    };
                    if resolved.is_some() {
                        return false;
                    }
                    let column = &schema.columns[column];
                    resolved = Some((
                        column.data_type.clone(),
                        column.unsigned,
                        column
                            .collation
                            .as_deref()
                            .map(crate::charset::collation_id),
                    ));
                }
                resolved.as_ref() == Some(target)
            }
            _ => false,
        }
    }
    matches(db, set, index, &target)
}

fn collect_outer_sources(db: &Database, select: &Select, derived_alias: &str) -> Vec<SourceInfo> {
    let derived_alias = derived_alias.to_ascii_lowercase();
    let mut out = Vec::new();
    let mut note = |table_ref: &crate::parser::TableRef| {
        if table_ref.generate_series.is_some() || table_ref.system_time.is_some() {
            return;
        }
        let alias = table_ref
            .alias
            .clone()
            .unwrap_or_else(|| short_table_name(&table_ref.name))
            .to_ascii_lowercase();
        if alias == derived_alias {
            return;
        }
        let Ok(schema) = db.table(&table_ref.name) else {
            return;
        };
        let columns = schema
            .iter_visible_columns()
            .map(|(_, c)| c.name.to_ascii_lowercase())
            .collect();
        out.push(SourceInfo {
            alias,
            table_name: table_ref.name.clone(),
            table_ref: table_ref.clone(),
            columns,
        });
    };
    for (idx, table) in select.from.iter().enumerate() {
        if idx == 0 && select.derived.is_some() {
            continue;
        }
        note(table);
    }
    for join in &select.joins {
        if join.derived.is_none()
            && !join.lateral
            && matches!(join.join_type, JoinType::Inner | JoinType::Cross)
        {
            note(&join.table);
        }
    }
    out
}

fn short_table_name(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_string()
}

fn source_owner_by_column(sources: &[SourceInfo]) -> HashMap<String, Option<String>> {
    let mut owners: HashMap<String, Option<String>> = HashMap::new();
    for source in sources {
        for column in &source.columns {
            owners
                .entry(column.clone())
                .and_modify(|slot| *slot = None)
                .or_insert_with(|| Some(source.alias.clone()));
        }
    }
    owners
}

fn collect_single_source_filters(
    ctx: &UnionPushdownContext,
    conjuncts: &[Expr],
) -> HashMap<String, Vec<Expr>> {
    let mut filters: HashMap<String, Vec<Expr>> = HashMap::new();
    for conj in conjuncts {
        if expr_has_subquery(conj) || !repeatable_union_filter(conj) {
            continue;
        }
        let mut aliases = HashSet::new();
        let mut touches_derived = false;
        let mut failed = false;
        collect_union_expr_sources(conj, ctx, &mut aliases, &mut touches_derived, &mut failed);
        if failed || touches_derived || aliases.len() != 1 {
            continue;
        }
        let alias = aliases.into_iter().next().unwrap();
        filters.entry(alias).or_default().push(conj.clone());
    }
    filters
}

fn collect_union_expr_sources(
    expr: &Expr,
    ctx: &UnionPushdownContext,
    aliases: &mut HashSet<String>,
    touches_derived: &mut bool,
    failed: &mut bool,
) {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            match resolve_union_outer_column(expr, ctx) {
                Some(col) if col.is_derived => *touches_derived = true,
                Some(col) => {
                    aliases.insert(col.alias);
                }
                None => *failed = true,
            }
        }
        Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Placeholder(_)
        | Expr::Grouping(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. } => {}
        Expr::Wildcard => *failed = true,
        Expr::Tuple(items) => {
            for item in items {
                collect_union_expr_sources(item, ctx, aliases, touches_derived, failed);
            }
        }
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            collect_union_expr_sources(expr, ctx, aliases, touches_derived, failed);
        }
        Expr::Binary { left, right, .. } => {
            collect_union_expr_sources(left, ctx, aliases, touches_derived, failed);
            collect_union_expr_sources(right, ctx, aliases, touches_derived, failed);
        }
        Expr::InList { expr, list, .. } => {
            collect_union_expr_sources(expr, ctx, aliases, touches_derived, failed);
            for item in list {
                collect_union_expr_sources(item, ctx, aliases, touches_derived, failed);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_union_expr_sources(expr, ctx, aliases, touches_derived, failed);
            collect_union_expr_sources(low, ctx, aliases, touches_derived, failed);
            collect_union_expr_sources(high, ctx, aliases, touches_derived, failed);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_union_expr_sources(expr, ctx, aliases, touches_derived, failed);
            collect_union_expr_sources(pattern, ctx, aliases, touches_derived, failed);
        }
        Expr::Function { args, .. } => {
            for arg in args {
                collect_union_expr_sources(arg, ctx, aliases, touches_derived, failed);
            }
        }
        Expr::GroupKeyRef { expr, .. } => {
            collect_union_expr_sources(expr, ctx, aliases, touches_derived, failed);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                collect_union_expr_sources(op, ctx, aliases, touches_derived, failed);
            }
            for condition in conditions {
                collect_union_expr_sources(condition, ctx, aliases, touches_derived, failed);
            }
            for result in results {
                collect_union_expr_sources(result, ctx, aliases, touches_derived, failed);
            }
            if let Some(else_expr) = else_result {
                collect_union_expr_sources(else_expr, ctx, aliases, touches_derived, failed);
            }
        }
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::QuantifiedComparison { .. }
        | Expr::WindowFunction { .. } => *failed = true,
    }
}

fn resolve_union_outer_column(expr: &Expr, ctx: &UnionPushdownContext) -> Option<ResolvedColumn> {
    match expr {
        Expr::Identifier(name) => {
            let column = name.to_ascii_lowercase();
            let derived = ctx.derived_columns.contains_key(&column);
            let source = ctx
                .source_owner_by_column
                .get(&column)
                .and_then(|owner| owner.clone());
            match (derived, source) {
                (true, None) => Some(ResolvedColumn {
                    alias: ctx.derived_alias.clone(),
                    column,
                    is_derived: true,
                }),
                (false, Some(alias)) => Some(ResolvedColumn {
                    alias,
                    column,
                    is_derived: false,
                }),
                _ => None,
            }
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let alias = parts[parts.len() - 2].to_ascii_lowercase();
            let column = parts[parts.len() - 1].to_ascii_lowercase();
            if alias == ctx.derived_alias && ctx.derived_columns.contains_key(&column) {
                return Some(ResolvedColumn {
                    alias,
                    column,
                    is_derived: true,
                });
            }
            if ctx
                .source_by_alias
                .get(&alias)
                .is_some_and(|source| source.columns.contains(&column))
            {
                return Some(ResolvedColumn {
                    alias,
                    column,
                    is_derived: false,
                });
            }
            None
        }
        _ => None,
    }
}

fn is_pure_union_all_setop(set_op: &crate::parser::SetOperation) -> bool {
    matches!(set_op.op, SetOp::Union)
        && set_op.all
        && set_op.order_by.is_empty()
        && set_op.limit.is_none()
        && set_op.offset.is_none()
        && statement_is_pure_union_all_leaf_or_tree(&set_op.left)
        && statement_is_pure_union_all_leaf_or_tree(&set_op.right)
}

fn statement_is_pure_union_all_leaf_or_tree<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
) -> bool {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryRef::Select(_) => true,
        crate::parser::QueryRef::SetOperation(set_op) => is_pure_union_all_setop(set_op),
        _ => false,
    }
}

fn union_output_columns<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
) -> Option<HashMap<String, usize>> {
    let stmt = stmt.into();
    let select = leftmost_union_select(stmt)?;
    let mut out: HashMap<String, Option<usize>> = HashMap::new();
    for (idx, item) in select.projection.iter().enumerate() {
        let SelectItem::Expr { expr, alias } = item else {
            continue;
        };
        let name = alias
            .clone()
            .or_else(|| direct_column_name(expr))
            .map(|s| s.to_ascii_lowercase())?;
        out.entry(name)
            .and_modify(|slot| *slot = None)
            .or_insert(Some(idx));
    }
    Some(
        out.into_iter()
            .filter_map(|(name, idx)| idx.map(|idx| (name, idx)))
            .collect(),
    )
}

fn leftmost_union_select<'q>(stmt: impl Into<crate::parser::QueryRef<'q>>) -> Option<&'q Select> {
    stmt.into().leftmost_select()
}

fn collect_union_branch_exprs<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
    index: usize,
) -> Option<Vec<Expr>> {
    let stmt = stmt.into();
    let mut out = Vec::new();
    collect_union_branch_exprs_inner(stmt, index, &mut out).then_some(out)
}

fn collect_union_branch_exprs_inner<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
    index: usize,
    out: &mut Vec<Expr>,
) -> bool {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryRef::Select(select) => {
            let Some(SelectItem::Expr { expr, .. }) = select.projection.get(index) else {
                return false;
            };
            if select.limit.is_some() || select.offset.is_some()
                || select.for_update || select.lock_clause.is_some()
                || select.from.iter().any(|table| table.system_time.is_some())
                || select.joins.iter().any(|join| join.table.system_time.is_some())
                || select.selection.as_ref().is_some_and(|expr| !repeatable_union_filter(expr))
                || select.projection.iter().any(|item| matches!(item,
                    SelectItem::Expr { expr, .. } if !repeatable_union_filter(expr)))
                || !select.group_by.is_empty() || select.having.is_some()
                || select.projection.iter().any(|item| matches!(item, SelectItem::Expr { expr, .. } if crate::parser::visit::has_window_expression(expr) || expr_has_aggregate(expr)))
                || direct_column_name(expr).is_none()
                || expr_has_aggregate(expr)
                || crate::parser::visit::has_window_expression(expr)
            {
                return false;
            }
            out.push(expr.clone());
            true
        }
        crate::parser::QueryRef::SetOperation(set_op) if is_pure_union_all_setop(set_op) => {
            collect_union_branch_exprs_inner(&set_op.left, index, out)
                && collect_union_branch_exprs_inner(&set_op.right, index, out)
        }
        _ => false,
    }
}

fn apply_union_branch_predicate<'q>(
    stmt: impl Into<crate::parser::QueryMut<'q>>,
    candidate: &UnionPushdownCandidate,
) -> usize {
    let stmt = stmt.into();
    let mut branch_exprs = candidate.branch_exprs.iter();
    apply_union_branch_predicate_inner(stmt, candidate, &mut branch_exprs)
}

fn apply_union_branch_predicate_inner<'a, 'q>(
    stmt: impl Into<crate::parser::QueryMut<'q>>,
    candidate: &UnionPushdownCandidate,
    branch_exprs: &mut std::slice::Iter<'a, Expr>,
) -> usize {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryMut::Select(select) => {
            let Some(branch_expr) = branch_exprs.next() else {
                return 0;
            };
            let predicate = Expr::InSubquery {
                expr: Box::new(branch_expr.clone()),
                subquery: Box::new(build_union_pushdown_subquery(candidate)),
                negated: false,
            };
            if selection_contains_conjunct(select.selection.as_ref(), &predicate) {
                return 0;
            }
            select.selection = Some(match select.selection.take() {
                Some(existing) => Expr::Binary {
                    left: Box::new(existing),
                    op: BinaryOp::And,
                    right: Box::new(predicate),
                },
                None => predicate,
            });
            1
        }
        crate::parser::QueryMut::SetOperation(set_op) if is_pure_union_all_setop(set_op) => {
            apply_union_branch_predicate_inner(&mut set_op.left, candidate, branch_exprs)
                + apply_union_branch_predicate_inner(&mut set_op.right, candidate, branch_exprs)
        }
        _ => 0,
    }
}

fn build_union_pushdown_subquery(candidate: &UnionPushdownCandidate) -> Select {
    let mut table_ref = candidate.source.table_ref.clone();
    table_ref.alias = Some(candidate.source.alias.clone());
    let projection_expr = Expr::CompoundIdentifier(vec![
        candidate.source.alias.clone(),
        candidate.source_column.clone(),
    ]);
    Select {
        projection: vec![SelectItem::Expr {
            expr: projection_expr,
            alias: None,
        }],
        table: Some(candidate.source.table_name.clone()),
        from: vec![table_ref],
        selection: combine_conjuncts(candidate.source_filters.clone()),
        ..Select::default()
    }
}

fn selection_contains_conjunct(selection: Option<&Expr>, needle: &Expr) -> bool {
    let Some(selection) = selection else {
        return false;
    };
    let mut conjuncts = Vec::new();
    split_conjuncts(selection.clone(), &mut conjuncts);
    conjuncts.iter().any(|conj| conj == needle)
}

fn expr_has_subquery(expr: &Expr) -> bool {
    crate::parser::visit::any_expr_matching(
        expr,
        |node| {
            matches!(
                node,
                Expr::Subquery(_)
                    | Expr::InSubquery { .. }
                    | Expr::Exists { .. }
                    | Expr::QuantifiedComparison { .. }
            )
        },
        |node| {
            crate::parser::visit::scalar_expr_children(node)
                || matches!(node, Expr::Tuple(_) | Expr::GroupKeyRef { .. })
        },
    )
}

/// Pushdown safety check on the derived body. Returns true only when
/// it's safe to push extra WHERE predicates into this derived.
fn derived_pushdown_safe(inner: &Select) -> bool {
    !inner.distinct
        && inner.limit.is_none()
        && inner.offset.is_none()
        && !inner.for_update
        && inner.having.is_none()
        && inner.named_windows.is_empty()
        && !projection_has_window(&inner.projection)
    // Group-key filters go to WHERE; eligible aggregate-output filters go
    // to HAVING. Each uses a separate projection map and safety proof.
}

fn projection_has_window(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => crate::parser::visit::has_window_expression(expr),
        _ => false,
    })
}

/// Build a map from derived-output-column-name (lowercase) to the underlying
/// inner expression for columns that are safe to push through. Unpushable
/// outputs (aggregate/window/complex projections, wildcard expansion) are left
/// out of the map; any outer conjunct that references them will fail rewrite
/// and remain above the derived table. This partial map matters for grouped
/// derived tables such as `SELECT key, avg(v) FROM ... GROUP BY key`: a
/// predicate on `key` can be pushed below the aggregate even though the
/// aggregate output itself cannot.
fn build_alias_map(inner: &Select) -> Option<HashMap<String, Expr>> {
    let group_keys: Vec<String> = inner
        .group_by
        .iter()
        .filter_map(direct_column_name)
        .collect();
    let group_active = !inner.group_by.is_empty();
    let mut map = HashMap::new();
    for item in &inner.projection {
        match item {
            SelectItem::Expr { expr, alias } => {
                if expr_has_aggregate(expr) || crate::parser::visit::has_window_expression(expr) {
                    continue;
                }
                // Only direct column references are safely pushable.
                // We could also pushdown literal projections, but those
                // are rare and the win is tiny.
                let inner_name = match expr {
                    Expr::Identifier(name) => name.to_ascii_lowercase(),
                    Expr::CompoundIdentifier(parts) => parts.last()?.to_ascii_lowercase(),
                    _ => continue,
                };
                if group_active
                    && !group_keys
                        .iter()
                        .any(|k| k.eq_ignore_ascii_case(&inner_name))
                {
                    // Column not in GROUP BY → can't push the equivalent
                    // predicate below the aggregation.
                    continue;
                }
                let out_name = alias
                    .clone()
                    .unwrap_or_else(|| inner_name.clone())
                    .to_ascii_lowercase();
                map.insert(out_name, expr.clone());
            }
            // Wildcard would force us to know all inner columns — we
            // can do this, but it requires the catalog. Skip for now.
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => continue,
        }
    }
    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

fn direct_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(name) => Some(name.to_ascii_lowercase()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|s| s.to_ascii_lowercase()),
        _ => None,
    }
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    crate::parser::visit::has_block_aggregate(expr)
}

/// Try to rewrite `conj` so every reference to a derived-output column
/// (`derived_alias.col` or unqualified `col` matching a derived alias)
/// is replaced with the inner expression it stood for. Returns `None`
/// if any reference fails the rewrite (caller leaves the conjunct at
/// the outer level).
fn rewrite_for_pushdown(
    conj: &Expr,
    derived_alias: &str,
    alias_map: &HashMap<String, Expr>,
) -> Option<Expr> {
    let mut out = conj.clone();
    let ok = rewrite_in_place(&mut out, derived_alias, alias_map);
    if ok {
        Some(out)
    } else {
        None
    }
}

fn rewrite_in_place(
    expr: &mut Expr,
    derived_alias: &str,
    alias_map: &HashMap<String, Expr>,
) -> bool {
    crate::parser::visit::try_rewrite_value_expr(expr, |node| {
        let column = match node {
            Expr::Identifier(name) => name.to_ascii_lowercase(),
            Expr::CompoundIdentifier(parts)
                if parts.len() >= 2
                    && parts[parts.len() - 2].to_ascii_lowercase() == derived_alias =>
            {
                parts[parts.len() - 1].to_ascii_lowercase()
            }
            Expr::CompoundIdentifier(_)
            | Expr::Wildcard
            | Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
            | Expr::WindowFunction { .. } => return Err(()),
            _ => return Ok(true),
        };
        *node = alias_map.get(&column).ok_or(())?.clone();
        Ok(false)
    })
    .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{BinaryOp, Expr, Literal, Select, SelectItem, SubQuery, TableRef};

    #[test]
    fn grouped_derived_aggregate_filters_move_to_having_with_scope_guards() {
        for (body, predicate, pushed) in [
            (
                "SELECT k,AVG(v) m,STDDEV_SAMP(v) sd FROM t GROUP BY k",
                "CASE m WHEN 0 THEN 0 ELSE sd/m END>1",
                true,
            ),
            ("SELECT AVG(v) m FROM t GROUP BY k", "m>2", true),
            ("SELECT k,AVG(v) m FROM t GROUP BY k LIMIT 3", "m>2", false),
            (
                "SELECT k,AVG(v) m FROM t GROUP BY k WITH ROLLUP",
                "m>2",
                false,
            ),
            (
                "SELECT k,AVG(v) m FROM t GROUP BY k",
                "some_udf(m)>2",
                false,
            ),
            ("SELECT k,AVG(v) m,RAND() r FROM t GROUP BY k", "m>2", false),
            ("SELECT k,v,AVG(v) m FROM t GROUP BY k", "v>2", false),
            ("SELECT k,AVG(v) m,MAX(v) m FROM t GROUP BY k", "m>2", false),
            ("SELECT AVG(v) m FROM t", "1=0", false),
        ] {
            let sql = format!("SELECT * FROM ({body}) d WHERE {predicate}");
            let crate::parser::Statement::Select(mut outer) =
                crate::parser::parse_statement(&sql).unwrap()
            else {
                panic!("SELECT")
            };
            assert_eq!(pushdown_derived_predicates(&mut outer) > 0, pushed, "{sql}");
            let inner = outer
                .derived
                .as_ref()
                .and_then(SubQuery::as_select)
                .unwrap();
            assert_eq!(inner.having.is_some(), pushed, "{sql}");
            assert!(
                inner.selection.is_none(),
                "aggregate results must not become a WHERE: {sql}"
            );
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
    fn gt(l: Expr, r: Expr) -> Expr {
        Expr::Binary {
            left: Box::new(l),
            op: BinaryOp::Gt,
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

    fn empty_select() -> Select {
        Select {
            distinct: false,
            projection: vec![],
            from: vec![TableRef {
                name: "t".into(),
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
            table: Some("t".into()),
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

    /// `SELECT * FROM (SELECT a, b FROM t WHERE c > 0) v WHERE v.a > 10`
    /// → derived gains `a > 10`.
    #[test]
    fn pushdown_simple_qualified_predicate() {
        let mut inner = empty_select();
        inner.projection = vec![
            SelectItem::Expr {
                expr: id("a"),
                alias: None,
            },
            SelectItem::Expr {
                expr: id("b"),
                alias: None,
            },
        ];
        inner.selection = Some(gt(id("c"), lit(0)));
        let mut outer = empty_select();
        outer.from = vec![TableRef {
            name: "v".into(),
            alias: Some("v".into()),
            system_time: None,
            index_hints: vec![],
            partitions: vec![],
            column_aliases: Vec::new(),
            generate_series: None,
        }];
        outer.derived = Some(SubQuery::Select(Box::new(inner)));
        outer.selection = Some(gt(qid("v", "a"), lit(10)));
        let pushed = pushdown_derived_predicates(&mut outer);
        assert_eq!(pushed, 1);
        assert!(outer.selection.is_none());
        let inner = outer.derived.as_ref().unwrap().as_select().unwrap();
        // The inner WHERE should now have `c > 0 AND a > 10`.
        let sel = inner.selection.as_ref().expect("inner WHERE");
        let mut conjuncts = Vec::new();
        split_conjuncts(sel.clone(), &mut conjuncts);
        assert_eq!(conjuncts.len(), 2);
    }

    /// Predicate references a column that is not in the derived
    /// projection — must NOT push.
    #[test]
    fn pushdown_unknown_column_blocked() {
        let mut inner = empty_select();
        inner.projection = vec![SelectItem::Expr {
            expr: id("a"),
            alias: None,
        }];
        let mut outer = empty_select();
        outer.from = vec![TableRef {
            name: "v".into(),
            alias: Some("v".into()),
            system_time: None,
            index_hints: vec![],
            partitions: vec![],
            column_aliases: Vec::new(),
            generate_series: None,
        }];
        outer.derived = Some(SubQuery::Select(Box::new(inner)));
        outer.selection = Some(gt(qid("v", "missing"), lit(10)));
        let pushed = pushdown_derived_predicates(&mut outer);
        assert_eq!(pushed, 0);
        assert!(outer.selection.is_some()); // stayed at outer
    }

    /// Derived has LIMIT — pushing the predicate would filter the wrong
    /// rows.
    #[test]
    fn pushdown_blocked_by_limit() {
        let mut inner = empty_select();
        inner.projection = vec![SelectItem::Expr {
            expr: id("a"),
            alias: None,
        }];
        inner.limit = Some(crate::parser::LimitValue::Rows(5));
        let mut outer = empty_select();
        outer.from = vec![TableRef {
            name: "v".into(),
            alias: Some("v".into()),
            system_time: None,
            index_hints: vec![],
            partitions: vec![],
            column_aliases: Vec::new(),
            generate_series: None,
        }];
        outer.derived = Some(SubQuery::Select(Box::new(inner)));
        outer.selection = Some(gt(qid("v", "a"), lit(10)));
        let pushed = pushdown_derived_predicates(&mut outer);
        assert_eq!(pushed, 0);
    }

    /// Aggregate projection — predicate over a non-group column can't
    /// be pushed below the aggregation.
    #[test]
    fn pushdown_blocked_by_aggregate_projection() {
        let mut inner = empty_select();
        inner.projection = vec![SelectItem::Expr {
            expr: Expr::Function {
                name: "count".into(),
                args: vec![Expr::Wildcard],
                distinct: false,
            },
            alias: Some("n".into()),
        }];
        let mut outer = empty_select();
        outer.from = vec![TableRef {
            name: "v".into(),
            alias: Some("v".into()),
            system_time: None,
            index_hints: vec![],
            partitions: vec![],
            column_aliases: Vec::new(),
            generate_series: None,
        }];
        outer.derived = Some(SubQuery::Select(Box::new(inner)));
        outer.selection = Some(gt(qid("v", "n"), lit(10)));
        let pushed = pushdown_derived_predicates(&mut outer);
        assert_eq!(pushed, 0);
    }

    /// Mixed conjuncts: some push, some stay.
    #[test]
    fn pushdown_partial() {
        let mut inner = empty_select();
        inner.projection = vec![SelectItem::Expr {
            expr: id("a"),
            alias: None,
        }];
        let mut outer = empty_select();
        outer.from = vec![TableRef {
            name: "v".into(),
            alias: Some("v".into()),
            system_time: None,
            index_hints: vec![],
            partitions: vec![],
            column_aliases: Vec::new(),
            generate_series: None,
        }];
        outer.derived = Some(SubQuery::Select(Box::new(inner)));
        outer.selection = Some(and(
            gt(qid("v", "a"), lit(10)),
            // `missing` doesn't exist in derived → stays outer
            gt(qid("v", "missing"), lit(0)),
        ));
        let pushed = pushdown_derived_predicates(&mut outer);
        assert_eq!(pushed, 1);
        assert!(outer.selection.is_some());
    }

    /// Alias rewrite: `SELECT a AS x FROM t` and outer says `v.x > 10`
    /// → derived gains `a > 10`.
    #[test]
    fn pushdown_through_alias() {
        let mut inner = empty_select();
        inner.projection = vec![SelectItem::Expr {
            expr: id("a"),
            alias: Some("x".into()),
        }];
        let mut outer = empty_select();
        outer.from = vec![TableRef {
            name: "v".into(),
            alias: Some("v".into()),
            system_time: None,
            index_hints: vec![],
            partitions: vec![],
            column_aliases: Vec::new(),
            generate_series: None,
        }];
        outer.derived = Some(SubQuery::Select(Box::new(inner)));
        outer.selection = Some(gt(qid("v", "x"), lit(10)));
        let pushed = pushdown_derived_predicates(&mut outer);
        assert_eq!(pushed, 1);
        // Inner WHERE references the underlying `a`, not the alias `x`.
        let inner = outer.derived.as_ref().unwrap().as_select().unwrap();
        let sel = inner.selection.as_ref().unwrap();
        match sel {
            Expr::Binary { left, .. } => {
                assert!(matches!(**left, Expr::Identifier(ref s) if s == "a"));
            }
            other => panic!("unexpected sel shape: {:?}", other),
        }
    }
}
