//! Window planning, execution and frame evaluation.
use super::*;

mod aggregate;
use aggregate::PartitionInput;
use aggregate::WindowAggregate;

enum PrefixState {
    CountStar(aggregate::Count<true>),
    Count(aggregate::Count<false>),
    Sum(aggregate::Numeric<false>),
    Avg(aggregate::Numeric<true>),
    Min(aggregate::Extremum<false>),
    Max(aggregate::Extremum<true>),
}

/// The canonical prefix accumulator shared by scalar and spooled batch
/// windows. It retains constant state instead of a partition-sized value list.
pub(crate) struct WindowPrefixState(PrefixState);

impl WindowPrefixState {
    pub(crate) fn new(name: &str, count_star: bool) -> Option<Self> {
        Some(Self(match name.to_ascii_uppercase().as_str() {
            "COUNT" if count_star => PrefixState::CountStar(Default::default()),
            "COUNT" => PrefixState::Count(Default::default()),
            "SUM" => PrefixState::Sum(Default::default()),
            "AVG" => PrefixState::Avg(Default::default()),
            "MIN" => PrefixState::Min(Default::default()),
            "MAX" => PrefixState::Max(Default::default()),
            _ => return None,
        }))
    }
    pub(crate) fn reset(&mut self) {
        match &mut self.0 {
            PrefixState::CountStar(state) => *state = Default::default(),
            PrefixState::Count(state) => *state = Default::default(),
            PrefixState::Sum(state) => *state = Default::default(),
            PrefixState::Avg(state) => *state = Default::default(),
            PrefixState::Min(state) => *state = Default::default(),
            PrefixState::Max(state) => *state = Default::default(),
        }
    }
    pub(crate) fn update(&mut self, value: Value) -> Result<(), QueryError> {
        match &mut self.0 {
            PrefixState::CountStar(state) => state.update(value),
            PrefixState::Count(state) => state.update(value),
            PrefixState::Sum(state) => state.update(value),
            PrefixState::Avg(state) => state.update(value),
            PrefixState::Min(state) => state.update(value),
            PrefixState::Max(state) => state.update(value),
        }
    }
    pub(crate) fn value(&self) -> Value {
        match &self.0 {
            PrefixState::CountStar(state) => state.value(),
            PrefixState::Count(state) => state.value(),
            PrefixState::Sum(state) => state.value(),
            PrefixState::Avg(state) => state.value(),
            PrefixState::Min(state) => state.value(),
            PrefixState::Max(state) => state.value(),
        }
    }
}

// Select an accumulator once per partition, before entering any row loop.
macro_rules! with_window_aggregate {
    ($name:expr, $count_star:expr, $state:ident, $body:block) => {
        match $name {
            "COUNT" if $count_star => {
                type $state = aggregate::Count<true>;
                $body
            }
            "COUNT" => {
                type $state = aggregate::Count<false>;
                $body
            }
            "SUM" => {
                type $state = aggregate::Numeric<false>;
                $body
            }
            "AVG" => {
                type $state = aggregate::Numeric<true>;
                $body
            }
            "MIN" => {
                type $state = aggregate::Extremum<false>;
                $body
            }
            "MAX" => {
                type $state = aggregate::Extremum<true>;
                $body
            }
            _ => unreachable!("simple window aggregate checked"),
        }
    };
}

#[cfg(test)]
mod aggregate_tests;

#[cfg(test)]
mod projection_rebind_tests;

fn materialize_window_sources(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &mut Select,
    registry: &SessionRegistry,
) -> Result<HashMap<String, Arc<crate::engine::api::CteResult>>, QueryError> {
    use crate::engine::api::CteResult;
    // Scalar decorrelation adds JOIN-position derived sources. Run it before
    // those sources are spooled; the later inner-SELECT preparation would
    // otherwise create derived joins that nothing materializes.
    if crate::planner::decorrelate_enabled_external() {
        crate::planner_decorrelate::scalar::decorrelate_scalar_subqueries(db, stmt);
    }
    let mut ctes: std::collections::HashMap<String, std::sync::Arc<CteResult>> =
        std::collections::HashMap::new();
    let derived_hint = stmt
        .derived
        .as_ref()
        .and_then(|derived| rollup_window_topk_hint_for_window_derived(stmt, derived));
    if let Some(derived) = stmt.derived.take() {
        let _rollup_topk_guard = derived_hint.map(|hint| {
            crate::engine::executor::select::plan::set_rollup_window_topk_hint(Some(hint))
        });
        let inner = crate::engine::executor::cte::with_cte_materialization_active(true, || {
            execute_subquery_response(db, txn, &derived, registry)
        })?;
        let alias = stmt
            .from
            .first()
            .map(|t| t.alias.clone().unwrap_or_else(|| t.name.clone()))
            .unwrap_or_else(|| "derived".to_string());
        ctes.insert(
            alias.to_ascii_lowercase(),
            Arc::new(
                crate::engine::executor::select::materialize_response_with_spill(
                    inner,
                    db.data_dir().to_path_buf(),
                )?,
            ),
        );
        if let Some(t) = stmt.from.first_mut() {
            t.name = alias.clone();
            t.alias = Some(alias);
        }
    }
    // JOIN-clause derived tables.
    for join in &mut stmt.joins {
        if let Some(derived) = join.derived.take() {
            let inner =
                crate::engine::executor::cte::with_cte_materialization_active(true, || {
                    execute_subquery_response(db, txn, &derived, registry)
                })?;
            let alias = join
                .table
                .alias
                .clone()
                .unwrap_or_else(|| join.table.name.clone());
            ctes.insert(
                alias.to_ascii_lowercase(),
                std::sync::Arc::new(
                    crate::engine::executor::select::materialize_response_with_spill(
                        inner,
                        db.data_dir().to_path_buf(),
                    )?,
                ),
            );
            join.table.name = alias.clone();
            join.table.alias = Some(alias);
        }
    }
    Ok(ctes)
}

pub(in crate::engine::executor) fn select_has_window_functions(stmt: &Select) -> bool {
    stmt.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_has_window(expr),
        _ => false,
    }) || stmt.order_by.iter().any(|o| expr_has_window(&o.expr))
}

pub(crate) fn expr_has_window(expr: &Expr) -> bool {
    crate::parser::visit::try_inspect_value_expr(expr, |expr| {
        if matches!(expr, Expr::WindowFunction { .. }) {
            Err(())
        } else {
            Ok(true)
        }
    })
    .is_err()
}

/// Replace each maximal aggregate sub-expression of `expr` with a reference to
/// an inner GROUP BY output column, so a window function computed over an
/// aggregate (`AVG(SUM(x)) OVER (...)`, TPC-DS q47) references the grouped
/// output instead of re-evaluating the aggregate over already-grouped rows.
///
/// An aggregate that structurally matches a directly-selected aggregate reuses
/// that column; any other is hoisted into `inner_projection` under a generated
/// `__winagg_N` alias (so the inner GROUP BY computes it). On encountering an
/// aggregate call we rewrite it and stop descending (it is maximal); otherwise
/// we recurse into sub-expressions.
fn hoist_window_aggregates_in_expr(
    expr: &mut Expr,
    agg_alias_map: &mut Vec<(Expr, String)>,
    inner_projection: &mut Vec<SelectItem>,
    counter: &mut usize,
) {
    crate::parser::visit::rewrite_value_expr(expr, |expr| {
        // Group bindings belong to the pre-window relation. Unwrap them
        // before matching or hoisting the expression in the materialized output.
        while let Expr::GroupKeyRef { expr: inner, .. } = expr {
            *expr = inner.as_ref().clone();
        }
        let is_hoistable_agg = match expr {
            Expr::Function { name, .. } => {
                crate::planner::is_aggregate_function(name)
                    || name.eq_ignore_ascii_case("grouping")
                    || name.eq_ignore_ascii_case(crate::parser::FT_MATCH_FUNCTION)
            }
            Expr::Grouping(_) => true,
            _ => false,
        };
        if is_hoistable_agg {
            let alias = if let Some((_, existing)) = agg_alias_map.iter().find(|(e, _)| e == expr) {
                existing.clone()
            } else {
                let generated = format!("__winagg_{counter}");
                *counter += 1;
                agg_alias_map.push((expr.clone(), generated.clone()));
                inner_projection.push(SelectItem::Expr {
                    expr: expr.clone(),
                    alias: Some(generated.clone()),
                });
                generated
            };
            *expr = Expr::Identifier(alias);
            return false;
        }

        // A nested window retains its own specification scope.
        !matches!(expr, Expr::WindowFunction { .. })
    });
}

/// Hoist aggregates appearing in a window function's arguments / PARTITION BY /
/// ORDER BY into the inner GROUP BY projection, rewriting them to reference the
/// grouped columns. No-op for ordinary windows whose operands are plain columns.
fn hoist_window_spec_aggregates(
    win_expr: &mut Expr,
    agg_alias_map: &mut Vec<(Expr, String)>,
    inner_projection: &mut Vec<SelectItem>,
    counter: &mut usize,
) {
    fn depends_on_inner_group(expr: &Expr) -> bool {
        match expr {
            Expr::Grouping(_) | Expr::GroupKeyRef { .. } => true,
            Expr::Function { name, args, .. } => {
                crate::planner::is_aggregate_function(name)
                    || name.eq_ignore_ascii_case("grouping")
                    || args.iter().any(depends_on_inner_group)
            }
            Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
                depends_on_inner_group(expr)
            }
            Expr::Binary { left, right, .. }
            | Expr::Like {
                expr: left,
                pattern: right,
                ..
            } => depends_on_inner_group(left) || depends_on_inner_group(right),
            Expr::InList { expr, list, .. } => {
                depends_on_inner_group(expr) || list.iter().any(depends_on_inner_group)
            }
            Expr::InSubquery { expr, .. } => depends_on_inner_group(expr),
            Expr::QuantifiedComparison { left, right, .. } => {
                depends_on_inner_group(left)
                    || match right {
                        crate::parser::QuantifiedComparisonRight::List(items) => {
                            items.iter().any(depends_on_inner_group)
                        }
                        crate::parser::QuantifiedComparisonRight::Subquery(_) => false,
                    }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                depends_on_inner_group(expr)
                    || depends_on_inner_group(low)
                    || depends_on_inner_group(high)
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                operand.as_deref().is_some_and(depends_on_inner_group)
                    || conditions.iter().any(depends_on_inner_group)
                    || results.iter().any(depends_on_inner_group)
                    || else_result.as_deref().is_some_and(depends_on_inner_group)
            }
            Expr::Tuple(items) => items.iter().any(depends_on_inner_group),
            // Nested query blocks own their aggregate context. A window
            // function cannot occur inside another window's specification.
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::WindowFunction { .. } => false,
            _ => false,
        }
    }

    fn hoist_dependency(
        expr: &mut Expr,
        agg_alias_map: &mut Vec<(Expr, String)>,
        inner_projection: &mut Vec<SelectItem>,
        counter: &mut usize,
    ) {
        if depends_on_inner_group(expr) {
            // Lift the complete scalar expression, not merely its aggregate
            // leaves. CASE/GROUPING/group-key combinations require the inner
            // rollup mask and $group markers together; those disappear after
            // materialisation into the window-input CTE.
            let alias = if let Some((_, alias)) = agg_alias_map.iter().find(|(e, _)| e == expr) {
                alias.clone()
            } else {
                let alias = format!("__winagg_{counter}");
                *counter += 1;
                agg_alias_map.push((expr.clone(), alias.clone()));
                inner_projection.push(SelectItem::Expr {
                    expr: expr.clone(),
                    alias: Some(alias.clone()),
                });
                alias
            };
            *expr = Expr::Identifier(alias);
        } else {
            hoist_window_aggregates_in_expr(expr, agg_alias_map, inner_projection, counter);
        }
    }

    if let Expr::WindowFunction { args, spec, .. } = win_expr {
        let crate::parser::WindowSpec {
            partition_by,
            order_by,
            ..
        } = spec.as_mut();
        for arg in args.iter_mut() {
            hoist_dependency(arg, agg_alias_map, inner_projection, counter);
        }
        for part in partition_by.iter_mut() {
            hoist_dependency(part, agg_alias_map, inner_projection, counter);
        }
        for ord in order_by.iter_mut() {
            hoist_dependency(&mut ord.expr, agg_alias_map, inner_projection, counter);
        }
    }
}

/// Part 2: replace every window function nested inside `expr` with a reference
/// to a generated `__win{N}__` output column, pushing a window spec (with its
/// aggregate operands hoisted) for each. After this returns, `expr` is free of
/// window functions and is evaluated as a post-window projection over the
/// window output columns. Drives TPC-DS q12/q20
/// (`SUM(x)*100/SUM(SUM(x)) OVER (PARTITION BY c)`).
fn extract_windows_into_specs(
    expr: &mut Expr,
    window_specs: &mut Vec<(String, Expr)>,
    agg_alias_map: &mut Vec<(Expr, String)>,
    inner_projection: &mut Vec<SelectItem>,
    hoist_counter: &mut usize,
    window_idx: &mut usize,
) {
    if matches!(expr, Expr::WindowFunction { .. }) {
        let alias = format!("__win{}__", *window_idx);
        *window_idx += 1;
        let mut win_expr = std::mem::replace(expr, Expr::Identifier(alias.clone()));
        hoist_window_spec_aggregates(
            &mut win_expr,
            agg_alias_map,
            inner_projection,
            hoist_counter,
        );
        window_specs.push((alias, win_expr));
        return;
    }
    let mut recurse = |e: &mut Expr| {
        extract_windows_into_specs(
            e,
            window_specs,
            agg_alias_map,
            inner_projection,
            hoist_counter,
            window_idx,
        )
    };
    match expr {
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => recurse(expr),
        Expr::Binary { left, right, .. } => {
            recurse(left);
            recurse(right);
        }
        Expr::Function { args, .. } => {
            for arg in args.iter_mut() {
                recurse(arg);
            }
        }
        Expr::InList { expr, list, .. } => {
            recurse(expr);
            for item in list {
                recurse(item);
            }
        }
        Expr::InSubquery { expr, .. } => recurse(expr),
        Expr::QuantifiedComparison { left, right, .. } => {
            recurse(left);
            if let crate::parser::QuantifiedComparisonRight::List(list) = right {
                for item in list {
                    recurse(item);
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                recurse(operand);
            }
            for cond in conditions.iter_mut() {
                recurse(cond);
            }
            for result in results.iter_mut() {
                recurse(result);
            }
            if let Some(else_result) = else_result {
                recurse(else_result);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            recurse(expr);
            recurse(low);
            recurse(high);
        }
        Expr::Like { expr, pattern, .. } => {
            recurse(expr);
            recurse(pattern);
        }
        Expr::GroupKeyRef { expr, .. } => recurse(expr),
        Expr::Tuple(items) => {
            for item in items {
                recurse(item);
            }
        }
        _ => {}
    }
}

/// Approximate the result column type of a Part 2 post-window projection
/// expression for result metadata. The runtime `Value` carries the precise
/// type; this only affects the advertised column type, so arithmetic / nested
/// shapes default to DOUBLE (TPC-DS window-in-expression cases are numeric).
fn window_output_expr_type(expr: &Expr) -> ColumnType {
    match expr {
        Expr::Unary { expr, .. } => window_output_expr_type(expr),
        Expr::Case {
            results,
            else_result,
            ..
        } => results
            .first()
            .or(else_result.as_deref())
            .map(window_output_expr_type)
            .unwrap_or(ColumnType::Double),
        _ => ColumnType::Double,
    }
}

/// Session-free product of [`prepare_window_execution`]: the windowed
/// projection rewritten into a windowless inner select + window specs + output
/// template, with the inner select already planned. Both the session-ful
/// top-level entry ([`execute_select_with_window`]) and the session-free
/// derived/subquery entry ([`execute_windowed_select`], which already holds a
/// `txn`) consume this.
struct WindowExecutionPlan {
    inner_planned: crate::planner::PlannedSelect,
    inner_stmt: Select,
    window_phys_specs: Vec<crate::planner::PhysicalWindowSpec>,
    output_template: Vec<OutputCol>,
    original_order_by: Vec<OrderBy>,
    /// `(projection expr, output column name)` for every top-level SELECT item,
    /// built from the *outer* projection so a final ORDER BY key that was
    /// expanded to a projection definition — a ROLLUP `GROUPING()` sum or a bare
    /// window function — can be re-pointed at its materialised output column.
    order_key_map: Vec<(Expr, String)>,
    original_limit: Option<LimitValue>,
    original_offset: Option<LimitValue>,
    final_options: WindowFinalOptions,
}

pub(in crate::engine::executor) struct WindowAnalyzeExecution {
    pub(in crate::engine::executor) plan: Box<PhysicalPlan>,
    pub(in crate::engine::executor) cte_plans: std::collections::HashMap<String, PhysicalPlan>,
    pub(in crate::engine::executor) rows_count: u64,
}

pub(in crate::engine::executor) struct WindowExplainPlan {
    pub(in crate::engine::executor) plan: Box<PhysicalPlan>,
    pub(in crate::engine::executor) cte_plans: std::collections::HashMap<String, PhysicalPlan>,
    pub(in crate::engine::executor) hint_warnings: Vec<String>,
}

/// Build the exact physical window topology used by execution without
/// running it.  Ordinary EXPLAIN and EXPLAIN ANALYZE both consume this shape;
/// ANALYZE merely attaches observations after execution.
pub(in crate::engine::executor) fn plan_windowed_select_for_explain(
    db: &Arc<Database>,
    stmt: Select,
) -> Result<WindowExplainPlan, QueryError> {
    let WindowExecutionPlan {
        inner_planned,
        inner_stmt,
        window_phys_specs,
        original_order_by,
        original_limit,
        original_offset,
        final_options,
        ..
    } = prepare_window_execution(db, stmt)?;
    let rows = crate::planner::estimated_rows(&inner_planned.physical);
    let pushed_limit =
        !final_options.distinct && original_order_by.is_empty() && original_limit.is_some();
    let pushed_limit_value = pushed_limit
        .then(|| crate::engine::executor::select::optional_limit_value(original_limit.as_ref()))
        .transpose()?
        .flatten();
    let pushed_offset_value = if pushed_limit {
        crate::engine::executor::select::limit_value_or_zero(original_offset.as_ref())?
    } else {
        0
    };
    let wrap_limit = |window: PhysicalPlan| {
        if pushed_limit {
            PhysicalPlan::Limit {
                input: Box::new(window),
                limit: pushed_limit_value,
                offset: Some(pushed_offset_value),
                binding: crate::planner::LimitBinding::default(),
                estimated_rows: pushed_limit_value.unwrap_or(0) as f64,
                cost: crate::engine::cbo::Cost::scalar(rows),
            }
        } else {
            window
        }
    };
    let hint_warnings = inner_planned.hint_warnings.clone();
    let inner_needs_materialization = inner_stmt.projection.iter().any(|p| matches!(p, SelectItem::Expr { alias: Some(a), .. } if a.starts_with("__wininput_") || a.starts_with("__windep_")))
        || !inner_stmt.group_by.is_empty()
        || crate::engine::executor::select::select_projection_has_aggregate(&inner_stmt.projection)
        || inner_stmt.projection.iter().any(|item| matches!(item,
            SelectItem::Expr { expr: Expr::Function { name, .. }, alias: Some(alias) }
                if name.eq_ignore_ascii_case(crate::parser::FT_MATCH_FUNCTION) && alias.starts_with("__winagg_")));
    if inner_needs_materialization {
        let name = "__window_input__".to_string();
        let columns = crate::engine::executor::select::describe_select_columns(db, &inner_stmt)
            .unwrap_or_default();
        let mut cte_plans = std::collections::HashMap::new();
        cte_plans.insert(name.clone(), inner_planned.physical);
        let scan = PhysicalPlan::CteScan {
            name: name.clone(),
            alias: Some(name),
            columns,
            filter: None,
            estimated_rows: rows,
            cost: crate::engine::cbo::Cost::scalar(rows),
            projected_columns: None,
        };
        Ok(WindowExplainPlan {
            plan: Box::new(wrap_limit(PhysicalPlan::WindowAgg {
                input: Box::new(scan),
                windows: window_phys_specs,
                estimated_rows: rows,
                cost: crate::engine::cbo::Cost::scalar(rows),
            })),
            cte_plans,
            hint_warnings,
        })
    } else {
        Ok(WindowExplainPlan {
            plan: Box::new(wrap_limit(PhysicalPlan::WindowAgg {
                input: Box::new(inner_planned.physical),
                windows: window_phys_specs,
                estimated_rows: rows,
                cost: crate::engine::cbo::Cost::scalar(rows),
            })),
            cte_plans: std::collections::HashMap::new(),
            hint_warnings,
        })
    }
}

/// Rewrite a window-bearing SELECT into its windowless inner query + window
/// specs and plan the inner. Session-free: the only thing the caller still owes
/// is a `txn` to run the planned window over (see `execute_window_plan_result`).
fn prepare_window_execution(
    db: &Arc<Database>,
    mut stmt: Select,
) -> Result<WindowExecutionPlan, QueryError> {
    // Describing/preflighting the same window may precede actual lowering.
    // Borrow its bound for this plan, and hide it from nested query blocks;
    // consuming the TLS hint here made the execution plan lose Top-K after
    // the first metadata-only preparation.
    let topk_hint = peek_window_topk_hint();
    let _nested_hint_scope = set_window_topk_hint(None);
    // Resolve SELECT aliases in window specifications before ordinary binding.
    let aliases: Vec<(String, Expr)> = stmt
        .projection
        .iter()
        .filter_map(|p| match p {
            SelectItem::Expr {
                expr,
                alias: Some(alias),
            } => Some((alias.clone(), expr.clone())),
            _ => None,
        })
        .collect();
    if let Some(having) = &stmt.having {
        let mut invalid = None;
        crate::planner::walk_expr_for_idents(having, &mut |e| {
            if let Expr::Identifier(name) = e {
                if aliases
                    .iter()
                    .any(|(a, e)| a.eq_ignore_ascii_case(name) && expr_has_window(e))
                {
                    invalid = Some(name.clone());
                }
            }
        });
        if let Some(alias) = invalid {
            return Err(error(
                crate::error::catalog::ER_WINDOW_INVALID_WINDOW_FUNC_ALIAS_USE,
                format!("window alias '{alias}' is not allowed in HAVING"),
            ));
        }
    }
    struct WindowAliases<'a>(&'a std::collections::HashMap<String, Expr>);
    impl crate::parser::visit::AstVisitorMut for WindowAliases<'_> {
        fn enter_expr(&mut self, expr: &mut Expr) {
            if let Expr::WindowFunction { spec, .. } = expr {
                let crate::parser::WindowSpec {
                    partition_by,
                    order_by,
                    ..
                } = spec.as_mut();
                for key in partition_by
                    .iter_mut()
                    .chain(order_by.iter_mut().map(|o| &mut o.expr))
                {
                    *key = crate::planner::resolve_having_alias_expr(key.clone(), self.0);
                }
            }
        }
    }
    // Input columns take precedence over SELECT aliases in window keys.
    let input_names: std::collections::HashSet<String> =
        if aliases.iter().any(|(_, e)| !expr_has_window(e)) {
            let mut source_select = stmt.clone();
            source_select.projection = vec![SelectItem::Wildcard];
            source_select.order_by.clear();
            source_select.named_windows.clear();
            source_select.having = None;
            crate::engine::executor::select::describe_select_columns(db, &source_select)?
                .into_iter()
                .map(|c| c.name.to_ascii_lowercase())
                .collect()
        } else {
            std::collections::HashSet::new()
        };
    let window_aliases = aliases
        .iter()
        .filter(|(a, e)| !expr_has_window(e) && !input_names.contains(&a.to_ascii_lowercase()))
        .map(|(a, e)| (a.to_ascii_lowercase(), e.clone()))
        .collect();
    for item in &mut stmt.projection {
        if let SelectItem::Expr { expr, .. } = item {
            crate::parser::visit::visit_expr_mut(&mut WindowAliases(&window_aliases), expr);
        }
    }
    for order in &mut stmt.order_by {
        crate::parser::visit::visit_expr_mut(&mut WindowAliases(&window_aliases), &mut order.expr);
    }
    // Window planning rewrites the projection before the ordinary SELECT
    // binder runs. Validate the original tree first so deterministic
    // charset/type errors in nested window arguments win over an unrelated
    // planner-routing error (notably LEAD/LAG defaults in constant SELECTs).
    crate::engine::executor::select::validate_select_static_collations(&stmt)?;
    // Tier 0 #1: predicate normalization on the outer select
    // (the inner stmt below inherits it).
    crate::planner::normalize_select_predicates(db, &mut stmt);
    // Rollup-bind the outer statement up front so projection AND ORDER BY use
    // the same bound `GROUPING([idx])` form. The window rewrite captures
    // `original_order_by` here and later re-points its projection-defined keys
    // (e.g. a ROLLUP `lochierarchy` alias) at the materialised output column; an
    // unbound ORDER BY `GROUPING(col)` would not match the bound projection and
    // its sort key would be silently dropped (TPC-DS q36/q70/q86). Idempotent.
    if stmt.group_by_modifier != crate::parser::GroupByModifier::None
        || !stmt.grouping_sets.is_empty()
    {
        crate::planner::bind_rollup_select_for_execution(db, &mut stmt)?;
    }
    if stmt
        .projection
        .iter()
        .any(|p| matches!(p, SelectItem::Wildcard | SelectItem::QualifiedWildcard(_)))
    {
        let mut expanded = Vec::new();
        for item in &stmt.projection {
            if matches!(
                item,
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_)
            ) {
                let mut input = stmt.clone();
                input.projection = vec![item.clone()];
                input.order_by.clear();
                for column in crate::engine::executor::select::describe_select_columns(db, &input)?
                {
                    let expr = if column.source.table.is_empty() {
                        Expr::Identifier(column.name.clone())
                    } else {
                        Expr::CompoundIdentifier(vec![column.source.table, column.name.clone()])
                    };
                    expanded.push(SelectItem::Expr {
                        expr,
                        alias: Some(column.name),
                    });
                }
            } else {
                expanded.push(item.clone());
            }
        }
        stmt.projection = expanded;
    }
    let mut final_options = WindowFinalOptions {
        distinct: stmt.distinct,
        hidden: 0,
    };
    stmt.distinct = false;
    // Sort expressions, including ORDER-only windows, stay materialized until the final sort.
    for order in &stmt.order_by {
        if !matches!(order.expr, Expr::Literal(crate::parser::Literal::Int(_) | crate::parser::Literal::UInt(_)))
            && !stmt.projection.iter().any(|p| match p {
                SelectItem::Expr { expr, alias } => expr == &order.expr || matches!((&order.expr,alias), (Expr::Identifier(name),Some(alias)) if name.eq_ignore_ascii_case(alias)),
                _ => false,
            })
        {
            let alias = format!("__winorder_{}", final_options.hidden);
            stmt.projection.push(SelectItem::Expr {
                expr: order.expr.clone(),
                alias: Some(alias),
            });
            final_options.hidden += 1;
        }
    }
    let original_projection = stmt.projection.clone();
    let original_order_by = stmt.order_by.clone();
    let original_limit = stmt.limit.clone();
    let original_offset = stmt.offset.clone();
    let mut inner_stmt = stmt;
    inner_stmt.order_by.clear();
    inner_stmt.limit = None;
    inner_stmt.offset = None;
    // Replace each projection that contains a window function with the dependent
    // expressions: we project the order/partition columns and any underlying args.
    let mut inner_projection: Vec<SelectItem> = Vec::new();
    let mut window_specs: Vec<(String, Expr)> = Vec::new(); // (placeholder column, window expr)
    let mut output_template: Vec<OutputCol> = Vec::new();
    let mut window_idx = 0usize;
    // Aggregates lifted out of window operands into hidden inner GROUP BY
    // columns. Keyed by the aggregate expression so the same aggregate used in
    // both a window operand and the surrounding expression of one projection
    // item (TPC-DS q12/q20: `SUM(v)*100/SUM(SUM(v)) OVER (...)`) shares one
    // hidden column. We deliberately do *not* reuse directly-selected
    // aggregates: their output column name is not reliably predictable here, so
    // we always materialise a fresh `__winagg_N` (computing it twice at worst).
    let mut agg_alias_map: Vec<(Expr, String)> = Vec::new();
    let mut hoist_counter = 0usize;
    for item in &original_projection {
        match item {
            SelectItem::Expr { expr, alias } if expr_has_window(expr) => {
                // Part 1: a bare window function. Its arguments / PARTITION BY /
                // ORDER BY may be aggregates (computed below the window) — those
                // are hoisted into the inner GROUP BY. A window function nested
                // inside a larger expression is Part 2 (handled via OutputCol::
                // Expr); a non-window-function that merely contains one is the
                // `other` arm's job, so the only thing rejected here is nothing.
                let alias_name = alias.clone().unwrap_or_else(|| match expr {
                    Expr::WindowFunction { name, .. } => name.to_lowercase(),
                    _ => format!("__win{window_idx}__"),
                });
                window_idx += 1;
                if matches!(expr, Expr::WindowFunction { .. }) {
                    // Bare window function: hoist any aggregate operands so the
                    // window evaluates over the grouped output, then run it.
                    let mut win_expr = expr.clone();
                    hoist_window_spec_aggregates(
                        &mut win_expr,
                        &mut agg_alias_map,
                        &mut inner_projection,
                        &mut hoist_counter,
                    );
                    window_specs.push((alias_name.clone(), win_expr));
                    output_template.push(OutputCol::Window {
                        name: alias_name,
                        spec_index: window_specs.len() - 1,
                    });
                } else {
                    // Part 2: window function(s) nested inside a larger
                    // expression. Extract each into its own spec (with aggregate
                    // hoisting), then evaluate the surrounding expression as a
                    // post-window projection.
                    let mut out_expr = expr.clone();
                    extract_windows_into_specs(
                        &mut out_expr,
                        &mut window_specs,
                        &mut agg_alias_map,
                        &mut inner_projection,
                        &mut hoist_counter,
                        &mut window_idx,
                    );
                    // Aggregates that live in the surrounding expression (outside
                    // any window) are also computed by the inner GROUP BY.
                    hoist_window_aggregates_in_expr(
                        &mut out_expr,
                        &mut agg_alias_map,
                        &mut inner_projection,
                        &mut hoist_counter,
                    );
                    let column_type = window_output_expr_type(&out_expr);
                    output_template.push(OutputCol::Expr {
                        name: alias_name,
                        expr: out_expr,
                        column_type,
                    });
                }
            }
            other => {
                match other {
                    SelectItem::Expr { expr, alias } => {
                        let output_name = alias.clone().unwrap_or_else(|| expr_name(expr));
                        let source_name = format!("__wininput_{}", inner_projection.len());
                        inner_projection.push(SelectItem::Expr {
                            expr: expr.clone(),
                            alias: Some(source_name.clone()),
                        });
                        // Every non-window projection is evaluated by the
                        // inner SELECT and materialized under its output name.
                        // Re-evaluating it after WindowAgg is both redundant
                        // and incorrect for aggregates/GROUPING expressions:
                        // their base columns and rollup markers no longer
                        // exist in the materialized row.
                        output_template.push(OutputCol::Direct {
                            name: output_name.clone(),
                            source_name,
                        });
                    }
                    SelectItem::Wildcard => {
                        inner_projection.push(other.clone());
                        output_template.push(OutputCol::Direct {
                            name: "*".to_string(),
                            source_name: "*".to_string(),
                        });
                    }
                    SelectItem::QualifiedWildcard(q) => {
                        inner_projection.push(other.clone());
                        let wildcard = format!("{q}.*");
                        output_template.push(OutputCol::Direct {
                            name: wildcard.clone(),
                            source_name: wildcard,
                        });
                    }
                }
            }
        }
    }
    // Materialize each dependency with a unique identity; never collapse a.x
    // and b.x to their common display name at the window boundary.
    struct WindowDeps<'a> {
        projection: &'a mut Vec<SelectItem>,
        bindings: Vec<(Expr, String)>,
        window_outputs: Vec<String>,
    }
    impl crate::parser::visit::AstVisitorMut for WindowDeps<'_> {
        fn enter_expr(&mut self, expr: &mut Expr) {
            // A subquery owns its column scope: materialize its value per
            // input row instead of lifting its inner columns to this SELECT.
            let subquery = matches!(
                expr,
                Expr::Subquery(_)
                    | Expr::InSubquery { .. }
                    | Expr::Exists { .. }
                    | Expr::QuantifiedComparison {
                        right: crate::parser::QuantifiedComparisonRight::Subquery(_),
                        ..
                    }
            );
            if !subquery && !matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                return;
            }
            if let Some((_, alias)) = self.bindings.iter().find(|(key, _)| key == expr) {
                *expr = Expr::Identifier(alias.clone());
                return;
            }
            if let Expr::Identifier(name) = expr {
                if self.window_outputs.iter().any(|a| a == name) {
                    return;
                }
                if self
                    .projection
                    .iter()
                    .any(|p| matches!(p, SelectItem::Expr { alias: Some(a), .. } if a == name))
                {
                    return;
                }
            }
            let alias = format!("__windep_{}", self.bindings.len());
            self.projection.push(SelectItem::Expr {
                expr: expr.clone(),
                alias: Some(alias.clone()),
            });
            self.bindings.push((expr.clone(), alias.clone()));
            *expr = Expr::Identifier(alias);
        }
    }
    let mut deps = WindowDeps {
        // Direct projections already materialize these exact input columns.
        // Reuse their unique output identities for window dependencies rather
        // than retaining a second copy of every partition/order/argument lane.
        // Structural matching keeps differently qualified columns distinct.
        bindings: inner_projection
            .iter()
            .filter_map(|item| match item {
                SelectItem::Expr {
                    expr: expr @ (Expr::Identifier(_) | Expr::CompoundIdentifier(_)),
                    alias: Some(alias),
                } => Some((expr.clone(), alias.clone())),
                _ => None,
            })
            .collect(),
        projection: &mut inner_projection,
        window_outputs: window_specs.iter().map(|(a, _)| a.clone()).collect(),
    };
    for (_, expr) in &mut window_specs {
        crate::parser::visit::visit_expr_mut(&mut deps, expr);
    }
    for output in &mut output_template {
        if let OutputCol::Expr { expr, .. } = output {
            crate::parser::visit::visit_expr_mut(&mut deps, expr);
        }
    }
    inner_stmt.projection = inner_projection;
    // Run the same SELECT preparation as the ordinary executor. The window
    // rewrite executes this inner SELECT directly, so planner-only rewrites
    // would not reach projection/group evaluation here. This includes rollup
    // binding, GROUP/ORDER alias resolution, predicate normalization, enum/set
    // rewrites, validation, and constant folding.
    crate::engine::executor::select::prepare_select_for_planning(db, &mut inner_stmt)?;
    // Build a PhysicalPlan::WindowAgg wrapping the inner plan so window
    // execution participates in the unified plan-tree pipeline (and shows up
    // in EXPLAIN). The inner Select has already been qualified by the
    // calling dispatcher, so we go straight to plan_select.
    let inner_planned = crate::planner::plan_select(db, &inner_stmt)?;
    // Window values are evaluated over the materialised output of
    // `inner_stmt`. Derive their physical type from that exact schema instead
    // of labelling every SUM/AVG as DOUBLE. The latter is observably wrong for
    // exact inputs: AVG(SUM(DECIMAL)) produces a DecimalCompact value, and a
    // row-shaped compatibility operator would legitimately encode it as NULL
    // when asked to populate a DOUBLE lane (TPC-DS q47/q51/q53/q63/q89).
    let inner_output_columns =
        crate::engine::executor::select::describe_select_columns(db, &inner_stmt)?;
    let resolve_window_column = |expr: &Expr| {
        let name = match expr {
            Expr::Identifier(name) => name.as_str(),
            Expr::CompoundIdentifier(parts) => parts.last()?.as_str(),
            _ => return None,
        };
        let column = inner_output_columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))?;
        Some(crate::engine::expr_type::ExprType::from_column_type(
            column.column_type,
            &column.source,
        ))
    };
    let resolve_placeholder = |_: usize| None;
    let window_type_context = crate::engine::expr_type::TypeContext::context_free(
        &resolve_window_column,
        &resolve_placeholder,
    );
    // Apply the Top-N pushdown hint the enclosing derived-table executor set
    // for this window subquery (`alias <= k`). Only honoured for a single
    // ranking window whose alias matches; multi-window groups keep every row.
    let single_window = window_specs.len() == 1;
    let mut window_phys_specs: Vec<crate::planner::PhysicalWindowSpec> = Vec::new();
    for (alias, win_expr) in &window_specs {
        let Expr::WindowFunction {
            name, args, spec, ..
        } = win_expr
        else {
            unreachable!("window_specs must hold WindowFunction expressions");
        };
        let crate::parser::WindowSpec {
            partition_by,
            order_by,
            frame,
            ..
        } = spec.as_ref();

        if let Some(frame) = frame {
            if matches!(frame.start, WindowFrameBound::UnboundedFollowing) {
                return Err(error(
                    crate::error::catalog::ER_WINDOW_FRAME_START_ILLEGAL,
                    "window frame cannot start with UNBOUNDED FOLLOWING",
                ));
            }
            if matches!(frame.end, WindowFrameBound::UnboundedPreceding) {
                return Err(error(
                    crate::error::catalog::ER_WINDOW_FRAME_END_ILLEGAL,
                    "window frame cannot end with UNBOUNDED PRECEDING",
                ));
            }
        }
        let output_type =
            crate::engine::expr_type::infer_expr_type(win_expr, &window_type_context).column_type();
        let top_k = topk_hint.as_ref().and_then(|(col, k)| {
            (single_window
                && col.eq_ignore_ascii_case(alias)
                && RankKind::from_name(name).is_some())
            .then_some(*k)
        });
        window_phys_specs.push(crate::planner::PhysicalWindowSpec {
            name: name.clone(),
            args: args.clone(),
            partition_by: partition_by.clone(),
            order_by: order_by.clone(),
            frame: frame.clone(),
            output_alias: alias.clone(),
            output_type,
            top_k,
        });
    }
    // Map every outer projection definition to its output column name so the
    // final ORDER BY can re-point expanded keys (ROLLUP `GROUPING()` sums, bare
    // window functions) at the already-materialised column.
    let order_key_map: Vec<(Expr, String)> = original_projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| expr_name(expr));
                Some((expr.clone(), name))
            }
            _ => None,
        })
        .collect();
    Ok(WindowExecutionPlan {
        inner_planned,
        inner_stmt,
        window_phys_specs,
        output_template,
        original_order_by,
        order_key_map,
        original_limit,
        original_offset,
        final_options,
    })
}

/// Session-ful top-level window entry: set up the statement transaction from
/// `session`, then delegate to the session-free [`execute_windowed_select`]
/// (shared with the derived/subquery path). The read-only determination is the
/// same for the original window SELECT as for its windowless inner rewrite —
/// the rewrite only touches the projection / ORDER BY / LIMIT, not locking — so
/// it is taken on `stmt` directly.
/// Dispatch a plain `SELECT` to the appropriate read-view / transaction
/// path. Extracted from the `Statement::Select` arm so the
/// SQL_CALC_FOUND_ROWS pre-pass can reuse the exact same routing after
/// stripping the user's LIMIT/OFFSET.
///
/// P2-C3: `SELECT SQL_CALC_FOUND_ROWS ... [LIMIT n [OFFSET m]]`. Strip the
/// user's LIMIT/OFFSET so the planner emits no TopK/TopWindow (every row
/// flows), materialise the full result to learn the pre-LIMIT total, then
/// re-apply the user's window in memory. `FOUND_ROWS()` reads the total via
/// `StreamingResultSet.found_rows` (the protocol/exec layers already prefer
/// it over the sent-row count). This deliberately abandons streaming for
/// these queries — MySQL does the same, which is why it deprecated the
/// modifier in 8.0.17.
pub(super) fn execute_select_with_window(
    db: &Arc<Database>,
    session: &mut SessionContext,
    stmt: Select,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    if can_use_read_only_select(session, &stmt) {
        let (txn, _snapshot_guard) = tx::autocommit_read_only_view_with_guard(db, session);
        return execute_windowed_select(db, txn, stmt, registry);
    }

    if can_use_lazy_read_only_transaction_select(session, &stmt) {
        let (txn, _snapshot_guard) = tx::lazy_read_only_transaction_view_with_guard(db, session);
        return execute_windowed_select(db, txn, stmt, registry);
    }

    execute_in_transaction_with_session(db, session, |db, session, txn| {
        if !stmt.for_update && txn.isolation == IsolationLevel::Serializable {
            acquire_serializable_select_table_locks(db, session, txn, &stmt)?;
        }
        let ro_snapshot = tx::session_read_only_snapshot(session);
        let txn = if stmt.for_update || txn.isolation == IsolationLevel::Serializable {
            db.session_statement_view(txn.txn_id, txn.isolation, ro_snapshot)
        } else if txn.isolation.uses_repeatable_snapshot() {
            let mut view = db.session_statement_view(txn.txn_id, txn.isolation, ro_snapshot);
            if let Some(snapshot_scn) = session.txn_snapshot_scn {
                view.snapshot_scn = snapshot_scn;
            } else {
                session.pin_transaction_snapshot(db, view.snapshot_scn);
            }
            view
        } else if txn.isolation == IsolationLevel::ReadUncommitted {
            let mut view = db.session_statement_view(txn.txn_id, txn.isolation, ro_snapshot);
            view.dirty_read = true;
            view
        } else {
            txn
        };
        execute_windowed_select(db, txn, stmt.clone(), registry)
    })
}

/// Session-free window entry for a window-bearing SELECT reached as a derived
/// table / subquery (the session-ful top-level path is
/// `execute_select_with_window`). The materialization bridge already holds a
/// `txn`, but `plan_select` cannot plan a derived-table FROM directly, so we
/// first materialize any derived FROM/JOIN sources into transient CTEs and
/// rewrite the references to name them; `prepare_window_execution` then plans
/// the inner over those `CteScan`s and `execute_window_plan_result` runs the
/// window. This is what makes `RANK() OVER (...)` and friends work inside a
/// derived table (e.g. TPC-DS q67's middle query).
pub(in crate::engine::executor) fn execute_windowed_select(
    db: &Arc<Database>,
    txn: TxnView,
    mut stmt: Select,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let _snapshot_guard =
        (!txn.snapshot_registered).then(|| db.register_statement_snapshot(txn.snapshot_scn));
    let mut ctes = materialize_window_sources(db, txn, &mut stmt, registry)?;
    // A FROM-less window query still has one logical input row in MySQL.
    // Feed that row through the same CTE-scan/window pipeline as every other
    // source, instead of asking the CBO for a catalog table named `$const`.
    if stmt.from.is_empty() && stmt.table.is_none() {
        let name = "__window_const".to_string();
        ctes.insert(
            name.clone(),
            std::sync::Arc::new(crate::engine::executor::cte::materialize_rows_with_spill(
                Vec::new(),
                vec![Vec::new()],
                db.data_dir().to_path_buf(),
            )?),
        );
        stmt.table = Some(name.clone());
        stmt.from.push(crate::parser::TableRef {
            name,
            alias: None,
            system_time: None,
            index_hints: Vec::new(),
            partitions: Vec::new(),
            column_aliases: Vec::new(),
            generate_series: None,
        });
    }
    let run = move || {
        let WindowExecutionPlan {
            inner_planned,
            inner_stmt,
            window_phys_specs,
            output_template,
            original_order_by,
            order_key_map,
            original_limit,
            original_offset,
            final_options,
        } = prepare_window_execution(db, stmt)?;
        execute_window_plan_result(
            db,
            txn,
            &inner_planned.physical,
            &inner_stmt,
            &window_phys_specs,
            output_template,
            original_order_by,
            order_key_map,
            original_limit,
            original_offset,
            final_options,
        )
    };
    if ctes.is_empty() {
        run()
    } else {
        crate::engine::executor::cte::with_cte_registry_extended(ctes, run)
    }
}

pub(in crate::engine::executor) fn execute_windowed_select_for_analyze(
    db: &Arc<Database>,
    txn: TxnView,
    mut stmt: Select,
    registry: &SessionRegistry,
) -> Result<WindowAnalyzeExecution, QueryError> {
    let ctes = materialize_window_sources(db, txn, &mut stmt, registry)?;
    let run = move || {
        let WindowExecutionPlan {
            inner_planned,
            inner_stmt,
            window_phys_specs,
            output_template,
            original_order_by,
            order_key_map,
            original_limit,
            original_offset,
            final_options,
        } = prepare_window_execution(db, stmt)?;
        execute_window_plan_result_for_analyze(
            db,
            txn,
            inner_planned.physical,
            &inner_stmt,
            &window_phys_specs,
            output_template,
            original_order_by,
            order_key_map,
            original_limit,
            original_offset,
            final_options,
        )
    };
    if ctes.is_empty() {
        run()
    } else {
        crate::engine::executor::cte::with_cte_registry_extended(ctes, run)
    }
}

fn plan_is_rollup_aggregate(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::HashRollupAggregate { .. }
            | PhysicalPlan::StreamingRollupAggregate { .. }
            | PhysicalPlan::ParallelRollupAggregate { .. }
    )
}

fn projection_alias_exprs(stmt: &Select) -> std::collections::HashMap<String, Expr> {
    let mut aliases = std::collections::HashMap::new();
    for item in &stmt.projection {
        if let SelectItem::Expr { expr, alias } = item {
            let name = alias.clone().unwrap_or_else(|| expr_name(expr));
            aliases.insert(name.to_ascii_lowercase(), expr.clone());
            if let Some(column_name) = crate::planner::expr_column_name(expr) {
                aliases
                    .entry(column_name.to_ascii_lowercase())
                    .or_insert_with(|| expr.clone());
            }
        }
    }
    aliases
}

fn rebind_window_expr_to_inner_projection(
    expr: &Expr,
    aliases: &std::collections::HashMap<String, Expr>,
) -> Expr {
    let mut rebound = expr.clone();
    crate::parser::visit::rewrite_value_expr(&mut rebound, |node| {
        // Replacement provenance may retain the same identifier. Leave the
        // replacement opaque instead of binding it recursively a second time.
        if let Some(name) = crate::planner::expr_column_name(node) {
            // Most parsed identifiers are already normalized. Borrow their
            // lookup key instead of allocating a temporary lowercase String.
            let mapped = if name.bytes().any(|byte| byte.is_ascii_uppercase()) {
                aliases.get(&name.to_ascii_lowercase())
            } else {
                aliases.get(name)
            };
            if let Some(mapped) = mapped {
                *node = mapped.clone();
                return false;
            }
        }
        true
    });
    rebound
}

fn hint_expr_matches_group_by(expr: &Expr, group: &Expr) -> bool {
    fn unwrap(expr: &Expr) -> &Expr {
        match expr {
            Expr::GroupKeyRef { expr, .. } => expr,
            other => other,
        }
    }
    let expr = unwrap(expr);
    let group = unwrap(group);
    if expr == group {
        return true;
    }
    match (
        crate::planner::expr_column_name(expr),
        crate::planner::expr_column_name(group),
    ) {
        (Some(a), Some(b)) => a.eq_ignore_ascii_case(&b),
        _ => false,
    }
}

fn bind_hint_expr_to_rollup_group(expr: Expr, group_by: &[Expr]) -> Expr {
    if let Some((index, _)) = group_by
        .iter()
        .enumerate()
        .find(|(_, group)| hint_expr_matches_group_by(&expr, group))
    {
        Expr::GroupKeyRef {
            index,
            expr: Box::new(expr),
        }
    } else {
        expr
    }
}

fn rebind_window_expr_to_rollup_derived_projection(
    expr: &Expr,
    aliases: &std::collections::HashMap<String, Expr>,
    group_by: &[Expr],
) -> Expr {
    let rebound = rebind_window_expr_to_inner_projection(expr, aliases);
    bind_hint_expr_to_rollup_group(rebound, group_by)
}

fn rollup_window_topk_hint_for_inner(
    inner_plan: &PhysicalPlan,
    inner_stmt: &Select,
    window_specs: &[crate::planner::PhysicalWindowSpec],
) -> Option<crate::engine::executor::select::plan::RollupWindowTopKHint> {
    if !plan_is_rollup_aggregate(inner_plan) || window_specs.len() != 1 {
        return None;
    }
    let spec = &window_specs[0];
    let k = spec.top_k?;
    let kind = RankKind::from_name(&spec.name)?;
    if spec.order_by.is_empty() {
        return None;
    }
    let aliases = projection_alias_exprs(inner_stmt);
    let partition_by = spec
        .partition_by
        .iter()
        .map(|expr| rebind_window_expr_to_inner_projection(expr, &aliases))
        .collect();
    let order_by = spec
        .order_by
        .iter()
        .map(|item| {
            let mut item = item.clone();
            item.expr = rebind_window_expr_to_inner_projection(&item.expr, &aliases);
            item
        })
        .collect();
    Some(
        crate::engine::executor::select::plan::RollupWindowTopKHint {
            partition_by,
            order_by,
            k,
            kind,
        },
    )
}

pub(in crate::engine::executor) fn rollup_window_topk_hint_for_window_derived(
    window_stmt: &Select,
    derived: &SubQuery,
) -> Option<crate::engine::executor::select::plan::RollupWindowTopKHint> {
    let (topk_col, k) = peek_window_topk_hint()?;
    let SubQuery::Select(inner) = derived else {
        return None;
    };
    if window_stmt.selection.is_some()
        || !window_stmt.joins.is_empty()
        || window_stmt.limit.is_some()
        || window_stmt.offset.is_some()
        || inner.limit.is_some()
        || inner.offset.is_some()
        || inner.group_by_modifier != crate::parser::GroupByModifier::Rollup
        || window_stmt
            .projection
            .iter()
            .filter(|item| matches!(item, SelectItem::Expr { expr, .. } if expr_has_window(expr)))
            .count()
            != 1
    {
        return None;
    }
    let mut matched: Option<(&Expr, RankKind)> = None;
    for item in &window_stmt.projection {
        let SelectItem::Expr { expr, alias } = item else {
            continue;
        };
        let Expr::WindowFunction { name, .. } = expr else {
            continue;
        };
        let out_name = alias.clone().unwrap_or_else(|| expr_name(expr));
        if out_name.eq_ignore_ascii_case(&topk_col) {
            let kind = RankKind::from_name(name)?;
            if matched.is_some() {
                return None;
            }
            matched = Some((expr, kind));
        }
    }
    let (win_expr, kind) = matched?;
    let Expr::WindowFunction { spec, .. } = win_expr else {
        return None;
    };
    let crate::parser::WindowSpec {
        partition_by,
        order_by,
        ..
    } = spec.as_ref();

    if order_by.is_empty() {
        return None;
    }
    let column_key =
        |expr: &Expr| matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_));
    if !partition_by.iter().all(column_key) || !order_by.iter().all(|item| column_key(&item.expr)) {
        return None;
    }
    let aliases = projection_alias_exprs(inner);
    Some(
        crate::engine::executor::select::plan::RollupWindowTopKHint {
            partition_by: partition_by
                .iter()
                .map(|expr| {
                    rebind_window_expr_to_rollup_derived_projection(expr, &aliases, &inner.group_by)
                })
                .collect(),
            order_by: order_by
                .iter()
                .map(|item| {
                    let mut item = item.clone();
                    item.expr = rebind_window_expr_to_rollup_derived_projection(
                        &item.expr,
                        &aliases,
                        &inner.group_by,
                    );
                    item
                })
                .collect(),
            k,
            kind,
        },
    )
}

/// Share limit pushdown decisions without changing either execution mode's
/// ownership of the input plan or its streaming/materialized result policy.
fn window_limit_wrapper(
    distinct: bool,
    ordered: bool,
    original_limit: &mut Option<LimitValue>,
    original_offset: &mut Option<LimitValue>,
    inner_rows_count: f64,
) -> Result<impl Fn(PhysicalPlan) -> PhysicalPlan, QueryError> {
    let pushed_limit = !distinct && !ordered && original_limit.is_some();
    let pushed_limit_value = pushed_limit
        .then(|| crate::engine::executor::select::optional_limit_value(original_limit.as_ref()))
        .transpose()?
        .flatten();
    let pushed_offset_value = if pushed_limit {
        crate::engine::executor::select::limit_value_or_zero(original_offset.as_ref())?
    } else {
        0
    };
    if pushed_limit {
        *original_limit = None;
        *original_offset = None;
    }
    Ok(move |window: PhysicalPlan| {
        if pushed_limit {
            PhysicalPlan::Limit {
                input: Box::new(window),
                limit: pushed_limit_value,
                offset: Some(pushed_offset_value),
                binding: crate::planner::LimitBinding::default(),
                estimated_rows: pushed_limit_value.unwrap_or(0) as f64,
                cost: crate::engine::cbo::Cost::scalar(inner_rows_count),
            }
        } else {
            window
        }
    })
}

fn window_input_needs_materialization(inner_stmt: &Select) -> bool {
    inner_stmt.projection.iter().any(|p| matches!(p, SelectItem::Expr { alias: Some(a), .. } if a.starts_with("__wininput_") || a.starts_with("__windep_")))
        || !inner_stmt.group_by.is_empty()
        || crate::engine::executor::select::select_projection_has_aggregate(&inner_stmt.projection)
        || inner_stmt.projection.iter().any(|item| matches!(item,
            SelectItem::Expr { expr: Expr::Function { name, .. }, alias: Some(alias) }
                if name.eq_ignore_ascii_case(crate::parser::FT_MATCH_FUNCTION) && alias.starts_with("__winagg_")))
}

fn materialize_window_input(
    db: &Arc<Database>,
    inner_response: QueryResponse,
    cte_name: String,
    inner_rows_count: f64,
) -> Result<
    (
        PhysicalPlan,
        HashMap<String, Arc<crate::engine::api::CteResult>>,
    ),
    QueryError,
> {
    let cte_result = std::sync::Arc::new(
        crate::engine::executor::select::materialize_response_with_spill(
            inner_response,
            db.data_dir().to_path_buf(),
        )?,
    );
    let cte_columns = cte_result.columns.clone();
    let mut registry = std::collections::HashMap::new();
    registry.insert(cte_name.clone(), cte_result);
    let scan = PhysicalPlan::CteScan {
        name: cte_name.clone(),
        alias: Some(cte_name),
        columns: cte_columns,
        filter: None,
        estimated_rows: inner_rows_count,
        cost: crate::engine::cbo::Cost::scalar(inner_rows_count),
        projected_columns: None,
    };
    Ok((scan, registry))
}

enum WindowExecutionOutput {
    Response(QueryResponse),
    Rows(Vec<Column>, Vec<Vec<Value>>),
}

fn window_aggregate_plan(
    input: PhysicalPlan,
    windows: &[crate::planner::PhysicalWindowSpec],
    estimated_rows: f64,
) -> PhysicalPlan {
    PhysicalPlan::WindowAgg {
        input: Box::new(input),
        windows: windows.to_vec(),
        estimated_rows,
        cost: crate::engine::cbo::Cost::scalar(estimated_rows),
    }
}

fn materialize_window_source(
    db: &Arc<Database>,
    txn: TxnView,
    statement: &Select,
    plan: &PhysicalPlan,
    rows: f64,
) -> Result<
    (
        PhysicalPlan,
        HashMap<String, Arc<crate::engine::api::CteResult>>,
    ),
    QueryError,
> {
    let response =
        crate::engine::executor::select::plan::execute_physical_select(db, txn, statement, plan)?;
    materialize_window_input(db, response, "__window_input__".to_string(), rows)
}

fn execute_window_plan_result(
    db: &Arc<Database>,
    txn: TxnView,
    inner_plan: &PhysicalPlan,
    inner_stmt: &Select,
    window_specs: &[crate::planner::PhysicalWindowSpec],
    output_template: Vec<OutputCol>,
    original_order_by: Vec<OrderBy>,
    order_key_map: Vec<(Expr, String)>,
    mut original_limit: Option<LimitValue>,
    mut original_offset: Option<LimitValue>,
    final_options: WindowFinalOptions,
) -> Result<QueryResponse, QueryError> {
    let window_count = window_specs.len();
    let inner_rows_count = crate::planner::estimated_rows(inner_plan);
    let wrap_limit = window_limit_wrapper(
        final_options.distinct,
        !original_order_by.is_empty(),
        &mut original_limit,
        &mut original_offset,
        inner_rows_count,
    )?;
    // Materialize the rewritten projection before WindowAgg. Row plans can
    // retain base fields beneath Projection; aggregates and unique dependency
    // aliases become real fields only at the SELECT result boundary. The
    // spill-capable CTE keeps those identities and their catalog metadata.
    let inner_needs_materialization = window_input_needs_materialization(inner_stmt);
    // A derived-table/CTE consumer can spool projected window rows itself.
    // Do not retain and copy the complete window result before it gets there.
    let stream_projection = !final_options.distinct
        && original_order_by.is_empty()
        && original_limit.is_none()
        && original_offset.is_none()
        && (crate::engine::executor::select::force_streaming_response_active()
            || crate::engine::executor::cte::cte_materialization_active());
    let _rollup_topk_guard = if inner_needs_materialization {
        rollup_window_topk_hint_for_inner(inner_plan, inner_stmt, window_specs).map(|hint| {
            crate::engine::executor::select::plan::set_rollup_window_topk_hint(Some(hint))
        })
    } else {
        None
    };
    let (input, registry) = if inner_needs_materialization {
        let (scan, registry) =
            materialize_window_source(db, txn, inner_stmt, inner_plan, inner_rows_count)?;
        (scan, Some(registry))
    } else {
        (inner_plan.clone(), None)
    };
    let wrapped_plan = wrap_limit(window_aggregate_plan(input, window_specs, inner_rows_count));
    let execute = || -> Result<_, QueryError> {
        if stream_projection {
            if let Some(response) = try_stream_window_projection(
                db,
                txn,
                &wrapped_plan,
                window_specs,
                &output_template,
            )? {
                return Ok(WindowExecutionOutput::Response(response));
            }
        }
        crate::engine::executor::select::plan::execute_plan_to_columns_and_values(
            db,
            txn,
            &wrapped_plan,
        )
        .map(|(columns, rows)| WindowExecutionOutput::Rows(columns, rows))
    };
    let output = match registry {
        Some(registry) => crate::engine::executor::cte::with_cte_registry(registry, execute),
        None => execute(),
    }?;
    drop(_rollup_topk_guard);
    let (all_columns, all_rows) = match output {
        WindowExecutionOutput::Rows(columns, rows) => (columns, rows),
        WindowExecutionOutput::Response(response) => return Ok(response),
    };
    finish_window_plan_result(
        all_columns,
        all_rows,
        window_count,
        window_specs,
        output_template,
        original_order_by,
        order_key_map,
        original_limit,
        original_offset,
        final_options,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_window_plan_result_for_analyze(
    db: &Arc<Database>,
    txn: TxnView,
    inner_plan: PhysicalPlan,
    inner_stmt: &Select,
    window_specs: &[crate::planner::PhysicalWindowSpec],
    output_template: Vec<OutputCol>,
    original_order_by: Vec<OrderBy>,
    order_key_map: Vec<(Expr, String)>,
    mut original_limit: Option<LimitValue>,
    mut original_offset: Option<LimitValue>,
    final_options: WindowFinalOptions,
) -> Result<WindowAnalyzeExecution, QueryError> {
    let window_count = window_specs.len();
    let inner_rows_count = crate::planner::estimated_rows(&inner_plan);
    let wrap_limit = window_limit_wrapper(
        final_options.distinct,
        !original_order_by.is_empty(),
        &mut original_limit,
        &mut original_offset,
        inner_rows_count,
    )?;
    let inner_needs_materialization = window_input_needs_materialization(inner_stmt);
    let mut cte_plans = std::collections::HashMap::new();
    let (plan, all_columns, all_rows) = if inner_needs_materialization {
        let cte_name = "__window_input__".to_string();
        cte_plans.insert(cte_name.clone(), inner_plan);
        let inner_plan_ref = cte_plans
            .get(&cte_name)
            .expect("inserted window input plan");
        let _rollup_topk_guard = rollup_window_topk_hint_for_inner(
            inner_plan_ref,
            inner_stmt,
            window_specs,
        )
        .map(|hint| crate::engine::executor::select::plan::set_rollup_window_topk_hint(Some(hint)));
        let (scan, registry) =
            materialize_window_source(db, txn, inner_stmt, inner_plan_ref, inner_rows_count)?;
        let plan = Box::new(wrap_limit(window_aggregate_plan(
            scan,
            window_specs,
            inner_rows_count,
        )));
        let (all_columns, all_rows) =
            crate::engine::executor::cte::with_cte_registry(registry, || {
                crate::engine::executor::select::plan::execute_plan_to_columns_and_values(
                    db, txn, &plan,
                )
            })?;
        (plan, all_columns, all_rows)
    } else {
        let plan = Box::new(wrap_limit(window_aggregate_plan(
            inner_plan,
            window_specs,
            inner_rows_count,
        )));
        let (all_columns, all_rows) =
            crate::engine::executor::select::plan::execute_plan_to_columns_and_values(
                db, txn, &plan,
            )?;
        (plan, all_columns, all_rows)
    };
    let response = finish_window_plan_result(
        all_columns,
        all_rows,
        window_count,
        window_specs,
        output_template,
        original_order_by,
        order_key_map,
        original_limit,
        original_offset,
        final_options,
    )?;
    let rows_count = match response {
        QueryResponse::ResultSet(rs) => rs.rows.len() as u64,
        QueryResponse::StreamingResultSet(mut srs) => {
            let mut count = 0u64;
            while srs.cursor.next_row()?.is_some() {
                count += 1;
            }
            count
        }
        _ => 0,
    };
    Ok(WindowAnalyzeExecution {
        plan,
        cte_plans,
        rows_count,
    })
}

enum WindowFinalCol {
    Index(usize),
    Eval(crate::engine::expr_program::CompiledTableExpr),
}

fn prepare_window_final_projection(
    all_columns: &[Column],
    window_count: usize,
    window_specs: &[crate::planner::PhysicalWindowSpec],
    output_template: &[OutputCol],
) -> (
    Vec<Column>,
    Vec<WindowFinalCol>,
    crate::storage::TableSchema,
) {
    // The inner ResultSet already includes the window output columns at the
    // end, so the post-projection logic below works against `inner` directly.
    // Build final result by mapping output_template
    let mut final_columns: Vec<Column> = Vec::new();
    let mut col_index_map: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for (idx, col) in all_columns.iter().enumerate() {
        col_index_map.insert(col.name.clone(), idx);
    }
    // `saturating_sub`: a zero-row input yields empty `all_columns` (columns
    // are derived from the first row), so guard against underflow — the result
    // is empty anyway, and column types fall back to each spec's output type.
    let win_start = all_columns.len().saturating_sub(window_count);
    // Each output column is produced either by picking an inner/window column
    // by index, or — for a Part 2 post-window projection — by evaluating an
    // expression over the full inner+window row.
    let post_schema = synthetic_set_op_schema(&all_columns);
    let mut final_cols: Vec<WindowFinalCol> = Vec::new();
    for (out_idx, col) in output_template.iter().enumerate() {
        match col {
            OutputCol::Direct { name, source_name } => {
                let idx = *col_index_map.get(source_name).unwrap_or(&out_idx);
                final_cols.push(WindowFinalCol::Index(idx));
                final_columns.push(Column {
                    source: all_columns
                        .get(idx)
                        .map(|c| c.source.clone())
                        .unwrap_or_default(),
                    name: name.clone(),
                    column_type: all_columns
                        .get(idx)
                        .map(|c| c.column_type)
                        .unwrap_or(ColumnType::VarString),
                });
            }
            OutputCol::Window {
                name, spec_index, ..
            } => {
                let idx = win_start + spec_index;
                final_cols.push(WindowFinalCol::Index(idx));
                let mut source = crate::types::ColumnSource::default();
                if window_specs.get(*spec_index).is_some_and(|s| {
                    matches!(
                        s.name.to_ascii_uppercase().as_str(),
                        "BIT_AND" | "BIT_OR" | "BIT_XOR"
                    )
                }) {
                    source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
                }
                final_columns.push(Column {
                    source,
                    name: name.clone(),
                    // Prefer the materialised column type; fall back to the
                    // spec's declared output type when the input was empty.
                    column_type: all_columns
                        .get(idx)
                        .map(|c| c.column_type)
                        .or_else(|| window_specs.get(*spec_index).map(|s| s.output_type))
                        .unwrap_or(ColumnType::VarString),
                });
            }
            OutputCol::Expr {
                name,
                expr,
                column_type,
            } => {
                final_cols.push(WindowFinalCol::Eval(
                    crate::engine::expr_program::CompiledTableExpr::compile_once(
                        expr,
                        &post_schema,
                    ),
                ));
                final_columns.push(Column {
                    source: Default::default(),
                    name: name.clone(),
                    column_type: *column_type,
                });
            }
        }
    }
    // Synthetic single-table view over the inner+window columns so a Part 2
    // projection expression can resolve `__win{N}__` / grouped column names.
    (final_columns, final_cols, post_schema)
}

struct WindowProjectionCursor {
    input: Box<dyn crate::engine::api::ResultCursor>,
    projections: Vec<WindowFinalCol>,
    schema: crate::storage::TableSchema,
    db: Arc<Database>,
    context: crate::engine::executor::select::execution_context::ExecutionContext,
    evaluates_expressions: bool,
    batch_schema: Arc<crate::engine::executor::vec::schema::BatchSchema>,
}

impl std::fmt::Debug for WindowProjectionCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowProjectionCursor")
            .field("columns", &self.projections.len())
            .finish_non_exhaustive()
    }
}

impl crate::engine::api::ResultCursor for WindowProjectionCursor {
    fn next_chunk(&mut self) -> Result<Option<crate::engine::api::ResultChunk>, QueryError> {
        use crate::engine::api::{ResultBatch, ResultChunk};
        use crate::engine::executor::vec::batch::{Batch, ColumnVector};
        if self.evaluates_expressions {
            return self.next_row().map(|row| row.map(ResultChunk::Row));
        }
        let Some(chunk) = self.input.next_chunk()? else {
            return Ok(None);
        };
        Ok(Some(match chunk {
            ResultChunk::Row(row) => ResultChunk::Row(
                self.projections
                    .iter()
                    .map(|projection| {
                        let WindowFinalCol::Index(index) = projection else {
                            unreachable!("index-only projection")
                        };
                        row.get(*index).cloned().unwrap_or(Value::Null)
                    })
                    .collect(),
            ),
            ResultChunk::Columnar(owned) => {
                let rows = owned.batch.num_rows();
                let selection = owned.batch.selection().cloned();
                let mut columns = owned.batch.into_columns()?;
                let mut charge = owned.charge;
                let header_growth = self
                    .projections
                    .len()
                    .saturating_sub(columns.len())
                    .saturating_mul(std::mem::size_of::<ColumnVector>());
                if header_growth > 0 {
                    charge
                        .get_or_insert_with(|| {
                            crate::engine::executor::mem_budget::ScopedCharge::new(0)
                        })
                        .add_force(header_growth)?;
                }
                let mut uses = vec![0usize; columns.len()];
                for projection in &self.projections {
                    let WindowFinalCol::Index(index) = projection else {
                        unreachable!("index-only projection")
                    };
                    if let Some(count) = uses.get_mut(*index) {
                        *count += 1;
                    }
                }
                let output = self
                    .projections
                    .iter()
                    .map(|projection| -> Result<ColumnVector, QueryError> {
                        let WindowFinalCol::Index(index) = projection else {
                            unreachable!("index-only projection")
                        };
                        let Some(column) = columns.get_mut(*index) else {
                            return Ok(ColumnVector::Null { len: rows });
                        };
                        uses[*index] -= 1;
                        if uses[*index] == 0 {
                            Ok(std::mem::replace(column, ColumnVector::Null { len: rows }))
                        } else {
                            // Repeated projections own another vector. Reserve
                            // its bytes before cloning and retain the charge
                            // with the batch through the materialization sink.
                            charge
                                .get_or_insert_with(|| {
                                    crate::engine::executor::mem_budget::ScopedCharge::new(0)
                                })
                                .add_force(column.memory_bytes())?;
                            Ok(column.clone())
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let batch = Batch::from_owned_columns(
                    self.batch_schema.clone(),
                    columns.replace_columns(output),
                );
                ResultChunk::Columnar(ResultBatch {
                    batch: match selection {
                        Some(selection) => batch.with_selection(selection),
                        None => batch,
                    },
                    charge,
                })
            }
        }))
    }

    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        let Some(row) = self.input.next_row()? else {
            return Ok(None);
        };
        let project = || {
            self.projections
                .iter()
                .map(|column| match column {
                    WindowFinalCol::Index(index) => {
                        Ok(row.get(*index).cloned().unwrap_or(Value::Null))
                    }
                    WindowFinalCol::Eval(expr) => expr.eval(&self.schema, &row),
                })
                .collect::<Result<Vec<_>, QueryError>>()
        };
        let projected = if self.evaluates_expressions {
            self.context.run(&self.db, project)?
        } else {
            project()?
        };
        Ok(Some(projected))
    }
    fn take_warnings(&mut self) -> crate::engine::api::SqlWarnings {
        self.input.take_warnings()
    }
}

fn try_stream_window_projection(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
    window_specs: &[crate::planner::PhysicalWindowSpec],
    output_template: &[OutputCol],
) -> Result<Option<QueryResponse>, QueryError> {
    let context =
        crate::engine::executor::select::execution_context::ExecutionContext::capture(txn);
    let Some(stream) = crate::engine::executor::select::vec_bridge::try_stream_native_plan(
        db.clone(),
        txn,
        Arc::new(plan.clone()),
        context.clone(),
    )?
    else {
        return Ok(None);
    };
    let (columns, projections, schema) = prepare_window_final_projection(
        &stream.columns,
        window_specs.len(),
        window_specs,
        output_template,
    );
    let batch_schema = Arc::new(crate::engine::executor::vec::schema::BatchSchema::new(
        columns
            .iter()
            .map(|column| crate::engine::executor::vec::schema::BatchField {
                table: String::new(),
                alias: None,
                name: column.name.clone(),
                column_type: column.column_type,
                source: column.source.clone(),
            })
            .collect(),
    ));
    let evaluates_expressions = projections
        .iter()
        .any(|column| matches!(column, WindowFinalCol::Eval(_)));
    Ok(Some(QueryResponse::StreamingResultSet(
        crate::engine::api::StreamingResultSet {
            columns,
            found_rows: stream.found_rows,
            cursor: Box::new(WindowProjectionCursor {
                input: stream.cursor,
                projections,
                schema,
                db: db.clone(),
                context,
                evaluates_expressions,
                batch_schema,
            }),
        },
    )))
}

#[allow(clippy::too_many_arguments)]
fn finish_window_plan_result(
    all_columns: Vec<Column>,
    all_rows: Vec<Vec<Value>>,
    window_count: usize,
    window_specs: &[crate::planner::PhysicalWindowSpec],
    output_template: Vec<OutputCol>,
    original_order_by: Vec<OrderBy>,
    order_key_map: Vec<(Expr, String)>,
    original_limit: Option<LimitValue>,
    original_offset: Option<LimitValue>,
    final_options: WindowFinalOptions,
) -> Result<QueryResponse, QueryError> {
    let (mut final_columns, final_cols, post_schema) =
        prepare_window_final_projection(&all_columns, window_count, window_specs, &output_template);
    let mut final_rows: Vec<Vec<Value>> =
        crate::engine::executor::mem_budget::try_vec_with_capacity(
            all_rows.len(),
            "window projection rows",
        )?;
    // RAII charge: released when this function returns, so the
    // intermediate materialisation does not linger on the budget once
    // the rows are handed off downstream.
    let mut final_rows_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    for row in all_rows {
        let mut out_row = Vec::with_capacity(final_cols.len());
        for fc in &final_cols {
            let value = match fc {
                WindowFinalCol::Index(i) => row.get(*i).cloned().unwrap_or(Value::Null),
                WindowFinalCol::Eval(expr) => expr.eval(&post_schema, &row)?,
            };
            out_row.push(value);
        }
        // Track but never fail here: these windowed rows are an intermediate
        // that flows straight into a spilling consumer — a CTE body
        // (`CteSpillBuilder`, which spills a large result to disk) or the
        // final result set (force-tracked at the SELECT entry). Raising 1037
        // here pinned a legitimately large window-over-CTE (TPC-DS Q51:
        // web_v1/store_v1 cumulative-sum CTEs) against the budget *before* its
        // rows could reach that spill, double-charging the same data. Mirrors
        // the CTE-scan accounting fix (§5).
        final_rows_charge.add_force(crate::engine::executor::mem_budget::estimate_row_bytes(
            &out_row,
        ))?;
        final_rows.push(out_row);
    }
    // Apply ORDER BY. Keys are evaluated over the final output columns with the
    // full expression engine, after re-pointing any expanded projection
    // definition (e.g. a ROLLUP `GROUPING(a)+GROUPING(b)` that an ORDER BY alias
    // was rewritten into) back at its materialised output column. This sorts
    // correctly for expression keys (`CASE WHEN lochierarchy = 0 ...`) that the
    // old identifier-only resolution silently dropped.
    if !original_order_by.is_empty() {
        let final_schema = synthetic_set_op_schema(&final_columns);
        let output_index: std::collections::HashMap<String, usize> = final_columns
            .iter()
            .enumerate()
            .map(|(idx, col)| (col.name.to_ascii_lowercase(), idx))
            .collect();
        let order_keys: Vec<Expr> = original_order_by
            .iter()
            .map(|item| {
                let mut e = item.expr.clone();
                rebind_expr_to_output_columns(&mut e, &order_key_map);
                e
            })
            .collect();
        let direct_order_positions: Vec<Option<usize>> = order_keys
            .iter()
            .enumerate()
            .map(|(index, expr)| {
                // Qualified expressions retain their projection slot even
                // when two output columns have the same display name.
                if let Some(position) = order_key_map
                    .iter()
                    .position(|(source, _)| source == &original_order_by[index].expr)
                {
                    return Some(position);
                }
                match expr {
                    Expr::Literal(crate::parser::Literal::Int(n)) if *n > 0 => {
                        Some(*n as usize - 1)
                    }
                    Expr::Literal(crate::parser::Literal::UInt(n)) if *n > 0 => {
                        Some(*n as usize - 1)
                    }
                    _ => crate::planner::expr_column_name(expr)
                        .and_then(|name| output_index.get(&name.to_ascii_lowercase()).copied()),
                }
            })
            .collect();
        let mut fields = crate::engine::executor::sort::fields_from_order_by(&original_order_by);
        for (index, (field, expr)) in fields.iter_mut().zip(&order_keys).enumerate() {
            field.collation_id = direct_order_positions[index]
                .and_then(|pos| final_columns.get(pos))
                .and_then(|c| c.source.collation_id)
                .or_else(|| {
                    crate::engine::expr::expr_collation(expr, &final_schema)
                        .map(|info| info.collation_id)
                });
        }
        crate::engine::executor::sort::sort_by_cached_keys(&mut final_rows, &fields, |row| {
            order_keys
                .iter()
                .enumerate()
                .map(|(idx, e)| {
                    direct_order_positions[idx]
                        .and_then(|pos| row.get(pos).cloned())
                        .unwrap_or_else(|| {
                            crate::engine::expr::eval_expr(e, &final_schema, row)
                                .unwrap_or(Value::Null)
                        })
                })
                .collect()
        });
    }
    let visible = final_columns.len().saturating_sub(final_options.hidden);
    final_columns.truncate(visible);
    for row in &mut final_rows {
        row.truncate(visible);
    }
    if final_options.distinct {
        let mut seen = std::collections::HashSet::new();
        final_rows.retain(|row| {
            seen.insert(
                row.iter()
                    .zip(&final_columns)
                    .map(|(v, c)| {
                        crate::engine::expr::ValueKey::from_with_collation(v, c.source.collation_id)
                    })
                    .collect::<Vec<_>>(),
            )
        });
    }
    // Resolve LIMIT/OFFSET through the shared helpers so an auto-parameterized
    // `LimitValue::Placeholder` (auto-param folds literal LIMIT/OFFSET) binds
    // against the thread-local params instead of being silently dropped.
    let offset = crate::engine::executor::select::limit_value_or_zero(original_offset.as_ref())?;
    let limit = crate::engine::executor::select::optional_limit_value(original_limit.as_ref())?;
    if offset >= final_rows.len() {
        final_rows.clear();
    } else {
        final_rows.drain(..offset);
    }
    if let Some(limit) = limit {
        final_rows.truncate(limit);
    }
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: final_columns,
        rows: final_rows,
    }))
}

#[derive(Clone, Copy)]
struct WindowFinalOptions {
    distinct: bool,
    hidden: usize,
}

enum OutputCol {
    Direct {
        name: String,
        source_name: String,
    },
    Window {
        name: String,
        spec_index: usize,
    },
    /// Part 2: a post-window projection expression evaluated over the window
    /// output columns (window functions already replaced by `__win{N}__`
    /// references and aggregates by their grouped columns).
    Expr {
        name: String,
        expr: Expr,
        column_type: ColumnType,
    },
}

/// Compare numeric RANGE boundaries in their exact domain. Integer and
/// DECIMAL keys must not pass through f64 (adjacent wide decimals collapse).
fn exact_range_bound(
    values: &[Value],
    current: usize,
    offset: &Value,
    preceding: bool,
    asc: bool,
    start: bool,
) -> Result<i64, QueryError> {
    let exact = |v: &Value| match v {
        Value::Float(_) | Value::Double(_) => v.clone(),
        _ => Value::Decimal(crate::storage::value_to_string(v)),
    };
    let op = if preceding == asc {
        crate::parser::BinaryOp::Sub
    } else {
        crate::parser::BinaryOp::Add
    };
    let target =
        crate::engine::expr::eval_arithmetic(&exact(&values[current]), op, &exact(offset))?;
    // NULLs are a contiguous prefix or suffix; exclude them from a numeric frame.
    let lo = values.partition_point(|v| matches!(v, Value::Null));
    let hi = if lo > 0 {
        values.len()
    } else {
        values.partition_point(|v| !matches!(v, Value::Null))
    };
    let index = values[lo..hi].partition_point(|v| {
        let ord = compare_values(v, &target).unwrap_or(std::cmp::Ordering::Equal);
        if asc {
            if start {
                ord.is_lt()
            } else {
                !ord.is_gt()
            }
        } else if start {
            ord.is_gt()
        } else {
            !ord.is_lt()
        }
    }) + lo;
    Ok(if start {
        index as i64
    } else {
        index as i64 - 1
    })
}

/// Schedule independent partition sorts with inherited cancellation and deadlines.
fn sort_window_partitions(
    partitions: &mut [Vec<usize>],
    row_count: usize,
    sort_one: impl Fn(&mut Vec<usize>) -> Result<(), QueryError> + Sync,
) -> Result<(), QueryError> {
    let degree = window_parallel_degree(row_count, partitions.len());
    if degree <= 1 {
        for partition in partitions.iter_mut() {
            sort_one(partition)?;
        }
    } else {
        let chunk = partitions.len().div_ceil(degree).max(1);
        let sort_one = &sort_one;
        let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
        std::thread::scope(|scope| -> Result<(), QueryError> {
            let mut workers = Vec::new();
            for part_chunk in partitions.chunks_mut(chunk) {
                let cancellation = cancellation.clone();
                workers.push(scope.spawn(move || -> Result<(), QueryError> {
                    let _scope = cancellation.enter();
                    for partition in part_chunk.iter_mut() {
                        sort_one(partition)?;
                    }
                    Ok(())
                }));
            }
            for worker in workers {
                worker.join().map_err(|_| {
                    error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "window sort worker panicked",
                    )
                })??;
            }
            Ok(())
        })?;
    }
    Ok(())
}

/// Resolve a RANGE-frame start bound with a numeric offset to a partition
/// index. `is_preceding` indicates whether the offset is `<N> PRECEDING`
/// (true) or `<N> FOLLOWING` (false). For ascending ORDER BY:
/// - PRECEDING N: first j where value[j] >= value[i] - N
/// - FOLLOWING N: first j where value[j] >= value[i] + N
/// For descending ORDER BY the comparisons flip.
fn range_bound_start(
    order_values: &[f64],
    i: usize,
    offset: f64,
    is_preceding: bool,
    asc: bool,
) -> i64 {
    range_bound::<true>(order_values, i, offset, is_preceding, asc)
}

/// Last included row; the common search preserves inclusive end semantics.
fn range_bound_end(
    order_values: &[f64],
    i: usize,
    offset: f64,
    is_preceding: bool,
    asc: bool,
) -> i64 {
    range_bound::<false>(order_values, i, offset, is_preceding, asc)
}

#[inline]
fn range_bound<const START: bool>(
    order_values: &[f64],
    i: usize,
    offset: f64,
    is_preceding: bool,
    asc: bool,
) -> i64 {
    let n = order_values.len();
    let cur = order_values[i];
    if cur.is_nan() {
        return i as i64;
    }
    let target = if asc {
        if is_preceding {
            cur - offset
        } else {
            cur + offset
        }
    } else if is_preceding {
        cur + offset
    } else {
        cur - offset
    };
    let lo = order_values.partition_point(|v| v.is_nan());
    let hi = if lo > 0 {
        n
    } else {
        order_values.partition_point(|v| !v.is_nan())
    };
    let bound = lo
        + order_values[lo..hi].partition_point(|v| match (asc, START) {
            (true, true) => *v < target,
            (false, true) => *v > target,
            (true, false) => *v <= target,
            (false, false) => *v >= target,
        });
    bound as i64 - i64::from(!START)
}

/// Ranking window function kind eligible for Top-N pushdown. Determined from
/// the window function name; only these three have a monotone rank along the
/// sorted partition so an `rk <= k` filter can be answered by a bounded prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RankKind {
    RowNumber,
    Rank,
    DenseRank,
}

impl RankKind {
    pub(crate) fn from_name(name: &str) -> Option<RankKind> {
        match name.to_ascii_uppercase().as_str() {
            "ROW_NUMBER" => Some(RankKind::RowNumber),
            "RANK" => Some(RankKind::Rank),
            "DENSE_RANK" => Some(RankKind::DenseRank),
            _ => None,
        }
    }
}

thread_local! {
    /// Top-N pushdown hint set by the enclosing derived-table executor right
    /// before it materialises a window-bearing subquery (e.g. q67's
    /// `... FROM (SELECT rank() OVER(...) rk ...) dw WHERE rk <= k`). Carries
    /// `(window_output_column, k)`. Window preparation borrows it for the
    /// matching alias and masks it while preparing nested query blocks.
    static WINDOW_TOPK_HINT: std::cell::RefCell<Option<(String, usize)>> =
        const { std::cell::RefCell::new(None) };
}

/// RAII guard scoping a [`WINDOW_TOPK_HINT`] to one derived-subquery
/// materialisation; restores the enclosing scope even on error or unwind.
pub(in crate::engine::executor) struct WindowTopKHintGuard(Option<(String, usize)>);

pub(in crate::engine::executor) fn set_window_topk_hint(
    hint: Option<(String, usize)>,
) -> WindowTopKHintGuard {
    WindowTopKHintGuard(WINDOW_TOPK_HINT.with(|h| h.replace(hint)))
}

impl Drop for WindowTopKHintGuard {
    fn drop(&mut self) {
        WINDOW_TOPK_HINT.with(|h| *h.borrow_mut() = self.0.take());
    }
}

fn peek_window_topk_hint() -> Option<(String, usize)> {
    WINDOW_TOPK_HINT.with(|h| h.borrow().clone())
}

/// Length of the prefix of a sorted partition whose ranking value is `<= k`.
/// `order_keys`/`sort_fields` describe the ORDER BY used to detect peer ties.
/// For `ROW_NUMBER` this is `min(len, k)`; for `RANK`/`DENSE_RANK` it extends
/// past `k` rows when the row at the boundary ties with later peers (e.g. three
/// rows tied at rank 1 are all kept for `rk <= 2`).
fn rank_bounded_prefix_len(
    partition: &[usize],
    order_keys: &[Vec<Value>],
    sort_fields: &[crate::engine::executor::sort::SortField],
    k: usize,
    kind: RankKind,
) -> usize {
    if k == 0 {
        return 0;
    }
    if matches!(kind, RankKind::RowNumber) {
        return partition.len().min(k);
    }
    let peers_equal = |a: usize, b: usize| -> bool {
        sort_fields.iter().enumerate().all(|(c, f)| {
            crate::engine::executor::sort::compare_value_for_sort(
                &order_keys[a][c],
                &order_keys[b][c],
                *f,
            ) == std::cmp::Ordering::Equal
        })
    };
    let mut rank: usize = 1;
    let mut keep = 0;
    for i in 0..partition.len() {
        if i > 0 && !peers_equal(partition[i - 1], partition[i]) {
            rank = match kind {
                RankKind::Rank => i + 1,
                RankKind::DenseRank => rank + 1,
                RankKind::RowNumber => unreachable!(),
            };
        }
        if rank > k {
            break;
        }
        keep = i + 1;
    }
    keep
}

fn window_bounded_topk_enabled() -> bool {
    crate::util::env::var("MYDB_WINDOW_BOUNDED_TOPK")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
        .unwrap_or(false)
}

fn compare_window_order_key(
    a: usize,
    b: usize,
    order_keys: &[Vec<Value>],
    sort_fields: &[crate::engine::executor::sort::SortField],
) -> std::cmp::Ordering {
    for (i, field) in sort_fields.iter().enumerate() {
        let ord = crate::engine::executor::sort::compare_value_for_sort(
            &order_keys[a][i],
            &order_keys[b][i],
            *field,
        );
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_window_order_row(
    a: usize,
    b: usize,
    rows: &[Vec<Value>],
    positions: &[usize],
    sort_fields: &[crate::engine::executor::sort::SortField],
) -> std::cmp::Ordering {
    for (&position, field) in positions.iter().zip(sort_fields) {
        let left = rows[a].get(position).unwrap_or(&Value::Null);
        let right = rows[b].get(position).unwrap_or(&Value::Null);
        let ordering = crate::engine::executor::sort::compare_value_for_sort(left, right, *field);
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

fn trim_partition_to_rank_candidate_set(
    partition: &mut Vec<usize>,
    order_keys: &[Vec<Value>],
    sort_fields: &[crate::engine::executor::sort::SortField],
    k: usize,
    kind: RankKind,
) -> Result<(), QueryError> {
    if k == 0 {
        partition.clear();
        return Ok(());
    }
    if partition.len() <= k {
        return Ok(());
    }
    match kind {
        RankKind::RowNumber | RankKind::Rank => {
            let boundary_pos = k - 1;
            partition.select_nth_unstable_by(boundary_pos, |&a, &b| {
                compare_window_order_key(a, b, order_keys, sort_fields)
            });
            if matches!(kind, RankKind::RowNumber) {
                partition.truncate(k);
                return Ok(());
            }
            let boundary = partition[boundary_pos];
            partition.retain(|&idx| {
                compare_window_order_key(idx, boundary, order_keys, sort_fields)
                    != std::cmp::Ordering::Greater
            });
        }
        RankKind::DenseRank => {
            let mut reps: Vec<usize> = Vec::with_capacity(k);
            'rows: for (position, &idx) in partition.iter().enumerate() {
                if position % 256 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                for &rep in &reps {
                    if compare_window_order_key(idx, rep, order_keys, sort_fields)
                        == std::cmp::Ordering::Equal
                    {
                        continue 'rows;
                    }
                }
                if reps.len() < k {
                    reps.push(idx);
                    continue;
                }
                let Some((worst_pos, &worst)) =
                    reps.iter().enumerate().max_by(|(_, &a), (_, &b)| {
                        compare_window_order_key(a, b, order_keys, sort_fields)
                    })
                else {
                    continue;
                };
                if compare_window_order_key(idx, worst, order_keys, sort_fields)
                    == std::cmp::Ordering::Less
                {
                    reps[worst_pos] = idx;
                }
            }
            if reps.len() < k {
                return Ok(());
            }
            let boundary = *reps
                .iter()
                .max_by(|&&a, &&b| compare_window_order_key(a, b, order_keys, sort_fields))
                .expect("non-empty dense-rank reps");
            partition.retain(|&idx| {
                compare_window_order_key(idx, boundary, order_keys, sort_fields)
                    != std::cmp::Ordering::Greater
            });
        }
    }
    crate::engine::cancel::check_cancelled()
}

/// For each row position within `partition`, return the inclusive `[start, end]`
/// frame indices (in partition coordinates) according to the frame definition.
/// The partition is assumed to already be sorted by `order_by`.
/// Compute partition+sort once for a window group sharing the same
/// `(partition_by, order_by)`. Returns a list of partitions where each
/// partition is a list of row indices sorted by `order_by`.
///
/// When `top_k` is `Some((k, kind))`, each partition is truncated to its
/// rank-bounded prefix (`rk <= k`) — the Top-N pushdown for an enclosing
/// `WHERE rk <= k` filter. Rows beyond the prefix are dropped from the
/// returned partitions, so the caller must emit only the surviving indices.
/// O3 (q51): worker count for the window operator's per-partition fan-out
/// (sort in `compute_window_partitions`, eval in `try_vec_window_fast_path`).
/// Returns 1 (serial) below `MYDB_WINDOW_PARALLEL_MIN_ROWS` rows or with too
/// few partitions to split. `MYDB_WINDOW_PARALLEL=off` forces serial.
pub(crate) fn window_parallel_degree(total_rows: usize, num_partitions: usize) -> usize {
    if crate::util::env::var("MYDB_WINDOW_PARALLEL")
        .map(|v| v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off"))
        .unwrap_or(false)
    {
        return 1;
    }
    let min_rows = crate::util::env::var("MYDB_WINDOW_PARALLEL_MIN_ROWS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(50_000);
    // Need enough total work and at least 2 partitions per worker to make the
    // fork-join worthwhile (a single huge partition can't be split this way).
    if total_rows < min_rows || num_partitions < 4 {
        return 1;
    }
    let degree = crate::planner::default_parallel_degree() as usize;
    degree.clamp(1, 8).min(num_partitions / 2)
}

fn window_sort_fields(
    columns: &[Column],
    order_by: &[OrderBy],
) -> Vec<crate::engine::executor::sort::SortField> {
    let schema = synthetic_set_op_schema(columns);
    order_by
        .iter()
        .map(|o| {
            let mut field = crate::engine::executor::sort::SortField::from_order_by(o);
            field.collation_id =
                crate::engine::expr::expr_collation(&o.expr, &schema).map(|c| c.collation_id);
            field
        })
        .collect()
}

/// Sort bounded runs, then merge with cancellation checks. A comparator
/// cannot return QueryError; checking only between partitions leaves a single
/// large partition uninterruptible. Ordinal tie-breaks preserve stable order.
pub(crate) fn cancellable_window_sort<T: Copy>(
    indices: &mut [T],
    compare: impl Fn(T, T) -> std::cmp::Ordering,
) -> Result<(), QueryError> {
    crate::engine::cancel::check_cancelled()?;
    let mut sorted = true;
    for (i, pair) in indices.windows(2).enumerate() {
        if i % 256 == 0 {
            crate::engine::cancel::check_cancelled()?;
        }
        if compare(pair[0], pair[1]).is_gt() {
            sorted = false;
            break;
        }
    }
    if sorted {
        return Ok(());
    }
    const RUN: usize = 1024;
    for chunk in indices.chunks_mut(RUN) {
        crate::engine::cancel::check_cancelled()?;
        chunk.sort_unstable_by(|a, b| compare(*a, *b));
    }
    if indices.len() <= RUN {
        return Ok(());
    }
    let _charge =
        crate::engine::executor::mem_budget::force_charge_scoped(std::mem::size_of_val(indices))?;
    let mut scratch = indices.to_vec();
    let mut width = RUN;
    while width < indices.len() {
        for start in (0..indices.len()).step_by(width.saturating_mul(2)) {
            let mid = start.saturating_add(width).min(indices.len());
            let end = mid.saturating_add(width).min(indices.len());
            let (mut left, mut right) = (start, mid);
            for out in start..end {
                if out % 256 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                if left < mid && (right == end || !compare(indices[left], indices[right]).is_gt()) {
                    scratch[out] = indices[left];
                    left += 1;
                } else {
                    scratch[out] = indices[right];
                    right += 1;
                }
            }
        }
        indices.copy_from_slice(&scratch);
        width = width.saturating_mul(2);
    }
    crate::engine::cancel::check_cancelled()
}

/// Reuse the scalar aggregate semantics while reading an already ordered
/// columnar partition. ROWS prefix frames need no owning row or frame index.
pub(crate) fn evaluate_window_prefix_values(
    name: &str,
    count_star: bool,
    values: impl IntoIterator<Item = Value>,
) -> Result<Vec<Value>, QueryError> {
    with_window_aggregate!(name, count_star, State, {
        aggregate::prefix_values::<State>(values)
    })
}

pub(crate) fn compute_window_partitions(
    columns: &[Column],
    rows: &[Vec<Value>],
    partition_by: &[Expr],
    order_by: &[OrderBy],
    top_k: Option<(usize, RankKind)>,
) -> Result<Vec<Vec<usize>>, QueryError> {
    let schema = synthetic_set_op_schema(columns);
    let partition_collations: Vec<_> = partition_by
        .iter()
        .map(|expr| crate::engine::expr::expr_collation(expr, &schema).map(|c| c.collation_id))
        .collect();
    let partition_positions: Vec<Option<usize>> = partition_by
        .iter()
        .map(|expr| expr_column_position(expr, columns))
        .collect();
    let order_positions: Vec<Option<usize>> = order_by
        .iter()
        .map(|o| expr_column_position(&o.expr, columns))
        .collect();
    // Row indices are the only O(N) side structure for direct column keys.
    // Keys are now built one row at a time and moved straight into the
    // partition map; ORDER BY reads the already-materialised row columns.
    // Complex expressions still need owning values and retain the old
    // conservative charge.
    let per_row_bytes = 8usize
        .saturating_add(
            partition_positions
                .iter()
                .filter(|position| position.is_none())
                .count()
                .saturating_mul(32),
        )
        .saturating_add(
            order_positions
                .iter()
                .filter(|position| position.is_none())
                .count()
                .saturating_mul(32),
        );
    let estimated_bytes = rows.len().saturating_mul(per_row_bytes);
    // The caller selects whole-input or partition-spool execution. Once a
    // partition is here its index scratch is mandatory to finish that pass.
    let _release_on_exit =
        crate::engine::executor::mem_budget::force_charge_scoped(estimated_bytes)?;
    // Group rows by partition key in O(N): a HashMap keyed by the partition
    // key replaces the previous linear `partition_keys_unique.position(...)`
    // scan, which was O(N·P) and went quadratic on high-cardinality
    // PARTITION BY (e.g. q67's `partition by i_category` over a rollup result).
    // Partitions are still appended in first-appearance order; downstream
    // scatters results back by original row index, so the order is immaterial.
    use hashbrown::hash_map::Entry;
    let mut partition_index: hashbrown::HashMap<Vec<ValueKey>, usize, ahash::RandomState> =
        hashbrown::HashMap::with_hasher(ahash::RandomState::new());
    let mut partitions: Vec<Vec<usize>> = Vec::new();
    for (idx, row) in rows.iter().enumerate() {
        if idx % 256 == 0 {
            crate::engine::cancel::check_cancelled()?;
        }
        let key = partition_by
            .iter()
            .zip(partition_positions.iter())
            .zip(&partition_collations)
            .map(|((expr, pos), collation)| -> Result<ValueKey, QueryError> {
                Ok(if let Some(pos) = pos {
                    row.get(*pos)
                        .map(|v| ValueKey::from_with_collation(v, *collation))
                        .unwrap_or(ValueKey::Null)
                } else {
                    ValueKey::from_with_collation(
                        &eval_expr_against_columns(expr, columns, row)?,
                        *collation,
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        match partition_index.entry(key) {
            Entry::Occupied(e) => partitions[*e.get()].push(idx),
            Entry::Vacant(e) => {
                let pos = partitions.len();
                partitions.push(vec![idx]);
                e.insert(pos);
            }
        }
    }
    if !order_by.is_empty() {
        let sort_fields = window_sort_fields(columns, order_by);
        if top_k.is_none() && order_positions.iter().all(Option::is_some) {
            let direct_positions: Vec<usize> = order_positions
                .iter()
                .map(|position| position.expect("checked direct ORDER BY positions"))
                .collect();
            let sort_one = |partition: &mut Vec<usize>| -> Result<(), QueryError> {
                cancellable_window_sort(partition, |a, b| {
                    compare_window_order_row(a, b, rows, &direct_positions, &sort_fields)
                        .then_with(|| a.cmp(&b))
                })
            };
            sort_window_partitions(&mut partitions, rows.len(), sort_one)?;
            return Ok(partitions);
        }
        // Schwartzian transform: evaluate each row's ORDER BY key values once
        // up front so the comparator does value comparisons instead of
        // re-evaluating the ORDER BY expressions O(N·log N) times per
        // partition.
        let order_keys: Vec<Vec<Value>> = rows
            .iter()
            .map(|row| {
                crate::engine::cancel::check_cancelled()?;
                order_by
                    .iter()
                    .zip(order_positions.iter())
                    .map(|(o, pos)| -> Result<Value, QueryError> {
                        Ok(if let Some(pos) = pos {
                            row.get(*pos).cloned().unwrap_or(Value::Null)
                        } else {
                            eval_expr_against_columns(&o.expr, columns, row)?
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Precompute SortField once per ORDER BY key. `SortField::from_order_by`
        // resolves NULL placement via `order_by_nulls_default_high()`, which
        // reads `MYDB_ORDER_BY_NULLS` through the libc env lock; calling it
        // inside the comparator made every comparison take that global lock
        // (O(N·log N) per partition). Hoist it out — same fix already applied
        // to the row/vector sort paths.
        // O3 (q51): the per-partition sorts are independent (each owns a
        // disjoint set of row indices and reads only the shared, immutable
        // `order_keys` / `sort_fields`). On a high-partition-count window
        // (q51 partitions by `item_sk` over ~495k rows) this sort dominates
        // `compute_window_partitions`. Fan the partition list across workers;
        // the comparator is pure (`compare_value_for_sort` + hoisted
        // `sort_fields`, no env / session reads — see the SortField hoist
        // note above). Workers inherit cancellation and the absolute deadline.
        let sort_one = |partition: &mut Vec<usize>| -> Result<(), QueryError> {
            crate::engine::cancel::check_cancelled()?;
            if let Some((k, kind)) = top_k {
                if window_bounded_topk_enabled() {
                    trim_partition_to_rank_candidate_set(
                        partition,
                        &order_keys,
                        &sort_fields,
                        k,
                        kind,
                    )?;
                }
            }
            cancellable_window_sort(partition, |a, b| {
                compare_window_order_key(a, b, &order_keys, &sort_fields).then_with(|| a.cmp(&b))
            })
        };
        sort_window_partitions(&mut partitions, rows.len(), sort_one)?;
        // Top-N pushdown: truncate each sorted partition to the rank-bounded
        // prefix. Safe because the rank value of a kept row depends only on
        // earlier rows in the sort order, all of which are in the prefix — so
        // the downstream rank/dense_rank/row_number values are identical to the
        // unbounded computation.
        if let Some((k, kind)) = top_k {
            for partition in partitions.iter_mut() {
                let keep = rank_bounded_prefix_len(partition, &order_keys, &sort_fields, k, kind);
                partition.truncate(keep);
            }
        }
    }
    Ok(partitions)
}

/// Evaluate a single window function given pre-computed partitions
/// (sorted). Shared partition layout lets multiple windows in the same
/// `(partition_by, order_by)` group avoid recomputing partition+sort.
pub(crate) fn evaluate_window_with_partitions(
    expr: &Expr,
    partitions: &[Vec<usize>],
    columns: &[Column],
    rows: &[Vec<Value>],
) -> Result<Vec<Value>, QueryError> {
    let Expr::WindowFunction { spec, .. } = expr else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "expected window function expression",
        ));
    };
    let crate::parser::WindowSpec {
        order_by, frame, ..
    } = spec.as_ref();

    // Determine effective frame (applied per-spec, MySQL default rules).
    let effective_frame: WindowFrame = match frame {
        Some(f) => f.clone(),
        None => {
            if order_by.is_empty() {
                WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start: WindowFrameBound::UnboundedPreceding,
                    end: WindowFrameBound::UnboundedFollowing,
                    exclusion: crate::parser::WindowFrameExclusion::NoOthers,
                }
            } else {
                WindowFrame {
                    units: WindowFrameUnits::Range,
                    start: WindowFrameBound::UnboundedPreceding,
                    end: WindowFrameBound::CurrentRow,
                    exclusion: crate::parser::WindowFrameExclusion::NoOthers,
                }
            }
        }
    };
    evaluate_window_per_partition(expr, &effective_frame, partitions, columns, rows)
}

#[derive(Clone, Copy)]
struct WindowFrameRange {
    start: usize,
    end: usize,
    current: usize,
    peer_first: usize,
    peer_last: usize,
    exclusion: WindowFrameExclusion,
}

impl WindowFrameRange {
    /// Cardinality after exclusion, without scanning the frame for COUNT(*).
    fn included_len(&self) -> usize {
        if self.end < self.start {
            return 0;
        }
        let total = self.end - self.start + 1;
        let current_in_frame = (self.start..=self.end).contains(&self.current);
        match self.exclusion {
            WindowFrameExclusion::NoOthers => total,
            WindowFrameExclusion::CurrentRow => total - usize::from(current_in_frame),
            WindowFrameExclusion::Group | WindowFrameExclusion::Ties => {
                let first = self.start.max(self.peer_first);
                let last = self.end.min(self.peer_last);
                let peers = if first <= last { last - first + 1 } else { 0 };
                let keep_current = self.exclusion == WindowFrameExclusion::Ties
                    && current_in_frame
                    && (self.peer_first..=self.peer_last).contains(&self.current);
                total - peers + usize::from(keep_current)
            }
        }
    }
}

fn frame_position_excluded(range: &WindowFrameRange, pos: usize) -> bool {
    match range.exclusion {
        WindowFrameExclusion::NoOthers => false,
        WindowFrameExclusion::CurrentRow => pos == range.current,
        WindowFrameExclusion::Group => pos >= range.peer_first && pos <= range.peer_last,
        WindowFrameExclusion::Ties => {
            pos != range.current && pos >= range.peer_first && pos <= range.peer_last
        }
    }
}

/// Decode a temporal frame offset once per partition. Calendar arithmetic
/// uses the same interval kernel as DATE_ADD, including composite units.
fn window_interval(bound: &WindowFrameBound) -> Result<Option<(Value, String)>, QueryError> {
    let (WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr)) = bound else {
        return Ok(None);
    };
    let Expr::Function { name, args, .. } = expr.as_ref() else {
        return Ok(None);
    };
    if !name.eq_ignore_ascii_case("__INTERVAL") {
        return Ok(None);
    }
    if args.len() != 2 {
        return Err(error(
            crate::error::catalog::ER_WINDOW_FRAME_ILLEGAL,
            "malformed window INTERVAL",
        ));
    }
    let amount = crate::engine::expr::eval_const_runtime(&args[0])?;
    let unit = crate::storage::value_to_string(&crate::engine::expr::eval_const_runtime(&args[1])?)
        .to_ascii_uppercase();
    if matches!(amount, Value::Null)
        || crate::storage::value_to_string(&amount)
            .trim_start()
            .starts_with('-')
    {
        return Err(error(
            crate::error::catalog::ER_WINDOW_FRAME_ILLEGAL,
            "window INTERVAL must be non-negative",
        ));
    }
    // Validate the unit and value even if all ORDER BY values are NULL.
    crate::engine::expr::add_interval_to_micros(0, &amount, &unit, false)?;
    Ok(Some((amount, unit)))
}

fn window_interval_offset(
    current: Option<i64>,
    interval: &Option<(Value, String)>,
    preceding: bool,
    asc: bool,
) -> Result<Option<Value>, QueryError> {
    let (Some(current), Some((amount, unit))) = (current, interval) else {
        return Ok(None);
    };
    let subtract = preceding == asc;
    let target = crate::engine::expr::add_interval_to_micros(current, amount, unit, subtract)?;
    let distance = if subtract {
        current.checked_sub(target)
    } else {
        target.checked_sub(current)
    };
    match distance {
        Some(distance) if distance >= 0 => Ok(Some(Value::Int(distance))),
        _ => Err(error(
            crate::error::catalog::ER_WINDOW_FRAME_ILLEGAL,
            "window INTERVAL boundary is out of range",
        )),
    }
}

/// Temporal RANGE keys retain every microsecond; NULL has its own lane.
fn temporal_value_to_micros(v: &Value) -> Result<Option<i64>, QueryError> {
    if matches!(v, Value::Null) {
        return Ok(None);
    }
    crate::engine::expr::parse_datetime(v)
        .map(|dt| Some(dt.and_utc().timestamp_micros()))
        .map_err(|_| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "RANGE frame with INTERVAL offset requires a temporal ORDER BY column",
            )
        })
}

fn compute_frame_ranges(
    partition: &[usize],
    frame: &WindowFrame,
    order_by: &[OrderBy],
    columns: &[Column],
    rows: &[Vec<Value>],
) -> Result<Vec<WindowFrameRange>, QueryError> {
    let n = partition.len();
    if n == 0 {
        return Ok(Vec::new());
    }
    // Pre-compute peer-group boundaries (inclusive [first, last]) for each row
    // in the partition. GROUPS frames use the same peer groups as RANGE
    // CURRENT ROW. With no ORDER BY, the entire partition is one peer group.
    let mut peer_first: Vec<usize> = vec![0; n];
    let mut peer_last: Vec<usize> = vec![n - 1; n];
    let mut group_firsts: Vec<usize> = Vec::new();
    let mut group_lasts: Vec<usize> = Vec::new();
    let mut group_for_pos: Vec<usize> = vec![0; n];
    if order_by.is_empty() {
        group_firsts.push(0);
        group_lasts.push(n - 1);
    } else {
        let key_of = |idx_in_part: usize| -> Vec<Value> {
            order_by
                .iter()
                .map(|o| {
                    eval_expr_against_columns(&o.expr, columns, &rows[partition[idx_in_part]])
                        .unwrap_or(Value::Null)
                })
                .collect()
        };
        let fields = window_sort_fields(columns, order_by);
        let mut i = 0;
        while i < n {
            crate::engine::cancel::check_cancelled()?;
            let key = key_of(i);
            let mut j = i + 1;
            while j < n
                && key_of(j).iter().zip(&key).zip(&fields).all(|((a, b), f)| {
                    crate::engine::executor::sort::compare_value_for_sort(a, b, *f).is_eq()
                })
            {
                if j % 256 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                j += 1;
            }
            let group_idx = group_firsts.len();
            group_firsts.push(i);
            group_lasts.push(j - 1);
            for k in i..j {
                peer_first[k] = i;
                peer_last[k] = j - 1;
                group_for_pos[k] = group_idx;
            }
            i = j;
        }
    }
    let resolve_offset = |bound: &WindowFrameBound| -> Result<f64, QueryError> {
        match bound {
            WindowFrameBound::Preceding(e) | WindowFrameBound::Following(e) => {
                // P2-C6c: `INTERVAL n UNIT` offsets resolve to microseconds.
                if window_interval(bound)?.is_some() {
                    return Ok(0.0);
                }
                let value = crate::engine::expr::eval_const_runtime(e)?;
                if !matches!(frame.units, WindowFrameUnits::Range) {
                    let integer = window_const_i64(e)?;
                    if integer < 0 {
                        return Err(error(
                            crate::error::catalog::ER_WINDOW_FRAME_ILLEGAL,
                            "window frame offset must be non-negative",
                        ));
                    }
                    return Ok(integer as f64);
                }
                let offset = numeric(&value)?;
                if !offset.is_finite()
                    || offset < 0.0
                    || (!matches!(frame.units, WindowFrameUnits::Range) && offset.fract() != 0.0)
                {
                    return Err(error(crate::error::catalog::ER_WINDOW_FRAME_ILLEGAL, "window frame offset must be non-negative and ROWS/GROUPS offsets must be integers"));
                }
                Ok(offset)
            }
            _ => Ok(0.0),
        }
    };
    let start_off = resolve_offset(&frame.start)?;
    let end_off = resolve_offset(&frame.end)?;
    // P2-C6c: an INTERVAL offset means the single ORDER BY column is temporal
    // and its values must be projected to the same microsecond scalar.
    let start_interval = window_interval(&frame.start)?;
    let end_interval = window_interval(&frame.end)?;
    let interval_frame = start_interval.is_some() || end_interval.is_some();
    if interval_frame && !matches!(frame.units, WindowFrameUnits::Range) {
        return Err(error(
            crate::error::catalog::ER_WINDOW_ROWS_INTERVAL_USE,
            "INTERVAL requires a RANGE frame",
        ));
    }
    // For RANGE with numeric offsets we need a single ORDER BY column whose
    // value is numeric. Pre-compute it for all partition rows so we can
    // binary-search later.
    let mut exact_order_values = Vec::new();
    let mut temporal_order_values = Vec::new();
    let order_values: Vec<f64> = if matches!(frame.units, WindowFrameUnits::Range)
        && (matches!(
            frame.start,
            WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
        ) || matches!(
            frame.end,
            WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
        )) {
        if order_by.len() != 1 {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "RANGE frame with numeric offset requires exactly one ORDER BY column",
            ));
        }
        let mut values = Vec::with_capacity(n);
        for &row_idx in partition {
            crate::engine::cancel::check_cancelled()?;
            let v = eval_expr_against_columns(&order_by[0].expr, columns, &rows[row_idx])?;
            exact_order_values.push(v.clone());
            let f = if interval_frame {
                // Temporal RANGE frame: project the ORDER BY value to
                // microseconds-since-epoch, the same scalar as the offset.
                let micros = temporal_value_to_micros(&v)?;
                *exact_order_values.last_mut().expect("key pushed") =
                    micros.map(Value::Int).unwrap_or(Value::Null);
                temporal_order_values.push(micros);
                // Temporal boundaries exclusively use the exact lane below.
                0.0
            } else {
                match v {
                    Value::Int(i) => i as f64,
                    Value::UInt(u) => u as f64,
                    Value::Float(f) => f as f64,
                    Value::Double(f) => f,
                    Value::Decimal(s) => s.parse::<f64>().map_err(|_| {
                        error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            "RANGE frame numeric offset requires numeric ORDER BY values",
                        )
                    })?,
                    Value::DecimalCompact(..) | Value::Decimal128(..) | Value::DecimalWide(..) => {
                        numeric(&v)?
                    }
                    Value::Null => f64::NAN,
                    _ => {
                        return Err(error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            "RANGE frame numeric offset requires numeric ORDER BY values",
                        ));
                    }
                }
            };
            values.push(f);
        }
        values
    } else {
        Vec::new()
    };
    let exact_offset = |bound: &WindowFrameBound| -> Result<Option<Value>, QueryError> {
        match bound {
            WindowFrameBound::Preceding(e) | WindowFrameBound::Following(e) if !interval_frame => {
                Ok(Some(crate::engine::expr::eval_const_runtime(e)?))
            }
            _ => Ok(None),
        }
    };
    let exact_start_offset = exact_offset(&frame.start)?;
    let exact_end_offset = exact_offset(&frame.end)?;
    // Determine sort direction (ASC vs DESC) for single-column ORDER BY when
    // resolving RANGE numeric offsets.
    let order_asc = order_by.first().map(|o| o.asc).unwrap_or(true);
    let group_count = group_firsts.len() as i64;
    let group_start = |group_idx: i64| -> i64 {
        if group_idx < 0 {
            0
        } else if group_idx >= group_count {
            n as i64
        } else {
            group_firsts[group_idx as usize] as i64
        }
    };
    let group_end = |group_idx: i64| -> i64 {
        if group_idx < 0 {
            -1
        } else if group_idx >= group_count {
            (n - 1) as i64
        } else {
            group_lasts[group_idx as usize] as i64
        }
    };
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        if i % 256 == 0 {
            crate::engine::cancel::check_cancelled()?;
        }
        let current_group = group_for_pos[i] as i64;
        let current = temporal_order_values.get(i).copied().flatten();
        let temporal_start = window_interval_offset(
            current,
            &start_interval,
            matches!(frame.start, WindowFrameBound::Preceding(_)),
            order_asc,
        )?;
        let temporal_end = window_interval_offset(
            current,
            &end_interval,
            matches!(frame.end, WindowFrameBound::Preceding(_)),
            order_asc,
        )?;
        let exact_start_offset = temporal_start.as_ref().or(exact_start_offset.as_ref());
        let exact_end_offset = temporal_end.as_ref().or(exact_end_offset.as_ref());
        let start = match &frame.start {
            WindowFrameBound::UnboundedPreceding => 0i64,
            WindowFrameBound::UnboundedFollowing => (n - 1) as i64,
            WindowFrameBound::CurrentRow => match frame.units {
                WindowFrameUnits::Rows => i as i64,
                WindowFrameUnits::Range => peer_first[i] as i64,
                WindowFrameUnits::Groups => group_start(current_group),
            },
            WindowFrameBound::Preceding(_) => match frame.units {
                WindowFrameUnits::Rows => (i as i64).saturating_sub(start_off as i64),
                WindowFrameUnits::Range => {
                    if matches!(exact_order_values[i], Value::Null) {
                        peer_first[i] as i64
                    } else if let Some(offset) = exact_start_offset {
                        exact_range_bound(&exact_order_values, i, offset, true, order_asc, true)?
                    } else {
                        range_bound_start(&order_values, i, start_off, true, order_asc)
                    }
                }
                WindowFrameUnits::Groups => {
                    group_start(current_group.saturating_sub(start_off as i64))
                }
            },
            WindowFrameBound::Following(_) => match frame.units {
                WindowFrameUnits::Rows => (i as i64).saturating_add(start_off as i64),
                WindowFrameUnits::Range => {
                    if matches!(exact_order_values[i], Value::Null) {
                        peer_first[i] as i64
                    } else if let Some(offset) = exact_start_offset {
                        exact_range_bound(&exact_order_values, i, offset, false, order_asc, true)?
                    } else {
                        range_bound_start(&order_values, i, start_off, false, order_asc)
                    }
                }
                WindowFrameUnits::Groups => {
                    group_start(current_group.saturating_add(start_off as i64))
                }
            },
        };
        let end = match &frame.end {
            WindowFrameBound::UnboundedPreceding => 0i64,
            WindowFrameBound::UnboundedFollowing => (n - 1) as i64,
            WindowFrameBound::CurrentRow => match frame.units {
                WindowFrameUnits::Rows => i as i64,
                WindowFrameUnits::Range => peer_last[i] as i64,
                WindowFrameUnits::Groups => group_end(current_group),
            },
            WindowFrameBound::Preceding(_) => match frame.units {
                WindowFrameUnits::Rows => (i as i64).saturating_sub(end_off as i64),
                WindowFrameUnits::Range => {
                    if matches!(exact_order_values[i], Value::Null) {
                        peer_last[i] as i64
                    } else if let Some(offset) = exact_end_offset {
                        exact_range_bound(&exact_order_values, i, offset, true, order_asc, false)?
                    } else {
                        range_bound_end(&order_values, i, end_off, true, order_asc)
                    }
                }
                WindowFrameUnits::Groups => group_end(current_group.saturating_sub(end_off as i64)),
            },
            WindowFrameBound::Following(_) => match frame.units {
                WindowFrameUnits::Rows => (i as i64).saturating_add(end_off as i64),
                WindowFrameUnits::Range => {
                    if matches!(exact_order_values[i], Value::Null) {
                        peer_last[i] as i64
                    } else if let Some(offset) = exact_end_offset {
                        exact_range_bound(&exact_order_values, i, offset, false, order_asc, false)?
                    } else {
                        range_bound_end(&order_values, i, end_off, false, order_asc)
                    }
                }
                WindowFrameUnits::Groups => group_end(current_group.saturating_add(end_off as i64)),
            },
        };
        let s = start.max(0) as usize;
        let e_clamped = end.min((n as i64) - 1).max(0) as usize;
        // Empty frame when start > end (e.g. ROWS BETWEEN 5 PRECEDING AND 3 PRECEDING
        // when row index < 5). We encode this as (1, 0) so callers can detect
        // emptiness with `e < s`.
        if start > end || start > (n as i64) - 1 || end < 0 {
            out.push(WindowFrameRange {
                start: 1,
                end: 0,
                current: i,
                peer_first: peer_first[i],
                peer_last: peer_last[i],
                exclusion: frame.exclusion,
            });
        } else {
            out.push(WindowFrameRange {
                start: s,
                end: e_clamped,
                current: i,
                peer_first: peer_first[i],
                peer_last: peer_last[i],
                exclusion: frame.exclusion,
            });
        }
    }
    Ok(out)
}

fn window_frame_is_whole_partition(frame: &WindowFrame, order_by: &[OrderBy]) -> bool {
    if !matches!(
        frame.exclusion,
        crate::parser::WindowFrameExclusion::NoOthers
    ) {
        return false;
    }
    if matches!(
        (&frame.start, &frame.end),
        (
            WindowFrameBound::UnboundedPreceding,
            WindowFrameBound::UnboundedFollowing
        )
    ) {
        return true;
    }
    // Without ORDER BY, RANGE/GROUPS CURRENT ROW addresses the single peer
    // group containing the whole partition. Parser/default-frame variants such
    // as RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW are therefore also
    // full-partition frames, even though they are not spelled as ROWS ...
    // UNBOUNDED FOLLOWING.
    order_by.is_empty()
        && matches!(
            frame.units,
            WindowFrameUnits::Range | WindowFrameUnits::Groups
        )
        && matches!(
            frame.start,
            WindowFrameBound::UnboundedPreceding | WindowFrameBound::CurrentRow
        )
        && matches!(
            frame.end,
            WindowFrameBound::UnboundedFollowing | WindowFrameBound::CurrentRow
        )
}

fn try_whole_partition_window_fast_path(
    upper: &str,
    args: &[Expr],
    effective_frame: &WindowFrame,
    order_by: &[OrderBy],
    partitions: &[Vec<usize>],
    columns: &[Column],
    rows: &[Vec<Value>],
) -> Result<Option<Vec<Value>>, QueryError> {
    if !order_by.is_empty() || !window_frame_is_whole_partition(effective_frame, order_by) {
        return Ok(None);
    }
    if !matches!(upper, "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
        return Ok(None);
    }
    let count_star =
        upper == "COUNT" && (args.is_empty() || args.iter().any(|a| matches!(a, Expr::Wildcard)));
    let arg = if count_star {
        None
    } else {
        Some(args.first().ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("{upper} requires an argument"),
            )
        })?)
    };
    let mut result = vec![Value::Null; rows.len()];
    for partition in partitions {
        crate::engine::cancel::check_cancelled()?;
        let value = if count_star {
            Value::Int(partition.len() as i64)
        } else {
            let arg = arg.expect("window aggregate argument checked");
            let input = PartitionInput {
                partition,
                columns,
                rows,
            };
            with_window_aggregate!(upper, false, State, { input.whole::<State>(arg) })?
        };
        for (window_pos, &row_idx) in partition.iter().enumerate() {
            if window_pos % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            result[row_idx] = value.clone();
        }
    }
    Ok(Some(result))
}

/// Per-spec window evaluation given pre-computed partitions (sorted).
/// Used both directly (for sort-sharing across multiple windows in the
/// same `(partition_by, order_by)` group) and via
/// `evaluate_window_with_partitions` which wraps it with default-frame
/// resolution.
/// Fold a window-function integer argument (NTILE bucket count, LAG/LEAD
/// offset, NTH_VALUE position) to a constant `i64`.
///
/// mydb also accepts integer constant expressions beyond MySQL's literal /
/// parameter syntax. Evaluate once per statement, resolve runtime parameters,
/// and reject fractional values instead of truncating them. Column references
/// and other non-constant arguments are rejected by `eval_const_runtime`.
pub(super) fn window_const_i64(expr: &Expr) -> Result<i64, QueryError> {
    let v = crate::engine::expr::eval_const_runtime(expr)?;
    if matches!(v, Value::Null) {
        return Err(error(
            crate::error::catalog::ER_WRONG_ARGUMENTS,
            "incorrect arguments to window function (constant NULL)",
        ));
    }
    let number = numeric(&v)?;
    if !number.is_finite() || number.fract() != 0.0 {
        return Err(error(
            crate::error::catalog::ER_WRONG_ARGUMENTS,
            "window argument must be an integer",
        ));
    }
    let integer = crate::storage::to_i64(v.clone())?;
    if !compare_values(&v, &Value::Int(integer)).is_ok_and(|o| o.is_eq()) {
        return Err(error(
            crate::error::catalog::ER_WRONG_ARGUMENTS,
            "window argument must be an integer",
        ));
    }
    Ok(integer)
}

pub(super) fn evaluate_window_per_partition(
    expr: &Expr,
    effective_frame: &WindowFrame,
    partitions: &[Vec<usize>],
    columns: &[Column],
    rows: &[Vec<Value>],
) -> Result<Vec<Value>, QueryError> {
    let collation = match expr {
        Expr::WindowFunction { name, args, .. }
            if name.eq_ignore_ascii_case("MIN") || name.eq_ignore_ascii_case("MAX") =>
        {
            let schema = synthetic_set_op_schema(columns);
            args.first()
                .and_then(|arg| crate::engine::expr::expr_collation(arg, &schema))
                .map(|info| info.collation_id)
        }
        _ => None,
    };
    crate::engine::expr::with_optional_compare_collation(collation, || {
        evaluate_window_per_partition_inner(expr, effective_frame, partitions, columns, rows)
    })
}

fn evaluate_window_per_partition_inner(
    expr: &Expr,
    effective_frame: &WindowFrame,
    partitions: &[Vec<usize>],
    columns: &[Column],
    rows: &[Vec<Value>],
) -> Result<Vec<Value>, QueryError> {
    let Expr::WindowFunction {
        name, args, spec, ..
    } = expr
    else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "expected window function expression",
        ));
    };
    let crate::parser::WindowSpec { order_by, .. } = spec.as_ref();

    let upper = name.to_ascii_uppercase();
    if let Some(out) = try_whole_partition_window_fast_path(
        &upper,
        args,
        effective_frame,
        order_by,
        partitions,
        columns,
        rows,
    )? {
        return Ok(out);
    }
    // P1-4: vec fast path. The vec engine (`vec::window::run_window_partition`)
    // handles ROW_NUMBER / RANK / DENSE_RANK + ROWS-frame SUM/COUNT/AVG/MIN/MAX
    // with NoOthers exclusion in incremental O(N) using a sliding window or
    // monotonic deque, instead of the legacy O(N·frame_len) per-partition
    // rescan. The shape gate is intentionally narrow — anything outside
    // (RANGE/GROUPS frames, EXCLUDE CURRENT/GROUP/TIES, complex bound
    // expressions, exotic functions like NTILE/LAG/LEAD/PERCENT_RANK/
    // CUME_DIST/FIRST_VALUE/LAST_VALUE/NTH_VALUE) falls through to the
    // legacy match below, which is the correctness owner.
    if let Some(out) = try_vec_window_fast_path(
        &upper,
        args,
        effective_frame,
        order_by,
        partitions,
        columns,
        rows,
    )? {
        return Ok(out);
    }
    let mut result = vec![Value::Null; rows.len()];
    let peer_fields = window_sort_fields(columns, order_by);
    // Functions that ignore the frame entirely (per MySQL semantics).
    let ignore_frame = matches!(
        upper.as_str(),
        "ROW_NUMBER"
            | "RANK"
            | "DENSE_RANK"
            | "NTILE"
            | "PERCENT_RANK"
            | "CUME_DIST"
            | "LAG"
            | "LEAD"
    );
    for partition in partitions.iter() {
        crate::engine::cancel::check_cancelled()?;
        // Pre-compute per-row frame [start, end] within the partition for
        // aggregate/value-style functions.
        let frame_ranges: Vec<WindowFrameRange> = if ignore_frame {
            Vec::new()
        } else {
            compute_frame_ranges(partition, effective_frame, order_by, columns, rows)?
        };
        // Prefix frames advance only at peer boundaries. Reuse the ordinary
        // aggregate state to preserve exact DECIMAL arithmetic and NULL rules.
        if matches!(upper.as_str(), "SUM" | "AVG" | "COUNT" | "MIN" | "MAX")
            && matches!(effective_frame.start, WindowFrameBound::UnboundedPreceding)
            && matches!(effective_frame.end, WindowFrameBound::CurrentRow)
            && matches!(effective_frame.exclusion, WindowFrameExclusion::NoOthers)
        {
            let count_star = upper == "COUNT"
                && (args.is_empty() || args.iter().any(|a| matches!(a, Expr::Wildcard)));
            let input = PartitionInput {
                partition,
                columns,
                rows,
            };
            with_window_aggregate!(upper.as_str(), count_star, State, {
                input.prefix::<State>(args, &frame_ranges, &mut result)
            })?;
            continue;
        }
        match upper.as_str() {
            "ROW_NUMBER" => {
                for (i, &row_idx) in partition.iter().enumerate() {
                    if i % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    result[row_idx] = Value::Int((i + 1) as i64);
                }
            }
            "RANK" | "DENSE_RANK" => {
                let mut prev_key: Option<Vec<Value>> = None;
                let mut current_rank: i64 = 0;
                for (i, &row_idx) in partition.iter().enumerate() {
                    if i % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let key: Vec<Value> = order_by
                        .iter()
                        .map(|o| eval_expr_against_columns(&o.expr, columns, &rows[row_idx]))
                        .collect::<Result<_, QueryError>>()?;
                    if !prev_key.as_ref().is_some_and(|prev| {
                        key.iter()
                            .zip(prev)
                            .zip(&peer_fields)
                            .all(|((a, b), field)| {
                                crate::engine::executor::sort::compare_value_for_sort(a, b, *field)
                                    .is_eq()
                            })
                    }) {
                        current_rank = if upper == "RANK" {
                            (i + 1) as i64
                        } else {
                            current_rank + 1
                        };
                        prev_key = Some(key);
                    }
                    result[row_idx] = Value::Int(current_rank);
                }
            }
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
                let count_star = upper == "COUNT"
                    && (args.is_empty() || args.iter().any(|a| matches!(a, Expr::Wildcard)));
                let arg = if count_star {
                    None
                } else {
                    Some(args.first().ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            format!("{name} requires an argument"),
                        )
                    })?)
                };
                let input = PartitionInput {
                    partition,
                    columns,
                    rows,
                };
                with_window_aggregate!(upper.as_str(), count_star, State, {
                    input.frames::<State>(arg, &frame_ranges, &mut result)
                })?;
            }
            "STD" | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP"
            | "VAR_SAMP" => {
                let arg = args.first().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        format!("{name} requires an argument"),
                    )
                })?;
                let sample = matches!(upper.as_str(), "STDDEV_SAMP" | "VAR_SAMP");
                let take_sqrt = matches!(
                    upper.as_str(),
                    "STD" | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP"
                );
                // Evaluate the argument once per partition row, then
                // answer every frame from prefix sums of (count, Σx, Σx²)
                // — O(1) per row instead of O(frame). Frame exclusions
                // fall back to the per-frame loop.
                let vals: Vec<Option<f64>> = partition
                    .iter()
                    .map(
                        |&i| match eval_expr_against_columns(arg, columns, &rows[i])? {
                            Value::Null => Ok(None),
                            v => numeric(&v).map(Some),
                        },
                    )
                    .collect::<Result<_, QueryError>>()?;
                let finish = |cnt: i64, sum: f64, sumsq: f64| -> Value {
                    if cnt == 0 || (sample && cnt < 2) {
                        return Value::Null;
                    }
                    let mean = sum / cnt as f64;
                    // Σ(x-μ)² computed as Σx² - n·μ²; clamp the tiny
                    // negative noise f64 cancellation can leave behind.
                    let mut var = (sumsq / cnt as f64 - mean * mean).max(0.0);
                    if sample {
                        var *= cnt as f64 / (cnt - 1) as f64;
                    }
                    Value::Double(if take_sqrt { var.sqrt() } else { var })
                };
                let no_exclusion = matches!(
                    effective_frame.exclusion,
                    crate::parser::WindowFrameExclusion::NoOthers
                );
                if no_exclusion {
                    let n = partition.len();
                    let mut pre_cnt = vec![0i64; n + 1];
                    let mut pre_sum = vec![0f64; n + 1];
                    let mut pre_sumsq = vec![0f64; n + 1];
                    for (i, v) in vals.iter().enumerate() {
                        let (c, s, q) = match v {
                            Some(x) => (1, *x, *x * *x),
                            None => (0, 0.0, 0.0),
                        };
                        pre_cnt[i + 1] = pre_cnt[i] + c;
                        pre_sum[i + 1] = pre_sum[i] + s;
                        pre_sumsq[i + 1] = pre_sumsq[i] + q;
                    }
                    for (pos, &row_idx) in partition.iter().enumerate() {
                        if pos % 256 == 0 {
                            crate::engine::cancel::check_cancelled()?;
                        }
                        let range = frame_ranges[pos];
                        if range.end < range.start {
                            result[row_idx] = Value::Null;
                            continue;
                        }
                        let (s, e) = (range.start, range.end + 1);
                        result[row_idx] = finish(
                            pre_cnt[e] - pre_cnt[s],
                            pre_sum[e] - pre_sum[s],
                            pre_sumsq[e] - pre_sumsq[s],
                        );
                    }
                } else {
                    for (pos, &row_idx) in partition.iter().enumerate() {
                        if pos % 256 == 0 {
                            crate::engine::cancel::check_cancelled()?;
                        }
                        let range = frame_ranges[pos];
                        let (mut cnt, mut sum, mut sumsq) = (0i64, 0f64, 0f64);
                        if range.end >= range.start {
                            for frame_pos in range.start..=range.end {
                                if frame_pos % 256 == 0 {
                                    crate::engine::cancel::check_cancelled()?;
                                }
                                if frame_position_excluded(&range, frame_pos) {
                                    continue;
                                }
                                if let Some(x) = vals[frame_pos] {
                                    cnt += 1;
                                    sum += x;
                                    sumsq += x * x;
                                }
                            }
                        }
                        result[row_idx] = finish(cnt, sum, sumsq);
                    }
                }
            }
            "BIT_AND" | "BIT_OR" | "BIT_XOR" => {
                let arg = args.first().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        format!("{name} requires an argument"),
                    )
                })?;
                let vals: Vec<Value> = partition
                    .iter()
                    .map(|&i| eval_expr_against_columns(arg, columns, &rows[i]))
                    .collect::<Result<_, QueryError>>()?;
                let mode = match upper.as_str() {
                    "BIT_AND" => "BIT_AND",
                    "BIT_OR" => "BIT_OR",
                    _ => "BIT_XOR",
                };
                for (pos, &row_idx) in partition.iter().enumerate() {
                    if pos % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let range = frame_ranges[pos];
                    let mut state = crate::engine::expr::BitAggregateState::new(mode);
                    if range.end >= range.start {
                        for frame_pos in range.start..=range.end {
                            if frame_pos % 256 == 0 {
                                crate::engine::cancel::check_cancelled()?;
                            }
                            if frame_position_excluded(&range, frame_pos) {
                                continue;
                            }
                            state.update(&vals[frame_pos])?;
                        }
                    }
                    result[row_idx] = state.finish()?;
                }
            }
            "JSON_ARRAYAGG" => {
                let arg = args.first().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "JSON_ARRAYAGG requires an argument",
                    )
                })?;
                let vals: Vec<Vec<u8>> = partition
                    .iter()
                    .map(|&i| -> Result<_, QueryError> {
                        Ok(crate::engine::expr::value_to_jsonb_typed(
                            &eval_expr_against_columns(arg, columns, &rows[i])?,
                            crate::engine::expr::expr_is_boolean_typed(arg),
                        ))
                    })
                    .collect::<Result<_, QueryError>>()?;
                for (pos, &row_idx) in partition.iter().enumerate() {
                    if pos % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let range = frame_ranges[pos];
                    let mut arr = Vec::new();
                    if range.end >= range.start {
                        for frame_pos in range.start..=range.end {
                            if frame_pos % 256 == 0 {
                                crate::engine::cancel::check_cancelled()?;
                            }
                            if frame_position_excluded(&range, frame_pos) {
                                continue;
                            }
                            arr.push(vals[frame_pos].clone());
                        }
                    }
                    result[row_idx] = crate::engine::expr::checked_json_value(
                        crate::engine::json::build_array(&arr),
                    )?;
                }
            }
            "JSON_OBJECTAGG" => {
                if args.len() != 2 {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
                        "JSON_OBJECTAGG requires a key and a value argument",
                    ));
                }
                let pairs: Vec<(Value, Vec<u8>)> = partition
                    .iter()
                    .map(|&i| -> Result<_, QueryError> {
                        let key = eval_expr_against_columns(&args[0], columns, &rows[i])?;
                        let val = crate::engine::expr::value_to_jsonb_typed(
                            &eval_expr_against_columns(&args[1], columns, &rows[i])?,
                            crate::engine::expr::expr_is_boolean_typed(&args[1]),
                        );
                        Ok((key, val))
                    })
                    .collect::<Result<_, QueryError>>()?;
                for (pos, &row_idx) in partition.iter().enumerate() {
                    if pos % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let range = frame_ranges[pos];
                    let mut map = std::collections::BTreeMap::new();
                    if range.end >= range.start {
                        for frame_pos in range.start..=range.end {
                            if frame_pos % 256 == 0 {
                                crate::engine::cancel::check_cancelled()?;
                            }
                            if frame_position_excluded(&range, frame_pos) {
                                continue;
                            }
                            let (key, val) = &pairs[frame_pos];
                            if matches!(key, Value::Null) {
                                return Err(error(
                                    crate::error::catalog::ER_JSON_DOCUMENT_NULL_KEY,
                                    "JSON documents may not contain NULL member names",
                                ));
                            }
                            map.insert(value_to_string(key), val.clone());
                        }
                    }
                    let pairs = map.into_iter().collect::<Vec<_>>();
                    result[row_idx] = crate::engine::expr::checked_json_value(
                        crate::engine::json::build_object(&pairs),
                    )?;
                }
            }
            "NTILE" => {
                let buckets: i64 = match args.first() {
                    Some(arg) => {
                        let n = window_const_i64(arg)?;
                        if n <= 0 {
                            return Err(error(
                                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                                "NTILE requires a positive integer bucket count",
                            ));
                        }
                        n
                    }
                    None => {
                        return Err(error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            "NTILE requires a positive integer bucket count",
                        ));
                    }
                };
                let n = partition.len() as i64;
                let base = n / buckets;
                let extra = n % buckets;
                // At most n nonempty buckets, even when buckets is i64::MAX.
                let large_rows = (base + 1) * extra;
                for (position, &row_idx) in partition.iter().enumerate() {
                    if position % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    if position % 1024 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let position = position as i64;
                    let bucket = if position < large_rows {
                        position / (base + 1) + 1
                    } else {
                        // base is zero only when all rows belong to large buckets.
                        extra + (position - large_rows) / base + 1
                    };
                    result[row_idx] = Value::Int(bucket);
                }
            }
            "FIRST_VALUE" => {
                let arg = args.first().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "FIRST_VALUE requires an argument",
                    )
                })?;
                for (pos, &row_idx) in partition.iter().enumerate() {
                    if pos % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let range = frame_ranges[pos];
                    let Some(first_pos) = (range.start..=range.end)
                        .find(|frame_pos| !frame_position_excluded(&range, *frame_pos))
                    else {
                        result[row_idx] = Value::Null;
                        continue;
                    };
                    let v = eval_expr_against_columns(arg, columns, &rows[partition[first_pos]])?;
                    result[row_idx] = v;
                }
            }
            "LAST_VALUE" => {
                let arg = args.first().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "LAST_VALUE requires an argument",
                    )
                })?;
                for (pos, &row_idx) in partition.iter().enumerate() {
                    if pos % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let range = frame_ranges[pos];
                    let Some(last_pos) = (range.start..=range.end)
                        .rev()
                        .find(|frame_pos| !frame_position_excluded(&range, *frame_pos))
                    else {
                        result[row_idx] = Value::Null;
                        continue;
                    };
                    let v = eval_expr_against_columns(arg, columns, &rows[partition[last_pos]])?;
                    result[row_idx] = v;
                }
            }
            "NTH_VALUE" => {
                let arg = args.first().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "NTH_VALUE requires a value argument",
                    )
                })?;
                let n: i64 = match args.get(1) {
                    Some(arg) => {
                        let n = window_const_i64(arg)?;
                        if n <= 0 {
                            return Err(error(
                                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                                "NTH_VALUE requires a positive integer position",
                            ));
                        }
                        n
                    }
                    None => {
                        return Err(error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            "NTH_VALUE requires a positive integer position",
                        ));
                    }
                };
                for (pos, &row_idx) in partition.iter().enumerate() {
                    if pos % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let range = frame_ranges[pos];
                    if range.end < range.start {
                        result[row_idx] = Value::Null;
                        continue;
                    }
                    let mut seen = 0i64;
                    let mut value = Value::Null;
                    for frame_pos in range.start..=range.end {
                        if frame_pos % 256 == 0 {
                            crate::engine::cancel::check_cancelled()?;
                        }
                        if frame_position_excluded(&range, frame_pos) {
                            continue;
                        }
                        seen += 1;
                        if seen == n {
                            value = eval_expr_against_columns(
                                arg,
                                columns,
                                &rows[partition[frame_pos]],
                            )?;
                            break;
                        }
                    }
                    result[row_idx] = value;
                }
            }
            "PERCENT_RANK" => {
                let n = partition.len() as f64;
                let mut prev_key: Option<Vec<Value>> = None;
                let mut current_rank: i64 = 0;
                for (i, &row_idx) in partition.iter().enumerate() {
                    if i % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let key: Vec<Value> = order_by
                        .iter()
                        .map(|o| eval_expr_against_columns(&o.expr, columns, &rows[row_idx]))
                        .collect::<Result<_, QueryError>>()?;
                    if !prev_key.as_ref().is_some_and(|prev| {
                        key.iter()
                            .zip(prev)
                            .zip(&peer_fields)
                            .all(|((a, b), field)| {
                                crate::engine::executor::sort::compare_value_for_sort(a, b, *field)
                                    .is_eq()
                            })
                    }) {
                        current_rank = (i + 1) as i64;
                        prev_key = Some(key);
                    }
                    let value = if n <= 1.0 {
                        0.0
                    } else {
                        (current_rank - 1) as f64 / (n - 1.0)
                    };
                    result[row_idx] = Value::Double(value);
                }
            }
            "CUME_DIST" => {
                let n = partition.len() as f64;
                let mut group_counts: Vec<usize> = Vec::with_capacity(partition.len());
                let mut prev_key: Option<Vec<Value>> = None;
                let mut count_in_group: usize = 0;
                let mut group_starts: Vec<usize> = Vec::new();
                for (i, &row_idx) in partition.iter().enumerate() {
                    if i % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let key: Vec<Value> = order_by
                        .iter()
                        .map(|o| eval_expr_against_columns(&o.expr, columns, &rows[row_idx]))
                        .collect::<Result<_, QueryError>>()?;
                    if !prev_key.as_ref().is_some_and(|prev| {
                        key.iter()
                            .zip(prev)
                            .zip(&peer_fields)
                            .all(|((a, b), field)| {
                                crate::engine::executor::sort::compare_value_for_sort(a, b, *field)
                                    .is_eq()
                            })
                    }) {
                        if count_in_group > 0 {
                            for _ in 0..count_in_group {
                                group_counts.push(count_in_group);
                            }
                        }
                        prev_key = Some(key);
                        count_in_group = 1;
                        group_starts.push(i);
                    } else {
                        count_in_group += 1;
                    }
                }
                for _ in 0..count_in_group {
                    group_counts.push(count_in_group);
                }
                let mut cumulative: usize = 0;
                let mut idx = 0;
                while idx < partition.len() {
                    let count = group_counts[idx];
                    cumulative += count;
                    let value = cumulative as f64 / n;
                    for k in 0..count {
                        result[partition[idx + k]] = Value::Double(value);
                    }
                    idx += count;
                }
            }
            "LAG" | "LEAD" => {
                let direction: i64 = if upper == "LAG" { -1 } else { 1 };
                let offset: i64 = match args.get(1) {
                    Some(arg) => {
                        let n = window_const_i64(arg)?;
                        if n < 0 {
                            return Err(error(
                                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                                format!("{upper} offset must be a non-negative constant"),
                            ));
                        }
                        n
                    }
                    None => 1,
                };
                for (i, &row_idx) in partition.iter().enumerate() {
                    if i % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let target = (i as i128) + i128::from(direction) * i128::from(offset);
                    let value = if target >= 0 && (target as usize) < partition.len() {
                        let ti = partition[target as usize];
                        if let Some(arg) = args.first() {
                            eval_expr_against_columns(arg, columns, &rows[ti])?
                        } else {
                            Value::Null
                        }
                    } else {
                        match args.get(2) {
                            Some(arg) => eval_expr_against_columns(arg, columns, &rows[row_idx])?,
                            None => Value::Null,
                        }
                    };
                    result[row_idx] = value;
                }
            }
            _ => {
                return Err(error(
                    crate::error::catalog::ER_SP_DOES_NOT_EXIST,
                    format!("window function '{name}' is not supported"),
                ));
            }
        }
    }
    Ok(result)
}

/// Rewrite an ORDER BY expression so any sub-expression that matches a
/// projection definition is replaced by a reference to that output column.
///
/// The top-level rollup/group-by binding expands an ORDER BY alias back to its
/// definition (`ORDER BY lochierarchy` → `ORDER BY GROUPING(a)+GROUPING(b)`).
/// Over the windowless output rows that definition is no longer computable
/// (`GROUPING()` needs the aggregate context), but the value already lives in
/// the materialised `lochierarchy` column. Re-pointing the expression at that
/// column lets the window-path final ORDER BY sort correctly (TPC-DS q36/q70/q86).
fn rebind_expr_to_output_columns(expr: &mut Expr, proj_map: &[(Expr, String)]) {
    crate::parser::visit::rewrite_value_expr(expr, |node| {
        if let Some((_, name)) = proj_map.iter().find(|(source, _)| source == node) {
            *node = Expr::Identifier(name.clone());
            return false;
        }
        // Bind scalar/query-predicate operands in this output scope. Nested
        // SELECTs, group-key provenance and window definitions own their
        // bindings; a whole-node projection match above remains valid.
        !matches!(node, Expr::GroupKeyRef { .. } | Expr::WindowFunction { .. })
    });
}

pub(super) fn eval_expr_against_columns(
    expr: &Expr,
    columns: &[Column],
    row: &[Value],
) -> Result<Value, QueryError> {
    match expr {
        Expr::Identifier(name) => columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
            .and_then(|i| row.get(i).cloned())
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    format!("unknown column '{name}'"),
                )
            }),
        Expr::CompoundIdentifier(parts) => {
            let last = parts.last().map(String::as_str).unwrap_or("");
            columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(last))
                .and_then(|i| row.get(i).cloned())
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_BAD_FIELD_ERROR,
                        format!("unknown column '{last}'"),
                    )
                })
        }
        Expr::Literal(lit) => Ok(literal_value(lit)),
        // Anything else (arithmetic, CASE, function calls — e.g. a window
        // `PARTITION BY GROUPING(a)+GROUPING(b)` whose grouping operands were
        // hoisted into `__winagg_*` columns) is evaluated with the full
        // expression engine over a synthetic schema built from the materialised
        // window-input columns. Without this the partition/order key collapses
        // to NULL for every row and all rows fall into one partition.
        other => {
            let schema = synthetic_set_op_schema(columns);
            crate::engine::expr::eval_expr(other, &schema, row)
        }
    }
}

pub(super) fn expr_column_position(expr: &Expr, columns: &[Column]) -> Option<usize> {
    let name = match expr {
        Expr::Identifier(name) => name.as_str(),
        Expr::CompoundIdentifier(parts) => parts.last().map(String::as_str).unwrap_or(""),
        _ => return None,
    };
    columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
}
