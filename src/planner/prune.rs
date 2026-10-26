//! Redundant GROUP BY key pruning.

use super::*;

/// Remove constant keys from an ordinary GROUP BY when at least one real key
/// remains. A constant cannot split an existing group, but retaining it makes
/// every hash-aggregate row evaluate, encode and store a redundant lane.  Do
/// not remove the last key: on an empty input `GROUP BY <constant>` emits no
/// rows, whereas an implicit aggregate group may emit one, and a non-aggregate
/// SELECT would otherwise cease to be a grouping query altogether. ROLLUP and
/// GROUPING SETS retain their exact key positions for GROUPING() semantics.
pub(crate) fn prune_redundant_constant_group_keys(select: &mut Select) {
    if select.group_by_modifier != GroupByModifier::None
        || !select.grouping_sets.is_empty()
        || select.group_by.len() < 2
    {
        return;
    }
    if !select
        .group_by
        .iter()
        .any(|expr| !crate::engine::expr::expr_is_constant(expr))
    {
        return;
    }
    select
        .group_by
        .retain(|expr| !crate::engine::expr::expr_is_constant(expr));
}

/// Remove warning-free integer expressions that are functionally determined
/// by another GROUP BY key.  A signed INT (or narrower) promoted through
/// addition/subtraction with integer literals has an exact BIGINT result when
/// its statically bounded range fits i64.  Keeping both `x` and `x - 1` as
/// grouping keys cannot split a group, but it does evaluate, materialize and
/// hash the derived lane for every input row.
///
/// This intentionally stays narrow: one catalog table, ordinary GROUP BY,
/// signed fixed-width columns no wider than INT, and +/- literal expression
/// trees only. BIGINT, unsigned, joins, stored functions and any expression
/// that may coerce, warn or overflow retain the original keys and semantics.
pub(crate) fn prune_redundant_functional_group_keys(db: &Database, select: &mut Select) {
    if select.group_by_modifier != GroupByModifier::None
        || !select.grouping_sets.is_empty()
        || select.group_by.len() < 2
        || !select.joins.is_empty()
        || select.derived.is_some()
        || select.from.len() != 1
    {
        return;
    }
    let Ok(table) = db.table(&select.from[0].name) else {
        return;
    };

    let bases = select
        .group_by
        .iter()
        .filter_map(|expr| {
            let name = expr_column_name(expr)?;
            let index = table.column_index(name).ok()?;
            let column = &table.columns[index];
            if column.unsigned
                || !matches!(
                    column.data_type.column_type(),
                    crate::types::ColumnType::Tiny
                        | crate::types::ColumnType::Short
                        | crate::types::ColumnType::MediumInt
                        | crate::types::ColumnType::Long
                )
            {
                return None;
            }
            Some(name.to_ascii_lowercase())
        })
        .collect::<std::collections::HashSet<_>>();
    if bases.is_empty() {
        return;
    }

    select.group_by.retain(|expr| {
        // Bare columns are the determinant keys and must remain.
        if expr_column_name(expr).is_some() {
            return true;
        }
        !bases.iter().any(|base| {
            integer_affine_group_offset(expr, base).is_some_and(|offset| {
                i128::from(i32::MIN)
                    .checked_add(offset)
                    .is_some_and(|min| min >= i128::from(i64::MIN))
                    && i128::from(i32::MAX)
                        .checked_add(offset)
                        .is_some_and(|max| max <= i128::from(i64::MAX))
            })
        })
    });
}

/// Post-parallel pass that sets `output_columns_in_projection` on each
/// `ParallelSeqScan` whose scan filter references columns nothing else
/// in the plan touches. The streaming hash-join probe skips those
/// per-row `Value::clone`s entirely — see
/// [`StreamingProbeScanShape::output_columns_in_projection`].
///
/// Runs AFTER `apply_parallel_hints` because (a) the streaming probe
/// only handles `Exchange { ParallelSeqScan }`, and (b) the final plan
/// has all post-finalize wrappers (Sort, Limit, Projection,
/// outer-Aggregate) in place so reference collection sees every
/// downstream consumer.
pub(super) fn apply_output_column_pruning_pass(plan: &mut PhysicalPlan, select: &Select) {
    let Some(mut refs) = ColumnPruningRefs::new(plan, true) else {
        return;
    };
    refs.collect_plan(plan, true);
    // The physical join keys/residuals already reflect filter pushdown.
    // Folding the original ON clause would retain filter-only columns.
    refs.collect_select(select, false);
    apply_output_column_pruning(plan, &refs.tables, &refs.tab_refs, &refs.tab_wild);
}

pub(super) fn mark_covering_index_recursive(
    ctx: &crate::engine::cbo::CboContext,
    plan: &mut PhysicalPlan,
    select: &Select,
) {
    mark_covering_index_recursive_with_extra(ctx, plan, select, None);
}

pub(super) fn mark_covering_index_recursive_with_extra(
    ctx: &crate::engine::cbo::CboContext,
    plan: &mut PhysicalPlan,
    select: &Select,
    extra_columns_expr: Option<&Expr>,
) {
    match plan {
        PhysicalPlan::NullExtend { .. } | PhysicalPlan::RrfFusion { .. } => {}

        PhysicalPlan::IndexLookup {
            table,
            index,
            covering,
            filter,
            ..
        }
        | PhysicalPlan::IndexMultiLookup {
            table,
            index,
            covering,
            filter,
            ..
        }
        | PhysicalPlan::IndexInLookup {
            table,
            index,
            covering,
            filter,
            ..
        } => {
            let scan_extra = combine_optional_exprs(extra_columns_expr.cloned(), filter.clone());
            apply_covering_check(table, index, covering, select, scan_extra.as_ref());
        }
        PhysicalPlan::IndexRangeScan {
            table,
            index,
            filter,
            ..
        } => {
            // First check if this range scan would now cover; if so, swap
            // it for a properly-priced IndexFastScan in place.
            let mut covering = false;
            let scan_extra = combine_optional_exprs(extra_columns_expr.cloned(), filter.clone());
            apply_covering_check(table, index, &mut covering, select, scan_extra.as_ref());
            if covering {
                convert_range_to_fast_scan(ctx, plan);
            }
        }
        PhysicalPlan::IndexFastScan { .. } => {
            // Already covering by definition; nothing to flip.
        }
        PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_table,
            right_index,
            condition,
            right_covering,
            ..
        } => {
            let join_extra = combine_optional_exprs(extra_columns_expr.cloned(), condition.clone());
            mark_covering_index_recursive_with_extra(ctx, left, select, join_extra.as_ref());
            apply_covering_check(
                right_table,
                right_index,
                right_covering,
                select,
                join_extra.as_ref(),
            );
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            condition,
            ..
        } => {
            let join_extra = combine_optional_exprs(extra_columns_expr.cloned(), condition.clone());
            mark_covering_index_recursive_with_extra(ctx, left, select, join_extra.as_ref());
            mark_covering_index_recursive_with_extra(ctx, right, select, join_extra.as_ref());
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_keys,
            right_keys,
            residual,
            ..
        } => {
            let mut join_exprs = Vec::with_capacity(
                left_keys.len() + right_keys.len() + residual.as_ref().map_or(0, |_| 1),
            );
            join_exprs.extend(left_keys.iter().cloned());
            join_exprs.extend(right_keys.iter().cloned());
            if let Some(residual) = residual.as_ref() {
                join_exprs.push(residual.clone());
            }
            let join_extra =
                combine_optional_exprs(extra_columns_expr.cloned(), combine_conjuncts(join_exprs));
            mark_covering_index_recursive_with_extra(ctx, left, select, join_extra.as_ref());
            mark_covering_index_recursive_with_extra(ctx, right, select, join_extra.as_ref());
        }
        PhysicalPlan::AdaptiveJoin {
            left,
            right,
            candidates,
            ..
        } => {
            let adaptive_extra = adaptive_candidate_reference_expr(candidates);
            let side_extra = combine_optional_exprs(extra_columns_expr.cloned(), adaptive_extra);
            for candidate in candidates.iter_mut() {
                if let AdaptiveJoinCandidate::IndexNestedLoop {
                    right_table,
                    right_index,
                    condition,
                    right_covering,
                    ..
                } = candidate
                {
                    let candidate_extra =
                        combine_optional_exprs(side_extra.clone(), condition.clone());
                    apply_covering_check(
                        right_table,
                        right_index,
                        right_covering,
                        select,
                        candidate_extra.as_ref(),
                    );
                }
            }
            mark_covering_index_recursive_with_extra(ctx, left, select, side_extra.as_ref());
            mark_covering_index_recursive_with_extra(ctx, right, select, side_extra.as_ref());
        }
        PhysicalPlan::SetOp { left, right, .. } => {
            mark_covering_index_recursive_with_extra(ctx, left, select, extra_columns_expr);
            mark_covering_index_recursive_with_extra(ctx, right, select, extra_columns_expr);
        }
        PhysicalPlan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            mark_covering_index_recursive_with_extra(ctx, anchor, select, extra_columns_expr);
            mark_covering_index_recursive_with_extra(ctx, recursive, select, extra_columns_expr);
        }
        PhysicalPlan::Filter {
            input, predicate, ..
        }
        | PhysicalPlan::Apply {
            outer: input,
            predicate,
            ..
        } => {
            // Predicates pushed out of JOIN ON are no longer in the final
            // join's residual. Their columns must still be read before this
            // node can evaluate them; otherwise an index-only reconstruction
            // substitutes NULL for a required non-key column.
            let filter_extra =
                combine_optional_exprs(extra_columns_expr.cloned(), Some(predicate.clone()));
            mark_covering_index_recursive_with_extra(ctx, input, select, filter_extra.as_ref());
        }
        PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::HashRollupAggregate { input, .. }
        | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
        | PhysicalPlan::StreamingAggregate { input, .. }
        | PhysicalPlan::StreamingRollupAggregate { input, .. }
        | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::IncrementalSort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::TopWindow { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::WindowAgg { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. }
        | PhysicalPlan::Exchange { input, .. }
        | PhysicalPlan::ParallelHashAggregate { input, .. }
        | PhysicalPlan::ParallelRollupAggregate { input, .. }
        | PhysicalPlan::ParallelGroupingSetsAggregate { input, .. }
        | PhysicalPlan::Materialize { input, .. }
        | PhysicalPlan::ParallelSort { input, .. } => {
            mark_covering_index_recursive_with_extra(ctx, input, select, extra_columns_expr);
        }
        PhysicalPlan::ValuesScan { .. }
        | PhysicalPlan::JsonTableScan { .. }
        | PhysicalPlan::SystemScan { .. }
        | PhysicalPlan::SeqScan { .. }
        | PhysicalPlan::ZoneMapScan { .. }
        | PhysicalPlan::IndexMerge { .. }
        | PhysicalPlan::CteScan { .. }
        | PhysicalPlan::GenerateSeries { .. }
        | PhysicalPlan::FulltextScan { .. }
        | PhysicalPlan::SpatialScan { .. }
        | PhysicalPlan::VectorIndexScan { .. }
        | PhysicalPlan::VectorBruteScan { .. }
        | PhysicalPlan::IndexAggregate { .. }
        | PhysicalPlan::CoveringIndexAggregate { .. }
        | PhysicalPlan::ParallelSeqScan { .. }
        | PhysicalPlan::ParallelZoneMapScan { .. }
        | PhysicalPlan::ParallelIndexFastScan { .. }
        | PhysicalPlan::ParallelIndexRangeScan { .. }
        | PhysicalPlan::ParallelIndexAggregate { .. } => {}
    }
}

/// `MYDB_JOIN_OUTPUT_PRUNE` (default on).
pub(super) fn join_output_prune_disabled() -> bool {
    matches!(
        crate::util::env::var("MYDB_JOIN_OUTPUT_PRUNE")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no"
    )
}

/// `MYDB_JOIN_OUTPUT_PRUNE_TOP` (default on): top-level joins also write a projection.
/// The executor has fused the projection into the stitch (emit helpers clone only the kept
/// columns; see `HashJoinSpec::emit_joined` / `StreamEmitSlot` in plan.rs), so top-level pruning
/// no longer pays the historical post-hoc gather (the root cause of TPC-H Q5 +7%) and flips to a net win.
/// off restores the old "middle levels only" pruning behavior (for differential testing / rollback).
pub(super) fn join_output_prune_top_disabled() -> bool {
    matches!(
        crate::util::env::var("MYDB_JOIN_OUTPUT_PRUNE_TOP")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no"
    )
}

/// Enumerates a scan node's output fields in the same order as the executor's `push_table_fields`.
pub(super) fn join_prune_table_fields(
    table: &PlanTable,
    projection: Option<&[usize]>,
) -> Vec<JoinPruneField> {
    let mk = |i: usize| {
        table.schema.columns.get(i).map(|column| JoinPruneField {
            table: table.schema.name.to_ascii_lowercase(),
            alias: table.alias.as_ref().map(|a| a.to_ascii_lowercase()),
            column: column.name.to_ascii_lowercase(),
        })
    };
    match projection {
        Some(indices) => indices.iter().filter_map(|&i| mk(i)).collect(),
        None => table
            .schema
            .iter_visible_columns()
            .filter_map(|(i, _)| mk(i))
            .collect(),
    }
}

/// Top-down: writes HashJoin projections for the subtree and returns the subtree's final
/// output field order (with the projection written here applied). None = the subtree contains
/// a node with unknown output, so the whole subtree stays unpruned (the caller has reset this level's output_projection).
///
/// `parent_is_join`: pruning only nets a win when this level's output is stitched again by a
/// parent join (paying another full-column clone per output row); a top-level join (parent is
/// agg/sort/projection) narrowing on the materialized path instead pays an extra O(rows×cols) gather
/// — TPC-H Q5 measured +7% for closing at the top level. Hence only middle levels write projections.
pub(super) fn join_prune_walk(
    plan: &mut PhysicalPlan,
    needed: &JoinPruneNeed,
    parent_is_join: bool,
) -> Option<Vec<JoinPruneField>> {
    match plan {
        PhysicalPlan::SeqScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::ZoneMapScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::ParallelSeqScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::ParallelZoneMapScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexFastScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::ParallelIndexFastScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexRangeScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::ParallelIndexRangeScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::FulltextScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::SpatialScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::VectorIndexScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexLookup {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexMultiLookup {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexInLookup {
            table,
            projected_columns,
            ..
        } => Some(join_prune_table_fields(
            table,
            projected_columns.as_deref().map(|v| v.as_slice()),
        )),
        PhysicalPlan::VectorBruteScan { table, .. } => Some(join_prune_table_fields(table, None)),
        PhysicalPlan::Exchange { input, .. } => join_prune_walk(input, needed, parent_is_join),
        PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            residual,
            output_projection,
            ..
        } => {
            let mut child_needed = needed.clone();
            for key in left_keys.iter().chain(right_keys.iter()) {
                child_needed.add_expr(key);
            }
            if let Some(residual) = residual {
                child_needed.add_expr(residual);
            }
            let semi_anti = matches!(join_type, JoinType::Semi | JoinType::Anti);
            let left_fields = join_prune_walk(left, &child_needed, true);
            let right_fields = join_prune_walk(right, &child_needed, true);
            let (Some(left_fields), Some(right_fields)) = (left_fields, right_fields) else {
                *output_projection = None;
                return None;
            };
            let mut out_full = left_fields;
            if !semi_anti {
                out_full.extend(right_fields);
            }
            if !parent_is_join && (semi_anti || join_output_prune_top_disabled()) {
                // Two cases where the top-level join is not pruned:
                // * SEMI/ANTI: output rows are probe (scan) rows moved straight through; scan-level
                //   column pruning already narrowed them, so pruning again is a pure extra subset clone — a net loss.
                // * `MYDB_JOIN_OUTPUT_PRUNE_TOP=off`: restores the old behavior (back in the era of
                //   executor post-hoc gathers, top-level pruning cost TPC-H Q5 +7%; once the projection was
                //   fused into the stitch the default flipped to a net win, and off remains only as a differential/rollback switch).
                *output_projection = None;
                return Some(out_full);
            }
            let mut keep: Vec<usize> = out_full
                .iter()
                .enumerate()
                .filter(|(_, field)| needed.matches(field))
                .map(|(i, _)| i)
                .collect();
            if keep.is_empty() && !out_full.is_empty() {
                // Zero-column rows must not reach the executor: defensively keep the first column.
                keep.push(0);
            }
            if keep.len() == out_full.len() {
                *output_projection = None;
                return Some(out_full);
            }
            let projected: Vec<JoinPruneField> =
                keep.iter().map(|&i| out_full[i].clone()).collect();
            *output_projection = Some(std::sync::Arc::new(keep));
            Some(projected)
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            let mut child_needed = needed.clone();
            if let Some(condition) = condition {
                child_needed.add_expr(condition);
            }
            let semi_anti = matches!(join_type, JoinType::Semi | JoinType::Anti);
            let left_fields = join_prune_walk(left, &child_needed, true)?;
            let right_fields = join_prune_walk(right, &child_needed, true)?;
            let mut out = left_fields;
            if !semi_anti {
                out.extend(right_fields);
            }
            Some(out)
        }
        PhysicalPlan::AdaptiveJoin {
            left,
            right,
            join_type,
            candidates,
            ..
        } => {
            let mut child_needed = needed.clone();
            for candidate in candidates.iter() {
                match candidate {
                    AdaptiveJoinCandidate::Hash {
                        left_keys,
                        right_keys,
                        residual,
                        ..
                    } => {
                        for key in left_keys.iter().chain(right_keys.iter()) {
                            child_needed.add_expr(key);
                        }
                        if let Some(residual) = residual {
                            child_needed.add_expr(residual);
                        }
                    }
                    AdaptiveJoinCandidate::IndexNestedLoop { condition, .. } => {
                        if let Some(condition) = condition {
                            child_needed.add_expr(condition);
                        }
                    }
                    AdaptiveJoinCandidate::NestedLoop { condition, .. } => {
                        if let Some(condition) = condition {
                            child_needed.add_expr(condition);
                        }
                    }
                }
            }
            let semi_anti = matches!(join_type, JoinType::Semi | JoinType::Anti);
            let left_fields = join_prune_walk(left, &child_needed, true)?;
            let right_fields = join_prune_walk(right, &child_needed, true)?;
            let mut out = left_fields;
            if !semi_anti {
                out.extend(right_fields);
            }
            Some(out)
        }
        PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_table,
            join_type,
            condition,
            right_projected_columns,
            ..
        } => {
            let mut child_needed = needed.clone();
            if let Some(condition) = condition {
                child_needed.add_expr(condition);
            }
            let semi_anti = matches!(join_type, JoinType::Semi | JoinType::Anti);
            let left_fields = join_prune_walk(left, &child_needed, true)?;
            let right_fields_full = join_prune_table_fields(right_table, None);
            if !semi_anti {
                let mut keep: Vec<usize> = right_fields_full
                    .iter()
                    .enumerate()
                    .filter(|(_, field)| child_needed.matches(field))
                    .map(|(i, _)| i)
                    .collect();
                if keep.is_empty() && !right_fields_full.is_empty() {
                    keep.push(0);
                }
                if keep.len() == right_fields_full.len() {
                    *right_projected_columns = None;
                } else {
                    *right_projected_columns = Some(std::sync::Arc::new(keep));
                }
            }
            let mut out = left_fields;
            if !semi_anti {
                out.extend(join_prune_table_fields(
                    right_table,
                    right_projected_columns.as_deref().map(|v| v.as_slice()),
                ));
            }
            Some(out)
        }
        _ => None,
    }
}

/// finish_plan-stage entry: starting at the plan root, pass through wrapper operators (folding in
/// their expression references along the way), drill down into the join region, and write output_projection on HashJoins.
pub(super) fn apply_join_output_pruning(plan: &mut PhysicalPlan, select: &Select) {
    if !plan_contains_join_operator(plan) {
        return;
    }
    if join_output_prune_disabled() {
        return;
    }
    // Ordinary GROUP BY / aggregate query blocks are safe here: `needed`
    // below explicitly includes group expressions, aggregate arguments and
    // HAVING, and `collect_plan_fields` observes every projection written by
    // this pass before the aggregate programs are compiled.  Keeping all join
    // columns for those overwhelmingly common queries defeats the main point
    // of this pass (wide multi-join rows are cloned and Grace-spilled at every
    // level).  Grouping sets still carry synthesized GroupKeyRef provenance
    // that this field-name based pass cannot represent precisely, so retain
    // the conservative boundary only for that shape.
    if !select.grouping_sets.is_empty() {
        return;
    }
    if select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_)
        )
    }) {
        return;
    }
    let mut needed = JoinPruneNeed::default();
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            needed.add_expr(expr);
        }
    }
    for aggregate in collect_aggregate_calls(select) {
        needed.add_expr(&aggregate);
    }
    for ob in &select.order_by {
        needed.add_expr(&ob.expr);
    }
    for g in &select.group_by {
        needed.add_expr(g);
    }
    if let Some(having) = &select.having {
        needed.add_expr(having);
    }
    if let Some(selection) = &select.selection {
        // Most WHERE predicates are already pushed down into scans/residuals; folding the references
        // in here is defensive over-retention (the columns must still be there when leftover predicates evaluate on joined rows).
        needed.add_expr(selection);
    }

    fn descend(plan: &mut PhysicalPlan, needed: &JoinPruneNeed) {
        match plan {
            PhysicalPlan::Projection {
                input, projection, ..
            } => {
                let mut n = needed.clone();
                for item in projection.iter() {
                    match item {
                        SelectItem::Expr { expr, .. } => n.add_expr(expr),
                        // A wildcard appears in a plan-level projection: keep the whole subtree.
                        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => return,
                    }
                }
                descend(input, &n);
            }
            PhysicalPlan::Sort {
                input, order_by, ..
            }
            | PhysicalPlan::IncrementalSort {
                input, order_by, ..
            }
            | PhysicalPlan::TopK {
                input, order_by, ..
            }
            | PhysicalPlan::ParallelTopK {
                input, order_by, ..
            }
            | PhysicalPlan::ParallelSort {
                input, order_by, ..
            } => {
                let mut n = needed.clone();
                for ob in order_by.iter() {
                    n.add_expr(&ob.expr);
                }
                descend(input, &n);
            }
            PhysicalPlan::Limit { input, .. } | PhysicalPlan::Exchange { input, .. } => {
                descend(input, needed)
            }
            PhysicalPlan::HashAggregate {
                input,
                group_by,
                having,
                aggregates,
                ..
            }
            | PhysicalPlan::ParallelHashAggregate {
                input,
                group_by,
                having,
                aggregates,
                ..
            }
            | PhysicalPlan::StreamingAggregate {
                input,
                group_by,
                having,
                aggregates,
                ..
            } => {
                let mut n = needed.clone();
                for e in group_by.iter().chain(aggregates.iter()) {
                    n.add_expr(e);
                }
                if let Some(h) = having {
                    n.add_expr(h);
                }
                descend(input, &n);
            }
            PhysicalPlan::StreamingDistinct { input, .. } => descend(input, needed),
            PhysicalPlan::HashJoin { .. }
            | PhysicalPlan::NestedLoopJoin { .. }
            | PhysicalPlan::AdaptiveJoin { .. }
            | PhysicalPlan::IndexNestedLoopJoin { .. } => {
                let _ = join_prune_walk(plan, needed, false);
            }
            // Remaining nodes (window/ROLLUP/GROUPING SETS/CTE etc.): their output columns or
            // reference sets can't be characterized precisely here, so conservatively don't prune.
            _ => {}
        }
    }
    descend(plan, &needed);
}

/// * `IndexLookup` / `IndexMultiLookup` / `IndexInLookup` / `CteScan`
///   gained `projected_columns` for this pass, so the IN-subquery
///   row-id fan-out and CTE reuse paths also benefit.
pub(super) fn apply_column_pruning(plan: &mut PhysicalPlan, select: &Select) {
    let Some(mut refs) = ColumnPruningRefs::new(plan, false) else {
        return;
    };
    refs.collect_plan(plan, false);
    // Upper-pipeline nodes have not all been attached by finish_plan yet.
    refs.collect_select(select, true);
    refs.apply(plan);
}

/// One column-reference collection owns all parallel state, so AST and plan
/// projections use the same wildcard and alias-shadowing rules. Constructed
/// once per pass; recursive visits borrow it without cloning or allocating.
struct ColumnPruningRefs {
    tables: Vec<PlanTable>,
    cte_targets: Vec<(String, Option<String>, Vec<Column>)>,
    cte_col_names: Vec<HashSet<String>>,
    tab_refs: Vec<HashSet<String>>,
    tab_wild: Vec<bool>,
    cte_refs: Vec<HashSet<String>>,
    cte_wild: Vec<bool>,
}

impl ColumnPruningRefs {
    // The output pass needs base scans; avoid allocating CTE reference sets
    // when that pass cannot prune any output. Decode pruning also admits CTEs.
    fn new(plan: &PhysicalPlan, require_base_table: bool) -> Option<Self> {
        let mut tables = Vec::new();
        let mut cte_targets = Vec::new();
        collect_scan_targets(plan, &mut tables, &mut cte_targets);
        if tables.is_empty() && (require_base_table || cte_targets.is_empty()) {
            return None;
        }
        Some(Self {
            tab_refs: vec![HashSet::new(); tables.len()],
            tab_wild: vec![false; tables.len()],
            cte_refs: vec![HashSet::new(); cte_targets.len()],
            cte_wild: vec![false; cte_targets.len()],
            cte_col_names: cte_targets
                .iter()
                .map(|(_, _, columns)| {
                    columns
                        .iter()
                        .map(|c| normalize_expr_column(&c.name))
                        .collect()
                })
                .collect(),
            tables,
            cte_targets,
        })
    }

    fn collect_expr(&mut self, expr: &Expr) {
        if !self.tables.is_empty() {
            collect_expr_columns_by_table(expr, &self.tables, &mut self.tab_refs);
        }
        for (i, (name, alias, _)) in self.cte_targets.iter().enumerate() {
            collect_expr_cte_column_refs(
                expr,
                name,
                alias.as_deref(),
                &self.cte_col_names[i],
                &mut self.cte_refs[i],
            );
        }
    }

    fn collect_projection(&mut self, projection: &[SelectItem]) {
        for item in projection {
            match item {
                SelectItem::Wildcard => {
                    self.tab_wild.fill(true);
                    self.cte_wild.fill(true);
                }
                SelectItem::QualifiedWildcard(qual) => {
                    for (i, tab) in self.tables.iter().enumerate() {
                        if matches_table(qual, &tab.schema.name, tab.alias.as_deref()) {
                            self.tab_wild[i] = true;
                        }
                    }
                    for (i, (name, alias, _)) in self.cte_targets.iter().enumerate() {
                        // alias-shadows-name (see `matches_table`).
                        let hit = match alias.as_deref() {
                            Some(a) => qual.eq_ignore_ascii_case(a),
                            None => qual.eq_ignore_ascii_case(name),
                        };
                        if hit {
                            self.cte_wild[i] = true;
                        }
                    }
                }
                SelectItem::Expr { expr, .. } => self.collect_expr(expr),
            }
        }
    }

    /// Include AST join conditions only before the filter-only output pass.
    fn collect_select(&mut self, select: &Select, include_join_conditions: bool) {
        self.collect_projection(&select.projection);
        for aggregate in collect_aggregate_calls(select) {
            self.collect_expr(&aggregate);
        }
        if let Some(e) = &select.selection {
            self.collect_expr(e);
        }
        if include_join_conditions {
            for j in &select.joins {
                if let Some(c) = &j.condition {
                    self.collect_expr(c);
                }
            }
        }
        for ob in &select.order_by {
            self.collect_expr(&ob.expr);
        }
        for g in &select.group_by {
            self.collect_expr(g);
        }
        if let Some(h) = &select.having {
            self.collect_expr(h);
        }
    }

    fn collect_index_probe_columns(&mut self, table: &PlanTable, index: &IndexSchema) {
        for (i, target) in self.tables.iter().enumerate() {
            if plan_table_matches(target, &table.schema.name, table.alias.as_deref()) {
                for col in &index.columns {
                    let normalized = normalize_expr_column(col);
                    if table_has_column_name(target, &normalized) {
                        self.tab_refs[i].insert(normalized);
                    }
                }
            }
        }
    }

    /// Scan filters are excluded only when finding filter-only output columns.
    /// NullExtend and IndexInLookup retain their existing scope boundaries.
    fn collect_plan(&mut self, plan: &PhysicalPlan, skip_scan_filters: bool) {
        match plan {
            PhysicalPlan::NullExtend { .. } | PhysicalPlan::RrfFusion { .. } => {}

            PhysicalPlan::SetOp {
                left: _, right: _, ..
            } => {}
            PhysicalPlan::RecursiveUnion {
                anchor: _,
                recursive: _,
                ..
            } => {}
            PhysicalPlan::SeqScan { filter, .. }
            | PhysicalPlan::ZoneMapScan { filter, .. }
            | PhysicalPlan::ParallelSeqScan { filter, .. }
            | PhysicalPlan::ParallelZoneMapScan { filter, .. }
            | PhysicalPlan::CteScan { filter, .. }
            | PhysicalPlan::GenerateSeries { filter, .. }
            | PhysicalPlan::ValuesScan { filter, .. }
            | PhysicalPlan::JsonTableScan { filter, .. }
            | PhysicalPlan::SystemScan { filter, .. }
            | PhysicalPlan::IndexLookup { filter, .. }
            | PhysicalPlan::IndexMerge { filter, .. }
            | PhysicalPlan::IndexMultiLookup { filter, .. }
            | PhysicalPlan::IndexRangeScan { filter, .. }
            | PhysicalPlan::IndexFastScan { filter, .. }
            | PhysicalPlan::ParallelIndexFastScan { filter, .. }
            | PhysicalPlan::ParallelIndexRangeScan { filter, .. }
            | PhysicalPlan::SpatialScan { filter, .. } => {
                if !skip_scan_filters {
                    if let Some(e) = filter {
                        self.collect_expr(e);
                    }
                }
            }
            PhysicalPlan::VectorIndexScan {
                table,
                index,
                filter,
                order_by,
                ..
            } => {
                if !skip_scan_filters {
                    if let Some(filter) = filter {
                        self.collect_expr(filter);
                    }
                }
                for order in order_by {
                    self.collect_expr(&order.expr);
                }
                // The live heap vector is authoritative even when it is not
                // selected by an upper projection or an optimizer rewrite.
                self.collect_index_probe_columns(table, index);
            }
            // VectorBruteScan carries no residual filter expression (it is only
            // emitted for the no-WHERE shape) and its query vector is a constant,
            // so there are no column/CTE refs to collect.
            PhysicalPlan::VectorBruteScan { .. } => {}
            PhysicalPlan::FulltextScan {
                filter, columns, ..
            } => {
                if !skip_scan_filters {
                    if let Some(e) = filter {
                        self.collect_expr(e);
                    }
                }
                // BM25 (v33+) reads tf / doc_len / positions from the payload
                // that the inverted index carries in its second key part, so
                // the indexed columns are **not** required for scoring. They
                // are still needed if the *residual filter* / SELECT / ORDER
                // BY references them (already captured above via
                // `collect_expr(filter)`, and by upstream Projection /
                // Sort / Filter walks). The only surviving read-side use is
                // the phrase-mode positions-off fallback, which the caller
                // handles by re-fetching the row on demand from the FT
                // executor — not by forcing the columns to survive pruning
                // here. Suppress the forced-reference so plans like
                // `SELECT id ... WHERE MATCH(body) AGAINST('x')` can prune
                // `body` out of the row read (a big win when `body` is a
                // wide TEXT column).
                let _ = columns;
            }
            PhysicalPlan::IndexInLookup {
                filter,
                subquery,
                subquery_plan,
                ..
            } => {
                if !skip_scan_filters {
                    if let Some(e) = filter {
                        self.collect_expr(e);
                    }
                }
                // The outer table feeds rows whose `in_column` value comes
                // from the subquery — collect references the subquery
                // generates against outer tables, too.
                collect_select_columns_by_table(subquery, &self.tables, &mut self.tab_refs);
                self.collect_plan(subquery_plan, skip_scan_filters);
            }
            PhysicalPlan::HashJoin {
                left: _,
                right: _,
                left_keys,
                right_keys,
                residual,
                ..
            } => {
                for e in left_keys.iter().chain(right_keys.iter()) {
                    self.collect_expr(e);
                }
                if let Some(e) = residual {
                    self.collect_expr(e);
                }
            }
            PhysicalPlan::NestedLoopJoin {
                left: _,
                right: _,
                condition,
                ..
            } => {
                if let Some(e) = condition {
                    self.collect_expr(e);
                }
            }
            PhysicalPlan::AdaptiveJoin {
                left: _,
                right: _,
                candidates,
                ..
            } => {
                // Each candidate represents one possible join shape the
                // executor may pick at runtime. We don't know which one yet,
                // so be safe and collect column refs from all of them.
                for cand in candidates {
                    match cand {
                        AdaptiveJoinCandidate::Hash {
                            left_keys,
                            right_keys,
                            residual,
                            ..
                        } => {
                            for e in left_keys.iter().chain(right_keys.iter()) {
                                self.collect_expr(e);
                            }
                            if let Some(e) = residual {
                                self.collect_expr(e);
                            }
                        }
                        AdaptiveJoinCandidate::IndexNestedLoop {
                            right_table,
                            right_index,
                            condition,
                            ..
                        } => {
                            if let Some(e) = condition {
                                self.collect_expr(e);
                            }
                            self.collect_index_probe_columns(right_table, right_index);
                        }
                        AdaptiveJoinCandidate::NestedLoop { condition, .. } => {
                            if let Some(e) = condition {
                                self.collect_expr(e);
                            }
                        }
                    }
                }
            }
            PhysicalPlan::IndexNestedLoopJoin {
                left: _,
                right_table,
                right_index,
                condition,
                ..
            } => {
                if let Some(e) = condition {
                    self.collect_expr(e);
                }
                // INLJ probes the right table on the index's leading
                // columns. Even when the condition expression doesn't
                // textually mention them (e.g. fully covered by `condition`
                // already), the executor still needs them to evaluate the
                // probe — keep them.
                self.collect_index_probe_columns(right_table, right_index);
            }
            PhysicalPlan::HashAggregate {
                input: _,
                group_by,
                having,
                aggregates,
                ..
            }
            | PhysicalPlan::HashRollupAggregate {
                input: _,
                group_by,
                having,
                aggregates,
                ..
            }
            | PhysicalPlan::HashGroupingSetsAggregate {
                input: _,
                group_by,
                having,
                aggregates,
                ..
            }
            | PhysicalPlan::StreamingAggregate {
                input: _,
                group_by,
                having,
                aggregates,
                ..
            }
            | PhysicalPlan::StreamingRollupAggregate {
                input: _,
                group_by,
                having,
                aggregates,
                ..
            }
            | PhysicalPlan::StreamingGroupingSetsAggregate {
                input: _,
                group_by,
                having,
                aggregates,
                ..
            }
            | PhysicalPlan::ParallelHashAggregate {
                input: _,
                group_by,
                having,
                aggregates,
                ..
            }
            | PhysicalPlan::ParallelRollupAggregate {
                input: _,
                group_by,
                having,
                aggregates,
                ..
            }
            | PhysicalPlan::ParallelGroupingSetsAggregate {
                input: _,
                group_by,
                having,
                aggregates,
                ..
            }
            | PhysicalPlan::CoveringIndexAggregate {
                group_by,
                having,
                aggregates,
                ..
            }
            | PhysicalPlan::ParallelIndexAggregate {
                group_by,
                having,
                aggregates,
                ..
            } => {
                for e in group_by {
                    self.collect_expr(e);
                }
                if let Some(e) = having {
                    self.collect_expr(e);
                }
                for e in aggregates {
                    self.collect_expr(e);
                }
            }
            PhysicalPlan::IndexAggregate { .. } => {
                // Single-aggregate covering shape: every column it touches
                // already lives in the index, no heap projection involved.
            }
            PhysicalPlan::Sort {
                input: _, order_by, ..
            }
            | PhysicalPlan::IncrementalSort {
                input: _, order_by, ..
            }
            | PhysicalPlan::TopK {
                input: _, order_by, ..
            }
            | PhysicalPlan::TopWindow {
                input: _, order_by, ..
            }
            | PhysicalPlan::ParallelTopK {
                input: _, order_by, ..
            }
            | PhysicalPlan::ParallelSort {
                input: _, order_by, ..
            } => {
                for ob in order_by {
                    self.collect_expr(&ob.expr);
                }
            }
            PhysicalPlan::Limit { input: _, .. } | PhysicalPlan::Exchange { input: _, .. } => {}
            PhysicalPlan::Filter {
                input: _,
                predicate,
                ..
            }
            | PhysicalPlan::Apply {
                outer: _,
                predicate,
                ..
            } => {
                // The post-join WHERE predicate references columns that must not
                // be pruned away below it.
                self.collect_expr(predicate);
            }
            PhysicalPlan::Projection {
                input: _,
                projection,
                ..
            }
            | PhysicalPlan::StreamingDistinct {
                input: _,
                projection,
                ..
            } => {
                self.collect_projection(projection);
            }
            PhysicalPlan::Materialize { input: _, .. } => {}
            PhysicalPlan::WindowAgg {
                input: _, windows, ..
            } => {
                for w in windows {
                    for e in &w.args {
                        self.collect_expr(e);
                    }
                    for e in &w.partition_by {
                        self.collect_expr(e);
                    }
                    for ob in &w.order_by {
                        self.collect_expr(&ob.expr);
                    }
                }
            }
        }

        if !matches!(
            plan,
            PhysicalPlan::NullExtend { .. } | PhysicalPlan::IndexInLookup { .. }
        ) {
            for child in plan_children(plan) {
                self.collect_plan(child, skip_scan_filters);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
/// Set `output_columns_in_projection` on each scan node where the
/// scan filter references columns nothing else in the plan touches.
///
/// Precondition: `ColumnPruningRefs::apply` has already populated
/// `projected_columns` on every scan node — this pass works against
/// the already-fixed decode set, so the indices it emits are stable
/// positions inside `projected_columns`.
///
/// `tab_non_filter_refs[i]` holds the column names referenced by the
/// rest of the plan (i.e., everywhere EXCEPT this table's pushed-down
/// scan filter — see `ColumnPruningRefs::collect_plan`). If a
/// column appears in the scan's `projected_columns` but NOT in
/// `tab_non_filter_refs[i]`, then it's filter-only and the streaming
/// probe/scan worker can skip the per-row `values[pos].clone()` when
/// building the downstream `PlanRow`. For wide VARCHAR / TEXT columns
/// (e.g. `o_comment` ~50 B), that's the dominant cost on the hot
/// path; removing it saves the heap clone AND its eventual `drop`.
pub(super) fn apply_output_column_pruning(
    plan: &mut PhysicalPlan,
    tables: &[PlanTable],
    tab_non_filter_refs: &[HashSet<String>],
    tab_non_filter_wild: &[bool],
) {
    fn output_indices_in_projection(
        table: &PlanTable,
        projected_columns: &Option<Arc<Vec<usize>>>,
        non_filter_refs: &HashSet<String>,
        non_filter_wild: bool,
    ) -> Option<Arc<Vec<usize>>> {
        // Wildcard reference means "every column is consumed
        // downstream" — no pruning possible / needed.
        if non_filter_wild {
            return None;
        }
        // Without an explicit `projected_columns` the scan emits its
        // entire schema; we can't index a smaller output set.
        let projected = projected_columns.as_ref()?;
        let mut output: Vec<usize> = Vec::with_capacity(projected.len());
        for (pos, &schema_idx) in projected.iter().enumerate() {
            let column = match table.schema.columns.get(schema_idx) {
                Some(c) => &c.name,
                None => return None,
            };
            let normalized = normalize_expr_column(column);
            if non_filter_refs.contains(&normalized) {
                output.push(pos);
            }
        }
        // If every projected column is also non-filter-referenced
        // there's nothing to prune — keep `None` so the executor's
        // hot path uses its existing "iterate every projected column"
        // branch (avoids an extra `if let Some(...)` check per row).
        if output.len() == projected.len() {
            return None;
        }
        Some(Arc::new(output))
    }

    fn lookup_table_idx(tables: &[PlanTable], table: &PlanTable) -> Option<usize> {
        tables
            .iter()
            .position(|t| plan_table_matches(t, &table.schema.name, table.alias.as_deref()))
    }

    fn walk(
        plan: &mut PhysicalPlan,
        tables: &[PlanTable],
        non_filter_refs: &[HashSet<String>],
        non_filter_wild: &[bool],
    ) {
        match plan {
            PhysicalPlan::ParallelSeqScan {
                table,
                projected_columns,
                output_columns_in_projection,
                ..
            }
            | PhysicalPlan::ParallelZoneMapScan {
                table,
                projected_columns,
                output_columns_in_projection,
                ..
            } => {
                if output_columns_in_projection.is_some() {
                    return;
                }
                if let Some(idx) = lookup_table_idx(tables, table) {
                    *output_columns_in_projection = output_indices_in_projection(
                        table,
                        projected_columns,
                        &non_filter_refs[idx],
                        non_filter_wild[idx],
                    );
                }
            }
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::AdaptiveJoin { left, right, .. } => {
                walk(left, tables, non_filter_refs, non_filter_wild);
                walk(right, tables, non_filter_refs, non_filter_wild);
            }
            PhysicalPlan::IndexNestedLoopJoin { left, .. } => {
                walk(left, tables, non_filter_refs, non_filter_wild);
            }
            PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::HashRollupAggregate { input, .. }
            | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
            | PhysicalPlan::StreamingAggregate { input, .. }
            | PhysicalPlan::StreamingRollupAggregate { input, .. }
            | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
            | PhysicalPlan::ParallelHashAggregate { input, .. }
            | PhysicalPlan::ParallelRollupAggregate { input, .. }
            | PhysicalPlan::ParallelGroupingSetsAggregate { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::IncrementalSort { input, .. }
            | PhysicalPlan::TopK { input, .. }
            | PhysicalPlan::ParallelTopK { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::WindowAgg { input, .. }
            | PhysicalPlan::StreamingDistinct { input, .. }
            | PhysicalPlan::Exchange { input, .. }
            | PhysicalPlan::ParallelSort { input, .. } => {
                walk(input, tables, non_filter_refs, non_filter_wild);
            }
            _ => {}
        }
    }

    walk(plan, tables, tab_non_filter_refs, tab_non_filter_wild);
}

impl ColumnPruningRefs {
    fn apply(&self, plan: &mut PhysicalPlan) {
        let Self {
            tables,
            tab_refs,
            tab_wild,
            cte_targets,
            cte_refs,
            cte_wild,
            ..
        } = self;
        fn lookup_table_idx(
            tables: &[PlanTable],
            name: &str,
            alias: Option<&str>,
        ) -> Option<usize> {
            tables
                .iter()
                .position(|t| plan_table_matches(t, name, alias))
        }
        fn lookup_cte_idx(
            cte_targets: &[(String, Option<String>, Vec<crate::types::Column>)],
            name: &str,
            alias: Option<&str>,
        ) -> Option<usize> {
            cte_targets.iter().position(|(n, a, _)| {
                n.eq_ignore_ascii_case(name)
                    && match (a, alias) {
                        (Some(x), Some(y)) => x.eq_ignore_ascii_case(y),
                        (None, None) => true,
                        _ => false,
                    }
            })
        }
        let assign_table_proj = |projected_columns: &mut Option<Arc<Vec<usize>>>,
                                 table: &PlanTable| {
            let Some(idx) = lookup_table_idx(tables, &table.schema.name, table.alias.as_deref())
            else {
                return;
            };
            if tab_wild[idx] {
                *projected_columns = None;
                return;
            }
            if referenced_covers_visible_columns(table, &tab_refs[idx]) {
                *projected_columns = None;
                return;
            }
            let Some(indices) = indices_from_referenced(table, &tab_refs[idx]) else {
                return;
            };
            match projected_columns {
                Some(existing) => {
                    let mut merged: Vec<usize> = existing.iter().copied().collect();
                    for &idx in indices.iter() {
                        if !merged.contains(&idx) {
                            merged.push(idx);
                        }
                    }
                    merged.sort_unstable();
                    if projection_covers_visible_columns(table, &merged) {
                        *projected_columns = None;
                    } else if merged.as_slice() != existing.as_slice() {
                        *projected_columns = Some(Arc::new(merged));
                    }
                }
                None => {
                    *projected_columns = Some(indices);
                }
            }
        };
        match plan {
            PhysicalPlan::SeqScan {
                table,
                projected_columns,
                ..
            }
            | PhysicalPlan::ZoneMapScan {
                table,
                projected_columns,
                ..
            }
            | PhysicalPlan::IndexRangeScan {
                table,
                projected_columns,
                ..
            }
            | PhysicalPlan::IndexFastScan {
                table,
                projected_columns,
                ..
            }
            | PhysicalPlan::ParallelSeqScan {
                table,
                projected_columns,
                ..
            }
            | PhysicalPlan::ParallelZoneMapScan {
                table,
                projected_columns,
                ..
            }
            | PhysicalPlan::ParallelIndexFastScan {
                table,
                projected_columns,
                ..
            }
            | PhysicalPlan::ParallelIndexRangeScan {
                table,
                projected_columns,
                ..
            }
            | PhysicalPlan::IndexLookup {
                table,
                projected_columns,
                ..
            }
            | PhysicalPlan::IndexMultiLookup {
                table,
                projected_columns,
                ..
            }
            | PhysicalPlan::IndexInLookup {
                table,
                projected_columns,
                ..
            }
            | PhysicalPlan::VectorIndexScan {
                table,
                projected_columns,
                ..
            }
            | PhysicalPlan::FulltextScan {
                table,
                projected_columns,
                ..
            } => assign_table_proj(projected_columns, table),
            PhysicalPlan::CteScan {
                name,
                alias,
                columns,
                projected_columns,
                ..
            } => {
                if projected_columns.is_none() {
                    if let Some(idx) = lookup_cte_idx(cte_targets, name, alias.as_deref()) {
                        if !cte_wild[idx] {
                            if let Some(indices) =
                                indices_from_cte_referenced(columns, &cte_refs[idx])
                            {
                                *projected_columns = Some(indices);
                            }
                        }
                    }
                }
            }
            PhysicalPlan::SystemScan {
                name,
                relation,
                projected_columns,
                ..
            } => {
                if let Some(table) = tables
                    .iter()
                    .find(|table| plan_table_matches(table, name, Some(relation)))
                {
                    assign_table_proj(projected_columns, table);
                }
            }
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::AdaptiveJoin { left, right, .. } => {
                self.apply(left);
                self.apply(right);
            }
            PhysicalPlan::IndexNestedLoopJoin {
                left,
                right_table,
                right_projected_columns,
                ..
            } => {
                self.apply(left);
                assign_table_proj(right_projected_columns, right_table);
            }
            PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::HashRollupAggregate { input, .. }
            | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
            | PhysicalPlan::StreamingAggregate { input, .. }
            | PhysicalPlan::StreamingRollupAggregate { input, .. }
            | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::IncrementalSort { input, .. }
            | PhysicalPlan::TopK { input, .. }
            | PhysicalPlan::ParallelTopK { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::WindowAgg { input, .. }
            | PhysicalPlan::StreamingDistinct { input, .. }
            | PhysicalPlan::Exchange { input, .. }
            | PhysicalPlan::ParallelHashAggregate { input, .. }
            | PhysicalPlan::ParallelRollupAggregate { input, .. }
            | PhysicalPlan::ParallelGroupingSetsAggregate { input, .. }
            | PhysicalPlan::ParallelSort { input, .. } => self.apply(input),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cte_scan() -> PhysicalPlan {
        PhysicalPlan::CteScan {
            name: "source".into(),
            alias: Some("alias".into()),
            columns: ["id", "payload"]
                .into_iter()
                .map(|name| Column {
                    name: name.into(),
                    ..Column::default()
                })
                .collect(),
            filter: Some(Expr::Identifier("payload".into())),
            estimated_rows: 1.0,
            cost: crate::engine::cbo::Cost::ZERO,
            projected_columns: None,
        }
    }

    #[test]
    fn pruning_projection_preserves_cte_alias_shadowing() {
        for (projection, expected_wild, expected_refs) in [
            ("*", true, vec![]),
            ("ALIAS.*", true, vec![]),
            ("source.*", false, vec![]),
            ("alias.id", false, vec!["id"]),
            ("source.id", false, vec![]),
        ] {
            let crate::parser::Statement::Select(select) = crate::parser::parse_statement(
                &format!("SELECT {projection} FROM source AS alias"),
            )
            .unwrap() else {
                panic!("SELECT expected")
            };
            let mut from_ast = ColumnPruningRefs::new(&cte_scan(), false).unwrap();
            from_ast.collect_select(&select, true);
            let plan = PhysicalPlan::Projection {
                input: Box::new(cte_scan()),
                projection: select.projection,
                distinct: false,
                estimated_rows: 1.0,
                cost: crate::engine::cbo::Cost::ZERO,
            };
            let mut from_plan = ColumnPruningRefs::new(&plan, false).unwrap();
            from_plan.collect_plan(&plan, true);
            let expected: HashSet<String> = expected_refs.into_iter().map(str::to_owned).collect();
            for refs in [from_ast, from_plan] {
                assert_eq!(refs.cte_wild, [expected_wild], "{projection}");
                assert_eq!(refs.cte_refs, [expected.clone()], "{projection}");
            }
        }
    }

    #[test]
    fn pruning_excludes_only_scan_filters_and_keeps_post_scan_predicates() {
        let plan = PhysicalPlan::Filter {
            input: Box::new(cte_scan()),
            predicate: Expr::Identifier("id".into()),
            estimated_rows: 1.0,
            cost: crate::engine::cbo::Cost::ZERO,
        };
        // Targets belong to the scan scope; the collector must also account
        // for predicates in wrappers subsequently attached by the planner.
        for (skip_scan_filters, expected) in [(false, vec!["id", "payload"]), (true, vec!["id"])] {
            let mut refs = ColumnPruningRefs::new(&cte_scan(), false).unwrap();
            refs.collect_plan(&plan, skip_scan_filters);
            assert_eq!(
                refs.cte_refs[0],
                expected.into_iter().map(str::to_owned).collect()
            );
        }
        // Full decode pruning must keep the payload used by the scan filter.
        let mut refs = ColumnPruningRefs::new(&cte_scan(), false).unwrap();
        refs.collect_plan(&plan, false);
        let mut scan = cte_scan();
        refs.apply(&mut scan);
        let PhysicalPlan::CteScan {
            projected_columns, ..
        } = scan
        else {
            unreachable!()
        };
        assert!(projected_columns.is_none());
    }
}
