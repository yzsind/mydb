//! DPsub join enumeration (Phase 4).
//!
//! Replaces the legacy left-deep DP with a Selinger-style **DPsub**
//! search that enumerates every connected subset of the join graph and
//! every non-trivial bipartition of each subset. The result is the
//! optimal join tree for `n ≤ MAX_DP_TABLES` (default 12) under the
//! current cost model — including bushy plans (right child is a
//! multi-table sub-plan, not a single base relation).
//!
//! ## Why bushy
//!
//! Left-deep DP misses the optimum on classic shapes like:
//!
//! ```text
//!     ⋈        is sometimes worse than:        ⋈
//!    / \                                      / \
//!   ⋈   c                                    ⋈   ⋈
//!  / \                                       a b   c d
//! a   b
//! ```
//!
//! TPC-H Q5 / Q9 / Q21 are full of such shapes. Bushy doubles the
//! search space (every bipartition instead of "append next relation"). An
//! anytime multi-start greedy plan is therefore established first; DPsub may
//! improve it until its deterministic work allowance is exhausted.
//!
//! ## Algorithm
//!
//! ```text
//! for each base relation i:
//!     dp[1<<i] = leaf scan
//!
//! for size = 2 .. n:
//!     for each connected subset S of size `size`:
//!         for each non-empty proper subset L of S where (S\L) is connected
//!         and at least one join edge straddles (L, S\L):
//!             plan_l = dp[L].plan
//!             plan_r = dp[S\L].plan
//!             candidate = choose_join(plan_l, plan_r, predicates_between)
//!             if candidate.cost < dp[S].cost { update }
//!
//! return dp[(1<<n) - 1]
//! ```
//!
//! Each `dp[mask]` keeps a bounded Pareto frontier over physical properties
//! and cost dimensions, so an upstream operator can retain a useful ordering
//! or execution mode without allowing the search to grow without bound.
//!
//! ## Fallback
//!
//! `n > MAX_DP_TABLES`: return the same deterministic multi-start incumbent
//! used by timed-out DPsub. The source-order fallback remains reachable only
//! for islands too wide for the current `u32` relation mask.

use crate::engine::cbo::{CboContext, Cost, ExecutionMode, PhysicalProperties};
use crate::parser::{Expr, JoinType, QueryHint};
use crate::planner::{
    choose_join_with_hints, collect_physical_plan_tables, combine_conjuncts, estimated_rows,
    expr_can_resolve_in_table_set, expr_columns_resolved_across, expr_touches_table_set, plan_cost,
    PhysicalPlan, PlanTable,
};
use std::time::{Duration, Instant};

/// Maximum number of base relations the bushy DP searches exhaustively.
/// `2^N · ~N` candidates fit comfortably under the default planning
/// work allowance at N=12 on commodity hardware.
pub const MAX_DP_TABLES: usize = 12;

/// Translate the user-facing planning-time knob into a deterministic search
/// allowance.  A wall-clock cutoff made the physical plan depend on scheduler
/// timing: the same SQL could retain a different partially explored incumbent
/// under load.  Fixed work preserves bounded planning while making the result
/// a function only of the query, schema, statistics and CBO settings.
// One DP "operation" can build and cost several complete PhysicalPlan trees;
// it is not a cheap mask iteration.  The old 400 ops/ms conversion turned the
// default 50 ms allowance into 20,000 candidates and routinely spent more
// than a second on 9-12 relation islands.  Keep the deterministic work budget
// (and therefore plan stability), but calibrate it to the measured cost of the
// guarded unit of work.  The multi-start incumbent is complete before this
// allowance is consumed, so exhaustion still returns a valid, costed plan.
// One guarded iteration per millisecond leaves the default 50 ms setting a
// 50-operation deterministic allowance; the guard is deliberately coarse
// because one iteration may expand many frontier-cell pairs.
const DP_OPERATIONS_PER_BUDGET_MS: f64 = 1.0;
const MAX_DP_OPERATION_BUDGET: usize = 200_000;

fn configured_operation_budget(ctx: &CboContext) -> usize {
    (ctx.model.join_planning_budget_ms.max(f64::EPSILON) * DP_OPERATIONS_PER_BUDGET_MS)
        .round()
        .clamp(1.0, MAX_DP_OPERATION_BUDGET as f64) as usize
}

/// Deterministic operation allowance used in production.  `None` retains a
/// wall-clock guard for focused tests and callers that explicitly request it.
struct PlanningBudget {
    deadline: Instant,
    remaining_operations: Option<usize>,
}

impl PlanningBudget {
    fn new(deadline: Instant, remaining_operations: Option<usize>) -> Self {
        Self {
            deadline,
            remaining_operations,
        }
    }

    /// Consume one bounded search-loop operation. `false` means the caller
    /// must immediately return the already-complete incumbent.
    fn consume(&mut self) -> bool {
        if let Some(remaining) = self.remaining_operations.as_mut() {
            if *remaining == 0 {
                return false;
            }
            *remaining -= 1;
            true
        } else {
            Instant::now() < self.deadline
        }
    }
}

/// Compact representation of the join graph derived from a set of
/// scans plus a set of cross-cutting predicates (typically WHERE
/// conditions that aren't pinned to a single table).
pub struct JoinGraph {
    /// One entry per base relation: the leaf access plan as built by
    /// `choose_scan` upstream.
    pub scans: Vec<PhysicalPlan>,
    /// The set of base tables behind each scan (a single-table scan
    /// reports one entry; a CTE-with-join scan may report several).
    pub scan_tables: Vec<Vec<PlanTable>>,
    /// Boolean adjacency: `edges[i][j]` is true iff at least one
    /// predicate references columns from both relation `i` and `j`.
    pub edges: Vec<Vec<bool>>,
    /// All cross-relation predicates, in the order they were supplied.
    pub cross_preds: Vec<Expr>,
    /// Relations touched by each predicate. This makes the hot straddling
    /// test a pair of bit operations rather than a recursive AST walk.
    predicate_relation_masks: Vec<u32>,
    /// Precomputed table sets for every subset when the graph is small enough
    /// for DPsub.  Predicate placement used to rebuild these vectors for every
    /// cell pair, which made planning time dominated by Arc/String cloning on
    /// wide join islands.
    subset_tables: Option<Vec<Vec<PlanTable>>>,
    /// Connectivity is a pure property of a relation mask.  Cache it once for
    /// the DP-sized graph instead of walking the graph for every bipartition.
    connected_masks: Option<Vec<bool>>,
    /// `predicate_resolved_masks[p][s]` says every concrete column of
    /// predicate `p` resolves within subset `s`. Together with
    /// `predicate_relation_masks` this records the earliest legal placement
    /// stage for multi-table residuals.
    predicate_resolved_masks: Option<Vec<Vec<bool>>>,
}

impl JoinGraph {
    pub fn new(scans: Vec<PhysicalPlan>, cross_preds: Vec<Expr>) -> Self {
        let n = scans.len();
        let scan_tables: Vec<Vec<PlanTable>> =
            scans.iter().map(collect_physical_plan_tables).collect();
        let predicate_relation_masks: Vec<u32> = cross_preds
            .iter()
            .map(|predicate| {
                scan_tables
                    .iter()
                    .enumerate()
                    .fold(0u32, |mask, (idx, tables)| {
                        if idx < u32::BITS as usize && expr_touches_table_set(predicate, tables) {
                            mask | (1u32 << idx)
                        } else {
                            mask
                        }
                    })
            })
            .collect();
        let mut edges = vec![vec![false; n]; n];
        for predicate_mask in &predicate_relation_masks {
            for i in 0..n {
                for j in (i + 1)..n {
                    if i < u32::BITS as usize
                        && j < u32::BITS as usize
                        && (*predicate_mask & (1u32 << i)) != 0
                        && (*predicate_mask & (1u32 << j)) != 0
                    {
                        edges[i][j] = true;
                        edges[j][i] = true;
                    }
                }
            }
        }
        let subset_tables = if n <= MAX_DP_TABLES {
            let mut subsets = vec![Vec::new(); 1usize << n];
            for mask in 1usize..(1usize << n) {
                let bit = mask.trailing_zeros() as usize;
                let previous = mask & !(1usize << bit);
                subsets[mask] = subsets[previous].clone();
                subsets[mask].extend(scan_tables[bit].iter().cloned());
            }
            Some(subsets)
        } else {
            None
        };
        let predicate_resolved_masks = subset_tables.as_ref().map(|subsets| {
            cross_preds
                .iter()
                .map(|predicate| {
                    subsets
                        .iter()
                        .map(|tables| expr_columns_resolved_across(predicate, tables, &[]))
                        .collect()
                })
                .collect()
        });
        let mut graph = Self {
            scans,
            scan_tables,
            edges,
            cross_preds,
            predicate_relation_masks,
            subset_tables,
            connected_masks: None,
            predicate_resolved_masks,
        };
        if n <= MAX_DP_TABLES {
            let connected = (0..(1u32 << n))
                .map(|mask| graph.compute_connected(mask))
                .collect();
            graph.connected_masks = Some(connected);
        }
        graph
    }

    pub fn n(&self) -> usize {
        self.scans.len()
    }

    /// True if the bitmask represents a connected sub-graph under the
    /// adjacency. Disconnected subsets aren't considered as
    /// intermediate join targets — the only way they unify is via a
    /// Cartesian product, which we forbid in the DP and leave for a
    /// final fallback.
    pub fn is_connected(&self, mask: u32) -> bool {
        if let Some(cached) = self.connected_masks.as_ref() {
            return cached.get(mask as usize).copied().unwrap_or(false);
        }
        self.compute_connected(mask)
    }

    fn compute_connected(&self, mask: u32) -> bool {
        if mask == 0 {
            return false;
        }
        let n = self.n();
        let lowest_bit = mask.trailing_zeros() as usize;
        if lowest_bit >= n {
            return false;
        }
        let mut visited: u32 = 1 << lowest_bit;
        let mut frontier: u32 = visited;
        while frontier != 0 {
            let bit = frontier.trailing_zeros() as usize;
            frontier &= !(1u32 << bit);
            for j in 0..n {
                if (mask & (1 << j)) != 0 && (visited & (1 << j)) == 0 && self.edges[bit][j] {
                    visited |= 1 << j;
                    frontier |= 1 << j;
                }
            }
        }
        visited == mask
    }

    fn tables_for_mask(&self, mask: u32) -> Vec<PlanTable> {
        if let Some(subsets) = self.subset_tables.as_ref() {
            if let Some(tables) = subsets.get(mask as usize) {
                return tables.clone();
            }
        }
        (0..self.n())
            .filter(|i| (mask & (1 << i)) != 0)
            .flat_map(|i| self.scan_tables[i].iter().cloned())
            .collect()
    }

    /// True if there exists at least one predicate touching both
    /// `left_mask` and `right_mask` (both expected to be sub-graphs of
    /// the same overall set, with `left & right == 0`).
    pub fn has_edge_between(&self, left_mask: u32, right_mask: u32) -> bool {
        let n = self.n();
        for i in 0..n {
            if (left_mask & (1 << i)) == 0 {
                continue;
            }
            for j in 0..n {
                if (right_mask & (1 << j)) != 0 && self.edges[i][j] {
                    return true;
                }
            }
        }
        false
    }

    #[cfg(test)]
    /// Returns the predicates whose left/right tables straddle the
    /// (`left_mask`, `right_mask`) bipartition — these are the join
    /// conditions of the candidate join. Predicates that only touch
    /// one side (filters) or neither side (won't fire yet) are
    /// returned in `still_pending` for the caller to keep walking.
    pub fn straddling_predicates(
        &self,
        left_mask: u32,
        right_mask: u32,
        candidates: &[Expr],
    ) -> (Vec<Expr>, Vec<Expr>) {
        let left_tables: Vec<PlanTable> = (0..self.n())
            .filter(|i| (left_mask & (1 << i)) != 0)
            .flat_map(|i| self.scan_tables[i].clone())
            .collect();
        let right_tables: Vec<PlanTable> = (0..self.n())
            .filter(|i| (right_mask & (1 << i)) != 0)
            .flat_map(|i| self.scan_tables[i].clone())
            .collect();
        let mut joinable = Vec::new();
        let mut still = Vec::new();
        for pred in candidates {
            // A predicate is a join condition for this bipartition only if it
            // touches both sides AND every column it references is in scope
            // (left ∪ right). Without the second check a predicate spanning a
            // *third* table not yet joined — e.g. TPC-DS q13's
            // `(... cd ... hd ... ss ...) OR ...` evaluated at the
            // store_sales⋈household_demographics join, before customer_
            // demographics is present — is wrongly pinned here and fails at
            // runtime with `1054 unknown column`. Keep it pending until the
            // join that brings all its tables into scope. (The greedy fallback
            // path already applies both checks.)
            if expr_can_resolve_in_table_set(pred, &left_tables, &right_tables)
                && expr_columns_resolved_across(pred, &left_tables, &right_tables)
            {
                joinable.push(pred.clone());
            } else {
                still.push(pred.clone());
            }
        }
        (joinable, still)
    }

    /// ID-based counterpart used by DPsub and the anytime incumbent builder.
    /// Expressions live once in `cross_preds`; only predicates that actually
    /// become join conditions are cloned into a physical join node.
    fn straddling_predicate_ids(
        &self,
        left_mask: u32,
        right_mask: u32,
        candidates: &[usize],
    ) -> (Vec<usize>, Vec<usize>) {
        let mut joinable = Vec::new();
        let mut still = Vec::new();
        for &id in candidates {
            let Some(pred) = self.cross_preds.get(id) else {
                continue;
            };
            let cached_joinable = self
                .predicate_relation_masks
                .get(id)
                .zip(self.predicate_resolved_masks.as_ref())
                .and_then(|(&predicate_mask, resolved)| {
                    let combined = left_mask | right_mask;
                    resolved.get(id).and_then(|by_subset| {
                        by_subset.get(combined as usize).map(|&all_resolved| {
                            (predicate_mask & left_mask) != 0
                                && (predicate_mask & right_mask) != 0
                                && all_resolved
                        })
                    })
                });
            #[cfg(debug_assertions)]
            if let Some(cached) = cached_joinable {
                let left_tables = self.tables_for_mask(left_mask);
                let right_tables = self.tables_for_mask(right_mask);
                let legacy = expr_can_resolve_in_table_set(pred, &left_tables, &right_tables)
                    && expr_columns_resolved_across(pred, &left_tables, &right_tables);
                debug_assert_eq!(cached, legacy, "cached predicate placement diverged");
            }
            let can_join = cached_joinable.unwrap_or_else(|| {
                let left_tables = self.tables_for_mask(left_mask);
                let right_tables = self.tables_for_mask(right_mask);
                expr_can_resolve_in_table_set(pred, &left_tables, &right_tables)
                    && expr_columns_resolved_across(pred, &left_tables, &right_tables)
            });
            if can_join {
                joinable.push(id);
            } else {
                still.push(id);
            }
        }
        (joinable, still)
    }

    fn combine_predicate_ids(&self, ids: &[usize]) -> Option<Expr> {
        combine_conjuncts(
            ids.iter()
                .filter_map(|&id| self.cross_preds.get(id).cloned())
                .collect(),
        )
    }
}

/// One DP cell: the best plan found for a given subset, plus the
/// cumulative scalar cost and the predicates that haven't been
/// pinned to a join yet (they may apply higher up the tree).
#[derive(Clone)]
struct DpCell {
    plan: PhysicalPlan,
    cost: Cost,
    rows: f64,
    remaining: Vec<usize>,
    properties: PhysicalProperties,
    max_intermediate_rows: f64,
}

#[derive(Clone)]
struct CompletePlan {
    plan: PhysicalPlan,
    remaining: Vec<usize>,
    cost: Cost,
    max_intermediate_rows: f64,
    signature: Vec<String>,
    source: &'static str,
}

/// Keep the DP frontier intentionally tiny. This is enough to preserve
/// the two path properties that matter for follow-up joins (cheap total
/// work and small outer cardinality) without turning DPsub into a broad
/// Volcano-style memo.
const MAX_DP_CELL_ALTERNATIVES: usize = 8;

/// Bushy DPsub. Returns the plan covering all `n` base relations and
/// the predicates that *still* haven't found a home (they become a
/// post-join filter at the top of the tree).
pub fn enumerate_inner_joins(
    ctx: &CboContext,
    graph: &JoinGraph,
    query_hints: &[QueryHint],
) -> Option<(PhysicalPlan, Vec<Expr>)> {
    enumerate_inner_joins_with_operation_budget(
        ctx,
        graph,
        query_hints,
        Some(configured_operation_budget(ctx)),
    )
}

fn enumerate_inner_joins_with_operation_budget(
    ctx: &CboContext,
    graph: &JoinGraph,
    query_hints: &[QueryHint],
    operation_budget: Option<usize>,
) -> Option<(PhysicalPlan, Vec<Expr>)> {
    let n = graph.n();
    if n < 2 {
        crate::engine::cbo::trace::event(
            "join_dp_skipped",
            serde_json::json!({"relations": n, "reason": "too_few_relations"}),
        );
        return None;
    }

    let started = Instant::now();
    let budget =
        Duration::from_secs_f64((ctx.model.join_planning_budget_ms / 1_000.0).max(f64::EPSILON));
    let deadline = started + budget;
    let mut planning_budget = PlanningBudget::new(deadline, operation_budget);
    // Always establish a complete, connectivity-aware plan before entering
    // exponential DPsub.  If the exact search times out, returning this
    // incumbent is dramatically safer than discarding all search work and
    // restarting from the smallest base relation.
    let incumbent = multi_start_greedy(ctx, graph, query_hints)?;
    if n > MAX_DP_TABLES {
        crate::engine::cbo::trace::event(
            "join_dp_skipped",
            serde_json::json!({
                "relations": n,
                "maximum": MAX_DP_TABLES,
                "reason": "above_dp_cap",
                "selected": "multi_start_incumbent",
                "incumbent_source": incumbent.source,
                "incumbent_cost": incumbent.cost.total,
                "incumbent_max_intermediate_rows": incumbent.max_intermediate_rows,
            }),
        );
        return Some(complete_plan_result(graph, incumbent));
    }
    if !planning_budget.consume() {
        trace_timeout(graph, ctx, started, 1, 0, "incumbent", &incumbent);
        return Some(complete_plan_result(graph, incumbent));
    }

    let full: u32 = (1 << n) - 1;
    let mut dp: Vec<Vec<DpCell>> = vec![Vec::new(); (full + 1) as usize];
    let mut candidate_count = 0u64;

    // Base case: single-relation cells.
    for i in 0..n {
        let mask = 1u32 << i;
        let plan = graph.scans[i].clone();
        let cost = plan_cost(&plan);
        dp[mask as usize].push(DpCell {
            properties: physical_properties(&plan),
            plan,
            cost,
            rows: estimated_rows(&graph.scans[i]),
            remaining: (0..graph.cross_preds.len()).collect(),
            // Base-relation cardinality is fixed by the query graph. Track
            // only join outputs so the tie-breaker measures plan quality.
            max_intermediate_rows: 0.0,
        });
    }

    // DPsub: for size 2..=n, enumerate every connected subset and
    // every non-trivial bipartition.
    for size in 2..=n {
        // Walk every mask of the right popcount.
        let mut mask: u32 = (1 << size) - 1;
        loop {
            if !planning_budget.consume() {
                trace_timeout(
                    graph,
                    ctx,
                    started,
                    size,
                    candidate_count,
                    "outer_deadline",
                    &incumbent,
                );
                return Some(complete_plan_result(graph, incumbent));
            }
            if mask <= full {
                if graph.is_connected(mask) {
                    if !enumerate_bipartitions(
                        ctx,
                        graph,
                        mask,
                        query_hints,
                        &mut dp,
                        &mut planning_budget,
                        &mut candidate_count,
                    ) {
                        trace_timeout(
                            graph,
                            ctx,
                            started,
                            size,
                            candidate_count,
                            "bipartition_deadline",
                            &incumbent,
                        );
                        return Some(complete_plan_result(graph, incumbent));
                    }
                }
            }
            // Gosper's hack: next mask with the same popcount.
            if mask >= full {
                break;
            }
            let c = mask & mask.wrapping_neg();
            let r = mask.wrapping_add(c);
            let next = (((r ^ mask) >> 2) / c) | r;
            if next == 0 || next > full {
                break;
            }
            mask = next;
        }
    }

    let cell = dp[full as usize]
        .iter()
        .min_by(|a, b| dp_cell_cmp(a, b))?
        .clone();
    let exact = CompletePlan {
        plan: cell.plan,
        remaining: cell.remaining,
        cost: cell.cost,
        max_intermediate_rows: cell.max_intermediate_rows,
        signature: Vec::new(),
        source: "complete_dp",
    };
    let selected = if complete_plan_cmp(&exact, &incumbent).is_lt() {
        exact
    } else {
        incumbent
    };
    crate::engine::cbo::trace::event(
        "join_enumeration_result",
        serde_json::json!({
            "relations": n,
            "elapsed_ms": started.elapsed().as_secs_f64() * 1_000.0,
            "budget_ms": ctx.model.join_planning_budget_ms,
            "operation_budget": operation_budget,
            "candidate_count": candidate_count,
            "selected": selected.source,
            "selected_cost": selected.cost.total,
            "selected_max_intermediate_rows": selected.max_intermediate_rows,
        }),
    );
    Some(complete_plan_result(graph, selected))
}

fn complete_plan_result(graph: &JoinGraph, plan: CompletePlan) -> (PhysicalPlan, Vec<Expr>) {
    let remaining = plan
        .remaining
        .into_iter()
        .filter_map(|id| graph.cross_preds.get(id).cloned())
        .collect();
    (plan.plan, remaining)
}

fn complete_plan_cmp(a: &CompletePlan, b: &CompletePlan) -> std::cmp::Ordering {
    near_cost_streaming_cmp(
        a.cost.total,
        b.cost.total,
        physical_properties(&a.plan).streaming_join_depth,
        physical_properties(&b.plan).streaming_join_depth,
    )
    .then_with(|| a.max_intermediate_rows.total_cmp(&b.max_intermediate_rows))
    .then_with(|| a.cost.startup.total_cmp(&b.cost.startup))
    .then_with(|| a.signature.cmp(&b.signature))
}

fn trace_timeout(
    graph: &JoinGraph,
    ctx: &CboContext,
    started: Instant,
    subset_size: usize,
    candidate_count: u64,
    deadline_path: &'static str,
    incumbent: &CompletePlan,
) {
    crate::engine::cbo::trace::event(
        "join_planning_timeout",
        serde_json::json!({
            "relations": graph.n(),
            "budget_ms": ctx.model.join_planning_budget_ms,
            "elapsed_ms": started.elapsed().as_secs_f64() * 1_000.0,
            "completed_subset_size": subset_size.saturating_sub(1),
            "deadline_path": deadline_path,
            "candidate_count": candidate_count,
            "selected": "multi_start_incumbent",
            "incumbent_source": incumbent.source,
            "incumbent_cost": incumbent.cost.total,
            "incumbent_max_intermediate_rows": incumbent.max_intermediate_rows,
        }),
    );
}

/// Build a bounded, complete incumbent from every promising starting
/// relation.  The walk is connectivity-aware and uses the same physical join
/// chooser as DPsub, so it honours indexes, residuals, CBO constants, and join
/// hints.  Seed order is derived from scan estimates and stable table names,
/// never FROM ordinal, making a timeout insensitive to SQL source order.
fn multi_start_greedy(
    ctx: &CboContext,
    graph: &JoinGraph,
    query_hints: &[QueryHint],
) -> Option<CompletePlan> {
    let n = graph.n();
    if n == 0 || n >= u32::BITS as usize {
        return None;
    }
    let mut seeds: Vec<usize> = (0..n).collect();
    seeds.sort_by(|&a, &b| {
        estimated_rows(&graph.scans[a])
            .total_cmp(&estimated_rows(&graph.scans[b]))
            .then_with(|| stable_scan_name(graph, a).cmp(&stable_scan_name(graph, b)))
    });

    let mut best: Option<CompletePlan> = None;
    for seed in seeds {
        let Some(complete) = complete_greedy_walk(
            ctx,
            graph,
            query_hints,
            1u32 << seed,
            graph.scans[seed].clone(),
            (0..graph.cross_preds.len()).collect(),
            0.0,
            vec![stable_scan_name(graph, seed)],
            "relation_seed",
        ) else {
            break;
        };
        if best
            .as_ref()
            .is_none_or(|current| complete_plan_cmp(&complete, current).is_lt())
        {
            best = Some(complete);
        }
    }

    // A single greedy step can be a local trap: a tiny dimension-to-dimension
    // join may look better than attaching the filtered dimension directly to
    // a fact, even though the latter unlocks selective probes for the rest of
    // the graph. Seed the same greedy walk from every directed predicate edge
    // to cover that general case without paying for lookahead at every step.
    let all_predicates: Vec<usize> = (0..graph.cross_preds.len()).collect();
    let mut pair_seeds = Vec::new();
    for left_idx in 0..n {
        for right_idx in 0..n {
            if left_idx == right_idx {
                continue;
            }
            let (joinable, still) = graph.straddling_predicate_ids(
                1u32 << left_idx,
                1u32 << right_idx,
                &all_predicates,
            );
            if joinable.is_empty() {
                continue;
            }
            pair_seeds.push((
                stable_scan_name(graph, left_idx),
                stable_scan_name(graph, right_idx),
                left_idx,
                right_idx,
                joinable,
                still,
            ));
        }
    }
    pair_seeds.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    for (left_name, right_name, left_idx, right_idx, joinable, still) in pair_seeds {
        let head = choose_join_with_hints(
            ctx,
            graph.scans[left_idx].clone(),
            graph.scans[right_idx].clone(),
            JoinType::Inner,
            graph.combine_predicate_ids(&joinable),
            query_hints,
        );
        let rows = estimated_rows(&head);
        let Some(complete) = complete_greedy_walk(
            ctx,
            graph,
            query_hints,
            (1u32 << left_idx) | (1u32 << right_idx),
            head,
            still,
            rows,
            vec![left_name, right_name],
            "edge_seed",
        ) else {
            break;
        };
        if best
            .as_ref()
            .is_none_or(|current| complete_plan_cmp(&complete, current).is_lt())
        {
            best = Some(complete);
        }
    }
    best
}

#[allow(clippy::too_many_arguments)]
fn complete_greedy_walk(
    ctx: &CboContext,
    graph: &JoinGraph,
    query_hints: &[QueryHint],
    mut placed: u32,
    mut head: PhysicalPlan,
    mut remaining: Vec<usize>,
    mut max_intermediate_rows: f64,
    mut signature: Vec<String>,
    source: &'static str,
) -> Option<CompletePlan> {
    let full = (1u32 << graph.n()) - 1;
    while placed != full {
        let mut connected_best: Option<(usize, PhysicalPlan, Vec<usize>, f64, Cost, String)> = None;
        let mut disconnected_best: Option<(usize, PhysicalPlan, Vec<usize>, f64, Cost, String)> =
            None;
        for right_idx in 0..graph.n() {
            if (placed & (1u32 << right_idx)) != 0 {
                continue;
            }
            let right_mask = 1u32 << right_idx;
            let (joinable, still) = graph.straddling_predicate_ids(placed, right_mask, &remaining);
            let candidate = choose_join_with_hints(
                ctx,
                head.clone(),
                graph.scans[right_idx].clone(),
                JoinType::Inner,
                graph.combine_predicate_ids(&joinable),
                query_hints,
            );
            let rows = estimated_rows(&candidate);
            let cost = plan_cost(&candidate);
            let stable_name = stable_scan_name(graph, right_idx);
            let entry = (right_idx, candidate, still, rows, cost, stable_name);
            let slot = if joinable.is_empty() {
                &mut disconnected_best
            } else {
                &mut connected_best
            };
            if slot
                .as_ref()
                .is_none_or(|current| greedy_step_cmp(&entry, current).is_lt())
            {
                *slot = Some(entry);
            }
        }
        let (right_idx, candidate, still, rows, _cost, name) =
            connected_best.or(disconnected_best)?;
        placed |= 1u32 << right_idx;
        remaining = still;
        max_intermediate_rows = max_intermediate_rows.max(rows);
        signature.push(name);
        head = candidate;
    }
    Some(CompletePlan {
        cost: plan_cost(&head),
        plan: head,
        remaining,
        max_intermediate_rows,
        signature,
        source,
    })
}

fn greedy_step_cmp(
    a: &(usize, PhysicalPlan, Vec<usize>, f64, Cost, String),
    b: &(usize, PhysicalPlan, Vec<usize>, f64, Cost, String),
) -> std::cmp::Ordering {
    a.3.total_cmp(&b.3)
        .then_with(|| a.4.total.total_cmp(&b.4.total))
        .then_with(|| a.4.startup.total_cmp(&b.4.startup))
        .then_with(|| a.5.cmp(&b.5))
}

fn stable_scan_name(graph: &JoinGraph, idx: usize) -> String {
    let mut names: Vec<String> = graph.scan_tables[idx]
        .iter()
        .map(|table| {
            format!(
                "{}.{}.{}",
                table.schema.database,
                table.schema.name,
                table.alias.as_deref().unwrap_or("")
            )
        })
        .collect();
    names.sort();
    names.join("+")
}

/// Deterministic total ordering: cost first, then cardinality. Physical
/// properties must be represented explicitly in the memo rather than by
/// benchmark-shaped operator preferences.
fn dp_cell_cmp(a: &DpCell, b: &DpCell) -> std::cmp::Ordering {
    near_cost_streaming_cmp(
        a.cost.weighted_total(),
        b.cost.weighted_total(),
        a.properties.streaming_join_depth,
        b.properties.streaming_join_depth,
    )
    .then_with(|| a.cost.startup.total_cmp(&b.cost.startup))
    .then_with(|| a.rows.total_cmp(&b.rows))
}

/// A streaming hash-join spine is a real physical property: the executor
/// overlaps its levels and lets an INLJ tail feed chunks directly, while a
/// bushy alternative with the same joins can force that tail through a
/// single consumer. Cost formulas are not precise enough to distinguish
/// nearby alternatives precisely. Fixed logarithmic buckets (linear below
/// cost one) prefer the longer pipeline within a bucket. Unlike a pairwise
/// tolerance this defines a transitive total order required by sort_by.
fn near_cost_streaming_cmp(
    left_cost: f64,
    right_cost: f64,
    left_depth: u8,
    right_depth: u8,
) -> std::cmp::Ordering {
    let bucket = |cost: f64| {
        if cost < 1.0 {
            (cost * 100.0).floor()
        } else {
            100.0 + (cost.ln() / 1.01_f64.ln()).floor()
        }
    };
    bucket(left_cost)
        .total_cmp(&bucket(right_cost))
        .then_with(|| right_depth.cmp(&left_depth))
        .then_with(|| left_cost.total_cmp(&right_cost))
}

fn enumerate_bipartitions(
    ctx: &CboContext,
    graph: &JoinGraph,
    mask: u32,
    query_hints: &[QueryHint],
    dp: &mut [Vec<DpCell>],
    planning_budget: &mut PlanningBudget,
    candidate_count: &mut u64,
) -> bool {
    // Iterate over every non-empty proper subset of `mask`.
    let mut sub: u32 = (mask - 1) & mask;
    while sub != 0 {
        if !planning_budget.consume() {
            return false;
        }
        // Both halves must be connected and joined by at least one
        // predicate to avoid generating Cartesian products inside DP.
        let other = mask ^ sub;
        // Skip the symmetric duplicate: we'll see (a, b) and (b, a)
        // for the same pair. Process only when `sub < other` so we
        // don't recompute the join in both orders. choose_join is
        // not symmetric (it picks build sides + algorithms based on
        // the (left, right) order), so we still try BOTH directions.
        if sub < other
            && graph.is_connected(sub)
            && graph.is_connected(other)
            && graph.has_edge_between(sub, other)
        {
            *candidate_count += try_join(ctx, graph, sub, other, query_hints, dp) as u64;
            // Try the reverse so choose_join can pick the better
            // build/probe assignment for asymmetric algorithms.
            if sub != other {
                *candidate_count += try_join(ctx, graph, other, sub, query_hints, dp) as u64;
            }
        }
        sub = (sub - 1) & mask;
    }
    true
}

fn try_join(
    ctx: &CboContext,
    graph: &JoinGraph,
    left_mask: u32,
    right_mask: u32,
    query_hints: &[QueryHint],
    dp: &mut [Vec<DpCell>],
) -> usize {
    let combined = left_mask | right_mask;
    if dp[left_mask as usize].is_empty() {
        return 0;
    }
    if dp[right_mask as usize].is_empty() {
        return 0;
    }

    // Build candidates in a local vector while the two source cells are
    // immutably borrowed, then update the destination slot.  The previous
    // implementation cloned both complete cell vectors (and every recursive
    // PhysicalPlan they contained) for every bipartition.
    let mut candidates_out = Vec::new();
    for left_cell in &dp[left_mask as usize] {
        for right_cell in &dp[right_mask as usize] {
            // The candidate predicate pool is the intersection of what's
            // still pending on each side. A predicate may have been pinned
            // already in a deeper join; only those that *both* DP cells
            // still consider pending qualify for this join.
            let candidates: Vec<usize> = left_cell
                .remaining
                .iter()
                .filter(|id| right_cell.remaining.contains(id))
                .copied()
                .collect();
            let (joinable, still) =
                graph.straddling_predicate_ids(left_mask, right_mask, &candidates);
            if joinable.is_empty() {
                continue;
            }
            let cond = graph.combine_predicate_ids(&joinable);
            let new_plan = choose_join_with_hints(
                ctx,
                left_cell.plan.clone(),
                right_cell.plan.clone(),
                JoinType::Inner,
                cond,
                query_hints,
            );
            // `plan_cost_total` already covers both subtrees because every
            // join operator's cost formula sums `outer_cost.total` (and, for
            // hash join, `inner_cost.total`) into its own total. Adding
            // `left_cell.cost` on top double-counts the outer subtree and
            // biases DP toward whichever side currently sits to the left.
            let new_cost = plan_cost(&new_plan);
            if !new_cost.is_valid() {
                crate::engine::cbo::trace::event(
                    "join_candidate_rejected",
                    serde_json::json!({
                        "reason": "invalid_cost",
                        "startup": new_cost.startup,
                        "total": new_cost.total,
                    }),
                );
                continue;
            }
            let rows = estimated_rows(&new_plan);
            candidates_out.push(DpCell {
                rows,
                properties: physical_properties(&new_plan),
                plan: new_plan,
                cost: new_cost,
                remaining: still,
                max_intermediate_rows: left_cell
                    .max_intermediate_rows
                    .max(right_cell.max_intermediate_rows)
                    .max(rows),
            });
        }
    }
    let count = candidates_out.len();
    for candidate in candidates_out {
        insert_frontier_cell(&mut dp[combined as usize], candidate);
    }
    count
}

fn insert_frontier_cell(slot: &mut Vec<DpCell>, candidate: DpCell) {
    if slot
        .iter()
        .any(|existing| cell_dominates(existing, &candidate))
    {
        return;
    }
    slot.retain(|existing| !cell_dominates(&candidate, existing));
    slot.push(candidate);
    prune_frontier(slot);
}

fn cell_dominates(left: &DpCell, right: &DpCell) -> bool {
    if left.remaining != right.remaining || left.properties != right.properties {
        return false;
    }
    if !(left.rows <= right.rows
        && left.cost.startup <= right.cost.startup
        && left.cost.total <= right.cost.total
        && left.cost.mem_bytes <= right.cost.mem_bytes
        && left.cost.spill_bytes <= right.cost.spill_bytes)
    {
        return false;
    }
    let strictly_better = left.rows < right.rows
        || left.cost.startup < right.cost.startup
        || left.cost.total < right.cost.total
        || left.cost.mem_bytes < right.cost.mem_bytes
        || left.cost.spill_bytes < right.cost.spill_bytes;
    if !strictly_better {
        return false;
    }
    true
}

fn prune_frontier(slot: &mut Vec<DpCell>) {
    if slot.len() <= MAX_DP_CELL_ALTERNATIVES {
        return;
    }

    let cheapest_cost = slot
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| dp_cell_cmp(a, b))
        .map(|(idx, _)| idx);
    let fewest_rows = slot
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| {
            a.rows
                .total_cmp(&b.rows)
                .then_with(|| a.cost.cmp_total(&b.cost))
        })
        .map(|(idx, _)| idx);
    let mut keep = Vec::new();
    // Preserve the cheapest representative of every physical-property
    // class before filling the remaining bounded frontier by scalar cost.
    for idx in 0..slot.len() {
        let property_already_kept = keep
            .iter()
            .any(|kept: &usize| slot[*kept].properties == slot[idx].properties);
        if property_already_kept {
            continue;
        }
        let best = slot
            .iter()
            .enumerate()
            .filter(|(_, cell)| cell.properties == slot[idx].properties)
            .min_by(|(_, a), (_, b)| dp_cell_cmp(a, b))
            .map(|(candidate, _)| candidate)
            .unwrap_or(idx);
        keep.push(best);
        if keep.len() >= MAX_DP_CELL_ALTERNATIVES {
            break;
        }
    }
    for idx in [cheapest_cost, fewest_rows].into_iter().flatten() {
        if !keep.contains(&idx) {
            keep.push(idx);
        }
    }

    let mut by_cost: Vec<usize> = (0..slot.len()).collect();
    by_cost.sort_by(|&a, &b| dp_cell_cmp(&slot[a], &slot[b]));
    for idx in by_cost {
        if keep.len() >= MAX_DP_CELL_ALTERNATIVES {
            break;
        }
        if !keep.contains(&idx) {
            keep.push(idx);
        }
    }

    let old = std::mem::take(slot);
    *slot = keep.into_iter().map(|idx| old[idx].clone()).collect();
}

fn physical_properties(plan: &PhysicalPlan) -> PhysicalProperties {
    let mut properties = PhysicalProperties {
        parallel_degree: 1,
        rewindable: false,
        ..PhysicalProperties::default()
    };
    match plan {
        PhysicalPlan::IndexLookup { index, .. }
        | PhysicalPlan::IndexMultiLookup { index, .. }
        | PhysicalPlan::IndexFastScan { index, .. }
        | PhysicalPlan::IndexInLookup { index, .. } => {
            properties.ordering = index.columns.clone();
        }
        PhysicalPlan::IndexRangeScan { index, reverse, .. } => {
            properties.ordering = index
                .columns
                .iter()
                .map(|column| {
                    if *reverse {
                        format!("-{column}")
                    } else {
                        column.clone()
                    }
                })
                .collect();
        }
        PhysicalPlan::NestedLoopJoin { left, .. }
        | PhysicalPlan::IndexNestedLoopJoin { left, .. } => {
            properties = physical_properties(left);
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            build_side,
            ..
        } => {
            let probe = match build_side {
                crate::planner::BuildSide::Right => left,
                crate::planner::BuildSide::Left => right,
            };
            properties.streaming_join_depth =
                1u8.saturating_add(physical_properties(probe).streaming_join_depth);
        }
        PhysicalPlan::Exchange {
            input,
            degree,
            output_order,
            merge_keys,
            ..
        } => {
            properties = physical_properties(input);
            properties.parallel_degree = *degree;
            if matches!(output_order, crate::planner::ExchangeOrder::Sorted) {
                properties.ordering = merge_keys
                    .iter()
                    .map(|key| {
                        let mut expr = key.expr.clone();
                        crate::parser::canonicalize_expr_for_plan_cache(&mut expr);
                        format!("{expr:?}:{}", key.asc)
                    })
                    .collect();
            } else {
                properties.ordering.clear();
            }
        }
        PhysicalPlan::ParallelSeqScan { degree, .. }
        | PhysicalPlan::ParallelZoneMapScan { degree, .. }
        | PhysicalPlan::ParallelIndexFastScan { degree, .. }
        | PhysicalPlan::ParallelIndexRangeScan { degree, .. } => {
            properties.parallel_degree = *degree;
        }
        PhysicalPlan::VectorIndexScan { .. } | PhysicalPlan::VectorBruteScan { .. } => {
            properties.execution_mode = ExecutionMode::Vectorized;
        }
        _ => {}
    }
    properties
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::cbo::CboContext;
    use crate::parser::BinaryOp;

    fn empty_ctx() -> CboContext {
        CboContext::defaults_for_test()
    }

    #[test]
    fn near_cost_plans_prefer_the_longer_streaming_join_spine() {
        assert_eq!(
            near_cost_streaming_cmp(100.0, 99.9, 3, 1),
            std::cmp::Ordering::Less,
            "inside the same fixed one-percent cost bucket, the deeper executable pipeline wins"
        );
        assert_eq!(
            near_cost_streaming_cmp(102.0, 100.0, 3, 1),
            std::cmp::Ordering::Greater,
            "outside the uncertainty band, scalar cost remains authoritative"
        );
    }

    #[test]
    fn planner_audit_cost_order_is_transitive() {
        let candidates = [
            (0.1, 4),
            (0.105, 2),
            (0.9, 1),
            (99.9, 1),
            (100.0, 3),
            (100.8, 2),
            (101.0, 4),
            (102.0, 0),
        ];
        for &(a, ad) in &candidates {
            for &(b, bd) in &candidates {
                for &(c, cd) in &candidates {
                    let ab = near_cost_streaming_cmp(a, b, ad, bd);
                    let bc = near_cost_streaming_cmp(b, c, bd, cd);
                    if ab.is_le() && bc.is_le() {
                        assert!(near_cost_streaming_cmp(a, c, ad, cd).is_le());
                    }
                    assert_eq!(ab, near_cost_streaming_cmp(b, a, bd, ad).reverse());
                }
            }
        }
    }

    fn fake_graph(n: usize, edges: &[(usize, usize)]) -> JoinGraph {
        // Build a graph with synthetic empty scans — connectivity
        // testing only needs the adjacency matrix and table_count.
        let scans: Vec<PhysicalPlan> = (0..n)
            .map(|_| {
                use std::sync::Arc;
                let mut schema = super::super::test_support::table_schema(&[]);
                schema.id = 0;
                schema.object_id = 0;
                let schema = Arc::new(schema);
                PhysicalPlan::SeqScan {
                    table: PlanTable::new(0, schema, None),
                    filter: None,
                    estimated_rows: 0.0,
                    access_rows: 0.0,
                    cost: crate::engine::cbo::Cost::ZERO,
                    possible_keys: vec![],
                    runtime_filters: vec![],
                    hint_note: None,
                    projected_columns: None,
                }
            })
            .collect();
        let mut graph = JoinGraph::new(scans, vec![]);
        // Override adjacency manually with the supplied edges.
        for (i, j) in edges {
            graph.edges[*i][*j] = true;
            graph.edges[*j][*i] = true;
        }
        if n <= MAX_DP_TABLES {
            graph.connected_masks = Some(
                (0..(1u32 << n))
                    .map(|mask| graph.compute_connected(mask))
                    .collect(),
            );
        }
        graph
    }

    fn named_scan(name: &str, rows: u64, columns: &[&str]) -> PhysicalPlan {
        use std::sync::Arc;

        let columns: Vec<crate::types::Column> = columns
            .iter()
            .map(|name| crate::types::Column {
                name: (*name).to_string(),
                ..Default::default()
            })
            .collect();
        let mut schema = crate::storage::TableSchema::synthetic_for_planning(name, &columns);
        schema.database = "test".to_string();
        schema.stats.row_count = rows;
        PhysicalPlan::SeqScan {
            table: PlanTable::new(0, Arc::new(schema), None),
            filter: None,
            estimated_rows: rows as f64,
            access_rows: rows as f64,
            cost: Cost::ZERO,
            possible_keys: vec![],
            runtime_filters: vec![],
            hint_note: None,
            projected_columns: None,
        }
    }

    fn equi(left_table: &str, left_column: &str, right_table: &str, right_column: &str) -> Expr {
        Expr::Binary {
            left: Box::new(Expr::CompoundIdentifier(vec![
                left_table.to_string(),
                left_column.to_string(),
            ])),
            op: BinaryOp::Eq,
            right: Box::new(Expr::CompoundIdentifier(vec![
                right_table.to_string(),
                right_column.to_string(),
            ])),
        }
    }

    #[test]
    fn empty_mask_is_not_connected() {
        let g = fake_graph(3, &[(0, 1), (1, 2)]);
        assert!(!g.is_connected(0));
    }

    #[test]
    fn singleton_is_connected() {
        let g = fake_graph(3, &[(0, 1)]);
        assert!(g.is_connected(1));
        assert!(g.is_connected(2));
        assert!(g.is_connected(4));
    }

    #[test]
    fn linear_chain_is_connected() {
        let g = fake_graph(4, &[(0, 1), (1, 2), (2, 3)]);
        assert!(g.is_connected(0b1111));
        assert!(g.is_connected(0b0111));
        // Non-adjacent pair without bridge → disconnected.
        assert!(!g.is_connected(0b1001));
    }

    #[test]
    fn star_subset_disconnected_when_center_missing() {
        // 0 is the center, 1/2/3 hang off it.
        let g = fake_graph(4, &[(0, 1), (0, 2), (0, 3)]);
        assert!(g.is_connected(0b1111));
        // Subset {1, 2, 3} excludes the center so it's three isolated nodes.
        assert!(!g.is_connected(0b1110));
    }

    #[test]
    fn has_edge_between_detects_bridge() {
        let g = fake_graph(3, &[(0, 1), (1, 2)]);
        assert!(g.has_edge_between(0b001, 0b010));
        assert!(g.has_edge_between(0b011, 0b100));
        // {0} and {2} have no direct edge.
        assert!(!g.has_edge_between(0b001, 0b100));
    }

    #[test]
    fn dp_returns_none_for_too_few_relations() {
        let g = fake_graph(1, &[]);
        let r = enumerate_inner_joins(&empty_ctx(), &g, &[]);
        assert!(r.is_none());
    }

    #[test]
    fn above_dp_cap_returns_complete_multi_start_plan() {
        let g = fake_graph(MAX_DP_TABLES + 1, &[]);
        let r = enumerate_inner_joins(&empty_ctx(), &g, &[]);
        assert!(r.is_some());
    }

    #[test]
    fn deterministic_budget_is_not_cut_short_by_wall_clock() {
        let expired = Instant::now() - Duration::from_millis(1);
        let mut budget = PlanningBudget::new(expired, Some(2));
        assert!(budget.consume());
        assert!(budget.consume());
        assert!(!budget.consume());
    }

    #[test]
    fn configured_budget_is_stable_and_bounded() {
        assert_eq!(configured_operation_budget(&empty_ctx()), 50);
    }

    #[test]
    fn deterministic_operation_budget_traces_both_timeout_exits() {
        let graph = fake_graph(2, &[(0, 1)]);
        // One operation passes the post-incumbent check and expires at the
        // outer subset loop. Two operations expire inside bipartition
        // enumeration. Both paths must return the complete incumbent.
        for (connection_id, operations, expected_path) in [
            (91_001, 1, "outer_deadline"),
            (91_002, 2, "bipartition_deadline"),
        ] {
            let _guard = crate::engine::cbo::trace::activate(connection_id, true);
            crate::engine::cbo::trace::begin("synthetic timeout");
            let (plan, _) = enumerate_inner_joins_with_operation_budget(
                &empty_ctx(),
                &graph,
                &[],
                Some(operations),
            )
            .expect("a timeout must return the complete multi-start incumbent");
            crate::engine::cbo::trace::finish(&plan);
            let rows = crate::engine::cbo::trace::rows();
            assert_eq!(rows.len(), 1);
            let rendered = rows[0][1].to_display_string();
            assert!(rendered.contains("join_planning_timeout"), "{rendered}");
            assert!(rendered.contains(expected_path), "{rendered}");
            assert!(rendered.contains("multi_start_incumbent"), "{rendered}");
            assert!(rendered.contains("candidate_count"), "{rendered}");
            assert!(rendered.contains("completed_subset_size"), "{rendered}");
            assert!(
                rendered.contains("incumbent_max_intermediate_rows"),
                "{rendered}"
            );
        }
    }

    #[test]
    fn timeout_incumbent_is_source_order_invariant_on_snowflake_graph() {
        let scans = vec![
            named_scan("date_filtered", 365, &["date_sk"]),
            named_scan(
                "catalog_sales",
                1_400_000,
                &["sold_date_sk", "item_sk", "demo_sk"],
            ),
            named_scan("inventory", 11_700_000, &["date_sk", "item_sk"]),
            named_scan("date_inventory", 73_000, &["date_sk"]),
            named_scan("demographics", 1_200, &["demo_sk"]),
        ];
        let predicates = vec![
            equi("date_filtered", "date_sk", "catalog_sales", "sold_date_sk"),
            equi("catalog_sales", "item_sk", "inventory", "item_sk"),
            equi("inventory", "date_sk", "date_inventory", "date_sk"),
            equi("catalog_sales", "demo_sk", "demographics", "demo_sk"),
        ];
        let permutations = [
            [0, 1, 2, 3, 4],
            [4, 2, 0, 3, 1],
            [2, 3, 1, 4, 0],
            [1, 0, 4, 2, 3],
        ];
        let mut reference: Option<(Vec<String>, f64, f64)> = None;
        for permutation in permutations {
            let graph = JoinGraph::new(
                permutation
                    .into_iter()
                    .map(|idx| scans[idx].clone())
                    .collect(),
                predicates.clone(),
            );
            let (plan, leftover) =
                enumerate_inner_joins_with_operation_budget(&empty_ctx(), &graph, &[], Some(0))
                    .expect("the anytime search must return a complete plan");
            assert!(leftover.is_empty());
            let table_order: Vec<String> = collect_physical_plan_tables(&plan)
                .into_iter()
                .map(|table| table.schema.name.clone())
                .collect();
            let observed = (table_order, plan_cost(&plan).total, estimated_rows(&plan));
            if let Some(reference) = &reference {
                assert_eq!(observed.0, reference.0);
                assert_eq!(observed.1, reference.1);
                assert_eq!(observed.2, reference.2);
            } else {
                reference = Some(observed);
            }
        }
    }

    #[test]
    fn straddling_predicates_splits_correctly() {
        // 3 tables wired in a chain: 0 - 1 - 2.
        // Predicate "0 joins 2 directly" (a synthetic skip-link) should be
        // returned as joinable iff one mask covers 0 and the other 2.
        // We don't construct real predicates here — instead we call
        // straddling_predicates with an empty candidate list to verify
        // the API contract on edge inputs.
        let g = fake_graph(3, &[(0, 1), (1, 2)]);
        let (j, s) = g.straddling_predicates(0b001, 0b110, &[]);
        assert!(j.is_empty());
        assert!(s.is_empty());
    }

    #[test]
    fn dominance_requires_equal_properties_and_all_cost_dimensions() {
        let g = fake_graph(1, &[]);
        let make = |cost: Cost, properties: PhysicalProperties| DpCell {
            plan: g.scans[0].clone(),
            cost,
            rows: 1.0,
            remaining: Vec::new(),
            properties,
            max_intermediate_rows: 1.0,
        };
        let cheap = make(
            Cost::from_components(1.0, 5.0, 0.0, 0.0, 2.0, 64.0, 0.0),
            PhysicalProperties::default(),
        );
        let expensive = make(
            Cost::from_components(2.0, 6.0, 0.0, 0.0, 3.0, 128.0, 10.0),
            PhysicalProperties::default(),
        );
        assert!(cell_dominates(&cheap, &expensive));

        let mut ordered_properties = PhysicalProperties::default();
        ordered_properties.ordering.push("id".to_string());
        let ordered = make(expensive.cost, ordered_properties);
        assert!(!cell_dominates(&cheap, &ordered));

        let lower_total_higher_memory = make(
            Cost::from_components(0.5, 4.0, 0.0, 0.0, 2.0, 256.0, 0.0),
            PhysicalProperties::default(),
        );
        assert!(!cell_dominates(&lower_total_higher_memory, &expensive));
    }
}
