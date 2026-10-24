//! Join and qualification cost policy.

use super::*;

/// Shared with the vector executor: reverse existence joins retain the
/// preserved input and stream the larger input through its key set.
pub(crate) fn reverse_semi_anti_hash_join_eligible(
    join_type: JoinType,
    has_residual: bool,
    probe_rows: f64,
    build_rows: f64,
) -> bool {
    matches!(join_type, JoinType::Semi | JoinType::Anti)
        && !has_residual
        && probe_rows * 2.0 < build_rows
}

/// Approximate per-row CPU cost (in cost-units) of evaluating a filter
/// predicate. Counts the number of arithmetic / comparison / function
/// nodes and multiplies by `cbo_cpu_operator_cost`. LIKE and REGEXP
/// patterns get a 50× multiplier; subqueries fall back to a fixed
/// estimate (Phase 5 will price them properly).
pub(super) fn qual_cost_per_row(expr: &Expr) -> f64 {
    use crate::engine::cbo::cbo_consts;
    let consts = cbo_consts();
    let ops = count_qual_operators(expr);
    ops * consts.cpu_operator_cost()
}

pub(super) fn count_qual_operators(expr: &Expr) -> f64 {
    match expr {
        Expr::Literal(_)
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Wildcard
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Grouping(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. } => 0.0,
        Expr::Placeholder(_) | Expr::BoundValue(_) => 0.0,
        Expr::GroupKeyRef { expr, .. } => count_qual_operators(expr),
        Expr::Tuple(items) => items.iter().map(count_qual_operators).sum::<f64>(),
        Expr::Binary { left, right, .. } => {
            1.0 + count_qual_operators(left) + count_qual_operators(right)
        }
        Expr::Unary { expr, .. } => 1.0 + count_qual_operators(expr),
        Expr::IsNull(e) | Expr::IsNotNull(e) => 1.0 + count_qual_operators(e),
        Expr::Between {
            expr, low, high, ..
        } => {
            2.0 + count_qual_operators(expr)
                + count_qual_operators(low)
                + count_qual_operators(high)
        }
        Expr::Like { expr, pattern, .. } => {
            // LIKE is roughly 50x a normal compare due to pattern walk.
            50.0 + count_qual_operators(expr) + count_qual_operators(pattern)
        }
        Expr::InList { expr, list, .. } => {
            (list.len() as f64).max(1.0) + count_qual_operators(expr)
        }
        Expr::InSubquery { expr, .. } => 10.0 + count_qual_operators(expr),
        Expr::QuantifiedComparison { left, .. } => 10.0 + count_qual_operators(left),
        Expr::Subquery(_) | Expr::Exists { .. } => 10.0,
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let mut sum = 1.0;
            if let Some(o) = operand {
                sum += count_qual_operators(o);
            }
            sum += conditions.iter().map(count_qual_operators).sum::<f64>();
            sum += results.iter().map(count_qual_operators).sum::<f64>();
            if let Some(e) = else_result {
                sum += count_qual_operators(e);
            }
            sum
        }
        Expr::Function { name, args, .. } | Expr::WindowFunction { name, args, .. } => {
            // Phase 5: route function calls through the volatility-aware
            // cost table. The returned cost is in absolute cost-units;
            // convert back to "operator units" by dividing by
            // `cpu_operator_cost` so the caller's `× cpu_operator_cost`
            // multiplication stays consistent across all expression
            // nodes.
            use crate::engine::cbo::function_cost::function_call_cost;
            let consts = crate::engine::cbo::cbo_consts();
            let unit = consts.cpu_operator_cost().max(1e-12);
            let multiplier = function_call_cost(name) / unit;
            multiplier + args.iter().map(count_qual_operators).sum::<f64>()
        }
    }
}

/// Phase 4: build-side picker that consults plan-level cost when row
/// counts alone are uninformative (the common case before `ANALYZE
/// TABLE` has run, where `row_count` defaults to 0 → estimate 1.0).
/// The cost is a strict tiebreak — when row counts are reliable the
/// existing row-based heuristic wins.
pub(super) fn choose_build_side_with_costs(
    join_type: JoinType,
    left_rows: f64,
    right_rows: f64,
    left_cost: Option<f64>,
    right_cost: Option<f64>,
) -> BuildSide {
    const BUILD_FLIP_RATIO: f64 = 0.7;
    let stats_unreliable = left_rows <= 1.0 || right_rows <= 1.0;
    // When stats can't tell us which side is smaller, prefer the one
    // whose physical plan reports the lower cost (typically the side
    // whose scan touches fewer pages). Falls back to the original
    // BuildSide::Right default if no cost was supplied.
    let cost_signal = match (left_cost, right_cost) {
        (Some(l), Some(r)) if (l - r).abs() > 0.0 => Some(if l < r {
            BuildSide::Left
        } else {
            BuildSide::Right
        }),
        _ => None,
    };
    match join_type {
        // The full-join kernel tracks unmatched rows on both sides, including
        // its Grace path. It is symmetric too: building the larger side only
        // increases resident state and spill traffic.
        JoinType::Inner | JoinType::Cross | JoinType::Full => {
            if stats_unreliable {
                cost_signal.unwrap_or(BuildSide::Right)
            } else if left_rows < right_rows {
                BuildSide::Left
            } else {
                BuildSide::Right
            }
        }
        JoinType::Left => {
            if !stats_unreliable && left_rows < right_rows * BUILD_FLIP_RATIO {
                BuildSide::Left
            } else if stats_unreliable {
                cost_signal.unwrap_or(BuildSide::Right)
            } else {
                BuildSide::Right
            }
        }
        JoinType::Right => {
            if !stats_unreliable && right_rows < left_rows * BUILD_FLIP_RATIO {
                BuildSide::Right
            } else if stats_unreliable {
                cost_signal.unwrap_or(BuildSide::Left)
            } else {
                BuildSide::Left
            }
        }
        // Phase B #B1: Semi/Anti always probe LEFT, so build side
        // must be RIGHT — this is a correctness constraint, not a
        // tunable. The HashJoin executor relies on it to emit exactly
        // one (or zero, for Anti) output row per LEFT row.
        JoinType::Semi | JoinType::Anti => BuildSide::Right,
    }
}

/// Heap page count for cost purposes: the actual count persisted by
/// the last ANALYZE when available, otherwise the width-based estimate
/// (which can run ~1.7× high on wide rows — enough to mis-trigger
/// buffer-pool-pressure costing right at the cache-size boundary).
pub(super) fn table_heap_pages_for_cost(schema: &TableSchema) -> f64 {
    use crate::engine::cbo::operator_cost;
    if schema.stats.heap_page_count > 0 {
        return schema.stats.heap_page_count as f64;
    }
    let rows = schema.stats.row_count.max(1) as f64;
    let width = operator_cost::estimated_row_width(schema);
    operator_cost::estimated_heap_pages(rows, width)
}

/// Expected inner rows inspected by an existence lookup with a right-only
/// residual. For `n` candidate rows and independent pass probability `p`, a
/// SEMI/ANTI probe stops at the first passing row, so its truncated geometric
/// expectation is `(1 - (1-p)^n) / p`, bounded by the full candidate count.
/// This is execution work, not output cardinality: ANTI pays the same search
/// before deciding that no passing row exists.
pub(super) fn semi_anti_expected_inner_rows(rows_per_key: f64, residual_selectivity: f64) -> f64 {
    let candidates = rows_per_key.max(0.0);
    if candidates == 0.0 {
        return 0.0;
    }
    let pass = residual_selectivity.clamp(0.0, 1.0);
    if pass <= f64::EPSILON {
        return candidates;
    }
    if pass >= 1.0 - f64::EPSILON {
        return 1.0_f64.min(candidates);
    }
    ((1.0 - (1.0 - pass).powf(candidates)) / pass).min(candidates)
}

/// In mydb the PRIMARY index is not clustered — heap rows live in a
/// separate file, so a PRIMARY probe and a same-shape secondary probe
/// pay the same I/O. The cost model historically gave PRIMARY several
/// "free" lanes (no large-NLJ penalty, full sequential correlation,
/// bounded heap working set). A secondary whose leading column is
/// shared with a PRIMARY/UNIQUE composite (e.g. lineitem's
/// idx_lineitem_orderkey vs PRIMARY(l_orderkey, l_linenumber))
/// performs the exact same kind of probe and deserves the same
/// treatment — without it the cost model over-priced shadow
/// secondaries by ~9× at TPC-H sf=1.
pub(super) fn acts_like_primary_for_cost(schema: &TableSchema, index: &IndexSchema) -> bool {
    if index.primary {
        return true;
    }
    if index.unique {
        return false;
    }
    index
        .columns
        .first()
        .is_some_and(|c| shared_leading_pk_or_unique_composite(schema, c))
}

pub(super) fn should_cost_index_nlj_as_small_batch(
    join_type: JoinType,
    left_rows: f64,
    rows_per_lookup: f64,
) -> bool {
    matches!(join_type, JoinType::Inner | JoinType::Left)
        && left_rows <= 1_024.0
        && left_rows * rows_per_lookup <= 32_768.0
}

pub(super) fn should_cost_index_nlj_as_batch(
    join_type: JoinType,
    left_rows: f64,
    rows_per_lookup: f64,
    right_total_rows: f64,
) -> bool {
    // Conservative CBO policy: price as batched only when left ≥ 8192 or
    // (right ≥ 1k && left ≥ 64). Executor WS-N3 also goes batched at
    // right ≥ 256 && left ≥ 8, but the CBO does not bank that tier — otherwise
    // INLJ cost on small joins is underestimated and steals the path from Hash
    // (the lesson from recalibrating the historical INLJ/HJ inversion, see
    // `project_cbo_inlj_hash_inversion_fix`). If the CBO does pick INLJ, the executor still uses the lightweight batch — semantics intact, only the cost pricing is conservative.
    let reaches_executor_threshold =
        left_rows >= 8_192.0 || (right_total_rows >= 1_000.0 && left_rows >= 64.0);
    matches!(
        join_type,
        JoinType::Inner | JoinType::Left | JoinType::Semi | JoinType::Anti
    ) && reaches_executor_threshold
        && left_rows * rows_per_lookup <= 100_000_000.0
}

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_hash_join_parallel_probe_cost_adjustment(
    cost: &mut crate::engine::cbo::Cost,
    probe_plan: &PhysicalPlan,
    probe_cost: &crate::engine::cbo::Cost,
    build_rows: f64,
    probe_rows: f64,
    join_type: JoinType,
    build_side: BuildSide,
    residual: Option<&Expr>,
    match_rate: f64,
) {
    // The executor's parallel probe (C1) serves Inner joins and
    // probe-preserved outer joins: LEFT with build=right / RIGHT with
    // build=left, where unmatched probe rows NULL-extend inside the
    // worker. Pricing only Inner here over-stated LEFT-join probe CPU
    // by the full parallelism factor relative to what actually runs.
    let probe_preserved = matches!(
        (join_type, build_side),
        (JoinType::Inner, _)
            | (JoinType::Left, BuildSide::Right)
            | (JoinType::Right, BuildSide::Left)
    );
    if !probe_preserved || residual.is_some() {
        return;
    }
    let degree = hash_join_parallel_probe_degree_for_cost(build_rows, probe_rows);
    if degree <= 1 {
        return;
    }
    let d = degree as f64;

    if let Some(parallel_probe_cost) =
        parallelized_probe_scan_cost_for_hash_join(probe_plan, degree)
    {
        let saved = (probe_cost.total - parallel_probe_cost.total).max(0.0);
        cost.total = (cost.total - saved).max(cost.startup);
        cost.io_seq_pages = (cost.io_seq_pages
            - (probe_cost.io_seq_pages - parallel_probe_cost.io_seq_pages).max(0.0))
        .max(0.0);
        cost.io_rnd_pages = (cost.io_rnd_pages
            - (probe_cost.io_rnd_pages - parallel_probe_cost.io_rnd_pages).max(0.0))
        .max(0.0);
    }

    let consts = crate::engine::cbo::cbo_consts();
    let cpu_probe = probe_rows
        * (consts.hash_probe_cost() + match_rate.clamp(0.0, 1.0) * consts.cpu_operator_cost());
    let cpu_saved = cpu_probe * (1.0 - 1.0 / d);
    cost.total = (cost.total - cpu_saved).max(cost.startup);
    cost.cpu_units = (cost.cpu_units - cpu_saved).max(0.0);
}

pub(super) fn hash_join_parallel_probe_degree_for_cost(build_rows: f64, probe_rows: f64) -> u32 {
    if crate::util::env::var("MYDB_HASH_JOIN_PARALLEL_PROBE_DISABLE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
        .unwrap_or(false)
    {
        return 1;
    }
    let parallelism = crate::util::env::var("MYDB_HASH_JOIN_PARALLELISM")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|&v| v >= 1)
        .unwrap_or_else(|| default_parallel_degree().min(8));
    if parallelism <= 1 {
        return 1;
    }
    let threshold = crate::util::env::var("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(10_000) as f64;
    let max_build_rows = crate::util::env::var("MYDB_HASH_JOIN_PARALLEL_PROBE_MAX_BUILD_ROWS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8_192) as f64;
    if build_rows > max_build_rows
        || build_rows + probe_rows < threshold
        || probe_rows < parallelism as f64
    {
        return 1;
    }
    parallelism
}

pub(super) fn parallelized_probe_scan_cost_for_hash_join(
    plan: &PhysicalPlan,
    degree: u32,
) -> Option<crate::engine::cbo::Cost> {
    match plan {
        PhysicalPlan::Exchange { cost, .. } => Some(*cost),
        PhysicalPlan::SeqScan {
            table,
            estimated_rows,
            access_rows,
            cost,
            ..
        }
        | PhysicalPlan::ZoneMapScan {
            table,
            estimated_rows,
            access_rows,
            cost,
            ..
        } => {
            if cbo_wants_parallel_for_seqscan(
                *estimated_rows,
                *access_rows,
                &table.schema,
                *cost,
                degree,
            ) {
                Some(parallel_cost_from_serial(*cost, degree))
            } else {
                None
            }
        }
        PhysicalPlan::IndexFastScan {
            estimated_rows,
            access_rows,
            cost,
            reverse,
            ..
        } => {
            if !*reverse
                && cbo_wants_parallel_for_index_fast_scan(
                    *estimated_rows,
                    *access_rows,
                    *cost,
                    degree,
                )
            {
                Some(parallel_cost_from_serial(*cost, degree))
            } else {
                None
            }
        }
        PhysicalPlan::IndexRangeScan {
            estimated_rows,
            access_rows,
            cost,
            reverse,
            ..
        } => {
            if !*reverse
                && cbo_wants_parallel_for_index_range_scan(
                    *estimated_rows,
                    *access_rows,
                    *cost,
                    degree,
                )
            {
                Some(parallel_cost_from_serial(*cost, degree))
            } else {
                None
            }
        }
        _ => None,
    }
}

pub(crate) fn adaptive_candidate_cost(
    candidate: &AdaptiveJoinCandidate,
) -> crate::engine::cbo::Cost {
    match candidate {
        AdaptiveJoinCandidate::Hash { cost, .. }
        | AdaptiveJoinCandidate::IndexNestedLoop { cost, .. }
        | AdaptiveJoinCandidate::NestedLoop { cost, .. } => *cost,
    }
}

/// Additional contextual risk for a Grace hash join when the same join has an
/// exact, unique, non-spilling lookup alternative.
///
/// `Cost::spill_bytes` intentionally reports the admitted build-state size,
/// while the ordinary Grace I/O term prices the logical bytes written/read.
/// That is sufficient when hash is the only bounded algorithm, but it omits
/// the executor's per-partition routing and per-value encoding/decoding work:
/// rows are routed among the default 16 writer streams and replayed through
/// the typed row codec. A unique point lookup does neither and has bounded
/// output (`<= outer rows`), so use the remaining partition fanout as a
/// deterministic severe-spill risk envelope. This factor represents omitted
/// codec/routing work; it is not a claim that the bytes are written 16 times.
///
/// This does not globally raise `cbo_spill_page_cost`: joins without a safe
/// unique lookup alternative, or whose build is below the severe-overflow
/// boundary, retain their existing ordering.
pub(super) fn grace_hash_unique_lookup_risk_surcharge(cost: crate::engine::cbo::Cost) -> f64 {
    if cost.spill_bytes <= 0.0 {
        return 0.0;
    }
    const PAGE_BYTES: f64 = (16 * 1024) as f64;
    const GRACE_PARTITIONS: f64 = 16.0;
    let build_pages = (cost.spill_bytes / PAGE_BYTES).ceil();
    build_pages
        * 2.0
        * crate::engine::cbo::cbo_consts().spill_page_cost()
        * (GRACE_PARTITIONS - 1.0)
}

/// Cost-side mirror of `engine::executor::select::plan::inlj_outer_parallelism`.
/// The eligibility threshold is an explicit CBO opt-in, while the degree is
/// read from the already-built physical child so runtime worker knobs cannot
/// rewrite the static join comparison.
pub(super) fn inlj_outer_parallelism_for_cost(
    left: &PhysicalPlan,
    left_rows: f64,
    right_rows: f64,
) -> usize {
    // Do not let potentially stale cardinality/NDV statistics change join
    // ordering merely because the executor can parallelise a materialised
    // outer. Only an explicit threshold opts the CBO into that discount; the
    // default executor policy is deliberately based on actual rows at runtime.
    let Some(threshold) = crate::util::env::var("MYDB_INLJ_OUTER_PARALLEL_THRESHOLD")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    else {
        return 1;
    };
    let left_usize = left_rows.max(0.0) as usize;
    let right_u64 = right_rows.max(0.0) as u64;
    if left_usize < threshold || right_u64 < 1_000 {
        return 1;
    }
    // Cost the topology already present in the physical child. Runtime
    // worker/batch environment knobs may change how that operator executes,
    // but must not feed back into static join selection or its plan cost.
    let requested = plan_physical_properties(left).parallel_degree as usize;
    requested.clamp(1, 8)
}

/// WS-N7a mirrors `engine::executor::select::plan::nlj_outer_parallelism`:
/// decide whether an NLJ runs in parallel by comparing |L|×|R| against a
/// threshold, in sync with the executor's rule, so the planner can discount
/// CPU + total for parallel NLJ. `MYDB_CBO_NLJ_OUTER_PARALLEL_DISCOUNT=off` disables the discount.
pub(super) fn nlj_outer_parallelism_for_cost(left_rows: f64, right_rows: f64) -> usize {
    let comparisons = (left_rows.max(0.0) as u64).saturating_mul(right_rows.max(0.0) as u64);
    let threshold = crate::util::env::var("MYDB_NLJ_OUTER_PARALLEL_THRESHOLD")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(10_000_000);
    if comparisons < threshold {
        return 1;
    }
    let requested = crate::util::env::var("MYDB_NLJ_OUTER_PARALLELISM")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| default_parallel_degree() as usize);
    requested
        .clamp(1, 8)
        .min((left_rows.max(0.0) as usize).max(1))
}
