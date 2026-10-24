//! WS-S1 chained streaming hash join -- inter-stage pipelining for multi-table joins.
//!
//! Design doc: `docs/multi-join-stream-optimization-design.md`.
//!
//! Shape: when the probe side of a `HashJoin` is itself a supported `HashJoin` (a chain), this module
//! rewires the whole spine into streaming stages -- each level materializes only its own build, and intermediate
//! join results are no longer materialized wholesale as `Vec<PlanRow>`:
//!
//! ```text
//! Materialized (old):  J1(A⋈B) fully materialized → J2 probe fully materialized → J3 …
//! Chained (new):  A scan → J1 core → J2 core → J3 core → consumer
//!                    (build B)   (build C)  (build D)
//! ```
//!
//! Key invariants:
//! - **The correctness ground truth is the materialized waterfall** (`execute_hash_join`). Unsupported shapes
//!   (null-safe keys, FULL shape, subqueries, CTEs, build estimate over the limit) fall back for the whole level;
//!   a runtime build overflow also falls back (this level materializes, upper levels keep streaming).
//! - Bucket tables / int-key specialization / residual pre-evaluation (A1/A2) / adaptive bloom (C2)
//!   all reuse the materialized path's implementations -- the chain core only changes how probe rows are supplied.
//! - **Cross-level runtime filter injection keeps materialized-path semantics**: once this level's build
//!   completes, the bloom (or IndexMultiLookup rewrite) is injected into every deeper level's
//!   not-yet-started build plan and the chain-tail leaf; deep builds materialize only afterwards, consistent
//!   with today's `materialize_hash_join_inputs` build-then-probe injection order.
//! - Pipelined mode (default) uses one worker thread per level: probes overlap across levels, and the memory
//!   window is bounded by channel depth (batch × depth, default 64×4 rows/level).
//!
//! GUCs: `MYDB_HASH_JOIN_STREAM` (default on), `MYDB_HASH_JOIN_STREAM_PIPELINE`
//! (default on; off = same-thread chain pull, for differential testing), `MYDB_HASH_JOIN_STREAM_BOTTOM`
//! (default off; on = the chain-bottom join's bare-scan probe also goes through the chain core, experimental --
//! the default keeps streaming-inner's heap-page direct-scan parallel probe).

use super::plan::{
    bloom_from_buckets, build_join_buckets, collect_plan_fields, compute_join_key_with_plan,
    estimate_rows_bytes, execute_plan_rows, execute_plan_rows_inner, execute_plan_stream,
    hash_join_limits, hash_join_residual_precheck_disabled, hash_join_typed_key_disabled,
    increment_hash_join_count, inject_runtime_filters_from_hash_build, intern_col_meta,
    is_int_join_column_type, join_rows, join_rows_projected, new_join_lookup,
    probe_unmatched_must_output, project_joined_row_in_place, record_hash_join_bloom_built,
    record_hash_join_bloom_filtered, record_hash_join_build_rows, record_hash_join_probe_rows,
    record_hash_join_residual_precheck, record_hash_join_typed_key_fast,
    resolve_hash_join_key_index, split_hash_join_residual_for_fields, AdaptiveBloomSampler,
    BucketMatches, ColMeta, HashJoinKey, HashJoinKeyPlan, IntKeyOut, JoinBuckets,
    MaterializedStream, PlanField, PlanRow, PlanStream, RowSchema, SplitHashResidual,
};
use super::plan::{int_key_at, JoinedResidualPre};
use super::*;
use crate::engine::executor::select::bloom::BloomFilter;
use crate::parser::JoinType;
use crate::planner::BuildSide;
use crate::util::env::is_disabled as env_off;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;

// ---------------------------------------------------------------------------
// Counters (SHOW ENGINE MYDB STATUS)
// ---------------------------------------------------------------------------

/// Number of join levels executed by the chained streaming core.
pub(crate) static HASH_JOIN_STREAM_CHAINED: AtomicU64 = AtomicU64::new(0);
/// S3a: number of build subtrees prebuilt in parallel.
pub(crate) static HASH_JOIN_STREAM_PREBUILT: AtomicU64 = AtomicU64::new(0);
/// Join rows produced by the chain cores (cf. `hash_join_materialized_out_rows`).
pub(crate) static HASH_JOIN_STREAM_ROWS: AtomicU64 = AtomicU64::new(0);
/// Times a level's runtime build exceeded the limit and the whole level fell back to materialization.
pub(crate) static HASH_JOIN_STREAM_FALLBACK: AtomicU64 = AtomicU64::new(0);
/// S5: times the chain tail fed probe via an INLJ chunk stream (instead of full materialization).
pub(crate) static HASH_JOIN_STREAM_INLJ_TAIL: AtomicU64 = AtomicU64::new(0);
/// M0 observation: output rows of materialized-form hash joins (all execute_hash_join paths).
pub(crate) static HASH_JOIN_MATERIALIZED_OUT_ROWS: AtomicU64 = AtomicU64::new(0);
pub(super) fn record_hash_join_materialized_out_rows(n: u64) {
    HASH_JOIN_MATERIALIZED_OUT_ROWS.fetch_add(n, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// GUC
// ---------------------------------------------------------------------------

fn env_on(name: &str) -> bool {
    matches!(
        crate::util::env::var(name)
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "on" | "1" | "true" | "yes"
    )
}

/// `MYDB_HASH_JOIN_STREAM` (default on).
fn hash_join_stream_disabled() -> bool {
    env_off("MYDB_HASH_JOIN_STREAM")
}

/// `MYDB_HASH_JOIN_STREAM_PIPELINE` (default on). off = same-thread chain pull.
fn pipeline_disabled() -> bool {
    env_off("MYDB_HASH_JOIN_STREAM_PIPELINE")
}

/// `MYDB_HASH_JOIN_STREAM_BOTTOM` (default off). on = the bare-scan probe of the chain-bottom
/// join also enters the chain core (replacing streaming-inner's direct-scan parallel probe, for experimental comparison).
fn bottom_enabled() -> bool {
    env_on("MYDB_HASH_JOIN_STREAM_BOTTOM")
}

/// `MYDB_HASH_JOIN_STREAM_INLJ` (default on). S5: when the chain tail is a supported INLJ,
/// the chain gate admits it (depth 1 suffices) and the tail is supplied by an INLJ chunk stream instead of full materialization.
/// off restores the old behavior of "only HashJoin chains of depth ≥ 2 enter the chain + tail materialized".
fn stream_inlj_disabled() -> bool {
    env_off("MYDB_HASH_JOIN_STREAM_INLJ")
}

/// `MYDB_JOIN_BUILD_PARALLEL` (default on). S3a: each level's build subtree on the chain is
/// independent of the others, so they are pre-materialized in parallel. When a cross-level bloom injection
/// opportunity exists (some level's probe key hits a deeper level's build table), it yields to serial automatically to keep the injection benefit.
fn build_parallel_disabled() -> bool {
    env_off("MYDB_JOIN_BUILD_PARALLEL")
}

/// `MYDB_HASH_JOIN_STREAM_SPILL` (default on). Under memory pressure, rewires the single-level (or CTE-chain)
/// hash join output beneath TopK / Limit / top-level ORDER BY into streaming stages, avoiding the whole
/// join-output materialization hitting 1037. Enabled only when the estimated output exceeds half the remaining
/// query budget; normal budgets keep the parallel materialized path (zero regression). off falls back to pure materialization.
fn spill_stream_disabled() -> bool {
    env_off("MYDB_HASH_JOIN_STREAM_SPILL")
}

// ---------------------------------------------------------------------------
// Shape checks
// ---------------------------------------------------------------------------

/// Whether a single HashJoin level can run as a chain core (pure shape; the probe subtree is not considered).
fn level_shape_supported(plan: &PhysicalPlan, force_outer_null: bool) -> bool {
    let PhysicalPlan::HashJoin {
        left,
        right,
        join_type,
        left_keys,
        right_keys,
        null_safe,
        build_side,
        residual,
        ..
    } = plan
    else {
        return false;
    };
    if !matches!(
        join_type,
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Semi | JoinType::Anti
    ) {
        return false;
    }
    if left_keys.is_empty() || left_keys.len() != right_keys.len() {
        return false;
    }
    // null-safe (<=>) keys: both the int specialization and bloom modes require non-null-safe keys,
    // keeping the same exclusion surface as streaming-inner; the materialized path is the fallback.
    if null_safe.iter().any(|&b| b) {
        return false;
    }
    if left_keys
        .iter()
        .chain(right_keys.iter())
        .any(super::simple::expr_has_subquery)
    {
        return false;
    }
    if residual
        .as_ref()
        .is_some_and(super::simple::expr_has_subquery)
    {
        return false;
    }
    // Levels whose build-side estimate exceeds the limit do not enter the chain (the materialized path handles grace spill itself).
    // With `force_outer_null`, validate against the forced build side (outer join → null-supplying side),
    // consistent with `decompose`; otherwise the wrong side's estimate can misjudge streamability.
    let effective_build = stream_build_side(*join_type, *build_side, force_outer_null);
    let build_plan = match effective_build {
        BuildSide::Right => right,
        BuildSide::Left => left,
    };
    let limits = hash_join_limits();
    if crate::planner::estimated_rows(build_plan) > limits.max_build_rows as f64 {
        return false;
    }
    true
}

/// Whether the chain tail is a natively streamable bare-scan shape (used by bottom mode, kept consistent
/// with the streaming arm of `execute_plan_stream`).
fn leaf_streamable(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::SeqScan { filter, .. } | PhysicalPlan::ZoneMapScan { filter, .. } => !filter
            .as_ref()
            .is_some_and(super::simple::expr_has_subquery),
        PhysicalPlan::Exchange {
            input,
            output_order: crate::planner::ExchangeOrder::Unordered,
            merge_keys,
            ..
        } if merge_keys.is_empty() => match input.as_ref() {
            PhysicalPlan::ParallelSeqScan { filter, .. }
            | PhysicalPlan::ParallelZoneMapScan { filter, .. } => !filter
                .as_ref()
                .is_some_and(super::simple::expr_has_subquery),
            _ => false,
        },
        _ => false,
    }
}

/// WS-C: under memory pressure, outer joins force the build onto the null-supplying side and stream the preserved side.
///
/// The CBO's `choose_build_side` builds the preserved (left) side for a LEFT join when
/// `left_rows < right_rows*0.7`; when that estimate is shrunken and distorted by predicate pushdown, the actually-large preserved
/// side gets picked as build, forcing full materialization (`execute_hash_join_from_rows` returns the complete
/// `Vec<PlanRow>`) → hitting the query budget 1037. The memory-pressure streaming path pins outer-join build to
/// the null-supplying side: the preserved side instead streams row by row as probe, and build keeps only the (usually small) dimension side.
/// Results are independent of the build side (standard probe-preserved streaming LEFT, see `ChainCore`), so this is purely safe.
/// Inner/Semi/Anti keep the planner's pick (build=Right for Semi/Anti is a correctness constraint).
fn stream_build_side(
    join_type: JoinType,
    plan_build_side: BuildSide,
    force_outer_null: bool,
) -> BuildSide {
    if force_outer_null {
        match join_type {
            JoinType::Left => return BuildSide::Right,
            JoinType::Right => return BuildSide::Left,
            _ => {}
        }
    }
    plan_build_side
}

/// The probe-side subplan. For `force_outer_null` see [`stream_build_side`].
fn probe_side(plan: &PhysicalPlan, force_outer_null: bool) -> Option<&PhysicalPlan> {
    let PhysicalPlan::HashJoin {
        left,
        right,
        join_type,
        build_side,
        ..
    } = plan
    else {
        return None;
    };
    Some(
        match stream_build_side(*join_type, *build_side, force_outer_null) {
            BuildSide::Right => left.as_ref(),
            BuildSide::Left => right.as_ref(),
        },
    )
}

/// Whether the subtree contains a CteScan: CTE results live in a thread-local registry that pipeline
/// worker threads cannot see, so the whole chain is excluded.
fn plan_contains_cte(plan: &PhysicalPlan) -> bool {
    use crate::planner::PhysicalPlan as P;
    match plan {
        P::CteScan { .. } => true,
        P::HashJoin { left, right, .. }
        | P::NestedLoopJoin { left, right, .. }
        | P::AdaptiveJoin { left, right, .. } => {
            plan_contains_cte(left) || plan_contains_cte(right)
        }
        P::IndexNestedLoopJoin { left, .. } => plan_contains_cte(left),
        P::HashAggregate { input, .. }
        | P::HashRollupAggregate { input, .. }
        | P::HashGroupingSetsAggregate { input, .. }
        | P::Sort { input, .. }
        | P::IncrementalSort { input, .. }
        | P::TopK { input, .. }
        | P::ParallelTopK { input, .. }
        | P::Projection { input, .. }
        | P::Limit { input, .. }
        | P::StreamingAggregate { input, .. }
        | P::StreamingRollupAggregate { input, .. }
        | P::StreamingGroupingSetsAggregate { input, .. }
        | P::StreamingDistinct { input, .. }
        | P::Exchange { input, .. }
        | P::ParallelHashAggregate { input, .. }
        | P::ParallelRollupAggregate { input, .. }
        | P::ParallelGroupingSetsAggregate { input, .. }
        | P::ParallelSort { input, .. }
        | P::WindowAgg { input, .. } => plan_contains_cte(input),
        P::IndexInLookup { subquery_plan, .. } => plan_contains_cte(subquery_plan),
        _ => false,
    }
}

/// Gate for the HashJoin arm of `execute_plan_stream`: chain depth ≥ 2 (at least one more streamable join
/// level below this one on the probe side), or in bottom mode chain depth ≥ 1 with a bare-scan tail.
pub(super) fn chain_applicable(_db: &Arc<Database>, plan: &PhysicalPlan) -> bool {
    if hash_join_stream_disabled() {
        return false;
    }
    if !level_shape_supported(plan, false) {
        return false;
    }
    if plan_contains_cte(plan) {
        return false;
    }
    let mut depth = 1usize;
    let mut cur = probe_side(plan, false).expect("HashJoin probe side");
    while level_shape_supported(cur, false) {
        depth += 1;
        cur = probe_side(cur, false).expect("HashJoin probe side");
    }
    if depth >= 2 {
        return true;
    }
    // S5: a supported INLJ tail → depth 1 also enters the chain (the INLJ chunk stream feeds probe,
    // upper hash levels consume it streaming, avoiding full materialization of both the INLJ output and the join output).
    if !stream_inlj_disabled() && super::plan::inlj_chunk_stream_applicable(cur) {
        return true;
    }
    bottom_enabled() && leaf_streamable(cur)
}

// ---------------------------------------------------------------------------
// Spine decomposition
// ---------------------------------------------------------------------------

/// All owned (and Send) construction data for one chain level.
struct ChainLevel {
    join_type: JoinType,
    build_is_left: bool,
    build_keys: Vec<Expr>,
    probe_keys: Vec<Expr>,
    null_safe: Vec<bool>,
    residual: Option<Expr>,
    /// This level's build subplan (runtime filters from higher levels are injected here).
    build_plan: PhysicalPlan,
    /// Full subtree clone used to fall the whole level back when runtime build exceeds the limit.
    fallback_plan: PhysicalPlan,
    build_metas: Vec<ColMeta>,
    probe_metas: Vec<ColMeta>,
    estimated_probe_rows: f64,
    /// S2 join-level projection: column positions kept in this level's output (concatenation order).
    output_projection: Option<std::sync::Arc<Vec<usize>>>,
    display_plan_key: Option<crate::planner::PlanNodeId>,
    build_display_keys: Option<super::plan::PlanStatsKeyTree>,
}

/// Splits the spine top-down into levels + tail. Consumes the owned plan passed in.
/// `force_outer_null` see [`stream_build_side`] (under memory pressure, outer-join build is pinned to the null side).
fn decompose(
    plan: PhysicalPlan,
    levels: &mut VecDeque<ChainLevel>,
    force_outer_null: bool,
    display_keys: Option<super::plan::PlanStatsKeyTree>,
) -> (PhysicalPlan, Option<super::plan::PlanStatsKeyTree>) {
    if !level_shape_supported(&plan, force_outer_null) {
        return (plan, display_keys);
    }
    let fallback_plan = plan.clone();
    let PhysicalPlan::HashJoin {
        left,
        right,
        join_type,
        left_keys,
        right_keys,
        null_safe,
        build_side,
        residual,
        output_projection,
        ..
    } = plan
    else {
        unreachable!("level_shape_supported guarantees HashJoin");
    };
    let effective_build = stream_build_side(join_type, build_side, force_outer_null);
    let (build_display_keys, probe_display_keys) = match effective_build {
        BuildSide::Right => (
            display_keys
                .as_ref()
                .and_then(|keys| keys.child(1))
                .cloned(),
            display_keys
                .as_ref()
                .and_then(|keys| keys.child(0))
                .cloned(),
        ),
        BuildSide::Left => (
            display_keys
                .as_ref()
                .and_then(|keys| keys.child(0))
                .cloned(),
            display_keys
                .as_ref()
                .and_then(|keys| keys.child(1))
                .cloned(),
        ),
    };
    let (build_plan, probe_plan, build_keys, probe_keys, build_is_left) = match effective_build {
        BuildSide::Right => (*right, *left, right_keys, left_keys, false),
        BuildSide::Left => (*left, *right, left_keys, right_keys, true),
    };
    let mut build_metas = Vec::new();
    collect_plan_fields(&build_plan, &mut build_metas);
    let mut probe_metas = Vec::new();
    collect_plan_fields(&probe_plan, &mut probe_metas);
    levels.push_back(ChainLevel {
        join_type,
        build_is_left,
        build_keys,
        probe_keys,
        null_safe,
        residual,
        estimated_probe_rows: crate::planner::estimated_rows(&probe_plan),
        build_plan,
        fallback_plan,
        build_metas,
        probe_metas,
        output_projection,
        display_plan_key: display_keys.as_ref().map(|keys| keys.key()),
        build_display_keys,
    });
    decompose(probe_plan, levels, force_outer_null, probe_display_keys)
}

// ---------------------------------------------------------------------------
// Entrypoints
// ---------------------------------------------------------------------------

pub(super) fn open_chain_stream(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<Box<dyn PlanStream>, QueryError> {
    open_chain_stream_inner(db, txn, plan, false)
}

/// Streaming entry under memory pressure (admitted by `spill_stream_applicable`). Forces same-thread chain
/// pulling (`force_serial`), avoiding extra workers and channel buffers holding memory. The semantics
/// are fully identical to `open_chain_stream` (same `open_levels` core); only the supply is serialized.
pub(super) fn open_single_level_stream(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<Box<dyn PlanStream>, QueryError> {
    open_chain_stream_inner(db, txn, plan, true)
}

fn open_chain_stream_inner(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
    force_serial: bool,
) -> Result<Box<dyn PlanStream>, QueryError> {
    let mut levels = VecDeque::new();
    // A `force_serial` entry is exactly the memory-pressure streaming path; it also pins outer-join build to the
    // null-supplying side (the preserved side streams as probe), consistent with the streamability check in `spill_stream_applicable`.
    let display_keys =
        super::analyze_stats::is_recording().then(|| super::plan::capture_plan_stats_keys(plan));
    let (tail, tail_display_keys) =
        decompose(plan.clone(), &mut levels, force_serial, display_keys);
    // `decompose` grows a VecDeque and moves each owned root enum while doing
    // so. Bind only after the final layout is stable, before parallel build
    // workers can start recording.
    for level in &levels {
        if let Some(keys) = level.build_display_keys.as_ref() {
            super::plan::bind_plan_stats_key_tree(&level.build_plan, keys);
        }
    }
    if let Some(keys) = tail_display_keys.as_ref() {
        super::plan::bind_plan_stats_key_tree(&tail, keys);
    }
    // Pipeline gate: if the chain-top probe estimate is too small, pull the chain on the same thread (saves worker-thread overhead).
    // With `force_serial` it is unconditionally same-thread, avoiding extra workers holding memory under memory pressure.
    let pipelined = !force_serial
        && !pipeline_disabled()
        && levels
            .front()
            .map(|l| l.estimated_probe_rows >= 1024.0)
            .unwrap_or(false);
    // S3a: each level's build subtree is mutually independent → pre-materialize in parallel (leaf scans overlap).
    // When a cross-level bloom injection opportunity exists, yield to serial (injection can only target deep
    // builds not yet executed); in parallel mode the injection still targets the chain-tail leaf. With `force_serial` skip
    // it, to avoid the extra memory of concurrent builds.
    let prebuilt = if !force_serial
        && !build_parallel_disabled()
        && levels.len() >= 2
        && !chain_has_deep_build_injection_opportunity(&levels)
    {
        parallel_prebuild(db, txn, &levels)?
    } else {
        VecDeque::new()
    };
    open_levels(
        db,
        txn,
        levels,
        tail,
        tail_display_keys,
        pipelined,
        prebuilt,
    )
}

pub(super) fn spill_stream_applicable(_db: &Arc<Database>, plan: &PhysicalPlan) -> bool {
    if hash_join_stream_disabled() || spill_stream_disabled() {
        return false;
    }
    // Streamability check after forcing (outer-join build pinned to the null side) -- uses the same
    // `force_outer_null=true` as the runtime `decompose`; otherwise the wrong side's build estimate misjudges.
    if !level_shape_supported(plan, true) {
        return false;
    }
    // Enabled only under memory pressure: the estimated materialized bytes exceed half the remaining query budget.
    // It never triggers under normal / large budgets, keeping the parallel materialized path (zero performance
    // regression). It does not intervene when there is no query budget (system scans, etc.).
    let Some(budget) = crate::engine::executor::mem_budget::current_budget() else {
        return false;
    };
    let remaining = budget.remaining();
    // Estimation robustness (design §2.2 safety principle: memory safety must not depend on estimate
    // accuracy): the materialized path builds the whole join output as a `Vec<PlanRow>`. An output estimate shrunken by
    // predicate pushdown (typically: the CTE body was already filtered on the pushed-down column, and the outer
    // level discounts it again by the same column's selectivity) must never route an actually-large join to
    // materialization. Use instead the largest cardinality in the whole join subtree (a materialized CteScan leaf's
    // `estimated_rows` is the exact row count) and per-row bytes from output width (wide joins far exceed the default 256B), taking the more conservative of the two.
    let est_rows = crate::planner::estimated_rows(plan).max(0.0);
    let robust_rows = max_subtree_rows(plan);
    let effective_rows = est_rows.max(robust_rows);
    let mut metas = Vec::new();
    collect_plan_fields(plan, &mut metas);
    let bytes_per_row = (metas.len().saturating_mul(32))
        .max(crate::engine::executor::mem_budget::ESTIMATED_ROW_BYTES)
        as f64;
    let est_out_bytes = effective_rows * bytes_per_row;
    est_out_bytes > (remaining as f64) * 0.5
}

/// The maximum `estimated_rows` over any node in the whole subtree. Leaves (especially materialized CteScan, whose
/// `estimated_rows` = the exact `rows.len()`) are reliable lower bounds, hence immune to "intermediate /
/// output estimates shrunken by pushdown". Used only for memory-safety checks; never affects the plan or results.
fn max_subtree_rows(plan: &PhysicalPlan) -> f64 {
    use crate::planner::PhysicalPlan as P;
    let here = crate::planner::estimated_rows(plan).max(0.0);
    let child_max = match plan {
        P::HashJoin { left, right, .. }
        | P::NestedLoopJoin { left, right, .. }
        | P::AdaptiveJoin { left, right, .. } => {
            max_subtree_rows(left).max(max_subtree_rows(right))
        }
        P::IndexNestedLoopJoin { left, .. } => max_subtree_rows(left),
        P::HashAggregate { input, .. }
        | P::HashRollupAggregate { input, .. }
        | P::HashGroupingSetsAggregate { input, .. }
        | P::Sort { input, .. }
        | P::IncrementalSort { input, .. }
        | P::TopK { input, .. }
        | P::ParallelTopK { input, .. }
        | P::Projection { input, .. }
        | P::Filter { input, .. }
        | P::Limit { input, .. }
        | P::StreamingAggregate { input, .. }
        | P::StreamingRollupAggregate { input, .. }
        | P::StreamingGroupingSetsAggregate { input, .. }
        | P::StreamingDistinct { input, .. }
        | P::Exchange { input, .. }
        | P::ParallelHashAggregate { input, .. }
        | P::ParallelRollupAggregate { input, .. }
        | P::ParallelGroupingSetsAggregate { input, .. }
        | P::ParallelSort { input, .. }
        | P::WindowAgg { input, .. } => max_subtree_rows(input),
        _ => 0.0,
    };
    here.max(child_max)
}

/// When level k's probe key (qualified name) hits a deeper level's build table/alias, serial building can
/// inject level k's bloom / IndexMultiLookup rewrite into that build table's scan -- when such an
/// opportunity exists, no parallel prebuild is done. Unqualified keys are conservatively treated as opportunities.
fn chain_has_deep_build_injection_opportunity(levels: &VecDeque<ChainLevel>) -> bool {
    for (idx, level) in levels.iter().enumerate() {
        let mut qualifiers: Vec<String> = Vec::new();
        let mut has_bare = false;
        for key in &level.probe_keys {
            match key {
                Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                    qualifiers.push(parts[parts.len() - 2].to_ascii_lowercase());
                }
                Expr::Identifier(_) => has_bare = true,
                _ => has_bare = true,
            }
        }
        for deeper in levels.iter().skip(idx + 1) {
            if has_bare {
                return true;
            }
            for meta in &deeper.build_metas {
                let table = meta.table.to_ascii_lowercase();
                let alias = meta.alias.as_ref().map(|a| a.to_ascii_lowercase());
                if qualifiers
                    .iter()
                    .any(|q| *q == table || alias.as_deref() == Some(q.as_str()))
                {
                    return true;
                }
            }
        }
    }
    false
}

/// Materialize independent build subtrees with the complete statement context.
/// System scans and nested operators must inherit the same registry, session,
/// CTEs, budget and execution settings as the connection thread.
fn parallel_prebuild(
    db: &Arc<Database>,
    txn: TxnView,
    levels: &VecDeque<ChainLevel>,
) -> Result<VecDeque<Vec<PlanRow>>, QueryError> {
    let context = super::execution_context::ExecutionContext::capture(txn);

    let mut results: Vec<Result<Vec<PlanRow>, QueryError>> = Vec::with_capacity(levels.len());
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(levels.len());
        for level in levels.iter() {
            let db = Arc::clone(db);
            let context = context.clone();
            let build_plan = &level.build_plan;
            handles.push(
                scope.spawn(move || context.run(&db, || execute_plan_rows(&db, txn, build_plan))),
            );
        }
        for handle in handles {
            results.push(handle.join().unwrap_or_else(|_| {
                Err(error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "join build prebuild worker panicked",
                ))
            }));
        }
    });
    let mut out = VecDeque::with_capacity(results.len());
    for result in results {
        out.push_back(result?);
    }
    HASH_JOIN_STREAM_PREBUILT.fetch_add(out.len() as u64, Ordering::Relaxed);
    Ok(out)
}

fn open_levels(
    db: &Arc<Database>,
    txn: TxnView,
    mut levels: VecDeque<ChainLevel>,
    mut tail: PhysicalPlan,
    tail_display_keys: Option<super::plan::PlanStatsKeyTree>,
    pipelined: bool,
    mut prebuilt: VecDeque<Vec<PlanRow>>,
) -> Result<Box<dyn PlanStream>, QueryError> {
    let Some(level) = levels.pop_front() else {
        // S5: the chain tail is supplied by an INLJ chunk stream (the left side is opened via execute_plan_stream,
        // supporting mixed-chain recursion); other shapes keep the original behavior (bare-scan streaming / materialized wrapper).
        if !stream_inlj_disabled() && super::plan::inlj_chunk_stream_applicable(&tail) {
            HASH_JOIN_STREAM_INLJ_TAIL.fetch_add(1, Ordering::Relaxed);
            return match tail_display_keys {
                Some(keys) => super::plan::open_inlj_chunk_stream_with_stats(db, txn, tail, keys),
                None => super::plan::open_inlj_chunk_stream(db, txn, tail),
            };
        }
        if let Some(keys) = tail_display_keys {
            return super::plan::open_owned_plan_stream_with_stats(db, txn, tail, keys);
        }
        return execute_plan_stream(db, txn, &tail);
    };

    let analyze_started = level
        .display_plan_key
        .map(|_| (std::time::Instant::now(), db.buffer_stats_counts()));
    // `pop_front` moves the ChainLevel once more for the serial-build path.
    if let Some(keys) = level.build_display_keys.as_ref() {
        super::plan::bind_plan_stats_key_tree(&level.build_plan, keys);
    }

    // 1) Build materialization (taken directly on an S3a prebuild hit; otherwise executed in the caller's context).
    let build_rows = match prebuilt.pop_front() {
        Some(rows) => rows,
        None => execute_plan_rows(db, txn, &level.build_plan)?,
    };
    let limits = hash_join_limits();
    let build_too_big = build_rows.len() as u64 > limits.max_build_rows
        || estimate_rows_bytes(&build_rows) > limits.mem_limit_bytes;
    if build_too_big {
        // Rare (planning already intercepts by estimate): this level falls back entirely to the materialized waterfall
        // (grace spill etc.), upper levels keep streaming. The already-materialized build is dropped; the cost is one build scan.
        HASH_JOIN_STREAM_FALLBACK.fetch_add(1, Ordering::Relaxed);
        drop(build_rows);
        let rows = execute_plan_rows_inner(db, txn, &level.fallback_plan)?;
        return Ok(Box::new(MaterializedStream::new(rows)));
    }
    increment_hash_join_count();
    record_hash_join_build_rows(build_rows.len() as u64);

    // 2) Residual split (split per side for Inner/Semi/Anti; joined-only for outer).
    let residual_split = match level.join_type {
        JoinType::Inner | JoinType::Semi | JoinType::Anti => match level.residual.as_ref() {
            Some(residual) => split_hash_join_residual_for_fields(
                residual,
                &level.build_metas,
                &level.probe_metas,
            ),
            None => SplitHashResidual::default(),
        },
        _ => SplitHashResidual {
            build_only: None,
            probe_only: None,
            joined: level.residual.clone(),
        },
    };

    // 3) Key plan (pure column references compiled to field indices) + int-specialization gate.
    let build_lookup = FieldLookup::build(&level.build_metas);
    let probe_lookup = FieldLookup::build(&level.probe_metas);
    let mut build_key_plan =
        key_plan_from_lookup(&level.build_keys, &build_lookup, &level.build_metas);
    let mut probe_key_plan =
        key_plan_from_lookup(&level.probe_keys, &probe_lookup, &level.probe_metas);
    if let (Some(build), Some(probe)) = (&mut build_key_plan, &mut probe_key_plan) {
        for i in 0..build.collation_ids.len().min(probe.collation_ids.len()) {
            let common = super::plan::common_hash_join_collation(
                build.collation_ids[i],
                probe.collation_ids[i],
            )?;
            build.collation_ids[i] = common;
            probe.collation_ids[i] = common;
        }
    }
    let typed_pair = (!level.null_safe.iter().any(|&bit| bit))
        .then(|| {
            typed_gate_from_metas(
                build_key_plan.as_ref(),
                probe_key_plan.as_ref(),
                &level.build_metas,
                &level.probe_metas,
            )
        })
        .flatten();

    // 4) Bucket table + bloom (implementation shared with the materialized path; build_only filtered at bucket-build time).
    let bloom_worthwhile = limits.bloom_worthwhile(build_rows.len());
    let (buckets, bloom) = build_join_buckets(
        &level.build_keys,
        &level.null_safe,
        build_key_plan.as_ref(),
        &build_rows,
        typed_pair.as_ref().map(|(build, _)| build.as_slice()),
        bloom_worthwhile,
        false,
        residual_split.build_only.as_ref(),
    )?;
    if !buckets.is_typed() && bloom.is_some() {
        record_hash_join_bloom_built();
    }
    let typed_probe_indices = if buckets.is_typed() {
        typed_pair.map(|(_, probe)| probe)
    } else {
        None
    };
    let build_preserved = matches!(
        (level.join_type, level.build_is_left),
        (JoinType::Left, true) | (JoinType::Right, false)
    );

    // 5) Empty-bucket short-circuit: the INNER/SEMI result is necessarily empty; the whole probe subchain need not run.
    if buckets.is_empty() && matches!(level.join_type, JoinType::Inner | JoinType::Semi) {
        HASH_JOIN_STREAM_CHAINED.fetch_add(1, Ordering::Relaxed);
        return Ok(Box::new(MaterializedStream::new(Vec::new())));
    }
    // Build-preserved outer + empty buckets: the output is exactly the NULL extension of all build rows;
    // the probe subchain likewise need not run.
    let skip_upstream = buckets.is_empty() && build_preserved;

    // 6) Cross-level runtime filters: this level's bloom / IndexMultiLookup rewrite is injected into the
    //    deeper, not-yet-executed build plans and the chain tail (semantically consistent with the materialized
    //    path's build→probe injection order).
    let build_side = if level.build_is_left {
        BuildSide::Left
    } else {
        BuildSide::Right
    };
    let inject_ok = !skip_upstream
        && !buckets.is_empty()
        && limits.inject_bloom_worthwhile(build_rows.len(), level.estimated_probe_rows)
        && !probe_unmatched_must_output(level.join_type, build_side);
    if inject_ok {
        for deeper in levels.iter_mut() {
            inject_runtime_filters_from_hash_build(
                db,
                &mut deeper.build_plan,
                &build_rows,
                &level.build_keys,
                &level.probe_keys,
                &level.null_safe,
            )?;
        }
        inject_runtime_filters_from_hash_build(
            db,
            &mut tail,
            &build_rows,
            &level.build_keys,
            &level.probe_keys,
            &level.null_safe,
        )?;
    }

    let runtime = LevelRuntime {
        join_type: level.join_type,
        build_is_left: level.build_is_left,
        build_keys: level.build_keys,
        probe_keys: level.probe_keys,
        null_safe: level.null_safe,
        build_only: residual_split.build_only,
        probe_only: residual_split.probe_only,
        joined_residual: residual_split.joined,
        build_metas: level.build_metas,
        probe_metas: level.probe_metas,
        build_key_plan,
        probe_key_plan,
        typed_probe_indices,
        build_rows,
        buckets,
        bloom,
        estimated_probe_rows: level.estimated_probe_rows,
        output_projection: level.output_projection,
        display_plan_key: level.display_plan_key,
        analyze_started,
    };
    HASH_JOIN_STREAM_CHAINED.fetch_add(1, Ordering::Relaxed);

    if pipelined && !skip_upstream {
        return Ok(Box::new(spawn_level_worker(
            db,
            txn,
            runtime,
            levels,
            tail,
            tail_display_keys,
            prebuilt,
        )));
    }
    let upstream: Box<dyn PlanStream> = if skip_upstream {
        Box::new(MaterializedStream::new(Vec::new()))
    } else {
        open_levels(
            db,
            txn,
            levels,
            tail,
            tail_display_keys,
            pipelined,
            prebuilt,
        )?
    };
    Ok(Box::new(ChainCore::new(db, runtime, upstream)))
}

fn key_plan_from_lookup(
    keys: &[Expr],
    lookup: &FieldLookup,
    metas: &[ColMeta],
) -> Option<HashJoinKeyPlan> {
    let mut field_indices = smallvec::SmallVec::with_capacity(keys.len());
    let mut collation_ids = smallvec::SmallVec::with_capacity(keys.len());
    for key in keys {
        let idx = resolve_hash_join_key_index(key, lookup)?;
        field_indices.push(idx);
        collation_ids.push(metas.get(idx).and_then(|meta| meta.source.collation_id));
    }
    Some(HashJoinKeyPlan {
        field_indices,
        collation_ids,
    })
}

/// Metadata-only typed integer-key gate shared by every key arity.
fn typed_gate_from_metas(
    build_key_plan: Option<&HashJoinKeyPlan>,
    probe_key_plan: Option<&HashJoinKeyPlan>,
    build_metas: &[ColMeta],
    probe_metas: &[ColMeta],
) -> Option<(Vec<usize>, Vec<usize>)> {
    if hash_join_typed_key_disabled() {
        return None;
    }
    let build_indices = build_key_plan?.field_indices.as_slice();
    let probe_indices = probe_key_plan?.field_indices.as_slice();
    if build_indices.is_empty() || build_indices.len() != probe_indices.len() {
        return None;
    }
    for &index in build_indices {
        if !is_int_join_column_type(build_metas.get(index)?.column_type) {
            return None;
        }
    }
    for &index in probe_indices {
        if !is_int_join_column_type(probe_metas.get(index)?.column_type) {
            return None;
        }
    }
    Some((build_indices.to_vec(), probe_indices.to_vec()))
}

fn compact_int_key_at(row: &PlanRow, indices: &[usize], out: &mut Vec<i64>) -> Result<bool, ()> {
    out.clear();
    out.reserve(indices.len());
    for &index in indices {
        match row.fields.get(index).map(|field| field.value_ref()) {
            Some(Value::Int(value)) => out.push(*value),
            Some(Value::UInt(value)) if *value <= i64::MAX as u64 => out.push(*value as i64),
            Some(Value::Null) => return Ok(false),
            _ => return Err(()),
        }
    }
    Ok(true)
}

// ---------------------------------------------------------------------------
// Chain core
// ---------------------------------------------------------------------------

/// Runtime data for one chain-core level (fully owned and Send; non-Send candidates such as residual
/// evaluators are constructed inside `ChainCore::new`, on the target thread).
struct LevelRuntime {
    join_type: JoinType,
    build_is_left: bool,
    build_keys: Vec<Expr>,
    probe_keys: Vec<Expr>,
    null_safe: Vec<bool>,
    build_only: Option<Expr>,
    probe_only: Option<Expr>,
    joined_residual: Option<Expr>,
    build_metas: Vec<ColMeta>,
    probe_metas: Vec<ColMeta>,
    build_key_plan: Option<HashJoinKeyPlan>,
    probe_key_plan: Option<HashJoinKeyPlan>,
    typed_probe_indices: Option<Vec<usize>>,
    build_rows: Vec<PlanRow>,
    buckets: JoinBuckets,
    bloom: Option<BloomFilter>,
    estimated_probe_rows: f64,
    output_projection: Option<std::sync::Arc<Vec<usize>>>,
    display_plan_key: Option<crate::planner::PlanNodeId>,
    analyze_started: Option<(std::time::Instant, (u64, u64))>,
}

struct ChainCore {
    db: Arc<Database>,
    rt: LevelRuntime,
    semi_or_anti: bool,
    build_preserved: bool,
    probe_preserved: bool,
    residual_pre: Option<JoinedResidualPre>,
    adaptive: AdaptiveBloomSampler,
    build_matched: Vec<bool>,
    build_null_fields: Vec<PlanField>,
    probe_null_fields: Vec<PlanField>,
    upstream: Box<dyn PlanStream>,
    pending: VecDeque<PlanRow>,
    upstream_done: bool,
    unmatched_cursor: usize,
    join_lk: std::sync::Arc<RowSchema>,
    rows_out: u64,
    probe_rows_in: u64,
    tick: u32,
    typed_probe_scratch: Vec<i64>,
    typed_arity: Option<usize>,
    typed_bloom_built: u64,
}

impl ChainCore {
    fn new(db: &Arc<Database>, rt: LevelRuntime, upstream: Box<dyn PlanStream>) -> Self {
        let typed_arity = rt.buckets.typed_arity();
        let typed_bloom_built = u64::from(typed_arity.is_some() && rt.bloom.is_some());
        let semi_or_anti = matches!(rt.join_type, JoinType::Semi | JoinType::Anti);
        let build_preserved = matches!(
            (rt.join_type, rt.build_is_left),
            (JoinType::Left, true) | (JoinType::Right, false)
        );
        let probe_preserved = matches!(
            (rt.join_type, rt.build_is_left),
            (JoinType::Left, false) | (JoinType::Right, true)
        );
        let residual_pre = rt.joined_residual.as_ref().and_then(|expr| {
            if hash_join_residual_precheck_disabled() {
                return None;
            }
            let (l, r) = if rt.build_is_left {
                (&rt.build_metas, &rt.probe_metas)
            } else {
                (&rt.probe_metas, &rt.build_metas)
            };
            JoinedResidualPre::compile_from_metas(expr, l, r)
        });
        if residual_pre.is_some() {
            record_hash_join_residual_precheck();
        }
        let limits = hash_join_limits();
        let adaptive = AdaptiveBloomSampler::new(
            AdaptiveBloomSampler::gate(&limits, rt.bloom.is_some(), rt.build_rows.len()),
            rt.estimated_probe_rows.max(0.0) as u64,
        );
        let null_fields = |metas: &[ColMeta]| -> Vec<PlanField> {
            metas
                .iter()
                .map(|meta| PlanField {
                    meta: intern_col_meta(meta.clone()),
                    value: Value::Null,
                })
                .collect()
        };
        let build_matched = if build_preserved {
            vec![false; rt.build_rows.len()]
        } else {
            Vec::new()
        };
        Self {
            db: Arc::clone(db),
            semi_or_anti,
            build_preserved,
            probe_preserved,
            residual_pre,
            adaptive,
            build_matched,
            build_null_fields: null_fields(&rt.build_metas),
            probe_null_fields: null_fields(&rt.probe_metas),
            upstream,
            pending: VecDeque::new(),
            upstream_done: false,
            unmatched_cursor: 0,
            join_lk: new_join_lookup(),
            rows_out: 0,
            probe_rows_in: 0,
            tick: 0,
            typed_probe_scratch: Vec::new(),
            typed_arity,
            typed_bloom_built,
            rt,
        }
    }

    /// Declares that an int-typed column hit a non-int value at runtime (Bail): rebuilds the generic bucket
    /// table in place from the already-materialized build rows; the bloom is bound to the int-mode encoding and is
    /// dropped along with it (the adaptive sampler can still rebuild it later in generic mode).
    fn degrade_to_generic(&mut self) -> Result<(), QueryError> {
        let (buckets, _bloom) = build_join_buckets(
            &self.rt.build_keys,
            &self.rt.null_safe,
            self.rt.build_key_plan.as_ref(),
            &self.rt.build_rows,
            None,
            false,
            false,
            self.rt.build_only.as_ref(),
        )?;
        self.rt.buckets = buckets;
        self.rt.bloom = None;
        self.rt.typed_probe_indices = None;
        self.typed_arity = None;
        self.typed_bloom_built = 0;
        Ok(())
    }

    fn null_extend_probe(&self, probe_row: PlanRow) -> PlanRow {
        let mut fields = Vec::with_capacity(self.build_null_fields.len() + probe_row.fields.len());
        if self.rt.build_is_left {
            fields.extend(self.build_null_fields.iter().cloned());
            fields.extend(probe_row.fields);
        } else {
            fields.extend(probe_row.fields);
            fields.extend(self.build_null_fields.iter().cloned());
        }
        let mut row = PlanRow {
            fields,
            group: None,
            agg_values: None,
            schema: self.join_lk.clone().into(),
        };
        if let Some(projection) = self.rt.output_projection.as_deref() {
            project_joined_row_in_place(&mut row, projection);
        }
        row
    }

    fn null_extend_build(&self, bi: usize) -> PlanRow {
        let build_row = &self.rt.build_rows[bi];
        let mut fields = Vec::with_capacity(build_row.fields.len() + self.probe_null_fields.len());
        if self.rt.build_is_left {
            fields.extend(build_row.fields.iter().cloned());
            fields.extend(self.probe_null_fields.iter().cloned());
        } else {
            fields.extend(self.probe_null_fields.iter().cloned());
            fields.extend(build_row.fields.iter().cloned());
        }
        let mut row = PlanRow {
            fields,
            group: None,
            agg_values: None,
            schema: self.join_lk.clone().into(),
        };
        if let Some(projection) = self.rt.output_projection.as_deref() {
            project_joined_row_in_place(&mut row, projection);
        }
        row
    }

    /// Processes one probe row: hits go into `pending`. Aligned branch by branch with
    /// the probe loop of `do_in_memory_hash_join` (and its int specialization).
    fn process_probe(&mut self, probe_row: PlanRow) -> Result<(), QueryError> {
        self.probe_rows_in += 1;
        self.tick = self.tick.wrapping_add(1);
        if self.tick & 0x3FF == 0 {
            crate::engine::cancel::check_cancelled()?;
        }
        if let Some(expr) = self.rt.probe_only.as_ref() {
            if !eval_plan_bool(expr, &probe_row)? {
                if self.rt.join_type == JoinType::Anti {
                    self.pending.push_back(self.project_semi_anti(probe_row));
                }
                return Ok(());
            }
        }

        enum KeyOut {
            Int(i128),
            IntComposite,
            Gen(HashJoinKey),
            Null,
        }
        let key = loop {
            if let Some(indices) = self.rt.typed_probe_indices.as_deref() {
                if indices.len() == 1 {
                    match int_key_at(&probe_row, indices[0]) {
                        IntKeyOut::Key(key) => break KeyOut::Int(key),
                        IntKeyOut::Null => break KeyOut::Null,
                        IntKeyOut::Bail => {
                            self.degrade_to_generic()?;
                            continue;
                        }
                    }
                } else {
                    match compact_int_key_at(&probe_row, indices, &mut self.typed_probe_scratch) {
                        Ok(true) => break KeyOut::IntComposite,
                        Ok(false) => break KeyOut::Null,
                        Err(()) => {
                            self.degrade_to_generic()?;
                            continue;
                        }
                    }
                }
            }
            match compute_join_key_with_plan(
                &self.rt.probe_keys,
                &self.rt.null_safe,
                self.rt.probe_key_plan.as_ref(),
                &probe_row,
            )? {
                Some(k) => break KeyOut::Gen(k),
                None => break KeyOut::Null,
            }
        };

        let mut probe_matched = false;
        if !matches!(key, KeyOut::Null) {
            let bloom_hit = match (&self.rt.bloom, &key) {
                (Some(bf), KeyOut::Int(k)) => bf.contains(k),
                (Some(bf), KeyOut::IntComposite) => bf.contains(&self.typed_probe_scratch),
                (Some(bf), KeyOut::Gen(k)) => bf.contains(k.as_slice()),
                _ => true,
            };
            if !bloom_hit {
                record_hash_join_bloom_filtered();
            }
            let matches = if bloom_hit {
                match (&self.rt.buckets, &key) {
                    (JoinBuckets::Int { map, next }, KeyOut::Int(k)) => map
                        .get(k)
                        .map(|&(head, _)| BucketMatches::Chain { next, cursor: head }),
                    (JoinBuckets::IntComposite { map, next, .. }, KeyOut::IntComposite) => map
                        .get(&self.typed_probe_scratch)
                        .map(|&(head, _)| BucketMatches::Chain { next, cursor: head }),
                    (JoinBuckets::Gen(..) | JoinBuckets::GenChained { .. }, KeyOut::Gen(k)) => {
                        self.rt.buckets.gen_matches(k)
                    }
                    _ => None,
                }
            } else {
                None
            };
            if self.adaptive.observe(matches.is_some()) {
                self.rt.bloom = Some(bloom_from_buckets(&self.rt.buckets));
                if self.typed_arity.is_some() {
                    self.typed_bloom_built += 1;
                } else {
                    record_hash_join_bloom_built();
                }
            }
            if let Some(matches) = matches {
                for bi in matches {
                    let build_row = &self.rt.build_rows[bi];
                    let mut prebuilt: Option<PlanRow> = None;
                    let passes = if let Some(pre) = self.residual_pre.as_ref() {
                        pre.eval_build_probe(self.rt.build_is_left, build_row, &probe_row)?
                    } else {
                        let joined = if self.rt.build_is_left {
                            join_rows(build_row, &probe_row, &self.join_lk)
                        } else {
                            join_rows(&probe_row, build_row, &self.join_lk)
                        };
                        let ok = self
                            .rt
                            .joined_residual
                            .as_ref()
                            .map(|expr| eval_plan_bool(expr, &joined))
                            .transpose()?
                            .unwrap_or(true);
                        if ok {
                            prebuilt = Some(joined);
                        }
                        ok
                    };
                    if !passes {
                        continue;
                    }
                    probe_matched = true;
                    if self.semi_or_anti {
                        if self.rt.join_type == JoinType::Semi {
                            self.pending
                                .push_back(self.project_semi_anti(probe_row.clone()));
                        }
                        break;
                    }
                    if self.build_preserved {
                        self.build_matched[bi] = true;
                    }
                    // S2 projection: when the residual pre-evaluation hits (or there is no residual), assemble
                    // the narrow row from the kept columns directly; the legacy "join-fully-then-evaluate" path
                    // already holds the full-width row and narrows it in place.
                    let joined = match prebuilt {
                        Some(mut full) => {
                            if let Some(projection) = self.rt.output_projection.as_deref() {
                                project_joined_row_in_place(&mut full, projection);
                            }
                            full
                        }
                        None => match self.rt.output_projection.as_deref() {
                            Some(projection) => {
                                if self.rt.build_is_left {
                                    join_rows_projected(
                                        build_row,
                                        &probe_row,
                                        &self.join_lk,
                                        projection,
                                    )
                                } else {
                                    join_rows_projected(
                                        &probe_row,
                                        build_row,
                                        &self.join_lk,
                                        projection,
                                    )
                                }
                            }
                            None => {
                                if self.rt.build_is_left {
                                    join_rows(build_row, &probe_row, &self.join_lk)
                                } else {
                                    join_rows(&probe_row, build_row, &self.join_lk)
                                }
                            }
                        },
                    };
                    self.pending.push_back(joined);
                }
            }
        }
        if !probe_matched {
            if self.semi_or_anti {
                if self.rt.join_type == JoinType::Anti {
                    self.pending.push_back(self.project_semi_anti(probe_row));
                }
            } else if self.probe_preserved {
                let extended = self.null_extend_probe(probe_row);
                self.pending.push_back(extended);
            }
        }
        Ok(())
    }

    /// Semi/Anti output = the probe (left) row; the projection index space is the left field order.
    fn project_semi_anti(&self, mut probe_row: PlanRow) -> PlanRow {
        if let Some(projection) = self.rt.output_projection.as_deref() {
            project_joined_row_in_place(&mut probe_row, projection);
        }
        probe_row
    }
}

impl PlanStream for ChainCore {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        loop {
            if let Some(row) = self.pending.pop_front() {
                self.rows_out += 1;
                return Ok(Some(row));
            }
            if self.upstream_done {
                if self.build_preserved {
                    while self.unmatched_cursor < self.rt.build_rows.len() {
                        let bi = self.unmatched_cursor;
                        self.unmatched_cursor += 1;
                        if !self.build_matched[bi] {
                            let row = self.null_extend_build(bi);
                            self.rows_out += 1;
                            return Ok(Some(row));
                        }
                    }
                }
                return Ok(None);
            }
            match self.upstream.next()? {
                None => {
                    self.upstream_done = true;
                }
                Some(probe_row) => self.process_probe(probe_row)?,
            }
        }
    }
}

impl Drop for ChainCore {
    fn drop(&mut self) {
        if let Some(arity) = self.typed_arity {
            record_hash_join_typed_key_fast(arity);
            if self.typed_bloom_built > 0 {
                super::plan::HASH_JOIN_BLOOM_BUILT
                    .fetch_add(self.typed_bloom_built, Ordering::Relaxed);
            }
        }
        record_hash_join_probe_rows(self.probe_rows_in);
        HASH_JOIN_STREAM_ROWS.fetch_add(self.rows_out, Ordering::Relaxed);
        if let (Some(plan_key), Some((started, io_start))) =
            (self.rt.display_plan_key, self.rt.analyze_started.take())
        {
            super::analyze_stats::record_key_with_io_snapshot(
                &self.db,
                plan_key,
                started.elapsed().as_nanos(),
                self.rows_out,
                io_start,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Pipeline worker
// ---------------------------------------------------------------------------

/// One worker per level: opens the downstream on its own thread (deeper levels recursively expand
/// on their own workers), builds the chain core, and ships rows out in batches. Teardown follows the same
/// protocol as ChannelPlanStream: consumer Drop → send fails → worker exits, cascading to the bottom.
struct ChainChannelStream {
    rx: Option<mpsc::Receiver<Result<Vec<PlanRow>, QueryError>>>,
    worker: Option<thread::JoinHandle<()>>,
    cancellation: crate::engine::cancel::CancellationSnapshot,
    current: std::vec::IntoIter<PlanRow>,
}

impl PlanStream for ChainChannelStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        loop {
            if let Some(row) = self.current.next() {
                return Ok(Some(row));
            }
            match self.rx.as_ref().expect("chain receiver present").recv() {
                Ok(Ok(batch)) => self.current = batch.into_iter(),
                Ok(Err(err)) => return Err(err),
                Err(_) => {
                    if let Some(worker) = self.worker.take() {
                        super::stream::join_stream_thread(worker)?;
                    }
                    return Ok(None);
                }
            }
        }
    }
}

impl Drop for ChainChannelStream {
    fn drop(&mut self) {
        self.cancellation
            .finish_channel_worker(&mut self.rx, &mut self.worker);
    }
}

fn spawn_level_worker(
    db: &Arc<Database>,
    txn: TxnView,
    runtime: LevelRuntime,
    levels: VecDeque<ChainLevel>,
    tail: PhysicalPlan,
    tail_display_keys: Option<super::plan::PlanStatsKeyTree>,
    prebuilt: VecDeque<Vec<PlanRow>>,
) -> ChainChannelStream {
    let batch_rows = super::stream::stream_batch_rows();
    let channel_batches = super::stream::stream_channel_batches();
    let (tx, rx) = mpsc::sync_channel::<Result<Vec<PlanRow>, QueryError>>(channel_batches);
    let db = Arc::clone(db);
    let context =
        super::execution_context::ExecutionContext::capture(txn).fork_worker_cancellation();
    let cancellation = context.cancellation().clone();

    let worker = thread::spawn(move || {
        let tx_err = tx.clone();
        let result = context.run(&db, || -> Result<(), QueryError> {
            let upstream = open_levels(&db, txn, levels, tail, tail_display_keys, true, prebuilt)?;
            let mut core = ChainCore::new(&db, runtime, upstream);
            let mut batch: Vec<PlanRow> = Vec::with_capacity(batch_rows);
            while let Some(row) = core.next()? {
                batch.push(row);
                if batch.len() >= batch_rows {
                    let chunk = std::mem::replace(&mut batch, Vec::with_capacity(batch_rows));
                    if tx.send(Ok(chunk)).is_err() {
                        // Consumer closed early (LIMIT/cancellation).
                        return Ok(());
                    }
                }
            }
            if !batch.is_empty() {
                let _ = tx.send(Ok(std::mem::take(&mut batch)));
            }
            Ok(())
        });
        if let Err(err) = result {
            let _ = tx_err.send(Err(err));
        }
    });

    ChainChannelStream {
        rx: Some(rx),
        worker: Some(worker),
        cancellation,
        current: Vec::new().into_iter(),
    }
}

#[cfg(test)]
mod stream_build_side_tests {
    use super::stream_build_side;
    use crate::parser::JoinType;
    use crate::planner::BuildSide;

    #[test]
    fn sqlproc_sus_chain_drop_waits_for_worker_teardown() {
        let (_tx, rx) = std::sync::mpsc::sync_channel(1);
        let finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = finished.clone();
        let worker = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            flag.store(true, std::sync::atomic::Ordering::Release);
        });
        let parent = crate::engine::cancel::CancellationSnapshot::capture();
        let stream = super::ChainChannelStream {
            rx: Some(rx),
            worker: Some(worker),
            cancellation: parent.fork_local(),
            current: Vec::new().into_iter(),
        };
        drop(stream);
        assert!(finished.load(std::sync::atomic::Ordering::Acquire));
        assert!(
            parent.check().is_ok(),
            "dropping a child must not cancel its query"
        );
    }

    /// WS-C: under memory pressure (`force_outer_null = true`) an outer join
    /// must build the null-supplying side (LEFT→right, RIGHT→left) regardless
    /// of the planner's pick, so the (possibly underestimated, actually large)
    /// preserved side streams as probe instead of being materialised as build.
    #[test]
    fn force_outer_null_flips_outer_join_build_to_null_side() {
        // LEFT: build the right (null-supplying) side, whatever the planner said.
        assert_eq!(
            stream_build_side(JoinType::Left, BuildSide::Left, true),
            BuildSide::Right
        );
        assert_eq!(
            stream_build_side(JoinType::Left, BuildSide::Right, true),
            BuildSide::Right
        );
        // RIGHT: build the left (null-supplying) side.
        assert_eq!(
            stream_build_side(JoinType::Right, BuildSide::Right, true),
            BuildSide::Left
        );
        // INNER/SEMI/ANTI: keep the planner's pick (no preserved side to protect).
        assert_eq!(
            stream_build_side(JoinType::Inner, BuildSide::Left, true),
            BuildSide::Left
        );
        assert_eq!(
            stream_build_side(JoinType::Semi, BuildSide::Right, true),
            BuildSide::Right
        );
    }

    /// Without the force flag the planner's choice is always preserved (the
    /// parallel chain path must be byte-for-byte unchanged).
    #[test]
    fn no_force_preserves_planner_build_side() {
        for jt in [
            JoinType::Left,
            JoinType::Right,
            JoinType::Inner,
            JoinType::Semi,
            JoinType::Anti,
        ] {
            assert_eq!(
                stream_build_side(jt, BuildSide::Left, false),
                BuildSide::Left
            );
            assert_eq!(
                stream_build_side(jt, BuildSide::Right, false),
                BuildSide::Right
            );
        }
    }
}
