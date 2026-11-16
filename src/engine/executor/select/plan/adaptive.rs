use super::*;

type AdaptiveIndexCandidate<'a> = (
    &'a PlanTable,
    &'a IndexSchema,
    Option<&'a Expr>,
    bool,
    Option<&'a [usize]>,
);

#[allow(clippy::too_many_arguments)]
fn execute_adaptive_inlj_candidate(
    db: &Arc<Database>,
    txn: TxnView,
    left: &PhysicalPlan,
    left_rows: &[PlanRow],
    join_type: crate::parser::JoinType,
    candidate: AdaptiveIndexCandidate<'_>,
    preferred: AdaptiveJoinChoice,
) -> Result<Vec<PlanRow>, QueryError> {
    let (right_table, right_index, condition, right_covering, right_projected_columns) = candidate;
    record_adaptive_choice(AdaptiveJoinChoice::IndexNestedLoop, preferred);
    if let Some(condition) = condition {
        let lookup_plan = compile_index_lookup_plan(condition, right_table, right_index)?;
        return execute_index_nested_loop_join_from_left_rows(
            db,
            txn,
            left,
            left_rows,
            right_table,
            right_index,
            join_type,
            &lookup_plan,
            right_covering,
            right_projected_columns,
            &[],
        );
    }
    let right_plan = inlj_fallback_right_seq_scan(right_table, right_index);
    let right_rows = execute_plan_rows(db, txn, &right_plan)?;
    execute_nested_loop_join_from_rows(
        db,
        txn,
        left,
        &right_plan,
        join_type,
        None,
        left_rows,
        &right_rows,
    )
}

pub(super) fn execute_adaptive_join(
    db: &Arc<Database>,
    txn: TxnView,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    join_type: crate::parser::JoinType,
    candidates: &[AdaptiveJoinCandidate],
    preferred: AdaptiveJoinChoice,
) -> Result<Vec<PlanRow>, QueryError> {
    record_adaptive_join_count();
    let index_candidate = candidates.iter().find_map(|candidate| {
        if let AdaptiveJoinCandidate::IndexNestedLoop {
            right_table,
            right_index,
            condition,
            right_covering,
            right_projected_columns,
            ..
        } = candidate
        {
            Some((
                right_table,
                right_index,
                condition.as_ref(),
                *right_covering,
                right_projected_columns.as_deref().map(|v| v.as_slice()),
            ))
        } else {
            None
        }
    });
    let hash_candidate = candidates.iter().find_map(|candidate| {
        if let AdaptiveJoinCandidate::Hash {
            left_keys,
            right_keys,
            null_safe,
            build_side,
            residual,
            ..
        } = candidate
        {
            Some((
                left_keys,
                right_keys,
                null_safe,
                *build_side,
                residual.as_ref(),
            ))
        } else {
            None
        }
    });
    let nested_condition = candidates
        .iter()
        .find_map(|candidate| {
            if let AdaptiveJoinCandidate::NestedLoop { condition, .. } = candidate {
                Some(condition.as_ref())
            } else {
                None
            }
        })
        .flatten();

    // A rejected candidate still owns the input it read. Carry it into the
    // next choice instead of reopening the child (which can be volatile).
    let mut materialized_left = None;
    if preferred == AdaptiveJoinChoice::IndexNestedLoop {
        if let Some(candidate) = index_candidate {
            let left_rows = execute_plan_rows(db, txn, left)?;
            if left_rows.len() <= adaptive_inlj_max_left_rows() {
                return execute_adaptive_inlj_candidate(
                    db, txn, left, &left_rows, join_type, candidate, preferred,
                );
            }
            materialized_left = Some(left_rows);
        }
    }

    if preferred == AdaptiveJoinChoice::NestedLoop {
        let left_rows = execute_plan_rows(db, txn, left)?;
        let estimated_right_rows = crate::planner::estimated_rows(right).ceil().max(1.0) as u64;
        let estimated_comparisons = (left_rows.len() as u64).saturating_mul(estimated_right_rows);
        if hash_candidate.is_none() || estimated_comparisons <= adaptive_nlj_max_comparisons() {
            let right_rows = execute_plan_rows(db, txn, right)?;
            let actual_comparisons =
                (left_rows.len() as u64).saturating_mul(right_rows.len() as u64);
            if hash_candidate.is_none() || actual_comparisons <= adaptive_nlj_max_comparisons() {
                record_adaptive_choice(AdaptiveJoinChoice::NestedLoop, preferred);
                return execute_nested_loop_join_from_rows(
                    db,
                    txn,
                    left,
                    right,
                    join_type,
                    nested_condition,
                    &left_rows,
                    &right_rows,
                );
            }
            if let Some((left_keys, right_keys, null_safe, build_side, residual)) = hash_candidate {
                let (build_rows, probe_rows) = match build_side {
                    BuildSide::Left => (left_rows, right_rows),
                    BuildSide::Right => (right_rows, left_rows),
                };
                increment_hash_join_count();
                record_adaptive_choice(AdaptiveJoinChoice::Hash, preferred);
                return execute_hash_join_from_rows(
                    db, txn, left, right, join_type, left_keys, right_keys, null_safe, build_side,
                    residual, build_rows, probe_rows, None,
                );
            }
        }
        materialized_left = Some(left_rows);
    }

    if let Some((left_keys, right_keys, null_safe, build_side, residual)) = hash_candidate {
        return execute_adaptive_hash_join(
            db,
            txn,
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            null_safe,
            build_side,
            residual,
            index_candidate,
            materialized_left,
            preferred,
        );
    }

    let left_rows = match materialized_left {
        Some(rows) => rows,
        None => execute_plan_rows(db, txn, left)?,
    };
    if let Some(candidate) = index_candidate {
        if hash_candidate.is_none()
            && left_rows.len() <= adaptive_inlj_max_left_rows()
            && adaptive_runtime_inlj_safe(left)
        {
            return execute_adaptive_inlj_candidate(
                db, txn, left, &left_rows, join_type, candidate, preferred,
            );
        }
    }

    let right_rows = execute_plan_rows(db, txn, right)?;

    record_adaptive_choice(AdaptiveJoinChoice::NestedLoop, preferred);
    execute_nested_loop_join_from_rows(
        db,
        txn,
        left,
        right,
        join_type,
        nested_condition,
        &left_rows,
        &right_rows,
    )
}

fn execute_adaptive_hash_join(
    db: &Arc<Database>,
    txn: TxnView,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    join_type: crate::parser::JoinType,
    left_keys: &[Expr],
    right_keys: &[Expr],
    null_safe: &[bool],
    build_side: BuildSide,
    residual: Option<&Expr>,
    index_candidate: Option<AdaptiveIndexCandidate<'_>>,
    mut materialized_left: Option<Vec<PlanRow>>,
    preferred: AdaptiveJoinChoice,
) -> Result<Vec<PlanRow>, QueryError> {
    let limits = hash_join_limits();
    let (build_plan, build_keys_arr, probe_plan, probe_keys_arr, build_is_left) = match build_side {
        BuildSide::Right => (right, right_keys, left, left_keys, false),
        BuildSide::Left => (left, left_keys, right, right_keys, true),
    };

    let probe_must_output_unmatched = probe_unmatched_must_output(join_type, build_side);
    if should_prefilter_hash_build(join_type, build_side, build_plan, probe_plan, &limits) {
        if materialized_left.is_none() {
            record_adaptive_choice(AdaptiveJoinChoice::Hash, preferred);
            // AdaptiveJoin has no join-level output projection.
            return execute_hash_join(
                db, txn, left, right, join_type, left_keys, right_keys, null_safe, build_side,
                residual, None,
            );
        }
        // Keep the ordinary hash join's columnar kernels when a candidate
        // already consumed the left child. A rejected preflight leaves those
        // rows intact for the scalar fallback below.
        if let Some(rows) = crate::engine::executor::select::vec_bridge::try_vec_hash_join(
            db,
            txn,
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            null_safe,
            build_side,
            residual,
            &mut materialized_left,
        )? {
            increment_hash_join_count();
            record_adaptive_choice(AdaptiveJoinChoice::Hash, preferred);
            return Ok(rows);
        }
        if !build_is_left {
            let probe_rows = materialized_left.take().expect("left input already read");
            let build_rows = execute_hash_join_build_from_probe(
                db,
                txn,
                build_plan,
                build_keys_arr,
                &probe_rows,
                probe_keys_arr,
                null_safe,
                limits.bloom_max_build_rows,
            )?;
            increment_hash_join_count();
            record_adaptive_choice(AdaptiveJoinChoice::Hash, preferred);
            return execute_hash_join_from_rows(
                db, txn, left, right, join_type, left_keys, right_keys, null_safe, build_side,
                residual, build_rows, probe_rows, None,
            );
        }
        // The build input has already been read. Continue build-first below;
        // reversing the scan order now would discard and execute it again.
    }

    let build_rows = match (build_is_left, materialized_left.take()) {
        (true, Some(rows)) => rows,
        (_, rows) => {
            materialized_left = rows;
            execute_plan_rows(db, txn, build_plan)?
        }
    };
    let build_bytes = estimate_rows_bytes(&build_rows);
    let build_too_big =
        build_rows.len() as u64 > limits.max_build_rows || build_bytes > limits.mem_limit_bytes;
    let estimated_build_rows = crate::planner::estimated_rows(build_plan).max(1.0);
    let estimated_build_bytes = (estimated_build_rows * build_plan.output_width()).max(1.0);
    let overshoot_factor = adaptive_build_overshoot_factor();
    let build_overshot_estimate = (build_rows.len() as f64)
        > estimated_build_rows * overshoot_factor
        || (build_bytes as f64) > estimated_build_bytes * overshoot_factor;
    let estimated_probe_rows = crate::planner::estimated_rows(probe_plan).ceil().max(1.0) as u64;
    let estimated_comparisons = (build_rows.len() as u64).saturating_mul(estimated_probe_rows);

    if build_is_left {
        if let Some((
            right_table,
            right_index,
            condition,
            right_covering,
            right_projected_columns,
        )) = index_candidate
        {
            if let Some(condition) = condition {
                let max_left_rows = adaptive_inlj_max_left_rows();
                let should_switch_to_inlj = build_too_big
                    || build_overshot_estimate
                    || build_rows.len() <= adaptive_small_build_rows();
                if should_switch_to_inlj && build_rows.len() <= max_left_rows {
                    if build_too_big {
                        record_hash_join_mem_exceeded();
                    }
                    let lookup_plan =
                        compile_index_lookup_plan(condition, right_table, right_index)?;
                    record_adaptive_choice(AdaptiveJoinChoice::IndexNestedLoop, preferred);
                    return execute_index_nested_loop_join_from_left_rows(
                        db,
                        txn,
                        left,
                        &build_rows,
                        right_table,
                        right_index,
                        join_type,
                        &lookup_plan,
                        right_covering,
                        right_projected_columns,
                        &[],
                    );
                }
            }
        }
    }

    let nlj_comparisons_ok = estimated_comparisons <= adaptive_nlj_max_comparisons();
    let build_is_tiny = build_rows.len() <= adaptive_small_build_rows();
    if (build_is_tiny || build_overshot_estimate || build_too_big) && nlj_comparisons_ok {
        if build_too_big {
            record_hash_join_mem_exceeded();
        }
        let probe_rows = match materialized_left.take() {
            Some(rows) => rows,
            None => execute_plan_rows(db, txn, probe_plan)?,
        };
        record_hash_join_fallback();
        record_adaptive_choice(AdaptiveJoinChoice::NestedLoop, preferred);
        let spec = build_hash_join_spec(
            left, right, join_type, left_keys, right_keys, null_safe, build_side, residual,
        );
        return execute_hash_join_nlj_fallback_from_spec(&spec, build_rows, probe_rows);
    }

    if build_too_big && limits.fallback_error {
        record_hash_join_mem_exceeded();
        return Err(error(crate::error::catalog::ER_UNKNOWN_ERROR,
            "hash join build side exceeds MYDB_HASH_JOIN_MEM_LIMIT_MB / MYDB_HASH_JOIN_MAX_BUILD_ROWS",
        ));
    }

    let inject_bloom = limits
        .inject_bloom_worthwhile(build_rows.len(), crate::planner::estimated_rows(probe_plan))
        && !probe_must_output_unmatched;
    let probe_rows = match materialized_left {
        Some(rows) => rows,
        None => execute_hash_join_probe(
            db,
            txn,
            probe_plan,
            &build_rows,
            build_keys_arr,
            probe_keys_arr,
            null_safe,
            inject_bloom,
        )?,
    };

    increment_hash_join_count();
    record_adaptive_choice(AdaptiveJoinChoice::Hash, preferred);
    execute_hash_join_from_rows(
        db, txn, left, right, join_type, left_keys, right_keys, null_safe, build_side, residual,
        build_rows, probe_rows, None,
    )
}

fn build_hash_join_spec<'a>(
    left: &'a PhysicalPlan,
    right: &'a PhysicalPlan,
    join_type: crate::parser::JoinType,
    left_keys: &'a [Expr],
    right_keys: &'a [Expr],
    null_safe: &'a [bool],
    build_side: BuildSide,
    residual: Option<&'a Expr>,
) -> HashJoinSpec<'a> {
    let (build_plan, build_keys, probe_plan, probe_keys, build_is_left) = match build_side {
        BuildSide::Right => (right, right_keys, left, left_keys, false),
        BuildSide::Left => (left, left_keys, right, right_keys, true),
    };
    let (build_key_plan, probe_key_plan) =
        compile_hash_join_key_plans(build_plan, build_keys, probe_plan, probe_keys);
    HashJoinSpec {
        left_plan: left,
        right_plan: right,
        join_type,
        build_keys,
        probe_keys,
        build_key_plan,
        probe_key_plan,
        null_safe,
        build_is_left,
        residual,
        output_projection: None,
    }
}

fn inlj_fallback_right_seq_scan(
    right_table: &PlanTable,
    right_index: &IndexSchema,
) -> PhysicalPlan {
    PhysicalPlan::SeqScan {
        table: right_table.clone(),
        filter: None,
        estimated_rows: 1.0,
        access_rows: 1.0,
        cost: crate::engine::cbo::Cost::scalar(1.0),
        possible_keys: vec![right_index.name.clone()],
        runtime_filters: Vec::new(),
        hint_note: None,
        projected_columns: None,
    }
}

fn adaptive_inlj_max_left_rows() -> usize {
    crate::util::env::var("MYDB_ADAPTIVE_JOIN_INLJ_MAX_LEFT_ROWS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8192)
}

fn adaptive_small_build_rows() -> usize {
    crate::util::env::var("MYDB_ADAPTIVE_JOIN_SMALL_BUILD_ROWS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(64)
}

pub(super) fn adaptive_nlj_max_comparisons() -> u64 {
    crate::util::env::var("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1_000_000)
}

fn adaptive_build_overshoot_factor() -> f64 {
    crate::util::env::var("MYDB_ADAPTIVE_JOIN_BUILD_OVERSHOOT_FACTOR")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v >= 1.0)
        .unwrap_or(4.0)
}

fn adaptive_runtime_inlj_safe(left: &PhysicalPlan) -> bool {
    matches!(
        left,
        PhysicalPlan::SeqScan { .. }
            | PhysicalPlan::IndexLookup { .. }
            | PhysicalPlan::IndexRangeScan { .. }
            | PhysicalPlan::IndexFastScan { .. }
            | PhysicalPlan::IndexInLookup { .. }
            | PhysicalPlan::CteScan { .. }
            | PhysicalPlan::Exchange { .. }
            | PhysicalPlan::ParallelSeqScan { .. }
            | PhysicalPlan::ParallelIndexFastScan { .. }
            | PhysicalPlan::ParallelIndexRangeScan { .. }
    )
}

#[cfg(test)]
#[path = "adaptive_tests.rs"]
mod tests;
