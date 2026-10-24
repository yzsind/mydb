//! nested loop execution and preparation.

use super::*;

pub(super) struct NestedLoopStream {
    db: Arc<Database>,
    txn: TxnView,
    left: Box<dyn PlanStream>,
    right: Vec<PlanRow>,
    current: Option<PlanRow>,
    cursor: usize,
    condition: Option<Expr>,
    compiled: Option<CompiledPlanPredicate>,
    lookup: Arc<RowSchema>,
    eval_ctx: EvalQueryContext,
}

impl PlanStream for NestedLoopStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        if self.right.is_empty() {
            return Ok(None);
        }
        loop {
            crate::engine::cancel::check_cancelled()?;
            if self.current.is_none() || self.cursor == self.right.len() {
                self.current = self.left.next()?;
                self.cursor = 0;
            }
            let Some(left) = &self.current else {
                return Ok(None);
            };
            let row = join_rows(left, &self.right[self.cursor], &self.lookup);
            self.cursor += 1;
            if let Some(condition) = &self.condition {
                let compiled = self
                    .compiled
                    .get_or_insert_with(|| CompiledPlanPredicate::compile_once(condition, &row));
                if !compiled.eval_bool_with_db(&self.db, self.txn, &row, &mut self.eval_ctx)? {
                    continue;
                }
            }
            return Ok(Some(row));
        }
    }
}

pub(super) fn open_nested_loop_stream(
    db: &Arc<Database>,
    txn: TxnView,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    condition: Option<&Expr>,
) -> Result<Box<dyn PlanStream>, QueryError> {
    record_nlj_execution();
    let mut left = execute_plan_stream_limited(db, txn, left, usize::MAX)?;
    let current = left.next()?;
    let right = if current.is_some() {
        execute_plan_rows(db, txn, right)?
    } else {
        Vec::new()
    };
    Ok(Box::new(NestedLoopStream {
        db: db.clone(),
        txn,
        left,
        right,
        current,
        cursor: 0,
        condition: condition.cloned(),
        compiled: None,
        lookup: new_join_lookup(),
        eval_ctx: EvalQueryContext::default(),
    }))
}

pub(in crate::engine::executor::select) fn execute_nested_loop_join(
    db: &Arc<Database>,
    txn: TxnView,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    join_type: crate::parser::JoinType,
    condition: Option<&Expr>,
) -> Result<Vec<PlanRow>, QueryError> {
    let left_rows = execute_plan_rows(db, txn, left)?;
    let right_rows = execute_plan_rows(db, txn, right)?;
    // Executor-level equi-join fast path: the plan is still NestedLoopJoin, but when the condition carries
    // extractable equality keys and |L|×|R| exceeds the comparisons NLJ can bear, execute by the hash join
    // algorithm (isomorphic to execute_adaptive_join's runtime switch). Non-extractable conditions or
    // small sizes take the naive double loop. `MYDB_NLJ_HASH=off` disables it for differential testing.
    let comparisons = (left_rows.len() as u64).saturating_mul(right_rows.len() as u64);
    if comparisons > adaptive_nlj_max_comparisons()
        && !nlj_hash_fast_path_disabled()
        && matches!(
            join_type,
            crate::parser::JoinType::Inner
                | crate::parser::JoinType::Left
                | crate::parser::JoinType::Right
                | crate::parser::JoinType::Semi
                | crate::parser::JoinType::Anti
        )
    {
        if let Some((left_keys, right_keys, null_safe, residual)) = condition
            .and_then(|expr| crate::planner::extract_join_keys_for_plans(left, right, expr))
        {
            let build_side =
                nlj_hash_build_side(join_type, left_rows.len() as f64, right_rows.len() as f64);
            let (build_rows, probe_rows) = match build_side {
                BuildSide::Left => (left_rows, right_rows),
                BuildSide::Right => (right_rows, left_rows),
            };
            increment_hash_join_count();
            record_nlj_hash_fast_path();
            return execute_hash_join_from_rows(
                db,
                txn,
                left,
                right,
                join_type,
                &left_keys,
                &right_keys,
                &null_safe,
                build_side,
                residual.as_ref(),
                build_rows,
                probe_rows,
                None,
            );
        }
    }
    execute_nested_loop_join_from_rows(
        db,
        txn,
        left,
        right,
        join_type,
        condition,
        &left_rows,
        &right_rows,
    )
}

pub(super) fn nlj_hash_fast_path_disabled() -> bool {
    matches!(
        crate::util::env::var("MYDB_NLJ_HASH")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no" | "disable" | "disabled"
    )
}

/// WS-N1 Block-NL kill switch: `MYDB_NLJ_BLOCK=off` forces the legacy per-pair
/// main loop (differential ground truth).
pub(super) fn nlj_block_disabled() -> bool {
    matches!(
        crate::util::env::var("MYDB_NLJ_BLOCK")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no" | "disable" | "disabled"
    )
}

/// WS-N3 vectorized-residual kill switch: `MYDB_NLJ_VECTORIZE=off` turns off SIMD numeric
/// comparison, keeping only the row-wise block-major path (the row-wise path remains the vectorized path's differential oracle).
pub(super) fn nlj_vectorize_disabled() -> bool {
    matches!(
        crate::util::env::var("MYDB_NLJ_VECTORIZE")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no" | "disable" | "disabled"
    )
}

/// WS-N1/N3 Block-NL outer block size. Under block-major, B truly matters: the inner side is
/// streamed just once per outer block, so the re-scan count = ⌈|left|/B⌉; the larger B is, the less inner
/// memory traffic; but the B outer rows' column lanes (vectorized) / pointers (row-wise) should stay resident
/// in L1/L2, and too large spills instead. The default 64 is the starting point of the platform region. Out-of-range env values are clamped to [8, 4096].
pub(super) fn nlj_block_outer_size() -> usize {
    #[cfg(not(test))]
    let default_rows = 64;
    #[cfg(test)]
    let default_rows = 8;
    let raw = crate::util::env::var("MYDB_NLJ_BLOCK_OUTER")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(default_rows);
    raw.clamp(8, 4096)
}

/// WS-N2 bare-NLJ outer parallelism. Mirrors `inlj_outer_parallelism` but the gate uses
/// the comparison count (|L|×|R|), matching NLJ's CPU model:
///   * `comparisons < MYDB_NLJ_OUTER_PARALLEL_THRESHOLD` (default 1e7) → serial;
///   * otherwise min(`MYDB_NLJ_OUTER_PARALLELISM` (unset → `MYDB_PARALLEL_DEGREE`,
///     then unset → 2), left_rows), clamped to [1, 8].
///
/// The single-threaded short-circuit returns 1 early (saving an atomic env var read).
pub(super) fn nlj_outer_parallelism(left_rows: usize, right_rows: usize) -> usize {
    let comparisons = (left_rows as u64).saturating_mul(right_rows as u64);
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
        .unwrap_or_else(|| crate::planner::default_parallel_degree() as usize);
    requested.clamp(1, 8).min(left_rows.max(1))
}

/// Runtime build-side selection: row counts are exact, mirroring the planner's `choose_build_side` rules
/// but without the "unreliable statistics" branch. SEMI/ANTI must use build=Right (probe rows =
/// left rows; the executor emits on that assumption).
pub(super) fn nlj_hash_build_side(
    join_type: crate::parser::JoinType,
    left_rows: f64,
    right_rows: f64,
) -> BuildSide {
    const BUILD_FLIP_RATIO: f64 = 0.7;
    match join_type {
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti => BuildSide::Right,
        crate::parser::JoinType::Left => {
            if left_rows < right_rows * BUILD_FLIP_RATIO {
                BuildSide::Left
            } else {
                BuildSide::Right
            }
        }
        crate::parser::JoinType::Right => {
            if right_rows < left_rows * BUILD_FLIP_RATIO {
                BuildSide::Right
            } else {
                BuildSide::Left
            }
        }
        _ => {
            if left_rows < right_rows {
                BuildSide::Left
            } else {
                BuildSide::Right
            }
        }
    }
}

pub(super) fn execute_nested_loop_join_from_rows(
    db: &Arc<Database>,
    txn: TxnView,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    join_type: crate::parser::JoinType,
    condition: Option<&Expr>,
    left_rows: &[PlanRow],
    right_rows: &[PlanRow],
) -> Result<Vec<PlanRow>, QueryError> {
    record_nlj_execution();
    // G7 safety net: a join with no condition is a true cross product and
    // materialises |left| × |right| rows into `out`. That growth is otherwise
    // unaccounted, so a degenerate cartesian (e.g. a query the planner could
    // not form a join graph for) would OOM-kill the whole process instead of
    // failing the one query. Charge the worst-case output size up front so it
    // fails fast with 1037 (ER_OUT_OF_SORTMEMORY) before allocating. Filtered
    // joins only retain matching rows (bounded by selectivity), so they are not
    // charged here — only the unconditional cross product is.
    let _cross_product_charge = if condition.is_none() {
        let out_rows = match join_type {
            crate::parser::JoinType::Semi | crate::parser::JoinType::Anti => left_rows.len(),
            _ => left_rows.len().saturating_mul(right_rows.len()),
        };
        Some(crate::engine::executor::mem_budget::force_charge_scoped(
            out_rows.saturating_mul(crate::engine::executor::mem_budget::ESTIMATED_ROW_BYTES),
        )?)
    } else {
        None
    };
    // FULL needs both preservation bitmaps. Keep it in one explicit generic
    // loop so the block-NL and scalar fallback paths cannot accidentally
    // implement only LEFT or only RIGHT semantics. This is also the row
    // oracle for the columnar hash join's dual NULL-extension pass.
    if join_type == crate::parser::JoinType::Full {
        let join_lk = new_join_lookup();
        let mut residual_pre = LazyJoinedResidualPre::new();
        let mut eval_ctx = EvalQueryContext::default();
        let mut right_matched = vec![false; right_rows.len()];
        let mut out = Vec::new();
        let mut pairs_compared = 0u64;
        let mut pairs_kept = 0u64;
        for left_row in left_rows {
            let mut left_matched = false;
            for (right_index, right_row) in right_rows.iter().enumerate() {
                pairs_compared = pairs_compared.saturating_add(1);
                let accepted = match condition {
                    None => true,
                    Some(condition) => match residual_pre.get(condition, left_row, right_row) {
                        Some(pre) => pre.eval_pair(left_row, right_row)?,
                        None => {
                            let joined = join_rows(left_row, right_row, &join_lk);
                            eval_plan_bool_with_db(db, txn, condition, &joined, &mut eval_ctx)?
                        }
                    },
                };
                if !accepted {
                    continue;
                }
                pairs_kept = pairs_kept.saturating_add(1);
                left_matched = true;
                right_matched[right_index] = true;
                push_nlj_materialized_row(&mut out, join_rows(left_row, right_row, &join_lk))?;
            }
            if !left_matched {
                push_nlj_materialized_row(&mut out, null_extend_right(left_row, right))?;
            }
        }
        for (right_index, right_row) in right_rows.iter().enumerate() {
            if !right_matched[right_index] {
                push_nlj_materialized_row(&mut out, null_extend_left(right_row, left))?;
            }
        }
        record_nlj_pairs(pairs_compared, pairs_kept);
        return Ok(out);
    }
    // WS-N1 Block-NL: when the condition compiles to an ExprProgram, the outer side iterates in blocks,
    // and the whole (outer block × inner) shares one `with_eval_scratch` RefCell borrow,
    // calling the compiled eval directly (removing LazyJoinedResidualPre's Option::get indirection
    // and the per-pair scratch borrows). Falling back to None takes the old per-pair path.
    if !nlj_block_disabled() {
        if let Some(rows) = try_execute_nested_loop_join_block(
            db, txn, left, right, join_type, condition, left_rows, right_rows,
        )? {
            return Ok(rows);
        }
    }
    let mut pairs_compared: u64 = 0;
    let mut pairs_kept: u64 = 0;
    let mut out = Vec::new();
    let join_lk = new_join_lookup();
    // P0-5 isomorphism: evaluate the condition on the (left, right) pair via the compiled program first,
    // and materialize the joined row only on a hit -- rejected pairs never touch join_rows / drop at all.
    // Shapes the expression compiler doesn't support fall back to the original "join-then-evaluate" path (legacy is the correctness baseline).
    let mut residual_pre = LazyJoinedResidualPre::new();
    // Conditions the compiler can't lower (e.g. a `col IN (subquery)`
    // join residual) fall through `residual_pre` to the interpreter; it
    // must carry (db, txn) so the subquery executes instead of silently
    // evaluating to false.
    let mut eval_ctx = EvalQueryContext::default();
    if join_type == crate::parser::JoinType::Right {
        let mut right_matched = vec![false; right_rows.len()];
        for left_row in left_rows {
            for (ri, right_row) in right_rows.iter().enumerate() {
                pairs_compared += 1;
                if let Some(cond) = condition {
                    match residual_pre.get(cond, left_row, right_row) {
                        Some(pre) => {
                            if !pre.eval_pair(left_row, right_row)? {
                                continue;
                            }
                        }
                        None => {
                            let joined = join_rows(left_row, right_row, &join_lk);
                            if eval_plan_bool_with_db(db, txn, cond, &joined, &mut eval_ctx)? {
                                pairs_kept += 1;
                                right_matched[ri] = true;
                                push_nlj_materialized_row(&mut out, joined)?;
                            }
                            continue;
                        }
                    }
                }
                pairs_kept += 1;
                right_matched[ri] = true;
                push_nlj_materialized_row(&mut out, join_rows(left_row, right_row, &join_lk))?;
            }
        }
        for (ri, right_row) in right_rows.iter().enumerate() {
            if !right_matched[ri] {
                push_nlj_materialized_row(&mut out, null_extend_left(right_row, left))?;
            }
        }
        record_nlj_pairs(pairs_compared, pairs_kept);
        return Ok(out);
    }
    let semi_or_anti = matches!(
        join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    );
    for left_row in left_rows {
        let mut matched = false;
        for right_row in right_rows {
            pairs_compared += 1;
            if let Some(cond) = condition {
                match residual_pre.get(cond, left_row, right_row) {
                    Some(pre) => {
                        if !pre.eval_pair(left_row, right_row)? {
                            continue;
                        }
                    }
                    None => {
                        let joined = join_rows(left_row, right_row, &join_lk);
                        if eval_plan_bool_with_db(db, txn, cond, &joined, &mut eval_ctx)? {
                            pairs_kept += 1;
                            matched = true;
                            if !semi_or_anti {
                                push_nlj_materialized_row(&mut out, joined)?;
                            } else if join_type == crate::parser::JoinType::Semi {
                                // Phase B #B1: emit the left row at most once.
                                push_nlj_materialized_row(&mut out, left_row.clone())?;
                                break;
                            }
                            // Anti: keep scanning to confirm match.
                        }
                        continue;
                    }
                }
            }
            pairs_kept += 1;
            matched = true;
            if !semi_or_anti {
                push_nlj_materialized_row(&mut out, join_rows(left_row, right_row, &join_lk))?;
            } else if join_type == crate::parser::JoinType::Semi {
                // Phase B #B1: emit the left row at most once.
                push_nlj_materialized_row(&mut out, left_row.clone())?;
                break;
            }
            // Anti: keep scanning to confirm match; we just suppress the row.
        }
        if !matched {
            match join_type {
                crate::parser::JoinType::Left => {
                    push_nlj_materialized_row(&mut out, null_extend_right(left_row, right))?;
                }
                crate::parser::JoinType::Anti => {
                    push_nlj_materialized_row(&mut out, left_row.clone())?;
                }
                _ => {}
            }
        }
    }
    record_nlj_pairs(pairs_compared, pairs_kept);
    Ok(out)
}

/// WS-N1/N2 Block-NL chunk processing body. Handles one outer subset × the whole inner,
/// returning `(chunk_out, right_matched_optional, pairs_evaluated, pairs_kept)`.
/// For RIGHT, `right_matched` is a per-bit inner match bitmap (the parallel wrapper ORs the
/// multiple workers' bitmaps, then `null_extend_left`s the unmatched inner rows).
///
/// LEFT/ANTI unmatched handling completes inside this function (per-outer local, unrelated across
/// workers → just emit directly); SEMI's break-on-hit is likewise per-outer local.
#[allow(clippy::too_many_arguments)]
pub(super) fn nlj_block_process_chunk(
    right_plan: &PhysicalPlan,
    left_rows: &[PlanRow],
    right_rows: &[PlanRow],
    pre: &JoinedResidualPre,
    join_type: crate::parser::JoinType,
    block_size: usize,
    vectorized: Option<&VectorizedJoinPredicate>,
) -> Result<(Vec<PlanRow>, Option<Vec<bool>>, u64, u64), QueryError> {
    let join_lk = new_join_lookup();
    let semi_or_anti = matches!(
        join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    );
    crate::engine::expr_program::with_eval_scratch(|scratch| {
        let mut out: Vec<PlanRow> = Vec::new();
        let mut pairs_evaluated: u64 = 0;
        let mut pairs_kept: u64 = 0;
        let mut right_matched: Option<Vec<bool>> =
            (join_type == crate::parser::JoinType::Right).then(|| vec![false; right_rows.len()]);
        // Existence-only shapes (SEMI/ANTI) only need to know whether each outer has matched; once matched,
        // skip that outer's later comparisons (keeping legacy early-break's core benefit);
        // emit-all shapes (INNER/LEFT/RIGHT) materialize every matching pair into the corresponding outer's bucket.
        let existence_only = semi_or_anti;
        // Per-outer output buckets + matched flags, reused across blocks (capacity kept,
        // avoiding a reallocation per block).
        let mut buckets: Vec<Vec<PlanRow>> = Vec::new();
        let mut matched: Vec<bool> = Vec::new();

        // True block-major: the inner side is hoisted to the middle layer, and each outer block streams the
        // inner just once (re-scan count drops from |left| to ⌈|left|/block_size⌉). Per-outer buckets are replayed
        // in outer order at flush time, reproducing legacy's outer-major output order and thus matching the
        // `MYDB_NLJ_BLOCK=off` per-pair oracle row for row.
        for outer_block in left_rows.chunks(block_size) {
            let n = outer_block.len();
            if buckets.len() < n {
                buckets.resize_with(n, Vec::new);
            }
            for bucket in buckets.iter_mut().take(n) {
                bucket.clear();
            }
            matched.clear();
            matched.resize(n, false);

            // Fill this block's per-outer buckets + matched. The SIMD path (if available) calls the const
            // kernels directly for each inner row's B-bit mask; otherwise evaluate pair by pair. Both paths'
            // charge follows the materialization point (rows are allocated here); the flush stage only moves pointers.
            let filled_by_simd = if let Some(vp) = vectorized {
                nlj_simd_fill_block(
                    vp,
                    outer_block,
                    right_rows,
                    existence_only,
                    &join_lk,
                    &mut buckets,
                    &mut matched,
                    right_matched.as_mut(),
                    pre,
                    scratch,
                    &mut pairs_evaluated,
                    &mut pairs_kept,
                )?
            } else {
                false
            };
            if !filled_by_simd {
                nlj_rowwise_fill_block(
                    pre,
                    outer_block,
                    right_rows,
                    existence_only,
                    &join_lk,
                    &mut buckets,
                    &mut matched,
                    right_matched.as_mut(),
                    scratch,
                    &mut pairs_evaluated,
                    &mut pairs_kept,
                )?;
            }

            for (i, left_row) in outer_block.iter().enumerate() {
                match join_type {
                    crate::parser::JoinType::Semi => {
                        if matched[i] {
                            push_nlj_materialized_row(&mut out, left_row.clone())?;
                        }
                    }
                    crate::parser::JoinType::Anti => {
                        if !matched[i] {
                            push_nlj_materialized_row(&mut out, left_row.clone())?;
                        }
                    }
                    crate::parser::JoinType::Left => {
                        if matched[i] {
                            out.append(&mut buckets[i]);
                        } else {
                            push_nlj_materialized_row(
                                &mut out,
                                null_extend_right(left_row, right_plan),
                            )?;
                        }
                    }
                    _ => {
                        // Inner / Right: matching pairs are already in the buckets. Right's unmatched right
                        // rows are appended by the caller after merging right_matched.
                        out.append(&mut buckets[i]);
                    }
                }
            }
        }

        Ok((out, right_matched, pairs_evaluated, pairs_kept))
    })
}

/// Row-wise (per-pair) fill of one outer block's per-outer match buckets +
/// `matched` flags. This is the block-major NLJ's non-vectorized path: the
/// fallback when no SIMD term is available, and the per-block fallback when
/// SIMD lane extraction fails. Streams `right_rows` once for the whole block
/// (inner in the middle loop), so the inner re-scan count is ⌈|left|/B⌉.
#[allow(clippy::too_many_arguments)]
pub(super) fn nlj_rowwise_fill_block(
    pre: &JoinedResidualPre,
    outer_block: &[PlanRow],
    right_rows: &[PlanRow],
    existence_only: bool,
    join_lk: &std::sync::Arc<RowSchema>,
    buckets: &mut [Vec<PlanRow>],
    matched: &mut [bool],
    mut right_matched: Option<&mut Vec<bool>>,
    scratch: &mut crate::engine::expr_program::EvalScratch,
    pairs_evaluated: &mut u64,
    pairs_kept: &mut u64,
) -> Result<(), QueryError> {
    for (ri, right_row) in right_rows.iter().enumerate() {
        for (i, left_row) in outer_block.iter().enumerate() {
            if existence_only && matched[i] {
                continue;
            }
            *pairs_evaluated += 1;
            if !pre.eval_pair_with_scratch(left_row, right_row, scratch)? {
                continue;
            }
            *pairs_kept += 1;
            matched[i] = true;
            if let Some(rm) = right_matched.as_deref_mut() {
                rm[ri] = true;
            }
            if !existence_only {
                push_nlj_materialized_row(
                    &mut buckets[i],
                    join_rows(left_row, right_row, join_lk),
                )?;
            }
        }
    }
    Ok(())
}

// ===================== Vectorized join residual (BNL SIMD) =====================
//
// A pure NLJ residual that is a conjunction of same-lane column-vs-column
// comparisons (`a.x < b.y AND a.z = b.w`, …) is evaluated by SIMD const
// kernels in the block-major loop: for one fixed inner row, the inner column
// is a scalar and the outer block's column is an i64/f64 slice, so each
// predicate term is one `cmp_<lane>_const_<op>_simd` call producing a B-bit
// mask. This amortises the `ExprProgram` per-pair interpreter dispatch across
// the whole outer block. Non-vectorizable conjuncts (constants, cross-side
// expressions, Decimal/string/function operands, `<=>`) stay in `residual`
// and are evaluated per-pair only on the rows the SIMD mask kept.

#[derive(Clone, Copy)]
pub(super) enum SimdCmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl SimdCmpOp {
    pub(super) fn from_binary(op: crate::parser::BinaryOp) -> Option<Self> {
        use crate::parser::BinaryOp;
        Some(match op {
            BinaryOp::Eq => SimdCmpOp::Eq,
            BinaryOp::NotEq => SimdCmpOp::Ne,
            BinaryOp::Lt => SimdCmpOp::Lt,
            BinaryOp::LtEq => SimdCmpOp::Le,
            BinaryOp::Gt => SimdCmpOp::Gt,
            BinaryOp::GtEq => SimdCmpOp::Ge,
            // NullSafeEq (<=>) has NULL-matches-NULL semantics that the
            // null-bitmap AND path can't express — leave it to the residual.
            _ => return None,
        })
    }

    /// Swap operand order: applied when the parsed `left` operand resolved
    /// to the NLJ-right column, so the canonical `outer OP inner` flips.
    pub(super) fn flip(self) -> Self {
        match self {
            SimdCmpOp::Eq => SimdCmpOp::Eq,
            SimdCmpOp::Ne => SimdCmpOp::Ne,
            SimdCmpOp::Lt => SimdCmpOp::Gt,
            SimdCmpOp::Le => SimdCmpOp::Ge,
            SimdCmpOp::Gt => SimdCmpOp::Lt,
            SimdCmpOp::Ge => SimdCmpOp::Le,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub(super) enum SimdLane {
    I64,
    F64,
}

/// Map a declared column type to a SIMD lane; unsupported types (Decimal,
/// temporal, string, …) return `None` so the term falls back to per-pair.
pub(super) fn simd_lane_of(ct: ColumnType) -> Option<SimdLane> {
    match ct {
        ColumnType::Tiny
        | ColumnType::Short
        | ColumnType::MediumInt
        | ColumnType::Long
        | ColumnType::LongLong => Some(SimdLane::I64),
        ColumnType::Float | ColumnType::Double => Some(SimdLane::F64),
        _ => None,
    }
}

/// One vectorizable conjunct, normalised to `outer_col OP inner_col` on a
/// single numeric lane. `left_col` indexes a `left_rows` row, `right_col`
/// a `right_rows` row.
#[derive(Clone, Copy)]
pub(super) struct SimdTerm {
    pub(super) left_col: usize,
    pub(super) right_col: usize,
    pub(super) op: SimdCmpOp,
    pub(super) lane: SimdLane,
}

pub(super) struct VectorizedJoinPredicate {
    pub(super) simd_terms: Vec<SimdTerm>,
    pub(super) residual: Option<JoinedResidualPre>,
}

/// One outer block's column values for a single SIMD term, hoisted out of
/// the inner-row loop (extracted once per block, reused across all inner
/// rows). `nulls` is `None` when the block has no NULL in this column.
pub(super) enum OuterLane {
    I64 {
        data: Vec<i64>,
        nulls: Option<crate::engine::executor::vec::bitmap::Bitmap>,
    },
    F64 {
        data: Vec<f64>,
        nulls: Option<crate::engine::executor::vec::bitmap::Bitmap>,
    },
}

pub(super) enum InnerScalar {
    I64(i64),
    F64(f64),
    /// Inner column is NULL: `x OP NULL` is UNKNOWN → no row matches.
    Null,
    /// Inner value's runtime type doesn't fit the lane (e.g. lane is I64
    /// but the value is a Decimal). Fall the whole inner row back to
    /// per-pair evaluation.
    Bad,
}

pub(super) fn simd_extract_col_ref(expr: &Expr) -> Option<(Option<&str>, &str)> {
    match expr {
        Expr::Identifier(name) => Some((None, name.as_str())),
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [col] => Some((None, col.as_str())),
            [.., qual, col] => Some((Some(qual.as_str()), col.as_str())),
            [] => None,
        },
        _ => None,
    }
}

/// Local conjunction split (planner's `split_conjuncts` is module-private).
pub(super) fn simd_split_conjuncts(expr: &Expr, out: &mut Vec<Expr>) {
    if let Expr::Binary {
        left,
        op: crate::parser::BinaryOp::And,
        right,
    } = expr
    {
        simd_split_conjuncts(left, out);
        simd_split_conjuncts(right, out);
    } else {
        out.push(expr.clone());
    }
}

pub(super) fn simd_combine_and(conjuncts: Vec<Expr>) -> Expr {
    let mut iter = conjuncts.into_iter();
    let mut acc = iter.next().expect("non-empty conjunct list");
    for next in iter {
        acc = Expr::Binary {
            left: Box::new(acc),
            op: crate::parser::BinaryOp::And,
            right: Box::new(next),
        };
    }
    acc
}

impl VectorizedJoinPredicate {
    /// Best-effort compile. `None` ⇒ no vectorizable term (caller uses the
    /// row-wise block-major path) or the leftover residual failed to compile.
    pub(super) fn compile(
        condition: &Expr,
        left_template: &PlanRow,
        right_template: &PlanRow,
    ) -> Option<Self> {
        let left_len = left_template.fields.len();
        let mut columns = Vec::with_capacity(left_len + right_template.fields.len());
        for field in left_template
            .fields
            .iter()
            .chain(right_template.fields.iter())
        {
            columns.push(field.meta.input_column());
        }
        let schema = crate::engine::expr_program::InputSchema::new(columns);

        let mut conjuncts: Vec<Expr> = Vec::new();
        simd_split_conjuncts(condition, &mut conjuncts);

        let mut simd_terms = Vec::new();
        let mut residual_conjuncts: Vec<Expr> = Vec::new();
        for conj in conjuncts {
            match Self::try_simd_term(&conj, &schema, left_len, left_template, right_template) {
                Some(term) => simd_terms.push(term),
                None => residual_conjuncts.push(conj),
            }
        }
        if simd_terms.is_empty() {
            return None;
        }
        let residual = if residual_conjuncts.is_empty() {
            None
        } else {
            let residual_expr = simd_combine_and(residual_conjuncts);
            Some(JoinedResidualPre::compile(
                &residual_expr,
                left_template,
                right_template,
            )?)
        };
        Some(Self {
            simd_terms,
            residual,
        })
    }

    pub(super) fn try_simd_term(
        conj: &Expr,
        schema: &crate::engine::expr_program::InputSchema,
        left_len: usize,
        left_template: &PlanRow,
        right_template: &PlanRow,
    ) -> Option<SimdTerm> {
        let Expr::Binary { left, op, right } = conj else {
            return None;
        };
        let op = SimdCmpOp::from_binary(*op)?;
        let (lq, lc) = simd_extract_col_ref(left)?;
        let (rq, rc) = simd_extract_col_ref(right)?;
        let li = schema.column_index(lq, lc).ok()?;
        let ri = schema.column_index(rq, rc).ok()?;
        // Exactly one side must be NLJ-left (< left_len), the other NLJ-right.
        let (left_col, right_col, op) = if li < left_len && ri >= left_len {
            (li, ri - left_len, op)
        } else if li >= left_len && ri < left_len {
            (ri, li - left_len, op.flip())
        } else {
            return None;
        };
        let outer_ct = left_template.fields.get(left_col)?.column_type;
        let inner_ct = right_template.fields.get(right_col)?.column_type;
        let lane = simd_lane_of(outer_ct)?;
        if simd_lane_of(inner_ct)? != lane {
            return None;
        }
        Some(SimdTerm {
            left_col,
            right_col,
            op,
            lane,
        })
    }
}

pub(super) fn simd_extract_outer_lane(
    outer_block: &[PlanRow],
    term: &SimdTerm,
) -> Option<OuterLane> {
    use crate::engine::executor::vec::bitmap::Bitmap;
    let n = outer_block.len();
    match term.lane {
        SimdLane::I64 => {
            let mut data = Vec::with_capacity(n);
            let mut nulls: Option<Bitmap> = None;
            for (i, row) in outer_block.iter().enumerate() {
                match row.fields.get(term.left_col)?.value_ref() {
                    Value::Int(v) => data.push(*v),
                    Value::UInt(u) if *u <= i64::MAX as u64 => data.push(*u as i64),
                    Value::Null => {
                        data.push(0);
                        nulls
                            .get_or_insert_with(|| Bitmap::new_all_valid(n))
                            .set(i, false);
                    }
                    // UInt overflow / Decimal / anything else: the whole
                    // block falls back to per-pair (caller sees `false`).
                    _ => return None,
                }
            }
            Some(OuterLane::I64 { data, nulls })
        }
        SimdLane::F64 => {
            let mut data = Vec::with_capacity(n);
            let mut nulls: Option<Bitmap> = None;
            for (i, row) in outer_block.iter().enumerate() {
                match row.fields.get(term.left_col)?.value_ref() {
                    Value::Double(v) => data.push(*v),
                    Value::Float(f) => data.push(*f as f64),
                    Value::Int(v) => data.push(*v as f64),
                    Value::Null => {
                        data.push(0.0);
                        nulls
                            .get_or_insert_with(|| Bitmap::new_all_valid(n))
                            .set(i, false);
                    }
                    _ => return None,
                }
            }
            Some(OuterLane::F64 { data, nulls })
        }
    }
}

pub(super) fn simd_extract_inner_scalar(right_row: &PlanRow, term: &SimdTerm) -> InnerScalar {
    let Some(field) = right_row.fields.get(term.right_col) else {
        return InnerScalar::Bad;
    };
    match (term.lane, field.value_ref()) {
        (_, Value::Null) => InnerScalar::Null,
        (SimdLane::I64, Value::Int(v)) => InnerScalar::I64(*v),
        (SimdLane::I64, Value::UInt(u)) if *u <= i64::MAX as u64 => InnerScalar::I64(*u as i64),
        (SimdLane::F64, Value::Double(v)) => InnerScalar::F64(*v),
        (SimdLane::F64, Value::Float(f)) => InnerScalar::F64(*f as f64),
        (SimdLane::F64, Value::Int(v)) => InnerScalar::F64(*v as f64),
        _ => InnerScalar::Bad,
    }
}

pub(super) fn simd_apply_i64_const(
    op: SimdCmpOp,
    a: &[i64],
    rhs: i64,
    out: &mut crate::engine::executor::vec::bitmap::Bitmap,
) {
    use crate::engine::executor::vec::expr_batch as k;
    match op {
        SimdCmpOp::Eq => k::cmp_i64_const_eq_simd(a, rhs, out),
        SimdCmpOp::Ne => k::cmp_i64_const_ne_simd(a, rhs, out),
        SimdCmpOp::Lt => k::cmp_i64_const_lt_simd(a, rhs, out),
        SimdCmpOp::Le => k::cmp_i64_const_le_simd(a, rhs, out),
        SimdCmpOp::Gt => k::cmp_i64_const_gt_simd(a, rhs, out),
        SimdCmpOp::Ge => k::cmp_i64_const_ge_simd(a, rhs, out),
    }
}

pub(super) fn simd_apply_f64_const(
    op: SimdCmpOp,
    a: &[f64],
    rhs: f64,
    out: &mut crate::engine::executor::vec::bitmap::Bitmap,
) {
    use crate::engine::executor::vec::expr_batch as k;
    match op {
        SimdCmpOp::Eq => k::cmp_f64_const_eq_simd(a, rhs, out),
        SimdCmpOp::Ne => k::cmp_f64_const_ne_simd(a, rhs, out),
        SimdCmpOp::Lt => k::cmp_f64_const_lt_simd(a, rhs, out),
        SimdCmpOp::Le => k::cmp_f64_const_le_simd(a, rhs, out),
        SimdCmpOp::Gt => k::cmp_f64_const_gt_simd(a, rhs, out),
        SimdCmpOp::Ge => k::cmp_f64_const_ge_simd(a, rhs, out),
    }
}

/// Vectorized fill of one outer block. Returns `Ok(false)` when a per-block
/// outer-lane extraction hit an unexpected runtime type (caller then runs
/// the row-wise path for this block); `Ok(true)` once the block is filled
/// (individual inner rows whose scalar didn't fit the lane were handled
/// per-pair via `pre` inline). Semantics are identical to the row-wise
/// path — verified by the `MYDB_NLJ_VECTORIZE=off` differential tests.
#[allow(clippy::too_many_arguments)]
pub(super) fn nlj_simd_fill_block(
    vp: &VectorizedJoinPredicate,
    outer_block: &[PlanRow],
    right_rows: &[PlanRow],
    existence_only: bool,
    join_lk: &std::sync::Arc<RowSchema>,
    buckets: &mut [Vec<PlanRow>],
    matched: &mut [bool],
    mut right_matched: Option<&mut Vec<bool>>,
    pre: &JoinedResidualPre,
    scratch: &mut crate::engine::expr_program::EvalScratch,
    pairs_evaluated: &mut u64,
    pairs_kept: &mut u64,
) -> Result<bool, QueryError> {
    use crate::engine::executor::vec::bitmap::Bitmap;
    let n = outer_block.len();
    if n == 0 {
        return Ok(true);
    }
    // Hoist each term's outer column out of the inner-row loop (once/block).
    let mut lanes: Vec<OuterLane> = Vec::with_capacity(vp.simd_terms.len());
    for term in &vp.simd_terms {
        match simd_extract_outer_lane(outer_block, term) {
            Some(lane) => lanes.push(lane),
            None => return Ok(false),
        }
    }
    let mut mask = Bitmap::new_all_invalid(n);
    let mut term_bits = Bitmap::new_all_invalid(n);
    let mut used_simd = false;

    for (ri, right_row) in right_rows.iter().enumerate() {
        mask.fill_true(n);
        let mut fallback = false;
        let mut no_match = false;
        for (t, term) in vp.simd_terms.iter().enumerate() {
            match (&lanes[t], simd_extract_inner_scalar(right_row, term)) {
                (OuterLane::I64 { data, nulls }, InnerScalar::I64(s)) => {
                    term_bits.fill_false(n);
                    simd_apply_i64_const(term.op, data, s, &mut term_bits);
                    if let Some(nm) = nulls {
                        term_bits.and_assign(nm);
                    }
                    mask.and_assign(&term_bits);
                }
                (OuterLane::F64 { data, nulls }, InnerScalar::F64(s)) => {
                    term_bits.fill_false(n);
                    simd_apply_f64_const(term.op, data, s, &mut term_bits);
                    if let Some(nm) = nulls {
                        term_bits.and_assign(nm);
                    }
                    mask.and_assign(&term_bits);
                }
                (_, InnerScalar::Null) => {
                    no_match = true;
                    break;
                }
                _ => {
                    fallback = true;
                    break;
                }
            }
        }
        if no_match {
            continue;
        }
        if fallback {
            // This inner row's scalar didn't fit the lane: evaluate the full
            // condition per-pair against the whole block.
            for (i, left_row) in outer_block.iter().enumerate() {
                if existence_only && matched[i] {
                    continue;
                }
                *pairs_evaluated += 1;
                if pre.eval_pair_with_scratch(left_row, right_row, scratch)? {
                    *pairs_kept += 1;
                    matched[i] = true;
                    if let Some(rm) = right_matched.as_deref_mut() {
                        rm[ri] = true;
                    }
                    if !existence_only {
                        push_nlj_materialized_row(
                            &mut buckets[i],
                            join_rows(left_row, right_row, join_lk),
                        )?;
                    }
                }
            }
            continue;
        }
        // SIMD compared every outer row against this inner row.
        *pairs_evaluated += n as u64;
        used_simd = true;
        // Refine the SIMD mask with the non-vectorizable residual, only on
        // rows the mask kept.
        if let Some(res) = &vp.residual {
            let hits: smallvec::SmallVec<[usize; 16]> = mask.iter_true().collect();
            for i in hits {
                if !res.eval_pair_with_scratch(&outer_block[i], right_row, scratch)? {
                    mask.set(i, false);
                }
            }
        }
        for i in mask.iter_true() {
            if existence_only && matched[i] {
                continue;
            }
            *pairs_kept += 1;
            matched[i] = true;
            if let Some(rm) = right_matched.as_deref_mut() {
                rm[ri] = true;
            }
            if !existence_only {
                push_nlj_materialized_row(
                    &mut buckets[i],
                    join_rows(&outer_block[i], right_row, join_lk),
                )?;
            }
        }
    }
    if used_simd {
        NLJ_SIMD_FILL_BLOCKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(true)
}

pub(super) fn push_nlj_materialized_row(
    out: &mut Vec<PlanRow>,
    row: PlanRow,
) -> Result<(), QueryError> {
    push_materialized_plan_row_strict(out, row)
}

pub(super) fn push_materialized_plan_row(
    out: &mut Vec<PlanRow>,
    row: PlanRow,
) -> Result<(), QueryError> {
    if strict_plan_row_output_accounting_enabled() {
        charge_transient_materialization(materialized_plan_row_charge(&row))?;
    }
    out.push(row);
    Ok(())
}

pub(super) fn push_materialized_plan_row_strict(
    out: &mut Vec<PlanRow>,
    row: PlanRow,
) -> Result<(), QueryError> {
    charge_transient_materialization(materialized_plan_row_charge(&row))?;
    out.push(row);
    Ok(())
}

pub(in crate::engine::executor::select) fn materialized_plan_row_charge(row: &PlanRow) -> usize {
    // Structural estimate of one materialized intermediate row. This is
    // charged against the per-query budget while a blocking operator is
    // building its output and released when that operator body returns
    // (see `MaterializationChargeScope`), so it tracks *peak* live
    // intermediate state rather than a cumulative sum — no inflation
    // multiplier is applied.
    estimate_row_bytes(row) as usize
}

/// WS-N1 Block-NL main body. Returns:
///   * `Ok(Some(rows))`: the block path produced the output (condition compiled successfully, or no condition);
///   * `Ok(None)`: shape unavailable (condition failed to compile, column templates unobtainable); the caller falls back to legacy.
///
/// When the block path runs:
///   * one `with_eval_scratch` call borrows out an `EvalScratch`; the inner loop calls
///     `pre.eval_pair_with_scratch` for each pair directly, removing LazyJoinedResidualPre's
///     Option::get indirection and the per-pair RefCell::borrow_mut.
///   * outer is chunked by `MYDB_NLJ_BLOCK_OUTER` (default 64); the inner loop fully scans the inner,
///     keeping the outer block resident in L1/L2 while the whole inner is processed.
///   * All shapes Inner/Left/Right/Semi/Anti are covered, with the same semantics as the legacy path.
///   * WS-N2: when `|L|×|R|` crosses `MYDB_NLJ_OUTER_PARALLEL_THRESHOLD` (default 1e7),
///     chunks execute in parallel per `MYDB_NLJ_OUTER_PARALLELISM`. Each worker borrows its own
///     thread-local scratch; RIGHT's unmatched bitmaps are merged at the end.
pub(super) fn try_execute_nested_loop_join_block(
    db: &Arc<Database>,
    txn: TxnView,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    join_type: crate::parser::JoinType,
    condition: Option<&Expr>,
    left_rows: &[PlanRow],
    right_rows: &[PlanRow],
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    // Right-join unmatched rows go through `null_extend_left(right_row, left)`,
    // Left/Anti through `null_extend_right(left_row, right)` -- both of these paths
    // degenerate on empty right / empty left (producing only null-extends). This block path handles them
    // directly; routing condition-less Right through the block path also keeps the Right shape stable and consistent.
    let join_lk = new_join_lookup();
    let block_size = nlj_block_outer_size();
    let mut out: Vec<PlanRow> = Vec::new();
    let mut pairs_evaluated: u64 = 0;
    let mut pairs_kept: u64 = 0;
    let semi_or_anti = matches!(
        join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    );

    // condition = None, a pure cartesian product: no residual evaluation, joined directly in blocks.
    // Whatever the join_type, every outer matches at least |right| times, so the LEFT/ANTI unmatched
    // branch cannot trigger (except for an empty right row set; see the early short-circuits below).
    if condition.is_none() {
        // Empty right: Inner/Semi/Right output the empty set; Left/Anti output the outer rows.
        if right_rows.is_empty() {
            match join_type {
                crate::parser::JoinType::Left => {
                    for left_row in left_rows {
                        push_nlj_materialized_row(&mut out, null_extend_right(left_row, right))?;
                    }
                }
                crate::parser::JoinType::Anti => {
                    for left_row in left_rows {
                        push_nlj_materialized_row(&mut out, left_row.clone())?;
                    }
                }
                crate::parser::JoinType::Right => {
                    // Empty left is already implied; non-empty left × empty right → no outer row appears
                    // on the right side, so unmatched right is also empty.
                }
                _ => {}
            }
            record_nlj_block_execution(0, 0);
            return Ok(Some(out));
        }
        // Empty left: no shape produces (left, right) pairs; RIGHT's unmatched goes
        // through null_extend_left, and the rest is empty.
        if left_rows.is_empty() {
            if join_type == crate::parser::JoinType::Right {
                for right_row in right_rows {
                    push_nlj_materialized_row(&mut out, null_extend_left(right_row, left))?;
                }
            }
            record_nlj_block_execution(0, 0);
            return Ok(Some(out));
        }

        if join_type == crate::parser::JoinType::Right {
            // Every right matches at least once → the unmatched set is empty.
            for outer_block in left_rows.chunks(block_size) {
                for left_row in outer_block {
                    for right_row in right_rows {
                        push_nlj_materialized_row(
                            &mut out,
                            join_rows(left_row, right_row, &join_lk),
                        )?;
                    }
                }
            }
            pairs_evaluated = (left_rows.len() as u64) * (right_rows.len() as u64);
            pairs_kept = pairs_evaluated;
            record_nlj_block_execution(pairs_evaluated, pairs_kept);
            return Ok(Some(out));
        }
        // Semi: every left has at least one match → emit left once (deduplicated);
        // Anti: every left is matched → emit nothing;
        // Inner/Left: cartesian product.
        match join_type {
            crate::parser::JoinType::Semi => {
                for outer_block in left_rows.chunks(block_size) {
                    for left_row in outer_block {
                        push_nlj_materialized_row(&mut out, left_row.clone())?;
                    }
                }
                pairs_evaluated = left_rows.len() as u64;
                pairs_kept = pairs_evaluated;
            }
            crate::parser::JoinType::Anti => {
                // All matched → output the empty set.
                pairs_evaluated = left_rows.len() as u64;
                pairs_kept = pairs_evaluated;
            }
            _ => {
                for outer_block in left_rows.chunks(block_size) {
                    for left_row in outer_block {
                        for right_row in right_rows {
                            push_nlj_materialized_row(
                                &mut out,
                                join_rows(left_row, right_row, &join_lk),
                            )?;
                        }
                    }
                }
                pairs_evaluated = (left_rows.len() as u64) * (right_rows.len() as u64);
                pairs_kept = pairs_evaluated;
            }
        }
        record_nlj_block_execution(pairs_evaluated, pairs_kept);
        return Ok(Some(out));
    }

    // With a condition: eagerly compile JoinedResidualPre; shapes that fail to compile (subqueries,
    // ambiguous column names, etc.) return Ok(None) back to legacy.
    let cond = condition.expect("condition presence checked");
    let (Some(left_template), Some(right_template)) = (left_rows.first(), right_rows.first())
    else {
        // Either side empty → same semantics as legacy; the block path's trivial branch suffices:
        if right_rows.is_empty() {
            match join_type {
                crate::parser::JoinType::Left => {
                    for left_row in left_rows {
                        push_nlj_materialized_row(&mut out, null_extend_right(left_row, right))?;
                    }
                }
                crate::parser::JoinType::Anti => {
                    for left_row in left_rows {
                        push_nlj_materialized_row(&mut out, left_row.clone())?;
                    }
                }
                _ => {}
            }
        } else if left_rows.is_empty() && join_type == crate::parser::JoinType::Right {
            for right_row in right_rows {
                push_nlj_materialized_row(&mut out, null_extend_left(right_row, left))?;
            }
        }
        record_nlj_block_execution(0, 0);
        return Ok(Some(out));
    };
    let Some(pre) = JoinedResidualPre::compile(cond, left_template, right_template) else {
        return Ok(None);
    };
    // WS-N3: split out the vectorizable same-lane numeric comparison conjuncts; at runtime, SIMD const
    // kernels produce the mask for the whole outer block, amortizing ExprProgram's per-pair interpretation.
    // No vectorizable term, or `MYDB_NLJ_VECTORIZE=off` → None, taking the row-wise block-major path (differential oracle).
    let vectorized = if nlj_vectorize_disabled() {
        None
    } else {
        VectorizedJoinPredicate::compile(cond, left_template, right_template)
    };
    let vectorized_ref = vectorized.as_ref();
    // Give the later (possibly parallel) stages one stable immutable reference.
    let _ = semi_or_anti;
    let _ = join_lk;
    let _ = pairs_evaluated;
    let _ = pairs_kept;

    // WS-N2: |L|×|R| over the threshold and outer parallel degree > 1 → chunked parallel; otherwise single-threaded.
    let degree = nlj_outer_parallelism(left_rows.len(), right_rows.len());
    if degree <= 1 {
        let (chunk_out, right_matched_opt, pe, pk) = nlj_block_process_chunk(
            right,
            left_rows,
            right_rows,
            &pre,
            join_type,
            block_size,
            vectorized_ref,
        )?;
        out.extend(chunk_out);
        if let Some(right_matched) = right_matched_opt {
            for (ri, m) in right_matched.iter().enumerate() {
                if !*m {
                    push_nlj_materialized_row(&mut out, null_extend_left(&right_rows[ri], left))?;
                }
            }
        }
        record_nlj_block_execution(pe, pk);
        return Ok(Some(out));
    }

    // Parallel fan-out: modeled on `run_inlj_outer_chunks`' scoped-thread structure,
    // it splits left_rows into N equal chunks, each processing its chunk independently. session_info / params /
    // analyze collector handles are rebuilt inside the workers, with semantics identical to the serial path.
    let chunk_size = left_rows.len().div_ceil(degree).max(1);
    let chunk_count = left_rows.len().div_ceil(chunk_size).max(1).min(degree);
    record_nlj_outer_parallel_chunks(chunk_count as u64);

    let execution_context =
        crate::engine::executor::select::execution_context::ExecutionContext::capture(txn);

    let pre_ref = &pre;
    type ChunkResult = (Vec<PlanRow>, Option<Vec<bool>>, u64, u64);
    let parts_result: Result<Vec<ChunkResult>, QueryError> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for chunk in left_rows.chunks(chunk_size) {
            let worker_context = execution_context.clone();

            handles.push(scope.spawn(move || -> Result<ChunkResult, QueryError> {
                let run = || {
                    nlj_block_process_chunk(
                        right,
                        chunk,
                        right_rows,
                        pre_ref,
                        join_type,
                        block_size,
                        vectorized_ref,
                    )
                };
                worker_context.run(db, || {
                    let _materialization_scope = MaterializationChargeScope::enter();
                    run()
                })
            }));
        }
        let mut all = Vec::with_capacity(handles.len());
        for h in handles {
            let part = h.join().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "NLJ outer worker panicked",
                )
            })??;
            all.push(part);
        }
        Ok(all)
    });
    let parts = parts_result?;

    let mut total_eval = 0u64;
    let mut total_kept = 0u64;
    if join_type == crate::parser::JoinType::Right {
        let mut combined_matched = vec![false; right_rows.len()];
        for (chunk_out, matched_opt, pe, pk) in parts {
            out.extend(chunk_out);
            total_eval += pe;
            total_kept += pk;
            if let Some(matched) = matched_opt {
                for (i, m) in matched.into_iter().enumerate() {
                    combined_matched[i] |= m;
                }
            }
        }
        for (ri, m) in combined_matched.iter().enumerate() {
            if !*m {
                push_nlj_materialized_row(&mut out, null_extend_left(&right_rows[ri], left))?;
            }
        }
    } else {
        for (chunk_out, _matched, pe, pk) in parts {
            out.extend(chunk_out);
            total_eval += pe;
            total_kept += pk;
        }
    }
    record_nlj_block_execution(total_eval, total_kept);
    Ok(Some(out))
}
