//! hash join execution and preparation.

use super::*;

/// Immutable parameter bundle, reusable across the in-memory / parallel / Grace variants.
/// The build / probe field order is already arranged upstream from build_side; this struct never flips it internally.
pub(in crate::engine::executor::select) struct HashJoinSpec<'a> {
    pub(in crate::engine::executor::select) left_plan: &'a PhysicalPlan,
    pub(in crate::engine::executor::select) right_plan: &'a PhysicalPlan,
    pub(in crate::engine::executor::select) join_type: crate::parser::JoinType,
    pub(in crate::engine::executor::select) build_keys: &'a [Expr],
    pub(in crate::engine::executor::select) probe_keys: &'a [Expr],
    pub(super) build_key_plan: Option<HashJoinKeyPlan>,
    pub(super) probe_key_plan: Option<HashJoinKeyPlan>,
    pub(in crate::engine::executor::select) null_safe: &'a [bool],
    pub(in crate::engine::executor::select) build_is_left: bool,
    pub(in crate::engine::executor::select) residual: Option<&'a Expr>,
    /// S2 join-level projection (positions of this level's retained output columns in left++right concatenation order).
    /// When Some, every emit helper assembles only the retained columns (the projection is fused into the concatenation, avoiding
    /// the post-hoc gather in `execute_hash_join`); execution paths that carry it must guarantee
    /// that every output row goes through an emit helper or an explicit `project_joined_row_in_place`.
    pub(in crate::engine::executor::select) output_projection: Option<&'a [usize]>,
}

#[derive(Clone)]
pub(in crate::engine::executor::select) struct HashJoinKeyPlan {
    pub(in crate::engine::executor::select) field_indices: smallvec::SmallVec<[usize; 2]>,
    /// Effective text collation for each key part.  It is carried beside the
    /// compiled field offset so every row-mode bucket, bloom and partition
    /// hash uses the same schema-derived semantics as the vector engine.
    pub(in crate::engine::executor::select) collation_ids: smallvec::SmallVec<[Option<u16>; 2]>,
}

impl<'a> HashJoinSpec<'a> {
    pub(super) fn build_is_outer_preserved(&self) -> bool {
        // FULL preserves BOTH sides regardless of which is the build, so the
        // build side always emits its unmatched rows NULL-extended (the emit
        // loops drive this off `build_matched`). LEFT/RIGHT preserve the build
        // only when the build IS the preserved (outer) side.
        self.join_type == crate::parser::JoinType::Full
            || matches!(
                (self.join_type, self.build_is_left),
                (crate::parser::JoinType::Left, true) | (crate::parser::JoinType::Right, false)
            )
    }
    pub(super) fn probe_is_outer_preserved(&self) -> bool {
        // FULL preserves the probe side too; LEFT/RIGHT only when the probe is
        // the preserved (outer) side.
        self.join_type == crate::parser::JoinType::Full
            || matches!(
                (self.join_type, self.build_is_left),
                (crate::parser::JoinType::Left, false) | (crate::parser::JoinType::Right, true)
            )
    }
    /// Phase B #B1: semi-join emits the probe row at most once per
    /// match; anti-join emits the probe row only when no match exists.
    /// Both rely on probe == left side (enforced by `choose_build_side`).
    pub(super) fn is_semi_or_anti(&self) -> bool {
        matches!(
            self.join_type,
            crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
        )
    }

    // ---- S2 fused emit helpers ------------------------------------------
    // All materializing paths that carry `output_projection` produce their
    // output rows through these four helpers: with a projection they clone only the retained columns (no post-hoc gather); without one
    // they are fully equivalent to the original `join_rows` / `null_extend_*` / `probe_row.clone()`.

    /// Assembles one matched output row (build×probe → left++right order).
    #[inline]
    pub(super) fn emit_joined(
        &self,
        build_row: &PlanRow,
        probe_row: &PlanRow,
        lk: &std::sync::Arc<RowSchema>,
    ) -> PlanRow {
        let (left, right) = if self.build_is_left {
            (build_row, probe_row)
        } else {
            (probe_row, build_row)
        };
        match self.output_projection {
            Some(projection) => join_rows_projected(left, right, lk, projection),
            None => join_rows(left, right, lk),
        }
    }

    /// Outer join with the probe side preserved: the probe row has no match, the build side is filled with NULL.
    #[inline]
    pub(super) fn emit_probe_unmatched(&self, probe_row: &PlanRow) -> PlanRow {
        match (self.output_projection, self.build_is_left) {
            (Some(projection), true) => {
                null_extend_left_projected(probe_row, self.left_plan, projection)
            }
            (Some(projection), false) => {
                null_extend_right_projected(probe_row, self.right_plan, projection)
            }
            (None, true) => null_extend_left(probe_row, self.left_plan),
            (None, false) => null_extend_right(probe_row, self.right_plan),
        }
    }

    /// Outer join with the build side preserved: the build row has no match, the probe side is filled with NULL.
    #[inline]
    pub(super) fn emit_build_unmatched(&self, build_row: &PlanRow) -> PlanRow {
        match (self.output_projection, self.build_is_left) {
            (Some(projection), true) => {
                null_extend_right_projected(build_row, self.right_plan, projection)
            }
            (Some(projection), false) => {
                null_extend_left_projected(build_row, self.left_plan, projection)
            }
            (None, true) => null_extend_right(build_row, self.right_plan),
            (None, false) => null_extend_left(build_row, self.left_plan),
        }
    }

    /// SEMI/ANTI: output = the probe (left) row itself. The planner projection for semi/anti
    /// covers only left fields, so the positions index the probe row directly.
    #[inline]
    pub(super) fn emit_probe_row(&self, probe_row: &PlanRow) -> PlanRow {
        match self.output_projection {
            Some(projection) => project_row_fields(probe_row, projection),
            None => probe_row.clone(),
        }
    }
}

/// Stable IDs for one display-plan subtree. Streaming join pipelines execute
/// owned clones because their streams can outlive the function that opens
/// them; retaining only these keys lets the clone report into the original
/// EXPLAIN tree after it has been moved into its final stream object.
#[derive(Clone)]
pub(in crate::engine::executor::select) struct PlanStatsKeyTree {
    pub(super) key: PlanNodeId,
    pub(super) children: Vec<PlanStatsKeyTree>,
}

impl PlanStatsKeyTree {
    pub(in crate::engine::executor::select) fn key(&self) -> PlanNodeId {
        self.key
    }

    pub(in crate::engine::executor::select) fn child(
        &self,
        index: usize,
    ) -> Option<&PlanStatsKeyTree> {
        self.children.get(index)
    }
}

pub(in crate::engine::executor::select) fn capture_plan_stats_keys(
    plan: &PhysicalPlan,
) -> PlanStatsKeyTree {
    PlanStatsKeyTree {
        key: crate::engine::executor::select::analyze_stats::plan_key(plan)
            .expect("capturing plan stats keys requires an active collector"),
        children: plan_stats_children(plan)
            .into_iter()
            .map(capture_plan_stats_keys)
            .collect(),
    }
}

pub(in crate::engine::executor::select) fn bind_plan_stats_key_tree(
    executed: &PhysicalPlan,
    keys: &PlanStatsKeyTree,
) {
    crate::engine::executor::select::analyze_stats::bind_plan_key(executed, keys.key);
    let executed_children = plan_stats_children(executed);
    assert_eq!(
        executed_children.len(),
        keys.children.len(),
        "cannot bind ANALYZE key tree with different topology"
    );
    for (executed_child, child_keys) in executed_children.into_iter().zip(&keys.children) {
        bind_plan_stats_key_tree(executed_child, child_keys);
    }
}

pub(in crate::engine::executor::select) fn plan_stats_children(
    plan: &PhysicalPlan,
) -> crate::planner::PlanChildren<'_> {
    plan.children()
}

/// Pair every node in an owned execution-plan clone with the corresponding
/// node in the immutable display/cache plan. Streaming cursors must own their
/// plan tree, while EXPLAIN ANALYZE formats the original tree; this structural
/// mapping lets worker-collected counters be mirrored back without changing
/// execution routing.
pub(in crate::engine::executor::select) fn bind_plan_stats_keys(
    executed: &PhysicalPlan,
    display: &PhysicalPlan,
) {
    crate::engine::executor::select::analyze_stats::bind_plan_clone(executed, display);
}

pub(super) fn mirror_cloned_plan_stats(original: &PhysicalPlan, executed: &PhysicalPlan) {
    crate::engine::executor::select::analyze_stats::mirror(executed, original);
    let original_children = original.children();
    let executed_children = executed.children();
    assert_eq!(
        original_children.len(),
        executed_children.len(),
        "cannot mirror ANALYZE stats across different topology"
    );
    for (original_child, executed_child) in original_children.into_iter().zip(executed_children) {
        mirror_cloned_plan_stats(original_child, executed_child);
    }
}

/// Executes a hash join, supporting:
/// - composite hash keys over multiple equi-join keys (`Vec<ValueKey>`)
/// - SQL-standard NULL semantics: under `=` rows with NULL keys are neither bucketed nor matched; under `<=>` NULL=NULL compares equal
/// - build side selection (either side), including outer-join flips: unmatched build rows are collected in a bitmap and emitted NULL-extended
/// - residual non-equi predicates applied as a final filter after a join key hit
/// - early Bloom filter probe filtering (enabled when build rows <= the bloom threshold)
/// - spilling hash-partitioned rows to disk once rows/bytes exceed thresholds, then an independent in-memory hash join per partition (Grace)
/// - hash-partitioning to multiple worker threads for parallel execution when both sides reach parallelism_threshold
#[allow(clippy::too_many_arguments)]
pub(in crate::engine::executor::select) fn execute_hash_join(
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
    output_projection: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    if let Some(leaf_pairs) =
        partition_wise_hash_join_leaf_pairs(left, right, left_keys, right_keys)
    {
        let mut rows = Vec::new();
        let mut projection_applied = output_projection.is_some();
        for (left_storage_id, right_storage_id) in leaf_pairs {
            let (mut leaf_rows, leaf_projection_applied) =
                crate::engine::executor::partition_router::with_pruned_partitions(
                    vec![left_storage_id, right_storage_id],
                    || {
                        execute_hash_join_inner(
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
                            output_projection,
                        )
                    },
                )?;
            projection_applied &= leaf_projection_applied;
            rows.append(&mut leaf_rows);
        }
        if let (Some(projection), false) = (output_projection, projection_applied) {
            project_joined_rows_in_place(&mut rows, projection);
        }
        crate::engine::executor::select::join_stream::record_hash_join_materialized_out_rows(
            rows.len() as u64,
        );
        return Ok(rows);
    }
    let (mut rows, projection_applied) = execute_hash_join_inner(
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
        output_projection,
    )?;
    // S2 projection pruning: the main materializing paths (in-memory/int/parallel/grace) already fuse the
    // projection into the concatenation (emit helpers assemble only retained columns); sub-paths that do not fuse (vec bridge /
    // streaming-inner / streaming semi-anti) close it out here as a unified post-pass. Chained streaming
    // paths assemble projected rows directly at emit time and never pass through here.
    if let (Some(projection), false) = (output_projection, projection_applied) {
        project_joined_rows_in_place(&mut rows, projection);
    }
    // M0 observability: output row count of the materialized hash join. Chained streaming paths (join_stream)
    // bypass this; their output goes to HASH_JOIN_STREAM_ROWS — the ratio of the two measures how much
    // intermediate-result materialization has been replaced by streaming.
    crate::engine::executor::select::join_stream::record_hash_join_materialized_out_rows(
        rows.len() as u64,
    );
    Ok(rows)
}

/// Return same-ordinal physical leaf pairs when an equijoin fully covers two
/// identical partition keys/topologies. The decision depends only on schema
/// and the finalized join keys, so it is stable across cache residency and
/// runtime load.
pub(in crate::engine::executor::select) fn partition_wise_hash_join_leaf_pairs(
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    left_keys: &[Expr],
    right_keys: &[Expr],
) -> Option<Vec<(u64, u64)>> {
    let left_tables = crate::planner::collect_physical_plan_tables(left);
    let right_tables = crate::planner::collect_physical_plan_tables(right);
    if left_tables.len() != 1 || right_tables.len() != 1 {
        return None;
    }
    let left_table = &left_tables[0];
    let right_table = &right_tables[0];
    let left_part = left_table.schema.partitioning.as_ref()?;
    let right_part = right_table.schema.partitioning.as_ref()?;
    if left_part.method != right_part.method
        || left_part.key_algorithm != right_part.key_algorithm
        || left_part.partitions.len() != right_part.partitions.len()
        || left_part
            .subpartitioning
            .as_ref()
            .map(|sub| (sub.method, sub.key_algorithm, sub.count))
            != right_part
                .subpartitioning
                .as_ref()
                .map(|sub| (sub.method, sub.key_algorithm, sub.count))
        || left_part
            .partitions
            .iter()
            .zip(&right_part.partitions)
            .any(|(left, right)| {
                left.bounds != right.bounds || left.subpartitions.len() != right.subpartitions.len()
            })
    {
        return None;
    }

    let left_columns = simple_partition_columns(left_part)?;
    let right_columns = simple_partition_columns(right_part)?;
    if left_columns.len() != right_columns.len() {
        return None;
    }
    for (left_column, right_column) in left_columns.iter().zip(&right_columns) {
        let covered = left_keys
            .iter()
            .zip(right_keys)
            .any(|(left_key, right_key)| {
                expr_matches_plan_table_column(left_key, left_table, left_column)
                    && expr_matches_plan_table_column(right_key, right_table, right_column)
            });
        if !covered {
            return None;
        }
        let left_schema = left_table
            .schema
            .columns
            .get(left_table.schema.column_index(left_column).ok()?)?;
        let right_schema = right_table
            .schema
            .columns
            .get(right_table.schema.column_index(right_column).ok()?)?;
        if left_schema.data_type != right_schema.data_type
            || left_schema
                .collation
                .as_deref()
                .unwrap_or("")
                .to_ascii_lowercase()
                != right_schema
                    .collation
                    .as_deref()
                    .unwrap_or("")
                    .to_ascii_lowercase()
        {
            return None;
        }
    }
    let left_ids = left_part.storage_ids();
    let right_ids = right_part.storage_ids();
    (left_ids.len() == right_ids.len() && left_ids.len() > 1)
        .then(|| left_ids.into_iter().zip(right_ids).collect())
}

pub(super) fn simple_partition_columns(
    part: &crate::storage::PartitionInfo,
) -> Option<Vec<String>> {
    let mut columns = if !part.columns.is_empty() {
        part.columns.clone()
    } else {
        match part.expr_ast.as_ref()? {
            Expr::Identifier(column) => vec![column.clone()],
            Expr::CompoundIdentifier(parts) => vec![parts.last()?.clone()],
            _ => return None,
        }
    };
    if let Some(subpartitioning) = &part.subpartitioning {
        if !subpartitioning.columns.is_empty() {
            columns.extend(subpartitioning.columns.clone());
        } else {
            match part.sub_expr_ast.as_ref()? {
                Expr::Identifier(column) => columns.push(column.clone()),
                Expr::CompoundIdentifier(parts) => columns.push(parts.last()?.clone()),
                _ => return None,
            }
        }
    }
    columns.sort_by_key(|column| column.to_ascii_lowercase());
    columns.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
    Some(columns)
}

pub(in crate::engine::executor::select) fn partition_wise_aggregate_storage_ids(
    input: &PhysicalPlan,
    group_by: &[Expr],
) -> Option<Vec<u64>> {
    let tables = crate::planner::collect_physical_plan_tables(input);
    if tables.len() != 1 {
        return None;
    }
    let table = &tables[0];
    let partitioning = table.schema.partitioning.as_ref()?;
    let columns = simple_partition_columns(partitioning)?;
    if columns.iter().any(|column| {
        !group_by
            .iter()
            .any(|expr| expr_matches_plan_table_column(expr, table, column))
    }) {
        return None;
    }
    let storage_ids = partitioning.storage_ids();
    (storage_ids.len() > 1).then_some(storage_ids)
}

thread_local! {
    pub(super) static PARTITION_WISE_AGG_ACTIVE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Is `projection` a strictly ascending index sequence that is fully in bounds?
///
/// This is the common shape of a join-level projection (a retained-column subset in original order), and the only
/// shape that can be compacted in place.
#[inline]
pub(super) fn projection_is_ascending_subset(projection: &[usize], len: usize) -> bool {
    match projection.last() {
        None => true,
        Some(&last) => last < len && projection.windows(2).all(|w| w[0] < w[1]),
    }
}

/// Narrows a row's fields to the retained columns per the join-level projection (truly in place, order preserved).
///
/// For the ascending-subset shape, `swap` retained columns forward then `truncate`: values are moved, so neither
/// a new `Vec` is allocated nor any `Value` cloned (dropped columns destruct normally at truncate).
/// The old "fresh Vec + per-field clone" implementation paid, at every join level, one allocation
/// plus N `Value::clone`/`drop` calls per output row — String-bearing columns added a malloc each.
/// Non-ascending / duplicate-index / out-of-bounds shapes keep the original rebuild semantics (per-field clone).
#[inline]
pub(super) fn project_joined_row_fields_in_place(row: &mut PlanRow, projection: &[usize]) {
    if projection_is_ascending_subset(projection, row.fields.len()) {
        let mut write = 0usize;
        for &read in projection {
            // `read >= write` always holds (projection is strictly ascending, write advances by 1),
            // and every later read is strictly greater than the current one, so a dropped column swapped into
            // position read is never read again.
            if read != write {
                row.fields.swap(read, write);
            }
            write += 1;
        }
        row.fields.truncate(write);
        return;
    }
    let mut fields = Vec::with_capacity(projection.len());
    for &i in projection {
        if let Some(field) = row.fields.get(i) {
            fields.push(field.clone());
        }
    }
    row.fields = fields;
}

/// Narrows a row's fields to the retained columns per the join-level projection (in place, order preserved).
pub(in crate::engine::executor::select) fn project_joined_row_in_place(
    row: &mut PlanRow,
    projection: &[usize],
) {
    project_joined_row_fields_in_place(row, projection);
    row.schema = RowSchema::from_fields(&row.fields).into();
}

/// Batch version: rows in one batch share column metadata by construction, so the `RowSchema` is built once and
/// shared via `Arc` (the single-row version pays two allocations per row — metas Vec + Arc — plus a per-row
/// `FieldLookup` rebuild). A metadata mismatch rebuilds the schema for that row, keeping equivalence with the single-row version.
pub(in crate::engine::executor::select) fn project_joined_rows_in_place(
    rows: &mut [PlanRow],
    projection: &[usize],
) {
    let mut shared: Option<std::sync::Arc<RowSchema>> = None;
    for row in rows.iter_mut() {
        project_joined_row_fields_in_place(row, projection);
        let schema = match shared.as_ref() {
            Some(schema) if schema.matches_fields(&row.fields) => schema.clone(),
            _ => {
                let schema = RowSchema::from_fields(&row.fields);
                shared = Some(schema.clone());
                schema
            }
        };
        row.schema = schema.into();
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn try_execute_bounded_inner_join(
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
    output_projection: Option<&[usize]>,
) -> Result<Option<(Vec<PlanRow>, bool)>, QueryError> {
    // An INNER join whose children are exact unique probes has at most one
    // candidate pair. Building BatchOperator schemas, expression programs,
    // a hash table, bloom state and a WorkArea for that pair is pure fixed
    // overhead. This gate is a structural cardinality proof (never an
    // estimate), so it cannot change with cache residency or runtime load.
    if join_type != crate::parser::JoinType::Inner
        || !bounded_result_plan(left)
        || !bounded_result_plan(right)
        || left_keys.len() != right_keys.len()
    {
        return Ok(None);
    }

    let left_rows = execute_plan_rows(db, txn, left)?;
    let right_rows = execute_plan_rows(db, txn, right)?;
    let (build_rows, probe_rows) = match build_side {
        BuildSide::Left => (left_rows.len(), right_rows.len()),
        BuildSide::Right => (right_rows.len(), left_rows.len()),
    };
    record_hash_join_build_rows(build_rows as u64);
    record_hash_join_probe_rows(probe_rows as u64);

    let mut rows = Vec::with_capacity(left_rows.len().min(right_rows.len()));
    let join_lookup = new_join_lookup();
    let mut output_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    let mut eval_ctx = EvalQueryContext::default();
    for left_row in &left_rows {
        for right_row in &right_rows {
            if !rows_match_on_keys(left_keys, right_keys, null_safe, left_row, right_row)? {
                continue;
            }
            let mut joined = join_rows(left_row, right_row, &join_lookup);
            if !residual
                .map(|expr| eval_plan_bool_with_db(db, txn, expr, &joined, &mut eval_ctx))
                .transpose()?
                .unwrap_or(true)
            {
                continue;
            }
            if let Some(projection) = output_projection {
                project_joined_row_in_place(&mut joined, projection);
            }
            if strict_plan_row_output_accounting_enabled() {
                output_charge.add(materialized_plan_row_charge(&joined))?;
            }
            rows.push(joined);
        }
    }
    BOUNDED_INNER_JOIN_HITS.fetch_add(1, Ordering::Relaxed);
    Ok(Some((rows, output_projection.is_some())))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_hash_join_inner(
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
    output_projection: Option<&[usize]>,
) -> Result<(Vec<PlanRow>, bool), QueryError> {
    increment_hash_join_count();
    if let Some(result) = try_execute_bounded_inner_join(
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
        output_projection,
    )? {
        return Ok(result);
    }
    let limits = hash_join_limits();

    if let Some(rows) = try_execute_streaming_semi_anti_neq_join(
        db, txn, left, right, join_type, left_keys, right_keys, null_safe, build_side, residual,
        &limits,
    )? {
        return Ok((rows, false));
    }
    if let Some(rows) = try_execute_streaming_semi_anti_keyset_join(
        db, txn, left, right, join_type, left_keys, right_keys, null_safe, build_side, residual,
    )? {
        return Ok((rows, false));
    }
    if let Some(rows) = crate::engine::executor::select::vec_bridge::try_vec_hash_join(
        db, txn, left, right, join_type, left_keys, right_keys, null_safe, build_side, residual,
        &mut None,
    )? {
        return Ok((rows, false));
    }
    if let Some(rows) = try_execute_streaming_inner_hash_join(
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
        &limits,
        output_projection,
    )? {
        // The projection is already fused into emit via StreamEmitSlot.
        if output_projection.is_some() {
            record_hash_join_projection_fused();
        }
        return Ok((rows, output_projection.is_some()));
    }

    let (build_rows, probe_rows) = materialize_hash_join_inputs(
        db, txn, left, right, join_type, left_keys, right_keys, null_safe, build_side, &limits,
    )?;

    let rows = execute_hash_join_from_rows(
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
        build_rows,
        probe_rows,
        output_projection,
    )?;
    // When from_rows carries a projection, all sub-paths (in-memory/int/parallel/grace/NLJ
    // fallback) already fuse it and produce projected rows.
    Ok((rows, output_projection.is_some()))
}

/// True when the join's semantics require us to emit every probe-side
/// row, even those that find no match on the build side. When this is
/// true a probe-side bloom (which permanently drops rows the build
/// can't match) would change the result set and must not be injected.
///
/// `build_side` matters because OUTER joins preserve the *outer* side
/// (LEFT preserves left, RIGHT preserves right). Whether that outer
/// side ends up as the build or the probe is a planner choice. When
/// the outer side lands on the build, the probe is the inner side and
/// its unmatched rows are dropped anyway — a probe bloom is safe.
pub(in crate::engine::executor::select) fn probe_unmatched_must_output(
    join_type: crate::parser::JoinType,
    build_side: BuildSide,
) -> bool {
    use crate::parser::JoinType::*;
    match join_type {
        Inner | Cross | Semi => false,
        // ANTI emits probe rows that have no build match. A probe bloom
        // would drop the exact rows ANTI wants to emit.
        Anti => true,
        // LEFT preserves the left side; when left ends up as the probe
        // (BuildSide::Right is the common case) probe unmatched must
        // survive. When left is the build, the probe is right (inner)
        // and unmatched probe rows are discarded — safe to bloom.
        Left => build_side == BuildSide::Right,
        Right => build_side == BuildSide::Left,
        // FULL preserves BOTH sides: whichever side is the probe, its
        // unmatched rows must survive (a probe bloom would drop them).
        Full => true,
    }
}

/// Dual of `probe_unmatched_must_output`: true when the join must emit
/// every *build*-side row, NULL-extending those that find no probe
/// match. That happens exactly when the preserved (outer) side of an
/// OUTER join lands on the build: LEFT with build=left / RIGHT with
/// build=right — a shape the planner produces whenever the preserved
/// side is the smaller input (`choose_build_side_with_costs`).
///
/// When this is true, prefiltering the build scan from the probe's key
/// set (`prefilter_build_from_probe`) would permanently drop unmatched
/// build rows before `execute_hash_join_from_rows` ever sees them, and
/// its `build_matched` bitmap can only NULL-extend rows still present
/// in `build_rows` — the dropped rows silently vanish from the result.
pub(in crate::engine::executor::select) fn build_unmatched_must_output(
    join_type: crate::parser::JoinType,
    build_side: BuildSide,
) -> bool {
    use crate::parser::JoinType::*;
    match join_type {
        // SEMI/ANTI output only probe-side rows; INNER/CROSS drop
        // unmatched rows on both sides. None of them ever emit an
        // unmatched build row.
        Inner | Cross | Semi | Anti => false,
        Left => build_side == BuildSide::Left,
        Right => build_side == BuildSide::Right,
        // FULL preserves BOTH sides: unmatched build rows must be emitted
        // regardless of which side is the build.
        Full => true,
    }
}

/// Run the scan side of a hash join: read build rows, optionally build a
/// bloom filter from the build keys, inject it as a runtime filter into
/// the probe plan (so the probe-side scan skips obvious non-matches),
/// then read probe rows. Returns `(build_rows, probe_rows)` in
/// build-vs-probe order (not left-vs-right). Shared between
/// `execute_hash_join` and the fused HashAggregate-over-HashJoin fast
/// path so both reuse the same bloom / runtime-filter machinery.
pub(super) fn execute_hash_join_probe(
    db: &Arc<Database>,
    txn: TxnView,
    probe_plan: &PhysicalPlan,
    build_rows: &[PlanRow],
    build_keys: &[Expr],
    probe_keys: &[Expr],
    null_safe: &[bool],
    inject_bloom: bool,
) -> Result<Vec<PlanRow>, QueryError> {
    let probe_plan_owned: Option<PhysicalPlan> = if inject_bloom {
        let mut probe_clone = probe_plan.clone();
        if inject_runtime_filters_from_hash_build(
            db,
            &mut probe_clone,
            build_rows,
            build_keys,
            probe_keys,
            null_safe,
        )? {
            Some(probe_clone)
        } else {
            None
        }
    } else {
        None
    };
    let probe_rows = match probe_plan_owned.as_ref() {
        Some(p) => {
            let started = std::time::Instant::now();
            let io_start = db.buffer_stats_counts();
            let rows = execute_plan_rows(db, txn, p)?;
            if crate::engine::executor::select::analyze_stats::is_recording() {
                mirror_cloned_plan_stats(probe_plan, p);
                crate::engine::executor::select::analyze_stats::record_if_absent_with_io_snapshot(
                    db,
                    probe_plan,
                    started.elapsed().as_nanos(),
                    rows.len() as u64,
                    io_start,
                );
            }
            rows
        }
        None => execute_plan_rows(db, txn, probe_plan)?,
    };
    Ok(probe_rows)
}

/// Read an unconsumed build input using an already materialized probe.
/// Shared by ordinary hash joins and adaptive joins switching after a scan.
pub(super) fn execute_hash_join_build_from_probe(
    db: &Arc<Database>,
    txn: TxnView,
    build_plan: &PhysicalPlan,
    build_keys: &[Expr],
    probe_rows: &[PlanRow],
    probe_keys: &[Expr],
    null_safe: &[bool],
    bloom_max: u64,
) -> Result<Vec<PlanRow>, QueryError> {
    Ok(
        if let Some(rows) = try_execute_hash_semi_anti_build_keyset(
            db, txn, build_plan, probe_rows, probe_keys, build_keys, null_safe,
        )? {
            rows
        } else {
            let mut build_clone = build_plan.clone();
            if (probe_rows.len() as u64) <= bloom_max
                && inject_runtime_filters_from_hash_build(
                    db,
                    &mut build_clone,
                    probe_rows,
                    probe_keys,
                    build_keys,
                    null_safe,
                )?
            {
                let rows = execute_plan_rows(db, txn, &build_clone)?;
                if crate::engine::executor::select::analyze_stats::is_recording() {
                    mirror_cloned_plan_stats(build_plan, &build_clone);
                }
                rows
            } else {
                execute_plan_rows(db, txn, build_plan)?
            }
        },
    )
}

pub(super) fn should_prefilter_hash_build(
    join_type: crate::parser::JoinType,
    build_side: BuildSide,
    build_plan: &PhysicalPlan,
    probe_plan: &PhysicalPlan,
    limits: &HashJoinLimits,
) -> bool {
    let bloom_max = limits.bloom_max_build_rows;
    let semi_or_anti = matches!(
        join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    );
    let prefilter_build_safe = matches!(
        join_type,
        crate::parser::JoinType::Inner
            | crate::parser::JoinType::Left
            | crate::parser::JoinType::Right
            | crate::parser::JoinType::Semi
            | crate::parser::JoinType::Anti
    ) && !build_unmatched_must_output(join_type, build_side);
    bloom_max > 0
        && prefilter_build_safe
        && (semi_or_anti
            || (crate::planner::estimated_rows(probe_plan) <= bloom_max as f64
                && crate::planner::estimated_rows(build_plan)
                    >= limits.bloom_min_build_rows as f64))
}

pub(super) fn materialize_hash_join_inputs(
    db: &Arc<Database>,
    txn: TxnView,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    join_type: crate::parser::JoinType,
    left_keys: &[Expr],
    right_keys: &[Expr],
    null_safe: &[bool],
    build_side: BuildSide,
    limits: &HashJoinLimits,
) -> Result<(Vec<PlanRow>, Vec<PlanRow>), QueryError> {
    // Phase E: build the build side first so we can construct a bloom
    // filter on its join keys and inject it as a `RuntimeFilter::BloomKey`
    // into the probe-side plan. The probe-side `SeqScan` consults the
    // filter via `runtime_filters_admit`, dropping rows whose join key
    // can't possibly match — saving downstream join work and memory.
    //
    // Safety: a bloom on the probe-side scan permanently drops probe
    // rows, so it's only valid when the join's semantics let us discard
    // unmatched probe rows. `probe_unmatched_must_output` enumerates the
    // join-type × build-side combinations where that holds.
    //
    // The reverse direction — `prefilter_build_from_probe` below — runs
    // the probe first and then constrains the build's scan to keys we
    // saw on the probe side. That's safe whenever a join "unmatched
    // build" row would not appear in the output anyway: INNER, SEMI,
    // ANTI, and the OUTER joins whose *probe* side is the preserved one
    // (LEFT with build=right / RIGHT with build=left). When the build
    // side is the preserved one instead (`build_unmatched_must_output`),
    // unmatched build rows must surface as NULL-extended output rows,
    // so dropping them at the scan loses rows — same two-factor
    // (join type × build side) reasoning as the probe-bloom gate above.
    let (build_plan, build_keys_arr, probe_plan, probe_keys_arr) = match build_side {
        BuildSide::Right => (right, right_keys, left, left_keys),
        BuildSide::Left => (left, left_keys, right, right_keys),
    };
    let probe_must_output_unmatched = probe_unmatched_must_output(join_type, build_side);
    let prefilter_build_from_probe =
        should_prefilter_hash_build(join_type, build_side, build_plan, probe_plan, limits);

    let (build_rows, probe_rows) = if prefilter_build_from_probe {
        let probe_rows = execute_plan_rows(db, txn, probe_plan)?;
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
        (build_rows, probe_rows)
    } else {
        let build_rows = execute_plan_rows(db, txn, build_plan)?;
        let inject_bloom = (limits
            .inject_bloom_worthwhile(build_rows.len(), crate::planner::estimated_rows(probe_plan))
            || probe_plan_accepts_inlj_right_runtime_filter(probe_plan, probe_keys_arr))
            && !probe_must_output_unmatched;
        let probe_rows = execute_hash_join_probe(
            db,
            txn,
            probe_plan,
            &build_rows,
            build_keys_arr,
            probe_keys_arr,
            null_safe,
            inject_bloom,
        )?;
        (build_rows, probe_rows)
    };

    Ok((build_rows, probe_rows))
}

pub(super) fn execute_hash_join_from_rows(
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
    build_rows: Vec<PlanRow>,
    probe_rows: Vec<PlanRow>,
    output_projection: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    let (build_plan, build_keys_arr, probe_plan, probe_keys_arr, build_is_left) = match build_side {
        BuildSide::Right => (right, right_keys, left, left_keys, false),
        BuildSide::Left => (left, left_keys, right, right_keys, true),
    };
    let (build_key_plan, probe_key_plan) =
        compile_hash_join_key_plans(build_plan, build_keys_arr, probe_plan, probe_keys_arr);
    let limits = hash_join_limits();
    // SEMI/ANTI projection positions are laid out over left fields and the emitted row is the probe row; the two coincide
    // only when probe == left (build = right). Defensive fallback: other shapes hand the projection
    // back to the caller post-pass (the planner never produces such plans).
    let fused_projection = if matches!(
        join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    ) && build_is_left
    {
        None
    } else {
        output_projection
    };
    let needs_post_projection = output_projection.is_some() && fused_projection.is_none();
    if fused_projection.is_some() {
        record_hash_join_projection_fused();
    }
    let spec = HashJoinSpec {
        left_plan: left,
        right_plan: right,
        join_type,
        build_keys: build_keys_arr,
        probe_keys: probe_keys_arr,
        build_key_plan,
        probe_key_plan,
        null_safe,
        build_is_left,
        residual,
        output_projection: fused_projection,
    };

    let build_bytes = estimate_rows_bytes(&build_rows);
    let build_too_big =
        build_rows.len() as u64 > limits.max_build_rows || build_bytes > limits.mem_limit_bytes;
    let semi_anti_in_memory_ok = spec.is_semi_or_anti()
        && build_rows.len() as u64 <= limits.max_build_rows.saturating_mul(2).max(1_000_000)
        && build_bytes <= limits.mem_limit_bytes.saturating_mul(4);

    // Account the in-memory build (rows + bucket index + bloom) against
    // the per-query / process `MemBudget` umbrella so N concurrent joins
    // — each holding up to `MYDB_HASH_JOIN_MEM_LIMIT_MB` — collectively
    // honour `process_mem_limit_bytes()`. A build that doesn't fit the
    // budget spills to grace alongside the oversized-build case
    // (graceful, disk-bound) instead of erroring. SEMI/ANTI may exceed the
    // operator-local size heuristic, but never a query/process hard limit;
    // safety pressure sends those joins through the same grace path. With no
    // active budget (tests / non-SELECT paths) `try_charge` is a no-op.
    let bloom_est = if limits.bloom_worthwhile(build_rows.len()) {
        BloomFilter::estimated_byte_size(build_rows.len()) as u64
    } else {
        0
    };
    // Account row payload plus the bucket/control/next arrays and any
    // outer-join match ledger.  The old charge covered only row payload and
    // bloom, materially understating duplicate-key and generic-key joins.
    let hash_index_est = (build_rows.len() as u64).saturating_mul(
        (std::mem::size_of::<HashJoinKey>()
            + std::mem::size_of::<(u32, u32)>()
            + std::mem::size_of::<u32>()
            + 16) as u64,
    );
    let matched_est = if spec.build_is_outer_preserved() {
        build_rows.len() as u64
    } else {
        0
    };
    let build_charge = build_bytes
        .saturating_add(hash_index_est)
        .saturating_add(matched_est)
        .saturating_add(bloom_est) as usize;
    let probe_charge = estimate_rows_bytes(&probe_rows) as usize;
    let safety_charge = build_charge.saturating_add(probe_charge);
    let (mut build_charge_guard, budget_error) =
        match crate::engine::executor::mem_budget::try_charge_for_spill(safety_charge) {
            Ok(()) => (Some(HashJoinBudgetCharge::new(safety_charge)), None),
            Err(err) => {
                HASH_JOIN_MEM_DENIED_QUERY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                (None, Some(err))
            }
        };
    let budget_ok = build_charge_guard.is_some();

    // WS-P0-2: WorkAreaManager integration. Beyond the per-query mem_budget, build
    // bytes also enter the global PGA arbiter pool (`MYDB_EXECUTOR_MEMORY_TARGET`) so N
    // concurrent hash joins globally respect the executor memory target — hash join shares that pool with sort /
    // hash_agg / distinct. A returned `SpillHint::Spill` → switches to grace on the same
    // path as a mem_budget failure (grace then does its own independent accounting).
    // Every non-empty build enters the shared pool; many small joins aggregated together likewise cannot bypass
    // the executor target.
    let work_area_enabled = !hash_join_work_area_disabled() && build_charge > 0;
    let mut work_area_handle = if work_area_enabled {
        Some(crate::engine::executor::work_area::WorkAreaHandle::register())
    } else {
        None
    };
    let work_area_wants_spill = matches!(
        work_area_handle
            .as_ref()
            .map(|h| h.try_charge(build_charge)),
        Some(crate::engine::executor::work_area::SpillHint::Spill),
    );
    if work_area_wants_spill {
        HASH_JOIN_MEM_DENIED_WORK_AREA.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    if build_too_big {
        record_hash_join_mem_exceeded();
    }
    let mut rows = 'rows: {
        let safety_gate_requires_spill = !budget_ok || work_area_wants_spill;
        let operator_limit_requires_spill = build_too_big && !semi_anti_in_memory_ok;
        if operator_limit_requires_spill {
            HASH_JOIN_MEM_DENIED_OPERATOR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if safety_gate_requires_spill || operator_limit_requires_spill {
            // Grace is disk-bound — release the in-memory reservation.
            drop(build_charge_guard.take());
            // WorkArea: release the PGA reservation synchronously so the grace path accounts independently.
            drop(work_area_handle.take());
            if limits.fallback_error {
                if let Some(err) = budget_error {
                    return Err(err);
                }
                return Err(error(crate::error::catalog::ER_UNKNOWN_ERROR,
                    "hash join build side exceeds MYDB_HASH_JOIN_MEM_LIMIT_MB / MYDB_HASH_JOIN_MAX_BUILD_ROWS",
                ));
            }
            // Prefer the Grace hash join (hash-partition into disk temp files, run each partition independently).
            // When recursion depth is exhausted or a hot partition cannot shrink, use the bounded chunked hash join.
            break 'rows execute_grace_hash_join(
                db, &spec, &limits, build_rows, probe_rows, 0, None,
            )?;
        }

        record_hash_join_build_rows(build_rows.len() as u64);
        record_hash_join_probe_rows(probe_rows.len() as u64);

        if let Some(rows) = try_execute_inner_hash_join_parallel_probe(
            db,
            txn,
            &spec,
            &limits,
            &build_rows,
            &probe_rows,
        )? {
            break 'rows rows;
        }

        // Decide whether to go parallel: only worth it when both sides are large enough.
        //
        // Phase 5: SEMI/ANTI joins are now allowed in the parallel path.
        // The hash partitioning by `compute_join_key % degree` ensures
        // every (left key, right key) match candidate lands in the same
        // partition (build and probe use identical hashing), so per-
        // partition `do_in_memory_hash_join` produces correct SEMI/ANTI
        // semantics without cross-worker coordination. Non-null-safe NULL
        // keys hash to partition 0; SEMI emits nothing for them (correct
        // since NULL = NULL is unknown), ANTI emits them (also correct).
        let total_rows = build_rows.len() as u64 + probe_rows.len() as u64;
        if limits.parallelism > 1 && total_rows >= limits.parallelism_threshold {
            break 'rows execute_hash_join_parallel(
                db, txn, &spec, &limits, build_rows, probe_rows,
            )?;
        }

        do_in_memory_hash_join(&spec, build_rows, probe_rows, &limits, true)?
    };
    if needs_post_projection {
        if let Some(projection) = output_projection {
            project_joined_rows_in_place(&mut rows, projection);
        }
    }
    Ok(rows)
}

pub(super) fn compile_hash_join_key_plan(
    plan: &PhysicalPlan,
    keys: &[Expr],
) -> Option<HashJoinKeyPlan> {
    let mut fields = Vec::new();
    collect_plan_fields(plan, &mut fields);
    let lookup = FieldLookup::build(&fields);
    let mut field_indices = smallvec::SmallVec::with_capacity(keys.len());
    let mut collation_ids = smallvec::SmallVec::with_capacity(keys.len());
    for key in keys {
        let idx = resolve_hash_join_key_index(key, &lookup)?;
        field_indices.push(idx);
        collation_ids.push(fields.get(idx).and_then(|field| field.source.collation_id));
    }
    Some(HashJoinKeyPlan {
        field_indices,
        collation_ids,
    })
}

/// Compile both sides together so equal-key rows are encoded under one
/// comparison collation.  Column/column operands have equal coercibility in
/// MySQL; a binary collation wins a same-character-set tie.  The remaining
/// same-character-set tie is kept deterministic on the build declaration,
/// matching the legacy evaluator's first-column resolution while, critically,
/// never allowing build and probe to hash under different collations.
pub(super) fn compile_hash_join_key_plans(
    build_plan: &PhysicalPlan,
    build_keys: &[Expr],
    probe_plan: &PhysicalPlan,
    probe_keys: &[Expr],
) -> (Option<HashJoinKeyPlan>, Option<HashJoinKeyPlan>) {
    let mut build = compile_hash_join_key_plan(build_plan, build_keys);
    let mut probe = compile_hash_join_key_plan(probe_plan, probe_keys);
    if let (Some(build), Some(probe)) = (&mut build, &mut probe) {
        for i in 0..build.collation_ids.len().min(probe.collation_ids.len()) {
            let Ok(common) =
                common_hash_join_collation(build.collation_ids[i], probe.collation_ids[i])
            else {
                // The ordinary evaluator reports the illegal collation mix.
                return (None, None);
            };
            build.collation_ids[i] = common;
            probe.collation_ids[i] = common;
        }
    }
    (build, probe)
}

/// Stable, schema-derived comparison collations used by EXPLAIN and plan
/// diagnostics. This intentionally depends only on the physical children and
/// key expressions; runtime memory pressure never enters the description.
pub(in crate::engine::executor::select) fn hash_join_key_collation_names(
    left: &PhysicalPlan,
    left_keys: &[Expr],
    right: &PhysicalPlan,
    right_keys: &[Expr],
) -> Vec<String> {
    let (left_plan, right_plan) = compile_hash_join_key_plans(left, left_keys, right, right_keys);
    match (left_plan, right_plan) {
        (Some(left_plan), Some(_)) => left_plan
            .collation_ids
            .iter()
            .map(|id| match id {
                Some(id) => crate::charset::collation_name_for_id(*id).to_string(),
                None => "non_text".to_string(),
            })
            .collect(),
        _ => Vec::new(),
    }
}

pub(in crate::engine::executor::select) fn common_hash_join_collation(
    left: Option<u16>,
    right: Option<u16>,
) -> Result<Option<u16>, QueryError> {
    match (left, right) {
        (None, None) => Ok(None),
        (Some(id), None) | (None, Some(id)) => Ok(Some(id)),
        (Some(left), Some(right)) if left == right => Ok(Some(left)),
        (Some(left), Some(right)) => {
            let operand = |id| Expr::Function {
                name: "__COLLATED_CONST".into(),
                args: vec![
                    Expr::Literal(crate::parser::Literal::String(String::new())),
                    Expr::Literal(crate::parser::Literal::String(
                        crate::charset::collation_name_for_id(id).into(),
                    )),
                    Expr::Literal(crate::parser::Literal::Int(2)),
                ],
                distinct: false,
            };
            crate::engine::expr::aggregate_static_expr_collations(
                &[&operand(left), &operand(right)],
                "comparison",
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn try_execute_streaming_semi_anti_neq_join(
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
    limits: &HashJoinLimits,
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    if !matches!(
        join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    ) || null_safe.iter().any(|&v| v)
        || left_keys.len() != 1
        || right_keys.len() != 1
    {
        return Ok(None);
    }
    let (build_plan, build_keys_arr, probe_plan, probe_keys_arr, build_is_left) = match build_side {
        BuildSide::Right => (right, right_keys, left, left_keys, false),
        BuildSide::Left => (left, left_keys, right, right_keys, true),
    };
    let Some((build_table, build_filter, _)) = build_keyset_scan_info(build_plan) else {
        return Ok(None);
    };
    if build_filter
        .as_ref()
        .is_some_and(|expr| crate::engine::executor::select::simple::expr_has_subquery(expr))
    {
        return Ok(None);
    }

    let (build_key_plan, probe_key_plan) =
        compile_hash_join_key_plans(build_plan, build_keys_arr, probe_plan, probe_keys_arr);
    let spec = HashJoinSpec {
        left_plan: left,
        right_plan: right,
        join_type,
        build_keys: build_keys_arr,
        probe_keys: probe_keys_arr,
        build_key_plan,
        probe_key_plan,
        null_safe,
        build_is_left,
        residual,
        output_projection: None,
    };
    let residual_split = split_hash_join_residual_by_side(&spec);
    if residual_split.build_only.is_some() || residual_split.probe_only.is_some() {
        return Ok(None);
    }
    let Some(neq) = compile_semi_anti_neq_residual(&spec, residual_split.joined.as_ref()) else {
        return Ok(None);
    };

    let mut build_fields = Vec::new();
    collect_plan_fields(build_plan, &mut build_fields);
    let build_neq_col_name = build_fields
        .get(neq.build_idx)
        .map(|f| f.column.as_str())
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "semi/anti build residual index out of bounds",
            )
        })?;
    let build_neq_col = build_table.schema.column_index(build_neq_col_name).ok();
    let build_key_col = simple_column_index_of(&build_keys_arr[0], &build_table.schema);
    let (Some(build_key_col), Some(build_neq_col)) = (build_key_col, build_neq_col) else {
        return Ok(None);
    };

    let probe_rows = execute_plan_rows(db, txn, probe_plan)?;
    record_hash_join_probe_rows(probe_rows.len() as u64);
    if probe_rows.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let mut probe_keys_set: hashbrown::HashSet<HashJoinKey, ahash::RandomState> =
        hashbrown::HashSet::with_capacity_and_hasher(probe_rows.len(), ahash::RandomState::new());
    for row in &probe_rows {
        if let Some(key) = compute_join_key_with_plan(
            spec.probe_keys,
            spec.null_safe,
            spec.probe_key_plan.as_ref(),
            row,
        )? {
            probe_keys_set.insert(key);
        }
    }
    if probe_keys_set.is_empty() {
        return Ok(Some(if join_type == crate::parser::JoinType::Anti {
            probe_rows
        } else {
            Vec::new()
        }));
    }

    let mut needed_set = std::collections::BTreeSet::new();
    needed_set.insert(build_key_col);
    needed_set.insert(build_neq_col);
    if let Some(filter) = build_filter {
        if !collect_table_column_refs(filter, &build_table.schema, &mut needed_set) {
            return Ok(None);
        }
    }
    let needed: Vec<usize> = needed_set.into_iter().collect();
    let key_pos = needed
        .iter()
        .position(|&idx| idx == build_key_col)
        .expect("build key was inserted");
    let neq_pos = needed
        .iter()
        .position(|&idx| idx == build_neq_col)
        .expect("build residual column was inserted");
    let projected_schema = projected_schema_view(&build_table.schema, &needed);
    let qualified = db.storage_key_for_table(&build_table.schema);

    type NeqSummaryMap = hashbrown::HashMap<HashJoinKey, NeqBuildSummary, ahash::RandomState>;
    fn merge_neq_summary(dst: &mut NeqBuildSummary, src: NeqBuildSummary) {
        if src.non_null == 0 {
            return;
        }
        if dst.non_null == 0 {
            *dst = src;
            return;
        }
        dst.all_same = dst.all_same && src.all_same && dst.first == src.first;
        dst.non_null = dst.non_null.saturating_add(src.non_null);
    }
    fn update_neq_summary(map: &mut NeqSummaryMap, key: HashJoinKey, value: &Value) {
        if matches!(value, Value::Null) {
            map.entry(key).or_insert_with(|| NeqBuildSummary {
                non_null: 0,
                first: ValueKey::Null,
                all_same: true,
            });
            return;
        }
        let value_key = ValueKey::from(value);
        map.entry(key)
            .and_modify(|state| {
                state.non_null = state.non_null.saturating_add(1);
                if state.non_null == 1 {
                    state.first = value_key.clone();
                    state.all_same = true;
                } else if state.first != value_key {
                    state.all_same = false;
                }
            })
            .or_insert_with(|| NeqBuildSummary {
                non_null: 1,
                first: value_key,
                all_same: true,
            });
    }

    let page_count = db.heap_page_count_by_name(&qualified)?;
    let workers = limits.parallelism.max(1);
    let effective = workers.min(page_count.max(1) as u32).max(1);
    let parts = crate::engine::executor::select::parallel::page_partition(page_count, effective);
    let mut results: Vec<Result<(NeqSummaryMap, u64), QueryError>> = (0..effective as usize)
        .map(|_| {
            Ok((
                hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
                0,
            ))
        })
        .collect();

    let execution_context =
        crate::engine::executor::select::execution_context::ExecutionContext::capture(txn);

    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(effective as usize);
        for (lo, hi) in parts {
            let worker_context = execution_context.clone();

            let db_ref = db;
            let qualified_ref = &qualified;
            let needed_ref = &needed;
            let projected_schema_ref = &projected_schema;
            let probe_keys_ref = &probe_keys_set;

            handles.push(
                s.spawn(move || -> Result<(NeqSummaryMap, u64), QueryError> {
                    let run = || -> Result<(NeqSummaryMap, u64), QueryError> {
                        let mut local: NeqSummaryMap = hashbrown::HashMap::with_capacity_and_hasher(
                            1024,
                            ahash::RandomState::new(),
                        );
                        let mut local_count = 0u64;
                        db_ref.scan_rows_columns_streaming_range(
                            qualified_ref,
                            txn,
                            needed_ref,
                            lo,
                            hi,
                            |values| {
                                if let Some(filter) = build_filter {
                                    if !crate::engine::expr::eval_bool(
                                        filter,
                                        projected_schema_ref,
                                        values,
                                    )? {
                                        return Ok(());
                                    }
                                }
                                let key_value = &values[key_pos];
                                if matches!(key_value, Value::Null) {
                                    record_hash_join_null_skipped();
                                    return Ok(());
                                }
                                let key: HashJoinKey =
                                    smallvec::smallvec![ValueKey::from(key_value)];
                                if !probe_keys_ref.contains(&key) {
                                    return Ok(());
                                }
                                local_count = local_count.saturating_add(1);
                                update_neq_summary(&mut local, key, &values[neq_pos]);
                                Ok(())
                            },
                        )?;
                        Ok((local, local_count))
                    };
                    worker_context.run(db, || run())
                }),
            );
        }
        for (idx, handle) in handles.into_iter().enumerate() {
            results[idx] = match handle.join() {
                Ok(r) => r,
                Err(_) => Err(error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "semi/anti summary worker panicked",
                )),
            };
        }
    });
    let mut summaries: NeqSummaryMap = hashbrown::HashMap::with_capacity_and_hasher(
        probe_keys_set.len().min(1_000_000),
        ahash::RandomState::new(),
    );
    let mut scanned_build_rows: u64 = 0;
    for result in results {
        let (local, count) = result?;
        scanned_build_rows = scanned_build_rows.saturating_add(count);
        for (key, state) in local {
            summaries
                .entry(key)
                .and_modify(|dst| merge_neq_summary(dst, state.clone()))
                .or_insert(state);
        }
    }
    record_hash_join_build_rows(scanned_build_rows);

    let mut out = Vec::new();
    for probe_row in probe_rows {
        let matched = if let Some(key) = compute_join_key_with_plan(
            spec.probe_keys,
            spec.null_safe,
            spec.probe_key_plan.as_ref(),
            &probe_row,
        )? {
            if let Some(summary) = summaries.get(&key) {
                if summary.non_null == 0 {
                    false
                } else {
                    let Some(field) = probe_row.fields.get(neq.probe_idx) else {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "semi/anti probe residual index out of bounds",
                        ));
                    };
                    let value = field.value_ref();
                    !matches!(value, Value::Null)
                        && (!summary.all_same || summary.first != ValueKey::from(value))
                }
            } else {
                false
            }
        } else {
            false
        };
        match join_type {
            crate::parser::JoinType::Semi if matched => out.push(probe_row),
            crate::parser::JoinType::Anti if !matched => out.push(probe_row),
            _ => {}
        }
    }
    Ok(Some(out))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn try_execute_streaming_semi_anti_keyset_join(
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
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    if !matches!(
        join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    ) || residual.is_some()
        || !crate::util::env::var("MYDB_HASH_SEMI_STREAM_KEYSET")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
            .unwrap_or(false)
        || null_safe.iter().any(|&v| v)
        || left_keys.len() != 1
        || right_keys.len() != 1
    {
        return Ok(None);
    }
    let (build_plan, build_keys_arr, probe_plan, probe_keys_arr) = match build_side {
        BuildSide::Right => (right, right_keys, left, left_keys),
        BuildSide::Left => (left, left_keys, right, right_keys),
    };
    let Some((build_table, build_filter, _)) = build_keyset_scan_info(build_plan) else {
        return Ok(None);
    };
    if build_filter
        .as_ref()
        .is_some_and(|expr| crate::engine::executor::select::simple::expr_has_subquery(expr))
    {
        return Ok(None);
    }
    let Some(build_key_col) = simple_column_index_of(&build_keys_arr[0], &build_table.schema)
    else {
        return Ok(None);
    };
    let probe_key_plan = compile_hash_join_key_plan(probe_plan, probe_keys_arr);
    let probe_rows = execute_plan_rows(db, txn, probe_plan)?;
    record_hash_join_probe_rows(probe_rows.len() as u64);
    if probe_rows.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let mut needed_set = std::collections::BTreeSet::new();
    needed_set.insert(build_key_col);
    if let Some(filter) = build_filter {
        if !collect_table_column_refs(filter, &build_table.schema, &mut needed_set) {
            return Ok(None);
        }
    }
    let needed: Vec<usize> = needed_set.into_iter().collect();
    let key_pos = needed
        .iter()
        .position(|&idx| idx == build_key_col)
        .expect("build key was inserted");
    let projected_schema = projected_schema_view(&build_table.schema, &needed);
    let qualified = db.storage_key_for_table(&build_table.schema);
    let mut keys: hashbrown::HashSet<HashJoinKey, ahash::RandomState> =
        hashbrown::HashSet::with_capacity_and_hasher(1024, ahash::RandomState::new());
    let mut build_count = 0u64;
    db.scan_rows_columns_streaming(&qualified, txn, &needed, |values| {
        if let Some(filter) = build_filter {
            if !crate::engine::expr::eval_bool(filter, &projected_schema, values)? {
                return Ok(());
            }
        }
        let value = &values[key_pos];
        if matches!(value, Value::Null) {
            record_hash_join_null_skipped();
            return Ok(());
        }
        keys.insert(smallvec::smallvec![ValueKey::from(value)]);
        build_count = build_count.saturating_add(1);
        Ok(())
    })?;
    record_hash_join_build_rows(build_count);

    let mut out = Vec::new();
    for probe_row in probe_rows {
        let matched = if let Some(key) = compute_join_key_with_plan(
            probe_keys_arr,
            null_safe,
            probe_key_plan.as_ref(),
            &probe_row,
        )? {
            keys.contains(&key)
        } else {
            false
        };
        match join_type {
            crate::parser::JoinType::Semi if matched => out.push(probe_row),
            crate::parser::JoinType::Anti if !matched => out.push(probe_row),
            _ => {}
        }
    }
    Ok(Some(out))
}

pub(in crate::engine::executor::select) fn resolve_hash_join_key_index(
    expr: &Expr,
    lookup: &FieldLookup,
) -> Option<usize> {
    match expr {
        Expr::Identifier(column) => resolve_unqualified_hash_join_key(column, lookup),
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last()?;
            if parts.len() >= 2 {
                let qualifier = &parts[parts.len() - 2];
                get_field_qualified_ci(&lookup.by_qualified, qualifier, column)
                    .copied()
                    .filter(|&index| index != usize::MAX)
            } else {
                resolve_unqualified_hash_join_key(column, lookup)
            }
        }
        _ => None,
    }
}

pub(super) fn resolve_unqualified_hash_join_key(
    column: &str,
    lookup: &FieldLookup,
) -> Option<usize> {
    match lookup.get_unqualified(column) {
        Some(indices) if indices.len() == 1 => Some(indices[0]),
        _ => None,
    }
}

/// In-memory hash join core: build a hash table + optional bloom + single-threaded probe.
/// With `count_bloom_built`=true, building a bloom increments HASH_JOIN_BLOOM_BUILT.
/// A1: "before-concatenation" evaluator for the joined residual.
///
/// The old path ran `join_rows` first for every in-bucket candidate pair (cloning both sides in full), then
/// `eval_plan_bool`; at low residual selectivity the rejected pairs paid the concatenation
/// cost for nothing. Here the joined residual is compiled into an `ExprProgram` evaluated through a virtual
/// `RowSource` spanning the two source rows (left, right) — the column index space matches
/// the joined row produced by `join_rows` (left.fields ++ right.fields) position for position,
/// so the semantics equal "concatenate first, then evaluate". Shapes the compiler does not support (subqueries,
/// ambiguous column names, etc.) return None and the caller keeps the old path.
pub(in crate::engine::executor::select) struct JoinedResidualPre {
    pub(super) program: crate::engine::expr_program::ExprProgram,
    pub(super) left_len: usize,
}

/// Lazily-compiled pair-wise joined-residual evaluator for the INLJ
/// loops (P0-5, docs/expr-performance-optimization-plan.md). First use
/// compiles against the *actual* (left, right) row shapes, so the column
/// space always matches whatever projection produced the rows; shapes
/// `ExprCompiler` declines stay `None` and the caller keeps the legacy
/// join-then-eval order.
pub(in crate::engine::executor::select) struct LazyJoinedResidualPre(
    Option<Option<JoinedResidualPre>>,
);

impl LazyJoinedResidualPre {
    pub(in crate::engine::executor::select) fn new() -> Self {
        Self(None)
    }

    #[inline]
    pub(in crate::engine::executor::select) fn get(
        &mut self,
        residual: &Expr,
        left: &PlanRow,
        right: &PlanRow,
    ) -> Option<&JoinedResidualPre> {
        self.0
            .get_or_insert_with(|| JoinedResidualPre::compile(residual, left, right))
            .as_ref()
    }
}

pub(super) struct JoinPairSource<'a> {
    pub(super) left: &'a PlanRow,
    pub(super) right: &'a PlanRow,
    pub(super) left_len: usize,
}

impl crate::engine::expr_program::RowSource for JoinPairSource<'_> {
    #[inline]
    fn value_ref(&self, idx: usize) -> Option<&Value> {
        if idx < self.left_len {
            self.left.fields.get(idx).map(|field| field.value_ref())
        } else {
            self.right
                .fields
                .get(idx - self.left_len)
                .map(|field| field.value_ref())
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.left_len + self.right.fields.len()
    }
}

/// WS-P0-3: RowSource over both sides of (build_row, &[Value]) on the streaming-inner path.
/// `build_is_left` decides whether build comes first in the joined row order (idx<left_len) or probe does.
pub(super) struct StreamingJoinPairSource<'a> {
    pub(super) build_row: &'a PlanRow,
    pub(super) probe_values: &'a [Value],
    pub(super) left_len: usize,
    pub(super) build_is_left: bool,
}

impl crate::engine::expr_program::RowSource for StreamingJoinPairSource<'_> {
    #[inline]
    fn value_ref(&self, idx: usize) -> Option<&Value> {
        if self.build_is_left {
            if idx < self.left_len {
                self.build_row
                    .fields
                    .get(idx)
                    .map(|field| field.value_ref())
            } else {
                self.probe_values.get(idx - self.left_len)
            }
        } else if idx < self.left_len {
            self.probe_values.get(idx)
        } else {
            self.build_row
                .fields
                .get(idx - self.left_len)
                .map(|field| field.value_ref())
        }
    }

    #[inline]
    fn len(&self) -> usize {
        if self.build_is_left {
            self.left_len + self.probe_values.len()
        } else {
            self.left_len + self.build_row.fields.len()
        }
    }
}

impl JoinedResidualPre {
    /// `left` / `right` are template rows of the two sides in joined row order (any row works —
    /// only column metadata is read). The schema matches the field concatenation order of `join_rows`.
    pub(super) fn compile(expr: &Expr, left: &PlanRow, right: &PlanRow) -> Option<Self> {
        let mut columns = Vec::with_capacity(left.fields.len() + right.fields.len());
        for field in left.fields.iter().chain(right.fields.iter()) {
            columns.push(field.meta.input_column());
        }
        let schema = crate::engine::expr_program::InputSchema::new(columns);
        let program = crate::engine::expr_program::ExprCompiler::compile(expr, &schema).ok()?;
        Some(Self {
            program,
            left_len: left.fields.len(),
        })
    }

    #[inline]
    pub(super) fn eval_pair(&self, left: &PlanRow, right: &PlanRow) -> Result<bool, QueryError> {
        let source = JoinPairSource {
            left,
            right,
            left_len: self.left_len,
        };
        let value = crate::engine::expr_program::with_eval_scratch(|scratch| {
            self.program.eval_source(&source, scratch)
        })?;
        Ok(crate::engine::expr::truthy(&value))
    }

    /// Batch evaluation entry that reuses a scratch across a block: Block-NL borrows an
    /// `EvalScratch` once in the outer loop and each inner pair calls this function directly, amortizing the
    /// `with_eval_scratch` RefCell::borrow_mut over the whole outer block × inner block.
    #[inline]
    pub(in crate::engine::executor::select) fn eval_pair_with_scratch(
        &self,
        left: &PlanRow,
        right: &PlanRow,
        scratch: &mut crate::engine::expr_program::EvalScratch,
    ) -> Result<bool, QueryError> {
        let source = JoinPairSource {
            left,
            right,
            left_len: self.left_len,
        };
        let value = self.program.eval_source(&source, scratch)?;
        Ok(crate::engine::expr::truthy(&value))
    }

    /// Equivalent to `compile`, but the templates come from column metadata slices (used by chained streaming
    /// joins when no template row exists — the evaluator must be built even for empty build/probe).
    pub(in crate::engine::executor::select) fn compile_from_metas(
        expr: &Expr,
        left: &[ColMeta],
        right: &[ColMeta],
    ) -> Option<Self> {
        let mut columns = Vec::with_capacity(left.len() + right.len());
        for meta in left.iter().chain(right.iter()) {
            columns.push(meta.input_column());
        }
        let schema = crate::engine::expr_program::InputSchema::new(columns);
        let program = crate::engine::expr_program::ExprCompiler::compile(expr, &schema).ok()?;
        Some(Self {
            program,
            left_len: left.len(),
        })
    }

    /// WS-P0-3: for the streaming-inner path — the probe side has no PlanRow, only a `&[Value]`
    /// projected per `probe_projection`. This entry assembles the "joined row order" InputSchema by `build_is_left`
    /// (same semantics as `compile` / `eval_build_probe`), and the compiled artifact evaluates
    /// (build_row, &[Value]) directly in the hot loop via [`eval_pair_streaming`],
    /// without constructing an intermediate PlanRow.
    pub(in crate::engine::executor::select) fn compile_streaming(
        expr: &Expr,
        build_template: &[PlanField],
        probe_table: &TableSchema,
        probe_projection: &[usize],
        build_alias: &str,
        probe_alias: &str,
        build_is_left: bool,
    ) -> Option<Self> {
        use crate::engine::expr_program::InputColumn;
        let build_cols: Vec<InputColumn> = build_template
            .iter()
            .map(|f| f.meta.input_column())
            .collect();
        let probe_cols: Vec<InputColumn> = probe_projection
            .iter()
            .map(|&i| {
                let col = &probe_table.columns[i];
                InputColumn {
                    table: probe_table.name.clone(),
                    alias: if probe_alias.is_empty() {
                        None
                    } else {
                        Some(probe_alias.to_string())
                    },
                    name: col.name.clone(),
                    column_type: probe_table.result_type(i),
                    unsigned: col.unsigned
                        || matches!(
                            col.data_type,
                            crate::storage::DataTypeKind::Year
                                | crate::storage::DataTypeKind::Bit { .. }
                        ),
                    collation_id: crate::engine::expr_program::resolve_column_collation_id(
                        probe_table,
                        col,
                    ),
                }
            })
            .collect();
        let _ = build_alias; // kept only to mirror the alias parameter of compile_from_metas; not otherwise referenced
        let mut columns = Vec::with_capacity(build_cols.len() + probe_cols.len());
        let left_len;
        if build_is_left {
            columns.extend(build_cols);
            left_len = columns.len();
            columns.extend(probe_cols);
        } else {
            columns.extend(probe_cols);
            left_len = columns.len();
            columns.extend(build_cols);
        }
        let schema = crate::engine::expr_program::InputSchema::new(columns);
        let program = crate::engine::expr_program::ExprCompiler::compile(expr, &schema).ok()?;
        Some(Self { program, left_len })
    }

    /// WS-P0-3: companion of [`compile_streaming`]; evaluates (build_row, probe_values)
    /// directly on the streaming-inner probe hot path without constructing an intermediate PlanRow.
    /// `build_is_left` must match the value used at compile time.
    #[inline]
    pub(in crate::engine::executor::select) fn eval_pair_streaming(
        &self,
        build_row: &PlanRow,
        probe_values: &[Value],
        build_is_left: bool,
    ) -> Result<bool, QueryError> {
        let source = StreamingJoinPairSource {
            build_row,
            probe_values,
            left_len: self.left_len,
            build_is_left,
        };
        let value = crate::engine::expr_program::with_eval_scratch(|scratch| {
            self.program.eval_source(&source, scratch)
        })?;
        Ok(crate::engine::expr::truthy(&value))
    }

    /// Picks the argument order from where build sits in the joined row.
    #[inline]
    pub(in crate::engine::executor::select) fn eval_build_probe(
        &self,
        build_is_left: bool,
        build_row: &PlanRow,
        probe_row: &PlanRow,
    ) -> Result<bool, QueryError> {
        if build_is_left {
            self.eval_pair(build_row, probe_row)
        } else {
            self.eval_pair(probe_row, build_row)
        }
    }
}

/// Compiles the joined-residual pre-evaluator for this join. When either side is empty the joined
/// residual can never be evaluated (NULL-extended rows skip the residual), so it returns None directly.
pub(super) fn compile_joined_residual_pre(
    spec: &HashJoinSpec<'_>,
    residual_split: &SplitHashResidual,
    build_rows: &[PlanRow],
    probe_rows: &[PlanRow],
) -> Option<JoinedResidualPre> {
    if hash_join_residual_precheck_disabled() {
        return None;
    }
    let expr = residual_split.joined.as_ref()?;
    let (build_template, probe_template) = (build_rows.first()?, probe_rows.first()?);
    let (left, right) = if spec.build_is_left {
        (build_template, probe_template)
    } else {
        (probe_template, build_template)
    };
    JoinedResidualPre::compile(expr, left, right)
}

#[inline]
pub(in crate::engine::executor::select) fn is_int_join_column_type(
    ty: crate::types::ColumnType,
) -> bool {
    matches!(
        ty,
        crate::types::ColumnType::Tiny
            | crate::types::ColumnType::Short
            | crate::types::ColumnType::MediumInt
            | crate::types::ColumnType::Long
            | crate::types::ColumnType::LongLong
    )
}

pub(in crate::engine::executor::select) enum IntKeyOut {
    Key(i128),
    Null,
    /// A column declared integer holds a value other than Int/UInt/Null — abandon the specialized path.
    Bail,
}

#[inline]
pub(in crate::engine::executor::select) fn int_key_at(
    row: &PlanRow,
    field_idx: usize,
) -> IntKeyOut {
    match row.fields.get(field_idx).map(|field| field.value_ref()) {
        Some(Value::Int(v)) => IntKeyOut::Key(*v as i128),
        Some(Value::UInt(v)) => IntKeyOut::Key(*v as i128),
        Some(Value::Null) => IntKeyOut::Null,
        _ => IntKeyOut::Bail,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TypedIntKeyLayout {
    Scalar,
    Fixed2,
    Fixed3,
    Fixed4,
    Dynamic,
}

#[derive(Debug, Clone)]
pub(super) struct TypedIntKeyPlan {
    pub(super) build_indices: smallvec::SmallVec<[usize; 4]>,
    pub(super) probe_indices: smallvec::SmallVec<[usize; 4]>,
    pub(super) layout: TypedIntKeyLayout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TypedKeyFallbackReason {
    UIntRange,
    RuntimeType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TypedKeyState {
    Valid,
    Null,
    Bail(TypedKeyFallbackReason),
}

pub(super) fn typed_int_key_plan(
    spec: &HashJoinSpec<'_>,
    build_rows: &[PlanRow],
    probe_rows: &[PlanRow],
) -> Option<TypedIntKeyPlan> {
    if hash_join_typed_key_disabled() || spec.null_safe.iter().any(|&bit| bit) {
        return None;
    }
    let build_plan = spec.build_key_plan.as_ref()?;
    let probe_plan = spec.probe_key_plan.as_ref()?;
    if build_plan.field_indices.is_empty()
        || build_plan.field_indices.len() != probe_plan.field_indices.len()
    {
        return None;
    }
    let build = build_rows.first()?;
    let probe = probe_rows.first()?;
    for &index in &build_plan.field_indices {
        if !is_int_join_column_type(build.fields.get(index)?.column_type) {
            return None;
        }
    }
    for &index in &probe_plan.field_indices {
        if !is_int_join_column_type(probe.fields.get(index)?.column_type) {
            return None;
        }
    }
    let layout = match build_plan.field_indices.len() {
        1 => TypedIntKeyLayout::Scalar,
        2 => TypedIntKeyLayout::Fixed2,
        3 => TypedIntKeyLayout::Fixed3,
        4 => TypedIntKeyLayout::Fixed4,
        _ => TypedIntKeyLayout::Dynamic,
    };
    Some(TypedIntKeyPlan {
        build_indices: build_plan.field_indices.iter().copied().collect(),
        probe_indices: probe_plan.field_indices.iter().copied().collect(),
        layout,
    })
}

pub(super) trait TypedIntKeyCodec {
    type Key: Clone + Eq + std::hash::Hash;
    type Indices;
    const REUSE_HEAP_SCRATCH: bool;

    fn prepare_indices(indices: &[usize]) -> Option<Self::Indices>;
    fn scratch(arity: usize) -> Self::Key;
    fn extract(row: &PlanRow, indices: &Self::Indices, scratch: &mut Self::Key) -> TypedKeyState;
}

pub(super) struct ScalarIntCodec;

impl TypedIntKeyCodec for ScalarIntCodec {
    type Key = i128;
    type Indices = usize;
    const REUSE_HEAP_SCRATCH: bool = false;

    fn prepare_indices(indices: &[usize]) -> Option<Self::Indices> {
        let [index] = indices else { return None };
        Some(*index)
    }

    fn scratch(_arity: usize) -> Self::Key {
        0
    }

    #[inline(always)]
    fn extract(row: &PlanRow, index: &Self::Indices, scratch: &mut Self::Key) -> TypedKeyState {
        match row.fields.get(*index).map(|field| field.value_ref()) {
            Some(Value::Int(value)) => {
                *scratch = *value as i128;
                TypedKeyState::Valid
            }
            Some(Value::UInt(value)) => {
                *scratch = *value as i128;
                TypedKeyState::Valid
            }
            Some(Value::Null) => TypedKeyState::Null,
            _ => TypedKeyState::Bail(TypedKeyFallbackReason::RuntimeType),
        }
    }
}

pub(super) struct FixedIntCodec<const N: usize>;

#[inline(always)]
pub(super) fn read_compact_int_key_part(
    row: &PlanRow,
    index: usize,
) -> Result<Option<i64>, TypedKeyFallbackReason> {
    match row.fields.get(index).map(|field| field.value_ref()) {
        Some(Value::Int(value)) => Ok(Some(*value)),
        Some(Value::UInt(value)) if *value <= i64::MAX as u64 => Ok(Some(*value as i64)),
        Some(Value::UInt(_)) => Err(TypedKeyFallbackReason::UIntRange),
        Some(Value::Null) => Ok(None),
        _ => Err(TypedKeyFallbackReason::RuntimeType),
    }
}

impl<const N: usize> TypedIntKeyCodec for FixedIntCodec<N> {
    type Key = [i64; N];
    type Indices = [usize; N];
    const REUSE_HEAP_SCRATCH: bool = false;

    fn prepare_indices(indices: &[usize]) -> Option<Self::Indices> {
        indices.try_into().ok()
    }

    fn scratch(_arity: usize) -> Self::Key {
        [0; N]
    }

    #[inline(always)]
    fn extract(row: &PlanRow, indices: &Self::Indices, scratch: &mut Self::Key) -> TypedKeyState {
        for (slot, &index) in scratch.iter_mut().zip(indices) {
            match read_compact_int_key_part(row, index) {
                Ok(Some(value)) => *slot = value,
                Ok(None) => return TypedKeyState::Null,
                Err(reason) => return TypedKeyState::Bail(reason),
            }
        }
        TypedKeyState::Valid
    }
}

pub(super) struct DynamicIntCodec;

impl TypedIntKeyCodec for DynamicIntCodec {
    type Key = Vec<i64>;
    type Indices = Vec<usize>;
    const REUSE_HEAP_SCRATCH: bool = true;

    fn prepare_indices(indices: &[usize]) -> Option<Self::Indices> {
        (!indices.is_empty()).then(|| indices.to_vec())
    }

    fn scratch(arity: usize) -> Self::Key {
        vec![0; arity]
    }

    #[inline(always)]
    fn extract(row: &PlanRow, indices: &Self::Indices, scratch: &mut Self::Key) -> TypedKeyState {
        if scratch.len() != indices.len() {
            scratch.resize(indices.len(), 0);
        }
        for (slot, &index) in scratch.iter_mut().zip(indices) {
            match read_compact_int_key_part(row, index) {
                Ok(Some(value)) => *slot = value,
                Ok(None) => return TypedKeyState::Null,
                Err(reason) => return TypedKeyState::Bail(reason),
            }
        }
        TypedKeyState::Valid
    }
}

#[derive(Default)]
pub(super) struct TypedJoinAttemptStats {
    pub(super) bloom_built: u64,
    pub(super) bloom_filtered: u64,
    pub(super) null_skipped: u64,
}

impl TypedJoinAttemptStats {
    pub(super) fn commit(self, arity: usize) {
        if self.bloom_built > 0 {
            HASH_JOIN_BLOOM_BUILT.fetch_add(self.bloom_built, Ordering::Relaxed);
        }
        if self.bloom_filtered > 0 {
            record_hash_join_bloom_filtered_n(self.bloom_filtered);
        }
        if self.null_skipped > 0 {
            HASH_JOIN_NULL_SKIPPED.fetch_add(self.null_skipped, Ordering::Relaxed);
        }
        record_hash_join_typed_key_fast(arity);
    }
}

pub(super) fn record_typed_key_fallback(reason: TypedKeyFallbackReason) {
    match reason {
        TypedKeyFallbackReason::UIntRange => {
            HASH_JOIN_TYPED_KEY_FALLBACK_UINT_RANGE.fetch_add(1, Ordering::Relaxed);
        }
        TypedKeyFallbackReason::RuntimeType => {
            HASH_JOIN_TYPED_KEY_FALLBACK_RUNTIME_TYPE.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[inline]
pub(super) fn push_typed_attempt_row(
    out: &mut Vec<PlanRow>,
    charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
    row: PlanRow,
) -> Result<(), QueryError> {
    if strict_plan_row_output_accounting_enabled() {
        charge.add(materialized_plan_row_charge(&row))?;
    }
    out.push(row);
    Ok(())
}

pub(super) fn bloom_from_typed_buckets<K: std::hash::Hash + Eq>(
    map: &hashbrown::HashMap<K, (u32, u32), ahash::RandomState>,
) -> BloomFilter {
    let mut bloom = BloomFilter::new(map.len().max(1));
    for key in map.keys() {
        bloom.insert(key);
    }
    bloom
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_typed_hash_join<C: TypedIntKeyCodec>(
    spec: &HashJoinSpec<'_>,
    build_rows: &[PlanRow],
    probe_rows: &[PlanRow],
    limits: &HashJoinLimits,
    count_bloom_built: bool,
    residual_split: &SplitHashResidual,
    residual_pre: Option<&JoinedResidualPre>,
    build_indices: &[usize],
    probe_indices: &[usize],
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    use hashbrown::hash_map::RawEntryMut;

    if build_rows.len() >= u32::MAX as usize {
        return Ok(None);
    }
    let arity = build_indices.len();
    let Some(build_indices) = C::prepare_indices(build_indices) else {
        return Ok(None);
    };
    let Some(probe_indices) = C::prepare_indices(probe_indices) else {
        return Ok(None);
    };
    let semi_or_anti = spec.is_semi_or_anti();
    let mut stats = TypedJoinAttemptStats::default();
    let mut output_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    let mut bloom = limits
        .bloom_worthwhile(build_rows.len())
        .then(|| BloomFilter::new(build_rows.len().max(1)));
    if bloom.is_some() && count_bloom_built {
        stats.bloom_built += 1;
    }

    let mut buckets: hashbrown::HashMap<C::Key, (u32, u32), ahash::RandomState> =
        hashbrown::HashMap::with_capacity_and_hasher(build_rows.len(), ahash::RandomState::new());
    let mut next = vec![u32::MAX; build_rows.len()];
    let mut build_key = C::scratch(arity);
    for (index, row) in build_rows.iter().enumerate() {
        if residual_split
            .build_only
            .as_ref()
            .map(|expr| eval_plan_bool(expr, row))
            .transpose()?
            .is_some_and(|keep| !keep)
        {
            continue;
        }
        match C::extract(row, &build_indices, &mut build_key) {
            TypedKeyState::Valid => {
                if let Some(filter) = bloom.as_mut() {
                    filter.insert(&build_key);
                }
                let row_index = index as u32;
                if C::REUSE_HEAP_SCRATCH {
                    match buckets.raw_entry_mut().from_key(&build_key) {
                        RawEntryMut::Occupied(mut entry) => {
                            let (_, tail) = entry.get_mut();
                            next[*tail as usize] = row_index;
                            *tail = row_index;
                        }
                        RawEntryMut::Vacant(entry) => {
                            entry.insert(build_key.clone(), (row_index, row_index));
                        }
                    }
                } else {
                    match buckets.entry(build_key.clone()) {
                        hashbrown::hash_map::Entry::Occupied(mut entry) => {
                            let (_, tail) = entry.get_mut();
                            next[*tail as usize] = row_index;
                            *tail = row_index;
                        }
                        hashbrown::hash_map::Entry::Vacant(entry) => {
                            entry.insert((row_index, row_index));
                        }
                    }
                }
            }
            TypedKeyState::Null => stats.null_skipped += 1,
            TypedKeyState::Bail(reason) => {
                record_typed_key_fallback(reason);
                return Ok(None);
            }
        }
    }

    let build_is_outer_preserved = spec.build_is_outer_preserved();
    let probe_is_outer_preserved = spec.probe_is_outer_preserved();
    let mut build_matched = if build_is_outer_preserved {
        vec![false; build_rows.len()]
    } else {
        Vec::new()
    };
    let mut out = Vec::new();
    let join_lookup = new_join_lookup();
    let mut adaptive = AdaptiveBloomSampler::new(
        AdaptiveBloomSampler::gate(limits, bloom.is_some(), build_rows.len()),
        probe_rows.len() as u64,
    );
    let mut probe_key = C::scratch(arity);
    for probe_row in probe_rows {
        let mut probe_matched = false;
        if residual_split
            .probe_only
            .as_ref()
            .map(|expr| eval_plan_bool(expr, probe_row))
            .transpose()?
            .is_some_and(|keep| !keep)
        {
            if semi_or_anti && spec.join_type == crate::parser::JoinType::Anti {
                push_typed_attempt_row(
                    &mut out,
                    &mut output_charge,
                    spec.emit_probe_row(probe_row),
                )?;
            }
            continue;
        }
        let has_key = match C::extract(probe_row, &probe_indices, &mut probe_key) {
            TypedKeyState::Valid => true,
            TypedKeyState::Null => false,
            TypedKeyState::Bail(reason) => {
                record_typed_key_fallback(reason);
                return Ok(None);
            }
        };
        if has_key {
            let bloom_hit = bloom
                .as_ref()
                .is_none_or(|filter| filter.contains(&probe_key));
            if !bloom_hit {
                stats.bloom_filtered += 1;
            }
            let head = bloom_hit
                .then(|| buckets.get(&probe_key).copied())
                .flatten();
            if adaptive.observe(head.is_some()) {
                bloom = Some(bloom_from_typed_buckets(&buckets));
                if count_bloom_built {
                    stats.bloom_built += 1;
                }
            }
            if let Some((head, _)) = head {
                let mut cursor = head;
                'matches: while cursor != u32::MAX {
                    let build_index = cursor as usize;
                    cursor = next[build_index];
                    let build_row = &build_rows[build_index];
                    let mut prebuilt = None;
                    let passes = if let Some(pre) = residual_pre {
                        pre.eval_build_probe(spec.build_is_left, build_row, probe_row)?
                    } else if residual_split.joined.is_some() {
                        let joined = if spec.build_is_left {
                            join_rows(build_row, probe_row, &join_lookup)
                        } else {
                            join_rows(probe_row, build_row, &join_lookup)
                        };
                        let keep = residual_split
                            .joined
                            .as_ref()
                            .map(|expr| eval_plan_bool(expr, &joined))
                            .transpose()?
                            .unwrap_or(true);
                        if keep {
                            prebuilt = Some(joined);
                        }
                        keep
                    } else {
                        true
                    };
                    if !passes {
                        continue 'matches;
                    }
                    probe_matched = true;
                    if semi_or_anti {
                        if spec.join_type == crate::parser::JoinType::Semi {
                            push_typed_attempt_row(
                                &mut out,
                                &mut output_charge,
                                spec.emit_probe_row(probe_row),
                            )?;
                        }
                        break 'matches;
                    }
                    if build_is_outer_preserved {
                        build_matched[build_index] = true;
                    }
                    let row = match prebuilt {
                        Some(mut joined) => {
                            if let Some(projection) = spec.output_projection {
                                project_joined_row_in_place(&mut joined, projection);
                            }
                            joined
                        }
                        None => spec.emit_joined(build_row, probe_row, &join_lookup),
                    };
                    push_typed_attempt_row(&mut out, &mut output_charge, row)?;
                }
            }
        }
        if !probe_matched {
            if semi_or_anti {
                if spec.join_type == crate::parser::JoinType::Anti {
                    push_typed_attempt_row(
                        &mut out,
                        &mut output_charge,
                        spec.emit_probe_row(probe_row),
                    )?;
                }
            } else if probe_is_outer_preserved {
                push_typed_attempt_row(
                    &mut out,
                    &mut output_charge,
                    spec.emit_probe_unmatched(probe_row),
                )?;
            }
        }
    }

    if build_is_outer_preserved {
        for (index, matched) in build_matched.iter().enumerate() {
            if !*matched {
                push_typed_attempt_row(
                    &mut out,
                    &mut output_charge,
                    spec.emit_build_unmatched(&build_rows[index]),
                )?;
            }
        }
    }
    stats.commit(arity);
    commit_transient_materialization_charge(output_charge);
    Ok(Some(out))
}

/// A2/A4 shared structure: the bucket table for the in-memory / streaming-inner / parallel-probe
/// paths. `Int` = single-integer-key specialization (i128 + chained row numbers; see the notes on
/// `do_in_memory_hash_join_int`); `GenChained` = the chained layout of the ValueKey bucket table
/// (A4, default): bucket values are a fixed 8 bytes (head,tail), duplicate keys need zero heap
/// allocations, and the row-number chain lives on the `next` array; `Gen` = the original SmallVec bucket table
/// (the fallback when `MYDB_HASH_JOIN_CHAINED_LAYOUT=off`, kept for differential testing).
pub(in crate::engine::executor::select) enum JoinBuckets {
    Gen(hashbrown::HashMap<HashJoinKey, smallvec::SmallVec<[usize; 1]>, ahash::RandomState>),
    GenChained {
        map: hashbrown::HashMap<HashJoinKey, (u32, u32), ahash::RandomState>,
        next: Vec<u32>,
    },
    Int {
        map: hashbrown::HashMap<i128, (u32, u32), ahash::RandomState>,
        next: Vec<u32>,
    },
    IntComposite {
        map: hashbrown::HashMap<Vec<i64>, (u32, u32), ahash::RandomState>,
        next: Vec<u32>,
        arity: usize,
    },
}

impl JoinBuckets {
    pub(in crate::engine::executor::select) fn is_empty(&self) -> bool {
        match self {
            JoinBuckets::Gen(map) => map.is_empty(),
            JoinBuckets::GenChained { map, .. } => map.is_empty(),
            JoinBuckets::Int { map, .. } => map.is_empty(),
            JoinBuckets::IntComposite { map, .. } => map.is_empty(),
        }
    }

    pub(in crate::engine::executor::select) fn is_typed(&self) -> bool {
        matches!(
            self,
            JoinBuckets::Int { .. } | JoinBuckets::IntComposite { .. }
        )
    }

    pub(in crate::engine::executor::select) fn typed_arity(&self) -> Option<usize> {
        match self {
            JoinBuckets::Int { .. } => Some(1),
            JoinBuckets::IntComposite { arity, .. } => Some(*arity),
            _ => None,
        }
    }

    /// Bucket lookup for generic (non-int) keys; unifies match iteration over both Gen layouts.
    #[inline]
    pub(in crate::engine::executor::select) fn gen_matches(
        &self,
        key: &HashJoinKey,
    ) -> Option<BucketMatches<'_>> {
        match self {
            JoinBuckets::Gen(map) => map
                .get(key)
                .map(|idxs| BucketMatches::Slice(idxs.as_slice().iter())),
            JoinBuckets::GenChained { map, next } => map
                .get(key)
                .map(|&(head, _)| BucketMatches::Chain { next, cursor: head }),
            JoinBuckets::Int { .. } | JoinBuckets::IntComposite { .. } => None,
        }
    }

    /// Whether a generic key exists (used by adaptive bloom sampling).
    #[inline]
    pub(in crate::engine::executor::select) fn gen_contains(&self, key: &HashJoinKey) -> bool {
        match self {
            JoinBuckets::Gen(map) => map.contains_key(key),
            JoinBuckets::GenChained { map, .. } => map.contains_key(key),
            JoinBuckets::Int { .. } | JoinBuckets::IntComposite { .. } => false,
        }
    }
}

/// A4 switch: default on. `MYDB_HASH_JOIN_CHAINED_LAYOUT=off|0|false|no` falls back to
/// the SmallVec bucket layout (for differential testing).
pub(in crate::engine::executor::select) fn hash_join_chained_layout_disabled() -> bool {
    matches!(
        crate::util::env::var("MYDB_HASH_JOIN_CHAINED_LAYOUT")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no"
    )
}

/// In-bucket match iterator: Gen mode walks a SmallVec slice, Int mode walks a linked chain.
/// Both yield in build-row insertion order, keeping output order identical to the original path.
pub(in crate::engine::executor::select) enum BucketMatches<'a> {
    Slice(std::slice::Iter<'a, usize>),
    Chain { next: &'a [u32], cursor: u32 },
}

impl Iterator for BucketMatches<'_> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        match self {
            BucketMatches::Slice(iter) => iter.next().copied(),
            BucketMatches::Chain { next, cursor } => {
                if *cursor == u32::MAX {
                    return None;
                }
                let bi = *cursor as usize;
                *cursor = next[bi];
                Some(bi)
            }
        }
    }
}

/// C2: adaptive bloom sampler. A large build beyond the static band ([bloom_min, bloom_max])
/// (> bloom_max) no longer gives up on bloom wholesale: the probe first samples
/// `SAMPLE` rows to measure the bucket miss rate; when the miss rate is >= 1/2 and enough probe rows remain,
/// a bloom is rebuilt from **the bucket table keys** (O(NDV), no build rescan) so the remaining
/// probes short-circuit on the bitmap, avoiding cache misses on large-table lookups.
pub(in crate::engine::executor::select) struct AdaptiveBloomSampler {
    pub(super) active: bool,
    pub(super) sampled: u32,
    pub(super) misses: u32,
}

impl AdaptiveBloomSampler {
    #[cfg(not(test))]
    pub(super) const SAMPLE: u32 = 4096;
    #[cfg(test)]
    pub(super) const SAMPLE: u32 = 64;
    #[cfg(not(test))]
    pub(super) const MIN_REMAINING: u64 = 16_384;
    #[cfg(test)]
    pub(super) const MIN_REMAINING: u64 = 128;

    pub(in crate::engine::executor::select) fn new(active: bool, probe_total_hint: u64) -> Self {
        Self {
            active: active && probe_total_hint > Self::MIN_REMAINING,
            sampled: 0,
            misses: 0,
        }
    }

    /// Adaptive gate for large builds: the static band built no bloom, the feature is not disabled, and the build row count
    /// exceeds the static cap ("over the cap" only exists when bloom_max>0).
    pub(in crate::engine::executor::select) fn gate(
        limits: &HashJoinLimits,
        bloom_built: bool,
        build_len: usize,
    ) -> bool {
        !bloom_built
            && limits.bloom_max_build_rows > 0
            && (build_len as u64) > limits.bloom_max_build_rows
            && !hash_join_bloom_adaptive_disabled()
    }

    /// Feed one probe bucket-lookup outcome; returns true = the sampling window closed and the miss rate
    /// qualifies, so the caller should rebuild the bloom immediately. After returning true the sampler disables itself.
    #[inline]
    pub(in crate::engine::executor::select) fn observe(&mut self, matched: bool) -> bool {
        if !self.active {
            return false;
        }
        self.sampled += 1;
        if !matched {
            self.misses += 1;
        }
        if self.sampled < Self::SAMPLE {
            return false;
        }
        self.active = false;
        self.misses * 2 >= self.sampled
    }
}

/// `MYDB_HASH_JOIN_BLOOM_ADAPTIVE` (default on).
pub(super) fn hash_join_bloom_adaptive_disabled() -> bool {
    matches!(
        crate::util::env::var("MYDB_HASH_JOIN_BLOOM_ADAPTIVE")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no"
    )
}

/// Rebuild a bloom from the generic bucket table key set (adaptive path).
pub(in crate::engine::executor::select) fn bloom_from_gen_buckets(
    map: &hashbrown::HashMap<HashJoinKey, smallvec::SmallVec<[usize; 1]>, ahash::RandomState>,
) -> BloomFilter {
    let mut bf = BloomFilter::new(map.len().max(1));
    for key in map.keys() {
        bf.insert(key.as_slice());
    }
    bf
}

/// Rebuild a bloom from the chained generic bucket table key set (adaptive path).
pub(in crate::engine::executor::select) fn bloom_from_gen_chained_buckets(
    map: &hashbrown::HashMap<HashJoinKey, (u32, u32), ahash::RandomState>,
) -> BloomFilter {
    let mut bf = BloomFilter::new(map.len().max(1));
    for key in map.keys() {
        bf.insert(key.as_slice());
    }
    bf
}

/// Unified bloom-rebuild entry over bucket tables (adaptive path, all three layouts).
pub(in crate::engine::executor::select) fn bloom_from_buckets(
    buckets: &JoinBuckets,
) -> BloomFilter {
    match buckets {
        JoinBuckets::Gen(map) => bloom_from_gen_buckets(map),
        JoinBuckets::GenChained { map, .. } => bloom_from_gen_chained_buckets(map),
        JoinBuckets::Int { map, .. } => bloom_from_int_buckets(map),
        JoinBuckets::IntComposite { map, .. } => bloom_from_typed_buckets(map),
    }
}

/// Rebuild a bloom from the int bucket table key set (adaptive path).
pub(in crate::engine::executor::select) fn bloom_from_int_buckets(
    map: &hashbrown::HashMap<i128, (u32, u32), ahash::RandomState>,
) -> BloomFilter {
    let mut bf = BloomFilter::new(map.len().max(1));
    for key in map.keys() {
        bf.insert(key);
    }
    bf
}

/// Shared bucket builder for streaming-inner and parallel-probe paths.
/// Direct integer columns of any arity use a typed layout; a build-side
/// capability miss rebuilds through the generic ValueKey layout. Bloom keys
/// always follow the selected bucket representation.
#[allow(clippy::too_many_arguments)]
pub(in crate::engine::executor::select) fn build_join_buckets(
    build_keys: &[Expr],
    null_safe: &[bool],
    build_key_plan: Option<&HashJoinKeyPlan>,
    build_rows: &[PlanRow],
    typed_build_indices: Option<&[usize]>,
    bloom_worthwhile: bool,
    count_bloom_built: bool,
    build_only: Option<&Expr>,
) -> Result<(JoinBuckets, Option<BloomFilter>), QueryError> {
    if let Some([field_idx]) = typed_build_indices {
        if build_rows.len() < u32::MAX as usize {
            let mut bloom: Option<BloomFilter> = if bloom_worthwhile {
                Some(BloomFilter::new(build_rows.len().max(1)))
            } else {
                None
            };
            let mut map: hashbrown::HashMap<i128, (u32, u32), ahash::RandomState> =
                hashbrown::HashMap::with_capacity_and_hasher(
                    build_rows.len(),
                    ahash::RandomState::new(),
                );
            let mut next: Vec<u32> = vec![u32::MAX; build_rows.len()];
            let mut null_skipped = 0u64;
            let mut bailed = false;
            for (idx, row) in build_rows.iter().enumerate() {
                crate::engine::cancel::check_cancelled()?;
                if build_only
                    .map(|expr| eval_plan_bool(expr, row))
                    .transpose()?
                    .is_some_and(|keep| !keep)
                {
                    continue;
                }
                match int_key_at(row, *field_idx) {
                    IntKeyOut::Key(key) => {
                        if let Some(bf) = bloom.as_mut() {
                            bf.insert(&key);
                        }
                        let idx = idx as u32;
                        match map.entry(key) {
                            hashbrown::hash_map::Entry::Occupied(mut entry) => {
                                let (_, tail) = entry.get_mut();
                                next[*tail as usize] = idx;
                                *tail = idx;
                            }
                            hashbrown::hash_map::Entry::Vacant(entry) => {
                                entry.insert((idx, idx));
                            }
                        }
                    }
                    IntKeyOut::Null => null_skipped += 1,
                    IntKeyOut::Bail => {
                        bailed = true;
                        break;
                    }
                }
            }
            if !bailed {
                // NULL counts enter the global counter only once we are sure there is no fallback (after a fallback
                // the generic path re-counts them).
                for _ in 0..null_skipped {
                    record_hash_join_null_skipped();
                }
                if bloom.is_some() && count_bloom_built {
                    record_hash_join_bloom_built();
                }
                return Ok((JoinBuckets::Int { map, next }, bloom));
            }
        }
    }

    if let Some(indices) = typed_build_indices.filter(|indices| indices.len() >= 2) {
        if build_rows.len() < u32::MAX as usize {
            use hashbrown::hash_map::RawEntryMut;
            let mut bloom = bloom_worthwhile.then(|| BloomFilter::new(build_rows.len().max(1)));
            let mut map: hashbrown::HashMap<Vec<i64>, (u32, u32), ahash::RandomState> =
                hashbrown::HashMap::with_capacity_and_hasher(
                    build_rows.len(),
                    ahash::RandomState::new(),
                );
            let mut next = vec![u32::MAX; build_rows.len()];
            let typed_indices = indices.to_vec();
            let mut scratch = DynamicIntCodec::scratch(typed_indices.len());
            let mut null_skipped = 0u64;
            let mut fallback = None;
            for (index, row) in build_rows.iter().enumerate() {
                crate::engine::cancel::check_cancelled()?;
                if build_only
                    .map(|expr| eval_plan_bool(expr, row))
                    .transpose()?
                    .is_some_and(|keep| !keep)
                {
                    continue;
                }
                match DynamicIntCodec::extract(row, &typed_indices, &mut scratch) {
                    TypedKeyState::Valid => {
                        if let Some(filter) = bloom.as_mut() {
                            filter.insert(&scratch);
                        }
                        let row_index = index as u32;
                        match map.raw_entry_mut().from_key(&scratch) {
                            RawEntryMut::Occupied(mut entry) => {
                                let (_, tail) = entry.get_mut();
                                next[*tail as usize] = row_index;
                                *tail = row_index;
                            }
                            RawEntryMut::Vacant(entry) => {
                                entry.insert(scratch.clone(), (row_index, row_index));
                            }
                        }
                    }
                    TypedKeyState::Null => null_skipped += 1,
                    TypedKeyState::Bail(reason) => {
                        fallback = Some(reason);
                        break;
                    }
                }
            }
            if let Some(reason) = fallback {
                record_typed_key_fallback(reason);
            } else {
                HASH_JOIN_NULL_SKIPPED.fetch_add(null_skipped, Ordering::Relaxed);
                if bloom.is_some() && count_bloom_built {
                    record_hash_join_bloom_built();
                }
                return Ok((
                    JoinBuckets::IntComposite {
                        map,
                        next,
                        arity: typed_indices.len(),
                    },
                    bloom,
                ));
            }
        }
    }

    let mut bloom: Option<BloomFilter> = if bloom_worthwhile {
        Some(BloomFilter::new(build_rows.len().max(1)))
    } else {
        None
    };
    if bloom.is_some() && count_bloom_built {
        record_hash_join_bloom_built();
    }
    // A4: default chained layout (fixed 8-byte bucket values, zero heap allocation for duplicate keys, row numbers
    // chained FIFO in insertion order, matching SmallVec layout output order); GUC off falls back to the original layout.
    if !hash_join_chained_layout_disabled() && build_rows.len() < u32::MAX as usize {
        let mut map: hashbrown::HashMap<HashJoinKey, (u32, u32), ahash::RandomState> =
            hashbrown::HashMap::with_capacity_and_hasher(
                build_rows.len(),
                ahash::RandomState::new(),
            );
        let mut next: Vec<u32> = vec![u32::MAX; build_rows.len()];
        for (idx, row) in build_rows.iter().enumerate() {
            crate::engine::cancel::check_cancelled()?;
            if build_only
                .map(|expr| eval_plan_bool(expr, row))
                .transpose()?
                .is_some_and(|keep| !keep)
            {
                continue;
            }
            if let Some(key) =
                compute_join_key_with_plan(build_keys, null_safe, build_key_plan, row)?
            {
                if let Some(bf) = bloom.as_mut() {
                    bf.insert(key.as_slice());
                }
                let idx = idx as u32;
                match map.entry(key) {
                    hashbrown::hash_map::Entry::Occupied(mut entry) => {
                        let (_, tail) = entry.get_mut();
                        next[*tail as usize] = idx;
                        *tail = idx;
                    }
                    hashbrown::hash_map::Entry::Vacant(entry) => {
                        entry.insert((idx, idx));
                    }
                }
            } else {
                record_hash_join_null_skipped();
            }
        }
        return Ok((JoinBuckets::GenChained { map, next }, bloom));
    }
    let mut map: hashbrown::HashMap<
        HashJoinKey,
        smallvec::SmallVec<[usize; 1]>,
        ahash::RandomState,
    > = hashbrown::HashMap::with_capacity_and_hasher(build_rows.len(), ahash::RandomState::new());
    for (idx, row) in build_rows.iter().enumerate() {
        crate::engine::cancel::check_cancelled()?;
        if build_only
            .map(|expr| eval_plan_bool(expr, row))
            .transpose()?
            .is_some_and(|keep| !keep)
        {
            continue;
        }
        if let Some(key) = compute_join_key_with_plan(build_keys, null_safe, build_key_plan, row)? {
            if let Some(bf) = bloom.as_mut() {
                bf.insert(key.as_slice());
            }
            map.entry(key).or_default().push(idx);
        } else {
            record_hash_join_null_skipped();
        }
    }
    Ok((JoinBuckets::Gen(map), bloom))
}

pub(super) fn do_in_memory_hash_join(
    spec: &HashJoinSpec<'_>,
    build_rows: Vec<PlanRow>,
    probe_rows: Vec<PlanRow>,
    limits: &HashJoinLimits,
    count_bloom_built: bool,
) -> Result<Vec<PlanRow>, QueryError> {
    let semi_or_anti = spec.is_semi_or_anti();
    let residual_split = if matches!(
        spec.join_type,
        crate::parser::JoinType::Inner
            | crate::parser::JoinType::Semi
            | crate::parser::JoinType::Anti
    ) {
        split_hash_join_residual_by_side(spec)
    } else {
        SplitHashResidual::joined_only(spec.residual)
    };

    if semi_or_anti && residual_split.joined.is_none() {
        return do_in_memory_hash_semi_anti_keyset_join(
            spec,
            build_rows,
            probe_rows,
            limits,
            count_bloom_built,
            residual_split.build_only.as_ref(),
            residual_split.probe_only.as_ref(),
        );
    }
    if semi_or_anti {
        if let Some(neq) = compile_semi_anti_neq_residual(spec, residual_split.joined.as_ref()) {
            return do_in_memory_hash_semi_anti_neq_join(
                spec,
                build_rows,
                probe_rows,
                limits,
                count_bloom_built,
                residual_split.build_only.as_ref(),
                residual_split.probe_only.as_ref(),
                &neq,
            );
        }
    }

    // A1: joined-residual pre-evaluator (compile failure falls back to "concatenate first, then evaluate").
    let residual_pre = compile_joined_residual_pre(spec, &residual_split, &build_rows, &probe_rows);
    if residual_pre.is_some() {
        record_hash_join_residual_precheck();
    }

    // One semantics kernel serves every integer-key arity. Fixed layouts keep
    // the common 1..=4 cases allocation-free; larger composite keys reuse a
    // Vec scratch for probes and allocate only once per distinct build key.
    if let Some(plan) = typed_int_key_plan(spec, &build_rows, &probe_rows) {
        let result = match plan.layout {
            TypedIntKeyLayout::Scalar => execute_typed_hash_join::<ScalarIntCodec>(
                spec,
                &build_rows,
                &probe_rows,
                limits,
                count_bloom_built,
                &residual_split,
                residual_pre.as_ref(),
                &plan.build_indices,
                &plan.probe_indices,
            )?,
            TypedIntKeyLayout::Fixed2 => execute_typed_hash_join::<FixedIntCodec<2>>(
                spec,
                &build_rows,
                &probe_rows,
                limits,
                count_bloom_built,
                &residual_split,
                residual_pre.as_ref(),
                &plan.build_indices,
                &plan.probe_indices,
            )?,
            TypedIntKeyLayout::Fixed3 => execute_typed_hash_join::<FixedIntCodec<3>>(
                spec,
                &build_rows,
                &probe_rows,
                limits,
                count_bloom_built,
                &residual_split,
                residual_pre.as_ref(),
                &plan.build_indices,
                &plan.probe_indices,
            )?,
            TypedIntKeyLayout::Fixed4 => execute_typed_hash_join::<FixedIntCodec<4>>(
                spec,
                &build_rows,
                &probe_rows,
                limits,
                count_bloom_built,
                &residual_split,
                residual_pre.as_ref(),
                &plan.build_indices,
                &plan.probe_indices,
            )?,
            TypedIntKeyLayout::Dynamic => execute_typed_hash_join::<DynamicIntCodec>(
                spec,
                &build_rows,
                &probe_rows,
                limits,
                count_bloom_built,
                &residual_split,
                residual_pre.as_ref(),
                &plan.build_indices,
                &plan.probe_indices,
            )?,
        };
        if let Some(rows) = result {
            return Ok(rows);
        }
    }

    // Bloom filter: built when build rows <= MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS.
    // Bucket construction goes through the shared `build_join_buckets` (the int specialization was already tried
    // above, so generic mode is forced here), and the A4 chained layout applies accordingly.
    let (buckets, mut bloom) = build_join_buckets(
        spec.build_keys,
        spec.null_safe,
        spec.build_key_plan.as_ref(),
        &build_rows,
        None,
        limits.bloom_worthwhile(build_rows.len()),
        count_bloom_built,
        residual_split.build_only.as_ref(),
    )?;

    let build_is_outer_preserved = spec.build_is_outer_preserved();
    let probe_is_outer_preserved = spec.probe_is_outer_preserved();
    let mut build_matched = if build_is_outer_preserved {
        vec![false; build_rows.len()]
    } else {
        Vec::new()
    };

    let mut out = Vec::new();
    let join_lk = new_join_lookup();
    // C2: adaptive bloom for large builds outside the static band.
    let mut adaptive = AdaptiveBloomSampler::new(
        AdaptiveBloomSampler::gate(limits, bloom.is_some(), build_rows.len()),
        probe_rows.len() as u64,
    );
    for probe_row in &probe_rows {
        let mut probe_matched = false;
        if residual_split
            .probe_only
            .as_ref()
            .map(|expr| eval_plan_bool(expr, probe_row))
            .transpose()?
            .is_some_and(|keep| !keep)
        {
            if semi_or_anti && spec.join_type == crate::parser::JoinType::Anti {
                out.push(spec.emit_probe_row(probe_row));
            }
            continue;
        }
        if let Some(key) = compute_join_key_with_plan(
            spec.probe_keys,
            spec.null_safe,
            spec.probe_key_plan.as_ref(),
            probe_row,
        )? {
            let bloom_hit = bloom.as_ref().is_none_or(|bf| bf.contains(key.as_slice()));
            if !bloom_hit {
                record_hash_join_bloom_filtered();
            }
            let idxs = if bloom_hit {
                buckets.gen_matches(&key)
            } else {
                None
            };
            if adaptive.observe(idxs.is_some()) {
                bloom = Some(bloom_from_buckets(&buckets));
                if count_bloom_built {
                    record_hash_join_bloom_built();
                }
            }
            if let Some(idxs) = idxs {
                'inner: for bi in idxs {
                    let build_row = &build_rows[bi];
                    // A1: with a pre-evaluator, check residual before concatenating; when the joined
                    // residual failed to compile, keep the old "concatenate first, then evaluate" path
                    // (evaluate on the full-width concatenation, then apply the projection after it passes).
                    let mut prebuilt: Option<PlanRow> = None;
                    let passes = if let Some(pre) = residual_pre.as_ref() {
                        pre.eval_build_probe(spec.build_is_left, build_row, probe_row)?
                    } else if residual_split.joined.is_some() {
                        let joined = if spec.build_is_left {
                            join_rows(build_row, probe_row, &join_lk)
                        } else {
                            join_rows(probe_row, build_row, &join_lk)
                        };
                        let ok = residual_split
                            .joined
                            .as_ref()
                            .map(|expr| eval_plan_bool(expr, &joined))
                            .transpose()?
                            .unwrap_or(true);
                        if ok {
                            prebuilt = Some(joined);
                        }
                        ok
                    } else {
                        true
                    };
                    if !passes {
                        continue 'inner;
                    }
                    probe_matched = true;
                    if semi_or_anti {
                        // Phase B #B1: probe row IS the LEFT row;
                        // for Semi we emit it exactly once and stop
                        // probing the bucket. Anti suppresses; the
                        // outer "if !probe_matched" branch handles it.
                        if spec.join_type == crate::parser::JoinType::Semi {
                            push_materialized_plan_row(&mut out, spec.emit_probe_row(probe_row))?;
                        }
                        break 'inner;
                    }
                    if build_is_outer_preserved {
                        build_matched[bi] = true;
                    }
                    let row = match prebuilt {
                        Some(mut joined) => {
                            if let Some(projection) = spec.output_projection {
                                project_joined_row_in_place(&mut joined, projection);
                            }
                            joined
                        }
                        None => spec.emit_joined(build_row, probe_row, &join_lk),
                    };
                    push_materialized_plan_row(&mut out, row)?;
                }
            }
        }
        if !probe_matched {
            if semi_or_anti {
                if spec.join_type == crate::parser::JoinType::Anti {
                    push_materialized_plan_row(&mut out, spec.emit_probe_row(probe_row))?;
                }
            } else if probe_is_outer_preserved {
                push_materialized_plan_row(&mut out, spec.emit_probe_unmatched(probe_row))?;
            }
        }
    }

    if build_is_outer_preserved {
        for (bi, matched) in build_matched.iter().enumerate() {
            if !*matched {
                push_materialized_plan_row(&mut out, spec.emit_build_unmatched(&build_rows[bi]))?;
            }
        }
    }

    Ok(out)
}

pub(super) fn hash_join_parallel_probe_disabled() -> bool {
    crate::util::env::var("MYDB_HASH_JOIN_PARALLEL_PROBE_DISABLE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
        .unwrap_or(false)
}

/// A1 switch: default on. `MYDB_HASH_JOIN_RESIDUAL_PRECHECK=off|0|false|no` falls back to
/// the old "concatenate first, then evaluate" path (for differential testing).
pub(in crate::engine::executor::select) fn hash_join_residual_precheck_disabled() -> bool {
    matches!(
        crate::util::env::var("MYDB_HASH_JOIN_RESIDUAL_PRECHECK")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no"
    )
}

/// WS-P0-3 switch: whether streaming-inner takes residuals. Default off (residual support on);
/// `MYDB_HASH_JOIN_STREAMING_INNER_RESIDUAL_DISABLE=1|on|true|yes` restores the old
/// "bail to the materialize path on any residual" behavior (emergency fallback / differential testing).
#[inline]
pub(super) fn streaming_inner_residual_disabled() -> bool {
    matches!(
        crate::util::env::var("MYDB_HASH_JOIN_STREAMING_INNER_RESIDUAL_DISABLE")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "on" | "true" | "yes"
    )
}

/// WS-P1-1 switch: whether build-preserved outer joins (LEFT+build=left / RIGHT+build=right)
/// enter the parallel probe path. Default off (parallel enabled); set to on to fall back to the serial
/// `do_in_memory_hash_join` (differential testing / emergency fallback).
#[inline]
pub(super) fn parallel_probe_build_preserved_disabled() -> bool {
    matches!(
        crate::util::env::var("MYDB_HASH_JOIN_PARALLEL_PROBE_BUILD_PRESERVED_DISABLE")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "on" | "true" | "yes"
    )
}

/// Hash join integrates with the `WorkAreaManager` PGA arbiter by default, sharing the query-level
/// executor memory pool with sort/hash_agg/distinct. `MYDB_HASH_JOIN_WORK_AREA=off` is only a compatibility escape hatch;
/// even when disabled, the query-level `mem_budget` hard quota still applies.
#[inline]
pub(in crate::engine::executor::select) fn hash_join_work_area_disabled() -> bool {
    let v = crate::util::env::var("MYDB_HASH_JOIN_WORK_AREA")
        .unwrap_or_default()
        .to_ascii_lowercase();
    matches!(v.as_str(), "0" | "off" | "false" | "no")
}

/// Typed integer-key acceleration is default-on. Disabling it keeps the
/// generic `ValueKey` implementation as the differential oracle.
pub(in crate::engine::executor::select) fn hash_join_typed_key_disabled() -> bool {
    matches!(
        crate::util::env::var("MYDB_HASH_JOIN_TYPED_KEY")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no"
    )
}

/// Explicitly configured row-count cap (legacy GUC compatibility); when unset, the byte cap applies instead.
pub(super) fn hash_join_parallel_probe_max_build_rows_opt() -> Option<usize> {
    crate::util::env::var("MYDB_HASH_JOIN_PARALLEL_PROBE_MAX_BUILD_ROWS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
}

/// C1: the parallel probe build-size cap is now measured in bytes (default 8 MiB ≈ per-worker
/// L2 scale) — a fixed 8192 rows was too conservative for narrow builds. When the legacy
/// `..._MAX_BUILD_ROWS` is set, the row-count rule still applies.
pub(super) fn hash_join_parallel_probe_max_build_bytes() -> u64 {
    crate::util::env::var("MYDB_HASH_JOIN_PARALLEL_PROBE_MAX_BUILD_BYTES")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(8 * 1024 * 1024)
}

pub(super) fn hash_join_streaming_inner_disabled() -> bool {
    crate::util::env::var("MYDB_HASH_JOIN_STREAMING_INNER_DISABLE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
        .unwrap_or(false)
}

/// Probe-side scan shape we can drive without materialising probe rows
/// into a `Vec<PlanRow>` first. Used by `try_execute_streaming_inner_hash_join`.
pub(super) struct StreamingProbeScanShape<'a> {
    pub(super) table: &'a PlanTable,
    pub(super) filter: Option<&'a Expr>,
    pub(super) runtime_filters: &'a [RuntimeFilter],
    pub(super) projected_columns: Option<&'a [usize]>,
    /// Positions within `projected_columns` whose values should be
    /// included in the joined `PlanRow`. The planner populates this
    /// when it can prove that some projected columns are referenced
    /// *only* by the scan filter — they're decoded so the filter can
    /// run, but never seen by anything downstream, so we skip the
    /// per-row `Value::clone` for those slots. `None` means "include
    /// every projected column" (legacy shape).
    pub(super) output_columns_in_projection: Option<&'a [usize]>,
    /// Effective parallelism for the underlying scan. `1` for plain
    /// `SeqScan`; for `Exchange { ParallelSeqScan }` the gather's degree.
    pub(super) degree: u32,
    pub(super) use_zone_map: bool,
}

pub(super) fn extract_streaming_probe_scan_shape(
    plan: &PhysicalPlan,
) -> Option<StreamingProbeScanShape<'_>> {
    use crate::planner::ExchangeOrder;
    match plan {
        PhysicalPlan::SeqScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            ..
        } => Some(StreamingProbeScanShape {
            table,
            filter: filter.as_ref(),
            runtime_filters,
            projected_columns: projected_columns.as_deref().map(|v| v.as_slice()),
            // `SeqScan` doesn't yet carry an output-projection field;
            // fall back to "include every projected column".
            output_columns_in_projection: None,
            degree: 1,
            use_zone_map: false,
        }),
        PhysicalPlan::ZoneMapScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            ..
        } => Some(StreamingProbeScanShape {
            table,
            filter: filter.as_ref(),
            runtime_filters,
            projected_columns: projected_columns.as_deref().map(|v| v.as_slice()),
            output_columns_in_projection: None,
            degree: 1,
            use_zone_map: true,
        }),
        PhysicalPlan::Exchange {
            input,
            degree,
            output_order,
            merge_keys,
            ..
        } => {
            if !matches!(output_order, ExchangeOrder::Unordered) || !merge_keys.is_empty() {
                return None;
            }
            match input.as_ref() {
                PhysicalPlan::ParallelSeqScan {
                    table,
                    filter,
                    runtime_filters,
                    projected_columns,
                    output_columns_in_projection,
                    ..
                } => Some(StreamingProbeScanShape {
                    table,
                    filter: filter.as_ref(),
                    runtime_filters,
                    projected_columns: projected_columns.as_deref().map(|v| v.as_slice()),
                    output_columns_in_projection: output_columns_in_projection
                        .as_deref()
                        .map(|v| v.as_slice()),
                    degree: *degree,
                    use_zone_map: false,
                }),
                PhysicalPlan::ParallelZoneMapScan {
                    table,
                    filter,
                    runtime_filters,
                    projected_columns,
                    output_columns_in_projection,
                    ..
                } => Some(StreamingProbeScanShape {
                    table,
                    filter: filter.as_ref(),
                    runtime_filters,
                    projected_columns: projected_columns.as_deref().map(|v| v.as_slice()),
                    output_columns_in_projection: output_columns_in_projection
                        .as_deref()
                        .map(|v| v.as_slice()),
                    degree: *degree,
                    use_zone_map: true,
                }),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Streaming INNER hash join: build side is materialised normally
/// (small relative to probe in the targeted workload), but the probe
/// side is consumed straight from `scan_rows_columns_streaming_range`
/// as raw `&[Value]` tuples. Per probe row we hash the join key, run
/// the bloom check, look up the bucket and only allocate a `PlanRow`
/// on a real match. The intermediate `Vec<PlanRow>` for the probe
/// stream (6M rows on TPC-H Q9 SF=1) is eliminated entirely.
///
/// Returns `Ok(None)` for any shape the fast path can't handle so the
/// caller falls back to the materialising legacy path.
#[allow(clippy::too_many_arguments)]
pub(super) fn try_execute_streaming_inner_hash_join(
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
    limits: &HashJoinLimits,
    output_projection: Option<&[usize]>,
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    // --- Eligibility gates ---------------------------------------------------
    if hash_join_streaming_inner_disabled() {
        return Ok(None);
    }
    // The streaming path handles INNER joins directly and OUTER joins
    // where the *build* side is the preserved (outer) one. In that
    // OUTER mode we additionally bookkeep matched build rows and emit
    // NULL-extended rows for unmatched build entries after the probe
    // completes — see `streaming_outer_emit_unmatched` below. The
    // alternative case (probe side outer-preserved) would require
    // emitting NULL-extended probe rows on no-match; that's a different
    // shape and stays on the legacy `Vec<PlanRow>`-materialising path.
    let build_is_outer_preserved_shape = matches!(
        (join_type, build_side),
        (crate::parser::JoinType::Left, BuildSide::Left)
            | (crate::parser::JoinType::Right, BuildSide::Right)
    );
    if join_type != crate::parser::JoinType::Inner && !build_is_outer_preserved_shape {
        return Ok(None);
    }
    // WS-P0-3: residual can now be wired in (`JoinedResidualPre::compile_streaming` +
    // `eval_pair_streaming`). `MYDB_HASH_JOIN_STREAMING_INNER_RESIDUAL_DISABLE`
    // restores the old "bail on any residual" behavior (emergency fallback); default off.
    let streaming_residual_disabled = streaming_inner_residual_disabled();
    if residual.is_some() && streaming_residual_disabled {
        return Ok(None);
    }
    if null_safe.iter().any(|&v| v) {
        return Ok(None);
    }
    if left_keys.is_empty() || left_keys.len() != right_keys.len() {
        return Ok(None);
    }
    let (build_plan, build_keys_arr, probe_plan, probe_keys_arr, build_is_left) = match build_side {
        BuildSide::Right => (right, right_keys, left, left_keys, false),
        BuildSide::Left => (left, left_keys, right, right_keys, true),
    };
    let track_matched = build_is_outer_preserved_shape;

    let Some(probe_shape) = extract_streaming_probe_scan_shape(probe_plan) else {
        return Ok(None);
    };

    // Subqueries inside the probe-scan filter can't be evaluated by
    // the value-based compiled predicate path; bail.
    if probe_shape
        .filter
        .map(crate::engine::executor::select::simple::expr_has_subquery)
        .unwrap_or(false)
    {
        return Ok(None);
    }

    // Compile join-key plans against PlanRow shape. `field_indices`
    // address into PlanRow.fields which mirrors the probe scan's
    // projection order, so they double as projected-value indices.
    let (build_key_plan, probe_key_plan) =
        compile_hash_join_key_plans(build_plan, build_keys_arr, probe_plan, probe_keys_arr);
    let probe_key_plan = match probe_key_plan {
        Some(p) => p,
        None => return Ok(None),
    };
    let probe_key_indices: smallvec::SmallVec<[usize; 2]> =
        probe_key_plan.field_indices.iter().copied().collect();

    // --- Materialise build side (unchanged from legacy path) -----------------
    let build_rows = execute_plan_rows(db, txn, build_plan)?;
    let build_bytes = estimate_rows_bytes(&build_rows);
    if build_rows.len() as u64 > limits.max_build_rows || build_bytes > limits.mem_limit_bytes {
        // Build is too big for in-memory hash join — defer to the
        // legacy path which knows how to spill via grace-hash.
        return Ok(None);
    }
    record_hash_join_build_rows(build_rows.len() as u64);

    // Bucket-table construction is deferred until after projection resolution (the A2 probe-side integer gate
    // must map probe_key_indices back to the table schema column types); see the
    // `build_join_buckets` call below; the early-exit branches in between thus also save the build cost.

    // --- Resolve the projection actually read from the heap ------------------
    let projection_owned: Vec<usize> = match probe_shape.projected_columns {
        Some(p) => p.to_vec(),
        None => (0..probe_shape.table.schema.columns.len()).collect(),
    };
    // Bloom-key indices on the probe must map into the projected-value
    // vector (worker_projected_streaming feeds `values: &[Value]` sized
    // to `projection.len()`). If any join-key column was pruned out
    // (shouldn't happen, but be defensive), bail.
    if probe_key_indices
        .iter()
        .any(|&i| i >= projection_owned.len())
    {
        return Ok(None);
    }

    // Resolve which projection positions go into the joined `PlanRow`.
    // The planner sets `output_columns_in_projection` when some
    // projected columns are referenced only by the scan filter (e.g.
    // TPC-H Q13's `o_comment NOT LIKE '%special%requests%'`) — those
    // positions are filter-only and can be dropped from the per-row
    // build, saving a `Value::clone` (typically a heap-allocated
    // `Value::String`) on every joined row. The join keys are still
    // read from `values[probe_key_indices[k]]` so we don't need them
    // in `output_positions`.
    let output_positions: Vec<usize> = match probe_shape.output_columns_in_projection {
        Some(positions) => {
            // Defensive: drop any out-of-range entry. Should not
            // happen if the planner is sound but lets us fall back
            // gracefully on malformed plans.
            positions
                .iter()
                .copied()
                .filter(|&pos| pos < projection_owned.len())
                .collect()
        }
        None => (0..projection_owned.len()).collect(),
    };
    let probe_templates: Vec<PlanField> = output_positions
        .iter()
        .map(|&pos| {
            let i = projection_owned[pos];
            PlanField {
                meta: intern_col_meta(ColMeta::from_table(&probe_shape.table, i)),
                value: Value::Null,
            }
        })
        .collect();

    // WS-P0-3: streaming-inner residual evaluator. When build_rows is non-empty, compile a
    // `JoinedResidualPre` from build_template + the full probe projection; the probe hot loop
    // then evaluates (build_row, &[Value]) directly, with no intermediate PlanRow. Shapes the
    // compiler declines (subqueries / unsupported expressions) bail back to the materialize path.
    //
    // With empty build_rows just use None: no candidates → emit is never reached.
    let residual_pre: Option<JoinedResidualPre> = match residual {
        None => None,
        Some(expr) => {
            if build_rows.is_empty() {
                None
            } else {
                let probe_alias_ref = probe_shape.table.alias.as_deref().unwrap_or("");
                match JoinedResidualPre::compile_streaming(
                    expr,
                    &build_rows[0].fields,
                    &probe_shape.table.schema,
                    &projection_owned,
                    "",
                    probe_alias_ref,
                    build_is_left,
                ) {
                    Some(pre) => {
                        record_hash_join_residual_precheck();
                        Some(pre)
                    }
                    None => return Ok(None),
                }
            }
        }
    };
    let residual_pre_ref = residual_pre.as_ref();

    // S2 fused emit slots: join-level projection positions (left++right concatenation order from the
    // planner view, probe-side width = the scan full projection width) pre-resolved into build column
    // indices or probe value indices. Probe-slot metas are constructed like probe_templates.
    let emit_slots: Option<Vec<StreamEmitSlot>> = output_projection.map(|projection| {
        let mut build_cols = Vec::new();
        collect_plan_fields(build_plan, &mut build_cols);
        let build_len = build_cols.len();
        let probe_len = projection_owned.len();
        let mk_probe = |pos: usize| {
            let i = projection_owned[pos];
            StreamEmitSlot::Probe {
                meta: intern_col_meta(ColMeta::from_table(&probe_shape.table, i)),
                values_idx: pos,
            }
        };
        projection
            .iter()
            .filter_map(|&i| {
                if build_is_left {
                    if i < build_len {
                        Some(StreamEmitSlot::Build(i))
                    } else if i - build_len < probe_len {
                        Some(mk_probe(i - build_len))
                    } else {
                        None
                    }
                } else if i < probe_len {
                    Some(mk_probe(i))
                } else if i - probe_len < build_len {
                    Some(StreamEmitSlot::Build(i - probe_len))
                } else {
                    None
                }
            })
            .collect()
    });

    // Compile probe-scan filter for value-based evaluation.
    let compiled_filter = probe_shape.filter.and_then(|expr| {
        let projected_schema = projected_schema_view(&probe_shape.table.schema, &projection_owned);
        compile_projected_row_predicate(expr, &projected_schema)
    });
    if probe_shape.filter.is_some() && compiled_filter.is_none() {
        // Filter exists but couldn't be compiled — keep legacy semantics.
        return Ok(None);
    }

    // Precompute runtime bloom-key column indices against the projected
    // value layout. Any non-trivial expression aborts the fast path so
    // the legacy `runtime_filters_admit` is run on a full PlanRow.
    let runtime_filters = probe_shape.runtime_filters;
    let precomputed_rfs: Vec<Option<CompiledRuntimeFilterProbe>> = runtime_filters
        .iter()
        .map(|rf| {
            compile_runtime_filter_probe_projected(rf, &probe_shape.table.schema, &projection_owned)
        })
        .collect();
    if runtime_filters
        .iter()
        .zip(&precomputed_rfs)
        .any(|(_, p)| p.is_none())
    {
        return Ok(None);
    }

    // --- Parallel probe scan -------------------------------------------------
    let requested_degree = probe_shape.degree.max(limits.parallelism).max(1);
    let (probe_tasks, worker_count) = crate::engine::executor::select::parallel::scan_morsels(
        db,
        probe_shape.table,
        probe_shape.filter,
        probe_shape.use_zone_map,
        requested_degree,
    )?;
    // With nothing to probe (empty probe scan, or every build key NULL) an
    // outer-preserved build side still emits each build row NULL-extended.
    let only_unmatched_build = || {
        let mut out = Vec::new();
        if track_matched {
            streaming_outer_emit_unmatched(
                &build_rows,
                &vec![false; build_rows.len()],
                &probe_templates,
                emit_slots.as_deref(),
                build_is_left,
                &new_join_lookup(),
                &mut out,
            );
        }
        out
    };
    if probe_tasks.is_empty() {
        return Ok(Some(only_unmatched_build()));
    }

    // Build the bucket map + optional bloom filter. Direct integer columns of
    // any arity share the typed representation; the scan-side projection
    // indices are already aligned with `probe_key_indices`.
    let typed_build_indices: Option<Vec<usize>> = (|| {
        if hash_join_typed_key_disabled() {
            return None;
        }
        if null_safe.iter().any(|&b| b) {
            return None;
        }
        let build_indices = build_key_plan.as_ref()?.field_indices.as_slice();
        if build_indices.is_empty() || build_indices.len() != probe_key_indices.len() {
            return None;
        }
        let build = build_rows.first()?;
        for &index in build_indices {
            if !is_int_join_column_type(build.fields.get(index)?.column_type) {
                return None;
            }
        }
        for &index in &probe_key_indices {
            let ty = probe_shape
                .table
                .schema
                .result_type(projection_owned[index]);
            if !is_int_join_column_type(ty) {
                return None;
            }
        }
        Some(build_indices.to_vec())
    })();
    let (buckets, bloom) = build_join_buckets(
        build_keys_arr,
        null_safe,
        build_key_plan.as_ref(),
        &build_rows,
        typed_build_indices.as_deref(),
        limits.bloom_worthwhile(build_rows.len()),
        true,
        None,
    )?;
    if buckets.is_empty() {
        return Ok(Some(only_unmatched_build()));
    }
    // Set when the probe-side int mode hits a declared/value mismatch: the whole fast path is abandoned
    // and the caller falls back to the legacy materializing path (the probe scan is read-only and safely recomputable).
    let int_bail = std::sync::atomic::AtomicBool::new(false);
    // Use the larger of the probe scan's own degree (from an upstream
    // Exchange) and the hash join's configured parallelism — even a
    // serial-SeqScan probe earns parallel page partitioning here.
    let effective_degree = worker_count.max(1) as u32;
    // Record that we're running N parallel page-partitions, matching the
    // existing parallel-hash-join observability hook so tests / SHOW
    // ENGINE MYDB STATUS still see this code path advance the counter.
    if effective_degree > 1 {
        record_hash_join_parallel_partitions(effective_degree as u64);
    }

    let join_lk = new_join_lookup();
    let probe_rows_seen = AtomicU64::new(0);

    let buckets_ref = &buckets;
    let build_ref = &build_rows;
    let bloom_ref = bloom.as_ref();
    let probe_key_indices_ref: &[usize] = &probe_key_indices;
    let probe_key_collations_ref: &[Option<u16>] = &probe_key_plan.collation_ids;
    let probe_templates_ref: &[PlanField] = &probe_templates;
    let projection_ref: &[usize] = &projection_owned;
    let output_positions_ref: &[usize] = &output_positions;
    let emit_slots_ref: Option<&[StreamEmitSlot]> = emit_slots.as_deref();
    let precomputed_rfs_ref = &precomputed_rfs;
    let compiled_filter_ref = compiled_filter.as_ref();
    let null_safe_ref = null_safe;
    let join_lk_ref = &join_lk;
    let probe_rows_seen_ref = &probe_rows_seen;
    let int_bail_ref = &int_bail;
    let residual_pre_for_workers: Option<&JoinedResidualPre> = residual_pre_ref;
    let probe_tasks = std::sync::Arc::new(probe_tasks);
    let next_task = std::sync::Arc::new(AtomicUsize::new(0));

    let execution_context =
        crate::engine::executor::select::execution_context::ExecutionContext::capture(txn);

    type WorkerOut = (Vec<PlanRow>, Vec<bool>);
    let mut results: Vec<Result<WorkerOut, QueryError>> = (0..effective_degree as usize)
        .map(|_| Ok((Vec::new(), Vec::new())))
        .collect();

    let build_len = build_ref.len();
    let track_matched_ref = track_matched;

    if effective_degree <= 1 {
        // Run inline to avoid the thread-scope overhead for tiny probes.
        let mut out = Vec::new();
        let mut matched = if track_matched {
            vec![false; build_len]
        } else {
            Vec::new()
        };
        for task in probe_tasks.iter() {
            let (rows, task_matched) = streaming_probe_one_worker(
                db,
                txn,
                probe_shape.table,
                task.storage_id,
                projection_ref,
                task.page_lo,
                task.page_hi,
                buckets_ref,
                build_ref,
                bloom_ref,
                probe_key_indices_ref,
                probe_key_collations_ref,
                probe_templates_ref,
                output_positions_ref,
                emit_slots_ref,
                precomputed_rfs_ref,
                compiled_filter_ref,
                null_safe_ref,
                join_lk_ref,
                build_is_left,
                track_matched_ref,
                probe_rows_seen_ref,
                int_bail_ref,
                residual_pre_for_workers,
            )?;
            out.extend(rows);
            if track_matched {
                for (slot, bit) in matched.iter_mut().zip(task_matched) {
                    *slot |= bit;
                }
            }
        }
        record_hash_join_probe_rows(probe_rows_seen.load(Ordering::Relaxed));
        if int_bail.load(Ordering::Relaxed) {
            return Ok(None);
        }
        if let Some(arity) = buckets.typed_arity() {
            record_hash_join_typed_key_fast(arity);
        }
        let mut final_out = out;
        if track_matched {
            streaming_outer_emit_unmatched(
                build_ref,
                &matched,
                probe_templates_ref,
                emit_slots_ref,
                build_is_left,
                &join_lk,
                &mut final_out,
            );
        }
        return Ok(Some(final_out));
    }

    std::thread::scope(|s| {
        let mut handles: Vec<std::thread::ScopedJoinHandle<'_, Result<WorkerOut, QueryError>>> =
            Vec::with_capacity(effective_degree as usize);
        for _ in 0..effective_degree {
            let db_ref = db;
            let worker_context = execution_context.clone();

            let probe_tasks = std::sync::Arc::clone(&probe_tasks);
            let next_task = std::sync::Arc::clone(&next_task);
            handles.push(s.spawn(move || -> Result<WorkerOut, QueryError> {
                let run = || -> Result<WorkerOut, QueryError> {
                    (|| {
                        let mut out = Vec::new();
                        let mut matched = if track_matched_ref {
                            vec![false; build_ref.len()]
                        } else {
                            Vec::new()
                        };
                        loop {
                            let index = next_task.fetch_add(1, Ordering::Relaxed);
                            let Some(task) = probe_tasks.get(index).copied() else {
                                break;
                            };
                            let (rows, task_matched) = streaming_probe_one_worker(
                                db_ref,
                                txn,
                                probe_shape.table,
                                task.storage_id,
                                projection_ref,
                                task.page_lo,
                                task.page_hi,
                                buckets_ref,
                                build_ref,
                                bloom_ref,
                                probe_key_indices_ref,
                                probe_key_collations_ref,
                                probe_templates_ref,
                                output_positions_ref,
                                emit_slots_ref,
                                precomputed_rfs_ref,
                                compiled_filter_ref,
                                null_safe_ref,
                                join_lk_ref,
                                build_is_left,
                                track_matched_ref,
                                probe_rows_seen_ref,
                                int_bail_ref,
                                residual_pre_for_workers,
                            )?;
                            out.extend(rows);
                            if track_matched_ref {
                                for (slot, bit) in matched.iter_mut().zip(task_matched) {
                                    *slot |= bit;
                                }
                            }
                        }
                        Ok((out, matched))
                    })()
                };
                worker_context.run(db, || run())
            }));
        }
        for (i, h) in handles.into_iter().enumerate() {
            results[i] = match h.join() {
                Ok(r) => r,
                Err(_) => Err(error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "hash join streaming probe worker panicked",
                )),
            };
        }
    });

    // Merge per-worker outputs. When `track_matched` is set, OR the
    // per-worker `matched` bitmaps together so we can emit NULL-extended
    // rows for build entries no worker ever matched against.
    let mut out: Vec<PlanRow> = Vec::new();
    let mut matched_merged: Vec<bool> = if track_matched {
        vec![false; build_len]
    } else {
        Vec::new()
    };
    for r in results {
        let (rows, matched) = r?;
        out.extend(rows);
        if track_matched && !matched.is_empty() {
            debug_assert_eq!(matched.len(), build_len);
            for (slot, bit) in matched_merged.iter_mut().zip(matched.iter()) {
                *slot |= *bit;
            }
        }
    }
    record_hash_join_probe_rows(probe_rows_seen.load(Ordering::Relaxed));
    if int_bail.load(Ordering::Relaxed) {
        return Ok(None);
    }
    if let Some(arity) = buckets.typed_arity() {
        record_hash_join_typed_key_fast(arity);
    }
    if track_matched {
        streaming_outer_emit_unmatched(
            build_ref,
            &matched_merged,
            probe_templates_ref,
            emit_slots_ref,
            build_is_left,
            &join_lk,
            &mut out,
        );
    }
    Ok(Some(out))
}

/// Emit NULL-extended rows for every build entry that no probe row
/// matched. Used by the streaming outer hash join when the build side
/// is the preserved one (`LEFT JOIN` with `build=left`, `RIGHT JOIN`
/// with `build=right`). The shape mirrors what `do_in_memory_hash_join`
/// produces via `null_extend_right`/`null_extend_left`, but builds the
/// NULL field vector from the pre-interned `probe_templates` slice so
/// we avoid the per-row `intern_col_meta` round-trip and the
/// `collect_plan_fields` walk over the probe plan tree.
/// S2 fused emit slots (streaming-inner only): join-level projection positions pre-resolved to
/// "column j of the build row" or "probe values index idx + template meta". Probe positions
/// index the scan full projection `values` directly (S2 retained columns are always referenced upstream, so they never
/// fall into the filter-only columns pruned by the Q13 `output_columns_in_projection`),
/// eliminating the positional misalignment a post-hoc gather would hit when both prunings stack.
pub(in crate::engine::executor::select) enum StreamEmitSlot {
    Build(usize),
    Probe { meta: ColMetaRef, values_idx: usize },
}

pub(super) fn streaming_outer_emit_unmatched(
    build_rows: &[PlanRow],
    matched: &[bool],
    probe_templates: &[PlanField],
    emit_slots: Option<&[StreamEmitSlot]>,
    build_is_left: bool,
    join_lk: &std::sync::Arc<RowSchema>,
    out: &mut Vec<PlanRow>,
) {
    if matched.len() != build_rows.len() {
        return;
    }
    let unmatched = matched.iter().filter(|m| !**m).count();
    if unmatched == 0 {
        return;
    }
    out.reserve(unmatched);
    let probe_len = probe_templates.len();
    for (bi, m) in matched.iter().enumerate() {
        if *m {
            continue;
        }
        let build_row = &build_rows[bi];
        let fields: Vec<PlanField> = if let Some(slots) = emit_slots {
            let mut fields = Vec::with_capacity(slots.len());
            for slot in slots {
                match slot {
                    StreamEmitSlot::Build(j) => {
                        if let Some(field) = build_row.fields.get(*j) {
                            fields.push(field.clone());
                        }
                    }
                    StreamEmitSlot::Probe { meta, .. } => fields.push(PlanField {
                        meta: meta.clone(),
                        value: Value::Null,
                    }),
                }
            }
            fields
        } else {
            let mut fields: Vec<PlanField> = Vec::with_capacity(build_row.fields.len() + probe_len);
            if build_is_left {
                fields.extend(build_row.fields.iter().cloned());
                for t in probe_templates {
                    fields.push(PlanField {
                        meta: t.meta.clone(),
                        value: Value::Null,
                    });
                }
            } else {
                for t in probe_templates {
                    fields.push(PlanField {
                        meta: t.meta.clone(),
                        value: Value::Null,
                    });
                }
                fields.extend(build_row.fields.iter().cloned());
            }
            fields
        };
        out.push(PlanRow {
            fields,
            group: None,
            agg_values: None,
            schema: join_lk.clone().into(),
        });
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn streaming_probe_one_worker(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    storage_id: u64,
    projection: &[usize],
    page_lo: u64,
    page_hi: u64,
    buckets: &JoinBuckets,
    build_rows: &[PlanRow],
    bloom: Option<&BloomFilter>,
    probe_key_indices: &[usize],
    probe_key_collations: &[Option<u16>],
    probe_templates: &[PlanField],
    output_positions: &[usize],
    emit_slots: Option<&[StreamEmitSlot]>,
    precomputed_rfs: &[Option<CompiledRuntimeFilterProbe>],
    compiled_filter: Option<&CompiledRowPredicate>,
    null_safe: &[bool],
    join_lk: &std::sync::Arc<RowSchema>,
    build_is_left: bool,
    track_matched: bool,
    probe_rows_seen: &AtomicU64,
    int_bail: &std::sync::atomic::AtomicBool,
    residual_pre: Option<&JoinedResidualPre>,
) -> Result<(Vec<PlanRow>, Vec<bool>), QueryError> {
    debug_assert_eq!(probe_templates.len(), output_positions.len());
    let mut out: Vec<PlanRow> = Vec::new();
    let mut matched_local: Vec<bool> = if track_matched {
        vec![false; build_rows.len()]
    } else {
        Vec::new()
    };
    // Lightweight visit-side error/abort signalling matches the
    // pattern used by `worker_projected_streaming`: the visitor
    // closure cannot return an arbitrary error to interrupt the scan
    // cleanly, so propagate via `RefCell`.
    let visit_err: std::cell::RefCell<Option<QueryError>> = std::cell::RefCell::new(None);
    let stop_flag = std::cell::Cell::new(false);
    let mut local_seen: u64 = 0;
    let mut typed_probe_scratch = Vec::with_capacity(probe_key_indices.len());
    db.scan_rows_columns_storage_streaming_range(
        &table.schema,
        storage_id,
        txn,
        projection,
        page_lo,
        page_hi,
        |values: &[Value]| -> Result<(), QueryError> {
            if stop_flag.get() {
                return Ok(());
            }
            local_seen += 1;
            // Periodic cancel check — once per ~64K visited rows is
            // plenty given typical visit cost.
            if local_seen & 0xFFFF == 0 {
                if let Err(e) = crate::engine::cancel::check_cancelled() {
                    *visit_err.borrow_mut() = Some(e);
                    stop_flag.set(true);
                    return Ok(());
                }
            }
            if let Some(cp) = compiled_filter {
                match cp.eval(values) {
                    Ok(true) => {}
                    Ok(false) => return Ok(()),
                    Err(e) => {
                        *visit_err.borrow_mut() = Some(e);
                        stop_flag.set(true);
                        return Ok(());
                    }
                }
            }
            if !runtime_filters_admit_values_indexed(precomputed_rfs, values) {
                return Ok(());
            }
            // Compute join key from raw values & look up bucket matches.
            let matches = match buckets {
                JoinBuckets::Gen(..) | JoinBuckets::GenChained { .. } => {
                    let mut key: HashJoinKey =
                        smallvec::SmallVec::with_capacity(probe_key_indices.len());
                    let mut has_unsafe_null = false;
                    for (i, &col_idx) in probe_key_indices.iter().enumerate() {
                        let v = &values[col_idx];
                        let is_null = matches!(v, Value::Null);
                        let safe = null_safe.get(i).copied().unwrap_or(false);
                        if is_null && !safe {
                            has_unsafe_null = true;
                            break;
                        }
                        key.push(ValueKey::from_with_collation(
                            v,
                            probe_key_collations.get(i).copied().flatten(),
                        ));
                    }
                    if has_unsafe_null {
                        record_hash_join_null_skipped();
                        return Ok(());
                    }
                    if let Some(bf) = bloom {
                        if !bf.contains(key.as_slice()) {
                            record_hash_join_bloom_filtered();
                            return Ok(());
                        }
                    }
                    let Some(matches) = buckets.gen_matches(&key) else {
                        return Ok(());
                    };
                    matches
                }
                JoinBuckets::Int { map, next } => {
                    // A2: the gate guarantees key columns are declared integer; any other value variant is a
                    // declared/value mismatch — set bail to abort the whole fast path; the caller
                    // falls back to the legacy materializing path and recomputes (the probe scan is read-only and re-entrant).
                    let key = match &values[probe_key_indices[0]] {
                        Value::Int(v) => *v as i128,
                        Value::UInt(v) => *v as i128,
                        Value::Null => {
                            record_hash_join_null_skipped();
                            return Ok(());
                        }
                        _ => {
                            int_bail.store(true, Ordering::Relaxed);
                            stop_flag.set(true);
                            return Ok(());
                        }
                    };
                    if let Some(bf) = bloom {
                        if !bf.contains(&key) {
                            record_hash_join_bloom_filtered();
                            return Ok(());
                        }
                    }
                    let Some(&(head, _)) = map.get(&key) else {
                        return Ok(());
                    };
                    BucketMatches::Chain { next, cursor: head }
                }
                JoinBuckets::IntComposite { map, next, arity } => {
                    debug_assert_eq!(*arity, probe_key_indices.len());
                    typed_probe_scratch.clear();
                    for &index in probe_key_indices {
                        match &values[index] {
                            Value::Int(value) => typed_probe_scratch.push(*value),
                            Value::UInt(value) if *value <= i64::MAX as u64 => {
                                typed_probe_scratch.push(*value as i64)
                            }
                            Value::Null => {
                                record_hash_join_null_skipped();
                                return Ok(());
                            }
                            _ => {
                                int_bail.store(true, Ordering::Relaxed);
                                stop_flag.set(true);
                                return Ok(());
                            }
                        }
                    }
                    if let Some(filter) = bloom {
                        if !filter.contains(&typed_probe_scratch) {
                            record_hash_join_bloom_filtered();
                            return Ok(());
                        }
                    }
                    let Some(&(head, _)) = map.get(&typed_probe_scratch) else {
                        return Ok(());
                    };
                    BucketMatches::Chain { next, cursor: head }
                }
            };
            for bi in matches {
                let build_row = &build_rows[bi];
                // WS-P0-3: the joined residual is evaluated before emit. For LEFT/RIGHT preserved sides a
                // (build, probe) pair rejected by the residual does not count as a match, so
                // matched_local is not set here either — a build row that passed equality+residual on no probe
                // still takes the unmatched NULL-extension path.
                if let Some(pre) = residual_pre {
                    match pre.eval_pair_streaming(build_row, values, build_is_left) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(e) => {
                            *visit_err.borrow_mut() = Some(e);
                            stop_flag.set(true);
                            return Ok(());
                        }
                    }
                }
                let fields = if let Some(slots) = emit_slots {
                    // S2 fusion: assemble retained columns directly per the join-level projection. Probe slots
                    // index the scan full projection `values` (see the StreamEmitSlot docs).
                    let mut fields = Vec::with_capacity(slots.len());
                    for slot in slots {
                        match slot {
                            StreamEmitSlot::Build(j) => {
                                if let Some(field) = build_row.fields.get(*j) {
                                    fields.push(field.clone());
                                }
                            }
                            StreamEmitSlot::Probe { meta, values_idx } => {
                                fields.push(PlanField {
                                    meta: meta.clone(),
                                    value: values[*values_idx].clone(),
                                });
                            }
                        }
                    }
                    fields
                } else {
                    let probe_len = probe_templates.len();
                    let build_len = build_row.fields.len();
                    let mut fields = Vec::with_capacity(build_len + probe_len);
                    if build_is_left {
                        fields.extend(build_row.fields.iter().cloned());
                        for (i, t) in probe_templates.iter().enumerate() {
                            // `output_positions[i]` indexes into `values`,
                            // skipping filter-only projection slots so the
                            // per-row `Value::clone` (typically a 50+ B
                            // `Value::String` for wide VARCHAR columns) is
                            // avoided entirely for those positions.
                            fields.push(PlanField {
                                meta: t.meta.clone(),
                                value: values[output_positions[i]].clone(),
                            });
                        }
                    } else {
                        for (i, t) in probe_templates.iter().enumerate() {
                            fields.push(PlanField {
                                meta: t.meta.clone(),
                                value: values[output_positions[i]].clone(),
                            });
                        }
                        fields.extend(build_row.fields.iter().cloned());
                    }
                    fields
                };
                out.push(PlanRow {
                    fields,
                    group: None,
                    agg_values: None,
                    schema: join_lk.clone().into(),
                });
                if track_matched {
                    // Safe: `matched_local` was sized to `build_rows.len()`
                    // when `track_matched` was true. Bucket indices come
                    // straight from the build-row enumeration above so
                    // they're always in-range.
                    if let Some(slot) = matched_local.get_mut(bi) {
                        *slot = true;
                    }
                }
            }
            Ok(())
        },
    )?;
    if let Some(err) = visit_err.into_inner() {
        return Err(err);
    }
    probe_rows_seen.fetch_add(local_seen, Ordering::Relaxed);
    Ok((out, matched_local))
}

/// Parallel probe for the common INNER/no-residual case.
///
/// The older parallel path hash-partitions both inputs, then each worker
/// rebuilds a private bucket map for its partition. That helps very large
/// builds, but TPC-H Q5 spends a lot of time on the extra key computation
/// and repartition pass before producing rows. This path builds one shared
/// immutable map over the build side and slices the probe side across
/// workers, so every input row computes its join key once in the join
/// operator and no `Vec<PlanRow>` repartitioning is needed.
pub(super) fn try_execute_inner_hash_join_parallel_probe(
    db: &Arc<Database>,
    txn: TxnView,
    spec: &HashJoinSpec<'_>,
    limits: &HashJoinLimits,
    build_rows: &[PlanRow],
    probe_rows: &[PlanRow],
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    // C1: beyond INNER, probe-preserved LEFT/RIGHT are also allowed (each probe row independently
    // decides match/NULL-extension; no shared state across workers).
    // WS-P1-1: build-preserved LEFT/RIGHT is parallelized too — per-worker local matched
    // bitmaps, OR-merged on the main thread, which then emits the NULL-extended rows (see the merge section at the end).
    let probe_preserved_outer = spec.probe_is_outer_preserved();
    let build_preserved_outer = spec.build_is_outer_preserved();
    let build_preserved_enabled =
        build_preserved_outer && !parallel_probe_build_preserved_disabled();
    if hash_join_parallel_probe_disabled()
        || limits.parallelism <= 1
        || !(spec.join_type == crate::parser::JoinType::Inner
            || probe_preserved_outer
            || build_preserved_enabled)
    {
        return Ok(None);
    }
    let total_rows = build_rows.len() as u64 + probe_rows.len() as u64;
    if total_rows < limits.parallelism_threshold || probe_rows.len() < limits.parallelism as usize {
        return Ok(None);
    }
    // C1: the build-size cap is measured in bytes; with the legacy row-count GUC set explicitly, rows are used.
    match hash_join_parallel_probe_max_build_rows_opt() {
        Some(cap) => {
            if build_rows.len() > cap {
                return Ok(None);
            }
        }
        None => {
            if estimate_rows_bytes(build_rows) > hash_join_parallel_probe_max_build_bytes() {
                return Ok(None);
            }
        }
    }
    if probe_rows.is_empty() {
        return Ok(Some(Vec::new()));
    }
    if build_rows.is_empty() && !probe_preserved_outer {
        return Ok(Some(Vec::new()));
    }

    // C1: residual support. The joined part must compile into the two-row pre-evaluator (A1) to go
    // parallel, otherwise fall back to the serial path (workers never do the legacy "concatenate then evaluate");
    // build_only is filtered at bucket-table build, probe_only row-by-row inside the worker.
    let residual_split = if spec.join_type == crate::parser::JoinType::Inner {
        split_hash_join_residual_by_side(spec)
    } else {
        SplitHashResidual::joined_only(spec.residual)
    };
    let residual_pre = compile_joined_residual_pre(spec, &residual_split, build_rows, probe_rows);
    if residual_split.joined.is_some() && residual_pre.is_none() {
        return Ok(None);
    }
    if residual_pre.is_some() {
        record_hash_join_residual_precheck();
    }

    let typed_plan = typed_int_key_plan(spec, build_rows, probe_rows);
    let (buckets, bloom) = build_join_buckets(
        spec.build_keys,
        spec.null_safe,
        spec.build_key_plan.as_ref(),
        build_rows,
        typed_plan
            .as_ref()
            .map(|plan| plan.build_indices.as_slice()),
        limits.bloom_worthwhile(build_rows.len()),
        true,
        residual_split.build_only.as_ref(),
    )?;
    // All-NULL build keys match nothing, but a preserved build side still
    // emits every row NULL-extended after the (matchless) probe pass.
    if buckets.is_empty() && !probe_preserved_outer && !build_preserved_outer {
        return Ok(Some(Vec::new()));
    }
    let typed_probe_indices = typed_plan
        .as_ref()
        .filter(|_| buckets.is_typed())
        .map(|plan| plan.probe_indices.to_vec())
        .unwrap_or_default();
    let int_bail = std::sync::atomic::AtomicBool::new(false);

    // C2: adaptive bloom for large builds — the bitmap is shared read-only across workers, so the
    // main thread samples the probe head before spawning (no in-worker sampling).
    let mut bloom = bloom;
    if AdaptiveBloomSampler::gate(limits, bloom.is_some(), build_rows.len())
        && probe_rows.len() as u64 > AdaptiveBloomSampler::MIN_REMAINING
    {
        let sample_n = (AdaptiveBloomSampler::SAMPLE as usize).min(probe_rows.len());
        let mut misses = 0usize;
        let mut aborted = false;
        let mut typed_scratch = Vec::with_capacity(typed_probe_indices.len());
        for probe_row in &probe_rows[..sample_n] {
            let matched = match &buckets {
                JoinBuckets::Gen(..) | JoinBuckets::GenChained { .. } => {
                    match compute_join_key_with_plan(
                        spec.probe_keys,
                        spec.null_safe,
                        spec.probe_key_plan.as_ref(),
                        probe_row,
                    )? {
                        Some(key) => buckets.gen_contains(&key),
                        None => false,
                    }
                }
                JoinBuckets::Int { map, .. } => match int_key_at(probe_row, typed_probe_indices[0])
                {
                    IntKeyOut::Key(key) => map.contains_key(&key),
                    IntKeyOut::Null => false,
                    IntKeyOut::Bail => {
                        // Declared/value mismatch: leave it to the worker Bail flow for a wholesale fallback.
                        aborted = true;
                        break;
                    }
                },
                JoinBuckets::IntComposite { map, .. } => {
                    match DynamicIntCodec::extract(
                        probe_row,
                        &typed_probe_indices,
                        &mut typed_scratch,
                    ) {
                        TypedKeyState::Valid => map.contains_key(&typed_scratch),
                        TypedKeyState::Null => false,
                        TypedKeyState::Bail(_) => {
                            aborted = true;
                            break;
                        }
                    }
                }
            };
            if !matched {
                misses += 1;
            }
        }
        if !aborted && misses * 2 >= sample_n {
            bloom = Some(bloom_from_buckets(&buckets));
            record_hash_join_bloom_built();
        }
    }

    let workers = (limits.parallelism as usize).min(probe_rows.len()).max(1);
    record_hash_join_parallel_partitions(workers as u64);
    // Probe-side parallelism uses a shared atomic cursor instead of a
    // static `chunk = probe_rows.len() / workers` slice per worker. The
    // static-slice form left the rest of the pool idle whenever one
    // chunk hit a hash-table hot spot or a slow predicate; with the
    // cursor every idle worker just claims the next batch until the
    // probe array is exhausted. Batch size trades scheduling overhead
    // against load-balancing granularity — 1024 rows is large enough
    // that atomic increments are noise vs. join work, small enough
    // that a slow batch can be backfilled by a peer mid-scan.
    const PROBE_BATCH: usize = 1024;
    let cursor = AtomicUsize::new(0);
    let probe_total = probe_rows.len();
    let bloom_ref = bloom.as_ref();

    let execution_context =
        crate::engine::executor::select::execution_context::ExecutionContext::capture(txn);

    // WS-P1-1: workers return (rows, matched_local). matched_local is non-empty only in the
    // build-preserved scenario, recording the build row indices this worker probes matched.
    type WorkerOut = (Vec<PlanRow>, Vec<bool>);
    let mut results: Vec<Result<WorkerOut, QueryError>> =
        (0..workers).map(|_| Ok((Vec::new(), Vec::new()))).collect();
    let join_lk = new_join_lookup();

    let build_len = build_rows.len();
    let track_matched = build_preserved_enabled;
    let strict_output_accounting = strict_plan_row_output_accounting_enabled();
    if track_matched {
        record_hash_join_parallel_build_preserved();
    }
    std::thread::scope(|s| {
        let mut handles: Vec<std::thread::ScopedJoinHandle<'_, Result<WorkerOut, QueryError>>> =
            Vec::with_capacity(workers);
        for _ in 0..workers {
            let buckets_ref = &buckets;
            let build_ref = build_rows;
            let spec_ref = spec;
            let join_lk_ref = &join_lk;
            let worker_context = execution_context.clone();

            let probe_ref = probe_rows;
            let cursor_ref = &cursor;
            let int_bail_ref = &int_bail;
            let probe_only_ref = residual_split.probe_only.as_ref();
            let residual_pre_ref = residual_pre.as_ref();

            let typed_probe_indices_ref = &typed_probe_indices;
            handles.push(s.spawn(move || -> Result<WorkerOut, QueryError> {
                let run = || -> Result<WorkerOut, QueryError> {
                    (|| {
                        let mut out = Vec::new();
                        // WS-P1-1: per-worker matched bitmap. Sized to build_len when the
                        // build side is preserved, otherwise empty (avoiding a useless allocation).
                        let mut matched_local: Vec<bool> = if track_matched {
                            vec![false; build_len]
                        } else {
                            Vec::new()
                        };
                        let mut typed_scratch = Vec::with_capacity(typed_probe_indices_ref.len());
                        loop {
                            if int_bail_ref.load(Ordering::Relaxed) {
                                break;
                            }
                            let lo = cursor_ref.fetch_add(PROBE_BATCH, Ordering::Relaxed);
                            if lo >= probe_total {
                                break;
                            }
                            let hi = (lo + PROBE_BATCH).min(probe_total);
                            crate::engine::cancel::check_cancelled()?;
                            for probe_row in &probe_ref[lo..hi] {
                                // C1: probe_only residual (split out for Inner only) —
                                // failing it drops the whole row, matching do_in_memory.
                                if let Some(expr) = probe_only_ref {
                                    if !eval_plan_bool(expr, probe_row)? {
                                        continue;
                                    }
                                }
                                // NULL key / bloom miss / bucket miss all count as
                                // "no match": Inner drops the row, probe-preserved sides NULL-extend it.
                                let matches = match buckets_ref {
                                    JoinBuckets::Gen(..) | JoinBuckets::GenChained { .. } => {
                                        match compute_join_key_with_plan(
                                            spec_ref.probe_keys,
                                            spec_ref.null_safe,
                                            spec_ref.probe_key_plan.as_ref(),
                                            probe_row,
                                        )? {
                                            None => None,
                                            Some(key) => {
                                                let bloom_hit = bloom_ref
                                                    .is_none_or(|bf| bf.contains(key.as_slice()));
                                                if !bloom_hit {
                                                    record_hash_join_bloom_filtered();
                                                    None
                                                } else {
                                                    buckets_ref.gen_matches(&key)
                                                }
                                            }
                                        }
                                    }
                                    JoinBuckets::Int { map, next } => {
                                        match int_key_at(probe_row, typed_probe_indices_ref[0]) {
                                            IntKeyOut::Key(key) => {
                                                let bloom_hit =
                                                    bloom_ref.is_none_or(|bf| bf.contains(&key));
                                                if !bloom_hit {
                                                    record_hash_join_bloom_filtered();
                                                    None
                                                } else {
                                                    map.get(&key).map(|&(head, _)| {
                                                        BucketMatches::Chain { next, cursor: head }
                                                    })
                                                }
                                            }
                                            IntKeyOut::Null => None,
                                            IntKeyOut::Bail => {
                                                // Declared/value mismatch: abandon the whole fast path;
                                                // the caller falls back (this output is discarded wholesale).
                                                int_bail_ref.store(true, Ordering::Relaxed);
                                                return Ok((out, matched_local));
                                            }
                                        }
                                    }
                                    JoinBuckets::IntComposite { map, next, .. } => {
                                        match DynamicIntCodec::extract(
                                            probe_row,
                                            typed_probe_indices_ref,
                                            &mut typed_scratch,
                                        ) {
                                            TypedKeyState::Valid => {
                                                let bloom_hit = bloom_ref.is_none_or(|filter| {
                                                    filter.contains(&typed_scratch)
                                                });
                                                if !bloom_hit {
                                                    record_hash_join_bloom_filtered();
                                                    None
                                                } else {
                                                    map.get(&typed_scratch).map(|&(head, _)| {
                                                        BucketMatches::Chain { next, cursor: head }
                                                    })
                                                }
                                            }
                                            TypedKeyState::Null => None,
                                            TypedKeyState::Bail(_) => {
                                                int_bail_ref.store(true, Ordering::Relaxed);
                                                return Ok((out, matched_local));
                                            }
                                        }
                                    }
                                };
                                let mut probe_matched = false;
                                if let Some(matches) = matches {
                                    for bi in matches {
                                        let build_row = &build_ref[bi];
                                        // C1: the joined residual is decided before concatenation via the A1
                                        // pre-evaluator (shapes that failed to compile never reach
                                        // this function).
                                        if let Some(pre) = residual_pre_ref {
                                            if !pre.eval_build_probe(
                                                spec_ref.build_is_left,
                                                build_row,
                                                probe_row,
                                            )? {
                                                continue;
                                            }
                                        }
                                        probe_matched = true;
                                        // WS-P1-1: only matches that actually pass the residual
                                        // set matched_local; unmatched build rows are NULL-emitted
                                        // uniformly after the main thread OR-merge.
                                        if track_matched {
                                            if let Some(slot) = matched_local.get_mut(bi) {
                                                *slot = true;
                                            }
                                        }
                                        push_materialized_plan_row(
                                            &mut out,
                                            spec_ref.emit_joined(build_row, probe_row, join_lk_ref),
                                        )?;
                                    }
                                }
                                if !probe_matched && probe_preserved_outer {
                                    push_materialized_plan_row(
                                        &mut out,
                                        spec_ref.emit_probe_unmatched(probe_row),
                                    )?;
                                }
                            }
                        }
                        Ok((out, matched_local))
                    })()
                };
                worker_context.run(db, || {
                    let _materialization_scope = MaterializationChargeScope::enter();
                    with_strict_plan_row_output_accounting(strict_output_accounting, run)
                })
            }));
        }
        for (i, handle) in handles.into_iter().enumerate() {
            results[i] = match handle.join() {
                Ok(r) => r,
                Err(_) => Err(error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "hash join probe worker panicked",
                )),
            };
        }
    });

    let mut out = Vec::new();
    // WS-P1-1: when the build side is preserved, OR-merge the matched bitmaps of all workers.
    let mut merged_matched: Vec<bool> = if track_matched {
        vec![false; build_len]
    } else {
        Vec::new()
    };
    for result in results {
        let (rows, matched) = result?;
        out.extend(rows);
        if track_matched && !matched.is_empty() {
            debug_assert_eq!(matched.len(), build_len);
            for (slot, bit) in merged_matched.iter_mut().zip(matched.iter()) {
                *slot |= *bit;
            }
        }
    }
    if int_bail.load(Ordering::Relaxed) {
        return Ok(None);
    }
    if let Some(arity) = buckets.typed_arity() {
        record_hash_join_typed_key_fast(arity);
    }
    if track_matched {
        // WS-P1-1: unmatched build rows are NULL-extended in one batch (serial; the count is usually
        // << the total matched output, so not a bottleneck).
        for (bi, m) in merged_matched.iter().enumerate() {
            if !*m {
                push_materialized_plan_row(&mut out, spec.emit_build_unmatched(&build_rows[bi]))?;
            }
        }
    }
    Ok(Some(out))
}

#[derive(Default)]
pub(in crate::engine::executor::select) struct SplitHashResidual {
    pub(in crate::engine::executor::select) build_only: Option<Expr>,
    pub(in crate::engine::executor::select) probe_only: Option<Expr>,
    pub(in crate::engine::executor::select) joined: Option<Expr>,
}

impl SplitHashResidual {
    pub(super) fn joined_only(residual: Option<&Expr>) -> Self {
        Self {
            build_only: None,
            probe_only: None,
            joined: residual.cloned(),
        }
    }
}

pub(super) fn split_hash_join_residual_by_side(spec: &HashJoinSpec<'_>) -> SplitHashResidual {
    let Some(residual) = spec.residual else {
        return SplitHashResidual::default();
    };
    let mut build_fields = Vec::new();
    let mut probe_fields = Vec::new();
    if spec.build_is_left {
        collect_plan_fields(spec.left_plan, &mut build_fields);
        collect_plan_fields(spec.right_plan, &mut probe_fields);
    } else {
        collect_plan_fields(spec.right_plan, &mut build_fields);
        collect_plan_fields(spec.left_plan, &mut probe_fields);
    }
    split_hash_join_residual_for_fields(residual, &build_fields, &probe_fields)
}

/// Same semantics as `split_hash_join_residual_by_side`, but the two field lists are supplied by the caller
/// (chained streaming joins split directly with each level output column metadata).
pub(in crate::engine::executor::select) fn split_hash_join_residual_for_fields(
    residual: &Expr,
    build_fields: &[ColMeta],
    probe_fields: &[ColMeta],
) -> SplitHashResidual {
    let build_refs = field_ref_set(build_fields);
    let probe_refs = field_ref_set(probe_fields);
    let mut build_only = Vec::new();
    let mut probe_only = Vec::new();
    let mut joined = Vec::new();
    for conjunct in expr_conjuncts(residual) {
        if expr_references_only_field_set(conjunct, &build_refs, &probe_refs) {
            build_only.push(conjunct.clone());
        } else if expr_references_only_field_set(conjunct, &probe_refs, &build_refs) {
            probe_only.push(conjunct.clone());
        } else {
            joined.push(conjunct.clone());
        }
    }
    SplitHashResidual {
        build_only: crate::planner::combine_conjuncts(build_only),
        probe_only: crate::planner::combine_conjuncts(probe_only),
        joined: crate::planner::combine_conjuncts(joined),
    }
}

pub(super) fn expr_references_only_field_set(
    expr: &Expr,
    own: &FieldRefSet,
    other: &FieldRefSet,
) -> bool {
    let mut saw_column = false;
    expr_refs_field_set_inner(expr, own, other, &mut saw_column) && saw_column
}

pub(super) fn expr_refs_field_set_inner(
    expr: &Expr,
    own: &FieldRefSet,
    other: &FieldRefSet,
    saw_column: &mut bool,
) -> bool {
    crate::parser::visit::try_inspect_value_expr::<()>(expr, |expr| match expr {
        Expr::Identifier(name) => {
            let col = name.to_ascii_lowercase();
            if other.unqualified.contains(&col) || !own.unqualified.contains(&col) {
                return Err(());
            }
            *saw_column = true;
            Ok(true)
        }
        Expr::CompoundIdentifier(parts) => {
            let Some(column) = parts.last() else {
                return Err(());
            };
            let col = column.to_ascii_lowercase();
            if parts.len() >= 2 {
                let qualifier = parts[parts.len() - 2].to_ascii_lowercase();
                let key = (qualifier, col);
                if other.qualified.contains(&key) || !own.qualified.contains(&key) {
                    return Err(());
                }
            } else if other.unqualified.contains(&col) || !own.unqualified.contains(&col) {
                return Err(());
            }
            *saw_column = true;
            Ok(true)
        }
        Expr::Subquery(_)
        | Expr::InSubquery { .. }
        | Expr::QuantifiedComparison { .. }
        | Expr::Exists { .. } => Err(()),
        _ => Ok(true),
    })
    .is_ok()
}

pub(super) fn do_in_memory_hash_semi_anti_keyset_join(
    spec: &HashJoinSpec<'_>,
    build_rows: Vec<PlanRow>,
    probe_rows: Vec<PlanRow>,
    limits: &HashJoinLimits,
    count_bloom_built: bool,
    build_only_residual: Option<&Expr>,
    probe_only_residual: Option<&Expr>,
) -> Result<Vec<PlanRow>, QueryError> {
    let mut bloom: Option<BloomFilter> = if limits.bloom_worthwhile(build_rows.len()) {
        Some(BloomFilter::new(build_rows.len().max(1)))
    } else {
        None
    };
    if bloom.is_some() && count_bloom_built {
        record_hash_join_bloom_built();
    }

    let mut keys: hashbrown::HashSet<HashJoinKey, ahash::RandomState> =
        hashbrown::HashSet::with_capacity_and_hasher(build_rows.len(), ahash::RandomState::new());
    for row in build_rows {
        if build_only_residual
            .map(|expr| eval_plan_bool(expr, &row))
            .transpose()?
            .is_some_and(|keep| !keep)
        {
            continue;
        }
        if let Some(key) = compute_join_key_with_plan(
            spec.build_keys,
            spec.null_safe,
            spec.build_key_plan.as_ref(),
            &row,
        )? {
            if let Some(bf) = bloom.as_mut() {
                bf.insert(key.as_slice());
            }
            keys.insert(key);
        } else {
            record_hash_join_null_skipped();
        }
    }

    let mut out = Vec::new();
    for probe_row in &probe_rows {
        let probe_residual_passes = probe_only_residual
            .map(|expr| eval_plan_bool(expr, probe_row))
            .transpose()?
            .unwrap_or(true);
        let mut matched = false;
        if probe_residual_passes {
            if let Some(key) = compute_join_key_with_plan(
                spec.probe_keys,
                spec.null_safe,
                spec.probe_key_plan.as_ref(),
                probe_row,
            )? {
                let bloom_hit = bloom.as_ref().is_none_or(|bf| bf.contains(key.as_slice()));
                if !bloom_hit {
                    record_hash_join_bloom_filtered();
                }
                matched = bloom_hit && keys.contains(&key);
            }
        }
        match spec.join_type {
            crate::parser::JoinType::Semi if matched => out.push(spec.emit_probe_row(probe_row)),
            crate::parser::JoinType::Anti if !matched => out.push(spec.emit_probe_row(probe_row)),
            _ => {}
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SemiAntiNeqResidual {
    pub(super) build_idx: usize,
    pub(super) probe_idx: usize,
}

#[derive(Debug, Clone)]
pub(super) struct NeqBuildSummary {
    pub(super) non_null: u32,
    pub(super) first: ValueKey,
    pub(super) all_same: bool,
}

pub(super) fn compile_semi_anti_neq_residual(
    spec: &HashJoinSpec<'_>,
    residual: Option<&Expr>,
) -> Option<SemiAntiNeqResidual> {
    let Expr::Binary {
        left,
        op: BinaryOp::NotEq,
        right,
    } = residual?
    else {
        return None;
    };

    let mut build_fields = Vec::new();
    let mut probe_fields = Vec::new();
    if spec.build_is_left {
        collect_plan_fields(spec.left_plan, &mut build_fields);
        collect_plan_fields(spec.right_plan, &mut probe_fields);
    } else {
        collect_plan_fields(spec.right_plan, &mut build_fields);
        collect_plan_fields(spec.left_plan, &mut probe_fields);
    }
    let build_lookup = FieldLookup::build(&build_fields);
    let probe_lookup = FieldLookup::build(&probe_fields);

    let left_build = resolve_hash_join_key_index(left, &build_lookup);
    let left_probe = resolve_hash_join_key_index(left, &probe_lookup);
    let right_build = resolve_hash_join_key_index(right, &build_lookup);
    let right_probe = resolve_hash_join_key_index(right, &probe_lookup);

    match (left_build, left_probe, right_build, right_probe) {
        (Some(build_idx), None, None, Some(probe_idx))
        | (None, Some(probe_idx), Some(build_idx), None) => {
            let build = &build_fields[build_idx];
            let probe = &probe_fields[probe_idx];
            // The summary compares canonical keys instead of evaluating the
            // residual. Mixed comparison domains and nondefault collations
            // must use the ordinary residual evaluator.
            let exact_numeric = |kind| {
                matches!(
                    kind,
                    ColumnType::Tiny
                        | ColumnType::Short
                        | ColumnType::MediumInt
                        | ColumnType::Long
                        | ColumnType::LongLong
                        | ColumnType::Bit { .. }
                        | ColumnType::Decimal { .. }
                )
            };
            let approximate_numeric = |kind| matches!(kind, ColumnType::Float | ColumnType::Double);
            let same_domain = build.column_type == probe.column_type
                || (exact_numeric(build.column_type) && exact_numeric(probe.column_type))
                || (approximate_numeric(build.column_type)
                    && approximate_numeric(probe.column_type));
            let default_collation = crate::charset::default_collation_info().id;
            let text_uses_default = !matches!(build.column_type, ColumnType::VarString)
                || (build.source.collation_id.unwrap_or(default_collation) == default_collation
                    && probe.source.collation_id.unwrap_or(default_collation) == default_collation);
            if !same_domain || !text_uses_default {
                return None;
            }
            Some(SemiAntiNeqResidual {
                build_idx,
                probe_idx,
            })
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn do_in_memory_hash_semi_anti_neq_join(
    spec: &HashJoinSpec<'_>,
    build_rows: Vec<PlanRow>,
    probe_rows: Vec<PlanRow>,
    limits: &HashJoinLimits,
    count_bloom_built: bool,
    build_only_residual: Option<&Expr>,
    probe_only_residual: Option<&Expr>,
    neq: &SemiAntiNeqResidual,
) -> Result<Vec<PlanRow>, QueryError> {
    let mut bloom: Option<BloomFilter> = if limits.bloom_worthwhile(build_rows.len()) {
        Some(BloomFilter::new(build_rows.len().max(1)))
    } else {
        None
    };
    if bloom.is_some() && count_bloom_built {
        record_hash_join_bloom_built();
    }

    let mut summaries: hashbrown::HashMap<HashJoinKey, NeqBuildSummary, ahash::RandomState> =
        hashbrown::HashMap::with_capacity_and_hasher(build_rows.len(), ahash::RandomState::new());
    for row in build_rows {
        if build_only_residual
            .map(|expr| eval_plan_bool(expr, &row))
            .transpose()?
            .is_some_and(|keep| !keep)
        {
            continue;
        }
        let Some(key) = compute_join_key_with_plan(
            spec.build_keys,
            spec.null_safe,
            spec.build_key_plan.as_ref(),
            &row,
        )?
        else {
            record_hash_join_null_skipped();
            continue;
        };
        if let Some(bf) = bloom.as_mut() {
            bf.insert(key.as_slice());
        }
        let Some(field) = row.fields.get(neq.build_idx) else {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "semi/anti residual build column index out of bounds",
            ));
        };
        let value = field.value_ref();
        if matches!(value, Value::Null) {
            summaries.entry(key).or_insert_with(|| NeqBuildSummary {
                non_null: 0,
                first: ValueKey::Null,
                all_same: true,
            });
            continue;
        }
        let value_key = ValueKey::from(value);
        summaries
            .entry(key)
            .and_modify(|state| {
                state.non_null = state.non_null.saturating_add(1);
                if state.non_null == 1 {
                    state.first = value_key.clone();
                    state.all_same = true;
                } else if state.first != value_key {
                    state.all_same = false;
                }
            })
            .or_insert_with(|| NeqBuildSummary {
                non_null: 1,
                first: value_key,
                all_same: true,
            });
    }

    let mut out = Vec::new();
    for probe_row in &probe_rows {
        let probe_residual_passes = probe_only_residual
            .map(|expr| eval_plan_bool(expr, probe_row))
            .transpose()?
            .unwrap_or(true);
        let mut matched = false;
        if probe_residual_passes {
            if let Some(key) = compute_join_key_with_plan(
                spec.probe_keys,
                spec.null_safe,
                spec.probe_key_plan.as_ref(),
                probe_row,
            )? {
                let bloom_hit = bloom.as_ref().is_none_or(|bf| bf.contains(key.as_slice()));
                if !bloom_hit {
                    record_hash_join_bloom_filtered();
                }
                if bloom_hit {
                    if let Some(summary) = summaries.get(&key) {
                        if summary.non_null > 0 {
                            let Some(field) = probe_row.fields.get(neq.probe_idx) else {
                                return Err(error(
                                    crate::error::catalog::ER_GET_ERRNO,
                                    "semi/anti residual probe column index out of bounds",
                                ));
                            };
                            let value = field.value_ref();
                            if !matches!(value, Value::Null) {
                                let value_key = ValueKey::from(value);
                                matched = !summary.all_same || summary.first != value_key;
                            }
                        }
                    }
                }
            }
        }
        match spec.join_type {
            crate::parser::JoinType::Semi if matched => out.push(spec.emit_probe_row(probe_row)),
            crate::parser::JoinType::Anti if !matched => out.push(spec.emit_probe_row(probe_row)),
            _ => {}
        }
    }
    Ok(out)
}

pub(super) fn try_execute_hash_semi_anti_build_keyset(
    db: &Arc<Database>,
    txn: TxnView,
    build_plan: &PhysicalPlan,
    probe_rows: &[PlanRow],
    probe_keys: &[Expr],
    build_keys: &[Expr],
    null_safe: &[bool],
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    let Some((table, filter, projected_columns)) = build_keyset_scan_info(build_plan) else {
        return Ok(None);
    };
    let Some((key_idx, index, data_type, collation)) =
        build_keyset_index_choice(table, build_keys, probe_keys)
    else {
        return Ok(None);
    };
    if null_safe.get(key_idx).copied().unwrap_or(false) {
        return Ok(None);
    }

    let max_keys = crate::util::env::var("MYDB_HASH_SEMI_KEYSET_MAX_KEYS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    if max_keys == 0 {
        return Ok(None);
    }
    let mut keys: hashbrown::HashSet<IndexKey, ahash::RandomState> =
        hashbrown::HashSet::with_capacity_and_hasher(
            probe_rows.len().min(max_keys),
            ahash::RandomState::new(),
        );
    for row in probe_rows {
        let value = eval_plan_expr(&probe_keys[key_idx], row)?;
        if matches!(value, Value::Null) {
            continue;
        }
        keys.insert(IndexKey::from(vec![key_part_for_data_type_with_collation(
            data_type, collation, &value,
        )?]));
        if keys.len() > max_keys {
            return Ok(None);
        }
    }
    if keys.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let mut row_ids = Vec::new();
    for key in keys {
        row_ids.extend(db.lookup_index_visible(&table.schema, index, key, txn)?);
    }
    if row_ids.is_empty() {
        return Ok(Some(Vec::new()));
    }
    row_ids.sort_unstable();
    row_ids.dedup();

    let projection_owned;
    let projection = if let Some(projected) = projected_columns {
        projected
    } else {
        projection_owned = (0..table.schema.columns.len()).collect::<Vec<_>>();
        projection_owned.as_slice()
    };
    let heap = db.cached_heap_handle(table.schema.id)?;
    let mut out = Vec::new();
    let mut eval_ctx = EvalQueryContext::default();
    for (_row_id, values) in
        db.read_row_columns_many_with_handle(&heap, &table.schema, &row_ids, txn, projection)?
    {
        let row = if projected_columns.is_some() {
            projected_values_to_plan_row(table, projection, values)?
        } else {
            stored_row_to_plan_row(table, values)?
        };
        let keep = match filter {
            Some(expr) => eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)?,
            None => true,
        };
        if keep {
            out.push(row);
        }
    }
    Ok(Some(out))
}

pub(super) fn build_keyset_scan_info(
    plan: &PhysicalPlan,
) -> Option<(&PlanTable, Option<&Expr>, Option<&[usize]>)> {
    match plan {
        PhysicalPlan::SeqScan {
            table,
            filter,
            projected_columns,
            ..
        }
        | PhysicalPlan::ParallelSeqScan {
            table,
            filter,
            projected_columns,
            ..
        } => Some((
            table,
            filter.as_ref(),
            projected_columns.as_deref().map(|v| v.as_slice()),
        )),
        PhysicalPlan::Exchange { input, .. } => build_keyset_scan_info(input),
        _ => None,
    }
}

pub(super) fn build_keyset_index_choice<'a>(
    table: &'a PlanTable,
    build_keys: &[Expr],
    probe_keys: &[Expr],
) -> Option<(usize, &'a IndexSchema, &'a DataTypeKind, Option<&'a str>)> {
    build_keys
        .iter()
        .zip(probe_keys.iter())
        .enumerate()
        .filter_map(|(idx, (build_key, probe_key))| {
            let column = runtime_filter_simple_column_for_table(build_key, table)?;
            if !matches!(probe_key, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                return None;
            }
            let index = table.schema.iter_queryable_indexes().find(|index| {
                if index.has_prefix_columns() {
                    return false;
                }
                index
                    .columns
                    .first()
                    .is_some_and(|index_col| index_col.eq_ignore_ascii_case(column))
            })?;
            let column_idx = table.schema.column_index(column).ok()?;
            let col = &table.schema.columns[column_idx];
            Some((idx, index, &col.data_type, col.collation.as_deref()))
        })
        .min_by_key(|(_, index, _, _)| index.columns.len())
}

pub(super) fn build_bloom_from_key_subset(
    build_rows: &[PlanRow],
    build_keys: &[Expr],
    null_safe: &[bool],
    key_indices: &[usize],
) -> Result<Option<BloomFilter>, QueryError> {
    if key_indices.is_empty() || build_rows.is_empty() {
        return Ok(None);
    }
    let mut bf = BloomFilter::new(build_rows.len());
    let mut inserted = 0usize;
    for row in build_rows {
        let mut key = HashJoinKey::with_capacity(key_indices.len());
        let mut has_unsafe_null = false;
        for &idx in key_indices {
            let value = eval_plan_expr(&build_keys[idx], row)?;
            let is_null = matches!(value, Value::Null);
            let safe = null_safe.get(idx).copied().unwrap_or(false);
            if is_null && !safe {
                has_unsafe_null = true;
                break;
            }
            key.push(ValueKey::from(&value));
        }
        if has_unsafe_null {
            continue;
        }
        bf.insert(key.as_slice());
        inserted += 1;
    }
    if inserted == 0 {
        Ok(None)
    } else {
        Ok(Some(bf))
    }
}

pub(super) fn build_keyset_from_key_subset(
    build_rows: &[PlanRow],
    build_keys: &[Expr],
    null_safe: &[bool],
    key_indices: &[usize],
) -> Result<Option<Vec<Vec<Value>>>, QueryError> {
    if key_indices.is_empty() || build_rows.is_empty() {
        return Ok(None);
    }
    let mut seen: hashbrown::HashSet<Vec<ValueKey>, ahash::RandomState> =
        hashbrown::HashSet::with_capacity_and_hasher(build_rows.len(), ahash::RandomState::new());
    let mut out = Vec::new();
    for row in build_rows {
        let mut values = Vec::with_capacity(key_indices.len());
        let mut key = Vec::with_capacity(key_indices.len());
        let mut has_unsafe_null = false;
        for &idx in key_indices {
            let value = eval_plan_expr(&build_keys[idx], row)?;
            let is_null = matches!(value, Value::Null);
            let safe = null_safe.get(idx).copied().unwrap_or(false);
            if is_null && !safe {
                has_unsafe_null = true;
                break;
            }
            key.push(ValueKey::from(&value));
            values.push(value);
        }
        if has_unsafe_null {
            continue;
        }
        if seen.insert(key) {
            out.push(values);
        }
    }
    if out.is_empty() {
        Ok(None)
    } else {
        Ok(Some(out))
    }
}

pub(in crate::engine::executor::select) fn inject_runtime_filters_from_hash_build(
    db: &Arc<Database>,
    plan: &mut PhysicalPlan,
    build_rows: &[PlanRow],
    build_keys: &[Expr],
    probe_keys: &[Expr],
    null_safe: &[bool],
) -> Result<bool, QueryError> {
    let mut injected = false;
    inject_runtime_filters_from_hash_build_inner(
        db,
        plan,
        build_rows,
        build_keys,
        probe_keys,
        null_safe,
        &mut injected,
    )?;
    Ok(injected)
}

pub(super) fn inject_runtime_filters_from_hash_build_inner(
    db: &Arc<Database>,
    plan: &mut PhysicalPlan,
    build_rows: &[PlanRow],
    build_keys: &[Expr],
    probe_keys: &[Expr],
    null_safe: &[bool],
    injected: &mut bool,
) -> Result<(), QueryError> {
    match plan {
        PhysicalPlan::SeqScan { .. }
        | PhysicalPlan::ZoneMapScan { .. }
        | PhysicalPlan::ParallelSeqScan { .. }
        | PhysicalPlan::ParallelZoneMapScan { .. } => {
            // A runtime filter may refine rows inside this scan, but must not
            // replace the planned access operator.  The former scan ->
            // IndexMultiLookup mutation made ANALYZE execute a node ordinary
            // EXPLAIN could never show.  Keep this generic path Bloom-only;
            // dynamic index probing must be selected explicitly by planning.
            let (table, runtime_filters) = match plan {
                PhysicalPlan::SeqScan {
                    table,
                    runtime_filters,
                    ..
                }
                | PhysicalPlan::ZoneMapScan {
                    table,
                    runtime_filters,
                    ..
                }
                | PhysicalPlan::ParallelSeqScan {
                    table,
                    runtime_filters,
                    ..
                }
                | PhysicalPlan::ParallelZoneMapScan {
                    table,
                    runtime_filters,
                    ..
                } => (table, runtime_filters),
                _ => unreachable!(),
            };
            let key_indices = runtime_filter_key_indices_for_table(probe_keys, table);
            if key_indices.is_empty() {
                return Ok(());
            }
            if let Some(filter) =
                build_bloom_from_key_subset(build_rows, build_keys, null_safe, &key_indices)?
            {
                runtime_filters.push(RuntimeFilter::BloomKey {
                    keys: key_indices
                        .iter()
                        .map(|&idx| probe_keys[idx].clone())
                        .collect(),
                    null_safe: key_indices
                        .iter()
                        .map(|&idx| null_safe.get(idx).copied().unwrap_or(false))
                        .collect(),
                    filter: std::sync::Arc::new(filter),
                    rejected: crate::engine::executor::select::analyze_stats::is_recording()
                        .then(|| std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0))),
                });
                *injected = true;
            }
        }
        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::AdaptiveJoin { left, right, .. } => {
            inject_runtime_filters_from_hash_build_inner(
                db, left, build_rows, build_keys, probe_keys, null_safe, injected,
            )?;
            inject_runtime_filters_from_hash_build_inner(
                db, right, build_rows, build_keys, probe_keys, null_safe, injected,
            )?;
        }
        PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::WindowAgg { input, .. }
        | PhysicalPlan::Exchange { input, .. }
        | PhysicalPlan::ParallelHashAggregate { input, .. }
        | PhysicalPlan::ParallelSort { input, .. } => {
            inject_runtime_filters_from_hash_build_inner(
                db, input, build_rows, build_keys, probe_keys, null_safe, injected,
            )?;
        }
        PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_table,
            right_runtime_filters,
            ..
        } => {
            let disabled = matches!(
                crate::util::env::var("MYDB_HASH_JOIN_INLJ_RIGHT_RUNTIME_FILTER")
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .as_str(),
                "off" | "0" | "false" | "no" | "disable" | "disabled"
            );
            let key_indices = if disabled {
                Vec::new()
            } else {
                runtime_filter_key_indices_for_table(probe_keys, right_table)
            };
            if !key_indices.is_empty() {
                if let Some(values) =
                    build_keyset_from_key_subset(build_rows, build_keys, null_safe, &key_indices)?
                {
                    let mut filter = BloomFilter::new(values.len());
                    for tuple in &values {
                        let key: Vec<ValueKey> = tuple.iter().map(ValueKey::from).collect();
                        filter.insert(key.as_slice());
                    }
                    right_runtime_filters.push(RuntimeFilter::KeySet {
                        keys: key_indices
                            .iter()
                            .map(|&idx| probe_keys[idx].clone())
                            .collect(),
                        null_safe: key_indices
                            .iter()
                            .map(|&idx| null_safe.get(idx).copied().unwrap_or(false))
                            .collect(),
                        values: std::sync::Arc::new(values),
                        filter: std::sync::Arc::new(filter),
                    });
                    *injected = true;
                    return Ok(());
                }
            }
            inject_runtime_filters_from_hash_build_inner(
                db, left, build_rows, build_keys, probe_keys, null_safe, injected,
            )?;
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn runtime_filter_key_indices_for_table(keys: &[Expr], table: &PlanTable) -> Vec<usize> {
    keys.iter()
        .enumerate()
        .filter_map(|(idx, key)| {
            if runtime_filter_key_resolves_in_table(key, table) {
                Some(idx)
            } else {
                None
            }
        })
        .collect()
}

pub(super) fn runtime_filter_key_resolves_in_table(key: &Expr, table: &PlanTable) -> bool {
    runtime_filter_simple_column_for_table(key, table).is_some()
}

pub(super) fn probe_plan_accepts_inlj_right_runtime_filter(
    plan: &PhysicalPlan,
    probe_keys: &[Expr],
) -> bool {
    match plan {
        PhysicalPlan::IndexNestedLoopJoin {
            left, right_table, ..
        } => {
            probe_keys
                .iter()
                .any(|key| runtime_filter_key_resolves_in_table(key, right_table))
                || probe_plan_accepts_inlj_right_runtime_filter(left, probe_keys)
        }
        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::AdaptiveJoin { left, right, .. } => {
            probe_plan_accepts_inlj_right_runtime_filter(left, probe_keys)
                || probe_plan_accepts_inlj_right_runtime_filter(right, probe_keys)
        }
        PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::WindowAgg { input, .. }
        | PhysicalPlan::Exchange { input, .. }
        | PhysicalPlan::ParallelHashAggregate { input, .. }
        | PhysicalPlan::ParallelSort { input, .. } => {
            probe_plan_accepts_inlj_right_runtime_filter(input, probe_keys)
        }
        _ => false,
    }
}

pub(super) fn runtime_filter_simple_column_for_table<'a>(
    key: &'a Expr,
    table: &PlanTable,
) -> Option<&'a str> {
    let (qualifier, column) = match key {
        Expr::Identifier(column) => (None, column.as_str()),
        Expr::CompoundIdentifier(parts) => {
            let Some(column) = parts.last() else {
                return None;
            };
            let qualifier = if parts.len() >= 2 {
                Some(parts[parts.len() - 2].as_str())
            } else {
                None
            };
            (qualifier, column.as_str())
        }
        _ => return None,
    };
    if !table
        .schema
        .columns
        .iter()
        .any(|col| col.name.eq_ignore_ascii_case(column))
    {
        return None;
    }
    // An alias shadows the table name, so `t.id` never addresses `t AS b`.
    if qualifier.is_none_or(|qualifier| match table.alias.as_deref() {
        Some(alias) => alias.eq_ignore_ascii_case(qualifier),
        None => {
            table.schema.name.eq_ignore_ascii_case(qualifier)
                || table
                    .schema
                    .qualified_name()
                    .eq_ignore_ascii_case(qualifier)
        }
    }) {
        Some(column)
    } else {
        None
    }
}

/// Multi-threaded hash join: splits build/probe into P parts by `stable_hash(keys) % P`,
/// spawns P workers via `std::thread::scope` each running the in-memory hash join, then concatenates the results.
///
/// The same (build_key, probe_key) pair always routes to the same partition (hashing is consistent),
/// so the outer-preserved side "unmatched NULL-extension" is produced independently per partition without duplication.
/// Rows with non-null-safe NULL keys are routed to partition 0 (they never hit any bucket and only serve
/// as outer-side "unmatched" NULL-extended output, handled by the worker of that partition).
pub(super) fn execute_hash_join_parallel(
    db: &Arc<Database>,
    txn: TxnView,
    spec: &HashJoinSpec<'_>,
    limits: &HashJoinLimits,
    build_rows: Vec<PlanRow>,
    probe_rows: Vec<PlanRow>,
) -> Result<Vec<PlanRow>, QueryError> {
    let workers = limits.parallelism as usize;
    // Use more partitions than workers so a hash-skewed partition no
    // longer pins the worker that owns it: idle workers grab the next
    // available partition off a shared atomic cursor. PARTITION_FANOUT
    // = 4 keeps build-side routing overhead modest (4× more hashing)
    // while giving real headroom against typical join-key skew.
    const PARTITION_FANOUT: usize = 4;
    let partitions = workers.saturating_mul(PARTITION_FANOUT).max(workers);
    record_hash_join_parallel_partitions(partitions as u64);
    if !build_rows.is_empty() && limits.bloom_worthwhile(build_rows.len()) {
        record_hash_join_bloom_built();
    }

    let build_cap = build_rows
        .len()
        .saturating_div(partitions)
        .saturating_add(1);
    let probe_cap = probe_rows
        .len()
        .saturating_div(partitions)
        .saturating_add(1);
    // WS-2/3a: the scatter pid computation (key encoding + hash, once per row) is chunk-parallelized;
    // the distribution itself (pure moves) stays single-threaded. Expression keys (no compiled key plan)
    // may depend on session context during evaluation and stay serial.
    let mut build_parts: Vec<Vec<PlanRow>> = (0..partitions)
        .map(|_| Vec::with_capacity(build_cap))
        .collect();
    let build_pids = compute_partition_ids(
        &build_rows,
        spec.build_keys,
        spec.null_safe,
        spec.build_key_plan.as_ref(),
        partitions as u64,
        workers,
    )?;
    for (row, pi) in build_rows.into_iter().zip(build_pids) {
        build_parts[pi as usize].push(row);
    }
    let mut probe_parts: Vec<Vec<PlanRow>> = (0..partitions)
        .map(|_| Vec::with_capacity(probe_cap))
        .collect();
    let probe_pids = compute_partition_ids(
        &probe_rows,
        spec.probe_keys,
        spec.null_safe,
        spec.probe_key_plan.as_ref(),
        partitions as u64,
        workers,
    )?;
    for (row, pi) in probe_rows.into_iter().zip(probe_pids) {
        probe_parts[pi as usize].push(row);
    }
    // Wrap each (build_part, probe_part) in an Option so workers can
    // Option::take it without needing a Mutex around the whole vector.
    let parts: Vec<std::sync::Mutex<Option<(Vec<PlanRow>, Vec<PlanRow>)>>> = build_parts
        .into_iter()
        .zip(probe_parts.into_iter())
        .map(|pair| std::sync::Mutex::new(Some(pair)))
        .collect();

    let limits_owned = *limits;

    let execution_context =
        crate::engine::executor::select::execution_context::ExecutionContext::capture(txn);

    let mut results: Vec<Result<Vec<PlanRow>, QueryError>> =
        (0..workers).map(|_| Ok(Vec::new())).collect();
    let cursor = AtomicUsize::new(0);

    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let spec_ref: &HashJoinSpec<'_> = spec;
            let limits_ref = &limits_owned;
            let worker_context = execution_context.clone();

            let parts_ref = &parts;
            let cursor_ref = &cursor;

            handles.push(s.spawn(move || -> Result<Vec<PlanRow>, QueryError> {
                let run_join = || -> Result<Vec<PlanRow>, QueryError> {
                    (|| {
                        let mut out = Vec::new();
                        loop {
                            let idx = cursor_ref.fetch_add(1, Ordering::Relaxed);
                            if idx >= parts_ref.len() {
                                break;
                            }
                            let pair = {
                                let mut slot = parts_ref[idx].lock().unwrap();
                                slot.take()
                            };
                            let Some((b, pr)) = pair else { continue };
                            if b.is_empty() && pr.is_empty() {
                                continue;
                            }
                            let part_rows =
                                do_in_memory_hash_join(spec_ref, b, pr, limits_ref, false)?;
                            out.extend(part_rows);
                        }
                        Ok(out)
                    })()
                };
                worker_context.run(db, || run_join())
            }));
        }
        for (i, h) in handles.into_iter().enumerate() {
            results[i] = match h.join() {
                Ok(r) => r,
                Err(_) => Err(error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "hash join worker panicked",
                )),
            };
        }
    });

    let mut out = Vec::new();
    for r in results {
        out.extend(r?);
    }
    Ok(out)
}

/// Grace hash join: when the build side exceeds the memory threshold, hash-partition into disk temp files
/// and run an independent in-memory hash join per partition. Same-hash rows always land in the same partition, so the outer
/// join "unmatched NULL-extension" completes independently within each partition without duplication.
///
/// The temp directory lives under `<MYDB_DATA_DIR>/tmp/hashjoin-<pid>-<seq>/` and is cleaned up automatically by an RAII guard.
/// If recursion reaches `grace_max_depth` still over the threshold, run the bounded chunked hash join (or
/// raise an error in fallback=error mode).
pub(super) fn execute_grace_hash_join(
    db: &Arc<Database>,
    spec: &HashJoinSpec<'_>,
    limits: &HashJoinLimits,
    build_rows: Vec<PlanRow>,
    probe_rows: Vec<PlanRow>,
    depth: u32,
    input_reservation: Option<crate::engine::executor::mem_budget::ScopedCharge>,
) -> Result<Vec<PlanRow>, QueryError> {
    HASH_JOIN_GRACE_MAX_DEPTH.fetch_max(depth as u64, std::sync::atomic::Ordering::Relaxed);
    if depth >= limits.grace_max_depth {
        // A non-shrinking hot-key partition cannot be rescued by another
        // seeded repartition.  Keep the hash algorithm and bound its table by
        // processing contiguous build chunks; never fall through to the
        // generic O(|L|*|R|) NLJ.
        if limits.fallback_error {
            return Err(error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "hash join build side still exceeds limit after grace recursion",
            ));
        }
        return execute_chunked_hot_partition_hash_join(spec, limits, build_rows, probe_rows);
    }

    // Target roughly half a grant per average build partition, rounded to a
    // power of two so salted rehashing stays cheap and deterministic. The
    // configured partition count is the stable upper bound, never a runtime
    // input to join order/topology.
    let build_bytes = estimate_rows_bytes(&build_rows).max(1);
    let grant = limits.mem_limit_bytes.max(64 * 1024);
    let needed = build_bytes
        .saturating_add(grant - 1)
        .saturating_div(grant)
        .saturating_mul(2)
        .clamp(2, 256) as usize;
    let p = needed
        .next_power_of_two()
        .min(limits.grace_partitions as usize)
        .max(2);
    let p_u64 = p as u64;
    let data_dir = db.data_dir().to_path_buf();
    let tmp = SpillTmpDir::create(&data_dir)?;
    record_hash_join_grace_partitions(p as u64);

    // Logical fanout may be large, but only 16 writers per side are live.
    // The two LRU pools therefore cap the coordinator at 32 descriptors and
    // share one bounded spill-buffer budget.
    let mut build_writers = LazyPartitionWriterPool::new(tmp.path(), "build", p, tmp.charger(), 16);
    let mut probe_writers = LazyPartitionWriterPool::new(tmp.path(), "probe", p, tmp.charger(), 16);

    // Route and write. WS-P0-4: NULL keys are hash-scattered by row ordinal across the P partitions (semantics unchanged;
    // NULL keys never hit the bucket table, and the outer preserved side NULL-extends independently per partition); `MYDB_HASH_JOIN_PARALLEL_NULL_KEY_SCATTER=off`
    // falls back to the old "NULL → partition 0" behavior (differential testing).
    for (idx, row) in build_rows.iter().enumerate() {
        let pi = partition_for_grace_row(
            spec.build_keys,
            spec.null_safe,
            spec.build_key_plan.as_ref(),
            row,
            p_u64,
            idx as u64,
            depth,
        )? as usize;
        build_writers.append_row(pi, row)?;
    }
    for (idx, row) in probe_rows.iter().enumerate() {
        let pi = partition_for_grace_row(
            spec.probe_keys,
            spec.null_safe,
            spec.probe_key_plan.as_ref(),
            row,
            p_u64,
            idx as u64,
            depth,
        )? as usize;
        probe_writers.append_row(pi, row)?;
    }

    let total_bytes = build_writers
        .finish()?
        .saturating_add(probe_writers.finish()?);
    record_hash_join_spill_bytes(total_bytes);

    // Release the original memory (already written to disk).
    drop(build_rows);
    drop(probe_rows);
    drop(input_reservation);

    let mut out = Vec::new();
    let mut output_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    for i in 0..p {
        let mut input_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        let build_part = read_join_partition(
            &tmp.path().join(format!("build-{i}")),
            tmp.charger(),
            &mut input_charge,
        )?;
        let probe_part = read_join_partition(
            &tmp.path().join(format!("probe-{i}")),
            tmp.charger(),
            &mut input_charge,
        )?;
        if build_part.is_empty() && probe_part.is_empty() {
            continue;
        }
        // Per-partition recursion check: still over the threshold means another grace repartition.
        let still_too_big = build_part.len() as u64 > limits.max_build_rows
            || estimate_rows_bytes(&build_part) > limits.mem_limit_bytes;
        let part_rows = if still_too_big {
            HASH_JOIN_GRACE_REPARTITIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            execute_grace_hash_join(
                db,
                spec,
                limits,
                build_part,
                probe_part,
                depth + 1,
                Some(input_charge),
            )?
        } else {
            let _scope = MaterializationChargeScope::enter();
            let rows = with_strict_plan_row_output_accounting(true, || {
                do_in_memory_hash_join(spec, build_part, probe_part, limits, false)
            })?;
            drop(input_charge);
            rows
        };
        for row in &part_rows {
            output_charge.add(materialized_plan_row_charge(row))?;
        }
        out.extend(part_rows);
    }

    HASH_JOIN_SPILL_READ_BYTES.fetch_add(total_bytes, std::sync::atomic::Ordering::Relaxed);

    // tmp guard drop -> recursive temp-directory deletion
    Ok(out)
}

/// Both sides of a materializing row join must share a query reservation.
/// Check incrementally rather than discovering an oversized probe after EOF.
pub(super) fn read_join_partition(
    path: &std::path::Path,
    charger: crate::engine::executor::spill_quota::SpillCharger,
    charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
) -> Result<Vec<PlanRow>, QueryError> {
    let Some(mut reader) =
        crate::engine::executor::select::grace::PartitionReader::open(path, charger, 2)?
    else {
        return Ok(Vec::new());
    };
    let mut rows = Vec::new();
    while let Some(row) = reader.next_row()? {
        crate::engine::cancel::check_cancelled()?;
        charge.add(materialized_plan_row_charge(&row))?;
        rows.push(row);
    }
    Ok(rows)
}

#[cfg(test)]
#[test]
pub(super) fn grace_probe_partition_read_checks_budget_before_eof() {
    use crate::engine::executor::mem_budget::{self, MemBudget, ScopedCharge};
    std::fs::create_dir_all("/tmp/mydb/join-fix").unwrap();
    let root = tempfile::tempdir_in("/tmp/mydb/join-fix").unwrap();
    let dir = SpillTmpDir::create(root.path()).unwrap();
    let path = dir.path().join("probe");
    let row = PlanRow {
        fields: vec![PlanField {
            meta: intern_col_meta(ColMeta {
                table: "p".into(),
                alias: None,
                column: "v".into(),
                column_type: crate::types::ColumnType::VarString,
                source: Default::default(),
                invisible: false,
            }),
            value: Value::String("x".repeat(64)),
        }],
        ..Default::default()
    };
    let mut writer = crate::engine::executor::select::grace::PartitionWriter::create_charged(
        &path,
        dir.charger(),
    )
    .unwrap();
    for _ in 0..4096 {
        writer.append_row(&row).unwrap();
    }
    writer.finish().unwrap();
    let budget = Arc::new(MemBudget::new(64 * 1024));
    mem_budget::with_query_budget(budget.clone(), || {
        let mut charge = ScopedCharge::new(0);
        let error = read_join_partition(&path, dir.charger(), &mut charge).unwrap_err();
        assert_eq!(error.code(), 1037);
        assert!(charge.charged() < 4096 * materialized_plan_row_charge(&row));
    });
    assert_eq!(budget.used(), 0);
}

pub(super) fn push_hot_join_row(
    charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
    out: &mut Vec<PlanRow>,
    row: PlanRow,
) -> Result<(), QueryError> {
    if out.len() & 1023 == 0 {
        crate::engine::cancel::check_cancelled()?;
    }
    charge.add(materialized_plan_row_charge(&row))?;
    out.push(row);
    Ok(())
}

/// Bounded hot-partition resolver. Each pass builds a normal hash table for a
/// grant-sized slice and scans the partition probe rows. Ordinal match ledgers
/// make probe-preserved OUTER and SEMI/ANTI semantics span all chunks without
/// duplicate unmatched rows.
pub(super) fn execute_chunked_hot_partition_hash_join(
    spec: &HashJoinSpec<'_>,
    limits: &HashJoinLimits,
    build_rows: Vec<PlanRow>,
    probe_rows: Vec<PlanRow>,
) -> Result<Vec<PlanRow>, QueryError> {
    let target_bytes = limits
        .mem_limit_bytes
        .max(64 * 1024)
        .saturating_div(2)
        .max(1);
    let target_rows = limits.max_build_rows.max(1) as usize;
    let probe_preserved = spec.probe_is_outer_preserved();
    let build_preserved = spec.build_is_outer_preserved();
    let semi_or_anti = spec.is_semi_or_anti();
    let mut probe_matched = if probe_preserved || semi_or_anti {
        vec![false; probe_rows.len()]
    } else {
        Vec::new()
    };
    let residual_split = if matches!(
        spec.join_type,
        crate::parser::JoinType::Inner
            | crate::parser::JoinType::Semi
            | crate::parser::JoinType::Anti
    ) {
        split_hash_join_residual_by_side(spec)
    } else {
        SplitHashResidual::joined_only(spec.residual)
    };
    let join_lk = new_join_lookup();
    let mut out = Vec::new();
    let mut output_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    let mut start = 0usize;
    while start < build_rows.len() {
        crate::engine::cancel::check_cancelled()?;
        let mut end = start;
        let mut bytes = 0u64;
        while end < build_rows.len() && end - start < target_rows {
            let row_bytes = estimate_row_bytes(&build_rows[end]).saturating_add(96);
            if end > start && bytes.saturating_add(row_bytes) > target_bytes {
                break;
            }
            bytes = bytes.saturating_add(row_bytes);
            end += 1;
        }
        if end == start {
            end += 1;
        }
        HASH_JOIN_SKEW_CHUNKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let chunk = &build_rows[start..end];
        let residual_pre = compile_joined_residual_pre(spec, &residual_split, chunk, &probe_rows);
        let (buckets, _) = build_join_buckets(
            spec.build_keys,
            spec.null_safe,
            spec.build_key_plan.as_ref(),
            chunk,
            None,
            false,
            false,
            residual_split.build_only.as_ref(),
        )?;
        let mut build_matched = if build_preserved {
            vec![false; chunk.len()]
        } else {
            Vec::new()
        };
        for (probe_idx, probe_row) in probe_rows.iter().enumerate() {
            crate::engine::cancel::check_cancelled()?;
            if residual_split
                .probe_only
                .as_ref()
                .map(|expr| eval_plan_bool(expr, probe_row))
                .transpose()?
                .is_some_and(|keep| !keep)
            {
                continue;
            }
            let Some(key) = compute_join_key_with_plan(
                spec.probe_keys,
                spec.null_safe,
                spec.probe_key_plan.as_ref(),
                probe_row,
            )?
            else {
                continue;
            };
            let Some(matches) = buckets.gen_matches(&key) else {
                continue;
            };
            for (match_idx, build_idx) in matches.enumerate() {
                if match_idx & 1023 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                let build_row = &chunk[build_idx];
                let passes = if let Some(pre) = residual_pre.as_ref() {
                    pre.eval_build_probe(spec.build_is_left, build_row, probe_row)?
                } else if let Some(residual) = residual_split.joined.as_ref() {
                    let joined = if spec.build_is_left {
                        join_rows(build_row, probe_row, &join_lk)
                    } else {
                        join_rows(probe_row, build_row, &join_lk)
                    };
                    eval_plan_bool(residual, &joined)?
                } else {
                    true
                };
                if !passes {
                    continue;
                }
                if !probe_matched.is_empty() {
                    probe_matched[probe_idx] = true;
                }
                if build_preserved {
                    build_matched[build_idx] = true;
                }
                if !semi_or_anti {
                    push_hot_join_row(
                        &mut output_charge,
                        &mut out,
                        spec.emit_joined(build_row, probe_row, &join_lk),
                    )?;
                }
            }
        }
        if build_preserved {
            for (idx, matched) in build_matched.iter().enumerate() {
                if !*matched {
                    push_hot_join_row(
                        &mut output_charge,
                        &mut out,
                        spec.emit_build_unmatched(&chunk[idx]),
                    )?;
                }
            }
        }
        start = end;
    }

    if semi_or_anti {
        for (idx, probe_row) in probe_rows.iter().enumerate() {
            let emit = match spec.join_type {
                crate::parser::JoinType::Semi => probe_matched[idx],
                crate::parser::JoinType::Anti => !probe_matched[idx],
                _ => false,
            };
            if emit {
                push_hot_join_row(&mut output_charge, &mut out, spec.emit_probe_row(probe_row))?;
            }
        }
    } else if probe_preserved {
        for (idx, probe_row) in probe_rows.iter().enumerate() {
            if !probe_matched[idx] {
                push_hot_join_row(
                    &mut output_charge,
                    &mut out,
                    spec.emit_probe_unmatched(probe_row),
                )?;
            }
        }
    }
    Ok(out)
}

/// "Already materialized" variant of the NLJ fallback: reuses the spec instead of reassembling arguments.
pub(super) fn execute_hash_join_nlj_fallback_from_spec(
    spec: &HashJoinSpec<'_>,
    build_rows: Vec<PlanRow>,
    probe_rows: Vec<PlanRow>,
) -> Result<Vec<PlanRow>, QueryError> {
    let build_side = if spec.build_is_left {
        BuildSide::Left
    } else {
        BuildSide::Right
    };
    // Recover left/right keys from the spec build/probe keys.
    let (left_keys, right_keys) = if spec.build_is_left {
        (spec.build_keys, spec.probe_keys)
    } else {
        (spec.probe_keys, spec.build_keys)
    };
    let mut rows = execute_hash_join_nlj_fallback(
        spec.left_plan,
        spec.right_plan,
        spec.join_type,
        left_keys,
        right_keys,
        spec.null_safe,
        build_side,
        spec.residual,
        build_rows,
        probe_rows,
    )?;
    // The NLJ fallback produces full-width rows; when the spec carries an S2 projection, close it out here so the
    // from_rows contract "carrying a projection means producing projected rows" holds for every sub-path.
    if let Some(projection) = spec.output_projection {
        project_joined_rows_in_place(&mut rows, projection);
    }
    Ok(rows)
}

/// Computes a row partition index: non-null-safe NULL keys land in partition 0, otherwise stable hash modulo.
pub(super) fn partition_for_grace_row(
    keys: &[Expr],
    null_safe: &[bool],
    key_plan: Option<&HashJoinKeyPlan>,
    row: &PlanRow,
    p: u64,
    row_idx: u64,
    depth: u32,
) -> Result<u64, QueryError> {
    let key = compute_join_key_with_plan(keys, null_safe, key_plan, row)?;
    match key {
        // Recursive Grace repartitioning must not reuse the first-level
        // partition function. Reusing `stable_hash_keys(key) % p` routes
        // every row in a parent partition straight back into one child,
        // so recursion never reduces the working set and eventually falls
        // through to the quadratic NLJ fallback. A depth-specific stable
        // seed keeps equal build/probe keys co-located while giving every
        // recursion level an independent distribution.
        Some(k) => Ok(crate::engine::executor::select::grace::partition_for_hash(
            stable_hash_keys(k.as_slice()),
            depth,
            p,
        )),
        None => {
            if hash_join_parallel_null_key_scatter_enabled() {
                record_hash_join_parallel_null_scattered();
                Ok(grace_null_scatter_partition(row_idx, p, depth))
            } else {
                Ok(0)
            }
        }
    }
}

/// WS-2/3a: batch partition-id computation. With enough rows and a compiled key plan (pure field access,
/// no expression evaluation → no session-context dependency) the work runs chunk-parallel; otherwise serial row by row.
/// `MYDB_HASH_JOIN_PARALLEL_SCATTER=off` forces serial (for differential testing).
#[cfg(not(test))]
pub(super) const PARALLEL_SCATTER_MIN_ROWS: usize = 32_768;
#[cfg(test)]
pub(super) const PARALLEL_SCATTER_MIN_ROWS: usize = 2_048;

pub(super) fn compute_partition_ids(
    rows: &[PlanRow],
    keys: &[Expr],
    null_safe: &[bool],
    key_plan: Option<&HashJoinKeyPlan>,
    p: u64,
    workers: usize,
) -> Result<Vec<u32>, QueryError> {
    let parallel_ok = workers > 1
        && rows.len() >= PARALLEL_SCATTER_MIN_ROWS
        && key_plan.is_some()
        && !hash_join_parallel_scatter_disabled();
    if !parallel_ok {
        let mut pids = Vec::with_capacity(rows.len());
        for (idx, row) in rows.iter().enumerate() {
            pids.push(
                partition_for_row_with_plan(keys, null_safe, key_plan, row, p, idx as u64)? as u32,
            );
        }
        return Ok(pids);
    }
    let plan = key_plan.expect("checked above");
    record_hash_join_parallel_scatter();
    let chunk = rows.len().div_ceil(workers).max(1);
    let chunks: Vec<&[PlanRow]> = rows.chunks(chunk).collect();
    let cancel_token = crate::engine::cancel::current_token();
    let mut results: Vec<Result<Vec<u32>, QueryError>> =
        (0..chunks.len()).map(|_| Ok(Vec::new())).collect();
    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(chunks.len());
        for (chunk_idx, rows_chunk) in chunks.iter().enumerate() {
            let rows_chunk: &[PlanRow] = rows_chunk;
            let cancel_token = cancel_token.clone();
            let chunk_off = (chunk_idx * chunk) as u64;
            handles.push(s.spawn(move || -> Result<Vec<u32>, QueryError> {
                let _cancel_guard = cancel_token.map(crate::engine::cancel::enter);
                let mut pids = Vec::with_capacity(rows_chunk.len());
                for (i, row) in rows_chunk.iter().enumerate() {
                    if i & 0xFFFF == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let row_idx = chunk_off + i as u64;
                    pids.push(partition_for_row_fast(plan, null_safe, row, p, row_idx)? as u32);
                }
                Ok(pids)
            }));
        }
        for (i, h) in handles.into_iter().enumerate() {
            results[i] = match h.join() {
                Ok(r) => r,
                Err(_) => Err(error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "hash join scatter worker panicked",
                )),
            };
        }
    });
    let mut pids = Vec::with_capacity(rows.len());
    for result in results {
        pids.extend(result?);
    }
    Ok(pids)
}

/// `MYDB_HASH_JOIN_PARALLEL_SCATTER` (default on).
pub(super) fn hash_join_parallel_scatter_disabled() -> bool {
    matches!(
        crate::util::env::var("MYDB_HASH_JOIN_PARALLEL_SCATTER")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no"
    )
}

/// WS-P0-4 switch: default on. `MYDB_HASH_JOIN_PARALLEL_NULL_KEY_SCATTER=off|0|false|no`
/// falls back to the old "NULL-key rows land in partition 0" behavior (differential testing / emergency fallback).
///
/// Semantics unchanged: a non-null-safe NULL key still never hits the bucket table. The switch only decides the
/// **distribution** — on: hash-scattered by row ordinal across partitions; off: all to partition 0 (original behavior).
/// NULL-key rows of the outer preserved side are NULL-extended independently inside `do_in_memory_hash_join`,
/// so any partition handles them correctly.
#[inline]
pub(super) fn hash_join_parallel_null_key_scatter_enabled() -> bool {
    !matches!(
        crate::util::env::var("MYDB_HASH_JOIN_PARALLEL_NULL_KEY_SCATTER")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no"
    )
}

/// Scatters "non-null-safe NULL-key rows" by row_idx to a partition number in `[0, p)`.
/// Same seed family as the `partition_for_row_fast` `StableHasher` but a different constant,
/// avoiding correlation with the valid key hash distribution.
#[inline]
pub(super) fn null_scatter_partition(row_idx: u64, p: u64) -> u64 {
    null_scatter_partition_seeded(row_idx, p, 0xfeed_face_cafe_d00d)
}

#[inline]
pub(super) fn null_scatter_partition_seeded(row_idx: u64, p: u64, seed: u64) -> u64 {
    if p <= 1 {
        return 0;
    }
    use std::hash::Hash;
    use std::hash::Hasher;
    let mut h = StableHasher::new(seed);
    row_idx.hash(&mut h);
    h.finish() % p
}

#[inline]
pub(super) fn grace_null_scatter_partition(row_idx: u64, p: u64, depth: u32) -> u64 {
    if p <= 1 {
        return 0;
    }
    use std::hash::Hash;
    use std::hash::Hasher;
    let mut h = StableHasher::new(0xfeed_face_cafe_d00d);
    row_idx.hash(&mut h);
    crate::engine::executor::select::grace::partition_for_hash(
        h.finish() ^ 0x2030_4050_6070_8090,
        depth,
        p,
    )
}

pub(super) fn partition_for_row_with_plan(
    keys: &[Expr],
    null_safe: &[bool],
    key_plan: Option<&HashJoinKeyPlan>,
    row: &PlanRow,
    p: u64,
    row_idx: u64,
) -> Result<u64, QueryError> {
    if let Some(plan) = key_plan {
        return partition_for_row_fast(plan, null_safe, row, p, row_idx);
    }
    let key = compute_join_key_with_plan(keys, null_safe, key_plan, row)?;
    match key {
        Some(k) => Ok(stable_hash_keys(k.as_slice()) % p),
        None => {
            if hash_join_parallel_null_key_scatter_enabled() {
                record_hash_join_parallel_null_scattered();
                Ok(null_scatter_partition(row_idx, p))
            } else {
                Ok(0)
            }
        }
    }
}

pub(super) fn partition_for_row_fast(
    key_plan: &HashJoinKeyPlan,
    null_safe: &[bool],
    row: &PlanRow,
    p: u64,
    row_idx: u64,
) -> Result<u64, QueryError> {
    use std::hash::Hash;
    use std::hash::Hasher;

    let mut h = StableHasher::new(0xdead_beef_cafe_babe);
    key_plan.field_indices.len().hash(&mut h);
    for (i, &field_idx) in key_plan.field_indices.iter().enumerate() {
        let value = row
            .fields
            .get(field_idx)
            .map(|field| field.value_ref())
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "hash join key field index out of bounds",
                )
            })?;
        let is_null = matches!(value, Value::Null);
        let null_safe_bit = null_safe.get(i).copied().unwrap_or(false);
        if is_null && !null_safe_bit {
            // WS-P0-4: scatter "non-null-safe NULL-key rows" by row_idx across the P partitions,
            // avoiding single-worker skew on large-table NULL-column outer joins. Semantics unchanged (NULL keys
            // still never hit the bucket table within a partition).
            if hash_join_parallel_null_key_scatter_enabled() {
                record_hash_join_parallel_null_scattered();
                return Ok(null_scatter_partition(row_idx, p));
            }
            return Ok(0);
        }
        ValueKey::from_with_collation(value, key_plan.collation_ids.get(i).copied().flatten())
            .hash(&mut h);
    }
    Ok(h.finish() % p)
}

/// Cross-thread stable 64-bit hash: an FNV-1a variant with a fixed seed.
/// Avoids the per-process random seed of std DefaultHasher so build and probe route equal keys to the same partition.
pub(in crate::engine::executor::select) fn stable_hash_keys(key: &[ValueKey]) -> u64 {
    use std::hash::Hash;
    use std::hash::Hasher;
    let mut h = StableHasher::new(0xdead_beef_cafe_babe);
    key.hash(&mut h);
    h.finish()
}

pub(super) struct StableHasher {
    pub(super) state: u64,
}
impl StableHasher {
    pub(super) fn new(seed: u64) -> Self {
        Self { state: seed }
    }
}
impl std::hash::Hasher for StableHasher {
    fn finish(&self) -> u64 {
        self.state
    }
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.state ^= *b as u64;
            self.state = self.state.wrapping_mul(0x100_0000_01b3);
        }
    }
}

/// Evaluates all join keys of a row and returns the composite ValueKey; if any key is NULL with the matching null_safe=false bit,
/// returns None (per SQL standard `=` it takes no part in matching). With null_safe=true, NULL enters the bucket and matches NULL.
pub(super) fn compute_join_key(
    keys: &[Expr],
    null_safe: &[bool],
    row: &PlanRow,
) -> Result<Option<HashJoinKey>, QueryError> {
    let mut out = HashJoinKey::with_capacity(keys.len());
    for (i, key) in keys.iter().enumerate() {
        let value = eval_plan_expr(key, row)?;
        let is_null = matches!(value, Value::Null);
        let null_safe_bit = null_safe.get(i).copied().unwrap_or(false);
        if is_null && !null_safe_bit {
            return Ok(None);
        }
        out.push(ValueKey::from(&value));
    }
    Ok(Some(out))
}

pub(in crate::engine::executor::select) fn compute_join_key_with_plan(
    keys: &[Expr],
    null_safe: &[bool],
    key_plan: Option<&HashJoinKeyPlan>,
    row: &PlanRow,
) -> Result<Option<HashJoinKey>, QueryError> {
    match key_plan {
        Some(plan) => compute_join_key_fast(plan, null_safe, row),
        None => compute_join_key(keys, null_safe, row),
    }
}

pub(super) fn compute_join_key_fast(
    key_plan: &HashJoinKeyPlan,
    null_safe: &[bool],
    row: &PlanRow,
) -> Result<Option<HashJoinKey>, QueryError> {
    let mut out = HashJoinKey::with_capacity(key_plan.field_indices.len());
    for (i, &field_idx) in key_plan.field_indices.iter().enumerate() {
        let value = row
            .fields
            .get(field_idx)
            .map(|field| field.value_ref())
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "hash join key field index out of bounds",
                )
            })?;
        let is_null = matches!(value, Value::Null);
        let null_safe_bit = null_safe.get(i).copied().unwrap_or(false);
        if is_null && !null_safe_bit {
            return Ok(None);
        }
        out.push(ValueKey::from_with_collation(
            value,
            key_plan.collation_ids.get(i).copied().flatten(),
        ));
    }
    Ok(Some(out))
}
