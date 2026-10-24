//! hash join execution and preparation.

use std::{borrow::Cow, sync::Arc};

use super::aggregation::scheduler_hash_join_workers;
use super::compute::ComputeColumnsBatchStream;
use super::grace_join::{
    drain_vec_join_build_to_cap, run_vec_grace_hash_join_stream, vec_grace_partition_count,
    GraceJoinOutput,
};
use super::topn::expr_column_index_in_template;
use super::{
    batch_field_template, batches_to_plan_rows, computed_batch_field, concat_templates,
    cte_join_enabled, cte_vec_build_side, is_simple_column_ref, materialize_join_batches,
    plan_is_cte_only, template_to_batch_schema, try_native_vec_input_stream,
    vec_hash_join_streaming_enabled, ProjectColumnsBatchStream, ScalarFilterBatchStream,
    VecBatchInput, VEC_HASH_JOIN_HITS, VEC_HASH_JOIN_STREAM_HITS,
};
use crate::engine::executor::select::{
    analyze_stats,
    plan::{PlanField, PlanRow},
};
use crate::engine::executor::vec::{
    batch::{Batch, ColumnVector, Selection, DEFAULT_BATCH_ROWS},
    expr_batch, hash_agg as vec_hash_agg, hash_join as vec_hash_join,
    schema::BatchSchema,
    stream::{BatchOperator, BatchProperties, MaterializedBatchOperator},
};
use crate::parser::Expr;
use crate::planner::{BuildSide, PhysicalPlan};
use crate::storage::{Database, TxnView};
use crate::types::QueryError;

mod aggregate;
#[cfg(test)]
mod tests;

/// Resolve the canonical comparison collation for each join-key pair.
fn join_key_collations(
    build_template: &[PlanField],
    probe_template: &[PlanField],
    build_keys: &[usize],
    probe_keys: &[usize],
) -> Result<Vec<Option<u16>>, QueryError> {
    build_keys
        .iter()
        .zip(probe_keys)
        .map(|(&build, &probe)| {
            crate::engine::executor::select::plan::common_hash_join_collation(
                build_template
                    .get(build)
                    .and_then(|field| field.source.collation_id),
                probe_template
                    .get(probe)
                    .and_then(|field| field.source.collation_id),
            )
        })
        .collect()
}

/// A previously read child remains available if capability preflight rejects
/// the vector join. Transfer ownership only when execution pulls its first
/// batch, then move values into bounded column buffers without cloning rows.
struct MaterializedJoinInput<'a> {
    rows: &'a mut Option<Vec<PlanRow>>,
    remaining: Option<std::vec::IntoIter<PlanRow>>,
    schema: Arc<BatchSchema>,
}

impl BatchOperator for MaterializedJoinInput<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        let remaining = self.remaining.get_or_insert_with(|| {
            self.rows
                .take()
                .expect("materialized join input")
                .into_iter()
        });
        let count = remaining.len().min(DEFAULT_BATCH_ROWS);
        if count == 0 {
            return Ok(None);
        }
        let mut columns: Vec<Vec<crate::types::Value>> = (0..self.schema.len())
            .map(|_| Vec::with_capacity(count))
            .collect();
        for row in remaining.by_ref().take(count) {
            for (column, field) in columns.iter_mut().zip(row.fields) {
                column.push(field.value);
            }
        }
        Ok(Some(Batch::with_row_count(
            self.schema.clone(),
            columns.into_iter().map(ColumnVector::from_values).collect(),
            count,
        )))
    }
}

fn hash_join_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
    rows: Option<&'a mut Option<Vec<PlanRow>>>,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let Some(rows) = rows.filter(|rows| rows.is_some()) else {
        return try_native_vec_input_stream(db, txn, plan);
    };
    let template = match rows.as_ref().and_then(|rows| rows.first()) {
        Some(row) => row
            .fields
            .iter()
            .map(|field| PlanField {
                meta: field.meta.clone(),
                value: crate::types::Value::Null,
            })
            .collect(),
        None => super::plan_output_template(plan),
    };
    let schema = template_to_batch_schema(&template);
    Ok(Some(VecBatchInput {
        stream: Box::new(MaterializedJoinInput {
            rows,
            remaining: None,
            schema,
        }),
        template,
    }))
}

pub(in crate::engine::executor::select) fn try_vec_hash_join(
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
    materialized_left: &mut Option<Vec<PlanRow>>,
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    use crate::parser::JoinType;

    // Differential oracle for row HashJoin development and regression tests.
    // The default remains capability-first vector execution; only an explicit
    // off value keeps the finalized HashJoin on the row implementation.
    if matches!(
        crate::util::env::var("MYDB_VEC_HASH_JOIN")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no"
    ) {
        return Ok(None);
    }

    // Capability, rather than an internal query-shape switch, decides whether
    // the physical HashJoin is executable.
    let cte_only = cte_join_enabled() && plan_is_cte_only(left) && plan_is_cte_only(right);
    // CTE-only outer joins: re-derive the build side onto the inner (probe-
    // preserving) side. The CBO's choice is a cost decision made from the
    // under-counted estimate; the vec preserved-probe kernel requires it, and
    // it's always correct for an outer join. Non-CTE joins keep the CBO side
    // and bail below if it's vec-incompatible.
    let build_side = if cte_only {
        cte_vec_build_side(join_type, build_side)
    } else {
        build_side
    };
    // Routing is determined by the finalized node's capabilities only. In
    // particular, materialized CTE row counts and current row width never
    // select a different executor after planning.
    if !matches!(
        join_type,
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Semi | JoinType::Anti
    ) || left_keys.len() != right_keys.len()
        || left_keys.is_empty()
    {
        return Ok(None);
    }
    // The preserved side must be the probe: LEFT requires build=right, RIGHT requires build=left.
    // Non-CTE joins whose build side the CBO flipped fall back to the row engine (CTE-only cases were corrected above).
    // SEMI/ANTI output probe (=left) rows with the planner always building on the right; defensive fallback.
    if (join_type == JoinType::Left && build_side != BuildSide::Right)
        || (join_type == JoinType::Right && build_side != BuildSide::Left)
        || (matches!(join_type, JoinType::Semi | JoinType::Anti) && build_side != BuildSide::Right)
    {
        return Ok(None);
    }
    let (build_plan, build_keys_arr, probe_plan, probe_keys_arr, build_is_left) = match build_side {
        BuildSide::Right => (right, right_keys, left, left_keys, false),
        BuildSide::Left => (left, left_keys, right, right_keys, true),
    };

    let (build_rows, probe_rows) = if build_is_left {
        (Some(materialized_left), None)
    } else {
        (None, Some(materialized_left))
    };

    // Build the build/probe inputs as BatchStreams. For CteScan / SeqScan
    // leaves this serves columns directly (skipping the PlanRow round-trip);
    // every other shape falls back to the row-stream→batch adapter, so the
    // join is shape-agnostic. An empty template means the side produced no
    // rows (or the row fallback couldn't probe a schema): INNER/SEMI
    // short-circuit to empty; Anti/Left/Right can't synthesize the missing
    // build-side schema cheaply, so they defer to the row join.
    let Some(VecBatchInput {
        stream: mut build_stream,
        template: build_template,
    }) = hash_join_input(db, txn, build_plan, build_rows)?
    else {
        return Ok(None);
    };
    if build_template.is_empty() {
        if matches!(join_type, JoinType::Inner | JoinType::Semi) {
            return Ok(Some(Vec::new()));
        }
        return Ok(None);
    }
    let Some(VecBatchInput {
        stream: mut probe_stream,
        template: probe_template,
    }) = hash_join_input(db, txn, probe_plan, probe_rows)?
    else {
        return Ok(None);
    };
    if probe_template.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let mut build_key_idx = Vec::with_capacity(build_keys_arr.len());
    for key in build_keys_arr {
        match expr_column_index_in_template(key, &build_template) {
            Some(i) => build_key_idx.push(i),
            None => return Ok(None),
        }
    }
    let mut probe_key_idx = Vec::with_capacity(probe_keys_arr.len());
    for key in probe_keys_arr {
        match expr_column_index_in_template(key, &probe_template) {
            Some(i) => probe_key_idx.push(i),
            None => return Ok(None),
        }
    }
    let key_collations = join_key_collations(
        &build_template,
        &probe_template,
        &build_key_idx,
        &probe_key_idx,
    )?;
    // The null_safe array is aligned per (left_key, right_key); from the
    // build/probe view the order is identical (one side of each key pair), so it is passed straight through.

    // Final output template (SQL column order); semi/anti output only the
    // probe(=left) columns, and the residual candidate template is fixed [probe, build] = [left, right].
    let semi_or_anti = matches!(join_type, JoinType::Semi | JoinType::Anti);
    let joined_template = match join_type {
        JoinType::Left => concat_templates(&probe_template, &build_template),
        JoinType::Right => concat_templates(&build_template, &probe_template),
        _ => {
            if build_is_left {
                concat_templates(&build_template, &probe_template)
            } else {
                concat_templates(&probe_template, &build_template)
            }
        }
    };
    let out_template: &[PlanField] = if semi_or_anti {
        &probe_template
    } else {
        &joined_template
    };

    // B2: residual compilation. Semi/anti compiles against the candidate order
    // ([probe, build] — joined_template is exactly that order since semi/anti
    // always build on the right); others compile against the final output order; compile failure falls back to the row engine.
    let residual_program = match residual {
        None => None,
        Some(expr) => {
            let schema = template_to_batch_schema(&joined_template);
            match expr_batch::BatchExprProgram::compile(expr, &schema)? {
                Some(program) => Some((program, schema)),
                None => return Ok(None),
            }
        }
    };

    // `build_stream` / `probe_stream` are already `Box<dyn BatchOperator>`
    // (native columnar leaf or row→batch adapter); the join driver below
    // consumes them via `.as_mut()`.

    // Register before retaining the first batch. Capacity-sized resident bytes
    // are charged incrementally, so a vector join may borrow unused executor
    // work-area budget without briefly escaping the global governor.
    let hj_limits = crate::engine::executor::select::plan::hash_join_limits();
    let grace_cap = vec_hash_join::BuildCap {
        max_rows: hj_limits.max_build_rows,
        max_bytes: hj_limits.mem_limit_bytes,
    };
    let work_area = register_hash_join_work_area();
    let admission_cap = vec_hash_join_admission_cap(
        hj_limits.max_build_rows,
        hj_limits.mem_limit_bytes,
        hj_limits.mem_limit_explicit,
        work_area.is_some(),
    );
    let build_schema = build_stream.schema().clone();
    let (drained_build, build_overflow, build_memory_charge) =
        drain_vec_join_build_to_cap(build_stream.as_mut(), admission_cap, work_area.as_ref())?;
    let grace_partitions = vec_grace_partition_count(
        hj_limits.grace_partitions as usize,
        &drained_build,
        crate::planner::estimated_rows(build_plan),
        grace_cap,
        scheduler_hash_join_workers(),
    );
    if build_overflow {
        drop(work_area);
        let join_spec = (!semi_or_anti).then(|| vec_hash_join::VecJoinSpec {
            build_keys: &build_key_idx,
            probe_keys: &probe_key_idx,
            key_collations: &key_collations,
            null_safe,
            build_cols_first: match join_type {
                JoinType::Right => true,
                JoinType::Left => false,
                _ => build_is_left,
            },
            out_schema: template_to_batch_schema(&joined_template),
            residual: residual_program.as_ref().map(|(program, _)| &**program),
        });
        let mut output = run_vec_grace_hash_join_stream(
            db,
            txn,
            drained_build,
            Some(build_memory_charge),
            build_stream.as_mut(),
            probe_stream.as_mut(),
            &build_key_idx,
            &probe_key_idx,
            null_safe,
            &key_collations,
            join_type,
            join_spec.as_ref(),
            template_to_batch_schema(&probe_template),
            residual_program
                .as_ref()
                .map(|(program, schema)| (&**program, schema.clone())),
            grace_partitions,
            grace_cap,
            hj_limits.grace_max_depth,
            0,
            false,
        )?;
        VEC_HASH_JOIN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return Ok(Some(materialize_join_batches(
            std::iter::from_fn(|| output.next_batch().transpose()),
            out_template,
        )?));
    }
    // Batch clones share their column buffers.  Keep a replayable view until
    // the exact collector has accepted the build; if its key-aware accounting
    // rejects the preflighted input, Grace spill can continue without rescanning
    // the physical child.
    let grace_build = drained_build.clone();
    let mut materialized_build =
        MaterializedBatchOperator::new(build_schema.clone(), drained_build);
    let collector_cap = vec_hash_join_collector_cap(hj_limits, work_area.as_ref());

    let dispatch = vec_hash_join::with_vec_build_cap(
        collector_cap,
        || -> Result<Option<Vec<Batch>>, QueryError> {
            let out = match join_type {
                JoinType::Semi | JoinType::Anti => {
                    let out_schema = template_to_batch_schema(&probe_template);
                    vec_hash_join::run_semi_anti_hash_join(
                        &mut materialized_build,
                        probe_stream.as_mut(),
                        &build_key_idx,
                        &probe_key_idx,
                        &key_collations,
                        null_safe,
                        join_type == JoinType::Anti,
                        out_schema,
                        residual_program.as_ref().map(|(p, s)| (&**p, s.clone())),
                    )?
                }
                JoinType::Left | JoinType::Right => {
                    let spec = vec_hash_join::VecJoinSpec {
                        build_keys: &build_key_idx,
                        probe_keys: &probe_key_idx,
                        key_collations: &key_collations,
                        null_safe,
                        // LEFT: probe(=left) columns first; RIGHT: build(=left) columns first.
                        build_cols_first: join_type == JoinType::Right,
                        out_schema: template_to_batch_schema(&joined_template),
                        residual: residual_program.as_ref().map(|(p, _)| &**p),
                    };
                    vec_hash_join::run_preserved_probe_hash_join(
                        &mut materialized_build,
                        probe_stream.as_mut(),
                        &spec,
                    )?
                }
                JoinType::Inner => {
                    let spec = vec_hash_join::VecJoinSpec {
                        build_keys: &build_key_idx,
                        probe_keys: &probe_key_idx,
                        key_collations: &key_collations,
                        null_safe,
                        build_cols_first: build_is_left,
                        out_schema: template_to_batch_schema(&joined_template),
                        residual: residual_program.as_ref().map(|(p, _)| &**p),
                    };
                    // Both children being exact unique probes proves this
                    // join has only a tiny, structurally bounded input. Keep
                    // the faster vector kernel, but do not create scoped OS
                    // partition-build workers for a one-row hash table. The
                    // decision depends only on the finalized physical tree,
                    // so identical plans retain identical topology.
                    let workers =
                        if crate::engine::executor::select::plan::bounded_result_plan(left)
                            && crate::engine::executor::select::plan::bounded_result_plan(right)
                        {
                            1
                        } else {
                            scheduler_hash_join_workers()
                        };
                    if workers > 1 {
                        vec_hash_join::run_inner_hash_join_partitioned(
                            materialized_build.into_batches(),
                            probe_stream.as_mut(),
                            &spec,
                            workers,
                        )?
                    } else {
                        vec_hash_join::run_inner_hash_join(
                            &mut materialized_build,
                            probe_stream.as_mut(),
                            &spec,
                        )?
                    }
                }
                _ => return Ok(None),
            };
            Ok(Some(out))
        },
    );

    let out_batches = match dispatch {
        Ok(Some(b)) => b,
        Ok(None) => {
            return Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "precompiled vector hash join reached an invalid execution shape",
            ))
        }
        Err(e) if vec_hash_join::is_vec_build_overflow(&e) => {
            drop(work_area);
            let join_spec = (!semi_or_anti).then(|| vec_hash_join::VecJoinSpec {
                build_keys: &build_key_idx,
                probe_keys: &probe_key_idx,
                key_collations: &key_collations,
                null_safe,
                build_cols_first: match join_type {
                    JoinType::Right => true,
                    JoinType::Left => false,
                    _ => build_is_left,
                },
                out_schema: template_to_batch_schema(&joined_template),
                residual: residual_program.as_ref().map(|(program, _)| &**program),
            });
            let mut no_remaining = MaterializedBatchOperator::new(build_schema, Vec::new());
            let mut output = run_vec_grace_hash_join_stream(
                db,
                txn,
                grace_build,
                Some(build_memory_charge),
                &mut no_remaining,
                probe_stream.as_mut(),
                &build_key_idx,
                &probe_key_idx,
                null_safe,
                &key_collations,
                join_type,
                join_spec.as_ref(),
                template_to_batch_schema(&probe_template),
                residual_program
                    .as_ref()
                    .map(|(program, schema)| (&**program, schema.clone())),
                grace_partitions,
                grace_cap,
                hj_limits.grace_max_depth,
                0,
                false,
            )?;
            VEC_HASH_JOIN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(Some(materialize_join_batches(
                std::iter::from_fn(|| output.next_batch().transpose()),
                out_template,
            )?));
        }
        Err(e) => return Err(e),
    };
    VEC_HASH_JOIN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(Some(batches_to_plan_rows(out_batches, out_template)?))
}

pub(crate) struct HashJoinBatchOperator<'a> {
    build_plan: Option<&'a PhysicalPlan>,
    pushed_runtime_rejected: Option<Arc<std::sync::atomic::AtomicU64>>,
    db: Arc<Database>,
    txn: TxnView,
    build: Box<dyn BatchOperator + 'a>,
    probe: Box<dyn BatchOperator + 'a>,
    build_keys: Vec<usize>,
    probe_keys: Vec<usize>,
    probe_key_exprs: Vec<Expr>,
    key_collations: Vec<Option<u16>>,
    null_safe: Vec<bool>,
    join_type: crate::parser::JoinType,
    build_is_left: bool,
    prefer_spill_key_locality: bool,
    schema: Arc<BatchSchema>,
    probe_schema: Arc<BatchSchema>,
    residual: Option<(Arc<expr_batch::BatchExprProgram>, Arc<BatchSchema>)>,
    state: Option<HashJoinState>,
    output: Option<std::vec::IntoIter<Batch>>,
    grace_output: Option<GraceJoinOutput>,
    work_area: Option<crate::engine::executor::work_area::WorkAreaHandle>,
    build_memory_charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
    finished: bool,
    /// An empty ANTI build preserves every probe row unchanged.  Keep this
    /// as a streaming mode instead of manufacturing an empty hash payload
    /// and one selection vector per probe batch.
    empty_build_probe_passthrough: bool,
    reverse_semi_anti: bool,
    pushed_exact_runtime_rejected:
        Option<Arc<std::sync::OnceLock<Arc<std::sync::atomic::AtomicU64>>>>,
    pushed_runtime_rejected_reported: u64,
}

struct ParallelDirectCountPartial {
    counts: Vec<Vec<u64>>,
    matched_words: Vec<u64>,
    match_rows: u64,
}

pub(super) fn merge_direct_count_lane_parallel(target: &mut [u64], source: &[u64], workers: usize) {
    debug_assert_eq!(target.len(), source.len());
    if target.is_empty() {
        return;
    }
    let chunk = target.len().div_ceil(workers.max(1));
    std::thread::scope(|scope| {
        for (target, source) in target.chunks_mut(chunk).zip(source.chunks(chunk)) {
            scope.spawn(move || {
                for (target, source) in target.iter_mut().zip(source) {
                    *target = target.saturating_add(*source);
                }
            });
        }
    });
}

#[inline]
pub(super) fn direct_count_group_matched(words: &[u64], group: usize) -> bool {
    words
        .get(group / u64::BITS as usize)
        .is_some_and(|word| word & (1u64 << (group % u64::BITS as usize)) != 0)
}

fn run_parallel_unique_build_count_probe(
    probe: &mut dyn BatchOperator,
    probe_key: usize,
    build_index: &vec_hash_join::I64BuildIndex,
    build_columns: &[ColumnVector],
    aggregate_sources: &[Option<vec_hash_agg::DirectJoinColumn>],
    groups: usize,
    workers: usize,
) -> Result<(Vec<Vec<u64>>, Vec<u64>, u64, u64, u64), QueryError> {
    let workers = workers.max(1);
    let single_count_source = match aggregate_sources {
        [source] => Some(*source),
        _ => None,
    };
    let single_build_count_nonnull = match single_count_source {
        Some(Some(vec_hash_agg::DirectJoinColumn::Build(column))) => build_columns
            .get(column)
            .is_some_and(|values| values.count_non_null() == values.len()),
        _ => false,
    };
    let mut probe_batches = 0u64;
    let mut probe_rows = 0u64;
    std::thread::scope(|scope| {
        let mut senders = Vec::with_capacity(workers);
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (sender, receiver) = std::sync::mpsc::sync_channel::<Batch>(2);
            senders.push(sender);
            handles.push(
                scope.spawn(move || -> Result<ParallelDirectCountPartial, QueryError> {
                    let mut counts = aggregate_sources
                        .iter()
                        .map(|_| vec![0u64; groups])
                        .collect::<Vec<_>>();
                    let mut matched_words = vec![0u64; groups.div_ceil(u64::BITS as usize)];
                    let mut match_rows = 0u64;
                    while let Ok(batch) = receiver.recv() {
                        let key_column = batch.columns().get(probe_key).ok_or_else(|| {
                            crate::storage::error(
                                crate::error::catalog::ER_INTERNAL_ERROR,
                                "parallel direct join probe key is out of bounds",
                            )
                        })?;
                        let Some(keys) = key_column.as_i64_slice() else {
                            return Err(crate::storage::error(
                                crate::error::catalog::ER_INTERNAL_ERROR,
                                "parallel direct join probe key is not an integer lane",
                            ));
                        };
                        let key_has_nulls = key_column.count_non_null() != key_column.len();
                        let single_count_nonnull = match single_count_source {
                            Some(None) => true,
                            Some(Some(vec_hash_agg::DirectJoinColumn::Build(_))) => {
                                single_build_count_nonnull
                            }
                            Some(Some(vec_hash_agg::DirectJoinColumn::Probe(column))) => batch
                                .columns()
                                .get(column)
                                .is_some_and(|values| values.count_non_null() == values.len()),
                            None => false,
                        };
                        for probe_row in batch.visible_indices() {
                            if key_has_nulls && key_column.is_null(probe_row) {
                                continue;
                            }
                            let Some(build_row) = build_index.get_unique(&keys[probe_row]) else {
                                continue;
                            };
                            let build_row = build_row as usize;
                            matched_words[build_row / u64::BITS as usize] |=
                                1u64 << (build_row % u64::BITS as usize);
                            match_rows = match_rows.saturating_add(1);
                            if single_count_nonnull {
                                counts[0][build_row] = counts[0][build_row].saturating_add(1);
                            } else {
                                for (slot, source) in aggregate_sources.iter().enumerate() {
                                    let increment = match source {
                                        None => true,
                                        Some(vec_hash_agg::DirectJoinColumn::Build(column)) => {
                                            !build_columns[*column].is_null(build_row)
                                        }
                                        Some(vec_hash_agg::DirectJoinColumn::Probe(column)) => {
                                            !batch.columns()[*column].is_null(probe_row)
                                        }
                                    };
                                    if increment {
                                        counts[slot][build_row] =
                                            counts[slot][build_row].saturating_add(1);
                                    }
                                }
                            }
                        }
                    }
                    Ok(ParallelDirectCountPartial {
                        counts,
                        matched_words,
                        match_rows,
                    })
                }),
            );
        }

        let mut next_worker = 0usize;
        while let Some(batch) = probe.next_batch()? {
            probe_batches = probe_batches.saturating_add(1);
            probe_rows = probe_rows.saturating_add(batch.visible_rows() as u64);
            senders[next_worker].send(batch).map_err(|_| {
                crate::storage::error(
                    crate::error::catalog::ER_INTERNAL_ERROR,
                    "parallel direct join count worker stopped early",
                )
            })?;
            next_worker = (next_worker + 1) % workers;
        }
        drop(senders);

        let mut partials = Vec::with_capacity(workers);
        for handle in handles {
            partials.push(handle.join().map_err(|_| {
                crate::storage::error(
                    crate::error::catalog::ER_INTERNAL_ERROR,
                    "parallel direct join count worker panicked",
                )
            })??);
        }
        let mut merged = partials.pop().expect("at least one direct count worker");
        for partial in partials {
            merged.match_rows = merged.match_rows.saturating_add(partial.match_rows);
            for (target, source) in merged.counts.iter_mut().zip(partial.counts) {
                merge_direct_count_lane_parallel(target, &source, workers);
            }
            for (target, source) in merged.matched_words.iter_mut().zip(partial.matched_words) {
                *target |= source;
            }
        }
        Ok((
            merged.counts,
            merged.matched_words,
            probe_batches,
            probe_rows,
            merged.match_rows,
        ))
    })
}

/// Register only when the shared work-area policy is enabled.
fn register_hash_join_work_area() -> Option<crate::engine::executor::work_area::WorkAreaHandle> {
    if crate::engine::executor::select::plan::hash_join_work_area_disabled() {
        return None;
    }
    Some(crate::engine::executor::work_area::WorkAreaHandle::register())
}

pub(super) fn vec_hash_join_admission_cap(
    max_build_rows: u64,
    mem_limit_bytes: u64,
    mem_limit_explicit: bool,
    work_area_registered: bool,
) -> vec_hash_join::BuildCap {
    vec_hash_join::BuildCap {
        max_rows: max_build_rows,
        // The query quota is a soft spill watermark. A WorkArea handle is the
        // resident-memory authority for vector joins and grows only inside the
        // executor target/fair share, so do not let a parent's conservative
        // query-budget reservation force this child to disk. Explicit
        // MYDB_HASH_JOIN_MEM_LIMIT_MB remains a hard operator cap.
        max_bytes: if work_area_registered && !mem_limit_explicit {
            0
        } else {
            mem_limit_bytes
        },
    }
}

fn vec_hash_join_collector_cap(
    limits: crate::engine::executor::select::plan::HashJoinLimits,
    work_area: Option<&crate::engine::executor::work_area::WorkAreaHandle>,
) -> vec_hash_join::BuildCap {
    let granted = work_area
        .map(|handle| handle.reserved_bytes() as u64)
        .unwrap_or(limits.mem_limit_bytes);
    vec_hash_join::BuildCap {
        max_rows: limits.max_build_rows,
        max_bytes: if limits.mem_limit_explicit {
            granted.min(limits.mem_limit_bytes)
        } else {
            // The collector's key-aware estimate is intentionally more
            // conservative than the preflight capacity charge. Treating the
            // exact current reservation as a second hard cap can therefore
            // spill a small composite-key build even though WorkArea admitted
            // it and the ordinary query replay quota has ample room. Actual
            // resident bytes remain governed by the preflight reservation.
            granted.max(limits.mem_limit_bytes)
        },
    }
}

impl HashJoinBatchOperator<'_> {
    /// Release resident data before relinquishing its governed reservation.
    fn finish_input(&mut self) {
        self.state = None;
        self.output = None;
        self.grace_output = None;
        self.work_area = None;
        self.build_memory_charge = None;
        self.finished = true;
    }

    fn report_pushed_runtime_rejections(&mut self) {
        let Some(rejected) = self.pushed_runtime_rejected.as_ref().or_else(|| {
            self.pushed_exact_runtime_rejected
                .as_ref()
                .and_then(|rejected| rejected.get())
        }) else {
            return;
        };
        let observed = rejected.load(std::sync::atomic::Ordering::Relaxed);
        let delta = observed.saturating_sub(self.pushed_runtime_rejected_reported);
        if delta != 0 {
            analyze_stats::add_pending_runtime_filter_rejected(delta);
            self.pushed_runtime_rejected_reported = observed;
        }
    }

    fn install_probe_runtime_filter(&mut self, payload: &vec_hash_join::BuildPayload) {
        use crate::engine::executor::select::bloom::BloomFilter;
        use crate::engine::expr::ValueKey;
        use crate::parser::{Expr, JoinType};
        use crate::planner::RuntimeFilter;

        if self.join_type != JoinType::Inner
            || self.build_keys.is_empty()
            || self.build_keys.len() != self.probe_keys.len()
        {
            return;
        }
        let rows = payload.columns.first().map(ColumnVector::len).unwrap_or(0);
        if rows == 0 {
            return;
        }
        let keys = if self.probe_key_exprs.len() == self.probe_keys.len() {
            self.probe_key_exprs.clone()
        } else {
            let mut keys = Vec::with_capacity(self.probe_keys.len());
            for &index in &self.probe_keys {
                let Some(field) = self.probe_schema.fields().get(index) else {
                    return;
                };
                let qualifier = field
                    .alias
                    .as_deref()
                    .unwrap_or(&field.table)
                    .rsplit('.')
                    .next()
                    .unwrap_or(&field.table)
                    .to_string();
                keys.push(Expr::CompoundIdentifier(vec![
                    qualifier,
                    field.name.clone(),
                ]));
            }
            keys
        };

        if let Some(filter) = vec_hash_join::build_exact_int_runtime_filter(
            payload,
            &self.build_keys,
            &self.key_collations,
        ) {
            let rejected = Arc::new(std::sync::OnceLock::new());
            let installed = self
                .probe
                .install_runtime_filter(RuntimeFilter::ExactIntKey {
                    keys,
                    null_safe: self.null_safe.clone(),
                    filter,
                    rejected: rejected.clone(),
                });
            if installed {
                self.pushed_exact_runtime_rejected = Some(rejected);
            }
            return;
        }

        let native_int_pair = self.build_keys.len() == 2
            && self.null_safe.iter().all(|value| !*value)
            && self.key_collations.iter().all(Option::is_none)
            && self.build_keys.iter().all(|&column| {
                payload
                    .columns
                    .get(column)
                    .and_then(ColumnVector::as_i64_slice)
                    .is_some()
            });
        let rejected =
            analyze_stats::is_recording().then(|| Arc::new(std::sync::atomic::AtomicU64::new(0)));
        if native_int_pair {
            let mut installed = false;
            for (component, &column) in self.build_keys.iter().enumerate() {
                let mut bloom = BloomFilter::new_int(rows.max(1).div_ceil(2));
                let values = payload.columns[column]
                    .as_i64_slice()
                    .expect("checked i64 pair");
                for row in 0..rows {
                    if self
                        .build_keys
                        .iter()
                        .all(|&key| !payload.columns[key].is_null(row))
                    {
                        bloom.insert_int(i128::from(values[row]));
                    }
                }
                installed |= self.probe.install_runtime_filter(RuntimeFilter::BloomKey {
                    keys: vec![keys[component].clone()],
                    null_safe: vec![false],
                    filter: Arc::new(bloom),
                    rejected: rejected.clone(),
                });
            }
            if installed {
                self.pushed_runtime_rejected = rejected;
            }
            return;
        }
        // RuntimeFilter has no collation metadata; encoded join keys remain
        // authoritative for collated text.
        if self.key_collations.iter().any(Option::is_some) {
            return;
        }
        let (bloom, inserted) = {
            let mut bloom = BloomFilter::new(rows);
            let mut inserted = 0usize;
            let mut tuple = Vec::with_capacity(self.build_keys.len());
            for row in 0..rows {
                tuple.clear();
                let mut usable = true;
                for (slot, &column) in self.build_keys.iter().enumerate() {
                    let Some(values) = payload.columns.get(column) else {
                        return;
                    };
                    if values.is_null(row) && !self.null_safe.get(slot).copied().unwrap_or(false) {
                        usable = false;
                        break;
                    }
                    tuple.push(ValueKey::from(&values.value_at(row)));
                }
                if usable {
                    bloom.insert(tuple.as_slice());
                    inserted += 1;
                }
            }
            (bloom, inserted)
        };
        if inserted == 0 {
            return;
        }
        if self.probe.install_runtime_filter(RuntimeFilter::BloomKey {
            keys,
            null_safe: self.null_safe.clone(),
            filter: Arc::new(bloom),
            rejected: rejected.clone(),
        }) {
            self.pushed_runtime_rejected = rejected;
        }
    }

    /// SEMI/ANTI joins only need existence, not build payload.  When the
    /// preserved probe is materially smaller, consume it first into a bounded
    /// key set and stream the nominal build through that set.  This is the
    /// batch equivalent of a dynamic runtime filter: existence joins retain
    /// only matching build keys instead of constructing a payload for every
    /// qualifying fact row.
    fn try_initialize_reverse_semi_anti(&mut self) -> Result<bool, QueryError> {
        use crate::engine::executor::mem_budget;
        use crate::engine::executor::work_area::{SpillHint, WorkAreaHandle};
        use crate::parser::JoinType;

        if !self.reverse_semi_anti
            || !matches!(self.join_type, JoinType::Semi | JoinType::Anti)
            || self.residual.is_some()
        {
            return Ok(false);
        }

        let probe_schema = self.probe.schema().clone();
        let mut native_i64 = self.build_keys.len() == 1
            && self.probe_keys.len() == 1
            && self.null_safe.iter().all(|value| !*value)
            && self.key_collations.iter().all(Option::is_none)
            && vec_hash_join::schema_column_uses_i64_lane(self.build.schema(), self.build_keys[0])
            && vec_hash_join::schema_column_uses_i64_lane(&probe_schema, self.probe_keys[0]);
        let handle = WorkAreaHandle::register();
        let mut retained_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        let own_limit = mem_budget::current_budget().map(|budget| budget.limit());
        let mut probe_batches = Vec::new();
        let mut probe_rows = 0usize;
        let mut key_bytes = 0usize;
        while let Some(batch) = self.probe.next_batch()? {
            let rows = batch.visible_rows();
            if rows == 0 {
                continue;
            }
            // A selective scan's tail still owns full decode capacity, and a
            // selection view still owns rejected rows. Retain only visible
            // values with compact capacities, using the common typed gather
            // so nullable, collated, composite and variable-width lanes agree.
            let mut indices: Vec<u32> =
                mem_budget::try_vec_with_capacity(rows, "retained semi/anti indices")?;
            indices.extend(batch.visible_indices().map(|row| row as u32));
            let compact = Batch::with_row_count(
                probe_schema.clone(),
                batch
                    .columns()
                    .iter()
                    .map(|column| column.take_indices(&indices))
                    .collect(),
                rows,
            );
            drop(batch);
            drop(indices);
            // Computed keys may produce a Generic lane despite signed-i64
            // schema metadata. Keep the runtime type check, and upgrade the
            // previous key reservations before selecting generic encoding.
            let upgrade_keys = if native_i64
                && !matches!(
                    compact.columns().get(self.probe_keys[0]),
                    Some(ColumnVector::I64 { .. } | ColumnVector::Null { .. })
                ) {
                native_i64 = false;
                key_bytes
            } else {
                0
            };
            let resident_bytes = compact
                .memory_bytes()
                .saturating_add(std::mem::size_of::<Batch>() * 2)
                .saturating_add(compact.columns().len() * std::mem::size_of::<ColumnVector>());
            let bytes_per_row = if native_i64 { 80usize } else { 160 };
            let added_keys = rows
                .saturating_mul(bytes_per_row)
                .saturating_add(upgrade_keys);
            let added = resident_bytes.saturating_add(added_keys);
            let memory_wants_spill = own_limit
                .is_some_and(|limit| retained_charge.charged().saturating_add(added) > limit)
                || retained_charge.add(added).is_err();
            if memory_wants_spill {
                // Only the real crossing batch must stay attributed for
                // replay. Do not force-charge a key set we will never create.
                retained_charge.add_force(resident_bytes)?;
            } else {
                key_bytes = key_bytes.saturating_add(added_keys);
            }
            probe_rows = probe_rows.saturating_add(rows);
            probe_batches.push(compact);
            if memory_wants_spill || matches!(handle.try_charge(added), SpillHint::Spill) {
                retained_charge.release_bytes(key_bytes);
                let remainder = std::mem::replace(
                    &mut self.probe,
                    Box::new(MaterializedBatchOperator::new(probe_schema, Vec::new())),
                );
                self.probe = Box::new(RetainedProbeStream {
                    prefix: probe_batches.into_iter(),
                    remainder,
                    memory_charge: Some(retained_charge),
                });
                return Ok(false);
            }
        }
        if probe_rows == 0 {
            self.output = Some(Vec::new().into_iter());
            return Ok(true);
        }

        // The dominant SEMI/ANTI shape is a single signed integer equality
        // (fact FK against a much smaller preserved key domain).  Keep that
        // domain in its native lane.  The generic representation below is
        // required for collated/composite keys, but encoding every integer as
        // a tagged Vec<u8> and cloning matching buffers makes the fact-side
        // scan allocation/hash bound even though existence is the only
        // required result.
        if native_i64 {
            let probe_key = self.probe_keys[0];
            let build_key = self.build_keys[0];
            let mut candidate_builder =
                crate::engine::executor::select::bloom::ExactIntKeySetBuilder::with_capacity(
                    probe_rows,
                )?;
            for batch in &probe_batches {
                let column = &batch.columns()[probe_key];
                let Some(values) = column.as_i64_slice() else {
                    // An all-NULL typed batch has no i64 payload and cannot
                    // contribute to an ordinary (non-null-safe) equality.
                    debug_assert!(matches!(column, ColumnVector::Null { .. }));
                    continue;
                };
                for row in batch.visible_indices() {
                    if !column.is_null(row) {
                        candidate_builder.insert(i128::from(values[row]));
                    }
                }
            }
            let candidates = Arc::new(candidate_builder.finish()?);

            // The build source has not been opened yet. Publish the exact
            // preserved-side key domain through the ordinary runtime-filter
            // interface so storage decodes the integer key first and avoids
            // materializing local-predicate/output columns for non-members.
            // The parent semi/anti check remains the semantic authority.
            if let Some(field) = self.build.schema().fields().get(build_key) {
                let qualifier = field
                    .alias
                    .as_deref()
                    .unwrap_or(&field.table)
                    .rsplit('.')
                    .next()
                    .unwrap_or(&field.table)
                    .to_string();
                let rejected = Arc::new(std::sync::OnceLock::new());
                if self
                    .build
                    .install_runtime_filter(crate::planner::RuntimeFilter::ExactIntKey {
                        keys: vec![Expr::CompoundIdentifier(vec![
                            qualifier,
                            field.name.clone(),
                        ])],
                        null_safe: vec![false],
                        filter: Arc::clone(&candidates),
                        rejected: Arc::clone(&rejected),
                    })
                {
                    self.pushed_exact_runtime_rejected = Some(rejected);
                }
            }

            let mut matched: hashbrown::HashSet<i64, ahash::RandomState> =
                hashbrown::HashSet::with_hasher(ahash::RandomState::new());
            matched.try_reserve(probe_rows).map_err(|_| {
                crate::engine::executor::mem_budget::allocation_error(
                    "unable to allocate reverse semi/anti integer match set",
                )
            })?;
            let mut build_rows = 0u64;
            let mut match_rows = 0u64;
            while let Some(batch) = self.build.next_batch()? {
                let column = &batch.columns()[build_key];
                let Some(values) = column.as_i64_slice() else {
                    if matches!(column, ColumnVector::Null { .. }) {
                        build_rows = build_rows.saturating_add(batch.visible_rows() as u64);
                        analyze_stats::add_pending_runtime_filter_rejected(
                            batch.visible_rows() as u64
                        );
                        continue;
                    }
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_INTERNAL_ERROR,
                        "signed integer hash-join key produced a non-integer batch lane",
                    ));
                };
                for row in batch.visible_indices() {
                    build_rows = build_rows.saturating_add(1);
                    if !column.is_null(row) && candidates.contains(i128::from(values[row])) {
                        match_rows = match_rows.saturating_add(1);
                        matched.insert(values[row]);
                    } else {
                        analyze_stats::add_pending_runtime_filter_rejected(1);
                    }
                }
            }

            let mut output = crate::engine::executor::mem_budget::try_vec_with_capacity(
                probe_batches.len(),
                "reverse semi/anti output batches",
            )?;
            for batch in probe_batches {
                let column = &batch.columns()[probe_key];
                let values = column.as_i64_slice();
                let mut selected = crate::engine::executor::mem_budget::try_vec_with_capacity(
                    batch.visible_rows(),
                    "reverse semi/anti selection",
                )?;
                for row in batch.visible_indices() {
                    let exists = values.is_some_and(|values| {
                        !column.is_null(row) && matched.contains(&values[row])
                    });
                    if exists == (self.join_type == JoinType::Semi) {
                        selected.push(row as u32);
                    }
                }
                if !selected.is_empty() {
                    output.push(batch.with_selection(Selection::Indices(selected)));
                }
            }
            analyze_stats::add_pending_join_runtime(build_rows, probe_rows as u64, match_rows);
            self.work_area = Some(handle);
            self.build_memory_charge = Some(retained_charge);
            self.output = Some(output.into_iter());
            return Ok(true);
        }

        let mut candidates: hashbrown::HashSet<Vec<u8>, ahash::RandomState> =
            hashbrown::HashSet::with_hasher(ahash::RandomState::new());
        candidates.try_reserve(probe_rows).map_err(|_| {
            crate::engine::executor::mem_budget::allocation_error(
                "unable to allocate reverse semi/anti candidate set",
            )
        })?;
        let mut encoded = Vec::with_capacity(64);
        for batch in &probe_batches {
            for row in batch.visible_indices() {
                if vec_hash_join::encode_join_key_with_collations(
                    batch,
                    &self.probe_keys,
                    &self.null_safe,
                    &self.key_collations,
                    row,
                    &mut encoded,
                ) {
                    if candidates.contains(encoded.as_slice()) {
                        continue;
                    }
                    retained_charge.add_force(encoded.len())?;
                    candidates.insert(crate::engine::executor::mem_budget::try_copy_bytes(
                        &encoded,
                        "reverse semi/anti candidate key",
                    )?);
                }
            }
        }

        let mut matched: hashbrown::HashSet<Vec<u8>, ahash::RandomState> =
            hashbrown::HashSet::with_hasher(ahash::RandomState::new());
        matched.try_reserve(candidates.len()).map_err(|_| {
            crate::engine::executor::mem_budget::allocation_error(
                "unable to allocate reverse semi/anti match set",
            )
        })?;
        let mut build_rows = 0u64;
        let mut match_rows = 0u64;
        while let Some(batch) = self.build.next_batch()? {
            for row in batch.visible_indices() {
                build_rows = build_rows.saturating_add(1);
                let usable = vec_hash_join::encode_join_key_with_collations(
                    &batch,
                    &self.build_keys,
                    &self.null_safe,
                    &self.key_collations,
                    row,
                    &mut encoded,
                );
                if usable && candidates.contains(encoded.as_slice()) {
                    match_rows = match_rows.saturating_add(1);
                    if !matched.contains(encoded.as_slice()) {
                        retained_charge.add_force(encoded.len())?;
                        matched.insert(crate::engine::executor::mem_budget::try_copy_bytes(
                            &encoded,
                            "reverse semi/anti matched key",
                        )?);
                    }
                } else {
                    analyze_stats::add_pending_runtime_filter_rejected(1);
                }
            }
        }

        let mut output = crate::engine::executor::mem_budget::try_vec_with_capacity(
            probe_batches.len(),
            "reverse semi/anti output batches",
        )?;
        for batch in probe_batches {
            let mut selected = crate::engine::executor::mem_budget::try_vec_with_capacity(
                batch.visible_rows(),
                "reverse semi/anti selection",
            )?;
            for row in batch.visible_indices() {
                let usable = vec_hash_join::encode_join_key_with_collations(
                    &batch,
                    &self.probe_keys,
                    &self.null_safe,
                    &self.key_collations,
                    row,
                    &mut encoded,
                );
                let exists = usable && matched.contains(encoded.as_slice());
                if exists == (self.join_type == JoinType::Semi) {
                    selected.push(row as u32);
                }
            }
            if !selected.is_empty() {
                output.push(batch.with_selection(Selection::Indices(selected)));
            }
        }
        analyze_stats::add_pending_join_runtime(build_rows, probe_rows as u64, match_rows);
        self.work_area = Some(handle);
        self.build_memory_charge = Some(retained_charge);
        self.output = Some(output.into_iter());
        Ok(true)
    }

    fn initialize(&mut self) -> Result<(), QueryError> {
        use crate::parser::JoinType;
        if self.finished
            || self.empty_build_probe_passthrough
            || self.output.is_some()
            || self.grace_output.is_some()
            || self.state.is_some()
        {
            return Ok(());
        }
        if self.try_initialize_reverse_semi_anti()? {
            return Ok(());
        }
        let limits = crate::engine::executor::select::plan::hash_join_limits();
        let grace_cap = vec_hash_join::BuildCap {
            max_rows: limits.max_build_rows,
            max_bytes: limits.mem_limit_bytes,
        };
        let work_area = register_hash_join_work_area();
        let admission_cap = vec_hash_join_admission_cap(
            limits.max_build_rows,
            limits.mem_limit_bytes,
            limits.mem_limit_explicit,
            work_area.is_some(),
        );
        let build_schema = self.build.schema().clone();
        let (prefix, overflow, build_memory_charge) =
            drain_vec_join_build_to_cap(self.build.as_mut(), admission_cap, work_area.as_ref())?;
        let grace_partitions = self
            .build_plan
            .map_or(limits.grace_partitions as usize, |plan| {
                vec_grace_partition_count(
                    limits.grace_partitions as usize,
                    &prefix,
                    crate::planner::estimated_rows(plan),
                    grace_cap,
                    scheduler_hash_join_workers(),
                )
            });
        let build_cols_first = self.build_is_left;
        let spec = (!matches!(self.join_type, JoinType::Semi | JoinType::Anti)).then(|| {
            vec_hash_join::VecJoinSpec {
                build_keys: &self.build_keys,
                probe_keys: &self.probe_keys,
                key_collations: &self.key_collations,
                null_safe: &self.null_safe,
                build_cols_first,
                out_schema: self.schema.clone(),
                residual: self.residual.as_ref().map(|(program, _)| &**program),
            }
        });
        if overflow {
            drop(work_area);
            let batches = run_vec_grace_hash_join_stream(
                &self.db,
                self.txn,
                prefix,
                Some(build_memory_charge),
                self.build.as_mut(),
                self.probe.as_mut(),
                &self.build_keys,
                &self.probe_keys,
                &self.null_safe,
                &self.key_collations,
                self.join_type,
                spec.as_ref(),
                self.probe_schema.clone(),
                self.residual
                    .as_ref()
                    .map(|(program, schema)| (&**program, schema.clone())),
                grace_partitions,
                grace_cap,
                limits.grace_max_depth,
                0,
                self.prefer_spill_key_locality,
            )?;
            self.grace_output = Some(batches);
        } else {
            let replay_prefix = prefix.clone();
            let mut build = MaterializedBatchOperator::new(build_schema, prefix);
            let collector_cap = vec_hash_join_collector_cap(limits, work_area.as_ref());
            let preserve_build = self.join_type == JoinType::Full
                || (self.join_type == JoinType::Left && self.build_is_left)
                || (self.join_type == JoinType::Right && !self.build_is_left);
            let native_i64_pair = self.build_keys.len() == 2
                && self.null_safe.iter().all(|value| !*value)
                && self.key_collations.iter().all(Option::is_none)
                && self.build_keys.iter().all(|&column| {
                    vec_hash_join::schema_column_uses_i64_lane(build.schema(), column)
                })
                && self.probe_keys.iter().all(|&column| {
                    vec_hash_join::schema_column_uses_i64_lane(self.probe.schema(), column)
                });
            let payload = vec_hash_join::with_vec_build_cap(collector_cap, || {
                if preserve_build
                    && self.join_type == JoinType::Full
                    && native_i64_pair
                    && self.probe_key_exprs.iter().all(is_simple_column_ref)
                    && replay_prefix.iter().all(|batch| {
                        self.build_keys.iter().all(|&column| {
                            matches!(
                                batch.columns()[column],
                                ColumnVector::I64 { .. } | ColumnVector::Null { .. }
                            )
                        })
                    })
                {
                    let reserve_rows = replay_prefix
                        .iter()
                        .map(|batch| {
                            batch
                                .visible_indices()
                                .filter(|&row| {
                                    self.build_keys
                                        .iter()
                                        .all(|&column| !batch.columns()[column].is_null(row))
                                })
                                .count()
                        })
                        .sum();
                    vec_hash_join::collect_full_build_payload_i64_pair_only(
                        &mut build,
                        &self.build_keys,
                        reserve_rows,
                    )
                } else if preserve_build {
                    // A preserved build side must retain rows whose ordinary
                    // equality key is unusable (most importantly NULL for
                    // non-null-safe equality). They cannot enter the hash
                    // index, but the final NULL-extension pass still has to
                    // emit them exactly once.
                    vec_hash_join::collect_full_build_payload(
                        &mut build,
                        &self.build_keys,
                        &self.null_safe,
                        &self.key_collations,
                    )
                } else if self.build_keys.len() == 1
                    && self.null_safe.iter().all(|value| !*value)
                    && self.key_collations.iter().all(Option::is_none)
                    && vec_hash_join::schema_column_uses_i64_lane(
                        build.schema(),
                        self.build_keys[0],
                    )
                    && vec_hash_join::schema_column_uses_i64_lane(
                        self.probe.schema(),
                        self.probe_keys[0],
                    )
                {
                    vec_hash_join::collect_build_payload_i64_only(
                        &mut build,
                        &self.build_keys,
                        &self.null_safe,
                        &self.key_collations,
                    )
                } else if native_i64_pair {
                    vec_hash_join::collect_build_payload_i64_pair_only(
                        &mut build,
                        &self.build_keys,
                        &self.null_safe,
                        &self.key_collations,
                    )
                } else {
                    vec_hash_join::collect_build_payload_with_collations(
                        &mut build,
                        &self.build_keys,
                        &self.null_safe,
                        &self.key_collations,
                    )
                }
            });
            let payload = match payload {
                Ok(payload) => payload,
                Err(error) if vec_hash_join::is_vec_build_overflow(&error) => {
                    // Exact encoded-key accounting can exceed the coarse
                    // batch preflight.  The collector has not probed yet, so
                    // replay the shared build prefix through Grace spill.
                    drop(work_area);
                    let batches = run_vec_grace_hash_join_stream(
                        &self.db,
                        self.txn,
                        replay_prefix,
                        Some(build_memory_charge),
                        self.build.as_mut(),
                        self.probe.as_mut(),
                        &self.build_keys,
                        &self.probe_keys,
                        &self.null_safe,
                        &self.key_collations,
                        self.join_type,
                        spec.as_ref(),
                        self.probe_schema.clone(),
                        self.residual
                            .as_ref()
                            .map(|(program, schema)| (&**program, schema.clone())),
                        grace_partitions,
                        grace_cap,
                        limits.grace_max_depth,
                        0,
                        self.prefer_spill_key_locality,
                    )?;
                    self.grace_output = Some(batches);
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            // The payload owns every retained build column. Drop the exhausted
            // child graph now; preserve only its schema for column mapping.
            self.build = Box::new(MaterializedBatchOperator::new(
                self.build.schema().clone(),
                Vec::new(),
            ));
            self.work_area = work_area;
            let mut build_memory_charge = build_memory_charge;
            if let Some(retained) = self.build_memory_charge.as_mut() {
                retained.absorb(&mut build_memory_charge);
            } else {
                self.build_memory_charge = Some(build_memory_charge);
            }
            let build_rows = payload.columns.first().map(ColumnVector::len).unwrap_or(0);
            if build_rows == 0 {
                match self.join_type {
                    // An empty build makes INNER/SEMI unsatisfiable.  Do not
                    // open the probe: it can be an arbitrarily expensive
                    // scan or join subtree, and no row from it is observable.
                    JoinType::Inner | JoinType::Semi => {
                        self.probe = Box::new(MaterializedBatchOperator::new(
                            self.probe.schema().clone(),
                            Vec::new(),
                        ));
                        self.work_area = None;
                        self.build_memory_charge = None;
                        self.finished = true;
                        return Ok(());
                    }
                    // ANTI preserves the SQL-left probe verbatim.  Its
                    // published schema is already probe_schema, so batches
                    // can pass through without a materialization or bitmap.
                    JoinType::Anti => {
                        self.work_area = None;
                        self.build_memory_charge = None;
                        self.empty_build_probe_passthrough = true;
                        return Ok(());
                    }
                    _ => {}
                }
            }
            self.install_probe_runtime_filter(&payload);
            analyze_stats::add_pending_join_runtime(build_rows as u64, 0, 0);
            let streaming_probe = (self.residual.is_none()
                && !preserve_build
                && matches!(
                    self.join_type,
                    JoinType::Inner | JoinType::Left | JoinType::Right
                )
                && self.null_safe.iter().all(|value| !*value)
                && payload.i64_pair_index.is_none())
            .then(|| {
                vec_hash_join::StreamingProbe::new(&payload, &self.probe_keys, &self.key_collations)
            });
            self.state = Some(HashJoinState {
                payload,
                streaming_probe,
                matched_build: preserve_build.then(|| vec![false; build_rows]),
                pending: {
                    let mut queue =
                        crate::engine::executor::vec::join_output::JoinOutputQueue::new();
                    queue.set_data_dir(self.db.data_dir());
                    queue
                },
                probe_started: false,
                probe_done: false,
                unmatched_build_emitted: false,
            });
        }
        Ok(())
    }

    /// Evaluate INNER-join residuals in bounded pair morsels instead of once
    /// per probe-key bucket.  PK/FK joins commonly have one build hit per
    /// probe row; the generic loop would then allocate and evaluate thousands
    /// of one-row batches. INNER joins need no per-probe unmatched bookkeeping,
    /// so all candidate pairs in the input batch can share one residual call.
    fn process_probe_batch_inner_residual(&mut self, batch: Batch) -> Result<(), QueryError> {
        use crate::parser::JoinType;

        debug_assert_eq!(self.join_type, JoinType::Inner);
        let (program, _) = self
            .residual
            .as_ref()
            .expect("inner residual batch path requires a residual");
        let program = Arc::clone(program);
        let schema = Arc::clone(&self.schema);
        let build_cols_first = self.build_is_left;
        let probe_keys = self.probe_keys.clone();
        let null_safe = self.null_safe.clone();
        let key_collations = self.key_collations.clone();
        let state = self.state.as_mut().expect("general hash join initialized");
        let probe_rows = batch.visible_rows() as u64;
        let mut match_rows = 0u64;
        let mut pair_build = Vec::with_capacity(DEFAULT_BATCH_ROWS);
        let mut pair_probe = Vec::with_capacity(DEFAULT_BATCH_ROWS);
        let mut key = Vec::with_capacity(64);
        let lookup = vec_hash_join::PayloadProbe::new(
            &state.payload,
            &batch,
            &probe_keys,
            &null_safe,
            &key_collations,
        );

        let mut flush = |pair_build: &mut Vec<u32>,
                         pair_probe: &mut Vec<u32>,
                         match_rows: &mut u64|
         -> Result<(), QueryError> {
            if pair_build.is_empty() {
                return Ok(());
            }
            let candidates = vec_hash_join::gather_pairs_batch(
                &state.payload.columns,
                &batch,
                pair_build,
                pair_probe,
                build_cols_first,
                &schema,
            );
            let accepted = program.eval_predicate_bitmap(&candidates)?;
            let accepted_rows = accepted.count_ones();
            *match_rows = match_rows.saturating_add(accepted_rows as u64);
            if accepted_rows != 0 {
                state
                    .pending
                    .push_back(candidates.with_selection(Selection::Mask(accepted)))?;
            }
            pair_build.clear();
            pair_probe.clear();
            Ok(())
        };

        for probe_row in batch.visible_indices() {
            let (usable, hits) = lookup.lookup(probe_row, &mut key);
            if usable && hits.is_none() {
                analyze_stats::add_pending_runtime_filter_rejected(1);
            }
            if let Some(hits) = hits {
                for &build_row in hits {
                    pair_build.push(build_row);
                    pair_probe.push(probe_row as u32);
                    if pair_build.len() == DEFAULT_BATCH_ROWS {
                        flush(&mut pair_build, &mut pair_probe, &mut match_rows)?;
                    }
                }
            }
        }
        flush(&mut pair_build, &mut pair_probe, &mut match_rows)?;
        analyze_stats::add_pending_join_runtime(0, probe_rows, match_rows);
        Ok(())
    }

    /// Evaluate residual candidates for SEMI/ANTI joins as pair batches.
    ///
    /// The generic pair loop used to invoke `BatchExprProgram` separately for
    /// every probe key bucket. A PK/FK anti join therefore built and evaluated
    /// one one-row Batch per probe row (TPC-H Q16 is 118k such allocations).
    /// SEMI/ANTI only need an existence bitmap, so accumulate bounded pair
    /// morsels, evaluate each morsel once, and mark the corresponding probe
    /// rows. This is the same residual kernel and produces the same result;
    /// only the call granularity changes.
    fn process_probe_batch_semi_anti_residual(&mut self, batch: Batch) -> Result<(), QueryError> {
        use crate::engine::executor::vec::bitmap::Bitmap;
        use crate::parser::JoinType;

        debug_assert!(matches!(self.join_type, JoinType::Semi | JoinType::Anti));
        let (program, _) = self
            .residual
            .as_ref()
            .expect("residual semi/anti path requires a residual");
        let program = Arc::clone(program);
        let schema = Arc::clone(&self.schema);
        let build_cols_first = self.build_is_left;
        let probe_keys = self.probe_keys.clone();
        let null_safe = self.null_safe.clone();
        let key_collations = self.key_collations.clone();
        let join_type = self.join_type;
        let state = self.state.as_mut().expect("general hash join initialized");
        let probe_rows = batch.visible_rows() as u64;
        let mut match_rows = 0u64;
        let mut matched_probe = Bitmap::new_all_invalid(batch.num_rows());
        let mut pair_build = Vec::with_capacity(DEFAULT_BATCH_ROWS);
        let mut pair_probe = Vec::with_capacity(DEFAULT_BATCH_ROWS);
        let mut key = Vec::with_capacity(64);
        let lookup = vec_hash_join::PayloadProbe::new(
            &state.payload,
            &batch,
            &probe_keys,
            &null_safe,
            &key_collations,
        );

        let flush = |pair_build: &mut Vec<u32>,
                     pair_probe: &mut Vec<u32>,
                     matched_probe: &mut Bitmap,
                     match_rows: &mut u64|
         -> Result<(), QueryError> {
            if pair_build.is_empty() {
                return Ok(());
            }
            let candidates = vec_hash_join::gather_pairs_batch(
                &state.payload.columns,
                &batch,
                pair_build,
                pair_probe,
                build_cols_first,
                &schema,
            );
            let accepted = program.eval_predicate_bitmap(&candidates)?;
            for (position, &probe_row) in pair_probe.iter().enumerate() {
                if accepted.get(position) {
                    matched_probe.set_true(probe_row as usize);
                    *match_rows = match_rows.saturating_add(1);
                }
            }
            pair_build.clear();
            pair_probe.clear();
            Ok(())
        };

        for probe_row in batch.visible_indices() {
            let (usable, hits) = lookup.lookup(probe_row, &mut key);
            if usable && hits.is_none() {
                analyze_stats::add_pending_runtime_filter_rejected(1);
            }
            if let Some(hits) = hits {
                for &build_row in hits {
                    pair_build.push(build_row);
                    pair_probe.push(probe_row as u32);
                    if pair_build.len() == DEFAULT_BATCH_ROWS {
                        flush(
                            &mut pair_build,
                            &mut pair_probe,
                            &mut matched_probe,
                            &mut match_rows,
                        )?;
                    }
                }
            }
        }
        flush(
            &mut pair_build,
            &mut pair_probe,
            &mut matched_probe,
            &mut match_rows,
        )?;
        let selected: Vec<u32> = batch
            .visible_indices()
            .filter(|&row| matched_probe.get(row) == (join_type == JoinType::Semi))
            .map(|row| row as u32)
            .collect();
        if !selected.is_empty() {
            state
                .pending
                .push_back(batch.with_selection(Selection::Indices(selected)))?;
        }
        analyze_stats::add_pending_join_runtime(0, probe_rows, match_rows);
        Ok(())
    }

    fn process_probe_batch(&mut self, batch: Batch) -> Result<(), QueryError> {
        use crate::parser::JoinType;
        self.state
            .as_mut()
            .expect("initialized hash join")
            .probe_started = true;
        if self.join_type == JoinType::Inner && self.residual.is_some() {
            return self.process_probe_batch_inner_residual(batch);
        }
        if matches!(self.join_type, JoinType::Semi | JoinType::Anti) && self.residual.is_some() {
            return self.process_probe_batch_semi_anti_residual(batch);
        }
        let state = self.state.as_mut().expect("general hash join initialized");
        let build_cols_first = self.build_is_left;
        let semi_anti = matches!(self.join_type, JoinType::Semi | JoinType::Anti);
        let preserve_probe = self.join_type == JoinType::Full
            || (self.join_type == JoinType::Left && !self.build_is_left)
            || (self.join_type == JoinType::Right && self.build_is_left);
        if let Some(kernel) = state.streaming_probe.as_mut() {
            return kernel.process_batch(
                &state.payload,
                batch,
                build_cols_first,
                preserve_probe,
                &self.schema,
                &mut state.pending,
            );
        }
        let mut key = Vec::with_capacity(64);
        let mut pair_build = Vec::with_capacity(DEFAULT_BATCH_ROWS);
        let mut pair_probe = Vec::with_capacity(DEFAULT_BATCH_ROWS);
        let mut selected = Vec::new();
        let probe_rows = batch.visible_rows() as u64;
        let mut match_rows = 0u64;
        let lookup = vec_hash_join::PayloadProbe::new(
            &state.payload,
            &batch,
            &self.probe_keys,
            &self.null_safe,
            &self.key_collations,
        );
        for probe_row in batch.visible_indices() {
            let (usable, hits) = lookup.lookup(probe_row, &mut key);
            if usable && hits.is_none() {
                analyze_stats::add_pending_runtime_filter_rejected(1);
            }
            let mut matched = false;
            if let Some(hits) = hits {
                for chunk in hits.chunks(DEFAULT_BATCH_ROWS) {
                    let accepted = if let Some((program, _)) = self.residual.as_ref() {
                        let probe_indices = vec![probe_row as u32; chunk.len()];
                        let candidates = vec_hash_join::gather_pairs_batch(
                            &state.payload.columns,
                            &batch,
                            chunk,
                            &probe_indices,
                            build_cols_first,
                            &self.schema,
                        );
                        Some(program.eval_predicate_bitmap(&candidates)?)
                    } else {
                        None
                    };
                    for (position, &build_row) in chunk.iter().enumerate() {
                        if accepted.as_ref().is_some_and(|mask| !mask.get(position)) {
                            continue;
                        }
                        matched = true;
                        match_rows = match_rows.saturating_add(1);
                        if let Some(bitmap) = state.matched_build.as_mut() {
                            bitmap[build_row as usize] = true;
                        }
                        if !semi_anti {
                            pair_build.push(build_row);
                            pair_probe.push(probe_row as u32);
                        }
                    }
                    if !semi_anti && pair_build.len() >= DEFAULT_BATCH_ROWS {
                        state.pending.push_back(vec_hash_join::gather_pairs_batch(
                            &state.payload.columns,
                            &batch,
                            &pair_build,
                            &pair_probe,
                            build_cols_first,
                            &self.schema,
                        ))?;
                        pair_build.clear();
                        pair_probe.clear();
                    }
                }
            }
            if semi_anti {
                if matched != (self.join_type == JoinType::Anti) {
                    selected.push(probe_row as u32);
                }
            } else if preserve_probe && !matched {
                pair_build.push(u32::MAX);
                pair_probe.push(probe_row as u32);
            }
        }
        if semi_anti {
            if !selected.is_empty() {
                state
                    .pending
                    .push_back(batch.with_selection(Selection::Indices(selected)))?;
            }
        } else if !pair_build.is_empty() {
            state.pending.push_back(vec_hash_join::gather_pairs_batch(
                &state.payload.columns,
                &batch,
                &pair_build,
                &pair_probe,
                build_cols_first,
                &self.schema,
            ))?;
        }
        analyze_stats::add_pending_join_runtime(0, probe_rows, match_rows);
        Ok(())
    }

    fn emit_unmatched_build(&mut self) -> Result<(), QueryError> {
        use crate::parser::JoinType;
        let state = self.state.as_mut().expect("general hash join initialized");
        let preserve_build = self.join_type == JoinType::Full
            || (self.join_type == JoinType::Left && self.build_is_left)
            || (self.join_type == JoinType::Right && !self.build_is_left);
        if !preserve_build || state.unmatched_build_emitted {
            return Ok(());
        }
        state.unmatched_build_emitted = true;
        let null_probe = Batch::with_row_count(
            self.probe_schema.clone(),
            self.probe_schema
                .fields()
                .iter()
                .map(|_| ColumnVector::Null { len: 0 })
                .collect(),
            0,
        );
        let mut pair_build = Vec::with_capacity(DEFAULT_BATCH_ROWS);
        let mut pair_probe = Vec::with_capacity(DEFAULT_BATCH_ROWS);
        for (build_row, matched) in state
            .matched_build
            .as_ref()
            .expect("full join match bitmap")
            .iter()
            .enumerate()
        {
            if *matched {
                continue;
            }
            pair_build.push(build_row as u32);
            pair_probe.push(u32::MAX);
            if pair_build.len() == DEFAULT_BATCH_ROWS {
                state.pending.push_back(vec_hash_join::gather_pairs_batch(
                    &state.payload.columns,
                    &null_probe,
                    &pair_build,
                    &pair_probe,
                    self.build_is_left,
                    &self.schema,
                ))?;
                pair_build.clear();
                pair_probe.clear();
            }
        }
        if !pair_build.is_empty() {
            state.pending.push_back(vec_hash_join::gather_pairs_batch(
                &state.payload.columns,
                &null_probe,
                &pair_build,
                &pair_probe,
                self.build_is_left,
                &self.schema,
            ))?;
        }
        Ok(())
    }
}

impl BatchOperator for HashJoinBatchOperator<'_> {
    fn prefer_key_locality(&mut self, columns: &[usize]) -> bool {
        if self.finished
            || self.state.is_some()
            || self.grace_output.is_some()
            || self.output.is_some()
            || self.empty_build_probe_passthrough
            || columns.len() != 1
            || self.build_keys.len() != 1
            || self.probe_keys.len() != 1
        {
            return false;
        }
        let semi = matches!(
            self.join_type,
            crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
        );
        let probe_offset = if self.build_is_left && !semi {
            self.build.schema().len()
        } else {
            0
        };
        let build_offset = if self.build_is_left {
            0
        } else {
            self.probe_schema.len()
        };
        let matches = columns[0] == probe_offset + self.probe_keys[0]
            || (!semi && columns[0] == build_offset + self.build_keys[0]);
        if matches {
            self.prefer_spill_key_locality = true;
        }
        matches
    }

    fn schema(&self) -> &Arc<BatchSchema> {
        if matches!(
            self.join_type,
            crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
        ) {
            &self.probe_schema
        } else {
            &self.schema
        }
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        if self.probe.install_runtime_filter(filter.clone()) {
            true
        } else {
            self.build.install_runtime_filter(filter)
        }
    }

    fn consume_into_hash_aggregate(
        &mut self,
        aggregate: &mut vec_hash_agg::HashAggregateCore,
    ) -> Result<bool, QueryError> {
        self.consume_matches_into_hash_aggregate(aggregate, None)
    }

    fn consume_computed_into_hash_aggregate(
        &mut self,
        aggregate: &mut vec_hash_agg::HashAggregateCore,
        expressions: &[Expr],
    ) -> Result<bool, QueryError> {
        self.consume_matches_into_hash_aggregate(aggregate, Some(expressions))
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.finished {
            return Ok(None);
        }
        crate::engine::cancel::check_cancelled()?;
        self.initialize()?;
        if self.finished {
            return Ok(None);
        }
        if self.empty_build_probe_passthrough {
            let next = self.probe.next_batch()?;
            if next.is_none() {
                self.empty_build_probe_passthrough = false;
                self.finished = true;
            }
            return Ok(next);
        }
        self.report_pushed_runtime_rejections();
        if let Some(output) = self.grace_output.as_mut() {
            let next = output.next_batch()?;
            if next.is_none() {
                self.grace_output = None;
                self.finished = true;
            }
            return Ok(next);
        }
        if let Some(output) = self.output.as_mut() {
            let next = output.next();
            if next.is_none() {
                self.output = None;
                self.work_area = None;
                self.build_memory_charge = None;
                self.finished = true;
            }
            return Ok(next);
        }
        loop {
            if let Some(batch) = self
                .state
                .as_mut()
                .expect("general hash join state")
                .pending
                .pop_front()?
            {
                return Ok(Some(batch));
            }
            {
                let state = self.state.as_mut().expect("general hash join state");
                let preserve_probe = self.join_type == crate::parser::JoinType::Full
                    || (self.join_type == crate::parser::JoinType::Left && !self.build_is_left)
                    || (self.join_type == crate::parser::JoinType::Right && self.build_is_left);
                if let Some(kernel) = state.streaming_probe.as_mut() {
                    if kernel.resume_batch(
                        &state.payload,
                        self.build_is_left,
                        preserve_probe,
                        &self.schema,
                        &mut state.pending,
                    )? {
                        continue;
                    }
                }
            }
            if self
                .state
                .as_ref()
                .expect("general hash join state")
                .probe_done
            {
                self.emit_unmatched_build()?;
                if let Some(batch) = self
                    .state
                    .as_mut()
                    .expect("general hash join state")
                    .pending
                    .pop_front()?
                {
                    return Ok(Some(batch));
                }
                // No output can reference the build payload after this point.
                // Drop both the payload and its WorkArea handle before EOF is
                // observed by a blocking parent. This prevents an exhausted
                // child from transiently shrinking the parent's fair share at
                // the exact final-batch boundary.
                self.finish_input();
                return Ok(None);
            }
            let next = self.probe.next_batch()?;
            self.report_pushed_runtime_rejections();
            match next {
                Some(batch) => self.process_probe_batch(batch)?,
                None => {
                    self.state
                        .as_mut()
                        .expect("general hash join state")
                        .probe_done = true;
                }
            }
        }
    }
}

/// Append non-trivial hash-key expressions to an input batch.  Plain column
/// keys remain zero-copy.  The returned template includes the private key
/// columns so the join can derive collation and types; callers must project
/// them away at the join boundary.
fn append_join_key_columns<'a>(
    input: VecBatchInput<'a>,
    keys: &[Expr],
    table: &str,
) -> Result<Option<(Box<dyn BatchOperator + 'a>, Vec<PlanField>, Vec<usize>)>, QueryError> {
    let VecBatchInput {
        stream,
        mut template,
    } = input;
    let schema = stream.schema().clone();
    let base_len = template.len();
    let mut programs = Vec::new();
    let mut fields = schema.fields().to_vec();
    let mut key_indices = Vec::with_capacity(keys.len());
    for (slot, key) in keys.iter().enumerate() {
        if let Some(index) = expr_column_index_in_template(key, &template[..base_len]) {
            key_indices.push(index);
            continue;
        }
        let Some(program) = expr_batch::compile_for_batch(key, &schema)? else {
            return Ok(None);
        };
        let Some(field) = computed_batch_field(&program, table, format!("k{slot}")) else {
            return Ok(None);
        };
        key_indices.push(base_len + programs.len());
        template.push(batch_field_template(&field));
        fields.push(field);
        programs.push(program);
    }
    if programs.is_empty() {
        return Ok(Some((stream, template, key_indices)));
    }
    let row_major = programs.len() > 1
        && programs
            .iter()
            .any(|program| program.capability().requires_ordered_evaluation());
    let schema = Arc::new(BatchSchema::new(fields));
    Ok(Some((
        Box::new(ComputeColumnsBatchStream {
            pending_chunks: None,
            inner: stream,
            numeric_programs: vec![None; programs.len()],
            direct_expressions: Vec::new(),
            programs,
            schema,
            row_major,
        }),
        template,
        key_indices,
    )))
}

/// Remove internal expression-key columns and compose the planner's join
/// projection in one gather. `visible_indices[original_column]` addresses the
/// extended pair-batch layout produced by the hash kernel.
fn project_internal_join_columns<'a>(
    upstream: Box<dyn BatchOperator + 'a>,
    internal_schema: Arc<BatchSchema>,
    original_template: &[PlanField],
    visible_indices: &[usize],
    output_projection: Option<&[usize]>,
) -> (Box<dyn BatchOperator + 'a>, Vec<PlanField>) {
    let columns: Vec<usize> = match output_projection {
        Some(projection) => projection
            .iter()
            .filter_map(|&index| visible_indices.get(index).copied())
            .collect(),
        None => visible_indices.to_vec(),
    };
    let template: Vec<PlanField> = match output_projection {
        Some(projection) => projection
            .iter()
            .filter_map(|&index| original_template.get(index).cloned())
            .collect(),
        None => original_template.to_vec(),
    };
    let identity = columns.len() == internal_schema.len()
        && columns
            .iter()
            .enumerate()
            .all(|(index, column)| index == *column);
    if identity {
        return (upstream, template);
    }
    let fields = columns
        .iter()
        .filter_map(|&index| internal_schema.fields().get(index).cloned())
        .collect();
    (
        Box::new(ProjectColumnsBatchStream {
            upstream,
            columns,
            schema: Arc::new(BatchSchema::new(fields)),
        }),
        template,
    )
}

/// Resident columnar hash join state. The build is blocking; probe/output are
/// pulled one batch at a time. Only Grace overflow owns a materialized output
/// iterator because both sides must first be partitioned on disk.
struct HashJoinState {
    payload: vec_hash_join::BuildPayload,
    streaming_probe: Option<vec_hash_join::StreamingProbe>,
    matched_build: Option<Vec<bool>>,
    pending: crate::engine::executor::vec::join_output::JoinOutputQueue,
    probe_started: bool,
    probe_done: bool,
    unmatched_build_emitted: bool,
}

/// Replay an already retained prefix, then continue the unopened remainder.
/// Reverse existence joins use this when incremental admission requests Grace.
struct RetainedProbeStream<'a> {
    prefix: std::vec::IntoIter<Batch>,
    remainder: Box<dyn BatchOperator + 'a>,
    memory_charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
}

impl BatchOperator for RetainedProbeStream<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        self.remainder.schema()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        match self.prefix.next() {
            Some(batch) => Ok(Some(batch)),
            None => {
                // The prefix owns this reservation, including empty-build
                // ANTI passthrough. Release it once its batches are consumed.
                self.memory_charge = None;
                self.remainder.next_batch()
            }
        }
    }
}

/// Lower every supported equi-join to the same batch operator. Key expressions
/// and residuals are prepared before execution; build overflow continues through
/// Grace using the consumed prefix and remaining inputs, without rescanning.
pub(super) fn try_build_vec_hash_join_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    use crate::parser::JoinType;

    if matches!(
        crate::util::env::var("MYDB_VEC_HASH_JOIN")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no"
    ) {
        return Ok(None);
    }

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
        return Ok(None);
    };
    // CTE-only treatment normalizes only the probe-preserving build side; it
    // never inspects materialized row counts to choose another executor.
    let cte_only = cte_join_enabled() && plan_is_cte_only(plan);
    let build_side = match join_type {
        // SEMI/ANTI only publish the SQL left side, so it remains the probe.
        // Outer joins support either CBO-selected build side: when the
        // preserved side is built the general kernel retains a match bitmap
        // and emits its NULL-extended rows after probe completion.
        JoinType::Semi | JoinType::Anti => BuildSide::Right,
        _ if cte_only => cte_vec_build_side(*join_type, *build_side),
        _ => *build_side,
    };
    // Join routing is a physical-plan decision, not a query-shape switch.
    // Explicit vector mode and CBO auto admission both select this same
    // BatchOperator; runtime memory pressure only changes build/spill layout.
    if !vec_hash_join_streaming_enabled(plan)
        || !matches!(
            join_type,
            JoinType::Inner
                | JoinType::Left
                | JoinType::Right
                | JoinType::Semi
                | JoinType::Anti
                | JoinType::Full
        )
        || left_keys.len() != right_keys.len()
        || left_keys.is_empty()
    {
        return Ok(None);
    }
    let (build_plan, build_keys_arr, probe_plan, probe_keys_arr, build_is_left) = match build_side {
        BuildSide::Right => (right.as_ref(), right_keys, left.as_ref(), left_keys, false),
        BuildSide::Left => (left.as_ref(), left_keys, right.as_ref(), right_keys, true),
    };

    let Some(build_input) = try_native_vec_input_stream(db, txn, build_plan)? else {
        return Ok(None);
    };
    let Some(probe_input) = try_native_vec_input_stream(db, txn, probe_plan)? else {
        return Ok(None);
    };

    if build_input.template.is_empty() || probe_input.template.is_empty() {
        let template = if build_is_left {
            concat_templates(&build_input.template, &probe_input.template)
        } else {
            concat_templates(&probe_input.template, &build_input.template)
        };
        return Ok(Some(VecBatchInput {
            stream: Box::new(MaterializedBatchOperator::new(
                Arc::new(BatchSchema::new(Vec::new())),
                Vec::new(),
            )),
            template,
        }));
    }

    let build_original_template = build_input.template.clone();
    let probe_original_template = probe_input.template.clone();
    let Some((build_stream, build_template, build_key_idx)) =
        append_join_key_columns(build_input, build_keys_arr, "__build_key")?
    else {
        return Ok(None);
    };
    let Some((probe_stream, probe_template, probe_key_idx)) =
        append_join_key_columns(probe_input, probe_keys_arr, "__probe_key")?
    else {
        return Ok(None);
    };
    let key_collations = join_key_collations(
        &build_template,
        &probe_template,
        &build_key_idx,
        &probe_key_idx,
    )?;

    let full_output_template = if build_is_left {
        concat_templates(&build_template, &probe_template)
    } else {
        concat_templates(&probe_template, &build_template)
    };
    let original_output_template = if build_is_left {
        concat_templates(&build_original_template, &probe_original_template)
    } else {
        concat_templates(&probe_original_template, &build_original_template)
    };
    // Expression keys are internal columns appended to each input.  The join
    // kernel may read them, but they are never part of SQL-visible output.
    // This mapping converts original join-column positions to the extended
    // pair-batch layout and composes naturally with planner output pruning.
    let original_output_indices: Vec<usize> = if build_is_left {
        (0..build_original_template.len())
            .chain((build_template.len())..(build_template.len() + probe_original_template.len()))
            .collect()
    } else {
        (0..probe_original_template.len())
            .chain((probe_template.len())..(probe_template.len() + build_original_template.len()))
            .collect()
    };
    let out_schema = template_to_batch_schema(&full_output_template);
    let mut scalar_residual = None;
    let residual_program = match residual {
        None => None,
        Some(predicate) => {
            match expr_batch::BatchExprProgram::compile(predicate, &out_schema)? {
                Some(program) => Some((program, out_schema.clone())),
                None if *join_type == JoinType::Inner => {
                    // INNER residuals may filter the joined stream after key
                    // matching. Keep arbitrary scalar/subquery predicates in
                    // the bounded pipeline instead of materializing all join
                    // rows merely because there is no typed predicate kernel.
                    scalar_residual = Some(predicate);
                    None
                }
                None => return Ok(None),
            }
        }
    };
    let preserved_build = (*join_type == JoinType::Left && build_is_left)
        || (*join_type == JoinType::Right && !build_is_left);
    let input_contains_inlj = build_plan
        .any(|node| matches!(node, PhysicalPlan::IndexNestedLoopJoin { .. }))
        || probe_plan.any(|node| matches!(node, PhysicalPlan::IndexNestedLoopJoin { .. }));
    let defer_build = scalar_residual.is_some()
        || residual_program.is_some()
        || null_safe.iter().any(|value| *value)
        || preserved_build
        || input_contains_inlj
        || matches!(join_type, JoinType::Semi | JoinType::Anti | JoinType::Full);
    let probe_schema = probe_stream.schema().clone();
    let (base_template, base_schema, visible_indices) =
        if matches!(join_type, JoinType::Semi | JoinType::Anti) {
            (
                probe_original_template.clone(),
                probe_schema.clone(),
                (0..probe_original_template.len()).collect::<Vec<_>>(),
            )
        } else {
            (
                original_output_template.clone(),
                out_schema.clone(),
                original_output_indices.clone(),
            )
        };
    let mut operator = HashJoinBatchOperator {
        build_plan: Some(build_plan),
        pushed_runtime_rejected: None,
        prefer_spill_key_locality: false,
        grace_output: None,
        db: Arc::clone(db),
        txn,
        build: build_stream,
        probe: probe_stream,
        build_keys: build_key_idx,
        probe_keys: probe_key_idx,
        probe_key_exprs: probe_keys_arr.clone(),
        key_collations,
        null_safe: null_safe.clone(),
        join_type: *join_type,
        build_is_left,
        schema: out_schema.clone(),
        probe_schema,
        residual: residual_program,
        state: None,
        output: None,
        work_area: None,
        build_memory_charge: None,
        finished: false,
        empty_build_probe_passthrough: false,
        reverse_semi_anti: crate::planner::reverse_semi_anti_hash_join_eligible(
            *join_type,
            residual.is_some(),
            crate::planner::estimated_rows(probe_plan),
            crate::planner::estimated_rows(build_plan),
        ),
        pushed_exact_runtime_rejected: None,
        pushed_runtime_rejected_reported: 0,
    };
    // Preserve build-before-parent-open behavior of ordinary equality joins.
    if !defer_build {
        operator.initialize()?;
    }
    let operator: Box<dyn BatchOperator + 'a> = Box::new(operator);
    let operator: Box<dyn BatchOperator + 'a> = if let Some(predicate) = scalar_residual {
        let properties = operator.properties();
        Box::new(ScalarFilterBatchStream {
            db,
            txn,
            upstream: operator,
            predicate: Cow::Borrowed(predicate),
            template: full_output_template,
            schema: out_schema,
            properties,
            eval_ctx: crate::engine::executor::select::eval::EvalQueryContext::default(),
            analyze_plan: Some(plan),
            scalar_rows: 0,
            analyze_recorded: false,
        })
    } else {
        operator
    };
    let (stream, template) = project_internal_join_columns(
        operator,
        base_schema,
        &base_template,
        &visible_indices,
        output_projection.as_deref().map(Vec::as_slice),
    );
    VEC_HASH_JOIN_STREAM_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(Some(VecBatchInput { stream, template }))
}

/// Block/vector nested-loop implementation expressed through the common pair
/// batch kernel. A private constant key forms one candidate bucket; the ON
/// condition is evaluated as the residual over bounded pair batches. This
/// keeps CROSS/NLJ in the BatchOperator graph while retaining the CBO's NLJ
/// plan node and its observable semantics.
pub(super) fn try_build_vec_nested_loop_join_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    use crate::parser::JoinType;

    let PhysicalPlan::NestedLoopJoin {
        left,
        right,
        join_type,
        condition,
        ..
    } = plan
    else {
        return Ok(None);
    };
    if !matches!(
        join_type,
        JoinType::Inner
            | JoinType::Cross
            | JoinType::Left
            | JoinType::Right
            | JoinType::Full
            | JoinType::Semi
            | JoinType::Anti
    ) {
        return Ok(None);
    }

    // Preserve the SQL outer side as probe whenever possible. FULL keeps a
    // build match bitmap and therefore permits either side.
    let build_is_left = *join_type == JoinType::Right;
    let (build_plan, probe_plan) = if build_is_left {
        (left.as_ref(), right.as_ref())
    } else {
        (right.as_ref(), left.as_ref())
    };
    let Some(build_input) = try_native_vec_input_stream(db, txn, build_plan)? else {
        return Ok(None);
    };
    let Some(probe_input) = try_native_vec_input_stream(db, txn, probe_plan)? else {
        return Ok(None);
    };
    let build_original = build_input.template.clone();
    let probe_original = probe_input.template.clone();
    let constant_key = [Expr::Literal(crate::parser::Literal::Int(1))];
    let Some((build_stream, build_template, build_keys)) =
        append_join_key_columns(build_input, &constant_key, "__nlj_build")?
    else {
        return Ok(None);
    };
    let Some((probe_stream, probe_template, probe_keys)) =
        append_join_key_columns(probe_input, &constant_key, "__nlj_probe")?
    else {
        return Ok(None);
    };

    let extended_output_template = if build_is_left {
        concat_templates(&build_template, &probe_template)
    } else {
        concat_templates(&probe_template, &build_template)
    };
    let original_output_template = if build_is_left {
        concat_templates(&build_original, &probe_original)
    } else {
        concat_templates(&probe_original, &build_original)
    };
    let visible_indices: Vec<usize> = if build_is_left {
        (0..build_original.len())
            .chain(build_template.len()..build_template.len() + probe_original.len())
            .collect()
    } else {
        (0..probe_original.len())
            .chain(probe_template.len()..probe_template.len() + build_original.len())
            .collect()
    };
    let pair_schema = template_to_batch_schema(&extended_output_template);
    let residual = match condition {
        Some(condition) => {
            let Some(program) = expr_batch::BatchExprProgram::compile(condition, &pair_schema)?
            else {
                return Ok(None);
            };
            Some((program, pair_schema.clone()))
        }
        None => None,
    };
    let logical_join_type = if *join_type == JoinType::Cross {
        JoinType::Inner
    } else {
        *join_type
    };
    let probe_schema = probe_stream.schema().clone();
    let base_schema = if matches!(logical_join_type, JoinType::Semi | JoinType::Anti) {
        probe_schema.clone()
    } else {
        pair_schema.clone()
    };
    let base_template = if matches!(logical_join_type, JoinType::Semi | JoinType::Anti) {
        probe_original.clone()
    } else {
        original_output_template.clone()
    };
    let base_visible = if matches!(logical_join_type, JoinType::Semi | JoinType::Anti) {
        (0..probe_original.len()).collect::<Vec<_>>()
    } else {
        visible_indices
    };
    let operator: Box<dyn BatchOperator + 'a> = Box::new(HashJoinBatchOperator {
        build_plan: None,
        pushed_runtime_rejected: None,
        prefer_spill_key_locality: false,
        grace_output: None,
        db: Arc::clone(db),
        txn,
        build: build_stream,
        probe: probe_stream,
        build_keys,
        probe_keys,
        probe_key_exprs: Vec::new(),
        key_collations: vec![None],
        // Both synthetic keys are non-NULL. Ordinary equality also enables
        // bounded streaming probes, so LIMIT can stop a Cartesian product.
        null_safe: vec![false],
        join_type: logical_join_type,
        build_is_left,
        schema: pair_schema,
        probe_schema,
        residual,
        state: None,
        output: None,
        work_area: None,
        build_memory_charge: None,
        finished: false,
        empty_build_probe_passthrough: false,
        reverse_semi_anti: false,
        pushed_exact_runtime_rejected: None,
        pushed_runtime_rejected_reported: 0,
    });
    let (stream, template) =
        project_internal_join_columns(operator, base_schema, &base_template, &base_visible, None);
    Ok(Some(VecBatchInput { stream, template }))
}
