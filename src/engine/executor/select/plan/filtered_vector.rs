//! Filter scalar columns before touching vectors; materialize only ranked rows.
use super::*;
use crate::engine::vector::hnsw_core::Scored;
use crate::engine::vector::workspace::VectorQueryCharge;
use std::collections::BinaryHeap;

mod fused;

struct FilteredPredicate {
    columns: Vec<usize>,
    compiled: Option<CompiledPredicate>,
    eval_ctx: EvalQueryContext,
}
impl FilteredPredicate {
    fn new(table: &PlanTable, filter: &Option<Expr>) -> Result<Self, QueryError> {
        let columns = predicate_column_indices(filter, &table.schema)?;
        let schema = projected_schema_view(&table.schema, &columns);
        let compiled = filter
            .as_ref()
            .and_then(|expr| CompiledPredicate::compile(expr, &schema));
        Ok(Self {
            columns,
            compiled,
            eval_ctx: EvalQueryContext::default(),
        })
    }
}

#[cfg(test)]
thread_local! {
    pub(super) static AFTER_SNAPSHOT_CHECK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}

pub(super) fn execute_filtered_vector_topk(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::planner::FilteredVectorStrategy;
    let PhysicalPlan::FilteredVectorTopK {
        table,
        column_index,
        scalar_access,
        query_vector,
        query_vector_expr,
        metric,
        k,
        baked_offset,
        binding,
        strategy,
        index,
        projected_columns,
        filter,
        ef_hint,
        parallel_degree,
        exact_cost,
        predicate_cost,
        ..
    } = plan
    else {
        unreachable!()
    };
    let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
    let (k, _) = binding.resolve_k_offset(*k, *baked_offset);
    let query = resolve_knn_query_vector(table, *column_index, query_vector, query_vector_expr)?;
    let metric = crate::engine::vector::Metric::parse(metric).unwrap_or_default();
    validate_knn_query_vector(&query, metric)?;
    if k == 0 {
        return Ok(Vec::new());
    }
    let parallel_degree = (*parallel_degree)
        .min(crate::engine::executor::tuning::current_or_capture().scheduler_parallelism)
        .max(1);
    let mut stats = super::super::analyze_stats::FilteredVectorStats::default();
    let mut charge = VectorQueryCharge::try_new(0)?;
    let mut best: BinaryHeap<Scored<(u64, RowId)>> = BinaryHeap::new();
    let mut selection = None;
    let heap_required = db.vector_snapshot_requires_exact(txn)
        || db.xa_status_counters().0 > 0
        || db.scan_storage_ids(&table.schema).iter().any(|sid| {
            !db.pending_vector_upsert_row_ids(txn.txn_id, table.schema.id, *sid)
                .is_empty()
        });
    #[cfg(test)]
    AFTER_SNAPSHOT_CHECK.with(|hook| {
        let callback = hook.borrow_mut().take();
        if let Some(callback) = callback {
            callback();
        }
    });
    // A current scalar index need not enumerate old scalar keys either.
    // Historical/XA/private-vector completion uses the same heap oracle as
    // the existing ANN executor, with narrow predicate decoding retained.
    let heap_access = super::scan::ordered_vector_fallback_input(table, filter, None);
    let mut scalar_access = if heap_required {
        &heap_access
    } else {
        scalar_access.as_ref()
    };
    let mut force_exact = *strategy == FilteredVectorStrategy::Exact || heap_required;
    let mut predicate = FilteredPredicate::new(table, filter)?;
    let fused = !heap_required
        && *strategy == FilteredVectorStrategy::Exact
        && fused::try_score(
            db,
            txn,
            plan,
            table,
            scalar_access,
            filter,
            &predicate,
            *column_index,
            &query,
            metric,
            k,
            parallel_degree,
            &mut best,
            &mut charge,
            &mut stats,
        )?;
    if !fused
        && ((!force_exact && *strategy == FilteredVectorStrategy::PrefilterAnn)
            || (*strategy == FilteredVectorStrategy::Exact && parallel_degree > 1))
    {
        let attempt = (|| {
            let mut rows = crate::engine::vector::selection::RowSelection::new()?;
            visit_filtered_row_ids(
                db,
                txn,
                table,
                scalar_access,
                filter,
                &mut predicate,
                &mut stats,
                |sid, ids| rows.append(sid, ids),
            )?;
            rows.seal()?;
            Ok::<_, QueryError>(Arc::new(rows))
        })();
        match attempt {
            Ok(rows) => selection = Some(rows),
            Err(err) if err.is(crate::error::catalog::ER_OUTOFMEMORY) => {
                // Optional materialization must not turn a bounded streaming
                // exact query into an out-of-memory failure.
                force_exact = true;
                scalar_access = &heap_access;
                stats.fallbacks = 1;
            }
            Err(err) => return Err(err),
        }
    }
    let mut exact = force_exact;
    if !exact {
        let index = index.as_ref().expect("ANN strategy requires an index");
        let cfg = crate::engine::vector::session::current_config();
        let is_ivf = index.vector_algo() == crate::storage::schema::VectorAlgo::Ivf;
        let effort = if is_ivf {
            crate::engine::vector::session::effective_nprobe(
                &cfg,
                *ef_hint,
                index.vector_meta().default_nprobe.map(u64::from),
            )
        } else {
            crate::engine::vector::session::effective_ef_search(
                &cfg,
                *ef_hint,
                index.vector_meta().default_ef_search.map(u64::from),
                k,
            )
        };
        // Scalar work is already paid. Reprice the remaining heap, index,
        // distance and reranking work using the actual survivor count.
        if let Some(rows) = &selection {
            let (exact_work, ann_work) = crate::planner::filtered_vector_remaining_cost(
                db,
                table,
                index,
                query.dimension,
                rows.len(),
                k,
                effort,
            )?;
            exact = rows.len() == 0
                || (ef_hint.is_none() && cfg.brute_threshold != 0 && exact_work <= ann_work);
        }
        if !exact {
            exact = !search_filtered_ann(
                db,
                txn,
                table,
                index,
                *column_index,
                &query,
                metric,
                k,
                effort,
                filter,
                &mut predicate,
                *exact_cost,
                *predicate_cost,
                selection.clone(),
                &mut best,
                &mut charge,
                &mut stats,
            )
            .or_else(|err| {
                if err.code() == crate::error::catalog::ER_OUTOFMEMORY.code() {
                    Ok(false)
                } else {
                    Err(err)
                }
            })?;
        }
    }
    if exact && !fused {
        if *strategy != FilteredVectorStrategy::Exact {
            stats.fallbacks = stats.fallbacks.max(1);
        }
        best.clear();
        let mut score = |sid, ids: &[RowId]| {
            score_filtered_ids(
                db,
                txn,
                table,
                sid,
                ids,
                *column_index,
                &query,
                metric,
                k,
                &mut best,
                &mut charge,
            )
        };
        if let Some(rows) = selection.as_ref() {
            if parallel_degree > 1 && rows.len() > k {
                super::super::analyze_stats::add_vector_runtime(
                    plan,
                    0,
                    0,
                    (parallel_degree).min(rows.len().div_ceil(256)).max(1) as u32,
                );
                stats.reranks += rows.len() as u64;
                best = score_selection_parallel(
                    db,
                    txn,
                    table,
                    rows,
                    *column_index,
                    &query,
                    metric,
                    k,
                    parallel_degree,
                    &mut charge,
                )?;
            } else {
                rows.visit(|sid, ids| {
                    stats.reranks += ids.len() as u64;
                    score(sid, ids).map(|_| ())
                })?;
            }
        } else {
            // Source counters are collected separately to avoid aliasing the
            // scoring closure with the predicate evaluator's statistics.
            let mut source_stats = super::super::analyze_stats::FilteredVectorStats::default();
            let attempt = visit_filtered_row_ids(
                db,
                txn,
                table,
                scalar_access,
                filter,
                &mut predicate,
                &mut source_stats,
                |sid, ids| {
                    stats.reranks += ids.len() as u64;
                    score(sid, ids).map(|_| ())
                },
            );
            match attempt {
                Err(err)
                    if err.is(crate::error::catalog::ER_OUTOFMEMORY)
                        && !std::ptr::eq(scalar_access, &heap_access) =>
                {
                    best.clear();
                    stats.fallbacks = stats.fallbacks.max(1);
                    visit_filtered_row_ids(
                        db,
                        txn,
                        table,
                        &heap_access,
                        filter,
                        &mut predicate,
                        &mut source_stats,
                        |sid, ids| {
                            stats.reranks += ids.len() as u64;
                            score_filtered_ids(
                                db,
                                txn,
                                table,
                                sid,
                                ids,
                                *column_index,
                                &query,
                                metric,
                                k,
                                &mut best,
                                &mut charge,
                            )
                            .map(|_| ())
                        },
                    )?;
                }
                other => other?,
            }
            stats.predicate_evaluations += source_stats.predicate_evaluations;
        }
    }
    // A commit or cold rebuild can advance the index coverage floor after
    // the initial snapshot check. Its new ordering cannot rank an older
    // visible vector version, even if the cursor opened the new graph cleanly.
    if !heap_required && (db.vector_snapshot_requires_exact(txn) || db.xa_status_counters().0 > 0) {
        drop(selection.take());
        best.clear();
        stats.fallbacks = stats.fallbacks.max(1);
        let mut fallback_stats = super::super::analyze_stats::FilteredVectorStats::default();
        visit_filtered_row_ids(
            db,
            txn,
            table,
            &heap_access,
            filter,
            &mut predicate,
            &mut fallback_stats,
            |sid, ids| {
                stats.reranks += ids.len() as u64;
                score_filtered_ids(
                    db,
                    txn,
                    table,
                    sid,
                    ids,
                    *column_index,
                    &query,
                    metric,
                    k,
                    &mut best,
                    &mut charge,
                )
                .map(|_| ())
            },
        )?;
        stats.predicate_evaluations += fallback_stats.predicate_evaluations;
    }
    let output_columns = projected_columns
        .as_deref()
        .cloned()
        .unwrap_or_else(|| logical_visible_column_indices(&table.schema));
    let winners = best.into_sorted_vec();
    let mut out = Vec::new();
    let mut values = crate::util::fast_hash::AhHashMap::default();
    for sid in db.scan_storage_ids(&table.schema) {
        let ids: Vec<_> = winners
            .iter()
            .filter_map(|s| (s.id.0 == sid).then_some(s.id.1))
            .collect();
        if ids.is_empty() {
            continue;
        }
        let heap = db.cached_heap_handle(sid)?;
        for (row, v) in
            db.read_row_columns_many_with_handle(&heap, &table.schema, &ids, txn, &output_columns)?
        {
            charge.try_grow(crate::engine::executor::mem_budget::estimate_row_bytes(&v) + 128)?;
            values.insert((sid, row), v);
        }
    }
    for winner in winners {
        if let Some(values) = values.remove(&winner.id) {
            out.push(projected_values_to_plan_row(
                table,
                &output_columns,
                values,
            )?);
        }
    }
    stats.materialized += out.len() as u64;
    super::super::analyze_stats::add_vector_runtime(plan, 0, 0, 1);
    super::super::analyze_stats::add_filtered_vector_stats(plan, stats);
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn score_filtered_ids(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    sid: u64,
    ids: &[RowId],
    column: usize,
    query: &crate::vector::VectorValue,
    metric: crate::engine::vector::Metric,
    k: usize,
    best: &mut BinaryHeap<Scored<(u64, RowId)>>,
    charge: &mut VectorQueryCharge,
) -> Result<usize, QueryError> {
    let heap = db.cached_heap_handle(sid)?;
    let scores = db.score_vector_rows_many_with_handle(
        &heap,
        &table.schema,
        ids,
        txn,
        column,
        query,
        metric,
        None,
    )?;
    let count = scores.len();
    for (row, distance, _) in scores {
        let item = Scored {
            dist: distance,
            id: (sid, row),
        };
        if best.len() < k {
            if best.len() == best.capacity() {
                let extra = (k - best.len()).min(best.capacity().max(8));
                charge.try_grow(extra.saturating_mul(std::mem::size_of_val(&item)))?;
                best.try_reserve_exact(extra).map_err(|_| {
                    error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "filtered vector Top-K allocation failed",
                    )
                })?;
            }
            best.push(item);
        } else if best.peek().is_some_and(|worst| item < *worst) {
            *best.peek_mut().unwrap() = item;
        }
    }
    Ok(count)
}

#[allow(clippy::too_many_arguments)]
fn search_filtered_ann(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    column: usize,
    query: &crate::vector::VectorValue,
    metric: crate::engine::vector::Metric,
    k: usize,
    base_effort: usize,
    filter: &Option<Expr>,
    predicate: &mut FilteredPredicate,
    exact_cost: f64,
    predicate_cost: f64,
    selection: Option<Arc<crate::engine::vector::selection::RowSelection>>,
    best: &mut BinaryHeap<Scored<(u64, RowId)>>,
    charge: &mut VectorQueryCharge,
    stats: &mut super::super::analyze_stats::FilteredVectorStats,
) -> Result<bool, QueryError> {
    use crate::engine::vector::session::{current_config, effective_rerank_mult, IterativeScan};
    let cfg = current_config();
    let is_ivf = index.vector_algo() == crate::storage::schema::VectorAlgo::Ivf;
    let mode = if is_ivf {
        cfg.ivf_iterative_scan
    } else {
        cfg.iterative_scan
    };
    let max_scan = if is_ivf {
        cfg.ivf_max_scan_tuples
    } else {
        cfg.max_scan_tuples
    };
    let max_scan = if max_scan == 0 {
        usize::MAX
    } else {
        (max_scan as usize).max(k)
    };
    let target = k
        .saturating_mul(effective_rerank_mult(&cfg, index) as usize)
        .max(32)
        .max(k);
    let mut seen = std::collections::HashSet::new();
    let mut seen_charge = VectorQueryCharge::try_new_recoverable(0)?;
    let predicate_columns = if selection.is_none() {
        predicate.columns.as_slice()
    } else {
        &[]
    };
    let compiled = &predicate.compiled;
    let eval_ctx = &mut predicate.eval_ctx;
    let mut all_complete = true;
    for sid in db.scan_storage_ids(&table.schema) {
        let target = selection
            .as_ref()
            .map_or(target, |rows| target.min(rows.partition_len(sid)));
        if target == 0 {
            continue;
        }
        let mut cursor =
            db.vector_index_open_cursor(&table.schema, index, sid, query, selection.clone())?;
        cursor.max_navigation = max_scan;
        let mut effort = base_effort;
        let mut accepted = 0;
        loop {
            crate::engine::cancel::check_cancelled()?;
            let remaining = max_scan.saturating_sub(seen.len());
            if remaining == 0 {
                all_complete = false;
                break;
            }
            let Some(batch) = db.vector_index_cursor_next(
                &table.schema,
                index,
                &mut cursor,
                effort.min(max_scan),
                (if mode == IterativeScan::Off {
                    target
                } else {
                    target.min(256)
                })
                .max(1)
                .min(remaining),
            )?
            else {
                // Invalidation or a backend's request for heap completion is
                // a safety fallback even when iterative widening is disabled.
                stats.navigation_visits += cursor.work.visited;
                stats.navigation_distances += cursor.work.distances;
                return Ok(false);
            };
            stats.batches += 1;
            let mut ids = Vec::with_capacity(batch.len());
            for (id, _) in batch {
                if !seen.contains(&(sid, id)) {
                    seen_charge.try_grow_recoverable(64)?;
                    seen.insert((sid, id));
                    ids.push(id);
                }
            }
            let ids = if selection.is_some() {
                ids
            } else {
                let heap = db.cached_heap_handle(sid)?;
                let rows = db.read_row_columns_many_with_handle(
                    &heap,
                    &table.schema,
                    &ids,
                    txn,
                    &predicate_columns,
                )?;
                let _batch_charge = charge_predicate_rows(&rows)?;
                let mut kept = Vec::with_capacity(rows.len());
                for (id, values) in rows {
                    stats.predicate_evaluations += 1;
                    let keep = match (filter, compiled.as_ref()) {
                        (None, _) => true,
                        (_, Some(p)) => p.eval(&values)?,
                        (Some(expr), None) => {
                            let row =
                                projected_values_to_plan_row(table, &predicate_columns, values)?;
                            eval_plan_bool_with_db(db, txn, expr, &row, eval_ctx)?
                        }
                    };
                    if keep {
                        kept.push(id);
                    }
                }
                kept
            };
            stats.reranks += ids.len() as u64;
            accepted += score_filtered_ids(
                db, txn, table, sid, &ids, column, query, metric, k, best, charge,
            )?;
            if cursor.navigation_incomplete {
                all_complete = false;
                break;
            }
            if accepted >= target {
                break;
            }
            if mode == IterativeScan::Off {
                break;
            }
            if cursor.exhausted() {
                // Graph/list exhaustion cannot establish that every visible
                // relational row was considered or replace the rerank target.
                all_complete = false;
                break;
            }
            if cursor.hnsw.pending.is_empty() {
                if effort >= max_scan {
                    all_complete = false;
                    break;
                }
                effort = effort.saturating_mul(2).max(1);
            }
            if selection.is_none() && seen.len() >= 64 {
                let survival = (accepted as f64 + 2.0) / (seen.len() as f64 + 16.0);
                let remaining = (target.saturating_sub(accepted) as f64 / survival).ceil();
                let constants = crate::engine::cbo::cbo_consts();
                let distance =
                    (query.dimension as f64 / 8.0).max(1.0) * constants.cpu_operator_cost();
                let decode = crate::engine::cbo::operator_cost::per_row_decode_cost(16.0);
                let navigation = if cursor.hnsw.pending.len() as f64 >= remaining {
                    0.0
                } else {
                    remaining * cursor.work.distances as f64 / seen.len().max(1) as f64
                        * (distance + constants.cpu_tuple_cost())
                };
                let continuation = remaining * (predicate_cost + decode)
                    + navigation
                    + target.saturating_sub(accepted) as f64 * (distance + decode);
                if continuation > exact_cost {
                    all_complete = false;
                    break;
                }
            }
        }
        stats.navigation_visits += cursor.work.visited;
        stats.navigation_distances += cursor.work.distances;
        if !all_complete {
            break;
        }
    }
    Ok(all_complete || mode == IterativeScan::Off)
}

/// The internal scalar plan supplies identity, never a vector/LOB projection.
/// Index cursors are bounded; heap scans release latches before SQL evaluation.
fn visit_filtered_row_ids(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    access: &PhysicalPlan,
    filter: &Option<Expr>,
    predicate: &mut FilteredPredicate,
    stats: &mut super::super::analyze_stats::FilteredVectorStats,
    visit: impl FnMut(u64, &[RowId]) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    let columns = &predicate.columns;
    let compiled = &predicate.compiled;
    let eval_ctx = &mut predicate.eval_ctx;
    let storage_compiled = compiled
        .as_ref()
        .filter(|predicate| predicate.page_latch_safe());
    let visit = std::cell::RefCell::new(visit);
    let evaluated = std::cell::Cell::new(0u64);
    let result = run_plan_scan_with_partition_pruning(table, filter, || {
        for sid in db.scan_storage_ids(&table.schema) {
            crate::engine::executor::partition_router::with_pruned_partitions(vec![sid], || {
                let heap = db.cached_heap_handle(sid)?;
                let mut filter_rows = |rows: Vec<(RowId, Vec<Value>)>| -> Result<(), QueryError> {
                    let batch_charge = charge_predicate_rows(&rows)?;
                    let _ids_charge = VectorQueryCharge::try_new_recoverable(
                        rows.len().saturating_mul(std::mem::size_of::<RowId>()),
                    )?;
                    let mut admitted = Vec::with_capacity(rows.len());
                    for (row, values) in rows {
                        if filter.is_some() {
                            evaluated.set(evaluated.get().saturating_add(1));
                        }
                        let keep = match (filter, compiled.as_ref()) {
                            (None, _) => true,
                            (_, Some(compiled)) => compiled.eval(&values)?,
                            (Some(expr), None) => {
                                let row = projected_values_to_plan_row(table, &columns, values)?;
                                eval_plan_bool_with_db(db, txn, expr, &row, eval_ctx)?
                            }
                        };
                        if keep {
                            admitted.push(row);
                        }
                    }
                    drop(batch_charge);
                    (visit.borrow_mut())(sid, &admitted)
                };
                let mut seen = std::collections::HashSet::new();
                let mut seen_charge = VectorQueryCharge::try_new_recoverable(0)?;
                let mut candidates = |ids: &[RowId]| -> Result<(), QueryError> {
                    let Some(first) = ids.iter().position(|id| !seen.contains(id)) else {
                        return Ok(());
                    };
                    let ids = &ids[first..];
                    let _scratch_charge = VectorQueryCharge::try_new_recoverable(
                        ids.len().saturating_mul(std::mem::size_of::<RowId>()),
                    )?;
                    let mut fresh = Vec::with_capacity(ids.len());
                    for &id in ids {
                        if !seen.contains(&id) {
                            fresh.push(id);
                        }
                    }
                    // Charge once per bounded batch, before growing the set.
                    // Physical ordering also keeps heap page probes together.
                    fresh.sort_unstable();
                    fresh.dedup();
                    seen_charge.try_grow_recoverable(fresh.len().saturating_mul(64))?;
                    seen.extend(fresh.iter().copied());
                    let rows = db.read_row_columns_many_with_handle(
                        &heap,
                        &table.schema,
                        &fresh,
                        txn,
                        &columns,
                    )?;
                    filter_rows(rows)
                };
                match access {
                    access if scalar_index_access(access) => {
                        visit_scalar_access_ids(db, txn, table, access, filter, &mut candidates)?;
                    }
                    _ => {
                        let ranges = if matches!(access, PhysicalPlan::ZoneMapScan { .. }) {
                            filter.as_ref().and_then(|e| {
                                db.zone_map_candidate_ranges_for_storage_for_plan(
                                    &table.schema,
                                    sid,
                                    e,
                                )
                            })
                        } else {
                            None
                        };
                        let ranges: Vec<_> = match ranges {
                            Some(ranges) => ranges.into_iter().collect(),
                            None => vec![(0, db.heap_page_count_for_storage(sid)?)],
                        };
                        let _ranges_charge = VectorQueryCharge::try_new_recoverable(
                            ranges
                                .capacity()
                                .saturating_mul(std::mem::size_of::<(u64, u64)>()),
                        )?;
                        for (lo, hi) in ranges {
                            for page in lo..hi {
                                crate::engine::cancel::check_cancelled()?;
                                let mut ids = Vec::new();
                                let mut scratch = VectorQueryCharge::try_new_recoverable(0)?;
                                let scan_columns = if storage_compiled.is_some() {
                                    columns.as_slice()
                                } else {
                                    &[]
                                };
                                db.scan_rows_columns_storage_streaming_range_with_ids(
                                    &table.schema,
                                    sid,
                                    txn,
                                    scan_columns,
                                    page,
                                    page + 1,
                                    |id, values| {
                                        if let Some(predicate) = storage_compiled {
                                            evaluated.set(evaluated.get().saturating_add(1));
                                            if !predicate.eval(values)? {
                                                return Ok(());
                                            }
                                        }
                                        scratch.try_grow_recoverable(
                                            2 * std::mem::size_of::<RowId>(),
                                        )?;
                                        ids.push(id);
                                        Ok(())
                                    },
                                )?;
                                if storage_compiled.is_some() {
                                    (visit.borrow_mut())(sid, &ids)?;
                                } else {
                                    for window in ids.chunks(64) {
                                        filter_rows(db.read_row_columns_many_with_handle(
                                            &heap,
                                            &table.schema,
                                            window,
                                            txn,
                                            &columns,
                                        )?)?;
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(())
            })?;
        }
        Ok(())
    });
    stats.predicate_evaluations = stats.predicate_evaluations.saturating_add(evaluated.get());
    result
}
#[allow(clippy::too_many_arguments)]
fn score_selection_parallel(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    selection: &crate::engine::vector::selection::RowSelection,
    column: usize,
    query: &crate::vector::VectorValue,
    metric: crate::engine::vector::Metric,
    k: usize,
    degree: usize,
    charge: &mut VectorQueryCharge,
) -> Result<BinaryHeap<Scored<(u64, RowId)>>, QueryError> {
    let degree = degree.min(selection.len().div_ceil(256)).max(1);
    let context = super::super::execution_context::ExecutionContext::capture(txn);
    let mut best: BinaryHeap<Scored<(u64, RowId)>> = BinaryHeap::new();
    std::thread::scope(|scope| -> Result<(), QueryError> {
        let mut handles = Vec::new();
        for worker in 0..degree {
            let context = context.clone();
            handles.push(scope.spawn(move || {
                context.run(db, || {
                    let mut local = BinaryHeap::new();
                    let mut local_charge = VectorQueryCharge::try_new(0)?;
                    let mut batch = 0;
                    selection.visit(|sid, ids| {
                        let owns = batch % degree == worker;
                        batch += 1;
                        if owns {
                            score_filtered_ids(
                                db,
                                txn,
                                table,
                                sid,
                                ids,
                                column,
                                query,
                                metric,
                                k,
                                &mut local,
                                &mut local_charge,
                            )?;
                        }
                        Ok(())
                    })?;
                    Ok::<_, QueryError>((local, local_charge))
                })
            }));
        }
        let mut failure = None;
        for handle in handles {
            let result = handle
                .join()
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_QUERY_INTERRUPTED,
                        "filtered vector scoring worker failed",
                    )
                })
                .and_then(|r| r);
            match result {
                Ok((local, mut local_charge)) => {
                    charge.absorb(&mut local_charge)?;
                    for item in local {
                        if best.len() < k {
                            best.push(item);
                        } else if best.peek().is_some_and(|worst| item < *worst) {
                            *best.peek_mut().unwrap() = item;
                        }
                    }
                }
                Err(err) => {
                    if failure.is_none() {
                        failure = Some(err);
                    }
                }
            }
        }
        failure.map_or(Ok(()), Err)
    })?;
    Ok(best)
}

fn predicate_column_indices(
    filter: &Option<Expr>,
    schema: &TableSchema,
) -> Result<Vec<usize>, QueryError> {
    let mut indices = std::collections::BTreeSet::new();
    if let Some(expr) = filter {
        crate::parser::visit::try_inspect_value_expr(expr, |node| {
            match node {
                Expr::Identifier(name) => {
                    indices.insert(schema.column_index(name)?);
                }
                Expr::CompoundIdentifier(names) => {
                    if let Some(name) = names.last() {
                        indices.insert(schema.column_index(name)?);
                    }
                }
                _ => {}
            }
            Ok::<_, QueryError>(true)
        })?;
    }
    Ok(indices.into_iter().collect())
}

fn charge_predicate_rows(rows: &[(RowId, Vec<Value>)]) -> Result<VectorQueryCharge, QueryError> {
    VectorQueryCharge::try_new_recoverable(
        rows.iter()
            .map(|(_, values)| crate::engine::executor::mem_budget::estimate_row_bytes(values) + 64)
            .sum(),
    )
}

fn scalar_index_access(access: &PhysicalPlan) -> bool {
    matches!(
        access,
        PhysicalPlan::IndexLookup { .. }
            | PhysicalPlan::IndexMultiLookup { .. }
            | PhysicalPlan::IndexRangeScan { .. }
            | PhysicalPlan::IndexFastScan { .. }
            | PhysicalPlan::IndexMerge { .. }
    )
}

fn visit_scalar_access_ids(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    access: &PhysicalPlan,
    filter: &Option<Expr>,
    visit: &mut dyn FnMut(&[RowId]) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    match access {
        PhysicalPlan::IndexLookup { index, key, .. } => {
            let bound = Some(IndexBound {
                key: key.clone(),
                inclusive: true,
            });
            visit_scalar_index_cursor(db, txn, table, index, bound.clone(), bound, visit)
        }
        PhysicalPlan::IndexMultiLookup { index, keys, .. } => {
            for key in super::scan::runtime_ordered_multi_lookup_keys(keys)?.as_ref() {
                let bound = Some(IndexBound {
                    key: key.clone(),
                    inclusive: true,
                });
                visit_scalar_index_cursor(
                    db,
                    txn,
                    table,
                    index,
                    bound.clone(),
                    bound,
                    &mut *visit,
                )?;
            }
            Ok(())
        }
        PhysicalPlan::IndexRangeScan {
            index, low, high, ..
        }
        | PhysicalPlan::IndexFastScan {
            index, low, high, ..
        } => {
            let (mut low, mut high) = (low.clone(), high.clone());
            crate::planner::boolean_range::refine_runtime(
                &table.schema,
                index,
                filter.as_ref(),
                &mut low,
                &mut high,
            );
            visit_scalar_index_cursor(db, txn, table, index, low, high, visit)
        }
        PhysicalPlan::IndexMerge { root, .. } => {
            visit_scalar_merge_candidates(db, txn, table, root, visit)
        }
        _ => unreachable!("scalar identity access checked by caller"),
    }
}

#[allow(clippy::too_many_arguments)]
fn visit_scalar_index_cursor(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    mut visit: impl FnMut(&[RowId]) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    let mut cursor = db.range_index_snapshot_cursor(&table.schema, index, low, high, txn)?;
    loop {
        crate::engine::cancel::check_cancelled()?;
        let ids = cursor.next_chunk_row_ids(256)?;
        if ids.is_empty() {
            break;
        }
        visit(&ids)?;
    }
    Ok(())
}

/// The original predicate is reapplied by the caller. Union needs every
/// branch, while any intersection input is a safe superset of its result.
/// This avoids constructing unbudgeted intermediate merge vectors.
fn visit_scalar_merge_candidates(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    node: &crate::planner::IndexMergeNode,
    visit: &mut dyn FnMut(&[RowId]) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    use crate::planner::{IndexAccessKind, IndexMergeNode};
    crate::engine::cancel::check_cancelled()?;
    match node {
        IndexMergeNode::Leaf(leaf) => match &leaf.access {
            IndexAccessKind::Lookup { key, .. } => {
                let bound = Some(IndexBound {
                    key: key.clone(),
                    inclusive: true,
                });
                visit_scalar_index_cursor(db, txn, table, &leaf.index, bound.clone(), bound, visit)
            }
            IndexAccessKind::MultiLookup { keys, .. } => {
                for key in super::scan::runtime_ordered_multi_lookup_keys(keys)?.as_ref() {
                    let bound = Some(IndexBound {
                        key: key.clone(),
                        inclusive: true,
                    });
                    visit_scalar_index_cursor(
                        db,
                        txn,
                        table,
                        &leaf.index,
                        bound.clone(),
                        bound,
                        &mut *visit,
                    )?;
                }
                Ok(())
            }
            IndexAccessKind::Range { low, high } => visit_scalar_index_cursor(
                db,
                txn,
                table,
                &leaf.index,
                low.clone(),
                high.clone(),
                visit,
            ),
        },
        IndexMergeNode::Union { children, .. } => {
            for child in children {
                visit_scalar_merge_candidates(db, txn, table, child, visit)?;
            }
            Ok(())
        }
        IndexMergeNode::Intersection { children } => {
            if let Some(child) = children.first() {
                visit_scalar_merge_candidates(db, txn, table, child, visit)?;
            }
            Ok(())
        }
    }
}
