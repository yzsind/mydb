//! Exact predicate/score fusion over bounded, partition-local scalar identities.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

static ACTIVE_SCORERS: AtomicUsize = AtomicUsize::new(0);

/// Keep single-query parallel scoring while reducing worker fan-out when
/// several exact pipelines are ready. Admission never waits or holds an
/// additional storage lock, and session/planned degrees remain upper bounds.
struct ScoringShare(usize);

impl ScoringShare {
    fn enter() -> Self {
        Self(ACTIVE_SCORERS.fetch_add(1, Ordering::AcqRel) + 1)
    }

    fn degree(&self, requested: usize) -> usize {
        static CPUS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        let cpus =
            *CPUS.get_or_init(|| std::thread::available_parallelism().map_or(1, usize::from));
        let capacity =
            crate::planner::cpu_bound_parallel_degree(cpus.min(u32::MAX as usize) as u32) as usize;
        requested.min((capacity / self.0).max(1)).max(1)
    }
}

impl Drop for ScoringShare {
    fn drop(&mut self) {
        ACTIVE_SCORERS.fetch_sub(1, Ordering::AcqRel);
    }
}

/// These are unqualified source identities, deliberately not RowSelection.
struct Candidates {
    partitions: Vec<(u64, Vec<RowId>)>,
    _charge: VectorQueryCharge,
}

fn value_bound(kind: &DataTypeKind) -> Option<usize> {
    let bytes = match kind {
        DataTypeKind::Json
        | DataTypeKind::Geometry { .. }
        | DataTypeKind::Vector { dim: None, .. } => return None,
        DataTypeKind::Vector {
            datatype,
            dim: Some(dim),
        } => datatype.payload_len(*dim),
        DataTypeKind::Char { len } => len.unwrap_or(1).checked_mul(4)?,
        DataTypeKind::VarChar { len } => len.unwrap_or(255).checked_mul(4)?,
        DataTypeKind::Binary { len } => len.unwrap_or(1),
        DataTypeKind::VarBinary { len } => len.unwrap_or(255),
        DataTypeKind::Text { max_len } | DataTypeKind::Blob { max_len } => *max_len as usize,
        DataTypeKind::Enum { values } => values.iter().map(String::len).max().unwrap_or(0),
        DataTypeKind::Set { values } => values
            .iter()
            .try_fold(values.len(), |n, s| n.checked_add(s.len()))?,
        _ => 128,
    };
    bytes.checked_add(128)
}

fn collect(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    access: &PhysicalPlan,
    filter: &Option<Expr>,
) -> Result<Candidates, QueryError> {
    let mut charge = VectorQueryCharge::try_new_recoverable(0)?;
    let mut partitions = Vec::new();
    run_plan_scan_with_partition_pruning(table, filter, || {
        for sid in db.scan_storage_ids(&table.schema) {
            charge.try_grow_recoverable(128)?;
            let mut rows = Vec::new();
            crate::engine::executor::partition_router::with_pruned_partitions(vec![sid], || {
                visit_scalar_access_ids(db, txn, table, access, filter, &mut |ids| {
                    // Covers geometric capacity and temporary reallocation.
                    charge.try_grow_recoverable(ids.len().saturating_mul(32))?;
                    rows.extend_from_slice(ids);
                    Ok(())
                })
            })?;
            rows.sort_unstable();
            rows.dedup();
            partitions.push((sid, rows));
        }
        Ok(())
    })?;
    Ok(Candidates {
        partitions,
        _charge: charge,
    })
}

fn offer(best: &mut BinaryHeap<Scored<(u64, RowId)>>, item: Scored<(u64, RowId)>, k: usize) {
    if best.len() < k {
        best.push(item);
    } else if best.peek().is_some_and(|worst| item < *worst) {
        *best.peek_mut().unwrap() = item;
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn try_score(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
    table: &PlanTable,
    access: &PhysicalPlan,
    filter: &Option<Expr>,
    predicate: &FilteredPredicate,
    column: usize,
    query: &crate::vector::VectorValue,
    metric: crate::engine::vector::Metric,
    k: usize,
    degree: usize,
    best: &mut BinaryHeap<Scored<(u64, RowId)>>,
    charge: &mut VectorQueryCharge,
    stats: &mut super::super::super::analyze_stats::FilteredVectorStats,
) -> Result<bool, QueryError> {
    let Some(compiled) = predicate.compiled.as_ref().filter(|p| p.page_latch_safe()) else {
        return Ok(false);
    };
    if !scalar_index_access(access) {
        return Ok(false);
    }
    // Historical/LOB fallback can decode a full visible row. Bound that work
    // as well as the narrow hot path before choosing this optional pipeline.
    let Some(row_bound) = table
        .schema
        .columns
        .iter()
        .try_fold(0usize, |n, c| n.checked_add(value_bound(&c.data_type)?))
    else {
        return Ok(false);
    };
    let candidates = match collect(db, txn, table, access, filter) {
        Ok(rows) => rows,
        Err(err) if err.is(crate::error::catalog::ER_OUTOFMEMORY) => return Ok(false),
        Err(err) => return Err(err),
    };
    let job_count = candidates
        .partitions
        .iter()
        .map(|(_, rows)| rows.len().div_ceil(256))
        .sum::<usize>();
    let _jobs_charge = match VectorQueryCharge::try_new_recoverable(
        job_count.saturating_mul(48).saturating_add(128),
    ) {
        Ok(charge) => charge,
        Err(err) if err.is(crate::error::catalog::ER_OUTOFMEMORY) => return Ok(false),
        Err(err) => return Err(err),
    };
    let mut jobs = Vec::new();
    if jobs.try_reserve_exact(job_count).is_err() {
        return Ok(false);
    }
    for (partition, (_, rows)) in candidates.partitions.iter().enumerate() {
        for lo in (0..rows.len()).step_by(256) {
            jobs.push((partition, lo, (lo + 256).min(rows.len())));
        }
    }
    if jobs.is_empty() {
        best.clear();
        return Ok(true);
    }
    let share = ScoringShare::enter();
    let degree = share.degree(degree).min(jobs.len());
    let retained = k.saturating_mul(std::mem::size_of::<Scored<(u64, RowId)>>());
    let reservation = row_bound
        .saturating_mul(128)
        .saturating_add(128 * 1024)
        .saturating_add(retained.saturating_mul(2))
        .saturating_mul(degree)
        .saturating_add(retained);
    let mut workspace = match VectorQueryCharge::try_new_recoverable(reservation) {
        Ok(charge) => charge,
        Err(err) if err.is(crate::error::catalog::ER_OUTOFMEMORY) => return Ok(false),
        Err(err) => return Err(err),
    };
    let mut heaps = Vec::with_capacity(degree);
    for _ in 0..degree {
        let mut local = BinaryHeap::new();
        local.try_reserve_exact(k).map_err(|_| {
            error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "filtered exact heap allocation failed",
            )
        })?;
        heaps.push(local);
    }
    let mut merged = BinaryHeap::new();
    merged.try_reserve_exact(k).map_err(|_| {
        error(
            crate::error::catalog::ER_OUTOFMEMORY,
            "filtered exact merge allocation failed",
        )
    })?;
    let columns = &predicate.columns;
    let next = AtomicUsize::new(0);
    let datatype = match table.schema.columns[column].data_type {
        DataTypeKind::Vector { datatype, .. } => datatype,
        _ => unreachable!(),
    };
    let prepared =
        crate::engine::vector::distance::PreparedNativeDistance::try_new(query, datatype, metric);
    let decoded = (datatype == crate::vector::VectorDataType::Float32
        && query.datatype == crate::vector::VectorDataType::Float32
        && metric == crate::engine::vector::Metric::Cosine)
        .then(|| crate::engine::vector::to_f32(query));
    let float_prepared = decoded
        .as_deref()
        .map(|values| crate::engine::vector::distance::PreparedF32Distance::new(values, metric));
    let worker = |mut local: BinaryHeap<Scored<(u64, RowId)>>| -> Result<_, QueryError> {
        let mut evaluated = 0u64;
        let mut scored = 0u64;
        let mut batches = 0u64;
        let mut current_heap = None;
        loop {
            crate::engine::cancel::check_cancelled()?;
            let position = next.fetch_add(1, Ordering::Relaxed);
            let Some(&(partition, lo, hi)) = jobs.get(position) else {
                break;
            };
            let (sid, rows) = &candidates.partitions[partition];
            if current_heap.as_ref().is_none_or(|(id, _)| id != sid) {
                current_heap = Some((*sid, db.cached_heap_handle(*sid)?));
            }
            let heap = &current_heap.as_ref().unwrap().1;
            let scores = db.score_filtered_vector_rows_many_with_handle(
                heap,
                &table.schema,
                &rows[lo..hi],
                txn,
                column,
                query,
                metric,
                prepared.as_ref(),
                float_prepared.as_ref(),
                columns,
                |values| {
                    evaluated += 1;
                    compiled.eval(values)
                },
            )?;
            scored += scores.len() as u64;
            batches += 1;
            for (row, dist) in scores {
                offer(
                    &mut local,
                    Scored {
                        id: (*sid, row),
                        dist,
                    },
                    k,
                );
            }
        }
        Ok((local, evaluated, scored, batches))
    };
    // Once evaluation begins, errors propagate. Replaying an optional path at
    // this point could publish conversion diagnostics a second time.
    let results = if degree == 1 {
        vec![worker(heaps.pop().unwrap())?]
    } else {
        let context = super::super::super::execution_context::ExecutionContext::capture(txn);
        std::thread::scope(|scope| -> Result<Vec<_>, QueryError> {
            let mut handles = Vec::with_capacity(degree);
            for heap in heaps {
                let context = context.clone();
                let worker = &worker;
                handles.push(scope.spawn(move || context.run(db, || worker(heap))));
            }
            let mut results = Vec::with_capacity(degree);
            let mut failure = None;
            for handle in handles {
                match handle
                    .join()
                    .map_err(|_| {
                        error(
                            crate::error::catalog::ER_QUERY_INTERRUPTED,
                            "filtered exact worker failed",
                        )
                    })
                    .and_then(|r| r)
                {
                    Ok(result) => results.push(result),
                    Err(err) => {
                        if failure.is_none() {
                            failure = Some(err)
                        }
                    }
                }
            }
            if let Some(err) = failure {
                Err(err)
            } else {
                Ok(results)
            }
        })?
    };
    for (local, evaluated, scored, batches) in results {
        stats.predicate_evaluations += evaluated;
        stats.reranks += scored;
        stats.batches += batches;
        for item in local {
            offer(&mut merged, item, k);
        }
    }
    let retained = merged
        .capacity()
        .saturating_mul(std::mem::size_of::<Scored<(u64, RowId)>>());
    workspace.shrink(workspace.bytes().saturating_sub(retained));
    charge.absorb(&mut workspace)?;
    *best = merged;
    super::super::super::analyze_stats::add_vector_runtime(plan, 0, 0, degree as u32);
    Ok(true)
}
