//! Page-aligned post-probe reads shared by ordinary and predicate-first INLJ.
use super::*;

#[cfg(test)]
mod tests;

type ProbeRows = Vec<(RowId, Vec<Value>)>;

pub(super) fn read_inlj_probe_rows_parallel(
    db: &Arc<Database>,
    heap: &HeapHandle,
    right_table: &PlanTable,
    row_ids: &[RowId],
    txn: TxnView,
    projection: &[usize],
) -> Result<ProbeRows, QueryError> {
    let degree = inlj_probe_parallel_degree(row_ids.len());
    if degree <= 1 {
        return db.read_row_columns_many_with_handle(
            heap,
            &right_table.schema,
            row_ids,
            txn,
            projection,
        );
    }
    read_inlj_row_tasks(
        db,
        txn,
        row_ids,
        degree,
        "INLJ probe worker panicked",
        |rows| {
            db.read_row_columns_many_with_handle(heap, &right_table.schema, rows, txn, projection)
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn read_inlj_probe_rows_predicate_first_parallel(
    db: &Arc<Database>,
    heap: &HeapHandle,
    right_table: &PlanTable,
    row_ids: &[RowId],
    txn: TxnView,
    predicate_columns: &[usize],
    output_columns: &[usize],
    filter_schema: &TableSchema,
    residual: Option<&Expr>,
    compiled_filter: Option<&CompiledRowPredicate>,
    compiled_runtime_filters: &[Option<CompiledRuntimeFilterProbe>],
) -> Result<ProbeRows, QueryError> {
    let degree = inlj_probe_parallel_degree(row_ids.len());
    let read_rows = |rows: &[RowId]| {
        db.read_row_columns_many_predicate_first_with_handle(
            heap,
            &right_table.schema,
            rows,
            txn,
            predicate_columns,
            output_columns,
            |values| {
                let residual_admits = match compiled_filter {
                    Some(predicate) => predicate.eval(values)?,
                    None => residual
                        .map(|expr| crate::engine::expr::eval_bool(expr, filter_schema, values))
                        .transpose()?
                        .unwrap_or(true),
                };
                Ok(residual_admits
                    && runtime_filters_admit_values_indexed(compiled_runtime_filters, values))
            },
        )
    };
    if degree <= 1 {
        return read_rows(row_ids);
    }
    read_inlj_row_tasks(
        db,
        txn,
        row_ids,
        degree,
        "INLJ predicate-first worker panicked",
        read_rows,
    )
}

/// Inputs are ordered by page/slot. Extend task ends so no heap page is
/// split between workers, preserving sequential decode and cache locality.
fn page_aligned_row_tasks(row_ids: &[RowId], task_rows: usize) -> Vec<(usize, usize)> {
    debug_assert!(task_rows > 0);
    let mut read_tasks = Vec::with_capacity(row_ids.len().div_ceil(task_rows));
    let mut lo = 0;
    while lo < row_ids.len() {
        let mut hi = (lo + task_rows).min(row_ids.len());
        while hi < row_ids.len() && row_ids[hi].page_id == row_ids[hi - 1].page_id {
            hi += 1;
        }
        read_tasks.push((lo, hi));
        lo = hi;
    }
    read_tasks
}

/// Share only orchestration; the storage callback stays monomorphized and
/// retains each read mode's visibility, projection and predicate contracts.
fn read_inlj_row_tasks<F>(
    db: &Arc<Database>,
    txn: TxnView,
    row_ids: &[RowId],
    degree: usize,
    panic_message: &'static str,
    read_rows: F,
) -> Result<ProbeRows, QueryError>
where
    F: Fn(&[RowId]) -> Result<ProbeRows, QueryError> + Sync,
{
    debug_assert!(degree > 1);
    let read_tasks = page_aligned_row_tasks(row_ids, inlj_probe_task_rows());
    let task_count = read_tasks.len();
    record_inlj_probe_parallel(task_count as u64, row_ids.len() as u64);
    let next_task = AtomicUsize::new(0);
    let execution_context = super::super::execution_context::ExecutionContext::capture(txn);

    let read_tasks = &read_tasks;

    let read_rows = &read_rows;
    let mut completed = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(degree);
        for _ in 0..degree {
            let next_task = &next_task;
            let worker_context = execution_context.clone();

            handles.push(scope.spawn(move || {
                let run = || -> Result<Vec<(usize, Vec<(RowId, Vec<Value>)>)>, QueryError> {
                    worker_context.run(db, || {
                        let mut parts = Vec::new();
                        loop {
                            let task = next_task.fetch_add(1, Ordering::Relaxed);
                            if task >= task_count {
                                break;
                            }
                            crate::engine::cancel::check_cancelled()?;
                            let (lo, hi) = read_tasks[task];
                            let rows = read_rows(&row_ids[lo..hi])?;
                            parts.push((task, rows));
                        }
                        Ok(parts)
                    })
                };
                run()
            }));
        }
        let mut parts = Vec::with_capacity(task_count);
        for handle in handles {
            let mut worker_parts = handle
                .join()
                .map_err(|_| error(crate::error::catalog::ER_UNKNOWN_ERROR, panic_message))??;
            parts.append(&mut worker_parts);
        }
        Ok::<_, QueryError>(parts)
    })?;
    completed.sort_unstable_by_key(|(task, _)| *task);
    let mut out = Vec::with_capacity(row_ids.len());
    for (_, mut rows) in completed {
        out.append(&mut rows);
    }
    Ok(out)
}
