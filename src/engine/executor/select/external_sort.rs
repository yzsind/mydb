//! Run-and-merge external sort for `Vec<PlanRow>`.
//!
//! Both materialized and streaming inputs feed the same bounded run builder.
//! Keys are evaluated once and charged incrementally, before retaining the
//! next row. Query or work-area pressure flushes the current run; the final
//! merge buffers one decoded row per run. Materialized callers may retain
//! their input and output rows, but never an unbounded second copy of keys.
//!
//! Run files reuse [`super::grace`] and live in an RAII [`SpillTmpDir`].
//! [`sort_plan_rows_for_plan`] collects the result for blocking consumers;
//! [`sort_plan_input_stream`] returns a cursor owning the final merge and
//! temporary directory. Cancellation, errors and early cursor drop release
//! the directory and its quota reservations.
//!
//! Stability is relative to this operator's input. SQL peers may arrive in a
//! different order from another scan, aggregation or parallel execution plan.

use std::cmp::Ordering;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::engine::executor::mem_budget::{current_budget, ESTIMATED_ROW_BYTES};
use crate::parser::OrderBy;
use crate::types::{QueryError, Value};

#[cfg(test)]
use super::eval::sort_plan_rows_by_order;
use super::eval::{precompute_sort_keys_for_plan_row, sort_fields_for_plan_rows};
use super::grace::{PartitionReader, PartitionWriter, SpillTmpDir};
use super::plan::PlanRow;

/// Default target bytes per spill run when no query budget is active.
const DEFAULT_RUN_BYTES: usize = 8 * 1024 * 1024;
#[cfg(not(test))]
const MIN_RUN_BYTES: usize = 1024 * 1024;
#[cfg(test)]
const MIN_RUN_BYTES: usize = 16 * 1024;
#[cfg(not(test))]
const MIN_RUN_ROWS: usize = 1024;
#[cfg(test)]
const MIN_RUN_ROWS: usize = 32;

#[derive(Debug, Clone)]
struct SortRun {
    rows_path: PathBuf,
    keys_path: PathBuf,
    bytes: u64,
}

#[derive(Debug)]
struct KeyedRow {
    keys: Vec<Value>,
    row: PlanRow,
    ordinal: u64,
}

/// Sort one input-order chunk. `ordinal` is unique, so input-order stability is
/// encoded in the comparator itself; the in-place unstable sorter is both
/// deterministic and allocation-free.
fn sort_keyed_rows(
    chunk: &mut Vec<KeyedRow>,
    fields: &[crate::engine::executor::sort::SortField],
) -> Result<(), QueryError> {
    // A homogeneous integer key needs no coercion/collation dispatch in the
    // O(N log N) loop. Prove the lane once; NULL and every other shape retain
    // the shared SQL comparator. The ordinal tie-break remains identical.
    if fields.len() == 1
        && chunk.len() >= 32
        && chunk
            .iter()
            .all(|row| matches!(row.keys.as_slice(), [Value::Int(_)]))
    {
        let ascending = fields[0].ascending;
        chunk.sort_unstable_by(|a, b| {
            let (Value::Int(left), Value::Int(right)) = (&a.keys[0], &b.keys[0]) else {
                unreachable!("integer sort lane checked above")
            };
            let order = left.cmp(right);
            (if ascending { order } else { order.reverse() })
                .then_with(|| a.ordinal.cmp(&b.ordinal))
        });
        return Ok(());
    }
    if chunk.len() >= 256
        && crate::engine::executor::sort::key_encode::try_sort_by_borrowed_encoded_keys(
            chunk,
            fields,
            |row| row.keys.as_slice(),
        )?
    {
        return Ok(());
    }
    chunk.sort_unstable_by(|a, b| {
        crate::engine::executor::sort::compare_sort_keys(fields, &a.keys, &b.keys)
            .then_with(|| a.ordinal.cmp(&b.ordinal))
    });
    Ok(())
}

#[cfg(test)]
struct PreparedSortInput {
    rows: Vec<PlanRow>,
    keys: Vec<Vec<Value>>,
}

#[cfg(test)]
impl PreparedSortInput {
    fn prepare(rows: Vec<PlanRow>, order_by: &[OrderBy]) -> Result<Self, QueryError> {
        let mut keys = crate::engine::executor::mem_budget::try_vec_with_capacity(
            rows.len(),
            "sort key rows",
        )?;
        for row in &rows {
            crate::engine::cancel::check_cancelled()?;
            let row_keys = precompute_sort_keys_for_plan_row(row, order_by)?;
            keys.push(row_keys);
        }
        Ok(Self { rows, keys })
    }

    fn into_keyed_rows(self) -> Vec<KeyedRow> {
        self.rows
            .into_iter()
            .zip(self.keys)
            .enumerate()
            .map(|(ordinal, (row, keys))| KeyedRow {
                keys,
                row,
                ordinal: ordinal as u64,
            })
            .collect()
    }
}

#[derive(Default)]
struct SortMetrics {
    spill_bytes: u64,
    run_count: u64,
    merge_passes: u64,
    peak_memory_bytes: u64,
}

/// Sort `rows` according to `order_by`. Falls back to the in-memory
/// `Vec::sort_by` when the current per-query budget is comfortable;
/// otherwise spills sorted runs to disk under
/// `<db.data_dir()>/tmp/sort-<pid>-<seq>/` and merges them.
///
/// The spill directory is removed on drop, so a panic between spilling
/// and merging cannot leak files.
pub(super) fn sort_plan_rows(
    data_dir: &Path,
    rows: Vec<PlanRow>,
    order_by: &[OrderBy],
) -> Result<Vec<PlanRow>, QueryError> {
    let _sorting = crate::engine::cancel::enter_process_stage(
        crate::engine::session_registry::ProcessState::SortingForOrder,
    );
    sort_plan_rows_for_plan(data_dir, rows, order_by, None)
}

/// Spill-capable sort adapter for DML rows whose ORDER BY expressions have
/// already been evaluated. Keeping only keys plus the exact row reference in
/// each run avoids serialising full table rows and guarantees merge comparison
/// cannot execute (or hide errors from) an expression.
pub(in crate::engine::executor) fn sort_precomputed_dml_refs(
    data_dir: &Path,
    rows: Vec<(Vec<Value>, crate::storage::DmlRowRef)>,
    fields: &[crate::engine::executor::sort::SortField],
) -> Result<Vec<crate::storage::DmlRowRef>, QueryError> {
    use crate::engine::executor::select::plan::{PlanField, PlanRow};
    use crate::parser::{Expr, OrderBy};
    use crate::types::ColumnType;

    let key_names = (0..fields.len())
        .map(|idx| format!("__dml_sort_key_{idx}"))
        .collect::<Vec<_>>();
    let mut order_by = key_names
        .iter()
        .zip(fields.iter())
        .map(|(name, field)| OrderBy {
            expr: Expr::Identifier(name.clone()),
            asc: field.ascending,
            nulls_first: Some(field.nulls_first),
        })
        .collect::<Vec<_>>();
    for name in ["__dml_storage_id", "__dml_page_id", "__dml_slot_id"] {
        order_by.push(OrderBy {
            expr: Expr::Identifier(name.to_string()),
            asc: true,
            nulls_first: None,
        });
    }

    let plan_rows = rows
        .into_iter()
        .map(|(keys, row_ref)| {
            let mut plan_fields = Vec::with_capacity(keys.len() + 3);
            for ((name, value), field) in key_names.iter().zip(keys).zip(fields) {
                plan_fields.push(PlanField {
                    meta: super::plan::intern_col_meta(super::plan::ColMeta {
                        invisible: false,
                        table: String::new(),
                        alias: None,
                        column: name.clone(),
                        column_type: ColumnType::Blob,
                        source: crate::types::ColumnSource {
                            collation_id: field.collation_id,
                            ..Default::default()
                        },
                    }),
                    value,
                });
            }
            plan_fields.push(PlanField::new(
                "",
                None,
                "__dml_storage_id",
                ColumnType::LongLong,
                Value::UInt(row_ref.storage_id),
            ));
            plan_fields.push(PlanField::new(
                "",
                None,
                "__dml_page_id",
                ColumnType::LongLong,
                Value::UInt(row_ref.row_id.page_id as u64),
            ));
            plan_fields.push(PlanField::new(
                "",
                None,
                "__dml_slot_id",
                ColumnType::LongLong,
                Value::UInt(row_ref.row_id.slot_id as u64),
            ));
            PlanRow::from_fields(plan_fields)
        })
        .collect();
    let sorted = sort_plan_rows(data_dir, plan_rows, &order_by)?;
    sorted
        .into_iter()
        .map(|row| {
            let base = row.fields.len().saturating_sub(3);
            let storage_id = match row.fields.get(base).map(|field| &field.value) {
                Some(Value::UInt(value)) => *value,
                _ => {
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "invalid DML sort row",
                    ))
                }
            };
            let page_id = match row.fields.get(base + 1).map(|field| &field.value) {
                Some(Value::UInt(value)) => u32::try_from(*value).map_err(|_| {
                    crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "invalid DML page id",
                    )
                })?,
                _ => {
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "invalid DML sort row",
                    ))
                }
            };
            let slot_id = match row.fields.get(base + 2).map(|field| &field.value) {
                Some(Value::UInt(value)) => u16::try_from(*value).map_err(|_| {
                    crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "invalid DML slot id",
                    )
                })?,
                _ => {
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "invalid DML sort row",
                    ))
                }
            };
            Ok(crate::storage::DmlRowRef {
                storage_id,
                row_id: crate::storage::RowId { page_id, slot_id },
            })
        })
        .collect()
}

pub(super) fn sort_plan_rows_for_plan(
    data_dir: &Path,
    rows: Vec<PlanRow>,
    order_by: &[OrderBy],
    plan: Option<&crate::planner::PhysicalPlan>,
) -> Result<Vec<PlanRow>, QueryError> {
    let _sorting = crate::engine::cancel::enter_process_stage(
        crate::engine::session_registry::ProcessState::SortingForOrder,
    );
    crate::engine::cancel::check_cancelled()?;
    if order_by.is_empty() || rows.is_empty() {
        return Ok(rows);
    }
    let first_run_bytes = current_budget().map_or_else(
        crate::engine::executor::mem_budget::executor_memory_target_bytes,
        |budget| budget.remaining(),
    );
    let mut sorted = sort_plan_input_stream_with_limit(
        data_dir,
        Box::new(super::plan::MaterializedStream::new(rows)),
        order_by,
        plan,
        Some(first_run_bytes),
        1,
    )?;
    let mut out = Vec::new();
    while let Some(row) = sorted.next()? {
        out.push(row);
    }
    Ok(out)
}

pub(super) fn sort_plan_rows_parallel_spill(
    data_dir: &Path,
    rows: Vec<PlanRow>,
    order_by: &[OrderBy],
    degree: usize,
    context: Option<(
        &Arc<crate::storage::Database>,
        super::execution_context::ExecutionContext,
    )>,
) -> Result<Vec<PlanRow>, QueryError> {
    let _sorting = crate::engine::cancel::enter_process_stage(
        crate::engine::session_registry::ProcessState::SortingForOrder,
    );
    crate::engine::cancel::check_cancelled()?;
    if order_by.is_empty() || rows.len() < 2 || degree <= 1 {
        return sort_plan_rows(data_dir, rows, order_by);
    }
    let tmp = SpillTmpDir::create_kind(data_dir, "sort")?;
    let total_rows = rows.len();
    let fields = sort_fields_for_plan_rows(order_by, rows.first());
    let chunk_size = total_rows.div_ceil(degree);
    // Tail-first slicing avoids the O(N) memmove that drain triggers
    // on each chunk — see `split_off_into_chunks` in `parallel.rs`.
    let chunks: Vec<Vec<PlanRow>> = super::parallel::split_off_into_chunks(rows, chunk_size);
    let mut results: Vec<Option<Result<Vec<SortRun>, QueryError>>> =
        (0..chunks.len()).map(|_| None).collect();
    let fields_ref = &fields;
    let order_by_ref = order_by;
    let cancel_token = crate::engine::cancel::current_token();
    let budget = current_budget();
    let run_bytes = (run_bytes_for_budget(budget.as_deref()) / degree).max(1);
    let session_info = super::eval::current_session_expr_info();
    let session_info = &session_info;
    let params_snapshot = crate::engine::params::snapshot_current_context();
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(chunks.len());
        for (idx, chunk) in chunks.into_iter().enumerate() {
            let tmp_ref = &tmp;
            let cancel_token = cancel_token.clone();
            let budget = budget.clone();
            let context = context.clone();
            let params = params_snapshot.as_ref();
            handles.push((
                idx,
                scope.spawn(move || -> Result<Vec<SortRun>, QueryError> {
                    let _cancel_guard = cancel_token.map(crate::engine::cancel::enter);
                    let build = || {
                        let mut keyed = Vec::new();
                        let mut bytes = 0usize;
                        let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
                        let mut runs = Vec::new();
                        let prefix = format!("prun-{idx}");
                        for (ordinal, row) in chunk.into_iter().enumerate() {
                            crate::engine::cancel::check_cancelled()?;
                            let keys = precompute_sort_keys_for_plan_row(&row, order_by_ref)?;
                            let row_bytes = estimate_keyed_row_bytes(&row, &keys);
                            if !keyed.is_empty() && bytes.saturating_add(row_bytes) > run_bytes {
                                runs.push(flush_sort_run(
                                    tmp_ref,
                                    &mut keyed,
                                    fields_ref,
                                    runs.len(),
                                    &prefix,
                                )?);
                                bytes = 0;
                                charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
                            }
                            // A bounded run can cross a spill watermark but never
                            // the shared process hard limit.
                            charge.add_force(row_bytes)?;
                            keyed.push(KeyedRow {
                                keys,
                                row,
                                ordinal: ordinal as u64,
                            });
                            bytes = bytes.saturating_add(row_bytes);
                        }
                        if !keyed.is_empty() {
                            runs.push(flush_sort_run(
                                tmp_ref,
                                &mut keyed,
                                fields_ref,
                                runs.len(),
                                &prefix,
                            )?);
                        }
                        Ok(runs)
                    };
                    let run = || {
                        super::eval::with_session_expr_info_option(session_info, || match params {
                            Some(params) => params.run(build),
                            None => build(),
                        })
                    };
                    if let Some((db, context)) = context {
                        return context.run(db, run);
                    }
                    match budget {
                        Some(budget) => {
                            crate::engine::executor::mem_budget::with_query_budget(budget, run)
                        }
                        None => run(),
                    }
                }),
            ));
        }
        for (idx, handle) in handles {
            match handle.join() {
                Ok(result) => results[idx] = Some(result),
                Err(_) => {
                    results[idx] = Some(Err(crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "parallel sort spill worker panicked",
                    )));
                }
            }
        }
    });
    let mut runs = Vec::with_capacity(results.len());
    for result in results {
        let Some(result) = result else {
            return Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "parallel sort worker did not run",
            ));
        };
        runs.extend(result?);
    }
    let mut pass = 0usize;
    let fanin = sort_merge_fanin();
    while runs.len() > fanin {
        let mut next_runs = Vec::with_capacity(runs.len().div_ceil(fanin));
        for group in runs.chunks(fanin) {
            next_runs.push(merge_runs_to_run(
                &tmp,
                group,
                &fields,
                pass,
                next_runs.len(),
            )?);
        }
        remove_run_files(&tmp, &runs);
        runs = next_runs;
        pass += 1;
    }
    let out = merge_runs_to_vec(&tmp, &runs, &fields, total_rows)?;
    drop(tmp);
    Ok(out)
}

#[cfg(test)]
fn in_memory_sort(
    mut rows: Vec<PlanRow>,
    order_by: &[OrderBy],
) -> Result<Vec<PlanRow>, QueryError> {
    sort_plan_rows_by_order(&mut rows, order_by)?;
    Ok(rows)
}

#[cfg(test)]
fn spill_prepared_sort_at(
    data_dir: &Path,
    prepared: PreparedSortInput,
    fields: Vec<crate::engine::executor::sort::SortField>,
    run_bytes: usize,
    plan: Option<&crate::planner::PhysicalPlan>,
) -> Result<Vec<PlanRow>, QueryError> {
    let total_rows = prepared.rows.len();
    let (tmp, runs, metrics) = spill_prepared_to_runs(data_dir, prepared, &fields, run_bytes)?;
    if runs.is_empty() {
        return Ok(Vec::new());
    }
    let out = merge_runs_to_vec(&tmp, &runs, &fields, total_rows)?;
    report_spill_metrics(plan, &metrics);
    // tmp dropped here -> directory removed.
    drop(tmp);
    Ok(out)
}

#[cfg(test)]
fn spill_sort_at(
    data_dir: &Path,
    rows: Vec<PlanRow>,
    order_by: &[OrderBy],
    run_bytes: usize,
    plan: Option<&crate::planner::PhysicalPlan>,
) -> Result<Vec<PlanRow>, QueryError> {
    let fields = sort_fields_for_plan_rows(order_by, rows.first());
    let prepared = PreparedSortInput::prepare(rows, order_by)?;
    spill_prepared_sort_at(data_dir, prepared, fields, run_bytes, plan)
}

/// Run-generation phase shared by the materialising (`spill_sort_at`)
/// and streaming (`sort_plan_rows_stream`) spill paths: consume `rows`
/// into sorted on-disk runs and reduce them to at most
/// `MYDB_SORT_MERGE_FANIN` via intermediate merge passes, leaving the
/// final merge to the caller.
#[cfg(test)]
fn spill_to_runs(
    data_dir: &Path,
    rows: Vec<PlanRow>,
    order_by: &[OrderBy],
    run_bytes: usize,
) -> Result<
    (
        SpillTmpDir,
        Vec<SortRun>,
        Vec<crate::engine::executor::sort::SortField>,
        SortMetrics,
    ),
    QueryError,
> {
    let fields = sort_fields_for_plan_rows(order_by, rows.first());
    let prepared = PreparedSortInput::prepare(rows, order_by)?;
    let (tmp, runs, metrics) = spill_prepared_to_runs(data_dir, prepared, &fields, run_bytes)?;
    Ok((tmp, runs, fields, metrics))
}

#[cfg(test)]
fn spill_prepared_to_runs(
    data_dir: &Path,
    prepared: PreparedSortInput,
    fields: &[crate::engine::executor::sort::SortField],
    run_bytes: usize,
) -> Result<(SpillTmpDir, Vec<SortRun>, SortMetrics), QueryError> {
    crate::engine::cancel::check_cancelled()?;
    let tmp = SpillTmpDir::create_kind(data_dir, "sort")?;
    let mut runs: Vec<SortRun> = Vec::new();
    let mut chunk: Vec<KeyedRow> = Vec::new();
    let mut chunk_bytes = 0usize;
    let mut metrics = SortMetrics {
        peak_memory_bytes: run_bytes as u64,
        ..SortMetrics::default()
    };

    for keyed in prepared.into_keyed_rows() {
        crate::engine::cancel::check_cancelled()?;
        let row_bytes = estimate_keyed_row_bytes(&keyed.row, &keyed.keys);
        if !chunk.is_empty() && chunk_bytes.saturating_add(row_bytes) > run_bytes {
            let run = flush_sort_run(&tmp, &mut chunk, fields, runs.len(), "run")?;
            metrics.spill_bytes = metrics.spill_bytes.saturating_add(run.bytes);
            runs.push(run);
            chunk_bytes = 0;
        }
        chunk.push(keyed);
        chunk_bytes = chunk_bytes.saturating_add(row_bytes);
    }
    if !chunk.is_empty() {
        let run = flush_sort_run(&tmp, &mut chunk, fields, runs.len(), "run")?;
        metrics.spill_bytes = metrics.spill_bytes.saturating_add(run.bytes);
        runs.push(run);
    }
    metrics.run_count = runs.len() as u64;

    let fanin = sort_merge_fanin();
    let mut pass = 0usize;
    while runs.len() > fanin {
        let mut next_runs = Vec::with_capacity(runs.len().div_ceil(fanin));
        for group in runs.chunks(fanin) {
            let run = merge_runs_to_run(&tmp, group, fields, pass, next_runs.len())?;
            metrics.spill_bytes = metrics.spill_bytes.saturating_add(run.bytes);
            next_runs.push(run);
        }
        remove_run_files(&tmp, &runs);
        runs = next_runs;
        pass += 1;
        metrics.merge_passes = metrics.merge_passes.saturating_add(1);
    }
    Ok((tmp, runs, metrics))
}

fn report_spill_metrics(plan: Option<&crate::planner::PhysicalPlan>, metrics: &SortMetrics) {
    if let Some(plan) = plan {
        super::analyze_stats::add_spill_bytes(plan, metrics.spill_bytes);
        super::analyze_stats::add_sort_runs(plan, metrics.run_count, metrics.merge_passes);
        super::analyze_stats::add_peak_memory(plan, metrics.peak_memory_bytes);
    }
}

/// Streaming ORDER BY output. Same budget/spill decision as
/// [`sort_plan_rows_for_plan`], but the spilled case returns a
/// [`PlanStream`] that pulls one row at a time out of the final
/// LoserTree merge instead of re-materialising the whole sorted result
/// (`merge_runs_to_vec`): peak memory stays at O(fan-in read buffers)
/// and the first row is ready after one tournament round. The spill
/// directory's RAII handle rides inside the stream, so client
/// disconnect / KILL drops the cursor → worker thread unwinds → tmp
/// files are removed.
#[cfg(test)]
pub(super) fn sort_plan_rows_stream(
    data_dir: &Path,
    rows: Vec<PlanRow>,
    order_by: &[OrderBy],
    plan: Option<&crate::planner::PhysicalPlan>,
) -> Result<Box<dyn super::plan::PlanStream>, QueryError> {
    sort_plan_input_stream(
        data_dir,
        Box::new(super::plan::MaterializedStream::new(rows)),
        order_by,
        plan,
    )
}

fn flush_stream_chunk(
    tmp: &SpillTmpDir,
    chunk: &mut Vec<KeyedRow>,
    chunk_bytes: &mut usize,
    fields: &[crate::engine::executor::sort::SortField],
    runs: &mut Vec<SortRun>,
    metrics: &mut SortMetrics,
    work_handle: &crate::engine::executor::work_area::WorkAreaHandle,
    mem_charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
) -> Result<(), QueryError> {
    if chunk.is_empty() {
        work_handle.release(work_handle.used_bytes());
        *mem_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        return Ok(());
    }
    let run = flush_sort_run(tmp, chunk, fields, runs.len(), "run")?;
    metrics.spill_bytes = metrics.spill_bytes.saturating_add(run.bytes);
    runs.push(run);
    *chunk_bytes = 0;
    work_handle.record_spill(work_handle.used_bytes());
    work_handle.release(work_handle.used_bytes());
    *mem_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    Ok(())
}

/// Consume an upstream plan stream directly into a bounded run builder. This
/// is the primary top-level ORDER BY path: no full input `Vec` exists before
/// the spill decision. If the stream ends while the first chunk still fits,
/// it returns the normal in-memory sort.
pub(super) fn sort_plan_input_stream<'a>(
    data_dir: &Path,
    input: Box<dyn super::plan::PlanStream + 'a>,
    order_by: &[OrderBy],
    plan: Option<&crate::planner::PhysicalPlan>,
) -> Result<Box<dyn super::plan::PlanStream + 'a>, QueryError> {
    sort_plan_input_stream_with_limit(data_dir, input, order_by, plan, None, 1)
}

pub(super) fn sort_plan_input_stream_with_parallel_merge<'a>(
    data_dir: &Path,
    input: Box<dyn super::plan::PlanStream + 'a>,
    order_by: &[OrderBy],
    degree: usize,
) -> Result<Box<dyn super::plan::PlanStream + 'a>, QueryError> {
    sort_plan_input_stream_with_limit(data_dir, input, order_by, None, None, degree)
}

fn sort_plan_input_stream_with_limit<'a>(
    data_dir: &Path,
    mut input: Box<dyn super::plan::PlanStream + 'a>,
    order_by: &[OrderBy],
    plan: Option<&crate::planner::PhysicalPlan>,
    first_run: Option<usize>,
    merge_degree: usize,
) -> Result<Box<dyn super::plan::PlanStream + 'a>, QueryError> {
    let _sorting = crate::engine::cancel::enter_process_stage(
        crate::engine::session_registry::ProcessState::SortingForOrder,
    );
    if order_by.is_empty() {
        return Ok(input);
    }
    let budget = current_budget();
    let run_bytes = run_bytes_for_budget(budget.as_deref());
    let mut fields = crate::engine::executor::sort::fields_from_order_by(order_by);
    let fanin = sort_merge_fanin().max(2);
    let first_run_bytes = first_run.unwrap_or(run_bytes);

    let work_handle = crate::engine::executor::work_area::WorkAreaHandle::register();
    let mut mem_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    let mut tmp: Option<SpillTmpDir> = None;
    let mut runs = Vec::new();
    let mut chunk = Vec::new();
    let mut chunk_bytes = 0usize;
    let mut ordinal = 0u64;
    let mut program = None;
    let mut metrics = SortMetrics::default();
    let mut head_bytes = None;

    while let Some(row) = input.next()? {
        crate::engine::cancel::check_cancelled()?;
        if ordinal == 0 {
            if order_by.iter().all(|order| {
                matches!(
                    order.expr,
                    crate::parser::Expr::Identifier(_) | crate::parser::Expr::CompoundIdentifier(_)
                )
            }) {
                head_bytes = row.fields.iter().try_fold(256usize, |bytes, field| {
                    let payload = match field.column_type {
                        crate::types::ColumnType::VarString => {
                            usize::try_from(field.source.length?)
                                .ok()?
                                .saturating_mul(4)
                        }
                        crate::types::ColumnType::Blob
                        | crate::types::ColumnType::Json
                        | crate::types::ColumnType::Geometry
                        | crate::types::ColumnType::Vector => return None,
                        _ => 128,
                    };
                    Some(
                        bytes
                            .saturating_add(payload.saturating_mul(2))
                            .saturating_add(128),
                    )
                });
            }
            fields = sort_fields_for_plan_rows(order_by, Some(&row));
            program = Some(super::eval::PlanOrderProgram::compile_once(&row, order_by));
        }
        let mut keys = program
            .as_ref()
            .expect("first row compiles keys")
            .eval_keys(&row)?;
        super::eval::record_order_by_key_rows_prepared(1);
        if row.agg_values.is_some() && !row.fields.is_empty() {
            keys.extend(row.fields.iter().map(|field| field.value.clone()));
        }
        let row_bytes = estimate_keyed_row_bytes(&row, &keys);

        if tmp.is_none() {
            let work_hint = work_handle.try_charge(row_bytes);
            let mem_result = mem_charge.add(row_bytes);
            let fits_first_run = chunk_bytes.saturating_add(row_bytes) <= first_run_bytes;
            if matches!(
                work_hint,
                crate::engine::executor::work_area::SpillHint::Continue
            ) && mem_result.is_ok()
                && fits_first_run
            {
                chunk.push(KeyedRow { keys, row, ordinal });
                ordinal = ordinal.saturating_add(1);
                chunk_bytes = chunk_bytes.saturating_add(row_bytes);
                metrics.peak_memory_bytes = metrics.peak_memory_bytes.max(chunk_bytes as u64);
                continue;
            }
            tmp = Some(SpillTmpDir::create_kind(data_dir, "sort")?);
            flush_stream_chunk(
                tmp.as_ref().unwrap(),
                &mut chunk,
                &mut chunk_bytes,
                &fields,
                &mut runs,
                &mut metrics,
                &work_handle,
                &mut mem_charge,
            )?;
        }

        let tmp_ref = tmp.as_ref().expect("spill directory created above");
        if !chunk.is_empty() && chunk_bytes.saturating_add(row_bytes) > run_bytes {
            flush_stream_chunk(
                tmp_ref,
                &mut chunk,
                &mut chunk_bytes,
                &fields,
                &mut runs,
                &mut metrics,
                &work_handle,
                &mut mem_charge,
            )?;
        }

        // Both quota and work-area pressure need a useful minimum run.
        // A parent can hold its grant throughout this sort; flushing on each
        // Spill hint turns N rows into N files and repeated merge passes.
        let minimum_run_bytes = run_bytes.min(64 * 1024);
        let mut work_hint = work_handle.try_charge(row_bytes);
        if matches!(
            work_hint,
            crate::engine::executor::work_area::SpillHint::Spill
        ) && !chunk.is_empty()
            && chunk_bytes.saturating_add(row_bytes) > minimum_run_bytes
        {
            flush_stream_chunk(
                tmp_ref,
                &mut chunk,
                &mut chunk_bytes,
                &fields,
                &mut runs,
                &mut metrics,
                &work_handle,
                &mut mem_charge,
            )?;
            work_hint = work_handle.try_charge(row_bytes);
        }
        let minimum_run = mem_charge.add(row_bytes).is_err();
        if minimum_run {
            if !chunk.is_empty() && chunk_bytes.saturating_add(row_bytes) > minimum_run_bytes {
                flush_stream_chunk(
                    tmp_ref,
                    &mut chunk,
                    &mut chunk_bytes,
                    &fields,
                    &mut runs,
                    &mut metrics,
                    &work_handle,
                    &mut mem_charge,
                )?;
            }
            // A parent may retain more than the query spill watermark.
            // Sorting still needs a bounded run to make progress. Admit at
            // most 64 KiB (or one oversized row) through the hard governor,
            // instead of failing or generating one spill file per row.
            mem_charge.add_force(row_bytes)?;
        }
        chunk.push(KeyedRow { keys, row, ordinal });
        ordinal = ordinal.saturating_add(1);
        chunk_bytes = chunk_bytes.saturating_add(row_bytes);
        metrics.peak_memory_bytes = metrics.peak_memory_bytes.max(chunk_bytes as u64);
        if (matches!(
            work_hint,
            crate::engine::executor::work_area::SpillHint::Spill
        ) && chunk_bytes >= minimum_run_bytes)
            || row_bytes >= run_bytes
        {
            flush_stream_chunk(
                tmp_ref,
                &mut chunk,
                &mut chunk_bytes,
                &fields,
                &mut runs,
                &mut metrics,
                &work_handle,
                &mut mem_charge,
            )?;
        }
    }

    let Some(tmp) = tmp else {
        report_spill_metrics(plan, &metrics);
        sort_keyed_rows(&mut chunk, &fields)?;
        let rows = chunk.into_iter().map(|entry| entry.row).collect();
        return Ok(Box::new(super::plan::MaterializedStream::new(rows)));
    };
    flush_stream_chunk(
        &tmp,
        &mut chunk,
        &mut chunk_bytes,
        &fields,
        &mut runs,
        &mut metrics,
        &work_handle,
        &mut mem_charge,
    )?;
    metrics.run_count = runs.len() as u64;
    let mut pass = 0usize;
    while runs.len() > fanin {
        let next_runs =
            merge_stream_run_pass(&tmp, &runs, &fields, pass, fanin, merge_degree, head_bytes)?;
        metrics.spill_bytes = metrics
            .spill_bytes
            .saturating_add(next_runs.iter().map(|run| run.bytes).sum::<u64>());
        remove_run_files(&tmp, &runs);
        runs = next_runs;
        pass += 1;
        metrics.merge_passes = metrics.merge_passes.saturating_add(1);
    }
    report_spill_metrics(plan, &metrics);
    drop(mem_charge);
    drop(work_handle);
    if runs.is_empty() {
        return Ok(Box::new(super::plan::MaterializedStream::new(Vec::new())));
    }
    Ok(Box::new(
        SpillMergeStream::open(tmp, &runs, &fields)?.with_plan(plan),
    ))
}

fn merge_stream_run_pass(
    tmp: &SpillTmpDir,
    runs: &[SortRun],
    fields: &[crate::engine::executor::sort::SortField],
    pass: usize,
    fanin: usize,
    degree: usize,
    head_bytes: Option<usize>,
) -> Result<Vec<SortRun>, QueryError> {
    let groups = runs.chunks(fanin).collect::<Vec<_>>();
    let file_count = fanin.saturating_mul(2);
    let buffer = crate::engine::executor::spill_quota::writer_buffer_capacity(file_count);
    let per_worker = head_bytes.map_or(usize::MAX, |head| {
        file_count
            .saturating_mul(buffer)
            .saturating_add(fanin.saturating_mul(head))
            .saturating_add(5 * 1024 * 1024)
    });
    let available = current_budget().map_or(8 * 1024 * 1024, |budget| budget.remaining()) / 2;
    let workers = degree
        .min(groups.len())
        .min((available / per_worker).max(1));
    let stacks = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_recoverable(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        workers.saturating_mul(2 * 1024 * 1024 + 4096),
    );
    if workers <= 1 || stacks.is_err() {
        return groups
            .iter()
            .enumerate()
            .map(|(index, group)| merge_runs_to_run(tmp, group, fields, pass, index))
            .collect();
    }
    let _stacks = stacks.expect("stack reservation admitted");
    #[cfg(test)]
    PARALLEL_STREAM_MERGE_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let next = std::sync::atomic::AtomicUsize::new(0);
    let budget = current_budget();
    let cancel = crate::engine::cancel::CancellationSnapshot::capture();
    let session = crate::engine::expr::current_session_expr_info();
    let tuning = crate::engine::executor::tuning::current_or_capture();
    let params = crate::engine::params::snapshot_current_context();
    let env = crate::util::env::override_snapshot();
    let collector = super::analyze_stats::current_collector_handle();
    let mut merged = std::thread::scope(|scope| -> Result<Vec<(usize, SortRun)>, QueryError> {
        let mut handles = Vec::new();
        for _ in 0..workers {
            let groups = &groups;
            let next = &next;
            let budget = budget.clone();
            let cancel = cancel.clone();
            let session = session.clone();
            let tuning = tuning.clone();
            let params = params.clone();
            let env = env.clone();
            let collector = collector.clone();
            handles.push(
                std::thread::Builder::new()
                    .stack_size(2 * 1024 * 1024)
                    .name("mydb-sort-spill".into())
                    .spawn_scoped(scope, move || {
                        let _cancel = cancel.enter();
                        let _tuning = crate::engine::executor::tuning::enter(tuning);
                        let run = || -> Result<Vec<(usize, SortRun)>, QueryError> {
                            let mut output = Vec::new();
                            loop {
                                crate::engine::cancel::check_cancelled()?;
                                let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                if index >= groups.len() {
                                    break;
                                }
                                output.push((
                                    index,
                                    merge_runs_to_run(tmp, groups[index], fields, pass, index)?,
                                ));
                            }
                            Ok(output)
                        };
                        let run = || {
                            crate::engine::expr::with_session_expr_info_option(&session, || {
                                let run = || match budget {
                                    Some(budget) => {
                                        crate::engine::executor::mem_budget::with_query_budget(
                                            budget, run,
                                        )
                                    }
                                    None => run(),
                                };
                                match params {
                                    Some(params) => params.run(run),
                                    None => run(),
                                }
                            })
                        };
                        crate::util::env::with_override_snapshot(env, || {
                            super::analyze_stats::with_collector_handle_if_present(collector, run)
                        })
                    })
                    .map_err(|error| {
                        crate::error::catalog::ER_GET_ERRNO
                            .error(format!("cannot start external sort spill worker: {error}"))
                    })?,
            );
        }
        let mut output = Vec::new();
        for handle in handles {
            output.extend(handle.join().map_err(|_| {
                crate::error::catalog::ER_GET_ERRNO.error("external sort spill worker panicked")
            })??);
        }
        Ok(output)
    })?;
    // Leaf-index ties must retain the original chronological run order.
    merged.sort_unstable_by_key(|(index, _)| *index);
    Ok(merged.into_iter().map(|(_, run)| run).collect())
}

#[cfg(test)]
static PARALLEL_STREAM_MERGE_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Final-merge-as-a-stream over the reduced run set. Holds the spill
/// tmp dir alive for as long as rows are being pulled.
struct SpillMergeStream {
    _tmp: SpillTmpDir,
    readers: Vec<SortRunReader>,
    tree: super::loser_tree::LoserTree<MergeKey, PlanRow>,
    analyze_plan_key: Option<crate::planner::PlanNodeId>,
}

impl SpillMergeStream {
    fn open(
        tmp: SpillTmpDir,
        runs: &[SortRun],
        fields: &[crate::engine::executor::sort::SortField],
    ) -> Result<Self, QueryError> {
        let fields: Arc<[crate::engine::executor::sort::SortField]> = Arc::from(fields.to_vec());
        let charger = tmp.charger();
        let mut readers: Vec<SortRunReader> = Vec::with_capacity(runs.len());
        let mut initial_heads: Vec<Option<(MergeKey, PlanRow)>> = Vec::with_capacity(runs.len());
        for run in runs {
            let Some(mut reader) =
                SortRunReader::open(run, charger.clone(), runs.len().saturating_mul(2))?
            else {
                continue;
            };
            let Some((keys, row)) = reader.next_entry()? else {
                continue;
            };
            readers.push(reader);
            initial_heads.push(Some((
                MergeKey {
                    keys,
                    fields: fields.clone(),
                },
                row,
            )));
        }
        Ok(Self {
            _tmp: tmp,
            readers,
            tree: super::loser_tree::LoserTree::new(initial_heads),
            analyze_plan_key: None,
        })
    }

    fn with_plan(mut self, plan: Option<&crate::planner::PhysicalPlan>) -> Self {
        self.analyze_plan_key = plan.and_then(super::analyze_stats::plan_key);
        self
    }
}

impl super::plan::PlanStream for SpillMergeStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        let Some((key, row, leaf)) = self.tree.pop_winner() else {
            return Ok(None);
        };
        let fields = key.fields;
        let next = self.readers[leaf]
            .next_entry()?
            .map(|(next_keys, next_row)| {
                (
                    MergeKey {
                        keys: next_keys,
                        fields,
                    },
                    next_row,
                )
            });
        self.tree.refill(leaf, next);
        Ok(Some(row))
    }
}

impl Drop for SpillMergeStream {
    fn drop(&mut self) {
        if let Some(key) = self.analyze_plan_key {
            let bytes = self.readers.iter().map(SortRunReader::bytes_read).sum();
            super::analyze_stats::add_spill_bytes_read_key(key, bytes);
        }
    }
}

fn run_bytes_for_budget(budget: Option<&crate::engine::executor::mem_budget::MemBudget>) -> usize {
    if let Some(bytes) = crate::engine::executor::tuning::current_or_capture().sort_run_bytes {
        return bytes;
    }
    let Some(budget) = budget else {
        return DEFAULT_RUN_BYTES;
    };
    let target = (budget.limit() / 4).max(ESTIMATED_ROW_BYTES * MIN_RUN_ROWS);
    target.clamp(MIN_RUN_BYTES, 64 * 1024 * 1024)
}

fn sort_merge_fanin() -> usize {
    crate::engine::executor::tuning::current_or_capture().sort_merge_fanin
}

fn flush_sort_run(
    tmp: &SpillTmpDir,
    chunk: &mut Vec<KeyedRow>,
    fields: &[crate::engine::executor::sort::SortField],
    idx: usize,
    prefix: &str,
) -> Result<SortRun, QueryError> {
    sort_keyed_rows(chunk, fields)?;
    let rows_path = tmp.path().join(format!("{prefix}-{idx}.rows"));
    let keys_path = tmp.path().join(format!("{prefix}-{idx}.keys"));
    let mut row_writer = PartitionWriter::create_charged(&rows_path, tmp.charger())?;
    let mut key_writer = SortKeyWriter::create(&keys_path, fields.len(), tmp.charger(), 2)?;
    for keyed in chunk.drain(..) {
        crate::engine::cancel::check_cancelled()?;
        key_writer.append_keys(&keyed.keys)?;
        row_writer.append_row(&keyed.row)?;
    }
    let row_bytes = row_writer.finish()?;
    let key_bytes = key_writer.finish()?;
    Ok(SortRun {
        rows_path,
        keys_path,
        bytes: row_bytes.saturating_add(key_bytes),
    })
}

fn merge_runs_to_run(
    tmp: &SpillTmpDir,
    runs: &[SortRun],
    fields: &[crate::engine::executor::sort::SortField],
    pass: usize,
    idx: usize,
) -> Result<SortRun, QueryError> {
    let rows_path = tmp.path().join(format!("merge-{pass}-{idx}.rows"));
    let keys_path = tmp.path().join(format!("merge-{pass}-{idx}.keys"));
    let mut row_writer = PartitionWriter::create_charged(&rows_path, tmp.charger())?;
    let mut key_writer = SortKeyWriter::create(&keys_path, fields.len(), tmp.charger(), 2)?;
    merge_runs(tmp.charger(), runs, fields, |keys, row| {
        key_writer.append_keys(keys)?;
        row_writer.append_row(&row)
    })?;
    let row_bytes = row_writer.finish()?;
    let key_bytes = key_writer.finish()?;
    Ok(SortRun {
        rows_path,
        keys_path,
        bytes: row_bytes.saturating_add(key_bytes),
    })
}

fn merge_runs_to_vec(
    tmp: &SpillTmpDir,
    runs: &[SortRun],
    fields: &[crate::engine::executor::sort::SortField],
    total_rows: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    let mut out = crate::engine::executor::mem_budget::try_vec_with_capacity(
        total_rows,
        "external sort merge output",
    )?;
    merge_runs(tmp.charger(), runs, fields, |_, row| {
        out.push(row);
        Ok(())
    })?;
    Ok(out)
}

/// Thin Ord wrapper over (key vector, shared SortField metadata) handed
/// to the LoserTree. We *do not* embed the run/leaf index here —
/// `LoserTree` already tie-breaks on smaller leaf index when two heads
/// compare equal, which matches the historical `BinaryHeap` behaviour
/// where `then(run.cmp)` made smaller run-idx win.
struct MergeKey {
    keys: Vec<Value>,
    fields: Arc<[crate::engine::executor::sort::SortField]>,
}
impl PartialEq for MergeKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for MergeKey {}
impl PartialOrd for MergeKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MergeKey {
    fn cmp(&self, other: &Self) -> Ordering {
        crate::engine::executor::sort::compare_sort_keys(&self.fields, &self.keys, &other.keys)
    }
}

fn merge_runs(
    charger: crate::engine::executor::spill_quota::SpillCharger,
    runs: &[SortRun],
    fields: &[crate::engine::executor::sort::SortField],
    mut emit: impl FnMut(&[Value], PlanRow) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    let fields: Arc<[crate::engine::executor::sort::SortField]> = Arc::from(fields.to_vec());
    let mut readers: Vec<SortRunReader> = Vec::with_capacity(runs.len());
    let mut initial_heads: Vec<Option<(MergeKey, PlanRow)>> = Vec::with_capacity(runs.len());
    for run in runs {
        let Some(mut reader) =
            SortRunReader::open(run, charger.clone(), runs.len().saturating_mul(2))?
        else {
            continue;
        };
        let Some((keys, row)) = reader.next_entry()? else {
            continue;
        };
        readers.push(reader);
        initial_heads.push(Some((
            MergeKey {
                keys,
                fields: fields.clone(),
            },
            row,
        )));
    }

    let mut tree = super::loser_tree::LoserTree::new(initial_heads);
    while let Some((key, row, leaf)) = tree.pop_winner() {
        crate::engine::cancel::check_cancelled()?;
        emit(&key.keys, row)?;
        let next = readers[leaf].next_entry()?.map(|(next_keys, next_row)| {
            (
                MergeKey {
                    keys: next_keys,
                    fields: fields.clone(),
                },
                next_row,
            )
        });
        tree.refill(leaf, next);
    }
    Ok(())
}

fn remove_run_files(tmp: &SpillTmpDir, runs: &[SortRun]) {
    for run in runs {
        for path in [&run.rows_path, &run.keys_path] {
            // Failed removals retain their reservation for directory cleanup.
            let _ = crate::engine::executor::spill_quota::remove_spill_file(path, &tmp.charger());
        }
    }
}

/// Spill encoding for the per-run sort *keys* file. Each row's keys
/// uses a column schema and two bits per key: NULL and type override.
/// Homogeneous non-NULL values write only their payload; a type override
/// writes a value tag before the payload. NULL-first columns are back-patched
/// on the first non-NULL value. This is a private, query-local spill format.
struct SortKeyWriter {
    writer: crate::engine::executor::spill_quota::TrackedSpillWriter,
    key_count: usize,
    /// Per-column type tag detected from the first row's values. Empty
    /// until the first `append_keys` call; non-empty thereafter. A column
    /// whose first row is NULL starts as `VALUE_NULL` and is upgraded to the
    /// real tag when the first non-NULL value arrives (see `append_keys`).
    schema: Vec<u8>,
    /// Set when a `VALUE_NULL` column tag was upgraded to a real tag after
    /// the header was already written, so `finish` back-patches the header.
    schema_upgraded: bool,
    null_bitmap: Vec<u8>,
}

impl SortKeyWriter {
    fn create(
        path: &Path,
        key_count: usize,
        charger: crate::engine::executor::spill_quota::SpillCharger,
        writer_count: usize,
    ) -> Result<Self, QueryError> {
        let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(writer_count);
        let mut writer = crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
            path, charger, capacity,
        )?;
        super::grace::write_u32(&mut writer, key_count as u32).map_err(super::grace::io_err)?;
        Ok(Self {
            writer,
            key_count,
            schema: Vec::new(),
            schema_upgraded: false,
            null_bitmap: vec![0; key_count.saturating_mul(2).div_ceil(8).max(1)],
        })
    }

    fn append_keys(&mut self, keys: &[Value]) -> Result<(), QueryError> {
        debug_assert_eq!(keys.len(), self.key_count);
        if self.schema.is_empty() && self.key_count > 0 {
            // First row: derive and write the per-column schema. For
            // each column we adopt that row's tag. A NULL tag is upgraded
            // when the first non-NULL value arrives.
            let schema: Vec<u8> = keys.iter().map(super::grace::value_tag).collect();
            self.writer
                .write_all(&schema)
                .map_err(super::grace::io_err)?;
            self.schema = schema;
        }
        // Pack NULL and override bits. We emit ceil(2*K/8) bytes so
        // readers can parse without first knowing whether any column
        // actually has a NULL.
        self.null_bitmap.fill(0);
        for (i, key) in keys.iter().enumerate() {
            if matches!(key, Value::Null) {
                self.null_bitmap[i / 8] |= 1 << (i % 8);
            } else if self.schema[i] != super::grace::VALUE_NULL
                && self.schema[i] != super::grace::value_tag(key)
            {
                let bit = self.key_count + i;
                self.null_bitmap[bit / 8] |= 1 << (bit % 8);
            }
        }
        self.writer
            .write_all(&self.null_bitmap)
            .map_err(super::grace::io_err)?;
        // Homogeneous values omit tags. Drift bits select a per-value tag,
        // preserving the same accepted Value variants as in-memory sorting.
        for (i, key) in keys.iter().enumerate() {
            if matches!(key, Value::Null) {
                continue;
            }
            let expected_tag = self.schema[i];
            let actual_tag = super::grace::value_tag(key);
            if expected_tag == super::grace::VALUE_NULL {
                // First non-NULL value for a column whose earlier rows were
                // all NULL. NULL rows wrote no payload (only the bitmap), so
                // upgrading the column's tag and back-patching the header at
                // `finish` lets the reader decode every non-NULL payload with
                // the real tag. This makes a NULL-first sort key (common in
                // GROUP BY ... WITH ROLLUP, where the rolled-up prefix columns
                // are NULL) spill correctly instead of raising a generic error.
                self.schema[i] = actual_tag;
                self.schema_upgraded = true;
            } else if expected_tag != actual_tag {
                self.writer
                    .write_all(&[actual_tag])
                    .map_err(super::grace::io_err)?;
            }
            super::grace::write_value_payload(&mut self.writer, key)
                .map_err(super::grace::io_err)?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<u64, QueryError> {
        if self.key_count > 0 && self.schema.is_empty() {
            self.writer
                .write_all(&vec![super::grace::VALUE_NULL; self.key_count])
                .map_err(super::grace::io_err)?;
        }
        // Back-patch the schema header if any column tag was upgraded from
        // VALUE_NULL after it was first written. The header sits at offset 4
        // (immediately after the u32 key_count) and is exactly `key_count`
        // bytes; overwriting it leaves the row payloads that follow intact.
        if self.schema_upgraded && !self.schema.is_empty() {
            use std::io::{Seek, SeekFrom};
            self.writer.flush().map_err(super::grace::io_err)?;
            self.writer
                .seek(SeekFrom::Start(4))
                .map_err(super::grace::io_err)?;
            self.writer
                .write_all(&self.schema)
                .map_err(super::grace::io_err)?;
            self.writer
                .seek(SeekFrom::End(0))
                .map_err(super::grace::io_err)?;
        }
        self.writer.finish()
    }
}

struct SortKeyReader {
    reader: crate::engine::executor::spill_quota::TrackedSpillReader,
    key_count: usize,
    schema: Vec<u8>,
    null_bitmap: Vec<u8>,
}

impl SortKeyReader {
    fn open(
        path: &Path,
        charger: crate::engine::executor::spill_quota::SpillCharger,
        reader_count: usize,
    ) -> Result<Option<Self>, QueryError> {
        if charger.absent_partition(path)? {
            return Ok(None);
        }
        let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(reader_count);
        let mut reader = crate::engine::executor::spill_quota::TrackedSpillReader::open(
            path, charger, capacity,
        )?;
        let Some(key_count) =
            super::grace::read_optional_u32(&mut reader).map_err(super::grace::io_err)?
        else {
            return Ok(None);
        };
        let key_count = key_count as usize;
        // Read the schema header. Empty if `key_count` is zero.
        let mut schema = vec![0u8; key_count];
        if key_count > 0 {
            // It's valid for the file to be empty here (writer was
            // closed before flushing any row): mirror the old "EOF
            // before any row" semantics by returning Ok(None).
            reader
                .read_exact(&mut schema)
                .map_err(super::grace::io_err)?;
        }
        Ok(Some(Self {
            reader,
            key_count,
            schema,
            null_bitmap: vec![0; key_count.saturating_mul(2).div_ceil(8).max(1)],
        }))
    }

    fn next_keys(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        match self.reader.read(&mut self.null_bitmap[..1]) {
            Ok(0) => return Ok(None),
            Ok(_) => {}
            Err(e) => return Err(super::grace::io_err(e)),
        }
        if self.null_bitmap.len() > 1 {
            self.reader
                .read_exact(&mut self.null_bitmap[1..])
                .map_err(super::grace::io_err)?;
        }
        let mut keys = Vec::with_capacity(self.key_count);
        for i in 0..self.key_count {
            let is_null = (self.null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
            if is_null {
                keys.push(Value::Null);
            } else {
                let bit = self.key_count + i;
                let tag = if (self.null_bitmap[bit / 8] >> (bit % 8)) & 1 == 1 {
                    let mut tag = [0u8; 1];
                    self.reader
                        .read_exact(&mut tag)
                        .map_err(super::grace::io_err)?;
                    tag[0]
                } else {
                    self.schema[i]
                };
                keys.push(super::grace::read_value_payload(&mut self.reader, tag)?);
            }
        }
        Ok(Some(keys))
    }

    fn bytes_read(&self) -> u64 {
        self.reader.bytes_read()
    }
}

struct SortRunReader {
    row_reader: PartitionReader,
    key_reader: SortKeyReader,
}

impl SortRunReader {
    fn open(
        run: &SortRun,
        charger: crate::engine::executor::spill_quota::SpillCharger,
        reader_count: usize,
    ) -> Result<Option<Self>, QueryError> {
        let Some(row_reader) =
            PartitionReader::open(&run.rows_path, charger.clone(), reader_count)?
        else {
            return Ok(None);
        };
        let Some(key_reader) = SortKeyReader::open(&run.keys_path, charger, reader_count)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            row_reader,
            key_reader,
        }))
    }

    fn next_entry(&mut self) -> Result<Option<(Vec<Value>, PlanRow)>, QueryError> {
        let keys = self.key_reader.next_keys()?;
        let row = self.row_reader.next_row()?;
        match (keys, row) {
            (None, None) => Ok(None),
            (Some(keys), Some(row)) => Ok(Some((keys, row))),
            _ => Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "sort spill run key/row length mismatch",
            )),
        }
    }

    fn bytes_read(&self) -> u64 {
        self.row_reader
            .bytes_read()
            .saturating_add(self.key_reader.bytes_read())
    }
}

fn estimate_keyed_row_bytes(row: &PlanRow, keys: &[Value]) -> usize {
    estimate_plan_row_bytes(row)
        .saturating_add(keys.iter().map(estimate_value_bytes).sum::<usize>())
        .saturating_add(32)
}

/// Cheap routing estimate used before parallel sort workers own their rows.
/// It deliberately does not evaluate SQL expressions: capability/routing
/// checks must not execute user code or hide its errors. The operator's
/// run builder accounts for evaluated keys incrementally.
pub(super) fn estimate_sort_work_bytes(rows: &[PlanRow], order_by: &[OrderBy]) -> usize {
    let key_slots = order_by.len().saturating_mul(std::mem::size_of::<Value>());
    rows.iter().fold(0usize, |total, row| {
        total.saturating_add(
            estimate_plan_row_bytes(row)
                .saturating_add(key_slots)
                .saturating_add(32),
        )
    })
}

fn estimate_plan_row_bytes(row: &PlanRow) -> usize {
    let mut bytes = 64usize;
    for field in &row.fields {
        bytes = bytes
            .saturating_add(32)
            .saturating_add(estimate_value_bytes(field.value_ref()));
    }
    bytes
}

fn estimate_value_bytes(value: &Value) -> usize {
    crate::engine::executor::mem_budget::estimate_value_bytes(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::select::plan::{ColMeta, PlanField, PlanRow};
    use crate::parser::{Expr, OrderBy};
    use crate::types::{ColumnType, Value};

    #[test]
    fn concurrent_run_merge_keeps_equal_key_input_order_and_releases_budget() {
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
            64 * 1024 * 1024,
        ));
        crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
            let tmp = SpillTmpDir::create_kind(root.path(), "sort").unwrap();
            let fields = vec![crate::engine::executor::sort::SortField {
                ascending: true,
                nulls_first: false,
                collation_id: None,
            }];
            let mut runs = Vec::new();
            for group in 0..20 {
                let mut rows = (group * 20..group * 20 + 20)
                    .map(|n| KeyedRow {
                        keys: vec![Value::Int(n % 3)],
                        row: row_int("t", "n", n),
                        ordinal: n as u64,
                    })
                    .collect::<Vec<_>>();
                runs.push(
                    flush_sort_run(&tmp, &mut rows, &fields, group as usize, "test-run").unwrap(),
                );
            }
            let hits = PARALLEL_STREAM_MERGE_HITS.load(std::sync::atomic::Ordering::Relaxed);
            let merged = merge_stream_run_pass(&tmp, &runs, &fields, 0, 2, 4, Some(512)).unwrap();
            assert!(PARALLEL_STREAM_MERGE_HITS.load(std::sync::atomic::Ordering::Relaxed) > hits);
            let result = merge_runs_to_vec(&tmp, &merged, &fields, 400).unwrap();
            let mut expected = (0..400).collect::<Vec<i64>>();
            expected.sort_by_key(|n| n % 3);
            assert_eq!(
                result
                    .into_iter()
                    .map(|row| row.fields[0].value.clone())
                    .collect::<Vec<_>>(),
                expected.into_iter().map(Value::Int).collect::<Vec<_>>()
            );
        });
        assert_eq!(budget.used(), 0);
    }

    fn row_int(table: &str, col: &str, n: i64) -> PlanRow {
        PlanRow {
            fields: vec![PlanField {
                meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta {
                    invisible: false,
                    table: table.to_string(),
                    alias: None,
                    column: col.to_string(),
                    column_type: ColumnType::LongLong,
                    source: Default::default(),
                }),
                value: Value::Int(n),
            }],
            group: None,
            agg_values: None,
            schema: PlanRow::empty_schema(),
        }
    }

    fn order_by_col(table: &str, col: &str, asc: bool) -> OrderBy {
        OrderBy {
            expr: Expr::CompoundIdentifier(vec![table.to_string(), col.to_string()]),
            asc,
            nulls_first: None,
        }
    }

    #[test]
    fn collection_sort_bounds_wide_expression_keys() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        let root = tempfile::tempdir().unwrap();
        let budget = Arc::new(MemBudget::new(128 * 1024));
        let rows = (0..1024)
            .rev()
            .map(|i| row_str("t", "s", &format!("{i:04}")))
            .collect();
        let order = [OrderBy {
            expr: Expr::Function {
                name: "REPEAT".into(),
                distinct: false,
                args: vec![
                    Expr::Identifier("s".into()),
                    Expr::Literal(crate::parser::Literal::Int(1024)),
                ],
            },
            asc: true,
            nulls_first: None,
        }];
        let output =
            with_query_budget(budget.clone(), || sort_plan_rows(root.path(), rows, &order))
                .unwrap();
        assert_eq!(output.len(), 1024);
        for (i, row) in output.iter().enumerate() {
            assert_eq!(row.fields[0].value, Value::String(format!("{i:04}")));
        }
        // 4 MiB of keys must never be retained together. The bound
        // includes 32 merge reader buffers plus the bounded run workspace.
        assert!(
            budget.peak_used() < 3 * 1024 * 1024,
            "peak={}",
            budget.peak_used()
        );
        assert!(budget.peak_used() > 4096);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
    }

    #[test]
    fn parallel_spill_shares_budget_and_cleans_runs() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        let root = tempfile::tempdir().unwrap();
        let budget = Arc::new(MemBudget::new(128 * 1024));
        let rows = (0..4000).rev().map(|i| row_int("t", "a", i)).collect();
        let output = with_query_budget(budget.clone(), || {
            sort_plan_rows_parallel_spill(
                root.path(),
                rows,
                &[order_by_col("t", "a", true)],
                4,
                None,
            )
        })
        .unwrap();
        for (i, row) in output.iter().enumerate() {
            assert_eq!(row.fields[0].value, Value::Int(i as i64));
        }
        assert_eq!(output.len(), 4000);
        assert!(budget.peak_used() > 0);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
        assert_eq!(
            std::fs::read_dir(crate::storage::naming::system_tmp_dir(root.path()))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn mixed_numeric_keys_multi_pass_match_in_memory() {
        let root = tempfile::tempdir().unwrap();
        let make_rows = || {
            (0..72)
                .rev()
                .map(|i| {
                    let mut row = row_int("t", "a", i);
                    row.fields[0].value = match i % 4 {
                        0 => Value::Int(i),
                        1 => Value::UInt(i as u64),
                        2 => Value::DecimalCompact(i * 10, 1),
                        _ => Value::Decimal128(i as i128 * 100, 2),
                    };
                    row
                })
                .collect()
        };
        let order = [order_by_col("t", "a", true)];
        let expected = in_memory_sort(make_rows(), &order).unwrap();
        let (tmp, runs, fields, metrics) =
            spill_to_runs(root.path(), make_rows(), &order, 256).unwrap();
        assert!(
            metrics.merge_passes > 0,
            "fixture must exercise intermediate merges"
        );
        let actual = merge_runs_to_vec(&tmp, &runs, &fields, expected.len()).unwrap();
        for (a, b) in actual.iter().zip(&expected) {
            assert_eq!(a.fields[0].value, b.fields[0].value);
        }
        assert_eq!(actual.len(), expected.len());
    }

    #[test]
    fn in_memory_sort_path_orders_rows_ascending() {
        let rows = vec![
            row_int("t", "a", 3),
            row_int("t", "a", 1),
            row_int("t", "a", 2),
        ];
        let sorted = in_memory_sort(rows, &[order_by_col("t", "a", true)]).unwrap();
        let values: Vec<i64> = sorted
            .iter()
            .map(|r| match r.fields[0].value {
                Value::Int(n) => n,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[test]
    fn in_memory_sort_path_orders_rows_descending() {
        let rows = vec![
            row_int("t", "a", 3),
            row_int("t", "a", 1),
            row_int("t", "a", 2),
        ];
        let sorted = in_memory_sort(rows, &[order_by_col("t", "a", false)]).unwrap();
        let values: Vec<i64> = sorted
            .iter()
            .map(|r| match r.fields[0].value {
                Value::Int(n) => n,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(values, vec![3, 2, 1]);
    }

    #[test]
    fn spill_sort_round_trips_rows_through_disk_runs() {
        // Use a tiny byte target so spill_sort_at produces multiple
        // sorted runs that need k-way merging.
        let run_bytes = 4 * 1024;
        let n = 2_000i64;
        // Insert in reverse so the sort actually has work to do.
        let rows: Vec<PlanRow> = (0..n).rev().map(|i| row_int("t", "a", i)).collect();
        let tmp = tempfile::tempdir().expect("tempdir");
        let sorted = spill_sort_at(
            tmp.path(),
            rows,
            &[order_by_col("t", "a", true)],
            run_bytes,
            None,
        )
        .expect("spill sort succeeds");
        assert_eq!(sorted.len() as i64, n);
        // Verify ascending order.
        let mut prev: i64 = i64::MIN;
        for r in &sorted {
            let cur = match r.fields[0].value {
                Value::Int(n) => n,
                _ => unreachable!(),
            };
            assert!(
                prev <= cur,
                "spill output is out of order: {} > {}",
                prev,
                cur
            );
            prev = cur;
        }
        // First and last should be the extremes.
        assert!(matches!(
            sorted.first().unwrap().fields[0].value,
            Value::Int(0)
        ));
        assert!(matches!(sorted.last().unwrap().fields[0].value, Value::Int(v) if v == n - 1));
        // Spill directory must be cleaned up by SpillTmpDir's Drop.
        let leftover = std::fs::read_dir(crate::storage::naming::system_tmp_dir(tmp.path()))
            .map(|d| d.count())
            .unwrap_or(0);
        assert_eq!(leftover, 0, "sort tmp dir not cleaned up");
    }

    #[test]
    fn spill_sort_preserves_aggregate_sidecar_metadata() {
        let mut rows: Vec<PlanRow> = (0..200).rev().map(|i| row_int("t", "g", i)).collect();
        for row in &mut rows {
            let Value::Int(group) = row.fields[0].value else {
                unreachable!()
            };
            row.group = Some(Vec::new());
            row.agg_values = Some(Arc::new(std::collections::HashMap::from([(
                7,
                Value::Int(group * 10),
            )])));
        }
        let tmp = tempfile::tempdir().unwrap();
        let sorted =
            spill_sort_at(tmp.path(), rows, &[order_by_col("t", "g", true)], 512, None).unwrap();
        for (expected, row) in sorted.iter().enumerate() {
            assert!(row.group.is_some());
            assert_eq!(
                row.agg_values.as_ref().and_then(|values| values.get(&7)),
                Some(&Value::Int(expected as i64 * 10))
            );
        }
    }

    #[test]
    fn sort_key_writer_emits_schema_header_then_null_bitmap_then_payload() {
        // Verify the on-disk layout of the typed key encoding directly:
        // schema header bytes match the first row's tags, subsequent
        // rows have a per-column NULL bitmap, NULL values consume zero
        // payload bytes, present values write payload only (no tag).
        let tmp = tempfile::tempdir().expect("tempdir");
        let spill = SpillTmpDir::create_kind(tmp.path(), "sorttest").expect("spill dir");
        let path = spill.path().join("keys.bin");
        let mut writer =
            SortKeyWriter::create(&path, 2, spill.charger(), 1).expect("create writer");
        writer
            .append_keys(&[Value::Int(7), Value::String("ab".into())])
            .expect("row 1");
        writer
            .append_keys(&[Value::Int(3), Value::Null])
            .expect("row 2 with NULL");
        writer
            .append_keys(&[Value::Int(9), Value::String("cd".into())])
            .expect("row 3");
        let bytes_written = writer.finish().expect("finish");
        assert!(bytes_written > 0);

        // Read back via the matching reader.
        let mut reader = SortKeyReader::open(&path, spill.charger(), 1)
            .expect("open")
            .expect("Some");
        let row1 = reader.next_keys().unwrap().unwrap();
        let row2 = reader.next_keys().unwrap().unwrap();
        let row3 = reader.next_keys().unwrap().unwrap();
        assert!(reader.next_keys().unwrap().is_none(), "no more rows");
        assert_eq!(row1, vec![Value::Int(7), Value::String("ab".into())]);
        assert_eq!(row2, vec![Value::Int(3), Value::Null]);
        assert_eq!(row3, vec![Value::Int(9), Value::String("cd".into())]);
    }

    #[test]
    fn sort_key_writer_roundtrips_per_row_type_drift() {
        let tmp = tempfile::tempdir().unwrap();
        let spill = SpillTmpDir::create_kind(tmp.path(), "sorttest").unwrap();
        let path = spill.path().join("keys.bin");
        let values = vec![
            Value::Null,
            Value::Int(-1),
            Value::UInt(2),
            Value::Float(3.5),
            Value::Double(4.5),
            Value::DecimalCompact(55, 1),
            Value::Decimal128(650, 2),
            Value::DecimalWide(Box::new(crate::types::DecimalWide {
                magnitude: num_bigint::BigInt::from(123456789012345678901234567890i128),
                scale: 4,
            })),
            Value::Timestamp(123456789),
            Value::Date(123),
            Value::Time(-987654),
            Value::DateTime(123456789),
            Value::Year(24),
            Value::Decimal("123.45".into()),
            Value::Bytes(vec![0, 255, 1]),
            Value::Json(vec![1, 2]),
            Value::Geometry(vec![3, 4]),
            Value::Vector(vec![5, 6]),
            Value::String("seven".into()),
            Value::Null,
        ];
        // Exercise both bitmap bytes, NULL-first schema upgrades and drift
        // back to the original variant, without losing exact representations.
        let mut writer = SortKeyWriter::create(&path, 9, spill.charger(), 1).unwrap();
        for value in &values {
            writer.append_keys(&vec![value.clone(); 9]).unwrap();
        }
        writer.finish().unwrap();
        let mut reader = SortKeyReader::open(&path, spill.charger(), 1)
            .unwrap()
            .unwrap();
        for value in values {
            assert_eq!(reader.next_keys().unwrap().unwrap(), vec![value; 9]);
        }
        assert!(reader.next_keys().unwrap().is_none());
    }

    #[test]
    fn sort_key_writer_handles_all_columns_null() {
        // A row where every key is NULL must still round-trip; the
        // schema header records each column as VALUE_NULL and the
        // reader's bitmap-only path yields all Nulls.
        let tmp = tempfile::tempdir().expect("tempdir");
        let spill = SpillTmpDir::create_kind(tmp.path(), "sorttest").expect("spill dir");
        let path = spill.path().join("keys.bin");
        let mut writer = SortKeyWriter::create(&path, 3, spill.charger(), 1).expect("create");
        writer
            .append_keys(&[Value::Null, Value::Null, Value::Null])
            .expect("all null row");
        writer.finish().expect("finish");

        let mut reader = SortKeyReader::open(&path, spill.charger(), 1)
            .expect("open")
            .expect("Some");
        let row = reader.next_keys().unwrap().unwrap();
        assert_eq!(row, vec![Value::Null, Value::Null, Value::Null]);
        assert!(reader.next_keys().unwrap().is_none());
    }

    #[test]
    fn empty_order_by_returns_input_unchanged() {
        let rows = vec![row_int("t", "a", 9), row_int("t", "a", 1)];
        let sorted = in_memory_sort(rows, &[]).unwrap();
        let values: Vec<i64> = sorted
            .iter()
            .map(|r| match r.fields[0].value {
                Value::Int(n) => n,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(values, vec![9, 1]);
    }

    fn row_str(table: &str, col: &str, s: &str) -> PlanRow {
        PlanRow {
            fields: vec![PlanField {
                meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta {
                    invisible: false,
                    table: table.to_string(),
                    alias: None,
                    column: col.to_string(),
                    column_type: ColumnType::VarString,
                    source: Default::default(),
                }),
                value: Value::String(s.to_string()),
            }],
            group: None,
            agg_values: None,
            schema: PlanRow::empty_schema(),
        }
    }

    /// Phase D regression: streaming merge (`SpillMergeStream`) must produce exactly the same order
    /// as materialized merge (`merge_runs_to_vec`), and dropping the stream must remove
    /// its spill temporary directory through RAII.
    #[test]
    fn streaming_spill_matches_materialized_and_cleans_tmp() {
        let dir = tempfile::tempdir().unwrap();
        // Several runs with repeated keys suffice for merge/cleanup coverage.
        let n = 256i64;
        let make_rows =
            || -> Vec<PlanRow> { (0..n).map(|i| row_int("t", "a", (i * 7919) % 64)).collect() };
        let order = [order_by_col("t", "a", true)];
        let baseline = sort_plan_rows(dir.path(), make_rows(), &order).unwrap();
        let run_bytes = 4 * 1024;
        let (tmp, runs, fields, _metrics) =
            spill_to_runs(dir.path(), make_rows(), &order, run_bytes).unwrap();
        assert!(runs.len() > 1, "expected multiple runs, got {}", runs.len());
        let tmp_path = tmp.path().to_path_buf();
        assert!(tmp_path.exists());
        let mut stream = SpillMergeStream::open(tmp, &runs, &fields).unwrap();
        let mut streamed = Vec::new();
        while let Some(row) =
            crate::engine::executor::select::plan::PlanStream::next(&mut stream).unwrap()
        {
            streamed.push(row);
        }
        assert_eq!(streamed.len(), baseline.len());
        for (s, b) in streamed.iter().zip(baseline.iter()) {
            assert_eq!(s.fields[0].value, b.fields[0].value);
        }
        // Dropping the stream drops SpillTmpDir and removes the directory.
        drop(stream);
        assert!(!tmp_path.exists(), "spill tmp dir must be removed on drop");
    }

    /// Exercise the public budget-triggered path with a small budget and large working set;
    /// streaming and materialized outputs must have identical total order.
    #[test]
    fn sort_plan_rows_stream_spills_under_tight_budget() {
        let dir = tempfile::tempdir().unwrap();
        // Large string keys make sort_work_bytes exceed the run_bytes lower bound.
        let n = 512i64;
        let make_rows = || -> Vec<PlanRow> {
            (0..n)
                .map(|i| {
                    row_str(
                        "t",
                        "s",
                        &format!("payload-{:06}-{}", (i * 7919) % 5000, "x".repeat(200)),
                    )
                })
                .collect()
        };
        let order = [order_by_col("t", "s", true)];
        let baseline = sort_plan_rows(dir.path(), make_rows(), &order).unwrap();
        let budget = std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
            64 * 1024,
        ));
        let plan = crate::planner::PhysicalPlan::ValuesScan {
            rows: Vec::new(),
            columns: Vec::new(),
            filter: None,
            estimated_rows: n as f64,
            cost: crate::engine::cbo::Cost::scalar(1.0),
        };
        let (stats, result) = super::super::analyze_stats::with_collector(|| {
            super::super::analyze_stats::register_execution_plan(&plan)?;
            crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
                let mut stream =
                    sort_plan_rows_stream(dir.path(), make_rows(), &order, Some(&plan))?;
                let mut streamed = Vec::new();
                while let Some(row) = stream.next()? {
                    streamed.push(row);
                }
                Ok(streamed)
            })
        });
        let streamed = result.unwrap();
        let measured = super::super::analyze_stats::lookup(&stats, &plan).unwrap();
        assert!(measured.sort_runs > 1 && measured.spill_bytes > 0);
        assert_eq!(budget.used(), 0);
        assert_eq!(streamed.len(), baseline.len());
        for (s, b) in streamed.iter().zip(baseline.iter()) {
            assert_eq!(s.fields[0].value, b.fields[0].value);
        }
    }

    #[test]
    fn work_area_pressure_keeps_minimum_runs_instead_of_one_file_per_row() {
        use crate::engine::executor::work_area::WorkAreaHandle;
        let target = crate::engine::executor::work_area::global_manager().target_bytes();
        let parent = WorkAreaHandle::register();
        let _ = parent.try_charge(target);
        let root = tempfile::tempdir().unwrap();
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
            2 * 1024 * 1024,
        ));
        let plan = crate::planner::PhysicalPlan::ValuesScan {
            rows: Vec::new(),
            columns: Vec::new(),
            filter: None,
            estimated_rows: 6000.0,
            cost: crate::engine::cbo::Cost::scalar(1.0),
        };
        let (stats, result) = super::super::analyze_stats::with_collector(|| {
            super::super::analyze_stats::register_execution_plan(&plan)?;
            crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
                let rows = (0..6000).rev().map(|i| row_int("t", "a", i)).collect();
                let mut output = sort_plan_input_stream(
                    root.path(),
                    Box::new(super::super::plan::MaterializedStream::new(rows)),
                    &[order_by_col("t", "a", true)],
                    Some(&plan),
                )?;
                for expected in 0..6000 {
                    assert_eq!(
                        output.next()?.unwrap().fields[0].value,
                        Value::Int(expected)
                    );
                }
                assert!(output.next()?.is_none());
                Ok(())
            })
        });
        result.unwrap();
        let measured = super::super::analyze_stats::lookup(&stats, &plan).unwrap();
        assert!(
            measured.sort_runs > 1 && measured.sort_runs < 100,
            "runs={}",
            measured.sort_runs
        );
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn direct_input_stream_builds_bounded_runs_and_cleans_owner() {
        let root = tempfile::tempdir().unwrap();
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
            256 * 1024,
        ));
        let rows: Vec<PlanRow> = (0..6000)
            .rev()
            .map(|value| row_int("t", "a", value))
            .collect();
        let order = vec![OrderBy {
            expr: Expr::Identifier("a".into()),
            asc: true,
            nulls_first: None,
        }];
        crate::engine::executor::mem_budget::with_query_budget(Arc::clone(&budget), || {
            let input: Box<dyn super::super::plan::PlanStream> =
                Box::new(super::super::plan::MaterializedStream::new(rows));
            let mut output = sort_plan_input_stream(root.path(), input, &order, None).unwrap();
            let mut previous = -1i64;
            let mut count = 0usize;
            while let Some(row) = output.next().unwrap() {
                let Value::Int(value) = row.fields[0].value else {
                    panic!("unexpected value")
                };
                assert!(value >= previous);
                previous = value;
                count += 1;
            }
            assert_eq!(count, 6000);
            drop(output);
        });
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
        let leftover = std::fs::read_dir(crate::storage::naming::system_tmp_dir(root.path()))
            .map(|entries| entries.count())
            .unwrap_or(0);
        assert_eq!(leftover, 0);
    }

    /// P0-3 — an external sort whose run files would exceed
    /// `MYDB_TEMP_DISK_LIMIT` must fail with 1041 rather than writing
    /// unbounded spill files. (nextest runs each test in its own
    /// process, so the env / OnceLock cache is isolated.)
    #[test]
    fn spill_to_runs_respects_temp_disk_limit() {
        std::env::set_var("MYDB_TEMP_DISK_LIMIT", "65536"); // 64 KiB
        let dir = tempfile::tempdir().unwrap();
        let n = 6_000i64;
        let rows: Vec<PlanRow> = (0..n)
            .map(|i| {
                row_str(
                    "t",
                    "s",
                    &format!("payload-{:06}-{}", (i * 7919) % 5000, "x".repeat(64)),
                )
            })
            .collect();
        let order = [order_by_col("t", "s", true)];
        // Small run_bytes → many runs, total well over 64 KiB.
        let before = crate::engine::executor::spill_quota::global_spilled_bytes();
        let err = spill_to_runs(dir.path(), rows, &order, 4 * 1024);
        match err {
            Err(e) => assert_eq!(e.code(), 1041, "expected 1041, got {e:?}"),
            Ok(_) => panic!("spill_to_runs should have exceeded the temp-disk limit"),
        }
        // The SpillTmpDir returned inside the Err path is dropped, so the
        // counter returns to where it started — no leak on rejection.
        assert_eq!(
            crate::engine::executor::spill_quota::global_spilled_bytes(),
            before,
            "spill bytes must be released after a quota rejection"
        );
    }

    /// P0-3 — with a generous limit the same spill completes, and all
    /// charged bytes are released once the runs are consumed / dropped.
    #[test]
    fn spill_to_runs_releases_under_generous_limit() {
        std::env::set_var("MYDB_TEMP_DISK_LIMIT", "1073741824"); // 1 GiB
        let dir = tempfile::tempdir().unwrap();
        // Only multiple live runs are needed to verify quota release.
        let n = 64i64;
        let make_rows = || -> Vec<PlanRow> {
            (0..n)
                .map(|i| row_str("t", "s", &format!("payload-{:06}", (i * 7919) % 5000)))
                .collect()
        };
        let order = [order_by_col("t", "s", true)];
        let before = crate::engine::executor::spill_quota::global_spilled_bytes();
        let (tmp, runs, _fields, _m) =
            spill_to_runs(dir.path(), make_rows(), &order, 2 * 1024).unwrap();
        assert!(runs.len() > 1, "expected multiple runs");
        // While the runs are live the quota reflects their bytes.
        assert!(
            crate::engine::executor::spill_quota::global_spilled_bytes() > before,
            "live runs should be charged"
        );
        drop(tmp); // RAII removes files and releases the charge.
        assert_eq!(
            crate::engine::executor::spill_quota::global_spilled_bytes(),
            before,
            "all spill bytes released after the sort dir drops"
        );
    }
}
