//! window execution and preparation.

use super::*;

/// Execute a `WindowAgg` plan node. Materialises the input rows, then
/// computes each window function in-place and appends the result as a
/// virtual column on every PlanRow.
///
/// Sort sharing: windows with identical `(partition_by, order_by)` are
/// grouped so the partition+sort work runs once per group.
/// Evaluate co-partitioned windows without retaining the complete input and
/// output at once. Both clustering and ordinal restoration use bounded sort
/// runs. Restoring ordinals preserves the WindowAgg input-order contract.
pub(in crate::engine::executor::select) fn window_partition_input_stream<'a>(
    db: &'a Arc<Database>,
    input: Box<dyn PlanStream + 'a>,
    windows: &'a [crate::planner::PhysicalWindowSpec],
    plan: &'a PhysicalPlan,
    ordinal_name: String,
    ordinal_position: usize,
) -> Result<Box<dyn PlanStream + 'a>, QueryError> {
    // A row ranked below K within any input chunk is also below K globally:
    // adding rows cannot improve its rank. Discard only these proven losers
    // before sorting the wide input by partition. Final window evaluation
    // still determines ranks over the surviving complete candidate set.
    let input = window_rank_prefilter(input, windows);
    struct Ordinals<'a> {
        input: Box<dyn PlanStream + 'a>,
        meta: ColMetaRef,
        ordinal: i64,
    }
    impl PlanStream for Ordinals<'_> {
        fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
            let Some(mut row) = self.input.next()? else {
                return Ok(None);
            };
            row.push_field(PlanField {
                meta: self.meta.clone(),
                value: Value::Int(self.ordinal),
            });
            self.ordinal = self.ordinal.checked_add(1).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "window input ordinal overflow",
                )
            })?;
            Ok(Some(row))
        }
    }
    struct Partitions<'a> {
        db: &'a Arc<Database>,
        input: Box<dyn PlanStream + 'a>,
        windows: &'a [crate::planner::PhysicalWindowSpec],
        plan: &'a PhysicalPlan,
        order: Vec<OrderBy>,
        pending: Option<PlanRow>,
        output: std::vec::IntoIter<PlanRow>,
        charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
        target_bytes: usize,
    }
    impl PlanStream for Partitions<'_> {
        fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
            loop {
                if let Some(row) = self.output.next() {
                    return Ok(Some(row));
                }
                self.output = Vec::new().into_iter();
                self.charge = None;
                let first = match self.pending.take() {
                    Some(row) => row,
                    None => match self.input.next()? {
                        Some(row) => row,
                        None => return Ok(None),
                    },
                };
                let fields = crate::engine::executor::select::eval::sort_fields_for_plan_rows(
                    &self.order,
                    Some(&first),
                );
                let mut key =
                    crate::engine::executor::select::eval::precompute_sort_keys_for_plan_row(
                        &first,
                        &self.order,
                    )?;
                let mut rows = Vec::new();
                let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
                let mut retained_bytes = 0usize;
                let mut row = first;
                loop {
                    // Cover the retained row and its evaluation copy before
                    // collecting another input row. The hard reserve leaves
                    // a clear error for an indivisible oversized partition.
                    let bytes = std::mem::size_of::<PlanRow>()
                        + row.fields.capacity() * std::mem::size_of::<PlanField>()
                        + row
                            .fields
                            .iter()
                            .map(|f| {
                                crate::engine::executor::mem_budget::estimate_value_bytes(&f.value)
                            })
                            .sum::<usize>();
                    charge.add_force(bytes.saturating_mul(2))?;
                    retained_bytes = retained_bytes.saturating_add(bytes.saturating_mul(2));
                    rows.push(row);
                    let Some(next) = self.input.next()? else {
                        break;
                    };
                    let next_key =
                        crate::engine::executor::select::eval::precompute_sort_keys_for_plan_row(
                            &next,
                            &self.order,
                        )?;
                    if crate::engine::executor::sort::compare_sort_keys(&fields, &key, &next_key)
                        != std::cmp::Ordering::Equal
                    {
                        // Amortize window/schema preparation across small,
                        // complete partitions. Never split a SQL partition:
                        // frames, peers and Top-K still see all of its rows.
                        if retained_bytes >= self.target_bytes {
                            self.pending = Some(next);
                            break;
                        }
                        key = next_key;
                    }
                    row = next;
                }
                self.output =
                    execute_window_agg(self.db, rows, self.windows, self.plan)?.into_iter();
                self.charge = Some(charge);
            }
        }
    }
    struct RemoveOrdinal<'a> {
        input: Box<dyn PlanStream + 'a>,
        position: usize,
    }
    impl PlanStream for RemoveOrdinal<'_> {
        fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
            Ok(self.input.next()?.map(|mut row| {
                row.fields.remove(self.position);
                row.schema = PlanRow::empty_schema();
                row
            }))
        }
    }
    let order: Vec<OrderBy> = windows[0]
        .partition_by
        .iter()
        .map(|expr| OrderBy {
            expr: expr.clone(),
            asc: true,
            nulls_first: None,
        })
        .collect();
    let input = Ordinals {
        input,
        ordinal: 0,
        meta: intern_col_meta(ColMeta {
            column: ordinal_name.clone(),
            column_type: crate::types::ColumnType::LongLong,
            ..Default::default()
        }),
    };
    let clustered = crate::engine::executor::select::external_sort::sort_plan_input_stream(
        db.data_dir(),
        Box::new(input),
        &order,
        Some(plan),
    )?;
    let partitions = Partitions {
        db,
        input: clustered,
        windows,
        plan,
        order,
        pending: None,
        output: Vec::new().into_iter(),
        charge: None,
        target_bytes: crate::engine::executor::mem_budget::current_budget()
            .map_or(64 * 1024, |budget| {
                (budget.limit() / 16).clamp(1024, 64 * 1024)
            }),
    };
    let restored = crate::engine::executor::select::external_sort::sort_plan_input_stream(
        db.data_dir(),
        Box::new(partitions),
        &[OrderBy {
            expr: Expr::Identifier(ordinal_name),
            asc: true,
            nulls_first: None,
        }],
        Some(plan),
    )?;
    Ok(Box::new(RemoveOrdinal {
        input: restored,
        position: ordinal_position,
    }))
}

#[cfg(test)]
pub(in crate::engine::executor) static WINDOW_RANK_PREFILTER_REJECTED:
    std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub(super) fn window_rank_prefilter<'a>(
    input: Box<dyn PlanStream + 'a>,
    windows: &'a [crate::planner::PhysicalWindowSpec],
) -> Box<dyn PlanStream + 'a> {
    use crate::engine::executor::dispatch::{compute_window_partitions, RankKind};
    let Some(window) = windows.first().filter(|_| windows.len() == 1) else {
        return input;
    };
    let Some((k, kind)) = window.top_k.zip(RankKind::from_name(&window.name)) else {
        return input;
    };
    struct Candidates<'a> {
        input: Box<dyn PlanStream + 'a>,
        windows: &'a [crate::planner::PhysicalWindowSpec],
        k: usize,
        kind: RankKind,
        target: usize,
        pending: Option<PlanRow>,
        output: std::vec::IntoIter<PlanRow>,
        charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
        disabled: bool,
    }
    impl PlanStream for Candidates<'_> {
        fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
            loop {
                if let Some(row) = self.output.next() {
                    return Ok(Some(row));
                }
                self.output = Vec::new().into_iter();
                self.charge = None;
                if self.disabled {
                    return self
                        .pending
                        .take()
                        .map_or_else(|| self.input.next(), |row| Ok(Some(row)));
                }
                let mut rows = Vec::new();
                let mut bytes = 0usize;
                let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
                loop {
                    crate::engine::cancel::check_cancelled()?;
                    let next = match self.pending.take() {
                        Some(row) => Some(row),
                        None => self.input.next()?,
                    };
                    let Some(row) = next else {
                        break;
                    };
                    // Cover the row, projected key copy and partition/index
                    // scratch before buffering. On pressure, pass the original
                    // stream through; no restart or lost input is required.
                    let row_bytes = (estimate_row_bytes(&row) as usize).saturating_mul(3);
                    if !rows.is_empty() && bytes.saturating_add(row_bytes) > self.target {
                        self.pending = Some(row);
                        break;
                    }
                    if charge.add(row_bytes).is_err() {
                        self.pending = Some(row);
                        self.disabled = true;
                        break;
                    }
                    bytes = bytes.saturating_add(row_bytes);
                    rows.push(row);
                }
                if rows.is_empty() {
                    if self.disabled {
                        continue;
                    }
                    return Ok(None);
                }
                if !self.disabled {
                    let columns = rows[0]
                        .fields
                        .iter()
                        .map(|field| Column {
                            source: field.source.clone(),
                            name: field.column.clone(),
                            column_type: field.column_type,
                        })
                        .collect::<Vec<_>>();
                    let projection = window_group_input(&columns, self.windows, &[0]);
                    let values = rows
                        .iter()
                        .map(|row| {
                            projection
                                .positions
                                .iter()
                                .map(|&position| row.fields[position].value.clone())
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();
                    let window = &self.windows[0];
                    let partitions = compute_window_partitions(
                        &projection.columns,
                        &values,
                        &window.partition_by,
                        &window.order_by,
                        Some((self.k, self.kind)),
                    )?;
                    let mut keep = vec![false; rows.len()];
                    for index in partitions.iter().flatten() {
                        keep[*index] = true;
                    }
                    // Stop prefiltering high-cardinality/unselective input;
                    // retaining candidates from this chunk is still exact.
                    self.disabled = keep.iter().filter(|&&v| v).count() * 4 >= rows.len() * 3;
                    let mut index = 0usize;
                    #[cfg(test)]
                    WINDOW_RANK_PREFILTER_REJECTED.fetch_add(
                        keep.iter().filter(|&&value| !value).count(),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    rows.retain(|_| {
                        let retain = keep[index];
                        index += 1;
                        retain
                    });
                }
                self.output = rows.into_iter();
                self.charge = Some(charge);
            }
        }
    }
    Box::new(Candidates {
        input,
        windows,
        k,
        kind,
        target: crate::engine::executor::mem_budget::current_budget()
            .map_or(8 * 1024 * 1024, |budget| {
                (budget.limit() / 8).clamp(1024, 8 * 1024 * 1024)
            }),
        pending: None,
        output: Vec::new().into_iter(),
        charge: None,
        disabled: false,
    })
}

pub(in crate::engine::executor::select) fn execute_window_agg(
    db: &Arc<Database>,
    rows: Vec<PlanRow>,
    windows: &[crate::planner::PhysicalWindowSpec],
    plan: &PhysicalPlan,
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::engine::executor::dispatch::{
        compute_window_partitions, evaluate_window_with_partitions,
    };
    if rows.is_empty() {
        return Ok(rows);
    }
    // Release the partition / per-window value materialisation when this
    // operator body returns rather than leaving it on the budget.
    let _materialization_scope = MaterializationChargeScope::enter();
    let columns: Vec<Column> = rows[0]
        .fields
        .iter()
        .map(|f| Column {
            // Plumb the catalog-derived wire metadata from
            // ColMeta.source so window-function result columns
            // expose real flags / length to JDBC.
            source: f.source.clone(),
            name: f.column.clone(),
            column_type: f.column_type,
        })
        .collect();
    // Group windows by (partition_by, order_by) for sort sharing. Windows
    // in the same group reuse a single partition+sort pass.
    let mut group_keys: Vec<String> = Vec::new();
    let mut group_members: Vec<Vec<usize>> = Vec::new();
    for (i, w) in windows.iter().enumerate() {
        crate::engine::cancel::check_cancelled()?;
        let key = format!("{:?}|{:?}", w.partition_by, w.order_by);
        if let Some(pos) = group_keys.iter().position(|k| k == &key) {
            group_members[pos].push(i);
        } else {
            group_keys.push(key);
            group_members.push(vec![i]);
        }
    }
    let group_inputs: Vec<WindowGroupInput> = group_members
        .iter()
        .map(|member_indices| window_group_input(&columns, windows, member_indices))
        .collect();
    // Estimate the in-memory working copy (`value_rows`, a full clone of the
    // input) without building it. A window is a blocking operator, but the
    // *output* reuses the original `rows`; the clone is purely the eval
    // working set. When it would exceed the per-query budget (TPC-DS Q51/Q67:
    // a window over a wide ~75-178 MiB join/rollup result), take the
    // spill path (§4.5 / docs) — sort-cluster the input by partition key to
    // disk and process one partition at a time, so only a single partition's
    // values are resident. At a roomy budget the clone fits and the in-memory
    // path runs unchanged (no regression).
    let est_value_rows_bytes: usize = group_inputs
        .iter()
        .map(|input| estimate_window_group_rows_bytes(&rows, &input.positions))
        .max()
        .unwrap_or(0);
    // The output slots survive every partition pass. Admit them before the
    // optional evaluation copy so concurrent CTE workers cannot both spend
    // the same snapshot of remaining memory.
    let _output_charge = crate::engine::executor::mem_budget::force_charge_scoped(
        rows.len()
            .saturating_mul(windows.len())
            .saturating_mul(std::mem::size_of::<Value>()),
    )?;
    let copy_charge = (|| {
        let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        charge.add(est_value_rows_bytes)?;
        Ok::<_, QueryError>(charge)
    })();
    let want_spill = !window_spill_disabled() && copy_charge.is_err();
    let _copy_charge = if want_spill { None } else { Some(copy_charge?) };

    // Top-N pushdown: honoured only for a single ranking window in the
    // in-memory path. `compute_window_partitions` truncates each partition to
    // its `rk <= k` prefix; the surviving row indices are then the only rows
    // emitted (the rest would be dropped by the enclosing `WHERE rk <= k`).
    let top_k = if windows.len() == 1 && !want_spill {
        windows[0].top_k.and_then(|k| {
            crate::engine::executor::dispatch::RankKind::from_name(&windows[0].name)
                .map(|kind| (k, kind))
        })
    } else {
        None
    };
    let mut kept_indices: Option<Vec<usize>> = None;
    let mut peak_partition_rows = 0u64;

    let mut window_values: Vec<Vec<Value>> = vec![Vec::new(); windows.len()];
    if want_spill {
        for (member_indices, group_input) in group_members.iter().zip(&group_inputs) {
            crate::engine::cancel::check_cancelled()?;
            let leader = &windows[member_indices[0]];
            for &wi in member_indices {
                crate::engine::cancel::check_cancelled()?;
                window_values[wi] = vec![Value::Null; rows.len()];
            }
            window_group_values_spilled(
                db,
                &rows,
                &group_input.columns,
                &group_input.positions,
                leader,
                member_indices,
                windows,
                &mut window_values,
            )?;
        }
    } else {
        for (member_indices, group_input) in group_members.iter().zip(&group_inputs) {
            crate::engine::cancel::check_cancelled()?;
            let mut value_rows: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
            for (index, row) in rows.iter().enumerate() {
                if index % 256 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                value_rows.push(
                    group_input
                        .positions
                        .iter()
                        .map(|&pos| row.fields[pos].value.clone())
                        .collect(),
                );
            }
            let value_rows_bytes = value_rows.iter().fold(0usize, |acc, row| {
                acc.saturating_add(crate::engine::executor::mem_budget::estimate_row_bytes(row))
            });
            debug_assert!(value_rows_bytes <= est_value_rows_bytes);
            // Pick the first window's keys; all windows in this group share them.
            let leader = &windows[member_indices[0]];
            let partitions = compute_window_partitions(
                &group_input.columns,
                &value_rows,
                &leader.partition_by,
                &leader.order_by,
                top_k,
            )?;
            peak_partition_rows =
                peak_partition_rows.max(partitions.iter().map(Vec::len).max().unwrap_or(0) as u64);
            if top_k.is_some() {
                // Single-window group (top_k only set when windows.len()==1):
                // the kept rows are exactly the (truncated) partition members.
                kept_indices = Some(partitions.iter().flatten().copied().collect());
            }
            for &w_idx in member_indices {
                crate::engine::cancel::check_cancelled()?;
                let w = &windows[w_idx];
                let synth_expr = Expr::WindowFunction {
                    name: w.name.clone(),
                    args: w.args.clone(),
                    spec: Box::new(crate::parser::WindowSpec {
                        partition_by: w.partition_by.clone(),
                        order_by: w.order_by.clone(),
                        frame: w.frame.clone(),
                        inherits: None,
                    }),
                };
                let values = evaluate_window_with_partitions(
                    &synth_expr,
                    &partitions,
                    &group_input.columns,
                    &value_rows,
                )?;
                let values_bytes = values.iter().fold(0usize, |acc, value| {
                    acc.saturating_add(crate::engine::executor::mem_budget::estimate_value_bytes(
                        value,
                    ))
                });
                let inline_bytes = values
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Value>());
                if values_bytes > inline_bytes {
                    MATERIALIZATION_CHARGE_STACK.with(|stack| {
                        stack
                            .borrow_mut()
                            .last_mut()
                            .expect("window scope")
                            .add_force(values_bytes - inline_bytes)
                    })?;
                }
                window_values[w_idx] = values;
            }
        }
    }
    // Append window result columns to each PlanRow. We call
    // `push_field` (not the bare `fields.push`) so the lookup cache
    // gets reset; otherwise a previously-built schema lookup would not
    // know about the new window output column.
    let output_metas: Vec<_> = windows
        .iter()
        .map(|w| {
            intern_col_meta(ColMeta {
                invisible: false,
                table: String::new(),
                alias: None,
                column: w.output_alias.clone(),
                column_type: w.output_type,
                source: Default::default(),
            })
        })
        .collect();
    if let Some(kept) = kept_indices {
        // Top-N pushdown: emit only the surviving rows, in original input
        // order (the enclosing query's ORDER BY re-sorts downstream).
        let mut mask = vec![false; rows.len()];
        for &i in &kept {
            crate::engine::cancel::check_cancelled()?;
            mask[i] = true;
        }
        let mut out = Vec::with_capacity(kept.len());
        for (row_idx, mut row) in rows.into_iter().enumerate() {
            crate::engine::cancel::check_cancelled()?;
            if !mask[row_idx] {
                continue;
            }
            for (w_idx, meta) in output_metas.iter().enumerate() {
                crate::engine::cancel::check_cancelled()?;
                row.push_field(PlanField {
                    meta: meta.clone(),
                    value: std::mem::replace(&mut window_values[w_idx][row_idx], Value::Null),
                });
            }
            out.push(row);
        }
        crate::engine::executor::select::analyze_stats::add_window_runtime(
            plan,
            if want_spill {
                "partition_spool"
            } else {
                "range_state"
            },
            0,
            peak_partition_rows,
        );
        return Ok(out);
    }
    let mut out = rows;
    for (w_idx, meta) in output_metas.iter().enumerate() {
        crate::engine::cancel::check_cancelled()?;
        for (row_idx, row) in out.iter_mut().enumerate() {
            crate::engine::cancel::check_cancelled()?;
            row.push_field(PlanField {
                meta: meta.clone(),
                value: std::mem::replace(&mut window_values[w_idx][row_idx], Value::Null),
            });
        }
    }
    crate::engine::executor::select::analyze_stats::add_window_runtime(
        plan,
        if want_spill {
            "partition_spool"
        } else {
            "range_state"
        },
        0,
        peak_partition_rows,
    );
    Ok(out)
}

/// LIMIT-aware window evaluation. The child keeps its original row order, so
/// the requested output ordinals are known before window work begins. For
/// each shared window specification we retain only the complete partitions
/// containing those ordinals, evaluate them with the canonical frame engine,
/// and scatter just the requested results. Unbounded-following remains exact
/// because a selected partition is retained in full; unrelated partitions
/// never allocate result slots.
pub(super) fn execute_window_agg_limited(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    windows: &[crate::planner::PhysicalWindowSpec],
    limit: usize,
    offset: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::engine::executor::dispatch::{
        compute_window_partitions, evaluate_window_with_partitions,
    };
    use crate::engine::expr::ValueKey;

    if limit == 0 {
        return Ok(Vec::new());
    }
    let requested_end = offset.saturating_add(limit);
    let mut stream = execute_plan_stream(db, txn, input)?;
    // The caller caps `requested_end` at 4096. Keep this small prefix until
    // every requested row's partition key is known; rows before OFFSET may
    // belong to one of those partitions and therefore cannot be discarded
    // before the last requested key has been observed.
    let mut prefix = Vec::with_capacity(requested_end);
    while prefix.len() < requested_end {
        let Some(row) = stream.next()? else {
            break;
        };
        prefix.push(row);
    }
    if offset >= prefix.len() {
        return Ok(Vec::new());
    }
    let end = requested_end.min(prefix.len());
    let target_indices: Vec<usize> = (offset..end).collect();
    let target_position: hashbrown::HashMap<usize, usize, ahash::RandomState> = target_indices
        .iter()
        .copied()
        .enumerate()
        .map(|(position, index)| (index, position))
        .collect();
    let columns: Vec<Column> = prefix[0]
        .fields
        .iter()
        .map(|field| Column {
            source: field.source.clone(),
            name: field.column.clone(),
            column_type: field.column_type,
        })
        .collect();

    let mut group_keys: Vec<String> = Vec::new();
    let mut group_members: Vec<Vec<usize>> = Vec::new();
    for (index, window) in windows.iter().enumerate() {
        crate::engine::cancel::check_cancelled()?;
        let key = format!("{:?}|{:?}", window.partition_by, window.order_by);
        if let Some(position) = group_keys.iter().position(|candidate| candidate == &key) {
            group_members[position].push(index);
        } else {
            group_keys.push(key);
            group_members.push(vec![index]);
        }
    }

    struct LimitedWindowGroup {
        member_indices: Vec<usize>,
        input: WindowGroupInput,
        wanted: hashbrown::HashSet<Vec<ValueKey>, ahash::RandomState>,
        rows: Vec<Vec<Value>>,
        original_indices: Vec<usize>,
    }

    let mut groups = Vec::with_capacity(group_members.len());
    for member_indices in group_members {
        crate::engine::cancel::check_cancelled()?;
        let group_input = window_group_input(&columns, windows, &member_indices);
        let leader = &windows[member_indices[0]];
        let mut wanted: hashbrown::HashSet<Vec<ValueKey>, ahash::RandomState> =
            hashbrown::HashSet::with_hasher(ahash::RandomState::new());
        for &index in &target_indices {
            crate::engine::cancel::check_cancelled()?;
            let key = leader
                .partition_by
                .iter()
                .map(|expr| {
                    eval_plan_expr(expr, &prefix[index]).map(|value| ValueKey::from(&value))
                })
                .collect::<Result<Vec<_>, _>>()?;
            wanted.insert(key);
        }
        groups.push(LimitedWindowGroup {
            member_indices,
            input: group_input,
            wanted,
            rows: Vec::new(),
            original_indices: Vec::new(),
        });
    }

    let mut target_values = vec![vec![Value::Null; target_indices.len()]; windows.len()];
    let mut memory = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    let mut original_index = 0usize;
    let mut consume = |row: &PlanRow, original_index: usize| -> Result<(), QueryError> {
        for group in &mut groups {
            crate::engine::cancel::check_cancelled()?;
            let leader = &windows[group.member_indices[0]];
            let key = leader
                .partition_by
                .iter()
                .map(|expr| eval_plan_expr(expr, row).map(|value| ValueKey::from(&value)))
                .collect::<Result<Vec<_>, _>>()?;
            if !group.wanted.contains(&key) {
                continue;
            }
            let bytes = group
                .input
                .positions
                .iter()
                .map(|&position| {
                    crate::engine::executor::mem_budget::estimate_value_bytes(
                        &row.fields[position].value,
                    )
                })
                .sum::<usize>()
                .saturating_add(std::mem::size_of::<Vec<Value>>())
                .saturating_add(std::mem::size_of::<usize>());
            memory.add(bytes)?;
            group.rows.push(
                group
                    .input
                    .positions
                    .iter()
                    .map(|&position| row.fields[position].value.clone())
                    .collect(),
            );
            group.original_indices.push(original_index);
        }
        Ok(())
    };
    for row in &prefix {
        crate::engine::cancel::check_cancelled()?;
        consume(row, original_index)?;
        original_index += 1;
    }
    while let Some(row) = stream.next()? {
        consume(&row, original_index)?;
        original_index += 1;
    }
    for group in &groups {
        crate::engine::cancel::check_cancelled()?;
        let member_indices = &group.member_indices;
        let leader = &windows[member_indices[0]];
        let partitions = compute_window_partitions(
            &group.input.columns,
            &group.rows,
            &leader.partition_by,
            &leader.order_by,
            None,
        )?;
        for &window_index in member_indices {
            crate::engine::cancel::check_cancelled()?;
            let window = &windows[window_index];
            let expression = Expr::WindowFunction {
                name: window.name.clone(),
                args: window.args.clone(),
                spec: Box::new(crate::parser::WindowSpec {
                    partition_by: window.partition_by.clone(),
                    order_by: window.order_by.clone(),
                    frame: window.frame.clone(),
                    inherits: None,
                }),
            };
            let values = evaluate_window_with_partitions(
                &expression,
                &partitions,
                &group.input.columns,
                &group.rows,
            )?;
            for (subset_index, original_index) in group.original_indices.iter().copied().enumerate()
            {
                if let Some(&target) = target_position.get(&original_index) {
                    target_values[window_index][target] = values[subset_index].clone();
                }
            }
        }
    }

    let metas: Vec<ColMetaRef> = windows
        .iter()
        .map(|window| {
            intern_col_meta(ColMeta {
                invisible: false,
                table: String::new(),
                alias: None,
                column: window.output_alias.clone(),
                column_type: window.output_type,
                source: Default::default(),
            })
        })
        .collect();
    let mut output = Vec::with_capacity(target_indices.len());
    for (position, mut row) in prefix
        .into_iter()
        .skip(offset)
        .take(end.saturating_sub(offset))
        .enumerate()
    {
        for (window_index, meta) in metas.iter().cloned().enumerate() {
            crate::engine::cancel::check_cancelled()?;
            row.push_field(PlanField {
                meta,
                value: target_values[window_index][position].clone(),
            });
        }
        output.push(row);
    }
    Ok(output)
}

pub(super) fn window_spill_disabled() -> bool {
    crate::util::env::var("MYDB_WINDOW_SPILL_DISABLE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
        .unwrap_or(false)
}

pub(super) struct WindowGroupInput {
    pub(super) columns: Vec<Column>,
    pub(super) positions: Vec<usize>,
}

pub(super) fn window_group_input(
    columns: &[Column],
    windows: &[crate::planner::PhysicalWindowSpec],
    member_indices: &[usize],
) -> WindowGroupInput {
    let mut needed = std::collections::HashSet::new();
    for &wi in member_indices {
        needed.extend(crate::planner::window_referenced_columns(
            &windows[wi].partition_by,
            &windows[wi].order_by,
            &windows[wi].args,
        ));
    }
    if needed.is_empty() {
        return WindowGroupInput {
            columns: Vec::new(),
            positions: Vec::new(),
        };
    }
    let mut positions: Vec<usize> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for (i, column) in columns.iter().enumerate() {
        let lower = column.name.to_ascii_lowercase();
        if needed.contains(&lower) && seen.insert(lower) {
            positions.push(i);
        }
    }
    if positions.len() == columns.len() {
        return WindowGroupInput {
            columns: columns.to_vec(),
            positions: (0..columns.len()).collect(),
        };
    }
    WindowGroupInput {
        columns: positions.iter().map(|&i| columns[i].clone()).collect(),
        positions,
    }
}

pub(super) fn estimate_window_group_rows_bytes(rows: &[PlanRow], positions: &[usize]) -> usize {
    rows.iter()
        .map(|r| {
            positions
                .iter()
                .filter_map(|&pos| r.fields.get(pos))
                .map(|f| crate::engine::executor::mem_budget::estimate_value_bytes(&f.value))
                .sum::<usize>()
                .saturating_add(std::mem::size_of::<Vec<Value>>())
        })
        .sum()
}

/// Spilling window evaluation for one `(partition_by, order_by)` group.
/// Builds a lightweight copy of the input holding only the columns the
/// group's windows reference (`window_referenced_columns`) plus a row-index
/// column, sort-clusters it by partition key to disk
/// (`sort_plan_rows_stream`, which spills internally), then processes one
/// partition at a time. Each buffered partition is fed to the *same*
/// `compute_window_partitions` + `evaluate_window_with_partitions` the
/// in-memory path uses — so frame / rank / tie / NULL-order semantics are
/// byte-identical — and results scatter back into `window_values` by
/// original row index. Only one partition's values are resident at a time,
/// so a window whose working set dwarfs the budget completes by spilling
/// instead of raising 1037 (TPC-DS Q51/Q67).
#[allow(clippy::too_many_arguments)]
pub(super) fn window_group_values_spilled(
    db: &Arc<Database>,
    rows: &[PlanRow],
    columns: &[Column],
    positions: &[usize],
    leader: &crate::planner::PhysicalWindowSpec,
    member_indices: &[usize],
    windows: &[crate::planner::PhysicalWindowSpec],
    window_values: &mut [Vec<Value>],
) -> Result<(), QueryError> {
    use crate::engine::expr::ValueKey;

    // Lite schema = referenced columns (kept under their real names so the
    // window expressions resolve identically) + a trailing row-index column.
    let idx_name = "__mydb_win_row_idx";
    let mut lite_columns: Vec<Column> = columns.to_vec();
    lite_columns.push(Column {
        source: Default::default(),
        name: idx_name.to_string(),
        column_type: crate::types::ColumnType::LongLong,
    });
    let idx_pos = lite_columns.len() - 1;

    struct WindowLiteInput<'a> {
        rows: std::slice::Iter<'a, PlanRow>,
        positions: &'a [usize],
        columns: &'a [Column],
        ordinal: usize,
    }
    impl PlanStream for WindowLiteInput<'_> {
        fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
            crate::engine::cancel::check_cancelled()?;
            let Some(row) = self.rows.next() else {
                return Ok(None);
            };
            let mut fields = self
                .positions
                .iter()
                .zip(self.columns)
                .map(|(&p, column)| PlanField {
                    meta: intern_col_meta(ColMeta {
                        column: column.name.clone(),
                        column_type: column.column_type,
                        source: column.source.clone(),
                        ..Default::default()
                    }),
                    value: row.fields[p].value.clone(),
                })
                .collect::<Vec<_>>();
            fields.push(PlanField {
                meta: intern_col_meta(ColMeta {
                    column: "__mydb_win_row_idx".into(),
                    column_type: crate::types::ColumnType::LongLong,
                    ..Default::default()
                }),
                value: Value::Int(self.ordinal as i64),
            });
            self.ordinal += 1;
            Ok(Some(PlanRow::from_fields(fields)))
        }
    }
    let input = WindowLiteInput {
        rows: rows.iter(),
        positions,
        columns,
        ordinal: 0,
    };

    // Cluster rows of the same partition together (the order key is appended
    // only so the stream is deterministic; the per-partition re-sort below is
    // what the frame logic relies on). The stream spills internally.
    let mut order_by: Vec<OrderBy> = leader
        .partition_by
        .iter()
        .map(|e| OrderBy {
            expr: e.clone(),
            asc: true,
            nulls_first: None,
        })
        .collect();
    order_by.extend(leader.order_by.iter().cloned());
    let mut stream = crate::engine::executor::select::external_sort::sort_plan_input_stream(
        db.data_dir(),
        Box::new(input),
        &order_by,
        None,
    )?;

    let mut buf: Vec<PlanRow> = Vec::new();
    let mut buf_key: Option<Vec<ValueKey>> = None;

    fn flush_partition(
        buf: &mut Vec<PlanRow>,
        idx_pos: usize,
        lite_columns: &[Column],
        leader: &crate::planner::PhysicalWindowSpec,
        member_indices: &[usize],
        windows: &[crate::planner::PhysicalWindowSpec],
        window_values: &mut [Vec<Value>],
    ) -> Result<(), QueryError> {
        use crate::engine::executor::dispatch::{
            compute_window_partitions, evaluate_window_with_partitions,
        };
        if buf.is_empty() {
            return Ok(());
        }
        let mut part_values: Vec<Vec<Value>> = Vec::with_capacity(buf.len());
        for (index, row) in buf.iter().enumerate() {
            if index % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            part_values.push(row.fields.iter().map(|field| field.value.clone()).collect());
        }
        let orig_idx: Vec<usize> = buf
            .iter()
            .map(|r| match &r.fields[idx_pos].value {
                Value::Int(v) => *v as usize,
                _ => 0,
            })
            .collect();
        // Reuse the in-memory partition + eval on this buffered partition so
        // frame semantics match exactly. `compute_window_partitions` re-derives
        // the order-sorted partition(s) from `part_values`, so even if the
        // stream clustered a coarser/finer grouping it is corrected here.
        let partitions = compute_window_partitions(
            lite_columns,
            &part_values,
            &leader.partition_by,
            &leader.order_by,
            None,
        )?;
        for &wi in member_indices {
            crate::engine::cancel::check_cancelled()?;
            let w = &windows[wi];
            let synth = Expr::WindowFunction {
                name: w.name.clone(),
                args: w.args.clone(),
                spec: Box::new(crate::parser::WindowSpec {
                    partition_by: w.partition_by.clone(),
                    order_by: w.order_by.clone(),
                    frame: w.frame.clone(),
                    inherits: None,
                }),
            };
            let vals =
                evaluate_window_with_partitions(&synth, &partitions, lite_columns, &part_values)?;
            for (pos, val) in vals.into_iter().enumerate() {
                crate::engine::cancel::check_cancelled()?;
                window_values[wi][orig_idx[pos]] = val;
            }
        }
        buf.clear();
        Ok(())
    }

    while let Some(lite_row) = stream.next()? {
        crate::engine::cancel::check_cancelled()?;
        let pkey: Vec<ValueKey> = leader
            .partition_by
            .iter()
            .map(|e| eval_plan_expr(e, &lite_row).map(|v| ValueKey::from(&v)))
            .collect::<Result<Vec<_>, _>>()?;
        let same = matches!(&buf_key, Some(prev) if *prev == pkey);
        if !same {
            flush_partition(
                &mut buf,
                idx_pos,
                &lite_columns,
                leader,
                member_indices,
                windows,
                window_values,
            )?;
            buf_key = Some(pkey);
        }
        buf.push(lite_row);
    }
    flush_partition(
        &mut buf,
        idx_pos,
        &lite_columns,
        leader,
        member_indices,
        windows,
        window_values,
    )?;
    Ok(())
}

// ============================================================
// Phase B #B3: pull-model adapters.
//
// `PlanStream` is a thin row-at-a-time interface over a physical
// plan. The first pass keeps the existing `execute_plan_rows`
// (collection-mode) intact and wraps its output in
// `MaterializedStream`. As true streaming sources land (e.g., a
// SeqScan that walks `scan_rows_limited` lazily), individual
// adapters can swap in without reshaping the consumer side.
// ============================================================
