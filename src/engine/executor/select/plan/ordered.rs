//! ordered execution and preparation.

use super::*;

/// Phase B #B4: streaming aggregate. Walks `input` rows in their
/// natural order — the planner only emits this node when the input
/// is provably sorted on `group_by` — and emits one output row per
/// run of equal group keys without allocating a hash table.
///
/// The emitted row shape mirrors `execute_hash_aggregate` so the
/// downstream projection can reuse the same `$group.group_N` /
/// `$aggregate.$rows` lookup logic.
pub(super) struct CompactOrderScan<'a> {
    pub(super) table: &'a PlanTable,
    pub(super) scan_columns: Vec<usize>,
    pub(super) key_positions: Vec<usize>,
    pub(super) templates: Vec<PlanField>,
    pub(super) fields: Vec<crate::engine::executor::sort::SortField>,
}

#[derive(Clone)]
pub(super) struct CompactScanPayload {
    pub(super) values: Vec<Value>,
}

impl crate::engine::executor::memory_footprint::AllocatedBytes for CompactScanPayload {
    fn allocated_bytes(&self) -> usize {
        self.values.allocated_bytes()
    }
}

pub(super) struct CompactWindowEntry {
    pub(super) payload: CompactScanPayload,
    pub(super) ordinal: u64,
}

pub(super) fn try_compact_order_scan<'a>(
    input: &'a PhysicalPlan,
    order_by: &[OrderBy],
) -> Option<CompactOrderScan<'a>> {
    let PhysicalPlan::SeqScan {
        table,
        filter,
        runtime_filters,
        projected_columns,
        ..
    } = input
    else {
        return None;
    };
    if filter.is_some() || !runtime_filters.is_empty() || order_by.is_empty() {
        return None;
    }
    let scan_columns: Vec<usize> = projected_columns
        .as_ref()
        .map(|cols| cols.as_ref().clone())
        .unwrap_or_else(|| logical_visible_column_indices(&table.schema));
    let mut key_positions = Vec::with_capacity(order_by.len());
    for item in order_by {
        let column = simple_order_column_name(&item.expr)?;
        let table_col = table
            .schema
            .columns
            .iter()
            .position(|col| col.name.eq_ignore_ascii_case(column))?;
        let scan_pos = scan_columns.iter().position(|idx| *idx == table_col)?;
        key_positions.push(scan_pos);
    }
    let templates = scan_columns
        .iter()
        .map(|&idx| {
            let column = &table.schema.columns[idx];
            PlanField {
                meta: intern_col_meta(ColMeta {
                    invisible: column.invisible,
                    table: table.schema.name.clone(),
                    alias: table.alias.clone(),
                    column: column.name.clone(),
                    column_type: table.schema.result_type(idx),
                    source: Default::default(),
                }),
                value: Value::Null,
            }
        })
        .collect();
    Some(CompactOrderScan {
        table,
        scan_columns,
        key_positions,
        templates,
        fields: crate::engine::executor::sort::fields_from_order_by(order_by),
    })
}

pub(super) fn simple_order_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(name) => Some(name.as_str()),
        Expr::CompoundIdentifier(parts) => parts.last().map(String::as_str),
        _ => None,
    }
}

pub(super) fn compact_payload_to_plan_row(
    layout: &CompactOrderScan<'_>,
    payload: CompactScanPayload,
) -> PlanRow {
    let mut fields = layout.templates.clone();
    for (field, value) in fields.iter_mut().zip(payload.values.into_iter()) {
        field.value = value;
    }
    PlanRow {
        fields,
        group: None,
        agg_values: None,
        schema: PlanRow::empty_schema(),
    }
}

pub(super) fn try_execute_compact_topk_simple_scan(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    order_by: &[OrderBy],
    k: usize,
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    let Some(layout) = try_compact_order_scan(input, order_by) else {
        return Ok(None);
    };
    let qualified = db.storage_key_for_table(&layout.table.schema);
    let mut topk = crate::engine::executor::sort::BoundedTopK::new(&layout.fields, k)?;
    run_scan_with_partition_pruning(&layout.table.schema, &None, || {
        db.scan_rows_columns_streaming_until(&qualified, txn, &layout.scan_columns, |values| {
            crate::engine::cancel::check_cancelled()?;
            let keys = layout
                .key_positions
                .iter()
                .map(|pos| values[*pos].clone())
                .collect::<Vec<_>>();
            if topk.would_accept(&keys) {
                topk.push(
                    CompactScanPayload {
                        values: values.to_vec(),
                    },
                    keys,
                )?;
            }
            Ok(false)
        })
    })?;
    let rows = topk
        .into_sorted_vec()?
        .into_iter()
        .map(|payload| compact_payload_to_plan_row(&layout, payload))
        .collect();
    Ok(Some(rows))
}

pub(super) fn try_execute_compact_topwindow_simple_scan(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    order_by: &[OrderBy],
    limit: usize,
    offset: usize,
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    let Some(layout) = try_compact_order_scan(input, order_by) else {
        return Ok(None);
    };
    if limit == 0 {
        return Ok(Some(Vec::new()));
    }
    let qualified = db.storage_key_for_table(&layout.table.schema);
    let window = offset.saturating_add(limit);
    if window <= 8_192 {
        let mut topk = crate::engine::executor::sort::BoundedTopK::new(&layout.fields, window)?;
        run_scan_with_partition_pruning(&layout.table.schema, &None, || {
            db.scan_rows_columns_streaming_until(&qualified, txn, &layout.scan_columns, |values| {
                crate::engine::cancel::check_cancelled()?;
                let keys = layout
                    .key_positions
                    .iter()
                    .map(|pos| values[*pos].clone())
                    .collect::<Vec<_>>();
                if topk.would_accept(&keys) {
                    topk.push(
                        CompactScanPayload {
                            values: values.to_vec(),
                        },
                        keys,
                    )?;
                }
                Ok(false)
            })
        })?;
        let rows = topk
            .into_sorted_vec()?
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|payload| compact_payload_to_plan_row(&layout, payload))
            .collect();
        return Ok(Some(rows));
    }
    if compact_topwindow_parallel_enabled() && window <= 262_144 {
        if let Some(rows) =
            try_execute_parallel_compact_topwindow(db, txn, &layout, &qualified, limit, offset)?
        {
            return Ok(Some(rows));
        }
    }
    let mut entries: Vec<CompactWindowEntry> = Vec::new();
    let mut ordinal = 0u64;
    run_scan_with_partition_pruning(&layout.table.schema, &None, || {
        db.scan_rows_columns_streaming_until(&qualified, txn, &layout.scan_columns, |values| {
            crate::engine::cancel::check_cancelled()?;
            entries.push(CompactWindowEntry {
                payload: CompactScanPayload {
                    values: values.to_vec(),
                },
                ordinal,
            });
            ordinal = ordinal.saturating_add(1);
            Ok(false)
        })
    })?;
    if offset >= entries.len() {
        return Ok(Some(Vec::new()));
    }
    let end = offset.saturating_add(limit).min(entries.len());
    let page_len = end - offset;
    let directions: Vec<bool> = layout.fields.iter().map(|field| field.ascending).collect();
    let numeric_selected = crate::engine::executor::sort::try_select_nth_by_int_keys_multi(
        &mut entries,
        &directions,
        offset,
        |entry, col| compact_window_entry_i64_key(&layout, entry, col),
    );
    if numeric_selected {
        if offset + page_len < entries.len() {
            entries[offset..].select_nth_unstable_by(page_len, |left, right| {
                compare_compact_window_entries_i64(&layout, left, right)
                    .unwrap_or_else(|| compare_compact_window_entries(&layout, left, right))
            });
        }
        let mut page: Vec<CompactWindowEntry> = entries.drain(offset..end).collect();
        if !crate::engine::executor::sort::try_sort_by_int_keys_multi(
            &mut page,
            &directions,
            |entry, col| compact_window_entry_i64_key(&layout, entry, col),
        ) {
            page.sort_unstable_by(|left, right| {
                compare_compact_window_entries(&layout, left, right)
            });
        }
        let rows = page
            .into_iter()
            .map(|entry| compact_payload_to_plan_row(&layout, entry.payload))
            .collect();
        return Ok(Some(rows));
    }
    let compare = |left: &CompactWindowEntry, right: &CompactWindowEntry| {
        compare_compact_window_entries(&layout, left, right)
    };
    entries.select_nth_unstable_by(offset, compare);
    if offset + page_len < entries.len() {
        entries[offset..].select_nth_unstable_by(page_len, compare);
    }
    let mut page: Vec<CompactWindowEntry> = entries.drain(offset..end).collect();
    page.sort_unstable_by(compare);
    let rows = page
        .into_iter()
        .map(|entry| compact_payload_to_plan_row(&layout, entry.payload))
        .collect();
    Ok(Some(rows))
}

pub(super) fn compare_compact_window_entries(
    layout: &CompactOrderScan<'_>,
    left: &CompactWindowEntry,
    right: &CompactWindowEntry,
) -> std::cmp::Ordering {
    for (field, key_pos) in layout.fields.iter().zip(layout.key_positions.iter()) {
        let ord = crate::engine::executor::sort::compare_value_for_sort(
            left.payload.values.get(*key_pos).unwrap_or(&Value::Null),
            right.payload.values.get(*key_pos).unwrap_or(&Value::Null),
            *field,
        );
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    left.ordinal.cmp(&right.ordinal)
}

pub(super) fn compact_window_entry_i64_key(
    layout: &CompactOrderScan<'_>,
    entry: &CompactWindowEntry,
    col: usize,
) -> Option<i64> {
    let key_pos = *layout.key_positions.get(col)?;
    match entry.payload.values.get(key_pos)? {
        Value::Int(v) => Some(*v),
        Value::UInt(v) => i64::try_from(*v).ok(),
        Value::DecimalCompact(mag, _scale) => Some(*mag),
        Value::Date(v) => Some(*v as i64),
        Value::DateTime(v) | Value::Time(v) => Some(*v),
        Value::Year(v) => Some(*v as i64),
        _ => None,
    }
}

pub(super) fn compare_compact_window_entries_i64(
    layout: &CompactOrderScan<'_>,
    left: &CompactWindowEntry,
    right: &CompactWindowEntry,
) -> Option<std::cmp::Ordering> {
    for (col, field) in layout.fields.iter().enumerate() {
        let l = compact_window_entry_i64_key(layout, left, col)?;
        let r = compact_window_entry_i64_key(layout, right, col)?;
        let ord = l.cmp(&r);
        if ord != std::cmp::Ordering::Equal {
            return Some(if field.ascending { ord } else { ord.reverse() });
        }
    }
    Some(left.ordinal.cmp(&right.ordinal))
}

pub(super) fn compact_topwindow_parallel_enabled() -> bool {
    crate::util::env::var("MYDB_COMPACT_TOPWINDOW_PARALLEL")
        .map(|v| matches!(v.as_str(), "1" | "on" | "ON" | "true" | "TRUE"))
        .unwrap_or(false)
}

pub(super) fn compact_topwindow_parallel_degree() -> usize {
    let configured = crate::planner::default_parallel_degree() as usize;
    if configured <= 1 {
        return 1;
    }
    crate::util::env::var("MYDB_PARALLEL_DEGREE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(4)
        .min(8)
}

pub(super) fn try_execute_parallel_compact_topwindow(
    db: &Arc<Database>,
    txn: TxnView,
    layout: &CompactOrderScan<'_>,
    qualified: &str,
    limit: usize,
    offset: usize,
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    if layout.table.schema.is_partitioned() {
        return Ok(None);
    }
    let degree = compact_topwindow_parallel_degree();
    if degree <= 1 {
        return Ok(None);
    }
    let page_count = db.heap_page_count_by_name(qualified)?;
    if page_count < (degree as u64).saturating_mul(64) {
        return Ok(None);
    }
    let window = offset.saturating_add(limit);
    let pages_per_worker = page_count.div_ceil(degree as u64);
    let mut ranges = Vec::new();
    let mut start = 0u64;
    while start < page_count {
        let end = start.saturating_add(pages_per_worker).min(page_count);
        ranges.push((start, end));
        start = end;
    }
    if ranges.len() <= 1 {
        return Ok(None);
    }

    let mut per_worker: Vec<
        Result<crate::engine::executor::sort::TopKCandidates<CompactScanPayload>, QueryError>,
    > = (0..ranges.len())
        .map(|_| {
            Err(error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "TopK worker not started",
            ))
        })
        .collect();
    let execution_context =
        crate::engine::executor::select::execution_context::ExecutionContext::capture(txn);
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(ranges.len());
        for (page_lo, page_hi) in ranges {
            let worker_context = execution_context.clone();
            handles.push(scope.spawn(
                move || -> Result<crate::engine::executor::sort::TopKCandidates<CompactScanPayload>, QueryError> {
                    worker_context.run(db, || {
                    let mut topk =
                        crate::engine::executor::sort::BoundedTopK::new(&layout.fields, window)?;
                    db.scan_rows_columns_streaming_range(
                        qualified,
                        txn,
                        &layout.scan_columns,
                        page_lo,
                        page_hi,
                        |values| {
                            crate::engine::cancel::check_cancelled()?;
                            let keys = layout
                                .key_positions
                                .iter()
                                .map(|pos| values[*pos].clone())
                                .collect::<Vec<_>>();
                            if topk.would_accept(&keys) {
                                topk.push(
                                    CompactScanPayload {
                                        values: values.to_vec(),
                                    },
                                    keys,
                                )?;
                            }
                            Ok(())
                        },
                    )?;
                    Ok(topk.drain_sorted())
                    })
                },
            ));
        }
        for (idx, handle) in handles.into_iter().enumerate() {
            per_worker[idx] = match handle.join() {
                Ok(result) => result,
                Err(_) => Err(error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "compact top-window worker panicked",
                )),
            };
        }
    });

    let mut topk = crate::engine::executor::sort::BoundedTopK::new(&layout.fields, window)?;
    for worker in per_worker {
        for candidate in worker? {
            topk.push_candidate(candidate)?;
        }
    }
    let rows = topk
        .into_sorted_vec()?
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|payload| compact_payload_to_plan_row(layout, payload))
        .collect();
    Ok(Some(rows))
}

/// Phase B: bounded-memory ORDER BY + LIMIT operator. Maintains a
/// max-heap of size at most `k` so peak memory is O(k) rather than
/// O(input_row_count). The result is sorted ascending per `order_by`.
///
/// Implementation note: we want the *worst* row of the top-k window to
/// be the heap root (so it can be popped on insertion of a better row).
/// `BinaryHeap` is a max-heap, so we wrap entries in a comparator that
/// orders by `compare_plan_order_by` so the "largest" entry is the
/// row that should be evicted first.
pub(in crate::engine::executor::select) fn execute_topk(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    order_by: &[OrderBy],
    k: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    if k == 0 {
        // Drain the input for any side effects, return empty.
        let _ = execute_plan_rows(db, txn, input)?;
        return Ok(Vec::new());
    }

    if let Some(rows) = try_execute_compact_topk_simple_scan(db, txn, input, order_by, k)? {
        return Ok(rows);
    }

    // Phase 7 streaming fast path: when `MYDB_VECTORIZED` allows it and the
    // input shape is one the vec engine can handle, fold TopK and any
    // wrapped HashAggregate / Sort into a single streaming pass. The
    // bridge returns `None` for unsupported shapes (HAVING, complex
    // group-by, expression aggregates, etc.) and we fall back to the
    // row-based heap below.
    if let Some(rows) = crate::engine::executor::select::vec_bridge::try_vec_topn(
        db,
        txn,
        input,
        order_by,
        Some(k),
        None,
    )? {
        return Ok(rows);
    }

    let fields = crate::engine::executor::sort::fields_from_order_by(order_by);
    let mut topk = crate::engine::executor::sort::BoundedTopK::new(&fields, k)?;
    let mut stream = execute_plan_stream(db, txn, input)?;
    // Scratch buffer for the candidate's sort keys. We reuse it across
    // rows so a stream of N rows allocates O(K) key buffers instead of
    // O(N) — only winners' keys are cloned into the heap. For the common
    // shape `ORDER BY a, b LIMIT K` over a large scan, this cuts the
    // per-row Vec<Value> alloc that previously dominated the loop.
    let mut scratch: Vec<Value> = Vec::with_capacity(order_by.len());
    let mut prepared_rows = 0u64;
    while let Some(row) = stream.next()? {
        crate::engine::cancel::check_cancelled()?;
        scratch.clear();
        for item in order_by {
            scratch.push(
                crate::engine::executor::select::eval::eval_row_or_aggregate_expr(
                    &item.expr, &row,
                )?,
            );
        }
        prepared_rows = prepared_rows.saturating_add(1);
        if topk.would_accept(&scratch) {
            // Winner: hand the heap an owned clone, keep `scratch` alive
            // for the next row.
            topk.push(row, scratch.clone())?;
        }
    }
    crate::engine::executor::select::eval::ORDER_BY_KEY_ROWS_PREPARED
        .fetch_add(prepared_rows, std::sync::atomic::Ordering::Relaxed);
    Ok(topk.into_sorted_vec()?)
}

pub(super) struct DistinctTopKPayload {
    pub(super) row: PlanRow,
    pub(super) key: std::sync::Arc<[crate::engine::expr::ValueKey]>,
    pub(super) key_charge: usize,
}

impl crate::engine::executor::memory_footprint::AllocatedBytes for DistinctTopKPayload {
    fn allocated_bytes(&self) -> usize {
        self.row.allocated_bytes().saturating_add(self.key_charge)
    }
}

/// Consume an order-providing index a small leaf window at a time and stop as
/// soon as K distinct projected tuples have survived MVCC and the residual.
/// The ordinary IndexRangeScan materializer intentionally uses large MRR
/// windows; that is ideal for an unbounded scan but would read the complete
/// index before a DISTINCT TopK can observe its first row.
pub(super) fn try_execute_ordered_index_distinct_topk(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    order_by: &[OrderBy],
    projection: &[SelectItem],
    k: usize,
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    let (table, index, low, high, filter, reverse, projected_columns, covering) = match input {
        PhysicalPlan::IndexRangeScan {
            table,
            index,
            low,
            high,
            filter,
            reverse,
            projected_columns,
            covering,
            ..
        } => (
            table,
            index,
            low,
            high,
            filter,
            *reverse,
            projected_columns,
            *covering,
        ),
        PhysicalPlan::IndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            reverse,
            projected_columns,
            ..
        } => (
            table,
            index,
            low,
            high,
            filter,
            *reverse,
            projected_columns,
            true,
        ),
        _ => return Ok(None),
    };
    if !crate::planner::plan_satisfies_order_by(input, order_by)
        || table.schema.is_partitioned()
        || db.has_pending_index_repairs_for(table.schema.id, &index.name)
    {
        return Ok(None);
    }

    use crate::engine::executor::mem_budget;
    use crate::engine::executor::memory_footprint::AllocatedBytes;
    use crate::engine::expr::ValueKey;

    let mut dedup = crate::engine::executor::sort::TopKSet::new();
    let mut dedup_charge = mem_budget::ScopedCharge::new(0);

    let heap = index_scan_heap(db, table)?;
    let projection_columns = projected_columns
        .as_deref()
        .map(|columns| columns.as_slice());
    let projector = IndexEntryProjector::new(table, index, projection_columns);
    let mut cursor = if reverse {
        AnyRangeCursor::Reverse(db.range_index_snapshot_reverse_cursor(
            &table.schema,
            index,
            low.clone(),
            high.clone(),
            txn,
        )?)
    } else {
        AnyRangeCursor::Forward(db.range_index_snapshot_cursor(
            &table.schema,
            index,
            low.clone(),
            high.clone(),
            txn,
        )?)
    };
    let mut seen_row_ids = (!index.unique).then(std::collections::HashSet::new);
    let mut eval_ctx = EvalQueryContext::default();
    let mut compiled_filter = None;
    let mut rows = Vec::new();
    let mut examined = 0u64;
    const CHUNK: usize = 128;

    while rows.len() < k {
        let candidates = if covering {
            let entries = cursor.next_chunk(CHUNK)?;
            if entries.is_empty() {
                break;
            }
            examined = examined.saturating_add(entries.len() as u64);
            let visible = db.filter_visible_index_entries_mask_with_handle(
                &heap,
                &table.schema,
                index,
                &entries,
                txn,
            )?;
            entries
                .into_iter()
                .zip(visible)
                .filter_map(|((key, row_id), visible)| {
                    if !visible
                        || seen_row_ids
                            .as_mut()
                            .is_some_and(|seen| !seen.insert(row_id))
                    {
                        None
                    } else {
                        Some(projector.project(&key))
                    }
                })
                .collect::<Vec<_>>()
        } else {
            let entries = cursor.next_chunk(CHUNK)?;
            if entries.is_empty() {
                break;
            }
            let mut row_ids = entries
                .into_iter()
                .map(|(_, row_id)| row_id)
                .collect::<Vec<_>>();
            examined = examined.saturating_add(row_ids.len() as u64);
            if let Some(seen) = seen_row_ids.as_mut() {
                row_ids.retain(|row_id| seen.insert(*row_id));
            }
            record_mrr_window(&row_ids);
            materialize_row_ids_in_order(db, txn, table, &heap, &row_ids, projection_columns)?
        };

        for row in candidates {
            if !eval_covering_filter(db, txn, filter, &row, &mut eval_ctx, &mut compiled_filter)? {
                continue;
            }
            let mut distinct_values = Vec::with_capacity(projection.len());
            projected_dedup_key_into(&row, projection, &mut distinct_values)?;
            let key: std::sync::Arc<[ValueKey]> = distinct_values.into();
            if dedup.contains(&key) {
                continue;
            }
            let key_charge = std::mem::size_of::<usize>()
                .saturating_mul(2)
                .saturating_add(key.len().saturating_mul(std::mem::size_of::<ValueKey>()))
                .saturating_add(
                    key.iter()
                        .map(AllocatedBytes::allocated_bytes)
                        .sum::<usize>(),
                );
            dedup_charge.add(key_charge)?;
            dedup.insert(key)?;
            dedup_charge.add(row.allocated_bytes())?;
            if rows.len() == rows.capacity() {
                let old = rows.capacity();
                let target = old.saturating_mul(2).max(4).min(k);
                dedup_charge.add((target - old) * std::mem::size_of::<PlanRow>())?;
                mem_budget::try_reserve_vec_exact(
                    &mut rows,
                    target - old,
                    "ordered DISTINCT output",
                )?;
            }
            rows.push(row);
            if rows.len() >= k {
                break;
            }
        }
    }
    if crate::engine::executor::select::analyze_stats::is_recording() {
        crate::engine::executor::select::analyze_stats::add_range_scan_work(examined, examined, 0);
    }
    crate::engine::executor::select::eval::ORDER_BY_KEY_ROWS_PREPARED
        .fetch_add(rows.len() as u64, Ordering::Relaxed);
    Ok(Some(rows))
}

/// Bounded implementation of `Sort -> DISTINCT projection -> Limit` for the
/// planner-proven shape where every sort expression is projected. Duplicate
/// tuples therefore have identical order keys, and only keys currently owned
/// by the K-row heap need tracking: an evicted key can never beat the heap's
/// monotonically improving worst key if it appears again later.
pub(in crate::engine::executor::select) fn execute_distinct_topk(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    order_by: &[OrderBy],
    projection: &[SelectItem],
    k: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    if k == 0 {
        let _ = execute_plan_rows(db, txn, input)?;
        return Ok(Vec::new());
    }

    if let Some(rows) =
        try_execute_ordered_index_distinct_topk(db, txn, input, order_by, projection, k)?
    {
        return Ok(rows);
    }

    use crate::engine::executor::memory_footprint::AllocatedBytes;
    use crate::engine::expr::ValueKey;

    let fields = crate::engine::executor::sort::fields_from_order_by(order_by);
    let mut topk = crate::engine::executor::sort::BoundedTopK::new(&fields, k)?;
    let mut retained = crate::engine::executor::sort::TopKSet::new();

    // The planner only exposes this property when the complete input path
    // preserves the requested ordering. In that case the first K distinct
    // projected tuples are the exact answer: every later row has an equal or
    // worse order key. This is an execution shortcut, not result reuse; every
    // returned tuple is read under the current transaction snapshot.
    let ordered_early_stop = crate::planner::plan_satisfies_order_by(input, order_by);
    let mut stream = execute_plan_stream(db, txn, input)?;
    let mut sort_keys = Vec::with_capacity(order_by.len());
    let mut order_program: Option<crate::engine::executor::select::eval::PlanOrderProgram> = None;
    let mut prepared_rows = 0u64;
    while let Some(row) = stream.next()? {
        crate::engine::cancel::check_cancelled()?;
        sort_keys.clear();
        if order_program.is_none() {
            topk = topk.with_collations(
                order_by
                    .iter()
                    .map(|item| {
                        crate::engine::executor::select::eval::plan_expr_collation(&item.expr, &row)
                            .map(|info| info.id)
                    })
                    .collect(),
            );
        }
        let program = order_program.get_or_insert_with(|| {
            crate::engine::executor::select::eval::PlanOrderProgram::compile_once(&row, order_by)
        });
        if order_by.len() == 1 {
            sort_keys.push(program.eval_single_key(&row)?);
        } else {
            sort_keys.extend(program.eval_keys(&row)?);
        }
        prepared_rows = prepared_rows.saturating_add(1);
        if !topk.would_accept(&sort_keys) {
            continue;
        }

        let mut distinct_values = Vec::with_capacity(projection.len());
        projected_dedup_key_into(&row, projection, &mut distinct_values)?;
        let key: std::sync::Arc<[ValueKey]> = distinct_values.into();
        if retained.contains(&key) {
            continue;
        }
        let key_charge = std::mem::size_of::<usize>()
            .saturating_mul(2)
            .saturating_add(key.len().saturating_mul(std::mem::size_of::<ValueKey>()))
            .saturating_add(
                key.iter()
                    .map(AllocatedBytes::allocated_bytes)
                    .sum::<usize>(),
            );
        retained.insert(key.clone())?;
        let payload = DistinctTopKPayload {
            row,
            key,
            key_charge,
        };
        let (accepted, evicted) = topk.push_with_evicted(payload, sort_keys.clone())?;
        debug_assert!(accepted, "would_accept and push_with_evicted diverged");
        if let Some(evicted) = evicted {
            retained.remove(&evicted.item.key);
        }
        if ordered_early_stop && retained.len() >= k {
            break;
        }
    }
    crate::engine::executor::select::eval::ORDER_BY_KEY_ROWS_PREPARED
        .fetch_add(prepared_rows, std::sync::atomic::Ordering::Relaxed);
    Ok(topk
        .into_sorted_vec()?
        .into_iter()
        .map(|payload| payload.row)
        .collect())
}

pub(super) struct TopWindowEntry {
    pub(super) keys: Vec<Value>,
    pub(super) ordinal: u64,
    pub(super) row: PlanRow,
}

pub(in crate::engine::executor::select) fn execute_topwindow(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    order_by: &[OrderBy],
    limit: usize,
    offset: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    if let Some(rows) =
        try_execute_compact_topwindow_simple_scan(db, txn, input, order_by, limit, offset)?
    {
        return Ok(rows);
    }

    // TopWindow is blocking in both traced and untraced execution.  Keep the
    // direct materialising child path identical; tracing is observational.
    let rows = execute_plan_rows_inner(db, txn, input)?;
    if limit == 0 || offset >= rows.len() {
        return Ok(Vec::new());
    }
    let end = offset.saturating_add(limit).min(rows.len());
    let fields = crate::engine::executor::sort::fields_from_order_by(order_by);
    let key_charge_bytes = rows
        .len()
        .saturating_mul(order_by.len().max(1))
        .saturating_mul(std::mem::size_of::<Value>());
    crate::engine::executor::mem_budget::try_charge(key_charge_bytes)?;
    let _key_charge = crate::engine::executor::mem_budget::ScopedCharge::new(key_charge_bytes);

    let mut entries = Vec::with_capacity(rows.len());
    for (ordinal, row) in rows.into_iter().enumerate() {
        crate::engine::cancel::check_cancelled()?;
        let mut keys = Vec::with_capacity(order_by.len());
        for item in order_by {
            keys.push(
                crate::engine::executor::select::eval::eval_row_or_aggregate_expr(
                    &item.expr, &row,
                )?,
            );
        }
        entries.push(TopWindowEntry {
            keys,
            ordinal: ordinal as u64,
            row,
        });
    }
    crate::engine::executor::select::eval::ORDER_BY_KEY_ROWS_PREPARED
        .fetch_add(entries.len() as u64, std::sync::atomic::Ordering::Relaxed);

    let compare = |left: &TopWindowEntry, right: &TopWindowEntry| {
        crate::engine::executor::sort::compare_sort_keys(&fields, &left.keys, &right.keys)
            .then_with(|| left.ordinal.cmp(&right.ordinal))
    };
    entries.select_nth_unstable_by(offset, compare);
    let page_len = end - offset;
    if offset + page_len < entries.len() {
        entries[offset..].select_nth_unstable_by(page_len, compare);
    }
    let mut page: Vec<TopWindowEntry> = entries.drain(offset..end).collect();
    page.sort_unstable_by(compare);
    Ok(page.into_iter().map(|entry| entry.row).collect())
}

pub(in crate::engine::executor::select) fn execute_streaming_aggregate(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::engine::executor::select::aggregate::{
        collect_aggregate_slots, Accumulator, AggregateSlot,
    };
    let mut slot_exprs: Vec<&Expr> = aggregates.iter().collect();
    if let Some(h) = having {
        slot_exprs.push(h);
    }
    let mut slots: Vec<AggregateSlot> = collect_aggregate_slots(&slot_exprs);
    super::aggregate::bind_slot_comparisons(&mut slots, input);

    let mut out = Vec::new();
    let mut current_keys: Option<Vec<Value>> = None;
    let mut current_sample: Option<Vec<PlanField>> = None;
    let mut current_accs: Vec<Box<dyn Accumulator>> = Vec::new();
    let mut have_active = false;
    let mut compiled_group_by: Option<
        Vec<crate::engine::executor::select::eval::CompiledPlanExpr>,
    > = None;
    let mut compiled_slot_inputs: Option<
        Vec<Option<crate::engine::executor::select::eval::CompiledPlanExpr>>,
    > = None;

    let make_accs =
        || -> Vec<Box<dyn Accumulator>> { slots.iter().map(|s| s.make_accumulator()).collect() };

    let feed =
        |slots: &[AggregateSlot],
         accs: &mut [Box<dyn Accumulator>],
         row: &PlanRow,
         compiled_inputs: &[Option<crate::engine::executor::select::eval::CompiledPlanExpr>]|
         -> Result<(), QueryError> {
            for (slot_idx, (slot, acc)) in slots.iter().zip(accs.iter_mut()).enumerate() {
                if slot.is_count_star {
                    acc.update_count_star();
                } else if slot.args.is_empty() {
                    acc.update(Value::Null)?;
                } else {
                    let v = compiled_inputs
                        .get(slot_idx)
                        .and_then(Option::as_ref)
                        .map(|compiled| compiled.eval(row))
                        .unwrap_or_else(|| eval_plan_expr(&slot.args[0], row))?;
                    if slot.has_order_by() {
                        let keys = slot.eval_order_keys(|e| eval_plan_expr(e, row))?;
                        acc.update_ordered(v, keys)?;
                    } else {
                        acc.update(v)?;
                    }
                }
            }
            Ok(())
        };

    let mut stream = execute_plan_stream(db, txn, input)?;
    while let Some(row) = stream.next()? {
        crate::engine::cancel::check_cancelled()?;
        if compiled_group_by.is_none() {
            let schema = crate::engine::executor::select::eval::input_schema_for_plan_row(&row);
            compiled_group_by = Some(
                group_by
                    .iter()
                    .map(|expr| {
                        crate::engine::executor::select::eval::CompiledPlanExpr::compile_once(
                            expr, &schema,
                        )
                    })
                    .collect(),
            );
            compiled_slot_inputs = Some(
                slots
                    .iter()
                    .map(|slot| {
                        slot.args.first().map(|expr| {
                            crate::engine::executor::select::eval::CompiledPlanExpr::compile_once(
                                expr, &schema,
                            )
                        })
                    })
                    .collect(),
            );
        }
        let keys = compiled_group_by
            .as_ref()
            .expect("streaming aggregate expressions not compiled")
            .iter()
            .map(|compiled| compiled.eval(&row))
            .collect::<Result<Vec<_>, _>>()?;
        let same = matches!(&current_keys, Some(prev) if values_equal_for_grouping(prev, &keys));
        if !same && have_active {
            // Flush previous group.
            let keys_prev = current_keys.take().expect("group keys missing");
            let sample_fields = current_sample.take().expect("sample missing");
            let accs = std::mem::take(&mut current_accs);
            emit_streaming_accum_group(&mut out, &slots, keys_prev, sample_fields, accs, having)?;
            have_active = false;
        }
        if !have_active {
            current_keys = Some(keys);
            current_sample = Some(row.fields.clone());
            current_accs = make_accs();
            have_active = true;
        }
        feed(
            &slots,
            &mut current_accs,
            &row,
            compiled_slot_inputs
                .as_deref()
                .expect("streaming aggregate inputs not compiled"),
        )?;
    }
    if have_active {
        let keys_prev = current_keys.expect("group keys missing");
        let sample_fields = current_sample.expect("sample missing");
        emit_streaming_accum_group(
            &mut out,
            &slots,
            keys_prev,
            sample_fields,
            current_accs,
            having,
        )?;
    }
    Ok(out)
}

pub(in crate::engine::executor::select) fn execute_streaming_rollup_aggregate(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::engine::executor::select::aggregate::{collect_aggregate_slots, AggregateSlot};
    let mut slot_exprs: Vec<&Expr> = aggregates.iter().collect();
    if let Some(h) = having {
        slot_exprs.push(h);
    }
    let mut slots: Vec<AggregateSlot> = collect_aggregate_slots(&slot_exprs);
    super::aggregate::bind_slot_comparisons(&mut slots, input);
    let group_by_len = group_by.len();
    let mut out = Vec::new();
    let mut active: Vec<Option<RollupGroupState>> = (0..=group_by_len).map(|_| None).collect();
    let mut prev_keys: Option<Vec<Value>> = None;

    let common_prefix_len = |left: &[Value], right: &[Value]| -> usize {
        let mut len = 0usize;
        while len < left.len()
            && len < right.len()
            && values_equal_for_grouping(&left[len..=len], &right[len..=len])
        {
            len += 1;
        }
        len
    };

    let mut stream = execute_plan_stream(db, txn, input)?;
    while let Some(row) = stream.next()? {
        crate::engine::cancel::check_cancelled()?;
        let keys = group_by
            .iter()
            .map(|expr| eval_plan_expr(expr, &row))
            .collect::<Result<Vec<_>, _>>()?;
        let common = prev_keys
            .as_ref()
            .map(|prev| common_prefix_len(prev, &keys))
            .unwrap_or(0);
        if prev_keys.is_none() {
            active[0] = Some(RollupGroupState {
                prefix_len: 0,
                key_values: Vec::new(),
                sample_fields: row.fields.clone(),
                accumulators: slots.iter().map(|slot| slot.make_accumulator()).collect(),
            });
        } else {
            for prefix_len in ((common + 1)..=group_by_len).rev() {
                if let Some(state) = active[prefix_len].take() {
                    emit_rollup_group_state(&mut out, &slots, group_by_len, state, having)?;
                }
            }
        }
        for prefix_len in (common + 1)..=group_by_len {
            active[prefix_len] = Some(RollupGroupState {
                prefix_len,
                key_values: keys[..prefix_len].to_vec(),
                sample_fields: row.fields.clone(),
                accumulators: slots.iter().map(|slot| slot.make_accumulator()).collect(),
            });
        }
        let slot_inputs = rollup_slot_inputs(&slots, &row)?;
        for state in active.iter_mut().filter_map(Option::as_mut) {
            update_rollup_accumulators_ordered(state, &slots, &slot_inputs, &row)?;
        }
        prev_keys = Some(keys);
    }
    if prev_keys.is_none() {
        // Empty input: ROLLUP still emits the single `()` grand-total row
        // (MySQL parity, e.g. `NULL … NULL 0`). The loop above never seeded
        // `active[0]`, so do it here before the final flush.
        active[0] = Some(RollupGroupState {
            prefix_len: 0,
            key_values: Vec::new(),
            sample_fields: Vec::new(),
            accumulators: slots.iter().map(|slot| slot.make_accumulator()).collect(),
        });
    }
    for prefix_len in (0..=group_by_len).rev() {
        if let Some(state) = active[prefix_len].take() {
            emit_rollup_group_state(&mut out, &slots, group_by_len, state, having)?;
        }
    }
    Ok(out)
}

pub(super) fn emit_streaming_accum_group(
    out: &mut Vec<PlanRow>,
    slots: &[crate::engine::executor::select::aggregate::AggregateSlot],
    keys: Vec<Value>,
    sample_fields: Vec<PlanField>,
    accumulators: Vec<Box<dyn crate::engine::executor::select::aggregate::Accumulator>>,
    having: Option<&Expr>,
) -> Result<(), QueryError> {
    let mut cache: HashMap<u64, Value> = HashMap::with_capacity(slots.len());
    for (slot, acc) in slots.iter().zip(accumulators.into_iter()) {
        cache.insert(slot.key, acc.finalize()?);
    }
    let mut fields = sample_fields;
    for (idx, value) in keys.into_iter().enumerate() {
        fields.push(PlanField {
            meta: intern_col_meta(ColMeta {
                invisible: false,
                table: "$group".to_string(),
                alias: None,
                column: format!("group_{idx}"),
                column_type: ColumnType::VarString,
                source: Default::default(),
            }),
            value,
        });
    }
    let row = PlanRow {
        fields,
        group: Some(Vec::new()),
        agg_values: Some(std::sync::Arc::new(cache)),
        schema: PlanRow::empty_schema(),
    };
    if having
        .map(|expr| eval_aggregate_bool(expr, &[], &row))
        .transpose()?
        .unwrap_or(true)
    {
        out.push(row);
    }
    Ok(())
}

/// Compare two key vectors with the same 3VL semantics that hash
/// aggregate uses through `ValueKey`.
/// NULLs compare equal to NULLs (so all NULL-keyed rows form a
/// single group, matching SQL standard GROUP BY behaviour).
pub(super) fn values_equal_for_grouping(left: &[Value], right: &[Value]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    for (l, r) in left.iter().zip(right.iter()) {
        match (l, r) {
            (Value::Null, Value::Null) => continue,
            (Value::Null, _) | (_, Value::Null) => return false,
            _ => {
                if crate::engine::expr::ValueKey::from(l) != crate::engine::expr::ValueKey::from(r)
                {
                    return false;
                }
            }
        }
    }
    true
}

/// Phase B #B4: streaming distinct. Pulls rows from `input` (which
/// the planner only chooses when input is sorted on the projected
/// columns), projects each row, and only emits the row when the
/// projected key differs from the previously emitted one. Avoids
/// the HashSet allocation that `dedup_projected_rows` requires.
pub(in crate::engine::executor::select) fn execute_streaming_distinct(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    projection: &[SelectItem],
) -> Result<Vec<PlanRow>, QueryError> {
    // Phase 7: try the vectorized streaming DISTINCT first. Returns
    // None when the projection is anything other than plain column
    // references (incl. wildcards) so the legacy hash-set path stays
    // authoritative for complex projections.
    if let Some(rows) =
        crate::engine::executor::select::vec_bridge::try_vec_distinct(db, txn, input, projection)?
    {
        return Ok(rows);
    }
    let input_rows = execute_plan_rows(db, txn, input)?;
    let mut out = Vec::with_capacity(input_rows.len());
    // Adjacent-compare dedup over the (planner-guaranteed) sorted
    // input. Both key buffers are reused across rows: building the key
    // in place and swapping on emit keeps the loop free of per-row
    // String/Vec allocations.
    let mut last_key: Option<Vec<ValueKey>> = None;
    let mut key_buf: Vec<ValueKey> = Vec::new();
    for row in input_rows {
        key_buf.clear();
        projected_dedup_key_into(&row, projection, &mut key_buf)?;
        if last_key.as_deref() != Some(key_buf.as_slice()) {
            match last_key.as_mut() {
                Some(last) => std::mem::swap(last, &mut key_buf),
                None => last_key = Some(std::mem::take(&mut key_buf)),
            }
            out.push(row);
        }
    }
    Ok(out)
}

pub(super) fn projected_dedup_key_into(
    row: &PlanRow,
    projection: &[SelectItem],
    out: &mut Vec<ValueKey>,
) -> Result<(), QueryError> {
    for item in projection {
        match item {
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                for field in &row.fields {
                    if field.invisible {
                        continue;
                    }
                    if let SelectItem::QualifiedWildcard(qualifier) = item {
                        if !field
                            .alias
                            .as_deref()
                            .unwrap_or(&field.table)
                            .eq_ignore_ascii_case(qualifier)
                        {
                            continue;
                        }
                    }
                    out.push(ValueKey::from_with_collation(
                        &field.value,
                        field.source.collation_id,
                    ));
                }
            }
            SelectItem::Expr { expr, .. } => {
                let value =
                    crate::engine::executor::select::eval::eval_row_or_aggregate_expr(expr, row)?;
                let collation =
                    crate::engine::executor::select::eval::plan_expr_collation(expr, row)
                        .map(|info| info.id);
                out.push(ValueKey::from_with_collation(&value, collation));
            }
        }
    }
    Ok(())
}
