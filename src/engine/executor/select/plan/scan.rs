//! scan execution and preparation.

use super::*;

/// Wrap a scan call in a partition-pruning context derived from the
/// optional WHERE filter. When the table is partitioned and the
/// filter exposes equality / range / IN on the partition column(s),
/// `partition_router::with_pruned_partitions` installs a thread-local
/// list of `storage_id`s the storage layer's
/// `partition_storage_ids_for_scan` consults inside the partition
/// fan-out; the scan therefore visits only matching partitions.
///
/// For unpartitioned tables and unprunable filters this is a
/// transparent no-op — the closure runs without any extra work.
pub(in crate::engine::executor::select) fn run_scan_with_partition_pruning<F, T>(
    table: &crate::storage::TableSchema,
    filter: &Option<Expr>,
    body: F,
) -> Result<T, QueryError>
where
    F: FnOnce() -> Result<T, QueryError>,
{
    if !table.is_partitioned() {
        return body();
    }
    let pruned = crate::engine::executor::partition_router::pruned_storage_ids_from_filter(
        table,
        filter.as_ref(),
    );
    let pruned = match (
        crate::engine::executor::partition_router::current_pruned_partitions(),
        pruned,
    ) {
        (Some(current), Some(planned)) => Some(
            current
                .into_iter()
                .filter(|id| planned.contains(id))
                .collect(),
        ),
        (Some(current), None) => Some(current),
        (None, planned) => planned,
    };
    match pruned {
        Some(ids) => crate::engine::executor::partition_router::with_pruned_partitions(ids, body),
        None => body(),
    }
}

/// Execute a physical table access with the immutable pruning artifact chosen
/// by the planner. Static plans never re-interpret the predicate in the
/// executor; parameterized plans evaluate the pruning program against the
/// current bound expression values.
pub(in crate::engine::executor::select) fn run_plan_scan_with_partition_pruning<F, T>(
    table: &crate::planner::PlanTable,
    filter: &Option<Expr>,
    body: F,
) -> Result<T, QueryError>
where
    F: FnOnce() -> Result<T, QueryError>,
{
    if !table.schema.is_partitioned() {
        return body();
    }
    match table.resolved_partition_storage_ids(filter.as_ref()) {
        Some(storage_ids) => {
            crate::engine::executor::partition_router::with_pruned_partitions(storage_ids, body)
        }
        None => body(),
    }
}

pub(in crate::engine::executor::select) fn scan_plan_table(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: &Option<Expr>,
    runtime_filters: &[RuntimeFilter],
    projected_columns: Option<&[usize]>,
    use_zone_map: bool,
) -> Result<Vec<PlanRow>, QueryError> {
    // When the planner has computed a column-pruning list, use the
    // streaming-columns API: storage decodes only the projected columns
    // out of each heap row and we never allocate `StoredRow.values` for
    // the unreferenced ones. The full-row path remains for single-table
    // queries and any case where the planner couldn't project.
    if let Some(projection) = projected_columns {
        return scan_plan_table_projected(
            db,
            txn,
            table,
            filter,
            runtime_filters,
            projection,
            use_zone_map,
        );
    }
    let mut eval_ctx = EvalQueryContext::default();
    let mut out = Vec::new();
    let bloom_fast_indices: Vec<Option<CompiledRuntimeFilterProbe>> = runtime_filters
        .iter()
        .map(|rf| compile_runtime_filter_probe_full(rf, &table.schema))
        .collect();
    let all_fast = !runtime_filters.is_empty() && bloom_fast_indices.iter().all(|x| x.is_some());
    let compiled_filter = filter
        .as_ref()
        .and_then(|expr| CompiledPredicate::compile(expr, &table.schema));
    let qualified = db.storage_key_for_table(&table.schema);
    let stored_rows = run_plan_scan_with_partition_pruning(table, filter, || {
        if table.schema.is_partitioned() {
            let mut rows = Vec::new();
            scan_partition_ranges(
                db,
                &table.schema,
                filter.as_ref().filter(|_| use_zone_map),
                |storage_id, lo, hi| {
                    rows.extend(db.scan_rows_storage_range(
                        &table.schema,
                        storage_id,
                        txn,
                        lo,
                        hi,
                    )?);
                    Ok(())
                },
            )?;
            return Ok(rows);
        }
        if let Some(ranges) = use_zone_map
            .then(|| filter.as_ref())
            .flatten()
            .and_then(|expr| db.zone_map_candidate_ranges_for_plan(&table.schema, expr))
        {
            let mut rows = Vec::new();
            for (lo, hi) in ranges {
                rows.extend(db.scan_rows_range(&qualified, txn, lo, hi)?);
            }
            Ok(rows)
        } else {
            db.scan_rows(&qualified, txn)
        }
    })?;
    for stored in stored_rows {
        crate::engine::cancel::check_cancelled()?;
        if let Some(compiled) = compiled_filter.as_ref() {
            if !compiled.eval(&stored.values)? {
                continue;
            }
        }
        if all_fast && !runtime_filters_admit_values(&bloom_fast_indices, &stored.values) {
            HASH_JOIN_BLOOM_PROBE_FILTERED.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let row = stored_row_to_plan_row(table, stored.values)?;
        if !all_fast && !runtime_filters_admit(runtime_filters, &row)? {
            HASH_JOIN_BLOOM_PROBE_FILTERED.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let keep = match (filter, compiled_filter.as_ref()) {
            (Some(expr), None) => eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)?,
            _ => true,
        };
        if keep {
            out.push(row);
        }
    }
    Ok(out)
}

/// Visit selected partition heaps in storage order. An empty candidate set
/// skips that heap; only unavailable candidates fall back to its full range.
/// The owning candidate iterator keeps its scratch reservation through errors.
fn scan_partition_ranges(
    db: &Database,
    schema: &TableSchema,
    zone_map_filter: Option<&Expr>,
    mut visit: impl FnMut(u64, u64, u64) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    for storage_id in db.scan_storage_ids(schema) {
        let ranges = zone_map_filter.and_then(|expr| {
            db.zone_map_candidate_ranges_for_storage_for_plan(schema, storage_id, expr)
        });
        if let Some(ranges) = ranges {
            for (lo, hi) in ranges {
                visit(storage_id, lo, hi)?;
            }
        } else {
            let pages = db.heap_page_count_for_storage(storage_id)?;
            visit(storage_id, 0, pages)?;
        }
    }
    Ok(())
}

/// Physical routing shared by collected and channel-backed projected scans.
/// Nonpartitioned candidates are supplied by the caller: streaming scans must
/// retain the ranges and reservation captured before dispatching their worker.
/// Row filters and partition-pruning scope remain the caller's responsibility.
#[allow(clippy::too_many_arguments)]
pub(in crate::engine::executor::select) fn scan_projected_table<F>(
    db: &Database,
    schema: &TableSchema,
    txn: TxnView,
    columns: &[usize],
    zone_map_filter: Option<&Expr>,
    candidate_ranges: Option<&[(u64, u64)]>,
    outside_latch: bool,
    mut visit: F,
) -> Result<(), QueryError>
where
    F: FnMut(&[Value]) -> Result<(), QueryError>,
{
    if schema.is_partitioned() {
        scan_partition_ranges(db, schema, zone_map_filter, |storage_id, lo, hi| {
            scan_projected_range(
                db,
                schema,
                storage_id,
                txn,
                columns,
                lo,
                hi,
                outside_latch,
                &mut visit,
            )
        })
    } else if let Some(ranges) = candidate_ranges {
        for &(lo, hi) in ranges {
            scan_projected_range(
                db,
                schema,
                schema.id,
                txn,
                columns,
                lo,
                hi,
                outside_latch,
                &mut visit,
            )?;
        }
        Ok(())
    } else if outside_latch {
        let pages = db.heap_page_count_for_storage(schema.id)?;
        scan_projected_range(db, schema, schema.id, txn, columns, 0, pages, true, visit)
    } else {
        let qualified = db.storage_key_for_table(schema);
        db.scan_rows_columns_streaming(&qualified, txn, columns, visit)
    }
}

/// Visit projected rows outside page/heap latches when the consumer may
/// evaluate opaque SQL. Buffer one physical page, never a whole scan. The
/// caller's statement view is kept across pages, including historical rows.
/// Pure predicate kernels retain the original borrowed-value fast path.
#[allow(clippy::too_many_arguments)]
pub(in crate::engine::executor::select) fn scan_projected_range<F>(
    db: &Database,
    schema: &TableSchema,
    storage_id: u64,
    txn: TxnView,
    columns: &[usize],
    page_lo: u64,
    page_hi: u64,
    outside_latch: bool,
    mut visit: F,
) -> Result<(), QueryError>
where
    F: FnMut(&[Value]) -> Result<(), QueryError>,
{
    if !outside_latch {
        return db.scan_rows_columns_storage_streaming_range(
            schema, storage_id, txn, columns, page_lo, page_hi, visit,
        );
    }
    let mut rows = Vec::new();
    for page in page_lo..page_hi {
        crate::engine::cancel::check_cancelled()?;
        db.scan_rows_columns_storage_streaming_range(
            schema,
            storage_id,
            txn,
            columns,
            page,
            page + 1,
            |values| {
                rows.push(values.to_vec());
                Ok(())
            },
        )?;
        for row in rows.drain(..) {
            visit(&row)?;
        }
    }
    Ok(())
}

/// Streaming scan that only decodes columns in `projection` (relative to
/// `table.schema.columns`). Built on top of `scan_rows_columns_streaming`,
/// which feeds rows page-by-page without ever materialising a full
/// `StoredRow` Vec — important for the wide TPC-H tables where 90% of the
/// columns are never read by the query.
pub(super) fn scan_plan_table_projected(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: &Option<Expr>,
    runtime_filters: &[RuntimeFilter],
    projection: &[usize],
    use_zone_map: bool,
) -> Result<Vec<PlanRow>, QueryError> {
    crate::engine::cancel::check_cancelled()?;
    let bloom_specs: Vec<Option<CompiledRuntimeFilterProbe>> = runtime_filters
        .iter()
        .map(|rf| compile_runtime_filter_probe_projected(rf, &table.schema, projection))
        .collect();
    let all_fast = !runtime_filters.is_empty() && bloom_specs.iter().all(|x| x.is_some());
    // Pre-intern each projection slot's ColMeta to an `&'static ColMeta`
    // once, then build each emitted `PlanRow.fields` in a single
    // (meta, value) zip. The earlier two-step template-clone pattern
    // copied `PlanField` rows full of `Value::Null` placeholders before
    // overwriting their `.value` slots — pure waste on the hot path.
    let metas: Vec<_> = projection
        .iter()
        .map(|&i| {
            intern_col_meta(ColMeta {
                invisible: table.schema.columns[i].invisible,
                table: table.schema.name.clone(),
                alias: table.alias.clone(),
                column: table.schema.columns[i].name.clone(),
                column_type: table.schema.result_type(i),
                source: Default::default(),
            })
        })
        .collect();
    let mut out: Vec<PlanRow> = Vec::new();
    let mut eval_ctx = EvalQueryContext::default();
    // If the row filter contains a subquery, evaluating it inside
    // `scan_rows_columns_streaming` deadlocks: the streaming scan holds
    // the heap's RwLock write lock for the duration of the visit
    // callback, and any subquery body that scans the same heap would
    // re-enter `with_heap_mut` on the same thread and block forever
    // waiting for its own write lock.
    // Defer subquery-bearing filters to a post-scan pass.
    let filter_has_subquery = filter
        .as_ref()
        .is_some_and(crate::engine::executor::select::simple::expr_has_subquery);
    let projected_schema = projected_schema_view(&table.schema, projection);
    let compiled_filter = if filter_has_subquery {
        None
    } else {
        filter
            .as_ref()
            .and_then(|expr| compile_scan_predicate(expr, &projected_schema))
    };
    let outside_latch = (!filter_has_subquery
        && filter.is_some()
        && !compiled_filter
            .as_ref()
            .is_some_and(CompiledScanPredicate::page_latch_safe))
        || (!runtime_filters.is_empty() && !all_fast);
    run_plan_scan_with_partition_pruning(table, filter, || {
        let visit = |values: &[Value]| {
            crate::engine::cancel::check_cancelled()?;
            if all_fast && !runtime_filters_admit_values_indexed(&bloom_specs, values) {
                HASH_JOIN_BLOOM_PROBE_FILTERED.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            if let Some(predicate) = compiled_filter.as_ref() {
                if !predicate.eval(values)? {
                    return Ok(());
                }
            }
            let fields: Vec<PlanField> = metas
                .iter()
                .zip(values.iter())
                .map(|(meta, value)| PlanField {
                    meta: meta.clone(),
                    value: value.clone(),
                })
                .collect();
            let row = PlanRow {
                fields,
                group: None,
                agg_values: None,
                schema: PlanRow::empty_schema(),
            };
            if !all_fast && !runtime_filters_admit(runtime_filters, &row)? {
                HASH_JOIN_BLOOM_PROBE_FILTERED.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            if filter_has_subquery {
                // Stage the row for a post-scan pass — see comment above.
                out.push(row);
                return Ok(());
            }
            let keep = match (filter, compiled_filter.as_ref()) {
                (Some(expr), None) => eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)?,
                _ => true,
            };
            if keep {
                out.push(row);
            }
            Ok(())
        };
        let zone_map_filter = filter.as_ref().filter(|_| use_zone_map);
        let candidate_ranges = if table.schema.is_partitioned() {
            None
        } else {
            zone_map_filter
                .and_then(|expr| db.zone_map_candidate_ranges_for_plan(&table.schema, expr))
        };
        scan_projected_table(
            db,
            &table.schema,
            txn,
            projection,
            zone_map_filter,
            candidate_ranges.as_deref(),
            outside_latch,
            visit,
        )
    })?;
    if filter_has_subquery {
        if let Some(expr) = filter {
            let mut filtered = Vec::with_capacity(out.len());
            for row in out {
                if eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)? {
                    filtered.push(row);
                }
            }
            out = filtered;
        }
    }
    Ok(out)
}

/// `try_precompute_bloom_columns` flavour for a projected scan: the
/// runtime values vector contains only the projected columns in the
/// caller's order, so we resolve each bloom-key column to its position
/// inside that projected vector (not its schema-wide index).
pub(in crate::engine::executor::select) fn compile_runtime_filter_probe_projected(
    rf: &RuntimeFilter,
    schema: &crate::storage::schema::TableSchema,
    projection: &[usize],
) -> Option<CompiledRuntimeFilterProbe> {
    compile_runtime_filter_probe(rf, schema, Some(projection))
}

#[derive(Clone)]
pub(in crate::engine::executor::select) struct CompiledRuntimeFilterProbe {
    pub(super) membership: CompiledRuntimeFilterMembership,
    pub(super) key: CompiledRuntimeFilterKey,
}

#[derive(Clone)]
pub(super) enum CompiledRuntimeFilterMembership {
    Bloom {
        filter: Arc<crate::engine::executor::select::bloom::BloomFilter>,
        rejected: Option<Arc<std::sync::atomic::AtomicU64>>,
    },
    ExactInt {
        filter: Arc<crate::engine::executor::select::bloom::ExactIntKeySet>,
        rejected: Arc<std::sync::OnceLock<Arc<std::sync::atomic::AtomicU64>>>,
    },
    KeySet {
        filter: Arc<crate::engine::executor::select::bloom::BloomFilter>,
    },
}

#[derive(Clone)]
pub(super) enum CompiledRuntimeFilterKey {
    Fixed1 {
        index: usize,
        null_safe: bool,
    },
    Fixed2 {
        indices: [usize; 2],
        null_safe: [bool; 2],
    },
    Fixed3 {
        indices: [usize; 3],
        null_safe: [bool; 3],
    },
    Generic {
        indices: Vec<usize>,
        null_safe: Vec<bool>,
    },
}

pub(super) fn runtime_filter_fixed_width_type(data_type: &DataTypeKind) -> bool {
    matches!(
        data_type,
        DataTypeKind::TinyInt
            | DataTypeKind::SmallInt
            | DataTypeKind::MediumInt
            | DataTypeKind::Int
            | DataTypeKind::BigInt
            | DataTypeKind::Bool
            | DataTypeKind::Date
            | DataTypeKind::DateTime { .. }
            | DataTypeKind::Timestamp { .. }
    )
}

pub(super) fn compile_runtime_filter_probe(
    rf: &RuntimeFilter,
    schema: &crate::storage::schema::TableSchema,
    projection: Option<&[usize]>,
) -> Option<CompiledRuntimeFilterProbe> {
    let (keys, null_safe, membership) = match rf {
        RuntimeFilter::BloomKey {
            keys,
            null_safe,
            filter,
            rejected,
        } => (
            keys,
            null_safe,
            CompiledRuntimeFilterMembership::Bloom {
                filter: filter.clone(),
                rejected: rejected.clone(),
            },
        ),
        RuntimeFilter::ExactIntKey {
            keys,
            null_safe,
            filter,
            rejected,
        } => (
            keys,
            null_safe,
            CompiledRuntimeFilterMembership::ExactInt {
                filter: filter.clone(),
                rejected: rejected.clone(),
            },
        ),
        RuntimeFilter::KeySet {
            keys,
            null_safe,
            filter,
            ..
        } => (
            keys,
            null_safe,
            CompiledRuntimeFilterMembership::KeySet {
                filter: filter.clone(),
            },
        ),
    };
    let mut indices = Vec::with_capacity(keys.len());
    let mut fixed_width = keys.len() <= 3;
    for expr in keys {
        let col_name: &str = match expr {
            Expr::Identifier(n) => n.as_str(),
            Expr::CompoundIdentifier(parts) => parts.last()?.as_str(),
            _ => return None,
        };
        let schema_idx = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(col_name))?;
        fixed_width &= runtime_filter_fixed_width_type(&schema.columns[schema_idx].data_type);
        let value_idx = match projection {
            Some(projection) => projection.iter().position(|&p| p == schema_idx)?,
            None => schema_idx,
        };
        indices.push(value_idx);
    }
    let safe_at = |idx: usize| null_safe.get(idx).copied().unwrap_or(false);
    let key = if fixed_width {
        match indices.as_slice() {
            [index] => CompiledRuntimeFilterKey::Fixed1 {
                index: *index,
                null_safe: safe_at(0),
            },
            [first, second] => CompiledRuntimeFilterKey::Fixed2 {
                indices: [*first, *second],
                null_safe: [safe_at(0), safe_at(1)],
            },
            [first, second, third] => CompiledRuntimeFilterKey::Fixed3 {
                indices: [*first, *second, *third],
                null_safe: [safe_at(0), safe_at(1), safe_at(2)],
            },
            _ => CompiledRuntimeFilterKey::Generic {
                indices,
                null_safe: null_safe.clone(),
            },
        }
    } else {
        CompiledRuntimeFilterKey::Generic {
            indices,
            null_safe: null_safe.clone(),
        }
    };
    Some(CompiledRuntimeFilterProbe { membership, key })
}

/// Schema positions of a runtime filter's keys in `table`, or `None` when any
/// key belongs to another relation. Keys are qualified by the probe side's
/// alias/table, so a filter on `a.id` must never be installed on a sibling
/// relation that merely has a column of the same name.
pub(crate) fn runtime_filter_schema_indices(
    rf: &RuntimeFilter,
    table: &PlanTable,
) -> Option<Vec<usize>> {
    let keys = match rf {
        RuntimeFilter::BloomKey { keys, .. }
        | RuntimeFilter::ExactIntKey { keys, .. }
        | RuntimeFilter::KeySet { keys, .. } => keys,
    };
    keys.iter()
        .map(|expr| {
            let column = super::hash_join::runtime_filter_simple_column_for_table(expr, table)?;
            table
                .schema
                .columns
                .iter()
                .position(|candidate| candidate.name.eq_ignore_ascii_case(column))
        })
        .collect()
}

pub(in crate::engine::executor::select) fn runtime_filters_admit_values_indexed(
    precomputed: &[Option<CompiledRuntimeFilterProbe>],
    values: &[Value],
) -> bool {
    runtime_filters_admit_values_indexed_impl(precomputed, values, None)
}

pub(super) fn runtime_filters_admit_values_indexed_impl(
    precomputed: &[Option<CompiledRuntimeFilterProbe>],
    values: &[Value],
    mut rejected: Option<&mut [u64]>,
) -> bool {
    for (filter_index, compiled) in precomputed.iter().enumerate() {
        let Some(compiled) = compiled else {
            return true;
        };
        let contains = match &compiled.key {
            CompiledRuntimeFilterKey::Fixed1 { index, null_safe } => {
                let value = values.get(*index).unwrap_or(&Value::Null);
                if matches!(value, Value::Null) && !*null_safe {
                    false
                } else {
                    let key = [ValueKey::from(value)];
                    compiled.contains(&key[..])
                }
            }
            CompiledRuntimeFilterKey::Fixed2 { indices, null_safe } => {
                let first = values.get(indices[0]).unwrap_or(&Value::Null);
                let second = values.get(indices[1]).unwrap_or(&Value::Null);
                if (matches!(first, Value::Null) && !null_safe[0])
                    || (matches!(second, Value::Null) && !null_safe[1])
                {
                    false
                } else {
                    let key = [ValueKey::from(first), ValueKey::from(second)];
                    compiled.contains(&key[..])
                }
            }
            CompiledRuntimeFilterKey::Fixed3 { indices, null_safe } => {
                let first = values.get(indices[0]).unwrap_or(&Value::Null);
                let second = values.get(indices[1]).unwrap_or(&Value::Null);
                let third = values.get(indices[2]).unwrap_or(&Value::Null);
                if (matches!(first, Value::Null) && !null_safe[0])
                    || (matches!(second, Value::Null) && !null_safe[1])
                    || (matches!(third, Value::Null) && !null_safe[2])
                {
                    false
                } else {
                    let key = [
                        ValueKey::from(first),
                        ValueKey::from(second),
                        ValueKey::from(third),
                    ];
                    compiled.contains(&key[..])
                }
            }
            CompiledRuntimeFilterKey::Generic { indices, null_safe } => {
                let mut key = HashJoinKey::with_capacity(indices.len());
                let mut has_unsafe_null = false;
                for (position, &index) in indices.iter().enumerate() {
                    let value = values.get(index).unwrap_or(&Value::Null);
                    if matches!(value, Value::Null)
                        && !null_safe.get(position).copied().unwrap_or(false)
                    {
                        has_unsafe_null = true;
                        break;
                    }
                    key.push(ValueKey::from(value));
                }
                if has_unsafe_null {
                    false
                } else {
                    compiled.contains(key.as_slice())
                }
            }
        };
        if !contains {
            if let Some(counts) = rejected.as_deref_mut() {
                counts[filter_index] = counts[filter_index].saturating_add(1);
            } else {
                compiled.record_rejected();
            }
            return false;
        }
    }
    true
}

/// Runtime-filter probe used by predicate-first storage scans. Rejections are
/// accumulated in morsel-local counters and published when the morsel closes,
/// avoiding a shared atomic read-modify-write for every discarded fact row.
pub(in crate::engine::executor::select) fn runtime_filters_admit_values_indexed_deferred(
    precomputed: &[Option<CompiledRuntimeFilterProbe>],
    values: &[Value],
    rejected: &mut [u64],
) -> bool {
    debug_assert_eq!(precomputed.len(), rejected.len());
    runtime_filters_admit_values_indexed_impl(precomputed, values, Some(rejected))
}

/// Allocation-free runtime-filter probe for values decoded directly into the
/// predicate scratch lanes. Fixed-width scan keys never construct an
/// intermediate `Value`; generic/variable-width keys retain the same
/// canonical `ValueKey` semantics as the ordinary evaluator.
pub(super) fn runtime_filters_admit_predicate_values_indexed_impl(
    precomputed: &[Option<CompiledRuntimeFilterProbe>],
    values: &[crate::engine::executor::vec::scan_bridge::PredicateValue],
    mut rejected: Option<&mut [u64]>,
) -> bool {
    use crate::engine::executor::vec::scan_bridge::PredicateValue;

    // Fact-table scans overwhelmingly carry one runtime filter over one
    // integer join key. Keep that shape out of the generic slice loop and
    // nested key/membership dispatch: Q11-class scans execute this branch for
    // every input row, including the millions rejected before payload decode.
    // No allocation or additional resident state is introduced.
    if let [Some(compiled)] = precomputed {
        if let CompiledRuntimeFilterKey::Fixed1 { index, null_safe } = &compiled.key {
            let null = PredicateValue::Null;
            let value = values.get(*index).unwrap_or(&null);
            let contains = if matches!(value, PredicateValue::Null) && !*null_safe {
                false
            } else if let Some(key) = predicate_value_exact_int_key(value) {
                match &compiled.membership {
                    CompiledRuntimeFilterMembership::ExactInt { filter, .. } => {
                        filter.contains(key)
                    }
                    CompiledRuntimeFilterMembership::Bloom { filter, .. }
                        if filter.uses_int_keys() =>
                    {
                        filter.contains_int(key)
                    }
                    _ => compiled.contains(&[ValueKey::Int(key)]),
                }
            } else {
                compiled.contains(&[value.to_value_key()])
            };
            if !contains {
                if let Some(counts) = rejected.as_deref_mut() {
                    if let Some(count) = counts.first_mut() {
                        *count = count.saturating_add(1);
                    }
                } else {
                    compiled.record_rejected();
                }
            }
            return contains;
        }
    }

    for (filter_index, compiled) in precomputed.iter().enumerate() {
        let Some(compiled) = compiled else {
            return true;
        };
        let is_null = |value: &PredicateValue| matches!(value, PredicateValue::Null);
        let null = PredicateValue::Null;
        let contains = match &compiled.key {
            CompiledRuntimeFilterKey::Fixed1 { index, null_safe } => {
                let value = values.get(*index).unwrap_or(&null);
                if is_null(value) && !*null_safe {
                    false
                } else if let (
                    CompiledRuntimeFilterMembership::ExactInt { filter, .. },
                    Some(key),
                ) = (&compiled.membership, predicate_value_exact_int_key(value))
                {
                    // The overwhelmingly common fact-table runtime filter is
                    // one native integer key. Probe the exact bitmap/sorted
                    // set directly instead of constructing ValueKey, hashing
                    // its enum discriminant and re-matching it in `contains`.
                    filter.contains(key)
                } else if let (CompiledRuntimeFilterMembership::Bloom { filter, .. }, Some(key)) =
                    (&compiled.membership, predicate_value_exact_int_key(value))
                {
                    if filter.uses_int_keys() {
                        filter.contains_int(key)
                    } else {
                        // Keep the canonical ValueKey hash representation,
                        // but construct it directly from the already-decoded
                        // integer lane.
                        let key = [ValueKey::Int(key)];
                        filter.contains(&key[..])
                    }
                } else {
                    compiled.contains(&[value.to_value_key()])
                }
            }
            CompiledRuntimeFilterKey::Fixed2 { indices, null_safe } => {
                let first = values.get(indices[0]).unwrap_or(&null);
                let second = values.get(indices[1]).unwrap_or(&null);
                if (is_null(first) && !null_safe[0]) || (is_null(second) && !null_safe[1]) {
                    false
                } else if let (
                    CompiledRuntimeFilterMembership::Bloom { filter, .. },
                    Some(first_key),
                    Some(second_key),
                ) = (
                    &compiled.membership,
                    predicate_value_exact_int_key(first),
                    predicate_value_exact_int_key(second),
                ) {
                    if filter.uses_int_pair_keys() {
                        filter.contains_int_pair(first_key, second_key)
                    } else {
                        compiled.contains(&[ValueKey::Int(first_key), ValueKey::Int(second_key)])
                    }
                } else {
                    compiled.contains(&[first.to_value_key(), second.to_value_key()])
                }
            }
            CompiledRuntimeFilterKey::Fixed3 { indices, null_safe } => {
                let first = values.get(indices[0]).unwrap_or(&null);
                let second = values.get(indices[1]).unwrap_or(&null);
                let third = values.get(indices[2]).unwrap_or(&null);
                if (is_null(first) && !null_safe[0])
                    || (is_null(second) && !null_safe[1])
                    || (is_null(third) && !null_safe[2])
                {
                    false
                } else {
                    compiled.contains(&[
                        first.to_value_key(),
                        second.to_value_key(),
                        third.to_value_key(),
                    ])
                }
            }
            CompiledRuntimeFilterKey::Generic { indices, null_safe } => {
                let mut key = HashJoinKey::with_capacity(indices.len());
                let mut has_unsafe_null = false;
                for (position, &index) in indices.iter().enumerate() {
                    let value = values.get(index).unwrap_or(&null);
                    if is_null(value) && !null_safe.get(position).copied().unwrap_or(false) {
                        has_unsafe_null = true;
                        break;
                    }
                    key.push(value.to_value_key());
                }
                if has_unsafe_null {
                    false
                } else {
                    compiled.contains(key.as_slice())
                }
            }
        };
        if !contains {
            if let Some(counts) = rejected.as_deref_mut() {
                counts[filter_index] = counts[filter_index].saturating_add(1);
            } else {
                compiled.record_rejected();
            }
            return false;
        }
    }
    true
}

#[inline]
pub(super) fn predicate_value_exact_int_key(
    value: &crate::engine::executor::vec::scan_bridge::PredicateValue,
) -> Option<i128> {
    use crate::engine::executor::vec::scan_bridge::PredicateValue;
    match value {
        PredicateValue::I64(value) => Some(i128::from(*value)),
        PredicateValue::U64(value) => Some(i128::from(*value)),
        PredicateValue::Date(value) => Some(i128::from(*value)),
        PredicateValue::Year(value) => Some(if *value == 0 {
            0
        } else {
            1900 + i128::from(*value)
        }),
        PredicateValue::Time(value)
        | PredicateValue::DateTime(value)
        | PredicateValue::Timestamp(value) => Some(i128::from(*value)),
        PredicateValue::Null
        | PredicateValue::F64(_)
        | PredicateValue::Decimal(_, _)
        | PredicateValue::Value(_) => None,
    }
}

pub(in crate::engine::executor::select) fn runtime_filters_admit_predicate_values_indexed_deferred(
    precomputed: &[Option<CompiledRuntimeFilterProbe>],
    values: &[crate::engine::executor::vec::scan_bridge::PredicateValue],
    rejected: &mut [u64],
) -> bool {
    debug_assert_eq!(precomputed.len(), rejected.len());
    runtime_filters_admit_predicate_values_indexed_impl(precomputed, values, Some(rejected))
}

pub(in crate::engine::executor::select) fn flush_runtime_filter_rejections(
    precomputed: &[Option<CompiledRuntimeFilterProbe>],
    rejected: &mut [u64],
) {
    debug_assert_eq!(precomputed.len(), rejected.len());
    for (compiled, count) in precomputed.iter().zip(rejected) {
        let count = std::mem::take(count);
        if count != 0 {
            if let Some(compiled) = compiled {
                compiled.record_rejected_n(count);
            }
        }
    }
}

impl CompiledRuntimeFilterProbe {
    #[inline]
    pub(super) fn contains(&self, key: &[ValueKey]) -> bool {
        match &self.membership {
            CompiledRuntimeFilterMembership::Bloom { filter, .. } => {
                filter.contains_value_keys(key)
            }
            CompiledRuntimeFilterMembership::ExactInt { filter, .. } => match key {
                [ValueKey::Int(value)] => filter.contains(*value),
                // A schema/value mismatch cannot make a runtime filter reject
                // a row: the parent join remains the correctness check.
                _ => true,
            },
            CompiledRuntimeFilterMembership::KeySet { filter } => filter.contains(key),
        }
    }

    #[inline]
    pub(super) fn record_rejected(&self) {
        self.record_rejected_n(1);
    }

    #[inline]
    pub(super) fn record_rejected_n(&self, count: u64) {
        match &self.membership {
            CompiledRuntimeFilterMembership::Bloom { rejected, .. } => {
                record_hash_join_bloom_filtered_n(count);
                if let Some(rejected) = rejected {
                    rejected.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
                }
            }
            CompiledRuntimeFilterMembership::ExactInt { rejected, .. } => {
                record_hash_join_bloom_filtered_n(count);
                if let Some(rejected) = rejected.get() {
                    rejected.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
                }
            }
            CompiledRuntimeFilterMembership::KeySet { .. } => {
                INLJ_RUNTIME_KEYSET_CANDIDATE_REJECTED
                    .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}

/// If a runtime bloom filter's keys are all simple column references that
/// resolve to schema columns, return the column indices so the scan can
/// probe the bloom against `StoredRow.values` directly. Returns `None`
/// for any expression we can't resolve to a single column (e.g. arithmetic
/// or function calls in the join key) so the caller falls back to the
/// PlanRow-based path.
pub(super) fn compile_runtime_filter_probe_full(
    rf: &RuntimeFilter,
    schema: &crate::storage::schema::TableSchema,
) -> Option<CompiledRuntimeFilterProbe> {
    compile_runtime_filter_probe(rf, schema, None)
}

pub(super) fn runtime_filters_admit_values(
    precomputed: &[Option<CompiledRuntimeFilterProbe>],
    values: &[Value],
) -> bool {
    runtime_filters_admit_values_indexed(precomputed, values)
}

pub(in crate::engine::executor::select) fn runtime_filters_admit(
    runtime_filters: &[RuntimeFilter],
    row: &PlanRow,
) -> Result<bool, QueryError> {
    for rf in runtime_filters {
        match rf {
            RuntimeFilter::BloomKey {
                keys,
                null_safe,
                filter,
                rejected,
            } => {
                let mut value_keys = HashJoinKey::with_capacity(keys.len());
                let mut has_unsafe_null = false;
                for (idx, expr) in keys.iter().enumerate() {
                    let value = eval_plan_expr(expr, row)?;
                    let is_null = matches!(value, Value::Null);
                    let safe = null_safe.get(idx).copied().unwrap_or(false);
                    if is_null && !safe {
                        has_unsafe_null = true;
                        break;
                    }
                    value_keys.push(ValueKey::from(&value));
                }
                if has_unsafe_null {
                    record_hash_join_bloom_filtered();
                    if let Some(rejected) = rejected {
                        rejected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    return Ok(false);
                }
                if !filter.contains_value_keys(value_keys.as_slice()) {
                    // Phase E: scan-level bloom rejection bumps the
                    // same counter as the probe-time bloom so existing
                    // observability (`SHOW STATUS Hash_join_bloom_*`)
                    // accounts for both paths.
                    record_hash_join_bloom_filtered();
                    if let Some(rejected) = rejected {
                        rejected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    return Ok(false);
                }
            }
            RuntimeFilter::ExactIntKey {
                keys,
                null_safe,
                filter,
                rejected,
            } => {
                let Some(expr) = keys.first() else {
                    continue;
                };
                let value = eval_plan_expr(expr, row)?;
                if matches!(value, Value::Null) && !null_safe.first().copied().unwrap_or(false) {
                    record_hash_join_bloom_filtered();
                    if let Some(rejected) = rejected.get() {
                        rejected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    return Ok(false);
                }
                let ValueKey::Int(value) = ValueKey::from(&value) else {
                    continue;
                };
                if !filter.contains(value) {
                    record_hash_join_bloom_filtered();
                    if let Some(rejected) = rejected.get() {
                        rejected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    return Ok(false);
                }
            }
            RuntimeFilter::KeySet { .. } => {}
        }
    }
    Ok(true)
}

pub(in crate::engine::executor::select) fn read_plan_table_ids(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    ids: Vec<RowId>,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    if table.schema.is_partitioned() {
        // A RowId is local to one partition and therefore cannot be resolved
        // by probing every heap for the first matching numeric id. The
        // dispatcher above fans every partitioned B-tree operator out under
        // a single-storage-id prune context; preserve that storage identity
        // through the heap read and use the normal batched materializer.
        let storage_ids = table.in_scope_storage_ids();
        let [storage_id] = storage_ids.as_slice() else {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "partition index RowId reached heap read without a unique storage_id",
            ));
        };
        let heap = db.cached_heap_handle(*storage_id)?;
        return read_plan_table_ids_with_heap(db, txn, table, &heap, ids, filter, None, projection);
    }
    let heap = db.cached_heap_handle(table.schema.id)?;
    read_plan_table_ids_with_heap(db, txn, table, &heap, ids, filter, None, projection)
}

/// Index condition pushdown (ICP): the subset of a non-covering range
/// scan's residual filter that can be pre-evaluated on the index entry
/// itself, before paying the heap lookup. Pushed conjuncts are
/// evaluated on a row projected from the index key by the same
/// `IndexEntryProjector` covering scans trust; the full original
/// filter is still re-checked on the fetched heap row, so ICP is a
/// pure pre-filter — an MVCC-stale index entry that passes it can
/// never leak a wrong row into the result.
pub(in crate::engine::executor::select) struct IndexConditionPushdown {
    pub(super) projector: IndexEntryProjector,
    pub(super) condition: Expr,
}

pub(super) fn icp_enabled() -> bool {
    use std::sync::OnceLock;
    static FLAG: OnceLock<bool> = OnceLock::new();
    *FLAG.get_or_init(|| {
        crate::util::env::var("MYDB_ICP_DISABLE")
            .map(|v| !matches!(v.as_str(), "1" | "true" | "TRUE" | "on" | "ON"))
            .unwrap_or(true)
    })
}

/// Conservative whitelist walk: returns false for any expression shape
/// whose index-domain evaluation is not known to match heap-domain
/// evaluation (subqueries, functions, CASE, LIKE, user variables, ...).
/// Column references are collected into `out`.
pub(super) fn icp_collect_columns(expr: &Expr, out: &mut Vec<String>) -> bool {
    match expr {
        Expr::Identifier(name) => {
            out.push(name.to_ascii_lowercase());
            true
        }
        Expr::CompoundIdentifier(parts) => {
            if let Some(name) = parts.last() {
                out.push(name.to_ascii_lowercase());
            }
            true
        }
        Expr::Literal(_) | Expr::BoundValue(_) | Expr::Placeholder(_) => true,
        Expr::Unary { expr, .. } => icp_collect_columns(expr, out),
        Expr::Binary { left, op, right } => {
            matches!(
                op,
                BinaryOp::Or
                    | BinaryOp::And
                    | BinaryOp::Xor
                    | BinaryOp::Eq
                    | BinaryOp::NullSafeEq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
                    | BinaryOp::Add
                    | BinaryOp::Sub
                    | BinaryOp::Mul
                    | BinaryOp::Div
                    | BinaryOp::Mod
            ) && icp_collect_columns(left, out)
                && icp_collect_columns(right, out)
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => icp_collect_columns(inner, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            icp_collect_columns(expr, out)
                && icp_collect_columns(low, out)
                && icp_collect_columns(high, out)
        }
        Expr::InList { expr, list, .. } => {
            icp_collect_columns(expr, out) && list.iter().all(|item| icp_collect_columns(item, out))
        }
        _ => false,
    }
}

pub(in crate::engine::executor::select) enum CteRowPredicate {
    And(Box<CteRowPredicate>, Box<CteRowPredicate>),
    Or(Box<CteRowPredicate>, Box<CteRowPredicate>),
    Cmp {
        col: usize,
        op: BinaryOp,
        value: Value,
        flipped: bool,
    },
    InList {
        col: usize,
        values: Vec<Value>,
        negated: bool,
    },
    Between {
        col: usize,
        low: Value,
        high: Value,
        negated: bool,
    },
    /// Evaluate the inner column predicate under the column's collation:
    /// a materialized `utf8mb4_bin` column keeps binary comparison against
    /// a coercible literal, exactly as on the base table.
    Collated(u16, Box<CteRowPredicate>),
    Const(bool),
}

/// Bind a single-column leaf predicate to that column's string collation.
fn collated_cte_leaf(
    columns: &[crate::types::Column],
    col: usize,
    predicate: CteRowPredicate,
) -> CteRowPredicate {
    match columns[col].source.collation_id {
        Some(id) if id != crate::charset::BINARY_COLLATION_ID => {
            CteRowPredicate::Collated(id, Box::new(predicate))
        }
        _ => predicate,
    }
}

pub(in crate::engine::executor::select) fn compile_cte_row_predicate(
    expr: &Expr,
    columns: &[crate::types::Column],
) -> Option<CteRowPredicate> {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => Some(CteRowPredicate::And(
            Box::new(compile_cte_row_predicate(left, columns)?),
            Box::new(compile_cte_row_predicate(right, columns)?),
        )),
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => Some(CteRowPredicate::Or(
            Box::new(compile_cte_row_predicate(left, columns)?),
            Box::new(compile_cte_row_predicate(right, columns)?),
        )),
        Expr::Binary { left, op, right }
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NullSafeEq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
            ) =>
        {
            if let Some(col) = cte_column_index(left, columns) {
                if let Some(value) = crate::engine::expr::expr_literal_value(right) {
                    let cmp = CteRowPredicate::Cmp {
                        col,
                        op: *op,
                        value,
                        flipped: false,
                    };
                    return Some(collated_cte_leaf(columns, col, cmp));
                }
            }
            if let Some(col) = cte_column_index(right, columns) {
                if let Some(value) = crate::engine::expr::expr_literal_value(left) {
                    let cmp = CteRowPredicate::Cmp {
                        col,
                        op: *op,
                        value,
                        flipped: true,
                    };
                    return Some(collated_cte_leaf(columns, col, cmp));
                }
            }
            None
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let col = cte_column_index(expr, columns)?;
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                values.push(crate::engine::expr::expr_literal_value(item)?);
            }
            let in_list = CteRowPredicate::InList {
                col,
                values,
                negated: *negated,
            };
            Some(collated_cte_leaf(columns, col, in_list))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let col = cte_column_index(expr, columns)?;
            let between = CteRowPredicate::Between {
                col,
                low: crate::engine::expr::expr_literal_value(low)?,
                high: crate::engine::expr::expr_literal_value(high)?,
                negated: *negated,
            };
            Some(collated_cte_leaf(columns, col, between))
        }
        Expr::Literal(_) | Expr::BoundValue(_) | Expr::Placeholder(_) => {
            let value = crate::engine::expr::expr_literal_value(expr)?;
            Some(CteRowPredicate::Const(crate::engine::expr::truthy(&value)))
        }
        _ => None,
    }
}

pub(super) fn cte_column_index(expr: &Expr, columns: &[crate::types::Column]) -> Option<usize> {
    let name = match expr {
        Expr::Identifier(name) => name,
        Expr::CompoundIdentifier(parts) => parts.last()?,
        _ => return None,
    };
    columns
        .iter()
        .position(|col| col.name.eq_ignore_ascii_case(name))
}

pub(in crate::engine::executor::select) fn eval_cte_row_predicate(
    predicate: &CteRowPredicate,
    row: &[Value],
) -> Result<bool, QueryError> {
    match predicate {
        CteRowPredicate::And(left, right) => {
            Ok(eval_cte_row_predicate(left, row)? && eval_cte_row_predicate(right, row)?)
        }
        CteRowPredicate::Or(left, right) => {
            Ok(eval_cte_row_predicate(left, row)? || eval_cte_row_predicate(right, row)?)
        }
        CteRowPredicate::Cmp {
            col,
            op,
            value,
            flipped,
        } => eval_cte_row_cmp(row.get(*col).unwrap_or(&Value::Null), *op, value, *flipped),
        CteRowPredicate::InList {
            col,
            values,
            negated,
        } => {
            let value = row.get(*col).unwrap_or(&Value::Null);
            let mut found = false;
            if !matches!(value, Value::Null) {
                for item in values {
                    if !matches!(item, Value::Null)
                        && crate::engine::expr::compare_values(value, item)?
                            == std::cmp::Ordering::Equal
                    {
                        found = true;
                        break;
                    }
                }
            }
            Ok(if *negated {
                !matches!(value, Value::Null)
                    && !found
                    && !values.iter().any(|item| matches!(item, Value::Null))
            } else {
                found
            })
        }
        CteRowPredicate::Between {
            col,
            low,
            high,
            negated,
        } => {
            let value = row.get(*col).unwrap_or(&Value::Null);
            if matches!(value, Value::Null)
                || matches!(low, Value::Null)
                || matches!(high, Value::Null)
            {
                return Ok(false);
            }
            let ge_low =
                crate::engine::expr::compare_values(value, low)? != std::cmp::Ordering::Less;
            let le_high =
                crate::engine::expr::compare_values(value, high)? != std::cmp::Ordering::Greater;
            let result = ge_low && le_high;
            Ok(if *negated { !result } else { result })
        }
        CteRowPredicate::Collated(id, inner) => {
            crate::engine::expr::with_compare_collation(Some(*id), || {
                eval_cte_row_predicate(inner, row)
            })
        }
        CteRowPredicate::Const(value) => Ok(*value),
    }
}

pub(super) fn eval_cte_row_cmp(
    left: &Value,
    op: BinaryOp,
    right: &Value,
    flipped: bool,
) -> Result<bool, QueryError> {
    if op == BinaryOp::NullSafeEq {
        if matches!((left, right), (Value::Null, Value::Null)) {
            return Ok(true);
        }
        if matches!(left, Value::Null) || matches!(right, Value::Null) {
            return Ok(false);
        }
    } else if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(false);
    }
    let ord = crate::engine::expr::compare_values(left, right)?;
    let ord = if flipped { ord.reverse() } else { ord };
    Ok(match op {
        BinaryOp::Eq | BinaryOp::NullSafeEq => ord == std::cmp::Ordering::Equal,
        BinaryOp::NotEq => ord != std::cmp::Ordering::Equal,
        BinaryOp::Lt => ord == std::cmp::Ordering::Less,
        BinaryOp::LtEq => ord != std::cmp::Ordering::Greater,
        BinaryOp::Gt => ord == std::cmp::Ordering::Greater,
        BinaryOp::GtEq => ord != std::cmp::Ordering::Less,
        _ => false,
    })
}

/// Index-domain evaluation is trusted whenever the key-to-value
/// round-trip and the eventual expression comparison both round-trip
/// the heap-domain semantics bit-for-bit.
///
/// Numeric / temporal types round-trip cleanly: their KeyPart variants
/// hold the same canonical i64/u64/days/microseconds magnitudes the
/// `Value::*` form does, and the operator dispatch never consults
/// collation, so the index-domain pre-filter agrees with the heap-domain
/// re-check by construction.
///
/// Binary string families (`BINARY` / `VARBINARY` / `TINYBLOB` … `LONGBLOB`)
/// store and compare as raw bytes both at the IndexKey layer (always
/// `KeyPart::Bytes`) and at the expression layer (no collation lookup
/// on `Value::Bytes`), so they're equally safe.
///
/// Character string families (`CHAR` / `VARCHAR` / `TINYTEXT` … `LONGTEXT`)
/// are only safe under a binary collation. With a binary collation,
/// `charset::compare_strings_for_info` short-circuits to
/// `left.as_bytes().cmp(right.as_bytes())` (charset.rs:1018-1022) — the
/// same byte-wise ordering the IndexKey's derived `Ord` uses. For a
/// case-insensitive / accent-insensitive collation, ICP would have to
/// re-derive the collation key per entry, which the projector doesn't
/// do — so we conservatively bail out and let the heap-side re-check
/// keep the wide-collation correctness story unchanged. The conservatism
/// here is intentional: ICP is a pure pre-filter, so over-bailing only
/// costs throughput, never correctness.
pub(super) fn icp_safe_column(column: &crate::storage::ColumnSchema) -> bool {
    use DataTypeKind::*;
    match &column.data_type {
        TinyInt
        | SmallInt
        | MediumInt
        | Int
        | BigInt
        | Bool
        | Float { .. }
        | Double { .. }
        | Decimal { .. }
        | Date
        | Time { .. }
        | DateTime { .. }
        | Timestamp { .. }
        | Year => true,
        Binary { .. } | VarBinary { .. } | Blob { .. } => true,
        Char { .. } | VarChar { .. } | Text { .. } => column
            .collation
            .as_deref()
            .and_then(|name| crate::charset::resolve_collation(name).ok())
            .is_some_and(|info| info.binary),
        _ => false,
    }
}

impl IndexConditionPushdown {
    pub(in crate::engine::executor::select) fn try_new(
        table: &PlanTable,
        index: &IndexSchema,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        filter: &Option<Expr>,
    ) -> Option<Self> {
        if !icp_enabled() {
            return None;
        }
        let filter = filter.as_ref()?;
        if index.has_multi_valued_parts() || !index.key_expressions.is_empty() {
            return None;
        }
        // Index columns usable for pushdown: full (non-prefix) columns
        // of an ICP-safe type. `column_prefix_lengths` aligns with
        // `columns`; INCLUDE columns are always stored whole.
        let mut safe: std::collections::HashSet<String> = std::collections::HashSet::new();
        // Columns already constrained by the scan bounds. The residual
        // filter usually re-states the bound predicates (the heap-side
        // re-check needs them), but on the *entry* domain they are true
        // by construction — pushing a conjunct that only touches bound
        // columns buys zero filtering and pays a projection + eval per
        // entry. Measured on TPC-H date-range scans (Q4/Q10/Q14) that
        // pure overhead was a 15-50% per-query regression.
        let bound_prefix = low
            .map(|b| b.key.0.len())
            .max(high.map(|b| b.key.0.len()))
            .unwrap_or(0);
        let mut beyond_bounds: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (pos, name) in index.effective_columns().iter().enumerate() {
            let truncated = pos < index.columns.len()
                && index
                    .column_prefix_lengths
                    .get(pos)
                    .is_some_and(|len| len.is_some());
            if truncated {
                continue;
            }
            let lowered = name.to_ascii_lowercase();
            let Some(column) = table
                .schema
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name))
            else {
                continue;
            };
            if icp_safe_column(column) {
                if pos >= bound_prefix {
                    beyond_bounds.insert(lowered.clone());
                }
                safe.insert(lowered);
            }
        }
        if beyond_bounds.is_empty() {
            return None;
        }
        let mut conjuncts: Vec<&Expr> = Vec::new();
        collect_and_conjuncts(filter, &mut conjuncts);
        let mut pushable: Vec<Expr> = Vec::new();
        for conjunct in conjuncts {
            let mut cols = Vec::new();
            if icp_collect_columns(conjunct, &mut cols)
                && !cols.is_empty()
                && cols.iter().all(|col| safe.contains(col))
                && cols.iter().any(|col| beyond_bounds.contains(col))
            {
                pushable.push(conjunct.clone());
            }
        }
        let condition = pushable.into_iter().reduce(|left, right| Expr::Binary {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        })?;
        Some(Self {
            projector: IndexEntryProjector::new(table, index, None),
            condition,
        })
    }

    pub(in crate::engine::executor::select) fn matches(
        &self,
        db: &Arc<Database>,
        txn: TxnView,
        key: &IndexKey,
        eval_ctx: &mut EvalQueryContext,
    ) -> Result<bool, QueryError> {
        let row = self.projector.project(key);
        eval_plan_bool_with_db(db, txn, &self.condition, &row, eval_ctx)
    }
}

pub(super) fn collect_and_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Binary { left, op, right } if *op == BinaryOp::And => {
            collect_and_conjuncts(left, out);
            collect_and_conjuncts(right, out);
        }
        other => out.push(other),
    }
}

pub(super) const MRR_BYTES_PER_RID: usize = 32;
pub(super) const MRR_MIN_WINDOW_ROWS: usize = 4_096;
pub(super) const MRR_MAX_WINDOW_ROWS: usize = 262_144;

/// Runtime-only work area for non-covering index-to-heap fetches. Its grant
/// controls I/O batching but is intentionally absent from `PhysicalPlan` and
/// every plan-cache key.
pub(crate) struct MrrWorkArea {
    pub(super) handle: Option<crate::engine::executor::work_area::WorkAreaHandle>,
    pub(super) window_rows: usize,
    pub(super) charged_bytes: usize,
    pub(super) spill_released: AtomicBool,
}

impl MrrWorkArea {
    pub(crate) fn new() -> Self {
        if !mrr_sort_enabled() {
            return Self {
                handle: None,
                window_rows: 4_096,
                charged_bytes: 0,
                spill_released: AtomicBool::new(true),
            };
        }

        use crate::engine::executor::work_area::{SpillHint, WorkAreaHandle};

        let handle = WorkAreaHandle::register();
        let fair_bytes = handle.manager().fair_share_bytes();
        let target_bytes = fair_bytes
            .min(MRR_MAX_WINDOW_ROWS * MRR_BYTES_PER_RID)
            .max(MRR_MIN_WINDOW_ROWS * MRR_BYTES_PER_RID);
        let spill = matches!(handle.try_charge(target_bytes), SpillHint::Spill);
        let window_rows = if spill {
            // MRR has no disk-spill representation: the current RID window is
            // itself the spill unit. Return the denied reservation at once
            // and continue with the 4K minimum window.
            handle.release(target_bytes);
            MRR_MIN_WINDOW_ROWS
        } else {
            (target_bytes / MRR_BYTES_PER_RID).clamp(MRR_MIN_WINDOW_ROWS, MRR_MAX_WINDOW_ROWS)
        };
        if spill {
            MRR_WORKAREA_FLUSHES.fetch_add(1, Ordering::Relaxed);
            crate::engine::executor::select::analyze_stats::add_mrr_work(0, 0, 0, 1, 0);
        }
        Self {
            handle: Some(handle),
            window_rows,
            charged_bytes: if spill { 0 } else { target_bytes },
            spill_released: AtomicBool::new(spill),
        }
    }

    pub(crate) fn window_rows(&self) -> usize {
        let Some(handle) = self.handle.as_ref() else {
            return self.window_rows;
        };
        if self.spill_released.load(Ordering::Relaxed) {
            return MRR_MIN_WINDOW_ROWS;
        }
        if handle.must_spill()
            && self
                .spill_released
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            // A new contender or a runtime target shrink can revoke the
            // earlier fair share between windows. Flush by releasing the
            // reservation and use minimum windows for the rest of this scan.
            handle.release(self.charged_bytes);
            MRR_WORKAREA_FLUSHES.fetch_add(1, Ordering::Relaxed);
            crate::engine::executor::select::analyze_stats::add_mrr_work(0, 0, 0, 1, 0);
            return MRR_MIN_WINDOW_ROWS;
        }
        self.window_rows
    }

    /// Cap a RID window by the caller's full resident footprint, not merely
    /// the 32-byte MRR bookkeeping estimate. Native columnar scans retain
    /// decoded projected values until the Batch is built, so wide projections
    /// must consume fewer rows from the same grant.
    pub(crate) fn window_rows_for_resident_bytes(
        &self,
        resident_bytes_per_row: usize,
        max_rows: usize,
    ) -> usize {
        let rows = self.window_rows().min(max_rows).max(1);
        if self.spill_released.load(Ordering::Relaxed) {
            return rows.min(MRR_MIN_WINDOW_ROWS);
        }
        rows.min((self.charged_bytes / resident_bytes_per_row.max(1)).max(1))
    }
}

pub(super) fn read_plan_table_ids_range_chunked(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    mut low: Option<IndexBound>,
    mut high: Option<IndexBound>,
    reverse: bool,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    crate::planner::boolean_range::refine_runtime(
        &table.schema,
        index,
        filter.as_ref(),
        &mut low,
        &mut high,
    );
    if db.has_pending_index_repairs_for(table.schema.id, &index.name) {
        let ids = range_index_ids_visible_ordered(db, txn, table, index, low, high, reverse)?;
        return read_plan_table_ids_with_heap(
            db,
            txn,
            table,
            &index_scan_heap(db, table)?,
            ids,
            filter,
            None,
            projection,
        );
    }

    // ICP: pre-filter index entries before the heap lookup when part
    // of the residual filter is evaluable on the index key.
    if let Some(icp) =
        IndexConditionPushdown::try_new(table, index, low.as_ref(), high.as_ref(), filter)
    {
        return read_plan_table_ids_range_icp(
            db, txn, table, index, low, high, reverse, filter, projection, &icp,
        );
    }

    let mrr = MrrWorkArea::new();
    let mut rows = Vec::new();
    let mut eval_ctx = EvalQueryContext::default();
    let mut seen = (!index.unique).then(std::collections::HashSet::new);
    let heap = index_scan_heap(db, table)?;

    if reverse {
        let mut cursor = db.range_index_snapshot_reverse_cursor(
            &table.schema,
            index,
            low.clone(),
            high.clone(),
            txn,
        )?;
        loop {
            // Non-covering: keys are never consulted — pull bare RowIds.
            let chunk = cursor.next_chunk_row_ids(mrr.window_rows())?;
            if chunk.is_empty() {
                break;
            }
            read_visible_row_id_chunk_with_heap(
                db,
                txn,
                table,
                &heap,
                filter,
                &mut eval_ctx,
                &mut rows,
                &mut seen,
                chunk,
                projection,
            )?;
        }
    } else {
        let mut cursor =
            db.range_index_snapshot_cursor(&table.schema, index, low.clone(), high.clone(), txn)?;
        loop {
            // Non-covering: the index key is never consulted past this
            // point, so pull bare RowIds and skip the per-entry
            // IndexKey clone.
            let chunk = cursor.next_chunk_row_ids(mrr.window_rows())?;
            if chunk.is_empty() {
                break;
            }
            read_visible_row_id_chunk_with_heap(
                db,
                txn,
                table,
                &heap,
                filter,
                &mut eval_ctx,
                &mut rows,
                &mut seen,
                chunk,
                projection,
            )?;
        }
    }

    Ok(rows)
}

/// Non-covering range scan with index condition pushdown: stream
/// borrowed entries off the cursor, evaluate the pushed conjuncts on
/// the projected key, and heap-fetch only the survivors (in index
/// order, preserving the heap-read batching and prefetch window). The
/// full filter is re-applied to the heap rows.
#[allow(clippy::too_many_arguments)]
pub(super) fn read_plan_table_ids_range_icp(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    reverse: bool,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
    icp: &IndexConditionPushdown,
) -> Result<Vec<PlanRow>, QueryError> {
    let mrr = MrrWorkArea::new();
    let heap = index_scan_heap(db, table)?;
    let mut rows = Vec::new();
    let mut eval_ctx = EvalQueryContext::default();
    let mut icp_ctx = EvalQueryContext::default();
    let mut seen = (!index.unique).then(std::collections::HashSet::new);
    let mut pending: Vec<RowId> = Vec::with_capacity(mrr.window_rows());
    {
        let mut consume = |entries: &[(&IndexKey, RowId)]| -> Result<(), QueryError> {
            for (key, row_id) in entries {
                if !icp.matches(db, txn, key, &mut icp_ctx)? {
                    continue;
                }
                pending.push(*row_id);
                if pending.len() >= mrr.window_rows() {
                    read_visible_row_id_chunk_with_heap(
                        db,
                        txn,
                        table,
                        &heap,
                        filter,
                        &mut eval_ctx,
                        &mut rows,
                        &mut seen,
                        std::mem::take(&mut pending),
                        projection,
                    )?;
                }
            }
            Ok(())
        };
        if reverse {
            let mut cursor =
                db.range_index_snapshot_reverse_cursor(&table.schema, index, low, high, txn)?;
            cursor.visit_entries(&mut consume)?;
        } else {
            let mut cursor =
                db.range_index_snapshot_cursor(&table.schema, index, low, high, txn)?;
            cursor.visit_entries(&mut consume)?;
        }
    }
    if !pending.is_empty() {
        read_visible_row_id_chunk_with_heap(
            db,
            txn,
            table,
            &heap,
            filter,
            &mut eval_ctx,
            &mut rows,
            &mut seen,
            pending,
            projection,
        )?;
    }
    Ok(rows)
}

/// LIMIT-aware ICP variant of `read_plan_table_ids_range_icp`. Same
/// borrowed-entry walk + index-key pre-filter, but stops feeding the
/// heap-fetch sink once `max_rows` survivors have landed in `rows`.
#[allow(clippy::too_many_arguments)]
pub(super) fn read_plan_table_ids_range_icp_limited(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    reverse: bool,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
    max_rows: usize,
    icp: &IndexConditionPushdown,
) -> Result<Vec<PlanRow>, QueryError> {
    let mrr = MrrWorkArea::new();
    let heap = index_scan_heap(db, table)?;
    let mut rows = Vec::with_capacity(max_rows.min(64));
    let mut eval_ctx = EvalQueryContext::default();
    let mut icp_ctx = EvalQueryContext::default();
    let mut seen = (!index.unique).then(std::collections::HashSet::new);
    let mut pending: Vec<RowId> = Vec::with_capacity(mrr.window_rows().min(max_rows.max(1)));
    let mut stop = false;
    {
        let mut consume = |entries: &[(&IndexKey, RowId)]| -> Result<(), QueryError> {
            if stop {
                return Ok(());
            }
            for (key, row_id) in entries {
                if rows.len() >= max_rows {
                    stop = true;
                    return Ok(());
                }
                if !icp.matches(db, txn, key, &mut icp_ctx)? {
                    continue;
                }
                pending.push(*row_id);
                if pending.len() >= mrr.window_rows() {
                    read_visible_row_id_chunk_limited_with_heap(
                        db,
                        txn,
                        table,
                        &heap,
                        filter,
                        &mut eval_ctx,
                        &mut rows,
                        &mut seen,
                        std::mem::take(&mut pending),
                        max_rows,
                        projection,
                    )?;
                    if rows.len() >= max_rows {
                        stop = true;
                        return Ok(());
                    }
                }
            }
            Ok(())
        };
        if reverse {
            let mut cursor =
                db.range_index_snapshot_reverse_cursor(&table.schema, index, low, high, txn)?;
            cursor.visit_entries(&mut consume)?;
        } else {
            let mut cursor =
                db.range_index_snapshot_cursor(&table.schema, index, low, high, txn)?;
            cursor.visit_entries(&mut consume)?;
        }
    }
    if !pending.is_empty() && rows.len() < max_rows {
        read_visible_row_id_chunk_limited_with_heap(
            db,
            txn,
            table,
            &heap,
            filter,
            &mut eval_ctx,
            &mut rows,
            &mut seen,
            pending,
            max_rows,
            projection,
        )?;
    }
    Ok(rows)
}

pub(super) fn read_plan_table_ids_range_chunked_limited(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    mut low: Option<IndexBound>,
    mut high: Option<IndexBound>,
    reverse: bool,
    filter: &Option<Expr>,
    max_rows: usize,
    projection: Option<&[usize]>,
    allow_icp: bool,
) -> Result<Vec<PlanRow>, QueryError> {
    crate::planner::boolean_range::refine_runtime(
        &table.schema,
        index,
        filter.as_ref(),
        &mut low,
        &mut high,
    );
    if max_rows == 0 {
        return Ok(Vec::new());
    }
    if db.has_pending_index_repairs_for(table.schema.id, &index.name) {
        let ids = range_index_ids_visible_ordered(db, txn, table, index, low, high, reverse)?;
        return read_plan_table_ids_with_heap(
            db,
            txn,
            table,
            &index_scan_heap(db, table)?,
            ids,
            filter,
            Some(max_rows),
            projection,
        );
    }

    // ICP: pre-filter index entries before the heap lookup when part
    // of the residual filter is evaluable on the index key. Symmetric
    // with the unbounded sibling `read_plan_table_ids_range_chunked`
    // — a LIMIT cap doesn't change the pre-filter's correctness
    // (every entry the ICP rejects could never satisfy the filter
    // anyway), and the pruning is strictly cheaper than paying the
    // heap fetch + full filter on each non-matching row.
    if let Some(icp) = allow_icp
        .then(|| IndexConditionPushdown::try_new(table, index, low.as_ref(), high.as_ref(), filter))
        .flatten()
    {
        return read_plan_table_ids_range_icp_limited(
            db, txn, table, index, low, high, reverse, filter, projection, max_rows, &icp,
        );
    }

    let mrr = MrrWorkArea::new();
    const MIN_CHUNK: usize = 64;
    let mut rows = Vec::with_capacity(max_rows.min(64));
    let mut eval_ctx = EvalQueryContext::default();
    let mut seen = (!index.unique).then(std::collections::HashSet::new);
    let heap = index_scan_heap(db, table)?;

    if reverse {
        let mut cursor = db.range_index_snapshot_reverse_cursor(
            &table.schema,
            index,
            low.clone(),
            high.clone(),
            txn,
        )?;
        loop {
            let remaining = max_rows.saturating_sub(rows.len());
            if remaining == 0 {
                break;
            }
            let chunk_size = remaining.max(MIN_CHUNK).min(mrr.window_rows());
            // Non-covering: keys are never consulted — pull bare RowIds.
            let chunk = cursor.next_chunk_row_ids(chunk_size)?;
            if chunk.is_empty() {
                break;
            }
            read_visible_row_id_chunk_limited_with_heap(
                db,
                txn,
                table,
                &heap,
                filter,
                &mut eval_ctx,
                &mut rows,
                &mut seen,
                chunk,
                max_rows,
                projection,
            )?;
        }
    } else {
        let mut cursor =
            db.range_index_snapshot_cursor(&table.schema, index, low.clone(), high.clone(), txn)?;
        loop {
            let remaining = max_rows.saturating_sub(rows.len());
            if remaining == 0 {
                break;
            }
            let chunk_size = remaining.max(MIN_CHUNK).min(mrr.window_rows());
            // Non-covering: keys are never consulted — pull bare RowIds.
            let chunk = cursor.next_chunk_row_ids(chunk_size)?;
            if chunk.is_empty() {
                break;
            }
            read_visible_row_id_chunk_limited_with_heap(
                db,
                txn,
                table,
                &heap,
                filter,
                &mut eval_ctx,
                &mut rows,
                &mut seen,
                chunk,
                max_rows,
                projection,
            )?;
        }
    }

    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn read_plan_table_ids_range_windowed(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    reverse: bool,
    projection: Option<&[usize]>,
    offset: usize,
    limit: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mrr = MrrWorkArea::new();
    let heap = index_scan_heap(db, table)?;
    let mut skip = offset;
    let mut page_ids: Vec<RowId> = Vec::with_capacity(limit.min(64));
    let mut seen = (!index.unique).then(std::collections::HashSet::new);
    let mut cursor = if reverse {
        AnyRangeCursor::Reverse(db.range_index_snapshot_reverse_cursor(
            &table.schema,
            index,
            low,
            high,
            txn,
        )?)
    } else {
        AnyRangeCursor::Forward(db.range_index_snapshot_cursor(
            &table.schema,
            index,
            low,
            high,
            txn,
        )?)
    };
    while page_ids.len() < limit {
        // The MRR grant is an upper bound, not a mandatory fetch size.
        // Ordered pagination without a residual filter needs only the
        // remaining visible OFFSET + LIMIT rows. Pulling the whole work-area
        // window made `LIMIT 20 OFFSET 2` decode and visibility-check up to
        // 262K index entries. If tombstones/invisible versions leave us
        // short, the cursor simply refills with the newly remaining demand.
        let remaining = skip.saturating_add(limit.saturating_sub(page_ids.len()));
        let chunk_rows = remaining.max(1).min(mrr.window_rows());
        let (chunk, page_max_trx_id) = cursor.next_leaf_chunk_row_ids(chunk_rows)?;
        if chunk.is_empty() {
            break;
        }
        let mut row_ids = chunk;
        if let Some(seen) = seen.as_mut() {
            row_ids.retain(|row_id| seen.insert(*row_id));
        }
        if row_ids.is_empty() {
            continue;
        }
        record_mrr_window(&row_ids);
        // The cursor deliberately stops at a leaf boundary, so its persisted
        // PAGE_MAX_TRX_ID applies to every RowId in this batch. Old/static
        // leaves can therefore skip heap-header visibility probes entirely;
        // concurrent or recent leaves fall back to the full MVCC oracle.
        let visible_mask = db
            .try_index_page_max_trx_shortcut_for(
                table.schema.id,
                &index.name,
                page_max_trx_id,
                txn,
                row_ids.len(),
            )
            .map(Ok::<_, QueryError>)
            .unwrap_or_else(|| {
                db.filter_visible_row_ids_mask_with_handle(&heap, &table.schema, &row_ids, txn)
            })?;
        for (row_id, visible) in row_ids.into_iter().zip(visible_mask.into_iter()) {
            if !visible {
                continue;
            }
            if skip > 0 {
                skip -= 1;
                continue;
            }
            page_ids.push(row_id);
            if page_ids.len() >= limit {
                break;
            }
        }
    }
    materialize_row_ids_in_order(db, txn, table, &heap, &page_ids, projection)
}

pub(super) fn materialize_row_ids_in_order(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    heap: &HeapHandle,
    row_ids: &[RowId],
    projection: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    if row_ids.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(cols) = projection {
        let mut by_id = db
            .read_row_columns_many_with_handle(heap, &table.schema, row_ids, txn, cols)?
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        let mut rows = Vec::with_capacity(row_ids.len());
        for row_id in row_ids {
            if let Some(values) = by_id.remove(row_id) {
                rows.push(projected_values_to_plan_row(table, cols, values)?);
            }
        }
        Ok(rows)
    } else {
        let mut by_id = db
            .read_rows_many_with_handle(heap, &table.schema, row_ids, txn)?
            .into_iter()
            .map(|row| (row.row_id, row.values))
            .collect::<std::collections::HashMap<_, _>>();
        let mut rows = Vec::with_capacity(row_ids.len());
        for row_id in row_ids {
            if let Some(values) = by_id.remove(row_id) {
                rows.push(stored_row_to_plan_row(table, values)?);
            }
        }
        Ok(rows)
    }
}

/// `MYDB_MRR_SORT` is a diagnostic execution-only oracle. MRR is on by
/// default; explicit `0/off/false` restores the legacy 4K-window path without
/// changing the cached plan or EXPLAIN output.
pub(super) fn mrr_sort_enabled() -> bool {
    !crate::util::env::var("MYDB_MRR_SORT")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no" | "disable" | "disabled"
            )
        })
        .unwrap_or(false)
}

pub(crate) fn record_mrr_window(chunk: &[RowId]) {
    if !mrr_sort_enabled() || chunk.is_empty() {
        return;
    }
    let mut pages: Vec<u32> = chunk.iter().map(|row_id| row_id.page_id).collect();
    pages.sort_unstable();
    pages.dedup();
    let rids = chunk.len() as u64;
    let unique_pages = pages.len() as u64;
    let page_reuses = rids.saturating_sub(unique_pages);
    let bytes = rids.saturating_mul(MRR_BYTES_PER_RID as u64);
    MRR_WINDOWS.fetch_add(1, Ordering::Relaxed);
    MRR_RIDS.fetch_add(rids, Ordering::Relaxed);
    MRR_UNIQUE_PAGES.fetch_add(unique_pages, Ordering::Relaxed);
    MRR_PAGE_REUSES.fetch_add(page_reuses, Ordering::Relaxed);
    MRR_PEAK_BYTES.fetch_max(bytes, Ordering::Relaxed);
    crate::engine::executor::select::analyze_stats::add_mrr_work(1, rids, unique_pages, 0, bytes);
}

/// Visible-row heap fetch for RowId chunks pulled via
/// `IndexRangeCursor::next_chunk_row_ids` — the non-covering path never
/// looks at the index key, so the chunk arrives without any `IndexKey`
/// clones having been paid.
pub(in crate::engine::executor::select) fn read_visible_row_id_chunk_with_heap(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    heap: &HeapHandle,
    filter: &Option<Expr>,
    eval_ctx: &mut EvalQueryContext,
    rows: &mut Vec<PlanRow>,
    seen: &mut Option<std::collections::HashSet<RowId>>,
    mut chunk: Vec<RowId>,
    projection: Option<&[usize]>,
) -> Result<(), QueryError> {
    if let Some(seen) = seen.as_mut() {
        chunk.retain(|row_id| seen.insert(*row_id));
    }
    if chunk.is_empty() {
        return Ok(());
    }
    record_mrr_window(&chunk);
    if crate::engine::executor::select::analyze_stats::is_recording() {
        let mut pages: Vec<u32> = chunk.iter().map(|row_id| row_id.page_id).collect();
        pages.sort_unstable();
        pages.dedup();
        crate::engine::executor::select::analyze_stats::add_range_scan_work(
            chunk.len() as u64,
            chunk.len() as u64,
            pages.len() as u64,
        );
    }

    // Storage sorts the missing-page subset for one pin/decode per page;
    // materialization maps it back to `chunk` order, preserving index order.
    for row in materialize_row_ids_in_order(db, txn, table, heap, &chunk, projection)? {
        if filter
            .as_ref()
            .map(|expr| eval_plan_bool_with_db(db, txn, expr, &row, eval_ctx))
            .transpose()?
            .unwrap_or(true)
        {
            rows.push(row);
        }
    }
    Ok(())
}

/// Limited (max_rows-capped) variant of `read_visible_row_id_chunk_with_heap`.
pub(super) fn read_visible_row_id_chunk_limited_with_heap(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    heap: &HeapHandle,
    filter: &Option<Expr>,
    eval_ctx: &mut EvalQueryContext,
    rows: &mut Vec<PlanRow>,
    seen: &mut Option<std::collections::HashSet<RowId>>,
    mut chunk: Vec<RowId>,
    max_rows: usize,
    projection: Option<&[usize]>,
) -> Result<(), QueryError> {
    if let Some(seen) = seen.as_mut() {
        chunk.retain(|row_id| seen.insert(*row_id));
    }
    if chunk.is_empty() {
        return Ok(());
    }
    record_mrr_window(&chunk);
    if crate::engine::executor::select::analyze_stats::is_recording() {
        let mut pages: Vec<u32> = chunk.iter().map(|row_id| row_id.page_id).collect();
        pages.sort_unstable();
        pages.dedup();
        crate::engine::executor::select::analyze_stats::add_range_scan_work(
            chunk.len() as u64,
            chunk.len() as u64,
            pages.len() as u64,
        );
    }
    for row in materialize_row_ids_in_order(db, txn, table, heap, &chunk, projection)? {
        if rows.len() >= max_rows {
            break;
        }
        if filter
            .as_ref()
            .map(|expr| eval_plan_bool_with_db(db, txn, expr, &row, eval_ctx))
            .transpose()?
            .unwrap_or(true)
        {
            rows.push(row);
        }
    }
    Ok(())
}

/// Heap handle an index scan uses for its MVCC visibility probe / heap
/// fetch. For a partitioned table the entries come from one partition's
/// local index (the executor fans the scan out per partition), so the
/// probe must target that partition's heap — the base `table.id` heap
/// is empty. Resolves the in-scope partition from the active prune
/// context; unpartitioned tables read `table.id`.
pub(crate) fn index_scan_heap(
    db: &Arc<Database>,
    table: &PlanTable,
) -> Result<HeapHandle, QueryError> {
    let storage_id = table
        .in_scope_storage_ids()
        .into_iter()
        .next()
        .unwrap_or(table.schema.id);
    db.cached_heap_handle(storage_id)
}

/// Append one borrowed covering-index batch to `rows`.
///
/// The caller owns the output materialization policy, while this helper keeps
/// the storage-side index entries borrowed from the pinned leaf page.  In
/// particular it must never clone a whole worker leaf range into
/// `Vec<(IndexKey, RowId)>`; the input batch is bounded by the storage visitor.
pub(in crate::engine::executor::select) fn append_plan_table_entry_refs_chunk(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    heap: &HeapHandle,
    entries: &[(&IndexKey, RowId)],
    page_max_trx_id: u64,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
    rows: &mut Vec<PlanRow>,
    eval_ctx: &mut EvalQueryContext,
) -> Result<(), QueryError> {
    let projector = IndexEntryProjector::new(table, index, projection);
    let mut compiled_filter = None;
    let visible_mask = db
        .try_index_page_max_trx_shortcut_for(
            table.schema.id,
            &index.name,
            page_max_trx_id,
            txn,
            entries.len(),
        )
        .map(Ok::<_, QueryError>)
        .unwrap_or_else(|| {
            db.filter_visible_index_entries_refs_mask_with_handle(
                heap,
                &table.schema,
                index,
                entries,
                txn,
            )
        })?;
    for ((key, _row_id), visible) in entries.iter().zip(visible_mask.into_iter()) {
        if !visible {
            continue;
        }
        let row = projector.project(key);
        if eval_covering_filter(db, txn, filter, &row, eval_ctx, &mut compiled_filter)? {
            rows.push(row);
        }
    }
    Ok(())
}

pub(in crate::engine::executor::select) fn read_plan_table_entries(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    entries: Vec<(IndexKey, RowId)>,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    let heap = index_scan_heap(db, table)?;
    read_plan_table_entries_with_heap(db, txn, table, index, &heap, entries, filter, projection)
}

pub(super) fn read_plan_table_entries_with_visibility_bound(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    entries: Vec<(IndexKey, RowId)>,
    page_max_trx_id: Option<u64>,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    let heap = index_scan_heap(db, table)?;
    read_plan_table_entries_with_heap_and_bound(
        db,
        txn,
        table,
        index,
        &heap,
        entries,
        page_max_trx_id,
        filter,
        projection,
    )
}

pub(super) fn read_plan_table_entries_with_heap(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    heap: &HeapHandle,
    entries: Vec<(IndexKey, RowId)>,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    read_plan_table_entries_with_heap_and_bound(
        db, txn, table, index, heap, entries, None, filter, projection,
    )
}

pub(super) fn read_plan_table_entries_with_heap_and_bound(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    heap: &HeapHandle,
    entries: Vec<(IndexKey, RowId)>,
    page_max_trx_id: Option<u64>,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    let mut rows = Vec::with_capacity(entries.len());
    let mut eval_ctx = EvalQueryContext::default();
    let projector = IndexEntryProjector::new(table, index, projection);
    let mut compiled_filter = None;
    let visible_mask = match page_max_trx_id.and_then(|bound| {
        db.try_index_page_max_trx_shortcut_for(
            table.schema.id,
            &index.name,
            bound,
            txn,
            entries.len(),
        )
    }) {
        Some(mask) => mask,
        None => db.filter_visible_index_entries_mask_with_handle(
            heap,
            &table.schema,
            index,
            &entries,
            txn,
        )?,
    };
    for ((key, _row_id), visible) in entries.into_iter().zip(visible_mask.into_iter()) {
        if !visible {
            continue;
        }
        let row = projector.project(&key);
        if eval_covering_filter(db, txn, filter, &row, &mut eval_ctx, &mut compiled_filter)? {
            rows.push(row);
        }
    }
    Ok(rows)
}

/// Streaming covering-scan reader: walks the range via the borrowing
/// cursor visitor, so index keys are projected straight out of the
/// pinned leaf pages — no per-entry `IndexKey` clone and a single
/// visibility pass (the old shape filtered visibility once inside
/// `range_index_entries_visible_ordered` and a second time inside
/// `read_plan_table_entries`).
pub(super) fn read_plan_table_entries_streamed(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    mut low: Option<IndexBound>,
    mut high: Option<IndexBound>,
    reverse: bool,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    crate::planner::boolean_range::refine_runtime(
        &table.schema,
        index,
        filter.as_ref(),
        &mut low,
        &mut high,
    );
    if db.has_pending_index_repairs_for(table.schema.id, &index.name) {
        let entries =
            range_index_entries_visible_ordered(db, txn, table, index, low, high, reverse)?;
        return read_plan_table_entries(db, txn, table, index, entries, filter, projection);
    }
    let heap = index_scan_heap(db, table)?;
    let mut rows = Vec::new();
    let mut eval_ctx = EvalQueryContext::default();
    let projector = IndexEntryProjector::new(table, index, projection);
    let mut compiled_filter = None;
    let mut seen = (!index.unique).then(std::collections::HashSet::new);
    let mut consume = |entries: &[(&IndexKey, RowId)]| -> Result<(), QueryError> {
        let visible_mask = db.filter_visible_index_entries_refs_mask_with_handle(
            &heap,
            &table.schema,
            index,
            entries,
            txn,
        )?;
        for ((key, row_id), visible) in entries.iter().zip(visible_mask.into_iter()) {
            if !visible {
                continue;
            }
            if seen.as_mut().is_some_and(|seen| !seen.insert(*row_id)) {
                continue;
            }
            let row = projector.project(key);
            if eval_covering_filter(db, txn, filter, &row, &mut eval_ctx, &mut compiled_filter)? {
                rows.push(row);
            }
        }
        Ok(())
    };
    if reverse {
        let mut cursor =
            db.range_index_snapshot_reverse_cursor(&table.schema, index, low, high, txn)?;
        cursor.visit_entries(&mut consume)?;
    } else {
        let mut cursor = db.range_index_snapshot_cursor(&table.schema, index, low, high, txn)?;
        cursor.visit_entries(&mut consume)?;
    }
    Ok(rows)
}

pub(super) fn read_plan_table_ids_with_heap(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    heap: &HeapHandle,
    ids: Vec<RowId>,
    filter: &Option<Expr>,
    max_rows: Option<usize>,
    projection: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    let mut rows = Vec::with_capacity(max_rows.map_or(ids.len(), |max| max.min(ids.len())));
    let mut eval_ctx = EvalQueryContext::default();
    // Non-covering index scans used to route every residual row through the
    // DB-aware AST interpreter even when the predicate was a fully compilable
    // prepared expression (`column = ?`, conjunctions, ranges, ...).
    // Compile lazily against the first materialized row, exactly as covering
    // scans do. Unsupported/subquery shapes retain the DB-aware legacy path
    // through `CompiledPlanPredicate::eval_bool_with_db`.
    let mut compiled_filter = None;
    if let Some(cols) = projection {
        for (_row_id, values) in
            db.read_row_columns_many_with_handle(heap, &table.schema, &ids, txn, cols)?
        {
            if max_rows.is_some_and(|max| rows.len() >= max) {
                break;
            }
            let row = projected_values_to_plan_row(table, cols, values)?;
            if eval_covering_filter(db, txn, filter, &row, &mut eval_ctx, &mut compiled_filter)? {
                rows.push(row);
            }
        }
    } else {
        for row in db.read_rows_many_with_handle(heap, &table.schema, &ids, txn)? {
            if max_rows.is_some_and(|max| rows.len() >= max) {
                break;
            }
            let row = stored_row_to_plan_row(table, row.values)?;
            if eval_covering_filter(db, txn, filter, &row, &mut eval_ctx, &mut compiled_filter)? {
                rows.push(row);
            }
        }
    }
    Ok(rows)
}

pub(super) fn read_plan_table_entries_limited_with_heap(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    heap: &HeapHandle,
    entries: Vec<(IndexKey, RowId)>,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
    max_rows: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    let mut rows = Vec::with_capacity(max_rows.min(entries.len()));
    let mut eval_ctx = EvalQueryContext::default();
    let projector = IndexEntryProjector::new(table, index, projection);
    let mut compiled_filter = None;
    let visible_mask = db.filter_visible_index_entries_mask_with_handle(
        heap,
        &table.schema,
        index,
        &entries,
        txn,
    )?;
    for ((key, _row_id), visible) in entries.into_iter().zip(visible_mask.into_iter()) {
        if rows.len() >= max_rows {
            break;
        }
        if !visible {
            continue;
        }
        let row = projector.project(&key);
        if eval_covering_filter(db, txn, filter, &row, &mut eval_ctx, &mut compiled_filter)? {
            rows.push(row);
        }
    }
    Ok(rows)
}

pub(super) fn read_plan_table_entries_chunked(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    const CHUNK: usize = 4096;
    let mut rows = Vec::new();
    let heap = index_scan_heap(db, table)?;
    let mut cursor =
        db.range_index_snapshot_cursor(&table.schema, index, low.clone(), high.clone(), txn)?;
    loop {
        let entries = cursor.next_chunk(CHUNK)?;
        if entries.is_empty() {
            break;
        }
        rows.extend(read_plan_table_entries_with_heap(
            db, txn, table, index, &heap, entries, filter, projection,
        )?);
    }
    Ok(rows)
}

/// Direction-agnostic chunk source for LIMIT-bounded covering scans.
pub(super) enum AnyRangeCursor {
    Forward(crate::storage::IndexRangeCursor),
    Reverse(crate::storage::IndexRangeReverseCursor),
}

impl AnyRangeCursor {
    pub(super) fn next_chunk(
        &mut self,
        chunk_size: usize,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        match self {
            AnyRangeCursor::Forward(cursor) => cursor.next_chunk(chunk_size),
            AnyRangeCursor::Reverse(cursor) => cursor.next_chunk(chunk_size),
        }
    }

    pub(super) fn next_leaf_chunk_row_ids(
        &mut self,
        soft_cap: usize,
    ) -> Result<(Vec<RowId>, u64), QueryError> {
        match self {
            AnyRangeCursor::Forward(cursor) => cursor.next_leaf_chunk_row_ids(soft_cap),
            AnyRangeCursor::Reverse(cursor) => cursor.next_leaf_chunk_row_ids(soft_cap),
        }
    }
}

pub(super) fn read_plan_table_entries_chunked_limited(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    reverse: bool,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
    max_rows: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    const MAX_CHUNK: usize = 4096;
    const MIN_CHUNK: usize = 64;
    let mut rows = Vec::with_capacity(max_rows.min(64));
    let heap = index_scan_heap(db, table)?;
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
    while rows.len() < max_rows {
        let remaining = max_rows - rows.len();
        let entries = cursor.next_chunk(remaining.max(MIN_CHUNK).min(MAX_CHUNK))?;
        if entries.is_empty() {
            break;
        }
        rows.extend(read_plan_table_entries_limited_with_heap(
            db,
            txn,
            table,
            index,
            &heap,
            entries,
            filter,
            projection,
            max_rows - rows.len(),
        )?);
    }
    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn read_plan_table_entries_windowed(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    reverse: bool,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
    offset: usize,
    limit: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    const CHUNK: usize = 4096;
    let heap = index_scan_heap(db, table)?;
    let projector = IndexEntryProjector::new(table, index, projection);
    let mut eval_ctx = EvalQueryContext::default();
    let mut compiled_filter = None;
    let mut skip = offset;
    let mut rows = Vec::with_capacity(limit.min(64));
    let mut seen = (!index.unique).then(std::collections::HashSet::new);
    let mut cursor = if reverse {
        AnyRangeCursor::Reverse(db.range_index_snapshot_reverse_cursor(
            &table.schema,
            index,
            low,
            high,
            txn,
        )?)
    } else {
        AnyRangeCursor::Forward(db.range_index_snapshot_cursor(
            &table.schema,
            index,
            low,
            high,
            txn,
        )?)
    };
    while rows.len() < limit {
        let mut entries = cursor.next_chunk(CHUNK)?;
        if entries.is_empty() {
            break;
        }
        if let Some(seen) = seen.as_mut() {
            entries.retain(|(_, row_id)| seen.insert(*row_id));
        }
        if entries.is_empty() {
            continue;
        }
        let visible_mask = db.filter_visible_index_entries_mask_with_handle(
            &heap,
            &table.schema,
            index,
            &entries,
            txn,
        )?;
        for ((key, _row_id), visible) in entries.into_iter().zip(visible_mask.into_iter()) {
            if !visible {
                continue;
            }
            if filter.is_none() {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                rows.push(projector.project(&key));
            } else {
                let row = projector.project(&key);
                if !eval_covering_filter(
                    db,
                    txn,
                    filter,
                    &row,
                    &mut eval_ctx,
                    &mut compiled_filter,
                )? {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                rows.push(row);
            }
            if rows.len() >= limit {
                break;
            }
        }
    }
    Ok(rows)
}

/// Evaluate a scan residual through the compiled expression engine.
///
/// The materialized row shape is stable for the lifetime of one scan, so
/// compiling against the first visible row is enough for every subsequent
/// entry. Unsupported expressions retain the exact legacy/subquery behavior
/// through `CompiledPlanPredicate`'s fallback. The historical function name
/// remains because covering-index scans were its first caller.
pub(in crate::engine::executor::select) fn eval_covering_filter(
    db: &Arc<Database>,
    txn: TxnView,
    filter: &Option<Expr>,
    row: &PlanRow,
    eval_ctx: &mut EvalQueryContext,
    compiled: &mut Option<CompiledPlanPredicate>,
) -> Result<bool, QueryError> {
    let Some(expr) = filter.as_ref() else {
        return Ok(true);
    };
    let predicate = compiled.get_or_insert_with(|| CompiledPlanPredicate::compile_once(expr, row));
    predicate.eval_bool_with_db(db, txn, row, eval_ctx)
}

pub(in crate::engine::executor::select) fn read_plan_table_entries_chunked_for_parallel_fallback(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    filter: Option<&Expr>,
    projection: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    read_plan_table_entries_chunked(
        db,
        txn,
        table,
        index,
        low,
        high,
        &filter.cloned(),
        projection,
    )
}

/// Execute an `IndexAggregate` operator. Single-table aggregate
/// pushed down onto an index scan: yields one row whose `agg_values`
/// cache holds the computed value keyed by `agg_key` (matching the
/// `aggregate_key(expr)` the projection layer will look up).
///
/// COUNT(*) walks the entire visible-entry stream; MIN/MAX walk the
/// index in forward/reverse order and return the first non-NULL,
/// visible entry — early-terminating, so they're typically O(1) reads.
pub(super) fn execute_index_aggregate(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    filter: Option<&Expr>,
    op: &IndexAggOp,
    agg_key: u64,
    output_alias: &str,
    output_type: ColumnType,
) -> Result<Vec<PlanRow>, QueryError> {
    if table.schema.is_partitioned() {
        // IndexAggregate bypasses the heap scan adapter, so account for each
        // partition-local index opened by this operator here. The active outer
        // pruning scope narrows this list for partition predicates.
        for _ in table.in_scope_storage_ids() {
            crate::engine::executor::partition_router::record_partition_visit();
        }
    }
    let value = match op {
        IndexAggOp::CountStar => {
            // Single-pass leaf walk + visibility check, no
            // intermediate Vec of 6M entries and no per-chunk tree
            // re-descent.
            let count =
                db.count_index_entries_visible(&table.schema, index, low, high, txn, None)?;
            Value::Int(count)
        }
        IndexAggOp::CountColumn(col) => {
            // Locate `col` inside the chosen covering index's key
            // layout once. The storage helper inspects this slot in
            // each leaf entry to skip NULLs in the streaming walk.
            let pos = index
                .effective_columns()
                .iter()
                .position(|c| c.eq_ignore_ascii_case(col))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        format!("COUNT({col}) pushdown column not in index {}", index.name),
                    )
                })?;
            let count =
                db.count_index_entries_visible(&table.schema, index, low, high, txn, Some(pos))?;
            Value::Int(count)
        }
        IndexAggOp::CountDistinctColumn(col) => {
            debug_assert!(
                !index.has_prefix_columns()
                    && index
                        .columns
                        .first()
                        .is_some_and(|first| first.eq_ignore_ascii_case(col)),
                "planner must prove ordered full-column DISTINCT input"
            );
            Value::Int(db.count_distinct_index_prefix_visible(
                &table.schema,
                index,
                low,
                high,
                txn,
            )?)
        }
        IndexAggOp::Min(col) => {
            if let Some(filter) = filter {
                filtered_index_min_max_value(db, txn, table, index, low, high, filter, col, false)?
            } else {
                index_min_max_value(db, txn, table, index, col, false)?
            }
        }
        IndexAggOp::Max(col) => {
            if let Some(filter) = filter {
                filtered_index_min_max_value(db, txn, table, index, low, high, filter, col, true)?
            } else {
                index_min_max_value(db, txn, table, index, col, true)?
            }
        }
    };

    let mut cache: HashMap<u64, Value> = HashMap::with_capacity(1);
    cache.insert(agg_key, value.clone());

    let row = PlanRow {
        fields: vec![PlanField {
            meta: intern_col_meta(index_aggregate_output_meta(
                table,
                op,
                output_alias,
                output_type,
            )),
            value,
        }],
        group: Some(Vec::new()),
        agg_values: Some(std::sync::Arc::new(cache)),
        schema: PlanRow::empty_schema(),
    };
    Ok(vec![row])
}

#[allow(clippy::too_many_arguments)]
pub(super) fn filtered_index_min_max_value(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    filter: &Expr,
    col: &str,
    reverse: bool,
) -> Result<Value, QueryError> {
    let col_idx = table
        .schema
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(col))
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("MIN/MAX pushdown column {col} missing on table"),
            )
        })?;
    // MIN/MAX ignore NULL. Fold that rule into the residual only when the
    // aggregate column is actually nullable, avoiding a redundant predicate
    // for the common NOT NULL primary-key case.
    let filter = if table.schema.columns[col_idx].nullable {
        Expr::Binary {
            left: Box::new(filter.clone()),
            op: crate::parser::BinaryOp::And,
            right: Box::new(Expr::IsNotNull(Box::new(Expr::Identifier(col.to_string())))),
        }
    } else {
        filter.clone()
    };
    // Do not split this residual into ICP + heap predicates. A limited ICP
    // deliberately batches a large MRR window before evaluating heap-only
    // conjuncts, while an aggregate edge walk must evaluate each small leading
    // window so it can stop immediately.
    let rows = read_plan_table_ids_range_chunked_limited(
        db,
        txn,
        table,
        index,
        low,
        high,
        reverse,
        &Some(filter),
        1,
        None,
        false,
    )?;
    Ok(rows
        .first()
        .and_then(|row| row.fields.get(col_idx))
        .map(|field| field.value.clone())
        .unwrap_or(Value::Null))
}

#[derive(Clone)]
pub(super) enum CoveringAggInput {
    CountStar,
    Column {
        key_pos: usize,
        data_type: DataTypeKind,
    },
    Null,
}

pub(in crate::engine::executor::select) struct CoveringIndexAggregatePartial {
    pub(in crate::engine::executor::select) groups:
        hashbrown::HashMap<Vec<ValueKey>, CoveringIndexAggregateGroup, ahash::RandomState>,
    pub(in crate::engine::executor::select) native_rows: u64,
}

pub(in crate::engine::executor::select) struct CoveringIndexAggregateGroup {
    pub(super) key_values: Vec<Value>,
    pub(super) accumulators: Vec<Box<dyn crate::engine::executor::select::aggregate::Accumulator>>,
}

pub(in crate::engine::executor::select) struct CoveringIndexAggregateSpec {
    pub(super) slots: Vec<crate::engine::executor::select::aggregate::AggregateSlot>,
    pub(super) slot_inputs: Vec<CoveringAggInput>,
    pub(super) slot_fixed_scales: Vec<Option<u8>>,
    pub(super) group_inputs: Vec<CoveringAggInput>,
    pub(super) group_table_indices: Vec<usize>,
}

pub(in crate::engine::executor::select) fn execute_covering_index_aggregate(
    db: &Arc<Database>,
    txn: TxnView,
    runtime_plan: &PhysicalPlan,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
) -> Result<Vec<PlanRow>, QueryError> {
    let spec = build_covering_index_aggregate_spec(table, index, group_by, having, aggregates)?;
    if db.has_pending_index_repairs_for(table.schema.id, &index.name) {
        let entries =
            db.range_index_bounds_entries_visible(&table.schema, index, low, high, txn)?;
        let _exact = crate::storage::ExactIndexVisibility::enter();
        let rows = read_plan_table_entries(db, txn, table, index, entries, &None, None)?;
        return aggregate_plan_rows(rows, group_by, having, &spec.slots);
    }
    let leaf_ids = db.range_index_leaf_ids(&table.schema, index, low.clone(), high.clone())?;
    let partial = scan_covering_index_aggregate_leaf_ids(
        db, txn, table, index, low, high, &leaf_ids, None, &spec,
    )?;
    let native_rows = partial.native_rows;
    let rows = finalize_covering_index_aggregate_partials(table, having, &spec, vec![partial])?;
    crate::engine::executor::select::analyze_stats::add_aggregate_runtime(
        runtime_plan,
        "ordered_index",
        native_rows,
        0,
        0,
    );
    Ok(rows)
}

pub(in crate::engine::executor::select) fn build_covering_index_aggregate_spec(
    table: &PlanTable,
    index: &IndexSchema,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
) -> Result<CoveringIndexAggregateSpec, QueryError> {
    let mut slot_exprs: Vec<&Expr> = aggregates.iter().collect();
    if let Some(h) = having {
        slot_exprs.push(h);
    }
    let mut slots =
        crate::engine::executor::select::aggregate::collect_aggregate_slots(&slot_exprs);
    for slot in &mut slots {
        if matches!(
            slot.kind(),
            Some(super::aggregate::AggregateKind::Min | super::aggregate::AggregateKind::Max)
        ) {
            slot.comparison_collation = slot
                .args
                .first()
                .and_then(|expr| crate::engine::expr::expr_collation(expr, &table.schema))
                .map(|info| info.collation_id);
        }
    }
    if slots.iter().any(|s| s.has_order_by()) {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "GROUP_CONCAT/JSON_ARRAYAGG with ORDER BY is not supported by covering index aggregate",
        ));
    }
    let key_map: HashMap<String, usize> = index
        .effective_columns()
        .iter()
        .enumerate()
        .map(|(idx, col)| (col.to_ascii_lowercase(), idx))
        .collect();
    let input_for_expr = |expr: &Expr| -> Result<CoveringAggInput, QueryError> {
        let source_expr = match expr {
            Expr::Function {
                name,
                args,
                distinct: false,
            } if name == "__DISTINCT_COLLATION_KEY" && args.len() == 1 => &args[0],
            _ => expr,
        };
        let Some(col_idx) = simple_column_index_of(source_expr, &table.schema) else {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "covering index aggregate only supports simple column arguments",
            ));
        };
        let col = &table.schema.columns[col_idx];
        let Some(key_pos) = key_map.get(&col.name.to_ascii_lowercase()).copied() else {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("column {} is not covered by index {}", col.name, index.name),
            ));
        };
        Ok(CoveringAggInput::Column {
            key_pos,
            data_type: col.data_type.clone(),
        })
    };
    let mut group_inputs = Vec::with_capacity(group_by.len());
    let mut group_table_indices = Vec::with_capacity(group_by.len());
    for expr in group_by {
        let Some(col_idx) = simple_column_index_of(expr, &table.schema) else {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "covering index aggregate only supports simple GROUP BY columns",
            ));
        };
        group_inputs.push(input_for_expr(expr)?);
        group_table_indices.push(col_idx);
    }
    let mut slot_inputs = Vec::with_capacity(slots.len());
    let mut slot_fixed_scales = Vec::with_capacity(slots.len());
    for slot in &slots {
        if slot.is_count_star {
            slot_inputs.push(CoveringAggInput::CountStar);
            slot_fixed_scales.push(None);
        } else if slot.args.is_empty() {
            slot_inputs.push(CoveringAggInput::Null);
            slot_fixed_scales.push(None);
        } else {
            let input = input_for_expr(&slot.args[0])?;
            let scale = if slot.name.eq_ignore_ascii_case("SUM") {
                let column_index = simple_column_index_of(&slot.args[0], &table.schema);
                column_index.and_then(|index| match table.schema.columns[index].data_type {
                    DataTypeKind::Float {
                        scale: Some(scale), ..
                    }
                    | DataTypeKind::Double {
                        scale: Some(scale), ..
                    } => Some(scale),
                    _ => None,
                })
            } else {
                None
            };
            slot_inputs.push(input);
            slot_fixed_scales.push(scale);
        }
    }
    Ok(CoveringIndexAggregateSpec {
        slots,
        slot_inputs,
        slot_fixed_scales,
        group_inputs,
        group_table_indices,
    })
}

pub(in crate::engine::executor::select) fn scan_covering_index_aggregate_leaf_ids(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    leaf_ids: &[u64],
    seed_leaf_id: Option<u64>,
    spec: &CoveringIndexAggregateSpec,
) -> Result<CoveringIndexAggregatePartial, QueryError> {
    let heap = db.cached_heap_handle(table.schema.id)?;
    let streaming = covering_group_is_index_prefix(index, spec);
    let mut repairs =
        db.snapshot_leaf_repairs(&table.schema, index, low.clone(), high.clone(), txn);
    let epoch = db.index_repair_rollback_epoch(table.schema.id, &index.name);
    // A writer retiring keys mid-stream restarts the slice in buffered mode.
    let state = loop {
        let mut state = CoveringStreamingState::new(spec, streaming);
        let complete = db.visit_index_leaf_entry_chunks_for_snapshot(
            &mut repairs,
            &table.schema,
            index,
            leaf_ids,
            seed_leaf_id,
            low.clone(),
            high.clone(),
            |entries, page_max_trx_id| {
                // InnoDB-style PAGE_MAX_TRX_ID shortcut: skip the whole
                // per-entry visibility filter when the leaf's writer
                // upper bound proves visibility to `txn` and no repairs pend.
                // Covering aggregates then consume index values directly
                // without probing heap MVCC headers.
                let _exact = (db.index_repair_rollback_epoch(table.schema.id, &index.name)
                    != epoch)
                    .then(crate::storage::ExactIndexVisibility::enter);
                let visible = db
                    .try_index_page_max_trx_shortcut_for(
                        table.schema.id,
                        &index.name,
                        page_max_trx_id,
                        txn,
                        entries.len(),
                    )
                    .map(Ok::<_, QueryError>)
                    .unwrap_or_else(|| {
                        db.filter_visible_index_entries_refs_mask_with_handle(
                            &heap,
                            &table.schema,
                            index,
                            entries,
                            txn,
                        )
                    })?;
                state.consume(spec, entries, &visible)
            },
        )?;
        if complete {
            break state;
        }
    };
    Ok(state.finish(spec))
}

/// True when every GROUP BY column maps to a distinct index key
/// position 0..k (in order) — i.e. the group keys form a prefix of
/// the index sort key. In that case the index leaf scan delivers
/// rows already grouped: same group_key entries arrive contiguously
/// and group_keys are monotonic. We can keep one active accumulator
/// at a time and flush on group-key change (O(1) memory + 0 hash
/// operations on the hot path).
///
/// Conservative: only returns `true` when the group columns line up
/// 1:1 with `key_pos = 0,1,..k-1`. A reordered or non-prefix group
/// list keeps the hash path.
pub(super) fn covering_group_is_index_prefix(
    _index: &IndexSchema,
    spec: &CoveringIndexAggregateSpec,
) -> bool {
    if spec.group_inputs.is_empty() {
        return false;
    }
    for (i, input) in spec.group_inputs.iter().enumerate() {
        let key_pos = match input {
            CoveringAggInput::Column { key_pos, .. } => *key_pos,
            // CountStar / Null are not GROUP BY columns — bail.
            _ => return false,
        };
        if key_pos != i {
            return false;
        }
    }
    true
}

pub(super) struct CoveringStreamingState {
    pub(super) streaming: bool,
    /// Set when the aggregate is `COUNT(*)` (one or more) with no
    /// GROUP BY. Per-entry hashbrown probe + per-slot dispatch then
    /// collapses to a single popcount per chunk; group key extraction
    /// is skipped entirely (no `Vec<Value>` / `Vec<ValueKey>` per row).
    pub(super) count_star_only: bool,
    /// Hash path: keyed by group_key.
    pub(super) groups:
        hashbrown::HashMap<Vec<ValueKey>, CoveringIndexAggregateGroup, ahash::RandomState>,
    /// Streaming path: only the currently-open group. When the next
    /// entry produces a different group_key, we flush this into
    /// `groups` (one insert per distinct group, never an upsert).
    pub(super) active: Option<(Vec<ValueKey>, CoveringIndexAggregateGroup)>,
    /// Single-group accumulator state for the `count_star_only` fast
    /// path. Lazily initialised at the first non-empty `consume()`;
    /// finalised by `finish()` into the synthetic empty group.
    pub(super) count_star_state: Option<CoveringIndexAggregateGroup>,
    /// All other ungrouped aggregates also have exactly one logical group.
    /// Keep it directly instead of allocating two empty key vectors and
    /// probing the same hash bucket for every visible index entry.
    pub(super) scalar_state: Option<CoveringIndexAggregateGroup>,
    pub(super) native_rows: u64,
}

impl CoveringStreamingState {
    pub(super) fn new(spec: &CoveringIndexAggregateSpec, streaming: bool) -> Self {
        let count_star_only = spec.group_inputs.is_empty()
            && !spec.slot_inputs.is_empty()
            && spec
                .slot_inputs
                .iter()
                .all(|input| matches!(input, CoveringAggInput::CountStar));
        Self {
            streaming,
            count_star_only,
            groups: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
            active: None,
            count_star_state: None,
            scalar_state: None,
            native_rows: 0,
        }
    }

    pub(super) fn consume(
        &mut self,
        spec: &CoveringIndexAggregateSpec,
        entries: &[(&IndexKey, RowId)],
        visible: &[bool],
    ) -> Result<(), QueryError> {
        let visible_count = visible.iter().filter(|visible| **visible).count();
        self.native_rows = self.native_rows.saturating_add(visible_count as u64);
        // Fast path: `COUNT(*)` with no GROUP BY. The dominant TPC-H
        // shape `SELECT count(*) FROM lineitem` lands here. Without
        // this branch the per-entry loop below performed (a) two `Vec`
        // allocations for an empty group key, (b) a `hashbrown::entry()`
        // probe per row, and (c) a per-slot match dispatch — all to
        // produce a single integer increment. Here we instead popcount
        // the visible mask (the compiler auto-vectorises `iter().filter`
        // into a SIMD masked count on both AVX2 and NEON) and update
        // every accumulator by that count in one call.
        if self.count_star_only {
            self.consume_count_star_batch(spec, visible_count);
            // Silence the `entries` warning when this branch fires.
            let _ = entries;
            return Ok(());
        }
        if spec.group_inputs.is_empty() {
            let state = self
                .scalar_state
                .get_or_insert_with(|| CoveringIndexAggregateGroup {
                    key_values: Vec::new(),
                    accumulators: spec.slots.iter().map(|s| s.make_accumulator()).collect(),
                });
            for ((key, _row_id), is_visible) in entries.iter().zip(visible.iter()) {
                if *is_visible {
                    update_covering_state(spec, key, state)?;
                }
            }
            return Ok(());
        }
        for ((key, _row_id), is_visible) in entries.iter().zip(visible.iter()) {
            if !*is_visible {
                continue;
            }
            let key: &IndexKey = *key;
            // Hot loop: build the per-row group_key once and reuse it
            // for both the streaming compare path and the hash insert
            // path. The Vec allocation here is the same as the legacy
            // code; the win is avoiding the *second* allocation (the
            // hash-map probe key) that the old path did per row.
            let key_values: Vec<Value> = spec
                .group_inputs
                .iter()
                .map(|input| covering_input_value(input, key))
                .collect();
            let group_key: Vec<ValueKey> = key_values.iter().map(ValueKey::from).collect();

            if self.streaming {
                // Streaming path: same key as the active group? Just
                // update its accumulators in place — no hash, no probe
                // key clone, no Vec alloc beyond what we already paid
                // for the column extract.
                let same_as_active = self
                    .active
                    .as_ref()
                    .is_some_and(|(active_key, _)| active_key == &group_key);
                if same_as_active {
                    let (_, state) = self.active.as_mut().unwrap();
                    update_covering_state(spec, key, state)?;
                    continue;
                }
                // Group changed. Flush the prior active into `groups`
                // (one .insert() per distinct group) before opening the
                // new active. The leaf-scan order guarantees this group
                // can never reappear later in the same scan, so the
                // insert is always vacant — but we still let the
                // HashMap handle a duplicate gracefully (a partial
                // boundary across parallel workers can land the same
                // key on more than one shard, and finalize_* will
                // merge).
                if let Some((prev_key, prev_state)) = self.active.take() {
                    self.groups.insert(prev_key, prev_state);
                }
                let mut new_state = CoveringIndexAggregateGroup {
                    key_values,
                    accumulators: spec.slots.iter().map(|s| s.make_accumulator()).collect(),
                };
                update_covering_state(spec, key, &mut new_state)?;
                self.active = Some((group_key, new_state));
            } else {
                use hashbrown::hash_map::Entry;
                let state = match self.groups.entry(group_key) {
                    Entry::Occupied(e) => e.into_mut(),
                    Entry::Vacant(e) => e.insert(CoveringIndexAggregateGroup {
                        key_values,
                        accumulators: spec.slots.iter().map(|s| s.make_accumulator()).collect(),
                    }),
                };
                update_covering_state(spec, key, state)?;
            }
        }
        Ok(())
    }

    pub(super) fn consume_count_star_batch(
        &mut self,
        spec: &CoveringIndexAggregateSpec,
        count: usize,
    ) {
        if count == 0 {
            return;
        }
        let state = self
            .count_star_state
            .get_or_insert_with(|| CoveringIndexAggregateGroup {
                key_values: Vec::new(),
                accumulators: spec.slots.iter().map(|s| s.make_accumulator()).collect(),
            });
        for acc in state.accumulators.iter_mut() {
            acc.update_count_star_batch(count);
        }
    }

    pub(super) fn finish(
        mut self,
        spec: &CoveringIndexAggregateSpec,
    ) -> CoveringIndexAggregatePartial {
        crate::engine::perf_events::record_rows_examined(self.native_rows);
        if let Some((key, state)) = self.active.take() {
            self.groups.insert(key, state);
        }
        // Promote the count(*) fast-path accumulator into the synthetic
        // empty group so finalize / merge see one canonical shape
        // regardless of which consume path produced the partial.
        if let Some(state) = self.count_star_state.take() {
            self.groups.insert(Vec::new(), state);
        }
        if let Some(state) = self.scalar_state.take() {
            self.groups.insert(Vec::new(), state);
        }
        if spec.group_inputs.is_empty() && self.groups.is_empty() {
            self.groups.insert(
                Vec::new(),
                CoveringIndexAggregateGroup {
                    key_values: Vec::new(),
                    accumulators: spec.slots.iter().map(|s| s.make_accumulator()).collect(),
                },
            );
        }
        CoveringIndexAggregatePartial {
            groups: self.groups,
            native_rows: self.native_rows,
        }
    }
}

pub(super) fn update_covering_state(
    spec: &CoveringIndexAggregateSpec,
    key: &IndexKey,
    state: &mut CoveringIndexAggregateGroup,
) -> Result<(), QueryError> {
    for (input, acc) in spec.slot_inputs.iter().zip(state.accumulators.iter_mut()) {
        match input {
            CoveringAggInput::CountStar => acc.update_count_star(),
            CoveringAggInput::Null => acc.update(Value::Null)?,
            CoveringAggInput::Column { key_pos, .. } => {
                match key.0.get(*key_pos) {
                    Some(KeyPart::Null) | None => acc.update_ref(&Value::Null)?,
                    Some(KeyPart::Int(value)) => acc.update_i64(*value)?,
                    Some(KeyPart::UInt(value)) => acc.update_u64(*value)?,
                    Some(KeyPart::Float32(value)) => {
                        acc.update_f64(f64::from(crate::storage::f32_from_order_u32(*value)))?
                    }
                    Some(KeyPart::Float64(value)) => {
                        acc.update_f64(crate::storage::f64_from_order_u64(*value))?
                    }
                    Some(KeyPart::Decimal { value, scale }) => {
                        acc.update_decimal_parts(*value, *scale)?
                    }
                    // Variable-width, temporal and wide-decimal index parts
                    // retain the established Value oracle.
                    Some(_) => {
                        let value = covering_input_value(input, key);
                        acc.update_ref(&value)?;
                    }
                }
            }
        }
    }
    Ok(())
}

pub(in crate::engine::executor::select) fn scan_covering_index_aggregate_page_range(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    page_lo: u64,
    page_hi: u64,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    spec: &CoveringIndexAggregateSpec,
) -> Result<CoveringIndexAggregatePartial, QueryError> {
    let heap = db.cached_heap_handle(table.schema.id)?;
    let streaming = covering_group_is_index_prefix(index, spec);
    let mut state = CoveringStreamingState::new(spec, streaming);
    let mut scan_page_lo = page_lo;
    if state.count_star_only && low.is_none() && high.is_none() {
        let (visible_prefix_count, resume_page) = db.count_visible_index_leaf_prefix_page_range(
            &table.schema,
            index,
            page_lo,
            page_hi,
            txn,
        )?;
        state.native_rows = state.native_rows.saturating_add(visible_prefix_count);
        state.consume_count_star_batch(spec, visible_prefix_count as usize);
        if resume_page >= page_hi {
            return Ok(state.finish(spec));
        }
        scan_page_lo = resume_page;
    }
    if low.is_none()
        && high.is_none()
        && spec.group_inputs.is_empty()
        && !matches!(
            crate::engine::executor::vec::config::effective_mode(),
            crate::engine::executor::vec::config::VecMode::Off
        )
    {
        let input = spec.slot_inputs.iter().find_map(|input| match input {
            CoveringAggInput::Column { key_pos, data_type } => Some((*key_pos, data_type)),
            _ => None,
        });
        if let Some((position, data_type)) = input.filter(|(position, data_type)| {
            matches!(
                data_type,
                DataTypeKind::TinyInt
                    | DataTypeKind::SmallInt
                    | DataTypeKind::MediumInt
                    | DataTypeKind::Int
                    | DataTypeKind::BigInt
                    | DataTypeKind::Date
                    | DataTypeKind::Time { .. }
                    | DataTypeKind::DateTime { .. }
                    | DataTypeKind::Timestamp { .. }
                    | DataTypeKind::Year
            ) && spec.slot_inputs.iter().all(|input| match input {
                CoveringAggInput::Column { key_pos, .. } => key_pos == position,
                CoveringAggInput::CountStar => true,
                CoveringAggInput::Null => false,
            })
        }) {
            scan_page_lo = db.visit_visible_index_numeric_prefix_page_range(
                &table.schema,
                index,
                scan_page_lo,
                page_hi,
                txn,
                position,
                data_type,
                |values| {
                    let scalar =
                        state
                            .scalar_state
                            .get_or_insert_with(|| CoveringIndexAggregateGroup {
                                key_values: Vec::new(),
                                accumulators: spec
                                    .slots
                                    .iter()
                                    .map(|s| s.make_accumulator())
                                    .collect(),
                            });
                    for (input, accumulator) in
                        spec.slot_inputs.iter().zip(&mut scalar.accumulators)
                    {
                        if matches!(input, CoveringAggInput::CountStar) {
                            accumulator.update_count_star_batch(values.len());
                        } else {
                            accumulator.update_batch(values)?;
                        }
                    }
                    state.native_rows = state.native_rows.saturating_add(values.len() as u64);
                    Ok(())
                },
            )?;
            if scan_page_lo >= page_hi {
                return Ok(state.finish(spec));
            }
        }
    }
    db.visit_index_leaf_page_range_entry_chunks_with_bound(
        &table.schema,
        index,
        scan_page_lo,
        page_hi,
        low,
        high,
        |entries, page_max_trx_id| {
            let visible = db
                .try_index_page_max_trx_shortcut_for(
                    table.schema.id,
                    &index.name,
                    page_max_trx_id,
                    txn,
                    entries.len(),
                )
                .map(Ok::<_, QueryError>)
                .unwrap_or_else(|| {
                    db.filter_visible_index_entries_refs_mask_with_handle(
                        &heap,
                        &table.schema,
                        index,
                        entries,
                        txn,
                    )
                })?;
            state.consume(spec, entries, &visible)
        },
    )?;
    Ok(state.finish(spec))
}

pub(super) fn covering_input_value(input: &CoveringAggInput, key: &IndexKey) -> Value {
    match input {
        CoveringAggInput::CountStar | CoveringAggInput::Null => Value::Null,
        CoveringAggInput::Column { key_pos, data_type } => key
            .0
            .get(*key_pos)
            .map(|part| part.to_value(data_type))
            .unwrap_or(Value::Null),
    }
}

pub(in crate::engine::executor::select) fn finalize_covering_index_aggregate_partials(
    table: &PlanTable,
    having: Option<&Expr>,
    spec: &CoveringIndexAggregateSpec,
    partials: Vec<CoveringIndexAggregatePartial>,
) -> Result<Vec<PlanRow>, QueryError> {
    use hashbrown::hash_map::Entry;
    let mut merged: hashbrown::HashMap<
        Vec<ValueKey>,
        CoveringIndexAggregateGroup,
        ahash::RandomState,
    > = hashbrown::HashMap::with_hasher(ahash::RandomState::new());
    let parallel_count = if spec.group_inputs.is_empty()
        && spec.slots.len() == 1
        && !matches!(
            crate::engine::executor::vec::config::effective_mode(),
            crate::engine::executor::vec::config::VecMode::Off
        ) {
        if let Some(budget) = crate::engine::executor::mem_budget::current_budget() {
            let mut sources = smallvec::SmallVec::<
                [&hashbrown::HashSet<ValueKey, ahash::RandomState>; 16],
            >::new();
            let mut eligible = partials.len() <= 16;
            if eligible {
                for partial in &partials {
                    if partial.groups.len() > 1 {
                        eligible = false;
                        break;
                    }
                    if let Some((key, group)) = partial.groups.iter().next() {
                        if !key.is_empty() {
                            eligible = false;
                            break;
                        }
                        let Some(keys) = group
                            .accumulators
                            .first()
                            .and_then(|acc| acc.count_distinct_keys())
                        else {
                            eligible = false;
                            break;
                        };
                        sources.push(keys);
                    }
                }
            }
            if eligible {
                crate::engine::executor::hash_aggregate::parallel_distinct::count_union(
                    &sources, &budget,
                )?
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };
    if let Some(count) = parallel_count {
        merged.insert(
            Vec::new(),
            CoveringIndexAggregateGroup {
                key_values: Vec::new(),
                accumulators: vec![
                    crate::engine::executor::select::aggregate::finalized_distinct_count(count),
                ],
            },
        );
        drop(partials);
    } else {
        for partial in partials {
            for (key, group) in partial.groups {
                match merged.entry(key) {
                    Entry::Vacant(e) => {
                        e.insert(group);
                    }
                    Entry::Occupied(mut e) => {
                        let dst = e.get_mut();
                        for (dst_acc, src_acc) in dst
                            .accumulators
                            .iter_mut()
                            .zip(group.accumulators.into_iter())
                        {
                            dst_acc.merge_from(src_acc.into_any());
                        }
                    }
                }
            }
        }
    }
    if spec.group_inputs.is_empty() && merged.is_empty() {
        merged.insert(
            Vec::new(),
            CoveringIndexAggregateGroup {
                key_values: Vec::new(),
                accumulators: spec.slots.iter().map(|s| s.make_accumulator()).collect(),
            },
        );
    }

    // Hot-loop hoist: previously the row-builder loop below called
    // `intern_col_meta` once per (group × column), each call running
    // SipHash over the full `ColMeta` (table+alias+column+type+source)
    // — for TPC-H Q18 sf=5 that's 7.5M groups × 16 lineitem columns
    // = 120M hashes, which profile shows as 23% of the *main* thread's
    // CPU. The `&'static ColMeta` is invariant across all groups for a
    // given (table, column) pair, so build the vec once and reuse.
    // Same for `group_table_indices` lookup: O(group_by.len()) per
    // column per group → precompute `column_idx → Some(group_pos)`.
    let col_metas: Vec<ColMetaRef> = table
        .schema
        .columns
        .iter()
        .enumerate()
        .map(|(idx, _)| intern_col_meta(ColMeta::from_table(table, idx)))
        .collect();
    let mut col_to_group_pos: Vec<Option<usize>> = vec![None; table.schema.columns.len()];
    for (pos, &group_idx) in spec.group_table_indices.iter().enumerate() {
        if let Some(slot) = col_to_group_pos.get_mut(group_idx) {
            *slot = Some(pos);
        }
    }

    let mut out = Vec::with_capacity(merged.len());
    for (_, group) in merged {
        let mut cache: HashMap<u64, Value> = HashMap::with_capacity(spec.slots.len());
        for ((slot, scale), acc) in spec
            .slots
            .iter()
            .zip(spec.slot_fixed_scales.iter())
            .zip(group.accumulators.into_iter())
        {
            let value = acc.finalize()?;
            let value = match (scale, value) {
                (Some(scale), Value::Double(value)) => {
                    Value::Decimal(format!("{value:.scale$}", scale = usize::from(*scale)))
                }
                (_, value) => value,
            };
            cache.insert(slot.key, value);
        }
        let mut fields = Vec::with_capacity(col_metas.len() + group.key_values.len());
        for (idx, meta) in col_metas.iter().enumerate() {
            let value = match col_to_group_pos[idx] {
                Some(pos) => group.key_values[pos].clone(),
                None => Value::Null,
            };
            fields.push(PlanField {
                meta: meta.clone(),
                value,
            });
        }
        for (idx, value) in group.key_values.into_iter().enumerate() {
            fields.push(crate::engine::executor::select::aggregate::group_marker_field(idx, value));
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
    }
    Ok(out)
}

pub(super) fn index_min_max_value(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    col: &str,
    reverse: bool,
) -> Result<Value, QueryError> {
    let col_idx = index
        .columns
        .iter()
        .position(|c| c.eq_ignore_ascii_case(col))
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("MIN/MAX pushdown column {col} not in index {}", index.name),
            )
        })?;
    let table_col = table
        .schema
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(col))
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("MIN/MAX pushdown column {col} missing on table"),
            )
        })?;
    const CHUNK: usize = 256;
    let heap = db.cached_heap_handle(table.schema.id)?;
    if reverse {
        let mut cursor =
            db.range_index_snapshot_reverse_cursor(&table.schema, index, None, None, txn)?;
        loop {
            let chunk = cursor.next_chunk(CHUNK)?;
            if chunk.is_empty() {
                return Ok(Value::Null);
            }
            for (key, row_id) in &chunk {
                if !db.is_index_entry_version_visible(&table.schema, index, key, *row_id, txn)? {
                    continue;
                }
                if let Some(part) = key.0.get(col_idx) {
                    let v = part.to_value(&table_col.data_type);
                    if !matches!(v, Value::Null) {
                        return Ok(v);
                    }
                }
            }
        }
    } else {
        let mut cursor = db.range_index_snapshot_cursor(&table.schema, index, None, None, txn)?;
        loop {
            let chunk = cursor.next_chunk(CHUNK)?;
            if chunk.is_empty() {
                return Ok(Value::Null);
            }
            let visible_mask = db.filter_visible_index_entries_mask_with_handle(
                &heap,
                &table.schema,
                index,
                &chunk,
                txn,
            )?;
            for ((key, _row_id), visible) in chunk.into_iter().zip(visible_mask.into_iter()) {
                if !visible {
                    continue;
                }
                if let Some(part) = key.0.get(col_idx) {
                    let v = part.to_value(&table_col.data_type);
                    if !matches!(v, Value::Null) {
                        return Ok(v);
                    }
                }
            }
        }
    }
}

pub(crate) struct IndexEntryProjector {
    pub(super) metas: Vec<ColMetaRef>,
    pub(super) key_positions: Vec<Option<usize>>,
    pub(super) data_types: Vec<DataTypeKind>,
}

impl IndexEntryProjector {
    pub(crate) fn new(
        table: &PlanTable,
        index: &IndexSchema,
        projection: Option<&[usize]>,
    ) -> Self {
        // Both declared key columns and INCLUDE columns are encoded into
        // the IndexKey (in that order), so a covering scan can read either.
        let key_map: HashMap<String, usize> = index
            .effective_columns()
            .iter()
            .enumerate()
            .map(|(i, name)| (name.to_ascii_lowercase(), i))
            .collect();
        let columns: Vec<usize> = projection
            .map(|p| p.to_vec())
            .unwrap_or_else(|| logical_visible_column_indices(&table.schema));
        let mut metas = Vec::with_capacity(columns.len());
        let mut key_positions = Vec::with_capacity(columns.len());
        let mut data_types = Vec::with_capacity(columns.len());
        for col_idx in columns {
            let Some(column) = table.schema.columns.get(col_idx) else {
                continue;
            };
            key_positions.push(key_map.get(&column.name.to_ascii_lowercase()).copied());
            data_types.push(column.data_type.clone());
            metas.push(intern_col_meta(ColMeta::from_table(table, col_idx)));
        }
        Self {
            metas,
            key_positions,
            data_types,
        }
    }

    pub(crate) fn single_lossless_i64_key_position(&self) -> Option<usize> {
        let [meta] = self.metas.as_slice() else {
            return None;
        };
        if !matches!(
            meta.column_type,
            ColumnType::Tiny
                | ColumnType::Short
                | ColumnType::MediumInt
                | ColumnType::Long
                | ColumnType::LongLong
        ) {
            return None;
        }
        let unsigned = meta.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0;
        if unsigned && meta.column_type == ColumnType::LongLong {
            return None;
        }
        self.key_positions.first().copied().flatten()
    }

    pub(in crate::engine::executor::select) fn project(&self, key: &IndexKey) -> PlanRow {
        let mut fields = Vec::with_capacity(self.metas.len());
        for ((meta, key_pos), data_type) in self
            .metas
            .iter()
            .zip(self.key_positions.iter())
            .zip(self.data_types.iter())
        {
            let value = key_pos
                .and_then(|pos| key.0.get(pos))
                .map(|part| part.to_value(data_type))
                .unwrap_or(Value::Null);
            fields.push(PlanField {
                meta: meta.clone(),
                value,
            });
        }
        PlanRow {
            fields,
            group: None,
            agg_values: None,
            schema: PlanRow::empty_schema(),
        }
    }

    /// Append one covering-index key straight into native column builders.
    /// This is the batch equivalent of `project_values`, but it avoids the
    /// short-lived `Vec<Value>` allocation that a scan would otherwise pay
    /// once per index entry.
    pub(crate) fn append_to_accumulators(
        &self,
        key: &IndexKey,
        accumulators: &mut [crate::engine::executor::vec::batch::ColumnAccumulator],
    ) {
        for (((key_pos, data_type), accumulator), _meta) in self
            .key_positions
            .iter()
            .zip(self.data_types.iter())
            .zip(accumulators.iter_mut())
            .zip(self.metas.iter())
        {
            let value = key_pos
                .and_then(|pos| key.0.get(pos))
                .map(|part| part.to_value(data_type))
                .unwrap_or(Value::Null);
            accumulator.push(&value);
        }
    }
}

pub(in crate::engine::executor::select) fn execute_index_in_lookup_ids(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    index: &IndexSchema,
    key_prefix: &IndexKey,
    in_column: &str,
    subquery: &Select,
    subquery_plan: &PhysicalPlan,
) -> Result<
    (
        Vec<RowId>,
        crate::engine::executor::mem_budget::ScopedCharge,
    ),
    QueryError,
> {
    let rows = match execute_physical_select(db, txn, subquery, subquery_plan)
        .and_then(QueryResponse::materialize)?
    {
        QueryResponse::ResultSet(result) => result.rows,
        QueryResponse::SharedResultSet(result) => result.rows,
        _ => {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "IN subquery must return rows",
            ))
        }
    };
    // The IN key (`key_prefix` ∪ `in_column`) spans the whole index only
    // when it reaches the last index column. On a composite index where
    // `in_column` is a non-terminal prefix (e.g. inventory's PRIMARY
    // `(inv_date_sk, inv_item_sk, inv_warehouse_sk)` driven by
    // `inv_date_sk IN (...)`), an *exact* seek matches no full key and
    // silently returns zero rows. Fall back to a prefix scan so every row
    // whose leading columns equal the key is visited.
    let exact = key_prefix.0.len() + 1 >= index.columns.len();
    let mut row_ids = Vec::new();
    let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    let mut seen_values = HashSet::new();
    for row in rows {
        let Some(value) = row.first() else {
            continue;
        };
        if matches!(value, Value::Null) || !seen_values.insert(ValueKey::from(value)) {
            continue;
        }
        let mut parts = key_prefix.0.clone();
        let column = &table.columns[table.column_index(in_column)?];
        let target = crate::storage::ParamTarget::for_column_with_collation(
            &column.data_type,
            column.unsigned,
            column.collation.as_deref(),
        );
        let Some(part) = key_part_for_lookup_target(target, value)? else {
            continue;
        };
        parts.push(part);
        let key = IndexKey::from(parts);
        let ids = if exact {
            db.lookup_index_visible(table, index, key, txn)?
        } else {
            db.prefix_index_visible(table, index, key, txn)?
        };
        crate::engine::cancel::check_cancelled()?;
        append_lookup_row_ids(&mut row_ids, ids, &mut charge)?;
    }
    // Different SQL values can encode to the same index key (collation,
    // coercion), and MVCC repair can revisit an identifier. Deduplicate the
    // compact candidate array itself instead of retaining a second full-size
    // hash table. Physical order also gives downstream heap reads locality.
    row_ids.sort_unstable();
    row_ids.dedup();
    Ok((row_ids, charge))
}

pub(in crate::engine::executor::select) fn execute_index_multi_lookup_ids(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    index: &IndexSchema,
    keys: &[IndexKey],
    exact: bool,
) -> Result<Vec<RowId>, QueryError> {
    let mut row_ids = Vec::new();
    let mut seen = HashSet::new();
    let ordered_keys = runtime_ordered_multi_lookup_keys(keys)?;
    for key in ordered_keys.as_ref() {
        let ids = if exact {
            db.lookup_index_visible(table, index, key.clone(), txn)?
        } else {
            db.prefix_index_visible(table, index, key.clone(), txn)?
        };
        for row_id in ids {
            if seen.insert(row_id) {
                row_ids.push(row_id);
            }
        }
    }
    Ok(row_ids)
}

fn append_lookup_row_ids(
    out: &mut Vec<RowId>,
    ids: Vec<RowId>,
    charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
) -> Result<(), QueryError> {
    if out.capacity() == 0 {
        // Transfer the first probe's allocation instead of allocating and
        // copying it again. Subsequent growth is geometric, so a large keyset
        // cannot cause one full-array reallocation per probe.
        charge.add_force(ids.capacity().saturating_mul(std::mem::size_of::<RowId>()))?;
        *out = ids;
        return Ok(());
    }
    let required = out.len().checked_add(ids.len()).ok_or_else(|| {
        crate::engine::executor::mem_budget::allocation_error("lookup row identifier overflow")
    })?;
    if required > out.capacity() {
        let capacity = required.max(out.capacity().saturating_mul(2));
        crate::engine::executor::mem_budget::try_reserve_vec_hard(
            out,
            capacity - out.len(),
            charge,
            "lookup row identifiers",
        )?;
    }
    out.extend(ids);
    Ok(())
}

/// Execute the RowId-only part of an Index Merge. Each recursive result is
/// sorted and unique, which makes nested union/intersection deterministic and
/// gives the final heap fetch physical-page locality.
pub(crate) fn execute_index_merge_row_ids(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    node: &crate::planner::IndexMergeNode,
) -> Result<Vec<RowId>, QueryError> {
    let (ids, candidate_rowids, duplicates_removed) =
        execute_index_merge_row_ids_inner(db, txn, table, node)?;
    crate::engine::executor::select::analyze_stats::add_index_merge_work(
        node.leaf_count() as u64,
        candidate_rowids,
        duplicates_removed,
    );
    Ok(ids)
}

pub(super) fn execute_index_merge_row_ids_inner(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    node: &crate::planner::IndexMergeNode,
) -> Result<(Vec<RowId>, u64, u64), QueryError> {
    use crate::planner::{IndexAccessKind, IndexMergeNode};

    let (mut ids, candidate_rowids, mut duplicates_removed) = match node {
        IndexMergeNode::Leaf(leaf) => {
            let ids = match &leaf.access {
                IndexAccessKind::Lookup { key, exact } => {
                    if *exact {
                        db.lookup_index_visible(table, &leaf.index, key.clone(), txn)?
                    } else {
                        db.prefix_index_visible(table, &leaf.index, key.clone(), txn)?
                    }
                }
                IndexAccessKind::MultiLookup { keys, exact } => {
                    execute_index_multi_lookup_ids(db, txn, table, &leaf.index, keys, *exact)?
                }
                IndexAccessKind::Range { low, high } => db.range_index_bounds_visible(
                    table,
                    &leaf.index,
                    low.clone(),
                    high.clone(),
                    txn,
                )?,
            };
            crate::engine::executor::select::analyze_stats::add_range_scan_work(
                ids.len() as u64,
                0,
                0,
            );
            let candidates = ids.len() as u64;
            (ids, candidates, 0u64)
        }
        IndexMergeNode::Union { children, .. } => {
            let mut merged = Vec::new();
            let mut candidates = 0u64;
            let mut duplicates = 0u64;
            for child in children {
                crate::engine::cancel::check_cancelled()?;
                let (child_ids, child_candidates, child_duplicates) =
                    execute_index_merge_row_ids_inner(db, txn, table, child)?;
                candidates = candidates.saturating_add(child_candidates);
                duplicates = duplicates.saturating_add(child_duplicates);
                merged.extend(child_ids);
            }
            let before = merged.len();
            merged.sort_unstable();
            merged.dedup();
            duplicates = duplicates.saturating_add((before - merged.len()) as u64);
            return Ok((merged, candidates, duplicates));
        }
        IndexMergeNode::Intersection { children } => {
            let mut children = children.iter();
            let Some(first) = children.next() else {
                return Ok((Vec::new(), 0, 0));
            };
            let (mut intersection, mut candidates, mut duplicates) =
                execute_index_merge_row_ids_inner(db, txn, table, first)?;
            for child in children {
                crate::engine::cancel::check_cancelled()?;
                let (other, child_candidates, child_duplicates) =
                    execute_index_merge_row_ids_inner(db, txn, table, child)?;
                candidates = candidates.saturating_add(child_candidates);
                duplicates = duplicates.saturating_add(child_duplicates);
                intersection.retain(|row_id| other.binary_search(row_id).is_ok());
                if intersection.is_empty() {
                    break;
                }
            }
            return Ok((intersection, candidates, duplicates));
        }
    };
    let before = ids.len();
    ids.sort_unstable();
    ids.dedup();
    duplicates_removed = duplicates_removed.saturating_add((before - ids.len()) as u64);
    Ok((ids, candidate_rowids, duplicates_removed))
}

pub(in crate::engine::executor::select) fn execute_index_multi_lookup_entries(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    index: &IndexSchema,
    keys: &[IndexKey],
    exact: bool,
) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    let ordered_keys = runtime_ordered_multi_lookup_keys(keys)?;
    for key in ordered_keys.as_ref() {
        let key_entries = if exact {
            db.lookup_index_entries_visible(table, index, key.clone(), txn)?
        } else {
            db.prefix_index_entries_visible(table, index, key.clone(), txn)?
        };
        for entry in key_entries {
            if seen.insert(entry.1) {
                entries.push(entry);
            }
        }
    }
    Ok(entries)
}

/// Return multi-lookup probes in index-key order after resolving prepared
/// parameters. Plans deliberately retain `KeyPart::Param` so they are reusable;
/// sorting those placeholders at plan time orders by parameter number, not by
/// the bound key. MySQL's range/multi-range access emits rows in index order,
/// and a stable runtime ordering also prevents execution-route-dependent row
/// order. Concrete plans stay allocation-free.
pub(in crate::engine::executor::select) fn runtime_ordered_multi_lookup_keys(
    keys: &[IndexKey],
) -> Result<std::borrow::Cow<'_, [IndexKey]>, QueryError> {
    if !keys.iter().any(IndexKey::contains_param) {
        return Ok(std::borrow::Cow::Borrowed(keys));
    }
    let mut keyed = keys
        .iter()
        .map(|key| Ok((key.resolve_params()?, key.clone())))
        .collect::<Result<Vec<_>, QueryError>>()?;
    keyed.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    keyed.dedup_by(|left, right| left.0 == right.0);
    Ok(std::borrow::Cow::Owned(
        keyed.into_iter().map(|(_, original)| original).collect(),
    ))
}

pub(in crate::engine::executor::select) fn execute_index_in_lookup_entries(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    index: &IndexSchema,
    key_prefix: &IndexKey,
    in_column: &str,
    subquery: &Select,
    subquery_plan: &PhysicalPlan,
) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
    let QueryResponse::ResultSet(result) =
        execute_physical_select(db, txn, subquery, subquery_plan)
            .and_then(QueryResponse::materialize)?
    else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "IN subquery must return rows",
        ));
    };
    // See `execute_index_in_lookup_ids`: a non-terminal prefix of a
    // composite index needs a prefix scan, not an exact seek, or the
    // covering IN-lookup returns zero entries.
    let exact = key_prefix.0.len() + 1 >= index.columns.len();
    let mut entries = Vec::new();
    let mut seen_values = HashSet::new();
    let mut seen_row_ids = HashSet::new();
    for row in result.rows {
        let Some(value) = row.first() else {
            continue;
        };
        if matches!(value, Value::Null) || !seen_values.insert(ValueKey::from(value)) {
            continue;
        }
        let mut parts = key_prefix.0.clone();
        let column = &table.columns[table.column_index(in_column)?];
        let target = crate::storage::ParamTarget::for_column_with_collation(
            &column.data_type,
            column.unsigned,
            column.collation.as_deref(),
        );
        let Some(part) = key_part_for_lookup_target(target, value)? else {
            continue;
        };
        parts.push(part);
        let key = IndexKey::from(parts);
        let key_entries = if exact {
            db.lookup_index_entries_visible(table, index, key, txn)?
        } else {
            db.prefix_index_entries_visible(table, index, key, txn)?
        };
        for entry in key_entries {
            if seen_row_ids.insert(entry.1) {
                entries.push(entry);
            }
        }
    }
    Ok(entries)
}

pub(in crate::engine::executor::select) fn push_table_fields(
    out: &mut Vec<ColMeta>,
    table: &PlanTable,
    projection: Option<&[usize]>,
) {
    if let Some(indices) = projection {
        for &i in indices {
            if i < table.schema.columns.len() {
                out.push(ColMeta::from_table(table, i));
            }
        }
    } else {
        for (idx, _) in table.schema.iter_visible_columns() {
            out.push(ColMeta::from_table(table, idx));
        }
    }
}

/// Return the cached `&'static ColMeta` slice for `table`, populating it
/// on first call. Index `i` holds the meta for schema column `i`, so both
/// full-row and projected materialisers can index directly without
/// hashing, cloning, or probing the global intern table per cell.
pub(in crate::engine::executor::select) fn cached_column_metas(table: &PlanTable) -> &[ColMetaRef] {
    table
        .column_metas
        .get_or_init(|| {
            table
                .schema
                .columns
                .iter()
                .enumerate()
                .map(|(idx, _)| intern_col_meta(ColMeta::from_table(table, idx)))
                .collect()
        })
        .as_slice()
}

/// A positive query term's scoring state: its document frequency `df`
/// (for IDF) plus a per-candidate-row `(tf, doc_len)` map decoded from
/// the postings — so scoring is a pure payload read, no heap retable.
pub(super) struct FtScoringTerm {
    pub(super) df: usize,
    pub(super) by_row: AhHashMap<RowId, (u32, u32)>,
}

/// Fetch (and memoise) a token's postings via the fast payload-decoding
/// scan API. Boolean evaluation touches the same token from set
/// membership, phrase verification and scoring; caching keeps the leaf
/// walk + payload decode to once per distinct token.
///
/// `want_positions=true` decodes the position arena (phrase clauses);
/// `false` decodes only `(doc_len, tf)` (skips the tf-many varints per
/// posting for pure BM25 / set-membership use).
pub(super) fn fulltext_cached_postings<'c>(
    cache: &'c mut AhHashMap<String, crate::storage::fulltext::FtPostingBatch>,
    want_positions_cache: &mut AhHashSet<String>,
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    token: &str,
    want_positions: bool,
) -> Result<&'c crate::storage::fulltext::FtPostingBatch, QueryError> {
    // If a previous fetch skipped positions but this call needs them, redo
    // the scan with positions enabled.
    let needs_rescan =
        want_positions && cache.contains_key(token) && !want_positions_cache.contains(token);
    if needs_rescan {
        cache.remove(token);
    }
    if !cache.contains_key(token) {
        let batch = db.fulltext_scan_token(&table.schema, index, token, want_positions, txn)?;
        cache.insert(token.to_string(), batch);
        if want_positions {
            want_positions_cache.insert(token.to_string());
        }
    }
    Ok(cache.get(token).expect("just inserted"))
}

/// True when the phrase tokens occur at consecutive positions in some
/// document — `positions[0]` holds the first token's positions, etc. Each
/// list is ascending (emitted in token order), so membership is a binary
/// search. Replaces the heap re-tokenise + `windows()` adjacency check.
pub(super) fn positions_phrase_match(per_token: &[&[u32]]) -> bool {
    let Some(first) = per_token.first() else {
        return true;
    };
    'start: for &p0 in first.iter() {
        for (i, positions) in per_token.iter().enumerate().skip(1) {
            let want = p0.wrapping_add(i as u32);
            if positions.binary_search(&want).is_err() {
                continue 'start;
            }
        }
        return true;
    }
    false
}

/// Re-tokenize each column separately, preserving the write-side token order
/// without introducing adjacency between different columns.
pub(super) fn fulltext_row_contains_phrase(
    tokenizer: &dyn crate::storage::fulltext::Tokenizer,
    table: &TableSchema,
    columns: &[String],
    values: &[Value],
    needle: &[String],
) -> Result<bool, QueryError> {
    if needle.is_empty() {
        return Ok(true);
    }
    for column in columns {
        let idx = table.column_index(column)?;
        let text = match values.get(idx) {
            Some(Value::Null) | None => continue,
            Some(value) => value.to_display_string(),
        };
        let tokens = tokenizer.tokenize(&text);
        if tokens.windows(needle.len()).any(|window| window == needle) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Natural-language candidates: union of per-token postings (OR), plus the
/// per-term scoring state (df + per-row tf/doc_len from payloads) for BM25.
///
/// Fast path (2026-07): uses `Database::fulltext_scan_token` which decodes
/// payloads inline off B-tree leaves. NL never needs positions, so the
/// scan skips the tf-many position varints per posting AND avoids
/// cloning the `String` token / `Vec<u8>` payload per entry that the
/// generic `prefix_index_entries` path pays. For `df ≈ 22k` hot queries
/// this eliminates ~44k allocations per token.
///
/// `need_scoring=false` fast-fastest path: when the caller only needs the
/// candidate set (no BM25 score reference anywhere in the plan) we skip
/// per-term `by_row` maps entirely and use one shared `AhHashSet<RowId>`.
pub(super) fn fulltext_natural_language_candidates(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    tokenizer: &dyn crate::storage::fulltext::Tokenizer,
    query: &str,
    need_scoring: bool,
) -> Result<(Vec<RowId>, Vec<FtScoringTerm>), QueryError> {
    // Distinct query tokens preserve first-encounter order for
    // determinism; the actual set operation is a sort/dedup over the
    // per-token postings, avoiding a `df`-sized `AhHashSet<RowId>` insert
    // per posting. For 2 tokens × 22k postings a Vec+sort is ~1ms
    // versus ~2-3ms of hashing.
    let mut seen_terms = ah_hash_set();
    let mut row_ids: Vec<RowId> = Vec::new();
    let mut terms: Vec<FtScoringTerm> = Vec::new();
    for token in tokenizer.tokenize(query) {
        if !seen_terms.insert(token.clone()) {
            continue;
        }
        if need_scoring {
            let batch = db.fulltext_scan_token(&table.schema, index, &token, false, txn)?;
            if batch.postings.is_empty() {
                continue;
            }
            let mut by_row = ah_hash_map_with_capacity(batch.postings.len());
            row_ids.reserve(batch.postings.len());
            for p in &batch.postings {
                row_ids.push(p.row_id);
                by_row.insert(p.row_id, (p.tf, p.doc_len));
            }
            terms.push(FtScoringTerm {
                df: batch.postings.len(),
                by_row,
            });
        } else {
            // Row-id-only fast path: NL union with `need_score=false`
            // needs only the row-id set. Skips payload decode entirely,
            // which halves per-posting work again for `df ≈ 22k` tokens.
            let ids = db.fulltext_scan_token_ids(&table.schema, index, &token, txn)?;
            if ids.is_empty() {
                continue;
            }
            let df = ids.len();
            row_ids.reserve(df);
            row_ids.extend(ids);
            // Still track `df` so downstream avgdl floor is honest.
            terms.push(FtScoringTerm {
                df,
                by_row: ah_hash_map(),
            });
        }
    }
    row_ids.sort_unstable();
    row_ids.dedup();
    Ok((row_ids, terms))
}

/// Boolean-mode candidates: evaluate `+ - " " *` clauses into a row set
/// (intersection of Musts, else union of Shoulds, minus MustNots) and
/// collect the positive tokens' scoring state for BM25. Phrase adjacency
/// is verified from persisted positions (no heap retable); only payloads
/// built with positions disabled fall back to re-tokenising the row.
///
/// Fast path (2026-07): uses `Database::fulltext_scan_token` /
/// `fulltext_scan_prefix` which decode payloads inline off B-tree
/// leaves — no `IndexKey` clone per posting; positions live in a
/// per-token arena so phrase intersection can share `&[u32]` slices
/// instead of cloning `Vec<u32>` for every posting.
#[allow(clippy::too_many_arguments)]
pub(super) fn fulltext_boolean_candidates(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    tokenizer: &dyn crate::storage::fulltext::Tokenizer,
    columns: &[String],
    query: &str,
    need_scoring: bool,
) -> Result<(Vec<RowId>, Vec<FtScoringTerm>), QueryError> {
    use crate::storage::fulltext::boolean::{parse_boolean_query, Matcher, Occur};
    use crate::util::fast_hash::{AhHashMap as HashMap, AhHashSet as HashSet};

    // Unsupported boolean operators (`( )`, `> < ~`, `@`) now surface as
    // error 1235 instead of being silently dropped.
    let clauses = parse_boolean_query(query, tokenizer)?;
    if clauses.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    // One prefix scan per distinct token, reused across membership, phrase
    // verification and scoring.
    let mut cache: HashMap<String, crate::storage::fulltext::FtPostingBatch> = ah_hash_map();
    let mut positions_ready: HashSet<String> = ah_hash_set();
    // Positive (non-MustNot) Term/Phrase tokens, first-seen order, for scoring.
    let mut positive_tokens: Vec<String> = Vec::new();
    let mut positive_seen: HashSet<String> = ah_hash_set();
    let mut must_sets: Vec<HashSet<RowId>> = Vec::new();
    let mut should_sets: Vec<HashSet<RowId>> = Vec::new();
    let mut mustnot_sets: Vec<HashSet<RowId>> = Vec::new();

    // Pre-scan clauses to know which tokens need positions (phrase clauses).
    let mut phrase_tokens: AhHashSet<&str> = ah_hash_set();
    for clause in &clauses {
        if let Matcher::Phrase(tokens) = &clause.matcher {
            for t in tokens {
                phrase_tokens.insert(t.as_str());
            }
        }
    }

    for clause in &clauses {
        let positive = clause.occur != Occur::MustNot;
        let rowset = match &clause.matcher {
            Matcher::Term(tokens) => {
                let mut set: Option<HashSet<RowId>> = None;
                for tok in tokens {
                    let want_pos = phrase_tokens.contains(tok.as_str());
                    let batch = fulltext_cached_postings(
                        &mut cache,
                        &mut positions_ready,
                        db,
                        txn,
                        table,
                        index,
                        tok,
                        want_pos,
                    )?;
                    let s: HashSet<RowId> = batch.postings.iter().map(|p| p.row_id).collect();
                    set = Some(match set {
                        None => s,
                        Some(prev) => prev.intersection(&s).copied().collect(),
                    });
                    if positive && positive_seen.insert(tok.clone()) {
                        positive_tokens.push(tok.clone());
                    }
                }
                set.unwrap_or_default()
            }
            Matcher::Prefix(prefix) => {
                // Expand `word*` into the concrete tokens it matches and
                // feed each into the shared postings cache. Positive
                // clauses also promote those tokens into `positive_tokens`
                // so BM25 scores wildcard hits with their real tf/df — MySQL
                // parity, and the fix for previously-zero prefix scores.
                let groups = db.fulltext_scan_prefix(&table.schema, index, prefix, txn)?;
                let mut rowset: HashSet<RowId> = ah_hash_set();
                for (token, batch) in groups {
                    for p in &batch.postings {
                        rowset.insert(p.row_id);
                    }
                    if positive && positive_seen.insert(token.clone()) {
                        positive_tokens.push(token.clone());
                    }
                    // Prefix scan does not include positions; store the
                    // batch so scoring can reuse (tf, doc_len) without a
                    // re-scan.
                    if !cache.contains_key(&token) {
                        cache.insert(token.clone(), batch);
                    }
                }
                rowset
            }
            Matcher::Phrase(tokens) => {
                // Fetch each phrase token's postings with positions and
                // record the per-row position slice range (into that
                // token's arena) so adjacency can be verified without
                // cloning any `Vec<u32>`. We *own* the batches here
                // (bypassing the shared `cache`) so downstream Rust
                // borrow rules stay simple; phrase tokens are usually
                // used only in the phrase clause anyway, so cache reuse
                // is minimal. Scoring later re-fetches the same tokens
                // via the cache (a fresh scan; still cheap thanks to
                // fast_hash + arena decode).
                let mut per_token_batches: Vec<crate::storage::fulltext::FtPostingBatch> =
                    Vec::with_capacity(tokens.len());
                let mut per_token_entries: Vec<Vec<(RowId, u32, u32)>> =
                    Vec::with_capacity(tokens.len());
                let mut candidate: Option<HashSet<RowId>> = None;
                for tok in tokens {
                    let batch = db.fulltext_scan_token(&table.schema, index, tok, true, txn)?;
                    let mut entries: Vec<(RowId, u32, u32)> =
                        Vec::with_capacity(batch.postings.len());
                    let mut rowset: HashSet<RowId> =
                        ah_hash_set_with_capacity(batch.postings.len());
                    for p in &batch.postings {
                        rowset.insert(p.row_id);
                        entries.push((p.row_id, p.pos_start, p.pos_len));
                    }
                    entries.sort_unstable_by_key(|(row_id, _, _)| *row_id);
                    candidate = Some(match candidate {
                        None => rowset,
                        Some(prev) => prev.intersection(&rowset).copied().collect(),
                    });
                    per_token_entries.push(entries);
                    per_token_batches.push(batch);
                    if positive && positive_seen.insert(tok.clone()) {
                        positive_tokens.push(tok.clone());
                    }
                }
                let candidate = candidate.unwrap_or_default();
                let mut verified = ah_hash_set();
                let mut positions_scratch: Vec<&[u32]> = Vec::with_capacity(tokens.len());
                for row_id in candidate {
                    positions_scratch.clear();
                    let mut have_positions = true;
                    for (entries, batch) in per_token_entries.iter().zip(per_token_batches.iter()) {
                        // Binary search by row_id (sorted ascending).
                        let idx = entries.binary_search_by_key(&row_id, |(r, _, _)| *r);
                        let pos_slice = match idx {
                            Ok(i) => {
                                let (_, start, len) = entries[i];
                                let s = start as usize;
                                let e = s + len as usize;
                                &batch.positions_arena[s..e]
                            }
                            Err(_) => &[][..],
                        };
                        if pos_slice.is_empty() {
                            // tf>0 but no positions → built with positions off.
                            have_positions = false;
                            break;
                        }
                        positions_scratch.push(pos_slice);
                    }
                    let matched = if have_positions && positions_scratch.len() == tokens.len() {
                        positions_phrase_match(&positions_scratch)
                    } else {
                        // Fallback: re-tokenise the row and check adjacency.
                        match db.read_row(&table.schema, row_id, txn)? {
                            Some(stored) => fulltext_row_contains_phrase(
                                tokenizer,
                                &table.schema,
                                columns,
                                &stored.values,
                                tokens,
                            )?,
                            None => false,
                        }
                    };
                    if matched {
                        verified.insert(row_id);
                    }
                }
                verified
            }
        };
        match clause.occur {
            Occur::Must => must_sets.push(rowset),
            Occur::Should => should_sets.push(rowset),
            Occur::MustNot => mustnot_sets.push(rowset),
        }
    }

    // Membership: all Musts intersected; without any Must, the union of
    // Shoulds. MustNots are then subtracted.
    let mut base: HashSet<RowId> = if !must_sets.is_empty() {
        let mut iter = must_sets.into_iter();
        let mut acc = iter.next().unwrap_or_default();
        for s in iter {
            acc = acc.intersection(&s).copied().collect();
        }
        acc
    } else {
        let mut acc = ah_hash_set();
        for s in should_sets {
            acc.extend(s);
        }
        acc
    };
    for s in &mustnot_sets {
        for id in s {
            base.remove(id);
        }
    }

    // Build scoring state from the cached postings of positive tokens.
    // Skip when the caller does not need BM25 — saves per-term
    // hashmaps for the "score not referenced" query shape.
    let mut scoring: Vec<FtScoringTerm> = Vec::with_capacity(positive_tokens.len());
    if need_scoring {
        for tok in &positive_tokens {
            let batch = fulltext_cached_postings(
                &mut cache,
                &mut positions_ready,
                db,
                txn,
                table,
                index,
                tok,
                false,
            )?;
            let mut by_row = ah_hash_map_with_capacity(batch.postings.len());
            for p in &batch.postings {
                by_row.insert(p.row_id, (p.tf, p.doc_len));
            }
            scoring.push(FtScoringTerm {
                df: batch.postings.len(),
                by_row,
            });
        }
    } else {
        for tok in &positive_tokens {
            let batch = fulltext_cached_postings(
                &mut cache,
                &mut positions_ready,
                db,
                txn,
                table,
                index,
                tok,
                false,
            )?;
            scoring.push(FtScoringTerm {
                df: batch.postings.len(),
                by_row: ah_hash_map(),
            });
        }
    }

    let mut candidates: Vec<RowId> = base.into_iter().collect();
    candidates.sort_unstable();
    Ok((candidates, scoring))
}

pub(super) struct FtMatchRuntime {
    pub(super) fingerprint: u64,
    pub(super) restrict: bool,
    pub(super) need_score: bool,
    pub(super) candidates: Vec<RowId>,
    pub(super) scoring_terms: Vec<FtScoringTerm>,
    pub(super) idfs: Vec<f64>,
    pub(super) doc_len_of: AhHashMap<RowId, u32>,
    pub(super) avgdl: f64,
}

pub(super) fn resolve_fulltext_query(
    query: &str,
    query_param: Option<usize>,
) -> Result<String, QueryError> {
    let Some(idx) = query_param else {
        return Ok(query.to_string());
    };
    let value = crate::engine::params::current_param(idx).ok_or_else(|| {
        error(
            crate::error::catalog::ER_WRONG_ARGUMENTS,
            "parameter is not bound",
        )
    })?;
    Ok(match value {
        Value::Null => String::new(),
        Value::String(s) | Value::Decimal(s) => s,
        Value::Bytes(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        other => other.to_display_string(),
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_fulltext_match_runtime(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    mode: crate::parser::FtMode,
    query: &str,
    query_param: Option<usize>,
    columns: &[String],
    fingerprint: u64,
    restrict: bool,
    need_score: bool,
) -> Result<FtMatchRuntime, QueryError> {
    let query = resolve_fulltext_query(query, query_param)?;
    let default_meta = crate::storage::schema::FulltextMeta::default();
    let meta = index.fulltext.as_ref().unwrap_or(&default_meta);
    let tokenizer = crate::storage::fulltext::build_tokenizer_from_meta(meta);
    let (candidates, scoring_terms) = match mode {
        crate::parser::FtMode::NaturalLanguage => fulltext_natural_language_candidates(
            db,
            txn,
            table,
            index,
            tokenizer.as_ref(),
            &query,
            need_score,
        )?,
        crate::parser::FtMode::Boolean => fulltext_boolean_candidates(
            db,
            txn,
            table,
            index,
            tokenizer.as_ref(),
            columns,
            &query,
            need_score,
        )?,
    };
    let mut idfs = Vec::new();
    let mut doc_len_of = ah_hash_map();
    let mut avgdl = 1.0;
    if need_score {
        let frozen = super::super::fulltext_context::statistics(table, index)?;
        // The unsigned modified-row counter counts INSERT, UPDATE and DELETE
        // and is only a staleness trigger. BM25's document count must use the
        // signed committed row delta, otherwise update-heavy tables make IDF
        // drift upward without gaining a single document.
        let est = frozen.base_n;
        let max_df = scoring_terms.iter().map(|term| term.df).max().unwrap_or(0) as f64;
        let n_docs = est.max(candidates.len() as f64).max(max_df).max(1.0);
        idfs = scoring_terms
            .iter()
            .map(|term| crate::storage::fulltext::bm25::idf(n_docs, term.df as f64))
            .collect();
        for term in &scoring_terms {
            for (row_id, (_, doc_len)) in &term.by_row {
                doc_len_of.entry(*row_id).or_insert(*doc_len);
            }
        }
        let (sum, count) = candidates
            .iter()
            .fold((0u64, 0u64), |(sum, count), row_id| {
                doc_len_of
                    .get(row_id)
                    .map(|len| (sum + *len as u64, count + 1))
                    .unwrap_or((sum, count))
            });
        if count > 0 {
            avgdl = (sum as f64 / count as f64).max(1.0);
        }
        if let Some(corpus_avg) = frozen.corpus_avg {
            avgdl = corpus_avg;
        }
    }
    Ok(FtMatchRuntime {
        fingerprint,
        restrict,
        need_score,
        candidates,
        scoring_terms,
        idfs,
        doc_len_of,
        avgdl,
    })
}

pub(super) fn fulltext_runtime_score(runtime: &FtMatchRuntime, row_id: RowId) -> f64 {
    if !runtime.need_score {
        return 0.0;
    }
    let params = crate::storage::fulltext::bm25::global_params();
    let dl = runtime.doc_len_of.get(&row_id).copied().unwrap_or(0) as f64;
    runtime
        .scoring_terms
        .iter()
        .enumerate()
        .filter_map(|(idx, term)| {
            term.by_row.get(&row_id).map(|(tf, _)| {
                crate::storage::fulltext::bm25::term_score(
                    runtime.idfs[idx],
                    *tf as f64,
                    dl,
                    runtime.avgdl,
                    params,
                )
            })
        })
        .fold(0.0, |score, term| score + term)
}

pub(super) fn execute_fulltext_like_prefilter(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    plan: &crate::planner::FulltextLikePrefilterPlan,
    filter: &Option<Expr>,
    projected_columns: Option<&Arc<Vec<usize>>>,
) -> Result<Vec<PlanRow>, QueryError> {
    let runtime_tokens = if let Some(param) = plan.pattern_param {
        let Some(value) = crate::engine::params::current_param(param) else {
            return Err(error(
                crate::error::catalog::ER_WRONG_ARGUMENTS,
                "LIKE parameter is not bound",
            ));
        };
        let pattern = match value {
            Value::Null => return Ok(Vec::new()),
            Value::String(text) => text,
            Value::Bytes(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            other => other.to_display_string(),
        };
        let segments = crate::engine::expr::like_literal_segments(&pattern, plan.escape_char)
            .unwrap_or_default();
        let default_meta = crate::storage::FulltextMeta::default();
        let meta = index.fulltext.as_ref().unwrap_or(&default_meta);
        let tokenizer = crate::storage::fulltext::build_tokenizer_from_meta(meta);
        let column = table
            .schema
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&plan.column))
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    "LIKE ngram column not found",
                )
            })?;
        let mut seen = AhHashSet::default();
        let mut tokens = Vec::new();
        for segment in segments {
            for token in tokenizer.tokenize(&segment) {
                if crate::planner::ngram_token_is_like_safe(column, &token)
                    && seen.insert(token.clone())
                {
                    tokens.push(token);
                    if tokens.len() >= 64 {
                        break;
                    }
                }
            }
            if tokens.len() >= 64 {
                break;
            }
        }
        tokens
    } else {
        plan.tokens.clone()
    };
    if runtime_tokens.is_empty() || !crate::storage::schema::like_ngram_prefilter_enabled() {
        crate::engine::status_counters::record_like_ngram_no_safe_token();
        let ids = db
            .scan_rows(&table.schema.qualified_name(), txn)?
            .into_iter()
            .map(|row| row.row_id)
            .collect();
        return read_plan_table_ids(
            db,
            txn,
            table,
            ids,
            filter,
            projected_columns.map(|columns| columns.as_slice()),
        );
    }
    let mut postings = Vec::with_capacity(runtime_tokens.len());
    for token in &runtime_tokens {
        let mut ids = db.fulltext_scan_token_ids(&table.schema, index, token, txn)?;
        ids.sort_unstable();
        ids.dedup();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        postings.push((ids.len(), ids));
    }
    let posting_rows = postings.iter().map(|(df, _)| *df as u64).sum::<u64>();
    postings.sort_unstable_by_key(|(df, _)| *df);
    let mut selected = postings.into_iter().take(8);
    let Some((_, mut candidates)) = selected.next() else {
        return Ok(Vec::new());
    };
    for (_, ids) in selected {
        let keep: AhHashSet<RowId> = ids.into_iter().collect();
        candidates.retain(|row_id| keep.contains(row_id));
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
    }
    crate::engine::status_counters::record_like_ngram_rows(posting_rows, candidates.len() as u64);
    let mut out = Vec::with_capacity(candidates.len());
    let mut eval_ctx = EvalQueryContext::default();
    for row_id in candidates {
        let Some(stored) = db.read_row(&table.schema, row_id, txn)? else {
            continue;
        };
        let row = match projected_columns {
            Some(columns) => stored_row_to_plan_row_projected(table, stored.values, columns)?,
            None => stored_row_to_plan_row(table, stored.values)?,
        };
        let keep = match filter {
            Some(expr) => eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)?,
            None => true,
        };
        if keep {
            out.push(row);
        }
    }
    Ok(out)
}

/// Execute a `MATCH(cols) AGAINST(query [mode])` full-text search. Computes
/// the matching row set for the mode (natural-language OR vs boolean
/// `+ - " " *`), scores each candidate with BM25 over the positive query
/// tokens, and injects the relevance as a hidden `__ft_score_<fingerprint>`
/// pseudo-column so WHERE / SELECT / ORDER BY occurrences of the same MATCH
/// read it back by fingerprint. `filter` is the WHERE remainder after the
/// bare MATCH predicate was stripped at plan time, applied per candidate row.
///
/// Score push-down (2026-07): when the enclosing plan never reads back
/// `__ft_score_<fingerprint>` (SELECT/ORDER BY don't reference the same
/// MATCH, and `filter` doesn't either), scoring is skipped entirely —
/// no per-term `by_row` map, no `doc_len_of` hashmap, no BM25 arithmetic.
/// The FT scan still produces the score pseudo-column set to 0.0 so any
/// residual expression that stumbles onto it via `resolve_ft_score`
/// remains well-defined (returns 0 rather than error 1235).
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_fulltext_scan(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    mode: crate::parser::FtMode,
    query: &str,
    query_param: Option<usize>,
    columns: &[String],
    match_fingerprint: u64,
    filter: &Option<Expr>,
    projected_columns: Option<&Arc<Vec<usize>>>,
    need_score: bool,
    restrict_to_matches: bool,
    additional_matches: &[crate::planner::FulltextMatchPlan],
    like_prefilter: Option<&crate::planner::FulltextLikePrefilterPlan>,
    base_access: Option<&crate::planner::FulltextBaseAccess>,
) -> Result<Vec<PlanRow>, QueryError> {
    if let Some(like_prefilter) = like_prefilter {
        return execute_fulltext_like_prefilter(
            db,
            txn,
            table,
            index,
            like_prefilter,
            filter,
            projected_columns,
        );
    }
    let primary = build_fulltext_match_runtime(
        db,
        txn,
        table,
        index,
        mode,
        query,
        query_param,
        columns,
        match_fingerprint,
        restrict_to_matches,
        need_score,
    )?;
    let mut runtimes = Vec::with_capacity(additional_matches.len() + 1);
    runtimes.push(primary);
    for spec in additional_matches {
        runtimes.push(build_fulltext_match_runtime(
            db,
            txn,
            table,
            &spec.index,
            spec.mode,
            &spec.query,
            spec.query_param,
            &spec.columns,
            spec.match_fingerprint,
            spec.restrict_to_matches,
            spec.needs_score,
        )?);
    }

    let restrict_count = runtimes.iter().filter(|runtime| runtime.restrict).count();
    if restrict_count > 1 {
        crate::engine::status_counters::record_fulltext_multi_intersection();
    } else if restrict_count == 0 {
        crate::engine::status_counters::record_fulltext_aware_full_scan();
    }
    let base_candidates = match base_access {
        Some(crate::planner::FulltextBaseAccess::Lookup { index, key, exact }) => Some(if *exact {
            db.lookup_index_visible(&table.schema, index, key.clone(), txn)?
        } else {
            db.prefix_index_visible(&table.schema, index, key.clone(), txn)?
        }),
        Some(crate::planner::FulltextBaseAccess::MultiLookup { index, keys, exact }) => Some(
            execute_index_multi_lookup_ids(db, txn, &table.schema, index, keys, *exact)?,
        ),
        Some(crate::planner::FulltextBaseAccess::Range { index, low, high }) => Some(
            db.range_index_bounds_visible(&table.schema, index, low.clone(), high.clone(), txn)?,
        ),
        Some(crate::planner::FulltextBaseAccess::ZoneMap { filter }) => {
            let ids = if let Some(ranges) =
                db.zone_map_candidate_ranges_for_plan(&table.schema, filter)
            {
                let mut ids = Vec::new();
                for (page_lo, page_hi) in ranges {
                    ids.extend(
                        db.scan_rows_storage_range(
                            &table.schema,
                            table.schema.id,
                            txn,
                            page_lo,
                            page_hi,
                        )?
                        .into_iter()
                        .map(|row| row.row_id),
                    );
                }
                ids
            } else {
                db.scan_rows(&table.schema.qualified_name(), txn)?
                    .into_iter()
                    .map(|row| row.row_id)
                    .collect()
            };
            Some(ids)
        }
        None => None,
    };
    let mut restricting = runtimes.iter().filter(|runtime| runtime.restrict);
    let mut candidates = if let Some(first) = restricting.next() {
        let mut base = first.candidates.clone();
        for runtime in restricting {
            let keep: AhHashSet<RowId> = runtime.candidates.iter().copied().collect();
            base.retain(|row_id| keep.contains(row_id));
        }
        if let Some(index_candidates) = base_candidates {
            let keep: AhHashSet<RowId> = index_candidates.into_iter().collect();
            base.retain(|row_id| keep.contains(row_id));
        }
        base
    } else if let Some(index_candidates) = base_candidates {
        index_candidates
    } else {
        db.scan_rows(&table.schema.qualified_name(), txn)?
            .into_iter()
            .map(|row| row.row_id)
            .collect()
    };
    candidates.sort_unstable();
    candidates.dedup();
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    // Score each candidate from its payloads, read the heap row once for
    // output + residual filter (scoring no longer needs the heap).
    //
    // Hot-loop micro-tweaks:
    //   - Skip pushing the score pseudo-field when no downstream reader
    //     will ever consume it (see `mark_fulltext_score_needs`).
    //   - When column-pruning nailed the row down to a projected subset,
    //     use `read_row_columns_many_with_handle` — it only decodes the
    //     kept columns from the heap page instead of the full record.
    //     For a `WHERE MATCH(body) … ORDER BY id LIMIT 20` query on a
    //     narrow `(id INT, body TEXT)` table this drops the per-row
    //     read cost by ~half.
    let table_name = table.schema.name.clone();
    let table_alias = table.alias.clone();
    let score_columns: Vec<String> = runtimes
        .iter()
        .map(|runtime| format!("{FT_SCORE_PREFIX}{:016x}", runtime.fingerprint))
        .collect();
    let score_count = runtimes.iter().filter(|runtime| runtime.need_score).count();
    let mut out: Vec<PlanRow> = Vec::with_capacity(candidates.len());
    let mut eval_ctx = EvalQueryContext::default();

    // Fast bulk path: projected read via cached heap handle.
    //
    // `read_row_columns_many_with_handle` is the workhorse used by
    // non-covering INLJ / range scans; it takes a slice of row_ids and a
    // projected column list, opens one heap handle for the batch, and
    // returns `(row_id, [projected values])` for every visible row. For a
    // large-df NL / boolean query this cuts the per-row heap decode down
    // to the columns the outer plan actually needs, and reuses the cached
    // heap handle instead of re-locating the partition on every read.
    if let Some(projected) = projected_columns {
        if !table.schema.is_partitioned() {
            let heap = db.cached_heap_handle(table.schema.id)?;
            let read_rows = db.read_row_columns_many_with_handle(
                &heap,
                &table.schema,
                &candidates,
                txn,
                projected.as_ref(),
            )?;
            let metas = cached_column_metas(table);
            for (row_id, mut values) in read_rows {
                let mut fields: Vec<PlanField> = Vec::with_capacity(projected.len() + score_count);
                for (offset, &idx) in projected.iter().enumerate() {
                    let meta = metas.get(idx).cloned().ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "projected column index out of bounds",
                        )
                    })?;
                    let value = values
                        .get_mut(offset)
                        .map(|slot| std::mem::replace(slot, Value::Null))
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "projected row has too few values",
                            )
                        })?;
                    fields.push(PlanField { meta, value });
                }
                let mut row = PlanRow {
                    fields,
                    group: None,
                    agg_values: None,
                    schema: PlanRow::empty_schema(),
                };
                for (runtime, score_column) in runtimes.iter().zip(score_columns.iter()) {
                    if runtime.need_score {
                        row.push_field(PlanField::new(
                            table_name.clone(),
                            table_alias.clone(),
                            score_column.clone(),
                            ColumnType::Double,
                            Value::Double(fulltext_runtime_score(runtime, row_id)),
                        ));
                    }
                }
                let keep = match filter {
                    Some(expr) => eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)?,
                    None => true,
                };
                if keep {
                    out.push(row);
                }
            }
            return Ok(out);
        }
    }

    for row_id in candidates {
        let Some(stored) = db.read_row(&table.schema, row_id, txn)? else {
            continue;
        };
        let mut row = match projected_columns {
            Some(cols) => stored_row_to_plan_row_projected(table, stored.values, cols)?,
            None => stored_row_to_plan_row(table, stored.values)?,
        };
        for (runtime, score_column) in runtimes.iter().zip(score_columns.iter()) {
            if runtime.need_score {
                row.push_field(PlanField::new(
                    table_name.clone(),
                    table_alias.clone(),
                    score_column.clone(),
                    ColumnType::Double,
                    Value::Double(fulltext_runtime_score(runtime, row_id)),
                ));
            }
        }
        let keep = match filter {
            Some(expr) => eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)?,
            None => true,
        };
        if keep {
            out.push(row);
        }
    }
    Ok(out)
}

/// Execute a `SpatialScan`: query the R-tree (built from the SPATIAL index's
/// durable MBR B-tree) for every row whose stored MBR intersects the search
/// window, read each candidate with MVCC visibility, then apply the residual
/// `filter` (the full WHERE, including the exact spatial predicate) so the
/// R-tree's superset is narrowed to the precise answer.
pub(super) fn execute_spatial_scan(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    query_probe: &planner::SpatialProbe,
    filter: &Option<Expr>,
) -> Result<Vec<PlanRow>, QueryError> {
    let query_mbr = match query_probe {
        planner::SpatialProbe::Constant(mbr) => *mbr,
        planner::SpatialProbe::Parameter(expr) => {
            let value = crate::engine::expr::eval_expr_legacy(expr, &table.schema, &[])?;
            let Value::Geometry(bytes) = value else {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_GIS_INVALID_DATA,
                    "Invalid GIS data provided to the spatial index probe.",
                ));
            };
            let geometry = crate::engine::geo::read_geometry(&bytes)?;
            let column_srid = index
                .columns
                .first()
                .and_then(|column| table.schema.column_index(column).ok())
                .and_then(|column| match table.schema.columns[column].data_type {
                    crate::storage::DataTypeKind::Geometry { srid, .. } => srid,
                    _ => None,
                });
            if let Some(column_srid) = column_srid {
                if geometry.srid != column_srid {
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_GIS_DIFFERENT_SRIDS,
                        format!(
                            "Spatial index probe has SRID {}, but indexed column requires SRID {}.",
                            geometry.srid, column_srid
                        ),
                    ));
                }
            }
            let mbr = geometry.geom.mbr().ok_or_else(|| {
                crate::storage::error(
                    crate::error::catalog::ER_GIS_INVALID_DATA,
                    "Empty geometry cannot be used as a spatial index probe.",
                )
            })?;
            [mbr.minx, mbr.miny, mbr.maxx, mbr.maxy]
        }
    };
    let window =
        crate::engine::geo::Mbr::new(query_mbr[0], query_mbr[1], query_mbr[2], query_mbr[3]);
    let candidates = db.spatial_search_visible(&table.schema, index, window, txn)?;
    let mut out: Vec<PlanRow> = Vec::with_capacity(candidates.len());
    let mut eval_ctx = EvalQueryContext::default();
    for row_id in candidates {
        let Some(stored) = db.read_row(&table.schema, row_id, txn)? else {
            continue;
        };
        let row = stored_row_to_plan_row(table, stored.values)?;
        let keep = match filter {
            Some(expr) => eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)?,
            None => true,
        };
        if keep {
            out.push(row);
        }
    }
    Ok(out)
}

/// Execute a `VectorIndexScan`: ask the HNSW graph for the nearest candidate
/// `RowId`s, read each under MVCC visibility, re-rank by the **live heap
/// vector** (so an UPDATE/DELETE that left a stale graph node only costs recall,
/// never correctness), apply the residual `filter`, then return the `k` nearest
/// rows ascending by distance. The graph is a candidate generator, never the
/// source of truth — matching the spatial R-tree model.
///
/// M5.2 — iterative scan. When MVCC visibility / the residual filter drops
/// candidates below `k`, the search widens `ef` (×2 each iteration) until
/// either `k` rows survive, the graph is exhausted, or we hit the
/// session-configured `mydb_hnsw_max_scan_tuples` cap. The widening policy is
/// chosen by `mydb_hnsw_iterative_scan` (off / relaxed_order / strict_order).
///
/// M5.5 — partitioned tables fan out across `table.storage_ids()` and merge
/// per-partition candidate lists before re-ranking. Each partition keeps its
/// own per-storage HNSW graph (RowIds are only unique within one partition,
/// so a single mixed graph would alias rows across partitions).
/// How many graph candidates to re-rank with an exact heap `read_row`, given
/// the final `k` and the exploration breadth `want` (= ef_search). Re-ranking
/// every explored candidate was the dominant fixed cost of a KNN query (one
/// random heap read each); the true top-k almost always sits within the
/// approximate top `k·mult`, so we cap reads there. `MYDB_HNSW_RERANK_MULT`
/// (default 4) tunes the safety margin; floored at 32 and never above `want`.
/// IVF scans (`nprobe`) and exact-rerank pool size are independent controls.
/// Reusing the HNSW helper capped the pool by nprobe, yielding only
/// `max(k,nprobe)` candidates instead of the documented `k * multiplier`.
pub(super) fn ivf_rerank_fetch(k: usize, mult: u64) -> usize {
    k.saturating_mul(mult.max(1) as usize).max(k)
}

pub(in crate::engine::executor::select) fn resolve_knn_query_vector(
    table: &PlanTable,
    column_index: usize,
    baked: &crate::vector::VectorValue,
    expr: &Option<Expr>,
) -> Result<crate::vector::VectorValue, QueryError> {
    let binary = matches!(
        table.schema.columns[column_index].data_type,
        crate::storage::DataTypeKind::Vector {
            datatype: crate::vector::VectorDataType::Binary,
            ..
        }
    );
    let bytes = if let Some(expr) = expr {
        let value = crate::engine::expr::eval_expr_legacy(expr, &table.schema, &[])?;
        (if binary && !matches!(value, Value::Vector(_)) {
            crate::engine::vector::codec::coerce_value(
                value,
                crate::vector::VectorDataType::Binary,
                None,
            )
            .ok()
        } else {
            crate::planner::value_to_vector_bytes(&value)
        })
        .ok_or_else(|| {
            error(
                crate::error::catalog::MYDB_ER_INVALID_VECTOR_VALUE,
                "query vector parameter is not a valid VECTOR value",
            )
        })?
    } else {
        baked.clone()
    };
    if let Some(crate::storage::schema::DataTypeKind::Vector { dim: Some(dim), .. }) =
        table.schema.columns.get(column_index).map(|c| &c.data_type)
    {
        let actual = bytes.dimension as usize;
        if actual != *dim as usize {
            return Err(error(
                crate::error::catalog::MYDB_ER_VECTOR_DIMENSION_MISMATCH,
                format!(
                    "query vector has {actual} dimensions but column '{}' is VECTOR({dim})",
                    table.schema.columns[column_index].name
                ),
            ));
        }
    }
    Ok(bytes)
}

pub(in crate::engine::executor::select) fn validate_knn_query_vector(
    bytes: &crate::vector::VectorValue,
    metric: crate::engine::vector::Metric,
) -> Result<(), QueryError> {
    let binary = bytes.datatype == crate::vector::VectorDataType::Binary;
    if binary
        != matches!(
            metric,
            crate::engine::vector::Metric::Hamming | crate::engine::vector::Metric::Jaccard
        )
    {
        return Err(crate::engine::vector::vector_err(
            "query datatype and distance metric are incompatible",
        ));
    }
    if metric == crate::engine::vector::Metric::Cosine
        && crate::engine::vector::distance::typed_is_zero(bytes)
    {
        return Err(error(
            crate::error::catalog::MYDB_ER_INVALID_VECTOR_VALUE,
            "zero query vector is not valid for cosine KNN search",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_vector_index_scan(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    query_vector: &crate::vector::VectorValue,
    metric: crate::engine::vector::Metric,
    k: usize,
    ef_search: usize,
    filter: &Option<Expr>,
    order_by: &[OrderBy],
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::engine::vector;
    use crate::engine::vector::session::IterativeScan;
    if k == 0 {
        return Ok(Vec::new());
    }
    let query = query_vector;
    let col_idx = table.schema.column_index(&index.columns[0])?;
    // A recovered/sealed graph contains physical current vectors. Prepared
    // writers may instead expose older vectors (or deleted rows) through undo;
    // those rows need not occur in the ANN candidate pool at all.
    if db.xa_status_counters().0 > 0 || db.vector_snapshot_requires_exact(txn) {
        if order_by.len() > 1 {
            return execute_ordered_vector_fallback(db, txn, table, filter, order_by, k);
        }
        return match filter {
            Some(residual) => execute_vector_brute_scan_filtered(
                db,
                txn,
                table,
                col_idx,
                query_vector,
                metric,
                k,
                residual,
            ),
            None => execute_vector_brute_scan(db, txn, table, col_idx, query_vector, metric, k),
        };
    }
    let hnsw_cfg = vector::session::current_config();
    // Algorithm-specific search semantics. For HNSW the `effort` argument is
    // an `ef` candidate-pool size, which must be at least `k`. For IVF it
    // is `nprobe` — a *cell count* that is independent of the result size; the
    // rerank fetch (`vector_rerank_fetch` below) still guarantees ≥ `k`
    // candidates, so inflating nprobe by `k` (the old `max(k)`) only
    // over-probes and silently defeats the `NPROBE=` / `mydb_ivf_nprobe`
    // tuning knob for large-`k` queries.
    let is_ivf = matches!(index.vector_algo(), crate::storage::schema::VectorAlgo::Ivf);
    let quantized = index
        .vector
        .as_ref()
        .is_some_and(|v| v.quantization.enabled());
    let rerank_mult = vector::session::effective_rerank_mult(&hnsw_cfg, index);
    let rerank_fetch = |want: usize| {
        want.min(k.saturating_mul(rerank_mult as usize).max(32))
            .max(k)
    };

    // Whether the backend's candidate ordering is *exact*. HNSW re-ranks on
    // full-f32 node vectors, and IVF's Dot lists store f32 — for those, the
    // first `k` visible candidates (already in exact-distance order) ARE the
    // answer, so the no-filter early-exit below is sound. IVF L2/Cosine orders
    // by *approximate* SQ8 distance, so a candidate ranked beyond `k` by SQ8
    // can be closer in exact f32; we must exact-score the whole rerank pool
    // before truncating, or recall silently drops to "SQ8 top-k".
    // The concrete candidate generator reports whether its returned distances
    // are authoritative. Ordinary IVF L2/Cosine is approximate SQ8, while the
    // governed IVF graph navigator and both HNSW backends finish on f32 and are
    // exact within their ANN candidate pool.
    let mut exact_order = order_by.len() == 1;
    let base_effort = if is_ivf {
        ef_search.max(1)
    } else {
        ef_search.max(k)
    };
    let iterative_scan = if is_ivf {
        hnsw_cfg.ivf_iterative_scan
    } else {
        hnsw_cfg.iterative_scan
    };
    let configured_max_scan = if is_ivf {
        hnsw_cfg.ivf_max_scan_tuples
    } else {
        hnsw_cfg.max_scan_tuples
    };
    // Iterative-scan upper bound. Zero disables the cap; otherwise we never
    // grow beyond it (we still cap-clamp on the first iteration so
    // hint-driven `ef >> max_scan_tuples` stays honest).
    let max_scan = if configured_max_scan == 0 {
        usize::MAX
    } else {
        (configured_max_scan as usize).max(k)
    };
    let mut eval_ctx = EvalQueryContext::default();
    let mut scored: Vec<(f64, PlanRow)> = Vec::new();
    let mut effort = if is_ivf {
        base_effort
    } else {
        base_effort.min(max_scan)
    };
    let mut candidate_want = if is_ivf {
        ivf_rerank_fetch(k, rerank_mult).max(if quantized { 32 } else { k })
    } else {
        rerank_fetch(effort)
    }
    .min(max_scan)
    .max(k);
    // For partitioned tables we issue one search per partition storage_id
    // and union the candidates before MVCC + filter re-rank. The graph map
    // is keyed by (table_id, storage_id, index_name) — see
    // `Database::vector_index_search_for_partition`.
    let storage_ids: Vec<u64> = if table.schema.is_partitioned() {
        // Honour any active partition prune set (e.g. a partition predicate
        // on the WHERE clause already routed by the pruning hook).
        match crate::engine::executor::partition_router::current_pruned_partitions() {
            Some(ids) if !ids.is_empty() => ids,
            _ => table.schema.storage_ids(),
        }
    } else {
        vec![table.schema.id]
    };

    // Separate the HNSW exact re-rank pool from the number of returned rows.
    // `rerank_limit` below preserves the original scoring work for both the
    // slab and paged backends before returning the nearest candidates.
    // For an unfiltered read, returning k per partition is therefore enough
    // for the global top-k. Keep exploration unchanged, and retain the wider
    // initial pool when widening is disabled or transaction-local vectors
    // require reordering by their heap values. Invisible rows are handled by
    // the existing candidate-limit widening loop below.
    if !is_ivf
        && !quantized
        && order_by.len() == 1
        && filter.is_none()
        && !matches!(iterative_scan, IterativeScan::Off)
        && storage_ids.iter().all(|sid| {
            db.pending_vector_upsert_row_ids(txn.txn_id, table.schema.id, *sid)
                .is_empty()
        })
    {
        candidate_want = k;
    }

    let partition_budget = max_scan / storage_ids.len().max(1);

    // Candidates already heap-read and filtered, keyed by partition-qualified
    // row identity. Widening re-runs the graph search with a larger pool but
    // never re-reads or re-evaluates a row an earlier pass already judged, so
    // `examined` counts distinct heap reads against `max_scan_tuples`.
    let mut seen: std::collections::HashSet<(u64, RowId)> = std::collections::HashSet::new();
    let mut examined = 0usize;
    let exact_fallback = loop {
        if !is_ivf && candidate_want.saturating_mul(storage_ids.len()) > max_scan {
            break true;
        }
        // Exchange candidates from every partition graph. The graph *explores*
        // `want` (= ef_search) breadth for recall, but we only re-rank the top
        // `rerank_want` of those by graph distance — each re-rank is a heap
        // `read_row`, so fetching ef_search of them was the dominant fixed
        // query cost. Widening below grows the pool when too few survive
        // MVCC visibility or the residual filter.
        let mut candidates: Vec<(u64, RowId, f64)> = Vec::new();
        let mut all_backends_exhausted = true;
        let mut pool_saturated = false;
        for sid in &storage_ids {
            let request = vector::VectorSearchRequest {
                candidate_limit: candidate_want,
                rerank_limit: if is_ivf {
                    candidate_want
                } else {
                    rerank_fetch(effort).min(partition_budget).max(k)
                },
                effort: if is_ivf {
                    vector::VectorSearchEffort::Ivf { nprobe: effort }
                } else {
                    vector::VectorSearchEffort::Hnsw {
                        ef: effort.min(partition_budget).max(k),
                    }
                },
            };
            let part_result = match db.vector_index_search_for_partition(
                &table.schema,
                index,
                *sid,
                &query,
                request,
            ) {
                Ok(result) => result,
                Err(index_error) => {
                    // Only a rebuildable storage/index failure, or an index
                    // that cannot be built within the memory budget, may
                    // degrade to the exact heap oracle. Configuration,
                    // dimension and hint errors (for example switching an
                    // existing index between memory and paged backends) must
                    // retain their explicit MySQL error semantics.
                    if !index_error.is(crate::error::catalog::ER_GET_ERRNO)
                        && !index_error.is(crate::error::catalog::ER_OUTOFMEMORY)
                    {
                        return Err(index_error);
                    }
                    if quantized {
                        db.record_vector_query_work(
                            &table.schema,
                            index,
                            *sid,
                            examined as u64,
                            true,
                        );
                    }
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Warning,
                         crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                        format!(
                            "vector index `{}` could not be read; falling back to an exact scan: {}",
                            index.name, index_error.message()
                        ),
                    );
                    if order_by.len() > 1 {
                        return execute_ordered_vector_fallback(
                            db, txn, table, filter, order_by, k,
                        );
                    }
                    if let Some(residual) = filter {
                        return execute_vector_brute_scan_filtered(
                            db,
                            txn,
                            table,
                            col_idx,
                            query_vector,
                            metric,
                            k,
                            residual,
                        );
                    } else {
                        return execute_vector_brute_scan(
                            db,
                            txn,
                            table,
                            col_idx,
                            query_vector,
                            metric,
                            k,
                        );
                    }
                }
            };
            exact_order &= part_result.exact_order;
            pool_saturated |= part_result.candidates.len() >= candidate_want;
            all_backends_exhausted &= part_result.exhausted;
            candidates.extend(
                part_result
                    .candidates
                    .into_iter()
                    .map(|(rid, d)| (*sid, rid, d)),
            );
            let txn_delta = db.pending_vector_upsert_row_ids(txn.txn_id, table.schema.id, *sid);
            if !txn_delta.is_empty() {
                // The graph distance for a transaction-local UPDATE would be
                // stale, so exact-score the complete merged pool below and
                // disable the exact-order early exit for this statement.
                exact_order = false;
                candidates.extend(
                    txn_delta
                        .into_iter()
                        .map(|rid| (*sid, rid, f64::NEG_INFINITY)),
                );
            }
        }
        // Sort the merged pool by graph distance so subsequent dedup keeps
        // the closest copy in stale-graph cases.
        candidates.sort_by(|a, b| a.2.total_cmp(&b.2));

        let mut budget_exhausted = false;
        for (sid, row_id, graph_dist) in candidates {
            if examined >= max_scan {
                budget_exhausted = true;
                break;
            }
            // (storage_id, row_id) is the partition-disambiguated identity.
            // A row judged by an earlier, narrower pass keeps its verdict.
            if !seen.insert((sid, row_id)) {
                continue;
            }
            examined += 1;
            // For partitioned tables, ensure read_row routes to the right
            // partition by installing a single-partition prune set.
            let stored_opt = if table.schema.is_partitioned() {
                crate::engine::executor::partition_router::with_pruned_partitions(
                    vec![sid],
                    || db.read_row(&table.schema, row_id, txn),
                )?
            } else {
                db.read_row(&table.schema, row_id, txn)?
            };
            let Some(stored) = stored_opt else {
                continue; // invisible / deleted under this snapshot
            };
            // Re-rank by the row's *current* heap vector, not the graph copy.
            // Score directly over the stored little-endian bytes — no per-
            // candidate `Vec<f32>` decode.
            let dist = match stored.values.get(col_idx) {
                Some(Value::Vector(bytes)) => {
                    if bytes.dimension != query_vector.dimension {
                        continue; // dimension drift — skip defensively
                    }
                    vector::distance::typed_ordering_distance(query_vector, bytes, metric)?
                }
                _ => continue,
            };
            // A superseded version kept for older snapshots sits at its old
            // graph position; the candidate order no longer matches the heap,
            // so this statement must rank the whole pool exactly.
            if exact_order
                && (dist - graph_dist).abs() > 1e-3 * dist.abs().max(graph_dist.abs()).max(1e-6)
            {
                exact_order = false;
            }
            let row = stored_row_to_plan_row(table, stored.values)?;
            let keep = match filter {
                Some(expr) => eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)?,
                None => true,
            };
            if keep {
                scored.push((dist, row));
                // When the candidate ordering is *exact* (`exact_order`:
                // HNSW's f32 node vectors, or IVF's f32 Dot lists), candidates
                // arrive in exact ascending-distance order, so with no residual
                // filter to drop/reorder survivors the first `k` visible rows
                // ARE the answer — stop before reading (and MVCC-fetching) the
                // rest of the candidate pool. `read_row` dominated the fixed
                // cost of a low-ef KNN query; this cuts it from ~`rerank_want`
                // heap reads to ~`k`. For IVF L2/Cosine the pool order is only
                // SQ8-approximate, so we must exact-score the whole pool (no
                // early break) and let the final sort recover the true top-k.
                // With a filter we must keep scanning regardless (dropped rows
                // need replacements — the iterative widening below handles it).
                if filter.is_none() && exact_order && scored.len() >= k {
                    break;
                }
            }
        }
        if budget_exhausted {
            break true;
        }
        if scored.len() >= k || matches!(iterative_scan, IterativeScan::Off) {
            break false;
        }
        // Every backend proved it holds no further candidates: a short
        // result is the true answer.
        if all_backends_exhausted && !pool_saturated {
            // An ANN frontier can exhaust a disconnected component, or contain
            // multiple versions of the same row. It cannot prove heap exhaustion.
            break !is_ivf;
        }
        // Rows exist beyond what the ANN budget may inspect. Returning a
        // short LIMIT would silently drop matching rows, so finish exactly.
        if examined >= max_scan {
            break true;
        }
        // Once observed filtering would require random heap reads for a
        // substantial part of the relation, a bounded streaming TopK is cheaper.
        // A smoothed survival estimate avoids treating one empty pool as proof
        // that a selective predicate has no matches.
        if !is_ivf && filter.is_some() && examined >= 64 {
            let population = crate::planner::table_row_estimate(db, table)?;
            let survival = (scored.len() as f64 + 2.0) / (examined as f64 + 16.0);
            if population > 0.0 && k as f64 / survival > population * 0.25 {
                break true;
            }
        }
        if is_ivf {
            // First retain more candidates from the already-selected cells;
            // only after that pool is exhausted widen nprobe.
            if pool_saturated && candidate_want < max_scan {
                candidate_want = candidate_want.saturating_mul(2).max(k).min(max_scan);
                continue;
            }
            if all_backends_exhausted {
                break true;
            }
            effort = effort.saturating_mul(2).max(1);
        } else {
            // HNSW returns its whole `ef` pool while widening, so pool and
            // exploration grow together. A pool beyond `max_scan` could only
            // hold candidates the budget forbids reading: finish exactly.
            if candidate_want >= max_scan {
                break true;
            }
            let next = effort.max(candidate_want).saturating_mul(2).min(max_scan);
            effort = next;
            candidate_want = next.max(k);
        }
    };
    if quantized {
        if let Some(&sid) = storage_ids.first() {
            db.record_vector_query_work(
                &table.schema,
                index,
                sid,
                examined as u64,
                exact_fallback || db.vector_snapshot_requires_exact(txn),
            );
        }
    }
    if exact_fallback || db.vector_snapshot_requires_exact(txn) {
        if order_by.len() > 1 {
            return execute_ordered_vector_fallback(db, txn, table, filter, order_by, k);
        }
        return match filter {
            Some(residual) => execute_vector_brute_scan_filtered(
                db,
                txn,
                table,
                col_idx,
                query_vector,
                metric,
                k,
                residual,
            ),
            None => execute_vector_brute_scan(db, txn, table, col_idx, query_vector, metric, k),
        };
    }

    if order_by.len() > 1 {
        let rows = scored.into_iter().map(|(_, row)| row).collect();
        let mut rows = super::super::external_sort::sort_plan_rows_for_plan(
            db.data_dir(),
            rows,
            order_by,
            None,
        )?;
        rows.truncate(k);
        return Ok(rows);
    }
    // strict_order — keep ascending distance order (default). relaxed_order
    // skips the final sort and just truncates: callers willing to trade
    // ordering for latency get the raw insertion order.
    if !matches!(iterative_scan, IterativeScan::RelaxedOrder) {
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));
    }
    scored.truncate(k);
    Ok(scored.into_iter().map(|(_, row)| row).collect())
}

/// Exact completion for a distance order with SQL tie-break columns.
fn execute_ordered_vector_fallback(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: &Option<Expr>,
    order_by: &[OrderBy],
    k: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    let rows = table.schema.stats.row_count as f64;
    let input = PhysicalPlan::SeqScan {
        table: table.clone(),
        filter: filter.clone(),
        estimated_rows: rows,
        access_rows: rows,
        cost: crate::engine::cbo::Cost::scalar(rows),
        possible_keys: Vec::new(),
        runtime_filters: Vec::new(),
        hint_note: None,
        projected_columns: None,
    };
    super::execute_topk(db, txn, &input, order_by, k)
}

/// Rare failure-path oracle used when an ANN file cannot be read or fails
/// structural validation and the plan also carries a residual predicate.
/// Scanning the heap is deliberately exact: storage/I/O damage must never be
/// reported as a normal approximate-index miss.
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_vector_brute_scan_filtered(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    col_idx: usize,
    query_vector: &crate::vector::VectorValue,
    metric: crate::engine::vector::Metric,
    k: usize,
    filter: &Expr,
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
    use crate::engine::vector::hnsw_core::Scored;
    if k == 0 {
        return Ok(Vec::new());
    }
    let mut eval_ctx = EvalQueryContext::default();
    let mut admitted =
        ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, 0)?;
    let mut best: std::collections::BinaryHeap<Scored<usize>> = std::collections::BinaryHeap::new();
    let mut retained: Vec<Option<PlanRow>> = Vec::new();
    let mut row_bytes: Vec<usize> = Vec::new();
    db.scan_rows_ordered_streaming(&table.schema.qualified_name(), txn, |values| {
        crate::engine::cancel::check_cancelled()?;
        let Some(Value::Vector(bytes)) = values.get(col_idx) else {
            return Ok(());
        };
        if bytes.dimension != query_vector.dimension {
            return Ok(());
        }
        let row = stored_row_to_plan_row(table, values.to_vec())?;
        if !eval_plan_bool_with_db(db, txn, filter, &row, &mut eval_ctx)? {
            return Ok(());
        }
        let distance =
            crate::engine::vector::distance::typed_ordering_distance(query_vector, bytes, metric)?;
        let distance = if distance.is_nan() {
            f64::NEG_INFINITY
        } else {
            distance
        };
        if best.len() < k {
            let bytes = super::nested_loop::materialized_plan_row_charge(&row);
            admitted.try_grow(bytes)?;
            let slot = retained.len();
            retained.push(Some(row));
            row_bytes.push(bytes);
            best.push(Scored {
                dist: distance,
                id: slot,
            });
        } else if best.peek().is_some_and(|worst| distance < worst.dist) {
            let mut worst = best.peek_mut().expect("nonempty bounded top-k");
            let slot = worst.id;
            let bytes = super::nested_loop::materialized_plan_row_charge(&row);
            admitted.try_grow(bytes.saturating_sub(row_bytes[slot]))?;
            admitted.shrink(row_bytes[slot].saturating_sub(bytes));
            row_bytes[slot] = bytes;
            retained[slot] = Some(row);
            worst.dist = distance;
        }
        Ok(())
    })?;
    Ok(best
        .into_sorted_vec()
        .into_iter()
        .map(|score| retained[score.id].take().expect("retained top-k row"))
        .collect())
}

/// Brute-force **exact** KNN over a `VECTOR(n)` column with no HNSW index
/// (the `PhysicalPlan::VectorBruteScan` node). The heavy lifting —
/// zero-copy in-place distance scoring off the heap payload, a bounded
/// top-`k` of `(distance, row_id)`, and the parallel page-range fan-out —
/// lives in [`Database::vector_brute_knn`]. Here we just re-materialise the
/// K winners (in ascending-distance order) into `PlanRow`s; the downstream
/// Projection recomputes the displayed `DISTANCE(...)` value, exactly as for
/// `VectorIndexScan`, and `provided_order_prefix_len` has already suppressed
/// the redundant Sort.
///
/// The planner only emits this node for unpartitioned tables (RowIds are
/// unique only within a partition), so a plain `read_row` correctly locates
/// each winner.
pub(super) fn execute_vector_brute_scan(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    col_idx: usize,
    query_vector: &crate::vector::VectorValue,
    metric: crate::engine::vector::Metric,
    k: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    if k == 0 {
        return Ok(Vec::new());
    }
    let winners = db.vector_brute_knn(&table.schema, txn, col_idx, query_vector, metric, k)?;
    let partitioned = table.schema.is_partitioned();
    let mut rows = Vec::with_capacity(winners.len());
    for (_dist, sid, row_id) in winners {
        // Same snapshot as the scan, so a winner is still visible; defensively
        // skip a row that somehow vanished rather than emitting a stub. For a
        // partitioned table, route the read to the winner's partition (RowIds
        // are only unique within one partition).
        let stored_opt = if partitioned {
            crate::engine::executor::partition_router::with_pruned_partitions(vec![sid], || {
                db.read_row(&table.schema, row_id, txn)
            })?
        } else {
            db.read_row(&table.schema, row_id, txn)?
        };
        if let Some(stored) = stored_opt {
            rows.push(stored_row_to_plan_row(table, stored.values)?);
        }
    }
    Ok(rows)
}

pub(in crate::engine::executor::select) fn stored_row_to_plan_row(
    table: &PlanTable,
    values: Vec<Value>,
) -> Result<PlanRow, QueryError> {
    let metas = cached_column_metas(table);
    let fields = logical_visible_column_indices(&table.schema)
        .into_iter()
        .map(|idx| {
            let meta = metas.get(idx).cloned().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "stored row metadata is incomplete",
                )
            })?;
            let value = values.get(idx).cloned().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "stored row has too few values",
                )
            })?;
            Ok(PlanField { meta, value })
        })
        .collect::<Result<Vec<_>, QueryError>>()?;
    Ok(PlanRow {
        fields,
        group: None,
        agg_values: None,
        schema: table
            .row_schema
            .get_or_init(|| {
                let visible_metas = logical_visible_column_indices(&table.schema)
                    .into_iter()
                    .filter_map(|index| metas.get(index).cloned())
                    .collect::<Vec<_>>();
                RowSchema::from_metas(&visible_metas)
            })
            .clone()
            .into(),
    })
}

/// Base-schema indices in SQL-visible logical order. Heap codecs and index
/// entries continue to address columns by their stable physical/schema slot;
/// a `PlanRow`, however, is a semantic row and must hide INSTANT-DROP
/// tombstones and reflect FIRST/AFTER ordering for wildcard projection.
pub(in crate::engine::executor::select) fn logical_visible_column_indices(
    schema: &TableSchema,
) -> Vec<usize> {
    schema.iter_visible_columns().map(|(idx, _)| idx).collect()
}

/// Like [`stored_row_to_plan_row`] but keeps only the columns whose base
/// index appears in `projection`. Consumed `values` are still the full
/// row (heap reads decode the whole record); this variant just skips the
/// `PlanField { meta, value }` construction — and, crucially, avoids
/// cloning large payload columns (TEXT/JSON/BYTES) into a `PlanRow` field
/// only to be dropped by an upstream projection. Used by the FT executor
/// when the outer plan doesn't reference the FT indexed columns for
/// downstream evaluation.
///
/// `projection` need not be sorted; column identity is by base index.
pub(in crate::engine::executor::select) fn stored_row_to_plan_row_projected(
    table: &PlanTable,
    values: Vec<Value>,
    projection: &[usize],
) -> Result<PlanRow, QueryError> {
    let metas = cached_column_metas(table);
    let mut values = values;
    // Emit fields in the base-column order they were requested. This
    // preserves the natural ordering used by other scan paths (e.g.
    // `projected_values_to_plan_row` operates on already-projected
    // values from covering scans).
    let mut fields: Vec<PlanField> = Vec::with_capacity(projection.len());
    for &idx in projection {
        let meta = metas.get(idx).cloned().ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "projected column index out of bounds",
            )
        })?;
        // `Value` isn't `Default`, so swap in a Null sentinel instead of
        // `mem::take` to move the value out without cloning.
        let slot = values.get_mut(idx).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "stored row has too few values",
            )
        })?;
        let value = std::mem::replace(slot, Value::Null);
        fields.push(PlanField { meta, value });
    }
    Ok(PlanRow {
        fields,
        group: None,
        agg_values: None,
        schema: PlanRow::empty_schema(),
    })
}

pub(super) fn projected_values_to_plan_row(
    table: &PlanTable,
    projection: &[usize],
    values: Vec<Value>,
) -> Result<PlanRow, QueryError> {
    let metas = cached_column_metas(table);
    let mut values = values.into_iter();
    let fields = projection
        .iter()
        .map(|&idx| {
            let meta = metas.get(idx).cloned().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "projected column index out of bounds",
                )
            })?;
            let value = values.next().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "projected row has too few values",
                )
            })?;
            Ok(PlanField { meta, value })
        })
        .collect::<Result<Vec<_>, QueryError>>()?;
    Ok(PlanRow {
        fields,
        group: None,
        agg_values: None,
        schema: PlanRow::empty_schema(),
    })
}

pub(super) fn null_extend_right(left: &PlanRow, right_plan: &PhysicalPlan) -> PlanRow {
    null_extend_plan::<false>(left, right_plan, None)
}

pub(super) fn null_extend_right_table(
    left: &PlanRow,
    right_table: &PlanTable,
    projection: Option<&[usize]>,
) -> PlanRow {
    let right_len = projection
        .map(|columns| columns.len())
        .unwrap_or(right_table.schema.columns.len());
    let mut fields = Vec::with_capacity(left.fields.len() + right_len);
    fields.extend(left.fields.iter().cloned());
    if let Some(projection) = projection {
        for &idx in projection {
            if let Some(column) = right_table.schema.columns.get(idx) {
                fields.push(PlanField {
                    meta: intern_col_meta(ColMeta {
                        invisible: column.invisible,
                        table: right_table.schema.name.clone(),
                        alias: right_table.alias.clone(),
                        column: column.name.clone(),
                        column_type: right_table.schema.result_type(idx),
                        source: Default::default(),
                    }),
                    value: Value::Null,
                });
            }
        }
    } else {
        for (idx, column) in right_table.schema.columns.iter().enumerate() {
            fields.push(PlanField {
                meta: intern_col_meta(ColMeta {
                    invisible: column.invisible,
                    table: right_table.schema.name.clone(),
                    alias: right_table.alias.clone(),
                    column: column.name.clone(),
                    column_type: right_table.schema.result_type(idx),
                    source: Default::default(),
                }),
                value: Value::Null,
            });
        }
    }
    PlanRow {
        fields,
        group: None,
        agg_values: None,
        schema: PlanRow::empty_schema(),
    }
}

pub(super) fn null_extend_left(right: &PlanRow, left_plan: &PhysicalPlan) -> PlanRow {
    null_extend_plan::<true>(right, left_plan, None)
}

/// Produce only the requested columns, keeping the plan's NULL-side width.
pub(super) fn null_extend_right_projected(
    left: &PlanRow,
    right_plan: &PhysicalPlan,
    projection: &[usize],
) -> PlanRow {
    null_extend_plan::<false>(left, right_plan, Some(projection))
}

pub(super) fn null_extend_left_projected(
    right: &PlanRow,
    left_plan: &PhysicalPlan,
    projection: &[usize],
) -> PlanRow {
    null_extend_plan::<true>(right, left_plan, Some(projection))
}

/// The side is a compile-time policy, preserving static row-layout paths.
#[inline]
fn null_extend_plan<const NULL_LEFT: bool>(
    row: &PlanRow,
    null_plan: &PhysicalPlan,
    projection: Option<&[usize]>,
) -> PlanRow {
    let mut null_cols = Vec::new();
    collect_plan_fields(null_plan, &mut null_cols);
    let left_len = if NULL_LEFT {
        null_cols.len()
    } else {
        row.fields.len()
    };
    let mut fields =
        Vec::with_capacity(projection.map_or(row.fields.len() + null_cols.len(), |p| p.len()));
    let null_field = |meta: ColMeta| PlanField {
        meta: intern_col_meta(meta),
        value: Value::Null,
    };
    if let Some(projection) = projection {
        for &i in projection {
            let null_side = if NULL_LEFT {
                i < left_len
            } else {
                i >= left_len
            };
            let position = if i < left_len { i } else { i - left_len };
            if null_side {
                if let Some(meta) = null_cols.get(position) {
                    fields.push(null_field(meta.clone()));
                }
            } else if let Some(field) = row.fields.get(position) {
                fields.push(field.clone());
            }
        }
    } else if NULL_LEFT {
        fields.extend(null_cols.into_iter().map(null_field));
        fields.extend(row.fields.iter().cloned());
    } else {
        fields.extend(row.fields.iter().cloned());
        fields.extend(null_cols.into_iter().map(null_field));
    }
    PlanRow {
        fields,
        group: None,
        agg_values: None,
        schema: PlanRow::empty_schema(),
    }
}
