//! scan execution and preparation.

use super::*;

/// A `BatchOperator` that reads directly from storage into columnar
/// `Batch`es, bypassing the `PlanRow` intermediate representation.
/// Uses schema-aware `ColumnAccumulator`s to push raw data from
/// `&Value` references directly into typed buffers — zero
/// `Value::clone`, zero type-inference pass.
///
/// Construction is metadata-only. `next_batch` scans one physical-page
/// morsel and exposes completed batches immediately.
#[derive(Clone)]
pub(super) struct StorageBatchStream {
    pub(super) db: Arc<Database>,
    pub(super) txn: TxnView,
    pub(super) table: crate::planner::PlanTable,
    pub(super) schema_cols: Vec<usize>,
    pub(super) tasks:
        std::collections::VecDeque<crate::engine::executor::select::parallel::ScanTask>,
    pub(super) morsel_pages: u64,
    pub(super) pending: std::collections::VecDeque<Batch>,
    pub(super) visited_storage_ids: std::collections::HashSet<u64>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) direct_scratch: crate::storage::DirectScanScratch,
}

pub(super) const VEC_SCAN_MORSEL_PAGES: u64 = 16;
// Parallel workers already own disjoint contiguous ranges and publish into a
// bounded queue. A wider internal morsel amortizes storage callback setup
// without returning to whole-range materialisation: at 16 KiB pages the raw
// input covered by one worker call remains bounded to 4 MiB.
pub(super) const VEC_PARALLEL_SCAN_MORSEL_PAGES: u64 = 256;

thread_local! {
    static SCAN_MORSEL_PAGES: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    static SCAN_BATCH_ROWS: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

pub(super) fn with_scan_limit_hint<T>(row_target: Option<usize>, f: impl FnOnce() -> T) -> T {
    SCAN_MORSEL_PAGES.with(|cell| {
        let previous = cell.replace(row_target.map(|_| 1));
        struct Restore<'a> {
            cell: &'a std::cell::Cell<Option<u64>>,
            previous: Option<u64>,
        }
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                self.cell.set(self.previous);
            }
        }
        let _restore = Restore { cell, previous };
        SCAN_BATCH_ROWS.with(|rows| {
            let previous_rows = rows.replace(row_target);
            struct RestoreRows<'a> {
                cell: &'a std::cell::Cell<Option<usize>>,
                previous: Option<usize>,
            }
            impl Drop for RestoreRows<'_> {
                fn drop(&mut self) {
                    self.cell.set(self.previous);
                }
            }
            let _restore_rows = RestoreRows {
                cell: rows,
                previous: previous_rows,
            };
            f()
        })
    })
}

/// Leave room for queued batches, one in-flight morsel per worker and the
/// downstream blocking work area. This changes batch sizing, not concurrency.
pub(super) fn scan_worker_buffer_bytes() -> Option<usize> {
    mem_budget::current_budget()
        .map(|budget| (budget.limit() / scheduler_hash_join_workers().max(1) / 8).max(32 * 1024))
}

pub(super) fn scan_decode_batch_rows(schema: &BatchSchema) -> usize {
    let requested = scan_batch_rows();
    scan_worker_buffer_bytes()
        .map(|bytes| {
            (bytes / schema.fields().len().max(1) / std::mem::size_of::<Value>())
                .clamp(1, requested)
        })
        .unwrap_or(requested)
}

pub(super) fn bounded_scan_morsel_pages(requested: u64) -> u64 {
    scan_worker_buffer_bytes()
        .map(|bytes| {
            // Typed decoding, variable-width offsets and completion buffers may
            // coexist with the encoded page. Bound the whole morsel, including
            // batches not yet published into the exchange queue.
            requested.min((bytes / crate::storage::PAGE_SIZE / 4).max(1) as u64)
        })
        .unwrap_or(requested)
        .max(1)
}

pub(super) fn scan_morsel_pages(parallel: bool) -> u64 {
    SCAN_MORSEL_PAGES
        .with(std::cell::Cell::get)
        .unwrap_or(if parallel {
            VEC_PARALLEL_SCAN_MORSEL_PAGES
        } else {
            VEC_SCAN_MORSEL_PAGES
        })
        .max(1)
}

/// Pop at most one bounded scan morsel while preserving the original task's
/// physical order. Parallel planning deliberately assigns one large,
/// contiguous range to each worker for sequential I/O. A batch source must
/// not consume that whole range in one `next_batch` call, however: both
/// storage scan callbacks run to the end of the supplied range, so doing so
/// retains every completed `Batch` for the worker's table partition before
/// bounded exchange backpressure can take effect.
pub(super) fn pop_bounded_scan_task(
    tasks: &mut std::collections::VecDeque<crate::engine::executor::select::parallel::ScanTask>,
    page_limit: u64,
) -> Option<crate::engine::executor::select::parallel::ScanTask> {
    let mut task = tasks.pop_front()?;
    let page_limit = page_limit.max(1);
    let page_hi = task.page_lo.saturating_add(page_limit).min(task.page_hi);
    let morsel = crate::engine::executor::select::parallel::ScanTask {
        storage_id: task.storage_id,
        page_lo: task.page_lo,
        page_hi,
    };
    if page_hi < task.page_hi {
        task.page_lo = page_hi;
        tasks.push_front(task);
    }
    Some(morsel)
}

pub(super) fn build_batch_scan_tasks(
    db: &Arc<Database>,
    table: &crate::planner::PlanTable,
    filter: Option<&Expr>,
    use_zone_map: bool,
    parallel_degree: Option<u32>,
) -> Result<Vec<crate::engine::executor::select::parallel::ScanTask>, QueryError> {
    if let Some(degree) = parallel_degree.filter(|degree| *degree > 1) {
        return Ok(crate::engine::executor::select::parallel::scan_morsels(
            db,
            table,
            filter,
            use_zone_map,
            degree,
        )?
        .0);
    }
    let morsel_pages = scan_morsel_pages(false);
    Ok(
        crate::engine::executor::select::parallel::scan_tasks(db, table, filter, use_zone_map)?
            .into_iter()
            .flat_map(|task| {
                (task.page_lo..task.page_hi)
                    .step_by(morsel_pages as usize)
                    .map(
                        move |page_lo| crate::engine::executor::select::parallel::ScanTask {
                            storage_id: task.storage_id,
                            page_lo,
                            page_hi: (page_lo + morsel_pages).min(task.page_hi),
                        },
                    )
            })
            .collect(),
    )
}

pub(crate) fn scan_batch_rows() -> usize {
    SCAN_BATCH_ROWS
        .with(std::cell::Cell::get)
        .unwrap_or(DEFAULT_BATCH_ROWS)
        .clamp(1, DEFAULT_BATCH_ROWS)
}

pub(crate) fn scan_row_goal() -> Option<usize> {
    SCAN_BATCH_ROWS.with(std::cell::Cell::get)
}

pub(super) fn scan_projection_columns(
    table: &crate::planner::PlanTable,
    projected_columns: Option<&[usize]>,
) -> Vec<usize> {
    match projected_columns {
        Some(columns) => columns.to_vec(),
        None => table
            .schema
            .iter_visible_columns()
            .map(|(index, _)| index)
            .collect(),
    }
}

pub(super) fn build_scan_batch_schema(
    table: &crate::planner::PlanTable,
    columns: &[usize],
) -> Arc<BatchSchema> {
    Arc::new(BatchSchema::new(
        columns
            .iter()
            .map(|&index| BatchField {
                table: table.schema.name.clone(),
                alias: table.alias.clone(),
                name: table.schema.columns[index].name.clone(),
                column_type: table.schema.result_type(index),
                source: table
                    .schema
                    .wire_column_source_with_alias(index, table.alias.as_deref()),
            })
            .collect(),
    ))
}

impl StorageBatchStream {
    pub(super) fn new(
        db: &Arc<Database>,
        txn: TxnView,
        table: &crate::planner::PlanTable,
        projected_columns: Option<&[usize]>,
        tasks: Vec<crate::engine::executor::select::parallel::ScanTask>,
        morsel_pages: u64,
    ) -> Result<Self, QueryError> {
        let schema_cols = scan_projection_columns(table, projected_columns);
        let schema = build_scan_batch_schema(table, &schema_cols);
        Ok(Self {
            db: Arc::clone(db),
            txn,
            table: table.clone(),
            schema_cols,
            tasks: tasks.into(),
            morsel_pages: morsel_pages.max(1),
            pending: std::collections::VecDeque::new(),
            visited_storage_ids: std::collections::HashSet::new(),
            direct_scratch: crate::storage::DirectScanScratch::default(),
            schema,
        })
    }
}

impl BatchOperator for StorageBatchStream {
    fn sample_scan_pages(&mut self, max_pages: usize) -> bool {
        sample_scan_tasks(&mut self.tasks, max_pages);
        true
    }

    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let batch_rows = scan_decode_batch_rows(&self.schema);
        while self.pending.is_empty() {
            crate::engine::cancel::check_cancelled()?;
            let Some(task) = pop_bounded_scan_task(
                &mut self.tasks,
                bounded_scan_morsel_pages(self.morsel_pages),
            ) else {
                return Ok(None);
            };
            if self.table.schema.is_partitioned()
                && self.visited_storage_ids.insert(task.storage_id)
            {
                crate::engine::executor::partition_router::record_partition_visit();
            }
            let mut output = ScanBatchBuilder::new(self.schema.clone(), batch_rows);
            let mut fallback_values = Vec::with_capacity(self.schema_cols.len());
            let mut consume = |hit: crate::storage::PayloadStreamHit<'_>| {
                match hit {
                    crate::storage::PayloadStreamHit::Payload(payload) => {
                        let fast = crate::storage::decode_row_into_sink_fast(
                            payload.payload,
                            payload.codec,
                            payload.layout_id,
                            payload.plan,
                            &mut crate::engine::executor::vec::batch::BatchAccumSink::new(
                                &mut output.columns,
                            ),
                            payload.has_external,
                        )?;
                        if !fast {
                            fallback_values.clear();
                            crate::storage::decode_row_schema_columns_into_with_plan_ext_for_layout(
                                payload.payload,
                                payload.codec,
                                payload.layout_id,
                                payload.plan,
                                &mut fallback_values,
                                payload.has_external,
                            )?;
                            for (accumulator, value) in
                                output.columns.iter_mut().zip(&fallback_values)
                            {
                                accumulator.push(value);
                            }
                        }
                    }
                    crate::storage::PayloadStreamHit::Values(values) => {
                        for (accumulator, value) in output.columns.iter_mut().zip(values) {
                            accumulator.push(value);
                        }
                    }
                }
                output.finish_row()?;
                Ok(())
            };
            self.db.scan_rows_payloads_storage_streaming_range(
                &self.table.schema,
                task.storage_id,
                self.txn,
                &self.schema_cols,
                task.page_lo,
                task.page_hi,
                &mut self.direct_scratch,
                &mut consume,
            )?;
            self.pending.extend(output.finish()?);
        }
        Ok(self.pending.pop_front())
    }
}

/// Build a PlanField template directly from table schema + projection,
/// without needing to probe a first row.
pub(super) fn build_scan_template(
    table: &crate::planner::PlanTable,
    projected_columns: Option<&[usize]>,
) -> Vec<PlanField> {
    let cols = scan_projection_columns(table, projected_columns);
    cols.iter()
        .map(|&i| PlanField {
            meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta::from_table(
                table, i,
            )),
            value: Value::Null,
        })
        .collect()
}

/// A `BatchOperator` that reads directly from storage with a predicate-
/// first scan: filter columns are decoded first, the compiled
/// predicate is evaluated, and only matching rows have the remaining
/// output columns decoded and pushed into typed accumulators.
#[derive(Clone)]
pub(super) struct FilteredStorageBatchStream {
    pub(super) db: Arc<Database>,
    pub(super) txn: TxnView,
    pub(super) table: crate::planner::PlanTable,
    pub(super) predicate_columns: Vec<usize>,
    pub(super) all_cols: Vec<usize>,
    // `None` is a runtime-filter-only predicate-first source.  Dynamic join
    // filtering must not depend on whether the unrelated SQL predicate is in
    // CompiledPredicate's smaller capability set; an upper FilterBatchStream
    // can evaluate that predicate after this source has discarded non-member
    // join keys.
    pub(super) compiled: Option<crate::engine::executor::select::plan::CompiledPredicate>,
    pub(super) decoded: Option<crate::engine::executor::select::plan::DecodedPredicateProgram>,
    pub(super) runtime_probes:
        Vec<Option<crate::engine::executor::select::plan::CompiledRuntimeFilterProbe>>,
    pub(super) tasks:
        std::collections::VecDeque<crate::engine::executor::select::parallel::ScanTask>,
    pub(super) morsel_pages: u64,
    pub(super) pending: std::collections::VecDeque<Batch>,
    pub(super) visited_storage_ids: std::collections::HashSet<u64>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) direct_scratch: crate::storage::DirectScanScratch,
}

#[cfg(test)]
pub(crate) static RUNTIME_RANGE_ZONE_PRUNE_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Minimal sink for an inclusive TIMESTAMP range.  It lets the row codec feed
/// the stored microsecond lane straight into two integer comparisons without
/// constructing `PredicateValue`s or walking a boolean expression tree for
/// every candidate row.
pub(super) struct TimestampRangeDecodeSink {
    pub(super) column: usize,
    pub(super) low: i64,
    pub(super) high: i64,
    pub(super) result: Option<bool>,
    pub(super) unsupported: bool,
}

impl TimestampRangeDecodeSink {
    pub(super) fn new(column: usize, low: i64, high: i64) -> Self {
        Self {
            column,
            low,
            high,
            result: None,
            unsupported: false,
        }
    }

    pub(super) fn finish(self) -> Option<bool> {
        (!self.unsupported).then_some(self.result.unwrap_or(false))
    }

    pub(super) fn reject_other(&mut self) {
        self.unsupported = true;
    }
}

impl crate::storage::RowDecodeSink for TimestampRangeDecodeSink {
    fn put_null(&mut self, col: usize) {
        if col == self.column {
            self.result = Some(false);
        } else {
            self.reject_other();
        }
    }
    fn put_timestamp(&mut self, col: usize, micros: i64) {
        if col == self.column {
            self.result = Some(micros >= self.low && micros <= self.high);
        } else {
            self.reject_other();
        }
    }

    reject_decode_lanes!(reject_other; put_i64, put_u64, put_f32, put_f64, put_decimal_compact, put_decimal_wide, put_date, put_year, put_time, put_datetime, put_bytes, put_string, put_json, put_geometry, put_vector, put_value);
}

impl FilteredStorageBatchStream {
    pub(super) fn new(
        db: &Arc<Database>,
        txn: TxnView,
        table: &crate::planner::PlanTable,
        filter: &Expr,
        projected_columns: Option<&[usize]>,
        tasks: &[crate::engine::executor::select::parallel::ScanTask],
        morsel_pages: u64,
    ) -> Result<Option<Self>, QueryError> {
        let all_cols = scan_projection_columns(table, projected_columns);
        let mut predicate_set = std::collections::BTreeSet::new();
        if !crate::engine::executor::select::plan::collect_table_column_refs(
            filter,
            &table.schema,
            &mut predicate_set,
        ) {
            return Ok(None);
        }
        let predicate_columns: Vec<usize> = predicate_set.iter().copied().collect();
        let predicate_schema = crate::engine::executor::select::plan::projected_schema_view(
            &table.schema,
            &predicate_columns,
        );
        let compiled =
            match crate::engine::executor::select::plan::CompiledPredicate::compile_for_storage(
                filter,
                &predicate_schema,
            ) {
                Some(c) => c,
                None => return Ok(None),
            };
        let decoded = crate::engine::executor::select::plan::DecodedPredicateProgram::compile(
            filter,
            &predicate_schema,
        );
        let schema = build_scan_batch_schema(table, &all_cols);
        Ok(Some(Self {
            db: Arc::clone(db),
            txn,
            table: table.clone(),
            predicate_columns,
            all_cols,
            compiled: Some(compiled),
            decoded,
            runtime_probes: Vec::new(),
            tasks: tasks.iter().copied().collect(),
            morsel_pages: morsel_pages.max(1),
            pending: std::collections::VecDeque::new(),
            visited_storage_ids: std::collections::HashSet::new(),
            direct_scratch: crate::storage::DirectScanScratch::default(),
            schema,
        }))
    }

    /// Build an unopened predicate-first alternative that initially has no
    /// local SQL predicate. A parent HashJoin may append its key columns and
    /// select this source before the first batch is read. `scan_filter` is
    /// still supplied to morsel planning so zone-map/partition pruning stays
    /// identical to the ordinary source; it is evaluated by the common batch
    /// expression operator above this source.
    pub(super) fn runtime_filter_candidate(
        db: &Arc<Database>,
        txn: TxnView,
        table: &crate::planner::PlanTable,
        projected_columns: Option<&[usize]>,
        tasks: &[crate::engine::executor::select::parallel::ScanTask],
        morsel_pages: u64,
    ) -> Result<Self, QueryError> {
        let all_cols = scan_projection_columns(table, projected_columns);
        let schema = build_scan_batch_schema(table, &all_cols);
        Ok(Self {
            db: Arc::clone(db),
            txn,
            table: table.clone(),
            predicate_columns: Vec::new(),
            all_cols,
            compiled: None,
            decoded: None,
            runtime_probes: Vec::new(),
            tasks: tasks.iter().copied().collect(),
            morsel_pages: morsel_pages.max(1),
            pending: std::collections::VecDeque::new(),
            visited_storage_ids: std::collections::HashSet::new(),
            direct_scratch: crate::storage::DirectScanScratch::default(),
            schema,
        })
    }
}

impl BatchOperator for FilteredStorageBatchStream {
    fn sample_scan_pages(&mut self, max_pages: usize) -> bool {
        sample_scan_tasks(&mut self.tasks, max_pages);
        true
    }

    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        let Some(columns) = crate::engine::executor::select::plan::runtime_filter_schema_indices(
            &filter,
            &self.table,
        ) else {
            return false;
        };
        let single_column = (columns.len() == 1).then(|| columns[0]);
        // Preserve the existing predicate program's positional layout and
        // append only missing dynamic-key columns. The storage callback then
        // decodes SQL-predicate and runtime-filter keys in one predicate-first
        // pass before any payload column is materialized.
        for column in columns {
            if !self.predicate_columns.contains(&column) {
                self.predicate_columns.push(column);
            }
        }
        let Some(compiled) =
            crate::engine::executor::select::plan::compile_runtime_filter_probe_projected(
                &filter,
                &self.table.schema,
                &self.predicate_columns,
            )
        else {
            return false;
        };
        self.runtime_probes.push(Some(compiled));
        if let (
            Some(column),
            crate::planner::RuntimeFilter::ExactIntKey {
                keys,
                null_safe,
                filter,
                ..
            },
        ) = (single_column, &filter)
        {
            // An exact integer domain also provides a conservative storage
            // range. Keep the row membership probe and parent join unchanged;
            // missing/stale/disabled zone metadata simply declines pruning.
            if !self.txn.dirty_read
                && self.table.system_time.is_none()
                && !self.table.schema.is_partitioned()
                && null_safe.iter().all(|safe| !*safe)
                && keys.len() == 1
                && matches!(keys[0], Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                && !self.table.schema.columns[column].unsigned
                && matches!(
                    self.table.schema.columns[column].data_type,
                    crate::storage::DataTypeKind::TinyInt
                        | crate::storage::DataTypeKind::SmallInt
                        | crate::storage::DataTypeKind::MediumInt
                        | crate::storage::DataTypeKind::Int
                        | crate::storage::DataTypeKind::BigInt
                )
            {
                if let Some((low, high)) = filter
                    .bounds()
                    .and_then(|(lo, hi)| Some((i64::try_from(lo).ok()?, i64::try_from(hi).ok()?)))
                {
                    let predicate = Expr::Between {
                        expr: Box::new(Expr::Identifier(
                            self.table.schema.columns[column].name.clone(),
                        )),
                        low: Box::new(Expr::BoundValue(Value::Int(low))),
                        high: Box::new(Expr::BoundValue(Value::Int(high))),
                        negated: false,
                    };
                    if let Some(ranges) = self
                        .db
                        .zone_map_candidate_ranges(&self.table.schema, &predicate)
                    {
                        #[cfg(test)]
                        let before = self
                            .tasks
                            .iter()
                            .map(|task| task.page_hi - task.page_lo)
                            .sum::<u64>();
                        self.tasks.retain_mut(|task| {
                            let first = ranges.partition_point(|(_, hi)| *hi <= task.page_lo);
                            let end = ranges.partition_point(|(lo, _)| *lo < task.page_hi);
                            if first >= end {
                                return false;
                            }
                            // Enclose surviving zones without splitting tasks,
                            // so a sparse map cannot grow the task allocation.
                            task.page_lo = task.page_lo.max(ranges[first].0);
                            task.page_hi = task.page_hi.min(ranges[end - 1].1);
                            task.page_lo < task.page_hi
                        });
                        #[cfg(test)]
                        if self
                            .tasks
                            .iter()
                            .map(|task| task.page_hi - task.page_lo)
                            .sum::<u64>()
                            < before
                        {
                            RUNTIME_RANGE_ZONE_PRUNE_HITS
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }
        }
        true
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let batch_rows = scan_decode_batch_rows(&self.schema);
        while self.pending.is_empty() {
            crate::engine::cancel::check_cancelled()?;
            let Some(task) = pop_bounded_scan_task(
                &mut self.tasks,
                bounded_scan_morsel_pages(self.morsel_pages),
            ) else {
                return Ok(None);
            };
            if self.table.schema.is_partitioned()
                && self.visited_storage_ids.insert(task.storage_id)
            {
                crate::engine::executor::partition_router::record_partition_visit();
            }
            let mut output = ScanBatchBuilder::new(self.schema.clone(), batch_rows);
            let mut evaluated_rows = 0u64;
            let mut runtime_filter_rejections = vec![0u64; self.runtime_probes.len()];
            let mut missing_columns = Vec::new();
            let mut missing_output_slots = Vec::new();
            let mut predicate_output_slots = Vec::new();
            for (output_slot, output_column) in self.all_cols.iter().copied().enumerate() {
                if let Some(predicate_slot) = self
                    .predicate_columns
                    .iter()
                    .position(|column| *column == output_column)
                {
                    predicate_output_slots.push((output_slot, predicate_slot));
                } else {
                    missing_columns.push(output_column);
                    missing_output_slots.push(output_slot);
                }
            }
            // Runtime probes already require typed scratch, so reusing even
            // one overlapping output avoids a decode without changing the
            // predicate representation. SQL-only predicates keep the wider
            // overlap gate before replacing their compact decoded program.
            let reuse_predicate_output = !predicate_output_slots.is_empty()
                && (predicate_output_slots.len() >= missing_columns.len()
                    || !self.runtime_probes.is_empty())
                && (self.decoded.is_none() || !self.runtime_probes.is_empty())
                && self
                    .compiled
                    .as_ref()
                    .map_or(true, |compiled| compiled.supports_typed_eval());
            let payload_output_columns = if reuse_predicate_output {
                missing_columns.as_slice()
            } else {
                self.all_cols.as_slice()
            };
            let payload_output_slots = reuse_predicate_output.then_some(missing_output_slots);
            let timestamp_range = (self.runtime_probes.is_empty()
                && self.predicate_columns.len() == 1)
                .then(|| {
                    self.compiled
                        .as_ref()
                        .and_then(crate::engine::executor::select::plan::CompiledPredicate::inclusive_timestamp_range)
                })
                .flatten();
            {
                let mut decoded_results = self
                    .decoded
                    .as_ref()
                    .map(|decoded| vec![None; decoded.leaf_count()])
                    .unwrap_or_default();
                let typed_values = std::cell::RefCell::new(vec![
                    crate::engine::executor::vec::scan_bridge::PredicateValue::Null;
                    self.predicate_columns.len()
                ]);
                let predicate_fallback =
                    std::cell::RefCell::new(Vec::with_capacity(self.predicate_columns.len()));
                let predicate_cache_typed = std::cell::Cell::new(false);
                let mut output_fallback = Vec::with_capacity(self.all_cols.len());
                let mut on_predicate =
                    |hit: crate::storage::PayloadStreamHit<'_>| -> Result<bool, QueryError> {
                        evaluated_rows = evaluated_rows.saturating_add(1);
                        predicate_cache_typed.set(false);
                        match hit {
                            crate::storage::PayloadStreamHit::Payload(payload) => {
                                if let Some((column, low, high)) = timestamp_range {
                                    let mut sink = TimestampRangeDecodeSink::new(column, low, high);
                                    let fast = crate::storage::decode_row_into_sink_fast(
                                        payload.payload,
                                        payload.codec,
                                        payload.layout_id,
                                        payload.plan,
                                        &mut sink,
                                        payload.has_external,
                                    )?;
                                    if fast {
                                        if let Some(keep) = sink.finish() {
                                            return Ok(keep);
                                        }
                                    }
                                }
                                if !reuse_predicate_output && self.runtime_probes.is_empty() {
                                    if let Some(decoded) = &self.decoded {
                                        let mut sink = crate::engine::executor::select::parallel::DecodedPredicateSink::new(
                                            decoded,
                                            &mut decoded_results,
                                        );
                                        let fast = crate::storage::decode_row_into_sink_fast(
                                            payload.payload,
                                            payload.codec,
                                            payload.layout_id,
                                            payload.plan,
                                            &mut sink,
                                            payload.has_external,
                                        )?;
                                        if fast {
                                            if let Some(keep) = sink.finish() {
                                                return Ok(keep);
                                            }
                                        }
                                    }
                                }
                                if self.compiled.as_ref().map_or(
                                    true,
                                    crate::engine::executor::select::plan::CompiledPredicate::supports_typed_eval,
                                ) {
                                    let mut typed_values = typed_values.borrow_mut();
                                    let fast = crate::storage::decode_row_into_sink_fast(
                                        payload.payload,
                                        payload.codec,
                                        payload.layout_id,
                                        payload.plan,
                                        &mut crate::engine::executor::vec::scan_bridge::PredicateValueSink::new(
                                            typed_values.as_mut_slice(),
                                        ),
                                        payload.has_external,
                                    )?;
                                    if fast {
                                        let keep = match &self.compiled {
                                            Some(compiled) => compiled.eval_typed(&typed_values)?,
                                            None => Some(true),
                                        };
                                        if let Some(keep) = keep {
                                            predicate_cache_typed.set(true);
                                            return Ok(keep
                                                && crate::engine::executor::select::plan::runtime_filters_admit_predicate_values_indexed_deferred(
                                                    &self.runtime_probes,
                                                    &typed_values,
                                                    &mut runtime_filter_rejections,
                                                ));
                                        }
                                    }
                                }
                                let mut predicate_fallback = predicate_fallback.borrow_mut();
                                predicate_fallback.clear();
                                crate::storage::decode_row_schema_columns_into_with_plan_ext_for_layout(
                                    payload.payload,
                                    payload.codec,
                                    payload.layout_id,
                                    payload.plan,
                                    &mut predicate_fallback,
                                    payload.has_external,
                                )?;
                                Ok(self.compiled.as_ref().map_or(Ok(true), |compiled| {
                                    compiled.eval(&predicate_fallback)
                                })?
                                    && crate::engine::executor::select::plan::runtime_filters_admit_values_indexed_deferred(
                                        &self.runtime_probes,
                                        &predicate_fallback,
                                        &mut runtime_filter_rejections,
                                    ))
                            }
                            crate::storage::PayloadStreamHit::Values(values) => {
                                let mut predicate_fallback = predicate_fallback.borrow_mut();
                                predicate_fallback.clear();
                                predicate_fallback.extend_from_slice(values);
                                Ok(self
                                    .compiled
                                    .as_ref()
                                    .map_or(Ok(true), |compiled| compiled.eval(values))?
                                    && crate::engine::executor::select::plan::runtime_filters_admit_values_indexed_deferred(
                                        &self.runtime_probes,
                                        values,
                                        &mut runtime_filter_rejections,
                                    ))
                            }
                        }
                    };
                let mut on_output =
                    |hit: crate::storage::PayloadStreamHit<'_>| -> Result<(), QueryError> {
                        match hit {
                            crate::storage::PayloadStreamHit::Payload(payload) => {
                                if reuse_predicate_output {
                                    if predicate_cache_typed.get() {
                                        let typed_values = typed_values.borrow();
                                        for &(output_slot, predicate_slot) in
                                            &predicate_output_slots
                                        {
                                            typed_values[predicate_slot]
                                                .push_to(&mut output.columns[output_slot]);
                                        }
                                    } else {
                                        let predicate_fallback = predicate_fallback.borrow();
                                        for &(output_slot, predicate_slot) in
                                            &predicate_output_slots
                                        {
                                            output.columns[output_slot]
                                                .push(&predicate_fallback[predicate_slot]);
                                        }
                                    }
                                }
                                let fast = match payload_output_slots.as_deref() {
                                    Some(output_slots) => crate::storage::decode_row_into_sink_fast(
                                        payload.payload,
                                        payload.codec,
                                        payload.layout_id,
                                        payload.plan,
                                        &mut crate::engine::executor::vec::batch::BatchAccumSink::mapped(
                                            &mut output.columns,
                                            output_slots,
                                        ),
                                        payload.has_external,
                                    )?,
                                    None => crate::storage::decode_row_into_sink_fast(
                                        payload.payload,
                                        payload.codec,
                                        payload.layout_id,
                                        payload.plan,
                                        &mut crate::engine::executor::vec::batch::BatchAccumSink::new(
                                            &mut output.columns,
                                        ),
                                        payload.has_external,
                                    )?,
                                };
                                if !fast {
                                    output_fallback.clear();
                                    crate::storage::decode_row_schema_columns_into_with_plan_ext_for_layout(
                                        payload.payload,
                                        payload.codec,
                                        payload.layout_id,
                                        payload.plan,
                                        &mut output_fallback,
                                        payload.has_external,
                                    )?;
                                    match payload_output_slots.as_deref() {
                                        Some(output_slots) => {
                                            for (&output_slot, value) in
                                                output_slots.iter().zip(&output_fallback)
                                            {
                                                output.columns[output_slot].push(value);
                                            }
                                        }
                                        None => {
                                            for (accumulator, value) in
                                                output.columns.iter_mut().zip(&output_fallback)
                                            {
                                                accumulator.push(value);
                                            }
                                        }
                                    }
                                }
                            }
                            crate::storage::PayloadStreamHit::Values(values) => {
                                for (accumulator, value) in output.columns.iter_mut().zip(values) {
                                    accumulator.push(value);
                                }
                            }
                        }
                        output.finish_row()?;
                        Ok(())
                    };
                self.db
                    .scan_rows_payloads_predicate_first_storage_streaming_range(
                        &self.table.schema,
                        task.storage_id,
                        self.txn,
                        &self.predicate_columns,
                        &self.all_cols,
                        payload_output_columns,
                        task.page_lo,
                        task.page_hi,
                        &mut self.direct_scratch,
                        &mut on_predicate,
                        &mut on_output,
                    )?;
            }
            crate::engine::executor::select::plan::flush_runtime_filter_rejections(
                &self.runtime_probes,
                &mut runtime_filter_rejections,
            );
            analyze_stats::add_pending_expression_rows(evaluated_rows, 0);
            self.pending.extend(output.finish()?);
        }
        Ok(self.pending.pop_front())
    }
}

/// A scan with a locally compiled SQL predicate keeps both physical layouts
/// unopened until its parent pipeline is complete. The CBO-selected plain
/// layout remains the default; if a HashJoin later publishes a runtime filter,
/// the source atomically selects the already-lowered predicate-first layout so
/// the SQL predicate and build membership are evaluated in one decode pass.
/// No input is consumed and no plan topology changes during this selection.
pub(super) struct AdaptiveFilteredStorageBatchStream {
    pub(super) plain: StorageBatchStream,
    pub(super) filtered: Option<FilteredStorageBatchStream>,
    pub(super) use_filtered: bool,
    pub(super) opened: bool,
}

impl BatchOperator for AdaptiveFilteredStorageBatchStream {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.plain.schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        if self.opened {
            return false;
        }
        let Some(filtered) = self.filtered.as_mut() else {
            return false;
        };
        if !filtered.install_runtime_filter(filter) {
            return false;
        }
        self.use_filtered = true;
        true
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.opened = true;
        if self.use_filtered {
            self.filtered
                .as_mut()
                .expect("runtime-filtered scan selected")
                .next_batch()
        } else {
            self.plain.next_batch()
        }
    }
}

pub(super) enum ParallelStorageWorker {
    Plain(StorageBatchStream),
    Filtered(FilteredStorageBatchStream),
}

pub(super) struct AggregateStorageWorker {
    pub(super) source: ParallelStorageWorker,
    pub(super) _charge: Arc<mem_budget::ControlledMemoryCharge>,
    pub(super) context: crate::engine::executor::select::execution_context::ExecutionContext,
    pub(super) work: Arc<AggregateScanWork>,
    pub(super) worker: usize,
}

pub(super) struct AggregateScanWork {
    pub(super) queues: Vec<
        parking_lot::Mutex<
            std::collections::VecDeque<crate::engine::executor::select::parallel::ScanTask>,
        >,
    >,
    pub(super) morsel_pages: u64,
    pub(super) _charge: Arc<mem_budget::ControlledMemoryCharge>,
}

impl AggregateScanWork {
    pub(super) fn next(
        &self,
        worker: usize,
    ) -> Option<crate::engine::executor::select::parallel::ScanTask> {
        // Preserve each scan's original locality until its assigned range is
        // exhausted, then help a peer. Taking a bounded page range under its
        // queue lock gives every page exactly one owner without adding workers.
        for offset in 0..self.queues.len() {
            let mut queue = self.queues[(worker + offset) % self.queues.len()].lock();
            if let Some(task) = pop_bounded_scan_task(&mut queue, self.morsel_pages) {
                return Some(task);
            }
        }
        None
    }
}

impl crate::engine::executor::vec::stream::ParallelBatchWorker for AggregateStorageWorker {
    fn run(
        mut self: Box<Self>,
        consume: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
        abort: &std::sync::atomic::AtomicBool,
    ) -> Result<(), QueryError> {
        let db = self.source.db();
        self.context.run(&db, || loop {
            self.context.cancellation().check()?;
            if abort.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(
                    crate::error::catalog::ER_QUERY_INTERRUPTED.error("parallel aggregate aborted")
                );
            }
            if let Some(batch) = self.source.next_batch()? {
                consume(batch)?;
            } else if let Some(task) = self.work.next(self.worker) {
                match &mut self.source {
                    ParallelStorageWorker::Plain(source) => source.tasks.push_back(task),
                    ParallelStorageWorker::Filtered(source) => source.tasks.push_back(task),
                }
            } else {
                return Ok(());
            }
        })
    }
}

impl ParallelStorageWorker {
    pub(super) fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        match self {
            Self::Plain(stream) => stream.next_batch(),
            Self::Filtered(stream) => stream.next_batch(),
        }
    }

    pub(super) fn db(&self) -> Arc<Database> {
        match self {
            Self::Plain(stream) => Arc::clone(&stream.db),
            Self::Filtered(stream) => Arc::clone(&stream.db),
        }
    }
}

/// Native parallel scan source. Planning fixes the degree; opening partitions
/// the immutable page-morsel list exactly once and every worker inherits the
/// same ExecutionContext. Runtime pressure may backpressure the bounded queue
/// but cannot change scan topology or duplicate a morsel.
pub(super) struct ParallelStorageBatchStream {
    pub(super) schema: Arc<BatchSchema>,
    pub(super) properties: BatchProperties,
    pub(super) execution: ScanWorkers,
    pub(super) pending: Option<ParallelStoragePending>,
}

pub(super) enum ParallelStoragePending {
    Plain {
        source: StorageBatchStream,
        filtered: Option<FilteredStorageBatchStream>,
        degree: usize,
    },
    Filtered(FilteredStorageBatchStream, usize),
}

impl ParallelStorageBatchStream {
    pub(super) fn from_plain(source: StorageBatchStream, degree: usize) -> Self {
        Self::pending(ParallelStoragePending::Plain {
            source,
            filtered: None,
            degree,
        })
    }

    pub(super) fn from_plain_with_filtered(
        source: StorageBatchStream,
        filtered: FilteredStorageBatchStream,
        degree: usize,
    ) -> Self {
        Self::pending(ParallelStoragePending::Plain {
            source,
            filtered: Some(filtered),
            degree,
        })
    }

    pub(super) fn start_plain(
        mut source: StorageBatchStream,
        degree: usize,
        context: crate::engine::executor::select::execution_context::ExecutionContext,
    ) -> Self {
        let tasks = std::mem::take(&mut source.tasks);
        let workers = split_parallel_scan_workers(
            tasks,
            degree,
            |worker_tasks| {
                let mut worker = source.clone();
                worker.tasks = worker_tasks;
                worker.pending.clear();
                worker.visited_storage_ids.clear();
                ParallelStorageWorker::Plain(worker)
            },
            Arc::clone(&source.schema),
            context,
        );
        workers
    }

    pub(super) fn from_filtered(source: FilteredStorageBatchStream, degree: usize) -> Self {
        Self::pending(ParallelStoragePending::Filtered(source, degree))
    }

    pub(super) fn start_filtered(
        mut source: FilteredStorageBatchStream,
        degree: usize,
        context: crate::engine::executor::select::execution_context::ExecutionContext,
    ) -> Self {
        let tasks = std::mem::take(&mut source.tasks);
        split_parallel_scan_workers(
            tasks,
            degree,
            |worker_tasks| {
                let mut worker = source.clone();
                worker.tasks = worker_tasks;
                worker.pending.clear();
                worker.visited_storage_ids.clear();
                ParallelStorageWorker::Filtered(worker)
            },
            Arc::clone(&source.schema),
            context,
        )
    }

    pub(super) fn pending(pending: ParallelStoragePending) -> Self {
        let schema = match &pending {
            ParallelStoragePending::Plain { source, .. } => source.schema.clone(),
            ParallelStoragePending::Filtered(source, _) => source.schema.clone(),
        };
        let degree = match &pending {
            ParallelStoragePending::Plain { degree, .. }
            | ParallelStoragePending::Filtered(_, degree) => *degree,
        };
        let txn = match &pending {
            ParallelStoragePending::Plain { source, .. } => source.txn,
            ParallelStoragePending::Filtered(source, _) => source.txn,
        };
        Self {
            schema,
            properties: BatchProperties {
                ordering: BatchOrdering::Unordered,
                partitioning: BatchPartitioning::RoundRobin { partitions: degree },
            },
            execution: ScanWorkers::new(
                crate::engine::executor::select::execution_context::ExecutionContext::capture(txn)
                    .fork_worker_cancellation(),
                "parallel batch scan worker panicked",
            ),
            pending: Some(pending),
        }
    }

    pub(super) fn ensure_started(&mut self) -> Result<(), QueryError> {
        if self.pending.is_none() {
            return Ok(());
        }
        // Acquire before consuming pending state so an interrupted open
        // never leaves an apparently started stream without a receiver.
        let pressure = self
            .execution
            .worker_context
            .enter_parallel_read_pressure()?;
        let started = match self.pending.take().expect("pending scan checked") {
            ParallelStoragePending::Plain { source, degree, .. } => {
                Self::start_plain(source, degree, self.execution.worker_context.clone())
            }
            ParallelStoragePending::Filtered(source, degree) => {
                Self::start_filtered(source, degree, self.execution.worker_context.clone())
            }
        };
        // The started stream inherits this cancellation branch. Retire the
        // unopened shell before replacing it so its Drop cannot cancel the
        // newly spawned workers sharing that branch.
        self.execution.finished = true;
        *self = started;
        self.execution.txn_pressure = pressure;
        Ok(())
    }
}

pub(super) fn split_parallel_scan_workers(
    tasks: std::collections::VecDeque<crate::engine::executor::select::parallel::ScanTask>,
    degree: usize,
    mut make_worker: impl FnMut(
        std::collections::VecDeque<crate::engine::executor::select::parallel::ScanTask>,
    ) -> ParallelStorageWorker,
    schema: Arc<BatchSchema>,
    worker_context: crate::engine::executor::select::execution_context::ExecutionContext,
) -> ParallelStorageBatchStream {
    let degree = degree.max(1).min(tasks.len().max(1));
    let mut partitions = vec![std::collections::VecDeque::new(); degree];
    for (index, task) in tasks.into_iter().enumerate() {
        partitions[index % degree].push_back(task);
    }
    let partition_visit_collector =
        crate::engine::executor::partition_router::current_partition_visit_collector();
    let (sender, receiver) = std::sync::mpsc::sync_channel(degree.saturating_mul(2).max(2));
    let mut workers = Vec::with_capacity(degree);
    for tasks in partitions {
        let mut worker = make_worker(tasks);
        let sender = sender.clone();
        let context = worker_context.clone();
        let db = worker.db();
        let visit_collector = partition_visit_collector.clone();
        workers.push(std::thread::spawn(move || {
            crate::engine::executor::partition_router::with_partition_visit_collector(
                visit_collector,
                || {
                    context.run(&db, || loop {
                        if context.cancellation().check().is_err() {
                            break;
                        }
                        match worker.next_batch() {
                            Ok(Some(batch)) => {
                                if !send_parallel_scan_message(&sender, &context, Ok(batch)) {
                                    break;
                                }
                            }
                            Ok(None) => break,
                            Err(error) => {
                                let error = context.terminal_error(error);
                                let _ = send_parallel_scan_message(&sender, &context, Err(error));
                                break;
                            }
                        }
                    });
                },
            );
        }));
    }
    drop(sender);
    ParallelStorageBatchStream {
        schema,
        properties: BatchProperties {
            ordering: BatchOrdering::Unordered,
            partitioning: BatchPartitioning::RoundRobin { partitions: degree },
        },
        execution: ScanWorkers::new(worker_context, "parallel batch scan worker panicked")
            .with_channel(receiver, workers),
        pending: None,
    }
}

pub(super) struct QueuedScanBatch {
    pub(super) batch: Batch,
    pub(super) _charge: mem_budget::ScopedCharge,
}

pub(super) fn send_parallel_scan_message(
    sender: &std::sync::mpsc::SyncSender<Result<QueuedScanBatch, QueryError>>,
    context: &crate::engine::executor::select::execution_context::ExecutionContext,
    message: Result<Batch, QueryError>,
) -> bool {
    let mut message = message.and_then(|batch| {
        let charge = mem_budget::force_charge_scoped(batch.memory_bytes())?;
        Ok(QueuedScanBatch {
            batch,
            _charge: charge,
        })
    });
    loop {
        match sender.try_send(message) {
            Ok(()) => return true,
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => return false,
            Err(std::sync::mpsc::TrySendError::Full(returned)) => {
                message = returned;
                if context.cancellation().check().is_err() {
                    return false;
                }
                // Bounded backpressure without an uninterruptible blocking
                // send: early LIMIT/client close cancels the worker scope and
                // Drop can join promptly even while the queue is full.
                std::thread::park_timeout(std::time::Duration::from_micros(50));
            }
        }
    }
}

impl BatchOperator for ParallelStorageBatchStream {
    fn sample_scan_pages(&mut self, max_pages: usize) -> bool {
        match self.pending.as_mut() {
            Some(ParallelStoragePending::Plain {
                source, filtered, ..
            }) => {
                sample_scan_tasks(&mut source.tasks, max_pages);
                if let Some(filtered) = filtered {
                    sample_scan_tasks(&mut filtered.tasks, max_pages);
                }
                true
            }
            Some(ParallelStoragePending::Filtered(source, _)) => {
                sample_scan_tasks(&mut source.tasks, max_pages);
                true
            }
            None => false,
        }
    }

    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }

    fn is_native_parallel_scan(&self) -> bool {
        self.pending.is_some() && !self.execution.finished
    }

    fn take_parallel_workers(
        &mut self,
        degree: usize,
    ) -> Result<
        Option<Vec<Box<dyn crate::engine::executor::vec::stream::ParallelBatchWorker>>>,
        QueryError,
    > {
        let Some(pending) = self.pending.as_ref() else {
            return Ok(None);
        };
        let planned = match pending {
            ParallelStoragePending::Plain { degree, .. }
            | ParallelStoragePending::Filtered(_, degree) => *degree,
        };
        if degree <= 1 || degree != planned {
            return Ok(None);
        }
        let task_count = match pending {
            ParallelStoragePending::Plain { source, .. } => source.tasks.len(),
            ParallelStoragePending::Filtered(source, _) => source.tasks.len(),
        };
        let worker_slots = degree.min(task_count.max(1));
        // Replaces the scan exchange's worker descriptors and queues. Reserve
        // owned worker boxes, Vec headers and exact deque capacities before
        // allocation; the last source worker releases the shared reservation.
        let metadata_bytes = worker_slots
            .saturating_mul(
                std::mem::size_of::<AggregateStorageWorker>()
                    + std::mem::size_of::<
                        std::collections::VecDeque<
                            crate::engine::executor::select::parallel::ScanTask,
                        >,
                    >()
                    + std::mem::size_of::<
                        parking_lot::Mutex<
                            std::collections::VecDeque<
                                crate::engine::executor::select::parallel::ScanTask,
                            >,
                        >,
                    >()
                    + std::mem::size_of::<crate::engine::executor::select::parallel::ScanTask>()
                    + std::mem::size_of::<
                        Box<dyn crate::engine::executor::vec::stream::ParallelBatchWorker>,
                    >(),
            )
            .saturating_add(task_count.saturating_mul(std::mem::size_of::<
                crate::engine::executor::select::parallel::ScanTask,
            >()))
            .saturating_add(
                std::mem::size_of::<AggregateScanWork>() + 2 * std::mem::size_of::<usize>(),
            );
        let charge = Arc::new(mem_budget::ControlledMemoryCharge::try_new(
            mem_budget::ControlledMemoryCategory::StorageScratch,
            metadata_bytes,
        )?);
        self.execution.txn_pressure = self
            .execution
            .worker_context
            .enter_parallel_read_pressure()?;
        let mut source = match self.pending.take().expect("unopened source checked") {
            ParallelStoragePending::Plain { source, .. } => ParallelStorageWorker::Plain(source),
            ParallelStoragePending::Filtered(source, _) => ParallelStorageWorker::Filtered(source),
        };
        let tasks = match &mut source {
            ParallelStorageWorker::Plain(source) => std::mem::take(&mut source.tasks),
            ParallelStorageWorker::Filtered(source) => std::mem::take(&mut source.tasks),
        };
        let degree = degree.min(tasks.len().max(1));
        let mut partitions: Vec<_> = (0..degree)
            .map(|worker| {
                std::collections::VecDeque::with_capacity(
                    task_count / degree + usize::from(worker < task_count % degree),
                )
            })
            .collect();
        for (index, task) in tasks.into_iter().enumerate() {
            partitions[index % degree].push_back(task);
        }
        let morsel_pages = match &source {
            ParallelStorageWorker::Plain(source) => source.morsel_pages,
            ParallelStorageWorker::Filtered(source) => source.morsel_pages,
        };
        let work = Arc::new(AggregateScanWork {
            queues: partitions
                .into_iter()
                .map(parking_lot::Mutex::new)
                .collect(),
            morsel_pages,
            _charge: charge.clone(),
        });
        let mut result: Vec<Box<dyn crate::engine::executor::vec::stream::ParallelBatchWorker>> =
            Vec::with_capacity(degree);
        for index in 0..degree {
            let worker = match &source {
                ParallelStorageWorker::Plain(source) => {
                    let mut source = source.clone();
                    source.tasks = std::collections::VecDeque::with_capacity(1);
                    ParallelStorageWorker::Plain(source)
                }
                ParallelStorageWorker::Filtered(source) => {
                    let mut source = source.clone();
                    source.tasks = std::collections::VecDeque::with_capacity(1);
                    ParallelStorageWorker::Filtered(source)
                }
            };
            result.push(Box::new(AggregateStorageWorker {
                source: worker,
                _charge: charge.clone(),
                context: self.execution.worker_context.clone(),
                work: work.clone(),
                worker: index,
            }));
        }
        // Ownership (including cancellation) now follows the transferred
        // workers. Dropping the exhausted exchange must not cancel them.
        self.execution.finished = true;
        Ok(Some(result))
    }

    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        let Some(pending) = self.pending.take() else {
            return false;
        };
        match pending {
            ParallelStoragePending::Plain {
                source,
                mut filtered,
                degree,
            } => {
                if let Some(candidate) = filtered.as_mut() {
                    if candidate.install_runtime_filter(filter) {
                        self.pending = Some(ParallelStoragePending::Filtered(
                            filtered.expect("checked candidate"),
                            degree,
                        ));
                        return true;
                    }
                }
                self.pending = Some(ParallelStoragePending::Plain {
                    source,
                    filtered,
                    degree,
                });
                false
            }
            ParallelStoragePending::Filtered(mut source, degree) => {
                let installed = source.install_runtime_filter(filter);
                self.pending = Some(ParallelStoragePending::Filtered(source, degree));
                installed
            }
        }
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.execution.finished {
            return Ok(None);
        }
        self.ensure_started()?;
        self.execution.next_batch()
    }
}

/// Parallel B-tree source for planner-fixed `ParallelIndexFastScan` and
/// `ParallelIndexRangeScan` nodes. Workers own disjoint contiguous leaf
/// ranges and publish bounded batches into the same pull graph used by other
/// native scans. Runtime filters are installed on every unopened worker
/// before I/O.
pub(super) struct ParallelIndexBatchStream {
    pub(super) db: Arc<Database>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) properties: BatchProperties,
    pub(super) pending: Option<Vec<vec_index_scan::IndexBatchStream>>,
    pub(super) execution: ScanWorkers,
}

pub(crate) static PARALLEL_INDEX_BATCH_SCAN_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

impl ParallelIndexBatchStream {
    pub(super) fn new(
        db: &Arc<Database>,
        txn: TxnView,
        workers: Vec<vec_index_scan::IndexBatchStream>,
    ) -> Self {
        let degree = workers.len();
        let schema = workers
            .first()
            .expect("parallel index stream requires workers")
            .schema()
            .clone();
        Self {
            db: Arc::clone(db),
            schema,
            properties: BatchProperties {
                ordering: BatchOrdering::Unordered,
                partitioning: BatchPartitioning::RoundRobin { partitions: degree },
            },
            pending: Some(workers),
            execution: ScanWorkers::new(
                crate::engine::executor::select::execution_context::ExecutionContext::capture(txn)
                    .fork_worker_cancellation(),
                "parallel index batch scan worker panicked",
            ),
        }
    }

    pub(super) fn ensure_started(&mut self) -> Result<(), QueryError> {
        if self.pending.is_none() {
            return Ok(());
        }
        self.execution.txn_pressure = self
            .execution
            .worker_context
            .enter_parallel_read_pressure()?;
        let workers = self.pending.take().expect("pending index scan checked");
        PARALLEL_INDEX_BATCH_SCAN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let degree = workers.len();
        let (sender, receiver) = std::sync::mpsc::sync_channel(degree.saturating_mul(2).max(2));
        let visit_collector =
            crate::engine::executor::partition_router::current_partition_visit_collector();
        let range_plan_key = analyze_stats::current_range_scan_plan_key();
        for mut worker in workers {
            let sender = sender.clone();
            let context = self.execution.worker_context.clone();
            let db = Arc::clone(&self.db);
            let visit_collector = visit_collector.clone();
            self.execution.workers.push(std::thread::spawn(move || {
                analyze_stats::with_range_scan_plan_key_if_present(range_plan_key, || {
                    crate::engine::executor::partition_router::with_partition_visit_collector(
                        visit_collector,
                        || {
                            context.run(&db, || loop {
                                if context.cancellation().check().is_err() {
                                    break;
                                }
                                match worker.next_batch() {
                                    Ok(Some(batch)) => {
                                        if !send_parallel_scan_message(&sender, &context, Ok(batch))
                                        {
                                            break;
                                        }
                                    }
                                    Ok(None) => break,
                                    Err(error) => {
                                        let error = context.terminal_error(error);
                                        let _ = send_parallel_scan_message(
                                            &sender,
                                            &context,
                                            Err(error),
                                        );
                                        break;
                                    }
                                }
                            });
                        },
                    );
                });
            }));
        }
        drop(sender);
        self.execution.receiver = Some(receiver);
        Ok(())
    }
}

impl BatchOperator for ParallelIndexBatchStream {
    fn limit_column_prefixes(&mut self, limits: &[(usize, usize)]) {
        if let Some(workers) = self.pending.as_mut() {
            for worker in workers {
                worker.limit_column_prefixes(limits);
            }
        }
    }
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }

    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        let Some(workers) = self.pending.as_mut() else {
            return false;
        };
        let mut installed = true;
        for worker in workers {
            installed &= worker.install_runtime_filter(filter.clone());
        }
        installed
    }

    fn grow_candidate_batch_rows(&mut self) -> Option<usize> {
        self.pending
            .as_mut()?
            .iter_mut()
            .filter_map(BatchOperator::grow_candidate_batch_rows)
            .max()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.execution.finished {
            return Ok(None);
        }
        self.ensure_started()?;
        self.execution.next_batch()
    }
}

/// Try to build a direct storage→Batch stream for a SeqScan plan.
/// Handles both unfiltered scans and filter-first predicate scans.
/// Returns `Ok(None)` for any shape that requires the legacy row-
/// based path.
pub(super) fn try_build_direct_batch_scan<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let inner = peel_projection(plan);
    let (
        table,
        filter,
        projected_columns,
        runtime_filters,
        estimated_rows,
        access_rows,
        use_zone_map,
        zone_map,
        parallel_degree,
    ) = match inner {
        PhysicalPlan::SeqScan {
            table,
            filter,
            projected_columns,
            runtime_filters,
            estimated_rows,
            access_rows,
            ..
        } => (
            table,
            filter,
            projected_columns,
            runtime_filters,
            *estimated_rows,
            *access_rows,
            false,
            None,
            None,
        ),
        PhysicalPlan::ZoneMapScan {
            table,
            filter,
            projected_columns,
            runtime_filters,
            estimated_rows,
            access_rows,
            zone_map,
            ..
        } => (
            table,
            filter,
            projected_columns,
            runtime_filters,
            *estimated_rows,
            *access_rows,
            true,
            Some(zone_map),
            None,
        ),
        PhysicalPlan::ParallelSeqScan {
            table,
            filter,
            projected_columns,
            runtime_filters,
            degree,
            estimated_rows,
            access_rows,
            ..
        } => (
            table,
            filter,
            projected_columns,
            runtime_filters,
            *estimated_rows,
            *access_rows,
            false,
            None,
            Some(*degree as usize),
        ),
        PhysicalPlan::ParallelZoneMapScan {
            table,
            filter,
            projected_columns,
            runtime_filters,
            degree,
            estimated_rows,
            access_rows,
            zone_map,
            ..
        } => (
            table,
            filter,
            projected_columns,
            runtime_filters,
            *estimated_rows,
            *access_rows,
            true,
            Some(zone_map),
            Some(*degree as usize),
        ),
        _ => return Ok(None),
    };
    let proj = projected_columns.as_ref().map(|v| v.as_slice());
    let morsel_pages = scan_morsel_pages(parallel_degree.is_some_and(|degree| degree > 1));
    let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
    let template = build_scan_template(table, proj);
    if template.is_empty() {
        return Ok(None);
    }
    if let Some(f) = filter {
        let mut predicate_columns = std::collections::BTreeSet::new();
        let predicate_columns_known =
            crate::engine::executor::select::plan::collect_table_column_refs(
                f,
                &table.schema,
                &mut predicate_columns,
            );
        let output_columns = proj.map(<[usize]>::to_vec).unwrap_or_else(|| {
            table
                .schema
                .iter_visible_columns()
                .map(|(i, _)| i)
                .collect()
        });
        let predicate_columns = predicate_columns.into_iter().collect::<Vec<_>>();
        // `access_rows` may carry a row-goal-adjusted estimate even though a
        // blocking consumer (HashAggregate/TopN) must drain every surviving
        // Zone Map page.  Predicate-first is an operator-internal decode
        // layout choice, so derive its denominator from the immutable physical
        // page-survival ratio and ANALYZE row count. This prevents a small
        // LIMIT estimate from making a highly selective timestamp predicate
        // decode all projected payload columns before filtering.
        let predicate_access_rows = zone_map.map_or(access_rows, |zone| {
            if zone.total_pages == 0 || table.schema.stats.row_count == 0 {
                return access_rows;
            }
            let page_fraction = zone.surviving_pages as f64 / zone.total_pages.max(1) as f64;
            access_rows.max(table.schema.stats.row_count as f64 * page_fraction)
        });
        let physical_zone_predicate_first = zone_map.is_some()
            && predicate_access_rows > 0.0
            && estimated_rows / predicate_access_rows <= 0.15
            && output_columns
                .iter()
                .any(|column| !predicate_columns.contains(column));
        let predicate_first = predicate_columns_known
            && (physical_zone_predicate_first
                || crate::engine::executor::select::plan::should_use_predicate_first_projected_scan(
                    estimated_rows,
                    predicate_access_rows,
                    &predicate_columns,
                    &output_columns,
                ));
        let mut runtime_filtered_candidate = None;
        if let Some(mut stream) =
            FilteredStorageBatchStream::new(db, txn, table, f, proj, &[], morsel_pages)?
        {
            for runtime_filter in runtime_filters {
                if !stream.install_runtime_filter(runtime_filter.clone()) {
                    return Ok(None);
                }
            }
            // LIKE can match borrowed variable-width data before it becomes
            // an owned String/Value, which remains a win at a high match
            // rate. Fixed-width/date comparisons use predicate-first only
            // when the CBO selectivity/column-width rule selects it.
            let borrowed_like = stream.decoded.as_ref().is_some_and(
                crate::engine::executor::select::plan::DecodedPredicateProgram::has_string_like,
            );
            if predicate_first || borrowed_like || !runtime_filters.is_empty() {
                let scan_tasks = build_batch_scan_tasks(
                    db,
                    table,
                    Some(f),
                    use_zone_map,
                    parallel_degree.map(|degree| degree as u32),
                )?;
                stream.tasks = scan_tasks.into();
                if use_zone_map {
                    ZONE_MAP_BATCH_SCAN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                let stream: Box<dyn BatchOperator + 'a> = match parallel_degree {
                    Some(degree) if degree > 1 => {
                        Box::new(ParallelStorageBatchStream::from_filtered(stream, degree))
                    }
                    _ => Box::new(stream),
                };
                return Ok(Some(VecBatchInput { stream, template }));
            }
            // The local predicate was not selective enough to justify its
            // own predicate-first layout.  Keep the ordinary batch filter
            // above the scan and reserve the unopened alternative for join
            // keys only: when a parent hash join publishes a highly
            // selective runtime filter, decoding that narrow key first
            // avoids decoding every local-predicate/output column for rows
            // the join will discard anyway.
            drop(stream);
        }
        // Predicate-first decoding is an internal scan optimization, not a
        // capability boundary.  Expressions outside CompiledPredicate's
        // deliberately small repertoire (for example `IS NULL OR x >= 4`)
        // are evaluated by the common batch expression program over the same
        // native scan.  Capability discovery still happens before the source
        // is constructed/opened, so this never consumes input and retries the
        // legacy row executor.
        let schema = template_to_batch_schema(&template);
        let Some(program) = expr_batch::BatchExprProgram::compile(f, &schema)? else {
            return Ok(None);
        };
        // A physical scan leaf owns one immutable candidate artifact.  Build
        // it only after vector capability discovery has succeeded, then share
        // it among the unopened plain and runtime-filter alternatives.  A
        // legacy evaluator fallback can therefore build its own leaf without
        // inheriting a discarded candidate generation from this probe.
        let scan_tasks = build_batch_scan_tasks(
            db,
            table,
            Some(f),
            use_zone_map,
            parallel_degree.map(|degree| degree as u32),
        )?;
        if runtime_filtered_candidate.is_none() {
            runtime_filtered_candidate =
                Some(FilteredStorageBatchStream::runtime_filter_candidate(
                    db,
                    txn,
                    table,
                    proj,
                    &scan_tasks,
                    morsel_pages,
                )?);
        }
        if let Some(candidate) = runtime_filtered_candidate.as_mut() {
            for runtime_filter in runtime_filters {
                if !candidate.install_runtime_filter(runtime_filter.clone()) {
                    return Ok(None);
                }
            }
        }
        let source =
            StorageBatchStream::new(db, txn, table, proj, scan_tasks.clone(), morsel_pages)?;
        let upstream: Box<dyn BatchOperator + 'a> =
            match (parallel_degree, runtime_filtered_candidate) {
                (Some(degree), Some(filtered)) if degree > 1 && !runtime_filters.is_empty() => {
                    Box::new(ParallelStorageBatchStream::from_filtered(filtered, degree))
                }
                (Some(degree), Some(filtered)) if degree > 1 => Box::new(
                    ParallelStorageBatchStream::from_plain_with_filtered(source, filtered, degree),
                ),
                (Some(degree), None) if degree > 1 => {
                    Box::new(ParallelStorageBatchStream::from_plain(source, degree))
                }
                (_, Some(filtered)) if !runtime_filters.is_empty() => Box::new(filtered),
                (_, Some(filtered)) => Box::new(AdaptiveFilteredStorageBatchStream {
                    plain: source,
                    filtered: Some(filtered),
                    use_filtered: false,
                    opened: false,
                }),
                _ => Box::new(source),
            };
        let properties = upstream.properties();
        if use_zone_map {
            ZONE_MAP_BATCH_SCAN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let filtered: Box<dyn BatchOperator + 'a> = Box::new(FilterBatchStream {
            upstream,
            program,
            schema,
            properties,
            analyze_plan: Some(plan),
            analyze_recorded: false,
        });
        return Ok(Some(VecBatchInput {
            stream: Box::new(SparseSelectionCoalesceStream::new(filtered)),
            template,
        }));
    }
    let scan_tasks = build_batch_scan_tasks(
        db,
        table,
        None,
        use_zone_map,
        parallel_degree.map(|degree| degree as u32),
    )?;
    let stream = StorageBatchStream::new(db, txn, table, proj, scan_tasks.clone(), morsel_pages)?;
    let mut runtime_filtered_candidate = FilteredStorageBatchStream::runtime_filter_candidate(
        db,
        txn,
        table,
        proj,
        &scan_tasks,
        morsel_pages,
    )?;
    for runtime_filter in runtime_filters {
        if !runtime_filtered_candidate.install_runtime_filter(runtime_filter.clone()) {
            return Ok(None);
        }
    }
    let stream: Box<dyn BatchOperator + 'a> = match parallel_degree {
        Some(degree) if degree > 1 && !runtime_filters.is_empty() => Box::new(
            ParallelStorageBatchStream::from_filtered(runtime_filtered_candidate, degree),
        ),
        Some(degree) if degree > 1 => {
            Box::new(ParallelStorageBatchStream::from_plain_with_filtered(
                stream,
                runtime_filtered_candidate,
                degree,
            ))
        }
        _ if !runtime_filters.is_empty() => Box::new(runtime_filtered_candidate),
        _ => Box::new(AdaptiveFilteredStorageBatchStream {
            plain: stream,
            filtered: Some(runtime_filtered_candidate),
            use_filtered: false,
            opened: false,
        }),
    };
    if use_zone_map {
        ZONE_MAP_BATCH_SCAN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(Some(VecBatchInput { stream, template }))
}

/// Build any B-tree access leaf as a native batch source. Predicate lowering
/// happens before the lazy source is opened, so an unsupported residual never
/// consumes an index morsel and restarts through the row executor.
pub(super) fn try_build_direct_index_batch_scan<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let (table, index, mut access, covering, filter, projected_columns, parallel_degree) =
        match plan {
            PhysicalPlan::IndexLookup {
                table,
                index,
                key,
                exact,
                covering,
                filter,
                projected_columns,
                ..
            } => (
                table,
                index,
                vec_index_scan::IndexBatchAccess::Lookup {
                    key: key.clone(),
                    exact: *exact,
                },
                *covering,
                filter,
                projected_columns,
                None,
            ),
            PhysicalPlan::IndexMultiLookup {
                table,
                index,
                keys,
                exact,
                covering,
                filter,
                projected_columns,
                ..
            } => (
                table,
                index,
                vec_index_scan::IndexBatchAccess::MultiLookup {
                    keys: keys.clone(),
                    exact: *exact,
                },
                *covering,
                filter,
                projected_columns,
                None,
            ),
            PhysicalPlan::IndexRangeScan {
                table,
                index,
                low,
                high,
                reverse,
                covering,
                filter,
                projected_columns,
                ..
            } => (
                table,
                index,
                vec_index_scan::IndexBatchAccess::Range {
                    low: low.clone(),
                    high: high.clone(),
                    reverse: *reverse,
                },
                *covering,
                filter,
                projected_columns,
                None,
            ),
            PhysicalPlan::IndexFastScan {
                table,
                index,
                low,
                high,
                reverse,
                filter,
                projected_columns,
                ..
            } => (
                table,
                index,
                vec_index_scan::IndexBatchAccess::Range {
                    low: low.clone(),
                    high: high.clone(),
                    reverse: *reverse,
                },
                true,
                filter,
                projected_columns,
                None,
            ),
            PhysicalPlan::ParallelIndexRangeScan {
                table,
                index,
                low,
                high,
                filter,
                projected_columns,
                degree,
                ..
            } => (
                table,
                index,
                vec_index_scan::IndexBatchAccess::Range {
                    low: low.clone(),
                    high: high.clone(),
                    reverse: false,
                },
                false,
                filter,
                projected_columns,
                Some(*degree as usize),
            ),
            PhysicalPlan::ParallelIndexFastScan {
                table,
                index,
                low,
                high,
                filter,
                projected_columns,
                degree,
                ..
            } => (
                table,
                index,
                vec_index_scan::IndexBatchAccess::Range {
                    low: low.clone(),
                    high: high.clone(),
                    reverse: false,
                },
                true,
                filter,
                projected_columns,
                Some(*degree as usize),
            ),
            _ => return Ok(None),
        };
    let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
    if let vec_index_scan::IndexBatchAccess::Range { low, high, .. } = &mut access {
        crate::planner::boolean_range::refine_runtime(
            &table.schema,
            index,
            filter.as_ref(),
            low,
            high,
        );
    }
    let effective_filter = match (&access, filter.as_ref()) {
        (vec_index_scan::IndexBatchAccess::Lookup { key, exact }, Some(predicate))
            if !db.has_active_write_txns() =>
        {
            let access = crate::engine::executor::indexes::EngineIndexAccess::Lookup {
                key: key.clone(),
                exact: *exact,
            };
            crate::engine::executor::indexes::index_access_residual_filter(
                &table.schema,
                index,
                &access,
                predicate,
            )
        }
        (_, predicate) => predicate.cloned(),
    };
    let projection = projected_columns
        .as_deref()
        .map(|columns| columns.as_slice());
    let template = build_scan_template(table, projection);
    if template.is_empty() {
        return Ok(None);
    }
    let schema = template_to_batch_schema(&template);
    let mut predicate_first = if !covering
        && parallel_degree.is_none()
        && !table.schema.is_partitioned()
        && matches!(&access, vec_index_scan::IndexBatchAccess::Range { .. })
    {
        effective_filter.as_ref().and_then(|predicate| {
            let mut predicate_columns = std::collections::BTreeSet::new();
            if !crate::engine::executor::select::plan::collect_table_column_refs(
                predicate,
                &table.schema,
                &mut predicate_columns,
            ) {
                return None;
            }
            let predicate_columns = predicate_columns.into_iter().collect::<Vec<_>>();
            let output_columns = scan_projection_columns(table, projection);
            // Ordered LIMIT is especially sensitive to correlation between
            // index order and an uncovered residual: a one-row goal can walk
            // a long physical run even when global NDV predicts many hits.
            // Predicate-first is still a pure decode-layout choice. Require
            // at least two output-only columns so a high-survival run retains
            // enough avoided work to amortize its narrow predicate pass.
            // Decide before compiling: the storage programs and zone-map
            // probe below are discarded whenever there is no wide row goal.
            let row_goal_wide = scan_row_goal().is_some()
                && output_columns
                    .iter()
                    .filter(|column| !predicate_columns.contains(column))
                    .count()
                    >= 2;
            if !row_goal_wide {
                return None;
            }
            let predicate_schema = crate::engine::executor::select::plan::projected_schema_view(
                &table.schema,
                &predicate_columns,
            );
            let compiled =
                crate::engine::executor::select::plan::CompiledPredicate::compile_for_storage(
                    predicate,
                    &predicate_schema,
                )?;
            let decoded = crate::engine::executor::select::plan::DecodedPredicateProgram::compile(
                predicate,
                &predicate_schema,
            );
            let candidate_heap_ranges = db
                .zone_map_candidate_ranges(&table.schema, predicate)
                .and_then(|ranges| {
                    let covered_pages = ranges
                        .iter()
                        .map(|(low, high)| high.saturating_sub(*low))
                        .sum::<u64>();
                    let heap_pages = db.heap_page_count_for_storage(table.schema.id).ok()?;
                    (covered_pages < heap_pages).then_some(ranges)
                });
            Some((predicate_columns, compiled, decoded, candidate_heap_ranges))
        })
    } else {
        None
    };
    // Compile the ordinary batch residual even when predicate-first is
    // eligible. The latter is optional under memory pressure; a failed
    // metadata reservation must atomically retain this semantic fallback.
    let program = effective_filter
        .as_ref()
        .map(|predicate| expr_batch::BatchExprProgram::compile(predicate, &schema))
        .transpose()?
        .flatten();
    if effective_filter.is_some() && program.is_none() {
        return Ok(None);
    }
    let mut predicate_first_enabled = false;
    let mut stream: Box<dyn BatchOperator + 'a> = if parallel_degree
        .is_some_and(|degree| degree > 1)
        && !table.schema.is_partitioned()
        && matches!(
            &access,
            vec_index_scan::IndexBatchAccess::Range { reverse: false, .. }
        ) {
        let vec_index_scan::IndexBatchAccess::Range { low, high, .. } = &access else {
            unreachable!("parallel index source preflighted as forward range")
        };
        if let Some(workers) = vec_index_scan::IndexBatchStream::parallel_range_workers(
            db,
            txn,
            table,
            index,
            low.clone(),
            high.clone(),
            covering,
            projection,
            parallel_degree.expect("checked parallel degree"),
        )? {
            Box::new(ParallelIndexBatchStream::new(db, txn, workers))
        } else {
            let Some(source) = vec_index_scan::IndexBatchStream::new(
                db, txn, table, index, access, covering, projection,
            )?
            else {
                return Ok(None);
            };
            Box::new(source)
        }
    } else if table.schema.is_partitioned() {
        let Some(source) = vec_index_scan::PartitionedIndexBatchStream::new(
            db, txn, table, index, access, covering, projection,
        )?
        else {
            return Ok(None);
        };
        Box::new(source)
    } else {
        let Some(mut source) = vec_index_scan::IndexBatchStream::new(
            db, txn, table, index, access, covering, projection,
        )?
        else {
            return Ok(None);
        };
        if let Some((columns, compiled, decoded, candidate_heap_ranges)) = predicate_first.take() {
            predicate_first_enabled =
                source.enable_predicate_first(columns, compiled, decoded, candidate_heap_ranges);
        }
        Box::new(source)
    };
    if let Some(program) = program.filter(|_| !predicate_first_enabled) {
        let properties = stream.properties();
        stream = Box::new(FilterBatchStream {
            upstream: stream,
            program,
            schema,
            properties,
            analyze_plan: Some(plan),
            analyze_recorded: false,
        });
    }
    Ok(Some(VecBatchInput { stream, template }))
}

/// Lazy native batch source for Index Merge. RowId collection starts on the
/// first pull, after the complete batch graph and residual program have been
/// compiled, then reuses the ordinary native MRR heap decoder.
pub(super) struct IndexMergeBatchStream {
    pub(super) db: Arc<Database>,
    pub(super) txn: TxnView,
    pub(super) table: crate::planner::PlanTable,
    pub(super) root: crate::planner::IndexMergeNode,
    pub(super) first_index: crate::storage::IndexSchema,
    pub(super) projection: Option<Vec<usize>>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) properties: BatchProperties,
    pub(super) inner: Option<vec_index_scan::IndexBatchStream>,
}

impl BatchOperator for IndexMergeBatchStream {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.inner.is_none() {
            let ids = crate::engine::executor::select::plan::execute_index_merge_row_ids(
                &self.db,
                self.txn,
                &self.table.schema,
                &self.root,
            )?;
            self.inner = vec_index_scan::IndexBatchStream::from_row_ids(
                &self.db,
                self.txn,
                &self.table,
                &self.first_index,
                ids,
                self.projection.as_deref(),
            )?;
            if self.inner.is_none() {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "Index Merge native source has no output columns",
                ));
            }
        }
        self.inner
            .as_mut()
            .expect("index merge source initialized")
            .next_batch()
    }
}

pub(super) fn try_build_direct_index_merge_batch<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let PhysicalPlan::IndexMerge {
        table,
        root,
        filter,
        projected_columns,
        ..
    } = plan
    else {
        return Ok(None);
    };
    // Partition fan-out owns a partition-local RowId namespace in the row
    // dispatcher. Keep that correctness oracle until the batch scheduler can
    // carry storage_id alongside every RowId lane.
    if table.schema.is_partitioned() {
        return Ok(None);
    }
    let Some(first_index) = root.first_leaf().map(|leaf| leaf.index.clone()) else {
        return Ok(None);
    };
    let projection = projected_columns
        .as_deref()
        .map(|columns| columns.as_slice());
    let template = build_scan_template(table, projection);
    if template.is_empty() {
        return Ok(None);
    }
    let schema = template_to_batch_schema(&template);
    let program = filter
        .as_ref()
        .map(|predicate| expr_batch::BatchExprProgram::compile(predicate, &schema))
        .transpose()?
        .flatten();
    if filter.is_some() && program.is_none() {
        return Ok(None);
    }
    let properties = BatchProperties::UNORDERED_SINGLE;
    let mut stream: Box<dyn BatchOperator + 'a> = Box::new(IndexMergeBatchStream {
        db: Arc::clone(db),
        txn: crate::engine::executor::temporal::txn_for_table(txn, table),
        table: table.clone(),
        root: root.clone(),
        first_index,
        projection: projection.map(<[usize]>::to_vec),
        schema: Arc::clone(&schema),
        properties: properties.clone(),
        inner: None,
    });
    if let Some(program) = program {
        stream = Box::new(FilterBatchStream {
            upstream: stream,
            program,
            schema,
            properties,
            analyze_plan: Some(plan),
            analyze_recorded: false,
        });
    }
    Ok(Some(VecBatchInput { stream, template }))
}

/// Count of times the CTE columnar batch-scan leaf actually fired. Bumped once
/// per qualifying `CteScan` (not per row), so the overhead is negligible. Read
/// by tests to prove the branch engaged (rather than the query silently taking
/// the row fallback or being inlined away).
pub(crate) static CTE_BATCH_SCAN_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
