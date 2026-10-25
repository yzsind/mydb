impl Database {
    fn scan_cache_policy_for_storage_ids(
        &self,
        storage_ids: &[u64],
    ) -> Result<ScanCachePolicy, QueryError> {
        let mut physical_pages = 0u64;
        for &storage_id in storage_ids {
            physical_pages = physical_pages
                .saturating_add(self.with_heap_read(storage_id, |heap| heap.page_count())?);
        }
        Ok(self.buffer_pool.scan_cache_policy(physical_pages))
    }

    /// Conservative policy for a range worker that was handed one physical
    /// heap but not the query's partition-prune set. Classifying against the
    /// whole logical relation prevents a large partitioned table from being
    /// warmed one small worker range at a time.
    fn scan_cache_policy_for_table(
        &self,
        table: &TableSchema,
    ) -> Result<ScanCachePolicy, QueryError> {
        let storage_ids = if table.is_partitioned() {
            table.storage_ids()
        } else {
            vec![table.id]
        };
        self.scan_cache_policy_for_storage_ids(&storage_ids)
    }

    /// Pad a heap-decoded row out to `table.columns.len()` by appending
    /// each missing column's catalog DEFAULT (or NULL when no default is
    /// declared), then normalize `CHAR(N)` values to the SQL-visible
    /// compact form (no synthetic trailing pad spaces).
    ///
    /// If the row is already schema-aligned, only the cheap `CHAR`
    /// read-normalization pass remains.
    pub(crate) fn pad_full_row_to_schema(values: &mut Vec<Value>, table: &TableSchema) {
        let target_len = table.columns.len();
        values.reserve(target_len.saturating_sub(values.len()));
        while values.len() < target_len {
            let col = &table.columns[values.len()];
            if col.generated_expr.is_some() {
                let idx = values.len();
                match generated_column_value(table, idx, values) {
                    Ok(value) => values.push(value),
                    Err(_) => values.push(Value::Null),
                }
            } else {
                values.push(crate::engine::executor::insert::column_read_backfill_default(col));
            }
        }
        normalize_projected_char_values_for_read(values, None, table);
    }

    /// Normalize projected CHAR values after layout-aware decoding has
    /// resolved catalog defaults by stable column ID.
    ///
    /// This is the projected-scan analogue of [`pad_full_row_to_schema`].
    pub(crate) fn backfill_projected_with_defaults(
        values: &mut [Value],
        columns: &[usize],
        stored_count: usize,
        table: &TableSchema,
    ) {
        let plan = ProjectedReadPlan::new(columns, table);
        if !plan.needs_adjustment(values, stored_count) {
            return;
        }
        plan.apply(values, columns, stored_count, table);
    }

    pub fn scan_rows(&self, table_name: &str, txn: TxnView) -> Result<Vec<StoredRow>, QueryError> {
        let table = self
            .table_by_key(table_name)
            .ok_or_else(|| crate::error::messages::no_such_table(table_name))?;
        self.check_primary_read_view(&table, txn)?;
        let storage_ids = partition_storage_ids_for_scan(&table);
        let scan_policy = self.scan_cache_policy_for_storage_ids(&storage_ids)?;
        let mut out: Vec<StoredRow> = Vec::new();
        for sid in storage_ids {
            let rows = self.with_heap_read(sid, |heap| heap.scan_mvcc_with_policy(scan_policy))?;
            for row in rows {
                if let Some(mut visible) = self.visible_row(row, txn)? {
                    Self::pad_full_row_to_schema(&mut visible.values, &table);
                    out.push(visible);
                }
            }
        }
        // P1-D3: bump the per-statement rows-examined counter so the
        // slow query log can report what storage actually walked,
        // distinct from the WHERE/LIMIT-filtered rows that ultimately
        // reach the client (rows_sent).
        crate::engine::perf_events::record_scan_rows_examined(out.len() as u64);
        Ok(out)
    }

    /// Visit full MVCC-visible rows in the same partition/page/slot order as
    /// `scan_rows`, without retaining the whole relation. Unlike projected
    /// streaming scans, undo and forwarded rows must not move to the end:
    /// order-sensitive consumers (e.g. rolling checksums) depend on this.
    ///
    /// Collect a small physical range, release heap/page latches, then resolve
    /// undo and invoke the visitor. Memory scales with one range's decoded
    /// values (including any LOBs), rather than the table's row count.
    pub(crate) fn scan_rows_ordered_streaming<F>(
        &self,
        table_name: &str,
        txn: TxnView,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(&[Value]) -> Result<(), QueryError>,
    {
        const BATCH_PAGES: u64 = 64;
        let table = self
            .table_by_key(table_name)
            .ok_or_else(|| crate::error::messages::no_such_table(table_name))?;
        self.check_primary_read_view(&table, txn)?;
        let storage_ids = partition_storage_ids_for_scan(&table);
        let policy = self.scan_cache_policy_for_storage_ids(&storage_ids)?;
        let mut batch = Vec::new();
        let mut vis_cache = ScanVisibilityCache::new();
        let mut examined = 0u64;
        for storage_id in storage_ids {
            let page_count = self.with_heap_read(storage_id, |heap| heap.page_count())?;
            for page_lo in (0..page_count).step_by(BATCH_PAGES as usize) {
                self.with_heap_read(storage_id, |heap| {
                    heap.scan_mvcc_pages_range_until_with_policy(
                        page_lo,
                        page_lo.saturating_add(BATCH_PAGES).min(page_count),
                        policy,
                        |row| {
                            batch.push(row);
                            Ok(false)
                        },
                    )
                })?;
                for row in batch.drain(..) {
                    let mut values =
                        if self.row_version_visible_cached(&mut vis_cache, row.header, txn)? {
                            if row.header.is_deleted() {
                                continue;
                            }
                            row.values
                        } else {
                            let Some(visible) = self.visible_row(row, txn)? else {
                                continue;
                            };
                            visible.values
                        };
                    Self::pad_full_row_to_schema(&mut values, &table);
                    visit(&values)?;
                    examined += 1;
                }
            }
        }
        crate::engine::perf_events::record_scan_rows_examined(examined);
        Ok(())
    }

    /// Like [`scan_rows`], but each returned row is tagged with the
    /// `storage_id` of the partition it came from. Multi-table DML uses this
    /// so a later by-id write targets the exact partition — a bare `row_id`
    /// is ambiguous across partitions (each partition numbers rows from
    /// (0,1,0)). For unpartitioned tables the tag is always `table.id`.
    pub fn scan_rows_with_storage(
        &self,
        table_name: &str,
        txn: TxnView,
    ) -> Result<Vec<(u64, StoredRow)>, QueryError> {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        self.check_primary_read_view(&table, txn)?;
        let storage_ids = partition_storage_ids_for_scan(&table);
        let scan_policy = self.scan_cache_policy_for_storage_ids(&storage_ids)?;
        let mut out: Vec<(u64, StoredRow)> = Vec::new();
        for sid in storage_ids {
            let rows = self.with_heap_read(sid, |heap| heap.scan_mvcc_with_policy(scan_policy))?;
            for row in rows {
                if let Some(mut visible) = self.visible_row(row, txn)? {
                    Self::pad_full_row_to_schema(&mut visible.values, &table);
                    out.push((sid, visible));
                }
            }
        }
        crate::engine::perf_events::record_scan_rows_examined(out.len() as u64);
        Ok(out)
    }

    /// Page count of `table_name`'s heap. Used by parallel sequential
    /// scan to compute per-worker `[lo, hi)` partitions. Acquires the
    /// heap's shared read lock (cheap, does not block writers).
    ///
    /// Partitioned tables report 0 here so the planner's page-based
    /// parallel-scan threshold stays off the regular `[lo,hi)` fan-out.
    /// For "physical pages of a single storage id" (e.g. IS.tables hot
    /// path, or one specific partition), use [`Self::heap_page_count`].
    pub fn heap_page_count_by_name(&self, table_name: &str) -> Result<u64, QueryError> {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        if table.is_partitioned() {
            // Partitioned tables intentionally report 0 pages here so
            // the planner's page-based parallel-scan threshold stays
            // off the regular `[lo,hi)` page-range fan-out — page
            // numbering is per-partition, not table-wide. Partition-
            // aware parallelism is opt-in via the partition-pruned
            // gather stream
            // (`parallel_exchange_partitioned_stream`), which is
            // selected explicitly by callers that hold a `PlanTable`
            // for a partitioned source.
            return Ok(0);
        }
        self.with_heap_read(table.id, |heap| heap.page_count())
    }

    /// Physical page count for one concrete heap. Unlike
    /// [`Self::heap_page_count_by_name`], this is well-defined for a
    /// partitioned table because `storage_id` identifies exactly one
    /// partition.
    pub fn heap_page_count_for_storage(&self, storage_id: u64) -> Result<u64, QueryError> {
        self.with_heap_read(storage_id, |heap| heap.page_count())
    }

    /// Resolve the physical heaps selected for the current scan context.
    /// This preserves partition pruning and records the same diagnostics as
    /// the ordinary logical-table scan.
    pub fn scan_storage_ids(&self, table: &TableSchema) -> Vec<u64> {
        partition_storage_ids_for_scan(table)
    }

    /// Scan a contiguous heap page range `[page_lo, page_hi)`. Mirrors
    /// `scan_rows` but restricted to a single page slice — used by
    /// each worker of a parallel sequential scan. Holds the heap's
    /// shared read lock so multiple workers can scan disjoint ranges
    /// concurrently.
    pub fn scan_rows_range(
        &self,
        table_name: &str,
        txn: TxnView,
        page_lo: u64,
        page_hi: u64,
    ) -> Result<Vec<StoredRow>, QueryError> {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        self.check_primary_read_view(&table, txn)?;
        self.scan_rows_storage_range(&table, table.id, txn, page_lo, page_hi)
    }

    /// Scan one physical heap page range while interpreting rows with the
    /// owning logical table schema. This is the partition-aware primitive
    /// used by ZoneMapScan; row visibility remains on the existing MVCC
    /// path.
    pub fn scan_rows_storage_range(
        &self,
        table: &TableSchema,
        storage_id: u64,
        txn: TxnView,
        page_lo: u64,
        page_hi: u64,
    ) -> Result<Vec<StoredRow>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let scan_policy = self.scan_cache_policy_for_table(table)?;
        let mut collected: Vec<MvccStoredRow> = Vec::new();
        self.with_heap_read(storage_id, |heap| {
            heap.scan_mvcc_pages_range_until_with_policy(page_lo, page_hi, scan_policy, |row| {
                collected.push(row);
                Ok(false)
            })
        })?;
        let visible: Result<Vec<_>, _> = collected
            .into_iter()
            .filter_map(|row| match self.visible_row(row, txn) {
                Ok(Some(mut visible)) => {
                    Self::pad_full_row_to_schema(&mut visible.values, table);
                    Some(Ok(visible))
                }
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect();
        if let Ok(rows) = &visible {
            crate::engine::perf_events::record_scan_rows_examined(rows.len() as u64);
        }
        visible
    }

    /// Column-projected variant of `scan_rows_range`. Mirrors
    /// `scan_rows_columns_streaming` but restricted to `[page_lo, page_hi)`.
    /// Each parallel-scan worker calls this with its own range so visit
    /// callbacks fire on disjoint subsets of the heap.
    pub fn scan_rows_columns_streaming_range<F>(
        &self,
        table_name: &str,
        txn: TxnView,
        columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(&[Value]) -> Result<(), QueryError>,
    {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        self.check_primary_read_view(&table, txn)?;
        // For partitioned tables the `[page_lo, page_hi)` page range
        // doesn't translate (each partition has its own page numbering).
        // Fall back to a partition-aware scan that ignores the range —
        // `heap_page_count_by_name` returns 0 for partitioned tables so the
        // parallel scan threshold is never met and this branch only
        // fires for the rare callers that ignore that threshold.
        if table.is_partitioned() {
            return self.scan_partitioned_rows_columns_streaming_until(
                &table,
                txn,
                columns,
                &mut |values| {
                    visit(values)?;
                    Ok(false)
                },
            );
        }
        self.scan_rows_columns_storage_streaming_range(
            &table, table.id, txn, columns, page_lo, page_hi, visit,
        )
    }

    /// Projected streaming scan over a page range in one concrete heap.
    /// Deferred historical versions are re-read from that same storage id,
    /// which is required because RowIds are only unique within a partition.
    pub fn scan_rows_columns_storage_streaming_range<F>(
        &self,
        table: &TableSchema,
        storage_id: u64,
        txn: TxnView,
        columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(&[Value]) -> Result<(), QueryError>,
    {
        self.scan_rows_columns_storage_streaming_range_with_ids(
            table,
            storage_id,
            txn,
            columns,
            page_lo,
            page_hi,
            |_, values| visit(values),
        )
    }

    /// Projected scans retain physical identity for snapshot validation logs.
    pub(crate) fn scan_rows_columns_storage_streaming_range_with_ids<F>(
        &self,
        table: &TableSchema,
        storage_id: u64,
        txn: TxnView,
        columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(RowId, &[Value]) -> Result<(), QueryError>,
    {
        self.scan_rows_columns_storage_streaming_range_with_ids_impl(
            table, storage_id, txn, columns, page_lo, page_hi, true, visit,
        )
    }

    pub(crate) fn scan_rows_columns_storage_streaming_range_with_ids_impl<F>(
        &self,
        table: &TableSchema,
        storage_id: u64,
        txn: TxnView,
        columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        validate_generation: bool,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(RowId, &[Value]) -> Result<(), QueryError>,
    {
        if validate_generation {
            self.check_primary_read_view(table, txn)?;
        }
        let examined = crate::engine::perf_events::RowsExaminedBatch::new();
        let scan_policy = self.scan_cache_policy_for_table(table)?;
        // The caller owns this immutable catalog snapshot through deferred
        // MVCC reads too. Cloning it here would copy all histogram/index and
        // conditional-domain statistics once per scan morsel.
        let table_schema = table;
        let mut deferred: Vec<RowId> = Vec::new();
        let mut visit_err: Option<QueryError> = None;
        let mut vis_cache = ScanVisibilityCache::new();
        let mut backfill_scratch: Vec<Value> = Vec::new();
        let read_plan = ProjectedReadPlan::new(columns, table_schema);
        self.with_heap_read(storage_id, |heap| {
            heap.scan_mvcc_columns_range_until_with_policy(
                columns,
                page_lo,
                page_hi,
                scan_policy,
                |row| {
                    if visit_err.is_some() {
                        return Ok(true);
                    }
                    let visible =
                        self.row_version_visible_cached(&mut vis_cache, row.header, txn)?;
                    if visible {
                        if !row.header.is_deleted() {
                            let values_to_visit: &[Value] =
                                if read_plan.needs_adjustment(row.values, row.stored_count) {
                                    backfill_scratch.clear();
                                    backfill_scratch.extend_from_slice(row.values);
                                    read_plan.apply(
                                        &mut backfill_scratch,
                                        columns,
                                        row.stored_count,
                                        table_schema,
                                    );
                                    &backfill_scratch
                                } else {
                                    row.values
                                };
                            examined.add(1);
                            if let Err(e) = visit(row.row_id, values_to_visit) {
                                visit_err = Some(e);
                                return Ok(true);
                            }
                        }
                    } else {
                        deferred.push(row.row_id);
                    }
                    Ok(false)
                },
            )
        })?;
        if let Some(e) = visit_err {
            return Err(e);
        }
        for row_id in deferred {
            if let Some(values) =
                self.read_row_columns_at(storage_id, table_schema, row_id, txn, columns)?
            {
                visit(row_id, &values)?;
            }
        }
        Ok(())
    }

    /// Storage-id aware payload scan used by range-task consumers. It keeps
    /// the raw-payload fast path for partition heaps and resolves deferred
    /// MVCC rows against the same physical heap.
    pub(crate) fn scan_rows_payloads_storage_streaming_range(
        &self,
        table_schema: &TableSchema,
        storage_id: u64,
        txn: TxnView,
        columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        scratch: &mut DirectScanScratch,
        on_payload: &mut dyn for<'a> FnMut(PayloadStreamHit<'a>) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let scan_policy = self.scan_cache_policy_for_table(table_schema)?;
        let examined = crate::engine::perf_events::RowsExaminedBatch::new();
        let mut deferred: Vec<RowId> = Vec::new();
        let mut visit_err: Option<QueryError> = None;
        let mut vis_cache = ScanVisibilityCache::new();
        self.with_heap_read(storage_id, |heap| {
            let mut scan = |row: PayloadMvccStoredRow<'_>| {
                if visit_err.is_some() {
                    return Ok(true);
                }
                let visible = self.row_version_visible_cached(&mut vis_cache, row.header, txn)?;
                if !visible {
                    deferred.push(row.row_id);
                    return Ok(false);
                }
                if row.header.is_deleted() {
                    return Ok(false);
                }
                examined.add(1);
                if let Err(e) = on_payload(PayloadStreamHit::Payload(PayloadHitPayload {
                    payload: &row.payload[row.body_offset..],
                    codec: row.codec,
                    plan: row.plan,
                    layout_id: row.layout_id,
                    has_external: row.has_external,
                })) {
                    visit_err = Some(e);
                    return Ok(true);
                }
                Ok(false)
            };
            if scan_policy == ScanCachePolicy::Bulk {
                heap.scan_mvcc_payloads_direct_range_reusing_until(
                    columns, page_lo, page_hi, scratch, &mut scan,
                )
            } else {
                heap.scan_mvcc_payloads_range_until_with_policy(
                    columns,
                    page_lo,
                    page_hi,
                    scan_policy,
                    &mut scan,
                )
            }
        })?;
        if let Some(e) = visit_err {
            return Err(e);
        }
        for row_id in deferred {
            if let Some(values) =
                self.read_row_columns_at(storage_id, table_schema, row_id, txn, columns)?
            {
                on_payload(PayloadStreamHit::Values(&values))?;
            }
        }
        Ok(())
    }

    /// Raw-payload predicate-first scan used by the vectorized executor.
    ///
    /// Visible current rows first expose a narrow predicate plan. Kept rows
    /// are then exposed through `payload_output_columns`, which lets the
    /// vectorized caller request only output columns not already present in
    /// its predicate scratch. Both callbacks receive the same pinned row
    /// payload and historical versions retain the value-mode fallback.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_rows_payloads_predicate_first_storage_streaming_range(
        &self,
        table: &TableSchema,
        storage_id: u64,
        txn: TxnView,
        predicate_columns: &[usize],
        output_columns: &[usize],
        payload_output_columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        scratch: &mut DirectScanScratch,
        predicate: &mut dyn for<'a> FnMut(PayloadStreamHit<'a>) -> Result<bool, QueryError>,
        visit: &mut dyn for<'a> FnMut(PayloadStreamHit<'a>) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        self.check_primary_read_view(table, txn)?;
        let scan_policy = self.scan_cache_policy_for_table(table)?;
        let examined = crate::engine::perf_events::RowsExaminedBatch::new();
        // The caller owns this immutable catalog snapshot through deferred
        // MVCC reads too. Cloning it here would copy all histogram/index and
        // conditional-domain statistics once per scan morsel.
        let table_schema = table;
        let mut deferred: Vec<RowId> = Vec::new();
        let mut callback_error: Option<QueryError> = None;
        let mut vis_cache = ScanVisibilityCache::new();
        let mut output_plan: Option<RowDecodePlan> = None;

        self.with_heap_read(storage_id, |heap| {
            let mut scan = |row: PayloadMvccStoredRow<'_>| {
                if callback_error.is_some() {
                    return Ok(true);
                }
                let visible = self.row_version_visible_cached(&mut vis_cache, row.header, txn)?;
                if !visible {
                    deferred.push(row.row_id);
                    return Ok(false);
                }
                if row.header.is_deleted() {
                    return Ok(false);
                }

                examined.add(1);
                let predicate_hit = PayloadStreamHit::Payload(PayloadHitPayload {
                    payload: &row.payload[row.body_offset..],
                    codec: row.codec,
                    plan: row.plan,
                    layout_id: row.layout_id,
                    has_external: row.has_external,
                });
                let keep = match predicate(predicate_hit) {
                    Ok(keep) => keep,
                    Err(error) => {
                        callback_error = Some(error);
                        return Ok(true);
                    }
                };
                if !keep {
                    return Ok(false);
                }

                let plan = output_plan.get_or_insert_with(|| {
                    RowDecodePlan::compile_with_codec(payload_output_columns, row.codec)
                });
                let output_hit = PayloadStreamHit::Payload(PayloadHitPayload {
                    payload: &row.payload[row.body_offset..],
                    codec: row.codec,
                    plan,
                    layout_id: row.layout_id,
                    has_external: row.has_external,
                });
                if let Err(error) = visit(output_hit) {
                    callback_error = Some(error);
                    return Ok(true);
                }
                Ok(false)
            };
            if scan_policy == ScanCachePolicy::Bulk {
                heap.scan_mvcc_payloads_direct_range_reusing_until(
                    predicate_columns,
                    page_lo,
                    page_hi,
                    scratch,
                    &mut scan,
                )
            } else {
                heap.scan_mvcc_payloads_range_until_with_policy(
                    predicate_columns,
                    page_lo,
                    page_hi,
                    scan_policy,
                    &mut scan,
                )
            }
        })?;
        if let Some(error) = callback_error {
            return Err(error);
        }

        for row_id in deferred {
            let Some(predicate_values) =
                self.read_row_columns_at(storage_id, table_schema, row_id, txn, predicate_columns)?
            else {
                continue;
            };
            if !predicate(PayloadStreamHit::Values(&predicate_values))? {
                continue;
            }
            if let Some(output_values) =
                self.read_row_columns_at(storage_id, table_schema, row_id, txn, output_columns)?
            {
                visit(PayloadStreamHit::Values(&output_values))?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    /// Predicate-first projected scan over a heap page range. The caller
    /// supplies a narrow `predicate_columns` projection used to decide
    /// whether a row is interesting, and a wider `output_columns`
    /// projection that is decoded only for rows accepted by `predicate`.
    pub fn scan_rows_columns_predicate_first_streaming_range<P, V>(
        &self,
        table_name: &str,
        txn: TxnView,
        predicate_columns: &[usize],
        output_columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        mut predicate: P,
        mut visit: V,
    ) -> Result<(), QueryError>
    where
        P: FnMut(&[Value]) -> Result<bool, QueryError>,
        V: FnMut(&[Value]) -> Result<(), QueryError>,
    {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        self.check_primary_read_view(&table, txn)?;
        let examined = crate::engine::perf_events::RowsExaminedBatch::new();
        // Partitioned tables: fall back to a per-partition scan with a
        // simple predicate+visit shape. The page range is ignored
        // (partitions don't share a page-numbering space).
        if table.is_partitioned() {
            return self.scan_partitioned_rows_columns_streaming_until(
                &table,
                txn,
                output_columns,
                &mut |values| {
                    examined.add(1);
                    // The predicate operates on `predicate_columns`. We
                    // decode `output_columns` here; if the two
                    // projections overlap exactly the predicate input
                    // is already the same slice — otherwise re-decode
                    // is needed. The partitioned fast-path uses the
                    // simpler "decode once" model where output covers
                    // predicate, which matches every caller below
                    // (planner emits predicate-first only when
                    // output_columns ⊃ predicate_columns).
                    let pred_values: Vec<Value> = predicate_columns
                        .iter()
                        .map(|pi| {
                            output_columns
                                .iter()
                                .position(|oi| oi == pi)
                                .and_then(|pos| values.get(pos).cloned())
                                .unwrap_or(Value::Null)
                        })
                        .collect();
                    if !predicate(&pred_values)? {
                        return Ok(false);
                    }
                    visit(values)?;
                    Ok(false)
                },
            );
        }
        let table_id = table.id;
        let table_schema = table.clone();
        let scan_policy = self.scan_cache_policy_for_table(&table_schema)?;
        let mut deferred: Vec<RowId> = Vec::new();
        let visit_err: std::cell::RefCell<Option<QueryError>> = std::cell::RefCell::new(None);
        let mut vis_cache = ScanVisibilityCache::new();
        let mut pred_backfill: Vec<Value> = Vec::new();
        let mut out_backfill: Vec<Value> = Vec::new();
        let pred_plan = ProjectedReadPlan::new(predicate_columns, &table_schema);
        let out_plan = ProjectedReadPlan::new(output_columns, &table_schema);
        // Pre-compute `missing_columns` / `output_sources` once here so
        // that the heap scanner — which may run per-partition across
        // multiple worker threads in the parallel-scan path — doesn't
        // rebuild the same lookup on every call.
        let pf_plan = PredicateFirstReadPlan::new(predicate_columns, output_columns);
        self.with_heap_read(table_id, |heap| {
            heap.scan_mvcc_columns_predicate_first_range_until_with_policy(
                predicate_columns,
                output_columns,
                &pf_plan.missing_columns,
                &pf_plan.output_sources,
                page_lo,
                page_hi,
                scan_policy,
                |row| {
                    if visit_err.borrow().is_some() {
                        return Ok(true);
                    }
                    let visible =
                        self.row_version_visible_cached(&mut vis_cache, row.header, txn)?;
                    if !visible {
                        deferred.push(row.row_id);
                        return Ok(false);
                    }
                    if row.header.is_deleted() {
                        return Ok(false);
                    }
                    examined.add(1);
                    let pred_values = projected_values_for_read(
                        row.values,
                        predicate_columns,
                        &pred_plan,
                        row.stored_count,
                        &table_schema,
                        &mut pred_backfill,
                    );
                    match predicate(pred_values) {
                        Ok(keep) => Ok(keep),
                        Err(e) => {
                            *visit_err.borrow_mut() = Some(e);
                            Ok(true)
                        }
                    }
                },
                |row| {
                    if visit_err.borrow().is_some() {
                        return Ok(true);
                    }
                    let values_to_visit = projected_values_for_read(
                        row.values,
                        output_columns,
                        &out_plan,
                        row.stored_count,
                        &table_schema,
                        &mut out_backfill,
                    );
                    if let Err(e) = visit(values_to_visit) {
                        *visit_err.borrow_mut() = Some(e);
                        return Ok(true);
                    }
                    Ok(false)
                },
            )
        })?;
        if let Some(e) = visit_err.into_inner() {
            return Err(e);
        }
        for row_id in deferred {
            let Some(pred_values) =
                self.read_row_columns(&table_schema, row_id, txn, predicate_columns)?
            else {
                continue;
            };
            if !predicate(&pred_values)? {
                continue;
            }
            if let Some(values) =
                self.read_row_columns(&table_schema, row_id, txn, output_columns)?
            {
                visit(&values)?;
            }
        }
        Ok(())
    }

    fn dml_candidate_rows(
        &self,
        table: &TableSchema,
        txn: TxnView,
        candidates: Option<Vec<RowId>>,
    ) -> Result<Vec<StoredRow>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let mut rows: Vec<StoredRow> = match candidates {
            Some(ids) => ids
                .into_iter()
                .filter_map(|row_id| self.dml_candidate_row(table, row_id, txn).transpose())
                .collect::<Result<Vec<_>, _>>()?,
            // Full-scan fallback must visit every partition storage for a
            // partitioned table (rows never live in `table.id`'s own heap),
            // honouring any active partition prune set.
            None => {
                let mut out = Vec::new();
                let storage_ids = partition_storage_ids_for_scan(table);
                let scan_policy = self.scan_cache_policy_for_storage_ids(&storage_ids)?;
                for storage_id in storage_ids {
                    let scanned = self.with_heap_mut(storage_id, |heap| {
                        heap.scan_mvcc_with_policy(scan_policy)
                    })?;
                    for row in scanned {
                        if let Some(candidate) = self.dml_candidate_from_mvcc(row, txn)? {
                            out.push(candidate);
                        }
                    }
                }
                out
            }
        };
        // Pad each visible row out to the latest schema so DML callers
        // (UPDATE/DELETE) see and re-emit values for every Public
        // column even if the heap row predates a recent INSTANT ADD
        // COLUMN. dml_candidate_row already pads its own path.
        for row in &mut rows {
            Self::pad_full_row_to_schema(&mut row.values, table);
        }
        Ok(rows)
    }

    pub fn dml_candidate_row_for_write(
        &self,
        table: &TableSchema,
        row_id: RowId,
        txn: TxnView,
    ) -> Result<Option<StoredRow>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        self.dml_candidate_row(table, row_id, txn)
    }

    /// Read DML candidates while retaining their exact heap/partition origin.
    /// The explicit identity is required because partition-local RowIds can
    /// collide and probing by RowId can select a different partition.
    pub fn dml_candidate_rows_with_origin_for_write(
        &self,
        table: &TableSchema,
        txn: TxnView,
        candidates: Option<Vec<crate::storage::DmlRowRef>>,
    ) -> Result<Vec<crate::storage::DmlStoredRow>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let mut rows = Vec::new();
        match candidates {
            Some(candidates) => {
                for row_ref in candidates {
                    if let Some(row) = self.dml_candidate_row_at_for_write(table, row_ref, txn)? {
                        rows.push(row);
                    }
                }
            }
            None => {
                let storage_ids = partition_storage_ids_for_scan(table);
                let scan_policy = self.scan_cache_policy_for_storage_ids(&storage_ids)?;
                for storage_id in storage_ids {
                    let scanned = self.with_heap_mut(storage_id, |heap| {
                        heap.scan_mvcc_with_policy(scan_policy)
                    })?;
                    for row in scanned {
                        if let Some(mut candidate) = self.dml_candidate_from_mvcc(row, txn)? {
                            Self::pad_full_row_to_schema(&mut candidate.values, table);
                            rows.push(crate::storage::DmlStoredRow {
                                row_ref: crate::storage::DmlRowRef {
                                    storage_id,
                                    row_id: candidate.row_id,
                                },
                                values: candidate.values,
                            });
                        }
                    }
                }
            }
        }
        Ok(rows)
    }

    pub fn dml_candidate_row_at_for_write(
        &self,
        table: &TableSchema,
        row_ref: crate::storage::DmlRowRef,
        txn: TxnView,
    ) -> Result<Option<crate::storage::DmlStoredRow>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let Some(row) =
            self.with_heap_mut(row_ref.storage_id, |heap| heap.read_mvcc(row_ref.row_id))?
        else {
            return Ok(None);
        };
        let Some(mut candidate) = self.dml_candidate_from_mvcc(row, txn)? else {
            return Ok(None);
        };
        Self::pad_full_row_to_schema(&mut candidate.values, table);
        Ok(Some(crate::storage::DmlStoredRow {
            row_ref,
            values: candidate.values,
        }))
    }

    /// Physical-order DML scan with early stop and exact partition identity.
    /// Visible top versions are delivered directly from the heap walk;
    /// versions requiring undo visibility are revisited after releasing the
    /// heap handle. This is the no-ORDER-BY, small-LIMIT candidate path.
    pub fn dml_candidate_rows_with_origin_streaming_until<F>(
        &self,
        table: &TableSchema,
        txn: TxnView,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(crate::storage::DmlStoredRow) -> Result<bool, QueryError>,
    {
        self.check_primary_read_view(table, txn)?;
        let storage_ids = partition_storage_ids_for_scan(table);
        let scan_policy = self.scan_cache_policy_for_storage_ids(&storage_ids)?;
        for storage_id in storage_ids {
            let mut deferred = Vec::new();
            let mut visit_err = None;
            let mut stopped = false;
            let mut vis_cache = ScanVisibilityCache::new();
            self.with_heap_mut(storage_id, |heap| {
                heap.scan_mvcc_until_with_policy(scan_policy, |row| {
                    if stopped || visit_err.is_some() {
                        return Ok(true);
                    }
                    if self.row_version_visible_cached(&mut vis_cache, row.header, txn)? {
                        if !row.header.is_deleted() {
                            let mut values = row.values;
                            Self::pad_full_row_to_schema(&mut values, table);
                            let candidate = crate::storage::DmlStoredRow {
                                row_ref: crate::storage::DmlRowRef {
                                    storage_id,
                                    row_id: row.row_id,
                                },
                                values,
                            };
                            match visit(candidate) {
                                Ok(stop) => stopped = stop,
                                Err(err) => visit_err = Some(err),
                            }
                        }
                    } else {
                        deferred.push(crate::storage::DmlRowRef {
                            storage_id,
                            row_id: row.row_id,
                        });
                    }
                    Ok(stopped || visit_err.is_some())
                })
            })?;
            if let Some(err) = visit_err {
                return Err(err);
            }
            if stopped {
                return Ok(());
            }
            for row_ref in deferred {
                if let Some(candidate) = self.dml_candidate_row_at_for_write(table, row_ref, txn)? {
                    if visit(candidate)? {
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    fn dml_candidate_row(
        &self,
        table: &TableSchema,
        row_id: RowId,
        txn: TxnView,
    ) -> Result<Option<StoredRow>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        // For partitioned tables the row_id is per-partition; resolve the
        // owning partition storage before reading (collapses to `table.id`
        // for unpartitioned tables).
        let storage_id = self.locate_row_storage_id(table, row_id)?;
        let Some(row) = self.with_heap_mut(storage_id, |heap| heap.read_mvcc(row_id))? else {
            return Ok(None);
        };
        let mut candidate = match self.dml_candidate_from_mvcc(row, txn)? {
            Some(c) => c,
            None => return Ok(None),
        };
        Self::pad_full_row_to_schema(&mut candidate.values, table);
        Ok(Some(candidate))
    }

    fn dml_candidate_from_mvcc(
        &self,
        row: MvccStoredRow,
        txn: TxnView,
    ) -> Result<Option<StoredRow>, QueryError> {
        if row.header.last_txn_id == txn.txn_id {
            return if row.header.is_deleted() {
                Ok(None)
            } else {
                Ok(Some(StoredRow {
                    row_id: row.row_id,
                    values: row.values,
                }))
            };
        }
        let last_txn_status = {
            let txns = self.txns_read()?;
            txns.status(row.header.last_txn_id)
        };
        match last_txn_status {
            Some(TxnStatus::Active | TxnStatus::Prepared) => {
                if let Some(visible) = self.visible_row(row.clone(), txn)? {
                    return Ok(Some(visible));
                }
                Ok(Some(StoredRow {
                    row_id: row.row_id,
                    values: row.values,
                }))
            }
            Some(TxnStatus::RolledBack) => self.visible_row(row, txn),
            Some(TxnStatus::Committed) | None => {
                if row.header.is_deleted() {
                    Ok(None)
                } else {
                    Ok(Some(StoredRow {
                        row_id: row.row_id,
                        values: row.values,
                    }))
                }
            }
        }
    }

    pub fn scan_rows_limited<F>(
        &self,
        table_name: &str,
        txn: TxnView,
        mut accept: F,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<Vec<StoredRow>, QueryError>
    where
        F: FnMut(&StoredRow) -> Result<bool, QueryError>,
    {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        self.check_primary_read_view(&table, txn)?;
        let max_rows = limit.unwrap_or(usize::MAX);
        if max_rows == 0 {
            return Ok(Vec::new());
        }
        let mut skipped = 0;
        let mut out = Vec::new();
        let storage_ids = partition_storage_ids_for_scan(&table);
        let scan_policy = self.scan_cache_policy_for_storage_ids(&storage_ids)?;
        'outer: for sid in storage_ids {
            let mut stop = false;
            self.with_heap_mut(sid, |heap| {
                heap.scan_mvcc_until_with_policy(scan_policy, |row| {
                    let Some(mut row) = self.visible_row(row, txn)? else {
                        return Ok(false);
                    };
                    Self::pad_full_row_to_schema(&mut row.values, &table);
                    if !accept(&row)? {
                        return Ok(false);
                    }
                    if skipped < offset {
                        skipped += 1;
                        return Ok(false);
                    }
                    out.push(row);
                    if out.len() >= max_rows {
                        stop = true;
                        return Ok(true);
                    }
                    Ok(false)
                })
            })?;
            if stop {
                break 'outer;
            }
        }
        Ok(out)
    }

    #[cfg(test)]
    pub fn table_row_count(&self, table_name: &str) -> Result<usize, QueryError> {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        if table.stats.row_count > 0
            && table.stats.analyzed_at_unix_ms.is_some()
            && table.stats.modified_rows_since_analyze == 0
            && self.table_modification_delta(table.id) == 0
        {
            return Ok(table.stats.row_count as usize);
        }
        let mut row_count = 0;
        let scan_policy = self.scan_cache_policy_for_table(&table)?;
        self.with_heap_mut(table.id, |heap| {
            heap.scan_mvcc_until_with_policy(scan_policy, |row| {
                if !row.header.is_deleted() {
                    row_count += 1;
                }
                Ok(false)
            })
        })?;
        Ok(row_count)
    }

    /// Streaming, column-pruned heap scan with MVCC visibility.
    ///
    /// Decodes only the columns named in `columns` (heap row codec
    /// supports per-column projection). Each visible row is presented
    /// to `visit` as a `&[Value]` of length `columns.len()`, in the
    /// same order as `columns`. No `StoredRow` / `Vec` materialisation.
    ///
    /// MVCC handling:
    ///   * Top version is checked via `row_version_visible` while the
    ///     heap latch is held; visible non-deleted rows are emitted
    ///     immediately.
    ///   * Rows whose top version is invisible (concurrent writer or
    ///     older snapshot) are deferred: their RowIds are collected and
    ///     resolved through `read_row_columns` after the latch drops.
    ///     `read_row_columns` walks the undo chain and reapplies the
    ///     projection, preserving correctness for the rare case.
    ///
    /// Used by the HashAgg-over-SeqScan fused executor (TPC-H Q1
    /// pattern) to avoid the dual `Vec<StoredRow>` + `Vec<PlanRow>`
    /// materialisation and the full-row decode of unreferenced columns.
    pub fn scan_rows_columns_streaming<F>(
        &self,
        table_name: &str,
        txn: TxnView,
        columns: &[usize],
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(&[Value]) -> Result<(), QueryError>,
    {
        self.scan_rows_columns_streaming_until(table_name, txn, columns, |values| {
            visit(values)?;
            Ok(false)
        })
    }

    /// Visibility-only heap scan used by descriptor-level LOB projections.
    /// Decoding an empty column set still yields RowId + MVCC header, so the
    /// scan never materializes an inline or external value. Invisible top
    /// versions are resolved after releasing the heap latch.
    pub(crate) fn scan_visible_row_ids(
        &self,
        table: &TableSchema,
        txn: TxnView,
    ) -> Result<Vec<RowId>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        if table.is_partitioned() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "descriptor-only LOB scan does not support partitioned tables",
            ));
        }
        let scan_policy = self.scan_cache_policy_for_table(table)?;
        let mut visible = Vec::new();
        let mut deferred = Vec::new();
        let mut vis_cache = ScanVisibilityCache::new();
        self.with_heap_read(table.id, |heap| {
            heap.scan_mvcc_columns_until_with_policy(&[], scan_policy, |row| {
                if self.row_version_visible_cached(&mut vis_cache, row.header, txn)? {
                    if !row.header.is_deleted() {
                        visible.push(row.row_id);
                    }
                } else {
                    deferred.push(row.row_id);
                }
                Ok(false)
            })
        })?;
        for row_id in deferred {
            if self.is_row_id_visible(table, row_id, txn)? {
                visible.push(row_id);
            }
        }
        Ok(visible)
    }

    /// Column-projected streaming scan that lets the visitor stop the
    /// scan early by returning `Ok(true)`.
    pub fn scan_rows_columns_streaming_until<F>(
        &self,
        table_name: &str,
        txn: TxnView,
        columns: &[usize],
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(&[Value]) -> Result<bool, QueryError>,
    {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        self.check_primary_read_view(&table, txn)?;
        let examined = crate::engine::perf_events::RowsExaminedBatch::new();
        if table.is_partitioned() {
            return self.scan_partitioned_rows_columns_streaming_until(
                &table,
                txn,
                columns,
                &mut |values| {
                    examined.add(1);
                    visit(values)
                },
            );
        }
        let table_id = table.id;
        let table_schema = table.clone();
        let scan_policy = self.scan_cache_policy_for_table(&table_schema)?;
        // Rows whose top version is not visible at our snapshot (rare
        // under analytical workloads) are revisited via `read_row_columns`
        // after the heap latch is released, to avoid recursive locking.
        let mut deferred: Vec<RowId> = Vec::new();
        let mut visit_err: Option<QueryError> = None;
        let mut stopped = false;
        // Hot-path scan-local cache: a direct-mapped (txn_id -> visible)
        // memo replaces the per-row thread-local RefCell + LRU probe; see
        // `row_version_visible_cached` for the soundness argument.
        let mut vis_cache = ScanVisibilityCache::new();
        // Reusable per-row scratch for default-backfilled values.
        // `decode_row_columns_into` writes into the heap's internal
        // buffer (a `&[Value]` we don't own); when the projection
        // touches an INSTANT-added column we copy into this owned
        // buffer, substitute defaults, and hand `visit` the corrected
        // slice.
        let mut backfill_scratch: Vec<Value> = Vec::new();
        let read_plan = ProjectedReadPlan::new(columns, &table_schema);
        self.with_heap_mut(table_id, |heap| {
            heap.scan_mvcc_columns_until_with_policy(columns, scan_policy, |row| {
                if visit_err.is_some() || stopped {
                    return Ok(true);
                }
                let visible = self.row_version_visible_cached(&mut vis_cache, row.header, txn)?;
                if visible {
                    if !row.header.is_deleted() {
                        let values_to_visit: &[Value] =
                            if read_plan.needs_adjustment(row.values, row.stored_count) {
                                backfill_scratch.clear();
                                backfill_scratch.extend_from_slice(row.values);
                                read_plan.apply(
                                    &mut backfill_scratch,
                                    columns,
                                    row.stored_count,
                                    &table_schema,
                                );
                                &backfill_scratch
                            } else {
                                row.values
                            };
                        examined.add(1);
                        match visit(values_to_visit) {
                            Ok(stop) => {
                                if stop {
                                    stopped = true;
                                    return Ok(true);
                                }
                            }
                            Err(e) => {
                                visit_err = Some(e);
                                return Ok(true);
                            }
                        }
                    }
                } else {
                    deferred.push(row.row_id);
                }
                Ok(false)
            })
        })?;
        if let Some(e) = visit_err {
            return Err(e);
        }
        if stopped {
            return Ok(());
        }
        for row_id in deferred {
            if let Some(values) = self.read_row_columns(&table_schema, row_id, txn, columns)? {
                if visit(&values)? {
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn analyze_table(&self, table_name: &str) -> Result<(), QueryError> {
        let table = self.table(table_name)?;
        if is_temp_table_id(table.id) {
            let stats = self
                .collect_table_stats_with_sample_pages_and_zone(
                    &table,
                    None,
                    None,
                    TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
                )?
                .0;
            let mut tables = self.temp_tables.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?;
            let stored = tables.get_mut(table_name).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    "Temporary table no longer exists",
                )
            })?;
            let stored = Arc::make_mut(stored);
            stored.stats = stats;
            stored.bump_schema_version_without_system_time_epoch();
            return Ok(());
        }
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        if is_system_database(&database) {
            return Err(error(
                crate::error::catalog::ER_DBACCESS_DENIED_ERROR,
                format!("access denied for database '{database}'"),
            ));
        }
        let key = table_key(&database, &table_only);
        let _role = self.zone_map_gate.read_recursive();
        self.ensure_statistics_writable()?;
        self.refresh_table_stats(&key, true)?;
        self.commit_catalog_txn()
    }

    /// Build explicitly requested SQL histograms from a bounded reservoir.
    /// Ordinary ANALYZE continues to maintain the independent CBO statistics.
    pub fn update_table_histograms(
        self: &Arc<Self>,
        table_name: &str,
        columns: &[String],
        buckets: usize,
    ) -> Result<Vec<String>, QueryError> {
        let _role = self.zone_map_gate.read_recursive();
        self.ensure_statistics_writable()?;
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        if is_system_database(&database) {
            return Err(error(
                crate::error::catalog::ER_DBACCESS_DENIED_ERROR,
                format!("access denied for database '{database}'"),
            ));
        }
        if !(1..=1024).contains(&buckets) {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                "Histogram bucket count must be between 1 and 1024",
            ));
        }
        let key = table_key(&database, &table_only);
        let analyze_lock = self.analyze_lock(&key);
        let _table_analyze = analyze_lock.lock();
        let table = self.table(&key)?;
        let selected: Vec<_> = columns
            .iter()
            .map(|name| {
                table
                    .iter_visible_columns()
                    .find(|(_, c)| c.name.eq_ignore_ascii_case(name))
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_BAD_FIELD_ERROR,
                            format!("Unknown column '{name}' in '{key}'"),
                        )
                    })
            })
            .collect::<Result<_, _>>()?;
        for (_, column) in &selected {
            if matches!(
                column.data_type,
                DataTypeKind::Json | DataTypeKind::Geometry { .. } | DataTypeKind::Vector { .. }
            ) {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!(
                        "Histogram not supported for column '{}' of this data type",
                        column.name
                    ),
                ));
            }
        }
        let positions: Vec<_> = selected.iter().map(|(pos, _)| *pos).collect();
        let (txn, _snapshot) =
            self.read_only_statement_view_with_guard(IsolationLevel::ReadCommitted, None);
        #[cfg(test)]
        {
            // Histogram collection uses its own snapshot path. Pause after
            // capture so the same online-ANALYZE race test can verify that a
            // concurrent commit or rollback cannot change the sampled view.
            let hook = self.analyze_test_hook.lock().unwrap().take();
            if let Some(hook) = hook {
                hook.pause();
            }
        }
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        use rand::{Rng, SeedableRng};
        const SAMPLE_LIMIT: usize = 8192;
        let mut charge = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            SAMPLE_LIMIT * std::mem::size_of::<Vec<Value>>(),
        )?;
        let mut sample: Vec<Vec<Value>> = Vec::with_capacity(SAMPLE_LIMIT);
        let mut seen = 0_u64;
        let mut random = rand::rngs::StdRng::seed_from_u64(0x9e3779b97f4a7c15);
        // Reserve for retained values plus sorting/grouping copies and JSON output.
        let row_bytes = |row: &[Value]| {
            row.iter()
                .map(|v| std::mem::size_of::<Value>() + v.allocated_bytes())
                .sum::<usize>()
                .saturating_mul(4)
        };
        self.scan_rows_columns_streaming(&key, txn, &positions, |row| {
            seen += 1;
            let index = if sample.len() < SAMPLE_LIMIT {
                sample.len() as u64
            } else {
                random.gen_range(0..seen)
            };
            if index < SAMPLE_LIMIT as u64 {
                let index = index as usize;
                let old = sample.get(index).map_or(0, |v| row_bytes(v));
                let new = row_bytes(row);
                charge.try_grow(new.saturating_sub(old))?;
                if index == sample.len() {
                    sample.push(row.to_vec());
                } else {
                    sample[index] = row.to_vec();
                }
                charge.shrink(old.saturating_sub(new));
            }
            Ok(())
        })?;
        let mut histograms = BTreeMap::new();
        for (pos, (_, column)) in selected.iter().enumerate() {
            let values: Vec<_> = sample.iter().map(|row| row[pos].clone()).collect();
            histograms.insert(
                normalize_name(&column.name),
                crate::storage::stats::sql_histogram(values, column, buckets, seen),
            );
        }
        let mut combined = table.stats.explicit_histograms.clone();
        combined.extend(histograms);
        // The DD statistics payload is bounded. Explicit SQL metadata must
        // fail atomically instead of being silently trimmed on persistence.
        if serde_json::to_vec(&combined).map_or(true, |json| {
            json.len() > crate::storage::data_dictionary::DD_STATS_JSON_CAP_BYTES / 2
        }) {
            return Err(error(
                crate::error::catalog::ER_OUT_OF_RESOURCES,
                "Explicit histogram metadata exceeds the per-table catalog budget",
            ));
        }
        {
            let mut catalog = self.catalog_write()?;
            let stored = catalog.tables.get_mut(&key).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    format!("Table '{key}' doesn't exist"),
                )
            })?;
            if stored.schema_version != table.schema_version {
                return Err(error(
                    crate::error::catalog::ER_NEED_REPREPARE,
                    "Table changed while building histogram",
                ));
            }
            let stored = Arc::make_mut(stored);
            stored.stats.explicit_histograms = combined;
            stored.bump_schema_version_without_system_time_epoch();
            self.note_catalog_table(stored.id);
        }
        self.commit_catalog_txn()?;
        Ok(selected.iter().map(|(_, c)| c.name.clone()).collect())
    }

    /// `ANALYZE TABLE ... DROP HISTOGRAM ON cols`. Clears the named
    /// columns' histograms (leaving their other stats intact). Returns
    /// the resolved column names.
    pub fn drop_table_histograms(
        &self,
        table_name: &str,
        columns: &[String],
    ) -> Result<Vec<String>, QueryError> {
        let _role = self.zone_map_gate.read_recursive();
        self.ensure_statistics_writable()?;
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        if is_system_database(&database) {
            return Err(error(
                crate::error::catalog::ER_DBACCESS_DENIED_ERROR,
                format!("access denied for database '{database}'"),
            ));
        }
        let key = table_key(&database, &table_only);
        let analyze_lock = self.analyze_lock(&key);
        let _table_analyze = analyze_lock.lock();
        self.validate_histogram_columns(&key, columns)?;
        let mut resolved = Vec::with_capacity(columns.len());
        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog.tables.get_mut(&key).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    format!("Table '{key}' doesn't exist"),
                )
            })?;
            let table = Arc::make_mut(table_arc);
            for col in columns {
                let norm = normalize_name(col);
                resolved.push(
                    table
                        .columns
                        .iter()
                        .find(|column| normalize_name(&column.name) == norm)
                        .expect("histogram column validated above")
                        .name
                        .clone(),
                );
                table.stats.explicit_histograms.remove(&norm);
            }
            table.bump_schema_version_without_system_time_epoch();
            self.note_catalog_table(table_arc.id);
        }
        self.commit_catalog_txn()?;
        Ok(resolved)
    }

    /// Confirm every requested histogram column exists on the table,
    /// returning MySQL error 1054 (unknown column) otherwise.
    fn validate_histogram_columns(
        &self,
        table_key: &str,
        columns: &[String],
    ) -> Result<(), QueryError> {
        let catalog = self.catalog_read()?;
        let table = catalog.tables.get(table_key).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_key}' doesn't exist"),
            )
        })?;
        for col in columns {
            let exists = table
                .columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(col));
            if !exists {
                return Err(error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    format!("Unknown column '{col}' in '{table_key}'"),
                ));
            }
        }
        Ok(())
    }

    pub fn refresh_stale_stats_for_tables<'a>(
        &self,
        table_names: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), QueryError> {
        // Hold the role barrier through the catalog commit, not only the scan.
        let _role = self.zone_map_gate.read_recursive();
        self.ensure_operational()?;
        if self.physical_replica_mode() || self.is_snapshot_read_only() {
            return Ok(());
        }
        let mut refreshed = false;
        let mut seen = HashSet::new();
        for name in table_names {
            let key = normalize_table_key(DEFAULT_DATABASE, name);
            if !seen.insert(key.clone()) {
                continue;
            }
            let needs_refresh = {
                let catalog = self.catalog_read()?;
                catalog.tables.get(&key).map(|table| {
                    let delta = self.table_modification_delta(table.id);
                    table.stats_needs_refresh_with_delta(delta)
                })
            };
            if let Some(true) = needs_refresh {
                refreshed |= self.refresh_table_stats(&key, false)?;
            }
        }
        if refreshed {
            self.commit_catalog_txn()?;
        }
        Ok(())
    }

    /// Partition-aware fan-out for the streaming column scan. Visits
    /// each partition's heap in declaration order; deferred rows are
    /// re-read against the same partition's heap so MVCC visibility
    /// stays correct.
    fn scan_partitioned_rows_columns_streaming_until(
        &self,
        table: &TableSchema,
        txn: TxnView,
        columns: &[usize],
        visit: &mut dyn FnMut(&[Value]) -> Result<bool, QueryError>,
    ) -> Result<(), QueryError> {
        self.check_primary_read_view(table, txn)?;
        let storage_ids = partition_storage_ids_for_scan(table);
        let scan_policy = self.scan_cache_policy_for_storage_ids(&storage_ids)?;
        let read_plan = ProjectedReadPlan::new(columns, table);
        let mut stopped = false;
        for storage_id in storage_ids {
            if stopped {
                break;
            }
            let mut deferred: Vec<RowId> = Vec::new();
            let mut visit_err: Option<QueryError> = None;
            let mut vis_cache = ScanVisibilityCache::new();
            let mut backfill_scratch: Vec<Value> = Vec::new();
            self.with_heap_mut(storage_id, |heap| {
                heap.scan_mvcc_columns_until_with_policy(columns, scan_policy, |row| {
                    if visit_err.is_some() || stopped {
                        return Ok(true);
                    }
                    let visible =
                        self.row_version_visible_cached(&mut vis_cache, row.header, txn)?;
                    if visible {
                        if !row.header.is_deleted() {
                            let values_to_visit: &[Value] =
                                if read_plan.needs_adjustment(row.values, row.stored_count) {
                                    backfill_scratch.clear();
                                    backfill_scratch.extend_from_slice(row.values);
                                    read_plan.apply(
                                        &mut backfill_scratch,
                                        columns,
                                        row.stored_count,
                                        table,
                                    );
                                    &backfill_scratch
                                } else {
                                    row.values
                                };
                            match visit(values_to_visit) {
                                Ok(stop) => {
                                    if stop {
                                        stopped = true;
                                        return Ok(true);
                                    }
                                }
                                Err(e) => {
                                    visit_err = Some(e);
                                    return Ok(true);
                                }
                            }
                        }
                    } else {
                        deferred.push(row.row_id);
                    }
                    Ok(false)
                })
            })?;
            if let Some(e) = visit_err {
                return Err(e);
            }
            if stopped {
                break;
            }
            // Re-read deferred rows against this partition's heap.
            for row_id in deferred {
                if let Some(values) =
                    self.read_row_columns_at(storage_id, table, row_id, txn, columns)?
                {
                    if visit(&values)? {
                        stopped = true;
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ProjectedCharNormalize {
    out_idx: usize,
    len: Option<usize>,
}

#[derive(Debug, Clone)]
pub(crate) struct ProjectedReadPlan {
    char_normalizes: smallvec::SmallVec<[ProjectedCharNormalize; 4]>,
}

/// Precomputed routing tables for the predicate-first scan path. The
/// scanner performs a two-pass decode per row: first the
/// `predicate_columns` (used by the WHERE), then — only if the row
/// passes — any `output_columns` not already covered (the "missing"
/// set). For each output slot, `output_sources[i]` records whether to
/// take it from the predicate decode buffer (cheaper) or from the
/// missing decode buffer.
///
/// Previously rebuilt inside the heap scanner on every call; now built
/// once here so parallel partition scans share it.
#[derive(Debug)]
pub(crate) struct PredicateFirstReadPlan {
    pub(crate) missing_columns: Vec<usize>,
    pub(crate) output_sources: Vec<OutputSource>,
}

impl PredicateFirstReadPlan {
    pub(crate) fn new(predicate_columns: &[usize], output_columns: &[usize]) -> Self {
        let mut missing_columns: Vec<usize> = Vec::new();
        let mut output_sources: Vec<OutputSource> = Vec::with_capacity(output_columns.len());
        for output_col in output_columns {
            if let Some(pred_idx) = predicate_columns
                .iter()
                .position(|predicate_col| predicate_col == output_col)
            {
                output_sources.push(OutputSource::Predicate(pred_idx));
            } else {
                let missing_idx = missing_columns.len();
                missing_columns.push(*output_col);
                output_sources.push(OutputSource::Missing(missing_idx));
            }
        }
        Self {
            missing_columns,
            output_sources,
        }
    }
}

impl ProjectedReadPlan {
    pub(crate) fn new(columns: &[usize], table: &TableSchema) -> Self {
        let mut char_normalizes = smallvec::SmallVec::new();
        for (out_idx, col_idx) in columns.iter().copied().enumerate() {
            let Some(column) = table.columns.get(col_idx) else {
                continue;
            };
            if let DataTypeKind::Char { len } = column.data_type {
                char_normalizes.push(ProjectedCharNormalize { out_idx, len });
            }
        }
        Self { char_normalizes }
    }

    #[inline]
    fn needs_char_normalization(&self, values: &[Value]) -> bool {
        self.char_normalizes
            .iter()
            .any(|slot| char_value_needs_read_normalize(values.get(slot.out_idx), slot.len))
    }

    #[inline]
    pub(crate) fn needs_adjustment(&self, values: &[Value], _stored_count: usize) -> bool {
        self.needs_char_normalization(values)
    }

    pub(crate) fn apply(
        &self,
        values: &mut [Value],
        _columns: &[usize],
        _stored_count: usize,
        _table: &TableSchema,
    ) {
        // RowCodec resolves absent columns by stable column ID and row layout.
        // A physical position can contain holes and is never a stored ordinal.
        for slot in &self.char_normalizes {
            if let Some(value) = values.get_mut(slot.out_idx) {
                normalize_char_string_value_for_read(value, slot.len);
            }
        }
    }
}

#[cfg(test)]
fn projected_values_for_read<'a>(
    values: &'a [Value],
    columns: &[usize],
    plan: &ProjectedReadPlan,
    stored_count: usize,
    table: &TableSchema,
    scratch: &'a mut Vec<Value>,
) -> &'a [Value] {
    if !plan.needs_adjustment(values, stored_count) {
        return values;
    }
    scratch.clear();
    scratch.extend_from_slice(values);
    plan.apply(scratch, columns, stored_count, table);
    scratch
}

fn normalize_projected_char_values_for_read(
    values: &mut [Value],
    columns: Option<&[usize]>,
    table: &TableSchema,
) {
    match columns {
        Some(columns) => {
            for (out_idx, col_idx) in columns.iter().copied().enumerate() {
                if let (Some(value), Some(column)) =
                    (values.get_mut(out_idx), table.columns.get(col_idx))
                {
                    normalize_char_value_for_read(value, &column.data_type);
                }
            }
        }
        None => {
            for (value, column) in values.iter_mut().zip(table.columns.iter()) {
                normalize_char_value_for_read(value, &column.data_type);
            }
        }
    }
}

fn normalize_char_value_for_read(value: &mut Value, data_type: &DataTypeKind) {
    let DataTypeKind::Char { len } = data_type else {
        return;
    };
    normalize_char_string_value_for_read(value, *len);
}

/// Active partition storage_id list for a scan over `table`. For
/// partitioned tables, defers to
/// [`crate::engine::executor::partition_router::current_pruned_partitions`]
/// — the planner installs the candidate set right before the scan
/// runs. For unpartitioned tables (or partitioned tables without an
/// active prune set), returns every partition's storage id (or
/// `[table.id]` for unpartitioned).
pub(crate) fn partition_storage_ids_for_scan(table: &TableSchema) -> Vec<u64> {
    let pruned = crate::engine::executor::partition_router::current_pruned_partitions();
    let ids = if !table.is_partitioned() {
        vec![table.id]
    } else {
        let all_ids = table.storage_ids();
        match pruned {
            Some(pruned) => {
                let mut out: Vec<u64> = pruned
                    .into_iter()
                    .filter(|sid| all_ids.contains(sid))
                    .collect();
                out.sort_unstable();
                out.dedup();
                out
            }
            None => all_ids,
        }
    };
    // Tick the per-thread diagnostic counter once per partition we
    // are about to open. Tests use this to verify pruning kicked in.
    for _ in &ids {
        crate::engine::executor::partition_router::record_partition_visit();
    }
    ids
}

fn normalize_char_string_value_for_read(value: &mut Value, len: Option<usize>) {
    match value {
        Value::String(text) => {
            crate::storage::schema::normalize_char_string_for_read(text, len);
        }
        Value::SmallString(b) => {
            let compact_len = crate::util::pad_space::trim_trailing_spaces_len(b.as_slice());
            b.truncate(compact_len);
        }
        _ => {}
    }
}

fn char_value_needs_read_normalize(value: Option<&Value>, _len: Option<usize>) -> bool {
    let bytes = match value {
        Some(Value::String(text)) => text.as_bytes(),
        Some(Value::SmallString(b)) => b.as_slice(),
        _ => return false,
    };
    crate::util::pad_space::trim_trailing_spaces_len(bytes) != bytes.len()
}
