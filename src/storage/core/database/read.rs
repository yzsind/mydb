impl Database {
    pub fn read_row(
        &self,
        table: &TableSchema,
        row_id: RowId,
        txn: TxnView,
    ) -> Result<Option<StoredRow>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        // For partitioned tables a `row_id` is only meaningful in the
        // context of one partition's heap. Locate the owning partition
        // by probing each in turn; the first match wins. For
        // unpartitioned tables this short-circuits to `table.id`.
        let storage_id = self.locate_row_storage_id(table, row_id)?;
        self.read_row_at(table, storage_id, row_id, txn)
    }

    /// Like [`read_row`], but with the owning partition's `storage_id`
    /// already known by the caller (e.g. multi-table DML that scanned the
    /// row per-partition). Skips the ambiguous `locate_row_storage_id`
    /// probe — essential for partitioned tables where a bare `row_id` can
    /// collide across partitions.
    pub(crate) fn read_row_at(
        &self,
        table: &TableSchema,
        storage_id: u64,
        row_id: RowId,
        txn: TxnView,
    ) -> Result<Option<StoredRow>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let Some(row) = self.read_mvcc_row(storage_id, row_id)? else {
            return Ok(None);
        };
        let Some(mut visible) = self.visible_row(row, txn)? else {
            return Ok(None);
        };
        // Pad short rows out to the latest schema length so callers
        // (DML, projections, expression evaluation) observe column
        // values for INSTANT-added columns even when the underlying
        // heap row pre-dates the ADD COLUMN.
        Self::pad_full_row_to_schema(&mut visible.values, table);
        crate::engine::perf_events::record_rows_examined(1);
        Ok(Some(visible))
    }

    /// Visibility-only fast path for INLJ on SEMI / ANTI joins (Fix D).
    ///
    /// SEMI/ANTI never project right-side values — they just need to
    /// know whether *any* visible row matches each outer key. When the
    /// index bounds already enforce every join predicate (residual is
    /// `None`), we don't have to decode the row payload at all: we just
    /// need a yes/no on visibility.
    ///
    /// Reuses a pre-cached heap handle (Fix B) and goes through the
    /// header-only buffered path that decodes just the 18-byte MVCC
    /// header instead of the full row. Falls back to the full
    /// `read_row_with_handle` only when the buffered header isn't
    /// authoritative (e.g. visible version lives on the undo chain).
    pub(crate) fn is_row_id_visible_with_handle(
        &self,
        heap: &HeapHandle,
        table: &TableSchema,
        row_id: RowId,
        txn: TxnView,
    ) -> Result<bool, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let heap_read = heap.read();
        match self.is_row_id_visible_buffered_in_heap(&heap_read, row_id, txn)? {
            Some(visible) => Ok(visible),
            None => {
                // Visible version may live on the undo chain. Drop the
                // shared read guard before falling back so the slower
                // path can manage its own locking.
                drop(heap_read);
                Ok(self
                    .read_row_with_handle(heap, table, row_id, txn)?
                    .is_some())
            }
        }
    }

    /// Hot-path `read_row` for callers (e.g. INLJ) that have already
    /// cached the heap handle. Skips the per-call `heap_cache` HashMap
    /// + name normalize cycle that `read_mvcc_row` would otherwise pay.
    pub(crate) fn read_row_with_handle(
        &self,
        heap: &HeapHandle,
        table: &TableSchema,
        row_id: RowId,
        txn: TxnView,
    ) -> Result<Option<StoredRow>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let row = {
            let heap_read = heap.read();
            heap_read.with_lob(|| -> Result<Option<MvccStoredRow>, QueryError> {
                Ok(match heap_read.read_mvcc_cached(row_id)? {
                    CachedMvccRead::Hit(row) => Some(row),
                    CachedMvccRead::NotFound => None,
                    CachedMvccRead::MissingPage => heap_read.read_mvcc(row_id)?,
                })
            })?
        };
        let Some(row) = row else { return Ok(None) };
        let Some(mut visible) = self.visible_row(row, txn)? else {
            return Ok(None);
        };
        Self::pad_full_row_to_schema(&mut visible.values, table);
        Ok(Some(visible))
    }

    pub(crate) fn read_row_columns_with_handle(
        &self,
        heap: &HeapHandle,
        table: &TableSchema,
        row_id: RowId,
        txn: TxnView,
        columns: &[usize],
    ) -> Result<Option<Vec<Value>>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let projected_plan = ProjectedReadPlan::new(columns, table);
        let projected = {
            let heap_read = heap.read();
            heap_read.with_lob(
                || -> Result<Option<(RowHeader, Vec<Value>, usize)>, QueryError> {
                    Ok(match heap_read.read_mvcc_columns_cached(row_id, columns)? {
                        CachedProjectedRead::Hit(header, values, stored_count) => {
                            Some((header, values, stored_count))
                        }
                        CachedProjectedRead::NotFound => None,
                        CachedProjectedRead::MissingPage => {
                            heap_read.read_mvcc_columns(row_id, columns)?
                        }
                    })
                },
            )?
        };
        let Some((header, mut values, stored_count)) = projected else {
            return Ok(None);
        };
        if self.row_version_visible(header, txn)? {
            if header.is_deleted() {
                return Ok(None);
            }
            if projected_plan.needs_adjustment(&values, stored_count) {
                projected_plan.apply(&mut values, columns, stored_count, table);
            }
            crate::engine::perf_events::record_rows_examined(1);
            return Ok(Some(values));
        }
        let Some(row) = self.read_row_with_handle(heap, table, row_id, txn)? else {
            return Ok(None);
        };
        Ok(Some(
            columns
                .iter()
                .map(|idx| row.values.get(*idx).cloned().unwrap_or(Value::Null))
                .collect(),
        ))
    }

    /// Non-covering range scan / INLJ inner batch read path.
    /// Reads MVCC headers under one shared heap lock, prefetches
    /// pages ahead of the cursor, and consults `row_version_visible`
    /// per row — no external visibility bitmap is used here, so
    /// racing-DML bytes cannot leak through a stale short-circuit.
    pub(crate) fn read_row_columns_many_with_handle(
        &self,
        heap: &HeapHandle,
        table: &TableSchema,
        row_ids: &[RowId],
        txn: TxnView,
        columns: &[usize],
    ) -> Result<Vec<(RowId, Vec<Value>)>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let projected_plan = ProjectedReadPlan::new(columns, table);
        self.read_row_columns_many_with_handle_and_plan(
            heap,
            table,
            row_ids,
            txn,
            columns,
            None,
            &projected_plan,
        )
    }

    /// Decode one resident non-covering-index window directly into a typed
    /// vector sink. `Some(rows)` means every authoritative current version
    /// was consumed (deleted/vacant rows are skipped). `None` asks the caller
    /// to discard its partially-filled sink and retry the complete window via
    /// [`Self::read_row_columns_many_with_handle`].
    ///
    /// The all-or-fallback contract is important: it preserves index order
    /// without retaining borrowed page payloads or constructing a RowId map,
    /// while invisible top versions, forwarding rows, cold pages and old row
    /// layouts still go through the existing MVCC/default-value oracle.
    pub(crate) fn read_row_columns_many_into_sink<S: crate::storage::RowDecodeSink>(
        &self,
        heap: &HeapHandle,
        row_ids: &[RowId],
        txn: TxnView,
        columns: &[usize],
        sink: &mut S,
    ) -> Result<Option<usize>, QueryError> {
        if row_ids.is_empty() {
            return Ok(Some(0));
        }
        let mut vis_cache = ScanVisibilityCache::new();
        let mut rows = 0usize;
        let heap_read = heap.read();
        let complete = heap_read.with_lob(|| {
            heap_read.visit_mvcc_payloads_many_cached_with_plan(row_ids, columns, |row| {
                if !self.row_version_visible_cached(&mut vis_cache, row.header, txn)? {
                    return Ok(false);
                }
                if row.header.is_deleted() {
                    return Ok(true);
                }
                let decoded = crate::storage::decode_row_into_sink_fast(
                    &row.payload[row.body_offset..],
                    row.codec,
                    row.layout_id,
                    row.plan,
                    sink,
                    row.has_external,
                )?;
                if decoded {
                    rows += 1;
                }
                Ok(decoded)
            })
        })?;
        if complete {
            crate::engine::perf_events::record_rows_examined(rows as u64);
        }
        Ok(complete.then_some(rows))
    }

    pub(crate) fn read_row_columns_many_with_handle_and_plan(
        &self,
        heap: &HeapHandle,
        table: &TableSchema,
        row_ids: &[RowId],
        txn: TxnView,
        columns: &[usize],
        decode_plan: Option<&RowDecodePlan>,
        projected_plan: &ProjectedReadPlan,
    ) -> Result<Vec<(RowId, Vec<Value>)>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(row_ids.len());
        let mut fallback = Vec::new();
        // A batch normally contains many rows created by the same loader
        // transaction.  Resolving every header independently repeatedly
        // takes the sharded transaction-slot RwLock for the same txn id.
        // Keep the scan-local direct-mapped memo alive across both the
        // buffer-pool hit pass and the raw-page fallback pass.
        let mut vis_cache = ScanVisibilityCache::new();
        {
            let heap_read = heap.read();
            let mut hits = Vec::with_capacity(row_ids.len());
            heap_read.with_lob(|| {
                heap_read.read_mvcc_columns_many_cached_with_optional_plan(
                    row_ids,
                    columns,
                    decode_plan,
                    &mut hits,
                    &mut fallback,
                )
            })?;
            for (row_id, header, mut values, stored_count) in hits {
                if self.row_version_visible_cached(&mut vis_cache, header, txn)? {
                    if !header.is_deleted() {
                        if projected_plan.needs_adjustment(&values, stored_count) {
                            projected_plan.apply(&mut values, columns, stored_count, table);
                        }
                        out.push((row_id, values));
                    }
                } else {
                    fallback.push(row_id);
                }
            }
        }
        // Sliding-window heap prefetch + page-batched decode under a
        // single shared heap lock. The old loop took one `heap.read()`
        // per row (inside `read_row_columns_with_handle`) plus a separate
        // re-acquisition every 64 rows just to drive `prefetch_page` —
        // typical 1k-row cold fallback paid ~1k `RwLock` acquisitions
        // before even reaching the page decoder. The two-pass shape
        // below holds one shared guard for the whole raw read, batches
        // by `page_id` so consecutive same-page reads collapse onto one
        // pinned frame, and drops the guard before any visibility check
        // — so the rare undo-walk fallback can re-enter
        // `read_row_with_handle` without nested-lock hazards.
        if !fallback.is_empty() {
            fallback.sort_unstable_by_key(|row_id| (row_id.page_id, row_id.slot_id));
            fallback.dedup();
            let raw_reads: Vec<(RowId, RowHeader, Vec<Value>, usize)> = {
                let heap_read = heap.read();
                let mut out_local = Vec::with_capacity(fallback.len());
                heap_read.with_lob(|| -> Result<(), QueryError> {
                    if heap_read.try_read_mvcc_columns_dense_probe(
                        &fallback,
                        columns,
                        decode_plan,
                        &mut out_local,
                    )? {
                        return Ok(());
                    }
                    let mut unique_pages: Vec<u64> = fallback
                        .iter()
                        .map(|row_id| row_id.page_id as u64)
                        .collect();
                    unique_pages.dedup();
                    let ordered_mrr = !crate::util::env::var("MYDB_MRR_SORT")
                        .map(|value| {
                            matches!(
                                value.trim().to_ascii_lowercase().as_str(),
                                "0" | "off" | "false" | "no" | "disable" | "disabled"
                            )
                        })
                        .unwrap_or(false);
                    let load_window = heap_read.ordered_probe_window_pages();
                    let mut page_group = 0usize;
                    let mut loaded_from = 0usize;
                    let mut loaded_through = 0usize;
                    let mut i = 0;
                    while i < fallback.len() {
                        if page_group == loaded_through {
                            let target = (page_group + load_window).min(unique_pages.len());
                            if ordered_mrr {
                                loaded_from = page_group;
                                heap_read
                                    .load_ordered_probe_pages(&unique_pages[page_group..target])?;
                            } else {
                                heap_read.prefetch_probe_pages(&unique_pages[page_group..target]);
                            }
                            loaded_through = target;
                        }
                        let first_page = fallback[i].page_id;
                        let mut end = i + 1;
                        while end < fallback.len() && fallback[end].page_id == first_page {
                            end += 1;
                        }
                        heap_read.read_mvcc_columns_probe_page(
                            &fallback[i..end],
                            columns,
                            decode_plan,
                            &mut out_local,
                        )?;
                        i = end;
                        page_group += 1;
                        if ordered_mrr && page_group == loaded_through {
                            heap_read.demote_ordered_probe_pages(
                                &unique_pages[loaded_from..loaded_through],
                            )?;
                        }
                    }
                    Ok(())
                })?;
                out_local
            };
            for (row_id, header, mut values, stored_count) in raw_reads {
                if self.row_version_visible_cached(&mut vis_cache, header, txn)? {
                    if !header.is_deleted() {
                        if projected_plan.needs_adjustment(&values, stored_count) {
                            projected_plan.apply(&mut values, columns, stored_count, table);
                        }
                        out.push((row_id, values));
                    }
                } else if let Some(row) = self.read_row_with_handle(heap, table, row_id, txn)? {
                    out.push((
                        row_id,
                        columns
                            .iter()
                            .map(|idx| row.values.get(*idx).cloned().unwrap_or(Value::Null))
                            .collect(),
                    ));
                }
            }
        }
        crate::engine::perf_events::record_rows_examined(out.len() as u64);
        Ok(out)
    }

    /// Predicate-first counterpart of `read_row_columns_many_with_handle`.
    /// The predicate projection is decoded first; output-only columns are
    /// decoded from the same pinned heap payload only for accepted rows.
    pub(crate) fn read_row_columns_many_predicate_first_with_handle<P>(
        &self,
        heap: &HeapHandle,
        table: &TableSchema,
        row_ids: &[RowId],
        txn: TxnView,
        predicate_columns: &[usize],
        output_columns: &[usize],
        mut predicate: P,
    ) -> Result<Vec<(RowId, Vec<Value>)>, QueryError>
    where
        P: FnMut(&[Value]) -> Result<bool, QueryError>,
    {
        self.check_primary_read_view(table, txn)?;
        let examined = crate::engine::perf_events::RowsExaminedBatch::new_index();
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        debug_assert!(row_ids.windows(2).all(|pair| pair[0] <= pair[1]));

        let predicate_read_plan = ProjectedReadPlan::new(predicate_columns, table);
        let output_read_plan = ProjectedReadPlan::new(output_columns, table);
        let routing = PredicateFirstReadPlan::new(predicate_columns, output_columns);
        let mut vis_cache = ScanVisibilityCache::new();
        let mut predicate_backfill = Vec::new();
        let mut hits = Vec::with_capacity(row_ids.len());
        let mut deferred = Vec::new();
        {
            let heap_read = heap.read();
            heap_read.with_lob(|| {
                heap_read.read_mvcc_columns_many_predicate_first(
                    row_ids,
                    predicate_columns,
                    output_columns,
                    &routing.missing_columns,
                    &routing.output_sources,
                    |_row_id, header, values, stored_count| {
                        if !self.row_version_visible_cached(&mut vis_cache, header, txn)? {
                            return Ok(PredicateFirstDecision::Defer);
                        }
                        if header.is_deleted() {
                            return Ok(PredicateFirstDecision::Reject);
                        }
                        examined.add(1);
                        let values = if predicate_read_plan.needs_adjustment(values, stored_count) {
                            predicate_backfill.clear();
                            predicate_backfill.extend_from_slice(values);
                            predicate_read_plan.apply(
                                &mut predicate_backfill,
                                predicate_columns,
                                stored_count,
                                table,
                            );
                            predicate_backfill.as_slice()
                        } else {
                            values
                        };
                        Ok(if predicate(values)? {
                            PredicateFirstDecision::Accept
                        } else {
                            PredicateFirstDecision::Reject
                        })
                    },
                    &mut hits,
                    &mut deferred,
                )
            })?;
        }

        let mut out = Vec::with_capacity(hits.len());
        for (row_id, _header, mut values, stored_count) in hits {
            if output_read_plan.needs_adjustment(&values, stored_count) {
                output_read_plan.apply(&mut values, output_columns, stored_count, table);
            }
            out.push((row_id, values));
        }

        // Forwarding rows and rows whose visible version lives on an undo
        // chain retain the established, fully general read semantics.
        if !deferred.is_empty() {
            deferred.sort_unstable();
            deferred.dedup();
            let predicate_rows = self.read_row_columns_many_with_handle(
                heap,
                table,
                &deferred,
                txn,
                predicate_columns,
            )?;
            let mut accepted = Vec::new();
            for (row_id, values) in predicate_rows {
                if predicate(&values)? {
                    accepted.push(row_id);
                }
            }
            if !accepted.is_empty() {
                out.extend(self.read_row_columns_many_with_handle(
                    heap,
                    table,
                    &accepted,
                    txn,
                    output_columns,
                )?);
            }
        }
        Ok(out)
    }

    /// Resident-page predicate-first probe that exposes the narrow predicate
    /// projection as a borrowed payload. `Some(rows)` means every candidate
    /// was handled under the current-layout/current-version fast-path;
    /// `None` asks the caller to discard the partial result and retry through
    /// [`Self::read_row_columns_many_predicate_first_with_handle`].
    ///
    /// This is the random-RowId counterpart of
    /// `scan_rows_payloads_predicate_first_storage_streaming_range`: string
    /// predicates can compare page bytes in place, and output-only columns
    /// are decoded from the same pinned payload only for accepted rows.
    pub(crate) fn read_row_columns_many_predicate_first_payload_with_handle<P>(
        &self,
        heap: &HeapHandle,
        table: &TableSchema,
        row_ids: &[RowId],
        txn: TxnView,
        predicate_columns: &[usize],
        output_columns: &[usize],
        mut predicate: P,
    ) -> Result<Option<Vec<(RowId, Vec<Value>)>>, QueryError>
    where
        P: for<'a> FnMut(crate::storage::PayloadStreamHit<'a>) -> Result<Option<bool>, QueryError>,
    {
        self.check_primary_read_view(table, txn)?;
        if row_ids.is_empty() {
            return Ok(Some(Vec::new()));
        }
        debug_assert!(row_ids.windows(2).all(|pair| pair[0] <= pair[1]));

        let output_read_plan = ProjectedReadPlan::new(output_columns, table);
        let mut output_decode_plan = None;
        let mut output_values = Vec::with_capacity(output_columns.len());
        let mut rows = Vec::new();
        let mut vis_cache = ScanVisibilityCache::new();
        let heap_read = heap.read();
        let complete = heap_read.with_lob(|| {
            heap_read.visit_mvcc_payloads_many_cached_with_plan(row_ids, predicate_columns, |row| {
                if row.layout_id != table.active_layout_id
                    || !self.row_version_visible_cached(&mut vis_cache, row.header, txn)?
                {
                    return Ok(false);
                }
                if row.header.is_deleted() {
                    return Ok(true);
                }
                let hit = crate::storage::PayloadStreamHit::Payload(
                    crate::storage::core::PayloadHitPayload {
                        payload: &row.payload[row.body_offset..],
                        codec: row.codec,
                        plan: row.plan,
                        layout_id: row.layout_id,
                        has_external: row.has_external,
                    },
                );
                let Some(keep) = predicate(hit)? else {
                    return Ok(false);
                };
                if !keep {
                    return Ok(true);
                }

                let plan = output_decode_plan.get_or_insert_with(|| {
                    crate::storage::RowDecodePlan::compile_with_codec(output_columns, row.codec)
                });
                output_values.clear();
                let stored_count =
                    crate::storage::decode_row_schema_columns_into_with_plan_ext_for_layout(
                        &row.payload[row.body_offset..],
                        row.codec,
                        row.layout_id,
                        plan,
                        &mut output_values,
                        row.has_external,
                    )?;
                if output_read_plan.needs_adjustment(&output_values, stored_count) {
                    output_read_plan.apply(&mut output_values, output_columns, stored_count, table);
                }
                rows.push((row.row_id, std::mem::take(&mut output_values)));
                output_values = Vec::with_capacity(output_columns.len());
                Ok(true)
            })
        })?;
        Ok(complete.then_some(rows))
    }

    /// Score every candidate under the authoritative snapshot without copying
    /// resident fixed VECTOR payloads. Cold/forwarded/invisible/old-layout
    /// windows use the ordinary undo-aware projected reader in bounded batches.
    pub(crate) fn score_vector_rows_many_with_handle(
        &self,
        heap: &HeapHandle,
        table: &TableSchema,
        row_ids: &[RowId],
        txn: TxnView,
        column: usize,
        query: &crate::vector::VectorValue,
        metric: crate::engine::vector::Metric,
    ) -> Result<Vec<(RowId, f64)>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let DataTypeKind::Vector {
            datatype,
            dim: Some(dimension),
        } = table.columns[column].data_type
        else {
            return Err(crate::engine::vector::vector_err(
                "candidate scoring requires a fixed VECTOR column",
            ));
        };
        let _metadata =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_recoverable(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                row_ids.len().saturating_mul(
                    std::mem::size_of::<RowId>() + std::mem::size_of::<(RowId, f64)>(),
                ),
            )?;
        let mut ordered = row_ids.to_vec();
        ordered.sort_unstable();
        ordered.dedup();
        let mut out = Vec::with_capacity(ordered.len());
        let mut vis_cache = ScanVisibilityCache::new();
        for window in ordered.chunks(64) {
            crate::engine::cancel::check_cancelled()?;
            let base = out.len();
            let complete = {
                let heap_read = heap.read();
                heap_read.with_lob(|| {
                    heap_read.visit_mvcc_payloads_many_cached_with_plan(window, &[column], |row| {
                        if row.layout_id != table.active_layout_id
                            || !self.row_version_visible_cached(&mut vis_cache, row.header, txn)?
                        {
                            return Ok(false);
                        }
                        if row.header.is_deleted() {
                            return Ok(true);
                        }
                        let body = &row.payload[row.body_offset..];
                        let Some(range) = fixed_column_byte_range_in_body(
                            body,
                            row.codec,
                            row.layout_id,
                            row.has_external,
                            column,
                        )?
                        else {
                            return Ok(false);
                        };
                        if range.width != datatype.payload_len(dimension) {
                            return Ok(false);
                        }
                        let value = crate::vector::VectorRef {
                            datatype,
                            dimension,
                            payload: &body
                                [range.offset_in_payload..range.offset_in_payload + range.width],
                        };
                        crate::engine::vector::validate_internal(&value)?;
                        let score = crate::engine::vector::distance::typed_ordering_distance(
                            query, &value, metric,
                        )?;
                        out.push((row.row_id, score));
                        Ok(true)
                    })
                })?
            };
            if complete {
                crate::engine::perf_events::record_rows_examined((out.len() - base) as u64);
                continue;
            }
            out.truncate(base);
            let _fallback_scratch =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_recoverable(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    window
                        .len()
                        .saturating_mul(datatype.payload_len(dimension).saturating_add(128)),
                )?;
            for (row, values) in
                self.read_row_columns_many_with_handle(heap, table, window, txn, &[column])?
            {
                if let Some(Value::Vector(value)) = values.first().filter(
                    |v| matches!(v, Value::Vector(value) if value.dimension == query.dimension),
                ) {
                    out.push((
                        row,
                        crate::engine::vector::distance::typed_ordering_distance(
                            query, value, metric,
                        )?,
                    ));
                }
            }
        }
        Ok(out)
    }

    /// Batch full-row counterpart of `read_row_with_handle`. Keeps the
    /// same page-local fast path and visibility semantics, but returns
    /// schema-aligned `StoredRow`s so callers that need the whole row
    /// can avoid a separate visibility pass followed by per-row reads.
    pub(crate) fn read_rows_many_with_handle(
        &self,
        heap: &HeapHandle,
        table: &TableSchema,
        row_ids: &[RowId],
        txn: TxnView,
    ) -> Result<Vec<StoredRow>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let columns: Vec<usize> = (0..table.columns.len()).collect();
        self.read_row_columns_many_with_handle(heap, table, row_ids, txn, &columns)
            .map(|rows| {
                rows.into_iter()
                    .map(|(row_id, values)| StoredRow { row_id, values })
                    .collect()
            })
    }

    pub fn is_row_id_visible(
        &self,
        table: &TableSchema,
        row_id: RowId,
        txn: TxnView,
    ) -> Result<bool, QueryError> {
        self.check_primary_read_view(table, txn)?;
        Ok(self.read_row_columns(table, row_id, txn, &[])?.is_some())
    }

    /// Variant of [`is_row_id_visible_buffered`] that takes an
    /// already-locked `HeapFile` reference, letting the caller hoist
    /// the per-table heap handle lookup and the heap `RwLock` read
    /// guard out of a hot inner loop.
    ///
    /// Equally important, this path decodes only the 18-byte MVCC
    /// row header instead of the full row — for an index-only scan
    /// the values are already in the index entry, so paying to
    /// materialise them from the heap was pure overhead. On a wide
    /// table (e.g. TPC-H `lineitem`'s 16 columns) this avoids
    /// roughly an order of magnitude of per-row CPU.
    fn is_row_id_visible_buffered_in_heap(
        &self,
        heap: &HeapFile,
        row_id: RowId,
        txn: TxnView,
    ) -> Result<Option<bool>, QueryError> {
        match heap.read_mvcc_header_cached(row_id)? {
            CachedMvccHeaderRead::Hit(header) => {
                if self.row_version_visible(header, txn)? {
                    Ok(Some(!header.is_deleted()))
                } else {
                    // Latest cached version not visible to this txn.
                    // The visible version may live on the undo chain;
                    // signal `None` so the caller falls back to the
                    // full lookup which can walk undo / disk pages.
                    Ok(None)
                }
            }
            CachedMvccHeaderRead::NotFound => Ok(Some(false)),
            CachedMvccHeaderRead::MissingPage => Ok(None),
        }
    }

    /// Visibility filter sized for a covering / index-only scan. Locks
    /// the table's heap once, then walks `entries` checking visibility
    /// against a single shared header-only fast path. Returns a `bool`
    /// vector parallel to `entries` (`true` = visible to `txn`).
    ///
    /// This collapses the per-row `heap_handle` lookups +
    /// `RwLock<HeapFile>` acquires + full row decodes that the
    /// per-row API was paying for on a TPC-H lineitem scan into a
    /// single handle lookup and per-row header-only inspection. On a
    /// covering query like `SELECT count(l_shipdate) FROM lineitem`,
    /// this is the change that makes `INDEX_FFS` actually beat a full
    /// table scan.
    pub fn filter_visible_index_entries_mask(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        entries: &[(IndexKey, RowId)],
        txn: TxnView,
    ) -> Result<Vec<bool>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let heap = self.heap_handle(table.id)?;
        self.filter_visible_index_entries_mask_with_handle(&heap, table, index, entries, txn)
    }

    pub(crate) fn filter_visible_index_entries_mask_with_handle(
        &self,
        heap: &HeapHandle,
        table: &TableSchema,
        index: &IndexSchema,
        entries: &[(IndexKey, RowId)],
        txn: TxnView,
    ) -> Result<Vec<bool>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let suspects = match self.index_entry_suspects(table, index) {
            Some(IndexEntrySuspects::All) => {
                return self.filter_visible_index_entries_mask_exact(table, index, entries, txn)
            }
            Some(IndexEntrySuspects::Rows(rows)) => Some(rows),
            None => None,
        };
        let row_ids: Vec<RowId> = entries.iter().map(|(_, row_id)| *row_id).collect();
        let mut mask =
            self.filter_visible_via_heap_with_handle(heap, table, &row_ids, txn, |row_id, idx| {
                self.is_index_entry_version_visible(table, index, &entries[idx].0, row_id, txn)
            })?;
        if let Some(suspects) = suspects {
            for (visible, (key, row_id)) in mask.iter_mut().zip(entries) {
                if *visible && suspects.contains(row_id) {
                    *visible =
                        self.is_index_entry_version_visible(table, index, key, *row_id, txn)?;
                }
            }
        }
        Ok(mask)
    }

    /// Borrowed-entries variant of `filter_visible_index_entries_mask_with_handle`
    /// used by the covering-index aggregate pipeline. Same algorithm
    /// but the chunk lives inside a pinned B-tree leaf and only lends
    /// its `IndexKey`s — passing the owned-key signature would force a
    /// per-row clone that visibility filtering doesn't need.
    pub(crate) fn filter_visible_index_entries_refs_mask_with_handle(
        &self,
        heap: &HeapHandle,
        table: &TableSchema,
        index: &IndexSchema,
        entries: &[(&IndexKey, RowId)],
        txn: TxnView,
    ) -> Result<Vec<bool>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let suspects = match self.index_entry_suspects(table, index) {
            Some(IndexEntrySuspects::All) => {
                return entries
                    .iter()
                    .map(|(key, row_id)| {
                        self.is_index_entry_version_visible(table, index, key, *row_id, txn)
                    })
                    .collect()
            }
            Some(IndexEntrySuspects::Rows(rows)) => Some(rows),
            None => None,
        };
        let row_ids: Vec<RowId> = entries.iter().map(|(_, row_id)| *row_id).collect();
        let mut mask =
            self.filter_visible_via_heap_with_handle(heap, table, &row_ids, txn, |row_id, idx| {
                self.is_index_entry_version_visible(table, index, entries[idx].0, row_id, txn)
            })?;
        if let Some(suspects) = suspects {
            for (visible, (key, row_id)) in mask.iter_mut().zip(entries) {
                if *visible && suspects.contains(row_id) {
                    *visible =
                        self.is_index_entry_version_visible(table, index, key, *row_id, txn)?;
                }
            }
        }
        Ok(mask)
    }

    /// The latest-header shortcut proves an index entry only while the entry
    /// belongs to the latest version. A key-moving writer that is still
    /// registered, or whose rollback the caller's read overlapped (see
    /// [`ExactIndexVisibility`]), can leave entries of versions this snapshot
    /// never saw next to a heap row whose restored header is visible.
    /// While a writer is registered only the rows it retired keys of can
    /// carry such entries; a read that overlapped a rollback trusts none.
    fn index_entry_suspects(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> Option<IndexEntrySuspects> {
        if ExactIndexVisibility::active() {
            return Some(IndexEntrySuspects::All);
        }
        if !self.has_pending_index_repairs_for(table.id, &index.name) {
            return None;
        }
        let rows: HashSet<RowId> = self
            .index_repair_state
            .candidates(table.id, &index.name, None, None, |_| true)
            .into_iter()
            .map(|candidate| candidate.row_id)
            .collect();
        (!rows.is_empty()).then_some(IndexEntrySuspects::Rows(rows))
    }

    /// [`Self::filter_visible_index_entries_mask_with_handle`] without the
    /// latest-header shortcut: every entry is checked against the key of the
    /// row version `txn` sees. Used for reads that overlapped an index-key
    /// writer, whose chunk may hold keys of versions this snapshot never saw.
    pub(crate) fn filter_visible_index_entries_mask_exact(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        entries: &[(IndexKey, RowId)],
        txn: TxnView,
    ) -> Result<Vec<bool>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        entries
            .iter()
            .map(|(key, row_id)| {
                self.is_index_entry_version_visible(table, index, key, *row_id, txn)
            })
            .collect()
    }

    /// A row's visible older version must actually own this index key.
    pub(crate) fn is_index_entry_version_visible(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: &IndexKey,
        row_id: RowId,
        txn: TxnView,
    ) -> Result<bool, QueryError> {
        let Some(row) = self.read_row(table, row_id, txn)? else {
            return Ok(false);
        };
        if index.is_fulltext() || index.has_multi_valued_parts() {
            Ok(table
                .index_keys(index, &row.values)?
                .iter()
                .any(|candidate| candidate == key))
        } else {
            Ok(table.index_key(index, &row.values)? == *key)
        }
    }

    /// Raw integer covering streams retain their allocation-free leaf decoder;
    /// only an invisible latest version needs a decoded key from the undo row.
    pub(crate) fn filter_visible_index_i64_entries_mask_with_handle(
        &self,
        heap: &HeapHandle,
        table: &TableSchema,
        index: &IndexSchema,
        entries: &[(Option<i64>, RowId)],
        key_position: usize,
        txn: TxnView,
    ) -> Result<Vec<bool>, QueryError> {
        let row_ids: Vec<RowId> = entries.iter().map(|entry| entry.1).collect();
        self.filter_visible_via_heap_with_handle(heap, table, &row_ids, txn, |row_id, idx| {
            let Some(row) = self.read_row(table, row_id, txn)? else {
                return Ok(false);
            };
            let key = table.index_key(index, &row.values)?;
            let value = match key.0.get(key_position) {
                Some(KeyPart::Null) => None,
                Some(KeyPart::Int(v) | KeyPart::Time(v) | KeyPart::DateTime(v)) => Some(*v),
                Some(KeyPart::UInt(v)) => i64::try_from(*v).ok(),
                Some(KeyPart::Date(v)) => Some(i64::from(*v)),
                Some(KeyPart::Year(v)) => Some(if *v == 0 { 0 } else { 1900 + i64::from(*v) }),
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "unsupported key in integer index visibility filter",
                    ))
                }
            };
            Ok(value == entries[idx].0)
        })
    }

    /// Row-presence visibility only. Index-only consumers must also compare
    /// the visible version's key via the index-entry variants above.
    /// Walks the batched buffered-header path with
    /// sliding-window prefetch, falling back to the full heap+undo
    /// lookup only on cache miss or non-visible cached header.
    pub(crate) fn filter_visible_row_ids_mask_with_handle(
        &self,
        heap: &HeapHandle,
        table: &TableSchema,
        row_ids: &[RowId],
        txn: TxnView,
    ) -> Result<Vec<bool>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.filter_visible_via_heap_with_handle(heap, table, row_ids, txn, |row_id, _| {
            self.is_row_id_visible(table, row_id, txn)
        })
    }

    /// Heap-side visibility batch: buffered header fast path + sliding
    /// prefetch, with per-row disk fallback when the cached header
    /// doesn't resolve visibility. `row_ids` may be the full input in
    /// arbitrary key order. The caller's fallback distinguishes row presence
    /// from index-key visibility without cloning keys on the header fast path.
    fn filter_visible_via_heap_with_handle(
        &self,
        heap: &HeapHandle,
        table: &TableSchema,
        row_ids: &[RowId],
        txn: TxnView,
        fallback: impl Fn(RowId, usize) -> Result<bool, QueryError>,
    ) -> Result<Vec<bool>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let mut visible = Vec::with_capacity(row_ids.len());
        if row_ids.is_empty() {
            return Ok(visible);
        }
        // Sliding-window heap-page prefetch. The buffered fast path
        // costs ~nothing on a cache hit, but a miss stalls the caller
        // until the page is paged in synchronously. Queueing the
        // distinct heap pages for upcoming entries ahead of the
        // cursor lets the buffer pool's I/O threads stream those
        // MVCC headers in parallel — same trick `HeapFile::prefetch_after`
        // applies on a sequential heap scan.
        let prefetch_window = {
            let r = heap.read();
            r.prefetch_window_pages()
        };
        let mut prefetch_cursor = 0usize;
        let mut last_prefetched_page: Option<u64> = None;
        let initial_horizon = prefetch_window.saturating_mul(2).min(row_ids.len());
        if initial_horizon > 0 {
            let r = heap.read();
            for rid in &row_ids[..initial_horizon] {
                let page_id = rid.page_id as u64;
                if last_prefetched_page != Some(page_id) {
                    r.prefetch_page(page_id);
                    last_prefetched_page = Some(page_id);
                }
            }
            prefetch_cursor = initial_horizon;
        }

        let mut vis_cache = ScanVisibilityCache::new();
        let mut header_scratch = Vec::new();
        let mut idx = 0;
        while idx < row_ids.len() {
            let heap_read = heap.read();
            let mut fallback_row: Option<RowId> = None;
            while idx < row_ids.len() {
                if prefetch_cursor < row_ids.len()
                    && prefetch_cursor.saturating_sub(idx) < prefetch_window
                {
                    let new_to = prefetch_cursor
                        .saturating_add(prefetch_window)
                        .min(row_ids.len());
                    for rid in &row_ids[prefetch_cursor..new_to] {
                        let page_id = rid.page_id as u64;
                        if last_prefetched_page != Some(page_id) {
                            heap_read.prefetch_page(page_id);
                            last_prefetched_page = Some(page_id);
                        }
                    }
                    prefetch_cursor = new_to;
                }

                let page_id = row_ids[idx].page_id;
                let start = idx;
                let mut end = idx + 1;
                while end < row_ids.len() && row_ids[end].page_id == page_id {
                    end += 1;
                }

                heap_read.read_mvcc_headers_cached_same_page_into(
                    &row_ids[start..end],
                    &mut header_scratch,
                )?;
                for (offset, header_read) in header_scratch.drain(..).enumerate() {
                    let row_id = row_ids[start + offset];
                    match self.visible_from_cached_header(header_read, txn, &mut vis_cache)? {
                        Some(v) => {
                            visible.push(v);
                            idx += 1;
                        }
                        None => {
                            fallback_row = Some(row_id);
                            break;
                        }
                    }
                }
                if fallback_row.is_some() {
                    break;
                }
            }
            if idx >= row_ids.len() {
                break;
            }
            drop(heap_read);
            let row_id = fallback_row.unwrap_or(row_ids[idx]);
            visible.push(fallback(row_id, idx)?);
            idx += 1;
        }
        Ok(visible)
    }

    fn visible_from_cached_header(
        &self,
        header_read: CachedMvccHeaderRead,
        txn: TxnView,
        vis_cache: &mut ScanVisibilityCache,
    ) -> Result<Option<bool>, QueryError> {
        match header_read {
            CachedMvccHeaderRead::Hit(header) => {
                if self.row_version_visible_cached(vis_cache, header, txn)? {
                    Ok(Some(!header.is_deleted()))
                } else {
                    Ok(None)
                }
            }
            CachedMvccHeaderRead::NotFound => Ok(Some(false)),
            CachedMvccHeaderRead::MissingPage => Ok(None),
        }
    }

    pub fn read_row_columns(
        &self,
        table: &TableSchema,
        row_id: RowId,
        txn: TxnView,
        columns: &[usize],
    ) -> Result<Option<Vec<Value>>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        // For partitioned tables the storage_id depends on which
        // partition the row lives in. `locate_row_storage_id` finds
        // it by probing each partition; unpartitioned tables
        // short-circuit to `table.id`.
        let storage_id = self.locate_row_storage_id(table, row_id)?;
        self.read_row_columns_at(storage_id, table, row_id, txn, columns)
    }

    /// Partition-aware variant of [`Self::read_row_columns`]. `storage_id`
    /// names the partition's physical heap. For an unpartitioned table
    /// this equals `table.id` and the function collapses to the
    /// previous behaviour.
    pub(crate) fn read_row_columns_at(
        &self,
        storage_id: u64,
        table: &TableSchema,
        row_id: RowId,
        txn: TxnView,
        columns: &[usize],
    ) -> Result<Option<Vec<Value>>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        self.read_row_columns_at_with_plan(storage_id, table, row_id, txn, columns, None, None)
    }

    pub(crate) fn read_row_columns_at_with_plan(
        &self,
        storage_id: u64,
        table: &TableSchema,
        row_id: RowId,
        txn: TxnView,
        columns: &[usize],
        decode_plan: Option<&RowDecodePlan>,
        projected_plan: Option<&ProjectedReadPlan>,
    ) -> Result<Option<Vec<Value>>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let Some((header, mut values, stored_count)) =
            self.read_mvcc_row_columns_with_plan(storage_id, row_id, columns, decode_plan)?
        else {
            return Ok(None);
        };
        if self.row_version_visible(header, txn)? {
            if header.is_deleted() {
                return Ok(None);
            }
            // Substitute catalog DEFAULTs for projected columns whose
            // physical slot is past the row's stored extent — the row
            // pre-dates an INSTANT ADD COLUMN.
            if let Some(plan) = projected_plan {
                if plan.needs_adjustment(&values, stored_count) {
                    plan.apply(&mut values, columns, stored_count, table);
                }
            } else {
                Self::backfill_projected_with_defaults(&mut values, columns, stored_count, table);
            }
            return Ok(Some(values));
        }
        // Top version is invisible to this snapshot — fall through to
        // the full-row read which walks the undo chain. `read_row`
        // already pads to the latest schema, so a positional
        // `row.values.get(idx)` after that always returns a coherent
        // value (DEFAULT or otherwise).
        let Some(row) = self.read_row_at(table, storage_id, row_id, txn)? else {
            return Ok(None);
        };
        Ok(Some(
            columns
                .iter()
                .map(|idx| row.values.get(*idx).cloned().unwrap_or(Value::Null))
                .collect(),
        ))
    }

    /// Read a row's externalized-column descriptors `(physical_pos,
    /// LobRef)` without fetching any chain bytes (installs a
    /// `LobRefCollector` instead of the overflow reader). Used by
    /// UPDATE/DELETE to capture the old version's chains into undo and
    /// by INSERT rollback to learn which chains a row owns. Returns an
    /// empty vec when the row is gone or has no off-page columns.
    fn read_row_external_refs(
        &self,
        table_id: u64,
        row_id: RowId,
    ) -> Result<Vec<(usize, LobRef)>, QueryError> {
        let heap = self.heap_handle(table_id)?;
        let heap_read = heap.read();
        let collector = std::sync::Arc::new(LobRefCollector::new());
        let installed: std::sync::Arc<dyn LobMaterializer> = collector.clone();
        with_lob_materializer(Some(installed), || heap_read.read_mvcc(row_id))?;
        Ok(collector.take())
    }

    /// M2 (partial materialization) — fetch the externalized chain descriptor of a
    /// row column (if that column is externalized) without reading chain bytes.
    /// `None` means the column is not externalized (the caller takes the normal fetch path); `Some(lob)` means externalized, so len / range can be pushed down onto it.
    // The next wave of SQL-layer pushdown will call these helpers; currently test-driven.
    #[cfg(test)]
    fn row_column_external_ref(
        &self,
        table_id: u64,
        row_id: RowId,
        col_pos: usize,
    ) -> Result<Option<LobRef>, QueryError> {
        let refs = self.read_row_external_refs(table_id, row_id)?;
        Ok(refs
            .into_iter()
            .find(|(i, _)| *i == col_pos)
            .map(|(_, lob)| lob))
    }

    #[cfg(test)]
    /// M2 (partial materialization) — byte-length pushdown for externalized columns:
    /// on a hit, return `LobRef.total_len` directly with **zero overflow page reads**
    /// (the storage-side pushdown entry for `LENGTH/OCTET_LENGTH`). `None` means the
    /// column is not externalized (the caller applies plain `LENGTH` to the inline value).
    pub(crate) fn lob_column_byte_length(
        &self,
        table_id: u64,
        row_id: RowId,
        col_pos: usize,
    ) -> Result<Option<u64>, QueryError> {
        match self.row_column_external_ref(table_id, row_id, col_pos)? {
            Some(lob) => {
                // materialize_len returns total_len by default with zero page reads.
                let reader = OverflowReader::new(self.overflow_handle(table_id)?);
                Ok(Some(reader.materialize_len(col_pos, lob)?))
            }
            None => Ok(None),
        }
    }

    #[cfg(test)]
    /// M2 (partial materialization) — byte-range pushdown for externalized columns:
    /// on a hit, read only the pages covering `[offset, offset+len)` (the storage-side
    /// pushdown entry for `SUBSTRING/LEFT/RIGHT`). `kind` decides the resulting Value shape. `None` means not externalized.
    pub(crate) fn lob_column_substring(
        &self,
        table_id: u64,
        row_id: RowId,
        col_pos: usize,
        kind: &ColumnKind,
        offset: u64,
        len: u64,
    ) -> Result<Option<Value>, QueryError> {
        match self.row_column_external_ref(table_id, row_id, col_pos)? {
            Some(lob) => {
                let reader = OverflowReader::new(self.overflow_handle(table_id)?);
                Ok(Some(
                    reader.materialize_range(col_pos, kind, lob, offset, len)?,
                ))
            }
            None => Ok(None),
        }
    }

    /// Snapshot-aware SQL projection entry for LENGTH/OCTET_LENGTH on TEXT and
    /// BLOB columns. The common current-version path decodes the heap row with
    /// a descriptor collector and returns `LobRef.total_len`; an older-snapshot
    /// hit deliberately falls back to the normal materializer as the semantic
    /// oracle because its descriptor may live in undo.
    pub(crate) fn lob_column_byte_length_visible(
        &self,
        table: &TableSchema,
        row_id: RowId,
        txn: TxnView,
        col_pos: usize,
    ) -> Result<Option<Value>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let heap = self.heap_handle(table.id)?;
        let heap_read = heap.read();
        let collector = std::sync::Arc::new(LobRefCollector::new());
        let installed: std::sync::Arc<dyn LobMaterializer> = collector.clone();
        let current = with_lob_materializer(Some(installed), || heap_read.read_mvcc(row_id))?;
        drop(heap_read);
        let Some(current) = current else {
            return Ok(None);
        };
        if self.row_version_visible(current.header, txn)? {
            if current.header.is_deleted() {
                return Ok(None);
            }
            if let Some((_, lob)) = collector
                .take()
                .into_iter()
                .find(|(col, _)| *col == col_pos)
            {
                return Ok(Some(Value::Int(lob.total_len.min(i64::MAX as u64) as i64)));
            }
            let value = current.values.get(col_pos).cloned().unwrap_or(Value::Null);
            return Ok(Some(match value {
                Value::Null => Value::Null,
                Value::String(value) | Value::Decimal(value) => Value::Int(value.len() as i64),
                Value::SmallString(value) => Value::Int(value.len() as i64),
                Value::Bytes(value) => Value::Int(value.len() as i64),
                other => Value::Int(crate::engine::expr::value_bytes(&other).len() as i64),
            }));
        }
        let Some(values) = self.read_row_columns(table, row_id, txn, &[col_pos])? else {
            return Ok(None);
        };
        let value = values.into_iter().next().unwrap_or(Value::Null);
        Ok(Some(match value {
            Value::Null => Value::Null,
            Value::String(value) | Value::Decimal(value) => Value::Int(value.len() as i64),
            Value::SmallString(value) => Value::Int(value.len() as i64),
            Value::Bytes(value) => Value::Int(value.len() as i64),
            other => Value::Int(crate::engine::expr::value_bytes(&other).len() as i64),
        }))
    }

    /// Snapshot-aware byte slice for BLOB/VARBINARY SQL projections. External
    /// current versions read only the requested chain pages; inline and undo
    /// versions use the existing byte-slice oracle.
    pub(crate) fn lob_column_binary_range_visible(
        &self,
        table: &TableSchema,
        row_id: RowId,
        txn: TxnView,
        col_pos: usize,
        offset: u64,
        len: u64,
    ) -> Result<Option<Value>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        let heap = self.heap_handle(table.id)?;
        let heap_read = heap.read();
        let collector = std::sync::Arc::new(LobRefCollector::new());
        let installed: std::sync::Arc<dyn LobMaterializer> = collector.clone();
        let current = with_lob_materializer(Some(installed), || heap_read.read_mvcc(row_id))?;
        drop(heap_read);
        let Some(current) = current else {
            return Ok(None);
        };
        if self.row_version_visible(current.header, txn)? {
            if current.header.is_deleted() {
                return Ok(None);
            }
            if let Some((_, lob)) = collector
                .take()
                .into_iter()
                .find(|(col, _)| *col == col_pos)
            {
                let reader = OverflowReader::new(self.overflow_handle(table.id)?);
                return Ok(Some(reader.materialize_range(
                    col_pos,
                    &ColumnKind::VarBytes,
                    lob,
                    offset,
                    len,
                )?));
            }
            let value = current.values.get(col_pos).cloned().unwrap_or(Value::Null);
            return Ok(Some(match value {
                Value::Null => Value::Null,
                Value::Bytes(bytes) => {
                    let start = (offset as usize).min(bytes.len());
                    let end = start.saturating_add(len as usize).min(bytes.len());
                    Value::Bytes(bytes[start..end].to_vec())
                }
                other => slice_value_bytes(&other, offset, len),
            }));
        }
        let Some(values) = self.read_row_columns(table, row_id, txn, &[col_pos])? else {
            return Ok(None);
        };
        let value = values.into_iter().next().unwrap_or(Value::Null);
        Ok(Some(match value {
            Value::Null => Value::Null,
            Value::Bytes(bytes) => {
                let start = (offset as usize).min(bytes.len());
                let end = start.saturating_add(len as usize).min(bytes.len());
                Value::Bytes(bytes[start..end].to_vec())
            }
            other => slice_value_bytes(&other, offset, len),
        }))
    }

    /// Read a row's values (externalized columns left as `Value::Null`)
    /// together with its chain descriptors, in a single collector-backed
    /// decode that never fetches LOB bytes. Used by INSERT rollback,
    /// which needs the row's non-LOB values for index-delete keys and
    /// its chains to free.
    fn read_row_collecting_external(
        &self,
        table_id: u64,
        row_id: RowId,
    ) -> Result<Option<(Vec<Value>, Vec<(usize, LobRef)>)>, QueryError> {
        let heap = self.heap_handle(table_id)?;
        let heap_read = heap.read();
        let collector = std::sync::Arc::new(LobRefCollector::new());
        let installed: std::sync::Arc<dyn LobMaterializer> = collector.clone();
        let row = with_lob_materializer(Some(installed), || heap_read.read_mvcc(row_id))?;
        Ok(row.map(|r| (r.values, collector.take())))
    }

    fn read_mvcc_row(
        &self,
        table_id: u64,
        row_id: RowId,
    ) -> Result<Option<MvccStoredRow>, QueryError> {
        let heap = self.heap_handle(table_id)?;
        let heap_read = heap.read();
        // Try the buffer-pool-cached fast path first; fall back to the
        // full read which fetches the page through the buffer pool. Both
        // paths run under a shared lock — pages are loaded via
        // `BufferPool::pin_page_with_intent`, which has its own internal
        // synchronization, so no write lock on `HeapFile` is needed.
        heap_read.with_lob(|| -> Result<Option<MvccStoredRow>, QueryError> {
            match heap_read.read_mvcc_cached(row_id)? {
                CachedMvccRead::Hit(row) => Ok(Some(row)),
                CachedMvccRead::NotFound => Ok(None),
                CachedMvccRead::MissingPage => heap_read.read_mvcc(row_id),
            }
        })
    }

    pub(crate) fn read_mvcc_row_columns_with_plan(
        &self,
        table_id: u64,
        row_id: RowId,
        columns: &[usize],
        decode_plan: Option<&RowDecodePlan>,
    ) -> Result<Option<(RowHeader, Vec<Value>, usize)>, QueryError> {
        let heap = self.heap_handle(table_id)?;
        let heap_read = heap.read();
        heap_read.with_lob(
            || -> Result<Option<(RowHeader, Vec<Value>, usize)>, QueryError> {
                let cached = match decode_plan {
                    Some(plan) => {
                        heap_read.read_mvcc_columns_cached_with_plan(row_id, columns, plan)?
                    }
                    None => heap_read.read_mvcc_columns_cached(row_id, columns)?,
                };
                match cached {
                    CachedProjectedRead::Hit(header, values, stored_count) => {
                        Ok(Some((header, values, stored_count)))
                    }
                    CachedProjectedRead::NotFound => Ok(None),
                    CachedProjectedRead::MissingPage => match decode_plan {
                        Some(plan) => heap_read.read_mvcc_columns_uncached_with_plan(row_id, plan),
                        None => heap_read.read_mvcc_columns(row_id, columns),
                    },
                }
            },
        )
    }

    #[cfg(test)]
    pub fn lock_row_for_update(
        &self,
        txn: TxnView,
        table: &TableSchema,
        row_id: RowId,
    ) -> Result<(), QueryError> {
        self.check_primary_read_view(table, txn)?;
        let Some(row) = self.with_heap_mut(table.id, |heap| heap.read_mvcc(row_id))? else {
            return Ok(());
        };
        self.ensure_write_allowed(txn, table.id, &row)?;
        self.lock_row_nonblocking(txn.txn_id, table.id, row_id)
    }

    /// Partition-safe counterpart used by ordered DML before firing row
    /// triggers or capturing RETURNING images. The wait completes before the
    /// executor re-reads and rechecks WHERE.
    pub fn lock_row_for_update_at(
        &self,
        txn: TxnView,
        table: &TableSchema,
        storage_id: u64,
        row_id: RowId,
    ) -> Result<(), QueryError> {
        self.check_primary_read_view(table, txn)?;
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        let Some(row) = self.with_heap_mut(storage_id, |heap| heap.read_mvcc(row_id))? else {
            return Ok(());
        };
        self.ensure_write_allowed(txn, storage_id, &row)?;
        self.lock_row_nonblocking(txn.txn_id, storage_id, row_id)
    }

    pub fn lock_rows_for_update(
        &self,
        txn: TxnView,
        table: &TableSchema,
        row_refs: &[crate::storage::DmlRowRef],
    ) -> Result<(), QueryError> {
        self.check_primary_read_view(table, txn)?;
        if row_refs.is_empty() {
            return Ok(());
        }
        // M2.2 - Acquire IX lock at table level for SELECT FOR UPDATE.
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        if !table.is_partitioned() {
            let mut existing = Vec::with_capacity(row_refs.len());
            for row_ref in row_refs {
                debug_assert_eq!(row_ref.storage_id, table.id);
                if self.read_mvcc_row(table.id, row_ref.row_id)?.is_some() {
                    existing.push(row_ref.row_id);
                }
            }
            return self.lock_rows_blocking(txn.txn_id, table.id, existing);
        }
        let mut existing = std::collections::BTreeMap::<u64, Vec<RowId>>::new();
        for row_ref in row_refs {
            if self
                .read_mvcc_row(row_ref.storage_id, row_ref.row_id)?
                .is_some()
            {
                existing
                    .entry(row_ref.storage_id)
                    .or_default()
                    .push(row_ref.row_id);
            }
        }
        for (storage_id, row_ids) in existing {
            self.lock_rows_blocking(txn.txn_id, storage_id, row_ids)?;
        }
        Ok(())
    }

    /// Single-candidate counterpart for a unique-index point read. The caller
    /// re-reads and revalidates the row after the lock, so an eager full-row
    /// existence decode and a one-element batch Vec add no correctness. A
    /// stale/reused RowId is harmless: it is locked, then rejected by the
    /// caller's current-row key check.
    pub(crate) fn lock_row_id_for_update_blocking(
        &self,
        txn: TxnView,
        table: &TableSchema,
        row_id: RowId,
    ) -> Result<(), QueryError> {
        self.check_primary_read_view(table, txn)?;
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        self.acquire_row_lock_blocking(txn.txn_id, table.id, row_id, LockMode::Exclusive)
    }

    /// M1.3 - Lock rows in shared mode for `SELECT ... FOR SHARE`.
    pub fn lock_rows_for_share(
        &self,
        txn: TxnView,
        table: &TableSchema,
        row_refs: &[crate::storage::DmlRowRef],
    ) -> Result<(), QueryError> {
        self.check_primary_read_view(table, txn)?;
        if row_refs.is_empty() {
            return Ok(());
        }
        // IS table lock for FOR SHARE
        if txn.txn_id != 0 {
            self.acquire_table_lock_sharded(txn.txn_id, table.id, TableLockMode::IntentShared)?;
        }
        if !table.is_partitioned() {
            let mut existing = Vec::with_capacity(row_refs.len());
            for row_ref in row_refs {
                debug_assert_eq!(row_ref.storage_id, table.id);
                if self.read_mvcc_row(table.id, row_ref.row_id)?.is_some() {
                    existing.push(row_ref.row_id);
                }
            }
            return self.lock_rows_blocking_mode(txn.txn_id, table.id, existing, LockMode::Shared);
        }
        let mut existing = std::collections::BTreeMap::<u64, Vec<RowId>>::new();
        for row_ref in row_refs {
            if self
                .read_mvcc_row(row_ref.storage_id, row_ref.row_id)?
                .is_some()
            {
                existing
                    .entry(row_ref.storage_id)
                    .or_default()
                    .push(row_ref.row_id);
            }
        }
        for (storage_id, row_ids) in existing {
            self.lock_rows_blocking_mode(txn.txn_id, storage_id, row_ids, LockMode::Shared)?;
        }
        Ok(())
    }

    /// M1.3 - Try to acquire row locks with NOWAIT semantics.
    /// Returns error 3572 immediately if any row is held by another transaction.
    pub fn try_acquire_row_locks_nowait(
        &self,
        txn: TxnView,
        table: &TableSchema,
        row_refs: &[crate::storage::DmlRowRef],
        mode: LockMode,
    ) -> Result<(), QueryError> {
        self.check_primary_read_view(table, txn)?;
        if row_refs.is_empty() {
            return Ok(());
        }
        if txn.txn_id != 0 {
            let table_mode = match mode {
                LockMode::Shared => TableLockMode::IntentShared,
                LockMode::Exclusive => TableLockMode::IntentExclusive,
            };
            self.acquire_table_lock_sharded(txn.txn_id, table.id, table_mode)?;
        }
        for row_ref in row_refs {
            let storage_id = row_ref.storage_id;
            let row_id = row_ref.row_id;
            if self.read_mvcc_row(storage_id, row_id)?.is_none() {
                continue;
            }
            if !self.try_row_lock_without_wait(txn.txn_id, storage_id, row_id, mode)? {
                return Err(error(crate::error::catalog::ER_LOCK_NOWAIT,
                    "Statement aborted because lock(s) could not be acquired immediately and NOWAIT is set",
                ));
            }
        }
        Ok(())
    }

    /// M1.3 - Try-lock rows for SKIP LOCKED variant; returns the subset that was successfully locked.
    pub fn try_lock_rows_skip_locked(
        &self,
        txn: TxnView,
        table: &TableSchema,
        row_refs: &[crate::storage::DmlRowRef],
        mode: LockMode,
    ) -> Result<Vec<crate::storage::DmlRowRef>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        if row_refs.is_empty() {
            return Ok(Vec::new());
        }
        if txn.txn_id != 0 {
            let table_mode = match mode {
                LockMode::Shared => TableLockMode::IntentShared,
                LockMode::Exclusive => TableLockMode::IntentExclusive,
            };
            self.acquire_table_lock_sharded(txn.txn_id, table.id, table_mode)?;
        }
        let mut acquired = Vec::with_capacity(row_refs.len());
        for &row_ref in row_refs {
            let storage_id = row_ref.storage_id;
            let row_id = row_ref.row_id;
            if self.read_mvcc_row(storage_id, row_id)?.is_none() {
                continue;
            }
            // A single atomic try-acquire never creates waiter state. Only
            // conflicts are skipped; resource and storage errors propagate.
            if self.try_row_lock_without_wait(txn.txn_id, storage_id, row_id, mode)? {
                acquired.push(row_ref);
            }
        }
        Ok(acquired)
    }
}

enum IndexEntrySuspects {
    All,
    Rows(HashSet<RowId>),
}

thread_local! {
    static EXACT_INDEX_VISIBILITY: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Scope in which index-entry visibility masks check every entry's key
/// against the snapshot's row version. A reader enters it when its physical
/// read overlapped a key-moving writer's rollback.
pub(crate) struct ExactIndexVisibility(());

impl ExactIndexVisibility {
    pub(crate) fn enter() -> Self {
        EXACT_INDEX_VISIBILITY.with(|depth| depth.set(depth.get() + 1));
        Self(())
    }

    fn active() -> bool {
        EXACT_INDEX_VISIBILITY.with(|depth| depth.get() != 0)
    }
}

impl Drop for ExactIndexVisibility {
    fn drop(&mut self) {
        EXACT_INDEX_VISIBILITY.with(|depth| depth.set(depth.get() - 1));
    }
}
