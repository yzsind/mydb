// Bound heap latch ownership independently of table size and worker count.
const ANALYZE_SCAN_BATCH_PAGES: u64 = 32;

/// A non-waiting, short population barrier. Writers retain IX through
/// deferred commit completion, including publication of signed row deltas.
/// No transaction slot or WAL is needed for this synthetic lock owner.
struct AnalyzePopulationBarrier<'a> {
    db: &'a Database,
    owner: u64,
}

impl<'a> AnalyzePopulationBarrier<'a> {
    fn try_new(db: &'a Database, table: &TableSchema) -> Option<Self> {
        let guard = Self {
            db,
            owner: db.allocate_nontransaction_id(),
        };
        let mut ids = table.storage_ids();
        ids.push(table.id);
        ids.sort_unstable();
        ids.dedup();
        for id in ids {
            match db
                .table_locks_sharded
                .try_acquire(guard.owner, id, TableLockMode::Shared)
            {
                TableAcquireOutcome::Acquired { .. } | TableAcquireOutcome::AlreadyHeld => {}
                TableAcquireOutcome::Conflict { .. } | TableAcquireOutcome::SchemaChanged => {
                    return None
                }
            }
        }
        Some(guard)
    }
}

impl Drop for AnalyzePopulationBarrier<'_> {
    fn drop(&mut self) {
        if !self
            .db
            .table_locks_sharded
            .release_txn(self.owner)
            .is_empty()
        {
            self.db.notify_non_row_lock_waiters();
        }
    }
}

type ColumnStatsBuilders = Vec<(String, ColumnStatsBuilder)>;
type IndexPrefixStatsBuilders = Vec<(String, Vec<DistinctStatsBuilder<IndexKey>>)>;
type GroupStatsBuilders = Vec<(String, DistinctStatsBuilder<IndexKey>)>;
type ConditionalStatsBuilders = Vec<ConditionalStatsScanBuilder>;
type StatsScanPartial = (
    ColumnStatsBuilders,
    IndexPrefixStatsBuilders,
    GroupStatsBuilders,
    ConditionalStatsBuilders,
    u64,
    Option<AnalyzeZonePartial>,
);

struct ConditionalStatsScanBuilder {
    builder: ConditionalKeyDomainBuilder,
    filter_indices: Vec<usize>,
    key_indices: Vec<usize>,
}

impl ConditionalStatsScanBuilder {
    fn for_table(table: &TableSchema) -> Result<ConditionalStatsBuilders, QueryError> {
        conditional_key_domain_candidates(table)
            .into_iter()
            .map(|(filter_columns, key_columns)| {
                let filter_indices = filter_columns
                    .iter()
                    .map(|column| table.column_index(column))
                    .collect::<Result<Vec<_>, _>>()?;
                let key_indices = key_columns
                    .iter()
                    .map(|column| table.column_index(column))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self {
                    builder: ConditionalKeyDomainBuilder::new_unique(filter_columns, key_columns),
                    filter_indices,
                    key_indices,
                })
            })
            .collect()
    }

    fn record(&mut self, values: &[Value], page_id: u32) {
        if self.builder.is_overflowed() {
            return;
        }
        let mut filter_values = Vec::with_capacity(self.filter_indices.len());
        for &idx in &self.filter_indices {
            let value = values[idx].clone();
            if matches!(value, Value::Null) {
                return;
            }
            filter_values.push(value);
        }
        let mut key_parts = Vec::with_capacity(self.key_indices.len());
        let mut key_value_for_range = None;
        for &idx in &self.key_indices {
            let value = values[idx].clone();
            if matches!(value, Value::Null) {
                return;
            }
            if key_value_for_range.is_none() && self.key_indices.len() == 1 {
                key_value_for_range = Some(value.clone());
            }
            key_parts.push(KeyPart::from_value(&value));
        }
        self.builder.record(
            filter_values,
            IndexKey::from(key_parts),
            key_value_for_range,
            false,
            page_id,
        );
    }

    fn record_captured(&mut self, values: &[Option<Value>], page_id: u32) {
        if self.builder.is_overflowed() {
            return;
        }
        let mut filter_values = Vec::with_capacity(self.filter_indices.len());
        for &idx in &self.filter_indices {
            let value = values[idx]
                .as_ref()
                .expect("typed analyze conditional value")
                .clone();
            if matches!(value, Value::Null) {
                return;
            }
            filter_values.push(value);
        }
        let mut key_parts = Vec::with_capacity(self.key_indices.len());
        let mut key_value_for_range = None;
        for &idx in &self.key_indices {
            let value = values[idx]
                .as_ref()
                .expect("typed analyze conditional value")
                .clone();
            if matches!(value, Value::Null) {
                return;
            }
            if key_value_for_range.is_none() && self.key_indices.len() == 1 {
                key_value_for_range = Some(value.clone());
            }
            key_parts.push(KeyPart::from_value(&value));
        }
        self.builder.record(
            filter_values,
            IndexKey::from(key_parts),
            key_value_for_range,
            false,
            page_id,
        );
    }

    fn merge(&mut self, other: Self) -> Result<(), QueryError> {
        if self.filter_indices != other.filter_indices || self.key_indices != other.key_indices {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "analyze conditional-domain schema changed during scan",
            ));
        }
        self.builder.merge(other.builder);
        Ok(())
    }
}

struct AnalyzeScanGuard<'a> {
    active: &'a AtomicU64,
}

impl Drop for AnalyzeScanGuard<'_> {
    fn drop(&mut self) {
        let previous = self.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "ANALYZE scan counter underflow");
    }
}

struct AnalyzeZonePartial {
    batch: ZoneMapDeltaBatch,
    deferred: Vec<RowId>,
    membership_complete: bool,
    _membership_charges: Vec<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

/// Per-ANALYZE-worker zone accumulator.  Page chunks are contiguous, so at
/// most one zone is live in the hot loop; completed zones are appended to the
/// worker's bounded result and merged after the scoped worker pool joins.
struct AnalyzeZoneAccumulator<'a> {
    table: &'a TableSchema,
    relevant_columns: Vec<(usize, u32)>,
    column_slots: Vec<Option<usize>>,
    collation_ids: Vec<u16>,
    zone_pages: u64,
    build_scn: u64,
    batch: ZoneMapDeltaBatch,
    deferred: Vec<RowId>,
    current_zone: Option<u64>,
    current_rows: u64,
    running: Vec<WorkerColumnState>,
    membership_layouts: Vec<Option<ZoneMembershipLayout>>,
    membership_charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
    text_scratch: Vec<u8>,
    text_lease: ZoneScratchLease,
}

impl<'a> AnalyzeZoneAccumulator<'a> {
    fn new(
        table: &'a TableSchema,
        context: &AnalyzeZoneBuildContext,
        page_lo: u64,
        page_hi: u64,
    ) -> Self {
        let relevant_columns = zone_map_relevant_columns(table);
        let mut column_slots = vec![None; table.columns.len()];
        let collation_ids = relevant_columns
            .iter()
            .enumerate()
            .map(|(slot, (column_idx, _))| {
                column_slots[*column_idx] = Some(slot);
                crate::charset::collation_id(
                    table.columns[*column_idx]
                        .collation
                        .as_deref()
                        .unwrap_or("utf8mb4_0900_ai_ci"),
                )
            })
            .collect::<Vec<_>>();
        let mut membership_columns = zone_map_provisional_membership_columns(table);
        let zone_count = if page_hi > page_lo {
            (page_hi - 1) / u64::from(context.map.zone_pages)
                - page_lo / u64::from(context.map.zone_pages)
                + 1
        } else {
            0
        };
        let membership_bytes = membership_columns.values().fold(0usize, |total, layout| {
            total.saturating_add(
                layout
                    .words
                    .saturating_mul(std::mem::size_of::<u64>())
                    .saturating_mul(zone_count as usize),
            )
        });
        let membership_charge =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                membership_bytes,
            );
        if membership_charge.is_none() && membership_bytes != 0 {
            membership_columns.clear();
            ZONE_MAP_MEMORY_FALLBACKS.fetch_add(1, Ordering::Relaxed);
        }
        let membership_layouts = relevant_columns
            .iter()
            .map(|(_, column_id)| membership_columns.get(column_id).copied())
            .collect::<Vec<_>>();
        let running = relevant_columns
            .iter()
            .zip(&membership_layouts)
            .map(|((_, _), layout)| {
                let mut state = WorkerColumnState::default();
                if let Some(layout) = *layout {
                    state.enable_membership_with_layout(layout);
                }
                state
            })
            .collect();
        Self {
            table,
            relevant_columns,
            column_slots,
            collation_ids,
            zone_pages: u64::from(context.map.zone_pages),
            build_scn: context.build_scn,
            batch: ZoneMapDeltaBatch {
                storage_id: context.storage_id,
                zone_pages: context.map.zone_pages,
                schema_fingerprint: context.map.schema_fingerprint,
                zones: BTreeMap::new(),
            },
            deferred: Vec::new(),
            current_zone: None,
            current_rows: 0,
            running,
            membership_layouts,
            membership_charge,
            text_scratch: Vec::new(),
            text_lease: ZoneScratchLease::default(),
        }
    }

    #[inline]
    fn begin_row(&mut self, row_id: RowId) {
        let zone_id = u64::from(row_id.page_id) / self.zone_pages;
        if self.current_zone != Some(zone_id) {
            self.flush_current();
            self.current_zone = Some(zone_id);
        }
        self.current_rows = self.current_rows.saturating_add(1);
    }

    #[inline]
    fn defer(&mut self, row_id: RowId) {
        self.deferred.push(row_id);
    }

    #[inline]
    fn observe_value(&mut self, column_idx: usize, value: &Value) {
        let Some(slot) = self.column_slots[column_idx] else {
            return;
        };
        self.running[slot].observe(
            &self.table.columns[column_idx],
            self.collation_ids[slot],
            value,
            &mut self.text_scratch,
            &mut self.text_lease,
        );
    }

    #[inline]
    fn observe_typed_value(&mut self, column_idx: usize, value: &Value) {
        let Some(slot) = self.column_slots[column_idx] else {
            return;
        };
        let state = &mut self.running[slot];
        let scalar = match value {
            Value::Null => return state.observe_null(),
            Value::Int(value) => WorkerScalar::Signed(*value),
            Value::UInt(value) => WorkerScalar::Unsigned(*value),
            Value::Float(value) if !value.is_nan() => WorkerScalar::Float32(value.to_bits()),
            Value::Double(value) if !value.is_nan() => WorkerScalar::Float64(value.to_bits()),
            Value::DecimalCompact(magnitude, scale) => WorkerScalar::DecimalCompact {
                mag: i128::from(*magnitude),
                scale: *scale,
            },
            Value::Date(value) => WorkerScalar::Temporal(i64::from(*value)),
            Value::Time(value) | Value::DateTime(value) | Value::Timestamp(value) => {
                WorkerScalar::Temporal(*value)
            }
            Value::Year(value) => WorkerScalar::Temporal(i64::from(*value)),
            _ => return self.observe_value(column_idx, value),
        };
        state.observe_worker_scalar(scalar);
    }

    #[inline]
    fn observe_string(&mut self, column_idx: usize, bytes: &[u8]) {
        let Some(slot) = self.column_slots[column_idx] else {
            return;
        };
        self.running[slot].observe_text_bytes(
            self.collation_ids[slot],
            bytes,
            &mut self.text_scratch,
            &mut self.text_lease,
        );
    }

    #[inline]
    fn observe_bytes(&mut self, column_idx: usize, bytes: &[u8]) {
        let Some(slot) = self.column_slots[column_idx] else {
            return;
        };
        self.running[slot].observe_binary_bytes(bytes);
    }

    fn flush_current(&mut self) {
        let Some(zone_id) = self.current_zone.take() else {
            return;
        };
        let zone = self.batch.zones.entry(zone_id).or_default();
        zone.row_count_estimate = zone.row_count_estimate.saturating_add(self.current_rows);
        zone.rebuild_scn = self.build_scn;
        for (slot, (_, column_id)) in self.relevant_columns.iter().enumerate() {
            let summary = worker_state_into_summary(std::mem::take(&mut self.running[slot]));
            if let Some(layout) = self.membership_layouts[slot] {
                self.running[slot].enable_membership_with_layout(layout);
            }
            match zone.columns.entry(*column_id) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(summary);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    merge_column_summary_into(entry.get_mut(), summary);
                }
            }
        }
        self.current_rows = 0;
    }

    fn finish(mut self) -> AnalyzeZonePartial {
        self.flush_current();
        AnalyzeZonePartial {
            batch: self.batch,
            deferred: self.deferred,
            membership_complete: true,
            _membership_charges: self.membership_charge.into_iter().collect(),
        }
    }
}

fn merge_analyze_zone_partials(
    target: &mut Option<AnalyzeZonePartial>,
    incoming: Option<AnalyzeZonePartial>,
) {
    let Some(mut incoming) = incoming else {
        return;
    };
    let Some(target) = target.as_mut() else {
        *target = Some(incoming);
        return;
    };
    target.deferred.append(&mut incoming.deferred);
    target.membership_complete &= incoming.membership_complete;
    target
        ._membership_charges
        .append(&mut incoming._membership_charges);
    for (zone_id, zone) in incoming.batch.zones {
        match target.batch.zones.entry(zone_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(zone);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let existing = entry.get_mut();
                existing.row_count_estimate = existing
                    .row_count_estimate
                    .saturating_add(zone.row_count_estimate);
                existing.rebuild_scn = existing.rebuild_scn.max(zone.rebuild_scn);
                for (column_id, summary) in zone.columns {
                    match existing.columns.entry(column_id) {
                        std::collections::btree_map::Entry::Vacant(column) => {
                            column.insert(summary);
                        }
                        std::collections::btree_map::Entry::Occupied(mut column) => {
                            merge_column_summary_into(column.get_mut(), summary);
                        }
                    }
                }
            }
        }
    }
}

/// Pre-resolved shape for the common plain B-tree statistics path. The
/// generic `TableSchema::index_keys` path remains the oracle for expression,
/// multi-valued, FULLTEXT and non-BTREE indexes.
struct AnalyzePlainIndexLayout {
    column_indices: Vec<usize>,
    reuse_column_key: Vec<bool>,
    reuse_column_ndv: Vec<bool>,
}

fn analyze_plain_index_layout(
    table: &TableSchema,
    index: &IndexSchema,
) -> Option<AnalyzePlainIndexLayout> {
    if !index.index_type.eq_ignore_ascii_case("BTREE")
        || index.fulltext.is_some()
        || index.has_expression_parts()
        || index.has_multi_valued_parts()
    {
        return None;
    }
    let mut column_indices = Vec::with_capacity(index.columns.len());
    let mut reuse_column_key = Vec::with_capacity(index.columns.len());
    let mut reuse_column_ndv = Vec::with_capacity(index.columns.len());
    for name in &index.columns {
        let idx = table.column_index(name).ok()?;
        let column = table.columns.get(idx)?;
        column_indices.push(idx);
        // Character index keys carry collation/pad semantics that ordinary
        // column statistics deliberately do not. Every other stored scalar
        // has the same canonical KeyPart in both paths.
        let reuse = !matches!(
            column.data_type,
            DataTypeKind::Char { .. }
                | DataTypeKind::VarChar { .. }
                | DataTypeKind::Text { .. }
                | DataTypeKind::Enum { .. }
                | DataTypeKind::Set { .. }
        );
        reuse_column_key.push(reuse);
        let has_prefix = index
            .column_prefix_lengths
            .get(reuse_column_key.len() - 1)
            .copied()
            .flatten()
            .is_some();
        reuse_column_ndv.push(reuse && !column.nullable && !has_prefix);
    }
    Some(AnalyzePlainIndexLayout {
        column_indices,
        reuse_column_key,
        reuse_column_ndv,
    })
}

/// Direct row-codec sink for approximate ANALYZE. Fixed-width values stay as
/// stack scalars; string/binary payloads are borrowed until an accumulator
/// actually retains an extremum or TOP-K candidate.
struct AnalyzeColumnSink<'a, 'z, 't> {
    builders: &'a mut ColumnStatsBuilders,
    keys: &'a mut [Option<KeyPart>],
    key_needed: &'a [bool],
    conditional_values: &'a mut [Option<Value>],
    conditional_value_needed: &'a [bool],
    page_id: u32,
    zone: Option<&'z mut AnalyzeZoneAccumulator<'t>>,
}

impl AnalyzeColumnSink<'_, '_, '_> {
    #[inline]
    fn record_value(&mut self, col: usize, value: Value, key: KeyPart) {
        if let Some(zone) = self.zone.as_mut() {
            zone.observe_typed_value(col, &value);
        }
        if self.conditional_value_needed[col] {
            self.conditional_values[col] = Some(value.clone());
        }
        self.builders[col]
            .1
            .record_approx_typed(value, self.page_id);
        if self.key_needed[col] {
            self.keys[col] = Some(key);
        }
    }
}

impl RowDecodeSink for AnalyzeColumnSink<'_, '_, '_> {
    fn put_null(&mut self, col: usize) {
        if let Some(zone) = self.zone.as_mut() {
            zone.observe_typed_value(col, &Value::Null);
        }
        self.builders[col].1.record_null();
        if self.conditional_value_needed[col] {
            self.conditional_values[col] = Some(Value::Null);
        }
        if self.key_needed[col] {
            self.keys[col] = Some(KeyPart::Null);
        }
    }

    fn put_i64(&mut self, col: usize, value: i64) {
        self.record_value(col, Value::Int(value), KeyPart::Int(value));
    }

    fn put_u64(&mut self, col: usize, value: u64) {
        self.record_value(col, Value::UInt(value), KeyPart::UInt(value));
    }

    fn put_f32(&mut self, col: usize, value: f32) {
        self.record_value(
            col,
            Value::Float(value),
            KeyPart::Float32(f32_to_order_u32(value)),
        );
    }

    fn put_f64(&mut self, col: usize, value: f64) {
        self.record_value(
            col,
            Value::Double(value),
            KeyPart::Float64(f64_to_order_u64(value)),
        );
    }

    fn put_decimal_compact(&mut self, col: usize, magnitude: i64, scale: u8) {
        self.record_value(
            col,
            Value::DecimalCompact(magnitude, scale),
            KeyPart::Decimal {
                value: magnitude as i128,
                scale,
            },
        );
    }

    fn put_decimal_wide(&mut self, col: usize, magnitude: i128, scale: u8) {
        let value = if let Ok(compact) = i64::try_from(magnitude) {
            Value::DecimalCompact(compact, scale)
        } else {
            Value::Decimal(format_decimal_v3(
                magnitude.unsigned_abs(),
                scale,
                magnitude < 0,
            ))
        };
        self.record_value(
            col,
            value,
            KeyPart::Decimal {
                value: magnitude,
                scale,
            },
        );
    }

    fn put_date(&mut self, col: usize, days: i32) {
        self.record_value(col, Value::Date(days), KeyPart::Date(days));
    }

    fn put_year(&mut self, col: usize, year: u8) {
        self.record_value(col, Value::Year(year), KeyPart::Year(year));
    }

    fn put_time(&mut self, col: usize, micros: i64) {
        self.record_value(col, Value::Time(micros), KeyPart::Time(micros));
    }

    fn put_datetime(&mut self, col: usize, micros: i64) {
        self.record_value(col, Value::DateTime(micros), KeyPart::DateTime(micros));
    }

    fn put_bytes(&mut self, col: usize, bytes: &[u8]) {
        if let Some(zone) = self.zone.as_mut() {
            zone.observe_bytes(col, bytes);
        }
        self.builders[col]
            .1
            .record_approx_bytes(bytes, self.page_id);
        if self.key_needed[col] {
            self.keys[col] = Some(KeyPart::Bytes(bytes.to_vec()));
        }
    }

    fn put_string(&mut self, col: usize, bytes: &[u8]) {
        if let Some(zone) = self.zone.as_mut() {
            zone.observe_string(col, bytes);
        }
        self.builders[col]
            .1
            .record_approx_string(bytes, self.page_id);
        if self.key_needed[col] {
            self.keys[col] = Some(KeyPart::String(unsafe {
                String::from_utf8_unchecked(bytes.to_vec())
            }));
        }
    }

    fn put_json(&mut self, col: usize, bytes: &[u8]) {
        self.put_value(col, Value::Json(bytes.to_vec()));
    }

    fn put_geometry(&mut self, col: usize, bytes: &[u8]) {
        self.put_value(col, Value::Geometry(bytes.to_vec()));
    }

    fn put_vector(&mut self, col: usize, bytes: &[u8]) {
        self.put_value(col, Value::Vector(bytes.to_vec().into()));
    }

    fn put_value(&mut self, col: usize, value: Value) {
        let key = KeyPart::from_value(&value);
        self.record_value(col, value, key);
    }
}

const FULLTEXT_STATS_CANDIDATES: usize = 512;
const FULLTEXT_STATS_TOP_K: usize = 128;

fn estimated_analyzed_index_height(entries: u64) -> u64 {
    if entries <= 1 {
        return 1;
    }
    // B-tree pages typically fan out by hundreds of entries. This height is
    // stored alongside the observed physical page count; future index-file
    // metadata can replace the estimate without changing the catalog shape.
    let mut level_entries = entries;
    let mut height = 1u64;
    while level_entries > 256 {
        level_entries = level_entries.div_ceil(256);
        height += 1;
    }
    height
}

#[derive(Default)]
struct FulltextStatsBuilder {
    doc_count: u64,
    total_token_count: u64,
    token_df: std::collections::HashMap<String, u64>,
}

struct FulltextIndexStatsBuilder {
    name: String,
    column_indices: Vec<usize>,
    tokenizer: Box<dyn crate::storage::fulltext::Tokenizer>,
    stats: FulltextStatsBuilder,
}

impl FulltextIndexStatsBuilder {
    fn for_table(table: &TableSchema) -> Result<Vec<Self>, QueryError> {
        let mut out = Vec::new();
        for index in table.indexes.iter().filter(|index| index.is_fulltext()) {
            let default_meta = crate::storage::FulltextMeta::default();
            let meta = index.fulltext.as_ref().unwrap_or(&default_meta);
            out.push(Self {
                name: normalize_name(&index.name),
                column_indices: index
                    .columns
                    .iter()
                    .map(|column| table.column_index(column))
                    .collect::<Result<Vec<_>, _>>()?,
                tokenizer: crate::storage::fulltext::build_tokenizer_from_meta(meta),
                stats: FulltextStatsBuilder::default(),
            });
        }
        Ok(out)
    }

    fn record_row(&mut self, values: &[Value]) {
        let mut tokens = Vec::new();
        for &idx in &self.column_indices {
            let Some(value) = values.get(idx) else {
                continue;
            };
            let text = match value {
                Value::Null => continue,
                Value::String(text) => text.clone(),
                Value::SmallString(bytes) => String::from_utf8_lossy(bytes.as_ref()).into_owned(),
                other => other.to_display_string(),
            };
            tokens.extend(self.tokenizer.tokenize(&text));
        }
        self.stats.record(tokens);
    }
}

impl FulltextStatsBuilder {
    fn record(&mut self, tokens: Vec<String>) {
        self.doc_count = self.doc_count.saturating_add(1);
        self.total_token_count = self.total_token_count.saturating_add(tokens.len() as u64);
        let unique = tokens.into_iter().collect::<std::collections::HashSet<_>>();
        for token in unique {
            if let Some(count) = self.token_df.get_mut(&token) {
                *count = count.saturating_add(1);
            } else if self.token_df.len() < FULLTEXT_STATS_CANDIDATES {
                self.token_df.insert(token, 1);
            } else {
                // Bounded Misra-Gries candidate set: memory stays fixed even
                // for very high-cardinality corpora while frequent terms
                // survive and remain useful for conservative costing.
                self.token_df.retain(|_, count| {
                    *count = count.saturating_sub(1);
                    *count != 0
                });
            }
        }
    }

    fn finish(self, count_scale: f64) -> crate::storage::FulltextStats {
        let avg_token_count = if self.doc_count == 0 {
            0.0
        } else {
            self.total_token_count as f64 / self.doc_count as f64
        };
        let mut top = self
            .token_df
            .into_iter()
            .map(|(token, count)| crate::storage::TokenFrequency {
                token,
                document_count: ((count as f64) * count_scale).round() as u64,
            })
            .collect::<Vec<_>>();
        top.sort_by(|a, b| {
            b.document_count
                .cmp(&a.document_count)
                .then_with(|| a.token.cmp(&b.token))
        });
        top.truncate(FULLTEXT_STATS_TOP_K);
        let mut frequencies = top
            .iter()
            .map(|entry| entry.document_count)
            .collect::<Vec<_>>();
        frequencies.sort_unstable();
        let df_histogram = if frequencies.is_empty() {
            Vec::new()
        } else {
            (1..=8)
                .map(|bucket| {
                    let idx = (frequencies.len() * bucket).div_ceil(8).saturating_sub(1);
                    frequencies[idx.min(frequencies.len() - 1)]
                })
                .collect()
        };
        crate::storage::FulltextStats {
            doc_count: ((self.doc_count as f64) * count_scale).round() as u64,
            avg_token_count,
            token_df_top: top,
            df_histogram,
        }
    }
}

impl Database {
    #[cfg(test)]
    pub(crate) fn install_analyze_test_hook(&self) -> Arc<DdlBackfillTestHook> {
        let hook = Arc::new(DdlBackfillTestHook::new());
        *self.analyze_test_hook.lock().unwrap() = Some(Arc::clone(&hook));
        hook
    }

    #[cfg(test)]
    pub(crate) fn install_analyze_batch_test_hook(&self) -> Arc<DdlBackfillTestHook> {
        let hook = Arc::new(DdlBackfillTestHook::new());
        *self.analyze_batch_test_hook.lock().unwrap() = Some(Arc::clone(&hook));
        hook
    }

    #[cfg(test)]
    fn pause_analyze_batch_for_test(&self) {
        let hook = self.analyze_batch_test_hook.lock().unwrap().take();
        if let Some(hook) = hook {
            hook.pause();
        }
    }

    fn analyze_lock(&self, table_key: &str) -> Arc<parking_lot::Mutex<()>> {
        let mut locks = self.analyze_locks.lock();
        if let Some(lock) = locks.get(table_key).and_then(std::sync::Weak::upgrade) {
            return lock;
        }
        locks.retain(|_, lock| lock.strong_count() != 0);
        let lock = Arc::new(parking_lot::Mutex::new(()));
        locks.insert(table_key.to_owned(), Arc::downgrade(&lock));
        lock
    }

    fn enter_analyze_scan(&self) -> AnalyzeScanGuard<'_> {
        self.analyze_scan_active.fetch_add(1, Ordering::AcqRel);
        AnalyzeScanGuard {
            active: &self.analyze_scan_active,
        }
    }

    fn analyze_scan_is_active(&self) -> bool {
        self.analyze_scan_active.load(Ordering::Acquire) != 0
    }

    fn collect_fulltext_stats(
        &self,
        table: &TableSchema,
        scan_pages: Option<&[u64]>,
        count_scale: f64,
        txn: TxnView,
    ) -> Result<std::collections::BTreeMap<String, crate::storage::FulltextStats>, QueryError> {
        let mut builders = FulltextIndexStatsBuilder::for_table(table)?;
        if builders.is_empty() {
            return Ok(std::collections::BTreeMap::new());
        }
        let pages = match scan_pages {
            Some(pages) => pages.to_vec(),
            None => (0..self.with_heap_read(table.id, |heap| heap.page_count())?).collect(),
        };
        for pages in pages.chunks(ANALYZE_SCAN_BATCH_PAGES as usize) {
            crate::engine::cancel::check_cancelled()?;
            let mut rows = Vec::new();
            self.with_heap_read(table.id, |heap| {
                if let (Some(&lo), Some(&hi)) = (pages.first(), pages.last()) {
                    if hi - lo + 1 == pages.len() as u64 {
                        return heap.scan_mvcc_pages_range_until(lo, hi + 1, |row| {
                            rows.push(row);
                            Ok(false)
                        });
                    }
                }
                heap.scan_mvcc_pages_until(Some(pages), |row| {
                    rows.push(row);
                    Ok(false)
                })
            })?;
            for row in rows {
                if let Some(row) = self.visible_row(row, txn)? {
                    for builder in &mut builders {
                        builder.record_row(&row.values);
                    }
                }
            }
        }
        Ok(builders
            .into_iter()
            .map(|builder| (builder.name, builder.stats.finish(count_scale)))
            .collect())
    }

    #[cfg(test)]
    fn collect_fulltext_stats_from_values(
        table: &TableSchema,
        rows: &[Vec<Value>],
    ) -> Result<std::collections::BTreeMap<String, crate::storage::FulltextStats>, QueryError> {
        let mut builders = FulltextIndexStatsBuilder::for_table(table)?;
        for row in rows {
            for builder in &mut builders {
                builder.record_row(row);
            }
        }
        Ok(builders
            .into_iter()
            .map(|builder| (builder.name, builder.stats.finish(1.0)))
            .collect())
    }

    /// Lock-free fast path for "this DML touched N rows of table T". The
    /// stale-stats check (`should_refresh_stats`) and the planner cost
    /// estimate both read `modified_rows_since_analyze` only as a
    /// rough threshold trigger, so accumulating into a sidecar atomic
    /// and folding the delta into the persisted figure on the next
    /// ANALYZE / stats refresh is observationally indistinguishable
    /// from updating the field eagerly.
    ///
    /// The old eager path (`Arc::make_mut(table_arc).stats.modified_rows_since_analyze +=
    /// modified_rows`) deep-cloned the entire `TableSchema` (columns,
    /// indexes, constraints, top-value sketches, partitioning) on every
    /// INSERT / UPDATE / DELETE — TPC-C profiles showed this alone
    /// accounting for ≈ 1 % CPU just in the `Vec<T>::clone` walk inside
    /// `record_non_temp_table_modifications`. The hot path now takes a
    /// shared read lock, finds or inserts the per-table `AtomicU64`
    /// counter, and `fetch_add`s the delta — no `Arc::make_mut`, no
    /// schema clone.
    /// Fast-path counter bump. Splits the table-id lookup from the
    /// shared-read happy path so the read-mostly atomic increment isn't
    /// gated behind a write lock the way `Arc::make_mut` was.
    pub(crate) fn record_table_modifications_by_id(&self, table_id: u64, modified_rows: u64) {
        if modified_rows == 0 {
            return;
        }
        // CTE result cache: any DML on this table advances its monotonic
        // write epoch so cached results depending on it are invalidated.
        self.bump_table_write_epoch(table_id);
        // Happy path: the table already has a counter — shared read
        // lock + atomic fetch_add. Contention only on first-touch.
        let deltas = self.table_modification_deltas.read();
        if let Some(counter) = deltas.get(&table_id) {
            counter.fetch_add(modified_rows, std::sync::atomic::Ordering::Relaxed);
            return;
        }
        drop(deltas);
        let mut deltas = self.table_modification_deltas.write();
        let counter = deltas
            .entry(table_id)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));
        counter.fetch_add(modified_rows, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record a transaction-local signed population change. The change is
    /// intentionally invisible to planners until `commit_txn` publishes it.
    pub(crate) fn record_pending_live_row_delta(&self, txn_id: u64, table_id: u64, delta: i64) {
        if delta == 0 {
            return;
        }
        let mut pending = self.pending_live_row_deltas
            [txn_id as usize & (PENDING_LIVE_ROW_DELTA_SHARDS - 1)]
            .lock();
        let entry = pending
            .entry(txn_id)
            .or_default()
            .entry(table_id)
            .or_default();
        *entry = entry.saturating_add(delta);
    }

    pub(crate) fn publish_pending_live_row_deltas(&self, txn_id: u64) {
        let committed = {
            let mut pending = self.pending_live_row_deltas
                [txn_id as usize & (PENDING_LIVE_ROW_DELTA_SHARDS - 1)]
                .lock();
            pending.remove(&txn_id).unwrap_or_default()
        };
        for (table_id, delta) in committed {
            let counters = self.table_live_row_deltas.read();
            if let Some(counter) = counters.get(&table_id) {
                counter.fetch_add(delta, std::sync::atomic::Ordering::Relaxed);
                continue;
            }
            drop(counters);
            self.table_live_row_deltas
                .write()
                .entry(table_id)
                .or_insert_with(|| Arc::new(AtomicI64::new(0)))
                .fetch_add(delta, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub(crate) fn discard_pending_live_row_deltas(&self, txn_id: u64) {
        self.pending_live_row_deltas[txn_id as usize & (PENDING_LIVE_ROW_DELTA_SHARDS - 1)]
            .lock()
            .remove(&txn_id);
    }

    pub(crate) fn pending_live_row_deltas_snapshot(
        &self,
        txn_id: u64,
    ) -> smallvec::SmallVec<[(u64, i64); 4]> {
        self.pending_live_row_deltas[txn_id as usize & (PENDING_LIVE_ROW_DELTA_SHARDS - 1)]
            .lock()
            .get(&txn_id)
            .map(|tables| {
                tables
                    .iter()
                    .map(|(&table, &delta)| (table, delta))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn restore_pending_live_row_deltas(&self, txn_id: u64, snapshot: &[(u64, i64)]) {
        let mut pending = self.pending_live_row_deltas
            [txn_id as usize & (PENDING_LIVE_ROW_DELTA_SHARDS - 1)]
            .lock();
        if snapshot.is_empty() {
            pending.remove(&txn_id);
        } else {
            pending.insert(txn_id, snapshot.iter().copied().collect());
        }
    }
    pub(crate) fn table_live_row_delta(&self, table_id: u64) -> i64 {
        self.table_live_row_deltas
            .read()
            .get(&table_id)
            .map(|counter| counter.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Snapshot the sidecar delta for `table_id`, returning the
    /// accumulated rows since the last reset. Does not clear the
    /// counter.
    pub(crate) fn table_modification_delta(&self, table_id: u64) -> u64 {
        let deltas = self.table_modification_deltas.read();
        deltas
            .get(&table_id)
            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Drop the sidecar counter entirely. Used by DROP TABLE so a
    /// recreated table-id doesn't inherit a phantom delta.
    pub(crate) fn drop_table_modification_delta(&self, table_id: u64) {
        // TRUNCATE (via `reset_table_stats`) and DROP reach here; both
        // change the table's data, so advance the monotonic write epoch.
        self.bump_table_write_epoch(table_id);
        self.drop_table_modification_delta_without_epoch(table_id);
    }

    /// DROP removes the table identity altogether, so it must not create a
    /// fresh write epoch while clearing its sidecar counters.
    pub(crate) fn drop_table_modification_delta_without_epoch(&self, table_id: u64) {
        let mut deltas = self.table_modification_deltas.write();
        deltas.remove(&table_id);
        self.table_live_row_deltas.write().remove(&table_id);
        for shard in &self.pending_live_row_deltas {
            shard.lock().retain(|_, tables| {
                tables.remove(&table_id);
                !tables.is_empty()
            });
        }
    }

    /// Bump the monotonic write epoch for `table_id` (CTE result cache
    /// dependency tracking). See the `table_write_epochs` field doc.
    pub(crate) fn bump_table_write_epoch(&self, table_id: u64) {
        let epochs = self.table_write_epochs.read();
        if let Some(counter) = epochs.get(&table_id) {
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
        drop(epochs);
        self.table_write_epochs
            .write()
            .entry(table_id)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Current monotonic write epoch for `table_id` (0 if untouched).
    pub(crate) fn table_write_epoch(&self, table_id: u64) -> u64 {
        self.table_write_epochs
            .read()
            .get(&table_id)
            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Whether every `(table_id, schema_version, write_epoch)` dependency
    /// still matches the live catalog + write epoch. The session CTE result
    /// cache uses this to invalidate an entry exactly when one of the CTE's
    /// base tables changes: DML bumps the epoch, ALTER bumps schema_version,
    /// DROP removes the table (→ false). An empty dependency set means the
    /// body reads no base table (`SELECT 1`) and is always fresh.
    pub(crate) fn cte_dep_versions_match(&self, deps: &[(u64, u64, u64)]) -> bool {
        let schema_ok = {
            let Ok(catalog) = self.catalog.read() else {
                return false;
            };
            deps.iter().all(|(table_id, schema_version, _)| {
                catalog
                    .tables
                    .values()
                    .find(|table| table.id == *table_id)
                    .map(|table| table.schema_version == *schema_version)
                    .unwrap_or(false)
            })
        };
        schema_ok
            && deps
                .iter()
                .all(|(table_id, _, write_epoch)| self.table_write_epoch(*table_id) == *write_epoch)
    }

    fn mark_table_stats_stale(
        &self,
        table_key: &str,
        modified_rows: u64,
    ) -> Result<(), QueryError> {
        let mut catalog = self.catalog_write()?;
        let table_arc = catalog.tables.get_mut(table_key).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_key}' doesn't exist"),
            )
        })?;
        let table = Arc::make_mut(table_arc);
        table.stats.modified_rows_since_analyze = table
            .stats
            .modified_rows_since_analyze
            .saturating_add(modified_rows.max(1));
        Ok(())
    }

    /// Reject explicit persistent statistics writes before changing live state.
    fn ensure_statistics_writable(&self) -> Result<(), QueryError> {
        self.ensure_operational()?;
        self.ensure_writable()?;
        if self.physical_replica_mode() {
            return Err(error(
                crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
                "persistent optimizer statistics are read-only on a physical replica",
            ));
        }
        Ok(())
    }

    /// Recompute a table's optimizer statistics.
    ///
    /// `is_explicit_analyze` controls the optional side effects:
    ///
    /// * Explicit `ANALYZE TABLE` passes `true`. Bumps the schema
    ///   version so cached physical plans built against the old
    ///   statistics are invalidated (plan-cache entries record
    ///   (table_id, schema_version) dependencies — without this a
    ///   plan built on an empty table keeps being reused after the
    ///   table is bulk-loaded), and rebuilds the zone-map sidecar to
    ///   reflect the freshly-committed data.
    ///
    /// * Implicit refresh and background Auto-ANALYZE pass `false`.
    ///   They publish sampled cardinalities and invalidate dependent plans
    ///   just like explicit ANALYZE, but do not rebuild the zone map.
    /// Background Auto-ANALYZE: one pass over every non-system base table,
    /// folding the live modification delta into the staleness check and
    /// re-sampling tables whose accumulated changes exceed `ratio` of their
    /// row count. Runs off the query hot path (driven by the
    /// `mydb-auto-analyze` worker), so a stale-stats refresh never blocks a
    /// user query. Returns the number of tables refreshed.
    ///
    /// Candidate selection holds only a shared catalog read lock and is
    /// released before any re-sampling; each table is then refreshed
    /// independently so a concurrent DROP / ANALYZE racing one table only
    /// skips that table rather than aborting the whole pass.
    pub fn auto_analyze_stale_tables(&self, ratio: f64) -> Result<u64, QueryError> {
        let _role = self.zone_map_gate.read_recursive();
        self.ensure_operational()?;
        if self.physical_replica_mode() || self.is_snapshot_read_only() {
            // Optimizer statistics belong to the source dictionary. Reads use
            // those statistics and conservative estimates without publishing.
            return Ok(0);
        }
        // Phase 1: collect stale candidate keys under a read lock.
        let candidates: Vec<String> = {
            let catalog = self.catalog_read()?;
            catalog
                .tables
                .iter()
                .filter_map(|(key, table)| {
                    if table.system || crate::storage::naming::is_system_database(&table.database) {
                        return None;
                    }
                    let delta = self.table_modification_delta(table.id);
                    if table.stats_needs_auto_analyze(delta, ratio) {
                        Some(key.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        // Phase 2: re-sample each candidate off the catalog read lock.
        let mut refreshed = 0u64;
        for key in candidates {
            // A table may have been dropped or concurrently analyzed
            // between phases; treat any per-table failure as "skip", not
            // "abort the pass".
            match self.refresh_table_stats(&key, false) {
                Ok(true) => {
                    refreshed = refreshed.saturating_add(1);
                }
                Ok(false) | Err(_) => continue,
            }
        }

        if refreshed > 0 {
            self.commit_catalog_txn()?;
        }
        Ok(refreshed)
    }

    fn refresh_table_stats(
        &self,
        table_key: &str,
        is_explicit_analyze: bool,
    ) -> Result<bool, QueryError> {
        self.refresh_table_stats_with_zone_errors(table_key, is_explicit_analyze, false)
    }

    /// OPTIMIZE requires a completed Zone Map refresh. Share ANALYZE's fused
    /// statistics/zone scan, but propagate a failed fallback rebuild instead
    /// of reporting successful physical maintenance with an incomplete map.
    fn refresh_table_stats_with_zone_errors(
        &self,
        table_key: &str,
        is_explicit_analyze: bool,
        require_zone_success: bool,
    ) -> Result<bool, QueryError> {
        let analyze_lock = self.analyze_lock(table_key);
        let _table_analyze = if is_explicit_analyze {
            loop {
                crate::engine::cancel::check_cancelled()?;
                if let Some(guard) = analyze_lock.try_lock_for(Duration::from_millis(10)) {
                    break guard;
                }
            }
        } else {
            let Some(guard) = analyze_lock.try_lock() else {
                return Ok(false);
            };
            guard
        };
        // Low-priority zone-map maintenance may have been deferred by LOAD or
        // bulk DDL and wake just as ANALYZE starts. Keep the guard across the
        // complete stats/zone publication lifecycle so that background pass
        // cancels instead of performing a duplicate heap scan.
        let _analyze_scan = self.enter_analyze_scan();
        // Keep a fused stats/zone scan in one switch generation. Ordinary
        // statistics still run when zone maps are disabled.
        let _zone_operation = self.zone_map_gate.read_recursive();
        if self.physical_replica_mode() || self.is_snapshot_read_only() {
            if is_explicit_analyze {
                self.ensure_statistics_writable()?;
            }
            return Ok(false);
        }
        let table = self
            .catalog_read()?
            .tables
            .get(table_key)
            .map(Arc::clone)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    format!("Table '{table_key}' doesn't exist"),
                )
            })?;
        // Background statistics are optional while an online DDL owns a
        // descriptor. Publishing them would bump the same dependency revision
        // that the DDL validates at its final barrier, aborting a long build.
        if !is_explicit_analyze && self.catalog.online_pinned_ids.read().contains(&table.id) {
            return Ok(false);
        }
        // Fold only counters aligned with this snapshot. A writer's IX lock
        // survives MVCC publication until its live-row delta is published, so
        // this also closes the publish-before-durability completion window.
        // Busy tables do not delay ANALYZE: retain their population baseline.
        let population_barrier = AnalyzePopulationBarrier::try_new(self, &table);
        let baseline_aligned = population_barrier.is_some();
        let modification_baseline = if baseline_aligned {
            self.table_modification_delta(table.id)
        } else {
            0
        };
        let live_baseline = self.table_live_row_delta(table.id);
        let snapshot = StorageReadSnapshot::new(self);
        drop(population_barrier);
        let zone_context = if is_explicit_analyze {
            // Zone publication remains best-effort. Failure to acquire its
            // snapshot must never turn a successful statistics refresh into
            // an ANALYZE error.
            self.begin_analyze_zone_build(&table).ok().flatten()
        } else {
            None
        };
        #[cfg(test)]
        {
            let hook = self.analyze_test_hook.lock().unwrap().take();
            if let Some(hook) = hook {
                hook.pause();
            }
        }
        let collected = self.collect_table_stats_with_sample_pages_and_zone(
            &table,
            None,
            zone_context.as_ref(),
            snapshot.txn,
        );
        // Collected statistics own their values. Release their read horizon
        // before rebuilding zones, whose batches register their own snapshots.
        // Otherwise an unrelated commit can make this finished scan the old
        // reader that prevents every subsequent batch from replacing Unknown.
        drop(snapshot);
        let (mut stats, zone_partial) = match collected {
            Ok(collected) => collected,
            Err(err) => {
                if let Some(context) = zone_context {
                    self.cancel_analyze_zone_build(context);
                }
                return Err(err);
            }
        };
        let publication = (|| {
            if !is_explicit_analyze && self.catalog.online_pinned_ids.read().contains(&table.id) {
                return Ok(false);
            }
            if !baseline_aligned {
                stats.row_count = table.stats.row_count;
            }
            stats.analyzed_at_unix_ms = Some(
                current_unix_ms().max(
                    table
                        .stats
                        .analyzed_at_unix_ms
                        .unwrap_or(0)
                        .saturating_add(1),
                ),
            );
            stats.analyze_revision =
                table.stats.analyze_revision.checked_add(1).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "statistics analysis revision exhausted",
                    )
                })?;
            stats.last_analyze_live_baseline = if baseline_aligned { live_baseline } else { 0 };
            stats.last_analyze_modification_baseline = modification_baseline;
            stats.explicit_histograms = table.stats.explicit_histograms.clone();
            let mut candidate = (*table).clone();
            candidate.stats = stats;
            self.publish_stats_candidate_checked(&table, candidate, !is_explicit_analyze)
        })();
        match publication {
            Ok(true) => {}
            Ok(false) => {
                if let Some(context) = zone_context {
                    self.cancel_analyze_zone_build(context);
                }
                return Ok(false);
            }
            Err(err) => {
                if let Some(context) = zone_context {
                    self.cancel_analyze_zone_build(context);
                }
                return Err(err);
            }
        }

        if is_explicit_analyze {
            let refreshed_table = self.snapshot_table_schema(table_key).ok();
            let zone_table = refreshed_table.as_ref().unwrap_or(&table);
            // Force a GC pass so the watermark reflects all currently
            // committable transactions before we rebuild the zone map.
            self.advance_min_retained_txn_id();
            let zone_result = match (zone_context, zone_partial) {
                (Some(context), Some(partial)) if partial.membership_complete => self
                    .publish_analyze_zone_build(zone_table, context, partial)
                    .or_else(|_| self.rebuild_zone_map_for_table(zone_table)),
                (Some(context), Some(_)) => {
                    self.cancel_analyze_zone_build(context);
                    self.rebuild_zone_map_for_table(zone_table)
                }
                (Some(context), None) => {
                    self.cancel_analyze_zone_build(context);
                    self.rebuild_zone_map_for_table(zone_table)
                }
                (None, _) => self.rebuild_zone_map_for_table(zone_table),
            };
            if require_zone_success {
                zone_result?;
            }
        }
        Ok(true)
    }

    /// Refresh persistent statistics for a selected set of physical leaves
    /// without opening any other partition heap. This is the implementation
    /// behind `ANALYZE PARTITION`; table-level row/page totals are rebuilt
    /// once the catalog has a statistic for every current leaf.
    fn analyze_partition_storage_ids(
        &self,
        table: &TableSchema,
        storage_ids: &[u64],
    ) -> Result<(), QueryError> {
        let _role = self.zone_map_gate.read_recursive();
        self.ensure_statistics_writable()?;
        let analyze_lock = self.analyze_lock(&table.qualified_name());
        let _analyze = analyze_lock.lock();
        let population_barrier = AnalyzePopulationBarrier::try_new(self, table);
        let baseline_aligned = population_barrier.is_some();
        let live_baseline = self.table_live_row_delta(table.id);
        let modification_baseline = if baseline_aligned {
            self.table_modification_delta(table.id)
        } else {
            0
        };
        let txn = self.begin_read_only_txn()?;
        drop(population_barrier);
        let _read_guard = PartitionReadTxnGuard {
            db: self,
            txn_id: txn.txn_id,
        };
        let analyzed_at = Some(current_unix_ms());
        let mut refreshed = BTreeMap::new();
        for &storage_id in storage_ids {
            let page_count = self.with_heap_read(storage_id, |heap| heap.page_count())?;
            // Use the same bounded sketches/sampling policy as ordinary
            // ANALYZE. Never retain a whole physical leaf or exact unbounded
            // prefix-key sets in memory merely because it is partitioned.
            let mut leaf_schema = table.clone();
            leaf_schema.id = storage_id;
            leaf_schema.partitioning = None;
            let (leaf, _) =
                self.collect_table_stats_with_sample_pages_and_zone(&leaf_schema, None, None, txn)?;
            refreshed.insert(
                storage_id,
                PartitionStats {
                    row_count: leaf.row_count,
                    heap_page_count: page_count,
                    columns: leaf.columns,
                    indexes: leaf.indexes,
                    modified_rows_since_analyze: 0,
                    analyzed_at_unix_ms: analyzed_at,
                    check_ok: None,
                },
            );
        }

        let mut candidate = table.clone();
        let stored = &mut candidate;
        stored.stats.analyze_revision =
            table.stats.analyze_revision.checked_add(1).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "statistics analysis revision exhausted",
                )
            })?;
        stored.stats.last_analyze_live_baseline = 0;
        stored.stats.last_analyze_modification_baseline = 0;
        stored.stats.partition_stats.extend(refreshed);
        let current_ids = stored.storage_ids();
        stored
            .stats
            .partition_stats
            .retain(|id, _| current_ids.contains(id));
        if current_ids
            .iter()
            .all(|storage_id| stored.stats.partition_stats.contains_key(storage_id))
        {
            let previous_row_count = stored.stats.row_count;
            let new_row_count: u64 = current_ids
                .iter()
                .filter_map(|storage_id| stored.stats.partition_stats.get(storage_id))
                .map(|stats| stats.row_count)
                .sum();
            if baseline_aligned {
                // Only the part of the live delta now represented by the
                // refreshed leaf counts can be removed. Other leaves may
                // still have unapplied DML in the table-wide sidecar.
                let represented = (new_row_count as i128 - previous_row_count as i128)
                    .clamp(i64::MIN as i128, i64::MAX as i128)
                    as i64;
                let aligned = if represented >= 0 {
                    represented.min(live_baseline.max(0))
                } else {
                    represented.max(live_baseline.min(0))
                };
                stored.stats.last_analyze_live_baseline = aligned;
            }
            stored.stats.row_count = new_row_count;
            stored.stats.heap_page_count = current_ids
                .iter()
                .filter_map(|storage_id| stored.stats.partition_stats.get(storage_id))
                .map(|stats| stats.heap_page_count)
                .sum();
            stored.stats.analyzed_at_unix_ms = Some(
                current_unix_ms().max(
                    stored
                        .stats
                        .analyzed_at_unix_ms
                        .unwrap_or(0)
                        .saturating_add(1),
                ),
            );
            if baseline_aligned && current_ids.iter().all(|id| storage_ids.contains(id)) {
                stored.stats.last_analyze_modification_baseline = modification_baseline;
                stored.stats.modified_rows_since_analyze = 0;
            }
        }
        self.publish_stats_candidate(table, candidate)
    }

    #[cfg(test)]
    fn collect_table_stats_with_sample_pages(
        &self,
        table: &TableSchema,
        sample_pages: Option<&[u64]>,
    ) -> Result<TableStats, QueryError> {
        let snapshot = StorageReadSnapshot::new(self);
        self.collect_table_stats_with_sample_pages_and_zone(table, sample_pages, None, snapshot.txn)
            .map(|(stats, _)| stats)
    }

    fn collect_table_stats_with_sample_pages_and_zone(
        &self,
        table: &TableSchema,
        sample_pages: Option<&[u64]>,
        zone_context: Option<&AnalyzeZoneBuildContext>,
        txn: TxnView,
    ) -> Result<(TableStats, Option<AnalyzeZonePartial>), QueryError> {
        crate::engine::cancel::check_cancelled()?;
        let (
            column_builders,
            index_prefixes,
            group_builders,
            conditional_builders,
            _sampled_row_count,
            zone_partial,
            row_count,
            precise,
            count_scale,
            page_sample_fraction,
            page_count,
            sampled_pages,
            partition_stats,
        ) = if table.is_partitioned() {
            // Keep both leaf and table-wide sketches bounded. Leaf statistics
            // use the ordinary collection policy; the table-wide scan uses a
            // uniform page sample across the virtual concatenation of leaves.
            let storage_ids = table.storage_ids();
            let mut leaves = Vec::with_capacity(storage_ids.len());
            let mut total_pages = 0u64;
            for storage_id in storage_ids {
                let pages = self.with_heap_read(storage_id, |heap| heap.page_count())?;
                total_pages = total_pages.saturating_add(pages);
                leaves.push((storage_id, pages));
            }
            let selected_pages = sample_pages
                .map(|pages| pages.to_vec())
                .or_else(|| analyze_sample_pages(table.id, total_pages, PAGE_SIZE));
            let mut partials = Vec::with_capacity(leaves.len());
            let mut partition_stats = BTreeMap::new();
            let mut row_count = 0u64;
            let mut page_offset = 0u64;
            for (storage_id, pages) in leaves {
                crate::engine::cancel::check_cancelled()?;
                let mut leaf_schema = table.clone();
                leaf_schema.id = storage_id;
                leaf_schema.partitioning = None;
                let (leaf, _) = self.collect_table_stats_with_sample_pages_and_zone(
                    &leaf_schema,
                    None,
                    None,
                    txn,
                )?;
                row_count = row_count.saturating_add(leaf.row_count);
                partition_stats.insert(
                    storage_id,
                    PartitionStats {
                        row_count: leaf.row_count,
                        heap_page_count: pages,
                        columns: leaf.columns,
                        indexes: leaf.indexes,
                        modified_rows_since_analyze: 0,
                        analyzed_at_unix_ms: leaf.analyzed_at_unix_ms,
                        check_ok: None,
                    },
                );
                let leaf_pages = match &selected_pages {
                    Some(selected) => selected
                        .iter()
                        .copied()
                        .filter(|page| *page >= page_offset && *page < page_offset + pages)
                        .map(|page| page - page_offset)
                        .collect::<Vec<_>>(),
                    None => (0..pages).collect(),
                };
                partials.push(self.collect_table_stats_partition(
                    &leaf_schema,
                    Some(&leaf_pages),
                    false,
                    None,
                    txn,
                ));
                page_offset = page_offset.saturating_add(pages);
            }
            let (columns, indexes, groups, conditional, sampled_rows, _) =
                self.merge_stats_scan_partials(partials)?;
            let sampled_page_count = selected_pages
                .as_ref()
                .map_or(total_pages, |p| p.len() as u64);
            let page_fraction = if total_pages == 0 {
                1.0
            } else {
                (sampled_page_count as f64 / total_pages as f64).min(1.0)
            };
            let count_scale = if sampled_rows == 0 {
                1.0
            } else {
                row_count as f64 / sampled_rows as f64
            };
            (
                columns,
                indexes,
                groups,
                conditional,
                sampled_rows,
                None,
                row_count,
                false,
                count_scale,
                page_fraction,
                total_pages,
                None,
                partition_stats,
            )
        } else {
            let forced_sample_pages = sample_pages.map(|pages| pages.to_vec());

            let page_count = self.with_heap_read(table.id, |heap| heap.page_count())?;
            // Default page-selection:
            //   1. Caller-forced list (test fixtures).
            //   2. Random-sample list when the heap is large enough that
            //      `analyze_sample_pages` decides to sample.
            //   3. Otherwise serial full-scan (sampled_pages = None).
            //
            // (3) leaves medium-sized but under-sampling-threshold tables
            // (customer / part at ~30 MB) running single-threaded, so
            // they cost as much as lineitem after the parallel win.
            // Synthesise a full-page list in that case — the parallel
            // path then splits it. The precise path requires ≤4096 live
            // rows, which a 256-page table physically cannot hit (row
            // would have to exceed 1 KiB average), so losing precise
            // mode here is purely theoretical.
            let mut sampled_pages = forced_sample_pages
                .or_else(|| analyze_sample_pages(table.id, page_count, PAGE_SIZE));
            if sampled_pages.is_none()
                && page_count >= STATS_FORCE_FULL_PAGE_LIST_PAGES
                && crate::planner::default_parallel_degree() > 1
            {
                sampled_pages = Some((0..page_count).collect());
            }
            let analyzed_pages = sampled_pages.clone().unwrap_or_default();
            let scan_pages: Option<&[u64]> = sampled_pages.as_deref();

            // The "precise" path tracks per-value exact counts and runs
            // only when we're walking the entire heap AND the live row
            // count stays under `ANALYZE_EXACT_ROW_LIMIT`. Sampled mode
            // (and any parallel split) can never satisfy the first
            // condition — so we know up-front whether the slow per-row
            // `track_exact` path can ever fire.
            let unsampled = scan_pages.is_none();
            // A fused zone image must cover every heap page. Large-table random
            // sampling keeps its existing page-selection semantics and falls back
            // to the independent full zone rebuild until the all-page/sample-page
            // dual traversal is selected below.
            let fused_zone_context = zone_context.filter(|_| {
                scan_pages.is_some_and(|pages| {
                    pages.len() as u64 == page_count && pages.iter().copied().eq(0..page_count)
                })
            });

            // Choose degree. Only sampled mode parallelizes: in unsampled
            // mode the per-row `track_exact` flag depends on a running
            // count that has no meaning when split across workers, and
            // the table is small enough that serial is fast anyway.
            let degree = match scan_pages {
                Some(pages) => collect_stats_parallel_degree(pages.len() as u64),
                None => 1,
            };

            let (
                column_builders,
                index_prefixes,
                group_builders,
                conditional_builders,
                sampled_row_count,
                zone_partial,
            ) = if degree <= 1 {
                self.collect_table_stats_partition(
                    table,
                    scan_pages,
                    unsampled,
                    fused_zone_context,
                    txn,
                )?
            } else {
                self.collect_table_stats_parallel(
                    table,
                    scan_pages.unwrap(),
                    degree,
                    fused_zone_context,
                    txn,
                )?
            };

            let row_count =
                estimate_analyzed_row_count(page_count, &analyzed_pages, sampled_row_count);
            let precise = unsampled && sampled_row_count <= ANALYZE_EXACT_ROW_LIMIT;

            let count_scale = if precise || sampled_row_count == 0 {
                1.0
            } else {
                row_count as f64 / sampled_row_count as f64
            };
            // Fraction of heap pages the builders actually saw — drives the
            // sample→population NDV extrapolation in `finish`. The forced
            // full-page-list path yields exactly 1.0.
            let page_sample_fraction = if scan_pages.is_none() || page_count == 0 {
                1.0
            } else {
                (analyzed_pages.len() as f64 / page_count as f64).min(1.0)
            };
            (
                column_builders,
                index_prefixes,
                group_builders,
                conditional_builders,
                sampled_row_count,
                zone_partial,
                row_count,
                precise,
                count_scale,
                page_sample_fraction,
                page_count,
                sampled_pages,
                BTreeMap::new(),
            )
        };
        let scan_pages = sampled_pages.as_deref();

        let columns: BTreeMap<String, crate::storage::stats::ColumnStats> = column_builders
            .into_iter()
            .map(|(name, builder)| {
                (
                    name,
                    builder.finish(precise, count_scale, row_count, page_sample_fraction),
                )
            })
            .collect();
        let index_layouts = table
            .indexes
            .iter()
            .map(|index| analyze_plain_index_layout(table, index))
            .collect::<Vec<_>>();
        let mut indexes: std::collections::BTreeMap<String, IndexStats> = index_prefixes
            .into_iter()
            .zip(index_layouts.iter())
            .map(|((name, prefixes), layout)| {
                (
                    name,
                    IndexStats {
                        prefix_distinct_counts: prefixes
                            .into_iter()
                            .enumerate()
                            .map(|(pos, prefix)| {
                                if pos == 0
                                    && layout
                                        .as_ref()
                                        .is_some_and(|layout| layout.reuse_column_ndv[pos])
                                {
                                    let column_idx = layout.as_ref().unwrap().column_indices[pos];
                                    let column_name =
                                        normalize_name(&table.columns[column_idx].name);
                                    return columns
                                        .get(&column_name)
                                        .map(|stats| stats.distinct_count)
                                        .unwrap_or(0);
                                }
                                prefix.finish(precise, count_scale, row_count, page_sample_fraction)
                            })
                            .collect(),
                        leaf_page_count: None,
                        internal_page_count: None,
                        tree_height: None,
                        correlation: None,
                        fulltext: None,
                    },
                )
            })
            .collect();
        for index in &table.indexes {
            if let Some(stats) = indexes.get_mut(&normalize_name(&index.name)) {
                stats.correlation = if table.is_partitioned() {
                    let weighted = partition_stats
                        .values()
                        .filter_map(|leaf| {
                            leaf.indexes
                                .get(&normalize_name(&index.name))
                                .and_then(|stats| stats.correlation)
                                .map(|correlation| (correlation, leaf.row_count))
                        })
                        .collect::<Vec<_>>();
                    let rows: u64 = weighted.iter().map(|(_, rows)| *rows).sum();
                    (rows > 0).then(|| {
                        weighted
                            .iter()
                            .map(|(correlation, rows)| correlation * *rows as f64)
                            .sum::<f64>()
                            / rows as f64
                    })
                } else {
                    self.index_heap_order_correlation(table, index)
                };
                let total_pages = if table.is_partitioned() {
                    partition_stats
                        .keys()
                        .map(|storage_id| self.index_page_count(*storage_id, &index.name))
                        .sum()
                } else {
                    self.index_page_count(table.id, &index.name)
                };
                if total_pages > 0 {
                    let height = estimated_analyzed_index_height(row_count);
                    let internal_pages = height.saturating_sub(1).min(total_pages);
                    stats.tree_height = Some(height.max(1));
                    stats.internal_page_count = Some(internal_pages);
                    stats.leaf_page_count = Some(total_pages.saturating_sub(internal_pages));
                }
            }
        }
        if !table.is_partitioned() {
            for (name, fulltext) in
                self.collect_fulltext_stats(table, scan_pages, count_scale, txn)?
            {
                if let Some(stats) = indexes.get_mut(&name) {
                    stats.fulltext = Some(fulltext);
                }
            }
        }
        let conditional_key_domains = conditional_builders
            .into_iter()
            .filter_map(|builder| {
                builder
                    .builder
                    .finish(false, count_scale, row_count, page_sample_fraction)
            })
            .collect();

        // Finish the joint-NDV sketches into `multi_column_stats`, keyed by
        // canonical column set. Recompute the group definitions (cheap,
        // deterministic) to recover each group's column list for display.
        let group_cols: BTreeMap<String, Vec<String>> = multi_column_groups(table)
            .into_iter()
            .map(|(key, cols, _)| (key, cols))
            .collect();
        let group_defs = multi_column_groups(table);
        let multi_column_stats = group_builders
            .into_iter()
            .enumerate()
            .filter_map(|(group_idx, (key, builder))| {
                let source = group_defs.get(group_idx).and_then(|(_, _, idxs)| {
                    index_layouts
                        .iter()
                        .enumerate()
                        .find_map(|(index_idx, layout)| {
                            let layout = layout.as_ref()?;
                            (layout.column_indices == *idxs
                                && layout.reuse_column_key.iter().all(|reuse| *reuse)
                                && table.indexes[index_idx]
                                    .column_prefix_lengths
                                    .iter()
                                    .all(Option::is_none))
                            .then_some((index_idx, idxs.len() - 1))
                        })
                });
                let distinct_count = source
                    .and_then(|(index_idx, prefix_idx)| {
                        indexes
                            .get(&normalize_name(&table.indexes[index_idx].name))
                            .and_then(|stats| stats.prefix_distinct_counts.get(prefix_idx).copied())
                    })
                    .unwrap_or_else(|| {
                        builder.finish(precise, count_scale, row_count, page_sample_fraction)
                    });
                group_cols.get(&key).map(|cols| {
                    (
                        key.clone(),
                        crate::storage::MultiColumnStats {
                            columns: cols.clone(),
                            distinct_count,
                        },
                    )
                })
            })
            .collect();

        let stats = TableStats {
            analyze_revision: 0,
            explicit_histograms: std::collections::BTreeMap::new(),
            row_count,
            heap_page_count: page_count,
            sample_fraction: Some(page_sample_fraction),
            confidence: Some(if precise || page_sample_fraction >= 1.0 {
                1.0
            } else {
                page_sample_fraction.sqrt().clamp(0.1, 0.99)
            }),
            columns,
            indexes,
            partition_stats,
            conditional_key_domains,
            multi_column_stats,
            modified_rows_since_analyze: 0,
            analyzed_at_unix_ms: Some(current_unix_ms()),
            last_analyze_live_baseline: 0,
            last_analyze_modification_baseline: 0,
        };
        let mut zone_partial = zone_partial;
        if let Some(partial) = &mut zone_partial {
            let mut analyzed_table = table.clone();
            analyzed_table.stats = stats.clone();
            let selected = zone_map_membership_requirements(&analyzed_table);
            for zone in partial.batch.zones.values_mut() {
                for (column_id, summary) in &mut zone.columns {
                    if !selected.contains_key(column_id) {
                        summary.membership = None;
                    }
                }
                for (&column_id, &(layout, local_ndv)) in &selected {
                    let Some(summary) = zone.columns.get_mut(&column_id) else {
                        partial.membership_complete = false;
                        continue;
                    };
                    if matches!(summary.state, ZoneMapState::Unknown | ZoneMapState::Empty)
                        || !summary.has_non_null
                    {
                        summary.membership = None;
                        continue;
                    }
                    if !summary
                        .membership
                        .as_mut()
                        .is_some_and(|membership| membership.prepare_for_layout(layout, local_ndv))
                    {
                        partial.membership_complete = false;
                    }
                }
            }
        }
        Ok((stats, zone_partial))
    }

    /// One worker's slice of `collect_table_stats`. Walks the supplied
    /// `scan_pages` (None = full scan of the heap) and accumulates
    /// per-column / per-index distinct sketches + min/max/top-K. The
    /// parallel path passes a disjoint chunk of the sampled-page
    /// list to each worker and merges the partials via
    /// `ColumnStatsBuilder::merge` / `DistinctStatsBuilder::merge`.
    fn collect_table_stats_partition(
        &self,
        table: &TableSchema,
        scan_pages: Option<&[u64]>,
        unsampled: bool,
        zone_context: Option<&AnalyzeZoneBuildContext>,
        txn: TxnView,
    ) -> Result<
        (
            ColumnStatsBuilders,
            IndexPrefixStatsBuilders,
            GroupStatsBuilders,
            ConditionalStatsBuilders,
            u64,
            Option<AnalyzeZonePartial>,
        ),
        QueryError,
    > {
        let mut column_builders = table
            .columns
            .iter()
            .map(|column| (normalize_name(&column.name), ColumnStatsBuilder::new()))
            .collect::<Vec<_>>();
        let mut index_prefixes = table
            .indexes
            .iter()
            .map(|index| {
                (
                    normalize_name(&index.name),
                    (0..index.columns.len())
                        .map(|_| DistinctStatsBuilder::new())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let index_layouts = table
            .indexes
            .iter()
            .map(|index| analyze_plain_index_layout(table, index))
            .collect::<Vec<_>>();
        // Phase 3 (full): joint-NDV sketches for correlated column groups.
        // Built identically to the per-index sketches above so the parallel
        // merge folds them the same way.
        let multi_groups = multi_column_groups(table);
        let group_index_sources = multi_groups
            .iter()
            .map(|(_, _, idxs)| {
                index_layouts
                    .iter()
                    .enumerate()
                    .find_map(|(index_idx, layout)| {
                        let layout = layout.as_ref()?;
                        (layout.column_indices == *idxs
                            && layout.reuse_column_key.iter().all(|reuse| *reuse)
                            && table.indexes[index_idx]
                                .column_prefix_lengths
                                .iter()
                                .all(Option::is_none))
                        .then_some((index_idx, idxs.len() - 1))
                    })
            })
            .collect::<Vec<_>>();
        let mut group_builders: GroupStatsBuilders = multi_groups
            .iter()
            .map(|(key, _, _)| (key.clone(), DistinctStatsBuilder::new()))
            .collect();
        let mut conditional_builders = ConditionalStatsScanBuilder::for_table(table)?;
        let mut sampled_row_count = 0u64;
        let mut key_needed = vec![false; table.columns.len()];
        for layout in index_layouts.iter().flatten() {
            for (&column_idx, &reuse) in layout.column_indices.iter().zip(&layout.reuse_column_key)
            {
                if reuse {
                    key_needed[column_idx] = true;
                }
            }
        }
        for (_, _, idxs) in &multi_groups {
            for &idx in idxs {
                key_needed[idx] = true;
            }
        }
        let mut conditional_value_needed = vec![false; table.columns.len()];
        for builder in &conditional_builders {
            for &idx in builder.filter_indices.iter().chain(&builder.key_indices) {
                conditional_value_needed[idx] = true;
            }
        }
        let mut column_keys: Vec<Option<KeyPart>> = Vec::with_capacity(table.columns.len());
        let contiguous_range = scan_pages.and_then(|pages| {
            let first = *pages.first()?;
            let last = *pages.last()?;
            ((last - first + 1) as usize == pages.len() && pages.iter().copied().eq(first..=last))
                .then_some((first, last + 1))
        });

        // The SF=1 lineitem shape (and the common OLAP fact-table shape) is
        // a parallel approximate scan over plain scalar B-tree indexes. It
        // can stay entirely in the row codec's typed sink and never build a
        // per-row Vec<Value>. More exotic index forms retain the value-mode
        // oracle below.
        let typed_sink_supported = !unsampled
            && contiguous_range.is_some()
            && index_layouts.iter().all(|layout| {
                layout
                    .as_ref()
                    .is_some_and(|layout| layout.reuse_column_key.iter().all(|reuse| *reuse))
            });
        let mut deferred_stats = Vec::new();
        let mut zone_partial = None;
        if typed_sink_supported {
            let (page_lo, page_hi) = contiguous_range.unwrap();
            let columns = (0..table.columns.len()).collect::<Vec<_>>();
            let mut keys = vec![None; table.columns.len()];
            let mut conditional_values = vec![None; table.columns.len()];
            let mut value_scratch = Vec::with_capacity(table.columns.len());
            let mut zone_accumulator = zone_context
                .map(|context| AnalyzeZoneAccumulator::new(table, context, page_lo, page_hi));
            let mut visibility_cache = ScanVisibilityCache::new();
            let mut stats_visibility = ScanVisibilityCache::new();
            for batch_lo in (page_lo..page_hi).step_by(ANALYZE_SCAN_BATCH_PAGES as usize) {
                crate::engine::cancel::check_cancelled()?;
                self.with_heap_read(table.id, |heap| {
                    heap.scan_mvcc_payloads_direct_range_until(
                        &columns,
                        batch_lo,
                        (batch_lo + ANALYZE_SCAN_BATCH_PAGES).min(page_hi),
                        |row| {
                            if !self.row_version_visible_cached(
                                &mut stats_visibility,
                                row.header,
                                txn,
                            )? {
                                deferred_stats.push(row.row_id);
                                if let Some(zone) = zone_accumulator.as_mut() {
                                    zone.defer(row.row_id);
                                }
                                return Ok(false);
                            }
                            if row.header.is_deleted() {
                                return Ok(false);
                            }
                            let zone_visible = if let Some(context) = zone_context {
                                let visible = self.row_version_visible_cached(
                                    &mut visibility_cache,
                                    row.header,
                                    context.txn,
                                )?;
                                if visible {
                                    zone_accumulator
                                        .as_mut()
                                        .expect("analyze zone accumulator")
                                        .begin_row(row.row_id);
                                } else {
                                    zone_accumulator
                                        .as_mut()
                                        .expect("analyze zone accumulator")
                                        .defer(row.row_id);
                                }
                                visible
                            } else {
                                false
                            };
                            sampled_row_count = sampled_row_count.saturating_add(1);
                            conditional_values.fill(None);
                            let fast = {
                                let zone_sink = if zone_visible {
                                    zone_accumulator.as_mut()
                                } else {
                                    None
                                };
                                let mut sink = AnalyzeColumnSink {
                                    builders: &mut column_builders,
                                    keys: &mut keys,
                                    key_needed: &key_needed,
                                    conditional_values: &mut conditional_values,
                                    conditional_value_needed: &conditional_value_needed,
                                    page_id: row.row_id.page_id,
                                    zone: zone_sink,
                                };
                                decode_row_into_sink_fast(
                                    &row.payload[row.body_offset..],
                                    row.codec,
                                    row.layout_id,
                                    row.plan,
                                    &mut sink,
                                    row.has_external,
                                )?
                            };
                            if !fast {
                                value_scratch.clear();
                                decode_row_schema_columns_into_with_plan_ext_for_layout(
                                    &row.payload[row.body_offset..],
                                    row.codec,
                                    row.layout_id,
                                    row.plan,
                                    &mut value_scratch,
                                    row.has_external,
                                )?;
                                for (idx, ((_, builder), value)) in
                                    column_builders.iter_mut().zip(&value_scratch).enumerate()
                                {
                                    builder.record_approx(value, row.row_id.page_id);
                                    if key_needed[idx] {
                                        keys[idx] = Some(KeyPart::from_value(value));
                                    }
                                    if zone_visible {
                                        zone_accumulator
                                            .as_mut()
                                            .expect("analyze zone accumulator")
                                            .observe_value(idx, value);
                                    }
                                }
                            }

                            if fast {
                                for builder in &mut conditional_builders {
                                    builder
                                        .record_captured(&conditional_values, row.row_id.page_id);
                                }
                            } else {
                                for builder in &mut conditional_builders {
                                    builder.record(&value_scratch, row.row_id.page_id);
                                }
                            }

                            for (((_, prefixes), index), layout) in index_prefixes
                                .iter_mut()
                                .zip(&table.indexes)
                                .zip(&index_layouts)
                            {
                                let layout = layout.as_ref().expect("typed analyze layout");
                                let mut parts = smallvec::SmallVec::<[KeyPart; 4]>::with_capacity(
                                    layout.column_indices.len(),
                                );
                                for (pos, &column_idx) in layout.column_indices.iter().enumerate() {
                                    let part = keys[column_idx]
                                        .as_ref()
                                        .expect("typed analyze key")
                                        .clone();
                                    parts.push(index.apply_prefix_to_key_part(pos, part));
                                    if pos == 0 && layout.reuse_column_ndv[pos] {
                                        continue;
                                    }
                                    let refs =
                                        parts.iter().collect::<smallvec::SmallVec<[&KeyPart; 4]>>();
                                    prefixes[pos].record_hash_only(
                                        stats_hash_index_parts(&refs),
                                        row.row_id.page_id,
                                    );
                                }
                            }
                            for (group_idx, ((_, builder), (_, _, idxs))) in
                                group_builders.iter_mut().zip(&multi_groups).enumerate()
                            {
                                if group_index_sources[group_idx].is_some() {
                                    continue;
                                }
                                let refs = idxs
                                    .iter()
                                    .map(|&idx| {
                                        keys[idx].as_ref().expect("typed analyze group key")
                                    })
                                    .collect::<smallvec::SmallVec<[&KeyPart; 4]>>();
                                builder.record_hash_only(
                                    stats_hash_index_parts(&refs),
                                    row.row_id.page_id,
                                );
                            }
                            Ok(false)
                        },
                    )
                })?;
                #[cfg(test)]
                self.pause_analyze_batch_for_test();
            }
            zone_partial = zone_accumulator.map(AnalyzeZoneAccumulator::finish);
        }

        let mut record_row = |values: &[Value], page_id: u32| -> Result<(), QueryError> {
            sampled_row_count = sampled_row_count.saturating_add(1);
            // Exact-tracking is only meaningful in the unsampled serial
            // path. Approximate mode can feed pre-hashed index prefixes
            // without allocating transient IndexKeys.
            let track_exact = unsampled && sampled_row_count <= ANALYZE_EXACT_ROW_LIMIT;
            column_keys.clear();
            column_keys.extend(values.iter().enumerate().map(|(idx, value)| {
                (track_exact || key_needed[idx]).then(|| KeyPart::from_value(value))
            }));
            for (idx, ((_, builder), value)) in
                column_builders.iter_mut().zip(values.iter()).enumerate()
            {
                if track_exact {
                    builder.record_with_key(value, column_keys[idx].as_ref(), true, page_id);
                } else {
                    builder.record_approx(value, page_id);
                }
            }

            for (((_, prefixes), index), layout) in index_prefixes
                .iter_mut()
                .zip(&table.indexes)
                .zip(&index_layouts)
            {
                if !track_exact {
                    if let Some(layout) = layout {
                        let mut parts = smallvec::SmallVec::<[KeyPart; 4]>::with_capacity(
                            layout.column_indices.len(),
                        );
                        for (pos, (&column_idx, &reuse)) in layout
                            .column_indices
                            .iter()
                            .zip(&layout.reuse_column_key)
                            .enumerate()
                        {
                            let part = if reuse {
                                column_keys[column_idx]
                                    .as_ref()
                                    .expect("precompiled analyze key")
                                    .clone()
                            } else {
                                let column = &table.columns[column_idx];
                                KeyPart::from_column_value_with_collation(
                                    &values[column_idx],
                                    &column.data_type,
                                    column.collation.as_deref(),
                                )
                            };
                            parts.push(index.apply_prefix_to_key_part(pos, part));
                            if pos == 0 && layout.reuse_column_ndv[pos] {
                                continue;
                            }
                            let refs = parts.iter().collect::<smallvec::SmallVec<[&KeyPart; 4]>>();
                            prefixes[pos].record_hash_only(stats_hash_index_parts(&refs), page_id);
                        }
                        continue;
                    }
                }
                for key in table.index_keys(index, values)? {
                    let key_len = index.columns.len().min(key.0.len());
                    for prefix_len in 1..=key_len {
                        prefixes[prefix_len - 1].record(
                            &IndexKey::from(key.0[..prefix_len].to_vec()),
                            track_exact,
                            page_id,
                        );
                    }
                }
            }

            for (group_idx, ((_, builder), (_, _cols, idxs))) in
                group_builders.iter_mut().zip(&multi_groups).enumerate()
            {
                if group_index_sources[group_idx].is_some() {
                    continue;
                }
                if track_exact {
                    let parts = idxs
                        .iter()
                        .map(|&i| column_keys[i].as_ref().expect("exact analyze key").clone())
                        .collect::<Vec<_>>();
                    builder.record(&IndexKey::from(parts), true, page_id);
                } else {
                    let refs = idxs
                        .iter()
                        .map(|&i| {
                            column_keys[i]
                                .as_ref()
                                .expect("precompiled analyze group key")
                        })
                        .collect::<smallvec::SmallVec<[&KeyPart; 4]>>();
                    builder.record_hash_only(stats_hash_index_parts(&refs), page_id);
                }
            }
            for builder in &mut conditional_builders {
                builder.record(values, page_id);
            }
            Ok(())
        };

        // A forced full-page list is the normal large-table path. Treat it
        // as the contiguous range it really is so the heap scanner reuses a
        // row decode buffer and enables sequential read-ahead instead of
        // allocating a Vec<Value> per row under random-access intent.
        let columns = (0..table.columns.len()).collect::<Vec<_>>();
        if !typed_sink_supported {
            let pages = match scan_pages {
                Some(pages) => pages.to_vec(),
                None => (0..self.with_heap_read(table.id, |heap| heap.page_count())?).collect(),
            };
            let mut visibility = ScanVisibilityCache::new();
            for batch in pages.chunks(ANALYZE_SCAN_BATCH_PAGES as usize) {
                crate::engine::cancel::check_cancelled()?;
                self.with_heap_read(table.id, |heap| {
                    if let (Some(&lo), Some(&hi)) = (batch.first(), batch.last()) {
                        if hi - lo + 1 == batch.len() as u64 {
                            return heap.scan_mvcc_columns_range_until(
                                &columns,
                                lo,
                                hi + 1,
                                |row| {
                                    if !self.row_version_visible_cached(
                                        &mut visibility,
                                        row.header,
                                        txn,
                                    )? {
                                        deferred_stats.push(row.row_id);
                                    } else if !row.header.is_deleted() {
                                        record_row(row.values, row.row_id.page_id)?;
                                    }
                                    Ok(false)
                                },
                            );
                        }
                    }
                    heap.scan_mvcc_pages_until(Some(batch), |row| {
                        if !self.row_version_visible_cached(&mut visibility, row.header, txn)? {
                            deferred_stats.push(row.row_id);
                        } else if !row.header.is_deleted() {
                            record_row(&row.values, row.row_id.page_id)?;
                        }
                        Ok(false)
                    })
                })?;
                #[cfg(test)]
                self.pause_analyze_batch_for_test();
                for row_id in deferred_stats.drain(..) {
                    if let Some(values) =
                        self.read_row_columns_at(table.id, table, row_id, txn, &columns)?
                    {
                        record_row(&values, row_id.page_id)?;
                    }
                }
            }
        }
        for row_id in deferred_stats {
            if let Some(values) =
                self.read_row_columns_at(table.id, table, row_id, txn, &columns)?
            {
                record_row(&values, row_id.page_id)?;
            }
        }

        Ok((
            column_builders,
            index_prefixes,
            group_builders,
            conditional_builders,
            sampled_row_count,
            zone_partial,
        ))
    }

    /// Parallel page-list partitioning for `collect_table_stats`.
    /// Each worker handles a disjoint contiguous slice of the
    /// sampled-page list; the per-page scan still goes through the
    /// per-worker `with_heap_read` guard. Merge folds the partial
    /// builders back together — semantics match what serial
    /// `record(..., track_exact=false)` would have produced.
    fn collect_table_stats_parallel(
        &self,
        table: &TableSchema,
        pages: &[u64],
        degree: u32,
        zone_context: Option<&AnalyzeZoneBuildContext>,
        txn: TxnView,
    ) -> Result<
        (
            ColumnStatsBuilders,
            IndexPrefixStatsBuilders,
            GroupStatsBuilders,
            ConditionalStatsBuilders,
            u64,
            Option<AnalyzeZonePartial>,
        ),
        QueryError,
    > {
        let degree = degree.max(2) as usize;
        let chunk_size = pages.len().div_ceil(degree).max(1);
        let chunks: Vec<&[u64]> = pages.chunks(chunk_size).collect();
        let chunk_count = chunks.len();

        let mut partials: Vec<
            Result<
                (
                    ColumnStatsBuilders,
                    IndexPrefixStatsBuilders,
                    GroupStatsBuilders,
                    ConditionalStatsBuilders,
                    u64,
                    Option<AnalyzeZonePartial>,
                ),
                QueryError,
            >,
        > = Vec::with_capacity(chunk_count);
        let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(chunk_count);
            for chunk in chunks {
                let cancellation = cancellation.clone();
                let h = scope.spawn(move || {
                    let _cancel_scope = cancellation.enter();
                    // Sampled mode → track_exact is always false; we
                    // pass `unsampled = false` to make that explicit
                    // in the worker too.
                    self.collect_table_stats_partition(table, Some(chunk), false, zone_context, txn)
                });
                handles.push(h);
            }
            for h in handles {
                partials.push(
                    h.join()
                        .map_err(|_| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "analyze stats worker panicked",
                            )
                        })
                        .and_then(|r| r),
                );
            }
        });

        self.merge_stats_scan_partials(partials)
    }

    fn merge_stats_scan_partials(
        &self,
        partials: Vec<Result<StatsScanPartial, QueryError>>,
    ) -> Result<StatsScanPartial, QueryError> {
        let mut iter = partials.into_iter();
        let first = iter.next().ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "analyze stats: empty parallel partition",
            )
        })??;
        let (
            mut column_builders,
            mut index_prefixes,
            mut group_builders,
            mut conditional_builders,
            mut sampled_row_count,
            mut zone_partial,
        ) = first;
        for partial in iter {
            let (other_cols, other_idx, other_groups, other_conditional, other_rows, other_zone) =
                partial?;
            if column_builders.len() != other_cols.len()
                || index_prefixes.len() != other_idx.len()
                || group_builders.len() != other_groups.len()
                || conditional_builders.len() != other_conditional.len()
            {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "analyze stats schema changed during scan",
                ));
            }

            for ((name, slot), (other_name, other_builder)) in
                column_builders.iter_mut().zip(other_cols)
            {
                if *name != other_name {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "analyze stats schema changed during scan",
                    ));
                }
                slot.merge(other_builder);
            }
            for (slot, other_builder) in conditional_builders.iter_mut().zip(other_conditional) {
                slot.merge(other_builder)?;
            }

            for ((name, slot), (other_name, other_prefixes)) in
                index_prefixes.iter_mut().zip(other_idx)
            {
                if *name != other_name || slot.len() != other_prefixes.len() {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "analyze stats schema changed during scan",
                    ));
                }
                for (builder, other_builder) in slot.iter_mut().zip(other_prefixes) {
                    builder.merge(other_builder);
                }
            }

            for ((name, slot), (other_name, other_builder)) in
                group_builders.iter_mut().zip(other_groups)
            {
                if *name != other_name {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "analyze stats schema changed during scan",
                    ));
                }
                slot.merge(other_builder);
            }
            sampled_row_count = sampled_row_count.saturating_add(other_rows);
            merge_analyze_zone_partials(&mut zone_partial, other_zone);
        }

        Ok((
            column_builders,
            index_prefixes,
            group_builders,
            conditional_builders,
            sampled_row_count,
            zone_partial,
        ))
    }

    #[cfg(test)]
    fn collect_table_stats_exact_values(
        &self,
        table: &TableSchema,
        live_rows: &[Vec<Value>],
    ) -> Result<TableStats, QueryError> {
        let mut column_builders = table
            .columns
            .iter()
            .map(|column| (normalize_name(&column.name), ColumnStatsBuilder::new()))
            .collect::<BTreeMap<_, _>>();
        let mut index_prefixes = table
            .indexes
            .iter()
            .map(|index| {
                (
                    normalize_name(&index.name),
                    (0..index.columns.len())
                        .map(|_| HashSet::<IndexKey>::new())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        for row in live_rows {
            for (idx, column) in table.columns.iter().enumerate() {
                let key = normalize_name(&column.name);
                if let Some(builder) = column_builders.get_mut(&key) {
                    builder.record(&row[idx], true, 0);
                }
            }
            for index in &table.indexes {
                if let Some(prefixes) = index_prefixes.get_mut(&normalize_name(&index.name)) {
                    for key in table.index_keys(index, row)? {
                        // Statistics expose logical index-column prefixes.
                        // Derived physical keys (e.g. a four-part spatial MBR)
                        // may be wider, as in the streaming collector above.
                        for (prefix, prefix_len) in prefixes.iter_mut().zip(1..=key.0.len()) {
                            prefix.insert(IndexKey::from(key.0[..prefix_len].to_vec()));
                        }
                    }
                }
            }
        }

        let columns = column_builders
            .into_iter()
            .map(|(name, builder)| (name, builder.finish(true, 1.0, live_rows.len() as u64, 1.0)))
            .collect();
        let mut indexes: BTreeMap<String, IndexStats> = index_prefixes
            .into_iter()
            .map(|(name, prefixes)| {
                (
                    name,
                    IndexStats {
                        prefix_distinct_counts: prefixes
                            .into_iter()
                            .map(|prefix| prefix.len() as u64)
                            .collect(),
                        leaf_page_count: None,
                        internal_page_count: None,
                        tree_height: None,
                        correlation: None,
                        fulltext: None,
                    },
                )
            })
            .collect();
        for (name, fulltext) in Self::collect_fulltext_stats_from_values(table, live_rows)? {
            if let Some(stats) = indexes.get_mut(&name) {
                stats.fulltext = Some(fulltext);
            }
        }

        Ok(TableStats {
            analyze_revision: 0,
            explicit_histograms: std::collections::BTreeMap::new(),
            row_count: live_rows.len() as u64,
            heap_page_count: 0,
            sample_fraction: Some(1.0),
            confidence: Some(1.0),
            columns,
            indexes,
            partition_stats: BTreeMap::new(),
            conditional_key_domains: Vec::new(),
            multi_column_stats: BTreeMap::new(),
            modified_rows_since_analyze: 0,
            analyzed_at_unix_ms: Some(current_unix_ms()),
            last_analyze_live_baseline: 0,
            last_analyze_modification_baseline: 0,
        })
    }

    #[cfg(test)]
    fn collect_table_stats_legacy_exact(
        &self,
        table: &TableSchema,
    ) -> Result<TableStats, QueryError> {
        let rows = self.with_heap_mut(table.id, |heap| heap.scan_mvcc())?;
        let live_rows = rows
            .into_iter()
            .filter(|row| !row.header.is_deleted())
            .map(|row| row.values)
            .collect::<Vec<_>>();
        self.collect_table_stats_exact_values(table, &live_rows)
    }

    /// Sample the index in key order and measure how closely heap page
    /// ids follow: |Pearson r| between entry rank and heap page id over
    /// up to 64 evenly spaced leaves. Feeds the range-scan cost model's
    /// correlated-vs-random heap fetch split; `None` (empty index,
    /// non-BTREE access structure, sampling failure) keeps the legacy
    /// default.
    fn index_heap_order_correlation(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> Option<f64> {
        if !index.index_type.eq_ignore_ascii_case("BTREE") || index.fulltext.is_some() {
            return None;
        }
        const MAX_SAMPLE_LEAVES: usize = 64;
        let pages: Vec<f64> = self
            .with_index_read(table.id, &index.name, |index_file| {
                let leaf_ids = index_file.range_leaf_ids(None, None)?;
                if leaf_ids.is_empty() {
                    return Ok(Vec::new());
                }
                let step = leaf_ids.len().div_ceil(MAX_SAMPLE_LEAVES).max(1);
                let mut pages = Vec::new();
                for &leaf_id in leaf_ids.iter().step_by(step) {
                    if let IndexPage::Leaf(leaf) = index_file.read_page(leaf_id)?.as_ref() {
                        pages.extend(leaf.entries.iter().map(|e| e.row_id.page_id as f64));
                    }
                }
                Ok(pages)
            })
            .ok()?;
        rank_page_correlation(&pages)
    }

    #[cfg(test)]
    fn collect_table_stats_with_sample_pages_for_test(
        &self,
        table: &TableSchema,
        sample_pages: &[u64],
    ) -> Result<TableStats, QueryError> {
        self.collect_table_stats_with_sample_pages(table, Some(sample_pages))
    }
}

/// Correlated column groups worth a joint distinct-value count (Phase 3
/// full): the column sets of composite (≥2 column) plain B-tree indexes
/// and the local column tuples of multi-column foreign keys. These are the
/// groups whose columns are most likely to be correlated *and* queried
/// together (composite filters, multi-column joins). De-duplicated by
/// canonical key; each entry is `(canonical_key, columns, column_indices)`.
/// Expression / prefix / multi-valued / fulltext index parts are skipped
/// because their key bytes aren't a plain tuple of column values.
fn multi_column_groups(table: &TableSchema) -> Vec<(String, Vec<String>, Vec<usize>)> {
    let mut seen = BTreeSet::new();
    let mut groups: Vec<(String, Vec<String>, Vec<usize>)> = Vec::new();
    let index_cols = table.indexes.iter().filter_map(|index| {
        if index.has_expression_parts()
            || index.has_multi_valued_parts()
            || index.has_prefix_columns()
            || index.fulltext.is_some()
            || !index.index_type.eq_ignore_ascii_case("BTREE")
        {
            return None;
        }
        Some(index.columns.clone())
    });
    let fk_cols = table.foreign_keys.iter().map(|fk| fk.columns.clone());
    for cols in index_cols.chain(fk_cols) {
        if cols.len() < 2 {
            continue;
        }
        let key = crate::storage::multi_column_group_key(&cols);
        if !seen.insert(key.clone()) {
            continue;
        }
        let mut idxs = Vec::with_capacity(cols.len());
        let mut ok = true;
        for c in &cols {
            match table.column_index(c) {
                Ok(i) => idxs.push(i),
                Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            groups.push((key, cols, idxs));
        }
    }
    groups
}

fn conditional_key_domain_candidates(table: &TableSchema) -> Vec<(Vec<String>, Vec<String>)> {
    let Some(key_columns) = conditional_domain_key_columns(table) else {
        return Vec::new();
    };
    let key_set = key_columns
        .iter()
        .map(|c| normalize_name(c))
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut candidates = Vec::new();
    for index in &table.indexes {
        if index.primary
            || !index.index_type.eq_ignore_ascii_case("BTREE")
            || index.fulltext.is_some()
            || index.has_expression_parts()
            || index.has_prefix_columns()
            || index.has_multi_valued_parts()
        {
            continue;
        }
        let max_prefix = index.columns.len().min(3);
        for prefix_len in 1..=max_prefix {
            let filter_columns = index
                .columns
                .iter()
                .take(prefix_len)
                .map(|c| normalize_name(c))
                .collect::<Vec<_>>();
            if filter_columns.iter().any(|c| key_set.contains(c)) {
                continue;
            }
            if filter_columns
                .iter()
                .any(|c| table.column_index(c).is_err())
            {
                continue;
            }
            let signature = (filter_columns.clone(), key_columns.clone());
            if seen.insert(signature.clone()) {
                candidates.push(signature);
            }
        }
    }
    candidates
}

fn conditional_domain_key_columns(table: &TableSchema) -> Option<Vec<String>> {
    if table.primary_key.len() == 1 && table.column_index(&table.primary_key[0]).is_ok() {
        return Some(vec![normalize_name(&table.primary_key[0])]);
    }
    table
        .indexes
        .iter()
        .find(|index| {
            index.unique
                && !index.primary
                && index.columns.len() == 1
                && index.index_type.eq_ignore_ascii_case("BTREE")
                && index.fulltext.is_none()
                && !index.has_expression_parts()
                && !index.has_prefix_columns()
                && !index.has_multi_valued_parts()
                && table.column_index(&index.columns[0]).is_ok()
        })
        .map(|index| vec![normalize_name(&index.columns[0])])
}

/// Minimum heap page count (16 KiB pages → 4 MiB heap) at which a
/// table that would otherwise stay on the single-threaded full-scan
/// path is upgraded to "use all pages as the sample list" so the
/// parallel ANALYZE path can split it across workers. Picked so that
/// the spawn / merge overhead is amortised by the saved per-page
/// work; smaller heaps stay serial.
const STATS_FORCE_FULL_PAGE_LIST_PAGES: u64 = 256;

/// Pick how many worker threads ANALYZE's `collect_table_stats`
/// pass should split the sampled page list across.
///
/// The work per page is per-row column decode + KeyPart encoding +
/// KMV sketch hash + min/max compare + per-index multi-prefix
/// sketches, so the cross-thread spawn overhead is amortised easily
/// — but the partition merge cost grows with column / index count,
/// so the floor is set per-page-per-worker rather than
/// per-worker-total.
fn collect_stats_parallel_degree(sampled_page_count: u64) -> u32 {
    // Floor: at least 64 sampled pages per worker, otherwise the
    // merge cost (BTreeMap unions, top_candidates trim) is a wash
    // against the per-worker scan win.
    const STATS_PARALLEL_MIN_PAGES_PER_WORKER: u64 = 64;
    let configured = analyze_parallel_degree();
    if configured <= 1 {
        return 1;
    }
    let max_useful = (sampled_page_count / STATS_PARALLEL_MIN_PAGES_PER_WORKER).max(1) as u32;
    configured.min(max_useful).max(1)
}

/// ANALYZE participates in the same process-wide parallelism contract as
/// query execution. In particular, several independent ANALYZE statements
/// may run concurrently; defaulting each one to every available CPU silently
/// multiplied `MYDB_PARALLEL_DEGREE=4` into dozens of scan workers. The
/// ANALYZE-specific setting remains an explicit override.
fn analyze_parallel_degree() -> u32 {
    if std::env::var("MYDB_PARALLEL_DISABLE")
        .ok()
        .is_some_and(|value| matches!(value.trim(), "1" | "on" | "true" | "yes"))
    {
        return 1;
    }
    std::env::var("MYDB_ANALYZE_PARALLEL_DEGREE")
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|degree| *degree > 0)
        .unwrap_or_else(crate::planner::default_parallel_degree)
}

/// |Pearson r| between sample rank (0..n) and the heap page id at that
/// rank. A single-page sample is perfectly local (1.0); a constant rank
/// axis cannot happen for n >= 2.
fn rank_page_correlation(pages: &[f64]) -> Option<f64> {
    let n = pages.len();
    if n < 2 {
        return None;
    }
    let nf = n as f64;
    let mean_x = (nf - 1.0) / 2.0;
    let mean_y = pages.iter().sum::<f64>() / nf;
    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;
    for (i, &y) in pages.iter().enumerate() {
        let dx = i as f64 - mean_x;
        let dy = y - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }
    if var_y == 0.0 {
        return Some(1.0);
    }
    Some((cov / (var_x.sqrt() * var_y.sqrt())).abs().clamp(0.0, 1.0))
}
