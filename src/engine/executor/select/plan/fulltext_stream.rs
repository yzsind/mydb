//! Pull-based heap materialization after full-text candidate evaluation.

use super::*;
use crate::engine::executor::mem_budget::{estimate_row_bytes, ScopedCharge};

pub(in crate::engine::executor::select) const FULLTEXT_BATCH_ROWS: usize = 256;

pub(super) fn open(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<Box<dyn PlanStream>, QueryError> {
    let PhysicalPlan::FulltextScan {
        table,
        index,
        mode,
        query,
        query_param,
        columns,
        match_fingerprint,
        filter,
        projected_columns,
        needs_score,
        restrict_to_matches,
        additional_matches,
        like_prefilter,
        base_access,
        row_goal,
        ..
    } = plan
    else {
        unreachable!("full-text stream requires a FulltextScan");
    };
    let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
    // A membership-only positive prefix has no scoring/global set work.
    // Pull bounded posting chunks so LIMIT can stop before reading every
    // expansion. Other boolean shapes retain the complete-set evaluator.
    // The row goal selects access/batch size only; the stream is never capped.
    if row_goal.is_some()
        && !needs_score
        && *restrict_to_matches
        && additional_matches.is_empty()
        && like_prefilter.is_none()
        && base_access.is_none()
        && filter.is_none()
        && !txn.historical
        && !table.schema.is_partitioned()
        && index.predicate.is_none()
        && db.fulltext_index_covers_snapshot(index, txn)?
        && *mode == crate::parser::FtMode::Boolean
    {
        use crate::storage::fulltext::boolean::{parse_boolean_query, Matcher, Occur};
        let query = resolve_fulltext_query(query, *query_param)?;
        let meta = index.fulltext.clone().unwrap_or_default();
        let tokenizer = crate::storage::fulltext::build_tokenizer_from_meta(&meta);
        let clauses = parse_boolean_query(&query, tokenizer.as_ref())?;
        if let [clause] = clauses.as_slice() {
            if clause.occur != Occur::MustNot {
                if let Matcher::Prefix(prefix) = &clause.matcher {
                    let low = Some(crate::storage::IndexBound {
                        key: IndexKey::from(vec![KeyPart::String(prefix.clone())]),
                        inclusive: true,
                    });
                    let high =
                        crate::storage::fulltext::boolean::prefix_successor(prefix).map(|upper| {
                            crate::storage::IndexBound {
                                key: IndexKey::from(vec![KeyPart::String(upper)]),
                                inclusive: false,
                            }
                        });
                    let cursor =
                        db.range_index_snapshot_cursor(&table.schema, index, low, high, txn)?;
                    let mut stream = FulltextScanStream::new(
                        db,
                        txn,
                        table,
                        filter,
                        projected_columns.as_ref(),
                        Vec::new(),
                        Vec::new(),
                        None,
                    )?;
                    stream.prefix = Some(PrefixCandidates {
                        cursor,
                        index: index.clone(),
                        prefix: prefix.clone(),
                        seen: ah_hash_set(),
                        charge: ScopedCharge::new(0),
                        batch_rows: row_goal
                            .unwrap_or(FULLTEXT_BATCH_ROWS)
                            .clamp(1, FULLTEXT_BATCH_ROWS),
                        rollback_epoch: db
                            .index_repair_rollback_epoch(table.schema.id, &index.name),
                    });
                    return Ok(Box::new(stream));
                }
            }
        }
    }
    open_fulltext_scan(
        db,
        txn,
        table,
        index,
        *mode,
        query,
        *query_param,
        columns,
        *match_fingerprint,
        filter,
        projected_columns.as_ref(),
        *needs_score,
        *restrict_to_matches,
        additional_matches,
        like_prefilter.as_ref(),
        base_access.as_ref(),
    )
}

struct PrefixCandidates {
    cursor: crate::storage::IndexRangeCursor,
    index: IndexSchema,
    prefix: String,
    seen: AhHashSet<RowId>,
    charge: ScopedCharge,
    batch_rows: usize,
    rollback_epoch: u64,
}

impl PrefixCandidates {
    fn next_batch(
        &mut self,
        db: &Arc<Database>,
        table: &PlanTable,
        txn: TxnView,
    ) -> Result<Vec<RowId>, QueryError> {
        loop {
            crate::engine::cancel::check_cancelled()?;
            let entries = self.cursor.next_chunk(self.batch_rows)?;
            if entries.is_empty() {
                return Ok(Vec::new());
            }
            let bytes = entries.iter().fold(0usize, |bytes, (key, _)| {
                let parts = key.0.iter().fold(0usize, |bytes, part| {
                    bytes
                        .saturating_add(std::mem::size_of::<KeyPart>())
                        .saturating_add(match part {
                            KeyPart::String(value) => value.capacity(),
                            KeyPart::Bytes(value) => value.capacity(),
                            _ => 0,
                        })
                });
                bytes
                    .saturating_add(std::mem::size_of::<(IndexKey, RowId)>())
                    .saturating_add(parts)
            });
            let mut chunk_charge = ScopedCharge::new(0);
            chunk_charge.add_non_spillable(bytes)?;
            // Validate every visited posting, even for membership-only reads.
            // Full keys must reach visibility checks: a retired payload and
            // the current payload can share the same token and RowId.
            db.validate_fulltext_posting_entries(&table.schema, &self.index, &entries)?;
            let visible = db.retry_if_rollback_overlapped(&table.schema, &self.index, || {
                let _exact = (db.index_repair_rollback_epoch(table.schema.id, &self.index.name)
                    != self.rollback_epoch)
                    .then(crate::storage::ExactIndexVisibility::enter);
                db.filter_visible_index_entries_mask(&table.schema, &self.index, &entries, txn)
            })?;
            let mut ids = Vec::with_capacity(entries.len());
            for ((key, row_id), visible) in entries.into_iter().zip(visible) {
                if !visible
                    || !matches!(key.0.first(), Some(KeyPart::String(token)) if token.starts_with(&self.prefix))
                {
                    continue;
                }
                if !self.seen.contains(&row_id) {
                    // Hash-table growth headroom, retained until the stream
                    // is dropped. Deduplicate across all prefix expansions.
                    self.charge.add_non_spillable(64)?;
                    self.seen.insert(row_id);
                    ids.push(row_id);
                }
            }
            if !ids.is_empty() {
                return Ok(ids);
            }
        }
    }
}

pub(super) struct FulltextScanStream {
    db: Arc<Database>,
    txn: TxnView,
    table: PlanTable,
    filter: Option<Expr>,
    projected_columns: Option<Arc<Vec<usize>>>,
    runtimes: Vec<FtMatchRuntime>,
    candidates: std::vec::IntoIter<RowId>,
    prefix: Option<PrefixCandidates>,
    heap_documents: Option<(Vec<Vec<Value>>, ScopedCharge)>,
    heap: Option<HeapHandle>,
    columns: Vec<usize>,
    column_metas: Vec<ColMetaRef>,
    score_metas: Vec<(usize, ColMetaRef)>,
    row_schema: super::super::row_schema::RowSchemaRef,
    current: std::vec::IntoIter<(RowId, Vec<Value>)>,
    batch_charge: ScopedCharge,
    eval_ctx: EvalQueryContext,
}

impl FulltextScanStream {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        db: &Arc<Database>,
        txn: TxnView,
        table: &PlanTable,
        filter: &Option<Expr>,
        projected_columns: Option<&Arc<Vec<usize>>>,
        runtimes: Vec<FtMatchRuntime>,
        candidates: Vec<RowId>,
        heap_documents: Option<(Vec<Vec<Value>>, ScopedCharge)>,
    ) -> Result<Self, QueryError> {
        let heap = if projected_columns.is_some() && heap_documents.is_none() {
            Some(db.cached_heap_handle(table.schema.id)?)
        } else {
            None
        };
        // Hidden score columns have one stable shape per query. Intern once,
        // rather than allocate and hash the same strings for every match.
        let score_metas: Vec<_> = runtimes
            .iter()
            .enumerate()
            .filter(|(_, runtime)| runtime.need_score)
            .map(|(index, runtime)| {
                (
                    index,
                    intern_col_meta(ColMeta {
                        table: table.schema.name.clone(),
                        alias: table.alias.clone(),
                        column: format!("{FT_SCORE_PREFIX}{:016x}", runtime.fingerprint),
                        column_type: ColumnType::Double,
                        ..Default::default()
                    }),
                )
            })
            .collect();
        let columns = projected_columns
            .map(|columns| columns.as_ref().clone())
            .unwrap_or_else(|| logical_visible_column_indices(&table.schema));
        let metas = cached_column_metas(table);
        let column_metas = columns
            .iter()
            .map(|&index| {
                metas.get(index).cloned().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "projected column index out of bounds",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut all_metas = column_metas.clone();
        all_metas.extend(score_metas.iter().map(|(_, meta)| meta.clone()));
        // Narrow rows use linear lookup regardless of schema ownership.
        // Avoid an Arc increment/decrement per candidate on that path;
        // wide rows share the lazily built hashed lookup across the scan.
        let row_schema = if all_metas.len() > super::super::row_schema::LOOKUP_THRESHOLD {
            RowSchema::from_metas(&all_metas).into()
        } else {
            PlanRow::empty_schema()
        };
        Ok(Self {
            db: db.clone(),
            txn,
            table: table.clone(),
            filter: filter.clone(),
            projected_columns: projected_columns.cloned(),
            runtimes,
            candidates: candidates.into_iter(),
            prefix: None,
            heap_documents,
            heap,
            columns,
            column_metas,
            score_metas,
            row_schema,
            current: Vec::new().into_iter(),
            batch_charge: ScopedCharge::new(0),
            eval_ctx: EvalQueryContext::default(),
        })
    }

    fn refill_candidates(&mut self) -> Result<bool, QueryError> {
        let Some(prefix) = &mut self.prefix else {
            return Ok(false);
        };
        let ids = prefix.next_batch(&self.db, &self.table, self.txn)?;
        let nonempty = !ids.is_empty();
        self.candidates = ids.into_iter();
        Ok(nonempty)
    }

    fn next_values(&mut self) -> Result<Option<(RowId, Vec<Value>)>, QueryError> {
        if self.heap.is_some() && self.projected_columns.is_some() {
            loop {
                if let Some(row) = self.current.next() {
                    return Ok(Some(row));
                }
                self.batch_charge = ScopedCharge::new(0);
                if self.candidates.as_slice().is_empty() && !self.refill_candidates()? {
                    return Ok(None);
                }
                let remaining = self.candidates.as_slice();
                if remaining.is_empty() {
                    return Ok(None);
                }
                crate::engine::cancel::check_cancelled()?;
                let count = remaining.len().min(FULLTEXT_BATCH_ROWS);
                let rows = self.db.read_row_columns_many_with_handle(
                    self.heap.as_ref().unwrap(),
                    &self.table.schema,
                    &remaining[..count],
                    self.txn,
                    self.projected_columns.as_ref().unwrap(),
                )?;
                self.candidates.nth(count - 1);
                let bytes = rows.iter().fold(0usize, |bytes, (_, values)| {
                    bytes.saturating_add(estimate_row_bytes(values))
                });
                self.batch_charge.add_non_spillable(bytes)?;
                self.current = rows.into_iter();
            }
        }
        loop {
            let row_id = match self.candidates.next() {
                Some(id) => id,
                None if self.refill_candidates()? => continue,
                None => return Ok(None),
            };
            let values = if let Some((rows, _charge)) = &self.heap_documents {
                rows[super::fulltext_source::heap_document_offset(row_id)].clone()
            } else {
                let Some(stored) = self.db.read_row(&self.table.schema, row_id, self.txn)? else {
                    continue;
                };
                stored.values
            };
            return Ok(Some((row_id, values)));
        }
    }
}

impl PlanStream for FulltextScanStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        loop {
            // Projected, predicate-free rows are bounded by the heap batch;
            // check once per refill, as native batch scans do. Residual
            // expressions and row-at-a-time fallbacks retain per-row checks.
            if self.filter.is_some() || self.heap.is_none() {
                crate::engine::cancel::check_cancelled()?;
            }
            let Some((row_id, mut values)) = self.next_values()? else {
                return Ok(None);
            };
            let mut fields = Vec::with_capacity(self.column_metas.len() + self.score_metas.len());
            for (offset, (&column, meta)) in self.columns.iter().zip(&self.column_metas).enumerate()
            {
                let source = if self.heap.is_some() { offset } else { column };
                let value = values.get_mut(source).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "stored row has too few values",
                    )
                })?;
                fields.push(PlanField {
                    meta: meta.clone(),
                    value: std::mem::replace(value, Value::Null),
                });
            }
            for (index, meta) in &self.score_metas {
                fields.push(PlanField {
                    meta: meta.clone(),
                    value: Value::Double(fulltext_runtime_score(&self.runtimes[*index], row_id)),
                });
            }
            let row = PlanRow {
                fields,
                schema: self.row_schema.clone(),
                ..Default::default()
            };
            if let Some(filter) = &self.filter {
                if !eval_plan_bool_with_db(&self.db, self.txn, filter, &row, &mut self.eval_ctx)? {
                    continue;
                }
            }
            return Ok(Some(row));
        }
    }
}
