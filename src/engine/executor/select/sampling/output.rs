//! Common columnar output and spillable exact row selection.
use super::*;

enum Refs {
    Scan(RefScan),
    Rows(rounds::RoundScan),
}

/// Keep block-sample payload probes inside selected page ranges. The shared
/// dense MRR path can otherwise coalesce across an unselected one-page gap.
fn fetch_window_end(references: &[DmlRowRef], first: usize, blocks: bool) -> usize {
    let storage_id = references[first].storage_id;
    let mut end = first + 1;
    while end < references.len() && references[end].storage_id == storage_id {
        if blocks
            && references[end].row_id.page_id > references[end - 1].row_id.page_id.saturating_add(1)
        {
            break;
        }
        end += 1;
    }
    end
}

pub(in crate::engine::executor::select) struct SampleBatchStream {
    db: Arc<Database>,
    table: PlanTable,
    txn: TxnView,
    columns: Vec<usize>,
    schema: Arc<BatchSchema>,
    binding: Option<Resolved>,
    degree: usize,
    full_percent: bool,
    batch_rows: usize,
    max_batch_rows: usize,
    refs: Option<Refs>,
    pending: std::vec::IntoIter<DmlRowRef>,
    pending_charge: ScopedCharge,
    key: Option<crate::planner::PlanNodeId>,
}
impl SampleBatchStream {
    pub(in crate::engine::executor::select) fn new(
        db: &Arc<Database>,
        txn: TxnView,
        plan: &PhysicalPlan,
    ) -> Self {
        let (table, columns, degree) = match plan {
            PhysicalPlan::SampleScan {
                table,
                projected_columns,
                ..
            } => (table, projected_columns, 1),
            PhysicalPlan::ParallelSampleScan {
                table,
                projected_columns,
                degree,
                ..
            } => (table, projected_columns, *degree as usize),
            _ => unreachable!(),
        };
        let columns = columns
            .as_deref()
            .map(|cols| cols.clone())
            .unwrap_or_else(|| super::super::plan::logical_visible_column_indices(&table.schema));
        let schema = Arc::new(BatchSchema::new(
            columns
                .iter()
                .map(|&i| {
                    let meta = super::super::plan::ColMeta::from_table(table, i);
                    BatchField {
                        table: meta.table,
                        alias: meta.alias,
                        name: meta.column,
                        column_type: meta.column_type,
                        source: meta.source,
                    }
                })
                .collect(),
        ));
        let batch_rows = mem_budget::current_budget().map_or(DEFAULT_BATCH_ROWS, |budget| {
            let row_bytes = columns
                .len()
                .max(1)
                .saturating_mul(std::mem::size_of::<Value>())
                .saturating_add(std::mem::size_of::<DmlRowRef>() * 2);
            (budget.limit() / degree.max(1) / 8 / row_bytes).clamp(1, DEFAULT_BATCH_ROWS)
        });
        let hint = super::super::vec_bridge::scan_batch_rows();
        let full_percent = full_percent_allowed()
            && hint == DEFAULT_BATCH_ROWS
            && table.sample.as_ref().unwrap().method == SampleMethod::Bernoulli
            && table.sample.as_ref().unwrap().unit == SampleUnit::Percent;
        let batch_rows = batch_rows.min(hint);
        Self {
            db: db.clone(),
            table: table.clone(),
            txn: crate::engine::executor::temporal::txn_for_table(txn, table),
            binding: capture().and_then(|bindings| {
                bindings
                    .get(&table.sample.as_ref().unwrap().binding_id)
                    .copied()
            }),
            columns,
            schema,
            degree,
            full_percent,
            batch_rows,
            max_batch_rows: batch_rows,
            refs: None,
            pending: Vec::new().into_iter(),
            pending_charge: ScopedCharge::new(0),
            key: super::super::analyze_stats::plan_key(plan),
        }
    }
    fn start(&mut self) -> Result<(), QueryError> {
        if self.refs.is_some() {
            return Ok(());
        }
        let spec = self.table.sample.as_ref().unwrap();
        let sample = self.binding.map(Ok).unwrap_or_else(|| resolve(spec))?;
        let scan = RefScan::new(
            self.db.clone(),
            self.table.clone(),
            self.txn,
            sample,
            if sample.method == SampleMethod::Bernoulli && self.batch_rows <= 64 {
                1
            } else {
                self.degree
            },
            self.full_percent,
        )?;
        if sample.method == SampleMethod::Bernoulli {
            self.max_batch_rows = self.max_batch_rows.min(2048);
            self.batch_rows = self.batch_rows.min(256);
            if sample.unit == SampleUnit::Rows {
                self.max_batch_rows = self
                    .max_batch_rows
                    .min(usize::try_from(sample.rows).unwrap_or(usize::MAX).max(1));
                self.batch_rows = self
                    .batch_rows
                    .min(usize::try_from(sample.rows).unwrap_or(usize::MAX).max(1));
            }
        }
        self.refs = Some(if sample.method == SampleMethod::Bernoulli {
            Refs::Rows(rounds::RoundScan::new(scan, self.key))
        } else {
            Refs::Scan(scan)
        });
        Ok(())
    }
    fn next_refs(&mut self) -> Result<bool, QueryError> {
        loop {
            let chunk = match self.refs.as_mut().unwrap() {
                Refs::Scan(scan) => scan.next()?,
                Refs::Rows(scan) => scan.next(self.batch_rows)?,
            };
            let Some(mut chunk) = chunk else {
                return Ok(false);
            };
            self.pending = chunk.rows.into_vec(&mut chunk._charge)?.into_iter();
            self.pending_charge = chunk._charge;
            if self.pending.len() != 0 {
                return Ok(true);
            }
        }
    }
}
/// Locking readers use the same population selector before evaluating their
/// predicates and acquiring locks. No private origin column enters SQL.
pub(in crate::engine::executor) fn selected_refs(
    db: &Arc<Database>,
    txn: TxnView,
    reference: &crate::parser::TableRef,
) -> Result<(Vec<DmlRowRef>, ScopedCharge), QueryError> {
    let schema = db.table(&reference.name)?;
    let mut table = PlanTable::new(schema.id, schema, reference.alias.clone());
    table.sample = reference.sample.clone();
    table.explicit_partition_storage_ids =
        crate::engine::executor::partition_clause::resolve_partition_clause(
            db,
            &reference.name,
            &reference.partitions,
        )?;
    let plan = PhysicalPlan::SampleScan {
        table,
        filter: None,
        estimated_rows: 0.0,
        access_rows: 0.0,
        cost: crate::engine::cbo::Cost::ZERO,
        possible_keys: Vec::new(),
        runtime_filters: Vec::new(),
        hint_note: None,
        projected_columns: Some(Arc::new(Vec::new())),
    };
    let mut stream = SampleBatchStream::new(db, txn, &plan);
    stream.start()?;
    let mut rows = Vec::new();
    let mut charge = ScopedCharge::new(0);
    while stream.next_refs()? {
        mem_budget::try_reserve_vec_hard(
            &mut rows,
            stream.pending.len(),
            &mut charge,
            "locking sample origins",
        )?;
        rows.extend(stream.pending.by_ref());
    }
    Ok((rows, charge))
}
impl BatchOperator for SampleBatchStream {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.start()?;
        let mut accumulators: Vec<_> = self
            .schema
            .fields()
            .iter()
            .map(|field| ColumnAccumulator::for_field(field, self.batch_rows))
            .collect();
        let mut references = Vec::new();
        let mut charge = ScopedCharge::new(0);
        mem_budget::try_reserve_vec_hard(
            &mut references,
            self.batch_rows,
            &mut charge,
            "sample fetch window",
        )?;
        while references.len() < self.batch_rows {
            crate::engine::cancel::check_cancelled()?;
            if self.pending.len() == 0 {
                // A row sampler publishes bounded prefixes, even when they
                // do not fill a batch. Never wait for EOF at low percentages.
                if !references.is_empty()
                    && matches!(&self.refs,Some(Refs::Rows(scan)) if !scan.full_percent)
                {
                    break;
                }
                if !self.next_refs()? {
                    break;
                }
            }
            references.push(self.pending.next().unwrap());
        }
        let row_sample = matches!(self.refs, Some(Refs::Rows(_)));
        let row_order = matches!(&self.refs,Some(Refs::Rows(scan)) if !scan.full_percent);
        let mut physical_order = Vec::new();
        let mut order_charge = ScopedCharge::new(0);
        if row_order {
            mem_budget::try_reserve_vec_hard(
                &mut physical_order,
                references.len(),
                &mut order_charge,
                "sample identity order",
            )?;
            physical_order.extend(
                references
                    .iter()
                    .copied()
                    .enumerate()
                    .map(|(i, row)| (row, i)),
            );
            physical_order.sort_unstable_by_key(|(row, _)| *row);
        }
        references.sort_unstable();
        let mut fetched = Vec::new();
        let mut fetched_charge = ScopedCharge::new(0);
        if row_order && !self.columns.is_empty() {
            mem_budget::try_reserve_vec_hard(
                &mut fetched,
                references.len(),
                &mut fetched_charge,
                "sample output order",
            )?;
        }
        let mut count = 0;
        if self.columns.is_empty() {
            count = references.len();
        } else {
            let mut first = 0;
            while first < references.len() {
                let storage_id = references[first].storage_id;
                let end = fetch_window_end(
                    &references,
                    first,
                    self.table.sample.as_ref().unwrap().method == SampleMethod::System,
                );
                let ids = references[first..end]
                    .iter()
                    .map(|row| row.row_id)
                    .collect::<Vec<_>>();
                let heap = self.db.cached_heap_handle(storage_id)?;
                let rows = self.db.read_row_columns_many_with_handle(
                    &heap,
                    &self.table.schema,
                    &ids,
                    self.txn,
                    &self.columns,
                )?;
                for (row_id, values) in rows {
                    if row_order {
                        let identity = DmlRowRef { storage_id, row_id };
                        let position = physical_order
                            .binary_search_by_key(&identity, |(row, _)| *row)
                            .map(|i| physical_order[i].1)
                            .map_err(|_| invalid("unexpected sampled payload identity"))?;
                        fetched_charge
                            .add_non_spillable(mem_budget::estimate_row_bytes(&values))?;
                        fetched.push((position, values));
                    } else {
                        for (column, value) in accumulators.iter_mut().zip(values.iter()) {
                            column.push(value);
                        }
                    }
                    count += 1;
                }
                first = end;
            }
        }
        if row_order && !self.columns.is_empty() {
            fetched.sort_unstable_by_key(|(position, _)| *position);
            for (_, values) in fetched {
                for (column, value) in accumulators.iter_mut().zip(values.iter()) {
                    column.push(value);
                }
            }
        }
        if !self.columns.is_empty() {
            if let Some(Refs::Rows(scan)) = &mut self.refs {
                scan.stats.decoded_rows += count as u64;
            }
        }
        if count == 0 {
            return Ok(None);
        }
        let columns = accumulators
            .into_iter()
            .map(ColumnAccumulator::finish_validated)
            .collect::<Result<Vec<_>, _>>()?;
        // Publish the first bounded prefix promptly, then amortize payload
        // probes and downstream exchanges when the consumer keeps pulling.
        // The captured LIMIT/budget ceiling remains authoritative.
        if row_sample {
            self.batch_rows = self.batch_rows.saturating_mul(2).min(self.max_batch_rows);
        }
        Ok(Some(Batch::with_row_count(
            self.schema.clone(),
            columns,
            count,
        )))
    }
}

pub(in crate::engine::executor::select) fn open(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<Box<dyn super::super::plan::PlanStream>, QueryError> {
    Ok(Box::new(Rows {
        stream: SampleBatchStream::new(db, txn, plan),
        rows: Vec::new().into_iter(),
    }))
}
struct Rows {
    stream: SampleBatchStream,
    rows: std::vec::IntoIter<super::super::plan::PlanRow>,
}
impl super::super::plan::PlanStream for Rows {
    fn next(&mut self) -> Result<Option<super::super::plan::PlanRow>, QueryError> {
        loop {
            if let Some(row) = self.rows.next() {
                return Ok(Some(row));
            }
            let Some(batch) = self.stream.next_batch()? else {
                return Ok(None);
            };
            let mut rows = Vec::new();
            for index in batch.visible_indices() {
                let values = batch.columns().iter().map(|c| c.value_at(index)).collect();
                rows.push(super::super::plan::projected_values_to_plan_row(
                    &self.stream.table,
                    &self.stream.columns,
                    values,
                )?);
            }
            self.rows = rows.into_iter();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tablesample_payload_windows_never_bridge_unselected_pages_or_heaps() {
        for seed in 0..64 {
            let mut references = Vec::new();
            for storage_id in [7, 11] {
                for page_id in 0..129 {
                    if selected(page_priority(seed, storage_id, page_id), 85.0) {
                        for slot_id in [0, 3] {
                            references.push(DmlRowRef {
                                storage_id,
                                row_id: RowId::new(page_id as u32, slot_id),
                            });
                        }
                    }
                }
            }
            let selected_pages: std::collections::HashSet<_> = references
                .iter()
                .map(|r| (r.storage_id, r.row_id.page_id))
                .collect();
            let mut first = 0;
            while first < references.len() {
                let end = fetch_window_end(&references, first, true);
                let a = references[first];
                let b = references[end - 1];
                assert_eq!(a.storage_id, b.storage_id);
                for page in a.row_id.page_id..=b.row_id.page_id {
                    assert!(selected_pages.contains(&(a.storage_id, page)));
                }
                first = end;
            }
        }
    }
}
