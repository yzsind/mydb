//! Columnar adapter for fused indexed existence checks. Only key/comparison
//! lanes are read as Values. Sparse input is coalesced; dense and wide
//! payloads retain shared source buffers.
use super::*;

#[cfg(test)]
pub(crate) static FUSED_INLJ_BATCH_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

struct FusedNeqBatchOperator<'a> {
    plan: Option<&'a PhysicalPlan>,
    db: &'a Arc<Database>,
    txn: TxnView,
    upstream: Box<dyn BatchOperator + 'a>,
    spec: super::super::plan::FusedNeqBatchSpec,
    schema: Arc<BatchSchema>,
    output_columns: Vec<usize>,
    cursor: InljBatchCursor,
    output_charge: Option<InljChunkCharge>,
    global_checked: bool,
    replay: std::collections::VecDeque<Batch>,
    retained_charge: Option<mem_budget::ScopedCharge>,
    retained_area: Option<crate::engine::executor::work_area::WorkAreaHandle>,
    replay_input: Option<MaterializedBatchOperator>,
    replay_cursor: InljBatchCursor,
    global_output: Option<std::collections::VecDeque<Batch>>,
}

#[derive(Default)]
struct InljBatchCursor {
    current: Option<Batch>,
    visible: Vec<usize>,
    position: usize,
    finished: bool,
}

impl InljBatchCursor {
    fn advance(&mut self, upstream: &mut dyn BatchOperator) -> Result<bool, QueryError> {
        while self.position == self.visible.len() {
            self.current = None;
            if self.finished {
                return Ok(false);
            }
            crate::engine::cancel::check_cancelled()?;
            let Some(batch) = upstream.next_batch()? else {
                self.finished = true;
                return Ok(false);
            };
            self.visible.clear();
            self.visible.extend(batch.visible_indices());
            self.position = 0;
            self.current = Some(batch);
        }
        Ok(true)
    }

    /// Fill a resource-bounded probe chunk across sparse upstream batches.
    /// Source morsels are decode units, not MRR/parallel-probe task sizes.
    fn pull(
        &mut self,
        upstream: &mut dyn BatchOperator,
        work_bytes_per_row: usize,
    ) -> Result<Option<(Batch, InljChunkCharge)>, QueryError> {
        // Open the producer before reserving probe scratch: a blocking child
        // must be able to finish its own build using the same work-area pool.
        if !self.advance(upstream)? {
            return Ok(None);
        }
        let mut charge = InljChunkCharge::new();
        let manager = charge.area.manager();
        let available = manager
            .target_bytes()
            .saturating_sub(manager.allocated_bytes());
        let own_limit = mem_budget::current_budget().map_or(INLJ_BATCH_WORK_BYTES, |b| b.limit());
        let schema = upstream.schema().clone();
        let row_bytes = work_bytes_per_row
            .saturating_add(schema.len().saturating_mul(std::mem::size_of::<Value>()));
        let byte_limit = available.min(own_limit).min(INLJ_BATCH_WORK_BYTES);
        let target = (byte_limit / row_bytes.max(1))
            .max(1)
            .min(fused_inlj_stream_chunk_rows());
        let current = self.current.as_ref().expect("visible INLJ input");
        let remaining = self.visible.len() - self.position;
        // Dense inputs already fill a probe task. Wide payloads must keep the
        // source's shared buffers rather than doubling their resident bytes.
        if remaining >= target || current.memory_bytes() > byte_limit / 2 {
            let mut count = target.min(remaining);
            if !charge.add(count.saturating_mul(work_bytes_per_row))? {
                drop(charge);
                charge = InljChunkCharge::new();
                charge.add(work_bytes_per_row)?;
                count = 1;
            }
            let end = self.position + count;
            let selected = self.visible[self.position..end]
                .iter()
                .map(|&r| r as u32)
                .collect();
            self.position = end;
            return Ok(Some((
                current.clone().with_selection(Selection::Indices(selected)),
                charge,
            )));
        }
        let mut columns = vec![ColumnVector::Null { len: 0 }; schema.len()];
        let mut rows = 0;
        let mut payload_bytes = 0;
        loop {
            crate::engine::cancel::check_cancelled()?;
            let count = (target - rows).min(self.visible.len() - self.position);
            let end = self.position + count;
            let batch = self.current.as_ref().expect("visible INLJ input");
            for (column, source) in columns.iter_mut().zip(batch.columns()) {
                column.append_rows_from(source, &self.visible[self.position..end]);
            }
            self.position = end;
            rows += count;
            let bytes = columns
                .iter()
                .fold(0usize, |total, c| total.saturating_add(c.memory_bytes()));
            // Track owned payload capacities as well as the probe envelope.
            // Stop at the crossing fragment; consumed rows are never retried.
            let admitted = charge.add(
                count
                    .saturating_mul(work_bytes_per_row)
                    .saturating_add(bytes.saturating_sub(payload_bytes)),
            )?;
            payload_bytes = bytes;
            let charged = charge.charge.charged();
            let average = charged.div_ceil(rows);
            if !admitted
                || rows == target
                || byte_limit.saturating_sub(charged) < average
                || !self.advance(upstream)?
            {
                break;
            }
        }
        Ok(Some((Batch::with_row_count(schema, columns, rows), charge)))
    }
}

pub(super) fn try_build_fused_neq_batch_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let Some(left) = super::super::plan::inlj_consecutive_semi_anti_left(plan) else {
        return Ok(None);
    };
    let left_template = plan_output_template(left);
    let Some(spec) = super::super::plan::compile_fused_neq_batch_spec(plan, &left_template) else {
        return Ok(None);
    };
    let template = plan_output_template(plan);
    let Some(output_columns) = template
        .iter()
        .map(|field| {
            let qualifier = field.meta.alias.as_deref().unwrap_or(&field.meta.table);
            expr_column_index_in_template(
                &Expr::CompoundIdentifier(vec![qualifier.into(), field.meta.column.clone()]),
                &left_template,
            )
        })
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let Some(mut input) = try_native_vec_input_stream(db, txn, left)? else {
        return Ok(None);
    };
    // Preflight precedes every read. A privately extended child schema must
    // not reinterpret the positions compiled against the physical template.
    if input.template.len() != left_template.len()
        || input.template.iter().zip(&left_template).any(|(a, b)| {
            a.meta.table != b.meta.table
                || a.meta.alias != b.meta.alias
                || a.meta.column != b.meta.column
                || a.meta.column_type != b.meta.column_type
        })
    {
        return Ok(None);
    }
    if let Some(column) = spec.locality_column() {
        input.stream.prefer_key_locality(&[column]);
    }
    let schema = template_to_batch_schema(&template);
    let global_checked = !spec_global_probe_eligible(plan, &spec)
        || output_columns.len() != left_template.len()
        || output_columns
            .iter()
            .enumerate()
            .any(|(column, &source)| column != source);
    Ok(Some(VecBatchInput {
        stream: Box::new(FusedNeqBatchOperator {
            plan: Some(plan),
            db,
            txn,
            upstream: input.stream,
            spec,
            schema,
            output_columns,
            cursor: InljBatchCursor::default(),
            output_charge: None,
            global_checked,
            replay: Default::default(),
            retained_charge: None,
            retained_area: None,
            replay_input: None,
            replay_cursor: Default::default(),
            global_output: None,
        }),
        template,
    }))
}

fn spec_global_probe_eligible(
    plan: &PhysicalPlan,
    spec: &super::super::plan::FusedNeqBatchSpec,
) -> bool {
    spec.global_probe_eligible()
        && scan_row_goal().is_none()
        && super::super::plan::inlj_consecutive_semi_anti_left(plan).is_some_and(|left| {
            let rows = crate::planner::estimated_rows(left);
            let cap = mem_budget::current_budget().map_or(0, |budget| budget.limit() / 2);
            rows >= 65_536.0 && left.output_width() * rows <= cap as f64
        })
}

impl FusedNeqBatchOperator<'_> {
    fn initialize_global(&mut self) -> Result<(), QueryError> {
        self.global_checked = true;
        let cap = mem_budget::current_budget().map_or(0, |budget| budget.limit() / 2);
        let area = crate::engine::executor::work_area::WorkAreaHandle::register();
        let mut charge = mem_budget::ScopedCharge::new(0);
        let mut retained_bytes = 0usize;
        while let Some(batch) = self.upstream.next_batch()? {
            let resident = batch.memory_bytes().saturating_add(256);
            retained_bytes = retained_bytes.saturating_add(resident);
            // Owning scan/join batches carry their own leases. Preserve those
            // leases and charge only buffers whose producer used a transient
            // guard; selection views must not double-charge shared payloads.
            let bytes = resident.saturating_sub(batch.accounted_query_bytes());
            let admitted = retained_bytes <= cap && charge.add_within(bytes, cap);
            if !admitted {
                // This batch has already been consumed. Keep it with the
                // complete prefix for exact replay through the bounded path.
                charge.add_force(bytes)?;
            }
            self.replay.push_back(batch);
            if !admitted
                || matches!(
                    area.try_charge(resident),
                    crate::engine::executor::work_area::SpillHint::Spill
                )
            {
                self.retained_charge = Some(charge);
                self.retained_area = Some(area);
                self.replay_input = Some(MaterializedBatchOperator::new(
                    self.upstream.schema().clone(),
                    self.replay.drain(..).collect(),
                ));
                return Ok(());
            }
        }
        let schema = self.upstream.schema().clone();
        drop(std::mem::replace(
            &mut self.upstream,
            Box::new(MaterializedBatchOperator::new(schema, Vec::new())),
        ));
        let batches = self.replay.make_contiguous();
        let Some(selections) = super::super::plan::try_select_global_fused_neq_batches(
            self.db, self.txn, &self.spec, batches,
        )?
        else {
            self.retained_charge = Some(charge);
            self.retained_area = Some(area);
            self.replay_input = Some(MaterializedBatchOperator::new(
                self.upstream.schema().clone(),
                self.replay.drain(..).collect(),
            ));
            return Ok(());
        };
        let mut output = std::collections::VecDeque::new();
        charge.add_force(selections.iter().fold(0usize, |bytes, selected| {
            bytes
                .saturating_add(selected.capacity().saturating_mul(4))
                .saturating_add(256)
        }))?;
        for (mut batch, selected) in self.replay.drain(..).zip(selections) {
            if selected.is_empty() {
                continue;
            }
            batch.schema = self.schema.clone();
            output.push_back(batch.with_selection(Selection::Indices(selected)));
        }
        self.retained_charge = Some(charge);
        self.retained_area = Some(area);
        self.global_output = Some(output);
        if let Some(plan) = self.plan {
            super::super::analyze_stats::add_join_strategy(plan, "fused_domain_scan");
        }
        Ok(())
    }
}

impl BatchOperator for FusedNeqBatchOperator<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }
    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        self.upstream.install_runtime_filter(filter)
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.output_charge = None;
        if !self.global_checked {
            self.initialize_global()?;
        }
        if let Some(output) = self.global_output.as_mut() {
            let batch = output.pop_front();
            if batch.is_none() {
                self.retained_charge = None;
                self.retained_area = None;
            }
            return Ok(batch);
        }
        loop {
            crate::engine::cancel::check_cancelled()?;
            let next = if let Some(input) = self.replay_input.as_mut() {
                let next = self
                    .replay_cursor
                    .pull(input, self.spec.work_bytes_per_row)?;
                if next.is_none() {
                    self.replay_input = None;
                    self.retained_charge = None;
                    self.retained_area = None;
                    continue;
                }
                next
            } else {
                self.retained_charge = None;
                self.cursor
                    .pull(self.upstream.as_mut(), self.spec.work_bytes_per_row)?
            };
            let Some((batch, charge)) = next else {
                return Ok(None);
            };
            let visible: Vec<usize> = batch.visible_indices().collect();
            #[cfg(test)]
            FUSED_INLJ_BATCH_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let selected = super::super::plan::select_fused_neq_batch(
                self.db, self.txn, &self.spec, &batch, &visible,
            )?;
            if selected.is_empty() {
                continue;
            }
            let identity = self.output_columns.len() == batch.columns().len()
                && self.output_columns.iter().enumerate().all(|(a, b)| a == *b);
            let mut output = if identity && selected.len() == batch.visible_rows() {
                batch
            } else {
                Batch::with_row_count(
                    self.schema.clone(),
                    self.output_columns
                        .iter()
                        .map(|&column| batch.columns()[column].take_indices(&selected))
                        .collect(),
                    selected.len(),
                )
            };
            output.schema = self.schema.clone();
            self.output_charge = Some(charge);
            return Ok(Some(output));
        }
    }
}

#[cfg(test)]
mod tests;
