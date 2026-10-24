//! Columnar adapter for fused indexed existence checks. Only key/comparison
//! lanes are read as Values. Sparse input is coalesced; dense and wide
//! payloads retain shared source buffers.
use super::*;

#[cfg(test)]
pub(crate) static FUSED_INLJ_BATCH_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

struct FusedNeqBatchOperator<'a> {
    db: &'a Arc<Database>,
    txn: TxnView,
    upstream: Box<dyn BatchOperator + 'a>,
    spec: super::super::plan::FusedNeqBatchSpec,
    schema: Arc<BatchSchema>,
    output_columns: Vec<usize>,
    cursor: InljBatchCursor,
    output_charge: Option<InljChunkCharge>,
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
    Ok(Some(VecBatchInput {
        stream: Box::new(FusedNeqBatchOperator {
            db,
            txn,
            upstream: input.stream,
            spec,
            schema,
            output_columns,
            cursor: InljBatchCursor::default(),
            output_charge: None,
        }),
        template,
    }))
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
        loop {
            crate::engine::cancel::check_cancelled()?;
            let Some((batch, charge)) = self
                .cursor
                .pull(self.upstream.as_mut(), self.spec.work_bytes_per_row)?
            else {
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
