//! Budgeted columnar page selection, with replay into the governed sorter.
//! Only row handles and canonical encoded keys are partitioned. Payload stays
//! in producer batches, optionally spooled once; only the final page is decoded.

use super::*;
use crate::engine::executor::sort::{
    key_encode::{BorrowedKey, RowEncoder},
    SortField,
};
use std::collections::VecDeque;

mod fixed;
mod payload;
use payload::Spool;

#[cfg(test)]
pub(crate) static RESIDENT_WINDOW_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(test)]
pub(crate) static RESIDENT_WINDOW_REPLAYS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(test)]
pub(crate) static RESIDENT_WINDOW_REPLAYS_WITH_PREFIX: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn signed_interval(expr: &Expr, template: &[PlanField]) -> Option<(i128, i128)> {
    if let Some(column) = expr_column_index_in_template(expr, template) {
        let field = &template[column];
        if field.source.flags & 0x20 != 0 {
            return None;
        }
        let bits = match field.column_type {
            ColumnType::Tiny => 8,
            ColumnType::Short => 16,
            ColumnType::MediumInt => 24,
            ColumnType::Long => 32,
            ColumnType::LongLong => 64,
            _ => return None,
        };
        return Some((-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1));
    }
    let constant = match expr {
        Expr::Literal(crate::parser::Literal::Int(value)) => Some(*value),
        Expr::BoundValue(Value::Int(value)) => Some(*value),
        Expr::Placeholder(_) => match crate::engine::expr::expr_literal_value(expr)? {
            Value::Int(value) => Some(value),
            _ => None,
        },
        _ => None,
    };
    if let Some(value) = constant {
        return Some((value as i128, value as i128));
    }
    let bounds = match expr {
        Expr::Binary { left, op, right } => {
            let (a, b) = signed_interval(left, template)?;
            let (c, d) = signed_interval(right, template)?;
            match op {
                BinaryOp::Add => (a + c, b + d),
                BinaryOp::Sub => (a - d, b - c),
                BinaryOp::Mul => {
                    let products = [a * c, a * d, b * c, b * d];
                    (*products.iter().min()?, *products.iter().max()?)
                }
                _ => return None,
            }
        }
        Expr::Unary {
            op: crate::parser::UnaryOp::Neg,
            expr,
        } => {
            let (a, b) = signed_interval(expr, template)?;
            (-b, -a)
        }
        _ => return None,
    };
    (bounds.0 >= i64::MIN as i128 && bounds.1 <= i64::MAX as i128).then_some(bounds)
}

/// Optional speculation may evaluate a consumed prefix again after a quota
/// miss. Require immutable, diagnostic-free keys; arithmetic gets the same
/// guarantee only after proving its complete declared integer domain fits.
pub(super) fn preflight(
    input: &PhysicalPlan,
    order_by: &[OrderBy],
) -> Result<Option<TopNKeyPlan>, QueryError> {
    if !crate::engine::executor::sort::key_encode::sort_key_encode_enabled() {
        return Ok(None);
    }
    fn covering_source(input: &PhysicalPlan) -> bool {
        match input {
            PhysicalPlan::IndexFastScan { .. }
            | PhysicalPlan::ParallelIndexFastScan { .. }
            | PhysicalPlan::IndexRangeScan { covering: true, .. }
            | PhysicalPlan::SeqScan { .. }
            | PhysicalPlan::ParallelSeqScan { .. }
            | PhysicalPlan::ZoneMapScan { .. }
            | PhysicalPlan::ParallelZoneMapScan { .. }
            // The join has already evaluated matches and residuals into
            // authoritative batches. Quota fallback replays those batches,
            // so admitting them never repeats probes or changes multiplicity.
            | PhysicalPlan::HashJoin { .. } => true,
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Exchange { input, .. }
            | PhysicalPlan::Projection {
                input,
                distinct: false,
                ..
            } => covering_source(input),
            PhysicalPlan::SetOp { .. } => true,
            PhysicalPlan::CteScan { .. } | PhysicalPlan::IndexNestedLoopJoin { .. } => {
                super::super::plan::materialized_lookup_row_bound(input)
                    .is_some_and(|rows| rows <= 256)
            }
            _ => false,
        }
    }
    if !covering_source(input) {
        return Ok(None);
    }
    let Some(keys) = topn_key_preflight(input, order_by)? else {
        return Ok(None);
    };
    for (source, order) in keys.sources.iter().zip(order_by) {
        match source {
            TopNKeySource::Column(column) => {
                let field = &keys.template[*column];
                if matches!(
                    field.column_type,
                    ColumnType::Json | ColumnType::Geometry | ColumnType::Blob
                ) || (matches!(field.column_type, ColumnType::VarString)
                    && field.source.collation_id == Some(63))
                {
                    return Ok(None);
                }
            }
            TopNKeySource::Program { program, .. } => {
                let capability = program.capability();
                if !capability.deterministic
                    || capability.depends_on_session
                    || !(signed_interval(&order.expr, &keys.template).is_some()
                        || (!capability.may_error && program.has_only_value_string_operations()))
                    || crate::engine::executor::vec::evaluated_batch::result_bound(program)
                        .is_none()
                {
                    return Ok(None);
                }
            }
            TopNKeySource::UnseededRand(_) => return Ok(None),
        }
    }
    Ok(Some(keys))
}

#[derive(Clone, Copy)]
struct Entry {
    batch: u32,
    row: u32,
    start: u32,
    end: u32,
    ordinal: u32,
}

#[inline(never)]
fn canonical_decimal_key(value: Value, column_type: ColumnType) -> Option<Value> {
    let ColumnType::Decimal { precision, scale } = column_type else {
        return Some(value);
    };
    if matches!(value, Value::Null) || precision > 38 {
        return Some(value);
    }
    use crate::util::decimal::{rescale_trunc_i128, ExactDecimal};
    let (magnitude, from) = match value.exact_decimal()? {
        ExactDecimal::I64(magnitude, from) => (i128::from(magnitude), from),
        ExactDecimal::I128(magnitude, from) => (magnitude, from),
        ExactDecimal::Big(..) => return None,
    };
    let scaled = rescale_trunc_i128(magnitude, from, scale)?;
    // Metadata never licenses rounding during comparison. Decline any
    // runtime value whose extra fractional digits are not exactly zero.
    if from > scale && rescale_trunc_i128(scaled, scale, from)? != magnitude {
        return None;
    }
    if precision <= 18 {
        Some(Value::DecimalCompact(i64::try_from(scaled).ok()?, scale))
    } else {
        Some(Value::Decimal128(scaled, scale))
    }
}

struct Replay<'a> {
    prefix: VecDeque<(Option<Batch>, usize)>,
    spool: Option<Spool>,
    position: usize,
    input: Box<dyn BatchOperator + 'a>,
    charge: mem_budget::ScopedCharge,
}
impl BatchOperator for Replay<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        self.input.schema()
    }
    fn properties(&self) -> BatchProperties {
        self.input.properties()
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if let Some((batch, bytes)) = self.prefix.pop_front() {
            let index = self.position;
            self.position += 1;
            self.charge.release_bytes(bytes);
            return batch.map_or_else(
                || {
                    self.spool
                        .as_ref()
                        .expect("spooled page prefix")
                        .read(index, self.input.schema().clone())
                        .map(Some)
                },
                |batch| Ok(Some(batch)),
            );
        }
        self.input.next_batch()
    }
}

struct Window<'a> {
    input: Option<Box<dyn BatchOperator + 'a>>,
    schema: Arc<BatchSchema>,
    keys: TopNKeyPlan,
    fields: Arc<[SortField]>,
    offset: usize,
    limit: usize,
    estimated_rows: f64,
    data_dir: std::path::PathBuf,
    output: Option<Box<dyn BatchOperator + 'a>>,
    preserve_value_shape: bool,
}

struct ResidentOutput {
    schema: Arc<BatchSchema>,
    properties: BatchProperties,
    batches: Vec<(Option<Batch>, usize)>,
    spool: Option<Spool>,
    loaded: Option<(u32, Batch)>,
    entries: std::vec::IntoIter<Entry>,
    _charge: mem_budget::ScopedCharge,
    preserve_value_shape: bool,
}
impl BatchOperator for ResidentOutput {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let count = self.entries.len().min(DEFAULT_BATCH_ROWS);
        if count == 0 {
            return Ok(None);
        }
        let mut charge = mem_budget::force_charge_scoped(
            count
                .saturating_mul(self.schema.len())
                .saturating_mul(std::mem::size_of::<Value>())
                .saturating_add(1024),
        )?;
        let mut columns = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                if self.preserve_value_shape {
                    // Materialized expressions may retain a Value domain
                    // different from their protocol flags (for example COUNT).
                    // Infer the output lane from those exact runtime values.
                    ColumnAccumulator::Generic {
                        values: Vec::with_capacity(count),
                    }
                } else {
                    ColumnAccumulator::for_field(field, count)
                }
            })
            .collect::<Vec<_>>();
        for entry in self.entries.by_ref().take(count) {
            let batch = if let Some(batch) = self.batches[entry.batch as usize].0.as_ref() {
                batch
            } else {
                if self
                    .loaded
                    .as_ref()
                    .is_none_or(|(index, _)| *index != entry.batch)
                {
                    self.loaded = None;
                    let batch = self
                        .spool
                        .as_ref()
                        .expect("spooled page payload")
                        .read(entry.batch as usize, self.schema.clone())?;
                    self.loaded = Some((entry.batch, batch));
                }
                &self.loaded.as_ref().expect("loaded page payload").1
            };
            for (i, column) in columns.iter_mut().enumerate() {
                charge.add_with_workspace(
                    batch.columns()[i]
                        .owned_value_bytes(entry.row as usize)
                        .saturating_mul(2),
                )?;
                let value = batch.columns()[i]
                    .typed_value_at(entry.row as usize, self.schema.fields()[i].column_type);
                column.push(&value);
            }
        }
        let batch = Batch::with_row_count(
            self.schema.clone(),
            columns
                .into_iter()
                .map(|column| match column {
                    ColumnAccumulator::Generic { values } if self.preserve_value_shape => {
                        ColumnVector::from_values(values)
                    }
                    column => column.finish(),
                })
                .collect(),
            count,
        );
        Ok(Some(batch.with_memory_charge(charge)?))
    }
}

fn retain_batch(
    batches: &mut Vec<(Option<Batch>, usize)>,
    batch: Batch,
    bytes: usize,
    spool: &mut Option<Spool>,
    spill: bool,
    data_dir: &std::path::Path,
) -> Result<(), QueryError> {
    if batches.len() == batches.capacity() {
        // Per-batch admission already covers twice the metadata width.
        let additional = batches.capacity().max(1);
        mem_budget::try_reserve_vec_exact(batches, additional, "resident page batches")?;
    }
    if spill {
        if spool.is_none() {
            *spool = Some(Spool::new(data_dir)?);
        }
        spool
            .as_ref()
            .expect("payload spool initialized")
            .store(batches.len(), &batch)?;
        batches.push((None, bytes));
    } else {
        batches.push((Some(batch), bytes));
    }
    Ok(())
}

fn reserve<T>(
    values: &mut Vec<T>,
    additional: usize,
    charge: &mut mem_budget::ScopedCharge,
    cap: usize,
) -> Result<bool, QueryError> {
    let Some(required) = values.len().checked_add(additional) else {
        return Ok(false);
    };
    if required <= values.capacity() {
        return Ok(true);
    }
    let target = required.max(values.capacity().saturating_mul(2)).max(16);
    let extra = target
        .saturating_sub(values.capacity())
        .saturating_mul(std::mem::size_of::<T>());
    if !charge.add_within(extra, cap) {
        return Ok(false);
    }
    mem_budget::try_reserve_vec_exact(values, target - values.len(), "resident page window")?;
    Ok(true)
}

/// Copy only when the retained source has substantial unused capacity or
/// invisible rows. The input remains authoritative if optional admission is
/// refused; copying values never re-evaluates expressions or diagnostics.
#[inline(never)]
fn compact_retained_batch(batch: Batch) -> Result<Batch, QueryError> {
    let rows = batch.visible_rows();
    let value_header = std::mem::size_of::<Value>();
    let metadata = batch.columns().len().saturating_mul(
        std::mem::size_of::<ColumnVector>() + 32 + rows.div_ceil(8).saturating_mul(2),
    );
    let bound = batch.columns().iter().fold(
        rows.saturating_mul(std::mem::size_of::<u32>())
            .saturating_add(metadata)
            .saturating_add(256),
        |bytes, column| {
            let fixed = match column {
                ColumnVector::Null { .. } | ColumnVector::Bool { .. } => Some(0usize),
                ColumnVector::Date32 { .. } | ColumnVector::Decimal32 { .. } => Some(4),
                ColumnVector::I64 { .. }
                | ColumnVector::U64 { .. }
                | ColumnVector::F64 { .. }
                | ColumnVector::Time64 { .. }
                | ColumnVector::DateTime64 { .. }
                | ColumnVector::Timestamp64 { .. }
                | ColumnVector::Decimal64 { .. } => Some(8),
                ColumnVector::Decimal128 { .. } => Some(16),
                _ => None,
            };
            if let Some(width) = fixed {
                return bytes.saturating_add(rows.saturating_mul(width));
            }
            // Unselected variable lanes already expose their used byte span.
            // Only selections and individually allocated values need a row
            // walk; fixed lanes never inspect every value just to size a copy.
            if batch.selection().is_none() {
                match column {
                    ColumnVector::Utf8 { data, .. }
                    | ColumnVector::Bytes { data, .. }
                    | ColumnVector::Json { data, .. }
                    | ColumnVector::Geometry { data, .. }
                    | ColumnVector::Vector { data, .. } => {
                        return bytes
                            .saturating_add(data.len())
                            .saturating_add(rows.saturating_mul(4));
                    }
                    _ => {}
                }
            }
            batch.visible_indices().fold(bytes, |bytes, row| {
                let size = match column {
                    ColumnVector::DecimalWide { .. } => column
                        .owned_value_bytes(row)
                        .saturating_sub(value_header)
                        .max(std::mem::size_of::<crate::types::DecimalWide>() + 32),
                    ColumnVector::Generic { .. } => column.owned_value_bytes(row),
                    _ => column
                        .owned_value_bytes(row)
                        .saturating_sub(value_header)
                        .saturating_add(4),
                };
                bytes.saturating_add(size)
            })
        },
    );
    if batch.selection().is_none() && batch.resident_bytes() <= bound.saturating_mul(3) / 2 {
        return Ok(batch);
    }
    let mut charge = batch.memory_charge();
    if !charge.add_within(bound, charge.remaining().saturating_add(charge.charged())) {
        return Ok(batch);
    }
    let mut indices = mem_budget::try_vec_with_capacity(rows, "resident visible row positions")?;
    indices.extend(batch.visible_indices().map(|row| row as u32));
    let columns = batch
        .columns()
        .iter()
        .map(|column| column.take_indices_exact(&indices))
        .collect();
    let output = Batch::with_row_count(batch.schema().clone(), columns, rows);
    drop(indices);
    drop(batch);
    output.with_memory_charge(charge)
}

impl<'a> Window<'a> {
    fn spill(
        &mut self,
        input: Box<dyn BatchOperator + 'a>,
        prefix: Vec<(Option<Batch>, usize)>,
        charge: mem_budget::ScopedCharge,
        spool: Option<Spool>,
    ) {
        #[cfg(test)]
        RESIDENT_WINDOW_REPLAYS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        #[cfg(test)]
        if !prefix.is_empty() {
            RESIDENT_WINDOW_REPLAYS_WITH_PREFIX.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let sources = self
            .keys
            .sources
            .iter()
            .map(|source| match source {
                TopNKeySource::Column(column) => vec_sort::SortKeySource::Column(*column),
                TopNKeySource::Program { program, integer } => vec_sort::SortKeySource::Program {
                    program: program.clone(),
                    integer: integer.clone(),
                },
                TopNKeySource::UnseededRand(_) => unreachable!("immutable key preflight"),
            })
            .collect();
        self.output = Some(Box::new(
            vec_sort::ExternalSortBatchOperator::new(
                None,
                Box::new(Replay {
                    prefix: prefix.into(),
                    spool,
                    position: 0,
                    input,
                    charge,
                }),
                self.schema.clone(),
                self.keys.properties.clone(),
                sources,
                self.fields.to_vec(),
                &self.data_dir,
            )
            .with_topn(self.offset.saturating_add(self.limit), self.offset),
        ));
    }

    fn initialize<const DECIMAL_KEYS: bool>(&mut self) -> Result<(), QueryError> {
        if self.output.is_some() {
            return Ok(());
        }
        let mut input = self.input.take().expect("page source opens once");
        if self.limit == 0 {
            self.output = Some(Box::new(MaterializedBatchOperator::new(
                self.schema.clone(),
                Vec::new(),
            )));
            return Ok(());
        }
        let cap = mem_budget::current_budget().map_or(128 * 1024 * 1024, |budget| {
            budget.remaining().saturating_mul(3) / 4
        });
        let mut payload = mem_budget::ScopedCharge::new(0);
        let mut resident_bytes = 0usize;
        let mut work = mem_budget::ScopedCharge::new(0);
        let mut batches = Vec::new();
        let mut spool = None;
        let mut entries = Vec::new();
        let mut arena = Vec::new();
        let mut encoded = Vec::new();
        let mut fixed_mode = None;
        let mut scratch = mem_budget::ScopedCharge::new(0);
        let Some(mut encoder) = RowEncoder::new(self.fields.clone()) else {
            self.spill(input, batches, payload, spool);
            return Ok(());
        };
        while let Some(batch) = input.next_batch()? {
            crate::engine::cancel::check_cancelled()?;
            let batch = compact_retained_batch(batch)?;
            let metadata = 512 * self.schema.len() + 256;
            let retained = batch.resident_bytes().saturating_add(metadata);
            // Reserve room for all encoded keys independently of wide payloads.
            // Once that payload share fills, write immutable producer batches
            // once and decode only batches referenced by the final page.
            // Do not spool a wide prefix when even its keys are predicted to
            // need external sorting. Observed key widths refine the estimate;
            // runtime admission remains authoritative if estimates are wrong.
            let key_bytes = std::mem::size_of::<Entry>().saturating_add(if entries.is_empty() {
                self.keys.sources.len().saturating_mul(16)
            } else {
                arena.len().div_ceil(entries.len())
            });
            let keys_may_fit = self.estimated_rows.is_finite()
                && self.estimated_rows > 0.0
                && self.estimated_rows * key_bytes as f64 * 2.0 <= (cap / 2) as f64;
            let spill_payload = batch.selection().is_none()
                && keys_may_fit
                && resident_bytes.saturating_add(retained) > cap / 2;
            resident_bytes =
                resident_bytes.saturating_add(if spill_payload { metadata } else { retained });
            let bytes = if spill_payload {
                metadata
            } else {
                retained.saturating_sub(batch.accounted_query_bytes())
            };
            // All reservations precede retention. A failed speculation owns
            // one bounded producer batch and replays it without reopening I/O.
            if resident_bytes.saturating_add(work.charged()) > cap
                || !payload.add_within(bytes, cap.saturating_sub(work.charged()))
            {
                drop(entries);
                drop(arena);
                drop(work);
                drop(encoder);
                payload.add_with_workspace(bytes)?;
                retain_batch(
                    &mut batches,
                    batch,
                    bytes,
                    &mut spool,
                    spill_payload,
                    &self.data_dir,
                )?;
                self.spill(input, batches, payload, spool);
                return Ok(());
            }
            let mut temporary = batch.memory_charge();
            let bound = self
                .keys
                .sources
                .iter()
                .try_fold(0usize, |sum, source| match source {
                    TopNKeySource::Column(_) => Some(sum),
                    TopNKeySource::Program { program, .. } => Some(sum.saturating_add(
                        crate::engine::executor::vec::evaluated_batch::result_bound(program)?,
                    )),
                    _ => None,
                })
                .expect("bounded keys preflighted");
            if !temporary.add_within(
                bound
                    .saturating_mul(batch.num_rows())
                    .saturating_add(self.keys.sources.len().saturating_mul(128))
                    .saturating_add(1024),
                temporary.remaining().saturating_add(temporary.charged()),
            ) {
                drop(entries);
                drop(arena);
                drop(work);
                drop(encoder);
                retain_batch(
                    &mut batches,
                    batch,
                    bytes,
                    &mut spool,
                    spill_payload,
                    &self.data_dir,
                )?;
                self.spill(input, batches, payload, spool);
                return Ok(());
            }
            let computed = self
                .keys
                .sources
                .iter()
                .map(|source| match source {
                    TopNKeySource::Column(_) => Ok(None),
                    TopNKeySource::Program { program, integer } => integer
                        .as_ref()
                        .and_then(|integer| {
                            integer.try_eval_batch_integer(batch.columns(), batch.num_rows())
                        })
                        .map(Some)
                        .map(Ok)
                        .unwrap_or_else(|| expr_batch::eval_to_column(program, &batch).map(Some)),
                    _ => unreachable!("immutable keys preflighted"),
                })
                .collect::<Result<Vec<_>, QueryError>>()?;
            // Validate each UTF-8 lane once, then borrow row slices. A lane
            // with invalid bytes keeps the canonical per-value error path.
            let key_columns = self
                .keys
                .sources
                .iter()
                .enumerate()
                .map(|(i, source)| match source {
                    TopNKeySource::Column(column) => &batch.columns()[*column],
                    _ => computed[i].as_ref().expect("computed sort key"),
                })
                .collect::<Vec<_>>();
            let key_types = self
                .keys
                .sources
                .iter()
                .map(|source| match source {
                    TopNKeySource::Column(column) => (
                        self.schema.fields()[*column].column_type,
                        self.schema.fields()[*column].source.flags,
                    ),
                    TopNKeySource::Program { program, .. } => {
                        (program.result_type().column_type(), 0)
                    }
                    _ => unreachable!("immutable keys preflighted"),
                })
                .collect::<Vec<_>>();
            let fixed_plan = fixed::Plan::bind(&key_columns, &key_types, &self.fields);
            let use_fixed = *fixed_mode.get_or_insert(fixed_plan.is_some());
            let text = key_columns
                .iter()
                .map(|column| match column {
                    ColumnVector::Utf8 { data, .. } => std::str::from_utf8(data).ok(),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let mut values = smallvec::SmallVec::<[BorrowedKey<'_>; 4]>::new();
            let mut admitted = !use_fixed || fixed_plan.is_some();
            if use_fixed {
                if let Some(plan) = fixed_plan.as_ref() {
                    let count = batch.visible_rows();
                    let work_cap = cap.saturating_sub(resident_bytes);
                    let encoded_bytes = plan.encoded_bytes(&batch);
                    admitted = u32::try_from(arena.len().saturating_add(encoded_bytes)).is_ok()
                        && u32::try_from(entries.len().saturating_add(count)).is_ok()
                        && u32::try_from(batches.len()).is_ok()
                        && reserve(&mut entries, count, &mut work, work_cap)?
                        && reserve(&mut arena, encoded_bytes, &mut work, work_cap)?;
                    if admitted {
                        for row in batch.visible_indices() {
                            let start = arena.len() as u32;
                            if !plan.encode(row, &mut arena) {
                                admitted = false;
                                break;
                            }
                            entries.push(Entry {
                                batch: batches.len() as u32,
                                row: row as u32,
                                start,
                                end: arena.len() as u32,
                                ordinal: entries.len() as u32,
                            });
                        }
                    }
                }
            }
            for row in batch.visible_indices().filter(|_| !use_fixed) {
                values.clear();
                let value_bound =
                    self.keys
                        .sources
                        .iter()
                        .enumerate()
                        .fold(0usize, |sum, (i, source)| {
                            let column = match source {
                                TopNKeySource::Column(column) => &batch.columns()[*column],
                                _ => computed[i].as_ref().expect("computed sort key"),
                            };
                            sum.saturating_add(column.owned_value_bytes(row))
                        });
                // Admit cloned Values and the worst-case collation encoding
                // before allocating either buffer. Reuse this high-water mark.
                let scratch_bound = value_bound.saturating_mul(18).saturating_add(1024);
                if scratch_bound > scratch.charged()
                    && !scratch.add_within(
                        scratch_bound - scratch.charged(),
                        scratch.remaining().saturating_add(scratch.charged()),
                    )
                {
                    admitted = false;
                    break;
                }
                for (i, source) in self.keys.sources.iter().enumerate() {
                    if !key_columns[i].is_null(row) {
                        if let (Some(text), ColumnVector::Utf8 { offsets, .. }) =
                            (text[i], key_columns[i])
                        {
                            if let Some(value) =
                                text.get(offsets[row] as usize..offsets[row + 1] as usize)
                            {
                                values.push(BorrowedKey::Text(value));
                                continue;
                            }
                        }
                    }
                    let value = match source {
                        TopNKeySource::Column(column) => {
                            let field_type = self.schema.fields()[*column].column_type;
                            let lane = &batch.columns()[*column];
                            let value = lane.try_value_at(row)?;
                            if DECIMAL_KEYS && matches!(field_type, ColumnType::Decimal { .. }) {
                                let Some(value) = canonical_decimal_key(value, field_type) else {
                                    admitted = false;
                                    break;
                                };
                                value
                            } else {
                                value
                            }
                        }
                        _ => computed[i]
                            .as_ref()
                            .expect("computed sort key")
                            .try_value_at(row)?,
                    };
                    values.push(BorrowedKey::Scalar(value));
                }
                if !admitted {
                    break;
                }
                let Some(bound) = RowEncoder::borrowed_bound(&values) else {
                    admitted = false;
                    break;
                };
                if bound > scratch.charged()
                    && !scratch.add_within(
                        bound - scratch.charged(),
                        scratch.remaining().saturating_add(scratch.charged()),
                    )
                {
                    admitted = false;
                    break;
                }
                if encoded.capacity() < bound {
                    let additional = bound - encoded.len();
                    mem_budget::try_reserve_vec_exact(
                        &mut encoded,
                        additional,
                        "encoded page key",
                    )?;
                }
                if !encoder.encode_borrowed(&values, &mut encoded) {
                    admitted = false;
                    break;
                }
                let work_cap = cap.saturating_sub(resident_bytes);
                if !reserve(&mut entries, 1, &mut work, work_cap)?
                    || !reserve(&mut arena, encoded.len(), &mut work, work_cap)?
                    || u32::try_from(arena.len().saturating_add(encoded.len())).is_err()
                    || u32::try_from(entries.len()).is_err()
                    || u32::try_from(batches.len()).is_err()
                {
                    admitted = false;
                    break;
                }
                let start = arena.len() as u32;
                arena.extend_from_slice(&encoded);
                entries.push(Entry {
                    batch: batches.len() as u32,
                    row: row as u32,
                    start,
                    end: arena.len() as u32,
                    ordinal: entries.len() as u32,
                });
            }
            drop(values);
            drop(fixed_plan);
            drop(text);
            drop(key_columns);
            drop(computed);
            drop(temporary);
            retain_batch(
                &mut batches,
                batch,
                bytes,
                &mut spool,
                spill_payload,
                &self.data_dir,
            )?;
            if !admitted {
                drop(entries);
                drop(arena);
                drop(work);
                drop(encoder);
                self.spill(input, batches, payload, spool);
                return Ok(());
            }
        }
        #[cfg(test)]
        RESIDENT_WINDOW_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let compare = |a: &Entry, b: &Entry| {
            arena[a.start as usize..a.end as usize]
                .cmp(&arena[b.start as usize..b.end as usize])
                .then_with(|| a.ordinal.cmp(&b.ordinal))
        };
        let end = self.offset.saturating_add(self.limit).min(entries.len());
        if self.offset < end {
            entries.select_nth_unstable_by(self.offset, compare);
            if end < entries.len() {
                entries[self.offset..].select_nth_unstable_by(end - self.offset, compare);
            }
            entries[self.offset..end].sort_unstable_by(compare);
        }
        let input_rows = entries.len();
        entries.truncate(end);
        entries.drain(..self.offset.min(end));
        let mut retained = work.split(
            entries
                .capacity()
                .saturating_mul(std::mem::size_of::<Entry>()),
        );
        payload.absorb(&mut retained);
        crate::engine::executor::select::eval::ORDER_BY_KEY_ROWS_PREPARED
            .fetch_add(input_rows as u64, std::sync::atomic::Ordering::Relaxed);
        self.output = Some(Box::new(ResidentOutput {
            schema: self.schema.clone(),
            properties: self.keys.properties.clone(),
            batches,
            spool,
            loaded: None,
            entries: entries.into_iter(),
            _charge: payload,
            preserve_value_shape: self.preserve_value_shape,
        }));
        Ok(())
    }
}

impl BatchOperator for Window<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn properties(&self) -> BatchProperties {
        self.keys.properties.clone()
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        // Keep decimal normalization outside the compiled hot loop for
        // every other key domain, without inspecting runtime row values.
        let decimal_keys = self.keys.sources.iter().any(|source| {
            matches!(source, TopNKeySource::Column(column)
                if matches!(self.schema.fields()[*column].column_type, ColumnType::Decimal { .. }))
        });
        if decimal_keys {
            self.initialize::<true>()?;
        } else {
            self.initialize::<false>()?;
        }
        self.output.as_mut().expect("page initialized").next_batch()
    }
}

pub(super) fn build<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    input: &'a PhysicalPlan,
    order_by: &[OrderBy],
    offset: usize,
    limit: usize,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let Some(mut keys) = preflight(input, order_by)? else {
        return Ok(None);
    };
    super::topn::bind_vector_order_keys_at_open(&mut keys, order_by)?;
    let Some(VecBatchInput { stream, template }) = try_native_vec_input_stream(db, txn, input)?
    else {
        return Ok(None);
    };
    let schema = stream.schema().clone();
    let fields = keys
        .specs
        .iter()
        .zip(&keys.collations)
        .map(|(spec, collation_id)| SortField {
            ascending: spec.ascending,
            nulls_first: spec.nulls_first,
            collation_id: *collation_id,
        })
        .collect::<Vec<_>>()
        .into();
    Ok(Some(VecBatchInput {
        stream: Box::new(Window {
            input: Some(stream),
            schema,
            keys,
            fields,
            offset,
            limit,
            estimated_rows: crate::planner::estimated_rows(input),
            data_dir: db.data_dir().to_path_buf(),
            output: None,
            preserve_value_shape: super::super::plan::materialized_lookup_row_bound(input)
                .is_some()
                || input.any(|node| matches!(node, PhysicalPlan::HashJoin { .. })),
        }),
        template,
    }))
}

/// Prefer linear key selection to a candidate heap only when the complete
/// narrow input is predicted to fit. Small-result queries retain their
/// bounded heap when keeping the input would consume the spill reserve.
pub(super) fn resident_cost_is_favorable(input: &PhysicalPlan, key_count: usize, k: usize) -> bool {
    fn index_source(input: &PhysicalPlan) -> bool {
        match input {
            PhysicalPlan::IndexFastScan { .. }
            | PhysicalPlan::ParallelIndexFastScan { .. }
            | PhysicalPlan::IndexRangeScan { covering: true, .. } => true,
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Projection {
                input,
                distinct: false,
                ..
            }
            | PhysicalPlan::Exchange { input, .. } => index_source(input),
            _ => false,
        }
    }
    // Small heap-scan pages already have worker-local candidate reduction.
    // Collecting every payload/key before selection serializes that efficient
    // path. Index-only inputs can still amortize their scan/adapter cost;
    // large windows continue competing by the complete resident footprint.
    if k < vec_topn::DEFAULT_MAX_TOPN_K && !index_source(input) {
        return false;
    }
    let rows = crate::planner::estimated_rows(input);
    let bytes = rows
        * (input.output_width() + key_count as f64 * 16.0 + std::mem::size_of::<Entry>() as f64)
        * 2.0;
    rows >= k.max(1).saturating_mul(32).max(10000) as f64
        && mem_budget::current_budget()
            .is_some_and(|budget| bytes <= (budget.remaining().saturating_mul(3) / 4) as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimal_key_canonicalization_never_rounds_runtime_values() {
        let field = ColumnType::Decimal {
            precision: 12,
            scale: 2,
        };
        for value in [
            Value::Int(12),
            Value::DecimalCompact(12, 0),
            Value::DecimalCompact(120, 1),
            Value::DecimalCompact(12000, 3),
            Value::Decimal128(1200, 2),
        ] {
            assert_eq!(
                canonical_decimal_key(value, field),
                Some(Value::DecimalCompact(1200, 2))
            );
        }
        assert_eq!(
            canonical_decimal_key(Value::DecimalCompact(12001, 3), field),
            None
        );
        assert_eq!(canonical_decimal_key(Value::Null, field), Some(Value::Null));
    }

    #[test]
    fn materialized_sort_output_preserves_runtime_signedness_and_nulls() {
        for (flags, values) in [
            (
                crate::mysql::protocol::UNSIGNED_FLAG,
                vec![Value::Int(-1), Value::Int(2), Value::Null],
            ),
            (0, vec![Value::UInt(0), Value::UInt(u64::MAX), Value::Null]),
        ] {
            let mut source = crate::types::ColumnSource::default();
            source.flags = flags;
            let schema = Arc::new(BatchSchema::new(vec![BatchField {
                table: "t".into(),
                alias: None,
                name: "k".into(),
                column_type: ColumnType::LongLong,
                source,
            }]));
            let batch = Batch::new(
                schema.clone(),
                vec![ColumnVector::from_values(values.clone())],
            );
            let mut output = ResidentOutput {
                schema,
                properties: BatchProperties::UNORDERED_SINGLE,
                batches: vec![(Some(batch), 0)],
                spool: None,
                loaded: None,
                entries: (0..3)
                    .map(|row| Entry {
                        batch: 0,
                        row,
                        start: 0,
                        end: 0,
                        ordinal: row,
                    })
                    .collect::<Vec<_>>()
                    .into_iter(),
                _charge: mem_budget::ScopedCharge::new(0),
                preserve_value_shape: true,
            };
            let actual = output.next_batch().unwrap().unwrap();
            assert_eq!(
                (0..3)
                    .map(|row| actual.row(row)[0].clone())
                    .collect::<Vec<_>>(),
                values
            );
        }
    }

    #[test]
    fn retained_compaction_drops_spare_capacity_and_invisible_rows() {
        let budget = Arc::new(mem_budget::MemBudget::new(4 * 1024 * 1024));
        mem_budget::with_query_budget(budget.clone(), || {
            let schema = Arc::new(BatchSchema::new(vec![BatchField {
                table: "t".into(),
                alias: None,
                name: "k".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            }]));
            for selected in [false, true] {
                let mut data = Vec::with_capacity(32768);
                data.extend([1, 2, 3, 4]);
                let mut batch = Batch::with_row_count(
                    schema.clone(),
                    vec![ColumnVector::I64 { data, nulls: None }],
                    4,
                );
                if selected {
                    batch = batch.with_selection(Selection::Indices(vec![1, 3]));
                }
                let expected = batch
                    .visible_indices()
                    .map(|row| batch.row(row))
                    .collect::<Vec<_>>();
                let compact = compact_retained_batch(batch).unwrap();
                assert!(compact.selection().is_none());
                assert!(compact.resident_bytes() < 1024);
                assert_eq!(
                    compact
                        .visible_indices()
                        .map(|row| compact.row(row))
                        .collect::<Vec<_>>(),
                    expected
                );
            }
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn retained_batch_credit_only_recognizes_its_query_owner() {
        let owner = Arc::new(mem_budget::MemBudget::new(1024 * 1024));
        mem_budget::with_query_budget(owner.clone(), || {
            let schema = Arc::new(BatchSchema::new(vec![BatchField {
                table: "t".into(),
                alias: None,
                name: "k".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            }]));
            let batch = Batch::with_row_count(
                schema,
                vec![ColumnVector::I64 {
                    data: vec![1, 2, 3],
                    nulls: None,
                }],
                3,
            )
            .with_memory_charge(mem_budget::ScopedCharge::new(0))
            .unwrap();
            assert!(batch.accounted_query_bytes() >= batch.memory_bytes());
            let another = Arc::new(mem_budget::MemBudget::new(1024 * 1024));
            mem_budget::with_query_budget(another, || assert_eq!(batch.accounted_query_bytes(), 0));
        });
        assert_eq!(owner.used(), 0);
    }
}
