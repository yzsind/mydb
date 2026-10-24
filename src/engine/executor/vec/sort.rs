//! Stable, spillable ORDER BY over the common [`BatchOperator`] protocol.
//!
//! Runs contain precomputed sort keys, the input ordinal used as the final
//! stable tie breaker, and the payload columns.  They use the shared columnar
//! spill codec, so neither run generation nor merge constructs `PlanRow`.

use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::engine::expr_program::ExprProgram;
use crate::types::{ColumnType, QueryError, Value};

use super::batch::{Batch, ColumnVector, DEFAULT_BATCH_ROWS};
use super::expr_batch;
use super::schema::{BatchField, BatchSchema};
use super::stream::{BatchOperator, BatchProperties};

pub(crate) enum SortKeySource {
    Column(usize),
    UnseededRand(Arc<ExprProgram>),
    Program {
        program: Arc<ExprProgram>,
        integer: Option<crate::engine::executor::select::plan::CompiledNumericExpr>,
    },
}

struct SortRow {
    keys: SortKeyValues,
    payload: Vec<Value>,
    ordinal: u64,
}

enum SortKeyValues {
    One(Value),
    Many(Vec<Value>),
}

impl SortKeyValues {
    /// Keep one-key sorts inline throughout run generation and spill merge.
    /// Collection remains left-to-right and stops on the first key error.
    // Fuse collection with its caller to avoid moving the large Result payload per row.
    #[inline(always)]
    fn try_collect(
        mut values: impl ExactSizeIterator<Item = Result<Value, QueryError>>,
    ) -> Result<Self, QueryError> {
        if values.len() == 1 {
            Ok(Self::One(values.next().expect("single sort key present")?))
        } else {
            let mut keys = crate::engine::executor::mem_budget::try_vec_with_capacity(
                values.len(),
                "batch sort keys",
            )?;
            for value in values {
                keys.push(value?);
            }
            Ok(Self::Many(keys))
        }
    }

    fn as_slice(&self) -> &[Value] {
        match self {
            Self::One(value) => std::slice::from_ref(value),
            Self::Many(values) => values,
        }
    }

    fn take_values(&mut self) -> impl Iterator<Item = Value> + '_ {
        let values = match self {
            Self::One(value) => std::slice::from_mut(value),
            Self::Many(values) => values.as_mut_slice(),
        };
        values
            .iter_mut()
            .map(|value| std::mem::replace(value, Value::Null))
    }

    fn iter(&self) -> std::slice::Iter<'_, Value> {
        self.as_slice().iter()
    }

    fn get(&self, index: usize) -> Option<&Value> {
        self.as_slice().get(index)
    }
}

fn compare_rows(
    left: &SortRow,
    right: &SortRow,
    fields: &[crate::engine::executor::sort::SortField],
    payload_ties: bool,
) -> Ordering {
    crate::engine::executor::sort::compare_sort_keys(
        fields,
        left.keys.as_slice(),
        right.keys.as_slice(),
    )
    .then_with(|| {
        if payload_ties {
            crate::engine::executor::sort::compare_payload_rows(&left.payload, &right.payload)
        } else {
            Ordering::Equal
        }
    })
    .then_with(|| left.ordinal.cmp(&right.ordinal))
}

fn sort_rows(
    rows: &mut Vec<SortRow>,
    fields: &[crate::engine::executor::sort::SortField],
    payload_ties: bool,
) -> Result<(), QueryError> {
    if payload_ties {
        rows.sort_unstable_by(|left, right| compare_rows(left, right, fields, true));
        return Ok(());
    }
    let directions = fields
        .iter()
        .map(|field| field.ascending)
        .collect::<Vec<_>>();
    if crate::engine::executor::sort::try_sort_by_int_keys_multi(
        rows,
        &directions,
        |row, column| match row.keys.get(column) {
            Some(Value::Int(value)) => Some(*value),
            _ => None,
        },
    ) {
        return Ok(());
    }
    if rows.len() >= 256
        && crate::engine::executor::sort::key_encode::try_sort_by_borrowed_encoded_keys(
            rows,
            fields,
            |row| row.keys.as_slice(),
        )?
    {
        return Ok(());
    }
    rows.sort_unstable_by(|left, right| compare_rows(left, right, fields, false));
    Ok(())
}

fn estimate_sort_row(row: &SortRow) -> usize {
    use crate::engine::executor::memory_footprint::AllocatedBytes;
    let key_slots = match &row.keys {
        SortKeyValues::One(_) => 0,
        SortKeyValues::Many(values) => values.capacity(),
    };
    row.keys.iter().chain(&row.payload).fold(
        std::mem::size_of::<SortRow>().saturating_add(
            key_slots
                .saturating_add(row.payload.capacity())
                .saturating_mul(std::mem::size_of::<Value>()),
        ),
        |bytes, value| bytes.saturating_add(value.allocated_bytes()),
    )
}

fn copy_payload(columns: &[ColumnVector], row: usize) -> Result<Vec<Value>, QueryError> {
    let mut payload = crate::engine::executor::mem_budget::try_vec_with_capacity(
        columns.len(),
        "batch sort payload",
    )?;
    for column in columns {
        payload.push(column.try_value_at(row)?);
    }
    Ok(payload)
}

/// Retained rows and their capacity have distinct owners. Admission happens
/// before cloning input values; a soft miss flushes only previously admitted
/// rows, so computed expressions are never evaluated again.
struct RunGeneration<'a> {
    data_dir: &'a Path,
    fields: &'a [crate::engine::executor::sort::SortField],
    payload_ties: bool,
    schema: Arc<BatchSchema>,
    limit: usize,
    rows: Vec<SortRow>,
    bytes: usize,
    peak_bytes: usize,
    charge: crate::engine::executor::mem_budget::ScopedCharge,
    capacity_charge: crate::engine::executor::mem_budget::ScopedCharge,
    tmp: Option<crate::engine::executor::select::grace::SpillTmpDir>,
    runs: Vec<Run>,
}

impl RunGeneration<'_> {
    fn flush(&mut self) -> Result<(), QueryError> {
        let directory = match self.tmp.as_ref() {
            Some(directory) => directory,
            None => self.tmp.insert(
                crate::engine::executor::select::grace::SpillTmpDir::create_kind(
                    self.data_dir,
                    "sort",
                )?,
            ),
        };
        if !self.rows.is_empty() {
            self.runs.push(flush_run(
                directory,
                &mut self.rows,
                self.fields,
                self.payload_ties,
                self.schema.clone(),
                self.runs.len(),
            )?);
        }
        self.bytes = 0;
        self.charge = self.charge.sibling();
        Ok(())
    }

    fn admit(
        &mut self,
        bytes: usize,
    ) -> Result<crate::engine::executor::mem_budget::ScopedCharge, QueryError> {
        let slot = if self.rows.len() == self.rows.capacity() {
            std::mem::size_of::<SortRow>()
        } else {
            0
        };
        if !self.rows.is_empty()
            && self
                .bytes
                .saturating_add(self.capacity_charge.charged())
                .saturating_add(bytes)
                .saturating_add(slot)
                > self.limit
        {
            self.flush()?;
        }
        let mut charge = self.charge.sibling();
        if self.tmp.is_some() {
            charge.add_force(bytes)?;
        } else if charge.add(bytes).is_err() {
            self.flush()?;
            charge.add_force(bytes)?;
        }
        Ok(charge)
    }

    fn push(
        &mut self,
        row: SortRow,
        charge: crate::engine::executor::mem_budget::ScopedCharge,
        batch_rows: usize,
    ) -> Result<(), QueryError> {
        let mut owned = OwnedSortRow { row, charge };
        let slot_bytes = std::mem::size_of::<SortRow>();
        let row_bytes = estimate_sort_row(&owned.row).saturating_sub(slot_bytes);
        if row_bytes > owned.charge.charged() {
            let extra = row_bytes - owned.charge.charged();
            if self.tmp.is_none() && owned.charge.add(extra).is_err() {
                self.flush()?;
                owned.charge.add_force(extra)?;
            } else if self.tmp.is_some() {
                owned.charge.add_force(extra)?;
            }
        } else {
            owned
                .charge
                .release_bytes(owned.charge.charged() - row_bytes);
        }
        if !self.rows.is_empty()
            && self
                .bytes
                .saturating_add(self.capacity_charge.charged())
                .saturating_add(row_bytes)
                > self.limit
        {
            self.flush()?;
        }
        if self.rows.len() == self.rows.capacity() {
            let headroom = self
                .limit
                .saturating_sub(self.bytes)
                .saturating_sub(self.capacity_charge.charged())
                .saturating_sub(row_bytes)
                .min(self.capacity_charge.remaining());
            let additional = (headroom / row_bytes.saturating_add(slot_bytes).max(1))
                .max(1)
                .min(batch_rows.max(1))
                .min(DEFAULT_BATCH_ROWS);
            crate::engine::executor::mem_budget::try_reserve_vec_hard(
                &mut self.rows,
                additional,
                &mut self.capacity_charge,
                "batch sort run capacity",
            )?;
        }
        self.bytes = self.bytes.saturating_add(row_bytes);
        self.peak_bytes = self
            .peak_bytes
            .max(self.bytes.saturating_add(self.capacity_charge.charged()));
        self.charge.absorb(&mut owned.charge);
        self.rows.push(owned.row);
        Ok(())
    }
}

fn run_bytes() -> usize {
    if let Some(bytes) = crate::engine::executor::tuning::current_or_capture().sort_run_bytes {
        return bytes;
    }
    crate::engine::executor::mem_budget::current_budget()
        .map(|budget| (budget.limit() / 4).clamp(1024 * 1024, 64 * 1024 * 1024))
        .unwrap_or(8 * 1024 * 1024)
}

fn spill_block_bytes() -> usize {
    crate::engine::executor::mem_budget::current_budget().map_or(256 * 1024, |budget| {
        (budget.limit() / 16).clamp(16 * 1024, 256 * 1024)
    })
}

fn merge_fanin(configured: usize) -> usize {
    let per_reader = spill_block_bytes()
        .saturating_mul(4)
        .saturating_add(32 * 1024);
    let memory = crate::engine::executor::mem_budget::current_budget()
        .map_or(configured, |budget| budget.limit() / per_reader.max(1));
    let descriptors = {
        #[cfg(unix)]
        {
            let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
            // SAFETY: getrlimit writes the complete object on success.
            if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } == 0 {
                let limit = unsafe { limit.assume_init() }.rlim_cur;
                (limit.min(usize::MAX as u64) as usize / 2)
                    .saturating_sub(
                        crate::engine::executor::spill_quota::active_spill_files()
                            .min(usize::MAX as u64) as usize,
                    )
                    .saturating_sub(16)
                    .max(2)
            } else {
                configured
            }
        }
        #[cfg(not(unix))]
        {
            configured
        }
    };
    configured.min(memory.max(2)).min(descriptors).max(2)
}

/// Evaluate potentially wide keys once, emitting bounded logical chunks.
/// Hidden computed lanes never become part of the forwarded sort payload.
struct SortKeyInput<'a> {
    input: Box<dyn BatchOperator + 'a>,
    programs: Vec<Arc<ExprProgram>>,
    computed_columns: Vec<Option<usize>>,
    schema: Arc<BatchSchema>,
    pending: Option<super::evaluated_batch::ComputedRowCursor>,
}

impl SortKeyInput<'_> {
    fn next(&mut self) -> Result<Option<(Batch, bool)>, QueryError> {
        loop {
            if let Some(cursor) = self.pending.as_mut() {
                if let Some(batch) = cursor.next(&self.programs, &self.schema)? {
                    return Ok(Some((batch, true)));
                }
                self.pending = None;
            }
            let Some(batch) = self.input.next_batch()? else {
                return Ok(None);
            };
            if !self.programs.is_empty()
                && super::evaluated_batch::needs_chunks(&self.programs, batch.num_rows())
            {
                self.pending = Some(super::evaluated_batch::ComputedRowCursor::new(batch)?);
            } else {
                return Ok(Some((batch, false)));
            }
        }
    }
}

struct OwnedSortRow {
    row: SortRow,
    charge: crate::engine::executor::mem_budget::ScopedCharge,
}

struct Run {
    path: PathBuf,
    bytes: u64,
}

fn spill_schema(payload: &BatchSchema, key_count: usize) -> Arc<BatchSchema> {
    let mut fields = Vec::with_capacity(key_count + 1 + payload.len());
    fields.extend((0..key_count).map(|index| BatchField {
        table: "__sort".into(),
        alias: None,
        name: format!("k{index}"),
        column_type: ColumnType::Blob,
        source: Default::default(),
    }));
    fields.push(BatchField {
        table: "__sort".into(),
        alias: None,
        name: "ordinal".into(),
        column_type: ColumnType::LongLong,
        source: Default::default(),
    });
    fields.extend(payload.fields().iter().cloned());
    Arc::new(BatchSchema::new(fields))
}

fn batch_from_rows<R: IntoIterator<Item = Value>>(
    schema: Arc<BatchSchema>,
    rows: impl ExactSizeIterator<Item = R>,
) -> Batch {
    let row_count = rows.len();
    // Cloning an empty Vec does not preserve its spare capacity. Reserve
    // each column independently so every lane has room for the whole batch.
    let mut columns: Vec<Vec<Value>> = (0..schema.len())
        .map(|_| Vec::with_capacity(row_count))
        .collect();
    for row in rows {
        for (column, value) in row.into_iter().enumerate() {
            columns[column].push(value);
        }
    }
    Batch::with_row_count(
        schema,
        columns
            .into_iter()
            .map(|values| {
                // F64 lanes promote Value::Float, which changes a dynamic
                // payload's runtime domain depending on spill block boundaries.
                // Ordinary typed scan FLOAT lanes already arrive as F64; keep
                // explicitly retained Float values exact in generic payloads.
                if values.iter().any(|value| matches!(value, Value::Float(_))) {
                    ColumnVector::Generic { values }
                } else {
                    ColumnVector::from_values(values)
                }
            })
            .collect(),
        row_count,
    )
}

/// The caller consumes these rows after writing the batch. Move their values
/// into columns rather than keeping a second deep copy alive during spill.
fn rows_to_batch<'a>(
    schema: Arc<BatchSchema>,
    rows: impl ExactSizeIterator<Item = &'a mut SortRow>,
) -> Batch {
    batch_from_rows(
        schema,
        rows.map(|row| {
            let payload = std::mem::take(&mut row.payload);
            row.keys
                .take_values()
                .chain(std::iter::once(Value::UInt(row.ordinal)))
                .chain(payload)
        }),
    )
}

fn flush_run(
    tmp: &crate::engine::executor::select::grace::SpillTmpDir,
    rows: &mut Vec<SortRow>,
    fields: &[crate::engine::executor::sort::SortField],
    payload_ties: bool,
    schema: Arc<BatchSchema>,
    index: usize,
) -> Result<Run, QueryError> {
    sort_rows(rows, fields, payload_ties)?;
    let path = tmp.path().join(format!("run-{index}.batch"));
    let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(1);
    let mut writer = crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
        &path,
        tmp.charger(),
        capacity,
    )?;
    let mut start = 0;
    let target = spill_block_bytes();
    while start < rows.len() {
        let mut end = start;
        let mut bytes = 0usize;
        while end < rows.len()
            && end - start < DEFAULT_BATCH_ROWS
            && (end == start || bytes < target)
        {
            bytes = bytes.saturating_add(estimate_sort_row(&rows[end]));
            end += 1;
        }
        let _scratch = crate::engine::executor::mem_budget::force_charge_scoped(bytes)?;
        super::spill::write_batch(
            &mut writer,
            &rows_to_batch(schema.clone(), rows[start..end].iter_mut()),
        )?;
        start = end;
    }
    let bytes = writer.finish()?;
    rows.clear();
    Ok(Run { path, bytes })
}

struct RunReader {
    reader: crate::engine::executor::spill_quota::TrackedSpillReader,
    schema: Arc<BatchSchema>,
    key_count: usize,
    batch: Option<Batch>,
    row: usize,
}

impl RunReader {
    fn open(
        run: &Run,
        tmp: &crate::engine::executor::select::grace::SpillTmpDir,
        schema: Arc<BatchSchema>,
        key_count: usize,
        reader_count: usize,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            reader: crate::engine::executor::spill_quota::TrackedSpillReader::open(
                &run.path,
                tmp.charger(),
                crate::engine::executor::spill_quota::writer_buffer_capacity(reader_count),
            )?,
            schema,
            key_count,
            batch: None,
            row: 0,
        })
    }

    fn next_row(&mut self) -> Result<Option<OwnedSortRow>, QueryError> {
        loop {
            if self
                .batch
                .as_ref()
                .is_some_and(|batch| self.row < batch.num_rows())
            {
                let batch = self.batch.as_ref().expect("batch checked above");
                let row = self.row;
                self.row += 1;
                let mut charge = self.reader.memory_charge();
                let bytes = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(column, _)| *column != self.key_count)
                    .fold(std::mem::size_of::<SortRow>(), |bytes, (index, column)| {
                        bytes
                            .saturating_add(column.owned_value_bytes(row))
                            .saturating_sub(if index < self.key_count && self.key_count == 1 {
                                std::mem::size_of::<Value>()
                            } else {
                                0
                            })
                    });
                charge.add_force(bytes)?;
                let keys = SortKeyValues::try_collect(
                    (0..self.key_count).map(|column| batch.columns()[column].try_value_at(row)),
                )?;
                let ordinal = match batch.columns()[self.key_count].value_at(row) {
                    Value::UInt(value) => value,
                    Value::Int(value) if value >= 0 => value as u64,
                    _ => {
                        return Err(crate::storage::error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "invalid ordinal in batch sort spill run",
                        ));
                    }
                };
                let payload = copy_payload(&batch.columns()[self.key_count + 1..], row)?;
                return Ok(Some(OwnedSortRow {
                    row: SortRow {
                        keys,
                        payload,
                        ordinal,
                    },
                    charge,
                }));
            }
            self.batch = super::spill::read_owned_batch(&mut self.reader, self.schema.clone())?;
            self.row = 0;
            if self.batch.is_none() {
                return Ok(None);
            }
        }
    }
}

struct HeapRow {
    row: SortRow,
    charge: crate::engine::executor::mem_budget::ScopedCharge,
    fields: Arc<[crate::engine::executor::sort::SortField]>,
    payload_ties: bool,
}

impl PartialEq for HeapRow {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for HeapRow {}
impl PartialOrd for HeapRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapRow {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_rows(&self.row, &other.row, &self.fields, self.payload_ties)
    }
}

struct RunMerger {
    readers: Vec<RunReader>,
    tree: crate::engine::executor::select::loser_tree::LoserTree<HeapRow, ()>,
}

impl RunMerger {
    fn open(
        tmp: &crate::engine::executor::select::grace::SpillTmpDir,
        runs: &[Run],
        schema: Arc<BatchSchema>,
        key_count: usize,
        fields: Arc<[crate::engine::executor::sort::SortField]>,
        payload_ties: bool,
    ) -> Result<Self, QueryError> {
        let mut readers = Vec::with_capacity(runs.len());
        let mut heads = Vec::with_capacity(runs.len());
        for run in runs {
            let mut reader = RunReader::open(run, &tmp, schema.clone(), key_count, runs.len())?;
            heads.push(reader.next_row()?.map(|row| {
                (
                    HeapRow {
                        row: row.row,
                        charge: row.charge,
                        fields: fields.clone(),
                        payload_ties,
                    },
                    (),
                )
            }));
            readers.push(reader);
        }
        Ok(Self {
            readers,
            tree: crate::engine::executor::select::loser_tree::LoserTree::new(heads),
        })
    }

    fn next_sort_row(&mut self) -> Result<Option<OwnedSortRow>, QueryError> {
        let Some((item, (), run)) = self.tree.pop_winner() else {
            return Ok(None);
        };
        let output = OwnedSortRow {
            row: item.row,
            charge: item.charge,
        };
        self.tree.refill(
            run,
            self.readers[run].next_row()?.map(|row| {
                (
                    HeapRow {
                        row: row.row,
                        charge: row.charge,
                        fields: item.fields,
                        payload_ties: item.payload_ties,
                    },
                    (),
                )
            }),
        );
        Ok(Some(output))
    }
}

struct MergeState {
    _tmp: crate::engine::executor::select::grace::SpillTmpDir,
    merger: RunMerger,
}

impl MergeState {
    fn open(
        tmp: crate::engine::executor::select::grace::SpillTmpDir,
        runs: &[Run],
        schema: Arc<BatchSchema>,
        key_count: usize,
        fields: Arc<[crate::engine::executor::sort::SortField]>,
        payload_ties: bool,
    ) -> Result<Self, QueryError> {
        let merger = RunMerger::open(&tmp, runs, schema, key_count, fields, payload_ties)?;
        Ok(Self { _tmp: tmp, merger })
    }

    fn next_row(&mut self) -> Result<Option<OwnedSortRow>, QueryError> {
        self.merger.next_sort_row()
    }
}

fn merge_runs_to_run(
    tmp: &crate::engine::executor::select::grace::SpillTmpDir,
    runs: &[Run],
    schema: Arc<BatchSchema>,
    key_count: usize,
    fields: Arc<[crate::engine::executor::sort::SortField]>,
    payload_ties: bool,
    pass: usize,
    group: usize,
) -> Result<Run, QueryError> {
    let path = tmp.path().join(format!("merge-{pass}-{group}.batch"));
    let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(1);
    let mut writer = crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
        &path,
        tmp.charger(),
        capacity,
    )?;
    let mut merger = RunMerger::open(tmp, runs, schema.clone(), key_count, fields, payload_ties)?;
    let mut rows = Vec::new();
    let mut capacity = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    let mut bytes = 0usize;
    let flush = |rows: &mut Vec<OwnedSortRow>,
                 writer: &mut crate::engine::executor::spill_quota::TrackedSpillWriter|
     -> Result<(), QueryError> {
        let scratch = rows.iter().map(|row| estimate_sort_row(&row.row)).sum();
        let _scratch = crate::engine::executor::mem_budget::force_charge_scoped(scratch)?;
        super::spill::write_batch(
            writer,
            &rows_to_batch(schema.clone(), rows.iter_mut().map(|row| &mut row.row)),
        )?;
        rows.clear();
        Ok(())
    };
    while let Some(row) = merger.next_sort_row()? {
        bytes = bytes.saturating_add(estimate_sort_row(&row.row));
        if rows.len() == rows.capacity() {
            crate::engine::executor::mem_budget::try_reserve_vec_hard(
                &mut rows,
                32,
                &mut capacity,
                "sort merge rows",
            )?;
        }
        rows.push(row);
        if rows.len() == DEFAULT_BATCH_ROWS || bytes >= spill_block_bytes() {
            flush(&mut rows, &mut writer)?;
            bytes = 0;
        }
    }
    if !rows.is_empty() {
        flush(&mut rows, &mut writer)?;
    }
    let bytes = writer.finish()?;
    drop(merger);
    for run in runs {
        crate::engine::executor::spill_quota::remove_spill_file(&run.path, &tmp.charger())?;
    }
    Ok(Run { path, bytes })
}

enum OutputState {
    Memory(std::vec::IntoIter<SortRow>),
    Spill(MergeState),
    TopK(crate::engine::executor::sort::TopKCandidates<Vec<Value>>),
    SortedPayloads {
        rows: std::vec::IntoIter<Vec<Value>>,
        charge: crate::engine::executor::mem_budget::ScopedCharge,
    },
    Empty,
}

/// Deferred external sort. Capability discovery and child construction happen
/// before this operator is returned; the first pull is the only point that
/// consumes upstream input.
pub(crate) struct ExternalSortBatchOperator<'a> {
    plan: Option<&'a crate::planner::PhysicalPlan>,
    upstream: Option<Box<dyn BatchOperator + 'a>>,
    output_schema: Arc<BatchSchema>,
    properties: BatchProperties,
    sources: Vec<SortKeySource>,
    row_major_keys: bool,
    fields: Arc<[crate::engine::executor::sort::SortField]>,
    data_dir: PathBuf,
    output: Option<OutputState>,
    memory_charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
    capacity_charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
    topn: Option<usize>,
    output_offset: usize,
    output_remaining: Option<usize>,
    record_topn_keys: bool,
    payload_ties: bool,
    input_ordinal_column: Option<usize>,
}

impl<'a> ExternalSortBatchOperator<'a> {
    /// The producer has already ordered and admitted these rows. Reuse the
    /// byte-bounded owning output adapter without sorting or evaluating keys.
    pub(crate) fn from_sorted_rows(
        schema: Arc<BatchSchema>,
        properties: BatchProperties,
        rows: Vec<Vec<Value>>,
        charge: crate::engine::executor::mem_budget::ScopedCharge,
        offset: usize,
    ) -> Self {
        let mut output = Self::new(
            None,
            Box::new(super::stream::MaterializedBatchOperator::new(
                schema.clone(),
                Vec::new(),
            )),
            schema,
            properties,
            Vec::new(),
            Vec::new(),
            Path::new("."),
        );
        output.upstream = None;
        output.output = Some(OutputState::SortedPayloads {
            rows: rows.into_iter(),
            charge,
        });
        output.output_offset = offset;
        output
    }

    pub(crate) fn new(
        plan: Option<&'a crate::planner::PhysicalPlan>,
        upstream: Box<dyn BatchOperator + 'a>,
        output_schema: Arc<BatchSchema>,
        properties: BatchProperties,
        sources: Vec<SortKeySource>,
        fields: Vec<crate::engine::executor::sort::SortField>,
        data_dir: &Path,
    ) -> Self {
        let erroring_programs = sources
            .iter()
            .filter_map(|source| match source {
                SortKeySource::Program { program, .. } | SortKeySource::UnseededRand(program) => {
                    Some(program.capability())
                }
                SortKeySource::Column(_) => None,
            })
            .filter(|capability| capability.requires_ordered_evaluation())
            .count();
        Self {
            plan,
            upstream: Some(upstream),
            output_schema,
            properties,
            sources,
            row_major_keys: erroring_programs > 1,
            fields: fields.into(),
            data_dir: data_dir.to_path_buf(),
            output: None,
            memory_charge: None,
            capacity_charge: None,
            topn: None,
            output_offset: 0,
            output_remaining: None,
            record_topn_keys: false,
            payload_ties: false,
            input_ordinal_column: None,
        }
    }

    /// Retain a heap while its actual footprint fits, then migrate the
    /// already evaluated candidates and remaining input to ordinary runs.
    pub(crate) fn with_topn(mut self, k: usize, offset: usize) -> Self {
        self.topn = Some(k);
        self.output_offset = offset;
        self.output_remaining = Some(k.saturating_sub(offset));
        self.record_topn_keys = true;
        self
    }

    /// Workers have already evaluated these candidates. Sort their retained
    /// payload tie directly and apply K/OFFSET only to the final output.
    pub(crate) fn with_topn_candidates(mut self, k: usize, offset: usize) -> Self {
        self = self.with_topn(k, offset);
        self.topn = None;
        self.record_topn_keys = false;
        self
    }

    pub(crate) fn with_payload_ties(mut self, ordinal_column: usize) -> Self {
        assert!(
            self.topn.is_none(),
            "candidate replay sorts the existing payload tie directly"
        );
        self.payload_ties = true;
        self.input_ordinal_column = Some(ordinal_column);
        self
    }

    fn initialize(&mut self) -> Result<(), QueryError> {
        if self.output.is_some() {
            return Ok(());
        }
        if self.output_remaining == Some(0) {
            self.upstream = None;
            self.output = Some(OutputState::Empty);
            return Ok(());
        }
        let input = self.upstream.take().expect("batch sort initialized once");
        let mut programs = Vec::new();
        let mut computed_columns = Vec::with_capacity(self.sources.len());
        let mut computed_fields = self.output_schema.fields().to_vec();
        for source in &self.sources {
            if let SortKeySource::Program { program, .. } | SortKeySource::UnseededRand(program) =
                source
            {
                computed_columns.push(Some(self.output_schema.len() + programs.len()));
                programs.push(program.clone());
                computed_fields.push(BatchField {
                    table: "__sort_key".into(),
                    alias: None,
                    name: format!("key{}", programs.len()),
                    column_type: ColumnType::Blob,
                    source: Default::default(),
                });
            } else {
                computed_columns.push(None);
            }
        }
        let mut upstream = SortKeyInput {
            input,
            programs,
            computed_columns,
            schema: Arc::new(BatchSchema::new(computed_fields)),
            pending: None,
        };
        let limit = run_bytes();
        let spill_schema = spill_schema(&self.output_schema, self.sources.len());
        let mut generation = RunGeneration {
            data_dir: &self.data_dir,
            fields: &self.fields,
            payload_ties: self.payload_ties,
            schema: spill_schema.clone(),
            limit,
            rows: Vec::new(),
            bytes: 0,
            peak_bytes: 0,
            charge: crate::engine::executor::mem_budget::ScopedCharge::new(0),
            capacity_charge: crate::engine::executor::mem_budget::ScopedCharge::new(0),
            tmp: None,
            runs: Vec::new(),
        };
        let mut topk = self
            .topn
            .map(|k| crate::engine::executor::sort::BoundedTopK::new(&self.fields, k))
            .transpose()?;
        let topk_limit = crate::engine::executor::mem_budget::current_budget()
            .map_or(limit, |budget| (budget.limit() / 4).max(1))
            .min(limit);
        let mut ordinal = 0u64;
        while let Some((batch, precomputed)) = upstream.next()? {
            crate::engine::cancel::check_cancelled()?;
            if self.record_topn_keys {
                crate::engine::executor::select::eval::record_order_by_key_rows_prepared(
                    batch.visible_rows(),
                );
            }
            let row_major_keys = self.row_major_keys && !precomputed;
            let mut evaluated_charge = batch.memory_charge();
            if !row_major_keys && !precomputed {
                let bound = upstream
                    .programs
                    .iter()
                    .try_fold(0usize, |sum, program| {
                        Some(sum.saturating_add(super::evaluated_batch::result_bound(program)?))
                    })
                    .expect("unbounded sort keys use byte chunks");
                evaluated_charge.add_force(
                    bound
                        .saturating_mul(batch.num_rows())
                        .saturating_mul(2)
                        .saturating_add(
                            self.sources
                                .len()
                                .saturating_mul(std::mem::size_of::<ColumnVector>()),
                        ),
                )?;
            }
            let evaluated = if row_major_keys || precomputed {
                Vec::new()
            } else {
                self.sources
                    .iter()
                    .map(|source| match source {
                        SortKeySource::Column(_) => Ok(None),
                        SortKeySource::UnseededRand(_) => {
                            let mut data =
                                crate::engine::executor::mem_budget::try_vec_with_capacity(
                                    batch.num_rows(),
                                    "TopN RAND keys",
                                )?;
                            data.resize(batch.num_rows(), 0.0);
                            crate::engine::expr::fill_unseeded_mysql_rand_batch(
                                &mut data,
                                batch.visible_indices(),
                            );
                            Ok(Some(ColumnVector::F64 { data, nulls: None }))
                        }
                        SortKeySource::Program { program, integer } => integer
                            .as_ref()
                            .and_then(|integer| {
                                integer.try_eval_batch_integer(batch.columns(), batch.num_rows())
                            })
                            .map(Some)
                            .map(Ok)
                            .unwrap_or_else(|| {
                                expr_batch::eval_to_column(program, &batch).map(Some)
                            }),
                    })
                    .collect::<Result<Vec<_>, QueryError>>()?
            };
            for row_index in batch.visible_indices() {
                let row_ordinal = match self.input_ordinal_column {
                    Some(column) => match batch.columns()[column].value_at(row_index) {
                        Value::UInt(ordinal) => ordinal,
                        Value::Int(ordinal) if ordinal >= 0 => ordinal as u64,
                        _ => {
                            return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                                .error("invalid parallel TopN input ordinal"))
                        }
                    },
                    None => ordinal,
                };
                let value_slot = std::mem::size_of::<Value>();
                let mut predicted = batch.columns()[..self.output_schema.len()]
                    .iter()
                    .fold(0usize, |bytes, column| {
                        bytes.saturating_add(column.owned_value_bytes(row_index))
                    });
                if self.sources.len() != 1 {
                    predicted =
                        predicted.saturating_add(self.sources.len().saturating_mul(value_slot));
                }
                for (index, source) in self.sources.iter().enumerate() {
                    let column = match source {
                        SortKeySource::Column(column) => Some(&batch.columns()[*column]),
                        SortKeySource::Program { .. } | SortKeySource::UnseededRand(_)
                            if precomputed =>
                        {
                            Some(&batch.columns()[upstream.computed_columns[index].unwrap()])
                        }
                        SortKeySource::Program { .. } | SortKeySource::UnseededRand(_) => {
                            evaluated.get(index).and_then(Option::as_ref)
                        }
                    };
                    if column.is_none() && row_major_keys {
                        if let SortKeySource::Program { program, .. }
                        | SortKeySource::UnseededRand(program) = source
                        {
                            predicted = predicted.saturating_add(
                                super::evaluated_batch::result_bound(program)
                                    .unwrap_or(value_slot)
                                    .saturating_sub(value_slot),
                            );
                        }
                    }
                    if let Some(column) = column {
                        predicted = predicted.saturating_add(
                            column
                                .owned_value_bytes(row_index)
                                .saturating_sub(value_slot),
                        );
                    }
                }
                // A heap stores even a single key in a Vec. Reserve that
                // additional inline slot before creating the key envelope.
                let heap_predicted =
                    predicted.saturating_add(usize::from(self.sources.len() == 1) * value_slot);
                let mut heap_charge = generation.charge.sibling();
                if topk.as_ref().is_some_and(|heap| {
                    heap.resident_bytes().saturating_add(heap_predicted) > topk_limit
                        || heap_charge.add(heap_predicted).is_err()
                }) {
                    drop(heap_charge);
                    // Drain without cloning and keep each payload's original
                    // charge and ordinal. Previously rejected rows cannot be
                    // among the global K winners and need not be replayed.
                    let candidates = topk.take().expect("active heap").drain_sorted();
                    for candidate in candidates {
                        let (keys, payload, ordinal, charge) = candidate.into_parts();
                        generation.push(
                            SortRow {
                                keys: SortKeyValues::Many(keys),
                                payload,
                                ordinal,
                            },
                            charge,
                            DEFAULT_BATCH_ROWS,
                        )?;
                    }
                    generation.flush()?;
                    heap_charge = generation.charge.sibling();
                }
                let pending_charge = if topk.is_some() {
                    heap_charge
                } else {
                    generation.admit(predicted)?
                };
                let mut prepared_payload = None;
                let keys = if row_major_keys {
                    let payload =
                        copy_payload(&batch.columns()[..self.output_schema.len()], row_index)?;
                    let view = crate::engine::expr_program::RowView::from_values(&payload);
                    let keys =
                        SortKeyValues::try_collect(self.sources.iter().map(
                            |source| match source {
                                SortKeySource::Column(column) => {
                                    batch.columns()[*column].try_value_at(row_index)
                                }
                                SortKeySource::Program { program, .. }
                                | SortKeySource::UnseededRand(program) => {
                                    crate::engine::expr_program::with_eval_scratch(|scratch| {
                                        program.eval_row(&view, scratch)
                                    })
                                }
                            },
                        ))?;
                    prepared_payload = Some(payload);
                    keys
                } else {
                    SortKeyValues::try_collect(self.sources.iter().enumerate().map(
                        |(index, source)| {
                            match source {
                                SortKeySource::Column(column) => {
                                    batch.columns()[*column].try_value_at(row_index)
                                }
                                SortKeySource::Program { .. } | SortKeySource::UnseededRand(_)
                                    if precomputed =>
                                {
                                    batch.columns()[upstream.computed_columns[index].unwrap()]
                                        .try_value_at(row_index)
                                }
                                SortKeySource::Program { .. } | SortKeySource::UnseededRand(_) => {
                                    evaluated[index].as_ref().unwrap().try_value_at(row_index)
                                }
                            }
                        },
                    ))?
                };
                if let Some(heap) = topk.as_mut() {
                    if heap.would_accept(keys.as_slice()) {
                        let payload = match prepared_payload {
                            Some(payload) => payload,
                            None => copy_payload(
                                &batch.columns()[..self.output_schema.len()],
                                row_index,
                            )?,
                        };
                        let keys = match keys {
                            SortKeyValues::One(key) => vec![key],
                            SortKeyValues::Many(keys) => keys,
                        };
                        heap.push_reserved(payload, keys, row_ordinal, pending_charge)?;
                        generation.peak_bytes = generation.peak_bytes.max(heap.resident_bytes());
                    }
                } else {
                    let payload = match prepared_payload {
                        Some(payload) => payload,
                        None => {
                            copy_payload(&batch.columns()[..self.output_schema.len()], row_index)?
                        }
                    };
                    generation.push(
                        SortRow {
                            keys,
                            payload,
                            ordinal: row_ordinal,
                        },
                        pending_charge,
                        batch.num_rows(),
                    )?;
                }
                ordinal = ordinal.checked_add(1).ok_or_else(|| {
                    crate::error::catalog::ER_UNKNOWN_ERROR.error("sort input ordinal overflow")
                })?;
            }
        }
        let RunGeneration {
            mut rows,
            peak_bytes,
            charge,
            capacity_charge,
            tmp,
            mut runs,
            ..
        } = generation;
        if let Some(plan) = self.plan {
            crate::engine::executor::select::analyze_stats::add_peak_memory(
                plan,
                peak_bytes as u64,
            );
        }
        if let Some(heap) = topk {
            self.output = Some(OutputState::TopK(heap.drain_sorted()));
            return Ok(());
        }
        if let Some(directory) = tmp {
            if !rows.is_empty() {
                runs.push(flush_run(
                    &directory,
                    &mut rows,
                    &self.fields,
                    self.payload_ties,
                    spill_schema.clone(),
                    runs.len(),
                )?);
            }
            drop(rows);
            drop(capacity_charge);
            drop(charge);
            let initial_runs = runs.len();
            let mut spill_bytes = runs.iter().map(|run| run.bytes).sum::<u64>();
            let fanin =
                merge_fanin(crate::engine::executor::tuning::current_or_capture().sort_merge_fanin);
            let mut pass = 0usize;
            while runs.len() > fanin {
                let mut next = Vec::with_capacity(runs.len().div_ceil(fanin));
                for (group, chunk) in runs.chunks(fanin).enumerate() {
                    let run = merge_runs_to_run(
                        &directory,
                        chunk,
                        spill_schema.clone(),
                        self.sources.len(),
                        self.fields.clone(),
                        self.payload_ties,
                        pass,
                        group,
                    )?;
                    spill_bytes = spill_bytes.saturating_add(run.bytes);
                    next.push(run);
                }
                runs = next;
                pass += 1;
            }
            if let Some(plan) = self.plan {
                crate::engine::executor::select::analyze_stats::add_spill_bytes(plan, spill_bytes);
                crate::engine::executor::select::analyze_stats::add_sort_runs(
                    plan,
                    initial_runs as u64,
                    pass as u64,
                );
            }
            self.output = Some(OutputState::Spill(MergeState::open(
                directory,
                &runs,
                spill_schema,
                self.sources.len(),
                self.fields.clone(),
                self.payload_ties,
            )?));
        } else if rows.is_empty() {
            self.output = Some(OutputState::Empty);
        } else {
            sort_rows(&mut rows, &self.fields, self.payload_ties)?;
            self.memory_charge = Some(charge);
            self.capacity_charge = Some(capacity_charge);
            self.output = Some(OutputState::Memory(rows.into_iter()));
        }
        Ok(())
    }

    fn next_owned_row(&mut self) -> Result<Option<OwnedSortRow>, QueryError> {
        if self.output_remaining == Some(0) {
            self.output = Some(OutputState::Empty);
            self.memory_charge = None;
            self.capacity_charge = None;
            return Ok(None);
        }
        while self.output_offset > 0 {
            if self.next_owned_row_unlimited()?.is_none() {
                return Ok(None);
            }
            self.output_offset -= 1;
        }
        let row = self.next_owned_row_unlimited()?;
        if row.is_some() {
            if let Some(remaining) = &mut self.output_remaining {
                *remaining -= 1;
            }
        }
        Ok(row)
    }

    fn next_owned_row_unlimited(&mut self) -> Result<Option<OwnedSortRow>, QueryError> {
        let output = match self.output.as_mut().expect("batch sort output initialized") {
            OutputState::Memory(rows) => match rows.next() {
                Some(row) => {
                    let bytes =
                        estimate_sort_row(&row).saturating_sub(std::mem::size_of::<SortRow>());
                    let charge = self
                        .memory_charge
                        .as_mut()
                        .expect("resident sort retains its charge")
                        .split(bytes);
                    Ok(Some(OwnedSortRow { row, charge }))
                }
                None => Ok(None),
            },
            OutputState::Spill(merge) => merge.next_row(),
            OutputState::TopK(candidates) => Ok(candidates.next().map(|candidate| {
                let (keys, payload, ordinal, charge) = candidate.into_parts();
                OwnedSortRow {
                    row: SortRow {
                        keys: SortKeyValues::Many(keys),
                        payload,
                        ordinal,
                    },
                    charge,
                }
            })),
            OutputState::SortedPayloads { rows, charge } => {
                use crate::engine::executor::memory_footprint::AllocatedBytes;
                Ok(rows.next().map(|payload| {
                    let row_charge = charge.split(payload.allocated_bytes());
                    OwnedSortRow {
                        row: SortRow {
                            keys: SortKeyValues::Many(Vec::new()),
                            payload,
                            ordinal: 0,
                        },
                        charge: row_charge,
                    }
                }))
            }
            OutputState::Empty => Ok(None),
        }?;
        if output.is_none() {
            self.output = Some(OutputState::Empty);
            self.memory_charge = None;
            self.capacity_charge = None;
        }
        Ok(output)
    }

    #[cfg(test)]
    fn next_values(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        Ok(self.next_owned_row()?.map(|owned| owned.row.payload))
    }
}

impl BatchOperator for ExternalSortBatchOperator<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.output_schema
    }

    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.initialize()?;
        let mut rows = Vec::new();
        let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        let mut capacity = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        let mut bytes = 0usize;
        while rows.len() < DEFAULT_BATCH_ROWS && (rows.is_empty() || bytes < spill_block_bytes()) {
            let Some(mut owned) = self.next_owned_row()? else {
                break;
            };
            bytes = bytes.saturating_add(crate::engine::executor::mem_budget::estimate_row_bytes(
                &owned.row.payload,
            ));
            charge.absorb(&mut owned.charge);
            if rows.len() == rows.capacity() {
                crate::engine::executor::mem_budget::try_reserve_vec_hard(
                    &mut rows,
                    32,
                    &mut capacity,
                    "sort output rows",
                )?;
            }
            rows.push(owned.row.payload);
        }
        if rows.is_empty() {
            return Ok(None);
        }
        // Cover raw-column conversion while moved row values remain live.
        let _scratch =
            crate::engine::executor::mem_budget::force_charge_scoped(bytes.saturating_mul(2))?;
        let batch = batch_from_rows(self.output_schema.clone(), rows.into_iter());
        Ok(Some(batch.with_memory_charge(charge)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::vec::stream::{BatchOrdering, BatchPartitioning};

    fn schema() -> Arc<BatchSchema> {
        Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "k".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "payload".into(),
                column_type: ColumnType::VarString,
                source: Default::default(),
            },
        ]))
    }

    #[test]
    fn selected_sort_releases_reservations_in_memory_and_after_spill() {
        use super::super::batch::Selection;
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        for run_limit in [512, 1024 * 1024] {
            let budget = Arc::new(MemBudget::new(8 * 1024 * 1024));
            with_query_budget(budget.clone(), || {
                let data_dir = tempfile::tempdir().unwrap();
                let schema = schema();
                let batch = Batch::new(
                    schema.clone(),
                    vec![
                        ColumnVector::from_values((0..64).map(|i| Value::Int(i % 5)).collect()),
                        ColumnVector::from_values(
                            (0..64)
                                .map(|i| Value::String(format!("payload-{i}")))
                                .collect(),
                        ),
                    ],
                )
                .with_selection(Selection::Indices((0..64).filter(|i| i % 3 == 0).collect()));
                let input = super::super::stream::MaterializedBatchOperator::new(
                    schema.clone(),
                    vec![batch],
                );
                let snapshot = crate::engine::executor::tuning::ExecutorTuningSnapshot {
                    stream_batch_rows: 64,
                    stream_channel_batches: 2,
                    sort_run_bytes: Some(run_limit),
                    sort_merge_fanin: 2,
                    hash_join_bloom_adaptive: true,
                    scheduler_parallelism: 1,
                };
                let _tuning = crate::engine::executor::tuning::enter(snapshot);
                let mut sort = ExternalSortBatchOperator::new(
                    None,
                    Box::new(input),
                    schema,
                    BatchProperties {
                        ordering: BatchOrdering::Ordered(vec![]),
                        partitioning: BatchPartitioning::Single,
                    },
                    vec![SortKeySource::Column(0)],
                    vec![crate::engine::executor::sort::SortField {
                        ascending: true,
                        nulls_first: true,
                        collation_id: None,
                    }],
                    data_dir.path(),
                );
                sort.initialize().unwrap();
                if run_limit > 512 {
                    // A selection leaves unused slots in the batch reservation.
                    // They must remain charged for as long as the run is retained.
                    assert!(
                        sort.capacity_charge.as_ref().unwrap().charged()
                            > 22 * std::mem::size_of::<SortRow>()
                    );
                }
                let mut actual = Vec::new();
                while let Some(row) = sort.next_values().unwrap() {
                    actual.push(row);
                }
                let mut expected: Vec<_> = (0..64)
                    .filter(|i| i % 3 == 0)
                    .map(|i| vec![Value::Int(i % 5), Value::String(format!("payload-{i}"))])
                    .collect();
                expected.sort_by_key(|row| match row[0] {
                    Value::Int(v) => v,
                    _ => unreachable!(),
                });
                assert_eq!(actual, expected);
                drop(sort);
                assert_eq!(budget.used(), 0, "sort reservation leaked after drop");
            });
        }
    }

    #[test]
    fn columnar_spill_merge_is_stable_and_cleans_up() {
        let data_dir = tempfile::tempdir().unwrap();
        let schema = schema();
        let rows = (0..200i64)
            .rev()
            .map(|value| {
                vec![
                    Value::Int(value % 7),
                    Value::String(format!("row-{value:03}-{}", "x".repeat(32))),
                ]
            })
            .collect::<Vec<_>>();
        let mut columns = vec![Vec::new(), Vec::new()];
        for row in &rows {
            columns[0].push(row[0].clone());
            columns[1].push(row[1].clone());
        }
        let input = super::super::stream::MaterializedBatchOperator::new(
            schema.clone(),
            vec![Batch::new(
                schema.clone(),
                columns.into_iter().map(ColumnVector::from_values).collect(),
            )],
        );
        let snapshot = crate::engine::executor::tuning::ExecutorTuningSnapshot {
            stream_batch_rows: 64,
            stream_channel_batches: 2,
            sort_run_bytes: Some(1024),
            sort_merge_fanin: 2,
            hash_join_bloom_adaptive: true,
            scheduler_parallelism: 1,
        };
        let _tuning = crate::engine::executor::tuning::enter(snapshot);
        let before = crate::engine::executor::spill_quota::sort_spill_count();
        let properties = BatchProperties {
            ordering: BatchOrdering::Ordered(vec![]),
            partitioning: BatchPartitioning::Single,
        };
        let mut sort = ExternalSortBatchOperator::new(
            None,
            Box::new(input),
            schema,
            properties,
            vec![SortKeySource::Column(0)],
            vec![crate::engine::executor::sort::SortField {
                ascending: true,
                nulls_first: true,
                collation_id: None,
            }],
            data_dir.path(),
        );
        let mut actual = Vec::new();
        while let Some(batch) = sort.next_batch().unwrap() {
            for row in batch.visible_indices() {
                actual.push(vec![
                    batch.columns()[0].value_at(row),
                    batch.columns()[1].value_at(row),
                ]);
            }
        }
        let mut expected = rows;
        expected.sort_by_key(|row| match row[0] {
            Value::Int(value) => value,
            _ => unreachable!(),
        });
        assert_eq!(actual, expected);
        assert!(crate::engine::executor::spill_quota::sort_spill_count() > before);
        drop(sort);
        let tmp = crate::storage::naming::system_tmp_dir(data_dir.path());
        assert!(
            !tmp.exists() || std::fs::read_dir(tmp).unwrap().next().is_none(),
            "batch sort spill directory leaked"
        );
    }
}

#[cfg(test)]
#[path = "sort/batch_tests.rs"]
mod batch_tests;

#[cfg(test)]
mod spill_contract_regressions {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};

    fn file_bytes(path: &Path) -> u64 {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    file_bytes(&path)
                } else {
                    path.metadata().unwrap().len()
                }
            })
            .sum()
    }

    #[test]
    fn multi_pass_sort_live_quota_matches_remaining_files() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let budget = Arc::new(MemBudget::new(8 * 1024 * 1024));
        with_query_budget(budget.clone(), || {
            let schema = Arc::new(BatchSchema::new(vec![BatchField {
                table: "t".into(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::VarString,
                source: Default::default(),
            }]));
            let batch = Batch::new(
                schema.clone(),
                vec![ColumnVector::from_values(
                    (0..128)
                        .rev()
                        .map(|i| Value::String(format!("{i:04}{}", "x".repeat(128))))
                        .collect(),
                )],
            );
            let input =
                super::super::stream::MaterializedBatchOperator::new(schema.clone(), vec![batch]);
            let _tuning = crate::engine::executor::tuning::enter(
                crate::engine::executor::tuning::ExecutorTuningSnapshot {
                    stream_batch_rows: 64,
                    stream_channel_batches: 2,
                    sort_run_bytes: Some(1024),
                    sort_merge_fanin: 2,
                    hash_join_bloom_adaptive: true,
                    scheduler_parallelism: 1,
                },
            );
            let mut sort = ExternalSortBatchOperator::new(
                None,
                Box::new(input),
                schema,
                BatchProperties::UNORDERED_SINGLE,
                vec![SortKeySource::Column(0)],
                vec![crate::engine::executor::sort::SortField {
                    ascending: true,
                    nulls_first: true,
                    collation_id: None,
                }],
                root.path(),
            );
            sort.initialize().unwrap();
            assert_eq!(
                budget.spill_bytes(),
                file_bytes(&crate::storage::naming::system_tmp_dir(root.path()))
            );
            drop(sort);
            assert_eq!(budget.spill_bytes(), 0);
            assert_eq!(budget.used(), 0);
        });
    }
}
