//! Bounded final reduction when radix partitioning has exhausted its depth.

use super::*;
use crate::engine::executor::{
    mem_budget::{self, ScopedCharge},
    memory_footprint::AllocatedBytes,
    row_spool::OwnedRow,
    select::grace::{SpillTmpDir, ValueRowReader, ValueRowWriter},
    spill_quota::{self, SpillCharger},
    vec::{
        batch::{Batch, ColumnVector},
        schema::{BatchField, BatchSchema},
        sort::{ExternalSortBatchOperator, SortKeySource},
        stream::{BatchOperator, BatchProperties},
    },
};
use std::path::{Path, PathBuf};

fn malformed() -> QueryError {
    crate::error::catalog::ER_UNKNOWN_ERROR.error("invalid set-operation spill record")
}

struct KeyedPartition<'a> {
    reader: Option<ValueRowReader>,
    path: &'a Path,
    charger: SpillCharger,
    schema: Arc<BatchSchema>,
    collations: &'a [Option<u16>],
    width: usize,
}

impl BatchOperator for KeyedPartition<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let Some(reader) = self.reader.as_mut() else {
            return Ok(None);
        };
        let Some((tagged, charge)) = reader.next_owned()? else {
            self.reader = None;
            spill_quota::remove_spill_file(self.path, &self.charger)?;
            return Ok(None);
        };
        let mut owned = OwnedRow {
            values: tagged,
            charge,
        };
        if owned.values.len() != self.width + 3 {
            return Err(malformed());
        }
        for index in 0..3 {
            setop_u64(&owned.values, index)?;
        }
        // Reuse precisely the equality domain of ordinary set reduction.
        // The key is evaluated once and becomes an owned hidden sort lane.
        let key = set_row_key(self.collations, &owned.values[3..]).into_vec();
        owned.charge.add_force(key.capacity())?;
        let columns = self.schema.len();
        owned.charge.add_force(
            columns
                .saturating_mul(std::mem::size_of::<ColumnVector>() + std::mem::size_of::<Value>())
                .saturating_add(128),
        )?;
        let mut output = mem_budget::try_vec_with_capacity(columns, "set partition sort columns")?;
        for value in std::iter::once(Value::Bytes(key)).chain(owned.values) {
            let mut values = mem_budget::try_vec_with_capacity(1, "set partition sort cell")?;
            values.push(value);
            output.push(ColumnVector::Generic { values });
        }
        Ok(Some(
            Batch::new(self.schema.clone(), output).with_memory_charge(owned.charge)?,
        ))
    }
}

struct Pending {
    key: Vec<u8>,
    entry: SetCountEntry,
    charge: ScopedCharge,
}

fn flush_winners(
    winners: &mut Vec<(u64, u64, Vec<Value>)>,
    charge: &mut ScopedCharge,
    dir: &SpillTmpDir,
    width: usize,
    runs: &mut Vec<PathBuf>,
    next_run: &mut usize,
    run_charge: &mut ScopedCharge,
    metrics: &mut SetOpReplayMetrics,
) -> Result<(), QueryError> {
    if winners.is_empty() {
        return Ok(());
    }
    winners.sort_unstable_by_key(|(ordinal, _, _)| *ordinal);
    let path = dir.path().join(format!("setrun-{}", *next_run));
    *next_run = next_run.saturating_add(1);
    let mut writer = ValueRowWriter::create(&path, width.saturating_add(2), dir.charger())?;
    metrics.file_opens = metrics.file_opens.saturating_add(1);
    for (ordinal, repeats, row) in winners.drain(..) {
        crate::engine::cancel::check_cancelled()?;
        let _staging = mem_budget::force_charge_scoped(
            (width + 2).saturating_mul(std::mem::size_of::<Value>()),
        )?;
        let mut tagged = mem_budget::try_vec_with_capacity(width + 2, "set count output record")?;
        tagged.push(Value::UInt(ordinal));
        tagged.push(Value::UInt(repeats));
        tagged.extend(row);
        writer.append(&tagged)?;
    }
    metrics.bytes_written = metrics.bytes_written.saturating_add(writer.finish()?);
    // The backing array survives drain; only payload storage has gone away.
    let backing = winners
        .capacity()
        .saturating_mul(std::mem::size_of::<(u64, u64, Vec<Value>)>());
    charge.release_bytes(charge.charged().saturating_sub(backing));
    run_charge.add_force(path.capacity())?;
    if runs.len() == runs.capacity() {
        mem_budget::try_reserve_vec_hard(runs, runs.len().max(4), run_charge, "set output runs")?;
    }
    runs.push(path);
    Ok(())
}

pub(super) fn reduce(
    reader: ValueRowReader,
    path: &Path,
    collations: &[Option<u16>],
    op: SetOp,
    all: bool,
    width: usize,
    cap: usize,
    dir: &SpillTmpDir,
    runs: &mut Vec<PathBuf>,
    next_run: &mut usize,
    run_charge: &mut ScopedCharge,
    peak: &mut usize,
    metrics: &mut SetOpReplayMetrics,
) -> Result<(), QueryError> {
    if reader.column_count() != width + 3 {
        return Err(malformed());
    }
    let mut metadata = ScopedCharge::new(0);
    metadata.add_force(
        (width + 4)
            .saturating_mul(std::mem::size_of::<BatchField>() + 64)
            .saturating_add(128),
    )?;
    let mut fields = mem_budget::try_vec_with_capacity(width + 4, "set external schema")?;
    for index in 0..width + 4 {
        fields.push(BatchField {
            table: String::new(),
            alias: None,
            name: index.to_string(),
            column_type: ColumnType::Blob,
            source: Default::default(),
        });
    }
    let schema = Arc::new(BatchSchema::new(fields));
    let source = KeyedPartition {
        reader: Some(reader),
        path,
        charger: dir.charger(),
        schema: schema.clone(),
        collations,
        width,
    };
    let mut sorted = ExternalSortBatchOperator::new(
        None,
        Box::new(source),
        schema,
        BatchProperties::UNORDERED_SINGLE,
        vec![SortKeySource::Column(0), SortKeySource::Column(3)],
        vec![
            crate::engine::executor::sort::SortField {
                ascending: true,
                nulls_first: true,
                collation_id: Some(63),
            },
            crate::engine::executor::sort::SortField {
                ascending: true,
                nulls_first: true,
                collation_id: None,
            },
        ],
        dir.path(),
    );
    let mut pending: Option<Pending> = None;
    let mut winner_charge = ScopedCharge::new(0);
    let mut winners = Vec::new();
    let mut emit = |pending: Pending| -> Result<(), QueryError> {
        let Pending {
            key,
            entry,
            mut charge,
        } = pending;
        let repeats = set_count_repeats(op, all, &entry);
        if repeats == 0 {
            drop(key);
            drop(entry);
            return Ok(());
        }
        drop(key);
        charge.release_bytes(charge.charged().saturating_sub(entry.row.allocated_bytes()));
        winner_charge.absorb(&mut charge);
        if winners.len() == winners.capacity() {
            mem_budget::try_reserve_vec_hard(
                &mut winners,
                16,
                &mut winner_charge,
                "set reduced winners",
            )?;
        }
        winners.push((entry.first_ordinal, repeats, entry.row));
        *peak = (*peak).max(winner_charge.charged());
        if winner_charge.charged() >= cap {
            flush_winners(
                &mut winners,
                &mut winner_charge,
                dir,
                width,
                runs,
                next_run,
                run_charge,
                metrics,
            )?;
        }
        Ok(())
    };
    while let Some(batch) = sorted.next_batch()? {
        for row in batch.visible_indices() {
            crate::engine::cancel::check_cancelled()?;
            let bytes = batch.columns().iter().fold(0usize, |bytes, column| {
                bytes.saturating_add(column.owned_value_bytes(row))
            });
            let charge = mem_budget::force_charge_scoped(bytes)?;
            let mut values = mem_budget::try_vec_with_capacity(width + 4, "set sorted record")?;
            for column in batch.columns() {
                values.push(column.try_value_at(row)?);
            }
            let left = setop_u64(&values, 1)?;
            let right = setop_u64(&values, 2)?;
            let ordinal = setop_u64(&values, 3)?;
            let key = match values.remove(0) {
                Value::Bytes(key) => key,
                _ => return Err(malformed()),
            };
            values.drain(..3);
            if let Some(previous) = pending.as_mut().filter(|previous| previous.key == key) {
                previous.entry.left = previous.entry.left.saturating_add(left);
                previous.entry.right = previous.entry.right.saturating_add(right);
                continue;
            }
            if let Some(previous) = pending.take() {
                emit(previous)?;
            }
            // The sort orders equal canonical keys by original input ordinal,
            // preserving the earliest representative, including hidden types.
            pending = Some(Pending {
                key,
                entry: SetCountEntry {
                    row: values,
                    left,
                    right,
                    first_ordinal: ordinal,
                },
                charge,
            });
        }
    }
    if let Some(previous) = pending {
        emit(previous)?;
    }
    flush_winners(
        &mut winners,
        &mut winner_charge,
        dir,
        width,
        runs,
        next_run,
        run_charge,
        metrics,
    )
}
