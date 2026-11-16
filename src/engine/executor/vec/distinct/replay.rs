//! Bounded partition replay with canonical-key sort fallback. A partition
//! prefix may already have been emitted: its smaller ordinals let the sorted
//! replay suppress those representatives without retaining an unbounded set.
use super::*;
use crate::engine::executor::mem_budget::{self, ScopedCharge};
use crate::engine::executor::row_spool::{RowSpoolBatches, RowSpoolBuilder};
use crate::engine::executor::spill_quota::{self, TrackedSpillReader};

pub(super) struct DistinctPartitionRows {
    spiller: DistinctSpiller,
    schema: Arc<BatchSchema>,
    data_dir: PathBuf,
    partition: usize,
    reader: Option<TrackedSpillReader>,
    seen: AhHashSet<Vec<u8>>,
    seen_charge: ScopedCharge,
    seen_limit: usize,
    fallback: Option<SortedTail>,
    key: Vec<u8>,
}

fn read_record(
    reader: &mut TrackedSpillReader,
    width: usize,
) -> Result<Option<(u64, Vec<Value>, ScopedCharge)>, QueryError> {
    let Some(ordinal) = read_ordinal(reader)? else {
        return Ok(None);
    };
    let actual =
        read_row_width(reader)?.ok_or_else(|| spill_error("truncated row after ordinal"))?;
    if actual != width || width as u64 > reader.remaining_bytes() {
        return Err(spill_error("row width mismatch or truncated values"));
    }
    let mut charge = reader.memory_charge();
    charge.add_force(
        (width + 1).saturating_mul(std::mem::size_of::<Value>())
            + std::mem::size_of::<Vec<Value>>(),
    )?;
    let mut row = mem_budget::try_vec_with_capacity(width + 1, "DISTINCT replay row")?;
    for _ in 0..width {
        let mut tag = [0];
        reader.read_exact(&mut tag).map_err(spill_io_error)?;
        let available = reader.remaining_bytes();
        row.push(crate::engine::executor::spill_value::read_payload_limited(
            reader,
            tag[0],
            true,
            spill_io_error,
            Some(&mut charge),
            Some(available),
        )?);
    }
    row.push(Value::UInt(ordinal));
    Ok(Some((ordinal, row, charge)))
}

fn spill_error(message: &str) -> QueryError {
    crate::error::catalog::ER_UNKNOWN_ERROR.error(format!("DISTINCT spill: {message}"))
}

impl DistinctPartitionRows {
    pub(super) fn new(spiller: DistinctSpiller, schema: Arc<BatchSchema>, data_dir: &Path) -> Self {
        let seen_limit = mem_budget::current_budget().map_or(8 * 1024 * 1024, |budget| {
            (budget.limit() / 16).clamp(16 * 1024, 8 * 1024 * 1024)
        });
        Self {
            spiller,
            schema,
            data_dir: data_dir.to_path_buf(),
            partition: 0,
            reader: None,
            seen: ah_hash_set(),
            seen_charge: ScopedCharge::new(0),
            seen_limit,
            fallback: None,
            key: Vec::new(),
        }
    }

    fn complete_partition(&mut self) -> Result<(), QueryError> {
        self.reader = None;
        self.fallback = None;
        self.seen = ah_hash_set();
        self.seen_charge = self.seen_charge.sibling();
        let path = &self.spiller.paths[self.partition - 1];
        if path.exists() {
            spill_quota::remove_spill_file(path, &self.spiller.charger)?;
        }
        Ok(())
    }
}

impl BatchOperator for DistinctPartitionRows {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        loop {
            if let Some(tail) = self.fallback.as_mut() {
                if let Some(batch) = tail.next_batch()? {
                    return Ok(Some(batch));
                }
                self.complete_partition()?;
            }
            let mut rows = Vec::new();
            let mut charge = self.seen_charge.sibling();
            while rows.len() < DEFAULT_BATCH_ROWS && charge.charged() < output_batch_bytes() {
                crate::engine::cancel::check_cancelled()?;
                if self.reader.is_none() {
                    if self.partition == self.spiller.paths.len() {
                        break;
                    }
                    let path = &self.spiller.paths[self.partition];
                    self.partition += 1;
                    if self.spiller.charger.absent_partition(path)? {
                        continue;
                    }
                    self.reader = Some(TrackedSpillReader::open(
                        path,
                        self.spiller.charger.clone(),
                        spill_quota::writer_buffer_capacity(1),
                    )?);
                }
                let reader = self.reader.as_mut().expect("DISTINCT partition is open");
                let Some((ordinal, row, mut row_charge)) =
                    read_record(reader, self.spiller.output_schema.len())?
                else {
                    self.complete_partition()?;
                    continue;
                };
                encode_row_columns(
                    &row,
                    &self.spiller.key_positions,
                    &self.spiller.key_schema,
                    &mut self.key,
                );
                if self.seen.contains(self.key.as_slice()) {
                    continue;
                }
                let bytes = self.key.len().saturating_add(HASH_SET_ENTRY_OVERHEAD);
                if !self.seen_charge.add_within(bytes, self.seen_limit) {
                    // Drop the capped prefix set before opening sort runs.
                    // Ordinal < cutoff identifies every already-emitted key's
                    // first representative when the whole partition is sorted.
                    self.reader = None;
                    self.seen = ah_hash_set();
                    self.seen_charge = self.seen_charge.sibling();
                    self.fallback = Some(SortedTail::new(
                        self.spiller.paths[self.partition - 1].clone(),
                        self.spiller.charger.clone(),
                        self.schema.clone(),
                        self.spiller.key_schema.clone(),
                        self.spiller.key_positions.clone(),
                        self.data_dir.clone(),
                        ordinal,
                    ));
                    break;
                }
                self.seen
                    .try_reserve(1)
                    .map_err(|_| mem_budget::allocation_error("DISTINCT replay keys"))?;
                self.seen.insert(mem_budget::try_copy_bytes(
                    &self.key,
                    "DISTINCT replay key",
                )?);
                mem_budget::try_reserve_vec_hard(
                    &mut rows,
                    1,
                    &mut charge,
                    "DISTINCT output rows",
                )?;
                // Transposition can retain both row and column slot arrays.
                row_charge.add_force(row.len().saturating_mul(std::mem::size_of::<Value>()))?;
                charge.absorb(&mut row_charge);
                rows.push(row);
            }
            if !rows.is_empty() {
                return rows_to_batch(&self.schema, rows)
                    .with_memory_charge(charge)
                    .map(Some);
            }
            if self.fallback.is_none() {
                return Ok(None);
            }
        }
    }
}

struct SortedTail {
    schema: Arc<BatchSchema>,
    path: PathBuf,
    charger: spill_quota::SpillCharger,
    key_schema: Arc<BatchSchema>,
    key_positions: Vec<usize>,
    data_dir: PathBuf,
    cutoff: u64,
    input: Option<Box<dyn BatchOperator>>,
    previous: Vec<u8>,
    previous_charge: ScopedCharge,
    have_previous: bool,
}

impl SortedTail {
    #[allow(clippy::too_many_arguments)]
    fn new(
        path: PathBuf,
        charger: spill_quota::SpillCharger,
        schema: Arc<BatchSchema>,
        key_schema: Arc<BatchSchema>,
        key_positions: Vec<usize>,
        data_dir: PathBuf,
        cutoff: u64,
    ) -> Self {
        Self {
            schema,
            path,
            charger,
            key_schema,
            key_positions,
            data_dir,
            cutoff,
            input: None,
            previous: Vec::new(),
            previous_charge: ScopedCharge::new(0),
            have_previous: false,
        }
    }

    fn initialize(&mut self) -> Result<(), QueryError> {
        if self.input.is_some() {
            return Ok(());
        }
        let mut fields = self.schema.fields().to_vec();
        fields.push(super::super::schema::BatchField {
            table: String::new(),
            alias: None,
            name: "__distinct_canonical_key".into(),
            column_type: crate::types::ColumnType::Blob,
            source: Default::default(),
        });
        let schema = Arc::new(BatchSchema::new(fields));
        let ordinal_column = self.schema.len() - 1;
        let key_column = self.schema.len();
        let mut reader = TrackedSpillReader::open(
            &self.path,
            self.charger.clone(),
            spill_quota::writer_buffer_capacity(1),
        )?;
        let mut spool = RowSpoolBuilder::new(&self.data_dir, schema.len())?;
        let mut key = Vec::new();
        while let Some((_, mut row, mut charge)) = read_record(&mut reader, ordinal_column)? {
            encode_row_columns(&row, &self.key_positions, &self.key_schema, &mut key);
            charge.add_force(key.len() + std::mem::size_of::<Value>())?;
            mem_budget::try_reserve_vec_exact(&mut row, 1, "DISTINCT canonical key slot")?;
            row.push(Value::Bytes(mem_budget::try_copy_bytes(
                &key,
                "DISTINCT canonical key",
            )?));
            spool.append(&row)?;
        }
        drop(reader);
        let spool = spool.finish()?;
        let source = RowSpoolBatches::new(&spool, schema.clone())?;
        // The checked spool now owns all required rows; release the parent
        // generation before producing more sort runs.
        spill_quota::remove_spill_file(&self.path, &self.charger)?;
        let field = crate::engine::executor::sort::SortField {
            ascending: true,
            nulls_first: true,
            collation_id: None,
        };
        self.input = Some(Box::new(
            super::super::sort::ExternalSortBatchOperator::new(
                None,
                Box::new(source),
                schema,
                BatchProperties::UNORDERED_SINGLE,
                vec![
                    super::super::sort::SortKeySource::Column(key_column),
                    super::super::sort::SortKeySource::Column(ordinal_column),
                ],
                vec![field, field],
                &self.data_dir,
            ),
        ));
        Ok(())
    }
}

fn bytes_at(column: &ColumnVector, row: usize) -> Result<&[u8], QueryError> {
    match column {
        ColumnVector::Bytes { offsets, data, .. } => {
            Ok(&data[offsets[row] as usize..offsets[row + 1] as usize])
        }
        ColumnVector::Generic { values } => match &values[row] {
            Value::Bytes(bytes) => Ok(bytes),
            _ => Err(spill_error("invalid canonical key value")),
        },
        _ => Err(spill_error("invalid canonical key lane")),
    }
}

impl BatchOperator for SortedTail {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.initialize()?;
        while let Some(batch) = self.input.as_mut().unwrap().next_batch()? {
            let key_column = self.schema.len();
            let ordinal_column = key_column - 1;
            let mut selection_charge = self.previous_charge.sibling();
            selection_charge.add_force(
                batch
                    .visible_rows()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )?;
            let mut selected = mem_budget::try_vec_with_capacity(
                batch.visible_rows(),
                "DISTINCT sorted selection",
            )?;
            for row in batch.visible_indices() {
                let key = bytes_at(&batch.columns()[key_column], row)?;
                if self.have_previous && self.previous == key {
                    continue;
                }
                let mut next_charge = self.previous_charge.sibling();
                next_charge.add_force(key.len())?;
                self.previous = mem_budget::try_copy_bytes(key, "DISTINCT previous sorted key")?;
                self.previous_charge = next_charge;
                self.have_previous = true;
                let ordinal = match batch.columns()[ordinal_column].value_at(row) {
                    Value::UInt(ordinal) => ordinal,
                    _ => return Err(spill_error("invalid ordinal")),
                };
                if ordinal >= self.cutoff {
                    selected.push(row as u32);
                }
            }
            if selected.is_empty() {
                continue;
            }
            let mut columns = batch.into_columns()?;
            columns.attach_charge(selection_charge);
            columns.pop();
            return Ok(Some(
                Batch::from_owned_columns(self.schema.clone(), columns)
                    .with_selection(super::super::batch::Selection::Indices(selected)),
            ));
        }
        self.previous = Vec::new();
        self.previous_charge = self.previous_charge.sibling();
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::types::ColumnType;

    #[test]
    fn oversized_partition_sorts_tail_without_reemitting_prefix_or_changing_payload() {
        let dir = tempfile::tempdir().unwrap();
        let budget = Arc::new(MemBudget::new(1024 * 1024));
        with_query_budget(budget.clone(), || {
            let field = |name: &str, column_type| super::super::super::schema::BatchField {
                table: String::new(),
                alias: None,
                name: name.into(),
                column_type,
                source: Default::default(),
            };
            let output_schema = Arc::new(BatchSchema::new(vec![
                field("k", ColumnType::LongLong),
                field("p", ColumnType::VarString),
            ]));
            let key_schema = projected_schema(&output_schema, &[0]);
            let mut spiller = DistinctSpiller::new(
                dir.path(),
                output_schema.clone(),
                key_schema.clone(),
                vec![0],
            )
            .unwrap();
            let mut keys = Vec::new();
            let mut encoded = Vec::new();
            for i in 0..100_000 {
                encode_row(&[Value::Int(i)], &key_schema, &mut encoded);
                if super::super::super::key_encode::fxhash(&encoded) & 255 == 0 {
                    keys.push(i);
                }
                if keys.len() == 40 {
                    break;
                }
            }
            assert_eq!(keys.len(), 40);
            for pass in 0..3 {
                for (index, &key) in keys.iter().enumerate() {
                    encode_row(&[Value::Int(key)], &key_schema, &mut encoded);
                    spiller
                        .append(
                            &encoded,
                            (pass * keys.len() + index) as u64,
                            &[
                                Value::Int(key),
                                Value::String(
                                    if pass == 0 { "first" } else { "later" }.repeat(128),
                                ),
                            ],
                        )
                        .unwrap();
                }
            }
            for writer in spiller.writers.drain(..).flatten() {
                writer.finish().unwrap();
            }
            let mut fields = output_schema.fields().to_vec();
            fields.push(field("ordinal", ColumnType::LongLong));
            let schema = Arc::new(BatchSchema::new(fields));
            let mut replay = DistinctPartitionRows::new(spiller, schema, dir.path());
            replay.seen_limit = 128;
            let prefix = replay.next_batch().unwrap().unwrap();
            assert!(
                replay.fallback.is_some(),
                "must exercise external-key reduction"
            );
            let mut rows = prefix
                .visible_indices()
                .map(|i| prefix.row(i))
                .collect::<Vec<_>>();
            drop(prefix);
            let mut held = None;
            while let Some(batch) = replay.next_batch().unwrap() {
                rows.extend(batch.visible_indices().map(|i| batch.row(i)));
                held = Some(batch);
            }
            rows.sort_by_key(|row| match row[2] {
                Value::UInt(n) => n,
                _ => panic!("ordinal"),
            });
            assert_eq!(rows.len(), keys.len());
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(row[0], Value::Int(keys[i]));
                assert_eq!(
                    crate::types::value_str(&row[1]),
                    Some("first".repeat(128).as_str())
                );
                assert_eq!(row[2], Value::UInt(i as u64));
            }
            drop(replay);
            assert!(
                budget.used() > 0,
                "consumer-held output must keep its owner"
            );
            drop(held);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.spill_bytes(), 0);
        });
    }
}
