//! Streaming Distinct over a `BatchOperator`.
//!
//! Reuses the group-key encoder from the vectorized HashAggregate: every
//! row's projection key is encoded into a byte buffer and inserted into a
//! `HashSet`. Rows whose key was already present are dropped. Output preserves
//! first-occurrence order in memory and after spill. The stable ordinal is
//! required when DISTINCT consumes an ordered subtree (for example the
//! physical shape used by `SELECT DISTINCT .. ORDER BY ..`), and also keeps
//! unordered queries deterministic across memory-pressure changes.
//!
//! Memory: every distinct key + row added to the dedup set is charged
//! against a [`WorkAreaHandle`]. When the arbiter signals `Spill`, existing
//! and future rows are hash-partitioned into RAII-owned temporary files and
//! deduplicated one partition at a time.

use crate::engine::executor::mem_budget::estimate_row_bytes;
use crate::engine::executor::memory_grant::{GrantResize, OperatorMemoryGrant};
use crate::types::{QueryError, Value};
use crate::util::fast_hash::{ah_hash_set, AhHashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::batch::{Batch, ColumnVector, DEFAULT_BATCH_ROWS};
use super::schema::BatchSchema;
use super::stream::{BatchOperator, BatchProperties};

/// Per-entry fixed overhead for `hashbrown::HashSet<Vec<u8>>`:
/// 16 bytes for the table slot + 32 bytes for the `Vec<u8>` header.
const HASH_SET_ENTRY_OVERHEAD: usize = 16 + 32;

/// Consume `stream` and emit each distinct row, projecting only the
/// `output_columns`. The "key" used for deduplication is the projected
/// row itself.
pub fn run_distinct(
    stream: &mut dyn BatchOperator,
    output_columns: &[usize],
    data_dir: &Path,
) -> Result<Vec<Vec<Value>>, QueryError> {
    run_distinct_by(stream, output_columns, output_columns, data_dir)
}

/// Deduplicate by `key_columns` while preserving `output_columns` from the
/// first matching row. SELECT DISTINCT uses this with identity output so
/// hidden/non-projected fields remain available to the final protocol
/// projection without participating in equality.
pub fn run_distinct_by(
    stream: &mut dyn BatchOperator,
    key_columns: &[usize],
    output_columns: &[usize],
    data_dir: &Path,
) -> Result<Vec<Vec<Value>>, QueryError> {
    run_distinct_impl(stream, key_columns, output_columns, data_dir, None)
}

fn run_distinct_impl(
    stream: &mut dyn BatchOperator,
    key_columns: &[usize],
    output_columns: &[usize],
    data_dir: &Path,
    force_spill_after: Option<usize>,
) -> Result<Vec<Vec<Value>>, QueryError> {
    let mut output = prepare_distinct_impl(
        stream,
        key_columns,
        output_columns,
        data_dir,
        force_spill_after,
    )?;
    let mut rows = Vec::new();
    while let Some(batch) = output.next_batch()? {
        for row in batch.visible_indices() {
            rows.push(batch.row(row));
        }
    }
    Ok(rows)
}

/// Keep finalized DISTINCT output bounded, including after partition spill.
pub(crate) fn run_distinct_stream(
    stream: &mut dyn BatchOperator,
    key_columns: &[usize],
    output_columns: &[usize],
    data_dir: &Path,
) -> Result<Box<dyn BatchOperator>, QueryError> {
    prepare_distinct_impl(stream, key_columns, output_columns, data_dir, None)
}

/// Keep first representatives while allowing arbitrary output order. The
/// caller must prove that a later operator restores every required ordering.
pub(crate) fn run_distinct_stream_unordered(
    stream: &mut dyn BatchOperator,
    key_columns: &[usize],
    output_columns: &[usize],
    data_dir: &Path,
) -> Result<Box<dyn BatchOperator>, QueryError> {
    prepare_distinct_with_order(stream, key_columns, output_columns, data_dir, None, false)
}

fn prepare_distinct_impl(
    stream: &mut dyn BatchOperator,
    key_columns: &[usize],
    output_columns: &[usize],
    data_dir: &Path,
    force_spill_after: Option<usize>,
) -> Result<Box<dyn BatchOperator>, QueryError> {
    prepare_distinct_with_order(
        stream,
        key_columns,
        output_columns,
        data_dir,
        force_spill_after,
        true,
    )
}

fn prepare_distinct_with_order(
    stream: &mut dyn BatchOperator,
    key_columns: &[usize],
    output_columns: &[usize],
    data_dir: &Path,
    force_spill_after: Option<usize>,
    restore_order: bool,
) -> Result<Box<dyn BatchOperator>, QueryError> {
    let mut grant = OperatorMemoryGrant::register();
    let mut seen: AhHashSet<Vec<u8>> = ah_hash_set();
    let mut rows: Vec<(u64, Vec<Value>)> = Vec::new();
    let mut ordinal = 0u64;
    let mut key_buf = Vec::with_capacity(64);
    let mut spiller: Option<DistinctSpiller> = None;
    let key_schema = projected_schema(stream.schema(), key_columns);
    let output_schema = projected_schema(stream.schema(), output_columns);
    let key_positions: Vec<usize> = key_columns
        .iter()
        .map(|key| {
            output_columns
                .iter()
                .position(|output| output == key)
                .expect("DISTINCT key must be present in its output")
        })
        .collect();

    while let Some(batch) = stream.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        for physical_row in batch.visible_indices() {
            let row_ordinal = ordinal;
            ordinal = ordinal.checked_add(1).ok_or_else(|| {
                crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "DISTINCT input row ordinal overflow",
                )
            })?;
            encode_batch_row(&batch, key_columns, &key_schema, physical_row, &mut key_buf);
            if let Some(spiller) = spiller.as_mut() {
                let row_bytes = projected_row_bytes(&batch, output_columns, physical_row);
                let _row_charge =
                    crate::engine::executor::mem_budget::force_charge_scoped(row_bytes)?;
                let row = project_row(&batch, output_columns, physical_row)?;
                spiller.append(&key_buf, row_ordinal, &row)?;
                continue;
            }
            if seen.contains(key_buf.as_slice()) {
                continue;
            }
            let added_bytes = key_buf
                .len()
                .saturating_add(HASH_SET_ENTRY_OVERHEAD)
                .saturating_add(projected_row_bytes(&batch, output_columns, physical_row));
            let decision = if force_spill_after.is_some_and(|limit| seen.len() >= limit) {
                GrantResize::Spill
            } else {
                grant.resize(grant.charged_bytes().saturating_add(added_bytes))
            };
            let keep_in_memory = match decision {
                GrantResize::Granted => true,
                GrantResize::Spill => false,
            };
            if keep_in_memory {
                seen.try_reserve(1).map_err(|_| {
                    crate::engine::executor::mem_budget::allocation_error(
                        "unable to grow DISTINCT key set",
                    )
                })?;
                crate::engine::executor::mem_budget::try_reserve_vec_exact(
                    &mut rows,
                    1,
                    "DISTINCT retained rows",
                )?;
                let row = project_row(&batch, output_columns, physical_row)?;
                seen.insert(crate::engine::executor::mem_budget::try_copy_bytes(
                    &key_buf,
                    "DISTINCT key",
                )?);
                rows.push((row_ordinal, row));
            } else {
                // Query-budget rejection is a soft-pressure signal for this
                // spill-capable operator. Move already accepted rows to the
                // partitioned spill stream instead of surfacing 1037.
                let mut next = DistinctSpiller::new(
                    data_dir,
                    output_schema.clone(),
                    key_schema.clone(),
                    key_positions.clone(),
                )?;
                let mut existing_key = Vec::with_capacity(64);
                drop(std::mem::take(&mut seen));
                for (existing_ordinal, existing) in std::mem::take(&mut rows) {
                    encode_row_columns(&existing, &key_positions, &key_schema, &mut existing_key);
                    next.append(&existing_key, existing_ordinal, &existing)?;
                }
                let _ = grant.resize(0);
                let row_bytes = projected_row_bytes(&batch, output_columns, physical_row);
                let _row_charge =
                    crate::engine::executor::mem_budget::force_charge_scoped(row_bytes)?;
                let row = project_row(&batch, output_columns, physical_row)?;
                next.append(&key_buf, row_ordinal, &row)?;
                spiller = Some(next);
            }
        }
    }
    match spiller {
        Some(spiller) if restore_order => spiller.finish_stream(data_dir),
        Some(spiller) => spiller.finish_stream_with_order(data_dir, false),
        None => {
            drop(seen);
            let retained = rows
                .capacity()
                .saturating_mul(std::mem::size_of::<(u64, Vec<Value>)>())
                .saturating_add(
                    rows.iter()
                        .map(|(_, row)| {
                            estimate_row_bytes(row)
                                .saturating_sub(std::mem::size_of::<Vec<Value>>())
                        })
                        .sum::<usize>(),
                );
            let _ = grant.resize(retained);
            Ok(Box::new(ResidentDistinctRows {
                schema: output_schema,
                rows: rows.into_iter(),
                _grant: grant,
            }))
        }
    }
}

struct ResidentDistinctRows {
    schema: Arc<BatchSchema>,
    rows: std::vec::IntoIter<(u64, Vec<Value>)>,
    _grant: OperatorMemoryGrant,
}

fn output_batch_bytes() -> usize {
    crate::engine::executor::mem_budget::current_budget().map_or(4 * 1024 * 1024, |budget| {
        (budget.limit() / 8).clamp(32 * 1024, 4 * 1024 * 1024)
    })
}

fn rows_to_batch(schema: &Arc<BatchSchema>, rows: Vec<Vec<Value>>) -> Batch {
    let count = rows.len();
    let mut columns = (0..schema.len())
        .map(|_| Vec::with_capacity(count))
        .collect::<Vec<_>>();
    for row in rows {
        for (column, value) in columns.iter_mut().zip(row) {
            column.push(value);
        }
    }
    Batch::with_row_count(
        schema.clone(),
        columns.into_iter().map(ColumnVector::from_values).collect(),
        count,
    )
}

impl BatchOperator for ResidentDistinctRows {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let mut scratch = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        let mut rows = Vec::new();
        let mut bytes = 0;
        while rows.len() < DEFAULT_BATCH_ROWS && bytes < output_batch_bytes() {
            let Some((_, row)) = self.rows.next() else {
                break;
            };
            bytes += estimate_row_bytes(&row);
            if rows.len() == rows.capacity() {
                let additional = rows.capacity().max(8).min(DEFAULT_BATCH_ROWS - rows.len());
                crate::engine::executor::mem_budget::try_reserve_vec_hard(
                    &mut rows,
                    additional,
                    &mut scratch,
                    "resident DISTINCT output rows",
                )?;
            }
            rows.push(row);
        }
        if rows.is_empty() {
            self.rows = Vec::new().into_iter();
            let _ = self._grant.resize(0);
            return Ok(None);
        }
        scratch.add_force(bytes.saturating_mul(2))?;
        let released =
            bytes.saturating_sub(rows.len().saturating_mul(std::mem::size_of::<Vec<Value>>()));
        let batch = rows_to_batch(&self.schema, rows);
        let _ = self
            ._grant
            .resize(self._grant.charged_bytes().saturating_sub(released));
        batch.with_memory_charge(scratch).map(Some)
    }
}

fn projected_schema(schema: &Arc<BatchSchema>, columns: &[usize]) -> Arc<BatchSchema> {
    Arc::new(BatchSchema::new(
        columns
            .iter()
            .map(|&column| schema.fields()[column].clone())
            .collect(),
    ))
}

fn project_row(
    batch: &Batch,
    columns: &[usize],
    physical_row: usize,
) -> Result<Vec<Value>, QueryError> {
    let mut row = crate::engine::executor::mem_budget::try_vec_with_capacity(
        columns.len(),
        "DISTINCT projected row",
    )?;
    for &column in columns {
        row.push(batch.columns()[column].try_value_at(physical_row)?);
    }
    Ok(row)
}

fn projected_row_bytes(batch: &Batch, columns: &[usize], physical_row: usize) -> usize {
    columns
        .iter()
        .fold(std::mem::size_of::<Vec<Value>>(), |bytes, &column| {
            bytes.saturating_add(batch.columns()[column].materialized_value_bytes(physical_row))
        })
}

fn encode_batch_row(
    batch: &Batch,
    columns: &[usize],
    schema: &BatchSchema,
    physical_row: usize,
    key: &mut Vec<u8>,
) {
    key.clear();
    for (projected_column, &batch_column) in columns.iter().enumerate() {
        super::key_encode::encode_from_column_row_with_collation(
            &batch.columns()[batch_column],
            physical_row,
            schema.fields()[projected_column].source.collation_id,
            key,
        );
    }
}

#[cfg(test)]
fn encode_row(row: &[Value], schema: &BatchSchema, key: &mut Vec<u8>) {
    key.clear();
    for (column, value) in row.iter().enumerate() {
        super::key_encode::encode_with_collation(
            value,
            schema.fields()[column].source.collation_id,
            key,
        );
    }
}

fn encode_row_columns(row: &[Value], columns: &[usize], schema: &BatchSchema, key: &mut Vec<u8>) {
    key.clear();
    for (key_column, &row_column) in columns.iter().enumerate() {
        super::key_encode::encode_with_collation(
            &row[row_column],
            schema.fields()[key_column].source.collation_id,
            key,
        );
    }
}

const DISTINCT_SPILL_PARTITIONS: usize = 256;

struct DistinctSpiller {
    _dir: crate::engine::executor::select::grace::SpillTmpDir,
    paths: Vec<PathBuf>,
    writers: Vec<Option<crate::engine::executor::spill_quota::BufferedSpillAppendWriter>>,
    output_schema: Arc<BatchSchema>,
    key_schema: Arc<BatchSchema>,
    key_positions: Vec<usize>,
    charger: crate::engine::executor::spill_quota::SpillCharger,
}

impl DistinctSpiller {
    fn new(
        data_dir: &Path,
        output_schema: Arc<BatchSchema>,
        key_schema: Arc<BatchSchema>,
        key_positions: Vec<usize>,
    ) -> Result<Self, QueryError> {
        let dir =
            crate::engine::executor::select::grace::SpillTmpDir::create_kind(data_dir, "distinct")?;
        let paths = (0..DISTINCT_SPILL_PARTITIONS)
            .map(|partition| dir.path().join(format!("part-{partition:03}.bin")))
            .collect();
        let writers = (0..DISTINCT_SPILL_PARTITIONS).map(|_| None).collect();
        let charger = dir.charger();
        Ok(Self {
            _dir: dir,
            paths,
            writers,
            output_schema,
            key_schema,
            key_positions,
            charger,
        })
    }

    fn append(&mut self, key: &[u8], ordinal: u64, row: &[Value]) -> Result<(), QueryError> {
        let partition = (super::key_encode::fxhash(key) as usize) & (DISTINCT_SPILL_PARTITIONS - 1);
        if self.writers[partition].is_none() {
            let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(
                DISTINCT_SPILL_PARTITIONS,
            );
            self.writers[partition] = Some(
                crate::engine::executor::spill_quota::BufferedSpillAppendWriter::new(
                    &self.paths[partition],
                    self.charger.clone(),
                    capacity,
                ),
            );
        }
        let writer = self.writers[partition].as_mut().unwrap();
        writer
            .write_all(&ordinal.to_le_bytes())
            .map_err(spill_io_error)?;
        writer
            .write_all(&(row.len() as u32).to_le_bytes())
            .map_err(spill_io_error)?;
        for value in row {
            super::spill::write_value(writer, value)?;
        }
        Ok(())
    }

    fn finish_stream(self, data_dir: &Path) -> Result<Box<dyn BatchOperator>, QueryError> {
        self.finish_stream_with_order(data_dir, true)
    }

    fn finish_stream_with_order(
        mut self,
        data_dir: &Path,
        restore_order: bool,
    ) -> Result<Box<dyn BatchOperator>, QueryError> {
        for writer in self.writers.drain(..).flatten() {
            writer.finish()?;
        }
        let mut fields = self.output_schema.fields().to_vec();
        fields.push(super::schema::BatchField {
            table: String::new(),
            alias: None,
            name: "__distinct_ordinal".into(),
            column_type: crate::types::ColumnType::LongLong,
            source: Default::default(),
        });
        let schema = Arc::new(BatchSchema::new(fields));
        let ordinal_column = self.output_schema.len();
        let output_schema = self.output_schema.clone();
        let input = replay::DistinctPartitionRows::new(self, schema.clone(), data_dir);
        let sorted: Box<dyn BatchOperator> = if restore_order {
            Box::new(super::sort::ExternalSortBatchOperator::new(
                None,
                Box::new(input),
                schema,
                BatchProperties::UNORDERED_SINGLE,
                vec![super::sort::SortKeySource::Column(ordinal_column)],
                vec![crate::engine::executor::sort::SortField {
                    ascending: true,
                    nulls_first: true,
                    collation_id: None,
                }],
                data_dir,
            ))
        } else {
            Box::new(input)
        };
        Ok(Box::new(DistinctOutput {
            input: sorted,
            schema: output_schema,
        }))
    }
}

mod replay;

struct DistinctOutput {
    input: Box<dyn BatchOperator>,
    schema: Arc<BatchSchema>,
}

impl BatchOperator for DistinctOutput {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let Some(batch) = self.input.next_batch()? else {
            return Ok(None);
        };
        let mut columns = batch.into_columns()?;
        columns.pop();
        Ok(Some(Batch::from_owned_columns(
            self.schema.clone(),
            columns,
        )))
    }
}

fn read_ordinal(reader: &mut impl Read) -> Result<Option<u64>, QueryError> {
    let mut bytes = [0u8; 8];
    let read = reader.read(&mut bytes[..1]).map_err(spill_io_error)?;
    if read == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut bytes[1..]).map_err(spill_io_error)?;
    Ok(Some(u64::from_le_bytes(bytes)))
}

fn read_row_width(reader: &mut impl Read) -> Result<Option<usize>, QueryError> {
    let mut bytes = [0u8; 4];
    let read = reader.read(&mut bytes[..1]).map_err(spill_io_error)?;
    if read == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut bytes[1..]).map_err(spill_io_error)?;
    Ok(Some(u32::from_le_bytes(bytes) as usize))
}

fn spill_io_error(error: std::io::Error) -> QueryError {
    if let Some(error) = crate::engine::executor::spill_quota::query_error_from_io(&error) {
        return error;
    }
    crate::storage::error(
        crate::error::catalog::ER_UNKNOWN_ERROR,
        format!("distinct spill I/O: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::vec::batch::{Batch, ColumnVector};
    use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
    use crate::engine::executor::vec::stream::MaterializedBatchOperator;
    use crate::types::ColumnType;
    use std::sync::Arc;

    #[test]
    fn distinct_preserves_first_occurrence_order() {
        let s = Arc::new(BatchSchema::new(vec![BatchField {
            table: String::new(),
            alias: None,
            name: "k".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }]));
        let b = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Int(2),
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
                Value::Int(1),
            ])],
        );
        let mut stream = MaterializedBatchOperator::new(s, vec![b]);
        let dir = tempfile::tempdir().unwrap();
        let out = run_distinct(&mut stream, &[0], dir.path()).unwrap();
        let got: Vec<i64> = out
            .into_iter()
            .map(|r| match r[0] {
                Value::Int(x) => x,
                _ => panic!(),
            })
            .collect();
        assert_eq!(got, vec![2, 1, 3]);
    }

    #[test]
    fn distinct_handles_null_as_value() {
        let s = Arc::new(BatchSchema::new(vec![BatchField {
            table: String::new(),
            alias: None,
            name: "k".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }]));
        let b = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Null,
                Value::Int(1),
                Value::Null,
                Value::Int(1),
            ])],
        );
        let mut stream = MaterializedBatchOperator::new(s, vec![b]);
        let dir = tempfile::tempdir().unwrap();
        let out = run_distinct(&mut stream, &[0], dir.path()).unwrap();
        assert_eq!(out.len(), 2);
        assert!(matches!(out[0][0], Value::Null));
        assert!(matches!(out[1][0], Value::Int(1)));
    }

    #[test]
    fn typed_batch_key_matches_materialized_row_key() {
        let s = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: String::new(),
                alias: None,
                name: "i".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: String::new(),
                alias: None,
                name: "s".into(),
                column_type: ColumnType::VarString,
                source: Default::default(),
            },
        ]));
        let batch = Batch::new(
            s.clone(),
            vec![
                ColumnVector::from_values(vec![Value::Int(7), Value::Null]),
                ColumnVector::from_values(vec![Value::String("AbC".into()), Value::Null]),
            ],
        );
        for row_index in 0..batch.num_rows() {
            let row = project_row(&batch, &[0, 1], row_index).unwrap();
            let mut materialized = Vec::new();
            let mut typed = Vec::new();
            encode_row(&row, &s, &mut materialized);
            encode_batch_row(&batch, &[0, 1], &s, row_index, &mut typed);
            assert_eq!(typed, materialized);
        }
    }

    #[test]
    fn distinct_spill_deduplicates_and_cleans_temp_files() {
        let s = Arc::new(BatchSchema::new(vec![BatchField {
            table: String::new(),
            alias: None,
            name: "k".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }]));
        let b = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Int(3),
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
                Value::Int(2),
                Value::Int(4),
            ])],
        );
        let mut stream = MaterializedBatchOperator::new(s, vec![b]);
        let dir = tempfile::tempdir().unwrap();
        let out = run_distinct_impl(&mut stream, &[0], &[0], dir.path(), Some(2)).unwrap();
        assert_eq!(
            out,
            vec![
                vec![Value::Int(3)],
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(4)],
            ]
        );
        let tmp = crate::storage::naming::system_tmp_dir(dir.path());
        let leaked = std::fs::read_dir(tmp)
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().starts_with("distinct-"));
        assert!(!leaked, "distinct spill directory must be RAII-cleaned");
    }

    #[test]
    fn distinct_by_key_preserves_first_full_payload_across_spill() {
        let s = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: String::new(),
                alias: None,
                name: "k".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: String::new(),
                alias: None,
                name: "payload".into(),
                column_type: ColumnType::VarString,
                source: Default::default(),
            },
        ]));
        let b = Batch::new(
            s.clone(),
            vec![
                ColumnVector::from_values(vec![
                    Value::Int(1),
                    Value::Int(2),
                    Value::Int(1),
                    Value::Int(3),
                ]),
                ColumnVector::from_values(vec![
                    Value::String("first".into()),
                    Value::String("two".into()),
                    Value::String("duplicate".into()),
                    Value::String("three".into()),
                ]),
            ],
        );
        let mut stream = MaterializedBatchOperator::new(s, vec![b]);
        let dir = tempfile::tempdir().unwrap();
        let rows = run_distinct_impl(&mut stream, &[0], &[0, 1], dir.path(), Some(1)).unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows.contains(&vec![Value::Int(1), Value::String("first".into())]));
        assert!(!rows.contains(&vec![Value::Int(1), Value::String("duplicate".into())]));
    }

    #[cfg(unix)]
    #[test]
    fn distinct_spill_respects_low_file_descriptor_limit() {
        use std::os::unix::process::CommandExt;
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "engine::executor::vec::distinct::tests::distinct_stream_large_output_keeps_stable_payload_and_releases_budget",
            "--nocapture",
        ]);
        // Limit only the child, never other tests sharing the parent process.
        // SAFETY: the pre-exec hook invokes only getrlimit/setrlimit and does
        // not allocate or touch locks inherited from other threads.
        unsafe {
            command.pre_exec(|| {
                let mut limit = libc::rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                limit.rlim_cur = limit.rlim_cur.min(256);
                if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn distinct_stream_large_output_keeps_stable_payload_and_releases_budget() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: String::new(),
                alias: None,
                name: "k".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: String::new(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::VarString,
                source: Default::default(),
            },
        ]));
        let batches = (0..64)
            .map(|batch| {
                Batch::new(
                    schema.clone(),
                    vec![
                        ColumnVector::from_values(
                            (0..512)
                                .map(|row| Value::Int((32767 - (batch * 512 + row)) % 16384))
                                .collect(),
                        ),
                        ColumnVector::from_values(
                            (0..512)
                                .map(|_| Value::String("payload".repeat(64)))
                                .collect(),
                        ),
                    ],
                )
            })
            .collect();
        let budget = Arc::new(MemBudget::new(1024 * 1024));
        with_query_budget(budget.clone(), || {
            let mut input = MaterializedBatchOperator::new(schema, batches);
            let open_files = crate::engine::executor::spill_quota::active_spill_files();
            let mut output =
                prepare_distinct_impl(&mut input, &[0], &[0, 1], dir.path(), Some(8)).unwrap();
            assert_eq!(
                crate::engine::executor::spill_quota::active_spill_files(),
                open_files
            );
            let mut next = 16383;
            while let Some(batch) = output.next_batch().unwrap() {
                for row in batch.visible_indices() {
                    assert_eq!(batch.columns()[0].value_at(row), Value::Int(next));
                    assert_eq!(
                        crate::types::value_str(&batch.columns()[1].value_at(row)),
                        Some("payload".repeat(64).as_str())
                    );
                    next -= 1;
                }
            }
            assert_eq!(next, -1);
            drop(output);
            assert_eq!(budget.used(), 0);
            assert_eq!(
                crate::engine::executor::spill_quota::active_spill_files(),
                open_files
            );
            assert_eq!(budget.spill_bytes(), 0);
        });
    }
}

#[test]
fn distinct_one_large_partition_has_bounded_replay_memory() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let dir = tempfile::tempdir().unwrap();
    let budget = Arc::new(MemBudget::new(1024 * 1024));
    with_query_budget(budget.clone(), || {
        let schema = Arc::new(BatchSchema::new(vec![super::schema::BatchField {
            table: String::new(),
            alias: None,
            name: "key".into(),
            column_type: crate::types::ColumnType::Blob,
            source: Default::default(),
        }]));
        let spiller = DistinctSpiller::new(dir.path(), schema.clone(), schema, vec![0]).unwrap();
        // An externally partitioned input may contain arbitrary collisions.
        // Write a valid single partition directly rather than requiring the
        // test to search for thousands of equally routed wide byte keys.
        let mut writer = crate::engine::executor::spill_quota::BufferedSpillAppendWriter::new(
            &spiller.paths[0],
            spiller.charger.clone(),
            32 * 1024,
        );
        for ordinal in 0..4096u64 {
            let mut key = vec![b'x'; 4096];
            key[..8].copy_from_slice(&ordinal.to_le_bytes());
            writer.write_all(&ordinal.to_le_bytes()).unwrap();
            writer.write_all(&1u32.to_le_bytes()).unwrap();
            super::spill::write_value(&mut writer, &Value::Bytes(key)).unwrap();
        }
        writer.finish().unwrap();
        let mut output = spiller.finish_stream(dir.path()).unwrap();
        let mut count = 0u64;
        while let Some(batch) = output.next_batch().unwrap() {
            for row in batch.visible_indices() {
                let Value::Bytes(bytes) = batch.columns()[0].value_at(row) else {
                    panic!("key");
                };
                assert_eq!(u64::from_le_bytes(bytes[..8].try_into().unwrap()), count);
                count += 1;
            }
        }
        assert_eq!(count, 4096);
        assert!(
            budget.peak_used() < 4 * 1024 * 1024,
            "partition replay retained {} bytes for a 1 MiB work area",
            budget.peak_used()
        );
        drop(output);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
    });
}
