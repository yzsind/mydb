//! Vectorized push-style HashAggregate over a `BatchOperator`.
//!
//! This is a drop-in replacement for the row-based `execute_hash_aggregate`
//! path with partition-based spilling: when the per-query memory budget signals overflow, the operator
//! picks its largest partition, serialises its current per-group state to
//! a spill file under `<MYDB_DATA_DIR>/tmp/hashagg-<pid>-<seq>/`, then
//! clears that partition's in-memory buckets. On `finalize` the spilled
//! partial states are read back and merged with whatever in-memory state
//! has since accumulated for the same key, producing identical results to
//! the all-in-memory path.
//!
//! Key properties:
//!
//! - **Push model** — rows are inserted into the hash table batch-at-a-
//!   time as upstream produces batches, so the operator never materialises
//!   the full input.
//! - **Partitioned table** — the hash table is split into 64 partitions
//!   keyed by the top 6 bits of the group hash. This lays the groundwork
//!   for parallel HashAgg (one partition per worker, no final
//!   merge step) and for the per-partition spill flow above.
//! - **Typed group keys** — fixed-width keys (INT64 / FLOAT64) are encoded
//!   into a `[u8; 24]` buffer; variable-width keys fall back to a
//!   serialised byte vector. The hash table key is the raw byte sequence
//!   so equality / hash both work without a custom comparator.
//!
//! Aggregates implemented here: `COUNT(*)`, `COUNT(col)`,
//! `SUM(col)`, `MIN(col)`, `MAX(col)`, `AVG(col)` (as SUM/COUNT pair).
//! Other shapes (`COUNT(DISTINCT)`, `GROUP_CONCAT`, MEDIAN, etc.) cause
//! `try_build` to return `Ok(None)` so the caller can fall back to the
//! legacy row-based path.

mod spill_layout;
use spill_layout::{build_spill_schema, SpillLayout};

mod direct_join;
use direct_join::DirectJoinState;
#[cfg(test)]
use direct_join::JoinAggregateGroups;

#[cfg(test)]
mod lane_tests;
#[cfg(test)]
mod merge_tests;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Group-key → bucket-index map used across all HashAggregateCore partitions.
/// ahash hashing is 2-3× cheaper than std SipHash-1-3 on the short
/// encoded byte keys produced by `key_encode`, where the hot inner loop
/// runs `partition.by_key.get(&buf[..])` once per row.
type GroupMap<V> = hashbrown::HashMap<Vec<u8>, V, ahash::RandomState>;

#[derive(Clone, Default)]
pub(crate) struct U128BuildHasher;

#[derive(Default)]
pub(crate) struct U128Hasher(u64);

impl std::hash::BuildHasher for U128BuildHasher {
    type Hasher = U128Hasher;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        U128Hasher::default()
    }
}

impl std::hash::Hasher for U128Hasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0 = super::key_encode::fxhash(bytes);
    }

    #[inline]
    fn write_u128(&mut self, value: u128) {
        self.0 = fx_mix_u128(value);
    }
}

/// Allocation header retained once by each canonical `Arc<[u8]>`. Both the
/// dictionary map and reverse-token vector share that allocation, so count the
/// header here rather than charging the bytes twice at the two owners.
const ARC_SLICE_HEADER_BYTES: usize = 2 * std::mem::size_of::<usize>();

use crate::engine::executor::hash_aggregate::{
    merge_variance as variance_merge, update_bit as bit_update, update_variance as variance_update,
    AggregateState as AggState, DistinctState,
};
pub use crate::engine::executor::hash_aggregate::{
    AggregatePartial as AggStateSnapshot, ExtraAggregateState as ExtraAggState,
};

use crate::engine::executor::mem_budget::MemBudget;
use crate::engine::executor::select::aggregate::{
    accumulate_numeric_state, accumulate_numeric_state_decimal_parts,
    accumulate_numeric_state_float, merge_numeric_state, numeric_state_heap_bytes,
    numeric_state_to_value, InlineAggState, NumericAggState,
};
use crate::engine::executor::select::grace::SpillTmpDir;
use crate::engine::executor::vec::expr_batch::{
    count_non_null_in_selection, sum_decimal_column_with_nulls, sum_decimal_column_with_selection,
};
use crate::types::{ColumnType, QueryError, Value};

use super::batch::{Batch, ColumnAccumulator, ColumnVector, DEFAULT_BATCH_ROWS};
use super::bitmap::Bitmap;
use super::schema::{BatchField, BatchSchema};
use super::spill;
use super::stream::{BatchOperator, BatchProperties};

pub const NUM_PARTITIONS: usize = 64;
const PARTITION_MASK: u64 = (NUM_PARTITIONS as u64) - 1;

/// Cheap sufficient (not necessary) equality check for two rows in the same
/// typed column.  This is used only to reuse the already-resolved bucket for
/// adjacent equal GROUP BY keys.  Returning `false` merely falls back to the
/// canonical collation-aware encoder, so unfamiliar/generic values remain on
/// the semantic oracle path.
#[inline]
fn column_rows_physically_equal(column: &ColumnVector, left: usize, right: usize) -> bool {
    let left_null = column.is_null(left);
    let right_null = column.is_null(right);
    if left_null || right_null {
        return left_null && right_null;
    }

    let byte_range_equal = |offsets: &[u32], data: &[u8]| {
        data[offsets[left] as usize..offsets[left + 1] as usize]
            == data[offsets[right] as usize..offsets[right + 1] as usize]
    };
    match column {
        ColumnVector::Null { .. } => true,
        ColumnVector::Bool { values, .. } => values.get(left) == values.get(right),
        ColumnVector::I64 { data, .. }
        | ColumnVector::Time64 { data, .. }
        | ColumnVector::DateTime64 { data, .. }
        | ColumnVector::Timestamp64 { data, .. } => data[left] == data[right],
        ColumnVector::U64 { data, .. } => data[left] == data[right],
        ColumnVector::F64 { data, .. } => data[left].to_bits() == data[right].to_bits(),
        ColumnVector::Date32 { data, .. } | ColumnVector::Decimal32 { data, .. } => {
            data[left] == data[right]
        }
        ColumnVector::Decimal64 { data, .. } => data[left] == data[right],
        ColumnVector::Decimal128 { data, .. } => data[left] == data[right],
        ColumnVector::DecimalWide { data, .. } => data[left] == data[right],
        ColumnVector::Utf8 { offsets, data, .. }
        | ColumnVector::Bytes { offsets, data, .. }
        | ColumnVector::Json { offsets, data, .. }
        | ColumnVector::Geometry { offsets, data, .. }
        | ColumnVector::Vector { offsets, data, .. } => byte_range_equal(offsets, data),
        ColumnVector::Generic { .. } => false,
    }
}

/// One spill owner for a physical parallel HashAggregate. Worker shards append
/// to the same radix-partition files under per-partition locks; the coordinator
/// replays each file once after workers finish. File cardinality is therefore
/// bounded by `NUM_PARTITIONS`, independent of worker and batch counts.
pub(crate) struct SharedHashAggSpill {
    root: PathBuf,
    dir: Mutex<Option<Arc<SpillTmpDir>>>,
    partitions: Vec<Mutex<SharedHashAggPartition>>,
    dictionary_partitions: Vec<Mutex<SharedHashAggPartition>>,
    schema: Arc<BatchSchema>,
    dictionary_schema: Arc<BatchSchema>,
    group_count: usize,
    aggs: Vec<AggregateSpec>,
}

#[derive(Default)]
struct SharedHashAggPartition {
    written: bool,
    writer: Option<crate::engine::executor::spill_quota::TrackedSpillWriter>,
    replay_bytes: usize,
}

/// Resolve comparison domains before specs cross a worker or spill boundary.
/// Replay/finalize must not depend on thread-local collation state.
pub(crate) fn bind_aggregate_comparison_domains(
    aggs: &mut [AggregateSpec],
    schema: &BatchSchema,
) -> bool {
    let mut changed = false;
    for spec in aggs {
        if matches!(spec.func, AggregateKind::Min | AggregateKind::Max) {
            let collation = schema
                .fields()
                .get(spec.input_column)
                .and_then(|field| field.source.collation_id);
            changed |= spec.comparison_collation != collation;
            spec.comparison_collation = collation;
        }
    }
    changed
}

impl SharedHashAggSpill {
    pub(crate) fn new(
        root: PathBuf,
        input_schema: Arc<BatchSchema>,
        group_columns: &[usize],
        mut aggs: Vec<AggregateSpec>,
    ) -> Self {
        bind_aggregate_comparison_domains(&mut aggs, &input_schema);
        let schema = Arc::new(build_spill_schema(&input_schema, group_columns, &aggs));
        let mut dictionary_fields = Vec::with_capacity(schema.fields().len() + 1);
        dictionary_fields.push(BatchField {
            table: "__hashagg_spill".into(),
            alias: None,
            name: "canonical_key".into(),
            column_type: ColumnType::Blob,
            source: crate::types::ColumnSource {
                collation_id: Some(crate::charset::BINARY_COLLATION_ID),
                ..Default::default()
            },
        });
        dictionary_fields.extend(schema.fields().iter().cloned());
        Self {
            root,
            dir: Mutex::new(None),
            partitions: (0..NUM_PARTITIONS)
                .map(|_| Mutex::new(SharedHashAggPartition::default()))
                .collect(),
            dictionary_partitions: (0..NUM_PARTITIONS)
                .map(|_| Mutex::new(SharedHashAggPartition::default()))
                .collect(),
            schema,
            dictionary_schema: Arc::new(BatchSchema::new(dictionary_fields)),
            group_count: group_columns.len(),
            aggs,
        }
    }

    fn directory(&self) -> Result<Arc<SpillTmpDir>, QueryError> {
        let mut owner = self.dir.lock().map_err(|_| {
            crate::storage::error(
                crate::error::catalog::ER_GET_ERRNO,
                "shared hash-aggregate spill lock poisoned",
            )
        })?;
        if owner.is_none() {
            *owner = Some(Arc::new(SpillTmpDir::create_kind(&self.root, "hashagg")?));
        }
        Ok(Arc::clone(
            owner.as_ref().expect("spill directory initialized"),
        ))
    }

    fn append(&self, pidx: usize, batch: &Batch) -> Result<(u64, bool), QueryError> {
        self.append_kind(&self.partitions, "p", pidx, batch)
    }

    fn append_dictionary(&self, pidx: usize, batch: &Batch) -> Result<(u64, bool), QueryError> {
        self.append_kind(&self.dictionary_partitions, "d", pidx, batch)
    }

    fn append_kind(
        &self,
        partitions: &[Mutex<SharedHashAggPartition>],
        prefix: &str,
        pidx: usize,
        batch: &Batch,
    ) -> Result<(u64, bool), QueryError> {
        let dir = self.directory()?;
        let mut partition = partitions[pidx].lock().map_err(|_| {
            crate::storage::error(
                crate::error::catalog::ER_GET_ERRNO,
                "shared hash-aggregate partition lock poisoned",
            )
        })?;
        let first = !partition.written;
        if partition.writer.is_none() {
            let path = dir.path().join(format!("{prefix}{pidx:03}.bin"));
            // Persistent writers bound file opens by the radix fanout. Divide
            // the shared spill-buffer pool across that same bound so retained
            // capacity remains fully query-governed.
            let capacity =
                crate::engine::executor::spill_quota::writer_buffer_capacity(NUM_PARTITIONS);
            partition.writer = Some(if first {
                crate::engine::executor::spill_quota::TrackedSpillWriter::create_truncate(
                    &path,
                    dir.charger(),
                    capacity,
                )?
            } else {
                crate::engine::executor::spill_quota::TrackedSpillWriter::open_append(
                    &path,
                    dir.charger(),
                    capacity,
                )?
            });
        }
        let writer = partition
            .writer
            .as_mut()
            .expect("shared spill writer initialized");
        let before = writer.logical_bytes_written();
        spill::write_batch(writer, batch)?;
        let bytes = writer.logical_bytes_written().saturating_sub(before);
        partition.written = true;
        // Replay expands typed lanes into keys, aggregate state, snapshots
        // and finalized Value rows. File size alone misses those containers
        // (especially compressed numeric lanes). Count every partial as a
        // distinct group: duplicate epochs can only reduce the merge set.
        let per_group = std::mem::size_of::<GroupBucket>()
            .saturating_add(std::mem::size_of::<GroupBucketSnapshot>())
            .saturating_add(self.aggs.len().saturating_mul(
                std::mem::size_of::<AggState>() + std::mem::size_of::<AggStateSnapshot>(),
            ))
            .saturating_add(
                self.group_count
                    .saturating_add(self.aggs.len())
                    .saturating_mul(2 * std::mem::size_of::<Value>()),
            )
            .saturating_add(2 * (std::mem::size_of::<Vec<u8>>() + HASH_ENTRY_OVERHEAD));
        partition.replay_bytes = partition
            .replay_bytes
            .saturating_add(batch.memory_bytes().saturating_mul(4))
            .saturating_add(batch.visible_rows().saturating_mul(per_group));
        Ok((bytes, first))
    }

    /// Scheduling estimate for the largest partition, including generic and
    /// dictionary epochs. It limits parallel replay; hard allocation admission
    /// remains responsible for concurrent queries and unexpected pressure.
    pub(crate) fn max_partition_replay_bytes(&self) -> usize {
        self.partitions
            .iter()
            .zip(&self.dictionary_partitions)
            .map(|(generic, dictionary)| {
                let bytes = |partition: &Mutex<SharedHashAggPartition>| {
                    partition
                        .lock()
                        .map_or(usize::MAX, |partition| partition.replay_bytes)
                };
                bytes(generic).saturating_add(bytes(dictionary))
            })
            .max()
            .unwrap_or(0)
    }

    pub(crate) fn partition_snapshots(&self) -> Result<Vec<Option<PartitionPartial>>, QueryError> {
        let mut partials = Vec::with_capacity(NUM_PARTITIONS);
        for pidx in 0..NUM_PARTITIONS {
            partials.push(self.partition_snapshot(pidx)?);
        }
        Ok(partials)
    }

    pub(crate) fn partition_snapshot(
        &self,
        pidx: usize,
    ) -> Result<Option<PartitionPartial>, QueryError> {
        let directory = self
            .dir
            .lock()
            .map_err(|_| {
                crate::storage::error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "shared hash-aggregate spill lock poisoned",
                )
            })?
            .clone();
        let Some(dir) = directory else {
            return Ok(None);
        };
        let mut partition = self.partitions[pidx].lock().map_err(|_| {
            crate::storage::error(
                crate::error::catalog::ER_GET_ERRNO,
                "shared hash-aggregate partition lock poisoned",
            )
        })?;
        let mut dictionary_partition = self.dictionary_partitions[pidx].lock().map_err(|_| {
            crate::storage::error(
                crate::error::catalog::ER_GET_ERRNO,
                "shared hash-aggregate dictionary partition lock poisoned",
            )
        })?;
        if !partition.written && !dictionary_partition.written {
            return Ok(None);
        }
        // Replay begins only after all aggregate workers have finished. Flush
        // and close exactly once here, making every preceding append a pure
        // buffered write instead of an open/flush/close cycle.
        if let Some(writer) = partition.writer.take() {
            let _ = writer.finish()?;
        }
        if let Some(writer) = dictionary_partition.writer.take() {
            let _ = writer.finish()?;
        }
        let generic_written = partition.written;
        let dictionary_written = dictionary_partition.written;
        drop(partition);
        drop(dictionary_partition);
        let path = dir.path().join(format!("p{pidx:03}.bin"));
        if generic_written && !dictionary_written {
            if let Some(partial) = merge_fixed_width_spill_path(
                &path,
                Arc::clone(&self.schema),
                &self.aggs,
                dir.charger(),
            )? {
                return Ok(Some(partial));
            }
        }
        let mut buckets = if generic_written {
            merge_partition(
                Partition::default(),
                &[path],
                Arc::clone(&self.schema),
                &self.aggs,
                dir.charger(),
            )?
        } else {
            Vec::new()
        };
        if dictionary_written {
            merge_dictionary_count_spill_path(
                &mut buckets,
                &dir.path().join(format!("d{pidx:03}.bin")),
                Arc::clone(&self.dictionary_schema),
                self.group_count,
                &self.aggs,
                dir.charger(),
            )?;
        }
        Ok((!buckets.is_empty()).then(|| partition_partial_from_buckets(buckets)))
    }

    pub(crate) fn written_partition_count(&self) -> usize {
        self.partitions
            .iter()
            .zip(&self.dictionary_partitions)
            .filter(|(partition, dictionary)| {
                partition.lock().map(|state| state.written).unwrap_or(true)
                    || dictionary.lock().map(|state| state.written).unwrap_or(true)
            })
            .count()
    }

    /// Merge COUNT spill and resident tails once, retaining only a counter
    /// and representative per canonical key. The generic replay builds full
    /// AggregateState objects, snapshots them, then hashes every group again.
    /// The enclosing Top-K needs none of those intermediate representations.
    pub(crate) fn try_partition_count_topk(
        &self,
        pidx: usize,
        partials: &mut Vec<Vec<Option<PartitionPartial>>>,
        hint: AggregateCountTopKHint,
    ) -> Result<Option<Vec<Vec<Value>>>, QueryError> {
        if hint.k == 0
            || hint.k > 4096
            || !hint.descending
            || hint.aggregate_index != 0
            || !matches!(
                self.aggs.as_slice(),
                [AggregateSpec {
                    func: AggregateKind::CountStar | AggregateKind::Count,
                    ..
                }]
            )
            || partials.iter().flatten().flatten().any(|part| {
                part.dictionary_count
                    .as_ref()
                    .is_none_or(|part| part.n_aggs != 1)
            })
        {
            return Ok(None);
        }
        let dir = self
            .dir
            .lock()
            .map_err(|_| {
                crate::error::catalog::ER_GET_ERRNO
                    .error("shared hash-aggregate spill lock poisoned")
            })?
            .clone();
        let Some(dir) = dir else { return Ok(None) };
        let generic = self.partitions[pidx].lock().map_err(|_| {
            crate::error::catalog::ER_GET_ERRNO
                .error("shared hash-aggregate partition lock poisoned")
        })?;
        let mut dictionary = self.dictionary_partitions[pidx].lock().map_err(|_| {
            crate::error::catalog::ER_GET_ERRNO
                .error("shared hash-aggregate dictionary partition lock poisoned")
        })?;
        if generic.written || !dictionary.written {
            return Ok(None);
        }
        if let Some(writer) = dictionary.writer.take() {
            let _ = writer.finish()?;
        }
        drop(generic);
        drop(dictionary);

        let mut merged = CompactCountMerge::new();
        let topk_bytes = hint.k.saturating_mul(
            256usize.saturating_add(
                self.group_count
                    .saturating_mul(std::mem::size_of::<Value>()),
            ),
        );
        if !merged.reserve(0, topk_bytes) {
            return Ok(None);
        }
        let mut key = Vec::new();
        for partial in partials.iter().flatten().flatten() {
            let partial = partial.dictionary_count.as_ref().expect("shape checked");
            let encoder = &partial.encoder;
            if !merged.reserve(partial.partition.keys.len(), 0) {
                return Ok(None);
            }
            for (group, &local) in partial.partition.keys.iter().enumerate() {
                if group % DEFAULT_BATCH_ROWS == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                let key_bytes = encoder.columns.iter().fold(0usize, |bytes, column| {
                    let component = if let DictionaryColumnSource::Utf8 { canonical_keys, .. } =
                        &column.source
                    {
                        let token =
                            ((local >> column.shift) & u128::from(column.token_mask)) as usize;
                        if token == 0 {
                            1
                        } else {
                            canonical_keys[token - 1].len()
                        }
                    } else {
                        17
                    };
                    bytes.saturating_add(component)
                });
                if key_bytes > key.capacity() {
                    if !merged.reserve(0, key_bytes)
                        || key
                            .try_reserve_exact(key_bytes.saturating_sub(key.len()))
                            .is_err()
                    {
                        return Ok(None);
                    }
                }
                encoder.write_direct_canonical_key(local, &mut key);
                if let Some(entry) = merged.groups.get_mut(key.as_slice()) {
                    entry.count = entry
                        .count
                        .saturating_add(partial.partition.count(group, 0, 1));
                    continue;
                }
                let mut bytes = key.len() + encoder.columns.len() * std::mem::size_of::<Value>();
                for column in &encoder.columns {
                    if let DictionaryColumnSource::Utf8 { raw_values, .. } = &column.source {
                        let token =
                            ((local >> column.shift) & u128::from(column.token_mask)) as usize;
                        if token != 0 {
                            bytes = bytes.saturating_add(raw_values[token - 1].len());
                        }
                    }
                }
                if !merged.reserve(0, bytes) {
                    return Ok(None);
                }
                let ordinal = merged.groups.len();
                merged.groups.insert(
                    CompactCountKey::from_slice(&key),
                    CompactCountRow {
                        count: partial.partition.count(group, 0, 1),
                        values: encoder.direct_key_values(local, &[]),
                        ordinal,
                    },
                );
            }
        }
        let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(1);
        let mut reader = crate::engine::executor::spill_quota::TrackedSpillReader::open(
            &dir.path().join(format!("d{pidx:03}.bin")),
            dir.charger(),
            capacity,
        )?;
        while let Some(batch) =
            spill::read_owned_batch(&mut reader, self.dictionary_schema.clone())?
        {
            crate::engine::cancel::check_cancelled()?;
            let columns = batch.columns();
            let ColumnVector::Bytes { offsets, data, .. } = &columns[0] else {
                // Value-tagged spill blocks can retain a Generic byte lane.
                // Leave those on the existing decoder/merge oracle.
                return Ok(None);
            };
            // Lane capacities cover canonical/representative payload copies;
            // the per-row term covers the retained Value-vector capacities.
            let payload = columns
                .iter()
                .map(ColumnVector::memory_bytes)
                .sum::<usize>()
                .saturating_add(
                    batch
                        .num_rows()
                        .saturating_mul(self.group_count * std::mem::size_of::<Value>()),
                );
            if !merged.reserve(batch.num_rows(), payload) {
                return Ok(None);
            }
            for row in 0..batch.num_rows() {
                let key = &data[offsets[row] as usize..offsets[row + 1] as usize];
                let count = match columns[1 + self.group_count].value_at(row) {
                    Value::Int(count) => count.max(0) as u64,
                    _ => {
                        return Err(crate::error::catalog::ER_INTERNAL_ERROR
                            .error("dictionary COUNT spill lost its count"))
                    }
                };
                if let Some(entry) = merged.groups.get_mut(key) {
                    entry.count = entry.count.saturating_add(count);
                } else {
                    let ordinal = merged.groups.len();
                    merged.groups.insert(
                        CompactCountKey::from_slice(key),
                        CompactCountRow {
                            count,
                            values: columns[1..1 + self.group_count]
                                .iter()
                                .map(|column| column.value_at(row))
                                .collect(),
                            ordinal,
                        },
                    );
                }
            }
        }
        use std::cmp::Reverse;
        let mut best = std::collections::BinaryHeap::with_capacity(hint.k);
        for entry in merged.groups.values() {
            let candidate = (entry.count.min(i64::MAX as u64), entry.ordinal);
            if best.len() < hint.k {
                best.push(Reverse(candidate));
            } else if best.peek().is_some_and(|worst| candidate > worst.0) {
                best.pop();
                best.push(Reverse(candidate));
            }
        }
        let selected: hashbrown::HashSet<usize, ahash::RandomState> = best
            .into_iter()
            .map(|Reverse((_, ordinal))| ordinal)
            .collect();
        let rows = merged
            .groups
            .into_values()
            .filter_map(|mut entry| {
                if !selected.contains(&entry.ordinal) {
                    return None;
                }
                entry
                    .values
                    .push(Value::Int(entry.count.min(i64::MAX as u64) as i64));
                Some(entry.values)
            })
            .collect();
        partials.clear();
        Ok(Some(rows))
    }
}

struct CompactCountRow {
    count: u64,
    values: Vec<Value>,
    ordinal: usize,
}

/// Select an exact u128 packing from observed primitive domains. The
/// result is an internal representation; canonical partitioning is unchanged.
pub(crate) fn preferred_dictionary_layout(
    original: &[(u8, usize, u32, u32, u32)],
    domains: &[usize],
) -> smallvec::SmallVec<[(u8, usize, u32, u32, u32); 8]> {
    let mut layout = original
        .iter()
        .copied()
        .collect::<smallvec::SmallVec<[_; 8]>>();
    let mut best = None;
    for mask in 0usize..(1usize << layout.len()) {
        if layout.iter().enumerate().any(|(column, desc)| {
            let tag = desc.0 & 0x7f;
            (tag == 0 && mask & (1 << column) != 0)
                || (tag == 4 && (mask & (1 << column) != 0) != (desc.0 & 0x80 != 0))
        }) {
            continue;
        }
        let direct_bits = layout
            .iter()
            .enumerate()
            .filter(|(column, _)| mask & (1 << column) != 0)
            .map(|(_, desc)| match desc.0 & 0x7f {
                3 => 33,
                4 => 2,
                _ => 65,
            })
            .sum::<u32>();
        let tokens = layout.len() - mask.count_ones() as usize;
        let Some(available) = 128u32.checked_sub(direct_bits) else {
            continue;
        };
        let bits = if tokens == 0 {
            32
        } else {
            (available / tokens as u32).min(32)
        };
        if bits < 8 {
            continue;
        }
        let score = domains
            .iter()
            .enumerate()
            .filter(|(column, _)| mask & (1 << column) != 0)
            .map(|(_, domain)| *domain)
            .sum::<usize>();
        let rank = (score, mask.count_ones(), bits, usize::MAX - mask);
        if best.is_none_or(|prior| rank > prior) {
            best = Some(rank);
        }
    }
    if let Some((_, _, bits, inverse_mask)) = best {
        let mask = usize::MAX - inverse_mask;
        let token_mask = if bits == 32 {
            u32::MAX
        } else {
            (1u32 << bits) - 1
        };
        let mut shift = 0;
        for (column, desc) in layout.iter_mut().enumerate() {
            let tag = desc.0 & 0x7f;
            let direct = mask & (1 << column) != 0;
            let width = match tag {
                3 => 32,
                4 => 1,
                _ => 64,
            };
            desc.0 = tag | if direct { 0x80 } else { 0 };
            desc.2 = shift;
            desc.3 = if direct { shift + width } else { 0 };
            desc.4 = token_mask;
            shift += if direct { width + 1 } else { bits };
        }
    }
    layout
}

type CompactCountKey = smallvec::SmallVec<[u8; 48]>;

struct CompactCountMerge {
    groups: hashbrown::HashMap<CompactCountKey, CompactCountRow, ahash::RandomState>,
    charge: crate::engine::executor::mem_budget::ScopedCharge,
    payload_bytes: usize,
}

impl CompactCountMerge {
    fn new() -> Self {
        Self {
            groups: hashbrown::HashMap::default(),
            charge: crate::engine::executor::mem_budget::ScopedCharge::new(0),
            payload_bytes: 0,
        }
    }

    fn reserve(&mut self, rows: usize, payload: usize) -> bool {
        let wanted = self
            .groups
            .len()
            .saturating_add(rows)
            .max(self.groups.capacity())
            .max(1);
        let Some(slots) = wanted
            .checked_next_power_of_two()
            .and_then(|n| n.checked_mul(2))
        else {
            return false;
        };
        let payload = self.payload_bytes.saturating_add(payload);
        let bytes = slots
            .saturating_mul(std::mem::size_of::<(CompactCountKey, CompactCountRow)>() + 1)
            .saturating_add(payload);
        let extra = bytes.saturating_sub(self.charge.charged());
        // Optional merge layout: refusal drops this attempt and replays the
        // same partition through the established spill path. Reserve before
        // growth, using a raw-table capacity envelope rather than its length.
        if extra != 0
            && self
                .charge
                .add(extra.saturating_add(65535) / 65536 * 65536)
                .is_err()
        {
            return false;
        }
        self.payload_bytes = payload;
        self.groups.try_reserve(rows).is_ok()
    }
}

// Hash-table control/slot overhead is allocator-specific; use a conservative
// fixed allowance for it while all owned container payloads use real capacity.
const HASH_ENTRY_OVERHEAD: usize = 64;
const BUCKET_FIXED_OVERHEAD: usize = std::mem::size_of::<GroupBucket>();

/// Recursion ceiling for spill-during-merge. Each finalize pass re-reads
/// spilled segments into memory; if the merge itself overflows the budget
/// we error out rather than thrashing.
const SPILL_DEPTH_LIMIT: usize = 4;

/// Backward-compatible local spelling; the semantic capability table lives in
/// `select::aggregate` and is shared by every physical aggregate layout.
pub use crate::engine::executor::hash_aggregate::AggregateSpec;
pub use crate::engine::executor::select::aggregate::AggregateKind;

/// A comparison-only HAVING predicate that can be evaluated while finalized
/// aggregate state is still compact. More complex predicates stay in the
/// ordinary post-aggregate expression program.
#[derive(Debug, Clone)]
pub struct AggregateFinalFilter {
    pub aggregate_index: usize,
    pub comparison: AggregateFinalComparison,
    pub literal: Value,
    pub decimal_literal: Option<(i128, u8)>,
}

/// Safe partition-local Top-K reduction for a finalized COUNT slot.  Every
/// global Top-K row must be present in its radix partition's local Top-K, so
/// the parallel aggregate can avoid materializing millions of losing group
/// rows while the ordinary downstream TopN remains the final ordering oracle.
#[derive(Debug, Clone, Copy)]
pub struct AggregateCountTopKHint {
    pub aggregate_index: usize,
    pub k: usize,
    pub descending: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum AggregateFinalComparison {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl AggregateFinalFilter {
    fn keeps_decimal_parts(&self, magnitude: i128, scale: u8) -> Result<bool, QueryError> {
        let Some((literal, literal_scale)) = self.decimal_literal else {
            let value = if let Ok(value) = i64::try_from(magnitude) {
                Value::DecimalCompact(value, scale)
            } else {
                Value::Decimal128(magnitude, scale)
            };
            return self.keeps(self.aggregate_index, &value);
        };
        let common_scale = scale.max(literal_scale);
        let Some(left) = crate::util::decimal::rescale_round_i128(magnitude, scale, common_scale)
        else {
            return self.keeps(self.aggregate_index, &Value::Decimal128(magnitude, scale));
        };
        let Some(right) =
            crate::util::decimal::rescale_round_i128(literal, literal_scale, common_scale)
        else {
            return self.keeps(self.aggregate_index, &Value::Decimal128(magnitude, scale));
        };
        let ordering = left.cmp(&right);
        Ok(match self.comparison {
            AggregateFinalComparison::Eq => ordering == std::cmp::Ordering::Equal,
            AggregateFinalComparison::NotEq => ordering != std::cmp::Ordering::Equal,
            AggregateFinalComparison::Lt => ordering == std::cmp::Ordering::Less,
            AggregateFinalComparison::LtEq => ordering != std::cmp::Ordering::Greater,
            AggregateFinalComparison::Gt => ordering == std::cmp::Ordering::Greater,
            AggregateFinalComparison::GtEq => ordering != std::cmp::Ordering::Less,
        })
    }
    fn keeps(&self, aggregate_index: usize, value: &Value) -> Result<bool, QueryError> {
        if aggregate_index != self.aggregate_index || matches!(value, Value::Null) {
            return Ok(aggregate_index != self.aggregate_index);
        }
        let ordering = crate::engine::expr::compare_values(value, &self.literal)?;
        Ok(match self.comparison {
            AggregateFinalComparison::Eq => ordering == std::cmp::Ordering::Equal,
            AggregateFinalComparison::NotEq => ordering != std::cmp::Ordering::Equal,
            AggregateFinalComparison::Lt => ordering == std::cmp::Ordering::Less,
            AggregateFinalComparison::LtEq => ordering != std::cmp::Ordering::Greater,
            AggregateFinalComparison::Gt => ordering == std::cmp::Ordering::Greater,
            AggregateFinalComparison::GtEq => ordering != std::cmp::Ordering::Less,
        })
    }
}

/// Physical source of one aggregate input in an adjacent HashJoin.  This is
/// deliberately column-oriented and function-agnostic: `AggregateSpec::func` owns
/// NULL/DISTINCT/numeric semantics, while the join only supplies the value's
/// location (or a NULL extension for an unmatched outer row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectJoinColumn {
    Build(usize),
    Probe(usize),
}

// ---------------------------------------------------------------------
// B+2: typed packed group-key fast path
// ---------------------------------------------------------------------
//
// `Packed64Encoder` describes how to pack every GROUP BY column at one
// row into a single u64. The hot path in `consume_batch_packed64` does
// `for r in visible: key = pack(batch, r); lookup(key); …` — one i64-
// sized read per column instead of the Vec<u8> tag-and-length encoder
// + fxhash + byte-slice probe the generic path runs. This benefits every
// bounded fixed-width composite key and is independent of a SQL workload.
//
// Layout of the packed key (LSB → MSB):
//   bits[0..56]  — column data, packed at each col's `shift`
//   bits[56..64] — NULL mask, one bit per column at `null_bit_shift`
//
// Detection is one-shot on the first batch (any group column that
// can't be packed — wide composite, mixed-scale Decimal128, variable-
// width Utf8 — leaves `packed64_encoder` at `None` for the rest of
// the operator's lifetime, and we run the byte path forever).
// The operator-wide packed lookup points at canonical radix buckets. Thus
// repeated rows avoid byte encoding while snapshot/merge/spill see the same
// partition ownership regardless of the fast layout a worker selected.

#[derive(Debug, Clone)]
enum Packed64Encoder {
    /// The frequent categorical GROUP BY shape: two single-byte UTF-8
    /// columns. Keep it separate so the per-row loop does not repeatedly
    /// iterate a Vec and dispatch on two lane tags that are invariant for the
    /// operator lifetime.
    Utf8OnePair([Packed64Col; 2]),
    Generic(Vec<Packed64Col>),
}

/// Dictionary-packed composite GROUP BY keys. Variable-width strings are
/// interned by their canonical collation bytes, while repeated raw spellings
/// hit a borrowed-byte cache and avoid rebuilding the collation key. Primitive
/// lanes retain their raw fixed-width representation instead of allocating a
/// second high-cardinality dictionary. The complete lossless layout, including
/// one NULL bit per column, must fit in the existing u128 packed lookup.
struct DictionaryGroupEncoder {
    columns: Vec<DictionaryColumn>,
    #[cfg(test)]
    direct_primitives: bool,
    compact_count_eligible: bool,
    current_raw_tokens: smallvec::SmallVec<[u32; 4]>,
    merge_token_bytes: usize,
    // Follows the encoder when partition handoff freezes it in an Arc.
    _merge_token_charge: Option<crate::engine::executor::mem_budget::OwnedMemCharge>,
}

#[derive(Clone, Copy, Default)]
struct DictionaryToken {
    local: u32,
    raw_local: u32,
    stable_hash: u64,
}

#[derive(Clone, Copy, Default)]
struct RawTokenCacheEntry {
    hash: u64,
    token: DictionaryToken,
}

struct RawTokenCache {
    entries: Vec<RawTokenCacheEntry>,
    populated: usize,
    empty_token: Option<DictionaryToken>,
    _charge: Option<crate::engine::executor::mem_budget::OwnedMemCharge>,
}

impl RawTokenCache {
    // Optional acceleration must not consume the memory needed by hash
    // state and spill replay. Bound each cache to 1/1024 of the query quota,
    // with a small unbudgeted default. Admission happens before touching the
    // allocation; a refusal simply uses the canonical dictionary lookup.
    const CAPACITY: usize = 1 << 16;
    const WAYS: usize = 4;

    fn new() -> Self {
        let budget = crate::engine::executor::mem_budget::current_budget();
        let entry_bytes = std::mem::size_of::<RawTokenCacheEntry>();
        let desired = budget
            .as_ref()
            .map_or(4096, |b| b.limit() / 1024 / entry_bytes)
            .min(Self::CAPACITY);
        let capacity = if desired < Self::WAYS {
            0
        } else {
            1usize << (usize::BITS - 1 - desired.leading_zeros())
        };
        let mut cache = Self {
            entries: Vec::new(),
            populated: 0,
            empty_token: None,
            _charge: None,
        };
        if capacity == 0 {
            return cache;
        }
        if let Some(budget) = budget {
            let Ok(charge) = crate::engine::executor::mem_budget::OwnedMemCharge::new(
                budget,
                capacity * entry_bytes,
            ) else {
                return cache;
            };
            cache._charge = Some(charge);
        }
        if cache.entries.try_reserve_exact(capacity).is_err() {
            cache._charge = None;
            return cache;
        }
        cache
            .entries
            .resize(capacity, RawTokenCacheEntry::default());
        cache
    }

    fn set_mask(&self) -> usize {
        self.entries.len() / Self::WAYS - 1
    }

    #[inline(always)]
    fn get(&self, raw: &[u8], raw_values: &[Box<str>]) -> Option<DictionaryToken> {
        let hash = super::key_encode::fxhash(raw);
        self.get_hashed(raw, hash, raw_values)
    }

    #[inline(always)]
    fn get_hashed(
        &self,
        raw: &[u8],
        hash: u64,
        raw_values: &[Box<str>],
    ) -> Option<DictionaryToken> {
        if raw.is_empty() {
            return self.empty_token;
        }
        if self.entries.is_empty() {
            return None;
        }
        let base = ((hash as usize) & self.set_mask()) * Self::WAYS;
        for entry in &self.entries[base..base + Self::WAYS] {
            if entry.token.local != 0
                && entry.hash == hash
                && raw_values
                    .get(entry.token.raw_local as usize - 1)
                    .is_some_and(|value| value.as_bytes() == raw)
            {
                return Some(entry.token);
            }
        }
        None
    }

    fn insert(&mut self, raw: &[u8], token: DictionaryToken) {
        let hash = super::key_encode::fxhash(raw);
        self.insert_hashed(hash, token, raw.is_empty());
    }

    fn insert_hashed(&mut self, hash: u64, token: DictionaryToken, is_empty: bool) {
        if is_empty {
            self.empty_token = Some(token);
            return;
        }
        if self.entries.is_empty() {
            return;
        }
        let base = ((hash as usize) & self.set_mask()) * Self::WAYS;
        let slot = self.entries[base..base + Self::WAYS]
            .iter()
            .position(|entry| entry.token.local == 0)
            .map(|way| base + way)
            .unwrap_or_else(|| base + ((hash >> 48) as usize & (Self::WAYS - 1)));
        if self.entries[slot].token.local == 0 {
            self.populated += 1;
        }
        self.entries[slot] = RawTokenCacheEntry { hash, token };
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.populated
    }

    fn allocated_bytes(&self) -> usize {
        // Already owned by the cache, including after encoder handoff.
        if self._charge.is_some() {
            return 0;
        }
        self.entries
            .capacity()
            .saturating_mul(std::mem::size_of::<RawTokenCacheEntry>())
    }
}

struct DictionaryColumn {
    source: DictionaryColumnSource,
    shift: u32,
    null_bit: u32,
    direct: bool,
    token_mask: u32,
}

struct DenseI64TokenCache {
    min: i64,
    slots: Vec<DictionaryToken>,
}

impl DenseI64TokenCache {
    const MAX_SPAN: usize = 4096;

    fn new() -> Self {
        Self {
            min: 0,
            slots: Vec::new(),
        }
    }

    #[inline]
    fn get(&self, value: i64) -> Option<DictionaryToken> {
        let offset = value.checked_sub(self.min)? as usize;
        self.slots
            .get(offset)
            .copied()
            .filter(|token| token.local != 0)
    }

    fn insert(&mut self, value: i64, token: DictionaryToken) -> bool {
        if self.slots.is_empty() {
            self.min = value;
            self.slots.push(token);
            return true;
        }
        let current_max = self.min.saturating_add(self.slots.len() as i64 - 1);
        let new_min = self.min.min(value);
        let new_max = current_max.max(value);
        let Some(span) = new_max
            .checked_sub(new_min)
            .and_then(|span| usize::try_from(span).ok())
            .and_then(|span| span.checked_add(1))
        else {
            return false;
        };
        if span > Self::MAX_SPAN {
            return false;
        }
        if new_min < self.min {
            let prefix = usize::try_from(self.min - new_min).unwrap();
            let mut slots = vec![DictionaryToken::default(); span];
            slots[prefix..prefix + self.slots.len()].copy_from_slice(&self.slots);
            self.slots = slots;
            self.min = new_min;
        } else if span > self.slots.len() {
            self.slots.resize(span, DictionaryToken::default());
        }
        self.slots[(value - self.min) as usize] = token;
        true
    }
}

enum DictionaryColumnSource {
    Utf8 {
        column: usize,
        raw_to_token: RawTokenCache,
        canonical_to_token: hashbrown::HashMap<Arc<[u8]>, DictionaryToken, ahash::RandomState>,
        canonical_keys: Vec<Arc<[u8]>>,
        merge_tokens: Vec<u32>,
        /// One compact representative raw spelling per canonical key. A
        /// `Box<str>` avoids paying the much wider general-purpose `Value`
        /// discriminant for every distinct string group.
        raw_values: Vec<Box<str>>,
        raw_value_heap_bytes: usize,
        canonical_key_capacity_bytes: usize,
    },
    I64 {
        column: usize,
        values: hashbrown::HashMap<i64, DictionaryToken, ahash::RandomState>,
        dense_tokens: Option<DenseI64TokenCache>,
        raw_values: Vec<i64>,
        merge_tokens: Vec<u32>,
    },
    U64 {
        column: usize,
        values: hashbrown::HashMap<u64, DictionaryToken, ahash::RandomState>,
        raw_values: Vec<u64>,
        merge_tokens: Vec<u32>,
    },
    Date32 {
        column: usize,
        values: hashbrown::HashMap<i32, DictionaryToken, ahash::RandomState>,
        raw_values: Vec<i32>,
        merge_tokens: Vec<u32>,
    },
    Bool {
        column: usize,
    },
}

fn dictionary_primitive_sample_distinct(source: &DictionaryColumnSource, batch: &Batch) -> usize {
    const SAMPLE_ROWS: usize = 2048;
    macro_rules! count_distinct {
        ($column:expr, $data:expr) => {{
            let vector = &batch.columns()[$column];
            let mut values = hashbrown::HashSet::<_, ahash::RandomState>::with_capacity_and_hasher(
                SAMPLE_ROWS.min(batch.visible_rows()),
                ahash::RandomState::new(),
            );
            for row in batch.visible_indices().take(SAMPLE_ROWS) {
                if !vector.is_null(row) {
                    values.insert($data[row]);
                }
            }
            values.len()
        }};
    }
    match source {
        DictionaryColumnSource::Utf8 { .. } => 0,
        DictionaryColumnSource::I64 { column, .. } => {
            let ColumnVector::I64 { data, .. } = &batch.columns()[*column] else {
                return usize::MAX;
            };
            count_distinct!(*column, data)
        }
        DictionaryColumnSource::U64 { column, .. } => {
            let ColumnVector::U64 { data, .. } = &batch.columns()[*column] else {
                return usize::MAX;
            };
            count_distinct!(*column, data)
        }
        DictionaryColumnSource::Date32 { column, .. } => {
            let ColumnVector::Date32 { data, .. } = &batch.columns()[*column] else {
                return usize::MAX;
            };
            count_distinct!(*column, data)
        }
        DictionaryColumnSource::Bool { column } => {
            let ColumnVector::Bool { values, .. } = &batch.columns()[*column] else {
                return usize::MAX;
            };
            let vector = &batch.columns()[*column];
            let mut seen = 0u8;
            for row in batch.visible_indices().take(SAMPLE_ROWS) {
                if !vector.is_null(row) {
                    seen |= 1 << u8::from(values.get(row));
                }
            }
            seen.count_ones() as usize
        }
    }
}

impl DictionaryGroupEncoder {
    #[inline]
    fn try_build(group_columns: &[usize], batch: &Batch) -> Option<Self> {
        if group_columns.is_empty() || group_columns.len() > 8 {
            return None;
        }
        let mut has_utf8 = false;
        let mut sources = Vec::with_capacity(group_columns.len());
        let mut widths = Vec::with_capacity(group_columns.len());
        for &column in group_columns {
            let (source, width) = match batch.columns().get(column)? {
                vector @ ColumnVector::Utf8 { .. } => {
                    if !utf8_dictionary_worthwhile(vector, batch) {
                        return None;
                    }
                    has_utf8 = true;
                    (
                        DictionaryColumnSource::Utf8 {
                            column,
                            raw_to_token: RawTokenCache::new(),
                            canonical_to_token: hashbrown::HashMap::with_hasher(
                                ahash::RandomState::new(),
                            ),
                            canonical_keys: Vec::new(),
                            merge_tokens: Vec::new(),
                            raw_values: Vec::new(),
                            raw_value_heap_bytes: 0,
                            canonical_key_capacity_bytes: 0,
                        },
                        32,
                    )
                }
                ColumnVector::I64 { .. } => (
                    DictionaryColumnSource::I64 {
                        column,
                        values: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
                        dense_tokens: Some(DenseI64TokenCache::new()),
                        raw_values: Vec::new(),
                        merge_tokens: Vec::new(),
                    },
                    64,
                ),
                ColumnVector::U64 { .. } => (
                    DictionaryColumnSource::U64 {
                        column,
                        values: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
                        raw_values: Vec::new(),
                        merge_tokens: Vec::new(),
                    },
                    64,
                ),
                ColumnVector::Date32 { .. } => (
                    DictionaryColumnSource::Date32 {
                        column,
                        values: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
                        raw_values: Vec::new(),
                        merge_tokens: Vec::new(),
                    },
                    32,
                ),
                ColumnVector::Bool { .. } => (DictionaryColumnSource::Bool { column }, 1),
                _ => return None,
            };
            sources.push(source);
            widths.push(width);
        }
        if !has_utf8 {
            return None;
        }
        let value_bits = widths
            .iter()
            .try_fold(0u32, |total, width| total.checked_add(*width))?;
        let null_bits = u32::try_from(group_columns.len()).ok()?;
        let direct_primitives = value_bits
            .checked_add(null_bits)
            .is_some_and(|bits| bits <= 128);
        // If every primitive lane does not fit, retain the highest-cardinality
        // primitive(s) losslessly and dictionary-code only the remaining
        // lanes.  This is especially important for the general
        // `(large integer, low-cardinality integer, text)` GROUP BY shape:
        // tokenising the large integer creates a second high-cardinality hash
        // table, while 64 raw bits plus two 31-bit tokens and one NULL bit fit
        // exactly in the existing u128 key.
        let primitive_distinct = sources
            .iter()
            .map(|source| dictionary_primitive_sample_distinct(source, batch))
            .collect::<Vec<_>>();
        let primitive_mask = sources
            .iter()
            .enumerate()
            .filter(|(_, source)| !matches!(source, DictionaryColumnSource::Utf8 { .. }))
            .fold(0usize, |mask, (index, _)| mask | (1usize << index));
        let (direct_mask, token_bits) = if direct_primitives {
            (primitive_mask, 32)
        } else {
            let mut best: Option<(usize, u32, u32, usize)> = None;
            for candidate in 0usize..(1usize << sources.len()) {
                if candidate & !primitive_mask != 0 {
                    continue;
                }
                let direct_bits = widths
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| candidate & (1usize << index) != 0)
                    .try_fold(0u32, |total, (_, width)| total.checked_add(*width + 1))?;
                let token_count = sources.len() - candidate.count_ones() as usize;
                let Some(available) = 128u32.checked_sub(direct_bits) else {
                    continue;
                };
                let bits = if token_count == 0 {
                    32
                } else {
                    (available / token_count as u32).min(32)
                };
                if bits < 8 {
                    continue;
                }
                let score = primitive_distinct
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| candidate & (1usize << index) != 0)
                    .map(|(_, distinct)| *distinct)
                    .sum::<usize>();
                let rank = (score, candidate.count_ones(), bits, usize::MAX - candidate);
                if best.is_none_or(|current| rank > current) {
                    best = Some(rank);
                }
            }
            let winning = best?;
            (usize::MAX - winning.3, winning.2)
        };
        let token_mask = if token_bits == 32 {
            u32::MAX
        } else {
            (1u32 << token_bits) - 1
        };
        let mut shift = 0u32;
        let columns = sources
            .into_iter()
            .zip(widths)
            .enumerate()
            .map(|(position, (source, width))| {
                let direct = direct_mask & (1usize << position) != 0;
                let column = DictionaryColumn {
                    source,
                    shift,
                    null_bit: if direct { shift + width } else { 0 },
                    direct,
                    token_mask,
                };
                shift += if direct { width + 1 } else { token_bits };
                column
            })
            .collect();
        let compact_count_eligible =
            primitive_distinct
                .iter()
                .enumerate()
                .all(|(index, distinct)| {
                    direct_mask & (1usize << index) != 0
                        || *distinct <= batch.visible_rows().min(2048).div_ceil(2)
                });
        Some(Self {
            columns,
            #[cfg(test)]
            direct_primitives,
            compact_count_eligible,
            current_raw_tokens: smallvec::SmallVec::new(),
            merge_token_bytes: 0,
            _merge_token_charge: None,
        })
    }

    fn encode_row(
        &mut self,
        batch: &Batch,
        row: usize,
        scratch: &mut Vec<u8>,
    ) -> Option<(u128, u64)> {
        self.encode_row_inner::<true, true>(batch, row, scratch)
    }

    /// COUNT-only single-string groups already carry the canonical hash in
    /// their dictionary token. On a raw-token cache hit they need neither a
    /// second copy of the canonical bytes nor a second full-key hash.
    fn encode_count_row(
        &mut self,
        batch: &Batch,
        row: usize,
        scratch: &mut Vec<u8>,
        retain_raw_tokens: bool,
    ) -> Option<(u128, u64)> {
        if retain_raw_tokens {
            self.encode_row_inner::<false, true>(batch, row, scratch)
        } else {
            self.encode_row_inner::<false, false>(batch, row, scratch)
        }
    }

    /// Monomorphic COUNT kernel for one UTF-8 group column. It avoids the
    /// generic composite-key loop, `SmallVec` token staging and enum dispatch
    /// that otherwise remain visible for every row even after the layout has
    /// been fixed at operator open.
    fn encode_single_utf8_count_row(
        &mut self,
        batch: &Batch,
        row: usize,
        scratch: &mut Vec<u8>,
        known_raw_hash: Option<u64>,
    ) -> Option<(u128, u64, u32)> {
        let [DictionaryColumn {
            source:
                DictionaryColumnSource::Utf8 {
                    column,
                    raw_to_token,
                    canonical_to_token,
                    canonical_keys,
                    raw_values,
                    raw_value_heap_bytes,
                    canonical_key_capacity_bytes,
                    ..
                },
            shift,
            ..
        }] = self.columns.as_mut_slice()
        else {
            return None;
        };
        let vector = batch.columns().get(*column)?;
        scratch.clear();
        if vector.is_null(row) {
            // UTF8 dictionary lanes reserve token zero for NULL; null_bit
            // is only meaningful for direct primitive lanes.
            return Some((0, super::key_encode::fxhash(&[0]), 0));
        }
        let ColumnVector::Utf8 { offsets, data, .. } = vector else {
            return None;
        };
        let raw = &data[offsets[row] as usize..offsets[row + 1] as usize];
        let raw_hash = known_raw_hash.unwrap_or_else(|| super::key_encode::fxhash(raw));
        let token = if let Some(token) = raw_to_token.get_hashed(raw, raw_hash, raw_values) {
            token
        } else {
            super::key_encode::encode_from_column_row_with_collation(
                vector,
                row,
                batch.schema().fields()[*column].source.collation_id,
                scratch,
            );
            let canonical = scratch.as_slice();
            let token = if let Some(&token) = canonical_to_token.get(canonical) {
                token
            } else {
                let local = u32::try_from(canonical_to_token.len())
                    .ok()?
                    .checked_add(1)?;
                let token = DictionaryToken {
                    local,
                    raw_local: local,
                    stable_hash: super::key_encode::fxhash(canonical),
                };
                let canonical_key: Arc<[u8]> = Arc::from(canonical);
                *canonical_key_capacity_bytes = canonical_key_capacity_bytes
                    .saturating_add(canonical_key.len().saturating_add(ARC_SLICE_HEADER_BYTES));
                canonical_to_token.insert(canonical_key.clone(), token);
                canonical_keys.push(canonical_key);
                let text = unsafe { std::str::from_utf8_unchecked(raw) };
                raw_values.push(Box::<str>::from(text));
                *raw_value_heap_bytes = raw_value_heap_bytes.saturating_add(text.len());
                token
            };
            raw_to_token.insert_hashed(raw_hash, token, raw.is_empty());
            token
        };
        Some((
            u128::from(token.local) << *shift,
            token.stable_hash,
            token.raw_local,
        ))
    }

    fn encode_row_inner<const MATERIALIZE: bool, const RETAIN_RAW: bool>(
        &mut self,
        batch: &Batch,
        row: usize,
        scratch: &mut Vec<u8>,
    ) -> Option<(u128, u64)> {
        const NULL_HASH: u64 = 0x9e37_79b9_7f4a_7c15;
        scratch.clear();
        if RETAIN_RAW {
            self.current_raw_tokens.clear();
        }
        let mut key = 0u128;
        let mut stable_hash = 0u64;
        let mut first_component_hash = 0u64;
        for (position, column) in self.columns.iter_mut().enumerate() {
            let direct = column.direct;
            let token_mask = column.token_mask;
            let (value_bits, component_hash, is_null) = match &mut column.source {
                DictionaryColumnSource::Utf8 {
                    column,
                    raw_to_token,
                    canonical_to_token,
                    canonical_keys,
                    raw_values,
                    raw_value_heap_bytes,
                    canonical_key_capacity_bytes,
                    ..
                } => {
                    let vector = batch.columns().get(*column)?;
                    if vector.is_null(row) {
                        if MATERIALIZE {
                            scratch.push(0);
                        }
                        if RETAIN_RAW {
                            self.current_raw_tokens.push(0);
                        }
                        (0, NULL_HASH, true)
                    } else {
                        let ColumnVector::Utf8 { offsets, data, .. } = vector else {
                            return None;
                        };
                        let raw = &data[offsets[row] as usize..offsets[row + 1] as usize];
                        let token = if let Some(token) = raw_to_token.get(raw, raw_values) {
                            if MATERIALIZE {
                                let canonical = canonical_keys.get(token.local as usize - 1)?;
                                scratch.extend_from_slice(canonical);
                            }
                            token
                        } else {
                            let canonical_start = scratch.len();
                            super::key_encode::encode_from_column_row_with_collation(
                                vector,
                                row,
                                batch.schema().fields()[*column].source.collation_id,
                                scratch,
                            );
                            let canonical = &scratch[canonical_start..];
                            let token = if let Some(&token) = canonical_to_token.get(canonical) {
                                token
                            } else {
                                let local = u32::try_from(canonical_to_token.len())
                                    .ok()?
                                    .checked_add(1)?;
                                let token = DictionaryToken {
                                    local,
                                    // Retain one representative spelling per
                                    // exact collation class, not one per raw
                                    // spelling. Equality remains byte-exact on
                                    // the canonical key.
                                    raw_local: local,
                                    stable_hash: super::key_encode::fxhash(canonical),
                                };
                                let canonical_key: Arc<[u8]> = Arc::from(canonical);
                                *canonical_key_capacity_bytes = (*canonical_key_capacity_bytes)
                                    .saturating_add(
                                        canonical_key.len().saturating_add(ARC_SLICE_HEADER_BYTES),
                                    );
                                canonical_to_token.insert(canonical_key.clone(), token);
                                canonical_keys.push(canonical_key);
                                // ColumnVector::Utf8 guarantees valid UTF-8;
                                // avoid constructing a temporary Value just
                                // to retain its String.
                                let text = unsafe { std::str::from_utf8_unchecked(raw) };
                                let raw_value = Box::<str>::from(text);
                                *raw_value_heap_bytes =
                                    raw_value_heap_bytes.saturating_add(text.len());
                                raw_values.push(raw_value);
                                token
                            };
                            raw_to_token.insert(raw, token);
                            token
                        };
                        if RETAIN_RAW {
                            self.current_raw_tokens.push(token.raw_local);
                        }
                        if token.local > token_mask {
                            return None;
                        }
                        (u128::from(token.local), token.stable_hash, false)
                    }
                }
                DictionaryColumnSource::I64 {
                    column,
                    values,
                    dense_tokens,
                    raw_values,
                    ..
                } => {
                    let vector = batch.columns().get(*column)?;
                    if MATERIALIZE {
                        super::key_encode::encode_from_column_row_with_collation(
                            vector,
                            row,
                            batch.schema().fields()[*column].source.collation_id,
                            scratch,
                        );
                    }
                    let ColumnVector::I64 { data, .. } = vector else {
                        return None;
                    };
                    let null = vector.is_null(row);
                    let raw = if null { 0 } else { data[row] as u64 as u128 };
                    let encoded = if direct || null {
                        raw
                    } else {
                        u128::from(
                            dictionary_i64_token(values, dense_tokens, raw_values, data[row], raw)?
                                .local,
                        )
                    };
                    if encoded > u128::from(token_mask) && !direct {
                        return None;
                    }
                    (
                        encoded,
                        if null { NULL_HASH } else { fx_mix_u128(raw) },
                        null,
                    )
                }
                DictionaryColumnSource::U64 {
                    column,
                    values,
                    raw_values,
                    ..
                } => {
                    let vector = batch.columns().get(*column)?;
                    if MATERIALIZE {
                        super::key_encode::encode_from_column_row_with_collation(
                            vector,
                            row,
                            batch.schema().fields()[*column].source.collation_id,
                            scratch,
                        );
                    }
                    let ColumnVector::U64 { data, .. } = vector else {
                        return None;
                    };
                    let null = vector.is_null(row);
                    let raw = if null { 0 } else { data[row] as u128 };
                    let encoded = if direct || null {
                        raw
                    } else {
                        u128::from(
                            dictionary_primitive_token(
                                values,
                                raw_values,
                                data[row],
                                fx_mix_u128(raw),
                            )?
                            .local,
                        )
                    };
                    if encoded > u128::from(token_mask) && !direct {
                        return None;
                    }
                    (
                        encoded,
                        if null { NULL_HASH } else { fx_mix_u128(raw) },
                        null,
                    )
                }
                DictionaryColumnSource::Date32 {
                    column,
                    values,
                    raw_values,
                    ..
                } => {
                    let vector = batch.columns().get(*column)?;
                    if MATERIALIZE {
                        super::key_encode::encode_from_column_row_with_collation(
                            vector,
                            row,
                            batch.schema().fields()[*column].source.collation_id,
                            scratch,
                        );
                    }
                    let ColumnVector::Date32 { data, .. } = vector else {
                        return None;
                    };
                    let null = vector.is_null(row);
                    let raw = if null { 0 } else { data[row] as u32 as u128 };
                    let encoded = if direct || null {
                        raw
                    } else {
                        u128::from(
                            dictionary_primitive_token(
                                values,
                                raw_values,
                                data[row],
                                fx_mix_u128(raw),
                            )?
                            .local,
                        )
                    };
                    if encoded > u128::from(token_mask) && !direct {
                        return None;
                    }
                    (
                        encoded,
                        if null { NULL_HASH } else { fx_mix_u128(raw) },
                        null,
                    )
                }
                DictionaryColumnSource::Bool { column } => {
                    let vector = batch.columns().get(*column)?;
                    if MATERIALIZE {
                        super::key_encode::encode_from_column_row_with_collation(
                            vector,
                            row,
                            batch.schema().fields()[*column].source.collation_id,
                            scratch,
                        );
                    }
                    let ColumnVector::Bool { values, .. } = vector else {
                        return None;
                    };
                    let null = vector.is_null(row);
                    let raw = if null {
                        0
                    } else if direct {
                        u128::from(values.get(row))
                    } else {
                        1 + u128::from(values.get(row))
                    };
                    (raw, if null { NULL_HASH } else { fx_mix_u128(raw) }, null)
                }
            };
            key |= value_bits << column.shift;
            if direct && is_null {
                key |= 1u128 << column.null_bit;
            }
            stable_hash = fx_mix_u128(
                u128::from(stable_hash) ^ (u128::from(component_hash) << 64) ^ position as u128,
            );
            if position == 0 {
                first_component_hash = component_hash;
            }
        }
        Some((
            key,
            if self.columns.len() == 1 {
                first_component_hash
            } else {
                stable_hash
            },
        ))
    }

    fn is_single_utf8(&self) -> bool {
        matches!(
            self.columns.as_slice(),
            [DictionaryColumn {
                source: DictionaryColumnSource::Utf8 { .. },
                ..
            }]
        )
    }

    fn current_raw_tokens(&self) -> &[u32] {
        &self.current_raw_tokens
    }

    /// Whether every dictionary-coded lane can admit a worst-case batch in
    /// the current epoch. Each visible row can introduce at most one token
    /// per lane. Checking before the batch keeps the compact COUNT arena
    /// atomic: if an epoch is too full, the caller can spill it and retry the
    /// complete batch with a fresh encoder without duplicating any counts.
    fn can_admit_batch(&self, visible_rows: usize) -> bool {
        self.columns.iter().all(|column| {
            if column.direct {
                return true;
            }
            let used = match &column.source {
                DictionaryColumnSource::Utf8 {
                    canonical_to_token, ..
                } => canonical_to_token.len(),
                DictionaryColumnSource::I64 { values, .. } => values.len(),
                DictionaryColumnSource::U64 { values, .. } => values.len(),
                DictionaryColumnSource::Date32 { values, .. } => values.len(),
                // The non-direct Boolean representation uses the fixed
                // tokens 1 and 2 and therefore cannot grow with row count.
                DictionaryColumnSource::Bool { .. } => return true,
            };
            used.saturating_add(visible_rows) <= column.token_mask as usize
        })
    }

    #[inline]
    fn rows_physically_equal(&self, batch: &Batch, left: usize, right: usize) -> bool {
        self.columns.iter().all(|column| {
            let source = match &column.source {
                DictionaryColumnSource::Utf8 { column, .. }
                | DictionaryColumnSource::I64 { column, .. }
                | DictionaryColumnSource::U64 { column, .. }
                | DictionaryColumnSource::Date32 { column, .. }
                | DictionaryColumnSource::Bool { column } => *column,
            };
            column_rows_physically_equal(&batch.columns()[source], left, right)
        })
    }

    fn direct_key_values(&self, key: u128, raw_tokens: &[u32]) -> Vec<Value> {
        let _ = raw_tokens;
        self.direct_key_values_iter(key).collect()
    }

    fn direct_key_values_iter(&self, key: u128) -> impl Iterator<Item = Value> + '_ {
        self.columns.iter().map(move |column| {
            let lane = (key >> column.shift) & u128::from(column.token_mask);
            let is_null = if column.direct {
                key & (1u128 << column.null_bit) != 0
            } else {
                lane == 0
            };
            if is_null {
                return Value::Null;
            }
            match &column.source {
                DictionaryColumnSource::Utf8 { raw_values, .. } => {
                    Value::String(raw_values[lane as usize - 1].to_string())
                }
                DictionaryColumnSource::I64 { raw_values, .. } => {
                    if column.direct {
                        Value::Int((key >> column.shift) as u64 as i64)
                    } else {
                        Value::Int(raw_values[lane as usize - 1])
                    }
                }
                DictionaryColumnSource::U64 { raw_values, .. } => {
                    if column.direct {
                        Value::UInt((key >> column.shift) as u64)
                    } else {
                        Value::UInt(raw_values[lane as usize - 1])
                    }
                }
                DictionaryColumnSource::Date32 { raw_values, .. } => {
                    if column.direct {
                        Value::Date((key >> column.shift) as u32 as i32)
                    } else {
                        Value::Date(raw_values[lane as usize - 1])
                    }
                }
                DictionaryColumnSource::Bool { .. } => Value::Int(if column.direct {
                    i64::from(lane != 0)
                } else {
                    i64::from(lane == 2)
                }),
            }
        })
    }

    fn write_direct_canonical_key(&self, key: u128, out: &mut Vec<u8>) {
        out.clear();
        for column in &self.columns {
            let lane = (key >> column.shift) & u128::from(column.token_mask);
            let is_null = if column.direct {
                key & (1u128 << column.null_bit) != 0
            } else {
                lane == 0
            };
            if is_null {
                out.push(0);
                continue;
            }
            match &column.source {
                DictionaryColumnSource::Utf8 { canonical_keys, .. } => {
                    out.extend_from_slice(&canonical_keys[lane as usize - 1]);
                }
                DictionaryColumnSource::I64 { raw_values, .. } => {
                    let value = if column.direct {
                        (key >> column.shift) as u64 as i64
                    } else {
                        raw_values[lane as usize - 1]
                    };
                    super::key_encode::encode(&Value::Int(value), out);
                }
                DictionaryColumnSource::U64 { raw_values, .. } => {
                    let value = if column.direct {
                        (key >> column.shift) as u64
                    } else {
                        raw_values[lane as usize - 1]
                    };
                    super::key_encode::encode(&Value::UInt(value), out);
                }
                DictionaryColumnSource::Date32 { raw_values, .. } => {
                    let value = if column.direct {
                        (key >> column.shift) as u32 as i32
                    } else {
                        raw_values[lane as usize - 1]
                    };
                    super::key_encode::encode(&Value::Date(value), out);
                }
                DictionaryColumnSource::Bool { .. } => {
                    let value = if column.direct { lane != 0 } else { lane == 2 };
                    super::key_encode::encode(&Value::Int(i64::from(value)), out);
                }
            }
        }
    }

    fn align_merge_tokens(
        &mut self,
        registries: &mut Vec<hashbrown::HashMap<Arc<[u8]>, u32, ahash::RandomState>>,
    ) {
        let mut token_column = 0usize;
        for column in &mut self.columns {
            if column.direct {
                continue;
            }
            if token_column == registries.len() {
                registries.push(hashbrown::HashMap::with_hasher(ahash::RandomState::new()));
            }
            let registry = &mut registries[token_column];
            let mut intern = |canonical: Arc<[u8]>| {
                let next_token = u32::try_from(registry.len())
                    .expect("dictionary merge token count fits u32")
                    .saturating_add(1);
                match registry.entry(canonical) {
                    hashbrown::hash_map::Entry::Occupied(entry) => *entry.get(),
                    hashbrown::hash_map::Entry::Vacant(entry) => {
                        entry.insert(next_token);
                        next_token
                    }
                }
            };
            match &mut column.source {
                DictionaryColumnSource::Utf8 {
                    canonical_keys,
                    merge_tokens,
                    ..
                } => {
                    merge_tokens.clear();
                    merge_tokens.reserve(canonical_keys.len());
                    for canonical in canonical_keys {
                        merge_tokens.push(intern(canonical.clone()));
                    }
                }
                DictionaryColumnSource::I64 {
                    raw_values,
                    merge_tokens,
                    ..
                } => {
                    merge_tokens.clear();
                    merge_tokens.reserve(raw_values.len());
                    for &value in raw_values.iter() {
                        let mut bytes = Vec::with_capacity(9);
                        super::key_encode::encode(&Value::Int(value), &mut bytes);
                        merge_tokens.push(intern(Arc::from(bytes)));
                    }
                }
                DictionaryColumnSource::U64 {
                    raw_values,
                    merge_tokens,
                    ..
                } => {
                    merge_tokens.clear();
                    merge_tokens.reserve(raw_values.len());
                    for &value in raw_values.iter() {
                        let mut bytes = Vec::with_capacity(9);
                        super::key_encode::encode(&Value::UInt(value), &mut bytes);
                        merge_tokens.push(intern(Arc::from(bytes)));
                    }
                }
                DictionaryColumnSource::Date32 {
                    raw_values,
                    merge_tokens,
                    ..
                } => {
                    merge_tokens.clear();
                    merge_tokens.reserve(raw_values.len());
                    for &value in raw_values.iter() {
                        let mut bytes = Vec::with_capacity(5);
                        super::key_encode::encode(&Value::Date(value), &mut bytes);
                        merge_tokens.push(intern(Arc::from(bytes)));
                    }
                }
                DictionaryColumnSource::Bool { .. } => {}
            }
            token_column += 1;
        }
    }

    fn direct_merge_key(&self, mut key: u128) -> u128 {
        for column in &self.columns {
            if column.direct {
                continue;
            }
            let local = ((key >> column.shift) & u128::from(column.token_mask)) as usize;
            if local == 0 {
                continue;
            }
            let merged = match &column.source {
                DictionaryColumnSource::Utf8 { merge_tokens, .. }
                | DictionaryColumnSource::I64 { merge_tokens, .. }
                | DictionaryColumnSource::U64 { merge_tokens, .. }
                | DictionaryColumnSource::Date32 { merge_tokens, .. } => {
                    merge_tokens.get(local - 1).copied().unwrap_or(local as u32)
                }
                DictionaryColumnSource::Bool { .. } => local as u32,
            };
            key &= !(u128::from(column.token_mask) << column.shift);
            key |= u128::from(merged) << column.shift;
        }
        key
    }

    fn allocated_bytes(&self) -> usize {
        self.columns
            .iter()
            .map(|column| match &column.source {
                DictionaryColumnSource::Utf8 {
                    raw_to_token,
                    canonical_to_token,
                    canonical_keys,
                    merge_tokens,
                    raw_values,
                    raw_value_heap_bytes,
                    canonical_key_capacity_bytes,
                    ..
                } => raw_to_token
                    .allocated_bytes()
                    .saturating_add(canonical_to_token.capacity().saturating_mul(
                        HASH_ENTRY_OVERHEAD
                            + std::mem::size_of::<Arc<[u8]>>()
                            + std::mem::size_of::<DictionaryToken>(),
                    ))
                    .saturating_add(*canonical_key_capacity_bytes)
                    .saturating_add(
                        canonical_keys
                            .capacity()
                            .saturating_mul(std::mem::size_of::<Arc<[u8]>>()),
                    )
                    .saturating_add(
                        merge_tokens
                            .capacity()
                            .saturating_mul(std::mem::size_of::<u32>()),
                    )
                    .saturating_add(
                        raw_values
                            .capacity()
                            .saturating_mul(std::mem::size_of::<Box<str>>()),
                    )
                    .saturating_add(*raw_value_heap_bytes),
                DictionaryColumnSource::I64 {
                    values,
                    dense_tokens,
                    raw_values,
                    merge_tokens,
                    ..
                } => values
                    .capacity()
                    .saturating_mul(
                        HASH_ENTRY_OVERHEAD
                            + std::mem::size_of::<i64>()
                            + std::mem::size_of::<DictionaryToken>(),
                    )
                    .saturating_add(
                        raw_values
                            .capacity()
                            .saturating_mul(std::mem::size_of::<i64>()),
                    )
                    .saturating_add(
                        merge_tokens
                            .capacity()
                            .saturating_mul(std::mem::size_of::<u32>()),
                    )
                    .saturating_add(dense_tokens.as_ref().map_or(0, |cache| {
                        cache
                            .slots
                            .capacity()
                            .saturating_mul(std::mem::size_of::<DictionaryToken>())
                    })),
                DictionaryColumnSource::U64 {
                    values,
                    raw_values,
                    merge_tokens,
                    ..
                } => values
                    .capacity()
                    .saturating_mul(
                        HASH_ENTRY_OVERHEAD
                            + std::mem::size_of::<u64>()
                            + std::mem::size_of::<DictionaryToken>(),
                    )
                    .saturating_add(
                        raw_values
                            .capacity()
                            .saturating_mul(std::mem::size_of::<u64>()),
                    )
                    .saturating_add(
                        merge_tokens
                            .capacity()
                            .saturating_mul(std::mem::size_of::<u32>()),
                    ),
                DictionaryColumnSource::Date32 {
                    values,
                    raw_values,
                    merge_tokens,
                    ..
                } => values
                    .capacity()
                    .saturating_mul(
                        HASH_ENTRY_OVERHEAD
                            + std::mem::size_of::<i32>()
                            + std::mem::size_of::<DictionaryToken>(),
                    )
                    .saturating_add(
                        raw_values
                            .capacity()
                            .saturating_mul(std::mem::size_of::<i32>()),
                    )
                    .saturating_add(
                        merge_tokens
                            .capacity()
                            .saturating_mul(std::mem::size_of::<u32>()),
                    ),
                DictionaryColumnSource::Bool { .. } => 0,
            })
            .sum::<usize>()
            .saturating_sub(self.merge_token_bytes)
    }
}

fn dictionary_primitive_token<T: Copy + Eq + std::hash::Hash>(
    values: &mut hashbrown::HashMap<T, DictionaryToken, ahash::RandomState>,
    raw_values: &mut Vec<T>,
    value: T,
    stable_hash: u64,
) -> Option<DictionaryToken> {
    if let Some(&token) = values.get(&value) {
        return Some(token);
    }
    let token = DictionaryToken {
        local: u32::try_from(values.len()).ok()?.checked_add(1)?,
        raw_local: 0,
        stable_hash,
    };
    values.insert(value, token);
    raw_values.push(value);
    Some(token)
}

#[inline(always)]
fn dictionary_i64_token(
    values: &mut hashbrown::HashMap<i64, DictionaryToken, ahash::RandomState>,
    dense_tokens: &mut Option<DenseI64TokenCache>,
    raw_values: &mut Vec<i64>,
    value: i64,
    raw: u128,
) -> Option<DictionaryToken> {
    if let Some(token) = dense_tokens.as_ref().and_then(|cache| cache.get(value)) {
        return Some(token);
    }
    if let Some(&token) = values.get(&value) {
        if dense_tokens
            .as_mut()
            .is_some_and(|cache| !cache.insert(value, token))
        {
            *dense_tokens = None;
        }
        return Some(token);
    }
    let token = DictionaryToken {
        local: u32::try_from(values.len()).ok()?.checked_add(1)?,
        raw_local: 0,
        stable_hash: fx_mix_u128(raw),
    };
    values.insert(value, token);
    raw_values.push(value);
    if dense_tokens
        .as_mut()
        .is_some_and(|cache| !cache.insert(value, token))
    {
        *dense_tokens = None;
    }
    Some(token)
}

/// Compact grouped COUNT state for mixed fixed-width + dictionary UTF-8 keys.
/// Each radix partition owns one open-addressing table, so finalization can
/// move partitions independently to the planned aggregate workers. Keys,
/// counts and representative-string tokens are flat arrays; no per-group
/// `Vec` or general-purpose `AggState` allocation is retained.
struct DictionaryCountArena {
    partitions: Vec<DictionaryCountPartition>,
    raw_token_width: usize,
    n_aggs: usize,
    /// A single UTF-8 dictionary token is already a dense, unique group id.
    /// Map it directly to its radix-partition row and avoid a second hash
    /// table probe for every input row.
    direct_token_handles: Option<Vec<u32>>,
    /// Reused per-batch raw frequency scratch. Entries keep hashes and row
    /// indexes only, so no pointer outlives the current Batch.
    batch_frequencies: Option<hashbrown::HashMap<u64, BatchRawFrequency, ahash::RandomState>>,
    adjacent_reuse: Option<bool>,
}

enum BatchRawFrequency {
    One {
        row: u32,
        count: u32,
    },
    /// Distinct raw strings with the same 64-bit hash. This is expected to be
    /// empty in normal data but retains exact byte equality under adversarial
    /// collisions.
    Collision(Vec<(u32, u32)>),
}

struct DictionaryCountPartition {
    slots: Vec<u32>,
    mask: usize,
    keys: Vec<u128>,
    raw_tokens: Vec<u32>,
    // Partial COUNT states use four bytes in the overwhelmingly common case.
    // Groups that cross u32::MAX promote into the sparse side table, keeping
    // full SQL COUNT semantics without making every high-cardinality group
    // pay eight bytes per slot.
    counts: Vec<u32>,
    overflow_counts: hashbrown::HashMap<usize, u64, ahash::RandomState>,
}

impl DictionaryCountPartition {
    fn new(seed: usize, raw_token_width: usize, n_aggs: usize, hashed: bool) -> Self {
        let wanted = ((seed.max(1) * 10).saturating_add(6) / 7).max(16);
        let capacity = wanted.next_power_of_two();
        Self {
            slots: if hashed {
                vec![0; capacity]
            } else {
                Vec::new()
            },
            mask: if hashed { capacity - 1 } else { 0 },
            keys: Vec::with_capacity(seed),
            raw_tokens: Vec::with_capacity(seed.saturating_mul(raw_token_width)),
            counts: Vec::with_capacity(seed.saturating_mul(n_aggs)),
            overflow_counts: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
        }
    }

    fn grow(&mut self) {
        let new_len = self.slots.len().saturating_mul(2);
        let new_mask = new_len - 1;
        let mut slots = vec![0u32; new_len];
        for (group, &key) in self.keys.iter().enumerate() {
            let hash = fx_mix_u128(key);
            let mut slot = hash as usize & new_mask;
            while slots[slot] != 0 {
                slot = (slot + 1) & new_mask;
            }
            slots[slot] = group as u32 + 1;
        }
        self.slots = slots;
        self.mask = new_mask;
    }

    fn reserve_group(&mut self, raw_token_width: usize, n_aggs: usize) {
        if self.keys.len() < self.keys.capacity() {
            return;
        }
        // Millions of retained keys make Vec's doubling slack material to
        // the query grant. Grow payload lanes together in quarter-capacity
        // steps; the hash index keeps its normal power-of-two probing layout.
        let extra = (self.keys.capacity() / 4).max(64);
        self.keys.reserve_exact(extra);
        let groups = self.keys.capacity();
        self.counts.reserve_exact(
            groups
                .saturating_mul(n_aggs)
                .saturating_sub(self.counts.len()),
        );
        self.raw_tokens.reserve_exact(
            groups
                .saturating_mul(raw_token_width)
                .saturating_sub(self.raw_tokens.len()),
        );
    }

    #[inline]
    fn probe_or_insert(
        &mut self,
        key: u128,
        raw_tokens: &[u32],
        raw_token_width: usize,
        n_aggs: usize,
    ) -> usize {
        let hash = fx_mix_u128(key);
        let mut slot = hash as usize & self.mask;
        loop {
            let stored = self.slots[slot];
            if stored == 0 {
                let group = self.keys.len();
                self.reserve_group(raw_token_width, n_aggs);
                self.slots[slot] = group as u32 + 1;
                self.keys.push(key);
                debug_assert_eq!(raw_tokens.len(), raw_token_width);
                self.raw_tokens.extend_from_slice(raw_tokens);
                self.counts
                    .resize(self.counts.len().saturating_add(n_aggs), 0);
                if self.keys.len() * 10 >= self.slots.len() * 7 {
                    self.grow();
                }
                return group;
            }
            let group = (stored - 1) as usize;
            if self.keys[group] == key {
                return group;
            }
            slot = (slot + 1) & self.mask;
        }
    }

    fn allocated_bytes(&self) -> usize {
        self.slots
            .capacity()
            .saturating_mul(std::mem::size_of::<u32>())
            .saturating_add(
                self.keys
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u128>()),
            )
            .saturating_add(
                self.raw_tokens
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                self.counts
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(self.overflow_counts.capacity().saturating_mul(
                HASH_ENTRY_OVERHEAD + std::mem::size_of::<usize>() + std::mem::size_of::<u64>(),
            ))
    }

    #[inline]
    fn increment_count(&mut self, group: usize, slot: usize, n_aggs: usize, amount: u64) {
        let index = group * n_aggs + slot;
        let current = u64::from(self.counts[index]);
        // The compact u32 lane is the overwhelmingly common state. Avoid a
        // sparse overflow-map hash probe on every input row; only a saturated
        // slot can possibly have been promoted.
        if current == u64::from(u32::MAX) {
            if let Some(count) = self.overflow_counts.get_mut(&index) {
                *count = count.saturating_add(amount);
                return;
            }
        }
        let next = current.saturating_add(amount);
        if next <= u64::from(u32::MAX) {
            self.counts[index] = next as u32;
        } else {
            self.counts[index] = u32::MAX;
            self.overflow_counts.insert(index, next);
        }
    }

    #[inline]
    fn count(&self, group: usize, slot: usize, n_aggs: usize) -> u64 {
        let index = group * n_aggs + slot;
        let compact = self.counts[index];
        if compact != u32::MAX {
            return u64::from(compact);
        }
        self.overflow_counts
            .get(&index)
            .copied()
            .unwrap_or(u64::from(u32::MAX))
    }

    fn insert_unique(
        &mut self,
        key: u128,
        raw_tokens: &[u32],
        raw_token_width: usize,
        n_aggs: usize,
    ) -> usize {
        debug_assert!(self.slots.is_empty());
        let group = self.keys.len();
        self.reserve_group(raw_token_width, n_aggs);
        self.keys.push(key);
        debug_assert_eq!(raw_tokens.len(), raw_token_width);
        self.raw_tokens.extend_from_slice(raw_tokens);
        self.counts
            .resize(self.counts.len().saturating_add(n_aggs), 0);
        group
    }
}

impl DictionaryCountArena {
    fn new(seed: usize, raw_token_width: usize, n_aggs: usize, direct_tokens: bool) -> Self {
        let per_partition = seed.div_ceil(NUM_PARTITIONS).max(1);
        Self {
            partitions: (0..NUM_PARTITIONS)
                .map(|_| {
                    DictionaryCountPartition::new(
                        per_partition,
                        raw_token_width,
                        n_aggs,
                        !direct_tokens,
                    )
                })
                .collect(),
            raw_token_width,
            n_aggs,
            direct_token_handles: direct_tokens.then(|| Vec::with_capacity(seed)),
            batch_frequencies: direct_tokens.then(|| {
                hashbrown::HashMap::with_capacity_and_hasher(seed, ahash::RandomState::new())
            }),
            adjacent_reuse: None,
        }
    }

    fn consume_batch(
        &mut self,
        encoder: &mut DictionaryGroupEncoder,
        aggs: &[AggregateSpec],
        batch: &Batch,
        canonical_key: &mut Vec<u8>,
    ) -> Result<(), QueryError> {
        let single_utf8 = encoder.is_single_utf8();
        if single_utf8
            && aggs
                .iter()
                .all(|spec| matches!(spec.func, AggregateKind::CountStar))
        {
            return self.consume_single_utf8_count_star_batch(encoder, aggs, batch, canonical_key);
        }
        let reuse_adjacent = *self.adjacent_reuse.get_or_insert_with(|| {
            let mut rows = batch.visible_indices().take(2048);
            let Some(mut previous) = rows.next() else {
                return false;
            };
            let mut pairs = 0usize;
            let mut equal = 0usize;
            for row in rows {
                pairs += 1;
                equal += usize::from(encoder.rows_physically_equal(batch, previous, row));
                previous = row;
            }
            pairs != 0 && equal.saturating_mul(4) >= pairs
        });
        let mut previous: Option<(usize, usize, usize)> = None;
        for row in batch.visible_indices() {
            // Ordered/clustered inputs commonly contain short runs of the
            // same composite group. Reuse the already resolved arena handle
            // before paying for dictionary lookup, collation normalization,
            // u128 hashing and open-addressing again. This is exact physical
            // equality only; collation-equivalent spellings still take the
            // canonical dictionary path below.
            if reuse_adjacent {
                if let Some((previous_row, partition, group)) = previous {
                    if encoder.rows_physically_equal(batch, previous_row, row) {
                        for (slot, spec) in aggs.iter().enumerate() {
                            if matches!(spec.func, AggregateKind::CountStar)
                                || !batch.columns()[spec.input_column].is_null(row)
                            {
                                self.partitions[partition].increment_count(
                                    group,
                                    slot,
                                    self.n_aggs,
                                    1,
                                );
                            }
                        }
                        previous = Some((row, partition, group));
                        continue;
                    }
                }
            }
            let encoded = if single_utf8 {
                encoder
                    .encode_single_utf8_count_row(batch, row, canonical_key, None)
                    .map(|(key, hash, raw_token)| (key, hash, Some(raw_token)))
            } else {
                encoder
                    .encode_count_row(batch, row, canonical_key, self.raw_token_width != 0)
                    .map(|(key, hash)| (key, hash, None))
            };
            let Some((key, stable_hash, direct_raw_token)) = encoded else {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_INTERNAL_ERROR,
                    "compact dictionary aggregate key layout changed while consuming input",
                ));
            };
            // The encoder's hash is derived from raw fixed-width values and
            // canonical collation bytes, so it is stable across workers.
            // Using it directly avoids rebuilding the full canonical
            // composite key for every input row; canonical bytes are
            // reconstructed once per output/spill group from the retained
            // lossless packed key and dictionaries.
            let hash = stable_hash;
            let partition = (hash & PARTITION_MASK) as usize;
            let group = if let Some(handles) = self.direct_token_handles.as_mut() {
                let raw_token = direct_raw_token.expect("single UTF-8 encoder returns its token");
                let token = raw_token as usize;
                if handles.len() <= token {
                    handles.resize(token + 1, 0);
                }
                let stored = handles[token];
                if stored == 0 {
                    let group = self.partitions[partition].insert_unique(
                        key,
                        if self.raw_token_width == 0 {
                            &[]
                        } else {
                            std::slice::from_ref(&raw_token)
                        },
                        self.raw_token_width,
                        self.n_aggs,
                    );
                    handles[token] = packed_bucket_handle(partition, group)? + 1;
                    group
                } else {
                    let handle = stored - 1;
                    debug_assert_eq!((handle >> 24) as usize, partition);
                    (handle & 0x00ff_ffff) as usize
                }
            } else {
                self.partitions[partition].probe_or_insert(
                    key,
                    &encoder.current_raw_tokens()[..self.raw_token_width],
                    self.raw_token_width,
                    self.n_aggs,
                )
            };
            for (slot, spec) in aggs.iter().enumerate() {
                if matches!(spec.func, AggregateKind::CountStar)
                    || !batch.columns()[spec.input_column].is_null(row)
                {
                    self.partitions[partition].increment_count(group, slot, self.n_aggs, 1);
                }
            }
            previous = Some((row, partition, group));
        }
        Ok(())
    }

    fn consume_single_utf8_count_star_batch(
        &mut self,
        encoder: &mut DictionaryGroupEncoder,
        aggs: &[AggregateSpec],
        batch: &Batch,
        canonical_key: &mut Vec<u8>,
    ) -> Result<(), QueryError> {
        let column_index = match &encoder.columns[0].source {
            DictionaryColumnSource::Utf8 { column, .. } => *column,
            _ => unreachable!("single UTF-8 layout checked"),
        };
        let column = &batch.columns()[column_index];
        let ColumnVector::Utf8 { offsets, data, .. } = column else {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "compact dictionary aggregate key layout changed while consuming input",
            ));
        };
        let mut frequencies = self
            .batch_frequencies
            .take()
            .expect("single UTF-8 count retains batch frequency scratch");
        frequencies.clear();
        frequencies.reserve(batch.visible_rows());
        let mut null_row = None;
        let mut null_count = 0u32;
        for row in batch.visible_indices() {
            if column.is_null(row) {
                null_row.get_or_insert(row);
                null_count = null_count.saturating_add(1);
                continue;
            }
            let raw = &data[offsets[row] as usize..offsets[row + 1] as usize];
            let hash = super::key_encode::fxhash(raw);
            match frequencies.entry(hash) {
                hashbrown::hash_map::Entry::Vacant(entry) => {
                    entry.insert(BatchRawFrequency::One {
                        row: row as u32,
                        count: 1,
                    });
                }
                hashbrown::hash_map::Entry::Occupied(mut entry) => match entry.get_mut() {
                    BatchRawFrequency::One { row: first, count } => {
                        let first_row = *first;
                        let first = first_row as usize;
                        let first_raw = &data[offsets[first] as usize..offsets[first + 1] as usize];
                        if first_raw == raw {
                            *count = count.saturating_add(1);
                        } else {
                            let old = (first_row, *count);
                            *entry.get_mut() =
                                BatchRawFrequency::Collision(vec![old, (row as u32, 1)]);
                        }
                    }
                    BatchRawFrequency::Collision(values) => {
                        if let Some((_, count)) = values.iter_mut().find(|(candidate, _)| {
                            let candidate = *candidate as usize;
                            &data[offsets[candidate] as usize..offsets[candidate + 1] as usize]
                                == raw
                        }) {
                            *count = count.saturating_add(1);
                        } else {
                            values.push((row as u32, 1));
                        }
                    }
                },
            }
        }

        if let Some(row) = null_row {
            let (key, hash, raw_token) = encoder
                .encode_single_utf8_count_row(batch, row, canonical_key, None)
                .expect("single UTF-8 layout remains stable");
            self.add_direct_count_frequency(key, hash, raw_token, aggs.len(), null_count)?;
        }
        for (raw_hash, frequency) in frequencies.drain() {
            match frequency {
                BatchRawFrequency::One { row, count } => {
                    let (key, hash, raw_token) = encoder
                        .encode_single_utf8_count_row(
                            batch,
                            row as usize,
                            canonical_key,
                            Some(raw_hash),
                        )
                        .expect("single UTF-8 layout remains stable");
                    self.add_direct_count_frequency(key, hash, raw_token, aggs.len(), count)?;
                }
                BatchRawFrequency::Collision(values) => {
                    for (row, count) in values {
                        let (key, hash, raw_token) = encoder
                            .encode_single_utf8_count_row(
                                batch,
                                row as usize,
                                canonical_key,
                                Some(raw_hash),
                            )
                            .expect("single UTF-8 layout remains stable");
                        self.add_direct_count_frequency(key, hash, raw_token, aggs.len(), count)?;
                    }
                }
            }
        }
        self.batch_frequencies = Some(frequencies);
        Ok(())
    }

    fn add_direct_count_frequency(
        &mut self,
        key: u128,
        hash: u64,
        raw_token: u32,
        n_aggs: usize,
        frequency: u32,
    ) -> Result<(), QueryError> {
        let partition = (hash & PARTITION_MASK) as usize;
        let handles = self
            .direct_token_handles
            .as_mut()
            .expect("single UTF-8 count uses direct token handles");
        let token = raw_token as usize;
        if handles.len() <= token {
            handles.resize(token + 1, 0);
        }
        let stored = handles[token];
        let group = if stored == 0 {
            let group = self.partitions[partition].insert_unique(
                key,
                if self.raw_token_width == 0 {
                    &[]
                } else {
                    std::slice::from_ref(&raw_token)
                },
                self.raw_token_width,
                self.n_aggs,
            );
            handles[token] = packed_bucket_handle(partition, group)? + 1;
            group
        } else {
            let handle = stored - 1;
            debug_assert_eq!((handle >> 24) as usize, partition);
            (handle & 0x00ff_ffff) as usize
        };
        for slot in 0..n_aggs {
            self.partitions[partition].increment_count(
                group,
                slot,
                self.n_aggs,
                u64::from(frequency),
            );
        }
        Ok(())
    }

    fn allocated_bytes(&self) -> usize {
        self.partitions
            .iter()
            .map(DictionaryCountPartition::allocated_bytes)
            .sum::<usize>()
            .saturating_add(
                self.direct_token_handles
                    .as_ref()
                    .map(|handles| {
                        handles
                            .capacity()
                            .saturating_mul(std::mem::size_of::<u32>())
                    })
                    .unwrap_or(0),
            )
            .saturating_add(
                self.batch_frequencies
                    .as_ref()
                    .map(|frequencies| {
                        frequencies
                            .capacity()
                            .saturating_mul(
                                HASH_ENTRY_OVERHEAD
                                    + std::mem::size_of::<u64>()
                                    + std::mem::size_of::<BatchRawFrequency>(),
                            )
                            .saturating_add(
                                frequencies
                                    .values()
                                    .map(|frequency| match frequency {
                                        BatchRawFrequency::One { .. } => 0,
                                        BatchRawFrequency::Collision(values) => values
                                            .capacity()
                                            .saturating_mul(std::mem::size_of::<(u32, u32)>()),
                                    })
                                    .sum::<usize>(),
                            )
                    })
                    .unwrap_or(0),
            )
    }

    fn take_partition(&mut self, partition: usize) -> DictionaryCountPartition {
        std::mem::replace(
            &mut self.partitions[partition],
            DictionaryCountPartition::new(
                1,
                self.raw_token_width,
                self.n_aggs,
                self.direct_token_handles.is_none(),
            ),
        )
    }
}

/// Dictionary encoding is useful only when raw strings repeat within a batch.
/// Decline for a near-unique sample so arbitrary high-cardinality text uses
/// the canonical key arena directly instead of maintaining two redundant maps.
fn utf8_dictionary_worthwhile(column: &ColumnVector, batch: &Batch) -> bool {
    // A 64-row prefix is too sensitive to physical clustering: a globally
    // low-cardinality dimension can look unique in one short primary-key run.
    // Inspect at most one normal vector batch and still reject genuinely
    // near-unique text before the operator allocates its dictionaries.
    const SAMPLE_ROWS: usize = 2048;
    const MIN_SAMPLE: usize = 32;
    let ColumnVector::Utf8 { offsets, data, .. } = column else {
        return false;
    };
    let mut values = hashbrown::HashSet::<&[u8], ahash::RandomState>::with_capacity_and_hasher(
        SAMPLE_ROWS.min(batch.visible_rows()),
        ahash::RandomState::new(),
    );
    let mut sampled = 0usize;
    for row in batch.visible_indices().take(SAMPLE_ROWS) {
        sampled += 1;
        if column.is_null(row) {
            continue;
        }
        let value = &data[offsets[row] as usize..offsets[row + 1] as usize];
        values.insert(value);
    }
    sampled < MIN_SAMPLE || values.len() * 16 < sampled * 15
}

/// Direct lookup accelerator for a single bounded I64 GROUP BY lane.  It is
/// only a lookup layout: canonical keys and shared aggregate states continue
/// to live in the ordinary radix partitions, so finalize/merge/spill retain
/// one semantic implementation.  A range expansion that would exceed the
/// bound drops this accelerator and continues through Packed64.
struct DenseI64Lookup {
    min: i64,
    slots: Vec<u32>,
    null_slot: Option<u32>,
}

const DENSE_I64_EMPTY: u32 = u32::MAX;
// 2M × u32 = 8 MiB per worker. This is deliberately an operator-internal
// work-area choice, not a query-shape threshold; wider domains use Packed64.
const DENSE_I64_MAX_SLOTS: usize = 2_000_000;

impl DenseI64Lookup {
    fn try_from_batch(group_columns: &[usize], batch: &Batch) -> Option<Self> {
        let [column] = group_columns else { return None };
        let ColumnVector::I64 { data, nulls } = batch.columns().get(*column)? else {
            return None;
        };
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        for row in batch.visible_indices() {
            if nulls.as_ref().is_some_and(|valid| !valid.get(row)) {
                continue;
            }
            min = min.min(data[row]);
            max = max.max(data[row]);
        }
        if min == i64::MAX {
            min = 0;
            max = 0;
        }
        let span = i128::from(max) - i128::from(min) + 1;
        (span > 0 && span <= DENSE_I64_MAX_SLOTS as i128).then(|| Self {
            min,
            slots: vec![DENSE_I64_EMPTY; span as usize],
            null_slot: None,
        })
    }

    fn index_for(&mut self, value: i64) -> Option<usize> {
        let current_max = self
            .min
            .checked_add(self.slots.len().saturating_sub(1) as i64)?;
        if value >= self.min && value <= current_max {
            return Some((value - self.min) as usize);
        }
        let required_min = self.min.min(value);
        let required_max = current_max.max(value);
        let required_span = i128::from(required_max) - i128::from(required_min) + 1;
        if required_span <= 0 || required_span as usize > DENSE_I64_MAX_SLOTS {
            return None;
        }

        // Grow geometrically. Analytical GROUP BY input is commonly ordered
        // by its integer key; sizing this table to the exact observed span
        // copied every prior slot once per input batch (quadratic work for a
        // monotonic domain). Keep the lookup bounded by the same operator
        // work-area cap, but amortise expansion exactly like Vec does.
        let target_span = (self.slots.len().saturating_mul(2))
            .max(required_span as usize)
            .min(DENSE_I64_MAX_SLOTS);
        let target_span_i128 = target_span as i128;
        let new_min_i128 = if value < self.min {
            // The domain grew downwards: leave the geometric slack below the
            // old interval, with the old maximum anchored at the upper end.
            i128::from(current_max) - target_span_i128 + 1
        } else {
            // The domain grew upwards: retain the old minimum so the slack
            // is available above the newly observed value.
            i128::from(self.min)
        };
        let new_min = i64::try_from(new_min_i128).ok()?;
        let mut slots = vec![DENSE_I64_EMPTY; target_span];
        let shift = (i128::from(self.min) - new_min_i128) as usize;
        slots[shift..shift + self.slots.len()].copy_from_slice(&self.slots);
        self.min = new_min;
        self.slots = slots;
        Some((i128::from(value) - new_min_i128) as usize)
    }

    fn allocated_bytes(&self) -> usize {
        self.slots
            .capacity()
            .saturating_mul(std::mem::size_of::<u32>())
    }
}

#[derive(Debug, Clone, Copy)]
struct Packed64Col {
    col_idx: usize,
    /// Bit shift for this column's value bits in the packed u64. Always
    /// `< 56` so the high 8 bits stay reserved for the NULL mask.
    shift: u32,
    /// Bit position in the packed u64 to flip when this column's value
    /// at the current row is NULL. Each col gets a unique bit in
    /// `56..64`.
    null_bit_shift: u32,
    /// Mask to apply to the value before shifting; trims a narrow lane
    /// (e.g. Date32 → 32 bits) without sign-extending into adjacent
    /// slots.
    mask: u128,
    source: Packed64Source,
}

#[derive(Debug, Clone, Copy)]
enum Packed64Source {
    I64,
    Date32,
    Time64,
    DateTime64,
    Decimal64,
    /// Collation key up to six bytes, encoded losslessly as a four-bit
    /// length plus 48 payload bits. Two CHAR/VARCHAR keys therefore fit in
    /// the packed u128 lane used by low-cardinality analytical GROUP BYs.
    Utf8Small(&'static crate::charset::CollationInfo),
    /// Runtime-confirmed ASCII string lane containing at most one byte per
    /// value. The encoder still rejects a wider/non-ASCII value, so a later
    /// batch that disproves the first batch's shape safely switches the whole
    /// operator to the canonical byte-key path.
    Utf8One(&'static crate::charset::CollationInfo),
    /// CHAR(1) / single-byte UTF-8. The width is checked per row in the
    /// hot loop — a mismatched-width row aborts the packed batch and
    /// the byte path runs from scratch on the same batch (cheap escape
    /// hatch for the rare schema where, e.g., a VARCHAR(1) holds
    /// 2-byte characters).
    /// 1-bit lane.
    Bool,
}

/// SplitMix64 finalizer used for stable dictionary-token fingerprints. Radix
/// placement itself always comes from the canonical key bytes.
#[inline]
fn fx_mix_u128(x: u128) -> u64 {
    let folded = x as u64 ^ (x >> 64) as u64;
    let mut z = folded.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[inline]
fn packed_bucket_handle(partition: usize, group: usize) -> Result<u32, QueryError> {
    if partition >= NUM_PARTITIONS || group >= (1 << 24) {
        return Err(crate::storage::error(
            crate::error::catalog::ER_OUTOFMEMORY,
            "hash aggregate partition exceeds packed group capacity",
        ));
    }
    Ok(((partition as u32) << 24) | group as u32)
}

#[cfg(test)]
mod sqlproc_handle_audit {
    use super::*;
    #[test]
    fn sqlproc_sus_bucket_handles_never_alias_over_capacity() {
        assert_eq!(packed_bucket_handle(1, (1 << 24) - 1).unwrap(), 0x01ff_ffff);
        assert!(packed_bucket_handle(1, 1 << 24).is_err());
    }
}

/// Drive the Packed64 fast path: encode every visible row via
/// `Packed64Encoder`, look the resulting key up in the operator-wide
/// accelerator map, and append `(pidx << 24) | bucket_idx`
/// to `row_bucket_scratch`. Returns the number of newly-created buckets, or
/// `None` if a row fails to encode (UTF-8 width mismatch); the caller then
/// restarts the batch on the byte path.
///
/// A first-time-seen key records its canonical bytes and uses their stable
/// hash to choose the radix partition. The accelerator is operator-local,
/// but the partition assignment is therefore identical across workers even
/// when another worker selected the dictionary or canonical key layout.
///
/// Implemented as a free function with split-borrowed arguments so the
/// inner loop can mutate `partitions` + the scratch buffers
/// simultaneously without fighting the borrow checker on `&mut self`.
fn build_row_bucket_scratch_packed64(
    partitions: &mut [Partition],
    packed_lookup: &mut hashbrown::HashMap<u128, u32, ahash::RandomState>,
    inline_lookup: &mut smallvec::SmallVec<[(u128, u32); 8]>,
    encoder: &Packed64Encoder,
    group_columns: &[usize],
    aggs_len: usize,
    batch: &Batch,
    visible: impl Iterator<Item = usize>,
    row_bucket_scratch: &mut Vec<u32>,
    row_key_buf: &mut Vec<u8>,
) -> Result<Option<usize>, QueryError> {
    let mut new_buckets = 0usize;
    for r in visible {
        let key = match encoder.encode_row(batch, r) {
            Some(k) => k,
            None => return Ok(None),
        };
        if let Some(bucket) = inline_lookup
            .iter()
            .find_map(|(candidate, bucket)| (*candidate == key).then_some(*bucket))
        {
            row_bucket_scratch.push(bucket);
            continue;
        }
        if inline_lookup.len() >= inline_lookup.inline_size() {
            if let Some(&bucket) = packed_lookup.get(&key) {
                row_bucket_scratch.push(bucket);
                continue;
            }
        }

        // First occurrence of this packed key: also build the canonical
        // byte-encoded key retained by GroupBucket for snapshot/spill/output.
        // This is the only place a byte encode happens on the fast path
        // — for low-cardinality GROUP BY it fires O(distinct_keys) times, not
        // O(rows).
        row_key_buf.clear();
        for &c in group_columns {
            super::key_encode::encode_from_column_row_with_collation(
                &batch.columns()[c],
                r,
                batch.schema().fields()[c].source.collation_id,
                row_key_buf,
            );
        }
        let pidx = (super::key_encode::fxhash(row_key_buf) & PARTITION_MASK) as usize;
        let partition = &mut partitions[pidx];
        let bucket_idx = canonical_group_bucket(
            partition,
            row_key_buf,
            || {
                group_columns
                    .iter()
                    .map(|&c| batch.columns()[c].value_at(r))
                    .collect()
            },
            aggs_len,
            &mut new_buckets,
        );
        let bucket = packed_bucket_handle(pidx, bucket_idx)?;
        if inline_lookup.len() < inline_lookup.inline_size() {
            inline_lookup.push((key, bucket));
        } else {
            packed_lookup.insert(key, bucket);
        }
        row_bucket_scratch.push(bucket);
    }
    Ok(Some(new_buckets))
}

#[inline]
fn canonical_group_bucket(
    partition: &mut Partition,
    key: &[u8],
    values: impl FnOnce() -> Vec<Value>,
    aggs_len: usize,
    new_buckets: &mut usize,
) -> usize {
    if let Some(&index) = partition.by_key.get(key) {
        return index;
    }
    let index = partition.buckets.len();
    let structural_before = partition.structural_bytes();
    let canonical_key = key.to_vec();
    let canonical_key_bytes = canonical_key.capacity();
    partition.buckets.push(GroupBucket::new(
        canonical_key.clone(),
        values(),
        vec![AggState::default(); aggs_len],
    ));
    partition.by_key.insert(canonical_key, index);
    partition.record_insert(structural_before, canonical_key_bytes, index);
    *new_buckets += 1;
    index
}

fn build_row_bucket_scratch_dictionary(
    partitions: &mut [Partition],
    packed_lookup: &mut hashbrown::HashMap<u128, u32, ahash::RandomState>,
    inline_lookup: &mut smallvec::SmallVec<[(u128, u32); 8]>,
    encoder: &mut DictionaryGroupEncoder,
    group_columns: &[usize],
    aggs_len: usize,
    batch: &Batch,
    row_bucket_scratch: &mut Vec<u32>,
    row_key_buf: &mut Vec<u8>,
) -> Result<Option<usize>, QueryError> {
    let mut new_buckets = 0usize;
    for row in batch.visible_indices() {
        let Some((key, _stable_hash)) = encoder.encode_row(batch, row, row_key_buf) else {
            return Ok(None);
        };
        if let Some(bucket) = inline_lookup
            .iter()
            .find_map(|(candidate, bucket)| (*candidate == key).then_some(*bucket))
        {
            row_bucket_scratch.push(bucket);
            continue;
        }
        if inline_lookup.len() >= inline_lookup.inline_size() {
            if let Some(&bucket) = packed_lookup.get(&key) {
                row_bucket_scratch.push(bucket);
                continue;
            }
        }

        // `encode_row` leaves the exact canonical composite bytes in
        // `row_key_buf`. Reusing them here is essential for mixed keys: the
        // UTF-8 component may have required Unicode collation normalization,
        // while primitive components were appended directly without a second
        // dictionary or encoding pass.
        let pidx = (super::key_encode::fxhash(row_key_buf) & PARTITION_MASK) as usize;
        let partition = &mut partitions[pidx];
        let bucket_idx = canonical_group_bucket(
            partition,
            row_key_buf,
            || {
                group_columns
                    .iter()
                    .map(|&column| batch.columns()[column].value_at(row))
                    .collect()
            },
            aggs_len,
            &mut new_buckets,
        );
        let bucket = packed_bucket_handle(pidx, bucket_idx)?;
        if inline_lookup.len() < inline_lookup.inline_size() {
            inline_lookup.push((key, bucket));
        } else {
            packed_lookup.insert(key, bucket);
        }
        row_bucket_scratch.push(bucket);
    }
    Ok(Some(new_buckets))
}

fn build_row_bucket_scratch_dense_i64(
    partitions: &mut [Partition],
    lookup: &mut DenseI64Lookup,
    group_column: usize,
    aggs_len: usize,
    batch: &Batch,
    row_bucket_scratch: &mut Vec<u32>,
    row_key_buf: &mut Vec<u8>,
) -> Result<Option<usize>, QueryError> {
    let ColumnVector::I64 { data, nulls } = &batch.columns()[group_column] else {
        return Ok(None);
    };
    let mut new_buckets = 0usize;
    for row in batch.visible_indices() {
        let is_null = nulls.as_ref().is_some_and(|valid| !valid.get(row));
        let slot = if is_null {
            lookup.null_slot.unwrap_or(DENSE_I64_EMPTY)
        } else {
            let Some(index) = lookup.index_for(data[row]) else {
                return Ok(None);
            };
            lookup.slots[index]
        };
        if slot != DENSE_I64_EMPTY {
            row_bucket_scratch.push(slot);
            continue;
        }

        // Every layout uses the canonical key hash for radix placement, so
        // parallel workers can safely merge one partition at a time.
        row_key_buf.clear();
        super::key_encode::encode_from_column_row_with_collation(
            &batch.columns()[group_column],
            row,
            batch.schema().fields()[group_column].source.collation_id,
            row_key_buf,
        );
        let pidx = (super::key_encode::fxhash(row_key_buf) & PARTITION_MASK) as usize;
        let partition = &mut partitions[pidx];
        let structural_before = partition.structural_bytes();
        let bucket_idx = if let Some(&index) = partition.by_key.get(row_key_buf.as_slice()) {
            index
        } else {
            let index = partition.buckets.len();
            let canonical_key = row_key_buf.clone();
            let canonical_key_bytes = canonical_key.capacity();
            partition.buckets.push(GroupBucket::new(
                canonical_key.clone(),
                vec![batch.columns()[group_column].value_at(row)],
                vec![AggState::default(); aggs_len],
            ));
            partition.by_key.insert(canonical_key, index);
            partition.record_insert(structural_before, canonical_key_bytes, index);
            new_buckets += 1;
            index
        };
        let encoded = packed_bucket_handle(pidx, bucket_idx)?;
        if is_null {
            lookup.null_slot = Some(encoded);
        } else {
            let index = lookup
                .index_for(data[row])
                .expect("range admitted before bucket creation");
            lookup.slots[index] = encoded;
        }
        row_bucket_scratch.push(encoded);
    }
    Ok(Some(new_buckets))
}

impl Packed64Encoder {
    /// One-shot detection on the first batch with rows. Returns `None`
    /// for any schema that can't fit into 64 bits, or any lane the hot
    /// path doesn't handle (Decimal128, multi-byte UTF-8, Generic, …) —
    /// the caller then commits to the byte path for the operator's
    /// lifetime.
    fn try_build(group_columns: &[usize], batch: &Batch) -> Option<Self> {
        // Up to 8 columns (one NULL bit each). Beyond that the high-bit
        // mask runs out before the value bits do.
        if group_columns.is_empty() || group_columns.len() > 8 {
            return None;
        }
        let mut cols: Vec<Packed64Col> = Vec::with_capacity(group_columns.len());
        let mut next_shift: u32 = 0;
        for (i, &col_idx) in group_columns.iter().enumerate() {
            let col = batch.columns().get(col_idx)?;
            // Width (in bits) + lane kind. Reject anything wider than
            // the remaining budget (56 minus accumulated value bits).
            let (width_bits, source, mask) = match col {
                ColumnVector::Bool { .. } => (1u32, Packed64Source::Bool, 0x1u128),
                ColumnVector::I64 { .. } if group_columns.len() == 1 => {
                    (64, Packed64Source::I64, u128::from(u64::MAX))
                }
                ColumnVector::Date32 { .. } => (32, Packed64Source::Date32, 0xFFFF_FFFFu128),
                ColumnVector::Time64 { .. } => (64, Packed64Source::Time64, u128::from(u64::MAX)),
                ColumnVector::DateTime64 { .. } => {
                    (64, Packed64Source::DateTime64, u128::from(u64::MAX))
                }
                ColumnVector::Decimal64 { .. } => {
                    (64, Packed64Source::Decimal64, u128::from(u64::MAX))
                }
                ColumnVector::Utf8 {
                    offsets,
                    data,
                    nulls,
                } => {
                    let info = batch.schema().fields()[col_idx]
                        .source
                        .collation_id
                        .and_then(crate::charset::resolve_collation_id)
                        .unwrap_or_else(crate::charset::default_collation_info);
                    let one_byte = batch.visible_indices().all(|row| {
                        nulls.as_ref().is_some_and(|valid| !valid.get(row)) || {
                            let bytes = &data[offsets[row] as usize..offsets[row + 1] as usize];
                            bytes.len() <= 1 && bytes.is_ascii()
                        }
                    });
                    if one_byte {
                        (9, Packed64Source::Utf8One(info), (1u128 << 9) - 1)
                    } else {
                        (56, Packed64Source::Utf8Small(info), (1u128 << 56) - 1)
                    }
                }
                // Decimal32/Decimal128, Bytes, Generic, Null, F64 (NaN
                // comparison would break the typed-eq map) are not
                // worth fast-pathing today.
                _ => return None,
            };
            let value_budget = 120;
            if next_shift + width_bits > value_budget {
                return None;
            }
            cols.push(Packed64Col {
                col_idx,
                shift: next_shift,
                null_bit_shift: 120 + i as u32,
                mask,
                source,
            });
            next_shift += width_bits;
        }
        if let [first, second] = cols.as_slice() {
            if matches!(first.source, Packed64Source::Utf8One(_))
                && matches!(second.source, Packed64Source::Utf8One(_))
            {
                return Some(Packed64Encoder::Utf8OnePair([*first, *second]));
            }
        }
        Some(Packed64Encoder::Generic(cols))
    }

    /// Pack the values at `row` into a u64. NULL in a column flips the
    /// reserved high bit for that column instead of writing into the
    /// value slot — two rows that differ only in NULL/non-NULL never
    /// collide. Returns `None` if the batch lane no longer matches the
    /// detected fixed-width shape.
    #[inline]
    fn encode_row(&self, batch: &Batch, row: usize) -> Option<u128> {
        if let Self::Utf8OnePair(cols) = self {
            return Some(
                encode_utf8_one_packed_column(&batch.columns()[cols[0].col_idx], cols[0], row)?
                    | encode_utf8_one_packed_column(
                        &batch.columns()[cols[1].col_idx],
                        cols[1],
                        row,
                    )?,
            );
        }
        let Self::Generic(cols) = self else {
            unreachable!()
        };
        let mut key: u128 = 0;
        for col in cols {
            let cv = &batch.columns()[col.col_idx];
            let is_null = cv.nulls().map(|b| !b.get(row)).unwrap_or(false);
            if is_null {
                key |= 1u128 << col.null_bit_shift;
                continue;
            }
            let v: u128 = match (cv, col.source) {
                (ColumnVector::Bool { values, .. }, Packed64Source::Bool) => {
                    values.get(row) as u128
                }
                (ColumnVector::I64 { data, .. }, Packed64Source::I64) => data[row] as u64 as u128,
                (ColumnVector::Date32 { data, .. }, Packed64Source::Date32) => {
                    (data[row] as i64) as u64 as u128
                }
                (ColumnVector::Time64 { data, .. }, Packed64Source::Time64) => {
                    data[row] as u64 as u128
                }
                (ColumnVector::DateTime64 { data, .. }, Packed64Source::DateTime64) => {
                    data[row] as u64 as u128
                }
                (ColumnVector::Decimal64 { data, .. }, Packed64Source::Decimal64) => {
                    data[row] as u64 as u128
                }
                (ColumnVector::Utf8 { offsets, data, .. }, Packed64Source::Utf8One(info)) => {
                    let bytes = &data[offsets[row] as usize..offsets[row + 1] as usize];
                    let mut byte = match bytes {
                        [] => 0,
                        [byte] => *byte,
                        _ => return None,
                    };
                    if bytes.is_empty() {
                        0
                    } else {
                        if !byte.is_ascii() {
                            return None;
                        }
                        if (info.pad_space && byte == b' ')
                            || (info.uca_0900 && !info.binary && byte == 0)
                        {
                            0
                        } else {
                            if !info.binary && !info.case_sensitive {
                                byte = byte.to_ascii_lowercase();
                            }
                            u128::from(byte) + 1
                        }
                    }
                }
                (ColumnVector::Utf8 { offsets, data, .. }, Packed64Source::Utf8Small(info)) => {
                    let bytes = &data[offsets[row] as usize..offsets[row + 1] as usize];
                    let text = std::str::from_utf8(bytes).ok()?;
                    crate::charset::pack_short_collation_key_for_info(info, text)?
                }
                // Mismatch between detected source and current lane
                // means the schema actually changed across batches —
                // bail and the caller will rerun on the byte path.
                _ => return None,
            };
            key |= (v & col.mask) << col.shift;
        }
        Some(key)
    }
}

#[inline]
fn encode_utf8_one_packed_column(
    column: &ColumnVector,
    col: Packed64Col,
    row: usize,
) -> Option<u128> {
    let ColumnVector::Utf8 {
        offsets,
        data,
        nulls,
    } = column
    else {
        return None;
    };
    if nulls.as_ref().is_some_and(|valid| !valid.get(row)) {
        return Some(1u128 << col.null_bit_shift);
    }
    let Packed64Source::Utf8One(info) = col.source else {
        unreachable!()
    };
    let bytes = &data[offsets[row] as usize..offsets[row + 1] as usize];
    let mut byte = match bytes {
        [] => return Some(0),
        [byte] if byte.is_ascii() => *byte,
        _ => return None,
    };
    let value = if (info.pad_space && byte == b' ') || (info.uca_0900 && !info.binary && byte == 0)
    {
        0
    } else {
        if !info.binary && !info.case_sensitive {
            byte = byte.to_ascii_lowercase();
        }
        u128::from(byte) + 1
    };
    Some((value & col.mask) << col.shift)
}

/// Contiguous grouped state for fixed-width signed/temporal keys. This is an
/// internal layout of the same HashAggregate operator, not a query-shape fast
/// path: aggregate transitions, spill records, output schema and downstream
/// operators are shared with the canonical bucket layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactKeyKind {
    I64,
    Date32,
    Time64,
    DateTime64,
    Timestamp64,
}

impl CompactKeyKind {
    fn from_column(column: &ColumnVector) -> Option<Self> {
        Some(match column {
            ColumnVector::I64 { .. } => Self::I64,
            ColumnVector::Date32 { .. } => Self::Date32,
            ColumnVector::Time64 { .. } => Self::Time64,
            ColumnVector::DateTime64 { .. } => Self::DateTime64,
            ColumnVector::Timestamp64 { .. } => Self::Timestamp64,
            _ => return None,
        })
    }

    #[inline]
    fn raw_at(self, column: &ColumnVector, row: usize) -> Option<(i64, bool)> {
        let is_null = column.is_null(row);
        if is_null {
            return Some((0, true));
        }
        let raw = match (self, column) {
            (Self::I64, ColumnVector::I64 { data, .. }) => data[row],
            (Self::Date32, ColumnVector::Date32 { data, .. }) => i64::from(data[row]),
            (Self::Time64, ColumnVector::Time64 { data, .. })
            | (Self::DateTime64, ColumnVector::DateTime64 { data, .. })
            | (Self::Timestamp64, ColumnVector::Timestamp64 { data, .. }) => data[row],
            _ => return None,
        };
        Some((raw, false))
    }

    #[inline]
    fn to_value(self, raw: i64) -> Value {
        match self {
            Self::I64 => Value::Int(raw),
            Self::Date32 => Value::Date(raw as i32),
            Self::Time64 => Value::Time(raw),
            Self::DateTime64 => Value::DateTime(raw),
            Self::Timestamp64 => Value::Timestamp(raw),
        }
    }
}

fn fixed_width_initial_state(func: AggregateKind) -> InlineAggState {
    match func {
        AggregateKind::CountStar | AggregateKind::Count => InlineAggState::Count(0),
        AggregateKind::Sum => InlineAggState::Sum(NumericAggState::default()),
        AggregateKind::Avg => InlineAggState::Avg {
            state: NumericAggState::default(),
            n: 0,
        },
        AggregateKind::Min => InlineAggState::MinMax {
            is_min: true,
            best: None,
        },
        AggregateKind::Max => InlineAggState::MinMax {
            is_min: false,
            best: None,
        },
        _ => unreachable!("fixed_width aggregate eligibility checked"),
    }
}

struct FixedWidthStateArena {
    slots: Vec<u32>,
    mask: usize,
    hashes: Vec<u64>,
    keys: Vec<i64>,
    nulls: Vec<u8>,
    state_chunks: Vec<Vec<InlineAggState>>,
    /// Columnar state for the common fixed-width-key + single DECIMAL SUM
    /// shape. Keeping the common i64 magnitude and status in separate dense arrays
    /// avoids paying the 64-byte general-purpose enum (whose largest variant
    /// owns a `Value`) for every group. Exact overflow or an unexpected scale
    /// change promotes only that group to the canonical numeric state.
    decimal_state_columns: Vec<Option<NarrowDecimalStateColumn>>,
    decimal_state_slots: smallvec::SmallVec<[usize; 4]>,
    groups_per_chunk: usize,
    key_kinds: Vec<CompactKeyKind>,
    n_aggs: usize,
    funcs: Vec<AggregateKind>,
    dense_i64: Option<DenseI64Lookup>,
    /// Single-i64 inputs may arrive in nondecreasing key order (clustered
    /// scans and ordered indexes). Such groups never need lookup after their
    /// run closes, so append them without populating the open-addressing
    /// table. The first disorder materializes the complete canonical index
    /// once and permanently resumes the generic path.
    monotonic_i64_unindexed: bool,
    monotonic_i64_last: Option<(i64, u8, usize)>,
    partition_groups: [Vec<u32>; NUM_PARTITIONS],
    /// Parallel workers may defer duplicate elimination for the
    /// single-I64 + DECIMAL SUM layout. Input batches commonly contain
    /// ordered key runs even when an upstream exchange interleaves batches;
    /// appending one compact partial per run avoids building a worker-local
    /// hash table that the coordinator would immediately merge again.
    defer_i64_decimal_dedup: bool,
    /// Per-radix-partition ranges appended by each input batch. Every range is
    /// internally key-ordered; snapshotting sorts only these small run
    /// descriptors and concatenates non-overlapping runs, avoiding an O(G
    /// log G) comparison sort over all groups.
    deferred_partition_runs: [Vec<(u32, u32)>; NUM_PARTITIONS],
    /// Parallel single-I64 + DECIMAL SUM inputs are commonly ordered inside
    /// each storage batch even though the scheduler distributes those batches
    /// across workers. Keep one delta-coded run per batch instead of retaining
    /// a hash, radix id, full key and i64 state for every group. The
    /// coordinator sorts the run ranges and folds them once.
    compressed_i64_decimal_runs: Vec<CompressedI64DecimalRun>,
    compressed_i64_decimal_scale: Option<u8>,
    compressed_i64_decimal_allocated_bytes: usize,
}

const COMPRESSED_SUM_WIDE: u32 = u32::MAX;

struct CompressedI64DecimalRun {
    key_tokens: Vec<u8>,
    sum_tokens: Vec<u32>,
    wide_sums: Vec<i128>,
    first_key: (u8, i64),
    last_key: (u8, i64),
}

struct CompressedI64DecimalPartial {
    runs: Vec<CompressedI64DecimalRun>,
    scale: u8,
}

#[inline]
fn compressed_sort_key(key: i64, is_null: bool) -> (u8, i64) {
    (u8::from(!is_null), key)
}

#[inline]
fn push_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

#[inline]
fn read_varint(bytes: &[u8], cursor: &mut usize) -> u64 {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = bytes[*cursor];
        *cursor += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return value;
        }
        shift += 7;
    }
}

#[inline]
fn zigzag_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

#[inline]
fn unzigzag_i64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

impl CompressedI64DecimalRun {
    fn allocated_bytes(&self) -> usize {
        self.key_tokens
            .capacity()
            .saturating_add(
                self.sum_tokens
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                self.wide_sums
                    .capacity()
                    .saturating_mul(std::mem::size_of::<i128>()),
            )
    }

    fn cursor(&self) -> CompressedI64DecimalRunCursor<'_> {
        CompressedI64DecimalRunCursor {
            run: self,
            group: 0,
            key_cursor: 0,
            wide_cursor: 0,
            previous_key: 0,
        }
    }
}

struct CompressedI64DecimalRunCursor<'a> {
    run: &'a CompressedI64DecimalRun,
    group: usize,
    key_cursor: usize,
    wide_cursor: usize,
    previous_key: i64,
}

impl Iterator for CompressedI64DecimalRunCursor<'_> {
    type Item = (i64, bool, Option<i128>);

    fn next(&mut self) -> Option<Self::Item> {
        let &sum_token = self.run.sum_tokens.get(self.group)?;
        let key_token = read_varint(&self.run.key_tokens, &mut self.key_cursor);
        let (key, is_null) = if key_token == 0 {
            (0, true)
        } else if key_token == 1 {
            let end = self.key_cursor + std::mem::size_of::<i64>();
            let raw = i64::from_le_bytes(
                self.run.key_tokens[self.key_cursor..end]
                    .try_into()
                    .expect("compressed absolute I64 key has eight bytes"),
            );
            self.key_cursor = end;
            self.previous_key = raw;
            (raw, false)
        } else {
            let delta = unzigzag_i64(key_token - 2);
            let key = self.previous_key.wrapping_add(delta);
            self.previous_key = key;
            (key, false)
        };
        let sum = match sum_token {
            0 => None,
            COMPRESSED_SUM_WIDE => {
                let value = self.run.wide_sums[self.wide_cursor];
                self.wide_cursor += 1;
                Some(value)
            }
            token => Some(i128::from(unzigzag_i64(u64::from(token - 1)))),
        };
        self.group += 1;
        Some((key, is_null, sum))
    }
}

const COMPACT_SUM_EMPTY: u8 = 0;
const COMPACT_SUM_NARROW: u8 = 1;
const COMPACT_SUM_PROMOTED: u8 = 2;

struct NarrowDecimalStateColumn {
    scale: u8,
    // MySQL DECIMAL columns frequently use an i128 physical vector, but most
    // per-group subtotals fit in i64. Keeping that hot state narrow saves eight
    // resident bytes per group. The first wide input or checked-add overflow
    // promotes only that group to the exact arbitrary-precision oracle below.
    magnitudes: Vec<i64>,
    status: Vec<u8>,
    promoted: hashbrown::HashMap<u32, NumericAggState, ahash::RandomState>,
}

// A 64K-key chunk keeps the directory small while still making the common
// state two bytes per possible key.  Presence/value bitmaps preserve SQL's
// empty-vs-NULL distinction; values that do not fit the narrow lane promote
// individually to the exact numeric accumulator.
impl NarrowDecimalStateColumn {
    fn new(scale: u8, seed: usize) -> Self {
        Self {
            scale,
            magnitudes: Vec::with_capacity(seed),
            status: Vec::with_capacity(seed),
            promoted: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
        }
    }

    #[inline]
    fn push_group(&mut self) {
        self.magnitudes.push(0);
        self.status.push(COMPACT_SUM_EMPTY);
    }

    #[inline]
    fn update(&mut self, group: usize, magnitude: i128, scale: u8) {
        match self.status[group] {
            COMPACT_SUM_EMPTY if scale == self.scale => {
                if let Ok(magnitude) = i64::try_from(magnitude) {
                    self.magnitudes[group] = magnitude;
                    self.status[group] = COMPACT_SUM_NARROW;
                } else {
                    self.promote_and_add(group, magnitude, scale);
                }
            }
            COMPACT_SUM_NARROW if scale == self.scale => {
                if let Ok(magnitude) = i64::try_from(magnitude) {
                    if let Some(sum) = self.magnitudes[group].checked_add(magnitude) {
                        self.magnitudes[group] = sum;
                        return;
                    }
                }
                self.promote_and_add(group, magnitude, scale);
            }
            COMPACT_SUM_PROMOTED => {
                accumulate_numeric_state_decimal_parts(
                    self.promoted
                        .get_mut(&(group as u32))
                        .expect("promoted fixed_width SUM state exists"),
                    magnitude,
                    scale,
                );
            }
            COMPACT_SUM_EMPTY | COMPACT_SUM_NARROW => {
                // A typed DECIMAL schema normally has one stable scale. Keep
                // correctness if an upstream operator violates that contract:
                // promote just this group and let the shared decimal oracle
                // align scales exactly.
                self.promote_and_add(group, magnitude, scale);
            }
            _ => unreachable!("fixed_width SUM status is valid"),
        }
    }

    #[inline]
    fn update_value(&mut self, group: usize, value: &Value) -> Result<(), QueryError> {
        match value.exact_decimal() {
            Some(crate::util::decimal::ExactDecimal::I64(magnitude, scale)) => {
                self.update(group, i128::from(magnitude), scale);
            }
            Some(crate::util::decimal::ExactDecimal::I128(magnitude, scale)) => {
                self.update(group, magnitude, scale);
            }
            Some(crate::util::decimal::ExactDecimal::Big(_, _)) | None => {
                self.promote_and_add_value(group, value)?;
            }
        }
        Ok(())
    }

    #[cold]
    fn promote_and_add(&mut self, group: usize, magnitude: i128, scale: u8) {
        let mut state = NumericAggState::Empty;
        if self.status[group] == COMPACT_SUM_NARROW {
            accumulate_numeric_state_decimal_parts(
                &mut state,
                i128::from(self.magnitudes[group]),
                self.scale,
            );
        }
        accumulate_numeric_state_decimal_parts(&mut state, magnitude, scale);
        self.promoted.insert(group as u32, state);
        self.status[group] = COMPACT_SUM_PROMOTED;
    }

    #[cold]
    fn promote_and_add_value(&mut self, group: usize, value: &Value) -> Result<(), QueryError> {
        if self.status[group] == COMPACT_SUM_PROMOTED {
            return accumulate_numeric_state(
                self.promoted
                    .get_mut(&(group as u32))
                    .expect("promoted fixed_width SUM state exists"),
                value,
            );
        }
        let mut state = NumericAggState::Empty;
        if self.status[group] == COMPACT_SUM_NARROW {
            accumulate_numeric_state_decimal_parts(
                &mut state,
                i128::from(self.magnitudes[group]),
                self.scale,
            );
        }
        accumulate_numeric_state(&mut state, value)?;
        self.promoted.insert(group as u32, state);
        self.status[group] = COMPACT_SUM_PROMOTED;
        Ok(())
    }

    fn state(&self, group: usize) -> NumericAggState {
        match self.status[group] {
            COMPACT_SUM_EMPTY => NumericAggState::Empty,
            COMPACT_SUM_NARROW => {
                let mut state = NumericAggState::Empty;
                accumulate_numeric_state_decimal_parts(
                    &mut state,
                    i128::from(self.magnitudes[group]),
                    self.scale,
                );
                state
            }
            COMPACT_SUM_PROMOTED => self
                .promoted
                .get(&(group as u32))
                .expect("promoted fixed_width SUM state exists")
                .clone(),
            _ => unreachable!("fixed_width SUM status is valid"),
        }
    }

    fn allocated_bytes(&self) -> usize {
        self.magnitudes
            .capacity()
            .saturating_mul(std::mem::size_of::<i64>())
            .saturating_add(self.status.capacity())
            // Hashbrown owns control bytes in addition to key/value buckets.
            .saturating_add(
                self.promoted.capacity().saturating_mul(
                    std::mem::size_of::<(u32, NumericAggState)>().saturating_add(1),
                ),
            )
            .saturating_add(
                self.promoted
                    .values()
                    .map(numeric_state_heap_bytes)
                    .sum::<usize>(),
            )
    }
}

impl FixedWidthStateArena {
    fn try_new(group_columns: &[usize], aggs: &[AggregateSpec], batch: &Batch) -> Option<Self> {
        Self::try_new_internal(group_columns, aggs, batch)
    }

    fn try_new_internal(
        group_columns: &[usize],
        aggs: &[AggregateSpec],
        batch: &Batch,
    ) -> Option<Self> {
        if group_columns.is_empty()
            || group_columns.len() > 8
            || aggs.iter().any(|spec| {
                !matches!(
                    spec.func,
                    AggregateKind::CountStar
                        | AggregateKind::Count
                        | AggregateKind::Sum
                        | AggregateKind::Avg
                        | AggregateKind::Min
                        | AggregateKind::Max
                )
            })
        {
            return None;
        }
        let key_kinds = group_columns
            .iter()
            .map(|&column| CompactKeyKind::from_column(batch.columns().get(column)?))
            .collect::<Option<Vec<_>>>()?;
        let mut decimal_column_scales = Vec::with_capacity(aggs.len());
        for spec in aggs {
            let scale = if spec.func == AggregateKind::Sum {
                let column = batch.columns().get(spec.input_column)?;
                column.decimal_scale().or_else(|| {
                    match batch.schema().fields().get(spec.input_column)?.column_type {
                        ColumnType::Decimal { scale, .. } => Some(scale),
                        _ => None,
                    }
                })
            } else {
                None
            };
            decimal_column_scales.push(scale);
        }
        let aggregate = Self::new_with_decimal_columns(
            key_kinds,
            aggs.iter().map(|spec| spec.func).collect(),
            batch.visible_rows(),
            decimal_column_scales,
        );
        Some(aggregate)
    }

    fn has_decimal_state_column(aggs: &[AggregateSpec], batch: &Batch) -> bool {
        aggs.iter().any(|spec| {
            spec.func == AggregateKind::Sum
                && (batch
                    .columns()
                    .get(spec.input_column)
                    .is_some_and(|column| column.decimal_scale().is_some())
                    || matches!(
                        batch
                            .schema()
                            .fields()
                            .get(spec.input_column)
                            .map(|field| &field.column_type),
                        Some(ColumnType::Decimal { .. })
                    ))
        })
    }

    fn new(key_kinds: Vec<CompactKeyKind>, funcs: Vec<AggregateKind>, seed: usize) -> Self {
        let scales = vec![None; funcs.len()];
        Self::new_with_decimal_columns(key_kinds, funcs, seed, scales)
    }

    fn new_with_decimal_columns(
        key_kinds: Vec<CompactKeyKind>,
        funcs: Vec<AggregateKind>,
        seed: usize,
        decimal_column_scales: Vec<Option<u8>>,
    ) -> Self {
        let wanted = ((seed.max(1) * 10).saturating_add(6) / 7).max(1024);
        let capacity = wanted.next_power_of_two();
        let n_aggs = funcs.len();
        let monotonic_i64_unindexed = matches!(key_kinds.as_slice(), [CompactKeyKind::I64]);
        let decimal_state_columns = decimal_column_scales
            .into_iter()
            .map(|scale| scale.map(|scale| NarrowDecimalStateColumn::new(scale, seed)))
            .collect::<Vec<_>>();
        let decimal_state_slots = decimal_state_columns
            .iter()
            .enumerate()
            .filter_map(|(slot, column)| column.is_some().then_some(slot))
            .collect();
        Self {
            slots: vec![0; capacity],
            mask: capacity - 1,
            hashes: Vec::with_capacity(seed),
            keys: Vec::with_capacity(seed.saturating_mul(key_kinds.len())),
            nulls: Vec::with_capacity(seed),
            state_chunks: Vec::new(),
            decimal_state_columns,
            decimal_state_slots,
            groups_per_chunk: 8 * 1024,
            key_kinds,
            n_aggs,
            funcs,
            dense_i64: None,
            monotonic_i64_unindexed,
            monotonic_i64_last: None,
            partition_groups: std::array::from_fn(|_| Vec::new()),
            defer_i64_decimal_dedup: false,
            deferred_partition_runs: std::array::from_fn(|_| Vec::new()),
            compressed_i64_decimal_runs: Vec::new(),
            compressed_i64_decimal_scale: None,
            compressed_i64_decimal_allocated_bytes: 0,
        }
    }

    #[inline]
    fn hash_key(keys: &[i64], nullmask: u8) -> u64 {
        let mut hash =
            0x9e37_79b9_7f4a_7c15u64 ^ u64::from(nullmask).wrapping_mul(0xff51_afd7_ed55_8ccd);
        for &key in keys {
            let mut lane = key as u64;
            lane ^= lane >> 33;
            lane = lane.wrapping_mul(0xff51_afd7_ed55_8ccd);
            lane ^= lane >> 29;
            hash = (hash ^ lane).wrapping_mul(0x0000_0001_0000_01b3);
        }
        hash
    }

    #[inline]
    fn hash_i64(key: i64, nullmask: u8) -> u64 {
        let hash =
            0x9e37_79b9_7f4a_7c15u64 ^ u64::from(nullmask).wrapping_mul(0xff51_afd7_ed55_8ccd);
        let mut lane = key as u64;
        lane ^= lane >> 33;
        lane = lane.wrapping_mul(0xff51_afd7_ed55_8ccd);
        lane ^= lane >> 29;
        (hash ^ lane).wrapping_mul(0x0000_0001_0000_01b3)
    }

    fn grow(&mut self) {
        let new_len = self.slots.len().saturating_mul(2);
        let new_mask = new_len - 1;
        let mut slots = vec![0u32; new_len];
        for (group, &hash) in self.hashes.iter().enumerate() {
            let mut slot = hash as usize & new_mask;
            while slots[slot] != 0 {
                slot = (slot + 1) & new_mask;
            }
            slots[slot] = group as u32 + 1;
        }
        self.slots = slots;
        self.mask = new_mask;
    }

    fn materialize_monotonic_i64_index(&mut self) {
        let wanted = ((self.hashes.len().max(1) * 10).saturating_add(6) / 7).max(1024);
        let capacity = wanted.next_power_of_two();
        let mask = capacity - 1;
        let mut slots = vec![0u32; capacity];
        for (group, &hash) in self.hashes.iter().enumerate() {
            let mut slot = hash as usize & mask;
            while slots[slot] != 0 {
                slot = (slot + 1) & mask;
            }
            slots[slot] = group as u32 + 1;
        }
        self.slots = slots;
        self.mask = mask;
        self.monotonic_i64_unindexed = false;
        self.monotonic_i64_last = None;
    }

    fn materialize_compressed_i64_decimal_runs(&mut self) {
        // Retire deferred deduplication even if the first batch could not be
        // compressed. The fallback must hash arbitrary keys, not append an
        // unordered run that the coordinator would treat as sorted.
        self.defer_i64_decimal_dedup = false;
        let runs = std::mem::take(&mut self.compressed_i64_decimal_runs);
        self.compressed_i64_decimal_allocated_bytes = 0;
        let Some(scale) = self.compressed_i64_decimal_scale.take() else {
            return;
        };
        for run in &runs {
            for (key, key_is_null, sum) in run.cursor() {
                let group = self.probe_or_insert_i64(key, u8::from(key_is_null));
                if let Some(sum) = sum {
                    self.decimal_state_columns[0]
                        .as_mut()
                        .expect("compressed DECIMAL SUM retains its state column")
                        .update(group, sum, scale);
                }
            }
        }
    }

    #[inline]
    fn append_group_states(&mut self, group: usize) {
        for &slot in &self.decimal_state_slots {
            self.decimal_state_columns[slot]
                .as_mut()
                .expect("decimal state slot owns its column")
                .push_group();
        }
        // DECIMAL SUM slots own their complete state in the narrow columnar
        // store.  When every slot is columnar, allocating a parallel
        // `InlineAggState` matrix would retain a large array of permanently
        // unused `NumericAggState::Empty` values (and make capacity accounting
        // walk that array after every batch).  Mixed layouts keep the matrix
        // unchanged so aggregate indexes remain direct and branch-free.
        if self.decimal_state_slots.len() == self.n_aggs {
            return;
        }
        if group % self.groups_per_chunk == 0 {
            self.state_chunks.push(Vec::with_capacity(
                self.groups_per_chunk.saturating_mul(self.n_aggs),
            ));
        }
        self.state_chunks
            .last_mut()
            .expect("state chunk created for new group")
            .extend(self.funcs.iter().copied().map(fixed_width_initial_state));
    }

    #[inline]
    fn append_monotonic_i64_group(&mut self, key: i64, nullmask: u8) -> usize {
        let hash = Self::hash_i64(key, nullmask);
        let group = self.hashes.len();
        self.hashes.push(hash);
        self.partition_groups[(hash & PARTITION_MASK) as usize].push(group as u32);
        self.keys.push(key);
        self.nulls.push(nullmask);
        self.append_group_states(group);
        self.monotonic_i64_last = Some((key, nullmask, group));
        group
    }

    #[inline]
    fn append_deferred_i64_group(&mut self, key: i64, nullmask: u8) -> usize {
        let group = self.hashes.len();
        // Deferred runs are globally ordered before their single coordinator
        // merge, so hashing each first-seen group merely to scatter it across
        // 64 temporary vectors adds CPU and forces 64 snapshot/merge cycles.
        // Keep this private partial layout in partition zero; spill and final
        // merge still use the same exact state representation.
        self.hashes.push(0);
        self.partition_groups[0].push(group as u32);
        self.keys.push(key);
        self.nulls.push(nullmask);
        self.append_group_states(group);
        group
    }

    #[inline]
    fn probe_or_insert(&mut self, keys: &[i64], nullmask: u8) -> usize {
        if let [key] = keys {
            return self.probe_or_insert_i64(*key, nullmask);
        }
        let hash = Self::hash_key(keys, nullmask);
        let n_keys = self.key_kinds.len();
        let mut slot = hash as usize & self.mask;
        loop {
            let stored = self.slots[slot];
            if stored == 0 {
                let group = self.hashes.len();
                self.slots[slot] = group as u32 + 1;
                self.hashes.push(hash);
                self.partition_groups[(hash & PARTITION_MASK) as usize].push(group as u32);
                self.keys.extend_from_slice(keys);
                self.nulls.push(nullmask);
                self.append_group_states(group);
                if self.hashes.len() * 10 >= self.slots.len() * 7 {
                    self.grow();
                }
                // Dense lookup is installed only for a single I64 key, whose
                // probe returned through probe_or_insert_i64 above.
                return group;
            }
            let group = (stored - 1) as usize;
            if self.hashes[group] == hash
                && self.nulls[group] == nullmask
                && self.keys[group * n_keys..(group + 1) * n_keys] == *keys
            {
                return group;
            }
            slot = (slot + 1) & self.mask;
        }
    }

    #[inline]
    fn probe_or_insert_i64(&mut self, key: i64, nullmask: u8) -> usize {
        debug_assert_eq!(self.key_kinds.len(), 1);
        if self.monotonic_i64_unindexed {
            match self.monotonic_i64_last {
                Some((last_key, last_nullmask, group))
                    if key == last_key && nullmask == last_nullmask =>
                {
                    return group;
                }
                Some((last_key, 0, _)) if nullmask == 0 && key > last_key => {
                    return self.append_monotonic_i64_group(key, nullmask);
                }
                None => return self.append_monotonic_i64_group(key, nullmask),
                _ => self.materialize_monotonic_i64_index(),
            }
        }
        let mut retire_dense = false;
        let dense_index = self.dense_i64.as_mut().and_then(|lookup| {
            if nullmask != 0 {
                lookup.null_slot.map(|group| (None, group as usize))
            } else {
                match lookup.index_for(key) {
                    Some(index) => Some((Some(index), lookup.slots[index] as usize)),
                    None => {
                        retire_dense = true;
                        None
                    }
                }
            }
        });
        if retire_dense {
            self.dense_i64 = None;
        }
        if let Some((_, group)) = dense_index {
            if group as u32 != DENSE_I64_EMPTY {
                return group;
            }
        }

        let hash = Self::hash_i64(key, nullmask);
        let mut slot = hash as usize & self.mask;
        loop {
            let stored = self.slots[slot];
            if stored == 0 {
                let group = self.hashes.len();
                self.slots[slot] = group as u32 + 1;
                self.hashes.push(hash);
                self.partition_groups[(hash & PARTITION_MASK) as usize].push(group as u32);
                self.keys.push(key);
                self.nulls.push(nullmask);
                self.append_group_states(group);
                if self.hashes.len() * 10 >= self.slots.len() * 7 {
                    self.grow();
                }
                if let Some(lookup) = self.dense_i64.as_mut() {
                    if nullmask != 0 {
                        lookup.null_slot = Some(group as u32);
                    } else if let Some((Some(index), _)) = dense_index {
                        lookup.slots[index] = group as u32;
                    }
                }
                return group;
            }
            let group = (stored - 1) as usize;
            if self.hashes[group] == hash
                && self.nulls[group] == nullmask
                && self.keys[group] == key
            {
                return group;
            }
            slot = (slot + 1) & self.mask;
        }
    }

    fn consume_batch(
        &mut self,
        group_columns: &[usize],
        aggs: &[AggregateSpec],
        batch: &Batch,
    ) -> Result<(), QueryError> {
        // Ordered or clustered inputs commonly contain several adjacent rows
        // for one key. For the dense fixed-width-key + single-DECIMAL-SUM
        // layout, collapse each run before touching the hash table: equality
        // and aggregation remain exact, while one probe replaces N identical
        // probes. This applies equally to composite keys (for example an id +
        // date pair); random input simply produces one-row runs and retains
        // the canonical path's complexity. A selection can reorder rows, so
        // it uses the general visible-index loop below.
        if batch.selection().is_none()
            && aggs.len() == 1
            && self
                .decimal_state_columns
                .first()
                .is_some_and(Option::is_some)
        {
            let value_column = &batch.columns()[aggs[0].input_column];
            match value_column {
                ColumnVector::Decimal32 { data, scale, nulls } => {
                    self.consume_fixed_decimal_runs(
                        group_columns,
                        batch,
                        data,
                        nulls.as_ref(),
                        *scale,
                        |value| i128::from(value),
                    );
                    return Ok(());
                }
                ColumnVector::Decimal64 { data, scale, nulls } => {
                    self.consume_fixed_decimal_runs(
                        group_columns,
                        batch,
                        data,
                        nulls.as_ref(),
                        *scale,
                        |value| i128::from(value),
                    );
                    return Ok(());
                }
                ColumnVector::Decimal128 { data, scale, nulls } => {
                    self.consume_fixed_decimal_runs(
                        group_columns,
                        batch,
                        data,
                        nulls.as_ref(),
                        *scale,
                        |value| value,
                    );
                    return Ok(());
                }
                _ => {}
            }
        }
        // Selection vectors and non-fixed DECIMAL inputs bypass run encoding.
        // Merge any compressed prefix before entering the general hash path.
        if self.defer_i64_decimal_dedup {
            self.materialize_compressed_i64_decimal_runs();
        }
        if self.hashes.is_empty()
            && self.dense_i64.is_none()
            && !self.monotonic_i64_unindexed
            && matches!(self.key_kinds.as_slice(), [CompactKeyKind::I64])
        {
            self.dense_i64 = DenseI64Lookup::try_from_batch(group_columns, batch);
        }
        let mut keys = smallvec::SmallVec::<[i64; 8]>::with_capacity(self.key_kinds.len());
        for row in batch.visible_indices() {
            keys.clear();
            let mut nullmask = 0u8;
            for (key_index, (&column_index, &kind)) in
                group_columns.iter().zip(self.key_kinds.iter()).enumerate()
            {
                let (raw, is_null) = kind
                    .raw_at(&batch.columns()[column_index], row)
                    .expect("fixed_width aggregate schema is stable across batches");
                keys.push(raw);
                if is_null {
                    nullmask |= 1 << key_index;
                }
            }
            let group = self.probe_or_insert(&keys, nullmask);
            if self.decimal_state_slots.is_empty() {
                for (slot, spec) in aggs.iter().enumerate() {
                    if matches!(spec.func, AggregateKind::CountStar) {
                        self.state_mut(group, slot).update_count_star();
                        continue;
                    }
                    let column = &batch.columns()[spec.input_column];
                    if column.is_null(row) {
                        continue;
                    }
                    if let Some((data, scale)) = column.as_decimal_i32_slice() {
                        self.state_mut(group, slot)
                            .update_decimal_parts(i128::from(data[row]), scale)?;
                    } else if let Some((data, scale)) = column.as_decimal_i64_slice() {
                        self.state_mut(group, slot)
                            .update_decimal_parts(i128::from(data[row]), scale)?;
                    } else if let Some((data, scale)) = column.as_decimal_i128_slice() {
                        self.state_mut(group, slot)
                            .update_decimal_parts(data[row], scale)?;
                    } else {
                        self.state_mut(group, slot)
                            .update_ref(&column.value_at(row))?;
                    }
                }
                continue;
            }
            for (slot, spec) in aggs.iter().enumerate() {
                if let Some(decimal_state_column) = self.decimal_state_columns[slot].as_mut() {
                    let column = &batch.columns()[spec.input_column];
                    if column.is_null(row) {
                        continue;
                    }
                    if let Some((data, scale)) = column.as_decimal_i32_slice() {
                        decimal_state_column.update(group, i128::from(data[row]), scale);
                    } else if let Some((data, scale)) = column.as_decimal_i64_slice() {
                        decimal_state_column.update(group, i128::from(data[row]), scale);
                    } else if let Some((data, scale)) = column.as_decimal_i128_slice() {
                        decimal_state_column.update(group, data[row], scale);
                    } else {
                        decimal_state_column.update_value(group, &column.value_at(row))?;
                    }
                    continue;
                }
                self.update_inline_state_from_column(group, slot, spec, batch, row)?;
            }
        }
        Ok(())
    }

    #[inline]
    fn update_inline_state_from_column(
        &mut self,
        group: usize,
        slot: usize,
        spec: &AggregateSpec,
        batch: &Batch,
        row: usize,
    ) -> Result<(), QueryError> {
        if matches!(spec.func, AggregateKind::CountStar) {
            self.state_mut(group, slot).update_count_star();
            return Ok(());
        }
        let column = &batch.columns()[spec.input_column];
        if column.is_null(row) {
            return Ok(());
        }
        if let Some((data, scale)) = column.as_decimal_i32_slice() {
            self.state_mut(group, slot)
                .update_decimal_parts(i128::from(data[row]), scale)?;
        } else if let Some((data, scale)) = column.as_decimal_i64_slice() {
            self.state_mut(group, slot)
                .update_decimal_parts(i128::from(data[row]), scale)?;
        } else if let Some((data, scale)) = column.as_decimal_i128_slice() {
            self.state_mut(group, slot)
                .update_decimal_parts(data[row], scale)?;
        } else {
            self.state_mut(group, slot)
                .update_ref(&column.value_at(row))?;
        }
        Ok(())
    }

    fn consume_fixed_decimal_runs<T: Copy>(
        &mut self,
        group_columns: &[usize],
        batch: &Batch,
        values: &[T],
        value_nulls: Option<&Bitmap>,
        scale: u8,
        to_i128: impl Fn(T) -> i128,
    ) {
        debug_assert_eq!(batch.num_rows(), values.len());
        if let ([group_column], [CompactKeyKind::I64]) = (group_columns, self.key_kinds.as_slice())
        {
            if let ColumnVector::I64 {
                data: key_values,
                nulls: key_nulls,
            } = &batch.columns()[*group_column]
            {
                self.consume_i64_fixed_decimal_runs(
                    key_values,
                    key_nulls.as_ref(),
                    values,
                    value_nulls,
                    scale,
                    to_i128,
                );
                return;
            }
        }
        let mut keys = smallvec::SmallVec::<[i64; 8]>::with_capacity(self.key_kinds.len());
        let mut row = 0usize;
        while row < values.len() {
            keys.clear();
            let mut nullmask = 0u8;
            for (key_index, (&column_index, &kind)) in
                group_columns.iter().zip(self.key_kinds.iter()).enumerate()
            {
                let (raw, is_null) = kind
                    .raw_at(&batch.columns()[column_index], row)
                    .expect("fixed_width aggregate schema is stable across batches");
                keys.push(raw);
                if is_null {
                    nullmask |= 1 << key_index;
                }
            }

            let mut end = row + 1;
            'same_group: while end < values.len() {
                for (key_index, (&column_index, &kind)) in
                    group_columns.iter().zip(self.key_kinds.iter()).enumerate()
                {
                    let (raw, is_null) = kind
                        .raw_at(&batch.columns()[column_index], end)
                        .expect("fixed_width aggregate schema is stable across batches");
                    let prior_is_null = nullmask & (1 << key_index) != 0;
                    if is_null != prior_is_null || (!is_null && raw != keys[key_index]) {
                        break 'same_group;
                    }
                }
                end += 1;
            }

            let group = self.probe_or_insert(&keys, nullmask);
            let decimal_state_column = self.decimal_state_columns[0]
                .as_mut()
                .expect("run aggregation requires a DECIMAL SUM state column");
            let mut subtotal = 0i128;
            let mut seen = false;
            for value_row in row..end {
                if value_nulls.is_some_and(|valid| !valid.get(value_row)) {
                    continue;
                }
                let magnitude = to_i128(values[value_row]);
                if let Some(sum) = subtotal.checked_add(magnitude) {
                    subtotal = sum;
                    seen = true;
                } else {
                    // Preserve exactness on the rare run-local i128 overflow:
                    // flush the representable prefix, then let the ordinary
                    // state transition promote if the group total overflows.
                    if seen {
                        decimal_state_column.update(group, subtotal, scale);
                    }
                    subtotal = magnitude;
                    seen = true;
                }
            }
            if seen {
                decimal_state_column.update(group, subtotal, scale);
            }
            row = end;
        }
    }

    fn consume_i64_fixed_decimal_runs<T: Copy>(
        &mut self,
        keys: &[i64],
        key_nulls: Option<&Bitmap>,
        values: &[T],
        value_nulls: Option<&Bitmap>,
        scale: u8,
        to_i128: impl Fn(T) -> i128,
    ) {
        debug_assert_eq!(keys.len(), values.len());
        if self.defer_i64_decimal_dedup {
            let mut run = CompressedI64DecimalRun {
                // Four adjacent fact rows per key is a common analytical
                // shape. The small slack avoids a growth for boundary runs;
                // genuinely higher-cardinality batches grow normally and
                // remain exactly capacity-accounted.
                key_tokens: Vec::with_capacity(keys.len().saturating_div(4) + 16),
                sum_tokens: Vec::with_capacity(keys.len().saturating_div(4) + 16),
                wide_sums: Vec::new(),
                first_key: (0, 0),
                last_key: (0, 0),
            };
            let mut previous_encoded_key = 0i64;
            let mut previous_sort_key = None;
            let mut ordered = true;
            let mut row = 0usize;
            'compress: while row < values.len() {
                let key = keys[row];
                let key_is_null = key_nulls.is_some_and(|valid| !valid.get(row));
                let sort_key = compressed_sort_key(key, key_is_null);
                if previous_sort_key.is_some_and(|previous| sort_key < previous) {
                    ordered = false;
                    break;
                }
                if previous_sort_key.is_none() {
                    run.first_key = sort_key;
                }
                previous_sort_key = Some(sort_key);
                run.last_key = sort_key;

                if key_is_null {
                    push_varint(0, &mut run.key_tokens);
                } else if let Some(delta) = key.checked_sub(previous_encoded_key) {
                    let zigzag = zigzag_i64(delta);
                    if zigzag <= u64::MAX - 2 {
                        push_varint(zigzag + 2, &mut run.key_tokens);
                    } else {
                        push_varint(1, &mut run.key_tokens);
                        run.key_tokens.extend_from_slice(&key.to_le_bytes());
                    }
                    previous_encoded_key = key;
                } else {
                    push_varint(1, &mut run.key_tokens);
                    run.key_tokens.extend_from_slice(&key.to_le_bytes());
                    previous_encoded_key = key;
                }

                let mut end = row + 1;
                while end < values.len()
                    && key_nulls.is_some_and(|valid| !valid.get(end)) == key_is_null
                    && (key_is_null || keys[end] == key)
                {
                    end += 1;
                }
                let mut subtotal = 0i128;
                let mut seen = false;
                for value_row in row..end {
                    if value_nulls.is_some_and(|valid| !valid.get(value_row)) {
                        continue;
                    }
                    let magnitude = to_i128(values[value_row]);
                    let Some(next) = subtotal.checked_add(magnitude) else {
                        ordered = false;
                        break 'compress;
                    };
                    subtotal = next;
                    seen = true;
                }
                let token = if !seen {
                    0
                } else if let Ok(narrow) = i64::try_from(subtotal) {
                    let zigzag = zigzag_i64(narrow);
                    if zigzag <= u64::from(u32::MAX - 2) {
                        zigzag as u32 + 1
                    } else {
                        run.wide_sums.push(subtotal);
                        COMPRESSED_SUM_WIDE
                    }
                } else {
                    run.wide_sums.push(subtotal);
                    COMPRESSED_SUM_WIDE
                };
                run.sum_tokens.push(token);
                row = end;
            }
            let same_scale = self
                .compressed_i64_decimal_scale
                .is_none_or(|existing| existing == scale);
            if ordered && same_scale && !run.sum_tokens.is_empty() {
                self.compressed_i64_decimal_scale = Some(scale);
                let outer_before = self.compressed_i64_decimal_runs.capacity();
                let run_bytes = run.allocated_bytes();
                self.compressed_i64_decimal_runs.push(run);
                let outer_growth = self
                    .compressed_i64_decimal_runs
                    .capacity()
                    .saturating_sub(outer_before)
                    .saturating_mul(std::mem::size_of::<CompressedI64DecimalRun>());
                self.compressed_i64_decimal_allocated_bytes = self
                    .compressed_i64_decimal_allocated_bytes
                    .saturating_add(outer_growth)
                    .saturating_add(run_bytes);
                return;
            }
            // An arbitrary upstream may cease to be ordered inside a later
            // batch. Materialize the compact prefix once and resume the
            // canonical exact hash path for this and subsequent batches.
            self.materialize_compressed_i64_decimal_runs();
        }
        let partition_starts = self.defer_i64_decimal_dedup.then(|| {
            std::array::from_fn::<_, NUM_PARTITIONS, _>(|partition| {
                self.partition_groups[partition].len()
            })
        });
        let mut row = 0usize;
        while row < values.len() {
            let key = keys[row];
            let key_is_null = key_nulls.is_some_and(|valid| !valid.get(row));
            let mut end = row + 1;
            while end < values.len()
                && key_nulls.is_some_and(|valid| !valid.get(end)) == key_is_null
                && (key_is_null || keys[end] == key)
            {
                end += 1;
            }

            let group = if self.defer_i64_decimal_dedup {
                self.append_deferred_i64_group(key, u8::from(key_is_null))
            } else {
                self.probe_or_insert_i64(key, u8::from(key_is_null))
            };
            let decimal_state_column = self.decimal_state_columns[0]
                .as_mut()
                .expect("run aggregation requires a DECIMAL SUM state column");
            let mut subtotal = 0i128;
            let mut seen = false;
            for value_row in row..end {
                if value_nulls.is_some_and(|valid| !valid.get(value_row)) {
                    continue;
                }
                let magnitude = to_i128(values[value_row]);
                if let Some(sum) = subtotal.checked_add(magnitude) {
                    subtotal = sum;
                    seen = true;
                } else {
                    if seen {
                        decimal_state_column.update(group, subtotal, scale);
                    }
                    subtotal = magnitude;
                    seen = true;
                }
            }
            if seen {
                decimal_state_column.update(group, subtotal, scale);
            }
            row = end;
        }
        if let Some(starts) = partition_starts {
            for (partition, start) in starts.into_iter().enumerate() {
                let end = self.partition_groups[partition].len();
                if end != start {
                    self.deferred_partition_runs[partition].push((start as u32, end as u32));
                }
            }
        }
    }

    fn try_new_for_partial(
        batch: &Batch,
        group_count: usize,
        aggs: &[AggregateSpec],
    ) -> Option<Self> {
        if group_count == 0
            || group_count > 8
            || aggs.iter().any(|spec| {
                !matches!(
                    spec.func,
                    AggregateKind::CountStar
                        | AggregateKind::Count
                        | AggregateKind::Sum
                        | AggregateKind::Avg
                        | AggregateKind::Min
                        | AggregateKind::Max
                )
            })
        {
            return None;
        }
        let key_kinds = batch.columns()[..group_count]
            .iter()
            .map(CompactKeyKind::from_column)
            .collect::<Option<Vec<_>>>()?;
        let decimal_column_scales = vec![None; aggs.len()];
        Some(Self::new_with_decimal_columns(
            key_kinds,
            aggs.iter().map(|spec| spec.func).collect(),
            batch.num_rows(),
            decimal_column_scales,
        ))
    }

    fn merge_partial_batch(
        &mut self,
        batch: &Batch,
        group_count: usize,
        layout: &SpillLayout,
        aggs: &[AggregateSpec],
    ) -> Result<(), QueryError> {
        layout.validate_batch(batch)?;
        let mut keys = smallvec::SmallVec::<[i64; 8]>::with_capacity(group_count);
        for row in 0..batch.num_rows() {
            keys.clear();
            let mut nullmask = 0u8;
            for (key, kind) in self.key_kinds.iter().copied().enumerate() {
                let (raw, is_null) = kind
                    .raw_at(&batch.columns()[key], row)
                    .expect("fixed_width spill schema is stable");
                keys.push(raw);
                if is_null {
                    nullmask |= 1 << key;
                }
            }
            let group = self.probe_or_insert(&keys, nullmask);
            for (slot, layout_slot) in layout.slots.iter().enumerate() {
                crate::engine::expr::with_optional_compare_collation(
                    aggs[slot].comparison_collation,
                    || layout_slot.merge_fixed(self.state_mut(group, slot), batch.columns(), row),
                )?;
            }
        }
        Ok(())
    }

    fn allocated_bytes(&self) -> usize {
        self.slots
            .capacity()
            .saturating_mul(std::mem::size_of::<u32>())
            .saturating_add(
                self.hashes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
            .saturating_add(
                self.keys
                    .capacity()
                    .saturating_mul(std::mem::size_of::<i64>()),
            )
            .saturating_add(self.nulls.capacity())
            .saturating_add(
                self.dense_i64
                    .as_ref()
                    .map(DenseI64Lookup::allocated_bytes)
                    .unwrap_or(0),
            )
            .saturating_add(self.partition_groups.iter().fold(0usize, |bytes, groups| {
                bytes.saturating_add(groups.capacity().saturating_mul(std::mem::size_of::<u32>()))
            }))
            .saturating_add(
                self.deferred_partition_runs
                    .iter()
                    .fold(0usize, |bytes, runs| {
                        bytes.saturating_add(runs.capacity().saturating_mul(std::mem::size_of::<(
                            u32,
                            u32,
                        )>(
                        )))
                    }),
            )
            .saturating_add(self.compressed_i64_decimal_allocated_bytes)
            .saturating_add(self.state_chunks.iter().fold(0usize, |bytes, chunk| {
                bytes.saturating_add(
                    chunk
                        .capacity()
                        .saturating_mul(std::mem::size_of::<InlineAggState>()),
                )
            }))
            .saturating_add(
                self.state_chunks
                    .iter()
                    .flatten()
                    .map(InlineAggState::heap_bytes)
                    .sum::<usize>(),
            )
            .saturating_add(
                self.decimal_state_columns
                    .iter()
                    .flatten()
                    .map(NarrowDecimalStateColumn::allocated_bytes)
                    .sum(),
            )
    }

    fn group_key_values(&self, group: usize) -> Vec<Value> {
        let n_keys = self.key_kinds.len();
        let nullmask = self.nulls[group];
        (0..n_keys)
            .map(|key| {
                if nullmask & (1 << key) != 0 {
                    Value::Null
                } else {
                    self.key_kinds[key].to_value(self.keys[group * n_keys + key])
                }
            })
            .collect()
    }

    fn append_group_key_values(&self, group: usize, columns: &mut [Vec<Value>]) {
        let n_keys = self.key_kinds.len();
        let nullmask = self.nulls[group];
        for (key, column) in columns.iter_mut().enumerate().take(n_keys) {
            column.push(if nullmask & (1 << key) != 0 {
                Value::Null
            } else {
                self.key_kinds[key].to_value(self.keys[group * n_keys + key])
            });
        }
    }

    fn partitioned_groups(&self) -> (Vec<u32>, [usize; NUM_PARTITIONS + 1]) {
        let mut offsets = [0usize; NUM_PARTITIONS + 1];
        let mut groups = Vec::with_capacity(self.hashes.len());
        for (partition, partition_groups) in self.partition_groups.iter().enumerate() {
            groups.extend_from_slice(partition_groups);
            offsets[partition + 1] = groups.len();
        }
        (groups, offsets)
    }

    #[inline]
    fn state(&self, group: usize, aggregate: usize) -> &InlineAggState {
        let chunk = group / self.groups_per_chunk;
        let within = group % self.groups_per_chunk;
        &self.state_chunks[chunk][within * self.n_aggs + aggregate]
    }

    #[inline]
    fn state_mut(&mut self, group: usize, aggregate: usize) -> &mut InlineAggState {
        let chunk = group / self.groups_per_chunk;
        let within = group % self.groups_per_chunk;
        &mut self.state_chunks[chunk][within * self.n_aggs + aggregate]
    }

    fn finalized_value(&self, group: usize, aggregate: usize) -> Value {
        if let Some(decimal_state_column) = &self.decimal_state_columns[aggregate] {
            return numeric_state_to_value(&decimal_state_column.state(group));
        }
        self.state(group, aggregate).finalize_ref()
    }

    fn state_snapshot(&self, group: usize, aggregate: usize) -> AggStateSnapshot {
        if let Some(decimal_state_column) = &self.decimal_state_columns[aggregate] {
            let sum = decimal_state_column.state(group);
            return AggStateSnapshot {
                count: u64::from(!matches!(sum, NumericAggState::Empty)),
                sum,
                ..AggStateSnapshot::default()
            };
        }
        fixed_width_state_snapshot(self.state(group, aggregate))
    }
}

/// Finalize a DISTINCT aggregate from its dedup set, byte-for-byte
/// matching the row path (`CountDistinctAcc::finalize` /
/// `DistinctReducer<{SumAcc,AvgAcc}>::finalize`). An absent/empty set is
/// the empty-group case: COUNT → 0, SUM/AVG → NULL.
#[cfg(test)]
fn finalize_distinct(distinct: Option<&DistinctState>, func: AggregateKind) -> Value {
    let mut state = AggState::default();
    state.distinct = distinct.cloned().map(Box::new);
    state.finalize(func)
}

struct GroupBucket {
    key: Vec<u8>,
    key_values: Vec<Value>,
    aggs: Vec<AggState>,
    accounted_dynamic_bytes: usize,
}

impl GroupBucket {
    fn new(key: Vec<u8>, key_values: Vec<Value>, aggs: Vec<AggState>) -> Self {
        let mut bucket = Self {
            key,
            key_values,
            aggs,
            accounted_dynamic_bytes: 0,
        };
        bucket.accounted_dynamic_bytes = bucket.dynamic_bytes();
        bucket
    }

    fn dynamic_bytes(&self) -> usize {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        self.key
            .capacity()
            .saturating_add(
                self.key_values
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Value>()),
            )
            .saturating_add(
                self.key_values
                    .iter()
                    .map(AllocatedBytes::allocated_bytes)
                    .sum::<usize>(),
            )
            .saturating_add(
                self.aggs
                    .capacity()
                    .saturating_mul(std::mem::size_of::<AggState>()),
            )
            .saturating_add(
                self.aggs
                    .iter()
                    .map(AggState::allocated_bytes)
                    .sum::<usize>(),
            )
    }
}

#[derive(Default)]
struct Partition {
    by_key: GroupMap<usize>,
    buckets: Vec<GroupBucket>,
    resident_bytes: usize,
}

/// Lazily allocate the single bucket holding implicit-empty-GROUP-BY
/// accumulator state. Called once per partition the first time the
/// no-GROUP-BY fast path is invoked. Caller must pass the operator's
/// `aggs.len()` so the bucket allocates the right number of slots.
///
/// Taking a `&mut Partition` instead of a `&mut HashAggregateCore` lets the
/// hot-loop body keep its `&self.aggs` borrow alive across this call
/// (the borrow checker can split-borrow disjoint fields of `self` but
/// can't see through a `&mut self` method call).
#[inline]
fn ensure_implicit_single_bucket(partition: &mut Partition, aggs_len: usize) {
    if partition.buckets.is_empty() {
        let before = partition.structural_bytes();
        partition.buckets.push(GroupBucket::new(
            Vec::new(),
            Vec::new(),
            vec![AggState::default(); aggs_len],
        ));
        partition.by_key.insert(Vec::new(), 0);
        partition.record_insert(before, 0, 0);
    }
}

impl Partition {
    fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    fn structural_bytes(&self) -> usize {
        let mut bytes =
            self.by_key.capacity() * (HASH_ENTRY_OVERHEAD + std::mem::size_of::<usize>());
        bytes = bytes.saturating_add(
            self.buckets
                .capacity()
                .saturating_mul(BUCKET_FIXED_OVERHEAD),
        );
        bytes
    }

    fn recalculate_resident_bytes(&mut self) {
        self.resident_bytes = self.structural_bytes();
        self.resident_bytes = self
            .resident_bytes
            .saturating_add(self.by_key.keys().map(|key| key.capacity()).sum::<usize>());
        self.resident_bytes = self.resident_bytes.saturating_add(
            self.buckets
                .iter()
                .map(GroupBucket::dynamic_bytes)
                .sum::<usize>(),
        );
    }

    fn record_insert(
        &mut self,
        structural_bytes_before: usize,
        canonical_map_key_bytes: usize,
        bucket_idx: usize,
    ) {
        let structure_growth = self
            .structural_bytes()
            .saturating_sub(structural_bytes_before);
        self.resident_bytes = self
            .resident_bytes
            .saturating_add(structure_growth)
            .saturating_add(canonical_map_key_bytes)
            .saturating_add(self.buckets[bucket_idx].accounted_dynamic_bytes);
    }

    fn reconcile_bucket(&mut self, bucket_idx: usize) {
        let current = self.buckets[bucket_idx].dynamic_bytes();
        let previous = self.buckets[bucket_idx].accounted_dynamic_bytes;
        if current >= previous {
            self.resident_bytes = self.resident_bytes.saturating_add(current - previous);
        } else {
            self.resident_bytes = self.resident_bytes.saturating_sub(previous - current);
        }
        self.buckets[bucket_idx].accounted_dynamic_bytes = current;
    }

    #[cfg(debug_assertions)]
    fn estimated_bytes(&self) -> usize {
        let mut bytes = self.structural_bytes();
        bytes = bytes.saturating_add(self.by_key.keys().map(|key| key.capacity()).sum::<usize>());
        for b in &self.buckets {
            bytes = bytes.saturating_add(b.dynamic_bytes());
        }
        bytes
    }
}

/// Rebuild canonical lookup maps and defensively merge collation-equivalent
/// duplicates before a fast-layout fallback batch is consumed.
fn switch_partitions_to_canonical_lookup(partitions: &mut Vec<Partition>, aggs: &[AggregateSpec]) {
    let mut canonical = (0..NUM_PARTITIONS)
        .map(|_| Partition::default())
        .collect::<Vec<_>>();
    for mut old in std::mem::take(partitions) {
        for bucket in old.buckets.drain(..) {
            let pidx = (super::key_encode::fxhash(&bucket.key) & PARTITION_MASK) as usize;
            let target = &mut canonical[pidx];
            if let Some(&index) = target.by_key.get(&bucket.key) {
                let existing = &mut target.buckets[index];
                for (slot, spec) in aggs.iter().enumerate() {
                    merge_agg_state(&mut existing.aggs[slot], bucket.aggs[slot].clone(), spec);
                }
            } else {
                let index = target.buckets.len();
                target.by_key.insert(bucket.key.clone(), index);
                target.buckets.push(bucket);
            }
        }
    }
    for partition in &mut canonical {
        partition.recalculate_resident_bytes();
    }
    *partitions = canonical;
}

fn merge_agg_state(dst: &mut AggState, src: AggState, spec: &AggregateSpec) {
    crate::engine::expr::with_optional_compare_collation(spec.comparison_collation, || {
        dst.merge_from(src, spec.func)
    });
}

/// Per-operator metrics that get forwarded to `EXPLAIN ANALYZE` via
/// `analyze_stats::add_peak_memory` / `add_spill_bytes` / `set_streaming`
/// once the bridge knows the matching `PhysicalPlan` node.
#[derive(Debug, Clone, Copy, Default)]
pub struct AggMetrics {
    pub peak_memory_bytes: u64,
    pub spill_bytes: u64,
    pub spill_runs: u64,
    pub spill_passes: u64,
    pub spill_partitions: u64,
    pub did_spill: bool,
    pub is_streaming: bool,
    pub batches: u64,
    pub workers: u32,
    pub conversion_rows: u64,
    pub expression_mode: Option<&'static str>,
    pub native_expression_rows: u64,
    pub fallback_expression_rows: u64,
    pub aggregate_mode: Option<&'static str>,
    pub native_aggregate_rows: u64,
    pub fallback_aggregate_rows: u64,
    pub distinct_spill_bytes: u64,
}

pub struct HashAggregateCore {
    group_columns: Vec<usize>,
    aggs: Vec<AggregateSpec>,
    aggregate_plan: crate::engine::executor::hash_aggregate::AggregatePlan,
    group_key_program: Option<crate::engine::executor::hash_aggregate::GroupKeyProgram>,
    partitions: Vec<Partition>,

    // ---- Spill-related state ----
    input_schema: Option<Arc<BatchSchema>>,
    spill_schema: Option<Arc<BatchSchema>>,
    mem_budget: Option<Arc<MemBudget>>,
    /// PGA-style work-area handle. Drives the spill decision in
    /// concert with `mem_budget`: either gate can request a spill so
    /// the arbiter can pre-emptively shed memory under pool pressure
    /// even when the per-query budget still has headroom.
    memory_grant: Option<crate::engine::executor::memory_grant::OperatorMemoryGrant>,
    spill_root: PathBuf,
    spill_dir: Option<Arc<SpillTmpDir>>,
    spilled_paths: Vec<Vec<PathBuf>>,
    shared_spill: Option<Arc<SharedHashAggSpill>>,
    shared_spilled_partitions: Vec<bool>,
    next_segment_id: u64,
    spill_depth: usize,
    // ---- Metrics ----
    peak_mem_bytes: usize,
    spill_bytes_total: u64,
    is_streaming: bool,
    batches_consumed: u64,
    native_rows_consumed: u64,
    scalar_rows_consumed: u64,
    direct_join_input: bool,
    pub(crate) direct_build_rollup: bool,
    direct_join: DirectJoinState,
    // ---- Per-row scratch ----
    /// Reusable key-encoding buffer for `consume_row`. Re-using this
    /// across all rows in a worker pulls per-row allocation cost down
    /// from "Vec::with_capacity(64) per row" to "amortised zero" once
    /// the buffer has stabilised at the max group-key length seen.
    row_key_buf: Vec<u8>,
    /// Per-batch scratch: packed `(partition_idx << 24) | bucket_idx`
    /// for every visible row, one entry per visible row index. Reused
    /// across batches so the GROUP BY columnar-update pass doesn't
    /// allocate.
    row_bucket_scratch: Vec<u32>,
    /// Per-batch scratch: map from packed `(partition, bucket)` key to
    /// row indices in the current batch. Entries are removed between
    /// batches so high-cardinality queries retain at most one batch of
    /// group IDs. A small inline selection covers common short runs
    /// without making every hash-table slot hundreds of bytes wide.
    bucket_selection_index: hashbrown::HashMap<u32, usize, ahash::RandomState>,
    /// Selection buffers parallel to `bucket_selection_keys`. Capacities are
    /// retained across batches, so a stable low-cardinality grouping allocates
    /// once per operator rather than once per group per input batch.
    bucket_selections: Vec<smallvec::SmallVec<[u32; 8]>>,
    /// Per-batch scratch: list of bucket keys actually touched this
    /// batch. Used to iterate `bucket_selections` deterministically
    /// without paying for a `.values()` walk over stale entries from
    /// previous batches.
    bucket_selection_keys: Vec<u32>,

    // ---- B+2: packed group-key fast path ----
    /// One-shot detection result for the Packed64 fast path. `None` after
    /// detection means we tried and bailed (wide composite key, unsupported
    /// lane); the byte path runs forever in that case. `Some` once detected
    /// stays for the operator's lifetime — we don't redo detection across
    /// batches because that would require the new batch's schema to match,
    /// and a schema change mid-stream would corrupt `by_key` anyway.
    packed64_encoder: Option<Packed64Encoder>,
    /// Whether we've already attempted Packed64 detection. Detection
    /// needs a real first batch (its column variants determine the lane
    /// kind + Utf8 width), so we wait for the first non-empty batch
    /// before deciding. After that this stays `true`.
    packed64_probed: bool,
    dictionary_encoder: Option<DictionaryGroupEncoder>,
    dictionary_probed: bool,
    dictionary_repack_attempted: bool,
    dictionary_count: Option<DictionaryCountArena>,
    frozen_dictionary_encoder: Option<Arc<DictionaryGroupEncoder>>,
    /// Operator-local token/packed key → canonical `(partition, bucket)`.
    /// The packed key avoids canonical byte encoding on repeated rows; the
    /// bucket itself is always placed by the canonical key hash so workers
    /// using different internal layouts still agree on radix ownership.
    packed_group_lookup: hashbrown::HashMap<u128, u32, ahash::RandomState>,
    /// Tiny exact accelerator in front of `packed_group_lookup`. Analytical
    /// GROUP BYs with only a handful of keys avoid hashing every input row;
    /// overflow keys use the general table without changing partition
    /// ownership.
    packed_inline_lookup: smallvec::SmallVec<[(u128, u32); 8]>,
    dense_i64_lookup: Option<DenseI64Lookup>,
    dense_i64_probed: bool,
    /// Fixed-width key/state arena selected before the first non-empty batch.
    /// `fixed_width_probed` prevents a later batch from changing the physical
    /// state layout after input has already been consumed.
    fixed_width_probed: bool,
    fixed_width: Option<FixedWidthStateArena>,
    defer_parallel_i64_decimal_dedup: bool,
}

impl HashAggregateCore {
    pub(crate) fn unspilled_global_distinct_state(
        &self,
    ) -> Option<Option<&crate::engine::executor::hash_aggregate::DistinctState>> {
        if !self.group_columns.is_empty()
            || self.aggs.len() != 1
            || self.aggs[0].func != AggregateKind::CountDistinct
            || self.spilled_paths.iter().any(|paths| !paths.is_empty())
            || self
                .partitions
                .iter()
                .skip(1)
                .any(|partition| !partition.is_empty())
        {
            return None;
        }
        let partition = &self.partitions[0];
        if partition.buckets.is_empty() {
            return Some(None);
        }
        if partition.buckets.len() != 1 {
            return None;
        }
        Some(partition.buckets[0].aggs[0].distinct.as_deref())
    }

    /// A common packed layout with fresh token namespaces. Primitive and text
    /// dictionaries are aligned separately without changing their key layout.
    pub(crate) fn parallel_dictionary_layout(
        &self,
    ) -> Option<smallvec::SmallVec<[(u8, usize, u32, u32, u32); 8]>> {
        self.dictionary_count.as_ref()?;
        let encoder = self.dictionary_encoder.as_ref()?;
        if encoder.columns.len() > 8 || encoder._merge_token_charge.is_some() {
            return None;
        }
        let mut layout = smallvec::SmallVec::new();
        for column in &encoder.columns {
            let (tag, index) = match &column.source {
                DictionaryColumnSource::Utf8 {
                    column,
                    merge_tokens,
                    ..
                } if merge_tokens.capacity() == 0 => (0, *column),
                DictionaryColumnSource::I64 {
                    column,
                    merge_tokens,
                    ..
                } if merge_tokens.capacity() == 0 => (1, *column),
                DictionaryColumnSource::U64 {
                    column,
                    merge_tokens,
                    ..
                } if merge_tokens.capacity() == 0 => (2, *column),
                DictionaryColumnSource::Date32 {
                    column,
                    merge_tokens,
                    ..
                } if merge_tokens.capacity() == 0 => (3, *column),
                DictionaryColumnSource::Bool { column } => (4, *column),
                _ => return None,
            };
            layout.push((
                tag | if column.direct { 0x80 } else { 0 },
                index,
                column.shift,
                column.null_bit,
                column.token_mask,
            ));
        }
        Some(layout)
    }

    pub(crate) fn dictionary_token_count(&self, column: usize) -> usize {
        self.dictionary_encoder
            .as_ref()
            .and_then(|encoder| encoder.columns.get(column))
            .map_or(0, |column| match &column.source {
                DictionaryColumnSource::Utf8 { canonical_keys, .. } => canonical_keys.len(),
                DictionaryColumnSource::I64 { raw_values, .. } => raw_values.len(),
                DictionaryColumnSource::U64 { raw_values, .. } => raw_values.len(),
                DictionaryColumnSource::Date32 { raw_values, .. } => raw_values.len(),
                _ => 0,
            })
    }

    /// A terminal domain bound for choosing a merge layout. Dictionary lanes
    /// have their actual cardinality; raw lanes use their observed integer
    /// range, capped by group count. No new retained sampling state is needed.
    pub(crate) fn dictionary_primitive_domain(&self, position: usize) -> usize {
        let Some(encoder) = &self.dictionary_encoder else {
            return 0;
        };
        let Some(column) = encoder.columns.get(position) else {
            return 0;
        };
        if !column.direct {
            return self.dictionary_token_count(position);
        }
        let Some(arena) = &self.dictionary_count else {
            return 0;
        };
        let mut lo = i128::MAX;
        let mut hi = i128::MIN;
        let mut groups = 0usize;
        for partition in &arena.partitions {
            for &key in &partition.keys {
                if key & (1u128 << column.null_bit) != 0 {
                    continue;
                }
                let raw = (key >> column.shift) as u64;
                let raw = match column.source {
                    DictionaryColumnSource::Date32 { .. } => i128::from(raw as u32 as i32),
                    DictionaryColumnSource::Bool { .. } => i128::from(raw & 1),
                    DictionaryColumnSource::I64 { .. } => i128::from(raw as i64),
                    _ => i128::from(raw),
                };
                lo = lo.min(raw);
                hi = hi.max(raw);
                groups += 1;
            }
        }
        if groups == 0 {
            0
        } else {
            ((hi - lo + 1) as u128).min(groups as u128) as usize
        }
    }

    /// Repacking when observed domains warrant different primitive lanes.
    /// Canonical radix placement is unchanged; local key bits and lookup slots
    /// are rebuilt before shared token namespaces are installed.
    pub(crate) fn normalize_dictionary_layout(
        &mut self,
        target: &[(u8, usize, u32, u32, u32)],
    ) -> Result<bool, QueryError> {
        let Some(current) = self.parallel_dictionary_layout() else {
            return Ok(false);
        };
        if current.as_slice() == target {
            return Ok(true);
        }
        if current.len() != target.len()
            || current.iter().zip(target).any(|(old, new)| {
                old.0 & 0x7f != new.0 & 0x7f
                    || old.1 != new.1
                    || (old.0 & 0x7f == 4 && old.0 != new.0)
            })
            || self.spilled_paths.iter().any(|paths| !paths.is_empty())
            || self
                .shared_spilled_partitions
                .iter()
                .any(|spilled| *spilled)
        {
            return Ok(false);
        }
        let arena = self
            .dictionary_count
            .as_ref()
            .expect("layout owns compact state");
        if arena.direct_token_handles.is_some() {
            return Ok(false);
        }
        let groups = arena
            .partitions
            .iter()
            .map(|partition| partition.keys.len())
            .sum::<usize>();
        let mut bounds = [(i128::MAX, i128::MIN, false); 8];
        for (position, (old, new)) in current.iter().zip(target).enumerate() {
            if new.0 & 0x80 == 0
                && old.0 & 0x80 == 0
                && self.dictionary_token_count(position) > new.4 as usize
            {
                return Ok(false);
            }
        }
        for partition in &arena.partitions {
            for (index, &key) in partition.keys.iter().enumerate() {
                if index % 8192 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                for (position, (old, new)) in current.iter().zip(target).enumerate() {
                    if old.0 & 0x80 == 0 || new.0 & 0x80 != 0 || key & (1u128 << old.3) != 0 {
                        continue;
                    }
                    let value = match old.0 & 0x7f {
                        1 => i128::from((key >> old.2) as u64 as i64),
                        3 => i128::from((key >> old.2) as u32 as i32),
                        _ => i128::from((key >> old.2) as u64),
                    };
                    bounds[position].0 = bounds[position].0.min(value);
                    bounds[position].1 = bounds[position].1.max(value);
                    bounds[position].2 = true;
                }
            }
        }
        let mut extra = 0usize;
        for (position, bound) in bounds.iter().enumerate().take(target.len()) {
            if !bound.2 {
                continue;
            }
            let cardinality = ((bound.1 - bound.0 + 1) as u128).min(groups as u128);
            if cardinality > u128::from(target[position].4) {
                return Ok(false);
            }
            // Covers new primitive maps, raw-value capacities and their
            // transient growth, plus the bounded dense-I64 token cache.
            extra = extra
                .saturating_add((cardinality as usize).saturating_mul(128))
                .saturating_add(
                    2 * DenseI64TokenCache::MAX_SPAN * std::mem::size_of::<DictionaryToken>(),
                );
        }
        let Some(budget) = self.mem_budget.as_ref() else {
            return Ok(false);
        };
        let Ok(_charge) =
            crate::engine::executor::mem_budget::OwnedMemCharge::new(budget.clone(), extra)
        else {
            return Ok(false);
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let encoder = self
                .dictionary_encoder
                .as_mut()
                .expect("fresh dictionary encoder");
            let arena = self.dictionary_count.as_mut().expect("compact arena");
            for partition in &mut arena.partitions {
                for (index, key) in partition.keys.iter_mut().enumerate() {
                    if index % 8192 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let old_key = *key;
                    let mut repacked = 0u128;
                    for (position, column) in encoder.columns.iter_mut().enumerate() {
                        let new = target[position];
                        let direct = new.0 & 0x80 != 0;
                        let local =
                            ((old_key >> column.shift) & u128::from(column.token_mask)) as usize;
                        let null = if column.direct {
                            old_key & (1u128 << column.null_bit) != 0
                        } else {
                            local == 0
                        };
                        let bits = if null {
                            0
                        } else if column.direct == direct {
                            if !direct {
                                local as u128
                            } else if new.0 & 0x7f == 3 {
                                (old_key >> column.shift) as u32 as u128
                            } else if new.0 & 0x7f == 4 {
                                (old_key >> column.shift) & 1
                            } else {
                                (old_key >> column.shift) as u64 as u128
                            }
                        } else {
                            match &mut column.source {
                                DictionaryColumnSource::I64 {
                                    values,
                                    dense_tokens,
                                    raw_values,
                                    ..
                                } => {
                                    if direct {
                                        raw_values
                                            .get(local - 1)
                                            .copied()
                                            .map(|value| value as u64 as u128)
                                    } else {
                                        let raw = (old_key >> column.shift) as u64;
                                        dictionary_i64_token(
                                            values,
                                            dense_tokens,
                                            raw_values,
                                            raw as i64,
                                            raw as u128,
                                        )
                                        .map(|token| token.local as u128)
                                    }
                                }
                                DictionaryColumnSource::U64 {
                                    values, raw_values, ..
                                } => {
                                    if direct {
                                        raw_values.get(local - 1).copied().map(u128::from)
                                    } else {
                                        let raw = (old_key >> column.shift) as u64;
                                        dictionary_primitive_token(
                                            values,
                                            raw_values,
                                            raw,
                                            fx_mix_u128(raw as u128),
                                        )
                                        .map(|token| token.local as u128)
                                    }
                                }
                                DictionaryColumnSource::Date32 {
                                    values, raw_values, ..
                                } => {
                                    if direct {
                                        raw_values
                                            .get(local - 1)
                                            .copied()
                                            .map(|value| value as u32 as u128)
                                    } else {
                                        let raw = (old_key >> column.shift) as u32;
                                        dictionary_primitive_token(
                                            values,
                                            raw_values,
                                            raw as i32,
                                            fx_mix_u128(raw as u128),
                                        )
                                        .map(|token| token.local as u128)
                                    }
                                }
                                _ => None,
                            }
                            .ok_or_else(|| {
                                crate::error::catalog::ER_INTERNAL_ERROR
                                    .error("invalid dictionary token during layout normalization")
                            })?
                        };
                        repacked |= bits << new.2;
                        if null && direct {
                            repacked |= 1u128 << new.3;
                        }
                    }
                    *key = repacked;
                }
                // Preserve the complete arena invariant, even though the
                // next operation normally transfers this partition to merge.
                if partition.slots.is_empty() {
                    continue;
                }
                partition.slots.fill(0);
                for (group, &key) in partition.keys.iter().enumerate() {
                    let mut slot = fx_mix_u128(key) as usize & partition.mask;
                    while partition.slots[slot] != 0 {
                        slot = (slot + 1) & partition.mask;
                    }
                    partition.slots[slot] = group as u32 + 1;
                }
            }
            for (column, descriptor) in encoder.columns.iter_mut().zip(target) {
                let direct = descriptor.0 & 0x80 != 0;
                if direct && !column.direct {
                    match &mut column.source {
                        DictionaryColumnSource::I64 {
                            values,
                            dense_tokens,
                            raw_values,
                            ..
                        } => {
                            *values = Default::default();
                            *dense_tokens = None;
                            *raw_values = Vec::new();
                        }
                        DictionaryColumnSource::U64 {
                            values, raw_values, ..
                        } => {
                            *values = Default::default();
                            *raw_values = Vec::new();
                        }
                        DictionaryColumnSource::Date32 {
                            values, raw_values, ..
                        } => {
                            *values = Default::default();
                            *raw_values = Vec::new();
                        }
                        _ => {}
                    }
                }
                column.direct = direct;
                column.shift = descriptor.2;
                column.null_bit = descriptor.3;
                column.token_mask = descriptor.4;
            }
            self.update_dictionary_count_peak_and_maybe_spill()?;
            Ok::<_, QueryError>(())
        }))
        .unwrap_or_else(|_| {
            Err(crate::error::catalog::ER_GET_ERRNO
                .error("dictionary layout normalization worker panicked"))
        });
        if result.is_err() {
            // Release all newly allocated payloads before the temporary
            // reservation on this stack can be refunded during unwinding.
            self.dictionary_count = None;
            self.dictionary_encoder = None;
        }
        result.map(|()| self.parallel_dictionary_layout().as_deref() == Some(target))
    }

    pub(crate) fn visit_dictionary_tokens(
        &self,
        column: usize,
        mut visit: impl FnMut(&Arc<[u8]>, u64),
    ) {
        let Some(column) = self
            .dictionary_encoder
            .as_ref()
            .and_then(|encoder| encoder.columns.get(column))
        else {
            return;
        };
        if let DictionaryColumnSource::Utf8 {
            canonical_to_token, ..
        } = &column.source
        {
            for (key, token) in canonical_to_token {
                visit(key, token.stable_hash);
            }
        }
    }

    pub(crate) fn reserve_dictionary_merge_tokens(&mut self) -> Result<(), QueryError> {
        let Some(encoder) = self.dictionary_encoder.as_mut() else {
            return Ok(());
        };
        let bytes = encoder
            .columns
            .iter()
            .filter(|column| !column.direct)
            .map(|column| match &column.source {
                DictionaryColumnSource::Utf8 { canonical_keys, .. } => canonical_keys
                    .len()
                    .saturating_mul(std::mem::size_of::<u32>()),
                DictionaryColumnSource::I64 { raw_values, .. } => {
                    raw_values.len().saturating_mul(4)
                }
                DictionaryColumnSource::U64 { raw_values, .. } => {
                    raw_values.len().saturating_mul(4)
                }
                DictionaryColumnSource::Date32 { raw_values, .. } => {
                    raw_values.len().saturating_mul(4)
                }
                _ => 0,
            })
            .sum();
        let budget = self
            .mem_budget
            .as_ref()
            .expect("parallel dictionary state is spill capable");
        let charge =
            crate::engine::executor::mem_budget::OwnedMemCharge::new(budget.clone(), bytes)?;
        for column in &mut encoder.columns {
            if column.direct {
                continue;
            }
            match &mut column.source {
                DictionaryColumnSource::Utf8 {
                    canonical_keys,
                    merge_tokens,
                    ..
                } => *merge_tokens = vec![0; canonical_keys.len()],
                DictionaryColumnSource::I64 {
                    raw_values,
                    merge_tokens,
                    ..
                } => *merge_tokens = vec![0; raw_values.len()],
                DictionaryColumnSource::U64 {
                    raw_values,
                    merge_tokens,
                    ..
                } => *merge_tokens = vec![0; raw_values.len()],
                DictionaryColumnSource::Date32 {
                    raw_values,
                    merge_tokens,
                    ..
                } => *merge_tokens = vec![0; raw_values.len()],
                DictionaryColumnSource::Bool { .. } => {}
            }
        }
        encoder.merge_token_bytes = bytes;
        encoder._merge_token_charge = Some(charge);
        Ok(())
    }

    /// The build phase has ended. Lookup slots and batch-local probes are not
    /// needed by token alignment, spill serialization, or final output. Drop
    /// their allocations before requesting merge memory, then return exactly
    /// that released capacity to the shared work area.
    pub(crate) fn retire_dictionary_build_lookups(&mut self) -> Result<(), QueryError> {
        let Some(arena) = self.dictionary_count.as_mut() else {
            return Ok(());
        };
        for partition in &mut arena.partitions {
            partition.slots = Vec::new();
            partition.mask = 0;
        }
        arena.direct_token_handles = None;
        arena.batch_frequencies = None;
        self.update_dictionary_count_peak_and_maybe_spill()
    }

    pub(crate) fn visit_primitive_dictionary_values(
        &self,
        position: usize,
        mut visit: impl FnMut(u64),
    ) {
        let Some(column) = self
            .dictionary_encoder
            .as_ref()
            .and_then(|encoder| encoder.columns.get(position))
        else {
            return;
        };
        match &column.source {
            DictionaryColumnSource::I64 { raw_values, .. } => {
                for &value in raw_values {
                    visit(value as u64);
                }
            }
            DictionaryColumnSource::U64 { raw_values, .. } => {
                for &value in raw_values {
                    visit(value);
                }
            }
            DictionaryColumnSource::Date32 { raw_values, .. } => {
                for &value in raw_values {
                    visit(value as u32 as u64);
                }
            }
            _ => {}
        }
    }

    pub(crate) fn install_primitive_dictionary_tokens(
        &mut self,
        position: usize,
        registry: &hashbrown::HashMap<u64, u32, ahash::RandomState>,
    ) {
        let Some(column) = self
            .dictionary_encoder
            .as_mut()
            .and_then(|encoder| encoder.columns.get_mut(position))
        else {
            return;
        };
        macro_rules! install {
            ($values:expr, $tokens:expr, $cast:expr) => {
                for (value, token) in $values.iter().zip($tokens.iter_mut()) {
                    *token = registry[&($cast)(*value)];
                }
            };
        }
        match &mut column.source {
            DictionaryColumnSource::I64 {
                raw_values,
                merge_tokens,
                ..
            } => install!(raw_values, merge_tokens, |value: i64| value as u64),
            DictionaryColumnSource::U64 {
                raw_values,
                merge_tokens,
                ..
            } => install!(raw_values, merge_tokens, |value: u64| value),
            DictionaryColumnSource::Date32 {
                raw_values,
                merge_tokens,
                ..
            } => install!(raw_values, merge_tokens, |value: i32| value as u32 as u64),
            _ => {}
        }
    }

    pub(crate) fn install_dictionary_merge_tokens(
        &mut self,
        column: usize,
        shards: &[hashbrown::HashMap<Arc<[u8]>, u32, U128BuildHasher>],
        offsets: &[u32],
    ) {
        let Some(column) = self
            .dictionary_encoder
            .as_mut()
            .and_then(|encoder| encoder.columns.get_mut(column))
        else {
            return;
        };
        if let DictionaryColumnSource::Utf8 {
            canonical_to_token,
            merge_tokens,
            ..
        } = &mut column.source
        {
            for (key, token) in canonical_to_token.iter() {
                let shard = token.stable_hash as usize & (shards.len() - 1);
                let (_, local) = shards[shard]
                    .raw_entry()
                    .from_hash(token.stable_hash, |candidate| {
                        candidate.as_ref() == key.as_ref()
                    })
                    .expect("every canonical key was interned in its shard");
                merge_tokens[token.local as usize - 1] = offsets[shard] + local;
            }
        }
    }

    pub(crate) fn spill_dictionary_for_merge(&mut self) -> Result<(), QueryError> {
        if self.dictionary_count.is_some() {
            self.do_spill_dictionary_count()?;
            self.update_dictionary_count_peak_and_maybe_spill()?;
        }
        Ok(())
    }

    fn ensure_empty_implicit_group(&mut self) {
        if !self.group_columns.is_empty()
            || self
                .partitions
                .iter()
                .any(|partition| !partition.is_empty())
            || self.spilled_paths.iter().any(|paths| !paths.is_empty())
        {
            return;
        }
        ensure_implicit_single_bucket(&mut self.partitions[0], self.aggs.len());
        self.partitions[0].reconcile_bucket(0);
    }

    pub fn new(group_columns: Vec<usize>, aggs: Vec<AggregateSpec>) -> Self {
        let mut partitions = Vec::with_capacity(NUM_PARTITIONS);
        for _ in 0..NUM_PARTITIONS {
            partitions.push(Partition::default());
        }
        let spilled_paths = (0..NUM_PARTITIONS).map(|_| Vec::new()).collect();
        let aggregate_plan = crate::engine::executor::hash_aggregate::AggregatePlan::new(
            group_columns.clone(),
            aggs.clone(),
        );
        Self {
            group_columns,
            aggs,
            aggregate_plan,
            group_key_program: None,
            partitions,
            input_schema: None,
            spill_schema: None,
            mem_budget: None,
            // `new` cannot spill safely because it has no query owner/root.
            // Such an operator must not advertise itself as spillable to the
            // global arbiter; production spill-capable callers use
            // `with_budget`, which installs the handle below.
            memory_grant: None,
            spill_root: default_spill_root(),
            spill_dir: None,
            spilled_paths,
            shared_spill: None,
            shared_spilled_partitions: vec![false; NUM_PARTITIONS],
            next_segment_id: 0,
            spill_depth: 0,
            peak_mem_bytes: 0,
            spill_bytes_total: 0,
            is_streaming: false,
            batches_consumed: 0,
            native_rows_consumed: 0,
            scalar_rows_consumed: 0,
            direct_join_input: false,
            direct_build_rollup: false,
            direct_join: DirectJoinState::default(),
            row_key_buf: Vec::with_capacity(64),
            row_bucket_scratch: Vec::new(),
            bucket_selection_index: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
            bucket_selections: Vec::new(),
            bucket_selection_keys: Vec::new(),
            packed64_encoder: None,
            packed64_probed: false,
            dictionary_encoder: None,
            dictionary_probed: false,
            dictionary_repack_attempted: false,
            dictionary_count: None,
            frozen_dictionary_encoder: None,
            packed_group_lookup: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
            packed_inline_lookup: smallvec::SmallVec::new(),
            dense_i64_lookup: None,
            dense_i64_probed: false,
            fixed_width_probed: false,
            fixed_width: None,
            defer_parallel_i64_decimal_dedup: false,
        }
    }

    /// Enable the parallel partial-state layout that appends ordered I64 key
    /// runs and lets the coordinator perform the one required deduplication.
    /// Other key/state layouts ignore this flag.
    pub(crate) fn enable_parallel_i64_decimal_dedup(&mut self) {
        self.defer_parallel_i64_decimal_dedup = true;
    }

    /// Deferred ordered runs live in partition zero, while workers that
    /// encounter disorder use radix partitions. Normalize a mixed handoff
    /// before any partition is merged, so equal keys cannot be finalized
    /// independently in different partitions. Keep the all-ordered fast path.
    pub(crate) fn align_parallel_partitioning<'a>(
        workers: impl IntoIterator<Item = &'a mut Self>,
    ) -> Result<(), QueryError> {
        let mut workers = workers.into_iter().collect::<smallvec::SmallVec<[_; 16]>>();
        let mut deferred = false;
        let mut partitioned = false;
        for worker in &workers {
            if worker.native_rows_consumed == 0 && worker.scalar_rows_consumed == 0 {
                continue;
            }
            if worker
                .fixed_width
                .as_ref()
                .is_some_and(|arena| arena.defer_i64_decimal_dedup)
            {
                deferred = true;
            } else {
                partitioned = true;
            }
        }
        if deferred && partitioned {
            for worker in &mut workers {
                if let Some(arena) = worker.fixed_width.as_mut() {
                    if arena.defer_i64_decimal_dedup {
                        arena.materialize_compressed_i64_decimal_runs();
                        worker.update_fixed_width_peak_and_maybe_spill()?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn align_parallel_dictionary_tokens(
        &mut self,
        registries: &mut Vec<hashbrown::HashMap<Arc<[u8]>, u32, ahash::RandomState>>,
    ) {
        if self.dictionary_count.is_some() {
            self.dictionary_encoder
                .as_mut()
                .expect("compact dictionary state retains its encoder")
                .align_merge_tokens(registries);
        }
    }

    /// Build a spill-capable HashAgg. When `mem_budget.should_spill()`
    /// returns true at the end of a batch, the operator picks the
    /// largest partition and writes it to a temp file under
    /// `<data_dir>/tmp/hashagg-<pid>-<seq>/`. On `finalize`, spilled
    /// partitions are re-read and merged with the in-memory residue.
    pub fn with_budget(
        group_columns: Vec<usize>,
        aggs: Vec<AggregateSpec>,
        input_schema: Arc<BatchSchema>,
        mem_budget: Arc<MemBudget>,
        data_dir: impl Into<PathBuf>,
    ) -> Self {
        let mut s = Self::new(group_columns, aggs);
        s.configure_spill(input_schema, mem_budget, data_dir.into());
        // Both query and shared-pool admission charge actual resident bytes.
        s.memory_grant = Some(
            crate::engine::executor::memory_grant::OperatorMemoryGrant::with_budget(
                s.mem_budget
                    .as_ref()
                    .expect("spill configuration installs a memory budget")
                    .clone(),
            ),
        );
        s
    }

    fn configure_spill(
        &mut self,
        input_schema: Arc<BatchSchema>,
        mem_budget: Arc<MemBudget>,
        spill_root: PathBuf,
    ) {
        self.bind_comparison_domains(&input_schema);
        let spill_schema = build_spill_schema(&input_schema, &self.group_columns, &self.aggs);
        self.group_key_program = crate::engine::executor::hash_aggregate::GroupKeyProgram::compile(
            &self.group_columns,
            &input_schema,
        );
        self.input_schema = Some(input_schema);
        self.spill_schema = Some(Arc::new(spill_schema));
        self.mem_budget = Some(mem_budget);
        self.spill_root = spill_root;
        self.is_streaming = true;
    }

    /// Build a spill-capable worker that participates in one physical
    /// operator-level grant. Parallel workers must not register independent
    /// work areas: their hash tables are shards of one HashAggregate and must
    /// compete/borrow inside the same reservation.
    pub(crate) fn with_budget_and_work_area(
        group_columns: Vec<usize>,
        aggs: Vec<AggregateSpec>,
        input_schema: Arc<BatchSchema>,
        mem_budget: Arc<MemBudget>,
        data_dir: impl Into<PathBuf>,
        work_area: Arc<crate::engine::executor::work_area::WorkAreaHandle>,
    ) -> Self {
        let mut aggregate = Self::new(group_columns, aggs);
        aggregate.configure_spill(input_schema, mem_budget.clone(), data_dir.into());
        aggregate.memory_grant = Some(
            crate::engine::executor::memory_grant::OperatorMemoryGrant::with_budget_and_work_area(
                mem_budget, work_area,
            ),
        );
        aggregate
    }

    pub(crate) fn with_shared_spill(mut self, spill: Arc<SharedHashAggSpill>) -> Self {
        self.shared_spill = Some(spill);
        self
    }

    /// Number of partitions inside this operator. Workers in the
    /// parallel-aggregate path use this to iterate their per-partition
    /// state and hand it off to the main thread for cross-worker merge.
    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    /// Set the input schema (used to derive the spill / partial-state
    /// schema) on an operator built via `new`. Idempotent. Useful for
    /// the parallel worker path which builds the operator without a
    /// memory budget but still needs to emit partials.
    fn bind_comparison_domains(&mut self, schema: &BatchSchema) {
        let changed = bind_aggregate_comparison_domains(&mut self.aggs, schema);
        if changed {
            self.aggregate_plan = crate::engine::executor::hash_aggregate::AggregatePlan::new(
                self.group_columns.clone(),
                self.aggs.clone(),
            );
        }
    }

    pub fn set_input_schema(&mut self, schema: Arc<BatchSchema>) {
        if self.input_schema.is_none() {
            self.bind_comparison_domains(&schema);
            let spill_schema = build_spill_schema(&schema, &self.group_columns, &self.aggs);
            self.group_key_program =
                crate::engine::executor::hash_aggregate::GroupKeyProgram::compile(
                    &self.group_columns,
                    &schema,
                );
            self.spill_schema = Some(Arc::new(spill_schema));
            self.input_schema = Some(schema);
        }
    }

    /// Snapshot of operator metrics for forwarding into EXPLAIN ANALYZE.
    pub fn metrics(&self) -> AggMetrics {
        let private_spill_partitions = self
            .spilled_paths
            .iter()
            .filter(|paths| !paths.is_empty())
            .count() as u64;
        let shared_spill_partitions = self
            .shared_spilled_partitions
            .iter()
            .filter(|spilled| **spilled)
            .count() as u64;
        let spill_partitions = private_spill_partitions + shared_spill_partitions;
        AggMetrics {
            peak_memory_bytes: self.peak_mem_bytes as u64,
            spill_bytes: self.spill_bytes_total,
            spill_runs: self.next_segment_id,
            spill_passes: if self.next_segment_id > 0 {
                self.spill_depth as u64 + 1
            } else {
                0
            },
            spill_partitions,
            did_spill: spill_partitions > 0,
            is_streaming: self.is_streaming,
            batches: self.batches_consumed,
            workers: 1,
            conversion_rows: 0,
            expression_mode: None,
            native_expression_rows: 0,
            fallback_expression_rows: 0,
            aggregate_mode: Some(if self.direct_join_input {
                "join_matches"
            } else if self.group_columns.is_empty() {
                "typed_batch"
            } else {
                "grouped_typed"
            }),
            native_aggregate_rows: self.native_rows_consumed,
            fallback_aggregate_rows: self.scalar_rows_consumed,
            distinct_spill_bytes: if self.aggs.iter().any(|spec| spec.func.uses_distinct_state()) {
                self.spill_bytes_total
            } else {
                0
            },
        }
    }

    pub fn consume_stream(&mut self, stream: &mut dyn BatchOperator) -> Result<(), QueryError> {
        while let Some(batch) = stream.next_batch()? {
            // Cooperative KILL QUERY / max_execution_time point: the build
            // phase can ingest millions of rows before it ever yields a
            // result batch, so check once per input batch.
            crate::engine::cancel::check_cancelled()?;
            self.consume_batch(&batch)?;
        }
        Ok(())
    }

    pub fn consume_batch(&mut self, batch: &Batch) -> Result<(), QueryError> {
        self.batches_consumed = self.batches_consumed.saturating_add(1);
        let visible_rows = batch.visible_rows() as u64;
        // `new` is also used by serial callers. Capture the schema before
        // touching rows so output metadata never depends on the constructor
        // chosen by the driver.
        self.set_input_schema(batch.schema().clone());
        let scalar_mode = matches!(
            crate::engine::executor::vec::config::effective_mode(),
            crate::engine::executor::vec::config::VecMode::Off
        );
        if scalar_mode {
            self.scalar_rows_consumed = self.scalar_rows_consumed.saturating_add(visible_rows);
        } else {
            self.native_rows_consumed = self.native_rows_consumed.saturating_add(visible_rows);
        }
        // Single-implicit-group fast path: skip key encoding + bucket
        // lookup entirely and fold whole columns through the SIMD lanes
        // in `sum_decimal_column_with_nulls`. The selection-free guard
        // is so we don't have to project the lane slice through a row
        // index list — that takes a separate, less-hot path.
        if self.group_columns.is_empty() && batch.selection().is_none() {
            ensure_implicit_single_bucket(&mut self.partitions[0], self.aggs.len());
            let bucket = &mut self.partitions[0].buckets[0];
            let n_rows = batch.num_rows();
            if scalar_mode {
                // Scalar is a kernel choice inside the same batch pipeline.
                // Drive the canonical per-value transition while retaining
                // batch ownership: there is no temporary Vec<Value> per row,
                // and capacity accounting is reconciled once per batch.
                for row in 0..n_rows {
                    for (slot, spec) in self.aggs.iter().enumerate() {
                        if matches!(spec.func, AggregateKind::CountStar) {
                            bucket.aggs[slot].count = bucket.aggs[slot].count.saturating_add(1);
                        } else {
                            update_state(
                                &mut bucket.aggs[slot],
                                spec,
                                &batch.columns()[spec.input_column],
                                row,
                            )?;
                        }
                    }
                }
                self.partitions[0].reconcile_bucket(0);
                return self.update_peak_and_maybe_spill();
            }
            for &slot in self.aggregate_plan.count_star_slots() {
                let state = &mut bucket.aggs[slot];
                state.count = state.count.saturating_add(n_rows as u64);
            }
            for input_group in self.aggregate_plan.input_groups() {
                let col = &batch.columns()[input_group.input_column];
                if input_group.slots.len() > 1
                    && update_shared_primitive_lane(
                        &mut bucket.aggs,
                        &self.aggs,
                        &input_group.slots,
                        col,
                        None,
                    )
                {
                    continue;
                }
                for &slot in &input_group.slots {
                    update_state_from_lane(&mut bucket.aggs[slot], &self.aggs[slot], col, None)?;
                }
            }
            self.partitions[0].reconcile_bucket(0);
            return self.update_peak_and_maybe_spill();
        }

        // Select the contiguous layout once, before consuming the first
        // grouped batch. Parallel workers export the same raw partition
        // snapshots and append spill batches to their operator-level shared
        // spill owner, so layout selection never changes the pipeline graph.
        if !scalar_mode && !self.fixed_width_probed && batch.visible_rows() != 0 {
            self.fixed_width_probed = true;
            // The fixed_width layout is the one-pass/spill layout for constrained
            // work areas. A wide work area keeps the canonical radix buckets,
            // whose dense-key column kernel is faster when it can remain fully
            // resident. This choice is fixed at open from the configured query
            // quota; later pressure can only trigger spill and never changes
            // the layout or topology mid-query.
            const COMPACT_WORK_AREA_LIMIT: usize = 256 * 1024 * 1024;
            let fixed_width_layout =
                // Key-only GROUP BY has no accumulator lane to benefit from
                // the generic bucket kernel. Keep its compact set layout even
                // with a large grant instead of allocating a Vec per group.
                self.aggs.is_empty()
                    || FixedWidthStateArena::has_decimal_state_column(&self.aggs, batch)
                    || self
                        .mem_budget
                        .as_ref()
                        .is_none_or(|budget| budget.limit() <= COMPACT_WORK_AREA_LIMIT);
            if fixed_width_layout {
                self.fixed_width =
                    FixedWidthStateArena::try_new(&self.group_columns, &self.aggs, batch);
                if self.defer_parallel_i64_decimal_dedup {
                    if let Some(arena) = self.fixed_width.as_mut() {
                        arena.defer_i64_decimal_dedup =
                            matches!(arena.key_kinds.as_slice(), [CompactKeyKind::I64])
                                && arena.n_aggs == 1
                                && arena.decimal_state_columns[0].is_some();
                    }
                }
            }
        }
        if let Some(fixed_width) = self.fixed_width.as_mut() {
            fixed_width.consume_batch(&self.group_columns, &self.aggs, batch)?;
            return self.update_fixed_width_peak_and_maybe_spill();
        }

        let visible_rows = batch.visible_rows();
        // A compact dictionary key may devote fewer than 32 bits to a token
        // when a wide composite GROUP BY is packed into u128. Never let a
        // token overflow happen after part of a batch has already updated the
        // COUNT arena: close and spill the current epoch at the batch boundary
        // instead. The same physical aggregate then starts a fresh bounded
        // epoch, so memory-dependent layout choices cannot change SQL
        // correctness.
        if self.dictionary_count.is_some()
            && self
                .dictionary_encoder
                .as_ref()
                .is_some_and(|encoder| !encoder.can_admit_batch(visible_rows))
        {
            self.do_spill_dictionary_count()?;
        }
        // A mixed fixed-width + low-cardinality UTF-8 key with COUNT-only
        // state admits a substantially denser layout than general
        // `GroupBucket`. Restrict this to spill-capable parallel aggregates:
        // their partition handoff can move the compact state directly to the
        // finalize workers, while serial/no-budget callers retain the common
        // canonical implementation.
        if self.shared_spill.is_some()
            && !self.dictionary_probed
            && visible_rows != 0
            && self
                .aggs
                .iter()
                .all(|spec| matches!(spec.func, AggregateKind::CountStar | AggregateKind::Count))
        {
            self.dictionary_probed = true;
            self.dictionary_encoder = DictionaryGroupEncoder::try_build(&self.group_columns, batch);
            if let Some(encoder) = self.dictionary_encoder.as_ref().filter(|encoder| {
                encoder.compact_count_eligible && encoder.can_admit_batch(visible_rows)
            }) {
                self.dictionary_count = Some(DictionaryCountArena::new(
                    visible_rows,
                    0,
                    self.aggs.len(),
                    encoder.is_single_utf8(),
                ));
            }
        }
        if let Some(arena) = self.dictionary_count.as_mut() {
            arena.consume_batch(
                self.dictionary_encoder
                    .as_mut()
                    .expect("compact dictionary state retains its encoder"),
                &self.aggs,
                batch,
                &mut self.row_key_buf,
            )?;
            self.repack_growing_primitive_dictionary()?;
            return self.update_dictionary_count_peak_and_maybe_spill();
        }
        // Generic buckets own keys, per-group Vecs and optional boxed
        // variance/distinct state. A full vector batch can create tens of MiB
        // per worker before the old end-of-batch spill check. Bound that
        // transition independently of the producer's batch size; compact
        // fixed-width/COUNT kernels above keep their vector fast paths.
        let per_group = std::mem::size_of::<GroupBucket>()
            .saturating_add(
                self.aggs
                    .len()
                    .saturating_mul(std::mem::size_of::<AggState>() + 64),
            )
            .saturating_add(
                self.group_columns
                    .len()
                    .saturating_mul(std::mem::size_of::<Value>() + 64),
            )
            .saturating_add(128);
        let transition_rows = self
            .mem_budget
            .as_ref()
            .filter(|_| self.spill_schema.is_some())
            .map(|budget| {
                let workers = crate::planner::default_parallel_degree().max(1) as usize;
                let bytes = (budget.limit() / workers / 8).max(32 * 1024);
                (bytes / per_group.max(1)).max(1)
            })
            .unwrap_or(usize::MAX);
        if visible_rows <= transition_rows {
            return self.consume_generic_batch(batch, scalar_mode);
        }
        let mut visible = batch.visible_indices();
        loop {
            let indices = visible
                .by_ref()
                .take(transition_rows)
                .map(|row| row as u32)
                .collect::<Vec<_>>();
            if indices.is_empty() {
                break;
            }
            let chunk = Batch {
                schema: batch.schema.clone(),
                columns: batch.columns.clone(),
                num_rows: batch.num_rows(),
                selection: Some(super::batch::Selection::Indices(indices)),
            };
            self.consume_generic_batch(&chunk, scalar_mode)?;
        }
        Ok(())
    }

    fn consume_generic_batch(
        &mut self,
        batch: &Batch,
        scalar_mode: bool,
    ) -> Result<(), QueryError> {
        let visible_rows = batch.visible_rows();
        // Two-phase GROUP BY: first map every visible row to its
        // (partition, bucket); then group rows by destination bucket and
        // fold each bucket's column slice through the SIMD lane in
        // `sum_decimal_column_with_selection`. The per-row legacy path
        // calls `update_state(state, spec, col, r)` per (row, slot),
        // matching on `AggregateKind` + lane discriminant per row and slot.
        // With this rewrite each (bucket, slot) pair pays one
        // lane-typed loop over a `selection: &[u32]` instead, which the
        // optimizer turns into a tight `acc += data[r] as i128`.
        //
        // For low-cardinality inputs the per-batch mapping cost is amortised
        // across repeated aggregate updates. For high-cardinality GROUP BY
        // (approximately one row per bucket) the
        // selection paths degrade to a single-element loop, which is
        // close to the legacy per-row path; the bucket-mapping overhead
        // and bookkeeping `Vec` are the price paid by that case to keep
        // a single, predictable code path.
        self.row_bucket_scratch.clear();
        self.row_bucket_scratch.reserve(visible_rows);

        // A single bounded I64 domain uses a direct slot lookup while the
        // canonical buckets remain in the same radix partitions. Probe once;
        // a later range expansion beyond the bounded work area permanently
        // continues through Packed64 without replaying input.
        if !self.dense_i64_probed && !self.group_columns.is_empty() && visible_rows != 0 {
            self.dense_i64_probed = true;
            self.dense_i64_lookup = DenseI64Lookup::try_from_batch(&self.group_columns, batch);
        }

        let dense_new_buckets = if let (Some(lookup), [group_column]) = (
            self.dense_i64_lookup.as_mut(),
            self.group_columns.as_slice(),
        ) {
            build_row_bucket_scratch_dense_i64(
                &mut self.partitions,
                lookup,
                *group_column,
                self.aggs.len(),
                batch,
                &mut self.row_bucket_scratch,
                &mut self.row_key_buf,
            )?
        } else {
            None
        };
        let dense_succeeded = dense_new_buckets.is_some();
        if !dense_succeeded && self.dense_i64_lookup.is_some() {
            self.dense_i64_lookup = None;
            // Range expansion can fail after mapping a prefix of this batch.
            // Packed64 retries every visible row, so discard that prefix;
            // otherwise aggregate updates pair later rows with earlier keys.
            // The canonical buckets created so far are still valid and empty.
            self.row_bucket_scratch.clear();
        }

        // B+2: probe the Packed64 fast path on the first non-empty batch.
        // Detection is one-shot — once it bails to the byte path the
        // operator stays there for the rest of its lifetime (a schema
        // change mid-stream would corrupt `by_key`'s composite-byte
        // encoding regardless). Empty GROUP BY (implicit single group
        // reached via a selection mask) keeps the byte path so it lands
        // in partition 0; that's a 1-bucket case anyway and the encoder
        // would just add overhead.
        if !self.packed64_probed && !self.group_columns.is_empty() && visible_rows != 0 {
            self.packed64_probed = true;
            self.packed64_encoder = Packed64Encoder::try_build(&self.group_columns, batch);
        }

        // Packed64 hot loop: one u64 key per row + a `HashMap<u64, usize>`
        // probe per partition, replacing the `Vec<u8>` encoder + fxhash +
        // byte-slice probe. A lane mismatch falls through to the byte path
        // and restarts the same batch.
        let packed_was_configured = self.packed64_encoder.is_some();
        let packed_new_buckets = if dense_succeeded {
            None
        } else if let Some(encoder) = self.packed64_encoder.as_ref() {
            build_row_bucket_scratch_packed64(
                &mut self.partitions,
                &mut self.packed_group_lookup,
                &mut self.packed_inline_lookup,
                &encoder,
                &self.group_columns,
                self.aggs.len(),
                batch,
                batch.visible_indices(),
                &mut self.row_bucket_scratch,
                &mut self.row_key_buf,
            )?
        } else {
            None
        };
        let packed_succeeded = packed_new_buckets.is_some();
        if !dense_succeeded && !packed_succeeded && self.packed64_encoder.is_some() {
            // Stay on the canonical map after a lane mismatch. Repartition
            // before processing this batch because the packed and canonical
            // hashes do not necessarily choose the same radix partition.
            switch_partitions_to_canonical_lookup(&mut self.partitions, &self.aggs);
            self.packed_group_lookup.clear();
            self.packed_inline_lookup.clear();
            self.packed64_encoder = None;
        }

        if !dense_succeeded
            && !packed_succeeded
            && !packed_was_configured
            && !self.dictionary_probed
            && visible_rows != 0
        {
            self.dictionary_probed = true;
            self.dictionary_encoder = DictionaryGroupEncoder::try_build(&self.group_columns, batch);
        }
        let dictionary_new_buckets =
            if !dense_succeeded && !packed_succeeded && !packed_was_configured {
                if let Some(encoder) = self.dictionary_encoder.as_mut() {
                    build_row_bucket_scratch_dictionary(
                        &mut self.partitions,
                        &mut self.packed_group_lookup,
                        &mut self.packed_inline_lookup,
                        encoder,
                        &self.group_columns,
                        self.aggs.len(),
                        batch,
                        &mut self.row_bucket_scratch,
                        &mut self.row_key_buf,
                    )?
                } else {
                    None
                }
            } else {
                None
            };
        let dictionary_succeeded = dictionary_new_buckets.is_some();
        if !dense_succeeded
            && !packed_succeeded
            && !dictionary_succeeded
            && self.dictionary_encoder.is_some()
        {
            switch_partitions_to_canonical_lookup(&mut self.partitions, &self.aggs);
            self.packed_group_lookup.clear();
            self.packed_inline_lookup.clear();
            self.dictionary_encoder = None;
        }
        let mut new_buckets = dense_new_buckets
            .or(packed_new_buckets)
            .or(dictionary_new_buckets)
            .unwrap_or(0);

        if !dense_succeeded && !packed_succeeded && !dictionary_succeeded {
            self.row_bucket_scratch.clear();
            let mut previous_row_and_bucket: Option<(usize, u32)> = None;
            for r in batch.visible_indices() {
                if let Some((previous_row, bucket)) = previous_row_and_bucket {
                    if self.group_columns.iter().all(|&column| {
                        column_rows_physically_equal(&batch.columns()[column], previous_row, r)
                    }) {
                        self.row_bucket_scratch.push(bucket);
                        previous_row_and_bucket = Some((r, bucket));
                        continue;
                    }
                }
                self.group_key_program
                    .as_ref()
                    .expect("input schema compiles the canonical group-key program")
                    .encode_row(batch, r, &mut self.row_key_buf);
                let h = super::key_encode::fxhash(&self.row_key_buf);
                // Implicit single group (empty GROUP BY reached with a
                // selection mask) must land in partition 0, matching the
                // selection-free fast path and the consume_row paths —
                // otherwise the same logical group splits across two
                // partitions and the cross-worker merge emits two rows.
                let pidx = if self.group_columns.is_empty() {
                    0
                } else {
                    (h & PARTITION_MASK) as usize
                };
                let partition = &mut self.partitions[pidx];
                let bucket_idx = if let Some(&i) = partition.by_key.get(&self.row_key_buf[..]) {
                    i
                } else {
                    // Vacant: build the bucket's persistent `key_values`
                    // once via `value_at`. This is the only path where the
                    // Value-materialisation cost is paid.
                    let i = partition.buckets.len();
                    let structural_before = partition.structural_bytes();
                    let canonical_key = self.row_key_buf.clone();
                    let canonical_key_bytes = canonical_key.capacity();
                    let mut key_values = Vec::with_capacity(
                        self.group_columns.len().saturating_add(self.aggs.len()),
                    );
                    key_values.extend(
                        self.group_columns
                            .iter()
                            .map(|&c| batch.columns()[c].value_at(r)),
                    );
                    partition.buckets.push(GroupBucket::new(
                        self.row_key_buf.clone(),
                        key_values,
                        vec![AggState::default(); self.aggs.len()],
                    ));
                    partition.by_key.insert(canonical_key, i);
                    partition.record_insert(structural_before, canonical_key_bytes, i);
                    new_buckets += 1;
                    i
                };
                let packed_bucket = packed_bucket_handle(pidx, bucket_idx)?;
                self.row_bucket_scratch.push(packed_bucket);
                previous_row_and_bucket = Some((r, packed_bucket));
            }
        }

        debug_assert_eq!(self.row_bucket_scratch.len(), visible_rows);

        // Kernel routing is based solely on the resolved batch cardinality
        // and typed-lane capability.  Query/aggregate shapes do not own
        // separate execution paths: both modes update the same buckets and
        // states, and differ only in whether a per-group selection is built.
        let high_cardinality = new_buckets.saturating_mul(8) >= visible_rows.saturating_mul(7)
            || batch_is_high_cardinality(&self.row_bucket_scratch);
        let estimated_touched_groups = if high_cardinality {
            visible_rows
        } else {
            // The sampler proved fewer than 7/8 distinct.  Use the group-wise
            // admission boundary as a conservative estimate; the actual
            // touched count is materialised only after this decision.
            visible_rows.saturating_div(8).max(1)
        };
        let update_mode = crate::engine::executor::hash_aggregate::choose_update_mode(
            batch,
            estimated_touched_groups,
            self.aggregate_plan.aggregates(),
        );
        if update_mode == crate::engine::executor::hash_aggregate::AggregateUpdateMode::RowWise {
            let dynamic_state_can_grow = self.aggs.iter().any(|spec| {
                matches!(
                    spec.func,
                    AggregateKind::Min
                        | AggregateKind::Max
                        | AggregateKind::CountDistinct
                        | AggregateKind::SumDistinct
                        | AggregateKind::AvgDistinct
                ) || spec.func.is_bit()
                    || spec.func.variance_flags().is_some()
            });
            for (key, row) in self
                .row_bucket_scratch
                .iter()
                .copied()
                .zip(batch.visible_indices())
            {
                let pidx = (key >> 24) as usize;
                let bucket_idx = (key & 0x00FF_FFFF) as usize;
                let bucket = &mut self.partitions[pidx].buckets[bucket_idx];
                for (slot, spec) in self.aggs.iter().enumerate() {
                    if matches!(spec.func, AggregateKind::CountStar) {
                        bucket.aggs[slot].count = bucket.aggs[slot].count.saturating_add(1);
                        continue;
                    }
                    let col = &batch.columns()[spec.input_column];
                    update_state(&mut bucket.aggs[slot], spec, col, row)?;
                }
                if dynamic_state_can_grow {
                    self.partitions[pidx].reconcile_bucket(bucket_idx);
                }
            }
            return self.update_peak_and_maybe_spill();
        }

        // Group visible rows by destination bucket. Retain the map's table
        // allocation, but drop last batch's entries: bucket IDs refer to
        // persistent groups and would otherwise grow this scratch map to
        // total query cardinality.
        self.bucket_selection_keys.clear();
        self.bucket_selection_index.clear();
        for selection in &mut self.bucket_selections {
            selection.clear();
        }
        for (i, r) in batch.visible_indices().enumerate() {
            let key = self.row_bucket_scratch[i];
            let selection_index = if self.bucket_selection_index.is_empty() {
                if let Some(index) = self
                    .bucket_selection_keys
                    .iter()
                    .position(|candidate| *candidate == key)
                {
                    index
                } else if self.bucket_selection_keys.len() < 8 {
                    let index = self.bucket_selection_keys.len();
                    self.bucket_selection_keys.push(key);
                    index
                } else {
                    self.bucket_selection_index.extend(
                        self.bucket_selection_keys
                            .iter()
                            .copied()
                            .enumerate()
                            .map(|(index, key)| (key, index)),
                    );
                    let index = self.bucket_selection_keys.len();
                    self.bucket_selection_index.insert(key, index);
                    self.bucket_selection_keys.push(key);
                    index
                }
            } else if let Some(&index) = self.bucket_selection_index.get(&key) {
                index
            } else {
                let index = self.bucket_selection_keys.len();
                self.bucket_selection_index.insert(key, index);
                self.bucket_selection_keys.push(key);
                index
            };
            if selection_index == self.bucket_selections.len() {
                self.bucket_selections.push(smallvec::SmallVec::new());
            }
            self.bucket_selections[selection_index].push(r as u32);
        }

        for (selection_index, &key) in self.bucket_selection_keys.iter().enumerate() {
            let pidx = (key >> 24) as usize;
            let bucket_idx = (key & 0x00FF_FFFF) as usize;
            let selection = &self.bucket_selections[selection_index];
            let bucket = &mut self.partitions[pidx].buckets[bucket_idx];
            for &slot in self.aggregate_plan.count_star_slots() {
                bucket.aggs[slot].count = bucket.aggs[slot]
                    .count
                    .saturating_add(selection.len() as u64);
            }
            for input_group in self.aggregate_plan.input_groups() {
                let col = &batch.columns()[input_group.input_column];
                if !scalar_mode
                    && input_group.slots.len() > 1
                    && update_shared_primitive_lane(
                        &mut bucket.aggs,
                        &self.aggs,
                        &input_group.slots,
                        col,
                        Some(selection.as_slice()),
                    )
                {
                    continue;
                }
                for &slot in &input_group.slots {
                    if scalar_mode {
                        for &row in selection {
                            update_state(
                                &mut bucket.aggs[slot],
                                &self.aggs[slot],
                                col,
                                row as usize,
                            )?;
                        }
                    } else {
                        update_state_from_lane(
                            &mut bucket.aggs[slot],
                            &self.aggs[slot],
                            col,
                            Some(selection.as_slice()),
                        )?;
                    }
                }
            }
            self.partitions[pidx].reconcile_bucket(bucket_idx);
        }

        self.update_peak_and_maybe_spill()?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn retain_sampled_dictionary_layout_for_test(&mut self) {
        self.dictionary_repack_attempted = true;
    }

    fn repack_growing_primitive_dictionary(&mut self) -> Result<(), QueryError> {
        if self.dictionary_repack_attempted {
            return Ok(());
        }
        let Some(layout) = self.parallel_dictionary_layout() else {
            return Ok(());
        };
        // Wait for a full batch's worth of distinct primitive keys, rather
        // than replacing one noisy prefix estimate with another. Repack at
        // most once per epoch and well before the dictionary reaches its grant.
        if !layout.iter().enumerate().any(|(column, desc)| {
            matches!(desc.0, 1..=3) && self.dictionary_token_count(column) >= DEFAULT_BATCH_ROWS
        }) {
            return Ok(());
        }
        self.dictionary_repack_attempted = true;
        let domains = (0..layout.len())
            .map(|column| self.dictionary_primitive_domain(column))
            .collect::<smallvec::SmallVec<[usize; 8]>>();
        let target = preferred_dictionary_layout(&layout, &domains);
        if target != layout {
            self.normalize_dictionary_layout(&target)?;
        }
        Ok(())
    }

    fn update_dictionary_count_peak_and_maybe_spill(&mut self) -> Result<(), QueryError> {
        let total = self
            .dictionary_count
            .as_ref()
            .map(DictionaryCountArena::allocated_bytes)
            .unwrap_or(0)
            .saturating_add(
                self.dictionary_encoder
                    .as_ref()
                    .map(DictionaryGroupEncoder::allocated_bytes)
                    .unwrap_or(0),
            );
        self.peak_mem_bytes = self.peak_mem_bytes.max(total);
        let Some(grant) = self.memory_grant.as_mut() else {
            return Ok(());
        };
        use crate::engine::executor::memory_grant::GrantResize;
        match grant.resize(total) {
            GrantResize::Granted => Ok(()),
            GrantResize::Spill => {
                self.do_spill_dictionary_count()?;
                let after = self
                    .dictionary_count
                    .as_ref()
                    .map(DictionaryCountArena::allocated_bytes)
                    .unwrap_or(0)
                    .saturating_add(
                        self.dictionary_encoder
                            .as_ref()
                            .map(DictionaryGroupEncoder::allocated_bytes)
                            .unwrap_or(0),
                    );
                match self
                    .memory_grant
                    .as_mut()
                    .expect("dictionary COUNT spill retains its grant")
                    .resize_after_spill(after)
                {
                    GrantResize::Granted | GrantResize::Spill => Ok(()),
                }
            }
        }
    }

    fn do_spill_dictionary_count(&mut self) -> Result<(), QueryError> {
        let Some(mut arena) = self.dictionary_count.take() else {
            return Ok(());
        };
        let encoder = self
            .dictionary_encoder
            .take()
            .expect("compact dictionary state retains its encoder");
        self.spill_schema
            .as_ref()
            .expect("compact dictionary spill requires an input schema");
        let shared = self
            .shared_spill
            .as_ref()
            .expect("compact dictionary aggregate is enabled only with shared spill")
            .clone();
        for partition in 0..NUM_PARTITIONS {
            let compact = arena.take_partition(partition);
            let mut start = 0;
            while start < compact.keys.len() {
                crate::engine::cancel::check_cancelled()?;
                let mut end = (start + DEFAULT_BATCH_ROWS).min(compact.keys.len());
                let scratch_charge = loop {
                    let bytes = dictionary_spill_scratch_bytes(
                        &compact,
                        &encoder,
                        start..end,
                        arena.n_aggs,
                    );
                    match crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_recoverable(
                        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
                        bytes,
                    ) {
                        Ok(charge) => break charge,
                        Err(_) if end - start > 1 => end = start + (end - start) / 2,
                        Err(error) => {
                            return Err(crate::engine::executor::mem_budget::terminal_oom(error));
                        }
                    }
                };
                let batch = dictionary_count_partition_to_spill_batch(
                    &compact,
                    &encoder,
                    Arc::clone(&shared.dictionary_schema),
                    start..end,
                    arena.n_aggs,
                )?;
                let (bytes, first_file_write) = shared.append_dictionary(partition, &batch)?;
                self.spill_bytes_total = self.spill_bytes_total.saturating_add(bytes);
                self.shared_spilled_partitions[partition] = true;
                if first_file_write {
                    self.next_segment_id = self.next_segment_id.saturating_add(1);
                }
                drop(batch);
                drop(scratch_charge);
                start = end;
            }
        }
        // The dictionary owns the canonical and representative bytes, so
        // retaining it after spilling only the count arena does not actually
        // relieve memory pressure. Worse, once the dictionary itself exceeds
        // the grant, every following input batch spills a tiny count tail.
        // End the complete dictionary epoch here. The next batch may build a
        // fresh compact dictionary; spilled rows already contain real key
        // values and therefore do not depend on the retired local tokens.
        // This makes the reservation follow the live epoch and bounds both
        // resident memory and spill-run amplification for arbitrary
        // high-cardinality string GROUP BY inputs.
        self.dictionary_probed = false;
        self.dictionary_repack_attempted = false;
        self.frozen_dictionary_encoder = None;
        Ok(())
    }

    fn update_fixed_width_peak_and_maybe_spill(&mut self) -> Result<(), QueryError> {
        let total = self
            .fixed_width
            .as_ref()
            .map(FixedWidthStateArena::allocated_bytes)
            .unwrap_or(0);
        self.peak_mem_bytes = self.peak_mem_bytes.max(total);
        let Some(grant) = self.memory_grant.as_mut() else {
            return Ok(());
        };
        use crate::engine::executor::memory_grant::GrantResize;
        match grant.resize(total) {
            GrantResize::Granted => Ok(()),
            GrantResize::Spill => {
                self.do_spill_fixed_width()?;
                let after = self
                    .fixed_width
                    .as_ref()
                    .map(FixedWidthStateArena::allocated_bytes)
                    .unwrap_or(0);
                match self
                    .memory_grant
                    .as_mut()
                    .expect("fixed_width spill retains its grant")
                    .resize_after_spill(after)
                {
                    GrantResize::Granted | GrantResize::Spill => Ok(()),
                }
            }
        }
    }

    fn do_spill_fixed_width(&mut self) -> Result<(), QueryError> {
        let Some(arena) = self.fixed_width.take() else {
            return Ok(());
        };
        if arena.hashes.is_empty() {
            self.fixed_width = Some(arena);
            return Ok(());
        }
        let schema = self
            .spill_schema
            .clone()
            .expect("fixed_width spill requires an input schema");
        if self.shared_spill.is_none() && self.spill_dir.is_none() {
            self.spill_dir = Some(Arc::new(SpillTmpDir::create_kind(
                &self.spill_root,
                "hashagg",
            )?));
        }
        let (partitioned_groups, offsets) = arena.partitioned_groups();
        for partition in 0..NUM_PARTITIONS {
            let groups = &partitioned_groups[offsets[partition]..offsets[partition + 1]];
            if groups.is_empty() {
                continue;
            }
            let batch = fixed_width_partition_to_spill_batch(&arena, groups, &schema, &self.aggs);
            if let Some(shared) = &self.shared_spill {
                let (bytes, first_file_write) = shared.append(partition, &batch)?;
                self.spill_bytes_total = self.spill_bytes_total.saturating_add(bytes);
                self.shared_spilled_partitions[partition] = true;
                if first_file_write {
                    self.next_segment_id = self.next_segment_id.saturating_add(1);
                }
                continue;
            }
            let dir = self
                .spill_dir
                .as_ref()
                .expect("private spill directory initialized");
            let first_write = self.spilled_paths[partition].is_empty();
            let path = dir.path().join(format!("p{partition:03}.bin"));
            let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(1);
            let mut writer = if first_write {
                crate::engine::executor::spill_quota::TrackedSpillWriter::create_truncate(
                    &path,
                    dir.charger(),
                    capacity,
                )?
            } else {
                crate::engine::executor::spill_quota::TrackedSpillWriter::open_append(
                    &path,
                    dir.charger(),
                    capacity,
                )?
            };
            spill::write_batch(&mut writer, &batch)?;
            self.spill_bytes_total = self.spill_bytes_total.saturating_add(writer.finish()?);
            if first_write {
                self.spilled_paths[partition].push(path);
                self.next_segment_id = self.next_segment_id.saturating_add(1);
            }
        }
        let decimal_column_scales = arena
            .decimal_state_columns
            .iter()
            .map(|column| column.as_ref().map(|state| state.scale))
            .collect();
        self.fixed_width = Some(FixedWidthStateArena::new_with_decimal_columns(
            arena.key_kinds,
            arena.funcs,
            super::batch::DEFAULT_BATCH_ROWS,
            decimal_column_scales,
        ));
        Ok(())
    }

    fn update_peak_and_maybe_spill(&mut self) -> Result<(), QueryError> {
        #[cfg(debug_assertions)]
        for (index, partition) in self.partitions.iter().enumerate() {
            debug_assert_eq!(
                partition.resident_bytes,
                partition.estimated_bytes(),
                "HashAgg partition {index} incremental memory drifted"
            );
        }
        let dense_bytes = self
            .dense_i64_lookup
            .as_ref()
            .map(DenseI64Lookup::allocated_bytes)
            .unwrap_or(0);
        let dictionary_bytes = self
            .dictionary_encoder
            .as_ref()
            .map(DictionaryGroupEncoder::allocated_bytes)
            .unwrap_or(0);
        let packed_lookup_bytes = self.packed_group_lookup.capacity().saturating_mul(
            HASH_ENTRY_OVERHEAD + std::mem::size_of::<u128>() + std::mem::size_of::<u32>(),
        );
        let selection_scratch_bytes = self
            .row_bucket_scratch
            .capacity()
            .saturating_mul(std::mem::size_of::<u32>())
            .saturating_add(
                self.bucket_selection_keys
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(self.bucket_selection_index.capacity().saturating_mul(
                HASH_ENTRY_OVERHEAD + std::mem::size_of::<u32>() + std::mem::size_of::<usize>(),
            ))
            .saturating_add(
                self.bucket_selections
                    .capacity()
                    .saturating_mul(std::mem::size_of::<smallvec::SmallVec<[u32; 8]>>()),
            )
            .saturating_add(
                self.bucket_selections
                    .iter()
                    .map(|selection| {
                        selection
                            .spilled()
                            .then(|| {
                                selection
                                    .capacity()
                                    .saturating_mul(std::mem::size_of::<u32>())
                            })
                            .unwrap_or(0)
                    })
                    .sum::<usize>(),
            );
        let total: usize = self
            .partitions
            .iter()
            .map(|p| p.resident_bytes)
            .sum::<usize>()
            .saturating_add(dense_bytes)
            .saturating_add(dictionary_bytes)
            .saturating_add(packed_lookup_bytes)
            .saturating_add(selection_scratch_bytes);
        if total > self.peak_mem_bytes {
            self.peak_mem_bytes = total;
        }
        if self.memory_grant.is_none() {
            return Ok(());
        }
        use crate::engine::executor::memory_grant::GrantResize;
        let mut must_spill = match self
            .memory_grant
            .as_mut()
            .expect("checked above")
            .resize(total)
        {
            GrantResize::Granted => false,
            GrantResize::Spill => true,
        };
        if must_spill && self.dictionary_encoder.is_some() {
            // Generic buckets already own canonical keys and representatives.
            // The dictionary and token->bucket map are optional accelerators,
            // so evict them before spilling the mandatory aggregate states.
            self.dictionary_encoder = None;
            self.dictionary_probed = true;
            self.packed_group_lookup = Default::default();
            self.packed_inline_lookup.clear();
            return self.update_peak_and_maybe_spill();
        }
        // Spill loop — keep dumping the largest partition until both
        // gates clear. Spill is only safe when this operator was
        // constructed with `with_budget` (and therefore has a
        // `spill_schema`); the no-spill path that `HashAggregateCore::new()`
        // produces just records the pressure for the advisor and
        // keeps going (force_overflow is already tracked inside the
        // WorkAreaHandle).
        let spill_safe = self.spill_schema.is_some();
        loop {
            if !spill_safe || !must_spill {
                break;
            }
            let Some((pidx, _bytes)) = self
                .partitions
                .iter()
                .enumerate()
                .filter(|(_, p)| !p.is_empty())
                .map(|(i, p)| (i, p.resident_bytes))
                .max_by_key(|(_, b)| *b)
            else {
                break;
            };
            self.do_spill_partition(pidx)?;
            // Handles in the packed/dictionary accelerator contain
            // partition-local bucket indexes. Dropping all handles after a
            // spill is cheap and safe: a later miss reuses any resident
            // canonical bucket or creates a new partial that finalize merges
            // with the spilled state.
            self.packed_group_lookup.clear();
            self.packed_inline_lookup.clear();
            // Dense slots contain partition-local bucket indexes. A spilled
            // partition invalidates some of them, so retire the accelerator;
            // remaining and future rows continue via the canonical Packed64
            // maps and are merged with the spill partials at finalize.
            self.dense_i64_lookup = None;
            // Recompute footprint and return the difference to both
            // counters so the next iteration sees fresh head-room.
            let after: usize = self
                .partitions
                .iter()
                .map(|p| p.resident_bytes)
                .sum::<usize>()
                .saturating_add(
                    self.dictionary_encoder
                        .as_ref()
                        .map(DictionaryGroupEncoder::allocated_bytes)
                        .unwrap_or(0),
                )
                .saturating_add(self.packed_group_lookup.capacity().saturating_mul(
                    HASH_ENTRY_OVERHEAD + std::mem::size_of::<u128>() + std::mem::size_of::<u32>(),
                ))
                .saturating_add(selection_scratch_bytes);
            must_spill = match self
                .memory_grant
                .as_mut()
                .expect("checked above")
                .resize_after_spill(after)
            {
                GrantResize::Granted => false,
                GrantResize::Spill => true,
            };
        }
        Ok(())
    }

    fn do_spill_partition(&mut self, pidx: usize) -> Result<(), QueryError> {
        // Pre-conditions: with_budget() was used so spill_schema is set.
        let spill_schema = self
            .spill_schema
            .clone()
            .expect("spill called on HashAgg without a spill schema");
        let part = std::mem::take(&mut self.partitions[pidx]);
        let batch = partition_to_spill_batch(&part, &spill_schema, &self.group_columns, &self.aggs);

        if let Some(shared) = &self.shared_spill {
            let (bytes, first_file_write) = shared.append(pidx, &batch)?;
            self.spill_bytes_total = self.spill_bytes_total.saturating_add(bytes);
            self.shared_spilled_partitions[pidx] = true;
            if first_file_write {
                self.next_segment_id = self.next_segment_id.saturating_add(1);
            }
            return Ok(());
        }

        if self.spill_dir.is_none() {
            let dir = SpillTmpDir::create_kind(&self.spill_root, "hashagg")?;
            self.spill_dir = Some(Arc::new(dir));
        }
        let dir = self.spill_dir.as_ref().unwrap();
        let first_write = self.spilled_paths[pidx].is_empty();
        let path = dir.path().join(format!("p{:03}.bin", pidx));

        let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(1);
        let mut w = if first_write {
            crate::engine::executor::spill_quota::TrackedSpillWriter::create_truncate(
                &path,
                dir.charger(),
                capacity,
            )?
        } else {
            crate::engine::executor::spill_quota::TrackedSpillWriter::open_append(
                &path,
                dir.charger(),
                capacity,
            )?
        };
        spill::write_batch(&mut w, &batch)?;
        let bytes = w.finish()?;
        self.spill_bytes_total = self.spill_bytes_total.saturating_add(bytes);
        if first_write {
            self.next_segment_id += 1;
            self.spilled_paths[pidx].push(path);
        }
        Ok(())
    }

    /// Build the output BatchSchema (group cols + finalized agg cols).
    /// Cached on the operator so `finalize_stream` and external callers
    /// (e.g. vec_bridge → downstream operator) see the same shape.
    pub fn output_schema(&self) -> Arc<BatchSchema> {
        let mut fields: Vec<BatchField> =
            Vec::with_capacity(self.group_columns.len() + self.aggs.len());
        if let Some(input) = &self.input_schema {
            for &g in &self.group_columns {
                fields.push(input.fields()[g].clone());
            }
        } else {
            for (i, _) in self.group_columns.iter().enumerate() {
                fields.push(BatchField {
                    table: String::new(),
                    alias: None,
                    name: format!("g{i}"),
                    column_type: ColumnType::VarString,
                    source: Default::default(),
                });
            }
        }
        for (i, spec) in self.aggs.iter().enumerate() {
            let input_field = self
                .input_schema
                .as_ref()
                .and_then(|schema| schema.fields().get(spec.input_column));
            let column_type = spec
                .func
                .result_type(input_field.map(|field| field.column_type));
            let mut source = if matches!(spec.func, AggregateKind::Min | AggregateKind::Max) {
                input_field
                    .map(|field| field.source.clone().nullable_aggregate())
                    .unwrap_or_default()
            } else {
                crate::types::ColumnSource::default()
            };
            if spec.func.is_bit() {
                source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
            }
            fields.push(BatchField {
                table: "__agg".into(),
                alias: None,
                name: format!("a{i}"),
                column_type,
                source,
            });
        }
        Arc::new(BatchSchema::new(fields))
    }

    /// Streaming counterpart to `finalize` — returns a `BatchOperator` that
    /// emits one batch of up to `DEFAULT_BATCH_ROWS` output rows at a
    /// time. Spilled partitions are merged lazily on demand so memory
    /// stays bounded by the per-partition working set rather than the
    /// total result-set size.
    pub fn finalize_stream(mut self) -> Result<Box<dyn BatchOperator>, QueryError> {
        if let Some(stream) = self.take_direct_join_output()? {
            return Ok(stream);
        }
        // SQL global aggregation always emits one implicit group, including
        // when the upstream produced zero rows. COUNT finalizes to zero and
        // the nullable aggregates finalize to NULL from the same state
        // layout used for non-empty inputs.
        self.ensure_empty_implicit_group();
        let out_schema = self.output_schema();
        if self.fixed_width.is_some() {
            if self.spilled_paths.iter().any(|paths| !paths.is_empty()) {
                self.do_spill_fixed_width()?;
                self.fixed_width = None;
            } else {
                let stream = FixedWidthHashAggOutputStream {
                    out_schema,
                    aggs: self.aggs.clone(),
                    arena: Some(self.fixed_width.take().expect("fixed_width state checked")),
                    cursor: 0,
                    _memory_grant: self.memory_grant.take(),
                };
                return Ok(Box::new(stream));
            }
        }
        // Move both state and its accounting owner into the stream. Releasing
        // the charge here would make live buckets invisible while a slow
        // client is still consuming them.
        let partitions = std::mem::take(&mut self.partitions);
        let spilled_paths = std::mem::take(&mut self.spilled_paths);
        let memory_grant = self.memory_grant.take();
        let spill_schema = self.spill_schema.clone();
        let aggs = self.aggs.clone();
        // Keep the temp-dir alive until the stream is exhausted; without
        // this the spill_dir's `Drop` would fire when `self` goes out of
        // scope right after this function returns, wiping the segments
        // before the stream reads them back.
        let spill_dir = self.spill_dir.clone();
        let stream = HashAggOutputStream {
            out_schema,
            aggs,
            spill_schema,
            partitions: partitions.into_iter().collect(),
            spilled: spilled_paths,
            partition_cursor: 0,
            pending: std::collections::VecDeque::new(),
            pending_dynamic_bytes: 0,
            spill_depth: self.spill_depth,
            _memory_grant: memory_grant,
            _spill_dir: spill_dir,
        };
        Ok(Box::new(stream))
    }

    #[cfg(any(test, feature = "tools"))]
    /// Drain all partitions and produce one output row per group. Returns
    /// `Vec<Vec<Value>>` matching `[group_columns..., agg0, agg1, ...]`.
    ///
    /// Note on memory accounting: any bytes we've charged against
    /// `mem_budget` are released here. We don't tally the merged final
    /// buckets back into the budget because finalize is the last step —
    /// the caller is expected to consume the returned `Vec` and drop the
    /// operator immediately, so charging again would just race with the
    /// release that `Drop` performs below.
    pub fn finalize(mut self) -> Result<Vec<Vec<Value>>, QueryError> {
        self.ensure_empty_implicit_group();
        let mut rows = Vec::new();
        if let Some(arena) = self.fixed_width.take() {
            if self.spilled_paths.iter().all(|paths| paths.is_empty()) {
                for group in 0..arena.hashes.len() {
                    let mut row = arena.group_key_values(group);
                    for slot in 0..self.aggs.len() {
                        row.push(check_aggregate_value(arena.finalized_value(group, slot))?);
                    }
                    rows.push(row);
                }
                return Ok(rows);
            }
            self.fixed_width = Some(arena);
            self.do_spill_fixed_width()?;
            self.fixed_width = None;
        }
        for pidx in 0..NUM_PARTITIONS {
            let segments = std::mem::take(&mut self.spilled_paths[pidx]);
            let part = std::mem::take(&mut self.partitions[pidx]);
            if segments.is_empty() {
                for bucket in part.buckets {
                    let mut row = bucket.key_values;
                    for (slot, spec) in self.aggs.iter().enumerate() {
                        row.push(check_aggregate_value(finalize_state(
                            &bucket.aggs[slot],
                            spec,
                        ))?);
                    }
                    rows.push(row);
                }
                continue;
            }
            if self.spill_depth + 1 > SPILL_DEPTH_LIMIT {
                return Err(crate::engine::executor::mem_budget::allocation_error(
                    format!(
                        "hash-aggregate spill depth exceeded {} (raise MYDB_EXECUTOR_MEMORY_TARGET or shard input)",
                        SPILL_DEPTH_LIMIT
                    ),
                ));
            }
            let spill_schema = self
                .spill_schema
                .clone()
                .expect("spilled segment requires spill schema");
            let merged = merge_partition(
                part,
                &segments,
                spill_schema,
                &self.aggs,
                self.spill_dir
                    .as_ref()
                    .expect("spilled segments require temp directory")
                    .charger(),
            )?;
            for bucket in merged {
                let mut row = bucket.key_values;
                for (slot, spec) in self.aggs.iter().enumerate() {
                    row.push(check_aggregate_value(finalize_state(
                        &bucket.aggs[slot],
                        spec,
                    ))?);
                }
                rows.push(row);
            }
        }
        Ok(rows)
    }
}

fn batch_is_high_cardinality(bucket_ids: &[u32]) -> bool {
    const SAMPLE_ROWS: usize = 64;
    const MIN_ROWS: usize = 32;
    if bucket_ids.len() < MIN_ROWS {
        return false;
    }
    let sample_len = bucket_ids.len().min(SAMPLE_ROWS);
    let mut distinct = smallvec::SmallVec::<[u32; SAMPLE_ROWS]>::new();
    for &key in &bucket_ids[..sample_len] {
        if !distinct.contains(&key) {
            distinct.push(key);
        }
    }
    distinct.len() * 8 >= sample_len * 7
}

struct FixedWidthHashAggOutputStream {
    out_schema: Arc<BatchSchema>,
    aggs: Vec<AggregateSpec>,
    arena: Option<FixedWidthStateArena>,
    cursor: usize,
    _memory_grant: Option<crate::engine::executor::memory_grant::OperatorMemoryGrant>,
}

impl BatchOperator for FixedWidthHashAggOutputStream {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.out_schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        let Some(arena) = self.arena.as_ref() else {
            return Ok(None);
        };
        if self.cursor >= arena.hashes.len() {
            self.arena = None;
            self._memory_grant = None;
            return Ok(None);
        }
        let end = self
            .cursor
            .saturating_add(super::batch::DEFAULT_BATCH_ROWS)
            .min(arena.hashes.len());
        let group_count = arena.key_kinds.len();
        let mut columns: Vec<Vec<Value>> = (0..self.out_schema.fields().len())
            .map(|_| Vec::with_capacity(end - self.cursor))
            .collect();
        for group in self.cursor..end {
            arena.append_group_key_values(group, &mut columns);
            for slot in 0..self.aggs.len() {
                columns[group_count + slot]
                    .push(check_aggregate_value(arena.finalized_value(group, slot))?);
            }
        }
        self.cursor = end;
        let batch = Batch::new(
            self.out_schema.clone(),
            build_agg_output_columns(columns, group_count, &self.aggs),
        );
        if self.cursor == arena.hashes.len() {
            // The returned batch owns its values. No arena-backed rows remain
            // to produce, so release the consumed state before its consumer
            // starts retaining output (not only on the next EOF request).
            self.arena = None;
            self._memory_grant = None;
        }
        Ok(Some(batch))
    }
}

/// `BatchOperator` produced by `HashAggregateCore::finalize_stream`. Each call to
/// `next_batch` consumes up to `DEFAULT_BATCH_ROWS` buckets from the
/// stream's pending queue; when that runs out, we advance to the next
/// partition and (if necessary) merge any spilled segments for it
/// before refilling the pending queue.
struct HashAggOutputStream {
    out_schema: Arc<BatchSchema>,
    aggs: Vec<AggregateSpec>,
    spill_schema: Option<Arc<BatchSchema>>,
    partitions: Vec<Partition>,
    spilled: Vec<Vec<PathBuf>>,
    partition_cursor: usize,
    pending: std::collections::VecDeque<GroupBucket>,
    pending_dynamic_bytes: usize,
    spill_depth: usize,
    /// Accounting owner transferred with the aggregate state. This must stay
    /// live until success, cancellation, client early-stop, or stream error
    /// drops the cursor.
    _memory_grant: Option<crate::engine::executor::memory_grant::OperatorMemoryGrant>,
    /// Holds the spill temp directory alive for the stream's lifetime.
    /// Reading spilled segments requires the on-disk files to still
    /// exist; without this the directory's `Drop` fires when the parent
    /// `HashAggregateCore` is consumed by `finalize_stream`.
    _spill_dir: Option<Arc<SpillTmpDir>>,
}

impl HashAggOutputStream {
    fn refill_from_next_partition(&mut self) -> Result<bool, QueryError> {
        while self.partition_cursor < self.partitions.len() {
            let pidx = self.partition_cursor;
            self.partition_cursor += 1;
            let part = std::mem::take(&mut self.partitions[pidx]);
            let segments = std::mem::take(&mut self.spilled[pidx]);
            if segments.is_empty() {
                if !part.buckets.is_empty() {
                    self.pending_dynamic_bytes =
                        part.buckets.iter().map(GroupBucket::dynamic_bytes).sum();
                    self.pending.extend(part.buckets);
                    return Ok(true);
                }
                continue;
            }
            if self.spill_depth + 1 > SPILL_DEPTH_LIMIT {
                return Err(crate::engine::executor::mem_budget::allocation_error(
                    format!(
                        "hash-aggregate spill depth exceeded {} (raise MYDB_EXECUTOR_MEMORY_TARGET or shard input)",
                        SPILL_DEPTH_LIMIT
                    ),
                ));
            }
            let spill_schema = self
                .spill_schema
                .clone()
                .expect("spilled segment requires spill schema");
            let merged = merge_partition(
                part,
                &segments,
                spill_schema,
                &self.aggs,
                self._spill_dir
                    .as_ref()
                    .expect("spilled segments require temp directory")
                    .charger(),
            )?;
            if !merged.is_empty() {
                self.pending_dynamic_bytes = merged.iter().map(GroupBucket::dynamic_bytes).sum();
                self.pending.extend(merged);
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl BatchOperator for HashAggOutputStream {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.out_schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        if self.pending.is_empty() && !self.refill_from_next_partition()? {
            self.pending = std::collections::VecDeque::new();
            self._memory_grant = None;
            return Ok(None);
        }
        let take = std::cmp::min(self.pending.len(), super::batch::DEFAULT_BATCH_ROWS);
        let group_cnt = self
            .out_schema
            .fields()
            .len()
            .saturating_sub(self.aggs.len());
        let total_cols = self.out_schema.fields().len();
        let mut cols: Vec<Vec<Value>> = (0..total_cols).map(|_| Vec::with_capacity(take)).collect();
        for _ in 0..take {
            let bucket = self.pending.pop_front().unwrap();
            self.pending_dynamic_bytes = self
                .pending_dynamic_bytes
                .saturating_sub(bucket.dynamic_bytes());
            for (i, v) in bucket.key_values.iter().enumerate().take(group_cnt) {
                cols[i].push(v.clone());
            }
            for (slot, spec) in self.aggs.iter().enumerate() {
                cols[group_cnt + slot].push(check_aggregate_value(finalize_state(
                    &bucket.aggs[slot],
                    spec,
                ))?);
            }
        }
        if self.pending.is_empty() {
            // A consumed partition no longer needs its bucket backing array.
            // Free it before an adjacent blocking operator retains this batch.
            self.pending = std::collections::VecDeque::new();
        }
        let columns = build_agg_output_columns(cols, group_cnt, &self.aggs);
        let batch = Batch::new(self.out_schema.clone(), columns);
        // Consumed buckets and lookup maps have been freed. Retaining their
        // entire build grant until EOF starves an adjacent blocking consumer
        // (such as TopK), even though the actual resident state has shrunk.
        // Keep the current output batch and every still-live partition/slot
        // covered; reconcile cached totals rather than rescan remaining groups.
        if let Some(grant) = &mut self._memory_grant {
            let resident = self
                .partitions
                .iter()
                .map(|part| part.resident_bytes)
                .sum::<usize>()
                .saturating_add(
                    self.pending
                        .capacity()
                        .saturating_mul(std::mem::size_of::<GroupBucket>()),
                )
                .saturating_add(self.pending_dynamic_bytes)
                .saturating_add(batch.memory_bytes())
                .saturating_add(
                    batch
                        .columns
                        .capacity()
                        .saturating_mul(std::mem::size_of::<ColumnVector>()),
                );
            // Merged spill partitions have separate admission. This is only
            // a release of the original resident build grant, never a retry
            // or a second charge of merged state.
            let _ = grant.resize(resident.min(grant.charged_bytes()));
        }
        Ok(Some(batch))
    }
}

/// Hand-off type for the in-memory cross-worker merge path. Each worker
/// produces one of these per partition; the main thread merges them
/// directly without going through the spill / Batch round-trip. The
/// fields are deliberately the same `GroupBucket` shape the operator
/// uses internally so merging boils down to a hash-table union per
/// partition.
pub struct PartitionPartial {
    buckets: Vec<GroupBucketSnapshot>,
    fixed_width_decimal: Option<FixedWidthDecimalPartitionPartial>,
    compressed_i64_decimal: Option<CompressedI64DecimalPartial>,
    dictionary_count: Option<DictionaryCountPartitionPartial>,
}

struct DictionaryCountPartitionPartial {
    partition: DictionaryCountPartition,
    encoder: Arc<DictionaryGroupEncoder>,
    raw_token_width: usize,
    n_aggs: usize,
}

/// Partition-local columnar handoff for the fixed-width composite-key + one
/// DECIMAL SUM layout. Unlike `GroupBucketSnapshot`, this owns one flat key
/// buffer and one flat state buffer for the whole partition, so millions of
/// groups do not turn into millions of tiny heap allocations during spill
/// replay and the parallel cross-worker merge.
struct FixedWidthDecimalPartitionPartial {
    key_kinds: Vec<CompactKeyKind>,
    keys: Vec<i64>,
    nulls: Vec<u8>,
    sums: Vec<NumericAggState>,
    narrow_scale: Option<u8>,
    narrow_magnitudes: Vec<i64>,
    narrow_status: Vec<u8>,
    /// True when the worker retained the append-only single-I64 layout.
    /// Filtering that stream into radix partitions preserves its ordering,
    /// allowing the coordinator to use a k-way merge instead of rebuilding a
    /// second hash table over every high-cardinality group.
    sorted_i64: bool,
}

impl PartitionPartial {
    /// Consume the partial, yielding its per-group snapshots. Used by the
    /// ROLLUP fast path to seed row accumulators from the vectorized
    /// finest grouping (each `GroupBucketSnapshot` carries the group's key
    /// values and per-aggregate state).
    pub fn into_buckets(self) -> Vec<GroupBucketSnapshot> {
        if let Some(dictionary_count) = self.dictionary_count {
            return dictionary_count.into_buckets();
        }
        if let Some(compressed) = self.compressed_i64_decimal {
            return compressed.into_buckets();
        }
        if let Some(fixed_width) = self.fixed_width_decimal {
            return fixed_width.into_buckets();
        }
        self.buckets
    }
}

impl DictionaryCountPartitionPartial {
    fn into_buckets(self) -> Vec<GroupBucketSnapshot> {
        let mut buckets = Vec::with_capacity(self.partition.keys.len());
        let mut canonical_key = Vec::with_capacity(64);
        for (group, key) in self.partition.keys.iter().copied().enumerate() {
            let raw_start = group * self.raw_token_width;
            let raw_tokens =
                &self.partition.raw_tokens[raw_start..raw_start + self.raw_token_width];
            let key_values = self.encoder.direct_key_values(key, raw_tokens);
            self.encoder
                .write_direct_canonical_key(key, &mut canonical_key);
            let aggs = (0..self.n_aggs)
                .map(|slot| AggStateSnapshot {
                    count: self.partition.count(group, slot, self.n_aggs),
                    ..AggStateSnapshot::default()
                })
                .collect();
            buckets.push(GroupBucketSnapshot {
                key: canonical_key.clone(),
                key_values,
                aggs,
            });
        }
        buckets
    }
}

impl CompressedI64DecimalPartial {
    fn into_buckets(self) -> Vec<GroupBucketSnapshot> {
        let total = self.runs.iter().map(|run| run.sum_tokens.len()).sum();
        let mut buckets = Vec::with_capacity(total);
        for run in self.runs {
            for (key, key_is_null, sum) in run.cursor() {
                let key_value = if key_is_null {
                    Value::Null
                } else {
                    Value::Int(key)
                };
                let mut encoded = Vec::with_capacity(9);
                super::key_encode::encode(&key_value, &mut encoded);
                let mut state = NumericAggState::Empty;
                if let Some(sum) = sum {
                    accumulate_numeric_state_decimal_parts(&mut state, sum, self.scale);
                }
                buckets.push(GroupBucketSnapshot {
                    key: encoded,
                    key_values: vec![key_value],
                    aggs: vec![AggStateSnapshot {
                        count: u64::from(!matches!(state, NumericAggState::Empty)),
                        sum: state,
                        ..AggStateSnapshot::default()
                    }],
                });
            }
        }
        buckets
    }
}

impl FixedWidthDecimalPartitionPartial {
    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn key(&self, group: usize) -> &[i64] {
        let width = self.key_kinds.len();
        &self.keys[group * width..(group + 1) * width]
    }

    fn key_values(&self, group: usize) -> Vec<Value> {
        let nullmask = self.nulls[group];
        self.key_kinds
            .iter()
            .enumerate()
            .map(|(key, kind)| {
                if nullmask & (1 << key) != 0 {
                    Value::Null
                } else {
                    kind.to_value(self.key(group)[key])
                }
            })
            .collect()
    }

    fn numeric_state(&self, group: usize) -> NumericAggState {
        if let Some(scale) = self.narrow_scale {
            if self.narrow_status[group] == COMPACT_SUM_EMPTY {
                return NumericAggState::Empty;
            }
            let mut state = NumericAggState::Empty;
            accumulate_numeric_state_decimal_parts(
                &mut state,
                i128::from(self.narrow_magnitudes[group]),
                scale,
            );
            state
        } else {
            self.sums[group].clone()
        }
    }

    fn into_buckets(self) -> Vec<GroupBucketSnapshot> {
        let mut buckets = Vec::with_capacity(self.len());
        for group in 0..self.len() {
            let key_values = self.key_values(group);
            let mut key = Vec::with_capacity(key_values.len().saturating_mul(9));
            for value in &key_values {
                super::key_encode::encode(value, &mut key);
            }
            let sum = self.numeric_state(group);
            buckets.push(GroupBucketSnapshot {
                key,
                key_values,
                aggs: vec![AggStateSnapshot {
                    count: u64::from(!matches!(sum, NumericAggState::Empty)),
                    sum,
                    ..AggStateSnapshot::default()
                }],
            });
        }
        buckets
    }
}

#[derive(Debug, Clone)]
pub struct GroupBucketSnapshot {
    pub key: Vec<u8>,
    pub key_values: Vec<Value>,
    pub aggs: Vec<AggStateSnapshot>,
}

impl HashAggregateCore {
    /// Take a partition's current in-memory buckets as plain Rust data
    /// (no `Batch` allocation, no `Value` cloning where avoidable). The
    /// partition's in-memory state is cleared after extraction. This is
    /// the path the parallel-aggregate worker hands its slice of the
    /// hash table over to the merge thread.
    #[cfg(test)]
    pub fn take_partition_snapshot(&mut self, pidx: usize) -> Option<PartitionPartial> {
        if let Some(arena) = &self.fixed_width {
            return fixed_width_partition_partial(arena, pidx);
        }
        if pidx >= self.partitions.len() || self.partitions[pidx].is_empty() {
            return None;
        }
        let part = std::mem::take(&mut self.partitions[pidx]);
        Some(partition_partial_from_buckets(part.buckets))
    }

    /// Spill-aware counterpart used by parallel workers. Any segments
    /// written by this worker are merged with the in-memory tail before the
    /// raw aggregate state is handed to the coordinator. This preserves AVG,
    /// variance and DISTINCT merge semantics; finalising worker results and
    /// aggregating those values again would not.
    pub fn take_partition_merged_snapshot(
        &mut self,
        pidx: usize,
    ) -> Result<Option<PartitionPartial>, QueryError> {
        if pidx >= self.partitions.len() {
            return Ok(None);
        }
        if let Some(arena) = self.dictionary_count.as_mut() {
            if self.frozen_dictionary_encoder.is_none() {
                self.frozen_dictionary_encoder = Some(Arc::new(
                    self.dictionary_encoder
                        .take()
                        .expect("compact dictionary state retains its encoder"),
                ));
            }
            let raw_token_width = arena.raw_token_width;
            let n_aggs = arena.n_aggs;
            let partition = arena.take_partition(pidx);
            return Ok((!partition.keys.is_empty()).then(|| PartitionPartial {
                buckets: Vec::new(),
                fixed_width_decimal: None,
                compressed_i64_decimal: None,
                dictionary_count: Some(DictionaryCountPartitionPartial {
                    partition,
                    encoder: Arc::clone(
                        self.frozen_dictionary_encoder
                            .as_ref()
                            .expect("dictionary encoder frozen for handoff"),
                    ),
                    raw_token_width,
                    n_aggs,
                }),
            }));
        }
        if pidx == 0 {
            if let Some(arena) = self.fixed_width.as_mut() {
                if !arena.compressed_i64_decimal_runs.is_empty() {
                    let runs = std::mem::take(&mut arena.compressed_i64_decimal_runs);
                    arena.compressed_i64_decimal_allocated_bytes = 0;
                    let scale = arena
                        .compressed_i64_decimal_scale
                        .take()
                        .expect("compressed DECIMAL runs retain one scale");
                    return Ok(Some(PartitionPartial {
                        buckets: Vec::new(),
                        fixed_width_decimal: None,
                        compressed_i64_decimal: Some(CompressedI64DecimalPartial { runs, scale }),
                        dictionary_count: None,
                    }));
                }
            }
        }
        if self.fixed_width.is_some() {
            if self.spilled_paths.iter().any(|paths| !paths.is_empty()) {
                self.do_spill_fixed_width()?;
                self.fixed_width = None;
            } else {
                return Ok(fixed_width_partition_partial(
                    self.fixed_width
                        .as_ref()
                        .expect("fixed_width state checked"),
                    pidx,
                ));
            }
        }
        let segments = std::mem::take(&mut self.spilled_paths[pidx]);
        let part = std::mem::take(&mut self.partitions[pidx]);
        if segments.is_empty() {
            return Ok((!part.is_empty()).then(|| partition_partial_from_buckets(part.buckets)));
        }
        if self.spill_depth + 1 > SPILL_DEPTH_LIMIT {
            return Err(crate::engine::executor::mem_budget::allocation_error(
                format!(
                    "hash-aggregate spill depth exceeded {} (raise MYDB_EXECUTOR_MEMORY_TARGET or shard input)",
                    SPILL_DEPTH_LIMIT
                ),
            ));
        }
        let spill_schema = self
            .spill_schema
            .clone()
            .expect("spilled segment requires spill schema");
        let buckets = merge_partition(
            part,
            &segments,
            spill_schema,
            &self.aggs,
            self.spill_dir
                .as_ref()
                .expect("spilled segments require temp directory")
                .charger(),
        )?;
        Ok((!buckets.is_empty()).then(|| partition_partial_from_buckets(buckets)))
    }
}

fn fixed_width_partition_partial(
    arena: &FixedWidthStateArena,
    pidx: usize,
) -> Option<PartitionPartial> {
    let partition_groups = arena.partition_groups.get(pidx)?;
    let mut sorted_groups = Vec::new();
    let groups: &[u32] = if arena.defer_i64_decimal_dedup {
        let mut runs = arena.deferred_partition_runs[pidx].clone();
        runs.sort_unstable_by_key(|(start, _)| {
            deferred_i64_group_sort_key(arena, partition_groups[*start as usize])
        });
        let non_overlapping = runs.windows(2).all(|pair| {
            let left_last = partition_groups[pair[0].1 as usize - 1];
            let right_first = partition_groups[pair[1].0 as usize];
            deferred_i64_group_sort_key(arena, left_last)
                <= deferred_i64_group_sort_key(arena, right_first)
        });
        if non_overlapping {
            sorted_groups.reserve(partition_groups.len());
            for (start, end) in runs {
                sorted_groups.extend_from_slice(&partition_groups[start as usize..end as usize]);
            }
        } else {
            // Correctness fallback for an upstream operator that emits
            // overlapping ordered runs. This remains much cheaper than
            // worker-local hashing for the common range-disjoint scan, while
            // accepting arbitrary batch arrival order.
            sorted_groups.extend_from_slice(partition_groups);
            sorted_groups.sort_unstable_by_key(|group| deferred_i64_group_sort_key(arena, *group));
        }
        &sorted_groups
    } else {
        partition_groups
    };
    if arena.n_aggs == 1 {
        if let Some(decimal_state_column) = &arena.decimal_state_columns[0] {
            let key_width = arena.key_kinds.len();
            let mut keys = Vec::with_capacity(groups.len().saturating_mul(key_width));
            let mut nulls = Vec::with_capacity(groups.len());
            let use_narrow = decimal_state_column.promoted.is_empty();
            let mut sums = Vec::with_capacity(if use_narrow { 0 } else { groups.len() });
            let mut narrow_magnitudes =
                Vec::with_capacity(if use_narrow { groups.len() } else { 0 });
            let mut narrow_status = Vec::with_capacity(if use_narrow { groups.len() } else { 0 });
            for &group in groups {
                let group = group as usize;
                keys.extend_from_slice(&arena.keys[group * key_width..(group + 1) * key_width]);
                nulls.push(arena.nulls[group]);
                if use_narrow {
                    narrow_magnitudes.push(decimal_state_column.magnitudes[group]);
                    narrow_status.push(decimal_state_column.status[group]);
                } else {
                    sums.push(decimal_state_column.state(group));
                }
            }
            return (!groups.is_empty()).then_some(PartitionPartial {
                buckets: Vec::new(),
                fixed_width_decimal: Some(FixedWidthDecimalPartitionPartial {
                    key_kinds: arena.key_kinds.clone(),
                    keys,
                    nulls,
                    sums,
                    narrow_scale: use_narrow.then_some(decimal_state_column.scale),
                    narrow_magnitudes,
                    narrow_status,
                    sorted_i64: matches!(arena.key_kinds.as_slice(), [CompactKeyKind::I64])
                        && (arena.monotonic_i64_unindexed || arena.defer_i64_decimal_dedup),
                }),
                compressed_i64_decimal: None,
                dictionary_count: None,
            });
        }
    }
    let mut buckets = Vec::with_capacity(groups.len());
    for &group in groups {
        let group = group as usize;
        let key_values = arena.group_key_values(group);
        let mut key = Vec::with_capacity(key_values.len().saturating_mul(9));
        for value in &key_values {
            super::key_encode::encode(value, &mut key);
        }
        buckets.push(GroupBucketSnapshot {
            key,
            key_values,
            aggs: (0..arena.n_aggs)
                .map(|aggregate| arena.state_snapshot(group, aggregate))
                .collect(),
        });
    }
    (!buckets.is_empty()).then_some(PartitionPartial {
        buckets,
        fixed_width_decimal: None,
        compressed_i64_decimal: None,
        dictionary_count: None,
    })
}

#[inline]
fn deferred_i64_group_sort_key(arena: &FixedWidthStateArena, group: u32) -> (u8, i64) {
    let group = group as usize;
    (u8::from(arena.nulls[group] == 0), arena.keys[group])
}

fn merge_fixed_width_spill_path(
    path: &std::path::Path,
    schema: Arc<BatchSchema>,
    aggs: &[AggregateSpec],
    charger: crate::engine::executor::spill_quota::SpillCharger,
) -> Result<Option<PartitionPartial>, QueryError> {
    let layout = SpillLayout::for_schema(&schema, aggs)?;
    let group_count = layout.group_count;
    let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(1);
    let mut reader =
        crate::engine::executor::spill_quota::TrackedSpillReader::open(path, charger, capacity)?;
    let Some(first) = spill::read_owned_batch(&mut reader, schema.clone())? else {
        return Ok(None);
    };
    let Some(mut arena) = FixedWidthStateArena::try_new_for_partial(&first, group_count, aggs)
    else {
        return Ok(None);
    };
    arena.merge_partial_batch(&first, group_count, &layout, aggs)?;
    while let Some(batch) = spill::read_owned_batch(&mut reader, schema.clone())? {
        arena.merge_partial_batch(&batch, group_count, &layout, aggs)?;
    }
    let mut buckets = Vec::with_capacity(arena.hashes.len());
    for group in 0..arena.hashes.len() {
        let key_values = arena.group_key_values(group);
        let mut key = Vec::with_capacity(key_values.len().saturating_mul(9));
        for value in &key_values {
            super::key_encode::encode(value, &mut key);
        }
        buckets.push(GroupBucketSnapshot {
            key,
            key_values,
            aggs: (0..arena.n_aggs)
                .map(|aggregate| arena.state_snapshot(group, aggregate))
                .collect(),
        });
    }
    Ok((!buckets.is_empty()).then_some(PartitionPartial {
        buckets,
        fixed_width_decimal: None,
        compressed_i64_decimal: None,
        dictionary_count: None,
    }))
}

fn fixed_width_state_snapshot(state: &InlineAggState) -> AggStateSnapshot {
    let mut snapshot = AggStateSnapshot::default();
    match state {
        InlineAggState::Count(count) => snapshot.count = (*count).max(0) as u64,
        InlineAggState::Sum(sum) => {
            snapshot.sum = sum.clone();
            // The common partial-state format uses `count == 0` as SUM's
            // empty-state marker even though SUM does not otherwise need a
            // denominator. Preserve that marker when adapting the fixed_width
            // inline layout.
            snapshot.count = u64::from(!matches!(sum, NumericAggState::Empty));
        }
        InlineAggState::Avg { state, n } => {
            snapshot.sum = state.clone();
            snapshot.count = *n;
        }
        InlineAggState::MinMax { is_min, best } => {
            if *is_min {
                snapshot.min = best.clone();
            } else {
                snapshot.max = best.clone();
            }
        }
    }
    snapshot
}

fn partition_partial_from_buckets(buckets: Vec<GroupBucket>) -> PartitionPartial {
    let buckets = buckets
        .into_iter()
        .map(|b| GroupBucketSnapshot {
            key: b.key,
            key_values: b.key_values,
            aggs: b.aggs.into_iter().map(AggState::into_partial).collect(),
        })
        .collect();
    PartitionPartial {
        buckets,
        fixed_width_decimal: None,
        compressed_i64_decimal: None,
        dictionary_count: None,
    }
}

#[cfg(test)]
/// In-memory cross-worker merge. Each worker contributes one
/// `Option<PartitionPartial>` per partition (`partials[w][p]`). The
/// merger walks one partition at a time so the working set is bounded
/// by the largest partition's combined size rather than the full
/// result. Output rows have shape `[group_cols..., agg0_final, ...]`,
/// uses canonical partial states without a spill batch round-trip.
pub fn merge_worker_partition_snapshots(
    partials: Vec<Vec<Option<PartitionPartial>>>,
    aggs: &[AggregateSpec],
) -> Result<Vec<Vec<Value>>, QueryError> {
    merge_worker_partition_snapshots_filtered(partials, aggs, None)
}

#[cfg(test)]
pub fn merge_worker_partition_snapshots_filtered(
    partials: Vec<Vec<Option<PartitionPartial>>>,
    aggs: &[AggregateSpec],
    final_filter: Option<&AggregateFinalFilter>,
) -> Result<Vec<Vec<Value>>, QueryError> {
    merge_worker_partition_snapshots_filtered_topk(partials, aggs, final_filter, None)
}

pub fn merge_worker_partition_snapshots_filtered_topk(
    mut partials: Vec<Vec<Option<PartitionPartial>>>,
    aggs: &[AggregateSpec],
    final_filter: Option<&AggregateFinalFilter>,
    count_topk: Option<AggregateCountTopKHint>,
) -> Result<Vec<Vec<Value>>, QueryError> {
    if partials.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(rows) =
        merge_dictionary_count_partials(&mut partials, aggs, final_filter, count_topk)?
    {
        return Ok(rows);
    }
    let num_partitions = partials[0].len();
    // Transpose: per partition_idx, collect the contributing workers.
    let mut per_partition: Vec<Vec<PartitionPartial>> =
        (0..num_partitions).map(|_| Vec::new()).collect();
    for worker in partials {
        for (p_idx, opt) in worker.into_iter().enumerate() {
            if let Some(part) = opt {
                per_partition[p_idx].push(part);
            }
        }
    }
    let mut out: Vec<Vec<Value>> = Vec::new();
    for partition_contribs in per_partition.into_iter() {
        if partition_contribs.is_empty() {
            continue;
        }
        if matches!(
            aggs,
            [AggregateSpec {
                func: AggregateKind::Sum,
                ..
            }]
        ) && partition_contribs
            .iter()
            .all(|partial| partial.compressed_i64_decimal.is_some())
        {
            let compressed = partition_contribs
                .into_iter()
                .map(|partial| {
                    partial
                        .compressed_i64_decimal
                        .expect("compressed partition shape checked")
                })
                .collect();
            out.extend(merge_compressed_i64_decimal_partials(
                compressed,
                final_filter,
            )?);
            continue;
        }
        if matches!(
            aggs,
            [AggregateSpec {
                func: AggregateKind::Sum,
                ..
            }]
        ) && partition_contribs
            .iter()
            .all(|partial| partial.fixed_width_decimal.is_some())
        {
            let fixed_width = partition_contribs
                .into_iter()
                .map(|partial| {
                    partial
                        .fixed_width_decimal
                        .expect("fixed_width partition shape checked")
                })
                .collect();
            out.extend(merge_fixed_width_decimal_partitions_filtered(
                fixed_width,
                final_filter,
            )?);
            continue;
        }
        // Merge all workers' buckets for this partition_idx.
        let mut by_key: GroupMap<usize> = GroupMap::default();
        let mut buckets: Vec<GroupBucketSnapshot> = Vec::new();
        for part in partition_contribs {
            for bucket in part.into_buckets() {
                if let Some(&idx) = by_key.get(&bucket.key) {
                    let existing = &mut buckets[idx];
                    for (slot, spec) in aggs.iter().enumerate() {
                        merge_snapshot(&mut existing.aggs[slot], &bucket.aggs[slot], spec);
                    }
                } else {
                    by_key.insert(bucket.key.clone(), buckets.len());
                    buckets.push(bucket);
                }
            }
        }
        // DISTINCT must be unioned across every worker/spill epoch before
        // ranking. Once that merge is complete, count-based partition Top-K
        // is exact and avoids materializing millions of losing output rows.
        if let Some(hint) = count_topk.filter(|hint| {
            final_filter.is_none()
                // These reducers can emit string/temporal conversion
                // diagnostics during finalize. Do not skip losing groups'
                // finalization when such a reducer is present.
                && aggs.iter().all(|spec| !matches!(spec.func,
                    AggregateKind::SumDistinct | AggregateKind::AvgDistinct))
                && hint.k > 0
                && hint.k <= 4096
                && aggs.get(hint.aggregate_index).is_some_and(|spec| {
                    matches!(
                        spec.func,
                        AggregateKind::CountStar
                            | AggregateKind::Count
                            | AggregateKind::CountDistinct
                    )
                })
        }) {
            if buckets.len() > hint.k {
                let key = |bucket: &GroupBucketSnapshot| {
                    let Value::Int(count) = finalize_snapshot(
                        &bucket.aggs[hint.aggregate_index],
                        &aggs[hint.aggregate_index],
                    ) else {
                        unreachable!("COUNT finalizes to an integer")
                    };
                    if hint.descending {
                        -(count as i128)
                    } else {
                        count as i128
                    }
                };
                buckets.select_nth_unstable_by_key(hint.k - 1, key);
                buckets.truncate(hint.k);
            }
        }
        for bucket in buckets {
            let mut row = bucket.key_values;
            let mut finalized = Vec::with_capacity(aggs.len());
            for (slot, spec) in aggs.iter().enumerate() {
                finalized.push(check_aggregate_value(finalize_snapshot(
                    &bucket.aggs[slot],
                    spec,
                ))?);
            }
            if let Some(filter) = final_filter {
                if !filter.keeps(filter.aggregate_index, &finalized[filter.aggregate_index])? {
                    continue;
                }
            }
            row.extend(finalized);
            out.push(row);
        }
    }
    Ok(out)
}

fn merge_dictionary_count_partials(
    partials: &mut Vec<Vec<Option<PartitionPartial>>>,
    aggs: &[AggregateSpec],
    final_filter: Option<&AggregateFinalFilter>,
    count_topk: Option<AggregateCountTopKHint>,
) -> Result<Option<Vec<Vec<Value>>>, QueryError> {
    if !aggs
        .iter()
        .all(|spec| matches!(spec.func, AggregateKind::CountStar | AggregateKind::Count))
    {
        return Ok(None);
    }
    let mut saw_partial = false;
    for partial in partials.iter().flatten().flatten() {
        saw_partial = true;
        if partial.dictionary_count.is_none() {
            return Ok(None);
        }
    }
    if !saw_partial {
        return Ok(Some(Vec::new()));
    }

    let compact = std::mem::take(partials)
        .into_iter()
        .flatten()
        .flatten()
        .map(|partial| {
            partial
                .dictionary_count
                .expect("dictionary partial shape checked")
        })
        .collect::<Vec<_>>();
    let raw_token_width = compact[0].raw_token_width;
    let n_aggs = compact[0].n_aggs;
    let estimated_groups = compact
        .iter()
        .map(|partial| partial.partition.keys.len())
        .sum::<usize>();
    let mut by_key = hashbrown::HashMap::<u128, u32, U128BuildHasher>::with_capacity_and_hasher(
        estimated_groups,
        U128BuildHasher,
    );
    let mut sources = Vec::with_capacity(estimated_groups);
    let mut source_groups = Vec::with_capacity(estimated_groups);
    let mut counts: Vec<u64> = Vec::with_capacity(estimated_groups.saturating_mul(n_aggs));

    for (source, partial) in compact.iter().enumerate() {
        let encoder = &partial.encoder;
        let partition = &partial.partition;
        for (group, local_key) in partition.keys.iter().copied().enumerate() {
            let merged_key = encoder.direct_merge_key(local_key);
            // `get` followed by `insert` hashes every new key twice. Most
            // analytical partial groups are globally unique, so use the
            // entry API and pay exactly one hash/probe in that dominant case.
            let merged_group = match by_key.entry(merged_key) {
                hashbrown::hash_map::Entry::Occupied(entry) => *entry.get() as usize,
                hashbrown::hash_map::Entry::Vacant(entry) => {
                    let merged_group = sources.len();
                    entry.insert(merged_group as u32);
                    sources.push(source as u32);
                    source_groups.push(group as u32);
                    counts.resize(counts.len().saturating_add(n_aggs), 0);
                    merged_group
                }
            };
            for slot in 0..n_aggs {
                let count = &mut counts[merged_group * n_aggs + slot];
                *count = count.saturating_add(partition.count(group, slot, n_aggs));
            }
        }
    }

    let selected_groups: Vec<usize> = if let Some(hint) = count_topk.filter(|hint| {
        hint.k > 0
            && hint.aggregate_index < n_aggs
            && matches!(
                aggs[hint.aggregate_index].func,
                AggregateKind::CountStar | AggregateKind::Count
            )
    }) {
        use std::cmp::Reverse;
        let passes_filter = |group: usize| -> Result<bool, QueryError> {
            let Some(filter) = final_filter else {
                return Ok(true);
            };
            let value = Value::Int(
                counts[group * n_aggs + filter.aggregate_index].min(i64::MAX as u64) as i64,
            );
            filter.keeps(filter.aggregate_index, &value)
        };
        if hint.descending {
            let mut heap = std::collections::BinaryHeap::<Reverse<(u64, usize)>>::new();
            for group in 0..sources.len() {
                if !passes_filter(group)? {
                    continue;
                }
                let candidate = (
                    counts[group * n_aggs + hint.aggregate_index].min(i64::MAX as u64),
                    group,
                );
                if heap.len() < hint.k {
                    heap.push(Reverse(candidate));
                } else if heap.peek().is_some_and(|worst| candidate > worst.0) {
                    heap.pop();
                    heap.push(Reverse(candidate));
                }
            }
            heap.into_iter().map(|Reverse((_, group))| group).collect()
        } else {
            let mut heap = std::collections::BinaryHeap::<(u64, usize)>::new();
            for group in 0..sources.len() {
                if !passes_filter(group)? {
                    continue;
                }
                let candidate = (
                    counts[group * n_aggs + hint.aggregate_index].min(i64::MAX as u64),
                    group,
                );
                if heap.len() < hint.k {
                    heap.push(candidate);
                } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                    heap.pop();
                    heap.push(candidate);
                }
            }
            heap.into_iter().map(|(_, group)| group).collect()
        }
    } else {
        (0..sources.len()).collect()
    };

    let mut rows = Vec::with_capacity(selected_groups.len());
    for group in selected_groups {
        let partial = &compact[sources[group] as usize];
        let source_group = source_groups[group] as usize;
        let local_key = partial.partition.keys[source_group];
        let raw_start = source_group * raw_token_width;
        let mut row = partial.encoder.direct_key_values(
            local_key,
            &partial.partition.raw_tokens[raw_start..raw_start + raw_token_width],
        );
        let finalized = (0..n_aggs)
            .map(|slot| Value::Int(counts[group * n_aggs + slot].min(i64::MAX as u64) as i64))
            .collect::<Vec<_>>();
        if let Some(filter) = final_filter {
            if !filter.keeps(filter.aggregate_index, &finalized[filter.aggregate_index])? {
                continue;
            }
        }
        row.extend(finalized);
        rows.push(row);
    }
    Ok(Some(rows))
}

fn push_compressed_decimal_row(
    rows: &mut Vec<Vec<Value>>,
    key: i64,
    key_is_null: bool,
    magnitude: Option<i128>,
    scale: u8,
    final_filter: Option<&AggregateFinalFilter>,
) -> Result<(), QueryError> {
    let Some(magnitude) = magnitude else {
        if final_filter.is_some() {
            return Ok(());
        }
        rows.push(vec![
            if key_is_null {
                Value::Null
            } else {
                Value::Int(key)
            },
            Value::Null,
        ]);
        return Ok(());
    };
    if let Some(filter) = final_filter {
        if !filter.keeps_decimal_parts(magnitude, scale)? {
            return Ok(());
        }
    }
    rows.push(vec![
        if key_is_null {
            Value::Null
        } else {
            Value::Int(key)
        },
        if let Ok(value) = i64::try_from(magnitude) {
            Value::DecimalCompact(value, scale)
        } else {
            Value::Decimal128(magnitude, scale)
        },
    ]);
    Ok(())
}

/// Merge delta-coded worker runs. Storage scans hand batches to workers in
/// round-robin order, but each batch covers a key-ordered range. Sorting only
/// the few hundred thousand run descriptors reconstructs the global order;
/// the 150M+ group entries remain compact and are read once.
fn merge_compressed_i64_decimal_partials(
    inputs: Vec<CompressedI64DecimalPartial>,
    final_filter: Option<&AggregateFinalFilter>,
) -> Result<Vec<Vec<Value>>, QueryError> {
    let Some(scale) = inputs.first().map(|input| input.scale) else {
        return Ok(Vec::new());
    };
    if inputs.iter().any(|input| input.scale != scale) {
        let buckets = inputs
            .into_iter()
            .flat_map(CompressedI64DecimalPartial::into_buckets)
            .collect::<Vec<_>>();
        return merge_compressed_fallback_buckets(buckets, final_filter);
    }
    let mut runs = inputs
        .into_iter()
        .flat_map(|input| input.runs)
        .collect::<Vec<_>>();
    runs.sort_unstable_by_key(|run| run.first_key);
    if runs
        .windows(2)
        .any(|pair| pair[0].last_key > pair[1].first_key)
    {
        let buckets = CompressedI64DecimalPartial { runs, scale }.into_buckets();
        return merge_compressed_fallback_buckets(buckets, final_filter);
    }

    let mut rows = Vec::new();
    let mut current: Option<(i64, bool, Option<i128>)> = None;
    for run in &runs {
        for (key, key_is_null, sum) in run.cursor() {
            match current.as_mut() {
                Some((current_key, current_null, current_sum))
                    if *current_key == key && *current_null == key_is_null =>
                {
                    if let Some(sum) = sum {
                        *current_sum =
                            Some(current_sum.unwrap_or(0).checked_add(sum).ok_or_else(|| {
                                crate::storage::error(
                                    crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                                    "DECIMAL aggregate overflow",
                                )
                            })?);
                    }
                }
                _ => {
                    if let Some((old_key, old_null, old_sum)) =
                        current.replace((key, key_is_null, sum))
                    {
                        push_compressed_decimal_row(
                            &mut rows,
                            old_key,
                            old_null,
                            old_sum,
                            scale,
                            final_filter,
                        )?;
                    }
                }
            }
        }
    }
    if let Some((key, key_is_null, sum)) = current {
        push_compressed_decimal_row(&mut rows, key, key_is_null, sum, scale, final_filter)?;
    }
    Ok(rows)
}

fn merge_compressed_fallback_buckets(
    buckets: Vec<GroupBucketSnapshot>,
    final_filter: Option<&AggregateFinalFilter>,
) -> Result<Vec<Vec<Value>>, QueryError> {
    let mut by_key: GroupMap<usize> = GroupMap::default();
    let mut merged: Vec<GroupBucketSnapshot> = Vec::new();
    for bucket in buckets {
        if let Some(&index) = by_key.get(&bucket.key) {
            merge_snapshot(
                &mut merged[index].aggs[0],
                &bucket.aggs[0],
                &AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Sum,
                    input_column: 0,
                },
            );
        } else {
            by_key.insert(bucket.key.clone(), merged.len());
            merged.push(bucket);
        }
    }
    let mut rows = Vec::new();
    for bucket in merged {
        let value = finalize_snapshot(
            &bucket.aggs[0],
            &AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 0,
            },
        );
        if let Some(filter) = final_filter {
            if !filter.keeps(0, &value)? {
                continue;
            }
        }
        let mut row = bucket.key_values;
        row.push(value);
        rows.push(row);
    }
    Ok(rows)
}

/// Merge one radix partition while it is still in the fixed_width fixed-width
/// representation. The temporary key table is partition-local and key-only;
/// aggregate states live in one parallel Vec. This replaces three owned Vecs
/// plus an encoded-key clone per group in the generic snapshot merger.
#[cfg(test)]
fn merge_fixed_width_decimal_partitions(
    inputs: Vec<FixedWidthDecimalPartitionPartial>,
) -> Vec<Vec<Value>> {
    merge_fixed_width_decimal_partitions_filtered(inputs, None)
        .expect("unfiltered aggregate finalization cannot fail")
}

fn merge_fixed_width_decimal_partitions_filtered(
    inputs: Vec<FixedWidthDecimalPartitionPartial>,
    final_filter: Option<&AggregateFinalFilter>,
) -> Result<Vec<Vec<Value>>, QueryError> {
    let Some(first) = inputs.first() else {
        return Ok(Vec::new());
    };
    if inputs.iter().all(|input| input.sorted_i64) {
        return merge_sorted_i64_decimal_partitions(inputs, final_filter);
    }
    let key_kinds = first.key_kinds.clone();
    let group_count = key_kinds.len();
    debug_assert!(inputs.iter().all(|input| input.key_kinds == key_kinds));
    let total = inputs
        .iter()
        .map(FixedWidthDecimalPartitionPartial::len)
        .sum();
    let mut keys = FixedWidthStateArena::new(key_kinds, Vec::new(), total);
    let mut sums: Vec<NumericAggState> = Vec::with_capacity(total);
    for input in inputs {
        for group in 0..input.len() {
            let merged_group = keys.probe_or_insert(input.key(group), input.nulls[group]);
            if merged_group == sums.len() {
                sums.push(input.numeric_state(group));
            } else {
                merge_numeric_state(&mut sums[merged_group], input.numeric_state(group));
            }
        }
    }

    let mut rows = if final_filter.is_some() {
        Vec::new()
    } else {
        Vec::with_capacity(sums.len())
    };
    for (group, sum) in sums.iter().enumerate() {
        let value = numeric_state_to_value(sum);
        if let Some(filter) = final_filter {
            debug_assert_eq!(filter.aggregate_index, 0);
            if !filter.keeps(0, &value)? {
                continue;
            }
        }
        let mut row = keys.group_key_values(group);
        debug_assert_eq!(row.len(), group_count);
        row.push(value);
        rows.push(row);
    }
    Ok(rows)
}

/// Merge append-only single-I64 worker states without rehashing them. Each
/// worker list is already ordered, and radix partition filtering retains that
/// order, so the coordinator only keeps one heap entry per worker. Equal keys
/// are folded before emission; disjoint high-cardinality inputs are copied
/// exactly once.
fn merge_sorted_i64_decimal_partitions(
    mut inputs: Vec<FixedWidthDecimalPartitionPartial>,
    final_filter: Option<&AggregateFinalFilter>,
) -> Result<Vec<Vec<Value>>, QueryError> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    debug_assert!(inputs.iter().all(|input| {
        input.sorted_i64 && matches!(input.key_kinds.as_slice(), [CompactKeyKind::I64])
    }));
    if let Some(scale) = inputs.first().and_then(|input| input.narrow_scale) {
        if inputs.iter().all(|input| input.narrow_scale == Some(scale)) {
            return merge_sorted_i64_narrow_decimal_partitions(inputs, scale, final_filter);
        }
    }
    let total = inputs
        .iter()
        .map(FixedWidthDecimalPartitionPartial::len)
        .sum();
    let mut heap: BinaryHeap<Reverse<(u8, i64, usize, usize)>> = BinaryHeap::new();
    for (input, partial) in inputs.iter().enumerate() {
        if partial.len() != 0 {
            let nullmask = partial.nulls[0];
            let key = partial.keys[0];
            heap.push(Reverse((u8::from(nullmask == 0), key, input, 0)));
        }
    }

    let mut rows = if final_filter.is_some() {
        Vec::new()
    } else {
        Vec::with_capacity(total)
    };
    while let Some(Reverse((non_null, key, input, group))) = heap.pop() {
        let nullmask = inputs[input].nulls[group];
        let mut sum = std::mem::take(&mut inputs[input].sums[group]);
        push_next_sorted_partial(&inputs, &mut heap, input, group + 1);

        while let Some(Reverse((next_non_null, next_key, _, _))) = heap.peek().copied() {
            if next_non_null != non_null || (non_null != 0 && next_key != key) {
                break;
            }
            let Reverse((_, _, next_input, next_group)) =
                heap.pop().expect("peeked sorted aggregate entry exists");
            let next_sum = std::mem::take(&mut inputs[next_input].sums[next_group]);
            merge_numeric_state(&mut sum, next_sum);
            push_next_sorted_partial(&inputs, &mut heap, next_input, next_group + 1);
        }

        let value = numeric_state_to_value(&sum);
        if let Some(filter) = final_filter {
            debug_assert_eq!(filter.aggregate_index, 0);
            if !filter.keeps(0, &value)? {
                continue;
            }
        }
        rows.push(vec![
            if nullmask == 0 {
                Value::Int(key)
            } else {
                Value::Null
            },
            value,
        ]);
    }
    Ok(rows)
}

fn merge_sorted_i64_narrow_decimal_partitions(
    inputs: Vec<FixedWidthDecimalPartitionPartial>,
    scale: u8,
    final_filter: Option<&AggregateFinalFilter>,
) -> Result<Vec<Vec<Value>>, QueryError> {
    // The configured worker count is small (normally four). A flat cursor
    // scan is cheaper than maintaining a BinaryHeap for every one of millions
    // of groups: four integer comparisons beat heap sift/pop bookkeeping and
    // keep all cursor state in registers.
    let mut cursors = vec![0usize; inputs.len()];
    let mut rows = Vec::new();
    loop {
        let mut next: Option<(u8, i64)> = None;
        for (input, partial) in inputs.iter().enumerate() {
            let group = cursors[input];
            if group == partial.len() {
                continue;
            }
            let candidate = (u8::from(partial.nulls[group] == 0), partial.keys[group]);
            next = Some(next.map_or(candidate, |current| current.min(candidate)));
        }
        let Some((non_null, key)) = next else {
            break;
        };
        let mut nullmask = 0u8;
        let mut seen = false;
        let mut magnitude = 0i128;
        for (input, partial) in inputs.iter().enumerate() {
            while cursors[input] < partial.len() {
                let group = cursors[input];
                let candidate = (u8::from(partial.nulls[group] == 0), partial.keys[group]);
                if candidate != (non_null, key) {
                    break;
                }
                nullmask = partial.nulls[group];
                if partial.narrow_status[group] != COMPACT_SUM_EMPTY {
                    magnitude += i128::from(partial.narrow_magnitudes[group]);
                    seen = true;
                }
                cursors[input] += 1;
            }
        }
        let value = if !seen {
            Value::Null
        } else if let Ok(magnitude) = i64::try_from(magnitude) {
            Value::DecimalCompact(magnitude, scale)
        } else {
            Value::Decimal128(magnitude, scale)
        };
        if let Some(filter) = final_filter {
            if seen && !filter.keeps_decimal_parts(magnitude, scale)? {
                continue;
            }
            if !seen {
                continue;
            }
        }
        rows.push(vec![
            if nullmask == 0 {
                Value::Int(key)
            } else {
                Value::Null
            },
            value,
        ]);
    }
    Ok(rows)
}

fn push_next_sorted_partial(
    inputs: &[FixedWidthDecimalPartitionPartial],
    heap: &mut std::collections::BinaryHeap<std::cmp::Reverse<(u8, i64, usize, usize)>>,
    input: usize,
    group: usize,
) {
    let partial = &inputs[input];
    if group < partial.len() {
        let nullmask = partial.nulls[group];
        heap.push(std::cmp::Reverse((
            u8::from(nullmask == 0),
            partial.keys[group],
            input,
            group,
        )));
    }
}

/// Like [`merge_worker_partition_snapshots`] but returns the merged
/// per-group buckets with their raw (non-finalized) aggregate state
/// instead of finalized rows. The ROLLUP fast path needs the raw
/// `AggStateSnapshot` (sum/count/min/max) to seed the coarser-level
/// row accumulators, so it cannot use the finalizing merge.
pub fn merge_worker_partition_snapshots_into_buckets(
    partials: Vec<Vec<Option<PartitionPartial>>>,
    aggs: &[AggregateSpec],
) -> Vec<GroupBucketSnapshot> {
    if partials.is_empty() {
        return Vec::new();
    }
    let num_partitions = partials[0].len();
    let mut per_partition: Vec<Vec<PartitionPartial>> =
        (0..num_partitions).map(|_| Vec::new()).collect();
    for worker in partials {
        for (p_idx, opt) in worker.into_iter().enumerate() {
            if let Some(part) = opt {
                per_partition[p_idx].push(part);
            }
        }
    }
    let mut out: Vec<GroupBucketSnapshot> = Vec::new();
    for partition_contribs in per_partition.into_iter() {
        if partition_contribs.is_empty() {
            continue;
        }
        let mut by_key: GroupMap<usize> = GroupMap::default();
        let mut buckets: Vec<GroupBucketSnapshot> = Vec::new();
        for part in partition_contribs {
            for bucket in part.into_buckets() {
                if let Some(&idx) = by_key.get(&bucket.key) {
                    let existing = &mut buckets[idx];
                    for (slot, spec) in aggs.iter().enumerate() {
                        merge_snapshot(&mut existing.aggs[slot], &bucket.aggs[slot], spec);
                    }
                } else {
                    by_key.insert(bucket.key.clone(), buckets.len());
                    buckets.push(bucket);
                }
            }
        }
        out.extend(buckets);
    }
    out
}

fn merge_snapshot(dst: &mut AggStateSnapshot, src: &AggStateSnapshot, spec: &AggregateSpec) {
    crate::engine::expr::with_optional_compare_collation(spec.comparison_collation, || {
        dst.merge_from(src, spec.func)
    });
}

fn check_aggregate_value(value: Value) -> Result<Value, QueryError> {
    if matches!(value, Value::Double(v) if !v.is_finite()) {
        return Err(crate::error::catalog::ER_DATA_OUT_OF_RANGE
            .error("DOUBLE value is out of range in aggregate"));
    }
    Ok(value)
}

fn finalize_snapshot(state: &AggStateSnapshot, spec: &AggregateSpec) -> Value {
    state.finalize(spec.func)
}

/// Decode and merge one partial-state batch. Both resident worker partials
/// and spill replay use this kernel, including collation keys and DISTINCT.
fn merge_partial_batch_into(
    batch: &Batch,
    schema: &BatchSchema,
    aggs: &[AggregateSpec],
    layout: &SpillLayout,
    by_key: &mut GroupMap<usize>,
    buckets: &mut Vec<GroupBucket>,
    key_buf: &mut Vec<u8>,
) -> Result<(), QueryError> {
    layout.validate_batch(batch)?;
    let group_cnt = layout.group_count;
    let n = batch.num_rows();
    let cols = batch.columns();
    for r_idx in 0..n {
        key_buf.clear();
        let key_values: Vec<Value> = (0..group_cnt).map(|i| cols[i].value_at(r_idx)).collect();
        for (index, value) in key_values.iter().enumerate() {
            super::key_encode::encode_with_collation(
                value,
                schema.fields()[index].source.collation_id,
                key_buf,
            );
        }
        let entry_idx = if let Some(&i) = by_key.get(&key_buf[..]) {
            i
        } else {
            let i = buckets.len();
            buckets.push(GroupBucket::new(
                key_buf.clone(),
                key_values,
                vec![AggState::default(); aggs.len()],
            ));
            by_key.insert(key_buf.clone(), i);
            i
        };
        for (slot, (layout_slot, spec)) in layout.slots.iter().zip(aggs).enumerate() {
            crate::engine::expr::with_optional_compare_collation(
                spec.comparison_collation,
                || layout_slot.merge(&mut buckets[entry_idx].aggs[slot], spec.func, cols, r_idx),
            )?;
        }
    }
    Ok(())
}

/// Build the columns of an aggregate output batch from per-column
/// `Value` vectors, preserving the exact `Value` variant for aggregates
/// whose result `from_values` would silently re-tag. Today that is only
/// the BIT_* family: they finalise to `Value::UInt`, but `from_values`
/// folds UInt into the signed I64 lane (there is no unsigned lane), so
/// `value_at` would return `Value::Int` and diverge from the row path.
/// Carrying BIT columns through a `Generic` vector keeps the unsigned
/// tag. Shared by the serial (`HashAggOutputStream`) and parallel
/// (`hash_agg_driver`) output builders so the two can't drift.
///
/// `group_cnt` is the number of leading group-key columns; aggregate
/// columns start at `group_cnt` and map 1:1 onto `aggs`.
pub fn build_agg_output_columns(
    col_values: Vec<Vec<Value>>,
    group_cnt: usize,
    aggs: &[AggregateSpec],
) -> Vec<ColumnVector> {
    col_values
        .into_iter()
        .enumerate()
        .map(|(ci, vals)| {
            if ci >= group_cnt
                && aggs
                    .get(ci - group_cnt)
                    .map(|s| s.func.is_bit())
                    .unwrap_or(false)
            {
                ColumnVector::Generic { values: vals }
            } else {
                ColumnVector::from_values(vals)
            }
        })
        .collect()
}

#[inline]
fn numeric_or_zero(v: &Value) -> f64 {
    match v {
        Value::Double(d) => *d,
        Value::Float(f) => *f as f64,
        Value::Int(i) => *i as f64,
        Value::UInt(u) => *u as f64,
        _ => crate::engine::expr::numeric(v).unwrap_or(0.0),
    }
}

fn default_spill_root() -> PathBuf {
    crate::util::env::var("MYDB_DATA_DIR")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data"))
}

/// Serialize a DISTINCT dedup set into one `Value::Bytes` blob:
/// `u32` count + `write_value` of each distinct value. Reuses the spill
/// value codec (lossless for f64 — bit-exact `to_le_bytes`, unlike
/// `serde_json` which rejects NaN/Inf). The empty set serializes to a
/// bare count of 0, which decodes back to an empty union (the
/// empty-group identity).
fn encode_distinct_blob(map: Option<&DistinctState>) -> Value {
    use crate::engine::executor::memory_footprint::EncodedBytes;
    let encoded_capacity = map
        .map(|values| {
            let mut total = 4usize;
            values.for_each_value(|value| {
                total = total.saturating_add(value.encoded_bytes());
            });
            total
        })
        .unwrap_or(4);
    let mut buf: Vec<u8> = Vec::with_capacity(encoded_capacity);
    let n = map.map(|m| m.len()).unwrap_or(0) as u32;
    buf.extend_from_slice(&n.to_le_bytes());
    if let Some(m) = map {
        m.for_each_value(|v| {
            // Writing into a `Vec<u8>` is infallible.
            let _ = spill::write_value(&mut buf, v);
        });
    }
    Value::Bytes(buf)
}

/// Decode a blob written by [`encode_distinct_blob`] and union it into
/// `dst` (first-value-wins on key collision, matching the in-memory
/// merge). A non-`Bytes` value (shouldn't happen) unions nothing.
fn decode_distinct_blob_into(dst: &mut AggState, value: &Value) -> Result<(), QueryError> {
    let bytes = match value {
        Value::Bytes(b) => b.as_slice(),
        // The spill schema declares this column `Blob`; any other shape
        // is a corrupt segment.
        _ => {
            return Err(
                crate::error::catalog::ER_UNKNOWN_ERROR.error("invalid DISTINCT spill state type")
            )
        }
    };
    decode_distinct_bytes_into(dst, bytes)
}

fn decode_distinct_column_into(
    dst: &mut AggState,
    column: &ColumnVector,
    row: usize,
) -> Result<(), QueryError> {
    if column.is_null(row) {
        return Err(crate::error::catalog::ER_UNKNOWN_ERROR.error("null DISTINCT spill state"));
    }
    if let ColumnVector::Bytes { offsets, data, .. } = column {
        return decode_distinct_bytes_into(
            dst,
            &data[offsets[row] as usize..offsets[row + 1] as usize],
        );
    }
    decode_distinct_blob_into(dst, &column.value_at(row))
}

fn decode_distinct_bytes_into(dst: &mut AggState, bytes: &[u8]) -> Result<(), QueryError> {
    let mut r = std::io::Cursor::new(bytes);
    let mut cnt = [0u8; 4];
    use std::io::Read;
    r.read_exact(&mut cnt).map_err(|error| {
        crate::error::catalog::ER_UNKNOWN_ERROR
            .error(format!("truncated DISTINCT spill state: {error}"))
    })?;
    let n = u32::from_le_bytes(cnt) as usize;
    if n > bytes.len().saturating_sub(4) {
        return Err(
            crate::error::catalog::ER_UNKNOWN_ERROR.error("invalid DISTINCT spill state count")
        );
    }
    for _ in 0..n {
        let v = spill::read_value(&mut r)?;
        dst.distinct_insert(v);
    }
    if r.position() != bytes.len() as u64 {
        return Err(
            crate::error::catalog::ER_UNKNOWN_ERROR.error("trailing DISTINCT spill state bytes")
        );
    }
    Ok(())
}

fn partition_to_spill_batch(
    part: &Partition,
    schema: &Arc<BatchSchema>,
    group_cols: &[usize],
    aggs: &[AggregateSpec],
) -> Batch {
    let layout = SpillLayout::new(group_cols.len(), aggs);
    debug_assert_eq!(layout.width, schema.len());
    let n = part.buckets.len();
    let total_cols = schema.fields().len();
    let mut col_values: Vec<Vec<Value>> = (0..total_cols).map(|_| Vec::with_capacity(n)).collect();
    for bucket in &part.buckets {
        for (i, _) in group_cols.iter().enumerate() {
            col_values[i].push(bucket.key_values[i].clone());
        }
        for (slot, layout_slot) in layout.slots.iter().enumerate() {
            layout_slot.write(&bucket.aggs[slot], &mut col_values);
        }
    }
    let columns: Vec<ColumnVector> = col_values
        .into_iter()
        .map(ColumnVector::from_values)
        .collect();
    Batch::new(schema.clone(), columns)
}

fn dictionary_count_partition_to_spill_batch(
    partition: &DictionaryCountPartition,
    encoder: &DictionaryGroupEncoder,
    schema: Arc<BatchSchema>,
    groups: std::ops::Range<usize>,
    n_aggs: usize,
) -> Result<Batch, QueryError> {
    let group_count = encoder.columns.len();
    let mut columns: Vec<Vec<Value>> = (0..schema.fields().len())
        .map(|column| {
            if column == 0 {
                Vec::new()
            } else {
                Vec::with_capacity(groups.len())
            }
        })
        .collect();
    let mut offsets = Vec::with_capacity(groups.len() + 1);
    let mut data = Vec::new();
    let mut canonical = Vec::new();
    offsets.push(0);
    for group in groups {
        let key = partition.keys[group];
        encoder.write_direct_canonical_key(key, &mut canonical);
        let end = u32::try_from(data.len().saturating_add(canonical.len())).map_err(|_| {
            crate::engine::executor::mem_budget::allocation_error(
                "dictionary spill byte lane exceeds the 32-bit offset limit",
            )
        })?;
        data.extend_from_slice(&canonical);
        offsets.push(end);
        for (column, value) in encoder.direct_key_values_iter(key).enumerate() {
            columns[column + 1].push(value);
        }
        for slot in 0..n_aggs {
            let count = partition.count(group, slot, n_aggs).min(i64::MAX as u64);
            columns[1 + group_count + slot].push(Value::Int(count as i64));
        }
    }
    let mut vectors = Vec::with_capacity(columns.len());
    vectors.push(ColumnVector::Bytes {
        offsets,
        data,
        nulls: None,
    });
    vectors.extend(columns.into_iter().skip(1).map(ColumnVector::from_values));
    Ok(Batch::new(schema, vectors))
}

fn dictionary_spill_scratch_bytes(
    partition: &DictionaryCountPartition,
    encoder: &DictionaryGroupEncoder,
    groups: std::ops::Range<usize>,
    n_aggs: usize,
) -> usize {
    let rows = groups.len();
    let columns = encoder
        .columns
        .len()
        .saturating_add(n_aggs)
        .saturating_add(1);
    let mut payload = rows.saturating_mul(columns.saturating_mul(32));
    for group in groups {
        let key = partition.keys[group];
        for column in &encoder.columns {
            if let DictionaryColumnSource::Utf8 {
                raw_values,
                canonical_keys,
                ..
            } = &column.source
            {
                let token = ((key >> column.shift) & u128::from(column.token_mask)) as usize;
                if token != 0 {
                    payload = payload
                        .saturating_add(raw_values[token - 1].len())
                        .saturating_add(canonical_keys[token - 1].len());
                }
            }
        }
    }
    // Covers Value lanes and their simultaneous typed conversion, geometric
    // byte-buffer capacities, the canonical scratch and packed writer buffer.
    rows.saturating_mul(columns.saturating_mul(std::mem::size_of::<Value>()))
        .saturating_mul(2)
        .saturating_add(payload.saturating_mul(4))
        .saturating_add(16 * 1024)
}

fn fixed_width_partition_to_spill_batch(
    arena: &FixedWidthStateArena,
    groups: &[u32],
    schema: &Arc<BatchSchema>,
    aggs: &[AggregateSpec],
) -> Batch {
    let layout = SpillLayout::new(arena.key_kinds.len(), aggs);
    debug_assert_eq!(layout.width, schema.len());
    let mut columns: Vec<Vec<Value>> = (0..schema.fields().len())
        .map(|_| Vec::with_capacity(groups.len()))
        .collect();
    for &group in groups {
        let group = group as usize;
        arena.append_group_key_values(group, &mut columns);
        for (slot, layout_slot) in layout.slots.iter().enumerate() {
            layout_slot.write_fixed(&mut columns, arena, group, slot);
        }
    }
    Batch::new(
        schema.clone(),
        columns.into_iter().map(ColumnVector::from_values).collect(),
    )
}

/// Replay dictionary COUNT spill records using the canonical key captured at
/// spill time. The ordinary spill format stores only representative Values
/// and must rebuild their collation keys during replay; high-cardinality text
/// then pays the Unicode normalization cost a second time. This side format
/// keeps one binary canonical key beside the same representative/count state,
/// so merge is byte-exact and independent of worker-local dictionary tokens.
fn merge_dictionary_count_spill_path(
    buckets: &mut Vec<GroupBucket>,
    path: &std::path::Path,
    schema: Arc<BatchSchema>,
    group_count: usize,
    aggs: &[AggregateSpec],
    charger: crate::engine::executor::spill_quota::SpillCharger,
) -> Result<(), QueryError> {
    if !aggs
        .iter()
        .all(|spec| matches!(spec.func, AggregateKind::CountStar | AggregateKind::Count))
    {
        return Err(crate::storage::error(
            crate::error::catalog::ER_INTERNAL_ERROR,
            "dictionary COUNT spill received a non-count aggregate",
        ));
    }
    let mut by_key: GroupMap<usize> = GroupMap::default();
    for (index, bucket) in buckets.iter().enumerate() {
        by_key.insert(bucket.key.clone(), index);
    }
    let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(1);
    let mut reader =
        crate::engine::executor::spill_quota::TrackedSpillReader::open(path, charger, capacity)?;
    while let Some(batch) = spill::read_owned_batch(&mut reader, schema.clone())? {
        let columns = batch.columns();
        for row in 0..batch.num_rows() {
            let key = match columns[0].value_at(row) {
                Value::Bytes(key) => key,
                _ => {
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_INTERNAL_ERROR,
                        "dictionary COUNT spill lost its canonical key",
                    ));
                }
            };
            let entry = if let Some(&index) = by_key.get(key.as_slice()) {
                index
            } else {
                let index = buckets.len();
                let key_values = columns
                    .iter()
                    .skip(1)
                    .take(group_count)
                    .map(|column| column.value_at(row))
                    .collect();
                buckets.push(GroupBucket::new(
                    key.clone(),
                    key_values,
                    vec![AggState::default(); aggs.len()],
                ));
                by_key.insert(key, index);
                index
            };
            for slot in 0..aggs.len() {
                if let Value::Int(count) = columns[1 + group_count + slot].value_at(row) {
                    buckets[entry].aggs[slot].count = buckets[entry].aggs[slot]
                        .count
                        .saturating_add(count.max(0) as u64);
                }
            }
        }
    }
    Ok(())
}

fn merge_partition(
    inmem: Partition,
    segments: &[PathBuf],
    schema: Arc<BatchSchema>,
    aggs: &[AggregateSpec],
    charger: crate::engine::executor::spill_quota::SpillCharger,
) -> Result<Vec<GroupBucket>, QueryError> {
    let mut by_key: GroupMap<usize> = GroupMap::default();
    let mut buckets: Vec<GroupBucket> = Vec::new();
    for b in inmem.buckets {
        let i = buckets.len();
        by_key.insert(b.key.clone(), i);
        buckets.push(b);
    }
    let layout = SpillLayout::for_schema(&schema, aggs)?;
    let mut key_buf = Vec::with_capacity(64);
    for path in segments {
        let capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(1);
        let mut r = crate::engine::executor::spill_quota::TrackedSpillReader::open(
            path,
            charger.clone(),
            capacity,
        )?;
        while let Some(batch) = spill::read_owned_batch(&mut r, schema.clone())? {
            merge_partial_batch_into(
                &batch,
                &schema,
                aggs,
                &layout,
                &mut by_key,
                &mut buckets,
                &mut key_buf,
            )?;
        }
    }
    Ok(buckets)
}

/// Shared-lane kernel used by [`AggregateProgram`] for an implicit group.
/// It computes one set of column statistics and fans those partials out to
/// every compatible aggregate slot. Returning `false` is a capability
/// decision, never a semantic approximation: MIN/MAX, DISTINCT and
/// conversion-sensitive lanes continue through the established per-slot
/// implementation.
fn update_shared_primitive_lane(
    states: &mut [AggState],
    specs: &[AggregateSpec],
    slots: &[usize],
    col: &ColumnVector,
    selection: Option<&[u32]>,
) -> bool {
    if slots.iter().any(|&slot| {
        matches!(
            specs[slot].func,
            AggregateKind::Min
                | AggregateKind::Max
                | AggregateKind::CountDistinct
                | AggregateKind::SumDistinct
                | AggregateKind::AvgDistinct
                | AggregateKind::CountStar
        )
    }) {
        return false;
    }
    let contains_bit = slots.iter().any(|&slot| specs[slot].func.is_bit());
    let needs_sum = slots
        .iter()
        .any(|&slot| matches!(specs[slot].func, AggregateKind::Sum | AggregateKind::Avg));
    let needs_variance = slots
        .iter()
        .any(|&slot| specs[slot].func.variance_flags().is_some());

    // Exact DECIMAL lanes can share one input walk across COUNT/SUM/AVG.
    // Conversion-sensitive aggregates (BIT/variance) intentionally keep the
    // established per-slot path: their DECIMAL coercion is not represented by
    // the exact fixed-point partial below.
    let decimal_lane = matches!(
        col,
        ColumnVector::Decimal32 { .. }
            | ColumnVector::Decimal64 { .. }
            | ColumnVector::Decimal128 { .. }
    );
    if decimal_lane
        && slots.iter().any(|&slot| {
            !matches!(
                specs[slot].func,
                AggregateKind::Count | AggregateKind::Sum | AggregateKind::Avg
            )
        })
    {
        return false;
    }
    if decimal_lane {
        let Some((sum, scale)) = (match selection {
            Some(selection) => sum_decimal_column_with_selection(col, selection),
            None => sum_decimal_column_with_nulls(col),
        }) else {
            // No states have been changed. Retry with the wide exact path.
            return false;
        };
        let count = match selection {
            Some(selection) => count_non_null_in_selection(col, selection),
            None => col.count_non_null() as u64,
        };
        for &slot in slots {
            let state = &mut states[slot];
            match specs[slot].func {
                AggregateKind::Count => state.count = state.count.saturating_add(count),
                AggregateKind::Sum | AggregateKind::Avg if count != 0 => {
                    accumulate_numeric_state_decimal_parts(&mut state.sum, sum, scale);
                    state.count = state.count.saturating_add(count);
                }
                AggregateKind::Sum | AggregateKind::Avg => {}
                _ => unreachable!("DECIMAL capability guard rejected this slot"),
            }
        }
        return true;
    }

    let mut count = 0u64;
    let mut exact_sum = 0i128;
    let mut float_sum_updated_per_row = false;
    let mut variance = ExtraAggState::default();
    let mut bit_and = u64::MAX;
    let mut bit_or = 0u64;
    let mut bit_xor = 0u64;

    match col {
        ColumnVector::I64 { data, .. } => {
            for_lane_rows(data.len(), selection, |row| {
                if col.is_null(row) {
                    return;
                }
                let value = data[row];
                count += 1;
                if needs_sum {
                    exact_sum += i128::from(value);
                }
                if needs_variance {
                    variance_update(&mut variance, value as f64);
                }
                if contains_bit {
                    let bits = value as u64;
                    bit_and &= bits;
                    bit_or |= bits;
                    bit_xor ^= bits;
                }
            });
        }
        ColumnVector::U64 { data, .. } => {
            for_lane_rows(data.len(), selection, |row| {
                if col.is_null(row) {
                    return;
                }
                let value = data[row];
                count += 1;
                if needs_sum {
                    exact_sum += i128::from(value);
                }
                if needs_variance {
                    variance_update(&mut variance, value as f64);
                }
                if contains_bit {
                    bit_and &= value;
                    bit_or |= value;
                    bit_xor ^= value;
                }
            });
        }
        ColumnVector::F64 { data, .. } if !contains_bit => {
            float_sum_updated_per_row = true;
            for_lane_rows(data.len(), selection, |row| {
                if col.is_null(row) {
                    return;
                }
                let value = data[row];
                count += 1;
                // Preserve the exact legacy addition order for approximate
                // SUM/AVG. A batch reduction would change the final low bits.
                for &slot in slots {
                    if matches!(specs[slot].func, AggregateKind::Sum | AggregateKind::Avg) {
                        accumulate_numeric_state_float(&mut states[slot].sum, value);
                    }
                }
                if needs_variance {
                    variance_update(&mut variance, value);
                }
            });
        }
        _ => return false,
    }

    for &slot in slots {
        let state = &mut states[slot];
        match specs[slot].func {
            AggregateKind::Count => state.count = state.count.saturating_add(count),
            AggregateKind::Sum | AggregateKind::Avg => {
                if count != 0 {
                    if !float_sum_updated_per_row {
                        accumulate_numeric_state_decimal_parts(&mut state.sum, exact_sum, 0);
                    }
                    state.count = state.count.saturating_add(count);
                }
            }
            AggregateKind::BitAnd | AggregateKind::BitOr | AggregateKind::BitXor => {
                if count != 0 {
                    let partial = match specs[slot].func {
                        AggregateKind::BitAnd => bit_and,
                        AggregateKind::BitOr => bit_or,
                        AggregateKind::BitXor => bit_xor,
                        _ => unreachable!(),
                    };
                    bit_update(state.extra_mut(), specs[slot].func, partial);
                }
            }
            AggregateKind::VarPop
            | AggregateKind::VarSamp
            | AggregateKind::StddevPop
            | AggregateKind::StddevSamp => {
                variance_merge(
                    state.extra_mut(),
                    variance.var_n,
                    variance.var_mean,
                    variance.var_m2,
                );
            }
            AggregateKind::CountStar
            | AggregateKind::Min
            | AggregateKind::Max
            | AggregateKind::CountDistinct
            | AggregateKind::SumDistinct
            | AggregateKind::AvgDistinct => unreachable!("capability guard rejected this slot"),
        }
    }
    true
}

/// Fold a whole column or a group's selected rows through the same aggregate
/// dispatch. Keep whole-column bitmap counts and decimal SIMD sums, and use
/// the selection-aware kernels without allocating an intermediate row list.
#[inline]
fn update_state_from_lane(
    state: &mut AggState,
    spec: &AggregateSpec,
    col: &ColumnVector,
    selection: Option<&[u32]>,
) -> Result<(), QueryError> {
    match selection {
        Some(rows) => update_state_from_lane_impl::<true>(state, spec, col, rows),
        None => update_state_from_lane_impl::<false>(state, spec, col, &[]),
    }
}

// Specialize the two traversal modes so sharing dispatch does not inhibit
// inlining or loop optimization in the DISTINCT and typed-lane hot paths.
fn update_state_from_lane_impl<const SELECTED: bool>(
    state: &mut AggState,
    spec: &AggregateSpec,
    col: &ColumnVector,
    rows: &[u32],
) -> Result<(), QueryError> {
    let selection = SELECTED.then_some(rows);
    if spec.func.uses_distinct_state() {
        for_lane_rows(col.len(), selection, |row| {
            if !col.is_null(row)
                && (!matches!(spec.func, AggregateKind::CountDistinct)
                    || !count_distinct_key_from_lane(state, col, row))
            {
                distinct_insert_from_lane(state, col, row);
            }
        });
        return Ok(());
    }
    if spec.func.uses_extra() {
        if update_extra_from_typed_lane(state, spec.func, col, selection)? {
            return Ok(());
        }
    } else {
        let non_null_count = || match selection {
            Some(rows) => count_non_null_in_selection(col, rows),
            None => col.count_non_null() as u64,
        };
        match spec.func {
            AggregateKind::CountStar => {
                let count = selection.map_or_else(|| col.len(), <[u32]>::len);
                state.count = state.count.saturating_add(count as u64);
                return Ok(());
            }
            AggregateKind::Count => {
                // Generic/Null lanes have no bitmap; use their logical NULL count.
                state.count = state.count.saturating_add(non_null_count());
                return Ok(());
            }
            AggregateKind::Sum | AggregateKind::Avg => {
                let decimal_sum = match selection {
                    Some(rows) => sum_decimal_column_with_selection(col, rows),
                    None => sum_decimal_column_with_nulls(col),
                };
                if let Some((sum, scale)) = decimal_sum {
                    accumulate_numeric_state_decimal_parts(&mut state.sum, sum, scale);
                    state.count = state.count.saturating_add(non_null_count());
                    return Ok(());
                }
                if update_numeric_from_typed_lane(state, col, selection) {
                    return Ok(());
                }
            }
            AggregateKind::Min | AggregateKind::Max => {}
            AggregateKind::BitAnd
            | AggregateKind::BitOr
            | AggregateKind::BitXor
            | AggregateKind::VarPop
            | AggregateKind::VarSamp
            | AggregateKind::StddevPop
            | AggregateKind::StddevSamp
            | AggregateKind::CountDistinct
            | AggregateKind::SumDistinct
            | AggregateKind::AvgDistinct => {
                unreachable!("extra-state / distinct aggregates handled by the guard above")
            }
        }
    }
    // Preserve left-to-right evaluation and stop at the first conversion error.
    match selection {
        Some(rows) => {
            for &row in rows {
                update_state(state, spec, col, row as usize)?;
            }
        }
        None => {
            for row in 0..col.len() {
                update_state(state, spec, col, row)?;
            }
        }
    }
    Ok(())
}

/// Iterate either a whole lane or the caller's bucket selection without
/// allocating an intermediate index vector.
#[inline]
fn for_lane_rows(len: usize, selection: Option<&[u32]>, mut f: impl FnMut(usize)) {
    match selection {
        Some(rows) => {
            for &row in rows {
                f(row as usize);
            }
        }
        None => {
            for row in 0..len {
                f(row);
            }
        }
    }
}

/// SUM/AVG kernel for primitive numeric lanes. The decimal lanes are handled
/// by the SIMD helpers above; this covers integer and floating columns and
/// avoids one `Value` construction plus numeric coercion per input row.
fn update_numeric_from_typed_lane(
    state: &mut AggState,
    col: &ColumnVector,
    selection: Option<&[u32]>,
) -> bool {
    let mut seen = 0u64;
    match col {
        ColumnVector::I64 { data, .. } => {
            let mut total = 0i128;
            for_lane_rows(data.len(), selection, |row| {
                if !col.is_null(row) {
                    total += i128::from(data[row]);
                    seen += 1;
                }
            });
            if seen != 0 {
                accumulate_numeric_state_decimal_parts(&mut state.sum, total, 0);
            }
        }
        ColumnVector::U64 { data, .. } => {
            let mut total = 0i128;
            for_lane_rows(data.len(), selection, |row| {
                if !col.is_null(row) {
                    total += i128::from(data[row]);
                    seen += 1;
                }
            });
            if seen != 0 {
                accumulate_numeric_state_decimal_parts(&mut state.sum, total, 0);
            }
        }
        ColumnVector::F64 { data, .. } => {
            // Preserve row order within a worker. A tree reduction would be
            // faster but changes MySQL-visible rounding for approximate SUM.
            for_lane_rows(data.len(), selection, |row| {
                if !col.is_null(row) {
                    accumulate_numeric_state_float(&mut state.sum, data[row]);
                    seen += 1;
                }
            });
        }
        _ => return false,
    }
    state.count = state.count.saturating_add(seen);
    true
}

/// Direct primitive-lane update for BIT_* and variance/stddev. Returns false
/// when conversion semantics are not trivially equivalent; the caller then
/// invokes the legacy evaluator for that lane.
fn update_extra_from_typed_lane(
    state: &mut AggState,
    func: AggregateKind,
    col: &ColumnVector,
    selection: Option<&[u32]>,
) -> Result<bool, QueryError> {
    if func.is_bit() {
        match col {
            ColumnVector::I64 { data, .. } => for_lane_rows(data.len(), selection, |row| {
                if !col.is_null(row) {
                    bit_update(state.extra_mut(), func, data[row] as u64);
                }
            }),
            ColumnVector::U64 { data, .. } => for_lane_rows(data.len(), selection, |row| {
                if !col.is_null(row) {
                    bit_update(state.extra_mut(), func, data[row]);
                }
            }),
            _ => return Ok(false),
        }
        return Ok(true);
    }

    match col {
        ColumnVector::I64 { data, .. } => for_lane_rows(data.len(), selection, |row| {
            if !col.is_null(row) {
                variance_update(state.extra_mut(), data[row] as f64);
            }
        }),
        ColumnVector::U64 { data, .. } => for_lane_rows(data.len(), selection, |row| {
            if !col.is_null(row) {
                variance_update(state.extra_mut(), data[row] as f64);
            }
        }),
        ColumnVector::F64 { data, .. } => for_lane_rows(data.len(), selection, |row| {
            if !col.is_null(row) {
                variance_update(state.extra_mut(), data[row]);
            }
        }),
        ColumnVector::Decimal32 { data, scale, .. } => {
            let factor = 10f64.powi(i32::from(*scale));
            for_lane_rows(data.len(), selection, |row| {
                if !col.is_null(row) {
                    variance_update(state.extra_mut(), data[row] as f64 / factor);
                }
            });
        }
        ColumnVector::Decimal64 { data, scale, .. } => {
            let factor = 10f64.powi(i32::from(*scale));
            for_lane_rows(data.len(), selection, |row| {
                if !col.is_null(row) {
                    variance_update(state.extra_mut(), data[row] as f64 / factor);
                }
            });
        }
        ColumnVector::Decimal128 { data, scale, .. } => {
            let factor = 10f64.powi(i32::from(*scale));
            for_lane_rows(data.len(), selection, |row| {
                if !col.is_null(row) {
                    variance_update(state.extra_mut(), data[row] as f64 / factor);
                }
            });
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// Materialise primitive DISTINCT inputs without calling the generic
/// `ColumnVector::value_at` dispatcher. Variable-width values still use the
/// owning fallback because the dedup table must retain the winning value.
#[inline]
fn distinct_insert_from_lane(state: &mut AggState, col: &ColumnVector, row: usize) {
    let value = match col {
        ColumnVector::I64 { data, .. } => Value::Int(data[row]),
        ColumnVector::U64 { data, .. } => Value::UInt(data[row]),
        ColumnVector::F64 { data, .. } => Value::Double(data[row]),
        ColumnVector::Decimal32 { data, scale, .. } => {
            Value::DecimalCompact(i64::from(data[row]), *scale)
        }
        ColumnVector::Decimal64 { data, scale, .. } => Value::DecimalCompact(data[row], *scale),
        ColumnVector::Decimal128 { data, scale, .. } => Value::Decimal128(data[row], *scale),
        _ => col.value_at(row),
    };
    state.distinct_insert(value);
}

/// COUNT(DISTINCT text/binary) receives a canonical byte lane from
/// `__DISTINCT_COLLATION_KEY`. Probe that lane by reference and allocate one
/// compact `KeyBytes` only on a miss instead of materializing `Value::Bytes`,
/// cloning it again into `ValueKey`, and retaining both owners.
#[inline]
fn count_distinct_key_from_lane(state: &mut AggState, col: &ColumnVector, row: usize) -> bool {
    let (offsets, data) = match col {
        ColumnVector::Utf8 { offsets, data, .. } | ColumnVector::Bytes { offsets, data, .. } => {
            (offsets, data)
        }
        _ => return false,
    };
    let bytes = &data[offsets[row] as usize..offsets[row + 1] as usize];
    state.count_distinct_key_bytes(bytes);
    true
}

/// Fold a single non-NULL `Value` into the BIT_* / variance state,
/// byte-for-byte matching `select::aggregate::BitAcc::update` /
/// `VarianceAcc::update`. NULLs are skipped by the caller.
fn update_extra_with_value(
    state: &mut AggState,
    func: AggregateKind,
    value: &Value,
) -> Result<(), QueryError> {
    if func.is_bit() {
        let n = crate::engine::expr::int_value(value)? as u64;
        bit_update(state.extra_mut(), func, n);
        return Ok(());
    }
    // Variance / stddev family.
    let x = crate::engine::expr::numeric(value)?;
    variance_update(state.extra_mut(), x);
    Ok(())
}

fn update_state(
    state: &mut AggState,
    spec: &AggregateSpec,
    col: &ColumnVector,
    row: usize,
) -> Result<(), QueryError> {
    if spec.func.uses_distinct_state() {
        if !col.is_null(row) {
            if !matches!(spec.func, AggregateKind::CountDistinct)
                || !count_distinct_key_from_lane(state, col, row)
            {
                distinct_insert_from_lane(state, col, row);
            }
        }
        return Ok(());
    }
    if spec.func.uses_extra() {
        if !col.is_null(row) {
            let v = col.value_at(row);
            update_extra_with_value(state, spec.func, &v)?;
        }
        return Ok(());
    }
    match spec.func {
        AggregateKind::CountStar => {
            state.count += 1;
        }
        AggregateKind::Count => {
            if !col.is_null(row) {
                state.count += 1;
            }
        }
        AggregateKind::Sum | AggregateKind::Avg => {
            if col.is_null(row) {
                return Ok(());
            }
            // Decimal fast paths — skip `value_at`'s `Value::DecimalCompact`
            // boxing and the `match` dispatch inside
            // `accumulate_numeric_state`. The lane's slice + scale already
            // carry everything `ExactDecimalAcc::add_parts` needs.
            if let Some((data, scale)) = col.as_decimal_i32_slice() {
                accumulate_numeric_state_decimal_parts(&mut state.sum, data[row] as i128, scale);
                state.count += 1;
                return Ok(());
            }
            if let Some((data, scale)) = col.as_decimal_i64_slice() {
                accumulate_numeric_state_decimal_parts(&mut state.sum, data[row] as i128, scale);
                state.count += 1;
                return Ok(());
            }
            if let Some((data, scale)) = col.as_decimal_i128_slice() {
                accumulate_numeric_state_decimal_parts(&mut state.sum, data[row], scale);
                state.count += 1;
                return Ok(());
            }
            let value = col.value_at(row);
            accumulate_numeric_state(&mut state.sum, &value)?;
            state.count += 1;
        }
        AggregateKind::Min | AggregateKind::Max => {
            if col.is_null(row) {
                return Ok(());
            }
            let store = if matches!(spec.func, AggregateKind::Min) {
                &mut state.min
            } else {
                &mut state.max
            };
            if let (Some(existing), Some(value)) = (
                store.as_ref().and_then(crate::types::value_str),
                col.valid_utf8_at(row),
            ) {
                if let Some(order) = crate::engine::expr::with_optional_compare_collation(
                    spec.comparison_collation,
                    || crate::engine::expr::compare_borrowed_strings(existing, value),
                ) {
                    let replace = if matches!(spec.func, AggregateKind::Min) {
                        order == std::cmp::Ordering::Greater
                    } else {
                        order == std::cmp::Ordering::Less
                    };
                    if replace {
                        *store = Some(col.value_at(row));
                    }
                    return Ok(());
                }
            }
            let v = col.value_at(row);
            match store {
                None => *store = Some(v),
                Some(existing) => {
                    let order = crate::engine::expr::with_optional_compare_collation(
                        spec.comparison_collation,
                        || crate::engine::expr::compare_values(existing, &v),
                    )?;
                    let take = if matches!(spec.func, AggregateKind::Min) {
                        order == std::cmp::Ordering::Greater
                    } else {
                        order == std::cmp::Ordering::Less
                    };
                    if take {
                        *existing = v;
                    }
                }
            }
        }
        AggregateKind::BitAnd
        | AggregateKind::BitOr
        | AggregateKind::BitXor
        | AggregateKind::VarPop
        | AggregateKind::VarSamp
        | AggregateKind::StddevPop
        | AggregateKind::StddevSamp
        | AggregateKind::CountDistinct
        | AggregateKind::SumDistinct
        | AggregateKind::AvgDistinct => {
            unreachable!("extra-state / distinct aggregates handled by the guard above")
        }
    }
    Ok(())
}

fn finalize_state(state: &AggState, spec: &AggregateSpec) -> Value {
    state.finalize(spec.func)
}

/// Convenience builder — encapsulates the "consume stream, finalize"
/// pattern used by callers that don't need incremental output.
#[cfg(test)]
pub fn run_hash_agg(
    stream: &mut dyn BatchOperator,
    group_columns: Vec<usize>,
    aggs: Vec<AggregateSpec>,
) -> Result<Vec<Vec<Value>>, QueryError> {
    let mut agg = HashAggregateCore::new(group_columns, aggs);
    agg.consume_stream(stream)?;
    agg.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_build_group_growth_uses_its_existing_capacity_reservation() {
        use crate::engine::executor::mem_budget::{self, MemBudget};
        let rows = 5000usize;
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountStar,
            input_column: 0,
        }];
        let mut aggregate = HashAggregateCore::new(vec![0], specs);
        let reserved =
            aggregate.direct_join_group_capacity_bytes(rows) + rows * std::mem::size_of::<u32>();
        let budget = Arc::new(MemBudget::new(reserved));
        mem_budget::with_query_budget(budget.clone(), || {
            assert!(aggregate.reserve_direct_join_capacity(reserved));
            let columns = vec![ColumnVector::I64 {
                data: (0..rows as i64).collect(),
                nulls: None,
            }];
            let handles = aggregate
                .prepare_direct_join_build_groups(&columns, &[(0, 0)])
                .unwrap();
            assert_eq!(handles.len(), rows);
            assert_eq!(handles[rows - 1], (rows - 1) as u32);
            assert_eq!(
                budget.used(),
                reserved,
                "precharged build groups must not be charged twice"
            );
            drop(handles);
            drop(aggregate);
            assert_eq!(budget.used(), 0);
        });
        let mut aggregate = HashAggregateCore::new(
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountStar,
                input_column: 0,
            }],
        );
        mem_budget::with_query_budget(budget.clone(), || {
            assert!(aggregate.reserve_direct_join_capacity(reserved));
            aggregate.prepare_direct_join_lazy_build_groups(rows);
            let columns = vec![ColumnVector::I64 {
                data: (0..rows as i64).collect(),
                nulls: None,
            }];
            for row in 0..rows {
                assert_eq!(
                    aggregate
                        .direct_join_build_group(&columns, row, &[(0, 0)])
                        .unwrap(),
                    row as u32
                );
            }
            assert_eq!(budget.used(), reserved);
            drop(aggregate);
            assert_eq!(budget.used(), 0);
        });
    }

    #[test]
    fn direct_probe_groups_spill_partials_and_release_reservations() {
        use crate::engine::executor::mem_budget::{self, MemBudget};
        let schema = Arc::new(BatchSchema::new(vec![
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
                name: "v".into(),
                column_type: ColumnType::Decimal {
                    precision: 16,
                    scale: 2,
                },
                source: Default::default(),
            },
        ]));
        let specs = [
            AggregateKind::Count,
            AggregateKind::Sum,
            AggregateKind::Avg,
            AggregateKind::Min,
            AggregateKind::Max,
            AggregateKind::CountDistinct,
        ]
        .into_iter()
        .map(|func| AggregateSpec {
            comparison_collation: None,
            func,
            input_column: 1,
        })
        .collect::<Vec<_>>();
        let run = |limit| {
            let root = tempfile::tempdir().unwrap();
            let budget = Arc::new(MemBudget::new(limit));
            let result = mem_budget::with_query_budget(budget.clone(), || {
                let mut aggregate = HashAggregateCore::with_budget(
                    vec![0],
                    specs.clone(),
                    schema.clone(),
                    budget.clone(),
                    root.path(),
                );
                aggregate.set_input_schema(schema.clone());
                assert!(aggregate
                    .reserve_direct_join_capacity(aggregate.direct_join_group_capacity_bytes(64)));
                aggregate.prepare_direct_join_probe_groups(64);
                for pass in 0..2 {
                    let batch = Batch::new(
                        schema.clone(),
                        vec![
                            ColumnVector::from_values(
                                (0..2000)
                                    .map(|i| if i == 0 { Value::Null } else { Value::Int(i) })
                                    .collect(),
                            ),
                            ColumnVector::from_values(
                                (0..2000)
                                    .map(|i| {
                                        if i % 7 == 0 {
                                            Value::Null
                                        } else {
                                            Value::DecimalCompact(i + pass, 2)
                                        }
                                    })
                                    .collect(),
                            ),
                        ],
                    );
                    for row in 0..2000 {
                        let handle = aggregate
                            .direct_join_probe_group(&batch, row, &[(0, 0)])
                            .unwrap();
                        aggregate
                            .update_direct_join_group(
                                handle,
                                &[],
                                0,
                                Some((&batch, row)),
                                &vec![Some(DirectJoinColumn::Probe(1)); specs.len()],
                            )
                            .unwrap();
                    }
                }
                aggregate.finish_direct_join_input(2, 4000).unwrap();
                let spilled = aggregate.metrics().did_spill;
                let mut output = aggregate.finalize_stream().unwrap();
                let mut rows = Vec::new();
                while let Some(batch) = output.next_batch().unwrap() {
                    for row in batch.visible_indices() {
                        rows.push(format!(
                            "{:?}",
                            batch
                                .columns()
                                .iter()
                                .map(|column| column.value_at(row))
                                .collect::<Vec<_>>()
                        ));
                    }
                }
                rows.sort();
                (rows, spilled)
            });
            assert_eq!(budget.used(), 0);
            result
        };
        let (expected, spilled) = run(64 * 1024 * 1024);
        assert!(!spilled);
        let (actual, spilled) = run(256 * 1024);
        assert!(spilled);
        assert_eq!(actual, expected);
    }

    #[test]
    fn borrowed_string_min_max_matches_value_oracle_with_collations() {
        let typed = ColumnVector::from_values(vec![
            Value::Null,
            Value::String("z".repeat(100)),
            Value::String("Alpha".into()),
            Value::String("alpha".into()),
            Value::String("Álpha".into()),
            Value::String("0009".into()),
            Value::String("0010".into()),
        ]);
        let generic = ColumnVector::Generic {
            values: (0..typed.len()).map(|row| typed.value_at(row)).collect(),
        };
        for collation in [
            None,
            Some(crate::charset::UTF8MB4_0900_AI_CI_ID),
            Some(
                crate::charset::resolve_collation("utf8mb4_0900_as_cs")
                    .unwrap()
                    .id,
            ),
        ] {
            for func in [AggregateKind::Min, AggregateKind::Max] {
                let spec = AggregateSpec {
                    comparison_collation: None,
                    func,
                    input_column: 0,
                };
                let mut fast = AggState::default();
                let mut oracle = AggState::default();
                crate::engine::expr::with_compare_collation(collation, || {
                    for row in 0..typed.len() {
                        update_state(&mut fast, &spec, &typed, row).unwrap();
                        update_state(&mut oracle, &spec, &generic, row).unwrap();
                    }
                });
                assert_eq!(finalize_state(&fast, &spec), finalize_state(&oracle, &spec));
            }
        }
    }

    #[test]
    fn compact_direct_join_counts_preserve_zero_for_unmatched_probe_values() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "b".into(),
                alias: None,
                name: "k".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "p".into(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
        ]));
        let keys = ColumnVector::I64 {
            data: vec![10, 20],
            nulls: None,
        };
        let probe = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::I64 {
                    data: vec![0],
                    nulls: None,
                },
                ColumnVector::I64 {
                    data: vec![7],
                    nulls: None,
                },
            ],
        );
        let mut aggregate = HashAggregateCore::new(
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Count,
                input_column: 1,
            }],
        );
        aggregate.set_input_schema(schema);
        aggregate.prepare_direct_join_count_groups(&keys).unwrap();
        aggregate
            .update_direct_join_group(
                0,
                std::slice::from_ref(&keys),
                0,
                Some((&probe, 0)),
                &[Some(DirectJoinColumn::Probe(1))],
            )
            .unwrap();
        aggregate
            .update_direct_join_group(
                0,
                std::slice::from_ref(&keys),
                0,
                Some((&probe, 0)),
                &[Some(DirectJoinColumn::Probe(1))],
            )
            .unwrap();
        aggregate
            .update_direct_join_group(
                1,
                std::slice::from_ref(&keys),
                1,
                None,
                &[Some(DirectJoinColumn::Probe(1))],
            )
            .unwrap();
        aggregate.finish_direct_join_input(1, 3).unwrap();
        let mut output = aggregate.finalize_stream().unwrap();
        let batch = output.next_batch().unwrap().unwrap();
        assert_eq!(batch.columns()[0].as_i64_slice(), Some(&[10, 20][..]));
        assert_eq!(batch.columns()[1].as_i64_slice(), Some(&[2, 0][..]));
        assert!(output.next_batch().unwrap().is_none());
    }

    #[test]
    fn join_group_append_keeps_keys_and_state_arena_in_lockstep() {
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountStar,
            input_column: 0,
        }];
        let mut groups = JoinAggregateGroups {
            precharged_group_limit: None,
            by_key: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
            by_i64: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
            key_values: Vec::new(),
            states: crate::engine::executor::hash_aggregate::AggregateStateArena::with_capacity(
                &specs, 2,
            ),
        };
        assert_eq!(groups.append_group(vec![Value::Int(10)]).unwrap(), 0);
        assert_eq!(groups.append_group(vec![Value::Int(20)]).unwrap(), 1);
        assert_eq!(groups.key_values.len(), groups.states.groups());
        assert!(groups.states.state_mut(1, 0).is_some());
    }
    use crate::engine::executor::vec::batch::{Batch, ColumnVector};
    use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
    use crate::engine::executor::vec::stream::MaterializedBatchOperator;
    use crate::types::ColumnType;
    use std::sync::Arc;

    fn schema_kv() -> Arc<BatchSchema> {
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
                name: "v".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
        ]))
    }

    fn make_batch(s: &Arc<BatchSchema>, pairs: &[(i64, i64)]) -> Batch {
        let ks = pairs
            .iter()
            .map(|(k, _)| Value::Int(*k))
            .collect::<Vec<_>>();
        let vs = pairs
            .iter()
            .map(|(_, v)| Value::Int(*v))
            .collect::<Vec<_>>();
        Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(ks), ColumnVector::from_values(vs)],
        )
    }

    fn schema_key_decimal(scale: u8) -> Arc<BatchSchema> {
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
                name: "d".into(),
                column_type: ColumnType::Decimal {
                    precision: 65,
                    scale,
                },
                source: Default::default(),
            },
        ]))
    }

    fn decimal_batch(schema: &Arc<BatchSchema>, keys: Vec<Value>, values: Vec<Value>) -> Batch {
        Batch::new(
            schema.clone(),
            vec![
                ColumnVector::from_values(keys),
                ColumnVector::from_values(values),
            ],
        )
    }

    #[test]
    fn decimal_state_column_composes_with_canonical_slots_and_preserves_nulls() {
        let schema = schema_key_decimal(2);
        let batch = decimal_batch(
            &schema,
            vec![Value::Int(1), Value::Int(1), Value::Int(2)],
            vec![
                Value::DecimalCompact(125, 2),
                Value::DecimalCompact(275, 2),
                Value::Null,
            ],
        );
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::Sum,
            input_column: 1,
        }];
        let mut arena = FixedWidthStateArena::try_new(&[0], &specs, &batch).unwrap();
        assert!(arena.decimal_state_columns[0].is_some());
        arena.consume_batch(&[0], &specs, &batch).unwrap();
        assert_eq!(arena.finalized_value(0, 0), Value::DecimalCompact(400, 2));
        assert_eq!(arena.finalized_value(1, 0), Value::Null);
        assert!(arena.state_chunks.is_empty());
    }

    #[test]
    fn dense_i64_fallback_rebuilds_batch_bucket_mapping() {
        let schema = schema_kv();
        let specs = vec![
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 1,
            },
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountStar,
                input_column: 0,
            },
        ];
        for repetitions in [1, 16] {
            let mut agg = HashAggregateCore::new(vec![0], specs.clone());
            // Exercise the canonical/dense layout used by large work areas.
            agg.fixed_width_probed = true;
            agg.consume_batch(&make_batch(&schema, &[(1, 10), (2, 20)]))
                .unwrap();
            assert!(agg.dense_i64_lookup.is_some());
            let mut pairs = vec![(1, 30), (2, 40)];
            for _ in 0..repetitions {
                pairs.extend([(3_000_000, 50), (3_000_000, 60)]);
            }
            agg.consume_batch(&make_batch(&schema, &pairs)).unwrap();
            assert!(agg.dense_i64_lookup.is_none());
            let partials = vec![(0..NUM_PARTITIONS)
                .map(|p| agg.take_partition_merged_snapshot(p).unwrap())
                .collect()];
            let mut rows = merge_worker_partition_snapshots(partials, &specs).unwrap();
            rows.sort_by_key(|r| match r[0] {
                Value::Int(k) => k,
                _ => unreachable!(),
            });
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int(1), Value::DecimalCompact(40, 0), Value::Int(2)],
                    vec![Value::Int(2), Value::DecimalCompact(60, 0), Value::Int(2)],
                    vec![
                        Value::Int(3_000_000),
                        Value::DecimalCompact(110 * repetitions, 0),
                        Value::Int(2 * repetitions)
                    ],
                ],
                "repetitions {repetitions}"
            );
        }
    }

    #[test]
    fn fixed_width_i64_groups_preserve_values_after_order_changes() {
        let schema = schema_key_decimal(2);
        for with_count in [false, true] {
            let mut specs = vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 1,
            }];
            if with_count {
                specs.push(AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::CountStar,
                    input_column: 0,
                });
            }
            let mut agg = HashAggregateCore::new(vec![0], specs.clone());
            for start in (0..40000).step_by(777) {
                let end = (start + 777).min(40000);
                let keys = (start..end)
                    .map(|i| Value::Int((i % 20000 + 1) as i64))
                    .collect();
                let values = (start..end)
                    .map(|i| Value::DecimalCompact((i % 20000 + 1) as i64, 2))
                    .collect();
                agg.consume_batch(&decimal_batch(&schema, keys, values))
                    .unwrap();
            }
            let partials = vec![(0..NUM_PARTITIONS)
                .map(|p| agg.take_partition_merged_snapshot(p).unwrap())
                .collect()];
            let mut rows = merge_worker_partition_snapshots(partials, &specs).unwrap();
            rows.sort_by_key(|row| match row[0] {
                Value::Int(k) => k,
                _ => unreachable!(),
            });
            assert_eq!(rows.len(), 20000);
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(
                    row[1],
                    Value::DecimalCompact(2 * (i + 1) as i64, 2),
                    "key {} count {}",
                    i + 1,
                    with_count
                );
                if with_count {
                    assert_eq!(row[2], Value::Int(2));
                }
            }
        }
    }

    #[test]
    fn parallel_decimal_group_totals_survive_overlapping_batches() {
        let schema = schema_key_decimal(2);
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::Sum,
            input_column: 1,
        }];
        let mut workers = (0..2)
            .map(|_| {
                let mut worker = HashAggregateCore::new(vec![0], specs.clone());
                worker.enable_parallel_i64_decimal_dedup();
                worker
            })
            .collect::<Vec<_>>();
        for (batch_index, start) in (0..18).step_by(4).enumerate() {
            let end = (start + 4).min(18);
            let keys = (start..end)
                .map(|i| Value::Int((i % 9 + 1) as i64))
                .collect();
            let values = (start..end)
                .map(|i| Value::DecimalCompact(if (i % 9 + 1) % 3 == 0 { 16000 } else { 10000 }, 2))
                .collect();
            let batch = decimal_batch(&schema, keys, values);
            workers[batch_index % 2].consume_batch(&batch).unwrap();
        }
        HashAggregateCore::align_parallel_partitioning(workers.iter_mut()).unwrap();
        let partials = workers
            .iter_mut()
            .map(|worker| {
                (0..NUM_PARTITIONS)
                    .map(|p| worker.take_partition_merged_snapshot(p).unwrap())
                    .collect()
            })
            .collect();
        let mut rows = merge_worker_partition_snapshots(partials, &specs).unwrap();
        rows.sort_by_key(|row| match row[0] {
            Value::Int(k) => k,
            _ => unreachable!(),
        });
        assert_eq!(rows.len(), 9);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[0], Value::Int((i + 1) as i64));
            assert_eq!(
                row[1],
                Value::DecimalCompact(if (i + 1) % 3 == 0 { 32000 } else { 20000 }, 2),
                "key {}",
                i + 1
            );
        }
    }

    #[test]
    fn compressed_parallel_decimal_runs_merge_and_apply_final_filter() {
        let schema = schema_key_decimal(2);
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::Sum,
            input_column: 1,
        }];
        let first = decimal_batch(
            &schema,
            vec![Value::Int(1), Value::Int(1), Value::Int(2)],
            vec![
                Value::DecimalCompact(100, 2),
                Value::DecimalCompact(50, 2),
                Value::DecimalCompact(200, 2),
            ],
        );
        let second = decimal_batch(
            &schema,
            vec![Value::Int(2), Value::Int(3)],
            vec![Value::DecimalCompact(150, 2), Value::DecimalCompact(400, 2)],
        );
        let mut arena = FixedWidthStateArena::try_new(&[0], &specs, &first).unwrap();
        arena.defer_i64_decimal_dedup = true;
        arena.consume_batch(&[0], &specs, &first).unwrap();
        arena.consume_batch(&[0], &specs, &second).unwrap();
        assert!(arena.hashes.is_empty());
        assert_eq!(arena.compressed_i64_decimal_runs.len(), 2);
        assert_eq!(
            arena.compressed_i64_decimal_allocated_bytes,
            arena.compressed_i64_decimal_runs.capacity()
                * std::mem::size_of::<CompressedI64DecimalRun>()
                + arena
                    .compressed_i64_decimal_runs
                    .iter()
                    .map(CompressedI64DecimalRun::allocated_bytes)
                    .sum::<usize>()
        );

        let rows = merge_compressed_i64_decimal_partials(
            vec![CompressedI64DecimalPartial {
                runs: std::mem::take(&mut arena.compressed_i64_decimal_runs),
                scale: arena.compressed_i64_decimal_scale.take().unwrap(),
            }],
            Some(&AggregateFinalFilter {
                aggregate_index: 0,
                comparison: AggregateFinalComparison::Gt,
                literal: Value::DecimalCompact(300, 2),
                decimal_literal: Some((300, 2)),
            }),
        )
        .unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(2), Value::DecimalCompact(350, 2)],
                vec![Value::Int(3), Value::DecimalCompact(400, 2)],
            ]
        );
    }

    #[test]
    fn compressed_parallel_decimal_runs_fall_back_on_batch_disorder() {
        let schema = schema_key_decimal(2);
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::Sum,
            input_column: 1,
        }];
        let ordered = decimal_batch(
            &schema,
            vec![Value::Int(1), Value::Int(2)],
            vec![Value::DecimalCompact(100, 2), Value::DecimalCompact(200, 2)],
        );
        let disordered = decimal_batch(
            &schema,
            vec![Value::Int(2), Value::Int(1)],
            vec![Value::DecimalCompact(50, 2), Value::DecimalCompact(25, 2)],
        );
        let mut arena = FixedWidthStateArena::try_new(&[0], &specs, &ordered).unwrap();
        arena.defer_i64_decimal_dedup = true;
        arena.consume_batch(&[0], &specs, &ordered).unwrap();
        arena.consume_batch(&[0], &specs, &disordered).unwrap();
        assert!(!arena.defer_i64_decimal_dedup);
        assert!(arena.compressed_i64_decimal_runs.is_empty());
        assert_eq!(arena.hashes.len(), 2);
        let mut got = (0..arena.hashes.len())
            .map(|group| (arena.keys[group], arena.finalized_value(group, 0)))
            .collect::<Vec<_>>();
        got.sort_by_key(|row| row.0);
        assert_eq!(
            got,
            vec![
                (1, Value::DecimalCompact(125, 2)),
                (2, Value::DecimalCompact(250, 2)),
            ]
        );
    }

    #[test]
    fn compressed_parallel_decimal_runs_fall_back_on_first_batch_disorder() {
        let schema = schema_key_decimal(2);
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::Sum,
            input_column: 1,
        }];
        for with_later_batch in [false, true] {
            let mut worker = HashAggregateCore::new(vec![0], specs.clone());
            worker.enable_parallel_i64_decimal_dedup();
            worker
                .consume_batch(&decimal_batch(
                    &schema,
                    vec![Value::Int(2), Value::Int(1), Value::Int(2)],
                    vec![
                        Value::DecimalCompact(100, 2),
                        Value::DecimalCompact(200, 2),
                        Value::DecimalCompact(50, 2),
                    ],
                ))
                .unwrap();
            if with_later_batch {
                worker
                    .consume_batch(&decimal_batch(
                        &schema,
                        vec![Value::Int(1), Value::Int(2)],
                        vec![Value::DecimalCompact(25, 2), Value::DecimalCompact(75, 2)],
                    ))
                    .unwrap();
            }
            let partials = vec![(0..NUM_PARTITIONS)
                .map(|partition| worker.take_partition_merged_snapshot(partition).unwrap())
                .collect()];
            let mut rows = merge_worker_partition_snapshots(partials, &specs).unwrap();
            rows.sort_by_key(|row| match row[0] {
                Value::Int(key) => key,
                _ => unreachable!(),
            });
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Int(1),
                        Value::DecimalCompact(if with_later_batch { 225 } else { 200 }, 2)
                    ],
                    vec![
                        Value::Int(2),
                        Value::DecimalCompact(if with_later_batch { 225 } else { 150 }, 2)
                    ],
                ],
                "first-batch disorder must merge repeated keys (later batch: {with_later_batch})"
            );
        }
    }

    #[test]
    fn compressed_parallel_decimal_runs_fall_back_on_selection() {
        use crate::engine::executor::vec::batch::Selection;

        let schema = schema_key_decimal(2);
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::Sum,
            input_column: 1,
        }];
        let batch = decimal_batch(
            &schema,
            vec![Value::Int(1), Value::Int(2), Value::Int(3)],
            vec![
                Value::DecimalCompact(100, 2),
                Value::DecimalCompact(200, 2),
                Value::Null,
            ],
        );
        for with_prefix in [false, true] {
            let mut worker = HashAggregateCore::new(vec![0], specs.clone());
            worker.enable_parallel_i64_decimal_dedup();
            if with_prefix {
                worker.consume_batch(&batch).unwrap();
            }
            worker
                .consume_batch(
                    &batch
                        .clone()
                        .with_selection(Selection::Indices(vec![1, 0, 2])),
                )
                .unwrap();
            let partials = vec![(0..NUM_PARTITIONS)
                .map(|partition| worker.take_partition_merged_snapshot(partition).unwrap())
                .collect()];
            let mut rows = merge_worker_partition_snapshots(partials, &specs).unwrap();
            rows.sort_by_key(|row| match row[0] {
                Value::Int(key) => key,
                _ => unreachable!(),
            });
            let factor = if with_prefix { 2 } else { 1 };
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int(1), Value::DecimalCompact(100 * factor, 2)],
                    vec![Value::Int(2), Value::DecimalCompact(200 * factor, 2)],
                    vec![Value::Int(3), Value::Null],
                ]
            );
        }
    }

    #[test]
    fn fixed_width_arena_selects_decimal_state_per_slot() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "g".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "a".into(),
                column_type: ColumnType::Decimal {
                    precision: 12,
                    scale: 2,
                },
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "b".into(),
                column_type: ColumnType::Decimal {
                    precision: 12,
                    scale: 3,
                },
                source: Default::default(),
            },
        ]));
        let batch = Batch::new(
            schema,
            vec![
                ColumnVector::from_values(vec![Value::Int(1), Value::Int(1), Value::Int(2)]),
                ColumnVector::from_values(vec![
                    Value::DecimalCompact(125, 2),
                    Value::DecimalCompact(275, 2),
                    Value::Null,
                ]),
                ColumnVector::from_values(vec![
                    Value::DecimalCompact(1000, 3),
                    Value::DecimalCompact(2500, 3),
                    Value::DecimalCompact(7000, 3),
                ]),
            ],
        );
        let specs = vec![
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 1,
            },
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountStar,
                input_column: 0,
            },
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 2,
            },
        ];
        let mut arena = FixedWidthStateArena::try_new(&[0], &specs, &batch).unwrap();
        assert!(arena.decimal_state_columns[0].is_some());
        assert!(arena.decimal_state_columns[1].is_none());
        assert!(arena.decimal_state_columns[2].is_some());
        arena.consume_batch(&[0], &specs, &batch).unwrap();
        assert_eq!(arena.finalized_value(0, 0), Value::DecimalCompact(400, 2));
        assert_eq!(arena.finalized_value(0, 1), Value::Int(2));
        assert_eq!(arena.finalized_value(0, 2), Value::DecimalCompact(3500, 3));
        assert_eq!(arena.finalized_value(1, 0), Value::Null);
        assert_eq!(arena.finalized_value(1, 1), Value::Int(1));
        assert_eq!(arena.finalized_value(1, 2), Value::DecimalCompact(7000, 3));
    }

    #[test]
    fn fixed_width_decimal_sum_promotes_overflow_without_losing_precision() {
        let schema = schema_key_decimal(0);
        let batch = decimal_batch(
            &schema,
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Decimal128(i128::MAX, 0), Value::DecimalCompact(1, 0)],
        );
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::Sum,
            input_column: 1,
        }];
        let mut arena = FixedWidthStateArena::try_new(&[0], &specs, &batch).unwrap();
        arena.consume_batch(&[0], &specs, &batch).unwrap();
        assert_eq!(
            arena.finalized_value(0, 0).to_display_string(),
            "170141183460469231731687303715884105728"
        );
        assert_eq!(
            arena.decimal_state_columns[0].as_ref().unwrap().status[0],
            COMPACT_SUM_PROMOTED
        );
    }

    #[test]
    fn sorted_i64_decimal_partials_merge_duplicates_without_rehash() {
        let partial = |keys: Vec<i64>, sums: Vec<i64>| FixedWidthDecimalPartitionPartial {
            key_kinds: vec![CompactKeyKind::I64],
            nulls: vec![0; keys.len()],
            keys,
            sums: sums
                .into_iter()
                .map(|magnitude| {
                    let mut state = NumericAggState::Empty;
                    accumulate_numeric_state_decimal_parts(&mut state, i128::from(magnitude), 2);
                    state
                })
                .collect(),
            narrow_scale: None,
            narrow_magnitudes: Vec::new(),
            narrow_status: Vec::new(),
            sorted_i64: true,
        };
        let rows = merge_fixed_width_decimal_partitions(vec![
            partial(vec![1, 3, 5], vec![100, 300, 500]),
            partial(vec![1, 2, 5], vec![25, 200, 50]),
        ]);
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(1), Value::DecimalCompact(125, 2)],
                vec![Value::Int(2), Value::DecimalCompact(200, 2)],
                vec![Value::Int(3), Value::DecimalCompact(300, 2)],
                vec![Value::Int(5), Value::DecimalCompact(550, 2)],
            ]
        );

        let filtered = merge_fixed_width_decimal_partitions_filtered(
            vec![
                partial(vec![1, 3, 5], vec![100, 300, 500]),
                partial(vec![1, 2, 5], vec![25, 200, 50]),
            ],
            Some(&AggregateFinalFilter {
                aggregate_index: 0,
                comparison: AggregateFinalComparison::Gt,
                literal: Value::DecimalCompact(250, 2),
                decimal_literal: Some((250, 2)),
            }),
        )
        .unwrap();
        assert_eq!(
            filtered,
            vec![
                vec![Value::Int(3), Value::DecimalCompact(300, 2)],
                vec![Value::Int(5), Value::DecimalCompact(550, 2)],
            ]
        );
    }

    #[test]
    fn fixed_width_composite_decimal_runs_and_parallel_merge_preserve_semantics() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "order_id".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "order_date".into(),
                column_type: ColumnType::Date,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "priority".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "amount".into(),
                column_type: ColumnType::Decimal {
                    precision: 20,
                    scale: 2,
                },
                source: Default::default(),
            },
        ]));
        let mut first_validity = Bitmap::new_all_valid(3);
        first_validity.set(2, false);
        let first = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::I64 {
                    data: vec![1, 1, 2],
                    nulls: None,
                },
                ColumnVector::Date32 {
                    data: vec![10, 10, 11],
                    nulls: None,
                },
                ColumnVector::I64 {
                    data: vec![0, 0, 0],
                    nulls: Some(first_validity.clone()),
                },
                ColumnVector::Decimal64 {
                    data: vec![100, 200, 0],
                    scale: 2,
                    nulls: Some(first_validity),
                },
            ],
        );
        let second = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::I64 {
                    data: vec![1, 3],
                    nulls: None,
                },
                ColumnVector::Date32 {
                    data: vec![10, 12],
                    nulls: None,
                },
                ColumnVector::I64 {
                    data: vec![0, 1],
                    nulls: None,
                },
                ColumnVector::Decimal64 {
                    data: vec![300, 400],
                    scale: 2,
                    nulls: None,
                },
            ],
        );
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::Sum,
            input_column: 3,
        }];
        let mut workers = [
            HashAggregateCore::new(vec![0, 1, 2], specs.clone()),
            HashAggregateCore::new(vec![0, 1, 2], specs.clone()),
        ];
        workers[0].consume_batch(&first).unwrap();
        workers[1].consume_batch(&second).unwrap();
        assert_eq!(workers[0].fixed_width.as_ref().unwrap().hashes.len(), 2);

        let mut partials = Vec::new();
        for worker in &mut workers {
            partials.push(
                (0..NUM_PARTITIONS)
                    .map(|partition| worker.take_partition_snapshot(partition))
                    .collect(),
            );
        }
        let mut rows = merge_worker_partition_snapshots(partials, &specs).unwrap();
        rows.sort_by_key(|row| match row[0] {
            Value::Int(key) => key,
            _ => i64::MAX,
        });
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][3], Value::DecimalCompact(600, 2));
        assert_eq!(rows[1][2], Value::Null);
        assert_eq!(rows[1][3], Value::Null);
        assert_eq!(rows[2][3], Value::DecimalCompact(400, 2));
    }

    #[test]
    fn fixed_width_composite_shared_spill_replays_without_generic_snapshots() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "k1".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "d".into(),
                column_type: ColumnType::Date,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "k2".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::Decimal {
                    precision: 20,
                    scale: 2,
                },
                source: Default::default(),
            },
        ]));
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::Sum,
            input_column: 3,
        }];
        let root = temp_root();
        let shared = Arc::new(SharedHashAggSpill::new(
            root.clone(),
            schema.clone(),
            &[0, 1, 2],
            specs.clone(),
        ));
        let budget = Arc::new(MemBudget::new(64 * 1024));
        let mut worker = HashAggregateCore::with_budget(
            vec![0, 1, 2],
            specs.clone(),
            schema.clone(),
            budget,
            &root,
        )
        .with_shared_spill(shared.clone());
        for pass in 0..2 {
            for start in (0..3000i64).step_by(400) {
                let end = (start + 400).min(3000);
                let groups: Vec<i64> = (start..end).collect();
                let batch = Batch::new(
                    schema.clone(),
                    vec![
                        ColumnVector::I64 {
                            data: groups.clone(),
                            nulls: None,
                        },
                        ColumnVector::Date32 {
                            data: groups.iter().map(|group| (group % 31) as i32).collect(),
                            nulls: None,
                        },
                        ColumnVector::I64 {
                            data: groups.iter().map(|group| group % 7).collect(),
                            nulls: None,
                        },
                        ColumnVector::Decimal64 {
                            data: vec![100 + pass; groups.len()],
                            scale: 2,
                            nulls: None,
                        },
                    ],
                );
                worker.consume_batch(&batch).unwrap();
            }
        }
        assert!(shared.written_partition_count() > 0);

        let worker_partials: Vec<_> = (0..NUM_PARTITIONS)
            .map(|partition| worker.take_partition_merged_snapshot(partition).unwrap())
            .collect();
        let spill_partials = shared.partition_snapshots().unwrap();
        assert!(spill_partials.iter().flatten().any(|partial| {
            partial.fixed_width_decimal.is_some() || !partial.buckets.is_empty()
        }));
        let mut rows =
            merge_worker_partition_snapshots(vec![worker_partials, spill_partials], &specs)
                .unwrap();
        rows.sort_by_key(|row| match row[0] {
            Value::Int(key) => key,
            _ => i64::MAX,
        });
        assert_eq!(rows.len(), 3000);
        assert!(rows
            .iter()
            .all(|row| row[3] == Value::DecimalCompact(201, 2)));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn dense_i64_lookup_grows_geometrically_without_losing_slots() {
        let mut lookup = DenseI64Lookup {
            min: 0,
            slots: vec![DENSE_I64_EMPTY; 8],
            null_slot: None,
        };
        let mut expansions = 0usize;
        let mut previous_len = lookup.slots.len();
        for value in 0..150_000i64 {
            let index = lookup.index_for(value).unwrap();
            if lookup.slots.len() != previous_len {
                expansions += 1;
                previous_len = lookup.slots.len();
            }
            assert_eq!(lookup.slots[index], DENSE_I64_EMPTY);
            lookup.slots[index] = value as u32;
        }
        assert!(expansions < 20, "unexpected expansion count: {expansions}");
        for value in 0..150_000i64 {
            let index = lookup.index_for(value).unwrap();
            assert_eq!(lookup.slots[index], value as u32);
        }

        // The same amortised invariant holds when a stream arrives in
        // descending key order.
        let mut descending = DenseI64Lookup {
            min: 0,
            slots: vec![DENSE_I64_EMPTY; 8],
            null_slot: None,
        };
        for value in (-150_000i64..=0).rev() {
            let index = descending.index_for(value).unwrap();
            descending.slots[index] = (value + 150_000) as u32;
        }
        for value in -150_000i64..=0 {
            let index = descending.index_for(value).unwrap();
            assert_eq!(descending.slots[index], (value + 150_000) as u32);
        }
    }

    #[test]
    fn dictionary_group_encoder_declines_near_unique_strings() {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "category".into(),
            column_type: ColumnType::Blob,
            source: Default::default(),
        }]));
        let values = (0..64)
            .map(|value| Value::String(format!("category-{value:04}")))
            .collect();
        let batch = Batch::new(schema, vec![ColumnVector::from_values(values)]);
        assert!(DictionaryGroupEncoder::try_build(&[0], &batch).is_none());
    }

    #[test]
    fn raw_token_caches_reserve_before_parallel_allocation_and_release() {
        use crate::engine::executor::mem_budget::with_query_budget;
        let budget = Arc::new(MemBudget::new(63 * 1024 * 1024));
        std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for _ in 0..8 {
                let budget = budget.clone();
                workers.push(scope.spawn(move || {
                    with_query_budget(budget, || {
                        (0..8).map(|_| RawTokenCache::new()).collect::<Vec<_>>()
                    })
                }));
            }
            let caches = workers
                .into_iter()
                .flat_map(|w| w.join().unwrap())
                .collect::<Vec<_>>();
            let bytes = caches
                .iter()
                .map(|cache| cache.entries.capacity() * std::mem::size_of::<RawTokenCacheEntry>())
                .sum::<usize>();
            assert!(bytes > 0 && bytes <= budget.limit() / 16);
            assert_eq!(budget.used(), bytes);
            assert!(caches.iter().all(|cache| cache.allocated_bytes() == 0));
            drop(caches);
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn optional_raw_token_cache_refusal_keeps_query_usable() {
        use crate::engine::executor::mem_budget::with_query_budget;
        let budget = Arc::new(MemBudget::new(8 * 1024 * 1024));
        let pressure = crate::engine::executor::mem_budget::OwnedMemCharge::new(
            budget.clone(),
            budget.limit(),
        )
        .unwrap();
        with_query_budget(budget.clone(), || {
            let mut cache = RawTokenCache::new();
            assert!(cache.entries.is_empty());
            let token = DictionaryToken {
                local: 1,
                raw_local: 1,
                stable_hash: 7,
            };
            cache.insert(b"alpha", token);
            assert!(cache.get(b"alpha", &[Box::<str>::from("alpha")]).is_none());
            assert!(crate::engine::cancel::check_cancelled().is_ok());
        });
        drop(pressure);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn raw_token_cache_verifies_bytes_after_hash_slot_match() {
        let mut cache = RawTokenCache::new();
        let token = DictionaryToken {
            local: 1,
            raw_local: 1,
            stable_hash: 7,
        };
        let representatives = vec![Box::<str>::from("alpha")];
        cache.insert(b"alpha", token);
        assert_eq!(cache.get(b"alpha", &representatives).unwrap().local, 1);

        // Simulate an adversarial slot entry carrying the requested hash but
        // a different representative. The cache must miss and force the
        // caller through canonical collation instead of accepting a
        // probabilistic equality result.
        let other_hash = crate::engine::executor::vec::key_encode::fxhash(b"beta");
        let slot = ((other_hash as usize) & cache.set_mask()) * RawTokenCache::WAYS;
        cache.entries[slot] = RawTokenCacheEntry {
            hash: other_hash,
            token,
        };
        assert!(cache.get(b"beta", &representatives).is_none());
    }

    #[test]
    fn dictionary_group_encoder_samples_beyond_clustered_unique_prefix() {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "category".into(),
            column_type: ColumnType::Blob,
            source: Default::default(),
        }]));
        let values = (0..1024)
            .map(|row| Value::String(format!("category-{:04}", row % 128)))
            .collect();
        let batch = Batch::new(schema, vec![ColumnVector::from_values(values)]);
        assert!(DictionaryGroupEncoder::try_build(&[0], &batch).is_some());
    }

    #[test]
    fn dictionary_group_encoder_reuses_string_and_integer_tokens() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "category".into(),
                column_type: ColumnType::Blob,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "size".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
        ]));
        let batch = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::from_values(vec![
                    Value::String("alpha".into()),
                    Value::String("alpha".into()),
                    Value::String("beta".into()),
                ]),
                ColumnVector::from_values(vec![Value::Int(7), Value::Int(7), Value::Int(7)]),
            ],
        );
        let mut encoder = DictionaryGroupEncoder::try_build(&[0, 1], &batch).unwrap();
        let mut scratch = Vec::new();
        let first = encoder.encode_row(&batch, 0, &mut scratch).unwrap();
        let repeated = encoder.encode_row(&batch, 1, &mut scratch).unwrap();
        let other = encoder.encode_row(&batch, 2, &mut scratch).unwrap();

        assert_eq!(first, repeated);
        assert_ne!(first, other);

        let DictionaryColumnSource::Utf8 {
            raw_to_token,
            canonical_to_token,
            canonical_keys,
            raw_values,
            raw_value_heap_bytes,
            canonical_key_capacity_bytes,
            ..
        } = &encoder.columns[0].source
        else {
            panic!("expected UTF-8 dictionary")
        };
        assert_eq!(raw_to_token.len(), 2);
        assert_eq!(
            raw_values
                .iter()
                .map(|value| value.as_ref())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );
        let entry_bytes = HASH_ENTRY_OVERHEAD
            + std::mem::size_of::<Arc<[u8]>>()
            + std::mem::size_of::<DictionaryToken>();
        let recomputed = raw_to_token
            .allocated_bytes()
            .saturating_add(canonical_to_token.capacity().saturating_mul(entry_bytes))
            .saturating_add(*canonical_key_capacity_bytes)
            .saturating_add(
                canonical_keys
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Arc<[u8]>>()),
            )
            .saturating_add(
                raw_values
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Box<str>>()),
            )
            .saturating_add(*raw_value_heap_bytes);
        let DictionaryColumnSource::I64 { values, .. } = &encoder.columns[1].source else {
            panic!("expected integer dictionary")
        };
        let recomputed = recomputed.saturating_add(values.capacity().saturating_mul(
            HASH_ENTRY_OVERHEAD
                + std::mem::size_of::<i64>()
                + std::mem::size_of::<DictionaryToken>(),
        ));
        assert_eq!(encoder.allocated_bytes(), recomputed);
        assert_eq!(
            *canonical_key_capacity_bytes,
            canonical_to_token
                .keys()
                .map(|key| key.len() + ARC_SLICE_HEADER_BYTES)
                .sum::<usize>()
        );

        let reverse = Batch::new(
            schema,
            vec![
                ColumnVector::from_values(vec![
                    Value::String("beta".into()),
                    Value::String("alpha".into()),
                ]),
                ColumnVector::from_values(vec![Value::Int(7), Value::Int(7)]),
            ],
        );
        let mut reverse_encoder = DictionaryGroupEncoder::try_build(&[0, 1], &reverse).unwrap();
        let _beta = reverse_encoder
            .encode_row(&reverse, 0, &mut scratch)
            .unwrap();
        let reverse_alpha = reverse_encoder
            .encode_row(&reverse, 1, &mut scratch)
            .unwrap();
        assert_ne!(
            first.0, reverse_alpha.0,
            "local tokens should reflect encounter order"
        );
        assert_eq!(first.1, reverse_alpha.1, "fingerprint must be stable");
    }

    #[test]
    fn dictionary_group_encoder_keeps_wide_primitive_values_direct() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "user_id".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "phrase".into(),
                column_type: ColumnType::Blob,
                source: Default::default(),
            },
        ]));
        let first_id = i64::MAX - 7;
        let second_id = i64::MIN + 11;
        let batch = Batch::new(
            schema,
            vec![
                ColumnVector::from_values(vec![
                    Value::Int(first_id),
                    Value::Int(first_id),
                    Value::Int(second_id),
                ]),
                ColumnVector::from_values(vec![
                    Value::String("repeat".into()),
                    Value::String("repeat".into()),
                    Value::String("repeat".into()),
                ]),
            ],
        );
        let mut encoder = DictionaryGroupEncoder::try_build(&[0, 1], &batch).unwrap();
        assert!(encoder.direct_primitives);
        let mut scratch = Vec::new();
        let first = encoder.encode_row(&batch, 0, &mut scratch).unwrap();
        let first_canonical = scratch.clone();
        let repeated = encoder.encode_row(&batch, 1, &mut scratch).unwrap();
        assert_eq!(first, repeated);
        assert_eq!(first_canonical, scratch);
        let second = encoder.encode_row(&batch, 2, &mut scratch).unwrap();
        assert_ne!(first.0, second.0);
        let DictionaryColumnSource::I64 { values, .. } = &encoder.columns[0].source else {
            panic!("expected integer lane")
        };
        assert!(
            values.is_empty(),
            "direct fixed-width lanes must not allocate a high-cardinality dictionary"
        );
    }

    #[test]
    fn dictionary_count_arena_preserves_collation_and_representatives() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "user_id".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "phrase".into(),
                column_type: ColumnType::Blob,
                source: Default::default(),
            },
        ]));
        let batch = Batch::new(
            schema,
            vec![
                ColumnVector::from_values(vec![
                    Value::Int(i64::MAX - 1),
                    Value::Int(i64::MAX - 1),
                    Value::Int(i64::MAX - 1),
                    Value::Int(7),
                ]),
                ColumnVector::from_values(vec![
                    Value::String("Alpha".into()),
                    Value::String("alpha".into()),
                    Value::String("Beta".into()),
                    Value::String("Alpha".into()),
                ]),
            ],
        );
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountStar,
            input_column: 0,
        }];
        let mut encoder = DictionaryGroupEncoder::try_build(&[0, 1], &batch).unwrap();
        let mut arena =
            DictionaryCountArena::new(batch.num_rows(), 0, specs.len(), encoder.is_single_utf8());
        let mut scratch = Vec::new();
        arena
            .consume_batch(&mut encoder, &specs, &batch, &mut scratch)
            .unwrap();
        let encoder = Arc::new(encoder);
        let mut buckets = Vec::new();
        for partition in 0..NUM_PARTITIONS {
            let compact = arena.take_partition(partition);
            buckets.extend(
                DictionaryCountPartitionPartial {
                    partition: compact,
                    encoder: Arc::clone(&encoder),
                    raw_token_width: arena.raw_token_width,
                    n_aggs: arena.n_aggs,
                }
                .into_buckets(),
            );
        }
        let mut got = buckets
            .into_iter()
            .map(|bucket| {
                (
                    crate::storage::value_to_string(&bucket.key_values[0]),
                    crate::storage::value_to_string(&bucket.key_values[1]),
                    bucket.aggs[0].count,
                )
            })
            .collect::<Vec<_>>();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("7".into(), "Alpha".into(), 1),
                ((i64::MAX - 1).to_string(), "Alpha".into(), 2),
                ((i64::MAX - 1).to_string(), "Beta".into(), 1),
            ]
        );
    }

    #[test]
    fn dictionary_count_payload_growth_bounds_slack_and_keeps_lanes_aligned() {
        let mut partition = DictionaryCountPartition::new(1, 2, 2, true);
        for key in 0..100_000u128 {
            let group = partition.probe_or_insert(key, &[7, 9], 2, 2);
            partition.increment_count(group, 0, 2, key as u64 + 1);
            partition.increment_count(group, 1, 2, 3);
            assert!(partition.keys.capacity() <= partition.keys.len() * 5 / 4 + 64);
            assert_eq!(partition.counts.capacity(), partition.keys.capacity() * 2);
            assert_eq!(
                partition.raw_tokens.capacity(),
                partition.keys.capacity() * 2
            );
        }
        for key in 0..100_000u128 {
            let group = partition.probe_or_insert(key, &[0, 0], 2, 2);
            assert_eq!(partition.count(group, 0, 2), key as u64 + 1);
            assert_eq!(partition.count(group, 1, 2), 3);
            assert_eq!(&partition.raw_tokens[group * 2..group * 2 + 2], &[7, 9]);
        }
    }

    #[test]
    fn dictionary_count_promotes_only_overflowing_group_state() {
        let mut partition = DictionaryCountPartition::new(1, 0, 1, false);
        let group = partition.insert_unique(7, &[], 0, 1);
        partition.increment_count(group, 0, 1, u64::from(u32::MAX));
        assert_eq!(partition.count(group, 0, 1), u64::from(u32::MAX));
        assert!(partition.overflow_counts.is_empty());
        partition.increment_count(group, 0, 1, 9);
        assert_eq!(partition.count(group, 0, 1), u64::from(u32::MAX) + 9);
        assert_eq!(partition.overflow_counts.len(), 1);
    }

    #[test]
    fn dictionary_count_partition_topk_materializes_only_candidates() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "user_id".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "minute".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "phrase".into(),
                column_type: ColumnType::Blob,
                source: Default::default(),
            },
        ]));
        let mut users = Vec::new();
        let mut minutes = Vec::new();
        let mut phrases = Vec::new();
        for user in 0..100u64 {
            for _ in 0..=(user % 4) {
                users.push(Value::UInt(user));
                minutes.push(Value::Int((user % 3) as i64));
                phrases.push(Value::String(format!("phrase-{}", user % 4)));
            }
        }
        let batch = Batch::new(
            schema,
            vec![
                ColumnVector::from_values(users),
                ColumnVector::from_values(minutes),
                ColumnVector::from_values(phrases),
            ],
        );
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountStar,
            input_column: 0,
        }];
        let mut encoder = DictionaryGroupEncoder::try_build(&[0, 1, 2], &batch).unwrap();
        assert!(encoder.compact_count_eligible);
        assert!(encoder.columns.iter().any(|column| column.direct));
        let mut arena = DictionaryCountArena::new(batch.num_rows(), 0, 1, false);
        arena
            .consume_batch(&mut encoder, &specs, &batch, &mut Vec::new())
            .unwrap();
        let partition = arena
            .partitions
            .iter()
            .enumerate()
            .max_by_key(|(_, partition)| partition.keys.len())
            .map(|(index, _)| index)
            .unwrap();
        assert!(arena.partitions[partition].keys.len() > 1);
        let expected = (0..arena.partitions[partition].keys.len())
            .map(|group| arena.partitions[partition].count(group, 0, 1))
            .max()
            .unwrap();
        let compact = arena.take_partition(partition);
        let partial = PartitionPartial {
            buckets: Vec::new(),
            fixed_width_decimal: None,
            compressed_i64_decimal: None,
            dictionary_count: Some(DictionaryCountPartitionPartial {
                partition: compact,
                encoder: Arc::new(encoder),
                raw_token_width: 0,
                n_aggs: 1,
            }),
        };
        let rows = merge_worker_partition_snapshots_filtered_topk(
            vec![vec![Some(partial)]],
            &specs,
            None,
            Some(AggregateCountTopKHint {
                aggregate_index: 0,
                k: 1,
                descending: true,
            }),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].last(), Some(&Value::Int(expected as i64)));
    }

    #[test]
    fn dictionary_spill_scratch_pressure_does_not_cancel_recovered_query() {
        use crate::engine::executor::mem_budget::{
            controlled_memory_snapshot, with_query_budget, with_test_effective_memory_limit,
        };
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "category".into(),
            column_type: ColumnType::Blob,
            source: Default::default(),
        }]));
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(
                (0..2048)
                    .map(|row| Value::String(format!("{:04}{}", row % 256, "x".repeat(64 * 1024))))
                    .collect(),
            )],
        );
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountStar,
            input_column: 0,
        }];
        let budget = Arc::new(MemBudget::new(64 * 1024));
        let shared = Arc::new(SharedHashAggSpill::new(
            root.path().into(),
            schema.clone(),
            &[0],
            specs.clone(),
        ));
        let mut agg = HashAggregateCore::with_budget(
            vec![0],
            specs.clone(),
            schema,
            budget.clone(),
            root.path(),
        )
        .with_shared_spill(shared.clone());
        let rejected_before = controlled_memory_snapshot().3;
        with_test_effective_memory_limit(1024 * 1024, || {
            with_query_budget(budget.clone(), || {
                agg.consume_batch(&batch).unwrap();
                assert!(agg.metrics().did_spill);
                assert!(
                    controlled_memory_snapshot().3 > rejected_before,
                    "the regression must exercise scratch admission rejection"
                );
                assert!(budget.terminal_oom().is_none());
                crate::engine::cancel::check_cancelled().unwrap();
            });
        });
        let partials = shared.partition_snapshots().unwrap();
        let rows = merge_worker_partition_snapshots(vec![partials], &specs).unwrap();
        assert_eq!(rows.len(), 256);
        assert!(rows.iter().all(|row| row[1] == Value::Int(8)));
    }

    #[test]
    fn dictionary_count_spill_releases_the_complete_dictionary_epoch() {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "category".into(),
            column_type: ColumnType::Blob,
            source: Default::default(),
        }]));
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(
                (0..4096)
                    .map(|row| Value::String(format!("category-{:04}", row % 128)))
                    .collect(),
            )],
        );
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountStar,
            input_column: 0,
        }];
        let root = temp_root();
        let budget = Arc::new(MemBudget::new(64 * 1024));
        let shared = Arc::new(SharedHashAggSpill::new(
            root.clone(),
            schema.clone(),
            &[0],
            specs.clone(),
        ));
        let mut agg = HashAggregateCore::with_budget(vec![0], specs.clone(), schema, budget, &root)
            .with_shared_spill(Arc::clone(&shared));
        agg.consume_batch(&batch).unwrap();
        assert!(agg.metrics().did_spill);
        assert!(agg.dictionary_count.is_none());
        assert!(agg.dictionary_encoder.is_none());
        assert!(
            !agg.dictionary_probed,
            "the next batch must be allowed to start a bounded dictionary epoch"
        );
        let partials = shared.partition_snapshots().unwrap();
        let rows = merge_worker_partition_snapshots(vec![partials], &specs).unwrap();
        assert_eq!(rows.len(), 128);
        assert!(rows.iter().all(|row| row[1] == Value::Int(32)));
        drop(agg);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn dictionary_repack_during_build_keeps_future_probes_exact() {
        let schema = Arc::new(BatchSchema::new(
            ["id", "bucket", "label"]
                .into_iter()
                .map(|name| BatchField {
                    table: "t".into(),
                    alias: None,
                    name: name.into(),
                    column_type: if name == "label" {
                        ColumnType::VarString
                    } else {
                        ColumnType::LongLong
                    },
                    source: Default::default(),
                })
                .collect(),
        ));
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountStar,
            input_column: 0,
        }];
        let root = temp_root();
        let budget = Arc::new(MemBudget::new(512 * 1024 * 1024));
        let shared = Arc::new(SharedHashAggSpill::new(
            root.clone(),
            schema.clone(),
            &[0, 1, 2],
            specs.clone(),
        ));
        let mut agg = HashAggregateCore::with_budget(
            vec![0, 1, 2],
            specs.clone(),
            schema.clone(),
            budget,
            &root,
        )
        .with_shared_spill(shared);
        let make = |first: i64, rows: usize, prefix: bool| {
            Batch::new(
                schema.clone(),
                vec![
                    ColumnVector::I64 {
                        data: (0..rows)
                            .map(|row| if prefix { 0 } else { first + row as i64 })
                            .collect(),
                        nulls: None,
                    },
                    ColumnVector::I64 {
                        data: (0..rows).map(|row| row as i64 % 4 - 2).collect(),
                        nulls: None,
                    },
                    ColumnVector::from_values(vec![Value::String("label".into()); rows]),
                ],
            )
        };
        agg.consume_batch(&make(0, 512, true)).unwrap();
        assert_eq!(agg.dictionary_token_count(0), 1);
        let data = make(1, 40_000, false);
        agg.consume_batch(&data).unwrap();
        assert!(agg.dictionary_repack_attempted);
        assert_eq!(agg.dictionary_token_count(0), 0);
        agg.consume_batch(&data).unwrap();
        agg.consume_batch(&make(40_001, 1000, false)).unwrap();
        assert!(!agg.metrics().did_spill);
        let partials = (0..NUM_PARTITIONS)
            .map(|partition| agg.take_partition_merged_snapshot(partition).unwrap())
            .collect();
        let rows = merge_worker_partition_snapshots(vec![partials], &specs).unwrap();
        assert_eq!(rows.len(), 41_004);
        for row in rows {
            let Value::Int(id) = row[0] else { panic!("id") };
            assert_eq!(
                row[3],
                Value::Int(if id == 0 {
                    128
                } else if id <= 40_000 {
                    2
                } else {
                    1
                })
            );
        }
        drop(agg);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn generic_dictionary_cache_evicts_before_mandatory_state_spill() {
        let schema = Arc::new(BatchSchema::new(
            [
                ("label", ColumnType::VarString),
                ("v", ColumnType::LongLong),
            ]
            .into_iter()
            .map(|(name, column_type)| BatchField {
                table: "t".into(),
                alias: None,
                name: name.into(),
                column_type,
                source: crate::types::ColumnSource {
                    collation_id: Some(255),
                    ..Default::default()
                },
            })
            .collect(),
        ));
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountDistinct,
            input_column: 1,
        }];
        let root = temp_root();
        let budget = Arc::new(MemBudget::new(128 * 1024 * 1024));
        let mut agg =
            HashAggregateCore::with_budget(vec![0], specs, schema.clone(), budget.clone(), &root);
        let seed = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::from_values(vec![Value::String("dictionary-seed".into()); 64]),
                ColumnVector::from_values((0..64).map(Value::Int).collect()),
            ],
        );
        // The first batch retires the short-string packed-key probe; the
        // second admits the dictionary cache that this test will reclaim.
        agg.consume_batch(&seed).unwrap();
        agg.consume_batch(&seed).unwrap();
        assert!(agg.dictionary_encoder.is_some());
        let pressure = crate::engine::executor::mem_budget::OwnedMemCharge::new(
            budget.clone(),
            budget.limit() - budget.used() - 512 * 1024,
        )
        .unwrap();
        agg.consume_batch(&Batch::new(
            schema.clone(),
            vec![
                ColumnVector::from_values(
                    (1..=1_000)
                        .flat_map(|id| {
                            let label = format!("label-{id}");
                            [Value::String(label.clone()), Value::String(label)]
                        })
                        .collect(),
                ),
                ColumnVector::from_values(
                    (1..=1_000)
                        .flat_map(|id| [Value::Int(id), Value::Int(id)])
                        .collect(),
                ),
            ],
        ))
        .unwrap();
        assert!(agg.dictionary_encoder.is_none());
        assert!(agg.dictionary_probed);
        assert!(!agg.metrics().did_spill);
        drop(pressure);
        agg.consume_batch(&Batch::new(
            schema,
            vec![
                ColumnVector::from_values(vec![Value::String("DICTIONARY-SEED".into())]),
                ColumnVector::from_values(vec![Value::Int(100)]),
            ],
        ))
        .unwrap();
        let rows = agg.finalize().unwrap();
        assert_eq!(rows.len(), 1_001);
        assert_eq!(
            rows.iter().filter(|row| row[1] == Value::Int(65)).count(),
            1
        );
        assert_eq!(
            rows.iter().filter(|row| row[1] == Value::Int(1)).count(),
            1_000
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn dictionary_finalize_retires_lookup_capacity_without_losing_state() {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "label".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        }]));
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(
                (0..4096)
                    .map(|row| Value::String(format!("label-{}", row % 128)))
                    .collect(),
            )],
        );
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountStar,
            input_column: 0,
        }];
        let root = temp_root();
        let budget = Arc::new(MemBudget::new(512 * 1024 * 1024));
        let shared = Arc::new(SharedHashAggSpill::new(
            root.clone(),
            schema.clone(),
            &[0],
            specs.clone(),
        ));
        let mut agg =
            HashAggregateCore::with_budget(vec![0], specs.clone(), schema, budget.clone(), &root)
                .with_shared_spill(shared.clone());
        agg.consume_batch(&batch).unwrap();
        assert!(agg.dictionary_count.is_some());
        let before = budget.used();
        agg.retire_dictionary_build_lookups().unwrap();
        assert!(budget.used() < before);
        assert!(agg
            .dictionary_count
            .as_ref()
            .unwrap()
            .partitions
            .iter()
            .all(|part| part.slots.capacity() == 0));
        let partials = (0..NUM_PARTITIONS)
            .map(|partition| agg.take_partition_merged_snapshot(partition).unwrap())
            .collect();
        let rows = merge_worker_partition_snapshots(vec![partials], &specs).unwrap();
        assert_eq!(rows.len(), 128);
        assert!(rows.iter().all(|row| row[1] == Value::Int(32)));
        drop(agg);
        drop(shared);
        assert_eq!(budget.used(), 0);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn compact_count_spill_topk_merges_resident_overlap_and_releases_budget() {
        use crate::engine::executor::mem_budget::with_query_budget;
        let schema = Arc::new(BatchSchema::new(
            [
                ("id", ColumnType::LongLong),
                ("label", ColumnType::VarString),
            ]
            .into_iter()
            .map(|(name, column_type)| BatchField {
                table: "t".into(),
                alias: None,
                name: name.into(),
                column_type,
                source: Default::default(),
            })
            .collect(),
        ));
        let mut ids = Vec::new();
        let mut labels = Vec::new();
        for id in 0..100 {
            for _ in 0..=id {
                ids.push(if id == 0 { Value::Null } else { Value::Int(id) });
                labels.push(Value::String(if id % 2 == 0 {
                    String::new()
                } else {
                    "é中-long-label".repeat(8)
                }));
            }
        }
        let batch = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::from_values(ids),
                ColumnVector::from_values(labels),
            ],
        );
        let aggs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountStar,
            input_column: 0,
        }];
        let build = || {
            let mut encoder = DictionaryGroupEncoder::try_build(&[0, 1], &batch).unwrap();
            let mut arena = DictionaryCountArena::new(batch.num_rows(), 0, 1, false);
            arena
                .consume_batch(&mut encoder, &aggs, &batch, &mut Vec::new())
                .unwrap();
            (Arc::new(encoder), arena)
        };
        let root = temp_root();
        let shared = SharedHashAggSpill::new(root.clone(), schema, &[0, 1], aggs.clone());
        let (encoder, mut spilled) = build();
        for partition in 0..NUM_PARTITIONS {
            let part = spilled.take_partition(partition);
            if part.keys.is_empty() {
                continue;
            }
            let batch = dictionary_count_partition_to_spill_batch(
                &part,
                &encoder,
                shared.dictionary_schema.clone(),
                0..part.keys.len(),
                1,
            )
            .unwrap();
            // Two disk epochs and the resident tail must be summed before K
            // is applied; selecting each epoch's K independently is invalid.
            shared.append_dictionary(partition, &batch).unwrap();
            shared.append_dictionary(partition, &batch).unwrap();
        }
        let (actual_encoder, mut actual_arena) = build();
        let (oracle_encoder, mut oracle_arena) = build();
        let partial = |partition, encoder| {
            Some(PartitionPartial {
                buckets: Vec::new(),
                fixed_width_decimal: None,
                compressed_i64_decimal: None,
                dictionary_count: Some(DictionaryCountPartitionPartial {
                    partition,
                    encoder,
                    raw_token_width: 0,
                    n_aggs: 1,
                }),
            })
        };
        let budget = Arc::new(MemBudget::new(128 * 1024 * 1024));
        with_query_budget(budget.clone(), || {
            for partition in 0..NUM_PARTITIONS {
                let mut actual = vec![vec![partial(
                    actual_arena.take_partition(partition),
                    actual_encoder.clone(),
                )]];
                let mut expected = merge_worker_partition_snapshots(
                    vec![
                        vec![partial(
                            oracle_arena.take_partition(partition),
                            oracle_encoder.clone(),
                        )],
                        vec![shared.partition_snapshot(partition).unwrap()],
                    ],
                    &aggs,
                )
                .unwrap();
                if expected.is_empty() {
                    continue;
                }
                let before = budget.used();
                let mut rows = shared
                    .try_partition_count_topk(
                        partition,
                        &mut actual,
                        AggregateCountTopKHint {
                            aggregate_index: 0,
                            k: 3,
                            descending: true,
                        },
                    )
                    .unwrap()
                    .unwrap();
                assert!(actual.is_empty());
                assert_eq!(budget.used(), before);
                let order = |row: &Vec<Value>| match row.last().unwrap() {
                    Value::Int(count) => std::cmp::Reverse(*count),
                    _ => panic!("COUNT"),
                };
                expected.sort_by_key(order);
                expected.truncate(3);
                rows.sort_by_key(order);
                assert_eq!(rows, expected);
            }
        });
        assert_eq!(budget.used(), 0);
        // An optional merge that cannot reserve its map keeps the tails for
        // the ordinary spill replay instead of dropping counts or forcing memory.
        let (encoder, mut arena) = build();
        with_query_budget(Arc::new(MemBudget::new(1)), || {
            for partition in 0..NUM_PARTITIONS {
                let part = arena.take_partition(partition);
                if part.keys.is_empty() {
                    continue;
                }
                let mut tails = vec![vec![partial(part, encoder.clone())]];
                assert!(shared
                    .try_partition_count_topk(
                        partition,
                        &mut tails,
                        AggregateCountTopKHint {
                            aggregate_index: 0,
                            k: 3,
                            descending: true
                        }
                    )
                    .unwrap()
                    .is_none());
                assert_eq!(tails.len(), 1);
                break;
            }
        });
        drop(shared);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn dictionary_count_token_exhaustion_spills_and_falls_back_exactly() {
        let schema = Arc::new(BatchSchema::new(
            [
                ("traffic_source", ColumnType::LongLong),
                ("search_engine", ColumnType::LongLong),
                ("adv_engine", ColumnType::LongLong),
                ("source", ColumnType::Blob),
                ("url", ColumnType::Blob),
            ]
            .into_iter()
            .map(|(name, column_type)| BatchField {
                table: "hits".into(),
                alias: None,
                name: name.into(),
                column_type,
                source: Default::default(),
            })
            .collect(),
        ));
        const TEST_BATCH_ROWS: usize = 16 * 1024;
        let make_batch = |first_url: usize, distinct_urls: bool| {
            let rows = TEST_BATCH_ROWS;
            Batch::new(
                schema.clone(),
                vec![
                    ColumnVector::I64 {
                        data: vec![1; rows],
                        nulls: None,
                    },
                    ColumnVector::I64 {
                        data: vec![2; rows],
                        nulls: None,
                    },
                    ColumnVector::I64 {
                        data: vec![3; rows],
                        nulls: None,
                    },
                    ColumnVector::from_values(vec![Value::String(String::new()); rows]),
                    ColumnVector::from_values(
                        (0..rows)
                            .map(|row| {
                                Value::String(if distinct_urls {
                                    format!("https://example.test/{:05}", first_url + row)
                                } else {
                                    "https://example.test/seed".into()
                                })
                            })
                            .collect(),
                    ),
                ],
            )
        };
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountStar,
            input_column: 0,
        }];
        let root = temp_root();
        // Above COMPACT_WORK_AREA_LIMIT so this exercises the same resident
        // dictionary layout selected by a large MYDB_MEMORY_TARGET.
        let budget = Arc::new(MemBudget::new(512 * 1024 * 1024));
        let shared = Arc::new(SharedHashAggSpill::new(
            root.clone(),
            schema.clone(),
            &[0, 1, 2, 3, 4],
            specs.clone(),
        ));
        let mut agg = HashAggregateCore::with_budget(
            vec![0, 1, 2, 3, 4],
            specs.clone(),
            schema.clone(),
            budget,
            &root,
        )
        .with_shared_spill(Arc::clone(&shared));

        agg.consume_batch(&make_batch(0, false)).unwrap();
        let token_bits = agg
            .dictionary_encoder
            .as_ref()
            .expect("clustered first batch selects dictionary COUNT")
            .columns
            .iter()
            .filter(|column| !column.direct)
            .map(|column| column.token_mask.count_ones())
            .min()
            .unwrap();
        assert_eq!(token_bits, 15, "test must cover ClickBench Q40 layout");

        // Two full batches exhaust the 15-bit token space (including seed);
        // a third verifies continued generic aggregation after the spill.
        const DISTINCT_BATCHES: usize = 3;
        assert!(DISTINCT_BATCHES * TEST_BATCH_ROWS > (1 << token_bits));
        for batch in 0..DISTINCT_BATCHES {
            agg.consume_batch(&make_batch(batch * TEST_BATCH_ROWS, true))
                .unwrap();
        }
        assert!(
            agg.metrics().did_spill,
            "token exhaustion must close the compact dictionary epoch"
        );

        let mut tail = Vec::with_capacity(NUM_PARTITIONS);
        for partition in 0..NUM_PARTITIONS {
            tail.push(agg.take_partition_merged_snapshot(partition).unwrap());
        }
        let spilled = shared.partition_snapshots().unwrap();
        let rows = merge_worker_partition_snapshots(vec![spilled, tail], &specs).unwrap();
        assert_eq!(rows.len(), 1 + DISTINCT_BATCHES * TEST_BATCH_ROWS);
        assert_eq!(
            rows.iter()
                .map(|row| match row.last() {
                    Some(Value::Int(count)) => *count as usize,
                    other => panic!("unexpected COUNT result: {other:?}"),
                })
                .sum::<usize>(),
            (1 + DISTINCT_BATCHES) * TEST_BATCH_ROWS
        );
        let seed = rows
            .iter()
            .find(|row| row[4] == Value::String("https://example.test/seed".into()))
            .expect("seed URL group is preserved across the dictionary spill");
        assert_eq!(seed.last(), Some(&Value::Int(TEST_BATCH_ROWS as i64)));
        drop(agg);
        drop(shared);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn dictionary_count_parallel_merge_aligns_worker_local_tokens() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "user_id".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "phrase".into(),
                column_type: ColumnType::Blob,
                source: Default::default(),
            },
        ]));
        let make_batch = |phrases: &[&str]| {
            Batch::new(
                schema.clone(),
                vec![
                    ColumnVector::from_values(vec![Value::Int(99); phrases.len()]),
                    ColumnVector::from_values(
                        phrases
                            .iter()
                            .map(|phrase| Value::String((*phrase).into()))
                            .collect(),
                    ),
                ],
            )
        };
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountStar,
            input_column: 0,
        }];
        let batches = [
            make_batch(&["alpha", "beta"]),
            make_batch(&["beta", "alpha"]),
        ];
        let mut encoders = batches
            .iter()
            .map(|batch| DictionaryGroupEncoder::try_build(&[0, 1], batch).unwrap())
            .collect::<Vec<_>>();
        let mut arenas = Vec::new();
        for (batch, encoder) in batches.iter().zip(encoders.iter_mut()) {
            let mut arena = DictionaryCountArena::new(
                batch.num_rows(),
                0,
                specs.len(),
                encoder.is_single_utf8(),
            );
            arena
                .consume_batch(encoder, &specs, batch, &mut Vec::new())
                .unwrap();
            arenas.push(arena);
        }
        let mut registries = Vec::new();
        for encoder in &mut encoders {
            encoder.align_merge_tokens(&mut registries);
        }
        let encoders = encoders.into_iter().map(Arc::new).collect::<Vec<_>>();
        let mut worker_partials = Vec::new();
        for (arena, encoder) in arenas.iter_mut().zip(encoders) {
            let raw_token_width = arena.raw_token_width;
            let n_aggs = arena.n_aggs;
            worker_partials.push(
                (0..NUM_PARTITIONS)
                    .map(|partition| {
                        let partition = arena.take_partition(partition);
                        (!partition.keys.is_empty()).then(|| PartitionPartial {
                            buckets: Vec::new(),
                            fixed_width_decimal: None,
                            compressed_i64_decimal: None,
                            dictionary_count: Some(DictionaryCountPartitionPartial {
                                partition,
                                encoder: Arc::clone(&encoder),
                                raw_token_width,
                                n_aggs,
                            }),
                        })
                    })
                    .collect::<Vec<_>>(),
            );
        }
        let mut rows = merge_worker_partition_snapshots(worker_partials, &specs).unwrap();
        rows.sort_by(|left, right| {
            crate::storage::value_to_string(&left[1])
                .cmp(&crate::storage::value_to_string(&right[1]))
        });
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(99), Value::String("alpha".into()), Value::Int(2)],
                vec![Value::Int(99), Value::String("beta".into()), Value::Int(2)],
            ]
        );
    }

    #[test]
    fn dictionary_group_encoder_packs_six_column_olap_key_exactly() {
        let fields = [
            ("category", ColumnType::VarString),
            ("brand", ColumnType::VarString),
            ("store", ColumnType::VarString),
            ("company", ColumnType::VarString),
            ("year", ColumnType::LongLong),
            ("month", ColumnType::LongLong),
        ]
        .into_iter()
        .map(|(name, column_type)| BatchField {
            table: "sales".into(),
            alias: None,
            name: name.into(),
            column_type,
            source: Default::default(),
        })
        .collect();
        let schema = Arc::new(BatchSchema::new(fields));
        let batch = Batch::new(
            schema,
            vec![
                ColumnVector::from_values(vec![
                    Value::String("books".into()),
                    Value::String("books".into()),
                    Value::String("books".into()),
                ]),
                ColumnVector::from_values(vec![
                    Value::String("brand-a".into()),
                    Value::String("brand-a".into()),
                    Value::String("brand-a".into()),
                ]),
                ColumnVector::from_values(vec![
                    Value::String("store-1".into()),
                    Value::String("store-1".into()),
                    Value::String("store-1".into()),
                ]),
                ColumnVector::from_values(vec![
                    Value::String("company".into()),
                    Value::String("company".into()),
                    Value::String("company".into()),
                ]),
                ColumnVector::from_values(vec![
                    Value::Int(2001),
                    Value::Int(2001),
                    Value::Int(2001),
                ]),
                ColumnVector::from_values(vec![Value::Int(1), Value::Int(1), Value::Int(2)]),
            ],
        );
        let mut encoder = DictionaryGroupEncoder::try_build(&[0, 1, 2, 3, 4, 5], &batch)
            .expect("six-column categorical key should use dictionary packing");
        assert!(!encoder.direct_primitives);
        assert_eq!(encoder.columns[1].shift, 12);
        assert!(encoder.columns[5].direct);
        assert!(encoder.columns.iter().all(|column| {
            column.shift
                + if column.direct {
                    column.null_bit - column.shift + 1
                } else {
                    column.token_mask.count_ones()
                }
                <= 128
        }));
        let mut scratch = Vec::new();
        let first = encoder.encode_row(&batch, 0, &mut scratch).unwrap();
        let repeated = encoder.encode_row(&batch, 1, &mut scratch).unwrap();
        let other_month = encoder.encode_row(&batch, 2, &mut scratch).unwrap();
        assert_eq!(first, repeated);
        assert_ne!(first.0, other_month.0);
    }

    #[test]
    fn fixed_width_i64_retires_dense_lookup_after_domain_exceeds_cap() {
        let mut aggregate =
            FixedWidthStateArena::new(vec![CompactKeyKind::I64], vec![AggregateKind::CountStar], 1);
        aggregate.monotonic_i64_unindexed = false;
        aggregate.dense_i64 = Some(DenseI64Lookup {
            min: 0,
            slots: vec![DENSE_I64_EMPTY; 8],
            null_slot: None,
        });

        let group = aggregate.probe_or_insert(&[DENSE_I64_MAX_SLOTS as i64], 0);
        assert_eq!(group, 0);
        assert!(aggregate.dense_i64.is_none());
        assert_eq!(aggregate.keys, vec![DENSE_I64_MAX_SLOTS as i64]);
    }

    #[test]
    fn fixed_width_i64_monotonic_append_materializes_index_on_disorder() {
        let mut aggregate =
            FixedWidthStateArena::new(vec![CompactKeyKind::I64], vec![AggregateKind::CountStar], 1);
        let one = aggregate.probe_or_insert_i64(1, 0);
        assert_eq!(aggregate.probe_or_insert_i64(1, 0), one);
        let two = aggregate.probe_or_insert_i64(2, 0);
        let three = aggregate.probe_or_insert_i64(3, 0);
        assert_eq!((one, two, three), (0, 1, 2));
        assert!(aggregate.monotonic_i64_unindexed);
        assert!(aggregate.slots.iter().all(|slot| *slot == 0));
        assert_eq!(aggregate.hashes.len(), 3);

        assert_eq!(aggregate.probe_or_insert_i64(2, 0), two);
        assert!(!aggregate.monotonic_i64_unindexed);
        assert_eq!(aggregate.hashes.len(), 3, "disorder duplicated a group");
        assert_eq!(aggregate.probe_or_insert_i64(4, 0), 3);
    }

    #[test]
    fn count_sum_by_group() {
        let s = schema_kv();
        let b1 = make_batch(&s, &[(1, 10), (2, 20), (1, 5)]);
        let b2 = make_batch(&s, &[(2, 8), (3, 100)]);
        let mut stream = MaterializedBatchOperator::new(s, vec![b1, b2]);
        let out = run_hash_agg(
            &mut stream,
            vec![0],
            vec![
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::CountStar,
                    input_column: 0,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Sum,
                    input_column: 1,
                },
            ],
        )
        .unwrap();
        let mut got: Vec<(i64, i64, i64)> = out
            .into_iter()
            .map(|r| {
                let k = match r[0] {
                    Value::Int(x) => x,
                    _ => panic!(),
                };
                let c = match r[1] {
                    Value::Int(x) => x,
                    _ => panic!(),
                };
                // SUM over Int inputs finalises as `DecimalCompact(_, 0)`
                // (mirrors legacy `SumAcc` which now also preserves an
                // exact-decimal magnitude — see `aggregate::SumAcc::finalize`).
                let s = match &r[2] {
                    Value::DecimalCompact(mag, 0) => *mag,
                    other => panic!("unexpected sum {:?}", other),
                };
                (k, c, s)
            })
            .collect();
        got.sort_by_key(|t| t.0);
        assert_eq!(got, vec![(1, 2, 15), (2, 2, 28), (3, 1, 100)]);
    }

    #[test]
    fn packed_short_string_groups_switch_to_canonical_without_splitting_groups() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "k".into(),
                column_type: ColumnType::VarString,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
        ]));
        let batch = |keys: &[&str]| {
            Batch::new(
                schema.clone(),
                vec![
                    ColumnVector::from_values(
                        keys.iter()
                            .map(|key| Value::String((*key).to_owned()))
                            .collect(),
                    ),
                    ColumnVector::from_values(vec![Value::Int(1); keys.len()]),
                ],
            )
        };
        // The first batch selects the packed short-string representation.
        // A wide key in the second batch forces the one-way canonical-map
        // transition; the repeated "aa" must still find its original group.
        let mut input = MaterializedBatchOperator::new(
            schema.clone(),
            vec![batch(&["aa", "bb", "aa"]), batch(&["aa", "key-too-wide"])],
        );
        let rows = run_hash_agg(
            &mut input,
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountStar,
                input_column: 0,
            }],
        )
        .unwrap();
        let mut got = rows
            .into_iter()
            .map(|row| {
                let count = match row[1] {
                    Value::Int(count) => count,
                    ref other => panic!("unexpected count {other:?}"),
                };
                (crate::storage::value_to_string(&row[0]), count)
            })
            .collect::<Vec<_>>();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("aa".to_owned(), 3),
                ("bb".to_owned(), 1),
                ("key-too-wide".to_owned(), 1),
            ]
        );
    }

    #[test]
    fn packed_utf8_one_pair_groups_values_and_nulls_exactly() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "a".into(),
                column_type: ColumnType::VarString,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "b".into(),
                column_type: ColumnType::VarString,
                source: Default::default(),
            },
        ]));
        let batch = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::from_values(vec![
                    Value::String("A".into()),
                    Value::String("A".into()),
                    Value::String("B".into()),
                    Value::Null,
                    Value::String(String::new()),
                ]),
                ColumnVector::from_values(vec![
                    Value::String("F".into()),
                    Value::String("F".into()),
                    Value::String("O".into()),
                    Value::String("F".into()),
                    Value::String("F".into()),
                ]),
            ],
        );
        let mut input = MaterializedBatchOperator::new(schema, vec![batch]);
        let rows = run_hash_agg(
            &mut input,
            vec![0, 1],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountStar,
                input_column: 0,
            }],
        )
        .unwrap();
        let mut got = rows
            .into_iter()
            .map(|row| {
                let count = match row[2] {
                    Value::Int(count) => count,
                    ref other => panic!("unexpected count {other:?}"),
                };
                (
                    crate::storage::value_to_string(&row[0]),
                    crate::storage::value_to_string(&row[1]),
                    count,
                )
            })
            .collect::<Vec<_>>();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("".into(), "F".into(), 1),
                ("A".into(), "F".into(), 2),
                ("B".into(), "O".into(), 1),
                ("NULL".into(), "F".into(), 1),
            ]
        );
    }

    #[test]
    fn parallel_mixed_string_layouts_use_the_same_canonical_partition() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "nation".into(),
                column_type: ColumnType::VarString,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
        ]));
        let batch = |keys: &[&str]| {
            Batch::new(
                schema.clone(),
                vec![
                    ColumnVector::from_values(
                        keys.iter()
                            .map(|key| Value::String((*key).to_owned()))
                            .collect(),
                    ),
                    ColumnVector::from_values(vec![Value::Int(1); keys.len()]),
                ],
            )
        };
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountStar,
            input_column: 0,
        }];

        // Worker 0 stays on the short-string packed layout. Worker 1 sees a
        // wide key in its first batch and consumes canonically. Before the
        // fix, the two CHINA partials could land in different radix
        // partitions and survive the cross-worker merge as duplicate rows.
        let mut packed_worker = HashAggregateCore::new(vec![0], specs.clone());
        packed_worker.consume_batch(&batch(&["CHINA"])).unwrap();
        let mut canonical_worker = HashAggregateCore::new(vec![0], specs.clone());
        canonical_worker
            .consume_batch(&batch(&["INDONESIA", "CHINA"]))
            .unwrap();

        let num_partitions = packed_worker.num_partitions();
        let mut partials = Vec::new();
        for worker in [&mut packed_worker, &mut canonical_worker] {
            partials.push(
                (0..num_partitions)
                    .map(|partition| worker.take_partition_snapshot(partition))
                    .collect(),
            );
        }
        let mut got = merge_worker_partition_snapshots(partials, &specs)
            .unwrap()
            .into_iter()
            .map(|row| {
                let Value::Int(count) = row[1] else {
                    panic!("expected COUNT result")
                };
                (crate::storage::value_to_string(&row[0]), count)
            })
            .collect::<Vec<_>>();
        got.sort();
        assert_eq!(
            got,
            vec![("CHINA".to_owned(), 2), ("INDONESIA".to_owned(), 1)]
        );
    }

    #[test]
    fn high_cardinality_scratch_is_bounded_by_one_batch() {
        let schema = schema_kv();
        let mut agg = HashAggregateCore::new(
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountStar,
                input_column: 0,
            }],
        );
        const BATCH_ROWS: i64 = 64;
        for batch_no in 0..12i64 {
            let start = batch_no * BATCH_ROWS;
            let rows: Vec<(i64, i64)> = (start..start + BATCH_ROWS).map(|key| (key, key)).collect();
            agg.consume_batch(&make_batch(&schema, &rows)).unwrap();
            assert!(
                agg.bucket_selections.len() <= BATCH_ROWS as usize,
                "scratch retained groups from earlier batches"
            );
        }
    }

    #[test]
    fn avg_handles_null_argument() {
        let s = schema_kv();
        let b = Batch::new(
            s.clone(),
            vec![
                ColumnVector::from_values(vec![Value::Int(1), Value::Int(1), Value::Int(1)]),
                ColumnVector::from_values(vec![Value::Int(10), Value::Null, Value::Int(20)]),
            ],
        );
        let mut stream = MaterializedBatchOperator::new(s, vec![b]);
        let out = run_hash_agg(
            &mut stream,
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Avg,
                input_column: 1,
            }],
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        // AVG over exact ints: DECIMAL with scale div_precision_increment.
        assert_eq!(out[0][1], Value::DecimalCompact(150000, 4));
    }

    #[test]
    fn decimal_sum_avg_count_share_one_selection_lane() {
        let specs = vec![
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 0,
            },
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Avg,
                input_column: 0,
            },
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Count,
                input_column: 0,
            },
        ];
        let mut states = vec![AggState::default(); specs.len()];
        let column = ColumnVector::Decimal64 {
            data: vec![123, 456, 789],
            scale: 2,
            nulls: None,
        };
        assert!(update_shared_primitive_lane(
            &mut states,
            &specs,
            &[0, 1, 2],
            &column,
            Some(&[0, 2]),
        ));
        assert_eq!(
            finalize_state(&states[0], &specs[0]),
            Value::DecimalCompact(912, 2)
        );
        assert_eq!(
            finalize_state(&states[1], &specs[1]),
            Value::DecimalCompact(4560000, 6)
        );
        assert_eq!(finalize_state(&states[2], &specs[2]), Value::Int(2));
    }

    #[test]
    fn avg_snapshot_merge_matches_serial() {
        // Regression: the parallel cross-worker merge (`finalize_snapshot`)
        // used to finalise AVG via an f64 division, bypassing the shared
        // `finalize_avg_value` that the serial path (`finalize_state`) uses.
        // For exact (INT/DECIMAL) inputs that produced a binary `Double`
        // instead of a fixed-point `DecimalCompact`, so the same query gave
        // different types/precision depending on whether it ran serial or
        // parallel — and diverged from the row-wise oracle. Group 3's average
        // (400/3) does not divide evenly, so the Double-vs-Decimal difference
        // is observable; a byte-exact compare would fail before the fix.
        let s = schema_kv();
        let all: Vec<(i64, i64)> = vec![
            (1, 10),
            (1, 20),
            (2, 5),
            (3, 100),
            (1, 20),
            (1, 30),
            (2, 5),
            (3, 100),
            (3, 200),
        ];
        let avg_specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::Avg,
            input_column: 1,
        }];

        // Serial oracle over the full input.
        let mut serial_stream =
            MaterializedBatchOperator::new(s.clone(), vec![make_batch(&s, &all)]);
        let serial = run_hash_agg(&mut serial_stream, vec![0], avg_specs.clone()).unwrap();

        // Two workers over disjoint halves, merged exactly as the parallel
        // scheduler does.
        let mut w0 = HashAggregateCore::new(vec![0], avg_specs.clone());
        let mut w1 = HashAggregateCore::new(vec![0], avg_specs.clone());
        w0.consume_batch(&make_batch(&s, &all[..4])).unwrap();
        w1.consume_batch(&make_batch(&s, &all[4..])).unwrap();
        let nparts = w0.num_partitions();
        let mut partials: Vec<Vec<Option<PartitionPartial>>> = Vec::new();
        for w in [&mut w0, &mut w1] {
            let mut wp = Vec::with_capacity(nparts);
            for p in 0..nparts {
                wp.push(w.take_partition_snapshot(p));
            }
            partials.push(wp);
        }
        let merged = merge_worker_partition_snapshots(partials, &avg_specs).unwrap();

        let project = |rows: Vec<Vec<Value>>| -> Vec<(i64, Value)> {
            let mut out: Vec<(i64, Value)> = rows
                .into_iter()
                .map(|r| {
                    let k = match r[0] {
                        Value::Int(x) => x,
                        _ => panic!("group key"),
                    };
                    (k, r[1].clone())
                })
                .collect();
            out.sort_by_key(|t| t.0);
            out
        };
        let merged = project(merged);
        // The merged parallel result must equal the serial oracle byte-for-byte
        // and, specifically, keep fixed-point precision for the uneven group.
        assert_eq!(project(serial), merged);
        assert_eq!(merged[2].0, 3);
        assert_eq!(merged[2].1, Value::DecimalCompact(1333333, 4));
    }

    #[test]
    fn min_max_propagate() {
        let s = schema_kv();
        let b = Batch::new(
            s.clone(),
            vec![
                ColumnVector::from_values(vec![Value::Int(1), Value::Int(1), Value::Int(1)]),
                ColumnVector::from_values(vec![Value::Int(10), Value::Int(2), Value::Int(20)]),
            ],
        );
        let mut stream = MaterializedBatchOperator::new(s, vec![b]);
        let out = run_hash_agg(
            &mut stream,
            vec![0],
            vec![
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Min,
                    input_column: 1,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Max,
                    input_column: 1,
                },
            ],
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0][1], Value::Int(2));
        assert_eq!(out[0][2], Value::Int(20));
    }

    // -----------------------------------------------------------------
    // A2 — DISTINCT aggregates. These call HashAggregateCore directly so there
    // is no routing ambiguity: the distinct vec code is unconditionally
    // exercised (unlike SQL-level Off-vs-On, where a silent fall-back to
    // the row path would pass trivially). The row-side oracle is checked
    // by the SQL differential suite in `tests/a2_vec_agg_distinct.rs`.
    // -----------------------------------------------------------------

    fn distinct_specs() -> Vec<AggregateSpec> {
        vec![
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountDistinct,
                input_column: 1,
            },
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::SumDistinct,
                input_column: 1,
            },
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::AvgDistinct,
                input_column: 1,
            },
        ]
    }

    /// `(k, count_distinct, sum_distinct_magnitude, avg_distinct_value)`
    /// projected from a distinct result row; SUM(INT) → `DecimalCompact(_,0)`,
    /// AVG(INT) → `DecimalCompact(_,4)`.
    fn project_distinct(out: Vec<Vec<Value>>) -> Vec<(i64, i64, Value, Value)> {
        let mut got: Vec<(i64, i64, Value, Value)> = out
            .into_iter()
            .map(|r| {
                let k = match r[0] {
                    Value::Int(x) => x,
                    _ => panic!("key {:?}", r[0]),
                };
                let c = match r[1] {
                    Value::Int(x) => x,
                    _ => panic!("count {:?}", r[1]),
                };
                (k, c, r[2].clone(), r[3].clone())
            })
            .collect();
        got.sort_by_key(|t| t.0);
        got
    }

    #[test]
    fn distinct_topk_ranks_only_after_cross_worker_union() {
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountDistinct,
            input_column: 1,
        }];
        let inputs = || {
            (0..2)
                .map(|worker| {
                    let mut a = AggState::default();
                    let mut b = AggState::default();
                    for value in 0..10 {
                        a.distinct_insert(Value::Int(value));
                    }
                    for value in 0..6 {
                        b.distinct_insert(Value::Int(value + worker * 100));
                    }
                    vec![Some(PartitionPartial {
                        buckets: vec![
                            GroupBucketSnapshot {
                                key: vec![1],
                                key_values: vec![Value::Int(1)],
                                aggs: vec![a.into_partial()],
                            },
                            GroupBucketSnapshot {
                                key: vec![2],
                                key_values: vec![Value::Int(2)],
                                aggs: vec![b.into_partial()],
                            },
                        ],
                        fixed_width_decimal: None,
                        compressed_i64_decimal: None,
                        dictionary_count: None,
                    })]
                })
                .collect()
        };
        // Each worker prefers A (10 > 6), but their B sets are disjoint.
        // Pruning before union would silently return the wrong group.
        for descending in [true, false] {
            let rows = merge_worker_partition_snapshots_filtered_topk(
                inputs(),
                &specs,
                None,
                Some(AggregateCountTopKHint {
                    aggregate_index: 0,
                    k: 1,
                    descending,
                }),
            )
            .unwrap();
            assert_eq!(
                rows,
                if descending {
                    vec![vec![Value::Int(2), Value::Int(12)]]
                } else {
                    vec![vec![Value::Int(1), Value::Int(10)]]
                }
            );
        }
    }

    #[test]
    fn count_topk_retains_diagnostic_producing_distinct_reducers() {
        for reducer in [AggregateKind::SumDistinct, AggregateKind::AvgDistinct] {
            let specs = vec![
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::CountStar,
                    input_column: 0,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: reducer,
                    input_column: 1,
                },
            ];
            let buckets = [(1, 10, "1"), (2, 1, "not-a-number")]
                .into_iter()
                .map(|(key, count, value)| {
                    let mut distinct = AggState::default();
                    distinct.distinct_insert(Value::String(value.into()));
                    GroupBucketSnapshot {
                        key: vec![key],
                        key_values: vec![Value::Int(i64::from(key))],
                        aggs: vec![
                            AggStateSnapshot {
                                count,
                                ..Default::default()
                            },
                            distinct.into_partial(),
                        ],
                    }
                })
                .collect();
            let partial = PartitionPartial {
                buckets,
                fixed_width_decimal: None,
                compressed_i64_decimal: None,
                dictionary_count: None,
            };
            let rows = merge_worker_partition_snapshots_filtered_topk(
                vec![vec![Some(partial)]],
                &specs,
                None,
                Some(AggregateCountTopKHint {
                    aggregate_index: 0,
                    k: 1,
                    descending: true,
                }),
            )
            .unwrap();
            // The enclosing Top-N can discard group 2 only after the reducer
            // has performed its ordinary diagnostic-producing finalization.
            assert_eq!(rows.len(), 2);
        }
    }

    #[test]
    fn distinct_count_sum_avg_by_group() {
        let s = schema_kv();
        // g1: 10,20,10,20,30 → {10,20,30} count=3 sum=60 avg=20
        // g2: 5,5,5          → {5}        count=1 sum=5  avg=5
        let b1 = make_batch(&s, &[(1, 10), (1, 20), (1, 10), (2, 5), (2, 5)]);
        let b2 = make_batch(&s, &[(1, 20), (1, 30), (2, 5)]);
        let mut stream = MaterializedBatchOperator::new(s, vec![b1, b2]);
        let out = run_hash_agg(&mut stream, vec![0], distinct_specs()).unwrap();
        assert_eq!(
            project_distinct(out),
            vec![
                (
                    1,
                    3,
                    Value::DecimalCompact(60, 0),
                    Value::DecimalCompact(200000, 4)
                ),
                (
                    2,
                    1,
                    Value::DecimalCompact(5, 0),
                    Value::DecimalCompact(50000, 4)
                ),
            ]
        );
    }

    #[test]
    fn small_i64_distinct_promotes_without_semantic_drift() {
        let mut state = AggState::default();
        for value in 0..8 {
            state.distinct_insert(Value::Int(value));
            state.distinct_insert(Value::Int(value));
        }
        assert!(matches!(
            state.distinct.as_deref(),
            Some(DistinctState::SmallI64(values)) if values.len() == 8
        ));
        assert_eq!(
            finalize_distinct(state.distinct.as_deref(), AggregateKind::CountDistinct),
            Value::Int(8)
        );

        state.distinct_insert(Value::Int(8));
        assert!(matches!(
            state.distinct.as_deref(),
            Some(DistinctState::I64Set(values)) if values.len() == 9
        ));
        assert_eq!(
            finalize_distinct(state.distinct.as_deref(), AggregateKind::SumDistinct),
            Value::DecimalCompact(36, 0)
        );
    }

    #[test]
    fn count_distinct_byte_lane_uses_compact_borrowed_key_set() {
        let column = ColumnVector::Bytes {
            offsets: vec![0, 5, 10, 14],
            data: b"alphaalphabeta".to_vec(),
            nulls: None,
        };
        let mut state = AggState::default();
        update_state_from_lane(
            &mut state,
            &AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountDistinct,
                input_column: 0,
            },
            &column,
            None,
        )
        .unwrap();
        assert!(matches!(
            state.distinct.as_deref(),
            Some(DistinctState::CountKeys(keys)) if keys.len() == 2
        ));
        assert_eq!(state.finalize(AggregateKind::CountDistinct), Value::Int(2));
    }

    #[test]
    fn distinct_skips_nulls_and_empty_group_is_identity() {
        let s = schema_kv();
        // g1: all NULL → COUNT(DISTINCT)=0, SUM/AVG=NULL
        // g2: NULL,7,7 → {7} count=1 sum=7 avg=7
        let b = Batch::new(
            s.clone(),
            vec![
                ColumnVector::from_values(vec![
                    Value::Int(1),
                    Value::Int(1),
                    Value::Int(2),
                    Value::Int(2),
                    Value::Int(2),
                ]),
                ColumnVector::from_values(vec![
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Int(7),
                    Value::Int(7),
                ]),
            ],
        );
        let mut stream = MaterializedBatchOperator::new(s, vec![b]);
        let out = run_hash_agg(&mut stream, vec![0], distinct_specs()).unwrap();
        assert_eq!(
            project_distinct(out),
            vec![
                (1, 0, Value::Null, Value::Null),
                (
                    2,
                    1,
                    Value::DecimalCompact(7, 0),
                    Value::DecimalCompact(70000, 4)
                ),
            ]
        );
    }

    #[test]
    fn distinct_snapshot_merge_matches_serial() {
        // Two workers each consume a disjoint slice; their per-partition
        // snapshots are merged exactly as the parallel scheduler does.
        // The distinct sets must union (with overlap) to the same result
        // a single serial run over the full input produces.
        let s = schema_kv();
        let all: Vec<(i64, i64)> = vec![
            (1, 10),
            (1, 20),
            (2, 5),
            (3, 100),
            (1, 20), // dup within group 1 across workers
            (1, 30),
            (2, 5), // dup within group 2 across workers
            (3, 100),
            (3, 200),
        ];
        // Serial oracle over everything.
        let mut serial_stream =
            MaterializedBatchOperator::new(s.clone(), vec![make_batch(&s, &all)]);
        let serial = run_hash_agg(&mut serial_stream, vec![0], distinct_specs()).unwrap();

        // Two workers, disjoint halves.
        let mut w0 = HashAggregateCore::new(vec![0], distinct_specs());
        let mut w1 = HashAggregateCore::new(vec![0], distinct_specs());
        w0.consume_batch(&make_batch(&s, &all[..4])).unwrap();
        w1.consume_batch(&make_batch(&s, &all[4..])).unwrap();
        let nparts = w0.num_partitions();
        let mut partials: Vec<Vec<Option<PartitionPartial>>> = Vec::new();
        for w in [&mut w0, &mut w1] {
            let mut wp = Vec::with_capacity(nparts);
            for p in 0..nparts {
                wp.push(w.take_partition_snapshot(p));
            }
            partials.push(wp);
        }
        let merged = merge_worker_partition_snapshots(partials, &distinct_specs()).unwrap();

        assert_eq!(project_distinct(serial), project_distinct(merged));
    }

    #[test]
    fn distinct_spill_round_trip_matches_in_memory() {
        use crate::engine::executor::mem_budget::MemBudget;
        let s = schema_kv();
        // Many groups, each with duplicated values, so a tight budget
        // forces the operator to serialise its dedup sets to disk and
        // union them back at finalize.
        let mut pairs: Vec<(i64, i64)> = Vec::new();
        for r in 0..4000i64 {
            let g = r % 211;
            // 3 distinct values per group, each repeated.
            pairs.push((g, (r % 17) * 10));
        }
        let in_mem = {
            let mut stream =
                MaterializedBatchOperator::new(s.clone(), vec![make_batch(&s, &pairs)]);
            run_hash_agg(&mut stream, vec![0], distinct_specs()).unwrap()
        };

        let dir = tempfile::tempdir().unwrap();
        let budget = Arc::new(MemBudget::new(64 * 1024));
        let mut agg = HashAggregateCore::with_budget(
            vec![0],
            distinct_specs(),
            s.clone(),
            budget,
            dir.path(),
        );
        // Feed in several batches so partitions accumulate past the budget.
        for chunk in pairs.chunks(400) {
            agg.consume_batch(&make_batch(&s, chunk)).unwrap();
        }
        let spilled = agg.finalize().unwrap();

        assert_eq!(project_distinct(in_mem), project_distinct(spilled));
    }

    #[test]
    fn no_group_columns_means_single_row() {
        let s = schema_kv();
        let b = make_batch(&s, &[(1, 10), (2, 20), (3, 30)]);
        let mut stream = MaterializedBatchOperator::new(s, vec![b]);
        let out = run_hash_agg(
            &mut stream,
            vec![],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 1,
            }],
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        // SUM over Int inputs finalises as `DecimalCompact(_, 0)`
        // (matches legacy `SumAcc::finalize`).
        assert_eq!(out[0][0], Value::DecimalCompact(60, 0));
    }

    #[test]
    fn output_schema_uses_legacy_exact_types_and_unsigned_bit_metadata() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "d".into(),
                column_type: ColumnType::Decimal {
                    precision: 8,
                    scale: 2,
                },
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "i".into(),
                column_type: ColumnType::Long,
                source: Default::default(),
            },
        ]));
        let batch = Batch::new(
            schema,
            vec![
                ColumnVector::from_values(vec![Value::DecimalCompact(125, 2)]),
                ColumnVector::from_values(vec![Value::Int(3)]),
            ],
        );
        let mut agg = HashAggregateCore::new(
            Vec::new(),
            vec![
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Sum,
                    input_column: 0,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Avg,
                    input_column: 1,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::BitOr,
                    input_column: 1,
                },
            ],
        );
        agg.consume_batch(&batch).unwrap();
        let output = agg.output_schema();
        assert!(matches!(
            output.fields()[0].column_type,
            ColumnType::Decimal { scale: 2, .. }
        ));
        assert!(matches!(
            output.fields()[1].column_type,
            ColumnType::Decimal { .. }
        ));
        assert_ne!(
            output.fields()[2].source.flags & crate::mysql::protocol::UNSIGNED_FLAG,
            0
        );
    }

    #[test]
    fn distinct_partitions_independent() {
        // Sanity check: many partitions should still produce one row per
        // distinct key.
        let s = schema_kv();
        let pairs: Vec<(i64, i64)> = (0..1000).map(|i| (i % 250, i)).collect();
        let b = make_batch(&s, &pairs);
        let mut stream = MaterializedBatchOperator::new(s, vec![b]);
        let out = run_hash_agg(
            &mut stream,
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountStar,
                input_column: 0,
            }],
        )
        .unwrap();
        assert_eq!(out.len(), 250);
    }

    // ---- Spill tests ----

    #[test]
    fn key_only_compact_groups_use_large_grants_and_survive_spill() {
        let schema = schema_kv();
        let pairs = (0..4096).map(|i| (i - 2048, i % 17)).collect::<Vec<_>>();
        let batch = make_batch(&schema, &pairs);
        let nullable = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::from_values(vec![Value::Null, Value::Int(0), Value::Null]),
                ColumnVector::from_values(vec![Value::Int(0), Value::Null, Value::Int(0)]),
            ],
        );
        let root = temp_root();
        let budget = Arc::new(MemBudget::new(1024 * 1024 * 1024));
        let mut agg = HashAggregateCore::with_budget(
            vec![0, 1],
            vec![],
            schema.clone(),
            budget.clone(),
            &root,
        );
        agg.consume_batch(&batch).unwrap();
        agg.consume_batch(&nullable).unwrap();
        agg.consume_batch(&batch).unwrap();
        assert!(
            agg.fixed_width.is_some(),
            "wide grants must retain compact key-only state"
        );
        assert!(!agg.metrics().did_spill);
        let mut expected = agg.finalize().unwrap();
        assert_eq!(expected.len(), 4098);
        assert_eq!(budget.used(), 0);
        let (mut spilled, metrics) = run_with_budget(
            schema,
            vec![batch.clone(), nullable, batch],
            vec![0, 1],
            vec![],
            32 * 1024,
        );
        assert!(metrics.did_spill);
        expected.sort_by_key(|row| format!("{row:?}"));
        spilled.sort_by_key(|row| format!("{row:?}"));
        assert_eq!(spilled, expected);
        std::fs::remove_dir_all(root).unwrap();
    }

    fn temp_root() -> PathBuf {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let p = std::env::temp_dir().join(format!("mydb-hashagg-test-{pid}-{nanos}"));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn run_with_budget(
        s: Arc<BatchSchema>,
        batches: Vec<Batch>,
        group_cols: Vec<usize>,
        aggs: Vec<AggregateSpec>,
        budget_bytes: usize,
    ) -> (Vec<Vec<Value>>, AggMetrics) {
        let root = temp_root();
        let budget = Arc::new(MemBudget::new(budget_bytes));
        let mut agg = HashAggregateCore::with_budget(group_cols, aggs, s.clone(), budget, &root);
        let mut stream = MaterializedBatchOperator::new(s, batches);
        agg.consume_stream(&mut stream).unwrap();
        let metrics = agg.metrics();
        let out = agg.finalize().unwrap();
        let _ = std::fs::remove_dir_all(&root);
        (out, metrics)
    }

    #[test]
    fn exec_audit_binary_group_spill_matches_memory() {
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "k".into(),
                column_type: ColumnType::VarString,
                source: crate::types::ColumnSource {
                    collation_id: Some(
                        crate::charset::resolve_collation("utf8mb4_bin").unwrap().id,
                    ),
                    ..Default::default()
                },
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
        ]));
        let values: Vec<Value> = (0..8000)
            .map(|i| {
                Value::String(format!(
                    "{}{:04}",
                    if i % 2 == 0 { "a" } else { "A" },
                    i / 2
                ))
            })
            .collect();
        let batch = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::from_values(values),
                ColumnVector::from_values(vec![Value::Int(1); 8000]),
            ],
        );
        for budget_bytes in [128 * 1024, 64 * 1024 * 1024] {
            let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let budget = Arc::new(MemBudget::new(budget_bytes));
            let mut agg = HashAggregateCore::with_budget(
                vec![0],
                vec![AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Sum,
                    input_column: 1,
                }],
                schema.clone(),
                budget,
                root.path(),
            );
            for _ in 0..3 {
                agg.consume_batch(&batch).unwrap();
            }
            let spilled = agg.metrics().did_spill;
            let result = agg.finalize().unwrap();
            assert_eq!(result.len(), 8000, "budget {budget_bytes}");
            assert!(result.iter().all(|r| r[1].to_display_string() == "3"));
            if budget_bytes == 128 * 1024 {
                assert!(spilled);
            }
        }
    }

    #[test]
    fn generic_aggregate_bounds_state_growth_inside_large_selected_batch() {
        let schema = schema_kv();
        let mut pairs = vec![(-1, 999); 100];
        pairs.extend((0..16000).flat_map(|key| [(key, 10), (key, 14)]));
        let batch =
            make_batch(&schema, &pairs).with_selection(super::super::batch::Selection::Range {
                start: 100,
                len: 32000,
            });
        let (rows, metrics) = run_with_budget(
            schema,
            vec![batch],
            vec![0],
            vec![
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::CountStar,
                    input_column: 0,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::VarSamp,
                    input_column: 1,
                },
            ],
            1024 * 1024,
        );
        assert!(metrics.did_spill);
        assert!(
            metrics.peak_memory_bytes <= 2 * 1024 * 1024,
            "one producer batch must not inflate generic state before spill: {}",
            metrics.peak_memory_bytes
        );
        assert_eq!(rows.len(), 16000);
        for row in rows {
            assert_eq!(row[1], Value::Int(2));
            assert_eq!(row[2], Value::Double(8.0));
        }
    }

    #[test]
    fn spill_triggered_under_budget() {
        // 5K distinct keys at ~120B each forces at least one spill when
        // the budget is only 32 KiB.
        let s = schema_kv();
        let pairs: Vec<(i64, i64)> = (0..5000).map(|i| (i, i * 2)).collect();
        let b = make_batch(&s, &pairs);
        let (out, m) = run_with_budget(
            s,
            vec![b],
            vec![0],
            vec![
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::CountStar,
                    input_column: 0,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Sum,
                    input_column: 1,
                },
            ],
            32 * 1024,
        );
        assert_eq!(out.len(), 5000);
        assert!(
            m.did_spill,
            "expected at least one spill under 32 KiB budget"
        );
        assert!(m.spill_bytes > 0);
        assert!(m.is_streaming);
        // Spot-check the sum for key 1234 → 2468.
        let row = out
            .iter()
            .find(|r| matches!(r[0], Value::Int(k) if k == 1234))
            .unwrap();
        assert_eq!(row[2], Value::DecimalCompact(2468, 0));
    }

    #[test]
    fn spill_preserves_min_max_and_avg() {
        let s = schema_kv();
        // Many groups so the partition state grows past a tight budget
        // and at least one partition has to spill. Each key gets two
        // values, so MIN/MAX/AVG have something to merge across spills.
        let n_keys = 4000i64;
        let mut pairs: Vec<(i64, i64)> = Vec::new();
        for k in 0..n_keys {
            pairs.push((k, k * 2));
            pairs.push((k, k * 2 + 100));
        }
        let b = make_batch(&s, &pairs);
        let (out, m) = run_with_budget(
            s,
            vec![b],
            vec![0],
            vec![
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Min,
                    input_column: 1,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Max,
                    input_column: 1,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Avg,
                    input_column: 1,
                },
            ],
            32 * 1024,
        );
        assert!(m.did_spill, "expected spill under 32 KiB budget");
        assert_eq!(out.len(), n_keys as usize);
        // Spot-check key=1234 → values 2468 and 2568.
        let row = out
            .iter()
            .find(|r| matches!(r[0], Value::Int(k) if k == 1234))
            .unwrap();
        assert_eq!(row[1], Value::Int(2468)); // MIN
        assert_eq!(row[2], Value::Int(2568)); // MAX
        assert_eq!(row[3], Value::DecimalCompact(25_180_000, 4)); // AVG = 2518.0000
    }

    #[test]
    fn finalize_stream_yields_correct_batches() {
        // Drive a high-cardinality aggregate through finalize_stream and
        // verify (a) the stream produces multiple batches when the result
        // size exceeds DEFAULT_BATCH_ROWS and (b) the merged values match
        // a parallel finalize() reference run.
        let s = schema_kv();
        let rows: usize = super::super::batch::DEFAULT_BATCH_ROWS * 3 + 17;
        let pairs: Vec<(i64, i64)> = (0..rows as i64).map(|i| (i, i)).collect();
        let b = make_batch(&s, &pairs);

        let mut agg = HashAggregateCore::new(
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 1,
            }],
        );
        // Give the operator a schema so `output_schema` is non-trivial.
        agg.input_schema = Some(s.clone());
        let mut stream = MaterializedBatchOperator::new(s.clone(), vec![b.clone()]);
        agg.consume_stream(&mut stream).unwrap();

        let mut out_stream = agg.finalize_stream().unwrap();
        let mut batch_count = 0usize;
        let mut total_rows = 0usize;
        let mut seen_keys: Vec<i64> = Vec::with_capacity(rows);
        while let Some(batch) = out_stream.next_batch().unwrap() {
            batch_count += 1;
            total_rows += batch.num_rows();
            for r in 0..batch.num_rows() {
                if let Value::Int(k) = batch.columns()[0].value_at(r) {
                    seen_keys.push(k);
                }
            }
            // Each batch should respect the batch-row cap.
            assert!(batch.num_rows() <= super::super::batch::DEFAULT_BATCH_ROWS);
        }
        assert_eq!(total_rows, rows);
        assert!(batch_count > 1, "expected multiple output batches");
        seen_keys.sort();
        seen_keys.dedup();
        assert_eq!(seen_keys.len(), rows);
    }

    #[test]
    fn empty_global_aggregate_emits_one_implicit_group() {
        let schema = schema_kv();
        let mut agg = HashAggregateCore::new(
            Vec::new(),
            vec![
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::CountStar,
                    input_column: 0,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Count,
                    input_column: 1,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Sum,
                    input_column: 1,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Avg,
                    input_column: 1,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Min,
                    input_column: 1,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Max,
                    input_column: 1,
                },
            ],
        );
        agg.input_schema = Some(schema);
        let mut output = agg.finalize_stream().unwrap();
        let batch = output.next_batch().unwrap().expect("implicit group");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.columns()[0].value_at(0), Value::Int(0));
        assert_eq!(batch.columns()[1].value_at(0), Value::Int(0));
        for column in &batch.columns()[2..] {
            assert_eq!(column.value_at(0), Value::Null);
        }
        assert!(output.next_batch().unwrap().is_none());
    }

    #[test]
    fn finalize_stream_keeps_owner_charge_until_cursor_drop() {
        let s = schema_kv();
        let pairs: Vec<(i64, i64)> = (0..256).map(|i| (i, i)).collect();
        let b = make_batch(&s, &pairs);
        let root = temp_root();
        let budget = Arc::new(MemBudget::new(16 * 1024 * 1024));
        let mut agg = HashAggregateCore::with_budget(
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 1,
            }],
            s.clone(),
            Arc::clone(&budget),
            &root,
        );
        let mut input = MaterializedBatchOperator::new(s, vec![b]);
        agg.consume_stream(&mut input).unwrap();
        let charged = budget.used();
        assert!(charged > 0);

        let mut output = agg.finalize_stream().unwrap();
        assert_eq!(
            budget.used(),
            charged,
            "state remains owned by output cursor"
        );
        let mut rows = 0;
        while let Some(batch) = output.next_batch().unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 256);
        assert_eq!(budget.used(), 0, "exhausted state must not pin a grant");
        drop(output);
        assert_eq!(budget.used(), 0);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn generic_hash_agg_releases_consumed_output_state() {
        let s = schema_kv();
        let pairs: Vec<(i64, i64)> = (0..256).map(|i| (i, i)).collect();
        let b = make_batch(&s, &pairs);
        let root = temp_root();
        let budget = Arc::new(MemBudget::new(16 * 1024 * 1024));
        let mut agg = HashAggregateCore::with_budget(
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 1,
            }],
            s.clone(),
            Arc::clone(&budget),
            &root,
        );
        agg.fixed_width_probed = true; // Exercise the generic partition output cursor.
        let mut input = MaterializedBatchOperator::new(s, vec![b]);
        agg.consume_stream(&mut input).unwrap();
        let charged = budget.used();
        assert!(charged > 0);

        let mut output = agg.finalize_stream().unwrap();
        assert_eq!(
            budget.used(),
            charged,
            "state remains owned by output cursor"
        );
        let mut rows = 0;
        let mut released = false;
        while let Some(batch) = output.next_batch().unwrap() {
            rows += batch.num_rows();
            released |= budget.used() < charged;
        }
        assert_eq!(rows, 256);
        assert!(released, "consumed groups must release their grant");
        drop(output);
        assert_eq!(budget.used(), 0);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn consume_stream_observes_kill_query() {
        // Regression for the audit gap: vectorized operators had no
        // cooperative cancellation, so KILL QUERY could not interrupt a
        // long hash-agg build. A KILL fired before the operator runs must
        // surface as 1317 on the first input batch.
        use crate::engine::cancel::{enter, QueryCancelToken};
        use crate::engine::session_registry::SessionRegistry;

        let s = schema_kv();
        let rows: usize = super::super::batch::DEFAULT_BATCH_ROWS * 3 + 17;
        let pairs: Vec<(i64, i64)> = (0..rows as i64).map(|i| (i, i)).collect();
        let b = make_batch(&s, &pairs);

        let registry = SessionRegistry::new();
        let handle = registry.register(7777, "root".into(), "localhost".into(), "test".into());
        let token = QueryCancelToken::from_session(std::sync::Arc::clone(&handle));
        registry.kill_query(7777).unwrap();
        let _scope = enter(token);

        let mut agg = HashAggregateCore::new(
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 1,
            }],
        );
        agg.input_schema = Some(s.clone());
        let mut stream = MaterializedBatchOperator::new(s.clone(), vec![b.clone(), b.clone(), b]);
        let err = agg.consume_stream(&mut stream).unwrap_err();
        assert_eq!(
            err.code(),
            1317,
            "KILL QUERY must interrupt the vec hash-agg build phase"
        );
    }

    #[test]
    fn finalize_stream_works_with_spill() {
        let s = schema_kv();
        let pairs: Vec<(i64, i64)> = (0..5000).map(|i| (i, i * 3)).collect();
        let b = make_batch(&s, &pairs);
        let root = temp_root();
        let budget = Arc::new(MemBudget::new(32 * 1024));
        let mut agg = HashAggregateCore::with_budget(
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 1,
            }],
            s.clone(),
            budget,
            &root,
        );
        let mut stream = MaterializedBatchOperator::new(s.clone(), vec![b]);
        agg.consume_stream(&mut stream).unwrap();
        let metrics = agg.metrics();
        assert!(metrics.did_spill);
        assert!(
            metrics.spill_runs <= NUM_PARTITIONS as u64,
            "one append-only file per radix partition: {metrics:?}"
        );

        let mut out = agg.finalize_stream().unwrap();
        let mut all_rows: Vec<(i64, i64)> = Vec::new();
        while let Some(batch) = out.next_batch().unwrap() {
            for r in 0..batch.num_rows() {
                let k = match batch.columns()[0].value_at(r) {
                    Value::Int(x) => x,
                    other => panic!("unexpected key {:?}", other),
                };
                // SUM over Int inputs finalises as `DecimalCompact(_, 0)`.
                let sum = match batch.columns()[1].value_at(r) {
                    Value::DecimalCompact(mag, 0) => mag,
                    other => panic!("unexpected sum {:?}", other),
                };
                all_rows.push((k, sum));
            }
        }
        assert_eq!(all_rows.len(), 5000);
        all_rows.sort_by_key(|t| t.0);
        // Spot-check several rows.
        assert_eq!(all_rows[0], (0, 0));
        assert_eq!(all_rows[1234], (1234, 3702));
        assert_eq!(all_rows[4999], (4999, 14997));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn no_spill_when_budget_is_large() {
        let s = schema_kv();
        let pairs: Vec<(i64, i64)> = (0..100).map(|i| (i, i)).collect();
        let b = make_batch(&s, &pairs);
        let (out, m) = run_with_budget(
            s,
            vec![b],
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountStar,
                input_column: 0,
            }],
            // Wide budget.
            16 * 1024 * 1024,
        );
        assert_eq!(out.len(), 100);
        assert!(!m.did_spill);
        assert_eq!(m.spill_bytes, 0);
        assert!(
            m.is_streaming,
            "with_budget marks the operator as streaming"
        );
    }
}

#[cfg(test)]
mod spill_contract_regressions {
    use super::*;

    #[test]
    fn truncated_distinct_state_is_an_error() {
        for bytes in 0..4 {
            let mut state = AggState::default();
            assert!(decode_distinct_bytes_into(&mut state, &[1, 0, 0][..bytes]).is_err());
        }
        let mut state = AggState::default();
        assert!(decode_distinct_blob_into(&mut state, &Value::Int(1)).is_err());
    }
}

#[cfg(test)]
mod spill_comparison_domain_tests {
    use super::*;

    #[test]
    fn shared_spill_replay_uses_declared_minmax_collation_without_worker_context() {
        let dir = tempfile::tempdir().unwrap();
        let bin = crate::charset::resolve_collation("utf8mb4_bin").unwrap().id;
        let field = |name: &str, column_type, collation_id| BatchField {
            table: String::new(),
            alias: None,
            name: name.into(),
            column_type,
            source: crate::types::ColumnSource {
                collation_id,
                ..Default::default()
            },
        };
        let input = Arc::new(BatchSchema::new(vec![
            field("g", ColumnType::LongLong, None),
            field("s", ColumnType::VarString, Some(bin)),
        ]));
        let spill = SharedHashAggSpill::new(
            dir.path().to_path_buf(),
            input,
            &[0],
            vec![
                AggregateSpec {
                    func: AggregateKind::Min,
                    input_column: 1,
                    comparison_collation: None,
                },
                AggregateSpec {
                    func: AggregateKind::Max,
                    input_column: 1,
                    comparison_collation: None,
                },
            ],
        );
        for value in ["Z", "a"] {
            let batch = Batch::new(
                spill.schema.clone(),
                vec![
                    ColumnVector::from_values(vec![Value::Int(1)]),
                    ColumnVector::from_values(vec![Value::String(value.into())]),
                    ColumnVector::from_values(vec![Value::String(value.into())]),
                ],
            );
            spill.append(0, &batch).unwrap();
        }
        let partial = spill.partition_snapshot(0).unwrap().unwrap();
        let buckets = partial.into_buckets();
        assert_eq!(buckets.len(), 1);
        assert_eq!(
            crate::types::value_str(buckets[0].aggs[0].min.as_ref().unwrap()),
            Some("Z")
        );
        assert_eq!(
            crate::types::value_str(buckets[0].aggs[1].max.as_ref().unwrap()),
            Some("a")
        );
    }
}
