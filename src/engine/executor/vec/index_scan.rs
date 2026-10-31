//! Native B-tree sources for the unified `BatchOperator` pipeline.
//!
//! Lookup, multi-lookup, range and fast scans share this implementation.
//! Covering scans project index keys after heap-header MVCC validation;
//! non-covering scans batch heap reads and restore index order.

#[cfg(test)]
mod repair_tests;

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::sync::Arc;

use crate::engine::executor::select::plan::{
    index_scan_heap, CompiledPredicate, DecodedPredicateProgram, IndexEntryProjector,
};
use crate::engine::executor::vec::batch::{
    Batch, BatchAccumSink, ColumnAccumulator, DEFAULT_BATCH_ROWS,
};
use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
use crate::engine::executor::vec::stream::{
    BatchOperator, BatchOrdering, BatchPartitioning, BatchProperties, BatchSortKey,
};
use crate::parser::{BinaryOp, Expr};
use crate::planner::PlanTable;
use crate::storage::{
    Database, HeapHandle, IndexBound, IndexKey, IndexRangeCursor, IndexRangeReverseCursor,
    IndexSchema, KeyPart, RowId, TxnView,
};
use crate::types::{QueryError, Value};

#[derive(Debug, Clone)]
pub enum IndexBatchAccess {
    Lookup {
        key: IndexKey,
        exact: bool,
    },
    MultiLookup {
        keys: Vec<IndexKey>,
        exact: bool,
    },
    Range {
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        reverse: bool,
    },
}

enum ScanState {
    Uninitialized(IndexBatchAccess),
    Forward(IndexRangeCursor),
    RepairAwareForward(RepairAwareForward),
    Reverse(IndexRangeReverseCursor),
    RepairAwareReverse(RepairAwareReverse),
    Entries {
        values: Vec<(IndexKey, RowId)>,
        offset: usize,
        page_max_trx_id: Option<u64>,
    },
    RowIds {
        values: Vec<RowId>,
        offset: usize,
    },
    SpilledRowIds {
        _tmp: crate::engine::executor::select::grace::SpillTmpDir,
        reader: crate::engine::executor::spill_quota::TrackedSpillReader,
        remaining: u64,
    },
    /// One contiguous slice of a planner-fixed parallel covering scan. Each
    /// worker advances its own leaf slice; `seed_leaf_id` preserves the
    /// non-unique-index boundary de-duplication contract.
    LeafRange {
        leaf_ids: Vec<u64>,
        offset: usize,
        seed_leaf_id: Option<u64>,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    },
    Done,
}

/// A physical cursor's direction is a type property, so merge comparisons
/// and storage calls are statically dispatched in both scan directions.
trait OrderedIndexCursor {
    const REVERSE: bool;
    fn next_chunk(&mut self, rows: usize) -> Result<Vec<(IndexKey, RowId)>, QueryError>;
    fn is_finished(&self) -> bool;
    fn resume_position(&self) -> Option<(IndexKey, RowId)>;
    /// Discard everything read beyond `position` (a `resume_position`).
    fn rewind(&mut self, position: Option<(IndexKey, RowId)>);
}

impl OrderedIndexCursor for IndexRangeCursor {
    const REVERSE: bool = false;
    fn next_chunk(&mut self, rows: usize) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        IndexRangeCursor::next_chunk(self, rows)
    }
    fn is_finished(&self) -> bool {
        IndexRangeCursor::is_finished(self)
    }
    fn resume_position(&self) -> Option<(IndexKey, RowId)> {
        IndexRangeCursor::resume_position(self)
    }
    fn rewind(&mut self, position: Option<(IndexKey, RowId)>) {
        self.rewind_after(position);
    }
}

impl OrderedIndexCursor for IndexRangeReverseCursor {
    const REVERSE: bool = true;
    fn next_chunk(&mut self, rows: usize) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        IndexRangeReverseCursor::next_chunk(self, rows)
    }
    fn is_finished(&self) -> bool {
        IndexRangeReverseCursor::is_finished(self)
    }
    fn resume_position(&self) -> Option<(IndexKey, RowId)> {
        IndexRangeReverseCursor::resume_position(self)
    }
    fn rewind(&mut self, position: Option<(IndexKey, RowId)>) {
        self.rewind_before(position);
    }
}

type RepairAwareForward = RepairAwareCursor<IndexRangeCursor>;
type RepairAwareReverse = RepairAwareCursor<IndexRangeReverseCursor>;

/// Which old keys a repair refresh must consider for this scan.
#[derive(Clone)]
enum RepairScope {
    Range {
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    },
    Prefix(IndexKey),
}

/// Re-reads the MVCC repair directory for one index scan. A writer registers
/// an old key before it physically deletes that entry, so a refresh taken
/// after a physical read covers every entry missing from that read. The
/// version gate keeps an unchanged directory free of repeated lookups.
struct RepairRefresh {
    source: RepairSource,
    seen_version: Option<u64>,
}

enum RepairSource {
    Database {
        db: Arc<Database>,
        table: Arc<crate::storage::TableSchema>,
        index: IndexSchema,
        txn: TxnView,
        scope: RepairScope,
    },
    /// Unit tests: a fixed directory, returned once.
    #[cfg(test)]
    Fixed(Option<Vec<(IndexKey, RowId)>>),
}

impl RepairRefresh {
    fn new(
        db: &Arc<Database>,
        table: &Arc<crate::storage::TableSchema>,
        index: &IndexSchema,
        txn: TxnView,
        scope: RepairScope,
    ) -> Self {
        Self {
            source: RepairSource::Database {
                db: Arc::clone(db),
                table: Arc::clone(table),
                index: index.clone(),
                txn,
                scope,
            },
            seen_version: None,
        }
    }

    #[cfg(test)]
    fn fixed(repairs: Vec<(IndexKey, RowId)>) -> Self {
        Self {
            source: RepairSource::Fixed(Some(repairs)),
            seen_version: None,
        }
    }

    fn fetch_if_changed(&mut self) -> Result<Option<Vec<(IndexKey, RowId)>>, QueryError> {
        let (db, table, index, txn, scope) = match &mut self.source {
            RepairSource::Database {
                db,
                table,
                index,
                txn,
                scope,
            } => (&*db, &*table, &*index, *txn, &*scope),
            #[cfg(test)]
            RepairSource::Fixed(fixed) => return Ok(fixed.take()),
        };
        let version = db.index_repair_version();
        if self.seen_version == Some(version) {
            return Ok(None);
        }
        let entries = if db.has_pending_index_repairs_for(table.id, &index.name) {
            match scope {
                RepairScope::Range { low, high } => db.range_index_visibility_repair_entries(
                    table,
                    index,
                    low.clone(),
                    high.clone(),
                    txn,
                )?,
                RepairScope::Prefix(prefix) => {
                    db.prefix_index_visibility_repair_entries(table, index, prefix, txn)?
                }
            }
        } else {
            Vec::new()
        };
        self.seen_version = Some(version);
        Ok(Some(entries))
    }

    /// See [`Database::index_repair_rollback_epoch`].
    fn rollback_epoch(&self) -> u64 {
        match &self.source {
            RepairSource::Database {
                db, table, index, ..
            } => db.index_repair_rollback_epoch(table.id, &index.name),
            #[cfg(test)]
            RepairSource::Fixed(_) => 0,
        }
    }

    /// Leaf that owns a retired entry; parallel slices only.
    fn owning_leaf(&self, key: &IndexKey, row_id: RowId) -> Result<u64, QueryError> {
        match &self.source {
            RepairSource::Database {
                db, table, index, ..
            } => db.index_leaf_for_entry(table, index, key, row_id),
            #[cfg(test)]
            RepairSource::Fixed(_) => Ok(0),
        }
    }
}

/// Whether retired keys appeared inside an ordered scan's own scope. A
/// physical-only cursor switches to repair merging only then: retired keys
/// elsewhere in the index leave its fast path (and page shortcuts) intact.
fn scoped_repairs_appeared(
    probe: &mut Option<RepairRefresh>,
    scope: Option<&RepairScope>,
    db: &Arc<Database>,
    table: &Arc<crate::storage::TableSchema>,
    index: &IndexSchema,
    txn: TxnView,
) -> Result<bool, QueryError> {
    let Some(scope) = scope else {
        return Ok(false);
    };
    if !db.has_pending_index_repairs_for(table.id, &index.name) {
        return Ok(false);
    }
    let probe =
        probe.get_or_insert_with(|| RepairRefresh::new(db, table, index, txn, scope.clone()));
    Ok(probe
        .fetch_if_changed()?
        .is_some_and(|entries| !entries.is_empty()))
}

/// Repair merging for one planner-fixed parallel leaf slice. The shared range
/// lease freezes the B-tree topology, so each retired key has exactly one
/// owning leaf, and only the worker that reads that leaf emits it. Merging
/// after each physical read covers every entry missing from that read, and
/// suppressing the RowIds of all visible repairs keeps a moved row's newer
/// key, read by any worker, from producing it twice.
struct LeafRepairs {
    refresh: RepairRefresh,
    by_leaf: HashMap<u64, Vec<(IndexKey, RowId)>>,
    row_ids: HashSet<RowId>,
    seen: HashSet<(IndexKey, RowId)>,
    returned_row_ids: HashSet<RowId>,
}

impl LeafRepairs {
    fn new(refresh: RepairRefresh) -> Self {
        Self {
            refresh,
            by_leaf: HashMap::new(),
            row_ids: HashSet::new(),
            seen: HashSet::new(),
            returned_row_ids: HashSet::new(),
        }
    }

    fn refresh(&mut self) -> Result<(), QueryError> {
        let Some(entries) = self.refresh.fetch_if_changed()? else {
            return Ok(());
        };
        // A row updated several times has one repair per update; keep all
        // and let visibility select the snapshot's own version.
        for (key, row_id) in entries {
            self.row_ids.insert(row_id);
            if !self.seen.insert((key.clone(), row_id)) {
                continue;
            }
            let leaf = self.refresh.owning_leaf(&key, row_id)?;
            self.by_leaf.entry(leaf).or_default().push((key, row_id));
        }
        Ok(())
    }

    /// Merge one leaf read that started at rollback epoch `epoch_before`.
    /// False (nothing consumed) when the epoch moved: re-read the leaf.
    fn apply_entries(
        &mut self,
        leaf: u64,
        physical: &mut Vec<(IndexKey, RowId)>,
        epoch_before: u64,
    ) -> Result<bool, QueryError> {
        self.refresh()?;
        if self.refresh.rollback_epoch() != epoch_before {
            return Ok(false);
        }
        physical.retain(|entry| !self.row_ids.contains(&entry.1));
        if let Some(owned) = self.by_leaf.remove(&leaf) {
            physical.extend(owned);
        }
        Ok(true)
    }

    /// RowId consumers cannot tell a row's retired keys apart, so a repaired
    /// row is returned once, by the owner of its first retired key.
    fn apply_row_ids(
        &mut self,
        leaf: u64,
        physical: &mut Vec<RowId>,
        epoch_before: u64,
    ) -> Result<bool, QueryError> {
        self.refresh()?;
        if self.refresh.rollback_epoch() != epoch_before {
            return Ok(false);
        }
        physical.retain(|row_id| !self.row_ids.contains(row_id));
        if let Some(owned) = self.by_leaf.remove(&leaf) {
            physical.extend(
                owned
                    .into_iter()
                    .map(|(_, row_id)| row_id)
                    .filter(|row_id| self.returned_row_ids.insert(*row_id)),
            );
        }
        Ok(true)
    }
}

/// Merge persisted keys with the old keys visible to this snapshot. Retain
/// every repair RowId until exhaustion: even an already-emitted repair must
/// suppress a newer physical key encountered in a later storage chunk.
/// Repairs registered while the scan runs are merged after each physical
/// read, but only beyond the position already emitted: an old entry behind
/// that position was still physical when the cursor passed it.
struct RepairAwareCursor<C> {
    cursor: C,
    repairs: VecDeque<(IndexKey, RowId)>,
    repair_row_ids: HashSet<RowId>,
    repair_seen: HashSet<(IndexKey, RowId)>,
    returned_row_ids: HashSet<RowId>,
    physical: std::vec::IntoIter<(IndexKey, RowId)>,
    refresh: RepairRefresh,
    /// Last entry handed to the caller, in output order.
    emitted: Option<(IndexKey, RowId)>,
}

impl<C: OrderedIndexCursor> RepairAwareCursor<C> {
    /// `emitted` is the scan position already returned by a physical-only
    /// cursor that switches to repair merging midway; `pending` is the chunk
    /// that cursor read but has not returned yet, starting at rollback epoch
    /// `pending_epoch`. When that epoch moved, the chunk is read again.
    fn new(
        cursor: C,
        refresh: RepairRefresh,
        emitted: Option<(IndexKey, RowId)>,
        pending: Vec<(IndexKey, RowId)>,
        pending_epoch: Option<u64>,
    ) -> Result<Self, QueryError> {
        let mut merged = Self {
            cursor,
            repairs: VecDeque::new(),
            repair_row_ids: HashSet::new(),
            repair_seen: HashSet::new(),
            returned_row_ids: HashSet::new(),
            physical: Vec::new().into_iter(),
            refresh,
            emitted,
        };
        if !merged.accept_physical(pending, None, pending_epoch)? {
            let position = merged.emitted.clone();
            merged.cursor.rewind(position);
        }
        Ok(merged)
    }

    /// Merge a fixed repair directory (unit tests without a database feed).
    #[cfg(test)]
    fn with_static_repairs(cursor: C, repairs: Vec<(IndexKey, RowId)>) -> Self {
        Self::new(
            cursor,
            RepairRefresh::fixed(repairs),
            None,
            Vec::new(),
            None,
        )
        .expect("a fixed directory cannot fail")
    }

    fn after_emitted(entry: &(IndexKey, RowId), emitted: Option<&(IndexKey, RowId)>) -> bool {
        emitted.is_none_or(|emitted| {
            let order = entry
                .0
                .cmp(&emitted.0)
                .then_with(|| entry.1.cmp(&emitted.1));
            if C::REVERSE {
                order.is_lt()
            } else {
                order.is_gt()
            }
        })
    }

    /// Publish a freshly read physical chunk after merging the repairs that
    /// existed when it was read. A read that started at rollback epoch
    /// `epoch_before` is rejected (false) when the epoch moved meanwhile.
    fn accept_physical(
        &mut self,
        mut physical: Vec<(IndexKey, RowId)>,
        emitted: Option<&(IndexKey, RowId)>,
        epoch_before: Option<u64>,
    ) -> Result<bool, QueryError> {
        if let Some(mut fresh) = self.refresh.fetch_if_changed()? {
            let emitted = emitted.or(self.emitted.as_ref());
            // A repair behind the emitted position is not returned again: its
            // physical entry was still present when the cursor passed it. Its
            // RowId still suppresses the row's newer key further on. A row
            // updated several times has one repair per update; all are kept
            // and visibility selects the snapshot's own version.
            fresh.retain(|entry| {
                self.repair_row_ids.insert(entry.1);
                !self.repair_seen.contains(entry) && Self::after_emitted(entry, emitted)
            });
            self.repair_seen.extend(fresh.iter().cloned());
            if !fresh.is_empty() {
                fresh.extend(self.repairs.drain(..));
                fresh.sort_unstable_by(|left, right| {
                    left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
                });
                if C::REVERSE {
                    fresh.reverse();
                }
                self.repairs = fresh.into();
            }
        }
        if epoch_before.is_some_and(|epoch| epoch != self.refresh.rollback_epoch()) {
            return Ok(false);
        }
        physical.retain(|entry| !self.repair_row_ids.contains(&entry.1));
        self.physical = physical.into_iter();
        Ok(true)
    }

    fn refill_physical(
        &mut self,
        chunk_size: usize,
        emitted: Option<&(IndexKey, RowId)>,
    ) -> Result<(), QueryError> {
        while self.physical.len() == 0 && !self.cursor.is_finished() {
            let position = self.cursor.resume_position();
            let epoch = self.refresh.rollback_epoch();
            let physical = self.cursor.next_chunk(chunk_size.max(1))?;
            if !self.accept_physical(physical, emitted, Some(epoch))? {
                self.cursor.rewind(position);
            }
        }
        Ok(())
    }

    fn next_chunk(&mut self, chunk_size: usize) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        let mut out: Vec<(IndexKey, RowId)> = Vec::with_capacity(chunk_size);
        while out.len() < chunk_size {
            if self.physical.len() == 0 {
                let emitted = out.last().cloned();
                self.refill_physical(chunk_size - out.len(), emitted.as_ref())?;
            }
            let repair_first = match (self.physical.as_slice().first(), self.repairs.front()) {
                (None, None) => break,
                (Some(_), None) => false,
                (None, Some(_)) => true,
                (Some(physical), Some(repair)) => {
                    let order = repair
                        .0
                        .cmp(&physical.0)
                        .then_with(|| repair.1.cmp(&physical.1));
                    if C::REVERSE {
                        order.is_ge()
                    } else {
                        order.is_le()
                    }
                }
            };
            let entry = if repair_first {
                self.repairs.pop_front()
            } else {
                self.physical.next()
            };
            out.push(entry.expect("selected merge input is nonempty"));
        }
        if let Some(last) = out.last() {
            self.emitted = Some(last.clone());
        }
        Ok(out)
    }

    /// RowId consumers cannot tell a row's retired keys apart: a repaired row
    /// is returned once.
    fn next_row_ids(&mut self, chunk_size: usize) -> Result<Vec<RowId>, QueryError> {
        let entries = self.next_chunk(chunk_size)?;
        Ok(entries
            .into_iter()
            .map(|entry| entry.1)
            .filter(|row_id| {
                !self.repair_row_ids.contains(row_id) || self.returned_row_ids.insert(*row_id)
            })
            .collect())
    }

    fn is_finished(&self) -> bool {
        self.repairs.is_empty() && self.physical.len() == 0 && self.cursor.is_finished()
    }
}

struct ExactIndexRuntimeFilter {
    key_position: usize,
    filter: Arc<crate::engine::executor::select::bloom::ExactIntKeySet>,
    rejected: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl ExactIndexRuntimeFilter {
    #[inline]
    fn admits(&self, key: &IndexKey) -> bool {
        let Some(part) = key.0.get(self.key_position) else {
            return true;
        };
        let value = match part {
            KeyPart::Int(value) => i128::from(*value),
            KeyPart::UInt(value) => i128::from(*value),
            KeyPart::Date(value) => i128::from(*value),
            KeyPart::Time(value) | KeyPart::DateTime(value) => i128::from(*value),
            KeyPart::Year(value) => {
                if *value == 0 {
                    0
                } else {
                    1900 + i128::from(*value)
                }
            }
            // A schema/value mismatch must never turn a runtime filter into
            // a correctness filter. The parent hash join remains the oracle.
            _ => return true,
        };
        self.filter.contains(value)
    }
}

pub struct IndexBatchStream {
    db: Arc<Database>,
    txn: TxnView,
    table: PlanTable,
    index: IndexSchema,
    covering: bool,
    schema_cols: Vec<usize>,
    schema: Arc<BatchSchema>,
    properties: BatchProperties,
    projector: Option<IndexEntryProjector>,
    /// Raw-page projection for a single lossless integer column selected from
    /// a covering composite index. Parallel leaf workers can append this lane
    /// without allocating a complete `IndexKey` per entry.
    raw_i64_key_position: Option<usize>,
    heap: Option<HeapHandle>,
    state: ScanState,
    seen: Option<HashSet<RowId>>,
    batch_rows: usize,
    /// LIMIT/OFFSET row goal captured when this source is constructed.
    /// Residual-filter feedback may expand `batch_rows`, but never beyond the
    /// normal vector batch size or into a different access path.
    row_goal: Option<usize>,
    /// Runtime-only MRR grant. Serial/index-ordered sources retain the
    /// ordinary vector batch size so LIMIT can stop promptly; planner-fixed
    /// unordered parallel range workers opt into a wider governed window.
    mrr: Option<crate::engine::executor::select::plan::MrrWorkArea>,
    mrr_resident_bytes_per_row: usize,
    exact_runtime_filters: Vec<ExactIndexRuntimeFilter>,
    /// Query-executor reservation for runtime-filter-derived lookup keys,
    /// resolved probes and unique-key RowIds. It lives until the stream (the
    /// last owner) is dropped.
    runtime_lookup_charge: crate::engine::executor::mem_budget::ScopedCharge,
    /// Retain the shared topology guard after the planner's leaf list drops.
    /// All worker clones release it when the exchange closes/cancels them.
    topology_lease: Option<Arc<crate::storage::IndexRangeLease>>,
    /// Pre-merged RowIds have already been counted at their source leaves;
    /// the heap-batch stage must not count them as index entries again.
    count_rowids_as_index_entries: bool,
    /// Narrow residual evaluated while each heap payload is pinned.  Ordered
    /// row-goal scans can encounter a long run of rejected index entries;
    /// decoding the complete output projection for that run makes LIMIT pay
    /// wide-row materialization cost before it can stop.  This reuses the
    /// storage predicate-first oracle: predicate columns are decoded first,
    /// and output-only columns are decoded from the same payload only for
    /// accepted rows.
    predicate_first: Option<IndexPredicateFirst>,
    /// Aggregate-proven prefix demands, indexed by output projection slot.
    /// Used only by the current-version direct sink; MVCC fallback remains
    /// the complete-value oracle.
    prefix_limits: Vec<Option<usize>>,
    /// Key scope of an ordered physical cursor. A writer may retire old keys
    /// of this index after the scan started; the cursor then switches to
    /// repair merging for the remaining range.
    repair_scope: Option<RepairScope>,
    /// Repairs merged into a parallel leaf slice once the index has any.
    leaf_repairs: Option<LeafRepairs>,
    /// Version-gated probe for retired keys inside `repair_scope`.
    repair_probe: Option<RepairRefresh>,
}

struct IndexPredicateFirst {
    columns: Vec<usize>,
    compiled: CompiledPredicate,
    decoded: Option<DecodedPredicateProgram>,
    candidate_heap_ranges: Option<crate::storage::ZoneMapRanges>,
    /// Query-executor reservation for the retained compiled predicate and
    /// projected columns. Candidate ranges retain their own allocation lease.
    _metadata_charge: crate::engine::executor::mem_budget::ScopedCharge,
}

#[cfg(test)]
pub(crate) static PREDICATE_FIRST_INDEX_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

// A parallel worker may consume a wider RID window than the ordinary 2K
// vector batch, but each returned Batch remains bounded to 64K rows while the
// MRR work-area manager remains the reservation/revocation oracle.
const PARALLEL_MRR_MAX_BATCH_ROWS: usize = 65_536;
// A typed sink avoids per-row envelopes only after its fixed accumulator
// allocation is amortized. OLTP point/range probes below this threshold keep
// the compact row oracle; analytical/stock-level windows take the fused path.
const DIRECT_HEAP_BATCH_MIN_ROWS: usize = 8;
// Dynamic point probing is intentionally bounded. Above one ordinary pair of
// vector batches, a sequential prefix walk is usually cheaper and avoids a
// large transient probe array.
const RUNTIME_EXACT_MULTI_LOOKUP_MAX_KEYS: usize = 4_096;

fn runtime_int_key_part(value: i128, target: crate::storage::ParamTarget) -> Option<KeyPart> {
    use crate::storage::ParamTarget;
    let value = match target {
        ParamTarget::UInt => Value::UInt(u64::try_from(value).ok()?),
        ParamTarget::Date => Value::Date(i32::try_from(value).ok()?),
        ParamTarget::Time => Value::Time(i64::try_from(value).ok()?),
        ParamTarget::DateTime => Value::DateTime(i64::try_from(value).ok()?),
        ParamTarget::Timestamp => Value::Timestamp(i64::try_from(value).ok()?),
        ParamTarget::Year | ParamTarget::Int | ParamTarget::Decimal { .. } => {
            Value::Int(i64::try_from(value).ok()?)
        }
        // ExactIntKeySet is never built for these equality domains. Decline
        // conservatively if a future caller publishes an incompatible set.
        ParamTarget::Float32
        | ParamTarget::Float64
        | ParamTarget::Bytes
        | ParamTarget::JsonScalar
        | ParamTarget::CharString
        | ParamTarget::CollatedCharString { .. }
        | ParamTarget::String
        | ParamTarget::CollatedString { .. }
        | ParamTarget::LikePattern { .. } => return None,
    };
    KeyPart::for_target(value, target).ok()
}

#[cfg(test)]
pub(crate) static DIRECT_HEAP_BATCH_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) static DIRECT_HEAP_BATCH_FALLBACKS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) static RUNTIME_EXACT_MULTI_LOOKUP_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) static PREFIX_LOOKUP_CURSOR_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) static PREFIX_LOOKUP_REPAIR_CURSOR_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

impl IndexBatchStream {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: &Arc<Database>,
        txn: TxnView,
        table: &PlanTable,
        index: &IndexSchema,
        access: IndexBatchAccess,
        covering: bool,
        projected_columns: Option<&[usize]>,
    ) -> Result<Option<Self>, QueryError> {
        let schema_cols: Vec<usize> = projected_columns.map_or_else(
            || {
                table
                    .schema
                    .iter_visible_columns()
                    .map(|(idx, _)| idx)
                    .collect()
            },
            <[usize]>::to_vec,
        );
        if schema_cols.is_empty() {
            return Ok(None);
        }
        let fields = schema_cols
            .iter()
            .map(|&idx| BatchField {
                table: table.schema.name.clone(),
                alias: table.alias.clone(),
                name: table.schema.columns[idx].name.clone(),
                column_type: table.schema.result_type(idx),
                source: table
                    .schema
                    .wire_column_source_with_alias(idx, table.alias.as_deref()),
            })
            .collect();
        let schema = Arc::new(BatchSchema::new(fields));
        let reverse = matches!(&access, IndexBatchAccess::Range { reverse: true, .. });
        let ordering = index
            .columns
            .iter()
            .map_while(|name| {
                schema
                    .fields()
                    .iter()
                    .position(|field| field.name.eq_ignore_ascii_case(name))
                    .map(|column_index| {
                        // Storage keys are physically ascending, including DESC declarations.
                        let ascending = !reverse;
                        BatchSortKey {
                            column_index,
                            ascending,
                            nulls_first: ascending,
                        }
                    })
            })
            .collect::<Vec<_>>();
        let properties = BatchProperties {
            ordering: if ordering.is_empty() {
                BatchOrdering::Unknown
            } else {
                BatchOrdering::Ordered(ordering)
            },
            partitioning: BatchPartitioning::Single,
        };
        let row_goal = crate::engine::executor::select::vec_bridge::scan_row_goal();
        let projector = covering.then(|| IndexEntryProjector::new(table, index, projected_columns));
        let raw_i64_key_position = projector
            .as_ref()
            .and_then(IndexEntryProjector::single_lossless_i64_key_position);
        Ok(Some(Self {
            db: Arc::clone(db),
            txn,
            table: table.clone(),
            index: index.clone(),
            covering,
            schema_cols,
            schema,
            properties,
            projector,
            raw_i64_key_position,
            heap: None,
            state: ScanState::Uninitialized(access),
            // A scalar secondary index has exactly one entry per logical row,
            // even when many rows share the same key. Only a multi-valued
            // index can repeat one RowId under different keys and therefore
            // needs statement-wide de-duplication. Using `!unique` here made
            // every ordinary fact-table index-only scan hash and retain every
            // RowId for the full cursor lifetime.
            seen: index.has_multi_valued_parts().then(HashSet::new),
            batch_rows: crate::engine::executor::select::vec_bridge::scan_batch_rows(),
            row_goal,
            mrr: None,
            mrr_resident_bytes_per_row: 0,
            exact_runtime_filters: Vec::new(),
            runtime_lookup_charge: crate::engine::executor::mem_budget::ScopedCharge::new(0),
            topology_lease: None,
            count_rowids_as_index_entries: true,
            predicate_first: None,
            prefix_limits: Vec::new(),
            repair_scope: None,
            leaf_repairs: None,
            repair_probe: None,
        }))
    }

    /// Select predicate-first heap decoding before the cursor is opened.
    /// The physical access path and candidate order remain planner-fixed;
    /// this changes only the columns decoded for candidates rejected by the
    /// scan-local residual.
    pub(crate) fn enable_predicate_first(
        &mut self,
        columns: Vec<usize>,
        compiled: CompiledPredicate,
        decoded: Option<DecodedPredicateProgram>,
        candidate_heap_ranges: Option<crate::storage::ZoneMapRanges>,
    ) -> bool {
        debug_assert!(!columns.is_empty());
        debug_assert!(!self.covering);
        // Predicate-first is an optional decode layout. If its query-scoped
        // metadata cannot be reserved, keep the planner-selected ordinary
        // index source rather than turning memory pressure into a query
        // failure. Charge retained vectors by capacity; the conservative
        // fixed allowance covers the small compiled predicate trees and
        // cloned literal payloads.
        // Candidate ranges carry their own storage-scratch lease across
        // the stream lifetime. Charge only this source's additional metadata.
        let retained_bytes = columns
            .capacity()
            .saturating_mul(std::mem::size_of::<usize>())
            .saturating_add(1024);
        let mut metadata_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        if metadata_charge.add(retained_bytes).is_err() {
            return false;
        }
        self.predicate_first = Some(IndexPredicateFirst {
            columns,
            compiled,
            decoded,
            candidate_heap_ranges,
            _metadata_charge: metadata_charge,
        });
        #[cfg(test)]
        PREDICATE_FIRST_INDEX_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        true
    }

    #[inline]
    fn grow_row_goal_window(&mut self) -> Option<usize> {
        self.row_goal?;
        if self.mrr.is_some() || self.batch_rows >= DEFAULT_BATCH_ROWS {
            return None;
        }
        self.batch_rows = self
            .batch_rows
            .saturating_mul(8)
            .max(8)
            .min(DEFAULT_BATCH_ROWS);
        Some(self.batch_rows)
    }

    /// Native heap-batch source for a pre-merged, sorted RowId set. The
    /// reservation follows the stream until the final batch is consumed.
    pub(crate) fn from_row_ids(
        db: &Arc<Database>,
        txn: TxnView,
        table: &PlanTable,
        index: &IndexSchema,
        row_ids: Vec<RowId>,
        projected_columns: Option<&[usize]>,
    ) -> Result<Option<Self>, QueryError> {
        let mut resident = Some(row_ids);
        let bytes = resident
            .as_ref()
            .expect("index merge RowIds present")
            .capacity()
            .saturating_mul(std::mem::size_of::<RowId>());
        let charge = crate::engine::executor::mem_budget::ScopedCharge::new_strict(
            bytes,
            "index merge RowId set",
        );
        let mut spilled = None;
        if charge.is_err() {
            let tmp = crate::engine::executor::select::grace::SpillTmpDir::create_kind(
                db.data_dir(),
                "indexmerge",
            )?;
            let path = tmp.path().join("rowids.bin");
            let mut writer = crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
                &path,
                tmp.charger(),
                crate::engine::executor::spill_quota::writer_buffer_capacity(1),
            )?;
            let row_ids = resident.as_ref().expect("index merge RowIds present");
            for row_id in row_ids {
                writer
                    .write_all(&row_id.page_id.to_le_bytes())
                    .and_then(|_| writer.write_all(&row_id.slot_id.to_le_bytes()))
                    .map_err(|error| {
                        crate::storage::error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            format!("index merge spill I/O: {error}"),
                        )
                    })?;
            }
            let rows = row_ids.len() as u64;
            let _bytes = writer.finish()?;
            resident = None;
            let reader = crate::engine::executor::spill_quota::TrackedSpillReader::open(
                &path,
                tmp.charger(),
                crate::engine::executor::spill_quota::writer_buffer_capacity(1),
            )?;
            crate::engine::executor::spill_quota::record_operator_spill("index_merge");
            spilled = Some((tmp, reader, rows));
        }
        let mut stream = Self::new(
            db,
            txn,
            table,
            index,
            IndexBatchAccess::Lookup {
                key: IndexKey::from(Vec::new()),
                exact: true,
            },
            false,
            projected_columns,
        )?;
        if let Some(stream) = stream.as_mut() {
            stream.state = if let Some((tmp, reader, remaining)) = spilled {
                ScanState::SpilledRowIds {
                    _tmp: tmp,
                    reader,
                    remaining,
                }
            } else {
                ScanState::RowIds {
                    values: resident.take().expect("resident RowIds not spilled"),
                    offset: 0,
                }
            };
            stream.properties = BatchProperties {
                ordering: BatchOrdering::Unknown,
                partitioning: BatchPartitioning::Single,
            };
            stream.runtime_lookup_charge = charge
                .unwrap_or_else(|_| crate::engine::executor::mem_budget::ScopedCharge::new(0));
            stream.count_rowids_as_index_entries = false;
        }
        Ok(stream)
    }

    /// Split a forward range over contiguous B-tree leaf slices. Covering
    /// workers project keys directly; non-covering workers use a governed,
    /// wider MRR window before their page-local heap fetch. The returned
    /// workers are unopened and can therefore receive the same runtime filter
    /// before their owner starts any thread or performs I/O.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn parallel_range_workers(
        db: &Arc<Database>,
        txn: TxnView,
        table: &PlanTable,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        covering: bool,
        projected_columns: Option<&[usize]>,
        degree: usize,
    ) -> Result<Option<Vec<Self>>, QueryError> {
        if degree <= 1
            || db.has_pending_index_repairs_for(table.schema.id, &index.name)
            || table.schema.is_partitioned()
            // One logical MVI row may occur under keys assigned to different
            // workers. Keep its statement-wide RowId de-duplication in the
            // serial source until a shared bounded de-dup state exists.
            || index.has_multi_valued_parts()
        {
            return Ok(None);
        }
        let leaf_ids = db.range_index_leaf_ids(&table.schema, index, low.clone(), high.clone())?;
        let workers = degree.min(leaf_ids.len());
        if workers <= 1 {
            return Ok(None);
        }
        let chunk = leaf_ids.len().div_ceil(workers);
        let mut out = Vec::with_capacity(workers);
        for start in (0..leaf_ids.len()).step_by(chunk) {
            let end = start.saturating_add(chunk).min(leaf_ids.len());
            let mut worker = Self::new(
                db,
                txn,
                table,
                index,
                IndexBatchAccess::Range {
                    low: low.clone(),
                    high: high.clone(),
                    reverse: false,
                },
                covering,
                projected_columns,
            )?
            .ok_or_else(|| {
                crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "parallel index batch worker has no output columns",
                )
            })?;
            worker.properties = BatchProperties {
                ordering: BatchOrdering::Unknown,
                partitioning: BatchPartitioning::Single,
            };
            worker.topology_lease = leaf_ids.lease.clone();
            if !txn.historical {
                worker.repair_scope = Some(RepairScope::Range {
                    low: low.clone(),
                    high: high.clone(),
                });
            }
            worker.state = ScanState::LeafRange {
                leaf_ids: leaf_ids[start..end].to_vec(),
                offset: 0,
                seed_leaf_id: (start > 0).then(|| leaf_ids[start - 1]),
                low: low.clone(),
                high: high.clone(),
            };
            if !covering {
                let mrr = crate::engine::executor::select::plan::MrrWorkArea::new();
                let projected_width = 16usize.saturating_add(worker.schema_cols.iter().fold(
                    0usize,
                    |bytes, column_index| {
                        let Some(column) = worker.table.schema.columns.get(*column_index) else {
                            return bytes;
                        };
                        let width = worker
                            .table
                            .schema
                            .stats
                            .columns
                            .get(&column.name.to_ascii_lowercase())
                            .and_then(|stats| stats.average_width_bytes)
                            .unwrap_or(column.data_type.key_byte_length() as f64)
                            .ceil() as usize;
                        bytes.saturating_add(width)
                    },
                ));
                // The transient fetch result owns one Vec<Value> per row
                // before conversion into typed columns. Include both stack
                // shapes plus the established 32-byte RID/map allowance.
                worker.mrr_resident_bytes_per_row = projected_width
                    .saturating_add(std::mem::size_of::<Vec<Value>>())
                    .saturating_add(
                        worker
                            .schema_cols
                            .len()
                            .saturating_mul(std::mem::size_of::<Value>()),
                    )
                    .saturating_add(32);
                worker.batch_rows = mrr.window_rows_for_resident_bytes(
                    worker.mrr_resident_bytes_per_row,
                    PARALLEL_MRR_MAX_BATCH_ROWS,
                );
                worker.mrr = Some(mrr);
            }
            out.push(worker);
        }
        Ok(Some(out))
    }

    fn install_exact_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        // The key must name this scan's relation, not a sibling join input
        // that happens to share the column name.
        if crate::engine::executor::select::plan::runtime_filter_schema_indices(
            &filter,
            &self.table,
        )
        .is_none()
        {
            return false;
        }
        let crate::planner::RuntimeFilter::ExactIntKey {
            keys,
            null_safe,
            filter,
            rejected,
        } = filter
        else {
            return false;
        };
        if keys.len() != 1 || null_safe.first().copied().unwrap_or(false) {
            return false;
        }
        let column = match &keys[0] {
            Expr::Identifier(column) => column.as_str(),
            Expr::CompoundIdentifier(parts) => match parts.last() {
                Some(column) => column.as_str(),
                None => return false,
            },
            _ => return false,
        };
        let Some(key_position) = self
            .index
            .effective_columns()
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(column))
        else {
            return false;
        };

        // Exact runtime membership can become physical B-tree probes when it
        // supplies the first key part after an equality prefix. This is a
        // general dynamic-index-lookup transformation: instead of reading the
        // whole prefix range and rejecting rows after heap decode, probe only
        // the complete keys present on the hash-build side.
        if key_position < self.index.columns.len()
            && self.index.key_expression(key_position).is_none()
            && self.try_install_runtime_multi_lookup(key_position, &filter)
        {
            #[cfg(test)]
            RUNTIME_EXACT_MULTI_LOOKUP_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return true;
        }

        if !self.covering {
            return false;
        }
        // Build-side integer values do not prove the physical index domain:
        // an INT = DECIMAL equality is valid, but DECIMAL keys cannot use the
        // raw integer decoder. Leave other domains to the parent join.
        use crate::storage::schema::DataTypeKind;
        let Ok(column_index) = self.table.schema.column_index(column) else {
            return false;
        };
        if self.index.key_expression(key_position).is_some()
            || !matches!(
                self.table.schema.columns[column_index].data_type,
                DataTypeKind::TinyInt
                    | DataTypeKind::SmallInt
                    | DataTypeKind::MediumInt
                    | DataTypeKind::Int
                    | DataTypeKind::BigInt
                    | DataTypeKind::Bool
                    | DataTypeKind::Date
                    | DataTypeKind::Time { .. }
                    | DataTypeKind::DateTime { .. }
                    | DataTypeKind::Timestamp { .. }
                    | DataTypeKind::Year
            )
        {
            return false;
        }
        self.exact_runtime_filters.push(ExactIndexRuntimeFilter {
            key_position,
            filter,
            rejected: rejected.get().cloned(),
        });
        true
    }

    fn materialized_first_key_lower_bound(&self, expr: &Expr) -> Option<IndexBound> {
        fn find<'a>(
            expr: &'a Expr,
            table: &PlanTable,
            first_column: &str,
        ) -> Option<(&'a Expr, bool)> {
            if let Expr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } = expr
            {
                return find(left, table, first_column)
                    .or_else(|| find(right, table, first_column));
            }
            let Expr::Binary { left, op, right } = expr else {
                return None;
            };
            let is_target_column = |candidate: &Expr| -> bool {
                match candidate {
                    Expr::Identifier(name) => name.eq_ignore_ascii_case(first_column),
                    Expr::CompoundIdentifier(parts) => {
                        let Some(column) = parts.last() else {
                            return false;
                        };
                        if parts.len() >= 2 {
                            let qualifier = &parts[parts.len() - 2];
                            let matches_table = table.schema.name.eq_ignore_ascii_case(qualifier);
                            let matches_alias = table
                                .alias
                                .as_deref()
                                .is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier));
                            if !matches_table && !matches_alias {
                                return false;
                            }
                        }
                        column.eq_ignore_ascii_case(first_column)
                    }
                    _ => false,
                }
            };
            if is_target_column(left) {
                return match op {
                    BinaryOp::GtEq => Some((right, true)),
                    BinaryOp::Gt => Some((right, false)),
                    _ => None,
                };
            }
            if is_target_column(right) {
                return match op {
                    BinaryOp::LtEq => Some((left, true)),
                    BinaryOp::Lt => Some((left, false)),
                    _ => None,
                };
            }
            None
        }

        if self.index.key_expression(0).is_some() {
            return None;
        }
        let effective_columns = self.index.effective_columns();
        let first_column = effective_columns.first()?;
        let (constant, inclusive) = find(expr, &self.table, first_column)?;
        let value = crate::engine::expr::eval_const(constant).ok()?;
        if matches!(value, Value::Null) {
            return None;
        }
        let column = self
            .table
            .schema
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(first_column))?;
        let target = crate::storage::ParamTarget::for_column_with_collation(
            &column.data_type,
            column.unsigned,
            column.collation.as_deref(),
        );
        let part = KeyPart::for_target(value, target).ok()?;
        Some(IndexBound {
            key: IndexKey::from(vec![self.index.apply_prefix_to_key_part(0, part)]),
            inclusive,
        })
    }

    fn try_install_runtime_multi_lookup(
        &mut self,
        key_position: usize,
        filter: &crate::engine::executor::select::bloom::ExactIntKeySet,
    ) -> bool {
        if self.covering
            || !self.index.unique
            || filter.len() == 0
            || filter.len() > RUNTIME_EXACT_MULTI_LOOKUP_MAX_KEYS
            || key_position + 1 != self.index.columns.len()
        {
            return false;
        }
        let ScanState::Uninitialized(access) = &self.state else {
            return false;
        };
        let prefix = match access {
            IndexBatchAccess::Lookup { key, exact: false } if key.0.len() == key_position => {
                key.clone()
            }
            IndexBatchAccess::Range {
                low,
                high,
                reverse: false,
            } => match (low, high) {
                (None, None) if key_position == 0 => IndexKey::from(Vec::new()),
                (Some(low), Some(high))
                    if low.inclusive
                        && high.inclusive
                        && low.key == high.key
                        && low.key.0.len() == key_position =>
                {
                    low.key.clone()
                }
                _ => return false,
            },
            _ => return false,
        };
        // Keep every generated key inline. This avoids hidden per-key heap
        // payloads and lets capacity-based accounting below be exact.
        if prefix.0.len() >= 4
            || prefix.0.iter().any(|part| {
                !matches!(
                    part,
                    KeyPart::Null
                        | KeyPart::Int(_)
                        | KeyPart::UInt(_)
                        | KeyPart::Float32(_)
                        | KeyPart::Float64(_)
                        | KeyPart::Decimal { .. }
                        | KeyPart::Date(_)
                        | KeyPart::Time(_)
                        | KeyPart::DateTime(_)
                        | KeyPart::Year(_)
                        | KeyPart::Param { .. }
                )
            })
        {
            return false;
        }
        let Some(column_name) = self.index.columns.get(key_position) else {
            return false;
        };
        let Ok(column_index) = self.table.schema.column_index(column_name) else {
            return false;
        };
        let column = &self.table.schema.columns[column_index];
        let target = crate::storage::ParamTarget::for_column_with_collation(
            &column.data_type,
            column.unsigned,
            column.collation.as_deref(),
        );

        // Reserve every runtime-derived bulk allocation up front by capacity:
        // the retained key vector, initialize-time `(resolved, original)`
        // probes, and the at-most-one RowId per unique exact key. The charge
        // remains attached to this stream until its last owner is dropped.
        let bytes_per_key = std::mem::size_of::<IndexKey>()
            .saturating_add(std::mem::size_of::<(IndexKey, IndexKey)>())
            .saturating_add(std::mem::size_of::<RowId>());
        let requested = filter.len().saturating_mul(bytes_per_key);
        let mut attempt_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        if attempt_charge.add(requested).is_err() {
            return false;
        }
        let mut keys = Vec::new();
        if keys.try_reserve_exact(filter.len()).is_err() {
            return false;
        }
        let actual_key_bytes = keys
            .capacity()
            .saturating_mul(std::mem::size_of::<IndexKey>());
        if attempt_charge
            .add(
                actual_key_bytes
                    .saturating_sub(filter.len().saturating_mul(std::mem::size_of::<IndexKey>())),
            )
            .is_err()
        {
            return false;
        }
        let mut valid = true;
        filter.for_each_key(|value| {
            if !valid {
                return;
            }
            let Some(part) = runtime_int_key_part(value, target) else {
                valid = false;
                return;
            };
            let mut parts = prefix.0.clone();
            parts.push(part);
            keys.push(IndexKey::from(parts));
        });
        if !valid {
            return false;
        }
        self.state = ScanState::Uninitialized(IndexBatchAccess::MultiLookup { keys, exact: true });
        self.runtime_lookup_charge.absorb(&mut attempt_charge);
        true
    }

    fn heap(&mut self) -> Result<&HeapHandle, QueryError> {
        if self.heap.is_none() {
            self.heap = Some(index_scan_heap(&self.db, &self.table)?);
        }
        Ok(self.heap.as_ref().expect("index heap initialized"))
    }

    fn record_candidate_batch(&self, rows: usize) {
        if let Some(row_goal) = self.row_goal {
            crate::engine::executor::select::analyze_stats::add_index_candidate_batch(
                row_goal as u64,
                rows as u64,
                self.batch_rows as u64,
            );
        }
    }

    fn repair_refresh(&self) -> Result<RepairRefresh, QueryError> {
        let scope = self.repair_scope.clone().ok_or_else(|| {
            crate::error::catalog::ER_INTERNAL_ERROR
                .error("ordered index scan has no MVCC repair scope")
        })?;
        Ok(RepairRefresh::new(
            &self.db,
            &self.table.schema,
            &self.index,
            self.txn,
            scope,
        ))
    }

    /// Continue a physical-only ordered scan as a repair-merging one once a
    /// writer has retired keys of this index; checked after each read.
    /// `emitted` is the position already returned and `pending` the chunk
    /// read after it but not yet returned.
    fn switch_to_repair_aware(
        &mut self,
        emitted: Option<(IndexKey, RowId)>,
        pending: Vec<(IndexKey, RowId)>,
        pending_epoch: Option<u64>,
    ) -> Result<(), QueryError> {
        let refresh = self.repair_refresh()?;
        self.state =
            match std::mem::replace(&mut self.state, ScanState::Done) {
                ScanState::Forward(cursor) => ScanState::RepairAwareForward(
                    RepairAwareForward::new(cursor, refresh, emitted, pending, pending_epoch)?,
                ),
                ScanState::Reverse(cursor) => ScanState::RepairAwareReverse(
                    RepairAwareReverse::new(cursor, refresh, emitted, pending, pending_epoch)?,
                ),
                other => other,
            };
        Ok(())
    }

    fn initialize(&mut self) -> Result<(), QueryError> {
        if !matches!(self.state, ScanState::Uninitialized(_)) {
            return Ok(());
        }
        let ScanState::Uninitialized(access) = std::mem::replace(&mut self.state, ScanState::Done)
        else {
            unreachable!("index batch source initialization state changed")
        };
        self.state = match access {
            IndexBatchAccess::Range { low, high, reverse } => {
                if !self.txn.historical {
                    self.repair_scope = Some(RepairScope::Range {
                        low: low.clone(),
                        high: high.clone(),
                    });
                }
                if self
                    .db
                    .has_pending_index_repairs_for(self.table.schema.id, &self.index.name)
                    && !self.txn.historical
                {
                    let refresh = self.repair_refresh()?;
                    if reverse {
                        let cursor = self.db.range_index_entries_reverse_cursor(
                            &self.table.schema,
                            &self.index,
                            low,
                            high,
                        )?;
                        ScanState::RepairAwareReverse(RepairAwareReverse::new(
                            cursor,
                            refresh,
                            None,
                            Vec::new(),
                            None,
                        )?)
                    } else {
                        let cursor = self.db.range_index_entries_cursor(
                            &self.table.schema,
                            &self.index,
                            low,
                            high,
                        )?;
                        ScanState::RepairAwareForward(RepairAwareForward::new(
                            cursor,
                            refresh,
                            None,
                            Vec::new(),
                            None,
                        )?)
                    }
                } else if self
                    .db
                    .has_pending_index_repairs_for(self.table.schema.id, &self.index.name)
                {
                    // Historical scans also merge persisted history-index
                    // entries; retain their established materialized oracle.
                    let mut values = self.db.range_index_bounds_entries_visible(
                        &self.table.schema,
                        &self.index,
                        low,
                        high,
                        self.txn,
                    )?;
                    values.sort_by(|left, right| {
                        left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
                    });
                    if reverse {
                        values.reverse();
                    }
                    if self.covering {
                        ScanState::Entries {
                            values,
                            offset: 0,
                            page_max_trx_id: None,
                        }
                    } else {
                        ScanState::RowIds {
                            values: values.into_iter().map(|(_, row_id)| row_id).collect(),
                            offset: 0,
                        }
                    }
                } else if reverse {
                    ScanState::Reverse(self.db.range_index_entries_reverse_cursor(
                        &self.table.schema,
                        &self.index,
                        low,
                        high,
                    )?)
                } else {
                    ScanState::Forward(self.db.range_index_entries_cursor(
                        &self.table.schema,
                        &self.index,
                        low,
                        high,
                    )?)
                }
            }
            IndexBatchAccess::Lookup { key, exact } => {
                // A prefix lookup can cover an arbitrarily large suffix of a
                // composite index. Materializing that whole suffix before a
                // parent LIMIT gets its first batch defeats streaming (and in
                // queue/oldest-row workloads repeatedly decodes thousands of
                // keys to return one). Inclusive equal prefix bounds have the
                // exact same semantics in IndexRangeCursor: its raw bound
                // comparator treats longer stored keys that start with the
                // bound as prefix matches. Old-key MVCC repairs are merged as
                // a second ordered stream, so an in-flight writer no longer
                // forces the whole prefix to be materialised.
                let cursor_prefix = if !exact
                    && !self.txn.historical
                    // `exact=false` can also mean "full unique key plus a
                    // residual predicate". Keep that case on the compiled
                    // unique-point kernel; only a genuinely shorter key is an
                    // unbounded composite-prefix lookup.
                    && key.0.len() < self.index.effective_columns().len()
                {
                    Some(key.resolve_params()?)
                } else {
                    None
                };
                if let Some(cursor_prefix) = cursor_prefix {
                    self.repair_scope = Some(RepairScope::Prefix(cursor_prefix.clone()));
                    let has_repairs = scoped_repairs_appeared(
                        &mut self.repair_probe,
                        self.repair_scope.as_ref(),
                        &self.db,
                        &self.table.schema,
                        &self.index,
                        self.txn,
                    )?;
                    let bound = IndexBound {
                        key: cursor_prefix,
                        inclusive: true,
                    };
                    let cursor = self.db.range_index_entries_cursor(
                        &self.table.schema,
                        &self.index,
                        Some(bound.clone()),
                        Some(bound),
                    )?;
                    if !has_repairs {
                        #[cfg(test)]
                        PREFIX_LOOKUP_CURSOR_HITS
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        ScanState::Forward(cursor)
                    } else {
                        #[cfg(test)]
                        PREFIX_LOOKUP_REPAIR_CURSOR_HITS
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        ScanState::RepairAwareForward(RepairAwareForward::new(
                            cursor,
                            self.repair_refresh()?,
                            None,
                            Vec::new(),
                            None,
                        )?)
                    }
                } else if self.covering {
                    let (values, bound) = if exact {
                        self.db.lookup_index_entries_visible_with_bound(
                            &self.table.schema,
                            &self.index,
                            key,
                            self.txn,
                        )?
                    } else {
                        self.db.prefix_index_entries_visible_with_bound(
                            &self.table.schema,
                            &self.index,
                            key,
                            self.txn,
                        )?
                    };
                    ScanState::Entries {
                        values,
                        offset: 0,
                        page_max_trx_id: bound,
                    }
                } else {
                    let values = if exact {
                        self.db.lookup_index_visible(
                            &self.table.schema,
                            &self.index,
                            key,
                            self.txn,
                        )?
                    } else {
                        self.db.prefix_index_visible(
                            &self.table.schema,
                            &self.index,
                            key,
                            self.txn,
                        )?
                    };
                    ScanState::RowIds { values, offset: 0 }
                }
            }
            IndexBatchAccess::MultiLookup { keys, exact } => {
                let probe_capacity = keys.len();
                let mut probes = keys
                    .into_iter()
                    .map(|key| Ok((key.resolve_params()?, key)))
                    .collect::<Result<Vec<_>, QueryError>>()?;
                probes.sort_unstable_by(|left, right| left.0.cmp(&right.0));
                probes.dedup_by(|left, right| left.0 == right.0);
                let mut seen = HashSet::new();
                if self.covering {
                    let mut values = Vec::new();
                    for (_, key) in probes {
                        let entries = if exact {
                            self.db.lookup_index_entries_visible(
                                &self.table.schema,
                                &self.index,
                                key,
                                self.txn,
                            )?
                        } else {
                            self.db.prefix_index_entries_visible(
                                &self.table.schema,
                                &self.index,
                                key,
                                self.txn,
                            )?
                        };
                        values.extend(entries.into_iter().filter(|entry| seen.insert(entry.1)));
                    }
                    ScanState::Entries {
                        values,
                        offset: 0,
                        page_max_trx_id: None,
                    }
                } else {
                    let mut values = if exact && self.index.unique {
                        Vec::with_capacity(probe_capacity)
                    } else {
                        Vec::new()
                    };
                    for (_, key) in probes {
                        let ids = if exact {
                            self.db.lookup_index_visible(
                                &self.table.schema,
                                &self.index,
                                key,
                                self.txn,
                            )?
                        } else {
                            self.db.prefix_index_visible(
                                &self.table.schema,
                                &self.index,
                                key,
                                self.txn,
                            )?
                        };
                        if exact && self.index.unique {
                            values.extend(ids);
                        } else {
                            values.extend(ids.into_iter().filter(|row_id| seen.insert(*row_id)));
                        }
                    }
                    ScanState::RowIds { values, offset: 0 }
                }
            }
        };
        Ok(())
    }

    fn next_covering_entries(
        &mut self,
        soft_cap: usize,
        raw_filter: Option<(
            usize,
            Arc<crate::engine::executor::select::bloom::ExactIntKeySet>,
        )>,
    ) -> Result<
        Option<(
            Vec<(IndexKey, RowId)>,
            Option<u64>,
            usize,
            usize,
            Option<usize>,
        )>,
        QueryError,
    > {
        let mut skipped_examined = 0usize;
        let mut skipped_rejected = 0usize;
        loop {
            match &mut self.state {
                ScanState::Forward(cursor) => {
                    let emitted = self
                        .repair_scope
                        .is_some()
                        .then(|| cursor.resume_position())
                        .flatten();
                    let epoch = self.repair_scope.is_some().then(|| {
                        self.db
                            .index_repair_rollback_epoch(self.table.schema.id, &self.index.name)
                    });
                    let (values, bound, examined, rejected, raw_filter_index) =
                        if let Some((filter_index, filter)) = raw_filter.as_ref() {
                            let (values, bound, examined, rejected) = cursor
                                .next_leaf_chunk_filtered_first_int(soft_cap, |value| {
                                    value.is_some_and(|value| filter.contains(value))
                                })?;
                            (values, bound, examined, rejected, Some(*filter_index))
                        } else {
                            let (values, bound) = cursor.next_leaf_chunk(soft_cap)?;
                            let examined = values.len();
                            (values, bound, examined, 0, None)
                        };
                    let examined = examined.saturating_add(skipped_examined);
                    let rejected = rejected.saturating_add(skipped_rejected);
                    let appeared = scoped_repairs_appeared(
                        &mut self.repair_probe,
                        self.repair_scope.as_ref(),
                        &self.db,
                        &self.table.schema,
                        &self.index,
                        self.txn,
                    )?;
                    if epoch.is_some_and(|epoch| {
                        epoch
                            != self
                                .db
                                .index_repair_rollback_epoch(self.table.schema.id, &self.index.name)
                    }) {
                        // A rolled-back writer's retired keys left the
                        // directory during this read; repeat it. The probe
                        // has consumed any repairs it saw, so switch now.
                        cursor.rewind_after(emitted.clone());
                        if appeared {
                            self.switch_to_repair_aware(emitted, Vec::new(), None)?;
                        }
                        continue;
                    }
                    if appeared {
                        self.switch_to_repair_aware(emitted, values, epoch)?;
                        continue;
                    }
                    if values.is_empty() {
                        if cursor.is_finished() {
                            self.state = ScanState::Done;
                            if examined == 0 {
                                return Ok(None);
                            }
                            return Ok(Some((
                                values,
                                Some(bound),
                                examined,
                                rejected,
                                raw_filter_index,
                            )));
                        }
                        skipped_examined = examined;
                        skipped_rejected = rejected;
                        continue;
                    }
                    return Ok(Some((
                        values,
                        Some(bound),
                        examined,
                        rejected,
                        raw_filter_index,
                    )));
                }
                ScanState::RepairAwareForward(cursor) => {
                    let values = cursor.next_chunk(soft_cap)?;
                    if values.is_empty() {
                        debug_assert!(cursor.is_finished());
                        self.state = ScanState::Done;
                        return Ok(None);
                    }
                    let examined = values.len();
                    return Ok(Some((values, None, examined, 0, None)));
                }
                ScanState::RepairAwareReverse(cursor) => {
                    let values = cursor.next_chunk(soft_cap)?;
                    if values.is_empty() {
                        debug_assert!(cursor.is_finished());
                        self.state = ScanState::Done;
                        return Ok(None);
                    }
                    let examined = values.len();
                    return Ok(Some((values, None, examined, 0, None)));
                }
                ScanState::Reverse(cursor) => {
                    let emitted = self
                        .repair_scope
                        .is_some()
                        .then(|| cursor.resume_position())
                        .flatten();
                    let epoch = self.repair_scope.is_some().then(|| {
                        self.db
                            .index_repair_rollback_epoch(self.table.schema.id, &self.index.name)
                    });
                    let (values, bound) = cursor.next_leaf_chunk(soft_cap)?;
                    let appeared = scoped_repairs_appeared(
                        &mut self.repair_probe,
                        self.repair_scope.as_ref(),
                        &self.db,
                        &self.table.schema,
                        &self.index,
                        self.txn,
                    )?;
                    if epoch.is_some_and(|epoch| {
                        epoch
                            != self
                                .db
                                .index_repair_rollback_epoch(self.table.schema.id, &self.index.name)
                    }) {
                        cursor.rewind_before(emitted.clone());
                        if appeared {
                            self.switch_to_repair_aware(emitted, Vec::new(), None)?;
                        }
                        continue;
                    }
                    if appeared {
                        self.switch_to_repair_aware(emitted, values, epoch)?;
                        continue;
                    }
                    if values.is_empty() {
                        if cursor.is_finished() {
                            self.state = ScanState::Done;
                            return Ok(None);
                        }
                        continue;
                    }
                    let examined = values.len();
                    return Ok(Some((values, Some(bound), examined, 0, None)));
                }
                ScanState::Entries {
                    values,
                    offset,
                    page_max_trx_id,
                } => {
                    if *offset >= values.len() {
                        self.state = ScanState::Done;
                        return Ok(None);
                    }
                    let end = (*offset + soft_cap).min(values.len());
                    let chunk = values[*offset..end].to_vec();
                    *offset = end;
                    let examined = chunk.len();
                    return Ok(Some((chunk, *page_max_trx_id, examined, 0, None)));
                }
                ScanState::LeafRange {
                    leaf_ids,
                    offset,
                    seed_leaf_id,
                    low,
                    high,
                } => {
                    if *offset >= leaf_ids.len() {
                        self.state = ScanState::Done;
                        return Ok(None);
                    }
                    let leaf_id = leaf_ids[*offset];
                    let seed = *seed_leaf_id;
                    let mut entries = Vec::new();
                    let mut page_bound = None;
                    let epoch = self
                        .db
                        .index_repair_rollback_epoch(self.table.schema.id, &self.index.name);
                    self.db.visit_index_leaf_entry_chunks_with_bound(
                        &self.table.schema,
                        &self.index,
                        std::slice::from_ref(&leaf_id),
                        seed,
                        low.clone(),
                        high.clone(),
                        |chunk, bound| {
                            page_bound = Some(bound);
                            entries.extend(
                                chunk.iter().map(|(key, row_id)| ((*key).clone(), *row_id)),
                            );
                            Ok(())
                        },
                    )?;
                    let mut accepted = true;
                    if let Some(scope) = self.repair_scope.as_ref().filter(|_| {
                        self.leaf_repairs.is_some()
                            || self.db.has_pending_index_repairs_for(
                                self.table.schema.id,
                                &self.index.name,
                            )
                    }) {
                        // Before the raw filter: owned old keys must pass it too.
                        accepted = self
                            .leaf_repairs
                            .get_or_insert_with(|| {
                                LeafRepairs::new(RepairRefresh::new(
                                    &self.db,
                                    &self.table.schema,
                                    &self.index,
                                    self.txn,
                                    scope.clone(),
                                ))
                            })
                            .apply_entries(leaf_id, &mut entries, epoch)?;
                    } else if self.repair_scope.is_some() {
                        accepted = epoch
                            == self.db.index_repair_rollback_epoch(
                                self.table.schema.id,
                                &self.index.name,
                            );
                    }
                    if !accepted {
                        // A rolled-back writer's retired keys left the
                        // directory during this read; repeat the leaf.
                        continue;
                    }
                    *offset += 1;
                    *seed_leaf_id = Some(leaf_id);
                    let examined = entries.len();
                    let mut rejected = 0usize;
                    let raw_filter_index = raw_filter.as_ref().map(|(index, _)| *index);
                    if let Some((_, filter)) = raw_filter.as_ref() {
                        entries.retain(|(key, _)| {
                            let value = key.0.first().and_then(|part| match part {
                                KeyPart::Int(value) => Some(i128::from(*value)),
                                KeyPart::UInt(value) => Some(i128::from(*value)),
                                KeyPart::Date(value) => Some(i128::from(*value)),
                                KeyPart::Time(value) | KeyPart::DateTime(value) => {
                                    Some(i128::from(*value))
                                }
                                KeyPart::Year(value) => Some(if *value == 0 {
                                    0
                                } else {
                                    1900 + i128::from(*value)
                                }),
                                _ => None,
                            });
                            let admitted = value.is_some_and(|value| filter.contains(value));
                            rejected += usize::from(!admitted);
                            admitted
                        });
                    }
                    if entries.is_empty() {
                        if *offset >= leaf_ids.len() {
                            self.state = ScanState::Done;
                            if examined == 0 {
                                return Ok(None);
                            }
                            return Ok(Some((
                                entries,
                                page_bound,
                                examined,
                                rejected,
                                raw_filter_index,
                            )));
                        }
                        continue;
                    }
                    return Ok(Some((
                        entries,
                        page_bound,
                        examined,
                        rejected,
                        raw_filter_index,
                    )));
                }
                ScanState::Done => return Ok(None),
                ScanState::Uninitialized(_)
                | ScanState::RowIds { .. }
                | ScanState::SpilledRowIds { .. } => unreachable!(),
            }
        }
    }

    fn next_row_ids(&mut self) -> Result<Option<Vec<RowId>>, QueryError> {
        loop {
            let mut chunk = match &mut self.state {
                ScanState::Forward(cursor) => {
                    let emitted = self
                        .repair_scope
                        .is_some()
                        .then(|| cursor.resume_position())
                        .flatten();
                    let epoch = self.repair_scope.is_some().then(|| {
                        self.db
                            .index_repair_rollback_epoch(self.table.schema.id, &self.index.name)
                    });
                    let chunk = cursor.next_chunk_row_ids(self.batch_rows)?;
                    let appeared = scoped_repairs_appeared(
                        &mut self.repair_probe,
                        self.repair_scope.as_ref(),
                        &self.db,
                        &self.table.schema,
                        &self.index,
                        self.txn,
                    )?;
                    if epoch.is_some_and(|epoch| {
                        epoch
                            != self
                                .db
                                .index_repair_rollback_epoch(self.table.schema.id, &self.index.name)
                    }) {
                        cursor.rewind_after(emitted.clone());
                        if appeared {
                            self.switch_to_repair_aware(emitted, Vec::new(), None)?;
                        }
                        continue;
                    }
                    if appeared {
                        // RowIds carry no keys to merge or recheck; re-read
                        // this chunk through the repair-merging cursor.
                        cursor.rewind_after(emitted.clone());
                        self.switch_to_repair_aware(emitted, Vec::new(), None)?;
                        continue;
                    }
                    chunk
                }
                ScanState::RepairAwareForward(cursor) => cursor.next_row_ids(self.batch_rows)?,
                ScanState::RepairAwareReverse(cursor) => cursor.next_row_ids(self.batch_rows)?,
                ScanState::Reverse(cursor) => {
                    let emitted = self
                        .repair_scope
                        .is_some()
                        .then(|| cursor.resume_position())
                        .flatten();
                    let epoch = self.repair_scope.is_some().then(|| {
                        self.db
                            .index_repair_rollback_epoch(self.table.schema.id, &self.index.name)
                    });
                    let chunk = cursor.next_chunk_row_ids(self.batch_rows)?;
                    let appeared = scoped_repairs_appeared(
                        &mut self.repair_probe,
                        self.repair_scope.as_ref(),
                        &self.db,
                        &self.table.schema,
                        &self.index,
                        self.txn,
                    )?;
                    if epoch.is_some_and(|epoch| {
                        epoch
                            != self
                                .db
                                .index_repair_rollback_epoch(self.table.schema.id, &self.index.name)
                    }) {
                        cursor.rewind_before(emitted.clone());
                        if appeared {
                            self.switch_to_repair_aware(emitted, Vec::new(), None)?;
                        }
                        continue;
                    }
                    if appeared {
                        cursor.rewind_before(emitted.clone());
                        self.switch_to_repair_aware(emitted, Vec::new(), None)?;
                        continue;
                    }
                    chunk
                }
                ScanState::RowIds { values, offset } => {
                    if *offset >= values.len() {
                        self.state = ScanState::Done;
                        return Ok(None);
                    }
                    let end = (*offset + self.batch_rows).min(values.len());
                    let chunk = values[*offset..end].to_vec();
                    *offset = end;
                    chunk
                }
                ScanState::SpilledRowIds {
                    reader, remaining, ..
                } => {
                    if *remaining == 0 {
                        self.state = ScanState::Done;
                        return Ok(None);
                    }
                    let take = (*remaining as usize).min(self.batch_rows.max(1));
                    let mut row_ids = Vec::with_capacity(take);
                    for _ in 0..take {
                        let mut page = [0u8; 4];
                        let mut slot = [0u8; 2];
                        reader.read_exact(&mut page).map_err(|error| {
                            crate::storage::error(
                                crate::error::catalog::ER_UNKNOWN_ERROR,
                                format!("index merge spill read: {error}"),
                            )
                        })?;
                        reader.read_exact(&mut slot).map_err(|error| {
                            crate::storage::error(
                                crate::error::catalog::ER_UNKNOWN_ERROR,
                                format!("index merge spill read: {error}"),
                            )
                        })?;
                        row_ids.push(RowId::new(
                            u32::from_le_bytes(page),
                            u16::from_le_bytes(slot),
                        ));
                    }
                    *remaining -= take as u64;
                    row_ids
                }
                ScanState::LeafRange {
                    leaf_ids,
                    offset,
                    low,
                    high,
                    ..
                } => {
                    debug_assert!(!self.covering);
                    let target = self
                        .mrr
                        .as_ref()
                        .map(|mrr| {
                            mrr.window_rows_for_resident_bytes(
                                self.mrr_resident_bytes_per_row,
                                PARALLEL_MRR_MAX_BATCH_ROWS,
                            )
                        })
                        .unwrap_or(self.batch_rows)
                        .max(1);
                    let mut row_ids = Vec::with_capacity(target);
                    while *offset < leaf_ids.len() && row_ids.len() < target {
                        let leaf = &leaf_ids[*offset..*offset + 1];
                        let epoch = self
                            .db
                            .index_repair_rollback_epoch(self.table.schema.id, &self.index.name);
                        let mut leaf_row_ids = self.db.range_index_row_ids_for_leaf_ids(
                            &self.table.schema,
                            &self.index,
                            leaf,
                            low.clone(),
                            high.clone(),
                        )?;
                        if let Some(scope) = self.repair_scope.as_ref().filter(|_| {
                            self.leaf_repairs.is_some()
                                || self.db.has_pending_index_repairs_for(
                                    self.table.schema.id,
                                    &self.index.name,
                                )
                        }) {
                            let accepted = self
                                .leaf_repairs
                                .get_or_insert_with(|| {
                                    LeafRepairs::new(RepairRefresh::new(
                                        &self.db,
                                        &self.table.schema,
                                        &self.index,
                                        self.txn,
                                        scope.clone(),
                                    ))
                                })
                                .apply_row_ids(leaf[0], &mut leaf_row_ids, epoch)?;
                            if !accepted {
                                continue;
                            }
                        } else if self.repair_scope.is_some()
                            && epoch
                                != self.db.index_repair_rollback_epoch(
                                    self.table.schema.id,
                                    &self.index.name,
                                )
                        {
                            continue;
                        }
                        row_ids.extend(leaf_row_ids);
                        *offset += 1;
                    }
                    // Exchange makes this source explicitly unordered. Heap
                    // order turns sparse secondary-index RIDs into monotonic
                    // probe batches, maximising page coalescing and avoiding
                    // the per-batch HashMap reorder required by serial index
                    // order. Scalar indexes contain one entry per RowId;
                    // multi-valued indexes are excluded at worker creation.
                    row_ids.sort_unstable();
                    row_ids
                }
                ScanState::Done => return Ok(None),
                ScanState::Uninitialized(_) | ScanState::Entries { .. } => unreachable!(),
            };
            if let Some(seen) = self.seen.as_mut() {
                chunk.retain(|row_id| seen.insert(*row_id));
            }
            if !chunk.is_empty() {
                return Ok(Some(chunk));
            }
            let finished = match &self.state {
                ScanState::Forward(cursor) => cursor.is_finished(),
                ScanState::RepairAwareForward(cursor) => cursor.is_finished(),
                ScanState::RepairAwareReverse(cursor) => cursor.is_finished(),
                ScanState::Reverse(cursor) => cursor.is_finished(),
                ScanState::RowIds { values, offset } => *offset >= values.len(),
                ScanState::SpilledRowIds { remaining, .. } => *remaining == 0,
                ScanState::Done => true,
                ScanState::LeafRange {
                    leaf_ids, offset, ..
                } => *offset >= leaf_ids.len(),
                _ => false,
            };
            if finished {
                self.state = ScanState::Done;
                return Ok(None);
            }
        }
    }

    fn next_parallel_covering_i64_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let key_position = self
            .raw_i64_key_position
            .expect("raw integer covering projection was preflighted");
        let field = self
            .schema
            .fields()
            .first()
            .expect("single integer covering projection has one field");
        let mut accumulator = ColumnAccumulator::for_field(field, self.batch_rows);
        let mut count = 0usize;
        let mut leaf_values = Vec::new();
        let mut runtime_rejections = vec![0u64; self.exact_runtime_filters.len()];
        let mut leaf_rejections = vec![0u64; self.exact_runtime_filters.len()];
        let mut rewound = false;
        while count < self.batch_rows {
            crate::engine::cancel::check_cancelled()?;
            leaf_rejections.fill(0);
            let next = match &mut self.state {
                ScanState::LeafRange {
                    leaf_ids,
                    offset,
                    seed_leaf_id,
                    low,
                    high,
                } => {
                    let Some(&leaf_id) = leaf_ids.get(*offset) else {
                        self.state = ScanState::Done;
                        break;
                    };
                    let next = (leaf_id, *seed_leaf_id, low.clone(), high.clone());
                    *offset += 1;
                    *seed_leaf_id = Some(leaf_id);
                    next
                }
                ScanState::Done => break,
                _ => unreachable!("raw integer covering projection requires a leaf-range worker"),
            };
            let (leaf_id, seed_leaf_id, low, high) = next;
            let heap = self.heap()?.clone();
            let mut examined = 0usize;
            self.db
                .visit_index_lossless_i64_leaf_entry_chunks_with_bound(
                    &self.table.schema,
                    &self.index,
                    std::slice::from_ref(&leaf_id),
                    seed_leaf_id,
                    low,
                    high,
                    key_position,
                    |entries, page_max_trx_id| {
                        examined = examined.saturating_add(entries.len());
                        let visible = self
                            .db
                            .try_index_page_max_trx_shortcut_for(
                                self.table.schema.id,
                                &self.index.name,
                                page_max_trx_id,
                                self.txn,
                                entries.len(),
                            )
                            .map(Ok::<_, QueryError>)
                            .unwrap_or_else(|| {
                                self.db.filter_visible_index_i64_entries_mask_with_handle(
                                    &heap,
                                    &self.table.schema,
                                    &self.index,
                                    entries,
                                    key_position,
                                    self.txn,
                                )
                            })?;
                        for ((value, _), visible) in entries.iter().zip(visible) {
                            if !visible {
                                continue;
                            }
                            let mut admitted = true;
                            for (position, filter) in self.exact_runtime_filters.iter().enumerate()
                            {
                                if !value
                                    .is_some_and(|value| filter.filter.contains(i128::from(value)))
                                {
                                    leaf_rejections[position] += 1;
                                    admitted = false;
                                    break;
                                }
                            }
                            if admitted {
                                leaf_values.push(*value);
                            }
                        }
                        Ok(())
                    },
                )?;
            crate::engine::executor::select::analyze_stats::add_range_scan_work(
                examined as u64,
                0,
                0,
            );
            self.record_candidate_batch(examined);
            if self.repair_scope.is_some()
                && self
                    .db
                    .has_pending_index_repairs_for(self.table.schema.id, &self.index.name)
            {
                // A writer retired keys of this index; this lane carries no
                // keys or RowIds to merge repairs into. Hand the leaf back so
                // the keyed covering path re-reads it with repair merging.
                if let ScanState::LeafRange {
                    offset,
                    seed_leaf_id: seed,
                    ..
                } = &mut self.state
                {
                    *offset -= 1;
                    *seed = seed_leaf_id;
                } else if matches!(self.state, ScanState::Done) {
                    return Err(crate::error::catalog::ER_INTERNAL_ERROR
                        .error("parallel covering leaf slice ended during repair hand-off"));
                }
                rewound = true;
                break;
            }
            for (total, rejected) in runtime_rejections.iter_mut().zip(&leaf_rejections) {
                *total += rejected;
            }
            count += leaf_values.len();
            for value in leaf_values.drain(..) {
                accumulator.push_lossless_i64(value);
            }
        }
        for (filter, rejected) in self.exact_runtime_filters.iter().zip(runtime_rejections) {
            if let Some(counter) = &filter.rejected {
                counter.fetch_add(rejected, std::sync::atomic::Ordering::Relaxed);
            }
        }
        if rewound && count == 0 {
            return self.next_batch();
        }
        Ok((count != 0).then(|| {
            Batch::with_row_count(Arc::clone(&self.schema), vec![accumulator.finish()], count)
        }))
    }

    fn batch_from_rows(
        &self,
        rows: impl IntoIterator<Item = Vec<Value>>,
        candidate_rows: usize,
    ) -> Option<Batch> {
        let rows = rows.into_iter();
        // The fetch target is not a row-count bound: parallel scans append
        // whole leaves, and MRR can change its grant between windows. Vec
        // payloads grow on push, but the fixed-size NULL bitmaps do not.
        // Size every lane against this actual candidate window, using a
        // tighter iterator upper bound when predicate-first already filtered
        // rows. The candidate bound also covers iterators with no size hint.
        let capacity = rows
            .size_hint()
            .1
            .unwrap_or(candidate_rows)
            .min(candidate_rows);
        let mut accumulators = self
            .schema
            .fields()
            .iter()
            .map(|field| ColumnAccumulator::for_field(field, capacity))
            .collect::<Vec<_>>();
        let mut count = 0usize;
        for values in rows {
            for (accumulator, value) in accumulators.iter_mut().zip(values.iter()) {
                accumulator.push(value);
            }
            count += 1;
        }
        (count != 0).then(|| {
            Batch::with_row_count(
                Arc::clone(&self.schema),
                accumulators
                    .into_iter()
                    .map(ColumnAccumulator::finish)
                    .collect(),
                count,
            )
        })
    }
}

/// Partition fan-out for a B-tree source. Each child is pulled while its
/// single-storage-id prune context is installed, so lazy cursor creation,
/// visibility checks and non-covering heap fetches all resolve the same local
/// partition. RowIds therefore never escape the partition where they are
/// meaningful.
pub struct PartitionedIndexBatchStream {
    partitions: std::collections::VecDeque<(u64, bool, IndexBatchStream)>,
    schema: Arc<BatchSchema>,
    properties: BatchProperties,
}

impl PartitionedIndexBatchStream {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: &Arc<Database>,
        txn: TxnView,
        table: &PlanTable,
        index: &IndexSchema,
        access: IndexBatchAccess,
        covering: bool,
        projected_columns: Option<&[usize]>,
    ) -> Result<Option<Self>, QueryError> {
        let storage_ids = table.in_scope_storage_ids();
        let mut partitions = std::collections::VecDeque::with_capacity(storage_ids.len());
        let mut schema = None;
        for storage_id in storage_ids {
            let child = crate::engine::executor::partition_router::with_pruned_partitions(
                vec![storage_id],
                || {
                    IndexBatchStream::new(
                        db,
                        txn,
                        table,
                        index,
                        access.clone(),
                        covering,
                        projected_columns,
                    )
                },
            )?;
            let Some(child) = child else {
                return Ok(None);
            };
            schema.get_or_insert_with(|| child.schema().clone());
            partitions.push_back((storage_id, false, child));
        }
        let schema = schema.unwrap_or_else(|| {
            Arc::new(BatchSchema::new(
                projected_columns
                    .unwrap_or(&[])
                    .iter()
                    .map(|&idx| BatchField {
                        table: table.schema.name.clone(),
                        alias: table.alias.clone(),
                        name: table.schema.columns[idx].name.clone(),
                        column_type: table.schema.result_type(idx),
                        source: table
                            .schema
                            .wire_column_source_with_alias(idx, table.alias.as_deref()),
                    })
                    .collect(),
            ))
        });
        let properties = BatchProperties {
            ordering: if partitions.len() <= 1 {
                partitions
                    .front()
                    .map(|(_, _, child)| child.properties().ordering)
                    .unwrap_or(BatchOrdering::Unknown)
            } else {
                BatchOrdering::Unordered
            },
            partitioning: BatchPartitioning::Single,
        };
        Ok(Some(Self {
            partitions,
            schema,
            properties,
        }))
    }
}

impl BatchOperator for PartitionedIndexBatchStream {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }

    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        let mut installed = false;
        for (_, _, child) in &mut self.partitions {
            installed |= child.install_runtime_filter(filter.clone());
        }
        installed
    }

    fn grow_candidate_batch_rows(&mut self) -> Option<usize> {
        self.partitions
            .iter_mut()
            .filter_map(|(_, _, child)| child.grow_candidate_batch_rows())
            .max()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        loop {
            let Some((storage_id, visited, child)) = self.partitions.front_mut() else {
                return Ok(None);
            };
            if !*visited {
                crate::engine::executor::partition_router::record_partition_visit();
                *visited = true;
            }
            let batch = crate::engine::executor::partition_router::with_pruned_partitions(
                vec![*storage_id],
                || child.next_batch(),
            )?;
            if batch.is_some() {
                return Ok(batch);
            }
            self.partitions.pop_front();
        }
    }
}

impl BatchOperator for IndexBatchStream {
    fn limit_column_prefixes(&mut self, limits: &[(usize, usize)]) {
        if self.heap.is_some() || self.covering || self.predicate_first.is_some() {
            return;
        }
        self.prefix_limits = vec![None; self.schema_cols.len()];
        for &(column, limit) in limits {
            if self.schema.fields().get(column).is_some_and(|field| {
                matches!(
                    field.column_type,
                    crate::types::ColumnType::VarString | crate::types::ColumnType::Blob
                )
            }) {
                self.prefix_limits[column] = Some(limit);
            }
        }
    }
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }

    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        self.install_exact_runtime_filter(filter)
    }

    fn install_runtime_index_bound(&mut self, predicate: &Expr) -> bool {
        let Some(bound) = self.materialized_first_key_lower_bound(predicate) else {
            return false;
        };
        let ScanState::Uninitialized(IndexBatchAccess::Range { low, .. }) = &mut self.state else {
            return false;
        };
        let replaceable = low.as_ref().is_none_or(|existing| {
            existing.key.0.len() == 1 && matches!(existing.key.0.first(), Some(KeyPart::Null))
        });
        if !replaceable {
            return false;
        }
        *low = Some(bound);
        true
    }

    fn grow_candidate_batch_rows(&mut self) -> Option<usize> {
        self.grow_row_goal_window()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        self.initialize()?;
        if self.covering {
            if self.raw_i64_key_position.is_some()
                && self
                    .exact_runtime_filters
                    .iter()
                    .all(|filter| Some(filter.key_position) == self.raw_i64_key_position)
                && self.seen.is_none()
                && matches!(self.state, ScanState::LeafRange { .. } | ScanState::Done)
                && self.leaf_repairs.is_none()
                && !(self.repair_scope.is_some()
                    && self
                        .db
                        .has_pending_index_repairs_for(self.table.schema.id, &self.index.name))
            {
                return self.next_parallel_covering_i64_batch();
            }
            let mut accumulators = self
                .schema
                .fields()
                .iter()
                .map(|field| ColumnAccumulator::for_field(field, self.batch_rows))
                .collect::<Vec<_>>();
            let mut count = 0usize;
            let mut runtime_rejections = vec![0u64; self.exact_runtime_filters.len()];
            let raw_filter = self
                .exact_runtime_filters
                .iter()
                .enumerate()
                .find(|(_, filter)| filter.key_position == 0)
                .map(|(index, filter)| (index, Arc::clone(&filter.filter)));
            while count < self.batch_rows {
                let remaining = self.batch_rows - count;
                let (repairs_before_read, epoch_before_read) = self
                    .db
                    .index_repair_activity(self.table.schema.id, &self.index.name);
                let Some((
                    mut entries,
                    page_max_trx_id,
                    entries_examined,
                    raw_rejected,
                    raw_filter_index,
                )) = self.next_covering_entries(remaining, raw_filter.clone())?
                else {
                    break;
                };
                if let Some(filter_index) = raw_filter_index {
                    runtime_rejections[filter_index] =
                        runtime_rejections[filter_index].saturating_add(raw_rejected as u64);
                }
                if let Some(seen) = self.seen.as_mut() {
                    entries.retain(|entry| seen.insert(entry.1));
                }
                crate::engine::executor::select::analyze_stats::add_range_scan_work(
                    entries_examined as u64,
                    0,
                    0,
                );
                self.record_candidate_batch(entries_examined);
                if entries.is_empty() {
                    continue;
                }
                let heap = self.heap()?.clone();
                let mut mask = if repairs_before_read
                    && self
                        .db
                        .index_repair_rollback_epoch(self.table.schema.id, &self.index.name)
                        != epoch_before_read
                {
                    None
                } else {
                    page_max_trx_id
                        .and_then(|bound| {
                            self.db.try_index_page_max_trx_shortcut_for(
                                self.table.schema.id,
                                &self.index.name,
                                bound,
                                self.txn,
                                entries.len(),
                            )
                        })
                        .map(Ok::<_, QueryError>)
                        .unwrap_or_else(|| {
                            self.db.filter_visible_index_entries_mask_with_handle(
                                &heap,
                                &self.table.schema,
                                &self.index,
                                &entries,
                                self.txn,
                            )
                        })
                        .map(Some)?
                };
                // A latest heap version that is visible proves an entry only
                // while the entry belongs to that version. A writer that
                // overlapped this read may have left the chunk holding its
                // newer key while a rollback restored the old visible version.
                if mask.is_none()
                    || self
                        .db
                        .index_repair_rollback_epoch(self.table.schema.id, &self.index.name)
                        != epoch_before_read
                {
                    mask = Some(self.db.filter_visible_index_entries_mask_exact(
                        &self.table.schema,
                        &self.index,
                        &entries,
                        self.txn,
                    )?);
                }
                let mask = mask.expect("visibility mask computed");
                let projector = self.projector.as_ref().expect("covering projector");
                for ((key, _), visible) in entries.iter().zip(mask) {
                    if !visible {
                        continue;
                    }
                    let mut admitted = true;
                    for (filter_index, filter) in self.exact_runtime_filters.iter().enumerate() {
                        if Some(filter_index) == raw_filter_index {
                            continue;
                        }
                        if !filter.admits(key) {
                            runtime_rejections[filter_index] =
                                runtime_rejections[filter_index].saturating_add(1);
                            admitted = false;
                            break;
                        }
                    }
                    if admitted {
                        projector.append_to_accumulators(key, &mut accumulators);
                        count += 1;
                    }
                }
            }
            for (filter, rejected) in self
                .exact_runtime_filters
                .iter()
                .zip(runtime_rejections.into_iter())
            {
                if rejected != 0 {
                    if let Some(counter) = &filter.rejected {
                        counter.fetch_add(rejected, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
            return Ok((count != 0).then(|| {
                Batch::with_row_count(
                    Arc::clone(&self.schema),
                    accumulators
                        .into_iter()
                        .map(ColumnAccumulator::finish)
                        .collect(),
                    count,
                )
            }));
        }

        while let Some(mut row_ids) = self.next_row_ids()? {
            self.record_candidate_batch(row_ids.len());
            let entries_examined = if self.count_rowids_as_index_entries {
                row_ids.len() as u64
            } else {
                0
            };
            if let Some(ranges) = self
                .predicate_first
                .as_ref()
                .and_then(|predicate| predicate.candidate_heap_ranges.as_deref())
            {
                row_ids.retain(|row_id| {
                    let page = u64::from(row_id.page_id);
                    let position = ranges.partition_point(|(_, high)| *high <= page);
                    ranges
                        .get(position)
                        .is_some_and(|(low, high)| *low <= page && page < *high)
                });
                if row_ids.is_empty() {
                    crate::engine::executor::select::analyze_stats::add_range_scan_work(
                        entries_examined,
                        0,
                        0,
                    );
                    let _ = self.grow_row_goal_window();
                    continue;
                }
            }
            // Non-covering native index scans use the same page-local,
            // bounded RowId window as the row oracle. Publish it through the
            // common MRR counters so EXPLAIN ANALYZE does not lose operator
            // diagnostics merely because the transport is columnar.
            crate::engine::executor::select::plan::record_mrr_window(&row_ids);
            let heap_page_probes = if row_ids.len() <= 64 {
                let mut pages = smallvec::SmallVec::<[u32; 64]>::new();
                for page_id in row_ids.iter().map(|row_id| row_id.page_id) {
                    if !pages.contains(&page_id) {
                        pages.push(page_id);
                    }
                }
                pages.len() as u64
            } else {
                let mut pages =
                    hashbrown::HashSet::<u32, ahash::RandomState>::with_capacity_and_hasher(
                        row_ids.len().min(256),
                        ahash::RandomState::new(),
                    );
                pages.extend(row_ids.iter().map(|row_id| row_id.page_id));
                pages.len() as u64
            };
            let heap = self.heap()?.clone();

            if let Some(predicate_first) = self.predicate_first.as_ref() {
                // The storage primitive groups probes by heap page and
                // therefore requires monotonically ordered RowIds. Primary
                // index scans already have that shape; secondary indexes use
                // one bounded sorted scratch copy. Output is restored to the
                // immutable index order below, exactly like the ordinary MRR
                // lane.
                let sorted_row_ids: Vec<RowId>;
                let _sorted_row_ids_charge;
                let row_ids_are_ordered = row_ids.windows(2).all(|pair| pair[0] <= pair[1]);
                let read_row_ids = if row_ids_are_ordered {
                    row_ids.as_slice()
                } else {
                    _sorted_row_ids_charge =
                        crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                            row_ids
                                .len()
                                .saturating_mul(std::mem::size_of::<RowId>()),
                        )?;
                    sorted_row_ids = {
                        let mut sorted = row_ids.clone();
                        sorted.sort_unstable();
                        sorted.dedup();
                        sorted
                    };
                    sorted_row_ids.as_slice()
                };
                let fetched = if let Some(decoded) = predicate_first.decoded.as_ref() {
                    let mut results = vec![None; decoded.leaf_count()];
                    self.db
                        .read_row_columns_many_predicate_first_payload_with_handle(
                            &heap,
                            &self.table.schema,
                            read_row_ids,
                            self.txn,
                            &predicate_first.columns,
                            &self.schema_cols,
                            |hit| match hit {
                                crate::storage::PayloadStreamHit::Payload(payload) => {
                                    let mut sink = crate::engine::executor::select::parallel::DecodedPredicateSink::new(
                                        decoded,
                                        &mut results,
                                    );
                                    let fast = crate::storage::decode_row_into_sink_fast(
                                        payload.payload,
                                        payload.codec,
                                        payload.layout_id,
                                        payload.plan,
                                        &mut sink,
                                        payload.has_external,
                                    )?;
                                    Ok(fast.then(|| sink.finish()).flatten())
                                }
                                crate::storage::PayloadStreamHit::Values(_) => Ok(None),
                            },
                        )?
                } else {
                    None
                }
                .map(Ok)
                .unwrap_or_else(|| {
                    self.db.read_row_columns_many_predicate_first_with_handle(
                        &heap,
                        &self.table.schema,
                        read_row_ids,
                        self.txn,
                        &predicate_first.columns,
                        &self.schema_cols,
                        |values| predicate_first.compiled.eval(values),
                    )
                })?;
                crate::engine::executor::select::analyze_stats::add_pending_expression_rows(
                    row_ids.len() as u64,
                    0,
                );
                crate::engine::executor::select::analyze_stats::add_range_scan_work(
                    entries_examined,
                    row_ids.len() as u64,
                    heap_page_probes,
                );
                let batch = if row_ids_are_ordered {
                    // Predicate-first visits sorted RowIds in input order.
                    // The overwhelmingly common ordered PRIMARY scan can
                    // therefore feed accepted values directly into the batch
                    // without hashing every survivor merely to restore the
                    // order it already has.
                    self.batch_from_rows(
                        fetched.into_iter().map(|(_, values)| values),
                        row_ids.len(),
                    )
                } else {
                    let mut by_id: HashMap<RowId, Vec<Value>> = fetched.into_iter().collect();
                    self.batch_from_rows(
                        row_ids.iter().filter_map(|row_id| by_id.remove(row_id)),
                        row_ids.len(),
                    )
                };
                if let Some(batch) = batch {
                    return Ok(Some(batch));
                }
                // Predicate-first owns the residual, so there is no outer
                // FilterBatchStream to provide empty-batch feedback. Grow the
                // same bounded row-goal window here to avoid one-RID probes
                // through long rejected runs.
                let _ = self.grow_row_goal_window();
                continue;
            }

            // The ordinary row API materialises `Vec<Value>` for every heap
            // hit, builds a RowId hash map to restore index order, then
            // transposes those values into typed columns. Resident current-
            // layout rows can preserve the same order by decoding directly
            // into the column accumulators. Any cold/forwarded/historical or
            // old-layout row rejects the whole attempt; discard these lanes
            // and let the established row oracle handle the complete window.
            if row_ids.len() >= DIRECT_HEAP_BATCH_MIN_ROWS {
                let mut direct_accumulators = self
                    .schema
                    .fields()
                    .iter()
                    .map(|field| ColumnAccumulator::for_field(field, row_ids.len()))
                    .collect::<Vec<_>>();
                let direct_rows = self.db.read_row_columns_many_into_sink(
                    &heap,
                    &row_ids,
                    self.txn,
                    &self.schema_cols,
                    &mut BatchAccumSink::new(&mut direct_accumulators)
                        .with_prefix_limits(&self.prefix_limits),
                )?;
                if let Some(count) = direct_rows {
                    #[cfg(test)]
                    DIRECT_HEAP_BATCH_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    crate::engine::executor::select::analyze_stats::add_range_scan_work(
                        entries_examined,
                        count as u64,
                        heap_page_probes,
                    );
                    if count != 0 {
                        return Ok(Some(Batch::with_row_count(
                            Arc::clone(&self.schema),
                            direct_accumulators
                                .into_iter()
                                .map(ColumnAccumulator::finish_validated)
                                .collect::<Result<Vec<_>, _>>()?,
                            count,
                        )));
                    }
                    continue;
                }
                #[cfg(test)]
                DIRECT_HEAP_BATCH_FALLBACKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            let fetched = self.db.read_row_columns_many_with_handle(
                &heap,
                &self.table.schema,
                &row_ids,
                self.txn,
                &self.schema_cols,
            )?;
            crate::engine::executor::select::analyze_stats::add_range_scan_work(
                entries_examined,
                fetched.len() as u64,
                heap_page_probes,
            );
            let mut by_id: HashMap<RowId, Vec<Value>> = fetched.into_iter().collect();
            let rows = row_ids.iter().filter_map(|row_id| by_id.remove(row_id));
            if let Some(batch) = self.batch_from_rows(rows, row_ids.len()) {
                return Ok(Some(batch));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::api::QueryResponse;
    use crate::engine::executor::StorageQueryEngine;
    use crate::storage::{DurabilityMode, IsolationLevel, StorageConfig};

    #[test]
    fn parallel_raw_integer_runtime_filters_preserve_ranges_nulls_and_snapshots() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            StorageConfig {
                durability: DurabilityMode::Memory,
                ..StorageConfig::from_env()
            },
        )
        .unwrap();
        engine
            .execute("CREATE TABLE integer_filter (id INT PRIMARY KEY, k BIGINT, INDEX ik(k))")
            .unwrap();
        engine.execute("INSERT INTO integer_filter SELECT n, CASE WHEN MOD(n,11)=0 THEN NULL ELSE MOD(n,301)-150 END FROM generate_series(1,12000) gs(n)").unwrap();
        let db = engine.db_for_tests();
        let snapshot = db.visible_scn();
        let _snapshot = db.register_statement_snapshot(snapshot);
        let txn = TxnView::read_only(snapshot, IsolationLevel::RepeatableRead);
        let schema = db.snapshot_table_schema("test.integer_filter").unwrap();
        let index = schema
            .indexes
            .iter()
            .find(|index| index.name == "ik")
            .unwrap()
            .clone();
        let key_column = schema.column_index("k").unwrap();
        let table = PlanTable::new(schema.id, schema, None);
        let low = Some(IndexBound {
            key: IndexKey::from(vec![KeyPart::Int(-100)]),
            inclusive: false,
        });
        let high = Some(IndexBound {
            key: IndexKey::from(vec![KeyPart::Int(100)]),
            inclusive: true,
        });
        for mutate in [false, true] {
            if mutate {
                engine
                    .execute("UPDATE integer_filter SET k=999 WHERE MOD(id,3)=0")
                    .unwrap();
                engine
                    .execute("DELETE FROM integer_filter WHERE MOD(id,5)=0")
                    .unwrap();
            }
            for keys in [vec![], vec![-100, -99, 0, 100, 101], vec![999]] {
                let parallel = IndexBatchStream::parallel_range_workers(
                    db,
                    txn,
                    &table,
                    &index,
                    low.clone(),
                    high.clone(),
                    true,
                    Some(&[key_column]),
                    4,
                )
                .unwrap();
                let workers = if mutate {
                    assert!(
                        parallel.is_none(),
                        "pending repairs must decline parallel construction"
                    );
                    vec![IndexBatchStream::new(
                        db,
                        txn,
                        &table,
                        &index,
                        IndexBatchAccess::Range {
                            low: low.clone(),
                            high: high.clone(),
                            reverse: false,
                        },
                        true,
                        Some(&[key_column]),
                    )
                    .unwrap()
                    .unwrap()]
                } else {
                    let workers = parallel.expect("fixture must span multiple leaves");
                    assert!(workers.len() > 1);
                    workers
                };
                let mut builder =
                    crate::engine::executor::select::bloom::ExactIntKeySetBuilder::with_capacity(
                        keys.len(),
                    )
                    .unwrap();
                for key in &keys {
                    builder.insert(i128::from(*key));
                }
                let filter = Arc::new(builder.finish().unwrap());
                let mut actual = Vec::new();
                for mut worker in workers {
                    assert_eq!(worker.raw_i64_key_position, Some(0));
                    assert!(worker.install_runtime_filter(
                        crate::planner::RuntimeFilter::ExactIntKey {
                            keys: vec![Expr::Identifier("k".into())],
                            null_safe: vec![false],
                            filter: Arc::clone(&filter),
                            rejected: Arc::new(std::sync::OnceLock::new()),
                        }
                    ));
                    // Keep the native lane below one leaf; the repair oracle
                    // uses bounded batches rather than re-sorting its directory per row.
                    worker.batch_rows = if mutate { 128 } else { 1 };
                    while let Some(batch) = worker.next_batch().unwrap() {
                        for row in batch.visible_indices() {
                            let Value::Int(value) = batch.columns()[0].value_at(row) else {
                                panic!("integer output required")
                            };
                            actual.push(value);
                        }
                    }
                }
                let mut expected: Vec<_> = (1..=12000)
                    .filter_map(|id| {
                        let key = id % 301 - 150;
                        (id % 11 != 0 && key > -100 && key <= 100 && keys.contains(&key))
                            .then_some(key)
                    })
                    .collect();
                actual.sort_unstable();
                expected.sort_unstable();
                assert_eq!(actual, expected, "keys={keys:?}, mutate={mutate}");
            }
        }
    }

    #[test]
    fn row_fallback_null_masks_cover_rows_beyond_scan_target() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            StorageConfig {
                durability: DurabilityMode::Memory,
                ..StorageConfig::from_env()
            },
        )
        .unwrap();
        engine.execute("CREATE TABLE batch_capacity (id INT PRIMARY KEY, n INT, u BIGINT UNSIGNED, f DOUBLE, d DECIMAL(12,2), wide DECIMAL(30,2), day DATE, tm TIME, dt DATETIME, ts TIMESTAMP NULL, txt TEXT, raw BLOB, doc JSON)").unwrap();
        engine.execute("INSERT INTO batch_capacity VALUES (1,7,18446744073709551615,1.5,12.34,12345678901234567890.12,'2026-09-11','01:02:03','2026-09-11 01:02:03','2026-09-11 01:02:03','中🙂',X'FF00',JSON_OBJECT('x',1))").unwrap();
        let db = engine.db_for_tests();
        let schema = db.snapshot_table_schema("test.batch_capacity").unwrap();
        let index = schema
            .indexes
            .iter()
            .find(|index| index.primary)
            .unwrap()
            .clone();
        let table = PlanTable::new(schema.id, schema, None);
        let txn = TxnView::read_only(db.visible_scn(), IsolationLevel::ReadCommitted);
        let mut stream = IndexBatchStream::new(
            db,
            txn,
            &table,
            &index,
            IndexBatchAccess::Range {
                low: None,
                high: None,
                reverse: false,
            },
            false,
            None,
        )
        .unwrap()
        .unwrap();
        // Feed storage Values, before the SQL result boundary formats
        // temporal values for the client.
        let source = db.scan_rows("test.batch_capacity", txn).unwrap();
        let source = &source[0].values;
        let rows: Vec<_> = (0..130)
            .map(|row| {
                source
                    .iter()
                    .enumerate()
                    .map(|(col, value)| {
                        if row % 3 == 1 {
                            Value::Null
                        } else if col == 0 {
                            Value::Int(row)
                        } else {
                            value.clone()
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        // MRR fetches whole index leaves and can change its runtime grant.
        // Its target is not an upper bound on the actual fallback window.
        for target in [1, 63, 64, 65] {
            stream.batch_rows = target;
            let mut iter = rows.clone().into_iter();
            for batch in [
                stream.batch_from_rows(rows.clone(), rows.len()).unwrap(),
                stream
                    .batch_from_rows(std::iter::from_fn(|| iter.next()), rows.len())
                    .unwrap(),
            ] {
                assert_eq!(batch.num_rows(), rows.len());
                for (column, vector) in batch.columns().iter().enumerate() {
                    assert_eq!(vector.len(), rows.len());
                    assert_eq!(vector.nulls().unwrap().len(), rows.len());
                    assert_eq!(vector.nulls().unwrap().count_ones(), 87);
                    for (row, expected) in rows.iter().enumerate() {
                        assert_eq!(vector.is_null(row), matches!(expected[column], Value::Null));
                        assert_eq!(
                            vector.value_at(row).to_display_string(),
                            expected[column].to_display_string()
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn parallel_leaf_window_snapshot_fallback_exceeds_target_without_losing_rows() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            StorageConfig {
                durability: DurabilityMode::Memory,
                ..StorageConfig::from_env()
            },
        )
        .unwrap();
        engine
            .execute("CREATE TABLE leaf_capacity (id VARCHAR(128) PRIMARY KEY, n INT)")
            .unwrap();
        engine.execute("INSERT INTO leaf_capacity SELECT CONCAT(LPAD(n,4,'0'),REPEAT('x',96)), CASE WHEN MOD(n,3)=1 THEN NULL ELSE n END FROM generate_series(0,511) gs(n)").unwrap();
        let QueryResponse::ResultSet(expected) = engine
            .execute("SELECT id,n FROM leaf_capacity ORDER BY id")
            .unwrap()
        else {
            panic!("expected rows")
        };
        let db = engine.db_for_tests();
        let snapshot = db.visible_scn();
        let _snapshot = db.register_statement_snapshot(snapshot);
        let txn = TxnView::read_only(snapshot, IsolationLevel::RepeatableRead);
        // Current versions are invisible to the old snapshot. This makes
        // each direct sink decline and exercises the real heap/undo fallback.
        engine.execute("UPDATE leaf_capacity SET n=99").unwrap();
        let schema = db.snapshot_table_schema("test.leaf_capacity").unwrap();
        let index = schema
            .indexes
            .iter()
            .find(|index| index.primary)
            .unwrap()
            .clone();
        let table = PlanTable::new(schema.id, schema, None);
        let workers = IndexBatchStream::parallel_range_workers(
            db, txn, &table, &index, None, None, false, None, 4,
        )
        .unwrap()
        .expect("fixture must span multiple index leaves");
        assert!(workers.len() > 1);
        let fallback_before =
            DIRECT_HEAP_BATCH_FALLBACKS.load(std::sync::atomic::Ordering::Relaxed);
        let mut actual = std::thread::scope(|scope| {
            let handles: Vec<_> = workers
                .into_iter()
                .map(|mut worker| {
                    scope.spawn(move || {
                        // Simulate a small initial MRR target independently of test
                        // machine memory; whole-leaf windows still contain >1 row.
                        worker.batch_rows = 1;
                        let mut out = Vec::new();
                        let mut largest_batch = 0;
                        while let Some(batch) = worker.next_batch().unwrap() {
                            largest_batch = largest_batch.max(batch.num_rows());
                            for row in batch.visible_indices() {
                                out.push(
                                    batch
                                        .columns()
                                        .iter()
                                        .map(|column| column.value_at(row))
                                        .collect::<Vec<_>>(),
                                );
                            }
                        }
                        assert!(largest_batch > 1);
                        out
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(
            DIRECT_HEAP_BATCH_FALLBACKS.load(std::sync::atomic::Ordering::Relaxed)
                > fallback_before
        );
        actual.sort_by_key(|row| row[0].to_display_string());
        assert_eq!(actual, expected.rows);
    }
}
