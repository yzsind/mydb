//! Columnar storage of materialized CTE results + sharing across references (P0-A, P0-B).
//!
//! `CteStore` replaces the old `CteResult.rows: Vec<Vec<Value>>`:
//!   - **P0-A1**: multiple references share the same `Arc<CteStore>`, avoiding deep copies of the row set.
//!   - **P0-A2**: `SourceRows::SharedCteSelected { indices }` uses a selection
//!     view instead of fully deep-copying the predicate-pushed `Owned(kept)` (in `select::mod.rs`).
//!   - **P0-B**: `CteStore.builds` actually lands as
//!     `Mutex<HashMap<Vec<usize>, Arc<CachedHashBuild>>>`, reusing hash join
//!     build buckets across references. The key is the positions of the right-key columns in the CTE column list, alias-agnostic →
//!     the `year_total t_s_firstyear/t_s_secyear/...` 4-way self-join shares one bucket set.
//!
//! Native column batches stay columnar across materialization and repeated scans.
//! Row consumers obtain a separately admitted compatibility view on demand.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::engine::executor::mem_budget::{self, ControlledMemoryCategory, ControlledMemoryCharge};
use crate::engine::executor::memory_footprint::AllocatedBytes;
use crate::engine::executor::vec::batch::{Batch, ColumnVector, DEFAULT_BATCH_ROWS};
use crate::engine::expr::ValueKey;
use crate::types::{Column, ColumnType, QueryError, Value};

/// Physical representation of the materialized result.
#[derive(Debug)]
pub enum CteColumnar {
    /// Backing produced by scalar execution paths.
    RowMajor(Vec<Vec<Value>>),
    Batches {
        chunks: Arc<Vec<Batch>>,
        rows: usize,
        ends: Vec<usize>,
    },
}

impl CteColumnar {
    #[inline]
    pub fn nrows(&self) -> usize {
        match self {
            CteColumnar::RowMajor(r) => r.len(),
            CteColumnar::Batches { rows, .. } => *rows,
        }
    }

    /// Borrow scalar storage without forcing a column-to-row compatibility view.
    #[inline]
    pub fn row_major_rows(&self) -> Option<&[Vec<Value>]> {
        match self {
            CteColumnar::RowMajor(r) => Some(r.as_slice()),
            CteColumnar::Batches { .. } => None,
        }
    }
}

/// Columnar storage of materialized CTE results. Multiple references share one `Arc<CteStore>`;
/// predicate pushdown avoids copies via a shared selection view; hash builds are reused across
/// references through `CteHashBuildCache`.
#[derive(Debug)]
pub struct CteStore {
    pub schema: Arc<Vec<Column>>,
    pub columnar: CteColumnar,
    /// Cross-reference shared hash build tables.
    pub builds: CteHashBuildCache,
    /// Columnar-form cache: all columns, `ColumnVector`s chunked by `DEFAULT_BATCH_ROWS`.
    /// Lazily built by the first vec consumer and shared across references via the `Arc<CteStore>` —
    /// the row→column transpose + lane finalization of a multi-reference CTE is done once, not once per reference.
    col_chunks: OnceLock<Arc<Vec<Vec<ColumnVector>>>>,
    /// Unified-governor reservation for the resident buffers. The
    /// charge lives in the shared store so multi-reference and session-cache
    /// owners release it exactly when the final `Arc<CteStore>` is dropped.
    _memory_charge: Option<Arc<Mutex<ControlledMemoryCharge>>>,
    row_memory_bytes: usize,
    row_mirror: OnceLock<ResidentRows>,
    row_mirror_build: Mutex<()>,
}

#[derive(Debug)]
struct ResidentRows {
    rows: Vec<Vec<Value>>,
    charge: ControlledMemoryCharge,
}

impl CteStore {
    /// Wraps an existing `Vec<Vec<Value>>` into a store. All construction sites at the skeleton stage
    /// (including `CteResult::materialized` and recursive backfill) uniformly go through this path.
    pub fn from_dense_rows(schema: Vec<Column>, rows: Vec<Vec<Value>>) -> Arc<Self> {
        Arc::new(Self {
            schema: Arc::new(schema),
            columnar: CteColumnar::RowMajor(rows),
            builds: CteHashBuildCache::new(),
            col_chunks: OnceLock::new(),
            _memory_charge: None,
            row_memory_bytes: 0,
            row_mirror: OnceLock::new(),
            row_mirror_build: Mutex::new(()),
        })
    }

    pub(crate) fn from_dense_rows_charged(
        schema: Vec<Column>,
        rows: Vec<Vec<Value>>,
        memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    ) -> Arc<Self> {
        let row_memory_bytes = memory_charge.bytes();
        Arc::new(Self {
            schema: Arc::new(schema),
            columnar: CteColumnar::RowMajor(rows),
            builds: CteHashBuildCache::new(),
            col_chunks: OnceLock::new(),
            _memory_charge: Some(Arc::new(Mutex::new(memory_charge))),
            row_memory_bytes,
            row_mirror: OnceLock::new(),
            row_mirror_build: Mutex::new(()),
        })
    }

    /// Spilled form: the store holds no rows in memory; reads go through `CteSpillFile`.
    /// Constructs an empty skeleton to preserve the single-owner `Arc<CteStore>` convention.
    pub fn empty(schema: Vec<Column>) -> Arc<Self> {
        Arc::new(Self {
            schema: Arc::new(schema),
            columnar: CteColumnar::RowMajor(Vec::new()),
            builds: CteHashBuildCache::new(),
            col_chunks: OnceLock::new(),
            _memory_charge: None,
            row_memory_bytes: 0,
            row_mirror: OnceLock::new(),
            row_mirror_build: Mutex::new(()),
        })
    }

    pub(crate) fn from_batches_charged(
        schema: Vec<Column>,
        mut batches: Vec<Batch>,
        charge: ControlledMemoryCharge,
    ) -> Result<Arc<Self>, QueryError> {
        let owner = Arc::new(Mutex::new(charge));
        let mut rows = 0usize;
        let mut ends =
            mem_budget::try_vec_with_capacity(batches.len(), "resident CTE batch offsets")?;
        for batch in &batches {
            rows = rows
                .checked_add(batch.visible_rows())
                .ok_or_else(|| mem_budget::allocation_error("resident CTE row count overflow"))?;
            ends.push(rows);
        }
        let finalize = (|| {
            for batch in &mut batches {
                crate::engine::cancel::check_cancelled()?;
                if batch.selection().is_some() {
                    return Err(crate::error::catalog::ER_INTERNAL_ERROR
                        .error("resident CTE batch was not compacted"));
                }
                batch.set_resident_owner(owner.clone())?;
            }
            Ok::<_, QueryError>(())
        })();
        if let Err(error) = finalize {
            drop(batches);
            return Err(error);
        }
        Ok(Arc::new(Self {
            schema: Arc::new(schema),
            columnar: CteColumnar::Batches {
                chunks: Arc::new(batches),
                rows,
                ends,
            },
            builds: CteHashBuildCache::new(),
            col_chunks: OnceLock::new(),
            _memory_charge: Some(owner),
            row_memory_bytes: 0,
            row_mirror: OnceLock::new(),
            row_mirror_build: Mutex::new(()),
        }))
    }

    pub(crate) fn native_batches(&self) -> Option<Arc<Vec<Batch>>> {
        match &self.columnar {
            CteColumnar::Batches { chunks, .. } => Some(chunks.clone()),
            _ => None,
        }
    }

    pub(crate) fn is_native(&self) -> bool {
        matches!(&self.columnar, CteColumnar::Batches { .. })
    }

    pub(crate) fn copy_row(&self, index: usize) -> Result<Option<Vec<Value>>, QueryError> {
        if let Some(rows) = self.row_major_rows() {
            return rows
                .get(index)
                .map(|row| {
                    row.iter()
                        .map(|value| mem_budget::try_clone_value(value, "CTE row value"))
                        .collect()
                })
                .transpose();
        }
        let CteColumnar::Batches { chunks, rows, ends } = &self.columnar else {
            unreachable!()
        };
        if index >= *rows {
            return Ok(None);
        }
        let batch_index = ends.partition_point(|end| *end <= index);
        let start = if batch_index == 0 {
            0
        } else {
            ends[batch_index - 1]
        };
        let batch = &chunks[batch_index];
        let mut row =
            mem_budget::try_vec_with_capacity(self.schema.len(), "resident CTE compatibility row")?;
        for (column, field) in batch.columns().iter().zip(self.schema.iter()) {
            let value = column.try_value_at(index - start)?;
            row.push(match (field.column_type, value) {
                (ColumnType::Float, Value::Double(value)) => Value::Float(value as f32),
                (_, value) => value,
            });
        }
        Ok(Some(row))
    }

    #[inline]
    pub fn nrows(&self) -> usize {
        self.columnar.nrows()
    }

    #[inline]
    pub fn row_major_rows(&self) -> Option<&[Vec<Value>]> {
        self.columnar.row_major_rows()
    }

    /// Only row consumers request this compatibility view. Reserve it before
    /// cloning and propagate admission failure instead of hiding a row OOM.
    pub fn dense_rows(&self) -> Result<&[Vec<Value>], QueryError> {
        if let Some(rows) = self.row_major_rows() {
            return Ok(rows);
        }
        if let Some(mirror) = self.row_mirror.get() {
            return Ok(&mirror.rows);
        }
        let _build = self.row_mirror_build.lock().map_err(|_| {
            crate::error::catalog::ER_INTERNAL_ERROR
                .error("CTE row view initialization lock was poisoned")
        })?;
        crate::engine::cancel::check_cancelled()?;
        if let Some(mirror) = self.row_mirror.get() {
            return Ok(&mirror.rows);
        }
        let CteColumnar::Batches {
            chunks,
            rows: count,
            ..
        } = &self.columnar
        else {
            unreachable!()
        };
        let mut bytes = count
            .saturating_mul(std::mem::size_of::<Vec<Value>>())
            .saturating_add(128);
        for batch in chunks.iter() {
            crate::engine::cancel::check_cancelled()?;
            for row in batch.visible_indices() {
                bytes = bytes.saturating_add(
                    batch
                        .columns()
                        .iter()
                        .map(|column| column.owned_value_bytes(row))
                        .sum::<usize>(),
                );
            }
        }
        if mem_budget::current_budget().is_some_and(|budget| bytes > budget.limit()) {
            return Err(crate::error::catalog::ER_OUTOFMEMORY
                .error("CTE compatibility row view exceeds the query work-area limit"));
        }
        let mut charge =
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::DerivedCache, bytes)?;
        let mut rows = mem_budget::try_vec_with_capacity(*count, "CTE compatibility rows")?;
        for batch in chunks.iter() {
            for index in batch.visible_indices() {
                crate::engine::cancel::check_cancelled()?;
                let mut row =
                    mem_budget::try_vec_with_capacity(self.schema.len(), "CTE compatibility row")?;
                for (column, field) in batch.columns().iter().zip(self.schema.iter()) {
                    let value = column.try_value_at(index)?;
                    row.push(match (field.column_type, value) {
                        (ColumnType::Float, Value::Double(value)) => Value::Float(value as f32),
                        (_, value) => value,
                    });
                }
                rows.push(row);
            }
        }
        let actual = rows.allocated_bytes();
        if actual > charge.bytes() {
            charge.try_grow(actual - charge.bytes())?;
        } else {
            charge.shrink(charge.bytes() - actual);
        }
        let _ = self.row_mirror.set(ResidentRows { rows, charge });
        Ok(&self
            .row_mirror
            .get()
            .expect("CTE row view installed under lock")
            .rows)
    }

    pub(crate) fn resident_memory_bytes(&self) -> Option<usize> {
        self._memory_charge.as_ref().and_then(|charge| {
            charge.lock().ok().map(|charge| {
                charge
                    .bytes()
                    .saturating_add(self.row_mirror.get().map_or(0, |rows| rows.charge.bytes()))
            })
        })
    }

    /// All-column columnar form chunked by `DEFAULT_BATCH_ROWS` (lazy build + cache + Arc sharing).
    /// **Bit-for-bit equivalent** to per-consumption `ColumnVector::from_values` over per-column value
    /// sequences (same source of truth), so the cached path and the direct-build path yield identical
    /// batches — a multi-reference CTE self-join (e.g. q4's 4-6-way year_total) pays the transpose once,
    /// and each reference clones the columns its projection needs (a memcpy of finalized buffers, far cheaper than per-cell `Value` clones + re-finalization).
    pub fn columnar_chunks(&self) -> Option<Arc<Vec<Vec<ColumnVector>>>> {
        if let Some(chunks) = self.col_chunks.get() {
            return Some(chunks.clone());
        }
        let charge_mutex = self._memory_charge.as_ref()?;
        let rows = self.row_major_rows()?;
        let ncols = self.schema.len();
        let nchunks = rows.len().div_ceil(DEFAULT_BATCH_ROWS);
        // Reserve before allocating both the completed typed mirror and the
        // transient per-column Value clone used by `from_values`.  Each is
        // conservatively bounded by the charged row-major capacities; add
        // the chunk/column container metadata explicitly.  Once construction
        // finishes, return the difference from the actual capacities.
        let reservation_bytes = self
            .row_memory_bytes
            .saturating_mul(2)
            .saturating_add(nchunks.saturating_mul(std::mem::size_of::<Vec<ColumnVector>>()))
            .saturating_add(
                nchunks
                    .saturating_mul(ncols)
                    .saturating_mul(std::mem::size_of::<ColumnVector>()),
            );
        {
            let mut charge = charge_mutex.lock().ok()?;
            if !charge.try_grow_optional(reservation_bytes) {
                return None;
            }
        }
        let mut chunks: Vec<Vec<ColumnVector>> = Vec::with_capacity(nchunks);
        for chunk in rows.chunks(DEFAULT_BATCH_ROWS) {
            let cols: Vec<ColumnVector> = (0..ncols)
                .map(|c| {
                    let vals: Vec<Value> = chunk
                        .iter()
                        .map(|r| r.get(c).cloned().unwrap_or(Value::Null))
                        .collect();
                    ColumnVector::from_values(vals)
                })
                .collect();
            chunks.push(cols);
        }
        let actual_bytes = chunks
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<ColumnVector>>())
            .saturating_add(chunks.iter().fold(0usize, |total, columns| {
                total
                    .saturating_add(
                        columns
                            .capacity()
                            .saturating_mul(std::mem::size_of::<ColumnVector>()),
                    )
                    .saturating_add(columns.iter().fold(0usize, |bytes, column| {
                        bytes.saturating_add(column.memory_bytes())
                    }))
            }));
        let mut charge = charge_mutex.lock().ok()?;
        if actual_bytes > reservation_bytes {
            // The conservative bound should cover every current lane.  Keep
            // this path correct if a future lane has a larger representation.
            if !charge.try_grow_optional(actual_bytes - reservation_bytes) {
                charge.shrink(reservation_bytes);
                return None;
            }
        } else {
            charge.shrink(reservation_bytes - actual_bytes);
        }
        drop(charge);
        let chunks = Arc::new(chunks);
        if let Err(unused) = self.col_chunks.set(chunks.clone()) {
            if let Ok(mut charge) = charge_mutex.lock() {
                charge.shrink(actual_bytes);
            }
            drop(unused);
            return self.col_chunks.get().cloned();
        }
        Some(chunks)
    }
}

pub type CteHandle = Arc<CteStore>;

/// Hash build buckets shared across references.
///
/// The key is a `Vec<usize>` (positions of the right-key columns in the CTE column list, alias-agnostic) →
/// references of the same CTE under different aliases share one bucket. Enabled only when the right keys
/// are all simple column references (otherwise the old `select::mod.rs` path applies).
///
/// Bucket values = `Vec<u32>` row indices into the **complete** row set of `CteStore.dense_rows()`
/// (independent of the `SharedCteSelected` selection); referencing sides filter by their own
/// selection bitset at probe time, so the build is constructed only once.
pub struct CachedHashBuild {
    pub(crate) buckets: hashbrown::HashMap<Vec<ValueKey>, Vec<u32>, ahash::RandomState>,
}

impl CachedHashBuild {
    pub(crate) fn with_buckets(
        buckets: hashbrown::HashMap<Vec<ValueKey>, Vec<u32>, ahash::RandomState>,
    ) -> Self {
        Self { buckets }
    }
}

impl std::fmt::Debug for CachedHashBuild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedHashBuild")
            .field("buckets", &self.buckets.len())
            .finish()
    }
}

#[derive(Debug, Default)]
pub struct CteHashBuildCache {
    entries: Mutex<HashMap<Vec<usize>, Arc<CachedHashBuild>>>,
}

impl CteHashBuildCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Take out an already-built bucket.
    pub fn get(&self, key: &[usize]) -> Option<Arc<CachedHashBuild>> {
        self.entries.lock().ok()?.get(key).cloned()
    }

    /// Return on hit; on miss build via `build`, insert it, then return. `build` errors propagate unchanged.
    /// On multi-threaded races only the first successful bucket is kept (later ones are discarded, at the cost of one redundant build).
    pub fn get_or_init<F, E>(&self, key: Vec<usize>, build: F) -> Result<Arc<CachedHashBuild>, E>
    where
        F: FnOnce() -> Result<CachedHashBuild, E>,
    {
        if let Some(existing) = self.get(&key) {
            return Ok(existing);
        }
        let new_build = Arc::new(build()?);
        if let Ok(mut g) = self.entries.lock() {
            if let Some(existing) = g.get(&key) {
                return Ok(existing.clone());
            }
            g.insert(key, new_build.clone());
        }
        Ok(new_build)
    }
}
