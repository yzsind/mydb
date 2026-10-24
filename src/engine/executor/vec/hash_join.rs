//! Vectorized hash join (post-B1 columnar form).
//!
//! The build side accumulates batched input into **one contiguous set of typed columns** (`ColumnVector` +
//! `append_rows_from`); the hash index maps encoded key → build row number, while the probe side
//! collects matched `(build_idx, probe_idx)` pairs per batch, then gathers **a columnar output
//! Batch** column by column with `take_indices` (the left/right column order is arranged into the
//! final SQL output order at gather time). Compared with the old implementation (`Vec<Vec<Value>>`
//! row-wise materialization + whole-row clone per hit + final `from_values` conversion back to
//! columnar), it removes the two row/column format conversions and the per-cell `Value` clone per
//! hit; gathering a Utf8 column is a segment copy of offsets+bytes, with no `String` allocated per output cell.
//!
//! Outer joins (probe-preserved side) use a `u32::MAX` sentinel pair so `take_indices` injects
//! NULL directly; SEMI/ANTI without residual keep the build side as a key-only `HashSet`.
//! B2: `null_safe` (`<=>`) encodes NULL per column as a 0xFF sentinel that participates in matching;
//! residual is evaluated vectorized via `expr_batch::eval_predicate_bitmap` on the gathered candidate
//! batch — INNER uses it directly as a Selection mask, while the probe-preserved
//! side / SEMI / ANTI resolve pairs first before applying keep/NULL-extension semantics.
//!
//! Build collectors account for retained columns and keys through BuildCap.
//! The bridge spills the retained prefix and remaining input when the cap is
//! exceeded, preserving the input stream without replaying consumed rows.

use super::join_output::JoinOutputQueue;
use std::cell::Cell;
use std::sync::Arc;

use smallvec::SmallVec;

/// Build-side hash index: encoded join-key bytes → list of build-row
/// indices. ahash hashing is 2-3× cheaper than std SipHash-1-3 on the
/// short encoded keys produced by `key_encode_into`, and this map is
/// the hot path during the probe phase (one `get` per probe row).
pub(crate) type BuildIndex = hashbrown::HashMap<Vec<u8>, SmallVec<[u32; 1]>, ahash::RandomState>;

/// Native two-integer join-key index. Composite integer joins are common for
/// parent/child and bridge tables; keeping the pair typed avoids constructing
/// a canonical byte buffer and hashing that buffer for every probe row.
pub(crate) type I64PairBuildIndex =
    hashbrown::HashMap<(i64, i64), SmallVec<[u32; 1]>, ahash::RandomState>;

pub(crate) enum I64BuildIndex {
    /// One row per key in a compact direct-address table. `u32::MAX` marks
    /// an absent key, so the common PK/FK join needs one bounds check and one
    /// indexed load instead of hashing every probe value.
    DenseUnique {
        min: i64,
        entries: Vec<u32>,
    },
    /// Dense key domain with duplicate build keys. Offsets use CSR layout:
    /// the hits for key `k` are `rows[offsets[k]..offsets[k + 1]]`.
    DenseMulti {
        min: i64,
        offsets: Vec<u32>,
        rows: Vec<u32>,
    },
    Hash(hashbrown::HashMap<i64, SmallVec<[u32; 1]>, ahash::RandomState>),
}

impl I64BuildIndex {
    #[inline]
    pub(crate) fn get(&self, key: &i64) -> Option<&[u32]> {
        match self {
            Self::DenseUnique { min, entries } => {
                let Some(delta) = key.checked_sub(*min) else {
                    return None;
                };
                let offset = delta as u64;
                if offset >= entries.len() as u64 {
                    return None;
                }
                let row = &entries[offset as usize];
                (*row != u32::MAX).then(|| std::slice::from_ref(row))
            }
            Self::DenseMulti { min, offsets, rows } => {
                let Some(delta) = key.checked_sub(*min) else {
                    return None;
                };
                let offset = delta as u64;
                if offset >= offsets.len().saturating_sub(1) as u64 {
                    return None;
                }
                let offset = offset as usize;
                let hits = &rows[offsets[offset] as usize..offsets[offset + 1] as usize];
                (!hits.is_empty()).then_some(hits)
            }
            Self::Hash(index) => index.get(key).map(SmallVec::as_slice),
        }
    }

    /// Return the single row for `key`. The caller must first prove
    /// [`Self::all_unique`]; keeping this scalar avoids constructing and
    /// iterating a one-element slice in unique-key aggregate probe loops.
    #[inline]
    pub(crate) fn get_unique(&self, key: &i64) -> Option<u32> {
        match self {
            Self::DenseUnique { min, entries } => {
                let delta = key.checked_sub(*min)? as u64;
                let row = *entries.get(usize::try_from(delta).ok()?)?;
                (row != u32::MAX).then_some(row)
            }
            Self::DenseMulti { min, offsets, rows } => {
                let delta = key.checked_sub(*min)? as u64;
                let offset = usize::try_from(delta).ok()?;
                let start = *offsets.get(offset)? as usize;
                let end = *offsets.get(offset.checked_add(1)?)? as usize;
                (end == start + 1).then(|| rows[start])
            }
            Self::Hash(index) => index.get(key).and_then(|hits| {
                (hits.len() == 1).then(|| *hits.first().expect("unique hit is present"))
            }),
        }
    }

    #[inline]
    pub(crate) fn all_unique(&self) -> bool {
        match self {
            Self::DenseUnique { .. } => true,
            Self::DenseMulti { offsets, .. } => offsets
                .windows(2)
                .all(|range| range[1].saturating_sub(range[0]) <= 1),
            Self::Hash(index) => index.values().all(|hits| hits.len() <= 1),
        }
    }
}

/// Hash-table bookkeeping that is not represented by `Batch::memory_bytes`.
/// The exact collector additionally charges encoded key bytes and one u32
/// row id per build row. The bridge uses the conservative per-row estimate
/// before keys have been encoded, so it can choose Grace spill early.
const BUILD_INDEX_ENTRY_OVERHEAD: u64 =
    (std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<SmallVec<[u32; 1]>>() + 16) as u64;
const BUILD_INDEX_ROW_ID_BYTES: u64 = std::mem::size_of::<u32>() as u64;
const PREFLIGHT_KEY_BYTES_PER_ROW: u64 = 32;

pub(crate) fn estimate_build_index_bytes(rows: u64) -> u64 {
    rows.saturating_mul(
        BUILD_INDEX_ENTRY_OVERHEAD
            .saturating_add(BUILD_INDEX_ROW_ID_BYTES)
            .saturating_add(PREFLIGHT_KEY_BYTES_PER_ROW),
    )
}

/// Key-only build-side set for SEMI / ANTI joins. Same ahash motivation
/// as `BuildIndex`: SEMI/ANTI probes do one `contains` per probe row.
type BuildSet = hashbrown::HashSet<Vec<u8>, ahash::RandomState>;

use crate::types::QueryError;
#[cfg(test)]
use crate::types::Value;

use super::batch::{Batch, ColumnVector, Selection, DEFAULT_BATCH_ROWS};
use super::expr_batch::BatchExprProgram;
use super::schema::BatchSchema;
use super::stream::BatchOperator;

/// `u32::MAX` pair sentinel: the NULL-extended row of a probe-preserved outer join.
const NULL_BUILD: u32 = u32::MAX;

/// Build-side size cap for the vectorized hash join. Mirrors the
/// row-based join's `MYDB_HASH_JOIN_MEM_LIMIT_MB` / max-build-rows so an
/// under-estimated build that the CBO routed to the columnar path switches
/// to the bridge's vec grace-spill join instead of materialising an unbounded
/// build in memory (P0/B1).
///
/// Published as a thread-local by the bridge entry points rather than
/// threaded through every `run_*` signature, so the build collectors
/// deep in the call tree can consult it without churning the public
/// surface. Unset (tests, ad-hoc callers) ⇒ unbounded, preserving
/// existing behaviour.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BuildCap {
    pub(crate) max_rows: u64,
    pub(crate) max_bytes: u64,
}

impl BuildCap {
    #[inline]
    fn exceeded(&self, rows: u64, bytes: u64) -> bool {
        (self.max_rows > 0 && rows > self.max_rows)
            || (self.max_bytes > 0 && bytes > self.max_bytes)
    }
}

thread_local! {
    static VEC_BUILD_CAP: Cell<Option<BuildCap>> = const { Cell::new(None) };
}

/// Run `f` with `cap` published as the current vectorized-hash-join
/// build cap, restoring the previous value on return (nesting-safe).
pub(crate) fn with_vec_build_cap<F, R>(cap: BuildCap, f: F) -> R
where
    F: FnOnce() -> R,
{
    let prev = VEC_BUILD_CAP.with(|c| c.replace(Some(cap)));
    let result = f();
    VEC_BUILD_CAP.with(|c| c.set(prev));
    result
}

#[inline]
fn current_vec_build_cap() -> Option<BuildCap> {
    VEC_BUILD_CAP.with(|c| c.get())
}

/// Internal condition: "vectorized hash join build exceeded the size
/// cap; fall back to the row-based path". Caught at the bridge boundary
/// and converted to a fallback — never surfaced to a client.
#[inline]
pub(crate) fn vec_build_overflow_error() -> QueryError {
    QueryError::internal_control(
        crate::error::InternalErrorKind::VectorHashJoinFallback,
        crate::error::catalog::ER_OUT_OF_RESOURCES,
        "vectorized hash join build exceeded size cap; falling back to row-based join",
    )
}

#[inline]
pub(crate) fn is_vec_build_overflow(err: &QueryError) -> bool {
    err.is_internal(crate::error::InternalErrorKind::VectorHashJoinFallback)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg(test)]
pub enum VecJoinType {
    Inner,
    Left,
}

/// Common parameters of materialized joins. `out_schema` is the **final SQL output column order**
/// (left, right); `build_cols_first` says whether the build columns come first in that order
/// (i.e. whether the join's build side is the SQL left side). The residual (if any) must be a
/// `BatchExprProgram` compiled against `out_schema`. Expressions not yet vectorized only invoke
/// a scalar oracle within the current candidate pair batch, without changing the join's input pipeline.
pub struct VecJoinSpec<'a> {
    pub build_keys: &'a [usize],
    pub probe_keys: &'a [usize],
    pub key_collations: &'a [Option<u16>],
    pub null_safe: &'a [bool],
    pub build_cols_first: bool,
    pub out_schema: Arc<BatchSchema>,
    pub residual: Option<&'a BatchExprProgram>,
}

/// Encode one row's join key. Returning false means the row has a NULL key in a non-null-safe
/// column, which can never match under any join semantics. NULL in a null-safe column is encoded
/// as the 0xFF sentinel of `encode_from_column_row` (NULL <=> NULL hits).
#[inline]
pub(crate) fn encode_join_key_with_collations(
    batch: &Batch,
    keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
    r: usize,
    buf: &mut Vec<u8>,
) -> bool {
    buf.clear();
    let cols = batch.columns();
    for (i, &c) in keys.iter().enumerate() {
        let col = &cols[c];
        if col.is_null(r) {
            if !null_safe.get(i).copied().unwrap_or(false) {
                return false;
            }
            // Same canonical NULL tag as the shared value/column encoder.
            buf.push(0);
            continue;
        }
        // Fixed-width and binary lanes do not consult a text collation.
        // Avoid repeated schema lookups for those keys; Generic can contain
        // strings and must retain the normal per-column collation fallback.
        let collation = match col {
            ColumnVector::Utf8 { .. } | ColumnVector::Generic { .. } => key_collations
                .get(i)
                .copied()
                .flatten()
                .or(batch.schema().fields()[c].source.collation_id),
            _ => None,
        };
        super::key_encode::encode_nonnull_from_column_row_with_collation(col, r, collation, buf);
    }
    true
}

/// Fill the batch's visible row indices into the reusable buffer.
#[inline]
fn fill_visible(batch: &Batch, visible: &mut Vec<usize>) {
    visible.clear();
    visible.extend(batch.visible_indices());
}

/// Build-side accumulated result: contiguous typed columns + hash index.
pub(crate) struct BuildPayload {
    pub(crate) columns: Vec<ColumnVector>,
    pub(crate) index: BuildIndex,
    /// Native single-key integer lane. It is an internal layout of the same
    /// hash join, not a different execution route, and avoids allocating and
    /// hashing an encoded byte key for every fact-row probe.
    pub(crate) i64_index: Option<I64BuildIndex>,
    /// Native two-key integer lane; mutually exclusive with the encoded byte
    /// index when the caller proved both build/probe schemas are signed-i64.
    pub(crate) i64_pair_index: Option<I64PairBuildIndex>,
}

/// Integer values and validity are bound once per batch, avoiding repeated
/// ColumnVector type dispatch in native build/probe row loops.
#[derive(Clone, Copy)]
pub(crate) struct I64KeyColumn<'a> {
    values: &'a [i64],
    valid: Option<&'a super::bitmap::Bitmap>,
}

impl<'a> I64KeyColumn<'a> {
    fn new(column: &'a ColumnVector) -> Option<Self> {
        let ColumnVector::I64 { data, nulls } = column else {
            return None;
        };
        Some(Self {
            values: data,
            valid: nulls.as_ref(),
        })
    }

    #[inline]
    fn usable(self, row: usize) -> bool {
        self.valid.is_none_or(|valid| valid.get(row))
    }
}

/// Resolve the payload index lane once per input batch. Output policy and
/// residual evaluation remain with the caller; lookup returns SQL key
/// usability separately from an ordinary hash miss for runtime statistics.
pub(crate) enum PayloadProbe<'a> {
    I64 {
        index: &'a I64BuildIndex,
        column: I64KeyColumn<'a>,
    },
    Pair {
        index: &'a I64PairBuildIndex,
        first: I64KeyColumn<'a>,
        second: I64KeyColumn<'a>,
    },
    Encoded {
        index: &'a BuildIndex,
        batch: &'a Batch,
        keys: &'a [usize],
        null_safe: &'a [bool],
        collations: &'a [Option<u16>],
    },
}

impl<'a> PayloadProbe<'a> {
    pub(crate) fn new(
        payload: &'a BuildPayload,
        batch: &'a Batch,
        keys: &'a [usize],
        null_safe: &'a [bool],
        collations: &'a [Option<u16>],
    ) -> Self {
        if let (Some(index), Some(&first), Some(&second)) =
            (payload.i64_pair_index.as_ref(), keys.first(), keys.get(1))
        {
            if let (Some(first), Some(second)) = (
                batch.columns().get(first).and_then(I64KeyColumn::new),
                batch.columns().get(second).and_then(I64KeyColumn::new),
            ) {
                return Self::Pair {
                    index,
                    first,
                    second,
                };
            }
        }
        if let (Some(index), Some(&column)) = (payload.i64_index.as_ref(), keys.first()) {
            if let Some(column) = batch.columns().get(column).and_then(I64KeyColumn::new) {
                return Self::I64 { index, column };
            }
        }
        Self::Encoded {
            index: &payload.index,
            batch,
            keys,
            null_safe,
            collations,
        }
    }

    #[inline]
    pub(crate) fn lookup(&self, row: usize, key: &mut Vec<u8>) -> (bool, Option<&'a [u32]>) {
        match self {
            Self::I64 { index, column } => {
                let usable = column.usable(row);
                (
                    usable,
                    if usable {
                        index.get(&column.values[row])
                    } else {
                        None
                    },
                )
            }
            Self::Pair {
                index,
                first,
                second,
            } => {
                let usable = first.usable(row) && second.usable(row);
                (
                    usable,
                    if usable {
                        index
                            .get(&(first.values[row], second.values[row]))
                            .map(SmallVec::as_slice)
                    } else {
                        None
                    },
                )
            }
            Self::Encoded {
                index,
                batch,
                keys,
                null_safe,
                collations,
            } => {
                let usable =
                    encode_join_key_with_collations(batch, keys, null_safe, collations, row, key);
                (
                    usable,
                    if usable {
                        index.get(key.as_slice()).map(SmallVec::as_slice)
                    } else {
                        None
                    },
                )
            }
        }
    }
}

/// Build an exact membership state when the hash key is a single canonical
/// integer lane. Allocation pressure or a non-integer runtime value declines
/// this optional representation; callers can publish a Bloom filter instead.
pub(crate) fn build_exact_int_runtime_filter(
    payload: &BuildPayload,
    build_keys: &[usize],
    key_collations: &[Option<u16>],
) -> Option<Arc<crate::engine::executor::select::bloom::ExactIntKeySet>> {
    if build_keys.len() != 1 || key_collations.iter().any(Option::is_some) {
        return None;
    }
    let values = payload.columns.get(build_keys[0])?;
    let mut builder =
        crate::engine::executor::select::bloom::ExactIntKeySetBuilder::with_capacity(values.len())
            .ok()?;
    let mut inserted = 0usize;
    for row in 0..values.len() {
        if values.is_null(row) {
            continue;
        }
        let crate::engine::expr::ValueKey::Int(value) =
            crate::engine::expr::ValueKey::from(&values.value_at(row))
        else {
            return None;
        };
        builder.insert(value);
        inserted += 1;
    }
    if inserted == 0 {
        return None;
    }
    builder.finish().ok().map(Arc::new)
}

fn build_i64_index(
    columns: &[ColumnVector],
    build_keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
) -> Option<I64BuildIndex> {
    if build_keys.len() != 1
        || null_safe.first().copied().unwrap_or(false)
        || key_collations.first().copied().flatten().is_some()
    {
        return None;
    }
    let column = columns.get(build_keys[0])?;
    let values = column.as_i64_slice()?;
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    let mut non_null = 0usize;
    for (row, &value) in values.iter().enumerate() {
        if !column.is_null(row) {
            min = min.min(value);
            max = max.max(value);
            non_null += 1;
        }
    }
    if non_null == 0 {
        return Some(I64BuildIndex::Hash(hashbrown::HashMap::with_hasher(
            ahash::RandomState::new(),
        )));
    }
    let span = i128::from(max) - i128::from(min) + 1;
    // Direct addressing is an internal layout of the same hash join. The old
    // `Vec<SmallVec>` representation needed roughly 24 bytes per key and was
    // therefore capped at two million slots. Compact u32 entries/CSR offsets
    // need four bytes per slot, allowing large PK/FK domains without excess
    // resident memory. Keep an absolute bound as well as the density bound so
    // adversarial sparse integer ranges retain the hash representation.
    if span > 0 && span <= 64_000_000 && span <= (non_null as i128).saturating_mul(16) {
        let span = span as usize;
        let mut entries = vec![u32::MAX; span];
        let mut unique = true;
        for (row, &value) in values.iter().enumerate() {
            if !column.is_null(row) {
                let slot = &mut entries[(i128::from(value) - i128::from(min)) as usize];
                if *slot == u32::MAX {
                    *slot = row as u32;
                } else {
                    unique = false;
                    break;
                }
            }
        }
        if unique {
            return Some(I64BuildIndex::DenseUnique { min, entries });
        }
        drop(entries);

        // Duplicate keys use a compact CSR adjacency list. Build prefix ends
        // in `offsets`, then fill rows backwards so no second cursor array is
        // needed; finally shift starts into the conventional offsets layout.
        let mut offsets = vec![0u32; span + 1];
        for (row, &value) in values.iter().enumerate() {
            if !column.is_null(row) {
                let offset = (i128::from(value) - i128::from(min)) as usize;
                offsets[offset + 1] = offsets[offset + 1]
                    .checked_add(1)
                    .expect("dense i64 build bucket overflow");
            }
        }
        for offset in 1..offsets.len() {
            offsets[offset] = offsets[offset]
                .checked_add(offsets[offset - 1])
                .expect("dense i64 build row overflow");
        }
        let total = *offsets.last().unwrap_or(&0);
        let mut rows = vec![0u32; total as usize];
        for (row, &value) in values.iter().enumerate().rev() {
            if !column.is_null(row) {
                let offset = (i128::from(value) - i128::from(min)) as usize;
                offsets[offset + 1] -= 1;
                rows[offsets[offset + 1] as usize] = row as u32;
            }
        }
        for offset in 1..span {
            offsets[offset] = offsets[offset + 1];
        }
        offsets[span] = total;
        Some(I64BuildIndex::DenseMulti { min, offsets, rows })
    } else {
        let mut index =
            hashbrown::HashMap::with_capacity_and_hasher(non_null, ahash::RandomState::new());
        for (row, &value) in values.iter().enumerate() {
            if !column.is_null(row) {
                index
                    .entry(value)
                    .or_insert_with(SmallVec::new)
                    .push(row as u32);
            }
        }
        Some(I64BuildIndex::Hash(index))
    }
}

fn build_i64_pair_index(
    columns: &[ColumnVector],
    build_keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
) -> Option<I64PairBuildIndex> {
    if build_keys.len() != 2
        || null_safe.iter().any(|value| *value)
        || key_collations.iter().any(Option::is_some)
    {
        return None;
    }
    let first = columns.get(build_keys[0])?;
    let second = columns.get(build_keys[1])?;
    let first_values = first.as_i64_slice()?;
    let second_values = second.as_i64_slice()?;
    let non_null = (0..first_values.len())
        .filter(|&row| !first.is_null(row) && !second.is_null(row))
        .count();
    let mut index =
        I64PairBuildIndex::with_capacity_and_hasher(non_null, ahash::RandomState::new());
    for row in 0..first_values.len() {
        if !first.is_null(row) && !second.is_null(row) {
            index
                .entry((first_values[row], second_values[row]))
                .or_default()
                .push(row as u32);
        }
    }
    Some(index)
}

/// Collect the build stream: column accumulation + key indexing. Rows containing a non-null-safe
/// NULL key are skipped from the index **and the payload** together — INNER/probe-preserved/SEMI/
/// ANTI can never reference them (this module does not support build-preserved outer joins).
#[cfg(test)]
pub(crate) fn collect_build_payload(
    build: &mut dyn BatchOperator,
    build_keys: &[usize],
    null_safe: &[bool],
) -> Result<BuildPayload, QueryError> {
    collect_build_payload_with_collations(build, build_keys, null_safe, &[])
}

pub(crate) fn collect_build_payload_with_collations(
    build: &mut dyn BatchOperator,
    build_keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
) -> Result<BuildPayload, QueryError> {
    collect_build_payload_impl(build, build_keys, null_safe, key_collations, false)
}

/// Streaming joins whose build and probe schemas both guarantee the native
/// signed-integer lane do not need the encoded-byte index. Avoid constructing
/// and retaining that duplicate table; `i64_index` is the semantic index for
/// this layout. Other callers keep the general index as their correctness
/// fallback for dynamically typed or collated keys.
pub(crate) fn collect_build_payload_i64_only(
    build: &mut dyn BatchOperator,
    build_keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
) -> Result<BuildPayload, QueryError> {
    debug_assert_eq!(build_keys.len(), 1);
    debug_assert!(!null_safe.first().copied().unwrap_or(false));
    debug_assert!(key_collations.first().copied().flatten().is_none());
    collect_build_payload_impl(build, build_keys, null_safe, key_collations, true)
}

pub(crate) fn collect_build_payload_i64_pair_only(
    build: &mut dyn BatchOperator,
    build_keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
) -> Result<BuildPayload, QueryError> {
    debug_assert_eq!(build_keys.len(), 2);
    debug_assert!(null_safe.iter().all(|value| !*value));
    debug_assert!(key_collations.iter().all(Option::is_none));
    collect_build_payload_impl(build, build_keys, null_safe, key_collations, true)
}

fn collect_build_payload_impl(
    build: &mut dyn BatchOperator,
    build_keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
    i64_only: bool,
) -> Result<BuildPayload, QueryError> {
    let ncols = build.schema().fields().len();
    let mut columns: Vec<ColumnVector> =
        (0..ncols).map(|_| ColumnVector::Null { len: 0 }).collect();
    let mut index: BuildIndex = BuildIndex::default();
    let mut key_buf = Vec::with_capacity(64);
    let mut visible: Vec<usize> = Vec::new();
    let mut keep: Vec<usize> = Vec::new();
    let mut next_row_idx: u32 = 0;
    // P0/B1: bound in-memory build materialisation. `next_row_idx` is the
    // exact materialised row count; `build_bytes` accrues the kept-row
    // share of each batch's heap footprint (O(N) overall). Over-cap ⇒
    // sentinel error → bridge falls back to the row-based grace join.
    let cap = current_vec_build_cap();
    let mut build_bytes: u64 = 0;
    while let Some(batch) = build.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        fill_visible(&batch, &mut visible);
        keep.clear();
        for &r in &visible {
            if i64_only {
                // The caller's schema check guarantees I64 accumulators. An
                // all-NULL batch may still use Null vectors, so test validity
                // before consulting their native slices.
                if build_keys
                    .iter()
                    .any(|&column| batch.columns()[column].is_null(r))
                {
                    continue;
                }
                debug_assert!(build_keys
                    .iter()
                    .all(|&column| batch.columns()[column].as_i64_slice().is_some()));
                if cap.is_some() {
                    // Conservative hash-layout charge while the final key
                    // density is not yet known. Dense layouts consume less.
                    build_bytes = build_bytes
                        .saturating_add(BUILD_INDEX_ENTRY_OVERHEAD)
                        .saturating_add(BUILD_INDEX_ROW_ID_BYTES)
                        .saturating_add((build_keys.len() * std::mem::size_of::<i64>()) as u64);
                }
            } else {
                if !encode_join_key_with_collations(
                    &batch,
                    build_keys,
                    null_safe,
                    key_collations,
                    r,
                    &mut key_buf,
                ) {
                    continue;
                }
                if cap.is_some() {
                    build_bytes = build_bytes.saturating_add(BUILD_INDEX_ROW_ID_BYTES);
                    if !index.contains_key(key_buf.as_slice()) {
                        build_bytes = build_bytes
                            .saturating_add(BUILD_INDEX_ENTRY_OVERHEAD)
                            .saturating_add(key_buf.len() as u64);
                    }
                }
                index
                    .entry_ref(key_buf.as_slice())
                    .or_default()
                    .push(next_row_idx);
            }
            keep.push(r);
            next_row_idx = next_row_idx.checked_add(1).expect("build row overflow");
            if cap.is_some_and(|cap| cap.exceeded(next_row_idx as u64, build_bytes)) {
                return Err(vec_build_overflow_error());
            }
        }
        if !keep.is_empty() {
            for (c, col) in columns.iter_mut().enumerate() {
                col.append_rows_from(&batch.columns()[c], &keep);
            }
            if let Some(cap) = cap {
                let denom = batch.num_rows().max(1) as u128;
                let added = (batch.memory_bytes() as u128 * keep.len() as u128 / denom) as u64;
                build_bytes = build_bytes.saturating_add(added);
                if cap.exceeded(next_row_idx as u64, build_bytes) {
                    return Err(vec_build_overflow_error());
                }
            }
        }
    }
    let i64_index = build_i64_index(&columns, build_keys, null_safe, key_collations);
    let i64_pair_index = i64_only
        .then(|| build_i64_pair_index(&columns, build_keys, null_safe, key_collations))
        .flatten();
    debug_assert!(
        !i64_only || next_row_idx == 0 || i64_index.is_some() || i64_pair_index.is_some()
    );
    Ok(BuildPayload {
        columns,
        index,
        i64_index,
        i64_pair_index,
    })
}

/// FULL OUTER JOIN must retain every build row, including rows whose key
/// contains a non-null-safe NULL. Those rows are absent from the hash index but
/// remain addressable by the final unmatched-build pass.
pub(crate) fn collect_full_build_payload(
    build: &mut dyn BatchOperator,
    build_keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
) -> Result<BuildPayload, QueryError> {
    let ncols = build.schema().fields().len();
    let mut columns: Vec<ColumnVector> =
        (0..ncols).map(|_| ColumnVector::Null { len: 0 }).collect();
    let mut index: BuildIndex = BuildIndex::default();
    let mut key_buf = Vec::with_capacity(64);
    let mut visible = Vec::new();
    let mut next_row_idx = 0u32;
    let cap = current_vec_build_cap();
    let mut build_bytes = 0u64;
    while let Some(batch) = build.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        fill_visible(&batch, &mut visible);
        for &row in &visible {
            if encode_join_key_with_collations(
                &batch,
                build_keys,
                null_safe,
                key_collations,
                row,
                &mut key_buf,
            ) {
                if cap.is_some() {
                    build_bytes = build_bytes.saturating_add(BUILD_INDEX_ROW_ID_BYTES);
                    if !index.contains_key(key_buf.as_slice()) {
                        build_bytes = build_bytes
                            .saturating_add(BUILD_INDEX_ENTRY_OVERHEAD)
                            .saturating_add(key_buf.len() as u64);
                    }
                }
                index
                    .entry_ref(key_buf.as_slice())
                    .or_default()
                    .push(next_row_idx);
            }
            next_row_idx = next_row_idx.checked_add(1).expect("build row overflow");
        }
        if !visible.is_empty() {
            for (column_index, column) in columns.iter_mut().enumerate() {
                column.append_rows_from(&batch.columns()[column_index], &visible);
            }
            if let Some(cap) = cap {
                let denom = batch.num_rows().max(1) as u128;
                build_bytes = build_bytes.saturating_add(
                    (batch.memory_bytes() as u128 * visible.len() as u128 / denom) as u64,
                );
                if cap.exceeded(next_row_idx as u64, build_bytes) {
                    return Err(vec_build_overflow_error());
                }
            }
        }
    }
    let i64_index = build_i64_index(&columns, build_keys, null_safe, key_collations);
    let i64_pair_index = build_i64_pair_index(&columns, build_keys, null_safe, key_collations);
    Ok(BuildPayload {
        columns,
        index,
        i64_index,
        i64_pair_index,
    })
}

/// Preserved builds with proven signed-integer pairs need only one index.
/// NULL-key rows remain in the payload for the unmatched-build pass, while
/// the native index contains exactly the rows usable by ordinary equality.
pub(crate) fn collect_full_build_payload_i64_pair_only(
    build: &mut dyn BatchOperator,
    build_keys: &[usize],
    reserve_rows: usize,
) -> Result<BuildPayload, QueryError> {
    debug_assert_eq!(build_keys.len(), 2);
    let mut columns: Vec<ColumnVector> = (0..build.schema().fields().len())
        .map(|_| ColumnVector::Null { len: 0 })
        .collect();
    let mut index =
        I64PairBuildIndex::with_capacity_and_hasher(reserve_rows, ahash::RandomState::new());
    let cap = current_vec_build_cap();
    let mut bytes = 0u64;
    let mut next_row = 0u32;
    let mut visible = Vec::new();
    while let Some(batch) = build.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        fill_visible(&batch, &mut visible);
        let first = &batch.columns()[build_keys[0]];
        let second = &batch.columns()[build_keys[1]];
        let native = I64KeyColumn::new(first).zip(I64KeyColumn::new(second));
        for &row in &visible {
            let usable = native.map_or_else(
                || !first.is_null(row) && !second.is_null(row),
                |(first, second)| first.usable(row) && second.usable(row),
            );
            if usable {
                let Some((first, second)) = native else {
                    return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                        .error("native full-join key lane changed"));
                };
                let entry = index.entry((first.values[row], second.values[row]));
                if cap.is_some() {
                    bytes = bytes.saturating_add(BUILD_INDEX_ROW_ID_BYTES);
                    if matches!(&entry, hashbrown::hash_map::Entry::Vacant(_)) {
                        bytes = bytes
                            .saturating_add(BUILD_INDEX_ENTRY_OVERHEAD)
                            .saturating_add(16);
                    }
                }
                entry.or_default().push(next_row);
            }
            next_row = next_row
                .checked_add(1)
                .ok_or_else(vec_build_overflow_error)?;
            if cap.is_some_and(|cap| cap.exceeded(next_row as u64, bytes)) {
                return Err(vec_build_overflow_error());
            }
        }
        if !visible.is_empty() {
            for (column, source) in columns.iter_mut().zip(batch.columns()) {
                column.append_rows_from(source, &visible);
            }
            bytes = bytes.saturating_add(
                (batch.memory_bytes() as u128 * visible.len() as u128
                    / batch.num_rows().max(1) as u128) as u64,
            );
            if cap.is_some_and(|cap| cap.exceeded(next_row as u64, bytes)) {
                return Err(vec_build_overflow_error());
            }
        }
    }
    Ok(BuildPayload {
        columns,
        index: BuildIndex::default(),
        i64_index: None,
        i64_pair_index: Some(index),
    })
}

fn collect_build_keyset_with_collations(
    build: &mut dyn BatchOperator,
    build_keys: &[usize],
    null_safe: &[bool],
    key_collations: &[Option<u16>],
) -> Result<BuildSet, QueryError> {
    let mut set: BuildSet = BuildSet::default();
    let mut key_buf = Vec::with_capacity(64);
    let mut visible: Vec<usize> = Vec::new();
    // P0/B1: bound the key-only build set. The set holds distinct keys
    // (≤ build rows), so the cap applies to distinct-key count and the
    // summed key bytes (+ per-entry hashset overhead). Over-cap ⇒
    // sentinel → bridge falls back to the row-based join.
    let cap = current_vec_build_cap();
    let mut set_bytes: u64 = 0;
    while let Some(batch) = build.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        fill_visible(&batch, &mut visible);
        for &r in &visible {
            if !encode_join_key_with_collations(
                &batch,
                build_keys,
                null_safe,
                key_collations,
                r,
                &mut key_buf,
            ) {
                continue;
            }
            // Check existence by borrow first and clone only new keys — zero allocation for duplicate keys.
            if !set.contains(key_buf.as_slice()) {
                set.insert(key_buf.clone());
                if let Some(cap) = cap {
                    set_bytes = set_bytes
                        .saturating_add(key_buf.len() as u64)
                        .saturating_add(std::mem::size_of::<Vec<u8>>() as u64)
                        .saturating_add(16);
                    if cap.exceeded(set.len() as u64, set_bytes) {
                        return Err(vec_build_overflow_error());
                    }
                }
            }
        }
    }
    Ok(set)
}

/// Gather an output Batch in the final column order from the pair list.
/// `NULL_BUILD` sentinels in `pair_build` inject NULL into the build columns.
pub(crate) fn gather_pairs_batch(
    build_columns: &[ColumnVector],
    probe_batch: &Batch,
    pair_build: &[u32],
    pair_probe: &[u32],
    build_cols_first: bool,
    out_schema: &Arc<BatchSchema>,
) -> Batch {
    debug_assert_eq!(pair_build.len(), pair_probe.len());
    let mut columns: Vec<ColumnVector> =
        Vec::with_capacity(build_columns.len() + probe_batch.columns().len());
    if build_cols_first {
        for col in build_columns {
            columns.push(col.take_indices(pair_build));
        }
        for col in probe_batch.columns() {
            columns.push(col.take_indices(pair_probe));
        }
    } else {
        for col in probe_batch.columns() {
            columns.push(col.take_indices(pair_probe));
        }
        for col in build_columns {
            columns.push(col.take_indices(pair_build));
        }
    }
    Batch::new(out_schema.clone(), columns)
}

/// Gather only the build side when the output preserves every physical probe
/// row in its original order. The probe vectors can then be moved into the
/// output batch instead of being copied through `take_indices([0, 1, ...])`.
///
/// This is the common unique-key PK/FK shape. `pair_build` may contain the
/// NULL sentinel for a probe-preserving outer join; the probe side itself is
/// required to have no selection and therefore remains a zero-copy move.
fn gather_identity_probe_batch(
    build_columns: &[ColumnVector],
    probe_batch: Batch,
    pair_build: &[u32],
    build_cols_first: bool,
    out_schema: &Arc<BatchSchema>,
) -> Result<Batch, QueryError> {
    debug_assert!(probe_batch.selection().is_none());
    debug_assert_eq!(pair_build.len(), probe_batch.num_rows());
    let row_count = probe_batch.num_rows();
    let mut columns = Vec::with_capacity(build_columns.len() + probe_batch.columns.len());
    let (probe_columns, owner) = probe_batch.into_columns()?.into_parts();
    if build_cols_first {
        columns.extend(
            build_columns
                .iter()
                .map(|column| column.take_indices(pair_build)),
        );
        columns.extend(probe_columns);
    } else {
        columns.extend(probe_columns);
        columns.extend(
            build_columns
                .iter()
                .map(|column| column.take_indices(pair_build)),
        );
    }
    Ok(Batch::from_owned_columns(
        out_schema.clone(),
        super::batch::OwnedColumns::from_parts(columns, owner, row_count),
    ))
}

const ADAPTIVE_BLOOM_MIN_KEYS: usize = 128_000;
#[cfg(not(test))]
const ADAPTIVE_BLOOM_SAMPLE: u32 = 4096;
// Keep the production sampling confidence unchanged while allowing semantic
// unit tests to exercise both adaptive decisions without thousands of
// storage-backed probe rows.
#[cfg(test)]
const ADAPTIVE_BLOOM_SAMPLE: u32 = 64;

/// Shared probe hot path for materialized and streaming hash joins.
///
/// Key encoding, NULL handling, hash lookup, adaptive bloom sampling and
/// cancellation checks live here so every consumer observes identical probe
/// semantics. Output policy remains statically dispatched through `ProbeSink`.
struct ProbeKernel {
    probe_keys: Vec<usize>,
    null_safe: Vec<bool>,
    key_collations: Vec<Option<u16>>,
    key_buf: Vec<u8>,
    visible: Vec<usize>,
    bloom: Option<crate::engine::executor::select::bloom::BloomFilter>,
    adaptive_active: bool,
    sampled: u32,
    misses: u32,
}

trait ProbeSink {
    fn accept(&mut self, probe_row: u32, hits: Option<&[u32]>) -> Result<(), QueryError>;

    fn finish(&mut self) -> Result<(), QueryError>;
}

impl ProbeKernel {
    fn new(
        probe_keys: &[usize],
        null_safe: &[bool],
        key_collations: &[Option<u16>],
        index: &BuildIndex,
    ) -> Self {
        Self {
            probe_keys: probe_keys.to_vec(),
            null_safe: null_safe.to_vec(),
            key_collations: key_collations.to_vec(),
            key_buf: Vec::with_capacity(64),
            visible: Vec::new(),
            bloom: None,
            adaptive_active: index.len() >= ADAPTIVE_BLOOM_MIN_KEYS
                && crate::engine::executor::tuning::current_or_capture().hash_join_bloom_adaptive,
            sampled: 0,
            misses: 0,
        }
    }

    fn probe_batch<S: ProbeSink>(
        &mut self,
        index: &BuildIndex,
        batch: &Batch,
        sink: &mut S,
    ) -> Result<(), QueryError> {
        fill_visible(batch, &mut self.visible);
        for position in 0..self.visible.len() {
            let row = self.visible[position];
            if position > 0 && position & 1023 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            let hits = self.lookup_row(index, batch, row);
            sink.accept(row as u32, hits)?;
        }
        sink.finish()
    }

    #[inline]
    fn lookup_row<'a>(
        &mut self,
        index: &'a BuildIndex,
        batch: &Batch,
        row: usize,
    ) -> Option<&'a [u32]> {
        let usable = encode_join_key_with_collations(
            batch,
            &self.probe_keys,
            &self.null_safe,
            &self.key_collations,
            row,
            &mut self.key_buf,
        );
        if !usable {
            return None;
        }
        if self
            .bloom
            .as_ref()
            .is_some_and(|filter| !filter.contains(self.key_buf.as_slice()))
        {
            return None;
        }

        let hits = index.get(self.key_buf.as_slice()).map(SmallVec::as_slice);
        if hits.is_none() {
            crate::engine::executor::select::analyze_stats::add_pending_runtime_filter_rejected(1);
        }
        if self.adaptive_active {
            self.sampled += 1;
            if hits.is_none() {
                self.misses += 1;
            }
            if self.sampled >= ADAPTIVE_BLOOM_SAMPLE {
                self.adaptive_active = false;
                if self.misses * 2 >= self.sampled {
                    let mut filter = crate::engine::executor::select::bloom::BloomFilter::new(
                        index.len().max(1),
                    );
                    for key in index.keys() {
                        filter.insert(key.as_slice());
                    }
                    self.bloom = Some(filter);
                }
            }
        }
        hits
    }
}

struct MaterializedInnerSink<'a, 's> {
    build_columns: &'a [ColumnVector],
    probe_batch: &'a Batch,
    spec: &'a VecJoinSpec<'s>,
    out: &'a mut dyn FnMut(Batch) -> Result<(), QueryError>,
    batch_rows: usize,
    pair_build: &'a mut Vec<u32>,
    pair_probe: &'a mut Vec<u32>,
}

impl ProbeSink for MaterializedInnerSink<'_, '_> {
    fn accept(&mut self, probe_row: u32, hits: Option<&[u32]>) -> Result<(), QueryError> {
        let Some(hits) = hits else {
            return Ok(());
        };
        for (position, &build_row) in hits.iter().enumerate() {
            if position > 0 && position & 1023 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            self.pair_build.push(build_row);
            self.pair_probe.push(probe_row);
            if self.pair_build.len() >= self.batch_rows {
                flush_inner(
                    self.build_columns,
                    self.probe_batch,
                    self.pair_build,
                    self.pair_probe,
                    self.spec,
                    self.out,
                )?;
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), QueryError> {
        if !self.pair_build.is_empty() {
            flush_inner(
                self.build_columns,
                self.probe_batch,
                self.pair_build,
                self.pair_probe,
                self.spec,
                self.out,
            )?;
        }
        Ok(())
    }
}

struct MaterializedOuterSink<'a> {
    build_columns: &'a [ColumnVector],
    probe_batch: &'a Batch,
    build_cols_first: bool,
    out_schema: &'a Arc<BatchSchema>,
    out: &'a mut Vec<Batch>,
    pair_build: &'a mut Vec<u32>,
    pair_probe: &'a mut Vec<u32>,
}

impl MaterializedOuterSink<'_> {
    fn flush(&mut self) {
        self.out.push(gather_pairs_batch(
            self.build_columns,
            self.probe_batch,
            self.pair_build,
            self.pair_probe,
            self.build_cols_first,
            self.out_schema,
        ));
        self.pair_build.clear();
        self.pair_probe.clear();
    }
}

impl ProbeSink for MaterializedOuterSink<'_> {
    fn accept(&mut self, probe_row: u32, hits: Option<&[u32]>) -> Result<(), QueryError> {
        if let Some(hits) = hits {
            for (position, &build_row) in hits.iter().enumerate() {
                if position > 0 && position & 1023 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                self.pair_build.push(build_row);
                self.pair_probe.push(probe_row);
            }
        } else {
            self.pair_build.push(NULL_BUILD);
            self.pair_probe.push(probe_row);
        }
        if self.pair_build.len() >= DEFAULT_BATCH_ROWS {
            self.flush();
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), QueryError> {
        if !self.pair_build.is_empty() {
            self.flush();
        }
        Ok(())
    }
}

struct CandidateRangeSink<'a> {
    candidate_build: &'a mut Vec<u32>,
    candidate_probe: &'a mut Vec<u32>,
    row_ranges: &'a mut Vec<(u32, u32, u32)>,
}

impl ProbeSink for CandidateRangeSink<'_> {
    fn accept(&mut self, probe_row: u32, hits: Option<&[u32]>) -> Result<(), QueryError> {
        let start = self.candidate_build.len() as u32;
        if let Some(hits) = hits {
            for (position, &build_row) in hits.iter().enumerate() {
                if position > 0 && position & 1023 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                self.candidate_build.push(build_row);
                self.candidate_probe.push(probe_row);
            }
        }
        self.row_ranges
            .push((probe_row, start, self.candidate_build.len() as u32));
        Ok(())
    }

    fn finish(&mut self) -> Result<(), QueryError> {
        Ok(())
    }
}

/// INNER hash join: returns columnar output in the final column order. The residual (compiled
/// against `out_schema`) is attached to the output batch as a Selection mask.
pub fn run_inner_hash_join(
    build: &mut dyn BatchOperator,
    probe: &mut dyn BatchOperator,
    spec: &VecJoinSpec<'_>,
) -> Result<Vec<Batch>, QueryError> {
    let mut out = Vec::new();
    run_inner_hash_join_into(build, probe, spec, DEFAULT_BATCH_ROWS, &mut |batch| {
        out.push(batch);
        Ok(())
    })?;
    Ok(out)
}

/// Emit completed batches immediately, including within a single hot-key hit
/// list. A spill sink can therefore bound output independently of fanout.
pub(crate) fn run_inner_hash_join_into(
    build: &mut dyn BatchOperator,
    probe: &mut dyn BatchOperator,
    spec: &VecJoinSpec<'_>,
    batch_rows: usize,
    emit: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    assert_eq!(
        spec.build_keys.len(),
        spec.probe_keys.len(),
        "key arity mismatch"
    );
    let payload = collect_build_payload_with_collations(
        build,
        spec.build_keys,
        spec.null_safe,
        spec.key_collations,
    )?;
    if payload.index.is_empty() {
        return Ok(());
    }
    let mut kernel = ProbeKernel::new(
        spec.probe_keys,
        spec.null_safe,
        spec.key_collations,
        &payload.index,
    );
    let mut pair_build: Vec<u32> = Vec::new();
    let mut pair_probe: Vec<u32> = Vec::new();
    while let Some(batch) = probe.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        let mut sink = MaterializedInnerSink {
            build_columns: &payload.columns,
            probe_batch: &batch,
            spec,
            out: emit,
            batch_rows: batch_rows.max(1),
            pair_build: &mut pair_build,
            pair_probe: &mut pair_probe,
        };
        kernel.probe_batch(&payload.index, &batch, &mut sink)?;
    }
    Ok(())
}

fn flush_inner(
    build_columns: &[ColumnVector],
    probe_batch: &Batch,
    pair_build: &mut Vec<u32>,
    pair_probe: &mut Vec<u32>,
    spec: &VecJoinSpec<'_>,
    out: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    let batch = gather_pairs_batch(
        build_columns,
        probe_batch,
        pair_build,
        pair_probe,
        spec.build_cols_first,
        &spec.out_schema,
    );
    pair_build.clear();
    pair_probe.clear();
    let batch = match spec.residual {
        Some(program) => {
            let mask = program.eval_predicate_bitmap(&batch)?;
            if mask.count_ones() == 0 {
                return Ok(());
            }
            batch.with_selection(Selection::Mask(mask))
        }
        None => batch,
    };
    out(batch)
}

/// B3: INNER hash join with partitioned parallel build.
///
/// Build batches first go through a serial scatter (one key encoding + FNV partition number per
/// row, 1 byte/row); then `parts` threads each independently build their own partition's
/// payload + index — no shared buckets, no locks; the probe side encodes each row once and
/// routes via the same FNV to the matching partition for lookup. Per-partition tables are smaller and more cache-friendly.
///
/// Semantics are fully identical to `run_inner_hash_join` (output order is not guaranteed;
/// hash join is unordered by itself). residual/null_safe follow the spec conventions.
pub fn run_inner_hash_join_partitioned(
    build_batches: Vec<Batch>,
    probe: &mut dyn BatchOperator,
    spec: &VecJoinSpec<'_>,
    workers: usize,
) -> Result<Vec<Batch>, QueryError> {
    assert_eq!(
        spec.build_keys.len(),
        spec.probe_keys.len(),
        "key arity mismatch"
    );
    let parts = workers.clamp(2, 16);

    // --- Serial scatter: one partition id per row (u8::MAX = invisible/NULL-key rows).
    let mut pids: Vec<Vec<u8>> = Vec::with_capacity(build_batches.len());
    {
        let mut key_buf = Vec::with_capacity(64);
        let mut visible: Vec<usize> = Vec::new();
        for batch in &build_batches {
            let mut batch_pids = vec![u8::MAX; batch.num_rows()];
            fill_visible(batch, &mut visible);
            for &r in &visible {
                if encode_join_key_with_collations(
                    batch,
                    spec.build_keys,
                    spec.null_safe,
                    spec.key_collations,
                    r,
                    &mut key_buf,
                ) {
                    batch_pids[r] = (super::key_encode::fxhash(&key_buf) % parts as u64) as u8;
                }
            }
            pids.push(batch_pids);
        }
    }

    // --- Parallel per-partition build.
    let ncols = build_batches
        .first()
        .map(|b| b.columns().len())
        .unwrap_or(0);
    let payloads: Vec<BuildPayload> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..parts)
            .map(|pid| {
                let build_batches = &build_batches;
                let pids = &pids;
                let build_keys = spec.build_keys;
                let null_safe = spec.null_safe;
                s.spawn(move || -> Result<BuildPayload, QueryError> {
                    let mut columns: Vec<ColumnVector> =
                        (0..ncols).map(|_| ColumnVector::Null { len: 0 }).collect();
                    let mut index = BuildIndex::default();
                    let mut key_buf = Vec::with_capacity(64);
                    let mut keep: Vec<usize> = Vec::new();
                    let mut next_row_idx: u32 = 0;
                    for (bi, batch) in build_batches.iter().enumerate() {
                        keep.clear();
                        let batch_pids = &pids[bi];
                        for r in 0..batch.num_rows() {
                            if batch_pids[r] != pid as u8 {
                                continue;
                            }
                            // Rows marked by the scatter always encode successfully.
                            if !encode_join_key_with_collations(
                                &batch,
                                build_keys,
                                null_safe,
                                spec.key_collations,
                                r,
                                &mut key_buf,
                            ) {
                                continue;
                            }
                            index
                                .entry_ref(key_buf.as_slice())
                                .or_default()
                                .push(next_row_idx);
                            keep.push(r);
                            next_row_idx = next_row_idx.checked_add(1).expect("build row overflow");
                        }
                        if !keep.is_empty() {
                            for (c, col) in columns.iter_mut().enumerate() {
                                col.append_rows_from(&batch.columns()[c], &keep);
                            }
                        }
                    }
                    Ok(BuildPayload {
                        columns,
                        index,
                        i64_index: None,
                        i64_pair_index: None,
                    })
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("partition build worker panicked"))
            .collect::<Result<Vec<_>, QueryError>>()
    })?;

    // --- Serial probe routing: encode each row once → partition id → lookup in the matching partition.
    let mut out: Vec<Batch> = Vec::new();
    if payloads.iter().all(|p| p.index.is_empty()) {
        return Ok(out);
    }
    let mut key_buf = Vec::with_capacity(64);
    let mut visible: Vec<usize> = Vec::new();
    let mut pair_build: Vec<Vec<u32>> = vec![Vec::new(); parts];
    let mut pair_probe: Vec<Vec<u32>> = vec![Vec::new(); parts];
    while let Some(batch) = probe.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        fill_visible(&batch, &mut visible);
        for &r in &visible {
            if !encode_join_key_with_collations(
                &batch,
                spec.probe_keys,
                spec.null_safe,
                spec.key_collations,
                r,
                &mut key_buf,
            ) {
                continue;
            }
            let pid = (super::key_encode::fxhash(&key_buf) % parts as u64) as usize;
            let Some(hits) = payloads[pid].index.get(&key_buf) else {
                continue;
            };
            for &b in hits.iter() {
                pair_build[pid].push(b);
                pair_probe[pid].push(r as u32);
            }
            if pair_build[pid].len() >= DEFAULT_BATCH_ROWS {
                flush_inner(
                    &payloads[pid].columns,
                    &batch,
                    &mut pair_build[pid],
                    &mut pair_probe[pid],
                    spec,
                    &mut |batch| {
                        out.push(batch);
                        Ok(())
                    },
                )?;
            }
        }
        for pid in 0..parts {
            if !pair_build[pid].is_empty() {
                flush_inner(
                    &payloads[pid].columns,
                    &batch,
                    &mut pair_build[pid],
                    &mut pair_probe[pid],
                    spec,
                    &mut |batch| {
                        out.push(batch);
                        Ok(())
                    },
                )?;
            }
        }
    }
    Ok(out)
}

/// Probe-preserved outer join (LEFT with build=right, or RIGHT with build=left):
/// every probe row is output at least once; when nothing matches (or the residual rejects
/// everything), NULL is injected into the build columns. The residual must be resolved per pair
/// **before** the NULL-extension decision, so unlike INNER's Selection mask, the candidate batch
/// is gathered first, filtered, then gathered a second time into the final batch.
pub fn run_preserved_probe_hash_join(
    build: &mut dyn BatchOperator,
    probe: &mut dyn BatchOperator,
    spec: &VecJoinSpec<'_>,
) -> Result<Vec<Batch>, QueryError> {
    assert_eq!(
        spec.build_keys.len(),
        spec.probe_keys.len(),
        "key arity mismatch"
    );
    let payload = collect_build_payload_with_collations(
        build,
        spec.build_keys,
        spec.null_safe,
        spec.key_collations,
    )?;
    let mut out: Vec<Batch> = Vec::new();
    let mut kernel = ProbeKernel::new(
        spec.probe_keys,
        spec.null_safe,
        spec.key_collations,
        &payload.index,
    );
    let mut pair_build: Vec<u32> = Vec::new();
    let mut pair_probe: Vec<u32> = Vec::new();
    // Candidate buffers for the residual path: candidate pairs + a candidate range per probe row.
    let mut cand_build: Vec<u32> = Vec::new();
    let mut cand_probe: Vec<u32> = Vec::new();
    let mut row_ranges: Vec<(u32, u32, u32)> = Vec::new(); // (probe_row, start, end)
    while let Some(batch) = probe.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        pair_build.clear();
        pair_probe.clear();
        match spec.residual {
            None => {
                let mut sink = MaterializedOuterSink {
                    build_columns: &payload.columns,
                    probe_batch: &batch,
                    build_cols_first: spec.build_cols_first,
                    out_schema: &spec.out_schema,
                    out: &mut out,
                    pair_build: &mut pair_build,
                    pair_probe: &mut pair_probe,
                };
                kernel.probe_batch(&payload.index, &batch, &mut sink)?;
            }
            Some(program) => {
                // Collect candidates (with per-row ranges) and resolve them all at batch end.
                cand_build.clear();
                cand_probe.clear();
                row_ranges.clear();
                let mut sink = CandidateRangeSink {
                    candidate_build: &mut cand_build,
                    candidate_probe: &mut cand_probe,
                    row_ranges: &mut row_ranges,
                };
                kernel.probe_batch(&payload.index, &batch, &mut sink)?;
                let mask = if cand_build.is_empty() {
                    None
                } else {
                    let cand_batch = gather_pairs_batch(
                        &payload.columns,
                        &batch,
                        &cand_build,
                        &cand_probe,
                        spec.build_cols_first,
                        &spec.out_schema,
                    );
                    Some(program.eval_predicate_bitmap(&cand_batch)?)
                };
                for &(r, start, end) in &row_ranges {
                    let mut matched = false;
                    if let Some(mask) = mask.as_ref() {
                        for i in start..end {
                            if mask.get(i as usize) {
                                pair_build.push(cand_build[i as usize]);
                                pair_probe.push(r);
                                matched = true;
                            }
                        }
                    }
                    if !matched {
                        pair_build.push(NULL_BUILD);
                        pair_probe.push(r);
                    }
                }
            }
        }
        if spec.residual.is_some() && !pair_build.is_empty() {
            out.push(gather_pairs_batch(
                &payload.columns,
                &batch,
                &pair_build,
                &pair_probe,
                spec.build_cols_first,
                &spec.out_schema,
            ));
        }
    }
    Ok(out)
}

/// General outer hash join used when the build side may itself be preserved.
/// Both sides remain columnar: matched pairs and the selected NULL-extension
/// passes are represented by gather indices, with `u32::MAX` as the NULL
/// sentinel. A build match is recorded only after the residual predicate
/// accepts the concrete pair.
#[cfg(test)]
fn run_outer_hash_join(
    build: &mut dyn BatchOperator,
    probe: &mut dyn BatchOperator,
    spec: &VecJoinSpec<'_>,
    preserve_probe: bool,
    preserve_build: bool,
) -> Result<Vec<Batch>, QueryError> {
    let mut out = Vec::new();
    run_outer_hash_join_into(
        build,
        probe,
        spec,
        preserve_probe,
        preserve_build,
        DEFAULT_BATCH_ROWS,
        &mut |batch| {
            out.push(batch);
            Ok(())
        },
    )?;
    Ok(out)
}

/// Stream outer-join output while preserving residual and NULL-extension
/// semantics. Grace replay must not retain a partition's complete output:
/// bounded build state does not bound duplicate-key join cardinality.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_outer_hash_join_into(
    build: &mut dyn BatchOperator,
    probe: &mut dyn BatchOperator,
    spec: &VecJoinSpec<'_>,
    preserve_probe: bool,
    preserve_build: bool,
    output_batch_rows: usize,
    emit: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    assert_eq!(
        spec.build_keys.len(),
        spec.probe_keys.len(),
        "key arity mismatch"
    );
    let probe_schema = probe.schema().clone();
    let payload =
        collect_full_build_payload(build, spec.build_keys, spec.null_safe, spec.key_collations)?;
    let build_rows = payload.columns.first().map(ColumnVector::len).unwrap_or(0);
    let mut matched_build = vec![false; build_rows];
    let output_batch_rows = output_batch_rows.clamp(1, DEFAULT_BATCH_ROWS);
    let mut key_buf = Vec::with_capacity(64);
    let mut visible = Vec::new();
    let mut pair_build = Vec::with_capacity(output_batch_rows);
    let mut pair_probe = Vec::with_capacity(output_batch_rows);

    while let Some(batch) = probe.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        fill_visible(&batch, &mut visible);
        for &probe_row in &visible {
            let usable = encode_join_key_with_collations(
                &batch,
                spec.probe_keys,
                spec.null_safe,
                spec.key_collations,
                probe_row,
                &mut key_buf,
            );
            let hits = usable
                .then(|| payload.index.get(key_buf.as_slice()))
                .flatten();
            let mut probe_matched = false;
            if let Some(hits) = hits {
                for hit_chunk in hits.chunks(output_batch_rows) {
                    crate::engine::cancel::check_cancelled()?;
                    let probe_indices = vec![probe_row as u32; hit_chunk.len()];
                    let accepted = if let Some(program) = spec.residual {
                        let candidates = gather_pairs_batch(
                            &payload.columns,
                            &batch,
                            hit_chunk,
                            &probe_indices,
                            spec.build_cols_first,
                            &spec.out_schema,
                        );
                        Some(program.eval_predicate_bitmap(&candidates)?)
                    } else {
                        None
                    };
                    for (position, &build_row) in hit_chunk.iter().enumerate() {
                        if accepted.as_ref().is_some_and(|mask| !mask.get(position)) {
                            continue;
                        }
                        probe_matched = true;
                        matched_build[build_row as usize] = true;
                        pair_build.push(build_row);
                        pair_probe.push(probe_row as u32);
                        if pair_build.len() >= output_batch_rows {
                            emit(gather_pairs_batch(
                                &payload.columns,
                                &batch,
                                &pair_build,
                                &pair_probe,
                                spec.build_cols_first,
                                &spec.out_schema,
                            ))?;
                            pair_build.clear();
                            pair_probe.clear();
                        }
                    }
                }
            }
            if preserve_probe && !probe_matched {
                pair_build.push(NULL_BUILD);
                pair_probe.push(probe_row as u32);
                if pair_build.len() >= output_batch_rows {
                    emit(gather_pairs_batch(
                        &payload.columns,
                        &batch,
                        &pair_build,
                        &pair_probe,
                        spec.build_cols_first,
                        &spec.out_schema,
                    ))?;
                    pair_build.clear();
                    pair_probe.clear();
                }
            }
        }
        if !pair_build.is_empty() {
            emit(gather_pairs_batch(
                &payload.columns,
                &batch,
                &pair_build,
                &pair_probe,
                spec.build_cols_first,
                &spec.out_schema,
            ))?;
            pair_build.clear();
            pair_probe.clear();
        }
    }

    // No probe values are read for this pass; NULL sentinels cause every
    // probe column to manufacture a typed NULL lane.
    let null_probe = Batch::with_row_count(
        probe_schema.clone(),
        probe_schema
            .fields()
            .iter()
            .map(|_| ColumnVector::Null { len: 0 })
            .collect(),
        0,
    );
    if preserve_build {
        for (build_row, matched) in matched_build.into_iter().enumerate() {
            if matched {
                continue;
            }
            pair_build.push(build_row as u32);
            pair_probe.push(NULL_BUILD);
            if pair_build.len() >= output_batch_rows {
                emit(gather_pairs_batch(
                    &payload.columns,
                    &null_probe,
                    &pair_build,
                    &pair_probe,
                    spec.build_cols_first,
                    &spec.out_schema,
                ))?;
                pair_build.clear();
                pair_probe.clear();
            }
        }
    }
    if !pair_build.is_empty() {
        emit(gather_pairs_batch(
            &payload.columns,
            &null_probe,
            &pair_build,
            &pair_probe,
            spec.build_cols_first,
            &spec.out_schema,
        ))?;
    }
    Ok(())
}

/// FULL OUTER hash join: preserve both input sides.
#[cfg(test)]
pub fn run_full_hash_join(
    build: &mut dyn BatchOperator,
    probe: &mut dyn BatchOperator,
    spec: &VecJoinSpec<'_>,
) -> Result<Vec<Batch>, QueryError> {
    run_outer_hash_join(build, probe, spec, true, true)
}

/// LEFT/RIGHT hash join whose CBO-selected build side is the SQL-preserved
/// side. This is semantically distinct from the usual preserved-probe layout
/// but shares the same pair/residual/NULL-extension kernel.
#[cfg(test)]
pub fn run_preserved_build_hash_join(
    build: &mut dyn BatchOperator,
    probe: &mut dyn BatchOperator,
    spec: &VecJoinSpec<'_>,
) -> Result<Vec<Batch>, QueryError> {
    run_outer_hash_join(build, probe, spec, false, true)
}

/// SEMI / ANTI hash join. The output contains only the probe (= SQL left) columns.
///
/// - Without residual: a key-only `HashSet`, zero build payload;
/// - With residual: `residual` is a program compiled against the `[probe cols, build cols]`
///   candidate order (`cand_schema`); a probe row's verdict = some candidate pair passes
///   (SEMI) / no candidate pair passes (ANTI).
pub fn run_semi_anti_hash_join(
    build: &mut dyn BatchOperator,
    probe: &mut dyn BatchOperator,
    build_keys: &[usize],
    probe_keys: &[usize],
    key_collations: &[Option<u16>],
    null_safe: &[bool],
    is_anti: bool,
    out_schema: Arc<BatchSchema>,
    residual: Option<(&BatchExprProgram, Arc<BatchSchema>)>,
) -> Result<Vec<Batch>, QueryError> {
    let mut out = Vec::new();
    run_semi_anti_hash_join_into(
        build,
        probe,
        build_keys,
        probe_keys,
        key_collations,
        null_safe,
        is_anti,
        out_schema,
        residual,
        &mut |batch| {
            out.push(batch);
            Ok(())
        },
    )?;
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_semi_anti_hash_join_into(
    build: &mut dyn BatchOperator,
    probe: &mut dyn BatchOperator,
    build_keys: &[usize],
    probe_keys: &[usize],
    key_collations: &[Option<u16>],
    null_safe: &[bool],
    is_anti: bool,
    out_schema: Arc<BatchSchema>,
    residual: Option<(&BatchExprProgram, Arc<BatchSchema>)>,
    emit: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    assert_eq!(build_keys.len(), probe_keys.len(), "key arity mismatch");

    let mut key_buf = Vec::with_capacity(64);
    let mut visible: Vec<usize> = Vec::new();
    let mut sel: Vec<u32> = Vec::new();
    match residual {
        None => {
            let set =
                collect_build_keyset_with_collations(build, build_keys, null_safe, key_collations)?;
            while let Some(batch) = probe.next_batch()? {
                crate::engine::cancel::check_cancelled()?;
                fill_visible(&batch, &mut visible);
                sel.clear();
                for &r in &visible {
                    let usable = encode_join_key_with_collations(
                        &batch,
                        probe_keys,
                        null_safe,
                        key_collations,
                        r,
                        &mut key_buf,
                    );
                    let matched = usable && set.contains(key_buf.as_slice());
                    if matched != is_anti {
                        sel.push(r as u32);
                    }
                }
                if !sel.is_empty() {
                    emit(Batch::new(
                        out_schema.clone(),
                        batch
                            .columns()
                            .iter()
                            .map(|c| c.take_indices(&sel))
                            .collect(),
                    ))?;
                }
            }
        }
        Some((program, cand_schema)) => {
            let payload = collect_build_payload_with_collations(
                build,
                build_keys,
                null_safe,
                key_collations,
            )?;
            while let Some(batch) = probe.next_batch()? {
                crate::engine::cancel::check_cancelled()?;
                sel.clear();
                for r in batch.visible_indices() {
                    let usable = encode_join_key_with_collations(
                        &batch,
                        probe_keys,
                        null_safe,
                        key_collations,
                        r,
                        &mut key_buf,
                    );
                    let mut matched = false;
                    if let Some(hits) = usable
                        .then(|| payload.index.get(key_buf.as_slice()))
                        .flatten()
                    {
                        for chunk in hits.chunks(1024) {
                            crate::engine::cancel::check_cancelled()?;
                            let probes = vec![r as u32; chunk.len()];
                            let candidates = gather_pairs_batch(
                                &payload.columns,
                                &batch,
                                chunk,
                                &probes,
                                false,
                                &cand_schema,
                            );
                            if program.eval_predicate_bitmap(&candidates)?.count_ones() != 0 {
                                matched = true;
                                break;
                            }
                        }
                    }
                    if matched != is_anti {
                        sel.push(r as u32);
                    }
                }
                if !sel.is_empty() {
                    emit(Batch::new(
                        out_schema.clone(),
                        batch
                            .columns()
                            .iter()
                            .map(|c| c.take_indices(&sel))
                            .collect(),
                    ))?;
                }
            }
        }
    }
    Ok(())
}

/// Flatten columnar join output into rows (honoring Selection). The bridge's row-wise consumers
/// and unit tests use it as the boundary conversion.
#[cfg(test)]
pub fn flatten_batches_to_rows(batches: Vec<Batch>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for batch in batches {
        let ncols = batch.columns().len();
        let mut emit = |r: usize| {
            let mut row = Vec::with_capacity(ncols);
            for c in 0..ncols {
                row.push(batch.columns()[c].value_at(r));
            }
            rows.push(row);
        };
        for row in batch.visible_indices() {
            emit(row);
        }
    }
    rows
}

#[inline]
pub(crate) fn schema_column_uses_i64_lane(schema: &BatchSchema, column: usize) -> bool {
    let Some(field) = schema.fields().get(column) else {
        return false;
    };
    let non_i64_flags = crate::mysql::protocol::UNSIGNED_FLAG
        | crate::mysql::protocol::ENUM_FLAG
        | crate::mysql::protocol::SET_FLAG;
    field.source.flags & non_i64_flags == 0
        && matches!(
            field.column_type,
            crate::types::ColumnType::Tiny
                | crate::types::ColumnType::Short
                | crate::types::ColumnType::MediumInt
                | crate::types::ColumnType::Long
                | crate::types::ColumnType::LongLong
        )
}

/// Reusable ordinary-equality probe kernel. Owns scratch buffers, never inputs
/// or lifecycle; the bridge's single hash-join operator owns those resources.
pub(crate) struct StreamingProbe {
    probe_kernel: ProbeKernel,
    build_i64_unique: bool,
    pair_build: Vec<u32>,
    pair_probe: Vec<u32>,
    pending_probe: Option<Batch>,
    pending_rows: Vec<usize>,
    pending_position: usize,
    pending_hit: usize,
}
impl StreamingProbe {
    pub(crate) fn new(payload: &BuildPayload, keys: &[usize], collations: &[Option<u16>]) -> Self {
        Self {
            probe_kernel: ProbeKernel::new(
                keys,
                &vec![false; keys.len()],
                collations,
                &payload.index,
            ),
            build_i64_unique: payload
                .i64_index
                .as_ref()
                .is_some_and(I64BuildIndex::all_unique),
            pair_build: Vec::new(),
            pair_probe: Vec::new(),
            pending_probe: None,
            pending_rows: Vec::new(),
            pending_position: 0,
            pending_hit: 0,
        }
    }
    pub(crate) fn process_batch(
        &mut self,
        payload: &BuildPayload,
        batch: Batch,
        build_is_left: bool,
        preserve_probe: bool,
        out_schema: &Arc<BatchSchema>,
        pending: &mut JoinOutputQueue,
    ) -> Result<(), QueryError> {
        // A unique native integer build produces at most one output row per
        // probe row. When the probe batch has no selection, keep one aligned
        // build-row vector (using NULL_BUILD for misses) and transfer all
        // probe columns directly into the output. This removes both the
        // `(build, probe)` pair construction and the identity gather of every
        // probe column. Inner joins with misses compact back to the ordinary
        // pair gather, preserving exact semantics for arbitrary key domains.
        let batch_rows = batch.num_rows();
        let native_unique_column = payload
            .i64_index
            .as_ref()
            .zip(self.probe_kernel.probe_keys.first())
            .and_then(|(index, &column)| {
                (self.build_i64_unique && batch.selection().is_none())
                    .then(|| {
                        batch.columns()[column]
                            .as_i64_slice()
                            .map(|values| (index, column, values))
                    })
                    .flatten()
            });
        if let Some((index, column, values)) = native_unique_column {
            self.pair_build.clear();
            self.pair_build.reserve(batch_rows);
            let mut match_rows = 0u64;
            let mut all_matched = true;
            for row in 0..batch_rows {
                if row > 0 && row & 1023 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                let hit = (!batch.columns()[column].is_null(row))
                    .then(|| index.get(&values[row]))
                    .flatten();
                if let Some(hits) = hit {
                    debug_assert_eq!(hits.len(), 1, "unique i64 build index returned duplicates");
                    self.pair_build.push(hits[0]);
                    match_rows = match_rows.saturating_add(1);
                } else {
                    self.pair_build.push(NULL_BUILD);
                    all_matched = false;
                    crate::engine::executor::select::analyze_stats::add_pending_runtime_filter_rejected(1);
                }
            }

            if all_matched || preserve_probe {
                let out = gather_identity_probe_batch(
                    &payload.columns,
                    batch,
                    &self.pair_build,
                    build_is_left,
                    out_schema,
                )?;
                self.pair_build.clear();
                pending.push_back(out)?;
            } else {
                // INNER join misses remove rows, so construct compact pairs
                // only for this less common fallback. Reuse `pair_build` in
                // place to retain its governed capacity across batches.
                self.pair_probe.clear();
                let mut write = 0usize;
                for probe_row in 0..self.pair_build.len() {
                    let build_row = self.pair_build[probe_row];
                    if build_row != NULL_BUILD {
                        self.pair_build[write] = build_row;
                        self.pair_probe.push(probe_row as u32);
                        write += 1;
                    }
                }
                self.pair_build.truncate(write);
                if write != 0 {
                    let out = gather_pairs_batch(
                        &payload.columns,
                        &batch,
                        &self.pair_build,
                        &self.pair_probe,
                        build_is_left,
                        out_schema,
                    );
                    pending.push_back(out)?;
                }
                self.pair_build.clear();
                self.pair_probe.clear();
            }
            crate::engine::executor::select::analyze_stats::add_pending_join_runtime(
                0,
                batch_rows as u64,
                match_rows,
            );
            return Ok(());
        }

        debug_assert!(self.pending_probe.is_none());
        fill_visible(&batch, &mut self.pending_rows);
        self.pending_position = 0;
        self.pending_hit = 0;
        crate::engine::executor::select::analyze_stats::add_pending_join_runtime(
            0,
            batch.visible_rows() as u64,
            0,
        );
        self.pending_probe = Some(batch);
        self.resume_batch(payload, build_is_left, preserve_probe, out_schema, pending)?;
        Ok(())
    }

    /// Emit at most one output batch, retaining the current probe row and
    /// bucket offset. A duplicate-key bucket can be arbitrarily larger than
    /// the consumer's LIMIT, so queuing all its matches defeats streaming.
    pub(crate) fn resume_batch(
        &mut self,
        payload: &BuildPayload,
        build_is_left: bool,
        preserve_probe: bool,
        out_schema: &Arc<BatchSchema>,
        pending: &mut JoinOutputQueue,
    ) -> Result<bool, QueryError> {
        let Some(batch) = self.pending_probe.as_ref() else {
            return Ok(false);
        };
        self.pair_build.clear();
        self.pair_probe.clear();
        let mut matches = 0u64;
        let native = payload
            .i64_index
            .as_ref()
            .zip(self.probe_kernel.probe_keys.first().copied())
            .and_then(|(index, column)| {
                batch.columns()[column]
                    .as_i64_slice()
                    .map(|values| (index, column, values))
            });
        let mut visited = 0usize;
        while self.pending_position < self.pending_rows.len()
            && self.pair_build.len() < DEFAULT_BATCH_ROWS
        {
            if visited & 1023 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            visited += 1;
            let row = self.pending_rows[self.pending_position];
            let hits = if let Some((index, column, values)) = native {
                let hits = (!batch.columns()[column].is_null(row))
                    .then(|| index.get(&values[row]))
                    .flatten();
                if self.pending_hit == 0 && hits.is_none() {
                    crate::engine::executor::select::analyze_stats::add_pending_runtime_filter_rejected(1);
                }
                hits
            } else if self.pending_hit > 0 {
                payload
                    .index
                    .get(self.probe_kernel.key_buf.as_slice())
                    .map(SmallVec::as_slice)
            } else {
                self.probe_kernel.lookup_row(&payload.index, batch, row)
            };
            if let Some(hits) = hits {
                let count =
                    (DEFAULT_BATCH_ROWS - self.pair_build.len()).min(hits.len() - self.pending_hit);
                self.pair_build
                    .extend_from_slice(&hits[self.pending_hit..self.pending_hit + count]);
                self.pair_probe
                    .extend(std::iter::repeat_n(row as u32, count));
                matches += count as u64;
                self.pending_hit += count;
                if self.pending_hit < hits.len() {
                    break;
                }
            } else if preserve_probe {
                self.pair_build.push(NULL_BUILD);
                self.pair_probe.push(row as u32);
            }
            self.pending_position += 1;
            self.pending_hit = 0;
        }
        if !self.pair_build.is_empty() {
            pending.push_back(gather_pairs_batch(
                &payload.columns,
                batch,
                &self.pair_build,
                &self.pair_probe,
                build_is_left,
                out_schema,
            ))?;
        }
        if self.pending_position == self.pending_rows.len() {
            self.pending_probe = None;
        }
        crate::engine::executor::select::analyze_stats::add_pending_join_runtime(0, 0, matches);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::select::vec_bridge::HashJoinBatchOperator;
    use crate::engine::executor::vec::batch::{Batch, ColumnVector};
    use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
    use crate::engine::executor::vec::stream::MaterializedBatchOperator;
    use crate::types::ColumnType;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::sync::Arc;

    #[test]
    fn sqlaudit_duplicate_probe_yields_one_batch_and_resumes_all_key_domains() {
        let count = DEFAULT_BATCH_ROWS * 2 + 3;
        for (column_type, key, probe_key, miss, collation) in [
            (
                ColumnType::LongLong,
                Value::Int(1),
                Value::Int(1),
                Value::Int(2),
                None,
            ),
            (
                ColumnType::Double,
                Value::Double(1.5),
                Value::Double(1.5),
                Value::Double(2.5),
                None,
            ),
            (
                ColumnType::Decimal {
                    precision: 12,
                    scale: 2,
                },
                Value::DecimalCompact(150, 2),
                Value::DecimalCompact(150, 2),
                Value::DecimalCompact(250, 2),
                None,
            ),
            (
                ColumnType::VarString,
                Value::String("x".into()),
                Value::String("X".into()),
                Value::String("z".into()),
                Some(45),
            ),
        ] {
            let fields = |table: &str| {
                vec![
                    BatchField {
                        table: table.into(),
                        alias: None,
                        name: "k".into(),
                        column_type,
                        source: Default::default(),
                    },
                    BatchField {
                        table: table.into(),
                        alias: None,
                        name: "v".into(),
                        column_type: ColumnType::LongLong,
                        source: Default::default(),
                    },
                ]
            };
            let build_schema = Arc::new(BatchSchema::new(fields("b")));
            let build = Batch::new(
                build_schema,
                vec![
                    ColumnVector::from_values(vec![key; count]),
                    ColumnVector::from_values((0..count).map(|i| Value::Int(i as i64)).collect()),
                ],
            );
            let mut index = BuildIndex::with_hasher(ahash::RandomState::new());
            let mut encoded = Vec::new();
            for row in 0..count {
                encode_join_key_with_collations(
                    &build,
                    &[0],
                    &[false],
                    &[collation],
                    row,
                    &mut encoded,
                );
                index.entry(encoded.clone()).or_default().push(row as u32);
            }
            let payload = BuildPayload {
                columns: build.columns().to_vec(),
                index,
                i64_index: build_i64_index(build.columns(), &[0], &[false], &[collation]),
                i64_pair_index: None,
            };
            let probe = Batch::new(
                Arc::new(BatchSchema::new(fields("p"))),
                vec![
                    ColumnVector::from_values(vec![
                        probe_key.clone(),
                        miss,
                        Value::Null,
                        probe_key,
                    ]),
                    ColumnVector::from_values(vec![
                        Value::Int(100),
                        Value::Int(101),
                        Value::Int(102),
                        Value::Int(103),
                    ]),
                ],
            )
            .with_selection(Selection::Indices(vec![0, 1, 2, 3]));
            let schema = Arc::new(BatchSchema::new(
                fields("b").into_iter().chain(fields("p")).collect(),
            ));
            for preserve in [false, true] {
                let mut kernel = StreamingProbe::new(&payload, &[0], &[collation]);
                let mut output = JoinOutputQueue::new();
                kernel
                    .process_batch(
                        &payload,
                        probe.clone(),
                        true,
                        preserve,
                        &schema,
                        &mut output,
                    )
                    .unwrap();
                let first = output.pop_front().unwrap().unwrap();
                assert_eq!(first.num_rows(), DEFAULT_BATCH_ROWS);
                assert!(output.is_empty());
                assert!(kernel.pending_probe.is_some());
                assert_eq!(kernel.pending_position, 0);
                let mut rows = (0..first.num_rows())
                    .map(|row| {
                        (
                            first.columns()[1].value_at(row).to_display_string(),
                            first.columns()[3].value_at(row).to_display_string(),
                        )
                    })
                    .collect::<Vec<_>>();
                while kernel
                    .resume_batch(&payload, true, preserve, &schema, &mut output)
                    .unwrap()
                {
                    if let Some(batch) = output.pop_front().unwrap() {
                        assert!(batch.num_rows() <= DEFAULT_BATCH_ROWS);
                        rows.extend((0..batch.num_rows()).map(|row| {
                            (
                                batch.columns()[1].value_at(row).to_display_string(),
                                batch.columns()[3].value_at(row).to_display_string(),
                            )
                        }));
                    }
                    assert!(output.is_empty());
                }
                let mut expected = (0..count)
                    .flat_map(|row| {
                        [
                            (row.to_string(), "100".to_string()),
                            (row.to_string(), "103".to_string()),
                        ]
                    })
                    .collect::<Vec<_>>();
                if preserve {
                    expected.extend([("NULL".into(), "101".into()), ("NULL".into(), "102".into())]);
                }
                rows.sort();
                expected.sort();
                assert_eq!(rows, expected, "{column_type:?}");
            }
        }
    }

    struct RuntimeFilterProbe {
        schema: Arc<BatchSchema>,
        installed: Rc<Cell<bool>>,
    }

    struct CapturingRuntimeFilterProbe {
        schema: Arc<BatchSchema>,
        installed: Rc<RefCell<Vec<crate::planner::RuntimeFilter>>>,
    }

    impl BatchOperator for CapturingRuntimeFilterProbe {
        fn schema(&self) -> &Arc<BatchSchema> {
            &self.schema
        }

        fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
            self.installed.borrow_mut().push(filter);
            true
        }

        fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
            Ok(None)
        }
    }

    impl BatchOperator for RuntimeFilterProbe {
        fn schema(&self) -> &Arc<BatchSchema> {
            &self.schema
        }

        fn install_runtime_filter(&mut self, _filter: crate::planner::RuntimeFilter) -> bool {
            self.installed.set(true);
            true
        }

        fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
            Ok(None)
        }
    }

    #[test]
    fn payload_probe_lanes_match_encoded_lookup_with_nulls_and_duplicates() {
        let schema = schema_kv();
        let columns = vec![
            ColumnVector::from_values(vec![
                Value::Int(1),
                Value::Int(1),
                Value::Null,
                Value::Int(-7),
            ]),
            ColumnVector::from_values(vec![
                Value::Int(2),
                Value::Int(2),
                Value::Int(3),
                Value::Null,
            ]),
        ];
        let build = Batch::new(schema.clone(), columns.clone());
        let probe = Batch::new(
            schema,
            vec![
                ColumnVector::from_values(vec![
                    Value::Int(1),
                    Value::Null,
                    Value::Int(-7),
                    Value::Int(999),
                ]),
                ColumnVector::from_values(vec![
                    Value::Int(2),
                    Value::Int(3),
                    Value::Null,
                    Value::Int(2),
                ]),
            ],
        );
        for keys in [vec![0], vec![0, 1]] {
            for null_equal in [false, true] {
                let null_safe = vec![null_equal; keys.len()];
                let mut oracle = BuildIndex::with_hasher(ahash::RandomState::new());
                let mut key = Vec::new();
                for row in 0..build.num_rows() {
                    if encode_join_key_with_collations(
                        &build,
                        &keys,
                        &null_safe,
                        &[],
                        row,
                        &mut key,
                    ) {
                        oracle.entry(key.clone()).or_default().push(row as u32);
                    }
                }
                for typed in [false, true] {
                    let payload = BuildPayload {
                        columns: columns.clone(),
                        index: oracle.clone(),
                        i64_index: typed
                            .then(|| build_i64_index(&columns, &keys, &null_safe, &[]))
                            .flatten(),
                        i64_pair_index: typed
                            .then(|| build_i64_pair_index(&columns, &keys, &null_safe, &[]))
                            .flatten(),
                    };
                    let lookup = PayloadProbe::new(&payload, &probe, &keys, &null_safe, &[]);
                    for row in 0..probe.num_rows() {
                        let usable = encode_join_key_with_collations(
                            &probe,
                            &keys,
                            &null_safe,
                            &[],
                            row,
                            &mut key,
                        );
                        let expected = if usable {
                            oracle.get(&key).map(SmallVec::as_slice)
                        } else {
                            None
                        };
                        let actual = lookup.lookup(row, &mut Vec::new());
                        assert_eq!(actual, (usable, expected));
                    }
                }
            }
        }
    }

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

    fn joined_schema(s: &Arc<BatchSchema>) -> Arc<BatchSchema> {
        let mut fields = s.fields().to_vec();
        fields.extend(s.fields().to_vec());
        Arc::new(BatchSchema::new(fields))
    }

    fn batch(s: &Arc<BatchSchema>, pairs: &[(i64, i64)]) -> Batch {
        let ks: Vec<Value> = pairs.iter().map(|(k, _)| Value::Int(*k)).collect();
        let vs: Vec<Value> = pairs.iter().map(|(_, v)| Value::Int(*v)).collect();
        Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(ks), ColumnVector::from_values(vs)],
        )
    }

    fn run_inner_rows(
        build: &mut dyn BatchOperator,
        probe: &mut dyn BatchOperator,
        out_schema: Arc<BatchSchema>,
    ) -> Vec<Vec<Value>> {
        let spec = VecJoinSpec {
            build_keys: &[0],
            probe_keys: &[0],
            key_collations: &[],
            null_safe: &[false],
            build_cols_first: true,
            out_schema,
            residual: None,
        };
        flatten_batches_to_rows(run_inner_hash_join(build, probe, &spec).unwrap())
    }

    #[test]
    fn streaming_hot_fanout_spills_and_bounds_each_output_batch() {
        std::fs::create_dir_all("/tmp/mydb/join-fix").unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb/join-fix").unwrap();
        let s = schema_kv();
        let n = DEFAULT_BATCH_ROWS * 2 + 3;
        let mut build =
            MaterializedBatchOperator::new(s.clone(), vec![batch(&s, &vec![(1, 7); n])]);
        let probe =
            MaterializedBatchOperator::new(s.clone(), vec![batch(&s, &[(1, 9), (1, 9), (2, 10)])]);
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
            32 * 1024 * 1024,
        ));
        crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
            let mut stream = HashJoinBatchOperator::for_test(
                &mut build,
                Box::new(probe),
                &[0],
                &[0],
                &[],
                joined_schema(&s),
                true,
                VecJoinType::Left,
            )
            .unwrap();
            stream.set_output_data_dir(root.path());
            let mut rows = 0;
            let mut unmatched = 0;
            while let Some(batch) = stream.next_batch().unwrap() {
                assert!(batch.num_rows() <= DEFAULT_BATCH_ROWS);
                for r in batch.visible_indices() {
                    if batch.columns()[0].is_null(r) {
                        unmatched += 1;
                    }
                }
                rows += batch.visible_rows();
            }
            assert_eq!(rows, 2 * n + 1);
            assert_eq!(unmatched, 1);
        });
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
    }

    #[test]
    fn full_native_pair_payload_keeps_null_rows_without_a_duplicate_byte_index() {
        let schema = schema_kv();
        let input = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::from_values(vec![
                    Value::Int(1),
                    Value::Int(1),
                    Value::Null,
                    Value::Int(2),
                    Value::Null,
                ]),
                ColumnVector::from_values(vec![
                    Value::Int(2),
                    Value::Int(2),
                    Value::Int(9),
                    Value::Null,
                    Value::Null,
                ]),
            ],
        );
        let expected = input.clone();
        let mut build = MaterializedBatchOperator::new(schema, vec![input]);
        let payload = collect_full_build_payload_i64_pair_only(&mut build, &[0, 1], 2).unwrap();
        assert!(payload.index.is_empty());
        let index = payload.i64_pair_index.as_ref().unwrap();
        assert_eq!(index.len(), 1);
        assert_eq!(index.get(&(1, 2)).unwrap().as_slice(), &[0, 1]);
        for row in 0..5 {
            assert_eq!(
                payload
                    .columns
                    .iter()
                    .map(|column| column.value_at(row))
                    .collect::<Vec<_>>(),
                expected.row(row)
            );
        }
    }

    #[test]
    fn bounded_i64_build_index_is_dense_and_preserves_duplicates() {
        let schema = schema_kv();
        let input = batch(&schema, &[(10, 1), (12, 2), (10, 3), (11, 4)]);
        let index = build_i64_index(input.columns(), &[0], &[false], &[])
            .expect("single integer key is indexable");
        assert!(matches!(index, I64BuildIndex::DenseMulti { .. }));
        assert_eq!(index.get(&10), Some(&[0, 2][..]));
        assert_eq!(index.get(&11), Some(&[3][..]));
        assert_eq!(index.get_unique(&11), Some(3));
        assert_eq!(index.get_unique(&10), None);
        assert!(index.get(&9).is_none());
        assert!(index.get(&13).is_none());
    }

    #[test]
    fn native_i64_pair_index_preserves_composite_duplicates() {
        let schema = schema_kv();
        let input = batch(&schema, &[(10, 7), (10, 8), (10, 7), (11, 7)]);
        let index = build_i64_pair_index(input.columns(), &[0, 1], &[false, false], &[])
            .expect("two signed integer keys are indexable");
        assert_eq!(
            index.get(&(10, 7)).map(SmallVec::as_slice),
            Some(&[0, 2][..])
        );
        assert_eq!(index.get(&(10, 8)).map(SmallVec::as_slice), Some(&[1][..]));
        assert_eq!(index.get(&(11, 7)).map(SmallVec::as_slice), Some(&[3][..]));
        assert!(index.get(&(7, 10)).is_none());
    }

    #[test]
    fn pair_only_collector_does_not_retain_encoded_index() {
        let schema = schema_kv();
        let input = batch(&schema, &[(1, 10), (2, 20), (1, 10)]);
        let mut build = MaterializedBatchOperator::new(schema, vec![input]);
        let payload =
            collect_build_payload_i64_pair_only(&mut build, &[0, 1], &[false, false], &[]).unwrap();
        assert!(payload.index.is_empty());
        assert!(payload.i64_index.is_none());
        assert_eq!(
            payload
                .i64_pair_index
                .as_ref()
                .and_then(|index| index.get(&(1, 10)))
                .map(SmallVec::as_slice),
            Some(&[0, 2][..])
        );
    }

    #[test]
    fn large_dense_unique_i64_build_index_uses_compact_direct_layout() {
        // One slot beyond the former two-million-key cutoff verifies that the
        // compact representation, rather than the old SmallVec footprint,
        // governs eligibility.
        let values: Vec<i64> = (0..=2_000_000).collect();
        let column = ColumnVector::I64 {
            data: values,
            nulls: None,
        };
        let index = build_i64_index(&[column], &[0], &[false], &[])
            .expect("single integer key is indexable");
        let I64BuildIndex::DenseUnique { entries, .. } = &index else {
            panic!("large dense unique domain should use direct addressing");
        };
        assert_eq!(entries.len(), 2_000_001);
        assert_eq!(index.get(&0), Some(&[0][..]));
        assert_eq!(index.get(&2_000_000), Some(&[2_000_000][..]));
        assert_eq!(index.get_unique(&0), Some(0));
        assert_eq!(index.get_unique(&2_000_000), Some(2_000_000));
        assert!(index.get(&-1).is_none());
        assert!(index.get(&i64::MIN).is_none());
        assert!(index.get(&i64::MAX).is_none());
    }

    #[test]
    fn inner_output_sink_bounds_hot_key_batches_and_propagates_failure() {
        let schema = schema_kv();
        let make_build = || {
            MaterializedBatchOperator::new(
                schema.clone(),
                vec![batch(
                    &schema,
                    &(0..1000).map(|value| (1, value)).collect::<Vec<_>>(),
                )],
            )
        };
        let make_probe = || {
            MaterializedBatchOperator::new(schema.clone(), vec![batch(&schema, &[(1, 9), (1, 10)])])
        };
        let spec = VecJoinSpec {
            build_keys: &[0],
            probe_keys: &[0],
            null_safe: &[false],
            key_collations: &[],
            build_cols_first: true,
            out_schema: joined_schema(&schema),
            residual: None,
        };
        let mut rows = 0;
        run_inner_hash_join_into(
            &mut make_build(),
            &mut make_probe(),
            &spec,
            17,
            &mut |batch| {
                assert!(batch.num_rows() <= 17);
                rows += batch.num_rows();
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(rows, 2000);
        let expected = crate::error::catalog::ER_GET_ERRNO.error("injected output failure");
        let error =
            run_inner_hash_join_into(&mut make_build(), &mut make_probe(), &spec, 17, &mut |_| {
                Err(expected.clone())
            })
            .unwrap_err();
        assert_eq!(error, expected);
    }

    #[test]
    fn outer_output_sink_bounds_fanout_and_null_extension_batches() {
        let schema = schema_kv();
        let mut build_rows = (0..1000).map(|value| (1, value)).collect::<Vec<_>>();
        build_rows.push((3, 30));
        let mut probe_rows = vec![(1, 9), (1, 10)];
        probe_rows.extend((0..35).map(|value| (2, value)));
        let make_build =
            || MaterializedBatchOperator::new(schema.clone(), vec![batch(&schema, &build_rows)]);
        let make_probe =
            || MaterializedBatchOperator::new(schema.clone(), vec![batch(&schema, &probe_rows)]);
        for (preserve_probe, preserve_build) in [(true, false), (false, true), (true, true)] {
            for build_cols_first in [true, false] {
                let spec = VecJoinSpec {
                    build_keys: &[0],
                    probe_keys: &[0],
                    null_safe: &[false],
                    key_collations: &[],
                    build_cols_first,
                    out_schema: joined_schema(&schema),
                    residual: None,
                };
                let mut count = 0;
                let mut unmatched_probe = 0;
                let mut unmatched_build = 0;
                run_outer_hash_join_into(
                    &mut make_build(),
                    &mut make_probe(),
                    &spec,
                    preserve_probe,
                    preserve_build,
                    17,
                    &mut |batch| {
                        assert!(batch.num_rows() <= 17);
                        for row in flatten_batches_to_rows(vec![batch]) {
                            let (build, probe) = if build_cols_first { (0, 2) } else { (2, 0) };
                            unmatched_probe += usize::from(row[build] == Value::Null);
                            unmatched_build += usize::from(row[probe] == Value::Null);
                            count += 1;
                        }
                        Ok(())
                    },
                )
                .unwrap();
                assert_eq!(
                    count,
                    2000 + usize::from(preserve_probe) * 35 + usize::from(preserve_build)
                );
                assert_eq!(unmatched_probe, usize::from(preserve_probe) * 35);
                assert_eq!(unmatched_build, usize::from(preserve_build));
                let expected = crate::error::catalog::ER_GET_ERRNO.error("injected output failure");
                let error = run_outer_hash_join_into(
                    &mut make_build(),
                    &mut make_probe(),
                    &spec,
                    preserve_probe,
                    preserve_build,
                    17,
                    &mut |_| Err(expected.clone()),
                )
                .unwrap_err();
                assert_eq!(error, expected);
            }
        }
    }

    #[test]
    fn streaming_i64_join_keeps_only_the_native_build_index() {
        let schema = schema_kv();
        let build_batch = batch(&schema, &[(1, 100), (2, 200), (3, 300)]);
        let probe_batch = batch(&schema, &[(3, 30), (1, 10), (9, 90)]);
        let mut build = MaterializedBatchOperator::new(schema.clone(), vec![build_batch]);
        let probe = MaterializedBatchOperator::new(schema.clone(), vec![probe_batch]);
        let mut stream = HashJoinBatchOperator::for_test(
            &mut build,
            Box::new(probe),
            &[0],
            &[0],
            &[],
            joined_schema(&schema),
            true,
            VecJoinType::Inner,
        )
        .unwrap();

        assert!(stream.test_payload().unwrap().index.is_empty());
        assert!(matches!(
            stream.test_payload().unwrap().i64_index.as_ref(),
            Some(I64BuildIndex::DenseUnique { .. })
        ));

        let mut output = Vec::new();
        while let Some(batch) = stream.next_batch().unwrap() {
            output.push(batch);
        }
        let rows = flatten_batches_to_rows(output);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int(3));
        assert_eq!(rows[0][2], Value::Int(3));
        assert_eq!(rows[1][0], Value::Int(1));
        assert_eq!(rows[1][2], Value::Int(1));
        assert!(
            stream.test_payload().is_none(),
            "EOF releases the entire build state"
        );
        assert!(stream.next_batch().unwrap().is_none());
    }

    #[test]
    fn composite_integer_runtime_filter_splits_into_short_circuit_components() {
        let schema = schema_kv();
        let pairs = (0..4_096)
            .map(|key| (key as i64, (key as i64) * 17))
            .collect::<Vec<_>>();
        let build_batch = batch(&schema, &pairs);
        let mut build = MaterializedBatchOperator::new(schema.clone(), vec![build_batch]);
        let installed = Rc::new(RefCell::new(Vec::new()));
        let probe = CapturingRuntimeFilterProbe {
            schema: schema.clone(),
            installed: installed.clone(),
        };
        let _stream = HashJoinBatchOperator::for_test(
            &mut build,
            Box::new(probe),
            &[0, 1],
            &[0, 1],
            &[],
            joined_schema(&schema),
            true,
            VecJoinType::Inner,
        )
        .unwrap();

        let installed = installed.borrow();
        assert_eq!(installed.len(), 2);
        let mut total_bytes = 0usize;
        for (component, runtime_filter) in installed.iter().enumerate() {
            let crate::planner::RuntimeFilter::BloomKey { keys, filter, .. } = runtime_filter
            else {
                panic!("composite integer key should install Bloom components")
            };
            assert_eq!(keys.len(), 1);
            assert!(filter.uses_int_keys());
            assert!(!filter.uses_int_pair_keys());
            assert!(filter.contains_int(if component == 0 { 123 } else { 123 * 17 }));
            total_bytes = total_bytes.saturating_add(filter.byte_size());
        }
        assert_eq!(
            total_bytes,
            crate::engine::executor::select::bloom::BloomFilter::new_int_pair(pairs.len())
                .byte_size(),
            "component short-circuiting must not increase the ordinary bitmap budget"
        );
    }

    #[test]
    fn unified_join_fusion_matches_batches_and_releases_build() {
        use super::super::hash_agg::{AggregateKind, AggregateSpec, HashAggregateCore};
        for groups in [vec![], vec![0], vec![2]] {
            for large in [false, true] {
                let schema = schema_kv();
                let build_rows = if large {
                    (0..400).map(|i| (i / 2, i)).collect::<Vec<_>>()
                } else {
                    vec![(1, 10), (1, 20), (2, 30), (9, 90)]
                };
                let probe_rows = if large {
                    (0..150).map(|i| (i, i * 3)).collect::<Vec<_>>()
                } else {
                    vec![(1, 3), (1, 4), (2, 5), (7, 6)]
                };
                let build_batch = batch(&schema, &build_rows);
                let probe_batch = batch(&schema, &probe_rows);
                let mut expected = None;
                for fused in [false, true] {
                    let mut build =
                        MaterializedBatchOperator::new(schema.clone(), vec![build_batch.clone()]);
                    let probe =
                        MaterializedBatchOperator::new(schema.clone(), vec![probe_batch.clone()]);
                    let mut join = HashJoinBatchOperator::for_test(
                        &mut build,
                        Box::new(probe),
                        &[0],
                        &[0],
                        &[],
                        joined_schema(&schema),
                        true,
                        VecJoinType::Inner,
                    )
                    .unwrap();
                    let mut agg = HashAggregateCore::new(
                        groups.clone(),
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
                            AggregateSpec {
                                comparison_collation: None,
                                func: AggregateKind::Sum,
                                input_column: 3,
                            },
                        ],
                    );
                    agg.set_input_schema(joined_schema(&schema));
                    if fused {
                        assert!(
                            join.consume_into_hash_aggregate(&mut agg).unwrap(),
                            "{groups:?}"
                        );
                        assert!(
                            join.test_payload().is_none(),
                            "fusion must release build before parent finalization"
                        );
                        assert!(
                            join.next_batch().unwrap().is_none(),
                            "fusion must not emit matches twice"
                        );
                    } else {
                        while let Some(batch) = join.next_batch().unwrap() {
                            agg.consume_batch(&batch).unwrap();
                        }
                    }
                    let mut output = agg.finalize_stream().unwrap();
                    let mut rows = Vec::new();
                    while let Some(batch) = output.next_batch().unwrap() {
                        rows.extend(flatten_batches_to_rows(vec![batch]));
                    }
                    rows.sort_by_key(|row| format!("{row:?}"));
                    if let Some(expected) = &expected {
                        assert_eq!(&rows, expected, "{groups:?}");
                    } else {
                        expected = Some(rows);
                    }
                }
            }
        }
    }

    #[test]
    fn unified_join_rejects_fusion_after_output_without_losing_remaining_rows() {
        use super::super::hash_agg::{AggregateKind, AggregateSpec, HashAggregateCore};
        let schema = schema_kv();
        let mut build =
            MaterializedBatchOperator::new(schema.clone(), vec![batch(&schema, &[(1, 10)])]);
        let probe = MaterializedBatchOperator::new(
            schema.clone(),
            vec![batch(&schema, &[(1, 3)]), batch(&schema, &[(1, 4)])],
        );
        let mut join = HashJoinBatchOperator::for_test(
            &mut build,
            Box::new(probe),
            &[0],
            &[0],
            &[],
            joined_schema(&schema),
            true,
            VecJoinType::Inner,
        )
        .unwrap();
        assert_eq!(join.next_batch().unwrap().unwrap().visible_rows(), 1);
        let mut agg = HashAggregateCore::new(
            vec![],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountStar,
                input_column: 0,
            }],
        );
        assert!(!join.consume_into_hash_aggregate(&mut agg).unwrap());
        assert_eq!(join.next_batch().unwrap().unwrap().visible_rows(), 1);
        assert!(join.next_batch().unwrap().is_none());
    }

    #[test]
    fn unified_join_capacity_rejection_keeps_probe_available() {
        use super::super::hash_agg::{AggregateKind, AggregateSpec, HashAggregateCore};
        let schema = schema_kv();
        let mut build = MaterializedBatchOperator::new(
            schema.clone(),
            vec![batch(&schema, &[(1, 10), (1, 20)])],
        );
        let probe = MaterializedBatchOperator::new(schema.clone(), vec![batch(&schema, &[(1, 3)])]);
        let mut join = HashJoinBatchOperator::for_test(
            &mut build,
            Box::new(probe),
            &[0],
            &[0],
            &[],
            joined_schema(&schema),
            true,
            VecJoinType::Inner,
        )
        .unwrap();
        let mut agg = HashAggregateCore::new(
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountStar,
                input_column: 0,
            }],
        );
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(1));
        crate::engine::executor::mem_budget::with_query_budget(budget, || {
            assert!(!join.consume_into_hash_aggregate(&mut agg).unwrap());
        });
        assert_eq!(join.next_batch().unwrap().unwrap().visible_rows(), 2);
        assert!(join.next_batch().unwrap().is_none());
    }

    #[test]
    fn unified_empty_build_fused_count_is_zero_without_probe_pull() {
        use super::super::hash_agg::{AggregateKind, AggregateSpec, HashAggregateCore};
        let schema = schema_kv();
        let mut build = MaterializedBatchOperator::new(schema.clone(), vec![]);
        struct CountingProbe {
            schema: Arc<BatchSchema>,
            pulls: Rc<Cell<usize>>,
        }
        impl BatchOperator for CountingProbe {
            fn schema(&self) -> &Arc<BatchSchema> {
                &self.schema
            }
            fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
                self.pulls.set(self.pulls.get() + 1);
                Ok(None)
            }
        }
        let pulls = Rc::new(Cell::new(0));
        let probe = CountingProbe {
            schema: schema.clone(),
            pulls: pulls.clone(),
        };
        let mut join = HashJoinBatchOperator::for_test(
            &mut build,
            Box::new(probe),
            &[0],
            &[0],
            &[],
            joined_schema(&schema),
            true,
            VecJoinType::Inner,
        )
        .unwrap();
        let mut agg = HashAggregateCore::new(
            vec![],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountStar,
                input_column: 0,
            }],
        );
        assert!(join.consume_into_hash_aggregate(&mut agg).unwrap());
        let mut output = agg.finalize_stream().unwrap();
        let rows = flatten_batches_to_rows(vec![output.next_batch().unwrap().unwrap()]);
        assert_eq!(rows, vec![vec![Value::Int(0)]]);
        assert_eq!(pulls.get(), 0);
    }

    #[test]
    fn streaming_unique_i64_join_moves_identity_probe_columns() {
        let schema = schema_kv();
        let build_batch = batch(&schema, &[(1, 100), (2, 200), (3, 300)]);
        let probe_batch = batch(&schema, &[(3, 30), (1, 10), (2, 20)]);
        let probe_key_ptr = probe_batch.columns()[0]
            .as_i64_slice()
            .expect("native probe key")
            .as_ptr();
        let mut build = MaterializedBatchOperator::new(schema.clone(), vec![build_batch]);
        let probe = MaterializedBatchOperator::new(schema.clone(), vec![probe_batch]);
        let mut stream = HashJoinBatchOperator::for_test(
            &mut build,
            Box::new(probe),
            &[0],
            &[0],
            &[],
            joined_schema(&schema),
            true,
            VecJoinType::Inner,
        )
        .unwrap();

        let output = stream.next_batch().unwrap().expect("joined batch");
        assert_eq!(output.num_rows(), 3);
        assert_eq!(
            output.columns()[2]
                .as_i64_slice()
                .expect("moved probe key")
                .as_ptr(),
            probe_key_ptr,
            "identity probe output should retain the original column buffer"
        );
        assert_eq!(
            flatten_batches_to_rows(vec![output]),
            vec![
                vec![
                    Value::Int(3),
                    Value::Int(300),
                    Value::Int(3),
                    Value::Int(30)
                ],
                vec![
                    Value::Int(1),
                    Value::Int(100),
                    Value::Int(1),
                    Value::Int(10)
                ],
                vec![
                    Value::Int(2),
                    Value::Int(200),
                    Value::Int(2),
                    Value::Int(20)
                ],
            ]
        );
        assert!(stream.next_batch().unwrap().is_none());
    }

    #[test]
    fn streaming_join_forwards_parent_runtime_filter_to_unopened_probe() {
        let schema = schema_kv();
        let build_batch = batch(&schema, &[(1, 100)]);
        let mut build = MaterializedBatchOperator::new(schema.clone(), vec![build_batch]);
        let installed = Rc::new(Cell::new(false));
        let probe = RuntimeFilterProbe {
            schema: schema.clone(),
            installed: Rc::clone(&installed),
        };
        let mut stream = HashJoinBatchOperator::for_test(
            &mut build,
            Box::new(probe),
            &[0],
            &[0],
            &[],
            joined_schema(&schema),
            false,
            VecJoinType::Left,
        )
        .unwrap();

        let filter = crate::planner::RuntimeFilter::KeySet {
            keys: vec![crate::parser::Expr::Identifier("k".into())],
            null_safe: vec![false],
            values: Arc::new(Vec::new()),
            filter: Arc::new(crate::engine::executor::select::bloom::BloomFilter::new(1)),
        };
        assert!(stream.install_runtime_filter(filter));
        assert!(installed.get());
    }

    #[test]
    fn inner_join_basic_one_to_many() {
        let s = schema_kv();
        // Build: small dimension table
        let b = batch(&s, &[(1, 100), (2, 200), (3, 300)]);
        // Probe: fact table with duplicate keys
        let p = batch(&s, &[(2, 22), (1, 11), (2, 23), (4, 44), (3, 33)]);
        let js = joined_schema(&s);
        let mut build = MaterializedBatchOperator::new(s.clone(), vec![b]);
        let mut probe = MaterializedBatchOperator::new(s, vec![p]);
        let rows = run_inner_rows(&mut build, &mut probe, js);
        // Expected output ordering: probe-row-major, build-row order
        // inside each match group. 4 probe rows match (k=2,1,2,3); k=4
        // has no match. Schema: [build.k, build.v, probe.k, probe.v].
        let got: Vec<(i64, i64, i64, i64)> = rows
            .into_iter()
            .map(|r| {
                let g = |v: &Value| match v {
                    Value::Int(x) => *x,
                    _ => panic!(),
                };
                (g(&r[0]), g(&r[1]), g(&r[2]), g(&r[3]))
            })
            .collect();
        assert_eq!(
            got,
            vec![
                (2, 200, 2, 22),
                (1, 100, 1, 11),
                (2, 200, 2, 23),
                (3, 300, 3, 33),
            ]
        );
    }

    #[test]
    fn null_keys_never_match() {
        let s = schema_kv();
        let b = Batch::new(
            s.clone(),
            vec![
                ColumnVector::from_values(vec![Value::Null, Value::Int(1)]),
                ColumnVector::from_values(vec![Value::Int(99), Value::Int(100)]),
            ],
        );
        let p = Batch::new(
            s.clone(),
            vec![
                ColumnVector::from_values(vec![Value::Null, Value::Int(1)]),
                ColumnVector::from_values(vec![Value::Int(11), Value::Int(12)]),
            ],
        );
        let js = joined_schema(&s);
        let mut build = MaterializedBatchOperator::new(s.clone(), vec![b]);
        let mut probe = MaterializedBatchOperator::new(s, vec![p]);
        let rows = run_inner_rows(&mut build, &mut probe, js);
        // Only the (1=1) match should survive; both NULL keys must
        // drop, mirroring SQL's three-valued logic.
        assert_eq!(rows.len(), 1);
        let g = |v: &Value| match v {
            Value::Int(x) => *x,
            _ => panic!(),
        };
        assert_eq!(g(&rows[0][0]), 1);
        assert_eq!(g(&rows[0][2]), 1);
    }

    #[test]
    fn null_safe_keys_match_nulls() {
        let s = schema_kv();
        let b = Batch::new(
            s.clone(),
            vec![
                ColumnVector::from_values(vec![Value::Null, Value::Int(1)]),
                ColumnVector::from_values(vec![Value::Int(99), Value::Int(100)]),
            ],
        );
        let p = Batch::new(
            s.clone(),
            vec![
                ColumnVector::from_values(vec![Value::Null, Value::Int(2)]),
                ColumnVector::from_values(vec![Value::Int(11), Value::Int(12)]),
            ],
        );
        let js = joined_schema(&s);
        let mut build = MaterializedBatchOperator::new(s.clone(), vec![b]);
        let mut probe = MaterializedBatchOperator::new(s, vec![p]);
        let spec = VecJoinSpec {
            build_keys: &[0],
            probe_keys: &[0],
            key_collations: &[],
            null_safe: &[true],
            build_cols_first: true,
            out_schema: js,
            residual: None,
        };
        let rows =
            flatten_batches_to_rows(run_inner_hash_join(&mut build, &mut probe, &spec).unwrap());
        // NULL null-safe-equals NULL matches build row 0 with probe row 0.
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Int(99));
        assert_eq!(rows[0][3], Value::Int(11));
    }

    #[test]
    fn empty_probe_emits_nothing() {
        let s = schema_kv();
        let b = batch(&s, &[(1, 100)]);
        let js = joined_schema(&s);
        let mut build = MaterializedBatchOperator::new(s.clone(), vec![b]);
        let mut probe = MaterializedBatchOperator::new(s, vec![]);
        let rows = run_inner_rows(&mut build, &mut probe, js);
        assert!(rows.is_empty());
    }

    #[test]
    fn streaming_empty_build_does_not_pull_probe() {
        struct UnopenedProbe(Arc<BatchSchema>);
        impl BatchOperator for UnopenedProbe {
            fn schema(&self) -> &Arc<BatchSchema> {
                &self.0
            }
            fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
                panic!("empty INNER build must not pull probe");
            }
        }
        let s = schema_kv();
        for keys in [vec![0], vec![0, 1]] {
            let mut build = MaterializedBatchOperator::new(s.clone(), vec![]);
            let probe = Box::new(UnopenedProbe(s.clone()));
            let mut join = HashJoinBatchOperator::for_test(
                &mut build,
                probe,
                &keys,
                &keys,
                &[],
                joined_schema(&s),
                true,
                VecJoinType::Inner,
            )
            .unwrap();
            assert!(join.next_batch().unwrap().is_none());
            assert!(join.next_batch().unwrap().is_none());
        }
    }

    #[test]
    fn empty_build_emits_nothing() {
        let s = schema_kv();
        let p = batch(&s, &[(1, 100)]);
        let js = joined_schema(&s);
        let mut build = MaterializedBatchOperator::new(s.clone(), vec![]);
        let mut probe = MaterializedBatchOperator::new(s, vec![p]);
        let rows = run_inner_rows(&mut build, &mut probe, js);
        assert!(rows.is_empty());
    }

    #[test]
    fn semi_join_emits_matching_probe_rows_once() {
        let s = schema_kv();
        let b = batch(&s, &[(1, 100), (1, 101), (3, 300)]);
        let p = batch(&s, &[(1, 11), (2, 22), (3, 33)]);
        let mut build = MaterializedBatchOperator::new(s.clone(), vec![b]);
        let mut probe = MaterializedBatchOperator::new(s.clone(), vec![p]);
        let rows = flatten_batches_to_rows(
            run_semi_anti_hash_join(
                &mut build,
                &mut probe,
                &[0],
                &[0],
                &[],
                &[false],
                false,
                s,
                None,
            )
            .unwrap(),
        );
        // Probe keys 1/3 match; duplicate build key 1 still emits the probe only once.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int(1));
        assert_eq!(rows[1][0], Value::Int(3));
    }

    #[test]
    fn anti_join_emits_non_matching_probe_rows() {
        let s = schema_kv();
        let b = batch(&s, &[(1, 100)]);
        let p = Batch::new(
            s.clone(),
            vec![
                ColumnVector::from_values(vec![Value::Int(1), Value::Int(2), Value::Null]),
                ColumnVector::from_values(vec![Value::Int(11), Value::Int(22), Value::Int(33)]),
            ],
        );
        let mut build = MaterializedBatchOperator::new(s.clone(), vec![b]);
        let mut probe = MaterializedBatchOperator::new(s.clone(), vec![p]);
        let rows = flatten_batches_to_rows(
            run_semi_anti_hash_join(
                &mut build,
                &mut probe,
                &[0],
                &[0],
                &[],
                &[false],
                true,
                s,
                None,
            )
            .unwrap(),
        );
        // ANTI emits unmatched key 2 and NULL; matching key 1 is excluded.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int(2));
        assert_eq!(rows[1][0], Value::Null);
    }

    #[test]
    fn preserved_probe_join_null_extends_misses() {
        let s = schema_kv();
        // LEFT JOIN: probe is preserved left; build is right.
        let b = batch(&s, &[(1, 100), (1, 101)]);
        let p = batch(&s, &[(1, 11), (2, 22)]);
        let js = joined_schema(&s);
        let mut build = MaterializedBatchOperator::new(s.clone(), vec![b]);
        let mut probe = MaterializedBatchOperator::new(s, vec![p]);
        let spec = VecJoinSpec {
            build_keys: &[0],
            probe_keys: &[0],
            key_collations: &[],
            null_safe: &[false],
            // LEFT output places probe/left columns first.
            build_cols_first: false,
            out_schema: js,
            residual: None,
        };
        let rows = flatten_batches_to_rows(
            run_preserved_probe_hash_join(&mut build, &mut probe, &spec).unwrap(),
        );
        // Probe k=1 matches two build rows; k=2 has no match and NULL build columns.
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0],
            vec![
                Value::Int(1),
                Value::Int(11),
                Value::Int(1),
                Value::Int(100)
            ]
        );
        assert_eq!(
            rows[1],
            vec![
                Value::Int(1),
                Value::Int(11),
                Value::Int(1),
                Value::Int(101)
            ]
        );
        assert_eq!(
            rows[2],
            vec![Value::Int(2), Value::Int(22), Value::Null, Value::Null]
        );
    }

    #[test]
    fn partitioned_build_matches_serial_inner_join() {
        let s = schema_kv();
        // Multiple batches, duplicate keys, NULL keys, and cross-partition distribution.
        let mk = |lo: i64, n: i64, dup_every: i64| -> Batch {
            let mut ks = Vec::new();
            let mut vs = Vec::new();
            for i in lo..lo + n {
                ks.push(if i % 37 == 0 {
                    Value::Null
                } else {
                    Value::Int(i % 97)
                });
                vs.push(Value::Int(i));
                if i % dup_every == 0 {
                    ks.push(Value::Int(i % 97));
                    vs.push(Value::Int(i + 100_000));
                }
            }
            Batch::new(
                s.clone(),
                vec![ColumnVector::from_values(ks), ColumnVector::from_values(vs)],
            )
        };
        let build_batches = vec![mk(0, 300, 11), mk(300, 300, 7)];
        let probe_batches = vec![mk(0, 250, 13), mk(250, 250, 5), mk(500, 100, 9)];
        let js = joined_schema(&s);
        let spec = VecJoinSpec {
            build_keys: &[0],
            probe_keys: &[0],
            key_collations: &[],
            null_safe: &[false],
            build_cols_first: true,
            out_schema: js,
            residual: None,
        };

        let mut sb = MaterializedBatchOperator::new(s.clone(), build_batches.clone());
        let mut sp = MaterializedBatchOperator::new(s.clone(), probe_batches.clone());
        let mut serial =
            flatten_batches_to_rows(run_inner_hash_join(&mut sb, &mut sp, &spec).unwrap());

        let mut pp = MaterializedBatchOperator::new(s.clone(), probe_batches);
        let mut partitioned = flatten_batches_to_rows(
            run_inner_hash_join_partitioned(build_batches, &mut pp, &spec, 4).unwrap(),
        );

        let key = |r: &Vec<Value>| -> (i64, i64, i64, i64) {
            let g = |v: &Value| match v {
                Value::Int(x) => *x,
                _ => -1,
            };
            (g(&r[0]), g(&r[1]), g(&r[2]), g(&r[3]))
        };
        serial.sort_by_key(key);
        partitioned.sort_by_key(key);
        assert!(!serial.is_empty());
        assert_eq!(serial, partitioned);
    }

    #[test]
    fn semi_anti_join_with_residual_resolves_per_pair() {
        // Probe p(k,v), build b(k,v), candidate order [p,b];
        // residual p.v>b.v requires any candidate pair to pass for SEMI,
        // and every pair to fail for ANTI.
        let make_schema = |table: &str| {
            Arc::new(BatchSchema::new(vec![
                BatchField {
                    table: table.into(),
                    alias: None,
                    name: "k".into(),
                    column_type: ColumnType::LongLong,
                    source: Default::default(),
                },
                BatchField {
                    table: table.into(),
                    alias: None,
                    name: "v".into(),
                    column_type: ColumnType::LongLong,
                    source: Default::default(),
                },
            ]))
        };
        let ps = make_schema("p");
        let bs = make_schema("b");
        let mut cand_fields = ps.fields().to_vec();
        cand_fields.extend(bs.fields().to_vec());
        let cand_schema = Arc::new(BatchSchema::new(cand_fields));
        let residual = crate::parser::Expr::Binary {
            left: Box::new(crate::parser::Expr::CompoundIdentifier(vec![
                "p".into(),
                "v".into(),
            ])),
            op: crate::parser::BinaryOp::Gt,
            right: Box::new(crate::parser::Expr::CompoundIdentifier(vec![
                "b".into(),
                "v".into(),
            ])),
        };
        let program = crate::engine::executor::vec::expr_batch::BatchExprProgram::compile(
            &residual,
            &cand_schema,
        )
        .unwrap()
        .expect("residual should compile");

        let b = batch(&bs, &[(1, 100), (1, 5), (2, 50)]);
        let p = batch(&ps, &[(1, 50), (1, 3), (2, 60), (3, 9)]);
        for (is_anti, expected) in [
            (false, vec![(1i64, 50i64), (2, 60)]),
            (true, vec![(1, 3), (3, 9)]),
        ] {
            let mut build = MaterializedBatchOperator::new(bs.clone(), vec![b.clone()]);
            let mut probe = MaterializedBatchOperator::new(ps.clone(), vec![p.clone()]);
            let rows = flatten_batches_to_rows(
                run_semi_anti_hash_join(
                    &mut build,
                    &mut probe,
                    &[0],
                    &[0],
                    &[],
                    &[false],
                    is_anti,
                    ps.clone(),
                    Some((&program, cand_schema.clone())),
                )
                .unwrap(),
            );
            let got: Vec<(i64, i64)> = rows
                .iter()
                .map(|r| {
                    let g = |v: &Value| match v {
                        Value::Int(x) => *x,
                        _ => panic!(),
                    };
                    (g(&r[0]), g(&r[1]))
                })
                .collect();
            assert_eq!(got, expected, "is_anti={is_anti}");
        }
    }

    #[test]
    fn take_indices_gathers_with_null_sentinel() {
        let col = ColumnVector::from_values(vec![Value::Int(10), Value::Null, Value::Int(30)]);
        let out = col.take_indices(&[2, u32::MAX, 1, 0]);
        assert_eq!(out.len(), 4);
        assert_eq!(out.value_at(0), Value::Int(30));
        assert_eq!(out.value_at(1), Value::Null);
        assert_eq!(out.value_at(2), Value::Null);
        assert_eq!(out.value_at(3), Value::Int(10));
    }

    #[test]
    fn take_indices_utf8_copies_byte_ranges() {
        let col = ColumnVector::from_values(vec![
            Value::String("alpha".into()),
            Value::String("beta".into()),
            Value::Null,
        ]);
        let out = col.take_indices(&[1, u32::MAX, 0, 2]);
        assert_eq!(out.value_at(0), Value::String("beta".into()));
        assert_eq!(out.value_at(1), Value::Null);
        assert_eq!(out.value_at(2), Value::String("alpha".into()));
        assert_eq!(out.value_at(3), Value::Null);
    }

    #[test]
    fn append_rows_from_accumulates_and_promotes() {
        // Accumulate within the same lane.
        let mut acc = ColumnVector::Null { len: 0 };
        let a = ColumnVector::from_values(vec![Value::Int(1), Value::Int(2)]);
        let b = ColumnVector::from_values(vec![Value::Int(3), Value::Null]);
        acc.append_rows_from(&a, &[0, 1]);
        acc.append_rows_from(&b, &[0, 1]);
        assert_eq!(acc.len(), 4);
        assert_eq!(acc.value_at(2), Value::Int(3));
        assert_eq!(acc.value_at(3), Value::Null);
        // Lane drift from I64 accumulation to a Utf8 batch upgrades losslessly to Generic.
        let c = ColumnVector::from_values(vec![Value::String("x".into())]);
        acc.append_rows_from(&c, &[0]);
        assert_eq!(acc.len(), 5);
        assert_eq!(acc.value_at(0), Value::Int(1));
        assert_eq!(acc.value_at(4), Value::String("x".into()));
    }

    // ---- P0/B1: build-side size cap ----------------------------------

    fn two_batch_build(s: &Arc<BatchSchema>) -> MaterializedBatchOperator {
        MaterializedBatchOperator::new(
            s.clone(),
            vec![
                batch(s, &[(1, 10), (2, 20), (3, 30)]),
                batch(s, &[(4, 40), (5, 50), (6, 60)]),
            ],
        )
    }

    #[test]
    fn build_cap_payload_overflow_by_rows() {
        let s = schema_kv();
        let mut build = two_batch_build(&s);
        // max_rows=2; the first 3-row batch already crosses it.
        let cap = BuildCap {
            max_rows: 2,
            max_bytes: u64::MAX,
        };
        let res = with_vec_build_cap(cap, || collect_build_payload(&mut build, &[0], &[false]));
        match res {
            Err(e) => assert!(is_vec_build_overflow(&e), "code={}", e.code()),
            Ok(_) => panic!("build over row cap must raise the sentinel"),
        }
    }

    #[test]
    fn build_cap_payload_overflow_by_bytes() {
        let s = schema_kv();
        let mut build = two_batch_build(&s);
        let cap = BuildCap {
            max_rows: u64::MAX,
            max_bytes: 1, // any real batch exceeds 1 byte
        };
        let res = with_vec_build_cap(cap, || collect_build_payload(&mut build, &[0], &[false]));
        match res {
            Err(e) => assert!(is_vec_build_overflow(&e), "code={}", e.code()),
            Ok(_) => panic!("build over byte cap must raise the sentinel"),
        }
    }

    #[test]
    fn build_cap_keyset_overflow_by_distinct_keys() {
        let s = schema_kv();
        // 6 distinct keys; cap at 2 distinct → sentinel.
        let mut build = two_batch_build(&s);
        let cap = BuildCap {
            max_rows: 2,
            max_bytes: u64::MAX,
        };
        let res = with_vec_build_cap(cap, || {
            collect_build_keyset_with_collations(&mut build, &[0], &[false], &[])
        });
        match res {
            Err(e) => assert!(is_vec_build_overflow(&e), "code={}", e.code()),
            Ok(_) => panic!("keyset over distinct-key cap must raise the sentinel"),
        }
    }

    #[test]
    fn build_cap_unset_is_unbounded() {
        // No `with_vec_build_cap` wrapper → collectors never cap, so the
        // pre-existing behaviour (and every non-bridge caller / test) is
        // unchanged.
        let s = schema_kv();
        let mut build = two_batch_build(&s);
        let payload =
            collect_build_payload(&mut build, &[0], &[false]).expect("uncapped build must succeed");
        assert_eq!(payload.columns[0].len(), 6);
    }

    #[test]
    fn build_cap_generous_succeeds() {
        let s = schema_kv();
        let mut build = two_batch_build(&s);
        let cap = BuildCap {
            max_rows: 1_000_000,
            max_bytes: 1 << 30,
        };
        let payload = with_vec_build_cap(cap, || collect_build_payload(&mut build, &[0], &[false]))
            .expect("build under cap must succeed");
        assert_eq!(payload.columns[0].len(), 6);
    }

    #[test]
    fn run_inner_hash_join_propagates_build_overflow() {
        // End-to-end: the sentinel raised in the collector propagates out
        // of `run_inner_hash_join` so the bridge can catch it and fall
        // back to the row-based join.
        let s = schema_kv();
        let mut build = two_batch_build(&s);
        let mut probe =
            MaterializedBatchOperator::new(s.clone(), vec![batch(&s, &[(1, 1), (4, 4)])]);
        let js = joined_schema(&s);
        let spec = VecJoinSpec {
            build_keys: &[0],
            probe_keys: &[0],
            key_collations: &[],
            null_safe: &[false],
            build_cols_first: true,
            out_schema: js,
            residual: None,
        };
        let cap = BuildCap {
            max_rows: 2,
            max_bytes: u64::MAX,
        };
        let res = with_vec_build_cap(cap, || run_inner_hash_join(&mut build, &mut probe, &spec));
        match res {
            Err(e) => assert!(is_vec_build_overflow(&e), "code={}", e.code()),
            Ok(_) => panic!("run_inner_hash_join must propagate the build-overflow sentinel"),
        }
    }
}
