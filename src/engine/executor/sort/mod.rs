//! Shared sorting primitives for row and vector executors.
//!
//! The executor has several ORDER BY consumers (full sort, ordered
//! LIMIT, TopK, external merge sort, vector TopN). This module owns the
//! common comparison semantics and cached-key helpers so those paths do
//! not drift apart.

use std::cmp::Ordering;

#[cfg(test)]
use super::mem_budget::ScopedCharge;
#[cfg(test)]
use super::memory_footprint::AllocatedBytes;
use crate::types::Value;

pub(crate) mod key_encode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SortField {
    pub(crate) ascending: bool,
    pub(crate) nulls_first: bool,
    /// Effective collation of this ORDER BY expression. `None` means the
    /// expression is non-character or no schema metadata is available.
    /// Keeping it beside direction/null ordering prevents row, spill and
    /// Top-N sort paths from silently falling back to the session default.
    pub(crate) collation_id: Option<u16>,
}

impl SortField {
    #[cfg(test)]
    pub(crate) fn mysql_order(ascending: bool) -> Self {
        Self {
            ascending,
            // MySQL treats NULL as lower than any non-NULL value: ASC
            // places it first, DESC places it last.
            nulls_first: ascending,
            collation_id: None,
        }
    }

    /// Field for one ORDER BY key, honouring an explicit
    /// `NULLS FIRST`/`NULLS LAST` clause (mydb extension) and falling
    /// back to the MySQL direction-derived default.
    pub(crate) fn from_order_by(item: &crate::parser::OrderBy) -> Self {
        Self {
            ascending: item.asc,
            nulls_first: item.nulls_first_effective(),
            collation_id: match &item.expr {
                crate::parser::Expr::Function { name, .. }
                    if name.eq_ignore_ascii_case("COLLATE") =>
                {
                    crate::engine::expr::expression_metadata_collation_id(&item.expr)
                }
                _ => None,
            },
        }
    }
}

pub(crate) fn fields_from_order_by(order_by: &[crate::parser::OrderBy]) -> Vec<SortField> {
    order_by.iter().map(SortField::from_order_by).collect()
}

pub(crate) fn compare_value_for_sort(left: &Value, right: &Value, field: SortField) -> Ordering {
    let left_null = matches!(left, Value::Null);
    let right_null = matches!(right, Value::Null);
    match (left_null, right_null) {
        (true, true) => return Ordering::Equal,
        (true, false) => {
            return if field.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        (false, true) => {
            return if field.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        (false, false) => {}
    }

    // A resolved ORDER BY collation proves that two String/SmallString
    // operands are character values of the same typed expression.  Route
    // those pairs straight to the collation comparator.  The generic SQL
    // comparator must first probe temporal/string and numeric/string
    // coercions because it also serves predicates between unlike dynamic
    // values; those probes are both unnecessary and surprisingly expensive
    // inside O(n log n) sorting (notably sysbench's CHAR(120) range sorts).
    fn string_lane(value: &Value) -> Option<&str> {
        match value {
            Value::String(value) => Some(value.as_str()),
            Value::SmallString(value) => {
                // SAFETY: SmallString maintains a UTF-8 invariant.
                Some(unsafe { std::str::from_utf8_unchecked(value.as_ref()) })
            }
            _ => None,
        }
    }
    let typed_string_order = field.collation_id.and_then(|collation_id| {
        Some(crate::charset::compare_strings_by_id(
            collation_id,
            string_lane(left)?,
            string_lane(right)?,
        ))
    });
    // SQL predicate comparison is not a sorting relation. Use the shared
    // exact-rational total order for numeric lanes, including NaN and mixed
    // DECIMAL/float aggregate states.
    let raw = typed_string_order
        .or_else(|| crate::engine::expr::compare_numeric_for_sort(left, right))
        .unwrap_or_else(|| {
            // A dynamically typed slot can retain unlike runtime domains
            // (for example hidden aggregate payloads). Pairwise predicate
            // coercion is cyclic here: numeric/text, temporal/text and
            // text/text each choose different relations. Keep all numeric
            // representations and String/SmallString in their shared domain;
            // unlike domains have a deterministic order. Typed SQL casts
            // and comparisons still use their ordinary coercion rules.
            fn domain(value: &Value) -> u8 {
                match value {
                    Value::Null => 0,
                    Value::Int(_)
                    | Value::UInt(_)
                    | Value::Float(_)
                    | Value::Double(_)
                    | Value::Decimal(_)
                    | Value::DecimalCompact(..)
                    | Value::Decimal128(..)
                    | Value::DecimalWide(_) => 1,
                    Value::String(_) | Value::SmallString(_) => 2,
                    Value::Bytes(_) => 3,
                    Value::Date(_) => 4,
                    Value::Time(_) => 5,
                    Value::DateTime(_) => 6,
                    Value::Timestamp(_) => 7,
                    Value::Year(_) => 8,
                    Value::Json(_) => 9,
                    Value::Geometry(_) => 10,
                    Value::Vector(_) => 11,
                }
            }
            let domains = domain(left).cmp(&domain(right));
            if domains != Ordering::Equal {
                return domains;
            }
            // Binary sort keys retain byte ordering. A text descriptor must
            // not decode BLOB expression results and erase NUL distinctions.
            let collation = if matches!(left, Value::Bytes(_)) || matches!(right, Value::Bytes(_)) {
                None
            } else {
                field.collation_id
            };
            crate::engine::expr::with_compare_collation(collation, || {
                crate::engine::expr::compare_values(left, right).unwrap_or(Ordering::Equal)
            })
        });
    if field.ascending {
        raw
    } else {
        raw.reverse()
    }
}

pub(crate) fn compare_sort_keys(fields: &[SortField], left: &[Value], right: &[Value]) -> Ordering {
    debug_assert_eq!(left.len(), fields.len());
    debug_assert_eq!(right.len(), fields.len());
    for (field, (l, r)) in fields.iter().zip(left.iter().zip(right.iter())) {
        let ord = compare_value_for_sort(l, r, *field);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

pub(crate) fn compare_payload_rows(left: &[Value], right: &[Value]) -> Ordering {
    let field = SortField {
        ascending: true,
        nulls_first: true,
        collation_id: None,
    };
    for (left, right) in left.iter().zip(right) {
        let order = compare_value_for_sort(left, right, field);
        if order != Ordering::Equal {
            return order;
        }
    }
    left.len().cmp(&right.len())
}

struct KeyedIndex {
    keys: Vec<Value>,
    index: u32,
}

struct SingleKeyedIndex {
    key: Value,
    index: u32,
}

/// Sort `items` ascending under `fields`, evaluating each row's ORDER
/// BY key exactly once (Schwartzian).
///
/// The legacy implementation moved items into `Vec<Option<T>>` slots and
/// then wrote them back in sorted order via random `slots[idx].take()`
/// — a per-row random gather over `size_of::<Option<PlanRow>>` bytes,
/// which thrashes cache on wide rows. This version instead computes a
/// `Vec<u32>` inverse permutation alongside the sorted `keyed` array and
/// applies it to `items` in place via cycle decomposition: each element
/// moves at most once, swaps are local, and `items` keeps its original
/// capacity (no reallocation on the write-back).
pub(crate) fn sort_by_cached_keys<T>(
    items: &mut Vec<T>,
    fields: &[SortField],
    mut key_fn: impl FnMut(&T) -> Vec<Value>,
) {
    if fields.is_empty() || items.len() < 2 {
        return;
    }
    let mut keyed = build_keyed(items, &mut key_fn);
    keyed.sort_by(|a, b| {
        compare_sort_keys(fields, &a.keys, &b.keys).then_with(|| a.index.cmp(&b.index))
    });
    apply_sorted_perm_in_place(items.as_mut_slice(), &keyed);
}

/// Sort items using keys prepared by a fallible expression layer. Keeping
/// preparation outside the comparison loop lets SQL errors propagate instead
/// of being coerced to NULL, while each expression is evaluated once.
pub(crate) fn sort_by_precomputed_keys<T>(
    items: &mut Vec<T>,
    fields: &[SortField],
    keys: Vec<Vec<Value>>,
) {
    if fields.is_empty() || items.len() < 2 {
        return;
    }
    debug_assert_eq!(items.len(), keys.len());
    if apply_precomputed_int_sort(items, fields, &keys, None) {
        return;
    }
    let mut keyed: Vec<KeyedIndex> = keys
        .into_iter()
        .enumerate()
        .map(|(index, keys)| KeyedIndex {
            keys,
            index: index as u32,
        })
        .collect();
    keyed.sort_by(|a, b| {
        compare_sort_keys(fields, &a.keys, &b.keys).then_with(|| a.index.cmp(&b.index))
    });
    apply_sorted_perm_in_place(items.as_mut_slice(), &keyed);
}

/// Single-key counterpart that avoids allocating one inner `Vec<Value>` per
/// input row. Integer keys retain the encoded comparator fast path; other SQL
/// values are compared directly with the same stable input-ordinal tie-break.
pub(crate) fn sort_by_precomputed_single_key<T>(
    items: &mut Vec<T>,
    field: SortField,
    keys: Vec<Value>,
) {
    if items.len() < 2 {
        return;
    }
    debug_assert_eq!(items.len(), keys.len());
    if keys.iter().all(|value| matches!(value, Value::Int(_))) {
        let mut encoded = keys
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                let Value::Int(value) = value else {
                    unreachable!("integer capability checked above")
                };
                (encode_i64_for_sort(value, field.ascending), index as u32)
            })
            .collect::<Vec<_>>();
        encoded.sort_unstable_by_key(|&(key, index)| (key, index));
        apply_sorted_perm_in_place_raw(items, encoded.into_iter().map(|(_, index)| index));
        return;
    }
    let mut keyed = keys
        .into_iter()
        .enumerate()
        .map(|(index, key)| SingleKeyedIndex {
            key,
            index: index as u32,
        })
        .collect::<Vec<_>>();
    keyed.sort_by(|left, right| {
        compare_value_for_sort(&left.key, &right.key, field)
            .then_with(|| left.index.cmp(&right.index))
    });
    apply_sorted_perm_in_place_raw(items, keyed.into_iter().map(|entry| entry.index));
}

/// Typed single-key path used while expression evaluation is still producing
/// rows. It avoids first materialising `Value::Int` keys and then allocating a
/// second encoded permutation buffer.
pub(crate) fn sort_by_precomputed_i64_key<T>(
    items: &mut Vec<T>,
    field: SortField,
    mut keyed: Vec<(i64, u32)>,
) {
    if items.len() < 2 {
        return;
    }
    debug_assert_eq!(items.len(), keyed.len());
    keyed.sort_unstable_by(|left, right| {
        let value_order = if field.ascending {
            left.0.cmp(&right.0)
        } else {
            right.0.cmp(&left.0)
        };
        value_order.then_with(|| left.1.cmp(&right.1))
    });
    apply_sorted_perm_in_place_raw(items, keyed.into_iter().map(|entry| entry.1));
}

pub(crate) fn select_nth_by_precomputed_keys<T>(
    items: &mut Vec<T>,
    fields: &[SortField],
    nth: usize,
    keys: Vec<Vec<Value>>,
) {
    if fields.is_empty() || items.len() < 2 || nth >= items.len() {
        return;
    }
    debug_assert_eq!(items.len(), keys.len());
    if apply_precomputed_int_sort(items, fields, &keys, Some(nth)) {
        return;
    }
    let mut keyed: Vec<KeyedIndex> = keys
        .into_iter()
        .enumerate()
        .map(|(index, keys)| KeyedIndex {
            keys,
            index: index as u32,
        })
        .collect();
    keyed.select_nth_unstable_by(nth, |a, b| {
        compare_sort_keys(fields, &a.keys, &b.keys).then_with(|| a.index.cmp(&b.index))
    });
    apply_sorted_perm_in_place(items.as_mut_slice(), &keyed);
}

pub(crate) fn select_nth_by_precomputed_single_key<T>(
    items: &mut Vec<T>,
    field: SortField,
    nth: usize,
    keys: Vec<Value>,
) {
    if items.len() < 2 || nth >= items.len() {
        return;
    }
    debug_assert_eq!(items.len(), keys.len());
    if keys.iter().all(|value| matches!(value, Value::Int(_))) {
        let mut encoded = keys
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                let Value::Int(value) = value else {
                    unreachable!("integer capability checked above")
                };
                (encode_i64_for_sort(value, field.ascending), index as u32)
            })
            .collect::<Vec<_>>();
        encoded.select_nth_unstable_by_key(nth, |&(key, index)| (key, index));
        apply_sorted_perm_in_place_raw(items, encoded.into_iter().map(|(_, index)| index));
        return;
    }
    let mut keyed = keys
        .into_iter()
        .enumerate()
        .map(|(index, key)| SingleKeyedIndex {
            key,
            index: index as u32,
        })
        .collect::<Vec<_>>();
    keyed.select_nth_unstable_by(nth, |left, right| {
        compare_value_for_sort(&left.key, &right.key, field)
            .then_with(|| left.index.cmp(&right.index))
    });
    apply_sorted_perm_in_place_raw(items, keyed.into_iter().map(|entry| entry.index));
}

pub(crate) fn select_nth_by_precomputed_i64_key<T>(
    items: &mut Vec<T>,
    field: SortField,
    nth: usize,
    mut keyed: Vec<(i64, u32)>,
) {
    if items.len() < 2 || nth >= items.len() {
        return;
    }
    debug_assert_eq!(items.len(), keyed.len());
    keyed.select_nth_unstable_by(nth, |left, right| {
        let value_order = if field.ascending {
            left.0.cmp(&right.0)
        } else {
            right.0.cmp(&left.0)
        };
        value_order.then_with(|| left.1.cmp(&right.1))
    });
    apply_sorted_perm_in_place_raw(items, keyed.into_iter().map(|entry| entry.1));
}

/// Preserve the encoded-integer comparator fast path after expression keys
/// have been prepared fallibly. Capability is decided from the complete key
/// set, so no row is evaluated a second time when a later key is NULL or has
/// a different type.
fn apply_precomputed_int_sort<T>(
    items: &mut [T],
    fields: &[SortField],
    keys: &[Vec<Value>],
    nth: Option<usize>,
) -> bool {
    let row_count = items.len();
    let key_count = fields.len();
    if row_count < 2 || key_count == 0 {
        return true;
    }
    let mut encoded = Vec::with_capacity(row_count.saturating_mul(key_count));
    for row_keys in keys {
        if row_keys.len() != key_count {
            return false;
        }
        for (value, field) in row_keys.iter().zip(fields) {
            let Value::Int(value) = value else {
                return false;
            };
            encoded.push(encode_i64_for_sort(*value, field.ascending));
        }
    }
    let mut order: Vec<u32> = (0..row_count as u32).collect();
    let compare = |a: &u32, b: &u32| {
        let a = *a as usize;
        let b = *b as usize;
        encoded[a * key_count..(a + 1) * key_count]
            .cmp(&encoded[b * key_count..(b + 1) * key_count])
            .then_with(|| a.cmp(&b))
    };
    if let Some(nth) = nth {
        order.select_nth_unstable_by(nth, compare);
    } else {
        order.sort_unstable_by(compare);
    }
    apply_sorted_perm_in_place_raw(items, order);
    true
}

fn build_keyed<T>(items: &[T], key_fn: &mut impl FnMut(&T) -> Vec<Value>) -> Vec<KeyedIndex> {
    debug_assert!(
        items.len() <= u32::MAX as usize,
        "sort_by_cached_keys: in-memory sort exceeds u32::MAX rows"
    );
    items
        .iter()
        .enumerate()
        .map(|(index, item)| KeyedIndex {
            keys: key_fn(item),
            index: index as u32,
        })
        .collect()
}

/// Permute `items` in place so the element originally at
/// `keyed[i].index` ends up at position `i`. Uses an `Vec<u32>` inverse
/// permutation and walks each cycle once — total work O(n) swaps. The
/// `Vec<u32>` is 4 bytes/row regardless of row width, much cheaper than
/// the `Vec<Option<T>>` the legacy path used.
fn apply_sorted_perm_in_place<T>(items: &mut [T], keyed: &[KeyedIndex]) {
    apply_sorted_perm_in_place_raw(items, keyed.iter().map(|k| k.index));
}

/// Generic version of `apply_sorted_perm_in_place` that takes a
/// permutation as an iterator of original positions. Used by both the
/// `KeyedIndex`-based path (general sort) and the fast path below that
/// works directly on `(u64, u32)` encoded keys.
fn apply_sorted_perm_in_place_raw<T, I: IntoIterator<Item = u32>>(items: &mut [T], sorted_perm: I) {
    let n = items.len();
    let mut inv: Vec<u32> = vec![0u32; n];
    let mut count = 0usize;
    for (new_pos, orig) in sorted_perm.into_iter().enumerate() {
        inv[orig as usize] = new_pos as u32;
        count += 1;
    }
    debug_assert_eq!(count, n);
    let mut i = 0usize;
    while i < n {
        while inv[i] as usize != i {
            let target = inv[i] as usize;
            items.swap(i, target);
            inv.swap(i, target);
        }
        i += 1;
    }
}

/// Specialised single-column sort for `i64` keys, with optional NULL
/// support via `key_fn` returning `Option<i64>`. Returns `true` when
/// the sort completed via the fast path, `false` when any row reports
/// `None` (NULL — the caller falls back to the generic path so MySQL
/// NULL ordering stays correct).
///
/// Encoding: we map each `i64` to a `u64` whose unsigned order matches
/// the signed order — flipping the sign bit. For descending, we
/// additionally bitwise-NOT, so descending order maps to ascending u64
/// order. The pair `(encoded_u64, original_index)` is sorted with
/// `sort_unstable_by_key`, which the compiler reduces to a direct
/// integer compare (no closure dispatch, no `Value` enum match), then
/// `apply_sorted_perm_in_place_raw` walks the cycles in O(n).
///
/// Why "fast path" matters: the generic `compare_value_for_sort` path
/// dispatches on `Value` enum twice per comparison plus a per-step
/// NULL check, so an N-row sort runs ~N log N enum matches. For wide
/// numeric scans (`ORDER BY id`, `ORDER BY o_orderkey`, etc.) this
/// dominates the sort. Replacing it with a u64 compare cuts CPU
/// dramatically — and `sort_unstable_by_key` over `(u64,u32)` is
/// stable on ties because we encode the original index as the
/// secondary key.
#[cfg(test)]
pub(crate) fn try_sort_by_int_key<T>(
    items: &mut Vec<T>,
    asc: bool,
    mut key_fn: impl FnMut(&T) -> Option<i64>,
) -> bool {
    let n = items.len();
    if n < 2 {
        return true;
    }
    debug_assert!(n <= u32::MAX as usize);
    let mut keys: Vec<(u64, u32)> = Vec::with_capacity(n);
    for (i, item) in items.iter().enumerate() {
        match key_fn(item) {
            Some(v) => keys.push((encode_i64_for_sort(v, asc), i as u32)),
            None => return false,
        }
    }
    keys.sort_unstable_by_key(|(u, idx)| (*u, *idx));
    apply_sorted_perm_in_place_raw(items.as_mut_slice(), keys.iter().map(|(_, i)| *i));
    true
}

/// Specialised single-column quickselect for `i64` keys. Partitions
/// `items` so the first `nth` entries are the smallest (still
/// unordered among themselves, like `select_nth_unstable_by`). Returns
/// `true` if the fast path completed, `false` on any NULL (so the
/// caller can fall back to the generic Schwartzian quickselect).
#[cfg(test)]
pub(crate) fn try_select_nth_by_int_key<T>(
    items: &mut Vec<T>,
    asc: bool,
    nth: usize,
    mut key_fn: impl FnMut(&T) -> Option<i64>,
) -> bool {
    let n = items.len();
    if n < 2 || nth >= n {
        return true;
    }
    debug_assert!(n <= u32::MAX as usize);
    let mut keys: Vec<(u64, u32)> = Vec::with_capacity(n);
    for (i, item) in items.iter().enumerate() {
        match key_fn(item) {
            Some(v) => keys.push((encode_i64_for_sort(v, asc), i as u32)),
            None => return false,
        }
    }
    keys.select_nth_unstable_by_key(nth, |(u, idx)| (*u, *idx));
    apply_sorted_perm_in_place_raw(items.as_mut_slice(), keys.iter().map(|(_, i)| *i));
    true
}

/// Map an `i64` into a `u64` whose unsigned order matches the signed
/// order; flip for descending. Symmetric pure function, used as
/// `pub(crate)` helper so the tests can also assert it.
#[inline]
pub(crate) fn encode_i64_for_sort(v: i64, asc: bool) -> u64 {
    let unsigned = (v as u64) ^ (1u64 << 63);
    if asc {
        unsigned
    } else {
        !unsigned
    }
}

/// Multi-column Int sort fast path. Extends [`try_sort_by_int_key`]
/// to `K` columns of `Int`-typed keys. Returns `true` when the sort
/// completes via this path, `false` when any row hands back `None`
/// for some column (NULL — caller falls back to the generic path).
///
/// Storage layout: keys are packed in column-major SoA into a single
/// `Vec<u64>` of length `n * k` (row `i`, column `j` lives at
/// `i*k + j`), and a `Vec<u32>` of original row indices is sorted by
/// the lexicographic order of each row's `k`-element key slice. This
/// gives the comparator a tight `[u64]` cmp the optimiser is happy to
/// auto-vectorise (NEON/AVX2 compare-first-mismatch) — instead of the
/// `Value` enum dispatch the generic path pays per comparison.
///
/// Sort is *unstable* with an explicit `then(orig_idx.cmp)` tie-break,
/// so the operator is stable on equal keys (the secondary sort key is
/// the row's position in the input, which preserves source order).
///
/// Caveats:
///   - Bails if any column resolves to NULL on any row; MySQL NULL
///     ordering rules need the generic path.
///   - Stays a Schwartzian-style pre-encode: we eat one expression
///     evaluation per `(row, column)`; the same evals would happen in
///     the generic path. The win is in the comparator hot loop.
pub(crate) fn try_sort_by_int_keys_multi<T>(
    items: &mut Vec<T>,
    direction: &[bool],
    mut key_fn: impl FnMut(&T, usize) -> Option<i64>,
) -> bool {
    let n = items.len();
    let k = direction.len();
    if n < 2 || k == 0 {
        return true;
    }
    debug_assert!(n <= u32::MAX as usize);
    let mut keys_flat: Vec<u64> = Vec::with_capacity(n.saturating_mul(k));
    for (i, item) in items.iter().enumerate() {
        let _ = i;
        for (col, &asc) in direction.iter().enumerate() {
            match key_fn(item, col) {
                Some(v) => keys_flat.push(encode_i64_for_sort(v, asc)),
                None => return false,
            }
        }
    }
    let mut order: Vec<u32> = (0..n as u32).collect();
    if k == 1 {
        // A single encoded key needs no slice construction or lexicographic
        // comparison. Keep the same buffers and original-row tie breaker.
        order.sort_unstable_by(|&a, &b| {
            keys_flat[a as usize]
                .cmp(&keys_flat[b as usize])
                .then_with(|| a.cmp(&b))
        });
    } else {
        order.sort_unstable_by(|&a, &b| {
            // SAFETY note for the slice indexing: `a` and `b` are valid row
            // indices [0, n), and `keys_flat.len() == n * k`. The compiler
            // can prove these slices are equal-length so it reduces the
            // tuple comparator to a memcmp-like loop.
            let aa = &keys_flat[(a as usize) * k..(a as usize) * k + k];
            let bb = &keys_flat[(b as usize) * k..(b as usize) * k + k];
            aa.cmp(bb).then_with(|| a.cmp(&b))
        });
    }

    apply_sorted_perm_in_place_raw(items.as_mut_slice(), order);
    true
}

/// Multi-column quickselect counterpart of
/// [`try_sort_by_int_keys_multi`]. Partitions `items` so the first
/// `nth` entries are the smallest `nth` keys (still unordered among
/// themselves); used by `apply_ordered_limit` when LIMIT is small.
pub(crate) fn try_select_nth_by_int_keys_multi<T>(
    items: &mut Vec<T>,
    direction: &[bool],
    nth: usize,
    mut key_fn: impl FnMut(&T, usize) -> Option<i64>,
) -> bool {
    let n = items.len();
    let k = direction.len();
    if n < 2 || k == 0 || nth >= n {
        return true;
    }
    debug_assert!(n <= u32::MAX as usize);
    let mut keys_flat: Vec<u64> = Vec::with_capacity(n.saturating_mul(k));
    for item in items.iter() {
        for (col, &asc) in direction.iter().enumerate() {
            match key_fn(item, col) {
                Some(v) => keys_flat.push(encode_i64_for_sort(v, asc)),
                None => return false,
            }
        }
    }
    let mut order: Vec<u32> = (0..n as u32).collect();
    order.select_nth_unstable_by(nth, |&a, &b| {
        let aa = &keys_flat[(a as usize) * k..(a as usize) * k + k];
        let bb = &keys_flat[(b as usize) * k..(b as usize) * k + k];
        aa.cmp(bb).then_with(|| a.cmp(&b))
    });
    apply_sorted_perm_in_place_raw(items.as_mut_slice(), order);
    true
}

mod topk;
pub(crate) use topk::{BoundedTopK, PayloadThenOrdinal, TopKCandidates, TopKSet};

#[cfg(test)]
mod tests {
    use super::*;
    impl AllocatedBytes for i64 {
        fn allocated_bytes(&self) -> usize {
            0
        }
    }
    impl AllocatedBytes for u32 {
        fn allocated_bytes(&self) -> usize {
            0
        }
    }
    impl AllocatedBytes for &str {
        fn allocated_bytes(&self) -> usize {
            0
        }
    }

    #[test]
    fn mysql_null_ordering_is_direction_aware() {
        let asc = SortField::mysql_order(true);
        let desc = SortField::mysql_order(false);
        assert_eq!(
            compare_value_for_sort(&Value::Null, &Value::Int(1), asc),
            Ordering::Less
        );
        assert_eq!(
            compare_value_for_sort(&Value::Null, &Value::Int(1), desc),
            Ordering::Greater
        );
    }

    #[test]
    fn default_string_ordering_uses_mysql80_no_pad_collation() {
        let field = SortField::mysql_order(true);
        assert_eq!(
            compare_value_for_sort(
                &Value::String("abc   ".into()),
                &Value::String("abc".into()),
                field,
            ),
            Ordering::Greater
        );
    }

    #[test]
    fn explicit_pad_space_collation_still_ignores_trailing_spaces() {
        let mut field = SortField::mysql_order(true);
        field.collation_id = Some(crate::charset::collation_id("latin1_swedish_ci"));
        assert_eq!(
            compare_value_for_sort(
                &Value::String("abc   ".into()),
                &Value::String("abc".into()),
                field,
            ),
            Ordering::Equal
        );
    }

    #[test]
    fn typed_string_sort_uses_collation_without_dynamic_coercion() {
        let mut field = SortField::mysql_order(true);
        field.collation_id = Some(crate::charset::collation_id("utf8mb4_0900_ai_ci"));
        let left = Value::SmallString("2024-01-02".as_bytes().into());
        let right = Value::String("2024-01-10".into());
        assert_eq!(
            compare_value_for_sort(&left, &right, field),
            crate::charset::compare_strings_by_id(
                field.collation_id.unwrap(),
                "2024-01-02",
                "2024-01-10",
            )
        );
    }

    #[test]
    fn exact_numeric_sort_preserves_scale_and_integer_boundaries() {
        let cases = [
            (Value::Int(i64::MAX), Value::UInt(u64::MAX), Ordering::Less),
            (Value::Int(-1), Value::UInt(0), Ordering::Less),
            (
                Value::Decimal128(i128::MIN, 30),
                Value::Decimal128(i128::MAX, 30),
                Ordering::Less,
            ),
            (
                Value::DecimalCompact(123, 2),
                Value::Decimal128(123, 2),
                Ordering::Equal,
            ),
            (
                Value::DecimalCompact(-123, 2),
                Value::Decimal128(-1230, 3),
                Ordering::Equal,
            ),
            (
                Value::Decimal128(i128::MAX, 0),
                Value::Decimal128(i128::MAX, 1),
                Ordering::Greater,
            ),
            (
                Value::Decimal(
                    "99999999999999999999999999999999999999999999999999999999999999999".into(),
                ),
                Value::Decimal128(i128::MAX, 0),
                Ordering::Greater,
            ),
        ];
        for (left, right, expected) in cases {
            for ascending in [true, false] {
                let field = SortField::mysql_order(ascending);
                let expected = if ascending {
                    expected
                } else {
                    expected.reverse()
                };
                assert_eq!(compare_value_for_sort(&left, &right, field), expected);
                assert_eq!(
                    compare_value_for_sort(&right, &left, field),
                    expected.reverse()
                );
            }
        }
    }

    #[test]
    fn mixed_numeric_lanes_form_a_total_order() {
        let field = SortField::mysql_order(true);
        let values = [
            Value::Double(f64::NEG_INFINITY),
            Value::Int(-1),
            Value::Double(-0.0),
            Value::DecimalCompact(0, 4),
            Value::UInt(0),
            Value::Double(0.0),
            Value::DecimalCompact(9_007_199_254_740_992, 0),
            Value::Double(9_007_199_254_740_992.0),
            Value::Decimal128(9_007_199_254_740_993, 0),
            Value::Double(f64::INFINITY),
            Value::Double(f64::NAN),
        ];
        for a in &values {
            for b in &values {
                assert_eq!(
                    compare_value_for_sort(a, b, field),
                    compare_value_for_sort(b, a, field).reverse()
                );
                for c in &values {
                    let ab = compare_value_for_sort(a, b, field);
                    let bc = compare_value_for_sort(b, c, field);
                    if ab != Ordering::Greater && bc != Ordering::Greater {
                        assert_ne!(compare_value_for_sort(a, c, field), Ordering::Greater);
                    }
                }
            }
        }
    }

    #[test]
    fn mixed_runtime_sort_domains_form_a_total_order_without_predicate_coercion() {
        let values = vec![
            Value::Null,
            Value::Int(-7),
            Value::UInt(u64::MAX),
            Value::Float(-0.0),
            Value::Double(f64::NAN),
            Value::Double(1.5),
            Value::DecimalCompact(175, 2),
            Value::Decimal("1.75".into()),
            Value::String("a".into()),
            Value::String("a ".into()),
            Value::SmallString(b"a".as_slice().into()),
            Value::String("2024-01-01".into()),
            Value::Bytes(vec![0, 255]),
            Value::Bytes(b"a".to_vec()),
            Value::Date(0),
            Value::Date(crate::types::ZERO_DATE),
            Value::Time(-123),
            Value::DateTime(0),
            Value::Timestamp(0),
            Value::Year(0),
            Value::Json(crate::engine::json::parse_text_opt("{}").unwrap()),
            Value::Geometry(vec![0; 5]),
            Value::Vector(vec![0; 4].into()),
        ];
        for ascending in [true, false] {
            for collation_id in [None, Some(33), Some(255)] {
                let field = SortField {
                    ascending,
                    nulls_first: ascending,
                    collation_id,
                };
                for a in &values {
                    for b in &values {
                        let ab = compare_value_for_sort(a, b, field);
                        assert_eq!(ab, compare_value_for_sort(b, a, field).reverse());
                        for c in &values {
                            let bc = compare_value_for_sort(b, c, field);
                            if ab != Ordering::Greater && bc != Ordering::Greater {
                                assert_ne!(
                                    compare_value_for_sort(a, c, field),
                                    Ordering::Greater,
                                    "non-transitive {a:?}, {b:?}, {c:?}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn topk_budget_tracks_live_payloads_and_transfers_candidates() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        use std::sync::Arc;
        let budget = Arc::new(MemBudget::new(64 * 1024));
        with_query_budget(budget.clone(), || {
            let fields = [SortField::mysql_order(true)];
            let mut worker = BoundedTopK::new(&fields, usize::MAX).unwrap();
            for value in 0..10 {
                worker
                    .push(
                        vec![Value::String("x".repeat(128))],
                        vec![Value::Int(value)],
                    )
                    .unwrap();
            }
            let before = budget.used();
            let mut candidates = worker.drain_sorted();
            assert_eq!(
                before,
                budget.used(),
                "sorting must not release live payloads"
            );
            let mut master = BoundedTopK::new(&fields, 2).unwrap();
            let one = candidates.next().unwrap();
            assert!(
                budget.used() >= before,
                "extracting a candidate retains its charge"
            );
            master.push_candidate(one).unwrap();
            for candidate in candidates {
                master.push_candidate(candidate).unwrap();
            }
            assert!(
                budget.used() < before,
                "rejected worker rows must be released"
            );
            assert_eq!(master.into_sorted_vec().unwrap().len(), 2);
        });
        assert_eq!(budget.used(), 0);
        assert!(budget.peak_used() < 16 * 1024);
    }

    #[test]
    fn topk_continues_above_query_soft_watermark() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        use std::sync::Arc;
        let budget = Arc::new(MemBudget::process_tracked_for_test(8192));
        with_query_budget(budget.clone(), || {
            let mut heap = BoundedTopK::new(&[SortField::mysql_order(true)], 20).unwrap();
            // Model a spilling producer retaining its bounded I/O buffers.
            let mut producer = ScopedCharge::new(0);
            producer.add_force(8192).unwrap();
            for value in (0..100).rev() {
                heap.push(
                    vec![Value::Int(value), Value::String("x".repeat(1024))],
                    vec![Value::Int(value)],
                )
                .unwrap();
            }
            let rows = heap.into_sorted_vec().unwrap();
            assert_eq!(rows.len(), 20);
            for (value, row) in rows.iter().enumerate() {
                assert_eq!(row[0], Value::Int(value as i64));
            }
            assert_eq!(budget.used(), 8192);
        });
        assert_eq!(budget.used(), 0);
        assert!(budget.peak_used() < 64 * 1024);
    }

    #[test]
    fn topk_wide_values_enforce_budget_and_release_on_error() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        use std::sync::Arc;
        for constructor in [Value::Bytes, Value::Json, Value::Geometry, |v: Vec<u8>| {
            Value::Vector(v.into())
        }] {
            let budget = Arc::new(MemBudget::new(4096));
            with_query_budget(budget.clone(), || {
                let mut heap = BoundedTopK::new(&[SortField::mysql_order(true)], 1).unwrap();
                heap.push(vec![Value::Int(1)], vec![Value::Int(10)])
                    .unwrap();
                let used = budget.used();
                // A losing payload is neither copied nor charged.
                heap.push(vec![constructor(vec![0; 8192])], vec![Value::Int(20)])
                    .unwrap();
                assert_eq!(budget.used(), used);
                let err = heap
                    .push(vec![constructor(vec![0; 8192])], vec![Value::Int(0)])
                    .unwrap_err();
                assert_eq!(err.code(), 1037);
                assert_eq!(budget.used(), used);
                assert_eq!(heap.into_sorted_vec().unwrap(), vec![vec![Value::Int(1)]]);
            });
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn topk_eviction_keeps_old_payload_charged_until_dropped() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        use std::sync::Arc;
        let budget = Arc::new(MemBudget::new(64 * 1024));
        with_query_budget(budget.clone(), || {
            let mut heap = BoundedTopK::new(&[SortField::mysql_order(true)], 1).unwrap();
            heap.push(vec![Value::String("x".repeat(4096))], vec![Value::Int(10)])
                .unwrap();
            let before = budget.used();
            let (_, old) = heap
                .push_with_evicted(vec![Value::Int(1)], vec![Value::Int(0)])
                .unwrap();
            assert!(budget.used() >= before);
            drop(old);
            assert!(budget.used() < before);
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn topk_distinct_set_grows_with_members_and_enforces_budget() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        use std::sync::Arc;
        let budget = Arc::new(MemBudget::new(4096));
        with_query_budget(budget.clone(), || {
            let mut set = TopKSet::new();
            assert_eq!(budget.used(), 0);
            for value in 0..10u64 {
                set.insert(value).unwrap();
            }
            assert!(budget.used() < 1024);
            let mut exceeded = false;
            for value in 10..10000 {
                if set.insert(value).is_err() {
                    exceeded = true;
                    break;
                }
            }
            assert!(exceeded);
            assert!(set.contains(&0));
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn bounded_topk_keeps_best_rows_in_order() {
        let fields = [SortField::mysql_order(true)];
        let mut topk = BoundedTopK::new(&fields, 3).unwrap();
        for v in [7, 2, 5, 1, 9] {
            topk.push(v, vec![Value::Int(v)]).unwrap();
        }
        assert_eq!(topk.into_sorted_vec().unwrap(), vec![1, 2, 5]);
    }

    #[test]
    fn would_accept_admits_when_heap_not_full_then_rejects_worse() {
        let fields = [SortField::mysql_order(true)];
        let mut topk = BoundedTopK::new(&fields, 2).unwrap();
        // Empty heap accepts anything.
        assert!(topk.would_accept(&[Value::Int(100)]));
        topk.push(1u32, vec![Value::Int(3)]).unwrap();
        // K=2, still room.
        assert!(topk.would_accept(&[Value::Int(100)]));
        topk.push(2u32, vec![Value::Int(5)]).unwrap();
        // Heap full: worst is 5. Strictly better admitted.
        assert!(topk.would_accept(&[Value::Int(4)]));
        // Worse rejected.
        assert!(!topk.would_accept(&[Value::Int(6)]));
        // Tie on key rejected — push would lose on the ordinal tiebreak.
        assert!(!topk.would_accept(&[Value::Int(5)]));
    }

    #[test]
    fn would_accept_matches_push_outcome_stably() {
        // Run the same input two ways:
        //  (a) blindly push every row (legacy semantics)
        //  (b) gate push behind would_accept (new fast path)
        // The output must match exactly, in order — that's the stability
        // contract clients of `apply_ordered_limit` rely on.
        let fields = [SortField::mysql_order(true)];
        let inputs: Vec<(i64, &str)> = vec![
            (3, "a"),
            (1, "b"),
            (4, "c"),
            (1, "d"),
            (5, "e"),
            (9, "f"),
            (2, "g"),
            (6, "h"),
            (5, "i"),
            (3, "j"),
        ];
        let mut blind = BoundedTopK::new(&fields, 4).unwrap();
        let mut gated = BoundedTopK::new(&fields, 4).unwrap();
        for (n, tag) in &inputs {
            let keys = vec![Value::Int(*n)];
            blind.push(*tag, keys.clone()).unwrap();
            if gated.would_accept(&keys) {
                gated.push(*tag, keys).unwrap();
            }
        }
        assert_eq!(
            blind.into_sorted_vec().unwrap(),
            gated.into_sorted_vec().unwrap()
        );
    }

    #[test]
    fn cycle_leader_permute_handles_empty_and_singleton() {
        let fields = [SortField::mysql_order(true)];
        let mut empty: Vec<i32> = Vec::new();
        sort_by_cached_keys(&mut empty, &fields, |v| vec![Value::Int(*v as i64)]);
        assert!(empty.is_empty());
        let mut single: Vec<i32> = vec![42];
        sort_by_cached_keys(&mut single, &fields, |v| vec![Value::Int(*v as i64)]);
        assert_eq!(single, vec![42]);
    }

    #[test]
    fn cycle_leader_permute_is_stable_on_duplicate_keys() {
        // (key, payload). Duplicate keys must preserve insertion order.
        let fields = [SortField::mysql_order(true)];
        let inputs: Vec<(i64, &str)> = vec![
            (3, "a"),
            (1, "b"),
            (3, "c"),
            (1, "d"),
            (2, "e"),
            (3, "f"),
            (1, "g"),
            (2, "h"),
        ];
        let mut data = inputs.clone();
        sort_by_cached_keys(&mut data, &fields, |(k, _)| vec![Value::Int(*k)]);
        let expected: Vec<(i64, &str)> = vec![
            (1, "b"),
            (1, "d"),
            (1, "g"),
            (2, "e"),
            (2, "h"),
            (3, "a"),
            (3, "c"),
            (3, "f"),
        ];
        assert_eq!(data, expected);
    }

    #[test]
    fn cycle_leader_permute_handles_already_sorted_and_reversed() {
        let fields = [SortField::mysql_order(true)];
        // Already-sorted input must be a no-op (identity permutation).
        let mut sorted: Vec<i64> = (0..256).collect();
        let before_addr = sorted.as_ptr() as usize;
        sort_by_cached_keys(&mut sorted, &fields, |v| vec![Value::Int(*v)]);
        assert_eq!(sorted, (0..256).collect::<Vec<_>>());
        // In-place: pointer unchanged (no Vec::reserve / reallocation).
        assert_eq!(sorted.as_ptr() as usize, before_addr);
        // Fully reversed: single long cycle, hardest case for the in-place
        // swap loop.
        let mut reversed: Vec<i64> = (0..256).rev().collect();
        sort_by_cached_keys(&mut reversed, &fields, |v| vec![Value::Int(*v)]);
        assert_eq!(reversed, (0..256).collect::<Vec<_>>());
    }

    #[test]
    fn cycle_leader_permute_matches_legacy_under_randomized_inputs() {
        // Deterministic LCG-shuffled inputs, all-equal-key inputs, sparse-key
        // inputs. Compare against `slice::sort_by` ground truth.
        let fields = [SortField::mysql_order(true)];
        let mut rng: u64 = 0xC0FFEE;
        let mut next = || {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            rng
        };
        for &n in &[2usize, 7, 64, 1023, 2048] {
            let raw: Vec<(i64, u32)> = (0..n).map(|i| ((next() % 17) as i64, i as u32)).collect();
            let mut actual = raw.clone();
            sort_by_cached_keys(&mut actual, &fields, |(k, _)| vec![Value::Int(*k)]);
            // Ground truth: stable sort by key (slice::sort_by is stable).
            let mut expected = raw.clone();
            expected.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(actual, expected, "n={n}");
        }
    }

    #[test]
    fn cycle_leader_select_nth_partitions_correctly() {
        let fields = [SortField::mysql_order(true)];
        let inputs: Vec<i64> = vec![7, 2, 9, 1, 5, 3, 8, 4, 6, 0];
        // After select_nth at 4, items[0..=4] should be the 5 smallest
        // (in some order), items[5..] should be the 5 largest.
        let mut data = inputs.clone();
        let keys = data.iter().map(|v| vec![Value::Int(*v)]).collect();
        select_nth_by_precomputed_keys(&mut data, &fields, 4, keys);
        let mut lo: Vec<i64> = data[..=4].to_vec();
        let mut hi: Vec<i64> = data[5..].to_vec();
        lo.sort();
        hi.sort();
        assert_eq!(lo, vec![0, 1, 2, 3, 4]);
        assert_eq!(hi, vec![5, 6, 7, 8, 9]);
    }

    #[test]
    fn encode_i64_for_sort_preserves_signed_order_asc() {
        let a = encode_i64_for_sort(-100, true);
        let b = encode_i64_for_sort(0, true);
        let c = encode_i64_for_sort(100, true);
        let d = encode_i64_for_sort(i64::MIN, true);
        let e = encode_i64_for_sort(i64::MAX, true);
        assert!(d < a);
        assert!(a < b);
        assert!(b < c);
        assert!(c < e);
    }

    #[test]
    fn encode_i64_for_sort_preserves_signed_order_desc() {
        // DESC: bigger int → smaller encoded u64.
        let a = encode_i64_for_sort(-100, false);
        let b = encode_i64_for_sort(0, false);
        let c = encode_i64_for_sort(100, false);
        assert!(c < b);
        assert!(b < a);
    }

    #[test]
    fn try_sort_by_int_key_asc_basic() {
        let mut items: Vec<(i64, char)> = vec![(3, 'a'), (1, 'b'), (4, 'c'), (1, 'd'), (5, 'e')];
        let ok = try_sort_by_int_key(&mut items, true, |(v, _)| Some(*v));
        assert!(ok);
        // Stable on ties: 'b' before 'd'.
        assert_eq!(
            items,
            vec![(1, 'b'), (1, 'd'), (3, 'a'), (4, 'c'), (5, 'e')]
        );
    }

    #[test]
    fn try_sort_by_int_key_desc_basic() {
        let mut items: Vec<i64> = vec![3, 1, 4, 1, 5, 9, 2, 6];
        let ok = try_sort_by_int_key(&mut items, false, |v| Some(*v));
        assert!(ok);
        assert_eq!(items, vec![9, 6, 5, 4, 3, 2, 1, 1]);
    }

    #[test]
    fn try_sort_by_int_key_bails_on_null() {
        let mut items: Vec<Option<i64>> = vec![Some(3), None, Some(1)];
        let original = items.clone();
        let ok = try_sort_by_int_key(&mut items, true, |v| *v);
        assert!(!ok);
        // On bail, items are left untouched up to the bail point — we
        // don't promise *no* mutation but the contract is "the caller
        // must take the generic fallback". Verify we returned `false`.
        let _ = original;
    }

    #[test]
    fn try_sort_by_int_key_matches_serial_sort_under_randomized_inputs() {
        let mut rng: u64 = 0xBEEF_FACE;
        let mut next = || {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            rng
        };
        for &n in &[2usize, 31, 256, 4096] {
            let raw: Vec<i64> = (0..n).map(|_| (next() % 1024) as i64 - 512).collect();
            let mut actual = raw.clone();
            let ok = try_sort_by_int_key(&mut actual, true, |v| Some(*v));
            assert!(ok);
            let mut expected = raw.clone();
            expected.sort();
            assert_eq!(actual, expected, "n={n} ASC");

            let mut actual_desc = raw.clone();
            let ok = try_sort_by_int_key(&mut actual_desc, false, |v| Some(*v));
            assert!(ok);
            let mut expected_desc = raw.clone();
            expected_desc.sort_by(|a, b| b.cmp(a));
            assert_eq!(actual_desc, expected_desc, "n={n} DESC");
        }
    }

    #[test]
    fn integer_single_key_matches_stable_sort_and_preserves_null_fallback() {
        for ascending in [true, false] {
            for n in [0, 1, 2, 31, 257, 2048] {
                let keys = [i64::MAX, 0, -1, i64::MIN, 17, 0, i64::MAX];
                let original: Vec<_> = (0..n).map(|i| (keys[i % keys.len()], i)).collect();
                let mut actual = original.clone();
                assert!(try_sort_by_int_keys_multi(
                    &mut actual,
                    &[ascending],
                    |row, _| Some(row.0)
                ));
                let mut expected = original;
                expected.sort_by(|a, b| {
                    if ascending {
                        a.0.cmp(&b.0)
                    } else {
                        b.0.cmp(&a.0)
                    }
                });
                assert_eq!(actual, expected, "n={n}, ascending={ascending}");
            }
            let original = vec![Some(3), Some(1), None, Some(2)];
            let mut actual = original.clone();
            assert!(!try_sort_by_int_keys_multi(
                &mut actual,
                &[ascending],
                |row, _| *row
            ));
            assert_eq!(actual, original, "fallback must not reorder its input");
        }
    }

    #[test]
    fn try_sort_by_int_keys_multi_two_columns_asc_then_desc() {
        // ORDER BY a ASC, b DESC over 3-column rows. Tie on `a` resolves
        // by `b` DESC, then by row index (stable).
        let mut rows: Vec<(i64, i64, &'static str)> = vec![
            (2, 30, "x"),
            (1, 10, "a"),
            (2, 20, "y"),
            (1, 30, "b"),
            (2, 30, "z"),
        ];
        let direction = [true, false]; // a ASC, b DESC
        let ok = try_sort_by_int_keys_multi(&mut rows, &direction, |r, col| {
            Some(if col == 0 { r.0 } else { r.1 })
        });
        assert!(ok);
        // Expected: a=1: (1,30,"b") before (1,10,"a") (b DESC);
        // then a=2: b descending: (2,30,"x"), (2,30,"z") (stable tie),
        // (2,20,"y").
        assert_eq!(
            rows,
            vec![
                (1, 30, "b"),
                (1, 10, "a"),
                (2, 30, "x"),
                (2, 30, "z"),
                (2, 20, "y"),
            ]
        );
    }

    #[test]
    fn try_sort_by_int_keys_multi_bails_on_null_in_any_column() {
        let mut rows: Vec<(Option<i64>, Option<i64>)> =
            vec![(Some(1), Some(2)), (Some(3), None), (Some(5), Some(6))];
        let direction = [true, true];
        let ok =
            try_sort_by_int_keys_multi(
                &mut rows,
                &direction,
                |r, col| {
                    if col == 0 {
                        r.0
                    } else {
                        r.1
                    }
                },
            );
        assert!(!ok, "must bail because row 1 has NULL in col 1");
    }

    #[test]
    fn try_sort_by_int_keys_multi_matches_generic_under_randomized_inputs() {
        let mut rng: u64 = 0xAAAA_5555_C0DE_F00D;
        let mut next = || {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            rng
        };
        for &n in &[2usize, 31, 256, 2048] {
            let raw: Vec<(i64, i64, i64, u32)> = (0..n)
                .map(|i| {
                    (
                        (next() % 13) as i64,
                        (next() % 11) as i64 - 5,
                        (next() % 7) as i64,
                        i as u32,
                    )
                })
                .collect();
            let mut actual = raw.clone();
            let direction = [true, false, true];
            let ok = try_sort_by_int_keys_multi(&mut actual, &direction, |r, col| {
                Some(match col {
                    0 => r.0,
                    1 => r.1,
                    _ => r.2,
                })
            });
            assert!(ok);
            let mut expected = raw.clone();
            expected.sort_by(|a, b| {
                a.0.cmp(&b.0)
                    .then(b.1.cmp(&a.1)) // DESC on col 1
                    .then(a.2.cmp(&b.2))
                    .then(a.3.cmp(&b.3)) // stable on row idx
            });
            assert_eq!(actual, expected, "n={n}");
        }
    }

    #[test]
    fn try_select_nth_by_int_keys_multi_partitions_correctly() {
        let mut rows: Vec<(i64, i64, u32)> = vec![
            (5, 1, 0),
            (2, 9, 1),
            (5, 0, 2),
            (1, 7, 3),
            (3, 4, 4),
            (2, 2, 5),
            (4, 6, 6),
            (1, 0, 7),
        ];
        let direction = [true, true];
        let nth = 3;
        let ok = try_select_nth_by_int_keys_multi(&mut rows, &direction, nth, |r, col| {
            Some(if col == 0 { r.0 } else { r.1 })
        });
        assert!(ok);
        // First 4 entries should be the smallest 4 by (a,b) lex order:
        // (1,0), (1,7), (2,2), (2,9). Set membership is what matters.
        let mut lo: Vec<(i64, i64)> = rows[..=nth].iter().map(|r| (r.0, r.1)).collect();
        lo.sort();
        assert_eq!(lo, vec![(1, 0), (1, 7), (2, 2), (2, 9)]);
    }

    #[test]
    fn try_select_nth_by_int_key_partitions_correctly() {
        let mut items: Vec<i64> = vec![7, 2, 9, 1, 5, 3, 8, 4, 6, 0];
        let ok = try_select_nth_by_int_key(&mut items, true, 4, |v| Some(*v));
        assert!(ok);
        let mut lo: Vec<i64> = items[..=4].to_vec();
        let mut hi: Vec<i64> = items[5..].to_vec();
        lo.sort();
        hi.sort();
        assert_eq!(lo, vec![0, 1, 2, 3, 4]);
        assert_eq!(hi, vec![5, 6, 7, 8, 9]);
    }

    #[test]
    fn would_accept_admits_strictly_better_when_full() {
        // Critical correctness: with a full heap and a candidate strictly
        // better than the worst entry, would_accept MUST return true.
        // We use distinct item/key values to verify the heap stores the
        // item against the right key (not by accident lining them up).
        let fields = [SortField::mysql_order(true)];
        let mut topk = BoundedTopK::new(&fields, 3).unwrap();
        // Push 3 entries with item=key, so heap contents are
        // [(item=10,key=10), (item=20,key=20), (item=30,key=30)].
        for v in [10i64, 20, 30] {
            topk.push(v, vec![Value::Int(v)]).unwrap();
        }
        // Worst is the entry with key=30. Candidate with key=5 is
        // strictly better, so it evicts that entry; we tag its item as
        // 99 so the final vec lets us spot the substitution.
        assert!(topk.would_accept(&[Value::Int(5)]));
        topk.push(99i64, vec![Value::Int(5)]).unwrap();
        // Final keys ascending: 5 → item 99, 10 → item 10, 20 → item 20.
        let out = topk.into_sorted_vec().unwrap();
        assert_eq!(out, vec![99, 10, 20]);
    }
}
