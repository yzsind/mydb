//! Lossless mixed-radix integer keys with a sparse duplicate side table.
use super::*;

pub(crate) struct PackedBuildIndex {
    bounds: Vec<(i64, u64, u32)>,
    first: hashbrown::HashMap<u64, u32, ahash::RandomState>,
    duplicates: hashbrown::HashMap<u32, Vec<u32>, ahash::RandomState>,
    duplicate_bytes: usize,
}

impl PackedBuildIndex {
    pub(crate) fn layout(batches: &[Batch], keys: &[usize]) -> Option<Self> {
        if keys.is_empty() || keys.len() > 8 {
            return None;
        }
        // A stable fixed-width payload has a predictable retained capacity.
        // Mixed/variable-width batches keep the general collector and its
        // wider reservation; the compact key does not justify undercharging
        // their concatenation or an implicit Generic lane promotion.
        if let Some(first) = batches.first() {
            for batch in batches {
                if batch.columns().len() != first.columns().len() {
                    return None;
                }
                for (column, original) in batch.columns().iter().zip(first.columns()) {
                    if !matches!(
                        column,
                        ColumnVector::Null { .. }
                            | ColumnVector::I64 { .. }
                            | ColumnVector::U64 { .. }
                            | ColumnVector::F64 { .. }
                            | ColumnVector::Date32 { .. }
                            | ColumnVector::Time64 { .. }
                            | ColumnVector::DateTime64 { .. }
                            | ColumnVector::Timestamp64 { .. }
                            | ColumnVector::Decimal32 { .. }
                            | ColumnVector::Decimal64 { .. }
                            | ColumnVector::Decimal128 { .. }
                    ) || std::mem::discriminant(column) != std::mem::discriminant(original)
                        || column.decimal_scale() != original.decimal_scale()
                    {
                        return None;
                    }
                }
            }
        }
        let mut limits = vec![(i64::MAX, i64::MIN); keys.len()];
        let mut seen = false;
        for batch in batches {
            let columns = keys
                .iter()
                .map(|&k| batch.columns().get(k))
                .collect::<Option<Vec<_>>>()?;
            if columns
                .iter()
                .any(|c| !matches!(c, ColumnVector::I64 { .. } | ColumnVector::Null { .. }))
            {
                return None;
            }
            for row in batch.visible_indices() {
                if columns.iter().any(|column| column.is_null(row)) {
                    continue;
                }
                seen = true;
                for (limit, column) in limits.iter_mut().zip(&columns) {
                    let value = column.as_i64_slice()?[row];
                    limit.0 = limit.0.min(value);
                    limit.1 = limit.1.max(value);
                }
            }
        }
        let mut shift = 0u32;
        let mut bounds = Vec::with_capacity(keys.len());
        for (min, max) in limits {
            let (min, max) = if seen { (min, max) } else { (0, 0) };
            let span = u64::try_from(i128::from(max) - i128::from(min)).ok()?;
            let bits = u64::BITS - span.leading_zeros();
            if shift + bits > 64 {
                return None;
            }
            bounds.push((min, span, shift));
            shift += bits;
        }
        Some(Self {
            bounds,
            first: Default::default(),
            duplicates: Default::default(),
            duplicate_bytes: 0,
        })
    }

    /// Keep the existing direct-address pair index when its Cartesian domain
    /// fits the same density envelope as the ordinary pair collector.
    pub(crate) fn prefers_dense_pair(&self, rows: usize) -> bool {
        if self.bounds.len() != 2 {
            return false;
        }
        let limit = 64_000_000u128.min((rows as u128).saturating_mul(8));
        let a = u128::from(self.bounds[0].1) + 1;
        let b = u128::from(self.bounds[1].1) + 1;
        a <= limit && b <= limit && a * b <= limit
    }

    fn key(&self, batch: &Batch, keys: &[usize], row: usize) -> Option<u64> {
        let mut packed = 0u64;
        for (&column, &(min, span, shift)) in keys.iter().zip(&self.bounds) {
            let column = &batch.columns()[column];
            if column.is_null(row) {
                return None;
            }
            let value = match column.as_i64_slice() {
                Some(values) => values[row],
                None => match crate::engine::expr::ValueKey::from(&column.value_at(row)) {
                    crate::engine::expr::ValueKey::Int(value) => i64::try_from(value).ok()?,
                    _ => return None,
                },
            };
            let delta = u64::try_from(i128::from(value) - i128::from(min)).ok()?;
            if delta > span {
                return None;
            }
            // A constant trailing dimension may start at bit 64.
            if span != 0 {
                packed |= delta << shift;
            }
        }
        Some(packed)
    }

    pub(crate) fn lookup<'a>(
        &'a self,
        batch: &Batch,
        keys: &[usize],
        row: usize,
    ) -> (bool, Option<&'a [u32]>) {
        let usable = keys.iter().all(|&k| !batch.columns()[k].is_null(row));
        let hits = self
            .key(batch, keys, row)
            .and_then(|key| self.first.get(&key))
            .map(|first| {
                self.duplicates
                    .get(first)
                    .map(Vec::as_slice)
                    .unwrap_or_else(|| std::slice::from_ref(first))
            });
        (usable, hits)
    }

    pub(crate) fn all_unique(&self) -> bool {
        self.duplicates.is_empty()
    }

    fn memory_bytes(&self) -> usize {
        fn table<K, V>(capacity: usize) -> usize {
            if capacity == 0 {
                0
            } else {
                capacity
                    .next_power_of_two()
                    .saturating_mul(std::mem::size_of::<(K, V)>() + 1)
                    .saturating_add(32)
            }
        }
        table::<u64, u32>(self.first.capacity())
            .saturating_add(table::<u32, Vec<u32>>(self.duplicates.capacity()))
            .saturating_add(self.duplicate_bytes)
            .saturating_add(self.bounds.capacity() * std::mem::size_of::<(i64, u64, u32)>())
    }
}

/// The prefix is replayable until construction succeeds. Representation or
/// capacity refusal returns the existing overflow signal before any probe.
pub(crate) fn collect_packed_build_payload(
    batches: &[Batch],
    keys: &[usize],
    mut index: PackedBuildIndex,
) -> Result<BuildPayload, QueryError> {
    let rows: usize = batches.iter().map(Batch::visible_rows).sum();
    if rows >= u32::MAX as usize {
        return Err(vec_build_overflow_error());
    }
    let ncols = batches.first().map_or(0, |batch| batch.columns().len());
    let mut columns = vec![ColumnVector::Null { len: 0 }; ncols];
    let cap = current_vec_build_cap();
    // Reserve the first-hit table once; every later duplicate uses a small
    // separate list, leaving unique keys at one u32 per bucket.
    let table_bytes = if rows == 0 {
        0
    } else {
        rows.saturating_mul(8)
            .div_ceil(7)
            .next_power_of_two()
            .saturating_mul(17)
            .saturating_add(32)
    };
    if cap.is_some_and(|cap| cap.exceeded(rows as u64, table_bytes as u64)) {
        return Err(vec_build_overflow_error());
    }
    index
        .first
        .try_reserve(rows)
        .map_err(|_| vec_build_overflow_error())?;
    let mut next = 0u32;
    for batch in batches {
        crate::engine::cancel::check_cancelled()?;
        let mut keep = Vec::with_capacity(batch.visible_rows());
        for row in batch.visible_indices() {
            let Some(key) = index.key(batch, keys, row) else {
                continue;
            };
            match index.first.entry(key) {
                hashbrown::hash_map::Entry::Vacant(entry) => {
                    entry.insert(next);
                }
                hashbrown::hash_map::Entry::Occupied(entry) => {
                    let first = *entry.get();
                    let hits = index.duplicates.entry(first).or_insert_with(|| vec![first]);
                    let before = hits.capacity();
                    // Include the initial allocation when this is the first duplicate.
                    if hits.len() == 1 {
                        index.duplicate_bytes += before * std::mem::size_of::<u32>();
                    }
                    hits.push(next);
                    index.duplicate_bytes +=
                        (hits.capacity() - before) * std::mem::size_of::<u32>();
                }
            }
            keep.push(row);
            next += 1;
        }
        for (column, source) in columns.iter_mut().zip(batch.columns()) {
            if matches!(column, ColumnVector::Null { len: 0 })
                && !matches!(source, ColumnVector::Null { .. })
            {
                *column = source.empty_like_reserved(rows)?;
            }
            column.append_rows_from(source, &keep);
        }
        let bytes = columns
            .iter()
            .map(ColumnVector::memory_bytes)
            .sum::<usize>()
            .saturating_add(index.memory_bytes());
        if cap.is_some_and(|cap| cap.exceeded(next as u64, bytes as u64)) {
            return Err(vec_build_overflow_error());
        }
    }
    Ok(BuildPayload {
        columns,
        index: Default::default(),
        i64_index: None,
        i64_pair_index: None,
        packed_index: Some(index),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::vec::schema::BatchField;
    use crate::types::{ColumnType, Value};

    fn batch(rows: &[Vec<Value>]) -> Batch {
        let width = rows[0].len();
        let schema = Arc::new(BatchSchema::new(
            (0..width)
                .map(|i| BatchField {
                    table: "t".into(),
                    alias: None,
                    name: format!("c{i}"),
                    column_type: ColumnType::LongLong,
                    source: Default::default(),
                })
                .collect(),
        ));
        Batch::new(
            schema,
            (0..width)
                .map(|i| ColumnVector::from_values(rows.iter().map(|r| r[i].clone()).collect()))
                .collect(),
        )
    }

    #[test]
    fn packed_integer_keys_match_encoded_index_with_duplicates_nulls_and_selection() {
        for width in 2..=8 {
            let mut rows: Vec<Vec<Value>> = (0..100)
                .map(|row| {
                    (0..width)
                        .map(|col| {
                            Value::Int(if col == 0 {
                                -17
                            } else {
                                (row % 19) * (col as i64 + 1)
                            })
                        })
                        .collect()
                })
                .collect();
            rows[7][1] = Value::Null;
            let input = batch(&rows).with_selection(Selection::Indices(
                (0..100).filter(|r| r % 3 != 0).collect(),
            ));
            let keys: Vec<_> = (0..width).collect();
            let layout = PackedBuildIndex::layout(&[input.clone()], &keys).unwrap();
            let payload = collect_packed_build_payload(&[input.clone()], &keys, layout).unwrap();
            let packed = payload.packed_index.as_ref().unwrap();
            let retained: Vec<_> = input
                .visible_indices()
                .filter(|&r| rows[r].iter().all(|v| !matches!(v, Value::Null)))
                .map(|r| rows[r].clone())
                .collect();
            assert_eq!(payload.columns[0].len(), retained.len());
            let mut probes = rows.clone();
            let mut outside = rows[0].clone();
            outside[0] = Value::Int(-16);
            probes.push(outside);
            let mut outside = rows[0].clone();
            outside[1] = Value::Int(i64::MAX);
            probes.push(outside);
            let probe = batch(&probes);
            for (r, values) in probes.iter().enumerate() {
                let expected: Vec<u32> = retained
                    .iter()
                    .enumerate()
                    .filter(|(_, row)| {
                        *row == values && !values.iter().any(|v| matches!(v, Value::Null))
                    })
                    .map(|(i, _)| i as u32)
                    .collect();
                let (usable, hits) = packed.lookup(&probe, &keys, r);
                assert_eq!(usable, values.iter().all(|v| !matches!(v, Value::Null)));
                assert_eq!(hits.unwrap_or(&[]), expected);
            }
            for (r, expected) in retained.iter().enumerate() {
                assert_eq!(
                    payload
                        .columns
                        .iter()
                        .map(|c| c.value_at(r))
                        .collect::<Vec<_>>(),
                    *expected
                );
            }
        }
    }

    #[test]
    fn packed_integer_domains_cover_i64_extremes_and_reject_overwide_tuples() {
        let rows = vec![
            vec![Value::Int(i64::MIN), Value::Int(5), Value::Int(5)],
            vec![Value::Int(i64::MAX), Value::Int(5), Value::Int(5)],
        ];
        let input = batch(&rows);
        let layout = PackedBuildIndex::layout(&[input.clone()], &[0, 1, 2]).unwrap();
        let payload = collect_packed_build_payload(&[input.clone()], &[0, 1, 2], layout).unwrap();
        let index = payload.packed_index.unwrap();
        assert_eq!(index.lookup(&input, &[0, 1, 2], 0).1.unwrap(), &[0]);
        assert_eq!(index.lookup(&input, &[0, 1, 2], 1).1.unwrap(), &[1]);
        let mut overwide = rows.clone();
        overwide[1][1] = Value::Int(6);
        assert!(PackedBuildIndex::layout(&[batch(&overwide)], &[0, 1, 2]).is_none());
        let layout = PackedBuildIndex::layout(&[input.clone()], &[0, 1, 2]).unwrap();
        let error = with_vec_build_cap(
            BuildCap {
                max_rows: 0,
                max_bytes: 1,
            },
            || collect_packed_build_payload(&[input], &[0, 1, 2], layout),
        );
        assert!(error.is_err_and(|e| is_vec_build_overflow(&e)));
    }

    #[test]
    fn packed_probe_accepts_generic_and_equivalent_numeric_lanes() {
        let input = batch(&[vec![Value::Int(1), Value::Int(-2), Value::Int(3)]]);
        let keys = [0, 1, 2];
        let layout = PackedBuildIndex::layout(&[input.clone()], &keys).unwrap();
        let payload = collect_packed_build_payload(&[input.clone()], &keys, layout).unwrap();
        let probes = Batch::new(
            input.schema().clone(),
            vec![
                ColumnVector::Generic {
                    values: vec![Value::UInt(1), Value::String("1".into()), Value::Null],
                },
                ColumnVector::Generic {
                    values: vec![Value::DecimalCompact(-200, 2); 3],
                },
                ColumnVector::F64 {
                    data: vec![3.0; 3],
                    nulls: None,
                },
            ],
        );
        let index = payload.packed_index.unwrap();
        assert_eq!(index.lookup(&probes, &keys, 0), (true, Some(&[0u32][..])));
        assert_eq!(index.lookup(&probes, &keys, 1), (true, None));
        assert_eq!(index.lookup(&probes, &keys, 2), (false, None));
    }
}
