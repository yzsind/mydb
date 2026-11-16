//! Borrow flat exact aggregate states through parallel partition finalization.
use super::*;

pub(super) struct FrozenSumArena {
    pub(super) arena: FixedWidthStateArena,
    // Data is dropped before the reservation is returned. Partials may
    // outlive the worker core that produced them.
    pub(super) _grant: Option<crate::engine::executor::memory_grant::OperatorMemoryGrant>,
}

pub(super) struct ColumnarSumPartial {
    pub(super) owner: Arc<FrozenSumArena>,
    pub(super) partition: usize,
}

impl ColumnarSumPartial {
    fn rows(&self) -> &[u32] {
        &self.owner.arena.partition_groups[self.partition]
    }
    pub(super) fn into_buckets(self) -> Vec<GroupBucketSnapshot> {
        let arena = &self.owner.arena;
        self.rows()
            .iter()
            .map(|group| {
                let group = *group as usize;
                let key_values = arena.group_key_values(group);
                let mut key = Vec::with_capacity(key_values.len() * 9);
                for value in &key_values {
                    super::super::key_encode::encode(value, &mut key);
                }
                GroupBucketSnapshot {
                    key,
                    key_values,
                    aggs: (0..arena.n_aggs)
                        .map(|slot| arena.state_snapshot(group, slot))
                        .collect(),
                }
            })
            .collect()
    }
}

pub(super) fn eligible(arena: &FixedWidthStateArena) -> bool {
    arena.n_aggs > 0
        && (arena.key_kinds.len() > 1 || arena.n_aggs > 1)
        && arena.funcs.iter().all(|func| *func == AggregateKind::Sum)
        && arena.decimal_state_columns.iter().all(Option::is_some)
        && !arena.defer_i64_decimal_dedup
        && arena.compressed_i64_decimal_runs.is_empty()
}

pub(crate) fn restore_legacy_partials(partials: &mut [Vec<Option<PartitionPartial>>]) {
    for partial in partials.iter_mut().flatten().flatten() {
        if let Some(columns) = partial.columnar_sums.take() {
            *partial = fixed_width_partition_partial(&columns.owner.arena, columns.partition)
                .expect("a columnar partial has nonempty groups");
        }
    }
}

fn parts(column: &NarrowDecimalStateColumn, group: usize) -> Option<(i128, u8)> {
    match column.status[group] {
        COMPACT_SUM_EMPTY => Some((0, column.scale)),
        COMPACT_SUM_NARROW => Some((i128::from(column.magnitudes[group]), column.scale)),
        COMPACT_SUM_PROMOTED => {
            crate::engine::executor::select::aggregate::numeric_state_compact_parts(
                column.promoted.get(&(group as u32))?,
            )
        }
        _ => None,
    }
}
fn normalize(value: i128, scale: u8, target: u8) -> Option<i64> {
    if value == 0 {
        return Some(0);
    }
    i64::try_from(value.checked_mul(10i128.checked_pow(u32::from(target.checked_sub(scale)?))?)?)
        .ok()
}

/// An optional representation choice. Until success all input partials are
/// intact, so unsupported types, wide subtotals and memory pressure use the
/// canonical merger without reopening an input.
pub(crate) fn try_merge_columnar_sum_batches(
    partials: &mut Vec<Vec<Option<PartitionPartial>>>,
    aggs: &[AggregateSpec],
    schema: Arc<BatchSchema>,
) -> Result<Option<Vec<Batch>>, QueryError> {
    if aggs.is_empty() || aggs.len() > 8 || !aggs.iter().all(|spec| spec.func == AggregateKind::Sum)
    {
        return Ok(None);
    }
    let mut inputs = Vec::new();
    for partial in partials.iter().flatten().flatten() {
        let Some(input) = partial.columnar_sums.as_ref() else {
            return Ok(None);
        };
        if !eligible(&input.owner.arena) {
            return Ok(None);
        }
        inputs.push(input);
    }
    let Some(first) = inputs.first() else {
        return Ok(Some(Vec::new()));
    };
    let kinds = first.owner.arena.key_kinds.clone();
    if kinds.is_empty() || kinds.len() > 8 || schema.len() != kinds.len() + aggs.len() {
        return Ok(None);
    }
    let mut min = vec![i64::MAX; kinds.len()];
    let mut max = vec![i64::MIN; kinds.len()];
    let mut present = vec![false; kinds.len()];
    let mut scales = vec![0u8; aggs.len()];
    let mut rows = 0usize;
    for input in &inputs {
        let arena = &input.owner.arena;
        if arena.key_kinds != kinds || arena.n_aggs != aggs.len() {
            return Ok(None);
        }
        rows = rows.saturating_add(input.rows().len());
        for &group in input.rows() {
            let group = group as usize;
            if group % 1024 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            for key in 0..kinds.len() {
                if arena.nulls[group] & (1 << key) == 0 {
                    let value = arena.keys[group * kinds.len() + key];
                    min[key] = min[key].min(value);
                    max[key] = max[key].max(value);
                    present[key] = true;
                }
            }
            for slot in 0..aggs.len() {
                let column = arena.decimal_state_columns[slot]
                    .as_ref()
                    .expect("exact column capability checked");
                if column.status[group] != COMPACT_SUM_EMPTY {
                    let Some((_, scale)) = parts(column, group) else {
                        return Ok(None);
                    };
                    scales[slot] = scales[slot].max(scale);
                }
            }
        }
    }
    if rows == 0 || rows > u32::MAX as usize {
        return Ok(None);
    }
    let mut widths = Vec::with_capacity(kinds.len());
    for key in 0..kinds.len() {
        if !present[key] {
            min[key] = 0;
            max[key] = 0;
        }
        let largest = (i128::from(max[key]) - i128::from(min[key])) as u128 + 1;
        widths.push((u128::BITS - largest.leading_zeros()).max(1));
    }
    if widths.iter().sum::<u32>() > 128 {
        return Ok(None);
    }
    let mut shift = 0u32;
    let fields = widths
        .iter()
        .map(|bits| {
            let field = (shift, (1u128 << bits) - 1);
            shift += bits;
            field
        })
        .collect::<Vec<_>>();
    let mut absolute = vec![0u128; aggs.len()];
    for input in &inputs {
        for &group in input.rows() {
            let group = group as usize;
            for slot in 0..aggs.len() {
                let column = input.owner.arena.decimal_state_columns[slot]
                    .as_ref()
                    .unwrap();
                let (value, scale) = parts(column, group).expect("exact partial checked");
                let Some(value) = normalize(value, scale, scales[slot]) else {
                    return Ok(None);
                };
                absolute[slot] = absolute[slot].saturating_add(u128::from(value.unsigned_abs()));
                if absolute[slot] > i64::MAX as u128 {
                    return Ok(None);
                }
            }
        }
    }
    let estimate = rows
        .saturating_mul(96usize.saturating_add(aggs.len().saturating_mul(16)))
        .saturating_add(64 * 1024);
    let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    if !charge.add_within(estimate, usize::MAX) {
        return Ok(None);
    }
    let mut lookup = hashbrown::HashMap::<u128, usize, ahash::RandomState>::with_hasher(
        ahash::RandomState::new(),
    );
    lookup.try_reserve(rows).map_err(|_| {
        crate::engine::executor::mem_budget::allocation_error("columnar aggregate merge map")
    })?;
    let mut keys = crate::engine::executor::mem_budget::try_vec_with_capacity(
        rows,
        "columnar aggregate keys",
    )?;
    let mut totals = (0..aggs.len())
        .map(|_| {
            crate::engine::executor::mem_budget::try_vec_with_capacity::<i64>(
                rows,
                "columnar aggregate totals",
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut actual_scales = (0..aggs.len())
        .map(|_| {
            crate::engine::executor::mem_budget::try_vec_with_capacity::<u8>(
                rows,
                "columnar aggregate scales",
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut seen = crate::engine::executor::mem_budget::try_vec_with_capacity::<u16>(
        rows,
        "columnar aggregate validity",
    )?;
    for input in &inputs {
        let arena = &input.owner.arena;
        for &group in input.rows() {
            let group = group as usize;
            if group % 1024 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            let mut key = 0u128;
            for column in 0..kinds.len() {
                if arena.nulls[group] & (1 << column) == 0 {
                    let code = (i128::from(arena.keys[group * kinds.len() + column])
                        - i128::from(min[column])) as u128
                        + 1;
                    key |= code << fields[column].0;
                }
            }
            let index = if let Some(index) = lookup.get(&key) {
                *index
            } else {
                let index = keys.len();
                lookup.insert(key, index);
                keys.push(key);
                seen.push(0);
                for slot in 0..aggs.len() {
                    totals[slot].push(0);
                    actual_scales[slot].push(0);
                }
                index
            };
            for slot in 0..aggs.len() {
                let column = arena.decimal_state_columns[slot].as_ref().unwrap();
                if column.status[group] == COMPACT_SUM_EMPTY {
                    continue;
                }
                let (value, scale) = parts(column, group).expect("exact partial checked");
                let value = normalize(value, scale, scales[slot]).expect("magnitude bound checked");
                totals[slot][index] = totals[slot][index].checked_add(value).ok_or_else(|| {
                    crate::error::catalog::ER_INTERNAL_ERROR
                        .error("columnar aggregate magnitude bound violated")
                })?;
                actual_scales[slot][index] = actual_scales[slot][index].max(scale);
                seen[index] |= 1 << slot;
            }
        }
    }
    drop(lookup);
    let mut batches = Vec::with_capacity(keys.len().div_ceil(DEFAULT_BATCH_ROWS));
    for start in (0..keys.len()).step_by(DEFAULT_BATCH_ROWS) {
        crate::engine::cancel::check_cancelled()?;
        let end = (start + DEFAULT_BATCH_ROWS).min(keys.len());
        let mut output_charge = charge.sibling();
        let batch_bound = (end - start)
            .saturating_mul(
                kinds
                    .len()
                    .saturating_mul(9)
                    .saturating_add(aggs.len().saturating_mul(40)),
            )
            .saturating_add(4096);
        output_charge.add_force(batch_bound)?;
        let mut columns = Vec::with_capacity(schema.len());
        for column in 0..kinds.len() {
            let mut data = Vec::with_capacity(end - start);
            let mut valid = super::super::bitmap::Bitmap::new_all_valid(end - start);
            let mut any_null = false;
            for (row, key) in keys[start..end].iter().enumerate() {
                let code = (key >> fields[column].0) & fields[column].1;
                if code == 0 {
                    valid.set(row, false);
                    any_null = true;
                    data.push(0);
                } else {
                    data.push(
                        i64::try_from(i128::from(min[column]) + code as i128 - 1)
                            .expect("key range checked"),
                    );
                }
            }
            let nulls = any_null.then_some(valid);
            columns.push(match kinds[column] {
                CompactKeyKind::I64 => ColumnVector::I64 { data, nulls },
                CompactKeyKind::Date32 => ColumnVector::Date32 {
                    data: data.into_iter().map(|value| value as i32).collect(),
                    nulls,
                },
                CompactKeyKind::Time64 => ColumnVector::Time64 { data, nulls },
                CompactKeyKind::DateTime64 => ColumnVector::DateTime64 { data, nulls },
                CompactKeyKind::Timestamp64 => ColumnVector::Timestamp64 { data, nulls },
            });
        }
        for slot in 0..aggs.len() {
            let mixed = (start..end).any(|row| {
                seen[row] & (1 << slot) != 0 && actual_scales[slot][row] != scales[slot]
            });
            if mixed {
                let values = (start..end)
                    .map(|row| {
                        if seen[row] & (1 << slot) == 0 {
                            Value::Null
                        } else {
                            let scale = actual_scales[slot][row];
                            let magnitude = if totals[slot][row] == 0 {
                                0
                            } else {
                                totals[slot][row] / 10i64.pow(u32::from(scales[slot] - scale))
                            };
                            Value::DecimalCompact(magnitude, scale)
                        }
                    })
                    .collect();
                columns.push(ColumnVector::Generic { values });
            } else {
                let mut valid = super::super::bitmap::Bitmap::new_all_valid(end - start);
                let mut any_null = false;
                for row in start..end {
                    if seen[row] & (1 << slot) == 0 {
                        valid.set(row - start, false);
                        any_null = true;
                    }
                }
                columns.push(ColumnVector::Decimal64 {
                    data: totals[slot][start..end].to_vec(),
                    scale: scales[slot],
                    nulls: any_null.then_some(valid),
                });
            }
        }
        let batch = Batch::with_row_count(schema.clone(), columns, end - start)
            .with_memory_charge(output_charge)?;
        batches.push(batch);
    }
    drop(inputs);
    let old = std::mem::take(partials);
    drop(old);
    Ok(Some(batches))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};

    fn fixture(
        large: bool,
        budget: Arc<MemBudget>,
    ) -> (
        Arc<BatchSchema>,
        Vec<AggregateSpec>,
        Vec<HashAggregateCore>,
        Vec<Vec<Option<PartitionPartial>>>,
        Vec<Batch>,
    ) {
        let schema = Arc::new(BatchSchema::new(
            [
                ("k", ColumnType::LongLong),
                ("day", ColumnType::Date),
                ("quantity", ColumnType::LongLong),
                (
                    "price",
                    ColumnType::Decimal {
                        precision: 38,
                        scale: 2,
                    },
                ),
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
        let specs = [2, 3]
            .into_iter()
            .map(|input_column| AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column,
            })
            .collect::<Vec<_>>();
        let root = tempfile::tempdir().unwrap();
        let shared = Arc::new(SharedHashAggSpill::new(
            root.path().to_path_buf(),
            schema.clone(),
            &[0, 1],
            specs.clone(),
        ));
        let mut batches = Vec::new();
        let mut workers = Vec::new();
        let mut output = None;
        for reverse in [false, true] {
            let rows = (0..1024i64)
                .map(|row| if reverse { 1023 - row } else { row })
                .collect::<Vec<_>>();
            let columns = (0..4)
                .map(|column| {
                    ColumnVector::from_values(
                        rows.iter()
                            .map(|row| match column {
                                0 if row % 11 == 0 => Value::Null,
                                0 => Value::Int(row % 17 - 8),
                                1 if row % 13 == 0 => Value::Null,
                                1 => Value::Date((18000 + row % 7) as i32),
                                2 if row % 3 == 0 => Value::Null,
                                2 => Value::Int(row % 101 - 50),
                                3 if row % 5 == 0 => Value::Null,
                                _ if large => Value::Decimal128(i128::MAX / 4, 2),
                                _ => Value::DecimalCompact(row % 107 - 53, 2),
                            })
                            .collect(),
                    )
                })
                .collect();
            let batch = Batch::new(schema.clone(), columns);
            let mut worker = HashAggregateCore::with_budget(
                vec![0, 1],
                specs.clone(),
                schema.clone(),
                budget.clone(),
                root.path(),
            )
            .with_shared_spill(shared.clone());
            worker.consume_batch(&batch).unwrap();
            assert!(!worker.metrics().did_spill);
            output = Some(worker.output_schema());
            worker.request_columnar_sum_handoff();
            workers.push(worker);
            batches.push(batch);
        }
        let partials = workers
            .iter_mut()
            .map(|worker| {
                (0..NUM_PARTITIONS)
                    .map(|partition| worker.take_partition_merged_snapshot(partition).unwrap())
                    .collect()
            })
            .collect();
        (output.unwrap(), specs, workers, partials, batches)
    }
    fn canonical(rows: Vec<Vec<Value>>) -> Vec<Vec<String>> {
        let mut rows = rows
            .into_iter()
            .map(|row| row.iter().map(crate::storage::value_to_string).collect())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    }
    fn reference(batches: &[Batch]) -> Vec<Vec<Value>> {
        let mut groups =
            std::collections::BTreeMap::<(Option<i64>, Option<i32>), Vec<AggState>>::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let k = match batch.columns()[0].value_at(row) {
                    Value::Int(value) => Some(value),
                    _ => None,
                };
                let day = match batch.columns()[1].value_at(row) {
                    Value::Date(value) => Some(value),
                    _ => None,
                };
                let states = groups
                    .entry((k, day))
                    .or_insert_with(|| vec![AggState::default(); 2]);
                for slot in 0..2 {
                    states[slot]
                        .update_ref(AggregateKind::Sum, &batch.columns()[slot + 2].value_at(row))
                        .unwrap();
                }
            }
        }
        groups
            .into_iter()
            .map(|((k, day), states)| {
                vec![
                    k.map(Value::Int).unwrap_or(Value::Null),
                    day.map(Value::Date).unwrap_or(Value::Null),
                    states[0].finalize(AggregateKind::Sum),
                    states[1].finalize(AggregateKind::Sum),
                ]
            })
            .collect()
    }
    #[test]
    fn columnar_sum_handoff_owns_its_reservation_and_matches_nullable_scalar_groups() {
        let budget = Arc::new(MemBudget::new(128 * 1024 * 1024));
        with_query_budget(budget.clone(), || {
            let (schema, specs, workers, mut parts, input) = fixture(false, budget.clone());
            assert!(parts
                .iter()
                .flatten()
                .flatten()
                .all(|partial| partial.buckets.is_empty() && partial.columnar_sums.is_some()));
            drop(workers);
            assert!(
                budget.used() > 0,
                "borrowed partial buffers must retain their producer reservation"
            );
            let batches = try_merge_columnar_sum_batches(&mut parts, &specs, schema)
                .unwrap()
                .expect("bounded exact SUM columns stay columnar");
            assert!(parts.is_empty());
            let actual = batches
                .iter()
                .flat_map(|batch| batch.visible_indices().map(|row| batch.row(row)))
                .collect();
            assert_eq!(canonical(actual), canonical(reference(&input)));
            assert!(
                budget.used() > 0,
                "queued output batches own their memory charge"
            );
            drop(batches);
        });
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn columnar_sum_wide_fallback_preserves_partials_and_generic_merge() {
        let budget = Arc::new(MemBudget::new(128 * 1024 * 1024));
        with_query_budget(budget.clone(), || {
            let (schema, specs, workers, mut parts, input) = fixture(true, budget.clone());
            assert!(try_merge_columnar_sum_batches(&mut parts, &specs, schema)
                .unwrap()
                .is_none());
            assert!(!parts.is_empty());
            restore_legacy_partials(&mut parts);
            assert!(parts
                .iter()
                .flatten()
                .flatten()
                .all(|partial| partial.columnar_sums.is_none()));
            let rows = merge_worker_partition_snapshots(parts, &specs).unwrap();
            assert_eq!(canonical(rows), canonical(reference(&input)));
            drop(workers);
        });
        assert_eq!(budget.used(), 0);
    }
}
