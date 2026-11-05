//! Keep exact aggregate keys dictionary-coded through ordered ROLLUP.

use super::*;
use crate::engine::executor::memory_footprint::AllocatedBytes;

#[derive(Clone, Copy, Debug)]
pub(crate) struct CompactRollupGroup {
    pub(crate) key: u128,
    pub(crate) sum: i64,
    pub(crate) count: u32,
    pub(crate) scale: u8,
}

pub(crate) struct CompactRollupFinest {
    groups: Vec<CompactRollupGroup>,
    dictionaries: Vec<Vec<Value>>,
    fields: Vec<(u32, u128)>,
    scale: u8,
    _charge: crate::engine::executor::mem_budget::ScopedCharge,
}

impl CompactRollupFinest {
    pub(crate) fn len(&self) -> usize {
        self.groups.len()
    }
    pub(crate) fn group(&self, index: usize) -> CompactRollupGroup {
        self.groups[index]
    }
    pub(crate) fn key_value(&self, key: u128, column: usize) -> Result<Value, QueryError> {
        let Some(&(shift, mask)) = self.fields.get(column) else {
            return Ok(Value::Null);
        };
        crate::engine::executor::mem_budget::try_clone_value(
            &self.dictionaries[column][((key >> shift) & mask) as usize],
            "retained compact dictionary value",
        )
    }
    pub(crate) fn sum_value(&self, group: CompactRollupGroup) -> Value {
        if group.count == 0 {
            Value::Null
        } else {
            let magnitude = if group.sum == 0 {
                0
            } else {
                let factor = 10i64
                    .checked_pow(u32::from(self.scale - group.scale))
                    .expect("compact decimal rescale was bounded");
                debug_assert_eq!(group.sum % factor, 0);
                group.sum / factor
            };
            Value::DecimalCompact(magnitude, group.scale)
        }
    }
    pub(crate) fn common_prefix(&self, left: u128, right: u128) -> usize {
        self.fields
            .iter()
            .take_while(|(shift, mask)| ((left ^ right) >> *shift) & *mask == 0)
            .count()
    }
    pub(crate) fn prefix_key(&self, key: u128, prefix: usize) -> u128 {
        if prefix == 0 {
            return 0;
        }
        let shift = self.fields[prefix - 1].0;
        (key >> shift) << shift
    }
    pub(crate) fn merge(
        target: &mut CompactRollupGroup,
        source: CompactRollupGroup,
    ) -> Result<(), QueryError> {
        // Admission proves that the sum of all absolute partial magnitudes
        // and all non-NULL counts fit these lanes. Every subtotal is a subset.
        if source.count != 0 {
            target.scale = if target.count == 0 {
                source.scale
            } else {
                target.scale.max(source.scale)
            };
        }
        target.sum = target.sum.checked_add(source.sum).ok_or_else(|| {
            crate::error::catalog::ER_INTERNAL_ERROR
                .error("compact aggregate magnitude bound was violated")
        })?;
        target.count = target.count.checked_add(source.count).ok_or_else(|| {
            crate::error::catalog::ER_INTERNAL_ERROR
                .error("compact aggregate count bound was violated")
        })?;
        Ok(())
    }
}

fn domain_len(source: &DictionaryColumnSource) -> usize {
    match source {
        DictionaryColumnSource::Utf8 { canonical_keys, .. } => canonical_keys.len(),
        DictionaryColumnSource::I64 { raw_values, .. } => raw_values.len(),
        DictionaryColumnSource::U64 { raw_values, .. } => raw_values.len(),
        DictionaryColumnSource::Date32 { raw_values, .. } => raw_values.len(),
        DictionaryColumnSource::Bool { .. } => 2,
    }
}

fn domain_value(source: &DictionaryColumnSource, token: usize) -> Result<Value, QueryError> {
    Ok(match source {
        DictionaryColumnSource::Utf8 { raw_values, .. } => {
            let text = &raw_values[token - 1];
            if text.len() <= crate::types::SMALL_STRING_INLINE_CAP {
                Value::SmallString(crate::types::SmallStringBuf::from_slice(text.as_bytes()))
            } else {
                Value::String(crate::engine::executor::mem_budget::try_copy_string(
                    text,
                    "compact dictionary representative",
                )?)
            }
        }
        DictionaryColumnSource::I64 { raw_values, .. } => Value::Int(raw_values[token - 1]),
        DictionaryColumnSource::U64 { raw_values, .. } => Value::UInt(raw_values[token - 1]),
        DictionaryColumnSource::Date32 { raw_values, .. } => Value::Date(raw_values[token - 1]),
        DictionaryColumnSource::Bool { .. } => Value::Int(i64::from(token == 2)),
    })
}

fn domain_key(source: &DictionaryColumnSource, token: usize) -> Result<Arc<[u8]>, QueryError> {
    if let DictionaryColumnSource::Utf8 { canonical_keys, .. } = source {
        return Ok(canonical_keys[token - 1].clone());
    }
    let mut bytes =
        crate::engine::executor::mem_budget::try_vec_with_capacity(16, "compact primitive key")?;
    super::super::key_encode::encode(&domain_value(source, token)?, &mut bytes);
    Ok(bytes.into())
}

fn partial_parts(sum: &NarrowDecimalStateColumn, group: usize) -> Option<(i128, u8)> {
    match sum.status[group] {
        COMPACT_SUM_EMPTY => Some((0, sum.scale)),
        COMPACT_SUM_NARROW => Some((i128::from(sum.magnitudes[group]), sum.scale)),
        COMPACT_SUM_PROMOTED => {
            crate::engine::executor::select::aggregate::numeric_state_compact_parts(
                sum.promoted.get(&(group as u32))?,
            )
        }
        _ => None,
    }
}

fn normalize(magnitude: i128, from: u8, to: u8) -> Option<i64> {
    if magnitude == 0 {
        return Some(0);
    }
    let factor = 10i128.checked_pow(u32::from(to.checked_sub(from)?))?;
    i64::try_from(magnitude.checked_mul(factor)?).ok()
}

#[track_caller]
fn declined(reason: &'static str) -> Result<Option<CompactRollupFinest>, QueryError> {
    if crate::util::env::var_os("MYDB_DEBUG_COMPACT_ROLLUP").is_some() {
        eprintln!(
            "compact ROLLUP fallback at {}: {reason}",
            std::panic::Location::caller().line()
        );
    }
    Ok(None)
}

/// Decline before consuming any partial when exact bounded lanes, a common
/// comparison domain, packed key width, or optional memory admission fail.
/// Generic and spilled partials continue through the established row merger.
pub(crate) fn try_compact_rollup(
    partials: &mut Vec<Vec<Option<PartitionPartial>>>,
    collations: &[Option<u16>],
    aggs: &[AggregateSpec],
) -> Result<Option<CompactRollupFinest>, QueryError> {
    if !matches!(
        aggs,
        [AggregateSpec {
            func: AggregateKind::Sum,
            ..
        }]
    ) || collations.is_empty()
        || collations.len() > 8
    {
        return declined("capability mismatch");
    }
    let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    if !charge.add_within(16 * 1024, usize::MAX) {
        return declined("capability mismatch");
    }
    let mut encoders: smallvec::SmallVec<[Arc<DictionaryGroupEncoder>; 16]> =
        smallvec::SmallVec::new();
    let mut rows = 0usize;
    let mut absolute_sum = 0u128;
    let mut count = 0u128;
    let mut scale: Option<u8> = None;
    let mut domains = vec![0usize; collations.len()];
    let mut dictionary_payload = 0usize;
    for partial in partials.iter().flatten().flatten() {
        crate::engine::cancel::check_cancelled()?;
        let Some(part) = partial.dictionary_count.as_ref() else {
            return declined("capability mismatch");
        };
        if part.n_aggs != 1 || part.encoder.columns.len() != collations.len() {
            return declined("capability mismatch");
        }
        let Some(sum) = part.partition.decimal_sum.as_ref() else {
            return declined("capability mismatch");
        };
        rows = rows.saturating_add(part.partition.keys.len());
        for group in 0..part.partition.keys.len() {
            if group % 4096 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            let non_null = part.partition.count(group, 0, 1);
            if non_null == 0 {
                if sum.status[group] != COMPACT_SUM_EMPTY {
                    return declined("nonempty state without inputs");
                }
            } else {
                let Some((_, actual_scale)) = partial_parts(sum, group) else {
                    return declined("wide or approximate subtotal");
                };
                scale = Some(scale.map_or(actual_scale, |prior| prior.max(actual_scale)));
            }
            count = count.saturating_add(u128::from(non_null));
        }
        if count > u32::MAX as u128 {
            return declined("non-null count bound");
        }
        if encoders
            .iter()
            .any(|encoder| Arc::ptr_eq(encoder, &part.encoder))
        {
            continue;
        }
        for (column, entry) in part.encoder.columns.iter().enumerate() {
            if entry.direct {
                return declined("capability mismatch");
            }
            if let Some(first) = encoders.first() {
                if std::mem::discriminant(&entry.source)
                    != std::mem::discriminant(&first.columns[column].source)
                {
                    return declined("capability mismatch");
                }
            }
            domains[column] = domains[column].saturating_add(domain_len(&entry.source));
            if let DictionaryColumnSource::Utf8 { raw_values, .. } = &entry.source {
                dictionary_payload = dictionary_payload
                    .saturating_add(raw_values.iter().map(|value| value.len()).sum::<usize>());
            }
        }
        encoders.push(part.encoder.clone());
    }
    if rows == 0 || rows > u32::MAX as usize {
        return declined("capability mismatch");
    }
    let common_scale = scale.unwrap_or(0);
    for part in partials.iter().flatten().flatten() {
        let part = part
            .dictionary_count
            .as_ref()
            .expect("numeric partial checked");
        let sum = part
            .partition
            .decimal_sum
            .as_ref()
            .expect("numeric state checked");
        for group in 0..part.partition.keys.len() {
            if group % 4096 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            let (magnitude, actual_scale) =
                partial_parts(sum, group).expect("exact subtotal checked");
            let Some(normalized) = normalize(magnitude, actual_scale, common_scale) else {
                return declined("subtotal rescale bound");
            };
            absolute_sum = absolute_sum.saturating_add(u128::from(normalized.unsigned_abs()));
            if absolute_sum > i64::MAX as u128 {
                return declined("absolute magnitude bound");
            }
        }
    }
    // The row buffer, dictionary entries/maps and translated token arrays
    // share this reservation. Encoded-sort scratch is admitted separately
    // after dictionary deduplication, not as a multiple of all worker domains.
    let domain_overhead = (std::mem::size_of::<Arc<[u8]>>() + std::mem::size_of::<Value>())
        .saturating_mul(3)
        .saturating_add(64);
    let estimate = rows
        .saturating_mul(96)
        .saturating_add(
            domains
                .iter()
                .sum::<usize>()
                .saturating_mul(domain_overhead),
        )
        .saturating_add(dictionary_payload.saturating_mul(3))
        .saturating_add(64 * 1024);
    if crate::util::env::var_os("MYDB_DEBUG_COMPACT_ROLLUP").is_some() {
        if let Some(budget) = crate::engine::executor::mem_budget::current_budget() {
            eprintln!(
                "compact ROLLUP admission: records={rows}, estimate={estimate}, used={}, limit={}",
                budget.used(),
                budget.limit()
            );
        }
    }
    if crate::engine::executor::mem_budget::current_budget()
        .is_some_and(|budget| budget.used().saturating_add(estimate) > budget.limit())
    {
        return declined("optional work-area headroom");
    }
    if !charge.add_within(estimate, usize::MAX) {
        return declined("optional memory admission");
    }

    struct Entry {
        key: Arc<[u8]>,
        value: Value,
    }
    let mut registries = Vec::with_capacity(collations.len());
    let mut dictionaries = Vec::with_capacity(collations.len());
    let mut widths = Vec::with_capacity(collations.len());
    for (column, collation) in collations.iter().enumerate() {
        crate::engine::cancel::check_cancelled()?;
        let mut registry = hashbrown::HashMap::<Arc<[u8]>, u32, ahash::RandomState>::with_hasher(
            ahash::RandomState::new(),
        );
        registry.try_reserve(domains[column]).map_err(|_| {
            crate::engine::executor::mem_budget::allocation_error("compact aggregate dictionary")
        })?;
        let mut entries: Vec<Entry> = crate::engine::executor::mem_budget::try_vec_with_capacity(
            domains[column],
            "compact aggregate dictionary values",
        )?;
        for encoder in &encoders {
            let source = &encoder.columns[column].source;
            for token in 1..=domain_len(source) {
                if token % 1024 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                let key = domain_key(source, token)?;
                if let Some(&index) = registry.get(key.as_ref()) {
                    if let DictionaryColumnSource::Utf8 { raw_values, .. } = source {
                        let raw = raw_values[token - 1].as_bytes();
                        let representative = match &entries[index as usize].value {
                            Value::String(value) => value.as_bytes(),
                            Value::SmallString(value) => value.as_slice(),
                            _ => unreachable!("text dictionary representative"),
                        };
                        if raw != representative {
                            return declined("ambiguous text representatives");
                        }
                    }
                    continue;
                }
                registry.insert(key.clone(), entries.len() as u32);
                entries.push(Entry {
                    key,
                    value: domain_value(source, token)?,
                });
            }
        }
        let field = crate::engine::executor::sort::SortField {
            ascending: true,
            nulls_first: true,
            collation_id: *collation,
        };
        // Group and ordering keys use the same bound collation. Allow for
        // framing/escaping, geometric capacities and permutation metadata
        // using the distinct encoded payload, rather than repeated domains
        // from every worker. A refusal leaves the original partials intact.
        let sort_scratch = entries.len().saturating_mul(512).saturating_add(
            entries
                .iter()
                .map(|entry| entry.key.len())
                .sum::<usize>()
                .saturating_mul(16),
        );
        if crate::engine::executor::mem_budget::current_budget()
            .is_some_and(|budget| budget.used().saturating_add(sort_scratch) > budget.limit())
        {
            return declined("dictionary sort headroom");
        }
        if !crate::engine::executor::sort::key_encode::try_sort_by_borrowed_encoded_keys(
            &mut entries,
            &[field],
            |entry| std::slice::from_ref(&entry.value),
        )? {
            entries.sort_unstable_by(|left, right| {
                crate::engine::executor::sort::compare_value_for_sort(
                    &left.value,
                    &right.value,
                    field,
                )
            });
        }
        let mut values = crate::engine::executor::mem_budget::try_vec_with_capacity(
            entries.len() + 1,
            "compact aggregate ordered dictionary",
        )?;
        values.push(Value::Null);
        for entry in entries {
            let token = u32::try_from(values.len()).map_err(|_| {
                crate::engine::executor::mem_budget::allocation_error(
                    "compact dictionary token range",
                )
            })?;
            *registry
                .get_mut(entry.key.as_ref())
                .expect("dictionary key registered") = token;
            values.push(entry.value);
        }
        widths.push((u32::BITS - ((values.len() - 1) as u32).leading_zeros()).max(1));
        dictionaries.push(values);
        registries.push(registry);
    }
    let bits = widths.iter().sum::<u32>();
    if bits > 128 {
        return declined("capability mismatch");
    }
    let mut shift = bits;
    let fields = widths
        .iter()
        .map(|width| {
            shift -= width;
            (shift, (1u128 << width) - 1)
        })
        .collect::<Vec<_>>();
    let mut mappings = Vec::with_capacity(encoders.len());
    for encoder in &encoders {
        let mut columns = Vec::with_capacity(collations.len());
        for (column, entry) in encoder.columns.iter().enumerate() {
            let source = &entry.source;
            let mut tokens = crate::engine::executor::mem_budget::try_vec_with_capacity(
                domain_len(source) + 1,
                "compact dictionary translation",
            )?;
            tokens.push(0);
            for token in 1..=domain_len(source) {
                tokens.push(registries[column][domain_key(source, token)?.as_ref()]);
            }
            columns.push(tokens);
        }
        mappings.push(columns);
    }
    let mut groups = crate::engine::executor::mem_budget::try_vec_with_capacity(
        rows,
        "compact ordered aggregate groups",
    )?;
    for partial in partials.iter().flatten().flatten() {
        let part = partial
            .dictionary_count
            .as_ref()
            .expect("compact partial capability checked");
        let encoder_index = encoders
            .iter()
            .position(|encoder| Arc::ptr_eq(encoder, &part.encoder))
            .expect("compact dictionary registered");
        let sum = part
            .partition
            .decimal_sum
            .as_ref()
            .expect("compact numeric state checked");
        for group in 0..part.partition.keys.len() {
            if group % 4096 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            let local = part.partition.keys[group];
            let mut key = 0u128;
            for (column, entry) in part.encoder.columns.iter().enumerate() {
                let token = ((local >> entry.shift) & u128::from(entry.token_mask)) as usize;
                let rank = mappings[encoder_index][column][token];
                key |= u128::from(rank) << fields[column].0;
            }
            let (magnitude, actual_scale) =
                partial_parts(sum, group).expect("exact subtotal checked");
            let non_null = part.partition.count(group, 0, 1) as u32;
            groups.push(CompactRollupGroup {
                key,
                sum: normalize(magnitude, actual_scale, common_scale)
                    .expect("normalized magnitude bound checked"),
                count: non_null,
                scale: if non_null == 0 {
                    common_scale
                } else {
                    actual_scale
                },
            });
        }
    }
    crate::engine::cancel::check_cancelled()?;
    groups.sort_unstable_by_key(|group| group.key);
    let mut retained = 0usize;
    for position in 0..groups.len() {
        if position % 4096 == 0 {
            crate::engine::cancel::check_cancelled()?;
        }
        let group = groups[position];
        if retained != 0 && groups[retained - 1].key == group.key {
            CompactRollupFinest::merge(&mut groups[retained - 1], group)?;
        } else {
            groups[retained] = group;
            retained += 1;
        }
    }
    groups.truncate(retained);
    drop(registries);
    drop(mappings);
    drop(encoders);
    let old = std::mem::take(partials);
    drop(old);
    let actual = groups
        .capacity()
        .saturating_mul(std::mem::size_of::<CompactRollupGroup>())
        .saturating_add(dictionaries.allocated_bytes())
        .saturating_add(
            fields
                .capacity()
                .saturating_mul(std::mem::size_of::<(u32, u128)>()),
        )
        .saturating_add(256);
    if actual > charge.charged() {
        return Err(crate::engine::executor::mem_budget::allocation_error(
            "compact aggregate reservation bound",
        ));
    }
    charge.release_bytes(charge.charged() - actual);
    if crate::util::env::var_os("MYDB_DEBUG_COMPACT_ROLLUP").is_some() {
        eprintln!("compact ROLLUP accepted: groups={retained}, records={rows}, bits={bits}");
    }
    Ok(Some(CompactRollupFinest {
        groups,
        dictionaries,
        fields,
        scale: common_scale,
        _charge: charge,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};

    fn batches(
        scale: u8,
        labels: &[&str],
        large: bool,
    ) -> (Arc<BatchSchema>, Vec<Batch>, Vec<AggregateSpec>) {
        let schema = Arc::new(BatchSchema::new(
            [
                ("label", ColumnType::VarString),
                ("key", ColumnType::LongLong),
                ("day", ColumnType::Date),
                (
                    "value",
                    ColumnType::Decimal {
                        precision: 30,
                        scale: 2,
                    },
                ),
            ]
            .into_iter()
            .enumerate()
            .map(|(column, (name, column_type))| BatchField {
                table: "t".into(),
                alias: None,
                name: name.into(),
                column_type,
                source: crate::types::ColumnSource {
                    collation_id: (column == 0).then_some(45),
                    ..Default::default()
                },
            })
            .collect(),
        ));
        let batches = (0..2)
            .map(|worker| {
                let positions = if large {
                    vec![0]
                } else {
                    (0..4096i64)
                        .map(|row| if worker == 0 { row } else { 4095 - row })
                        .collect()
                };
                let values = |make: &dyn Fn(i64) -> Value| {
                    ColumnVector::from_values(positions.iter().copied().map(make).collect())
                };
                Batch::new(
                    schema.clone(),
                    vec![
                        values(&|row| {
                            if !large && row % 29 == 0 {
                                Value::Null
                            } else {
                                Value::String(labels[(row as usize + worker) % labels.len()].into())
                            }
                        }),
                        values(&|row| {
                            if !large && row % 31 == 0 {
                                Value::Null
                            } else {
                                Value::Int(row % 61 - 30)
                            }
                        }),
                        values(&|row| Value::Date((18000 + row % 7) as i32)),
                        values(&|row| {
                            if !large && row % 19 == 0 {
                                Value::Null
                            } else {
                                Value::DecimalCompact(
                                    if large { i64::MAX } else { row % 101 - 50 },
                                    if worker == 0 { 2 } else { scale },
                                )
                            }
                        }),
                    ],
                )
            })
            .collect();
        (
            schema,
            batches,
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 3,
            }],
        )
    }

    fn partials(
        schema: Arc<BatchSchema>,
        batches: &[Batch],
        specs: &[AggregateSpec],
        budget: Arc<MemBudget>,
    ) -> (Vec<HashAggregateCore>, Vec<Vec<Option<PartitionPartial>>>) {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap().keep();
        let shared = Arc::new(SharedHashAggSpill::new(
            root.clone(),
            schema.clone(),
            &[0, 1, 2],
            specs.to_vec(),
        ));
        let mut workers = Vec::new();
        for batch in batches {
            let mut worker = HashAggregateCore::with_budget(
                vec![0, 1, 2],
                specs.to_vec(),
                schema.clone(),
                budget.clone(),
                &root,
            )
            .with_shared_spill(shared.clone());
            worker.consume_batch(batch).unwrap();
            assert!(worker.dictionary_count.is_some());
            assert!(!worker.metrics().did_spill);
            workers.push(worker);
        }
        let partials = workers
            .iter_mut()
            .map(|worker| {
                (0..NUM_PARTITIONS)
                    .map(|partition| worker.take_partition_merged_snapshot(partition).unwrap())
                    .collect()
            })
            .collect();
        (workers, partials)
    }

    fn canonical(rows: Vec<Vec<Value>>) -> Vec<Vec<String>> {
        let mut rows = rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .enumerate()
                    .map(|(column, value)| {
                        if column == 0 {
                            format!(
                                "{:?}",
                                crate::engine::expr::ValueKey::from_with_collation(value, Some(45))
                            )
                        } else {
                            crate::storage::value_to_string(value)
                        }
                    })
                    .collect()
            })
            .collect::<Vec<_>>();
        rows.sort();
        rows
    }

    #[test]
    fn packed_rollup_translates_worker_tokens_and_matches_nullable_exact_groups() {
        for mode in 0..4 {
            let (schema, mut batches, specs) = batches(
                if mode == 1 { 3 } else { 2 },
                &["pear", "apple", "雪"],
                false,
            );
            if mode == 2 {
                for batch in &mut batches {
                    let mut columns = batch.columns().to_vec();
                    columns[3] = ColumnVector::Generic {
                        values: (0..batch.num_rows())
                            .map(|row| {
                                let value = columns[3].value_at(row);
                                if matches!(value, Value::Null) {
                                    Value::Int(0)
                                } else {
                                    value
                                }
                            })
                            .collect(),
                    };
                    *batch = Batch::new(schema.clone(), columns);
                }
            }
            let budget = Arc::new(MemBudget::new(if mode == 3 {
                1024 * 1024
            } else {
                128 * 1024 * 1024
            }));
            with_query_budget(budget.clone(), || {
                let (workers, mut parts) = partials(schema, &batches, &specs, budget.clone());
                let mut oracle = HashAggregateCore::new(vec![0, 1, 2], specs.clone());
                for batch in &batches {
                    oracle.consume_batch(batch).unwrap();
                }
                let native = try_compact_rollup(&mut parts, &[Some(45), None, None], &specs)
                    .unwrap()
                    .expect("bounded exact states stay encoded through ordering");
                assert!(parts.is_empty());
                let mut rows = Vec::new();
                for index in 0..native.len() {
                    let group = native.group(index);
                    if index != 0 {
                        assert!(native.group(index - 1).key < group.key);
                    }
                    let mut row = (0..3)
                        .map(|column| native.key_value(group.key, column).unwrap())
                        .collect::<Vec<_>>();
                    row.push(native.sum_value(group));
                    rows.push(row);
                }
                assert_eq!(canonical(rows), canonical(oracle.finalize().unwrap()));
                drop(native);
                drop(workers);
            });
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn packed_rollup_preserves_unsigned_extremes_and_nullable_boolean_keys() {
        use crate::engine::executor::vec::bitmap::Bitmap;
        for boolean in [false, true] {
            let (schema, mut batches, specs) = batches(2, &["pear", "apple", "雪"], false);
            let mut fields = schema.fields().to_vec();
            if !boolean {
                fields[1].source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
            }
            let schema = Arc::new(BatchSchema::new(fields));
            for batch in &mut batches {
                let mut columns = batch.columns().to_vec();
                let original = &batch.columns()[1];
                columns[1] = if boolean {
                    let mut values = Bitmap::new_all_invalid(batch.num_rows());
                    for row in 0..batch.num_rows() {
                        values.set(row, row % 2 == 0);
                    }
                    ColumnVector::Bool {
                        values,
                        nulls: original.nulls().cloned(),
                    }
                } else {
                    ColumnVector::from_values(
                        (0..batch.num_rows())
                            .map(|row| match original.value_at(row) {
                                Value::Int(value) => Value::UInt(value as u64),
                                value => value,
                            })
                            .collect(),
                    )
                };
                *batch = Batch::new(schema.clone(), columns);
            }
            let budget = Arc::new(MemBudget::new(128 * 1024 * 1024));
            with_query_budget(budget.clone(), || {
                let (workers, mut parts) = partials(schema, &batches, &specs, budget.clone());
                let mut oracle = HashAggregateCore::new(vec![0, 1, 2], specs.clone());
                for batch in &batches {
                    oracle.consume_batch(batch).unwrap();
                }
                let native = try_compact_rollup(&mut parts, &[Some(45), None, None], &specs)
                    .unwrap()
                    .unwrap();
                let rows = (0..native.len())
                    .map(|index| {
                        let group = native.group(index);
                        let mut row = (0..3)
                            .map(|column| native.key_value(group.key, column).unwrap())
                            .collect::<Vec<_>>();
                        row.push(native.sum_value(group));
                        row
                    })
                    .collect();
                assert_eq!(canonical(rows), canonical(oracle.finalize().unwrap()));
                drop(native);
                drop(workers);
            });
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn packed_rollup_declines_without_consuming_unbounded_or_ambiguous_states() {
        for (scale, labels, large, tiny_budget) in [
            (2, vec!["A", "a"], false, false),
            (2, vec!["same"], true, false),
            (2, vec!["same"], false, true),
        ] {
            let (schema, batches, specs) = batches(scale, &labels, large);
            let original_budget = Arc::new(MemBudget::new(128 * 1024 * 1024));
            let (workers, mut parts) = partials(schema, &batches, &specs, original_budget.clone());
            let budget = if tiny_budget {
                Arc::new(MemBudget::new(16 * 1024))
            } else {
                original_budget.clone()
            };
            with_query_budget(budget.clone(), || {
                assert!(
                    try_compact_rollup(&mut parts, &[Some(45), None, None], &specs)
                        .unwrap()
                        .is_none()
                );
            });
            assert!(!parts.is_empty());
            assert!(budget.terminal_oom().is_none());
            let actual = merge_worker_partition_snapshots(parts, &specs).unwrap();
            let mut oracle = HashAggregateCore::new(vec![0, 1, 2], specs.clone());
            for batch in &batches {
                oracle.consume_batch(batch).unwrap();
            }
            assert_eq!(canonical(actual), canonical(oracle.finalize().unwrap()));
            drop(workers);
            assert_eq!(original_budget.used(), 0);
            if tiny_budget {
                assert_eq!(budget.used(), 0);
            }
        }
    }
}
