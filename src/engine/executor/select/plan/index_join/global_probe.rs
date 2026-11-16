//! Budget-admitted, domain-restricted scans for fused existence probes.
//! A dense set of preserved keys should scan the heap once rather than
//! discover millions of RIDs and repeatedly fetch scattered physical pages.
use super::*;
use crate::engine::executor::mem_budget::{self, ScopedCharge};
use crate::engine::executor::select::bloom::ExactIntKeySetBuilder;
use crate::engine::executor::vec::scan_bridge::{PredicateValue, PredicateValueSink};
use crate::engine::executor::vec::Batch;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8};

#[derive(Default)]
struct Summary {
    first: AtomicI64,
    // 0 = empty, 1 = first writer, 2 = one distinct value, 3 = mixed.
    state: AtomicU8,
}

impl Summary {
    fn update(&self, value: i64) {
        let mut state = self.state.load(Ordering::Acquire);
        if state == 0 {
            match self
                .state
                .compare_exchange(0, 1, Ordering::Acquire, Ordering::Acquire)
            {
                Ok(_) => {
                    self.first.store(value, Ordering::Relaxed);
                    self.state.store(2, Ordering::Release);
                    return;
                }
                Err(now) => state = now,
            }
        }
        while state == 1 {
            std::hint::spin_loop();
            state = self.state.load(Ordering::Acquire);
        }
        if state == 2 && self.first.load(Ordering::Relaxed) != value {
            self.state.store(3, Ordering::Release);
        }
    }

    fn matches(&self, value: &Value) -> bool {
        let state = self.state.load(Ordering::Acquire);
        matches!(value, Value::Int(value)
            if state == 3 || (state == 2 && self.first.load(Ordering::Relaxed) != *value))
    }
}

pub(super) fn select(
    db: &Arc<Database>,
    txn: TxnView,
    spec: &FusedNeqBatchSpec,
    batches: &[Batch],
) -> Result<Option<Vec<Vec<u32>>>, QueryError> {
    let rows = batches.iter().map(Batch::visible_rows).sum::<usize>();
    if rows > u32::MAX as usize {
        return Ok(None);
    }
    let buckets = rows
        .saturating_mul(8)
        .div_ceil(7)
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX);
    let bytes = buckets
        .saturating_mul(17)
        .saturating_add(rows.saturating_mul(std::mem::size_of::<[Summary; 2]>() + 12));
    let cap = mem_budget::current_budget().map_or(0, |budget| budget.remaining());
    let mut charge = ScopedCharge::new(0);
    if !charge.add_within(bytes, cap) {
        return Ok(None);
    }
    let area = crate::engine::executor::work_area::WorkAreaHandle::register();
    if matches!(
        area.try_charge(bytes),
        crate::engine::executor::work_area::SpillHint::Spill
    ) {
        return Ok(None);
    }
    let key_column = spec.key_columns[0];
    let mut keys =
        hashbrown::HashMap::<i64, u32, ahash::RandomState>::with_hasher(ahash::RandomState::new());
    keys.try_reserve(rows)
        .map_err(|_| mem_budget::allocation_error("global existence key domain"))?;
    let mut row_groups = Vec::with_capacity(rows);
    let mut builder = ExactIntKeySetBuilder::with_capacity(rows)?;
    for batch in batches {
        crate::engine::cancel::check_cancelled()?;
        for row in batch.visible_indices() {
            for predicate in [&spec.inner, &spec.outer] {
                if !matches!(
                    batch.columns()[predicate.left_idx].value_at(row),
                    Value::Int(_) | Value::Null
                ) {
                    return Ok(None);
                }
            }
            let group = match batch.columns()[key_column].value_at(row) {
                Value::Int(key) if !spec.lookup.forced_empty => {
                    let next = keys.len() as u32;
                    *keys.entry(key).or_insert_with(|| {
                        builder.insert(i128::from(key));
                        next
                    })
                }
                Value::Null | Value::Int(_) => u32::MAX,
                _ => return Ok(None),
            };
            row_groups.push(group);
        }
    }
    // Sparse key populations keep indexed short-circuit probes. Requiring a
    // substantial predicted page footprint pays for reading rejected rows too.
    let pages = spec.table.schema.stats.heap_page_count;
    if !keys.is_empty() && (keys.len() as u64).saturating_mul(spec.fanout as u64) < pages / 2 {
        return Ok(None);
    }
    let domain = builder.finish_for_probe_rows(spec.table.schema.stats.row_count)?;
    let summaries = (0..keys.len())
        .map(|_| [Summary::default(), Summary::default()])
        .collect::<Vec<_>>();
    let right_key = spec.table.schema.column_index(&spec.index.columns[0])?;
    let key_position = spec
        .projection
        .iter()
        .position(|&column| column == right_key)
        .ok_or_else(|| mem_budget::allocation_error("missing existence scan key"))?;
    let heap_pages = if keys.is_empty() {
        0
    } else {
        db.heap_page_count_for_storage(spec.table.schema.id)?
    };
    let degree = default_inlj_outer_parallel_degree()
        .clamp(1, 8)
        .min(heap_pages.div_ceil(4096).max(1) as usize);
    let next_page = AtomicUsize::new(0);
    let unsupported = AtomicBool::new(false);
    let context = super::super::super::execution_context::ExecutionContext::capture(txn);
    let summaries = &summaries;
    let keys = &keys;
    let domain = &domain;
    crate::util::thread_spawn::scope(|scope| -> Result<(), QueryError> {
        let mut handles = Vec::with_capacity(degree);
        for _ in 0..degree {
            let next_page = &next_page;
            let unsupported = &unsupported;
            let context = context.clone();
            handles.push(scope.spawn(move || context.run(db, || {
                let mut scratch = crate::storage::DirectScanScratch::default();
                let mut predicate_values = Vec::new();
                let mut typed_key = [PredicateValue::Null];
                let mut output_values = Vec::new();
                loop {
                    crate::engine::cancel::check_cancelled()?;
                    let lo = next_page.fetch_add(4096, Ordering::Relaxed) as u64;
                    if lo >= heap_pages { break; }
                    let mut predicate = |hit: crate::storage::PayloadStreamHit<'_>| {
                        if let crate::storage::PayloadStreamHit::Payload(payload) = &hit {
                            if crate::storage::decode_row_into_sink_fast(
                                payload.payload, payload.codec, payload.layout_id, payload.plan,
                                &mut PredicateValueSink::new(&mut typed_key), payload.has_external,
                            )? {
                                match typed_key[0] {
                                    PredicateValue::I64(key) => return Ok(domain.contains(i128::from(key))),
                                    PredicateValue::Null => return Ok(false),
                                    _ => {}
                                }
                            }
                        }
                        let values = decode(hit, &mut predicate_values)?;
                        if !matches!(values.first(), Some(Value::Int(_) | Value::Null)) {
                            unsupported.store(true, Ordering::Relaxed);
                        }
                        Ok(matches!(values.first(), Some(Value::Int(key)) if domain.contains(i128::from(*key))))
                    };
                    let mut visit = |hit: crate::storage::PayloadStreamHit<'_>| {
                        let values = decode(hit, &mut output_values)?;
                        let Value::Int(key) = values[key_position] else { return Ok(()); };
                        let Some(&group) = keys.get(&key) else { return Ok(()); };
                        for (predicate, summary) in [&spec.inner, &spec.outer].into_iter().zip(&summaries[group as usize]) {
                            if predicate.right_only.as_ref().map(|p| p.eval(values)).transpose()?.unwrap_or(true) {
                                if let Value::Int(value) = values[predicate.right_idx] {
                                    summary.update(value);
                                } else if !matches!(values[predicate.right_idx], Value::Null) {
                                    unsupported.store(true, Ordering::Relaxed);
                                }
                            }
                        }
                        Ok(())
                    };
                    db.scan_rows_payloads_predicate_first_storage_streaming_range(
                        &spec.table.schema, spec.table.schema.id, txn,
                        &[right_key], &spec.projection, &spec.projection,
                        lo, (lo + 4096).min(heap_pages), &mut scratch, &mut predicate, &mut visit,
                    )?;
                }
                Ok::<_, QueryError>(())
            })));
        }
        for handle in handles {
            handle.join().map_err(|_| {
                crate::error::catalog::ER_INTERNAL_ERROR.error("existence scan worker panicked")
            })??;
        }
        Ok(())
    })?;
    if unsupported.load(Ordering::Relaxed) {
        return Ok(None);
    }
    let mut ordinal = 0;
    let mut selections = Vec::with_capacity(batches.len());
    for batch in batches {
        let mut selected = Vec::new();
        for row in batch.visible_indices() {
            let group = row_groups[ordinal];
            ordinal += 1;
            let matches = |side: usize, predicate: &FusedSemiAntiNeqSpec| {
                summaries.get(group as usize).is_some_and(|summary| {
                    summary[side].matches(&batch.columns()[predicate.left_idx].value_at(row))
                })
            };
            if matches(0, &spec.inner) == spec.inner_is_semi
                && matches(1, &spec.outer) == spec.outer_is_semi
            {
                selected.push(row as u32);
            }
        }
        selections.push(selected);
    }
    record_inlj_fusion_count();
    Ok(Some(selections))
}

fn decode<'a>(
    hit: crate::storage::PayloadStreamHit<'a>,
    scratch: &'a mut Vec<Value>,
) -> Result<&'a [Value], QueryError> {
    match hit {
        crate::storage::PayloadStreamHit::Values(values) => Ok(values),
        crate::storage::PayloadStreamHit::Payload(payload) => {
            scratch.clear();
            crate::storage::decode_row_schema_columns_into_with_plan_ext_for_layout(
                payload.payload,
                payload.codec,
                payload.layout_id,
                payload.plan,
                scratch,
                payload.has_external,
            )?;
            Ok(scratch)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn concurrent_summaries_preserve_extreme_values_and_null_probes() {
        for values in [
            vec![i64::MIN; 128],
            vec![i64::MAX; 128],
            (0..128)
                .map(|i| if i % 2 == 0 { i64::MIN } else { i64::MAX })
                .collect(),
        ] {
            let summary = Summary::default();
            std::thread::scope(|scope| {
                for part in values.chunks(16) {
                    let summary = &summary;
                    scope.spawn(move || {
                        for &value in part {
                            summary.update(value);
                        }
                    });
                }
            });
            for probe in [
                Value::Null,
                Value::Int(i64::MIN),
                Value::Int(0),
                Value::Int(i64::MAX),
            ] {
                assert_eq!(
                    summary.matches(&probe),
                    matches!(probe, Value::Int(v) if values.iter().any(|value| *value != v))
                );
            }
        }
        assert!(!Summary::default().matches(&Value::Int(1)));
    }
}
