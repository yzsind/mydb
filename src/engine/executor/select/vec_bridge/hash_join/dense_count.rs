//! Bounded domain-addressed aggregation for preserved integer-key joins.
use super::*;
use crate::engine::executor::mem_budget::{self, ScopedCharge};
use crate::engine::executor::vec::batch::ColumnAccumulator;
use crate::types::Value;

struct CountBuildPrefix<'a> {
    prefix: std::collections::VecDeque<Batch>,
    remaining: Box<dyn BatchOperator + 'a>,
    charge: ScopedCharge,
}

impl BatchOperator for CountBuildPrefix<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        self.remaining.schema()
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if let Some(batch) = self.prefix.pop_front() {
            self.charge.release_bytes(batch.memory_bytes());
            return Ok(Some(batch));
        }
        self.remaining.next_batch()
    }
}

impl HashJoinBatchOperator<'_> {
    /// Admit from actual key bounds, not estimates. Until uniqueness, density,
    /// and memory are proven, retain a replayable prefix of the same snapshot.
    /// No rejected candidate reopens its scan or consumes probe input.
    pub(super) fn try_dense_build_count(
        &mut self,
        aggregate: &mut vec_hash_agg::HashAggregateCore,
        group_sources: &[(usize, usize)],
        sources: &[Option<vec_hash_agg::DirectJoinColumn>],
    ) -> Result<bool, QueryError> {
        if self.state.is_some()
            || self.finished
            || self.grace_output.is_some()
            || self.output.is_some()
            || self.build.schema().len() != 1
            || self.build_keys.as_slice() != [0]
            || self.probe_keys.len() != 1
            || self.null_safe.iter().any(|value| *value)
            || self.key_collations.iter().any(Option::is_some)
            || !matches!(group_sources, [(_, 0)])
            || sources.is_empty()
            || !aggregate.direct_join_columns().1.iter().all(|spec| {
                matches!(
                    spec.func,
                    vec_hash_agg::AggregateKind::Count | vec_hash_agg::AggregateKind::CountStar
                )
            })
            || !vec_hash_join::schema_column_uses_i64_lane(self.build.schema(), 0)
            || !vec_hash_join::schema_column_uses_i64_lane(self.probe.schema(), self.probe_keys[0])
        {
            return Ok(false);
        }
        let limits = crate::engine::executor::select::plan::hash_join_limits();
        let mut cap = mem_budget::current_budget()
            .map_or(limits.mem_limit_bytes as usize, |budget| {
                budget.limit().saturating_sub(budget.used())
            });
        if limits.mem_limit_explicit {
            cap = cap.min(limits.mem_limit_bytes as usize);
        }
        // Leave bounded scan/output workspace within the same query budget.
        cap = cap.saturating_sub((cap / 16).max(64 * 1024));
        let mut prefix = std::collections::VecDeque::new();
        let mut charge = ScopedCharge::new(0);
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        let mut rows = 0usize;
        let mut eligible = true;
        while let Some(batch) = self.build.next_batch()? {
            crate::engine::cancel::check_cancelled()?;
            if batch.visible_rows() == 0 {
                continue;
            }
            // Scan batches can retain a partially used index-page capacity
            // or a selection view. Retain only visible keys, otherwise a
            // narrow domain can fail admission on unused scan workspace.
            let batch = if batch.columns()[0].as_i64_slice().is_some() {
                let indices = batch
                    .visible_indices()
                    .map(|row| row as u32)
                    .collect::<Vec<_>>();
                Batch::with_row_count(
                    batch.schema().clone(),
                    vec![batch.columns()[0].take_indices(&indices)],
                    indices.len(),
                )
            } else {
                batch
            };
            let bytes = batch.memory_bytes();
            let column = &batch.columns()[0];
            if let Some(keys) = column.as_i64_slice() {
                for row in batch.visible_indices() {
                    if column.is_null(row) {
                        eligible = false;
                        break;
                    }
                    min = min.min(keys[row]);
                    max = max.max(keys[row]);
                    rows = rows.saturating_add(1);
                }
            } else {
                eligible = false;
            }
            prefix.push_back(batch);
            if !charge.add_within(bytes, cap) {
                charge.add_force(bytes)?;
                eligible = false;
            }
            let span = (i128::from(max) - i128::from(min) + 1).max(0) as u128;
            let state_bytes = span.saturating_mul(sources.len() as u128 * 8 + 1);
            if state_bytes > cap as u128
                || (limits.max_build_rows > 0 && rows as u64 > limits.max_build_rows)
            {
                eligible = false;
            }
            if !eligible {
                break;
            }
        }
        let span = if rows == 0 {
            0
        } else {
            (i128::from(max) - i128::from(min) + 1).max(0) as usize
        };
        let bitmap_bytes = span.div_ceil(64).saturating_mul(8);
        // A bounded allocation is not sufficient evidence of a useful dense
        // layout: a few far-apart keys should keep the ordinary sparse index.
        eligible &= span <= rows.saturating_mul(16);
        let mut present = Vec::new();
        if eligible && charge.add_within(bitmap_bytes, cap) {
            present.resize(span.div_ceil(64), 0u64);
            for batch in &prefix {
                let keys = batch.columns()[0]
                    .as_i64_slice()
                    .expect("validated integer keys");
                for row in batch.visible_indices() {
                    let slot = (i128::from(keys[row]) - i128::from(min)) as usize;
                    let mask = 1u64 << (slot % 64);
                    if present[slot / 64] & mask != 0 {
                        eligible = false;
                        break;
                    }
                    present[slot / 64] |= mask;
                }
                if !eligible {
                    break;
                }
            }
        } else {
            eligible = false;
        }
        let state_bytes = span
            .saturating_mul(sources.len())
            .saturating_mul(8)
            .saturating_add(bitmap_bytes.saturating_mul(2));
        if eligible
            && state_bytes > charge.charged()
            && !charge.add_within(state_bytes - charge.charged(), cap)
        {
            eligible = false;
        }
        if !eligible {
            drop(present);
            let schema = self.build.schema().clone();
            let remaining = std::mem::replace(
                &mut self.build,
                Box::new(MaterializedBatchOperator::new(schema, Vec::new())),
            );
            self.build = Box::new(CountBuildPrefix {
                prefix,
                remaining,
                charge,
            });
            return Ok(false);
        }
        if rows == 0 {
            // A build-preserved join with no build rows has no output. Do not
            // open a potentially large probe merely to discover no matches.
            aggregate.install_dense_count_output(
                0,
                Vec::new(),
                sources.iter().map(|_| Vec::new()).collect(),
                charge,
            );
            aggregate.finish_direct_join_input(0, 0)?;
            self.finish_input();
            return Ok(true);
        }
        // Transfer the raw-key reservation to dense states before allocation.
        drop(prefix);
        charge.release_bytes(charge.charged().saturating_sub(state_bytes));
        let mut counts = sources.iter().map(|_| vec![0u64; span]).collect::<Vec<_>>();
        let mut matched = vec![0u64; span.div_ceil(64)];
        let mut probe_batches = 0u64;
        let mut probe_rows = 0u64;
        let mut match_rows = 0u64;
        while let Some(batch) = self.probe.next_batch()? {
            crate::engine::cancel::check_cancelled()?;
            probe_batches += 1;
            probe_rows += batch.visible_rows() as u64;
            let key_column = &batch.columns()[self.probe_keys[0]];
            // Expression/CTE adapters may expose canonical integer values in
            // a Generic lane. Normalize only that uncommon representation;
            // keys outside the signed domain cannot match this build.
            let normalization_charge = if key_column.as_i64_slice().is_none() {
                Some(mem_budget::force_charge_scoped(
                    key_column.len().saturating_mul(9),
                )?)
            } else {
                None
            };
            let normalized = if normalization_charge.is_some() {
                let mut column = ColumnAccumulator::for_field(
                    &self.probe.schema().fields()[self.probe_keys[0]],
                    key_column.len(),
                );
                for row in 0..key_column.len() {
                    let key = match crate::engine::expr::ValueKey::from(&key_column.value_at(row)) {
                        crate::engine::expr::ValueKey::Int(key) => i64::try_from(key).ok(),
                        _ => None,
                    };
                    column.push(&key.map_or(Value::Null, Value::Int));
                }
                Some(column.finish())
            } else {
                None
            };
            let key_column = normalized.as_ref().unwrap_or(key_column);
            let Some(keys) = key_column.as_i64_slice() else {
                continue;
            };
            let nonnull = sources
                .iter()
                .map(|source| match source {
                    None | Some(vec_hash_agg::DirectJoinColumn::Build(_)) => true,
                    Some(vec_hash_agg::DirectJoinColumn::Probe(column)) => {
                        let column = &batch.columns()[*column];
                        column.count_non_null() == column.len()
                    }
                })
                .collect::<Vec<_>>();
            for row in batch.visible_indices() {
                if key_column.is_null(row) {
                    continue;
                }
                let slot = i128::from(keys[row]) - i128::from(min);
                if slot < 0 || slot >= span as i128 {
                    continue;
                }
                let slot = slot as usize;
                let mask = 1u64 << (slot % 64);
                if present[slot / 64] & mask == 0 {
                    continue;
                }
                matched[slot / 64] |= mask;
                match_rows += 1;
                for (index, source) in sources.iter().enumerate() {
                    let increment = nonnull[index]
                        || match source {
                            Some(vec_hash_agg::DirectJoinColumn::Probe(column)) => {
                                !batch.columns()[*column].is_null(row)
                            }
                            _ => true,
                        };
                    if increment {
                        counts[index][slot] = counts[index][slot].saturating_add(1);
                    }
                }
            }
        }
        let mut unmatched = 0u64;
        for slot in 0..span {
            if slot % 65536 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            let mask = 1u64 << (slot % 64);
            if present[slot / 64] & mask != 0 && matched[slot / 64] & mask == 0 {
                unmatched += 1;
                for (counts, source) in counts.iter_mut().zip(sources) {
                    if !matches!(source, Some(vec_hash_agg::DirectJoinColumn::Probe(_))) {
                        counts[slot] = 1;
                    }
                }
            }
        }
        drop(matched);
        charge.release_bytes(bitmap_bytes);
        aggregate.install_dense_count_output(min, present, counts, charge);
        aggregate.finish_direct_join_input(probe_batches, match_rows.saturating_add(unmatched))?;
        analyze_stats::add_pending_join_runtime(rows as u64, probe_rows, match_rows);
        self.finish_input();
        Ok(true)
    }
}
