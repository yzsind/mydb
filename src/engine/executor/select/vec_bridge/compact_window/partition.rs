//! Native exact partition keys with an optional bounded direct-address cache.
use super::*;

#[derive(Default)]
pub(super) struct IntegerPartitions {
    lookup: hashbrown::HashMap<Option<i128>, usize, ahash::RandomState>,
    dense: Vec<u32>,
    base: i128,
    low: Option<i128>,
    high: Option<i128>,
    null: Option<usize>,
}

impl IntegerPartitions {
    pub(super) fn value(column: &ColumnVector, row: usize) -> Option<Option<i128>> {
        if column.is_null(row) {
            return Some(None);
        }
        Some(Some(match column {
            ColumnVector::I64 { data, .. }
            | ColumnVector::Time64 { data, .. }
            | ColumnVector::DateTime64 { data, .. }
            | ColumnVector::Timestamp64 { data, .. } => i128::from(data[row]),
            ColumnVector::U64 { data, .. } => i128::from(data[row]),
            ColumnVector::Date32 { data, .. } => i128::from(data[row]),
            ColumnVector::Bool { values, .. } => i128::from(values.get(row)),
            _ => return None,
        }))
    }
    #[inline]
    pub(super) fn get(&self, key: Option<i128>) -> Option<usize> {
        let Some(value) = key else {
            return self.null;
        };
        if let Some(offset) = value
            .checked_sub(self.base)
            .and_then(|value| usize::try_from(value).ok())
        {
            if let Some(&group) = self.dense.get(offset) {
                if group != u32::MAX {
                    return Some(group as usize);
                }
            }
        }
        self.lookup.get(&key).copied()
    }
    pub(super) fn insert(&mut self, key: Option<i128>, group: usize) {
        self.lookup.insert(key, group);
        let Some(value) = key else {
            self.null = Some(group);
            return;
        };
        self.low = Some(self.low.map_or(value, |low| low.min(value)));
        self.high = Some(self.high.map_or(value, |high| high.max(value)));
        if let Some(offset) = value
            .checked_sub(self.base)
            .and_then(|value| usize::try_from(value).ok())
        {
            if let (Some(slot), Ok(group)) = (self.dense.get_mut(offset), u32::try_from(group)) {
                *slot = group;
            }
        }
    }
    pub(super) fn maybe_cache(
        &mut self,
        charge: &mut mem_budget::ScopedCharge,
        reserved: &mut usize,
        other: usize,
        limit: usize,
    ) {
        const MAX_SPAN: usize = 1 << 20;
        if !self.lookup.len().is_power_of_two() {
            return;
        }
        let (Some(low), Some(high)) = (self.low, self.high) else {
            return;
        };
        let Some(span) = high
            .checked_sub(low)
            .and_then(|span| usize::try_from(span).ok())
            .and_then(|span| span.checked_add(1))
        else {
            return;
        };
        if span > MAX_SPAN || span > self.lookup.len().saturating_mul(32) {
            return;
        }
        if low >= self.base
            && high
                .checked_sub(self.base)
                .is_some_and(|offset| offset < self.dense.len() as i128)
        {
            return;
        }
        let span = span.max(64).next_power_of_two();
        let bytes = span * std::mem::size_of::<u32>();
        if other.saturating_add(*reserved).saturating_add(bytes) > limit
            || !try_charge_work(charge, bytes, limit)
        {
            return;
        }
        let mut dense = match mem_budget::try_vec_with_capacity::<u32>(
            span,
            "window partition direct-address cache",
        ) {
            Ok(dense) => dense,
            Err(_) => {
                charge.release_bytes(bytes);
                return;
            }
        };
        dense.resize(span, u32::MAX);
        for (&value, &group) in &self.lookup {
            if let (Some(value), Ok(group)) = (value, u32::try_from(group)) {
                dense[(value - low) as usize] = group;
            }
        }
        let old = self.dense.capacity() * std::mem::size_of::<u32>();
        self.dense = dense;
        self.base = low;
        charge.release_bytes(old);
        *reserved = reserved.saturating_add(bytes).saturating_sub(old);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    #[test]
    fn native_partition_cache_keeps_null_extremes_and_budget_denial_exact() {
        for limit in [128, 16 * 1024 * 1024] {
            let budget = Arc::new(MemBudget::new(limit));
            with_query_budget(budget.clone(), || {
                let mut lookup = IntegerPartitions::default();
                let mut reference = std::collections::BTreeMap::new();
                let mut charge = mem_budget::ScopedCharge::new(0);
                let mut reserved = 0;
                for key in (0..10000).map(|v| Some(v as i128 - 5000)).chain([
                    None,
                    Some(i128::MIN),
                    Some(i128::MAX),
                    Some(u64::MAX as i128),
                ]) {
                    assert_eq!(lookup.get(key), reference.get(&key).copied());
                    let group = reference.len();
                    reference.insert(key, group);
                    lookup.insert(key, group);
                    lookup.maybe_cache(&mut charge, &mut reserved, 0, limit);
                    assert_eq!(lookup.get(key), Some(group));
                }
                for (&key, &group) in &reference {
                    assert_eq!(lookup.get(key), Some(group));
                }
                if limit > 128 {
                    assert!(!lookup.dense.is_empty());
                } else {
                    assert!(lookup.dense.is_empty());
                }
                drop(lookup);
                drop(charge);
                assert_eq!(budget.used(), 0);
                assert!(budget.terminal_oom().is_none());
            });
        }
    }
}
