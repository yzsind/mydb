//! Shared physical Hash Aggregate description and batch-key program.
//!
//! This module deliberately contains no planner- or source-shape routing.
//! Scan, join, row and vector inputs compile to the same [`AggregatePlan`];
//! physical implementations may choose a typed state layout, but semantic
//! aggregate operations and canonical group-key encoding live here.

use std::sync::Arc;

pub(crate) mod parallel_distinct;

use crate::engine::executor::select::aggregate::{
    accumulate_numeric_state, accumulate_numeric_state_decimal_parts,
    accumulate_numeric_state_float, finalize_avg_value, merge_numeric_state,
    numeric_state_to_value, AggregateKind, NumericAggState,
};
use crate::engine::executor::vec::batch::{Batch, ColumnVector};
use crate::engine::executor::vec::schema::BatchSchema;
use crate::types::Value;

pub(crate) type DistinctMap =
    hashbrown::HashMap<crate::engine::expr::ValueKey, Value, ahash::RandomState>;
pub(crate) type CountDistinctKeySet =
    hashbrown::HashSet<crate::engine::expr::KeyBytes, ahash::RandomState>;

/// Canonical DISTINCT state shared by scalar/native layouts and partial-state
/// spill. Tiny signed-integer sets stay inline and promote without changing
/// equality or first-representative semantics.
#[derive(Debug, Clone)]
pub(crate) enum DistinctState {
    SmallI64(smallvec::SmallVec<[i64; 8]>),
    I64Set(hashbrown::HashSet<i64, ahash::RandomState>),
    U64Set(hashbrown::HashSet<u64, ahash::RandomState>),
    Map(DistinctMap),
    /// COUNT(DISTINCT text) needs only the canonical comparison key, not a
    /// second retained copy of the value. The batch aggregate inserts into
    /// this set through borrowed byte probes and allocates only on a miss.
    CountKeys(CountDistinctKeySet),
}

impl DistinctState {
    pub(crate) fn with_value(value: Value) -> Self {
        if let Value::Int(value) = value {
            let mut values = smallvec::SmallVec::new();
            values.push(value);
            Self::SmallI64(values)
        } else if let Value::UInt(value) = value {
            let mut values = hashbrown::HashSet::with_hasher(ahash::RandomState::new());
            values.insert(value);
            Self::U64Set(values)
        } else {
            let mut values = DistinctMap::with_hasher(ahash::RandomState::new());
            values.insert(crate::engine::expr::ValueKey::from(&value), value);
            Self::Map(values)
        }
    }

    /// Returns true iff `value` was not already present.
    pub(crate) fn insert(&mut self, value: Value) -> bool {
        if let (Self::SmallI64(values), Value::Int(value)) = (&mut *self, &value) {
            if values.contains(value) {
                return false;
            }
            if values.len() < values.inline_size() {
                values.push(*value);
                return true;
            }
        }
        if matches!(self, Self::SmallI64(_)) {
            let old = std::mem::replace(
                self,
                Self::I64Set(hashbrown::HashSet::with_hasher(ahash::RandomState::new())),
            );
            let Self::SmallI64(old) = old else {
                unreachable!()
            };
            let Self::I64Set(values) = self else {
                unreachable!()
            };
            values.reserve(old.len().saturating_add(1));
            for value in old {
                values.insert(value);
            }
        }
        if let Self::I64Set(values) = self {
            if let Value::Int(value) = value {
                return values.insert(value);
            }
            let old = std::mem::replace(
                self,
                Self::Map(DistinctMap::with_hasher(ahash::RandomState::new())),
            );
            let Self::I64Set(old) = old else {
                unreachable!()
            };
            let Self::Map(map) = self else { unreachable!() };
            map.reserve(old.len().saturating_add(1));
            for value in old {
                let value = Value::Int(value);
                map.insert(crate::engine::expr::ValueKey::from(&value), value);
            }
        }
        if let Self::U64Set(values) = self {
            if let Value::UInt(value) = value {
                return values.insert(value);
            }
            let old = std::mem::replace(
                self,
                Self::Map(DistinctMap::with_hasher(ahash::RandomState::new())),
            );
            let Self::U64Set(old) = old else {
                unreachable!()
            };
            let Self::Map(map) = self else { unreachable!() };
            map.reserve(old.len().saturating_add(1));
            for value in old {
                let value = Value::UInt(value);
                map.insert(crate::engine::expr::ValueKey::from(&value), value);
            }
        }
        if let Self::CountKeys(keys) = self {
            let key = crate::engine::expr::ValueKey::from(&value);
            let bytes = match key {
                crate::engine::expr::ValueKey::Bytes(bytes)
                | crate::engine::expr::ValueKey::String(bytes) => bytes,
                _ => {
                    let old = std::mem::replace(
                        self,
                        Self::Map(DistinctMap::with_hasher(ahash::RandomState::new())),
                    );
                    let Self::CountKeys(old) = old else {
                        unreachable!()
                    };
                    let Self::Map(map) = self else { unreachable!() };
                    map.reserve(old.len().saturating_add(1));
                    for key in old {
                        let value = Value::Bytes(key.into_vec());
                        map.insert(crate::engine::expr::ValueKey::from(&value), value);
                    }
                    return if let hashbrown::hash_map::Entry::Vacant(entry) =
                        map.entry(crate::engine::expr::ValueKey::from(&value))
                    {
                        entry.insert(value);
                        true
                    } else {
                        false
                    };
                }
            };
            return keys.insert(bytes);
        }
        let Self::Map(map) = self else { unreachable!() };
        let key = crate::engine::expr::ValueKey::from(&value);
        if let hashbrown::hash_map::Entry::Vacant(entry) = map.entry(key) {
            entry.insert(value);
            true
        } else {
            false
        }
    }

    pub(crate) fn with_count_key_bytes(bytes: &[u8]) -> Self {
        let mut values = CountDistinctKeySet::with_hasher(ahash::RandomState::new());
        values.insert(crate::engine::expr::KeyBytes::from_slice(bytes));
        Self::CountKeys(values)
    }

    pub(crate) fn insert_count_key_bytes(&mut self, bytes: &[u8]) -> bool {
        match self {
            Self::CountKeys(values) => {
                if values.contains(bytes) {
                    false
                } else {
                    values.insert(crate::engine::expr::KeyBytes::from_slice(bytes));
                    true
                }
            }
            // Spill replay and mixed physical paths can reconstruct the
            // canonical bytes through the general representation. Preserve
            // correctness instead of changing state layout mid-stream.
            _ => self.insert(Value::Bytes(bytes.to_vec())),
        }
    }

    fn count_key_allocated_bytes(&self, bytes: &[u8]) -> usize {
        match self {
            Self::CountKeys(values) => values
                .get(bytes)
                .filter(|key| key.spilled())
                .map_or(0, smallvec::SmallVec::capacity),
            Self::Map(_) => {
                bytes
                    .len()
                    .saturating_add(if bytes.len() > 22 { bytes.len() } else { 0 })
            }
            Self::SmallI64(_) | Self::I64Set(_) | Self::U64Set(_) => bytes.len(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::SmallI64(values) => values.len(),
            Self::I64Set(values) => values.len(),
            Self::U64Set(values) => values.len(),
            Self::Map(values) => values.len(),
            Self::CountKeys(values) => values.len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn for_each_value(&self, mut visit: impl FnMut(&Value)) {
        match self {
            Self::SmallI64(values) => {
                for value in values {
                    let value = Value::Int(*value);
                    visit(&value);
                }
            }
            Self::I64Set(values) => {
                for value in values {
                    let value = Value::Int(*value);
                    visit(&value);
                }
            }
            Self::U64Set(values) => {
                for value in values {
                    let value = Value::UInt(*value);
                    visit(&value);
                }
            }
            Self::Map(values) => values.values().for_each(visit),
            Self::CountKeys(values) => {
                for key in values {
                    let value = Value::Bytes(key.to_vec());
                    visit(&value);
                }
            }
        }
    }

    pub(crate) fn map_capacity(&self) -> usize {
        match self {
            Self::SmallI64(_) => 0,
            Self::I64Set(values) => values.capacity(),
            Self::U64Set(values) => values.capacity(),
            Self::Map(values) => values.capacity(),
            Self::CountKeys(values) => values.capacity(),
        }
    }

    pub(crate) fn into_values(self) -> Vec<Value> {
        match self {
            Self::SmallI64(values) => values.into_iter().map(Value::Int).collect(),
            Self::I64Set(values) => values.into_iter().map(Value::Int).collect(),
            Self::U64Set(values) => values.into_iter().map(Value::UInt).collect(),
            Self::Map(values) => values.into_values().collect(),
            Self::CountKeys(values) => values
                .into_iter()
                .map(|key| Value::Bytes(key.into_vec()))
                .collect(),
        }
    }
}

/// Canonical state for BIT and variance families.
#[derive(Default, Debug, Clone)]
pub struct ExtraAggregateState {
    pub(crate) bits: u64,
    pub(crate) bits_seen: bool,
    pub(crate) var_n: u64,
    pub(crate) var_mean: f64,
    pub(crate) var_m2: f64,
}

/// Transport representation used identically by worker handoff, spill replay
/// and ROLLUP derivation. Physical state layouts must convert to this type
/// rather than defining their own merge/finalize semantics.
#[derive(Default, Debug, Clone)]
pub struct AggregatePartial {
    pub(crate) sum: NumericAggState,
    pub count: u64,
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub extra: Option<Box<ExtraAggregateState>>,
    pub(crate) distinct: Option<Box<DistinctState>>,
}

/// Canonical per-group state. Native state columns may store a narrower
/// physical representation, but conversion, worker handoff, spill merge and
/// finalization all use this semantic state/partial contract.
#[derive(Default, Debug, Clone)]
pub(crate) struct AggregateState {
    pub(crate) sum: NumericAggState,
    pub(crate) count: u64,
    pub(crate) min: Option<Value>,
    pub(crate) max: Option<Value>,
    pub(crate) extra: Option<Box<ExtraAggregateState>>,
    pub(crate) distinct: Option<Box<DistinctState>>,
    distinct_payload_bytes: usize,
}

// Keep one SQL finalization definition while preserving each state's direct
// field access. An out-of-line helper taking all state parts can read unrelated
// fields even for COUNT; expansion retains the original per-kind hot path
// without a state-view allocation or an extra calling convention.
macro_rules! finalize_aggregate {
    ($state:expr, $kind:expr) => {{
        let state = $state;
        let kind = $kind;

        match kind {
            AggregateKind::CountStar | AggregateKind::Count => Value::Int(state.count as i64),
            AggregateKind::Sum => {
                if state.count == 0 {
                    Value::Null
                } else {
                    numeric_state_to_value(&state.sum)
                }
            }
            AggregateKind::Avg => finalize_avg_value(&state.sum, state.count),
            AggregateKind::Min => state.min.clone().unwrap_or(Value::Null),
            AggregateKind::Max => state.max.clone().unwrap_or(Value::Null),
            AggregateKind::BitAnd | AggregateKind::BitOr | AggregateKind::BitXor => state
                .extra
                .as_deref()
                .filter(|extra| extra.bits_seen)
                .map(|extra| Value::UInt(extra.bits))
                .unwrap_or_else(|| {
                    Value::UInt(if kind == AggregateKind::BitAnd {
                        u64::MAX
                    } else {
                        0
                    })
                }),
            AggregateKind::VarPop
            | AggregateKind::VarSamp
            | AggregateKind::StddevPop
            | AggregateKind::StddevSamp => finalize_variance(state.extra.as_deref(), kind),
            AggregateKind::CountDistinct
            | AggregateKind::SumDistinct
            | AggregateKind::AvgDistinct => finalize_distinct(state.distinct.as_deref(), kind),
        }
    }};
}

impl AggregateState {
    #[inline]
    pub(crate) fn extra_mut(&mut self) -> &mut ExtraAggregateState {
        self.extra
            .get_or_insert_with(|| Box::new(ExtraAggregateState::default()))
    }

    #[inline]
    pub(crate) fn distinct_insert(&mut self, value: Value) {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        let key = crate::engine::expr::ValueKey::from(&value);
        let payload_bytes = key
            .allocated_bytes()
            .saturating_add(value.allocated_bytes());
        let inserted = match self.distinct.as_mut() {
            Some(state) => state.insert(value),
            None => {
                self.distinct = Some(Box::new(DistinctState::with_value(value)));
                true
            }
        };
        if inserted {
            self.distinct_payload_bytes = self.distinct_payload_bytes.saturating_add(payload_bytes);
        }
    }

    #[inline]
    pub(crate) fn count_distinct_key_bytes(&mut self, bytes: &[u8]) {
        let inserted = match self.distinct.as_mut() {
            Some(state) => state.insert_count_key_bytes(bytes),
            None => {
                self.distinct = Some(Box::new(DistinctState::with_count_key_bytes(bytes)));
                true
            }
        };
        if inserted {
            let payload_bytes = self
                .distinct
                .as_deref()
                .map_or(bytes.len(), |state| state.count_key_allocated_bytes(bytes));
            self.distinct_payload_bytes = self.distinct_payload_bytes.saturating_add(payload_bytes);
        }
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        const HASH_ENTRY_OVERHEAD: usize = 16;
        let mut bytes = self
            .min
            .as_ref()
            .map(AllocatedBytes::allocated_bytes)
            .unwrap_or(0)
            .saturating_add(
                self.max
                    .as_ref()
                    .map(AllocatedBytes::allocated_bytes)
                    .unwrap_or(0),
            );
        if self.extra.is_some() {
            bytes = bytes.saturating_add(std::mem::size_of::<ExtraAggregateState>());
        }
        if let Some(distinct) = &self.distinct {
            let entry_bytes = match distinct.as_ref() {
                DistinctState::CountKeys(_) => {
                    HASH_ENTRY_OVERHEAD + std::mem::size_of::<crate::engine::expr::KeyBytes>()
                }
                DistinctState::I64Set(_) => HASH_ENTRY_OVERHEAD + std::mem::size_of::<i64>(),
                DistinctState::U64Set(_) => HASH_ENTRY_OVERHEAD + std::mem::size_of::<u64>(),
                _ => {
                    HASH_ENTRY_OVERHEAD
                        + std::mem::size_of::<crate::engine::expr::ValueKey>()
                        + std::mem::size_of::<Value>()
                }
            };
            bytes = bytes
                .saturating_add(std::mem::size_of::<DistinctState>())
                .saturating_add(distinct.map_capacity() * entry_bytes)
                .saturating_add(self.distinct_payload_bytes);
        }
        bytes
    }

    pub(crate) fn merge_from(&mut self, source: Self, kind: AggregateKind) {
        match kind {
            AggregateKind::CountStar | AggregateKind::Count => {
                self.count = self.count.saturating_add(source.count);
            }
            AggregateKind::Sum | AggregateKind::Avg => {
                merge_numeric_state(&mut self.sum, source.sum);
                self.count = self.count.saturating_add(source.count);
            }
            AggregateKind::Min => merge_minmax(&mut self.min, source.min, true),
            AggregateKind::Max => merge_minmax(&mut self.max, source.max, false),
            AggregateKind::BitAnd | AggregateKind::BitOr | AggregateKind::BitXor => {
                if let Some(extra) = source.extra.filter(|extra| extra.bits_seen) {
                    update_bit(self.extra_mut(), kind, extra.bits);
                }
            }
            AggregateKind::VarPop
            | AggregateKind::VarSamp
            | AggregateKind::StddevPop
            | AggregateKind::StddevSamp => {
                if let Some(extra) = source.extra {
                    merge_variance(self.extra_mut(), extra.var_n, extra.var_mean, extra.var_m2);
                }
            }
            AggregateKind::CountDistinct => {
                if let Some(values) = source.distinct {
                    if self.distinct.is_none() {
                        self.distinct = Some(values);
                        self.distinct_payload_bytes = self
                            .distinct_payload_bytes
                            .saturating_add(source.distinct_payload_bytes);
                    } else {
                        match values.as_ref() {
                            DistinctState::CountKeys(keys) => {
                                for key in keys {
                                    self.count_distinct_key_bytes(key);
                                }
                            }
                            _ => values.for_each_value(|value| self.distinct_insert(value.clone())),
                        }
                    }
                }
            }
            AggregateKind::SumDistinct | AggregateKind::AvgDistinct => {
                if let Some(values) = source.distinct {
                    for value in values.into_values() {
                        self.distinct_insert(value);
                    }
                }
            }
        }
    }

    pub(crate) fn into_partial(self) -> AggregatePartial {
        AggregatePartial {
            sum: self.sum,
            count: self.count,
            min: self.min,
            max: self.max,
            extra: self.extra,
            distinct: self.distinct,
        }
    }

    pub(crate) fn finalize(&self, kind: AggregateKind) -> Value {
        finalize_aggregate!(self, kind)
    }

    pub(crate) fn update_count_star_repeated(&mut self, repetitions: usize) {
        self.count = self.count.saturating_add(repetitions as u64);
    }

    pub(crate) fn update_repeated_ref(
        &mut self,
        kind: AggregateKind,
        value: &Value,
        repetitions: usize,
    ) -> Result<(), crate::types::QueryError> {
        match kind {
            AggregateKind::CountStar => self.update_count_star_repeated(repetitions),
            AggregateKind::Count if !matches!(value, Value::Null) => {
                self.update_count_star_repeated(repetitions)
            }
            AggregateKind::Min | AggregateKind::Max if repetitions != 0 => {
                self.update_ref(kind, value)?;
            }
            _ => {
                for _ in 0..repetitions {
                    self.update_ref(kind, value)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_ref(
        &mut self,
        kind: AggregateKind,
        value: &Value,
    ) -> Result<(), crate::types::QueryError> {
        if kind.is_distinct() {
            if !matches!(value, Value::Null) {
                self.distinct_insert(value.clone());
            }
            return Ok(());
        }
        if matches!(value, Value::Null) {
            return Ok(());
        }
        match kind {
            AggregateKind::CountStar | AggregateKind::Count => {
                self.count = self.count.saturating_add(1)
            }
            AggregateKind::Sum | AggregateKind::Avg => {
                accumulate_numeric_state(&mut self.sum, value)?;
                self.count = self.count.saturating_add(1);
            }
            AggregateKind::Min => update_minmax(&mut self.min, value, true)?,
            AggregateKind::Max => update_minmax(&mut self.max, value, false)?,
            AggregateKind::BitAnd | AggregateKind::BitOr | AggregateKind::BitXor => {
                let bits = crate::engine::expr::int_value(value)? as u64;
                update_bit(self.extra_mut(), kind, bits);
            }
            AggregateKind::VarPop
            | AggregateKind::VarSamp
            | AggregateKind::StddevPop
            | AggregateKind::StddevSamp => {
                update_variance(self.extra_mut(), crate::engine::expr::numeric(value)?);
                if !self.extra.as_ref().unwrap().var_m2.is_finite() {
                    return Err(crate::error::catalog::ER_DATA_OUT_OF_RANGE
                        .error("DOUBLE value is out of range in variance aggregate"));
                }
            }
            AggregateKind::CountDistinct
            | AggregateKind::SumDistinct
            | AggregateKind::AvgDistinct => unreachable!("DISTINCT handled before dispatch"),
        }
        Ok(())
    }

    pub(crate) fn update_i64(
        &mut self,
        kind: AggregateKind,
        value: i64,
    ) -> Result<(), crate::types::QueryError> {
        match kind {
            AggregateKind::CountStar | AggregateKind::Count => {
                self.count = self.count.saturating_add(1);
                Ok(())
            }
            AggregateKind::Sum | AggregateKind::Avg => {
                accumulate_numeric_state_decimal_parts(&mut self.sum, value.into(), 0);
                self.count = self.count.saturating_add(1);
                Ok(())
            }
            _ => self.update_ref(kind, &Value::Int(value)),
        }
    }

    pub(crate) fn update_u64(
        &mut self,
        kind: AggregateKind,
        value: u64,
    ) -> Result<(), crate::types::QueryError> {
        match kind {
            AggregateKind::CountStar | AggregateKind::Count => {
                self.count = self.count.saturating_add(1);
                Ok(())
            }
            AggregateKind::Sum | AggregateKind::Avg => {
                accumulate_numeric_state_decimal_parts(&mut self.sum, value.into(), 0);
                self.count = self.count.saturating_add(1);
                Ok(())
            }
            _ => self.update_ref(kind, &Value::UInt(value)),
        }
    }

    pub(crate) fn update_f64(
        &mut self,
        kind: AggregateKind,
        value: f64,
    ) -> Result<(), crate::types::QueryError> {
        match kind {
            AggregateKind::CountStar | AggregateKind::Count => {
                self.count = self.count.saturating_add(1);
                Ok(())
            }
            AggregateKind::Sum | AggregateKind::Avg => {
                accumulate_numeric_state_float(&mut self.sum, value);
                self.count = self.count.saturating_add(1);
                Ok(())
            }
            _ => self.update_ref(kind, &Value::Double(value)),
        }
    }

    pub(crate) fn update_decimal_parts(
        &mut self,
        kind: AggregateKind,
        magnitude: i128,
        scale: u8,
    ) -> Result<(), crate::types::QueryError> {
        match kind {
            AggregateKind::CountStar | AggregateKind::Count => {
                self.count = self.count.saturating_add(1);
                Ok(())
            }
            AggregateKind::Sum | AggregateKind::Avg => {
                accumulate_numeric_state_decimal_parts(&mut self.sum, magnitude, scale);
                self.count = self.count.saturating_add(1);
                Ok(())
            }
            _ => {
                let value = i64::try_from(magnitude)
                    .map(|value| Value::DecimalCompact(value, scale))
                    .unwrap_or(Value::Decimal128(magnitude, scale));
                self.update_ref(kind, &value)
            }
        }
    }
}

fn update_minmax(
    target: &mut Option<Value>,
    value: &Value,
    is_min: bool,
) -> Result<(), crate::types::QueryError> {
    let replace = if let Some(current) = target.as_ref() {
        let ordering = crate::engine::expr::compare_values(current, value)?;
        if is_min {
            ordering.is_gt()
        } else {
            ordering.is_lt()
        }
    } else {
        true
    };
    if replace {
        *target = Some(value.clone());
    }
    Ok(())
}

/// Slot-major canonical state arena used by source fusions and as the scalar
/// layout of the unified Hash Aggregate core.
#[derive(Debug, Clone)]
pub(crate) struct AggregateStateArena {
    specs: Arc<[AggregateSpec]>,
    columns: Vec<Vec<AggregateState>>,
    groups: usize,
}

impl AggregateStateArena {
    pub(crate) fn with_capacity(specs: &[AggregateSpec], groups: usize) -> Self {
        Self {
            specs: specs.to_vec().into(),
            columns: specs.iter().map(|_| Vec::with_capacity(groups)).collect(),
            groups: 0,
        }
    }

    pub(crate) fn add_group(&mut self) -> usize {
        let group = self.groups;
        for column in &mut self.columns {
            column.push(AggregateState::default());
        }
        self.groups += 1;
        group
    }

    pub(crate) fn reserve_groups(&mut self, additional: usize) {
        for column in &mut self.columns {
            column.reserve_exact(additional);
        }
    }

    pub(crate) fn groups(&self) -> usize {
        self.groups
    }

    pub(crate) fn slots(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn state_mut(&mut self, group: usize, slot: usize) -> Option<&mut AggregateState> {
        self.columns.get_mut(slot)?.get_mut(group)
    }

    pub(crate) fn spec(&self, slot: usize) -> Option<&AggregateSpec> {
        self.specs.get(slot)
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        self.columns
            .iter()
            .map(|column| {
                column
                    .capacity()
                    .saturating_mul(std::mem::size_of::<AggregateState>())
                    .saturating_add(
                        column
                            .iter()
                            .map(AggregateState::allocated_bytes)
                            .sum::<usize>(),
                    )
            })
            .sum()
    }

    pub(crate) fn take_group_partials(&mut self, group: usize) -> Option<Vec<AggregatePartial>> {
        if group >= self.groups {
            return None;
        }
        Some(
            self.columns
                .iter_mut()
                .map(|column| std::mem::take(&mut column[group]).into_partial())
                .collect(),
        )
    }
}

impl AggregatePartial {
    pub(crate) fn merge_from(&mut self, source: &Self, kind: AggregateKind) {
        match kind {
            AggregateKind::CountStar | AggregateKind::Count => {
                self.count = self.count.saturating_add(source.count);
            }
            AggregateKind::Sum | AggregateKind::Avg => {
                merge_numeric_state(&mut self.sum, source.sum.clone());
                self.count = self.count.saturating_add(source.count);
            }
            AggregateKind::Min => merge_minmax(&mut self.min, source.min.clone(), true),
            AggregateKind::Max => merge_minmax(&mut self.max, source.max.clone(), false),
            AggregateKind::BitAnd | AggregateKind::BitOr | AggregateKind::BitXor => {
                if let Some(extra) = source.extra.as_deref().filter(|extra| extra.bits_seen) {
                    update_bit(
                        self.extra
                            .get_or_insert_with(|| Box::new(ExtraAggregateState::default())),
                        kind,
                        extra.bits,
                    );
                }
            }
            AggregateKind::VarPop
            | AggregateKind::VarSamp
            | AggregateKind::StddevPop
            | AggregateKind::StddevSamp => {
                if let Some(extra) = source.extra.as_deref() {
                    merge_variance(
                        self.extra
                            .get_or_insert_with(|| Box::new(ExtraAggregateState::default())),
                        extra.var_n,
                        extra.var_mean,
                        extra.var_m2,
                    );
                }
            }
            AggregateKind::CountDistinct => {
                if let Some(source) = source.distinct.as_deref() {
                    match self.distinct.as_mut() {
                        None => self.distinct = Some(Box::new(source.clone())),
                        Some(target) => match source {
                            DistinctState::CountKeys(keys) => {
                                for key in keys {
                                    target.insert_count_key_bytes(key);
                                }
                            }
                            _ => source.for_each_value(|value| {
                                target.insert(value.clone());
                            }),
                        },
                    }
                }
            }
            AggregateKind::SumDistinct | AggregateKind::AvgDistinct => {
                if let Some(source) = source.distinct.as_deref() {
                    source.for_each_value(|value| match self.distinct.as_mut() {
                        Some(target) => {
                            target.insert(value.clone());
                        }
                        None => {
                            self.distinct =
                                Some(Box::new(DistinctState::with_value(value.clone())));
                        }
                    });
                }
            }
        }
    }

    pub(crate) fn finalize(&self, kind: AggregateKind) -> Value {
        finalize_aggregate!(self, kind)
    }
}

fn merge_minmax(target: &mut Option<Value>, source: Option<Value>, is_min: bool) {
    let Some(value) = source else { return };
    if target.as_ref().is_none_or(|current| {
        let ordering = crate::engine::expr::compare_values(current, &value)
            .unwrap_or(std::cmp::Ordering::Equal);
        if is_min {
            ordering == std::cmp::Ordering::Greater
        } else {
            ordering == std::cmp::Ordering::Less
        }
    }) {
        *target = Some(value);
    }
}

/// Fold one non-NULL numeric input or one non-empty partial into BIT state.
/// Callers retain responsibility for SQL conversion and skipping empty partials.
#[inline]
pub(crate) fn update_bit(target: &mut ExtraAggregateState, kind: AggregateKind, bits: u64) {
    target.bits = if !target.bits_seen {
        bits
    } else {
        match kind {
            AggregateKind::BitAnd => target.bits & bits,
            AggregateKind::BitOr => target.bits | bits,
            AggregateKind::BitXor => target.bits ^ bits,
            _ => unreachable!("BIT update requires BIT aggregate"),
        }
    };
    target.bits_seen = true;
}

pub(crate) fn merge_variance(target: &mut ExtraAggregateState, n: u64, mean: f64, m2: f64) {
    if n == 0 {
        return;
    }
    if target.var_n == 0 {
        target.var_n = n;
        target.var_mean = mean;
        target.var_m2 = m2;
        return;
    }
    let total = target.var_n.saturating_add(n);
    let delta = mean - target.var_mean;
    target.var_m2 += m2 + delta * delta * target.var_n as f64 * n as f64 / total as f64;
    target.var_mean += delta * n as f64 / total as f64;
    target.var_n = total;
}

#[inline]
pub(crate) fn update_variance(target: &mut ExtraAggregateState, value: f64) {
    target.var_n = target.var_n.saturating_add(1);
    let delta = value - target.var_mean;
    target.var_mean += delta / target.var_n as f64;
    let delta_after = value - target.var_mean;
    target.var_m2 += delta * delta_after;
}

fn finalize_distinct(distinct: Option<&DistinctState>, kind: AggregateKind) -> Value {
    match kind {
        AggregateKind::CountDistinct => {
            Value::Int(distinct.map(DistinctState::len).unwrap_or(0) as i64)
        }
        AggregateKind::SumDistinct => {
            if distinct.is_none_or(DistinctState::is_empty) {
                return Value::Null;
            }
            let mut state = NumericAggState::default();
            distinct
                .expect("non-empty DISTINCT state")
                .for_each_value(|value| {
                    let _ = accumulate_numeric_state(&mut state, value);
                });
            numeric_state_to_value(&state)
        }
        AggregateKind::AvgDistinct => {
            let mut state = NumericAggState::default();
            let mut count = 0;
            if let Some(distinct) = distinct {
                distinct.for_each_value(|value| {
                    let _ = accumulate_numeric_state(&mut state, value);
                    count += 1;
                });
            }
            finalize_avg_value(&state, count)
        }
        _ => unreachable!("DISTINCT finalization requires DISTINCT aggregate"),
    }
}

fn finalize_variance(extra: Option<&ExtraAggregateState>, kind: AggregateKind) -> Value {
    let (sample, sqrt) = kind
        .variance_flags()
        .expect("variance finalization requires variance aggregate");
    let (n, m2) = extra.map_or((0, 0.0), |state| (state.var_n, state.var_m2));
    if n == 0 || (sample && n < 2) {
        return Value::Null;
    }
    let denominator = if sample { n - 1 } else { n };
    let variance = m2 / denominator as f64;
    let variance = if variance.is_finite() {
        variance.max(0.0)
    } else {
        variance
    };
    Value::Double(if sqrt { variance.sqrt() } else { variance })
}

/// One aggregate input in the physical batch schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateSpec {
    /// MIN/MAX comparison domain, independent of the connection default.
    pub comparison_collation: Option<u16>,
    pub func: AggregateKind,
    /// Ignored by `COUNT(*)`.
    pub input_column: usize,
}

/// Slots that read one physical input lane.  Keeping this in the shared plan
/// lets scalar/native layouts make the same one-pass lane decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateInputGroup {
    pub input_column: usize,
    pub slots: Vec<usize>,
}

/// Immutable, source-independent physical description of a Hash Aggregate.
#[derive(Debug, Clone)]
pub struct AggregatePlan {
    group_columns: Vec<usize>,
    aggregates: Vec<AggregateSpec>,
    input_groups: Vec<AggregateInputGroup>,
    count_star_slots: Vec<usize>,
}

impl AggregatePlan {
    pub fn new(group_columns: Vec<usize>, aggregates: Vec<AggregateSpec>) -> Self {
        debug_assert!(
            aggregates
                .iter()
                .all(|spec| spec.func.is_unordered_mergeable()),
            "native Hash Aggregate plan accepts mergeable semantic operations only"
        );
        let mut input_groups: Vec<AggregateInputGroup> = Vec::new();
        let mut count_star_slots = Vec::new();
        for (slot, spec) in aggregates.iter().enumerate() {
            if spec.func == AggregateKind::CountStar {
                count_star_slots.push(slot);
                continue;
            }
            match input_groups
                .iter_mut()
                .find(|group| group.input_column == spec.input_column)
            {
                Some(group) => group.slots.push(slot),
                None => input_groups.push(AggregateInputGroup {
                    input_column: spec.input_column,
                    slots: vec![slot],
                }),
            }
        }
        Self {
            group_columns,
            aggregates,
            input_groups,
            count_star_slots,
        }
    }

    #[inline]
    pub fn group_columns(&self) -> &[usize] {
        &self.group_columns
    }

    #[inline]
    pub fn aggregates(&self) -> &[AggregateSpec] {
        &self.aggregates
    }

    #[inline]
    pub fn input_groups(&self) -> &[AggregateInputGroup] {
        &self.input_groups
    }

    #[inline]
    pub fn count_star_slots(&self) -> &[usize] {
        &self.count_star_slots
    }
}

/// Canonical key encoder compiled once from group column indexes and schema.
/// Optional dense/packed/dictionary indexes must return the same canonical
/// hash and may never redefine equality.
#[derive(Debug, Clone)]
pub struct GroupKeyProgram {
    columns: Vec<usize>,
    collations: Vec<Option<u16>>,
}

impl GroupKeyProgram {
    pub fn compile(columns: &[usize], schema: &BatchSchema) -> Option<Self> {
        let collations = columns
            .iter()
            .map(|column| {
                schema
                    .fields()
                    .get(*column)
                    .map(|field| field.source.collation_id)
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            columns: columns.to_vec(),
            collations,
        })
    }

    #[inline]
    pub fn encode_row(&self, batch: &Batch, row: usize, output: &mut Vec<u8>) {
        output.clear();
        for (column, collation) in self.columns.iter().zip(self.collations.iter()) {
            crate::engine::executor::vec::key_encode::encode_from_column_row_with_collation(
                &batch.columns()[*column],
                row,
                *collation,
                output,
            );
        }
    }
}

/// Per-batch state update strategy.  This is an internal batching choice,
/// never a physical-plan or semantic route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateUpdateMode {
    RowWise,
    GroupWise,
}

/// Choose the generic group-wise lane path only when the selection-building
/// cost is amortised by repeated groups and at least one typed numeric lane.
pub fn choose_update_mode(
    batch: &Batch,
    touched_groups: usize,
    aggregates: &[AggregateSpec],
) -> AggregateUpdateMode {
    const MIN_BATCH_ROWS: usize = 256;
    const MIN_ROWS_PER_GROUP: usize = 8;
    let rows = batch.visible_rows();
    if rows < MIN_BATCH_ROWS
        || touched_groups == 0
        || rows / touched_groups < MIN_ROWS_PER_GROUP
        || !aggregates.iter().any(|spec| {
            matches!(
                spec.func,
                AggregateKind::Count
                    | AggregateKind::CountStar
                    | AggregateKind::Sum
                    | AggregateKind::Avg
                    | AggregateKind::BitAnd
                    | AggregateKind::BitOr
                    | AggregateKind::BitXor
                    | AggregateKind::VarPop
                    | AggregateKind::VarSamp
                    | AggregateKind::StddevPop
                    | AggregateKind::StddevSamp
            ) && batch
                .columns()
                .get(spec.input_column)
                .is_some_and(is_typed_numeric_lane)
        })
    {
        AggregateUpdateMode::RowWise
    } else {
        AggregateUpdateMode::GroupWise
    }
}

fn is_typed_numeric_lane(column: &ColumnVector) -> bool {
    matches!(
        column,
        ColumnVector::I64 { .. }
            | ColumnVector::U64 { .. }
            | ColumnVector::F64 { .. }
            | ColumnVector::Decimal32 { .. }
            | ColumnVector::Decimal64 { .. }
            | ColumnVector::Decimal128 { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::vec::batch::Batch;
    use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
    use crate::types::{ColumnType, Value};

    fn schema() -> Arc<BatchSchema> {
        Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "g".into(),
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

    #[test]
    fn plan_coalesces_slots_by_input_lane() {
        let plan = AggregatePlan::new(
            vec![0],
            vec![
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::CountStar,
                    input_column: 0,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Count,
                    input_column: 1,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Sum,
                    input_column: 1,
                },
            ],
        );
        assert_eq!(plan.count_star_slots(), &[0]);
        assert_eq!(plan.input_groups().len(), 1);
        assert_eq!(plan.input_groups()[0].slots, vec![1, 2]);
    }

    #[test]
    fn update_mode_uses_cardinality_not_query_shape() {
        let schema = schema();
        let rows = 1024;
        let batch = Batch::new(
            schema,
            vec![
                ColumnVector::from_values(
                    (0..rows).map(|row| Value::Int((row % 4) as i64)).collect(),
                ),
                ColumnVector::from_values((0..rows).map(|row| Value::Int(row as i64)).collect()),
            ],
        );
        let specs = [AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::Sum,
            input_column: 1,
        }];
        assert_eq!(
            choose_update_mode(&batch, 4, &specs),
            AggregateUpdateMode::GroupWise
        );
        assert_eq!(
            choose_update_mode(&batch, rows, &specs),
            AggregateUpdateMode::RowWise
        );
    }

    #[test]
    fn canonical_partial_merge_matches_single_state_for_every_native_family() {
        let cases = [
            AggregateKind::Count,
            AggregateKind::Sum,
            AggregateKind::Avg,
            AggregateKind::Min,
            AggregateKind::Max,
            AggregateKind::BitAnd,
            AggregateKind::BitOr,
            AggregateKind::BitXor,
            AggregateKind::VarPop,
            AggregateKind::VarSamp,
            AggregateKind::StddevPop,
            AggregateKind::StddevSamp,
            AggregateKind::CountDistinct,
            AggregateKind::SumDistinct,
            AggregateKind::AvgDistinct,
        ];
        let left_values = [Value::Int(4), Value::Int(4), Value::Null];
        let right_values = [Value::Int(2), Value::Int(8)];
        for kind in cases {
            let mut left = AggregateState::default();
            let mut right = AggregateState::default();
            let mut oracle = AggregateState::default();
            for value in &left_values {
                left.update_ref(kind, value).unwrap();
                oracle.update_ref(kind, value).unwrap();
            }
            for value in &right_values {
                right.update_ref(kind, value).unwrap();
                oracle.update_ref(kind, value).unwrap();
            }
            let mut partial = left.into_partial();
            partial.merge_from(&right.into_partial(), kind);
            assert_eq!(
                partial.finalize(kind),
                oracle.finalize(kind),
                "partial merge mismatch for {kind:?}"
            );
        }
    }

    #[test]
    fn state_arena_is_slot_major_and_exports_common_partials() {
        let specs = vec![
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
        ];
        let mut arena = AggregateStateArena::with_capacity(&specs, 4);
        assert_eq!(arena.add_group(), 0);
        assert_eq!(arena.add_group(), 1);
        arena.state_mut(0, 0).unwrap().update_count_star_repeated(3);
        arena
            .state_mut(0, 1)
            .unwrap()
            .update_ref(AggregateKind::Sum, &Value::Int(7))
            .unwrap();
        arena.state_mut(1, 0).unwrap().update_count_star_repeated(1);
        let first = arena.take_group_partials(0).unwrap();
        assert_eq!(first[0].finalize(AggregateKind::CountStar), Value::Int(3));
        assert_eq!(
            first[1].finalize(AggregateKind::Sum),
            Value::DecimalCompact(7, 0)
        );
        let second = arena.take_group_partials(1).unwrap();
        assert_eq!(second[0].finalize(AggregateKind::CountStar), Value::Int(1));
        assert_eq!(second[1].finalize(AggregateKind::Sum), Value::Null);
    }
}
