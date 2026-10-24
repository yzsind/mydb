//! Partial-state wire layout. Compile once per spill operation, never per row.
//! The same codec describes schema, offsets, writes, and replay. Typed arenas
//! write directly into these columns without allocating canonical snapshots.
use super::*;

#[derive(Clone, Copy, Debug)]
pub(super) enum PartialCodec {
    Count,
    Numeric,
    Min,
    Max,
    Bits,
    Variance,
    Distinct,
}

#[derive(Clone, Copy)]
enum PartialColumn {
    Count,
    Total,
    Value,
    Bits,
    N,
    Mean,
    M2,
    Distinct,
}

impl PartialCodec {
    pub(super) fn for_kind(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::CountStar | AggregateKind::Count => Self::Count,
            AggregateKind::Sum | AggregateKind::Avg => Self::Numeric,
            AggregateKind::Min => Self::Min,
            AggregateKind::Max => Self::Max,
            AggregateKind::BitAnd | AggregateKind::BitOr | AggregateKind::BitXor => Self::Bits,
            AggregateKind::VarPop
            | AggregateKind::VarSamp
            | AggregateKind::StddevPop
            | AggregateKind::StddevSamp => Self::Variance,
            AggregateKind::CountDistinct
            | AggregateKind::SumDistinct
            | AggregateKind::AvgDistinct => Self::Distinct,
        }
    }

    fn columns(self) -> &'static [PartialColumn] {
        use PartialColumn::*;
        match self {
            Self::Count => &[Count],
            Self::Numeric => &[Total, Count],
            Self::Min | Self::Max => &[Value],
            Self::Bits => &[Bits],
            Self::Variance => &[N, Mean, M2],
            Self::Distinct => &[Distinct],
        }
    }

    pub(super) fn width(self) -> usize {
        self.columns().len()
    }

    fn write(self, state: &AggState, columns: &mut [Vec<Value>]) {
        match self {
            Self::Count => columns[0].push(Value::Int(state.count as i64)),
            Self::Numeric => {
                columns[0].push(numeric_state_to_value(&state.sum));
                columns[1].push(Value::Int(state.count as i64));
            }
            Self::Min => columns[0].push(state.min.clone().unwrap_or(Value::Null)),
            Self::Max => columns[0].push(state.max.clone().unwrap_or(Value::Null)),
            Self::Bits => columns[0].push(match &state.extra {
                Some(extra) if extra.bits_seen => Value::UInt(extra.bits),
                _ => Value::Null,
            }),
            Self::Variance => {
                let (n, mean, m2) = state
                    .extra
                    .as_ref()
                    .map_or((0, 0.0, 0.0), |e| (e.var_n, e.var_mean, e.var_m2));
                columns[0].push(Value::Int(n as i64));
                columns[1].push(Value::Double(mean));
                columns[2].push(Value::Double(m2));
            }
            Self::Distinct => columns[0].push(encode_distinct_blob(state.distinct.as_deref())),
        }
    }

    fn merge(
        self,
        state: &mut AggState,
        func: AggregateKind,
        columns: &[ColumnVector],
        row: usize,
    ) -> Result<(), QueryError> {
        match self {
            Self::Count => {
                if let Value::Int(n) = columns[0].value_at(row) {
                    state.count = state.count.saturating_add(n.max(0) as u64);
                }
            }
            Self::Numeric => {
                accumulate_numeric_state(&mut state.sum, &columns[0].value_at(row))?;
                if let Value::Int(n) = columns[1].value_at(row) {
                    state.count = state.count.saturating_add(n.max(0) as u64);
                }
            }
            Self::Min | Self::Max => {
                let value = columns[0].value_at(row);
                if !matches!(value, Value::Null) {
                    let (best, replace_order) = match self {
                        Self::Min => (&mut state.min, std::cmp::Ordering::Greater),
                        Self::Max => (&mut state.max, std::cmp::Ordering::Less),
                        _ => unreachable!(),
                    };
                    if best.as_ref().is_none_or(|old| {
                        crate::engine::expr::compare_values(old, &value)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            == replace_order
                    }) {
                        *best = Some(value);
                    }
                }
            }
            Self::Bits => {
                let value = columns[0].value_at(row);
                if !matches!(value, Value::Null) {
                    update_extra_with_value(state, func, &value)?;
                }
            }
            Self::Variance => {
                let n = match columns[0].value_at(row) {
                    Value::Int(n) => n.max(0) as u64,
                    _ => 0,
                };
                let mean = numeric_or_zero(&columns[1].value_at(row));
                let m2 = numeric_or_zero(&columns[2].value_at(row));
                variance_merge(state.extra_mut(), n, mean, m2);
            }
            Self::Distinct => decode_distinct_column_into(state, &columns[0], row)?,
        }
        Ok(())
    }
}

pub(super) struct SpillSlot {
    pub(super) offset: usize,
    codec: PartialCodec,
}

pub(super) struct SpillLayout {
    pub(super) group_count: usize,
    pub(super) slots: Vec<SpillSlot>,
    pub(super) width: usize,
}

impl SpillLayout {
    pub(super) fn new(group_count: usize, aggs: &[AggregateSpec]) -> Self {
        let mut width = group_count;
        let slots = aggs
            .iter()
            .map(|spec| {
                let codec = PartialCodec::for_kind(spec.func);
                let offset = width;
                width += codec.width();
                SpillSlot { offset, codec }
            })
            .collect();
        Self {
            group_count,
            slots,
            width,
        }
    }

    pub(super) fn for_schema(
        schema: &BatchSchema,
        aggs: &[AggregateSpec],
    ) -> Result<Self, QueryError> {
        let group_count = schema
            .len()
            .checked_sub(agg_state_col_count(aggs))
            .ok_or_else(|| {
                crate::storage::error(
                    crate::error::catalog::ER_INTERNAL_ERROR,
                    "hash aggregate spill schema is shorter than its partial states",
                )
            })?;
        Ok(Self::new(group_count, aggs))
    }

    pub(super) fn validate_batch(&self, batch: &Batch) -> Result<(), QueryError> {
        if batch.columns().len() != self.width {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "hash aggregate spill column count mismatch",
            ));
        }
        Ok(())
    }
}

impl SpillSlot {
    pub(super) fn write(&self, state: &AggState, columns: &mut [Vec<Value>]) {
        self.codec.write(
            state,
            &mut columns[self.offset..self.offset + self.codec.width()],
        );
    }

    pub(super) fn merge(
        &self,
        state: &mut AggState,
        func: AggregateKind,
        columns: &[ColumnVector],
        row: usize,
    ) -> Result<(), QueryError> {
        self.codec.merge(
            state,
            func,
            &columns[self.offset..self.offset + self.codec.width()],
            row,
        )
    }

    pub(super) fn merge_fixed(
        &self,
        state: &mut InlineAggState,
        columns: &[ColumnVector],
        row: usize,
    ) -> Result<(), QueryError> {
        let columns = &columns[self.offset..self.offset + self.codec.width()];
        match (self.codec, state) {
            (PartialCodec::Count, InlineAggState::Count(count)) => {
                if let Value::Int(n) = columns[0].value_at(row) {
                    *count += n.max(0);
                }
            }
            (PartialCodec::Numeric, InlineAggState::Sum(sum)) => {
                let value = columns[0].value_at(row);
                if !matches!(value, Value::Null) {
                    accumulate_numeric_state(sum, &value)?;
                }
            }
            (PartialCodec::Numeric, InlineAggState::Avg { state, n }) => {
                let value = columns[0].value_at(row);
                if !matches!(value, Value::Null) {
                    accumulate_numeric_state(state, &value)?;
                }
                if let Value::Int(count) = columns[1].value_at(row) {
                    *n = n.saturating_add(count.max(0) as u64);
                }
            }
            (PartialCodec::Min | PartialCodec::Max, state @ InlineAggState::MinMax { .. }) => {
                let value = columns[0].value_at(row);
                if !matches!(value, Value::Null) {
                    state.update_ref(&value)?;
                }
            }
            _ => unreachable!("fixed width state matches its spill codec"),
        }
        Ok(())
    }

    pub(super) fn write_fixed(
        &self,
        columns: &mut [Vec<Value>],
        arena: &FixedWidthStateArena,
        group: usize,
        slot: usize,
    ) {
        let columns = &mut columns[self.offset..self.offset + self.codec.width()];
        if let Some(decimal) = &arena.decimal_state_columns[slot] {
            debug_assert!(matches!(self.codec, PartialCodec::Numeric));
            let sum = decimal.state(group);
            columns[0].push(numeric_state_to_value(&sum));
            columns[1].push(Value::Int(i64::from(!matches!(
                sum,
                NumericAggState::Empty
            ))));
            return;
        }
        match (self.codec, arena.state(group, slot)) {
            (PartialCodec::Count, InlineAggState::Count(n)) => columns[0].push(Value::Int(*n)),
            (PartialCodec::Numeric, InlineAggState::Sum(sum)) => {
                columns[0].push(numeric_state_to_value(sum));
                columns[1].push(Value::Int(i64::from(!matches!(
                    sum,
                    NumericAggState::Empty
                ))));
            }
            (PartialCodec::Numeric, InlineAggState::Avg { state, n }) => {
                columns[0].push(numeric_state_to_value(state));
                columns[1].push(Value::Int(*n as i64));
            }
            (PartialCodec::Min | PartialCodec::Max, InlineAggState::MinMax { best, .. }) => {
                columns[0].push(best.clone().unwrap_or(Value::Null))
            }
            _ => unreachable!("fixed width state matches its spill codec"),
        }
    }
}

pub(super) fn build_spill_schema(
    input: &BatchSchema,
    group_cols: &[usize],
    aggs: &[AggregateSpec],
) -> BatchSchema {
    let layout = SpillLayout::new(group_cols.len(), aggs);
    let mut fields = Vec::with_capacity(layout.width);
    fields.extend(group_cols.iter().map(|&g| input.fields()[g].clone()));
    for (index, (slot, spec)) in layout.slots.iter().zip(aggs).enumerate() {
        for column in slot.codec.columns() {
            let (name, column_type) = match column {
                PartialColumn::Count => ("count", ColumnType::LongLong),
                PartialColumn::Total => ("total", ColumnType::Double),
                PartialColumn::Value => (
                    "v",
                    input
                        .fields()
                        .get(spec.input_column)
                        .map_or(ColumnType::VarString, |f| f.column_type),
                ),
                PartialColumn::Bits => ("bits", ColumnType::LongLong),
                PartialColumn::N => ("n", ColumnType::LongLong),
                PartialColumn::Mean => ("mean", ColumnType::Double),
                PartialColumn::M2 => ("m2", ColumnType::Double),
                PartialColumn::Distinct => ("distinct", ColumnType::Blob),
            };
            let source = if matches!(column, PartialColumn::Value) {
                input
                    .fields()
                    .get(spec.input_column)
                    .map(|f| f.source.clone())
                    .unwrap_or_default()
            } else {
                Default::default()
            };
            fields.push(BatchField {
                table: "__agg".into(),
                alias: None,
                name: format!("a{index}_{name}"),
                column_type,
                source,
            });
        }
    }
    BatchSchema::new(fields)
}

pub(super) fn agg_state_col_count(aggs: &[AggregateSpec]) -> usize {
    aggs.iter()
        .map(|s| PartialCodec::for_kind(s.func).width())
        .sum()
}
