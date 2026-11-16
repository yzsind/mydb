//! Fixed calendar/time intervals over exact temporal lanes. Out-of-calendar,
//! zero and incomplete dates retain the ordered scalar diagnostic path.
use super::*;

pub(super) struct TemporalShift {
    column: usize,
    input: ColumnType,
    delta: i64,
    date_result: bool,
}
fn interval_unit(node: &ExprNode) -> Option<(i64, bool)> {
    let parse = |text: &str| {
        if text.eq_ignore_ascii_case("DAY") {
            Some((86_400_000_000, true))
        } else if text.eq_ignore_ascii_case("WEEK") {
            Some((604_800_000_000, true))
        } else if text.eq_ignore_ascii_case("HOUR") {
            Some((3_600_000_000, false))
        } else if text.eq_ignore_ascii_case("MINUTE") {
            Some((60_000_000, false))
        } else if text.eq_ignore_ascii_case("SECOND") {
            Some((1_000_000, false))
        } else if text.eq_ignore_ascii_case("MICROSECOND") {
            Some((1, false))
        } else {
            None
        }
    };
    let value = |v: &Value| match v {
        Value::String(text) => parse(text),
        Value::SmallString(text) => parse(std::str::from_utf8(text).ok()?),
        _ => None,
    };
    match node {
        ExprNode::Const(v) => value(v),
        ExprNode::Placeholder(index) => {
            crate::engine::params::with_current_param(*index, |v| value(v?))
        }
        _ => None,
    }
}
impl TemporalShift {
    pub(super) fn bind(node: &ExprNode, schema: &BatchSchema) -> Option<Self> {
        let ExprNode::Function {
            kernel,
            args,
            result_coercion: None,
            ..
        } = node
        else {
            return None;
        };
        if !matches!(kernel, FunctionKernel::DateAdd | FunctionKernel::DateSub) || args.len() != 2 {
            return None;
        }
        let ExprNode::Column(column) = args[0] else {
            return None;
        };
        let input = schema.fields().get(column)?.column_type;
        if !matches!(
            input,
            ColumnType::Date | ColumnType::DateTime | ColumnType::Timestamp
        ) {
            return None;
        }
        let (amount, multiplier, calendar) = match &args[1] {
            ExprNode::Function {
                kernel: FunctionKernel::IntervalPair,
                args,
                ..
            } if args.len() == 2 => {
                let (multiplier, calendar) = interval_unit(&args[1])?;
                (&args[0], multiplier, calendar)
            }
            amount => (amount, 86_400_000_000, true),
        };
        let delta = integer(amount)?.checked_mul(multiplier)?;
        let delta = if matches!(kernel, FunctionKernel::DateSub) {
            delta.checked_neg()?
        } else {
            delta
        };
        Some(Self {
            column,
            input,
            delta,
            date_result: calendar && input == ColumnType::Date,
        })
    }
    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: Domain::Temporal,
            parallel: false,
        }
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let bytes = batch.num_rows().checked_mul(16)?;
        Some((bytes, bytes.checked_mul(3)?.checked_add(512)?))
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        let input = batch.columns().get(self.column)?;
        let rows = batch.num_rows();
        if matches!(input, ColumnVector::Null { .. }) {
            return Some(ColumnVector::Null { len: rows });
        }
        if !matches!(
            (self.input, input),
            (ColumnType::Date, ColumnVector::Date32 { .. })
                | (ColumnType::DateTime, ColumnVector::DateTime64 { .. })
                | (ColumnType::Timestamp, ColumnVector::Timestamp64 { .. })
        ) {
            return None;
        }
        let mut local = Vec::new();
        if let ColumnVector::Timestamp64 { data, nulls } = input {
            local.resize(rows, 0);
            crate::engine::expr::fill_session_timestamp_local_datetime_micros(
                &mut local,
                data,
                (0..rows).filter(|&r| nulls.as_ref().is_none_or(|mask| mask.get(r))),
            )?;
        }
        let mut values = Vec::with_capacity(rows);
        for row in 0..rows {
            if row & 63 == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            if input.is_null(row) {
                values.push(0);
                continue;
            }
            let raw = match input {
                ColumnVector::Date32 { data, .. } => {
                    let date = data[row];
                    if date == crate::types::ZERO_DATE
                        || crate::types::decode_tagged_date(date).is_some()
                    {
                        return None;
                    }
                    i64::from(date).checked_mul(86_400_000_000)?
                }
                ColumnVector::DateTime64 { data, .. } => data[row],
                ColumnVector::Timestamp64 { .. } => local[row],
                _ => return None,
            };
            if raw == crate::types::ZERO_DATETIME
                || crate::types::decode_tagged_datetime(raw).is_some()
                || !crate::types::datetime_micros_in_calendar_range(raw)
            {
                return None;
            }
            let value = raw.checked_add(self.delta)?;
            if !crate::types::datetime_micros_in_calendar_range(value) {
                return None;
            }
            values.push(if self.date_result {
                value.div_euclid(86_400_000_000)
            } else {
                value
            });
        }
        let nulls = validity(input);
        Some(if self.date_result {
            ColumnVector::Date32 {
                data: values.into_iter().map(|value| value as i32).collect(),
                nulls,
            }
        } else {
            ColumnVector::DateTime64 {
                data: values,
                nulls,
            }
        })
    }
}
