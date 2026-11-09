//! Calendar and epoch arithmetic over validated fixed lanes. Exceptional
//! conversions decline without diagnostics so the scalar oracle retains order.
use super::*;
use chrono::Datelike;

const DAY: i64 = 86_400_000_000;
fn null(input: &Input<'_>, row: usize) -> bool {
    match input {
        Input::Constant(value) => matches!(value.as_ref(), Value::Null),
        Input::Column(column, _) => column.is_null(row),
    }
}
fn integer(input: &Input<'_>, row: usize) -> Option<i128> {
    Some(match input {
        Input::Constant(value) => match value.as_ref() {
            Value::Int(v) => i128::from(*v),
            Value::UInt(v) => i128::from(*v),
            Value::Null => 0,
            _ => return None,
        },
        Input::Column(column, _) => match column.as_ref() {
            ColumnVector::I64 { data, .. } => i128::from(data[row]),
            ColumnVector::U64 { data, .. } => i128::from(data[row]),
            ColumnVector::Null { .. } => 0,
            _ => return None,
        },
    })
}
fn calendar<'a>(input: &'a Input<'_>, rows: usize) -> Option<Cow<'a, [i64]>> {
    if let Input::Constant(value) = input {
        let sink = crate::engine::api::StatementWarningSink::default();
        let parsed = crate::engine::api::with_statement_warning_sink(sink.clone(), || {
            crate::engine::expr::parse_datetime(value.as_ref())
        })
        .ok()?;
        if !sink.lock().is_empty() || !(0..=9999).contains(&parsed.year()) {
            return None;
        }
        return Some(Cow::Owned(vec![parsed.and_utc().timestamp_micros(); rows]));
    }
    let (data, time, _) = fast::input_data(input)?;
    (!time).then_some(data)
}
fn temporal_kind(input: &Input<'_>) -> Option<u8> {
    let Input::Column(column, _) = input else {
        return None;
    };
    Some(match column.as_ref() {
        ColumnVector::Date32 { .. } => 0,
        ColumnVector::DateTime64 { .. } | ColumnVector::Timestamp64 { .. } => 1,
        ColumnVector::Time64 { .. } => 2,
        _ => return None,
    })
}
fn output_nulls(inputs: &[Input<'_>], rows: usize) -> Option<Bitmap> {
    let mut mask = Bitmap::new_all_valid(rows);
    let mut any = false;
    for row in 0..rows {
        if inputs.iter().any(|input| null(input, row)) {
            mask.set(row, false);
            any = true;
        }
    }
    any.then_some(mask)
}
fn visible(mask: &Option<Bitmap>, row: usize) -> bool {
    mask.as_ref().is_none_or(|mask| mask.get(row))
}
fn linear_unit(unit: &str) -> Option<i64> {
    Some(match unit {
        "MICROSECOND" | "MICROSECONDS" => 1,
        "SECOND" | "SECONDS" => 1_000_000,
        "MINUTE" | "MINUTES" => 60_000_000,
        "HOUR" | "HOURS" => 3_600_000_000,
        "DAY" | "DAYS" => DAY,
        "WEEK" | "WEEKS" => 7 * DAY,
        _ => return None,
    })
}
fn check(row: usize) -> Option<()> {
    if row & 63 == 0 {
        crate::engine::cancel::check_cancelled().ok()?;
    }
    Some(())
}

pub(super) fn evaluate(
    read: &TemporalValue<'_>,
    inputs: &[Input<'_>],
    batch: &Batch,
) -> Option<ColumnVector> {
    let rows = batch.num_rows();
    if read.name.eq_ignore_ascii_case("FROM_UNIXTIME") && inputs.len() == 1 {
        let mut raw = Vec::with_capacity(rows);
        let mut nulls = Bitmap::new_all_valid(rows);
        let mut any_null = false;
        for row in 0..rows {
            check(row)?;
            let seconds = integer(&inputs[0], row)?;
            let micros = seconds.checked_mul(1_000_000)?;
            if null(&inputs[0], row)
                || seconds < 0
                || micros > i128::from(crate::types::MAX_DATETIME_MICROS)
            {
                nulls.set(row, false);
                any_null = true;
                raw.push(0);
            } else {
                raw.push(i64::try_from(micros).ok()?);
            }
        }
        let mut local = vec![0; rows];
        crate::engine::expr::fill_session_timestamp_local_datetime_micros(
            &mut local,
            &raw,
            (0..rows).filter(|&row| nulls.get(row)),
        )?;
        return Some(ColumnVector::DateTime64 {
            data: local,
            nulls: any_null.then_some(nulls),
        });
    }
    if read.name.eq_ignore_ascii_case("UNIX_TIMESTAMP") {
        let Input::Column(column, _) = &inputs[0] else {
            return None;
        };
        let timestamp = matches!(column.as_ref(), ColumnVector::Timestamp64 { .. });
        let zones = if timestamp {
            None
        } else {
            Some(crate::engine::expr::TimeZonePair::session_to_utc().ok()?)
        };
        let source = if let ColumnVector::Timestamp64 { data, .. } = column.as_ref() {
            Cow::Borrowed(data.as_slice())
        } else {
            calendar(&inputs[0], rows)?
        };
        let mut output = Vec::with_capacity(rows);
        for (row, &value) in source.iter().enumerate() {
            check(row)?;
            if null(&inputs[0], row) {
                output.push(Value::Null);
                continue;
            }
            if timestamp && value == crate::types::ZERO_DATETIME {
                output.push(Value::Int(0));
                continue;
            }
            let seconds = value.div_euclid(1_000_000);
            let epoch = zones
                .as_ref()
                .map_or(seconds, |zone| zone.utc_from_local(seconds));
            if epoch < 0 || (!timestamp && epoch > crate::types::MAX_TIMESTAMP_SECONDS) {
                output.push(Value::Int(0));
                continue;
            }
            let fraction = value.rem_euclid(1_000_000);
            output.push(if fraction == 0 {
                Value::Int(epoch)
            } else {
                Value::DecimalCompact(epoch.checked_mul(1_000_000)?.checked_add(fraction)?, 6)
            });
        }
        // Integer zero/whole seconds and six-place fractional decimals retain
        // the scalar oracle's distinct wire representations within one lane.
        return Some(ColumnVector::Generic { values: output });
    }
    if read.name.eq_ignore_ascii_case("TIMEDIFF") {
        if let (Some(a), Some(b)) = (temporal_kind(&inputs[0]), temporal_kind(&inputs[1])) {
            if a != b {
                return Some(ColumnVector::Null { len: rows });
            }
            let (a, _, _) = fast::input_data(&inputs[0])?;
            let (b, _, _) = fast::input_data(&inputs[1])?;
            let nulls = output_nulls(inputs, rows);
            let mut data = Vec::with_capacity(rows);
            for row in 0..rows {
                check(row)?;
                data.push(if visible(&nulls, row) {
                    a[row]
                        .saturating_sub(b[row])
                        .clamp(-3_020_399_000_000, 3_020_399_000_000)
                } else {
                    0
                });
            }
            return Some(ColumnVector::Time64 { data, nulls });
        }
    }
    let datediff = read.name.eq_ignore_ascii_case("DATEDIFF");
    let diff = read.name.eq_ignore_ascii_case("TIMESTAMPDIFF");
    let add = read.name.eq_ignore_ascii_case("TIMESTAMPADD");
    if !datediff && !diff && !add {
        return None;
    }
    let unit = if datediff {
        String::new()
    } else {
        crate::engine::expr::interval_unit_from_arg(read.syntax?.first()?)
    };
    let quantum = if datediff { DAY } else { linear_unit(&unit)? };
    let (left, right) = if datediff {
        (&inputs[0], &inputs[1])
    } else if diff {
        (&inputs[1], &inputs[2])
    } else {
        (&inputs[2], &inputs[2])
    };
    let a = calendar(left, rows)?;
    let b = if add {
        Cow::Borrowed(&[][..])
    } else {
        calendar(right, rows)?
    };
    let effective_inputs = if datediff { inputs } else { &inputs[1..] };
    let nulls = output_nulls(effective_inputs, rows);
    if datediff {
        // DATEDIFF rejects year zero before parsing either operand, including
        // a TIMESTAMP whose UTC year differs from its localized year.
        for input in [left, right] {
            if let Input::Column(column, _) = input {
                if let ColumnVector::Timestamp64 { data, .. } = column.as_ref() {
                    if data
                        .iter()
                        .enumerate()
                        .any(|(row, &v)| !column.is_null(row) && v.div_euclid(DAY) < -719_162)
                    {
                        return None;
                    }
                }
            }
        }
        if (0..rows).any(|row| {
            visible(&nulls, row)
                && (a[row].div_euclid(DAY) < -719_162 || b[row].div_euclid(DAY) < -719_162)
        }) {
            return None;
        }
    }
    let date_input = temporal_kind(left) == Some(0)
        || match left {
            Input::Constant(value) => {
                matches!(value.as_ref(), Value::Date(_))
                    || crate::types::value_str(value.as_ref())
                        .and_then(crate::util::temporal::parse_delimited_datetime_components)
                        .is_some_and(|parts| !parts.is_datetime)
            }
            _ => false,
        };
    let typed_add = add
        && (temporal_kind(left).is_some()
            || matches!(left, Input::Constant(value) if matches!(value.as_ref(), Value::Date(_) | Value::DateTime(_) | Value::Timestamp(_))));
    let date_output = matches!(unit.as_str(), "DAY" | "WEEK") && date_input;
    let mut numbers = Vec::with_capacity(if add && !typed_add { 0 } else { rows });
    let mut data = Vec::new();
    let mut offsets = Vec::new();
    if add && !typed_add {
        data.reserve(rows.checked_mul(27)?);
        offsets.reserve(rows + 1);
        offsets.push(0);
    }
    for row in 0..rows {
        check(row)?;
        if add {
            if typed_add {
                let value = if visible(&nulls, row) {
                    let amount = i64::try_from(integer(&inputs[1], row)?).ok()?;
                    let value = a[row].checked_add(amount.checked_mul(quantum)?)?;
                    if !crate::types::datetime_micros_in_calendar_range(value) {
                        return None;
                    }
                    value
                } else {
                    0
                };
                numbers.push(value);
                continue;
            }
            if visible(&nulls, row) {
                let amount = integer(&inputs[1], row)? as i64;
                let value = a[row].checked_add(amount.checked_mul(quantum)?)?;
                if !crate::types::datetime_micros_in_calendar_range(value) {
                    return None;
                }
                let date = matches!(unit.as_str(), "DAY" | "WEEK") && date_input;
                let mut buffer = [0; 32];
                if date {
                    data.extend_from_slice(crate::types::write_date_canonical(
                        value.div_euclid(DAY) as i32,
                        &mut buffer,
                    ));
                } else {
                    data.extend_from_slice(crate::types::write_datetime_canonical_with_fsp(
                        value,
                        Some(if value.rem_euclid(1_000_000) == 0 {
                            0
                        } else {
                            6
                        }),
                        &mut buffer,
                    ));
                }
            }
            offsets.push(u32::try_from(data.len()).ok()?);
        } else {
            numbers.push(if !visible(&nulls, row) {
                0
            } else if datediff {
                a[row].div_euclid(DAY) - b[row].div_euclid(DAY)
            } else {
                b[row].checked_sub(a[row])? / quantum
            });
        }
    }
    Some(if typed_add && date_output {
        ColumnVector::Date32 {
            data: numbers
                .into_iter()
                .map(|value| value.div_euclid(DAY) as i32)
                .collect(),
            nulls,
        }
    } else if typed_add {
        ColumnVector::DateTime64 {
            data: numbers,
            nulls,
        }
    } else if add {
        ColumnVector::Utf8 {
            data,
            offsets,
            nulls,
        }
    } else {
        ColumnVector::I64 {
            data: numbers,
            nulls,
        }
    })
}
