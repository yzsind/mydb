//! Fixed temporal lanes retain exact microseconds and render into one buffer.
use super::*;

fn text_input<'a>(input: &'a Input<'_>) -> Option<&'a str> {
    let Input::Constant(value) = input else {
        return None;
    };
    crate::types::value_str(value.as_ref())
}
pub(super) fn precision(micros: i64) -> u8 {
    let mut fraction = micros.rem_euclid(1_000_000);
    if fraction == 0 {
        return 0;
    }
    let mut digits = 6;
    while fraction % 10 == 0 {
        fraction /= 10;
        digits -= 1;
    }
    digits
}
pub(super) fn input_data<'a>(
    input: &'a Input<'_>,
) -> Option<(Cow<'a, [i64]>, bool, Option<Bitmap>)> {
    let Input::Column(column, _) = input else {
        return None;
    };
    let nulls = validity(column);
    let valid = |row: usize| !column.is_null(row);
    let (data, time) = match column.as_ref() {
        ColumnVector::DateTime64 { data, .. } => (Cow::Borrowed(data.as_slice()), false),
        ColumnVector::Time64 { data, .. } => (Cow::Borrowed(data.as_slice()), true),
        ColumnVector::Date32 { data, .. } => {
            let mut output = Vec::with_capacity(data.len());
            for (row, &value) in data.iter().enumerate() {
                if valid(row)
                    && (value == crate::types::ZERO_DATE
                        || crate::types::decode_tagged_date(value).is_some())
                {
                    return None;
                }
                output.push(i64::from(value) * 86_400_000_000);
            }
            (Cow::Owned(output), false)
        }
        ColumnVector::Timestamp64 { data, .. } => {
            let mut output = vec![0; data.len()];
            crate::engine::expr::fill_session_timestamp_local_datetime_micros(
                &mut output,
                data,
                (0..data.len()).filter(|&row| valid(row)),
            )?;
            (Cow::Owned(output), false)
        }
        _ => return None,
    };
    if !time
        && data.iter().enumerate().any(|(row, &value)| {
            valid(row)
                && (value == crate::types::ZERO_DATETIME
                    || crate::types::decode_tagged_datetime(value).is_some()
                    || !crate::types::datetime_micros_in_calendar_range(value))
        })
    {
        return None;
    }
    Some((data, time, nulls))
}

pub(super) fn evaluate(
    read: &TemporalValue<'_>,
    inputs: &[Input<'_>],
    batch: &Batch,
) -> Option<ColumnVector> {
    if read.name.eq_ignore_ascii_case("TIME_FORMAT") {
        let Input::Column(column, _) = &inputs[0] else {
            return None;
        };
        let (data, time, _) = input_data(&inputs[0])?;
        if time
            && data
                .iter()
                .enumerate()
                .any(|(row, v)| !column.is_null(row) && v.unsigned_abs() > 3_020_399_000_000)
        {
            return None;
        }
        let format = crate::engine::expr::date_format::DateFormatPlan::new(text_input(&inputs[1])?);
        if !format.supports_time() {
            return Some(ColumnVector::Null {
                len: batch.num_rows(),
            });
        }
        let mut output = Vec::with_capacity(batch.num_rows().checked_mul(read.width)?);
        let mut offsets = Vec::with_capacity(batch.num_rows() + 1);
        let mut cell = String::with_capacity(read.width);
        offsets.push(0);
        for (row, &value) in data.iter().enumerate() {
            if row & 63 == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            if !column.is_null(row) {
                cell.clear();
                format.append_time(
                    if time {
                        value
                    } else {
                        value.rem_euclid(86_400_000_000)
                    },
                    &mut cell,
                );
                output.extend_from_slice(cell.as_bytes());
            }
            offsets.push(u32::try_from(output.len()).ok()?);
        }
        return Some(ColumnVector::Utf8 {
            data: output,
            offsets,
            nulls: validity(column),
        });
    }
    let add =
        read.name.eq_ignore_ascii_case("ADDTIME") || read.name.eq_ignore_ascii_case("SUBTIME");
    let convert = read.name.eq_ignore_ascii_case("CONVERT_TZ");
    if !add && !convert {
        return None;
    }
    // A bound value/parameter has a distinct prepare-time TIME coercion in
    // ADDTIME. Preserve that syntax-sensitive conversion on the scalar path.
    if add
        && read.syntax.is_some_and(|args| {
            matches!(
                args.first(),
                Some(Expr::Placeholder(_) | Expr::BoundValue(_))
            )
        })
    {
        return None;
    }
    if let Input::Column(column, _) = &inputs[0] {
        if matches!(column.as_ref(), ColumnVector::Null { .. }) {
            return Some(ColumnVector::Null {
                len: batch.num_rows(),
            });
        }
    }
    let (source, time, nulls) = input_data(&inputs[0])?;
    let is_null = |row: usize| nulls.as_ref().is_some_and(|mask| !mask.get(row));
    let (delta, delta_fsp, zones) = if add {
        if let Input::Constant(value) = &inputs[1] {
            if crate::engine::expr::is_zero_datetime_value(value.as_ref()) {
                return None;
            }
        }
        let text = text_input(&inputs[1])?;
        let (negative, hour, minute, second, micros) =
            crate::engine::expr::parse_time_components(text).ok()?;
        let delta = i64::from(hour)
            .checked_mul(3_600_000_000)?
            .checked_add(i64::from(minute) * 60_000_000)?
            .checked_add(i64::from(second) * 1_000_000 + i64::from(micros))?;
        let delta = if negative {
            delta.checked_neg()?
        } else {
            delta
        };
        let delta = if read.name.eq_ignore_ascii_case("SUBTIME") {
            delta.checked_neg()?
        } else {
            delta
        };
        let fsp = text
            .rsplit_once('.')
            .filter(|(_, fraction)| {
                !fraction.is_empty() && fraction.bytes().all(|byte| byte.is_ascii_digit())
            })
            .map_or(0, |(_, fraction)| fraction.len().min(6) as u8);
        (delta, fsp, None)
    } else {
        if time {
            return None;
        }
        (
            0,
            0,
            Some(
                crate::engine::expr::TimeZonePair::resolve(
                    text_input(&inputs[1])?,
                    text_input(&inputs[2])?,
                )
                .ok()?,
            ),
        )
    };
    let mut times = Vec::new();
    let mut offsets = Vec::new();
    let mut data = Vec::new();
    if time {
        times.reserve(batch.num_rows());
    } else {
        offsets.reserve(batch.num_rows() + 1);
        offsets.push(0);
        data.reserve(batch.num_rows().checked_mul(27)?);
    }
    const LIMIT: i64 = (838 * 3600 + 59 * 60 + 59) * 1_000_000;
    for (row, &value) in source.iter().enumerate() {
        if row & 63 == 0 {
            crate::engine::cancel::check_cancelled().ok()?;
        }
        if is_null(row) {
            if time {
                times.push(0);
            } else {
                offsets.push(u32::try_from(data.len()).ok()?);
            }
            continue;
        }
        if time {
            if value.unsigned_abs() > (LIMIT + 999_999) as u64 {
                return None;
            }
            let result = value.saturating_add(delta);
            if result.unsigned_abs() > (LIMIT + 999_999) as u64 {
                return None;
            }
            times.push(result.clamp(-LIMIT, LIMIT));
        } else {
            let result = if let Some(zones) = &zones {
                let utc = zones.utc_from_local(value.div_euclid(1_000_000));
                if utc < 0 {
                    value
                } else {
                    if utc > crate::types::MAX_TIMESTAMP_SECONDS {
                        return None;
                    }
                    zones
                        .target_from_utc(utc)
                        .checked_mul(1_000_000)?
                        .checked_add(value.rem_euclid(1_000_000))?
                }
            } else {
                value.checked_add(delta)?
            };
            if !crate::types::datetime_micros_in_calendar_range(result) {
                return None;
            }
            let mut buffer = [0; 32];
            let bytes = crate::types::write_datetime_canonical_with_fsp(
                result,
                Some(precision(value).max(delta_fsp)),
                &mut buffer,
            );
            data.extend_from_slice(bytes);
            offsets.push(u32::try_from(data.len()).ok()?);
        }
    }
    Some(if time {
        ColumnVector::Time64 { data: times, nulls }
    } else {
        ColumnVector::Utf8 {
            offsets,
            data,
            nulls,
        }
    })
}
