//! Exact integer temporal operands avoid per-row Value and argument vectors.
use super::*;
use chrono::Datelike;

enum Lane<'a> {
    Signed(&'a [i64]),
    Unsigned(&'a [u64]),
    Constant(i64, f64),
    Null,
}
impl Lane<'_> {
    fn integer(&self, row: usize) -> i64 {
        match self {
            Self::Signed(v) => v[row],
            Self::Unsigned(v) => v[row] as i64,
            Self::Constant(v, _) => *v,
            Self::Null => 0,
        }
    }
    fn number(&self, row: usize) -> f64 {
        match self {
            Self::Signed(v) => v[row] as f64,
            Self::Unsigned(v) => v[row] as f64,
            Self::Constant(_, v) => *v,
            Self::Null => 0.0,
        }
    }
}

pub(super) fn evaluate(
    read: &TemporalValue<'_>,
    inputs: &[Input<'_>],
    batch: &Batch,
) -> Option<ColumnVector> {
    let name = read.name.to_ascii_uppercase();
    if !matches!(
        name.as_str(),
        "FROM_DAYS" | "MAKEDATE" | "MAKETIME" | "SEC_TO_TIME" | "PERIOD_ADD" | "PERIOD_DIFF"
    ) {
        return None;
    }
    let mut lanes = Vec::with_capacity(inputs.len());
    for input in inputs {
        lanes.push(match input {
            Input::Constant(value) => match value.as_ref() {
                Value::Int(v) => Lane::Constant(*v, *v as f64),
                Value::UInt(v) => Lane::Constant(*v as i64, *v as f64),
                Value::Null => Lane::Null,
                _ => return None,
            },
            Input::Column(column, _) => match column.as_ref() {
                ColumnVector::I64 { data, .. } => Lane::Signed(data),
                ColumnVector::U64 { data, .. } => Lane::Unsigned(data),
                ColumnVector::Null { .. } => Lane::Null,
                _ => return None,
            },
        });
    }
    let text = matches!(name.as_str(), "FROM_DAYS" | "MAKEDATE");
    let mut numbers = Vec::new();
    let mut data = Vec::new();
    let mut offsets = Vec::new();
    if text {
        data.reserve(batch.num_rows().checked_mul(10)?);
        offsets.reserve(batch.num_rows() + 1);
        offsets.push(0);
    } else {
        numbers.reserve(batch.num_rows());
    }
    let mut nulls = Bitmap::new_all_valid(batch.num_rows());
    let mut any_null = false;
    const LIMIT: i128 = 3_020_399_000_000;
    for row in 0..batch.num_rows() {
        if row & 63 == 0 {
            crate::engine::cancel::check_cancelled().ok()?;
        }
        let null = inputs.iter().any(|input| match input {
            Input::Constant(value) => matches!(value.as_ref(), Value::Null),
            Input::Column(column, _) => column.is_null(row),
        });
        let result = if null {
            None
        } else {
            let first = lanes[0].integer(row);
            match name.as_str() {
                "FROM_DAYS" => {
                    if first <= 0 {
                        data.extend_from_slice(b"0000-00-00");
                    } else {
                        let ce =
                            i32::try_from(first.checked_sub(if first < 60 { 366 } else { 365 })?)
                                .ok()?;
                        let date = chrono::NaiveDate::from_num_days_from_ce_opt(ce)?;
                        if date.year() > 9999 {
                            return None;
                        }
                        let mut buffer = [0; 32];
                        data.extend_from_slice(crate::types::write_date_canonical(
                            ce - 719_163,
                            &mut buffer,
                        ));
                    }
                    Some(0)
                }
                "MAKEDATE" => {
                    let day = lanes[1].integer(row);
                    if day < 1 || !(0..=9999).contains(&first) {
                        None
                    } else {
                        let year = if first < 100 {
                            first + if first < 70 { 2000 } else { 1900 }
                        } else {
                            first
                        };
                        let date = chrono::NaiveDate::from_ymd_opt(year as i32, 1, 1)?
                            .checked_add_signed(chrono::Duration::try_days(day - 1)?)?;
                        if date.year() > 9999 {
                            return None;
                        }
                        let mut buffer = [0; 32];
                        data.extend_from_slice(crate::types::write_date_canonical(
                            date.num_days_from_ce() - 719_163,
                            &mut buffer,
                        ));
                        Some(0)
                    }
                }
                "MAKETIME" => {
                    let minute = lanes[1].integer(row);
                    let second = lanes[2].number(row);
                    if !(0..=59).contains(&minute) || !(0.0..60.0).contains(&second) {
                        None
                    } else {
                        let magnitude = ((i128::from(first)).abs() * 3600
                            + i128::from(minute) * 60)
                            * 1_000_000
                            + (second * 1_000_000.0).round() as i128;
                        Some(
                            (if first < 0 { -magnitude } else { magnitude }).clamp(-LIMIT, LIMIT)
                                as i64,
                        )
                    }
                }
                "SEC_TO_TIME" => Some(
                    (lanes[0].number(row) * 1_000_000.0)
                        .round()
                        .clamp(-(LIMIT as f64), LIMIT as f64) as i64,
                ),
                "PERIOD_ADD" if first == 0 => Some(0),
                "PERIOD_ADD" => {
                    let (year, month) =
                        crate::engine::expr::checked_period_to_ym(first, "period_add").ok()?;
                    let total = (i64::from(year) * 12 + i64::from(month) - 1)
                        .checked_add(lanes[1].integer(row))?;
                    Some(
                        total
                            .div_euclid(12)
                            .checked_mul(100)?
                            .checked_add(total.rem_euclid(12) + 1)?,
                    )
                }
                "PERIOD_DIFF" => {
                    let (y1, m1) =
                        crate::engine::expr::checked_period_to_ym(first, "period_diff").ok()?;
                    let (y2, m2) = crate::engine::expr::checked_period_to_ym(
                        lanes[1].integer(row),
                        "period_diff",
                    )
                    .ok()?;
                    Some(
                        (i64::from(y1) * 12 + i64::from(m1)) - (i64::from(y2) * 12 + i64::from(m2)),
                    )
                }
                _ => unreachable!(),
            }
        };
        if result.is_none() {
            nulls.set(row, false);
            any_null = true;
        }
        if text {
            offsets.push(u32::try_from(data.len()).ok()?);
        } else {
            numbers.push(result.unwrap_or(0));
        }
    }
    let nulls = any_null.then_some(nulls);
    Some(if text {
        ColumnVector::Utf8 {
            data,
            offsets,
            nulls,
        }
    } else if matches!(name.as_str(), "MAKETIME" | "SEC_TO_TIME") {
        ColumnVector::Time64 {
            data: numbers,
            nulls,
        }
    } else {
        ColumnVector::I64 {
            data: numbers,
            nulls,
        }
    })
}
