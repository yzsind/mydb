//! Calendar reads over typed temporal lanes. Conversion diagnostics remain on
//! the coordinator; unsupported, zero and incomplete dates retain the oracle.
use super::*;
use chrono::{Datelike, Timelike};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Date,
    Time,
    Timestamp,
    DayOfWeek,
    DayOfYear,
    Weekday,
    Week,
    YearWeek,
    ToDays,
    ToSeconds,
    LastDay,
    DayName,
    MonthName,
    Microsecond,
    TimeToSec,
    CurrentDate,
    CurrentTime,
    CurrentDateTime,
}
#[derive(Clone, Copy)]
enum Cell {
    Number(i64),
    Text(&'static str),
}

pub(super) struct TemporalRead {
    column: usize,
    kind: Kind,
    mode: u32,
}
impl TemporalRead {
    pub(super) fn bind(node: &ExprNode, schema: &BatchSchema) -> Option<Self> {
        let ExprNode::Function { kernel, args, .. } = node else {
            return None;
        };
        let (kind, mode) = match kernel {
            FunctionKernel::Curdate | FunctionKernel::UtcDate if args.is_empty() => (
                Kind::CurrentDate,
                u32::from(matches!(kernel, FunctionKernel::UtcDate)) << 3,
            ),
            FunctionKernel::Now
            | FunctionKernel::Curtime
            | FunctionKernel::UtcTimestamp
            | FunctionKernel::UtcTime
                if args.len() <= 1 =>
            {
                let fsp = match args.first() {
                    Some(arg) => u32::try_from(integer(arg)?).ok()?,
                    None => 0,
                };
                if fsp > 6 {
                    return None;
                }
                (
                    if matches!(kernel, FunctionKernel::Curtime | FunctionKernel::UtcTime) {
                        Kind::CurrentTime
                    } else {
                        Kind::CurrentDateTime
                    },
                    fsp | (u32::from(matches!(
                        kernel,
                        FunctionKernel::UtcTime | FunctionKernel::UtcTimestamp
                    )) << 3),
                )
            }
            FunctionKernel::Date => (Kind::Date, 0),
            FunctionKernel::Dayofweek => (Kind::DayOfWeek, 0),
            FunctionKernel::Dayofyear => (Kind::DayOfYear, 0),
            FunctionKernel::Weekday => (Kind::Weekday, 0),
            FunctionKernel::ToDays => (Kind::ToDays, 0),
            FunctionKernel::LastDay => (Kind::LastDay, 0),
            FunctionKernel::Microsecond => (Kind::Microsecond, 0),
            FunctionKernel::Weekofyear if args.len() == 1 => (Kind::Week, 3),
            FunctionKernel::Week if (1..=2).contains(&args.len()) => (
                Kind::Week,
                match args.get(1) {
                    Some(arg) => integer(arg)?.rem_euclid(8) as u32,
                    None => crate::engine::expr::session_default_week_format() % 8,
                },
            ),
            FunctionKernel::LegacyDeterministic(name) | FunctionKernel::LegacySession(name) => {
                match *name {
                    "TIME" => (Kind::Time, 0),
                    "TIMESTAMP" => (Kind::Timestamp, 0),
                    "TIME_TO_SEC" => (Kind::TimeToSec, 0),
                    "TO_SECONDS" => (Kind::ToSeconds, 0),
                    "DAYNAME" => (Kind::DayName, 0),
                    "MONTHNAME" => (Kind::MonthName, 0),
                    "YEARWEEK" if (1..=2).contains(&args.len()) => (
                        Kind::YearWeek,
                        match args.get(1) {
                            Some(arg) => integer(arg)?,
                            None => 0,
                        }
                        .rem_euclid(8) as u32
                            | 2,
                    ),
                    _ => return None,
                }
            }
            _ => return None,
        };
        if matches!(
            kind,
            Kind::CurrentDate | Kind::CurrentTime | Kind::CurrentDateTime
        ) {
            return Some(Self {
                column: 0,
                kind,
                mode,
            });
        }
        if !matches!(kind, Kind::Week | Kind::YearWeek) && args.len() != 1 {
            return None;
        }
        let ExprNode::Column(column) = args.first()? else {
            return None;
        };
        let field = schema.fields().get(*column)?;
        match field.column_type {
            // Preserve the existing identity kernel for DATE(DATE).
            ColumnType::Date if kind == Kind::Date => return None,
            ColumnType::Date | ColumnType::DateTime | ColumnType::Timestamp => {}
            ColumnType::Time
                if matches!(kind, Kind::Time | Kind::TimeToSec | Kind::Microsecond) => {}
            _ => return None,
        }
        Some(Self {
            column: *column,
            kind,
            mode,
        })
    }
    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: match self.kind {
                Kind::DayName | Kind::MonthName => Domain::Text,
                Kind::Date
                | Kind::LastDay
                | Kind::Time
                | Kind::Timestamp
                | Kind::CurrentDate
                | Kind::CurrentTime
                | Kind::CurrentDateTime => Domain::Temporal,
                _ => Domain::Integer(i64::MIN, i64::MAX),
            },
            parallel: false,
        }
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let rows = batch.num_rows();
        let payload = if matches!(self.kind, Kind::DayName | Kind::MonthName) {
            let locale = crate::engine::expr::session_time_locale();
            let names: &[&str] = if self.kind == Kind::DayName {
                &locale.days
            } else {
                &locale.months
            };
            rows.checked_mul(names.iter().map(|n| n.len()).max().unwrap_or(0))?
                .checked_add(rows.checked_add(1)?.checked_mul(8)?)?
        } else {
            rows.checked_mul(16)?
        };
        Some((
            payload,
            payload
                .checked_mul(2)?
                .checked_add(rows.checked_mul(32)?)?
                .checked_add(512)?,
        ))
    }
    fn date_cell(
        &self,
        dt: chrono::NaiveDateTime,
        locale: &'static crate::engine::time_locale::TimeLocale,
    ) -> Cell {
        let date = dt.date();
        Cell::Number(match self.kind {
            Kind::Date => i64::from(date.num_days_from_ce()) - 719163,
            Kind::DayOfWeek => {
                (crate::engine::expr::mysql_calendar_day_number(
                    date.year(),
                    date.month(),
                    date.day(),
                ) + 6)
                    .rem_euclid(7)
                    + 1
            }
            Kind::Weekday => (crate::engine::expr::mysql_calendar_day_number(
                date.year(),
                date.month(),
                date.day(),
            ) + 5)
                .rem_euclid(7),
            Kind::DayOfYear => i64::from(date.ordinal()),
            Kind::ToDays => {
                i64::from(date.num_days_from_ce())
                    + 365
                    + i64::from(date.year() == 0 && date.month() < 3)
            }
            Kind::Week => i64::from(crate::engine::expr::mysql_calc_week(date, self.mode).1),
            Kind::YearWeek => {
                let (y, w) = crate::engine::expr::mysql_calc_week(date, self.mode);
                i64::from(y) * 100 + i64::from(w)
            }
            Kind::LastDay => {
                let last = match date.month() {
                    2 => {
                        if chrono::NaiveDate::from_ymd_opt(date.year(), 2, 29).is_some() {
                            29
                        } else {
                            28
                        }
                    }
                    4 | 6 | 9 | 11 => 30,
                    _ => 31,
                };
                i64::from(
                    date.with_day(last)
                        .expect("valid month end")
                        .num_days_from_ce(),
                ) - 719163
            }
            Kind::DayName => {
                return Cell::Text(locale.days[date.weekday().num_days_from_monday() as usize])
            }
            Kind::MonthName => return Cell::Text(locale.months[date.month0() as usize]),
            _ => unreachable!(),
        })
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        if matches!(
            self.kind,
            Kind::CurrentDate | Kind::CurrentTime | Kind::CurrentDateTime
        ) {
            // Read the statement-frozen clock at execution time, never bind
            // it into a reusable expression program. Overflow keeps the
            // per-row oracle and its original warning count.
            let utc = self.mode & 8 != 0;
            let now = if utc {
                crate::engine::expr::session_utc_now()
            } else {
                crate::engine::expr::session_now()
            };
            if !(0..=9999).contains(&now.year()) {
                return None;
            }
            let fsp = (self.mode & 7) as usize;
            let value = match (self.kind, utc) {
                (Kind::CurrentDate, false) => crate::engine::expr::eval_curdate_func(),
                (Kind::CurrentDate, true) => crate::engine::expr::eval_utc_date_func(),
                (Kind::CurrentTime, false) => crate::engine::expr::eval_curtime_fsp(fsp),
                (Kind::CurrentTime, true) => crate::engine::expr::eval_utc_time_fsp(fsp),
                (Kind::CurrentDateTime, false) => crate::engine::expr::eval_now_fsp(fsp),
                (Kind::CurrentDateTime, true) => crate::engine::expr::eval_utc_timestamp_fsp(fsp),
                _ => return None,
            };
            return Some(match value {
                Value::Date(value) => ColumnVector::Date32 {
                    data: vec![value; batch.num_rows()],
                    nulls: None,
                },
                Value::Time(value) => ColumnVector::Time64 {
                    data: vec![value; batch.num_rows()],
                    nulls: None,
                },
                Value::DateTime(value) => ColumnVector::DateTime64 {
                    data: vec![value; batch.num_rows()],
                    nulls: None,
                },
                _ => return None,
            });
        }
        let input = batch.columns().get(self.column)?;
        let rows = batch.num_rows();
        if matches!(input, ColumnVector::Null { .. }) {
            return Some(ColumnVector::Null { len: rows });
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
        let text = matches!(self.kind, Kind::DayName | Kind::MonthName);
        let mut numbers = Vec::with_capacity(if text { 0 } else { rows });
        let mut output = String::new();
        let mut offsets = if text {
            Vec::with_capacity(rows + 1)
        } else {
            Vec::new()
        };
        if text {
            offsets.push(0);
        }
        let locale = crate::engine::expr::session_time_locale();
        let mut previous: Option<(i32, Cell)> = None;
        for row in 0..rows {
            if row & 63 == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            if input.is_null(row) {
                if text {
                    offsets.push(u32::try_from(output.len()).ok()?);
                } else {
                    numbers.push(0);
                }
                continue;
            }
            let raw = match input {
                ColumnVector::Date32 { data, .. } => {
                    let days = data[row];
                    if days == crate::types::ZERO_DATE
                        || crate::types::decode_tagged_date(days).is_some()
                    {
                        return None;
                    }
                    i64::from(days).checked_mul(86_400_000_000)?
                }
                ColumnVector::DateTime64 { data, .. } => data[row],
                ColumnVector::Timestamp64 { .. } => local[row],
                ColumnVector::Time64 { data, .. } => {
                    let value = data[row];
                    if i128::from(value).abs() > 3_020_399_000_000 {
                        return None;
                    }
                    numbers.push(match self.kind {
                        Kind::Time => value,
                        Kind::TimeToSec => value / 1_000_000,
                        Kind::Microsecond => (value.unsigned_abs() % 1_000_000) as i64,
                        _ => return None,
                    });
                    continue;
                }
                _ => return None,
            };
            if raw == crate::types::ZERO_DATETIME
                || crate::types::decode_tagged_datetime(raw).is_some()
            {
                return None;
            }
            let dt = chrono::DateTime::from_timestamp_micros(raw)?.naive_utc();
            if !(0..=9999).contains(&dt.year()) {
                return None;
            }
            let cell = match self.kind {
                Kind::Timestamp => Cell::Number(raw),
                Kind::Time => Cell::Number(raw.rem_euclid(86_400_000_000)),
                Kind::TimeToSec => Cell::Number(i64::from(dt.time().num_seconds_from_midnight())),
                Kind::Microsecond => {
                    Cell::Number(i64::from(dt.and_utc().timestamp_subsec_micros()))
                }
                Kind::ToSeconds => Cell::Number(dt.and_utc().timestamp() + 62_167_219_200),
                _ => {
                    let day = dt.date().num_days_from_ce();
                    if let Some((_, value)) = previous.filter(|(key, _)| *key == day) {
                        value
                    } else {
                        let value = self.date_cell(dt, locale);
                        previous = Some((day, value));
                        value
                    }
                }
            };
            match cell {
                Cell::Number(n) => numbers.push(n),
                Cell::Text(s) => {
                    output.push_str(s);
                    offsets.push(u32::try_from(output.len()).ok()?);
                }
            }
        }
        let nulls = super::validity(input);
        Some(match self.kind {
            Kind::DayName | Kind::MonthName => ColumnVector::Utf8 {
                data: output.into_bytes(),
                offsets,
                nulls,
            },
            Kind::Date | Kind::LastDay => ColumnVector::Date32 {
                data: numbers.into_iter().map(|n| n as i32).collect(),
                nulls,
            },
            Kind::Time => ColumnVector::Time64 {
                data: numbers,
                nulls,
            },
            Kind::Timestamp => ColumnVector::DateTime64 {
                data: numbers,
                nulls,
            },
            _ => ColumnVector::I64 {
                data: numbers,
                nulls,
            },
        })
    }
}
