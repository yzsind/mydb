//! MySQL compact-numeric temporal parsing — the single source of truth for
//! interpreting delimiter-less numeric / numeric-string date & time values
//! (`20200101` → `2020-01-01`, `991231` → `1999-12-31`, `123045` → `12:30:45`).
//!
//! This mirrors MySQL's `number_to_datetime` / `number_to_time`
//! (`sql-common/my_time.cc`): a number (or an all-digit string) is
//! interpreted by magnitude/length into `YYMMDD` / `YYYYMMDD` /
//! `YYMMDDHHMMSS` / `YYYYMMDDHHMMSS`, with the MySQL two-digit-year window
//! (`00..=69` → `2000..=2069`, `70..=99` → `1970..=1999`).
//!
//! Shared by the expression evaluator (`CAST` / `DATE()` / `YEAR()` /
//! `INTERVAL` arithmetic, via `parse_datetime`) and the row codec (write
//! path, via `parse_date_to_days` / `parse_datetime_to_micros`) so every
//! entry point accepts the same compact forms.

/// MySQL's `YY_PART_YEAR`: two-digit years `< 70` map to `20YY`, else `19YY`.
const YY_PART_YEAR: i64 = 70;

/// A decomposed temporal value. Field ranges are validated by the producer;
/// `micros` is the sub-second part (0 for pure integer inputs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTimeParts {
    pub year: u32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub micros: u32,
    /// True when the input carried a time component (`YYYYMMDDHHMMSS` form),
    /// mirroring MySQL's `MYSQL_TIMESTAMP_DATETIME` vs `_DATE` classification.
    pub is_datetime: bool,
}

/// A decomposed TIME value (`HHMMSS`), sign-aware.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeParts {
    pub neg: bool,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub micros: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimeParseError {
    Syntax,
    MinuteOrSecond,
}

/// Fractional microseconds without allocating or cutting a UTF-8 codepoint.
pub(crate) fn parse_time_fraction(text: &str) -> Result<u32, TimeParseError> {
    if !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(TimeParseError::Syntax);
    }
    let mut micros = 0;
    for byte in text.bytes().take(6) {
        micros = micros * 10 + u32::from(byte - b'0');
    }
    Ok(micros * 10u32.pow(6 - text.len().min(6) as u32))
}

/// Duration grammar shared by expressions and stored TIME values. The hour
/// is capped at 839, a sentinel beyond the TIME endpoint; callers decide
/// whether to saturate, reject, or use components in temporal arithmetic.
pub(crate) fn parse_time_duration(text: &str) -> Result<TimeParts, TimeParseError> {
    let text = text.trim();
    let (neg, body) = if let Some(body) = text.strip_prefix('-') {
        (true, body)
    } else {
        (false, text.strip_prefix('+').unwrap_or(text))
    };
    let (main, fraction) = body.split_once('.').unwrap_or((body, ""));
    let micros = parse_time_fraction(fraction)?;
    let (hour, minute, second) = if main.contains(':') {
        let mut fields = main.split(':');
        let first = fields.next().ok_or(TimeParseError::Syntax)?;
        let minute = fields
            .next()
            .ok_or(TimeParseError::Syntax)?
            .parse::<u32>()
            .map_err(|_| TimeParseError::Syntax)?;
        let second = fields
            .next()
            .unwrap_or("0")
            .parse::<u32>()
            .map_err(|_| TimeParseError::Syntax)?;
        if fields.next().is_some() {
            return Err(TimeParseError::Syntax);
        }
        let mut hours = first.split_whitespace();
        let first = hours
            .next()
            .ok_or(TimeParseError::Syntax)?
            .parse::<u64>()
            .map_err(|_| TimeParseError::Syntax)?;
        let hour = if let Some(hour) = hours.next() {
            let hour = hour.parse::<u64>().map_err(|_| TimeParseError::Syntax)?;
            if hours.next().is_some() {
                return Err(TimeParseError::Syntax);
            }
            first
                .checked_mul(24)
                .and_then(|days| days.checked_add(hour))
                .ok_or(TimeParseError::Syntax)?
        } else {
            first
        };
        (hour.min(839) as u32, minute, second)
    } else {
        let number = main.parse::<u64>().map_err(|_| TimeParseError::Syntax)?;
        (
            (number / 10000).min(839) as u32,
            ((number / 100) % 100) as u32,
            (number % 100) as u32,
        )
    };
    if minute >= 60 || second >= 60 {
        return Err(TimeParseError::MinuteOrSecond);
    }
    Ok(TimeParts {
        neg,
        hour,
        minute,
        second,
        micros,
    })
}

/// Parse MySQL's permissive delimiter-bearing date/datetime forms into
/// components without requiring the calendar date itself to exist.
///
/// This is deliberately separate from [`parse_compact_datetime`]: callers
/// need the components of values such as `2014-02-31` (when
/// `ALLOW_INVALID_DATES` is active) and `9999-00-00`, while chrono can only
/// represent real calendar dates. MySQL also accepts redundant leading
/// zeroes in fields and the ISO compact form `YYYYMMDDTHHMMSS`.
pub fn parse_delimited_datetime_components(s: &str) -> Option<DateTimeParts> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }

    let (date_text, time_text) =
        if let Some(index) = trimmed.bytes().position(|byte| matches!(byte, b' ' | b'T')) {
            let date = &trimmed[..index];
            let time = trimmed[index + 1..].trim();
            if time.is_empty() || time.bytes().any(|byte| byte.is_ascii_whitespace()) {
                return None;
            }
            (date, Some(time))
        } else {
            (trimmed, None)
        };

    let parse_field = |field: &str| -> Option<u32> {
        (!field.is_empty() && field.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| field.parse::<u32>().ok())
            .flatten()
    };

    let (mut year, month, day) = if date_text
        .bytes()
        .any(|b| b.is_ascii_punctuation() && b != b':')
    {
        let mut fields = date_text.split(|c: char| c.is_ascii_punctuation());
        let year_text = fields.next()?;
        let month_text = fields.next()?;
        let day_text = fields.next()?;
        if fields.next().is_some() {
            return None;
        }
        let year = parse_field(year_text)?;
        let year = if year_text.len() <= 2 {
            if year < YY_PART_YEAR as u32 {
                2000 + year
            } else {
                1900 + year
            }
        } else {
            year
        };
        (year, parse_field(month_text)?, parse_field(day_text)?)
    } else if date_text.len() == 8 && date_text.bytes().all(|byte| byte.is_ascii_digit()) {
        (
            date_text[..4].parse().ok()?,
            date_text[4..6].parse().ok()?,
            date_text[6..8].parse().ok()?,
        )
    } else {
        return None;
    };
    if year > 9999 || month > 12 || day > 31 {
        return None;
    }

    let (hour, minute, second, micros, is_datetime) = match time_text {
        None => (0, 0, 0, 0, false),
        Some(time) => {
            // MySQL accepts punctuation as temporal field delimiters. In
            // particular `11:59.59.555555` means 11:59:59.555555. A final
            // dot is fractional only after the main time already has three
            // fields; otherwise a single dot can be the minute/second
            // delimiter (`11:59.59`).
            let colons = time.bytes().filter(|byte| *byte == b':').count();
            let dots = time.bytes().filter(|byte| *byte == b'.').count();
            let has_fraction =
                (colons >= 2 && dots >= 1) || (colons == 1 && dots >= 2) || dots >= 3;
            let (main, fraction) = if has_fraction {
                time.rsplit_once('.')
                    .map_or((time, None), |(main, frac)| (main, Some(frac)))
            } else {
                (time, None)
            };
            let (hour, minute, second) = if main.contains([':', '.']) {
                let mut fields = main.split([':', '.']);
                let hour = parse_field(fields.next()?)?;
                let minute = parse_field(fields.next()?)?;
                let second = match fields.next() {
                    Some(field) => parse_field(field)?,
                    None => 0,
                };
                if fields.next().is_some() {
                    return None;
                }
                (hour, minute, second)
            } else if (1..=2).contains(&main.len())
                && main.bytes().all(|byte| byte.is_ascii_digit())
            {
                (parse_field(main)?, 0, 0)
            } else if main.len() == 6 && main.bytes().all(|byte| byte.is_ascii_digit()) {
                (
                    main[..2].parse().ok()?,
                    main[2..4].parse().ok()?,
                    main[4..6].parse().ok()?,
                )
            } else {
                return None;
            };
            if hour > 23 || minute > 59 || second > 59 {
                return None;
            }
            let micros = match fraction {
                None => 0,
                Some(frac)
                    if !frac.is_empty() && frac.bytes().all(|byte| byte.is_ascii_digit()) =>
                {
                    fractional_digits_to_micros(frac)
                }
                Some(_) => return None,
            };
            (hour, minute, second, micros, true)
        }
    };
    // The abbreviated all-zero date is special only when its time is also
    // zero. `00-00-00 01:00:00` uses the ordinary two-digit pivot (year
    // 2000), while `00-00-00[ 00:00:00]` is MySQL's zero datetime.
    if date_text == "00-00-00" && hour == 0 && minute == 0 && second == 0 && micros == 0 {
        year = 0;
    }

    Some(DateTimeParts {
        year,
        month,
        day,
        hour,
        minute,
        second,
        micros,
        is_datetime,
    })
}

/// Split an all-digit temporal string into its integer digits and an
/// optional fractional-seconds micros value. Returns `None` if the string
/// (ignoring surrounding whitespace) is not `DIGITS` or `DIGITS.DIGITS`.
///
/// A leading `+` is accepted; anything else non-numeric (separators such as
/// `-`, `:`, letters) means it is not a compact numeric temporal and the
/// caller should fall through to its regular parser.
fn split_numeric_string(s: &str) -> Option<(&str, u32)> {
    let s = s.trim();
    let s = s.strip_prefix('+').unwrap_or(s);
    if s.is_empty() {
        return None;
    }
    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (s, None),
    };
    if int_part.is_empty() || !int_part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let micros = match frac_part {
        None => 0,
        Some(f) => {
            if !f.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            fractional_digits_to_micros(f)
        }
    };
    Some((int_part, micros))
}

/// Interpret a fractional-second digit string (the part after `.`) as
/// microseconds, truncating past 6 digits (MySQL rounds, but the compact
/// forms in practice carry ≤ 6 digits; truncation keeps this side-effect
/// free and never over-reads).
fn fractional_digits_to_micros(frac: &str) -> u32 {
    let mut micros = 0u32;
    for (i, b) in frac.bytes().enumerate() {
        if i >= 6 {
            break;
        }
        micros = micros * 10 + (b - b'0') as u32;
    }
    for _ in frac.len().min(6)..6 {
        micros *= 10;
    }
    micros
}

/// MySQL `number_to_datetime`: interpret `nr` (a non-negative integer, e.g.
/// `20200101` or `20200101123045`) as a date or datetime. Returns `None` for
/// zero (callers handle the zero sentinel separately), for values that fall
/// in MySQL's error gaps, or for values whose decomposed fields are out of
/// range. `micros` is folded into the result when a time component exists.
fn number_to_datetime(nr: i64, micros: u32) -> Option<DateTimeParts> {
    if nr < 0 {
        return None;
    }
    let mut n = nr;
    let mut is_datetime = false;

    if nr == 0 {
        return None;
    } else if nr >= 10_000_101_000_000 {
        // Already YYYYMMDDHHMMSS.
        is_datetime = true;
    } else if nr < 101 {
        return None;
    } else if nr <= (YY_PART_YEAR - 1) * 10_000 + 1231 {
        // YYMMDD, two-digit year 00..=69 → 2000..=2069.
        n = (nr + 20_000_000) * 1_000_000;
    } else if nr < YY_PART_YEAR * 10_000 + 101 {
        return None;
    } else if nr <= 991_231 {
        // YYMMDD, two-digit year 70..=99 → 1970..=1999.
        n = (nr + 19_000_000) * 1_000_000;
    } else if nr < 10_000_101 {
        return None;
    } else if nr <= 99_991_231 {
        // YYYYMMDD.
        n = nr * 1_000_000;
    } else if nr < 101_000_000 {
        return None;
    } else {
        // YYMMDDHHMMSS range.
        is_datetime = true;
        if nr <= (YY_PART_YEAR - 1) * 10_000_000_000 + 1_231_235_959 {
            n = nr + 20_000_000_000_000;
        } else if nr < YY_PART_YEAR * 10_000_000_000 + 101_000_000 {
            return None;
        } else if nr <= 991_231_235_959 {
            n = nr + 19_000_000_000_000;
        }
        // else: leave `n = nr` as-is; validated below (and rejected if bad).
    }

    let part1 = n / 1_000_000;
    let part2 = n - part1 * 1_000_000;
    let year = (part1 / 10_000) as u32;
    let md = part1 % 10_000;
    let month = (md / 100) as u32;
    let day = (md % 100) as u32;
    let hour = (part2 / 10_000) as u32;
    let hm = part2 % 10_000;
    let minute = (hm / 100) as u32;
    let second = (hm % 100) as u32;

    if year > 9999 || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(DateTimeParts {
        year,
        month,
        day,
        hour,
        minute,
        second,
        micros,
        is_datetime,
    })
}

/// Parse a value that may be a compact numeric date/datetime, from a string
/// rendering (`str_value`). Accepts all-digit strings (optionally with a
/// fractional `.f` part) such as `"20200101"`, `"20200101123045"`,
/// `"991231"`, `"20200101.0"`. Returns `None` if the string is not a compact
/// numeric temporal (letting the caller's normal date parser run).
pub fn parse_compact_datetime(s: &str) -> Option<DateTimeParts> {
    let (int_part, micros) = split_numeric_string(s)?;
    // Guard against absurdly long digit runs (they cannot be a compact
    // temporal and would overflow i64); a legitimate YYYYMMDDHHMMSS is 14
    // digits. Allow a little slack for leading zeros.
    if int_part.len() > 15 {
        return None;
    }
    let nr: i64 = int_part.parse().ok()?;
    number_to_datetime(nr, micros)
}

#[cfg(test)]
/// MySQL `number_to_time`: interpret `nr` (e.g. `123045`) as an `HHMMSS`
/// time. Values in the plain TIME range decompose directly; larger values
/// that are actually datetimes take the time-of-day component. Returns
/// `None` when the fields are out of range.
fn number_to_time(nr: i64, micros: u32) -> Option<TimeParts> {
    let neg = nr < 0;
    let mut n = nr.unsigned_abs() as i64;
    // A value large enough to be a datetime (>= 10000000000, i.e. ≥ 10
    // digits) is reinterpreted as YYYYMMDDHHMMSS and its time-of-day taken.
    if n >= 10_000_000_000 {
        let dt = number_to_datetime(n, micros)?;
        return Some(TimeParts {
            neg,
            hour: dt.hour,
            minute: dt.minute,
            second: dt.second,
            micros: dt.micros,
        });
    }
    let second = (n % 100) as u32;
    n /= 100;
    let minute = (n % 100) as u32;
    n /= 100;
    let hour = n as u32;
    if minute > 59 || second > 59 {
        return None;
    }
    // MySQL's TIME range is ±838:59:59.
    if hour > 838 {
        return None;
    }
    Some(TimeParts {
        neg,
        hour,
        minute,
        second,
        micros,
    })
}

#[cfg(test)]
/// Parse a value that may be a compact numeric time (`"123045"` → 12:30:45,
/// `"1230"` → 00:12:30, `"45"` → 00:00:45). Returns `None` when the string is
/// not an all-digit compact numeric time, letting the caller's colon-form
/// parser run.
pub fn parse_compact_time(s: &str) -> Option<TimeParts> {
    let (int_part, micros) = split_numeric_string(s)?;
    if int_part.len() > 15 {
        return None;
    }
    let nr: i64 = int_part.parse().ok()?;
    number_to_time(nr, micros)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refactor_time_duration_agrees_across_expression_and_storage() {
        let endpoint = (838 * 3600 + 59 * 60 + 59) * 1_000_000i64;
        for (text, expected) in [
            ("12:30:45.1234567", 45_045_123_456),
            ("1 10:30:00", 124_200_000_000),
            ("-1 10:30:00", -124_200_000_000),
            ("123045", 45_045_000_000),
            ("1230", 750_000_000),
            ("+00:00:01.5", 1_500_000),
            ("900:00:00", endpoint),
            ("-900:00:00", -endpoint),
            ("2562047789:00:00", endpoint),
        ] {
            let stored = crate::storage::parse_time_to_micros(text).unwrap();
            assert_eq!(stored, expected, "{text}");
            let (negative, hour, minute, second, micros) =
                crate::engine::expr::parse_time_components(text).unwrap();
            let magnitude = (i64::from(hour) * 3_600_000_000
                + i64::from(minute) * 60_000_000
                + i64::from(second) * 1_000_000
                + i64::from(micros))
            .min(endpoint);
            assert_eq!(
                if negative { -magnitude } else { magnitude },
                stored,
                "{text}"
            );
        }
        for text in [
            "10:99:00",
            "10:00:99",
            "1:2:3:4",
            "1 2 03:00:00",
            "00:00:01.汉字",
            "00:-1:00",
        ] {
            assert!(parse_time_duration(text).is_err(), "{text}");
            assert!(
                crate::engine::expr::parse_time_components(text).is_err(),
                "{text}"
            );
            assert!(
                crate::storage::parse_time_to_micros(text).is_err(),
                "{text}"
            );
        }
    }

    fn dt(s: &str) -> Option<(u32, u32, u32, u32, u32, u32)> {
        parse_compact_datetime(s).map(|p| (p.year, p.month, p.day, p.hour, p.minute, p.second))
    }

    #[test]
    fn compact_dates() {
        assert_eq!(dt("20200101"), Some((2020, 1, 1, 0, 0, 0)));
        assert_eq!(dt("20200101123045"), Some((2020, 1, 1, 12, 30, 45)));
        assert_eq!(dt("991231"), Some((1999, 12, 31, 0, 0, 0)));
        assert_eq!(dt("710101"), Some((1971, 1, 1, 0, 0, 0)));
        assert_eq!(dt("101"), Some((2000, 1, 1, 0, 0, 0)));
        assert_eq!(dt("691231"), Some((2069, 12, 31, 0, 0, 0)));
        assert_eq!(dt("20200101.0"), Some((2020, 1, 1, 0, 0, 0)));
    }

    #[test]
    fn compact_datetime_micros() {
        let p = parse_compact_datetime("20200101123045.5").unwrap();
        assert_eq!(p.micros, 500_000);
        assert!(p.is_datetime);
    }

    #[test]
    fn rejects_non_numeric_and_invalid() {
        assert_eq!(dt("2020-01-01"), None);
        assert_eq!(dt(""), None);
        assert_eq!(dt("0"), None);
        assert_eq!(dt("20201301"), None); // month 13
        assert_eq!(dt("20200132"), None); // day 32
        assert_eq!(dt("abc"), None);
    }

    #[test]
    fn delimiter_parser_preserves_mysql_components() {
        let parts = parse_delimited_datetime_components(
            "00000002006-000008-0000010 000010:0000011:00000012.0123450",
        )
        .unwrap();
        assert_eq!(
            (
                parts.year,
                parts.month,
                parts.day,
                parts.hour,
                parts.minute,
                parts.second,
                parts.micros,
            ),
            (2006, 8, 10, 10, 11, 12, 12_345)
        );
        let invalid = parse_delimited_datetime_components("2014-02-31").unwrap();
        assert_eq!((invalid.year, invalid.month, invalid.day), (2014, 2, 31));
        assert_eq!(
            parse_delimited_datetime_components("20030102T131415")
                .map(|p| (p.year, p.month, p.day, p.hour, p.minute, p.second)),
            Some((2003, 1, 2, 13, 14, 15))
        );
        assert_eq!(
            parse_delimited_datetime_components("12-00-00").map(|p| (p.year, p.month, p.day)),
            Some((2012, 0, 0))
        );
        assert!(parse_delimited_datetime_components("2003-0303 12:13:14").is_none());
    }

    #[test]
    fn compact_times() {
        let t = |s: &str| parse_compact_time(s).map(|p| (p.hour, p.minute, p.second));
        assert_eq!(t("123045"), Some((12, 30, 45)));
        assert_eq!(t("1230"), Some((0, 12, 30)));
        assert_eq!(t("45"), Some((0, 0, 45)));
        assert_eq!(t("125959"), Some((12, 59, 59)));
        assert_eq!(t("126099"), None); // minute 60
    }
}

/// Encode a YEAR integer with MySQL two-digit-year and range semantics.
pub(crate) fn year_byte_from_int(v: i64) -> Result<u8, crate::types::QueryError> {
    let normalised = match v {
        0 => return Ok(0),
        1..=69 => v + 2000,
        70..=99 => v + 1900,
        _ => v,
    };
    if !(1901..=2155).contains(&normalised) {
        return Err(crate::storage::error(
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            format!("year out of range: {}", v),
        ));
    }
    Ok((normalised - 1900) as u8)
}

#[inline]
pub(crate) fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

#[inline]
pub(crate) fn days_in_month(year: i32, month: u32) -> Option<u32> {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => Some(31),
        4 | 6 | 9 | 11 => Some(30),
        2 => Some(if is_leap_year(year) { 29 } else { 28 }),
        _ => None,
    }
}
