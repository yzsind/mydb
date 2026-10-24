//! Parsed MySQL format strings for allocation-free, column-wise rendering.
use crate::engine::time_locale::TimeLocale;
use chrono::{Datelike, NaiveDateTime, Timelike};

#[derive(Debug, Clone)]
pub(crate) struct DateFormatPlan {
    tokens: Vec<Token>,
    max_bytes: u32,
}

#[derive(Debug, Clone)]
enum Token {
    Literal(String),
    Spec(char),
}

impl DateFormatPlan {
    pub(crate) fn new(format: &str) -> Self {
        let mut tokens = Vec::new();
        let mut literal = String::new();
        let mut chars = format.chars();
        let mut max_bytes = 0u32;
        while let Some(ch) = chars.next() {
            if ch != '%' {
                literal.push(ch);
                continue;
            }
            let Some(spec) = chars.next() else {
                literal.push('%');
                break;
            };
            if !literal.is_empty() {
                max_bytes = max_bytes.saturating_add(literal.len().min(u32::MAX as usize) as u32);
                tokens.push(Token::Literal(std::mem::take(&mut literal)));
            }
            max_bytes = max_bytes.saturating_add(spec_width(spec));
            tokens.push(Token::Spec(spec));
        }
        max_bytes = max_bytes.saturating_add(literal.len().min(u32::MAX as usize) as u32);
        if !literal.is_empty() {
            tokens.push(Token::Literal(literal));
        }
        Self { tokens, max_bytes }
    }

    pub(crate) fn max_format_bytes(format: &str) -> u32 {
        let mut chars = format.chars();
        let mut width = 0u32;
        while let Some(ch) = chars.next() {
            width = width.saturating_add(if ch == '%' {
                chars.next().map_or(1, spec_width)
            } else {
                ch.len_utf8() as u32
            });
        }
        width
    }

    pub(crate) fn max_bytes(&self) -> u32 {
        self.max_bytes
    }

    pub(crate) fn append(&self, dt: NaiveDateTime, locale: &TimeLocale, out: &mut String) {
        let (year, month, day) = (dt.year(), dt.month(), dt.day());
        let (hour, minute, second) = (dt.hour(), dt.minute(), dt.second());
        let h12 = if hour % 12 == 0 { 12 } else { hour % 12 };
        for token in &self.tokens {
            let spec = match token {
                Token::Literal(text) => {
                    out.push_str(text);
                    continue;
                }
                Token::Spec(spec) => *spec,
            };
            match spec {
                'Y' => number(out, year as u32, 4),
                'y' => number(out, year.rem_euclid(100) as u32, 2),
                'm' => number(out, month, 2),
                'c' => number(out, month, 0),
                'd' => number(out, day, 2),
                'e' => number(out, day, 0),
                'H' => number(out, hour, 2),
                'k' => number(out, hour, 0),
                'h' | 'I' => number(out, h12, 2),
                'l' => number(out, h12, 0),
                'i' => number(out, minute, 2),
                's' | 'S' => number(out, second, 2),
                'f' => number(out, dt.and_utc().timestamp_subsec_micros(), 6),
                'j' => number(out, dt.ordinal(), 3),
                'w' => number(out, dt.weekday().num_days_from_sunday(), 0),
                'a' => out.push_str(locale.days_abbr[dt.weekday().num_days_from_monday() as usize]),
                'W' => out.push_str(locale.days[dt.weekday().num_days_from_monday() as usize]),
                'b' => out.push_str(locale.months_abbr[month as usize - 1]),
                'M' => out.push_str(locale.months[month as usize - 1]),
                'p' => out.push_str(if hour < 12 { "AM" } else { "PM" }),
                'r' | 'T' => {
                    number(out, if spec == 'r' { h12 } else { hour }, 2);
                    out.push(':');
                    number(out, minute, 2);
                    out.push(':');
                    number(out, second, 2);
                    if spec == 'r' {
                        out.push_str(if hour < 12 { " AM" } else { " PM" });
                    }
                }
                'D' => {
                    number(out, day, 0);
                    out.push_str(match day {
                        11..=13 => "th",
                        _ => match day % 10 {
                            1 => "st",
                            2 => "nd",
                            3 => "rd",
                            _ => "th",
                        },
                    });
                }
                'U' | 'u' | 'V' | 'v' | 'X' | 'x' => {
                    let mode = match spec {
                        'U' => 0,
                        'u' => 1,
                        'V' | 'X' => 2,
                        _ => 3,
                    };
                    let (year, week) = super::mysql_calc_week(dt.date(), mode);
                    if matches!(spec, 'X' | 'x') {
                        use std::fmt::Write;
                        let _ = write!(out, "{year:04}");
                    } else {
                        number(out, week, 2);
                    }
                }
                '%' => out.push('%'),
                other => {
                    out.push('%');
                    out.push(other);
                }
            }
        }
    }
}

fn number(out: &mut String, value: u32, width: usize) {
    let mut buffer = itoa::Buffer::new();
    let digits = buffer.format(value);
    for _ in digits.len()..width {
        out.push('0');
    }
    out.push_str(digits);
}

fn spec_width(spec: char) -> u32 {
    match spec {
        'a' | 'W' | 'b' | 'M' => 64,
        'Y' | 'X' | 'x' => 5,
        'f' => 6,
        'j' => 3,
        'r' => 11,
        'T' => 8,
        'D' => 4,
        '%' => 1,
        'y' | 'm' | 'c' | 'd' | 'e' | 'H' | 'k' | 'h' | 'I' | 'l' | 'i' | 's' | 'S' | 'w' | 'p'
        | 'U' | 'u' | 'V' | 'v' => 2,
        other => 1 + other.len_utf8() as u32,
    }
}
