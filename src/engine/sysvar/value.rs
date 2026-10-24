//! `SysVarValue` — the universal in-memory representation of a system
//! variable's value, with MySQL-aligned parsing and display.
//!
//! The registry stores values in this enum rather than `crate::types::Value`
//! so that:
//!   * type coercion (`SET x = 'YES'` → bool) is centralised here,
//!   * display formatting matches MySQL exactly for `SHOW VARIABLES`,
//!   * backends can return purely owned values without holding session locks.

use crate::types::Value;

/// In-memory representation of any system variable's current value.
///
/// MySQL's variable surface uses a handful of underlying types; we keep
/// them distinct so that a `BOOL` variable round-trips as `"OFF"/"ON"`
/// rather than `"0"/"1"`, and a `DOUBLE` value preserves precision when
/// echoed back via `SHOW VARIABLES`.
#[derive(Debug, Clone, PartialEq)]
pub enum SysVarValue {
    Bool(bool),
    Int(i64),
    UInt(u64),
    Double(f64),
    String(String),
    /// Only valid for `character_set_results` (MySQL allows NULL).
    Null,
}

impl SysVarValue {
    #[cfg(test)]
    /// Empty string sentinel — used for variables like `init_connect` that
    /// MySQL displays as an empty value by default.
    pub fn empty_string() -> Self {
        SysVarValue::String(String::new())
    }

    /// Value returned by `SELECT @@var`. MySQL serialises every system
    /// variable as a string on the wire (the wire types are CSTRING),
    /// so we keep the same shape: bool → "1"/"0", everything else → its
    /// MySQL-canonical display string, NULL stays NULL.
    pub fn to_select_value(&self) -> Value {
        match self {
            SysVarValue::Null => Value::Null,
            SysVarValue::Bool(b) => Value::String(if *b { "1" } else { "0" }.to_string()),
            SysVarValue::Int(i) => Value::String(i.to_string()),
            SysVarValue::UInt(u) => Value::String(u.to_string()),
            SysVarValue::Double(d) => Value::String(format_double(*d)),
            SysVarValue::String(s) => Value::String(s.clone()),
        }
    }

    /// MySQL-compatible display rendering used by `SHOW VARIABLES` and
    /// `information_schema.global_variables.VARIABLE_VALUE`.
    ///
    /// NULL renders as the literal string `NULL` to match the way mydb's
    /// JDBC/connector tests expect to see a nulled `character_set_results`
    /// in `SHOW VARIABLES` output (matching MySQL's `mysql` CLI when it
    /// runs `\W` to render the column).
    pub fn to_display_string(&self) -> String {
        match self {
            SysVarValue::Bool(b) => bool_on_off(*b).to_string(),
            SysVarValue::Int(i) => i.to_string(),
            SysVarValue::UInt(u) => u.to_string(),
            SysVarValue::Double(d) => format_double(*d),
            SysVarValue::String(s) => s.clone(),
            SysVarValue::Null => "NULL".to_string(),
        }
    }

    /// Coerce a value supplied by a `SET` statement (a parsed `Value`) into
    /// the target type. Used by `DataType::coerce`.
    pub fn from_sql_value(v: &Value) -> SysVarValue {
        match v {
            Value::Null => SysVarValue::Null,
            Value::Int(i) => SysVarValue::Int(*i),
            Value::UInt(u) => SysVarValue::UInt(*u),
            Value::Double(d) => SysVarValue::Double(*d),
            Value::Float(f) => SysVarValue::Double(*f as f64),
            Value::String(s) => SysVarValue::String(s.clone()),
            Value::Decimal(d) => SysVarValue::String(d.to_string()),
            Value::DecimalCompact(..) | Value::Decimal128(..) | Value::DecimalWide(..) => {
                SysVarValue::String(v.to_display_string())
            }
            other => SysVarValue::String(other.to_display_string()),
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            SysVarValue::Bool(b) => Some(*b),
            SysVarValue::Int(i) => Some(*i != 0),
            SysVarValue::UInt(u) => Some(*u != 0),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            SysVarValue::Bool(b) => Some(if *b { 1 } else { 0 }),
            SysVarValue::Int(i) => Some(*i),
            SysVarValue::UInt(u) => i64::try_from(*u).ok(),
            SysVarValue::Double(d) if d.is_finite() => Some(*d as i64),
            SysVarValue::String(s) => parse_int_lenient(s).ok(),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            SysVarValue::Bool(b) => Some(if *b { 1 } else { 0 }),
            SysVarValue::Int(i) if *i >= 0 => Some(*i as u64),
            SysVarValue::UInt(u) => Some(*u),
            SysVarValue::Double(d) if d.is_finite() && *d >= 0.0 => Some(*d as u64),
            SysVarValue::String(s) => parse_uint_lenient(s).ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            SysVarValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            SysVarValue::Int(i) => Some(*i as f64),
            SysVarValue::UInt(u) => Some(*u as f64),
            SysVarValue::Double(d) => Some(*d),
            SysVarValue::String(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            SysVarValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn is_null(&self) -> bool {
        matches!(self, SysVarValue::Null)
    }
}

fn bool_on_off(b: bool) -> &'static str {
    if b {
        "ON"
    } else {
        "OFF"
    }
}

#[cfg(test)]
/// Parse a MySQL-flavoured boolean literal. Accepts the spectrum the
/// shipped clients use:
///   * `0` / `1` (`Value::Int` or string form)
///   * `OFF` / `ON`
///   * `FALSE` / `TRUE`
///   * `NO` / `YES`
pub fn parse_bool(raw: &str) -> Result<bool, String> {
    let trimmed = raw.trim();
    let upper = trimmed.to_ascii_uppercase();
    match upper.as_str() {
        "1" | "ON" | "TRUE" | "YES" => Ok(true),
        "0" | "OFF" | "FALSE" | "NO" => Ok(false),
        _ => Err(format!("invalid boolean value: '{raw}'")),
    }
}

/// Strict boolean parser for **system variable** `SET` values, matching
/// MySQL 8.0 exactly. Unlike [`parse_bool`] (which also accepts the legacy
/// YES/NO and TRUE/FALSE *strings* for config-file parsing), a MySQL boolean
/// sysvar accepts only the `ON`/`OFF` keywords (case-insensitive) or the
/// numeric literals `0`/`1` when the value arrives as a string. Bareword
/// `TRUE`/`FALSE` are SQL boolean literals that reach the coercion layer as
/// integer `1`/`0`, so they are handled by the integer arm — not here.
/// Everything else (`'yes'`, `'No'`, `'True'`, `' '`, `''`, `2`, ...) is
/// rejected so the caller can raise ER_WRONG_VALUE_FOR_VAR (1231).
pub fn parse_bool_sysvar(raw: &str) -> Result<bool, ()> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "1" | "ON" => Ok(true),
        "0" | "OFF" => Ok(false),
        _ => Err(()),
    }
}

/// Strict integer parser for **system variable** `SET` values. Accepts only
/// a pure signed integer (optional sign + ASCII digits, surrounding
/// whitespace tolerated); rejects fractional / exponent / non-numeric forms
/// so the caller can raise ER_WRONG_TYPE_FOR_VAR (1232) exactly where MySQL
/// does (`SET x = 1.1`, `SET x = 'foo'`, `SET x = ' '`). Returns the value
/// widened to `i128` so both the signed and unsigned range checks can be
/// applied by the caller without re-parsing.
pub fn parse_int_sysvar(raw: &str) -> Option<i128> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<i128>().ok()
}

pub fn parse_int_lenient(raw: &str) -> Result<i64, String> {
    let trimmed = raw.trim();
    trimmed.parse::<i64>().or_else(|_| {
        // Accept trailing `.0`-style float-ish text.
        trimmed
            .parse::<f64>()
            .ok()
            .and_then(|f| {
                if f.is_finite() && f.fract() == 0.0 {
                    Some(f as i64)
                } else {
                    None
                }
            })
            .ok_or_else(|| format!("invalid integer value: '{raw}'"))
    })
}

pub fn parse_uint_lenient(raw: &str) -> Result<u64, String> {
    let trimmed = raw.trim();
    trimmed.parse::<u64>().or_else(|_| {
        trimmed
            .parse::<f64>()
            .ok()
            .and_then(|f| {
                if f.is_finite() && f >= 0.0 && f.fract() == 0.0 && f <= u64::MAX as f64 {
                    Some(f as u64)
                } else {
                    None
                }
            })
            .ok_or_else(|| format!("invalid unsigned value: '{raw}'"))
    })
}

/// Match MySQL's compact decimal rendering: trim trailing zeros, drop
/// trailing `.`, but keep at least the integer part. Integer-valued
/// doubles render without a decimal point.
pub fn format_double(d: f64) -> String {
    if !d.is_finite() {
        return d.to_string();
    }
    if d.fract() == 0.0 && d.abs() < 1e15 {
        return format!("{}", d as i64);
    }
    let mut s = format!("{:.6}", d);
    while s.ends_with('0') {
        s.pop();
    }
    if s.ends_with('.') {
        s.pop();
    }
    if s.is_empty() {
        "0".to_string()
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_display_round_trip() {
        assert_eq!(SysVarValue::Bool(true).to_display_string(), "ON");
        assert_eq!(SysVarValue::Bool(false).to_display_string(), "OFF");
    }

    #[test]
    fn select_value_for_bool_is_zero_one() {
        // MySQL `SELECT @@autocommit` returns 1/0 not ON/OFF.
        assert_eq!(
            SysVarValue::Bool(true).to_select_value(),
            Value::String("1".to_string())
        );
        assert_eq!(
            SysVarValue::Bool(false).to_select_value(),
            Value::String("0".to_string())
        );
    }

    #[test]
    fn parse_bool_accepts_mysql_synonyms() {
        for v in ["1", "ON", "on", "True", "YES"] {
            assert_eq!(parse_bool(v).unwrap(), true, "{v}");
        }
        for v in ["0", "OFF", "off", "False", "NO"] {
            assert_eq!(parse_bool(v).unwrap(), false, "{v}");
        }
        assert!(parse_bool("MAYBE").is_err());
    }

    #[test]
    fn format_double_strips_trailing_zeros() {
        assert_eq!(format_double(1.0), "1");
        assert_eq!(format_double(1.5), "1.5");
        assert_eq!(format_double(0.10000), "0.1");
        assert_eq!(format_double(0.0), "0");
    }

    #[test]
    fn parse_int_lenient_accepts_float_form() {
        assert_eq!(parse_int_lenient("42").unwrap(), 42);
        assert_eq!(parse_int_lenient("42.0").unwrap(), 42);
        assert!(parse_int_lenient("42.5").is_err());
    }

    #[test]
    fn as_u64_widens_int() {
        assert_eq!(SysVarValue::Int(7).as_u64(), Some(7));
        assert_eq!(SysVarValue::Int(-1).as_u64(), None);
    }
}
