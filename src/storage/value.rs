use std::cmp::Ordering;

use crate::types::{QueryError, Value};

use super::core::error;

pub(crate) fn to_i64(value: Value) -> Result<i64, QueryError> {
    match value {
        Value::Int(v) => Ok(v),
        Value::UInt(v) => i64::try_from(v).map_err(|_| {
            error(
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                "integer out of range",
            )
        }),
        Value::String(v) => v.parse().map_err(|_| {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                "incorrect integer value",
            )
        }),
        Value::SmallString(b) => {
            // SAFETY: SmallString upholds UTF-8 invariant.
            let s = unsafe { std::str::from_utf8_unchecked(&b) };
            s.parse().map_err(|_| {
                error(
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                    "incorrect integer value",
                )
            })
        }
        // A DECIMAL coerces to an integer by truncating toward zero, matching
        // the DecimalCompact / Double arms ("3.14" -> 3, "-3.99" -> -3).
        Value::Decimal(v) => v
            .split_once('.')
            .map_or(v.as_str(), |(int_part, _)| int_part)
            .parse()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                    "incorrect integer value",
                )
            }),
        Value::Float(v) => Ok(v as i64),
        Value::Double(v) => Ok(v as i64),
        Value::Bytes(v) => String::from_utf8_lossy(&v).parse().map_err(|_| {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                "incorrect integer value",
            )
        }),
        // Binary temporal / decimal forms project to integer via their
        // natural numeric component: a DECIMAL drops the scale, a DATE
        // yields its days-since-epoch count, a DATETIME yields the
        // microsecond timestamp, and YEAR yields the year number.
        Value::DecimalCompact(mag, scale) => {
            Ok(crate::util::decimal::rescale_trunc_i128(mag as i128, scale, 0).unwrap_or(0) as i64)
        }
        Value::Decimal128(mag, scale) => crate::util::decimal::rescale_trunc_i128(mag, scale, 0)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "decimal value out of integer range",
                )
            }),
        Value::DecimalWide(value) => {
            use num_traits::ToPrimitive;
            crate::util::decimal::rescale_exact(
                crate::util::decimal::ExactDecimal::Big(value.magnitude, value.scale),
                0,
                false,
            )
            .into_big_parts()
            .0
            .to_i64()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "decimal value out of integer range",
                )
            })
        }
        // Temporal → integer: MySQL's `YYYYMMDD[HHMMSS]` / `HHMMSS` decimal
        // number (single source of truth), zero sentinels → 0.
        Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Year(_) => Ok(crate::types::temporal_to_mysql_number(&value)
            .expect("temporal variant")
            .to_i64()),
        Value::Json(b) => Ok(crate::engine::json::coerce_i64(&b)),
        Value::Geometry(_) => Ok(0),
        Value::Vector(_) => Ok(0),
        Value::Null => Ok(0),
    }
}

pub(crate) fn to_u64(value: Value) -> Result<u64, QueryError> {
    match value {
        Value::UInt(v) => Ok(v),
        Value::Int(v) => {
            if v < 0 {
                return Err(error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "out of range value for column",
                ));
            }
            Ok(v as u64)
        }
        Value::String(v) => v.trim().parse().map_err(|_| {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                "incorrect integer value",
            )
        }),
        Value::SmallString(b) => {
            let s = unsafe { std::str::from_utf8_unchecked(&b) };
            s.trim().parse().map_err(|_| {
                error(
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                    "incorrect integer value",
                )
            })
        }
        Value::Decimal(v) => {
            let t = v.trim();
            t.split_once('.')
                .map_or(t, |(int_part, _)| int_part)
                .parse()
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                        "incorrect integer value",
                    )
                })
        }
        Value::Float(v) => Ok(v as u64),
        Value::Double(v) => Ok(v as u64),
        Value::Bytes(v) => String::from_utf8_lossy(&v).trim().parse().map_err(|_| {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                "incorrect integer value",
            )
        }),
        Value::DecimalCompact(mag, scale) => {
            if mag < 0 {
                return Err(error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "out of range value for column",
                ));
            }
            Ok(crate::util::decimal::rescale_trunc_i128(mag as i128, scale, 0).unwrap_or(0) as u64)
        }
        Value::Decimal128(mag, scale) => {
            if mag < 0 {
                return Err(error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "out of range value for column",
                ));
            }
            crate::util::decimal::rescale_trunc_i128(mag, scale, 0)
                .and_then(|value| u64::try_from(value).ok())
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                        "decimal value out of integer range",
                    )
                })
        }
        Value::DecimalWide(value) => {
            use num_traits::ToPrimitive;
            crate::util::decimal::rescale_exact(
                crate::util::decimal::ExactDecimal::Big(value.magnitude, value.scale),
                0,
                false,
            )
            .into_big_parts()
            .0
            .to_u64()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "decimal value out of integer range",
                )
            })
        }
        // Temporal → unsigned integer: MySQL's decimal-number form; a
        // negative TIME is out of range for an unsigned target (1264),
        // matching the `Value::Int` negative arm above.
        Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Year(_) => {
            let n = crate::types::temporal_to_mysql_number(&value)
                .expect("temporal variant")
                .to_i64();
            if n < 0 {
                return Err(error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "out of range value for column",
                ));
            }
            Ok(n as u64)
        }
        Value::Json(b) => Ok(crate::engine::json::coerce_i64(&b).max(0) as u64),
        Value::Geometry(_) => Ok(0),
        Value::Vector(_) => Ok(0),
        Value::Null => Ok(0),
    }
}

pub(crate) fn to_f64(value: Value) -> Result<f64, QueryError> {
    match value {
        Value::Int(v) => Ok(v as f64),
        Value::UInt(v) => Ok(v as f64),
        Value::Float(v) => Ok(v as f64),
        Value::Double(v) => Ok(v),
        Value::String(v) | Value::Decimal(v) => v.parse().map_err(|_| {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                "incorrect numeric value",
            )
        }),
        Value::SmallString(b) => {
            let s = unsafe { std::str::from_utf8_unchecked(&b) };
            s.parse().map_err(|_| {
                error(
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                    "incorrect numeric value",
                )
            })
        }
        Value::Bytes(v) => String::from_utf8_lossy(&v).parse().map_err(|_| {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                "incorrect numeric value",
            )
        }),
        Value::DecimalCompact(mag, scale) => Ok(mag as f64 / 10f64.powi(scale as i32)),
        Value::Decimal128(mag, scale) => Ok(mag as f64 / 10f64.powi(scale as i32)),
        Value::DecimalWide(value) => {
            crate::util::decimal::format_big_decimal_parts(&value.magnitude, value.scale)
                .parse::<f64>()
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                        "incorrect numeric value",
                    )
                })
        }
        // Temporal → real: MySQL's `YYYYMMDD[HHMMSS].ffffff` decimal number
        // (single source of truth), zero sentinels → 0.
        Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Year(_) => Ok(crate::types::temporal_to_mysql_number(&value)
            .expect("temporal variant")
            .to_f64()),
        Value::Json(b) => Ok(crate::engine::json::coerce_f64(&b)),
        Value::Geometry(_) => Ok(0.0),
        Value::Vector(_) => Ok(0.0),
        Value::Null => Ok(0.0),
    }
}

pub(crate) fn compare_stat_values(left: &Value, right: &Value) -> Ordering {
    if is_numeric_value(left) && is_numeric_value(right) {
        let left = numeric_stat_value(left);
        let right = numeric_stat_value(right);
        return left.partial_cmp(&right).unwrap_or(Ordering::Equal);
    }
    value_to_string(left).cmp(&value_to_string(right))
}

#[inline]
pub(crate) fn is_numeric_value(value: &Value) -> bool {
    matches!(
        value,
        Value::Int(_)
            | Value::UInt(_)
            | Value::Float(_)
            | Value::Double(_)
            | Value::Decimal(_)
            | Value::DecimalCompact(..)
            | Value::Decimal128(..)
            | Value::DecimalWide(..)
    )
}

fn numeric_stat_value(value: &Value) -> f64 {
    match value {
        Value::Int(v) => *v as f64,
        Value::UInt(v) => *v as f64,
        Value::Float(v) => *v as f64,
        Value::Double(v) => *v,
        Value::Decimal(v) => v.parse().unwrap_or(0.0),
        Value::DecimalCompact(mag, scale) => *mag as f64 / 10f64.powi(*scale as i32),
        Value::Decimal128(mag, scale) => *mag as f64 / 10f64.powi(*scale as i32),
        Value::DecimalWide(value) => {
            crate::util::decimal::format_big_decimal_parts(&value.magnitude, value.scale)
                .parse::<f64>()
                .unwrap_or(0.0)
        }
        _ => 0.0,
    }
}

pub fn value_to_string(value: &Value) -> String {
    // Delegates to the canonical `Value::to_display_string` so the
    // binary temporal / decimal variants stay in lockstep with the
    // SHOW / information_schema output format.
    value.to_display_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_decimal_integer_conversion_drops_scale() {
        assert_eq!(to_i64(Value::DecimalCompact(1_100_000, 4)).unwrap(), 110);
        assert_eq!(to_i64(Value::DecimalCompact(-39_900, 4)).unwrap(), -3);
        assert_eq!(to_u64(Value::DecimalCompact(1_100_000, 4)).unwrap(), 110);
    }
}
