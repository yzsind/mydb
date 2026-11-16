//! Shared value semantics for scalar transcendental functions. The compiled
//! evaluator binds the operation once; the interpreter keeps syntax-sensitive
//! literal conversion before calling the same value kernel.
use super::{error, expect_arg_count, expect_arg_range, numeric};
use crate::types::{QueryError, Value};

#[derive(Debug, Clone, Copy)]
pub(crate) enum MathFunction {
    Pow,
    Power,
    Ln,
    Log,
    Log2,
    Log10,
    Exp,
    Degrees,
    Radians,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Atan2,
    Cot,
}

impl MathFunction {
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "POW" => Self::Pow,
            "POWER" => Self::Power,
            "LN" => Self::Ln,
            "LOG" => Self::Log,
            "LOG2" => Self::Log2,
            "LOG10" => Self::Log10,
            "EXP" => Self::Exp,
            "DEGREES" => Self::Degrees,
            "RADIANS" => Self::Radians,
            "SIN" => Self::Sin,
            "COS" => Self::Cos,
            "TAN" => Self::Tan,
            "ASIN" => Self::Asin,
            "ACOS" => Self::Acos,
            "ATAN" => Self::Atan,
            "ATAN2" => Self::Atan2,
            "COT" => Self::Cot,
            _ => return None,
        })
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Pow => "POW",
            Self::Power => "POWER",
            Self::Ln => "LN",
            Self::Log => "LOG",
            Self::Log2 => "LOG2",
            Self::Log10 => "LOG10",
            Self::Exp => "EXP",
            Self::Degrees => "DEGREES",
            Self::Radians => "RADIANS",
            Self::Sin => "SIN",
            Self::Cos => "COS",
            Self::Tan => "TAN",
            Self::Asin => "ASIN",
            Self::Acos => "ACOS",
            Self::Atan => "ATAN",
            Self::Atan2 => "ATAN2",
            Self::Cot => "COT",
        }
    }

    pub(crate) fn check_arity<T>(self, name: &str, args: &[T]) -> Result<(), QueryError> {
        match self {
            Self::Log | Self::Atan => expect_arg_range(name, args, 1, 2),
            Self::Pow | Self::Power | Self::Atan2 => expect_arg_count(name, args, 2),
            _ => expect_arg_count(name, args, 1),
        }
    }

    pub(crate) fn numeric_hex_literals(self) -> bool {
        matches!(self, Self::Pow | Self::Power | Self::Exp)
    }

    pub(crate) fn evaluate(self, values: &[Value]) -> Result<Value, QueryError> {
        self.check_arity(self.name(), values)?;
        if values.iter().any(|value| matches!(value, Value::Null)) {
            return Ok(Value::Null);
        }
        let x = numeric(&values[0])?;
        let result = match self {
            Self::Ln => return super::eval_logarithm(x, f64::ln),
            Self::Log2 => return super::eval_logarithm(x, f64::log2),
            Self::Log10 => return super::eval_logarithm(x, f64::log10),
            Self::Log if values.len() == 1 => return super::eval_logarithm(x, f64::ln),
            Self::Log => {
                let value = numeric(&values[1])?;
                if x <= 0.0 || x == 1.0 {
                    return super::invalid_logarithm_value();
                }
                return super::eval_logarithm(value, |value| value.ln() / x.ln());
            }
            Self::Pow | Self::Power => {
                let y = numeric(&values[1])?;
                let result = x.powf(y);
                if !result.is_finite() {
                    return Err(error(
                        crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                        format!("DOUBLE value is out of range in 'pow({x},{y})'"),
                    ));
                }
                result
            }
            Self::Exp => {
                let result = x.exp();
                if !result.is_finite() {
                    return Err(error(
                        crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                        format!("DOUBLE value is out of range in 'exp({x})'"),
                    ));
                }
                result
            }
            Self::Degrees => {
                let result = x.to_degrees();
                if result.is_infinite() && x.is_finite() {
                    return Err(error(
                        crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                        format!("DOUBLE value is out of range in 'degrees({x})'"),
                    ));
                }
                result
            }
            Self::Radians => x.to_radians(),
            Self::Sin => x.sin(),
            Self::Cos => x.cos(),
            Self::Tan => x.tan(),
            Self::Asin | Self::Acos => {
                let result = if matches!(self, Self::Asin) {
                    x.asin()
                } else {
                    x.acos()
                };
                if result.is_nan() {
                    return Ok(Value::Null);
                }
                result
            }
            Self::Atan if values.len() == 1 => x.atan(),
            Self::Atan | Self::Atan2 => x.atan2(numeric(&values[1])?),
            Self::Cot => {
                let result = 1.0 / x.tan();
                if result.is_infinite() {
                    return Err(error(
                        crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                        format!("DOUBLE value is out of range in 'cot({x})'"),
                    ));
                }
                result
            }
        };
        Ok(Value::Double(result))
    }
}
