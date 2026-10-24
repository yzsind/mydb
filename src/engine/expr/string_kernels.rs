//! Value kernels shared by interpreted and compiled scalar execution.
use super::{
    mysql_bytes_replace, mysql_case_map, mysql_str_replace, null_or, str_value,
    string_value_for_connection, value_bytes,
};
use crate::types::{QueryError, Value};

/// Trim ASCII spaces while preserving connection-charset conversion errors.
#[inline]
pub(crate) fn eval_edge_trim_value(value: Value, leading: bool) -> Result<Value, QueryError> {
    null_or(value, |value| {
        let text = if matches!(value, Value::Bytes(_)) {
            std::borrow::Cow::Owned(string_value_for_connection(&value)?)
        } else {
            str_value(&value)
        };
        let trimmed = if leading {
            text.trim_start_matches(' ')
        } else {
            text.trim_end_matches(' ')
        };
        Ok(Value::String(trimmed.to_string()))
    })
}

/// Pack the leading character bytes; binary inputs use one byte.
#[inline]
pub(crate) fn eval_ord_value(value: Value) -> Result<Value, QueryError> {
    null_or(value, |value| {
        // Byte-oriented UTF-8 leader classification: matches the
        // SQL ORD byte semantics.
        let bytes = value_bytes(&value);
        if bytes.is_empty() {
            return Ok(Value::Int(0));
        }
        let leader = bytes[0];
        let cont_len = if matches!(value, Value::Bytes(_)) || leader & 0x80 == 0 {
            0
        } else if leader & 0xE0 == 0xC0 {
            1
        } else if leader & 0xF0 == 0xE0 {
            2
        } else if leader & 0xF8 == 0xF0 {
            3
        } else {
            0
        };
        let take = (cont_len + 1).min(bytes.len());
        let mut acc: i64 = 0;
        for &b in &bytes[..take] {
            acc = (acc << 8) | b as i64;
        }
        Ok(Value::Int(acc))
    })
}

/// Reverse binary bytes or Unicode scalars without cloning owned bytes.
#[inline]
pub(crate) fn eval_reverse_value(value: Value) -> Result<Value, QueryError> {
    null_or(value, |value| {
        if let Value::Bytes(bytes) = value {
            let mut bytes = bytes;
            bytes.reverse();
            return Ok(Value::Bytes(bytes));
        }
        Ok(Value::String(str_value(&value).chars().rev().collect()))
    })
}

/// Binary lengths count octets; text lengths count Unicode scalars.
#[inline]
pub(crate) fn eval_char_length_value(value: Value) -> Result<Value, QueryError> {
    null_or(value, |value| {
        let len = match &value {
            Value::Bytes(bytes) => bytes.len(),
            _ => str_value(&value).chars().count(),
        };
        Ok(Value::Int(len as i64))
    })
}

#[inline]
pub(crate) fn eval_case_map_value(value: Value, upper: bool) -> Result<Value, QueryError> {
    null_or(value, |value| {
        if matches!(value, Value::Bytes(_)) {
            return Ok(value);
        }
        Ok(Value::String(mysql_case_map(&str_value(&value), upper)))
    })
}

#[inline]
pub(crate) fn eval_replace_values(values: &[Value]) -> Result<Value, QueryError> {
    if matches!(values[0], Value::Bytes(_)) {
        return Ok(Value::Bytes(mysql_bytes_replace(
            &value_bytes(&values[0]),
            &value_bytes(&values[1]),
            &value_bytes(&values[2]),
        )));
    }
    let subject = string_value_for_connection(&values[0])?;
    let from = string_value_for_connection(&values[1])?;
    let to = string_value_for_connection(&values[2])?;
    Ok(Value::String(mysql_str_replace(&subject, &from, &to)))
}
