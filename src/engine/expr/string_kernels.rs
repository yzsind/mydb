//! Value kernels shared by interpreted and compiled scalar execution.
use super::{
    mysql_bytes_replace, mysql_case_map, mysql_str_replace, null_or, str_value,
    string_result_exceeds_packet, string_value_for_connection, value_bytes,
};
use crate::types::{QueryError, Value};

/// Trim ASCII spaces in the original binary or character domain.
#[inline]
pub(crate) fn eval_edge_trim_value(value: Value, leading: bool) -> Result<Value, QueryError> {
    null_or(value, |value| {
        if let Value::Bytes(bytes) = value {
            let range = if leading {
                bytes.iter().position(|b| *b != b' ').unwrap_or(bytes.len())..bytes.len()
            } else {
                0..bytes.iter().rposition(|b| *b != b' ').map_or(0, |i| i + 1)
            };
            return Ok(Value::Bytes(bytes[range].to_vec()));
        }
        let text = str_value(&value);
        let trimmed = if leading {
            text.trim_start_matches(' ')
        } else {
            text.trim_end_matches(' ')
        };
        Ok(Value::String(trimmed.to_string()))
    })
}

/// Read the original leading octet without cloning string or binary buffers.
#[inline]
pub(crate) fn eval_ascii_value(value: Value) -> Result<Value, QueryError> {
    null_or(value, |value| {
        let first = match &value {
            Value::String(text) => text.as_bytes().first().copied(),
            Value::SmallString(bytes) => bytes.first().copied(),
            Value::Bytes(bytes) => bytes.first().copied(),
            other => str_value(other).as_bytes().first().copied(),
        };
        Ok(Value::Int(i64::from(first.unwrap_or(0))))
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
        let subject = value_bytes(&values[0]);
        let from = value_bytes(&values[1]);
        let to = value_bytes(&values[2]);
        if replace_exceeds_packet(&subject, &from, to.len()) {
            return Ok(Value::Null);
        }
        return Ok(Value::Bytes(mysql_bytes_replace(&subject, &from, &to)));
    }
    let subject = string_value_for_connection(&values[0])?;
    let from = string_value_for_connection(&values[1])?;
    let to = string_value_for_connection(&values[2])?;
    if replace_exceeds_packet(subject.as_bytes(), from.as_bytes(), to.len()) {
        return Ok(Value::Null);
    }
    Ok(Value::String(mysql_str_replace(&subject, &from, &to)))
}

fn replace_exceeds_packet(subject: &[u8], from: &[u8], to_len: usize) -> bool {
    let mut total = subject.len() as u64;
    let cap = crate::engine::sysvar::builtin::connection::max_allowed_packet_bytes();
    if !from.is_empty() && to_len < from.len() && total > cap {
        let shrink = (from.len() - to_len) as u64;
        let mut rest = subject;
        while let Some(start) = memchr::memmem::find(rest, from) {
            total = total.saturating_sub(shrink);
            if total <= cap {
                return false;
            }
            rest = &rest[start + from.len()..];
        }
    }
    if !from.is_empty() && to_len > from.len() {
        let growth = (to_len - from.len()) as u64;
        // Normal small results retain the single-pass replacement kernel.
        if total.saturating_add(((subject.len() / from.len()) as u64).saturating_mul(growth)) > cap
        {
            let mut rest = subject;
            while let Some(start) = memchr::memmem::find(rest, from) {
                total = total.saturating_add(growth);
                if total > cap {
                    return string_result_exceeds_packet("replace", total);
                }
                rest = &rest[start + from.len()..];
            }
        }
    }
    string_result_exceeds_packet("replace", total)
}
