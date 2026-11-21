//! UUID column domain.
//!
//! MySQL has no native UUID column type. This module is the mydb extension:
//! a value is the 16-byte identity used by `UUID_TO_BIN` with `swap_flag = 0`,
//! and text input uses the same grammar as `IS_UUID` / `UUID_TO_BIN`.
use crate::types::Value;

/// Canonical lowercase `8-4-4-4-12` text.
pub(crate) fn format_text(bytes: &[u8; 16]) -> String {
    crate::util::uuid::format(bytes)
}

/// Identity accepted for assignment and for comparison with a UUID value.
///
/// Text follows [`crate::engine::expr::network::parse_uuid_bytes`] (canonical
/// hyphenated form, 32 hex digits, or a braced canonical form; any hex case).
/// A 16-byte binary value is the identity itself, which is what
/// `UUID_TO_BIN(expr)` returns when the swap flag is zero.
pub(crate) fn identity(value: &Value) -> Option<[u8; 16]> {
    match value {
        Value::Uuid(bytes) => Some(*bytes),
        Value::Bytes(bytes) if bytes.len() == 16 => bytes.as_slice().try_into().ok(),
        Value::String(text) => crate::engine::expr::network::parse_uuid_bytes(text),
        Value::SmallString(bytes) => {
            let text = std::str::from_utf8(bytes).ok()?;
            crate::engine::expr::network::parse_uuid_bytes(text)
        }
        Value::Decimal(text) => crate::engine::expr::network::parse_uuid_bytes(text),
        _ => None,
    }
}

pub(crate) fn coerce_assignment(
    value: Value,
    column: &str,
) -> Result<Value, crate::error::QueryError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let bytes = identity(&value).ok_or_else(|| {
        crate::storage::error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
            format!(
                "incorrect uuid value: '{}' for column '{column}'",
                preview(&value)
            ),
        )
    })?;
    Ok(Value::Uuid(bytes))
}

/// Order two values when at least one is a UUID.
///
/// Both sides that parse as the same identity compare by those 16 bytes.
/// A non-UUID operand is ordered against the canonical text of the UUID.
pub(crate) fn compare(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (identity(left), identity(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        _ => {
            let left = display(left);
            let right = display(right);
            left.cmp(&right)
        }
    }
}

fn display(value: &Value) -> std::borrow::Cow<'_, str> {
    match value {
        Value::Uuid(bytes) => std::borrow::Cow::Owned(format_text(bytes)),
        Value::String(text) => std::borrow::Cow::Borrowed(text),
        Value::SmallString(bytes) => {
            std::borrow::Cow::Borrowed(std::str::from_utf8(bytes).unwrap_or(""))
        }
        other => std::borrow::Cow::Owned(other.to_display_string()),
    }
}

fn preview(value: &Value) -> String {
    let text = match value {
        Value::Uuid(bytes) => return format_text(bytes),
        Value::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => other.to_display_string(),
    };
    let mut chars = text.chars();
    let head: String = chars.by_ref().take(64).collect();
    if chars.next().is_some() {
        format!("{head}...")
    } else {
        head
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_spellings_share_one_identity() {
        let canonical = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
        let expected = identity(&Value::String(canonical.into())).unwrap();
        for spelling in [
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
            "a0eebc999c0b4ef8bb6d6bb9bd380a11",
            "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
        ] {
            let parsed = identity(&Value::String(spelling.into())).unwrap();
            assert_eq!(parsed, expected, "{spelling}");
        }
        assert_eq!(
            identity(&Value::Bytes(expected.to_vec())).unwrap(),
            expected
        );
        assert_eq!(format_text(&expected), canonical);
        assert!(identity(&Value::String("not-a-uuid".into())).is_none());
        assert!(identity(&Value::String(
            " a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".into()
        ))
        .is_none());
        assert!(identity(&Value::Bytes(vec![1, 2, 3])).is_none());
    }
}
