//! High-performance binary JSON ("jsonb") for the MySQL-compatible JSON
//! data type.
//!
//! MySQL stores JSON on disk in a binary form and only serialises to text
//! when handing a value to a client. mydb follows the same model:
//!
//!   * On disk and inside [`crate::types::Value::Json`] a document is held
//!     as the compact, self-describing `jsonb` byte string defined in
//!     [`jsonb`]. Reading a column off the heap therefore costs *zero*
//!     JSON parsing — the bytes are already navigable.
//!   * The read / scan / compare hot paths ([`extract`], [`compare`],
//!     [`type_of`], length, keys, contains) walk the binary directly with
//!     no intermediate DOM.
//!   * Structural mutators edit a transient lossless tree whose scalar leaves
//!     remain encoded JSONB, so rich DECIMAL and temporal tags survive changes.
//!
//! Text is parsed exactly once, at write time (input is text); everything
//! afterwards operates on bytes. This is the core "high-performance JSON
//! data type" win over the previous "re-parse the whole document on every
//! function call" approach.

mod compare;
mod jsonb;
pub(crate) mod mysql_binary;
mod path;

#[cfg(test)]
mod tests;

pub(crate) use compare::{compare, compare_nodes, equality_key};
pub(crate) use jsonb::{build_array, build_object, JsonType, JsonbRef};
pub(crate) use path::{ConcreteLeg, JsonPath};

use crate::types::QueryError;

/// Build a [`QueryError`] for a JSON failure. Centralised so the SQLSTATE
/// and error-code conventions stay consistent with MySQL across the module.
#[inline]
pub(crate) fn json_err(spec: crate::error::ErrorSpec, message: impl Into<String>) -> QueryError {
    spec.error(message)
}

/// MySQL error 3141: invalid JSON text supplied to a function argument.
pub(crate) fn invalid_json_text(arg_pos: usize, func: &str) -> QueryError {
    json_err(
        crate::error::catalog::ER_INVALID_JSON_TEXT_IN_PARAM,
        format!("Invalid JSON text in argument {arg_pos} to function {func}."),
    )
}

/// A JSON document permits 100 container levels plus one scalar leaf level.
/// Deeper containers raise ER_JSON_DOCUMENT_TOO_DEEP (3157).
pub(crate) const MAX_JSON_DEPTH: usize = 101;

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_decode_jsonb(bytes: &[u8]) {
    let Ok(document) = JsonbRef::new(bytes) else {
        return;
    };
    if document.validate().is_err() {
        return;
    }
    let _ = document.to_owned_bytes();
    let _ = document.depth();
    let _ = document.to_serde();
    let mut text = String::new();
    document.write_text(&mut text);
}

/// Stack-based (never recursive) depth measurement for a serde DOM.
fn serde_max_depth(root: &serde_json::Value) -> usize {
    let mut stack: Vec<(&serde_json::Value, usize)> = vec![(root, 1)];
    let mut max_depth = 1usize;
    while let Some((node, d)) = stack.pop() {
        if d > max_depth {
            max_depth = d;
        }
        // Bail early once we know we've already exceeded MySQL's cap: no
        // subsequent child can lower the maximum, and continuing would just
        // burn cycles on a document we're already going to reject.
        if max_depth > MAX_JSON_DEPTH {
            return max_depth;
        }
        match node {
            serde_json::Value::Array(a) => {
                if d >= MAX_JSON_DEPTH {
                    return MAX_JSON_DEPTH + 1;
                }
                for e in a {
                    stack.push((e, d + 1));
                }
            }
            serde_json::Value::Object(o) => {
                if d >= MAX_JSON_DEPTH {
                    return MAX_JSON_DEPTH + 1;
                }
                for (_, v) in o {
                    stack.push((v, d + 1));
                }
            }
            _ => {}
        }
    }
    max_depth
}

pub(crate) fn json_too_deep() -> QueryError {
    json_err(
        crate::error::catalog::ER_JSON_DOCUMENT_TOO_DEEP,
        "The JSON document exceeds the maximum depth of 100 nesting levels.",
    )
}

// ---------------------------------------------------------------------------
// Unsigned LEB128 varint — used for all length prefixes in the format. Kept
// private to the json module so the format is fully self-contained.
// ---------------------------------------------------------------------------

#[inline]
pub(crate) fn write_uvarint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            out.push(byte | 0x80);
        } else {
            out.push(byte);
            return;
        }
    }
}

#[inline]
pub(crate) fn uvarint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

/// Read an unsigned LEB128 varint at `*pos`, advancing `*pos` past it.
#[inline]
pub(crate) fn read_uvarint(b: &[u8], pos: &mut usize) -> Result<u64, QueryError> {
    let mut shift = 0u32;
    let mut out = 0u64;
    loop {
        let byte = *b.get(*pos).ok_or_else(|| {
            json_err(
                crate::error::catalog::ER_INVALID_JSON_TEXT,
                "truncated jsonb varint",
            )
        })?;
        *pos += 1;
        out |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(out);
        }
        shift += 7;
        if shift >= 64 {
            return Err(json_err(
                crate::error::catalog::ER_INVALID_JSON_TEXT,
                "overlong jsonb varint",
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Public facade — the surface the rest of the engine uses.
// ---------------------------------------------------------------------------

/// Parse JSON text into a `jsonb` byte string. Returns a MySQL-style
/// `error 3141` (mentioning `func` / `arg_pos`) on invalid input.
pub(crate) fn parse_text(text: &str, arg_pos: usize, func: &str) -> Result<Vec<u8>, QueryError> {
    let dom = parse_dom(text, |_| invalid_json_text(arg_pos, func))?;
    encode_document(text, &dom)
}

/// Keep wide integer tokens exact instead of staging them through serde's
/// f64 fallback. Ordinary JSON keeps the existing single-parse fast path.
fn encode_document(text: &str, dom: &serde_json::Value) -> Result<Vec<u8>, QueryError> {
    fn has_wide_integer(text: &str) -> bool {
        let bytes = text.as_bytes();
        let (mut i, mut quoted, mut escaped) = (0, false, false);
        while i < bytes.len() {
            let byte = bytes[i];
            if quoted {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    quoted = false;
                }
            } else if byte == b'"' {
                quoted = true;
            } else if byte == b'-' || byte.is_ascii_digit() {
                let start = i;
                i += 1;
                while i < bytes.len()
                    && matches!(bytes[i], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
                {
                    i += 1;
                }
                let token = &text[start..i];
                if !token.contains(['.', 'e', 'E'])
                    && token.parse::<i64>().is_err()
                    && token.parse::<u64>().is_err()
                {
                    return true;
                }
                continue;
            }
            i += 1;
        }
        false
    }
    fn encode_raw(text: &str) -> Result<Vec<u8>, QueryError> {
        let text = text.trim();
        let invalid = |_| invalid_json_text(1, "JSON");
        match text.as_bytes().first() {
            Some(b'[') => {
                let values: Vec<Box<serde_json::value::RawValue>> =
                    serde_json::from_str(text).map_err(invalid)?;
                let nodes = values
                    .iter()
                    .map(|v| encode_raw(v.get()))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(build_array(&nodes))
            }
            Some(b'{') => {
                let values: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
                    serde_json::from_str(text).map_err(invalid)?;
                let pairs = values
                    .into_iter()
                    .map(|(key, value)| Ok((key, encode_raw(value.get())?)))
                    .collect::<Result<Vec<_>, QueryError>>()?;
                Ok(build_object(&pairs))
            }
            Some(b'-' | b'0'..=b'9')
                if !text.contains(['.', 'e', 'E'])
                    && text.parse::<i64>().is_err()
                    && text.parse::<u64>().is_err() =>
            {
                let mut out = Vec::with_capacity(text.len() + 6);
                jsonb::write_text_scalar(jsonb::T_DECIMAL, text, &mut out);
                Ok(out)
            }
            _ => Ok(from_serde(&parse_dom(text, invalid)?)),
        }
    }
    if has_wide_integer(text) {
        encode_raw(text)
    } else {
        Ok(from_serde(dom))
    }
}

fn normalize_integer_negative_zero(text: &str) -> std::borrow::Cow<'_, str> {
    if !text.contains("-0") {
        return std::borrow::Cow::Borrowed(text);
    }
    let bytes = text.as_bytes();
    let mut normalized = None;
    let (mut quoted, mut escaped) = (false, false);
    for (i, &byte) in bytes.iter().enumerate() {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else if byte == b'"' {
            quoted = true;
        } else if byte == b'-'
            && bytes.get(i + 1) == Some(&b'0')
            && bytes
                .get(i + 2)
                .is_none_or(|b| matches!(b, b',' | b']' | b'}' | b' ' | b'\n' | b'\r' | b'\t'))
        {
            normalized.get_or_insert_with(|| bytes.to_vec())[i] = b' ';
        }
    }
    match normalized {
        Some(bytes) => std::borrow::Cow::Owned(
            String::from_utf8(bytes).expect("ASCII replacement preserves UTF-8"),
        ),
        None => std::borrow::Cow::Borrowed(text),
    }
}

/// Keep serde's bounded parser stack, classifying depth failures separately.
fn parse_dom(
    text: &str,
    invalid: impl FnOnce(serde_json::Error) -> QueryError,
) -> Result<serde_json::Value, QueryError> {
    let normalized = normalize_integer_negative_zero(text);
    let dom = serde_json::from_str(&normalized).map_err(|err| {
        if err.to_string().starts_with("recursion limit exceeded") {
            json_too_deep()
        } else {
            invalid(err)
        }
    })?;
    if serde_max_depth(&dom) > MAX_JSON_DEPTH {
        return Err(json_too_deep());
    }
    Ok(dom)
}

pub(crate) fn parse_document(
    text: &str,
    arg: usize,
    func: &str,
) -> Result<serde_json::Value, QueryError> {
    parse_dom(text, |_| invalid_json_text(arg, func))
}

pub(crate) fn binary_charset_error() -> QueryError {
    crate::error::catalog::ER_INVALID_JSON_CHARSET
        .error("Cannot create a JSON value from a string with CHARACTER SET 'binary'.")
}

/// Shared by assignment and direct row encoding, including bulk import.
/// Already encoded JSON is borrowed without allocating or reparsing.
pub(crate) fn column_value<'a>(
    value: &'a crate::types::Value,
    column: &str,
) -> Result<std::borrow::Cow<'a, [u8]>, QueryError> {
    use crate::types::Value;
    use std::borrow::Cow;
    let text = match value {
        Value::Json(bytes) => return Ok(Cow::Borrowed(bytes)),
        Value::String(text) => text.as_str(),
        Value::SmallString(text) => {
            std::str::from_utf8(text).map_err(|_| binary_charset_error())?
        }
        Value::Bytes(_) => return Err(binary_charset_error()),
        _ => {
            return Err(crate::error::catalog::ER_INVALID_JSON_TEXT.error(format!(
                "Invalid JSON text: not a JSON text, may need CAST in value for column '{column}'"
            )))
        }
    };
    let dom = parse_dom(text, |err| {
        crate::error::catalog::ER_INVALID_JSON_TEXT.error(format!(
            "Invalid JSON text: {err} in value for column '{column}'"
        ))
    })?;
    Ok(Cow::Owned(encode_document(text, &dom)?))
}

/// Parse JSON text, returning `None` (rather than an error) on invalid
/// input. Used by the `->` / `->>` operators, which yield SQL NULL instead
/// of raising when the left operand is not valid JSON. Documents deeper
/// than [`MAX_JSON_DEPTH`] are treated as invalid (yielding `None`) so a
/// runaway path expression cannot silently accept a document a plain
/// [`parse_text`] would have rejected.
pub(crate) fn parse_text_opt(text: &str) -> Option<Vec<u8>> {
    parse_text(text, 1, "JSON path").ok()
}

/// Encode a [`serde_json::Value`] DOM into `jsonb` bytes.
pub(crate) fn from_serde(value: &serde_json::Value) -> Vec<u8> {
    jsonb::encode(value)
}

/// Decode `jsonb` bytes into a [`serde_json::Value`] DOM. Used by the
/// structural mutators that are easier to express on a tree.
pub(crate) fn to_serde(jsonb: &[u8]) -> Result<serde_json::Value, QueryError> {
    JsonbRef::new(jsonb)?.to_serde()
}

/// Serialise `jsonb` to MySQL's canonical compact text form
/// (`{"a": 1, "b": 2}` — space after `:` and `,`, object keys in
/// (length, byte) order).
pub(crate) fn to_text(jsonb: &[u8]) -> String {
    let mut out = String::new();
    to_text_into(jsonb, &mut out);
    out
}

/// Serialise `jsonb` to canonical JSON text, reusing the caller's buffer
/// (cleared first). Lets the result-set row encoder avoid one `String`
/// allocation per JSON cell by threading a scratch buffer across rows.
pub(crate) fn to_text_into(jsonb: &[u8], out: &mut String) {
    out.clear();
    if let Ok(r) = JsonbRef::new(jsonb) {
        r.write_text(out);
    }
}

/// Serialise `jsonb` to MySQL's `JSON_PRETTY` multi-line indented form.
pub(crate) fn to_pretty(jsonb: &[u8]) -> String {
    let mut out = String::new();
    if let Ok(r) = JsonbRef::new(jsonb) {
        r.write_pretty(&mut out, 0);
    }
    out
}

/// `JSON_UNQUOTE` semantics: a top-level JSON string yields its inner text
/// (unescaped); any other value yields its canonical JSON text. Also the
/// coercion used when a JSON document flows into a character column or the
/// `->>` operator.
pub(crate) fn unquote_text(jsonb: &[u8]) -> String {
    match JsonbRef::new(jsonb) {
        Ok(r) if r.opaque_value().is_some() => {
            let (ty, bytes) = r.opaque_value().expect("checked opaque value");
            opaque_text(ty, bytes)
        }
        Ok(r) => match r.as_str() {
            Some(s) => s.to_string(),
            None => {
                let mut out = String::new();
                r.write_text(&mut out);
                out
            }
        },
        Err(_) => String::new(),
    }
}

/// Numeric coercion of a JSON document (`CAST(json AS DOUBLE)` style): a
/// number yields its value; a string is parsed; a boolean yields 0/1; any
/// other value yields 0.0.
pub(crate) fn coerce_f64(jsonb: &[u8]) -> f64 {
    match JsonbRef::new(jsonb) {
        Ok(r) => {
            if let Some(f) = r.as_f64() {
                f
            } else if let Some(s) = r.as_str() {
                s.trim().parse().unwrap_or(0.0)
            } else if let Some(b) = r.as_bool() {
                if b {
                    1.0
                } else {
                    0.0
                }
            } else {
                0.0
            }
        }
        Err(_) => 0.0,
    }
}

/// Integer coercion of a JSON document (`CAST(json AS SIGNED)` style),
/// truncating toward zero like MySQL.
pub(crate) fn coerce_i64(jsonb: &[u8]) -> i64 {
    match JsonbRef::new(jsonb) {
        Ok(r) => {
            if let Some(i) = r.as_i64() {
                i
            } else if let Some(unsigned) = r.as_u64() {
                unsigned as i64
            } else if let Some(f) = r.as_f64() {
                f as i64
            } else if let Some(s) = r.as_str() {
                s.trim().parse().unwrap_or(0)
            } else if let Some(b) = r.as_bool() {
                i64::from(b)
            } else {
                0
            }
        }
        Err(_) => 0,
    }
}

/// Wrap a non-JSON [`Value`](crate::types::Value) as a JSON scalar document,
/// used when a SQL value flows into a JSON context (CAST … AS JSON on a
/// non-string, JSON_ARRAY element, comparison coercion). Strings are *not*
/// parsed here — that is the caller's choice via [`parse_text`].
pub(crate) fn scalar_from_value(value: &crate::types::Value) -> Vec<u8> {
    use crate::types::Value;
    // Decimal and temporal values encode under their own additive tags so the
    // JSON type and full precision survive (JSON_TYPE → DECIMAL / DATE / TIME /
    // DATETIME, matching MySQL). Their layout is a length-prefixed canonical
    // text, identical to a string node on disk.
    let tagged = match value {
        Value::Decimal(_)
        | Value::DecimalCompact(..)
        | Value::Decimal128(..)
        | Value::DecimalWide(..) => Some(jsonb::T_DECIMAL),
        Value::Date(_) => Some(jsonb::T_DATE),
        Value::Time(_) => Some(jsonb::T_TIME),
        Value::DateTime(_) | Value::Timestamp(_) => Some(jsonb::T_DATETIME),
        _ => None,
    };
    if let Some(tag) = tagged {
        let mut out = Vec::new();
        let mut text = match value {
            Value::Timestamp(micros) => {
                match crate::engine::expr::session_utc_timestamp_to_local_datetime_micros(*micros) {
                    Ok(local) => Value::DateTime(local).to_display_string(),
                    Err(err) => {
                        crate::engine::api::push_statement_condition(
                            crate::error::ConditionLevel::Warning,
                            &err,
                        );
                        return from_serde(&serde_json::Value::Null);
                    }
                }
            }
            _ => value.to_display_string(),
        };
        if matches!(
            value,
            Value::DateTime(_) | Value::Timestamp(_) | Value::Time(_)
        ) {
            if let Some(dot) = text.rfind('.') {
                let digits = text.len() - dot - 1;
                text.extend(std::iter::repeat_n('0', 6usize.saturating_sub(digits)));
            } else {
                text.push_str(".000000");
            }
        }
        jsonb::write_text_scalar(tag, &text, &mut out);
        return out;
    }
    let dom = match value {
        Value::Null => serde_json::Value::Null,
        Value::Int(v) => serde_json::Value::Number((*v).into()),
        Value::UInt(v) => serde_json::Value::Number((*v).into()),
        Value::Float(v) => serde_json::Number::from_f64(f64::from(*v))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Double(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Bytes(b) => return opaque_scalar(15, b),
        Value::Json(b) => return b.clone(),
        // Everything else (YEAR, geometry, vector, …) becomes a JSON string of
        // its canonical text.
        other => serde_json::Value::String(other.to_display_string()),
    };
    from_serde(&dom)
}

/// Source-aware SQL→JSON conversion. MySQL parses character strings as JSON
/// text, but ENUM/SET/BIT and binary-string families are SQL scalars whose
/// display bytes must be wrapped rather than reparsed as a document.
pub(crate) fn scalar_from_typed_value(
    value: &crate::types::Value,
    source: &crate::engine::expr::ExprTypeDescriptor,
) -> Vec<u8> {
    use crate::engine::expr::ExprTypeClass;
    match source.class {
        ExprTypeClass::Enum | ExprTypeClass::Set | ExprTypeClass::BinaryString => {
            scalar_from_value(value)
        }
        ExprTypeClass::Bit { .. } => {
            let unsigned = match value {
                crate::types::Value::UInt(v) => *v,
                crate::types::Value::Int(v) => *v as u64,
                other => other.to_display_string().parse::<u64>().unwrap_or(0),
            };
            let mut out = Vec::with_capacity(9);
            out.push(jsonb::T_U64);
            out.extend_from_slice(&unsigned.to_le_bytes());
            out
        }
        _ => scalar_from_value(value),
    }
}

/// Binary SQL strings retain every octet when represented in JSON text.
pub(crate) fn binary_json_text(bytes: &[u8]) -> String {
    use base64::Engine;
    format!(
        "base64:type15:{}",
        base64::engine::general_purpose::STANDARD.encode(bytes)
    )
}

/// Encode a validated rich scalar supplied by the binary-JSON receiver.
/// Keep the tag rather than passing through a JSON-text/serde conversion.
pub(crate) fn typed_text_scalar(ty: JsonType, text: &str) -> Result<Vec<u8>, QueryError> {
    let tag = match ty {
        JsonType::Date => jsonb::T_DATE,
        JsonType::Time => jsonb::T_TIME,
        JsonType::Datetime => jsonb::T_DATETIME,
        JsonType::Timestamp => jsonb::T_TIMESTAMP,
        _ => return Err(invalid_json_text(1, "replication typed scalar")),
    };
    let mut out = Vec::with_capacity(text.len() + 2);
    jsonb::write_text_scalar(tag, text, &mut out);
    Ok(out)
}

/// Preserve the source field type and raw bytes for non-temporal opaque JSON.
pub(crate) fn opaque_scalar(ty: u8, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 6);
    out.push(jsonb::T_OPAQUE);
    write_uvarint(&mut out, bytes.len() as u64 + 1);
    out.push(ty);
    out.extend_from_slice(bytes);
    out
}

pub(crate) fn opaque_text(ty: u8, bytes: &[u8]) -> String {
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
    let mut text = format!("base64:type{ty}:");
    for (i, chunk) in encoded.as_bytes().chunks(76).enumerate() {
        if i != 0 {
            text.push('\n');
        }
        // The encoder produces ASCII exclusively.
        text.push_str(std::str::from_utf8(chunk).expect("base64 is ASCII"));
    }
    text
}
