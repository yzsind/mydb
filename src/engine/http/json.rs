//! Strict bounded JSON decoding for HTTP options and node policy.
use super::error;
use crate::types::QueryError;
use serde::Deserialize;
use serde_json::Value as Json;
/// Enforce duplicate-key and nesting limits before creating a JSON DOM.
pub(crate) fn parse_json(input: &str, max: usize) -> Result<Json, QueryError> {
    if input.len() > max {
        return Err(error("ER_HTTP_INVALID_ARGUMENT", "JSON exceeds byte limit"));
    }
    crate::util::strict_json::define_strict_value!(Strict);
    // Count lexical nesting before recursively invoking serde.
    let (mut depth, mut quoted, mut escaped) = (0usize, false, false);
    for c in input.bytes() {
        if quoted {
            if escaped {
                escaped = false;
            } else if c == b'\\' {
                escaped = true;
            } else if c == b'"' {
                quoted = false;
            }
        } else {
            match c {
                b'"' => quoted = true,
                b'[' | b'{' => {
                    depth += 1;
                    if depth > 32 {
                        return Err(error("ER_HTTP_INVALID_ARGUMENT", "JSON nesting exceeds 32"));
                    }
                }
                b']' | b'}' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
    }
    let mut de = serde_json::Deserializer::from_str(input);
    let value = Strict::deserialize(&mut de)
        .map_err(|_| {
            error(
                "ER_HTTP_INVALID_ARGUMENT",
                "invalid JSON, duplicate key or unsupported value",
            )
        })?
        .0;
    de.end()
        .map_err(|_| error("ER_HTTP_INVALID_ARGUMENT", "trailing JSON data"))?;
    Ok(value)
}
