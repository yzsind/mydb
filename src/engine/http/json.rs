//! Strict bounded JSON decoding for HTTP options and node policy.
use super::error;
use crate::types::QueryError;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::Value as Json;
use std::fmt;
/// Enforce duplicate-key and nesting limits before creating a JSON DOM.
pub(crate) fn parse_json(input: &str, max: usize) -> Result<Json, QueryError> {
    if input.len() > max {
        return Err(error("ER_HTTP_INVALID_ARGUMENT", "JSON exceeds byte limit"));
    }
    struct Strict(Json);
    impl<'de> Deserialize<'de> for Strict {
        fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = Strict;
                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    f.write_str("bounded JSON")
                }
                fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Strict, E> {
                    serde_json::Number::from_f64(v)
                        .map(|n| Strict(Json::Number(n)))
                        .ok_or_else(|| E::custom("non-finite number"))
                }
                fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_unit<E: serde::de::Error>(self) -> Result<Strict, E> {
                    Ok(Strict(Json::Null))
                }
                fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                    let mut v = Vec::new();
                    while let Some(x) = a.next_element::<Strict>()? {
                        v.push(x.0);
                    }
                    Ok(Strict(Json::Array(v)))
                }
                fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                    let mut m = serde_json::Map::new();
                    while let Some(k) = a.next_key::<String>()? {
                        if m.contains_key(&k) {
                            return Err(serde::de::Error::custom("duplicate JSON key"));
                        }
                        let v = a.next_value::<Strict>()?;
                        m.insert(k, v.0);
                    }
                    Ok(Strict(Json::Object(m)))
                }
            }
            de.deserialize_any(V)
        }
    }
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
