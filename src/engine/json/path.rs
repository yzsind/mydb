//! MySQL JSON path expressions, evaluated directly against `jsonb` bytes.
//!
//! Supported syntax (a practical superset of the previous text engine):
//!
//! ```text
//!   $                 the document root
//!   .name             member by key (unquoted)
//!   ."quoted name"    member by key (quoted; supports \" and \\)
//!   .*                all members of an object
//!   [N]               array element N
//!   [last]            last array element
//!   [last - N]        N-from-last array element
//!   [M to N]          inclusive array range (either bound may use `last`)
//!   [*]               all elements of an array
//!   **                recursive descent (must be followed by another leg)
//! ```
//!
//! Evaluation returns every matching node. A path that contains a wildcard
//! or `**` is flagged via [`JsonPath::has_wildcard`] so callers can apply
//! MySQL's "auto-wrap multiple matches into an array" rule.

use super::json_err;
use super::jsonb::JsonbRef;
use crate::types::QueryError;

#[derive(Debug, Clone)]
enum Leg {
    Member(String),
    MemberWild,
    Index(IdxSpec),
    Range(IdxSpec, IdxSpec),
    IndexWild,
    Recursive,
}

#[derive(Debug, Clone, Copy)]
enum IdxSpec {
    FromStart(usize),
    FromLast(usize),
}

/// A concrete (wildcard-free) navigation leg. The mutators
/// (JSON_SET/INSERT/REPLACE/REMOVE/ARRAY_APPEND/ARRAY_INSERT), JSON_CONTAINS
/// and JSON_VALUE require a path that addresses at most one location; MySQL
/// rejects `*` / `**` / `[*]` there with error 3149.
#[derive(Debug, Clone)]
pub(crate) enum ConcreteLeg {
    /// `.key` / `."quoted key"`.
    Member(String),
    /// `[N]` counted from the start of the array.
    Index(usize),
    /// `[last]` (offset 0) / `[last - N]` counted from the end.
    IndexFromLast(usize),
}

#[derive(Debug, Clone)]
pub(crate) struct JsonPath {
    legs: Vec<Leg>,
    pub(crate) has_wildcard: bool,
}

impl JsonPath {
    /// Parse a MySQL JSON path. Returns error 3143 on malformed input,
    /// matching MySQL's "Invalid JSON path expression" error.
    pub(crate) fn parse(path: &str) -> Result<JsonPath, QueryError> {
        let bytes = path.trim().as_bytes();
        if bytes.first() != Some(&b'$') {
            return Err(invalid_path(path));
        }
        let mut legs = Vec::new();
        let mut has_wildcard = false;
        let mut i = 1;
        let n = bytes.len();
        while i < n {
            match bytes[i] {
                b' ' | b'\t' | b'\n' | b'\r' => {
                    i += 1;
                }
                b'.' => {
                    i += 1;
                    skip_ws(bytes, &mut i);
                    if i >= n {
                        return Err(invalid_path(path));
                    }
                    if bytes[i] == b'*' {
                        legs.push(Leg::MemberWild);
                        has_wildcard = true;
                        i += 1;
                    } else if bytes[i] == b'"' {
                        let key = parse_quoted_key(bytes, &mut i, path)?;
                        legs.push(Leg::Member(key));
                    } else {
                        let start = i;
                        while i < n && !matches!(bytes[i], b'.' | b'[' | b'*' | b' ' | b'\t') {
                            i += 1;
                        }
                        if i == start {
                            return Err(invalid_path(path));
                        }
                        let key = std::str::from_utf8(&bytes[start..i])
                            .map_err(|_| invalid_path(path))?
                            .to_string();
                        if !valid_unquoted_member(&key) {
                            return Err(invalid_path_at(path, start));
                        }
                        legs.push(Leg::Member(key));
                    }
                }
                b'[' => {
                    i += 1;
                    skip_ws(bytes, &mut i);
                    if i < n && bytes[i] == b'*' {
                        i += 1;
                        skip_ws(bytes, &mut i);
                        if i >= n || bytes[i] != b']' {
                            return Err(invalid_path(path));
                        }
                        i += 1;
                        legs.push(Leg::IndexWild);
                        has_wildcard = true;
                    } else {
                        let first = parse_index_spec(bytes, &mut i, path)?;
                        skip_ws(bytes, &mut i);
                        if consume_keyword(bytes, &mut i, b"to") {
                            skip_ws(bytes, &mut i);
                            let last = parse_index_spec(bytes, &mut i, path)?;
                            skip_ws(bytes, &mut i);
                            if range_is_statically_descending(first, last) {
                                return Err(invalid_path_at(path, i));
                            }
                            expect(bytes, &mut i, b']', path)?;
                            legs.push(Leg::Range(first, last));
                            has_wildcard = true;
                        } else {
                            expect(bytes, &mut i, b']', path)?;
                            legs.push(Leg::Index(first));
                        }
                    }
                }
                b'*' => {
                    // `**` recursive descent.
                    if i + 1 < n && bytes[i + 1] == b'*' {
                        legs.push(Leg::Recursive);
                        has_wildcard = true;
                        i += 2;
                    } else {
                        return Err(invalid_path(path));
                    }
                }
                _ => return Err(invalid_path(path)),
            }
        }
        // MySQL forbids a path that ends in `**`.
        if matches!(legs.last(), Some(Leg::Recursive)) {
            return Err(invalid_path(path));
        }
        Ok(JsonPath { legs, has_wildcard })
    }

    /// All nodes matching the path under `root`, in document order.
    pub(crate) fn eval<'a>(&self, root: JsonbRef<'a>) -> Vec<JsonbRef<'a>> {
        let mut out = Vec::new();
        let mut seen = self
            .legs
            .iter()
            .any(|leg| matches!(leg, Leg::Recursive))
            .then(std::collections::HashSet::new);
        let mut stack = vec![(root, 0usize)];
        while let Some((node, pos)) = stack.pop() {
            let Some(leg) = self.legs.get(pos) else {
                if seen
                    .as_mut()
                    .is_none_or(|seen| seen.insert(node.identity()))
                {
                    out.push(node);
                }
                continue;
            };
            let next = pos + 1;
            match leg {
                Leg::Member(key) => {
                    if let Some(child) = node.get(key) {
                        stack.push((child, next));
                    }
                }
                Leg::MemberWild => {
                    let children = node
                        .object_iter()
                        .map(|(_, value)| value)
                        .collect::<Vec<_>>();
                    stack.extend(children.into_iter().rev().map(|child| (child, next)));
                }
                Leg::Index(spec) => {
                    if let Some(child) = resolve_index(node, *spec) {
                        stack.push((child, next));
                    }
                }
                Leg::Range(first, last) => {
                    let len = if node.is_array() {
                        node.container_len().unwrap_or(0)
                    } else {
                        1
                    };
                    if let Some((start, end)) = resolve_range(*first, *last, len) {
                        if node.is_array() {
                            let children = node
                                .array_iter()
                                .skip(start)
                                .take(end - start + 1)
                                .collect::<Vec<_>>();
                            stack.extend(children.into_iter().rev().map(|child| (child, next)));
                        } else if start == 0 {
                            stack.push((node, next));
                        }
                    }
                }
                Leg::IndexWild => {
                    if node.is_array() {
                        let children = node.array_iter().collect::<Vec<_>>();
                        stack.extend(children.into_iter().rev().map(|child| (child, next)));
                    }
                }
                Leg::Recursive => {
                    // Try the remaining path at this node first, then revisit
                    // the recursive leg for each strict child. Explicit state
                    // avoids Rust stack growth for deeply nested documents or
                    // long paths.
                    let children = direct_children(node);
                    stack.extend(children.into_iter().rev().map(|child| (child, pos)));
                    stack.push((node, next));
                }
            }
        }
        out
    }

    /// The path as concrete navigation legs, or error 3149 if it contains a
    /// wildcard / recursive-descent token. Used by the single-location
    /// operations (mutators, JSON_CONTAINS, JSON_VALUE) that MySQL forbids
    /// from using `*`, `**`, or `[*]`.
    pub(crate) fn concrete_legs(&self) -> Result<Vec<ConcreteLeg>, QueryError> {
        let mut out = Vec::with_capacity(self.legs.len());
        for leg in &self.legs {
            out.push(match leg {
                Leg::Member(key) => ConcreteLeg::Member(key.clone()),
                Leg::Index(IdxSpec::FromStart(i)) => ConcreteLeg::Index(*i),
                Leg::Index(IdxSpec::FromLast(off)) => ConcreteLeg::IndexFromLast(*off),
                Leg::MemberWild | Leg::IndexWild | Leg::Range(_, _) | Leg::Recursive => {
                    return Err(json_err(
                        crate::error::catalog::ER_INVALID_JSON_PATH_WILDCARD,
                        "In this situation, path expressions may not contain the * and ** \
                         tokens or an array range.",
                    ));
                }
            });
        }
        Ok(out)
    }
}

fn direct_children(node: JsonbRef<'_>) -> Vec<JsonbRef<'_>> {
    if node.is_array() {
        node.array_iter().collect()
    } else if node.is_object() {
        node.object_iter().map(|(_, value)| value).collect()
    } else {
        Vec::new()
    }
}

fn resolve_index(node: JsonbRef<'_>, spec: IdxSpec) -> Option<JsonbRef<'_>> {
    let len = node.container_len().filter(|_| node.is_array());
    match (spec, len) {
        (IdxSpec::FromStart(idx), Some(arr_len)) => node.index(idx).filter(|_| idx < arr_len),
        (IdxSpec::FromLast(off), Some(arr_len)) => arr_len
            .checked_sub(1)
            .and_then(|last| last.checked_sub(off))
            .and_then(|idx| node.index(idx)),
        (IdxSpec::FromStart(0), None) | (IdxSpec::FromLast(0), None) => Some(node),
        _ => None,
    }
}

fn parse_quoted_key(bytes: &[u8], i: &mut usize, path: &str) -> Result<String, QueryError> {
    // Let serde's RFC 8259 string decoder own escapes, including `\uXXXX`
    // surrogate pairs. The path grammar uses the same double-quoted string
    // representation; sharing the decoder also rejects unknown escapes.
    let start = *i;
    *i += 1;
    let mut escaped = false;
    while *i < bytes.len() {
        let c = bytes[*i];
        *i += 1;
        if escaped {
            escaped = false;
        } else if c == b'\\' {
            escaped = true;
        } else if c == b'"' {
            return serde_json::from_slice::<String>(&bytes[start..*i])
                .map_err(|_| invalid_path_at(path, start));
        }
    }
    Err(invalid_path(path))
}

fn parse_index_spec(bytes: &[u8], i: &mut usize, path: &str) -> Result<IdxSpec, QueryError> {
    skip_ws(bytes, i);
    // Optional `last` keyword.
    if bytes[*i..].len() >= 4 && bytes[*i..*i + 4].eq_ignore_ascii_case(b"last") {
        *i += 4;
        skip_ws(bytes, i);
        let mut off = 0usize;
        if *i < bytes.len() && bytes[*i] == b'-' {
            *i += 1;
            skip_ws(bytes, i);
            off = parse_uint(bytes, i, path)?;
            skip_ws(bytes, i);
        }
        return Ok(IdxSpec::FromLast(off));
    }
    let v = parse_uint(bytes, i, path)?;
    Ok(IdxSpec::FromStart(v))
}

fn consume_keyword(bytes: &[u8], i: &mut usize, keyword: &[u8]) -> bool {
    let end = (*i).saturating_add(keyword.len());
    if end <= bytes.len() && bytes[*i..end].eq_ignore_ascii_case(keyword) {
        *i = end;
        true
    } else {
        false
    }
}

fn valid_unquoted_member(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first == '$' || first.is_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch == '$' || ch.is_alphanumeric())
}

fn range_is_statically_descending(first: IdxSpec, last: IdxSpec) -> bool {
    match (first, last) {
        (IdxSpec::FromStart(a), IdxSpec::FromStart(b)) => a > b,
        // A larger last-relative offset addresses an earlier element.
        (IdxSpec::FromLast(a), IdxSpec::FromLast(b)) => a < b,
        _ => false,
    }
}

fn resolve_range(first: IdxSpec, last: IdxSpec, len: usize) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let resolve_start = |spec| match spec {
        IdxSpec::FromStart(idx) => (idx < len).then_some(idx),
        IdxSpec::FromLast(off) => Some(len.saturating_sub(1).saturating_sub(off)),
    };
    let resolve_end = |spec| match spec {
        IdxSpec::FromStart(idx) => Some(idx.min(len - 1)),
        IdxSpec::FromLast(off) => Some(len.saturating_sub(1).saturating_sub(off)),
    };
    let start = resolve_start(first)?;
    let end = resolve_end(last)?;
    (start <= end).then_some((start, end))
}

fn parse_uint(bytes: &[u8], i: &mut usize, path: &str) -> Result<usize, QueryError> {
    let start = *i;
    while *i < bytes.len() && bytes[*i].is_ascii_digit() {
        *i += 1;
    }
    if *i == start {
        return Err(invalid_path(path));
    }
    std::str::from_utf8(&bytes[start..*i])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or_else(|| invalid_path(path))
}

fn expect(bytes: &[u8], i: &mut usize, want: u8, path: &str) -> Result<(), QueryError> {
    if *i < bytes.len() && bytes[*i] == want {
        *i += 1;
        Ok(())
    } else {
        Err(invalid_path(path))
    }
}

#[inline]
fn skip_ws(bytes: &[u8], i: &mut usize) {
    while *i < bytes.len() && matches!(bytes[*i], b' ' | b'\t' | b'\n' | b'\r') {
        *i += 1;
    }
}

fn invalid_path(path: &str) -> QueryError {
    invalid_path_at(path, 1)
}

fn invalid_path_at(path: &str, byte_pos: usize) -> QueryError {
    let char_pos = path[..byte_pos.min(path.len())].chars().count();
    json_err(
        crate::error::catalog::ER_INVALID_JSON_PATH,
        format!(
            "Invalid JSON path expression. The error is around character position {char_pos} in '{path}'."
        ),
    )
}
