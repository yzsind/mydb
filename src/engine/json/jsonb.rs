//! The `jsonb` binary container format and its zero-copy reader.
//!
//! Layout (every node is `tag(u8) || payload`, fully self-describing and
//! O(1)-skippable):
//!
//! ```text
//!   T_NULL  (0)        -> (no payload)
//!   T_TRUE  (1)        -> (no payload)
//!   T_FALSE (2)        -> (no payload)
//!   T_I64   (3)        -> 8B i64 LE
//!   T_U64   (4)        -> 8B u64 LE
//!   T_F64   (5)        -> 8B f64 LE
//!   T_STRING(6)        -> uvarint(len) || len bytes UTF-8
//!   T_ARRAY (7)        -> uvarint(payload_len) || payload
//!                         payload = uvarint(count) || count nodes
//!   T_OBJECT(8)        -> uvarint(payload_len) || payload
//!                         payload = uvarint(count) ||
//!                                   count * ( uvarint(klen) || key || node )
//!   T_DECIMAL(9)       -> uvarint(len) || len bytes  (canonical decimal text)
//!   T_DATE  (10)       -> uvarint(len) || len bytes  ("YYYY-MM-DD")
//!   T_TIME  (11)       -> uvarint(len) || len bytes  ("[-]HH:MM:SS[.ffffff]")
//!   T_DATETIME(12)     -> uvarint(len) || len bytes  ("YYYY-MM-DD HH:MM:SS[.ffffff]")
//!   T_OPAQUE(13)      -> uvarint(len) || mysql_type(u8) || raw bytes
//!   T_TIMESTAMP(14)   -> uvarint(len) || canonical datetime text
//! ```
//!
//! Tags 9-12 are *additive*: they never come from a `serde_json` DOM (which
//! has no decimal / temporal), only from [`super::scalar_from_value`] when a
//! SQL DECIMAL / DATE / TIME / DATETIME value flows into a JSON context, so
//! `JSON_TYPE` can return DECIMAL/DATE/TIME/DATETIME and a decimal keeps full
//! precision. Old documents contain none of them, so this is backward
//! compatible on read (no format-version bump). Structural mutators preserve
//! these encoded scalar leaves without routing them through a serde DOM.
//!
//! Object keys are stored sorted ascending by `(key.len(), key.as_bytes())`
//! — the same order MySQL uses — so `JSON_KEYS` reflects MySQL ordering and
//! a key lookup can stop early once it passes the target.

use std::cmp::Ordering;

use serde_json::Value as Json;

use super::{json_err, read_uvarint, uvarint_len, write_uvarint};
use crate::types::QueryError;

pub(super) const T_NULL: u8 = 0;
pub(super) const T_TRUE: u8 = 1;
pub(super) const T_FALSE: u8 = 2;
pub(super) const T_I64: u8 = 3;
pub(super) const T_U64: u8 = 4;
pub(super) const T_F64: u8 = 5;
pub(super) const T_STRING: u8 = 6;
pub(super) const T_ARRAY: u8 = 7;
pub(super) const T_OBJECT: u8 = 8;
pub(super) const T_DECIMAL: u8 = 9;
pub(super) const T_DATE: u8 = 10;
pub(super) const T_TIME: u8 = 11;
pub(super) const T_DATETIME: u8 = 12;
pub(super) const T_OPAQUE: u8 = 13;
pub(super) const T_TIMESTAMP: u8 = 14;

/// The MySQL `JSON_TYPE()` classification of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonType {
    Null,
    Boolean,
    Integer,
    UnsignedInteger,
    Double,
    Decimal,
    String,
    Array,
    Object,
    Date,
    Time,
    Datetime,
    Timestamp,
    Opaque,
}

impl JsonType {
    /// The exact spelling MySQL's `JSON_TYPE()` returns.
    #[allow(dead_code)] // Kept with the binary JSON type for future jsonb callers.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            JsonType::Null => "NULL",
            JsonType::Boolean => "BOOLEAN",
            JsonType::Integer => "INTEGER",
            JsonType::UnsignedInteger => "UNSIGNED INTEGER",
            JsonType::Double => "DOUBLE",
            JsonType::Decimal => "DECIMAL",
            JsonType::String => "STRING",
            JsonType::Array => "ARRAY",
            JsonType::Object => "OBJECT",
            JsonType::Date => "DATE",
            JsonType::Time => "TIME",
            JsonType::Datetime => "DATETIME",
            JsonType::Timestamp => "TIMESTAMP",
            JsonType::Opaque => "OPAQUE",
        }
    }

    /// Cross-type sort rank used by MySQL JSON comparison. Lower sorts
    /// first. Within a type, values compare by their natural order.
    ///
    /// MySQL precedence (ascending): NULL < numbers < string < object <
    /// array < boolean < (date/time/datetime/opaque…). Numbers of any
    /// kind share one rank so `1` and `1.0` compare numerically.
    pub(crate) fn sort_rank(self) -> u8 {
        match self {
            JsonType::Null => 0,
            // All numeric kinds share one rank so `1`, `1.0` and the decimal
            // `1.00` compare by numeric value, not by sub-type.
            JsonType::Integer
            | JsonType::UnsignedInteger
            | JsonType::Double
            | JsonType::Decimal => 1,
            JsonType::String => 2,
            JsonType::Object => 3,
            JsonType::Array => 4,
            JsonType::Boolean => 5,
            // Temporal ranks sit above boolean, matching the module's
            // "… < boolean < (date/time/datetime)" ascending order.
            JsonType::Date => 6,
            JsonType::Time => 7,
            JsonType::Datetime | JsonType::Timestamp => 8,
            JsonType::Opaque => 9,
        }
    }
}

// ---------------------------------------------------------------------------
// Encoder: serde_json DOM -> jsonb bytes
// ---------------------------------------------------------------------------

/// Container metadata in canonical preorder. Planning lengths once avoids
/// allocating and copying a full payload buffer at every nesting level.
struct ContainerPlan<'a> {
    payload_len: usize,
    entries: Vec<(&'a String, &'a Json)>,
}

fn plan_node<'a>(value: &'a Json, plans: &mut Vec<ContainerPlan<'a>>) -> usize {
    let count = match value {
        Json::Null | Json::Bool(_) => return 1,
        Json::Number(_) => return 9,
        Json::String(s) => return 1 + uvarint_len(s.len() as u64) + s.len(),
        Json::Array(a) => a.len(),
        Json::Object(o) => o.len(),
    };
    let index = plans.len();
    plans.push(ContainerPlan {
        payload_len: 0,
        entries: Vec::new(),
    });
    let mut payload_len = uvarint_len(count as u64);
    match value {
        Json::Array(a) => {
            for child in a {
                payload_len += plan_node(child, plans);
            }
        }
        Json::Object(o) => {
            let mut entries: Vec<_> = o.iter().collect();
            entries.sort_unstable_by(|(a, _), (b, _)| key_order(a.as_bytes(), b.as_bytes()));
            for (key, child) in &entries {
                payload_len += uvarint_len(key.len() as u64) + key.len() + plan_node(child, plans);
            }
            plans[index].entries = entries;
        }
        _ => unreachable!(),
    }
    plans[index].payload_len = payload_len;
    1 + uvarint_len(payload_len as u64) + payload_len
}

pub(super) fn encode(value: &Json) -> Vec<u8> {
    let mut plans = Vec::new();
    let size = plan_node(value, &mut plans);
    let mut out = Vec::with_capacity(size);
    write_planned_node(value, &plans, &mut 0, &mut out);
    debug_assert_eq!(out.len(), size);
    out
}

fn write_planned_node(
    value: &Json,
    plans: &[ContainerPlan<'_>],
    next: &mut usize,
    out: &mut Vec<u8>,
) {
    match value {
        Json::Null => out.push(T_NULL),
        Json::Bool(true) => out.push(T_TRUE),
        Json::Bool(false) => out.push(T_FALSE),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                out.push(T_I64);
                out.extend_from_slice(&i.to_le_bytes());
            } else if let Some(u) = n.as_u64() {
                out.push(T_U64);
                out.extend_from_slice(&u.to_le_bytes());
            } else {
                let f = n.as_f64().unwrap_or(0.0);
                out.push(T_F64);
                out.extend_from_slice(&f.to_le_bytes());
            }
        }
        Json::String(s) => {
            out.push(T_STRING);
            write_uvarint(out, s.len() as u64);
            out.extend_from_slice(s.as_bytes());
        }
        Json::Array(arr) => {
            let plan = &plans[*next];
            *next += 1;
            out.push(T_ARRAY);
            write_uvarint(out, plan.payload_len as u64);
            write_uvarint(out, arr.len() as u64);
            for child in arr {
                write_planned_node(child, plans, next, out);
            }
        }
        Json::Object(_) => {
            let plan = &plans[*next];
            *next += 1;
            out.push(T_OBJECT);
            write_uvarint(out, plan.payload_len as u64);
            write_uvarint(out, plan.entries.len() as u64);
            for (key, child) in &plan.entries {
                write_uvarint(out, key.len() as u64);
                out.extend_from_slice(key.as_bytes());
                write_planned_node(child, plans, next, out);
            }
        }
    }
}

/// Encode a length-prefixed text scalar under one of the additive tags
/// (`T_DECIMAL` / `T_DATE` / `T_TIME` / `T_DATETIME`). The layout is identical
/// to `T_STRING`, so `node_len` and the iterators skip it uniformly.
pub(super) fn write_text_scalar(tag: u8, text: &str, out: &mut Vec<u8>) {
    out.push(tag);
    write_uvarint(out, text.len() as u64);
    out.extend_from_slice(text.as_bytes());
}

/// MySQL object-key ordering: shorter keys first, ties broken by raw bytes.
#[inline]
pub(super) fn key_order(a: &[u8], b: &[u8]) -> Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

/// Build an ARRAY node from a list of already-encoded jsonb element nodes.
/// Used to autowrap multi-match `JSON_EXTRACT` results and to encode
/// `JSON_ARRAY(...)` directly (so decimal / temporal elements keep their tag
/// instead of degrading through a serde DOM).
pub(crate) fn build_array(nodes: &[Vec<u8>]) -> Vec<u8> {
    let mut payload = Vec::new();
    write_uvarint(&mut payload, nodes.len() as u64);
    for n in nodes {
        payload.extend_from_slice(n);
    }
    let mut out = Vec::with_capacity(payload.len() + 5);
    out.push(T_ARRAY);
    write_uvarint(&mut out, payload.len() as u64);
    out.extend_from_slice(&payload);
    out
}

/// Build an OBJECT node from `(key, encoded-value-node)` pairs, applying MySQL
/// `JSON_OBJECT` semantics: duplicate keys are last-wins, then members are
/// stored in canonical `(len, bytes)` key order — byte-identical to what
/// [`encode`] produces for the same object, so decimal / temporal values
/// keep their tag without a serde round-trip.
pub(crate) fn build_object(pairs: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut entries: Vec<(&str, &[u8])> = Vec::with_capacity(pairs.len());
    for (k, v) in pairs {
        if let Some(slot) = entries.iter_mut().find(|(ek, _)| *ek == k.as_str()) {
            slot.1 = v.as_slice(); // last-wins
        } else {
            entries.push((k.as_str(), v.as_slice()));
        }
    }
    entries.sort_by(|(a, _), (b, _)| key_order(a.as_bytes(), b.as_bytes()));
    let mut payload = Vec::new();
    write_uvarint(&mut payload, entries.len() as u64);
    for (key, val) in entries {
        write_uvarint(&mut payload, key.len() as u64);
        payload.extend_from_slice(key.as_bytes());
        payload.extend_from_slice(val);
    }
    let mut out = Vec::with_capacity(payload.len() + 5);
    out.push(T_OBJECT);
    write_uvarint(&mut out, payload.len() as u64);
    out.extend_from_slice(&payload);
    out
}

// ---------------------------------------------------------------------------
// Reader: a cursor over one jsonb node, borrowing the backing bytes.
// ---------------------------------------------------------------------------

/// A borrowed view of a single jsonb node. `bytes` starts at the node's tag
/// and extends at least to the end of the node (it may extend further; the
/// node's own length is computed on demand).
#[derive(Debug, Clone, Copy)]
pub(crate) struct JsonbRef<'a> {
    bytes: &'a [u8],
}

impl<'a> JsonbRef<'a> {
    /// Document-local node identity, independent of equal scalar contents.
    pub(crate) fn identity(self) -> usize {
        self.bytes.as_ptr() as usize
    }

    /// Wrap a full jsonb document, validating its outer node boundary.
    pub(crate) fn new(bytes: &'a [u8]) -> Result<Self, QueryError> {
        let len = Self::node_len(bytes)?;
        if len != bytes.len() {
            return Err(json_err(
                crate::error::catalog::ER_INVALID_JSON_TEXT,
                "trailing bytes after jsonb document",
            ));
        }
        Ok(Self { bytes })
    }

    /// Validate untrusted persisted/exported documents before recursive reads.
    /// Scalar payloads are borrowed and containers are bounded before descent.
    pub(crate) fn validate(&self) -> Result<(), QueryError> {
        fn check(bytes: &[u8], containers: usize) -> Result<(), QueryError> {
            let node = JsonbRef::new(bytes)?;
            let malformed = || {
                json_err(
                    crate::error::catalog::ER_INVALID_JSON_TEXT,
                    "malformed JSONB container or UTF-8 payload",
                )
            };
            if node.is_array() || node.is_object() {
                if containers >= super::MAX_JSON_DEPTH - 1 {
                    return Err(super::json_too_deep());
                }
                let mut pos = 1;
                read_uvarint(bytes, &mut pos)?;
                let count = read_uvarint(bytes, &mut pos)?;
                for _ in 0..count {
                    if node.is_object() {
                        let len = usize::try_from(read_uvarint(bytes, &mut pos)?)
                            .map_err(|_| malformed())?;
                        let end = pos
                            .checked_add(len)
                            .filter(|end| *end <= bytes.len())
                            .ok_or_else(malformed)?;
                        std::str::from_utf8(&bytes[pos..end]).map_err(|_| malformed())?;
                        pos = end;
                    }
                    let remaining = bytes.get(pos..).ok_or_else(malformed)?;
                    let len = JsonbRef::node_len(remaining)?;
                    check(&remaining[..len], containers + 1)?;
                    pos += len;
                }
                if pos != bytes.len() {
                    return Err(malformed());
                }
            } else if matches!(
                node.tag(),
                T_STRING | T_DECIMAL | T_DATE | T_TIME | T_DATETIME | T_TIMESTAMP
            ) && node.text_payload().is_none()
            {
                return Err(malformed());
            }
            Ok(())
        }
        check(self.bytes, 0)
    }

    #[inline]
    fn tag(&self) -> u8 {
        self.bytes[0]
    }

    /// Total on-disk byte length of the node at the start of `b`.
    fn node_len(b: &[u8]) -> Result<usize, QueryError> {
        let tag = *b.first().ok_or_else(|| {
            json_err(
                crate::error::catalog::ER_INVALID_JSON_TEXT,
                "truncated jsonb node",
            )
        })?;
        let len = match tag {
            T_NULL | T_TRUE | T_FALSE => 1,
            T_I64 | T_U64 | T_F64 => 9,
            // All length-prefixed text scalars share the string layout.
            T_STRING | T_DECIMAL | T_DATE | T_TIME | T_DATETIME | T_TIMESTAMP | T_OPAQUE => {
                let mut p = 1;
                let len = usize::try_from(read_uvarint(b, &mut p)?).map_err(|_| {
                    json_err(
                        crate::error::catalog::ER_INVALID_JSON_TEXT,
                        "jsonb text length exceeds addressable memory",
                    )
                })?;
                if tag == T_OPAQUE && len == 0 {
                    return Err(super::invalid_json_text(1, "opaque JSON"));
                }
                p.checked_add(len).ok_or_else(|| {
                    json_err(
                        crate::error::catalog::ER_INVALID_JSON_TEXT,
                        "jsonb text length overflow",
                    )
                })?
            }
            T_ARRAY | T_OBJECT => {
                let mut p = 1;
                let payload_len = usize::try_from(read_uvarint(b, &mut p)?).map_err(|_| {
                    json_err(
                        crate::error::catalog::ER_INVALID_JSON_TEXT,
                        "jsonb container length exceeds addressable memory",
                    )
                })?;
                p.checked_add(payload_len).ok_or_else(|| {
                    json_err(
                        crate::error::catalog::ER_INVALID_JSON_TEXT,
                        "jsonb container length overflow",
                    )
                })?
            }
            other => {
                return Err(json_err(
                    crate::error::catalog::ER_INVALID_JSON_TEXT,
                    format!("unknown jsonb tag {other}"),
                ))
            }
        };
        if len > b.len() {
            return Err(json_err(
                crate::error::catalog::ER_INVALID_JSON_TEXT,
                "truncated jsonb node",
            ));
        }
        Ok(len)
    }

    #[inline]
    fn fixed_payload(&self) -> Option<[u8; 8]> {
        self.bytes.get(1..9)?.try_into().ok()
    }

    /// The bytes of exactly this node (tag through end), copyable into a
    /// fresh `Value::Json`.
    pub(crate) fn to_owned_bytes(&self) -> Vec<u8> {
        let len = Self::node_len(self.bytes).unwrap_or(self.bytes.len());
        self.bytes[..len.min(self.bytes.len())].to_vec()
    }

    pub(crate) fn json_type(&self) -> JsonType {
        match self.tag() {
            T_NULL => JsonType::Null,
            T_TRUE | T_FALSE => JsonType::Boolean,
            T_I64 => JsonType::Integer,
            T_U64 => JsonType::UnsignedInteger,
            T_F64 => JsonType::Double,
            T_DECIMAL => JsonType::Decimal,
            T_STRING => JsonType::String,
            T_ARRAY => JsonType::Array,
            T_DATE => JsonType::Date,
            T_TIME => JsonType::Time,
            T_DATETIME => JsonType::Datetime,
            T_TIMESTAMP => JsonType::Timestamp,
            T_OPAQUE => JsonType::Opaque,
            _ => JsonType::Object,
        }
    }

    pub(crate) fn opaque_value(&self) -> Option<(u8, &'a [u8])> {
        if self.tag() != T_OPAQUE {
            return None;
        }
        let mut pos = 1;
        let len = usize::try_from(read_uvarint(self.bytes, &mut pos).ok()?).ok()?;
        let payload = self.bytes.get(pos..pos.checked_add(len)?)?;
        Some((*payload.first()?, &payload[1..]))
    }

    pub(crate) fn type_name(&self) -> &'static str {
        match self.opaque_value().map(|(ty, _)| ty) {
            Some(15 | 249..=254) => "BLOB",
            Some(16) => "BIT",
            Some(255) => "GEOMETRY",
            _ => self.json_type().as_str(),
        }
    }

    /// Raw text payload of any length-prefixed text scalar (string / decimal /
    /// temporal). Distinct from [`as_str`](Self::as_str), which is limited to
    /// the JSON-string-like tags.
    fn text_payload(&self) -> Option<&'a str> {
        match self.tag() {
            T_STRING | T_DECIMAL | T_DATE | T_TIME | T_DATETIME | T_TIMESTAMP => {
                let mut p = 1;
                let len = usize::try_from(read_uvarint(self.bytes, &mut p).ok()?).ok()?;
                let end = p.checked_add(len)?;
                simdutf8::basic::from_utf8(self.bytes.get(p..end)?).ok()
            }
            _ => None,
        }
    }

    /// Exact `(magnitude, scale)` of a number node, or `None` for a real double
    /// (inexact) / non-number. Integers use scale 0; a `T_U64` above
    /// `i64::MAX` stays exact because `i128` holds any `u64` (fixing the old
    /// f64 fallback that mis-ordered large unsigned integers); a decimal parses
    /// its canonical text. Used for precise JSON number comparison.
    pub(crate) fn exact_number(&self) -> Option<(i128, u8)> {
        match self.tag() {
            T_I64 => Some((i128::from(i64::from_le_bytes(self.fixed_payload()?)), 0)),
            T_U64 => Some((i128::from(u64::from_le_bytes(self.fixed_payload()?)), 0)),
            T_DECIMAL => crate::util::decimal::parse_exact_decimal_str(self.text_payload()?),
            _ => None,
        }
    }

    pub(crate) fn is_array(&self) -> bool {
        self.tag() == T_ARRAY
    }

    pub(crate) fn is_object(&self) -> bool {
        self.tag() == T_OBJECT
    }

    pub(crate) fn is_null(&self) -> bool {
        self.tag() == T_NULL
    }

    pub(crate) fn as_bool(&self) -> Option<bool> {
        match self.tag() {
            T_TRUE => Some(true),
            T_FALSE => Some(false),
            _ => None,
        }
    }

    pub(crate) fn as_u64(&self) -> Option<u64> {
        match self.tag() {
            T_U64 => Some(u64::from_le_bytes(self.fixed_payload()?)),
            T_I64 => u64::try_from(i64::from_le_bytes(self.fixed_payload()?)).ok(),
            _ => None,
        }
    }

    pub(crate) fn as_i64(&self) -> Option<i64> {
        match self.tag() {
            T_I64 => Some(i64::from_le_bytes(self.fixed_payload()?)),
            T_U64 => i64::try_from(u64::from_le_bytes(self.fixed_payload()?)).ok(),
            // An integer-valued decimal ("42", "42.0") coerces to i64.
            T_DECIMAL => {
                let (mag, scale) =
                    crate::util::decimal::parse_exact_decimal_str(self.text_payload()?)?;
                if scale == 0 {
                    i64::try_from(mag).ok()
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Numeric value as f64 for any number node (integer/unsigned/double/decimal).
    pub(crate) fn as_f64(&self) -> Option<f64> {
        match self.tag() {
            T_I64 => Some(i64::from_le_bytes(self.fixed_payload()?) as f64),
            T_U64 => Some(u64::from_le_bytes(self.fixed_payload()?) as f64),
            T_F64 => Some(f64::from_le_bytes(self.fixed_payload()?)),
            T_DECIMAL => self.text_payload()?.parse().ok(),
            _ => None,
        }
    }

    pub(crate) fn is_number(&self) -> bool {
        matches!(self.tag(), T_I64 | T_U64 | T_F64 | T_DECIMAL)
    }

    /// Canonical decimal text of a number node — the integer literal for
    /// I64/U64, the stored canonical text for DECIMAL, and the shortest
    /// round-tripping representation (possibly in `e` notation) for a DOUBLE,
    /// identical to how the value renders in JSON output. Used by the comparator
    /// to order JSON numbers by their exact decimal value (never by a lossy f64
    /// collapse of an exact integer), so `9.223372036854776e18` orders exactly
    /// like `9223372036854776000`. Returns `None` for a non-number node.
    pub(crate) fn number_text(&self) -> Option<String> {
        match self.tag() {
            T_I64 => Some(i64::from_le_bytes(self.fixed_payload()?).to_string()),
            T_U64 => Some(u64::from_le_bytes(self.fixed_payload()?).to_string()),
            T_DECIMAL => Some(self.text_payload()?.to_string()),
            T_F64 => {
                let mut s = String::new();
                write_f64(f64::from_le_bytes(self.fixed_payload()?), &mut s);
                Some(s)
            }
            _ => None,
        }
    }

    /// The raw UTF-8 of a string-like node (JSON string, or the canonical text
    /// of a temporal scalar — which MySQL renders as a quoted string). A
    /// decimal is *not* string-like (it renders as a bare number), so it is
    /// excluded here and handled through the numeric accessors.
    pub(crate) fn as_str(&self) -> Option<&'a str> {
        match self.tag() {
            T_STRING | T_DATE | T_TIME | T_DATETIME | T_TIMESTAMP => self.text_payload(),
            _ => None,
        }
    }

    /// Number of elements (array) / members (object); `None` for scalars.
    pub(crate) fn container_len(&self) -> Option<usize> {
        match self.tag() {
            T_ARRAY | T_OBJECT => {
                let mut p = 1;
                let _payload_len = read_uvarint(self.bytes, &mut p).ok()?;
                usize::try_from(read_uvarint(self.bytes, &mut p).ok()?).ok()
            }
            _ => None,
        }
    }

    /// Iterate array elements. Empty for non-arrays.
    pub(crate) fn array_iter(&self) -> ArrayIter<'a> {
        let mut p = 1;
        let count = if self.tag() == T_ARRAY {
            let _payload_len = read_uvarint(self.bytes, &mut p).unwrap_or(0);
            read_uvarint(self.bytes, &mut p)
                .ok()
                .and_then(|count| usize::try_from(count).ok())
                .unwrap_or(0)
        } else {
            0
        };
        ArrayIter {
            bytes: self.bytes,
            pos: p,
            remaining: count,
        }
    }

    /// Iterate object `(key, value)` members in stored order. Empty for
    /// non-objects.
    pub(crate) fn object_iter(&self) -> ObjectIter<'a> {
        let mut p = 1;
        let count = if self.tag() == T_OBJECT {
            let _payload_len = read_uvarint(self.bytes, &mut p).unwrap_or(0);
            read_uvarint(self.bytes, &mut p)
                .ok()
                .and_then(|count| usize::try_from(count).ok())
                .unwrap_or(0)
        } else {
            0
        };
        ObjectIter {
            bytes: self.bytes,
            pos: p,
            remaining: count,
        }
    }

    /// Look up `key` in an object node. Keys are sorted by (len, bytes), so
    /// the scan stops as soon as it passes the target.
    pub(crate) fn get(&self, key: &str) -> Option<JsonbRef<'a>> {
        if self.tag() != T_OBJECT {
            return None;
        }
        let target = key.as_bytes();
        for (k, v) in self.object_iter() {
            match key_order(k.as_bytes(), target) {
                Ordering::Less => continue,
                Ordering::Equal => return Some(v),
                Ordering::Greater => return None,
            }
        }
        None
    }

    /// Index into an array node. Non-arrays behave as a single-element array
    /// holding the value itself at index 0 (MySQL's auto-wrap rule); callers
    /// that need strict array semantics check [`is_array`](Self::is_array).
    pub(crate) fn index(&self, idx: usize) -> Option<JsonbRef<'a>> {
        if self.tag() != T_ARRAY {
            return if idx == 0 { Some(*self) } else { None };
        }
        self.array_iter().nth(idx)
    }

    // ---- conversions ----

    /// Decode this node into a serde_json DOM. Used by the structural
    /// mutators that are simpler to express on a tree.
    pub(crate) fn to_serde(&self) -> Result<Json, QueryError> {
        self.validate()?;
        self.to_serde_inner()
    }

    fn to_serde_inner(&self) -> Result<Json, QueryError> {
        Ok(match self.tag() {
            T_NULL => Json::Null,
            T_TRUE => Json::Bool(true),
            T_FALSE => Json::Bool(false),
            T_I64 => Json::Number(self.as_i64().unwrap_or(0).into()),
            T_U64 => {
                Json::Number(u64::from_le_bytes(self.fixed_payload().unwrap_or([0; 8])).into())
            }
            T_F64 => serde_json::Number::from_f64(self.as_f64().unwrap_or(0.0))
                .map(Json::Number)
                .unwrap_or(Json::Null),
            // serde has no decimal/temporal: the DOM-based mutators degrade a
            // decimal to a JSON number (best-effort, may lose precision) and a
            // temporal to its canonical text string. Read/compare paths never
            // take this route — they stay on the binary form.
            T_DECIMAL => serde_json::from_str::<Json>(self.text_payload().unwrap_or("0"))
                .ok()
                .filter(Json::is_number)
                .unwrap_or_else(|| {
                    Json::String(self.text_payload().unwrap_or_default().to_string())
                }),
            T_STRING | T_DATE | T_TIME | T_DATETIME | T_TIMESTAMP => {
                Json::String(self.text_payload().unwrap_or_default().to_string())
            }
            T_OPAQUE => {
                let (ty, data) = self
                    .opaque_value()
                    .ok_or_else(|| super::invalid_json_text(1, "opaque JSON"))?;
                Json::String(super::opaque_text(ty, data))
            }
            T_ARRAY => {
                let mut arr = Vec::new();
                for elem in self.array_iter() {
                    arr.push(elem.to_serde_inner()?);
                }
                Json::Array(arr)
            }
            _ => {
                let mut map = serde_json::Map::new();
                for (k, v) in self.object_iter() {
                    map.insert(k.to_string(), v.to_serde_inner()?);
                }
                Json::Object(map)
            }
        })
    }

    /// Append MySQL canonical compact text (`{"a": 1, "b": 2}`).
    pub(crate) fn write_text(&self, out: &mut String) {
        if let Err(err) = self.validate() {
            crate::engine::api::push_statement_condition(crate::error::ConditionLevel::Error, &err);
            out.push_str("null");
            return;
        }
        self.write_text_inner(out);
    }

    fn write_text_inner(&self, out: &mut String) {
        match self.tag() {
            T_NULL => out.push_str("null"),
            T_TRUE => out.push_str("true"),
            T_FALSE => out.push_str("false"),
            T_I64 => {
                use std::fmt::Write;
                let _ = write!(out, "{}", self.as_i64().unwrap_or(0));
            }
            T_U64 => {
                use std::fmt::Write;
                let u = u64::from_le_bytes(self.fixed_payload().unwrap_or([0; 8]));
                let _ = write!(out, "{u}");
            }
            T_F64 => write_f64(self.as_f64().unwrap_or(0.0), out),
            // A decimal renders as a bare number (`3.14`); a temporal renders
            // as a quoted string (`"2020-01-02"`) — MySQL's JSON text form.
            T_DECIMAL => out.push_str(self.text_payload().unwrap_or("0")),
            T_STRING | T_DATE | T_TIME | T_DATETIME | T_TIMESTAMP => {
                write_json_string(self.text_payload().unwrap_or_default(), out)
            }
            T_OPAQUE => {
                if let Some((ty, data)) = self.opaque_value() {
                    write_json_string(&super::opaque_text(ty, data), out);
                }
            }
            T_ARRAY => {
                out.push('[');
                for (i, elem) in self.array_iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    elem.write_text_inner(out);
                }
                out.push(']');
            }
            _ => {
                out.push('{');
                for (i, (k, v)) in self.object_iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    write_json_string(k, out);
                    out.push_str(": ");
                    v.write_text_inner(out);
                }
                out.push('}');
            }
        }
    }

    /// Append MySQL `JSON_PRETTY` indented text (two spaces per level).
    pub(crate) fn write_pretty(&self, out: &mut String, indent: usize) {
        if let Err(err) = self.validate() {
            crate::engine::api::push_statement_condition(crate::error::ConditionLevel::Error, &err);
            out.push_str("null");
            return;
        }
        self.write_pretty_inner(out, indent);
    }

    fn write_pretty_inner(&self, out: &mut String, indent: usize) {
        match self.tag() {
            T_ARRAY => {
                if self.container_len() == Some(0) {
                    out.push_str("[]");
                    return;
                }
                out.push_str("[\n");
                let inner = indent + 1;
                for (i, elem) in self.array_iter().enumerate() {
                    if i > 0 {
                        out.push_str(",\n");
                    }
                    push_indent(out, inner);
                    elem.write_pretty_inner(out, inner);
                }
                out.push('\n');
                push_indent(out, indent);
                out.push(']');
            }
            T_OBJECT => {
                if self.container_len() == Some(0) {
                    out.push_str("{}");
                    return;
                }
                out.push_str("{\n");
                let inner = indent + 1;
                for (i, (k, v)) in self.object_iter().enumerate() {
                    if i > 0 {
                        out.push_str(",\n");
                    }
                    push_indent(out, inner);
                    write_json_string(k, out);
                    out.push_str(": ");
                    v.write_pretty_inner(out, inner);
                }
                out.push('\n');
                push_indent(out, indent);
                out.push('}');
            }
            _ => self.write_text_inner(out),
        }
    }

    /// Storage size in bytes of this node, matching `JSON_STORAGE_SIZE`
    /// semantics closely enough for the binary form.
    #[cfg(test)]
    pub(crate) fn storage_size(&self) -> usize {
        Self::node_len(self.bytes).unwrap_or(0)
    }

    /// Maximum nesting depth (`JSON_DEPTH`): scalars = 1, empty
    /// array/object = 1, otherwise 1 + max child depth.
    pub(crate) fn depth(&self) -> usize {
        if let Err(err) = self.validate() {
            crate::engine::api::push_statement_condition(crate::error::ConditionLevel::Error, &err);
            return super::MAX_JSON_DEPTH + 1;
        }
        self.depth_inner()
    }

    fn depth_inner(&self) -> usize {
        match self.tag() {
            T_ARRAY => {
                1 + self
                    .array_iter()
                    .map(|e| e.depth_inner())
                    .max()
                    .unwrap_or(0)
            }
            T_OBJECT => {
                1 + self
                    .object_iter()
                    .map(|(_, v)| v.depth_inner())
                    .max()
                    .unwrap_or(0)
            }
            _ => 1,
        }
    }
}

/// Iterator over the elements of an array node.
pub(crate) struct ArrayIter<'a> {
    bytes: &'a [u8],
    pos: usize,
    remaining: usize,
}

impl<'a> Iterator for ArrayIter<'a> {
    type Item = JsonbRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let node = self.bytes.get(self.pos..)?;
        let len = JsonbRef::node_len(node).ok()?;
        let end = self.pos.checked_add(len)?;
        let item = JsonbRef {
            bytes: self.bytes.get(self.pos..end)?,
        };
        self.pos = end;
        self.remaining -= 1;
        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

/// Iterator over the `(key, value)` members of an object node.
pub(crate) struct ObjectIter<'a> {
    bytes: &'a [u8],
    pos: usize,
    remaining: usize,
}

impl<'a> Iterator for ObjectIter<'a> {
    type Item = (&'a str, JsonbRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let mut p = self.pos;
        let klen = usize::try_from(read_uvarint(self.bytes, &mut p).ok()?).ok()?;
        let key_end = p.checked_add(klen)?;
        let key = simdutf8::basic::from_utf8(self.bytes.get(p..key_end)?).ok()?;
        p = key_end;
        let node = self.bytes.get(p..)?;
        let vlen = JsonbRef::node_len(node).ok()?;
        let value_end = p.checked_add(vlen)?;
        let value = JsonbRef {
            bytes: self.bytes.get(p..value_end)?,
        };
        self.pos = value_end;
        self.remaining -= 1;
        Some((key, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

// ---------------------------------------------------------------------------
// Text serialisation helpers
// ---------------------------------------------------------------------------

/// Format an f64 the way MySQL renders a JSON double: shortest round-trip,
/// always carrying a decimal point or exponent so it reads as a double
/// (`1.0`, not `1`). Rust's `{:?}` for f64 gives exactly this.
pub(super) fn write_f64(v: f64, out: &mut String) {
    if !v.is_finite() {
        out.push_str("null");
        return;
    }
    let mut buffer = ryu::Buffer::new();
    let raw = buffer.format_finite(v);
    let text = crate::types::ryu_to_mysql_text(raw, String::new);
    out.push_str(&text);
    if !text.contains(['.', 'e', 'E']) {
        out.push_str(".0");
    }
}

/// Append `s` as a JSON string literal with the standard escapes.
pub(super) fn write_json_string(s: &str, out: &mut String) {
    // Copy unescaped UTF-8 runs intact. Only ASCII controls, quotes and
    // backslashes need escaping; decoding and re-encoding every character
    // otherwise dominates formatting large JSON documents.
    const LOW: u64 = 0x0101_0101_0101_0101;
    const HIGH: u64 = 0x8080_8080_8080_8080;
    fn has_zero_byte(word: u64) -> bool {
        word.wrapping_sub(LOW) & !word & HIGH != 0
    }
    out.reserve(s.len().saturating_add(2));
    out.push('"');
    let bytes = s.as_bytes();
    let mut start = 0;
    let mut pos = 0;
    while pos < bytes.len() {
        while let Some(chunk) = bytes.get(pos..pos.saturating_add(8)) {
            let word = u64::from_ne_bytes(chunk.try_into().unwrap());
            // Standard byte-lane tests. A borrow can flag an extra lane,
            // which only sends that chunk to the exact byte check below.
            let control = word.wrapping_sub(LOW * 0x20) & !word & HIGH != 0;
            if control || has_zero_byte(word ^ (LOW * 0x22)) || has_zero_byte(word ^ (LOW * 0x5c)) {
                break;
            }
            pos += 8;
        }
        if pos == bytes.len() {
            break;
        }
        let byte = bytes[pos];
        if byte < 0x20 || byte == b'"' || byte == b'\\' {
            // Every split is at an ASCII byte, hence a UTF-8 boundary.
            out.push_str(&s[start..pos]);
            match byte {
                b'"' => out.push_str("\\\""),
                b'\\' => out.push_str("\\\\"),
                b'\n' => out.push_str("\\n"),
                b'\r' => out.push_str("\\r"),
                b'\t' => out.push_str("\\t"),
                8 => out.push_str("\\b"),
                12 => out.push_str("\\f"),
                byte => {
                    const HEX: &[u8; 16] = b"0123456789abcdef";
                    out.push_str("\\u00");
                    out.push(HEX[(byte >> 4) as usize] as char);
                    out.push(HEX[(byte & 15) as usize] as char);
                }
            }
            start = pos + 1;
        }
        pos += 1;
    }
    out.push_str(&s[start..]);
    out.push('"');
}

fn push_indent(out: &mut String, level: usize) {
    for _ in 0..level {
        out.push_str("  ");
    }
}
