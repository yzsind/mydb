//! Immutable semantic view queries and their bounded catalog codec.
//! SQL presentation is deliberately absent from this execution format.
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::parser::{
    visit::{self, AstVisitor},
    Expr, Literal, QueryExpr, Select,
};
use crate::storage::error;
use crate::types::{QueryError, Value};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    hash::{Hash, Hasher},
    io::{self, Write},
    sync::Arc,
};

const MAX_JSON_BYTES: usize = 64 * 1024 * 1024;
/// Query expressions nest through set-operation chains (at most 1024
/// branches) and SELECT blocks; scalar expressions nest independently.
const MAX_QUERY_DEPTH: usize = 1024 + crate::parser::MAX_SELECT_NESTING;
/// Each syntax level encodes as at most a few JSON containers (enum tag,
/// struct, argument list).
const MAX_JSON_DEPTH: usize =
    4 * (crate::engine::expr_program::MAX_EXPRESSION_DEPTH + MAX_QUERY_DEPTH);
const MAX_JSON_CONTAINERS: usize = 1_000_000;
const MAX_JSON_SCALARS: usize = 4_000_000;
/// Serde recurses once per JSON container without checking the stack; the
/// codec reserves a segment for the deepest admitted encoding up front.
const CODEC_STACK_BYTES: usize = MAX_JSON_DEPTH * 1024;

#[derive(Clone)]
pub struct ViewQuery(Arc<QueryData>);
#[derive(Debug)]
struct QueryData {
    select: Select,
    encoded: String,
    identity: u64,
    // The canonical encoding is shared with the immutable query across catalog
    // snapshots. Admission lives as long as its actual allocation capacity.
    _encoded_charge: ControlledMemoryCharge,
}
#[derive(Serialize)]
struct WireRef<'a> {
    format_version: u16,
    select: &'a Select,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    format_version: u16,
    select: Select,
}

impl ViewQuery {
    pub const FORMAT_VERSION: u16 = 2;

    /// Validate and encode before any catalog mutation can publish the query.
    pub fn new(select: Select) -> Result<Self, QueryError> {
        crate::util::stack::with_reserved_stack_bytes(
            ControlledMemoryCategory::ParserTransient,
            CODEC_STACK_BYTES,
            || {
                Self::from_wire(Wire {
                    format_version: Self::FORMAT_VERSION,
                    select,
                })
            },
        )?
    }

    pub fn select(&self) -> &Select {
        &self.0.select
    }
    pub(crate) fn identity(&self) -> u64 {
        self.0.identity
    }
    pub(crate) fn encoded(&self) -> &str {
        &self.0.encoded
    }

    pub(crate) fn decode(source: &str) -> Result<Self, QueryError> {
        // Bound the wire graph before entering recursive Serde code. This
        // scanner neither builds a JSON tree nor guesses SQL semantics.
        let mut bounds = JsonBounds::default();
        bounds.scan(source.as_bytes()).map_err(invalid_wire)?;
        bounds.finish().map_err(invalid_wire)?;
        // Each container can admit at most a boxed syntax node or vector
        // growth. Reserve a conservative upper bound before deserializing;
        // string contents/scratch are bounded by the wire bytes themselves.
        let node_bytes = std::mem::size_of::<Select>()
            .max(std::mem::size_of::<Expr>())
            .max(std::mem::size_of::<crate::parser::SetOperation>());
        let scalar_bytes = std::mem::size_of::<String>().max(std::mem::size_of::<u128>());
        let scratch_bytes = source
            .len()
            .saturating_mul(4)
            .saturating_add(
                bounds
                    .scalars
                    .saturating_mul(scalar_bytes.saturating_mul(2)),
            )
            .saturating_add(
                bounds
                    .containers
                    .saturating_mul(node_bytes.saturating_mul(2)),
            );
        let _scratch = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::ParserTransient,
            scratch_bytes,
        )?;
        crate::util::stack::with_reserved_stack_bytes(
            ControlledMemoryCategory::ParserTransient,
            CODEC_STACK_BYTES,
            || {
                let mut decoder = serde_json::Deserializer::from_str(source);
                // The explicit bounds above replace the unrelated default JSON
                // limit, which rejects valid SQL expressions below depth 64.
                decoder.disable_recursion_limit();
                let wire =
                    Wire::deserialize(&mut decoder).map_err(|e| invalid_wire(e.to_string()))?;
                decoder.end().map_err(|e| invalid_wire(e.to_string()))?;
                Self::from_wire(wire).map_err(|e| {
                    if e.is(crate::error::catalog::ER_NOT_SUPPORTED_YET) {
                        invalid_wire(e.message().to_string())
                    } else {
                        e
                    }
                })
            },
        )?
    }

    fn from_wire(wire: Wire) -> Result<Self, QueryError> {
        if wire.format_version != Self::FORMAT_VERSION {
            return Err(invalid_wire(format!(
                "unsupported semantic query format version {} (expected {})",
                wire.format_version,
                Self::FORMAT_VERSION
            )));
        }
        let mut validation = QueryValidation::default();
        visit::visit_select(&mut validation, &wire.select);
        if let Some(reason) = validation.failure {
            return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET, reason));
        }
        let mut writer = QueryWriter::new()?;
        if let Err(e) = serde_json::to_writer(
            &mut writer,
            &WireRef {
                format_version: Self::FORMAT_VERSION,
                select: &wire.select,
            },
        ) {
            return Err(writer.failure.take().unwrap_or_else(|| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("cannot encode semantic query: {e}"),
                )
            }));
        }
        writer
            .bounds
            .finish()
            .map_err(|e| error(crate::error::catalog::ER_NOT_SUPPORTED_YET, e))?;
        writer
            .charge
            .try_grow(std::mem::size_of::<QueryData>() + 2 * std::mem::size_of::<usize>())?;
        let encoded = String::from_utf8(writer.bytes).map_err(|e| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("invalid semantic query encoding: {e}"),
            )
        })?;
        let mut identity = std::collections::hash_map::DefaultHasher::new();
        encoded.hash(&mut identity);
        Ok(Self(Arc::new(QueryData {
            select: wire.select,
            identity: identity.finish(),
            encoded,
            _encoded_charge: writer.charge,
        })))
    }
}

impl fmt::Debug for ViewQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ViewQuery")
            .field("format_version", &Self::FORMAT_VERSION)
            .field("select", self.select())
            .finish()
    }
}
impl Serialize for ViewQuery {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        crate::util::stack::with_reserved_stack_bytes(
            ControlledMemoryCategory::ParserTransient,
            CODEC_STACK_BYTES,
            || {
                WireRef {
                    format_version: Self::FORMAT_VERSION,
                    select: self.select(),
                }
                .serialize(serializer)
            },
        )
        .map_err(serde::ser::Error::custom)?
    }
}
impl<'de> Deserialize<'de> for ViewQuery {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        crate::util::stack::with_reserved_stack_bytes(
            ControlledMemoryCategory::ParserTransient,
            CODEC_STACK_BYTES,
            || {
                let wire = Wire::deserialize(deserializer)?;
                Self::from_wire(wire).map_err(serde::de::Error::custom)
            },
        )
        .map_err(serde::de::Error::custom)?
    }
}

fn invalid_wire(message: impl Into<String>) -> QueryError {
    let mut message = message.into();
    if message.len() > 4096 {
        let mut end = 4096;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
        message.push_str("…");
    }
    error(
        crate::error::catalog::ER_VIEW_INVALID,
        format!("invalid semantic query encoding: {message}"),
    )
}

#[derive(Default)]
struct QueryValidation {
    expression_depth: usize,
    query_depth: usize,
    select_depth: usize,
    failure: Option<String>,
}
impl AstVisitor for QueryValidation {
    fn enter_expr(&mut self, expr: &Expr) {
        self.expression_depth += 1;
        if self.expression_depth > crate::engine::expr_program::MAX_EXPRESSION_DEPTH {
            self.failure.get_or_insert_with(|| {
                format!(
                    "semantic query expression exceeds depth limit {}",
                    crate::engine::expr_program::MAX_EXPRESSION_DEPTH
                )
            });
        }
        if match expr {
            Expr::Literal(Literal::Float(value)) | Expr::BoundValue(Value::Double(value)) => {
                !value.is_finite()
            }
            Expr::BoundValue(Value::Float(value)) => !value.is_finite(),
            _ => false,
        } {
            self.failure
                .get_or_insert_with(|| "semantic query contains a nonfinite number".into());
        }
    }
    fn exit_expr(&mut self, _: &Expr) {
        self.expression_depth -= 1;
    }
    fn walk_expr_children(&self, _: &Expr) -> bool {
        self.failure.is_none()
    }
    fn enter_query(&mut self, _: &QueryExpr) {
        self.query_depth += 1;
        if self.query_depth > MAX_QUERY_DEPTH {
            self.failure.get_or_insert_with(|| {
                format!("semantic query exceeds query depth limit {MAX_QUERY_DEPTH}")
            });
        }
    }
    fn exit_query(&mut self, _: &QueryExpr) {
        self.query_depth -= 1;
    }
    fn enter_select(&mut self, _: &Select) {
        self.select_depth += 1;
        if self.select_depth > crate::parser::MAX_SELECT_NESTING {
            self.failure.get_or_insert_with(|| {
                format!(
                    "semantic query exceeds SELECT depth limit {}",
                    crate::parser::MAX_SELECT_NESTING
                )
            });
        }
    }
    fn exit_select(&mut self, _: &Select) {
        self.select_depth -= 1;
    }
}

/// Streaming structural bounds, including escaped quotes across write chunks.
#[derive(Default)]
struct JsonBounds {
    bytes: usize,
    containers: usize,
    scalars: usize,
    primitive: bool,
    depth: usize,
    quoted: bool,
    escaped: bool,
}
impl JsonBounds {
    fn scan(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.bytes = self.bytes.saturating_add(bytes.len());
        if self.bytes > MAX_JSON_BYTES {
            return Err("semantic query exceeds 64 MiB encoding limit".into());
        }
        for &byte in bytes {
            if self.quoted {
                if self.escaped {
                    self.escaped = false;
                } else if byte == b'\\' {
                    self.escaped = true;
                } else if byte == b'"' {
                    self.quoted = false;
                }
                continue;
            }
            match byte {
                b'"' => {
                    self.quoted = true;
                    self.primitive = false;
                    self.scalar()?;
                }
                b'{' | b'[' => {
                    self.primitive = false;
                    self.depth += 1;
                    self.containers += 1;
                    if self.depth > MAX_JSON_DEPTH {
                        return Err(format!(
                            "semantic query exceeds JSON depth limit {MAX_JSON_DEPTH}"
                        ));
                    }
                    if self.containers > MAX_JSON_CONTAINERS {
                        return Err("semantic query exceeds JSON container limit 1000000".into());
                    }
                }
                b'}' | b']' => {
                    self.primitive = false;
                    self.depth = self
                        .depth
                        .checked_sub(1)
                        .ok_or_else(|| "unbalanced semantic query encoding".to_string())?
                }
                b',' | b':' | b' ' | b'\n' | b'\r' | b'\t' => self.primitive = false,
                b'0'..=b'9' | b'-' | b't' | b'f' | b'n' if !self.primitive => {
                    self.primitive = true;
                    self.scalar()?;
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn scalar(&mut self) -> Result<(), String> {
        self.scalars += 1;
        if self.scalars > MAX_JSON_SCALARS {
            Err("semantic query exceeds JSON scalar limit 4000000".into())
        } else {
            Ok(())
        }
    }
    fn finish(&self) -> Result<(), String> {
        if self.quoted || self.escaped || self.depth != 0 {
            Err("incomplete semantic query encoding".into())
        } else {
            Ok(())
        }
    }
}
struct QueryWriter {
    bytes: Vec<u8>,
    bounds: JsonBounds,
    charge: ControlledMemoryCharge,
    failure: Option<QueryError>,
}
impl QueryWriter {
    fn new() -> Result<Self, QueryError> {
        Ok(Self {
            bytes: Vec::new(),
            bounds: JsonBounds::default(),
            charge: ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)?,
            failure: None,
        })
    }
    fn append(&mut self, bytes: &[u8]) -> Result<(), QueryError> {
        self.bounds
            .scan(bytes)
            .map_err(|e| error(crate::error::catalog::ER_NOT_SUPPORTED_YET, e))?;
        let needed = self.bytes.len() + bytes.len();
        if needed > self.bytes.capacity() {
            let capacity = needed
                .max(self.bytes.capacity().saturating_mul(2))
                .max(256)
                .min(MAX_JSON_BYTES);
            self.charge.try_grow(capacity - self.bytes.capacity())?;
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|e| {
                    error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        format!("cannot allocate semantic query encoding: {e}"),
                    )
                })?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}
impl Write for QueryWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Err(error) = self.append(bytes) {
            self.failure = Some(error);
            return Err(io::Error::other("semantic query encoding failed"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::mem_budget::controlled_memory_snapshot;
    use crate::parser::{parse_statement, BinaryOp, SelectItem, Statement};

    fn select(sql: &str) -> Select {
        match parse_statement(sql).unwrap() {
            Statement::Select(select) => select,
            other => panic!("expected SELECT: {other:?}"),
        }
    }

    #[test]
    fn codec_roundtrips_deep_expressions_and_complete_query_shapes() {
        for sql in [
            format!("SELECT id{} AS n FROM t", "+1".repeat(62)),
            "SELECT * FROM (WITH c AS(SELECT 1 n UNION ALL SELECT 2) SELECT n FROM c) d".into(),
            "SELECT ROW_NUMBER() OVER(PARTITION BY a ORDER BY b ROWS 1 PRECEDING) n FROM t PARTITION(p0)".into(),
            "SELECT JSON_VALUE('{}', '$.a' RETURNING SIGNED) AS n".into(),
            "SELECT VECTOR_FROM_TEXT('[1,2,3]') AS v".into(),
        ] {
            let original = select(&sql);
            let query = ViewQuery::new(original.clone()).unwrap();
            let decoded = ViewQuery::decode(query.encoded()).unwrap();
            assert_eq!(decoded.select(), &original, "{sql}");
            assert_eq!(decoded.encoded(), query.encoded());
            assert_eq!(decoded.identity(), query.identity());
        }
    }

    #[test]
    fn codec_rejects_bad_versions_unknown_fields_and_trailing_payloads() {
        let query = ViewQuery::new(select("SELECT 1 AS n")).unwrap();
        let mut value: serde_json::Value = serde_json::from_str(query.encoded()).unwrap();
        value["format_version"] = serde_json::json!(999);
        assert!(ViewQuery::decode(&value.to_string())
            .unwrap_err()
            .message()
            .contains("version 999"));
        value["format_version"] = serde_json::json!(ViewQuery::FORMAT_VERSION);
        value["select"]["unexpected_syntax"] = serde_json::json!(true);
        assert!(ViewQuery::decode(&value.to_string())
            .unwrap_err()
            .message()
            .contains("unknown field"));
        assert!(ViewQuery::decode(&format!("{} null", query.encoded())).is_err());
        assert!(ViewQuery::decode("{}").is_err());
        assert!(ViewQuery::decode("{\"format_version\":2,\"format_version\":2}").is_err());
    }

    #[test]
    fn codec_rejects_excessive_depth_and_nonfinite_values_without_leaking_admission() {
        let before = controlled_memory_snapshot().2;
        let malformed = format!(
            "{}{}",
            "[".repeat(MAX_JSON_DEPTH + 1),
            "]".repeat(MAX_JSON_DEPTH + 1)
        );
        assert!(ViewQuery::decode(&malformed)
            .unwrap_err()
            .message()
            .contains("depth limit"));
        for value in [Value::Double(f64::INFINITY), Value::Float(f32::NAN)] {
            let mut query = select("SELECT 1 n");
            query.projection = vec![SelectItem::Expr {
                expr: Expr::BoundValue(value),
                alias: Some("n".into()),
            }];
            assert!(ViewQuery::new(query)
                .unwrap_err()
                .message()
                .contains("nonfinite"));
        }
        let mut query = select("SELECT 1 n");
        query.projection = vec![SelectItem::Expr {
            expr: Expr::Literal(Literal::Float(f64::INFINITY)),
            alias: Some("n".into()),
        }];
        assert!(ViewQuery::new(query)
            .unwrap_err()
            .message()
            .contains("nonfinite"));
        let mut query = select("SELECT 1 n");
        let mut expression = Expr::Literal(Literal::Int(1));
        for _ in 0..crate::engine::expr_program::MAX_EXPRESSION_DEPTH {
            expression = Expr::Binary {
                left: Box::new(expression),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(Literal::Int(1))),
            };
        }
        query.projection = vec![SelectItem::Expr {
            expr: expression,
            alias: Some("n".into()),
        }];
        assert!(ViewQuery::new(query)
            .unwrap_err()
            .message()
            .contains("expression"));
        let after = controlled_memory_snapshot().2;
        assert_eq!(after[8], before[8]);
        assert_eq!(after[12], before[12]);
    }

    #[test]
    fn codec_preserves_escaped_json_text_and_shared_encoding_admission() {
        let mut original = select("SELECT 1 n");
        original.projection = vec![SelectItem::Expr {
            expr: Expr::Literal(Literal::String("{[}]\\\"é\n".into())),
            alias: Some("n".into()),
        }];
        let before = controlled_memory_snapshot().2[8];
        let query = ViewQuery::new(original.clone()).unwrap();
        let charged = controlled_memory_snapshot().2[8];
        assert_eq!(
            charged - before,
            query.0.encoded.capacity()
                + std::mem::size_of::<QueryData>()
                + 2 * std::mem::size_of::<usize>()
        );
        let cloned = query.clone();
        assert_eq!(controlled_memory_snapshot().2[8], charged);
        let decoded = ViewQuery::decode(query.encoded()).unwrap();
        assert_eq!(decoded.select(), &original);
        drop(decoded);
        drop(query);
        assert_eq!(controlled_memory_snapshot().2[8], charged);
        drop(cloned);
        assert_eq!(controlled_memory_snapshot().2[8], before);
    }

    #[test]
    fn codec_runs_with_a_small_caller_stack() {
        let original = select(&format!("SELECT id{} AS n FROM t", "+1".repeat(62)));
        std::thread::Builder::new()
            .stack_size(64 * 1024)
            .spawn(move || {
                let query = ViewQuery::new(original).unwrap();
                let decoded = ViewQuery::decode(query.encoded()).unwrap();
                assert_eq!(query.encoded(), decoded.encoded());
                assert_eq!(controlled_memory_snapshot().2[12], 0);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn streaming_bounds_ignore_strings_and_enforce_container_and_byte_limits() {
        let encoded = serde_json::to_vec(&serde_json::json!({"s": "[]{}\\\"é\n"})).unwrap();
        for chunk in 1..encoded.len() {
            let mut bounds = JsonBounds::default();
            for slice in encoded.chunks(chunk) {
                bounds.scan(slice).unwrap();
            }
            bounds.finish().unwrap();
            assert_eq!(bounds.containers, 1);
        }
        let mut bounds = JsonBounds {
            bytes: MAX_JSON_BYTES,
            ..JsonBounds::default()
        };
        assert!(bounds.scan(b" ").unwrap_err().contains("64 MiB"));
        let mut bounds = JsonBounds {
            scalars: MAX_JSON_SCALARS,
            ..JsonBounds::default()
        };
        assert!(bounds.scan(b"0").unwrap_err().contains("scalar limit"));
        let mut bounds = JsonBounds {
            containers: MAX_JSON_CONTAINERS,
            ..JsonBounds::default()
        };
        assert!(bounds.scan(b"[").unwrap_err().contains("container limit"));
    }
}
