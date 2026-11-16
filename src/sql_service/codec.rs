use super::failure::{Failure, Result};
use crate::{
    engine::api::SessionContext,
    types::{Column, ColumnType, QueryError, Value},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Deserialize;
use serde_json::{json, value::RawValue, Value as Json};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlItem {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<Box<RawValue>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlRequest {
    pub database: Option<String>,
    pub sql: Option<String>,
    #[serde(default)]
    pub params: Vec<Box<RawValue>>,
    pub statements: Option<Vec<SqlItem>>,
    #[serde(default)]
    pub atomic: bool,
    pub timeout_ms: Option<u64>,
}
impl SqlRequest {
    pub fn items(self) -> Result<(Option<String>, Vec<SqlItem>, bool, Option<u64>)> {
        let items = match (self.sql, self.statements) {
            (Some(sql), None) => vec![SqlItem {
                sql,
                params: self.params,
            }],
            (None, Some(v)) if self.params.is_empty() => v,
            _ => return Err(Failure::bad("Specify exactly one of sql or statements")),
        };
        if items.is_empty() || items.len() > 1024 {
            return Err(Failure::bad("A request requires 1 through 1024 statements"));
        }
        Ok((self.database, items, self.atomic, self.timeout_ms))
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Typed {
    #[serde(rename = "type")]
    kind: String,
    value: Json,
    datatype: Option<String>,
    dimension: Option<u32>,
    bits: Option<u8>,
}
fn invalid() -> QueryError {
    crate::error::catalog::ER_WRONG_ARGUMENTS.error("Invalid REST SQL parameter")
}
fn decimal(text: &str) -> std::result::Result<Value, QueryError> {
    if text.len() > 256 {
        return Err(invalid());
    }
    let (magnitude, scale) =
        crate::util::decimal::parse_exact_decimal_big(text).ok_or_else(invalid)?;
    let value = crate::util::decimal::format_big_decimal_parts(&magnitude, scale);
    if value.bytes().filter(u8::is_ascii_digit).count() > 65 || scale > 30 {
        return Err(invalid());
    }
    Ok(Value::Decimal(value))
}
fn binary_column(c: &Column) -> bool {
    matches!(c.column_type, ColumnType::Geometry | ColumnType::Bit { .. })
        || matches!(c.column_type, ColumnType::Blob | ColumnType::VarString)
            && c.source.collation_id == Some(crate::charset::BINARY_COLLATION_ID)
}
pub fn param(raw: &RawValue, s: &SessionContext) -> std::result::Result<Value, QueryError> {
    let text = raw.get();
    let j: Json = serde_json::from_str(text).map_err(|_| invalid())?;
    match j {
        Json::Null => Ok(Value::Null),
        Json::Bool(v) => Ok(Value::Int(i64::from(v))),
        Json::String(v) => Ok(Value::String(v)),
        Json::Number(n) => {
            if text.contains(['.', 'e', 'E']) {
                return decimal(text);
            }
            let value = n.as_i64().ok_or_else(invalid)?;
            if value.unsigned_abs() > 9007199254740991 {
                return Err(invalid());
            }
            Ok(Value::Int(value))
        }
        Json::Object(_) => {
            let t: Typed = serde_json::from_str(text).map_err(|_| invalid())?;
            let string = || t.value.as_str().ok_or_else(invalid);
            if t.kind != "vector" && (t.datatype.is_some() || t.dimension.is_some())
                || t.kind != "bit" && t.bits.is_some()
            {
                return Err(invalid());
            }
            match t.kind.as_str() {
                "int64" => Ok(Value::Int(string()?.parse().map_err(|_| invalid())?)),
                "uint64" => Ok(Value::UInt(string()?.parse().map_err(|_| invalid())?)),
                "decimal" => decimal(string()?),
                "float32" | "float64" => {
                    let n = t.value.as_f64().ok_or_else(invalid)?;
                    if !n.is_finite() {
                        return Err(invalid());
                    }
                    if t.kind == "float32" {
                        let v = n as f32;
                        if !v.is_finite() {
                            return Err(invalid());
                        }
                        Ok(Value::Float(v))
                    } else {
                        Ok(Value::Double(n))
                    }
                }
                "string" => Ok(Value::String(string()?.into())),
                "binary" => Ok(Value::Bytes(
                    STANDARD.decode(string()?).map_err(|_| invalid())?,
                )),
                "json" => Ok(Value::Json(crate::engine::json::parse_text(
                    string()?,
                    1,
                    "REST parameter",
                )?)),
                "geometry" => Ok(Value::Geometry(crate::engine::geo::normalize_internal(
                    &STANDARD.decode(string()?).map_err(|_| invalid())?,
                )?)),
                "vector" => {
                    let datatype = crate::vector::VectorDataType::parse(
                        t.datatype.as_deref().ok_or_else(invalid)?,
                    )
                    .ok_or_else(invalid)?;
                    let v = crate::engine::vector::codec::parse_typed_text(string()?, datatype)?;
                    if Some(v.dimension) != t.dimension {
                        return Err(invalid());
                    }
                    Ok(Value::Vector(v))
                }
                "bit" => {
                    let bits = t.bits.ok_or_else(invalid)?;
                    let v: u64 = string()?.parse().map_err(|_| invalid())?;
                    if !(1..=64).contains(&bits) || (bits < 64 && v >= (1u64 << bits)) {
                        return Err(invalid());
                    }
                    Ok(Value::UInt(v))
                }
                "year" => {
                    let v: u16 = string()?.parse().map_err(|_| invalid())?;
                    if v == 0 {
                        Ok(Value::Year(0))
                    } else if (1901..=2155).contains(&v) {
                        Ok(Value::Year((v - 1900) as u8))
                    } else {
                        Err(invalid())
                    }
                }
                "date" | "datetime" | "time" | "timestamp" => {
                    let kind = match t.kind.as_str() {
                        "date" => "DATE",
                        "time" => "TIME(6)",
                        _ => "DATETIME(6)",
                    };
                    let expr = crate::parser::parse_expr_fragment(&format!("CAST(? AS {kind})"))
                        .map_err(|_| invalid())?;
                    let value = crate::engine::expr::with_session_expr_info(s, || {
                        crate::engine::params::with_params(
                            vec![Value::String(string()?.into())],
                            || crate::engine::expr::eval_const_runtime(&expr),
                        )
                    })?;
                    if matches!(value, Value::Null) {
                        return Err(invalid());
                    }
                    if t.kind == "timestamp" {
                        if let Value::DateTime(v) = value {
                            Ok(Value::Timestamp(
                                crate::engine::expr::local_datetime_to_utc_timestamp_micros_in_tz(
                                    v,
                                    &s.sysvars.time_zone,
                                )?,
                            ))
                        } else {
                            Err(invalid())
                        }
                    } else {
                        Ok(value)
                    }
                }
                _ => Err(invalid()),
            }
        }
        _ => Err(invalid()),
    }
}
pub fn column(c: &Column) -> Json {
    let encoding = match c.column_type {
        ColumnType::Tiny
        | ColumnType::Short
        | ColumnType::MediumInt
        | ColumnType::Long
        | ColumnType::LongLong
        | ColumnType::Year => "integer_string",
        ColumnType::Decimal { .. } => "decimal_string",
        ColumnType::Float | ColumnType::Double => "float",
        ColumnType::Blob
        | ColumnType::VarString
        | ColumnType::Geometry
        | ColumnType::Bit { .. }
            if binary_column(c) =>
        {
            "base64"
        }
        ColumnType::Json => "json_text",
        ColumnType::Vector => "vector_text",
        _ => "text",
    };
    let (kind, precision, scale) = match c.column_type {
        ColumnType::Tiny => ("TINYINT", None, None),
        ColumnType::Short => ("SMALLINT", None, None),
        ColumnType::MediumInt => ("MEDIUMINT", None, None),
        ColumnType::Long => ("INT", None, None),
        ColumnType::LongLong => ("BIGINT", None, None),
        ColumnType::Decimal { precision, scale } => ("DECIMAL", Some(precision), Some(scale)),
        ColumnType::Float => ("FLOAT", None, None),
        ColumnType::Double => ("DOUBLE", None, None),
        ColumnType::Date => ("DATE", None, None),
        ColumnType::Time => ("TIME", None, None),
        ColumnType::DateTime => ("DATETIME", None, None),
        ColumnType::Timestamp => ("TIMESTAMP", None, None),
        ColumnType::Year => ("YEAR", None, None),
        ColumnType::Blob => ("BLOB", None, None),
        ColumnType::Json => ("JSON", None, None),
        ColumnType::Geometry => ("GEOMETRY", None, None),
        ColumnType::Vector => ("VECTOR", None, None),
        ColumnType::Bit { .. } => ("BIT", None, None),
        ColumnType::VarString => ("VARCHAR", None, None),
    };
    json!({"name":c.name,"sql_type":kind,"encoding":encoding,"unsigned":c.source.flags&32!=0,"length":c.source.length,"precision":precision,"scale":scale,"decimals":c.source.decimals,"collation_id":c.source.collation_id,"schema":c.source.schema,"table":c.source.table,"org_table":c.source.org_table,"org_name":c.source.org_name,"vector_type":c.source.vector_type.map(|(t,d)|json!({"datatype":t.name(),"dimension":d})),"bits":if let ColumnType::Bit{bits}=c.column_type{Some(bits)}else{None}})
}
pub fn row(values: Vec<Value>, columns: &[Column]) -> Result<Json> {
    if values.len() != columns.len() {
        return Err(Failure::internal("SQL result column count mismatch"));
    }
    let mut out = Vec::with_capacity(values.len());
    for (v, c) in values.into_iter().zip(columns) {
        let j = match v {
            Value::Null => Json::Null,
            Value::String(value) if binary_column(c) => {
                Json::String(STANDARD.encode(value.as_bytes()))
            }
            Value::String(value) => Json::String(value),
            Value::SmallString(value) if binary_column(c) => {
                Json::String(STANDARD.encode(value.as_slice()))
            }
            Value::Float(v) => {
                if v.is_finite() {
                    Json::Number(
                        ryu::Buffer::new()
                            .format_finite(v)
                            .parse()
                            .map_err(|_| Failure::bad("Invalid FLOAT result"))?,
                    )
                } else {
                    Json::Null
                }
            }
            Value::Double(v) => float(v),
            Value::Bytes(v) if !binary_column(c) => Json::String(
                String::from_utf8(v)
                    .map_err(|_| Failure::internal("Non-UTF8 bytes in a text result column"))?,
            ),
            Value::Bytes(v) => Json::String(STANDARD.encode(v)),
            Value::Geometry(v) => Json::String(STANDARD.encode(v)),
            Value::UInt(v) if matches!(c.column_type, ColumnType::Bit { .. }) => {
                let ColumnType::Bit { bits } = c.column_type else {
                    unreachable!()
                };
                Json::String(STANDARD.encode(&v.to_be_bytes()[8 - (bits as usize).div_ceil(8)..]))
            }
            Value::DateTime(v) | Value::Timestamp(v) => {
                let mut b = [0; 32];
                Json::String(
                    String::from_utf8_lossy(crate::types::write_datetime_canonical_with_fsp(
                        v,
                        temporal_fsp(c.source.decimals, crate::types::datetime_components(v).6),
                        &mut b,
                    ))
                    .into_owned(),
                )
            }
            Value::Time(v) => {
                let mut b = [0; 32];
                Json::String(
                    String::from_utf8_lossy(crate::types::write_time_canonical_with_fsp(
                        v,
                        temporal_fsp(c.source.decimals, (v.unsigned_abs() % 1_000_000) as u32),
                        &mut b,
                    ))
                    .into_owned(),
                )
            }
            other => Json::String(other.to_display_string()),
        };
        out.push(j);
    }
    Ok(Json::Array(out))
}
/// Fund SQL and bound parameters independently of the transport request. The
/// worker may outlive a cancelled HTTP/SDK request and must retain its charge.
pub fn input_memory_bytes(items: &[SqlItem]) -> Result<usize> {
    let overflow = || Failure::unavailable("SQL input memory size exceeds supported range");
    let mut bytes = std::mem::size_of::<SqlItem>()
        .checked_mul(items.len())
        .ok_or_else(overflow)?;
    for item in items {
        bytes = bytes.checked_add(item.sql.len()).ok_or_else(overflow)?;
        for parameter in &item.params {
            bytes = bytes
                .checked_add(parameter.get().len())
                .and_then(|n| {
                    n.checked_add(
                        std::mem::size_of::<Value>() + std::mem::size_of::<Box<RawValue>>(),
                    )
                })
                .ok_or_else(overflow)?;
        }
    }
    bytes
        .checked_mul(4)
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(overflow)
}

// Runtime temporal parameters can carry more precision than conservative
// expression metadata. Preserve all value digits while retaining declared padding.
fn temporal_fsp(declared: Option<u8>, fraction: u32) -> Option<u8> {
    if fraction == 0 {
        return declared;
    }
    let mut digits = 6;
    let mut fraction = fraction;
    while fraction % 10 == 0 {
        fraction /= 10;
        digits -= 1;
    }
    Some(declared.unwrap_or(0).max(digits))
}

fn float(v: f64) -> Json {
    if v.is_finite() {
        json!(v)
    } else {
        json!({"non_finite":if v.is_nan(){"nan"}else if v.is_sign_positive(){"infinity"}else{"-infinity"}})
    }
}

/// Measure encoded payloads without allocating a second serialized copy.
pub fn encoded_len<T: serde::Serialize + ?Sized>(value: &T) -> Result<usize> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("JSON length overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value)
        .map_err(|_| Failure::internal("Result encoding failed"))?;
    Ok(counter.0)
}

#[cfg(test)]
mod audit_tests {
    use super::*;
    #[test]
    fn rest_audit_decimal_binding_is_canonical_and_bounded() {
        let session = SessionContext::default();
        for (raw, expected) in [
            ("1e5", "100000"),
            (r#"{"type":"decimal","value":" +007.10 "}"#, "7.10"),
            (r#"{"type":"decimal","value":".5"}"#, "0.5"),
        ] {
            let raw = RawValue::from_string(raw.into()).unwrap();
            assert_eq!(param(&raw, &session).unwrap().to_display_string(), expected);
        }
        for raw in ["1e65", "1e-31"] {
            assert!(param(&RawValue::from_string(raw.into()).unwrap(), &session).is_err());
        }
    }
    #[test]
    fn rest_audit_result_encoding_matches_column_collation() {
        let mut column = Column {
            name: "s".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        };
        column.source.collation_id = Some(63);
        assert_eq!(super::column(&column)["encoding"], "base64");
        assert_eq!(
            row(vec![Value::String("abc".into())], &[column.clone()]).unwrap(),
            json!(["YWJj"])
        );
        column.column_type = ColumnType::Blob;
        column.source.collation_id = Some(255);
        assert_eq!(super::column(&column)["encoding"], "text");
        assert_eq!(
            row(vec![Value::Bytes(b"abc".to_vec())], &[column.clone()]).unwrap(),
            json!(["abc"])
        );
        column.column_type = ColumnType::Float;
        assert_eq!(
            row(vec![Value::Float(0.1)], &[column]).unwrap().to_string(),
            "[0.1]"
        );
    }
}

/// Conservative retained allocation estimate for buffered JSON. B-tree nodes
/// can have spare capacity; include that in addition to strings and arrays.
pub(super) fn retained_bytes(value: &Json) -> usize {
    match value {
        Json::String(text) => text.capacity(),
        Json::Array(values) => values
            .capacity()
            .saturating_mul(std::mem::size_of::<Json>())
            .saturating_add(values.iter().map(retained_bytes).sum::<usize>()),
        Json::Object(values) => values
            .len()
            .saturating_mul(
                2 * std::mem::size_of::<(String, Json)>() + 3 * std::mem::size_of::<usize>(),
            )
            .saturating_add(
                values
                    .iter()
                    .map(|(key, value)| key.capacity().saturating_add(retained_bytes(value)))
                    .sum::<usize>(),
            ),
        Json::Number(number) => number.to_string().len(),
        _ => 0,
    }
}
