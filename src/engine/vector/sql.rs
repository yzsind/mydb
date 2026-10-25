//! SQL vector functions with native datatypes and explicit binary construction.
use super::{
    codec,
    distance::{self, Metric},
};
use crate::{
    types::{QueryError, Value},
    vector::{VectorDataType, VectorValue},
};
use std::borrow::Cow;

const VEC_FUNCTIONS: &[&str] = &[
    "STRING_TO_VECTOR",
    "TO_VECTOR",
    "VECTOR_TO_STRING",
    "FROM_VECTOR",
    "VECTOR_DIM",
    "VECTOR_DATATYPE",
    "VECTOR_FROM_BINARY",
    "VECTOR_TO_BINARY",
    "DISTANCE",
    "VEC_DISTANCE",
    "L2_DISTANCE",
    "COSINE_DISTANCE",
    "DOT_PRODUCT",
    "INNER_PRODUCT",
    "VECTOR_NORM",
    "L2_NORMALIZE",
    "SUBVECTOR",
    "MAX_INNER_PRODUCT_DISTANCE",
    "HAMMING_DISTANCE",
    "JACCARD_DISTANCE",
];
pub(crate) fn is_vec_function(upper: &str) -> bool {
    VEC_FUNCTIONS.contains(&upper)
}
pub(crate) fn returns_vector(upper: &str) -> bool {
    matches!(
        upper,
        "STRING_TO_VECTOR" | "TO_VECTOR" | "VECTOR_FROM_BINARY" | "L2_NORMALIZE" | "SUBVECTOR"
    )
}

pub(crate) fn eval(upper: &str, name: &str, values: &[Value]) -> Result<Value, QueryError> {
    let (min, max) = match upper {
        "STRING_TO_VECTOR" | "TO_VECTOR" => (1, 2),
        "VECTOR_FROM_BINARY" | "SUBVECTOR" => (3, 3),
        "DISTANCE" | "VEC_DISTANCE" => (2, 3),
        "L2_DISTANCE"
        | "COSINE_DISTANCE"
        | "DOT_PRODUCT"
        | "INNER_PRODUCT"
        | "MAX_INNER_PRODUCT_DISTANCE"
        | "HAMMING_DISTANCE"
        | "JACCARD_DISTANCE" => (2, 2),
        _ => (1, 1),
    };
    if values.iter().any(|v| matches!(v, Value::Null)) {
        return Ok(Value::Null);
    }
    expect_args(name, values, min, max)?;
    match upper {
        "STRING_TO_VECTOR" | "TO_VECTOR" => {
            let datatype = if values.len() == 2 {
                datatype_arg(&values[1], name)?
            } else {
                VectorDataType::Float32
            };
            if let Value::Vector(value) = &values[0] {
                return codec::convert_value(value, datatype, None).map(Value::Vector);
            }
            Ok(Value::Vector(codec::parse_typed_text(
                &string_arg(&values[0], name)?,
                datatype,
            )?))
        }
        "VECTOR_FROM_BINARY" => {
            let datatype = datatype_arg(&values[1], name)?;
            let dimension = u32::try_from(int_arg(&values[2], name)?)
                .map_err(|_| codec::vector_err("invalid VECTOR dimension"))?;
            let payload = match &values[0] {
                Value::Bytes(bytes) => bytes.as_slice(),
                _ => {
                    return Err(codec::vector_err(
                        "VECTOR_FROM_BINARY requires a binary payload",
                    ))
                }
            };
            codec::validate_internal(&crate::vector::VectorRef {
                datatype,
                dimension,
                payload,
            })?;
            Ok(Value::Vector(VectorValue::new(
                datatype,
                dimension,
                payload.to_vec(),
            )))
        }
        "VECTOR_TO_BINARY" => Ok(Value::Bytes(
            vector_value(&values[0], name)?.payload.clone(),
        )),
        "VECTOR_DATATYPE" => Ok(Value::String(
            vector_value(&values[0], name)?.datatype.name().into(),
        )),
        "VECTOR_TO_STRING" | "FROM_VECTOR" => Ok(Value::String(codec::to_text(
            vector_value(&values[0], name)?.as_ref(),
        ))),
        "VECTOR_DIM" => Ok(Value::Int(vector_value(&values[0], name)?.dimension as i64)),
        "VECTOR_NORM" | "L2_NORMALIZE" => {
            let v = vector_value(&values[0], name)?;
            if v.datatype == VectorDataType::Binary {
                return Err(codec::vector_err(format!(
                    "{name} does not support BINARY vectors"
                )));
            }
            let norm = if v.datatype == VectorDataType::Float32 {
                distance::norm_bytes(&v.payload)
            } else {
                (0..v.dimension as usize)
                    .map(|i| v.element(i).powi(2))
                    .sum::<f64>()
                    .sqrt()
            };
            if upper == "VECTOR_NORM" {
                return Ok(Value::Double(norm));
            }
            if v.dimension > VectorDataType::Float32.max_dimension() {
                return Err(codec::vector_err(
                    "L2_NORMALIZE result exceeds FLOAT32 dimension limit",
                ));
            }
            let values: Vec<f32> = (0..v.dimension as usize)
                .map(|i| {
                    if norm == 0.0 {
                        v.element(i) as f32
                    } else {
                        (v.element(i) / norm) as f32
                    }
                })
                .collect();
            Ok(Value::Vector(codec::from_f32(&values)))
        }
        "SUBVECTOR" => {
            let v = vector_value(&values[0], name)?;
            let start = int_arg(&values[1], name)?;
            let count = int_arg(&values[2], name)?;
            if start < 1
                || count < 1
                || start
                    .checked_add(count - 1)
                    .is_none_or(|end| end > v.dimension as i64)
            {
                return Err(codec::vector_err(
                    "SUBVECTOR range exceeds vector dimension",
                ));
            }
            let begin = (start - 1) as usize;
            let count = count as u32;
            let payload = if v.datatype == VectorDataType::Binary {
                let mut bytes = vec![0; v.datatype.payload_len(count)];
                for i in 0..count as usize {
                    if v.element(begin + i) != 0.0 {
                        bytes[i / 8] |= 1 << (i % 8);
                    }
                }
                bytes
            } else {
                let width = v.datatype.payload_len(1);
                v.payload[begin * width..(begin + count as usize) * width].to_vec()
            };
            Ok(Value::Vector(VectorValue::new(v.datatype, count, payload)))
        }
        _ => {
            let explicit_binary = matches!(upper, "HAMMING_DISTANCE" | "JACCARD_DISTANCE")
                || (matches!(upper, "DISTANCE" | "VEC_DISTANCE")
                    && values.len() == 3
                    && Metric::parse(&string_arg(&values[2], name)?)
                        .is_some_and(|m| matches!(m, Metric::Hamming | Metric::Jaccard)));
            let binary = explicit_binary
                || matches!(&values[0], Value::Vector(v) if v.datatype == VectorDataType::Binary)
                || matches!(&values[1], Value::Vector(v) if v.datatype == VectorDataType::Binary);
            let a = vector_operand(&values[0], name, binary)?;
            let b = vector_operand(&values[1], name, binary)?;
            let metric = match upper {
                "L2_DISTANCE" => Metric::L2,
                "COSINE_DISTANCE" => Metric::Cosine,
                "DOT_PRODUCT" | "INNER_PRODUCT" | "MAX_INNER_PRODUCT_DISTANCE" => Metric::Dot,
                "HAMMING_DISTANCE" => Metric::Hamming,
                "JACCARD_DISTANCE" => Metric::Jaccard,
                "DISTANCE" | "VEC_DISTANCE" if values.len() == 3 => {
                    Metric::parse(&string_arg(&values[2], name)?)
                        .ok_or_else(|| codec::vector_err("unknown VECTOR distance metric"))?
                }
                "DISTANCE" | "VEC_DISTANCE" => {
                    if binary {
                        Metric::Hamming
                    } else {
                        Metric::L2
                    }
                }
                _ => return Err(codec::vector_err(format!("unknown vector function {name}"))),
            };
            let d = distance::typed_ordering_distance(a.as_ref(), b.as_ref(), metric)?;
            if d.is_nan() {
                return Ok(Value::Null);
            }
            Ok(Value::Double(
                if matches!(upper, "DOT_PRODUCT" | "INNER_PRODUCT") {
                    -d
                } else if metric == Metric::L2 {
                    d.sqrt()
                } else {
                    d
                },
            ))
        }
    }
}

pub(crate) fn cast_to_vector(value: &Value) -> Result<Value, QueryError> {
    codec::coerce_value(value.clone(), VectorDataType::Float32, None).map(Value::Vector)
}

fn datatype_arg(value: &Value, name: &str) -> Result<VectorDataType, QueryError> {
    let name = string_arg(value, name)?;
    VectorDataType::parse(&name)
        .ok_or_else(|| codec::vector_err(format!("unknown VECTOR datatype '{name}'")))
}

fn vector_value<'a>(v: &'a Value, name: &str) -> Result<Cow<'a, VectorValue>, QueryError> {
    vector_operand(v, name, false)
}
fn vector_operand<'a>(
    v: &'a Value,
    name: &str,
    binary: bool,
) -> Result<Cow<'a, VectorValue>, QueryError> {
    if let Value::Vector(v) = v {
        return Ok(Cow::Borrowed(v));
    }
    let text = string_arg(v, name)?;
    Ok(Cow::Owned(codec::parse_typed_text(
        &text,
        if binary {
            VectorDataType::Binary
        } else {
            VectorDataType::Float32
        },
    )?))
}

/// Extract a string argument (for the metric name / text-to-vector input).
fn string_arg(v: &Value, name: &str) -> Result<String, QueryError> {
    match v {
        Value::String(s) | Value::Decimal(s) => Ok(s.clone()),
        Value::Bytes(b) => Ok(String::from_utf8_lossy(b).into_owned()),
        Value::Vector(b) => Ok(codec::to_text(b)),
        other => Err(codec::vector_err(format!(
            "{name} expects a string argument, got {}",
            value_type_name(other)
        ))),
    }
}

/// Extract an integer argument (for `SUBVECTOR` start / count). Accepts the
/// integer value kinds and a numeric string.
fn int_arg(v: &Value, name: &str) -> Result<i64, QueryError> {
    match v {
        Value::Int(i) => Ok(*i),
        Value::UInt(u) => Ok(*u as i64),
        Value::String(s) | Value::Decimal(s) => s.trim().parse::<i64>().map_err(|_| {
            codec::vector_err(format!("{name} expects an integer argument, got '{s}'"))
        }),
        other => Err(codec::vector_err(format!(
            "{name} expects an integer argument, got {}",
            value_type_name(other)
        ))),
    }
}

fn expect_args(name: &str, values: &[Value], min: usize, max: usize) -> Result<(), QueryError> {
    if values.len() < min || values.len() > max {
        let want = if min == max {
            format!("{min}")
        } else {
            format!("{min}..{max}")
        };
        return Err(codec::vector_err(format!(
            "{name} takes {want} arguments, got {}",
            values.len()
        )));
    }
    Ok(())
}

fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "NULL",
        Value::Int(_) | Value::UInt(_) => "integer",
        Value::Float(_) | Value::Double(_) => "float",
        Value::Decimal(_)
        | Value::DecimalCompact(..)
        | Value::Decimal128(..)
        | Value::DecimalWide(..) => "decimal",
        Value::String(_) => "string",
        Value::Bytes(_) => "bytes",
        Value::Json(_) => "json",
        Value::Geometry(_) => "geometry",
        Value::Vector(_) => "vector",
        _ => "value",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(values: &[f32]) -> Value {
        Value::Vector(codec::from_f32(values))
    }

    #[test]
    fn string_to_vector_round_trip() {
        let got = eval(
            "STRING_TO_VECTOR",
            "string_to_vector",
            &[Value::String("[1, 2.5, -3]".into())],
        )
        .unwrap();
        assert_eq!(got, v(&[1.0, 2.5, -3.0]));
        let back = eval("VECTOR_TO_STRING", "vector_to_string", &[got]).unwrap();
        assert_eq!(back, Value::String("[1,2.5,-3]".into()));
    }

    #[test]
    fn vector_dim() {
        let got = eval("VECTOR_DIM", "vector_dim", &[v(&[1.0, 2.0, 3.0, 4.0])]).unwrap();
        assert_eq!(got, Value::Int(4));
    }

    #[test]
    fn distances() {
        let a = v(&[0.0, 0.0]);
        let b = v(&[3.0, 4.0]);
        // L2 = 5
        let d = eval("DISTANCE", "distance", &[a.clone(), b.clone()]).unwrap();
        assert_eq!(d, Value::Double(5.0));
        let d = eval("L2_DISTANCE", "l2_distance", &[a.clone(), b.clone()]).unwrap();
        assert_eq!(d, Value::Double(5.0));
        // DOT_PRODUCT = 0
        let d = eval("DOT_PRODUCT", "dot_product", &[a.clone(), b.clone()]).unwrap();
        assert_eq!(d, Value::Double(0.0));
        // explicit DOT distance = -0 = 0
        let d = eval(
            "DISTANCE",
            "distance",
            &[a.clone(), b.clone(), Value::String("DOT".into())],
        )
        .unwrap();
        assert_eq!(d, Value::Double(0.0));
    }

    #[test]
    fn null_propagates() {
        let got = eval("DISTANCE", "distance", &[Value::Null, v(&[1.0])]).unwrap();
        assert_eq!(got, Value::Null);
    }

    #[test]
    fn vector_norm_l2_length() {
        let got = eval("VECTOR_NORM", "vector_norm", &[v(&[3.0, 4.0])]).unwrap();
        assert_eq!(got, Value::Double(5.0));
        // Zero vector has zero norm.
        let got = eval("VECTOR_NORM", "vector_norm", &[v(&[0.0, 0.0, 0.0])]).unwrap();
        assert_eq!(got, Value::Double(0.0));
    }

    #[test]
    fn l2_normalize_unit_norm() {
        let got = eval("L2_NORMALIZE", "l2_normalize", &[v(&[3.0, 4.0])]).unwrap();
        assert_eq!(got, v(&[0.6, 0.8]));
        // A zero vector has no direction — returned unchanged (pgvector).
        let got = eval("L2_NORMALIZE", "l2_normalize", &[v(&[0.0, 0.0])]).unwrap();
        assert_eq!(got, v(&[0.0, 0.0]));
    }

    #[test]
    fn subvector_slice_is_one_indexed() {
        let got = eval(
            "SUBVECTOR",
            "subvector",
            &[v(&[10.0, 20.0, 30.0, 40.0]), Value::Int(2), Value::Int(2)],
        )
        .unwrap();
        assert_eq!(got, v(&[20.0, 30.0]));
        // Out-of-range slice errors rather than silently clamping.
        assert!(eval(
            "SUBVECTOR",
            "subvector",
            &[v(&[1.0, 2.0]), Value::Int(2), Value::Int(5)],
        )
        .is_err());
        // start < 1 is rejected.
        assert!(eval(
            "SUBVECTOR",
            "subvector",
            &[v(&[1.0, 2.0]), Value::Int(0), Value::Int(1)],
        )
        .is_err());
    }

    #[test]
    fn max_inner_product_distance_negates_dot() {
        // dot([1,2],[3,4]) = 11 → negative inner product = -11.
        let got = eval(
            "MAX_INNER_PRODUCT_DISTANCE",
            "max_inner_product_distance",
            &[v(&[1.0, 2.0]), v(&[3.0, 4.0])],
        )
        .unwrap();
        assert_eq!(got, Value::Double(-11.0));
    }

    #[test]
    fn dimension_mismatch_errors() {
        let err = eval("DISTANCE", "distance", &[v(&[1.0, 2.0]), v(&[1.0])]).unwrap_err();
        assert!(err.message().contains("dimension mismatch"));
    }

    #[test]
    fn cast_from_text() {
        let got = cast_to_vector(&Value::String("[1,2,3]".into())).unwrap();
        assert_eq!(got, v(&[1.0, 2.0, 3.0]));
    }
}
