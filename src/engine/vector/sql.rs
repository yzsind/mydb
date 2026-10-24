//! The vector SQL function library, routed from `engine::expr` exactly like the
//! GIS `ST_*` surface in `engine::geo::sql`. Arguments arrive already evaluated;
//! any NULL argument short-circuits to SQL NULL (MySQL semantics).

use super::codec;
use super::distance::{self, Metric};
use crate::types::{QueryError, Value};

/// Every vector function name (upper-case). `engine::expr` checks this before
/// dispatching into [`eval`].
const VEC_FUNCTIONS: &[&str] = &[
    "STRING_TO_VECTOR",
    "TO_VECTOR",
    "VECTOR_TO_STRING",
    "FROM_VECTOR",
    "VECTOR_DIM",
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
];

/// Functions whose result column type is `VECTOR` (drives
/// `expr::function_result_type`).
const VEC_RETURNS_VECTOR: &[&str] = &["STRING_TO_VECTOR", "TO_VECTOR", "L2_NORMALIZE", "SUBVECTOR"];

/// True if `upper` (already upper-cased) names a vector function.
pub(crate) fn is_vec_function(upper: &str) -> bool {
    VEC_FUNCTIONS.contains(&upper)
}

/// True if `upper` names a vector function that returns a `VECTOR` value.
pub(crate) fn returns_vector(upper: &str) -> bool {
    VEC_RETURNS_VECTOR.contains(&upper)
}

/// Evaluate a vector function. `upper` is the upper-cased name, `name` the
/// original spelling (for error messages), `values` the evaluated arguments.
pub(crate) fn eval(upper: &str, name: &str, values: &[Value]) -> Result<Value, QueryError> {
    // NULL propagation: any NULL argument yields NULL.
    if values.iter().any(|v| matches!(v, Value::Null)) {
        return Ok(Value::Null);
    }
    match upper {
        "STRING_TO_VECTOR" | "TO_VECTOR" => {
            expect_args(name, values, 1, 1)?;
            let s = string_arg(&values[0], name)?;
            Ok(Value::Vector(codec::parse_text(&s)?))
        }
        "VECTOR_TO_STRING" | "FROM_VECTOR" => {
            expect_args(name, values, 1, 1)?;
            let v = vector_bytes(&values[0], name)?;
            Ok(Value::String(codec::to_text(&v)))
        }
        "VECTOR_DIM" => {
            expect_args(name, values, 1, 1)?;
            let v = vector_bytes(&values[0], name)?;
            Ok(Value::Int(codec::dim(&v) as i64))
        }
        "DISTANCE" | "VEC_DISTANCE" => {
            expect_args(name, values, 2, 3)?;
            let metric = if values.len() == 3 {
                let m = string_arg(&values[2], name)?;
                Metric::parse(&m).ok_or_else(|| {
                    codec::vector_err(format!(
                        "unknown distance metric '{m}' (expected L2, COSINE, or DOT)"
                    ))
                })?
            } else {
                Metric::L2
            };
            distance_value(name, &values[0], &values[1], metric)
        }
        "L2_DISTANCE" => {
            expect_args(name, values, 2, 2)?;
            distance_value(name, &values[0], &values[1], Metric::L2)
        }
        "COSINE_DISTANCE" => {
            expect_args(name, values, 2, 2)?;
            distance_value(name, &values[0], &values[1], Metric::Cosine)
        }
        "DOT_PRODUCT" | "INNER_PRODUCT" => {
            expect_args(name, values, 2, 2)?;
            // Zero-alloc fast path when both operands are already vector bytes.
            if let (Value::Vector(ba), Value::Vector(bb)) = (&values[0], &values[1]) {
                if ba.len() != bb.len() || ba.is_empty() {
                    return Err(codec::vector_err(format!(
                        "{name}: vector dimension mismatch ({} vs {})",
                        ba.len() / 4,
                        bb.len() / 4
                    )));
                }
                return Ok(Value::Double(-distance::display_distance_bytes(
                    ba,
                    bb,
                    Metric::Dot,
                )));
            }
            let (a, b) = two_vectors(name, &values[0], &values[1])?;
            Ok(Value::Double(-distance::display_distance(
                &a,
                &b,
                Metric::Dot,
            )))
        }
        // pgvector `negative_inner_product` — the value the `<#>` operator
        // orders by. Equals `-DOT_PRODUCT(a, b)`.
        "MAX_INNER_PRODUCT_DISTANCE" => {
            expect_args(name, values, 2, 2)?;
            let (a, b) = two_vectors(name, &values[0], &values[1])?;
            Ok(Value::Double(distance::display_distance(
                &a,
                &b,
                Metric::Dot,
            )))
        }
        // L2 norm `|v|` (pgvector `vector_norm`).
        "VECTOR_NORM" => {
            expect_args(name, values, 1, 1)?;
            let b = vector_bytes(&values[0], name)?;
            if b.is_empty() {
                return Err(codec::vector_err(format!("{name}: empty vector")));
            }
            Ok(Value::Double(distance::norm_bytes(&b)))
        }
        // Unit vector `v / |v|` (pgvector `l2_normalize`). A zero vector has no
        // direction, so it is returned unchanged (matching pgvector).
        "L2_NORMALIZE" => {
            expect_args(name, values, 1, 1)?;
            let b = vector_bytes(&values[0], name)?;
            if b.is_empty() {
                return Err(codec::vector_err(format!("{name}: empty vector")));
            }
            let mut out = codec::to_f32(&b);
            distance::normalize_in_place(&mut out);
            Ok(Value::Vector(codec::from_f32(&out)))
        }
        // 1-indexed slice `v[start .. start+count)` (pgvector `subvector`).
        "SUBVECTOR" => {
            expect_args(name, values, 3, 3)?;
            let f = codec::to_f32(&vector_bytes(&values[0], name)?);
            let start = int_arg(&values[1], name)?;
            let count = int_arg(&values[2], name)?;
            if start < 1 || count < 1 {
                return Err(codec::vector_err(format!(
                    "{name}: start and count must be >= 1 (got start={start}, count={count})"
                )));
            }
            let begin = (start - 1) as usize;
            let len = count as usize;
            if begin + len > f.len() {
                return Err(codec::vector_err(format!(
                    "{name}: slice [{start}, {}] exceeds vector dimension {}",
                    start + count - 1,
                    f.len()
                )));
            }
            Ok(Value::Vector(codec::from_f32(&f[begin..begin + len])))
        }
        _ => Err(codec::vector_err(format!("unknown vector function {name}"))),
    }
}

/// `CAST(x AS VECTOR)` / `CONVERT(x, VECTOR)`. NULL is handled by the caller.
pub(crate) fn cast_to_vector(value: &Value) -> Result<Value, QueryError> {
    Ok(Value::Vector(vector_bytes(value, "cast")?))
}

/// Compute a ranking distance between two vector operands, validating equal
/// dimensions.
fn distance_value(name: &str, a: &Value, b: &Value, metric: Metric) -> Result<Value, QueryError> {
    // Zero-alloc fast path: both operands are already in internal byte form —
    // the overwhelmingly common case of a `VECTOR` column scored against a
    // constant-folded query vector. A brute-force `ORDER BY DISTANCE(col, q)`
    // hits this once per row, so skipping the two `Vec<f32>` decodes and the
    // validating clone in `two_vectors` is the dominant brute-KNN win. Stored
    // column values were validated on write and the constant on fold, so a
    // re-validation here would be redundant work in the hot loop.
    if let (Value::Vector(ba), Value::Vector(bb)) = (a, b) {
        if ba.len() != bb.len() {
            return Err(codec::vector_err(format!(
                "{name}: vector dimension mismatch ({} vs {})",
                ba.len() / 4,
                bb.len() / 4
            )));
        }
        if ba.is_empty() {
            return Err(codec::vector_err(format!("{name}: empty vector")));
        }
        // Cosine similarity is undefined for a zero vector — the scalar
        // function returns NULL rather than a fabricated distance (pgvector
        // returns NaN; MySQL 9.x rejects). Only the explicit `DISTANCE()` /
        // `COSINE_DISTANCE()` projection reaches here; brute-KNN ordering uses
        // `ordering_distance_bytes` directly and is unaffected.
        if metric == Metric::Cosine
            && (distance::is_zero_norm_bytes(ba) || distance::is_zero_norm_bytes(bb))
        {
            return Ok(Value::Null);
        }
        return Ok(Value::Double(distance::display_distance_bytes(
            ba, bb, metric,
        )));
    }
    let (fa, fb) = two_vectors(name, a, b)?;
    if metric == Metric::Cosine && (distance::is_zero_norm(&fa) || distance::is_zero_norm(&fb)) {
        return Ok(Value::Null);
    }
    Ok(Value::Double(distance::display_distance(&fa, &fb, metric)))
}

/// Decode two operands to aligned `f32` slices, requiring equal dimension.
fn two_vectors(name: &str, a: &Value, b: &Value) -> Result<(Vec<f32>, Vec<f32>), QueryError> {
    let ba = vector_bytes(a, name)?;
    let bb = vector_bytes(b, name)?;
    let fa = codec::to_f32(&ba);
    let fb = codec::to_f32(&bb);
    if fa.len() != fb.len() {
        return Err(codec::vector_err(format!(
            "{name}: vector dimension mismatch ({} vs {})",
            fa.len(),
            fb.len()
        )));
    }
    if fa.is_empty() {
        return Err(codec::vector_err(format!("{name}: empty vector")));
    }
    Ok((fa, fb))
}

/// Extract internal vector bytes from a value, coercing text/bytes.
fn vector_bytes(v: &Value, name: &str) -> Result<Vec<u8>, QueryError> {
    match v {
        Value::Vector(b) => codec::normalize_internal(b),
        Value::String(s) | Value::Decimal(s) => codec::coerce_bytes_to_internal(s.as_bytes()),
        Value::Bytes(b) => codec::coerce_bytes_to_internal(b),
        other => Err(codec::vector_err(format!(
            "{name} expects a VECTOR argument, got {}",
            value_type_name(other)
        ))),
    }
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
