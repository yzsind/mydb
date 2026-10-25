//! Native vector codecs, strict numeric conversion and canonical array text.
//!
//! Fixed-dimension heap lanes keep native little-endian payloads and obtain
//! datatype/dimension from their row layout. Standalone values, spills and
//! replication envelopes retain the explicit domain. Binary payloads pack bits
//! least-significant bit first. Exact kernels borrow payloads without per-row
//! FLOAT32 materialization.

use crate::vector::{VectorDataType, VectorInput, VectorValue};
use std::cmp::Ordering;

use crate::types::QueryError;

/// Portable row-binlog envelope for VECTOR values encoded as a MySQL LONG
/// BLOB. MySQL 8.0 replicas can store it losslessly in a BLOB column; mydb
/// replicas recognize the magic and recover the explicit datatype, dimension and native payload.
pub(crate) const VECTOR_BINLOG_MAGIC: &[u8; 8] = b"MYDBVEC2";

/// Build a `QueryError` for an invalid VECTOR value/operation. Centralised so
/// the error code / SQLSTATE conventions stay consistent across the module.
#[inline]
pub(crate) fn vector_err(message: impl Into<String>) -> QueryError {
    super::VectorError::InvalidValue(message.into()).into_query_error()
}

/// Maximum number of dimensions a single `VECTOR` value may carry. Matches
/// MySQL 9.0's `VECTOR` cap (16383 four-byte elements ≈ 64 KiB).
pub(crate) const MAX_DIM: usize = 16383;

pub(crate) fn to_f32<T: VectorInput + ?Sized>(internal: &T) -> Vec<f32> {
    (0..internal.dimension() as usize)
        .map(|i| internal.element(i) as f32)
        .collect()
}

pub(crate) fn from_f32(values: &[f32]) -> VectorValue {
    let mut out = Vec::with_capacity(values.len() * 4);
    for &v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out.into()
}

pub(crate) fn validate_internal<T: VectorInput + ?Sized>(value: &T) -> Result<(), QueryError> {
    let datatype = value.datatype();
    let dimension = value.dimension();
    let bytes = value.payload();
    if dimension == 0
        || dimension > datatype.max_dimension()
        || bytes.len() != datatype.payload_len(dimension)
    {
        return Err(vector_err(format!(
            "invalid {datatype} VECTOR dimension or payload length"
        )));
    }
    let finite = match datatype {
        VectorDataType::Float32 => bytes
            .chunks_exact(4)
            .all(|c| u32::from_le_bytes(c.try_into().unwrap()) & 0x7f80_0000 != 0x7f80_0000),
        VectorDataType::Float16 => bytes
            .chunks_exact(2)
            .all(|c| u16::from_le_bytes(c.try_into().unwrap()) & 0x7c00 != 0x7c00),
        VectorDataType::BFloat16 => bytes
            .chunks_exact(2)
            .all(|c| u16::from_le_bytes(c.try_into().unwrap()) & 0x7f80 != 0x7f80),
        VectorDataType::Int8 => true,
        VectorDataType::Binary => {
            dimension % 8 == 0 || bytes.last().is_some_and(|b| b >> (dimension % 8) == 0)
        }
    };
    if !finite {
        return Err(vector_err(
            "invalid VECTOR value: non-finite element or nonzero binary padding",
        ));
    }
    Ok(())
}

pub(crate) fn normalize_internal<T: VectorInput + ?Sized>(
    value: &T,
) -> Result<VectorValue, QueryError> {
    validate_internal(value)?;
    Ok(VectorValue::new(
        value.datatype(),
        value.dimension(),
        value.payload().to_vec(),
    ))
}

#[cfg(test)]
pub(crate) fn normalize_internal_owned(bytes: VectorValue) -> Result<VectorValue, QueryError> {
    validate_internal(&bytes)?;
    Ok(bytes)
}

/// Powers of ten that are *exactly* representable as `f32` (10^k = 2^k·5^k;
/// 5^10 = 9 765 625 < 2^24, so 10^0..=10^10 are exact, 10^11 is not).
const POW10_F32: [f32; 11] = [1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10];

/// Correctly-rounded fast parse of one `f32` decimal element starting at `*i`
/// in `b`, advancing `*i` past the number on success and returning `Some(v)`;
/// returns `None` (with `*i` unspecified — the caller resets) when the element
/// falls outside the provably-exact fast path and must defer to `f32::from_str`.
///
/// The number ends at the first whitespace, `,`, or end of input. The fast path
/// is Clinger's: when the significant digits form an integer `m <= 2^24`
/// (exactly representable in `f32`) and the decimal exponent `e` satisfies
/// `|e| <= 10` (so `10^|e|` is exactly representable in `f32`), the true value
/// `m · 10^e` is computed with a *single* IEEE `f32` multiply or divide. Both
/// operands are exact, so the one rounding is the correctly-rounded result —
/// bit-identical to `f32::from_str`, with no double-rounding hazard (we never
/// route through `f64`). Anything else — long mantissa, big exponent,
/// hex/inf/nan, stray characters — returns `None` for the std fallback. A
/// successful result is always finite (mantissa and exponent are both bounded),
/// so the caller need not re-check `is_finite`.
#[inline]
fn parse_f32_fast_at(b: &[u8], i: &mut usize) -> Option<f32> {
    let n = b.len();
    let mut j = *i;
    let neg = match b.get(j) {
        Some(b'-') => {
            j += 1;
            true
        }
        Some(b'+') => {
            j += 1;
            false
        }
        _ => false,
    };
    let mut mantissa: u64 = 0;
    let mut digits: u32 = 0;
    let mut frac_len: i32 = 0;
    let mut seen_dot = false;
    let mut any_digit = false;
    while j < n {
        match b[j] {
            d @ b'0'..=b'9' => {
                if digits >= 17 {
                    return None; // too many digits for the exact fast path
                }
                mantissa = mantissa * 10 + (d - b'0') as u64;
                digits += 1;
                if seen_dot {
                    frac_len += 1;
                }
                any_digit = true;
                j += 1;
            }
            b'.' if !seen_dot => {
                seen_dot = true;
                j += 1;
            }
            b'e' | b'E' => break,
            b',' => break,
            c if c.is_ascii_whitespace() => break,
            _ => return None, // unexpected byte — let std define the semantics
        }
    }
    if !any_digit {
        return None;
    }
    let mut exp10: i32 = -frac_len;
    if j < n && (b[j] == b'e' || b[j] == b'E') {
        // Exponent suffix `[eE][+-]?digits`.
        j += 1;
        let esign: i32 = match b.get(j) {
            Some(b'-') => {
                j += 1;
                -1
            }
            Some(b'+') => {
                j += 1;
                1
            }
            _ => 1,
        };
        let estart = j;
        let mut e: i32 = 0;
        while j < n {
            match b[j] {
                d @ b'0'..=b'9' => {
                    e = e.saturating_mul(10).saturating_add((d - b'0') as i32);
                    j += 1;
                }
                b',' => break,
                c if c.is_ascii_whitespace() => break,
                _ => return None,
            }
        }
        if j == estart {
            return None; // 'e' with no exponent digits
        }
        exp10 = exp10.saturating_add(esign * e);
    }
    if mantissa > 0x00FF_FFFF || !(-10..=10).contains(&exp10) {
        return None; // outside the exact-representation window
    }
    let m = mantissa as f32;
    let r = if exp10 >= 0 {
        m * POW10_F32[exp10 as usize]
    } else {
        m / POW10_F32[(-exp10) as usize]
    };
    *i = j;
    Some(if neg { -r } else { r })
}

/// Whole-string fast parse: like [`parse_f32_fast_at`] but requires the entire
/// (already trimmed) slice to be consumed. Standalone variant used by the
/// bit-exactness differential test.
#[cfg(test)]
#[inline]
fn parse_f32_fast(s: &str) -> Option<f32> {
    let b = s.as_bytes();
    let mut i = 0usize;
    let v = parse_f32_fast_at(b, &mut i)?;
    if i == b.len() {
        Some(v)
    } else {
        None
    }
}

/// Parse the JSON-array text form `"[a, b, c]"` into internal bytes. Accepts
/// integers, decimals, scientific notation, signs, and surrounding / interior
/// whitespace. `"[]"` is rejected (a vector needs at least one dimension).
pub(crate) fn parse_text(s: &str) -> Result<VectorValue, QueryError> {
    let trimmed = s.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|r| r.strip_suffix(']'))
        .ok_or_else(|| {
            vector_err(format!(
                "invalid VECTOR text '{s}': expected a '[..]' array literal"
            ))
        })?;
    if inner.trim().is_empty() {
        return Err(vector_err(
            "invalid VECTOR text '[]': dimension must be >= 1",
        ));
    }
    // True single pass over the element bytes: a correctly-rounded Clinger fast
    // parse writes each `f32` straight to the little-endian output buffer — no
    // `split`/`trim` per element, no `Vec<f32>` intermediate, no second
    // `from_f32` re-encode. Float parsing + per-element machinery is the
    // dominant cost of text vector ingest (profiled ≈ the entire storage path
    // at dim=768), so eliminating the overhead is the lever for bulk-load
    // throughput. The element count equals comma_count + 1; pre-reserve so a
    // high-dim literal never reallocates.
    let b = inner.as_bytes();
    let n = b.len();
    let elems = b.iter().filter(|&&c| c == b',').count() + 1;
    if elems > MAX_DIM {
        return Err(vector_err(format!(
            "VECTOR dimension exceeds the maximum of {MAX_DIM}"
        )));
    }
    let mut out: Vec<u8> = Vec::with_capacity(elems * 4);
    let mut i = 0usize;
    loop {
        while i < n && b[i].is_ascii_whitespace() {
            i += 1;
        }
        let start = i;
        match parse_f32_fast_at(b, &mut i) {
            Some(v) => {
                // Fast path: always finite, no `is_finite` re-check needed.
                out.extend_from_slice(&v.to_le_bytes());
                while i < n && b[i].is_ascii_whitespace() {
                    i += 1;
                }
                if i >= n {
                    break;
                }
                if b[i] != b',' {
                    // e.g. "1 2" (missing comma) — surface the offending token.
                    let mut j = i;
                    while j < n && b[j] != b',' {
                        j += 1;
                    }
                    let tok = inner[start..j].trim();
                    return Err(vector_err(format!("invalid VECTOR element '{tok}'")));
                }
                i += 1;
            }
            None => {
                // Slow tail: delimit the element and let std define the result
                // (and the exact error for malformed / empty tokens).
                let mut j = start;
                while j < n && b[j] != b',' {
                    j += 1;
                }
                let tok = inner[start..j].trim();
                let v: f32 = tok
                    .parse()
                    .map_err(|_| vector_err(format!("invalid VECTOR element '{tok}'")))?;
                if !v.is_finite() {
                    return Err(vector_err(
                        "invalid VECTOR value: elements must be finite (no NaN/Inf)",
                    ));
                }
                out.extend_from_slice(&v.to_le_bytes());
                i = j;
                if i >= n {
                    break;
                }
                i += 1;
            }
        }
    }
    Ok(out.into())
}

/// Render internal bytes as the canonical text form `"[a,b,c]"`. Returns an
/// empty string on undecodable input (lenient contract matching
/// `json::to_text` / `geo::to_text`). Elements use the shortest round-trippable
/// `f32` representation.
pub(crate) fn to_text<T: VectorInput + ?Sized>(internal: &T) -> String {
    let mut out = String::new();
    to_text_into(internal, &mut out);
    out
}

pub(crate) fn to_text_into<T: VectorInput + ?Sized>(internal: &T, out: &mut String) {
    out.clear();
    if validate_internal(internal).is_err() {
        return;
    }
    use std::fmt::Write;
    let width = match internal.datatype() {
        VectorDataType::Binary => 2,
        VectorDataType::Int8 => 5,
        VectorDataType::Float16 => 24,
        _ => 48,
    };
    out.reserve(internal.dimension() as usize * width + 2);
    out.push('[');
    let bytes = internal.payload();
    let mut integer = itoa::Buffer::new();
    for i in 0..internal.dimension() as usize {
        if i > 0 {
            out.push(',');
        }
        match internal.datatype() {
            VectorDataType::Binary => out.push(if bytes[i / 8] & (1 << (i % 8)) == 0 {
                '0'
            } else {
                '1'
            }),
            VectorDataType::Int8 => out.push_str(integer.format(bytes[i] as i8)),
            _ => {
                let value = internal.element(i) as f32;
                if value == 0.0 {
                    out.push('0');
                } else {
                    write!(out, "{value}").expect("String formatting");
                }
            }
        }
    }
    out.push(']');
}

/// Interpret an arbitrary byte string as internal vector bytes: first as the
/// internal little-endian `f32` array (the round-tripped common case), else as
/// `[..]` text. Used when a string/bytes value flows into a `VECTOR` column or
/// a vector function. `Value`-level coercion that also accepts an existing
/// `Value::Vector` lives in the storage / SQL layer.
pub(crate) fn coerce_bytes_to_internal(b: &[u8]) -> Result<VectorValue, QueryError> {
    if let Some(rest) = b.strip_prefix(VECTOR_BINLOG_MAGIC) {
        return decode_value(rest);
    }
    // Text form is unambiguous (starts with '['); try it first so a UTF-8
    // "[1,2,3]" is never misread as 3+ raw floats.
    if let Ok(text) = std::str::from_utf8(b) {
        let t = text.trim_start();
        if t.starts_with('[') {
            return parse_text(text);
        }
    }
    // Otherwise treat as raw internal bytes. Preserve valid binary values;
    // invalid UTF-8-compatible inputs also explain the accepted text form.
    normalize_internal(b).map_err(|err| {
        if std::str::from_utf8(b).is_ok() {
            vector_err(format!(
                "{}; vector text must use brackets, for example '[1,2,3]'",
                err.message()
            ))
        } else {
            err
        }
    })
}

/// Owned assignment variant of [`coerce_bytes_to_internal`]. Raw internal f32
/// bytes are validated in place and retain their allocation. Text and binlog
/// envelopes still use the canonical conversion path because their output is a
/// different byte range/representation.
#[cfg(test)]
pub(crate) fn coerce_bytes_to_internal_owned(b: Vec<u8>) -> Result<VectorValue, QueryError> {
    if b.starts_with(VECTOR_BINLOG_MAGIC) {
        return coerce_bytes_to_internal(&b);
    }
    if let Ok(text) = std::str::from_utf8(&b) {
        if text.trim_start().starts_with('[') {
            return parse_text(text);
        }
    }
    normalize_internal_owned(b.into())
}

/// Total ordering for `ORDER BY` / `DISTINCT` / `GROUP BY` on vector columns.
/// A vector space has no natural total order; MySQL/mydb sort by the internal
/// binary (deterministic and stable). Similarity ranking is expressed through
/// `DISTANCE()`, never through raw column ordering.
#[inline]
pub(crate) fn compare(a: &VectorValue, b: &VectorValue) -> Ordering {
    (a.datatype, a.dimension.to_le_bytes(), &a.payload).cmp(&(
        b.datatype,
        b.dimension.to_le_bytes(),
        &b.payload,
    ))
}

pub(crate) fn decode_value(bytes: &[u8]) -> Result<VectorValue, QueryError> {
    let value =
        VectorValue::decode(bytes).ok_or_else(|| vector_err("invalid typed VECTOR encoding"))?;
    validate_internal(&value)?;
    Ok(value)
}

pub(crate) fn parse_typed_text(
    text: &str,
    datatype: VectorDataType,
) -> Result<VectorValue, QueryError> {
    if datatype == VectorDataType::Float32 {
        return parse_text(text);
    }
    let text = text.trim();
    let inner = text
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .ok_or_else(|| vector_err("VECTOR text must be a '[..]' array"))?;
    let expected = inner.bytes().filter(|&b| b == b',').count() + 1;
    if expected > datatype.max_dimension() as usize {
        return Err(vector_err(format!(
            "{datatype} VECTOR dimension exceeds {}",
            datatype.max_dimension()
        )));
    }
    let mut payload = Vec::with_capacity(datatype.payload_len(expected as u32));
    let mut dimension = 0u32;
    for token in inner.split(',') {
        let number = token
            .trim()
            .parse::<f64>()
            .map_err(|_| vector_err(format!("invalid VECTOR element '{token}'")))?;
        if !number.is_finite() {
            return Err(vector_err("VECTOR elements must be finite"));
        }
        if dimension >= datatype.max_dimension() {
            return Err(vector_err(format!(
                "{datatype} VECTOR dimension exceeds {}",
                datatype.max_dimension()
            )));
        }
        if matches!(datatype, VectorDataType::Int8 | VectorDataType::Binary) {
            exact_integer_text(token.trim()).ok_or_else(|| {
                vector_err("integer VECTOR element has a fractional value or is out of range")
            })?;
        }
        if matches!(datatype, VectorDataType::Float16 | VectorDataType::BFloat16) {
            let bits = narrow_text_bits(number, token.trim(), datatype);
            let exponent = if datatype == VectorDataType::Float16 {
                0x7c00
            } else {
                0x7f80
            };
            if bits & exponent == exponent {
                return Err(vector_err(format!("{datatype} VECTOR element overflow")));
            }
            payload.extend_from_slice(&bits.to_le_bytes());
        } else {
            append_number(&mut payload, dimension, datatype, number)?;
        }
        dimension += 1;
    }
    let value = VectorValue::new(datatype, dimension, payload);
    validate_internal(&value)?;
    Ok(value)
}

fn exact_integer_text(token: &str) -> Option<i16> {
    // The common integer spelling needs no allocation; decimal/exponent
    // spellings retain the exact mathematical-integrality checks below.
    if let Ok(value) = token.parse::<i16>() {
        return Some(value);
    }
    let (mantissa, exponent) = token.split_once(['e', 'E']).map_or((token, 0), |(m, e)| {
        (m, e.parse::<i32>().unwrap_or(i32::MIN))
    });
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa.strip_prefix(['-', '+']).unwrap_or(mantissa);
    let fractional = mantissa.split_once('.').map_or(0, |(_, f)| f.len());
    let digits: std::string::String = mantissa.chars().filter(|&c| c != '.').collect();
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some(0);
    }
    let scale = (exponent as i64) - (fractional as i64);
    let mut digits = digits.to_owned();
    if scale < 0 {
        let cut = usize::try_from(-scale).ok()?;
        if cut >= digits.len() || !digits[digits.len() - cut..].bytes().all(|b| b == b'0') {
            return None;
        }
        digits.truncate(digits.len() - cut);
    } else {
        if digits.len() as i64 + scale > 3 {
            return None;
        }
        digits.extend(std::iter::repeat_n('0', scale as usize));
    }
    let value = digits.parse::<i16>().ok()?;
    Some(if negative { -value } else { value })
}

/// Repair the dependency's truncated/double-rounded f64 paths against exact
/// adjacent representable values. Binary32 inputs retain the hardware fast path.
fn narrow_f64_bits(number: f64, datatype: VectorDataType) -> u16 {
    let single = number as f32;
    if single as f64 == number {
        return if datatype == VectorDataType::Float16 {
            half::f16::from_f32(single).to_bits()
        } else {
            half::bf16::from_f32(single).to_bits()
        };
    }
    let sign = if number.is_sign_negative() { 0x8000 } else { 0 };
    let value = number.abs();
    let infinity = if datatype == VectorDataType::Float16 {
        0x7c00
    } else {
        0x7f80
    };
    let maximum = infinity - 1;
    let decode = |bits| {
        if datatype == VectorDataType::Float16 {
            half::f16::from_bits(bits).to_f64()
        } else {
            half::bf16::from_bits(bits).to_f64()
        }
    };
    let overflow = decode(maximum) + (decode(maximum) - decode(maximum - 1)) * 0.5;
    if value >= overflow {
        return sign | infinity;
    }
    let mut bits = (if datatype == VectorDataType::Float16 {
        half::f16::from_f64(value).to_bits()
    } else {
        half::bf16::from_f64(value).to_bits()
    })
    .min(maximum);
    let converted = decode(bits);
    if value < converted && bits > 0 {
        let middle = (decode(bits - 1) + converted) * 0.5;
        if value < middle || (value == middle && bits & 1 != 0) {
            bits -= 1;
        }
    } else if value > converted && bits < maximum {
        let middle = (decode(bits + 1) + converted) * 0.5;
        if value > middle || (value == middle && bits & 1 != 0) {
            bits += 1;
        }
    }
    sign | bits
}

/// Exact decimal comparison is needed only when f64 parsing lands precisely on
/// a destination midpoint. The reference has at most 135 decimal places; the
/// input is streamed without constructing an input-sized arbitrary integer.
fn compare_decimal_magnitude(token: &str, reference: f64) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let token = token
        .trim()
        .strip_prefix(['-', '+'])
        .unwrap_or(token.trim());
    let (mantissa, exponent) = token
        .split_once(['e', 'E'])
        .map_or((token, 0i64), |(m, e)| {
            (
                m,
                e.parse::<i64>()
                    .expect("finite nonzero half midpoint exponent fits i64"),
            )
        });
    let fraction = mantissa.split_once('.').map_or(0, |(_, part)| part.len());
    let mut digits = mantissa
        .bytes()
        .filter(|&b| b != b'.')
        .skip_while(|&b| b == b'0');
    let length = digits.clone().count();
    if length == 0 {
        return Ordering::Less;
    }
    let bits = reference.to_bits();
    let mut significant = (bits & 0x000f_ffff_ffff_ffff) | (1u64 << 52);
    let mut exponent2 = ((bits >> 52) & 0x7ff) as i32 - 1023 - 52;
    let zeros = significant.trailing_zeros();
    significant >>= zeros;
    exponent2 += zeros as i32;
    let (coefficient, scale) = if exponent2 >= 0 {
        (
            num_bigint::BigUint::from(significant) << exponent2 as usize,
            0usize,
        )
    } else {
        (
            num_bigint::BigUint::from(significant)
                * num_bigint::BigUint::from(5u8).pow((-exponent2) as u32),
            (-exponent2) as usize,
        )
    };
    let exact = coefficient.to_str_radix(10);
    let order = (length as i128 + exponent as i128 - fraction as i128)
        .cmp(&(exact.len() as i128 - scale as i128));
    if order != Ordering::Equal {
        return order;
    }
    for i in 0..length.max(exact.len()) {
        let order = digits
            .next()
            .unwrap_or(b'0')
            .cmp(&exact.as_bytes().get(i).copied().unwrap_or(b'0'));
        if order != Ordering::Equal {
            return order;
        }
    }
    Ordering::Equal
}

fn narrow_text_bits(number: f64, token: &str, datatype: VectorDataType) -> u16 {
    use std::cmp::Ordering;
    let bits = narrow_f64_bits(number, datatype);
    let sign = bits & 0x8000;
    let mut magnitude = bits & 0x7fff;
    let infinity = if datatype == VectorDataType::Float16 {
        0x7c00
    } else {
        0x7f80
    };
    let maximum = infinity - 1;
    let decode = |bits| {
        if datatype == VectorDataType::Float16 {
            half::f16::from_bits(bits).to_f64()
        } else {
            half::bf16::from_bits(bits).to_f64()
        }
    };
    let value = number.abs();
    let overflow = decode(maximum) + (decode(maximum) - decode(maximum - 1)) * 0.5;
    if magnitude == infinity {
        if value == overflow && compare_decimal_magnitude(token, value) == Ordering::Less {
            magnitude = maximum;
        }
    } else {
        let converted = decode(magnitude);
        if value > converted {
            let middle = if magnitude == maximum {
                overflow
            } else {
                (converted + decode(magnitude + 1)) * 0.5
            };
            if value == middle && compare_decimal_magnitude(token, middle) == Ordering::Greater {
                magnitude += 1;
            }
        } else if value < converted && magnitude > 0 {
            let middle = (converted + decode(magnitude - 1)) * 0.5;
            if value == middle && compare_decimal_magnitude(token, middle) == Ordering::Less {
                magnitude -= 1;
            }
        }
    }
    sign | magnitude
}

fn append_number(
    payload: &mut Vec<u8>,
    i: u32,
    datatype: VectorDataType,
    number: f64,
) -> Result<(), QueryError> {
    match datatype {
        VectorDataType::Float32 => {
            let number = number as f32;
            if !number.is_finite() {
                return Err(vector_err("FLOAT32 VECTOR element overflow"));
            }
            payload.extend_from_slice(&number.to_le_bytes());
        }
        VectorDataType::Float16 => {
            let number = half::f16::from_bits(narrow_f64_bits(number, datatype));
            if !number.is_finite() {
                return Err(vector_err("FLOAT16 VECTOR element overflow"));
            }
            payload.extend_from_slice(&number.to_le_bytes());
        }
        VectorDataType::BFloat16 => {
            let number = half::bf16::from_bits(narrow_f64_bits(number, datatype));
            if !number.is_finite() {
                return Err(vector_err("BFLOAT16 VECTOR element overflow"));
            }
            payload.extend_from_slice(&number.to_le_bytes());
        }
        VectorDataType::Int8 => {
            if number.fract() != 0.0 || !(-128.0..=127.0).contains(&number) {
                return Err(vector_err(
                    "INT8 VECTOR elements must be integers in [-128,127]",
                ));
            }
            payload.push(number as i8 as u8);
        }
        VectorDataType::Binary => {
            if number != 0.0 && number != 1.0 {
                return Err(vector_err("BINARY VECTOR elements must be 0 or 1"));
            }
            if i % 8 == 0 {
                payload.push(0);
            }
            if number == 1.0 {
                *payload.last_mut().unwrap() |= 1 << (i % 8);
            }
        }
    }
    Ok(())
}

pub(crate) fn convert_value(
    value: &VectorValue,
    datatype: VectorDataType,
    dimension: Option<u32>,
) -> Result<VectorValue, QueryError> {
    validate_internal(value)?;
    if dimension.is_some_and(|d| d != value.dimension) {
        return Err(super::VectorError::DimensionMismatch {
            actual: value.dimension as usize,
            declared: dimension.unwrap(),
        }
        .into_query_error());
    }
    if value.datatype == datatype {
        return Ok(value.clone());
    }
    if value.datatype == VectorDataType::Binary || datatype == VectorDataType::Binary {
        return Err(vector_err(
            "cannot convert between numeric and BINARY vectors",
        ));
    }
    if value.dimension > datatype.max_dimension() {
        return Err(vector_err(format!(
            "VECTOR dimension exceeds {datatype} limit"
        )));
    }
    let mut payload = Vec::with_capacity(datatype.payload_len(value.dimension));
    for i in 0..value.dimension {
        append_number(&mut payload, i, datatype, value.element(i as usize))?;
    }
    Ok(VectorValue::new(datatype, value.dimension, payload))
}

/// Recreate a native build row from the lossless FLOAT32 construction buffer.
pub(crate) fn from_build_f32(
    values: &[f32],
    datatype: VectorDataType,
) -> Result<VectorValue, QueryError> {
    let mut payload = Vec::with_capacity(datatype.payload_len(values.len() as u32));
    for (i, &v) in values.iter().enumerate() {
        append_number(&mut payload, i as u32, datatype, v as f64)?;
    }
    Ok(VectorValue::new(datatype, values.len() as u32, payload))
}

pub(crate) fn coerce_value(
    value: crate::types::Value,
    datatype: VectorDataType,
    dimension: Option<u32>,
) -> Result<VectorValue, QueryError> {
    use crate::types::Value;
    let value = match value {
        Value::Vector(value) => {
            if value.datatype == datatype {
                validate_internal(&value)?;
                if dimension.is_some_and(|d| d != value.dimension) {
                    return Err(super::VectorError::DimensionMismatch {
                        actual: value.dimension as usize,
                        declared: dimension.unwrap(),
                    }
                    .into_query_error());
                }
                return Ok(value);
            }
            return convert_value(&value, datatype, dimension);
        }
        Value::String(text) | Value::Decimal(text) => parse_typed_text(&text, datatype)?,
        Value::SmallString(text) => parse_typed_text(
            std::str::from_utf8(text.as_ref()).map_err(|_| vector_err("invalid VECTOR text"))?,
            datatype,
        )?,
        Value::Bytes(bytes) => {
            if let Some(body) = bytes.strip_prefix(VECTOR_BINLOG_MAGIC) {
                return convert_value(&decode_value(body)?, datatype, dimension);
            }
            parse_typed_text(
                std::str::from_utf8(&bytes)
                    .map_err(|_| vector_err("raw VECTOR payload requires VECTOR_FROM_BINARY"))?,
                datatype,
            )?
        }
        _ => return Err(vector_err("cannot convert value to VECTOR")),
    };
    if dimension.is_some_and(|d| d != value.dimension) {
        return Err(super::VectorError::DimensionMismatch {
            actual: value.dimension as usize,
            declared: dimension.unwrap(),
        }
        .into_query_error());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_internal_accepts_finite_rejects_non_finite() {
        // [1.0, -2.5] little-endian f32 — finite, accepted.
        let ok = from_f32(&[1.0, -2.5]);
        assert!(validate_internal(&ok).is_ok());
        // NaN and ±Inf share the all-ones exponent and must be rejected by the
        // bit-pattern test (no per-element f32 materialisation).
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let mut bytes = from_f32(&[1.0]);
            bytes.extend_from_slice(&bad.to_le_bytes());
            assert!(validate_internal(&bytes).is_err(), "must reject {bad}");
        }
        // Subnormal and zero are finite — must pass.
        let sub = from_f32(&[0.0, f32::from_bits(1)]);
        assert!(validate_internal(&sub).is_ok());
        // Non-multiple-of-4 length and empty are rejected.
        assert!(validate_internal(&[0u8, 0, 0]).is_err());
        assert!(validate_internal(&[]).is_err());
    }

    #[test]
    fn coerce_bytes_binary_path_matches_text_path() {
        // The binary ingestion path: raw little-endian f32 bytes (what a
        // binary-protocol VECTOR parameter carries) must coerce to the exact
        // same internal bytes as the equivalent `STRING_TO_VECTOR` text — no
        // float re-parsing, just a validated pass-through.
        let raw = from_f32(&[1.0, 2.0, 3.0, 4.0]);
        let from_raw = coerce_bytes_to_internal(&raw).unwrap();
        assert_eq!(from_raw, raw);
        let from_text = coerce_bytes_to_internal(b"[1,2,3,4]").unwrap();
        assert_eq!(from_text, raw);

        let owned = from_f32(&[1.0, 2.0, 3.0, 4.0]);
        let ptr = owned.as_ptr();
        let from_owned = coerce_bytes_to_internal_owned(owned.payload).unwrap();
        assert_eq!(from_owned, raw);
        assert_eq!(
            from_owned.as_ptr(),
            ptr,
            "raw owned bytes must not be copied"
        );

        let owned_text = b"[1,2,3,4]".to_vec();
        assert_eq!(coerce_bytes_to_internal_owned(owned_text).unwrap(), raw);
    }

    #[test]
    fn parse_f32_fast_is_bit_identical_to_std() {
        // Every value the fast path accepts must equal `f32::from_str` to the
        // bit. We probe a broad spread of formats and a deterministic PRNG
        // sweep; any mismatch (including a wrong fast-path acceptance) fails.
        let fixed = [
            "0",
            "-0",
            "0.0",
            "-0.0",
            "1",
            "-1",
            "3.21",
            "-3.21",
            "1.23456",
            "-1.23456",
            "0.0123456",
            "100",
            "1e2",
            "1e-2",
            "1.5e3",
            "-2.5e-4",
            "9999999",
            "16777215",
            "1.6777215e7",
            "0.0000000001",
            "1e10",
            "1e-10",
            "123.456",
            "+5",
            "+0.5",
            "0.1",
            "0.2",
            "0.3",
            "12345.678",
            ".5",
            "5.",
            "007",
            "1.000000",
        ];
        for s in fixed {
            if let Some(fast) = parse_f32_fast(s) {
                let std: f32 = s.parse().unwrap();
                assert_eq!(
                    fast.to_bits(),
                    std.to_bits(),
                    "fast/std mismatch for {s:?}: {fast} vs {std}"
                );
            }
        }
        // Deterministic LCG sweep: render with varying precision/exponent and
        // verify whatever the fast path accepts is bit-exact.
        let mut state: u64 = 0x9E3779B97F4A7C15;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            state
        };
        let mut checked = 0u64;
        for _ in 0..300_000 {
            let r = next();
            let sign = if r & 1 == 0 { "" } else { "-" };
            let int = (r >> 1) % 10_000_000;
            let frac = (r >> 24) % 1_000_000;
            let prec = ((r >> 40) % 7) as usize;
            for s in [
                format!("{sign}{int}"),
                format!("{sign}{int}.{frac:0prec$}"),
                format!("{sign}{int}.{frac}"),
                format!("{sign}0.{frac}"),
                format!("{sign}{int}e{}", (r % 21) as i64 - 10),
            ] {
                if let Some(fast) = parse_f32_fast(&s) {
                    let std: f32 = match s.parse() {
                        Ok(v) => v,
                        Err(_) => panic!("fast accepted but std rejected {s:?}"),
                    };
                    assert_eq!(
                        fast.to_bits(),
                        std.to_bits(),
                        "fast/std mismatch for {s:?}: {fast} vs {std}"
                    );
                    checked += 1;
                }
            }
        }
        assert!(checked > 100_000, "sweep too small: {checked}");
        // Inputs the fast path must decline (std fallback owns them).
        for s in [
            "inf",
            "-inf",
            "nan",
            "NaN",
            "0x1p4",
            "1.5f",
            "1e40",
            "1e-40",
            "12345678901234567890",
            "",
            "-",
            "e5",
            "1.2.3",
            " 1",
        ] {
            assert!(parse_f32_fast(s).is_none(), "fast path must decline {s:?}");
        }
    }

    #[test]
    fn parse_text_matches_naive_reference() {
        // Naive oracle: split on ',', trim, std-parse each element. The
        // single-pass `parse_text` must agree byte-for-byte on every accepted
        // input and error on every input the oracle errors on.
        fn naive(s: &str) -> Result<Vec<u8>, ()> {
            let t = s.trim();
            let inner = t
                .strip_prefix('[')
                .and_then(|r| r.strip_suffix(']'))
                .ok_or(())?;
            if inner.trim().is_empty() {
                return Err(());
            }
            let mut out = Vec::new();
            for part in inner.split(',') {
                let v: f32 = part.trim().parse().map_err(|_| ())?;
                if !v.is_finite() {
                    return Err(());
                }
                out.extend_from_slice(&v.to_le_bytes());
            }
            Ok(out)
        }
        let mut state: u64 = 0xD1B54A32D192ED03;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            state
        };
        for _ in 0..40_000 {
            let dim = 1 + (next() % 12) as usize;
            let mut parts = Vec::new();
            for _ in 0..dim {
                let r = next();
                let v = (r as f32 / u64::MAX as f32 - 0.5) * 2000.0;
                // Vary formatting: fixed precision, %g, exponent, ints, signs.
                let s = match r % 6 {
                    0 => format!("{v:.6}"),
                    1 => format!("{v:.6e}"),
                    2 => format!("{}", v as i64),
                    3 => format!("{v}"),
                    4 => format!("{:.2}", v / 1000.0),
                    _ => format!("{:.9}", v / 1e6),
                };
                parts.push(s);
            }
            // Randomly inject surrounding whitespace.
            let joiner = if next() % 2 == 0 { "," } else { ", " };
            let body = parts.join(joiner);
            let text = if next() % 2 == 0 {
                format!("[{body}]")
            } else {
                format!("  [ {body} ]  ")
            };
            match (parse_text(&text), naive(&text)) {
                (Ok(a), Ok(b)) => assert_eq!(a.payload, b, "byte mismatch for {text:?}"),
                (Err(_), Err(_)) => {}
                (got, exp) => panic!(
                    "accept/reject disagreement for {text:?}: parse_text={:?} naive_ok={}",
                    got.is_ok(),
                    exp.is_ok()
                ),
            }
        }
        // Explicit malformed / edge inputs — both must error.
        for bad in [
            "[]", "[ ]", "[1,,2]", "[1, ,3]", "[1 2]", "[abc]", "[1,nan]", "[inf]", "[1,2,]",
            "[,1]", "[1.2.3]",
        ] {
            assert!(parse_text(bad).is_err(), "must reject {bad:?}");
            assert!(naive(bad).is_err(), "oracle must reject {bad:?}");
        }
    }

    #[test]
    fn parse_text_single_pass_matches_expected_bytes() {
        // The single-pass parser writes elements straight to LE bytes.
        assert_eq!(parse_text("[1, 2, 3]").unwrap(), from_f32(&[1.0, 2.0, 3.0]));
        assert_eq!(parse_text("[-0.5,1e2]").unwrap(), from_f32(&[-0.5, 100.0]));
        assert!(parse_text("[]").is_err());
        assert!(parse_text("[1,nan]").is_err());
        assert!(parse_text("1,2,3").is_err());
    }
}

#[cfg(test)]
mod bench_attribution {
    use super::*;
    use std::time::Instant;

    fn make_text(dim: usize) -> String {
        let mut s = String::from("[");
        for i in 0..dim {
            if i > 0 {
                s.push(',');
            }
            // 6 significant figures, like the bench's f"{x:.6g}".
            let v = (i as f32 * 0.012_3 - 3.21).sin();
            s.push_str(&format!("{v:.6}"));
        }
        s.push(']');
        s
    }

    #[test]
    #[ignore = "manual microbench"]
    fn attribute_float_parse() {
        use std::time::Instant;
        let toks: Vec<String> = (0..768)
            .map(|i| format!("{:.6}", (i as f32 * 0.0123 - 3.21).sin()))
            .collect();
        let iters = 20_000usize;
        let mut acc = 0u32;
        let t0 = Instant::now();
        for _ in 0..iters {
            for t in &toks {
                acc ^= parse_f32_fast(t).unwrap().to_bits();
            }
        }
        let fast = t0.elapsed().as_nanos() as f64 / (iters * toks.len()) as f64;
        let t0 = Instant::now();
        for _ in 0..iters {
            for t in &toks {
                acc ^= t.parse::<f32>().unwrap().to_bits();
            }
        }
        let std = t0.elapsed().as_nanos() as f64 / (iters * toks.len()) as f64;
        println!("per-element: clinger {fast:.2}ns  std {std:.2}ns  (acc={acc})");
    }

    #[test]
    #[ignore = "manual microbench: cargo test --release vector::codec::bench -- --ignored --nocapture"]
    fn attribute_parse_cost() {
        for &dim in &[8usize, 128, 1536] {
            let text = make_text(dim);
            let internal = parse_text(&text).unwrap();
            let iters = if dim >= 1536 { 5_000 } else { 50_000 };

            // 1) Current parse_text: Vec<f32> intermediate + from_f32 re-encode.
            let t0 = Instant::now();
            let mut acc = 0u8;
            for _ in 0..iters {
                let b = parse_text(&text).unwrap();
                acc ^= b[0];
            }
            let parse_ns = t0.elapsed().as_nanos() as f64 / iters as f64;

            // 2) normalize_internal on already-canonical bytes (the redundant
            //    re-validate + clone the insert path pays after parse).
            let t0 = Instant::now();
            for _ in 0..iters {
                let b = normalize_internal(&internal).unwrap();
                acc ^= b[0];
            }
            let norm_ns = t0.elapsed().as_nanos() as f64 / iters as f64;

            // 3) finite-only validation pass (no clone) for reference.
            let t0 = Instant::now();
            for _ in 0..iters {
                for c in internal.chunks_exact(4) {
                    let v = f32::from_le_bytes([c[0], c[1], c[2], c[3]]);
                    if !v.is_finite() {
                        acc ^= 1;
                    }
                }
            }
            let finite_ns = t0.elapsed().as_nanos() as f64 / iters as f64;

            println!(
                "dim={dim:5} | parse_text {parse_ns:9.1}ns ({:.1}ns/dim) | normalize {norm_ns:8.1}ns | finite-only {finite_ns:7.1}ns | acc={acc}",
                parse_ns / dim as f64,
            );
        }
    }
}

#[cfg(test)]
mod native_datatype_tests {
    use super::*;
    #[test]
    fn native_half_all_bit_patterns_validate_and_round_trip() {
        for datatype in [VectorDataType::Float16, VectorDataType::BFloat16] {
            for bits in 0..=u16::MAX {
                let value = VectorValue::new(datatype, 1, bits.to_le_bytes().to_vec());
                let finite = match datatype {
                    VectorDataType::Float16 => half::f16::from_bits(bits).is_finite(),
                    _ => half::bf16::from_bits(bits).is_finite(),
                };
                assert_eq!(validate_internal(&value).is_ok(), finite);
                if finite {
                    assert_eq!(decode_value(&value.encode()).unwrap(), value);
                    let narrowed = match datatype {
                        VectorDataType::Float16 => half::f16::from_f64(value.element(0)).to_bits(),
                        _ => half::bf16::from_f64(value.element(0)).to_bits(),
                    };
                    assert_eq!(bits, narrowed);
                }
            }
        }
    }
    #[test]
    fn native_integer_text_is_exact_and_binary_padding_is_strict() {
        for text in [
            "[1.0000000000000000000001]",
            "[127.00000000000000001]",
            "[1e-999]",
            "[128]",
            "[-129]",
        ] {
            assert!(
                parse_typed_text(text, VectorDataType::Int8).is_err(),
                "{text}"
            );
        }
        for text in ["[1.000]", "[10e-1]", "[-128]", "[127]"] {
            assert!(
                parse_typed_text(text, VectorDataType::Int8).is_ok(),
                "{text}"
            );
        }
        for dim in [1, 7, 8, 9, 63, 64, 65, 65_536, 524_256] {
            let datatype = VectorDataType::Binary;
            let value = VectorValue::new(datatype, dim, vec![0; datatype.payload_len(dim)]);
            assert!(validate_internal(&value).is_ok());
            assert_eq!(decode_value(&value.encode()).unwrap().dimension, dim);
        }
        assert!(
            validate_internal(&VectorValue::new(VectorDataType::Binary, 7, vec![0x80])).is_err()
        );
    }
}

#[cfg(test)]
mod native_rounding_tests {
    use super::*;
    #[test]
    fn native_half_midpoints_and_adjacent_f64_values_round_to_even() {
        for datatype in [VectorDataType::Float16, VectorDataType::BFloat16] {
            let infinity = if datatype == VectorDataType::Float16 {
                0x7c00
            } else {
                0x7f80
            };
            let decode = |bits| {
                if datatype == VectorDataType::Float16 {
                    half::f16::from_bits(bits).to_f64()
                } else {
                    half::bf16::from_bits(bits).to_f64()
                }
            };
            for lower in 0..infinity - 1 {
                let upper = lower + 1;
                let middle = (decode(lower) + decode(upper)) * 0.5;
                for (number, expected) in [
                    (f64::from_bits(middle.to_bits() - 1), lower),
                    (middle, if lower & 1 == 0 { lower } else { upper }),
                    (f64::from_bits(middle.to_bits() + 1), upper),
                ] {
                    assert_eq!(
                        narrow_f64_bits(number, datatype),
                        expected,
                        "{datatype}/{lower:04x}/{number}"
                    );
                    assert_eq!(narrow_f64_bits(-number, datatype), expected | 0x8000);
                }
            }
        }
    }
    #[test]
    fn native_text_rounding_preserves_decimal_midpoint_sides() {
        for (datatype, text, expected) in [
            (VectorDataType::Float16, "1.00048828125", 0x3c00),
            (
                VectorDataType::Float16,
                "1.00048828125000000000000000001",
                0x3c01,
            ),
            (
                VectorDataType::Float16,
                "1.00146484374999999999999999999",
                0x3c01,
            ),
            (VectorDataType::Float16, "1.00146484375", 0x3c02),
            (
                VectorDataType::Float16,
                "65519.9999999999999999999999999",
                0x7bff,
            ),
            (VectorDataType::BFloat16, "1.00390625", 0x3f80),
            (
                VectorDataType::BFloat16,
                "1.00390625000000000000000000001",
                0x3f81,
            ),
            (
                VectorDataType::BFloat16,
                "1.01171874999999999999999999999",
                0x3f81,
            ),
            (VectorDataType::BFloat16, "1.01171875", 0x3f82),
        ] {
            let value = parse_typed_text(&format!("[{text}]"), datatype).unwrap();
            assert_eq!(
                u16::from_le_bytes(value.payload[..2].try_into().unwrap()),
                expected,
                "{datatype}/{text}"
            );
        }
    }
}

#[cfg(test)]
mod quality_tests {
    use super::*;

    #[test]
    fn vector_text_extreme_exponents_match_standard_parser() {
        for token in [
            "1.0e-2147483647",
            "-1.25e-2147483648",
            "1.0e-99999999999999999999999999",
            "0.0e99999999999999999999999999",
            "1.0e99999999999999999999999999",
        ] {
            let expected = token.parse::<f32>().unwrap();
            let actual = parse_text(&format!("[{token}]"));
            if expected.is_finite() {
                assert_eq!(actual.unwrap().payload, expected.to_le_bytes(), "{token}");
            } else {
                assert!(actual.is_err(), "{token}");
            }
        }
    }
}
