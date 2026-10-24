//! Internal binary codec for the MySQL-compatible `VECTOR` data type.
//!
//! A `VECTOR(n)` value is stored — on disk, inside [`crate::types::Value::Vector`],
//! and on the wire — as a bare little-endian `f32` array: `n` consecutive
//! 4-byte IEEE-754 floats, so `dim = bytes.len() / 4`. This mirrors MySQL 9.0's
//! internal vector format and keeps reads zero-parse (the bytes are already the
//! payload). The text form `[a, b, c]` is produced only at the protocol /
//! `VECTOR_TO_STRING` boundary, exactly like JSON/GEOMETRY render lazily.
//!
//! `Vec<u8>` is not guaranteed 4-byte aligned, so callers that do arithmetic
//! decode to an owned `Vec<f32>` ([`to_f32`]) first; distance kernels then run
//! over aligned `&[f32]` where the compiler can emit SIMD (see [`super::distance`]).

use std::cmp::Ordering;

use crate::types::QueryError;

/// Portable row-binlog envelope for VECTOR values encoded as a MySQL LONG
/// BLOB. MySQL 8.0 replicas can store it losslessly in a BLOB column; mydb
/// replicas recognize the magic and recover the native little-endian f32 body.
pub(crate) const VECTOR_BINLOG_MAGIC: &[u8; 8] = b"MYDBVEC1";

/// Build a `QueryError` for an invalid VECTOR value/operation. Centralised so
/// the error code / SQLSTATE conventions stay consistent across the module.
#[inline]
pub(crate) fn vector_err(message: impl Into<String>) -> QueryError {
    super::VectorError::InvalidValue(message.into()).into_query_error()
}

/// Maximum number of dimensions a single `VECTOR` value may carry. Matches
/// MySQL 9.0's `VECTOR` cap (16383 four-byte elements ≈ 64 KiB).
pub(crate) const MAX_DIM: usize = 16383;

/// Dimension (element count) of an internal vector byte string.
#[inline]
pub(crate) fn dim(internal: &[u8]) -> usize {
    internal.len() / 4
}

/// Decode internal little-endian bytes into an owned, aligned `Vec<f32>`.
/// A trailing partial element (should never occur after [`normalize_internal`])
/// is ignored.
#[inline]
pub(crate) fn to_f32(internal: &[u8]) -> Vec<f32> {
    internal
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

/// Encode an `f32` slice into internal little-endian bytes. Used by the
/// vector-producing SQL functions (`L2_NORMALIZE` / `SUBVECTOR`) and as a
/// convenience for constructing vector fixtures in tests.
#[inline]
pub(crate) fn from_f32(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for &v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

/// Validate an internal byte string in place (zero allocation): the length
/// must be a whole number of 4-byte elements, the dimension within
/// `[1, MAX_DIM]`, and every element finite. The finite test inspects the
/// IEEE-754 exponent bits directly — NaN and ±Inf are exactly the encodings
/// with all eight exponent bits set (`0x7F80_0000`), independent of sign or
/// mantissa — so it word-steps the buffer and the compiler can vectorise it,
/// instead of materialising an `f32` per element and calling `is_finite`.
pub(crate) fn validate_internal(bytes: &[u8]) -> Result<(), QueryError> {
    if bytes.len() % 4 != 0 {
        return Err(vector_err(
            "invalid VECTOR value: byte length is not a multiple of 4",
        ));
    }
    let d = bytes.len() / 4;
    if d == 0 {
        return Err(vector_err("invalid VECTOR value: dimension must be >= 1"));
    }
    if d > MAX_DIM {
        return Err(vector_err(format!(
            "VECTOR dimension {d} exceeds the maximum of {MAX_DIM}"
        )));
    }
    for c in bytes.chunks_exact(4) {
        let bits = u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        if bits & 0x7F80_0000 == 0x7F80_0000 {
            return Err(vector_err(
                "invalid VECTOR value: elements must be finite (no NaN/Inf)",
            ));
        }
    }
    Ok(())
}

/// Validate and canonicalise an internal byte string, returning an owned copy.
/// Callers that already hold the bytes and only need to copy them into a row
/// buffer should prefer [`validate_internal`] + a direct copy to avoid this
/// allocation; this owned form is for the text/bytes coercion paths that must
/// produce a fresh `Vec` anyway.
pub(crate) fn normalize_internal(bytes: &[u8]) -> Result<Vec<u8>, QueryError> {
    validate_internal(bytes)?;
    Ok(bytes.to_vec())
}

/// Owned twin of [`normalize_internal`]. Assignment coercion already owns its
/// `Value`, so validating and returning that allocation avoids copying every
/// high-dimensional vector before the row encoder copies it into page/WAL
/// storage.
pub(crate) fn normalize_internal_owned(bytes: Vec<u8>) -> Result<Vec<u8>, QueryError> {
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
        exp10 += esign * e;
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
pub(crate) fn parse_text(s: &str) -> Result<Vec<u8>, QueryError> {
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
    Ok(out)
}

/// Render internal bytes as the canonical text form `"[a,b,c]"`. Returns an
/// empty string on undecodable input (lenient contract matching
/// `json::to_text` / `geo::to_text`). Elements use the shortest round-trippable
/// `f32` representation.
pub(crate) fn to_text(internal: &[u8]) -> String {
    let mut out = String::new();
    to_text_into(internal, &mut out);
    out
}

/// Serialise internal vector bytes to `[a,b,c]` text, reusing the caller's
/// buffer (cleared first). Lets the result-set row encoder avoid one
/// `String` allocation per VECTOR cell by threading a scratch across rows.
pub(crate) fn to_text_into(internal: &[u8], out: &mut String) {
    out.clear();
    if internal.is_empty() || internal.len() % 4 != 0 {
        return;
    }
    out.reserve(internal.len() * 4 + 2);
    out.push('[');
    for (i, c) in internal.chunks_exact(4).enumerate() {
        if i > 0 {
            out.push(',');
        }
        let v = f32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        out.push_str(&format_f32(v));
    }
    out.push(']');
}

/// Shortest decimal that round-trips to `v` (Rust's default `f32` Display is
/// already shortest-round-trip). `-0.0` is normalised to `0`.
#[inline]
fn format_f32(v: f32) -> String {
    if v == 0.0 {
        "0".to_string()
    } else {
        v.to_string()
    }
}

/// Interpret an arbitrary byte string as internal vector bytes: first as the
/// internal little-endian `f32` array (the round-tripped common case), else as
/// `[..]` text. Used when a string/bytes value flows into a `VECTOR` column or
/// a vector function. `Value`-level coercion that also accepts an existing
/// `Value::Vector` lives in the storage / SQL layer.
pub(crate) fn coerce_bytes_to_internal(b: &[u8]) -> Result<Vec<u8>, QueryError> {
    if let Some(rest) = b.strip_prefix(VECTOR_BINLOG_MAGIC) {
        if rest.len() < 4 {
            return Err(vector_err("invalid VECTOR binlog envelope"));
        }
        let declared = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
        let payload = &rest[4..];
        if payload.len() != declared.saturating_mul(4) {
            return Err(vector_err("invalid VECTOR binlog envelope dimension"));
        }
        return normalize_internal(payload);
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
pub(crate) fn coerce_bytes_to_internal_owned(b: Vec<u8>) -> Result<Vec<u8>, QueryError> {
    if b.starts_with(VECTOR_BINLOG_MAGIC) {
        return coerce_bytes_to_internal(&b);
    }
    if let Ok(text) = std::str::from_utf8(&b) {
        if text.trim_start().starts_with('[') {
            return parse_text(text);
        }
    }
    normalize_internal_owned(b)
}

/// Total ordering for `ORDER BY` / `DISTINCT` / `GROUP BY` on vector columns.
/// A vector space has no natural total order; MySQL/mydb sort by the internal
/// binary (deterministic and stable). Similarity ranking is expressed through
/// `DISTANCE()`, never through raw column ordering.
#[inline]
pub(crate) fn compare(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
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
        let from_owned = coerce_bytes_to_internal_owned(owned).unwrap();
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
                (Ok(a), Ok(b)) => assert_eq!(a, b, "byte mismatch for {text:?}"),
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
