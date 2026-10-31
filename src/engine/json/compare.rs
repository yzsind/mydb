//! MySQL JSON value ordering, evaluated over `jsonb` bytes.
//!
//! Cross-type order (ascending), per the MySQL 8.0 manual:
//!
//! ```text
//!   NULL < numbers < STRING < OBJECT < ARRAY < BOOLEAN < (temporal / opaque …)
//! ```
//!
//! Same-type rules:
//!   * numbers   — exact decimal-value order across integers, decimals and
//!                 the shortest round-tripping decimal form of doubles;
//!   * strings   — `utf8mb4_bin`, i.e. raw byte order, case-sensitive;
//!   * booleans  — `false < true`;
//!   * arrays    — element-wise; a prefix array sorts before a longer one;
//!   * objects   — equal iff identical key/value sets; unequal objects get
//!                 a deterministic order (by member count, then by the
//!                 canonical-ordered key/value pairs).

use std::cmp::Ordering;

use super::jsonb::JsonType;
use super::jsonb::JsonbRef;

/// Total order over two whole jsonb documents.
pub(crate) fn compare(a: &[u8], b: &[u8]) -> Ordering {
    match (JsonbRef::new(a), JsonbRef::new(b)) {
        (Ok(a), Ok(b)) => compare_nodes(a, b),
        (Ok(_), Err(_)) => Ordering::Greater,
        (Err(_), Ok(_)) => Ordering::Less,
        (Err(_), Err(_)) => Ordering::Equal,
    }
}

pub(crate) fn compare_nodes(a: JsonbRef<'_>, b: JsonbRef<'_>) -> Ordering {
    let ra = a.json_type().sort_rank();
    let rb = b.json_type().sort_rank();
    if ra != rb {
        return ra.cmp(&rb);
    }
    // Same rank — apply the type-specific rule.
    if a.is_null() {
        return Ordering::Equal; // both NULL
    }
    if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
        return x.cmp(&y); // false < true
    }
    if a.is_number() && b.is_number() {
        return compare_numbers(a, b);
    }
    match a.json_type() {
        JsonType::Date => {
            if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
                if let (Ok(x), Ok(y)) = (
                    crate::storage::parse_date_to_days_bytes(x.as_bytes()),
                    crate::storage::parse_date_to_days_bytes(y.as_bytes()),
                ) {
                    return x.cmp(&y);
                }
            }
        }
        JsonType::Time => {
            if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
                if let (Ok(x), Ok(y)) = (
                    crate::storage::parse_time_to_micros(x),
                    crate::storage::parse_time_to_micros(y),
                ) {
                    return x.cmp(&y);
                }
            }
        }
        JsonType::Datetime | JsonType::Timestamp => {
            if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
                if let (Ok(x), Ok(y)) = (
                    crate::storage::parse_datetime_to_micros(x),
                    crate::storage::parse_datetime_to_micros(y),
                ) {
                    return x.cmp(&y);
                }
            }
        }
        JsonType::Opaque => return a.opaque_value().cmp(&b.opaque_value()),
        _ => {}
    }
    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
        return x.as_bytes().cmp(y.as_bytes());
    }
    if a.is_array() && b.is_array() {
        return compare_arrays(a, b);
    }
    if a.is_object() && b.is_object() {
        return compare_objects(a, b);
    }
    Ordering::Equal
}

fn compare_numbers(a: JsonbRef<'_>, b: JsonbRef<'_>) -> Ordering {
    // Prefer an exact (scaled-i128) comparison whenever both operands are
    // exact and fit i128 — integers (including u64 > i64::MAX) and decimals of
    // moderate scale. This fixes big-u64 ordering (the old `as_i64` returned
    // None for values above i64::MAX and fell back to lossy f64) and keeps
    // decimals precise.
    if let (Some((ma, sa)), Some((mb, sb))) = (a.exact_number(), b.exact_number()) {
        if let Some(ordering) = compare_scaled(ma, sa, mb, sb) {
            return ordering;
        }
    }
    // A real double, or a decimal too large for i128, on either side. MySQL
    // orders JSON numbers by their exact *decimal* value ("convert approximate
    // to exact"), NOT by collapsing both sides to f64. A pure f64 compare is
    // lossy and — fatally for `ORDER BY` — not even a total order: two
    // integers that are exactly distinct (`…806` and `…807`) both round to the
    // same double, so each compares *equal* to a `2^63` double while comparing
    // *unequal* to each other, an intransitivity that makes `sort_by` panic.
    // Comparing canonical decimal text sidesteps the f64 domain entirely: the
    // double renders to its shortest round-tripping decimal (e.g.
    // `9.223372036854776e18`), which then orders exactly like the integer
    // `9223372036854776000`, and an exact integer is never rounded.
    match (a.number_text(), b.number_text()) {
        (Some(ta), Some(tb)) => compare_decimal_text(&ta, &tb),
        _ => {
            let x = a.as_f64().unwrap_or(f64::NAN);
            let y = b.as_f64().unwrap_or(f64::NAN);
            x.partial_cmp(&y).unwrap_or(Ordering::Equal)
        }
    }
}

/// A decimal number decomposed into sign and fully-expanded (exponent-free)
/// integer / fraction digit strings, both normalised (no leading zeros in the
/// integer part beyond a single `0`, no trailing zeros in the fraction). Zero
/// is canonicalised to non-negative so `0.0` and `-0.0` compare equal.
struct DecimalParts {
    neg: bool,
    int_digits: Vec<u8>,
    frac_digits: Vec<u8>,
}

/// Parse a decimal literal (`[-+]?digits[.digits]?([eE][+-]?digits)?`) into
/// exponent-free digit strings. Anything unparseable yields a canonical zero,
/// keeping the comparator total even on malformed input.
fn parse_decimal_parts(s: &str) -> DecimalParts {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut neg = false;
    if i < bytes.len() && (bytes[i] == b'-' || bytes[i] == b'+') {
        neg = bytes[i] == b'-';
        i += 1;
    }
    let mut digits: Vec<u8> = Vec::with_capacity(bytes.len());
    // Decimal point index within `digits` (count of integer digits collected).
    let mut point: i64 = 0;
    let mut seen_point = false;
    let mut any_digit = false;
    while i < bytes.len() {
        match bytes[i] {
            b'0'..=b'9' => {
                digits.push(bytes[i]);
                if !seen_point {
                    point += 1;
                }
                any_digit = true;
            }
            b'.' if !seen_point => seen_point = true,
            b'e' | b'E' => break,
            _ => break,
        }
        i += 1;
    }
    // Exponent.
    if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
        i += 1;
        let mut exp_neg = false;
        if i < bytes.len() && (bytes[i] == b'-' || bytes[i] == b'+') {
            exp_neg = bytes[i] == b'-';
            i += 1;
        }
        let mut exp: i64 = 0;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            exp = exp
                .saturating_mul(10)
                .saturating_add((bytes[i] - b'0') as i64);
            i += 1;
        }
        point += if exp_neg { -exp } else { exp };
    }
    if !any_digit {
        return DecimalParts {
            neg: false,
            int_digits: vec![b'0'],
            frac_digits: Vec::new(),
        };
    }
    // Split `digits` at `point` into integer / fraction, padding with zeros
    // when the point falls outside the digit run.
    let (mut int_digits, mut frac_digits): (Vec<u8>, Vec<u8>) = if point <= 0 {
        let mut frac = vec![b'0'; (-point) as usize];
        frac.extend_from_slice(&digits);
        (Vec::new(), frac)
    } else if (point as usize) >= digits.len() {
        let mut int = digits.clone();
        int.extend(std::iter::repeat(b'0').take(point as usize - digits.len()));
        (int, Vec::new())
    } else {
        (
            digits[..point as usize].to_vec(),
            digits[point as usize..].to_vec(),
        )
    };
    // Normalise: strip integer leading zeros and fraction trailing zeros.
    let first_nz = int_digits
        .iter()
        .position(|&d| d != b'0')
        .unwrap_or(int_digits.len());
    int_digits.drain(..first_nz);
    while frac_digits.last() == Some(&b'0') {
        frac_digits.pop();
    }
    if int_digits.is_empty() && frac_digits.is_empty() {
        neg = false; // canonical zero
    }
    DecimalParts {
        neg,
        int_digits,
        frac_digits,
    }
}

/// Compare two decimal literals by exact value. Handles arbitrary magnitude and
/// scale (well beyond i128) and `e` notation, so it is safe for the huge
/// decimals / doubles JSON can hold. A true total order over decimal values.
fn compare_decimal_text(a: &str, b: &str) -> Ordering {
    let pa = parse_decimal_parts(a);
    let pb = parse_decimal_parts(b);
    match (pa.neg, pb.neg) {
        (false, true) => Ordering::Greater,
        (true, false) => Ordering::Less,
        (false, false) => compare_decimal_magnitude(&pa, &pb),
        (true, true) => compare_decimal_magnitude(&pb, &pa),
    }
}

fn compare_decimal_magnitude(a: &DecimalParts, b: &DecimalParts) -> Ordering {
    // Integer part: with leading zeros stripped, the longer digit run is the
    // larger number; equal lengths compare lexically.
    match a
        .int_digits
        .len()
        .cmp(&b.int_digits.len())
        .then_with(|| a.int_digits.cmp(&b.int_digits))
    {
        Ordering::Equal => {}
        ord => return ord,
    }
    // Fraction part: compare digit by digit, treating a missing digit as 0.
    let n = a.frac_digits.len().max(b.frac_digits.len());
    for k in 0..n {
        let da = a.frac_digits.get(k).copied().unwrap_or(b'0');
        let db = b.frac_digits.get(k).copied().unwrap_or(b'0');
        match da.cmp(&db) {
            Ordering::Equal => {}
            ord => return ord,
        }
    }
    Ordering::Equal
}

/// Compare two exact decimals given as `(magnitude, scale)`; lift both to the
/// larger scale in `i128`. Multiplication overflow returns `None`, allowing the
/// caller to use the arbitrary-width canonical-decimal comparator.
fn compare_scaled(ma: i128, sa: u8, mb: i128, sb: u8) -> Option<Ordering> {
    let scale = sa.max(sb);
    let lift = |m: i128, s: u8| -> Option<i128> {
        crate::util::decimal::pow10_i128(u32::from(scale - s)).and_then(|p| m.checked_mul(p))
    };
    match (lift(ma, sa), lift(mb, sb)) {
        (Some(x), Some(y)) => Some(x.cmp(&y)),
        _ => None,
    }
}

fn compare_arrays(a: JsonbRef<'_>, b: JsonbRef<'_>) -> Ordering {
    let mut ia = a.array_iter();
    let mut ib = b.array_iter();
    loop {
        match (ia.next(), ib.next()) {
            (Some(x), Some(y)) => {
                let ord = compare_nodes(x, y);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            (Some(_), None) => return Ordering::Greater, // a longer
            (None, Some(_)) => return Ordering::Less,    // b longer
            (None, None) => return Ordering::Equal,
        }
    }
}

fn compare_objects(a: JsonbRef<'_>, b: JsonbRef<'_>) -> Ordering {
    // Members are stored in canonical (len, bytes) key order in both, so a
    // pairwise walk is deterministic and yields Equal exactly when the
    // key/value sets match.
    let la = a.container_len().unwrap_or(0);
    let lb = b.container_len().unwrap_or(0);
    if la != lb {
        return la.cmp(&lb);
    }
    let mut ia = a.object_iter();
    let mut ib = b.object_iter();
    loop {
        match (ia.next(), ib.next()) {
            (Some((ka, va)), Some((kb, vb))) => {
                let kord = super::jsonb::key_order(ka.as_bytes(), kb.as_bytes());
                if kord != Ordering::Equal {
                    return kord;
                }
                let vord = compare_nodes(va, vb);
                if vord != Ordering::Equal {
                    return vord;
                }
            }
            (None, None) => return Ordering::Equal,
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
        }
    }
}

/// Hash equality must use exactly the same numeric and temporal domains as
/// compare_nodes, including scalar leaves nested inside arrays and objects.
pub(crate) fn equality_key(bytes: &[u8]) -> Vec<u8> {
    fn field(out: &mut Vec<u8>, value: &[u8]) {
        out.extend_from_slice(&(value.len() as u64).to_le_bytes());
        out.extend_from_slice(value);
    }
    fn append(node: JsonbRef<'_>, out: &mut Vec<u8>) {
        out.push(node.json_type().sort_rank());
        if node.is_number() {
            let parts = parse_decimal_parts(&node.number_text().unwrap_or_default());
            out.push(u8::from(parts.neg));
            field(out, &parts.int_digits);
            field(out, &parts.frac_digits);
        } else if node.is_array() {
            out.extend_from_slice(&(node.container_len().unwrap_or(0) as u64).to_le_bytes());
            for child in node.array_iter() {
                append(child, out);
            }
        } else if node.is_object() {
            out.extend_from_slice(&(node.container_len().unwrap_or(0) as u64).to_le_bytes());
            for (key, child) in node.object_iter() {
                field(out, key.as_bytes());
                append(child, out);
            }
        } else if let Some((ty, bytes)) = node.opaque_value() {
            out.push(ty);
            field(out, bytes);
        } else if let Some(text) = node.as_str() {
            let temporal = match node.json_type() {
                JsonType::Date => crate::storage::parse_date_to_days_bytes(text.as_bytes())
                    .ok()
                    .map(i64::from),
                JsonType::Time => crate::storage::parse_time_to_micros(text).ok(),
                JsonType::Datetime | JsonType::Timestamp => {
                    crate::storage::parse_datetime_to_micros(text).ok()
                }
                _ => None,
            };
            if let Some(value) = temporal {
                field(out, &value.to_le_bytes());
            } else {
                field(out, text.as_bytes());
            }
        } else if let Some(value) = node.as_bool() {
            out.push(u8::from(value));
        }
    }
    let mut out = Vec::with_capacity(bytes.len());
    if let Ok(node) = JsonbRef::new(bytes) {
        append(node, &mut out);
    }
    out
}
