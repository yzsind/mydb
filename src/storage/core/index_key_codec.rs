// Order-preserving (memcmp) canonical encoding for `IndexKey`.
//
// The current on-disk encoding (`encode_index_key_into_page` in
// `btree.rs`) is *deserialize-then-compare*: every lookup decodes the
// key back into `IndexKey` and uses `Ord` derived from `KeyPart`. The
// v7 layout switches to byte-level comparison so the leaf-page slot
// array can do a binary search without touching the entry payload at
// all. That requires an encoding where
//
//     a.cmp(b) == encode(a).as_slice().cmp(encode(b).as_slice())
//
// holds for every pair of keys that share the same column shape.
//
// Encoding rules (per `KeyPart`):
//   * Each part is prefixed with 1 byte:
//       0x00  → NULL marker (no payload follows)
//       0x01  → non-NULL marker (payload follows)
//     This guarantees `NULL < anything non-NULL`.
//   * Fixed-width numerics are encoded big-endian after a sign-bit flip
//     for signed integers, so lexicographic byte order matches numeric
//     order across positive *and* negative values. Float types reuse
//     the order-preserving u32/u64 representation that
//     `KeyPart::Float{32,64}` already stores (see
//     `row_codec::f64_to_order_u64`).
//   * Variable-length payloads (Bytes / String) escape `0x00` to
//     `0x00 0xff` and append a `0x00 0x01` terminator. The terminator
//     is strictly smaller than any byte the escaping can produce
//     (`0x00 0xff`), so shorter prefixes sort before longer ones with
//     the same prefix — matching `Vec<u8>::cmp` / `String::cmp`.
//   * `Decimal { value, scale }` flips the sign bit on `value` and
//     appends `scale` as a tie-breaker. Same-scale comparisons (the
//     common case for a single index column) are bit-exact; mixed-
//     scale columns fall back to a stable, deterministic order, which
//     matches the existing `#[derive(PartialOrd, Ord)]` shape.
//
// The encoding is *type-aware but not type-tagged*. Storing the column
// type once in the index meta page (instead of once per entry) is one
// of the v7 space wins: the v6 encoding pays one tag byte per part
// per entry (~10–20% of the per-entry cost on integer keys).
//
// This file is `include!()`-ed from `src/storage/core.rs`, the same
// pattern used by the other `storage/core/*.rs` submodules. It relies
// on `KeyPart` and `IndexKey` already being in scope at the include
// site (re-exported via `crate::storage::schema`).

/// Marker byte placed at the start of every encoded part.
const NULL_MARKER: u8 = 0x00;
const NON_NULL_MARKER: u8 = 0x01;

/// Terminator written after a variable-length payload. Strictly less
/// than any byte produced by the `0x00 → 0x00 0xff` escape, so
/// `"a" < "ab"` and `"" < "a"` both hold under memcmp.
const VARLEN_TERMINATOR: [u8; 2] = [0x00, 0x01];

/// Encode a single `KeyPart` into `out` using the memcmp-canonical
/// form. See module docs for the byte-level layout.
#[allow(dead_code)] // wired into IndexFile in Phase 2
pub(crate) fn encode_part_memcmp(part: &KeyPart, out: &mut Vec<u8>) {
    match part {
        KeyPart::Null => {
            out.push(NULL_MARKER);
        }
        KeyPart::Int(v) => {
            out.push(NON_NULL_MARKER);
            // Flip sign bit so negative numbers sort before positive
            // numbers under unsigned big-endian comparison.
            let u = (*v as u64) ^ 0x8000_0000_0000_0000;
            out.extend_from_slice(&u.to_be_bytes());
        }
        KeyPart::UInt(v) => {
            out.push(NON_NULL_MARKER);
            out.extend_from_slice(&v.to_be_bytes());
        }
        KeyPart::Float32(v) => {
            out.push(NON_NULL_MARKER);
            // `v` is already order-preserving u32 (see schema.rs).
            out.extend_from_slice(&v.to_be_bytes());
        }
        KeyPart::Float64(v) => {
            out.push(NON_NULL_MARKER);
            out.extend_from_slice(&v.to_be_bytes());
        }
        KeyPart::Decimal { value, scale } => {
            out.push(NON_NULL_MARKER);
            let u = (*value as u128) ^ 0x8000_0000_0000_0000_0000_0000_0000_0000u128;
            out.extend_from_slice(&u.to_be_bytes());
            out.push(*scale);
        }
        KeyPart::DecimalWide { value, scale } => {
            out.push(NON_NULL_MARKER);
            // DECIMAL(65) needs at most 28 signed bytes. Sign-extend to a
            // fixed width and flip the top bit so memcmp follows signed
            // numeric order without parsing or allocation during lookup.
            let signed = value.to_signed_bytes_be();
            assert!(signed.len() <= 28, "DECIMAL index coefficient exceeds 65 digits");
            let fill = if value.sign() == num_bigint::Sign::Minus {
                0xff
            } else {
                0x00
            };
            let start = 28 - signed.len();
            let mut fixed = [fill; 28];
            fixed[start..].copy_from_slice(&signed);
            fixed[0] ^= 0x80;
            out.extend_from_slice(&fixed);
            out.push(*scale);
        }
        KeyPart::Date(v) => {
            out.push(NON_NULL_MARKER);
            let u = (*v as u32) ^ 0x8000_0000;
            out.extend_from_slice(&u.to_be_bytes());
        }
        KeyPart::Time(v) => {
            out.push(NON_NULL_MARKER);
            let u = (*v as u64) ^ 0x8000_0000_0000_0000;
            out.extend_from_slice(&u.to_be_bytes());
        }
        KeyPart::DateTime(v) => {
            out.push(NON_NULL_MARKER);
            let u = (*v as u64) ^ 0x8000_0000_0000_0000;
            out.extend_from_slice(&u.to_be_bytes());
        }
        KeyPart::Year(v) => {
            out.push(NON_NULL_MARKER);
            out.push(*v);
        }
        KeyPart::Bytes(v) => {
            out.push(NON_NULL_MARKER);
            encode_varlen_into(v, out);
        }
        KeyPart::String(v) => {
            out.push(NON_NULL_MARKER);
            encode_varlen_into(v.as_bytes(), out);
        }
        KeyPart::CollatedString {
            collation_id,
            value,
        } => {
            out.push(NON_NULL_MARKER);
            out.extend_from_slice(&collation_id.to_be_bytes());
            // Stream the collation key bytes straight into the
            // memcmp encoder — the previous form allocated one
            // throwaway `Vec<u8>` per indexed column per row on
            // every INSERT / index backfill into a CHAR/VARCHAR
            // column with a non-binary collation.
            crate::charset::with_collation_key_by_id(*collation_id, value, |bytes| {
                encode_varlen_into(bytes, out);
            });
        }
        KeyPart::Param { .. } => {
            panic!("internal error: KeyPart::Param leaked into memcmp encoding")
        }
    }
}

#[allow(dead_code)]
fn encode_varlen_into(bytes: &[u8], out: &mut Vec<u8>) {
    out.reserve(bytes.len() + 2);
    for &b in bytes {
        out.push(b);
        if b == 0x00 {
            out.push(0xff);
        }
    }
    out.extend_from_slice(&VARLEN_TERMINATOR);
}

/// Encode a full `IndexKey` (all parts concatenated) into a fresh
/// buffer. Convenience wrapper used by tests and by the encoded-size
/// estimator.
#[allow(dead_code)] // wired into IndexFile in Phase 2
pub(crate) fn encode_key_memcmp(key: &IndexKey) -> Vec<u8> {
    let mut out = Vec::with_capacity(estimated_memcmp_len(key));
    for part in &key.0 {
        encode_part_memcmp(part, &mut out);
    }
    out
}

/// Cheap upper bound on the encoded length, used to size the
/// destination buffer. Exact for fixed-width parts; conservative for
/// variable-length parts (assumes no `0x00` to escape).
#[allow(dead_code)]
fn estimated_memcmp_len(key: &IndexKey) -> usize {
    key.0
        .iter()
        .map(|p| match p {
            KeyPart::Null => 1,
            KeyPart::Int(_)
            | KeyPart::UInt(_)
            | KeyPart::Time(_)
            | KeyPart::DateTime(_) => 1 + 8,
            KeyPart::Float32(_) => 1 + 4,
            KeyPart::Float64(_) => 1 + 8,
            KeyPart::Decimal { .. } => 1 + 16 + 1,
            KeyPart::DecimalWide { .. } => 1 + 28 + 1,
            KeyPart::Date(_) => 1 + 4,
            KeyPart::Year(_) => 1 + 1,
            KeyPart::Bytes(v) => 1 + v.len() + 2,
            KeyPart::String(v) => 1 + v.len() + 2,
            KeyPart::CollatedString { value, .. } => 1 + 2 + value.len() + 2,
            KeyPart::Param { .. } => 0,
        })
        .sum()
}

/// Decode a memcmp-canonical byte slice back to an `IndexKey`. The
/// caller must supply the column-type schema (a `Vec<ParamTarget>`)
/// because canonical encoding strips per-part type tags. In an
/// `IndexFile` deployment the schema is stored once in the index
/// meta page (the wiring of which is the Phase-5 follow-up); here it
/// is supplied directly so the codec can stand alone.
///
/// Returns an error if the byte stream is malformed or runs out
/// before every part is consumed.
#[allow(dead_code)] // wired into IndexFile in Phase 5
pub(crate) fn decode_key_memcmp(
    bytes: &[u8],
    schema: &[ParamTarget],
) -> Result<IndexKey, &'static str> {
    let mut parts = Vec::with_capacity(schema.len());
    let mut cursor = 0usize;
    for target in schema {
        let part = decode_part_memcmp(bytes, &mut cursor, target)?;
        parts.push(part);
    }
    if cursor != bytes.len() {
        return Err("trailing bytes after decoding all parts");
    }
    Ok(IndexKey::from(parts))
}

/// Decode a single `KeyPart` from `bytes` starting at `*cursor` and
/// advance `cursor`. The target column type tells us how many bytes
/// to consume.
#[allow(dead_code)]
fn decode_part_memcmp(
    bytes: &[u8],
    cursor: &mut usize,
    target: &ParamTarget,
) -> Result<KeyPart, &'static str> {
    let marker = *bytes.get(*cursor).ok_or("truncated part marker")?;
    *cursor += 1;
    if marker == NULL_MARKER {
        return Ok(KeyPart::Null);
    }
    if marker != NON_NULL_MARKER {
        return Err("invalid part marker");
    }
    fn take<'a>(bytes: &'a [u8], cursor: &mut usize, n: usize) -> Result<&'a [u8], &'static str> {
        if *cursor + n > bytes.len() {
            return Err("truncated fixed-width payload");
        }
        let out = &bytes[*cursor..*cursor + n];
        *cursor += n;
        Ok(out)
    }
    Ok(match target {
        ParamTarget::Int => {
            let s = take(bytes, cursor, 8)?;
            let u = u64::from_be_bytes(s.try_into().unwrap());
            KeyPart::Int((u ^ 0x8000_0000_0000_0000) as i64)
        }
        ParamTarget::UInt => {
            let s = take(bytes, cursor, 8)?;
            KeyPart::UInt(u64::from_be_bytes(s.try_into().unwrap()))
        }
        ParamTarget::Float32 => {
            let s = take(bytes, cursor, 4)?;
            KeyPart::Float32(u32::from_be_bytes(s.try_into().unwrap()))
        }
        ParamTarget::Float64 => {
            let s = take(bytes, cursor, 8)?;
            KeyPart::Float64(u64::from_be_bytes(s.try_into().unwrap()))
        }
        ParamTarget::Decimal {
            precision,
            scale: target_scale,
        } => {
            if *precision > 38 {
                let s = take(bytes, cursor, 28)?;
                let mut signed: [u8; 28] = s.try_into().unwrap();
                signed[0] ^= 0x80;
                let value = num_bigint::BigInt::from_signed_bytes_be(&signed);
                let scale = take(bytes, cursor, 1)?[0];
                debug_assert_eq!(scale, *target_scale);
                return Ok(KeyPart::DecimalWide { value, scale });
            }
            let s = take(bytes, cursor, 16)?;
            let u = u128::from_be_bytes(s.try_into().unwrap());
            let value = (u ^ 0x8000_0000_0000_0000_0000_0000_0000_0000u128) as i128;
            let s = take(bytes, cursor, 1)?;
            let scale = s[0];
            debug_assert_eq!(
                scale, *target_scale,
                "decoded decimal scale does not match schema"
            );
            KeyPart::Decimal { value, scale }
        }
        ParamTarget::Date => {
            let s = take(bytes, cursor, 4)?;
            let u = u32::from_be_bytes(s.try_into().unwrap());
            KeyPart::Date((u ^ 0x8000_0000) as i32)
        }
        ParamTarget::Time => {
            let s = take(bytes, cursor, 8)?;
            let u = u64::from_be_bytes(s.try_into().unwrap());
            KeyPart::Time((u ^ 0x8000_0000_0000_0000) as i64)
        }
        ParamTarget::DateTime | ParamTarget::Timestamp => {
            let s = take(bytes, cursor, 8)?;
            let u = u64::from_be_bytes(s.try_into().unwrap());
            KeyPart::DateTime((u ^ 0x8000_0000_0000_0000) as i64)
        }
        ParamTarget::Year => {
            let s = take(bytes, cursor, 1)?;
            KeyPart::Year(s[0])
        }
        ParamTarget::LikePattern { .. } => {
            return Err("search-only LIKE parameter target cannot decode persisted keys")
        }
        ParamTarget::Bytes | ParamTarget::JsonScalar => {
            let payload = decode_varlen_payload(bytes, cursor)?;
            KeyPart::Bytes(payload)
        }
        ParamTarget::CharString | ParamTarget::String => {
            let payload = decode_varlen_payload(bytes, cursor)?;
            // Canonical encoding only stores raw bytes; trust the
            // schema that this is UTF-8 (CREATE INDEX guarantees it
            // for String columns).
            let s = String::from_utf8(payload).map_err(|_| "invalid utf8 in string payload")?;
            KeyPart::String(s)
        }
        ParamTarget::CollatedCharString { collation_id }
        | ParamTarget::CollatedString { collation_id } => {
            let encoded_id = take(bytes, cursor, 2)?;
            debug_assert_eq!(
                u16::from_be_bytes(encoded_id.try_into().unwrap()),
                *collation_id,
                "decoded collation id does not match schema"
            );
            let payload = decode_varlen_payload(bytes, cursor)?;
            let s = String::from_utf8(payload).map_err(|_| "invalid utf8 in string payload")?;
            KeyPart::CollatedString {
                collation_id: *collation_id,
                value: s,
            }
        }
    })
}

#[allow(dead_code)]
fn decode_varlen_payload(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>, &'static str> {
    // Read until the terminator (0x00, 0x01). The escape rule is
    //   0x00 → 0x00 0xff
    // so any 0x00 inside the payload is followed by 0xff (not 0x01).
    let mut out = Vec::new();
    while *cursor < bytes.len() {
        let b = bytes[*cursor];
        if b == 0x00 {
            // Either escape (0x00 0xff) or terminator (0x00 0x01).
            let nxt = *bytes.get(*cursor + 1).ok_or("dangling 0x00 in varlen")?;
            if nxt == 0xff {
                out.push(0x00);
                *cursor += 2;
            } else if nxt == 0x01 {
                *cursor += 2;
                return Ok(out);
            } else {
                return Err("invalid escape sequence in varlen payload");
            }
        } else {
            out.push(b);
            *cursor += 1;
        }
    }
    Err("varlen payload missing terminator")
}

/// Compute the length the v6 on-disk encoding (`encode_index_key_into`)
/// would produce for the same key. Used by the `tests_v7` benchmarks
/// to quantify the v7 space win without actually opening an
/// `IndexFile`. Kept in sync manually with `encoded_index_key_len` in
/// `btree.rs`.
#[cfg(test)]
pub(crate) fn encoded_index_key_len_v6(key: &IndexKey) -> usize {
    let mut len = 4usize; // u32 part_count
    for part in &key.0 {
        len += 1; // tag
        len += match part {
            KeyPart::Null => 0,
            KeyPart::Int(_)
            | KeyPart::UInt(_)
            | KeyPart::Float64(_)
            | KeyPart::Time(_)
            | KeyPart::DateTime(_) => 8,
            KeyPart::Float32(_) => 4,
            KeyPart::Decimal { .. } => 16 + 1,
            KeyPart::DecimalWide { .. } => 28 + 1,
            KeyPart::Date(_) => 4,
            KeyPart::Year(_) => 1,
            KeyPart::Bytes(v) => 4 + v.len(),
            KeyPart::String(v) => 4 + v.as_bytes().len(),
            KeyPart::CollatedString { value, .. } => 2 + 4 + value.as_bytes().len(),
            KeyPart::Param { .. } => 0,
        };
    }
    len
}

#[cfg(test)]
mod index_key_codec_tests {
    use super::*;

    fn ik(parts: Vec<KeyPart>) -> IndexKey {
        IndexKey::from(parts)
    }

    /// Property: encoded byte order must match `IndexKey::cmp` for
    /// every pair of keys that share the same column shape. This is
    /// the invariant the v7 leaf binary search depends on.
    fn assert_order_preserving(pairs: &[IndexKey]) {
        for a in pairs {
            for b in pairs {
                let ord_logical = a.cmp(b);
                let ord_bytes = encode_key_memcmp(a).cmp(&encode_key_memcmp(b));
                assert_eq!(
                    ord_logical, ord_bytes,
                    "memcmp order disagrees with logical order: {:?} vs {:?}",
                    a, b
                );
            }
        }
    }

    #[test]
    fn order_preserving_i64_full_range() {
        let keys: Vec<_> = [
            i64::MIN,
            i64::MIN + 1,
            -1_000_000_000,
            -1,
            0,
            1,
            1_000_000_000,
            i64::MAX - 1,
            i64::MAX,
        ]
        .iter()
        .map(|v| ik(vec![KeyPart::Int(*v)]))
        .collect();
        assert_order_preserving(&keys);
    }

    #[test]
    fn order_preserving_u64() {
        let keys: Vec<_> = [0u64, 1, 100, 1_000_000, u64::MAX - 1, u64::MAX]
            .iter()
            .map(|v| ik(vec![KeyPart::UInt(*v)]))
            .collect();
        assert_order_preserving(&keys);
    }

    #[test]
    fn order_preserving_strings_including_prefix() {
        let keys: Vec<_> = ["", "a", "ab", "abc", "b", "ba", "z", "zz"]
            .iter()
            .map(|s| ik(vec![KeyPart::String((*s).into())]))
            .collect();
        assert_order_preserving(&keys);
    }

    #[test]
    fn order_preserving_bytes_with_embedded_null() {
        // Escaping correctness: bytes containing 0x00 must still sort
        // correctly against the column terminator.
        let keys: Vec<_> = [
            vec![],
            vec![0x00],
            vec![0x00, 0x00],
            vec![0x00, 0x01],
            vec![0x00, 0xff],
            vec![0x01],
            vec![0xff],
        ]
        .into_iter()
        .map(|v| ik(vec![KeyPart::Bytes(v)]))
        .collect();
        assert_order_preserving(&keys);
    }

    #[test]
    fn order_preserving_null_is_smallest() {
        // `KeyPart::Null` has the lowest enum discriminant, so the
        // derived `Ord` places it before every non-NULL part. The
        // memcmp encoding must agree.
        let keys = vec![
            ik(vec![KeyPart::Null]),
            ik(vec![KeyPart::Int(i64::MIN)]),
            ik(vec![KeyPart::Int(0)]),
            ik(vec![KeyPart::Int(i64::MAX)]),
        ];
        assert_order_preserving(&keys);
    }

    #[test]
    fn order_preserving_composite_two_columns() {
        let mut keys = Vec::new();
        for a in [-1i64, 0, 1, 5] {
            for b in ["", "x", "yy", "z"] {
                keys.push(ik(vec![KeyPart::Int(a), KeyPart::String(b.into())]));
            }
        }
        assert_order_preserving(&keys);
    }

    #[test]
    fn order_preserving_dates_and_years() {
        let dates: Vec<_> = [-10_000i32, -1, 0, 1, 19_000, i32::MAX]
            .iter()
            .map(|v| ik(vec![KeyPart::Date(*v)]))
            .collect();
        assert_order_preserving(&dates);

        let years: Vec<_> = (0u8..=u8::MAX)
            .step_by(17)
            .map(|y| ik(vec![KeyPart::Year(y)]))
            .collect();
        assert_order_preserving(&years);
    }

    #[test]
    fn varlen_terminator_smaller_than_any_escaped_byte() {
        // Sanity: terminator must be strictly less than 0x00 0xff
        // (the escaped form of an embedded NUL).
        assert!(VARLEN_TERMINATOR < [0x00u8, 0xff]);
    }

    #[test]
    fn v7_is_smaller_than_v6_for_i64_pk() {
        // 1M sequential i64 PKs.
        let mut v6 = 0usize;
        let mut v7 = 0usize;
        for i in 0i64..1_000_000 {
            let k = ik(vec![KeyPart::Int(i)]);
            v6 += encoded_index_key_len_v6(&k);
            v7 += encode_key_memcmp(&k).len();
        }
        // v6: 4 (count) + 1 (tag) + 8 (i64) = 13 B per entry
        // v7: 1 (marker) + 8 (BE i64) = 9 B per entry
        // → 30.7% smaller, before considering page-level prefix
        //   compression and row_id folding (those land in tests_v7).
        assert_eq!(v6, 13 * 1_000_000);
        assert_eq!(v7, 9 * 1_000_000);
        assert!(
            v7 < v6 * 7 / 10,
            "v7 i64 PK key encoding should be ≤70% of v6: {v7} vs {v6}",
        );
    }

    #[test]
    fn v7_is_smaller_than_v6_for_varchar_secondary() {
        // varchar(16) keys with a common 8-byte prefix.
        let mut v6 = 0usize;
        let mut v7 = 0usize;
        for i in 0u64..100_000 {
            let s = format!("prefix__{i:08}");
            let k = ik(vec![KeyPart::String(s)]);
            v6 += encoded_index_key_len_v6(&k);
            v7 += encode_key_memcmp(&k).len();
        }
        // v6: 4 (count) + 1 (tag) + 4 (len) + 16 (bytes) = 25 B
        // v7: 1 (marker) + 16 (bytes, no 0x00 to escape) + 2 (terminator) = 19 B
        // → 24% smaller per-entry from key encoding alone. Page-level
        //   prefix compression (next milestone) is expected to push
        //   this to ≥50%.
        assert!(v7 < v6, "varchar v7 ({v7}) should beat v6 ({v6})");
    }

    /// Phase 4a: canonical encoder + decoder must roundtrip for
    /// every supported KeyPart variant. This is the precondition for
    /// IndexFile (Phase 5) to be able to store keys in canonical
    /// form and decode them back when a query needs the original
    /// `IndexKey` (e.g. for projecting index columns in a covering
    /// scan).
    #[test]
    fn decode_roundtrip_all_keypart_variants() {
        // (schema, sample_keys). Each sample must encode + decode
        // back to the original.
        struct Case {
            schema: Vec<ParamTarget>,
            samples: Vec<IndexKey>,
        }

        let cases = vec![
            Case {
                schema: vec![ParamTarget::Int],
                samples: vec![
                    IndexKey::from(vec![KeyPart::Int(i64::MIN)]),
                    IndexKey::from(vec![KeyPart::Int(-1)]),
                    IndexKey::from(vec![KeyPart::Int(0)]),
                    IndexKey::from(vec![KeyPart::Int(i64::MAX)]),
                    IndexKey::from(vec![KeyPart::Null]),
                ],
            },
            Case {
                schema: vec![ParamTarget::UInt],
                samples: vec![
                    IndexKey::from(vec![KeyPart::UInt(0)]),
                    IndexKey::from(vec![KeyPart::UInt(u64::MAX)]),
                ],
            },
            Case {
                schema: vec![ParamTarget::Float32],
                samples: vec![IndexKey::from(vec![KeyPart::Float32(0x80000000)])], // -0
            },
            Case {
                schema: vec![ParamTarget::Float64],
                samples: vec![IndexKey::from(vec![KeyPart::Float64(0xBFF0000000000000)])],
            },
            Case {
                schema: vec![ParamTarget::Decimal {
                    precision: 18,
                    scale: 4,
                }],
                samples: vec![
                    IndexKey::from(vec![KeyPart::Decimal {
                        value: -123456,
                        scale: 4,
                    }]),
                    IndexKey::from(vec![KeyPart::Decimal {
                        value: 0,
                        scale: 4,
                    }]),
                ],
            },
            Case {
                schema: vec![ParamTarget::Decimal {
                    precision: 65,
                    scale: 0,
                }],
                samples: [
                    "-99999999999999999999999999999999999999999999999999999999999999999",
                    "-1",
                    "0",
                    "1",
                    "99999999999999999999999999999999999999999999999999999999999999999",
                ]
                .into_iter()
                .map(|text| {
                    IndexKey::from(vec![KeyPart::DecimalWide {
                        value: num_bigint::BigInt::parse_bytes(text.as_bytes(), 10).unwrap(),
                        scale: 0,
                    }])
                })
                .collect(),
            },
            Case {
                schema: vec![ParamTarget::Date],
                samples: vec![
                    IndexKey::from(vec![KeyPart::Date(-365)]),
                    IndexKey::from(vec![KeyPart::Date(0)]),
                    IndexKey::from(vec![KeyPart::Date(20_000)]),
                ],
            },
            Case {
                schema: vec![ParamTarget::Time],
                samples: vec![
                    IndexKey::from(vec![KeyPart::Time(-86_400_000_000)]),
                    IndexKey::from(vec![KeyPart::Time(0)]),
                    IndexKey::from(vec![KeyPart::Time(86_399_000_000)]),
                ],
            },
            Case {
                schema: vec![ParamTarget::DateTime],
                samples: vec![
                    IndexKey::from(vec![KeyPart::DateTime(0)]),
                    IndexKey::from(vec![KeyPart::DateTime(1_700_000_000_000_000)]),
                ],
            },
            Case {
                schema: vec![ParamTarget::Year],
                samples: vec![
                    IndexKey::from(vec![KeyPart::Year(0)]),
                    IndexKey::from(vec![KeyPart::Year(125)]),
                    IndexKey::from(vec![KeyPart::Year(255)]),
                ],
            },
            Case {
                schema: vec![ParamTarget::String],
                samples: vec![
                    IndexKey::from(vec![KeyPart::String("".into())]),
                    IndexKey::from(vec![KeyPart::String("hello mydb".into())]),
                    IndexKey::from(vec![KeyPart::String("中文字符".into())]),
                ],
            },
            Case {
                schema: vec![ParamTarget::CharString],
                samples: vec![IndexKey::from(vec![KeyPart::String("compact char".into())])],
            },
            Case {
                schema: vec![ParamTarget::Bytes],
                samples: vec![
                    IndexKey::from(vec![KeyPart::Bytes(vec![])]),
                    IndexKey::from(vec![KeyPart::Bytes(vec![0x00, 0x01, 0x00, 0xff])]),
                    IndexKey::from(vec![KeyPart::Bytes((0u8..=255).collect())]),
                ],
            },
            Case {
                schema: vec![ParamTarget::Int, ParamTarget::String],
                samples: vec![
                    IndexKey::from(vec![KeyPart::Int(42), KeyPart::String("foo".into())]),
                    IndexKey::from(vec![KeyPart::Null, KeyPart::String("bar".into())]),
                    IndexKey::from(vec![KeyPart::Int(-7), KeyPart::Null]),
                ],
            },
            Case {
                schema: vec![ParamTarget::Bytes, ParamTarget::Int, ParamTarget::String],
                samples: vec![IndexKey::from(vec![
                    KeyPart::Bytes(vec![0x00, 0xff, 0x00, 0x00, 0x01]),
                    KeyPart::Int(-1234),
                    KeyPart::String("postfix".into()),
                ])],
            },
        ];

        for case in &cases {
            assert_order_preserving(&case.samples);
            for sample in &case.samples {
                let encoded = encode_key_memcmp(sample);
                let decoded = decode_key_memcmp(&encoded, &case.schema)
                    .unwrap_or_else(|e| panic!("decode failed for {:?}: {e}", sample));
                assert_eq!(
                    &decoded, sample,
                    "roundtrip mismatch for {:?} (encoded: {:?})",
                    sample, encoded
                );
            }
        }
    }

    #[test]
    fn decode_rejects_truncated_input() {
        let key = IndexKey::from(vec![KeyPart::Int(42)]);
        let mut bytes = encode_key_memcmp(&key);
        bytes.pop(); // drop last byte
        assert!(decode_key_memcmp(&bytes, &[ParamTarget::Int]).is_err());
    }

    #[test]
    fn decode_rejects_extra_trailing_bytes() {
        let key = IndexKey::from(vec![KeyPart::Int(42)]);
        let mut bytes = encode_key_memcmp(&key);
        bytes.push(0x00); // junk
        assert!(decode_key_memcmp(&bytes, &[ParamTarget::Int]).is_err());
    }

    #[test]
    fn roundtrip_property_sample() {
        // We don't ship a decoder yet (the v7 page layout decodes
        // *suffix* bytes once the page-level common prefix is known
        // and the column type table is loaded), but every encoded
        // byte stream must be self-consistent and deterministic.
        let keys = vec![
            ik(vec![KeyPart::Int(-42)]),
            ik(vec![KeyPart::UInt(42)]),
            ik(vec![KeyPart::String("hello".into())]),
            ik(vec![KeyPart::Bytes(vec![0, 1, 0, 2, 0xff])]),
            ik(vec![
                KeyPart::Int(1),
                KeyPart::String("two".into()),
                KeyPart::Null,
            ]),
        ];
        for k in &keys {
            let a = encode_key_memcmp(k);
            let b = encode_key_memcmp(k);
            assert_eq!(a, b, "encoding must be deterministic");
            assert_eq!(a.len(), b.len());
        }
    }
}
