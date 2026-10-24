//! Unit tests for the binary jsonb core: encode/decode round-trips,
//! navigation, path evaluation, and MySQL-compatible comparison/ordering.

use std::cmp::Ordering;

use super::jsonb::{JsonType, JsonbRef};
use super::path::JsonPath;
use super::*;

/// text -> jsonb -> canonical text
fn canon(text: &str) -> String {
    let b = parse_text(text, 1, "test").expect("valid json");
    to_text(&b)
}

#[test]
fn scalars_round_trip() {
    assert_eq!(canon("null"), "null");
    assert_eq!(canon("true"), "true");
    assert_eq!(canon("false"), "false");
    assert_eq!(canon("123"), "123");
    assert_eq!(canon("-7"), "-7");
    assert_eq!(canon("18446744073709551615"), "18446744073709551615"); // u64 max
    assert_eq!(canon("\"hi\""), "\"hi\"");
}

#[test]
fn malformed_fixed_width_scalar_is_rejected_without_panicking() {
    // Regression for the corpus discovered by the JSONB fuzz target: the tag
    // declares an eight-byte i64 payload, but only three payload bytes follow.
    assert!(JsonbRef::new(&[3, 10, 255, 255]).is_err());
}

#[test]
fn double_keeps_decimal_point() {
    // MySQL renders a JSON double with a decimal point: 1.0 stays 1.0.
    assert_eq!(canon("1.0"), "1.0");
    assert_eq!(canon("1.5"), "1.5");
    assert_eq!(canon("3.14"), "3.14");
    assert_eq!(canon("-0.5"), "-0.5");
}

#[test]
fn array_canonical_spacing() {
    assert_eq!(canon("[1,2,3]"), "[1, 2, 3]");
    assert_eq!(canon("[]"), "[]");
    assert_eq!(canon("[[1],[2,3]]"), "[[1], [2, 3]]");
}

#[test]
fn object_keys_sorted_by_len_then_bytes() {
    // MySQL orders object keys by (length, bytes): short keys first.
    assert_eq!(
        canon(r#"{"bb":2,"a":1,"c":3}"#),
        r#"{"a": 1, "c": 3, "bb": 2}"#
    );
    assert_eq!(canon(r#"{"name":"x","id":1}"#), r#"{"id": 1, "name": "x"}"#);
    assert_eq!(canon("{}"), "{}");
}

#[test]
fn duplicate_keys_last_wins() {
    assert_eq!(canon(r#"{"a":1,"a":2}"#), r#"{"a": 2}"#);
}

#[test]
fn string_escapes() {
    assert_eq!(canon(r#""a\"b""#), r#""a\"b""#);
    assert_eq!(canon(r#""tab\there""#), r#""tab\there""#);
    assert_eq!(canon(r#""☃ unicode""#), "\"☃ unicode\"");
}

#[test]
fn string_escape_runs_match_serde_at_every_byte_alignment() {
    let alphabet: Vec<char> = (0..=127)
        .map(char::from)
        .chain(['é', '中', '🙂', '\u{2028}', '\u{10ffff}'])
        .collect();
    for pad in 0..24 {
        for &ch in &alphabet {
            let input = format!("{}中🙂{ch}{}é{ch}", "x".repeat(pad), "y".repeat(33 - pad));
            let expected = serde_json::to_string(&input).unwrap();
            let mut actual = String::from("prefix:");
            super::jsonb::write_json_string(&input, &mut actual);
            assert_eq!(
                actual,
                format!("prefix:{expected}"),
                "pad={pad} char={ch:?}"
            );
        }
    }
    for input in [
        String::new(),
        "中🙂énormal".repeat(16384),
        alphabet.iter().collect(),
    ] {
        let mut output = String::new();
        super::jsonb::write_json_string(&input, &mut output);
        assert_eq!(output, serde_json::to_string(&input).unwrap());
    }
}

#[test]
fn nested_round_trip() {
    let src = r#"{"user":{"id":7,"tags":["a","b"]},"active":true}"#;
    let b = parse_text(src, 1, "test").unwrap();
    // to_serde then back must be byte-identical (canonical form is stable).
    let dom = to_serde(&b).unwrap();
    let b2 = from_serde(&dom);
    assert_eq!(
        b, b2,
        "jsonb must be canonical & stable under DOM round-trip"
    );
}

#[test]
fn navigation_get_and_index() {
    let b = parse_text(r#"{"a":10,"bb":[100,200,300]}"#, 1, "t").unwrap();
    let root = JsonbRef::new(&b).unwrap();
    assert_eq!(root.json_type(), JsonType::Object);
    assert_eq!(root.container_len(), Some(2));
    assert_eq!(root.get("a").unwrap().as_i64(), Some(10));
    let arr = root.get("bb").unwrap();
    assert_eq!(arr.json_type(), JsonType::Array);
    assert_eq!(arr.container_len(), Some(3));
    assert_eq!(arr.index(0).unwrap().as_i64(), Some(100));
    assert_eq!(arr.index(2).unwrap().as_i64(), Some(300));
    assert!(arr.index(3).is_none());
    assert!(root.get("missing").is_none());
}

#[test]
fn json_type_classification() {
    let cases = [
        ("null", JsonType::Null),
        ("true", JsonType::Boolean),
        ("5", JsonType::Integer),
        ("18446744073709551615", JsonType::UnsignedInteger),
        ("1.5", JsonType::Double),
        ("\"s\"", JsonType::String),
        ("[1]", JsonType::Array),
        ("{\"a\":1}", JsonType::Object),
    ];
    for (text, want) in cases {
        let b = parse_text(text, 1, "t").unwrap();
        assert_eq!(JsonbRef::new(&b).unwrap().json_type(), want, "{text}");
    }
}

#[test]
fn depth_matches_mysql() {
    let d = |t: &str| {
        let b = parse_text(t, 1, "t").unwrap();
        JsonbRef::new(&b).unwrap().depth()
    };
    assert_eq!(d("1"), 1);
    assert_eq!(d("[]"), 1);
    assert_eq!(d("{}"), 1);
    assert_eq!(d("[10, 20]"), 2);
    assert_eq!(d(r#"[10, {"a": 20}]"#), 3);
    assert_eq!(d(r#"{"a": [1, 2]}"#), 3);
}

fn extract_texts(doc: &str, path: &str) -> Vec<String> {
    let b = parse_text(doc, 1, "t").unwrap();
    let root = JsonbRef::new(&b).unwrap();
    let p = JsonPath::parse(path).unwrap();
    p.eval(root)
        .into_iter()
        .map(|n| {
            let mut s = String::new();
            n.write_text(&mut s);
            s
        })
        .collect()
}

#[test]
fn path_member_and_index() {
    assert_eq!(extract_texts(r#"{"a":{"b":42}}"#, "$.a.b"), vec!["42"]);
    assert_eq!(extract_texts(r#"{"a":[1,2,3]}"#, "$.a[1]"), vec!["2"]);
    assert_eq!(extract_texts(r#"[10,20,30]"#, "$[last]"), vec!["30"]);
    assert_eq!(extract_texts(r#"[10,20,30]"#, "$[last-1]"), vec!["20"]);
    assert_eq!(
        extract_texts(r#"{"a":1}"#, "$.missing"),
        Vec::<String>::new()
    );
}

#[test]
fn path_quoted_key() {
    assert_eq!(extract_texts(r#"{"a.b":7}"#, r#"$."a.b""#), vec!["7"]);
    assert_eq!(extract_texts(r#"{"a b":7}"#, r#"$."a b""#), vec!["7"]);
    assert!(JsonPath::parse("$.a-b").is_err());
    assert!(JsonPath::parse("$.1a").is_err());
    assert!(JsonPath::parse("$.合法").is_ok());
    assert_eq!(extract_texts(r#"{"a":1}"#, r#"$."\u0061""#), vec!["1"]);
    assert_eq!(
        extract_texts(r#"{"😀":2}"#, r#"$."\uD83D\uDE00""#),
        vec!["2"]
    );
}

#[test]
fn path_wildcards() {
    // .* — all object members
    let mut got = extract_texts(r#"{"a":1,"b":2}"#, "$.*");
    got.sort();
    assert_eq!(got, vec!["1", "2"]);
    // [*] — all array elements
    assert_eq!(extract_texts(r#"[1,2,3]"#, "$[*]"), vec!["1", "2", "3"]);
    // ** recursive descent
    let mut got = extract_texts(r#"{"a":{"x":1},"b":{"x":2}}"#, "$**.x");
    got.sort();
    assert_eq!(got, vec!["1", "2"]);
}

#[test]
fn path_array_ranges_match_mysql_80() {
    assert_eq!(
        extract_texts(r#"[0,1,2,3,4]"#, "$[1 to 3]"),
        vec!["1", "2", "3"]
    );
    assert_eq!(
        extract_texts(r#"[0,1,2,3,4]"#, "$[last - 3 to last - 1]"),
        vec!["1", "2", "3"]
    );
    assert_eq!(
        extract_texts(r#"[0,1,2,3,4]"#, "$[2 to 99]"),
        vec!["2", "3", "4"]
    );
    assert_eq!(
        extract_texts(r#"[0,1,2,3,4]"#, "$[last - 99 to last - 2]"),
        vec!["0", "1", "2"]
    );
    assert_eq!(extract_texts("7", "$[0 to 2]"), vec!["7"]);
    assert!(JsonPath::parse("$[2 to 1]").is_err());
    assert!(JsonPath::parse("$[last - 1 to last - 3]").is_err());
}

#[test]
fn path_root() {
    assert_eq!(extract_texts(r#"{"a":1}"#, "$"), vec![r#"{"a": 1}"#]);
}

#[test]
fn path_rejects_trailing_recursive() {
    assert!(JsonPath::parse("$**").is_err());
    assert!(JsonPath::parse("no-dollar").is_err());
}

fn cmp(a: &str, b: &str) -> Ordering {
    let ba = parse_text(a, 1, "t").unwrap();
    let bb = parse_text(b, 1, "t").unwrap();
    compare(&ba, &bb)
}

#[test]
fn compare_cross_type_precedence() {
    // NULL < number < string < object < array < boolean
    assert_eq!(cmp("null", "0"), Ordering::Less);
    assert_eq!(cmp("999", "\"a\""), Ordering::Less);
    assert_eq!(cmp("\"z\"", "{}"), Ordering::Less);
    assert_eq!(cmp("{}", "[]"), Ordering::Less);
    assert_eq!(cmp("[]", "true"), Ordering::Less);
    assert_eq!(cmp("true", "null"), Ordering::Greater);
}

#[test]
fn compare_same_type() {
    assert_eq!(cmp("1", "2"), Ordering::Less);
    assert_eq!(cmp("2", "2"), Ordering::Equal);
    assert_eq!(cmp("1.5", "2"), Ordering::Less);
    // exact large-integer ordering
    assert_eq!(
        cmp("9223372036854775806", "9223372036854775807"),
        Ordering::Less
    );
    // strings: byte order, "a" < "ab" < "b"
    assert_eq!(cmp("\"a\"", "\"ab\""), Ordering::Less);
    assert_eq!(cmp("\"ab\"", "\"b\""), Ordering::Less);
    assert_eq!(cmp("\"A\"", "\"a\""), Ordering::Less); // case-sensitive
                                                       // booleans
    assert_eq!(cmp("false", "true"), Ordering::Less);
    // arrays: prefix sorts first
    assert_eq!(cmp("[]", "[1]"), Ordering::Less);
    assert_eq!(cmp("[1,2]", "[1,2,3]"), Ordering::Less);
    assert_eq!(cmp("[1,2]", "[1,3]"), Ordering::Less);
}

/// A JSON comparator must be a strict total order or `sort_by` panics on
/// `ORDER BY` over a JSON column. The dangerous region is JSON numbers: the old
/// code fell back to a lossy f64 compare when a double (or a huge decimal) was
/// involved, which is not transitive — two exactly-distinct integers both round
/// to the same double and so each compares *equal* to that double while
/// comparing *unequal* to each other. WL#8539's dataset hits this exactly.
#[test]
fn compare_json_numbers_and_types_total_order() {
    // Spread across every JSON type & sub-type, with the precision-collision
    // numbers from MySQL's WL#8539 ordering test woven in.
    let samples = [
        "null",
        "true",
        "false",
        "0",
        "1",
        "-1",
        "2",
        "1.5",
        "2.0",
        "3.14",
        "18446744073709551615",
        "9223372036854775807",
        "1.00",
        // Integers that round to the same f64 as the doubles below, plus the
        // doubles themselves — the exact intransitivity that crashed sort_by.
        "9223372036854775806",
        "9223372036854776000",
        "9223372036854776001",
        "9223372036854775808",
        "9223372036854775999",
        "9.223372036854776e18",
        "18446744073709551616",
        // Huge exact decimals and huge/tiny doubles (beyond i128 and f64 range
        // edges).
        "99999999999999999999999999999999999999999999999999999999999999999",
        "1e65",
        "1e100",
        "-1e81",
        "9.99e80",
        "1.5e-200",
        "-1.5e200",
        "1e-323",
        "0.0",
        "-0.0",
        "0.001",
        "-0.001",
        "\"\"",
        "\"a\"",
        "\"ab\"",
        "\"b\"",
        "\"z\"",
        "\"A\"",
        "[]",
        "[1]",
        "[1,2]",
        "[1,2,3]",
        "[2]",
        "[\"a\"]",
        "{}",
        "{\"a\":1}",
        "{\"a\":2}",
        "{\"b\":1}",
        "{\"a\":1,\"b\":2}",
        "[[1],[2,3]]",
        "{\"a\":[1,2]}",
    ];
    let mut bytes: Vec<Vec<u8>> = samples
        .iter()
        .map(|s| parse_text(s, 1, "t").unwrap())
        .collect();
    let mut samples: Vec<String> = samples.iter().map(|s| s.to_string()).collect();
    // Add jsonb temporal / decimal scalar nodes (not reachable via parse_text).
    use crate::types::Value;
    let extra = [
        Value::Decimal("1.00".to_string()),
        Value::Decimal("2.50".to_string()),
        Value::Decimal("100".to_string()),
        Value::Date(738000),
        Value::Date(738500),
        Value::Time(0),
        Value::Time(3_600_000_000),
        Value::Time(-3_600_000_000),
        Value::DateTime(0),
        Value::DateTime(1_000_000_000_000),
    ];
    for v in &extra {
        bytes.push(scalar_from_value(v));
        samples.push(format!("{v:?}"));
    }
    // Also arrays mixing temporal/decimal.
    for pair in [(
        scalar_from_value(&Value::Date(738000)),
        scalar_from_value(&Value::Decimal("1.00".into())),
    )] {
        bytes.push(super::jsonb::build_array(&[pair.0.clone(), pair.1.clone()]));
        samples.push("[date,decimal]".into());
    }
    // compare is pure: evaluate every ordered pair once, then check the
    // complete triple matrix. This retains all transitivity cases without
    // repeatedly decoding the same JSON/large decimal operands O(n^3) times.
    let comparisons: Vec<Vec<Ordering>> = bytes
        .iter()
        .map(|a| bytes.iter().map(|b| compare(a, b)).collect())
        .collect();
    // Antisymmetry
    for i in 0..bytes.len() {
        for j in 0..bytes.len() {
            let ab = comparisons[i][j];
            let ba = comparisons[j][i];
            assert_eq!(
                ab,
                ba.reverse(),
                "antisymmetry violated: cmp({},{})={:?} cmp({},{})={:?}",
                samples[i],
                samples[j],
                ab,
                samples[j],
                samples[i],
                ba
            );
        }
    }
    // Transitivity
    for i in 0..bytes.len() {
        for j in 0..bytes.len() {
            for k in 0..bytes.len() {
                let ab = comparisons[i][j];
                let bc = comparisons[j][k];
                let ac = comparisons[i][k];
                if ab == Ordering::Less && bc == Ordering::Less {
                    assert_eq!(
                        ac,
                        Ordering::Less,
                        "transitivity violated: {}<{}<{} but cmp={:?}",
                        samples[i],
                        samples[j],
                        samples[k],
                        ac
                    );
                }
                if ab == Ordering::Equal && bc == Ordering::Equal {
                    assert_eq!(
                        ac,
                        Ordering::Equal,
                        "equal-transitivity violated: {}=={}=={} but cmp={:?}",
                        samples[i],
                        samples[j],
                        samples[k],
                        ac
                    );
                }
            }
        }
    }

    // Value-level spot checks matching MySQL's WL#8539 ranks: a double orders
    // by its exact (shortest-decimal) value, so the largest signed BIGINT is
    // strictly below the double `9.223372036854776e18`, which in turn equals
    // the integer `9223372036854776000` and is below `…001`.
    assert_eq!(
        cmp("9223372036854775807", "9.223372036854776e18"),
        Ordering::Less
    );
    assert_eq!(
        cmp("9.223372036854776e18", "9223372036854776000"),
        Ordering::Equal
    );
    assert_eq!(
        cmp("9223372036854776000", "9223372036854776001"),
        Ordering::Less
    );
    assert_eq!(
        cmp("9223372036854775806", "9223372036854775807"),
        Ordering::Less
    );
    // Exact integer never collapses to the double: `…806` < the 2^63 double.
    assert_eq!(
        cmp("9223372036854775806", "9.223372036854776e18"),
        Ordering::Less
    );
    // Huge magnitudes still order by value; -0.0 == 0.0.
    assert_eq!(cmp("1e65", "1e100"), Ordering::Less);
    assert_eq!(cmp("-1e81", "9.99e80"), Ordering::Less);
    assert_eq!(cmp("-0.0", "0.0"), Ordering::Equal);
    assert_eq!(cmp("0.001", "0.01"), Ordering::Less);
}

#[test]
fn compare_objects_equal_regardless_of_input_order() {
    // {"a":1,"b":2} == {"b":2,"a":1}
    assert_eq!(cmp(r#"{"a":1,"b":2}"#, r#"{"b":2,"a":1}"#), Ordering::Equal);
    assert_eq!(cmp(r#"{"a":1}"#, r#"{"a":1,"b":2}"#), Ordering::Less); // fewer keys first
}

#[test]
fn pretty_printing() {
    let b = parse_text(r#"{"a":1,"b":[2,3]}"#, 1, "t").unwrap();
    let pretty = to_pretty(&b);
    let expected = "{\n  \"a\": 1,\n  \"b\": [\n    2,\n    3\n  ]\n}";
    assert_eq!(pretty, expected);
    // empty containers stay on one line
    let e = parse_text(r#"{"a":{},"b":[]}"#, 1, "t").unwrap();
    assert_eq!(to_pretty(&e), "{\n  \"a\": {},\n  \"b\": []\n}");
}

#[test]
fn storage_size_is_node_length() {
    let b = parse_text(r#"{"a":1}"#, 1, "t").unwrap();
    assert_eq!(JsonbRef::new(&b).unwrap().storage_size(), b.len());
}

// Original scratch-buffer encoder retained as an independent format oracle.
use super::jsonb::{
    key_order, T_ARRAY, T_F64, T_FALSE, T_I64, T_NULL, T_OBJECT, T_STRING, T_TRUE, T_U64,
};
use serde_json::Value as Json;
fn reference_write_node(value: &Json, out: &mut Vec<u8>) {
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
            out.push(T_ARRAY);
            // The payload-length varint width depends on the payload size,
            // so build the payload in a scratch buffer and splice it in —
            // simpler and cheaper than back-patching a reserved varint.
            let mut payload = Vec::new();
            write_uvarint(&mut payload, arr.len() as u64);
            for elem in arr {
                reference_write_node(elem, &mut payload);
            }
            write_uvarint(out, payload.len() as u64);
            out.extend_from_slice(&payload);
        }
        Json::Object(map) => {
            out.push(T_OBJECT);
            // serde_json's default Map is alphabetical; MySQL orders object
            // keys by (length, bytes). Re-sort so the on-disk form is the
            // MySQL canonical order. Duplicate keys cannot occur here (the
            // map already deduped, last-wins, during parsing).
            let mut entries: Vec<(&String, &Json)> = map.iter().collect();
            entries.sort_by(|(a, _), (b, _)| key_order(a.as_bytes(), b.as_bytes()));
            let mut payload = Vec::new();
            write_uvarint(&mut payload, entries.len() as u64);
            for (key, val) in entries {
                write_uvarint(&mut payload, key.len() as u64);
                payload.extend_from_slice(key.as_bytes());
                reference_write_node(val, &mut payload);
            }
            write_uvarint(out, payload.len() as u64);
            out.extend_from_slice(&payload);
        }
    }
}

fn reference_encode(value: &Json) -> Vec<u8> {
    fn estimate(v: &Json) -> usize {
        match v {
            Json::Null | Json::Bool(_) => 1,
            Json::Number(_) => 9,
            Json::String(s) => s.len() + 4,
            Json::Array(a) => 6 + a.iter().map(estimate).sum::<usize>(),
            Json::Object(o) => {
                6 + o
                    .iter()
                    .map(|(k, v)| k.len() + 2 + estimate(v))
                    .sum::<usize>()
            }
        }
    }
    let mut out = Vec::with_capacity(estimate(value));
    reference_write_node(value, &mut out);
    out
}

#[test]
fn planned_jsonb_encoding_matches_reference_at_length_boundaries() {
    for size in [0, 1, 120, 127, 128, 255, 16380, 16383, 16384, 65536] {
        let mut node = serde_json::json!({
            "long_key": [null, true, false, -1, u64::MAX, 1.25, "中文🙂\n\\"],
            "z": "x".repeat(size), "aa": {"a": [], "b": {}},
            "β": ["x".repeat(size), {"nested": [1, 2, 3]}]
        });
        for depth in 0..98 {
            if [0, 1, 2, 16, 64, 97].contains(&depth) {
                let actual = from_serde(&node);
                assert_eq!(
                    actual,
                    reference_encode(&node),
                    "size={size}, depth={depth}"
                );
                assert_eq!(to_serde(&actual).unwrap(), node);
            }
            node = if depth % 2 == 0 {
                serde_json::json!([node])
            } else {
                serde_json::json!({"n": node})
            };
        }
    }
    for count in [0, 127, 128, 16383, 16384] {
        let node = serde_json::json!([vec![Json::Null; count]]);
        assert_eq!(from_serde(&node), reference_encode(&node));
    }
    // serde last-wins deduplication and canonical key ordering stay identical.
    let node: Json = serde_json::from_str(r#"{"aa":1,"z":0,"aa":2,"a":3}"#).unwrap();
    assert_eq!(from_serde(&node), reference_encode(&node));
}

#[test]
#[ignore = "manual paired JSON encoder CPU benchmark; run release with --nocapture"]
fn jsonb_encoding_benchmark() {
    use std::hint::black_box;
    use std::time::Instant;
    for (bytes, depth, iterations) in [
        (1024, 1, 20000),
        (65536, 1, 2000),
        (65536, 64, 2000),
        (1048576, 64, 100),
    ] {
        let mut node = serde_json::json!({"padding": "a".repeat(bytes), "n": 1, "mixed": [null, true, 1.25, "中文"]});
        for _ in 1..depth {
            node = serde_json::json!({"nested": node});
        }
        assert_eq!(from_serde(&node), reference_encode(&node));
        for round in 0..5 {
            let mut elapsed = [0.0; 2];
            // Alternate order to reduce thermal/frequency/order bias.
            for which in if round % 2 == 0 { [0, 1] } else { [1, 0] } {
                let begin = Instant::now();
                for _ in 0..iterations {
                    black_box(if which == 0 {
                        reference_encode(black_box(&node))
                    } else {
                        from_serde(black_box(&node))
                    });
                }
                elapsed[which] = begin.elapsed().as_secs_f64();
            }
            eprintln!("JSON_ENCODER bytes={bytes} depth={depth} round={round} old_ms={:.3} new_ms={:.3} speedup={:.3}", elapsed[0]*1000.0, elapsed[1]*1000.0, elapsed[0]/elapsed[1]);
        }
    }
}
