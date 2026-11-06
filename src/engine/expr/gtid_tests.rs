use super::*;
use std::collections::BTreeSet;

const UUIDS: [&str; 2] = [
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
];

fn generated(seed: usize) -> (String, BTreeSet<(usize, u64)>) {
    let mut text = String::new();
    let mut expected = BTreeSet::new();
    for i in 0..16 {
        let sid = (seed + i) % 2;
        let lo = 1 + (seed * 17 + i * 23) as u64 % 63;
        let hi = (lo + (seed + i) as u64 % 8).min(64);
        if !text.is_empty() {
            text.push(',');
        }
        text.push_str(&format!(
            " {} : {lo} - {hi} ",
            UUIDS[sid].to_ascii_uppercase()
        ));
        expected.extend((lo..=hi).map(|n| (sid, n)));
    }
    (text, expected)
}

fn expanded(set: &GtidSet) -> BTreeSet<(usize, u64)> {
    let mut output = BTreeSet::new();
    for (uuid, intervals) in set {
        let sid = usize::from(uuid[0] == b'b');
        assert_eq!(uuid.as_slice(), UUIDS[sid].as_bytes());
        for &(lo, hi) in intervals {
            output.extend((lo..=hi).map(|n| (sid, n)));
        }
    }
    output
}

#[test]
fn inline_gtid_intervals_match_independent_discrete_set_arithmetic() {
    for seed in 0..128 {
        let (base, want_base) = generated(seed);
        let (minus, want_minus) = generated(seed + 41);
        let base = parse_gtid_set_uncached(&base).unwrap();
        let minus = parse_gtid_set_uncached(&minus).unwrap();
        assert_eq!(expanded(&base), want_base);
        assert_eq!(expanded(&minus), want_minus);
        assert_eq!(
            gtid_set_is_subset(&base, &minus),
            want_base.is_subset(&want_minus)
        );
        let result = gtid_set_subtract(&base, &minus);
        assert_eq!(
            expanded(&result),
            want_base.difference(&want_minus).copied().collect()
        );
        assert_eq!(
            parse_gtid_set_uncached(&format_gtid_set(&result)).unwrap(),
            result
        );
    }
}

#[test]
fn inline_gtid_intervals_keep_u64_edges_canonical_output_and_errors() {
    let uuid = UUIDS[0];
    let all = parse_gtid_set_uncached(&format!("{uuid}:1-{}", u64::MAX)).unwrap();
    let middle = parse_gtid_set_uncached(&format!("{uuid}:2-{}", u64::MAX - 1)).unwrap();
    assert_eq!(
        format_gtid_set(&gtid_set_subtract(&all, &middle)),
        format!("{uuid}:1:{}", u64::MAX)
    );
    let merged = parse_gtid_set_uncached(&format!(
        "{uuid}:{}:{},{}:1-{}",
        u64::MAX,
        u64::MAX - 1,
        uuid.to_ascii_uppercase(),
        u64::MAX - 2
    ))
    .unwrap();
    assert_eq!(merged, all);
    assert_eq!(format_gtid_set(&gtid_set_subtract(&all, &all)), "");
    for text in [format!("{uuid}"), format!("{uuid}:3-1")] {
        assert!(parse_gtid_set_uncached(&text).unwrap().is_empty());
    }
    for text in [
        "invalid".into(),
        format!("{uuid}:0"),
        format!("{uuid}:1--2"),
        format!("{uuid}:18446744073709551616"),
        format!("{uuid}:1,"),
        format!("{}:1", "数".repeat(12)),
    ] {
        let error = parse_gtid_set_uncached(&text).unwrap_err();
        assert_eq!(
            error.code(),
            crate::error::catalog::ER_MALFORMED_GTID_SET_SPECIFICATION.code()
        );
        assert!(error
            .to_string()
            .contains(&format!("Malformed GTID set specification '{text}'")));
    }
}
