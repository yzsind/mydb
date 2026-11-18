use super::*;

fn oracle(value: &str, pattern: &str, escape: Option<char>, info: &CollationInfo) -> bool {
    crate::engine::expr::like_match_chars_with_info(
        &value.chars().collect::<Vec<_>>(),
        &pattern.chars().collect::<Vec<_>>(),
        escape,
        info,
    )
}

const COLLATIONS: &[&str] = &[
    "utf8mb4_0900_ai_ci",
    "utf8mb4_general_ci",
    "utf8mb4_unicode_ci",
    "utf8mb4_bin",
    "latin1_german2_ci",
];

#[test]
fn unicode_like_prepared_keys_match_scalar_weights_across_unicode() {
    for name in COLLATIONS {
        let info = crate::charset::resolve_collation(name).unwrap();
        for literal in [
            '数', '据', 'é', 'ß', 'ﬃ', '🙂', '\u{301}', '\0', ' ', '가', 'ﷺ', 'ﷻ',
        ] {
            let prepared = PreparedLikeChar::new(info, literal);
            for scalar in (0..0x110000).step_by(257).chain(0..128) {
                let Some(value) = char::from_u32(scalar) else {
                    continue;
                };
                assert_eq!(
                    prepared.matches(info, value),
                    like_chars_equal_for_info(info, value, literal),
                    "{name}: value={value:?}, literal={literal:?}"
                );
            }
        }
    }
}

#[test]
fn unicode_like_stream_preserves_escapes_expansions_and_ascii_run_boundaries() {
    let cases = [
        ("", "", None),
        ("", "%", None),
        ("", "_", None),
        ("", "\\", Some('\\')),
        ("a\\", "a\\", Some('\\')),
        ("abc%", "a%\\%", Some('\\')),
        ("abc_", "a%!_", Some('!')),
        ("%", "%", Some('%')),
        ("%中", "%%中", Some('%')),
        ("_中", "__中", Some('_')),
        ("数据🙂", "数据_", None),
        ("数据🙂", "%数%据_%", None),
        ("x数据y", "%数据%", None),
        ("数据", "%%数%%%据%%", None),
        ("ß", "%ss%", None),
        ("ss", "%ß%", None),
        ("ẞ", "%ß%", None),
        ("ﬃ", "%ffi%", None),
        ("CAFÉ", "%café%", None),
        ("cafe\u{301}", "%café%", None),
        ("abc\0中", "%\0中", None),
        ("a\u{301}b", "a_b", None),
        ("K", "%Ｋ%", None),
        ("﨑", "%崎%", None),
        ("x!中", "%中", Some('中')),
        ("中", "中", Some('中')),
    ];
    for &name in COLLATIONS {
        let info = crate::charset::resolve_collation(name).unwrap();
        for (value, pattern, escape) in cases {
            let plan = UnicodeLikePlan::new(pattern, escape, info);
            assert_eq!(
                plan.matches(value, info),
                oracle(value, pattern, escape, info),
                "{name}: {value:?} LIKE {pattern:?} ESCAPE {escape:?}"
            );
        }
        for length in [
            0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 1024,
        ] {
            for suffix in ["", "数据", "数x据", "🙂数据é", "﨑", "e\u{301}"] {
                let value = format!("{}{suffix}", "x".repeat(length));
                for pattern in ["%数据%", "%数_据%", "%🙂%据é", "%崎%", "%é%", "%\u{301}%"]
                {
                    let plan = UnicodeLikePlan::new(pattern, None, info);
                    assert_eq!(
                        plan.matches(&value, info),
                        oracle(&value, pattern, None, info),
                        "{name}: length={length} suffix={suffix:?} pattern={pattern:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn unicode_like_randomized_stream_matches_character_oracle_with_cold_and_hot_cache() {
    let alphabet = [
        'a', 'A', 'e', 'É', 'é', 'ß', 'ẞ', 'K', 'Ｋ', 'Ⅷ', 'ﬃ', '中', '数', '据', '🙂', '𠀀', '﨑',
        '崎', '가', 'Я', 'я', '\u{301}', '\u{200b}', '\u{a0}', '\0', ' ', '%', '_', '\\', '!',
    ];
    let mut state = 0x5296_1a34_650a_9fedu64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state as usize
    };
    for &name in COLLATIONS {
        let info = crate::charset::resolve_collation(name).unwrap();
        for _ in 0..80 {
            let escape = [
                None,
                Some('\\'),
                Some('!'),
                Some('%'),
                Some('_'),
                Some('中'),
            ][next() % 6];
            let pattern = (0..next() % 16)
                .map(|_| alphabet[next() % alphabet.len()])
                .collect::<String>();
            let plan = UnicodeLikePlan::new(&pattern, escape, info);
            for _ in 0..60 {
                let value = (0..next() % 70)
                    .map(|_| alphabet[next() % alphabet.len()])
                    .collect::<String>();
                let expected = oracle(&value, &pattern, escape, info);
                assert_eq!(
                    plan.matches(&value, info),
                    expected,
                    "{name}: {value:?} LIKE {pattern:?}"
                );
                assert_eq!(plan.matches(&value, info), expected, "hot cache: {name}");
            }
            let cloned = plan.clone();
            assert_eq!(
                cloned.matches(&pattern, info),
                oracle(&pattern, &pattern, escape, info)
            );
        }
    }
}

#[test]
fn unicode_like_pair_cache_collision_and_parallel_publication_keep_exact_keys() {
    let info = crate::charset::default_collation_info();
    let plan = UnicodeLikePlan::new("%数据%é_!%%", Some('!'), info);
    std::thread::scope(|scope| {
        for thread in 0..4 {
            let plan = &plan;
            scope.spawn(move || {
                for n in 0..2048 {
                    let Some(ch) = char::from_u32(0x400 + n * 256 + thread) else {
                        continue;
                    };
                    for literal in ['数', '据', 'é', 'ß', '🙂'] {
                        assert_eq!(
                            plan.pairs
                                .matches(info, ch, &PreparedLikeChar::new(info, literal)),
                            like_chars_equal_for_info(info, ch, literal)
                        );
                    }
                    for value in ["abc数据É🙂%", "数据e中%", "数x据é中%", "数据é🙂"]
                    {
                        assert_eq!(
                            plan.matches(value, info),
                            oracle(value, "%数据%é_!%%", Some('!'), info)
                        );
                    }
                }
            });
        }
    });
    assert!(plan.heap_capacity_bytes() >= CACHE_SLOTS * std::mem::size_of::<AtomicU64>());
}
