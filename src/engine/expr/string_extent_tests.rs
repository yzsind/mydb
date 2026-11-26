use super::*;
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive};

#[test]
fn fixed_decimal_string_extents_match_wide_reference_and_diagnostics() {
    let mut magnitudes = vec![
        i128::MIN,
        i128::MIN + 1,
        i128::MAX,
        i128::from(i64::MIN) - 1,
        i128::from(i64::MIN),
        i128::from(i64::MAX),
        i128::from(i64::MAX) + 1,
        -150,
        -149,
        -15,
        -5,
        -1,
        0,
        1,
        5,
        15,
        149,
        150,
    ];
    for scale in 1..=18 {
        let factor = 10i128.pow(scale);
        for boundary in [i128::from(i64::MIN), i128::from(i64::MAX)] {
            let center = boundary * factor;
            for delta in [-factor, -factor / 2, -1, 0, 1, factor / 2, factor] {
                if let Some(value) = center.checked_add(delta) {
                    magnitudes.push(value);
                }
            }
        }
    }
    for magnitude in magnitudes {
        for scale in [0, 1, 2, 4, 18, 19, 30, 38, 39, 81, 255] {
            let wide = crate::util::decimal::rescale_exact(
                crate::util::decimal::ExactDecimal::Big(BigInt::from(magnitude), scale),
                0,
                true,
            )
            .into_big_parts()
            .0;
            let overflow = wide < BigInt::from(i64::MIN) || wide > BigInt::from(i64::MAX);
            let expected = wide.to_i64().unwrap_or_else(|| {
                if wide.is_negative() {
                    i64::MIN
                } else {
                    i64::MAX
                }
            });
            let mut values = vec![Value::Decimal128(magnitude, scale)];
            if let Ok(small) = i64::try_from(magnitude) {
                values.push(Value::DecimalCompact(small, scale));
            }
            for value in values {
                let sink = crate::engine::api::StatementWarningSink::default();
                let result = crate::engine::api::with_statement_warning_sink(sink.clone(), || {
                    string_extent_arg(&value)
                });
                assert_eq!(result, expected, "{value:?}");
                let warnings = sink.lock();
                assert_eq!(warnings.len(), usize::from(overflow), "{value:?}");
                if overflow {
                    let warning = &warnings[0];
                    assert_eq!(
                        warning.code(),
                        crate::error::catalog::ER_TRUNCATED_WRONG_VALUE.code().get()
                    );
                    assert_eq!(
                        warning.message(),
                        format!(
                            "Truncated incorrect DECIMAL value: '{}'",
                            value.to_display_string()
                        )
                    );
                }
            }
        }
    }
}

#[test]
fn substring_ascii_and_unicode_match_character_reference() {
    for text in [
        "",
        "abc",
        "0123456789abcdefghijklmnopqrstuvwxyz",
        "ab\0cd",
        "中🙂e\u{301}ß界",
        "a中b🙂c",
    ] {
        let chars = text.chars().collect::<Vec<_>>();
        for start in [i64::MIN, -100, -7, -3, -1, 0, 1, 2, 6, 100, i64::MAX] {
            for len in [
                None,
                Some(i64::MIN),
                Some(-1),
                Some(0),
                Some(1),
                Some(3),
                Some(i64::MAX),
            ] {
                let position = if start > 0 {
                    i128::from(start) - 1
                } else {
                    chars.len() as i128 + i128::from(start)
                };
                let expected: String = if start == 0 || position < 0 || len.is_some_and(|n| n <= 0)
                {
                    String::new()
                } else {
                    chars
                        .iter()
                        .skip(position as usize)
                        .take(len.unwrap_or(i64::MAX) as usize)
                        .collect()
                };
                assert_eq!(
                    mysql_substring_slice(text, start, len),
                    expected,
                    "{text:?}, {start}, {len:?}"
                );
                if text.is_ascii() {
                    assert_eq!(
                        mysql_substring_bytes(text.as_bytes(), start, len),
                        expected.as_bytes()
                    );
                }
            }
        }
    }
}
