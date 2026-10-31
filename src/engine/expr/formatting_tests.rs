use super::*;

#[test]
fn temporal_precision_matches_original_text_for_all_value_shapes() {
    let mut values = vec![
        Value::Null,
        Value::Int(-123),
        Value::Double(1.25),
        Value::Decimal("-1.2300".into()),
        Value::String("12:30:45.123400".into()),
        Value::Bytes(b"12:00:00.12\xff".to_vec()),
        Value::Date(crate::types::ZERO_DATE),
        Value::DateTime(crate::types::ZERO_DATETIME),
    ];
    for micros in [
        0,
        1,
        10,
        100,
        120000,
        123456,
        999999,
        -1,
        -120000,
        i64::MIN,
        i64::MAX,
    ] {
        values.push(Value::Time(micros));
        values.push(Value::DateTime(micros));
    }
    for value in values {
        let text = string_value(&value);
        let expected = match text.rsplit_once('.') {
            Some((_, frac)) if !frac.is_empty() && frac.bytes().all(|b| b.is_ascii_digit()) => {
                frac.len().min(6)
            }
            _ => 0,
        };
        assert_eq!(text_fsp(&value), expected, "{value:?}");
    }
}

#[test]
fn datetime_formatter_matches_chrono_fields_and_explicit_fractional_precision() {
    for year in [
        -1, 0, 1, 100, 1582, 1900, 1969, 1970, 2000, 2038, 2040, 9999, 10000,
    ] {
        for (month, day) in [(1, 1), (2, 28), (3, 1), (12, 31)] {
            for micros in [0, 1, 10, 123400, 123456, 999999] {
                let dt = NaiveDate::from_ymd_opt(year, month, day)
                    .unwrap()
                    .and_hms_micro_opt(23, 59, 59, micros)
                    .unwrap();
                for fsp in 0..=6 {
                    let mut expected = format!(
                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                        dt.year(),
                        dt.month(),
                        dt.day(),
                        dt.hour(),
                        dt.minute(),
                        dt.second()
                    );
                    if fsp > 0 {
                        expected.push('.');
                        expected.push_str(&format!("{micros:06}")[..fsp]);
                    }
                    assert_eq!(format_datetime_fsp(dt, fsp), expected);
                }
            }
        }
    }
}

#[test]
fn datetime_formatter_retains_chrono_leap_second_fallback() {
    let dt = NaiveDate::from_ymd_opt(2016, 12, 31)
        .unwrap()
        .and_hms_nano_opt(23, 59, 59, 1_000_000_000)
        .unwrap();
    assert_eq!(format_datetime_fsp(dt, 0), "2016-12-31 23:59:59");
    assert_eq!(format_datetime_fsp(dt, 6), "2016-12-31 23:59:59.000000");
}

#[test]
fn time_component_parser_keeps_calendar_duration_and_error_domains() {
    for (text, expected) in [
        ("12:34:56.1234567", (false, 12, 34, 56, 123456)),
        ("-00:00:00.000001", (true, 0, 0, 0, 1)),
        ("+838:59:59", (false, 838, 59, 59, 0)),
        ("35 00:00:00", (false, 839, 0, 0, 0)),
        ("123456", (false, 12, 34, 56, 0)),
        ("  01:02  ", (false, 1, 2, 0, 0)),
        ("2020-02-29 23:59:58.123400", (false, 23, 59, 58, 123400)),
        ("2020-2-9", (false, 0, 0, 0, 0)),
        ("+2020-02-29", (false, 0, 0, 0, 0)),
        ("0000-00-00.123400", (false, 0, 0, 0, 123400)),
        ("0000-00-00 00:00:00", (false, 0, 0, 0, 0)),
    ] {
        assert_eq!(parse_time_components(text).unwrap(), expected, "{text}");
    }
    for text in [
        "",
        "12:34:56.数据库",
        "12:34:56.1.2",
        "2021-02-29",
        "12-34:56",
        "1e3",
        "--1",
        "12:34:56:00",
    ] {
        assert_eq!(
            parse_time_components(text).unwrap_err().code(),
            1411,
            "{text}"
        );
    }
    for text in ["12:60:00", "12:00:60"] {
        assert_eq!(
            parse_time_components(text).unwrap_err().code(),
            1292,
            "{text}"
        );
    }
}

#[test]
fn digest_hash_preserves_token_boundaries_and_trailing_dual() {
    use sha2::Digest;
    let hash = |tokens: &[u16]| {
        let bytes: Vec<_> = tokens
            .iter()
            .flat_map(|token| token.to_le_bytes())
            .collect();
        crate::util::hex::encode_lower(&sha2::Sha256::digest(bytes))
    };
    for limit in 0..=16 {
        with_statement_digest_token_limit(Some(limit), || {
            let expected = hash(&[748, 1100][..(limit / 2).min(2)]);
            for text in [
                "SELECT 1",
                "select 999 FROM dual",
                "SELECT 1 FROM `DuAl`; -- tail",
            ] {
                assert_eq!(
                    statement_digest_hash(text, "utf8mb4"),
                    expected,
                    "{text}, {limit}"
                );
            }
        });
    }
    assert_eq!(
        statement_digest_hash("SELECT NULL IS NOT NULL", "utf8mb4"),
        hash(&[748, 1100, 509, 622, 627])
    );
    assert_eq!(
        statement_digest_hash("SELECT `IS`, NULL", "utf8mb4"),
        statement_digest_hash("select `IS`, 99", "utf8mb4")
    );
    assert_ne!(
        statement_digest_hash("SELECT 1 FROM dual WHERE 1", "utf8mb4"),
        statement_digest_hash("SELECT 1", "utf8mb4")
    );
    let long = "SELECT ".to_string() + &"1,".repeat(10000) + "2 FROM DUAL";
    assert_eq!(
        statement_digest_hash(&long, "utf8mb4"),
        statement_digest_hash(&long.replace(" FROM DUAL", ""), "utf8mb4")
    );
}

#[test]
fn observed_digest_matches_independent_tokenization_and_full_validation() {
    let inputs = [
        "SELECT 1",
        "select 999 from dual",
        "SELECT a+b,c FROM t WHERE id IN(1,2,3)",
        "SELECT NULL IS NOT NULL",
        "SELECT (1+2)*3",
        "SELECT 1e5",
        "SELECT 0b1010",
        "SELECT 0xFF",
        "SELECT 0xX",
        "SELECT TRUE,FALSE,NULL",
        "SELECT _utf8'abc'",
        "SELECT 'a' 'b'",
        "SELECT `IS`,NULL",
        "SELECT Å",
        "SELECT 𩸽",
        "SELECT 1 # comment",
        "SELECT 1 /* comment */ FROM DUAL",
        "/* leading */ SELECT 1",
        "SELECT 1; ",
        "UPDATE t SET x=1 WHERE y=2",
        "DELETE FROM t WHERE x=1",
        "CREATE TABLE t(a INT)",
        "SET @v=1",
        "SELECT @v:=1",
        "SELECT 1||2",
        "SELECT 1 <=> 2",
        "SELECT 1 <-> 2",
        "SELECT 1; SELECT 2",
        "SELECT (",
        "SELECT ?",
        "SELECT 'question?'",
        "",
        "SELECT 1\0",
    ];
    for mode in [
        0,
        crate::engine::api::SQL_MODE_ANSI_QUOTES,
        crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES,
        crate::engine::api::SQL_MODE_PIPES_AS_CONCAT,
    ] {
        let _mode = crate::parser::ParserSqlModeGuard::activate(mode);
        for limit in [None, Some(2), Some(8), Some(1024)] {
            with_statement_digest_token_limit(limit, || {
                for sql in inputs {
                    let valid = !sql.trim().is_empty()
                        && !sql.contains(['\0', '?'])
                        && crate::parser::parse_statements(sql)
                            .is_ok_and(|statements| statements.len() == 1);
                    for charset in [None, Some("utf8mb4"), Some("latin1")] {
                        let actual = validated_statement_digest(sql, charset.map(str::to_owned));
                        if valid {
                            let expected = charset.map_or_else(
                                || statement_digest_text(sql),
                                |charset| statement_digest_hash(sql, charset),
                            );
                            assert_eq!(
                                actual.unwrap(),
                                expected,
                                "{sql}, {mode}, {limit:?}, {charset:?}"
                            );
                        } else {
                            assert!(
                                actual
                                    .unwrap_err()
                                    .is(crate::error::catalog::ER_PARSE_ERROR_IN_DIGEST_FN),
                                "{sql}"
                            );
                        }
                    }
                }
            });
        }
    }
}
