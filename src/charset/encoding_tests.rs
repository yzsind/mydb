use super::*;

#[test]
fn strict_lossy_and_replacement_encoding_share_supported_bytes() {
    for charset in CHARSETS {
        for text in [
            "",
            "ASCII\0text",
            "é€",
            "中文",
            "日本語",
            "אבג",
            "Ж",
            "a😀b",
            "e\u{301}",
        ] {
            let strict = encode_string(charset.name, text);
            let borrowed = encode_string_borrowed(charset, text);
            assert_eq!(
                strict.is_ok(),
                borrowed.is_some(),
                "{}: {text}",
                charset.name
            );
            assert_eq!(
                validate_text_for_charset(charset.name, text).is_ok(),
                strict.is_ok()
            );
            let lossy = encode_string_lossy_cow_info(Some(charset), text);
            match strict {
                Ok(bytes) => {
                    assert_eq!(borrowed.unwrap().as_ref(), bytes);
                    assert_eq!(lossy.as_ref(), bytes);
                    assert_eq!(
                        encode_string_with_replacement(charset.name, text).unwrap(),
                        bytes
                    );
                }
                Err(error) => {
                    assert_eq!(error.code(), 1366);
                    let replacement = encode_string(charset.name, "?").unwrap();
                    let mut expected = Vec::new();
                    for ch in text.chars() {
                        let mut buf = [0; 4];
                        let encoded = encode_string(charset.name, ch.encode_utf8(&mut buf))
                            .unwrap_or_else(|_| replacement.clone());
                        expected.extend_from_slice(&encoded);
                    }
                    assert_eq!(lossy.as_ref(), expected);
                    assert_eq!(
                        encode_string_with_replacement(charset.name, text).unwrap(),
                        expected
                    );
                }
            }
        }
    }
}

#[test]
fn replacement_keeps_unicode_character_boundaries_and_target_byte_order() {
    assert_eq!(
        encode_string_with_replacement("ASCII", "aé中😀z").unwrap(),
        b"a???z"
    );
    assert_eq!(
        encode_string_with_replacement("'utf8'", "é😀中").unwrap(),
        "é?中".as_bytes()
    );
    assert_eq!(
        encode_string_with_replacement("ucs2", "A😀é").unwrap(),
        [0, 65, 0, 63, 0, 233]
    );
    for name in ["unknown_charset", "'UNKNOWN_CHARSET'"] {
        let expected = resolve_charset(name).unwrap_err();
        for error in [
            encode_string(name, "text").unwrap_err(),
            encode_string_with_replacement(name, "😀").unwrap_err(),
            validate_text_for_charset(name, "text").unwrap_err(),
        ] {
            assert_eq!(error.code(), expected.code());
            assert_eq!(error.to_string(), expected.to_string());
        }
    }
}

#[test]
fn native_and_ascii_encodings_borrow_the_input() {
    for name in ["binary", "utf8mb4", "utf8mb3", "ascii", "swe7", "latin1"] {
        let text = "unchanged ASCII";
        let encoded = encode_string_borrowed(resolve_charset(name).unwrap(), text).unwrap();
        assert!(matches!(encoded, Cow::Borrowed(_)), "{name}");
        assert_eq!(encoded.as_ptr(), text.as_ptr(), "{name}");
    }
}

#[test]
fn name_indexes_resolve_all_canonical_quoted_and_uppercase_names() {
    for charset in CHARSETS {
        for name in [
            charset.name.to_owned(),
            charset.name.to_uppercase(),
            format!("'{}'", charset.name),
        ] {
            assert_eq!(resolve_charset(&name).unwrap(), charset);
        }
    }
    for collation in collations() {
        for name in [
            collation.name.to_owned(),
            collation.name.to_uppercase(),
            format!("'{}'", collation.name),
        ] {
            assert_eq!(resolve_collation(&name).unwrap(), collation);
        }
    }
    assert_eq!(resolve_charset("utf8").unwrap().name, "utf8mb3");
    assert_eq!(
        resolve_collation("utf8_general_ci").unwrap().name,
        "utf8mb3_general_ci"
    );
}
