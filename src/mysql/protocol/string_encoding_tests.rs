use super::*;

fn column(column_type: ColumnType) -> Column {
    Column {
        name: "value".into(),
        column_type,
        source: Default::default(),
    }
}

fn small(text: &str) -> Value {
    Value::SmallString(crate::types::SmallStringBuf::from_slice(text.as_bytes()))
}

#[test]
fn binary_inline_temporal_strings_follow_advertised_column_types() {
    for (ct, text) in [
        (ColumnType::Date, "2024-02-29"),
        (ColumnType::Date, "0000-00-00"),
        (ColumnType::Time, "-26:03:04.000500"),
        (ColumnType::Time, "00:00:00"),
        (ColumnType::Time, "            -26:03:04.000500            "),
        (
            ColumnType::DateTime,
            "        2024-02-29 12:34:56.123456        ",
        ),
        (ColumnType::DateTime, "2024-02-29 12:34:56.123456"),
        (ColumnType::Timestamp, "1969-12-31 23:59:59.999999"),
    ] {
        let columns = [
            column(ct),
            column(ColumnType::Long),
            column(ColumnType::VarString),
        ];
        let expected = binary_row_packet(
            &columns,
            &[Value::String(text.into()), Value::Int(123456), Value::Null],
        );
        let actual = binary_row_packet(&columns, &[small(text), Value::Int(123456), Value::Null]);
        assert_eq!(actual, expected, "{ct:?}: {text}");
        // Validate the temporal body independently through the parameter decoder,
        // then ensure the following cell and NULL bitmap remain aligned.
        assert_eq!(actual[1], 1 << 4);
        let mut cursor = Cursor::new(&actual[2..]);
        if ct == ColumnType::Time {
            parse_binary_time(&mut cursor).unwrap();
        } else {
            parse_binary_temporal(&mut cursor).unwrap();
        }
        assert_eq!(cursor.read_bytes(4).unwrap(), &123456_i32.to_le_bytes());
    }
}

#[test]
fn binary_string_lanes_preserve_bytes_with_and_without_metadata() {
    for text in [
        "".to_string(),
        "a\0b".into(),
        "中文é".into(),
        "x".repeat(31),
        "x".repeat(32),
        "é".repeat(128),
    ] {
        for columns in [
            vec![],
            vec![column(ColumnType::VarString)],
            vec![column(ColumnType::Blob)],
        ] {
            assert_eq!(
                binary_row_packet(&columns, &[small(&text)]),
                binary_row_packet(&columns, &[Value::String(text.clone())])
            );
        }
    }
}

#[test]
fn text_string_lanes_share_charset_conversion_and_reused_buffers() {
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for charset in [
        None,
        Some("utf8mb4"),
        Some("latin1"),
        Some("ascii"),
        Some("gbk"),
        Some("binary"),
    ] {
        for text in [
            "".to_string(),
            "a\0b".into(),
            "中文é".into(),
            "é".repeat(128),
            "a".into(),
        ] {
            text_row_packet_into_with_charset(
                &[small(&text), Value::Null, Value::UInt(42)],
                &mut actual,
                charset,
            );
            text_row_packet_into_with_charset(
                &[Value::String(text), Value::Null, Value::UInt(42)],
                &mut expected,
                charset,
            );
            assert_eq!(actual, expected, "{charset:?}");
        }
    }
}

#[test]
fn length_encoded_strings_preserve_prefixes_and_capacity_boundaries() {
    let lengths = (0..=251).chain([255, 256, 65535, 65536]);
    for len in lengths {
        let value: Vec<u8> = (0..len).map(|i| (i % 256) as u8).collect();
        for prefix_len in [0, 1, 17] {
            for spare in [0, 1, len, len + 9] {
                let mut actual = Vec::with_capacity(prefix_len + spare);
                actual.resize(prefix_len, 0xa5);
                let mut expected = actual.clone();
                write_lenenc_int(&mut expected, len as u64);
                expected.extend_from_slice(&value);
                write_lenenc_string(&mut actual, &value);
                assert_eq!(
                    actual, expected,
                    "len={len}, prefix={prefix_len}, spare={spare}"
                );
                let mut cursor = Cursor::new(&actual[prefix_len..]);
                assert_eq!(cursor.read_lenenc_bytes().unwrap(), value);
                actual.truncate(prefix_len);
                write_lenenc_string(&mut actual, &value);
                assert_eq!(actual, expected);
            }
        }
    }
}
