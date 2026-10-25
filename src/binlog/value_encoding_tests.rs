use super::*;

fn column(kind: u8, metadata: Vec<u8>) -> BinlogColumn {
    BinlogColumn {
        name: "v".into(),
        physical_index: 0,
        column_type: kind,
        metadata,
        nullable: false,
        unsigned: false,
        enum_set_values: None,
        vector_dim: None,
        charset: None,
        collation_id: crate::charset::BINARY_COLLATION_ID,
    }
}

#[test]
fn decimal_encoding_appends_exact_bytes_and_keeps_prefix_on_errors() {
    for (text, expected) in [
        ("12.34", [0x8c, 0x22]),
        ("-12.34", [0x73, 0xdd]),
        ("0.00", [0x80, 0x00]),
    ] {
        let mut out = vec![0x12, 0x34, 0x80];
        encode_mysql_decimal_binary(text, 4, 2, &mut out).unwrap();
        assert_eq!(&out[..3], &[0x12, 0x34, 0x80]);
        assert_eq!(&out[3..], &expected);
    }
    for (text, code) in [
        ("1x.23", 1366),
        ("1.2.3", 1366),
        ("1e3", 1366),
        ("NaN", 1366),
        ("１２", 1366),
        ("123.45", 1264),
    ] {
        let mut out = vec![0x12, 0x34, 0x80];
        assert_eq!(
            encode_mysql_decimal_binary(text, 4, 2, &mut out)
                .unwrap_err()
                .code(),
            code,
            "{text}"
        );
        assert_eq!(out, [0x12, 0x34, 0x80]);
    }
}

#[cfg(feature = "mysql-client")]
#[test]
fn decimal_groups_and_value_lanes_round_trip_through_mysql_decoder() {
    use mysql_common::binlog::decimal::Decimal;
    for precision in 1..=65 {
        for scale in 0..=precision.min(30) {
            let integer = if precision > scale { "1" } else { "0" };
            let text = if scale == 0 {
                integer.to_string()
            } else {
                format!("{integer}.{}", "1".repeat(scale))
            };
            for text in [text.clone(), format!("-{text}")] {
                let mut out = vec![0xaa, 0xbb];
                encode_mysql_decimal_binary(&text, precision, scale, &mut out).unwrap();
                assert_eq!(&out[..2], &[0xaa, 0xbb]);
                assert_eq!(out.len() - 2, mysql_decimal_bin_size(precision, scale));
                assert_eq!(
                    Decimal::read_bin(&out[2..], precision, scale, false)
                        .unwrap()
                        .to_string(),
                    text
                );
            }
        }
    }
    let wide = Value::DecimalWide(Box::new(crate::types::DecimalWide {
        magnitude: "12345678901234567890123456789012345678901234567890"
            .parse()
            .unwrap(),
        scale: 7,
    }));
    let mut cases = vec![
        (wide, 7),
        (Value::Decimal128(i128::MIN, 9), 9),
        (Value::Decimal128(i128::MAX, 0), 0),
        (Value::Decimal("-12.3400".into()), 4),
        (Value::String("12.3400".into()), 4),
        (Value::SmallString(b"12.3400".iter().copied().collect()), 4),
    ];
    for scale in [0, 1, 2, 9, 18, 30] {
        for magnitude in [i64::MIN, -1234, -1, 0, 1, 1234, i64::MAX] {
            cases.push((Value::DecimalCompact(magnitude, scale), scale));
        }
    }
    for (value, scale) in cases {
        let mut out = vec![0xaa];
        encode_decimal_value(
            &column(MYSQL_TYPE_NEWDECIMAL, vec![65, scale]),
            &value,
            &mut out,
        )
        .unwrap();
        assert_eq!(out[0], 0xaa);
        assert_eq!(
            Decimal::read_bin(&out[1..], 65, usize::from(scale), false)
                .unwrap()
                .to_string(),
            value.to_display_string()
        );
    }
}

#[test]
fn binlog_byte_views_preserve_binary_and_text_lanes() {
    let mut geometry = vec![0, 0, 0, 0, 1, 1, 0, 0, 0];
    geometry.extend_from_slice(&1f64.to_le_bytes());
    geometry.extend_from_slice(&2f64.to_le_bytes());
    let cases = [
        Value::String("中\0文".into()),
        Value::SmallString(b"small\0text".iter().copied().collect()),
        Value::Decimal("-12.3400".into()),
        Value::Bytes(vec![0, 0xff, 0x80]),
        Value::Geometry(geometry),
        Value::Vector(1f32.to_le_bytes().to_vec().into()),
    ];
    for value in cases {
        if let Value::Vector(v) = &value {
            assert_eq!(value_bytes(&value).as_ref(), v.encode());
            continue;
        }
        let expected = match &value {
            Value::String(s) | Value::Decimal(s) => s.as_bytes(),
            Value::SmallString(s) => s.as_slice(),
            Value::Bytes(b) | Value::Geometry(b) => b.as_slice(),
            Value::Vector(b) => b.as_slice(),
            _ => unreachable!(),
        };
        let bytes = value_bytes(&value);
        assert!(matches!(&bytes, Cow::Borrowed(_)));
        assert_eq!(bytes.as_ref(), expected);
        assert_eq!(bytes.as_ptr(), expected.as_ptr());
        let mut out = vec![0xaa];
        encode_value(&column(MYSQL_TYPE_BLOB, vec![4]), &value, &mut out).unwrap();
        assert_eq!(
            u32::from_le_bytes(out[1..5].try_into().unwrap()) as usize,
            expected.len()
        );
        assert_eq!(&out[5..], expected);
    }
    let json = Value::Json(crate::engine::json::parse_text("{\"a\":[1,null]}", 1, "test").unwrap());
    for value in [
        json,
        Value::Int(-2),
        Value::UInt(u64::MAX),
        Value::Double(2.5),
        Value::Date(0),
        Value::Null,
    ] {
        assert_eq!(
            value_bytes(&value).as_ref(),
            value.to_display_string().as_bytes()
        );
    }
    let mut out = vec![0xaa];
    assert_eq!(
        encode_value(
            &column(MYSQL_TYPE_BLOB, vec![1]),
            &Value::Bytes(vec![0; 256]),
            &mut out
        )
        .unwrap_err()
        .code(),
        1406
    );
    assert_eq!(out, [0xaa]);
}
