use super::TableChecksum;
use crate::types::{DecimalWide, Value};

#[test]
fn checksum_digest_matches_display_bytes_for_all_value_variants_and_boundaries() {
    let mut values = vec![
        Value::Null,
        Value::Int(i64::MIN),
        Value::Int(i64::MAX),
        Value::UInt(u64::MAX),
        Value::Float(-0.0),
        Value::Float(f32::INFINITY),
        Value::Float(f32::NAN),
        Value::Double(1.23456789012345e-100),
        Value::Double(f64::NEG_INFINITY),
        Value::Decimal("-000.12300".into()),
        Value::Decimal128(i128::MIN, 30),
        Value::DecimalWide(Box::new(DecimalWide {
            magnitude: "123456789012345678901234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            scale: 30,
        })),
        Value::String("中文\0\u{1} trailing  ".into()),
        Value::SmallString(crate::types::SmallStringBuf::from_slice(b"a\0b")),
        Value::Bytes(vec![0, 0xff, 0xc0, b'a', 0xe2, 0x82]),
        Value::Bytes(b"valid utf8".to_vec()),
        Value::Date(i32::MIN),
        Value::Date(-1),
        Value::Date(20_000),
        Value::Time(-3_020_399_999_999),
        Value::Time(1),
        Value::DateTime(i64::MIN),
        Value::DateTime(1_700_000_000_123_456),
        Value::Timestamp(-1),
        Value::Year(0),
        Value::Year(255),
        Value::Json(crate::engine::json::parse_text_opt(r#"{"a":[1,true,null,"中文"]}"#).unwrap()),
        Value::Vector(
            [1.0f32, -0.25, 0.0]
                .into_iter()
                .flat_map(f32::to_le_bytes)
                .collect(),
        ),
    ];
    // The wide-scale fallback must not overflow the canonical stack buffer.
    for scale in [0, 1, 2, 18, 30, 31, 48, 255] {
        for magnitude in [i64::MIN, -1, 0, 1, i64::MAX] {
            values.push(Value::DecimalCompact(magnitude, scale));
        }
    }
    // A little-endian WKB point (1, 2).
    let mut point = vec![0, 0, 0, 0, 1, 1, 0, 0, 0];
    point.extend_from_slice(&1.0f64.to_le_bytes());
    point.extend_from_slice(&2.0f64.to_le_bytes());
    values.push(Value::Geometry(point));
    for len in [0, 1, 65535, 65536, 65537, 200000] {
        values.push(Value::String("x".repeat(len)));
    }
    let mut expected = crc32fast::Hasher::new();
    let mut actual = TableChecksum::new();
    // Check individual values too, so one variant cannot mask another error.
    for value in &values {
        let mut single = TableChecksum::new();
        single.update_row(std::slice::from_ref(value));
        let bytes = format!("{}\u{1}", value.to_display_string());
        assert_eq!(
            single.finalize(),
            crc32fast::hash(bytes.as_bytes()),
            "{value:?}"
        );
    }
    for row in [values.as_slice(), &[], values.as_slice()] {
        for (i, value) in row.iter().enumerate() {
            if i != 0 {
                expected.update(&[0]);
            }
            expected.update(value.to_display_string().as_bytes());
        }
        expected.update(&[1]);
        actual.update_row(row);
        assert!(actual.buffer.len() <= TableChecksum::BUFFER_BYTES);
    }
    assert_eq!(actual.finalize(), expected.finalize());
}
