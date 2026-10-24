use super::*;

fn packet(column_type: ColumnType, value: Value) -> Vec<u8> {
    binary_row_packet(
        &[Column {
            source: Default::default(),
            name: "value".into(),
            column_type,
        }],
        &[value],
    )
}

#[test]
fn time_wire_fields_preserve_sign_days_and_fraction() {
    for (micros, text, expected) in [
        (0, "00:00:00", vec![0]),
        (
            1,
            "00:00:00.000001",
            vec![12, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
        ),
        (
            -93_784_000_500,
            "-26:03:04.0005",
            vec![12, 1, 1, 0, 0, 0, 2, 3, 4, 244, 1, 0, 0],
        ),
        (
            3_020_399_999_999,
            "838:59:59.999999",
            vec![12, 0, 34, 0, 0, 0, 22, 59, 59, 63, 66, 15, 0],
        ),
        (93_784_000_000, "26:03:04", vec![8, 0, 1, 0, 0, 0, 2, 3, 4]),
    ] {
        let typed = packet(ColumnType::Time, Value::Time(micros));
        assert_eq!(&typed[..2], &[0, 0]);
        assert_eq!(&typed[2..], expected);
        let text = packet(ColumnType::Time, Value::String(text.into()));
        // The existing textual zero uses the valid eight-byte TIME body;
        // the compact lane uses the valid zero-length representation.
        if micros == 0 {
            assert_eq!(&text[2..], &[8, 0, 0, 0, 0, 0, 0, 0, 0]);
        } else {
            assert_eq!(text, typed);
        }
    }
}

#[test]
fn date_and_datetime_wire_fields_preserve_zero_and_pre_epoch_values() {
    for (ct, value, text, body) in [
        (
            ColumnType::Date,
            Value::Date(0),
            "1970-01-01",
            vec![4, 178, 7, 1, 1],
        ),
        (
            ColumnType::DateTime,
            Value::DateTime(0),
            "1970-01-01 00:00:00",
            vec![7, 178, 7, 1, 1, 0, 0, 0],
        ),
        (
            ColumnType::Timestamp,
            Value::Timestamp(-1),
            "1969-12-31 23:59:59.999999",
            vec![11, 177, 7, 12, 31, 23, 59, 59, 63, 66, 15, 0],
        ),
        (
            ColumnType::Timestamp,
            Value::Timestamp(crate::types::MAX_DATETIME_MICROS),
            "9999-12-31 23:59:59.999999",
            vec![11, 15, 39, 12, 31, 23, 59, 59, 63, 66, 15, 0],
        ),
    ] {
        let typed = packet(ct, value);
        assert_eq!(&typed[2..], body);
        assert_eq!(packet(ct, Value::String(text.into())), typed);
    }
    assert_eq!(
        packet(ColumnType::Date, Value::Date(crate::types::ZERO_DATE)),
        vec![0, 0, 0]
    );
    assert_eq!(
        packet(
            ColumnType::DateTime,
            Value::DateTime(crate::types::ZERO_DATETIME)
        ),
        vec![0, 0, 0]
    );
    let date = crate::types::encode_tagged_date(2024, 0, 5).unwrap();
    assert_eq!(
        &packet(ColumnType::Date, Value::Date(date))[2..],
        &[4, 232, 7, 0, 5]
    );
    let dt = crate::types::encode_tagged_datetime(2024, 0, 5, 12, 34, 56, 500).unwrap();
    assert_eq!(
        &packet(ColumnType::DateTime, Value::DateTime(dt))[2..],
        &[11, 232, 7, 0, 5, 12, 34, 56, 244, 1, 0, 0]
    );
}

#[test]
fn typed_and_textual_temporal_lanes_agree_over_fractional_ranges() {
    let mut seed = 17u64;
    for _ in 0..2048 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let time = (seed % 6_040_800_000_000) as i64 - 3_020_400_000_000;
        let datetime = (seed % 172_800_000_000_000) as i64 - 86_400_000_000_000;
        for (ct, value) in [
            (ColumnType::Time, Value::Time(time)),
            (ColumnType::DateTime, Value::DateTime(datetime)),
            (ColumnType::Timestamp, Value::Timestamp(datetime)),
        ] {
            let text = value.to_display_string();
            assert_eq!(packet(ct, value), packet(ct, Value::String(text)));
        }
    }
}
