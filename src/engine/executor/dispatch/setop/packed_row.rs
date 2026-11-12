//! Replay representatives retain scalar payloads rather than Value arrays.
//! Equality keys remain separate: collation/numeric normalization must never
//! change the first representative's original SQL value or physical type.
use super::*;
use crate::engine::executor::{mem_budget, spill_value};
use std::io::Write;

pub(super) struct PackedRow(Vec<u8>);

fn io_error(error: std::io::Error) -> QueryError {
    crate::error::catalog::ER_UNKNOWN_ERROR.error(format!("set representative encoding: {error}"))
}

impl PackedRow {
    pub(super) fn encoded_len(row: &[Value]) -> Result<usize, QueryError> {
        struct Counter(usize);
        impl Write for Counter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(bytes.len())
                    .ok_or_else(|| std::io::Error::other("set representative size overflow"))?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut counter = Counter(0);
        for value in row {
            spill_value::write_value(&mut counter, value).map_err(io_error)?;
        }
        Ok(counter.0)
    }

    // The caller reserves this exact payload size and the table growth before
    // allocating. No decoded payload is retained after inserting a new key.
    pub(super) fn encode(row: &[Value], bytes: usize) -> Result<Self, QueryError> {
        let mut encoded = mem_budget::try_vec_with_capacity(bytes, "packed set representative")?;
        for value in row {
            spill_value::write_value(&mut encoded, value).map_err(io_error)?;
        }
        debug_assert_eq!(encoded.len(), bytes);
        Ok(Self(encoded))
    }

    pub(super) fn encoded(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::select::grace::{SpillTmpDir, ValueRowReader, ValueRowWriter};

    #[test]
    fn encoded_representatives_preserve_all_payloads_and_empty_rows() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let dir = SpillTmpDir::create_kind(root.path(), "setop").unwrap();
        let row = vec![
            Value::Null,
            Value::Int(i64::MIN),
            Value::UInt(u64::MAX),
            Value::Float(f32::from_bits(0x7fc00123)),
            Value::Double(-0.0),
            Value::Decimal("123.450".into()),
            Value::DecimalCompact(-123, 2),
            Value::Decimal128(i128::MIN, 10),
            Value::DecimalWide(Box::new(crate::types::DecimalWide {
                magnitude: num_bigint::BigInt::from(i128::MAX) * 1000,
                scale: 4,
            })),
            Value::String("AbÉ短文本".into()),
            Value::String("wide".repeat(256)),
            Value::SmallString(crate::types::SmallStringBuf::from_slice(b"inline")),
            Value::Bytes(vec![0, 255]),
            Value::Date(crate::types::ZERO_DATE),
            Value::Time(-123),
            Value::DateTime(crate::types::ZERO_DATETIME),
            Value::Timestamp(12345678),
            Value::Year(99),
            Value::Json(vec![1, 2]),
            Value::Geometry(vec![3, 4]),
            Value::Vector(vec![5, 6, 0, 0].into()),
        ];
        for (index, row) in [row, vec![]].into_iter().enumerate() {
            let packed = PackedRow::encode(&row, PackedRow::encoded_len(&row).unwrap()).unwrap();
            for prefix in [vec![], vec![Value::UInt(4), Value::UInt(7)]] {
                let path = dir.path().join(format!("payload-{index}-{}", prefix.len()));
                let mut writer =
                    ValueRowWriter::create(&path, prefix.len() + row.len(), dir.charger()).unwrap();
                writer
                    .append_encoded_prefixed(&prefix, row.len(), packed.encoded())
                    .unwrap();
                writer
                    .append_encoded_prefixed(&prefix, row.len(), packed.encoded())
                    .unwrap();
                writer.finish().unwrap();
                let mut reader = ValueRowReader::open(&path, dir.charger()).unwrap().unwrap();
                let actual = reader.next().unwrap().unwrap();
                let expected = prefix
                    .iter()
                    .cloned()
                    .chain(row.clone())
                    .collect::<Vec<_>>();
                // Re-encoding also checks NaN bits, signed zero, DECIMAL
                // scale, representative casing and binary payload identity.
                let expected =
                    PackedRow::encode(&expected, PackedRow::encoded_len(&expected).unwrap())
                        .unwrap();
                let actual =
                    PackedRow::encode(&actual, PackedRow::encoded_len(&actual).unwrap()).unwrap();
                assert_eq!(actual.encoded(), expected.encoded());
                assert!(reader.next().unwrap().is_some());
                assert!(reader.next().unwrap().is_none());
                let mut reader = ValueRowReader::open(&path, dir.charger()).unwrap().unwrap();
                let first = reader.next_inline_reuse(Vec::new()).unwrap().unwrap();
                if !row.is_empty() {
                    assert!(matches!(first[prefix.len() + 9], Value::SmallString(_)));
                    assert!(matches!(first[prefix.len() + 10], Value::String(_)));
                }
                let address = first.as_ptr();
                let second = reader.next_inline_reuse(first).unwrap().unwrap();
                assert_eq!(second.as_ptr(), address, "replay must reuse its row buffer");
                let encoded =
                    PackedRow::encode(&second, PackedRow::encoded_len(&second).unwrap()).unwrap();
                assert_eq!(encoded.encoded(), expected.encoded());
                assert!(reader.next_inline_reuse(second).unwrap().is_none());
            }
        }
    }
}
