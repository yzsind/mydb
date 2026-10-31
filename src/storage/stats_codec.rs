//! Versioned, checked binary payloads for individual optimizer-statistics objects.
use super::stats::*;
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::engine::executor::memory_footprint::AllocatedBytes;
use crate::types::{QueryError, Value};

pub(super) fn corrupt() -> QueryError {
    super::error(
        crate::error::catalog::ER_GET_ERRNO,
        "corrupt optimizer statistics object",
    )
}

pub(super) trait Binary: Sized {
    fn put(&self, out: &mut Vec<u8>) -> Result<(), QueryError>;
    fn get(input: &mut Reader<'_>) -> Result<Self, QueryError>;
    fn heap_bytes(&self) -> usize {
        0
    }
}

pub(super) struct Reader<'a> {
    bytes: &'a [u8],
    charge: ControlledMemoryCharge,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], QueryError> {
        if n > self.bytes.len() {
            return Err(corrupt());
        }
        let (head, tail) = self.bytes.split_at(n);
        self.bytes = tail;
        Ok(head)
    }
    fn reserve(&mut self, n: usize) -> Result<(), QueryError> {
        self.charge.try_grow(n)
    }
}

macro_rules! number {
    ($($t:ty),*) => {$ (
        impl Binary for $t {
            fn put(&self, out: &mut Vec<u8>) -> Result<(), QueryError> {
                out.extend_from_slice(&self.to_le_bytes()); Ok(())
            }
            fn get(r: &mut Reader<'_>) -> Result<Self, QueryError> {
                Ok(Self::from_le_bytes(r.take(std::mem::size_of::<Self>())?.try_into().map_err(|_| corrupt())?))
            }
        }
    )*};
}
number!(u8, u32, u64, i64, f64);
impl Binary for bool {
    fn put(&self, out: &mut Vec<u8>) -> Result<(), QueryError> {
        (*self as u8).put(out)
    }
    fn get(r: &mut Reader<'_>) -> Result<Self, QueryError> {
        match u8::get(r)? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(corrupt()),
        }
    }
}
impl<T: Binary> Binary for Option<T> {
    fn put(&self, out: &mut Vec<u8>) -> Result<(), QueryError> {
        self.is_some().put(out)?;
        if let Some(v) = self {
            v.put(out)?;
        }
        Ok(())
    }
    fn get(r: &mut Reader<'_>) -> Result<Self, QueryError> {
        if bool::get(r)? {
            Ok(Some(T::get(r)?))
        } else {
            Ok(None)
        }
    }
    fn heap_bytes(&self) -> usize {
        self.as_ref().map_or(0, Binary::heap_bytes)
    }
}
impl<T: Binary> Binary for Vec<T> {
    fn put(&self, out: &mut Vec<u8>) -> Result<(), QueryError> {
        u32::try_from(self.len()).map_err(|_| corrupt())?.put(out)?;
        for value in self {
            value.put(out)?;
        }
        Ok(())
    }
    fn get(r: &mut Reader<'_>) -> Result<Self, QueryError> {
        let count = u32::get(r)? as usize;
        // Every supported element consumes at least one byte. Validate before allocation.
        if count > r.bytes.len() {
            return Err(corrupt());
        }
        r.reserve(
            count
                .checked_mul(std::mem::size_of::<T>())
                .ok_or_else(corrupt)?,
        )?;
        let mut out = Vec::new();
        out.try_reserve_exact(count).map_err(|_| corrupt())?;
        for _ in 0..count {
            out.push(T::get(r)?);
        }
        Ok(out)
    }
    fn heap_bytes(&self) -> usize {
        self.capacity()
            .saturating_mul(std::mem::size_of::<T>())
            .saturating_add(self.iter().map(Binary::heap_bytes).sum::<usize>())
    }
}
impl Binary for String {
    fn put(&self, out: &mut Vec<u8>) -> Result<(), QueryError> {
        u32::try_from(self.len()).map_err(|_| corrupt())?.put(out)?;
        out.extend_from_slice(self.as_bytes());
        Ok(())
    }
    fn get(r: &mut Reader<'_>) -> Result<Self, QueryError> {
        let len = u32::get(r)? as usize;
        let bytes = r.take(len)?;
        r.reserve(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| corrupt())
    }
    fn heap_bytes(&self) -> usize {
        self.capacity()
    }
}
impl Binary for Value {
    fn put(&self, out: &mut Vec<u8>) -> Result<(), QueryError> {
        let offset = out.len();
        0u32.put(out)?;
        super::core::encode_row_into(std::slice::from_ref(self), out)?;
        let len = u32::try_from(out.len() - offset - 4).map_err(|_| corrupt())?;
        out[offset..offset + 4].copy_from_slice(&len.to_le_bytes());
        Ok(())
    }
    fn get(r: &mut Reader<'_>) -> Result<Self, QueryError> {
        let len = u32::get(r)? as usize;
        let bytes = r.take(len)?;
        // A one-value row always starts with a one-byte count. Never admit an attacker-controlled row count.
        if bytes.first() != Some(&1) || super::core::encoded_row_skip_len(bytes)? != bytes.len() {
            return Err(corrupt());
        }
        r.reserve(
            len.saturating_mul(2)
                .saturating_add(std::mem::size_of::<Value>()),
        )?;
        super::core::decode_row(bytes)?.pop().ok_or_else(corrupt)
    }
    fn heap_bytes(&self) -> usize {
        self.allocated_bytes()
    }
}
impl Binary for FrequencySource {
    fn put(&self, out: &mut Vec<u8>) -> Result<(), QueryError> {
        match self {
            Self::Exact => 0u8,
            Self::Sample => 1,
            Self::LowerBound => 2,
        }
        .put(out)
    }
    fn get(r: &mut Reader<'_>) -> Result<Self, QueryError> {
        match u8::get(r)? {
            0 => Ok(Self::Exact),
            1 => Ok(Self::Sample),
            2 => Ok(Self::LowerBound),
            _ => Err(corrupt()),
        }
    }
}
macro_rules! object {
    ($t:ty { $($field:ident),* $(,)? }) => {
        impl Binary for $t {
            fn put(&self, out: &mut Vec<u8>) -> Result<(), QueryError> { $(self.$field.put(out)?;)* Ok(()) }
            fn get(r: &mut Reader<'_>) -> Result<Self, QueryError> { Ok(Self { $($field: Binary::get(r)?),* }) }
            fn heap_bytes(&self) -> usize { 0usize $(.saturating_add(self.$field.heap_bytes()))* }
        }
    };
}
object!(FrequencyStats {
    source,
    sample_rows,
    complete,
    count_error
});
object!(ValueFrequency { value, count });
object!(HistogramBucket {
    upper,
    cumulative_count
});
object!(ColumnStats {
    null_count,
    distinct_count,
    min,
    max,
    top_values,
    frequency,
    average_width_bytes,
    histogram
});
object!(IndexStats {
    prefix_distinct_counts,
    leaf_page_count,
    internal_page_count,
    tree_height,
    correlation,
    fulltext
});
object!(FulltextStats {
    doc_count,
    avg_token_count,
    token_df_top,
    df_histogram
});
object!(TokenFrequency {
    token,
    document_count
});
object!(ConditionalKeyDomainEntry {
    filter_values,
    row_count,
    key_distinct_count,
    key_min,
    key_max
});
object!(SqlHistogramBucket {
    lower,
    upper,
    cumulative_frequency,
    distinct_count
});
object!(SqlHistogram {
    buckets,
    data_type,
    null_fraction,
    updated,
    sample_fraction,
    singleton,
    requested_buckets,
    collation_id
});

/// The checksum binds the version, kind and exact payload length to the body.
pub(super) fn encode<T: Binary>(kind: u8, value: &T) -> Result<Vec<u8>, QueryError> {
    let _charge = crate::engine::executor::mem_budget::force_charge_scoped(
        value.heap_bytes().saturating_mul(2).saturating_add(1024),
    )?;
    let mut out = b"MST\x01".to_vec();
    out.push(kind);
    0u32.put(&mut out)?;
    value.put(&mut out)?;
    let len = u32::try_from(out.len() - 9).map_err(|_| corrupt())?;
    out[5..9].copy_from_slice(&len.to_le_bytes());
    let crc = crc32fast::hash(&out);
    crc.put(&mut out)?;
    Ok(out)
}
pub(super) fn decode<T: Binary>(kind: u8, bytes: &[u8]) -> Result<T, QueryError> {
    if bytes.len() < 13 || &bytes[..4] != b"MST\x01" || bytes[4] != kind {
        return Err(corrupt());
    }
    let n = u32::from_le_bytes(bytes[5..9].try_into().map_err(|_| corrupt())?) as usize;
    if n != bytes.len() - 13 {
        return Err(corrupt());
    }
    let end = bytes.len() - 4;
    if crc32fast::hash(&bytes[..end])
        != u32::from_le_bytes(bytes[end..].try_into().map_err(|_| corrupt())?)
    {
        return Err(corrupt());
    }
    let mut input = Reader {
        bytes: &bytes[9..end],
        charge: ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)?,
    };
    let out = T::get(&mut input)?;
    if !input.bytes.is_empty() {
        return Err(corrupt());
    }
    Ok(out)
}

impl TableStats {
    /// Retained allocations charged by the existing bounded descriptor cache.
    pub(crate) fn allocated_bytes(&self) -> usize {
        fn map<T: Binary>(values: &std::collections::BTreeMap<String, T>) -> usize {
            values
                .iter()
                .map(|(k, v)| 64 + std::mem::size_of::<T>() + k.capacity() + v.heap_bytes())
                .sum()
        }
        map(&self.columns)
            + map(&self.indexes)
            + map(&self.explicit_histograms)
            + self
                .partition_stats
                .values()
                .map(|s| 128 + map(&s.columns) + map(&s.indexes))
                .sum::<usize>()
            + self
                .multi_column_stats
                .iter()
                .map(|(k, s)| 96 + k.capacity() + s.columns.heap_bytes())
                .sum::<usize>()
            + self
                .conditional_key_domains
                .iter()
                .map(|s| {
                    96 + s.filter_columns.heap_bytes()
                        + s.key_columns.heap_bytes()
                        + s.entries.heap_bytes()
                })
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn binary_stats_values_and_corruption() {
        let values = vec![
            Value::Null,
            Value::Int(i64::MIN),
            Value::UInt(u64::MAX),
            Value::Float(1.25),
            Value::Double(-9.5),
            Value::Decimal("12345678901234567890.123456789".into()),
            Value::DecimalCompact(-12345, 3),
            Value::Decimal128(i128::MAX, 10),
            Value::Bytes(vec![0, 255, 1]),
            Value::String("统计\0é".into()),
            Value::Date(i32::MIN),
            Value::Time(-123456),
            Value::DateTime(i64::MIN),
            Value::Timestamp(i64::MAX),
            Value::Year(255),
        ];
        let mut values = values;
        values.push(Value::DecimalWide(Box::new(crate::types::DecimalWide {
            magnitude: "12345678901234567890123456789012345678901234567890123456789012345"
                .parse()
                .unwrap(),
            scale: 20,
        })));
        values.push(Value::Json(
            crate::engine::json::parse_text_opt("{\"a\":[1,true,null]}").unwrap(),
        ));
        values.push(Value::Geometry(vec![0, 0, 0, 0, 1, 1, 0, 0, 0]));
        values.push(Value::Vector(crate::vector::VectorValue::new(
            crate::vector::VectorDataType::Float32,
            2,
            [1.0f32.to_le_bytes(), (-2.0f32).to_le_bytes()].concat(),
        )));
        let encoded = encode(1, &values).unwrap();
        let decoded: Vec<Value> = decode(1, &encoded).unwrap();
        assert_eq!(values, decoded);
        for cut in 0..encoded.len() {
            assert!(decode::<Vec<Value>>(1, &encoded[..cut]).is_err());
        }
        for i in 0..encoded.len() {
            let mut broken = encoded.clone();
            broken[i] ^= 0x80;
            assert!(decode::<Vec<Value>>(1, &broken).is_err());
        }
        assert!(decode::<Vec<Value>>(2, &encoded).is_err());
    }
}

impl<A: Binary, B: Binary> Binary for (A, B) {
    fn put(&self, out: &mut Vec<u8>) -> Result<(), QueryError> {
        self.0.put(out)?;
        self.1.put(out)
    }
    fn get(r: &mut Reader<'_>) -> Result<Self, QueryError> {
        Ok((A::get(r)?, B::get(r)?))
    }
    fn heap_bytes(&self) -> usize {
        self.0.heap_bytes().saturating_add(self.1.heap_bytes())
    }
}
impl<A: Binary, B: Binary, C: Binary> Binary for (A, B, C) {
    fn put(&self, out: &mut Vec<u8>) -> Result<(), QueryError> {
        self.0.put(out)?;
        self.1.put(out)?;
        self.2.put(out)
    }
    fn get(r: &mut Reader<'_>) -> Result<Self, QueryError> {
        Ok((A::get(r)?, B::get(r)?, C::get(r)?))
    }
    fn heap_bytes(&self) -> usize {
        self.0
            .heap_bytes()
            .saturating_add(self.1.heap_bytes())
            .saturating_add(self.2.heap_bytes())
    }
}
