//! Shared distinction between resident allocation and spill encoding size.

use crate::types::Value;

/// Heap capacity exclusively owned by a value. Container/header storage is
/// accounted by the operator that owns the container, preventing double count.
pub(crate) trait AllocatedBytes {
    fn allocated_bytes(&self) -> usize;
}

/// Bytes produced by the stable spill representation. This is intentionally
/// separate from [`AllocatedBytes`]: encoded length must never drive a resident
/// memory grant.
pub(crate) trait EncodedBytes {
    fn encoded_bytes(&self) -> usize;
}

/// Capacity owned by append/union-only hashbrown 0.14 tables. Include the
/// reserved control buckets and alignment, rather than counting live entries.
/// These callers never retain tombstones from partial deletion.
pub(crate) fn hash_table_capacity_bytes<T>(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    let Some(buckets) = capacity
        .saturating_add(capacity / 7)
        .checked_next_power_of_two()
    else {
        return usize::MAX;
    };
    buckets
        .saturating_mul(std::mem::size_of::<T>().saturating_add(1))
        .saturating_add(64)
}

impl<T: AllocatedBytes> AllocatedBytes for Vec<T> {
    fn allocated_bytes(&self) -> usize {
        self.capacity()
            .saturating_mul(std::mem::size_of::<T>())
            .saturating_add(
                self.iter()
                    .map(AllocatedBytes::allocated_bytes)
                    .sum::<usize>(),
            )
    }
}

impl AllocatedBytes for Value {
    fn allocated_bytes(&self) -> usize {
        match self {
            Value::Decimal(value) | Value::String(value) => value.capacity(),
            Value::DecimalWide(_) => self.decimal_heap_bytes(),
            Value::Bytes(value) | Value::Json(value) | Value::Geometry(value) => value.capacity(),
            Value::Vector(value) => value.capacity(),
            _ => 0,
        }
    }
}

impl AllocatedBytes for crate::engine::expr::ValueKey {
    fn allocated_bytes(&self) -> usize {
        use crate::engine::expr::ValueKey;
        match self {
            ValueKey::Bytes(value) | ValueKey::String(value) | ValueKey::Decimal(value)
                if value.spilled() =>
            {
                value.capacity()
            }
            _ => 0,
        }
    }
}

impl EncodedBytes for Value {
    fn encoded_bytes(&self) -> usize {
        match self {
            Value::Null => 1,
            Value::Int(_) | Value::UInt(_) | Value::Double(_) => 9,
            Value::Float(_) | Value::Date(_) => 5,
            Value::Time(_) | Value::DateTime(_) | Value::Timestamp(_) => 9,
            Value::Year(_) => 2,
            Value::DecimalCompact(..) => 10,
            Value::Decimal128(..) => 18,
            Value::DecimalWide(_) => 3usize.saturating_add(self.decimal_heap_bytes()),
            Value::SmallString(value) => 5usize.saturating_add(value.len()),
            Value::Decimal(value) | Value::String(value) => 5usize.saturating_add(value.len()),
            Value::Bytes(value) | Value::Json(value) | Value::Geometry(value) => {
                5usize.saturating_add(value.len())
            }
            Value::Vector(value) => 5usize.saturating_add(value.len()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocated_bytes_uses_capacity_and_covers_binary_values() {
        let value = |constructor: fn(Vec<u8>) -> Value| {
            let mut bytes = Vec::with_capacity(128);
            bytes.extend_from_slice(&[1, 2, 3]);
            constructor(bytes)
        };
        assert_eq!(value(Value::Json).allocated_bytes(), 128);
        assert_eq!(value(Value::Geometry).allocated_bytes(), 128);
        assert_eq!(value(|v| Value::Vector(v.into())).allocated_bytes(), 128);
        assert_eq!(Value::Int(1).allocated_bytes(), 0);
    }

    #[test]
    fn encoded_and_allocated_sizes_are_independent() {
        let mut value = String::with_capacity(256);
        value.push_str("abc");
        let value = Value::String(value);
        assert_eq!(value.allocated_bytes(), 256);
        assert_eq!(value.encoded_bytes(), 8);
    }
}
