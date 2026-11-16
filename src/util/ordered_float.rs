//! Reversible IEEE-754 total-order transforms for binary keys.
//!
//! Float transforms preserve IEEE total order, including the sign of zero and
//! NaN payloads. SQL-specific normalization and rejection stay with callers.

#[inline]
pub(crate) fn f32_to_order_u32(v: f32) -> u32 {
    let bits = v.to_bits();
    if bits & 0x8000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000
    }
}

#[inline]
pub(crate) fn f32_from_order_u32(v: u32) -> f32 {
    let bits = if v & 0x8000_0000 != 0 {
        v ^ 0x8000_0000
    } else {
        !v
    };
    f32::from_bits(bits)
}

#[inline]
pub(crate) fn f64_to_order_u64(v: f64) -> u64 {
    let bits = v.to_bits();
    if bits & 0x8000_0000_0000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000_0000_0000
    }
}

#[inline]
pub(crate) fn f64_from_order_u64(v: u64) -> f64 {
    let bits = if v & 0x8000_0000_0000_0000 != 0 {
        v ^ 0x8000_0000_0000_0000
    } else {
        !v
    };
    f64::from_bits(bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_f32(a: u32, b: u32) {
        let a = f32::from_bits(a);
        let b = f32::from_bits(b);
        let encoded = f32_to_order_u32(a);
        assert_eq!(f32_from_order_u32(encoded).to_bits(), a.to_bits());
        assert_eq!(encoded.cmp(&f32_to_order_u32(b)), a.total_cmp(&b));
    }

    fn check_f64(a: u64, b: u64) {
        let a = f64::from_bits(a);
        let b = f64::from_bits(b);
        let encoded = f64_to_order_u64(a);
        assert_eq!(f64_from_order_u64(encoded).to_bits(), a.to_bits());
        assert_eq!(encoded.cmp(&f64_to_order_u64(b)), a.total_cmp(&b));
    }

    #[test]
    fn float_keys_preserve_all_bits_and_ieee_total_order() {
        let patterns32 = [
            0,
            1,
            u32::MAX,
            1 << 31,
            0x7f80_0000,
            0xff80_0000,
            0x7f80_0001,
            0xff80_0001,
            0x7fc0_0000,
            0xffc0_0000,
        ];
        let patterns64 = [
            0,
            1,
            u64::MAX,
            1 << 63,
            0x7ff0_0000_0000_0000,
            0xfff0_0000_0000_0000,
            0x7ff0_0000_0000_0001,
            0xfff0_0000_0000_0001,
            0x7ff8_0000_0000_0000,
            0xfff8_0000_0000_0000,
        ];
        for a in patterns32 {
            for b in patterns32 {
                check_f32(a, b);
            }
        }
        for a in patterns64 {
            for b in patterns64 {
                check_f64(a, b);
            }
        }
        let mut bits = 0x1234_5678_9abc_def0u64;
        for _ in 0..16_384 {
            let previous = bits;
            bits ^= bits << 13;
            bits ^= bits >> 7;
            bits ^= bits << 17;
            check_f32(previous as u32, bits as u32);
            check_f64(previous, bits);
        }
    }
}
