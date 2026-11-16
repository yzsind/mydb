//! Static `10^N` lookup tables for fast decimal scale shifts.
//!
//! Replaces `10i128.pow(n)` / `10i64.pow(n)` inside per-row decimal
//! arithmetic. `i128::pow` is `unsigned -> i128` via repeated squaring in
//! a non-inlined helper, so it shows up at ~3.2% self time in TPC-H Q1's
//! `scale_shift_i128` (profile: 740 ms out of ~23 s total CPU). A
//! const lookup collapses it to one load + one multiply.
//!
//! Both tables are exhaustive over the valid scale range:
//!   * `POW10_I64`  — `10^0 .. 10^18` (10^19 overflows i64)
//!   * `POW10_I128` — `10^0 .. 10^38` (10^39 overflows i128)
//!
//! Indices outside that range are caller-guarded; the const arrays panic
//! on out-of-bounds rather than silently wrapping, matching the prior
//! `pow()` overflow semantics (which would panic in debug, wrap in
//! release).

/// `10^n` for `n in 0..=18`. `10^19` overflows `i64`.
pub const POW10_I64_MAX_EXP: u8 = 18;

/// `10^n` for `n in 0..=38`. `10^39` overflows `i128`.
pub const POW10_I128_MAX_EXP: u8 = 38;

pub const POW10_I64: [i64; 19] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
];

pub const POW10_I128: [i128; 39] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
    10_000_000_000_000_000_000,
    100_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000_000_000,
];

/// `10^n` as `i128`. Panics if `n > 38`.
#[inline(always)]
pub fn pow10_i128(n: u8) -> i128 {
    POW10_I128[n as usize]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pow10_i64_matches_runtime() {
        for n in 0u8..=18 {
            assert_eq!(POW10_I64[n as usize], 10i64.pow(n as u32));
        }
    }

    #[test]
    fn pow10_i128_matches_runtime() {
        for n in 0u8..=38 {
            assert_eq!(POW10_I128[n as usize], 10i128.pow(n as u32));
        }
    }

    #[test]
    fn max_exp_constants_align_with_arrays() {
        assert_eq!(POW10_I64.len(), POW10_I64_MAX_EXP as usize + 1);
        assert_eq!(POW10_I128.len(), POW10_I128_MAX_EXP as usize + 1);
    }
}
