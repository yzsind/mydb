//! Fixed-point decimal kernel — the single source of truth for MySQL-style
//! decimal rounding, rescaling, precision clamping and lenient numeric
//! string parsing. Shared by the expression evaluator (`ROUND` / `TRUNCATE`
//! / `CAST AS DECIMAL` / arithmetic), the row codec (column-scale coercion
//! on write) and aggregate finalisation (`AVG`), so every path rounds the
//! same way: half away from zero, matching MySQL's exact-value semantics.

use std::borrow::Cow;

use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};

/// MySQL's maximum DECIMAL scale.
pub const MAX_DECIMAL_SCALE: u8 = 30;
/// MySQL's maximum DECIMAL precision.
pub const MAX_DECIMAL_PRECISION: u8 = 65;
/// Maximum digits held by MySQL's internal `my_decimal` scratch buffer.
///
/// This is not a column/result precision: persisted DECIMAL values remain
/// capped at 65 digits and scale 30.  A few expression kernels (notably MOD)
/// consume the full nine base-1e9 words before shaping their result, so the
/// source literal must remain exact up to 81 digits on that cold path.
pub const MAX_DECIMAL_INTERMEDIATE_PRECISION: u8 = 81;
/// Maximum fractional digits carried in that scratch representation.  One
/// base-1e9 word is reserved for the integer part (including a leading zero),
/// leaving eight words / 72 fractional digits; metadata can still expose the
/// original scale up to 81 and pads the discarded tail with zeroes.
pub const MAX_DECIMAL_INTERMEDIATE_SCALE: u8 = 72;

/// Bound-time shape of a MySQL DECIMAL result. Keeping this beside the
/// arithmetic kernel prevents the planner, protocol metadata and executors
/// from growing subtly different copies of the precision rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DecimalResultSpec {
    pub precision: u8,
    pub scale: u8,
}

impl DecimalResultSpec {
    #[inline]
    pub const fn new(precision: u8, scale: u8) -> Self {
        let precision = if precision == 0 {
            1
        } else if precision > MAX_DECIMAL_PRECISION {
            MAX_DECIMAL_PRECISION
        } else {
            precision
        };
        let scale = if scale > MAX_DECIMAL_SCALE {
            MAX_DECIMAL_SCALE
        } else if scale > precision {
            precision
        } else {
            scale
        };
        Self { precision, scale }
    }

    pub const fn add_sub(left: Self, right: Self) -> Self {
        let scale = if left.scale > right.scale {
            left.scale
        } else {
            right.scale
        };
        let left_int = left.precision.saturating_sub(left.scale);
        let right_int = right.precision.saturating_sub(right.scale);
        let int_digits = if left_int > right_int {
            left_int
        } else {
            right_int
        };
        Self::new(int_digits.saturating_add(scale).saturating_add(1), scale)
    }

    pub const fn multiply(left: Self, right: Self) -> Self {
        Self::new(
            left.precision.saturating_add(right.precision),
            left.scale.saturating_add(right.scale),
        )
    }

    pub const fn divide(left: Self, right: Self, increment: u8) -> Self {
        Self::new(
            left.precision
                .saturating_add(right.scale)
                .saturating_add(increment),
            left.scale.saturating_add(increment),
        )
    }

    pub const fn modulo(left: Self, right: Self) -> Self {
        let scale = if left.scale > right.scale {
            left.scale
        } else {
            right.scale
        };
        let left_int = left.precision.saturating_sub(left.scale);
        let right_int = right.precision.saturating_sub(right.scale);
        let int_digits = if left_int < right_int {
            left_int
        } else {
            right_int
        };
        Self::new(int_digits.saturating_add(scale), scale)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalRoundingMode {
    HalfAwayFromZero,
    Truncate,
}

/// Execution-time decimal policy. `strict`/`dml` are deliberately plain
/// semantic flags rather than an engine `SessionContext` dependency: callers
/// use them when translating a kernel overflow/truncation into the shared
/// `dml_violation` warning/error policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalContext {
    pub result: Option<DecimalResultSpec>,
    pub div_precision_increment: u8,
    pub rounding: DecimalRoundingMode,
    pub strict: bool,
    pub dml: bool,
}

impl Default for DecimalContext {
    fn default() -> Self {
        Self {
            result: None,
            div_precision_increment: 4,
            rounding: DecimalRoundingMode::HalfAwayFromZero,
            strict: false,
            dml: false,
        }
    }
}

/// Borrowed, allocation-free input to the exact kernel.
#[derive(Debug, Clone, Copy)]
pub enum ExactDecimalRef<'a> {
    I64(i64, u8),
    I128(i128, u8),
    Big(&'a BigInt, u8),
}

impl ExactDecimalRef<'_> {
    #[inline]
    pub fn scale(self) -> u8 {
        match self {
            Self::I64(_, scale) | Self::I128(_, scale) | Self::Big(_, scale) => scale,
        }
    }

    #[inline]
    fn into_big(self) -> Cow<'static, BigInt> {
        match self {
            Self::I64(value, _) => Cow::Owned(BigInt::from(value)),
            Self::I128(value, _) => Cow::Owned(BigInt::from(value)),
            Self::Big(value, _) => Cow::Owned(value.clone()),
        }
    }
}

/// Owned exact result, demoted after every operation to keep common values
/// allocation-free.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExactDecimal {
    I64(i64, u8),
    I128(i128, u8),
    Big(BigInt, u8),
}

impl ExactDecimal {
    #[inline]
    pub fn from_i128(value: i128, scale: u8) -> Self {
        i64::try_from(value)
            .map(|value| Self::I64(value, scale))
            .unwrap_or(Self::I128(value, scale))
    }

    #[inline]
    pub fn from_big(value: BigInt, scale: u8) -> Self {
        if let Some(value) = value.to_i64() {
            Self::I64(value, scale)
        } else if let Some(value) = value.to_i128() {
            Self::I128(value, scale)
        } else {
            Self::Big(value, scale)
        }
    }

    #[inline]
    pub fn scale(&self) -> u8 {
        match self {
            Self::I64(_, scale) | Self::I128(_, scale) | Self::Big(_, scale) => *scale,
        }
    }

    #[inline]
    pub fn as_ref(&self) -> ExactDecimalRef<'_> {
        match self {
            Self::I64(value, scale) => ExactDecimalRef::I64(*value, *scale),
            Self::I128(value, scale) => ExactDecimalRef::I128(*value, *scale),
            Self::Big(value, scale) => ExactDecimalRef::Big(value, *scale),
        }
    }

    pub fn into_big_parts(self) -> (BigInt, u8) {
        match self {
            Self::I64(value, scale) => (BigInt::from(value), scale),
            Self::I128(value, scale) => (BigInt::from(value), scale),
            Self::Big(value, scale) => (value, scale),
        }
    }

    pub fn format(&self) -> String {
        match self {
            Self::I64(value, scale) => format_decimal_parts(i128::from(*value), *scale),
            Self::I128(value, scale) => format_decimal_parts(*value, *scale),
            Self::Big(value, scale) => format_big_decimal_parts(value, *scale),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

/// Exact binary arithmetic with checked fixed-width fast paths and BigInt
/// promotion. `None` denotes division by zero; f64 is never involved.
pub fn exact_binary(
    left: ExactDecimalRef<'_>,
    op: DecimalBinaryOp,
    right: ExactDecimalRef<'_>,
    context: DecimalContext,
) -> Result<Option<ExactDecimal>, &'static str> {
    let left_scale = left.scale();
    let right_scale = right.scale();
    if matches!(op, DecimalBinaryOp::Div | DecimalBinaryOp::Mod) && decimal_ref_is_zero(right) {
        return Ok(None);
    }

    if !matches!(left, ExactDecimalRef::Big(..))
        && !matches!(right, ExactDecimalRef::Big(..))
        && !matches!(op, DecimalBinaryOp::Div)
    {
        let l = match left {
            ExactDecimalRef::I64(v, _) => i128::from(v),
            ExactDecimalRef::I128(v, _) => v,
            ExactDecimalRef::Big(..) => unreachable!(),
        };
        let r = match right {
            ExactDecimalRef::I64(v, _) => i128::from(v),
            ExactDecimalRef::I128(v, _) => v,
            ExactDecimalRef::Big(..) => unreachable!(),
        };
        let small = match op {
            DecimalBinaryOp::Add | DecimalBinaryOp::Sub | DecimalBinaryOp::Mod => {
                let scale = left_scale.max(right_scale);
                match (
                    rescale_round_i128(l, left_scale, scale),
                    rescale_round_i128(r, right_scale, scale),
                ) {
                    (Some(l), Some(r)) => match op {
                        DecimalBinaryOp::Add => l.checked_add(r).map(|v| (v, scale)),
                        DecimalBinaryOp::Sub => l.checked_sub(r).map(|v| (v, scale)),
                        DecimalBinaryOp::Mod => l.checked_rem(r).map(|v| (v, scale)),
                        _ => unreachable!(),
                    },
                    _ => None,
                }
            }
            DecimalBinaryOp::Mul => l
                .checked_mul(r)
                .map(|v| (v, left_scale.saturating_add(right_scale))),
            DecimalBinaryOp::Div => unreachable!(),
        };
        if let Some((value, scale)) = small {
            return shape_exact(ExactDecimal::from_i128(value, scale), context).map(Some);
        }
    }

    // Exact division used to promote every pair to BigInt, even for common
    // AVG(INT) states whose scaled numerator and divisor both fit in i128.
    // Keep that lane allocation-free when the requested power-of-ten scaling
    // and rounded quotient are representable; any overflow falls through to
    // the existing arbitrary-precision implementation.
    if matches!(op, DecimalBinaryOp::Div)
        && !matches!(left, ExactDecimalRef::Big(..))
        && !matches!(right, ExactDecimalRef::Big(..))
    {
        let left_value = match left {
            ExactDecimalRef::I64(value, _) => i128::from(value),
            ExactDecimalRef::I128(value, _) => value,
            ExactDecimalRef::Big(..) => unreachable!(),
        };
        let right_value = match right {
            ExactDecimalRef::I64(value, _) => i128::from(value),
            ExactDecimalRef::I128(value, _) => value,
            ExactDecimalRef::Big(..) => unreachable!(),
        };
        let result_scale = context.result.map(|spec| spec.scale).unwrap_or_else(|| {
            left_scale
                .saturating_add(context.div_precision_increment)
                .min(MAX_DECIMAL_SCALE)
        });
        let exponent = result_scale as i32 + right_scale as i32 - left_scale as i32;
        let scaled = if exponent >= 0 {
            pow10_i128(exponent as u32)
                .and_then(|factor| left_value.checked_mul(factor))
                .map(|numerator| (numerator, right_value))
        } else {
            pow10_i128((-exponent) as u32)
                .and_then(|factor| right_value.checked_mul(factor))
                .map(|denominator| (left_value, denominator))
        };
        if let Some((numerator, denominator)) = scaled {
            let quotient = match context.rounding {
                DecimalRoundingMode::Truncate => numerator.checked_div(denominator),
                DecimalRoundingMode::HalfAwayFromZero => {
                    div_round_i128_checked(numerator, denominator)
                }
            };
            if let Some(quotient) = quotient {
                return shape_exact(ExactDecimal::from_i128(quotient, result_scale), context)
                    .map(Some);
            }
        }
    }

    let mut left_value = left.into_big().into_owned();
    let mut right_value = right.into_big().into_owned();
    let (value, scale) = match op {
        DecimalBinaryOp::Add | DecimalBinaryOp::Sub | DecimalBinaryOp::Mod => {
            let scale = left_scale.max(right_scale);
            if left_scale < scale {
                left_value *= pow10_big((scale - left_scale) as u32);
            }
            if right_scale < scale {
                right_value *= pow10_big((scale - right_scale) as u32);
            }
            let value = match op {
                DecimalBinaryOp::Add => left_value + right_value,
                DecimalBinaryOp::Sub => left_value - right_value,
                DecimalBinaryOp::Mod => left_value % right_value,
                _ => unreachable!(),
            };
            (value, scale)
        }
        DecimalBinaryOp::Mul => (
            left_value * right_value,
            left_scale.saturating_add(right_scale),
        ),
        DecimalBinaryOp::Div => {
            let result_scale = context.result.map(|spec| spec.scale).unwrap_or_else(|| {
                left_scale
                    .saturating_add(context.div_precision_increment)
                    .min(MAX_DECIMAL_SCALE)
            });
            let exponent = result_scale as i32 + right_scale as i32 - left_scale as i32;
            if exponent >= 0 {
                left_value *= pow10_big(exponent as u32);
            } else {
                right_value *= pow10_big((-exponent) as u32);
            }
            let quotient = if matches!(context.rounding, DecimalRoundingMode::Truncate) {
                left_value / right_value
            } else {
                div_round_big(left_value, right_value)
            };
            (quotient, result_scale)
        }
    };
    shape_exact(ExactDecimal::from_big(value, scale), context).map(Some)
}

fn decimal_ref_is_zero(value: ExactDecimalRef<'_>) -> bool {
    match value {
        ExactDecimalRef::I64(value, _) => value == 0,
        ExactDecimalRef::I128(value, _) => value == 0,
        ExactDecimalRef::Big(value, _) => value.is_zero(),
    }
}

#[inline]
fn pow10_big(exp: u32) -> BigInt {
    BigInt::from(10u8).pow(exp)
}

fn div_round_big(numer: BigInt, denom: BigInt) -> BigInt {
    let quotient = &numer / &denom;
    let remainder = &numer % &denom;
    if remainder.is_zero() || remainder.abs() * 2 < denom.abs() {
        quotient
    } else if remainder.is_negative() != denom.is_negative() {
        quotient - 1
    } else {
        quotient + 1
    }
}

#[inline]
fn div_round_i128_checked(numer: i128, denom: i128) -> Option<i128> {
    let quotient = numer.checked_div(denom)?;
    let remainder = numer.checked_rem(denom)?;
    if remainder == 0 {
        return Some(quotient);
    }
    let denominator = denom.unsigned_abs();
    let round_threshold = denominator / 2 + denominator % 2;
    if remainder.unsigned_abs() < round_threshold {
        Some(quotient)
    } else if (remainder < 0) != (denom < 0) {
        quotient.checked_sub(1)
    } else {
        quotient.checked_add(1)
    }
}

pub fn rescale_exact(value: ExactDecimal, to_scale: u8, round: bool) -> ExactDecimal {
    let from_scale = value.scale();
    if from_scale == to_scale {
        return value;
    }
    let (mut magnitude, _) = value.into_big_parts();
    if from_scale < to_scale {
        magnitude *= pow10_big((to_scale - from_scale) as u32);
    } else {
        let divisor = pow10_big((from_scale - to_scale) as u32);
        magnitude = if round {
            div_round_big(magnitude, divisor)
        } else {
            magnitude / divisor
        };
    }
    ExactDecimal::from_big(magnitude, to_scale)
}

fn shape_exact(
    mut value: ExactDecimal,
    context: DecimalContext,
) -> Result<ExactDecimal, &'static str> {
    let spec = context
        .result
        .unwrap_or_else(|| DecimalResultSpec::new(MAX_DECIMAL_PRECISION, value.scale()));
    if value.scale() != spec.scale {
        value = rescale_exact(
            value,
            spec.scale,
            matches!(context.rounding, DecimalRoundingMode::HalfAwayFromZero),
        );
    }
    let fixed_width_digits = match &value {
        ExactDecimal::I64(magnitude, _) => Some(decimal_digits(magnitude.unsigned_abs() as u128)),
        ExactDecimal::I128(magnitude, _) => Some(decimal_digits(magnitude.unsigned_abs())),
        ExactDecimal::Big(..) => None,
    };
    if let Some(digits) = fixed_width_digits {
        return if digits <= spec.precision as usize {
            Ok(value)
        } else {
            Err("DECIMAL value is out of range")
        };
    }
    let (magnitude, scale) = value.into_big_parts();
    if magnitude.abs().to_str_radix(10).len() > spec.precision as usize {
        return Err("DECIMAL value is out of range");
    }
    Ok(ExactDecimal::from_big(magnitude, scale))
}

#[inline]
fn decimal_digits(magnitude: u128) -> usize {
    if magnitude == 0 {
        1
    } else {
        magnitude.ilog10() as usize + 1
    }
}

/// `10^exp` as i128, `None` past the i128 range (exp > 38).
///
/// Table lookup rather than `checked_pow` — the runtime version is a
/// non-inlined repeated-squaring helper that shows up at ~3.2% self time
/// in TPC-H Q1's decimal scale-shift path (`rescale_round_i128` is the
/// hot caller). One indexed load + a bounds check collapses the whole
/// function to a couple of instructions.
#[inline]
pub fn pow10_i128(exp: u32) -> Option<i128> {
    crate::util::decimal_pow::POW10_I128
        .get(exp as usize)
        .copied()
}

/// Round `numer / denom` to the nearest integer, ties away from zero —
/// MySQL's exact-value rounding mode. `denom` must be non-zero.
#[inline]
pub fn div_round_i128(numer: i128, denom: i128) -> i128 {
    let q = numer / denom;
    let r = numer % denom;
    if r == 0 {
        return q;
    }
    if r.unsigned_abs() * 2 >= denom.unsigned_abs() {
        if (numer < 0) ^ (denom < 0) {
            q - 1
        } else {
            q + 1
        }
    } else {
        q
    }
}

/// Rescale a `(magnitude, scale)` fixed-point value to `to_scale`.
/// Downscaling rounds half away from zero; upscaling returns `None` on
/// i128 overflow. A downscale whose delta exceeds the i128 range
/// (`> 38`) collapses to `Some(0)` — every bit is mathematically past
/// the rounding cutoff, matching [`round_exact_parts`]'s `drop > 38`
/// early-out. Prior to that alignment this branch returned `None` and
/// callers such as `generate_series` translated it into a spurious 1210
/// "argument exceeds numeric range".
#[inline]
pub fn rescale_round_i128(mag: i128, from_scale: u8, to_scale: u8) -> Option<i128> {
    use std::cmp::Ordering;
    match from_scale.cmp(&to_scale) {
        Ordering::Equal => Some(mag),
        Ordering::Less => mag.checked_mul(pow10_i128((to_scale - from_scale) as u32)?),
        Ordering::Greater => match pow10_i128((from_scale - to_scale) as u32) {
            Some(shift) => Some(div_round_i128(mag, shift)),
            None => Some(0),
        },
    }
}

/// Rescale like [`rescale_round_i128`] but truncating toward zero on
/// downscale (MySQL `TRUNCATE()` semantics). A downscale delta > 38
/// collapses to `Some(0)` for the same reason as
/// [`rescale_round_i128`].
#[inline]
pub fn rescale_trunc_i128(mag: i128, from_scale: u8, to_scale: u8) -> Option<i128> {
    use std::cmp::Ordering;
    match from_scale.cmp(&to_scale) {
        Ordering::Equal => Some(mag),
        Ordering::Less => mag.checked_mul(pow10_i128((to_scale - from_scale) as u32)?),
        Ordering::Greater => match pow10_i128((from_scale - to_scale) as u32) {
            Some(shift) => Some(mag / shift),
            None => Some(0),
        },
    }
}

/// MySQL `ROUND(x, d)` over exact decimal parts. Returns the rounded
/// `(magnitude, scale)`; the result scale is `clamp(d, 0, scale)` — a `d`
/// beyond the stored scale is a no-op, a negative `d` zeroes digits left
/// of the decimal point and yields scale 0. `None` on (extremely rare)
/// i128 overflow from the negative-`d` shift-back.
pub fn round_exact_parts(mag: i128, scale: u8, d: i32) -> Option<(i128, u8)> {
    rounded_parts(mag, scale, d, true)
}

/// MySQL `TRUNCATE(x, d)` over exact decimal parts: like
/// [`round_exact_parts`] but truncating toward zero.
pub fn truncate_exact_parts(mag: i128, scale: u8, d: i32) -> Option<(i128, u8)> {
    rounded_parts(mag, scale, d, false)
}

/// Width-independent ROUND/TRUNCATE kernel. Fixed-width coefficients stay on
/// the allocation-free i128 path; only values that already require the wide
/// lane (or whose rounded result overflows i128) allocate a `BigInt`.
pub fn round_exact(value: ExactDecimal, d: i32, round: bool) -> ExactDecimal {
    let scale = value.scale();
    if d >= scale as i32 {
        return value;
    }

    match &value {
        ExactDecimal::I64(magnitude, _) => {
            if let Some((magnitude, scale)) = rounded_parts(i128::from(*magnitude), scale, d, round)
            {
                return ExactDecimal::from_i128(magnitude, scale);
            }
        }
        ExactDecimal::I128(magnitude, _) => {
            if let Some((magnitude, scale)) = rounded_parts(*magnitude, scale, d, round) {
                return ExactDecimal::from_i128(magnitude, scale);
            }
        }
        ExactDecimal::Big(..) => {}
    }

    let (magnitude, scale) = value.into_big_parts();
    let drop = i64::from(scale) - i64::from(d);
    debug_assert!(drop > 0);
    let digits = magnitude.abs().to_str_radix(10).len() as i64;
    if drop > digits {
        return ExactDecimal::I64(0, 0);
    }
    let divisor = pow10_big(drop as u32);
    let mut rounded = if round {
        div_round_big(magnitude, divisor)
    } else {
        magnitude / divisor
    };
    let result_scale = if d < 0 {
        rounded *= pow10_big(d.unsigned_abs());
        0
    } else {
        d as u8
    };
    ExactDecimal::from_big(rounded, result_scale)
}

fn rounded_parts(mag: i128, scale: u8, d: i32, round: bool) -> Option<(i128, u8)> {
    if d >= scale as i32 {
        // Already at (or below) the requested precision: value unchanged.
        // MySQL keeps the result scale at min(d, scale) = scale.
        return Some((mag, scale));
    }
    // Digits to drop, counted from the stored scale down to `d` (which may
    // be negative: ROUND(123.45, -1) drops 3 digits, then shifts back).
    // Compute in i64: `d` can be as low as i32::MIN (e.g. TRUNCATE(x, 2^31)
    // wraps to i32::MIN before reaching here), so `scale - d` overflows i32.
    let drop = scale as i64 - d as i64; // > 0 here, since d < scale
                                        // A drop past the i128 digit range (~39 digits) means the rounding
                                        // position sits far left of every stored digit: any finite value rounds
                                        // or truncates to a multiple of 10^|d| that is 0. MySQL agrees —
                                        // ROUND(1.5, -2147483649) = 0, TRUNCATE(1.5, -2147483649) = 0.
    if drop > 38 {
        return Some((0, 0));
    }
    let shift = pow10_i128(drop as u32)?;
    let mut out = if round {
        div_round_i128(mag, shift)
    } else {
        mag / shift
    };
    if d < 0 {
        // `-d` cannot overflow: the `drop > 38` guard above bounds
        // `d >= scale - 38 >= -38`, so `-d <= 38`.
        out = out.checked_mul(pow10_i128((-d) as u32)?)?;
        Some((out, 0))
    } else {
        Some((out, d as u8))
    }
}

/// Clamp a magnitude into `DECIMAL(precision, …)` range: at most
/// `precision` total digits, so |mag| ≤ 10^precision − 1 (the column scale
/// does not change the bound — magnitude digits = precision). Returns the
/// (possibly saturated) magnitude and whether clamping occurred — MySQL
/// saturates to ±99…9.9…9 and warns 1264.
pub fn clamp_to_precision(mag: i128, precision: u8) -> (i128, bool) {
    let precision = precision.clamp(1, MAX_DECIMAL_PRECISION) as u32;
    let max = match pow10_i128(precision) {
        Some(p) => p - 1,
        // precision 65 still fits comfortably? 10^65 > i128::MAX (~1.7e38),
        // so any i128 magnitude is in range past 38 digits.
        None => return (mag, false),
    };
    if mag > max {
        (max, true)
    } else if mag < -max {
        (-max, true)
    } else {
        (mag, false)
    }
}

/// Clamp an arbitrarily wide fixed-point magnitude into MySQL's declared
/// DECIMAL precision. This is the wide counterpart of
/// [`clamp_to_precision`] and deliberately returns a tiered result so a
/// DECIMAL(39..65) CAST never narrows through i128.
pub fn clamp_exact_to_precision(value: ExactDecimal, precision: u8) -> (ExactDecimal, bool) {
    let precision = precision.clamp(1, MAX_DECIMAL_PRECISION);
    match value {
        ExactDecimal::I64(magnitude, scale) => {
            let (magnitude, clamped) = clamp_to_precision(i128::from(magnitude), precision);
            (ExactDecimal::from_i128(magnitude, scale), clamped)
        }
        ExactDecimal::I128(magnitude, scale) => {
            let (magnitude, clamped) = clamp_to_precision(magnitude, precision);
            (ExactDecimal::from_i128(magnitude, scale), clamped)
        }
        ExactDecimal::Big(magnitude, scale) => {
            let max = pow10_big(u32::from(precision)) - 1u8;
            if magnitude > max {
                (ExactDecimal::from_big(max, scale), true)
            } else if magnitude < -&max {
                (ExactDecimal::from_big(-max, scale), true)
            } else {
                (ExactDecimal::from_big(magnitude, scale), false)
            }
        }
    }
}

/// Format `(magnitude, scale)` as the canonical decimal string MySQL
/// prints (`-12.3400` keeps trailing zeros to the stored scale).
pub fn format_decimal_parts(magnitude: i128, scale: u8) -> String {
    format_decimal_magnitude(magnitude.unsigned_abs(), scale, magnitude < 0)
}

/// Format an unsigned magnitude with an explicit sign, including a codec's
/// negative zero. Only the final string allocates; all u8 scales are valid.
pub(crate) fn format_decimal_magnitude(magnitude: u128, scale: u8, negative: bool) -> String {
    let mut buffer = itoa::Buffer::new();
    let digits = buffer.format(magnitude);
    let scale = usize::from(scale);
    let mut out = String::with_capacity(
        digits.len().max(scale + 1) + usize::from(scale != 0) + usize::from(negative),
    );
    if negative {
        out.push('-');
    }
    if scale == 0 {
        out.push_str(digits);
    } else if digits.len() > scale {
        let split = digits.len() - scale;
        out.push_str(&digits[..split]);
        out.push('.');
        out.push_str(&digits[split..]);
    } else {
        out.push_str("0.");
        for _ in digits.len()..scale {
            out.push('0');
        }
        out.push_str(digits);
    }
    out
}

/// Strict full-string parse of `[+-]?digits[.digits]([eE][+-]?digits)?`
/// into exact `(magnitude, scale)` parts. `None` on any other shape
/// (trailing garbage, empty) — callers wanting MySQL's lenient prefix
/// semantics use [`parse_lenient_f64`] instead.
///
/// Scientific notation is folded exactly: `"1e30"` → `(10^30, 0)` when
/// the exponent leaves the magnitude within i128, `"1.5e-3"` →
/// `(15, 4)`. If the resulting scale exceeds `u8::MAX` or the magnitude
/// overflows i128, this returns `None` and the caller (e.g. CAST AS
/// DECIMAL) falls back through the f64 path.
pub fn parse_exact_decimal_str(s: &str) -> Option<(i128, u8)> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (negative, body) = match s.as_bytes().first().copied() {
        Some(b'+') => (false, &s[1..]),
        Some(b'-') => (true, &s[1..]),
        _ => (false, s),
    };
    if body.is_empty() {
        return None;
    }
    // Split off the optional `[eE][+-]?digits` exponent first so the
    // mantissa parser can stay strict.
    let (mantissa, exp) = match body.find(|c: char| c == 'e' || c == 'E') {
        Some(idx) => {
            let (m, e_part) = body.split_at(idx);
            let e_str = &e_part[1..];
            if e_str.is_empty() {
                return None;
            }
            let exp: i64 = e_str.parse().ok()?;
            (m, exp)
        }
        None => (body, 0i64),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((int_part, frac_part)) => (int_part, frac_part),
        None => (mantissa, ""),
    };
    if (int_part.is_empty() && frac_part.is_empty())
        || !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let mantissa_scale = frac_part.len() as i64;
    let mut magnitude: i128 = 0;
    for b in int_part.bytes().chain(frac_part.bytes()) {
        magnitude = magnitude.checked_mul(10)?.checked_add((b - b'0') as i128)?;
    }
    // effective scale = mantissa_scale - exp:
    //   "1.5e2"  → 1 - 2 = -1  → multiply magnitude by 10, scale = 0
    //   "1.5e-3" → 1 - -3 = 4  → keep magnitude, scale = 4
    let effective_scale = mantissa_scale.checked_sub(exp)?;
    let (final_mag, final_scale) = if effective_scale >= 0 {
        (magnitude, effective_scale)
    } else {
        // Do not truncate a large i64 exponent through `as u32`: e.g.
        // 1e4294967297 used to wrap the shift to 1 and silently become 10.
        let extra = u32::try_from(effective_scale.unsigned_abs()).ok()?;
        (magnitude.checked_mul(pow10_i128(extra)?)?, 0)
    };
    let scale = u8::try_from(final_scale).ok()?;
    Some((if negative { -final_mag } else { final_mag }, scale))
}

/// Full-precision twin of [`parse_exact_decimal_str`].
///
/// The i128 parser remains the allocation-free hot path. This routine is
/// entered only after that path cannot represent the value and supports the
/// complete MySQL DECIMAL(65) range (plus wider intermediate values so the
/// caller can report/clamp them with the correct context).
pub fn parse_exact_decimal_big(s: &str) -> Option<(num_bigint::BigInt, u8)> {
    use num_bigint::BigInt;
    use num_traits::Zero;

    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (negative, body) = match s.as_bytes().first().copied() {
        Some(b'+') => (false, &s[1..]),
        Some(b'-') => (true, &s[1..]),
        _ => (false, s),
    };
    if body.is_empty() {
        return None;
    }
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(index) => {
            let exponent = body[index + 1..].parse::<i64>().ok()?;
            (&body[..index], exponent)
        }
        None => (body, 0),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if (integer.is_empty() && fraction.is_empty())
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let mut digits = String::with_capacity(integer.len() + fraction.len());
    digits.push_str(integer);
    digits.push_str(fraction);
    if digits.is_empty() {
        digits.push('0');
    }
    let mut magnitude = BigInt::parse_bytes(digits.as_bytes(), 10)?;
    let effective_scale = i64::try_from(fraction.len()).ok()?.checked_sub(exponent)?;
    let scale = if effective_scale < 0 {
        if magnitude.is_zero() {
            return Some((magnitude, 0));
        }
        let shift = usize::try_from(-effective_scale).ok()?;
        // Do not materialize an attacker-controlled scientific exponent as
        // a multi-gigabyte BigInt. MySQL DECIMAL stores at most 65 digits;
        // twice that bound covers exact binary-operation intermediates before
        // the shared overflow policy rejects or clamps the result.
        let significant_digits = digits.trim_start_matches('0').len().max(1);
        if significant_digits.saturating_add(shift) > MAX_DECIMAL_PRECISION as usize * 2 {
            return None;
        }
        let shift = u32::try_from(shift).ok()?;
        magnitude *= BigInt::from(10u8).pow(shift);
        0
    } else {
        u8::try_from(effective_scale).ok()?
    };
    if negative {
        magnitude = -magnitude;
    }
    Some((magnitude, scale))
}

/// Rescale exact decimal text to a fixed number of fractional digits.
///
/// Downscaling rounds half away from zero, matching DECIMAL assignment and
/// CAST. The returned string always contains exactly `target_scale`
/// fractional digits. This is the cold path used only when the magnitude does
/// not fit i128; ordinary OLTP decimals continue through
/// [`rescale_round_i128`].
pub fn rescale_decimal_big_str(s: &str, target_scale: u8) -> Option<String> {
    use num_bigint::BigInt;
    use num_traits::{Signed, Zero};

    let (mut magnitude, source_scale) = parse_exact_decimal_big(s)?;
    match source_scale.cmp(&target_scale) {
        std::cmp::Ordering::Less => {
            magnitude *= BigInt::from(10u8).pow((target_scale - source_scale) as u32);
        }
        std::cmp::Ordering::Greater => {
            let divisor = BigInt::from(10u8).pow((source_scale - target_scale) as u32);
            let quotient = &magnitude / &divisor;
            let remainder = &magnitude % &divisor;
            magnitude = quotient;
            if !remainder.is_zero() && remainder.abs() * 2 >= divisor {
                magnitude += if remainder.is_negative() { -1 } else { 1 };
            }
        }
        std::cmp::Ordering::Equal => {}
    }
    Some(format_big_decimal_parts(&magnitude, target_scale))
}

/// Canonical fixed-scale formatter for a large signed magnitude.
pub fn format_big_decimal_parts(magnitude: &num_bigint::BigInt, scale: u8) -> String {
    use num_traits::{Signed, Zero};

    let negative = magnitude.is_negative();
    let digits = magnitude.abs().to_str_radix(10);
    let body = if scale == 0 {
        digits
    } else {
        let scale = scale as usize;
        if digits.len() <= scale {
            format!("0.{digits:0>scale$}")
        } else {
            let split = digits.len() - scale;
            format!("{}.{}", &digits[..split], &digits[split..])
        }
    };
    if negative && !magnitude.is_zero() {
        format!("-{body}")
    } else {
        body
    }
}

/// Canonicalize an exact decimal without imposing the `i128` arithmetic
/// limit. This representation is intended for comparisons and metadata such
/// as Zone Maps, where MySQL DECIMAL(65) must remain lossless.
pub fn canonical_decimal_str(s: &str) -> Option<String> {
    let s = s.trim();
    let (negative, body) = match s.as_bytes().first().copied() {
        Some(b'+') => (false, &s[1..]),
        Some(b'-') => (true, &s[1..]),
        _ => (false, s),
    };
    let (mantissa, exponent) = match body.find(|c: char| c == 'e' || c == 'E') {
        Some(index) => {
            let exponent = body[index + 1..].parse::<i32>().ok()?;
            (&body[..index], exponent)
        }
        None => (body, 0),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if (integer.is_empty() && fraction.is_empty())
        || !integer.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let mut digits = format!("{integer}{fraction}");
    let mut scale = i32::try_from(fraction.len()).ok()?.checked_sub(exponent)?;
    while digits.starts_with('0') && digits.len() > 1 {
        digits.remove(0);
    }
    if scale < 0 {
        let extra = usize::try_from(-scale).ok()?;
        if digits.len().saturating_add(extra) > MAX_DECIMAL_PRECISION as usize {
            return None;
        }
        digits.push_str(&"0".repeat(extra));
        scale = 0;
    }
    if scale > MAX_DECIMAL_SCALE as i32
        || digits.len().max(scale.max(0) as usize) > MAX_DECIMAL_PRECISION as usize
    {
        return None;
    }
    let mut out = if scale == 0 {
        digits
    } else {
        let scale = scale as usize;
        if digits.len() <= scale {
            format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
        } else {
            let split = digits.len() - scale;
            format!("{}.{}", &digits[..split], &digits[split..])
        }
    };
    if let Some(dot) = out.find('.') {
        while out.ends_with('0') {
            out.pop();
        }
        if out.len() == dot + 1 {
            out.pop();
        }
    }
    let nonzero = out
        .bytes()
        .any(|byte| byte.is_ascii_digit() && byte != b'0');
    if negative && nonzero {
        out.insert(0, '-');
    }
    Some(out)
}

/// Encode an exact MySQL DECIMAL into a compact, portable BCD key.
///
/// Layout: `[flags:1][scale:1][digits:1][packed digits...]`, where bit 0 of
/// `flags` is the sign and two decimal digits occupy each following byte
/// (high nibble first; the unused low nibble for an odd digit count is 0xF).
/// The input is canonicalized first, so equivalent textual spellings have a
/// deterministic byte representation. This is intentionally independent of
/// the heap DECIMAL codec: metadata such as Zone Maps must cover DECIMAL(65),
/// beyond the i128 fast path used by most arithmetic.
pub fn encode_decimal_bcd(s: &str, out: &mut Vec<u8>) -> Option<()> {
    let canonical = canonical_decimal_str(s)?;
    let negative = canonical.starts_with('-');
    let unsigned = canonical.strip_prefix('-').unwrap_or(&canonical);
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut digits = String::with_capacity(integer.len() + fraction.len());
    digits.push_str(integer);
    digits.push_str(fraction);
    while digits.starts_with('0') && digits.len() > 1 {
        digits.remove(0);
    }
    let digit_count = u8::try_from(digits.len()).ok()?;
    let scale = u8::try_from(fraction.len()).ok()?;
    if digit_count == 0 || digit_count > MAX_DECIMAL_PRECISION || scale > MAX_DECIMAL_SCALE {
        return None;
    }
    out.push(u8::from(negative));
    out.push(scale);
    out.push(digit_count);
    let bytes = digits.as_bytes();
    for pair in bytes.chunks(2) {
        let high = pair[0].checked_sub(b'0')?;
        let low = if pair.len() == 2 {
            pair[1].checked_sub(b'0')?
        } else {
            0x0f
        };
        if high > 9 || (low > 9 && low != 0x0f) {
            return None;
        }
        out.push((high << 4) | low);
    }
    Some(())
}

/// Decode the exact inverse of [`encode_decimal_bcd`]. Returns the canonical
/// comparison string used by [`compare_decimal_str`].
pub fn decode_decimal_bcd(bytes: &[u8]) -> Option<(String, usize)> {
    let (&flags, rest) = bytes.split_first()?;
    let (&scale, rest) = rest.split_first()?;
    let (&digit_count, packed) = rest.split_first()?;
    if flags & !1 != 0
        || digit_count == 0
        || digit_count > MAX_DECIMAL_PRECISION
        || scale > MAX_DECIMAL_SCALE
    {
        return None;
    }
    let packed_len = (digit_count as usize).div_ceil(2);
    let packed = packed.get(..packed_len)?;
    let mut digits = String::with_capacity(digit_count as usize);
    for (index, byte) in packed.iter().copied().enumerate() {
        let high = byte >> 4;
        let low = byte & 0x0f;
        if high > 9 {
            return None;
        }
        digits.push(char::from(b'0' + high));
        if digits.len() < digit_count as usize {
            if low > 9 {
                return None;
            }
            digits.push(char::from(b'0' + low));
        } else if index + 1 == packed_len && low != 0x0f {
            return None;
        }
    }
    let scale = scale as usize;
    let mut value = if scale == 0 {
        digits
    } else if digits.len() <= scale {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if flags & 1 != 0 && value.bytes().any(|b| b.is_ascii_digit() && b != b'0') {
        value.insert(0, '-');
    }
    let canonical = canonical_decimal_str(&value)?;
    Some((canonical, 3 + packed_len))
}

/// Exact comparison for canonical or user-supplied decimal text, including
/// the full MySQL DECIMAL(65) range.
pub fn compare_decimal_str(a: &str, b: &str) -> Option<std::cmp::Ordering> {
    let a = canonical_decimal_str(a)?;
    let b = canonical_decimal_str(b)?;
    let a_negative = a.starts_with('-');
    let b_negative = b.starts_with('-');
    if a_negative != b_negative {
        return Some(if a_negative {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        });
    }
    let (ai, af) = decimal_text_parts(&a);
    let (bi, bf) = decimal_text_parts(&b);
    let magnitude = ai
        .len()
        .cmp(&bi.len())
        .then_with(|| ai.cmp(bi))
        .then_with(|| {
            let width = af.len().max(bf.len());
            (0..width)
                .map(|index| af.as_bytes().get(index).copied().unwrap_or(b'0'))
                .cmp((0..width).map(|index| bf.as_bytes().get(index).copied().unwrap_or(b'0')))
        });
    Some(if a_negative {
        magnitude.reverse()
    } else {
        magnitude
    })
}

fn decimal_text_parts(text: &str) -> (&str, &str) {
    let text = text.strip_prefix('-').unwrap_or(text);
    text.split_once('.').unwrap_or((text, ""))
}

/// Result of a lenient (MySQL implicit-conversion) numeric parse.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LenientNumeric {
    pub value: f64,
    /// `false` when trailing garbage was discarded or no digits were
    /// consumed from a non-empty string — the caller should raise MySQL
    /// warning 1292 ("Truncated incorrect ... value").
    pub clean: bool,
}

/// MySQL's string→number implicit conversion: skip leading/trailing
/// whitespace, parse the longest `[+-]?digits[.digits][eE[+-]digits]`
/// prefix as f64, ignore the rest. An empty (or all-whitespace) string is
/// 0 with no warning; a non-empty string with no usable prefix is 0 *with*
/// a warning; a partial prefix parses to its value with a warning.
pub fn parse_lenient_f64(s: &str) -> LenientNumeric {
    let trimmed = s.trim_matches(|c: char| c.is_ascii_whitespace());
    if trimmed.is_empty() {
        return LenientNumeric {
            value: 0.0,
            clean: true,
        };
    }
    let prefix = numeric_prefix(trimmed);
    if prefix.is_empty() {
        return LenientNumeric {
            value: 0.0,
            clean: false,
        };
    }
    let value: f64 = prefix.parse().unwrap_or_else(|_| {
        // Prefix grammar above is a strict subset of Rust's f64 grammar;
        // a parse failure can only be overflow-shaped. Saturate.
        if trimmed.starts_with('-') {
            -f64::MAX
        } else {
            f64::MAX
        }
    });
    let overflow = value.is_infinite();
    LenientNumeric {
        value: value.clamp(-f64::MAX, f64::MAX),
        clean: !overflow && prefix.len() == trimmed.len(),
    }
}

/// Borrow the complete numeric prefix without staging exact decimals through f64.
pub fn numeric_prefix(s: &str) -> &str {
    let trimmed = s.trim_matches(|c: char| c.is_ascii_whitespace());
    let bytes = trimmed.as_bytes();
    let mut i = 0usize;
    if matches!(bytes.first(), Some(b'+') | Some(b'-')) {
        i += 1;
    }
    let mut digits = 0usize;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
        digits += 1;
    }
    if i < bytes.len() && bytes[i] == b'.' {
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        // No mantissa digits at all ('abc', '+x', '.', '-.') → 0, warn.
        return "";
    }
    // Optional exponent — only consumed when it carries at least one digit
    // ('1e3' yes, '1e' / '1ex' no: the 'e' is trailing garbage).
    if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
        let mut j = i + 1;
        if j < bytes.len() && matches!(bytes[j], b'+' | b'-') {
            j += 1;
        }
        let exp_start = j;
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            j += 1;
        }
        if j > exp_start {
            i = j;
        }
    }
    &trimmed[..i]
}

/// Result of a lenient integer parse (CAST AS SIGNED/UNSIGNED on strings).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LenientInt {
    pub value: i128,
    /// Trailing garbage was discarded / no digits consumed.
    pub clean: bool,
}

/// MySQL's string→integer conversion: an optional sign plus a digit run.
/// `'3.7'` stops at the dot (→ 3, warn 1292); `'1e3'` stops at the 'e'.
/// Saturates past the i128 range (callers clamp to their target width).
pub fn parse_lenient_int(s: &str) -> LenientInt {
    let trimmed = s.trim_matches(|c: char| c.is_ascii_whitespace());
    if trimmed.is_empty() {
        return LenientInt {
            value: 0,
            clean: true,
        };
    }
    let bytes = trimmed.as_bytes();
    let mut i = 0usize;
    let negative = match bytes.first() {
        Some(b'-') => {
            i += 1;
            true
        }
        Some(b'+') => {
            i += 1;
            false
        }
        _ => false,
    };
    let mut value: i128 = 0;
    let mut digits = 0usize;
    let mut saturated = false;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        let d = (bytes[i] - b'0') as i128;
        value = match value.checked_mul(10).and_then(|v| v.checked_add(d)) {
            Some(v) => v,
            None => {
                saturated = true;
                i128::MAX
            }
        };
        digits += 1;
        i += 1;
    }
    if digits == 0 {
        return LenientInt {
            value: 0,
            clean: false,
        };
    }
    // Symmetric saturation: a hugely-negative input should reach
    // `i128::MIN` rather than the off-by-one `-i128::MAX = i128::MIN + 1`
    // that `-value` produced. Callers that saturate to i64::MIN happen to
    // still be right because they use `< i64::MIN as i128`, but any
    // consumer of raw `value` used to lose that terminal bit.
    let value = if negative {
        if saturated {
            i128::MIN
        } else {
            -value
        }
    } else {
        value
    };
    LenientInt {
        value,
        clean: i == bytes.len() && !saturated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn div_round_ties_away_from_zero() {
        assert_eq!(div_round_i128(25, 10), 3);
        assert_eq!(div_round_i128(-25, 10), -3);
        assert_eq!(div_round_i128(24, 10), 2);
        assert_eq!(div_round_i128(-24, 10), -2);
        assert_eq!(div_round_i128(26, 10), 3);
        assert_eq!(div_round_i128(20, 10), 2);
    }

    #[test]
    fn result_specs_follow_mysql_decimal_rules() {
        let left = DecimalResultSpec::new(10, 2);
        let right = DecimalResultSpec::new(12, 4);
        assert_eq!(
            DecimalResultSpec::add_sub(left, right),
            DecimalResultSpec::new(13, 4)
        );
        assert_eq!(
            DecimalResultSpec::multiply(left, right),
            DecimalResultSpec::new(22, 6)
        );
        assert_eq!(
            DecimalResultSpec::divide(left, right, 4),
            DecimalResultSpec::new(18, 6)
        );
        assert_eq!(
            DecimalResultSpec::modulo(left, right),
            DecimalResultSpec::new(12, 4)
        );
    }

    #[test]
    fn exact_binary_promotes_instead_of_falling_to_float() {
        let left = ExactDecimal::I128(i128::MAX, 0);
        let right = ExactDecimal::I64(1, 0);
        let result = exact_binary(
            left.as_ref(),
            DecimalBinaryOp::Add,
            right.as_ref(),
            DecimalContext::default(),
        )
        .unwrap()
        .unwrap();
        assert!(matches!(result, ExactDecimal::Big(..)));
        assert_eq!(result.format(), "170141183460469231731687303715884105728");
    }

    #[test]
    fn exact_binary_covers_decimal_65_and_rounds_division() {
        let (magnitude, scale) = parse_exact_decimal_big(
            "99999999999999999999999999999999999.999999999999999999999999999999",
        )
        .unwrap();
        let value = ExactDecimal::from_big(magnitude, scale);
        let zero = ExactDecimal::I64(0, scale);
        let same = exact_binary(
            value.as_ref(),
            DecimalBinaryOp::Add,
            zero.as_ref(),
            DecimalContext::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(same.format(), value.format());

        let one = ExactDecimal::I64(1, 0);
        let six = ExactDecimal::I64(6, 0);
        let quotient = exact_binary(
            one.as_ref(),
            DecimalBinaryOp::Div,
            six.as_ref(),
            DecimalContext::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(quotient.format(), "0.1667");
    }

    #[test]
    fn exact_division_keeps_fixed_width_values_out_of_bigint() {
        let context = DecimalContext {
            result: Some(DecimalResultSpec::new(65, 4)),
            ..Default::default()
        };
        for (numerator, denominator, expected) in [
            (1, 6, "0.1667"),
            (-1, 6, "-0.1667"),
            (1, -6, "-0.1667"),
            (-1, -6, "0.1667"),
            (25, 10, "2.5000"),
        ] {
            let left = ExactDecimal::I64(numerator, 0);
            let right = ExactDecimal::I64(denominator, 0);
            let result = exact_binary(left.as_ref(), DecimalBinaryOp::Div, right.as_ref(), context)
                .unwrap()
                .unwrap();
            assert!(
                !matches!(result, ExactDecimal::Big(..)),
                "small exact division unexpectedly promoted: {numerator}/{denominator}"
            );
            assert_eq!(result.format(), expected);
        }

        let truncated = exact_binary(
            ExactDecimal::I64(-1, 0).as_ref(),
            DecimalBinaryOp::Div,
            ExactDecimal::I64(6, 0).as_ref(),
            DecimalContext {
                result: Some(DecimalResultSpec::new(65, 4)),
                rounding: DecimalRoundingMode::Truncate,
                ..Default::default()
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(truncated.format(), "-0.1666");
    }

    #[test]
    fn exact_division_overflow_falls_back_to_bigint_without_semantic_change() {
        let result = exact_binary(
            ExactDecimal::I128(i128::MAX, 0).as_ref(),
            DecimalBinaryOp::Div,
            ExactDecimal::I64(1, 0).as_ref(),
            DecimalContext {
                result: Some(DecimalResultSpec::new(65, 4)),
                ..Default::default()
            },
        )
        .unwrap()
        .unwrap();
        assert!(matches!(result, ExactDecimal::Big(..)));
        assert_eq!(
            result.format(),
            "170141183460469231731687303715884105727.0000"
        );

        let min_over_minus_one = exact_binary(
            ExactDecimal::I128(i128::MIN, 0).as_ref(),
            DecimalBinaryOp::Div,
            ExactDecimal::I64(-1, 0).as_ref(),
            DecimalContext {
                result: Some(DecimalResultSpec::new(65, 0)),
                ..Default::default()
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            min_over_minus_one.format(),
            "170141183460469231731687303715884105728"
        );
    }

    #[test]
    fn exact_precision_clamp_keeps_fixed_width_values_out_of_bigint() {
        for (value, precision, expected, was_clamped) in [
            (ExactDecimal::I64(12_345, 2), 7, "123.45", false),
            (ExactDecimal::I64(12_345, 2), 3, "9.99", true),
            (ExactDecimal::I64(-12_345, 2), 3, "-9.99", true),
            (
                ExactDecimal::I128(i128::MAX, 0),
                65,
                "170141183460469231731687303715884105727",
                false,
            ),
        ] {
            let (result, clamped) = clamp_exact_to_precision(value, precision);
            assert_eq!(clamped, was_clamped);
            assert_eq!(result.format(), expected);
            assert!(
                !matches!(result, ExactDecimal::Big(..)),
                "fixed-width DECIMAL unexpectedly promoted at precision {precision}"
            );
        }
    }

    #[test]
    fn exact_precision_clamp_preserves_wide_decimal_semantics() {
        let (magnitude, scale) = parse_exact_decimal_big(
            "99999999999999999999999999999999999999999999999999999999999999999",
        )
        .unwrap();
        let (result, clamped) = clamp_exact_to_precision(ExactDecimal::Big(magnitude, scale), 39);
        assert!(clamped);
        assert_eq!(result.format(), "999999999999999999999999999999999999999");
        assert!(matches!(result, ExactDecimal::Big(..)));
    }

    #[test]
    fn decimal_bcd_round_trip_full_precision() {
        for value in [
            "0",
            "-0.0000",
            "12.3400",
            "-99999999999999999999999999999999999.000000000000000000000000000001",
            "99999999999999999999999999999999999999999999999999999999999999999",
        ] {
            let canonical = canonical_decimal_str(value).unwrap();
            let mut encoded = Vec::new();
            encode_decimal_bcd(value, &mut encoded).unwrap();
            let (decoded, used) = decode_decimal_bcd(&encoded).unwrap();
            assert_eq!(used, encoded.len());
            assert_eq!(decoded, canonical);
        }
    }

    #[test]
    fn decimal_big_parse_and_rescale_cover_mysql_65_digits() {
        let max = "99999999999999999999999999999999999.999999999999999999999999999999";
        let (magnitude, scale) = parse_exact_decimal_big(max).unwrap();
        assert_eq!(scale, 30);
        assert_eq!(magnitude.to_str_radix(10).len(), 65);
        assert_eq!(format_big_decimal_parts(&magnitude, scale), max);

        assert_eq!(
            rescale_decimal_big_str("1234567890123456789012345678901234567890.125", 2).as_deref(),
            Some("1234567890123456789012345678901234567890.13")
        );
        assert_eq!(
            rescale_decimal_big_str("-1234567890123456789012345678901234567890.125", 2).as_deref(),
            Some("-1234567890123456789012345678901234567890.13")
        );
        assert!(parse_exact_decimal_big("1.2.3").is_none());
        assert!(parse_exact_decimal_big("1e").is_none());
        assert!(parse_exact_decimal_big("1e4294967295").is_none());
        assert!(parse_exact_decimal_big("1e4294967297").is_none());
    }

    #[test]
    fn round_exact_basics() {
        // ROUND(2.675, 2) = 2.68 — the f64 path famously gets 2.67.
        assert_eq!(round_exact_parts(2675, 3, 2), Some((268, 2)));
        assert_eq!(round_exact_parts(-2675, 3, 2), Some((-268, 2)));
        // ROUND(2.5) = 3, ROUND(-2.5) = -3.
        assert_eq!(round_exact_parts(25, 1, 0), Some((3, 0)));
        assert_eq!(round_exact_parts(-25, 1, 0), Some((-3, 0)));
        // ROUND(123.45, -1) = 120.
        assert_eq!(round_exact_parts(12345, 2, -1), Some((120, 0)));
        // d >= scale is a no-op.
        assert_eq!(round_exact_parts(25, 1, 5), Some((25, 1)));
    }

    #[test]
    fn round_exact_wide_decimal_stays_exact() {
        let text = "1234567890123456789012345678901234567890.125";
        let (magnitude, scale) = parse_exact_decimal_big(text).unwrap();

        let rounded = round_exact(ExactDecimal::from_big(magnitude.clone(), scale), 2, true);
        let (magnitude, scale) = rounded.into_big_parts();
        assert_eq!(
            format_big_decimal_parts(&magnitude, scale),
            "1234567890123456789012345678901234567890.13"
        );

        let truncated = round_exact(ExactDecimal::from_big(magnitude, scale), -3, false);
        let (magnitude, scale) = truncated.into_big_parts();
        assert_eq!(
            format_big_decimal_parts(&magnitude, scale),
            "1234567890123456789012345678901234567000"
        );
    }

    #[test]
    fn truncate_exact_basics() {
        assert_eq!(truncate_exact_parts(2999, 3, 2), Some((299, 2)));
        assert_eq!(truncate_exact_parts(-2999, 3, 2), Some((-299, 2)));
        assert_eq!(truncate_exact_parts(12345, 2, -2), Some((100, 0)));
    }

    #[test]
    fn round_exact_extreme_second_arg() {
        // `scale - d` used to overflow i32 for hugely-negative `d` (panic in
        // debug, wrong magnitude in release). Rounding far left of the
        // decimal point yields 0 per MySQL. 1.5 = (15, 1).
        assert_eq!(round_exact_parts(15, 1, i32::MIN), Some((0, 0)));
        assert_eq!(truncate_exact_parts(15, 1, i32::MIN), Some((0, 0)));
        assert_eq!(round_exact_parts(15, 1, -2_000_000_000), Some((0, 0)));
        // Hugely-positive `d` is at/below no digit to drop → value unchanged.
        assert_eq!(round_exact_parts(15, 1, i32::MAX), Some((15, 1)));
        assert_eq!(truncate_exact_parts(15, 1, i32::MAX), Some((15, 1)));
        // Integer input (scale 0): 12345 rounds/truncates to 0 far left.
        assert_eq!(round_exact_parts(12345, 0, i32::MIN), Some((0, 0)));
        assert_eq!(truncate_exact_parts(12345, 0, i32::MIN), Some((0, 0)));
        assert_eq!(round_exact_parts(12345, 0, i32::MAX), Some((12345, 0)));
        // The boundary just past the i128 digit range still collapses to 0
        // rather than relying on pow10 (which is None past exp 38).
        assert_eq!(round_exact_parts(99, 0, -39), Some((0, 0)));
        assert_eq!(round_exact_parts(99, 0, -38), Some((0, 0)));
    }

    #[test]
    fn clamp_saturates() {
        // DECIMAL(3,1): max 99.9 → magnitude 999.
        assert_eq!(clamp_to_precision(99999, 3), (999, true));
        assert_eq!(clamp_to_precision(-99999, 3), (-999, true));
        assert_eq!(clamp_to_precision(998, 3), (998, false));
    }

    #[test]
    fn format_parts() {
        assert_eq!(format_decimal_parts(268, 2), "2.68");
        assert_eq!(format_decimal_parts(-5, 3), "-0.005");
        assert_eq!(format_decimal_parts(15000, 4), "1.5000");
        assert_eq!(format_decimal_parts(0, 2), "0.00");
        assert_eq!(format_decimal_parts(3, 0), "3");
    }

    #[test]
    fn lenient_f64() {
        let r = parse_lenient_f64("123abc");
        assert_eq!((r.value, r.clean), (123.0, false));
        let r = parse_lenient_f64("");
        assert_eq!((r.value, r.clean), (0.0, true));
        let r = parse_lenient_f64("  12.5x");
        assert_eq!((r.value, r.clean), (12.5, false));
        let r = parse_lenient_f64("1e3");
        assert_eq!((r.value, r.clean), (1000.0, true));
        let r = parse_lenient_f64("1e");
        assert_eq!((r.value, r.clean), (1.0, false));
        let r = parse_lenient_f64("abc");
        assert_eq!((r.value, r.clean), (0.0, false));
        let r = parse_lenient_f64("-2.5");
        assert_eq!((r.value, r.clean), (-2.5, true));
    }

    #[test]
    fn lenient_int() {
        let r = parse_lenient_int("3.7");
        assert_eq!((r.value, r.clean), (3, false));
        let r = parse_lenient_int("123abc");
        assert_eq!((r.value, r.clean), (123, false));
        let r = parse_lenient_int("1e3");
        assert_eq!((r.value, r.clean), (1, false));
        let r = parse_lenient_int("-12");
        assert_eq!((r.value, r.clean), (-12, true));
        let r = parse_lenient_int("");
        assert_eq!((r.value, r.clean), (0, true));
    }
}

#[cfg(test)]
mod shared_format_tests {
    use super::*;

    #[test]
    fn unsigned_format_matches_decimal_text_for_every_scale() {
        for magnitude in [
            0,
            1,
            9,
            10,
            i64::MAX as u128,
            (i128::MAX as u128) + 1,
            u128::MAX,
        ] {
            for scale in 0..=u8::MAX {
                let digits = magnitude.to_string();
                let padded = format!("{:0>width$}", digits, width = usize::from(scale) + 1);
                let mut expected = padded;
                if scale != 0 {
                    expected.insert(expected.len() - usize::from(scale), '.');
                }
                assert_eq!(format_decimal_magnitude(magnitude, scale, false), expected);
                assert_eq!(
                    format_decimal_magnitude(magnitude, scale, true),
                    format!("-{expected}")
                );
            }
        }
        assert_eq!(
            format_decimal_parts(i128::MIN, 2),
            "-1701411834604692317316873037158841057.28"
        );
    }
}

/// Convert a wide exact coefficient to f64 with signed infinity on overflow.
pub(crate) fn wide_to_f64(value: &crate::types::DecimalWide) -> f64 {
    format_big_decimal_parts(&value.magnitude, value.scale)
        .parse::<f64>()
        .unwrap_or(if value.magnitude.sign() == num_bigint::Sign::Minus {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        })
}

/// Checked integer conversion for JSON_VALUE and typed multi-valued keys.
/// This intentionally does not implement scalar CAST's unsigned wrapping.
pub(crate) fn checked_integer_text(text: &str, unsigned: bool) -> Option<crate::types::Value> {
    let (magnitude, scale) = parse_exact_decimal_big(text)?;
    let rounded = rescale_exact(ExactDecimal::from_big(magnitude, scale), 0, true);
    let (magnitude, _) = rounded.into_big_parts();
    if unsigned {
        magnitude.to_u64().map(crate::types::Value::UInt)
    } else {
        magnitude.to_i64().map(crate::types::Value::Int)
    }
}
