//! Cached-row scan / filter demo for the proposed vectorized predicate path.
//!
//! This is intentionally isolated from the production executor.  It models a
//! fixed-width `lineitem` projection in the same row-major shape a cached heap
//! page exposes, then compares:
//!   1. row decode + `Value` envelopes + numeric coercion (current hot path),
//!   2. an ideal exact scalar row loop,
//!   3. row storage -> typed batches -> SIMD selection -> late price decode,
//!   4. already-decoded typed columns -> SIMD selection (upper bound).
//!
//! Run with defaults matching TPC-H SF=1 lineitem cardinality:
//!   cargo run --release --features tools --bin vec_filter_simd_demo
//!
//! Optional positional arguments: ROWS ITERS BATCH_ROWS.

use std::hint::black_box;
use std::time::Instant;

use mydb::types::Value;

const DEFAULT_ROWS: usize = 6_001_215;
const DEFAULT_ITERS: usize = 7;
const DEFAULT_BATCH_ROWS: usize = 2_048;

// Mock fixed-width projected heap row.  Padding mirrors a naturally aligned
// layout and, more importantly, keeps the source row-major rather than giving
// the vector path an artificial columnar input.
const ROW_BYTES: usize = 32;
const SHIPDATE_OFF: usize = 0;
const DISCOUNT_OFF: usize = 8;
const QUANTITY_OFF: usize = 16;
const PRICE_OFF: usize = 24;

// Days since 1970-01-01 and DECIMAL scale=2 magnitudes.
const DATE_1994_01_01: i32 = 8_766;
const DATE_1995_01_01: i32 = 9_131;
const DISCOUNT_LOW: i64 = 5;
const DISCOUNT_HIGH: i64 = 7;
const QUANTITY_HIGH: i64 = 2_400;

#[derive(Default)]
struct PredicateScratch {
    shipdate: Vec<i32>,
    discount: Vec<i64>,
    quantity: Vec<i64>,
    selected: Vec<u32>,
}

impl PredicateScratch {
    fn with_capacity(n: usize) -> Self {
        Self {
            shipdate: Vec::with_capacity(n),
            discount: Vec::with_capacity(n),
            quantity: Vec::with_capacity(n),
            selected: Vec::with_capacity(n / 8),
        }
    }

    fn clear(&mut self) {
        self.shipdate.clear();
        self.discount.clear();
        self.quantity.clear();
        self.selected.clear();
    }
}

struct TypedColumns {
    shipdate: Vec<i32>,
    discount: Vec<i64>,
    quantity: Vec<i64>,
    price: Vec<i64>,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rows = parse_arg(&args, 1, DEFAULT_ROWS);
    let iters = parse_arg(&args, 2, DEFAULT_ITERS).max(1);
    let batch_rows = parse_arg(&args, 3, DEFAULT_BATCH_ROWS).max(64);

    println!(
        "vec_filter_simd_demo: rows={rows}, iters={iters}, batch_rows={batch_rows}, arch={}",
        std::env::consts::ARCH
    );
    println!("predicate: shipdate in 1994 AND discount in [0.05,0.07] AND quantity < 24");

    let build_start = Instant::now();
    let raw = build_mock_heap(rows);
    let typed = decode_all_columns(&raw, rows);
    println!(
        "prepared {:.1} MiB row store + {:.1} MiB typed columns in {:.3}s",
        raw.len() as f64 / 1_048_576.0,
        (rows * (4 + 8 + 8 + 8)) as f64 / 1_048_576.0,
        build_start.elapsed().as_secs_f64()
    );
    println!();
    println!(
        "{:<42} {:>10} {:>12} {:>12} {:>10}",
        "method", "median ms", "ns/row", "M rows/s", "speedup"
    );

    let expected = scan_row_value(&raw, rows);
    let value_ns = bench(
        "row + Value/coercion (current model)",
        iters,
        rows,
        None,
        || scan_row_value(black_box(&raw), rows),
    );

    let scalar = scan_row_typed(&raw, rows);
    assert_eq!(expected, scalar, "typed scalar result diverged");
    bench(
        "row exact typed (strong scalar oracle)",
        iters,
        rows,
        Some(value_ns),
        || scan_row_typed(black_box(&raw), rows),
    );

    let mut scratch = PredicateScratch::with_capacity(batch_rows);
    let raw_vec = scan_raw_vec(&raw, rows, batch_rows, &mut scratch);
    assert_eq!(expected, raw_vec, "raw->typed SIMD result diverged");
    let raw_vec_ns = bench(
        "row store -> typed batch -> SIMD",
        iters,
        rows,
        Some(value_ns),
        || scan_raw_vec(black_box(&raw), rows, batch_rows, &mut scratch),
    );

    let mut selected = Vec::with_capacity(batch_rows / 8);
    let typed_scalar = scan_typed_scalar(&typed);
    assert_eq!(
        expected, typed_scalar,
        "typed-column scalar result diverged"
    );
    bench(
        "typed columns -> scalar filter",
        iters,
        rows,
        Some(value_ns),
        || scan_typed_scalar(black_box(&typed)),
    );

    let columnar = scan_typed_vec(&typed, batch_rows, &mut selected);
    assert_eq!(expected, columnar, "typed-column SIMD result diverged");
    bench(
        "typed columns -> SIMD (materialized-column ceiling)",
        iters,
        rows,
        Some(value_ns),
        || scan_typed_vec(black_box(&typed), batch_rows, &mut selected),
    );

    println!();
    println!(
        "selected={} ({:.3}%), exact_sum={} (scale=4)",
        expected.1,
        expected.1 as f64 * 100.0 / rows as f64,
        expected.0
    );
    println!(
        "end-to-end raw-row SIMD vs current-model speedup: {:.2}x",
        value_ns / raw_vec_ns
    );
}

fn parse_arg(args: &[String], idx: usize, default: usize) -> usize {
    args.get(idx)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn build_mock_heap(rows: usize) -> Vec<u8> {
    let mut raw = vec![0u8; rows.saturating_mul(ROW_BYTES)];
    for i in 0..rows {
        // Independent deterministic hashes approximate Q6's selectivity
        // without handing the scalar loop an unrealistically periodic branch
        // pattern:
        // 1/7 year * 3/11 discount values * 23/50 quantities ~= 1.79%.
        let h0 = mix64(i as u64);
        let h1 = mix64(h0);
        let h2 = mix64(h1);
        let h3 = mix64(h2);
        let shipdate = 8_035 + (h0 % (365 * 7) as u64) as i32;
        let discount = (h1 % 11) as i64;
        let quantity = ((h2 % 50) as i64 + 1) * 100;
        let price = 10_000 + (h3 % 10_000_000) as i64;
        let base = i * ROW_BYTES;
        raw[base + SHIPDATE_OFF..base + SHIPDATE_OFF + 4].copy_from_slice(&shipdate.to_le_bytes());
        raw[base + DISCOUNT_OFF..base + DISCOUNT_OFF + 8].copy_from_slice(&discount.to_le_bytes());
        raw[base + QUANTITY_OFF..base + QUANTITY_OFF + 8].copy_from_slice(&quantity.to_le_bytes());
        raw[base + PRICE_OFF..base + PRICE_OFF + 8].copy_from_slice(&price.to_le_bytes());
    }
    raw
}

#[inline]
fn mix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn decode_all_columns(raw: &[u8], rows: usize) -> TypedColumns {
    let mut out = TypedColumns {
        shipdate: Vec::with_capacity(rows),
        discount: Vec::with_capacity(rows),
        quantity: Vec::with_capacity(rows),
        price: Vec::with_capacity(rows),
    };
    for row in 0..rows {
        let base = row * ROW_BYTES;
        // SAFETY: `build_mock_heap` allocated exactly rows * ROW_BYTES and
        // every field lies within its row. Unaligned reads model heap decode.
        unsafe {
            out.shipdate.push(read_i32(raw, base + SHIPDATE_OFF));
            out.discount.push(read_i64(raw, base + DISCOUNT_OFF));
            out.quantity.push(read_i64(raw, base + QUANTITY_OFF));
            out.price.push(read_i64(raw, base + PRICE_OFF));
        }
    }
    out
}

fn scan_row_value(raw: &[u8], rows: usize) -> (i128, usize) {
    let mut sum = 0i128;
    let mut selected = 0usize;
    for row in 0..rows {
        let base = row * ROW_BYTES;
        // SAFETY: see `decode_all_columns`.
        let (shipdate, discount, quantity) = unsafe {
            (
                read_i32(raw, base + SHIPDATE_OFF),
                read_i64(raw, base + DISCOUNT_OFF),
                read_i64(raw, base + QUANTITY_OFF),
            )
        };
        let values = [
            Value::Date(shipdate),
            Value::DecimalCompact(discount, 2),
            Value::DecimalCompact(quantity, 2),
        ];
        if !eval_value_predicate(&values) {
            continue;
        }
        let price = unsafe { read_i64(raw, base + PRICE_OFF) };
        sum += (price as i128) * (discount as i128);
        selected += 1;
    }
    (sum, selected)
}

// Kept out-of-line to preserve the dispatch/coercion boundary present in the
// production CompiledPredicate hot path.
#[inline(never)]
fn eval_value_predicate(values: &[Value; 3]) -> bool {
    let shipdate = match values[0] {
        Value::Date(v) => v,
        _ => return false,
    };
    if !(DATE_1994_01_01..DATE_1995_01_01).contains(&shipdate) {
        return false;
    }
    let discount = value_to_f64(&values[1]);
    if !(0.05..=0.07).contains(&discount) {
        return false;
    }
    value_to_f64(&values[2]) < 24.0
}

#[inline(never)]
fn value_to_f64(value: &Value) -> f64 {
    match value {
        Value::Int(v) => *v as f64,
        Value::UInt(v) => *v as f64,
        Value::Float(v) => *v as f64,
        Value::Double(v) => *v,
        Value::DecimalCompact(mag, scale) => *mag as f64 / 10f64.powi(*scale as i32),
        _ => f64::NAN,
    }
}

fn scan_row_typed(raw: &[u8], rows: usize) -> (i128, usize) {
    let mut sum = 0i128;
    let mut selected = 0usize;
    for row in 0..rows {
        let base = row * ROW_BYTES;
        let (shipdate, discount, quantity) = unsafe {
            (
                read_i32(raw, base + SHIPDATE_OFF),
                read_i64(raw, base + DISCOUNT_OFF),
                read_i64(raw, base + QUANTITY_OFF),
            )
        };
        if shipdate < DATE_1994_01_01
            || shipdate >= DATE_1995_01_01
            || !(DISCOUNT_LOW..=DISCOUNT_HIGH).contains(&discount)
            || quantity >= QUANTITY_HIGH
        {
            continue;
        }
        let price = unsafe { read_i64(raw, base + PRICE_OFF) };
        sum += (price as i128) * (discount as i128);
        selected += 1;
    }
    (sum, selected)
}

fn scan_raw_vec(
    raw: &[u8],
    rows: usize,
    batch_rows: usize,
    scratch: &mut PredicateScratch,
) -> (i128, usize) {
    let mut sum = 0i128;
    let mut total_selected = 0usize;
    for start in (0..rows).step_by(batch_rows) {
        let end = (start + batch_rows).min(rows);
        scratch.clear();
        for row in start..end {
            let base = row * ROW_BYTES;
            unsafe {
                scratch.shipdate.push(read_i32(raw, base + SHIPDATE_OFF));
                scratch.discount.push(read_i64(raw, base + DISCOUNT_OFF));
                scratch.quantity.push(read_i64(raw, base + QUANTITY_OFF));
            }
        }
        filter_simd(
            &scratch.shipdate,
            &scratch.discount,
            &scratch.quantity,
            &mut scratch.selected,
        );
        for &local in &scratch.selected {
            let local = local as usize;
            let row = start + local;
            let price = unsafe { read_i64(raw, row * ROW_BYTES + PRICE_OFF) };
            sum += (price as i128) * (scratch.discount[local] as i128);
        }
        total_selected += scratch.selected.len();
    }
    (sum, total_selected)
}

fn scan_typed_vec(
    columns: &TypedColumns,
    batch_rows: usize,
    selected: &mut Vec<u32>,
) -> (i128, usize) {
    let rows = columns.shipdate.len();
    let mut sum = 0i128;
    let mut total_selected = 0usize;
    for start in (0..rows).step_by(batch_rows) {
        let end = (start + batch_rows).min(rows);
        selected.clear();
        filter_simd(
            &columns.shipdate[start..end],
            &columns.discount[start..end],
            &columns.quantity[start..end],
            selected,
        );
        for &local in selected.iter() {
            let row = start + local as usize;
            sum += (columns.price[row] as i128) * (columns.discount[row] as i128);
        }
        total_selected += selected.len();
    }
    (sum, total_selected)
}

fn scan_typed_scalar(columns: &TypedColumns) -> (i128, usize) {
    let mut sum = 0i128;
    let mut selected = 0usize;
    for i in 0..columns.shipdate.len() {
        if columns.shipdate[i] < DATE_1994_01_01
            || columns.shipdate[i] >= DATE_1995_01_01
            || columns.discount[i] < DISCOUNT_LOW
            || columns.discount[i] > DISCOUNT_HIGH
            || columns.quantity[i] >= QUANTITY_HIGH
        {
            continue;
        }
        sum += (columns.price[i] as i128) * (columns.discount[i] as i128);
        selected += 1;
    }
    (sum, selected)
}

#[inline]
fn filter_simd(shipdate: &[i32], discount: &[i64], quantity: &[i64], out: &mut Vec<u32>) {
    debug_assert_eq!(shipdate.len(), discount.len());
    debug_assert_eq!(shipdate.len(), quantity.len());
    #[cfg(target_arch = "aarch64")]
    unsafe {
        filter_neon(shipdate, discount, quantity, out);
    }
    #[cfg(target_arch = "x86_64")]
    unsafe {
        if std::is_x86_feature_detected!("avx2") {
            filter_avx2(shipdate, discount, quantity, out);
        } else {
            filter_portable(shipdate, discount, quantity, out);
        }
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    filter_portable(shipdate, discount, quantity, out);
}

#[cfg(target_arch = "aarch64")]
unsafe fn filter_neon(shipdate: &[i32], discount: &[i64], quantity: &[i64], out: &mut Vec<u32>) {
    use std::arch::aarch64::*;
    let mut i = 0usize;
    let date_lo = vdupq_n_s32(DATE_1994_01_01);
    let date_hi = vdupq_n_s32(DATE_1995_01_01);
    let disc_lo = vdupq_n_s64(DISCOUNT_LOW);
    let disc_hi = vdupq_n_s64(DISCOUNT_HIGH);
    let qty_hi = vdupq_n_s64(QUANTITY_HIGH);
    while i + 4 <= shipdate.len() {
        let dates = vld1q_s32(shipdate.as_ptr().add(i));
        let date_mask = vandq_u32(vcgeq_s32(dates, date_lo), vcltq_s32(dates, date_hi));

        let d0 = vld1q_s64(discount.as_ptr().add(i));
        let q0 = vld1q_s64(quantity.as_ptr().add(i));
        let pred0 = vandq_u64(
            vandq_u64(vcgeq_s64(d0, disc_lo), vcleq_s64(d0, disc_hi)),
            vcltq_s64(q0, qty_hi),
        );
        if vgetq_lane_u32(date_mask, 0) != 0 && vgetq_lane_u64(pred0, 0) != 0 {
            out.push(i as u32);
        }
        if vgetq_lane_u32(date_mask, 1) != 0 && vgetq_lane_u64(pred0, 1) != 0 {
            out.push((i + 1) as u32);
        }

        let d1 = vld1q_s64(discount.as_ptr().add(i + 2));
        let q1 = vld1q_s64(quantity.as_ptr().add(i + 2));
        let pred1 = vandq_u64(
            vandq_u64(vcgeq_s64(d1, disc_lo), vcleq_s64(d1, disc_hi)),
            vcltq_s64(q1, qty_hi),
        );
        if vgetq_lane_u32(date_mask, 2) != 0 && vgetq_lane_u64(pred1, 0) != 0 {
            out.push((i + 2) as u32);
        }
        if vgetq_lane_u32(date_mask, 3) != 0 && vgetq_lane_u64(pred1, 1) != 0 {
            out.push((i + 3) as u32);
        }
        i += 4;
    }
    filter_tail(shipdate, discount, quantity, i, out);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_avx2(shipdate: &[i32], discount: &[i64], quantity: &[i64], out: &mut Vec<u32>) {
    use std::arch::x86_64::*;
    let mut i = 0usize;
    let date_lo_minus_one = _mm_set1_epi32(DATE_1994_01_01 - 1);
    let date_hi = _mm_set1_epi32(DATE_1995_01_01);
    let disc_lo_minus_one = _mm256_set1_epi64x(DISCOUNT_LOW - 1);
    let disc_hi_plus_one = _mm256_set1_epi64x(DISCOUNT_HIGH + 1);
    let qty_hi = _mm256_set1_epi64x(QUANTITY_HIGH);
    while i + 4 <= shipdate.len() {
        let dates = _mm_loadu_si128(shipdate.as_ptr().add(i) as *const __m128i);
        let date_mask = _mm_and_si128(
            _mm_cmpgt_epi32(dates, date_lo_minus_one),
            _mm_cmpgt_epi32(date_hi, dates),
        );
        let discs = _mm256_loadu_si256(discount.as_ptr().add(i) as *const __m256i);
        let qty = _mm256_loadu_si256(quantity.as_ptr().add(i) as *const __m256i);
        let numeric_mask = _mm256_and_si256(
            _mm256_and_si256(
                _mm256_cmpgt_epi64(discs, disc_lo_minus_one),
                _mm256_cmpgt_epi64(disc_hi_plus_one, discs),
            ),
            _mm256_cmpgt_epi64(qty_hi, qty),
        );
        let date_bits = _mm_movemask_ps(_mm_castsi128_ps(date_mask)) as u32;
        let numeric_bits = _mm256_movemask_pd(_mm256_castsi256_pd(numeric_mask)) as u32;
        let mut bits = date_bits & numeric_bits & 0x0f;
        while bits != 0 {
            let lane = bits.trailing_zeros() as usize;
            out.push((i + lane) as u32);
            bits &= bits - 1;
        }
        i += 4;
    }
    filter_tail(shipdate, discount, quantity, i, out);
}

#[allow(dead_code)]
fn filter_portable(shipdate: &[i32], discount: &[i64], quantity: &[i64], out: &mut Vec<u32>) {
    filter_tail(shipdate, discount, quantity, 0, out);
}

fn filter_tail(
    shipdate: &[i32],
    discount: &[i64],
    quantity: &[i64],
    start: usize,
    out: &mut Vec<u32>,
) {
    for i in start..shipdate.len() {
        if shipdate[i] >= DATE_1994_01_01
            && shipdate[i] < DATE_1995_01_01
            && discount[i] >= DISCOUNT_LOW
            && discount[i] <= DISCOUNT_HIGH
            && quantity[i] < QUANTITY_HIGH
        {
            out.push(i as u32);
        }
    }
}

#[inline]
unsafe fn read_i32(raw: &[u8], offset: usize) -> i32 {
    i32::from_le(std::ptr::read_unaligned(
        raw.as_ptr().add(offset) as *const i32
    ))
}

#[inline]
unsafe fn read_i64(raw: &[u8], offset: usize) -> i64 {
    i64::from_le(std::ptr::read_unaligned(
        raw.as_ptr().add(offset) as *const i64
    ))
}

fn bench<F>(name: &str, iters: usize, rows: usize, baseline_ns: Option<f64>, mut body: F) -> f64
where
    F: FnMut() -> (i128, usize),
{
    black_box(body());
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        black_box(body());
        samples.push(start.elapsed().as_secs_f64() * 1e9);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = samples[samples.len() / 2];
    let ns_per_row = median / rows.max(1) as f64;
    let speedup = baseline_ns
        .map(|base| format!("{:.2}x", base / median))
        .unwrap_or_else(|| "—".into());
    println!(
        "{:<42} {:>10.2} {:>12.2} {:>12.2} {:>10}",
        name,
        median / 1e6,
        ns_per_row,
        1_000.0 / ns_per_row,
        speedup
    );
    median
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_paths_match_with_partial_tail_batch() {
        let rows = 10_003;
        let batch_rows = 257;
        let raw = build_mock_heap(rows);
        let typed = decode_all_columns(&raw, rows);
        let expected = scan_row_value(&raw, rows);
        assert_eq!(expected, scan_row_typed(&raw, rows));
        assert_eq!(expected, scan_typed_scalar(&typed));

        let mut scratch = PredicateScratch::with_capacity(batch_rows);
        assert_eq!(expected, scan_raw_vec(&raw, rows, batch_rows, &mut scratch));
        let mut selected = Vec::new();
        assert_eq!(expected, scan_typed_vec(&typed, batch_rows, &mut selected));
    }
}
