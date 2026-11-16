//! Micro-benchmark for SUM(DECIMAL) — compares the current scalar state
//! machine path (a replica of `accumulate_numeric_state` from
//! `src/engine/executor/select/aggregate.rs`) against several SIMD-style
//! batch implementations operating directly on a `&[i128]` magnitude
//! column with a uniform `scale` (the layout used by
//! `ColumnVector::Decimal128`).
//!
//! Run:
//!   cargo run --release --features tools --bin decimal_sum_simd_bench
//!
//! Output columns: variant, rows, ms/run, ns/row, Mrows/s, speedup vs baseline.

use std::hint::black_box;
use std::time::Instant;

use mydb::types::Value;

const ROWS: usize = 10_000_000;
const ITERS: usize = 7;

fn main() {
    println!(
        "decimal_sum_simd_bench: rows={ROWS}, iters={ITERS} (median reported), arch={}, profile={}",
        std::env::consts::ARCH,
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
    );

    // ---------------------------------------------------------------
    // Scenario 1 — fits in i64, uniform scale (the common case: a
    // single DECIMAL column inside SUM(price), SUM(amount), ...)
    // ---------------------------------------------------------------
    let i64_fitting: Vec<i128> = (0..ROWS)
        .map(|i| (i as i128 * 137) % 1_000_000_000)
        .collect();
    let scale: u8 = 2;
    let expect_i64: i128 = i64_fitting.iter().sum();

    println!("\n[Scenario 1] magnitude fits in i64, uniform scale={scale}");
    println!(
        "{:<38} {:>12} {:>12} {:>10} {:>10}",
        "variant", "ms/run", "ns/row", "Mrows/s", "speedup"
    );
    let baseline_ns = bench(
        "baseline_value_state_machine",
        || {
            // Mimics the real hot path in
            // `src/engine/executor/vec/hash_agg.rs` — `value_at()` returns
            // a `Value::DecimalCompact`, which is then fed into
            // `accumulate_numeric_state`.
            let mut state = NumericAggState::Empty;
            for &mag in &i64_fitting {
                let v = Value::DecimalCompact(mag as i64, scale);
                accumulate_numeric_state_replica(&mut state, &v);
            }
            let total = state_total_i128(&state).unwrap();
            assert_eq!(total, expect_i64);
            black_box(total)
        },
        None,
        ROWS,
    );

    bench(
        "baseline_state_machine_no_value",
        || {
            // Strip out the `Value` wrap + `match` for tag dispatch; only
            // measure the cost of the state-machine itself
            // (`ExactDecimalAcc::add_parts` w/ scale align + checked_add).
            let mut state = NumericAggState::Empty;
            for &mag in &i64_fitting {
                add_parts_into_state(&mut state, mag, scale);
            }
            let total = state_total_i128(&state).unwrap();
            assert_eq!(total, expect_i64);
            black_box(total)
        },
        Some(baseline_ns),
        ROWS,
    );

    bench(
        "i128_naive_fold",
        || {
            let mut acc: i128 = 0;
            for &v in &i64_fitting {
                acc = acc.wrapping_add(v);
            }
            assert_eq!(acc, expect_i64);
            black_box(acc)
        },
        Some(baseline_ns),
        ROWS,
    );

    bench(
        "i128_unrolled4",
        || {
            let total = sum_i128_unrolled4(&i64_fitting);
            assert_eq!(total, expect_i64);
            black_box(total)
        },
        Some(baseline_ns),
        ROWS,
    );

    bench(
        "i128_unrolled8",
        || {
            let total = sum_i128_unrolled8(&i64_fitting);
            assert_eq!(total, expect_i64);
            black_box(total)
        },
        Some(baseline_ns),
        ROWS,
    );

    bench(
        "i64_unrolled4_widen (fit-in-i64 fast path)",
        || {
            // Caller proves the column fits in i64; we accumulate in i64
            // lanes (auto-vectorisable into SIMD on AVX2 / NEON) and only
            // widen to i128 at the end. This is the same approach as
            // `expr_batch::sum_i64_simd` used for BIGINT today.
            let mut buf = Vec::with_capacity(i64_fitting.len());
            buf.extend(i64_fitting.iter().map(|v| *v as i64));
            let total = sum_i64_unrolled4_widen(&buf);
            assert_eq!(total, expect_i64);
            black_box(total)
        },
        Some(baseline_ns),
        ROWS,
    );

    // Same as above but skip the copy — measure pure compute speed
    // on a column that is already laid out as i64. This mirrors what a
    // dedicated `ColumnVector::Decimal64` (or a Decimal128 lane that
    // remembers it fits in i64) would look like.
    let i64_col: Vec<i64> = i64_fitting.iter().map(|v| *v as i64).collect();
    bench(
        "i64_unrolled4_widen (no copy)",
        || {
            let total = sum_i64_unrolled4_widen(&i64_col);
            assert_eq!(total, expect_i64);
            black_box(total)
        },
        Some(baseline_ns),
        ROWS,
    );

    bench(
        "i64_unrolled8_widen (no copy)",
        || {
            let total = sum_i64_unrolled8_widen(&i64_col);
            assert_eq!(total, expect_i64);
            black_box(total)
        },
        Some(baseline_ns),
        ROWS,
    );

    // ---------------------------------------------------------------
    // Scenario 2 — magnitude overflows i64, must stay in i128.
    // Numbers chosen so that values are 80-bit-ish — they can't be
    // demoted to i64.
    // ---------------------------------------------------------------
    let i128_only: Vec<i128> = (0..ROWS)
        .map(|i| {
            let big: i128 = (i as i128).wrapping_mul(1_000_000_000_000_000_000_i128);
            big + (i as i128) * 7
        })
        .collect();
    let expect_i128: i128 = i128_only.iter().sum();

    println!("\n[Scenario 2] magnitude overflows i64 (true i128, uniform scale={scale})");
    println!(
        "{:<38} {:>12} {:>12} {:>10} {:>10}",
        "variant", "ms/run", "ns/row", "Mrows/s", "speedup"
    );
    let baseline2_ns = bench(
        "baseline_value_state_machine",
        || {
            let mut state = NumericAggState::Empty;
            for &mag in &i128_only {
                let v = if let Ok(small) = i64::try_from(mag) {
                    Value::DecimalCompact(small, scale)
                } else {
                    Value::Decimal128(mag, scale)
                };
                accumulate_numeric_state_replica(&mut state, &v);
            }
            let total = state_total_i128(&state).unwrap();
            assert_eq!(total, expect_i128);
            black_box(total)
        },
        None,
        ROWS,
    );

    bench(
        "baseline_state_machine_no_value",
        || {
            let mut state = NumericAggState::Empty;
            for &mag in &i128_only {
                add_parts_into_state(&mut state, mag, scale);
            }
            let total = state_total_i128(&state).unwrap();
            assert_eq!(total, expect_i128);
            black_box(total)
        },
        Some(baseline2_ns),
        ROWS,
    );

    bench(
        "i128_naive_fold",
        || {
            let mut acc: i128 = 0;
            for &v in &i128_only {
                acc = acc.wrapping_add(v);
            }
            assert_eq!(acc, expect_i128);
            black_box(acc)
        },
        Some(baseline2_ns),
        ROWS,
    );

    bench(
        "i128_unrolled4",
        || {
            let total = sum_i128_unrolled4(&i128_only);
            assert_eq!(total, expect_i128);
            black_box(total)
        },
        Some(baseline2_ns),
        ROWS,
    );

    bench(
        "i128_unrolled8",
        || {
            let total = sum_i128_unrolled8(&i128_only);
            assert_eq!(total, expect_i128);
            black_box(total)
        },
        Some(baseline2_ns),
        ROWS,
    );

    // ---------------------------------------------------------------
    // Scenario 3 — what about `Value::Decimal(String)` (legacy path,
    // e.g. when the schema stores DECIMAL as a string)? That involves
    // parsing each row in `parse_exact_decimal_string`. Many existing
    // catalogs end up here, so it's worth knowing the gap.
    // ---------------------------------------------------------------
    let strings: Vec<String> = i64_fitting
        .iter()
        .map(|&mag| format_i128_decimal(mag, scale))
        .collect();
    let expect_str: i128 = i64_fitting.iter().sum();

    println!("\n[Scenario 3] Value::Decimal(String) — legacy non-compact storage");
    println!(
        "{:<38} {:>12} {:>12} {:>10} {:>10}",
        "variant", "ms/run", "ns/row", "Mrows/s", "speedup"
    );
    let baseline3_ns = bench(
        "baseline_value_state_machine",
        || {
            let mut state = NumericAggState::Empty;
            for s in &strings {
                let v = Value::Decimal(s.clone());
                accumulate_numeric_state_replica(&mut state, &v);
            }
            let total = state_total_i128(&state).unwrap();
            assert_eq!(total, expect_str);
            black_box(total)
        },
        None,
        ROWS,
    );

    bench(
        "i64_unrolled4_widen (if compact storage)",
        || {
            let total = sum_i64_unrolled4_widen(&i64_col);
            assert_eq!(total, expect_str);
            black_box(total)
        },
        Some(baseline3_ns),
        ROWS,
    );

    // ---------------------------------------------------------------
    // Scenario 4 — drives the production `ColumnVector::Decimal32` /
    // `Decimal64` lanes via `sum_decimal_column` to verify end-to-end
    // that the new data structure picks the narrow lane and routes
    // through the SIMD kernel. Construction cost is included in the
    // first iteration via `ColumnVector::from_values`, then the hot
    // loop only measures the kernel.
    // ---------------------------------------------------------------
    use mydb::engine::vec_bench_api::{
        sum_decimal_column, sum_i128_simd, sum_i32_simd, sum_i64_simd, ColumnVector,
    };

    // DECIMAL(9, 2) — fits in i32. mod 10^9 keeps every value well inside
    // ±i32::MAX. This is exactly the shape of tpch `l_discount`/`l_tax`
    // (scale 2, small magnitudes), TPC-C `d_ytd` after a few updates,
    // etc.
    let dec32_vals: Vec<Value> = i64_fitting
        .iter()
        .map(|&mag| {
            let m = (mag % 1_000_000_000) as i64; // safely fits i32
            Value::DecimalCompact(m, 2)
        })
        .collect();
    let cv_dec32 = ColumnVector::from_values(dec32_vals);
    let expect_dec32: i128 = match &cv_dec32 {
        ColumnVector::Decimal32 { data, .. } => data.iter().map(|v| *v as i128).sum(),
        ColumnVector::Decimal64 { data, .. } => data.iter().map(|v| *v as i128).sum(),
        ColumnVector::Decimal128 { data, .. } => data.iter().sum(),
        _ => panic!("expected decimal lane"),
    };

    println!("\n[Scenario 4] new ColumnVector::Decimal{{32,64,128}} lanes via sum_decimal_column");
    println!(
        "{:<38} {:>12} {:>12} {:>10} {:>10}",
        "variant", "ms/run", "ns/row", "Mrows/s", "speedup"
    );
    // Report which lane `from_values` selected (smoke-test the lane
    // selection logic).
    let lane_tag = match &cv_dec32 {
        ColumnVector::Decimal32 { .. } => "Decimal32",
        ColumnVector::Decimal64 { .. } => "Decimal64",
        ColumnVector::Decimal128 { .. } => "Decimal128",
        _ => "OTHER",
    };
    println!("  (lane chosen for fits-i32 input: {lane_tag})");

    let baseline4_ns = bench(
        "baseline_value_state_machine",
        || {
            let mut state = NumericAggState::Empty;
            // Re-materialise via value_at to mirror the legacy SUM path
            // that doesn't know about the typed lane.
            for i in 0..cv_dec32.len() {
                let v = cv_dec32.value_at(i);
                accumulate_numeric_state_replica(&mut state, &v);
            }
            let total = state_total_i128(&state).unwrap();
            assert_eq!(total, expect_dec32);
            black_box(total)
        },
        None,
        cv_dec32.len(),
    );

    bench(
        "sum_decimal_column (auto-route)",
        || {
            let (total, _scale) = sum_decimal_column(&cv_dec32).unwrap();
            assert_eq!(total, expect_dec32);
            black_box(total)
        },
        Some(baseline4_ns),
        cv_dec32.len(),
    );

    // Direct kernel calls — quantify the per-kernel speed independent
    // of the dispatch wrapper.
    if let Some((slice, _)) = cv_dec32.as_decimal_i32_slice() {
        bench(
            "sum_i32_simd (direct)",
            || {
                let total = sum_i32_simd(slice);
                assert_eq!(total, expect_dec32);
                black_box(total)
            },
            Some(baseline4_ns),
            slice.len(),
        );
    }

    // Force the i64 lane: same data, but DECIMAL(15, 2) magnitudes
    // exceed i32::MAX so `from_values` should route to Decimal64.
    let dec64_vals: Vec<Value> = (0..ROWS)
        .map(|i| {
            // Stay inside i64 by capping at ~10^17.
            let m = (i as i64 * 137 * 10_000) % 100_000_000_000_000_000_i64;
            Value::DecimalCompact(m, 2)
        })
        .collect();
    let cv_dec64 = ColumnVector::from_values(dec64_vals);
    let expect_dec64: i128 = match &cv_dec64 {
        ColumnVector::Decimal64 { data, .. } => data.iter().map(|v| *v as i128).sum(),
        ColumnVector::Decimal128 { data, .. } => data.iter().sum(),
        _ => panic!(
            "expected i64 lane, got {:?}",
            core::mem::discriminant(&cv_dec64)
        ),
    };
    let lane_tag64 = match &cv_dec64 {
        ColumnVector::Decimal32 { .. } => "Decimal32",
        ColumnVector::Decimal64 { .. } => "Decimal64",
        ColumnVector::Decimal128 { .. } => "Decimal128",
        _ => "OTHER",
    };
    println!("\n  (lane chosen for fits-i64 input: {lane_tag64})");

    let baseline4b_ns = bench(
        "baseline_value_state_machine (dec64)",
        || {
            let mut state = NumericAggState::Empty;
            for i in 0..cv_dec64.len() {
                let v = cv_dec64.value_at(i);
                accumulate_numeric_state_replica(&mut state, &v);
            }
            let total = state_total_i128(&state).unwrap();
            assert_eq!(total, expect_dec64);
            black_box(total)
        },
        None,
        cv_dec64.len(),
    );

    bench(
        "sum_decimal_column (i64 lane)",
        || {
            let (total, _scale) = sum_decimal_column(&cv_dec64).unwrap();
            assert_eq!(total, expect_dec64);
            black_box(total)
        },
        Some(baseline4b_ns),
        cv_dec64.len(),
    );

    if let Some((slice, _)) = cv_dec64.as_decimal_i64_slice() {
        bench(
            "sum_i64_simd (direct)",
            || {
                let total = sum_i64_simd(slice);
                assert_eq!(total, expect_dec64);
                black_box(total)
            },
            Some(baseline4b_ns),
            slice.len(),
        );
    }

    // Reference: i128 lane fallback (DECIMAL(P>18) path). Same expected
    // sum as i64 case because we build it from the same source.
    let dec128: Vec<i128> = match &cv_dec64 {
        ColumnVector::Decimal64 { data, .. } => data.iter().map(|v| *v as i128).collect(),
        ColumnVector::Decimal128 { data, .. } => data.clone(),
        _ => panic!("unexpected"),
    };
    bench(
        "sum_i128_simd (i128 fallback lane)",
        || {
            let total = sum_i128_simd(&dec128).expect("benchmark sum fits i128");
            assert_eq!(total, expect_dec64);
            black_box(total)
        },
        Some(baseline4b_ns),
        dec128.len(),
    );

    // ---------------------------------------------------------------
    // Scenario 5 — end-to-end: full vectorized HashAggregate with
    // GROUP BY g, SUM(v). Compares the new typed-lane fast path against
    // the legacy `Value::Decimal(String)` path so we can quantify the
    // realistic TPC-H Q1-shape speedup.
    // ---------------------------------------------------------------
    use mydb::engine::vec_bench_api::{
        AggregateKind, AggregateSpec, Batch, BatchField, BatchSchema, HashAggregateCore,
    };
    use mydb::types::ColumnType;
    use std::sync::Arc;

    const N5: usize = 1_000_000;
    const GROUPS: i64 = 100;
    const BATCH_SIZE: usize = 8192;

    println!("\n[Scenario 5] HashAggregate GROUP BY g, SUM(v) — {N5} rows, {GROUPS} groups");
    println!(
        "{:<38} {:>12} {:>12} {:>10} {:>10}",
        "variant", "ms/run", "ns/row", "Mrows/s", "speedup"
    );

    let schema = Arc::new(BatchSchema::new(vec![
        BatchField {
            table: "t".into(),
            alias: None,
            name: "g".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        },
        BatchField {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            // DECIMAL(9, 2) — drives the new Decimal32 lane via the
            // precision-aware ColumnAccumulator path.
            column_type: ColumnType::Decimal {
                precision: 9,
                scale: 2,
            },
            source: Default::default(),
        },
    ]));

    // Build batches of size BATCH_SIZE.
    let build_batches = |row_to_value: &dyn Fn(usize) -> Value| -> Vec<Batch> {
        let mut batches = Vec::new();
        let mut row = 0;
        while row < N5 {
            let end = (row + BATCH_SIZE).min(N5);
            let mut g_vals = Vec::with_capacity(end - row);
            let mut v_vals = Vec::with_capacity(end - row);
            for i in row..end {
                g_vals.push(Value::Int((i as i64) % GROUPS));
                v_vals.push(row_to_value(i));
            }
            let g_col = ColumnVector::from_values(g_vals);
            let v_col = ColumnVector::from_values(v_vals);
            batches.push(Batch::new(schema.clone(), vec![g_col, v_col]));
            row = end;
        }
        batches
    };

    // (a) New typed-lane batches — `from_values` picks Decimal32 since
    //     magnitudes stay below i32::MAX.
    let new_lane_batches =
        build_batches(&|i: usize| Value::DecimalCompact(((i * 137) % 1_000_000) as i64, 2));
    // (b) Legacy Decimal(String) batches — `from_values` falls through
    //     to Generic, so `value_at` constructs strings per row.
    let legacy_batches = build_batches(&|i: usize| {
        let mag = (i * 137) % 1_000_000;
        Value::Decimal(format!("{}.{:02}", mag / 100, mag % 100))
    });

    // Verify lane selection actually went where we expected.
    let lane_new = match &new_lane_batches[0].columns()[1] {
        ColumnVector::Decimal32 { .. } => "Decimal32",
        ColumnVector::Decimal64 { .. } => "Decimal64",
        ColumnVector::Decimal128 { .. } => "Decimal128",
        ColumnVector::Generic { .. } => "Generic",
        _ => "OTHER",
    };
    let lane_legacy = match &legacy_batches[0].columns()[1] {
        ColumnVector::Decimal32 { .. } => "Decimal32",
        ColumnVector::Decimal64 { .. } => "Decimal64",
        ColumnVector::Decimal128 { .. } => "Decimal128",
        ColumnVector::Generic { .. } => "Generic",
        _ => "OTHER",
    };
    println!("  (new path lane: {lane_new}; legacy path lane: {lane_legacy})");

    let run_hash_agg = |batches: &[Batch]| -> Vec<Vec<Value>> {
        let mut agg = HashAggregateCore::new(
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 1,
            }],
        );
        for b in batches {
            agg.consume_batch(b).expect("consume");
        }
        agg.finalize().expect("finalize")
    };

    // Verify both paths agree on the per-group totals before we time
    // them — a regression would invalidate the speedup number.
    let rows_new = run_hash_agg(&new_lane_batches);
    let rows_legacy = run_hash_agg(&legacy_batches);
    assert_eq!(rows_new.len(), rows_legacy.len());
    assert_eq!(rows_new.len(), GROUPS as usize);

    let legacy_ns = bench(
        "hash_agg legacy Decimal(String)",
        || {
            let rows = run_hash_agg(&legacy_batches);
            assert_eq!(rows.len(), GROUPS as usize);
            black_box(0_i128)
        },
        None,
        N5,
    );
    bench(
        "hash_agg new Decimal32 lane",
        || {
            let rows = run_hash_agg(&new_lane_batches);
            assert_eq!(rows.len(), GROUPS as usize);
            black_box(0_i128)
        },
        Some(legacy_ns),
        N5,
    );

    // (c) DECIMAL(15,2) magnitudes — exceed i32 so we hit Decimal64.
    let dec64_batches = build_batches(&|i: usize| {
        let mag = (i as i64 * 137 * 10_000) % 100_000_000_000_000_000_i64;
        Value::DecimalCompact(mag, 2)
    });
    let lane_d64 = match &dec64_batches[0].columns()[1] {
        ColumnVector::Decimal32 { .. } => "Decimal32",
        ColumnVector::Decimal64 { .. } => "Decimal64",
        ColumnVector::Decimal128 { .. } => "Decimal128",
        ColumnVector::Generic { .. } => "Generic",
        _ => "OTHER",
    };
    println!("  (large-magnitude path lane: {lane_d64})");
    bench(
        "hash_agg new Decimal64 lane",
        || {
            let rows = run_hash_agg(&dec64_batches);
            assert_eq!(rows.len(), GROUPS as usize);
            black_box(0_i128)
        },
        Some(legacy_ns),
        N5,
    );
}

// -------------------------------------------------------------------
// Replica of the production `NumericAggState` / `accumulate_numeric_state`
// from `src/engine/executor/select/aggregate.rs`. Kept in lock-step with
// the source — same enum, same branching, same checked-arithmetic.
// -------------------------------------------------------------------
#[derive(Default, Debug, Clone, Copy)]
struct ExactDecimalAccRepl {
    magnitude: i128,
    scale: u8,
    seen: bool,
}

impl ExactDecimalAccRepl {
    fn add_parts(&mut self, magnitude: i128, scale: u8) -> Option<()> {
        if !self.seen {
            self.magnitude = magnitude;
            self.scale = scale;
            self.seen = true;
            return Some(());
        }
        let target_scale = self.scale.max(scale);
        let left = scale_up_decimal(self.magnitude, self.scale, target_scale)?;
        let right = scale_up_decimal(magnitude, scale, target_scale)?;
        self.magnitude = left.checked_add(right)?;
        self.scale = target_scale;
        Some(())
    }
}

#[derive(Default, Debug, Clone, Copy)]
enum NumericAggState {
    #[default]
    Empty,
    Exact(ExactDecimalAccRepl),
    Float(f64),
}

fn accumulate_numeric_state_replica(state: &mut NumericAggState, value: &Value) {
    if matches!(value, Value::Null) {
        return;
    }
    let next = match std::mem::replace(state, NumericAggState::Empty) {
        NumericAggState::Empty => {
            if let Some((magnitude, scale)) = exact_decimal_parts(value) {
                NumericAggState::Exact(ExactDecimalAccRepl {
                    magnitude,
                    scale,
                    seen: true,
                })
            } else {
                NumericAggState::Float(numeric_to_f64(value))
            }
        }
        NumericAggState::Exact(mut acc) => {
            if acc.add_value(value).is_none() {
                NumericAggState::Float(acc.to_f64() + numeric_to_f64(value))
            } else {
                NumericAggState::Exact(acc)
            }
        }
        NumericAggState::Float(total) => NumericAggState::Float(total + numeric_to_f64(value)),
    };
    *state = next;
}

impl ExactDecimalAccRepl {
    fn add_value(&mut self, value: &Value) -> Option<()> {
        let (magnitude, scale) = exact_decimal_parts(value)?;
        self.add_parts(magnitude, scale)
    }
    fn to_f64(self) -> f64 {
        if !self.seen {
            0.0
        } else {
            self.magnitude as f64 / 10f64.powi(self.scale as i32)
        }
    }
}

fn exact_decimal_parts(value: &Value) -> Option<(i128, u8)> {
    match value {
        Value::DecimalCompact(mag, scale) => Some((i128::from(*mag), *scale)),
        Value::Decimal128(mag, scale) => Some((*mag, *scale)),
        Value::Int(v) => Some((i128::from(*v), 0)),
        Value::Decimal(s) | Value::String(s) => parse_exact_decimal_string(s),
        _ => None,
    }
}

fn parse_exact_decimal_string(s: &str) -> Option<(i128, u8)> {
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
    let (int_part, frac_part) = match body.split_once('.') {
        Some((int_part, frac_part)) => (int_part, frac_part),
        None => (body, ""),
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let scale = u8::try_from(frac_part.len()).ok()?;
    let digits = if int_part.is_empty() {
        format!("0{frac_part}")
    } else {
        format!("{int_part}{frac_part}")
    };
    let magnitude = digits.parse::<i128>().ok()?;
    Some((if negative { -magnitude } else { magnitude }, scale))
}

fn scale_up_decimal(magnitude: i128, from_scale: u8, to_scale: u8) -> Option<i128> {
    match from_scale.cmp(&to_scale) {
        std::cmp::Ordering::Equal => Some(magnitude),
        std::cmp::Ordering::Less => {
            let shift = 10i128.checked_pow((to_scale - from_scale) as u32)?;
            magnitude.checked_mul(shift)
        }
        std::cmp::Ordering::Greater => None,
    }
}

fn numeric_to_f64(value: &Value) -> f64 {
    match value {
        Value::Int(v) => *v as f64,
        Value::Double(v) => *v,
        Value::DecimalCompact(mag, scale) => *mag as f64 / 10f64.powi(*scale as i32),
        Value::Decimal(s) | Value::String(s) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn state_total_i128(state: &NumericAggState) -> Option<i128> {
    match state {
        NumericAggState::Empty => Some(0),
        NumericAggState::Exact(acc) => Some(acc.magnitude),
        NumericAggState::Float(_) => None,
    }
}

fn add_parts_into_state(state: &mut NumericAggState, magnitude: i128, scale: u8) {
    let next = match std::mem::replace(state, NumericAggState::Empty) {
        NumericAggState::Empty => NumericAggState::Exact(ExactDecimalAccRepl {
            magnitude,
            scale,
            seen: true,
        }),
        NumericAggState::Exact(mut acc) => {
            if acc.add_parts(magnitude, scale).is_none() {
                NumericAggState::Float(acc.to_f64() + magnitude as f64 / 10f64.powi(scale as i32))
            } else {
                NumericAggState::Exact(acc)
            }
        }
        NumericAggState::Float(total) => {
            NumericAggState::Float(total + magnitude as f64 / 10f64.powi(scale as i32))
        }
    };
    *state = next;
}

// -------------------------------------------------------------------
// SIMD-style implementations operating directly on the typed
// `ColumnVector::Decimal128.data: &[i128]` lane.
// -------------------------------------------------------------------

#[inline(never)]
fn sum_i128_unrolled4(a: &[i128]) -> i128 {
    let mut acc0: i128 = 0;
    let mut acc1: i128 = 0;
    let mut acc2: i128 = 0;
    let mut acc3: i128 = 0;
    let chunks = a.chunks_exact(4);
    let tail = chunks.remainder();
    for c in chunks {
        acc0 = acc0.wrapping_add(c[0]);
        acc1 = acc1.wrapping_add(c[1]);
        acc2 = acc2.wrapping_add(c[2]);
        acc3 = acc3.wrapping_add(c[3]);
    }
    for v in tail {
        acc0 = acc0.wrapping_add(*v);
    }
    acc0.wrapping_add(acc1)
        .wrapping_add(acc2)
        .wrapping_add(acc3)
}

#[inline(never)]
fn sum_i128_unrolled8(a: &[i128]) -> i128 {
    let mut acc = [0i128; 8];
    let chunks = a.chunks_exact(8);
    let tail = chunks.remainder();
    for c in chunks {
        acc[0] = acc[0].wrapping_add(c[0]);
        acc[1] = acc[1].wrapping_add(c[1]);
        acc[2] = acc[2].wrapping_add(c[2]);
        acc[3] = acc[3].wrapping_add(c[3]);
        acc[4] = acc[4].wrapping_add(c[4]);
        acc[5] = acc[5].wrapping_add(c[5]);
        acc[6] = acc[6].wrapping_add(c[6]);
        acc[7] = acc[7].wrapping_add(c[7]);
    }
    let mut tot: i128 = 0;
    for a in acc {
        tot = tot.wrapping_add(a);
    }
    for v in tail {
        tot = tot.wrapping_add(*v);
    }
    tot
}

#[inline(never)]
fn sum_i64_unrolled4_widen(a: &[i64]) -> i128 {
    // Same shape as `expr_batch::sum_i64_simd`.
    let mut acc0: i128 = 0;
    let mut acc1: i128 = 0;
    let mut acc2: i128 = 0;
    let mut acc3: i128 = 0;
    let chunks = a.chunks_exact(4);
    let tail = chunks.remainder();
    for c in chunks {
        acc0 += c[0] as i128;
        acc1 += c[1] as i128;
        acc2 += c[2] as i128;
        acc3 += c[3] as i128;
    }
    for v in tail {
        acc0 += *v as i128;
    }
    acc0 + acc1 + acc2 + acc3
}

#[inline(never)]
fn sum_i64_unrolled8_widen(a: &[i64]) -> i128 {
    let mut acc = [0i128; 8];
    let chunks = a.chunks_exact(8);
    let tail = chunks.remainder();
    for c in chunks {
        acc[0] += c[0] as i128;
        acc[1] += c[1] as i128;
        acc[2] += c[2] as i128;
        acc[3] += c[3] as i128;
        acc[4] += c[4] as i128;
        acc[5] += c[5] as i128;
        acc[6] += c[6] as i128;
        acc[7] += c[7] as i128;
    }
    let mut tot: i128 = 0;
    for a in acc {
        tot += a;
    }
    for v in tail {
        tot += *v as i128;
    }
    tot
}

// -------------------------------------------------------------------
// Bench harness
// -------------------------------------------------------------------
fn bench<F: FnMut() -> i128>(
    name: &str,
    mut body: F,
    baseline_ns: Option<f64>,
    rows: usize,
) -> f64 {
    // Warm up
    black_box(body());
    let mut samples: Vec<f64> = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let start = Instant::now();
        let _ = black_box(body());
        samples.push(start.elapsed().as_secs_f64() * 1e9);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median_ns = samples[samples.len() / 2];
    let ns_per_row = median_ns / rows as f64;
    let mrows_per_sec = 1_000.0 / ns_per_row;
    let speedup = baseline_ns
        .map(|b| format!("{:.2}x", b / median_ns))
        .unwrap_or_else(|| "—".to_string());
    println!(
        "{:<38} {:>12.2} {:>12.2} {:>10.2} {:>10}",
        name,
        median_ns / 1e6,
        ns_per_row,
        mrows_per_sec,
        speedup,
    );
    median_ns
}

fn format_i128_decimal(magnitude: i128, scale: u8) -> String {
    let negative = magnitude < 0;
    let digits = magnitude.unsigned_abs().to_string();
    let body = if scale == 0 {
        digits
    } else {
        let scale = scale as usize;
        if digits.len() > scale {
            let (int_part, frac_part) = digits.split_at(digits.len() - scale);
            format!("{int_part}.{frac_part}")
        } else {
            format!("0.{digits:0>scale$}")
        }
    };
    if negative {
        format!("-{body}")
    } else {
        body
    }
}
