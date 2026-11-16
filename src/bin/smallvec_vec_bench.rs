//! smallvec_vec_bench: Compare Vec and SmallVec overhead at actual mydb hotspots.
//!
//! Usage:
//!   cargo run --release --features tools --bin smallvec_vec_bench
//!   cargo run --release --features tools --bin smallvec_vec_bench -- --n 2000000
//!
//! Scenarios corresponding to actual mydb usage:
//!   - Short-lived push/drop containers in hot loops, typically holding 0-4 elements.
//!   - Collect field_indices into SmallVec<[usize; 2]> in src/engine/executor/select/plan.rs.
//!   - hash join build payload:    SmallVec<[u32; 1]>    src/engine/executor/vec/hash_join.rs
//!   - row_codec dest_of:          SmallVec<[i16; 32]>   src/storage/core/row_codec.rs
//!   - cache deps:                 SmallVec<[(u64,u64); 4]>  src/engine/cache.rs
//!   - Spill beyond inline capacity to exercise worst-case heap allocation.
//!
//! Run each case ITERS times after one warmup; report ns/op and Vec/SmallVec speedup.
//! Values >1 favor SmallVec; values <1 favor Vec.

use std::hint::black_box;
use std::time::{Duration, Instant};

use ahash::RandomState as AHashState;
use hashbrown::HashMap as HbMap;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use smallvec::SmallVec;

fn now() -> Instant {
    Instant::now()
}

struct Args {
    n: usize,
}

fn parse_args() -> Args {
    let mut a = Args { n: 1_000_000 };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--n" => {
                a.n = it
                    .next()
                    .expect("--n requires a positive integer")
                    .parse()
                    .expect("--n must be a positive integer");
            }
            "-h" | "--help" => {
                eprintln!("smallvec_vec_bench [--n N]");
                std::process::exit(0);
            }
            x => {
                eprintln!("Unknown argument: {}", x);
                std::process::exit(2);
            }
        }
    }
    a
}

fn header(scenario: &str) {
    println!();
    println!("=== {} ===", scenario);
}

fn print_pair(label: &str, vec_ns: f64, sv_ns: f64) {
    let speedup = if sv_ns > 0.0 { vec_ns / sv_ns } else { 0.0 };
    let winner = if speedup > 1.05 {
        "SmallVec"
    } else if speedup < 0.95 {
        "Vec"
    } else {
        "Equal"
    };
    println!(
        "  {:<42}  Vec {:7.1} ns   SmallVec {:7.1} ns   speedup {:.2}x  [{}]",
        label, vec_ns, sv_ns, speedup, winner
    );
}

// -------- Scenario 1: push 0-k i64 values into a temporary container, then drop --------

fn bench_temp_push<const K: usize>(n: usize, payload: usize) {
    // Vec
    let t = now();
    let mut sink: u64 = 0;
    for i in 0..n {
        let mut v: Vec<i64> = Vec::new();
        for j in 0..payload {
            v.push((i as i64) ^ (j as i64));
        }
        sink = sink.wrapping_add(v.last().copied().unwrap_or(0) as u64);
        black_box(&v);
    }
    black_box(sink);
    let vec_d = t.elapsed();

    // SmallVec<[i64; K]>
    let t = now();
    let mut sink: u64 = 0;
    for i in 0..n {
        let mut v: SmallVec<[i64; K]> = SmallVec::new();
        for j in 0..payload {
            v.push((i as i64) ^ (j as i64));
        }
        sink = sink.wrapping_add(v.last().copied().unwrap_or(0) as u64);
        black_box(&v);
    }
    black_box(sink);
    let sv_d = t.elapsed();

    let label = format!("tmp push {} i64 → drop  (SmallVec inline = {})", payload, K);
    print_pair(&label, ns_per(vec_d, n), ns_per(sv_d, n));
}

fn ns_per(d: Duration, n: usize) -> f64 {
    d.as_nanos() as f64 / n as f64
}

// -------- Scenario 2: collect field_indices, usually 1-2 usize values --------

fn bench_field_indices(n: usize) {
    // Simulate plan.rs::by_column: usually one element, occasionally two, rarely four or more.
    // distribution: 70% × 1, 20% × 2, 8% × 3, 2% × 6
    let dist: &[usize] = &[1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 6];
    let pick = |i: usize| dist[i % dist.len()];

    let t = now();
    let mut sink: usize = 0;
    for i in 0..n {
        let k = pick(i);
        let mut v: Vec<usize> = Vec::new();
        for j in 0..k {
            v.push(i + j);
        }
        sink = sink.wrapping_add(v.iter().sum::<usize>());
        black_box(&v);
    }
    black_box(sink);
    let vec_d = t.elapsed();

    let t = now();
    let mut sink: usize = 0;
    for i in 0..n {
        let k = pick(i);
        let mut v: SmallVec<[usize; 2]> = SmallVec::new();
        for j in 0..k {
            v.push(i + j);
        }
        sink = sink.wrapping_add(v.iter().sum::<usize>());
        black_box(&v);
    }
    black_box(sink);
    let sv_d = t.elapsed();

    print_pair(
        "field_indices  (Vec<usize> vs SmallVec<[usize;2]>)",
        ns_per(vec_d, n),
        ns_per(sv_d, n),
    );
}

// -------- Scenario 3: hash join build payload (HashMap<K, container<u32>>) --------

fn bench_hash_join_build(n: usize, distinct_keys: usize) {
    // Simulate vec/hash_join.rs:
    //   HashMap<Vec<u8>, SmallVec<[u32; 1]>> usually receives one row_id per key.
    // Occasionally a duplicate key has two or three values.
    let mut rng = StdRng::seed_from_u64(0x4d59_4442_5f4a_4f4e);
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(n);
    for _ in 0..n {
        let v = rng.next_u64() % (distinct_keys as u64);
        keys.push(v.to_le_bytes().to_vec());
    }

    // Vec<u32> payload
    let t = now();
    let mut map: HbMap<Vec<u8>, Vec<u32>, AHashState> =
        HbMap::with_capacity_and_hasher(distinct_keys, AHashState::new());
    for (i, k) in keys.iter().enumerate() {
        map.entry(k.clone()).or_default().push(i as u32);
    }
    let vec_d = t.elapsed();
    let vec_groups = map.len();
    black_box(map);

    // SmallVec<[u32; 1]> payload
    let t = now();
    let mut map: HbMap<Vec<u8>, SmallVec<[u32; 1]>, AHashState> =
        HbMap::with_capacity_and_hasher(distinct_keys, AHashState::new());
    for (i, k) in keys.iter().enumerate() {
        map.entry(k.clone()).or_default().push(i as u32);
    }
    let sv_d = t.elapsed();
    let sv_groups = map.len();
    black_box(map);

    assert_eq!(vec_groups, sv_groups);
    let label = format!(
        "hash-join build  (n={}, distinct={}, dup={:.2})",
        n,
        distinct_keys,
        n as f64 / distinct_keys as f64
    );
    print_pair(&label, ns_per(vec_d, n), ns_per(sv_d, n));
}

// -------- Scenario 4: row codec dest_of (SmallVec<[i16; 32]>, length = needed) --------

fn bench_row_codec_dest_of(n: usize, needed: usize) {
    // Simulate row_codec.rs:
    // Initialize smallvec![-1; needed], then set dest_of[src_idx] = some_value.
    let t = now();
    let mut sink: i64 = 0;
    for i in 0..n {
        let mut dest_of: Vec<i16> = vec![-1; needed];
        for j in 0..needed {
            dest_of[j] = ((i + j) & 0x7fff) as i16;
        }
        sink = sink.wrapping_add(dest_of[needed - 1] as i64);
        black_box(&dest_of);
    }
    black_box(sink);
    let vec_d = t.elapsed();

    let t = now();
    let mut sink: i64 = 0;
    for i in 0..n {
        let mut dest_of: SmallVec<[i16; 32]> = smallvec::smallvec![-1; needed];
        for j in 0..needed {
            dest_of[j] = ((i + j) & 0x7fff) as i16;
        }
        sink = sink.wrapping_add(dest_of[needed - 1] as i64);
        black_box(&dest_of);
    }
    black_box(sink);
    let sv_d = t.elapsed();

    let label = format!(
        "row_codec dest_of  (needed={}, inline=32) {}",
        needed,
        if needed <= 32 { "[inline]" } else { "[spill]" }
    );
    print_pair(&label, ns_per(vec_d, n), ns_per(sv_d, n));
}

// -------- Scenario 5: cache deps (SmallVec<[(u64,u64); 4]>) --------

fn bench_cache_deps(n: usize) {
    // Simulate cache.rs::collect_plan_deps: single-table SELECT usually has one dependency.
    // Two-table JOIN has two, three tables have three, and complex queries occasionally exceed four.
    let dist: &[usize] = &[1, 1, 1, 1, 1, 2, 2, 3, 5];
    let pick = |i: usize| dist[i % dist.len()];

    let t = now();
    let mut sink: u64 = 0;
    for i in 0..n {
        let k = pick(i);
        let mut deps: Vec<(u64, u64)> = Vec::new();
        for j in 0..k {
            deps.push((i as u64, j as u64));
        }
        sink = sink.wrapping_add(deps.last().map(|d| d.0 ^ d.1).unwrap_or(0));
        black_box(&deps);
    }
    black_box(sink);
    let vec_d = t.elapsed();

    let t = now();
    let mut sink: u64 = 0;
    for i in 0..n {
        let k = pick(i);
        let mut deps: SmallVec<[(u64, u64); 4]> = SmallVec::new();
        for j in 0..k {
            deps.push((i as u64, j as u64));
        }
        sink = sink.wrapping_add(deps.last().map(|d| d.0 ^ d.1).unwrap_or(0));
        black_box(&deps);
    }
    black_box(sink);
    let sv_d = t.elapsed();

    print_pair(
        "cache deps  (Vec vs SmallVec<[(u64,u64);4]>)",
        ns_per(vec_d, n),
        ns_per(sv_d, n),
    );
}

// -------- Scenario 6: push far beyond inline capacity to test worst-case overflow --------

fn bench_overflow<const K: usize>(n: usize, payload: usize) {
    let t = now();
    let mut sink: u64 = 0;
    for i in 0..n {
        let mut v: Vec<u32> = Vec::new();
        for j in 0..payload {
            v.push((i ^ j) as u32);
        }
        sink = sink.wrapping_add(v[payload - 1] as u64);
        black_box(&v);
    }
    black_box(sink);
    let vec_d = t.elapsed();

    let t = now();
    let mut sink: u64 = 0;
    for i in 0..n {
        let mut v: SmallVec<[u32; K]> = SmallVec::new();
        for j in 0..payload {
            v.push((i ^ j) as u32);
        }
        sink = sink.wrapping_add(v[payload - 1] as u64);
        black_box(&v);
    }
    black_box(sink);
    let sv_d = t.elapsed();

    let label = format!(
        "overflow  ({} push, inline={}, {})",
        payload,
        K,
        if payload <= K { "inline" } else { "spilled" }
    );
    print_pair(&label, ns_per(vec_d, n), ns_per(sv_d, n));
}

// -------- Scenario 7: create only an empty container as a zero-work baseline --------

fn bench_create_empty<const K: usize>(n: usize) {
    let t = now();
    let mut sink: usize = 0;
    for _ in 0..n {
        let v: Vec<i64> = Vec::new();
        sink = sink.wrapping_add(v.capacity());
        black_box(&v);
    }
    black_box(sink);
    let vec_d = t.elapsed();

    let t = now();
    let mut sink: usize = 0;
    for _ in 0..n {
        let v: SmallVec<[i64; K]> = SmallVec::new();
        sink = sink.wrapping_add(v.capacity());
        black_box(&v);
    }
    black_box(sink);
    let sv_d = t.elapsed();

    print_pair(
        &format!("create empty + drop  (inline={})", K),
        ns_per(vec_d, n),
        ns_per(sv_d, n),
    );
}

// -------- Scenario 8: clone overhead, including hot-path hits_snapshot.clone() --------

fn bench_clone(n: usize, payload: usize) {
    // Simulate hash_join.rs:454: hits_snapshot: SmallVec<[u32; 1]> = build_index.get(...).clone().
    let mut src_vec: Vec<u32> = Vec::with_capacity(payload);
    let mut src_sv: SmallVec<[u32; 1]> = SmallVec::with_capacity(payload);
    for j in 0..payload {
        src_vec.push(j as u32);
        src_sv.push(j as u32);
    }

    let t = now();
    let mut sink: u32 = 0;
    for _ in 0..n {
        let c: Vec<u32> = src_vec.clone();
        sink = sink.wrapping_add(*c.last().unwrap_or(&0));
        black_box(&c);
    }
    black_box(sink);
    let vec_d = t.elapsed();

    let t = now();
    let mut sink: u32 = 0;
    for _ in 0..n {
        let c: SmallVec<[u32; 1]> = src_sv.clone();
        sink = sink.wrapping_add(*c.last().unwrap_or(&0));
        black_box(&c);
    }
    black_box(sink);
    let sv_d = t.elapsed();

    let label = format!(
        "clone source ({} elem) {}",
        payload,
        if payload <= 1 {
            "[inline]"
        } else {
            "[spilled]"
        }
    );
    print_pair(&label, ns_per(vec_d, n), ns_per(sv_d, n));
}

// -------- Scenario 9: container sizes and stack footprint --------

fn print_sizes() {
    use std::mem::size_of;
    println!();
    println!("=== Container stack sizes (bytes) ===");
    println!(
        "  Vec<i64>                       : {}",
        size_of::<Vec<i64>>()
    );
    println!(
        "  SmallVec<[i64; 1]>             : {}",
        size_of::<SmallVec<[i64; 1]>>()
    );
    println!(
        "  SmallVec<[i64; 2]>             : {}",
        size_of::<SmallVec<[i64; 2]>>()
    );
    println!(
        "  SmallVec<[i64; 4]>             : {}",
        size_of::<SmallVec<[i64; 4]>>()
    );
    println!(
        "  SmallVec<[i64; 8]>             : {}",
        size_of::<SmallVec<[i64; 8]>>()
    );
    println!(
        "  SmallVec<[u32; 1]>             : {}",
        size_of::<SmallVec<[u32; 1]>>()
    );
    println!(
        "  SmallVec<[i16; 32]>            : {}",
        size_of::<SmallVec<[i16; 32]>>()
    );
    println!(
        "  SmallVec<[(u64,u64); 4]>       : {}",
        size_of::<SmallVec<[(u64, u64); 4]>>()
    );
    println!(
        "  SmallVec<[u8; 22]> (KeyBytes)  : {}",
        size_of::<SmallVec<[u8; 22]>>()
    );
}

fn main() {
    let args = parse_args();

    println!("smallvec_vec_bench (Vec versus smallvec::SmallVec at actual mydb hotspots)");
    println!("Default N = {} iterations per case", args.n);
    println!("Rule: speedup = Vec / SmallVec; >1 means SmallVec is faster");

    print_sizes();

    header("1) Create and drop an empty container (zero-work baseline)");
    bench_create_empty::<1>(args.n);
    bench_create_empty::<4>(args.n);
    bench_create_empty::<32>(args.n);

    header("2) Push k temporary i64 values, then drop (short-lived hot-loop containers)");
    // Fits inline.
    bench_temp_push::<2>(args.n, 0);
    bench_temp_push::<2>(args.n, 1);
    bench_temp_push::<2>(args.n, 2);
    bench_temp_push::<4>(args.n, 3);
    bench_temp_push::<4>(args.n, 4);
    bench_temp_push::<8>(args.n, 6);
    bench_temp_push::<8>(args.n, 8);

    header("3) Collect field_indices (plan.rs by_column pattern)");
    bench_field_indices(args.n);

    header("4) row_codec dest_of (SmallVec<[i16; 32]>, smallvec! initialization)");
    bench_row_codec_dest_of(args.n / 4, 8); // inline
    bench_row_codec_dest_of(args.n / 4, 16); // inline
    bench_row_codec_dest_of(args.n / 4, 32); // At the inline capacity boundary.
    bench_row_codec_dest_of(args.n / 4, 64); // spill

    header("5) cache deps  (SmallVec<[(u64,u64); 4]>)");
    bench_cache_deps(args.n);

    header("6) hash-join build payload  (HashMap<Vec<u8>, container<u32>>)");
    bench_hash_join_build(args.n / 4, args.n / 4); // One element per key; almost all fit inline.
    bench_hash_join_build(args.n / 4, args.n / 8); // Approximately two per key; commonly spills.
    bench_hash_join_build(args.n / 4, args.n / 32); // Approximately eight per key; many duplicates.

    header("7) Overflow (push beyond inline capacity to test worst cases)");
    bench_overflow::<1>(args.n, 4); // Inline capacity 1; use 4.
    bench_overflow::<4>(args.n, 16); // Inline capacity 4; use 16.
    bench_overflow::<8>(args.n, 64); // Inline capacity 8; use 64.
    bench_overflow::<32>(args.n / 4, 128); // Inline capacity 32; use 128.

    header("8) Clone (hash_join.rs hits_snapshot pattern)");
    bench_clone(args.n, 1); // Fits inline in SmallVec<[u32; 1]>.
    bench_clone(args.n, 2); // Already spilled (>1).
    bench_clone(args.n, 8);
    bench_clone(args.n, 32);

    println!();
    println!("Notes:");
    println!(
        "  - Inline SmallVec stays on the stack, benefiting from zero allocations and locality"
    );
    println!("  - After spilling, SmallVec resembles Vec plus a tag and may be slightly slower");
    println!("  - Avoiding deallocation on drop amplifies the benefit for short-lived containers");
    println!("  - SmallVec occupies more stack space than Vec; consider cache footprint for struct fields");
}
