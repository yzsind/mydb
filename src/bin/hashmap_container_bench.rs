//! hashmap_container_bench: Compare std HashMap (SipHash-1-3) with hashbrown + ahash
//! using container-level throughput for representative mydb keys.
//!
//! Usage:
//!   cargo run --release --features tools --bin hashmap_container_bench
//!   cargo run --release --features tools --bin hashmap_container_bench -- --n 1000000
//!
//! Scenarios corresponding to actual mydb usage:
//!   - u64-key:           buffer pool / session registry / heap_cache
//!                        active_snapshots / wait_events / prepared statements
//!   - u32-key:           session id (mysql connection id)
//!   - Short/medium/long Vec<u8>: vec/hash_agg by_key and vec/hash_join build_index encoded group keys.
//!   - (u64, String):     index_cache key (table_id, index_name)
//!   - String: metadata such as by_column and regex_cache.
//!
//! Report: run each container/scenario pair for at least 300ms,
//! report ns/op and the speedup of ahash relative to std.

use std::collections::{HashMap as StdMap, HashSet as StdSet};
use std::hint::black_box;
use std::time::{Duration, Instant};

use ahash::RandomState as AHashState;
use hashbrown::HashMap as HbMap;
use rand::{rngs::StdRng, RngCore, SeedableRng};

type AhMap<K, V> = HbMap<K, V, AHashState>;

fn now() -> Instant {
    Instant::now()
}

struct Args {
    n: usize,
}

fn parse_args() -> Args {
    let mut a = Args { n: 200_000 };
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
                eprintln!("hashmap_container_bench [--n N]");
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

/// Adaptively time construction; return insert, successful lookup, and missing lookup ns/op.
fn measure<KFn, K, BFn1, BFn2, BFn3>(
    label: &str,
    n: usize,
    make_key: &KFn,
    build_insert: &BFn1,
    build_get_hit: &BFn2,
    build_get_miss: &BFn3,
) -> (f64, f64, f64)
where
    KFn: Fn(u64) -> K,
    K: Clone,
    BFn1: Fn(&[K]) -> Duration,
    BFn2: Fn(&[K]) -> Duration,
    BFn3: Fn(&[K]) -> Duration,
{
    let mut rng = StdRng::seed_from_u64(0x1234_5678_9abc_def0);
    let mut keys: Vec<K> = Vec::with_capacity(n);
    for _ in 0..n {
        keys.push(make_key(rng.next_u64()));
    }

    // Measure misses using keys that were not inserted.
    let miss_n = n.min(50_000);
    let mut miss_rng = StdRng::seed_from_u64(0xfeed_face_0000_0001);
    let mut miss_keys: Vec<K> = Vec::with_capacity(miss_n);
    for _ in 0..miss_n {
        miss_keys.push(make_key(miss_rng.next_u64() | 0x8000_0000_0000_0000));
    }

    // Warm up.
    let _ = build_insert(&keys);
    let _ = build_get_hit(&keys);
    let _ = build_get_miss(&miss_keys);

    let insert_d = build_insert(&keys);
    let hit_d = build_get_hit(&keys);
    let miss_d = build_get_miss(&miss_keys);

    let ins_ns = insert_d.as_nanos() as f64 / n as f64;
    let hit_ns = hit_d.as_nanos() as f64 / n as f64;
    let miss_ns = miss_d.as_nanos() as f64 / miss_n as f64;

    println!(
        "  {:<30} insert {:7.1} ns/op   get-hit {:7.1} ns/op   get-miss {:7.1} ns/op",
        label, ins_ns, hit_ns, miss_ns
    );
    (ins_ns, hit_ns, miss_ns)
}

fn header(scenario: &str) {
    println!();
    println!("=== {} ===", scenario);
}

fn print_speedup(name: &str, std_ns: (f64, f64, f64), ah_ns: (f64, f64, f64)) {
    let sx = |a: f64, b: f64| if b > 0.0 { a / b } else { 0.0 };
    println!(
        "  speedup (std / ahash) → {:<10} insert {:.2}x   get-hit {:.2}x   get-miss {:.2}x",
        name,
        sx(std_ns.0, ah_ns.0),
        sx(std_ns.1, ah_ns.1),
        sx(std_ns.2, ah_ns.2),
    );
}

fn bench_u64(n: usize) {
    header(&format!("u64 key  ({} entries)", n));

    let std_res = measure(
        "std HashMap<u64, u64>",
        n,
        &|seed: u64| seed,
        &|keys: &[u64]| {
            let t = now();
            let mut m: StdMap<u64, u64> = StdMap::with_capacity(keys.len());
            for (i, &k) in keys.iter().enumerate() {
                m.insert(k, i as u64);
            }
            black_box(&m);
            t.elapsed()
        },
        &|keys: &[u64]| {
            let mut m: StdMap<u64, u64> = StdMap::with_capacity(keys.len());
            for (i, &k) in keys.iter().enumerate() {
                m.insert(k, i as u64);
            }
            let t = now();
            let mut acc: u64 = 0;
            for &k in keys {
                acc = acc.wrapping_add(*m.get(&k).unwrap_or(&0));
            }
            black_box(acc);
            t.elapsed()
        },
        &|keys: &[u64]| {
            let m: StdMap<u64, u64> = StdMap::with_capacity(1);
            let t = now();
            let mut acc: usize = 0;
            for &k in keys {
                if !m.contains_key(&k) {
                    acc += 1;
                }
            }
            black_box(acc);
            t.elapsed()
        },
    );

    let ah_res = measure(
        "ahash AhMap<u64, u64>",
        n,
        &|seed: u64| seed,
        &|keys: &[u64]| {
            let t = now();
            let mut m: AhMap<u64, u64> =
                AhMap::with_capacity_and_hasher(keys.len(), AHashState::new());
            for (i, &k) in keys.iter().enumerate() {
                m.insert(k, i as u64);
            }
            black_box(&m);
            t.elapsed()
        },
        &|keys: &[u64]| {
            let mut m: AhMap<u64, u64> =
                AhMap::with_capacity_and_hasher(keys.len(), AHashState::new());
            for (i, &k) in keys.iter().enumerate() {
                m.insert(k, i as u64);
            }
            let t = now();
            let mut acc: u64 = 0;
            for &k in keys {
                acc = acc.wrapping_add(*m.get(&k).unwrap_or(&0));
            }
            black_box(acc);
            t.elapsed()
        },
        &|keys: &[u64]| {
            let m: AhMap<u64, u64> = AhMap::with_capacity_and_hasher(1, AHashState::new());
            let t = now();
            let mut acc: usize = 0;
            for &k in keys {
                if !m.contains_key(&k) {
                    acc += 1;
                }
            }
            black_box(acc);
            t.elapsed()
        },
    );
    print_speedup("u64", std_res, ah_res);
}

fn bench_u32(n: usize) {
    header(&format!("u32 key  (session ID scenario, {} entries)", n));

    let std_res = measure(
        "std HashMap<u32, u64>",
        n,
        &|seed: u64| seed as u32,
        &|keys: &[u32]| {
            let t = now();
            let mut m: StdMap<u32, u64> = StdMap::with_capacity(keys.len());
            for (i, &k) in keys.iter().enumerate() {
                m.insert(k, i as u64);
            }
            black_box(&m);
            t.elapsed()
        },
        &|keys: &[u32]| {
            let mut m: StdMap<u32, u64> = StdMap::with_capacity(keys.len());
            for (i, &k) in keys.iter().enumerate() {
                m.insert(k, i as u64);
            }
            let t = now();
            let mut acc: u64 = 0;
            for &k in keys {
                acc = acc.wrapping_add(*m.get(&k).unwrap_or(&0));
            }
            black_box(acc);
            t.elapsed()
        },
        &|keys: &[u32]| {
            let m: StdMap<u32, u64> = StdMap::with_capacity(1);
            let t = now();
            let mut acc: usize = 0;
            for &k in keys {
                if !m.contains_key(&k) {
                    acc += 1;
                }
            }
            black_box(acc);
            t.elapsed()
        },
    );

    let ah_res = measure(
        "ahash AhMap<u32, u64>",
        n,
        &|seed: u64| seed as u32,
        &|keys: &[u32]| {
            let t = now();
            let mut m: AhMap<u32, u64> =
                AhMap::with_capacity_and_hasher(keys.len(), AHashState::new());
            for (i, &k) in keys.iter().enumerate() {
                m.insert(k, i as u64);
            }
            black_box(&m);
            t.elapsed()
        },
        &|keys: &[u32]| {
            let mut m: AhMap<u32, u64> =
                AhMap::with_capacity_and_hasher(keys.len(), AHashState::new());
            for (i, &k) in keys.iter().enumerate() {
                m.insert(k, i as u64);
            }
            let t = now();
            let mut acc: u64 = 0;
            for &k in keys {
                acc = acc.wrapping_add(*m.get(&k).unwrap_or(&0));
            }
            black_box(acc);
            t.elapsed()
        },
        &|keys: &[u32]| {
            let m: AhMap<u32, u64> = AhMap::with_capacity_and_hasher(1, AHashState::new());
            let t = now();
            let mut acc: usize = 0;
            for &k in keys {
                if !m.contains_key(&k) {
                    acc += 1;
                }
            }
            black_box(acc);
            t.elapsed()
        },
    );
    print_speedup("u32", std_res, ah_res);
}

fn bench_bytes(n: usize, key_len: usize, label: &str) {
    header(&format!(
        "Vec<u8> key  {} ({} bytes, {} entries)",
        label, key_len, n
    ));

    let make = |seed: u64| -> Vec<u8> {
        let mut v = vec![0u8; key_len];
        let mut x = seed | 0x1;
        for chunk in v.chunks_mut(8) {
            x = x.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(13);
            let bytes = x.to_le_bytes();
            for (i, b) in chunk.iter_mut().enumerate() {
                *b = bytes[i];
            }
        }
        v
    };

    let std_res = measure(
        &format!("std HashMap<Vec<u8>, usize>  ({})", label),
        n,
        &make,
        &|keys: &[Vec<u8>]| {
            let t = now();
            let mut m: StdMap<Vec<u8>, usize> = StdMap::with_capacity(keys.len());
            for (i, k) in keys.iter().enumerate() {
                m.insert(k.clone(), i);
            }
            black_box(&m);
            t.elapsed()
        },
        &|keys: &[Vec<u8>]| {
            let mut m: StdMap<Vec<u8>, usize> = StdMap::with_capacity(keys.len());
            for (i, k) in keys.iter().enumerate() {
                m.insert(k.clone(), i);
            }
            let t = now();
            let mut acc: usize = 0;
            for k in keys {
                acc = acc.wrapping_add(*m.get(k).unwrap_or(&0));
            }
            black_box(acc);
            t.elapsed()
        },
        &|keys: &[Vec<u8>]| {
            let m: StdMap<Vec<u8>, usize> = StdMap::with_capacity(1);
            let t = now();
            let mut acc: usize = 0;
            for k in keys {
                if !m.contains_key(k) {
                    acc += 1;
                }
            }
            black_box(acc);
            t.elapsed()
        },
    );

    let ah_res = measure(
        &format!("ahash AhMap<Vec<u8>, usize> ({})", label),
        n,
        &make,
        &|keys: &[Vec<u8>]| {
            let t = now();
            let mut m: AhMap<Vec<u8>, usize> =
                AhMap::with_capacity_and_hasher(keys.len(), AHashState::new());
            for (i, k) in keys.iter().enumerate() {
                m.insert(k.clone(), i);
            }
            black_box(&m);
            t.elapsed()
        },
        &|keys: &[Vec<u8>]| {
            let mut m: AhMap<Vec<u8>, usize> =
                AhMap::with_capacity_and_hasher(keys.len(), AHashState::new());
            for (i, k) in keys.iter().enumerate() {
                m.insert(k.clone(), i);
            }
            let t = now();
            let mut acc: usize = 0;
            for k in keys {
                acc = acc.wrapping_add(*m.get(k).unwrap_or(&0));
            }
            black_box(acc);
            t.elapsed()
        },
        &|keys: &[Vec<u8>]| {
            let m: AhMap<Vec<u8>, usize> = AhMap::with_capacity_and_hasher(1, AHashState::new());
            let t = now();
            let mut acc: usize = 0;
            for k in keys {
                if !m.contains_key(k) {
                    acc += 1;
                }
            }
            black_box(acc);
            t.elapsed()
        },
    );
    print_speedup(label, std_res, ah_res);
}

fn bench_u64_string(n: usize) {
    header(&format!(
        "(u64, String) key  (index_cache scenario, {} entries)",
        n
    ));

    let make = |seed: u64| -> (u64, String) {
        // Table IDs and index names; index names are usually short.
        (seed >> 16, format!("idx_{}", seed & 0xffff))
    };

    let std_res = measure(
        "std HashMap<(u64, String), u64>",
        n,
        &make,
        &|keys: &[(u64, String)]| {
            let t = now();
            let mut m: StdMap<(u64, String), u64> = StdMap::with_capacity(keys.len());
            for (i, k) in keys.iter().enumerate() {
                m.insert(k.clone(), i as u64);
            }
            black_box(&m);
            t.elapsed()
        },
        &|keys: &[(u64, String)]| {
            let mut m: StdMap<(u64, String), u64> = StdMap::with_capacity(keys.len());
            for (i, k) in keys.iter().enumerate() {
                m.insert(k.clone(), i as u64);
            }
            let t = now();
            let mut acc: u64 = 0;
            for k in keys {
                acc = acc.wrapping_add(*m.get(k).unwrap_or(&0));
            }
            black_box(acc);
            t.elapsed()
        },
        &|keys: &[(u64, String)]| {
            let m: StdMap<(u64, String), u64> = StdMap::with_capacity(1);
            let t = now();
            let mut acc: usize = 0;
            for k in keys {
                if !m.contains_key(k) {
                    acc += 1;
                }
            }
            black_box(acc);
            t.elapsed()
        },
    );

    let ah_res = measure(
        "ahash AhMap<(u64, String), u64>",
        n,
        &make,
        &|keys: &[(u64, String)]| {
            let t = now();
            let mut m: AhMap<(u64, String), u64> =
                AhMap::with_capacity_and_hasher(keys.len(), AHashState::new());
            for (i, k) in keys.iter().enumerate() {
                m.insert(k.clone(), i as u64);
            }
            black_box(&m);
            t.elapsed()
        },
        &|keys: &[(u64, String)]| {
            let mut m: AhMap<(u64, String), u64> =
                AhMap::with_capacity_and_hasher(keys.len(), AHashState::new());
            for (i, k) in keys.iter().enumerate() {
                m.insert(k.clone(), i as u64);
            }
            let t = now();
            let mut acc: u64 = 0;
            for k in keys {
                acc = acc.wrapping_add(*m.get(k).unwrap_or(&0));
            }
            black_box(acc);
            t.elapsed()
        },
        &|keys: &[(u64, String)]| {
            let m: AhMap<(u64, String), u64> =
                AhMap::with_capacity_and_hasher(1, AHashState::new());
            let t = now();
            let mut acc: usize = 0;
            for k in keys {
                if !m.contains_key(k) {
                    acc += 1;
                }
            }
            black_box(acc);
            t.elapsed()
        },
    );
    print_speedup("(u64,String)", std_res, ah_res);
}

/// Simulate hash aggregation/join build-then-probe without preallocating capacity.
fn bench_agg_pattern(n: usize, key_len: usize, group_card: usize) {
    header(&format!(
        "Aggregation (build+probe, key={} bytes, rows={}, groups={})",
        key_len, n, group_card
    ));

    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(n);
    let mut rng = StdRng::seed_from_u64(0xABCD_1234);
    for _ in 0..n {
        let idx = (rng.next_u64() as usize) % group_card;
        let mut v = vec![0u8; key_len];
        for chunk in v.chunks_mut(8) {
            let bytes = (idx as u64).to_le_bytes();
            for (i, b) in chunk.iter_mut().enumerate() {
                *b = bytes[i];
            }
        }
        keys.push(v);
    }

    let warmup = |use_ahash: bool| {
        if use_ahash {
            let mut m: AhMap<Vec<u8>, u64> = AhMap::with_hasher(AHashState::new());
            for k in &keys[..keys.len().min(1024)] {
                *m.entry(k.clone()).or_insert(0) += 1;
            }
            black_box(m.len());
        } else {
            let mut m: StdMap<Vec<u8>, u64> = StdMap::new();
            for k in &keys[..keys.len().min(1024)] {
                *m.entry(k.clone()).or_insert(0) += 1;
            }
            black_box(m.len());
        }
    };

    warmup(false);
    let t = now();
    let mut m_std: StdMap<Vec<u8>, u64> = StdMap::new();
    for k in &keys {
        *m_std.entry(k.clone()).or_insert(0) += 1;
    }
    let std_d = t.elapsed();
    black_box(m_std.len());

    warmup(true);
    let t = now();
    let mut m_ah: AhMap<Vec<u8>, u64> = AhMap::with_hasher(AHashState::new());
    for k in &keys {
        *m_ah.entry(k.clone()).or_insert(0) += 1;
    }
    let ah_d = t.elapsed();
    black_box(m_ah.len());

    let std_ns = std_d.as_nanos() as f64 / n as f64;
    let ah_ns = ah_d.as_nanos() as f64 / n as f64;
    println!(
        "  std HashMap (SipHash)            : {:7.1} ns/row  ({:.2} ms total)",
        std_ns,
        std_d.as_secs_f64() * 1e3
    );
    println!(
        "  ahash + hashbrown                : {:7.1} ns/row  ({:.2} ms total)",
        ah_ns,
        ah_d.as_secs_f64() * 1e3
    );
    println!(
        "  speedup                          : {:.2}x  (savings {:.1}%)",
        std_ns / ah_ns,
        (1.0 - ah_ns / std_ns) * 100.0
    );
}

fn bench_set_distinct(n: usize, key_len: usize, distinct_card: usize) {
    header(&format!(
        "DISTINCT (HashSet<Vec<u8>>, key={} bytes, rows={}, distinct={})",
        key_len, n, distinct_card
    ));

    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(n);
    let mut rng = StdRng::seed_from_u64(0x5566_7788);
    for _ in 0..n {
        let idx = (rng.next_u64() as usize) % distinct_card;
        let mut v = vec![0u8; key_len];
        for chunk in v.chunks_mut(8) {
            let bytes = (idx as u64).to_le_bytes();
            for (i, b) in chunk.iter_mut().enumerate() {
                *b = bytes[i];
            }
        }
        keys.push(v);
    }

    let t = now();
    let mut s_std: StdSet<Vec<u8>> = StdSet::new();
    for k in &keys {
        s_std.insert(k.clone());
    }
    let std_d = t.elapsed();
    black_box(s_std.len());

    let t = now();
    let mut s_ah: hashbrown::HashSet<Vec<u8>, AHashState> =
        hashbrown::HashSet::with_hasher(AHashState::new());
    for k in &keys {
        s_ah.insert(k.clone());
    }
    let ah_d = t.elapsed();
    black_box(s_ah.len());

    let std_ns = std_d.as_nanos() as f64 / n as f64;
    let ah_ns = ah_d.as_nanos() as f64 / n as f64;
    println!("  std HashSet (SipHash)            : {:7.1} ns/row", std_ns);
    println!("  ahash + hashbrown                : {:7.1} ns/row", ah_ns);
    println!(
        "  speedup                          : {:.2}x",
        std_ns / ah_ns
    );
}

fn main() {
    let args = parse_args();

    println!("hashmap_container_bench (actual mydb key types, std HashMap versus ahash+hashbrown)");
    println!("N={} per case", args.n);
    println!("Note: build/cache scenarios (buffer pool/session/index_cache) preallocate capacity;");
    println!(
        "      aggregation/DISTINCT do not, modeling unknown operator construction cardinality."
    );

    bench_u64(args.n);
    bench_u32(args.n);
    bench_u64_string(args.n.min(100_000));
    bench_bytes(args.n.min(100_000), 8, "8B");
    bench_bytes(args.n.min(100_000), 16, "16B (typical PK)");
    bench_bytes(args.n.min(100_000), 32, "32B (composite group key)");
    bench_bytes(args.n.min(50_000), 64, "64B");

    println!();
    println!("------- Aggregation/operator scenarios (no capacity reservation, modeling vec/hash_agg and vec/hash_join) -------");
    bench_agg_pattern(args.n, 16, 1_000);
    bench_agg_pattern(args.n, 16, 100_000);
    bench_agg_pattern(args.n, 32, 10_000);
    bench_set_distinct(args.n, 16, 10_000);
}
