//! hash_bench: Measure performance and collision probability of ahash, SipHash-1-3, FNV-1a, SHA-256, MD5, and CRC32.
//!
//! Usage:
//!   cargo run --release --features tools --bin hash_bench
//!   cargo run --release --features tools --bin hash_bench -- --collision-n 2000000
//!   cargo run --release --features tools --bin hash_bench -- --sizes 8,64,1024 --no-collision
//!
//! Performance: run each algorithm/key-size pair for >=200ms; report ns/op and MB/s.
//! Collisions: generate N unique random strings and truncate each 64-bit hash to 16/24/32 bits,
//! then compare observed collisions with the birthday expectation N(N-1)/(2*2^w).
//!
//! SipHash-1-3 comes from std::collections::hash_map::DefaultHasher.
//! Use the first eight SHA-256/MD5 digest bytes as a 64-bit scalar, a common truncation method.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{BuildHasher, Hasher};
use std::hint::black_box;
use std::time::{Duration, Instant};

use ahash::RandomState as AHashState;
use crc32fast::Hasher as Crc32Hasher;
use md5::{Digest as _, Md5};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use sha2::Sha256;

trait HashAlgo: Sync {
    fn name(&self) -> &'static str;
    fn hash_u64(&self, data: &[u8]) -> u64;
}

struct AHashAlgo {
    state: AHashState,
}
impl AHashAlgo {
    fn new() -> Self {
        Self {
            state: AHashState::with_seeds(0xa5a5_a5a5, 0x5a5a_5a5a, 0xdead_beef, 0xc0ffee),
        }
    }
}
impl HashAlgo for AHashAlgo {
    fn name(&self) -> &'static str {
        "ahash"
    }
    fn hash_u64(&self, d: &[u8]) -> u64 {
        let mut h = self.state.build_hasher();
        h.write(d);
        h.finish()
    }
}

struct SipHashAlgo;
impl HashAlgo for SipHashAlgo {
    fn name(&self) -> &'static str {
        "siphash13"
    }
    fn hash_u64(&self, d: &[u8]) -> u64 {
        let mut h = DefaultHasher::new();
        h.write(d);
        h.finish()
    }
}

struct Fnv1aAlgo;
impl HashAlgo for Fnv1aAlgo {
    fn name(&self) -> &'static str {
        "fnv1a64"
    }
    fn hash_u64(&self, d: &[u8]) -> u64 {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;
        let mut h = OFFSET;
        for &b in d {
            h ^= b as u64;
            h = h.wrapping_mul(PRIME);
        }
        h
    }
}

struct Sha256Algo;
impl HashAlgo for Sha256Algo {
    fn name(&self) -> &'static str {
        "sha256"
    }
    fn hash_u64(&self, d: &[u8]) -> u64 {
        let mut h = Sha256::new();
        h.update(d);
        let out = h.finalize();
        u64::from_le_bytes(out[..8].try_into().unwrap())
    }
}

struct Md5Algo;
impl HashAlgo for Md5Algo {
    fn name(&self) -> &'static str {
        "md5"
    }
    fn hash_u64(&self, d: &[u8]) -> u64 {
        let mut h = Md5::new();
        h.update(d);
        let out = h.finalize();
        u64::from_le_bytes(out[..8].try_into().unwrap())
    }
}

struct Crc32Algo;
impl HashAlgo for Crc32Algo {
    fn name(&self) -> &'static str {
        "crc32"
    }
    fn hash_u64(&self, d: &[u8]) -> u64 {
        let mut h = Crc32Hasher::new();
        h.update(d);
        h.finalize() as u64
    }
}

fn time_one(algo: &dyn HashAlgo, data: &[u8]) -> f64 {
    // Warm up.
    let mut warm: u64 = 0;
    for _ in 0..4096 {
        warm = warm.wrapping_add(algo.hash_u64(black_box(data)));
    }
    black_box(warm);

    let target = Duration::from_millis(200);
    let mut iters: usize = 2048;
    loop {
        let start = Instant::now();
        let mut acc: u64 = 0;
        for _ in 0..iters {
            acc = acc.wrapping_add(algo.hash_u64(black_box(data)));
        }
        let elapsed = start.elapsed();
        black_box(acc);
        if elapsed >= target {
            return elapsed.as_nanos() as f64 / iters as f64;
        }
        if iters >= 200_000_000 {
            return elapsed.as_nanos() as f64 / iters as f64;
        }
        iters = iters.saturating_mul(4);
    }
}

fn fmt_size(n: usize) -> String {
    if n >= 1024 && n % 1024 == 0 {
        format!("{}KiB", n / 1024)
    } else {
        format!("{}B", n)
    }
}

fn bench_speed(algos: &[Box<dyn HashAlgo>], sizes: &[usize]) {
    println!();
    println!("=== Performance tests ===");
    println!("Adapt each algorithm/size pair to run >=200ms");
    println!();

    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF);
    let mut ns_table: Vec<Vec<f64>> = Vec::with_capacity(sizes.len());

    for &size in sizes {
        let mut data = vec![0u8; size];
        rng.fill_bytes(&mut data);
        let mut row = Vec::with_capacity(algos.len());
        for a in algos {
            row.push(time_one(a.as_ref(), &data));
        }
        ns_table.push(row);
    }

    let col_w = 12;
    print!("{:<10}", "ns/op");
    for a in algos {
        print!(" {:>w$}", a.name(), w = col_w);
    }
    println!();
    for (i, &size) in sizes.iter().enumerate() {
        print!("{:<10}", fmt_size(size));
        for &ns in &ns_table[i] {
            print!(" {:>w$.2}", ns, w = col_w);
        }
        println!();
    }

    println!();
    print!("{:<10}", "MB/s");
    for a in algos {
        print!(" {:>w$}", a.name(), w = col_w);
    }
    println!();
    for (i, &size) in sizes.iter().enumerate() {
        print!("{:<10}", fmt_size(size));
        for &ns in &ns_table[i] {
            let mb = size as f64 / (ns / 1e9) / 1e6;
            print!(" {:>w$.1}", mb, w = col_w);
        }
        println!();
    }
}

fn bench_collision(algos: &[Box<dyn HashAlgo>], n: usize, key_size: usize) {
    println!();
    println!("=== Collision tests ===");
    println!(
        "Generate {} unique random {}-byte strings; truncate each 64-bit hash to 16/24/32 bits and count duplicates",
        n, key_size
    );
    println!();

    let mut rng = StdRng::seed_from_u64(0xC0FF_EE42);
    let mut set: HashSet<Vec<u8>> = HashSet::with_capacity(n);
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(n);
    while keys.len() < n {
        let mut b = vec![0u8; key_size];
        rng.fill_bytes(&mut b);
        if set.insert(b.clone()) {
            keys.push(b);
        }
    }
    drop(set);

    let widths = [16u32, 24u32, 32u32];
    let col_w = 16;

    print!("{:<10}", "algo");
    for &w in &widths {
        print!(" {:>cw$}", format!("{}-bit", w), cw = col_w);
    }
    println!();

    // Exact occupancy expectation: nonempty buckets = m * (1 - (1 - 1/m)^N), approximately m * (1 - e^(-N/m)).
    // Expected collisions = N - expected nonempty buckets; for N << m this approaches N(N-1)/(2m).
    print!("{:<10}", "Expected collisions");
    for &w in &widths {
        let m = (1u64 << w) as f64;
        let n_f = n as f64;
        let unique = m * (1.0 - (-n_f / m).exp());
        let exp = n_f - unique;
        print!(" {:>cw$.2}", exp, cw = col_w);
    }
    println!();

    for a in algos {
        print!("{:<10}", a.name());
        for &w in &widths {
            let mask = if w == 64 { u64::MAX } else { (1u64 << w) - 1 };
            let mut seen: HashSet<u64> = HashSet::with_capacity(n);
            let mut col = 0usize;
            for k in &keys {
                let h = a.hash_u64(k) & mask;
                if !seen.insert(h) {
                    col += 1;
                }
            }
            print!(" {:>cw$}", col, cw = col_w);
        }
        println!();
    }
    println!();
    println!(
        "Expected collisions = N - m*(1-e^(-N/m)), including N above bucket count m=2^w;\n      for N << m this approaches N(N-1)/(2m). The 64-bit expectation is <1e-7 for this N and is omitted."
    );
}

struct Args {
    speed: bool,
    collision: bool,
    collision_n: usize,
    collision_key_size: usize,
    sizes: Vec<usize>,
}

fn parse_args() -> Args {
    let mut a = Args {
        speed: true,
        collision: true,
        collision_n: 1_000_000,
        collision_key_size: 16,
        sizes: vec![8, 16, 32, 64, 256, 1024, 4096],
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--no-speed" => a.speed = false,
            "--no-collision" => a.collision = false,
            "--collision-n" => {
                a.collision_n = it
                    .next()
                    .expect("--collision-n requires an integer argument")
                    .parse()
                    .expect("--collision-n must be a positive integer");
            }
            "--collision-key-size" => {
                a.collision_key_size = it
                    .next()
                    .expect("--collision-key-size requires an integer argument")
                    .parse()
                    .expect("--collision-key-size must be a positive integer");
            }
            "--sizes" => {
                let csv = it
                    .next()
                    .expect("--sizes requires a CSV list, such as 8,64,1024");
                a.sizes = csv
                    .split(',')
                    .map(|x| x.trim().parse().expect("size must be a positive integer"))
                    .collect();
            }
            "-h" | "--help" => {
                eprintln!(
                    "hash_bench [--no-speed] [--no-collision]\n           \
                     [--collision-n N] [--collision-key-size N]\n           \
                     [--sizes 8,16,64,256,...]"
                );
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

fn main() {
    let args = parse_args();

    let algos: Vec<Box<dyn HashAlgo>> = vec![
        Box::new(AHashAlgo::new()),
        Box::new(SipHashAlgo),
        Box::new(Fnv1aAlgo),
        Box::new(Sha256Algo),
        Box::new(Md5Algo),
        Box::new(Crc32Algo),
    ];

    println!("hash_bench");
    println!(
        "Algorithms: ahash, SipHash-1-3 (std DefaultHasher), FNV-1a (64), SHA-256, MD5, CRC32"
    );
    println!("SHA-256/MD5 use the first eight digest bytes as u64; other algorithms return u64 directly.");

    if args.speed {
        bench_speed(&algos, &args.sizes);
    }
    if args.collision {
        bench_collision(&algos, args.collision_n, args.collision_key_size);
    }
}
